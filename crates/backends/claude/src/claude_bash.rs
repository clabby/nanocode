use crate::tools::{BashExecution, BashExecutor, BashRequest, BashResult, ClaudeBash};
use async_trait::async_trait;
use nanocode_sync::{Atom, Signal};
use nanocode_tool_host::{
    SessionEnvironment, Tool, ToolContext, ToolDefinition, ToolInput, ToolOutput, ToolResult,
    ToolsBuilder,
};
use serde_json::{Value, json};
use std::{
    collections::BTreeMap,
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};
use tokio::sync::watch;

type Outcome = Result<BashResult, String>;
struct Job {
    result: watch::Receiver<Option<Outcome>>,
    finished: Arc<Signal>,
    abort: tokio::task::AbortHandle,
}
struct Finished(Arc<Signal>);
impl Drop for Finished {
    fn drop(&mut self) {
        self.0.fire();
    }
}
impl Drop for Job {
    fn drop(&mut self) {
        self.abort.abort();
    }
}
#[derive(Clone, Default)]
struct Entries {
    closed: bool,
    jobs: BTreeMap<String, Arc<Job>>,
}
#[derive(Default)]
pub(super) struct Jobs {
    next: AtomicU64,
    entries: Atom<Entries>,
}
impl Jobs {
    pub(super) async fn shutdown(&self) {
        let jobs = self.entries.update(|entries| {
            entries.closed = true;
            entries.jobs.values().cloned().collect::<Vec<_>>()
        });
        for job in &jobs {
            job.abort.abort();
        }
        for job in jobs {
            job.finished.wait().await;
        }
    }
}
#[derive(Clone)]
struct LocalBash {
    workspace: PathBuf,
    jobs: Arc<Jobs>,
    executor: Arc<dyn nanocode_process_contracts::ForegroundExecutor>,
}

async fn run(
    executor: &dyn nanocode_process_contracts::ForegroundExecutor,
    workspace: PathBuf,
    request: BashRequest,
    duration: Duration,
) -> Outcome {
    let output = executor
        .execute(nanocode_process_contracts::ForegroundCommand {
            workspace: &workspace,
            command: &request.command,
            timeout: duration,
            max_stdout: request.max_stdout_bytes,
            max_stderr: request.max_stderr_bytes,
            session: request.session.as_ref(),
        })
        .await?;
    Ok(BashResult {
        stdout: output.stdout,
        stderr: output.stderr,
        exit_code: output.exit_code,
        truncated: output.truncated,
    })
}
impl BashExecutor for LocalBash {
    async fn execute(&self, request: BashRequest) -> Outcome {
        let timeout = Duration::from_millis(request.timeout_ms);
        run(
            self.executor.as_ref(),
            self.workspace.clone(),
            request,
            timeout,
        )
        .await
    }
    async fn execute_with_background(
        &self,
        request: BashRequest,
        background: bool,
    ) -> Result<BashExecution, String> {
        let foreground_timeout = Duration::from_millis(request.timeout_ms);
        let hard_timeout = if background {
            Duration::MAX
        } else {
            foreground_timeout + Duration::from_secs(1800)
        };
        let id = format!(
            "{}-bash-{}",
            request
                .session
                .as_ref()
                .map_or("local", SessionEnvironment::session_id),
            self.jobs.next.fetch_add(1, Ordering::Relaxed)
        );
        let (sender, receiver) = watch::channel(None);
        let job = {
            let workspace = self.workspace.clone();
            let executor = self.executor.clone();
            let finished = Arc::new(Signal::new());
            let admitted = Arc::new(Signal::new());
            let guard = Finished(Arc::clone(&finished));
            let start = Arc::clone(&admitted);
            let task = tokio::spawn(async move {
                let _finished = guard;
                start.wait().await;
                let result = run(executor.as_ref(), workspace, request, hard_timeout).await;
                let _ = sender.send(Some(result));
            });
            let job = Arc::new(Job {
                abort: task.abort_handle(),
                finished,
                result: receiver,
            });
            self.jobs
                .entries
                .try_update(|entries| {
                    (!entries.closed).then(|| {
                        entries.jobs.insert(id.clone(), Arc::clone(&job));
                    })
                })
                .ok_or("Bash is unavailable because the agent is shutting down")?;
            admitted.fire();
            job
        };

        struct Admission(Option<tokio::task::AbortHandle>);
        impl Drop for Admission {
            fn drop(&mut self) {
                if let Some(abort) = self.0.take() {
                    abort.abort();
                }
            }
        }
        let mut admission = Admission(Some(job.abort.clone()));
        if !background
            && let Ok(result) = tokio::time::timeout(foreground_timeout, completion(&job)).await
        {
            self.jobs.entries.update(|entries| {
                entries.jobs.remove(&id);
            });
            return result.map(BashExecution::Completed);
        }
        admission.0 = None;
        Ok(BashExecution::Background { task_id: id })
    }
}
async fn completion(job: &Job) -> Outcome {
    let mut receiver = job.result.clone();
    loop {
        if let Some(result) = receiver.borrow_and_update().clone() {
            return result;
        }
        if receiver.changed().await.is_err() {
            return Err("Bash task stopped".into());
        }
    }
}

pub(super) fn install(
    mut tools: ToolsBuilder,
    workspace: PathBuf,
    session: Option<SessionEnvironment>,
    jobs: Arc<Jobs>,
    process: Arc<dyn nanocode_process_contracts::ForegroundExecutor>,
) -> ToolsBuilder {
    let executor = LocalBash {
        workspace,
        jobs,
        executor: process,
    };
    tools = tools.tool(BashTool {
        bash: ClaudeBash::new(executor.clone()),
        session,
    });
    for name in ["TaskOutput", "TaskStop"] {
        tools = tools.tool(TaskTool {
            name,
            jobs: executor.jobs.clone(),
        });
    }
    tools
}
struct BashTool {
    bash: ClaudeBash<LocalBash>,
    session: Option<SessionEnvironment>,
}
#[async_trait]
impl Tool for BashTool {
    fn definition(&self) -> ToolDefinition {
        let schema = ClaudeBash::<LocalBash>::definitions().remove(0);
        ToolDefinition::function(
            "Bash",
            schema["description"].as_str().unwrap_or_default(),
            schema["input_schema"].clone(),
        )
    }
    async fn execute(&self, input: ToolInput, _context: ToolContext<'_>) -> ToolResult {
        let input = input.decode_json::<Value>()?;
        Ok(
            match self
                .bash
                .execute_in_session("Bash", input, self.session.clone())
                .await
            {
                Ok(output) => {
                    let value = serde_json::from_str::<Value>(&output).unwrap_or(Value::Null);
                    ToolOutput::text(output).with_structured_result(value)
                }
                Err(error) => ToolOutput::error(error),
            },
        )
    }
}
struct TaskTool {
    name: &'static str,
    jobs: Arc<Jobs>,
}
#[async_trait]
impl Tool for TaskTool {
    fn definition(&self) -> ToolDefinition {
        if self.name == "TaskStop" {
            let definition = crate::upstream::tool_value(self.name);
            return ToolDefinition::function(
                self.name,
                definition["description"]
                    .as_str()
                    .expect("upstream tool description"),
                definition["input_schema"].clone(),
            );
        }
        ToolDefinition::function(
            self.name,
            "SDK extension: retrieve a session-owned Bash task's result, optionally waiting for completion.",
            json!({"type":"object","properties":{"task_id":{"type":"string"},"block":{"type":"boolean","default":true},"timeout":{"type":"integer","minimum":0,"maximum":300000,"default":30000}},"required":["task_id"],"additionalProperties":false}),
        )
    }
    async fn execute(&self, input: ToolInput, _context: ToolContext<'_>) -> ToolResult {
        let input = input.decode_json::<Value>()?;
        Ok(match self.invoke(input).await {
            Ok(value) => ToolOutput::text(value.to_string()).with_structured_result(value),
            Err(error) => ToolOutput::error(error),
        })
    }
}
impl TaskTool {
    async fn invoke(&self, input: Value) -> Result<Value, String> {
        let fields = input.as_object().ok_or("task input must be an object")?;
        for key in fields.keys() {
            if key != "task_id"
                && !(self.name == "TaskStop" && key == "shell_id")
                && !(self.name == "TaskOutput" && matches!(key.as_str(), "block" | "timeout"))
            {
                return Err(format!("unsupported task option: {key}"));
            }
        }
        if self.name == "TaskStop"
            && fields.contains_key("task_id")
            && fields.contains_key("shell_id")
        {
            return Err("provide task_id or its deprecated shell_id alias, not both".into());
        }
        let id = fields
            .get("task_id")
            .or_else(|| {
                if self.name == "TaskStop" {
                    fields.get("shell_id")
                } else {
                    None
                }
            })
            .and_then(Value::as_str)
            .ok_or("task_id or its deprecated shell_id alias must be a string")?;
        let job = self
            .jobs
            .entries
            .read(|entries| entries.jobs.get(id).cloned())
            .ok_or("unknown task_id in this session")?;
        if self.name == "TaskStop" {
            job.abort.abort();
            job.finished.wait().await;
            return Ok(
                json!({"task_id":id,"status":if job.result.borrow().is_some() {"completed"} else {"stopped"}}),
            );
        }
        let block = input
            .get("block")
            .map_or(Some(true), Value::as_bool)
            .ok_or("block must be boolean")?;
        let timeout = input
            .get("timeout")
            .map_or(Some(30000), Value::as_u64)
            .filter(|v| *v <= 300000)
            .ok_or("timeout must be 0..300000 milliseconds")?;
        let result = if block {
            tokio::time::timeout(Duration::from_millis(timeout), completion(&job))
                .await
                .ok()
        } else {
            job.result.borrow().clone().or_else(|| {
                job.abort
                    .is_finished()
                    .then(|| Err("Bash task stopped".into()))
            })
        };
        Ok(match result {
            None => json!({"task_id":id,"status":"running"}),
            Some(Err(error)) => json!({"task_id":id,"status":"error","error":error}),
            Some(Ok(result)) => {
                json!({"task_id":id,"status":"completed","stdout":result.stdout,"stderr":result.stderr,"exit_code":result.exit_code,"truncated":result.truncated})
            }
        })
    }
}
