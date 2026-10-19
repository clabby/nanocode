use crate::tools::SessionEnvironment;
use serde_json::{Value, json};

pub const MAX_COMMAND_BYTES: usize = 1024 * 1024;

pub const MAX_DESCRIPTION_BYTES: usize = 1024;

pub const MAX_STREAM_BYTES: usize = 30_000;

pub const DEFAULT_TIMEOUT_MS: u64 = 120_000;

pub const MAX_TIMEOUT_MS: u64 = 600_000;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BashRequest {
    pub command: String,

    pub description: Option<String>,

    pub timeout_ms: u64,

    pub max_stdout_bytes: usize,

    pub max_stderr_bytes: usize,

    pub session: Option<SessionEnvironment>,
}

impl BashRequest {
    #[cfg(not(target_family = "wasm"))]
    pub fn apply_session(&self, command: &mut std::process::Command) {
        SessionEnvironment::apply_or_clear(self.session.as_ref(), command);
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BashResult {
    pub stdout: String,

    pub stderr: String,

    pub exit_code: i32,

    pub truncated: bool,
}

pub trait SandboxBashExecutor: Send + Sync {
    fn execute(
        &self,
        request: BashRequest,
    ) -> impl std::future::Future<Output = Result<BashResult, String>> + Send;
}

pub enum BashExecution {
    Completed(BashResult),

    Background { task_id: String },
}

pub trait BashExecutor: Send + Sync {
    fn execute_with_background(
        &self,
        request: BashRequest,
        background: bool,
    ) -> impl std::future::Future<Output = Result<BashExecution, String>> + Send {
        async move {
            if background {
                return Err("this executor does not support background commands".into());
            }
            self.execute(request).await.map(BashExecution::Completed)
        }
    }

    fn execute(
        &self,
        request: BashRequest,
    ) -> impl std::future::Future<Output = Result<BashResult, String>> + Send;
}

impl<E: SandboxBashExecutor> BashExecutor for E {
    async fn execute(&self, request: BashRequest) -> Result<BashResult, String> {
        SandboxBashExecutor::execute(self, request).await
    }
}

pub struct ClaudeBash<E: BashExecutor> {
    executor: E,
}

impl<E: BashExecutor> ClaudeBash<E> {
    pub const fn new(executor: E) -> Self {
        Self { executor }
    }

    #[must_use]
    pub fn definitions() -> Vec<Value> {
        ["Bash"]
            .into_iter()
            .map(crate::upstream::tool_value)
            .collect()
    }

    pub async fn execute(&self, name: &str, input: Value) -> Result<String, String> {
        self.execute_in_session(name, input, None).await
    }

    pub async fn execute_in_session(
        &self,
        name: &str,
        input: Value,
        session: Option<SessionEnvironment>,
    ) -> Result<String, String> {
        if name != "Bash" {
            return Err(format!("unknown Claude Bash tool: {name}"));
        }
        let fields = input.as_object().ok_or("Bash input must be an object")?;
        for key in fields.keys() {
            if !matches!(
                key.as_str(),
                "command"
                    | "description"
                    | "timeout"
                    | "run_in_background"
                    | "dangerouslyDisableSandbox"
            ) {
                return Err(format!("unsupported Bash option: {key}"));
            }
        }

        if fields
            .get("dangerouslyDisableSandbox")
            .map_or(Some(false), Value::as_bool)
            .ok_or("invalid dangerouslyDisableSandbox")?
        {
            return Err("this host does not authorize disabling the sandbox".into());
        }
        let background = fields
            .get("run_in_background")
            .map_or(Some(false), Value::as_bool)
            .ok_or("invalid run_in_background")?;
        let command = fields
            .get("command")
            .and_then(Value::as_str)
            .ok_or("missing or invalid command")?;
        if command.trim().is_empty() || command.len() > MAX_COMMAND_BYTES {
            return Err(format!(
                "command must be nonblank and at most {MAX_COMMAND_BYTES} bytes"
            ));
        }
        let description = match fields.get("description") {
            None => None,
            Some(value) => {
                let text = value.as_str().ok_or("invalid description")?;
                if text.len() > MAX_DESCRIPTION_BYTES {
                    return Err(format!("description exceeds {MAX_DESCRIPTION_BYTES} bytes"));
                }
                Some(text.to_owned())
            }
        };
        let timeout_ms = fields
            .get("timeout")
            .map_or(Some(DEFAULT_TIMEOUT_MS), Value::as_u64)
            .ok_or("invalid timeout")?;
        let maximum = MAX_TIMEOUT_MS;
        if !(1..=maximum).contains(&timeout_ms) {
            return Err(format!("timeout must be 1 to {maximum} milliseconds"));
        }
        let request = BashRequest {
            command: command.to_owned(),
            description,
            timeout_ms,
            max_stdout_bytes: MAX_STREAM_BYTES,
            max_stderr_bytes: MAX_STREAM_BYTES,
            session,
        };
        let result = self
            .executor
            .execute_with_background(request, background)
            .await
            .map_err(|e| {
                let (bounded, _) = cap_utf8(&e, MAX_STREAM_BYTES);
                format!("command executor: {bounded}")
            })?;
        let result = match result {
            BashExecution::Completed(result) => result,
            BashExecution::Background { task_id } => {
                return Ok(json!({
                    "task_id": task_id, "status": "running", "background": true,
                })
                .to_string());
            }
        };
        let (stdout, stdout_cut) = cap_utf8(&result.stdout, MAX_STREAM_BYTES);
        let (stderr, stderr_cut) = cap_utf8(&result.stderr, MAX_STREAM_BYTES);
        Ok(json!({
            "stdout": stdout,
            "stderr": stderr,
            "exit_code": result.exit_code,
            "interrupted": false,
            "truncated": result.truncated || stdout_cut || stderr_cut
        })
        .to_string())
    }
}

fn cap_utf8(value: &str, max: usize) -> (&str, bool) {
    if value.len() <= max {
        return (value, false);
    }
    (&value[..value.floor_char_boundary(max)], true)
}
