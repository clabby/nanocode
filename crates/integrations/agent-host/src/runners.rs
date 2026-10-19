pub use nanocode_tool_runtime::attachment::{
    AttachmentError, Runner, RunnerEvent, RunnerPool, RunnerProtocol, RunnerServerConfig,
    RunnerStatus,
};

use std::{
    collections::HashMap,
    sync::{Arc, Mutex, PoisonError},
};
use tokio::sync::{mpsc, oneshot};

pub(crate) struct RunnerAgents {
    pool: RunnerPool,
    state: Mutex<RunnerAgentState>,
    initialized: tokio::sync::OnceCell<Result<(), AttachmentError>>,
}

const NOTICE_PREFIX: &str = "Runner ";

struct RunnerAgentState {
    code_mode_hosts: HashMap<String, bool>,
    events: Option<mpsc::UnboundedReceiver<RunnerEvent>>,
    agents: Vec<(String, crate::WeakAgent, mpsc::UnboundedSender<Notice>)>,
    folded: Vec<(String, crate::WeakAgent, String)>,
    runners: Vec<Runner>,
    servers: Vec<RunnerServerConfig>,
    startup: Vec<tokio::sync::watch::Receiver<Option<Result<(), AttachmentError>>>>,
}

struct Notice {
    text: String,
    prefix: Option<String>,
    completed: Option<oneshot::Sender<nanocode_agent_core::Result<()>>>,
}

impl RunnerAgents {
    pub(crate) fn new(
        pool: RunnerPool,
        runners: Vec<Runner>,
        servers: Vec<RunnerServerConfig>,
    ) -> Arc<Self> {
        let events = pool.subscribe();
        Arc::new(Self {
            pool,
            initialized: tokio::sync::OnceCell::new(),
            state: Mutex::new(RunnerAgentState {
                code_mode_hosts: HashMap::new(),
                events: Some(events),
                agents: Vec::new(),
                folded: Vec::new(),
                runners,
                servers,
                startup: Vec::new(),
            }),
        })
    }

    pub(crate) fn activate(self: &Arc<Self>) {
        let Ok(runtime) = tokio::runtime::Handle::try_current() else {
            return;
        };
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        let (events, runners, servers) = (
            state.events.take(),
            std::mem::take(&mut state.runners),
            std::mem::take(&mut state.servers),
        );
        if let Some(mut events) = events {
            let weak = Arc::downgrade(self);
            runtime.spawn(async move {
                while let Some(event) = events.recv().await {
                    let Some(agents) = weak.upgrade() else { break };
                    agents.notify(event);
                }
            });
        }
        for runner in runners {
            if let Err(error) = self.pool.keep_connected(runner) {
                state
                    .startup
                    .push(tokio::sync::watch::channel(Some(Err(error))).1);
            }
        }
        for server in servers {
            let pool = self.pool.clone();
            let (receipt, result) = tokio::sync::watch::channel(None);
            runtime.spawn(async move {
                let outcome = match pool.listen(server).await {
                    Ok(_) => Ok(()),
                    Err(error) => {
                        pool.report_failure("runner-server", error.to_string());
                        Err(error)
                    }
                };
                receipt.send_replace(Some(outcome));
            });
            state.startup.push(result);
        }
    }

    pub(crate) async fn initialize(self: &Arc<Self>) -> Result<(), AttachmentError> {
        tokio::runtime::Handle::try_current()
            .map_err(|error| AttachmentError::Transport(error.to_string().into()))?;
        self.activate();
        self.initialized
            .get_or_init(|| async {
                let startup = self
                    .state
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .startup
                    .clone();
                let mut result = Ok(());
                for mut receipt in startup {
                    let outcome = loop {
                        if let Some(outcome) = receipt.borrow().clone() {
                            break outcome;
                        }
                        if receipt.changed().await.is_err() {
                            break Err(AttachmentError::Transport(
                                "runner startup stopped before completion".into(),
                            ));
                        }
                    };
                    if result.is_ok() {
                        result = outcome;
                    }
                }
                result
            })
            .await
            .clone()
    }

    fn summary(&self) -> String {
        let runners = self.pool.list();
        let ready = |protocol| {
            runners.iter().filter(move |runner| {
                runner.status == RunnerStatus::Ready && runner.protocol == protocol
            })
        };
        let mounts = ready(RunnerProtocol::Tools)
            .map(|runner| {
                format!(
                    "/{} (mode: {:?}, tools: {})",
                    runner.name, runner.mode, runner.tool_count,
                )
            })
            .collect::<Vec<_>>();
        let executors = ready(RunnerProtocol::CodeMode)
            .map(|runner| {
                format!(
                    "{} (mode: {:?}, directory: {}, tools: {})",
                    runner.name,
                    runner.mode,
                    runner
                        .workspace
                        .as_ref()
                        .map_or_else(|| "unadvertised".into(), |path| path.display().to_string()),
                    runner.tool_count,
                )
            })
            .collect::<Vec<_>>();
        let mut summary = if mounts.is_empty() {
            "Runner mounts: no remote runners are currently available.".to_owned()
        } else {
            format!("Runner mounts currently available: {}.", mounts.join("; "))
        };
        if !executors.is_empty() {
            summary.push_str(&format!(
                " Hands connected: {}. The runners tool with action=select runs a session's exec cells on one hand, several, or every hand at once.",
                executors.join("; ")
            ));
        }
        summary
    }

    fn notify(&self, event: RunnerEvent) {
        let notice = match event {
            RunnerEvent::StatusChanged { info } => match info.status {
                RunnerStatus::Ready | RunnerStatus::Disconnected | RunnerStatus::Failed(_) => {
                    match info.protocol {
                        RunnerProtocol::Tools => format!(
                            "Runner /{} status: {:?}. {}",
                            info.name,
                            info.status,
                            self.summary()
                        ),
                        RunnerProtocol::CodeMode => format!(
                            "Hand {} status: {:?}. {}",
                            info.name,
                            info.status,
                            self.summary()
                        ),
                    }
                }
                RunnerStatus::Connecting => return,
            },
            RunnerEvent::Failed { name, error } => {
                format!("Runner /{name} failed: {error}. {}", self.summary())
            }
        };
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        state.agents.retain(|(_, agent, notices)| {
            agent.is_live()
                && notices
                    .send(Notice {
                        text: notice.clone(),
                        prefix: None,
                        completed: None,
                    })
                    .is_ok()
        });
    }

    pub(crate) fn unregister(&self, session_id: &str) {
        self.pool.code_mode_executors().forget(session_id);
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        state.agents.retain(|(id, _, _)| id != session_id);
        state.folded.retain(|(id, _, _)| id != session_id);
        state.code_mode_hosts.remove(session_id);
    }

    pub(crate) fn initial_notice(
        self: &Arc<Self>,
        session_id: &str,
        parent_session_id: Option<&str>,
    ) -> Option<String> {
        self.activate();
        let state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        let configured = state
            .code_mode_hosts
            .get(session_id)
            .copied()
            .or_else(|| {
                parent_session_id.and_then(|parent| state.code_mode_hosts.get(parent).copied())
            })
            .unwrap_or(false);
        (!configured).then(|| self.summary())
    }

    pub(crate) fn initial_notice_committed(&self, agent: &crate::Agent, text: String) {
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        state.folded.retain(|(_, agent, _)| agent.is_live());
        state
            .folded
            .push((agent.session_id().to_owned(), agent.downgrade(), text));
    }

    pub(crate) fn code_mode_host(&self, session_id: &str, configured: bool) {
        self.state
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .code_mode_hosts
            .insert(session_id.to_owned(), configured);
    }

    pub(crate) async fn register(
        self: &Arc<Self>,
        agent: &crate::Agent,
        await_initial: bool,
    ) -> nanocode_agent_core::Result<()> {
        {
            let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
            let configured = state
                .code_mode_hosts
                .get(agent.session_id())
                .copied()
                .or_else(|| {
                    agent
                        .session()
                        .lineage
                        .parent_session_id
                        .as_ref()
                        .and_then(|parent| state.code_mode_hosts.get(parent).copied())
                })
                .unwrap_or(false);
            if configured {
                state
                    .code_mode_hosts
                    .insert(agent.session_id().to_owned(), true);
                return Ok(());
            }
        }
        self.activate();
        if !agent.capabilities().developer_messages {
            return Ok(());
        }
        let weak = agent.downgrade();
        let worker = weak.clone();
        let (notices, mut receiver) = mpsc::unbounded_channel::<Notice>();
        let (completed, result) = oneshot::channel();
        let awaited;
        {
            let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
            state
                .agents
                .retain(|(_, agent, notices)| agent.is_live() && !notices.is_closed());
            state.folded.retain(|(_, agent, _)| agent.is_live());
            if state
                .agents
                .iter()
                .any(|(id, _, _)| id == agent.session_id())
            {
                return Ok(());
            }
            let folded = state
                .folded
                .iter()
                .position(|(id, _, _)| id == agent.session_id())
                .map(|index| state.folded.swap_remove(index).2);
            awaited = await_initial && folded.is_none();
            let summary = self.summary();
            if folded.as_ref() != Some(&summary) {
                notices
                    .send(Notice {
                        text: summary,
                        prefix: Some(NOTICE_PREFIX.to_owned()),
                        completed: awaited.then_some(completed),
                    })
                    .map_err(|_| crate::NanocodeError::AgentStopped)?;
            }
            state
                .agents
                .push((agent.session_id().to_owned(), weak, notices));
        }
        tokio::spawn(async move {
            while let Some(notice) = receiver.recv().await {
                let outcome = worker
                    .append_developer_notice(notice.text, notice.prefix)
                    .await
                    .unwrap_or(Err(crate::NanocodeError::AgentStopped))
                    .map(|_| ());
                let failed = outcome.is_err();
                if let Some(completed) = notice.completed {
                    let _ = completed.send(outcome);
                }
                if failed {
                    break;
                }
            }
        });
        if awaited {
            result
                .await
                .map_err(|_| crate::NanocodeError::AgentStopped)?
        } else {
            Ok(())
        }
    }
}
