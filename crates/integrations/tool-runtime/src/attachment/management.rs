use super::{Runner, RunnerInfo, RunnerPool, RunnerProtocol, RunnerServerConfig};
use crate::{
    Tool, ToolContext, ToolDefinition, ToolInput, ToolOutput, ToolResult,
    remote::{ExecutorSelection, HandRoute, Placement},
};
use async_trait::async_trait;
use serde::Deserialize;
use serde_json::{Value, json};
use std::time::Duration;

pub(crate) struct RunnerManagement {
    pub pool: RunnerPool,
}

#[derive(Deserialize)]
#[serde(tag = "action", rename_all = "snake_case", deny_unknown_fields)]
enum Request {
    List {},
    Connect {
        url: String,
        token: String,
        name: Option<String>,
        mode: Option<Mode>,
        call_timeout_ms: Option<u64>,
    },
    Listen {
        address: String,
        token: Option<String>,
        mode: Option<Mode>,
        call_timeout_ms: Option<u64>,
    },
    Select {
        name: Option<String>,
        names: Option<Vec<String>>,
    },
    Disconnect {
        name: String,
    },
    StopListening {},
}

#[derive(Deserialize)]
#[serde(rename_all = "snake_case")]
enum Mode {
    Alongside,
    Replace,
}

#[async_trait]
impl Tool for RunnerManagement {
    fn definition(&self) -> ToolDefinition {
        ToolDefinition::function(
            "runners",
            "Manage hands, the machines that run exec cells, shared by all agents; legacy tool runners mount at /name instead. action=list shows hands, listeners and where this session's exec cells run. action=connect requires url and token and connects a hand, which then runs this session's later exec cells with their stored values and running cells, or the cells of every session without its own selection when mode=replace. Subagents follow their root session. action=select routes this session's exec cells to hand name, to several hands at once with names or name=\"*\" for every connected hand, or back to this machine when both are omitted; cells sent to several hands run concurrently and their outputs are labelled per hand. action=listen requires address, accepts hands that dial in, and returns an endpoint and credential to give them; call it only when the user requests a listener. action=disconnect requires name; disconnecting a hand returns its sessions to this machine. action=stop_listening stops all listeners. Hands are announced in conversation messages; tool definitions stay unchanged. Credentials are excluded from list/connect outputs. call_timeout_ms is an optional caller deadline policy, in positive whole milliseconds.",
            json!({
                "type":"object",
                "properties":{
                    "action":{"type":"string","enum":["list","connect","listen","select","disconnect","stop_listening"]},
                    "url":{"type":"string"},
                    "token":{"type":"string"},
                    "name":{"type":"string"},
                    "names":{"type":"array","items":{"type":"string"},"minItems":1},
                    "address":{"type":"string"},
                    "mode":{"type":"string","enum":["alongside","replace"]},
                    "call_timeout_ms":{"type":"integer","minimum":1}
                },
                "required":["action"],
                "additionalProperties":false
            }),
        )
    }

    async fn execute(&self, input: ToolInput, context: ToolContext<'_>) -> ToolResult {
        let request: Request = input.decode_json()?;
        let session = context.session_id();
        let result = match request {
            Request::List {} => {
                json!({"runners":self.pool.list().into_iter().map(info).collect::<Vec<_>>(),"listeners":self.pool.servers().iter().map(|server| json!({"url":server.endpoint(),"token":"[REDACTED]"})).collect::<Vec<_>>(),"code_mode":self.placement(session)})
            }
            Request::Connect {
                url,
                token,
                name,
                mode,
                call_timeout_ms,
            } => {
                let mut config = Runner::connect(url).token(token);
                if let Some(name) = name {
                    config = config.name(name);
                }
                let replace = matches!(mode, Some(Mode::Replace));
                if replace {
                    config = config.replace();
                }
                if let Some(ms) = call_timeout_ms {
                    config = config.call_timeout(Duration::from_millis(ms));
                }
                let connected = self.pool.connect(config).await?.info();
                if connected.protocol == RunnerProtocol::CodeMode && !replace {
                    self.pool.code_mode_executors().select(
                        session,
                        Some(ExecutorSelection::Executor(connected.name.as_str().into())),
                    );
                }
                let protocol = connected.protocol;
                let mut result = info(connected);
                if protocol == RunnerProtocol::CodeMode {
                    result["code_mode"] = self.placement(session);
                }
                result
            }
            Request::Listen {
                address,
                token,
                mode,
                call_timeout_ms,
            } => {
                let mut config = RunnerServerConfig::listen(address);
                if let Some(token) = token {
                    config = config.token(token);
                }
                if matches!(mode, Some(Mode::Replace)) {
                    config = config.replace();
                }
                if let Some(ms) = call_timeout_ms {
                    config = config.call_timeout(Duration::from_millis(ms));
                }
                let server = self.pool.listen(config).await?;
                json!({"url":server.endpoint(), "token":server.token(), "instruction":"Give this endpoint and credential to the hand, for example `nanocode hand URL` with NANOCODE_RUNNER_TOKEN set to the credential."})
            }
            Request::Select { name, names } => {
                let executors = self.pool.code_mode_executors();
                let selection = match (name, names) {
                    (Some(_), Some(_)) => return Err("pass name or names, not both".into()),
                    (Some(name), None) if name == "*" => ExecutorSelection::All,
                    (Some(name), None) => {
                        executors
                            .connected(&name)
                            .map_err(|error| error.to_string())?;
                        ExecutorSelection::Executor(name.into())
                    }
                    (None, Some(names)) => {
                        for name in &names {
                            executors
                                .connected(name)
                                .map_err(|error| error.to_string())?;
                        }
                        ExecutorSelection::Executors(names.into_iter().map(Into::into).collect())
                    }
                    (None, None) => ExecutorSelection::Local,
                };
                executors.select(session, Some(selection));
                json!({"code_mode":self.placement(session)})
            }
            Request::Disconnect { name } => {
                self.pool.disconnect(&name).await?;
                json!({"name":name,"status":"disconnected"})
            }
            Request::StopListening {} => {
                for server in self.pool.servers() {
                    server.stop().await?;
                }
                json!({"status":"stopped_listening"})
            }
        };
        Ok(ToolOutput::json(&result).with_structured_result(result))
    }
}

impl RunnerManagement {
    fn placement(&self, session: &str) -> Value {
        match self.pool.code_mode_executors().placement(session) {
            Placement::Local => json!("this machine"),
            Placement::Hands { hands, broadcast } => {
                let note = if broadcast {
                    "Later exec cells for this session run on every listed hand at once, each starting in its own directory; outputs are labelled [hand <name>] and wait resumes running cells on each of them."
                } else {
                    "Later exec cells for this session run on this hand, starting in its directory; use its paths rather than this machine's."
                };
                json!({"hands":hands.iter().map(|hand| self.hand(hand, session)).collect::<Vec<_>>(),"broadcast":broadcast,"note":note})
            }
        }
    }

    fn hand(&self, hand: &HandRoute, session: &str) -> Value {
        let runner = self
            .pool
            .list()
            .into_iter()
            .find(|runner| *runner.name == *hand.name);
        let tools = hand
            .host
            .as_ref()
            .and_then(|host| host.tool_definitions(session).ok())
            .map(|definitions| {
                definitions
                    .iter()
                    .map(|definition| definition.name().to_owned())
                    .collect::<Vec<_>>()
            });
        json!({"name":&*hand.name,"status":runner.as_ref().map_or_else(|| "disconnected".to_owned(), |runner| status(&runner.status)),"directory":runner.and_then(|runner| runner.workspace),"tools":tools})
    }
}

fn status(status: &super::RunnerStatus) -> String {
    match status {
        super::RunnerStatus::Connecting => "connecting".to_owned(),
        super::RunnerStatus::Ready => "ready".to_owned(),
        super::RunnerStatus::Disconnected => "disconnected".to_owned(),
        super::RunnerStatus::Failed(error) => format!("failed: {error}"),
    }
}

fn info(info: RunnerInfo) -> Value {
    let mode = match info.mode {
        super::RemoteMode::Alongside => "alongside",
        super::RemoteMode::Replace => "replace",
    };
    match info.protocol {
        RunnerProtocol::Tools => {
            json!({"name":info.name,"protocol":"tools","mount":format!("/{}",info.name),"url":info.endpoint,"mode":mode,"status":status(&info.status),"tool_count":info.tool_count})
        }
        RunnerProtocol::CodeMode => {
            json!({"name":info.name,"protocol":"code_mode","url":info.endpoint,"mode":mode,"status":status(&info.status),"workspace":info.workspace,"tool_count":info.tool_count})
        }
    }
}
