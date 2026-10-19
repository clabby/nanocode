mod executors;

use std::sync::Arc;

pub use executors::{CodeModeExecutors, ExecutorSelection, HandRoute, Placement};

use crate::embedded::{CodeModeUpdate, NestedToolCall};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::{
    Tool, ToolContext, ToolDefinition,
    embedded::{
        CodeModeExecution, CodeModeHost, CodeModeHostError, CodeModeObserver, EmbeddedToolMode,
        HostFuture, OwnedToolContext,
    },
};

#[derive(Clone, Serialize, Deserialize)]
pub struct RemoteContext {
    pub model: String,
    pub session_id: String,
    pub call_id: String,
    pub turn_id: Option<String>,
    #[serde(skip)]
    pub history: Arc<Vec<nanocode_types_core::responses::ResponseItem>>,
    #[serde(
        serialize_with = "serialize_output_budget",
        deserialize_with = "deserialize_output_budget"
    )]
    pub output_token_budget: usize,
    pub instruction_revision: Option<u64>,
    pub host_context: Option<String>,
}

fn serialize_output_budget<S: serde::Serializer>(
    value: &usize,
    serializer: S,
) -> Result<S::Ok, S::Error> {
    serializer.serialize_u64(if *value == usize::MAX {
        u64::MAX
    } else {
        *value as u64
    })
}

fn deserialize_output_budget<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> Result<usize, D::Error> {
    let value = u64::deserialize(deserializer)?;
    if value == u64::MAX {
        Ok(usize::MAX)
    } else {
        usize::try_from(value).map_err(serde::de::Error::custom)
    }
}

impl RemoteContext {
    pub fn from_context(context: ToolContext<'_>) -> Self {
        Self {
            model: context.model().into(),
            session_id: context.session_id().into(),
            call_id: context.call_id().into(),
            turn_id: context.turn_id().map(Into::into),
            history: Arc::new(context.history().to_vec()),
            output_token_budget: context.output_token_budget(),
            instruction_revision: context.instruction_revision(),
            host_context: context.host_context().map(Into::into),
        }
    }

    pub fn owned(&self) -> OwnedToolContext {
        OwnedToolContext::new(
            &self.model,
            &self.session_id,
            &self.call_id,
            Arc::clone(&self.history),
            self.output_token_budget,
        )
        .with_turn_id(self.turn_id.as_deref().map(Arc::from))
        .with_instruction_revision(self.instruction_revision)
        .with_host_context(self.host_context.as_deref().map(Arc::from))
    }
}

#[derive(Serialize, Deserialize)]
pub enum RemoteOperation {
    Execute {
        source: String,
        context: RemoteContext,
        local_tools: Vec<ToolDefinition>,
    },
    Wait {
        input: String,
        context: RemoteContext,
    },
    DetachTurn {
        session_id: String,
    },
    CancelTurn {
        session_id: String,
    },
    Cancel {
        session_id: String,
    },
    BeginTurn {
        session_id: String,
    },
    Preempt {
        session_id: String,
    },
    SettlePreempt {
        session_id: String,
    },
}

pub trait RemoteTransport: Send + Sync + 'static {
    fn request<'a>(
        &'a self,
        operation: RemoteOperation,
        local_tools: Vec<Arc<dyn Tool>>,
        observer: Option<&'a mut dyn CodeModeObserver>,
    ) -> HostFuture<'a, Result<Option<CodeModeExecution>, CodeModeHostError>>;
    fn notify(&self, operation: RemoteOperation) -> Result<(), CodeModeHostError>;
    fn detach_turn(
        &self,
        session_id: &str,
        observer: &mut (dyn FnMut(&str) -> Box<dyn CodeModeObserver> + Send),
    ) -> Result<(), CodeModeHostError>;
}

pub struct RemoteCodeModeHost {
    transport: Arc<dyn RemoteTransport>,
    definitions: Vec<ToolDefinition>,
}

impl RemoteCodeModeHost {
    pub fn new(transport: impl RemoteTransport, definitions: Vec<ToolDefinition>) -> Self {
        Self {
            transport: Arc::new(transport),
            definitions,
        }
    }

    async fn run<'a>(
        &'a self,
        operation: RemoteOperation,
        tools: Vec<Arc<dyn Tool>>,
        observer: Option<&'a mut dyn CodeModeObserver>,
    ) -> Result<CodeModeExecution, CodeModeHostError> {
        self.transport
            .request(operation, tools, observer)
            .await?
            .ok_or_else(|| CodeModeHostError::new("remote runner omitted execution result"))
    }
}

impl CodeModeHost for RemoteCodeModeHost {
    fn tool_mode(&self) -> EmbeddedToolMode {
        EmbeddedToolMode::CodeOnly
    }
    fn supports_cells(&self) -> bool {
        true
    }
    fn tool_definitions(&self, _: &str) -> Result<Vec<ToolDefinition>, CodeModeHostError> {
        Ok(self.definitions.clone())
    }
    fn execute<'a>(
        &'a self,
        source: &'a str,
        context: ToolContext<'a>,
    ) -> HostFuture<'a, Result<CodeModeExecution, CodeModeHostError>> {
        self.execute_with_local_tools(source, context, Vec::new(), None)
    }
    fn execute_with_updates<'a>(
        &'a self,
        source: &'a str,
        context: ToolContext<'a>,
        observer: &'a mut dyn CodeModeObserver,
    ) -> HostFuture<'a, Result<CodeModeExecution, CodeModeHostError>> {
        self.execute_with_local_tools(source, context, Vec::new(), Some(observer))
    }
    fn execute_with_local_tools<'a>(
        &'a self,
        source: &'a str,
        context: ToolContext<'a>,
        tools: Vec<Arc<dyn Tool>>,
        observer: Option<&'a mut dyn CodeModeObserver>,
    ) -> HostFuture<'a, Result<CodeModeExecution, CodeModeHostError>> {
        let operation = RemoteOperation::Execute {
            source: source.into(),
            context: RemoteContext::from_context(context),
            local_tools: tools.iter().map(|tool| tool.definition()).collect(),
        };
        Box::pin(async move { self.run(operation, tools, observer).await })
    }
    fn wait_with_updates<'a>(
        &'a self,
        input: &'a str,
        context: ToolContext<'a>,
        observer: &'a mut dyn CodeModeObserver,
    ) -> HostFuture<'a, Result<CodeModeExecution, CodeModeHostError>> {
        Box::pin(async move {
            self.run(
                RemoteOperation::Wait {
                    input: input.into(),
                    context: RemoteContext::from_context(context),
                },
                Vec::new(),
                Some(observer),
            )
            .await
        })
    }
    fn begin_turn(&self, session_id: &str) {
        if let Err(error) = self.transport.notify(RemoteOperation::BeginTurn {
            session_id: session_id.into(),
        }) {
            tracing::error!(%error, "remote begin turn failed");
        }
    }
    fn settle_preempt(&self, session_id: &str) {
        if let Err(error) = self.transport.notify(RemoteOperation::SettlePreempt {
            session_id: session_id.into(),
        }) {
            tracing::error!(%error, "remote settle preempt failed");
        }
    }
    fn preempt_turn<'a>(
        &'a self,
        session_id: &'a str,
    ) -> HostFuture<'a, Result<(), CodeModeHostError>> {
        Box::pin(async move {
            self.transport
                .request(
                    RemoteOperation::Preempt {
                        session_id: session_id.into(),
                    },
                    Vec::new(),
                    None,
                )
                .await
                .map(|_| ())
        })
    }
    fn detach_turn(
        &self,
        session_id: &str,
        observer: &mut (dyn FnMut(&str) -> Box<dyn CodeModeObserver> + Send),
    ) {
        if let Err(error) = self.transport.detach_turn(session_id, observer) {
            tracing::error!(%error, "remote detach turn failed");
        }
    }
    fn cancel_turn<'a>(
        &'a self,
        session_id: &'a str,
    ) -> HostFuture<'a, Result<(), CodeModeHostError>> {
        Box::pin(async move {
            self.transport
                .request(
                    RemoteOperation::CancelTurn {
                        session_id: session_id.into(),
                    },
                    Vec::new(),
                    None,
                )
                .await
                .map(|_| ())
        })
    }
    fn cancel_with_updates<'a>(
        &'a self,
        session_id: &'a str,
        observer: &'a mut dyn CodeModeObserver,
    ) -> HostFuture<'a, Result<(), CodeModeHostError>> {
        Box::pin(async move {
            self.transport
                .request(
                    RemoteOperation::Cancel {
                        session_id: session_id.into(),
                    },
                    Vec::new(),
                    Some(observer),
                )
                .await
                .map(|_| ())
        })
    }
    fn cancel_turn_with_updates<'a>(
        &'a self,
        session_id: &'a str,
        observer: &'a mut dyn CodeModeObserver,
    ) -> HostFuture<'a, Result<(), CodeModeHostError>> {
        Box::pin(async move {
            self.transport
                .request(
                    RemoteOperation::CancelTurn {
                        session_id: session_id.into(),
                    },
                    Vec::new(),
                    Some(observer),
                )
                .await
                .map(|_| ())
        })
    }
    fn cancel<'a>(&'a self, session_id: &'a str) -> HostFuture<'a, Result<(), CodeModeHostError>> {
        Box::pin(async move {
            self.transport
                .request(
                    RemoteOperation::Cancel {
                        session_id: session_id.into(),
                    },
                    Vec::new(),
                    None,
                )
                .await
                .map(|_| ())
        })
    }
}

#[derive(Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum RemoteFrame {
    Catalog {
        definitions: Vec<ToolDefinition>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        name: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        workspace: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        runtime_id: Option<String>,
    },
    Request {
        id: u64,
        operation: RemoteOperation,
    },
    Reply {
        id: u64,
        result: Result<Option<Value>, String>,
    },
    Update {
        id: u64,
        update: Value,
    },
    Callback {
        id: u64,
        name: String,
        input: Value,
        freeform: bool,
        context: RemoteContext,
    },
    CallbackCancel {
        id: u64,
    },
    DetachedUpdate {
        session_id: String,
        origin_call_id: String,
        update: Value,
    },
    DetachedComplete {
        session_id: String,
        origin_call_id: String,
    },
    CallbackResult {
        id: u64,
        result: Result<Value, String>,
    },
}

impl RemoteFrame {
    pub fn encode(&self, max_frame_bytes: usize) -> Result<String, CodeModeHostError> {
        let text = serde_json::to_string(self)
            .map_err(|error| CodeModeHostError::new(error.to_string()))?;
        if text.len() > max_frame_bytes {
            return Err(CodeModeHostError::new(
                "remote frame exceeds caller max_frame_bytes policy",
            ));
        }
        Ok(text)
    }
    pub fn decode(text: &str, max_frame_bytes: usize) -> Result<Self, CodeModeHostError> {
        if text.len() > max_frame_bytes {
            return Err(CodeModeHostError::new(
                "remote frame exceeds caller max_frame_bytes policy",
            ));
        }
        serde_json::from_str(text).map_err(|error| CodeModeHostError::new(error.to_string()))
    }
}

pub fn nested(call: &NestedToolCall) -> Value {
    json!({"call_id":call.call_id,"name":call.name,"input":call.input,"output":call.output,"structured_result":call.structured_result,"success":call.success,"started_after_ns":call.started_after_ns,"duration_ns":call.duration_ns,"metadata":call.metadata})
}
pub fn execution_to_wire(value: CodeModeExecution) -> Value {
    json!({"cell":value.cell,"output":value.output,"success":value.success,"nested_calls":value.nested_calls.iter().map(nested).collect::<Vec<_>>(),"notifications":value.notifications.iter().map(|n| json!({"call_id":n.call_id,"text":n.text})).collect::<Vec<_>>()})
}
pub fn deliver_update(
    observer: &mut dyn CodeModeObserver,
    value: Value,
) -> Result<(), CodeModeHostError> {
    if let Some(call) = value.get("completed") {
        let call: NestedToolCall = serde_json::from_value(call.clone())
            .map_err(|error| CodeModeHostError::new(error.to_string()))?;
        observer.update(CodeModeUpdate::NestedCallCompleted(&call));
    } else {
        let call_id = value["call_id"]
            .as_str()
            .ok_or_else(|| CodeModeHostError::new("missing update call_id"))?;
        let name = value["name"]
            .as_str()
            .ok_or_else(|| CodeModeHostError::new("missing update name"))?;
        observer.update(CodeModeUpdate::NestedCallStarted {
            call_id,
            name,
            input: &value["input"],
        });
    }
    Ok(())
}
