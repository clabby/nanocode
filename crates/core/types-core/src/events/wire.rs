use std::sync::Arc;

use serde::{Deserialize, Serialize};
use serde_json::value::RawValue;

pub const AGENT_EVENT_PROTOCOL_VERSION: u32 = 1;

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct AgentEvent {
    pub protocol_version: u32,

    pub request_id: Arc<str>,

    pub seq: u64,

    #[serde(rename = "type")]
    pub kind: AgentEventKind,

    pub payload: Arc<RawValue>,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum AgentEventKind {
    #[serde(rename = "input.accepted")]
    InputAccepted,

    #[serde(rename = "api.event")]
    ApiEvent,

    #[serde(rename = "assistant.delta")]
    AssistantDelta,

    #[serde(rename = "assistant.message")]
    AssistantMessage,

    #[serde(rename = "reasoning.summary.delta")]
    ReasoningSummaryDelta,

    #[serde(rename = "run.started")]
    RunStarted,

    #[serde(rename = "run.steered")]
    RunSteered,

    #[serde(rename = "run.error")]
    RunError,

    #[serde(rename = "run.completed")]
    RunCompleted,

    #[serde(rename = "run.failed")]
    RunFailed,

    #[serde(rename = "tool.call")]
    ToolCall,

    #[serde(rename = "tool.result")]
    ToolResult,

    #[serde(rename = "model.warmup.started")]
    ModelWarmupStarted,

    #[serde(rename = "model.warmup.completed")]
    ModelWarmupCompleted,

    #[serde(rename = "model.warmup.failed")]
    ModelWarmupFailed,

    #[serde(rename = "model.call.started")]
    ModelCallStarted,

    #[serde(rename = "model.call.completed")]
    ModelCallCompleted,

    #[serde(rename = "model.call.failed")]
    ModelCallFailed,

    #[serde(rename = "model.compaction.started")]
    ModelCompactionStarted,

    #[serde(rename = "model.compaction.completed")]
    ModelCompactionCompleted,

    #[serde(rename = "model.compaction.failed")]
    ModelCompactionFailed,

    #[serde(rename = "model.attempt.started")]
    ModelAttemptStarted,

    #[serde(rename = "model.attempt.failed")]
    ModelAttemptFailed,

    #[serde(rename = "model.attempt.retrying")]
    ModelAttemptRetrying,

    #[serde(rename = "model.connection.started")]
    ModelConnectionStarted,

    #[serde(rename = "model.connection.completed")]
    ModelConnectionCompleted,

    #[serde(rename = "model.connection.failed")]
    ModelConnectionFailed,
}

impl AgentEventKind {
    #[must_use]
    pub const fn is_terminal(self) -> bool {
        matches!(self, Self::RunCompleted | Self::RunFailed)
    }
}

impl AgentEvent {
    pub fn model_call_timing(&self) -> Result<Option<super::ModelCallTiming>, serde_json::Error> {
        #[derive(Deserialize)]
        struct Payload {
            timing: Option<super::ModelCallTiming>,
        }
        serde_json::from_str::<Payload>(self.payload.get()).map(|value| value.timing)
    }

    pub fn model_elapsed_ns(&self) -> Result<Option<u64>, serde_json::Error> {
        #[derive(Deserialize)]
        struct Payload {
            model_elapsed_ns: Option<u64>,
        }
        serde_json::from_str::<Payload>(self.payload.get()).map(|value| value.model_elapsed_ns)
    }

    pub fn data(&self) -> Result<super::AgentEventData, serde_json::Error> {
        use super::{
            AgentEventData, AssistantEvent, ContextEvent, ModelEvent, ReasoningEvent, RunEvent,
            ToolEvent, TransportEvent,
        };

        Ok(match self.kind {
            AgentEventKind::InputAccepted => AgentEventData::InputAccepted(self.decode_payload()?),
            AgentEventKind::ApiEvent => AgentEventData::OpenAi(self.decode_payload()?),
            AgentEventKind::AssistantDelta => {
                AgentEventData::Assistant(AssistantEvent::Delta(self.decode_payload()?))
            }
            AgentEventKind::AssistantMessage => {
                AgentEventData::Assistant(AssistantEvent::Message(self.decode_payload()?))
            }
            AgentEventKind::ReasoningSummaryDelta => {
                AgentEventData::Reasoning(ReasoningEvent::SummaryDelta(self.decode_payload()?))
            }
            AgentEventKind::RunStarted => {
                AgentEventData::Run(RunEvent::Started(self.decode_payload()?))
            }
            AgentEventKind::RunSteered => {
                AgentEventData::Run(RunEvent::Steered(self.decode_payload()?))
            }
            AgentEventKind::RunError => {
                AgentEventData::Run(RunEvent::Error(self.decode_payload()?))
            }
            AgentEventKind::RunCompleted => {
                AgentEventData::Run(RunEvent::Completed(Box::new(self.decode_payload()?)))
            }
            AgentEventKind::RunFailed => {
                AgentEventData::Run(RunEvent::Failed(Box::new(self.decode_payload()?)))
            }
            AgentEventKind::ToolCall => {
                AgentEventData::Tool(ToolEvent::Call(self.decode_payload()?))
            }
            AgentEventKind::ToolResult => {
                AgentEventData::Tool(ToolEvent::Result(self.decode_payload()?))
            }
            AgentEventKind::ModelWarmupStarted => {
                AgentEventData::Model(ModelEvent::WarmupStarted(self.decode_payload()?))
            }
            AgentEventKind::ModelWarmupCompleted => {
                AgentEventData::Model(ModelEvent::WarmupCompleted(self.decode_payload()?))
            }
            AgentEventKind::ModelWarmupFailed => {
                AgentEventData::Model(ModelEvent::WarmupFailed(self.decode_payload()?))
            }
            AgentEventKind::ModelCallStarted => {
                AgentEventData::Model(ModelEvent::CallStarted(self.decode_payload()?))
            }
            AgentEventKind::ModelCallCompleted => {
                AgentEventData::Model(ModelEvent::CallCompleted(self.decode_payload()?))
            }
            AgentEventKind::ModelCallFailed => {
                AgentEventData::Model(ModelEvent::CallFailed(self.decode_payload()?))
            }
            AgentEventKind::ModelCompactionStarted => {
                AgentEventData::Context(ContextEvent::CompactionStarted(self.decode_payload()?))
            }
            AgentEventKind::ModelCompactionCompleted => {
                AgentEventData::Context(ContextEvent::CompactionCompleted(self.decode_payload()?))
            }
            AgentEventKind::ModelCompactionFailed => {
                AgentEventData::Context(ContextEvent::CompactionFailed(self.decode_payload()?))
            }
            AgentEventKind::ModelAttemptStarted
            | AgentEventKind::ModelAttemptFailed
            | AgentEventKind::ModelAttemptRetrying
            | AgentEventKind::ModelConnectionStarted
            | AgentEventKind::ModelConnectionCompleted
            | AgentEventKind::ModelConnectionFailed => {
                AgentEventData::Transport(TransportEvent::new(self.kind, Arc::clone(&self.payload)))
            }
        })
    }

    pub fn text_delta(&self) -> Option<Result<super::TextDeltaRef<'_>, serde_json::Error>> {
        matches!(
            self.kind,
            AgentEventKind::AssistantDelta | AgentEventKind::ReasoningSummaryDelta
        )
        .then(|| serde_json::from_str(self.payload.get()))
    }

    pub fn decode_payload<T: serde::de::DeserializeOwned>(&self) -> Result<T, serde_json::Error> {
        serde_json::from_str(self.payload.get())
    }
}
