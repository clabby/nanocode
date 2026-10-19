mod error;
pub use error::EventError;

mod data;
mod wire;

#[doc(inline)]
pub use data::OpenAiEvent;
#[doc(inline)]
pub use data::{
    AcceptedInput, AgentEventData, AssistantDelta, AssistantEvent, AssistantMessage,
    CompactionCompleted, CompactionFailed, CompactionStarted, ContextEvent, EventUsage,
    ModelCallCompleted, ModelCallFailed, ModelCallPipeline, ModelCallStarted, ModelCallTiming,
    ModelEvent, ModelWarmupCompleted, ModelWarmupFailed, ModelWarmupStarted, ReasoningEvent,
    ReasoningSummaryDelta, RunError, RunEvent, RunMetrics, RunStarted, RunStatus, RunSteered,
    RunTerminal, TextDeltaRef, ToolCall, ToolEvent, ToolNotification, ToolResultEvent, ToolStatus,
    TransportEvent,
};
pub use wire::{AGENT_EVENT_PROTOCOL_VERSION, AgentEvent, AgentEventKind};
