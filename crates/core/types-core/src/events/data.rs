use std::{borrow::Cow, sync::Arc};

use serde::{Deserialize, Serialize};
use serde_json::{Value, value::RawValue};

use super::wire::AgentEventKind;
use crate::{
    pricing::{CostStatus, EstimatedUsdCost},
    responses::{MessagePhase, Usage},
    tools::ToolOutputBody,
};

#[derive(Clone, Debug)]
#[non_exhaustive]
pub enum AgentEventData {
    OpenAi(OpenAiEvent),

    Assistant(AssistantEvent),

    Reasoning(ReasoningEvent),

    InputAccepted(AcceptedInput),

    Run(RunEvent),

    Tool(ToolEvent),

    Model(ModelEvent),

    Context(ContextEvent),

    Transport(TransportEvent),
}

#[derive(Clone, Debug, Deserialize, serde::Serialize)]
pub struct AcceptedInput {
    pub session_id: String,

    pub turn_id: String,

    pub item_id: String,

    pub kind: String,

    pub request_id: Option<String>,

    pub input: crate::PromptInput,
}

#[derive(Clone, Debug, Deserialize)]
pub struct OpenAiEvent {
    pub direction: String,

    pub transport: String,

    pub phase: String,

    pub model_call_index: Option<u32>,

    pub event: Box<RawValue>,
}

#[derive(Clone, Debug)]
#[non_exhaustive]
pub enum AssistantEvent {
    Delta(AssistantDelta),

    Message(AssistantMessage),
}

#[derive(Clone, Debug, Deserialize)]
pub struct AssistantDelta {
    pub model_call_index: u32,

    pub item_id: Option<String>,

    pub phase: Option<MessagePhase>,

    pub text: String,
}

#[derive(Clone, Debug, Deserialize)]
pub struct AssistantMessage {
    pub model_call_index: u32,

    pub item_id: Option<String>,

    pub phase: Option<MessagePhase>,

    pub text: String,
}

#[derive(Clone, Debug)]
#[non_exhaustive]
pub enum ReasoningEvent {
    SummaryDelta(ReasoningSummaryDelta),
}

#[derive(Clone, Debug, Deserialize)]
pub struct ReasoningSummaryDelta {
    pub model_call_index: u32,

    pub text: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct TextDeltaRef<'a> {
    pub model_call_index: u32,

    #[serde(
        borrow,
        default,
        deserialize_with = "borrowed_option",
        skip_serializing_if = "Option::is_none"
    )]
    pub item_id: Option<Cow<'a, str>>,

    #[serde(
        borrow,
        default,
        deserialize_with = "borrowed_option",
        skip_serializing_if = "Option::is_none"
    )]
    pub call_id: Option<Cow<'a, str>>,

    #[serde(
        borrow,
        default,
        deserialize_with = "borrowed_option",
        skip_serializing_if = "Option::is_none"
    )]
    pub tool: Option<Cow<'a, str>>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub phase: Option<MessagePhase>,

    #[serde(borrow)]
    pub text: Cow<'a, str>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model_elapsed_ns: Option<u64>,
}

fn borrowed_option<'de: 'a, 'a, D>(deserializer: D) -> Result<Option<Cow<'a, str>>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    #[derive(Deserialize)]
    struct Borrowed<'a>(#[serde(borrow)] Cow<'a, str>);
    Option::<Borrowed<'a>>::deserialize(deserializer).map(|value| value.map(|value| value.0))
}

#[derive(Clone, Debug)]
#[non_exhaustive]
pub enum RunEvent {
    Started(RunStarted),

    Steered(RunSteered),

    Error(RunError),

    Completed(Box<RunTerminal>),

    Failed(Box<RunTerminal>),
}

#[derive(Clone, Debug, Deserialize)]
pub struct RunStarted {
    pub mode: String,

    pub model: String,

    pub reasoning_mode: String,

    pub effort: String,

    pub transport: String,

    pub orchestration: String,

    pub websocket_url: String,

    pub workspace: Option<String>,

    pub instruction_bytes: usize,
}

#[derive(Clone, Copy, Debug, Deserialize)]
pub struct RunSteered {
    pub steer_index: u32,

    pub instruction_bytes: usize,
}

#[derive(Clone, Debug, Deserialize)]
pub struct RunError {
    pub message: String,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum RunStatus {
    Completed,

    Cancelled,

    Failed,
}

#[derive(Clone, Copy, Debug, Default, Deserialize)]
pub struct EventUsage {
    pub input_tokens: u64,

    pub cached_input_tokens: u64,

    pub cache_write_input_tokens: u64,

    pub output_tokens: u64,

    pub reasoning_output_tokens: u64,

    pub total_tokens: u64,
}

#[derive(Clone, Debug, Default, Deserialize)]
pub struct RunMetrics {
    pub model_calls: u32,

    pub steers: u32,

    pub compactions: u32,

    pub tool_calls: u32,

    pub connection_attempts: u32,

    pub websocket_reconnects: u32,

    pub response_attempts: u32,

    pub response_retries: u32,

    pub connection_duration_ns: u64,

    pub retry_backoff_duration_ns: u64,

    pub model_duration_ns: u64,

    pub compaction_duration_ns: u64,

    pub warmup_duration_ns: u64,

    pub tool_work_duration_ns: u64,

    pub tool_wall_duration_ns: u64,

    pub usage: EventUsage,

    pub warmup_usage: EventUsage,
}

#[derive(Clone, Debug, Deserialize)]
pub struct RunTerminal {
    pub status: RunStatus,

    pub model: String,

    pub reasoning_mode: String,

    pub effort: String,

    pub transport: String,

    pub orchestration: String,

    pub duration_ms: u64,

    pub duration_ns: u64,

    #[serde(flatten)]
    pub metrics: RunMetrics,

    pub estimated_cost: Option<EstimatedUsdCost>,

    pub cost_usd: Option<f64>,

    #[serde(default)]
    pub cost_status: CostStatus,
}

#[derive(Clone, Debug)]
#[non_exhaustive]
pub enum ToolEvent {
    Call(ToolCall),

    Result(ToolResultEvent),

    Notification(ToolNotification),

    InputDelta(ToolInputDelta),
}

#[derive(Clone, Debug, Deserialize)]
pub struct ToolCall {
    pub call_id: String,

    pub tool: String,

    pub arguments: Box<RawValue>,

    pub model_call_index: u32,
}

impl ToolCall {
    pub fn decode_arguments<T: serde::de::DeserializeOwned>(&self) -> Result<T, serde_json::Error> {
        serde_json::from_str(self.arguments.get())
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum ToolStatus {
    Completed,

    Failed,

    Cancelled,
}

#[derive(Clone, Debug, Deserialize)]
pub struct ToolResultEvent {
    pub call_id: String,

    pub tool: String,

    pub status: ToolStatus,

    pub duration_ns: u64,

    pub started_after_ns: Option<u64>,

    pub result: ToolOutputBody,

    pub structured_result: Value,

    pub metadata: Option<Box<RawValue>>,
}

#[derive(Clone, Debug, Deserialize)]
pub struct ToolInputDelta {
    pub model_call_index: u32,

    pub call_id: String,

    pub tool: String,

    pub text: String,
}

#[derive(Clone, Debug, Deserialize)]
pub struct ToolNotification {
    pub call_id: String,

    pub text: String,
}

#[derive(Clone, Debug)]
#[non_exhaustive]
pub enum ModelEvent {
    WarmupStarted(ModelWarmupStarted),

    WarmupCompleted(ModelWarmupCompleted),

    WarmupFailed(ModelWarmupFailed),

    CallStarted(ModelCallStarted),

    CallCompleted(ModelCallCompleted),

    CallFailed(ModelCallFailed),
}

#[derive(Clone, Debug, Deserialize)]
pub struct ModelWarmupStarted {
    pub model: String,

    pub prompt_cache_key: String,
}

#[derive(Clone, Debug, Deserialize)]
pub struct ModelWarmupCompleted {
    pub source: String,

    pub attempt: Option<u32>,

    pub connection_generation: Option<u32>,

    pub duration_ns: u64,

    pub usage: Option<Usage>,
}

#[derive(Clone, Debug, Deserialize)]
pub struct ModelWarmupFailed {
    pub duration_ns: u64,

    pub error: String,
}

#[derive(Clone, Debug, Deserialize)]
pub struct ModelCallStarted {
    pub call_index: u32,

    pub model: String,

    pub reasoning_mode: String,

    pub effort: String,

    #[serde(default)]
    pub previous_response_id: Option<String>,

    #[serde(default)]
    pub prompt_cache_key: Option<String>,

    #[serde(default)]
    pub cache_ttl: Option<String>,
}

#[derive(Clone, Debug, Deserialize)]
pub struct ModelCallCompleted {
    pub call_index: u32,

    pub model: String,

    pub attempt: u32,

    pub connection_generation: u32,

    pub status: String,

    pub duration_ns: u64,

    pub time_to_first_event_ns: u64,

    pub time_to_first_output_ns: Option<u64>,

    pub tool_calls: usize,

    pub usage: Option<Usage>,
}

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(default)]
pub struct ModelCallTiming {
    pub replayed: bool,

    pub preparation_ns: Option<u64>,

    pub queue_ns: Option<u64>,

    pub attempt_started_ns: Option<u64>,

    pub connect_ns: Option<u64>,

    pub encode_ns: Option<u64>,

    pub send_ns: Option<u64>,

    pub request_sent_ns: Option<u64>,

    pub response_headers_ns: Option<u64>,

    pub first_event_ns: Option<u64>,

    pub first_output_ns: Option<u64>,

    pub response_completed_ns: Option<u64>,

    pub total_ns: Option<u64>,

    pub pipeline: Option<ModelCallPipeline>,
}

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(default)]
pub struct ModelCallPipeline {
    pub event_count: Option<u64>,

    pub event_bytes: Option<u64>,

    pub receive_wait_ns: Option<u64>,

    pub receive_decode_ns: Option<u64>,

    pub decode_ns: Option<u64>,

    pub api_event_emit_ns: Option<u64>,

    pub response_forward_ns: Option<u64>,

    pub display_delta_emit_ns: Option<u64>,

    pub collect_ns: Option<u64>,

    pub socket_queue_ns: Option<u64>,

    pub display_delta_count: Option<u64>,

    pub display_delta_bytes: Option<u64>,

    pub inter_delta_gap_max_ns: Option<u64>,

    pub inter_delta_stall_50ms_count: Option<u64>,

    pub inter_delta_stall_100ms_count: Option<u64>,

    pub inter_delta_stall_250ms_count: Option<u64>,
}

#[derive(Clone, Debug, Deserialize)]
pub struct ModelCallFailed {
    pub call_index: u32,

    pub model: String,

    pub duration_ns: u64,

    pub error: String,
}

#[derive(Clone, Debug)]
#[non_exhaustive]
pub enum ContextEvent {
    CompactionStarted(CompactionStarted),

    CompactionCompleted(CompactionCompleted),

    CompactionFailed(CompactionFailed),
}

#[derive(Clone, Debug, Deserialize)]
pub struct CompactionStarted {
    pub after_model_call_index: u32,

    pub active_context_tokens: u64,

    pub auto_compact_token_limit: u64,
}

#[derive(Clone, Debug, Deserialize)]
pub struct CompactionCompleted {
    pub after_model_call_index: u32,

    pub attempt: u32,

    pub connection_generation: u32,

    pub status: String,

    pub duration_ns: u64,

    pub time_to_first_event_ns: u64,

    pub time_to_first_output_ns: Option<u64>,

    pub usage: Option<Usage>,
}

#[derive(Clone, Debug, Deserialize)]
pub struct CompactionFailed {
    pub after_model_call_index: u32,

    pub duration_ns: u64,

    pub error: String,
}

#[derive(Clone, Debug)]
pub struct TransportEvent {
    kind: AgentEventKind,
    payload: Arc<RawValue>,
}

impl TransportEvent {
    #[doc(hidden)]
    pub const fn new(kind: AgentEventKind, payload: Arc<RawValue>) -> Self {
        Self { kind, payload }
    }

    #[must_use]
    pub const fn kind(&self) -> AgentEventKind {
        self.kind
    }

    #[must_use]
    pub fn raw_payload(&self) -> &RawValue {
        &self.payload
    }

    pub fn decode_payload<T: serde::de::DeserializeOwned>(&self) -> Result<T, serde_json::Error> {
        serde_json::from_str(self.payload.get())
    }
}
