use std::borrow::Cow;

use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::AttachmentMachine;

pub(crate) const JSON_SAFE_INTEGER_MAX: u64 = 9_007_199_254_740_991;

pub(crate) const HEARTBEAT_INTERVAL: std::time::Duration = std::time::Duration::from_secs(20);

#[derive(Debug, Default, Clone, PartialEq, Serialize, Deserialize)]
pub struct ReceiptTiming {
    pub scheduler_ms: f64,
    pub execution_gate_ms: f64,
    pub execution_ms: f64,
    pub result_encode_ms: f64,
    pub result_queue_ms: f64,
    pub host_elapsed_ms: f64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DiagnosticStage {
    Received,
    ExecutionStarted,
    ExecutionFinished,
    ResultPrepared,
}

impl DiagnosticStage {
    #[must_use]
    pub const fn name(self) -> &'static str {
        match self {
            Self::Received => "received",
            Self::ExecutionStarted => "execution_started",
            Self::ExecutionFinished => "execution_finished",
            Self::ResultPrepared => "result_prepared",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CommandStatus {
    Running,
    Missing,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum RunnerFrame<'a> {
    Catalog {
        capabilities: Vec<Cow<'a, str>>,
        runtime_id: Cow<'a, str>,
        command_recovery: bool,
        turn_lifecycle: bool,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        diagnostics: Option<bool>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        connection_id: Option<Cow<'a, str>>,
        tools: Cow<'a, Value>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        machines: Option<Cow<'a, [AttachmentMachine]>>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        attachment_id: Option<Cow<'a, str>>,
    },
    Diagnostic {
        call_id: Cow<'a, str>,
        stage: DiagnosticStage,
        elapsed_ms: f64,
    },
    Result {
        call_id: Cow<'a, str>,
        outcome: Cow<'a, Value>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        timing: Option<ReceiptTiming>,
    },
    Status {
        call_id: Cow<'a, str>,
        state: CommandStatus,
    },
    Drain {},
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AttachmentCall {
    pub session_id: String,
    #[serde(
        default,
        deserialize_with = "deserialize_turn_id",
        skip_serializing_if = "Option::is_none"
    )]
    pub turn_id: Option<String>,
    pub call_id: String,
    pub model: String,
    pub name: String,
    pub input: Value,
    pub output_token_budget: u64,
    pub output_byte_budget: u64,
    pub deadline_at: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum EndpointFrame {
    Ready {},
    Call(AttachmentCall),
    TurnEnded {
        session_id: String,
        turn_id: String,
        hook_event_name: String,
    },
    Cancel {
        call_id: String,
    },
    Ack {
        call_id: String,
    },
    Recover {
        call_ids: Vec<String>,
    },
    Draining {},
}

impl EndpointFrame {
    #[must_use]
    pub const fn kind(&self) -> &'static str {
        match self {
            Self::Ready {} => "ready",
            Self::Call { .. } => "call",
            Self::Cancel { .. } => "cancel",
            Self::TurnEnded { .. } => "turn_ended",
            Self::Ack { .. } => "ack",
            Self::Recover { .. } => "recover",
            Self::Draining {} => "draining",
        }
    }

    pub fn parse(text: &str) -> Result<Self, &'static str> {
        let frame: Self = serde_json::from_str(text).map_err(|_| "invalid attachment frame")?;
        frame.validate()?;
        Ok(frame)
    }

    fn validate(&self) -> Result<(), &'static str> {
        match self {
            Self::Ready {} | Self::Draining {} => Ok(()),
            Self::Call(AttachmentCall {
                session_id,
                turn_id,
                call_id,
                model: _,
                name,
                input,
                output_token_budget,
                output_byte_budget,
                deadline_at,
            }) => {
                if !valid_identifier(session_id)
                    || turn_id
                        .as_ref()
                        .is_some_and(|turn| turn.is_empty() || turn.len() > 256)
                    || !valid_identifier(call_id)
                    || !valid_tool_name(name)
                    || !(input.is_object() || input.is_string())
                    || *output_token_budget > JSON_SAFE_INTEGER_MAX
                    || !positive(*output_byte_budget)
                    || !positive(*deadline_at)
                {
                    return Err("invalid call");
                }
                Ok(())
            }
            Self::TurnEnded {
                session_id,
                turn_id,
                hook_event_name,
            } => {
                if valid_identifier(session_id)
                    && !turn_id.is_empty()
                    && turn_id.len() <= 256
                    && matches!(
                        hook_event_name.as_str(),
                        "Stop" | "Interrupt" | "SubagentStop"
                    )
                {
                    Ok(())
                } else {
                    Err("invalid turn identity")
                }
            }
            Self::Cancel { call_id } | Self::Ack { call_id } => {
                if valid_identifier(call_id) {
                    Ok(())
                } else {
                    Err("invalid call identity")
                }
            }
            Self::Recover { call_ids } => {
                if call_ids.is_empty()
                    || call_ids.len() > 100
                    || !call_ids.iter().all(|id| valid_identifier(id))
                {
                    Err("invalid recovery batch")
                } else {
                    Ok(())
                }
            }
        }
    }
}

fn deserialize_turn_id<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> Result<Option<String>, D::Error> {
    String::deserialize(deserializer).map(Some)
}

const fn positive(value: u64) -> bool {
    value > 0 && value <= JSON_SAFE_INTEGER_MAX
}

fn valid_identifier(value: &str) -> bool {
    value.len() <= 128
        && value
            .as_bytes()
            .first()
            .is_some_and(u8::is_ascii_alphanumeric)
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b':' | b'-'))
}

fn valid_tool_name(value: &str) -> bool {
    valid_identifier(value) && !matches!(value, "exec" | "tool_search" | "wait")
}
