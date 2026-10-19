mod contract;
pub use contract::*;

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(from = "TranscriptWire", into = "TranscriptWire")]
pub enum TranscriptItem {
    User(String),

    Reasoning(String),

    Assistant(String),

    Tool {
        call_id: String,

        name: String,

        arguments: String,

        #[serde(default, skip_serializing_if = "Option::is_none")]
        parent_call_id: Option<String>,
    },

    ToolResult {
        call_id: String,

        output: String,

        outcome: ToolOutcome,
    },
}

#[derive(serde::Deserialize, serde::Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum TranscriptWire {
    User {
        text: String,
    },

    Reasoning {
        text: String,
    },

    Assistant {
        text: String,
    },

    Tool {
        call_id: String,

        name: String,

        arguments: String,

        #[serde(default, skip_serializing_if = "Option::is_none")]
        parent_call_id: Option<String>,
    },

    ToolResult {
        call_id: String,

        output: String,

        outcome: ToolOutcome,
    },
}

impl From<TranscriptWire> for TranscriptItem {
    fn from(wire: TranscriptWire) -> Self {
        match wire {
            TranscriptWire::User { text } => Self::User(text),
            TranscriptWire::Reasoning { text } => Self::Reasoning(text),
            TranscriptWire::Assistant { text } => Self::Assistant(text),
            TranscriptWire::Tool {
                call_id,
                name,
                arguments,
                parent_call_id,
            } => Self::Tool {
                call_id,
                name,
                arguments,
                parent_call_id,
            },
            TranscriptWire::ToolResult {
                call_id,
                output,
                outcome,
            } => Self::ToolResult {
                call_id,
                output,
                outcome,
            },
        }
    }
}

impl From<TranscriptItem> for TranscriptWire {
    fn from(item: TranscriptItem) -> Self {
        match item {
            TranscriptItem::User(text) => Self::User { text },
            TranscriptItem::Reasoning(text) => Self::Reasoning { text },
            TranscriptItem::Assistant(text) => Self::Assistant { text },
            TranscriptItem::Tool {
                call_id,
                name,
                arguments,
                parent_call_id,
            } => Self::Tool {
                call_id,
                name,
                arguments,
                parent_call_id,
            },
            TranscriptItem::ToolResult {
                call_id,
                output,
                outcome,
            } => Self::ToolResult {
                call_id,
                output,
                outcome,
            },
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ToolOutcome {
    Completed,

    Failed,

    Unknown,
}

impl TranscriptItem {
    pub const MAX_TOOL_OUTPUT_BYTES: usize = 16 * 1024;

    #[must_use]
    pub fn tool_result(call_id: impl Into<String>, output: &str, outcome: ToolOutcome) -> Self {
        let end = output.floor_char_boundary(Self::MAX_TOOL_OUTPUT_BYTES);
        let mut bounded = output[..end].to_owned();
        if end < output.len() {
            bounded.push_str("\n[output truncated]");
        }
        Self::ToolResult {
            call_id: call_id.into(),
            output: bounded,
            outcome,
        }
    }
}
