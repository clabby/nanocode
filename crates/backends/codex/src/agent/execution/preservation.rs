use crate::client::responses::{ContentItem, MessageRole, ResponseHistory, ResponseItem};
use serde::{Deserialize, Serialize};

use super::ExecutionFuture;
use crate::{NanocodeError, Result};

pub trait BeforeCompaction: Send + Sync {
    fn preserve(
        &self,
        request: BeforeCompactionRequest,
    ) -> ExecutionFuture<'_, Result<CompactionReceipt>>;
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct BeforeCompactionRequest {
    pub boundary_id: String,

    pub session_id: String,

    pub root_session_id: String,

    pub messages: Vec<CompactionMessage>,

    pub truncated: bool,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct CompactionMessage {
    pub role: MessageRole,

    pub text: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CompactionReceipt {
    pub receipt_id: String,
}

impl CompactionReceipt {
    pub(crate) fn validate(&self) -> Result<()> {
        if self.receipt_id.trim().is_empty() || self.receipt_id.len() > 256 {
            return Err(NanocodeError::BeforeCompactionFailed(
                "host returned an invalid durable receipt".into(),
            ));
        }
        Ok(())
    }
}

impl BeforeCompactionRequest {
    pub(crate) fn from_history(
        boundary_id: String,
        session_id: String,
        root_session_id: String,
        history: &ResponseHistory,
    ) -> Self {
        let mut messages = Vec::new();
        let mut remaining = 32 * 1024;
        let mut truncated = false;

        'history: for item in history.iter_rev() {
            let ResponseItem::Message {
                role: role @ (MessageRole::User | MessageRole::Assistant),
                content,
                ..
            } = item
            else {
                continue;
            };

            if *role == MessageRole::User && content.iter().any(|part| matches!(part,
                ContentItem::InputText { text } if text.starts_with("<environment_context>")
                    || text.starts_with("# AGENTS.md instructions") || text.starts_with("<turn_aborted>"))) { continue; }
            let mut text = String::new();
            for part in content {
                let part = match (role, part) {
                    (MessageRole::User, ContentItem::InputText { text })
                    | (MessageRole::Assistant, ContentItem::OutputText { text, .. })
                        if !text.is_empty() =>
                    {
                        text
                    }
                    _ => continue,
                };

                let bytes = text
                    .len()
                    .saturating_add(part.len())
                    .saturating_add(usize::from(!text.is_empty()));
                if messages.len() == 64 || bytes > remaining {
                    truncated = true;
                    break 'history;
                }
                if !text.is_empty() {
                    text.push('\n');
                }
                text.push_str(part);
            }
            if !text.is_empty() {
                remaining -= text.len();
                messages.push(CompactionMessage { role: *role, text });
            }
        }
        messages.reverse();
        Self {
            boundary_id,
            session_id,
            root_session_id,
            messages,
            truncated,
        }
    }
}
