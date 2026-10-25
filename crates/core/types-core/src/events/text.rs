use super::{AgentEventData, AssistantEvent};
use std::collections::HashMap;

/// Folds a session's streamed assistant text into whole messages, for hosts
/// that show each message as it streams: [`AssistantText::observe`] adds a
/// delta to its message, or takes a final message as it is, and returns the
/// message's text so far. A message is keyed by its item ID, or
/// `model-<model_call_index>` when the backend gives it none, so keys are unique
/// within one agent's events. A message is forgotten once its final text is
/// returned.
#[derive(Clone, Debug, Default)]
pub struct AssistantText {
    streaming: HashMap<String, String>,
}

/// One message's text after an assistant event.
#[derive(Clone, Debug)]
pub struct AssistantTextUpdate<'a> {
    pub key: String,
    pub text: &'a str,
    /// Whether this is the message's final text.
    pub complete: bool,
}

impl AssistantText {
    /// The key of a message with `item_id` from model call `model_call_index`.
    pub fn key(item_id: Option<&str>, model_call_index: u32) -> String {
        item_id.map_or_else(|| format!("model-{model_call_index}"), str::to_owned)
    }

    /// The text of the message `data` adds to or completes; `None` for
    /// every other event.
    pub fn observe<'a>(&'a mut self, data: &'a AgentEventData) -> Option<AssistantTextUpdate<'a>> {
        let AgentEventData::Assistant(event) = data else {
            return None;
        };
        match event {
            AssistantEvent::Delta(delta) => {
                let key = Self::key(delta.item_id.as_deref(), delta.model_call_index);
                let text = self.streaming.entry(key.clone()).or_default();
                text.push_str(&delta.text);
                Some(AssistantTextUpdate {
                    key,
                    text,
                    complete: false,
                })
            }
            AssistantEvent::Message(message) => {
                let key = Self::key(message.item_id.as_deref(), message.model_call_index);
                self.streaming.remove(&key);
                Some(AssistantTextUpdate {
                    key,
                    text: &message.text,
                    complete: true,
                })
            }
        }
    }
}
