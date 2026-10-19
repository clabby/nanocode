use crate::client::responses::{ContentItem, MessageRole, ResponseItem};

const ASSISTANT_CONTEXT_TOKEN_LIMIT: usize = 1_000;
const APPROX_BYTES_PER_TOKEN: usize = 4;

pub(super) fn recent_input(history: &[ResponseItem]) -> Option<Vec<ResponseItem>> {
    let mut messages = history
        .iter()
        .filter_map(visible_message)
        .collect::<Vec<_>>();
    let latest_user = messages.iter().rposition(is_user_message)?;
    messages.truncate(latest_user + 1);
    let first_retained = messages
        .iter()
        .enumerate()
        .rev()
        .filter(|(_, item)| is_user_message(item))
        .take(2)
        .last()
        .map_or(latest_user, |(index, _)| index);
    messages.drain(..first_retained);
    truncate_assistant_text(&mut messages, ASSISTANT_CONTEXT_TOKEN_LIMIT);
    (!messages.is_empty()).then_some(messages)
}

fn visible_message(item: &ResponseItem) -> Option<ResponseItem> {
    match item {
        ResponseItem::Message {
            role: MessageRole::Assistant,
            ..
        } => {
            let mut message = item.clone();
            message.strip_id();
            Some(message)
        }
        ResponseItem::Message {
            role: MessageRole::User,
            ..
        } => visible_user_message(item),
        _ => None,
    }
}

fn visible_user_message(item: &ResponseItem) -> Option<ResponseItem> {
    let mut message = item.clone();
    let ResponseItem::Message { id, content, .. } = &mut message else {
        return None;
    };
    content.retain(|part| matches!(part, ContentItem::InputText { .. }));
    if content.is_empty() || content.iter().all(is_context_item) {
        return None;
    }
    *id = None;
    Some(message)
}

fn is_context_item(item: &ContentItem) -> bool {
    match item {
        ContentItem::InputText { text } => {
            let text = text.trim_start();
            text.starts_with("# AGENTS.md instructions for ")
                || text.starts_with("<environment_context>")
        }
        ContentItem::InputImage { .. }
        | ContentItem::InputImageFile { .. }
        | ContentItem::InputAudio { .. }
        | ContentItem::OutputText { .. } => false,
    }
}

const fn is_user_message(item: &ResponseItem) -> bool {
    item.is_user_message()
}

fn truncate_assistant_text(messages: &mut Vec<ResponseItem>, max_tokens: usize) {
    let mut remaining = max_tokens;
    messages.retain_mut(|message| {
        let ResponseItem::Message { role, content, .. } = message else {
            return true;
        };
        if *role != MessageRole::Assistant {
            return true;
        }
        content.retain_mut(|part| {
            let ContentItem::OutputText { text, .. } = part else {
                return true;
            };
            if remaining == 0 {
                return false;
            }
            let tokens = approx_tokens(text.len());
            if tokens <= remaining {
                remaining -= tokens;
                return true;
            }
            *text = truncate_middle(text, remaining).into_boxed_str();
            remaining = 0;
            true
        });
        !content.is_empty()
    });
}

const fn approx_tokens(bytes: usize) -> usize {
    bytes.saturating_add(APPROX_BYTES_PER_TOKEN - 1) / APPROX_BYTES_PER_TOKEN
}

fn truncate_middle(text: &str, max_tokens: usize) -> String {
    let max_bytes = max_tokens.saturating_mul(APPROX_BYTES_PER_TOKEN);
    if max_tokens > 0 && text.len() <= max_bytes {
        return text.to_owned();
    }
    if max_bytes == 0 {
        return format!("…{} tokens truncated…", approx_tokens(text.len()));
    }
    let left_end = text.floor_char_boundary(max_bytes / 2);
    let right_start = text
        .ceil_char_boundary(text.len().saturating_sub(max_bytes - max_bytes / 2))
        .max(left_end);
    let removed = approx_tokens(right_start.saturating_sub(left_end));
    format!(
        "{}…{removed} tokens truncated…{}",
        &text[..left_end],
        &text[right_start..]
    )
}
