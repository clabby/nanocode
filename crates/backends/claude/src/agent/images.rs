use crate::{
    ContentBlock, ToolResultContent,
    prompt::{IMAGE_URL_UNSUPPORTED, ImageResolution, prepare_base64},
};
use nanocode_media::image::ImageHistory;
use serde_json::{Value, json};

use super::history::HistoryMessage;

pub(super) async fn prepare_tool_images(results: &mut [ContentBlock], resolution: ImageResolution) {
    let images: Vec<&mut Value> = results
        .iter_mut()
        .filter_map(|result| match result {
            ContentBlock::ToolResult {
                content: ToolResultContent::Blocks(blocks),
                ..
            } => Some(blocks),
            _ => None,
        })
        .flatten()
        .filter(|block| {
            block["type"] == "image"
                && matches!(block["source"]["type"].as_str(), Some("base64" | "url"))
        })
        .collect();
    for block in images {
        if block["source"]["type"] == "url" {
            *block = json!({"type": "text", "text": IMAGE_URL_UNSUPPORTED});
            continue;
        }
        let data = match block["source"]["data"].take() {
            Value::String(data) => data,
            _ => String::new(),
        };
        match prepare_base64(&data, resolution).await {
            Ok((data, media_type)) => {
                block["source"]["data"] = data.into();
                block["source"]["media_type"] = media_type.into();
            }
            Err(omission) => *block = json!({"type": "text", "text": omission}),
        }
    }
}

fn reference(source: &Value) -> String {
    match source["type"].as_str() {
        Some("base64") => format!("data:{}", source["media_type"].as_str().unwrap_or("image")),
        Some("url") => source["url"].as_str().unwrap_or_default().to_owned(),
        Some("file") => source["file_id"].as_str().unwrap_or_default().to_owned(),
        _ => source.to_string(),
    }
}

fn image_sources(message: &HistoryMessage) -> impl Iterator<Item = &Value> {
    message.content.iter().flat_map(|block| match block {
        ContentBlock::Image { source, .. } => vec![source],
        ContentBlock::ToolResult {
            content: ToolResultContent::Blocks(blocks),
            ..
        } => blocks
            .iter()
            .filter(|block| block["type"] == "image")
            .map(|block| &block["source"])
            .collect(),
        _ => Vec::new(),
    })
}

fn omit(message: &mut HistoryMessage, mut placeholder: impl FnMut(&Value) -> Option<String>) {
    let placeholders: Vec<_> = image_sources(message).map(&mut placeholder).collect();
    if placeholders.iter().all(Option::is_none) {
        return;
    }
    let mut placeholders = placeholders.into_iter();
    for block in &mut message.make_mut().content {
        match block {
            ContentBlock::Image { extra, .. } => {
                if let Some(text) = placeholders.next().flatten() {
                    *block = ContentBlock::Text {
                        text,
                        extra: std::mem::take(extra),
                    };
                }
            }
            ContentBlock::ToolResult {
                content: ToolResultContent::Blocks(blocks),
                ..
            } => {
                for block in blocks.iter_mut().filter(|block| block["type"] == "image") {
                    if let Some(text) = placeholders.next().flatten() {
                        let mut text = json!({"type": "text", "text": text});
                        if let Some(control) = block.get("cache_control") {
                            text["cache_control"] = control.clone();
                        }
                        *block = text;
                    }
                }
            }
            _ => {}
        }
    }
}

pub(super) fn evict_images(
    images: &ImageHistory,
    mut messages: Vec<HistoryMessage>,
) -> Vec<HistoryMessage> {
    let total = messages
        .iter()
        .map(|message| image_sources(message).count())
        .sum();
    let evicted = images.evicted(total);
    let mut ordinal = 0;
    for message in &mut messages {
        if ordinal >= evicted {
            break;
        }
        omit(message, |source| {
            ordinal += 1;
            (ordinal <= evicted).then(|| images.omitted(&reference(source)))
        });
    }
    messages
}
