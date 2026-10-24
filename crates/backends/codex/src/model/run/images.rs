use super::*;
use crate::client::responses::FunctionOutputContent;

enum Image<'a> {
    Url(&'a str),
    File(&'a str),
}

fn images(item: &ResponseItem) -> Vec<Image<'_>> {
    match item {
        ResponseItem::Message { content, .. } => content
            .iter()
            .filter_map(|part| match part {
                ContentItem::InputImage { image_url, .. } => Some(Image::Url(image_url)),
                ContentItem::InputImageFile { file_id, .. } => Some(Image::File(file_id)),
                _ => None,
            })
            .collect(),
        ResponseItem::FunctionCallOutput {
            output: FunctionOutputBody::Content(content),
            ..
        }
        | ResponseItem::CustomToolCallOutput {
            output: FunctionOutputBody::Content(content),
            ..
        } => content
            .iter()
            .filter_map(|part| match part {
                FunctionOutputContent::InputImage { image_url, .. } => Some(Image::Url(image_url)),
                FunctionOutputContent::InputImageFile { file_id, .. } => Some(Image::File(file_id)),
                _ => None,
            })
            .collect(),
        _ => Vec::new(),
    }
}

fn omit(item: &mut ResponseItem, mut placeholder: impl FnMut(Image<'_>) -> Option<String>) {
    match item {
        ResponseItem::Message { content, .. } => {
            for part in content {
                let replacement = match part {
                    ContentItem::InputImage { image_url, .. } => placeholder(Image::Url(image_url)),
                    ContentItem::InputImageFile { file_id, .. } => {
                        placeholder(Image::File(file_id))
                    }
                    _ => continue,
                };
                if let Some(text) = replacement {
                    *part = ContentItem::InputText { text: text.into() };
                }
            }
        }
        ResponseItem::FunctionCallOutput {
            output: FunctionOutputBody::Content(content),
            ..
        }
        | ResponseItem::CustomToolCallOutput {
            output: FunctionOutputBody::Content(content),
            ..
        } => {
            for part in content {
                let replacement = match part {
                    FunctionOutputContent::InputImage { image_url, .. } => {
                        placeholder(Image::Url(image_url))
                    }
                    FunctionOutputContent::InputImageFile { file_id, .. } => {
                        placeholder(Image::File(file_id))
                    }
                    _ => continue,
                };
                if let Some(text) = replacement {
                    *part = FunctionOutputContent::InputText { text: text.into() };
                }
            }
        }
        _ => {}
    }
}

pub(super) fn evict_images(limit: &ImageHistory, history: &RequestHistory) -> RequestHistory {
    let full = history.full();
    let mut total = 0;
    let mut sent_before = 0;
    let mut start = None;
    for (index, item) in full.iter().enumerate() {
        let count = images(item).len();
        if count > 0 && start.is_none() {
            start = Some(index);
        }
        if index < history.incremental_start() {
            sent_before += count;
        }
        total += count;
    }
    let evicted = limit.evicted(total);
    let Some(start) = start.filter(|_| evicted > 0) else {
        return history.clone();
    };
    let mut ordinal = 0;
    let replacement = full
        .iter_from(start)
        .cloned()
        .map(|mut item| {
            omit(&mut item, |image| {
                ordinal += 1;
                (ordinal <= evicted).then(|| {
                    limit.omitted(match image {
                        Image::Url(url) => url,
                        Image::File(file_id) => file_id,
                    })
                })
            });
            item
        })
        .collect();
    let mut sent = history.clone();
    sent.replace_from(start, replacement, limit.evicted(sent_before) == evicted);
    sent
}
