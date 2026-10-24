use std::collections::HashMap;

use super::*;
use crate::client::responses::FunctionOutputContent;

pub(super) fn image_store_error(error: nanocode_durability::StoreError) -> NanocodeError {
    nanocode_durability::agent_error(error.into())
}

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

enum Rewrite {
    Omit(String),
    Inline(Box<str>),
}

fn rewrite(item: &mut ResponseItem, mut replace: impl FnMut(Image<'_>) -> Option<Rewrite>) {
    match item {
        ResponseItem::Message { content, .. } => {
            for part in content {
                let replacement = match part {
                    ContentItem::InputImage { image_url, .. } => replace(Image::Url(image_url)),
                    ContentItem::InputImageFile { file_id, .. } => replace(Image::File(file_id)),
                    _ => continue,
                };
                match (replacement, part) {
                    (Some(Rewrite::Omit(text)), part) => {
                        *part = ContentItem::InputText { text: text.into() };
                    }
                    (Some(Rewrite::Inline(url)), ContentItem::InputImage { image_url, .. }) => {
                        *image_url = url;
                    }
                    _ => {}
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
                        replace(Image::Url(image_url))
                    }
                    FunctionOutputContent::InputImageFile { file_id, .. } => {
                        replace(Image::File(file_id))
                    }
                    _ => continue,
                };
                match (replacement, part) {
                    (Some(Rewrite::Omit(text)), part) => {
                        *part = FunctionOutputContent::InputText { text: text.into() };
                    }
                    (
                        Some(Rewrite::Inline(url)),
                        FunctionOutputContent::InputImage { image_url, .. },
                    ) => *image_url = url,
                    _ => {}
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
            rewrite(&mut item, |image| {
                ordinal += 1;
                (ordinal <= evicted).then(|| {
                    Rewrite::Omit(limit.omitted(match image {
                        Image::Url(url) => url,
                        Image::File(file_id) => file_id,
                    }))
                })
            });
            item
        })
        .collect();
    let mut sent = history.clone();
    sent.replace_from(start, replacement, limit.evicted(sent_before) == evicted);
    sent
}

pub(super) async fn inline_images(
    limit: &ImageHistory,
    history: RequestHistory,
) -> Result<RequestHistory> {
    let full = history.full();
    let mut start = None;
    let mut loaded = HashMap::new();
    for (index, item) in full.iter().enumerate() {
        for image in images(item) {
            let Image::Url(url) = image else { continue };
            if loaded.contains_key(url) {
                continue;
            }
            let Some(data) = limit.inline(url).await.map_err(image_store_error)? else {
                continue;
            };
            start.get_or_insert(index);
            loaded.insert(url.to_owned(), data.into_boxed_str());
        }
    }
    let Some(start) = start else {
        return Ok(history);
    };
    let replacement = full
        .iter_from(start)
        .cloned()
        .map(|mut item| {
            rewrite(&mut item, |image| match image {
                Image::Url(url) => loaded.get(url).cloned().map(Rewrite::Inline),
                Image::File(_) => None,
            });
            item
        })
        .collect();
    let mut sent = history.clone();
    sent.replace_from(start, replacement, true);
    Ok(sent)
}
