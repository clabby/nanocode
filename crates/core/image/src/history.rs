use std::sync::Arc;

use nanocode_durability_core::{BlobStore, BlobUrl, StoreError};
use nanocode_types_core::{
    PromptInput, UserInput,
    responses::ContentItem,
    tools::{ToolOutputBody, ToolOutputContent},
};

pub const DEFAULT_INLINE_IMAGES: usize = 8;

pub type SharedBlobStore = Arc<dyn BlobStore + Send + Sync>;

#[derive(Clone)]
pub struct ImageHistory {
    inline_images: usize,
    blobs: Option<SharedBlobStore>,
}

impl Default for ImageHistory {
    fn default() -> Self {
        Self::new(DEFAULT_INLINE_IMAGES)
    }
}

impl std::fmt::Debug for ImageHistory {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ImageHistory")
            .field("inline_images", &self.inline_images)
            .field("blob_store", &self.blobs.is_some())
            .finish()
    }
}

impl ImageHistory {
    #[must_use]
    pub const fn new(inline_images: usize) -> Self {
        Self {
            inline_images,
            blobs: None,
        }
    }

    #[must_use]
    pub fn with_blob_store(mut self, store: SharedBlobStore) -> Self {
        self.blobs = Some(store);
        self
    }

    #[must_use]
    pub const fn inline_images(&self) -> usize {
        self.inline_images
    }

    #[must_use]
    pub const fn evicted(&self, images: usize) -> usize {
        let limit = self.inline_images;
        if images <= limit {
            return 0;
        }
        let batch = if limit < 2 { 1 } else { limit.div_ceil(2) };
        let evicted = (images - limit).div_ceil(batch) * batch;
        if evicted < images { evicted } else { images }
    }

    #[must_use]
    pub fn omitted(&self, reference: &str) -> String {
        let reference = reference
            .strip_prefix("data:")
            .and_then(|data| data.split([';', ',']).next())
            .map_or_else(|| reference.to_owned(), |media| format!("inline {media}"));
        format!(
            "[image omitted from this request, which keeps only the newest {} images: {reference}]",
            self.inline_images
        )
    }

    pub async fn inline(&self, url: &str) -> Result<Option<String>, StoreError> {
        let Some(blob) = BlobUrl::parse(url) else {
            return Ok(None);
        };
        let Some(store) = &self.blobs else {
            return Err(StoreError::Backend(format!(
                "image {url} needs a blob store, but none is configured"
            )));
        };
        blob.load_data_url(store.as_ref()).await.map(Some)
    }

    pub async fn store(&self, url: &str) -> Result<Option<String>, StoreError> {
        let Some(store) = &self.blobs else {
            return Ok(None);
        };
        Ok(BlobUrl::store_data_url(store.as_ref(), url)
            .await?
            .map(|blob| blob.to_string()))
    }

    pub async fn store_output(&self, output: &mut ToolOutputBody) -> Result<(), StoreError> {
        let ToolOutputBody::Content(content) = output else {
            return Ok(());
        };
        for item in content {
            if let ToolOutputContent::InputImage { image_url, .. } = item
                && let Some(blob) = self.store(image_url).await?
            {
                *image_url = blob;
            }
        }
        Ok(())
    }

    pub async fn store_prompt(&self, input: &mut PromptInput) -> Result<(), StoreError> {
        let PromptInput::Content(items) = input else {
            return Ok(());
        };
        for item in items {
            if let UserInput::Image { image_url, .. } = item
                && let Some(blob) = self.store(image_url).await?
            {
                *image_url = blob;
            }
        }
        Ok(())
    }

    pub async fn load_prompt(&self, input: &mut PromptInput) -> Result<(), StoreError> {
        let PromptInput::Content(items) = input else {
            return Ok(());
        };
        for item in items {
            let UserInput::Image { image_url, .. } = item else {
                continue;
            };
            if !BlobUrl::is_blob_url(image_url) {
                continue;
            }
            if self.blobs.is_none() {
                *item = UserInput::Text {
                    text: format!(
                        "image content omitted because no blob store is configured to read {image_url}"
                    ),
                };
                continue;
            }
            if let Some(data) = self.inline(image_url).await? {
                *image_url = data;
            }
        }
        Ok(())
    }

    pub async fn prepare_user_input(
        &self,
        input: &PromptInput,
    ) -> Result<Vec<ContentItem>, StoreError> {
        let mut content = if has_blob_image(input) {
            let mut loaded = input.clone();
            self.load_prompt(&mut loaded).await?;
            crate::prepare_user_input(&loaded).await
        } else {
            crate::prepare_user_input(input).await
        };
        for item in &mut content {
            if let ContentItem::InputImage { image_url, .. } = item
                && let Some(blob) = self.store(image_url).await?
            {
                *image_url = blob.into_boxed_str();
            }
        }
        Ok(content)
    }

    pub async fn prepare_output_images(
        &self,
        output: &mut ToolOutputBody,
    ) -> Result<(), StoreError> {
        crate::prepare_output_images(output).await;
        self.store_output(output).await
    }
}

fn has_blob_image(input: &PromptInput) -> bool {
    let PromptInput::Content(items) = input else {
        return false;
    };
    items.iter().any(
        |item| matches!(item, UserInput::Image { image_url, .. } if BlobUrl::is_blob_url(image_url)),
    )
}
