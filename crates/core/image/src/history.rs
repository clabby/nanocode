pub const DEFAULT_INLINE_IMAGES: usize = 8;

#[derive(Clone, Debug)]
pub struct ImageHistory {
    inline_images: usize,
}

impl Default for ImageHistory {
    fn default() -> Self {
        Self::new(DEFAULT_INLINE_IMAGES)
    }
}

impl ImageHistory {
    #[must_use]
    pub const fn new(inline_images: usize) -> Self {
        Self { inline_images }
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
}
