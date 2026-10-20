use std::path::PathBuf;

use crate::client::auth::OpenAiAuth;

pub struct WebSearchConfig {
    pub endpoint: String,

    pub auth: OpenAiAuth,

    #[cfg(not(target_family = "wasm"))]
    pub http_layer: Option<std::sync::Arc<dyn nanocode_http::layer::Layer>>,
}

pub struct ImageGenerationConfig {
    pub api_base_url: String,

    pub auth: OpenAiAuth,

    pub save_root: PathBuf,

    #[cfg(not(target_family = "wasm"))]
    pub http_layer: Option<std::sync::Arc<dyn nanocode_http::layer::Layer>>,
}
