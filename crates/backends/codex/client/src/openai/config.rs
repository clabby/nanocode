use std::{borrow::Cow, sync::Arc};

use crate::client::{
    CONTEXT_WINDOW_TOKENS, Model, OpenAiAuth, ReasoningMode, ResponsesHistory, ResponsesTransport,
    Thinking, pricing::ServiceTier, responses::StrictJsonSchema,
};

const SOL_SYSTEM_PROMPT: &str = include_str!("../../prompts/sol.md");
const LUNA_SYSTEM_PROMPT: &str = include_str!("../../prompts/luna.md");
const GLM_SYSTEM_PROMPT: &str = include_str!("../../prompts/glm.md");
const ASTRA_SYSTEM_PROMPT: &str = include_str!("../../prompts/astra.md");

#[derive(Clone)]
pub struct ModelConfig {
    pub model: Model,

    pub model_id_prefix: Option<Arc<str>>,

    pub auth: OpenAiAuth,

    pub reasoning_mode: ReasoningMode,

    pub thinking: Thinking,

    #[doc(hidden)]
    pub thinking_explicit: bool,

    pub service_tier: ServiceTier,

    pub context_window_tokens: u64,

    pub responses_transport: ResponsesTransport,

    pub websocket_warmup: bool,

    pub raw_api_events: bool,

    pub responses_history: ResponsesHistory,

    pub store_responses: bool,

    pub strict_json_schema: Option<StrictJsonSchema>,

    pub websocket_url: String,

    pub api_base_url: String,

    #[cfg(any(target_family = "wasm", docsrs))]
    pub host_transport: Option<Arc<dyn crate::client::transport::host::HostTransport>>,

    #[cfg(not(target_family = "wasm"))]
    pub chat_gateway: Option<Arc<crate::client::transport::ChatGateway>>,

    pub responses_gateway: Option<Arc<crate::client::transport::ResponsesGateway>>,

    pub system_prompt: Option<Arc<str>>,

    pub additional_instructions: Option<Arc<str>>,
}

impl ModelConfig {
    pub(crate) fn wire_model_id(&self, model: Model) -> Cow<'static, str> {
        match self.model_id_prefix.as_deref() {
            Some(prefix) => Cow::Owned(format!("{prefix}/{}", model.as_str())),
            None => Cow::Borrowed(model.as_str()),
        }
    }

    #[must_use]
    pub const fn supports_reasoning_effort_updates(&self, model: Model) -> bool {
        model.supports_reasoning_effort_updates() && self.model_id_prefix.is_none()
    }

    #[must_use]
    pub const fn orchestration() -> &'static str {
        "local_code_mode"
    }

    #[must_use]
    #[doc(hidden)]
    pub fn base_system_prompt(&self) -> Cow<'_, str> {
        let base = self.system_prompt.as_deref().unwrap_or(match self.model {
            Model::Astra => ASTRA_SYSTEM_PROMPT,
            Model::Glm53 | Model::Kimi | Model::Mimo => GLM_SYSTEM_PROMPT,
            Model::Sol => SOL_SYSTEM_PROMPT,
            Model::Luna => LUNA_SYSTEM_PROMPT,
            _ => ASTRA_SYSTEM_PROMPT,
        });
        if self.system_prompt.is_none() && matches!(self.model, Model::Kimi | Model::Mimo) {
            Cow::Owned(base.replacen(
                "powered by Z.ai GLM-5.3",
                &format!("powered by {}", self.model.as_str()),
                1,
            ))
        } else {
            Cow::Borrowed(base)
        }
    }

    #[must_use]
    pub fn system_prompt(&self) -> Cow<'_, str> {
        Cow::Owned(format!(
            "{}\n\n{}",
            self.base_system_prompt(),
            self.host_instructions()
        ))
    }

    #[doc(hidden)]
    pub fn host_instructions(&self) -> String {
        let mut instructions = String::new();
        if let Some(additional) = self.additional_instructions.as_deref()
            && !additional.is_empty()
        {
            instructions.push_str(additional);
            instructions.push_str("\n\n");
        }
        instructions.push_str(&format!(
            "<runtime_model_identity>\nmodel_id: {}\n\
             This is the runtime-selected model serving this agent. When asked which model \
             you are, report this model ID. Nanocodex and Codex are product or harness names, \
             not the underlying model. Other agents and models listed in tools can differ \
             from this agent. Use this current runtime identity over conflicting identity \
             claims in earlier conversation or inherited instructions.\n\
             </runtime_model_identity>",
            self.model.as_str(),
        ));
        instructions
    }

    #[must_use]
    pub fn search_endpoint(&self) -> String {
        format!("{}/alpha/search", self.api_base_url.trim_end_matches('/'))
    }
}

impl Default for ModelConfig {
    fn default() -> Self {
        #[cfg(not(target_family = "wasm"))]
        {
            let mut config = Self {
                auth: OpenAiAuth::chatgpt(),
                ..Self::api_key_defaults()
            };
            let mode = config.auth.mode();
            super::apply_mode_defaults(&mut config, mode);
            config
        }
        #[cfg(target_family = "wasm")]
        Self::api_key_defaults()
    }
}

impl ModelConfig {
    fn api_key_defaults() -> Self {
        Self {
            model: Model::default(),
            model_id_prefix: None,
            auth: OpenAiAuth::api_key(String::new()),
            reasoning_mode: ReasoningMode::default(),
            thinking: Thinking::default(),
            thinking_explicit: false,
            service_tier: ServiceTier::Standard,
            context_window_tokens: CONTEXT_WINDOW_TOKENS,
            responses_transport: ResponsesTransport::default(),
            websocket_warmup: true,
            raw_api_events: true,
            responses_history: ResponsesHistory::default(),
            store_responses: false,
            strict_json_schema: None,
            websocket_url: "wss://api.openai.com/v1/responses".to_owned(),
            api_base_url: "https://api.openai.com/v1".to_owned(),
            #[cfg(any(target_family = "wasm", docsrs))]
            host_transport: None,
            #[cfg(not(target_family = "wasm"))]
            chat_gateway: None,
            responses_gateway: None,
            system_prompt: None,
            additional_instructions: None,
        }
    }
}
