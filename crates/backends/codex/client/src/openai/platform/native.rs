use std::{num::NonZeroU32, sync::Arc};

use crate::client::{
    DefaultResponsesService, ModelConfig, OpenAiError, ResponsesService, ResponsesTransport,
};

#[derive(Clone, Default)]
pub(crate) struct FactoryPlatform {
    http_client: Option<reqwest::Client>,
}

impl FactoryPlatform {
    pub(crate) const fn new() -> Self {
        Self { http_client: None }
    }

    pub(crate) fn set_http_client(&mut self, client: reqwest::Client) {
        self.http_client = Some(client);
    }

    pub(crate) fn validate_config(&self, config: &ModelConfig) -> Result<(), OpenAiError> {
        if config.http_layer.is_some() {
            if config.host_transport.is_some() || config.chat_gateway.is_some() {
                return Err(OpenAiError::InvalidConfiguration {
                    detail: "an HTTP layer wraps the native Responses HTTPS transport; remove the host transport or Chat Completions gateway",
                });
            }
            if !matches!(config.responses_transport, ResponsesTransport::Https) {
                return Err(OpenAiError::InvalidConfiguration {
                    detail: "an HTTP layer wraps Responses HTTPS; select the HTTPS Responses transport",
                });
            }
        }
        if config.host_transport.is_none() {
            return Ok(());
        }
        if !matches!(config.responses_transport, ResponsesTransport::Https) {
            return Err(OpenAiError::InvalidConfiguration {
                detail: "native Responses host transports serve HTTPS; select the HTTPS Responses transport",
            });
        }
        Ok(())
    }

    pub(crate) fn make(
        &self,
        config: Arc<ModelConfig>,
        max_attempts: NonZeroU32,
    ) -> DefaultResponsesService {
        if let Some(client) = &self.http_client {
            ResponsesService::standard_with_http_client_and_max_attempts(
                config,
                client.clone(),
                max_attempts,
            )
        } else {
            ResponsesService::standard_with_max_attempts(config, max_attempts)
        }
    }
}
