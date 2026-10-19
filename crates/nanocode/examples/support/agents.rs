use crate::support::Result;
use nanocode::{ClaudeClient, Nanocode, OpenAi, Thinking, Tools};

pub fn runtime(tools: Tools) -> Result<nanocode::NanocodeBuilder> {
    let (openai, claude) = match std::env::var("NANOCODE_EXAMPLE_API_BASE") {
        Ok(base) => (
            OpenAi::builder(std::env::var("OPENAI_API_KEY")?)
                .api_base_url(base.trim_end_matches('/'))
                .transport(nanocode::oai::transport::ResponsesTransport::Https)
                .websocket_warmup(false)
                .build()?,
            ClaudeClient::with_api_key(
                format!("{}/messages", base.trim_end_matches('/')),
                std::env::var("ANTHROPIC_API_KEY")?,
            ),
        ),
        Err(std::env::VarError::NotPresent) => (OpenAi::default(), ClaudeClient::default()),
        Err(error) => return Err(error.into()),
    };
    Ok(Nanocode::builder()
        .openai(openai)
        .claude(claude)
        .tools(tools)
        .without_cua()
        .runner_management(false)
        .thinking(Thinking::Low))
}

pub async fn spawn(
    runtime: &Nanocode,
    backend: &str,
) -> Result<(nanocode::Agent, nanocode::AgentEvents)> {
    Ok(match backend {
        "claude" => {
            runtime
                .spawn()
                .backend::<nanocode::Claude>()
                .model(nanocode::ClaudeModel::Haiku55)
                .build()
                .await?
        }
        "codex" => {
            runtime
                .spawn()
                .backend::<nanocode::Codex>()
                .model(nanocode::Model::Luna)
                .build()
                .await?
        }
        _ => return Err("backend must be claude or codex".into()),
    })
}
