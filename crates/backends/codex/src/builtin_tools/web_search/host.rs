use std::{sync::Arc, time::Duration};

use nanocode_tool_host::{FunctionHandler, FunctionTool, ToolContext, embedded::HostFuture};
use serde_json::Value;

use crate::client::transport::host::{HostConnectRequest, HostError, HostTransport};

#[path = "history.rs"]
mod history;
#[path = "schema.rs"]
mod schema;
#[path = "wire.rs"]
mod wire;

use self::{
    history::recent_input,
    schema::commands_schema,
    wire::{SearchCommands, SearchRequest, SearchResponse, SearchSettings},
};

const DESCRIPTION: &str = include_str!("web_run_description.md");
const MAX_RESPONSE_BYTES: usize = 1024 * 1024;
const MAX_ATTEMPTS: usize = 2;
const RETRY_DELAY: Duration = Duration::from_millis(200);

struct HostSearch {
    endpoint: String,
    transport: Arc<dyn HostTransport>,
}

#[must_use]
pub fn web_search_tool(api_base_url: &str, transport: Arc<dyn HostTransport>) -> FunctionTool {
    FunctionTool::new(
        "web__run",
        DESCRIPTION,
        commands_schema(),
        Arc::new(HostSearch {
            endpoint: format!("{}/alpha/search", api_base_url.trim_end_matches('/')),
            transport,
        }),
    )
    .parallel_tool_calls(true)
}

impl FunctionHandler for HostSearch {
    fn call<'a>(
        &'a self,
        _name: &'a str,
        input: Value,
        context: ToolContext<'a>,
    ) -> HostFuture<'a, Result<Value, String>> {
        Box::pin(async move {
            let commands: SearchCommands = if input.is_null() {
                SearchCommands::default()
            } else {
                serde_json::from_value(input)
                    .map_err(|error| format!("failed to parse web.run arguments: {error}"))?
            };
            let history = recent_input(context.history());
            let body = serde_json::to_string(&SearchRequest {
                id: context.session_id(),
                model: context.model(),
                input: history.as_deref(),
                commands: &commands,
                settings: SearchSettings {
                    allowed_callers: ["direct"],
                    external_web_access: true,
                },
                max_output_tokens: (context.output_token_budget() != usize::MAX)
                    .then(|| u64::try_from(context.output_token_budget()).unwrap_or(u64::MAX)),
            })
            .map_err(|error| error.to_string())?;
            let session = context.session_id();
            let mut attempt = 1;
            let response = loop {
                match self.send(session, &body).await {
                    Err(error) if error.is_reconnectable() && attempt < MAX_ATTEMPTS => {
                        attempt += 1;
                        self.transport.sleep(session, RETRY_DELAY).await;
                    }
                    Err(error) => {
                        return Err(format!("standalone web search failed: {error}"));
                    }
                    Ok(response) => break response,
                }
            };
            let SearchResponse { output, results: _ } =
                serde_json::from_slice(&response).map_err(|error| {
                    format!("failed to decode standalone web search response: {error}")
                })?;
            Ok(Value::String(output))
        })
    }
}

impl HostSearch {
    async fn send(&self, session: &str, body: &str) -> Result<Vec<u8>, HostError> {
        let request = HostConnectRequest::new(&self.endpoint, "", None, false, session, None);
        let mut response = self.transport.http(request, body).await?;
        let mut bytes = Vec::new();
        while let Some(chunk) = response.body.next().await? {
            bytes.extend(chunk);
            if bytes.len() > MAX_RESPONSE_BYTES {
                return Err(HostError::new(
                    "standalone web search response exceeded 1 MiB",
                ));
            }
        }
        Ok(bytes)
    }
}
