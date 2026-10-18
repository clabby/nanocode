#[cfg(not(target_family = "wasm"))]
#[path = "support/stream.rs"]
mod stream;
#[cfg(not(target_family = "wasm"))]
mod support;

#[cfg(not(target_family = "wasm"))]
const HELP: &str = "cargo run --locked -p nanocode --example responses_gateway -- GATEWAY [PROMPT...]
Runs one Codex agent through a third-party Responses API gateway with registered Rust tools.

GATEWAY and credentials:
  openrouter           OPENROUTER_API_KEY
  vercel               AI_GATEWAY_API_KEY or VERCEL_OIDC_TOKEN
  cloudflare           CLOUDFLARE_ACCOUNT_ID, CLOUDFLARE_API_TOKEN, optional CLOUDFLARE_GATEWAY_ID
  cloudflare-gateway   CLOUDFLARE_ACCOUNT_ID, CLOUDFLARE_GATEWAY_ID, OPENAI_API_KEY and/or CF_AIG_TOKEN

Optional settings:
  NANOCODE_GATEWAY_MODEL             sol, luna or astra (default: luna)
  NANOCODE_GATEWAY_PROVIDER_ROUTING  JSON object for OpenRouter provider or Vercel providerOptions.gateway
  NANOCODE_GATEWAY_TRANSPORT         https or websocket (default: https; websocket is Vercel-only)
  NANOCODE_EXAMPLE_API_BASE          Replace the gateway base URL, for example a local capture server";

#[cfg(not(target_family = "wasm"))]
fn optional(key: &str) -> support::Result<Option<String>> {
    match std::env::var(key) {
        Ok(value) => Ok(Some(value)),
        Err(std::env::VarError::NotPresent) => Ok(None),
        Err(error) => Err(error.into()),
    }
}

#[cfg(not(target_family = "wasm"))]
fn required(key: &str) -> support::Result<String> {
    optional(key)?.ok_or_else(|| format!("{key} is required; use --help").into())
}

#[cfg(not(target_family = "wasm"))]
fn gateway(
    name: &str,
) -> support::Result<(
    nanocode::codex::transport::ResponsesGateway,
    nanocode::codex::auth::OpenAiAuth,
)> {
    use nanocode::codex::{auth::OpenAiAuth, transport::ResponsesGateway};

    Ok(match name {
        "openrouter" => (
            ResponsesGateway::openrouter().app_title("Nanocodex SDK example"),
            OpenAiAuth::api_key(required("OPENROUTER_API_KEY")?),
        ),
        "vercel" => (
            ResponsesGateway::vercel(),
            OpenAiAuth::api_key(match optional("AI_GATEWAY_API_KEY")? {
                Some(key) => key,
                None => required("VERCEL_OIDC_TOKEN")?,
            }),
        ),
        "cloudflare" => {
            let mut gateway = ResponsesGateway::cloudflare(required("CLOUDFLARE_ACCOUNT_ID")?);
            if let Some(id) = optional("CLOUDFLARE_GATEWAY_ID")? {
                gateway = gateway.gateway_id(id);
            }
            (
                gateway,
                OpenAiAuth::api_key(required("CLOUDFLARE_API_TOKEN")?),
            )
        }
        "cloudflare-gateway" => {
            let mut gateway = ResponsesGateway::cloudflare_gateway(
                required("CLOUDFLARE_ACCOUNT_ID")?,
                required("CLOUDFLARE_GATEWAY_ID")?,
            );
            let token = optional("CF_AIG_TOKEN")?;
            if let Some(token) = &token {
                gateway = gateway.gateway_token(token.clone());
            }
            let auth = match (optional("OPENAI_API_KEY")?, token) {
                (Some(key), _) => OpenAiAuth::api_key(key),
                (None, Some(_)) => OpenAiAuth::gateway_managed(),
                (None, None) => return Err("set OPENAI_API_KEY or CF_AIG_TOKEN; use --help".into()),
            };
            (gateway, auth)
        }
        _ => {
            return Err(
                "expected openrouter, vercel, cloudflare or cloudflare-gateway; use --help".into(),
            );
        }
    })
}

#[cfg(not(target_family = "wasm"))]
async fn run() -> support::Result<()> {
    use nanocode::{Model, Nanocodex, OpenAi, Thinking};

    let mut args = std::env::args().skip(1);
    let Some(name) = args.next().filter(|name| name != "--help" && name != "-h") else {
        println!("{HELP}");
        return Ok(());
    };
    let (mut gateway, auth) = gateway(&name)?;
    if let Some(base) = optional("NANOCODE_EXAMPLE_API_BASE")? {
        gateway = gateway.base_url(base);
    }
    if let Some(routing) = optional("NANOCODE_GATEWAY_PROVIDER_ROUTING")? {
        gateway = gateway.provider_routing(serde_json::from_str(&routing)?);
    }
    let model: Model = optional("NANOCODE_GATEWAY_MODEL")?
        .as_deref()
        .unwrap_or("luna")
        .parse()?;
    let transport: nanocode::codex::transport::ResponsesTransport =
        optional("NANOCODE_GATEWAY_TRANSPORT")?
            .as_deref()
            .unwrap_or("https")
            .parse()?;
    let openai = OpenAi::builder(auth)
        .responses_gateway(gateway)
        .transport(transport)
        .model(model)
        .build()?;
    eprintln!(
        "gateway {} at {} serving {} over {}",
        name,
        openai.config().api_base_url,
        model.as_str(),
        transport
    );
    let prompt: Vec<String> = args.collect();
    let prompt = if prompt.is_empty() {
        "Use Code Mode to call tools.checked_sum({a:19,b:23}), then reply with the sum only."
            .to_owned()
    } else {
        prompt.join(" ")
    };
    let runtime = Nanocodex::builder()
        .openai(openai)
        .tools(support::tools()?)
        .without_cua()
        .runner_management(false)
        .thinking(Thinking::Low)
        .build_async()
        .await?;
    let outcome: support::Result<()> = async {
        let (agent, mut events) = runtime
            .spawn()
            .backend::<nanocode::Codex>()
            .model(model)
            .build()
            .await?;
        eprintln!("session {}", agent.session_id());
        stream::prompt(&agent, &mut events, prompt).await?;
        agent.flush().await?;
        Ok(())
    }
    .await;
    let shutdown = runtime.shutdown().await;
    outcome?;
    shutdown?;
    Ok(())
}

#[cfg(not(target_family = "wasm"))]
fn main() {
    support::main(run());
}

#[cfg(target_family = "wasm")]
fn main() {}
