use std::path::{Path, PathBuf};

use nanocode::{
    Claude, ClaudeHarness, ClaudeModel, Codex, CodexHarness, Model, ModelSelection, Nanocode,
    OpenAi, Runner, RunnerServerConfig, ServiceTier, Thinking,
    agent::{Agent, AgentEvents},
    claude::ClaudeClient,
    durability::SessionStore,
};

use crate::cli::{GatewayArg, GatewayArgs, RemoteArgs, RunnerModeArg};

pub type Error = Box<dyn std::error::Error + Send + Sync>;
pub type Result<T> = std::result::Result<T, Error>;

#[derive(Clone)]
pub struct HostConfig {
    pub workspace: PathBuf,
    pub store: Option<SessionStore>,
    pub subagents: bool,
    pub runners: Vec<Runner>,
    pub runner_servers: Vec<RunnerServerConfig>,
    pub gateway: Option<Gateway>,
}

#[derive(Clone)]
pub struct Gateway {
    route: nanocode::codex::transport::ResponsesGateway,
    auth: nanocode::codex::OpenAiAuth,
    credential: &'static str,
    base_url: String,
}

impl Gateway {
    pub fn serves(&self, model: &ModelSelection) -> bool {
        model
            .as_codex()
            .is_none_or(|model| self.route.serves(model))
    }

    pub fn check(&self, model: &ModelSelection) -> Result<()> {
        if self.serves(model) {
            return Ok(());
        }
        let served = ModelSelection::for_family(nanocode::HarnessFamily::Codex)
            .filter(|candidate| self.serves(candidate))
            .map(|candidate| candidate.to_string())
            .collect::<Vec<_>>()
            .join(", ");
        Err(format!(
            "{model} is not available through the {} gateway; choose one of {served} or a Claude model",
            self.route.name()
        )
        .into())
    }

    pub const fn name(&self) -> &'static str {
        self.route.name()
    }

    pub fn compacts(&self, model: &ModelSelection) -> bool {
        model.as_codex().is_none() || self.route.supports_inline_compaction()
    }

    pub fn describe(&self) -> String {
        format!(
            "Codex via {} {} · credential {}",
            self.route.name(),
            self.base_url,
            self.credential
        )
    }
}

fn env_secret(name: &'static str) -> Option<(&'static str, String)> {
    std::env::var(name)
        .ok()
        .filter(|value| !value.trim().is_empty())
        .map(|value| (name, value))
}

pub fn gateway(args: &GatewayArgs) -> Result<Option<Gateway>> {
    use nanocode::codex::{OpenAiAuth, transport::ResponsesGateway};
    let Some(kind) = args.gateway else {
        return Ok(None);
    };
    let account = || {
        args.cloudflare_account
            .clone()
            .filter(|value| !value.trim().is_empty())
            .ok_or_else(|| {
                format!(
                    "--gateway {} needs --cloudflare-account or CLOUDFLARE_ACCOUNT_ID",
                    kind_name(kind)
                )
            })
    };
    let missing = |names: &str| -> Error {
        format!(
            "--gateway {} needs {names} in the environment",
            kind_name(kind)
        )
        .into()
    };
    let (mut route, auth, credential) = match kind {
        GatewayArg::Openrouter => {
            let (name, key) =
                env_secret("OPENROUTER_API_KEY").ok_or_else(|| missing("OPENROUTER_API_KEY"))?;
            (
                ResponsesGateway::openrouter(),
                OpenAiAuth::api_key(key),
                name,
            )
        }
        GatewayArg::Vercel => {
            let (name, key) = env_secret("AI_GATEWAY_API_KEY")
                .or_else(|| env_secret("VERCEL_OIDC_TOKEN"))
                .ok_or_else(|| missing("AI_GATEWAY_API_KEY or VERCEL_OIDC_TOKEN"))?;
            (ResponsesGateway::vercel(), OpenAiAuth::api_key(key), name)
        }
        GatewayArg::Cloudflare => {
            let (name, key) = env_secret("CLOUDFLARE_API_TOKEN")
                .ok_or_else(|| missing("CLOUDFLARE_API_TOKEN"))?;
            let mut route = ResponsesGateway::cloudflare(account()?);
            if let Some(gateway) = args
                .cloudflare_gateway
                .clone()
                .filter(|value| !value.trim().is_empty())
            {
                route = route.gateway_id(gateway);
            }
            (route, OpenAiAuth::api_key(key), name)
        }
        GatewayArg::CloudflareGateway => {
            let gateway = args
                .cloudflare_gateway
                .clone()
                .filter(|value| !value.trim().is_empty())
                .ok_or("--gateway cloudflare-gateway needs --cloudflare-gateway or CLOUDFLARE_GATEWAY_ID")?;
            let mut route = ResponsesGateway::cloudflare_gateway(account()?, gateway);
            let gateway_token = env_secret("CF_AIG_TOKEN");
            if let Some((_, token)) = &gateway_token {
                route = route.gateway_token(token.clone());
            }
            match (
                env_secret("OPENAI_API_KEY"),
                gateway_token,
                env_secret("CLOUDFLARE_API_TOKEN"),
            ) {
                (Some((name, key)), _, _) => (route, OpenAiAuth::api_key(key), name),
                (None, Some(_), _) => (route, OpenAiAuth::gateway_managed(), "CF_AIG_TOKEN"),
                (None, None, Some((name, key))) => (route, OpenAiAuth::api_key(key), name),
                (None, None, None) => {
                    return Err(missing(
                        "OPENAI_API_KEY, CF_AIG_TOKEN or CLOUDFLARE_API_TOKEN",
                    ));
                }
            }
        }
    };
    if let Some(url) = &args.gateway_base_url {
        route = route.base_url(url.clone());
    }
    let base_url = route.api_base_url().to_owned();
    Ok(Some(Gateway {
        route,
        auth,
        credential,
        base_url,
    }))
}

const fn kind_name(kind: GatewayArg) -> &'static str {
    match kind {
        GatewayArg::Openrouter => "openrouter",
        GatewayArg::Vercel => "vercel",
        GatewayArg::Cloudflare => "cloudflare",
        GatewayArg::CloudflareGateway => "cloudflare-gateway",
    }
}

#[derive(Clone)]
pub struct Host {
    pub config: HostConfig,
    pub runtime: Nanocode,
}

impl Host {
    pub async fn build(
        config: HostConfig,
        model: &ModelSelection,
        thinking: Thinking,
    ) -> Result<Self> {
        let openai = match &config.gateway {
            Some(gateway) => {
                gateway.check(model)?;
                OpenAi::builder(gateway.auth.clone()).responses_gateway(gateway.route.clone())
            }
            None => OpenAi::builder(nanocode::codex::OpenAiAuth::chatgpt()),
        }
        .raw_api_events(false)
        .build()?;
        let codex = CodexHarness::new(openai);
        let claude = ClaudeHarness::new(ClaudeClient::default());
        let mut builder = Nanocode::builder()
            .openai(codex)
            .claude(claude)
            .workspace(config.workspace.clone());
        for runner in &config.runners {
            builder = builder.runner(runner.clone());
        }
        for server in &config.runner_servers {
            builder = builder.runner_server(server.clone());
        }
        builder = match &config.store {
            Some(store) => builder.durability(store.clone()),
            None => builder.durability(nanocode::durability::MemoryStore::new()?),
        };
        if !config.subagents {
            builder = builder.without_subagents();
        }
        let builder = match (model.as_codex(), model.as_claude()) {
            (Some(model), _) => {
                builder
                    .for_backend::<Codex>()
                    .model(model)
                    .thinking(thinking)
                    .build_async()
                    .await
            }
            (_, Some(model)) => {
                builder
                    .for_backend::<Claude>()
                    .model(model)
                    .thinking(thinking)
                    .build_async()
                    .await
            }
            _ => return Err(format!("unsupported model {model}").into()),
        };
        Ok(Self {
            config,
            runtime: builder?,
        })
    }

    pub async fn start(
        &self,
        model: &ModelSelection,
        thinking: Thinking,
    ) -> Result<(Agent, AgentEvents)> {
        let started = match (model.as_codex(), model.as_claude()) {
            (Some(model), _) => {
                self.runtime
                    .spawn()
                    .backend::<Codex>()
                    .model(model)
                    .thinking(thinking)
                    .build()
                    .await
            }
            (_, Some(model)) => {
                self.runtime
                    .spawn()
                    .backend::<Claude>()
                    .model(model)
                    .thinking(thinking)
                    .build()
                    .await
            }
            _ => return Err(format!("unsupported model {model}").into()),
        };
        Ok(started?)
    }

    pub async fn open(
        &self,
        store: &SessionStore,
        stored: &nanocode::durability::StoredSession,
    ) -> Result<(Agent, AgentEvents)> {
        Ok(self.runtime.open_loaded(store, stored).await?)
    }
}

pub fn parse_model(value: &str) -> Result<ModelSelection> {
    let value = value.trim();
    if let Ok(model) = value.parse::<Model>() {
        return Ok(ModelSelection::Codex(model));
    }
    if let Ok(model) = value.parse::<ClaudeModel>() {
        return Ok(ModelSelection::Claude(model));
    }
    let known = ModelSelection::for_family(nanocode::HarnessFamily::Codex)
        .chain(ModelSelection::for_family(nanocode::HarnessFamily::Claude))
        .map(|model| model.to_string())
        .collect::<Vec<_>>()
        .join(", ");
    Err(format!("unknown model {value:?}; known models: {known}").into())
}

pub fn parse_thinking(value: &str) -> Result<Thinking> {
    Thinking::ALL
        .into_iter()
        .find(|thinking| thinking.as_str().eq_ignore_ascii_case(value.trim()))
        .ok_or_else(|| {
            let known = Thinking::ALL.map(Thinking::as_str).join(", ");
            format!("unknown thinking level {value:?}; expected one of {known}").into()
        })
}

pub fn parse_tier(value: &str) -> Result<ServiceTier> {
    match value {
        "standard" => Ok(ServiceTier::Standard),
        "fast" => Ok(ServiceTier::Fast),
        "ultrafast" => Ok(ServiceTier::Ultrafast),
        other => Err(format!("unknown service tier {other:?}").into()),
    }
}

pub fn home_dir(explicit: Option<&Path>) -> Result<PathBuf> {
    if let Some(home) = explicit {
        return Ok(home.to_path_buf());
    }
    let home = nanocode_home::AgentHome::from_env()?;
    Ok(home.nanocode_home().to_path_buf())
}

pub async fn open_store(home: &Path) -> Result<SessionStore> {
    let store = nanocode_store_jsonl::JsonlStore::open_home_async(home).await?;
    Ok(nanocode::session_store(store)?)
}

pub fn runner_configs(args: &RemoteArgs) -> Result<(Vec<Runner>, Vec<RunnerServerConfig>)> {
    let mut runners = Vec::new();
    let mut servers = Vec::new();
    match (&args.runner_connect, args.runner_host) {
        (Some(url), _) => {
            let token = args
                .runner_token
                .as_ref()
                .ok_or("--runner-connect requires --runner-token or NANOCODE_RUNNER_TOKEN")?;
            let mut runner = Runner::connect(url).token(token);
            if let Some(name) = &args.runner_name {
                runner = runner.name(name);
            }
            runner = match args.runner_mode {
                RunnerModeArg::Alongside => runner.alongside(),
                RunnerModeArg::Replace => runner.replace(),
            };
            if let Some(workspace) = &args.runner_workspace {
                runner = runner.workspace(workspace.clone());
            }
            if let Some(ms) = args.runner_call_timeout_ms {
                runner = runner.call_timeout(std::time::Duration::from_millis(ms));
            }
            runners.push(runner);
        }
        (None, Some(address)) => {
            let mut server = RunnerServerConfig::listen(address.to_string());
            if let Some(token) = &args.runner_token {
                server = server.token(token);
            }
            server = match args.runner_mode {
                RunnerModeArg::Alongside => server.alongside(),
                RunnerModeArg::Replace => server.replace(),
            };
            if let Some(workspace) = &args.runner_workspace {
                server = server.workspace(workspace.clone());
            }
            if let Some(ms) = args.runner_call_timeout_ms {
                server = server.call_timeout(std::time::Duration::from_millis(ms));
            }
            servers.push(server);
        }
        (None, None) if matches!(args.runner_mode, RunnerModeArg::Replace) => {
            return Err("replace mode requires --runner-connect or --runner-host".into());
        }
        (None, None) => {}
    }
    Ok((runners, servers))
}
