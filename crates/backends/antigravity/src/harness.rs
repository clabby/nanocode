use nanocode_agent_config::configuration::{AgentResult, RuntimeBackend, RuntimeRequest};
use nanocode_agent_core::{
    BackendId, HarnessFamily, Lineage, ModelSelection, NanocodeError, Result, Thinking,
    backend::BackendFuture,
};
use nanocode_backend_antigravity_client::{
    CacheConfig, Endpoint, GoogleConfig, RetryPolicy, Sender,
};
use nanocode_durability::CheckpointFormat;
use nanocode_process_contracts::{FileHost, ShellExecutor};
use std::sync::Arc;

pub const FAMILY: &str = "antigravity";
pub const DEFAULT_MODEL: &str = "gemini-4-neon";

#[derive(Clone, Default)]
pub struct AntigravityHarness {
    endpoint: Option<Endpoint>,
    google: GoogleConfig,
    retry: Option<RetryPolicy>,
    cache: CacheConfig,
    model: Option<String>,
    app_data: Option<std::path::PathBuf>,
    sender: Option<Arc<dyn Sender>>,
    files: Option<Arc<dyn FileHost>>,
    shell: Option<Arc<dyn ShellExecutor>>,
    connection: Arc<nanocode_sync::OnceCell<Connection>>,
}

#[derive(Clone)]
struct Connection {
    sender: Arc<dyn Sender>,
    endpoint: Endpoint,
}

impl From<GoogleConfig> for AntigravityHarness {
    fn from(google: GoogleConfig) -> Self {
        Self::new().google(google)
    }
}

impl AntigravityHarness {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    #[must_use]
    pub fn endpoint(mut self, endpoint: Endpoint) -> Self {
        self.endpoint = Some(endpoint);
        self
    }

    #[must_use]
    pub fn google(mut self, google: GoogleConfig) -> Self {
        self.google = google;
        self
    }

    #[must_use]
    pub const fn retry(mut self, retry: RetryPolicy) -> Self {
        self.retry = Some(retry);
        self
    }

    #[must_use]
    pub const fn explicit_cache(mut self, cache: CacheConfig) -> Self {
        self.cache = cache;
        self
    }

    #[must_use]
    pub fn model(mut self, model: impl Into<String>) -> Self {
        self.model = Some(model.into());
        self
    }

    #[must_use]
    pub fn app_data(mut self, path: impl Into<std::path::PathBuf>) -> Self {
        self.app_data = Some(path.into());
        self
    }

    #[must_use]
    pub fn sender(mut self, sender: Arc<dyn Sender>) -> Self {
        self.sender = Some(sender);
        self
    }

    #[must_use]
    pub fn files(mut self, files: Arc<dyn FileHost>) -> Self {
        self.files = Some(files);
        self
    }

    #[must_use]
    pub fn shell(mut self, shell: Arc<dyn ShellExecutor>) -> Self {
        self.shell = Some(shell);
        self
    }

    pub fn selection(model: impl Into<String>) -> ModelSelection {
        ModelSelection::new(HarnessFamily::new(FAMILY), model)
    }
}

pub const fn level(thinking: Thinking) -> Option<&'static str> {
    match thinking {
        Thinking::None => Some("MINIMAL"),
        Thinking::Low => Some("LOW"),
        Thinking::Medium => Some("MEDIUM"),
        _ => Some("HIGH"),
    }
}

impl nanocode_agent_config::configuration::BuiltinFormats for AntigravityHarness {
    fn builtin_formats() -> Vec<Arc<dyn CheckpointFormat>> {
        vec![Arc::new(crate::durability::AntigravityCheckpointFormat)]
    }
}

impl RuntimeBackend for AntigravityHarness {
    fn id(&self) -> BackendId {
        BackendId::new(FAMILY)
    }

    fn is_harness(&self) -> bool {
        true
    }

    fn default_model(&self) -> ModelSelection {
        Self::selection(
            self.model
                .clone()
                .unwrap_or_else(|| DEFAULT_MODEL.to_owned()),
        )
    }

    fn default_thinking(&self) -> Option<Thinking> {
        Some(Thinking::High)
    }

    fn validate_model(&self, model: &str) -> Result<()> {
        let lower = model.to_ascii_lowercase();
        match lower.starts_with("gemini") || lower.starts_with("claude") {
            true => Ok(()),
            false => Err(NanocodeError::InvalidRequest(format!(
                "the antigravity harness runs gemini-* models (or claude-* through an Antigravity gateway), not {model:?}"
            ))),
        }
    }

    fn checkpoint_format(&self) -> Option<Arc<dyn CheckpointFormat>> {
        Some(Arc::new(crate::durability::AntigravityCheckpointFormat))
    }

    fn build(&self, request: RuntimeRequest) -> BackendFuture<AgentResult> {
        let harness = self.clone();
        Box::pin(async move { harness.assemble(request).await })
    }
}

impl AntigravityHarness {
    async fn assemble(&self, request: RuntimeRequest) -> AgentResult {
        self.assemble_child(request, None).await
    }

    pub(crate) async fn assemble_child(
        &self,
        request: RuntimeRequest,
        explicit: Option<Lineage>,
    ) -> AgentResult {
        use crate::driver::{Config, Driver};
        use crate::nanotools::NanoTools;
        use crate::profile::Profile;
        use crate::request::{Ids, Prefix};
        use nanocode_agent_config::configuration::DelegationAuthority;
        use nanocode_agent_core::{AgentHandle, backend::BackendRuntime};
        use nanocode_backend_antigravity_client::{Client, ExplicitCache};
        use nanocode_backend_antigravity_tools::{DECLARATIONS, Engine};
        use nanocode_tool_contracts::ToolCollection as _;
        use nanocode_tool_host::{Tools, runtime::ToolRuntime};
        let invalid = |error: String| NanocodeError::InvalidRequest(error);
        let RuntimeRequest {
            request: mut spec,
            settings,
            defaults,
            workspace,
            subagents,
            services,
        } = request;
        let recipe_settings = settings.clone();
        let recipe_defaults = defaults.clone();
        let recipe_subagents = subagents.clone();
        let settings = settings.or(&defaults);
        let model = spec.model.clone();
        let session = spec
            .durable_state
            .as_ref()
            .map(|state| state.state_id().to_owned())
            .or_else(|| spec.options.session_id.clone())
            .unwrap_or_else(|| {
                new_id(&format!(
                    "{}{:?}",
                    model.as_str(),
                    nanocode_platform::system_time()
                ))
            });
        let mut fresh = false;
        let connection = self
            .connection
            .get_or_try_init(|| async {
                fresh = true;
                let sender = match self.sender.clone() {
                    Some(sender) => sender,
                    None => native_sender()?,
                };
                let endpoint = match self.endpoint.clone() {
                    Some(endpoint) => endpoint,
                    None => Endpoint::discover(
                        &self.google,
                        &|name: &str| std::env::var(name).ok(),
                        Arc::clone(&sender),
                        model.as_str(),
                    )
                    .await
                    .map_err(invalid)?,
                };
                Ok::<_, NanocodeError>(Connection { sender, endpoint })
            })
            .await?
            .clone();
        let notice = connection.endpoint.notice.clone().filter(|_| fresh);
        let Connection { sender, endpoint } = connection;
        if endpoint.route.is_vertex() && model.as_str().to_ascii_lowercase().starts_with("claude") {
            return Err(invalid(format!(
                "{model} on Vertex AI is served by Anthropic's Model Garden publisher (streamRawPredict with the Messages API), not the Gemini generateContent API the antigravity harness speaks; run it with --harness claude and CLAUDE_CODE_USE_VERTEX=1 (ANTHROPIC_VERTEX_PROJECT_ID, CLOUD_ML_REGION), which uses the same Application Default Credentials",
                model = model.as_str()
            )));
        }
        let mut client = Client::new(endpoint, sender);
        if let Some(retry) = self.retry {
            client = client.with_retry(retry);
        }
        let initial_notice = self.initial_notice(&spec, &settings, services.as_ref());
        let mut durable = match spec.durable_state.take() {
            Some(state) => Some(acquire(state, spec.thinking).await?),
            None => None,
        };
        let stored = durable.as_ref().and_then(|durable| durable.stored.clone());
        let pages = durable.as_mut().and_then(|durable| durable.pages.take());
        let imported = match (&stored, &spec.checkpoint) {
            (None, Some(checkpoint)) => Some(
                checkpoint
                    .payload()
                    .decode::<serde_json::Value>()
                    .map_err(|error| NanocodeError::InvalidCheckpoint(error.to_string()))?,
            ),
            _ => None,
        };
        let model = stored
            .as_ref()
            .or(imported.as_ref())
            .and_then(crate::driver::payload_model)
            .filter(|stored| stored.family() == model.family())
            .unwrap_or(model);
        let thinking = durable
            .as_ref()
            .and_then(|durable| durable.initial)
            .unwrap_or(spec.thinking);
        let initial_notice = initial_notice.filter(|_| stored.is_none() && imported.is_none());
        let facts = match durable.as_ref().and_then(|durable| durable.session.facts()) {
            Some(log) => {
                let facts = crate::facts::AntigravityFacts::open(log).await?;
                if let Some(payload) = stored.as_ref().or(imported.as_ref()) {
                    crate::record::import_base(&facts, &session, payload).await?;
                }
                Some(facts)
            }
            None => None,
        };
        let committed = stored.is_some() || imported.is_some();
        let lineage = explicit
            .or_else(|| {
                durable
                    .as_ref()
                    .and_then(|durable| durable.record.as_ref())
                    .map(|record| record.lineage.clone())
            })
            .or_else(|| {
                spec.checkpoint
                    .as_ref()
                    .map(|checkpoint| checkpoint.lineage().clone())
            })
            .or_else(|| {
                spec.parent.as_ref().map(|parent| {
                    Lineage::new(
                        parent.root_session_id(),
                        Some(parent.session_id().to_owned()),
                        nanocode_agent_core::Origin::Subagent,
                        1,
                    )
                })
            })
            .unwrap_or_else(|| Lineage::root(session.clone()));
        let profile = Profile::for_model(model.as_str()).with_level(level(thinking));
        let root = spec.parent.as_ref().map_or_else(
            || session.clone(),
            |parent| parent.root_session_id().to_owned(),
        );
        let factory = Arc::new(crate::factory::AntigravityFactory::new(
            crate::factory::Recipe {
                harness: self.clone(),
                settings: recipe_settings,
                defaults: recipe_defaults,
                workspace: Arc::clone(&workspace),
                subagents: recipe_subagents,
                services: Arc::clone(&services),
                routed: spec.spawn_factory.clone(),
                branches: durable.as_ref().and_then(|durable| {
                    durable
                        .record
                        .clone()
                        .map(|record| (durable.store.clone(), record))
                }),
            },
        ));
        let handle = AgentHandle::new(
            session.clone(),
            model.clone(),
            Arc::clone(&factory) as Arc<dyn nanocode_agent_core::backend::AgentFactory>,
        )
        .with_root_session_id(root)
        .with_spawn_factory(spec.spawn_factory.clone())
        .with_child_journal(durable.as_ref().map(|durable| durable.journal.clone()));
        let selected = match &settings.tools {
            Some(tools) => tools.materialize(handle.clone()),
            None => Tools::builder().without_defaults().build(),
        }
        .map_err(|error| invalid(error.to_string()))?;
        let selected = services
            .prepare_tools(
                &handle,
                selected,
                durable.as_ref().map(|durable| durable.store.clone()),
            )
            .map_err(|error| invalid(error.to_string()))?;
        let mut additive = services.shared_tools(false).await?;
        if let Some(subagents) = &subagents {
            additive.extend(subagents.tools(handle.clone(), DelegationAuthority::Turn));
        }
        let selected = selected.add_shared_tools(additive)?;
        let runtime = ToolRuntime::new_with_tools(workspace.to_path_buf(), Vec::new(), &selected);
        let nano = NanoTools::new(runtime, &session);
        let app_data = self
            .app_data
            .clone()
            .unwrap_or_else(|| home(&workspace).join(".nanocode").join("antigravity"));
        let brain = app_data.join("brain").join(&session);
        let shell = match self.shell.clone() {
            Some(shell) => shell,
            None => default_shell(services.as_ref())?,
        };
        let files = self
            .files
            .clone()
            .unwrap_or_else(|| Arc::new(nanocode_backend_antigravity_tools::LocalFiles));
        let search = crate::search::GoogleSearch {
            client: client.clone(),
            model: profile.model.clone(),
        };
        let engine = Engine::new(
            shell,
            files,
            workspace.to_path_buf(),
            brain.clone(),
            session.clone(),
            Some(Arc::new(search)),
        );
        engine.keep_step_files(durable.is_none());
        let skills = skills(&workspace);
        let system = crate::prompts::system(&crate::prompts::Environment {
            os: OS,
            workspace: &workspace,
            app_data: &app_data,
            conversation: &session,
            skills: &skills,
        });
        let system = match settings.instructions.as_deref() {
            Some(extra) if !extra.trim().is_empty() => format!("{system}\n{extra}"),
            _ => system,
        };
        let mut declarations = DECLARATIONS
            .lines()
            .map(|line| {
                match line.starts_with("{\"name\":\"read_file\"") && profile.claude_read_file {
                    true => nanocode_backend_antigravity_tools::READ_FILE_CLAUDE
                        .trim_end()
                        .to_owned(),
                    false => line.to_owned(),
                }
            })
            .collect::<Vec<_>>();
        declarations.extend(nano.declarations.iter().cloned());
        let prefix = Prefix::new(&system, &declarations);
        let mut cache_config = self.cache;
        if std::env::var("NANOCODE_ANTIGRAVITY_EXPLICIT_CACHE").is_ok_and(|value| value == "1") {
            cache_config.enabled = true;
        }
        let cache = cache_config
            .enabled
            .then(|| Arc::new(ExplicitCache::new(cache_config)));
        let ids = Ids {
            cascade: session.clone(),
            trajectory: new_id(&format!("trajectory{session}")),
        };
        let (runtime, events) = BackendRuntime::new(session.clone());
        let runtime = runtime.with_lineage(lineage.clone());
        let logs = durable
            .is_none()
            .then(|| brain.join(".system_generated").join("logs"));
        let (owner, keep) = match durable {
            Some(durable) => (Some(durable.owner), Some(durable.session)),
            None => (None, None),
        };
        let driver = Driver::new(Config {
            client,
            profile,
            prefix,
            ids,
            engine,
            nano,
            model,
            thinking,
            session,
            host_context: spec.host_context,
            cache,
            logs,
            notice,
            durable: owner,
            lineage,
            workspace: workspace.to_string_lossy().into_owned(),
            events: runtime.events(),
            keep,
            handle,
            committed,
            facts,
        });
        factory.bind(&driver.inner);
        let driver = match (stored, imported) {
            (Some(stored), _) => {
                let driver = driver.restore_payload(&stored)?;
                driver.inner.keep_pages(pages);
                driver
            }
            (None, Some(imported)) => {
                let driver = driver.restore_payload(&imported)?;
                driver.import().await?;
                driver
            }
            (None, None) => driver,
        };
        let Some(text) = initial_notice else {
            return Ok((runtime.bind(driver), events));
        };
        driver.seed_notice(&text);
        let built =
            nanocode_agent_core::Agent::persist_created(Ok((runtime.bind(driver), events))).await?;
        services.initial_notice_committed(&built.0, text);
        Ok(built)
    }

    fn initial_notice(
        &self,
        spec: &nanocode_agent_config::HarnessRequest,
        settings: &nanocode_agent_config::configuration::Settings,
        services: &dyn nanocode_agent_config::configuration::HostServices,
    ) -> Option<String> {
        use nanocode_agent_config::configuration::AgentTools;
        let remote = settings
            .tools
            .as_ref()
            .is_some_and(AgentTools::has_code_mode_host);
        let folds = !remote
            && spec.checkpoint.is_none()
            && !matches!(settings.tools, Some(AgentTools::PerAgent(_)));
        let state = spec.durable_state.as_ref().filter(|_| folds)?;
        state
            .described_record()
            .filter(|record| record.initial.is_none())?;
        services.initial_notice(
            state.state_id(),
            spec.parent
                .as_ref()
                .map(nanocode_agent_core::AgentHandle::session_id),
        )
    }
}

const MAINTENANCE_KINDS: &[&str] = &["compact"];

struct Acquired {
    owner: nanocode_durability::DurableOwner,
    session: nanocode_durability::DurableSession,
    journal: nanocode_agent_core::backend::ChildJournal,
    store: nanocode_durability::SharedStore,
    stored: Option<serde_json::Value>,
    pages: Option<crate::pages::Pages>,
    initial: Option<Thinking>,
    record: Option<nanocode_durability::SessionRecord>,
}

async fn acquire(
    state: nanocode_durability::DurableSession,
    thinking: Thinking,
) -> Result<Acquired> {
    use nanocode_durability::agent_error;
    let journal = state.child_journal();
    let store = state.shared_store();
    let (owner, stored) = state
        .acquire_agent_with_initial(nanocode_durability::InitialSettings {
            thinking,
            service_tier: nanocode_agent_core::ServiceTier::Standard,
        })
        .await
        .map_err(agent_error)?;
    owner
        .abandon_stale_kinds(MAINTENANCE_KINDS)
        .await
        .map_err(agent_error)?;
    let record = state.record().await.map_err(agent_error)?;
    let (stored, pages) = match stored {
        Some(payload) => {
            let stored = owner
                .load_payload(payload)
                .await
                .map_err(agent_error)?
                .decode::<serde_json::Value>()
                .map_err(agent_error)?;
            let (stored, pages) = crate::pages::hydrate(
                nanocode_durability::PayloadReader::from(&owner),
                stored,
                &mut std::collections::HashSet::new(),
            )
            .await
            .map_err(agent_error)?;
            (Some(stored), pages)
        }
        None => (None, None),
    };
    let initial = match &stored {
        Some(_) => None,
        None => record
            .as_ref()
            .and_then(|record| record.initial)
            .map(|initial| initial.thinking),
    };
    Ok(Acquired {
        owner,
        session: state,
        journal,
        store,
        stored,
        pages,
        initial,
        record,
    })
}

#[cfg(not(target_family = "wasm"))]
const OS: &str = std::env::consts::OS;

#[cfg(target_family = "wasm")]
const OS: &str = "linux";

#[cfg(not(target_family = "wasm"))]
fn native_sender() -> Result<Arc<dyn Sender>> {
    nanocode_backend_antigravity_client::Client::native_sender()
        .map_err(|error| NanocodeError::InvalidRequest(error.to_string()))
}

#[cfg(target_family = "wasm")]
fn native_sender() -> Result<Arc<dyn Sender>> {
    Err(NanocodeError::InvalidRequest(
        "the antigravity harness on wasm needs a host sender: AntigravityHarness::sender".into(),
    ))
}

#[cfg(not(target_family = "wasm"))]
fn home(_workspace: &std::path::Path) -> std::path::PathBuf {
    std::env::var_os("HOME").map_or_else(std::env::temp_dir, std::path::PathBuf::from)
}

#[cfg(not(target_family = "wasm"))]
fn default_shell(
    services: &dyn nanocode_agent_config::configuration::HostServices,
) -> Result<Arc<dyn ShellExecutor>> {
    Ok(services.process_executors().shell(Vec::new()))
}

#[cfg(target_family = "wasm")]
fn default_shell(
    _services: &dyn nanocode_agent_config::configuration::HostServices,
) -> Result<Arc<dyn ShellExecutor>> {
    Err(NanocodeError::InvalidRequest(
        "the antigravity harness on wasm needs a host shell: AntigravityHarness::shell".into(),
    ))
}

#[cfg(target_family = "wasm")]
fn home(workspace: &std::path::Path) -> std::path::PathBuf {
    workspace.to_path_buf()
}

#[cfg(target_family = "wasm")]
const fn skills(_workspace: &std::path::Path) -> Vec<(String, String, String)> {
    Vec::new()
}

#[cfg(not(target_family = "wasm"))]
fn skills(workspace: &std::path::Path) -> Vec<(String, String, String)> {
    let Ok(home) = nanocode_home::AgentHome::from_env() else {
        return Vec::new();
    };
    let project = nanocode_home::ProjectHome::discover(workspace);
    home.skills_for(&project)
        .skills
        .into_iter()
        .map(|skill| {
            let text = std::fs::read_to_string(&skill.skill_md).unwrap_or_default();
            let description = text
                .strip_prefix("---")
                .and_then(|rest| {
                    rest.split_once(
                        "
---",
                    )
                })
                .and_then(|(front, _)| {
                    front
                        .lines()
                        .find_map(|line| line.strip_prefix("description:"))
                        .map(|value| value.trim().trim_matches('"').to_owned())
                })
                .unwrap_or_default();
            (
                skill.name,
                skill.skill_md.display().to_string(),
                description,
            )
        })
        .collect()
}

pub(crate) fn new_id(seed: &str) -> String {
    use std::hash::{DefaultHasher, Hash, Hasher};
    let mut first = DefaultHasher::new();
    (seed, 1_u8).hash(&mut first);
    let mut second = DefaultHasher::new();
    (seed, 2_u8).hash(&mut second);
    let hex = format!("{:016x}{:016x}", first.finish(), second.finish());
    format!(
        "{}-{}-4{}-a{}-{}",
        &hex[0..8],
        &hex[8..12],
        &hex[13..16],
        &hex[17..20],
        &hex[20..32]
    )
}
