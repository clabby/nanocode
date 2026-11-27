use crate::nanotools::NanoTools;
use crate::profile::Profile;
use crate::request::{Ids, Prefix};
use nanocode_agent_core::{
    Agent, AgentSessionContext, Capabilities, ForkRequest, HarnessFamily, Lineage, ModelSelection,
    ModelTransport, NanocodeError, Persistence, Result, SessionCheckpoint, SideAnswer,
    SpawnOptions, SteerReceiptStatus, Thinking,
    backend::{
        BackendFuture, BackendPrompt, BackendPromptRoute, BackendTurn, BackendTurnKey,
        LifecycleBackend, ParkedCall, TurnBoundary,
    },
    events::{AgentEventKind, AgentEventPublisher, AgentEvents},
    input::{Prompt, PromptInput, UserInput},
};
use nanocode_backend_antigravity_client::{Client, Content, ExplicitCache, FunctionCall, Part};
use nanocode_backend_antigravity_tools::Engine;
use nanocode_durability::{DurableOwner, EncodedPayload, agent_error};
use nanocode_sync::{Atom, Signal};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json, value::RawValue};
use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicU32, Ordering};

pub const USAGE_LOG: &str = "usage.jsonl";

pub struct Config {
    pub client: Client,
    pub profile: Profile,
    pub prefix: Prefix,
    pub ids: Ids,
    pub engine: Engine,
    pub nano: NanoTools,
    pub model: ModelSelection,
    pub thinking: Thinking,
    pub session: String,
    pub host_context: Option<Arc<str>>,
    pub cache: Option<Arc<ExplicitCache>>,
    pub logs: Option<PathBuf>,
    pub notice: Option<String>,
    pub durable: Option<DurableOwner>,
    pub lineage: Lineage,
    pub workspace: String,
    pub events: AgentEventPublisher,
    pub keep: Option<nanocode_durability::DurableSession>,
    pub handle: nanocode_agent_core::AgentHandle,
    pub committed: bool,
    pub facts: Option<crate::facts::AntigravityFacts>,
}

#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize)]
pub(crate) struct Usage {
    pub(crate) input: u64,
    pub(crate) cached: u64,
    pub(crate) output: u64,
    pub(crate) reasoning: u64,
    pub(crate) total: u64,
}

#[derive(Clone, Default, Serialize, Deserialize)]
pub(crate) struct Progress {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) instruction_revision: Option<u64>,
    #[serde(default)]
    pub(crate) usage: Usage,
    #[serde(default)]
    pub(crate) calls: u32,
    #[serde(default)]
    pub(crate) steers: Vec<(u32, u32)>,
    #[serde(default)]
    pub(crate) responses: Vec<Part>,
    #[serde(default)]
    pub(crate) message: String,
}

#[derive(Clone, Default)]
pub struct State {
    pub(crate) contents: Vec<Content>,
    pub(crate) step: u64,
    pub(crate) calls: u64,
    pub(crate) streak: u32,
    pub(crate) requests: Vec<String>,
    pub(crate) settings_sent: bool,
    pub(crate) prompt_tokens: u64,
    pub(crate) last_execution: Option<String>,
    pub(crate) executions: u64,
    pub(crate) operation: Option<String>,
    pub(crate) progress: Progress,
    pub(crate) transcript: Vec<Content>,
}

pub(crate) const OPEN: u8 = 0;
pub(crate) const CLOSING: u8 = 1;
pub(crate) const STOPPED: u8 = 2;

pub(crate) type Job = Box<dyn FnOnce() -> BackendFuture<()> + Send>;

#[derive(Clone)]
pub(crate) struct Steer {
    pub(crate) index: Option<u32>,
    pub(crate) id: Option<String>,
    pub(crate) prompt: Prompt,
}

pub(crate) struct Turn {
    pub(crate) key: u64,
    pub(crate) id: String,
    pub(crate) host_context: Arc<str>,
    pub(crate) pending: bool,
    pub(crate) automatic: bool,
    pub(crate) cancel_on_admission: bool,
    pub(crate) cancel: Signal,
    pub(crate) stop: Signal,
    pub(crate) started: AtomicBool,
    pub(crate) accepting: AtomicBool,
    pub(crate) calls: AtomicU32,
    pub(crate) steers: Atom<Vec<Steer>>,
    pub(crate) settled: Signal,
    pub(crate) events: AgentEventPublisher,
}

pub(crate) struct Inner {
    pub(crate) config: Config,
    pub(crate) state: Atom<State>,
    pub(crate) settled: Atom<State>,
    pub(crate) profile: Atom<Profile>,
    pub(crate) thinking: Atom<Thinking>,
    pub(crate) turns: Atom<BTreeMap<u64, Arc<Turn>>>,
    pub(crate) jobs: nanocode_sync::mpsc::UnboundedSender<Job>,
    pub(crate) life: AtomicU8,
    pub(crate) noticed: AtomicBool,
    pub(crate) notices: Atom<Vec<String>>,
    pub(crate) initialized: AtomicBool,
    pub(crate) pages: nanocode_sync::Slot<crate::pages::Pages>,
    pub(crate) auto_compact: Atom<Option<u64>>,
}

pub struct Driver {
    pub(crate) inner: Arc<Inner>,
}

pub(crate) fn emit(events: &AgentEventPublisher, kind: AgentEventKind, mut value: Value) {
    if let Some(map) = value.as_object_mut()
        && !map.contains_key("turn_id")
    {
        map.insert(
            "turn_id".to_owned(),
            Value::String(events.turn_id().unwrap_or(events.request_id()).to_owned()),
        );
    }
    if let Ok(raw) = RawValue::from_string(value.to_string()) {
        drop(events.publish_next(kind, Arc::from(raw)));
    }
}

pub(crate) fn prompt_text(input: &PromptInput) -> String {
    match input {
        PromptInput::Text(text) => text.clone(),
        PromptInput::Content(items) => items
            .iter()
            .filter_map(|item| match item {
                UserInput::Text { text } => Some(text.as_str()),
                _ => None,
            })
            .collect::<Vec<_>>()
            .join("\n"),
    }
}

pub(crate) fn response(call: &FunctionCall, key: &str, body: String) -> Part {
    let mut part = Part::response(call.name.clone(), key, body);
    if let Some(response) = part.function_response.as_mut() {
        response.id.clone_from(&call.id);
    }
    part
}

impl State {
    pub(crate) fn push(&mut self, content: Content) {
        self.transcript.push(content.clone());
        self.contents.push(content);
    }

    pub(crate) fn settle(&mut self) {
        self.operation = None;
        self.progress = Progress::default();
    }

    pub(crate) fn restored(payload: &Value) -> Result<Self> {
        let invalid =
            |error: serde_json::Error| NanocodeError::InvalidCheckpoint(error.to_string());
        if crate::pages::paged(payload) {
            return Err(NanocodeError::InvalidCheckpoint(
                "an antigravity checkpoint was restored before its pages were loaded".into(),
            ));
        }
        let contents: Vec<Content> = payload
            .get("contents")
            .cloned()
            .map(serde_json::from_value)
            .transpose()
            .map_err(invalid)?
            .unwrap_or_default();
        let number = |field: &str| payload.get(field).and_then(Value::as_u64).unwrap_or(0);
        let text = |field: &str| {
            payload
                .get(field)
                .and_then(Value::as_str)
                .map(str::to_owned)
        };
        let requests = payload
            .get("requests")
            .cloned()
            .map(serde_json::from_value)
            .transpose()
            .map_err(invalid)?
            .unwrap_or_default();
        let progress = payload
            .get("progress")
            .filter(|progress| !progress.is_null())
            .cloned()
            .map(serde_json::from_value)
            .transpose()
            .map_err(invalid)?
            .unwrap_or_default();
        let streak = u32::try_from(number("streak")).map_err(|_| {
            NanocodeError::InvalidCheckpoint("antigravity streak exceeds u32".into())
        })?;
        let settings_sent = payload
            .get("settings_sent")
            .and_then(Value::as_bool)
            .unwrap_or(!contents.is_empty());
        Ok(Self {
            settings_sent,
            transcript: contents.clone(),
            contents,
            step: number("step"),
            calls: number("calls"),
            streak,
            requests,
            prompt_tokens: number("prompt_tokens"),
            last_execution: text("last_execution"),
            executions: number("executions"),
            operation: text("operation"),
            progress,
        })
    }
}

pub(crate) fn payload_model(payload: &Value) -> Option<ModelSelection> {
    match payload.get("model")? {
        Value::String(model) if !model.trim().is_empty() => {
            Some(crate::harness::AntigravityHarness::selection(model.clone()))
        }
        model @ Value::Object(_) => serde_json::from_value(model.clone()).ok(),
        _ => None,
    }
}

impl Inner {
    pub(crate) fn now() -> std::time::SystemTime {
        nanocode_platform::system_time()
    }

    pub(crate) fn load(&self) -> State {
        State::clone(&self.state.get())
    }

    pub(crate) fn save(&self, state: &State) {
        self.state.set(state.clone());
    }

    pub(crate) fn life(&self) -> u8 {
        self.life.load(Ordering::SeqCst)
    }

    pub(crate) fn stop(&self) {
        self.life.store(STOPPED, Ordering::SeqCst);
    }

    pub(crate) fn compaction_limit(&self) -> u64 {
        let limit = self.profile.get().compaction_limit;
        self.auto_compact.get().map_or(limit, |set| set.min(limit))
    }

    pub(crate) fn thinking(&self) -> Thinking {
        *self.thinking.get()
    }

    pub(crate) fn set_thinking(&self, thinking: Thinking) {
        self.thinking.set(thinking);
        self.profile.set(
            Profile::clone(&self.profile.get())
                .with_level(crate::harness::level(&self.config.model, thinking)),
        );
    }

    pub(crate) fn payload(&self, state: &State) -> Value {
        let mut payload = self.head(state);
        payload.insert(
            "contents".to_owned(),
            serde_json::to_value(&state.contents).expect("antigravity contents encode as JSON"),
        );
        Value::Object(payload)
    }

    pub(crate) fn head(&self, state: &State) -> serde_json::Map<String, Value> {
        let Value::Object(head) = json!({
            "format": crate::durability::FORMAT,
            "provider": crate::harness::FAMILY,
            "model": self.config.model,
            "thinking": self.thinking(),
            "workspace": self.config.workspace,
            "step": state.step,
            "settings_sent": state.settings_sent,
            "calls": state.calls,
            "streak": state.streak,
            "requests": state.requests,
            "prompt_tokens": state.prompt_tokens,
            "executions": state.executions,
            "last_execution": state.last_execution,
            "operation": state.operation,
            "progress": state.operation.as_ref().map(|_| &state.progress),
        }) else {
            unreachable!("a JSON object literal is an object");
        };
        head
    }

    pub(crate) fn keep_pages(&self, pages: Option<crate::pages::Pages>) {
        if let Some(pages) = pages {
            drop(self.pages.put(pages));
        }
    }

    pub(crate) async fn write<T, F, Fut>(
        &self,
        state: &State,
        write: F,
    ) -> Result<nanocode_durability::Result<T>>
    where
        F: FnOnce(EncodedPayload) -> Fut,
        Fut: std::future::Future<Output = nanocode_durability::Result<T>>,
    {
        let (mut pages, held) = match self.pages.take() {
            Some(pages) => (pages, true),
            None => (crate::pages::Pages::default(), false),
        };
        let payload = match pages.encode(self.head(state), &state.contents) {
            Ok(payload) => payload,
            Err(error) => {
                if held {
                    drop(self.pages.put(crate::pages::Pages::default()));
                }
                return Err(agent_error(error));
            }
        };
        let written = write(payload).await;
        if held {
            if written.is_ok() {
                pages.committed();
            }
            drop(self.pages.put(pages));
        }
        Ok(written)
    }

    pub(crate) async fn persist(&self, state: &State) -> Result<()> {
        self.save(state);
        let (Some(durable), Some(operation)) = (&self.config.durable, &state.operation) else {
            return Ok(());
        };
        self.write(state, |payload| durable.advance(operation.clone(), payload))
            .await?
            .map_err(agent_error)
    }

    pub(crate) fn checkpoint_of(&self, state: &State) -> SessionCheckpoint {
        SessionCheckpoint::native(
            self.config.session.clone(),
            self.config.model.clone(),
            self.thinking(),
            self.config.lineage.clone(),
            self.config.session.clone(),
            None,
            true,
            self.payload(state),
        )
    }

    pub(crate) fn checkpoint(&self) -> SessionCheckpoint {
        self.checkpoint_of(&self.settled.get())
    }

    pub(crate) fn boundary(&self) -> TurnBoundary {
        TurnBoundary::portable(self.checkpoint())
    }
}

impl Driver {
    pub fn new(config: Config) -> Self {
        let (jobs, mut queue) = nanocode_sync::mpsc::unbounded_channel::<Job>();
        nanocode_platform::spawn(async move {
            while let Some(job) = queue.recv().await {
                job().await;
            }
        });
        let profile = config.profile.clone();
        let thinking = config.thinking;
        Self {
            inner: Arc::new(Inner {
                config,
                state: Atom::default(),
                settled: Atom::default(),
                profile: Atom::new(profile),
                thinking: Atom::new(thinking),
                turns: Atom::default(),
                jobs,
                life: AtomicU8::new(OPEN),
                noticed: AtomicBool::new(false),
                notices: Atom::default(),
                initialized: AtomicBool::new(false),
                pages: nanocode_sync::Slot::new(Some(crate::pages::Pages::default())),
                auto_compact: Atom::default(),
            }),
        }
    }

    pub fn restore_payload(self, payload: &Value) -> Result<Self> {
        let mut state = State::restored(payload)?;
        state.settle();
        if let Some(thinking) = payload
            .get("thinking")
            .filter(|thinking| !thinking.is_null())
            .cloned()
            .map(serde_json::from_value::<Thinking>)
            .transpose()
            .map_err(|error| NanocodeError::InvalidCheckpoint(error.to_string()))?
        {
            self.inner.set_thinking(thinking);
        }
        self.inner.state.set(state.clone());
        self.inner.settled.set(state);
        Ok(self)
    }

    pub fn seed_notice(&self, text: &str) {
        let mut state = self.inner.settled_state();
        state.step += 1;
        state.push(Content::user(vec![Part::text(
            nanocode_backend_antigravity_tools::system_message(text),
        )]));
        self.inner.state.set(state.clone());
        self.inner.settled.set(state);
    }

    pub async fn import(&self) -> Result<()> {
        let (Some(owner), Some(session)) = (&self.inner.config.durable, &self.inner.config.keep)
        else {
            return Ok(());
        };
        if !session
            .state()
            .await
            .map_err(agent_error)?
            .pending_operations()
            .is_empty()
        {
            return Ok(());
        }
        let settled = self.inner.settled.get();
        self.inner
            .write(&settled, |payload| owner.commit_checkpoint(payload))
            .await?
            .map_err(agent_error)
    }
}

fn unsupported<T: Send + 'static>(capability: &'static str) -> BackendFuture<Result<T>> {
    Box::pin(async move { Err(NanocodeError::UnsupportedCapability { capability }) })
}

fn done() -> BackendFuture<Result<()>> {
    Box::pin(async { Ok(()) })
}

impl LifecycleBackend for Driver {
    fn harness_family(&self) -> HarnessFamily {
        HarnessFamily::new(crate::harness::FAMILY)
    }

    fn capabilities(&self) -> Capabilities {
        let mut capabilities = Capabilities::NONE;
        capabilities.checkpoint = true;
        capabilities.compaction = true;
        capabilities.resume = self.inner.config.durable.is_some();
        capabilities.steering = true;
        capabilities.identified_steering = true;
        capabilities.side_question = true;
        capabilities.fork = true;
        capabilities.fork_at = true;
        capabilities.side_conversation = true;
        capabilities.spawn = true;
        capabilities.developer_messages = true;
        capabilities.thinking = nanocode_agent_core::Mutability::BetweenTurns;
        capabilities
    }

    fn persistence(&self) -> Option<Persistence> {
        self.inner
            .config
            .durable
            .as_ref()
            .map(|_| Persistence::durable(self.inner.config.session.clone()))
    }

    fn submit(&self, prompt: BackendPrompt) -> BackendFuture<Result<BackendTurn>> {
        let inner = Arc::clone(&self.inner);
        Box::pin(async move { inner.admit(prompt).await })
    }

    fn route(&self, prompt: BackendPrompt) -> BackendFuture<Result<BackendPromptRoute>> {
        let inner = Arc::clone(&self.inner);
        Box::pin(async move { inner.route(prompt).await })
    }

    fn steer(&self, key: BackendTurnKey, prompt: Prompt) -> BackendFuture<Result<()>> {
        let inner = Arc::clone(&self.inner);
        Box::pin(async move { inner.steer(key.0, None, prompt).await })
    }

    fn steer_with_id(
        &self,
        key: BackendTurnKey,
        id: String,
        prompt: Prompt,
    ) -> BackendFuture<Result<()>> {
        let inner = Arc::clone(&self.inner);
        Box::pin(async move { inner.steer(key.0, Some(id), prompt).await })
    }

    fn withdraw_steer(&self, key: BackendTurnKey, id: String) -> BackendFuture<Result<bool>> {
        let inner = Arc::clone(&self.inner);
        Box::pin(async move { inner.withdraw_steer(key.0, &id).await })
    }

    fn steer_receipt(
        &self,
        id: String,
        prompt: Prompt,
    ) -> BackendFuture<Result<SteerReceiptStatus>> {
        let inner = Arc::clone(&self.inner);
        Box::pin(async move {
            match &inner.config.durable {
                Some(owner) => owner.steer_receipt(id, &prompt).await.map_err(agent_error),
                None => Ok(SteerReceiptStatus::Unknown),
            }
        })
    }

    fn unfinished_request(&self, id: String) -> BackendFuture<Result<bool>> {
        let inner = Arc::clone(&self.inner);
        Box::pin(async move {
            match &inner.config.durable {
                Some(owner) => owner.unfinished_operation(id).await.map_err(agent_error),
                None => Ok(false),
            }
        })
    }

    fn cancel(&self, key: BackendTurnKey) -> BackendFuture<Result<()>> {
        let inner = Arc::clone(&self.inner);
        Box::pin(async move { inner.cancel(key.0).await })
    }

    fn cancel_active(&self) -> BackendFuture<Result<()>> {
        let inner = Arc::clone(&self.inner);
        Box::pin(async move { inner.cancel_active().await })
    }

    fn persist_initial(&self) -> BackendFuture<Result<()>> {
        let inner = Arc::clone(&self.inner);
        Box::pin(async move { inner.persist_initial().await })
    }

    fn discard_initial(&self) -> BackendFuture<Result<()>> {
        let inner = Arc::clone(&self.inner);
        Box::pin(async move { inner.discard_initial().await })
    }

    fn set_harness_model(&self, model: ModelSelection) -> BackendFuture<Result<()>> {
        let same = model == self.inner.config.model;
        Box::pin(async move {
            match same {
                true => Ok(()),
                false => Err(NanocodeError::InvalidRequest(
                    "the model is fixed for this thread; start a new thread to use another model"
                        .into(),
                )),
            }
        })
    }

    fn set_thinking(&self, thinking: Thinking) -> BackendFuture<Result<()>> {
        let inner = Arc::clone(&self.inner);
        Box::pin(async move {
            inner
                .config
                .model
                .capabilities(ModelTransport::Native)
                .check_thinking(thinking)?;
            inner.set_thinking(thinking);
            Ok(())
        })
    }

    fn set_auto_compact_token_limit(&self, limit: Option<u64>) -> BackendFuture<Result<()>> {
        let inner = Arc::clone(&self.inner);
        Box::pin(async move {
            if inner.life() != OPEN {
                return Err(NanocodeError::AgentStopped);
            }
            inner.auto_compact.set(limit);
            Ok(())
        })
    }

    fn settings(
        &self,
    ) -> BackendFuture<Result<(ModelSelection, Thinking, nanocode_agent_core::ReasoningMode)>> {
        let inner = Arc::clone(&self.inner);
        Box::pin(async move {
            if inner.life() != OPEN {
                return Err(NanocodeError::AgentStopped);
            }
            Ok((
                inner.config.model.clone(),
                inner.thinking(),
                nanocode_agent_core::ReasoningMode::Standard,
            ))
        })
    }

    fn set_fast_mode(&self, _enabled: bool) -> BackendFuture<Result<()>> {
        done()
    }

    fn compact(&self) -> BackendFuture<Result<()>> {
        let inner = Arc::clone(&self.inner);
        Box::pin(async move { inner.queue_compaction().await })
    }

    fn side_question(&self, question: String) -> BackendFuture<Result<SideAnswer>> {
        let inner = Arc::clone(&self.inner);
        Box::pin(async move { inner.side_question(question).await })
    }

    fn transcript(&self) -> BackendFuture<Result<Vec<nanocode_agent_core::input::PromptMessage>>> {
        let settled = self.inner.settled.get();
        Box::pin(async move { Ok(crate::durability::prompt_messages(&settled.contents)) })
    }

    fn append_developer_message(&self, text: String) -> BackendFuture<Result<AgentSessionContext>> {
        let inner = Arc::clone(&self.inner);
        Box::pin(async move {
            inner.append_notice(text, None).await?;
            Ok(AgentSessionContext::from_backend(
                inner.config.workspace.clone(),
                Vec::new(),
            ))
        })
    }

    fn append_developer_notice(
        &self,
        text: String,
        prefix: Option<String>,
    ) -> BackendFuture<Result<bool>> {
        let inner = Arc::clone(&self.inner);
        Box::pin(async move { inner.append_notice(text, prefix).await })
    }

    fn context(&self) -> BackendFuture<Result<AgentSessionContext>> {
        unsupported("context")
    }

    fn checkpoint(&self) -> BackendFuture<Result<SessionCheckpoint>> {
        let checkpoint = self.inner.checkpoint();
        Box::pin(async move { Ok(checkpoint) })
    }

    fn spawn(&self, options: SpawnOptions) -> BackendFuture<Result<(Agent, AgentEvents)>> {
        let handle = self.inner.config.handle.clone();
        Box::pin(async move { handle.spawn_with(options).await })
    }

    fn fork(&self, request: ForkRequest) -> BackendFuture<Result<(Agent, AgentEvents)>> {
        let handle = self.inner.config.handle.clone();
        Box::pin(async move { handle.fork(request).await })
    }

    fn flush(&self) -> BackendFuture<Result<()>> {
        done()
    }

    fn shutdown(&self) -> BackendFuture<Result<()>> {
        let inner = Arc::clone(&self.inner);
        Box::pin(async move { inner.shutdown().await })
    }

    fn set_parking(&self, _parking: bool) {}

    fn parked(&self, _after: u64) -> BackendFuture<u64> {
        Box::pin(std::future::pending())
    }

    fn park(&self) -> BackendFuture<Result<Option<Arc<dyn ParkedCall>>>> {
        Box::pin(async { Ok(None) })
    }

    fn unpark(&self, _call: Arc<dyn ParkedCall>) {}

    fn is_stopped(&self) -> bool {
        self.inner.life() != OPEN
    }
}
