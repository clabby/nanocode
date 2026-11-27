mod delegation;

use std::{
    collections::VecDeque,
    ops::Range,
    process::ExitCode,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::Duration,
};

use nanocode::{
    Nanocode, PromptRequest, Thinking, Tools,
    antigravity::{AntigravityHarness, Auth, Endpoint, GoogleBuilder, Route},
    backend::Prompt,
    durability::SessionStore,
};
use nanocode_agent_types::sim::Mutation as Types;
use nanocode_backend_antigravity::{AntigravityFold, sim::Mutation as Production};
use nanocode_durability_core::{Digest, Fact, FoldError, Folded, KeyValueStore};
use nanocode_sync::{Atom, Signal};
use serde_json::{Value, json};

use crate::{
    disk::Disk,
    executor::{Clock, Ended},
    faults::{Durability, Profile},
    rng::{FNV_OFFSET, Rng, fnv},
    steercarry::{epoch, event_stream},
    store::{Mutation as StoreMutation, SimKv},
    turnlog::SimHost,
};

const SESSION: &str = "0190f5a2-7c1e-7000-8000-0000000a6d01";
const MODEL: &str = "gemini-4-neon";
const TURN: &str = "antigravity-turn";
const PROMPT: &str = "PROMPTMARK run the three tools";
const PROMPT_MARK: &str = "PROMPTMARK";
const PROMPT_REVISION: u64 = 1;
const STEER_REVISION: u64 = PROMPT_REVISION + 1;
const HOLD: &str = "hold";
const QUICK: [&str; 2] = ["quick_first", "quick_second"];
const CALLS: usize = 3;
const DONE: &str = "done";
const REFUSED: &str = "refused";
const POLL: Duration = Duration::from_millis(5);
const EPOCHS_MAX: u64 = 4;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AntigravityMutation {
    None,
    MovedForgetsResults,
    ReplayReruns,
    FactSyncSkipped,
    RevisionMissing,
    HostContextMissing,
    SteerRevisionIgnored,
    RevisionForgotten,
    OriginMissing,
    CompactionOriginMissing,
    XhighAsHigh,
    HarnessDefaultsHigh,
    Gemini4OffersNone,
    Gemini4DefaultsHigh,
}

impl AntigravityMutation {
    const ALL: [Self; 14] = [
        Self::None,
        Self::MovedForgetsResults,
        Self::ReplayReruns,
        Self::FactSyncSkipped,
        Self::RevisionMissing,
        Self::HostContextMissing,
        Self::SteerRevisionIgnored,
        Self::RevisionForgotten,
        Self::OriginMissing,
        Self::CompactionOriginMissing,
        Self::XhighAsHigh,
        Self::HarnessDefaultsHigh,
        Self::Gemini4OffersNone,
        Self::Gemini4DefaultsHigh,
    ];

    pub fn parse(name: &str) -> Option<Self> {
        Self::ALL
            .into_iter()
            .find(|mutation| mutation.name() == name)
    }

    pub const fn name(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::MovedForgetsResults => "antigravity_moved_forgets_results",
            Self::ReplayReruns => "antigravity_replay_reruns",
            Self::FactSyncSkipped => "antigravity_fact_sync_skipped",
            Self::RevisionMissing => "antigravity_revision_missing",
            Self::HostContextMissing => "antigravity_host_context_missing",
            Self::SteerRevisionIgnored => "antigravity_steer_revision_ignored",
            Self::RevisionForgotten => "antigravity_revision_forgotten",
            Self::OriginMissing => "antigravity_origin_missing",
            Self::CompactionOriginMissing => "antigravity_compaction_origin_missing",
            Self::XhighAsHigh => "antigravity_xhigh_as_high",
            Self::HarnessDefaultsHigh => "antigravity_harness_defaults_high",
            Self::Gemini4OffersNone => "antigravity_gemini4_offers_none",
            Self::Gemini4DefaultsHigh => "antigravity_gemini4_defaults_high",
        }
    }

    const fn production(self) -> Production {
        match self {
            Self::None => Production::None,
            Self::MovedForgetsResults => Production::MovedForgetsResults,
            Self::ReplayReruns => Production::ReplayReruns,
            Self::FactSyncSkipped => Production::FactSyncSkipped,
            Self::RevisionMissing => Production::RevisionMissing,
            Self::HostContextMissing => Production::HostContextMissing,
            Self::SteerRevisionIgnored => Production::SteerRevisionIgnored,
            Self::RevisionForgotten => Production::RevisionForgotten,
            Self::OriginMissing => Production::OriginMissing,
            Self::CompactionOriginMissing => Production::CompactionOriginMissing,
            Self::XhighAsHigh => Production::XhighAsHigh,
            Self::HarnessDefaultsHigh => Production::HarnessDefaultsHigh,
            Self::Gemini4OffersNone | Self::Gemini4DefaultsHigh => Production::None,
        }
    }

    const fn types(self) -> Types {
        match self {
            Self::Gemini4OffersNone => Types::Gemini4OffersNone,
            Self::Gemini4DefaultsHigh => Types::Gemini4DefaultsHigh,
            Self::None
            | Self::MovedForgetsResults
            | Self::ReplayReruns
            | Self::FactSyncSkipped
            | Self::RevisionMissing
            | Self::HostContextMissing
            | Self::SteerRevisionIgnored
            | Self::RevisionForgotten
            | Self::OriginMissing
            | Self::CompactionOriginMissing
            | Self::XhighAsHigh
            | Self::HarnessDefaultsHigh => Types::None,
        }
    }

    pub fn names() -> String {
        Self::ALL
            .iter()
            .skip(1)
            .map(|mutation| mutation.name())
            .collect::<Vec<_>>()
            .join("|")
    }
}

#[derive(Clone, Copy, Debug, Default)]
pub struct AntigravityStats {
    pub seeds: u64,
    pub cases: u64,
    pub epochs: u64,
    pub crashes: u64,
    pub model_calls: u64,
    pub fact_requests: u64,
    pub thinking_levels: u64,
}

impl AntigravityStats {
    pub const fn add(&mut self, other: &Self) {
        self.seeds += other.seeds;
        self.cases += other.cases;
        self.epochs += other.epochs;
        self.crashes += other.crashes;
        self.model_calls += other.model_calls;
        self.fact_requests += other.fact_requests;
        self.thinking_levels += other.thinking_levels;
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Store {
    Kv,
    Log,
}

#[derive(Clone, Copy, Debug)]
struct Case {
    store: Store,
    crash: bool,
    hold: usize,
}

#[derive(Default)]
struct Wire {
    bodies: Atom<Vec<String>>,
    wrong: Atom<Vec<String>>,
    waiting_for_steer: Signal,
    steered: Signal,
    script: Atom<VecDeque<bool>>,
    calls: AtomicU64,
    hold: AtomicU64,
    hang: AtomicBool,
    recovered: AtomicU64,
    runs: Atom<Vec<String>>,
}

impl Wire {
    fn check_tool_context(&self, name: &str, context: &nanocode::tools::ToolContext<'_>) {
        if context.instruction_revision() != Some(STEER_REVISION) {
            self.wrong.update(|wrong| {
                wrong.push(format!(
                    "tool {name} saw instruction revision {:?}, expected {STEER_REVISION}",
                    context.instruction_revision()
                ));
            });
        }
        if context.host_context() != Some(TURN) {
            self.wrong.update(|wrong| {
                wrong.push(format!(
                    "tool {name} saw host context {:?}, expected {TURN:?}",
                    context.host_context()
                ));
            });
        }
    }

    fn names(&self) -> Vec<&'static str> {
        let hold = usize::try_from(self.hold.load(Ordering::Acquire)).expect("hold fits usize");
        let mut quick = QUICK.iter();
        (0..CALLS)
            .map(|position| match position == hold {
                true => HOLD,
                false => quick.next().expect("two quick calls around the hold"),
            })
            .collect()
    }

    fn ran(&self, name: &str) -> usize {
        self.runs
            .read(|runs| runs.iter().filter(|run| *run == name).count())
    }
}

struct Sender(Arc<Wire>);

impl nanocode_http::layer::Sender for Sender {
    fn send(
        &self,
        request: nanocode_http::layer::Request,
    ) -> nanocode_http::layer::BoxFuture<
        '_,
        Result<nanocode_http::layer::Response, nanocode_http::layer::Error>,
    > {
        Box::pin(async move {
            let wire = &self.0;
            let call = wire.calls.fetch_add(1, Ordering::AcqRel);
            if let Err(error) = check_origin(SESSION, Some(TURN)) {
                wire.wrong.update(|wrong| wrong.push(error.clone()));
            }
            wire.bodies
                .update(|bodies| bodies.push(String::from_utf8_lossy(&request.body).into_owned()));
            if call == 0 {
                return Ok(model_response(
                    vec![
                        json!({"functionCall": {"name": "manage_task", "args": {"Action": "list"}}}),
                    ],
                    true,
                ));
            }
            if !wire.steered.is_fired() {
                wire.waiting_for_steer.fire();
                wire.steered.wait().await;
                return Ok(model_response(
                    vec![json!({"text": "Awaiting instructions"})],
                    true,
                ));
            }
            let tools = wire.script.update(VecDeque::pop_front).unwrap_or(false);
            let parts = match tools {
                true => wire
                    .names()
                    .into_iter()
                    .map(|name| json!({"functionCall": {"name": name, "args": {}}}))
                    .collect::<Vec<_>>(),
                false => vec![json!({"text": DONE})],
            };
            Ok(model_response(parts, true))
        })
    }
}

fn model_response(parts: Vec<Value>, streaming: bool) -> nanocode_http::layer::Response {
    let chunk = json!({
        "candidates": [{"content": {"role": "model", "parts": parts}, "finishReason": "STOP"}],
        "usageMetadata": {"promptTokenCount": 1, "candidatesTokenCount": 1, "totalTokenCount": 2},
        "modelVersion": MODEL,
    });
    let (body, headers) = match streaming {
        true => (format!("data: {chunk}\n\n"), event_stream()),
        false => (chunk.to_string(), nanocode_http::types::HeaderMap::new()),
    };
    nanocode_http::layer::Response::new(
        nanocode_http::types::StatusCode::OK,
        headers,
        Box::pin(nanocode_sync::stream::once(
            async move { Ok(body.into_bytes()) },
        )),
    )
}

fn check_origin(expected_session: &str, expected_context: Option<&str>) -> Result<(), String> {
    use nanocode_http::layer::origin;
    let session = origin::session();
    let context = origin::current();
    if session.as_deref() != Some(expected_session) || context.as_deref() != expected_context {
        return Err(format!(
            "HTTP origin named session {session:?}, context {context:?}; expected {expected_session:?}, {expected_context:?}"
        ));
    }
    Ok(())
}

struct Tool(Arc<Wire>);

impl nanocode::FunctionHandler for Tool {
    fn call<'a>(
        &'a self,
        name: &'a str,
        _input: Value,
        context: nanocode::tools::ToolContext<'a>,
    ) -> nanocode::HostFuture<'a, Result<Value, String>> {
        Box::pin(async move {
            self.0.check_tool_context(name, &context);
            self.0.runs.update(|runs| runs.push(name.to_owned()));
            if name == HOLD && self.0.hang.swap(false, Ordering::AcqRel) {
                loop {
                    nanocode_platform::sleep(POLL).await;
                }
            }
            Ok(Value::String(format!("{name} output")))
        })
    }
}

struct Recovery(Arc<Wire>);

impl nanocode::ToolRecovery for Recovery {
    fn recover<'a>(
        &'a self,
        name: &'a str,
        context: nanocode::tools::ToolContext<'a>,
    ) -> nanocode::HostFuture<'a, Option<nanocode_tool_runtime::ToolOutput>> {
        Box::pin(async move {
            self.0.recovered.fetch_add(1, Ordering::AcqRel);
            self.0.check_tool_context(name, &context);
            None
        })
    }
}

struct NoShell;

impl nanocode::process::ShellExecutor for NoShell {
    fn execute<'a>(
        &'a self,
        _command: nanocode::process::ExecCommand,
        _workspace: &'a std::path::Path,
    ) -> nanocode::process::ProcessFuture<'a, nanocode::process::ExecCommandResult> {
        Box::pin(async { refused() })
    }

    fn write_stdin(
        &self,
        _request: nanocode::process::WriteStdin,
    ) -> nanocode::process::ProcessFuture<'_, nanocode::process::ExecCommandResult> {
        Box::pin(async { refused() })
    }

    fn terminate_all(&self) -> nanocode::process::ProcessFuture<'_, ()> {
        Box::pin(async {})
    }
}

fn refused() -> nanocode::process::ExecCommandResult {
    nanocode::process::ExecCommandResult {
        error: Some("the simulated session runs no commands".to_owned()),
        chunk_id: None,
        wall_time_seconds: 0.0,
        exit_code: None,
        session_id: None,
        original_token_count: None,
        output: String::new(),
        time_left_seconds: None,
    }
}

struct NoFiles;

impl nanocode::process::FileHost for NoFiles {
    fn read<'a>(
        &'a self,
        _path: &'a std::path::Path,
    ) -> nanocode::process::ProcessFuture<'a, Result<Option<String>, String>> {
        Box::pin(async { Ok(None) })
    }

    fn write<'a>(
        &'a self,
        _path: &'a std::path::Path,
        _contents: &'a str,
    ) -> nanocode::process::ProcessFuture<'a, Result<(), String>> {
        Box::pin(async { Ok(()) })
    }

    fn remove<'a>(
        &'a self,
        _path: &'a std::path::Path,
    ) -> nanocode::process::ProcessFuture<'a, Result<(), String>> {
        Box::pin(async { Ok(()) })
    }

    fn read_bytes<'a>(
        &'a self,
        _path: &'a std::path::Path,
    ) -> nanocode::process::ProcessFuture<'a, Result<Option<Vec<u8>>, String>> {
        Box::pin(async { Ok(None) })
    }

    fn write_bytes<'a>(
        &'a self,
        _path: &'a std::path::Path,
        _bytes: &'a [u8],
    ) -> nanocode::process::ProcessFuture<'a, Result<(), String>> {
        Box::pin(async { Ok(()) })
    }
}

#[derive(Clone, Copy)]
struct Gemini;

impl nanocode::BackendKind for Gemini {
    type Model = &'static str;
    fn backend_id() -> nanocode::BackendId {
        nanocode::BackendId::new(nanocode::antigravity::FAMILY)
    }
    fn model_id(model: &Self::Model) -> &str {
        model
    }
}

fn tools(wire: &Arc<Wire>, gate: Option<Arc<dyn nanocode::turnlog::EffectGate>>) -> Tools {
    let mut builder = Tools::builder().without_defaults();
    for name in [HOLD, QUICK[0], QUICK[1]] {
        builder = builder.tool(nanocode::FunctionTool::new(
            name,
            "Run a simulated step",
            json!({"type": "object", "properties": {}}),
            Arc::new(Tool(Arc::clone(wire))),
        ));
    }
    let builder = builder
        .exposure(nanocode_tool_provider::ToolExposure::DirectOnly)
        .tool_recovery(Arc::new(Recovery(Arc::clone(wire))));
    match gate {
        Some(gate) => builder.effect_gate(gate),
        None => builder,
    }
    .build()
    .expect("the antigravity sim tools build")
}

fn harness(sender: Arc<dyn nanocode_http::layer::Sender>) -> AntigravityHarness {
    AntigravityHarness::new()
        .endpoint(Endpoint {
            base_url: "http://model.sim".to_owned(),
            auth: Auth::ApiKey("sim".to_owned()),
            user_agent: "sim".to_owned(),
            route: Route::GeminiApi,
            notice: None,
        })
        .sender(sender)
        .shell(Arc::new(NoShell))
        .files(Arc::new(NoFiles))
        .app_data(std::env::temp_dir().join("nanocode-sim-antigravity"))
}

fn runtime(
    sender: Arc<dyn nanocode_http::layer::Sender>,
    store: SessionStore,
    tools: Tools,
) -> Result<Nanocode, String> {
    Nanocode::builder()
        .tools(tools)
        .runner_management(false)
        .thinking(Thinking::Low)
        .workspace(std::env::temp_dir())
        .durability(store)
        .google(harness(sender))
        .for_backend::<Gemini>()
        .model(MODEL)
        .build()
        .map_err(|error| format!("runtime build: {error}"))
}

async fn level_sent(
    kv: SimKv,
    model: &'static str,
    thinking: Option<Thinking>,
) -> Result<String, String> {
    let wire = Arc::new(Wire::default());
    let store = nanocode::session_store(KeyValueStore::new(kv))
        .map_err(|error| format!("session store: {error}"))?;
    let runtime = Nanocode::builder()
        .tools(tools(&wire, None))
        .runner_management(false)
        .workspace(std::env::temp_dir())
        .durability(store)
        .google(harness(Arc::new(Sender(Arc::clone(&wire)))).model(model))
        .for_backend::<Gemini>()
        .model(model)
        .build()
        .map_err(|error| format!("runtime build: {error}"))?;
    let spawn = runtime.spawn();
    let spawn = match thinking {
        Some(thinking) => spawn.thinking(thinking),
        None => spawn,
    };
    let (agent, events) = match spawn.build().await {
        Ok(built) => built,
        Err(error) if error.to_string().contains("does not support") => {
            return Ok(REFUSED.to_owned());
        }
        Err(error) => return Err(format!("spawn: {error}")),
    };
    drop(events);
    agent
        .prompt(PromptRequest::new(PROMPT))
        .await
        .map_err(|error| format!("admit: {error}"))?
        .result()
        .await
        .map_err(|error| format!("turn: {error}"))?;
    let bodies = wire.bodies.get();
    let body = bodies.last().ok_or("no model request was made")?;
    serde_json::from_str::<Value>(body)
        .map_err(|error| format!("request unreadable: {error}"))?["generationConfig"]
        ["thinkingConfig"]["thinkingLevel"]
        .as_str()
        .map(str::to_owned)
        .ok_or_else(|| "the request carries no thinkingLevel".to_owned())
}

fn check_levels(seed: u64, stats: &mut AntigravityStats, digest: &mut u64) -> Vec<String> {
    let clock = Clock::new();
    crate::host::enter(seed, &clock);
    let mut wrong = Vec::new();
    for (model, thinking, expected) in [
        ("gemini-4-neon", Some(Thinking::Xhigh), "XHIGH"),
        ("gemini-4-carbon", None, "MEDIUM"),
        ("gemini-4-carbon", Some(Thinking::None), REFUSED),
    ] {
        let asked = thinking.map_or("default", Thinking::as_str);
        let label = format!("antigravity_durable level {model} {asked}");
        let kv = media(seed, &clock).kv;
        let sent = match epoch(seed, &clock, &label, level_sent(kv, model, thinking)) {
            (Ended::Finished, Some(Ok(sent))) => sent,
            (Ended::Finished, Some(Err(error))) => error,
            (ended, _) => format!("ended {ended:?} without a result"),
        };
        stats.thinking_levels += 1;
        *digest = fnv(*digest, format!("{label} {sent}").as_bytes());
        if sent != expected {
            wrong.push(format!(
                "a {model} turn at {asked} thinking ended with {sent:?}, not {expected:?}"
            ));
        }
    }
    wrong
}

struct Media {
    kv: SimKv,
    disk: Arc<Disk>,
}

impl Media {
    async fn open(
        &self,
        store: Store,
    ) -> Result<(SessionStore, Option<nanocode::turnlog::Parts>), String> {
        match store {
            Store::Kv => Ok((
                nanocode::session_store(KeyValueStore::new(self.kv.clone()))
                    .map_err(|error| format!("session store: {error}"))?,
                None,
            )),
            Store::Log => {
                let host: Arc<dyn nanocode::turnlog::LogHost> =
                    Arc::new(SimHost::new(Arc::clone(&self.disk)));
                let policy = nanocode::turnlog::Policy {
                    chain_bytes_max: None,
                    effects: nanocode::turnlog::Effects::default(),
                    host_call_bound: None,
                    max_row_bytes: None,
                    max_tail_bytes: None,
                    restarts: u64::MAX,
                    retain_finished: None,
                    retired_max: None,
                    snapshot_ratio: 2,
                    snapshot_rows: None,
                    wakes_max: None,
                };
                let parts = nanocode::turnlog(host, "sim", policy)
                    .await
                    .map_err(|error| format!("turn log: {error}"))?;
                parts.log.settle_durably();
                Ok((parts.store.clone(), Some(parts)))
            }
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Stage {
    Fresh,
    Running,
    Replay,
    Done,
}

#[derive(Default)]
struct Run {
    wrong: Vec<String>,
    epochs: u64,
    crashes: u64,
    fact_requests: u64,
}

async fn epoch_work(
    case: &Case,
    wire: &Arc<Wire>,
    media: &Media,
    stage: &Atom<Stage>,
) -> Result<(), String> {
    let (store, parts) = media.open(case.store).await?;
    let gate = parts.as_ref().map(|parts| Arc::clone(&parts.gate));
    let runtime = runtime(Arc::new(Sender(Arc::clone(wire))), store, tools(wire, gate))?;
    let fresh = *stage.get() == Stage::Fresh;
    let (agent, events) = match fresh {
        true => runtime.spawn().session_id(SESSION).build().await,
        false => runtime.open_session(SESSION).await,
    }
    .map_err(|error| format!("open: {error}"))?;
    drop(events);
    let prompt = || Prompt::new(PROMPT).with_instruction_revision(PROMPT_REVISION);
    if *stage.get() != Stage::Replay {
        let request = match fresh {
            true => PromptRequest::new(prompt()),
            false => PromptRequest::new(
                runtime
                    .durable_prompt(&agent, TURN)
                    .await
                    .map_err(|error| format!("durable prompt: {error}"))?
                    .ok_or("the moved turn's prompt is not stored")?,
            ),
        };
        stage.set(Stage::Running);
        let turn = agent
            .prompt(request.request_id(TURN))
            .await
            .map_err(|error| format!("admit: {error}"))?;
        if fresh {
            wire.waiting_for_steer.wait().await;
            turn.control()
                .steer_with_id(
                    "revision-steer".to_owned(),
                    Prompt::new("Use the revised instruction")
                        .with_instruction_revision(STEER_REVISION),
                )
                .await
                .map_err(|error| format!("steer: {error}"))?;
            wire.steered.fire();
        }
        let done = turn
            .result()
            .await
            .map_err(|error| format!("turn: {error}"))?;
        if done.final_message != DONE {
            return Err(format!("the turn ended with {:?}", done.final_message));
        }
        stage.set(Stage::Replay);
    }
    let before = wire.calls.load(Ordering::Acquire);
    let replayed = agent
        .prompt(PromptRequest::new(prompt()).request_id(TURN))
        .await
        .map_err(|error| format!("replay admit: {error}"))?
        .result()
        .await
        .map_err(|error| format!("replay: {error}"))?;
    if replayed.final_message != DONE {
        return Err(format!("the replay answered {:?}", replayed.final_message));
    }
    if wire.calls.load(Ordering::Acquire) != before {
        return Err("replaying a finished turn called the model again".to_owned());
    }
    stage.set(Stage::Done);
    Ok(())
}

async fn refolds(log: &Arc<nanocode::turnlog::OneLog>, bodies: &[String]) -> Result<u64, String> {
    let store = log.store();
    let logs = nanocode_durability_core::StateStore::facts(&store)
        .ok_or("antigravity_facts: a log store serves no fact logs")?;
    let replay = logs
        .scoped(SESSION, true)
        .replay()
        .await
        .map_err(|error| format!("antigravity_facts_unreadable: {error}"))?;
    let blobs = &replay.blobs;
    let lookup = |digest: &Digest| {
        blobs
            .get(digest)
            .map(|json| &**json)
            .ok_or(FoldError::MissingBlob { digest: *digest })
    };
    let sent = bodies
        .iter()
        .map(|body| {
            serde_json::from_str::<Value>(body)
                .map(|body| body["contents"].clone())
                .map_err(|error| format!("antigravity_facts_request_unreadable: {error}"))
        })
        .collect::<Result<Vec<_>, _>>()?;
    let facts = &replay.facts;
    let mut next = 0;
    let mut checked = 0;
    for at in 0..facts.len() {
        if !matches!(facts[at].1, Fact::ModelOut(_)) {
            continue;
        }
        let (folded, _) = Folded::<AntigravityFold>::open(None, facts[..at].to_vec(), lookup)
            .map_err(|error| {
                format!(
                    "antigravity_facts_refold_failed before fact {}: {error}",
                    facts[at].0
                )
            })?;
        let request = folded
            .request()
            .ok()
            .and_then(|request| serde_json::from_slice::<Value>(&request).ok())
            .ok_or("antigravity_facts_request_unencodable")?;
        let Some(position) = sent[next..]
            .iter()
            .position(|contents| *contents == request)
        else {
            return Err(format!(
                "antigravity_facts_request_differs: the fold before fact {} builds contents no model call received",
                facts[at].0
            ));
        };
        next += position + 1;
        checked += 1;
    }
    Ok(checked)
}

fn media(seed: u64, clock: &Arc<Clock>) -> Media {
    let mut profile = Profile::of(seed);
    profile.before_ppm = 0;
    profile.after_ppm = 0;
    profile.torn_ppm = 0;
    profile.read_ppm = 0;
    profile.durability = Durability::OnAck;
    Media {
        kv: SimKv::new(seed, profile, StoreMutation::None, false, Arc::clone(clock)),
        disk: Disk::new(),
    }
}

fn simulate_case(seed: u64, case: &Case) -> (Run, u64) {
    let clock = Clock::new();
    crate::host::enter(seed, &clock);
    let media = media(seed, &clock);
    let wire = Arc::new(Wire::default());
    wire.script.set([true, false].into_iter().collect());
    wire.hold.store(
        u64::try_from(case.hold).expect("hold fits u64"),
        Ordering::Release,
    );
    wire.hang.store(case.crash, Ordering::Release);
    let stage = Atom::new(Stage::Fresh);
    let mut run = Run::default();
    let label = format!("antigravity_durable {case:?}");
    for number in 0..EPOCHS_MAX {
        run.epochs += 1;
        let (ended, result) = epoch(
            seed,
            &clock,
            &format!("{label} {number}"),
            epoch_work(case, &wire, &media, &stage),
        );
        match (ended, result) {
            (Ended::Finished, Some(Ok(()))) => break,
            (Ended::Crashed | Ended::Stuck, _) => run.crashes += 1,
            (Ended::Finished, Some(Err(error))) => {
                run.wrong.push(format!("epoch {number}: {error}"));
                break;
            }
            (Ended::Finished, None) => {
                run.wrong
                    .push(format!("epoch {number} ended without a result"));
                break;
            }
        }
    }
    let calls = wire.calls.load(Ordering::Acquire);
    if !run.wrong.is_empty() {
        return (run, calls);
    }
    if *stage.get() != Stage::Done {
        run.wrong.push(format!(
            "the turn stopped at {:?} after {EPOCHS_MAX} epochs",
            *stage.get()
        ));
        return (run, calls);
    }
    check_runs(case, &wire, &mut run);
    if case.store == Store::Log {
        let bodies = wire.bodies.get();
        let checked = epoch(seed, &clock, &format!("{label} facts"), async {
            let (_, parts) = media.open(Store::Log).await?;
            let parts = parts.ok_or("a log store opened without its turn log")?;
            refolds(&parts.log, &bodies).await
        });
        match checked {
            (Ended::Finished, Some(Ok(checked))) => run.fact_requests += checked,
            (Ended::Finished, Some(Err(error))) => run.wrong.push(error),
            (ended, _) => run
                .wrong
                .push(format!("the fact check ended {ended:?} without a result")),
        }
    }
    (run, calls)
}

fn check_runs(case: &Case, wire: &Wire, run: &mut Run) {
    run.wrong.extend(wire.wrong.get().iter().cloned());
    for name in [HOLD, QUICK[0], QUICK[1]] {
        let ran = wire.ran(name);
        if ran != 1 {
            run.wrong.push(format!("{name} ran {ran} times, not once"));
        }
    }
    if case.crash && wire.recovered.load(Ordering::Acquire) == 0 {
        run.wrong
            .push("no interrupted tool call was offered for recovery".to_owned());
    }
    let bodies = wire.bodies.get();
    if let Some(body) = bodies
        .iter()
        .find(|body| body.matches(PROMPT_MARK).count() != 1)
    {
        run.wrong.push(format!(
            "a model request held the prompt {} times",
            body.matches(PROMPT_MARK).count()
        ));
    }
    let Some(last) = bodies.last() else {
        run.wrong.push("no model request was made".to_owned());
        return;
    };
    for name in QUICK {
        let output = format!("{name} output");
        if last.matches(&output).count() != 1 {
            run.wrong.push(format!(
                "the answered request does not hold {name}'s output once"
            ));
        }
    }
    let held = last.matches(&format!("{HOLD} output")).count();
    match case.crash {
        true if held != 0 => run
            .wrong
            .push("the moved call's output reached the model though it never returned".to_owned()),
        true if !last.contains("nknown") => run
            .wrong
            .push("the moved call was not answered as an unknown outcome".to_owned()),
        false if held != 1 => run.wrong.push(format!(
            "the held call's output reached the model {held} times"
        )),
        _ => {}
    }
}

struct Outcome {
    digest: u64,
    stats: AntigravityStats,
    wrong: Option<String>,
}

fn simulate(seed: u64, mutation: AntigravityMutation) -> Outcome {
    nanocode_backend_antigravity::sim::mutate(mutation.production());
    nanocode_agent_types::sim::mutate(mutation.types());
    let mut rng = Rng::derive(seed, "antigravity_durable cases");
    let mut stats = AntigravityStats {
        seeds: 1,
        ..AntigravityStats::default()
    };
    let mut digest = FNV_OFFSET;
    let mut wrong = Vec::new();
    for store in [Store::Kv, Store::Log] {
        for crash in [false, true] {
            let hold = usize::try_from(rng.below(u64::try_from(CALLS).expect("calls fit u64")))
                .expect("a call position fits usize");
            let case = Case { store, crash, hold };
            let (run, calls) = simulate_case(seed, &case);
            stats.cases += 1;
            stats.epochs += run.epochs;
            stats.crashes += run.crashes;
            stats.model_calls += calls;
            stats.fact_requests += run.fact_requests;
            digest = fnv(
                digest,
                format!(
                    "{case:?} {} {} {calls} {}",
                    run.epochs, run.crashes, run.fact_requests
                )
                .as_bytes(),
            );
            if !run.wrong.is_empty() {
                wrong.push(format!("{case:?}: {}", run.wrong.join("; ")));
            }
        }
    }
    let (calls, errors) = delegation::simulate(seed);
    stats.cases += 1;
    stats.epochs += 1;
    stats.model_calls += calls;
    digest = fnv(digest, &calls.to_le_bytes());
    wrong.extend(errors);
    wrong.extend(check_levels(seed, &mut stats, &mut digest));
    nanocode_backend_antigravity::sim::mutate(Production::None);
    nanocode_agent_types::sim::mutate(Types::None);
    Outcome {
        digest,
        stats,
        wrong: (!wrong.is_empty()).then(|| wrong.join(" | ")),
    }
}

pub fn judge(
    seed: u64,
    mutation: AntigravityMutation,
    twice: bool,
    verbose: bool,
    totals: &mut AntigravityStats,
) -> Option<String> {
    let first = simulate(seed, mutation);
    totals.add(&first.stats);
    if verbose {
        println!("seed {seed} digest {:016x} {:?}", first.digest, first.stats);
    }
    if first.wrong.is_some() || !twice {
        return first.wrong;
    }
    let second = simulate(seed, mutation);
    (second.digest != first.digest || second.wrong.is_some()).then(|| {
        format!(
            "nondeterminism: digest {:016x} then {:016x}",
            first.digest, second.digest
        )
    })
}

pub fn replay(seed: u64, mutation: AntigravityMutation) -> String {
    let mut command = format!("nanocode-sim --scenario antigravity_durable --seed {seed}");
    if mutation != AntigravityMutation::None {
        command.push_str(" --mutation ");
        command.push_str(mutation.name());
    }
    command
}

pub fn report(
    seeds: &Range<u64>,
    passed: u64,
    failed: u64,
    twice: bool,
    mutation: AntigravityMutation,
    totals: &AntigravityStats,
) -> ExitCode {
    println!(
        "RESULT seeds={}..{} scenario=antigravity_durable mutation={} passed={passed} failed={failed} twice={twice} stats={totals:?}",
        seeds.start,
        seeds.end,
        mutation.name()
    );
    match mutation {
        AntigravityMutation::None if failed == 0 => ExitCode::SUCCESS,
        AntigravityMutation::None => ExitCode::FAILURE,
        _ if failed > 0 => {
            println!("MUTATION {} caught by {failed} seeds", mutation.name());
            ExitCode::SUCCESS
        }
        _ => {
            println!("MUTATION {} NOT caught", mutation.name());
            ExitCode::FAILURE
        }
    }
}
