use std::{
    future::Future,
    ops::Range,
    path::PathBuf,
    pin::pin,
    process::ExitCode,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use nanocode::{
    ClaudeClient, HarnessFamily, HostFuture, ModelSelection, Nanocode, Thinking, Tools,
    backend::Prompt,
    claude::transport::{HttpError, HttpFuture, HttpRequest, HttpResponse, HttpTransport},
    durability::SessionRecord,
    session_host::{
        Delivery, Driven, InboxHost, LoggedEvent, Next, PassEnd, Submission, TurnInbox, TurnRecord,
        TurnState, inbox_sim,
    },
    turnlog::OneLog,
};
use nanocode_subagents::sim::Mutation as SubagentMutation;
use nanocode_sync::Atom;
use serde_json::{Value, json};

use crate::{
    disk::Disk,
    executor::{Clock, Ended, Executor, Task},
    rng::{FNV_OFFSET, Rng, fnv},
    server::text_of,
    turnlog::SimHost,
};

const SESSION: &str = "request-close";
const MODEL: &str = "claude-haiku-5-5";
const REQUEST: &str = "req";
const PROMPT: &str = "ROOTREQ delegate two lookups and answer";
const NEXT_REQUEST: &str = "next";
const NEXT_PROMPT: &str = "NEXTREQ continue";
const CHILD_A: &str = "CHILDA";
const CHILD_B: &str = "CHILDB";
const RESULT_A: &str = "found-alpha";
const RESULT_B: &str = "found-bravo";
const COMPLETION_MARK: &str = "<subagent_completion";
const RUNNING_MARK: &str = "Script running with cell ID ";
const COMPLETION_PREFIX: &str = "subagent-completion:";
const EPOCHS_MAX: u64 = 6;
const CRASH_AFTER: Duration = Duration::from_secs(600);
const DRIVES_MAX: u64 = 400;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RequestCloseMutation {
    None,
    OwnedRequestHeld,
    OwnedCancelCloses,
    CancelHoldKept,
}

impl RequestCloseMutation {
    const ALL: [Self; 3] = [
        Self::OwnedRequestHeld,
        Self::OwnedCancelCloses,
        Self::CancelHoldKept,
    ];

    pub const fn name(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::OwnedRequestHeld => "owned_request_held",
            Self::OwnedCancelCloses => "owned_cancel_closes",
            Self::CancelHoldKept => SubagentMutation::CancelHoldKept.name(),
        }
    }

    pub fn parse(name: &str) -> Option<Self> {
        std::iter::once(Self::None)
            .chain(Self::ALL)
            .find(|mutation| mutation.name() == name)
    }

    fn apply(self) {
        inbox_sim::mutate(match self {
            Self::OwnedRequestHeld => inbox_sim::Mutation::OwnedRequestHeld,
            Self::OwnedCancelCloses => inbox_sim::Mutation::OwnedCancelCloses,
            Self::None | Self::CancelHoldKept => inbox_sim::Mutation::None,
        });
        nanocode_subagents::sim::mutate(if self == Self::CancelHoldKept {
            SubagentMutation::CancelHoldKept
        } else {
            SubagentMutation::None
        });
    }
}

pub fn names() -> String {
    RequestCloseMutation::ALL
        .iter()
        .map(|mutation| mutation.name())
        .collect::<Vec<_>>()
        .join("|")
}

#[derive(Clone, Copy, Debug, Default)]
pub struct RequestCloseStats {
    pub seeds: u64,
    pub cases: u64,
    pub epochs: u64,
    pub crashes: u64,
    pub model_calls: u64,
}

impl RequestCloseStats {
    pub const fn add(&mut self, other: &Self) {
        self.seeds += other.seeds;
        self.cases += other.cases;
        self.epochs += other.epochs;
        self.crashes += other.crashes;
        self.model_calls += other.model_calls;
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Case {
    NoCrash,
    CrashWhileAwaitingChild,
    CrashInContinuation,
    CrashWhileClosing,
    CancelWhileAwaitingChild,
    HostOwned,
    HostOwnedCancel,
    HostOwnedCancelLate,
}

const CASES: [Case; 8] = [
    Case::NoCrash,
    Case::CrashWhileAwaitingChild,
    Case::CrashInContinuation,
    Case::CrashWhileClosing,
    Case::CancelWhileAwaitingChild,
    Case::HostOwned,
    Case::HostOwnedCancel,
    Case::HostOwnedCancelLate,
];

#[derive(Default)]
struct Wire {
    root_calls: Atom<Vec<String>>,
    calls: Atom<u64>,
    released: AtomicBool,
    held: AtomicBool,
    late: AtomicBool,
    hang: Atom<Option<(String, String)>>,
}

struct WireTransport(Arc<Wire>);

fn messages(body: &str) -> Vec<Value> {
    serde_json::from_str::<Value>(body)
        .ok()
        .and_then(|value| value.get("messages").and_then(Value::as_array).cloned())
        .unwrap_or_default()
}

fn content(message: &Value) -> String {
    text_of(message.get("content").unwrap_or(&Value::Null))
}

fn exec(code: &str) -> (Value, Option<Value>, &'static str) {
    tool("exec", &json!({ "code": code }))
}

fn tool(name: &str, input: &Value) -> (Value, Option<Value>, &'static str) {
    (
        json!({"type": "tool_use", "id": format!("toolu_{}", nanocode_platform::uuid_v7().simple()), "name": name, "input": {}}),
        Some(json!({"type": "input_json_delta", "partial_json": input.to_string()})),
        "tool_use",
    )
}

fn say(text: &str) -> (Value, Option<Value>, &'static str) {
    (
        json!({"type": "text", "text": ""}),
        Some(json!({"type": "text_delta", "text": text})),
        "end_turn",
    )
}

fn running_cell(last: &str) -> Option<String> {
    let at = last.find(RUNNING_MARK)? + RUNNING_MARK.len();
    last[at..]
        .split(|character: char| !character.is_ascii_alphanumeric())
        .next()
        .map(str::to_owned)
}

impl Wire {
    async fn reply(&self, body: &str) -> (Value, Option<Value>, &'static str) {
        let messages = messages(body);
        let first = messages.first().map(content).unwrap_or_default();
        let last = messages.last().map(content).unwrap_or_default();
        if self.late.load(Ordering::Acquire) && last.contains(NEXT_PROMPT) {
            self.released.store(true, Ordering::Release);
            nanocode_platform::sleep(Duration::from_secs(5)).await;
        }
        if let Some(cell) = running_cell(&last) {
            return tool("wait", &json!({"cell_id": cell, "yield_time_ms": 30_000}));
        }
        let child = [(CHILD_A, RESULT_A), (CHILD_B, RESULT_B)]
            .into_iter()
            .find(|(mark, _)| first.contains(mark));
        if let Some((mark, result)) = child {
            if messages.len() > 1 {
                return say("child finished");
            }
            if mark == CHILD_B {
                while !self.released.load(Ordering::Acquire) {
                    nanocode_platform::sleep(Duration::from_millis(5)).await;
                }
            }
            return exec(&format!(
                "const result = await tools.submit_result({{ output: \"{result}\" }});\ntext(JSON.stringify(result));"
            ));
        }
        self.root_calls.update(|calls| calls.push(last.clone()));
        let fresh = messages.len() == 1 || last.contains(PROMPT) && !last.contains("spawned");
        if fresh && !last.contains(COMPLETION_MARK) {
            return exec(&format!(
                "const ids = [];\nfor (const task of [\"{CHILD_A} lookup\", \"{CHILD_B} lookup\"]) {{ const spawned = await tools.spawn_agent({{ role: \"worker\", task, harness: null, model: null, thinking: null, reasoning_mode: null, output_contract: {{ kind: \"string\" }} }}); ids.push(spawned.agent_id); }}\ntext(\"spawned \" + ids.length);"
            ));
        }
        let history: String = messages.iter().map(content).collect();
        let seen: Vec<&str> = [RESULT_A, RESULT_B]
            .into_iter()
            .filter(|result| history.contains(result))
            .collect();
        let text = format!("answer using [{}]", seen.join(","));
        if !self.held.load(Ordering::Acquire) {
            self.released.store(true, Ordering::Release);
        }
        say(&text)
    }
}

impl HttpTransport for WireTransport {
    fn post(&self, request: HttpRequest) -> HttpFuture<'_, Result<HttpResponse, HttpError>> {
        Box::pin(async move {
            let body = request.body;
            let hang = self.0.hang.read(|hang| {
                hang.as_ref().is_some_and(|(first, last)| {
                    let messages = messages(&body);
                    messages
                        .first()
                        .map(content)
                        .is_some_and(|text| text.contains(first.as_str()))
                        && messages
                            .last()
                            .map(content)
                            .is_some_and(|text| text.contains(last.as_str()))
                })
            });
            if hang {
                self.0.hang.set(None);
                std::future::pending::<()>().await;
            }
            nanocode_platform::sleep(Duration::from_millis(20)).await;
            let (block, delta, stop) = self.0.reply(&body).await;
            self.0.calls.update(|calls| *calls += 1);
            let count = *self.0.calls.get();
            let sse = crate::server::events(MODEL, &format!("rc{count}"), &block, delta, stop);
            let mut headers = nanocode_http::types::HeaderMap::new();
            headers.insert(
                nanocode_http::types::header::CONTENT_TYPE,
                nanocode_http::types::HeaderValue::from_static("text/event-stream"),
            );
            headers.insert(
                "request-id",
                nanocode_http::types::HeaderValue::from_static("req_sim"),
            );
            Ok(HttpResponse::new(
                nanocode_http::types::StatusCode::OK,
                headers,
                Box::pin(nanocode_sync::stream::once(
                    async move { Ok(sse.into_bytes()) },
                )),
            ))
        })
    }
}

#[derive(Default)]
struct Inbox {
    hang_result: AtomicBool,
    owns: AtomicBool,
}

impl InboxHost for Inbox {
    fn owns_completion_turns(&self) -> bool {
        self.owns.load(Ordering::Acquire)
    }

    fn publish(&self, events: Vec<LoggedEvent>) -> HostFuture<'_, Result<(), String>> {
        Box::pin(async move {
            let closing = events.iter().any(|event| {
                event.request_id.as_deref() == Some(REQUEST)
                    && event.event.get().contains("\"type\":\"result\"")
            });
            if closing && self.hang_result.swap(false, Ordering::AcqRel) {
                std::future::pending::<()>().await;
            }
            Ok(())
        })
    }

    fn next<'a>(
        &'a self,
        _request_id: &'a str,
        _end: PassEnd,
        _delivery: &'a Delivery,
    ) -> HostFuture<'a, Next> {
        Box::pin(async { Next::Done })
    }
}

struct Process {
    inbox: TurnInbox<OneLog>,
    pump: HostFuture<'static, ()>,
    runtime: Nanocode,
}

async fn open(disk: &Arc<Disk>, wire: &Arc<Wire>, inbox: &Arc<Inbox>) -> Result<Process, String> {
    let log_host: Arc<dyn nanocode::turnlog::LogHost> = Arc::new(SimHost::new(Arc::clone(disk)));
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
    let parts = nanocode::turnlog(log_host, "sim", policy)
        .await
        .map_err(|error| format!("turn log: {error}"))?;
    parts.log.settle_durably();
    let claude = ClaudeClient::messages_api(
        "http://model.sim/v1",
        "sk-sim",
        Duration::from_secs(10),
        Duration::from_secs(120),
    )
    .map_err(|error| format!("claude client: {error}"))?
    .with_transport(WireTransport(Arc::clone(wire)));
    let tools = Tools::builder()
        .without_defaults()
        .effect_gate(Arc::clone(&parts.gate))
        .cell_journals(parts.journals.clone())
        .build()
        .map_err(|error| format!("tools: {error}"))?;
    let runtime = Nanocode::builder()
        .claude(nanocode::ClaudeHarness::new(claude))
        .tools(tools)
        .runner_management(false)
        .thinking(Thinking::Low)
        .workspace(PathBuf::from("/nonexistent/nanocode-sim"))
        .durability(parts.store.clone())
        .build()
        .map_err(|error| format!("runtime build: {error}"))?;
    let record = SessionRecord::root(
        SESSION,
        ModelSelection::new(HarnessFamily::Claude, MODEL),
        None,
    );
    let host: Arc<dyn InboxHost> = Arc::clone(inbox) as Arc<dyn InboxHost>;
    let (inbox, pump) = TurnInbox::open(
        (*runtime).clone(),
        &parts.store,
        record,
        Arc::clone(&parts.log),
        host,
        Duration::from_millis(5),
    )
    .await
    .map_err(|error| format!("open inbox: {error}"))?;
    Ok(Process {
        inbox,
        pump,
        runtime,
    })
}

async fn pumped<T>(pump: &mut HostFuture<'static, ()>, work: impl Future<Output = T>) -> T {
    let mut work = pin!(work);
    let mut pumping = true;
    std::future::poll_fn(|context| {
        if pumping && pump.as_mut().poll(context).is_ready() {
            pumping = false;
        }
        work.as_mut().poll(context)
    })
    .await
}

async fn submit(process: &Process, request_id: &str, prompt: &str) -> Result<(), String> {
    process
        .inbox
        .log()
        .submit(&Submission {
            request_id: request_id.to_owned(),
            prompt: Prompt::new(prompt),
            hand: None,
            metadata: serde_json::value::to_raw_value(&json!({})).expect("an empty object encodes"),
        })
        .await
        .map(drop)
        .map_err(|error| format!("submit {request_id}: {error}"))
}

async fn serve(process: &Process, wire: &Wire, first: bool, case: Case) -> Result<(), String> {
    if first {
        submit(process, REQUEST, PROMPT).await?;
    }
    let settling = pin!(settle(process));
    let watching = pin!(watch(process, wire, case));
    match nanocode_sync::future::select(settling, watching).await {
        nanocode_sync::future::Either::Left((settled, _)) => settled?,
        nanocode_sync::future::Either::Right((watched, settling)) => {
            watched?;
            settling.await?;
        }
    }
    if first && case == Case::HostOwnedCancel {
        submit(process, NEXT_REQUEST, NEXT_PROMPT).await?;
        settle(process).await?;
    }
    Ok(())
}

async fn watch(process: &Process, wire: &Wire, case: Case) -> Result<(), String> {
    let inbox = &process.inbox;
    let cancels = matches!(
        case,
        Case::CancelWhileAwaitingChild | Case::HostOwnedCancel | Case::HostOwnedCancelLate
    );
    if !cancels && case != Case::HostOwned {
        return Ok(());
    }
    loop {
        nanocode_platform::sleep(Duration::from_millis(50)).await;
        let state = inbox
            .log()
            .turn(REQUEST)
            .await
            .ok()
            .flatten()
            .map(|record| record.state);
        let open = state == Some(TurnState::Running);
        let ready = match case {
            Case::CancelWhileAwaitingChild => open && inbox.agent().turn_controls().is_empty(),
            Case::HostOwnedCancel | Case::HostOwnedCancelLate => {
                open && wire.hang.read(Option::is_none)
            }
            _ => state.is_some_and(|state| state.is_terminal()),
        };
        if !ready {
            continue;
        }
        if cancels {
            drop(inbox.cancel(REQUEST, &serde_json::Map::new()).await);
        }
        if case == Case::HostOwnedCancelLate {
            return submit(process, NEXT_REQUEST, NEXT_PROMPT).await;
        }
        wire.released.store(true, Ordering::Release);
        return Ok(());
    }
}

async fn settle(process: &Process) -> Result<(), String> {
    for _ in 0..DRIVES_MAX {
        let driven = process
            .inbox
            .drive(None)
            .await
            .map_err(|error| format!("drive: {error}"))?;
        if matches!(driven, Driven::Idle) {
            let settled = process
                .runtime
                .settle(SESSION)
                .await
                .map_err(|error| format!("settle: {error}"))?;
            if settled && process.inbox.agent().turn_controls().is_empty() {
                return Ok(());
            }
        }
        nanocode_platform::sleep(Duration::from_millis(250)).await;
    }
    Err(format!(
        "the session did not settle after {DRIVES_MAX} drives"
    ))
}

struct Seen {
    record: Option<TurnRecord>,
    events: Vec<LoggedEvent>,
}

async fn inspect(process: &Process) -> Result<Seen, String> {
    let record = process
        .inbox
        .log()
        .turn(REQUEST)
        .await
        .map_err(|error| format!("turn: {error}"))?;
    let events = process
        .inbox
        .log()
        .page(0, 100_000)
        .await
        .map_err(|error| format!("log page: {error}"))?
        .events;
    Ok(Seen { record, events })
}

#[derive(Default)]
struct Run {
    wrong: Vec<String>,
    epochs: u64,
    crashes: u64,
}

fn epoch<T>(
    seed: u64,
    clock: &Arc<Clock>,
    label: &str,
    work: impl Future<Output = Result<T, String>>,
) -> (Ended, Option<Result<T, String>>) {
    let result = std::cell::RefCell::new(None);
    let mut executor = Executor::new(Arc::clone(clock), Rng::derive(seed, label));
    let task: Task<'_> = Box::pin(async {
        result.replace(Some(work.await));
    });
    let crash_at = clock.now_ns() + u64::try_from(CRASH_AFTER.as_nanos()).unwrap_or(u64::MAX);
    let ended = executor.run(vec![task], Some(crash_at));
    drop(executor);
    loop {
        let spawned = crate::host::spawned();
        if spawned.is_empty() {
            break;
        }
        drop(spawned);
    }
    (ended, result.into_inner())
}

fn final_message(record: &TurnRecord) -> Option<String> {
    let outcome: Value = serde_json::from_str(record.result.as_ref()?.get()).ok()?;
    outcome
        .pointer("/ok/message")
        .and_then(Value::as_str)
        .map(str::to_owned)
}

fn simulate_case(seed: u64, case: Case) -> (Run, u64) {
    let clock = Clock::new();
    crate::host::enter(seed, &clock);
    let disk = Disk::new();
    let wire = Arc::new(Wire::default());
    let inbox = Arc::new(Inbox::default());
    match case {
        Case::NoCrash => {}
        Case::CrashWhileAwaitingChild => {
            wire.hang
                .set(Some((CHILD_B.to_owned(), CHILD_B.to_owned())));
        }
        Case::CrashInContinuation => {
            wire.hang
                .set(Some((PROMPT.to_owned(), RESULT_B.to_owned())));
        }
        Case::CrashWhileClosing => inbox.hang_result.store(true, Ordering::Release),
        Case::CancelWhileAwaitingChild => wire.held.store(true, Ordering::Release),
        Case::HostOwned => {
            inbox.owns.store(true, Ordering::Release);
            wire.held.store(true, Ordering::Release);
        }
        Case::HostOwnedCancel | Case::HostOwnedCancelLate => {
            inbox.owns.store(true, Ordering::Release);
            wire.held.store(true, Ordering::Release);
            wire.late
                .store(case == Case::HostOwnedCancelLate, Ordering::Release);
            wire.hang
                .set(Some((PROMPT.to_owned(), "spawned".to_owned())));
        }
    }
    let mut run = Run::default();
    let mut done = false;
    for number in 0..EPOCHS_MAX {
        run.epochs += 1;
        let (ended, result) = epoch(
            seed,
            &clock,
            &format!("request_close {case:?} {number}"),
            async {
                let mut process = open(&disk, &wire, &inbox).await?;
                let mut pump = std::mem::replace(&mut process.pump, Box::pin(async {}));
                pumped(&mut pump, serve(&process, &wire, number == 0, case)).await
            },
        );
        match (ended, result) {
            (Ended::Finished, Some(Ok(()))) => {
                done = true;
                break;
            }
            (Ended::Finished, Some(Err(error))) => {
                run.wrong.push(format!("epoch {number}: {error}"));
                break;
            }
            (Ended::Crashed | Ended::Stuck, _) => run.crashes += 1,
            (Ended::Finished, None) => {
                run.wrong
                    .push(format!("epoch {number} ended without a result"));
                break;
            }
        }
    }
    if !done && run.wrong.is_empty() {
        run.wrong
            .push(format!("still open after {EPOCHS_MAX} epochs"));
    }
    if matches!(
        case,
        Case::CrashWhileAwaitingChild | Case::CrashInContinuation | Case::CrashWhileClosing
    ) {
        run.wrong
            .extend((run.crashes == 0).then(|| "the crash never happened".to_owned()));
    }
    let calls = *wire.calls.get();
    if !run.wrong.is_empty() {
        return (run, calls);
    }
    let (_, seen) = epoch(
        seed,
        &clock,
        &format!("request_close verify {case:?}"),
        async {
            let mut process = open(&disk, &wire, &inbox).await?;
            let mut pump = std::mem::replace(&mut process.pump, Box::pin(async {}));
            pumped(&mut pump, inspect(&process)).await
        },
    );
    let seen = match seen {
        Some(Ok(seen)) => seen,
        Some(Err(error)) => {
            run.wrong.push(format!("verify: {error}"));
            return (run, calls);
        }
        None => {
            run.wrong.push("verify did not finish".to_owned());
            return (run, calls);
        }
    };
    let state = seen.record.as_ref().map(|record| record.state);
    let results = seen
        .events
        .iter()
        .filter(|event| event.request_id.as_deref() == Some(REQUEST))
        .filter(|event| event.event.get().contains("\"type\":\"result\""))
        .count();
    if results != 1 {
        run.wrong.push(format!(
            "the request recorded {results} results instead of one"
        ));
    }
    let stray = seen
        .events
        .iter()
        .filter(|event| {
            event
                .request_id
                .as_deref()
                .is_some_and(|id| id.starts_with(COMPLETION_PREFIX))
        })
        .count();
    let root_calls = wire.root_calls.get();
    let root_calls: &Vec<String> = &root_calls;
    let delivered = |result: &str| {
        root_calls
            .iter()
            .filter(|last| last.contains(result))
            .count()
    };
    let message = seen
        .record
        .as_ref()
        .and_then(final_message)
        .unwrap_or_default();
    match case {
        Case::CancelWhileAwaitingChild => {
            if state != Some(TurnState::Cancelled) {
                run.wrong.push(format!(
                    "a request cancelled while waiting for a child ended {state:?}"
                ));
            }
            if stray > 0 {
                run.wrong.push(format!(
                    "{stray} events of child-completion passes belong to no request after the cancel"
                ));
            }
            let woke = delivered(RESULT_B);
            if woke > 0 {
                run.wrong.push(format!(
                    "after the request was cancelled, child B still woke the root agent in {woke} model calls"
                ));
            }
        }
        Case::HostOwned | Case::HostOwnedCancel | Case::HostOwnedCancelLate => {
            let expected = if case == Case::HostOwned {
                TurnState::Completed
            } else {
                TurnState::Cancelled
            };
            if state != Some(expected) {
                run.wrong.push(format!(
                    "a request whose host owns completion turns ended {state:?}"
                ));
            }
            if case == Case::HostOwned && (message.is_empty() || message.contains(RESULT_B)) {
                run.wrong.push(format!(
                    "the request closed with {message:?} instead of its first pass's answer while child B ran"
                ));
            }
            if case == Case::HostOwned && stray == 0 {
                run.wrong.push(
                    "no child-completion pass kept its own request id though the host owns them"
                        .to_owned(),
                );
            }
            let woke = delivered(RESULT_B);
            if woke != 1 {
                run.wrong.push(format!(
                    "child B's result reached the root {woke} times after the request closed instead of once"
                ));
            }
        }
        _ => {
            if state != Some(TurnState::Completed) {
                run.wrong.push(format!("the request ended {state:?}"));
            }
            if !message.contains(RESULT_B) {
                run.wrong.push(format!(
                    "the request closed with {message:?}, before the root read child B's result"
                ));
            }
            if stray > 0 {
                run.wrong.push(format!(
                    "{stray} events of child-completion passes belong to no request"
                ));
            }
            for result in [RESULT_A, RESULT_B] {
                let count = delivered(result);
                let ok = if case == Case::NoCrash {
                    count == 1
                } else {
                    count >= 1
                };
                if !ok {
                    run.wrong
                        .push(format!("the root read {result} in {count} new inputs"));
                }
            }
        }
    }
    (run, calls)
}

struct Outcome {
    digest: u64,
    stats: RequestCloseStats,
    wrong: Option<String>,
}

fn simulate(seed: u64) -> Outcome {
    let mut stats = RequestCloseStats {
        seeds: 1,
        ..RequestCloseStats::default()
    };
    let mut digest = FNV_OFFSET;
    let mut wrong = Vec::new();
    for case in CASES {
        let (run, calls) = simulate_case(seed, case);
        stats.cases += 1;
        stats.epochs += run.epochs;
        stats.crashes += run.crashes;
        stats.model_calls += calls;
        digest = fnv(
            digest,
            format!("{case:?} {} {} {calls}", run.epochs, run.crashes).as_bytes(),
        );
        if !run.wrong.is_empty() {
            wrong.push(format!("{case:?}: {}", run.wrong.join("; ")));
        }
    }
    Outcome {
        digest,
        stats,
        wrong: (!wrong.is_empty()).then(|| wrong.join(" | ")),
    }
}

pub fn judge(
    seed: u64,
    mutation: RequestCloseMutation,
    twice: bool,
    verbose: bool,
    totals: &mut RequestCloseStats,
) -> Option<String> {
    mutation.apply();
    let first = simulate(seed);
    totals.add(&first.stats);
    if verbose {
        println!("seed {seed} digest {:016x} {:?}", first.digest, first.stats);
    }
    if first.wrong.is_some() || !twice {
        return first.wrong;
    }
    let second = simulate(seed);
    (second.digest != first.digest || second.wrong.is_some()).then(|| {
        format!(
            "nondeterminism: digest {:016x} then {:016x}",
            first.digest, second.digest
        )
    })
}

pub fn replay(seed: u64, mutation: RequestCloseMutation) -> String {
    let mut command = format!("nanocode-sim --scenario request_close --seed {seed}");
    if mutation != RequestCloseMutation::None {
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
    mutation: RequestCloseMutation,
    totals: &RequestCloseStats,
) -> ExitCode {
    println!(
        "RESULT seeds={}..{} scenario=request_close mutation={} passed={passed} failed={failed} twice={twice} stats={totals:?}",
        seeds.start,
        seeds.end,
        mutation.name()
    );
    match mutation {
        RequestCloseMutation::None if failed == 0 => ExitCode::SUCCESS,
        RequestCloseMutation::None => ExitCode::FAILURE,
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
