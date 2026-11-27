use std::{
    collections::BTreeSet,
    future::Future,
    ops::Range,
    path::PathBuf,
    pin::pin,
    process::ExitCode,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    task::Poll,
    time::Duration,
};

use nanocode::{
    ClaudeClient, HarnessFamily, HostFuture, ModelSelection, Nanocode, Thinking, Tools,
    backend::Prompt,
    claude::transport::{HttpError, HttpFuture, HttpRequest, HttpResponse, HttpTransport},
    durability::SessionRecord,
    session_host::{
        Delivery, Guarded, InboxHost, LoggedEvent, MovedQueued, Next, PassEnd, Replaced,
        Submission, TurnInbox, TurnRecord, TurnState,
    },
    turnlog::OneLog,
};
use nanocode_sync::Atom;
use serde_json::{Map, Value, json};

use crate::{
    disk::Disk,
    executor::{Clock, Ended, Executor, Task},
    rng::{FNV_OFFSET, Rng, fnv},
    server::text_of,
    turnlog::SimHost,
};

const SESSION: &str = "inbox-stress";
const MODEL: &str = "claude-haiku-5-5";
const RUNNING_MARK: &str = "Script running with cell ID ";
const EPOCHS_MAX: u64 = 6;
const CRASH_AFTER: Duration = Duration::from_secs(600);
const DRIVES_MAX: u64 = 400;
const DRIVE_PAUSE: Duration = Duration::from_millis(100);
const R1: &str = "r1";
const F1: &str = "f1";
const FOLLOW: &str = "FOLLOWUP1";
const QUEUED: &str = "q1";
const DROPPED: &str = "DROPPEDREQ";
const EDIT: &str = "EDITED1";
const EDIT_ID: &str = "e1";
const TOKENS: [&str; 2] = [FOLLOW, EDIT];

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum InboxStressMutation {
    None,
    CancelSkipsSteerGuard,
    EditSkipsSteerGuard,
    MoveUsesUnreservedPrompt,
    EditRetrySkipsReceipt,
}

impl InboxStressMutation {
    const ALL: [Self; 5] = [
        Self::None,
        Self::CancelSkipsSteerGuard,
        Self::EditSkipsSteerGuard,
        Self::MoveUsesUnreservedPrompt,
        Self::EditRetrySkipsReceipt,
    ];

    pub fn parse(name: &str) -> Option<Self> {
        Self::ALL
            .into_iter()
            .find(|mutation| mutation.name() == name)
    }

    pub const fn name(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::CancelSkipsSteerGuard => "cancel_skips_steer_guard",
            Self::EditSkipsSteerGuard => "edit_skips_steer_guard",
            Self::MoveUsesUnreservedPrompt => "move_uses_unreserved_prompt",
            Self::EditRetrySkipsReceipt => "edit_retry_skips_receipt",
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
pub struct InboxStressStats {
    pub seeds: u64,
    pub cases: u64,
    pub epochs: u64,
    pub crashes: u64,
    pub model_calls: u64,
    pub moved: u64,
    pub refused: u64,
}

impl InboxStressStats {
    pub const fn add(&mut self, other: &Self) {
        self.seeds += other.seeds;
        self.cases += other.cases;
        self.epochs += other.epochs;
        self.crashes += other.crashes;
        self.model_calls += other.model_calls;
        self.moved += other.moved;
        self.refused += other.refused;
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Case {
    FollowUpAtPassEnd,
    FollowUpAtPassEndCrash,
    CancelQueuedWhileRunning,
    FollowUpThenCancel,
    FollowUpThenFail,
    EditWhileSteering,
    CancelWhileSteering,
    EditAfterUnread,
    CancelAfterUnread,
    MoveBeforeEdit,
    MoveBeforeCancel,
    EditBeforeMove,
    CancelBeforeMove,
    RetryEditWhileSteering,
}

const CASES: [Case; 14] = [
    Case::FollowUpAtPassEnd,
    Case::FollowUpAtPassEndCrash,
    Case::CancelQueuedWhileRunning,
    Case::FollowUpThenCancel,
    Case::FollowUpThenFail,
    Case::EditWhileSteering,
    Case::CancelWhileSteering,
    Case::EditAfterUnread,
    Case::CancelAfterUnread,
    Case::MoveBeforeEdit,
    Case::MoveBeforeCancel,
    Case::EditBeforeMove,
    Case::CancelBeforeMove,
    Case::RetryEditWhileSteering,
];

#[derive(Default)]
struct Wire {
    root_calls: Atom<Vec<String>>,
    calls: Atom<u64>,
    gate: AtomicBool,
    at_gate: AtomicBool,
    root_answers: AtomicU64,
    hang_root: AtomicBool,
    fail_root: AtomicBool,
}

enum Reply {
    Ok((Value, Option<Value>, &'static str)),
    Fail,
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
    async fn reply(&self, body: &str) -> Reply {
        let reply = self.decide(body).await;
        if self.fail_root.swap(false, Ordering::AcqRel) {
            return Reply::Fail;
        }
        Reply::Ok(reply)
    }

    async fn decide(&self, body: &str) -> (Value, Option<Value>, &'static str) {
        let messages = messages(body);
        let last = messages.last().map(content).unwrap_or_default();
        if let Some(cell) = running_cell(&last) {
            return tool("wait", &json!({"cell_id": cell, "yield_time_ms": 30_000}));
        }
        if self.hang_root.swap(false, Ordering::AcqRel) {
            std::future::pending::<()>().await;
        }
        self.root_calls.update(|calls| calls.push(body.to_owned()));
        if self.gate.load(Ordering::Acquire) {
            self.at_gate.store(true, Ordering::Release);
            while self.gate.load(Ordering::Acquire) {
                nanocode_platform::sleep(Duration::from_millis(2)).await;
            }
        }
        let history: String = messages.iter().map(content).collect();
        let seen: Vec<&str> = TOKENS
            .into_iter()
            .filter(|token| history.contains(token))
            .collect();
        self.root_answers.fetch_add(1, Ordering::AcqRel);
        say(&format!("answer [{}]", seen.join(",")))
    }
}

impl HttpTransport for WireTransport {
    fn post(&self, request: HttpRequest) -> HttpFuture<'_, Result<HttpResponse, HttpError>> {
        Box::pin(async move {
            let body = request.body;
            nanocode_platform::sleep(Duration::from_millis(20)).await;
            let reply = self.0.reply(&body).await;
            self.0.calls.update(|calls| *calls += 1);
            let (block, delta, stop) = match reply {
                Reply::Ok(reply) => reply,
                Reply::Fail => {
                    let mut headers = nanocode_http::types::HeaderMap::new();
                    headers.insert(
                        nanocode_http::types::header::CONTENT_TYPE,
                        nanocode_http::types::HeaderValue::from_static("application/json"),
                    );
                    let body = json!({"type": "error", "error": {"type": "invalid_request_error", "message": "the request was rejected"}}).to_string();
                    return Ok(HttpResponse::new(
                        nanocode_http::types::StatusCode::BAD_REQUEST,
                        headers,
                        Box::pin(nanocode_sync::stream::once(
                            async move { Ok(body.into_bytes()) },
                        )),
                    ));
                }
            };
            let count = *self.0.calls.get();
            let sse = crate::server::events(MODEL, &format!("is{count}"), &block, delta, stop);
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

#[derive(Clone, Debug)]
struct Received {
    seq: u64,
    request_id: Option<String>,
    kind: String,
}

#[derive(Default)]
struct Host {
    received: Atom<Vec<Received>>,
}

fn kind(event: &LoggedEvent) -> String {
    serde_json::from_str::<Value>(event.event.get())
        .ok()
        .and_then(|value| value.get("type").and_then(Value::as_str).map(str::to_owned))
        .unwrap_or_default()
}

impl InboxHost for Host {
    fn publish(&self, events: Vec<LoggedEvent>) -> HostFuture<'_, Result<(), String>> {
        Box::pin(async move {
            self.received.update(|received| {
                received.extend(events.iter().map(|event| Received {
                    seq: event.seq,
                    request_id: event.request_id.clone(),
                    kind: kind(event),
                }));
            });
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

async fn open(disk: &Arc<Disk>, wire: &Arc<Wire>, host: &Arc<Host>) -> Result<Process, String> {
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
    let host: Arc<dyn InboxHost> = Arc::clone(host) as Arc<dyn InboxHost>;
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

async fn both<A, B>(left: impl Future<Output = A>, right: impl Future<Output = B>) -> (A, B) {
    let mut left = pin!(left);
    let mut right = pin!(right);
    let mut left_done = None;
    let mut right_done = None;
    std::future::poll_fn(|context| {
        if left_done.is_none()
            && let Poll::Ready(value) = left.as_mut().poll(context)
        {
            left_done = Some(value);
        }
        if right_done.is_none()
            && let Poll::Ready(value) = right.as_mut().poll(context)
        {
            right_done = Some(value);
        }
        if left_done.is_some() && right_done.is_some() {
            Poll::Ready((
                left_done.take().expect("checked above"),
                right_done.take().expect("checked above"),
            ))
        } else {
            Poll::Pending
        }
    })
    .await
}

fn submission(request_id: &str, prompt: &str) -> Submission {
    Submission {
        request_id: request_id.to_owned(),
        prompt: Prompt::new(prompt),
        hand: None,
        metadata: serde_json::value::to_raw_value(&json!({})).expect("an empty object encodes"),
    }
}

#[derive(Default)]
struct Script {
    submitted: AtomicBool,
    directed: AtomicBool,
    paused: AtomicBool,
    moved: Atom<Option<String>>,
    touched: Atom<Option<String>>,
}

struct World {
    case: Case,
    mutation: InboxStressMutation,
    seed: u64,
    disk: Arc<Disk>,
    wire: Arc<Wire>,
    host: Arc<Host>,
    script: Script,
}

async fn until(check: impl Fn() -> bool) {
    while !check() {
        nanocode_platform::sleep(Duration::from_millis(2)).await;
    }
}

async fn submit(process: &Process, world: &World, id: &str, prompt: &str) -> Result<(), String> {
    let submitted = process
        .inbox
        .log()
        .submit(&submission(id, prompt))
        .await
        .map_err(|error| format!("submit {id}: {error}"))?;
    if let nanocode::session_host::Submitted::Accepted { event, .. } = submitted {
        drop(world.host.publish(vec![event]).await);
    }
    Ok(())
}

async fn follow_up(process: &Process, world: &World) -> Result<(), String> {
    submit(
        process,
        world,
        F1,
        &format!("{FOLLOW} please also cover this"),
    )
    .await?;
    move_follow_up(process, world).await
}

async fn move_follow_up(process: &Process, world: &World) -> Result<(), String> {
    let moved = if world.mutation == InboxStressMutation::MoveUsesUnreservedPrompt
        && matches!(world.case, Case::MoveBeforeEdit | Case::MoveBeforeCancel)
    {
        let prompt = process
            .inbox
            .log()
            .turn(F1)
            .await
            .map_err(|error| error.to_string())?
            .filter(|record| record.state == TurnState::Queued)
            .and_then(|record| record.prompt)
            .ok_or_else(|| "the move did not snapshot a queued prompt".to_owned())?;
        until(|| world.script.touched.read(Option::is_some)).await;
        let moved = process
            .inbox
            .steer(Some(R1), F1, prompt)
            .await
            .map_err(|error| error.to_string())?;
        if moved {
            MovedQueued::Moved
        } else {
            MovedQueued::NotRunning
        }
    } else {
        process
            .inbox
            .move_queued(F1, Some(R1))
            .await
            .map_err(|error| format!("move_queued: {error}"))?
    };
    let name = match moved {
        MovedQueued::Moved => "moved",
        MovedQueued::NotQueued => "not_queued",
        MovedQueued::NotRunning => "not_running",
        MovedQueued::Conflict => "conflict",
        MovedQueued::Unknown => "unknown",
    };
    world.script.moved.set(Some(name.to_owned()));
    Ok(())
}

fn edit_text() -> String {
    format!("{EDIT} cover this instead")
}

async fn edit(process: &Process, world: &World) -> Result<&'static str, String> {
    let text = edit_text();
    let replaced = if world.mutation == InboxStressMutation::EditSkipsSteerGuard {
        let replaced = process
            .inbox
            .log()
            .replace_queued(F1, EDIT_ID, &text)
            .await
            .map_err(|error| format!("replace_queued: {error}"))?;
        if let Replaced::Applied(Some(event)) = &replaced {
            drop(world.host.publish(vec![event.clone()]).await);
        }
        Ok(Guarded::Ready(replaced))
    } else {
        process.inbox.replace_queued(F1, EDIT_ID, &text).await
    };
    Ok(match replaced {
        Ok(Guarded::Ready(Replaced::Applied(_))) => "applied",
        Ok(Guarded::Ready(Replaced::NotQueued)) => "not_queued",
        Ok(Guarded::Ready(Replaced::Conflict)) => "conflict",
        Ok(Guarded::Ready(Replaced::TooLarge { .. })) => "too_large",
        Ok(Guarded::Steering) => "refused",
        Err(error) => return Err(format!("replace_queued: {error}")),
    })
}

async fn cancel_follow_up(process: &Process, world: &World) -> Result<&'static str, String> {
    let cancelled = if world.mutation == InboxStressMutation::CancelSkipsSteerGuard {
        let cancelled = process
            .inbox
            .log()
            .cancel(F1, &Map::new())
            .await
            .map_err(|error| format!("cancel: {error}"))?;
        if let Some((_, Some(event))) = &cancelled {
            drop(world.host.publish(vec![event.clone()]).await);
        }
        Ok(Guarded::Ready(
            cancelled.map(|(record, event)| (record, event.is_some())),
        ))
    } else {
        process.inbox.cancel_guarded(F1, &Map::new()).await
    };
    Ok(match cancelled {
        Ok(Guarded::Ready(Some((_, true)))) => "cancelled",
        Ok(Guarded::Ready(Some((_, false)))) => "not_queued",
        Ok(Guarded::Ready(None)) => "missing",
        Ok(Guarded::Steering) => "refused",
        Err(error) => return Err(format!("cancel: {error}")),
    })
}

async fn touch(process: &Process, world: &World) -> Result<(), String> {
    let touched = match world.case {
        Case::EditWhileSteering
        | Case::EditAfterUnread
        | Case::MoveBeforeEdit
        | Case::EditBeforeMove => edit(process, world).await?,
        _ => cancel_follow_up(process, world).await?,
    };
    world.script.touched.set(Some(touched.to_owned()));
    Ok(())
}

async fn released(process: &Process) -> Result<(), String> {
    for _ in 0..DRIVES_MAX {
        let ended = process
            .inbox
            .log()
            .turn(R1)
            .await
            .map_err(|error| format!("turn {R1}: {error}"))?
            .is_some_and(|record| record.state.is_terminal());
        let steering = process
            .inbox
            .steering(F1)
            .await
            .map_err(|error| format!("steering: {error}"))?;
        if ended && !steering {
            return Ok(());
        }
        nanocode_platform::sleep(DRIVE_PAUSE).await;
    }
    Err(format!(
        "the moved follow-up was still held as a steer {DRIVES_MAX} polls after its target was cancelled"
    ))
}

async fn direct(process: &Process, world: &World) -> Result<(), String> {
    if world.script.directed.load(Ordering::Acquire) {
        return Ok(());
    }
    let wire = &world.wire;
    let jitter = Duration::from_millis(world.seed % 23);
    match world.case {
        Case::FollowUpAtPassEnd | Case::FollowUpAtPassEndCrash => {
            until(|| wire.at_gate.load(Ordering::Acquire)).await;
            wire.gate.store(false, Ordering::Release);
            nanocode_platform::sleep(jitter).await;
            follow_up(process, world).await?;
            if world.case == Case::FollowUpAtPassEndCrash
                && world
                    .script
                    .moved
                    .read(|moved| moved.as_deref() == Some("moved"))
            {
                wire.hang_root.store(true, Ordering::Release);
            }
        }
        Case::CancelQueuedWhileRunning => {
            until(|| wire.at_gate.load(Ordering::Acquire)).await;
            submit(process, world, QUEUED, &format!("{DROPPED} never mind")).await?;
            process
                .inbox
                .cancel(QUEUED, &Map::new())
                .await
                .map_err(|error| format!("cancel: {error}"))?;
            wire.gate.store(false, Ordering::Release);
        }
        Case::FollowUpThenCancel | Case::FollowUpThenFail => {
            until(|| wire.at_gate.load(Ordering::Acquire)).await;
            follow_up(process, world).await?;
            if world.case == Case::FollowUpThenCancel {
                process
                    .inbox
                    .cancel(R1, &Map::new())
                    .await
                    .map_err(|error| format!("cancel: {error}"))?;
            } else {
                wire.fail_root.store(true, Ordering::Release);
            }
            wire.gate.store(false, Ordering::Release);
        }
        Case::EditWhileSteering | Case::CancelWhileSteering => {
            until(|| wire.at_gate.load(Ordering::Acquire)).await;
            follow_up(process, world).await?;
            wire.gate.store(false, Ordering::Release);
            let late = u32::try_from(world.seed % 4).expect("a remainder of four fits u32");
            nanocode_platform::sleep(DRIVE_PAUSE * late).await;
            touch(process, world).await?;
        }
        Case::MoveBeforeEdit
        | Case::MoveBeforeCancel
        | Case::EditBeforeMove
        | Case::CancelBeforeMove => {
            until(|| wire.at_gate.load(Ordering::Acquire)).await;
            submit(
                process,
                world,
                F1,
                &format!("{FOLLOW} please also cover this"),
            )
            .await?;
            if matches!(world.case, Case::MoveBeforeEdit | Case::MoveBeforeCancel) {
                let (moved, touched) =
                    both(move_follow_up(process, world), touch(process, world)).await;
                moved?;
                touched?;
            } else {
                let (touched, moved) =
                    both(touch(process, world), move_follow_up(process, world)).await;
                touched?;
                moved?;
            }
            wire.gate.store(false, Ordering::Release);
        }
        Case::RetryEditWhileSteering => {
            until(|| wire.at_gate.load(Ordering::Acquire)).await;
            submit(
                process,
                world,
                F1,
                &format!("{FOLLOW} please also cover this"),
            )
            .await?;
            let text = edit_text();
            let first = process
                .inbox
                .replace_queued(F1, EDIT_ID, &text)
                .await
                .map_err(|error| error.to_string())?;
            if !matches!(first, Guarded::Ready(Replaced::Applied(Some(_)))) {
                return Err(format!("could not commit the first edit: {first:?}"));
            }
            drop(first);
            move_follow_up(process, world).await?;
            let retry = if world.mutation == InboxStressMutation::EditRetrySkipsReceipt
                && process
                    .inbox
                    .steering(F1)
                    .await
                    .map_err(|error| error.to_string())?
            {
                Guarded::Steering
            } else {
                process
                    .inbox
                    .replace_queued(F1, EDIT_ID, &text)
                    .await
                    .map_err(|error| error.to_string())?
            };
            let conflict = process
                .inbox
                .replace_queued(F1, EDIT_ID, FOLLOW)
                .await
                .map_err(|error| error.to_string())?;
            let new_edit = process
                .inbox
                .replace_queued(F1, "new_edit", FOLLOW)
                .await
                .map_err(|error| error.to_string())?;
            wire.gate.store(false, Ordering::Release);
            if !matches!(retry, Guarded::Ready(Replaced::Applied(None)))
                || !matches!(conflict, Guarded::Ready(Replaced::Conflict))
                || !matches!(new_edit, Guarded::Steering)
            {
                return Err(format!(
                    "edit after move: retry={retry:?}, conflict={conflict:?}, new={new_edit:?}"
                ));
            }
        }
        Case::EditAfterUnread | Case::CancelAfterUnread => {
            until(|| wire.at_gate.load(Ordering::Acquire)).await;
            follow_up(process, world).await?;
            world.script.paused.store(true, Ordering::Release);
            process
                .inbox
                .cancel(R1, &Map::new())
                .await
                .map_err(|error| format!("cancel: {error}"))?;
            wire.gate.store(false, Ordering::Release);
            released(process).await?;
            touch(process, world).await?;
            world.script.paused.store(false, Ordering::Release);
        }
    }
    world.script.directed.store(true, Ordering::Release);
    Ok(())
}

async fn drive(process: &Process, world: &World) -> Result<(), String> {
    for _ in 0..DRIVES_MAX {
        until(|| !world.script.paused.load(Ordering::Acquire)).await;
        match process.inbox.drive(None).await {
            Ok(nanocode::session_host::Driven::Idle) => {
                let settled = process
                    .runtime
                    .settle(SESSION)
                    .await
                    .map_err(|error| format!("settle: {error}"))?;
                let queued = process
                    .inbox
                    .log()
                    .pending()
                    .await
                    .map_err(|error| format!("pending: {error}"))?;
                if settled
                    && queued.is_empty()
                    && process.inbox.agent().turn_controls().is_empty()
                    && world.script.directed.load(Ordering::Acquire)
                {
                    return Ok(());
                }
            }
            Ok(_) => {}
            Err(error) => return Err(format!("drive: {error}")),
        }
        nanocode_platform::sleep(DRIVE_PAUSE).await;
    }
    Err(format!(
        "the session did not settle after {DRIVES_MAX} drives"
    ))
}

async fn serve(process: &Process, world: &World) -> Result<(), String> {
    if !world.script.submitted.swap(true, Ordering::AcqRel) {
        let prompt = "plain question one".to_owned();
        submit(process, world, R1, &prompt).await?;
    }
    let (driven, directed) = both(drive(process, world), direct(process, world)).await;
    directed?;
    driven
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

fn answer(record: &TurnRecord) -> Option<String> {
    let outcome: Value = serde_json::from_str(record.result.as_ref()?.get()).ok()?;
    outcome
        .pointer("/ok/message")
        .and_then(Value::as_str)
        .map(str::to_owned)
}

fn steered(record: &TurnRecord) -> bool {
    record
        .report()
        .ok()
        .is_some_and(|report| report.get("steered") == Some(&Value::Bool(true)))
}

struct Seen {
    records: Vec<(String, Option<TurnRecord>)>,
    events: Vec<LoggedEvent>,
}

async fn inspect(process: &Process, ids: &[&str]) -> Result<Seen, String> {
    let mut records = Vec::new();
    for id in ids {
        let record = process
            .inbox
            .log()
            .turn(id)
            .await
            .map_err(|error| format!("turn {id}: {error}"))?;
        records.push(((*id).to_owned(), record));
    }
    let events = process
        .inbox
        .log()
        .page(0, 100_000)
        .await
        .map_err(|error| format!("log page: {error}"))?
        .events;
    Ok(Seen { records, events })
}

#[derive(Default)]
struct Run {
    wrong: Vec<String>,
    epochs: u64,
    crashes: u64,
    refused: bool,
}

impl Run {
    fn check(&mut self, ok: bool, what: impl Into<String>) {
        if !ok {
            self.wrong.push(what.into());
        }
    }
}

fn simulate_case(seed: u64, case: Case, mutation: InboxStressMutation) -> (Run, u64, bool) {
    let clock = Clock::new();
    crate::host::enter(seed, &clock);
    let world = World {
        case,
        mutation,
        seed,
        disk: Disk::new(),
        wire: Arc::new(Wire::default()),
        host: Arc::new(Host::default()),
        script: Script::default(),
    };
    world.wire.gate.store(true, Ordering::Release);
    let mut run = Run::default();
    let mut done = false;
    for number in 0..EPOCHS_MAX {
        run.epochs += 1;
        let (ended, result) = epoch(
            seed,
            &clock,
            &format!("inbox_stress {case:?} {number}"),
            async {
                let mut process = open(&world.disk, &world.wire, &world.host).await?;
                let mut pump = std::mem::replace(&mut process.pump, Box::pin(async {}));
                pumped(&mut pump, serve(&process, &world)).await
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
    let calls = *world.wire.calls.get();
    let moved = world
        .script
        .moved
        .read(|moved| moved.as_deref() == Some("moved"));
    run.refused = world
        .script
        .touched
        .read(|touched| touched.as_deref() == Some("refused"));
    if !run.wrong.is_empty() {
        return (run, calls, moved);
    }
    let ids: Vec<&str> = match case {
        Case::FollowUpAtPassEnd
        | Case::FollowUpAtPassEndCrash
        | Case::FollowUpThenCancel
        | Case::FollowUpThenFail
        | Case::EditWhileSteering
        | Case::CancelWhileSteering
        | Case::EditAfterUnread
        | Case::CancelAfterUnread
        | Case::MoveBeforeEdit
        | Case::MoveBeforeCancel
        | Case::EditBeforeMove
        | Case::CancelBeforeMove
        | Case::RetryEditWhileSteering => vec![R1, F1],
        Case::CancelQueuedWhileRunning => vec![R1, QUEUED],
    };
    let (_, seen) = epoch(
        seed,
        &clock,
        &format!("inbox_stress verify {case:?}"),
        async {
            let mut process = open(&world.disk, &world.wire, &world.host).await?;
            let mut pump = std::mem::replace(&mut process.pump, Box::pin(async {}));
            pumped(&mut pump, inspect(&process, &ids)).await
        },
    );
    let seen = match seen {
        Some(Ok(seen)) => seen,
        Some(Err(error)) => {
            run.wrong.push(format!("verify: {error}"));
            return (run, calls, moved);
        }
        None => {
            run.wrong.push("verify did not finish".to_owned());
            return (run, calls, moved);
        }
    };
    judge_case(&mut run, &world, &seen, moved);
    (run, calls, moved)
}

fn record<'s>(seen: &'s Seen, id: &str) -> Option<&'s TurnRecord> {
    seen.records
        .iter()
        .find(|(name, _)| name == id)
        .and_then(|(_, record)| record.as_ref())
}

fn answered_in(r1_answer: &str, f1_answer: &str) -> &'static str {
    match (r1_answer.contains(FOLLOW), f1_answer.contains(FOLLOW)) {
        (true, false) => "r1",
        (false, true) => "f1",
        (true, true) => "both",
        (false, false) => "neither",
    }
}

fn judge_case(run: &mut Run, world: &World, seen: &Seen, moved: bool) {
    let case = world.case;
    for (id, record) in &seen.records {
        let results = seen
            .events
            .iter()
            .filter(|event| event.request_id.as_deref() == Some(id.as_str()))
            .filter(|event| event.event.get().contains("\"type\":\"result\""))
            .count();
        run.check(
            results == 1,
            format!("request {id} recorded {results} results instead of one"),
        );
        run.check(
            record
                .as_ref()
                .is_some_and(|record| record.state.is_terminal()),
            format!(
                "request {id} ended {:?}",
                record.as_ref().map(|record| record.state)
            ),
        );
    }
    let logged: BTreeSet<u64> = seen.events.iter().map(|event| event.seq).collect();
    let received: BTreeSet<u64> = world
        .host
        .received
        .read(|received| received.iter().map(|event| event.seq).collect());
    let missed: Vec<u64> = logged.difference(&received).copied().collect();
    let results_received = world.host.received.read(|received| {
        received
            .iter()
            .filter(|event| event.kind == "result")
            .filter_map(|event| event.request_id.clone().map(|id| (id, event.seq)))
            .collect::<BTreeSet<_>>()
    });
    for (id, _) in &seen.records {
        let pushed = results_received
            .iter()
            .filter(|(request, _)| request == id)
            .count();
        run.check(
            pushed == 1,
            format!("the host was pushed {pushed} distinct results for request {id}"),
        );
    }
    let missed: Vec<String> = seen
        .events
        .iter()
        .filter(|event| missed.contains(&event.seq))
        .map(|event| {
            format!(
                "{}:{}",
                event.request_id.clone().unwrap_or_default(),
                kind(event)
            )
        })
        .collect();
    run.check(
        missed.is_empty(),
        format!("the host was never pushed logged events {missed:?}"),
    );
    let root_calls = world.wire.root_calls.get();
    let r1 = record(seen, R1);
    let r1_answer = r1.and_then(answer).unwrap_or_default();
    match case {
        Case::FollowUpAtPassEnd | Case::FollowUpAtPassEndCrash => {
            let f1 = record(seen, F1);
            let by_steer = f1.is_some_and(steered);
            let f1_answer = f1.and_then(answer).unwrap_or_default();
            let answered_in = answered_in(&r1_answer, &f1_answer);
            run.check(
                (by_steer && answered_in == "r1") || (!by_steer && answered_in == "f1"),
                format!(
                    "the follow-up was {} (move_queued said {}) but its text is answered in {answered_in}: r1={r1_answer:?} f1={:?}",
                    if by_steer { "closed as steered into r1" } else { "run as its own request" },
                    world.script.moved.read(|moved| moved.clone().unwrap_or_default()),
                    f1.map(|record| record.state),
                ),
            );
            run.check(
                moved == by_steer,
                format!("move_queued reported moved={moved} but the follow-up record says steered={by_steer}"),
            );
        }
        Case::FollowUpThenCancel | Case::FollowUpThenFail => {
            let f1 = record(seen, F1);
            let read = root_calls.iter().any(|body| {
                messages(body)
                    .iter()
                    .any(|message| content(message).contains(FOLLOW))
            });
            let answered = r1_answer.contains(FOLLOW)
                || f1
                    .and_then(answer)
                    .is_some_and(|text| text.contains(FOLLOW));
            run.check(
                read && answered || f1.is_some_and(|record| !steered(record) && record.state != TurnState::Cancelled),
                format!(
                    "the follow-up was lost: move_queued said {}, its record ended {:?} steered={}, the model {} its text, and no answer covers it (r1 ended {:?})",
                    world.script.moved.read(|moved| moved.clone().unwrap_or_default()),
                    f1.map(|record| record.state),
                    f1.is_some_and(steered),
                    if read { "read" } else { "never read" },
                    r1.map(|record| record.state),
                ),
            );
        }
        Case::CancelQueuedWhileRunning => {
            let queued = record(seen, QUEUED);
            run.check(
                queued.map(|record| record.state) == Some(TurnState::Cancelled),
                format!(
                    "the cancelled queued request ended {:?}",
                    queued.map(|record| record.state)
                ),
            );
            run.check(
                !root_calls.iter().any(|body| body.contains(DROPPED)),
                "a cancelled queued request reached the model",
            );
        }
        Case::EditWhileSteering
        | Case::CancelWhileSteering
        | Case::EditAfterUnread
        | Case::CancelAfterUnread => judge_touched(run, world, seen, moved),
        Case::MoveBeforeEdit
        | Case::MoveBeforeCancel
        | Case::EditBeforeMove
        | Case::CancelBeforeMove
        | Case::RetryEditWhileSteering => {
            let f1 = record(seen, F1);
            let move_result = world.script.moved.get();
            let touched = world.script.touched.get();
            let read = |token: &str| root_calls.iter().any(|body| body.contains(token));
            let (expected_move, expected_touch, token) = match case {
                Case::MoveBeforeEdit | Case::MoveBeforeCancel => ("moved", Some("refused"), FOLLOW),
                Case::EditBeforeMove => ("moved", Some("applied"), EDIT),
                Case::RetryEditWhileSteering => ("moved", None, EDIT),
                Case::CancelBeforeMove => ("not_queued", Some("cancelled"), ""),
                _ => unreachable!(),
            };
            run.check(
                move_result.as_deref() == Some(expected_move) && touched.as_deref() == expected_touch,
                format!("expected move={expected_move} touch={expected_touch:?}, got move={move_result:?} touch={touched:?}"),
            );
            for candidate in TOKENS {
                run.check(
                    read(candidate) == (token == candidate),
                    format!(
                        "model saw {candidate}={}, expected only {token:?}; r1={r1_answer:?}",
                        read(candidate)
                    ),
                );
            }
            if token.is_empty() {
                run.check(
                    f1.is_some_and(|f1| f1.state == TurnState::Cancelled && !steered(f1)),
                    "the cancelled follow-up was not cancelled in the queue",
                );
            } else {
                run.check(f1.is_some_and(steered) && r1_answer.contains(token),
                    format!("the target did not answer the steered version {token}: r1={r1_answer:?}, f1={:?}", f1.and_then(answer)));
            }
            if case == Case::RetryEditWhileSteering {
                let published = world.host.received.read(|events| {
                    events
                        .iter()
                        .filter(|event| {
                            event.kind == "replaced" && event.request_id.as_deref() == Some(F1)
                        })
                        .count()
                });
                let logged = seen
                    .events
                    .iter()
                    .filter(|event| {
                        kind(event) == "replaced" && event.request_id.as_deref() == Some(F1)
                    })
                    .count();
                run.check(
                    published == 1 && logged == 1,
                    format!("the edit was logged {logged} times and published {published} times"),
                );
            }
        }
    }
    if case == Case::FollowUpAtPassEndCrash && moved {
        run.check(run.crashes >= 1, "the crash never happened");
    }
}

fn judge_touched(run: &mut Run, world: &World, seen: &Seen, moved: bool) {
    let case = world.case;
    let root_calls = world.wire.root_calls.get();
    let read = |token: &str| root_calls.iter().any(|body| body.contains(token));
    let r1_answer = record(seen, R1).and_then(answer).unwrap_or_default();
    let f1 = record(seen, F1);
    let f1_answer = f1.and_then(answer).unwrap_or_default();
    let by_steer = f1.is_some_and(steered);
    let touched = world
        .script
        .touched
        .read(|touched| touched.clone().unwrap_or_default());
    let edits = matches!(case, Case::EditWhileSteering | Case::EditAfterUnread);
    let expected: &[&str] = match case {
        Case::EditWhileSteering | Case::CancelWhileSteering => &["refused", "not_queued"],
        Case::EditAfterUnread => &["applied"],
        _ => &["cancelled"],
    };
    run.check(
        moved && expected.contains(&touched.as_str()),
        format!(
            "the host's {} of the moved follow-up was {touched} (move_queued said {}), expected one of {expected:?}",
            if edits { "edit" } else { "cancel" },
            world.script.moved.read(|moved| moved.clone().unwrap_or_default()),
        ),
    );
    if touched == "refused" || touched == "not_queued" {
        let answered_in = answered_in(&r1_answer, &f1_answer);
        run.check(
            (by_steer && answered_in == "r1") || (!by_steer && answered_in == "f1"),
            format!(
                "the follow-up the host could not {} was steered={by_steer} but its text is answered in {answered_in}: r1={r1_answer:?} f1={f1_answer:?}",
                if edits { "edit" } else { "cancel" },
            ),
        );
        run.check(
            !read(EDIT),
            "an edit that was not applied reached the model",
        );
        return;
    }
    run.check(
        !read(FOLLOW),
        format!(
            "the host {} the moved follow-up but the model still read it: r1={r1_answer:?} f1={f1_answer:?}",
            if edits { "edited" } else { "cancelled" },
        ),
    );
    if edits {
        run.check(
            !by_steer && f1_answer.contains(EDIT) && !r1_answer.contains(EDIT),
            format!("the edited follow-up was steered={by_steer}, answered r1={r1_answer:?} f1={f1_answer:?}"),
        );
    } else {
        run.check(
            f1.map(|record| record.state) == Some(TurnState::Cancelled) && !by_steer,
            format!(
                "the cancelled follow-up ended {:?} steered={by_steer}",
                f1.map(|record| record.state)
            ),
        );
    }
}

struct Outcome {
    digest: u64,
    stats: InboxStressStats,
    wrong: Option<String>,
}

fn simulate(seed: u64, only: Option<&str>, mutation: InboxStressMutation) -> Outcome {
    let mut stats = InboxStressStats {
        seeds: 1,
        ..InboxStressStats::default()
    };
    let mut digest = FNV_OFFSET;
    let mut wrong = Vec::new();
    for case in CASES {
        if only.is_some_and(|only| format!("{case:?}") != only) {
            continue;
        }
        let (run, calls, moved) = simulate_case(seed, case, mutation);
        stats.cases += 1;
        stats.epochs += run.epochs;
        stats.crashes += run.crashes;
        stats.model_calls += calls;
        stats.moved += u64::from(moved);
        stats.refused += u64::from(run.refused);
        digest = fnv(
            digest,
            format!(
                "{case:?} {} {} {calls} {moved} {}",
                run.epochs, run.crashes, run.refused
            )
            .as_bytes(),
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
    mutation: InboxStressMutation,
    twice: bool,
    verbose: bool,
    totals: &mut InboxStressStats,
) -> Option<String> {
    let only = std::env::var("INBOX_STRESS_CASE").ok();
    let first = simulate(seed, only.as_deref(), mutation);
    totals.add(&first.stats);
    if verbose {
        println!("seed {seed} digest {:016x} {:?}", first.digest, first.stats);
    }
    if first.wrong.is_some() || !twice {
        return first.wrong;
    }
    let second = simulate(seed, only.as_deref(), mutation);
    (second.digest != first.digest || second.wrong.is_some()).then(|| {
        format!(
            "nondeterminism: digest {:016x} then {:016x}",
            first.digest, second.digest
        )
    })
}

pub fn replay(seed: u64, mutation: InboxStressMutation) -> String {
    let mut command = format!("nanocode-sim --scenario inbox_stress --seed {seed}");
    if mutation != InboxStressMutation::None {
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
    mutation: InboxStressMutation,
    totals: &InboxStressStats,
) -> ExitCode {
    println!(
        "RESULT seeds={}..{} scenario=inbox_stress mutation={} passed={passed} failed={failed} twice={twice} stats={totals:?}",
        seeds.start,
        seeds.end,
        mutation.name()
    );
    match mutation {
        InboxStressMutation::None if failed == 0 => ExitCode::SUCCESS,
        InboxStressMutation::None => ExitCode::FAILURE,
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
