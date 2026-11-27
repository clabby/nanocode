use std::{
    borrow::Cow,
    collections::{BTreeSet, HashMap, HashSet},
    future::{Future, poll_fn},
    pin::pin,
    sync::{
        Arc, Weak,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    task::Poll,
    time::Duration,
};

use nanocode_sync::{Atom, Signal, Slot};
use nanocode_tool_contracts::{ToolOutput, ToolReceipt};

use nanocode_sync::{
    FutureExt, StreamExt,
    future::{AbortHandle, Abortable, Either, select},
    stream::FuturesUnordered,
};
use nanocode_sync::{mpsc, oneshot};
use serde_json::Value;

use super::{
    Answers, HAND_RESTARTED, LOCAL_TOOLS_UNKNOWN, NOT_RECEIVED, REQUEST_IN_USE, ReceivedCalls,
    RemoteCodeModeHost, RemoteContext, RemoteFrame as Frame, RemoteOperation, RemoteTransport,
    answer_key, asked_key, encode_output, hand::fit_images, local_tools_key,
};
use crate::{
    Tool, ToolDefinition, ToolInput,
    code_mode_spec::{DEFAULT_YIELD, source_yielding, wait_yielding},
    embedded::{
        CodeModeExecution, CodeModeHostError, CodeModeObserver, HandCalls, HostFuture,
        nested_tool_input,
    },
};

pub const PEER_CLOSED: &str = "runner disconnected";
pub const LOCAL_CLOSED: &str = "remote connection closed";

const ACK_DELAY: Duration = Duration::from_millis(10);

pub type ResumedReply = HostFuture<'static, Result<Option<Value>, CodeModeHostError>>;

pub type CallReply = HostFuture<'static, Result<Value, CodeModeHostError>>;

pub type RemoteReply = Result<Option<Value>, String>;

pub struct RemoteRequest {
    pub id: u64,
    pub session_id: String,
    pub call_id: String,
    pub turn_id: Option<String>,
}

pub struct Carried {
    pub host: usize,
    pub statements: Vec<(&'static str, Vec<String>)>,
}

pub trait RemoteBarrier: Send + Sync + 'static {
    fn settle(&self) -> HostFuture<'_, Result<(), String>>;
    fn carry(&self, carried: Carried) -> HostFuture<'_, Result<bool, String>>;
    fn ride(&self, _carried: Carried) -> HostFuture<'static, bool> {
        Box::pin(async { false })
    }
}

pub trait RemoteLedger: Send + Sync + 'static {
    fn sent(&self, request: RemoteRequest) -> HostFuture<'_, Result<(), String>>;
    fn sent_after<'a>(
        &'a self,
        request: RemoteRequest,
        barrier: &'a dyn RemoteBarrier,
    ) -> HostFuture<'a, Result<(), String>> {
        Box::pin(async move {
            barrier.settle().await?;
            self.sent(request).await
        })
    }
    fn replied<'a>(&'a self, id: u64, reply: &'a RemoteReply)
    -> HostFuture<'a, Result<(), String>>;
    fn rider(&self, _id: u64, _reply: &RemoteReply) -> Option<Carried> {
        None
    }
    fn rode(&self) {}
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RemoteDelivery {
    Durable,
    Immediate,
}

pub trait RemoteLink: Send + Sync + 'static {
    fn supports_tool_selection(&self) -> bool {
        false
    }

    fn caches_local_tools(&self) -> bool {
        false
    }

    fn send(
        &self,
        frames: Vec<Frame>,
        delivery: RemoteDelivery,
    ) -> HostFuture<'_, Result<(), CodeModeHostError>>;
    fn send_encoded(
        &self,
        frames: Vec<(Frame, Option<String>)>,
        delivery: RemoteDelivery,
    ) -> HostFuture<'_, Result<(), CodeModeHostError>> {
        self.send(
            frames.into_iter().map(|(frame, _)| frame).collect(),
            delivery,
        )
    }
    fn receive(&self) -> HostFuture<'_, Result<Vec<Frame>, CodeModeHostError>>;
    fn close<'a>(&'a self, reason: &'a str) -> HostFuture<'a, ()>;
    fn changed(&self) {}
    fn max_frame_bytes(&self) -> Option<usize> {
        None
    }
    fn resumes(&self) -> Option<String> {
        None
    }
}

#[derive(Default)]
struct Observer {
    attached: AtomicBool,
    current: Slot<Box<dyn CodeModeObserver>>,
}

impl Observer {
    fn attach(&self, observer: Box<dyn CodeModeObserver>) {
        self.current.put(observer);
        self.attached.store(true, Ordering::SeqCst);
    }

    fn deliver(
        &self,
        update: Value,
        answers: &Arc<Answers>,
        session_id: &str,
    ) -> Result<(), CodeModeHostError> {
        let Some(mut observer) = self.current.take() else {
            return Ok(());
        };
        let delivered = ReceivedCalls::default()
            .answered(Arc::clone(answers), session_id)
            .deliver(Some(observer.as_mut()), update);
        if let Some(newer) = self.current.put(observer) {
            self.current.put(newer);
        }
        delivered
    }
}

#[derive(Clone)]
struct CallbackState {
    tools: Vec<Arc<dyn Tool>>,
    context: RemoteContext,
    generation: u64,
    observer: Arc<Observer>,
}

type Callbacks = HashMap<(String, String), CallbackState>;

#[derive(Clone)]
enum Adoption {
    Awaited(Arc<Signal>),
    Held {
        tools: Vec<Arc<dyn Tool>>,
        context: RemoteContext,
    },
}

type Adoptions = HashMap<String, Adoption>;

type HeldReceipts = Arc<Atom<Vec<Arc<dyn ToolReceipt>>>>;
type Receipts = HashMap<(String, String), HeldReceipts>;

type Sending<'a> = (
    HostFuture<'a, Result<(), CodeModeHostError>>,
    Vec<u64>,
    Vec<u64>,
);

type Encoded = (Frame, Option<String>);

#[derive(Clone)]
struct Answered {
    session_id: String,
    result: Result<Value, String>,
    text: Option<String>,
    delivery: RemoteDelivery,
}

#[derive(Clone)]
struct Waiter {
    replies: mpsc::UnboundedSender<(Frame, bool)>,
    written: Arc<AtomicBool>,
}

type Waiters = HashMap<u64, Waiter>;

enum Outgoing {
    Send {
        frames: Vec<(Frame, Option<String>)>,
        delivery: RemoteDelivery,
        waiters: Vec<(u64, Waiter)>,
        idle: bool,
    },
    Forget(u64),
    Probe(u64),
    Boundary(Frame),
}

impl Outgoing {
    fn frames(frames: Vec<Frame>, delivery: RemoteDelivery) -> Self {
        Self::Send {
            frames: frames.into_iter().map(|frame| (frame, None)).collect(),
            delivery,
            idle: false,
            waiters: Vec::new(),
        }
    }
}

fn waiter() -> (Waiter, mpsc::UnboundedReceiver<(Frame, bool)>) {
    let (replies, receiver) = mpsc::unbounded_channel();
    (
        Waiter {
            replies,
            written: Arc::default(),
        },
        receiver,
    )
}

fn works(operation: &RemoteOperation) -> Option<&str> {
    match operation {
        RemoteOperation::Execute { context, .. }
        | RemoteOperation::Wait { context, .. }
        | RemoteOperation::Call { context, .. } => Some(&context.session_id),
        _ => None,
    }
}

fn scoped(operation: &RemoteOperation) -> Option<&str> {
    match operation {
        RemoteOperation::Execute { context, .. }
        | RemoteOperation::Wait { context, .. }
        | RemoteOperation::Call { context, .. } => Some(&context.session_id),
        RemoteOperation::DetachTurn { session_id }
        | RemoteOperation::CancelTurn { session_id, .. }
        | RemoteOperation::Cancel { session_id }
        | RemoteOperation::CancelCall { session_id, .. }
        | RemoteOperation::BeginTurn { session_id }
        | RemoteOperation::Preempt { session_id }
        | RemoteOperation::SettlePreempt { session_id }
        | RemoteOperation::EndTurn { session_id, .. }
        | RemoteOperation::Notices { session_id, .. } => Some(session_id),
        RemoteOperation::Capabilities
        | RemoteOperation::RequestCapability { .. }
        | RemoteOperation::HostInfo => None,
    }
}

const fn boundary(operation: &RemoteOperation) -> bool {
    matches!(
        operation,
        RemoteOperation::BeginTurn { .. } | RemoteOperation::SettlePreempt { .. }
    )
}

fn hold(boundaries: &mut HashMap<String, Vec<Frame>>, frame: Frame) {
    let Frame::Request { operation, .. } = &frame else {
        return;
    };
    let Some(session) = scoped(operation) else {
        return;
    };
    let kind = std::mem::discriminant(operation);
    let held = boundaries.entry(session.to_owned()).or_default();
    held.retain(|kept| {
        !matches!(kept, Frame::Request { operation, .. } if std::mem::discriminant(operation) == kind)
    });
    held.push(frame);
}

fn quiet(
    quiet: &mut HashSet<String>,
    cancelling: &mut HashMap<u64, String>,
    frames: Vec<(Frame, Option<String>)>,
    skipped: &mut Vec<u64>,
) -> Vec<(Frame, Option<String>)> {
    frames
        .into_iter()
        .filter(|(frame, _)| {
            let Frame::Request { id, operation, .. } = frame else {
                return true;
            };
            let Some(session) = scoped(operation) else {
                return true;
            };
            if boundary(operation) {
                return true;
            }
            let turn = matches!(operation, RemoteOperation::CancelTurn { turn_id: None, .. });
            if turn && quiet.contains(session) {
                skipped.push(*id);
                return false;
            }
            quiet.remove(session);
            cancelling.retain(|_, cancelled| cancelled != session);
            if turn || matches!(operation, RemoteOperation::Cancel { .. }) {
                cancelling.insert(*id, session.to_owned());
            }
            true
        })
        .collect()
}

fn release(
    boundaries: &mut HashMap<String, Vec<Frame>>,
    frames: Vec<(Frame, Option<String>)>,
) -> Vec<(Frame, Option<String>)> {
    if boundaries.is_empty() {
        return frames;
    }
    let mut released = Vec::with_capacity(frames.len());
    for (frame, key) in frames {
        if let Frame::Request { operation, .. } = &frame
            && let Some(session) = scoped(operation)
            && let Some(held) = boundaries.remove(session)
        {
            released.extend(held.into_iter().map(|held| (held, None)));
        }
        released.push((frame, key));
    }
    released
}

fn stops(operation: &RemoteOperation) -> Option<&str> {
    match operation {
        RemoteOperation::Cancel { session_id }
        | RemoteOperation::CancelTurn {
            session_id,
            turn_id: None,
        } => Some(session_id),
        _ => None,
    }
}

fn answer(ids: &[u64], pending: &mut Waiters) {
    for id in ids {
        if let Some(waiter) = pending.remove(id) {
            let _ = waiter.replies.send((
                Frame::Reply {
                    id: *id,
                    result: Ok(None),
                },
                false,
            ));
        }
    }
}

fn mark_written(pending: &Waiters, ids: &[u64]) {
    for id in ids {
        if let Some(waiter) = pending.get(id) {
            waiter.written.store(true, Ordering::SeqCst);
        }
    }
}

struct Client {
    outgoing: mpsc::UnboundedSender<Outgoing>,
    callbacks: Atom<Callbacks>,
    adoptions: Atom<Adoptions>,
    receipts: Atom<Receipts>,
    answers: Arc<Answers>,
    running: Atom<nanocode_sync::Map<u64, (String, AbortHandle)>>,
    next: AtomicU64,
    turns: Atom<nanocode_sync::Map<String, u64>>,
    operation_timeout: Option<Duration>,
    failure: Atom<Option<String>>,
    hands: Atom<Option<Arc<dyn HandCalls>>>,
    max_frame_bytes: Option<usize>,
    resumes: Option<String>,
    settles: bool,
    tool_selection: bool,
    caches_local_tools: bool,
    local_tools_keyed: Atom<Option<(Vec<ToolDefinition>, String)>>,
    ledger: Option<Arc<dyn RemoteLedger>>,
    unacked: Atom<BTreeSet<u64>>,
    served: Atom<nanocode_sync::Map<u64, Arc<Answered>>>,
    _stop: oneshot::Sender<()>,
}

#[derive(Clone)]
pub struct RemoteClient {
    client: Arc<Client>,
}

#[derive(Clone)]
pub struct RemoteActivity(Weak<Client>);

fn error(value: impl std::fmt::Display) -> CodeModeHostError {
    CodeModeHostError::new(value.to_string())
}

fn replied(message: String) -> CodeModeHostError {
    if message == NOT_RECEIVED || message == REQUEST_IN_USE {
        CodeModeHostError::unsent(message)
    } else {
        CodeModeHostError::new(message)
    }
}

struct Resumed {
    client: Weak<Client>,
    id: u64,
}

impl Drop for Resumed {
    fn drop(&mut self) {
        if let Some(client) = self.client.upgrade() {
            let _ = client.outgoing.send(Outgoing::Forget(self.id));
        }
    }
}

fn executed(result: RemoteReply) -> Result<RemoteReply, CodeModeHostError> {
    match result {
        Ok(Some(mut value)) => {
            ReceivedCalls::default().execution(&mut value)?;
            Ok(Ok(Some(value)))
        }
        other => Ok(other),
    }
}

fn orphaned(
    client: Weak<Client>,
    id: u64,
    mut replies: mpsc::UnboundedReceiver<(Frame, bool)>,
) -> HostFuture<'static, ()> {
    Box::pin(async move {
        while let Some((frame, from_hand)) = replies.recv().await {
            let Frame::Reply { result, .. } = frame else {
                continue;
            };
            if from_hand
                && let Ok(result) = executed(result)
                && let Some(client) = client.upgrade()
            {
                client.settle_reply_behind(id, &result, None);
            }
            return;
        }
    })
}

fn resumed(
    client: Weak<Client>,
    id: u64,
    mut replies: mpsc::UnboundedReceiver<(Frame, bool)>,
) -> ResumedReply {
    Box::pin(async move {
        let _resumed = Resumed {
            client: Weak::clone(&client),
            id,
        };
        while let Some((frame, from_hand)) = replies.recv().await {
            match frame {
                Frame::Reply { result, .. } => {
                    let result = match result {
                        Ok(Some(mut value)) => {
                            ReceivedCalls::default().execution(&mut value)?;
                            Ok(Some(value))
                        }
                        other => other,
                    };
                    if from_hand && let Some(client) = client.upgrade() {
                        client.settle_reply_behind(id, &result, None);
                    }
                    return result.map_err(replied);
                }
                Frame::Update { .. } => {}
                _ => return Err(error("unexpected operation reply")),
            }
        }
        Err(error(
            "remote reply channel closed; operation outcome unknown",
        ))
    })
}

async fn adopted(
    client: &Weak<Client>,
    session_id: &str,
    name: &str,
) -> Option<(Option<Arc<dyn Tool>>, RemoteContext)> {
    let timeout = client.upgrade()?.operation_timeout;
    let held = async {
        loop {
            let signal = match client.upgrade()?.adoption(session_id) {
                Adoption::Held { tools, context } => {
                    let tool = tools
                        .into_iter()
                        .find(|tool| tool.definition().name() == name);
                    return Some((tool, context));
                }
                Adoption::Awaited(signal) => signal,
            };
            signal.wait().await;
        }
    };
    let Some(timeout) = timeout else {
        return held.await;
    };
    match select(pin!(held), pin!(nanocode_platform::sleep(timeout))).await {
        Either::Left((held, _)) => held,
        Either::Right(_) => None,
    }
}

fn origin(call_id: &str) -> &str {
    call_id
        .rsplit_once("/code-")
        .map_or(call_id, |(origin, _)| origin)
}

fn fits(
    frame: &Frame,
    max_frame_bytes: Option<usize>,
) -> Result<Option<String>, CodeModeHostError> {
    max_frame_bytes.map(|limit| frame.encode(limit)).transpose()
}

const fn delivery(operation: &RemoteOperation) -> RemoteDelivery {
    match operation {
        RemoteOperation::Execute { .. } | RemoteOperation::Cancel { .. } => RemoteDelivery::Durable,
        _ => RemoteDelivery::Immediate,
    }
}

fn name_yield(operation: &mut RemoteOperation) {
    match operation {
        RemoteOperation::Execute { source, .. } => {
            if let Cow::Owned(named) = source_yielding(source, DEFAULT_YIELD) {
                *source = named;
            }
        }
        RemoteOperation::Wait { input, .. } => {
            if let Cow::Owned(named) = wait_yielding(input, DEFAULT_YIELD) {
                *input = named;
            }
        }
        _ => {}
    }
}

fn requested_yield(operation: &RemoteOperation) -> Duration {
    if let RemoteOperation::Call {
        run_time_ms: Some(run_time),
        ..
    } = operation
    {
        return Duration::from_millis(*run_time);
    }
    let requested = match operation {
        RemoteOperation::Execute { source, .. } => source
            .lines()
            .next()
            .and_then(|line| line.trim_start().strip_prefix("// @exec:"))
            .and_then(|pragma| serde_json::from_str::<Value>(pragma).ok()),
        RemoteOperation::Wait { input, .. } => serde_json::from_str::<Value>(input).ok(),
        RemoteOperation::Call { input, .. } => Some(input.clone()),
        RemoteOperation::Notices { yield_time_ms, .. } => {
            return Duration::from_millis(*yield_time_ms);
        }
        _ => None,
    };
    requested
        .and_then(|value| value.get("yield_time_ms")?.as_u64())
        .map_or(Duration::ZERO, Duration::from_millis)
}

impl RemoteActivity {
    #[must_use]
    pub fn is_alive(&self) -> bool {
        self.0.strong_count() > 0
    }

    #[must_use]
    pub fn busy(&self) -> bool {
        self.0.upgrade().is_some_and(|client| {
            !client.running.read(nanocode_sync::Map::is_empty)
                || client.callbacks.read(|callbacks| {
                    callbacks
                        .values()
                        .any(|state| state.observer.attached.load(Ordering::SeqCst))
                })
        })
    }
}

impl RemoteClient {
    pub fn new(
        link: impl RemoteLink,
        operation_timeout: Option<Duration>,
    ) -> (Self, HostFuture<'static, String>) {
        let (client, driver, _) = Self::open(link, operation_timeout, Vec::new(), false, None);
        (client, driver)
    }

    pub fn resuming(
        link: impl RemoteLink,
        operation_timeout: Option<Duration>,
        pending: Vec<u64>,
    ) -> (Self, HostFuture<'static, String>, Vec<(u64, ResumedReply)>) {
        Self::open(link, operation_timeout, pending, true, None)
    }

    pub fn durable(
        link: impl RemoteLink,
        operation_timeout: Option<Duration>,
        pending: Vec<u64>,
        ledger: Arc<dyn RemoteLedger>,
    ) -> (Self, HostFuture<'static, String>, Vec<(u64, ResumedReply)>) {
        Self::open(link, operation_timeout, pending, true, Some(ledger))
    }

    fn open(
        link: impl RemoteLink,
        operation_timeout: Option<Duration>,
        pending: Vec<u64>,
        settles: bool,
        ledger: Option<Arc<dyn RemoteLedger>>,
    ) -> (Self, HostFuture<'static, String>, Vec<(u64, ResumedReply)>) {
        let (outgoing, frames) = mpsc::unbounded_channel();
        let (stop, stopped) = oneshot::channel();
        let max_frame_bytes = link.max_frame_bytes();
        let resumes = link.resumes();
        let tool_selection = link.supports_tool_selection();
        let caches_local_tools = link.caches_local_tools();
        let mut waiters = HashMap::new();
        let mut receivers = Vec::new();
        if resumes.is_some() {
            for id in &pending {
                let (replies, receiver) = mpsc::unbounded_channel();
                waiters.insert(
                    *id,
                    Waiter {
                        replies,
                        written: Arc::new(AtomicBool::new(true)),
                    },
                );
                receivers.push((*id, receiver));
            }
            let _ = outgoing.send(Outgoing::frames(
                vec![Frame::Resume { pending }],
                RemoteDelivery::Immediate,
            ));
        } else {
            for id in pending {
                let (replies, receiver) = mpsc::unbounded_channel();
                let _ = replies.send((
                    Frame::Reply {
                        id,
                        result: Err(HAND_RESTARTED.to_owned()),
                    },
                    false,
                ));
                receivers.push((id, receiver));
            }
            let _ = outgoing.send(Outgoing::frames(
                vec![Frame::Ack { ids: Vec::new() }],
                RemoteDelivery::Immediate,
            ));
        }
        let client = Arc::new(Client {
            outgoing,
            callbacks: Atom::default(),
            adoptions: Atom::default(),
            receipts: Atom::default(),
            answers: Arc::default(),
            running: Atom::default(),
            next: AtomicU64::new(nanocode_platform::uuid_v4().as_u64_pair().0 >> 12),
            turns: Atom::default(),
            operation_timeout,
            failure: Atom::default(),
            hands: Atom::default(),
            max_frame_bytes,
            resumes,
            settles,
            tool_selection,
            caches_local_tools,
            local_tools_keyed: Atom::default(),
            ledger,
            unacked: Atom::default(),
            served: Atom::default(),
            _stop: stop,
        });
        let resumed = receivers
            .into_iter()
            .map(|(id, receiver)| (id, resumed(Arc::downgrade(&client), id, receiver)))
            .collect();
        let driver = Box::pin(drive(
            Arc::downgrade(&client),
            link,
            frames,
            stopped,
            waiters,
        ));
        (Self { client }, driver, resumed)
    }

    pub fn probe(&self, id: u64) -> Result<(), CodeModeHostError> {
        self.client
            .outgoing
            .send(Outgoing::Probe(id))
            .map_err(error)
    }

    pub fn ack(&self, ids: Vec<u64>) -> Result<(), CodeModeHostError> {
        self.client
            .outgoing
            .send(Outgoing::frames(
                vec![Frame::Ack { ids }],
                RemoteDelivery::Immediate,
            ))
            .map_err(error)
    }

    #[must_use]
    pub fn activity(&self) -> RemoteActivity {
        RemoteActivity(Arc::downgrade(&self.client))
    }

    #[must_use]
    pub fn failure(&self) -> Option<String> {
        Option::clone(&self.client.failure.get())
    }

    #[must_use]
    pub fn into_host(self, definitions: Vec<ToolDefinition>) -> RemoteCodeModeHost {
        RemoteCodeModeHost::new(self, definitions)
    }

    pub fn calls(&self, calls: Vec<(u64, RemoteOperation)>) -> Vec<CallReply> {
        if self.client.ledger.is_some() {
            return calls
                .into_iter()
                .map(|_| -> CallReply {
                    Box::pin(async {
                        Err(CodeModeHostError::unsent(
                            "a durable client records each call before it dispatches it; the operation was not dispatched",
                        ))
                    })
                })
                .collect();
        }
        let mut frames = Vec::new();
        let mut waiters = Vec::new();
        let mut replies = Vec::new();
        for (id, operation) in calls {
            match prepare_call(&self.client, id, operation, None) {
                Ok((frame, waiter, reply)) => {
                    frames.push(frame);
                    waiters.push(waiter);
                    replies.push(reply);
                }
                Err(issue) => replies.push(Box::pin(async move { Err(issue) })),
            }
        }
        if !frames.is_empty()
            && self
                .client
                .outgoing
                .send(Outgoing::Send {
                    frames,
                    delivery: RemoteDelivery::Immediate,
                    waiters,
                    idle: false,
                })
                .is_err()
        {
            return replies
                .into_iter()
                .map(|reply| -> CallReply {
                    drop(reply);
                    Box::pin(async {
                        Err(CodeModeHostError::unsent(
                            "remote connection closed; the operation was not dispatched",
                        ))
                    })
                })
                .collect();
        }
        replies
    }
}

fn prepare_call(
    client: &Arc<Client>,
    id: u64,
    operation: RemoteOperation,
    barrier: Option<Arc<dyn RemoteBarrier>>,
) -> Result<(Encoded, (u64, Waiter), CallReply), CodeModeHostError> {
    client.not_dispatched()?;
    let requested = requested_yield(&operation);
    let frame = client.request_frame(id, operation);
    let text = fits(&frame, client.max_frame_bytes).map_err(|issue| {
        CodeModeHostError::unsent(format!("{issue}; the operation was not dispatched"))
    })?;
    let frame = (frame, text);
    let (registered, mut replies) = waiter();
    let guard = Pending {
        client: Arc::clone(client),
        id,
        session_id: None,
        origin: None,
        complete: false,
    };
    let reply = Box::pin(async move {
        let reply = async {
            while let Some((frame, from_hand)) = replies.recv().await {
                match frame {
                    Frame::Reply { result, .. } => {
                        if from_hand {
                            guard
                                .client
                                .settle_reply_behind(id, &result, barrier.as_deref());
                        }
                        return result
                            .map_err(replied)?
                            .ok_or_else(|| error("hand omitted the tool result"));
                    }
                    Frame::Update { .. } => {}
                    _ => return Err(error("unexpected operation reply")),
                }
            }
            Err(error(
                "remote reply channel closed; operation outcome unknown",
            ))
        };
        guard
            .client
            .deadline(
                reply,
                requested,
                "remote operation deadline elapsed; outcome unknown; no retry",
            )
            .await?
    });
    Ok((frame, (id, registered), reply))
}

fn recorded(operation: &RemoteOperation) -> Option<(String, String, Option<String>)> {
    match operation {
        RemoteOperation::Execute { context, .. }
        | RemoteOperation::Wait { context, .. }
        | RemoteOperation::Call { context, .. } => Some((
            context.session_id.clone(),
            context.call_id.clone(),
            context.turn_id.clone(),
        )),
        _ => None,
    }
}

impl Client {
    async fn record(
        &self,
        id: u64,
        recorded: Option<&(String, String, Option<String>)>,
        barrier: Option<&dyn RemoteBarrier>,
    ) -> Result<(), CodeModeHostError> {
        let (Some(ledger), Some((session_id, call_id, turn_id))) = (&self.ledger, recorded) else {
            if let Some(barrier) = barrier {
                barrier.settle().await.map_err(|issue| {
                    CodeModeHostError::unsent(format!(
                        "the turn log was not saved: {issue}; the operation was not dispatched"
                    ))
                })?;
            }
            return Ok(());
        };
        let request = RemoteRequest {
            id,
            session_id: session_id.clone(),
            call_id: call_id.clone(),
            turn_id: turn_id.clone(),
        };
        let sent = match barrier {
            Some(barrier) => ledger.sent_after(request, barrier).await,
            None => ledger.sent(request).await,
        };
        sent.map_err(|issue| {
            CodeModeHostError::unsent(format!(
                "the request was not recorded: {issue}; the operation was not dispatched"
            ))
        })
    }

    fn settle_reply_behind(
        self: &Arc<Self>,
        id: u64,
        reply: &RemoteReply,
        barrier: Option<&dyn RemoteBarrier>,
    ) {
        let Some(ledger) = &self.ledger else {
            return;
        };
        let ride = barrier
            .and_then(|barrier| ledger.rider(id, reply).map(|carried| barrier.ride(carried)));
        let client = Arc::clone(self);
        let reply = reply.clone();
        nanocode_platform::spawn(async move { client.settle_reply(id, &reply, ride).await });
    }

    async fn settle_reply(
        &self,
        id: u64,
        reply: &RemoteReply,
        ride: Option<HostFuture<'static, bool>>,
    ) {
        let Some(ledger) = &self.ledger else {
            return;
        };
        let rode = match ride {
            Some(ride) => ride.await,
            None => false,
        };
        let recorded = if rode {
            ledger.rode();
            Ok(())
        } else {
            ledger.replied(id, reply).await
        };
        match recorded {
            Ok(()) if self.resumes.is_some() => {
                self.unacked.update(|unacked| unacked.remove(&id));
                let _ = self.outgoing.send(Outgoing::frames(
                    vec![Frame::Ack { ids: vec![id] }],
                    RemoteDelivery::Immediate,
                ));
            }
            Ok(()) => {}
            Err(issue) => {
                nanocode_trace::error!(%issue, id, "remote reply not recorded; the hand keeps it");
            }
        }
    }

    fn orphan(
        self: &Arc<Self>,
        id: u64,
        waiter: &Waiter,
    ) -> Option<(Waiter, HostFuture<'static, ()>)> {
        if self.ledger.is_none() || self.resumes.is_none() {
            return None;
        }
        let (replies, receiver) = mpsc::unbounded_channel();
        let kept = Waiter {
            replies,
            written: Arc::clone(&waiter.written),
        };
        Some((kept, orphaned(Arc::downgrade(self), id, receiver)))
    }

    fn probe(
        self: &Arc<Self>,
        id: u64,
        pending: &mut Waiters,
        callbacks: &mut FuturesUnordered<HostFuture<'static, ()>>,
    ) -> Option<Frame> {
        if self.ledger.is_none()
            || self.resumes.is_none()
            || pending.contains_key(&id)
            || self.unacked.read(|unacked| unacked.contains(&id))
        {
            return None;
        }
        let mut ids: Vec<u64> = pending
            .iter()
            .filter(|(_, waiter)| waiter.written.load(Ordering::SeqCst))
            .map(|(id, _)| *id)
            .collect();
        ids.extend(
            self.unacked
                .read(|unacked| unacked.iter().copied().collect::<Vec<_>>()),
        );
        ids.push(id);
        ids.sort_unstable();
        ids.dedup();
        let (replies, receiver) = mpsc::unbounded_channel();
        pending.insert(
            id,
            Waiter {
                replies,
                written: Arc::new(AtomicBool::new(true)),
            },
        );
        callbacks.push(orphaned(Arc::downgrade(self), id, receiver));
        Some(Frame::Resume { pending: ids })
    }

    fn adoption(&self, session_id: &str) -> Adoption {
        self.adoptions.update(|adoptions| {
            adoptions
                .entry(session_id.to_owned())
                .or_insert_with(|| Adoption::Awaited(Arc::default()))
                .clone()
        })
    }

    fn dispatch(
        &self,
        frame: (Frame, Option<String>),
        delivery: RemoteDelivery,
        waiter: Option<(u64, Waiter)>,
    ) -> Result<(), CodeModeHostError> {
        let idle = match &frame.0 {
            Frame::Request { operation, .. } => stops(operation).is_some_and(|session_id| {
                self.callbacks
                    .read(|callbacks| !callbacks.keys().any(|(session, _)| session == session_id))
                    && self
                        .running
                        .read(|running| !running.values().any(|(session, _)| session == session_id))
            }),
            _ => false,
        };
        if let (
            Frame::Request {
                operation: RemoteOperation::Cancel { session_id },
                ..
            },
            _,
        ) = &frame
        {
            self.callbacks
                .update(|callbacks| callbacks.retain(|(session, _), _| session != session_id));
            self.adoptions.update(|adoptions| {
                adoptions.remove(session_id);
            });
            self.receipts
                .update(|receipts| receipts.retain(|(session, _), _| session != session_id));
            self.served
                .update(|served| served.retain(|_, answered| answered.session_id != *session_id));
            self.answers.forget(&answer_key(session_id, ""));
            self.turns.update(|turns| {
                turns.remove(session_id);
            });
            let cancelled = self.running.update(|running| {
                let cancelled = running
                    .iter()
                    .filter(|(_, (session, _))| session == session_id)
                    .map(|(id, (_, abort))| (*id, abort.clone()))
                    .collect::<Vec<_>>();
                for (id, _) in &cancelled {
                    running.remove(id);
                }
                cancelled
            });
            for (_, abort) in cancelled {
                abort.abort();
            }
        }
        self.outgoing
            .send(Outgoing::Send {
                frames: vec![frame],
                delivery,
                waiters: waiter.into_iter().collect(),
                idle,
            })
            .map_err(error)
    }

    fn receive(
        self: &Arc<Self>,
        frame: Frame,
        pending: &mut Waiters,
    ) -> Result<Option<HostFuture<'static, ()>>, CodeModeHostError> {
        match frame {
            Frame::Reply { id, result } => {
                if let Some(waiter) = pending.remove(&id) {
                    if self.ledger.is_some() && self.resumes.is_some() {
                        self.unacked.update(|unacked| unacked.insert(id));
                    }
                    let _ = waiter.replies.send((Frame::Reply { id, result }, true));
                } else if self.unacked.read(|unacked| unacked.contains(&id))
                    && let Ok(result) = executed(result)
                {
                    self.settle_reply_behind(id, &result, None);
                }
            }
            Frame::Update { id, .. } => {
                if let Some(waiter) = pending.get(&id) {
                    let _ = waiter.replies.send((frame, true));
                }
            }
            Frame::Catalog {
                runtime_id,
                resumable,
                ..
            } => {
                if !resumable || runtime_id.is_none() || runtime_id != self.resumes {
                    return Err(error(HAND_RESTARTED));
                }
                let mut pending: Vec<u64> = pending
                    .iter()
                    .filter(|(_, waiter)| waiter.written.load(Ordering::SeqCst))
                    .map(|(id, _)| *id)
                    .collect();
                pending.extend(
                    self.unacked
                        .read(|unacked| unacked.iter().copied().collect::<Vec<_>>()),
                );
                pending.sort_unstable();
                self.outgoing
                    .send(Outgoing::frames(
                        vec![Frame::Resume { pending }],
                        RemoteDelivery::Immediate,
                    ))
                    .map_err(error)?;
            }
            Frame::CallbackCancel { id } => {
                if let Some((_, abort)) = self.running.update(|running| running.remove(&id)) {
                    abort.abort();
                }
            }
            Frame::DetachedUpdate {
                session_id,
                origin_call_id,
                update,
            } => {
                let key = (session_id, origin_call_id);
                if let Some(observer) = self
                    .callbacks
                    .read(|callbacks| callbacks.get(&key).map(|state| Arc::clone(&state.observer)))
                {
                    observer.deliver(update, &self.answers, &key.0)?;
                }
            }
            Frame::DetachedComplete {
                session_id,
                origin_call_id,
            } => {
                let key = (session_id, origin_call_id);
                self.settle(key.0, key.1, None);
            }
            Frame::Callback {
                id,
                name,
                input,
                freeform,
                context,
                hand,
                compact,
                output_prefix,
                answers,
                asked,
            } => {
                if self.served_again(id) {
                    return Ok(None);
                }
                if asked {
                    let mut kept = input.clone();
                    if let (Some(hand), Value::Object(fields)) = (&hand, &mut kept) {
                        fields.insert("hand".into(), Value::String(hand.clone()));
                    }
                    self.answers.keep(
                        answer_key(&context.session_id, &asked_key(&context.call_id)),
                        kept,
                    );
                }
                return Ok(Some(self.callback(
                    id,
                    name,
                    input,
                    freeform,
                    context,
                    hand,
                    compact,
                    output_prefix,
                    answers,
                )));
            }
            _ => return Err(error("unexpected runner frame")),
        }
        Ok(None)
    }

    #[allow(clippy::too_many_arguments)]
    fn callback(
        self: &Arc<Self>,
        id: u64,
        name: String,
        input: Value,
        freeform: bool,
        context: RemoteContext,
        hand: Option<String>,
        compact: bool,
        output_prefix: bool,
        answers: bool,
    ) -> HostFuture<'static, ()> {
        let recorded = answers.then(|| answer_key(&context.session_id, &context.call_id));
        let answers = Arc::clone(&self.answers);
        let key = (
            context.session_id.clone(),
            origin(&context.call_id).to_owned(),
        );
        let local = self.callbacks.read(|callbacks| {
            callbacks.get(&key).map(|state| {
                (
                    state
                        .tools
                        .iter()
                        .find(|tool| tool.definition().name() == name)
                        .cloned(),
                    state.context.clone(),
                )
            })
        });
        let adopter = Arc::downgrade(self);
        let hands = Option::clone(&self.hands.get());
        let durable = self.ledger.is_some();
        let session_id = context.session_id.clone();
        let delivery = if hand.is_some() {
            RemoteDelivery::Immediate
        } else {
            RemoteDelivery::Durable
        };
        let (abort, registration) = AbortHandle::new_pair();
        self.running.update(|running| {
            running.insert(id, (context.session_id.clone(), abort.clone()));
        });
        let outgoing = self.outgoing.clone();
        let client = Arc::downgrade(self);
        let held = self
            .receipts
            .update(|receipts| Arc::downgrade(receipts.entry(key.clone()).or_default()));
        let max_frame_bytes = self.max_frame_bytes;
        Box::pin(async move {
            let hold = |output: &mut ToolOutput| {
                let receipts = output.take_receipts();
                if !receipts.is_empty()
                    && let Some(held) = held.upgrade()
                {
                    for receipt in &receipts {
                        receipt.carried_by(None);
                    }
                    held.update(|held| held.extend(receipts.iter().cloned()));
                }
                if let Some(recorded) = &recorded {
                    answers.record(recorded.clone(), output);
                }
            };
            let execution = Abortable::new(
                async move {
                    let local = match local {
                        Some(local) => Some(local),
                        None => adopted(&adopter, &context.session_id, &name).await,
                    };
                    if let Some(hand) = hand {
                        let (_, mut authoritative) = local.ok_or_else(|| {
                            error(format!(
                                "brain callback origin for `{name}` on hand `{hand}` unavailable"
                            ))
                        })?;
                        let hands = hands
                            .or_else(|| {
                                adopter
                                    .upgrade()
                                    .and_then(|client| Option::clone(&client.hands.get()))
                            })
                            .ok_or_else(|| {
                                error("this brain does not route calls between hands")
                            })?;
                        authoritative.call_id = context.call_id;
                        let owned = authoritative.owned();
                        let mut output = if durable {
                            hands
                                .call_once(
                                    &hand,
                                    &name,
                                    input,
                                    owned.as_context().with_deferred_output(),
                                )
                                .await
                        } else {
                            hands
                                .call(
                                    &hand,
                                    &name,
                                    input,
                                    owned.as_context().with_deferred_output(),
                                )
                                .await
                        };
                        hold(&mut output);
                        return encode_output(output, compact, output_prefix);
                    }
                    let Some((Some(tool), mut authoritative)) = local else {
                        return Err(error(format!("brain callback tool `{name}` unavailable")));
                    };
                    authoritative.call_id = context.call_id;
                    let owned = authoritative.owned();
                    let input = if freeform {
                        Ok(ToolInput::Freeform(
                            input
                                .as_str()
                                .ok_or_else(|| error("freeform callback must be a string"))?
                                .into(),
                        ))
                    } else {
                        nested_tool_input(&tool.definition(), &name, input)
                    };
                    let mut output = match input {
                        Ok(input) => tool
                            .execute(input, owned.as_context().with_deferred_output())
                            .await
                            .map_err(error)?,
                        Err(refused) => refused,
                    };
                    hold(&mut output);
                    encode_output(output, compact, output_prefix)
                },
                registration,
            );
            if let Ok(result) = execution.await {
                let mut frame = Frame::CallbackResult {
                    id,
                    result: result.map_err(|issue: CodeModeHostError| issue.to_string()),
                };
                let text = match fits(&frame, max_frame_bytes) {
                    Ok(text) => text,
                    Err(issue) => match max_frame_bytes.and_then(|limit| fit_images(&frame, limit))
                    {
                        Some((fitted, text)) => {
                            frame = fitted;
                            Some(text)
                        }
                        None => {
                            frame = Frame::CallbackResult {
                                id,
                                result: Err(format!(
                                    "{issue}; the brain callback's result was not sent"
                                )),
                            };
                            None
                        }
                    },
                };
                if let (Some(client), Frame::CallbackResult { result, .. }) =
                    (client.upgrade(), &frame)
                {
                    let answered = Arc::new(Answered {
                        session_id: session_id.clone(),
                        result: result.clone(),
                        text: text.clone(),
                        delivery,
                    });
                    client
                        .served
                        .update(|served| served.insert(id, Arc::clone(&answered)));
                }
                let _ = outgoing.send(Outgoing::Send {
                    frames: vec![(frame, text)],
                    delivery,
                    waiters: Vec::new(),
                    idle: false,
                });
            }
            if let Some(client) = client.upgrade() {
                client.running.update(|running| {
                    running.remove(&id);
                });
            }
        })
    }

    fn served_again(&self, id: u64) -> bool {
        if self.running.read(|running| running.contains_key(&id)) {
            return true;
        }
        let again = self.served.read(|served| served.get(&id).cloned());
        let Some(answered) = again else {
            return false;
        };
        let _ = self.outgoing.send(Outgoing::Send {
            frames: vec![(
                Frame::CallbackResult {
                    id,
                    result: answered.result.clone(),
                },
                answered.text.clone(),
            )],
            delivery: answered.delivery,
            waiters: Vec::new(),
            idle: false,
        });
        true
    }

    fn terminate(&self, message: &str, pending: &mut Waiters) {
        for (_, (_, abort)) in self.running.update(std::mem::take) {
            abort.abort();
        }
        self.callbacks.set(HashMap::new());
        self.adoptions.set(HashMap::new());
        self.receipts.set(HashMap::new());
        self.served.set(nanocode_sync::Map::new());
        self.answers.forget("");
        let uncertain = format!(
            "{message}; the dispatched operation has an unknown outcome and was not retried"
        );
        let unsent = format!("{message}; the operation was not dispatched");
        self.failure.set(Some(message.to_owned()));
        let mut ended: Vec<(u64, Waiter)> = pending.drain().collect();
        ended.sort_unstable_by_key(|(id, _)| *id);
        for (id, waiter) in ended {
            let result = if waiter.written.load(Ordering::SeqCst) {
                uncertain.clone()
            } else {
                unsent.clone()
            };
            let _ = waiter.replies.send((
                Frame::Reply {
                    id,
                    result: Err(result),
                },
                false,
            ));
        }
    }

    fn refuse(ids: &[u64], message: &str, pending: &mut Waiters) {
        let refused = format!("{message}; the operation was not dispatched");
        for (id, waiter) in ids
            .iter()
            .filter_map(|id| pending.remove(id).map(|waiter| (*id, waiter)))
        {
            let _ = waiter.replies.send((
                Frame::Reply {
                    id,
                    result: Err(refused.clone()),
                },
                false,
            ));
        }
    }

    fn settle(&self, session_id: String, origin: String, carrier: Option<(&str, Option<&str>)>) {
        self.answers
            .forget(&answer_key(&session_id, &format!("{origin}/")));
        let key = (session_id, origin);
        self.callbacks.update(|callbacks| {
            callbacks.remove(&key);
        });
        let held = self.receipts.update(|receipts| receipts.remove(&key));
        if let (Some(held), Some((call_id, turn_id))) = (held, carrier) {
            for receipt in held.update(std::mem::take) {
                receipt.carried_by_in_turn(call_id, turn_id);
            }
        }
    }

    fn local_tools_key(&self, definitions: &[ToolDefinition]) -> Option<String> {
        let known = self.local_tools_keyed.read(|keyed| {
            keyed
                .as_ref()
                .filter(|(known, _)| known.as_slice() == definitions)
                .map(|(_, key)| key.clone())
        });
        if known.is_some() {
            return known;
        }
        let key = local_tools_key(definitions)?;
        self.local_tools_keyed
            .set(Some((definitions.to_vec(), key.clone())));
        Some(key)
    }

    fn elide_local_tools(
        &self,
        operation: RemoteOperation,
    ) -> (RemoteOperation, Option<RemoteOperation>) {
        let RemoteOperation::Execute {
            source,
            context,
            local_tools,
            local_tools_key: None,
            hand,
            selected_tools,
        } = operation
        else {
            return (operation, None);
        };
        let key = if self.caches_local_tools && !local_tools.is_empty() {
            self.local_tools_key(&local_tools)
        } else {
            None
        };
        let Some(key) = key else {
            return (
                RemoteOperation::Execute {
                    source,
                    context,
                    local_tools,
                    local_tools_key: None,
                    hand,
                    selected_tools,
                },
                None,
            );
        };
        (
            RemoteOperation::Execute {
                source: source.clone(),
                context: context.clone(),
                local_tools: Vec::new(),
                local_tools_key: Some(key.clone()),
                hand: hand.clone(),
                selected_tools: selected_tools.clone(),
            },
            Some(RemoteOperation::Execute {
                source,
                context,
                local_tools,
                local_tools_key: Some(key),
                hand,
                selected_tools,
            }),
        )
    }

    const fn request_frame(&self, id: u64, operation: RemoteOperation) -> Frame {
        Frame::Request {
            id,
            operation,
            max_frame_bytes: self.max_frame_bytes,
            compact: true,
            output_prefix: true,
            resumable: self.resumes.is_some(),
            answers: true,
            asks: true,
            shared_images: true,
        }
    }

    fn not_dispatched(&self) -> Result<(), CodeModeHostError> {
        match Option::clone(&self.failure.get()) {
            Some(failure) => Err(error(format!(
                "{failure}; the operation was not dispatched"
            ))),
            None => Ok(()),
        }
    }

    async fn deadline<T>(
        &self,
        reply: impl Future<Output = T>,
        requested: Duration,
        elapsed: &str,
    ) -> Result<T, CodeModeHostError> {
        let Some(timeout) = self.operation_timeout else {
            return Ok(reply.await);
        };
        match select(
            pin!(reply),
            pin!(nanocode_platform::sleep(timeout.saturating_add(requested))),
        )
        .await
        {
            Either::Left((result, _)) => Ok(result),
            Either::Right(_) => Err(error(elapsed)),
        }
    }
}

async fn drive(
    client: Weak<Client>,
    link: impl RemoteLink,
    mut frames: mpsc::UnboundedReceiver<Outgoing>,
    mut stopped: oneshot::Receiver<()>,
    mut pending: Waiters,
) -> String {
    let mut callbacks = FuturesUnordered::new();
    let mut acks = Vec::new();
    let mut ack_due = None;
    let mut dispatched: HashMap<u64, String> = HashMap::new();
    let mut controls: HashSet<u64> = HashSet::new();
    let mut boundaries: HashMap<String, Vec<Frame>> = HashMap::new();
    let mut quieted: HashSet<String> = HashSet::new();
    let mut cancelling: HashMap<u64, String> = HashMap::new();
    let result = {
        let mut receiving = link.receive();
        let mut sending: Option<Sending<'_>> = None;
        let result = poll_fn(|cx| {
            loop {
                if stopped.poll_unpin(cx).is_ready() {
                    return Poll::Ready(Ok(()));
                }
                let mut progressed = false;
                if let Some((send, ids, settled)) = sending.as_mut() {
                    match send.as_mut().poll(cx) {
                        Poll::Ready(Ok(())) => {
                            mark_written(&pending, ids);
                            answer(settled, &mut pending);
                            sending = None;
                            progressed = true;
                        }
                        Poll::Ready(Err(issue))
                            if !issue.refused_requests().is_empty()
                                && issue.refused_requests().iter().all(|id| ids.contains(id)) =>
                        {
                            let refused = issue.refused_requests();
                            let sent: Vec<u64> = ids
                                .iter()
                                .copied()
                                .filter(|id| !refused.contains(id))
                                .collect();
                            mark_written(&pending, &sent);
                            Client::refuse(refused, issue.message(), &mut pending);
                            for id in refused {
                                dispatched.remove(id);
                            }
                            settled.retain(|id| !refused.contains(id));
                            answer(settled, &mut pending);
                            sending = None;
                            progressed = true;
                        }
                        Poll::Ready(Err(issue)) => {
                            if !issue.is_unsent() {
                                mark_written(&pending, ids);
                            }
                            sending = None;
                            return Poll::Ready(Err(issue));
                        }
                        Poll::Pending => {}
                    }
                }
                if sending.is_none() {
                    let mut batch = Vec::new();
                    let mut settled = Vec::new();
                    let mut delivery = RemoteDelivery::Immediate;
                    while let Poll::Ready(next) = frames.poll_recv(cx) {
                        match next {
                            None => return Poll::Ready(Ok(())),
                            Some(Outgoing::Send {
                                frames,
                                delivery: mut needs,
                                waiters,
                                idle,
                            }) => {
                                let mut skipped = Vec::new();
                                let frames =
                                    quiet(&mut quieted, &mut cancelling, frames, &mut skipped);
                                let frames = release(&mut boundaries, frames);
                                for (frame, _) in &frames {
                                    let Frame::Request { id, operation, .. } = frame else {
                                        continue;
                                    };
                                    if works(operation).is_none() {
                                        controls.insert(*id);
                                    }
                                    if let Some(session) = works(operation) {
                                        dispatched.insert(*id, session.to_owned());
                                    } else if let Some(session) = stops(operation)
                                        && idle
                                        && waiters.iter().any(|(waiting, _)| waiting == id)
                                        && !dispatched.values().any(|working| working == session)
                                    {
                                        needs = RemoteDelivery::Immediate;
                                        settled.push(*id);
                                    }
                                }
                                pending.extend(waiters);
                                answer(&skipped, &mut pending);
                                if needs == RemoteDelivery::Durable {
                                    delivery = needs;
                                }
                                batch.extend(frames);
                            }
                            Some(Outgoing::Forget(id)) => {
                                if let Some(waiter) = pending.remove(&id)
                                    && let Some(client) = client.upgrade()
                                    && let Some((kept, recorder)) = client.orphan(id, &waiter)
                                {
                                    pending.insert(id, kept);
                                    callbacks.push(recorder);
                                }
                            }
                            Some(Outgoing::Probe(id)) => {
                                if let Some(client) = client.upgrade()
                                    && let Some(resume) =
                                        client.probe(id, &mut pending, &mut callbacks)
                                {
                                    batch.push((resume, None));
                                }
                            }
                            Some(Outgoing::Boundary(frame)) => hold(&mut boundaries, frame),
                        }
                    }
                    if !acks.is_empty() {
                        let due = ack_due
                            .get_or_insert_with(|| Box::pin(nanocode_platform::sleep(ACK_DELAY)));
                        if !batch.is_empty() || due.as_mut().poll(cx).is_ready() {
                            ack_due = None;
                            batch.insert(
                                0,
                                (
                                    Frame::Ack {
                                        ids: std::mem::take(&mut acks),
                                    },
                                    None,
                                ),
                            );
                        }
                    }
                    if !batch.is_empty() {
                        let ids = batch
                            .iter()
                            .filter_map(|(frame, _)| match frame {
                                Frame::Request { id, .. } => Some(*id),
                                _ => None,
                            })
                            .collect();
                        sending = Some((link.send_encoded(batch, delivery), ids, settled));
                        continue;
                    }
                }
                match receiving.as_mut().poll(cx) {
                    Poll::Ready(Ok(received)) => {
                        let Some(client) = client.upgrade() else {
                            return Poll::Ready(Ok(()));
                        };
                        for frame in received {
                            if let Frame::Reply { id, result } = &frame {
                                if let Some(session) = cancelling.remove(id)
                                    && result.is_ok()
                                {
                                    quieted.insert(session);
                                }
                                dispatched.remove(id);
                                let control = controls.remove(id);
                                let unawaited = control || !pending.contains_key(id);
                                let recorded = client.ledger.is_some() && !control;
                                if client.resumes.is_some()
                                    && !recorded
                                    && (!client.settles || unawaited)
                                {
                                    acks.push(*id);
                                }
                            }
                            match client.receive(frame, &mut pending) {
                                Ok(Some(callback)) => callbacks.push(callback),
                                Ok(None) => {}
                                Err(issue) => return Poll::Ready(Err(issue)),
                            }
                        }
                        link.changed();
                        receiving = link.receive();
                        progressed = true;
                    }
                    Poll::Ready(Err(issue)) => return Poll::Ready(Err(issue)),
                    Poll::Pending => {}
                }
                while let Poll::Ready(Some(())) = callbacks.poll_next_unpin(cx) {
                    link.changed();
                    progressed = true;
                }
                if !progressed {
                    return Poll::Pending;
                }
            }
        })
        .await;
        if let Some((_, ids, _)) = sending.take() {
            mark_written(&pending, &ids);
        }
        result
    };
    drop(callbacks);
    frames.close();
    while let Ok(next) = frames.try_recv() {
        match next {
            Outgoing::Send { waiters, .. } => pending.extend(waiters),
            Outgoing::Forget(id) => {
                pending.remove(&id);
            }
            Outgoing::Probe(_) | Outgoing::Boundary(_) => {}
        }
    }
    let message = result.map_or_else(|issue| issue.to_string(), |()| LOCAL_CLOSED.to_owned());
    if let Some(client) = client.upgrade() {
        client.terminate(&message, &mut pending);
    }
    link.close(&message).await;
    message
}

struct Pending {
    client: Arc<Client>,
    id: u64,
    session_id: Option<String>,
    origin: Option<(String, String)>,
    complete: bool,
}

impl Drop for Pending {
    fn drop(&mut self) {
        let _ = self.client.outgoing.send(Outgoing::Forget(self.id));
        if !self.complete {
            if let Some(origin) = self.origin.take() {
                self.client.settle(origin.0, origin.1, None);
            }
            if let Some(session_id) = self.session_id.take() {
                let id = self.client.next.fetch_add(1, Ordering::Relaxed);
                let _ = self.client.outgoing.send(Outgoing::frames(
                    vec![self.client.request_frame(
                        id,
                        RemoteOperation::CancelTurn {
                            session_id,
                            turn_id: None,
                        },
                    )],
                    RemoteDelivery::Immediate,
                ));
            }
        }
    }
}

impl RemoteClient {
    async fn requested(
        &self,
        mut operation: RemoteOperation,
        tools: Vec<Arc<dyn Tool>>,
        mut observer: Option<&mut dyn CodeModeObserver>,
        barrier: Option<&dyn RemoteBarrier>,
    ) -> Result<Option<CodeModeExecution>, CodeModeHostError> {
        let client = &self.client;
        client.not_dispatched()?;
        if client.operation_timeout.is_some() {
            name_yield(&mut operation);
        }
        let executed = match &operation {
            RemoteOperation::Execute { context, .. } => Some(context.clone()),
            _ => None,
        };
        let origin_call_id = executed.as_ref().map(|context| context.call_id.clone());
        let carrier = match &operation {
            RemoteOperation::Execute { context, .. } | RemoteOperation::Wait { context, .. } => {
                Some((context.call_id.clone(), context.turn_id.clone()))
            }
            _ => None,
        };
        let session_id = match &operation {
            RemoteOperation::Execute { context, .. } | RemoteOperation::Wait { context, .. } => {
                Some(context.session_id.clone())
            }
            _ => None,
        };
        let requested = requested_yield(&operation);
        let delivery = delivery(&operation);
        let recorded = recorded(&operation);
        let (operation, mut full) = client.elide_local_tools(operation);
        let id = client.next.fetch_add(1, Ordering::Relaxed);
        let frame = client.request_frame(id, operation);
        let text = fits(&frame, client.max_frame_bytes)
            .map_err(|issue| error(format!("{issue}; the operation was not dispatched")))?;
        client.record(id, recorded.as_ref(), barrier).await?;
        if let Some(context) = executed {
            let generation = client
                .turns
                .read(|turns| turns.get(&context.session_id).copied())
                .unwrap_or(0);
            let key = (context.session_id.clone(), context.call_id.clone());
            let state = CallbackState {
                tools,
                context,
                generation,
                observer: Arc::default(),
            };
            client.callbacks.modify(|callbacks| {
                if callbacks.contains_key(&key) {
                    return Err(error("callback origin is already active"));
                }
                callbacks.insert(key.clone(), state.clone());
                Ok(())
            })?;
        }
        let (registered, mut replies) = waiter();
        let mut guard = Pending {
            client: Arc::clone(client),
            id,
            session_id: session_id.clone(),
            origin: session_id.clone().zip(origin_call_id.clone()),
            complete: false,
        };
        client
            .dispatch((frame, text), delivery, Some((id, registered)))
            .map_err(|_| error("remote connection closed; the operation was not dispatched"))?;
        let reply = async {
            let mut received = match &session_id {
                Some(session_id) => {
                    ReceivedCalls::default().answered(Arc::clone(&client.answers), session_id)
                }
                None => ReceivedCalls::default(),
            };
            let mut undelivered = None;
            while let Some((frame, from_hand)) = replies.recv().await {
                match frame {
                    Frame::Reply {
                        result: Err(message),
                        ..
                    } if message == LOCAL_TOOLS_UNKNOWN && full.is_some() => {
                        let Some(operation) = full.take() else {
                            continue;
                        };
                        if from_hand && client.ledger.is_some() {
                            let _ = client.outgoing.send(Outgoing::frames(
                                vec![Frame::Ack {
                                    ids: vec![guard.id],
                                }],
                                RemoteDelivery::Immediate,
                            ));
                        }
                        let id = client.next.fetch_add(1, Ordering::Relaxed);
                        let frame = client.request_frame(id, operation);
                        let text = fits(&frame, client.max_frame_bytes).map_err(|issue| {
                            error(format!("{issue}; the operation was not dispatched"))
                        })?;
                        client.record(id, recorded.as_ref(), barrier).await?;
                        let registered;
                        (registered, replies) = waiter();
                        guard.id = id;
                        client
                            .dispatch((frame, text), delivery, Some((id, registered)))
                            .map_err(|_| {
                                error("remote connection closed; the operation was not dispatched")
                            })?;
                    }
                    Frame::Reply { result, .. } => {
                        let result = match result {
                            Ok(Some(mut value)) => {
                                received.execution(&mut value)?;
                                Ok(Some(value))
                            }
                            other => other,
                        };
                        if from_hand {
                            client.settle_reply_behind(guard.id, &result, barrier);
                        }
                        let result = result.map_err(replied)?;
                        if let Some(issue) = undelivered {
                            return Err(issue);
                        }
                        return result
                            .map(|value| {
                                serde_json::from_value::<CodeModeExecution>(value).map_err(error)
                            })
                            .transpose();
                    }
                    Frame::Update { update, .. } => {
                        if undelivered.is_none()
                            && let Err(issue) = received.deliver(observer.as_deref_mut(), update)
                        {
                            undelivered = Some(issue);
                        }
                    }
                    _ => return Err(error("unexpected operation reply")),
                }
            }
            Err(error(
                "remote reply channel closed; operation outcome unknown",
            ))
        };
        let result = client
            .deadline(
                reply,
                requested,
                "remote operation deadline elapsed; outcome unknown; cancellation requested; no retry",
            )
            .await?;
        if let (Some(session), Ok(Some(execution))) = (&session_id, &result)
            && let Some(cell) = execution.cell.as_ref().filter(|cell| cell.running)
        {
            let generation = client
                .turns
                .read(|turns| turns.get(session).copied())
                .unwrap_or(0);
            let key = (session.clone(), cell.origin_call_id.clone());
            client.callbacks.update(|callbacks| {
                if let Some(state) = callbacks.get_mut(&key) {
                    state.generation = generation;
                }
            });
        }
        if let Some(session_id) = session_id {
            let completed = match &result {
                Ok(Some(execution)) => match &execution.cell {
                    Some(cell) if cell.running => None,
                    Some(cell) => Some(cell.origin_call_id.clone()),
                    None => origin_call_id,
                },
                Err(_) => origin_call_id,
                Ok(None) => None,
            };
            if let Some(origin) = completed {
                let carrier = match (&result, &carrier) {
                    (Ok(Some(_)), Some((call, turn))) => Some((call.as_str(), turn.as_deref())),
                    _ => None,
                };
                client.settle(session_id, origin, carrier);
            }
        }
        guard.complete = true;
        result
    }

    async fn called(
        &self,
        operation: RemoteOperation,
        barrier: Option<Arc<dyn RemoteBarrier>>,
    ) -> Result<Value, CodeModeHostError> {
        let client = &self.client;
        let id = client.next.fetch_add(1, Ordering::Relaxed);
        let recorded = recorded(&operation);
        let (frame, registered, reply) = prepare_call(client, id, operation, barrier.clone())?;
        client
            .record(id, recorded.as_ref(), barrier.as_deref())
            .await?;
        client
            .dispatch(frame, RemoteDelivery::Immediate, Some(registered))
            .map_err(|_| error("remote connection closed; the operation was not dispatched"))?;
        reply.await
    }
}

impl RemoteTransport for RemoteClient {
    fn supports_tool_selection(&self) -> bool {
        self.client.tool_selection
    }
    fn adopt(&self, context: &RemoteContext, tools: Vec<Arc<dyn Tool>>) {
        let held = Adoption::Held {
            tools,
            context: RemoteContext {
                history: Arc::default(),
                ..context.clone()
            },
        };
        let previous = self
            .client
            .adoptions
            .update(|adoptions| adoptions.insert(context.session_id.clone(), held.clone()));
        if let Some(Adoption::Awaited(signal)) = previous {
            signal.fire();
        }
    }
    fn delivered(&self, context: &RemoteContext, execution: &CodeModeExecution) {
        let origin = match &execution.cell {
            Some(cell) if cell.running => return,
            Some(cell) => cell.origin_call_id.clone(),
            None => context.call_id.clone(),
        };
        self.client.settle(
            context.session_id.clone(),
            origin,
            Some((&context.call_id, context.turn_id.as_deref())),
        );
    }
    fn request<'a>(
        &'a self,
        operation: RemoteOperation,
        tools: Vec<Arc<dyn Tool>>,
        observer: Option<&'a mut dyn CodeModeObserver>,
    ) -> HostFuture<'a, Result<Option<CodeModeExecution>, CodeModeHostError>> {
        Box::pin(self.requested(operation, tools, observer, None))
    }

    fn request_after<'a>(
        &'a self,
        operation: RemoteOperation,
        tools: Vec<Arc<dyn Tool>>,
        observer: Option<&'a mut dyn CodeModeObserver>,
        barrier: Arc<dyn RemoteBarrier>,
    ) -> HostFuture<'a, Result<Option<CodeModeExecution>, CodeModeHostError>> {
        Box::pin(async move {
            self.requested(operation, tools, observer, Some(&*barrier))
                .await
        })
    }

    fn call(&self, operation: RemoteOperation) -> HostFuture<'_, Result<Value, CodeModeHostError>> {
        Box::pin(self.called(operation, None))
    }

    fn call_after(
        &self,
        operation: RemoteOperation,
        barrier: Arc<dyn RemoteBarrier>,
    ) -> HostFuture<'_, Result<Value, CodeModeHostError>> {
        Box::pin(async move { self.called(operation, Some(barrier)).await })
    }

    fn route_hands(&self, hands: Arc<dyn HandCalls>) {
        self.client.hands.set(Some(hands));
    }

    fn notify(&self, operation: RemoteOperation) -> Result<(), CodeModeHostError> {
        if let RemoteOperation::BeginTurn { session_id } = &operation {
            self.client.turns.update(|turns| {
                *turns.entry(session_id.clone()).or_default() += 1;
            });
        }
        if let Some(failure) = Option::clone(&self.client.failure.get()) {
            return Err(error(failure));
        }
        let delivery = delivery(&operation);
        let lazy = boundary(&operation);
        let frame = self
            .client
            .request_frame(self.client.next.fetch_add(1, Ordering::Relaxed), operation);
        if lazy {
            return self
                .client
                .outgoing
                .send(Outgoing::Boundary(frame))
                .map_err(|_| error(LOCAL_CLOSED));
        }
        self.client.dispatch((frame, None), delivery, None)
    }

    fn detach_turn(
        &self,
        session_id: &str,
        factory: &mut (dyn FnMut(&str) -> Box<dyn CodeModeObserver> + Send),
    ) -> Result<(), CodeModeHostError> {
        let generation = self
            .client
            .turns
            .read(|turns| turns.get(session_id).copied())
            .unwrap_or(0);
        let callbacks = self.client.callbacks.get();
        for ((session, origin), state) in callbacks.iter() {
            if session == session_id && state.generation == generation {
                state.observer.attach(factory(origin));
            }
        }
        self.notify(RemoteOperation::DetachTurn {
            session_id: session_id.into(),
        })
    }
}
