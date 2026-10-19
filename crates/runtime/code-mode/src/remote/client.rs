use std::{
    collections::HashMap,
    future::{Future, poll_fn},
    pin::pin,
    sync::{
        Arc, Mutex, MutexGuard, PoisonError, Weak,
        atomic::{AtomicU64, Ordering},
    },
    task::Poll,
    time::Duration,
};

use futures_util::{
    FutureExt, StreamExt,
    future::{AbortHandle, Abortable, Either, select},
    stream::FuturesUnordered,
};
use serde_json::Value;
use tokio::sync::{mpsc, oneshot};

use super::{
    RemoteCodeModeHost, RemoteContext, RemoteFrame as Frame, RemoteOperation, RemoteTransport,
    deliver_update,
};
use crate::{
    Tool, ToolDefinition, ToolInput,
    embedded::{CodeModeExecution, CodeModeHostError, CodeModeObserver, HandCalls, HostFuture},
};

pub const PEER_CLOSED: &str = "runner disconnected";
pub const LOCAL_CLOSED: &str = "remote connection closed";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RemoteDelivery {
    Durable,
    Immediate,
}

pub trait RemoteLink: Send + Sync + 'static {
    fn send(
        &self,
        frames: Vec<Frame>,
        delivery: RemoteDelivery,
    ) -> HostFuture<'_, Result<(), CodeModeHostError>>;
    fn receive(&self) -> HostFuture<'_, Result<Vec<Frame>, CodeModeHostError>>;
    fn close<'a>(&'a self, reason: &'a str) -> HostFuture<'a, ()>;
    fn changed(&self) {}
}

struct CallbackState {
    tools: Vec<Arc<dyn Tool>>,
    context: RemoteContext,
    generation: u64,
    observer: Option<Box<dyn CodeModeObserver>>,
}

struct Client {
    outgoing: mpsc::UnboundedSender<(Frame, RemoteDelivery)>,
    pending: Mutex<HashMap<u64, mpsc::UnboundedSender<Frame>>>,
    callbacks: Mutex<HashMap<(String, String), CallbackState>>,
    running: Mutex<HashMap<u64, (String, AbortHandle)>>,
    next: AtomicU64,
    turns: Mutex<HashMap<String, u64>>,
    operation_timeout: Option<Duration>,
    failure: Mutex<Option<String>>,
    hands: Mutex<Option<Arc<dyn HandCalls>>>,
    _stop: oneshot::Sender<()>,
}

#[derive(Clone)]
pub struct RemoteClient {
    client: Arc<Client>,
}

#[derive(Clone)]
pub struct RemoteActivity(Weak<Client>);

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

fn error(value: impl std::fmt::Display) -> CodeModeHostError {
    CodeModeHostError::new(value.to_string())
}

fn origin(call_id: &str) -> &str {
    call_id
        .rsplit_once("/code-")
        .map_or(call_id, |(origin, _)| origin)
}

impl RemoteActivity {
    #[must_use]
    pub fn is_alive(&self) -> bool {
        self.0.strong_count() > 0
    }

    #[must_use]
    pub fn busy(&self) -> bool {
        self.0.upgrade().is_some_and(|client| {
            !lock(&client.running).is_empty()
                || lock(&client.callbacks)
                    .values()
                    .any(|state| state.observer.is_some())
        })
    }
}

impl RemoteClient {
    pub fn new(
        link: impl RemoteLink,
        operation_timeout: Option<Duration>,
    ) -> (Self, HostFuture<'static, String>) {
        let (outgoing, frames) = mpsc::unbounded_channel();
        let (stop, stopped) = oneshot::channel();
        let client = Arc::new(Client {
            outgoing,
            pending: Mutex::default(),
            callbacks: Mutex::default(),
            running: Mutex::default(),
            next: AtomicU64::new(0),
            turns: Mutex::default(),
            operation_timeout,
            failure: Mutex::default(),
            hands: Mutex::default(),
            _stop: stop,
        });
        let driver = Box::pin(drive(Arc::downgrade(&client), link, frames, stopped));
        (Self { client }, driver)
    }

    #[must_use]
    pub fn activity(&self) -> RemoteActivity {
        RemoteActivity(Arc::downgrade(&self.client))
    }

    #[must_use]
    pub fn failure(&self) -> Option<String> {
        lock(&self.client.failure).clone()
    }

    #[must_use]
    pub fn into_host(self, definitions: Vec<ToolDefinition>) -> RemoteCodeModeHost {
        RemoteCodeModeHost::new(self, definitions)
    }
}

impl Client {
    fn dispatch(&self, frame: Frame, delivery: RemoteDelivery) -> Result<(), CodeModeHostError> {
        if let Frame::Request {
            operation: RemoteOperation::Cancel { session_id },
            ..
        } = &frame
        {
            lock(&self.callbacks).retain(|(session, _), _| session != session_id);
            lock(&self.turns).remove(session_id);
            lock(&self.running).retain(|_, (session, abort)| {
                if session == session_id {
                    abort.abort();
                    false
                } else {
                    true
                }
            });
        }
        self.outgoing.send((frame, delivery)).map_err(error)
    }

    fn receive(
        self: &Arc<Self>,
        frame: Frame,
    ) -> Result<Option<HostFuture<'static, ()>>, CodeModeHostError> {
        match frame {
            Frame::Reply { id, .. } | Frame::Update { id, .. } => {
                if let Some(channel) = lock(&self.pending).get(&id) {
                    let _ = channel.send(frame);
                }
            }
            Frame::CallbackCancel { id } => {
                if let Some((_, abort)) = lock(&self.running).remove(&id) {
                    abort.abort();
                }
            }
            Frame::DetachedUpdate {
                session_id,
                origin_call_id,
                update,
            } => {
                if let Some(observer) = lock(&self.callbacks)
                    .get_mut(&(session_id, origin_call_id))
                    .and_then(|state| state.observer.as_deref_mut())
                {
                    deliver_update(observer, update)?;
                }
            }
            Frame::DetachedComplete {
                session_id,
                origin_call_id,
            } => {
                lock(&self.callbacks).remove(&(session_id, origin_call_id));
            }
            Frame::Callback {
                id,
                name,
                input,
                freeform,
                context,
                hand,
            } => return Ok(Some(self.callback(id, name, input, freeform, context, hand))),
            _ => return Err(error("unexpected runner frame")),
        }
        Ok(None)
    }

    fn callback(
        self: &Arc<Self>,
        id: u64,
        name: String,
        input: Value,
        freeform: bool,
        context: RemoteContext,
        hand: Option<String>,
    ) -> HostFuture<'static, ()> {
        let local = lock(&self.callbacks)
            .get(&(
                context.session_id.clone(),
                origin(&context.call_id).to_owned(),
            ))
            .map(|state| {
                (
                    state
                        .tools
                        .iter()
                        .find(|tool| tool.definition().name() == name)
                        .cloned(),
                    state.context.clone(),
                )
            });
        let hands = lock(&self.hands).clone();
        let delivery = if hand.is_some() {
            RemoteDelivery::Immediate
        } else {
            RemoteDelivery::Durable
        };
        let (abort, registration) = AbortHandle::new_pair();
        lock(&self.running).insert(id, (context.session_id.clone(), abort));
        let outgoing = self.outgoing.clone();
        let client = Arc::downgrade(self);
        Box::pin(async move {
            let execution = Abortable::new(
                async move {
                    if let Some(hand) = hand {
                        let (_, mut authoritative) = local.ok_or_else(|| {
                            error(format!(
                                "brain callback origin for `{name}` on hand `{hand}` unavailable"
                            ))
                        })?;
                        let hands = hands.ok_or_else(|| {
                            error("this brain does not route calls between hands")
                        })?;
                        authoritative.call_id = context.call_id;
                        let owned = authoritative.owned();
                        let output = hands.call(&hand, &name, input, owned.as_context()).await;
                        return serde_json::to_value(output.into_wire().map_err(error)?)
                            .map_err(error);
                    }
                    let Some((Some(tool), mut authoritative)) = local else {
                        return Err(error(format!("brain callback tool `{name}` unavailable")));
                    };
                    authoritative.call_id = context.call_id;
                    let owned = authoritative.owned();
                    let input = if freeform {
                        ToolInput::Freeform(
                            input
                                .as_str()
                                .ok_or_else(|| error("freeform callback must be a string"))?
                                .into(),
                        )
                    } else {
                        ToolInput::Function(serde_json::value::to_raw_value(&input).map_err(error)?)
                    };
                    let output = tool
                        .execute(input, owned.as_context())
                        .await
                        .map_err(error)?;
                    serde_json::to_value(output.into_wire().map_err(error)?).map_err(error)
                },
                registration,
            );
            if let Ok(result) = execution.await {
                let _ = outgoing.send((
                    Frame::CallbackResult {
                        id,
                        result: result.map_err(|issue: CodeModeHostError| issue.to_string()),
                    },
                    delivery,
                ));
            }
            if let Some(client) = client.upgrade() {
                lock(&client.running).remove(&id);
            }
        })
    }

    fn terminate(&self, message: &str) {
        for (_, (_, abort)) in lock(&self.running).drain() {
            abort.abort();
        }
        lock(&self.callbacks).clear();
        let uncertain = format!(
            "{message}; the dispatched operation has an unknown outcome and was not retried"
        );
        *lock(&self.failure) = Some(message.to_owned());
        for (id, channel) in lock(&self.pending).drain() {
            let _ = channel.send(Frame::Reply {
                id,
                result: Err(uncertain.clone()),
            });
        }
    }

    fn not_dispatched(&self) -> Result<(), CodeModeHostError> {
        match lock(&self.failure).clone() {
            Some(failure) => Err(error(format!(
                "{failure}; the operation was not dispatched"
            ))),
            None => Ok(()),
        }
    }

    async fn deadline<T>(
        &self,
        reply: impl Future<Output = T>,
        elapsed: &str,
    ) -> Result<T, CodeModeHostError> {
        let Some(timeout) = self.operation_timeout else {
            return Ok(reply.await);
        };
        match select(pin!(reply), pin!(nanocode_platform::sleep(timeout))).await {
            Either::Left((result, _)) => Ok(result),
            Either::Right(_) => Err(error(elapsed)),
        }
    }
}

async fn drive(
    client: Weak<Client>,
    link: impl RemoteLink,
    mut frames: mpsc::UnboundedReceiver<(Frame, RemoteDelivery)>,
    mut stopped: oneshot::Receiver<()>,
) -> String {
    let mut callbacks = FuturesUnordered::new();
    let result = {
        let mut receiving = link.receive();
        let mut sending: Option<HostFuture<'_, Result<(), CodeModeHostError>>> = None;
        poll_fn(|cx| {
            loop {
                if stopped.poll_unpin(cx).is_ready() {
                    return Poll::Ready(Ok(()));
                }
                let mut progressed = false;
                if let Some(send) = sending.as_mut() {
                    match send.as_mut().poll(cx) {
                        Poll::Ready(Ok(())) => {
                            sending = None;
                            progressed = true;
                        }
                        Poll::Ready(Err(issue)) => return Poll::Ready(Err(issue)),
                        Poll::Pending => {}
                    }
                }
                if sending.is_none() {
                    let mut batch = Vec::new();
                    let mut delivery = RemoteDelivery::Immediate;
                    while let Poll::Ready(next) = frames.poll_recv(cx) {
                        let Some((frame, needs)) = next else {
                            return Poll::Ready(Ok(()));
                        };
                        if needs == RemoteDelivery::Durable {
                            delivery = needs;
                        }
                        batch.push(frame);
                    }
                    if !batch.is_empty() {
                        sending = Some(link.send(batch, delivery));
                        continue;
                    }
                }
                match receiving.as_mut().poll(cx) {
                    Poll::Ready(Ok(received)) => {
                        let Some(client) = client.upgrade() else {
                            return Poll::Ready(Ok(()));
                        };
                        for frame in received {
                            match client.receive(frame) {
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
        .await
    };
    drop(callbacks);
    let message = result.map_or_else(|issue| issue.to_string(), |()| LOCAL_CLOSED.to_owned());
    if let Some(client) = client.upgrade() {
        client.terminate(&message);
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
        lock(&self.client.pending).remove(&self.id);
        if !self.complete {
            if let Some(origin) = self.origin.take() {
                lock(&self.client.callbacks).remove(&origin);
            }
            if let Some(session_id) = self.session_id.take() {
                let id = self.client.next.fetch_add(1, Ordering::Relaxed);
                let _ = self.client.outgoing.send((
                    Frame::Request {
                        id,
                        operation: RemoteOperation::CancelTurn { session_id },
                    },
                    RemoteDelivery::Durable,
                ));
            }
        }
    }
}

impl RemoteTransport for RemoteClient {
    fn request<'a>(
        &'a self,
        operation: RemoteOperation,
        tools: Vec<Arc<dyn Tool>>,
        mut observer: Option<&'a mut dyn CodeModeObserver>,
    ) -> HostFuture<'a, Result<Option<CodeModeExecution>, CodeModeHostError>> {
        Box::pin(async move {
            let client = &self.client;
            client.not_dispatched()?;
            let origin_call_id = match &operation {
                RemoteOperation::Execute { context, .. } => Some(context.call_id.clone()),
                _ => None,
            };
            let session_id = match &operation {
                RemoteOperation::Execute { context, .. } => {
                    let generation = *lock(&client.turns)
                        .get(&context.session_id)
                        .unwrap_or(&0);
                    let mut callbacks = lock(&client.callbacks);
                    let key = (context.session_id.clone(), context.call_id.clone());
                    if callbacks.contains_key(&key) {
                        return Err(error("callback origin is already active"));
                    }
                    callbacks.insert(
                        key,
                        CallbackState {
                            tools,
                            context: context.clone(),
                            generation,
                            observer: None,
                        },
                    );
                    Some(context.session_id.clone())
                }
                RemoteOperation::Wait { context, .. } => Some(context.session_id.clone()),
                _ => None,
            };
            let id = client.next.fetch_add(1, Ordering::Relaxed);
            let (sender, mut replies) = mpsc::unbounded_channel();
            lock(&client.pending).insert(id, sender);
            let mut guard = Pending {
                client: Arc::clone(client),
                id,
                session_id: session_id.clone(),
                origin: session_id.clone().zip(origin_call_id.clone()),
                complete: false,
            };
            client
                .dispatch(Frame::Request { id, operation }, RemoteDelivery::Durable)
                .map_err(|_| error("remote connection closed; the operation was not dispatched"))?;
            let reply = async {
                while let Some(frame) = replies.recv().await {
                    match frame {
                        Frame::Reply { result, .. } => {
                            return result
                                .map_err(error)?
                                .map(|value| {
                                    serde_json::from_value::<CodeModeExecution>(value)
                                        .map_err(error)
                                })
                                .transpose();
                        }
                        Frame::Update { update, .. } => {
                            if let Some(observer) = observer.as_deref_mut() {
                                deliver_update(observer, update)?;
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
                    "remote operation deadline elapsed; outcome unknown; cancellation requested; no retry",
                )
                .await?;
            if let (Some(session), Ok(Some(execution))) = (&session_id, &result)
                && let Some(cell) = execution.cell.as_ref().filter(|cell| cell.running)
                && let Some(state) =
                    lock(&client.callbacks).get_mut(&(session.clone(), cell.origin_call_id.clone()))
            {
                state.generation = *lock(&client.turns).get(session).unwrap_or(&0);
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
                    lock(&client.callbacks).remove(&(session_id, origin));
                }
            }
            guard.complete = true;
            result
        })
    }

    fn call(&self, operation: RemoteOperation) -> HostFuture<'_, Result<Value, CodeModeHostError>> {
        Box::pin(async move {
            let client = &self.client;
            client.not_dispatched()?;
            let id = client.next.fetch_add(1, Ordering::Relaxed);
            let (sender, mut replies) = mpsc::unbounded_channel();
            lock(&client.pending).insert(id, sender);
            let mut guard = Pending {
                client: Arc::clone(client),
                id,
                session_id: None,
                origin: None,
                complete: false,
            };
            client
                .dispatch(Frame::Request { id, operation }, RemoteDelivery::Immediate)
                .map_err(|_| error("remote connection closed; the operation was not dispatched"))?;
            let reply = async {
                while let Some(frame) = replies.recv().await {
                    match frame {
                        Frame::Reply { result, .. } => {
                            return result
                                .map_err(error)?
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
            let result = client
                .deadline(
                    reply,
                    "remote operation deadline elapsed; outcome unknown; no retry",
                )
                .await?;
            guard.complete = true;
            result
        })
    }

    fn route_hands(&self, hands: Arc<dyn HandCalls>) {
        *lock(&self.client.hands) = Some(hands);
    }

    fn notify(&self, operation: RemoteOperation) -> Result<(), CodeModeHostError> {
        if let RemoteOperation::BeginTurn { session_id } = &operation {
            *lock(&self.client.turns)
                .entry(session_id.clone())
                .or_default() += 1;
        }
        if let Some(failure) = lock(&self.client.failure).clone() {
            return Err(error(failure));
        }
        self.client.dispatch(
            Frame::Request {
                id: self.client.next.fetch_add(1, Ordering::Relaxed),
                operation,
            },
            RemoteDelivery::Durable,
        )
    }

    fn detach_turn(
        &self,
        session_id: &str,
        factory: &mut (dyn FnMut(&str) -> Box<dyn CodeModeObserver> + Send),
    ) -> Result<(), CodeModeHostError> {
        let generation = *lock(&self.client.turns).get(session_id).unwrap_or(&0);
        for ((session, origin), state) in lock(&self.client.callbacks).iter_mut() {
            if session == session_id && state.generation == generation {
                state.observer = Some(factory(origin));
            }
        }
        self.notify(RemoteOperation::DetachTurn {
            session_id: session_id.into(),
        })
    }
}
