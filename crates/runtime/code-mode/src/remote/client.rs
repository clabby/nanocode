use std::{
    collections::HashMap,
    future::{Future, poll_fn},
    pin::pin,
    sync::{
        Arc, Weak,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    task::Poll,
    time::Duration,
};

use nanocode_sync::{Atom, Slot};

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

    fn deliver(&self, update: Value) -> Result<(), CodeModeHostError> {
        let Some(mut observer) = self.current.take() else {
            return Ok(());
        };
        let delivered = deliver_update(observer.as_mut(), update);
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

struct Client {
    outgoing: mpsc::UnboundedSender<(Frame, RemoteDelivery)>,
    pending: Atom<HashMap<u64, mpsc::UnboundedSender<Frame>>>,
    callbacks: Atom<Callbacks>,
    running: Atom<HashMap<u64, (String, AbortHandle)>>,
    next: AtomicU64,
    turns: Atom<HashMap<String, u64>>,
    operation_timeout: Option<Duration>,
    failure: Atom<Option<String>>,
    hands: Atom<Option<Arc<dyn HandCalls>>>,
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
            !client.running.read(HashMap::is_empty)
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
        let (outgoing, frames) = mpsc::unbounded_channel();
        let (stop, stopped) = oneshot::channel();
        let client = Arc::new(Client {
            outgoing,
            pending: Atom::default(),
            callbacks: Atom::default(),
            running: Atom::default(),
            next: AtomicU64::new(0),
            turns: Atom::default(),
            operation_timeout,
            failure: Atom::default(),
            hands: Atom::default(),
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
        Option::clone(&self.client.failure.get())
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
            self.callbacks
                .update(|callbacks| callbacks.retain(|(session, _), _| session != session_id));
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
        self.outgoing.send((frame, delivery)).map_err(error)
    }

    fn receive(
        self: &Arc<Self>,
        frame: Frame,
    ) -> Result<Option<HostFuture<'static, ()>>, CodeModeHostError> {
        match frame {
            Frame::Reply { id, .. } | Frame::Update { id, .. } => {
                if let Some(channel) = self.pending.read(|pending| pending.get(&id).cloned()) {
                    let _ = channel.send(frame);
                }
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
                    observer.deliver(update)?;
                }
            }
            Frame::DetachedComplete {
                session_id,
                origin_call_id,
            } => {
                let key = (session_id, origin_call_id);
                self.callbacks.update(|callbacks| {
                    callbacks.remove(&key);
                });
            }
            Frame::Callback {
                id,
                name,
                input,
                freeform,
                context,
                hand,
            } => {
                return Ok(Some(
                    self.callback(id, name, input, freeform, context, hand),
                ));
            }
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
        let hands = Option::clone(&self.hands.get());
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
                client.running.update(|running| {
                    running.remove(&id);
                });
            }
        })
    }

    fn terminate(&self, message: &str) {
        for (_, (_, abort)) in self.running.update(std::mem::take) {
            abort.abort();
        }
        self.callbacks.set(HashMap::new());
        let uncertain = format!(
            "{message}; the dispatched operation has an unknown outcome and was not retried"
        );
        self.failure.set(Some(message.to_owned()));
        for (id, channel) in self.pending.update(std::mem::take) {
            let _ = channel.send(Frame::Reply {
                id,
                result: Err(uncertain.clone()),
            });
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
        self.client.pending.update(|pending| {
            pending.remove(&self.id);
        });
        if !self.complete {
            if let Some(origin) = self.origin.take() {
                self.client.callbacks.update(|callbacks| {
                    callbacks.remove(&origin);
                });
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
                    let generation = client
                        .turns
                        .read(|turns| turns.get(&context.session_id).copied())
                        .unwrap_or(0);
                    let key = (context.session_id.clone(), context.call_id.clone());
                    let state = CallbackState {
                        tools,
                        context: context.clone(),
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
                    Some(context.session_id.clone())
                }
                RemoteOperation::Wait { context, .. } => Some(context.session_id.clone()),
                _ => None,
            };
            let id = client.next.fetch_add(1, Ordering::Relaxed);
            let (sender, mut replies) = mpsc::unbounded_channel();
            client.pending.update(|pending| {
                pending.insert(id, sender.clone());
            });
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
                    let key = (session_id, origin);
                    client.callbacks.update(|callbacks| {
                        callbacks.remove(&key);
                    });
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
            client.pending.update(|pending| {
                pending.insert(id, sender.clone());
            });
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
