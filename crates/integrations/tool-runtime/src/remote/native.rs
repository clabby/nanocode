use super::*;
use crate::{
    ToolInput, ToolOutput,
    contract::{ToolError, ToolOutputWire, ToolResult},
    embedded::{CodeModeUpdate, HandCalls},
    runtime::ToolRuntime,
};
use futures_util::{Sink, SinkExt, Stream, StreamExt};
use serde_json::{Value, json};
use std::{
    collections::{HashMap, HashSet},
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};
use tokio::{
    io::{AsyncRead, AsyncWrite},
    net::{TcpListener, TcpStream},
    sync::{mpsc, oneshot, watch},
};
use tokio_tungstenite::{
    WebSocketStream,
    tungstenite::{self, Message, client::IntoClientRequest},
};
use tokio_util::sync::{CancellationToken, DropGuard};

#[derive(Clone, Copy)]
pub struct SocketPolicy {
    pub max_frame_bytes: usize,
    pub request_timeout: Duration,
}

impl SocketPolicy {
    #[must_use]
    pub fn runner() -> Self {
        Self {
            max_frame_bytes: tungstenite::protocol::WebSocketConfig::default()
                .max_message_size
                .unwrap_or(usize::MAX),
            request_timeout: crate::attachment::HEARTBEAT_INTERVAL * 2,
        }
    }
}

#[derive(Clone, Default)]
pub struct ExecutorOptions {
    pub name: Option<String>,
    pub runtime_id: Option<String>,
    pub callback_timeout: Option<Duration>,
}

#[derive(Clone)]
pub(crate) struct SocketShutdown {
    token: CancellationToken,
    reason: Arc<Mutex<Option<&'static str>>>,
}

impl SocketShutdown {
    pub(crate) fn close(&self) {
        self.token.cancel();
    }

    pub(crate) fn reject(&self, reason: &'static str) {
        *self.reason.lock().unwrap() = Some(reason);
        self.token.cancel();
    }

    pub(crate) fn revoke(&self) {
        self.reject(nanocode_tool_host::attachment::ATTACHMENT_REVOKED_REASON);
    }
}

use super::RemoteFrame as Frame;

struct CallbackState {
    tools: Vec<Arc<dyn Tool>>,
    context: RemoteContext,
    generation: u64,
    observer: Option<Box<dyn CodeModeObserver>>,
}
struct Client {
    outgoing: mpsc::UnboundedSender<Frame>,
    pending: Mutex<HashMap<u64, mpsc::UnboundedSender<Frame>>>,
    callbacks: Mutex<HashMap<(String, String), CallbackState>>,
    next: AtomicU64,
    turns: Mutex<HashMap<String, u64>>,
    operation_timeout: Option<Duration>,
    failure: Mutex<Option<String>>,
    closed: watch::Sender<Option<String>>,
    hands: Mutex<Option<Arc<dyn HandCalls>>>,
}

pub struct SocketTransport {
    client: Arc<Client>,
    definitions: Vec<ToolDefinition>,
    name: Option<String>,
    workspace: Option<String>,
    runtime_id: Option<String>,
    shutdown: SocketShutdown,
    _shutdown: DropGuard,
}

pub(super) fn error(value: impl std::fmt::Display) -> CodeModeHostError {
    CodeModeHostError::new(value.to_string())
}

pub(super) fn validate_policy(policy: SocketPolicy) -> Result<(), CodeModeHostError> {
    if policy.max_frame_bytes == 0 || policy.request_timeout.is_zero() {
        return Err(error(
            "max_frame_bytes and request_timeout must be positive",
        ));
    }
    if tokio::time::Instant::now()
        .checked_add(policy.request_timeout)
        .is_none()
    {
        return Err(error("request_timeout exceeds the clock range"));
    }
    if (policy.request_timeout / 2).is_zero() {
        return Err(error(
            "request_timeout must be at least 2ns so the heartbeat can ping every half request_timeout",
        ));
    }
    Ok(())
}

struct Liveness {
    origin: tokio::time::Instant,
    last_inbound_us: AtomicU64,
    probe_us: AtomicU64,
}

impl Liveness {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            origin: tokio::time::Instant::now(),
            last_inbound_us: AtomicU64::new(0),
            probe_us: AtomicU64::new(0),
        })
    }
    fn elapsed_us(&self) -> u64 {
        u64::try_from(self.origin.elapsed().as_micros()).unwrap_or(u64::MAX)
    }
    fn touch(&self) {
        self.last_inbound_us
            .store(self.elapsed_us(), Ordering::Relaxed);
    }
    fn probe_sent(&self, started_us: u64) {
        if self.probe_us.load(Ordering::Relaxed) <= self.last_inbound_us.load(Ordering::Relaxed) {
            self.probe_us.store(started_us, Ordering::Relaxed);
        }
    }
    fn unanswered_probe_age(&self) -> Option<Duration> {
        let probe = self.probe_us.load(Ordering::Relaxed);
        (probe > self.last_inbound_us.load(Ordering::Relaxed))
            .then(|| Duration::from_micros(self.elapsed_us().saturating_sub(probe)))
    }
    async fn watch(&self, window: Duration) {
        loop {
            match self.unanswered_probe_age() {
                Some(age) if age >= window => return,
                Some(age) => tokio::time::sleep(window - age).await,
                None => tokio::time::sleep(window).await,
            }
        }
    }
}

pub(crate) const PEER_CLOSED: &str = "runner disconnected";
pub(crate) const LOCAL_CLOSED: &str = "remote connection closed";

const UNRESPONSIVE: &str =
    "remote peer did not answer a WebSocket heartbeat ping within half of request_timeout";

async fn read_live<S: Stream<Item = Result<Message, tungstenite::Error>> + Unpin>(
    socket: &mut S,
    liveness: &Liveness,
    watchdog: std::pin::Pin<&mut impl std::future::Future<Output = ()>>,
) -> Result<Option<Frame>, CodeModeHostError> {
    tokio::select! {
        biased;
        frame = read(socket, liveness) => frame,
        () = watchdog => Err(error(UNRESPONSIVE)),
    }
}

fn config(policy: SocketPolicy) -> tungstenite::protocol::WebSocketConfig {
    tungstenite::protocol::WebSocketConfig::default()
        .max_message_size(Some(policy.max_frame_bytes))
        .max_frame_size(Some(policy.max_frame_bytes))
}

type DialedSocket = WebSocketStream<tokio_tungstenite::MaybeTlsStream<TcpStream>>;

pub(super) enum Dial {
    Rejected(CodeModeHostError),
    Failed(CodeModeHostError),
}

async fn connect(
    target: crate::attachment::AttachmentTarget,
    policy: SocketPolicy,
) -> Result<DialedSocket, CodeModeHostError> {
    dial(&target, policy)
        .await
        .map_err(|failure| match failure {
            Dial::Rejected(error) | Dial::Failed(error) => error,
        })
}

pub(super) async fn dial(
    target: &crate::attachment::AttachmentTarget,
    policy: SocketPolicy,
) -> Result<DialedSocket, Dial> {
    validate_policy(policy).map_err(Dial::Rejected)?;
    nanocode_tls::install_default_rustls_crypto_provider();
    let mut request = target
        .endpoint()
        .as_str()
        .into_client_request()
        .map_err(|issue| Dial::Rejected(error(issue)))?;
    let mut bearer = http::HeaderValue::from_str(&format!("Bearer {}", target.bearer()))
        .map_err(|issue| Dial::Rejected(error(issue)))?;
    bearer.set_sensitive(true);
    request
        .headers_mut()
        .insert(http::header::AUTHORIZATION, bearer);
    tokio::time::timeout(
        policy.request_timeout,
        tokio_tungstenite::connect_async_with_config(request, Some(config(policy)), true),
    )
    .await
    .map_err(|issue| Dial::Failed(error(issue)))?
    .map(|(socket, _)| socket)
    .map_err(|issue| match issue {
        tungstenite::Error::Http(response) if matches!(response.status().as_u16(), 401 | 403) => {
            Dial::Rejected(error(format!(
                "brain rejected the runner credential with HTTP {}",
                response.status()
            )))
        }
        issue => Dial::Failed(error(issue)),
    })
}

async fn accept(
    listener: &TcpListener,
    bearer: &str,
    policy: SocketPolicy,
) -> Result<WebSocketStream<TcpStream>, CodeModeHostError> {
    validate_policy(policy)?;
    if !listener.local_addr().map_err(error)?.ip().is_loopback() {
        return Err(error(
            "plaintext runner listeners require loopback; use a TLS proxy for remote access",
        ));
    }
    let (stream, _) = listener.accept().await.map_err(error)?;
    handshake(stream, bearer, policy).await
}

async fn handshake(
    stream: TcpStream,
    bearer: &str,
    policy: SocketPolicy,
) -> Result<WebSocketStream<TcpStream>, CodeModeHostError> {
    validate_policy(policy)?;
    if bearer.is_empty() {
        return Err(error("runner bearer must not be empty"));
    }
    stream.set_nodelay(true).map_err(error)?;
    let authenticate = |request: &tungstenite::handshake::server::Request,
                        response: tungstenite::handshake::server::Response| {
        if request
            .headers()
            .get(http::header::AUTHORIZATION)
            .and_then(|h| h.to_str().ok())
            .and_then(|h| h.strip_prefix("Bearer "))
            == Some(bearer)
        {
            Ok(response)
        } else {
            let mut rejection = tungstenite::handshake::server::ErrorResponse::new(Some(
                "invalid runner credential".into(),
            ));
            *rejection.status_mut() = http::StatusCode::UNAUTHORIZED;
            Err(rejection)
        }
    };
    tokio::time::timeout(
        policy.request_timeout,
        tokio_tungstenite::accept_hdr_async_with_config(stream, authenticate, Some(config(policy))),
    )
    .await
    .map_err(error)?
    .map_err(error)
}

async fn read<S: Stream<Item = Result<Message, tungstenite::Error>> + Unpin>(
    socket: &mut S,
    liveness: &Liveness,
) -> Result<Option<Frame>, CodeModeHostError> {
    while let Some(message) = socket.next().await {
        let message = message.map_err(error)?;
        liveness.touch();
        match message {
            Message::Text(text) => return serde_json::from_str(&text).map(Some).map_err(error),
            Message::Close(frame) => {
                return match frame.filter(|frame| !frame.reason.is_empty()) {
                    Some(frame) => Err(error(frame.reason.as_str())),
                    None => Ok(None),
                };
            }
            Message::Ping(_) | Message::Pong(_) => {}
            _ => return Err(error("remote code mode requires JSON text frames")),
        }
    }
    Ok(None)
}

async fn write<S: Sink<Message, Error = tungstenite::Error> + Unpin>(
    socket: &mut S,
    frame: Frame,
    policy: SocketPolicy,
) -> Result<(), CodeModeHostError> {
    let text = serde_json::to_string(&frame).map_err(error)?;
    if text.len() > policy.max_frame_bytes {
        return Err(error(
            "remote code mode frame exceeds caller max_frame_bytes policy",
        ));
    }
    tokio::time::timeout(policy.request_timeout, socket.send(Message::text(text)))
        .await
        .map_err(|_| error("remote write deadline elapsed"))?
        .map_err(error)
}

struct Writer(tokio::task::JoinHandle<Result<(), CodeModeHostError>>);
impl Drop for Writer {
    fn drop(&mut self) {
        self.0.abort();
    }
}
fn writer<S: Sink<Message, Error = tungstenite::Error> + Unpin + Send + 'static>(
    mut sink: S,
    policy: SocketPolicy,
    liveness: Arc<Liveness>,
    reason: Arc<Mutex<Option<&'static str>>>,
) -> (mpsc::UnboundedSender<Frame>, Writer) {
    let (tx, mut rx) = mpsc::unbounded_channel();
    let task = tokio::spawn(async move {
        let period = policy.request_timeout / 2;
        let mut heartbeat = tokio::time::interval_at(tokio::time::Instant::now() + period, period);
        heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                frame = rx.recv() => match frame {
                    Some(frame) => write(&mut sink, frame, policy).await?,
                    None => {
                        let reason = *reason.lock().unwrap();
                        let close = reason.map(|reason| tungstenite::protocol::CloseFrame {
                            code: tungstenite::protocol::frame::coding::CloseCode::Policy,
                            reason: reason.into(),
                        });
                        tokio::time::timeout(policy.request_timeout, sink.send(Message::Close(close)))
                            .await
                            .map_err(|_| error("remote write deadline elapsed"))?
                            .map_err(error)?;
                        return Ok(());
                    }
                },
                _ = heartbeat.tick() => {
                    let started_us = liveness.elapsed_us();
                    tokio::time::timeout(policy.request_timeout, sink.send(Message::Ping(Default::default())))
                        .await
                        .map_err(|_| error("remote write deadline elapsed"))?
                        .map_err(error)?;
                    liveness.probe_sent(started_us);
                }
            }
        }
    });
    (tx, Writer(task))
}

impl SocketTransport {
    pub async fn connect(
        target: crate::attachment::AttachmentTarget,
        policy: SocketPolicy,
    ) -> Result<Self, CodeModeHostError> {
        Self::start(
            connect(target, policy).await?,
            None,
            policy,
            Some(policy.request_timeout),
        )
        .await
    }
    pub async fn accept(
        listener: &TcpListener,
        bearer: &str,
        policy: SocketPolicy,
    ) -> Result<Self, CodeModeHostError> {
        Self::start(
            accept(listener, bearer, policy).await?,
            None,
            policy,
            Some(policy.request_timeout),
        )
        .await
    }
    pub(crate) async fn established<S: AsyncRead + AsyncWrite + Unpin + Send + 'static>(
        socket: WebSocketStream<S>,
        catalog: String,
        policy: SocketPolicy,
        operation_timeout: Option<Duration>,
    ) -> Result<Self, CodeModeHostError> {
        validate_policy(policy)?;
        Self::start(socket, Some(catalog), policy, operation_timeout).await
    }
    pub fn definitions(&self) -> Vec<ToolDefinition> {
        self.definitions.clone()
    }
    pub(crate) fn name(&self) -> Option<&str> {
        self.name.as_deref()
    }
    pub(crate) fn workspace(&self) -> Option<&str> {
        self.workspace.as_deref()
    }
    pub(crate) fn closed(&self) -> watch::Receiver<Option<String>> {
        self.client.closed.subscribe()
    }
    pub(crate) fn runtime_id(&self) -> Option<&str> {
        self.runtime_id.as_deref()
    }
    pub(crate) fn shutdown_handle(&self) -> SocketShutdown {
        self.shutdown.clone()
    }
    pub fn into_host(self) -> RemoteCodeModeHost {
        let definitions = self.definitions();
        RemoteCodeModeHost::new(self, definitions)
    }

    async fn start<S: AsyncRead + AsyncWrite + Unpin + Send + 'static>(
        mut socket: WebSocketStream<S>,
        catalog: Option<String>,
        policy: SocketPolicy,
        operation_timeout: Option<Duration>,
    ) -> Result<Self, CodeModeHostError> {
        let liveness = Liveness::new();
        let catalog = match catalog {
            Some(text) => Some(serde_json::from_str::<Frame>(&text).map_err(error)?),
            None => tokio::time::timeout(policy.request_timeout, read(&mut socket, &liveness))
                .await
                .map_err(error)??,
        };
        let Some(Frame::Catalog {
            definitions,
            name,
            workspace,
            runtime_id,
        }) = catalog
        else {
            return Err(error("runner did not publish a code mode catalog"));
        };
        crate::host::validate_public_tool_catalog_names(
            definitions.iter().map(ToolDefinition::name),
        )
        .map_err(|issue| {
            error(format!(
                "runner published an invalid code mode catalog: {issue}"
            ))
        })?;
        let (sink, mut socket) = socket.split();
        let shutdown = SocketShutdown {
            token: CancellationToken::new(),
            reason: Arc::default(),
        };
        let (wire, mut writer) = writer(
            sink,
            policy,
            Arc::clone(&liveness),
            Arc::clone(&shutdown.reason),
        );
        let (outgoing, mut incoming) = mpsc::unbounded_channel();
        let client = Arc::new(Client {
            outgoing,
            pending: Mutex::new(HashMap::new()),
            callbacks: Mutex::new(HashMap::new()),
            next: AtomicU64::new(0),
            operation_timeout,
            turns: Mutex::new(HashMap::new()),
            failure: Mutex::new(None),
            closed: watch::channel(None).0,
            hands: Mutex::new(None),
        });
        let weak = Arc::downgrade(&client);
        let stopped = shutdown.token.clone();
        tokio::spawn(async move {
            let watchdog = liveness.watch(policy.request_timeout / 2);
            tokio::pin!(watchdog);
            let mut callback_tasks = tokio::task::JoinSet::new();
            let mut callback_aborts =
                HashMap::<u64, (String, u64, tokio::task::AbortHandle)>::new();
            let result: Result<(), CodeModeHostError> = async {
                loop {
                    tokio::select! {
                        () = stopped.cancelled() => {
                            drop(wire);
                            return (&mut writer.0).await.map_err(error)?;
                        },
                        result = &mut writer.0 => return result.map_err(error)?,
                        frame = incoming.recv() => match frame {
                            Some(frame) => {
                                if let Frame::Request { operation: RemoteOperation::Cancel { session_id }, .. } = &frame {
                                    if let Some(client) = weak.upgrade() {
                                        client.callbacks.lock().unwrap().retain(|(session, _), _| session != session_id);
                                        client.turns.lock().unwrap().remove(session_id);
                                    }
                                    callback_aborts.retain(|_, (session, _, abort)| { if session == session_id { abort.abort(); false } else { true } });
                                }
                                wire.send(frame).map_err(error)?;
                            }, None => return Ok(())
                        },
                        Some(completed) = callback_tasks.join_next(), if !callback_tasks.is_empty() => { if let Ok(id) = completed { callback_aborts.remove(&id); } },
                        frame = read_live(&mut socket, &liveness, watchdog.as_mut()) => {
                            let Some(frame) = frame.map_err(|issue| error(format!("runner connection failed: {issue}")))? else { return Err(error(PEER_CLOSED)); };
                            let Some(client) = weak.upgrade() else { return Ok(()); };
                            match frame {
                                Frame::Reply { id, .. } | Frame::Update { id, .. } => {
                                    if let Some(channel) = client.pending.lock().unwrap().get(&id) { let _ = channel.send(frame); }
                                },
                                Frame::CallbackCancel { id } => { if let Some((_, _, abort)) = callback_aborts.remove(&id) { abort.abort(); } },
                                Frame::DetachedUpdate { session_id, origin_call_id, update } => {
                                    if let Some(state) = client.callbacks.lock().unwrap().get_mut(&(session_id, origin_call_id)) { if let Some(observer) = state.observer.as_deref_mut() { deliver_update(observer, update)?; } }
                                },
                                Frame::DetachedComplete { session_id, origin_call_id } => { client.callbacks.lock().unwrap().remove(&(session_id, origin_call_id)); },
                                Frame::Callback { id, name, input, context, hand: Some(hand), .. } => {
                                    let authoritative = client.callbacks.lock().unwrap().get(&(context.session_id.clone(), context.call_id.rsplit_once("/code-").map_or(context.call_id.as_str(), |(origin, _)| origin).to_owned())).map(|state| (state.context.clone(), state.generation));
                                    let callback_generation = authoritative.as_ref().map_or(0, |(_, generation)| *generation);
                                    let hands = client.hands.lock().unwrap().clone();
                                    let outgoing = client.outgoing.clone();
                                    let callback_session = context.session_id.clone();
                                    let abort = callback_tasks.spawn(async move {
                                        let result = async {
                                            let (mut authoritative, _) = authoritative.ok_or_else(|| error(format!("brain callback origin for `{name}` on hand `{hand}` unavailable")))?;
                                            let hands = hands.ok_or_else(|| error("this brain does not route calls between hands"))?;
                                            authoritative.call_id = context.call_id;
                                            let owned = authoritative.owned();
                                            let output = hands.call(&hand, &name, input, owned.as_context()).await;
                                            serde_json::to_value(output.into_wire().map_err(error)?).map_err(error)
                                        }.await.map_err(|e: CodeModeHostError| e.to_string());
                                        let _ = outgoing.send(Frame::CallbackResult { id, result });
                                        id
                                    });
                                    callback_aborts.insert(id, (callback_session, callback_generation, abort));
                                },
                                Frame::Callback { id, name, input, freeform, context, hand: None } => {
                                    let local = client.callbacks.lock().unwrap().get(&(context.session_id.clone(), context.call_id.rsplit_once("/code-").map_or(context.call_id.as_str(), |(origin, _)| origin).to_owned())).and_then(|state| state.tools.iter().find(|t| t.definition().name() == name).map(|tool| (Arc::clone(tool), state.context.clone(), state.generation)));
                                    let outgoing = client.outgoing.clone();
                                    let callback_session = context.session_id.clone();
                                    let callback_generation = local.as_ref().map_or(0, |(_, _, generation)| *generation);
                                    let abort = callback_tasks.spawn(async move {
                                        let result = async {
                                            let (tool, mut authoritative, _) = local.ok_or_else(|| error(format!("brain callback tool `{name}` unavailable")))?;
                                            authoritative.call_id = context.call_id;
                                            let owned = authoritative.owned();
                                            let input = if freeform { ToolInput::Freeform(input.as_str().ok_or_else(|| error("freeform callback must be a string"))?.into()) } else { ToolInput::Function(serde_json::value::to_raw_value(&input).map_err(error)?) };
                                            let output = tool.execute(input, owned.as_context()).await.map_err(error)?;
                                            serde_json::to_value(output.into_wire().map_err(error)?).map_err(error)
                                        }.await.map_err(|e: CodeModeHostError| e.to_string());
                                        let _ = outgoing.send(Frame::CallbackResult { id, result });
                                        id
                                    });
                                    callback_aborts.insert(id, (callback_session, callback_generation, abort));
                                },
                                _ => return Err(error("unexpected runner frame")),
                            }
                        }
                    }
                }
            }.await;
            callback_tasks.abort_all();
            if let Some(client) = weak.upgrade() {
                client.callbacks.lock().unwrap().clear();
                let message = result
                    .err()
                    .map_or_else(|| LOCAL_CLOSED.into(), |e| e.to_string());
                let uncertain = format!(
                    "{message}; the dispatched operation has an unknown outcome and was not retried"
                );
                *client.failure.lock().unwrap() = Some(message.clone());
                for (id, channel) in client.pending.lock().unwrap().drain() {
                    let _ = channel.send(Frame::Reply {
                        id,
                        result: Err(uncertain.clone()),
                    });
                }
                client.closed.send_replace(Some(message));
            }
        });
        Ok(Self {
            client,
            definitions,
            name,
            workspace,
            runtime_id,
            _shutdown: shutdown.token.clone().drop_guard(),
            shutdown,
        })
    }
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
        self.client.pending.lock().unwrap().remove(&self.id);
        if !self.complete {
            if let Some(origin) = self.origin.take() {
                self.client.callbacks.lock().unwrap().remove(&origin);
            }
            if let Some(session_id) = self.session_id.take() {
                let id = self.client.next.fetch_add(1, Ordering::Relaxed);
                let _ = self.client.outgoing.send(Frame::Request {
                    id,
                    operation: RemoteOperation::CancelTurn { session_id },
                });
            }
        }
    }
}

impl RemoteTransport for SocketTransport {
    fn request<'a>(
        &'a self,
        operation: RemoteOperation,
        tools: Vec<Arc<dyn Tool>>,
        mut observer: Option<&'a mut dyn CodeModeObserver>,
    ) -> HostFuture<'a, Result<Option<CodeModeExecution>, CodeModeHostError>> {
        Box::pin(async move {
            if let Some(failure) = self.client.failure.lock().unwrap().clone() {
                return Err(error(format!(
                    "{failure}; the operation was not dispatched"
                )));
            }
            let origin_call_id = match &operation {
                RemoteOperation::Execute { context, .. } => Some(context.call_id.clone()),
                _ => None,
            };
            let session_id = match &operation {
                RemoteOperation::Execute { context, .. } => {
                    let generation = *self
                        .client
                        .turns
                        .lock()
                        .unwrap()
                        .get(&context.session_id)
                        .unwrap_or(&0);
                    let mut callbacks = self.client.callbacks.lock().unwrap();
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
            let id = self.client.next.fetch_add(1, Ordering::Relaxed);
            let (tx, mut rx) = mpsc::unbounded_channel();
            self.client.pending.lock().unwrap().insert(id, tx);
            let mut guard = Pending {
                client: Arc::clone(&self.client),
                id,
                session_id: session_id.clone(),
                origin: session_id.clone().zip(origin_call_id.clone()),
                complete: false,
            };
            self.client
                .outgoing
                .send(Frame::Request { id, operation })
                .map_err(|_| error("remote connection closed; the operation was not dispatched"))?;
            let reply = async {
                while let Some(frame) = rx.recv().await {
                    match frame {
                        Frame::Reply { result, .. } => {
                            return result
                                .map_err(error)?
                                .map(|value| serde_json::from_value(value).map_err(error))
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
            let result: Result<Option<CodeModeExecution>, CodeModeHostError> = match self.client.operation_timeout {
                Some(deadline) => tokio::time::timeout(deadline, reply).await.map_err(|_| error("remote operation deadline elapsed; outcome unknown; cancellation requested; no retry"))?,
                None => reply.await,
            };
            if let (Some(session), Ok(Some(execution))) = (&session_id, &result) {
                if let Some(cell) = &execution.cell {
                    if cell.running {
                        if let Some(state) = self
                            .client
                            .callbacks
                            .lock()
                            .unwrap()
                            .get_mut(&(session.clone(), cell.origin_call_id.clone()))
                        {
                            state.generation =
                                *self.client.turns.lock().unwrap().get(session).unwrap_or(&0);
                        }
                    }
                }
            }
            if let Some(session_id) = session_id {
                let completed_origin = match &result {
                    Ok(Some(execution)) => match &execution.cell {
                        Some(cell) if cell.running => None,
                        Some(cell) => Some(cell.origin_call_id.clone()),
                        None => origin_call_id,
                    },
                    Err(_) => origin_call_id,
                    _ => None,
                };
                if let Some(origin) = completed_origin {
                    self.client
                        .callbacks
                        .lock()
                        .unwrap()
                        .remove(&(session_id, origin));
                }
            }
            guard.complete = true;
            result
        })
    }
    fn call(&self, operation: RemoteOperation) -> HostFuture<'_, Result<Value, CodeModeHostError>> {
        Box::pin(async move {
            if let Some(failure) = self.client.failure.lock().unwrap().clone() {
                return Err(error(format!(
                    "{failure}; the operation was not dispatched"
                )));
            }
            let id = self.client.next.fetch_add(1, Ordering::Relaxed);
            let (tx, mut rx) = mpsc::unbounded_channel();
            self.client.pending.lock().unwrap().insert(id, tx);
            let mut guard = Pending {
                client: Arc::clone(&self.client),
                id,
                session_id: None,
                origin: None,
                complete: false,
            };
            self.client
                .outgoing
                .send(Frame::Request { id, operation })
                .map_err(|_| error("remote connection closed; the operation was not dispatched"))?;
            let reply = async {
                while let Some(frame) = rx.recv().await {
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
            let result = match self.client.operation_timeout {
                Some(deadline) => tokio::time::timeout(deadline, reply).await.map_err(|_| {
                    error("remote operation deadline elapsed; outcome unknown; no retry")
                })?,
                None => reply.await,
            };
            guard.complete = true;
            result
        })
    }
    fn route_hands(&self, hands: Arc<dyn HandCalls>) {
        *self.client.hands.lock().unwrap() = Some(hands);
    }
    fn notify(&self, operation: RemoteOperation) -> Result<(), CodeModeHostError> {
        if let RemoteOperation::BeginTurn { session_id } = &operation {
            *self
                .client
                .turns
                .lock()
                .unwrap()
                .entry(session_id.clone())
                .or_default() += 1;
        }
        if let Some(failure) = self.client.failure.lock().unwrap().clone() {
            return Err(error(failure));
        }
        self.client
            .outgoing
            .send(Frame::Request {
                id: self.client.next.fetch_add(1, Ordering::Relaxed),
                operation,
            })
            .map_err(error)
    }
    fn detach_turn(
        &self,
        session_id: &str,
        factory: &mut (dyn FnMut(&str) -> Box<dyn CodeModeObserver> + Send),
    ) -> Result<(), CodeModeHostError> {
        let generation = *self
            .client
            .turns
            .lock()
            .unwrap()
            .get(session_id)
            .unwrap_or(&0);
        for ((session, origin), state) in self.client.callbacks.lock().unwrap().iter_mut() {
            if session == session_id && state.generation == generation {
                state.observer = Some(factory(origin));
            }
        }
        self.notify(RemoteOperation::DetachTurn {
            session_id: session_id.into(),
        })
    }
}

struct Observer {
    id: u64,
    outgoing: mpsc::UnboundedSender<Frame>,
}
impl CodeModeObserver for Observer {
    fn update(&mut self, update: CodeModeUpdate<'_>) {
        let update = match update {
            CodeModeUpdate::NestedCallStarted {
                call_id,
                name,
                input,
            } => json!({"call_id":call_id,"name":name,"input":input}),
            CodeModeUpdate::NestedCallCompleted(call) => json!({"completed":nested(call)}),
        };
        let _ = self.outgoing.send(Frame::Update {
            id: self.id,
            update,
        });
    }
}

struct DetachedObserver {
    session_id: String,
    origin_call_id: String,
    outgoing: mpsc::UnboundedSender<Frame>,
    catalog: CallbackCatalog,
}
impl CodeModeObserver for DetachedObserver {
    fn update(&mut self, update: CodeModeUpdate<'_>) {
        let update = match update {
            CodeModeUpdate::NestedCallStarted {
                call_id,
                name,
                input,
            } => json!({"call_id":call_id,"name":name,"input":input}),
            CodeModeUpdate::NestedCallCompleted(call) => json!({"completed":nested(call)}),
        };
        let _ = self.outgoing.send(Frame::DetachedUpdate {
            session_id: self.session_id.clone(),
            origin_call_id: self.origin_call_id.clone(),
            update,
        });
    }
    fn completed(&mut self) {
        self.catalog.finish(&self.origin_call_id);
    }
}

struct CallbackBus {
    outgoing: mpsc::UnboundedSender<Frame>,
    pending: Mutex<HashMap<u64, oneshot::Sender<Result<Value, String>>>>,
    next: AtomicU64,
    timeout: Option<Duration>,
}
struct CallbackPending {
    bus: Arc<CallbackBus>,
    id: u64,
}
impl Drop for CallbackPending {
    fn drop(&mut self) {
        if self.bus.pending.lock().unwrap().remove(&self.id).is_some() {
            let _ = self
                .bus
                .outgoing
                .send(Frame::CallbackCancel { id: self.id });
        }
    }
}

async fn callback(bus: &Arc<CallbackBus>, frame: impl FnOnce(u64) -> Frame) -> ToolResult {
    let id = bus.next.fetch_add(1, Ordering::Relaxed);
    let (tx, rx) = oneshot::channel();
    bus.pending.lock().unwrap().insert(id, tx);
    let _pending = CallbackPending {
        bus: Arc::clone(bus),
        id,
    };
    bus.outgoing
        .send(frame(id))
        .map_err(|e| -> ToolError { e.to_string().into() })?;
    let result = match bus.timeout {
        Some(deadline) => tokio::time::timeout(deadline, rx)
            .await
            .map_err(|_| -> ToolError {
                "brain callback deadline elapsed; outcome unknown".into()
            })?,
        None => rx.await,
    };
    let value = result.map_err(|e| -> ToolError { e.to_string().into() })??;
    ToolOutput::from_wire(serde_json::from_value::<ToolOutputWire>(value)?).map_err(Into::into)
}

struct CallbackTool {
    definition: ToolDefinition,
    bus: Arc<CallbackBus>,
}
#[async_trait::async_trait]
impl Tool for CallbackTool {
    fn definition(&self) -> ToolDefinition {
        self.definition.clone()
    }
    async fn execute(&self, input: ToolInput, context: ToolContext<'_>) -> ToolResult {
        let (input, freeform) = match input {
            ToolInput::Function(input) => (serde_json::from_str(input.get())?, false),
            ToolInput::Freeform(input) => (Value::String(input), true),
        };
        let context = RemoteContext::from_context(context);
        callback(&self.bus, |id| Frame::Callback {
            id,
            name: self.definition.name().into(),
            input,
            freeform,
            context,
            hand: None,
        })
        .await
    }
}

struct HandCallbacks {
    bus: Arc<CallbackBus>,
    name: Mutex<Option<String>>,
}

impl HandCalls for HandCallbacks {
    fn here(&self, hand: &str) -> bool {
        self.name.lock().unwrap().as_deref() == Some(hand)
    }

    fn call<'a>(
        &'a self,
        hand: &'a str,
        name: &'a str,
        input: Value,
        context: ToolContext<'a>,
    ) -> HostFuture<'a, ToolOutput> {
        let context = RemoteContext::from_context(context);
        Box::pin(async move {
            callback(&self.bus, |id| Frame::Callback {
                id,
                name: name.into(),
                input,
                freeform: false,
                context,
                hand: Some(hand.into()),
            })
            .await
            .unwrap_or_else(|error| ToolOutput::error(error.to_string()))
        })
    }
}

struct CallbackOrigin {
    id: String,
    turn: u64,
    definitions: Vec<ToolDefinition>,
}

#[derive(Default)]
struct CallbackCatalogState {
    turn: u64,
    origins: Vec<CallbackOrigin>,
}

#[derive(Clone)]
struct CallbackCatalog {
    session_id: Arc<str>,
    state: Arc<Mutex<CallbackCatalogState>>,
    bus: Arc<CallbackBus>,
}

impl CallbackCatalog {
    fn register(
        &self,
        origin: &str,
        definitions: Vec<ToolDefinition>,
    ) -> Result<(), CodeModeHostError> {
        let mut state = self.state.lock().unwrap();
        if state.origins.iter().any(|entry| entry.id == origin) {
            return Err(error("callback origin is already active"));
        }
        let turn = state.turn;
        state.origins.push(CallbackOrigin {
            id: origin.into(),
            turn,
            definitions,
        });
        Ok(())
    }

    fn turn(&self) -> u64 {
        self.state.lock().unwrap().turn
    }

    fn begin_turn(&self) {
        self.state.lock().unwrap().turn += 1;
    }

    fn adopt(&self, origin: &str, turn: u64) {
        if let Some(entry) = self
            .state
            .lock()
            .unwrap()
            .origins
            .iter_mut()
            .find(|entry| entry.id == origin)
        {
            entry.turn = turn;
        }
    }

    fn finish(&self, origin: &str) {
        let mut state = self.state.lock().unwrap();
        if let Some(index) = state.origins.iter().position(|entry| entry.id == origin) {
            state.origins.remove(index);
            let _ = self.bus.outgoing.send(Frame::DetachedComplete {
                session_id: self.session_id.to_string(),
                origin_call_id: origin.into(),
            });
        }
    }

    fn finish_turn(&self, turn: Option<u64>) {
        let origins = self
            .state
            .lock()
            .unwrap()
            .origins
            .iter()
            .filter(|entry| turn.is_none_or(|turn| entry.turn == turn))
            .map(|entry| entry.id.clone())
            .collect::<Vec<_>>();
        for origin in origins {
            self.finish(&origin);
        }
    }
}

struct CallbackOriginGuard {
    catalog: CallbackCatalog,
    origin: String,
    retained: bool,
}

impl Drop for CallbackOriginGuard {
    fn drop(&mut self) {
        if !self.retained {
            self.catalog.finish(&self.origin);
        }
    }
}

#[async_trait::async_trait]
impl crate::runtime::DynamicToolProvider for CallbackCatalog {
    fn start(&self) {}
    fn direct_tools(&self) -> Vec<Arc<dyn Tool>> {
        Vec::new()
    }
    fn available_definitions(&self) -> Vec<ToolDefinition> {
        let state = self.state.lock().unwrap();
        let mut names = HashSet::new();
        state
            .origins
            .iter()
            .rev()
            .flat_map(|entry| entry.definitions.iter())
            .filter(|definition| names.insert(definition.name().to_owned()))
            .cloned()
            .collect()
    }
    async fn execute(
        &self,
        name: &str,
        input: Value,
        context: ToolContext<'_>,
    ) -> Option<ToolOutput> {
        let origin = context
            .call_id()
            .rsplit_once("/code-")
            .map_or(context.call_id(), |(origin, _)| origin);
        let definition = self
            .state
            .lock()
            .unwrap()
            .origins
            .iter()
            .find(|entry| entry.id == origin)?
            .definitions
            .iter()
            .find(|definition| definition.name() == name)
            .cloned()?;
        let input = match &definition {
            ToolDefinition::Custom { .. } => match input.as_str() {
                Some(source) => ToolInput::Freeform(source.into()),
                None => return Some(ToolOutput::error("custom callback input must be a string")),
            },
            _ => match serde_json::value::to_raw_value(&input) {
                Ok(raw) => ToolInput::Function(raw),
                Err(error) => return Some(ToolOutput::error(error.to_string())),
            },
        };
        let proxy = CallbackTool {
            definition,
            bus: Arc::clone(&self.bus),
        };
        Some(match proxy.execute(input, context).await {
            Ok(output) => output,
            Err(error) => ToolOutput::error(error.to_string()),
        })
    }
}

pub async fn serve_connect(
    target: crate::attachment::AttachmentTarget,
    workspace: &Path,
    tools: crate::Tools,
    policy: SocketPolicy,
) -> Result<(), CodeModeHostError> {
    serve(
        connect(target, policy).await?,
        workspace.to_owned(),
        tools,
        policy,
        ExecutorOptions {
            name: None,
            runtime_id: None,
            callback_timeout: Some(policy.request_timeout),
        },
    )
    .await
}
pub async fn serve_accept(
    listener: &TcpListener,
    bearer: &str,
    workspace: &Path,
    tools: crate::Tools,
    policy: SocketPolicy,
) -> Result<(), CodeModeHostError> {
    serve(
        accept(listener, bearer, policy).await?,
        workspace.to_owned(),
        tools,
        policy,
        ExecutorOptions {
            name: None,
            runtime_id: None,
            callback_timeout: Some(policy.request_timeout),
        },
    )
    .await
}
pub async fn serve_connect_with(
    target: crate::attachment::AttachmentTarget,
    workspace: &Path,
    tools: crate::Tools,
    policy: SocketPolicy,
    options: ExecutorOptions,
) -> Result<(), CodeModeHostError> {
    serve(
        connect(target, policy).await?,
        workspace.to_owned(),
        tools,
        policy,
        options,
    )
    .await
}
pub async fn serve_stream(
    stream: TcpStream,
    bearer: &str,
    workspace: &Path,
    tools: crate::Tools,
    policy: SocketPolicy,
    options: ExecutorOptions,
) -> Result<(), CodeModeHostError> {
    serve(
        handshake(stream, bearer, policy).await?,
        workspace.to_owned(),
        tools,
        policy,
        options,
    )
    .await
}

struct Sessions(HashMap<String, (Arc<ToolRuntime>, CallbackCatalog)>);
impl std::ops::Deref for Sessions {
    type Target = HashMap<String, (Arc<ToolRuntime>, CallbackCatalog)>;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
impl std::ops::DerefMut for Sessions {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}
impl Drop for Sessions {
    fn drop(&mut self) {
        for (_, (runtime, _)) in self.0.drain() {
            tokio::spawn(async move {
                runtime.control().cancel().await;
            });
        }
    }
}

pub(super) async fn serve<S: AsyncRead + AsyncWrite + Unpin + Send + 'static>(
    mut socket: WebSocketStream<S>,
    workspace: PathBuf,
    tools: crate::Tools,
    policy: SocketPolicy,
    options: ExecutorOptions,
) -> Result<(), CodeModeHostError> {
    let catalog_runtime = ToolRuntime::new_with_tools(workspace.clone(), Vec::new(), &tools);
    let definitions = catalog_runtime.registered_code_mode_definitions();
    catalog_runtime.control().cancel().await;
    write(
        &mut socket,
        Frame::Catalog {
            definitions,
            name: options.name.clone(),
            workspace: Some(workspace.display().to_string()),
            runtime_id: options.runtime_id,
        },
        policy,
    )
    .await?;
    let (sink, mut socket) = socket.split();
    let liveness = Liveness::new();
    let watchdog = liveness.watch(policy.request_timeout / 2);
    tokio::pin!(watchdog);
    let (outgoing, mut writer) = writer(sink, policy, Arc::clone(&liveness), Arc::default());
    let bus = Arc::new(CallbackBus {
        outgoing: outgoing.clone(),
        pending: Mutex::new(HashMap::new()),
        next: AtomicU64::new(0),
        timeout: options.callback_timeout,
    });
    let callbacks = Arc::new(HandCallbacks {
        bus: Arc::clone(&bus),
        name: Mutex::new(options.name),
    });
    let hands: Arc<dyn HandCalls> = callbacks.clone();
    let mut runtimes = Sessions(HashMap::new());
    let mut tasks = tokio::task::JoinSet::new();
    let result = async {
        loop {
            tokio::select! {
                result = &mut writer.0 => return result.map_err(error)?,
                frame = read_live(&mut socket, &liveness, watchdog.as_mut()) => {
                    let Some(frame) = frame? else { return Ok(()); };
                    match frame {
                        Frame::CallbackResult { id, result } => { if let Some(tx) = bus.pending.lock().unwrap().remove(&id) { let _ = tx.send(result); } },
                        Frame::Request { id, operation } => {
                            let session_id = match &operation { RemoteOperation::Execute { context, .. } | RemoteOperation::Wait { context, .. } | RemoteOperation::Call { context, .. } => &context.session_id, RemoteOperation::Cancel { session_id } | RemoteOperation::CancelTurn { session_id } | RemoteOperation::DetachTurn { session_id } | RemoteOperation::BeginTurn { session_id } | RemoteOperation::Preempt { session_id } | RemoteOperation::SettlePreempt { session_id } => session_id }.clone();
                            if matches!(&operation, RemoteOperation::Execute { .. } | RemoteOperation::Call { .. }) && !runtimes.contains_key(&session_id) {
                                let catalog = CallbackCatalog {
                                    session_id: Arc::from(session_id.as_str()),
                                    state: Arc::new(Mutex::new(CallbackCatalogState::default())),
                                    bus: Arc::clone(&bus),
                                };
                                match tools.clone().into_builder().provider(catalog.clone()).hand_calls(Arc::clone(&hands)).build() {
                                    Ok(selected) => { runtimes.insert(session_id.clone(), (Arc::new(ToolRuntime::new_with_tools(workspace.clone(), Vec::new(), &selected)), catalog)); },
                                    Err(issue) => {
                                        outgoing.send(Frame::Reply { id, result: Err(issue.to_string()) }).map_err(error)?;
                                        continue;
                                    }
                                }
                            }
                            if let RemoteOperation::Execute { hand: Some(hand), .. } = &operation {
                                *callbacks.name.lock().unwrap() = Some(hand.clone());
                            }
                            if let RemoteOperation::Execute { local_tools, context, .. } = &operation
                                && let Some((_, catalog)) = runtimes.get(&session_id)
                                && let Err(issue) = catalog.register(&context.call_id, local_tools.clone())
                            {
                                outgoing.send(Frame::Reply { id, result: Err(issue.to_string()) }).map_err(error)?;
                                continue;
                            }
                            let runtime = if matches!(&operation, RemoteOperation::Cancel { .. }) { runtimes.0.remove(&session_id) } else { runtimes.get(&session_id).map(|(runtime, catalog)| (Arc::clone(runtime), catalog.clone())) };
                            let runtime = runtime.map(|(runtime, catalog)| {
                                let turn = catalog.turn();
                                (runtime, catalog, turn)
                            });
                            if let Some((runtime, catalog, turn)) = &runtime {
                                match &operation {
                                    RemoteOperation::CancelTurn { .. } => { runtime.control().cancel_turn_with_updates(&mut Observer { id, outgoing:outgoing.clone() }).await; catalog.finish_turn(Some(*turn)); outgoing.send(Frame::Reply { id, result:Ok(None) }).map_err(error)?; continue; },
                                    RemoteOperation::BeginTurn { .. } => { runtime.control().begin_turn(); catalog.begin_turn(); outgoing.send(Frame::Reply { id, result:Ok(None) }).map_err(error)?; continue; },
                                    RemoteOperation::SettlePreempt { .. } => { runtime.control().settle_preempt(); outgoing.send(Frame::Reply { id, result:Ok(None) }).map_err(error)?; continue; },
                                    RemoteOperation::DetachTurn { session_id } => { runtime.control().detach_turn_with_updates(&mut |origin| Box::new(DetachedObserver { session_id:session_id.clone(), origin_call_id:origin.into(), outgoing:outgoing.clone(), catalog:catalog.clone() })).await; outgoing.send(Frame::Reply { id, result:Ok(None) }).map_err(error)?; continue; },
                                    _ => {},
                                }
                            }
                            let outgoing = outgoing.clone();
                            tasks.spawn(async move {
                                let result = async {
                                    let Some((runtime, catalog, turn)) = runtime else {
                                        return match operation { RemoteOperation::Execute { .. } | RemoteOperation::Wait { .. } | RemoteOperation::Call { .. } => Err(error("unknown runner session")), _ => Ok(None) };
                                    };
                                    let mut observer = Observer { id, outgoing:outgoing.clone() };
                                    match operation {
                                        RemoteOperation::Execute { source, context, .. } => {
                                            let mut origin = CallbackOriginGuard { catalog:catalog.clone(), origin:context.call_id.clone(), retained:false };
                                            let result = runtime.execute_code_owned_with_updates(&source, context.owned(), &mut observer).await;
                                            origin.retained = result.as_ref().is_ok_and(|execution| execution.cell.as_ref().is_some_and(|cell| cell.running));
                                            result.map(|value| Some(execution_to_wire(value)))
                                        },
                                        RemoteOperation::Wait { input, context } => {
                                            let execution = runtime.wait_for_code_with_updates(&input, context.owned().as_context(), &mut observer).await?;
                                            if let Some(cell) = &execution.cell {
                                                if cell.running { catalog.adopt(&cell.origin_call_id, turn); }
                                                else { catalog.finish(&cell.origin_call_id); }
                                            }
                                            Ok(Some(execution_to_wire(execution)))
                                        },
                                        RemoteOperation::Call { name, input, context } => {
                                            let owned = context.owned();
                                            let output = runtime.execute_nested_tool(&name, input, owned.as_context()).await;
                                            serde_json::to_value(output.into_wire().map_err(error)?).map(Some).map_err(error)
                                        },
                                        RemoteOperation::Cancel { .. } => { runtime.control().cancel_with_updates(&mut observer).await; catalog.finish_turn(None); Ok(None) },
                                        RemoteOperation::CancelTurn { .. } => { runtime.control().cancel_turn_with_updates(&mut observer).await; catalog.finish_turn(Some(turn)); Ok(None) },
                                        RemoteOperation::DetachTurn { session_id } => { runtime.control().detach_turn_with_updates(&mut |origin| Box::new(DetachedObserver { session_id:session_id.clone(), origin_call_id:origin.into(), outgoing:outgoing.clone(), catalog:catalog.clone() })).await; Ok(None) },
                                    RemoteOperation::BeginTurn { .. } => { runtime.control().begin_turn(); catalog.begin_turn(); Ok(None) },
                                        RemoteOperation::Preempt { .. } => { runtime.control().preempt_turn().await; Ok(None) },
                                        RemoteOperation::SettlePreempt { .. } => { runtime.control().settle_preempt(); Ok(None) },
                                    }
                                }.await.map_err(|e| e.to_string());
                                let _ = outgoing.send(Frame::Reply { id, result });
                            });
                        },
                        _ => return Err(error("unexpected brain frame")),
                    }
                },
                Some(result) = tasks.join_next(), if !tasks.is_empty() => { result.map_err(error)?; },
            }
        }
    }.await;
    tasks.abort_all();
    bus.pending.lock().unwrap().clear();
    for (_, (runtime, _)) in runtimes.0.drain() {
        runtime.control().cancel().await;
    }
    result
}
