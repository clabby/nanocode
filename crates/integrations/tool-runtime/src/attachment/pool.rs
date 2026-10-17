use super::{
    AttachmentError, AttachmentTarget, RemoteMode, RemoteRunner, RunnerConnection, routing::Route,
};
use futures_util::{SinkExt, StreamExt, future};
use std::{
    collections::{BTreeMap, HashSet},
    fmt,
    path::PathBuf,
    sync::{Arc, Mutex, MutexGuard, PoisonError, Weak},
    time::Duration,
};
use tokio::{
    net::{TcpListener, TcpStream},
    sync::{mpsc, watch},
    task::{AbortHandle, JoinHandle, JoinSet},
};
use tokio_tungstenite::tungstenite::{
    self, Message,
    handshake::server::{ErrorResponse, Request, Response},
    protocol::{CloseFrame, frame::coding::CloseCode},
};
use tokio_util::sync::CancellationToken;

#[derive(Clone)]
pub struct Runner {
    endpoint: String,
    token: String,
    name: String,
    mode: RemoteMode,
    timeout: Option<Duration>,
    workspace: Option<PathBuf>,
}

impl Runner {
    pub fn workspace(mut self, workspace: impl Into<PathBuf>) -> Self {
        self.workspace = Some(workspace.into());
        self
    }
    pub fn validate(&self) -> Result<(), AttachmentError> {
        if self
            .workspace
            .as_ref()
            .is_some_and(|workspace| !workspace.is_absolute())
        {
            return Err(AttachmentError::Catalog(
                "runner workspace override must be absolute".into(),
            ));
        }
        AttachmentTarget::new(&self.endpoint, self.token.clone())?;
        let remote = RemoteRunner::new(&self.name)?;
        if let Some(timeout) = self.timeout {
            remote.with_call_timeout(timeout)?;
        }
        Ok(())
    }
    pub fn connect(url: impl Into<String>) -> Self {
        Self {
            endpoint: url.into(),
            token: String::new(),
            name: "runner".into(),
            mode: RemoteMode::Alongside,
            timeout: None,
            workspace: None,
        }
    }
    pub fn token(mut self, token: impl Into<String>) -> Self {
        self.token = token.into();
        self
    }
    pub fn name(mut self, name: impl Into<String>) -> Self {
        self.name = name.into();
        self
    }
    pub fn alongside(mut self) -> Self {
        self.mode = RemoteMode::Alongside;
        self
    }
    pub fn replace(mut self) -> Self {
        self.mode = RemoteMode::Replace;
        self
    }
    pub fn call_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }
}
impl fmt::Debug for Runner {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Runner")
            .field("endpoint", &self.endpoint)
            .field("name", &self.name)
            .field("mode", &self.mode)
            .finish_non_exhaustive()
    }
}

#[derive(Clone)]
pub struct RunnerServerConfig {
    address: String,
    token: Option<String>,
    timeout: Option<Duration>,
    mode: RemoteMode,
    workspace: Option<PathBuf>,
}
impl RunnerServerConfig {
    pub fn workspace(mut self, workspace: impl Into<PathBuf>) -> Self {
        self.workspace = Some(workspace.into());
        self
    }
    pub fn validate(&self) -> Result<(), AttachmentError> {
        if self
            .workspace
            .as_ref()
            .is_some_and(|workspace| !workspace.is_absolute())
        {
            return Err(AttachmentError::Catalog(
                "runner workspace override must be absolute".into(),
            ));
        }
        if self.address.trim().is_empty() {
            return Err(error("runner listen address must not be empty"));
        }
        if self
            .token
            .as_ref()
            .is_some_and(|token| token.trim().is_empty())
        {
            return Err(AttachmentError::Authentication(
                "bearer credential must not be empty".into(),
            ));
        }
        if let Some(timeout) = self.timeout {
            RemoteRunner::new("validation")?.with_call_timeout(timeout)?;
        }
        Ok(())
    }
    pub fn listen(address: impl Into<String>) -> Self {
        Self {
            address: address.into(),
            token: None,
            timeout: None,
            workspace: None,
            mode: RemoteMode::Alongside,
        }
    }
    pub fn token(mut self, token: impl Into<String>) -> Self {
        self.token = Some(token.into());
        self
    }
    pub fn alongside(mut self) -> Self {
        self.mode = RemoteMode::Alongside;
        self
    }
    pub fn replace(mut self) -> Self {
        self.mode = RemoteMode::Replace;
        self
    }
    pub fn call_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }
}
impl fmt::Debug for RunnerServerConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RunnerServerConfig")
            .field("address", &self.address)
            .finish_non_exhaustive()
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RunnerStatus {
    Connecting,
    Ready,
    Disconnected,
    Failed(String),
}
#[derive(Clone, Debug)]
pub struct RunnerInfo {
    pub name: String,
    pub endpoint: String,
    pub mode: RemoteMode,
    pub status: RunnerStatus,
    pub workspace: Option<PathBuf>,
    pub tool_count: usize,
}
#[derive(Clone, Debug)]
pub enum RunnerEvent {
    StatusChanged { info: RunnerInfo },
    Failed { name: String, error: String },
}

#[derive(Clone)]
pub struct RunnerPool {
    inner: Arc<PoolInner>,
    model_management: bool,
}
impl Default for RunnerPool {
    fn default() -> Self {
        Self {
            inner: Arc::default(),
            model_management: true,
        }
    }
}
#[derive(Default)]
struct PoolInner {
    state: Mutex<PoolState>,
}
#[derive(Default)]
struct PoolState {
    runners: BTreeMap<String, Entry>,
    revocations: HashSet<(String, Box<str>)>,
    servers: Vec<Arc<ServerInner>>,
    subscribers: Vec<mpsc::UnboundedSender<RunnerEvent>>,
    closed: bool,
}
struct Entry {
    remote: RemoteRunner,
    endpoint: String,
    mode: RemoteMode,
    status: watch::Sender<RunnerStatus>,
    connection: Option<RunnerConnection>,
    cancel: CancellationToken,
    monitor: Option<AbortHandle>,
    identity: Arc<()>,
    active: bool,
    workspace: Option<PathBuf>,
}
impl Drop for Entry {
    fn drop(&mut self) {
        self.cancel.cancel();
        if let Some(monitor) = self.monitor.take() {
            monitor.abort();
        }
    }
}
impl PoolInner {
    fn state(&self) -> MutexGuard<'_, PoolState> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }
    fn update(&self, name: &str, status: RunnerStatus, identity: &Arc<()>) {
        let mut state = self.state();
        let Some(entry) = state.runners.get_mut(name) else {
            return;
        };
        if !Arc::ptr_eq(&entry.identity, identity)
            || !entry.active
            || *entry.status.borrow() == status
        {
            return;
        }
        entry.status.send_replace(status.clone());
        let info = entry.info(name);
        tracing::info!(target: "nanocode_code_mode::attachment", runner = name, ?status, "runner status changed");
        emit(&mut state, RunnerEvent::StatusChanged { info });
        if let RunnerStatus::Failed(error) = status {
            emit(
                &mut state,
                RunnerEvent::Failed {
                    name: name.into(),
                    error,
                },
            );
        }
    }
}
impl Entry {
    fn info(&self, name: &str) -> RunnerInfo {
        let tools = self.remote.tools_snapshot();
        RunnerInfo {
            name: name.into(),
            endpoint: self.endpoint.clone(),
            mode: self.mode,
            status: self.status.borrow().clone(),
            workspace: self.workspace.clone().or_else(|| {
                tools
                    .as_ref()
                    .and_then(|tools| tools.workspace().map(PathBuf::from))
            }),
            tool_count: tools.map_or(0, |tools| tools.iter().count()),
        }
    }
}
fn emit(state: &mut PoolState, event: RunnerEvent) {
    state
        .subscribers
        .retain(|sender| sender.send(event.clone()).is_ok());
}
fn error(value: impl fmt::Display) -> AttachmentError {
    AttachmentError::Transport(value.to_string().into())
}

impl RunnerPool {
    pub fn new() -> Self {
        Self::default()
    }
    pub fn with_model_management(mut self, enabled: bool) -> Self {
        self.model_management = enabled;
        self
    }
    pub fn model_management_enabled(&self) -> bool {
        self.model_management
    }
    pub fn report_failure(&self, name: impl Into<String>, failure: impl Into<String>) {
        emit(
            &mut self.inner.state(),
            RunnerEvent::Failed {
                name: name.into(),
                error: failure.into(),
            },
        );
    }
    pub fn servers(&self) -> Vec<RunnerServer> {
        self.inner
            .state()
            .servers
            .iter()
            .filter(|server| !server.cancel.is_cancelled())
            .map(|inner| RunnerServer {
                inner: inner.clone(),
            })
            .collect()
    }
    pub fn start(&self, config: Runner) -> Result<RunnerHandle, AttachmentError> {
        let name = config.name.clone();
        let result = self.start_inner(config);
        if let Err(error) = &result {
            self.report_failure(name, error.to_string());
        }
        result
    }
    fn start_inner(&self, config: Runner) -> Result<RunnerHandle, AttachmentError> {
        config.validate()?;
        tokio::runtime::Handle::try_current().map_err(error)?;
        super::install_default_rustls_crypto_provider();
        let target = AttachmentTarget::new(&config.endpoint, config.token)?;
        let mut remote = RemoteRunner::new(&config.name)?;
        if let Some(timeout) = config.timeout {
            remote = remote.with_call_timeout(timeout)?;
        }
        let mut state = self.inner.state();
        validate_registration(&state, &config.name, config.mode)?;
        if config.mode == RemoteMode::Replace
            && state
                .servers
                .iter()
                .any(|server| server.mode == RemoteMode::Replace && !server.cancel.is_cancelled())
        {
            return Err(AttachmentError::Catalog(
                "a replacement listener is already configured".into(),
            ));
        }
        let connection = remote.connect(target)?;
        let status = watch::channel(RunnerStatus::Connecting).0;
        let identity = Arc::new(());
        let handle = RunnerHandle {
            pool: self.clone(),
            name: config.name.clone(),
            endpoint: config.endpoint.clone(),
            mode: config.mode,
            workspace: config.workspace.clone(),
            remote: remote.clone(),
            identity: identity.clone(),
            status: status.subscribe(),
        };
        let monitor = monitor(
            Arc::downgrade(&self.inner),
            config.name.clone(),
            remote.clone(),
            identity.clone(),
        );
        state.runners.insert(
            config.name.clone(),
            Entry {
                remote,
                endpoint: config.endpoint,
                mode: config.mode,
                status,
                connection: Some(connection),
                cancel: CancellationToken::new(),
                monitor: Some(monitor),
                identity,
                active: true,
                workspace: config.workspace,
            },
        );
        let info = state.runners[&config.name].info(&config.name);
        emit(&mut state, RunnerEvent::StatusChanged { info });
        Ok(handle)
    }
    pub fn connect<'call>(
        &'call self,
        config: Runner,
    ) -> futures_util::future::BoxFuture<'call, Result<RunnerHandle, AttachmentError>> {
        Box::pin(async move {
            let timeout = config.timeout;
            let handle = self.start(config)?;
            let wait = handle.ready();
            let result = match timeout {
                Some(timeout) => tokio::time::timeout(timeout, wait)
                    .await
                    .map_err(error)
                    .and_then(|result| result),
                None => wait.await,
            };
            if let Err(error) = result {
                self.report_failure(handle.name.clone(), error.to_string());
                handle.disconnect().await?;
                return Err(error);
            }
            Ok(handle)
        })
    }
    pub fn list(&self) -> Vec<RunnerInfo> {
        self.inner
            .state()
            .runners
            .iter()
            .map(|(name, entry)| entry.info(name))
            .collect()
    }
    pub fn subscribe(&self) -> mpsc::UnboundedReceiver<RunnerEvent> {
        let (sender, receiver) = mpsc::unbounded_channel();
        self.inner.state().subscribers.push(sender);
        receiver
    }
    pub fn disconnect<'call>(
        &'call self,
        name: &'call str,
    ) -> futures_util::future::BoxFuture<'call, Result<(), AttachmentError>> {
        Box::pin(async move { self.disconnect_entry(name, None).await })
    }
    async fn disconnect_entry(
        &self,
        name: &str,
        identity: Option<&Arc<()>>,
    ) -> Result<(), AttachmentError> {
        Self::wait_disconnected(self.begin_disconnect(name, identity)?).await
    }
    fn begin_disconnect(
        &self,
        name: &str,
        identity: Option<&Arc<()>>,
    ) -> Result<RemoteRunner, AttachmentError> {
        let remote = {
            let mut state = self.inner.state();
            let entry = state
                .runners
                .get_mut(name)
                .ok_or_else(|| error(format!("runner `{name}` is not registered")))?;
            if identity.is_some_and(|identity| !Arc::ptr_eq(&entry.identity, identity)) {
                return Err(error(format!(
                    "runner handle `{name}` is stale; the name belongs to a newer registration"
                )));
            }
            entry.active = false;
            entry.cancel.cancel();
            if let Some(monitor) = entry.monitor.take() {
                monitor.abort();
            }
            entry.connection.take();
            entry.status.send_replace(RunnerStatus::Disconnected);
            let remote = entry.remote.clone();
            let revoked = remote
                .runtime_id()
                .map(|runtime_id| (name.to_owned(), runtime_id));
            let info = entry.info(name);
            if let Some(revoked) = revoked {
                state.revocations.insert(revoked);
            }
            emit(&mut state, RunnerEvent::StatusChanged { info });
            remote
        };
        remote.revoke();
        Ok(remote)
    }
    async fn wait_disconnected(remote: RemoteRunner) -> Result<(), AttachmentError> {
        let mut connected = remote.connection_status();
        connected
            .wait_for(|connected| !connected)
            .await
            .map_err(error)?;
        Ok(())
    }
    pub fn shutdown<'call>(
        &'call self,
    ) -> futures_util::future::BoxFuture<'call, Result<(), AttachmentError>> {
        Box::pin(async move {
            let (names, servers) = {
                let mut state = self.inner.state();
                state.closed = true;
                (
                    state.runners.keys().cloned().collect::<Vec<_>>(),
                    std::mem::take(&mut state.servers),
                )
            };
            for inner in &servers {
                inner.cancel.cancel();
            }
            let remotes = names
                .into_iter()
                .map(|name| self.begin_disconnect(&name, None))
                .collect::<Result<Vec<_>, _>>()?;
            for remote in remotes {
                Self::wait_disconnected(remote).await?;
            }
            for inner in servers {
                RunnerServer { inner }.stop().await?;
            }
            self.inner.state().subscribers.clear();
            Ok(())
        })
    }
    pub(crate) fn routes(&self) -> Vec<Route> {
        self.inner
            .state()
            .runners
            .values()
            .filter(|entry| entry.active && entry.remote.is_connected())
            .filter_map(|entry| {
                let tools = entry.remote.tools_snapshot()?;
                Some(Route {
                    workspace: entry
                        .workspace
                        .clone()
                        .or_else(|| tools.workspace().map(PathBuf::from))?,
                    tools,
                    mode: entry.mode,
                })
            })
            .collect()
    }
    pub(crate) fn reserved_mounts(&self) -> Vec<String> {
        self.inner.state().runners.keys().cloned().collect()
    }
    pub(crate) fn replacement(&self) -> Option<String> {
        let state = self.inner.state();
        state
            .runners
            .iter()
            .find(|(_, entry)| entry.mode == RemoteMode::Replace)
            .map(|(name, _)| name.clone())
            .or_else(|| {
                state
                    .servers
                    .iter()
                    .find(|server| {
                        server.mode == RemoteMode::Replace && !server.cancel.is_cancelled()
                    })
                    .map(|_| "incoming replacement runner".into())
            })
    }
    pub fn listen<'call>(
        &'call self,
        config: RunnerServerConfig,
    ) -> futures_util::future::BoxFuture<'call, Result<RunnerServer, AttachmentError>> {
        Box::pin(async move {
            super::install_default_rustls_crypto_provider();
            config.validate()?;
            let token = match config.token {
                Some(token) if !token.trim().is_empty() => token,
                Some(_) => {
                    return Err(AttachmentError::Authentication(
                        "bearer credential must not be empty".into(),
                    ));
                }
                None => {
                    let mut bytes = [0u8; 32];
                    getrandom::fill(&mut bytes).map_err(error)?;
                    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
                }
            };
            let listener = TcpListener::bind(&config.address).await.map_err(error)?;
            let address = listener.local_addr().map_err(error)?;
            if !address.ip().is_loopback() {
                return Err(error(
                    "plaintext runner listeners require a loopback address; use a TLS proxy for remote access",
                ));
            }
            let endpoint = format!("ws://{address}/");
            let cancel = CancellationToken::new();
            let weak = Arc::downgrade(&self.inner);
            let mut state = self.inner.state();
            if state.closed {
                return Err(AttachmentError::Closed);
            }
            if config.mode == RemoteMode::Replace
                && (state
                    .runners
                    .values()
                    .any(|entry| entry.mode == RemoteMode::Replace)
                    || state.servers.iter().any(|server| {
                        server.mode == RemoteMode::Replace && !server.cancel.is_cancelled()
                    }))
            {
                return Err(AttachmentError::Catalog(
                    "a replacement runner or listener is already configured".into(),
                ));
            }
            let task = tokio::spawn(accept(
                listener,
                weak,
                token.clone(),
                endpoint.clone(),
                config.timeout,
                config.mode,
                config.workspace,
                cancel.clone(),
            ));
            let inner = Arc::new(ServerInner {
                endpoint,
                token,
                cancel,
                task: Mutex::new(Some(task)),
                pool: Arc::downgrade(&self.inner),
                mode: config.mode,
            });
            state.servers.push(inner.clone());
            Ok(RunnerServer { inner })
        })
    }
}
fn validate_registration(
    state: &PoolState,
    name: &str,
    mode: RemoteMode,
) -> Result<(), AttachmentError> {
    if state.closed {
        return Err(AttachmentError::Closed);
    }
    if state.runners.get(name).is_some_and(|entry| entry.active) {
        return Err(AttachmentError::Catalog(
            format!("runner `{name}` is already registered").into(),
        ));
    }
    if mode == RemoteMode::Replace
        && state
            .runners
            .iter()
            .any(|(existing, entry)| existing != name && entry.mode == RemoteMode::Replace)
    {
        return Err(AttachmentError::Catalog(
            "a replacement runner is already configured".into(),
        ));
    }
    Ok(())
}
fn monitor(
    pool: Weak<PoolInner>,
    name: String,
    remote: RemoteRunner,
    identity: Arc<()>,
) -> AbortHandle {
    tokio::spawn(async move {
        let mut connected = remote.connection_status();
        let mut failure = remote.failure_status();
        loop {
            let status = if let Some(error) = failure.borrow().clone() { RunnerStatus::Failed(error.to_string()) } else if *connected.borrow() { RunnerStatus::Ready } else { RunnerStatus::Disconnected };
            let Some(pool) = pool.upgrade() else { return };
            if status != RunnerStatus::Disconnected || remote.tools_snapshot().is_some() { pool.update(&name, status, &identity); }
            drop(pool);
            tokio::select! { result = connected.changed() => if result.is_err() { return }, result = failure.changed() => if result.is_err() { return } }
        }
    }).abort_handle()
}

#[derive(Clone)]
pub struct RunnerHandle {
    pool: RunnerPool,
    name: String,
    endpoint: String,
    mode: RemoteMode,
    workspace: Option<PathBuf>,
    remote: RemoteRunner,
    identity: Arc<()>,
    status: watch::Receiver<RunnerStatus>,
}
impl RunnerHandle {
    pub fn info(&self) -> RunnerInfo {
        let tools = self.remote.tools_snapshot();
        RunnerInfo {
            name: self.name.clone(),
            endpoint: self.endpoint.clone(),
            mode: self.mode,
            status: self.status(),
            workspace: self.workspace.clone().or_else(|| {
                tools
                    .as_ref()
                    .and_then(|tools| tools.workspace().map(PathBuf::from))
            }),
            tool_count: tools.map_or(0, |tools| tools.iter().count()),
        }
    }
    pub fn status(&self) -> RunnerStatus {
        self.status.borrow().clone()
    }
    pub fn subscribe(&self) -> watch::Receiver<RunnerStatus> {
        self.status.clone()
    }
    pub fn disconnect<'call>(
        &'call self,
    ) -> futures_util::future::BoxFuture<'call, Result<(), AttachmentError>> {
        Box::pin(async move {
            self.pool
                .disconnect_entry(&self.name, Some(&self.identity))
                .await
        })
    }
    async fn ready(&self) -> Result<(), AttachmentError> {
        let mut status = self.subscribe();
        loop {
            match status.borrow().clone() {
                RunnerStatus::Ready => return Ok(()),
                RunnerStatus::Failed(reason) => {
                    let failure = self.remote.failure_status().borrow().clone();
                    return Err(failure.unwrap_or_else(|| error(reason)));
                }
                RunnerStatus::Disconnected => return Err(AttachmentError::Closed),
                RunnerStatus::Connecting => {}
            }
            status.changed().await.map_err(error)?;
        }
    }
}
impl fmt::Debug for RunnerHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.info().fmt(f)
    }
}

#[derive(Clone)]
pub struct RunnerServer {
    inner: Arc<ServerInner>,
}
struct ServerInner {
    endpoint: String,
    token: String,
    cancel: CancellationToken,
    task: Mutex<Option<JoinHandle<()>>>,
    pool: Weak<PoolInner>,
    mode: RemoteMode,
}
impl Drop for ServerInner {
    fn drop(&mut self) {
        self.cancel.cancel();
        if let Some(task) = self
            .task
            .get_mut()
            .unwrap_or_else(PoisonError::into_inner)
            .take()
        {
            task.abort();
        }
    }
}
impl RunnerServer {
    pub fn endpoint(&self) -> &str {
        &self.inner.endpoint
    }
    pub fn token(&self) -> &str {
        &self.inner.token
    }
    pub fn stop<'call>(
        &'call self,
    ) -> futures_util::future::BoxFuture<'call, Result<(), AttachmentError>> {
        Box::pin(async move {
            self.inner.cancel.cancel();
            let task = self
                .inner
                .task
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .take();
            if let Some(task) = task {
                task.await.map_err(error)?;
            }
            if let Some(inner) = self.inner.pool.upgrade() {
                let pool = RunnerPool {
                    inner,
                    model_management: true,
                };
                let names = pool
                    .list()
                    .into_iter()
                    .filter(|info| info.endpoint == self.endpoint())
                    .map(|info| info.name)
                    .collect::<Vec<_>>();
                for name in names {
                    pool.disconnect(&name).await?;
                }
            }
            Ok(())
        })
    }
}
impl fmt::Debug for RunnerServer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RunnerServer")
            .field("endpoint", &self.endpoint())
            .finish_non_exhaustive()
    }
}
async fn accept(
    listener: TcpListener,
    pool: Weak<PoolInner>,
    token: String,
    endpoint: String,
    timeout: Option<Duration>,
    mode: RemoteMode,
    workspace: Option<PathBuf>,
    cancel: CancellationToken,
) {
    let mut connections = JoinSet::new();
    loop {
        tokio::select! {
            _ = cancel.cancelled() => { connections.shutdown().await; return },
            accepted = listener.accept() => match accepted {
                Ok((socket, _)) => {
                    let pool = pool.clone(); let token = token.clone(); let endpoint = endpoint.clone(); let workspace = workspace.clone();
                    connections.spawn(async move {
                        if let Err(error) = incoming(socket, pool.clone(), token, endpoint, timeout, mode, workspace).await {
                            tracing::warn!(target: "nanocode_code_mode::attachment", %error, "runner listener connection failed");
                            if let Some(pool) = pool.upgrade() { emit(&mut pool.state(), RunnerEvent::Failed { name: String::new(), error: error.to_string() }); }
                        }
                    });
                },
                Err(error) => { tracing::warn!(target: "nanocode_code_mode::attachment", %error, "runner listener accept failed"); return }
            },
            Some(_) = connections.join_next(), if !connections.is_empty() => {}
        }
    }
}
async fn incoming(
    stream: TcpStream,
    pool: Weak<PoolInner>,
    token: String,
    endpoint: String,
    timeout: Option<Duration>,
    mode: RemoteMode,
    workspace: Option<PathBuf>,
) -> Result<(), AttachmentError> {
    let authenticate = |request: &Request, mut response: Response| {
        let bearer = request
            .headers()
            .get(http::header::AUTHORIZATION)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.strip_prefix("Bearer "));
        let protocol = request
            .headers()
            .get(http::header::SEC_WEBSOCKET_PROTOCOL)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| {
                let mut values = value.split(',').map(str::trim);
                if values.next() == Some("bearer") {
                    values.next()
                } else {
                    None
                }
            });
        if bearer.or(protocol) == Some(token.as_str()) {
            if protocol.is_some() {
                response.headers_mut().insert(
                    http::header::SEC_WEBSOCKET_PROTOCOL,
                    http::HeaderValue::from_static("bearer"),
                );
            }
            Ok(response)
        } else {
            let mut response = ErrorResponse::new(Some("invalid runner credential".into()));
            *response.status_mut() = http::StatusCode::UNAUTHORIZED;
            Err(response)
        }
    };
    let socket = tokio::time::timeout(
        super::driver::HANDSHAKE_TIMEOUT,
        tokio_tungstenite::accept_hdr_async(stream, authenticate),
    )
    .await
    .map_err(error)?
    .map_err(error)?;
    let mut frames = socket
        .with(|text: String| future::ready(Ok::<_, tungstenite::Error>(Message::text(text))))
        .filter_map(|message| {
            future::ready(match message {
                Ok(Message::Text(text)) => Some(Ok(text.to_string())),
                Ok(Message::Binary(_)) => Some(Err(tungstenite::Error::Io(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "runner sent a binary frame",
                )))),
                Ok(_) => None,
                Err(error) => Some(Err(error)),
            })
        });
    let first = tokio::time::timeout(super::driver::HANDSHAKE_TIMEOUT, frames.next())
        .await
        .map_err(error)?
        .ok_or(AttachmentError::Closed)?
        .map_err(error)?;
    let catalog: serde_json::Value = serde_json::from_str(&first).map_err(error)?;
    if catalog.get("type").and_then(serde_json::Value::as_str) != Some("catalog") {
        return Err(AttachmentError::Catalog(
            "runner must publish its catalog first".into(),
        ));
    }
    let name = catalog
        .pointer("/machines/0/id")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| {
            AttachmentError::Catalog("incoming runner catalog requires machines[0].id".into())
        })?
        .to_owned();
    let registration = (|| {
        let pool = pool.upgrade().ok_or(AttachmentError::Closed)?;
        let mut state = pool.state();
        if catalog
            .get("runtime_id")
            .and_then(serde_json::Value::as_str)
            .is_some_and(|runtime_id| {
                state
                    .revocations
                    .contains(&(name.clone(), runtime_id.into()))
            })
        {
            return Err(AttachmentError::Fenced(
                super::driver::ATTACHMENT_REVOKED_REASON.into(),
            ));
        }
        validate_registration(&state, &name, mode)?;
        let mut remote = match state.runners.get(&name) {
            Some(entry) if !entry.cancel.is_cancelled() => entry.remote.clone(),
            _ => RemoteRunner::new(&name)?,
        };
        if let Some(timeout) = timeout {
            remote = remote.with_call_timeout(timeout)?;
        }

        let status = watch::channel(RunnerStatus::Connecting).0;
        let cancel = CancellationToken::new();
        let identity = Arc::new(());
        let monitor = monitor(
            Arc::downgrade(&pool),
            name.clone(),
            remote.clone(),
            identity.clone(),
        );
        state.runners.insert(
            name.clone(),
            Entry {
                remote: remote.clone(),
                endpoint,
                mode,
                status,
                connection: None,
                cancel: cancel.clone(),
                monitor: Some(monitor),
                identity: identity.clone(),
                active: true,
                workspace,
            },
        );
        let info = state.runners[&name].info(&name);
        emit(&mut state, RunnerEvent::StatusChanged { info });
        Ok((remote, cancel, identity))
    })();
    let (remote, cancel, identity) = match registration {
        Ok(registration) => registration,
        Err(error) => {
            if error.is_revoked() {
                let _ = frames
                    .get_mut()
                    .get_mut()
                    .close(Some(revoked_close()))
                    .await;
            }
            return Err(error);
        }
    };
    let (result, cancelled) = tokio::select! { biased; _ = cancel.cancelled() => (Ok(()), true), result = remote.serve_catalog(first, &mut frames) => (result, false) };
    if cancelled {
        let _ = frames
            .get_mut()
            .get_mut()
            .close(Some(revoked_close()))
            .await;
    }
    if let Some(pool) = pool.upgrade() {
        pool.update(
            &name,
            match &result {
                Ok(()) => RunnerStatus::Disconnected,
                Err(error) => RunnerStatus::Failed(error.to_string()),
            },
            &identity,
        );
        if let Some(entry) = pool.state().runners.get_mut(&name) {
            if Arc::ptr_eq(&entry.identity, &identity) {
                entry.active = false;
            }
        }
    }
    result
}

fn revoked_close() -> CloseFrame {
    CloseFrame {
        code: CloseCode::Policy,
        reason: super::driver::ATTACHMENT_REVOKED_REASON.into(),
    }
}
