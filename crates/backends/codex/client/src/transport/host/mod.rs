use std::{future::Future, pin::Pin, time::Duration};

#[cfg(target_family = "wasm")]
pub(crate) mod socket;

#[cfg(not(target_family = "wasm"))]
pub type HostFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

#[cfg(target_family = "wasm")]
pub type HostFuture<'a, T> = Pin<Box<dyn Future<Output = T> + 'a>>;

pub trait HostTransport: Send + Sync + 'static {
    fn connect<'a>(
        &'a self,
        request: HostConnectRequest<'a>,
    ) -> HostFuture<'a, Result<ConnectedHost, HostError>>;

    fn http<'a>(
        &'a self,
        _request: HostConnectRequest<'a>,
        _body: &'a str,
    ) -> HostFuture<'a, Result<HostHttpResponse, HostError>> {
        Box::pin(async { Err(HostError::new("host HTTPS transport is unavailable")) })
    }

    fn sleep<'a>(&'a self, session_id: &'a str, duration: Duration) -> HostFuture<'a, ()>;
}

pub trait HostConnection: Send + 'static {
    fn send<'a>(&'a self, message: &'a str) -> HostFuture<'a, Result<(), HostError>>;

    fn next(&mut self) -> HostFuture<'_, Result<HostMessage, HostError>>;

    fn is_closed(&self) -> bool {
        false
    }

    fn close(&mut self);
}

#[derive(Clone, Copy)]
pub struct HostConnectRequest<'a> {
    endpoint: &'a str,
    bearer_token: &'a str,
    account_id: Option<&'a str>,
    fedramp: bool,
    session_id: &'a str,
    thread_id: &'a str,
    turn_state: Option<&'a str>,
    headers: &'a [super::GatewayHeader],
}

impl<'a> HostConnectRequest<'a> {
    #[must_use]
    pub const fn new(
        endpoint: &'a str,
        bearer_token: &'a str,
        account_id: Option<&'a str>,
        fedramp: bool,
        session_id: &'a str,
        turn_state: Option<&'a str>,
    ) -> Self {
        Self::new_with_thread_id(
            endpoint,
            bearer_token,
            account_id,
            fedramp,
            session_id,
            session_id,
            turn_state,
        )
    }

    pub(crate) const fn new_with_thread_id(
        endpoint: &'a str,
        bearer_token: &'a str,
        account_id: Option<&'a str>,
        fedramp: bool,
        session_id: &'a str,
        thread_id: &'a str,
        turn_state: Option<&'a str>,
    ) -> Self {
        Self {
            endpoint,
            bearer_token,
            account_id,
            fedramp,
            session_id,
            thread_id,
            turn_state,
            headers: &[],
        }
    }

    pub(crate) const fn with_headers(mut self, headers: &'a [super::GatewayHeader]) -> Self {
        self.headers = headers;
        self
    }

    #[must_use]
    pub const fn headers(&self) -> &'a [super::GatewayHeader] {
        self.headers
    }

    #[must_use]
    pub const fn endpoint(&self) -> &'a str {
        self.endpoint
    }

    #[must_use]
    pub const fn bearer_token(&self) -> &'a str {
        self.bearer_token
    }

    #[must_use]
    pub const fn sends_bearer_token(&self) -> bool {
        !self.bearer_token.is_empty()
    }

    #[must_use]
    pub const fn account_id(&self) -> Option<&'a str> {
        self.account_id
    }

    #[must_use]
    pub const fn is_fedramp(&self) -> bool {
        self.fedramp
    }

    #[must_use]
    pub const fn session_id(&self) -> &'a str {
        self.session_id
    }

    #[must_use]
    pub const fn thread_id(&self) -> &'a str {
        self.thread_id
    }

    #[must_use]
    pub const fn turn_state(&self) -> Option<&'a str> {
        self.turn_state
    }
}

pub struct ConnectedHost {
    pub(crate) connection: Box<dyn HostConnection>,
    pub(crate) metadata: HostConnectionMetadata,
}

impl ConnectedHost {
    #[must_use]
    pub fn new(connection: impl HostConnection, metadata: HostConnectionMetadata) -> Self {
        Self {
            connection: Box::new(connection),
            metadata,
        }
    }

    #[must_use]
    pub fn into_parts(self) -> (Box<dyn HostConnection>, HostConnectionMetadata) {
        (self.connection, self.metadata)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HostConnectionMetadata {
    pub(crate) status: u16,
    pub(crate) request_id: Option<String>,
    pub(crate) server_model: Option<String>,
    pub(crate) reasoning_included: bool,
    pub(crate) turn_state: Option<String>,
}

impl HostConnectionMetadata {
    #[must_use]
    pub const fn new(status: u16) -> Self {
        Self {
            status,
            request_id: None,
            server_model: None,
            reasoning_included: false,
            turn_state: None,
        }
    }

    #[must_use]
    pub fn with_request_id(mut self, request_id: impl Into<String>) -> Self {
        self.request_id = Some(request_id.into());
        self
    }

    #[must_use]
    pub fn with_server_model(mut self, server_model: impl Into<String>) -> Self {
        self.server_model = Some(server_model.into());
        self
    }

    #[must_use]
    pub const fn with_reasoning_included(mut self, included: bool) -> Self {
        self.reasoning_included = included;
        self
    }

    #[must_use]
    pub fn with_turn_state(mut self, turn_state: impl Into<String>) -> Self {
        self.turn_state = Some(turn_state.into());
        self
    }

    #[must_use]
    pub const fn status(&self) -> u16 {
        self.status
    }

    #[must_use]
    pub fn request_id(&self) -> Option<&str> {
        self.request_id.as_deref()
    }

    #[must_use]
    pub fn server_model(&self) -> Option<&str> {
        self.server_model.as_deref()
    }

    #[must_use]
    pub const fn reasoning_included(&self) -> bool {
        self.reasoning_included
    }

    #[must_use]
    pub fn turn_state(&self) -> Option<&str> {
        self.turn_state.as_deref()
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum HostMessage {
    Text(String),

    Closed { detail: String },

    Binary,
}

#[derive(Clone, Debug, thiserror::Error)]
pub enum HostError {
    #[error("{detail}")]
    Transport { detail: String, reconnectable: bool },

    #[error("WebSocket handshake was rejected with HTTP {status}: {body}")]
    HandshakeRejected {
        status: u16,

        body: String,

        retry_after: Option<super::RetryAfter>,
    },
}

impl HostError {
    #[must_use]
    pub fn new(detail: impl Into<String>) -> Self {
        Self::Transport {
            detail: detail.into(),
            reconnectable: false,
        }
    }

    #[must_use]
    pub fn handshake_rejected(
        status: u16,
        body: impl Into<String>,
        retry_after: Option<super::RetryAfter>,
    ) -> Self {
        Self::HandshakeRejected {
            status,
            body: body.into(),
            retry_after,
        }
    }

    #[must_use]
    pub fn with_reconnectable(self, reconnectable: bool) -> Self {
        match self {
            Self::Transport { detail, .. } => Self::Transport {
                detail,
                reconnectable,
            },
            rejected @ Self::HandshakeRejected { .. } => rejected,
        }
    }

    #[must_use]
    pub fn detail(&self) -> &str {
        match self {
            Self::Transport { detail, .. } => detail,
            Self::HandshakeRejected { body, .. } => body,
        }
    }

    #[must_use]
    pub const fn is_reconnectable(&self) -> bool {
        matches!(
            self,
            Self::Transport {
                reconnectable: true,
                ..
            }
        )
    }
}

pub struct HostHttpResponse {
    pub body: Box<dyn HostHttpBody>,

    pub metadata: HostConnectionMetadata,
}

pub trait HostHttpBody: Send + 'static {
    fn next(&mut self) -> HostFuture<'_, Result<Option<Vec<u8>>, HostError>>;
}
