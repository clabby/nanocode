use crate::client::{OpenAiAuthSnapshot, monotonic_now_ns};
use http::header;

#[cfg(not(target_family = "wasm"))]
use crate::client::RetryAfter;
use crate::client::{EncodedRequest, ResponsesError, socket::ReceivedText};

#[cfg(not(target_family = "wasm"))]
const RESPONSES_LITE_HEADER: &str = "x-openai-internal-codex-responses-lite";
#[cfg(not(target_family = "wasm"))]
const TURN_STATE_HEADER: &str = "x-codex-turn-state";

#[derive(Clone)]
pub(crate) struct ResponsesHttp {
    #[cfg(not(target_family = "wasm"))]
    client: reqwest::Client,

    #[cfg(not(target_family = "wasm"))]
    chat_gateway: Option<(std::sync::Arc<super::chat::ChatGateway>, reqwest::Client)>,
    #[cfg(target_family = "wasm")]
    host: Option<std::sync::Arc<dyn crate::client::transport::host::HostTransport>>,
    gateway: Option<std::sync::Arc<super::ResponsesGateway>>,
}

pub(crate) struct ResponsesHttpStream {
    #[cfg(not(target_family = "wasm"))]
    response: Option<reqwest::Response>,

    #[cfg(not(target_family = "wasm"))]
    chat: Option<Box<super::chat::ChatStream>>,
    #[cfg(target_family = "wasm")]
    response: Box<dyn crate::client::transport::host::HostHttpBody>,
    decoder: SseDecoder,
    ended: bool,
}

pub(crate) struct HttpMetadata {
    pub(crate) reasoning_included: bool,
    pub(crate) turn_state: Option<String>,
}

#[cfg(not(target_family = "wasm"))]
impl ResponsesHttp {
    pub(crate) fn new(
        client: reqwest::Client,
        chat_gateway: Option<std::sync::Arc<super::chat::ChatGateway>>,
        gateway: Option<std::sync::Arc<super::ResponsesGateway>>,
    ) -> Self {
        Self {
            client,
            chat_gateway: chat_gateway
                .map(|gateway| (gateway, super::chat::ChatGateway::http_client())),
            gateway,
        }
    }

    pub(crate) async fn send(
        &self,
        api_base_url: &str,
        auth: &OpenAiAuthSnapshot,
        session_id: &str,
        thread_id: &str,
        turn_state: Option<&str>,
        request: &EncodedRequest,
    ) -> Result<(ResponsesHttpStream, HttpMetadata), ResponsesError> {
        if let Some((gateway, client)) = &self.chat_gateway {
            return gateway
                .send(client, api_base_url, auth, request.raw().get())
                .await;
        }
        let endpoint = format!("{}/responses", api_base_url.trim_end_matches('/'));
        let mut builder = self
            .client
            .post(endpoint)
            .header(header::CONTENT_TYPE, "application/json")
            .header(header::ACCEPT, "text/event-stream")
            .header(RESPONSES_LITE_HEADER, "true")
            .header("session-id", session_id)
            .header("thread-id", thread_id)
            .header("x-client-request-id", thread_id)
            .header(
                header::USER_AGENT,
                concat!("nanocode/", env!("CARGO_PKG_VERSION")),
            )
            .body(request.raw().get().to_owned());
        if auth.sends_bearer() {
            builder = builder.bearer_auth(auth.bearer());
        }
        if let Some(account_id) = auth.account_id() {
            builder = builder.header("ChatGPT-Account-ID", account_id);
        }
        if auth.is_fedramp() {
            builder = builder.header("X-OpenAI-Fedramp", "true");
        }
        for gateway_header in self
            .gateway
            .as_deref()
            .map_or(&[][..], super::ResponsesGateway::headers)
        {
            builder = builder.header(gateway_header.name(), gateway_header.value());
        }
        if let Some(turn_state) =
            turn_state.and_then(|value| header::HeaderValue::from_bytes(value.as_bytes()).ok())
        {
            builder = builder.header(TURN_STATE_HEADER, turn_state);
        }
        let response = builder.send().await.map_err(map_http_error)?;
        let status = response.status();
        if !status.is_success() {
            let retry_after = retry_after(response.headers());

            tracing::debug!(
                target: "nanocode::responses::http",
                status = status.as_u16(),
                retry_after_deadline_ms = retry_after.map(RetryAfter::deadline_unix_ms),
                "Responses HTTPS rejection advice captured before body"
            );
            let body = response.text().await.unwrap_or_default();
            return Err(ResponsesError::http_rejected(
                status.as_u16(),
                body,
                retry_after,
            ));
        }
        let metadata = HttpMetadata {
            reasoning_included: response.headers().contains_key("x-reasoning-included"),
            turn_state: response
                .headers()
                .get(TURN_STATE_HEADER)
                .and_then(|value| value.to_str().ok())
                .map(str::to_owned),
        };
        Ok((
            ResponsesHttpStream {
                response: Some(response),
                chat: None,
                decoder: SseDecoder::default(),
                ended: false,
            },
            metadata,
        ))
    }
}

impl ResponsesHttpStream {
    #[cfg(not(target_family = "wasm"))]
    pub(crate) fn chat(stream: super::chat::ChatStream) -> Self {
        Self {
            response: None,
            chat: Some(Box::new(stream)),
            decoder: SseDecoder::default(),
            ended: true,
        }
    }

    pub(crate) async fn next_text(&mut self) -> Result<ReceivedText, ResponsesError> {
        #[cfg(not(target_family = "wasm"))]
        if let Some(chat) = self.chat.as_mut() {
            return chat.next_text().await;
        }
        loop {
            if let Some(text) = self.decoder.next()? {
                return Ok(ReceivedText {
                    #[cfg(not(target_family = "wasm"))]
                    text: text.into(),
                    #[cfg(target_family = "wasm")]
                    text,
                    received_ns: monotonic_now_ns(),
                    retry_receipt: self
                        .decoder
                        .retry_receipt
                        .unwrap_or_else(super::RetryReceipt::now),
                });
            }
            if self.ended {
                return Err(ResponsesError::UnexpectedEnd);
            }
            #[cfg(not(target_family = "wasm"))]
            let chunk = match self.response.as_mut() {
                Some(response) => response.chunk().await.map_err(map_http_error)?,
                None => None,
            };
            #[cfg(target_family = "wasm")]
            let chunk = self.response.next().await.map_err(map_host_error)?;
            if let Some(chunk) = chunk {
                self.decoder.push(&chunk);
            } else {
                self.ended = true;
                self.decoder.finish();
            }
        }
    }
}

#[derive(Default)]
pub(crate) struct SseDecoder {
    bytes: Vec<u8>,
    cursor: usize,
    scan_start: usize,
    data: Vec<String>,
    finished: bool,
    retry_receipt: Option<super::RetryReceipt>,
}

impl SseDecoder {
    pub(crate) fn push(&mut self, chunk: &[u8]) {
        self.retry_receipt = Some(super::RetryReceipt::now());
        self.compact();
        self.bytes.extend_from_slice(chunk);
    }

    pub(crate) fn finish(&mut self) {
        self.finished = true;
        self.compact();
        if !self.bytes.is_empty() {
            self.bytes.push(b'\n');
        }
        self.bytes.push(b'\n');
    }

    pub(crate) fn next(&mut self) -> Result<Option<String>, ResponsesError> {
        loop {
            let Some(relative_newline) = memchr::memchr(b'\n', &self.bytes[self.scan_start..])
            else {
                self.scan_start = self.bytes.len();
                return Ok(None);
            };
            let line_start = self.cursor;
            let newline = self.scan_start + relative_newline;
            self.cursor = newline + 1;
            self.scan_start = self.cursor;
            let line_end = if newline > line_start && self.bytes.get(newline - 1) == Some(&b'\r') {
                newline - 1
            } else {
                newline
            };
            let line = std::str::from_utf8(&self.bytes[line_start..line_end]).map_err(|error| {
                ResponsesError::InvalidSseUtf8 {
                    detail: error.to_string(),
                }
            })?;
            if line.is_empty() {
                if self.data.is_empty() {
                    if self.finished && self.cursor == self.bytes.len() {
                        return Ok(None);
                    }
                    continue;
                }
                let event = if self.data.len() == 1 {
                    self.data.pop().expect("one SSE data line exists")
                } else {
                    let event = self.data.join("\n");
                    self.data.clear();
                    event
                };
                if event == "[DONE]" {
                    continue;
                }
                return Ok(Some(event));
            }
            if let Some(data) = line.strip_prefix("data:") {
                self.data
                    .push(data.strip_prefix(' ').unwrap_or(data).to_owned());
            }
        }
    }

    fn compact(&mut self) {
        if self.cursor == 0 {
            return;
        }
        let remaining = self.bytes.len() - self.cursor;
        self.bytes.copy_within(self.cursor.., 0);
        self.bytes.truncate(remaining);
        self.scan_start -= self.cursor;
        self.cursor = 0;
    }
}

#[cfg(not(target_family = "wasm"))]
fn retry_after(headers: &reqwest::header::HeaderMap) -> Option<RetryAfter> {
    headers
        .get(header::RETRY_AFTER)
        .and_then(|value| value.to_str().ok())
        .and_then(RetryAfter::from_header)
}

#[cfg(not(target_family = "wasm"))]
fn map_http_error(error: reqwest::Error) -> ResponsesError {
    ResponsesError::HttpRequest {
        retryable: error.is_connect() || error.is_body() || error.is_request(),
        timeout: error.is_timeout(),
        detail: error.to_string(),
    }
}

#[cfg(target_family = "wasm")]
impl ResponsesHttp {
    pub(crate) fn new(
        host: Option<std::sync::Arc<dyn crate::client::transport::host::HostTransport>>,
        gateway: Option<std::sync::Arc<super::ResponsesGateway>>,
    ) -> Self {
        Self { host, gateway }
    }

    pub(crate) async fn send(
        &self,
        api_base_url: &str,
        auth: &OpenAiAuthSnapshot,
        session_id: &str,
        thread_id: &str,
        turn_state: Option<&str>,
        request: &EncodedRequest,
    ) -> Result<(ResponsesHttpStream, HttpMetadata), ResponsesError> {
        use crate::client::transport::host::HostConnectRequest;
        let host = self
            .host
            .as_deref()
            .ok_or(ResponsesError::HostUnavailable)?;
        let endpoint = format!("{}/responses", api_base_url.trim_end_matches('/'));
        let turn_state =
            turn_state.filter(|value| header::HeaderValue::from_bytes(value.as_bytes()).is_ok());
        let response = host
            .http(
                HostConnectRequest::new_with_thread_id(
                    &endpoint,
                    auth.bearer(),
                    auth.account_id(),
                    auth.is_fedramp(),
                    session_id,
                    thread_id,
                    turn_state,
                )
                .with_headers(
                    self.gateway
                        .as_deref()
                        .map_or(&[][..], super::ResponsesGateway::headers),
                ),
                request.raw().get(),
            )
            .await
            .map_err(map_host_error)?;
        let metadata = HttpMetadata {
            reasoning_included: response.metadata.reasoning_included(),
            turn_state: response.metadata.turn_state().map(str::to_owned),
        };
        Ok((
            ResponsesHttpStream {
                response: response.body,
                decoder: SseDecoder::default(),
                ended: false,
            },
            metadata,
        ))
    }
}

#[cfg(target_family = "wasm")]
fn map_host_error(error: crate::client::transport::host::HostError) -> ResponsesError {
    use crate::client::transport::host::HostError;
    match error {
        HostError::HandshakeRejected {
            status,
            body,
            retry_after,
        } => ResponsesError::http_rejected(status, body, retry_after),
        HostError::Transport {
            detail,
            reconnectable,
        } => ResponsesError::HttpRequest {
            detail,
            retryable: reconnectable,
            timeout: false,
        },
    }
}
