pub mod host;

pub use host::{HttpBody, HttpError, HttpFuture, HttpRequest, HttpResponse, HttpTransport};

#[cfg(not(target_family = "wasm"))]
pub type HttpClient = reqwest::Client;
#[cfg(target_family = "wasm")]
pub use host::HttpClient;

pub(crate) const fn is_transient(error: &HttpError) -> bool {
    matches!(
        error,
        HttpError::Transport {
            retryable: true,
            ..
        } | HttpError::Transport { timeout: true, .. }
    )
}

#[cfg(not(target_family = "wasm"))]
const GZIP_LEVEL: u32 = 2;
#[cfg(not(target_family = "wasm"))]
const GZIP_PADDING_LENGTHS: usize = 257;

#[cfg(not(target_family = "wasm"))]
pub(crate) async fn post(
    client: &HttpClient,
    endpoint: &str,
    mut headers: http::HeaderMap,
    body: String,
    gzip: bool,
) -> Result<HttpResponse, HttpError> {
    use futures_util::TryStreamExt;

    headers
        .entry(http::header::ACCEPT_ENCODING)
        .or_insert(http::HeaderValue::from_static("gzip, br, deflate, zstd"));
    let span = tracing::Span::current();
    span.record("request.bytes", body.len());
    let body = if gzip {
        let padded = gzip_padded(body)?;
        let encoded = tokio::task::spawn_blocking(move || nanocode_http::gzip(&padded, GZIP_LEVEL))
            .await
            .map_err(|error| encode_error(&error))?
            .map_err(|error| encode_error(&error))?;
        span.record("request.gzip_bytes", encoded.len());
        headers.insert(
            http::header::CONTENT_ENCODING,
            http::HeaderValue::from_static("gzip"),
        );
        reqwest::Body::from(encoded)
    } else {
        reqwest::Body::from(body)
    };
    let mut response = client
        .post(endpoint)
        .header("anthropic-version", crate::ANTHROPIC_VERSION)
        .headers(headers)
        .header("content-type", "application/json")
        .body(body)
        .send()
        .await
        .map(nanocode_http::decode_response)
        .map_err(reqwest_error)?;
    let status = response.status();
    let headers = std::mem::take(response.headers_mut());
    Ok(HttpResponse::new(
        status,
        headers,
        Box::pin(
            response
                .bytes_stream()
                .map_ok(Vec::from)
                .map_err(reqwest_error),
        ),
    ))
}

#[cfg(not(target_family = "wasm"))]
fn gzip_padded(body: String) -> Result<Vec<u8>, HttpError> {
    let mut random = [0_u8; 2 + GZIP_PADDING_LENGTHS];
    nanocode_platform::fill_random(&mut random).map_err(|error| encode_error(&error))?;
    let length = usize::from(u16::from_le_bytes([random[0], random[1]])) % GZIP_PADDING_LENGTHS;
    let mut padded = body.into_bytes();
    padded.push(b' ');
    padded.extend(
        random[2..2 + length]
            .iter()
            .map(|byte| if byte & 1 == 0 { b' ' } else { b'\t' }),
    );
    Ok(padded)
}

#[cfg(not(target_family = "wasm"))]
fn encode_error(error: &dyn std::error::Error) -> HttpError {
    HttpError::Transport {
        retryable: false,
        timeout: false,
        detail: format!("gzip request body: {error}"),
    }
}

#[cfg(not(target_family = "wasm"))]
fn reqwest_error(error: reqwest::Error) -> HttpError {
    let mut detail = error.to_string();
    let mut source = std::error::Error::source(&error);
    while let Some(cause) = source {
        detail.push_str(": ");
        detail.push_str(&cause.to_string());
        source = cause.source();
    }
    HttpError::Transport {
        retryable: error.is_request() || error.is_body(),
        timeout: error.is_timeout(),
        detail,
    }
}

#[cfg(target_family = "wasm")]
pub(crate) async fn post(
    client: &HttpClient,
    endpoint: &str,
    headers: http::HeaderMap,
    body: String,
    _gzip: bool,
) -> Result<HttpResponse, HttpError> {
    post_with(client.0.as_ref(), endpoint, headers, body).await
}

pub(crate) async fn post_with(
    transport: &dyn HttpTransport,
    endpoint: &str,
    mut headers: http::HeaderMap,
    body: String,
) -> Result<HttpResponse, HttpError> {
    headers
        .entry("anthropic-version")
        .or_insert(http::HeaderValue::from_static(crate::ANTHROPIC_VERSION));
    headers.append(
        http::header::CONTENT_TYPE,
        http::HeaderValue::from_static("application/json"),
    );
    transport
        .post(HttpRequest {
            endpoint: endpoint.to_owned(),
            headers,
            body,
        })
        .await
}
