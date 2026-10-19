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
pub(crate) async fn post(
    client: &HttpClient,
    endpoint: &str,
    mut headers: http::HeaderMap,
    body: String,
) -> Result<HttpResponse, HttpError> {
    use futures_util::TryStreamExt;

    headers
        .entry(http::header::ACCEPT_ENCODING)
        .or_insert(http::HeaderValue::from_static("gzip, br, deflate, zstd"));
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
fn reqwest_error(error: reqwest::Error) -> HttpError {
    HttpError::Transport {
        retryable: error.is_request() || error.is_body(),
        timeout: error.is_timeout(),
        detail: error.to_string(),
    }
}

#[cfg(target_family = "wasm")]
pub(crate) async fn post(
    client: &HttpClient,
    endpoint: &str,
    headers: http::HeaderMap,
    body: String,
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
