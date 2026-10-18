use super::{RetryAfter, RetryReceipt};
use std::collections::HashMap;

use serde::Deserialize;

pub(super) fn retryable_api_error(event: &str) -> Option<(&'static str, Option<RetryAfter>)> {
    retryable_api_error_received(event, RetryReceipt::now())
}

pub(super) fn retryable_api_error_received(
    event: &str,
    received: RetryReceipt,
) -> Option<(&'static str, Option<RetryAfter>)> {
    let event: ApiErrorEnvelope = serde_json::from_str(event).ok()?;
    let error = event.error();
    let code = event.code();
    let discriminator = code.or_else(|| error.and_then(|error| error.kind.as_deref()));

    if event.is_terminal() {
        return None;
    }
    let class = match event.event_type.as_deref() {
        Some("response.incomplete") => "api_incomplete",
        Some("response.failed") => {
            if code.is_some_and(is_terminal_response_failure) {
                return None;
            }
            match discriminator {
                Some("server_is_overloaded" | "slow_down") => "api_overload",
                Some("rate_limit_exceeded") => "api_rate_limit",
                Some("server_error" | "websocket_connection_limit_reached") => "api_server",
                _ => "api_failed",
            }
        }
        _ => match discriminator {
            Some("server_is_overloaded" | "slow_down") => "api_overload",
            Some("server_error" | "websocket_connection_limit_reached") => "api_server",
            Some("rate_limit_exceeded") => "api_rate_limit",
            _ => return None,
        },
    };

    let server_delay = error
        .and_then(|error| error.retry_after)
        .and_then(|seconds| std::time::Duration::try_from_secs_f64(seconds).ok())
        .and_then(|delay| RetryAfter::from_delay_received(delay, received))
        .or_else(|| retry_after_header(&event.headers, received));
    Some((class, server_delay))
}

pub(super) fn api_error_has_code(event: &str, expected: &str) -> bool {
    let Ok(event) = serde_json::from_str::<ApiErrorEnvelope>(event) else {
        return false;
    };
    event.code() == Some(expected)
}

pub(super) fn api_error_is_connection_expired(event: &str) -> bool {
    let Ok(event) = serde_json::from_str::<ApiErrorEnvelope>(event) else {
        return false;
    };
    event.event_type.as_deref() == Some("error")
        && !event.is_terminal()
        && event.code() == Some("websocket_connection_limit_reached")
}

pub(super) fn invalid_tool_schema_path(event: &str) -> Option<Vec<usize>> {
    let event: ApiErrorEnvelope = serde_json::from_str(event).ok()?;
    let (code, param) = if event.code.is_some() {
        (event.code.as_deref(), event.param.as_deref())
    } else {
        let error = event.error()?;
        (error.code.as_deref(), error.param.as_deref())
    };
    if code != Some("invalid_function_parameters") {
        return None;
    }
    let (input, mut rest) = path_index(param?.strip_prefix("input[")?)?;
    let mut indices = vec![input];
    while let Some(suffix) = rest.strip_prefix(".tools[") {
        let (index, suffix) = path_index(suffix)?;
        indices.push(index);
        rest = suffix;
    }
    let suffix = rest.strip_prefix(".parameters")?;
    (indices.len() >= 2
        && (suffix.is_empty() || suffix.starts_with('.') || suffix.starts_with('[')))
    .then_some(indices)
}

fn path_index(path: &str) -> Option<(usize, &str)> {
    let (index, rest) = path.split_once(']')?;
    if index.is_empty() || !index.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    Some((index.parse().ok()?, rest))
}

pub(super) fn api_error_is_checkpoint_missing(event: &str) -> bool {
    let Ok(event) = serde_json::from_str::<ApiErrorEnvelope>(event) else {
        return false;
    };
    let Some(error) = event.error() else {
        return false;
    };
    if error.code.as_deref() == Some("previous_response_not_found") {
        return true;
    }
    error.kind.as_deref() == Some("invalid_request_error")
        && error
            .message
            .as_deref()
            .is_some_and(|message| message.eq_ignore_ascii_case("Invalid `previous_response_id`."))
}

pub(super) fn is_terminal_response_failure(code: &str) -> bool {
    matches!(
        code,
        "context_length_exceeded"
            | "insufficient_quota"
            | "usage_not_included"
            | "cyber_policy"
            | "misalignment_policy_violation"
            | "invalid_prompt"
            | "bio_policy"
    )
}

pub(super) fn api_error_is_terminal(event: &str) -> bool {
    serde_json::from_str::<ApiErrorEnvelope>(event)
        .ok()
        .is_some_and(|event| event.is_terminal())
}

fn retry_after_header(
    headers: &HashMap<String, RetryAfterValue>,
    received: RetryReceipt,
) -> Option<RetryAfter> {
    headers
        .iter()
        .find(|(name, _)| name.eq_ignore_ascii_case("retry-after"))
        .and_then(|(_, value)| match value {
            RetryAfterValue::Number(seconds) => std::time::Duration::try_from_secs_f64(*seconds)
                .ok()
                .and_then(|delay| RetryAfter::from_delay_received(delay, received)),
            RetryAfterValue::String(value) => RetryAfter::from_header_received(value, received),
        })
}

#[derive(Deserialize)]
struct ApiErrorEnvelope {
    #[serde(default, rename = "type")]
    event_type: Option<Box<str>>,
    #[serde(default)]
    code: Option<Box<str>>,
    #[serde(default)]
    param: Option<Box<str>>,
    #[serde(default)]
    error: Option<ApiErrorDetail>,
    #[serde(default)]
    response: Option<ApiErrorResponse>,
    #[serde(default)]
    headers: HashMap<String, RetryAfterValue>,
}

impl ApiErrorEnvelope {
    fn is_terminal(&self) -> bool {
        [self.code.as_deref(), self.event_type.as_deref()]
            .into_iter()
            .flatten()
            .any(is_terminal_response_failure)
            || [
                self.error.as_ref(),
                self.response
                    .as_ref()
                    .and_then(|response| response.error.as_ref()),
            ]
            .into_iter()
            .flatten()
            .any(|error| {
                [error.code.as_deref(), error.kind.as_deref()]
                    .into_iter()
                    .flatten()
                    .any(is_terminal_response_failure)
            })
    }

    fn error(&self) -> Option<&ApiErrorDetail> {
        self.error
            .as_ref()
            .or_else(|| self.response.as_ref()?.error.as_ref())
    }

    fn code(&self) -> Option<&str> {
        self.code
            .as_deref()
            .or_else(|| self.error().and_then(|error| error.code.as_deref()))
    }
}

#[derive(Deserialize)]
struct ApiErrorResponse {
    #[serde(default)]
    error: Option<ApiErrorDetail>,
}

#[derive(Deserialize)]
struct ApiErrorDetail {
    #[serde(default, rename = "type")]
    kind: Option<Box<str>>,
    #[serde(default)]
    code: Option<Box<str>>,
    #[serde(default)]
    param: Option<Box<str>>,
    #[serde(default)]
    message: Option<Box<str>>,
    #[serde(default)]
    retry_after: Option<f64>,
}

#[derive(Deserialize)]
#[serde(untagged)]
enum RetryAfterValue {
    Number(f64),
    String(Box<str>),
}
