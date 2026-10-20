use serde_json::Value;

pub const MAX_WEB_OUTPUT_BYTES: usize = 32 * 1024;

const MAX_SAFE_INTEGER: u64 = (1 << 53) - 1;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WebSearchRequest {
    pub query: String,

    pub allowed_domains: Vec<String>,

    pub blocked_domains: Vec<String>,

    pub max_output_bytes: usize,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WebFetchRequest {
    pub url: String,

    pub prompt: String,

    pub offset: Option<u64>,

    pub max_output_bytes: usize,
}

pub trait ApprovedWebProvider: Send + Sync {
    fn search(
        &self,
        request: WebSearchRequest,
    ) -> impl std::future::Future<Output = Result<String, String>> + Send;

    fn fetch(
        &self,
        request: WebFetchRequest,
    ) -> impl std::future::Future<Output = Result<String, String>> + Send;
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ApprovedPage {
    pub final_url: String,

    pub content: String,
}

pub trait ApprovedWebFetchSource: Send + Sync {
    fn fetch_source(
        &self,
        request: WebFetchRequest,
    ) -> impl std::future::Future<Output = Result<ApprovedPage, String>> + Send;
}

pub struct ClaudeWeb<P: ApprovedWebProvider> {
    provider: P,
}

impl<P: ApprovedWebProvider> ClaudeWeb<P> {
    pub const fn new(provider: P) -> Self {
        Self { provider }
    }

    #[must_use]
    pub fn definitions() -> Vec<Value> {
        ["WebSearch", "WebFetch"]
            .into_iter()
            .map(crate::upstream::tool_value)
            .collect()
    }

    pub async fn execute(&self, name: &str, input: Value) -> Result<String, String> {
        if !matches!(name, "WebSearch" | "WebFetch") {
            return Err(format!("unknown Claude web tool: {name}"));
        }
        if serde_json::to_vec(&input).map_err(|e| e.to_string())?.len() > 16 * 1024 {
            return Err("web input exceeds 16 KiB".into());
        }
        let fields = input.as_object().ok_or("web input must be an object")?;
        let result = match name {
            "WebSearch" => {
                reject_extra(fields, &["query", "allowed_domains", "blocked_domains"])?;
                let query = required_text(fields, "query", 8 * 1024)?;
                if query.chars().count() < 2 {
                    return Err("query must contain at least two characters".into());
                }
                let allowed_domains = domains(fields.get("allowed_domains"))?;
                let blocked_domains = domains(fields.get("blocked_domains"))?;
                if !allowed_domains.is_empty() && !blocked_domains.is_empty() {
                    return Err("allowed_domains and blocked_domains are mutually exclusive".into());
                }
                self.provider
                    .search(WebSearchRequest {
                        query: query.to_owned(),
                        allowed_domains,
                        blocked_domains,
                        max_output_bytes: MAX_WEB_OUTPUT_BYTES,
                    })
                    .await
            }
            "WebFetch" => {
                reject_extra(fields, &["url", "prompt", "offset"])?;
                let url = required_text(fields, "url", 2048)?;
                validate_url(url)?;
                let prompt = required_text(fields, "prompt", 8 * 1024)?;
                let offset = fields
                    .get("offset")
                    .map(|value| {
                        value
                            .as_u64()
                            .filter(|offset| *offset <= MAX_SAFE_INTEGER)
                            .ok_or("offset must be a nonnegative integer")
                    })
                    .transpose()?;
                self.provider
                    .fetch(WebFetchRequest {
                        url: url.to_owned(),
                        prompt: prompt.to_owned(),
                        offset,
                        max_output_bytes: MAX_WEB_OUTPUT_BYTES,
                    })
                    .await
            }
            _ => unreachable!(),
        };
        result
            .map(|text| cap_utf8(&text, MAX_WEB_OUTPUT_BYTES).to_owned())
            .map_err(|error| format!("approved web provider: {}", cap_utf8(&error, 1024)))
    }
}

fn reject_extra(fields: &serde_json::Map<String, Value>, allowed: &[&str]) -> Result<(), String> {
    if let Some(key) = fields.keys().find(|key| !allowed.contains(&key.as_str())) {
        return Err(format!("unsupported web field: {key}"));
    }
    Ok(())
}

fn required_text<'a>(
    fields: &'a serde_json::Map<String, Value>,
    key: &str,
    max: usize,
) -> Result<&'a str, String> {
    let value = fields
        .get(key)
        .and_then(Value::as_str)
        .ok_or_else(|| format!("missing or invalid {key}"))?;
    if value.trim().is_empty()
        || value.len() > max
        || value
            .chars()
            .any(|ch| ch.is_control() && !(key == "prompt" && matches!(ch, '\n' | '\r' | '\t')))
    {
        return Err(format!(
            "{key} must be nonblank, at most {max} bytes, and contain no unsupported control characters"
        ));
    }
    Ok(value)
}

fn domains(value: Option<&Value>) -> Result<Vec<String>, String> {
    let Some(value) = value else {
        return Ok(vec![]);
    };
    let entries = value.as_array().ok_or("domain filter must be an array")?;
    if entries.len() > 16 {
        return Err("too many domain filters".into());
    }
    entries
        .iter()
        .map(|value| {
            let value = value.as_str().ok_or("invalid domain filter")?;
            let (host, path) = value.split_once('/').unwrap_or((value, ""));
            if value.len() > 256
                || !valid_host(host)
                || path
                    .bytes()
                    .any(|b| !b.is_ascii_graphic() || b == b'\\' || b == b'?' || b == b'#')
            {
                return Err("domain filters must be bare DNS domains with an optional path".into());
            }
            Ok(value.to_owned())
        })
        .collect()
}

fn validate_url(url: &str) -> Result<(), String> {
    let rest = url
        .strip_prefix("https://")
        .or_else(|| url.strip_prefix("http://"))
        .ok_or("web fetch URL must be HTTP(S)")?;
    if !url.is_ascii()
        || url
            .bytes()
            .any(|b| b.is_ascii_whitespace() || b.is_ascii_control() || b == b'\\')
    {
        return Err("invalid web fetch URL".into());
    }
    let authority = rest.split(['/', '?', '#']).next().unwrap_or_default();
    if authority.contains('@') {
        return Err("web fetch URL must not contain credentials".into());
    }
    let (host, port) = authority.split_once(':').unwrap_or((authority, ""));
    if !valid_host(host)
        || (!port.is_empty()
            && (port.parse::<u16>().ok().filter(|&n| n > 0).is_none()
                || !port.bytes().all(|b| b.is_ascii_digit())))
        || authority.ends_with(':')
    {
        return Err("invalid web fetch host".into());
    }
    Ok(())
}

fn valid_host(host: &str) -> bool {
    host.len() <= 253
        && host.contains('.')
        && host.split('.').all(|label| {
            !label.is_empty()
                && label.len() <= 63
                && label
                    .bytes()
                    .next()
                    .is_some_and(|b| b.is_ascii_alphanumeric())
                && label
                    .bytes()
                    .last()
                    .is_some_and(|b| b.is_ascii_alphanumeric())
                && label
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b == b'-')
        })
}

fn cap_utf8(value: &str, max: usize) -> &str {
    &value[..value.floor_char_boundary(max)]
}
