use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::collections::BTreeSet;

#[derive(Clone, Copy, Debug)]
pub struct HostContext<'a> {
    model: &'a str,
    session_id: &'a str,
    turn_id: Option<&'a str>,
    call_id: &'a str,
    max_output_tokens: usize,
}
impl<'a> HostContext<'a> {
    #[must_use]
    pub const fn new(
        model: &'a str,
        session_id: &'a str,
        call_id: &'a str,
        max_output_tokens: usize,
    ) -> Self {
        Self {
            model,
            session_id,
            turn_id: None,
            call_id,
            max_output_tokens,
        }
    }

    #[must_use]
    pub const fn with_turn_id(mut self, turn_id: Option<&'a str>) -> Self {
        self.turn_id = turn_id;
        self
    }

    #[must_use]
    pub const fn model(self) -> &'a str {
        self.model
    }

    #[must_use]
    pub const fn session_id(self) -> &'a str {
        self.session_id
    }

    #[must_use]
    pub const fn turn_id(self) -> Option<&'a str> {
        self.turn_id
    }

    #[must_use]
    pub const fn call_id(self) -> &'a str {
        self.call_id
    }

    #[must_use]
    pub const fn max_output_tokens(self) -> usize {
        self.max_output_tokens
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ImageSource {
    Base64 { media_type: String, data: String },

    Url { url: String },
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ToolResultBlock {
    Text { text: String },

    Image { source: ImageSource },

    Document { file_data: String },

    UnsupportedMedia { media_type: String },
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum ToolContent {
    Text(String),

    Blocks(Vec<ToolResultBlock>),
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ToolOutput {
    pub content: ToolContent,

    pub is_error: bool,

    pub structured_result: Option<Value>,

    pub metadata: Option<Value>,
}
impl ToolOutput {
    #[must_use]
    pub fn text(text: impl Into<String>) -> Self {
        let text = text.into();
        Self {
            structured_result: Some(Value::String(text.clone())),
            content: ToolContent::Text(text),
            is_error: false,
            metadata: None,
        }
    }

    #[must_use]
    pub fn error(text: impl Into<String>) -> Self {
        Self {
            is_error: true,
            ..Self::text(text)
        }
    }

    #[must_use]
    pub const fn content(blocks: Vec<ToolResultBlock>) -> Self {
        Self {
            content: ToolContent::Blocks(blocks),
            is_error: false,
            structured_result: None,
            metadata: None,
        }
    }

    #[must_use]
    pub fn with_structured_result(mut self, result: Value) -> Self {
        self.structured_result = Some(result);
        self
    }

    #[must_use]
    pub fn with_metadata(mut self, metadata: Value) -> Self {
        self.metadata = Some(metadata);
        self
    }
}

pub const MAX_HOST_INPUT_BYTES: usize = 64 * 1024;

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum HostTool {
    Agent,

    TaskOutput,

    TaskStop,

    AskUserQuestion,

    EnterPlanMode,

    ExitPlanMode,

    EnterWorktree,

    ExitWorktree,
}
impl HostTool {
    pub const ALL: [Self; 8] = [
        Self::Agent,
        Self::TaskOutput,
        Self::TaskStop,
        Self::AskUserQuestion,
        Self::EnterPlanMode,
        Self::ExitPlanMode,
        Self::EnterWorktree,
        Self::ExitWorktree,
    ];

    #[must_use]
    pub const fn name(self) -> &'static str {
        match self {
            Self::Agent => "Agent",
            Self::TaskOutput => "TaskOutput",
            Self::TaskStop => "TaskStop",
            Self::AskUserQuestion => "AskUserQuestion",
            Self::EnterPlanMode => "EnterPlanMode",
            Self::ExitPlanMode => "ExitPlanMode",
            Self::EnterWorktree => "EnterWorktree",
            Self::ExitWorktree => "ExitWorktree",
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum HostRequest {
    Agent(AgentRequest),

    TaskOutput(TaskOutputRequest),

    TaskStop(TaskStopRequest),

    AskUserQuestion(QuestionsRequest),

    EnterPlanMode,

    ExitPlanMode(ExitPlanRequest),

    EnterWorktree(EnterWorktreeRequest),

    ExitWorktree(ExitWorktreeRequest),
}

pub trait ClaudeHost: Send + Sync {
    fn execute(
        &self,
        request: HostRequest,
        context: HostContext<'_>,
    ) -> impl std::future::Future<Output = Result<ToolOutput, String>> + Send;
}

pub struct ClaudeHostTools<H: ClaudeHost> {
    host: H,
    enabled: BTreeSet<HostTool>,
}
impl<H: ClaudeHost> ClaudeHostTools<H> {
    pub fn new(host: H, enabled: impl IntoIterator<Item = HostTool>) -> Self {
        Self {
            host,
            enabled: enabled.into_iter().collect(),
        }
    }

    #[must_use]
    pub fn definitions(&self) -> Vec<Value> {
        self.enabled.iter().map(|tool| definition(*tool)).collect()
    }

    pub async fn execute(
        &self,
        name: &str,
        input: Value,
        context: HostContext<'_>,
    ) -> Result<ToolOutput, String> {
        let tool = self
            .enabled
            .iter()
            .find(|tool| tool.name() == name)
            .copied()
            .ok_or_else(|| format!("Claude host tool is unavailable: {name}"))?;
        if context.session_id().is_empty()
            || context.call_id().is_empty()
            || context.turn_id().is_none_or(str::is_empty)
        {
            return Err("host tools require real session, turn and call identities".into());
        }
        let request = match tool {
            HostTool::Agent => {
                let request: AgentRequest = decode(input)?;
                if request.max_turns.is_some_and(|turns| turns == 0) {
                    return Err("max_turns must be positive".into());
                }
                if matches!(request.isolation, Some(AgentIsolation::Remote))
                    && !request.run_in_background
                {
                    return Err("remote Agent isolation requires background execution".into());
                }
                if request.run_in_background && !self.enabled.contains(&HostTool::TaskStop) {
                    return Err("background Agent requires an installed TaskStop capability".into());
                }
                HostRequest::Agent(request)
            }
            HostTool::TaskOutput => {
                let request: TaskOutputRequest = decode(input)?;
                HostRequest::TaskOutput(request)
            }
            HostTool::TaskStop => {
                let request: TaskStopRequest = decode(input)?;
                HostRequest::TaskStop(request)
            }
            HostTool::AskUserQuestion => {
                let request: QuestionsRequest = decode(input)?;
                if !(1..=4).contains(&request.questions.len()) {
                    return Err("questions requires 1..4 entries".into());
                }
                for question in &request.questions {
                    if question.header.chars().count() > 12 {
                        return Err("header must contain at most 12 characters".into());
                    }
                    if !(2..=4).contains(&question.options.len()) {
                        return Err("question requires 2..4 options".into());
                    }
                    let mut labels = BTreeSet::new();
                    for option in &question.options {
                        if !labels.insert(&option.label) {
                            return Err("duplicate option label".into());
                        }
                    }
                }
                HostRequest::AskUserQuestion(request)
            }
            HostTool::EnterPlanMode => {
                let _: EmptyRequest = decode(input)?;
                HostRequest::EnterPlanMode
            }
            HostTool::ExitPlanMode => {
                let request: ExitPlanRequest = decode(input)?;
                HostRequest::ExitPlanMode(request)
            }
            HostTool::EnterWorktree => {
                let request: EnterWorktreeRequest = decode(input)?;
                if request.name.is_some() && request.path.is_some() {
                    return Err("name and path are mutually exclusive".into());
                }
                HostRequest::EnterWorktree(request)
            }
            HostTool::ExitWorktree => HostRequest::ExitWorktree(decode(input)?),
        };
        self.host.execute(request, context).await
    }
}
fn decode<T: serde::de::DeserializeOwned>(input: Value) -> Result<T, String> {
    serde_json::from_value(input).map_err(|e| format!("invalid Claude host input: {e}"))
}
fn schema<T: JsonSchema>() -> Value {
    serde_json::to_value(schemars::schema_for!(T)).expect("JSON schema serialization")
}
fn definition(tool: HostTool) -> Value {
    match tool {
        HostTool::TaskOutput => json!({
            "name": tool.name(),
            "description": "SDK extension: read a host-owned background task's status and output, optionally waiting up to timeout milliseconds.",
            "input_schema": schema::<TaskOutputRequest>()
        }),
        HostTool::Agent => {
            let mut definition = crate::upstream::tool_value(tool.name());
            definition["input_schema"]["properties"]["model"]["enum"] =
                schema::<AgentModel>()["enum"].clone();
            definition
        }
        _ => crate::upstream::tool_value(tool.name()),
    }
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct AgentRequest {
    pub prompt: String,

    pub description: String,

    #[serde(default = "default_subagent_type")]
    pub subagent_type: String,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model: Option<AgentModel>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resume: Option<String>,

    #[serde(default = "default_block")]
    pub run_in_background: bool,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_turns: Option<u32>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub isolation: Option<AgentIsolation>,
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum AgentModel {
    Sonnet,

    Opus,

    Haiku,

    Fable,

    Mythos,
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum AgentIsolation {
    Worktree,

    Remote,
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct TaskOutputRequest {
    pub task_id: String,

    #[serde(default = "default_block")]
    pub block: bool,

    #[serde(default = "default_timeout")]
    pub timeout: u64,
}
fn default_subagent_type() -> String {
    "general-purpose".to_owned()
}
const fn default_block() -> bool {
    true
}
const fn default_timeout() -> u64 {
    30_000
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct TaskStopRequest {
    #[serde(alias = "shell_id")]
    pub task_id: String,
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct QuestionsRequest {
    pub questions: Vec<UserQuestion>,
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct UserQuestion {
    pub question: String,

    pub header: String,

    pub options: Vec<QuestionOption>,

    #[serde(rename = "multiSelect")]
    pub multi_select: bool,
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct QuestionOption {
    pub label: String,

    pub description: String,

    #[serde(
        default,
        rename = "preview",
        alias = "markdown",
        skip_serializing_if = "Option::is_none"
    )]
    pub markdown: Option<String>,
}
#[derive(Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct EmptyRequest {}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ExitPlanRequest {
    #[serde(default, rename = "allowedPrompts")]
    pub allowed_prompts: Vec<PlanPrompt>,
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct PlanPrompt {
    pub tool: PlanTool,

    pub prompt: String,
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
pub enum PlanTool {
    Bash,
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct EnterWorktreeRequest {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub path: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ExitWorktreeRequest {
    pub action: WorktreeAction,
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum WorktreeAction {
    Keep,

    Remove,
}

pub fn mcp_tool_output(result: Value) -> Result<ToolOutput, String> {
    let content = result
        .get("content")
        .and_then(Value::as_array)
        .ok_or("MCP result omitted its content array")?;
    let mut blocks = Vec::new();
    for item in content {
        match item.get("type").and_then(Value::as_str) {
            Some("text") => blocks.push(ToolResultBlock::Text {
                text: item
                    .get("text")
                    .and_then(Value::as_str)
                    .ok_or("MCP text content omitted text")?
                    .to_owned(),
            }),
            Some("image") => {
                let media_type = item
                    .get("mimeType")
                    .and_then(Value::as_str)
                    .ok_or("MCP image omitted mimeType")?;
                if !matches!(
                    media_type,
                    "image/png" | "image/jpeg" | "image/gif" | "image/webp"
                ) {
                    return Err(format!(
                        "MCP image type is unsupported by Claude: {media_type}"
                    ));
                }
                blocks.push(ToolResultBlock::Image {
                    source: ImageSource::Base64 {
                        media_type: media_type.to_owned(),
                        data: item
                            .get("data")
                            .and_then(Value::as_str)
                            .ok_or("MCP image omitted data")?
                            .to_owned(),
                    },
                });
            }
            Some("resource") => {
                let resource = item
                    .get("resource")
                    .ok_or("MCP resource omitted resource body")?;

                if let Some(blob) = resource.get("blob") {
                    let data = blob.as_str().ok_or("MCP resource blob must be a string")?;
                    let media_type = resource
                        .get("mimeType")
                        .and_then(Value::as_str)
                        .ok_or("MCP binary resource omitted mimeType")?;
                    match media_type {
                        "application/pdf" | "text/plain" => {
                            blocks.push(ToolResultBlock::Document {
                                file_data: format!("data:{media_type};base64,{data}"),
                            })
                        }
                        "image/png" | "image/jpeg" | "image/gif" | "image/webp" => {
                            blocks.push(ToolResultBlock::Image {
                                source: ImageSource::Base64 {
                                    media_type: media_type.to_owned(),
                                    data: data.to_owned(),
                                },
                            })
                        }
                        _ => {
                            return Err(format!(
                                "MCP binary resource type is unsupported by Claude: {media_type}"
                            ));
                        }
                    }
                } else if resource.get("text").and_then(Value::as_str).is_some() {
                    blocks.push(ToolResultBlock::Text {
                        text: serde_json::to_string(resource).map_err(|e| e.to_string())?,
                    });
                } else {
                    return Err("MCP resource omitted text or blob".into());
                }
            }
            Some("resource_link") => blocks.push(ToolResultBlock::Text {
                text: serde_json::to_string(item).map_err(|e| e.to_string())?,
            }),
            kind => {
                return Err(format!(
                    "MCP content cannot be represented in Claude Messages: {}",
                    kind.unwrap_or("missing type")
                ));
            }
        }
    }
    let mut output = ToolOutput::content(blocks);
    output.is_error = result
        .get("isError")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    output.metadata = result.get("_meta").cloned();
    output.structured_result = Some(result);
    Ok(output)
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct McpToolDefinition {
    pub name: String,

    pub description: String,

    pub input_schema: Value,
}
impl McpToolDefinition {
    #[must_use]
    pub fn new(
        name: impl Into<String>,
        description: impl Into<String>,
        input_schema: Value,
    ) -> Self {
        Self {
            name: name.into(),
            description: description.into(),
            input_schema,
        }
    }
}

pub trait ClaudeMcpProvider: Send + Sync {
    fn start(&self);

    fn available_definitions(&self) -> Vec<McpToolDefinition>;

    fn execute(
        &self,
        name: &str,
        input: Value,
        context: HostContext<'_>,
    ) -> impl std::future::Future<Output = Result<Option<ToolOutput>, String>> + Send;
}

pub struct ClaudeMcp<P: ClaudeMcpProvider + ?Sized> {
    provider: std::sync::Arc<P>,
}
impl<P: ClaudeMcpProvider + ?Sized> ClaudeMcp<P> {
    #[must_use]
    pub const fn new(provider: std::sync::Arc<P>) -> Self {
        Self { provider }
    }

    pub fn start(&self) {
        self.provider.start();
    }

    pub fn definitions(&self) -> Result<Vec<Value>, String> {
        let mut names = BTreeSet::new();
        self.provider.available_definitions().into_iter().map(|definition| {
            let name = &definition.name;
            if !name.starts_with("mcp__") || !names.insert(name.clone()) {
                return Err(format!("invalid or duplicate MCP tool name: {name}"));
            }
            if !definition.input_schema.is_object() { return Err("MCP input schema must be an object".into()); }
            Ok(json!({"name":name,"description":definition.description,"input_schema":definition.input_schema}))
        }).collect()
    }

    pub async fn execute(
        &self,
        name: &str,
        input: Value,
        context: HostContext<'_>,
    ) -> Result<ToolOutput, String> {
        if !self
            .definitions()?
            .iter()
            .any(|definition| definition["name"].as_str() == Some(name))
        {
            return Err(format!("MCP tool is no longer available: {name}"));
        }
        if !input.is_object() {
            return Err("MCP input must be an object".into());
        }
        self.provider
            .execute(name, input, context)
            .await?
            .ok_or_else(|| format!("MCP tool became unavailable: {name}"))
    }
}
