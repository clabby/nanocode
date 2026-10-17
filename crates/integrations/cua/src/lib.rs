#![cfg(not(target_family = "wasm"))]

use async_trait::async_trait;
use base64::{Engine as _, engine::general_purpose::STANDARD};
use nanocode_tool_contracts::{
    Tool, ToolContext, ToolDefinition, ToolError, ToolInput, ToolOutput, ToolOutputContent,
    ToolResult,
};
use nanocode_types::ImageDetail;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeMap, ffi::OsString, path::PathBuf, process::Stdio, sync::Arc, time::Duration,
};
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    process::{Child, ChildStdin, ChildStdout, Command},
    sync::{mpsc, oneshot},
};

const PROVIDER_STARTUP_TIMEOUT: Duration = Duration::from_secs(120);
const PROVIDER_ENVIRONMENT: &[&str] = &[
    "PATH",
    "HOME",
    "USER",
    "LOGNAME",
    "TMPDIR",
    "TEMP",
    "SystemRoot",
    "LOCALAPPDATA",
    "DISPLAY",
    "XAUTHORITY",
    "WAYLAND_DISPLAY",
    "HYPRLAND_INSTANCE_SIGNATURE",
    "XDG_RUNTIME_DIR",
    "DBUS_SESSION_BUS_ADDRESS",
    "LANG",
    "SKY_ENABLE_AUDIO",
];

#[derive(Clone, Debug)]
pub struct ComputerConfig {
    pub executable: PathBuf,
    pub args: Vec<OsString>,
    pub environment: BTreeMap<OsString, OsString>,
    provider_catalog: Option<Vec<ProviderTool>>,
    catalog_cache: Option<CatalogCache>,
}

#[derive(Clone, Debug)]
struct CatalogCache {
    path: PathBuf,
    inputs: Vec<PathBuf>,
}

#[derive(Deserialize, Serialize)]
struct CachedCatalog {
    fingerprint: Value,
    catalog: Vec<ProviderTool>,
}

impl CatalogCache {
    fn entry_path(&self, fingerprint: &Value) -> PathBuf {
        self.path.with_extension("").join(format!(
            "{}.json",
            base64::engine::general_purpose::URL_SAFE_NO_PAD
                .encode(Sha256::digest(fingerprint.to_string().as_bytes()))
        ))
    }

    fn fingerprint(&self, config: &ComputerConfig) -> Value {
        let lossy = |value: &std::ffi::OsStr| value.to_string_lossy().into_owned();
        let inputs = self
            .inputs
            .iter()
            .map(|path| {
                let stamp = std::fs::metadata(path).ok().map(|metadata| {
                    let modified = metadata
                        .modified()
                        .ok()
                        .and_then(|time| time.duration_since(std::time::UNIX_EPOCH).ok())
                        .map(|elapsed| elapsed.as_nanos().to_string());
                    json!({"len": metadata.len(), "modified": modified})
                });
                json!({"path": lossy(path.as_os_str()), "stamp": stamp})
            })
            .collect::<Vec<_>>();
        let inherited = PROVIDER_ENVIRONMENT
            .iter()
            .map(|name| json!([name, std::env::var_os(name).as_deref().map(lossy)]))
            .collect::<Vec<_>>();
        json!({
            "version": env!("CARGO_PKG_VERSION"),
            "executable": lossy(config.executable.as_os_str()),
            "args": config.args.iter().map(|arg| lossy(arg)).collect::<Vec<_>>(),
            "environment": config
                .environment
                .iter()
                .map(|(name, value)| [lossy(name), lossy(value)])
                .collect::<Vec<_>>(),
            "inherited": inherited,
            "inputs": inputs,
        })
    }

    fn load(&self, fingerprint: &Value) -> Option<Vec<ProviderTool>> {
        let entry = self.entry_path(fingerprint);
        for path in [&entry, &self.path] {
            let Some(cached) = std::fs::read(path)
                .ok()
                .and_then(|bytes| serde_json::from_slice::<CachedCatalog>(&bytes).ok())
            else {
                continue;
            };
            if cached.fingerprint == *fingerprint && validate_catalog(&cached.catalog).is_ok() {
                if path == &self.path {
                    drop(self.store(fingerprint.clone(), &cached.catalog));
                }
                return Some(cached.catalog);
            }
        }
        None
    }

    fn store(&self, fingerprint: Value, catalog: &[ProviderTool]) -> std::io::Result<()> {
        let path = self.entry_path(&fingerprint);
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let staged = path.with_extension(format!("json.{}.tmp", std::process::id()));
        std::fs::write(
            &staged,
            serde_json::to_vec(&json!({"fingerprint": fingerprint, "catalog": catalog}))?,
        )?;
        std::fs::rename(&staged, &path)
    }

    fn invalidate(&self, fingerprint: &Value) {
        drop(std::fs::remove_file(self.entry_path(fingerprint)));
        if std::fs::read(&self.path)
            .ok()
            .and_then(|bytes| serde_json::from_slice::<CachedCatalog>(&bytes).ok())
            .is_some_and(|cached| cached.fingerprint == *fingerprint)
        {
            drop(std::fs::remove_file(&self.path));
        }
    }
}

impl ComputerConfig {
    pub fn installed() -> Result<Self, ToolError> {
        if !cfg!(target_os = "macos") {
            return Err(
                "Default Sky CUA requires macOS; supply an explicit ComputerTools connection"
                    .into(),
            );
        }
        let home = std::env::var_os("HOME").ok_or("Default Sky CUA requires HOME")?;
        let codex_home = std::env::var_os("CODEX_HOME")
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from(home).join(".codex"));
        let base = PathBuf::from("/Applications/ChatGPT.app/Contents/Resources/cua_node");
        let mut config = Self::new(base.join("bin/node_repl"));
        let service = codex_home.join("computer-use/Codex Computer Use.app");
        if !config.executable.is_file() || !service.is_dir() {
            return Err("Default Sky CUA installation missing: expected ChatGPT.app/Contents/Resources/cua_node/bin/node_repl and CODEX_HOME/computer-use/Codex Computer Use.app".into());
        }
        config.environment = [
            (
                "NODE_REPL_NODE_PATH",
                base.join("bin/node").into_os_string(),
            ),
            (
                "NODE_REPL_NODE_MODULE_DIRS",
                base.join("lib/node_modules").into_os_string(),
            ),
            (
                "NODE_REPL_TRUSTED_CODE_PATHS",
                base.join("lib/node_modules").into_os_string(),
            ),
            (
                "NODE_REPL_TRUSTED_SERVICES",
                OsString::from(r#"{"sky":"@oai/sky/service"}"#),
            ),
            ("SKY_CUA_SERVICE_PATH", service.clone().into_os_string()),
            ("CODEX_HOME", codex_home.clone().into_os_string()),
        ]
        .into_iter()
        .map(|(key, value)| (OsString::from(key), value))
        .collect();
        config.catalog_cache = Some(CatalogCache {
            path: codex_home.join("nanocode").join("cua-catalog.json"),
            inputs: vec![
                config.executable.clone(),
                base.join("bin/node"),
                base.join("lib/node_modules/.package-map.json"),
                service.join("Contents/Info.plist"),
                codex_home.join("computer-use/config.json"),
            ],
        });
        Ok(config)
    }

    pub fn new(executable: impl Into<PathBuf>) -> Self {
        Self {
            executable: executable.into(),
            args: Vec::new(),
            environment: BTreeMap::new(),
            provider_catalog: None,
            catalog_cache: None,
        }
    }

    pub fn mcp(executable: impl Into<PathBuf>) -> Self {
        Self::new(executable)
    }
}

#[async_trait]
pub trait ComputerExecutor: Send + Sync + 'static {
    async fn end_turn(
        &self,
        _session_id: &str,
        _turn_id: &str,
        _hook_event_name: &str,
    ) -> Result<(), ToolError> {
        Ok(())
    }

    async fn invoke_tool(
        &self,
        name: &str,
        arguments: Value,
        context: ToolContext<'_>,
    ) -> ToolResult;
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct ProviderTool {
    pub name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,

    #[serde(flatten)]
    pub metadata: BTreeMap<String, Value>,
    #[serde(rename = "inputSchema")]
    pub input_schema: Value,
}

impl ProviderTool {
    pub fn model_visible(&self) -> bool {
        self.metadata
            .get("_meta")
            .and_then(|meta| meta.pointer("/ui/visibility"))
            .and_then(Value::as_array)
            .is_none_or(|visibility| visibility.iter().any(|value| value == "model"))
    }
}

#[derive(Clone)]
pub struct ComputerTools {
    executor: Arc<dyn ComputerExecutor>,
    catalog: Arc<Vec<ProviderTool>>,
}
impl ComputerTools {
    pub async fn connect(mut config: ComputerConfig) -> Result<Self, ToolError> {
        let cache = config
            .catalog_cache
            .clone()
            .map(|cache| (cache.fingerprint(&config), cache));
        if let Some((fingerprint, cache)) = &cache
            && let Some(catalog) = cache.load(fingerprint)
        {
            config.provider_catalog = Some(catalog);
            return Ok(Self::local(config));
        }
        let process = Process::start(&config).await?;
        if let Some((fingerprint, cache)) = cache {
            drop(cache.store(fingerprint, &process.catalog));
        }
        config.provider_catalog = Some(process.catalog.clone());
        Ok(Self::local(config))
    }
    fn local(config: ComputerConfig) -> Self {
        let catalog = Arc::new(
            config
                .provider_catalog
                .clone()
                .expect("connect discovers the upstream catalog before registration"),
        );
        let (dispatch, requests) = mpsc::unbounded_channel();
        tokio::spawn(route_sessions(config, requests));
        Self {
            executor: Arc::new(LocalComputer { dispatch }),
            catalog,
        }
    }

    pub fn new(executor: impl ComputerExecutor, catalog: Vec<ProviderTool>) -> Self {
        Self {
            executor: Arc::new(executor),
            catalog: Arc::new(catalog),
        }
    }

    pub fn catalog(&self) -> &[ProviderTool] {
        &self.catalog
    }

    pub fn tools(&self) -> impl Iterator<Item = ComputerTool> + '_ {
        self.catalog
            .iter()
            .filter(|definition| definition.model_visible())
            .cloned()
            .map(|definition| ComputerTool {
                executor: self.executor.clone(),
                definition,
            })
    }
    pub fn tool(&self, name: &str) -> Option<ComputerTool> {
        self.catalog
            .iter()
            .find(|definition| definition.name == name)
            .cloned()
            .map(|definition| ComputerTool {
                executor: self.executor.clone(),
                definition,
            })
    }

    pub async fn end_turn(
        &self,
        session_id: &str,
        turn_id: &str,
        hook_event_name: &str,
    ) -> Result<(), ToolError> {
        self.executor
            .end_turn(session_id, turn_id, hook_event_name)
            .await
    }
    pub fn js(&self) -> ComputerTool {
        self.tool("js").expect("CUA provider does not publish js")
    }
    pub fn reset(&self) -> ComputerTool {
        self.tool("js_reset")
            .expect("CUA provider does not publish js_reset")
    }
}

#[derive(Clone)]
pub struct ComputerTool {
    executor: Arc<dyn ComputerExecutor>,
    definition: ProviderTool,
}

impl ComputerTool {
    pub fn provider_definition(&self) -> &ProviderTool {
        &self.definition
    }
}

#[async_trait]
impl Tool for ComputerTool {
    async fn end_turn(
        &self,
        session_id: &str,
        turn_id: &str,
        hook_event_name: &str,
    ) -> Result<(), ToolError> {
        self.executor
            .end_turn(session_id, turn_id, hook_event_name)
            .await
    }

    fn supports_parallel_tool_calls(&self) -> bool {
        true
    }
    fn definition(&self) -> ToolDefinition {
        let definition = ToolDefinition::function(
            format!("mcp__cua_repl__{}", self.definition.name),
            self.definition.description.as_deref().unwrap_or(""),
            self.definition.input_schema.clone(),
        );
        match self.definition.metadata.get("outputSchema") {
            Some(schema) => definition.with_output_schema(schema.clone()),
            None => definition,
        }
    }
    async fn execute(&self, input: ToolInput, context: ToolContext<'_>) -> ToolResult {
        self.executor
            .invoke_tool(
                &self.definition.name,
                input.decode_json::<Value>()?,
                context,
            )
            .await
    }
}

struct LocalComputer {
    dispatch: mpsc::UnboundedSender<SessionRequest>,
}

struct SessionRequest {
    lifecycle: bool,
    session: String,
    turn_id: Option<String>,
    call_id: String,
    model: String,
    name: String,
    arguments: Value,
    response: oneshot::Sender<ToolResult>,
}

#[async_trait]
impl ComputerExecutor for LocalComputer {
    async fn end_turn(
        &self,
        session_id: &str,
        turn_id: &str,
        hook_event_name: &str,
    ) -> Result<(), ToolError> {
        if !matches!(hook_event_name, "Stop" | "Interrupt" | "SubagentStop") {
            return Err("Invalid CUA turn lifecycle event".into());
        }
        let (response, result) = oneshot::channel();
        self.dispatch
            .send(SessionRequest {
                lifecycle: true,
                session: session_id.into(),
                turn_id: Some(turn_id.into()),
                call_id: String::new(),
                model: String::new(),
                name: "turn_ended".into(),
                arguments: json!({"hook_event_name":hook_event_name,"session_id":session_id,"turn_id":turn_id}),
                response,
            })
            .map_err(|_| "CUA attachment is closed")?;
        let output = result.await.map_err(|_| "CUA attachment is closed")??;
        if !output.success {
            return Err("CUA turn_ended failed; cleanup was not retried".into());
        }
        Ok(())
    }

    async fn invoke_tool(
        &self,
        name: &str,
        arguments: Value,
        context: ToolContext<'_>,
    ) -> ToolResult {
        let session = context.session_id().to_owned();
        let (response, result) = oneshot::channel();
        self.dispatch
            .send(SessionRequest {
                lifecycle: false,
                session,
                turn_id: context.turn_id().map(str::to_owned),
                call_id: context.call_id().to_owned(),
                model: context.model().to_owned(),
                name: name.into(),
                arguments,
                response,
            })
            .map_err(|_| "CUA attachment is closed")?;
        result.await.map_err(|_| "CUA attachment is closed")?
    }
}

async fn route_sessions(
    config: ComputerConfig,
    mut requests: mpsc::UnboundedReceiver<SessionRequest>,
) {
    let mut sessions = BTreeMap::<String, mpsc::UnboundedSender<SessionRequest>>::new();
    while let Some(request) = requests.recv().await {
        let session = request.session.clone();
        if request.lifecycle && !sessions.contains_key(&session) {
            let _ = request.response.send(Ok(ToolOutput::text("")));
            continue;
        }
        let owner = sessions
            .entry(session.clone())
            .or_insert_with(|| {
                let (sender, receiver) = mpsc::unbounded_channel();
                tokio::spawn(run_session(config.clone(), session.clone(), receiver));
                sender
            })
            .clone();
        if let Err(error) = owner.send(request) {
            if error.0.lifecycle {
                let _ = error.0.response.send(Ok(ToolOutput::text("")));
                sessions.remove(&session);
                continue;
            }
            let (sender, receiver) = mpsc::unbounded_channel();
            tokio::spawn(run_session(config.clone(), session.clone(), receiver));
            let _ = sender.send(error.0);
            sessions.insert(session, sender);
        }
    }
}

fn call_meta(session: &str, turn: Option<&str>, call: &str, model: &str) -> Value {
    json!({"x-codex-turn-metadata": turn_metadata(session, turn, call, model)})
}

fn turn_metadata(session: &str, turn: Option<&str>, call: &str, model: &str) -> Value {
    let mut metadata =
        json!({"session_id":session,"thread_id":session,"call_id":call,"model":model});
    if let Some(turn) = turn.filter(|turn| !turn.trim().is_empty()) {
        metadata["turn_id"] = json!(turn);
    }
    metadata
}

async fn run_session(
    config: ComputerConfig,
    session: String,
    mut requests: mpsc::UnboundedReceiver<SessionRequest>,
) {
    let mut process = None;
    let mut interrupted = false;
    let mut turns = BTreeMap::<String, (String, String)>::new();
    while let Some(request) = requests.recv().await {
        let SessionRequest {
            lifecycle,
            turn_id,
            mut call_id,
            mut model,
            name,
            arguments,
            mut response,
            ..
        } = request;

        if response.is_closed() {
            continue;
        }
        if lifecycle {
            let retained = turn_id.as_ref().and_then(|turn| turns.remove(turn));
            let Some((previous_call, previous_model)) = retained else {
                let _ = response.send(Ok(ToolOutput::text("")));
                continue;
            };
            let hook = config
                .provider_catalog
                .as_ref()
                .and_then(|catalog| catalog.iter().find(|tool| tool.name == "turn_ended"));
            if process.is_none() || interrupted || hook.is_none() {
                let _ = response.send(Ok(ToolOutput::text("")));
                continue;
            }
            call_id = previous_call;
            model = previous_model;
        }
        if interrupted && name != "js_reset" {
            let _ = response.send(Err("CUA session interrupted by caller cancellation or transport failure. Upstream/native input may still be running and effects are uncertain; do not replay uncertain input. Call cua_repl.js_reset, then inspect the surface before continuing. Reset does not prove earlier input stopped.".into()));
            continue;
        }

        let previous = process.take();
        if !lifecycle && let Some(turn) = &turn_id {
            turns.insert(turn.clone(), (call_id.clone(), model.clone()));
        }
        let execution = async {
            let mut process = match previous {
                Some(process) => process,
                None => Process::start(&config).await?,
            };
            let value = process
                .rpc(
                    "tools/call",
                    json!({"name":name,"arguments":arguments,
                "_meta": call_meta(&session, turn_id.as_deref(), &call_id, &model)}),
                )
                .await?;
            let output = output(value)?;
            Ok::<_, ToolError>((process, output))
        };
        let outcome = tokio::select! {
            biased;
            () = response.closed() => {
                turns.clear();
                interrupted = true;
                continue;
            }
            outcome = execution => outcome,
        };
        match outcome {
            Ok((owned, output)) => {
                process = Some(owned);
                if name == "js_reset" && output.success {
                    interrupted = false;
                }
                if response.send(Ok(output)).is_err() {
                    process = None;
                    turns.clear();
                    interrupted = true;
                }
            }
            Err(error) => {
                turns.clear();
                interrupted = true;
                let _ = response.send(Err(error));
            }
        }
    }
}

struct Process {
    _child: Child,
    input: ChildStdin,
    output: BufReader<ChildStdout>,
    next_id: u64,
    catalog: Vec<ProviderTool>,
}
impl Process {
    async fn start(config: &ComputerConfig) -> Result<Self, ToolError> {
        tokio::time::timeout(PROVIDER_STARTUP_TIMEOUT, Self::start_and_discover(config))
            .await
            .map_err(|_| "CUA provider startup timed out after 120 seconds during initialization or catalog discovery; owned transport discarded. Upstream/native work may continue.")?
    }

    async fn start_and_discover(config: &ComputerConfig) -> Result<Self, ToolError> {
        let mut command = Command::new(&config.executable);
        command.args(&config.args).env_clear();

        for name in PROVIDER_ENVIRONMENT {
            if let Some(value) = std::env::var_os(name) {
                command.env(name, value);
            }
        }
        command.envs(&config.environment);
        command
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .kill_on_drop(true);
        let mut child = command.spawn().map_err(|error| {
            format!(
                "Cannot start upstream Sky MCP provider {}: {error}. Check the host provider installation.",
                config.executable.display()
            )
        })?;
        let input = child.stdin.take().ok_or("CUA stdin unavailable")?;
        let output = BufReader::new(child.stdout.take().ok_or("CUA stdout unavailable")?);
        let mut process = Self {
            _child: child,
            input,
            output,
            next_id: 0,
            catalog: Vec::new(),
        };
        process.rpc("initialize", json!({"protocolVersion":"2025-06-18","capabilities":{},"clientInfo":{"name":"nanocode-cua","version":env!("CARGO_PKG_VERSION")}})).await?;
        process
            .send(json!({"jsonrpc":"2.0","method":"notifications/initialized"}))
            .await?;
        let catalog = process.discover().await?;
        if config
            .provider_catalog
            .as_ref()
            .is_some_and(|expected| expected != &catalog)
        {
            if let Some(cache) = &config.catalog_cache {
                cache.invalidate(&cache.fingerprint(config));
            }
            return Err(
                "CUA provider catalog changed; reconnect the attachment before invoking it".into(),
            );
        }
        process.catalog = catalog;
        Ok(process)
    }
    async fn discover(&mut self) -> Result<Vec<ProviderTool>, ToolError> {
        let mut tools = Vec::new();
        let mut cursor: Option<String> = None;
        let mut cursors = std::collections::BTreeSet::new();
        loop {
            let page = self
                .rpc(
                    "tools/list",
                    cursor
                        .as_ref()
                        .map_or_else(|| json!({}), |cursor| json!({"cursor": cursor})),
                )
                .await?;
            tools.extend(
                page["tools"]
                    .as_array()
                    .ok_or("CUA provider did not return an MCP tools/list catalog")?
                    .iter()
                    .cloned(),
            );
            match page.get("nextCursor") {
                None => break,
                Some(value) => {
                    let next = value
                        .as_str()
                        .filter(|value| !value.is_empty())
                        .ok_or("CUA provider returned an invalid tools/list cursor")?;
                    if !cursors.insert(next.to_owned()) {
                        return Err("CUA provider returned a repeated tools/list cursor".into());
                    }
                    cursor = Some(next.to_owned());
                }
            }
        }
        let catalog: Vec<ProviderTool> = tools
            .into_iter()
            .map(serde_json::from_value)
            .collect::<Result<_, _>>()?;
        validate_catalog(&catalog)?;
        Ok(catalog)
    }
    async fn send(&mut self, value: Value) -> Result<(), ToolError> {
        let mut bytes = serde_json::to_vec(&value)?;
        bytes.push(b'\n');
        self.input.write_all(&bytes).await?;
        self.input.flush().await?;
        Ok(())
    }
    async fn rpc(&mut self, method: &str, params: Value) -> Result<Value, ToolError> {
        self.next_id += 1;
        let id = self.next_id;
        self.send(json!({"jsonrpc":"2.0","id":id,"method":method,"params":params}))
            .await?;
        let mut line = Vec::new();
        loop {
            if self.output.read_until(b'\n', &mut line).await? == 0 {
                return Err("CUA runtime closed its output".into());
            }
            let value: Value = serde_json::from_slice(&line)?;
            line.clear();
            if value.get("method").is_some() {
                if let Some(request_id) = value.get("id") {
                    self.send(json!({"jsonrpc":"2.0","id":request_id,"error":{"code":-32601,"message":"Unsupported provider server request"}})).await?;
                }
                continue;
            }
            if value["id"] != id {
                return Err("CUA response belongs to a different call".into());
            }
            if let Some(error) = value.get("error") {
                return Err(format!(
                    "CUA: {}",
                    error["message"].as_str().unwrap_or("runtime error")
                )
                .into());
            }
            return value
                .get("result")
                .cloned()
                .ok_or_else(|| "CUA response is missing its result".into());
        }
    }
}

fn validate_catalog(catalog: &[ProviderTool]) -> Result<(), ToolError> {
    let mut names = std::collections::BTreeSet::new();
    for tool in catalog {
        if tool.name.is_empty() || !names.insert(&tool.name) {
            return Err("CUA provider tool names must be non-empty and unique".into());
        }
        if !tool.input_schema.is_object() {
            return Err("CUA provider tool inputSchema must be a JSON schema object".into());
        }
    }
    Ok(())
}

pub fn output(value: Value) -> ToolResult {
    let mut content = Vec::new();
    for item in value["content"]
        .as_array()
        .ok_or("CUA result has no content array")?
    {
        match item["type"].as_str() {
            Some("text") => content.push(ToolOutputContent::InputText {
                text: item["text"].as_str().ok_or("Invalid CUA text")?.into(),
            }),
            Some("image") => {
                let mime = item["mimeType"].as_str().ok_or("Invalid CUA image type")?;
                if !matches!(mime, "image/png" | "image/jpeg" | "image/webp") {
                    return Err("Unsupported CUA image type".into());
                }
                let data = item["data"].as_str().ok_or("Invalid CUA image")?;
                base64_simd::STANDARD
                    .check(data.as_bytes())
                    .or_else(|_| STANDARD.decode(data).map(|_| ()))?;
                let header = &data.as_bytes()[..data.len().min(16)];
                let mut bytes = [0; 12];
                let bytes = base64_simd::STANDARD
                    .decode(header, base64_simd::Out::from_slice(&mut bytes))?;
                let mime = if bytes.starts_with(b"\x89PNG\r\n\x1a\n") {
                    "image/png"
                } else if bytes.starts_with(&[0xff, 0xd8, 0xff]) {
                    "image/jpeg"
                } else if bytes.starts_with(b"RIFF") && bytes.get(8..12) == Some(b"WEBP") {
                    "image/webp"
                } else {
                    return Err("Invalid CUA image bytes".into());
                };
                content.push(ToolOutputContent::InputImage {
                    image_url: format!("data:{mime};base64,{data}"),
                    detail: ImageDetail::Original,
                });
            }
            Some("audio") => {
                let mime = item["mimeType"].as_str().ok_or("Invalid CUA audio type")?;
                if !matches!(mime, "audio/wav" | "audio/mpeg") {
                    return Err("Unsupported CUA audio type".into());
                }
                let data = item["data"].as_str().ok_or("Invalid CUA audio")?;
                base64_simd::STANDARD
                    .check(data.as_bytes())
                    .or_else(|_| STANDARD.decode(data).map(|_| ()))?;
                content.push(ToolOutputContent::InputAudio {
                    audio_url: format!("data:{mime};base64,{data}"),
                });
            }
            _ => return Err("Unsupported CUA output content".into()),
        }
    }
    let success = value["isError"] != true;
    let metadata = value.get("_meta").cloned();
    let mut output = ToolOutput::content(content).with_structured_result(value);
    if let Some(metadata) = metadata {
        output = output.with_metadata(metadata);
    }
    output.success = success;
    Ok(output)
}
