use std::{
    path::{Path, PathBuf},
    sync::Arc,
};

use crate::tools::{BashRequest, BashResult, ClaudeBash, SandboxBashExecutor, text_files};
use nanocode_process_contracts::{FileHost, ForegroundCommand, ForegroundExecutor};
use nanocode_tool_host::{
    Tool, ToolContext, ToolDefinition, ToolInput, ToolOutput, ToolResult, Tools, ToolsBuildError,
    embedded::HostFuture,
};
use serde_json::Value;

pub fn host_tools(
    workspace: PathBuf,
    files: Arc<dyn FileHost>,
    shell: Arc<dyn ForegroundExecutor>,
) -> Result<Tools, ToolsBuildError> {
    let workspace: Arc<Path> = workspace.into();
    let mut tools = Tools::builder().without_defaults();
    for schema in text_files::definitions() {
        let name = schema["name"].as_str().unwrap_or_default().to_owned();
        tools = tools.tool(HostFileTool {
            definition: ToolDefinition::function(
                name.clone(),
                schema["description"].as_str().unwrap_or_default(),
                schema["input_schema"].clone(),
            ),
            name,
            workspace: Arc::clone(&workspace),
            files: Arc::clone(&files),
        });
    }
    tools
        .tool(HostBashTool(ClaudeBash::new(HostBash { workspace, shell })))
        .build()
        .map(|tools| tools.with_code_mode_policy(crate::code_mode_policy::claude_tool_policy()))
}

struct HostFileTool {
    name: String,
    definition: ToolDefinition,
    workspace: Arc<Path>,
    files: Arc<dyn FileHost>,
}

impl HostFileTool {
    async fn run(&self, input: &Value, budget: usize) -> Result<String, String> {
        text_files::check(&self.name, input)?;
        let file_path = input
            .get("file_path")
            .and_then(Value::as_str)
            .filter(|path| !path.is_empty())
            .ok_or("missing or invalid file_path")?;
        let path = self.workspace.join(file_path);
        match self.name.as_str() {
            "Read" => {
                if input.get("pages").is_some() {
                    return Err("pages is only applicable to PDF files".into());
                }
                let content = self.existing(&path).await?;
                text_files::read(file_path, content.len() as u64, input, budget, || {
                    Ok(content.lines().map(|line| Ok(line.to_owned())))
                })
            }
            "Write" => {
                let content = input
                    .get("content")
                    .and_then(Value::as_str)
                    .ok_or("missing or invalid content")?;
                self.files.write(&path, content).await?;
                Ok(format!("Wrote {}", path.display()))
            }
            _ => {
                let edit = text_files::Edit::parse(input)?;
                let content = self.existing(&path).await?;
                let (updated, count) = edit.apply(&content)?;
                self.files.write(&path, &updated).await?;
                Ok(format!(
                    "Edited {} ({count} replacement(s))",
                    path.display()
                ))
            }
        }
    }

    async fn existing(&self, path: &Path) -> Result<String, String> {
        self.files
            .read(path)
            .await?
            .ok_or_else(|| format!("path: {} does not exist", path.display()))
    }
}

impl Tool for HostFileTool {
    fn definition(&self) -> ToolDefinition {
        self.definition.clone()
    }

    fn execute<'life0, 'life1, 'async_trait>(
        &'life0 self,
        input: ToolInput,
        context: ToolContext<'life1>,
    ) -> HostFuture<'async_trait, ToolResult>
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        Self: 'async_trait,
    {
        Box::pin(async move {
            let input: Value = input.decode_json()?;
            Ok(
                match self.run(&input, context.output_token_budget()).await {
                    Ok(text) => ToolOutput::text(text),
                    Err(error) => ToolOutput::error(error),
                },
            )
        })
    }
}

struct HostBash {
    workspace: Arc<Path>,
    shell: Arc<dyn ForegroundExecutor>,
}

impl SandboxBashExecutor for HostBash {
    async fn execute(&self, request: BashRequest) -> Result<BashResult, String> {
        let output = self
            .shell
            .execute(ForegroundCommand {
                workspace: &self.workspace,
                command: &request.command,
                timeout: std::time::Duration::from_millis(request.timeout_ms),
                max_stdout: request.max_stdout_bytes,
                max_stderr: request.max_stderr_bytes,
                session: request.session.as_ref(),
                output: None,
            })
            .await?;
        Ok(BashResult {
            stdout: output.stdout,
            stderr: output.stderr,
            exit_code: output.exit_code,
            truncated: output.truncated,
        })
    }
}

struct HostBashTool(ClaudeBash<HostBash>);

impl Tool for HostBashTool {
    fn definition(&self) -> ToolDefinition {
        let schema = ClaudeBash::<HostBash>::definitions().remove(0);
        ToolDefinition::function(
            "Bash",
            schema["description"].as_str().unwrap_or_default(),
            schema["input_schema"].clone(),
        )
    }

    fn execute<'life0, 'life1, 'async_trait>(
        &'life0 self,
        input: ToolInput,
        context: ToolContext<'life1>,
    ) -> HostFuture<'async_trait, ToolResult>
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        Self: 'async_trait,
    {
        Box::pin(async move {
            let input = input.decode_json::<Value>()?;
            Ok(
                match self
                    .0
                    .execute_call("Bash", input, None, Some(context.call_id()))
                    .await
                {
                    Ok(output) => {
                        let value = serde_json::from_str::<Value>(&output).unwrap_or(Value::Null);
                        let text = value["message"].as_str().map_or(output, str::to_owned);
                        ToolOutput::text(text).with_structured_result(value)
                    }
                    Err(error) => ToolOutput::error(error),
                },
            )
        })
    }
}
