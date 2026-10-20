pub(crate) mod runtime;

use std::{collections::HashSet, sync::Arc};

use crate::{ToolDefinition, Tools};

use runtime::PreparedToolEntry;
pub(crate) use runtime::{PreparedToolCall, PreparedToolError, PreparedToolRuntime};

pub(crate) struct PreparedTools {
    entries: Vec<PreparedToolEntry>,

    mcps: Vec<Arc<crate::mcp::Mcp>>,

    workspaces: Vec<Arc<dyn crate::workspace_runtime::WorkspaceExecutor>>,
}

impl PreparedTools {
    pub(crate) fn prepare(tools: &Tools) -> Result<Self, PreparedToolError> {
        if tools.workspace_enabled() && tools.workspace_source().is_none() {
            return Err(PreparedToolError::NonAttachable(
                "enabled workspace tools require a pinned WorkspaceTools source".into(),
            ));
        }
        if tools.web_search_enabled() {
            return Err(PreparedToolError::NonAttachable(
                "built-in web search has no attached executor; disable it for this placement"
                    .into(),
            ));
        }
        if tools.image_generation_enabled() {
            return Err(PreparedToolError::NonAttachable(
                "built-in image generation has no attached executor; disable it for this placement"
                    .into(),
            ));
        }
        if tools.has_unattachable_provider() {
            return Err(PreparedToolError::NonAttachable(
                "a generic dynamic provider cannot produce an immutable catalog; add MCP with ToolsBuilder::add".into(),
            ));
        }

        #[allow(unused_mut, reason = "workspace and MCP features append entries")]
        let mut entries = tools
            .registered_tools()
            .map(|tool| {
                let definition = tool.definition();
                validate_definition(&definition)?;
                Ok(PreparedToolEntry::fixed(
                    definition,
                    tool.supports_parallel_tool_calls(),
                    Arc::clone(tool),
                ))
            })
            .collect::<Result<Vec<_>, PreparedToolError>>()?;

        let mut workspaces = Vec::new();

        if tools.workspace_enabled()
            && let Some(workspace) = tools.workspace_source()
        {
            let workspace = workspace.create(tools.process_environment().as_ref().clone(), None);
            for tool in workspace.tools() {
                let definition = tool.definition();
                validate_definition(&definition)?;
                entries.push(PreparedToolEntry::workspace(
                    definition,
                    workspace.timeout_ms(tool.definition().name()),
                    tool,
                ));
            }

            workspaces.push(workspace);
        }

        let mut names = HashSet::with_capacity(entries.len());
        for entry in &entries {
            let name = entry.definition().name();
            if !names.insert(name.to_owned()) {
                return Err(PreparedToolError::DuplicateTool(name.into()));
            }
        }

        let policies = tools.code_mode_policies();
        for entry in &mut entries {
            entry.classify(&policies);
        }

        let mcps = tools
            .attachable_providers()
            .iter()
            .map(|provider| {
                Arc::clone(provider)
                    .downcast::<crate::mcp::Mcp>()
                    .map_err(|_| {
                        PreparedToolError::NonAttachable(
                            "an attachable provider is not an MCP catalog".into(),
                        )
                    })
            })
            .collect::<Result<Vec<_>, _>>()?;

        Ok(Self {
            entries,

            mcps,

            workspaces,
        })
    }
}

fn validate_definition(definition: &ToolDefinition) -> Result<(), PreparedToolError> {
    match definition {
        ToolDefinition::Function { .. } => Ok(()),
        ToolDefinition::Custom { format, .. } if &*format.kind == "grammar" => Ok(()),
        ToolDefinition::Custom { name, .. } | ToolDefinition::Namespace { name, .. } => {
            Err(PreparedToolError::UnsupportedDefinition(name.clone()))
        }
        ToolDefinition::ToolSearch { .. } => Err(PreparedToolError::UnsupportedDefinition(
            "tool_search".into(),
        )),
    }
}
