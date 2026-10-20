use std::sync::OnceLock;

use serde_json::Value;

use crate::ToolDefinition;

pub const VERSION: &str = "2.1.304";
pub const PROVENANCE: &str = include_str!("../prompts/upstream.json");

pub fn system_blocks() -> Vec<Value> {
    static BLOCKS: OnceLock<Vec<Value>> = OnceLock::new();
    BLOCKS
        .get_or_init(|| {
            serde_json::from_str(include_str!("../prompts/claude-code-system.json"))
                .expect("verified upstream Claude system blocks")
        })
        .clone()
}

pub fn tool_definitions() -> &'static [ToolDefinition] {
    static TOOLS: OnceLock<Vec<ToolDefinition>> = OnceLock::new();
    TOOLS.get_or_init(|| {
        serde_json::from_str(nanocode_backend_claude_tools::upstream::TOOL_DEFINITIONS)
            .expect("verified upstream Claude tool contracts")
    })
}

pub fn tool_definition(name: &str) -> Option<&'static ToolDefinition> {
    tool_definitions().iter().find(|tool| tool.name == name)
}

#[cfg(not(target_family = "wasm"))]
pub(crate) fn tool_value(name: &str) -> Value {
    nanocode_backend_claude_tools::upstream::tool_value(name)
}
