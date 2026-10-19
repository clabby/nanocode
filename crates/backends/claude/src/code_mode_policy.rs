use std::sync::{Arc, LazyLock};

use nanocode_tool_host::{CodeModeToolPolicy, WorkspaceToolRouting};

use crate::tools::host::HostTool;

static POLICY: LazyLock<Arc<dyn CodeModeToolPolicy>> = LazyLock::new(|| Arc::new(ClaudeToolPolicy));

pub(crate) fn claude_tool_policy() -> Arc<dyn CodeModeToolPolicy> {
    Arc::clone(&POLICY)
}

struct ClaudeToolPolicy;

impl CodeModeToolPolicy for ClaudeToolPolicy {
    fn workspace_routing(&self, name: &str) -> Option<WorkspaceToolRouting> {
        if name == "Bash" {
            return Some(WorkspaceToolRouting::Shell);
        }
        [
            "Read",
            "Write",
            "Edit",
            HostTool::TaskOutput.name(),
            HostTool::TaskStop.name(),
        ]
        .contains(&name)
        .then_some(WorkspaceToolRouting::File)
    }

    fn exclusive(&self, name: &str) -> bool {
        ["Write", "Edit"].contains(&name)
    }
}
