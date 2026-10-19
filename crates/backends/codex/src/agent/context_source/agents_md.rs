use std::{path::Path, sync::Arc};

use nanocode_home::{AgentHome, Convention, Diagnostic, Level, ProjectHome, combine_instructions};
use tracing::warn;

pub(crate) fn load_global_instructions(
    nanocode_home: Option<&Path>,
    claude_home: Option<&Path>,
) -> Option<Arc<str>> {
    let (nanocode, claude) = match (nanocode_home, claude_home) {
        (None, None) => return None,
        (Some(nanocode), Some(claude)) => (nanocode, claude),
        (Some(home), None) | (None, Some(home)) => (home, home),
    };
    let global = AgentHome::new(nanocode, claude).global_instructions();
    report(&global.diagnostics);

    let texts = global
        .sources
        .iter()
        .filter(|source| match source.convention {
            Convention::Codex => nanocode_home.is_some(),
            Convention::Claude => claude_home.is_some(),
        })
        .map(|source| source.text.as_str())
        .collect::<Vec<_>>();
    (!texts.is_empty()).then(|| Arc::from(texts.join("\n\n")))
}

pub(super) fn load_instructions(
    workspace: &Path,
    global_instructions: Option<&str>,
) -> Option<String> {
    combine_instructions(
        global_instructions,
        load_project_instructions(workspace).as_deref(),
    )
}

fn load_project_instructions(workspace: &Path) -> Option<String> {
    let instructions = ProjectHome::discover(workspace).read_instructions();
    report(&instructions.diagnostics);
    instructions.combined()
}

fn report(diagnostics: &[Diagnostic]) {
    for diagnostic in diagnostics {
        if matches!(diagnostic.level, Level::Warning) {
            warn!(
                path = %diagnostic.path.display(),
                message = %diagnostic.message,
                "instruction file skipped"
            );
        }
    }
}
