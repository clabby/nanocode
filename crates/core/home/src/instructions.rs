use std::path::{Path, PathBuf};

use serde::Serialize;

use crate::{
    AgentHome, Diagnostic, Scope,
    fs_util::{PathState, path_state, read_bounded},
};

pub const MAX_INSTRUCTION_FILE_BYTES: usize = 32 * 1024;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum InstructionKind {
    AgentsOverride,

    Agents,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct InstructionSource {
    pub path: PathBuf,
    pub canonical_path: PathBuf,
    pub scope: Scope,
    pub kind: InstructionKind,

    pub text: String,
    pub truncated: bool,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize)]
pub struct GlobalInstructions {
    pub sources: Vec<InstructionSource>,
    pub diagnostics: Vec<Diagnostic>,
}

impl GlobalInstructions {
    pub fn combined(&self) -> Option<String> {
        combine(&self.sources)
    }
}

pub const PROJECT_DOC_SEPARATOR: &str = "\n\n--- project-doc ---\n\n";

#[must_use]
pub fn combine_instructions(global: Option<&str>, project: Option<&str>) -> Option<String> {
    match (global, project) {
        (Some(global), Some(project)) => Some(format!("{global}{PROJECT_DOC_SEPARATOR}{project}")),
        (Some(text), None) | (None, Some(text)) => Some(text.to_owned()),
        (None, None) => None,
    }
}

pub(crate) fn combine(sources: &[InstructionSource]) -> Option<String> {
    (!sources.is_empty()).then(|| {
        sources
            .iter()
            .map(|source| source.text.as_str())
            .collect::<Vec<_>>()
            .join("\n\n")
    })
}

pub(crate) enum Loaded {
    Source(InstructionSource),

    Empty,
    Missing,
}

pub(crate) fn load_candidate(
    path: &Path,
    scope: Scope,
    kind: InstructionKind,
    limit: usize,
    diagnostics: &mut Vec<Diagnostic>,
) -> Loaded {
    let canonical = match path_state(path) {
        Ok(PathState::File { canonical }) => canonical,
        Ok(PathState::Absent) => return Loaded::Missing,
        Ok(PathState::Dangling) => {
            diagnostics.push(Diagnostic::warning(path, "dangling symlink ignored"));
            return Loaded::Missing;
        }
        Ok(_) => {
            diagnostics.push(Diagnostic::warning(path, "not a regular file; ignored"));
            return Loaded::Missing;
        }
        Err(error) => {
            diagnostics.push(Diagnostic::warning(path, format!("unreadable: {error}")));
            return Loaded::Missing;
        }
    };
    match read_bounded(&canonical, limit) {
        Ok((text, truncated)) => {
            if truncated {
                diagnostics.push(Diagnostic::warning(
                    path,
                    format!("exceeds {limit} bytes; truncated"),
                ));
            }
            let text = text.trim();
            if text.is_empty() {
                return Loaded::Empty;
            }
            Loaded::Source(InstructionSource {
                path: path.to_path_buf(),
                canonical_path: canonical,
                scope,
                kind,
                text: text.to_owned(),
                truncated,
            })
        }
        Err(error) => {
            diagnostics.push(Diagnostic::warning(path, format!("unreadable: {error}")));
            Loaded::Missing
        }
    }
}

pub(crate) fn push_unique(
    sources: &mut Vec<InstructionSource>,
    source: InstructionSource,
    diagnostics: &mut Vec<Diagnostic>,
) -> bool {
    if let Some(existing) = sources
        .iter()
        .find(|s| s.canonical_path == source.canonical_path)
    {
        diagnostics.push(Diagnostic::info(
            &source.path,
            format!("same file as {}; included once", existing.path.display()),
        ));
        return false;
    }
    if let Some(existing) = sources.iter().find(|s| s.text == source.text) {
        diagnostics.push(Diagnostic::info(
            &source.path,
            format!("identical to {}; included once", existing.path.display()),
        ));
        return false;
    }
    sources.push(source);
    true
}

pub fn global_instructions(nanocode_home: &Path) -> GlobalInstructions {
    let mut result = GlobalInstructions::default();
    for (name, kind) in [
        ("AGENTS.override.md", InstructionKind::AgentsOverride),
        ("AGENTS.md", InstructionKind::Agents),
    ] {
        match load_candidate(
            &nanocode_home.join(name),
            Scope::User,
            kind,
            MAX_INSTRUCTION_FILE_BYTES,
            &mut result.diagnostics,
        ) {
            Loaded::Source(source) => {
                push_unique(&mut result.sources, source, &mut result.diagnostics);
                break;
            }
            Loaded::Empty | Loaded::Missing => {}
        }
    }
    result
}

impl AgentHome {
    pub fn global_instructions(&self) -> GlobalInstructions {
        global_instructions(self.nanocode_home())
    }
}
