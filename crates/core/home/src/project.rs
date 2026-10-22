use std::{
    fs, io,
    path::{Path, PathBuf},
};

use serde::Serialize;

use crate::{
    Diagnostic, Scope,
    fs_util::{PathState, path_state},
    instructions::{
        InstructionKind, InstructionSource, Loaded, combine, load_candidate, push_unique,
    },
    skills::SkillRoot,
};

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct ProjectHome {
    root: PathBuf,
    workspace: PathBuf,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct InstructionFile {
    pub path: PathBuf,
    pub canonical_path: PathBuf,

    pub directory: PathBuf,
    pub kind: InstructionKind,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize)]
pub struct ProjectInstructions {
    pub sources: Vec<InstructionSource>,
    pub diagnostics: Vec<Diagnostic>,
}

impl ProjectInstructions {
    pub fn combined(&self) -> Option<String> {
        combine(&self.sources)
    }
}

impl ProjectHome {
    pub fn discover(workspace: impl AsRef<Path>) -> Self {
        let workspace = workspace.as_ref();
        let workspace = fs::canonicalize(workspace).unwrap_or_else(|_| workspace.to_path_buf());
        let root = workspace
            .ancestors()
            .find(|dir| fs::symlink_metadata(dir.join(".git")).is_ok())
            .unwrap_or(&workspace)
            .to_path_buf();
        Self { root, workspace }
    }

    pub fn new(root: impl Into<PathBuf>, workspace: impl Into<PathBuf>) -> io::Result<Self> {
        let (root, workspace) = (root.into(), workspace.into());
        if !workspace.starts_with(&root) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "workspace is outside root",
            ));
        }
        Ok(Self { root, workspace })
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    pub fn workspace(&self) -> &Path {
        &self.workspace
    }

    pub fn directories(&self) -> Vec<PathBuf> {
        let mut dirs = self
            .workspace
            .ancestors()
            .take_while(|dir| dir.starts_with(&self.root))
            .map(Path::to_path_buf)
            .collect::<Vec<_>>();
        dirs.reverse();
        dirs
    }

    pub fn instruction_files(&self) -> (Vec<InstructionFile>, Vec<Diagnostic>) {
        let mut files: Vec<InstructionFile> = Vec::new();
        let mut diagnostics = Vec::new();
        for directory in self.directories() {
            let [(first, first_kind), (second, second_kind)] = crate::PROJECT_INSTRUCTION_FILES;
            let override_path = directory.join(first);
            let (path, kind) = match path_state(&override_path) {
                Ok(PathState::File { .. }) => (override_path, first_kind),
                _ => (directory.join(second), second_kind),
            };
            let canonical_path = match path_state(&path) {
                Ok(PathState::File { canonical }) => canonical,
                Ok(PathState::Absent) => continue,
                Ok(PathState::Dangling) => {
                    diagnostics.push(Diagnostic::warning(&path, "dangling symlink ignored"));
                    continue;
                }
                Ok(_) => continue,
                Err(error) => {
                    diagnostics.push(Diagnostic::warning(&path, format!("unreadable: {error}")));
                    continue;
                }
            };
            if let Some(first) = files.iter().find(|f| f.canonical_path == canonical_path) {
                diagnostics.push(Diagnostic::info(
                    &path,
                    format!("same file as {}; listed once", first.path.display()),
                ));
                continue;
            }
            files.push(InstructionFile {
                path,
                canonical_path,
                directory,
                kind,
            });
        }
        (files, diagnostics)
    }

    pub fn read_instructions(&self) -> ProjectInstructions {
        let (files, mut diagnostics) = self.instruction_files();
        let mut sources = Vec::new();
        for file in files {
            if let Loaded::Source(source) = load_candidate(
                &file.path,
                Scope::Project,
                file.kind,
                usize::MAX,
                &mut diagnostics,
            ) {
                push_unique(&mut sources, source, &mut diagnostics);
            }
        }
        ProjectInstructions {
            sources,
            diagnostics,
        }
    }

    pub fn skill_roots(&self) -> Vec<SkillRoot> {
        let mut roots = Vec::new();
        for directory in self.directories().into_iter().rev() {
            roots.push(SkillRoot::new(
                directory.join(".agents/skills"),
                Scope::Project,
            ));
        }
        roots
    }
}
