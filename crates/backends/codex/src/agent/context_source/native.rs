use std::{
    path::{Path, PathBuf},
    sync::Arc,
};

use super::agents_md::{combine_instructions, load_global_instructions, load_instructions};
use crate::{NanocodeError, Result};

#[derive(Clone, Default)]
pub(crate) struct ContextSourceConfig {
    nanocode_home: Option<PathBuf>,
    claude_home: Option<PathBuf>,
    execution_environment: Option<super::ExecutionEnvironment>,
}

impl ContextSourceConfig {
    pub(crate) fn set_nanocode_home(&mut self, nanocode_home: PathBuf) {
        self.nanocode_home = Some(nanocode_home);
    }

    pub(crate) fn nanocode_home(&self) -> Option<&Path> {
        self.nanocode_home.as_deref()
    }

    pub(crate) fn set_claude_home(&mut self, claude_home: PathBuf) {
        self.claude_home = Some(claude_home);
    }

    pub(crate) fn set_execution_environment(&mut self, environment: super::ExecutionEnvironment) {
        self.execution_environment = Some(environment);
    }

    pub(crate) const fn execution_environment(&self) -> Option<&super::ExecutionEnvironment> {
        self.execution_environment.as_ref()
    }

    pub(crate) fn build(&self) -> ContextSource {
        ContextSource {
            global_instructions: load_global_instructions(
                self.nanocode_home(),
                self.claude_home.as_deref(),
            ),
            execution_environment: self.execution_environment.clone(),
        }
    }
}

#[derive(Clone)]
pub(crate) struct ContextSource {
    global_instructions: Option<Arc<str>>,
    execution_environment: Option<super::ExecutionEnvironment>,
}

impl ContextSource {
    pub(crate) fn resolve_workspace(&self, requested: Option<&str>) -> Result<String> {
        if self.execution_environment.is_some() {
            return Ok(requested.unwrap_or(".").to_owned());
        }
        let requested = PathBuf::from(requested.unwrap_or("."));
        let resolved = std::fs::canonicalize(&requested).map_err(|source| {
            NanocodeError::ResolveWorkspace {
                path: requested,
                source,
            }
        })?;
        if !resolved.is_dir() {
            return Err(NanocodeError::WorkspaceNotDirectory { path: resolved });
        }
        resolved
            .into_os_string()
            .into_string()
            .map_err(|path| NanocodeError::WorkspaceNotUtf8 {
                path: PathBuf::from(path),
            })
    }

    pub(crate) fn project_instructions(&self, workspace: &str) -> Option<String> {
        if let Some(environment) = &self.execution_environment {
            combine_instructions(
                self.global_instructions.as_deref(),
                environment.project_instructions.as_deref(),
            )
        } else {
            load_instructions(Path::new(workspace), self.global_instructions.as_deref())
        }
    }

    pub(crate) fn global_instructions(&self) -> Option<Arc<str>> {
        self.global_instructions.as_ref().map(Arc::clone)
    }

    pub(crate) const fn execution_environment(&self) -> Option<&super::ExecutionEnvironment> {
        self.execution_environment.as_ref()
    }

    pub(crate) fn with_fallback_global(mut self, fallback: Option<Arc<str>>) -> Self {
        if self.global_instructions.is_none() {
            self.global_instructions = fallback;
        }
        self
    }
}
