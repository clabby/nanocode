use std::{any::Any, path::PathBuf, sync::Arc};

use async_trait::async_trait;
use nanocode_code_mode::remote::CodeModeExecutors;
use nanocode_tool_contracts::ToolError;
use serde_json::Value;

use crate::host::RoutingCatalog;

pub const ATTACHMENT_REVOKED_REASON: &str =
    "attachment endpoint permanently revoked this attachment";

#[async_trait]
pub trait ProcessRouteStore: Send + Sync {
    async fn load(&self, session_id: &str) -> Result<Option<Value>, ToolError>;
    async fn save(&self, session_id: &str, routes: Value) -> Result<(), ToolError>;
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RemoteMode {
    Alongside,
    Replace,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum AttachmentCallOutcome {
    Completed,

    Unavailable,

    Ambiguous,

    Cancelled,
}

impl AttachmentCallOutcome {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Completed => "completed",
            Self::Unavailable => "unavailable",
            Self::Ambiguous => "ambiguous",
            Self::Cancelled => "cancelled",
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
#[non_exhaustive]
pub enum AttachmentError {
    #[error("attachment authentication failed: {0}")]
    Authentication(Box<str>),

    #[error("attached catalog failed: {0}")]
    Catalog(Box<str>),

    #[error("attachment was fenced: {0}")]
    Fenced(Box<str>),

    #[error("attachment transport failed: {0}")]
    Transport(Box<str>),

    #[error(
        "attachment call `{call_id}` timed out on runner `{runner}` (outcome: {outcome:?}); a dispatched call may have executed"
    )]
    Timeout {
        runner: Box<str>,
        call_id: Box<str>,
        outcome: AttachmentCallOutcome,
    },

    #[error("attachment is closed")]
    Closed,
}

impl AttachmentError {
    #[must_use]
    pub fn is_revoked(&self) -> bool {
        matches!(self, Self::Fenced(reason) if &**reason == ATTACHMENT_REVOKED_REASON)
    }
}

#[doc(hidden)]
#[derive(Clone)]
pub struct RemoteRoute {
    pub tools: Arc<dyn RemoteRunnerCatalog>,
    pub namespace: Arc<str>,
    pub workspace: PathBuf,
    pub mode: RemoteMode,
}

#[doc(hidden)]
pub trait RemoteRunnerCatalog: Send + Sync + 'static {
    fn as_any(&self) -> &dyn Any;
    fn router(&self) -> Arc<dyn WorkspaceRouter>;
}

#[doc(hidden)]
pub trait WorkspaceRouter: Send + Sync + 'static {
    fn route(
        &self,
        catalog: &mut RoutingCatalog<'_>,
        routes: &[RemoteRoute],
        store: Option<Arc<dyn ProcessRouteStore>>,
        pool: Option<&RunnerRouting>,
    );
}

#[derive(Clone)]
pub struct RunnerRouting {
    pool: Arc<dyn Any + Send + Sync>,
    router: Arc<dyn WorkspaceRouter>,
    executors: Option<CodeModeExecutors>,
}

impl RunnerRouting {
    #[doc(hidden)]
    pub fn new(pool: Arc<dyn Any + Send + Sync>, router: Arc<dyn WorkspaceRouter>) -> Self {
        Self {
            pool,
            router,
            executors: None,
        }
    }

    #[doc(hidden)]
    #[must_use]
    pub fn with_code_mode_executors(mut self, executors: CodeModeExecutors) -> Self {
        self.executors = Some(executors);
        self
    }

    #[must_use]
    pub const fn code_mode_executors(&self) -> Option<&CodeModeExecutors> {
        self.executors.as_ref()
    }

    #[doc(hidden)]
    #[must_use]
    pub fn pool(&self) -> &(dyn Any + Send + Sync) {
        self.pool.as_ref()
    }

    pub(crate) fn router(&self) -> &Arc<dyn WorkspaceRouter> {
        &self.router
    }
}
