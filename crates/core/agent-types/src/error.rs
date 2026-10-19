use std::sync::Arc;
use std::{io, path::PathBuf};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ExecutionPolicyDisposition {
    Retry,

    Reopen,

    Fatal,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
pub enum CompactionRecovery {
    #[default]
    None,

    ReplaceRejectedImages,
}

#[derive(Debug, thiserror::Error)]
pub enum NanocodeError {
    #[error("invalid task request: {0}")]
    InvalidRequest(String),

    #[error("failed to resolve task workspace {path}: {source}")]
    ResolveWorkspace {
        path: PathBuf,

        #[source]
        source: io::Error,
    },

    #[error("task workspace is not a directory: {path}")]
    WorkspaceNotDirectory { path: PathBuf },

    #[error("task workspace path is not valid UTF-8: {path}")]
    WorkspaceNotUtf8 { path: PathBuf },

    #[error("an active agent session cannot change workspace from {current} to {requested}")]
    WorkspaceChanged { current: String, requested: String },

    #[error("malformed Responses API event: {detail}")]
    MalformedResponse { detail: &'static str },

    #[error("invalid Responses attempt state: {detail}")]
    InvalidAttemptState { detail: &'static str },

    #[error("compaction failed: {detail}")]
    CompactionFailed {
        detail: String,

        requires_session_stop: bool,

        recovery: CompactionRecovery,
    },

    #[error("failed to fingerprint the immutable prompt prefix: {0}")]
    SerializePromptPrefix(#[source] serde_json::Error),

    #[error("the agent stopped before accepting the command")]
    AgentStopped,

    #[error("the execution-policy-owned agent stopped and must be reopened")]
    ExecutionPolicyOwnerStopped,

    #[error("the agent stopped before the turn completed")]
    TurnStopped,

    #[error(transparent)]
    Shutdown(Arc<Self>),

    #[error("the targeted turn is queued, completed, or otherwise not active for steering")]
    TurnNotSteerable,

    #[error("the active turn's steering queue is full")]
    SteerQueueFull,

    #[error("the targeted turn has already completed or been cancelled")]
    TurnNotCancellable,

    #[error("the turn was cancelled")]
    TurnCancelled,

    #[error("the agent has no safe conversation boundary to fork")]
    ForkBeforeCompletedTurn,

    #[error("the completed turn belongs to a different conversation lineage")]
    CheckpointLineageMismatch,

    #[error("invalid session checkpoint: {0}")]
    InvalidCheckpoint(String),

    #[error("{layer} execution policy failed: {source}")]
    ExecutionPolicy {
        layer: &'static str,

        disposition: ExecutionPolicyDisposition,

        #[source]
        source: Arc<dyn std::error::Error + Send + Sync>,
    },

    #[error("identified prompt submission requires a configured execution policy")]
    ExecutionPolicyNotConfigured,

    #[error("invalid execution policy state: {0}")]
    InvalidExecutionPolicy(String),

    #[error("pre-compaction preservation failed: {0}")]
    BeforeCompactionFailed(String),

    #[error("execution policy does not implement required capability `{capability}`")]
    ExecutionPolicyCapabilityUnsupported { capability: &'static str },

    #[error(
        "cannot {operation} from an agent with an attached execution policy; build the branch with its own execution policy"
    )]
    ExecutionPolicyBranchUnsupported { operation: &'static str },

    #[error("execution policy payload is invalid: {0}")]
    ExecutionPayload(#[source] serde_json::Error),

    #[error("a policy-replayed result cannot be used as an in-process fork checkpoint")]
    ReplayedCheckpointUnavailable,

    #[error("checkpoint belongs to the {found:?} harness, not {expected:?}")]
    CheckpointFamilyMismatch {
        expected: crate::HarnessFamily,

        found: crate::HarnessFamily,
    },

    #[error("the selected agent backend does not support {capability}")]
    UnsupportedCapability { capability: &'static str },

    #[error("agent backend violated the lifecycle contract: {detail}")]
    BackendContract { detail: &'static str },

    #[error("{backend} backend failed: {source}")]
    Backend {
        backend: &'static str,

        #[source]
        source: Arc<dyn std::error::Error + Send + Sync>,
    },

    #[error("durable operation previously failed: {0}")]
    ReplayedExecutionFailed(String),

    #[error("building an agent requires an active Tokio runtime")]
    TokioRuntimeUnavailable,

    #[error(transparent)]
    Event(#[from] nanocode_types_core::events::EventError),

    #[error(transparent)]
    Response(BackendError),

    #[error("failed to build tools for an agent driver: {0}")]
    Tools(#[from] nanocode_tool_contracts::ToolsBuildError),
}

impl NanocodeError {
    #[doc(hidden)]
    pub fn backend<E>(backend: &'static str, source: E) -> Self
    where
        E: std::error::Error + Send + Sync + 'static,
    {
        Self::Backend {
            backend,
            source: Arc::new(source),
        }
    }

    #[doc(hidden)]
    pub fn execution_policy<E>(layer: &'static str, source: E) -> Self
    where
        E: std::error::Error + Send + Sync + 'static,
    {
        Self::ExecutionPolicy {
            layer,
            disposition: ExecutionPolicyDisposition::Fatal,
            source: Arc::new(source),
        }
    }

    #[doc(hidden)]
    pub fn execution_policy_with_disposition<E>(
        layer: &'static str,
        disposition: ExecutionPolicyDisposition,
        source: E,
    ) -> Self
    where
        E: std::error::Error + Send + Sync + 'static,
    {
        Self::ExecutionPolicy {
            layer,
            disposition,
            source: Arc::new(source),
        }
    }

    #[must_use]
    pub fn execution_policy_disposition(&self) -> Option<ExecutionPolicyDisposition> {
        match self {
            Self::ExecutionPolicy { disposition, .. } => Some(*disposition),
            Self::ExecutionPolicyOwnerStopped => Some(ExecutionPolicyDisposition::Reopen),
            Self::Shutdown(source) => source.execution_policy_disposition(),
            _ => None,
        }
    }

    #[must_use]
    pub fn requires_image_repair(&self) -> bool {
        match self {
            Self::CompactionFailed {
                recovery,
                requires_session_stop,
                ..
            } => !requires_session_stop && *recovery == CompactionRecovery::ReplaceRejectedImages,
            Self::Shutdown(source) => source.requires_image_repair(),
            Self::Response(error) => error.requires_image_repair(),
            _ => false,
        }
    }
}

pub type Result<T> = std::result::Result<T, NanocodeError>;

pub trait BackendFailure: std::error::Error + Send + Sync + 'static {
    fn requires_image_repair(&self) -> bool {
        false
    }

    fn retryable(&self) -> bool {
        false
    }

    fn as_any(&self) -> &dyn std::any::Any;
}

#[derive(Debug)]
pub struct BackendError(pub Box<dyn BackendFailure>);
impl std::fmt::Display for BackendError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}
impl std::error::Error for BackendError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self.0.as_ref())
    }
}
impl std::ops::Deref for BackendError {
    type Target = dyn BackendFailure;
    fn deref(&self) -> &Self::Target {
        self.0.as_ref()
    }
}
