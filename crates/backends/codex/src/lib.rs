#![deny(rustdoc::broken_intra_doc_links)]

#[cfg(all(target_family = "wasm", not(target_os = "unknown")))]
compile_error!(
    "nanocode-runtime supports browser/JavaScript WebAssembly \
     (`wasm32-unknown-unknown`), not WASI targets"
);

extern crate self as nanocode_runtime;

mod agent;
mod error;
mod harness;
mod reasoning;

mod registration;

mod service_tier_serde;
pub use harness::{ClaudeModel, HarnessFamily, ModelCapabilities, ModelSelection, ModelTransport};

mod model;

mod prompt_cache;

pub mod execution {
    pub use crate::agent::execution::*;
}

pub mod session;

pub mod usage;

#[doc(hidden)]
pub mod backend {
    pub use crate::agent::backend::*;
    pub use crate::agent::{AgentOwner, adopt};
    pub use crate::session::TurnBoundary;
}

pub use crate::client::{Model, ReasoningMode, Thinking, events::AgentEvents};

pub use crate::client::{OpenAi, ResponseError, ResponseErrorKind};
pub use agent::{
    Agent, AgentHandle, AgentSessionContext, BuilderBackend, PromptRequest, PromptRoute,
    SpawnOptions, Turn, TurnControl, TurnResult, WeakAgent,
};

pub use agent::{CodexBuilder, ExecutionEnvironment};

pub use error::CompactionRecovery;
pub use error::{ExecutionPolicyDisposition, NanocodeError, Result};
pub use nanocode_tool_host::tool;

pub use nanocode_tool_host::{Tool, Tools};
pub use session::{
    Capabilities, ForkPoint, ForkRequest, Lineage, Mutability, Origin, Persistence,
    SessionCheckpoint, SessionInfo,
};
pub use usage::{
    CostStatus, EstimatedUsdCost, ReportedTurnUsage, ServiceTier, TurnUsage, UsdAmount,
};

pub mod events {

    pub use crate::client::events::OpenAiEvent;
    pub use crate::client::events::{
        AcceptedInput, AgentEventData, AssistantDelta, AssistantEvent, AssistantMessage,
        CompactionCompleted, CompactionFailed, CompactionStarted, ContextEvent, EventUsage,
        ModelCallCompleted, ModelCallFailed, ModelCallPipeline, ModelCallStarted, ModelCallTiming,
        ModelEvent, ModelWarmupCompleted, ModelWarmupFailed, ModelWarmupStarted, ReasoningEvent,
        ReasoningSummaryDelta, RunError, RunEvent, RunMetrics, RunStarted, RunStatus, RunSteered,
        RunTerminal, ToolCall, ToolEvent, ToolInputDelta, ToolNotification, ToolResultEvent,
        ToolStatus, TransportEvent,
    };
    pub use crate::client::events::{
        AgentEvent, AgentEventKind, AgentEventPublisher, AgentEventTiming, AgentEvents, EventError,
        TimedAgentEvent, monotonic_now_ns,
    };
    pub use crate::client::responses::AgentMessageContent;
}

pub mod input {
    pub use crate::client::{
        ImageDetail, Prompt, PromptInput, PromptMessage, PromptMessageRole, UserInput,
        responses::{AgentMessageContent, ContentItem},
    };
}

#[cfg(not(target_family = "wasm"))]
pub mod transport {
    pub use crate::client::{
        responses::RequestProfile,
        tower::{
            DefaultResponsesService, ResponsesAttempt, ResponsesAttemptKind, ResponsesClient,
            ResponsesRetryPolicy, ResponsesServiceError, ResponsesServiceResponse,
        },
        transport::{ResponsesHistory, ResponsesTransport},
    };
    pub use crate::error::ResponsesError;
}

pub mod tools {
    pub use crate::__private;
    #[cfg(not(target_family = "wasm"))]
    pub use crate::builtin_tools::{WorkspaceTools, workspace_runtime};
    pub use crate::builtin_tools::{apply_patch, extensions, standard};
    #[doc(inline)]
    pub use nanocode_code_mode::{CodeModeToolObservation, CodeModeToolset};
    pub use nanocode_tool_host::*;
}

#[doc(hidden)]
pub mod __private {
    pub use nanocode_tool_host::__private::*;

    pub fn model_contract(
        runtime: &nanocode_tool_host::runtime::ToolRuntime,
        session_id: &str,
    ) -> (Vec<nanocode_tools::ToolDefinition>, Vec<(String, String)>) {
        let (mut definitions, summaries) =
            nanocode_tool_host::__private::model_contract(runtime, session_id);
        definitions.sort_by_key(|definition| match definition.name() {
            "exec" | "wait" => 0,
            "exec_command" => 1,
            "write_stdin" => 2,
            "update_plan" => 3,
            "apply_patch" => 4,
            "view_image" => 5,
            _ => 6,
        });
        (definitions, summaries)
    }
}

pub mod client;

pub mod builtin_tools;
pub mod durability;
pub use error::ErrorExt;

pub use agent::checkpoint::CodexCheckpointExt;

pub use crate::client::OpenAi as Codex;

mod runtime;
pub use runtime::CodexHarness;
