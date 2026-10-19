use crate::{ClaudeToolInvocation, ClaudeToolReply};
use serde_json::Value;
use std::{future::Future, pin::Pin};

#[cfg(not(target_family = "wasm"))]
pub type ClaudeHookFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;
#[cfg(target_family = "wasm")]
pub type ClaudeHookFuture<'a, T> = Pin<Box<dyn Future<Output = T> + 'a>>;

#[derive(Clone, Debug)]
pub enum ClaudeToolDecision {
    Allow,
    UpdateInput(Value),
    Deny(String),
}

pub trait ClaudeToolHooks: Send + Sync {
    fn handles_lifecycle(&self, _event: &ClaudeLifecycleEvent) -> bool {
        false
    }

    fn lifecycle<'a>(
        &'a self,
        _invocation: &'a ClaudeLifecycleInvocation,
    ) -> ClaudeHookFuture<'a, Result<ClaudeLifecycleOutcome, String>> {
        Box::pin(async { Ok(ClaudeLifecycleOutcome::default()) })
    }

    fn before<'a>(
        &'a self,
        name: &'a str,
        input: &'a Value,
        invocation: &'a ClaudeToolInvocation,
    ) -> ClaudeHookFuture<'a, Result<ClaudeToolDecision, String>>;

    fn after<'a>(
        &'a self,
        _name: &'a str,
        _input: &'a Value,
        _invocation: &'a ClaudeToolInvocation,
        _reply: &'a ClaudeToolReply,
    ) -> ClaudeHookFuture<'a, Result<(), String>> {
        Box::pin(async { Ok(()) })
    }
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
#[serde(tag = "hook_event_name")]
pub enum ClaudeLifecycleEvent {
    SessionStart {
        source: String,
    },
    UserPromptSubmit {
        prompt: String,
    },
    Stop {
        stop_hook_active: bool,
        last_assistant_message: String,
    },
    PreCompact {
        trigger: String,
        custom_instructions: String,
    },
    PostCompact {
        trigger: String,
        compact_summary: String,
    },
    StopFailure {
        error: String,
        error_details: String,
    },
    SubagentStart {
        agent_id: String,
        agent_type: String,
    },
    SubagentStop {
        agent_id: String,
        agent_type: String,
        stop_hook_active: bool,
        last_assistant_message: String,
    },
    SessionEnd {
        reason: String,
    },
}
impl ClaudeLifecycleEvent {
    pub const fn name(&self) -> &'static str {
        match self {
            Self::SessionStart { .. } => "SessionStart",
            Self::UserPromptSubmit { .. } => "UserPromptSubmit",
            Self::Stop { .. } => "Stop",
            Self::PreCompact { .. } => "PreCompact",
            Self::PostCompact { .. } => "PostCompact",
            Self::StopFailure { .. } => "StopFailure",
            Self::SubagentStart { .. } => "SubagentStart",
            Self::SubagentStop { .. } => "SubagentStop",
            Self::SessionEnd { .. } => "SessionEnd",
        }
    }

    pub fn matcher_value(&self) -> &str {
        match self {
            Self::SessionStart { source } => source,
            Self::PreCompact { trigger, .. } | Self::PostCompact { trigger, .. } => trigger,
            Self::StopFailure { error, .. } => error,
            Self::SubagentStart { agent_type, .. } | Self::SubagentStop { agent_type, .. } => {
                agent_type
            }
            Self::SessionEnd { reason } => reason,
            Self::UserPromptSubmit { .. } | Self::Stop { .. } => "",
        }
    }
    pub const fn can_block(&self) -> bool {
        matches!(
            self,
            Self::UserPromptSubmit { .. }
                | Self::Stop { .. }
                | Self::PreCompact { .. }
                | Self::SubagentStop { .. }
        )
    }
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct ClaudeLifecycleInvocation {
    pub session_id: String,

    #[serde(skip)]
    pub root_session_id: String,
    pub turn_id: String,
    pub event_id: String,
    pub model: String,
    pub instruction_revision: Option<u64>,
    #[serde(flatten)]
    pub event: ClaudeLifecycleEvent,
}

#[derive(Clone, Debug, Default, serde::Serialize, serde::Deserialize)]
pub enum ClaudeLifecycleDecision {
    #[default]
    Continue,

    Block(String),

    Stop(String),
}

#[derive(Clone, Debug, Default, serde::Serialize, serde::Deserialize)]
pub struct ClaudeLifecycleOutcome {
    pub decision: ClaudeLifecycleDecision,
    pub additional_context: Vec<String>,
    pub diagnostics: Vec<String>,
}

pub async fn run_lifecycle_hooks(
    hooks: &[std::sync::Arc<dyn ClaudeToolHooks>],
    invocation: &ClaudeLifecycleInvocation,
    policy: Option<&dyn crate::execution::ClaudeExecutionPolicy>,
) -> nanocode_agent_core::Result<ClaudeLifecycleOutcome> {
    use crate::execution::Step;
    use nanocode_agent_core::NanocodeError;
    let serialization_error =
        |error: serde_json::Error| NanocodeError::backend("Claude lifecycle receipt", error);
    let mut combined = ClaudeLifecycleOutcome::default();
    for (index, hook) in hooks.iter().enumerate() {
        if !hook.handles_lifecycle(&invocation.event) {
            continue;
        }
        let step_id = format!("lifecycle-{}-{index}", invocation.event_id);
        let admitted = match policy {
            Some(policy) => {
                policy
                    .begin_step(
                        invocation.turn_id.clone(),
                        step_id.clone(),
                        "claude_lifecycle_hook".to_owned(),
                        serde_json::to_value(invocation).map_err(serialization_error)?,
                    )
                    .await?
            }
            None => Step::Execute,
        };
        let outcome: ClaudeLifecycleOutcome = match admitted {
            Step::Interrupted => {
                return Err(nanocode_agent_core::NanocodeError::InvalidExecutionPolicy(
                    "lifecycle step was admitted with an interrupted tool outcome".into(),
                ));
            }
            Step::Replay(value) => serde_json::from_value(value).map_err(serialization_error)?,
            Step::Execute => {
                let uncertain = if let Some(policy) = policy {
                    let intent_id = format!("{step_id}-started");
                    match policy
                        .begin_step(
                            invocation.turn_id.clone(),
                            intent_id.clone(),
                            "claude_lifecycle_intent".into(),
                            serde_json::to_value(invocation).map_err(serialization_error)?,
                        )
                        .await?
                    {
                        Step::Replay(_) | Step::Interrupted => true,
                        Step::Execute => {
                            policy
                                .complete_step(
                                    invocation.turn_id.clone(),
                                    intent_id,
                                    serde_json::json!({"started":true}),
                                )
                                .await?;
                            false
                        }
                    }
                } else {
                    false
                };
                let result = if uncertain {
                    Err(format!(
                        "{} hook outcome unknown after interruption; external effects may have occurred. Reconcile event {} before retrying; hook was not repeated.",
                        invocation.event.name(),
                        invocation.event_id
                    ))
                } else {
                    hook.lifecycle(invocation).await
                };
                let outcome = match result {
                    Ok(outcome) => outcome,
                    Err(error) => {
                        let mut outcome = ClaudeLifecycleOutcome::default();
                        if matches!(
                            invocation.event,
                            ClaudeLifecycleEvent::UserPromptSubmit { .. }
                                | ClaudeLifecycleEvent::PreCompact { .. }
                        ) {
                            outcome.decision = ClaudeLifecycleDecision::Block(error);
                        } else {
                            outcome.diagnostics.push(error);
                        }
                        outcome
                    }
                };
                if let Some(policy) = policy {
                    policy
                        .complete_step(
                            invocation.turn_id.clone(),
                            step_id,
                            serde_json::to_value(&outcome).map_err(serialization_error)?,
                        )
                        .await?;
                }
                outcome
            }
        };
        combined
            .additional_context
            .extend(outcome.additional_context);
        combined.diagnostics.extend(outcome.diagnostics);
        if !matches!(outcome.decision, ClaudeLifecycleDecision::Continue) {
            if invocation.event.can_block() {
                combined.decision = outcome.decision;
                break;
            }
            combined.diagnostics.push(format!(
                "{} is observational; lifecycle decision ignored",
                invocation.event.name()
            ));
        }
    }
    Ok(combined)
}
