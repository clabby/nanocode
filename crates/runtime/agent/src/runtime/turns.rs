use std::fmt;

use super::{AgentRuntime, catalog_error};
use crate::{
    Agent, ExecutionPolicyDisposition, HarnessFamily, NanocodeError, PromptRequest,
    durability::TurnStatus, input::Prompt,
};
use nanocode_subagents::COMPLETION_REQUEST_PREFIX;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DurableOutcome {
    Completed(String),
    Failed(String),
    Cancelled,
    Waiting,
    Interrupted,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PromptRejection {
    ReservedRequestId(String),
    HarnessChanged {
        recorded: HarnessFamily,
        requested: HarnessFamily,
    },
}

impl fmt::Display for PromptRejection {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ReservedRequestId(request_id) => write!(
                formatter,
                "request ID `{request_id}` uses the prefix `{COMPLETION_REQUEST_PREFIX}` reserved for subagent completion turns"
            ),
            Self::HarnessChanged {
                recorded,
                requested,
            } => write!(
                formatter,
                "the session runs on the {recorded} harness and cannot continue on {requested}; start a new session"
            ),
        }
    }
}

impl std::error::Error for PromptRejection {}

pub fn admit_prompt(
    recorded: Option<&HarnessFamily>,
    requested: &HarnessFamily,
    request_id: &str,
) -> Result<(), PromptRejection> {
    if request_id.starts_with(COMPLETION_REQUEST_PREFIX) {
        return Err(PromptRejection::ReservedRequestId(request_id.into()));
    }
    match recorded {
        Some(recorded) if recorded != requested => Err(PromptRejection::HarnessChanged {
            recorded: recorded.clone(),
            requested: requested.clone(),
        }),
        _ => Ok(()),
    }
}

impl AgentRuntime {
    fn durable_store(&self) -> Result<&crate::durability::SessionStore, NanocodeError> {
        self.inner.durability.as_ref().ok_or_else(|| {
            NanocodeError::InvalidRequest("runtime durability is not configured".into())
        })
    }

    pub async fn durable_outcome(
        &self,
        session_id: &str,
        request_id: &str,
        result: Result<String, NanocodeError>,
    ) -> DurableOutcome {
        let error = match result {
            Ok(message) => return DurableOutcome::Completed(message),
            Err(error) => error,
        };
        let blocked =
            error.execution_policy_disposition() == Some(ExecutionPolicyDisposition::Retry);
        let Some(store) = self.inner.durability.as_ref() else {
            return if blocked {
                DurableOutcome::Waiting
            } else {
                DurableOutcome::Failed(error.to_string())
            };
        };
        match store
            .turn(session_id, request_id)
            .await
            .map(|turn| turn.map(|turn| turn.status))
        {
            Ok(Some(TurnStatus::Completed)) => self
                .recorded(session_id, request_id)
                .await
                .map_or(DurableOutcome::Interrupted, DurableOutcome::Completed),
            Ok(Some(TurnStatus::Failed)) => DurableOutcome::Failed(error.to_string()),
            Ok(Some(TurnStatus::Cancelled)) => DurableOutcome::Cancelled,
            Ok(Some(TurnStatus::Pending) | None) if blocked => DurableOutcome::Waiting,
            Ok(None) if matches!(error, NanocodeError::InvalidRequest(_)) => {
                DurableOutcome::Failed(error.to_string())
            }
            _ => DurableOutcome::Interrupted,
        }
    }

    async fn recorded(&self, session_id: &str, request_id: &str) -> Result<String, NanocodeError> {
        let (agent, _events) = self.open_session(session_id).await?;
        let prompt = self
            .durable_prompt(&agent, request_id)
            .await?
            .ok_or(NanocodeError::ReplayedCheckpointUnavailable)?;
        let turn = agent
            .prompt(PromptRequest::new(prompt).request_id(request_id))
            .await?;
        Ok(turn.result().await?.final_message().to_owned())
    }

    pub async fn pending_turns(&self, session_id: &str) -> Result<Vec<String>, NanocodeError> {
        Ok(self
            .durable_store()?
            .pending_turns(session_id)
            .await
            .map_err(catalog_error)?
            .into_iter()
            .map(|turn| turn.id)
            .collect())
    }

    pub async fn durable_prompt(
        &self,
        agent: &Agent,
        request_id: &str,
    ) -> Result<Option<Prompt>, NanocodeError> {
        match self
            .durable_store()?
            .turn(agent.session_id(), request_id)
            .await
            .map_err(catalog_error)?
        {
            Some(turn) => agent.decode_durable_prompt(turn.input).map(Some),
            None => Ok(None),
        }
    }

    pub async fn settle(&self, session_id: &str) -> Result<bool, NanocodeError> {
        let Some(control) = self.subagents() else {
            return Ok(self.pending_turns(session_id).await?.is_empty());
        };
        let before = control.activity(session_id).await;
        if !before.is_quiescent() || !self.pending_turns(session_id).await?.is_empty() {
            return Ok(false);
        }
        control
            .persist(session_id)
            .await
            .map_err(|error| NanocodeError::Backend {
                backend: "subagents",
                source: std::sync::Arc::new(error),
            })?;
        let after = control.activity(session_id).await;
        Ok(after.revision == before.revision
            && after.is_quiescent()
            && self.pending_turns(session_id).await?.is_empty())
    }
}
