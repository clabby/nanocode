use super::{AgentRuntime, catalog_error};
use crate::{
    Agent, ExecutionPolicyDisposition, NanocodeError, durability::TurnStatus, input::Prompt,
};

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DurableOutcome {
    Completed(String),
    Failed(String),
    Cancelled,
    Waiting,
    Interrupted,
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
            Ok(Some(TurnStatus::Failed)) => DurableOutcome::Failed(error.to_string()),
            Ok(Some(TurnStatus::Cancelled)) => DurableOutcome::Cancelled,
            Ok(Some(TurnStatus::Pending) | None) if blocked => DurableOutcome::Waiting,
            Ok(None) if matches!(error, NanocodeError::InvalidRequest(_)) => {
                DurableOutcome::Failed(error.to_string())
            }
            _ => DurableOutcome::Interrupted,
        }
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
