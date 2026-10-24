use std::sync::Arc;

use nanocode_agent_core::{
    Agent, AgentEvents, BackendRegistration, BackendRequest, ExecutionPolicyDisposition,
    NanocodeError, Result as AgentResult, SessionInfo, backend::BackendFuture,
};

use crate::{CheckpointFormat, DurableSession, Error, SessionRecord, SharedStore, StoreError};

pub trait DurableAgentExt: Sized {
    fn durability(self, state: DurableSession) -> impl Future<Output = AgentResult<Self>>;
}

pub trait DurableBackendRegistration: BackendRegistration {
    fn checkpoint_format(&self) -> Option<Arc<dyn CheckpointFormat>> {
        None
    }

    fn build_durable(
        &self,
        _request: BackendRequest,
        _state: DurableSession,
    ) -> BackendFuture<AgentResult<(Agent, AgentEvents)>> {
        Box::pin(async {
            Err(NanocodeError::UnsupportedCapability {
                capability: "durability",
            })
        })
    }
}

#[derive(Clone)]
pub struct Branches {
    pub store: SharedStore,
    pub record: Option<SessionRecord>,
}

impl Branches {
    pub fn child_record(&self, child: &SessionInfo) -> AgentResult<SessionRecord> {
        let parent = self.record.as_ref().ok_or_else(|| {
            NanocodeError::InvalidExecutionPolicy(
                "a durable session without a recorded model cannot persist forks; open it through SessionStore"
                    .to_owned(),
            )
        })?;
        Ok(parent
            .derive(child.session_id.clone(), child.lineage.origin)
            .with_lineage(child.lineage.clone()))
    }
}

#[must_use]
pub fn agent_store_error(error: StoreError) -> NanocodeError {
    agent_error(error.into())
}

pub fn agent_error(error: Error) -> NanocodeError {
    if matches!(
        error,
        Error::SteerConflict { .. } | Error::SteerWithdrawn { .. } | Error::SessionExists { .. }
    ) {
        return NanocodeError::InvalidRequest(error.to_string());
    }
    let disposition = match &error {
        Error::Store(StoreError::NotCommitted(_))
        | Error::OperationBlocked { .. }
        | Error::OperationActive { .. } => ExecutionPolicyDisposition::Retry,
        Error::Store(StoreError::Fenced | StoreError::Conflict { .. } | StoreError::Backend(_))
        | Error::ModelOwnerFenced
        | Error::DriverStopped => ExecutionPolicyDisposition::Reopen,
        _ => ExecutionPolicyDisposition::Fatal,
    };
    NanocodeError::execution_policy_with_disposition("durability", disposition, error)
}
