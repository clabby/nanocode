use super::runtime::{Registry, SubagentControl};
use std::{
    fmt,
    sync::{Arc, Weak},
};

#[derive(Clone)]
pub struct RootAgentAuthority {
    registry: Weak<Registry>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AuthorityError {
    RuntimeClosed,
    ChildSession,
}

impl fmt::Display for AuthorityError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::RuntimeClosed => "subagent runtime is closed",
            Self::ChildSession => "operation is only available to root agents",
        })
    }
}

impl std::error::Error for AuthorityError {}

impl RootAgentAuthority {
    pub fn new(registry: &Arc<Registry>) -> Self {
        Self {
            registry: Arc::downgrade(registry),
        }
    }

    pub async fn require_root(&self, session_id: &str) -> Result<(), AuthorityError> {
        let registry = self
            .registry
            .upgrade()
            .ok_or(AuthorityError::RuntimeClosed)?;
        if registry.is_root_session(session_id).await {
            Ok(())
        } else {
            Err(AuthorityError::ChildSession)
        }
    }
}

impl SubagentControl {
    pub fn root_agent_authority(&self) -> RootAgentAuthority {
        RootAgentAuthority::new(&self.registry)
    }

    pub fn spawn_policy(&self) -> crate::SpawnPolicy {
        self.registry.spawn_policy()
    }

    pub fn set_spawn_policy(&self, policy: crate::SpawnPolicy) {
        self.registry.set_spawn_policy(policy);
    }

    pub fn update_spawn_policy(&self, update: impl FnMut(&mut crate::SpawnPolicy)) {
        self.registry.update_spawn_policy(update);
    }

    pub fn registry(&self) -> &Arc<Registry> {
        &self.registry
    }
}
