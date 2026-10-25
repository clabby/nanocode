use super::{AgentStatus, SubagentControl, completion_continuation_prompt};
use crate::model::AgentId;
use std::time::Duration;

/// A root's subagents that were pending, running or closing when
/// [`SubagentControl::running`] looked, for a host that drives the root
/// itself (`without_subagent_continuation`) to wait for the next one to end.
#[derive(Clone)]
pub struct RunningSubagents {
    control: SubagentControl,
    root_session_id: String,
    ids: Vec<AgentId>,
}

/// A subagent that ended, with the prompt that continues its root with its
/// result ([`completion_continuation_prompt`]).
#[derive(Clone, Debug)]
pub struct SubagentCompletion {
    pub agent_id: AgentId,
    pub status: AgentStatus,
    pub prompt: String,
}

impl SubagentControl {
    /// The root's subagents still pending, running or closing; `None` when it
    /// has none or is not a root with subagents.
    pub async fn running(&self, root_session_id: &str) -> Option<RunningSubagents> {
        let ids: Vec<_> = self
            .agents(root_session_id)
            .await
            .ok()?
            .into_iter()
            .filter(|entry| entry.status.is_active())
            .map(|entry| entry.agent_id)
            .collect();
        (!ids.is_empty()).then(|| RunningSubagents {
            control: self.clone(),
            root_session_id: root_session_id.to_owned(),
            ids,
        })
    }

    /// Interrupt every subagent of a root, as [`SubagentControl::cancel_all`]:
    /// `None` when it started none, else whether they stopped within
    /// `timeout`.
    pub async fn stop_all(&self, root_session_id: &str, timeout: Duration) -> Option<bool> {
        if !self
            .agents(root_session_id)
            .await
            .is_ok_and(|agents| !agents.is_empty())
        {
            return None;
        }
        Some(
            nanocode_platform::timeout(timeout, self.cancel_all(root_session_id))
                .await
                .is_ok(),
        )
    }
}

impl RunningSubagents {
    pub fn ids(&self) -> &[AgentId] {
        &self.ids
    }

    /// Wait up to `timeout` for the first of them to complete, fail or be
    /// interrupted, woken by the registry's changes rather than polling.
    /// `None` once every one is closed or idle without ending that way, when
    /// the root's directory cannot be read, or on timeout.
    pub async fn next_completion(&self, timeout: Duration) -> Option<SubagentCompletion> {
        let mut changes = self.control.registry.revision.subscribe();
        let next = async {
            loop {
                let agents = self.control.agents(&self.root_session_id).await.ok()?;
                let mut active = false;
                for entry in agents
                    .into_iter()
                    .filter(|entry| self.ids.contains(&entry.agent_id))
                {
                    match entry.status {
                        status if status.is_active() => active = true,
                        AgentStatus::Closed => {}
                        status => {
                            return Some(SubagentCompletion {
                                agent_id: entry.agent_id,
                                prompt: completion_continuation_prompt(entry.agent_id, &status),
                                status,
                            });
                        }
                    }
                }
                if !active {
                    return None;
                }
                changes.changed().await.ok()?;
            }
        };
        nanocode_platform::timeout(timeout, next)
            .await
            .ok()
            .flatten()
    }
}
