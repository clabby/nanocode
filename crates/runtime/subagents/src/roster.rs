use super::model::{AgentDescriptor, AgentId, AgentStatus, AgentUpdate};
use nanocode_agent_core::{ModelSelection, ReasoningMode, Thinking};
use serde::Serialize;

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize)]
pub struct SubagentRoster {
    pub max_subagents: usize,
    pub agents: Vec<SubagentNode>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct SubagentNode {
    pub id: AgentId,
    pub parent: Option<AgentId>,
    pub session_id: String,
    pub role: String,
    pub task: String,
    pub model: Option<ModelSelection>,
    pub thinking: Option<Thinking>,
    pub reasoning_mode: ReasoningMode,
    pub status: AgentStatus,
}

impl SubagentRoster {
    pub const fn new(max_subagents: usize) -> Self {
        Self {
            max_subagents,
            agents: Vec::new(),
        }
    }

    pub fn apply(&mut self, update: &AgentUpdate) -> bool {
        match update {
            AgentUpdate::Added(descriptor) => {
                if let Some(node) = self.agent_mut(descriptor.id) {
                    return node.describe(descriptor);
                }
                self.agents.push(SubagentNode::new(descriptor));
                true
            }
            AgentUpdate::Status { id, status } | AgentUpdate::Completion { id, status, .. } => {
                let Some(node) = self.agent_mut(*id) else {
                    return false;
                };
                if node.status == *status || matches!(node.status, AgentStatus::Closed) {
                    return false;
                }
                node.status = status.clone();
                true
            }
            AgentUpdate::Event { .. } | AgentUpdate::Message(_) => false,
        }
    }

    pub fn agent(&self, id: AgentId) -> Option<&SubagentNode> {
        self.agents.iter().find(|node| node.id == id)
    }

    fn agent_mut(&mut self, id: AgentId) -> Option<&mut SubagentNode> {
        self.agents.iter_mut().find(|node| node.id == id)
    }

    pub fn children(&self, parent: Option<AgentId>) -> impl Iterator<Item = &SubagentNode> {
        self.agents.iter().filter(move |node| node.parent == parent)
    }

    pub fn active_count(&self) -> usize {
        self.agents
            .iter()
            .filter(|node| node.status.is_active())
            .count()
    }
}

impl SubagentNode {
    fn new(descriptor: &AgentDescriptor) -> Self {
        Self {
            id: descriptor.id,
            parent: descriptor.parent,
            session_id: descriptor.session_id.clone(),
            role: descriptor.role.clone(),
            task: descriptor.task.clone(),
            model: descriptor.model.clone(),
            thinking: descriptor.thinking,
            reasoning_mode: descriptor.reasoning_mode,
            status: AgentStatus::Running,
        }
    }

    fn describe(&mut self, descriptor: &AgentDescriptor) -> bool {
        let described = Self {
            status: self.status.clone(),
            ..Self::new(descriptor)
        };
        let changed = *self != described;
        *self = described;
        changed
    }
}
