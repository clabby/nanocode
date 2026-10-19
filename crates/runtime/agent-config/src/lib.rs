pub use nanocode_agent_core::{
    Agent, AgentEvents, ModelSelection, SessionCheckpoint, SpawnOptions, Thinking,
};
use nanocode_agent_core::{AgentHandle, backend::AgentFactory};
use std::sync::Arc;
pub mod configuration;
pub struct HarnessRequest {
    pub model: ModelSelection,

    pub thinking: Thinking,

    pub options: SpawnOptions,

    pub parent: Option<AgentHandle>,

    pub host_context: Option<Arc<str>>,

    pub checkpoint: Option<SessionCheckpoint>,

    pub durable_state: Option<nanocode_durability::DurableSession>,

    pub spawn_factory: Arc<dyn AgentFactory>,
}
