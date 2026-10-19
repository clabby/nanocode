use super::backend::{
    BackendFuture, BackendPrompt, BackendPromptRoute, BackendTurn, BackendTurnKey, LifecycleBackend,
};
use super::*;
use std::sync::atomic::AtomicBool;

#[doc(hidden)]
pub trait AgentOwner: Send + Sync + 'static {
    fn admit(&self) -> Result<()>;

    fn finish(&self);

    fn adopt(&self, key: usize, agent: Agent) -> bool;

    fn release(&self, key: usize);
}

#[doc(hidden)]
pub async fn adopt(agent: Agent, owner: &Arc<dyn AgentOwner>) -> Result<Agent> {
    let key = Arc::as_ptr(&agent.backend).cast::<()>() as usize;
    let backend: Arc<dyn LifecycleBackend> = Arc::new(Owned {
        inner: Arc::clone(&agent.backend),
        owner: Arc::downgrade(owner),
        key,
        stopped: Arc::default(),
    });
    let owned = Agent {
        backend,
        events: agent.events.clone(),
        next_turn: Arc::clone(&agent.next_turn),
        agent_id: Arc::clone(&agent.agent_id),
        session: Arc::clone(&agent.session),
        driver_turn_ids: agent.driver_turn_ids,
        seed: Arc::clone(&agent.seed),
        interrupted: Arc::clone(&agent.interrupted),
    };
    if owner.adopt(key, owned.clone()) {
        return Ok(owned);
    }

    agent.backend.shutdown().await?;
    Err(NanocodeError::AgentStopped)
}

struct Admitted(Arc<dyn AgentOwner>);

impl Drop for Admitted {
    fn drop(&mut self) {
        self.0.finish();
    }
}

async fn adopt_created(
    owner: std::sync::Weak<dyn AgentOwner>,
    create: impl FnOnce() -> BackendFuture<Result<(Agent, AgentEvents)>>,
) -> Result<(Agent, AgentEvents)> {
    let Some(owner) = owner.upgrade() else {
        return create().await;
    };
    owner.admit()?;
    let admitted = Admitted(Arc::clone(&owner));
    let (agent, events) = create().await?;
    let agent = adopt(agent, &owner).await?;
    drop(admitted);
    Ok((agent, events))
}

struct Owned {
    inner: Arc<dyn LifecycleBackend>,
    owner: std::sync::Weak<dyn AgentOwner>,
    key: usize,
    stopped: Arc<AtomicBool>,
}

fn requires_reopen(error: &NanocodeError) -> bool {
    matches!(
        error,
        NanocodeError::AgentStopped
            | NanocodeError::TurnStopped
            | NanocodeError::ExecutionPolicyOwnerStopped
    ) || error.execution_policy_disposition() == Some(crate::ExecutionPolicyDisposition::Reopen)
}

fn observe<T>(stopped: &AtomicBool, result: Result<T>) -> Result<T> {
    if let Err(error) = &result
        && requires_reopen(error)
    {
        stopped.store(true, Ordering::Release);
    }
    result
}

fn observed_turn(stopped: &Arc<AtomicBool>, turn: BackendTurn) -> BackendTurn {
    let stopped = Arc::clone(stopped);
    let result = turn.result;
    BackendTurn {
        request_id: turn.request_id,
        result: Box::pin(async move { observe(&stopped, result.await) }),
    }
}

impl LifecycleBackend for Owned {
    fn decode_durable_prompt(&self, input: serde_json::Value) -> Result<Prompt> {
        self.inner.decode_durable_prompt(input)
    }
    fn harness_family(&self) -> crate::HarnessFamily {
        self.inner.harness_family()
    }
    fn capabilities(&self) -> Capabilities {
        self.inner.capabilities()
    }
    fn persistence(&self) -> Option<Persistence> {
        self.inner.persistence()
    }
    fn submit(&self, prompt: BackendPrompt) -> BackendFuture<Result<BackendTurn>> {
        let submitted = self.inner.submit(prompt);
        let stopped = Arc::clone(&self.stopped);
        Box::pin(async move {
            observe(&stopped, submitted.await).map(|turn| observed_turn(&stopped, turn))
        })
    }
    fn route(&self, prompt: BackendPrompt) -> BackendFuture<Result<BackendPromptRoute>> {
        let routed = self.inner.route(prompt);
        let stopped = Arc::clone(&self.stopped);
        Box::pin(async move {
            observe(&stopped, routed.await).map(|route| match route {
                BackendPromptRoute::Started(turn) => {
                    BackendPromptRoute::Started(observed_turn(&stopped, turn))
                }
                BackendPromptRoute::Steered => BackendPromptRoute::Steered,
            })
        })
    }
    fn is_stopped(&self) -> bool {
        self.stopped.load(Ordering::Acquire) || self.inner.is_stopped()
    }
    fn steer(&self, key: BackendTurnKey, prompt: Prompt) -> BackendFuture<Result<()>> {
        self.inner.steer(key, prompt)
    }
    fn steer_with_id(
        &self,
        key: BackendTurnKey,
        id: String,
        prompt: Prompt,
    ) -> BackendFuture<Result<()>> {
        self.inner.steer_with_id(key, id, prompt)
    }
    fn withdraw_steer(&self, key: BackendTurnKey, id: String) -> BackendFuture<Result<bool>> {
        self.inner.withdraw_steer(key, id)
    }
    fn cancel(&self, key: BackendTurnKey) -> BackendFuture<Result<()>> {
        self.inner.cancel(key)
    }
    fn cancel_active(&self) -> BackendFuture<Result<()>> {
        self.inner.cancel_active()
    }
    fn set_harness_model(&self, model: crate::ModelSelection) -> BackendFuture<Result<()>> {
        self.inner.set_harness_model(model)
    }
    fn set_thinking(&self, thinking: Thinking) -> BackendFuture<Result<()>> {
        self.inner.set_thinking(thinking)
    }
    fn set_fast_mode(&self, enabled: bool) -> BackendFuture<Result<()>> {
        self.inner.set_fast_mode(enabled)
    }
    fn set_service_tier(&self, service_tier: ServiceTier) -> BackendFuture<Result<()>> {
        self.inner.set_service_tier(service_tier)
    }
    fn compact(&self) -> BackendFuture<Result<()>> {
        self.inner.compact()
    }
    fn preconnect(&self) -> BackendFuture<Result<()>> {
        self.inner.preconnect()
    }
    fn append_developer_message(
        &self,
        text: String,
    ) -> BackendFuture<Result<crate::AgentSessionContext>> {
        self.inner.append_developer_message(text)
    }
    fn append_developer_notice(
        &self,
        text: String,
        prefix: Option<String>,
    ) -> BackendFuture<Result<bool>> {
        self.inner.append_developer_notice(text, prefix)
    }
    fn context(&self) -> BackendFuture<Result<crate::AgentSessionContext>> {
        self.inner.context()
    }
    fn side_question(&self, question: String) -> BackendFuture<Result<crate::SideAnswer>> {
        self.inner.side_question(question)
    }
    fn transcript(&self) -> BackendFuture<Result<Vec<nanocode_types::PromptMessage>>> {
        self.inner.transcript()
    }
    fn checkpoint(&self) -> BackendFuture<Result<SessionCheckpoint>> {
        self.inner.checkpoint()
    }
    fn persist_initial(&self) -> BackendFuture<Result<()>> {
        self.inner.persist_initial()
    }
    fn discard_initial(&self) -> BackendFuture<Result<()>> {
        self.inner.discard_initial()
    }
    fn spawn(&self, options: SpawnOptions) -> BackendFuture<Result<(Agent, AgentEvents)>> {
        let inner = Arc::clone(&self.inner);
        Box::pin(adopt_created(self.owner.clone(), move || {
            inner.spawn(options)
        }))
    }
    fn fork(&self, request: ForkRequest) -> BackendFuture<Result<(Agent, AgentEvents)>> {
        let inner = Arc::clone(&self.inner);
        Box::pin(adopt_created(self.owner.clone(), move || {
            inner.fork(request)
        }))
    }
    fn flush(&self) -> BackendFuture<Result<()>> {
        self.inner.flush()
    }
    fn disconnect(&self) -> BackendFuture<Result<()>> {
        let disconnected = self.inner.disconnect();
        let owner = self.owner.clone();
        let key = self.key;
        Box::pin(async move {
            let result = disconnected.await;
            if let Some(owner) = owner.upgrade() {
                owner.release(key);
            }
            result
        })
    }
    fn shutdown(&self) -> BackendFuture<Result<()>> {
        let stopped = self.inner.shutdown();
        let owner = self.owner.clone();
        let key = self.key;
        Box::pin(async move {
            let result = stopped.await;
            if let Some(owner) = owner.upgrade() {
                owner.release(key);
            }
            result
        })
    }
}
