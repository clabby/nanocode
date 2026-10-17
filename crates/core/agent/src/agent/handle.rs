use super::backend::{
    BackendPrompt, BackendPromptRoute, BackendTurn, BackendTurnKey, LifecycleBackend,
};
use super::*;

pub struct Nanocodex {
    pub(super) backend: Arc<dyn LifecycleBackend>,
    pub(super) events: nanocode_types::events::AgentEventPublisher,
    pub(super) next_turn: Arc<AtomicU64>,
    pub(super) agent_id: Arc<str>,
    pub(super) session: Arc<SessionInfo>,

    pub(super) driver_turn_ids: bool,

    pub(super) seed: Arc<std::sync::Mutex<SeedState>>,
}

#[derive(Default)]
pub(super) struct SeedState {
    transcript: Option<Vec<nanocode_types::PromptMessage>>,
    in_flight: bool,
}

struct SeedClaim {
    seed: Arc<std::sync::Mutex<SeedState>>,
    completed: bool,
}

impl Drop for SeedClaim {
    fn drop(&mut self) {
        release_seed(&self.seed, self.completed);
    }
}

fn release_seed(seed: &std::sync::Mutex<SeedState>, completed: bool) {
    let mut state = seed.lock().expect("transcript seed lock poisoned");
    state.in_flight = false;
    if completed {
        state.transcript = None;
    }
}

impl Clone for Nanocodex {
    fn clone(&self) -> Self {
        Self {
            backend: Arc::clone(&self.backend),
            events: self.events.clone(),
            next_turn: Arc::clone(&self.next_turn),
            agent_id: Arc::clone(&self.agent_id),
            session: Arc::clone(&self.session),
            driver_turn_ids: self.driver_turn_ids,
            seed: Arc::clone(&self.seed),
        }
    }
}

#[derive(Clone)]
pub struct WeakAgent {
    backend: std::sync::Weak<dyn LifecycleBackend>,
}

impl WeakAgent {
    #[must_use]
    pub fn is_live(&self) -> bool {
        self.backend.strong_count() > 0
    }

    pub async fn append_developer_message(
        &self,
        text: impl Into<String>,
    ) -> Option<Result<AgentSessionContext>> {
        let backend = self.backend.upgrade()?;
        let text = text.into();
        if text.trim().is_empty() {
            return Some(Err(NanocodexError::InvalidRequest(
                "developer message must not be empty".to_owned(),
            )));
        }
        Some(backend.append_developer_message(text).await)
    }

    pub async fn append_developer_notice(
        &self,
        text: impl Into<String>,
        prefix: Option<String>,
    ) -> Option<Result<bool>> {
        let backend = self.backend.upgrade()?;
        let text = text.into();
        if text.trim().is_empty() {
            return Some(Err(NanocodexError::InvalidRequest(
                "developer message must not be empty".to_owned(),
            )));
        }
        Some(backend.append_developer_notice(text, prefix).await)
    }

    pub async fn shutdown(&self) -> Option<Result<()>> {
        let backend = self.backend.upgrade()?;
        Some(backend.shutdown().await)
    }
}

impl fmt::Debug for WeakAgent {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("WeakAgent")
            .field("live", &self.is_live())
            .finish()
    }
}

#[derive(Clone)]
pub struct AgentHandle {
    pub(super) session_id: Arc<str>,
    root_session_id: Arc<str>,
    pub(super) model: crate::ModelSelection,
    native_model_id: Arc<str>,
    pub(super) native: Arc<dyn super::backend::AgentFactory>,
    pub(super) factory: Option<Arc<dyn super::backend::AgentFactory>>,
    child_journal: Option<super::backend::ChildJournal>,
}

impl AgentHandle {
    pub fn new(
        session_id: impl Into<Arc<str>>,
        model: crate::ModelSelection,
        native: Arc<dyn super::backend::AgentFactory>,
    ) -> Self {
        let session_id: Arc<str> = session_id.into();
        Self {
            root_session_id: Arc::clone(&session_id),
            session_id,
            model: model.clone(),
            native_model_id: Arc::from(model.as_str()),
            native,
            factory: None,
            child_journal: None,
        }
    }

    #[must_use]
    pub fn with_child_journal(mut self, journal: Option<super::backend::ChildJournal>) -> Self {
        self.child_journal = journal;
        self
    }

    pub fn child_journal(&self) -> Option<&super::backend::ChildJournal> {
        self.child_journal
            .as_ref()
            .filter(|journal| journal.claim(&self.session_id))
    }

    #[must_use]
    pub fn with_spawn_factory(mut self, factory: Arc<dyn super::backend::AgentFactory>) -> Self {
        self.factory = Some(factory);
        self
    }

    pub fn session_id(&self) -> &str {
        &self.session_id
    }

    #[must_use]
    pub fn with_root_session_id(mut self, root_session_id: impl Into<Arc<str>>) -> Self {
        self.root_session_id = root_session_id.into();
        self
    }

    pub fn root_session_id(&self) -> &str {
        &self.root_session_id
    }

    #[must_use]
    pub fn session_environment(&self) -> nanocode_home::SessionEnvironment {
        nanocode_home::SessionEnvironment::new(&self.session_id, &self.root_session_id)
    }

    #[must_use]
    pub fn with_native_model_id(mut self, model: impl Into<Arc<str>>) -> Self {
        self.native_model_id = model.into();
        self.model = crate::ModelSelection::new(self.model.family(), self.native_model_id.as_ref());
        self
    }

    pub fn native_model_id(&self) -> &str {
        &self.native_model_id
    }

    pub fn catalog_model(&self) -> Option<crate::ModelSelection> {
        (self.model.as_codex().is_some() || self.model.as_claude().is_some())
            .then(|| self.model.clone())
    }

    pub fn harness_model(&self) -> crate::ModelSelection {
        self.model.clone()
    }

    pub async fn settings(&self) -> Result<(crate::ModelSelection, Thinking)> {
        self.native.settings(self.clone()).await
    }

    pub async fn reasoning_mode(&self) -> Result<crate::ReasoningMode> {
        self.native.reasoning_mode(self.clone()).await
    }

    pub fn harness_family(&self) -> crate::HarnessFamily {
        self.model.family()
    }

    pub async fn ensure_available(&self) -> Result<()> {
        self.native.ensure_available(self.clone()).await
    }

    pub async fn spawn(&self) -> Result<(Nanocodex, AgentEvents)> {
        self.spawn_with(SpawnOptions::new()).await
    }

    pub async fn spawn_with(&self, options: SpawnOptions) -> Result<(Nanocodex, AgentEvents)> {
        self.spawn_with_host_context(options, None).await
    }

    pub async fn spawn_with_host_context(
        &self,
        options: SpawnOptions,
        host_context: Option<Arc<str>>,
    ) -> Result<(Nanocodex, AgentEvents)> {
        options.validate_harness()?;
        self.native.ensure_available(self.clone()).await?;
        created(
            self.factory
                .as_ref()
                .unwrap_or(&self.native)
                .spawn(self.clone(), options, host_context)
                .await,
        )
        .await
    }

    pub async fn spawn_native_with_host_context(
        &self,
        options: SpawnOptions,
        host_context: Option<Arc<str>>,
    ) -> Result<(Nanocodex, AgentEvents)> {
        options.validate_harness()?;
        created(self.native.spawn(self.clone(), options, host_context).await).await
    }

    pub async fn restore_runtime(
        &self,
        checkpoint: SessionCheckpoint,
        host_context: Option<Arc<str>>,
    ) -> Result<(Nanocodex, AgentEvents)> {
        self.native.ensure_available(self.clone()).await?;
        let restored = if let Some(factory) = &self.factory {
            factory
                .restore(self.clone(), checkpoint, host_context)
                .await
        } else if checkpoint.family() == self.harness_family() {
            self.native
                .restore(self.clone(), checkpoint, host_context)
                .await
        } else {
            return Err(NanocodexError::UnsupportedCapability {
                capability: "cross_family_restore",
            });
        };
        created(restored).await
    }

    pub async fn restore_native_runtime(
        &self,
        checkpoint: SessionCheckpoint,
        host_context: Option<Arc<str>>,
    ) -> Result<(Nanocodex, AgentEvents)> {
        created(
            self.native
                .restore(self.clone(), checkpoint, host_context)
                .await,
        )
        .await
    }

    pub async fn spawn_many(&self, count: usize) -> Result<Vec<(Nanocodex, AgentEvents)>> {
        self.spawn_many_observed_with_host_context(count, |_| {}, None)
            .await
    }

    pub async fn spawn_many_observed(
        &self,
        count: usize,
        observer: impl Fn(&str) + Send + Sync + 'static,
    ) -> Result<Vec<(Nanocodex, AgentEvents)>> {
        self.spawn_many_observed_with_host_context(count, observer, None)
            .await
    }

    pub async fn spawn_many_observed_with_host_context(
        &self,
        count: usize,
        observer: impl Fn(&str) + Send + Sync + 'static,
        host_context: Option<Arc<str>>,
    ) -> Result<Vec<(Nanocodex, AgentEvents)>> {
        self.native.ensure_available(self.clone()).await?;

        created_batch(
            self.factory
                .as_ref()
                .unwrap_or(&self.native)
                .spawn_many(self.clone(), count, Arc::new(observer), host_context)
                .await,
        )
        .await
    }

    pub async fn spawn_many_native_with_host_context(
        &self,
        count: usize,
        observer: impl Fn(&str) + Send + Sync + 'static,
        host_context: Option<Arc<str>>,
    ) -> Result<Vec<(Nanocodex, AgentEvents)>> {
        self.native.ensure_available(self.clone()).await?;
        created_batch(
            self.native
                .spawn_many(self.clone(), count, Arc::new(observer), host_context)
                .await,
        )
        .await
    }

    pub async fn fork(&self, request: ForkRequest) -> Result<(Nanocodex, AgentEvents)> {
        self.native.ensure_available(self.clone()).await?;
        created(self.native.fork(self.clone(), request).await).await
    }
}

pub async fn created(child: Result<(Nanocodex, AgentEvents)>) -> Result<(Nanocodex, AgentEvents)> {
    let child = child?;
    if let Err(error) = child.0.backend.persist_initial().await {
        child.0.abandon_created().await;
        return Err(error);
    }
    Ok(child)
}

pub async fn created_batch(
    children: Result<Vec<(Nanocodex, AgentEvents)>>,
) -> Result<Vec<(Nanocodex, AgentEvents)>> {
    let children = children?;
    let mut failure = None;
    for (child, _) in &children {
        if let Err(error) = child.backend.persist_initial().await {
            failure.get_or_insert(error);
        }
    }
    if let Some(error) = failure {
        for (child, _) in &children {
            child.abandon_created().await;
        }
        return Err(error);
    }
    Ok(children)
}

impl Nanocodex {
    #[must_use]
    pub fn builder<B>(backend: B) -> B::Builder
    where
        B: BuilderBackend,
    {
        backend.into_builder()
    }

    #[must_use]
    pub fn harness_family(&self) -> crate::HarnessFamily {
        self.session.family.clone()
    }

    #[must_use]
    pub fn downgrade(&self) -> WeakAgent {
        WeakAgent {
            backend: Arc::downgrade(&self.backend),
        }
    }

    #[must_use]
    pub fn session(&self) -> &SessionInfo {
        &self.session
    }

    #[must_use]
    pub fn capabilities(&self) -> Capabilities {
        self.backend.capabilities()
    }

    #[must_use]
    pub fn persistence(&self) -> Option<Persistence> {
        self.backend.persistence()
    }

    pub async fn set_harness_model(&self, model: crate::ModelSelection) -> Result<()> {
        if model.family() != self.harness_family() {
            return Err(NanocodexError::InvalidRequest(
                "model belongs to another harness family".into(),
            ));
        }
        self.backend.set_harness_model(model).await
    }

    #[must_use]
    pub fn agent_id(&self) -> &str {
        &self.agent_id
    }

    #[must_use]
    pub fn session_id(&self) -> &str {
        &self.session.session_id
    }

    pub async fn flush(&self) -> Result<()> {
        self.backend.flush().await
    }

    pub async fn disconnect(&self) -> Result<()> {
        self.backend.disconnect().await
    }

    pub async fn shutdown(&self) -> Result<()> {
        self.backend.shutdown().await
    }

    pub fn decode_durable_prompt(&self, input: serde_json::Value) -> Result<Prompt> {
        self.backend.decode_durable_prompt(input)
    }

    pub async fn prompt(&self, request: impl Into<PromptRequest>) -> Result<Turn> {
        let PromptRequest {
            prompt,
            request_id,
            cancel_on_admission,
        } = request.into();
        prompt
            .validate()
            .map_err(|error| NanocodexError::InvalidRequest(error.to_string()))?;
        if request_id
            .as_deref()
            .is_some_and(|request_id| request_id.trim().is_empty())
        {
            return Err(NanocodexError::InvalidRequest(
                "request ID must not be empty".to_owned(),
            ));
        }
        let key = BackendTurnKey(self.next_turn.fetch_add(1, Ordering::Relaxed));
        let (events, event_stream) = self.events.mirrored_channel();
        let turn_id = nanocode_platform::uuid_v7().to_string();
        let events = events.with_turn_id(turn_id.clone());
        let (prompt, claimed) = self.claim_seed(prompt).await?;
        let submitted = self
            .backend
            .submit(BackendPrompt {
                key,
                prompt,
                request_id,
                cancel_on_admission,
                events,
            })
            .await;
        let BackendTurn { request_id, result } = submitted?;
        let result = self.settle_seed(claimed, result);
        let turn_id = self.canonical_turn_id(turn_id, request_id.as_deref());
        Ok(Turn {
            result: Self::turn_result(&turn_id, result),
            turn_id,
            control: TurnControl {
                key,
                backend: Arc::clone(&self.backend),
            },
            request_id,
            events: event_stream,
        })
    }

    fn turn_result(
        turn_id: &str,
        result: super::backend::BackendFuture<Result<TurnResult>>,
    ) -> super::backend::BackendFuture<Result<TurnResult>> {
        let turn_id = turn_id.to_owned();
        Box::pin(async move { result.await.map(|result| result.with_turn_id(turn_id)) })
    }

    fn canonical_turn_id(&self, generated: String, request_id: Option<&str>) -> String {
        if self.driver_turn_ids {
            return generated;
        }
        request_id.map(str::to_owned).unwrap_or(generated)
    }

    pub async fn route_prompt(&self, prompt: impl Into<Prompt>) -> Result<PromptRoute> {
        let prompt = prompt.into();
        self.route_prompt_request(PromptRequest::new(prompt)).await
    }

    pub async fn route_prompt_request(
        &self,
        request: impl Into<PromptRequest>,
    ) -> Result<PromptRoute> {
        let PromptRequest {
            prompt,
            request_id,
            cancel_on_admission,
        } = request.into();
        prompt
            .validate()
            .map_err(|error| NanocodexError::InvalidRequest(error.to_string()))?;
        if request_id
            .as_deref()
            .is_some_and(|request_id| request_id.trim().is_empty())
        {
            return Err(NanocodexError::InvalidRequest(
                "request ID must not be empty".to_owned(),
            ));
        }
        let key = BackendTurnKey(self.next_turn.fetch_add(1, Ordering::Relaxed));
        let (events, event_stream) = self.events.mirrored_channel();
        let turn_id = nanocode_platform::uuid_v7().to_string();
        let events = events.with_turn_id(turn_id.clone());
        let (prompt, claimed) = self.claim_seed(prompt).await?;
        let routed = self
            .backend
            .route(BackendPrompt {
                key,
                prompt,
                request_id,
                cancel_on_admission,
                events,
            })
            .await;
        match routed {
            Ok(BackendPromptRoute::Started(BackendTurn { request_id, result })) => {
                let result = self.settle_seed(claimed, result);
                let turn_id = self.canonical_turn_id(turn_id, request_id.as_deref());
                Ok(PromptRoute::Started(Turn {
                    result: Self::turn_result(&turn_id, result),
                    turn_id,
                    control: TurnControl {
                        key,
                        backend: Arc::clone(&self.backend),
                    },
                    request_id,
                    events: event_stream,
                }))
            }
            Ok(BackendPromptRoute::Steered) => Ok(PromptRoute::Steered),
            Err(error) => Err(error),
        }
    }

    pub async fn cancel_active_turn(&self) -> Result<()> {
        self.backend.cancel_active().await
    }

    pub async fn set_thinking(&self, thinking: Thinking) -> Result<()> {
        self.backend.set_thinking(thinking).await
    }

    pub async fn set_fast_mode(&self, enabled: bool) -> Result<()> {
        self.set_service_tier(ServiceTier::from_fast_mode(enabled))
            .await
    }

    pub async fn set_service_tier(&self, service_tier: ServiceTier) -> Result<()> {
        self.backend.set_service_tier(service_tier).await
    }

    pub async fn compact(&self) -> Result<()> {
        self.backend.compact().await
    }

    pub async fn append_developer_message(
        &self,
        text: impl Into<String>,
    ) -> Result<AgentSessionContext> {
        let text = text.into();
        if text.trim().is_empty() {
            return Err(NanocodexError::InvalidRequest(
                "developer message must not be empty".to_owned(),
            ));
        }
        self.backend.append_developer_message(text).await
    }

    pub async fn context(&self) -> Result<AgentSessionContext> {
        self.backend.context().await
    }

    pub async fn transcript(&self) -> Result<Vec<nanocode_types::PromptMessage>> {
        let seed = self.pending_seed();
        let transcript = self.backend.transcript().await?;
        Ok(match seed {
            Some(seed) if transcript.is_empty() => seed,
            _ => transcript,
        })
    }

    pub fn with_transcript(self, transcript: Vec<nanocode_types::PromptMessage>) -> Result<Self> {
        Prompt::new("transcript")
            .with_transcript(transcript.clone())
            .validate()
            .map_err(|error| NanocodexError::InvalidRequest(error.to_string()))?;
        *self.seed.lock().expect("transcript seed lock poisoned") = SeedState {
            transcript: (!transcript.is_empty()).then_some(transcript),
            in_flight: false,
        };
        Ok(self)
    }

    fn pending_seed(&self) -> Option<Vec<nanocode_types::PromptMessage>> {
        self.seed
            .lock()
            .expect("transcript seed lock poisoned")
            .transcript
            .clone()
    }

    async fn claim_seed(&self, prompt: Prompt) -> Result<(Prompt, Option<SeedClaim>)> {
        let pending = {
            let state = self.seed.lock().expect("transcript seed lock poisoned");
            if state.in_flight {
                None
            } else {
                state.transcript.clone()
            }
        };
        let Some(seed) = pending else {
            return Ok((prompt, None));
        };
        let committed = match self.backend.transcript().await {
            Ok(transcript) => !transcript.is_empty(),
            Err(NanocodexError::UnsupportedCapability { .. }) => false,
            Err(error) => return Err(error),
        };
        let mut state = self.seed.lock().expect("transcript seed lock poisoned");
        if committed {
            state.transcript = None;
            return Ok((prompt, None));
        }
        if state.in_flight || state.transcript.is_none() {
            return Ok((prompt, None));
        }
        state.in_flight = true;
        drop(state);
        let transcript = seed
            .into_iter()
            .chain(prompt.transcript().iter().cloned())
            .collect::<Vec<_>>();
        Ok((
            prompt.with_transcript(transcript),
            Some(SeedClaim {
                seed: Arc::clone(&self.seed),
                completed: false,
            }),
        ))
    }

    fn settle_seed(
        &self,
        claimed: Option<SeedClaim>,
        result: super::backend::BackendFuture<Result<TurnResult>>,
    ) -> super::backend::BackendFuture<Result<TurnResult>> {
        let Some(mut claim) = claimed else {
            return result;
        };
        Box::pin(async move {
            let outcome = result.await;
            claim.completed = outcome.is_ok();
            drop(claim);
            outcome
        })
    }

    pub async fn side_question(&self, question: impl Into<String>) -> Result<SideAnswer> {
        let question = question.into();
        if question.trim().is_empty() {
            return Err(NanocodexError::InvalidRequest(
                "side question must not be empty".to_owned(),
            ));
        }
        self.backend.side_question(question).await
    }

    pub async fn checkpoint(&self) -> Result<SessionCheckpoint> {
        self.backend.checkpoint().await
    }

    pub async fn spawn(&self) -> Result<(Self, AgentEvents)> {
        self.spawn_with(SpawnOptions::new()).await
    }

    pub async fn spawn_with(&self, options: SpawnOptions) -> Result<(Self, AgentEvents)> {
        self.backend.spawn(options).await
    }

    pub async fn fork(&self, request: ForkRequest) -> Result<(Self, AgentEvents)> {
        created(self.backend.fork(request).await).await
    }

    #[doc(hidden)]
    pub async fn persist_created(
        child: Result<(Self, AgentEvents)>,
    ) -> Result<(Self, AgentEvents)> {
        created(child).await
    }

    #[doc(hidden)]
    pub async fn abandon_created(&self) {
        let _ = self.backend.persist_initial().await;
        let _ = self.backend.discard_initial().await;
        let _ = self.shutdown().await;
    }
}
