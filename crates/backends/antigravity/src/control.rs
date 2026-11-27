use crate::driver::{CLOSING, Inner, OPEN, STOPPED, State, Steer, Turn, emit, prompt_text};
use crate::turn::{INTERRUPTED, effort};
use nanocode_agent_core::{
    ExecutionPolicyDisposition, NanocodeError, Result, TurnResult, TurnUsage,
    backend::{BackendPrompt, BackendPromptRoute, BackendTurn},
    events::{AgentEventKind, AgentEventPublisher},
    input::Prompt,
};
use nanocode_backend_antigravity_client::{Content, Part};
use nanocode_backend_antigravity_tools::system_message;
use nanocode_durability::{
    Admission, DurableOwner, Error as DurableError, OperationStatus, agent_error,
};
use nanocode_sync::future::{Either, select};
use nanocode_sync::{Atom, Signal};
use serde::Deserialize;
use serde_json::{Value, json};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};

const TRANSPORT: &str = "gemini_sse";
const ORCHESTRATION: &str = "antigravity";

pub(crate) fn candidate_id(kind: &str) -> String {
    format!("antigravity-{kind}-{}", nanocode_platform::uuid_v4())
}

pub(crate) fn recovery_error(error: impl std::fmt::Display) -> NanocodeError {
    NanocodeError::execution_policy_with_disposition(
        "antigravity recovery",
        ExecutionPolicyDisposition::Reopen,
        NanocodeError::InvalidCheckpoint(error.to_string()),
    )
}

fn reopen(error: NanocodeError) -> NanocodeError {
    match error.execution_policy_disposition() {
        Some(ExecutionPolicyDisposition::Retry) => recovery_error(error),
        _ => error,
    }
}

fn stopped() -> NanocodeError {
    NanocodeError::AgentStopped
}

enum Ended {
    Finished(Result<TurnResult>),
    Cancelled,
    Stopped,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Output {
    final_message: String,
    #[serde(default)]
    usage: Option<TurnUsage>,
}

impl Inner {
    fn labels(&self) -> (String, String) {
        let profile = self.profile.get();
        (profile.model.clone(), effort(&profile))
    }

    fn emit_finished(
        &self,
        events: &AgentEventPublisher,
        result: &Result<TurnResult>,
        replayed: bool,
        nanos: u64,
    ) {
        if let Err(error) = result
            && !replayed
        {
            emit(
                events,
                AgentEventKind::RunError,
                json!({"message": error.to_string()}),
            );
        }
        let (status, kind) = match result {
            Ok(_) => ("completed", AgentEventKind::RunCompleted),
            Err(NanocodeError::TurnCancelled | NanocodeError::TurnStopped) => {
                ("cancelled", AgentEventKind::RunFailed)
            }
            Err(_) => ("failed", AgentEventKind::RunFailed),
        };
        let (model, effort) = self.labels();
        let usage = result.as_ref().ok().and_then(TurnResult::usage);
        let cost = usage.and_then(TurnUsage::estimated_cost);
        emit(
            events,
            kind,
            json!({
                "status": status,
                "model": model,
                "reasoning_mode": "standard",
                "effort": effort,
                "transport": TRANSPORT,
                "orchestration": ORCHESTRATION,
                "replayed": replayed,
                "duration_ms": nanos / 1_000_000,
                "duration_ns": nanos,
                "final_message": result.as_ref().ok().map(TurnResult::final_message),
                "usage": usage,
                "error": result.as_ref().err().map(ToString::to_string),
                "estimated_cost": cost,
                "cost_usd": cost.map(|cost| cost.amount().as_f64()),
                "cost_status": usage.map_or(nanocode_agent_core::CostStatus::Other, TurnUsage::cost_status),
            }),
        );
    }

    async fn replay(
        &self,
        owner: &DurableOwner,
        id: &str,
        output: Value,
        checkpoint: Option<Value>,
    ) -> Result<TurnResult> {
        let output: Output = serde_json::from_value(output).map_err(recovery_error)?;
        let boundary = match checkpoint {
            Some(payload) => {
                let (payload, _) = crate::pages::hydrate(
                    nanocode_durability::PayloadReader::from(owner),
                    payload,
                    &mut std::collections::HashSet::new(),
                )
                .await
                .map_err(recovery_error)?;
                let mut state = State::restored(&payload).map_err(recovery_error)?;
                state.settle();
                nanocode_agent_core::backend::TurnBoundary::portable(self.checkpoint_of(&state))
            }
            None => self.boundary(),
        };
        Ok(TurnResult {
            request_id: Some(id.to_owned()),
            turn_id: Some(id.to_owned()),
            final_message: output.final_message,
            usage: output.usage,
            boundary: Some(boundary),
        })
    }

    async fn retire(&self, owner: &DurableOwner, id: &str) {
        if !self.cancel_unstarted(owner, id).await {
            let _ = owner.release_claim(id.to_owned()).await;
        }
    }

    async fn cancel_unstarted(&self, owner: &DurableOwner, id: &str) -> bool {
        match owner.cancel(id.to_owned(), None).await {
            Ok(()) => true,
            Err(DurableError::CancellationCheckpointRequired { .. }) => {
                let settled = self.settled.get();
                matches!(
                    self.write(&settled, |payload| owner
                        .cancel(id.to_owned(), Some(payload)))
                        .await,
                    Ok(Ok(()))
                )
            }
            Err(_) => false,
        }
    }

    pub(crate) async fn admit(self: &Arc<Self>, request: BackendPrompt) -> Result<BackendTurn> {
        if self.life() != OPEN {
            return Err(stopped());
        }
        if !request.carried_steers.is_empty() && self.config.durable.is_none() {
            return Err(NanocodeError::UnsupportedCapability {
                capability: "carried_steers",
            });
        }
        let automatic = request.request_id.is_none();
        let (id, pending) = match &self.config.durable {
            None => (
                request
                    .request_id
                    .clone()
                    .unwrap_or_else(|| format!("{}-{}", self.config.session, request.key.0)),
                false,
            ),
            Some(owner) => {
                let (id, admission) = match &request.request_id {
                    Some(id) => (
                        id.clone(),
                        owner
                            .admit_typed::<_, Value, Value>(id.clone(), &request.prompt)
                            .await
                            .map_err(agent_error)?,
                    ),
                    None => owner
                        .admit_automatic_typed::<_, Value, Value>(
                            candidate_id("turn"),
                            &request.prompt,
                        )
                        .await
                        .map_err(agent_error)?
                        .into_parts(),
                };
                let events = request.events.clone().with_turn_id(id.clone());
                let terminal = match admission {
                    Admission::Accepted => None,
                    Admission::Pending => {
                        return self.enqueue(request, id, true, automatic, events).await;
                    }
                    Admission::Completed { .. }
                        if crate::sim::mutated(crate::sim::Mutation::ReplayReruns) =>
                    {
                        None
                    }
                    Admission::Completed { output, checkpoint } => {
                        Some(self.replay(owner, &id, output, checkpoint).await)
                    }
                    Admission::Failed { error, .. } => {
                        Some(Err(NanocodeError::ReplayedExecutionFailed(error)))
                    }
                    Admission::Cancelled => Some(Err(NanocodeError::TurnCancelled)),
                };
                if let Some(result) = terminal {
                    self.emit_finished(&events, &result, true, 0);
                    return Ok(BackendTurn {
                        request_id: Some(id),
                        result: Box::pin(async move { result }),
                    });
                }
                (id, false)
            }
        };
        let events = request.events.clone().with_turn_id(id.clone());
        self.enqueue(request, id, pending, automatic, events).await
    }

    async fn enqueue(
        self: &Arc<Self>,
        request: BackendPrompt,
        id: String,
        pending: bool,
        automatic: bool,
        events: AgentEventPublisher,
    ) -> Result<BackendTurn> {
        if let Some(owner) = &self.config.durable
            && !request.carried_steers.is_empty()
        {
            let steers = request
                .carried_steers
                .iter()
                .map(|(message, prompt)| (message.clone(), prompt.clone(), prompt.clone()))
                .collect::<Vec<_>>();
            if let Err(error) = owner.carry_steers(id.clone(), &steers).await {
                self.retire(owner, &id).await;
                return Err(agent_error(error));
            }
        }
        let turn = Arc::new(Turn {
            key: request.key.0,
            id: id.clone(),
            host_context: self
                .config
                .host_context
                .clone()
                .unwrap_or_else(|| Arc::from(id.as_str())),
            pending,
            automatic,
            cancel_on_admission: request.cancel_on_admission,
            cancel: Signal::new(),
            stop: Signal::new(),
            started: AtomicBool::new(false),
            accepting: AtomicBool::new(true),
            calls: AtomicU32::new(0),
            steers: Atom::default(),
            settled: Signal::new(),
            events: events.clone(),
        });
        self.turns.update(|turns| {
            turns.insert(turn.key, Arc::clone(&turn));
        });
        emit(
            &events,
            AgentEventKind::InputAccepted,
            json!({
                "session_id": self.config.session,
                "turn_id": id,
                "item_id": format!("{id}:prompt"),
                "kind": "prompt",
                "request_id": request.request_id.as_deref().or(self.config.durable.as_ref().map(|_| id.as_str())),
                "input": request.prompt.instruction,
            }),
        );
        let (sender, receiver) = nanocode_sync::oneshot::channel();
        let inner = Arc::clone(self);
        let job_turn = Arc::clone(&turn);
        let prompt = request.prompt;
        let job: crate::driver::Job = Box::new(move || {
            Box::pin(async move {
                let result = inner.run_turn(&job_turn, &prompt).await;
                inner.turns.update(|turns| {
                    turns.remove(&job_turn.key);
                });
                job_turn.settled.fire();
                drop(sender.send(result));
            })
        });
        if self.jobs.send(job).is_err() {
            self.turns.update(|turns| {
                turns.remove(&turn.key);
            });
            if let Some(owner) = &self.config.durable {
                let _ = owner.release_claim(id).await;
            }
            return Err(stopped());
        }
        Ok(BackendTurn {
            request_id: Some(id),
            result: Box::pin(
                async move { receiver.await.unwrap_or(Err(NanocodeError::TurnStopped)) },
            ),
        })
    }

    async fn run_turn(&self, turn: &Turn, prompt: &Prompt) -> Result<TurnResult> {
        let started = nanocode_platform::now();
        let result = self
            .origin(Some(Arc::clone(&turn.host_context)))
            .scope(self.attempt(turn, prompt))
            .await;
        let nanos =
            u64::try_from(nanocode_platform::elapsed(started).as_nanos()).unwrap_or(u64::MAX);
        self.emit_finished(&turn.events, &result, false, nanos);
        result
    }

    async fn attempt(&self, turn: &Turn, prompt: &Prompt) -> Result<TurnResult> {
        let Some(owner) = &self.config.durable else {
            if turn.cancel.is_fired() {
                return Err(NanocodeError::TurnCancelled);
            }
            if self.life() != OPEN {
                return Err(NanocodeError::TurnStopped);
            }
            turn.started.store(true, Ordering::SeqCst);
            self.emit_started(turn, prompt);
            let ended = self.run(turn, prompt, self.settled_state()).await;
            return self.settle_local(ended);
        };
        if turn.cancel.is_fired() {
            self.retire(owner, &turn.id).await;
            return Err(NanocodeError::TurnCancelled);
        }
        if self.life() != OPEN {
            let _ = owner.release_claim(turn.id.clone()).await;
            return Err(match self.life() {
                STOPPED => NanocodeError::ExecutionPolicyOwnerStopped,
                _ => NanocodeError::TurnStopped,
            });
        }
        if let Err(error) = owner.begin_attempt(turn.id.clone()).await {
            let error = agent_error(error);
            let blocked =
                error.execution_policy_disposition() == Some(ExecutionPolicyDisposition::Retry);
            if blocked && turn.cancel_on_admission && self.cancel_unstarted(owner, &turn.id).await {
                return Err(NanocodeError::TurnCancelled);
            }
            match turn.automatic {
                true => self.retire(owner, &turn.id).await,
                false => {
                    let _ = owner.release_claim(turn.id.clone()).await;
                }
            }
            return Err(error);
        }
        turn.started.store(true, Ordering::SeqCst);
        let state = match self.resumed(owner, turn).await {
            Ok(state) => state,
            Err(error) => {
                self.stop();
                return Err(recovery_error(error));
            }
        };
        self.emit_started(turn, prompt);
        let ended = self.run(turn, prompt, state).await;
        self.settle(owner, turn, ended).await
    }

    pub(crate) fn settled_state(&self) -> State {
        State::clone(&self.settled.get())
    }

    async fn resumed(&self, owner: &DurableOwner, turn: &Turn) -> Result<State> {
        if !turn.pending {
            return Ok(self.settled_state());
        }
        match owner
            .continuation(turn.id.clone())
            .await
            .map_err(agent_error)?
        {
            Some(continuation) => {
                let payload = owner
                    .load_payload(continuation)
                    .await
                    .map_err(agent_error)?
                    .decode::<Value>()
                    .map_err(agent_error)?;
                let (payload, pages) = crate::pages::hydrate(
                    nanocode_durability::PayloadReader::from(owner),
                    payload,
                    &mut std::collections::HashSet::new(),
                )
                .await
                .map_err(agent_error)?;
                let state = State::restored(&payload)?;
                self.keep_pages(pages);
                Ok(state)
            }
            None => Ok(self.settled_state()),
        }
    }

    fn emit_started(&self, turn: &Turn, prompt: &Prompt) {
        let (model, effort) = self.labels();
        emit(
            &turn.events,
            AgentEventKind::RunStarted,
            json!({
                "mode": ORCHESTRATION,
                "model": model,
                "reasoning_mode": "standard",
                "effort": effort,
                "transport": TRANSPORT,
                "orchestration": ORCHESTRATION,
                "websocket_url": "",
                "workspace": self.config.workspace,
                "instruction_bytes": prompt.text_bytes(),
            }),
        );
    }

    async fn run(&self, turn: &Turn, prompt: &Prompt, state: State) -> Ended {
        let work = Box::pin(self.turn(state, prompt, turn, &turn.id));
        let signals = Box::pin(select(
            Box::pin(turn.cancel.wait()),
            Box::pin(turn.stop.wait()),
        ));
        match select(work, signals).await {
            Either::Left((result, _)) => Ended::Finished(result),
            Either::Right((Either::Left(_), _)) if self.life() == OPEN => Ended::Cancelled,
            Either::Right(_) => Ended::Stopped,
        }
    }

    fn settle_local(&self, ended: Ended) -> Result<TurnResult> {
        let mut state = self.load();
        match ended {
            Ended::Finished(Ok(done)) => {
                state.settle();
                self.save(&state);
                self.settled.set(state);
                Ok(TurnResult {
                    boundary: Some(self.boundary()),
                    ..done
                })
            }
            Ended::Finished(Err(error)) => {
                state.close_calls(INTERRUPTED);
                state.settle();
                self.save(&state);
                self.settled.set(state);
                Err(error)
            }
            Ended::Cancelled | Ended::Stopped => {
                state.close_calls(INTERRUPTED);
                state.settle();
                self.save(&state);
                self.settled.set(state);
                Err(match ended {
                    Ended::Cancelled => NanocodeError::TurnCancelled,
                    _ => NanocodeError::TurnStopped,
                })
            }
        }
    }

    async fn settle(&self, owner: &DurableOwner, turn: &Turn, ended: Ended) -> Result<TurnResult> {
        let id = turn.id.clone();
        let mut state = self.load();
        let (written, keep) = match ended {
            Ended::Finished(Err(error)) if error.execution_policy_disposition().is_some() => {
                self.stop();
                return Err(reopen(error));
            }
            Ended::Stopped => {
                return match owner.release_claim(id).await {
                    Ok(()) => Err(NanocodeError::TurnStopped),
                    Err(error) => {
                        self.stop();
                        Err(reopen(agent_error(error)))
                    }
                };
            }
            Ended::Finished(Err(NanocodeError::Response(error))) if error.retryable() => {
                let message = error.to_string();
                self.save(&self.settled_state());
                let written = owner.fail_attempt(id.clone(), message).await;
                (written.map(|()| Err(NanocodeError::Response(error))), false)
            }
            Ended::Finished(Ok(done)) => {
                state.settle();
                let output = json!({"final_message": done.final_message, "usage": done.usage});
                let written = self
                    .write(&state, |payload| {
                        owner.complete(id.clone(), payload, &output)
                    })
                    .await?;
                (written.map(|()| Ok(done)), true)
            }
            Ended::Cancelled => {
                if let Err(error) = self.abandon_calls(&state.open_ids(&id), INTERRUPTED).await {
                    self.stop();
                    return Err(recovery_error(error));
                }
                state.close_calls(INTERRUPTED);
                state.settle();
                let written = self
                    .write(&state, |payload| owner.cancel(id.clone(), Some(payload)))
                    .await?;
                (written.map(|()| Err(NanocodeError::TurnCancelled)), true)
            }
            Ended::Finished(Err(error)) => {
                if let Err(abandoned) = self.abandon_calls(&state.open_ids(&id), INTERRUPTED).await
                {
                    self.stop();
                    return Err(recovery_error(abandoned));
                }
                state.close_calls(INTERRUPTED);
                state.settle();
                let message = error.to_string();
                let written = self
                    .write(&state, |payload| owner.fail(id.clone(), payload, message))
                    .await?;
                (written.map(|()| Err(error)), true)
            }
        };
        match written {
            Ok(result) => {
                if keep {
                    self.save(&state);
                    self.settled.set(state);
                }
                result.map(|done| TurnResult {
                    boundary: Some(self.boundary()),
                    ..done
                })
            }
            Err(error) => Err(self.recover_failure(owner, id, agent_error(error)).await),
        }
    }

    async fn recover_failure(
        &self,
        owner: &DurableOwner,
        id: String,
        error: NanocodeError,
    ) -> NanocodeError {
        if matches!(
            error.execution_policy_disposition(),
            Some(ExecutionPolicyDisposition::Reopen | ExecutionPolicyDisposition::Fatal)
        ) {
            self.stop();
            return error;
        }
        let recovered = match owner.recover_failure(id).await {
            Ok(Some(OperationStatus::Failed { error, .. })) => {
                NanocodeError::ReplayedExecutionFailed(error)
            }
            Ok(Some(OperationStatus::Cancelled { .. })) => NanocodeError::TurnCancelled,
            Ok(Some(OperationStatus::Pending | OperationStatus::Completed { .. })) => {
                NanocodeError::execution_policy_with_disposition(
                    "durable operation recovery",
                    ExecutionPolicyDisposition::Retry,
                    error,
                )
            }
            Ok(None) => error,
            Err(error) => agent_error(error),
        };
        self.stop();
        reopen(recovered)
    }

    pub(crate) async fn route(
        self: &Arc<Self>,
        request: BackendPrompt,
    ) -> Result<BackendPromptRoute> {
        if self.life() != OPEN {
            return Err(stopped());
        }
        let unfinished = match (&self.config.durable, &request.request_id) {
            (Some(owner), Some(id)) => {
                if owner
                    .identified_steer_accepted(id.clone(), &request.prompt)
                    .await
                    .map_err(agent_error)?
                {
                    return Ok(BackendPromptRoute::Steered);
                }
                owner
                    .unfinished_operation(id.clone())
                    .await
                    .map_err(agent_error)?
            }
            _ => false,
        };
        let running = self
            .turns
            .get()
            .values()
            .find(|turn| {
                turn.accepting.load(Ordering::SeqCst) && turn.started.load(Ordering::SeqCst)
            })
            .map(|turn| turn.key);
        if !unfinished && let Some(key) = running {
            match self
                .steer(key, request.request_id.clone(), request.prompt.clone())
                .await
            {
                Ok(()) => return Ok(BackendPromptRoute::Steered),
                Err(NanocodeError::TurnNotSteerable) => {}
                Err(error) => return Err(error),
            }
        }
        self.admit(request).await.map(BackendPromptRoute::Started)
    }

    fn running(&self, key: u64) -> Result<Arc<Turn>> {
        self.turns
            .get()
            .get(&key)
            .filter(|turn| {
                turn.accepting.load(Ordering::SeqCst) && turn.started.load(Ordering::SeqCst)
            })
            .cloned()
            .ok_or(NanocodeError::TurnNotSteerable)
    }

    pub(crate) async fn steer(&self, key: u64, id: Option<String>, prompt: Prompt) -> Result<()> {
        let turn = self.running(key)?;
        let index = match &self.config.durable {
            Some(owner) => {
                let after = turn.calls.load(Ordering::SeqCst);
                let accepted = match &id {
                    Some(message) => {
                        owner
                            .accept_identified_steer(
                                turn.id.clone(),
                                message.clone(),
                                after,
                                &prompt,
                                &prompt,
                            )
                            .await
                    }
                    None => {
                        owner
                            .accept_steer(turn.id.clone(), after, &prompt, None)
                            .await
                    }
                };
                match accepted.map_err(agent_error)? {
                    Some(index) => Some(index),
                    None => return Ok(()),
                }
            }
            None => None,
        };
        let steer = Steer {
            index,
            id: id.clone(),
            prompt: prompt.clone(),
        };
        turn.steers.update(|steers| steers.push(steer.clone()));
        std::sync::atomic::fence(Ordering::SeqCst);
        if !turn.accepting.load(Ordering::SeqCst) && self.take_steer(&turn, index, id.as_deref()) {
            if let (Some(owner), Some(index)) = (&self.config.durable, index) {
                owner
                    .withdraw_steer(turn.id.clone(), index)
                    .await
                    .map_err(agent_error)?;
            }
            return Err(NanocodeError::TurnNotSteerable);
        }
        let item = id
            .clone()
            .unwrap_or_else(|| nanocode_platform::uuid_v7().to_string());
        emit(
            &turn.events,
            AgentEventKind::InputAccepted,
            json!({
                "session_id": self.config.session,
                "turn_id": turn.id,
                "item_id": format!("{}:steer:{item}", turn.id),
                "kind": "steer",
                "request_id": id,
                "input": prompt.instruction,
            }),
        );
        Ok(())
    }

    fn take_steer(&self, turn: &Turn, index: Option<u32>, id: Option<&str>) -> bool {
        turn.steers.update(|steers| {
            let found = steers
                .iter()
                .position(|steer| steer.index == index && steer.id.as_deref() == id);
            found.map(|position| steers.remove(position)).is_some()
        })
    }

    pub(crate) async fn withdraw_steer(&self, key: u64, id: &str) -> Result<bool> {
        let Some(turn) = self.turns.get().get(&key).cloned() else {
            return Ok(false);
        };
        let removed = turn.steers.update(|steers| {
            let found = steers
                .iter()
                .position(|steer| steer.id.as_deref() == Some(id));
            found.map(|position| steers.remove(position))
        });
        let Some(steer) = removed else {
            return Ok(false);
        };
        if let (Some(owner), Some(index)) = (&self.config.durable, steer.index) {
            owner
                .withdraw_steer(turn.id.clone(), index)
                .await
                .map_err(agent_error)?;
        }
        Ok(true)
    }

    pub(crate) async fn retain_steers(&self, state: &mut State, turn: &Turn) -> Result<()> {
        let Some(owner) = &self.config.durable else {
            return Ok(());
        };
        let retained = owner
            .retained_steers(turn.id.clone())
            .await
            .map_err(agent_error)?;
        for steer in retained {
            if let Some((index, call)) = state
                .progress
                .steers
                .iter()
                .copied()
                .find(|(index, _)| *index == steer.index)
            {
                if steer.state.model_call_index.is_none() {
                    owner
                        .bind_steer(turn.id.clone(), index, call)
                        .await
                        .map_err(agent_error)?;
                }
                continue;
            }
            let prompt: Prompt = owner
                .load_payload(steer.state.input.clone())
                .await
                .map_err(agent_error)?
                .decode()
                .map_err(recovery_error)?;
            turn.steers.update(|steers| {
                steers.push(Steer {
                    index: Some(steer.index),
                    id: steer.state.message_id.clone(),
                    prompt: prompt.clone(),
                });
            });
        }
        Ok(())
    }

    pub(crate) fn inject_steers(&self, state: &mut State, turn: &Turn) -> Vec<u32> {
        let steers = turn.steers.update(std::mem::take);
        let notices = self.notices.update(std::mem::take);
        if steers.is_empty() && notices.is_empty() {
            return Vec::new();
        }
        let call = state.progress.calls + 1;
        let mut parts = notices.into_iter().map(Part::text).collect::<Vec<_>>();
        let mut binds = Vec::with_capacity(steers.len());
        for steer in &steers {
            if let Some(revision) = steer.prompt.instruction_revision()
                && !crate::sim::mutated(crate::sim::Mutation::SteerRevisionIgnored)
            {
                state.progress.instruction_revision = Some(revision);
            }
            let text = prompt_text(&steer.prompt.instruction);
            state.requests.push(text.clone());
            parts.push(Part::text(crate::prompts::user_request(
                &text,
                &nanocode_backend_antigravity_tools::local_timestamp(Self::now()),
                None,
            )));
            if let Some(index) = steer.index {
                state.progress.steers.push((index, call));
                binds.push(index);
            }
        }
        state.step += 1;
        match state.contents.last_mut() {
            Some(last) if last.role == "user" => {
                state.transcript.push(Content::user(parts.clone()));
                last.parts.extend(parts);
            }
            _ => state.push(Content::user(parts)),
        }
        binds
    }

    pub(crate) async fn bind_steers(&self, turn: &Turn, binds: &[u32], call: u32) -> Result<()> {
        let Some(owner) = &self.config.durable else {
            return Ok(());
        };
        for index in binds {
            owner
                .bind_steer(turn.id.clone(), *index, call)
                .await
                .map_err(agent_error)?;
        }
        Ok(())
    }

    pub(crate) async fn cancel(&self, key: u64) -> Result<()> {
        let turn = self
            .turns
            .get()
            .get(&key)
            .cloned()
            .ok_or(NanocodeError::TurnNotCancellable)?;
        turn.accepting.store(false, Ordering::SeqCst);
        turn.cancel.fire();
        if self.config.durable.is_some() {
            turn.settled.wait().await;
            if self.life() == STOPPED {
                return Err(NanocodeError::ExecutionPolicyOwnerStopped);
            }
        }
        Ok(())
    }

    pub(crate) async fn cancel_active(&self) -> Result<()> {
        let active = self
            .turns
            .get()
            .values()
            .find(|turn| turn.started.load(Ordering::SeqCst))
            .map(|turn| turn.key);
        match active {
            Some(key) => self.cancel(key).await,
            None => Ok(()),
        }
    }

    pub(crate) async fn shutdown(self: &Arc<Self>) -> Result<()> {
        if self
            .life
            .compare_exchange(OPEN, CLOSING, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
            && self.life() == CLOSING
        {
            return Ok(());
        }
        for turn in self.turns.get().values() {
            turn.accepting.store(false, Ordering::SeqCst);
            turn.stop.fire();
        }
        let (sender, receiver) = nanocode_sync::oneshot::channel();
        let job: crate::driver::Job = Box::new(move || {
            Box::pin(async move {
                let _ = sender.send(());
            })
        });
        if self.jobs.send(job).is_ok() {
            let _ = receiver.await;
        }
        self.config.engine.shutdown().await;
        match &self.config.durable {
            Some(owner) => owner.shutdown().await.map_err(agent_error),
            None => Ok(()),
        }
    }

    pub(crate) async fn persist_initial(&self) -> Result<()> {
        let Some(owner) = &self.config.durable else {
            return Ok(());
        };
        if self.config.committed || self.initialized.load(Ordering::SeqCst) {
            return Ok(());
        }
        let settled = self.settled.get();
        self.write(&settled, |payload| owner.commit_checkpoint(payload))
            .await?
            .map_err(agent_error)?;
        self.initialized.store(true, Ordering::SeqCst);
        Ok(())
    }

    pub(crate) async fn discard_initial(&self) -> Result<()> {
        let Some(owner) = &self.config.durable else {
            return Ok(());
        };
        if !self.initialized.load(Ordering::SeqCst) {
            return Ok(());
        }
        owner.discard_unused().await.map_err(agent_error)
    }

    pub(crate) async fn queue_compaction(self: &Arc<Self>) -> Result<()> {
        let (sender, receiver) = nanocode_sync::oneshot::channel();
        let inner = Arc::clone(self);
        let job: crate::driver::Job =
            Box::new(move || Box::pin(async move { drop(sender.send(inner.compaction().await)) }));
        if self.jobs.send(job).is_err() {
            return Err(stopped());
        }
        receiver.await.unwrap_or(Err(NanocodeError::TurnStopped))
    }

    async fn compaction(&self) -> Result<()> {
        if self.life() != OPEN {
            return Err(stopped());
        }
        let mut state = self.settled_state();
        if state.contents.is_empty() {
            return Ok(());
        }
        let events = self.config.events.clone();
        let Some(owner) = &self.config.durable else {
            self.compaction_origin(None)
                .scope(self.compact(&mut state, &events))
                .await?;
            self.save(&state);
            self.settled.set(state);
            return Ok(());
        };
        let (id, admission) = owner
            .admit_automatic_typed::<_, Value, Value>(
                candidate_id("compact"),
                &json!({"provider": crate::harness::FAMILY, "kind": "compact"}),
            )
            .await
            .map_err(agent_error)?
            .into_parts();
        match admission {
            Admission::Completed { .. } => return Ok(()),
            Admission::Failed { error, .. } => {
                return Err(NanocodeError::ReplayedExecutionFailed(error));
            }
            Admission::Cancelled => return Err(NanocodeError::TurnCancelled),
            Admission::Accepted | Admission::Pending => {}
        }
        if let Err(error) = owner.begin_attempt(id.clone()).await {
            self.retire(owner, &id).await;
            return Err(agent_error(error));
        }
        let result = self
            .compaction_origin(Some(Arc::from(id.as_str())))
            .scope(self.compact(&mut state, &events))
            .await;
        let written = match &result {
            Err(error) if error.execution_policy_disposition().is_some() => {
                self.stop();
                return result.map_err(reopen);
            }
            Ok(()) => {
                self.write(&state, |payload| {
                    owner.complete(id.clone(), payload, &Value::Null)
                })
                .await?
            }
            Err(error) => {
                let message = error.to_string();
                let settled = self.settled_state();
                self.write(&settled, |payload| owner.fail(id.clone(), payload, message))
                    .await?
            }
        };
        if let Err(error) = written {
            return Err(self.recover_failure(owner, id, agent_error(error)).await);
        }
        if result.is_ok() {
            self.save(&state);
            self.settled.set(state);
        }
        result
    }

    pub(crate) async fn append_notice(
        self: &Arc<Self>,
        text: String,
        prefix: Option<String>,
    ) -> Result<bool> {
        let notice = system_message(&text);
        if !self.turns.get().is_empty() {
            self.notices.update(|notices| notices.push(notice.clone()));
            return Ok(true);
        }
        let (sender, receiver) = nanocode_sync::oneshot::channel();
        let inner = Arc::clone(self);
        let job: crate::driver::Job = Box::new(move || {
            Box::pin(async move { drop(sender.send(inner.commit_notice(notice, prefix).await)) })
        });
        if self.jobs.send(job).is_err() {
            return Err(stopped());
        }
        receiver.await.unwrap_or(Err(NanocodeError::TurnStopped))
    }

    async fn commit_notice(&self, notice: String, prefix: Option<String>) -> Result<bool> {
        if self.life() != OPEN {
            return Err(stopped());
        }
        let mut state = self.settled_state();
        let repeated = prefix.as_deref().is_some_and(|prefix| {
            state
                .contents
                .iter()
                .rev()
                .filter(|content| content.role == "user")
                .flat_map(|content| content.parts.iter().rev())
                .filter_map(|part| part.text.as_deref())
                .find(|text| text.contains(prefix))
                .is_some_and(|latest| latest.trim_end().ends_with(notice.trim_end()))
        });
        if repeated {
            return Ok(false);
        }
        state.step += 1;
        state.push(Content::user(vec![Part::text(notice)]));
        if let Some(owner) = &self.config.durable {
            self.write(&state, |payload| owner.commit_checkpoint(payload))
                .await?
                .map_err(agent_error)?;
        }
        self.save(&state);
        self.settled.set(state);
        Ok(true)
    }
}
