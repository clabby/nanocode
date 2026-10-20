use std::{
    collections::HashMap,
    fmt,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use nanocode_subagents::{AgentUpdate, ScopedAgentUpdate};
use nanocode_sync::Atom;
use nanocode_tool_host::embedded::HostFuture;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

use crate::{
    Agent, AgentRuntime, DurableOutcome, EventSink, NanocodeError, PromptRequest, SessionEvent,
    SessionEvents, TurnControl,
    durability::{SessionRecord, SessionStore, Sql, StoreError},
    input::Prompt,
    session_log::{LogEntry, LoggedEvent, SessionLog, Started, TurnRecord, TurnState},
};

pub trait InboxHost: Send + Sync + 'static {
    fn publish(&self, events: Vec<LoggedEvent>) -> HostFuture<'_, Result<(), String>>;

    fn annotate(&self) -> Map<String, Value> {
        Map::new()
    }

    fn finishing(&self, _request_id: &str) -> HostFuture<'_, Map<String, Value>> {
        Box::pin(async { Map::new() })
    }

    fn place(&self, _session_id: &str, _hand: Option<&str>) {}

    fn idle(&self) -> bool {
        true
    }
}

#[derive(Debug)]
pub enum InboxError {
    Store(StoreError),
    Agent(NanocodeError),
    Events(String),
}

impl fmt::Display for InboxError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Store(error) => error.fmt(formatter),
            Self::Agent(error) => error.fmt(formatter),
            Self::Events(error) => formatter.write_str(error),
        }
    }
}

impl std::error::Error for InboxError {}

impl From<StoreError> for InboxError {
    fn from(error: StoreError) -> Self {
        Self::Store(error)
    }
}

impl From<NanocodeError> for InboxError {
    fn from(error: NanocodeError) -> Self {
        Self::Agent(error)
    }
}

#[derive(Clone, Debug)]
pub enum Driven {
    Idle,
    Busy,
    Finished(TurnRecord),
    Resumed(String),
    Waiting(String),
    Interrupted(String),
}

#[derive(Clone, Default)]
struct Request {
    running: bool,
    inbox: bool,
    cancelled: bool,
    control: Option<TurnControl>,
}

#[derive(Clone, Default)]
struct Requests {
    requests: HashMap<String, Request>,
    turns: HashMap<String, String>,
}

impl Requests {
    fn cancelled(&self, id: &str) -> bool {
        self.requests
            .get(id)
            .is_some_and(|request| request.cancelled)
    }

    fn idle(&self) -> bool {
        !self.requests.values().any(|request| request.running)
    }

    fn tidy(&mut self, id: &str) {
        if self
            .requests
            .get(id)
            .is_some_and(|request| !request.running && !request.cancelled)
        {
            self.requests.remove(id);
        }
    }
}

struct Shared<H: ?Sized> {
    log: Arc<SessionLog<H>>,
    host: Arc<dyn InboxHost>,
    state: Atom<Requests>,
}

#[derive(Default, Deserialize)]
struct Identifiers {
    #[serde(default)]
    turn_id: Option<String>,
    #[serde(default)]
    request_id: Option<String>,
}

#[derive(Serialize)]
struct Envelope<'a> {
    #[serde(flatten)]
    event: &'a SessionEvent,
    #[serde(flatten)]
    annotation: &'a Map<String, Value>,
}

impl<H: ?Sized> Shared<H> {
    fn attribute(&self, event: &SessionEvent) -> Option<String> {
        let (event, agent) = match event {
            SessionEvent::Agent { event, .. } => (event, true),
            SessionEvent::Subagent(ScopedAgentUpdate {
                update: AgentUpdate::Event { event, .. },
                ..
            }) => (event, false),
            SessionEvent::Subagent(_) => return None,
        };
        let ids: Identifiers = serde_json::from_str(event.payload.get()).unwrap_or_default();
        match (ids.turn_id, ids.request_id) {
            (Some(turn), _) if !agent => Some(turn),
            (Some(turn), Some(accepted))
                if self.state.read(|state| {
                    state
                        .requests
                        .get(&accepted)
                        .is_some_and(|request| request.running && request.inbox)
                }) =>
            {
                self.state.update(|state| {
                    state.turns.insert(turn.clone(), accepted.clone());
                });
                Some(accepted)
            }
            (Some(turn), _) => Some(
                self.state
                    .read(|state| state.turns.get(&turn).cloned())
                    .unwrap_or(turn),
            ),
            (None, accepted) => accepted,
        }
    }
}

struct LogSink<H: ?Sized>(Arc<Shared<H>>);

impl<H: Sql + Send + Sync + ?Sized + 'static> EventSink for LogSink<H> {
    fn deliver(&self, events: Vec<SessionEvent>) -> HostFuture<'_, Result<(), String>> {
        Box::pin(async move {
            let annotation = self.0.host.annotate();
            let entries = events
                .iter()
                .map(|event| {
                    Ok(LogEntry {
                        request_id: self.0.attribute(event),
                        event: serde_json::value::to_raw_value(&Envelope {
                            event,
                            annotation: &annotation,
                        })?,
                    })
                })
                .collect::<Result<Vec<_>, serde_json::Error>>()
                .map_err(|error| format!("encode agent event: {error}"))?;
            let logged = self
                .0
                .log
                .append(&entries)
                .await
                .map_err(|error| error.to_string())?;
            self.0.host.publish(logged).await
        })
    }
}

struct Claim<'a, H: ?Sized>(&'a Shared<H>, &'a str);

impl<H: ?Sized> Drop for Claim<'_, H> {
    fn drop(&mut self) {
        self.0.state.update(|state| {
            if let Some(request) = state.requests.get_mut(self.1) {
                request.running = false;
                request.control = None;
            }
            state.tidy(self.1);
        });
    }
}

struct Busy<'a>(&'a AtomicBool);

impl Drop for Busy<'_> {
    fn drop(&mut self) {
        self.0.store(false, Ordering::Release);
    }
}

fn ignore_inactive(result: Result<(), NanocodeError>) -> Result<(), NanocodeError> {
    match result {
        Err(NanocodeError::TurnNotCancellable) => Ok(()),
        other => other,
    }
}

fn fields(value: Value) -> Map<String, Value> {
    match value {
        Value::Object(fields) => fields,
        _ => Map::new(),
    }
}

pub struct TurnInbox<H: ?Sized> {
    shared: Arc<Shared<H>>,
    runtime: AgentRuntime,
    agent: Agent,
    events: SessionEvents,
    model: String,
    busy: AtomicBool,
}

impl<H: Sql + Send + Sync + ?Sized + 'static> TurnInbox<H> {
    pub async fn open(
        runtime: AgentRuntime,
        store: &SessionStore,
        record: SessionRecord,
        log: Arc<SessionLog<H>>,
        host: Arc<dyn InboxHost>,
        window: Duration,
    ) -> Result<(Self, HostFuture<'static, ()>), InboxError> {
        let session_id = record.session_id.clone();
        let catalog = |error| InboxError::Agent(crate::runtime::catalog_error(error));
        if store.summary(&session_id).await.map_err(catalog)?.is_none() {
            store.session(record).await.map_err(catalog)?;
        }
        let subagents = runtime.take_subagent_updates();
        let stored = store.load(&session_id).await.map_err(catalog)?;
        let (agent, stream) = runtime.open_loaded(store, &stored).await?;
        let shared = Arc::new(Shared {
            log,
            host,
            state: Atom::default(),
        });
        let (events, pump) =
            SessionEvents::start(stream, subagents, window, LogSink(Arc::clone(&shared)));
        Ok((
            Self {
                shared,
                runtime,
                agent,
                events,
                model: stored.summary.record.model.as_str().to_owned(),
                busy: AtomicBool::new(false),
            },
            pump,
        ))
    }

    #[must_use]
    pub const fn agent(&self) -> &Agent {
        &self.agent
    }

    #[must_use]
    pub fn log(&self) -> &Arc<SessionLog<H>> {
        &self.shared.log
    }

    #[must_use]
    pub fn session_id(&self) -> &str {
        self.agent.session_id()
    }

    #[must_use]
    pub fn model(&self) -> &str {
        &self.model
    }

    pub async fn deliver_completions(&self) {
        if let Some(control) = self.runtime.subagents() {
            control
                .deliver_completions(&self.agent, self.agent.session_id())
                .await;
        }
    }

    pub async fn drive(&self, next: Option<&str>) -> Result<Driven, InboxError> {
        if self.busy.swap(true, Ordering::AcqRel) {
            return Ok(Driven::Busy);
        }
        let _busy = Busy(&self.busy);
        let root = self.session_id();
        if let Some(first) = self.runtime.pending_turns(root).await?.into_iter().next() {
            return match self.shared.log.turn(&first).await? {
                Some(record) if !record.state.is_terminal() => self.run(&first, true).await,
                record => {
                    let abandoned = record
                        .as_ref()
                        .is_some_and(|record| record.state != TurnState::Completed);
                    self.resume(first, record.and_then(|record| record.hand), abandoned)
                        .await
                }
            };
        }
        let next = match next {
            Some(next) => Some(next.to_owned()),
            None => self
                .shared
                .log
                .pending()
                .await?
                .into_iter()
                .next()
                .map(|record| record.request_id),
        };
        match next {
            Some(next) => self.run(&next, false).await,
            None => Ok(Driven::Idle),
        }
    }

    pub async fn cancel(
        &self,
        request_id: &str,
        fields: &Map<String, Value>,
    ) -> Result<Option<(TurnRecord, bool)>, InboxError> {
        let Some((record, event)) = self.shared.log.cancel(request_id, fields).await? else {
            return Ok(None);
        };
        let dequeued = event.is_some();
        if let Some(event) = event {
            self.publish(event).await?;
        }
        if record.state == TurnState::Running {
            self.shared.state.update(|state| {
                state
                    .requests
                    .entry(request_id.to_owned())
                    .or_default()
                    .cancelled = true;
            });
            let live = self.shared.state.read(|state| {
                state
                    .requests
                    .get(request_id)
                    .and_then(|request| request.control.clone())
            });
            if let Some(control) = live {
                ignore_inactive(control.cancel().await)?;
            }
        }
        Ok(Some((record, dequeued)))
    }

    pub async fn settle(&self) -> Result<bool, InboxError> {
        self.events.drained().await.map_err(InboxError::Events)?;
        if !self.idle() {
            return Ok(false);
        }
        Ok(self.runtime.settle(self.session_id()).await? && self.idle())
    }

    pub async fn shutdown(&self) -> Result<(), InboxError> {
        let shutdown = self.runtime.shutdown().await;
        let delivered = self.events.drained().await;
        shutdown?;
        delivered.map_err(InboxError::Events)
    }

    fn idle(&self) -> bool {
        self.shared.state.read(Requests::idle) && self.shared.host.idle()
    }

    async fn publish(&self, event: LoggedEvent) -> Result<(), InboxError> {
        self.shared
            .host
            .publish(vec![event])
            .await
            .map_err(InboxError::Events)
    }

    async fn run(&self, id: &str, accepted: bool) -> Result<Driven, InboxError> {
        let Some(Started {
            previous,
            record,
            event,
        }) = self.shared.log.start(id).await?
        else {
            return Ok(Driven::Idle);
        };
        self.publish(event).await?;
        self.shared
            .host
            .place(self.session_id(), record.hand.as_deref());
        let stored = if accepted || previous == TurnState::Running {
            self.runtime.durable_prompt(&self.agent, id).await?
        } else {
            None
        };
        let cancel = record.cancel_requested;
        let Some(prompt) = stored.or(record.prompt) else {
            return if cancel {
                self.finish(
                    id,
                    TurnState::Cancelled,
                    fields(serde_json::json!({"cancelled": true})),
                )
                .await
            } else {
                self.finish(
                    id,
                    TurnState::Failed,
                    fields(serde_json::json!({"error": "the turn's prompt is no longer stored"})),
                )
                .await
            };
        };
        match self.execute(id, prompt, cancel, true).await {
            DurableOutcome::Completed(message) => {
                let ok = serde_json::json!({"ok": {
                    "session_id": self.session_id(),
                    "model": self.model,
                    "message": message,
                }});
                self.finish(id, TurnState::Completed, fields(ok)).await
            }
            DurableOutcome::Failed(error) => {
                self.finish(
                    id,
                    TurnState::Failed,
                    fields(serde_json::json!({"error": error})),
                )
                .await
            }
            DurableOutcome::Cancelled => {
                self.finish(
                    id,
                    TurnState::Cancelled,
                    fields(serde_json::json!({"cancelled": true})),
                )
                .await
            }
            DurableOutcome::Waiting => Ok(Driven::Waiting(id.to_owned())),
            DurableOutcome::Interrupted => Ok(Driven::Interrupted(id.to_owned())),
        }
    }

    async fn resume(
        &self,
        id: String,
        hand: Option<String>,
        abandoned: bool,
    ) -> Result<Driven, InboxError> {
        let Some(prompt) = self.runtime.durable_prompt(&self.agent, &id).await? else {
            return Ok(Driven::Resumed(id));
        };
        self.shared.host.place(self.session_id(), hand.as_deref());
        let cancel = abandoned || self.shared.state.read(|state| state.cancelled(&id));
        Ok(match self.execute(&id, prompt, cancel, false).await {
            DurableOutcome::Waiting => Driven::Waiting(id),
            DurableOutcome::Interrupted => Driven::Interrupted(id),
            _ => Driven::Resumed(id),
        })
    }

    async fn finish(
        &self,
        id: &str,
        state: TurnState,
        mut outcome: Map<String, Value>,
    ) -> Result<Driven, InboxError> {
        outcome.extend(self.shared.host.finishing(id).await);
        let finished = self.shared.log.finish(id, state, &outcome).await?;
        self.shared.state.update(|state| {
            if let Some(request) = state.requests.get_mut(id) {
                request.cancelled = false;
            }
            state.tidy(id);
        });
        let Some((record, event)) = finished else {
            return Ok(match self.shared.log.turn(id).await? {
                Some(record) => Driven::Finished(record),
                None => Driven::Idle,
            });
        };
        self.publish(event).await?;
        Ok(Driven::Finished(record))
    }

    async fn execute(&self, id: &str, prompt: Prompt, cancel: bool, inbox: bool) -> DurableOutcome {
        let claimed = self.shared.state.update(|state| {
            let request = state.requests.entry(id.to_owned()).or_default();
            if request.running {
                return false;
            }
            request.running = true;
            request.inbox = inbox;
            request.control = None;
            true
        });
        if !claimed {
            return DurableOutcome::Waiting;
        }
        let _claim = Claim(&self.shared, id);
        let outcome = self.prompt(id, prompt, cancel).await;
        if self.events.drained().await.is_err() {
            return DurableOutcome::Interrupted;
        }
        self.shared.state.update(|state| {
            state.turns.retain(|_, request| request != id);
        });
        self.runtime
            .durable_outcome(self.session_id(), id, outcome)
            .await
    }

    async fn prompt(
        &self,
        id: &str,
        prompt: Prompt,
        cancel: bool,
    ) -> Result<String, NanocodeError> {
        let cancelled = || self.shared.state.read(|state| state.cancelled(id));
        let mut request = PromptRequest::new(prompt).request_id(id);
        if cancel || cancelled() {
            request = request.cancel_on_admission();
        }
        let turn = self.agent.prompt(request).await?;
        let control = turn.control();
        self.shared.state.update(|state| {
            if let Some(request) = state.requests.get_mut(id) {
                request.control = Some(control.clone());
            }
        });
        if cancelled() {
            ignore_inactive(turn.cancel().await)?;
        }
        let message = turn.result().await?.into_final_message();
        self.agent.flush().await?;
        Ok(message)
    }
}
