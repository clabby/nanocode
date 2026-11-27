use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    sync::{Arc, Weak},
};

use nanocode_subagents::{AgentUpdate, ScopedAgentUpdate};
use nanocode_sync::Atom;
use nanocode_tool_host::{
    ToolInput, ToolOutput,
    code_mode::{
        CellJournal, CellJournalSink, CellJournalUpdate, CellJournals, JournalEntry,
        JournaledCallState,
    },
    effects::{Carried, EffectCall, EffectGate, RemoteBarrier},
    embedded::HostFuture,
    hold::{HoldCall, HoldLedger, LedgerError, Running},
};
use nanocode_turnlog::{
    Answer, EffectClass, Effects, Fold, HeldCall, Hold, LogHost, Outcome, Phase, Point, Record,
    Resumption, Retired, Rider, Stats, Status, Turn, WakeRider, Writing, input_digest, tag,
};
use serde::Serialize;
use serde_json::{Map, Value, json, value::RawValue};

use crate::{
    SessionEvent,
    durability::{
        Committed, OwnedState, OwnerId, OwnerToken, Settle, SqlStatement, StateCommit, StateStore,
        StoreError, StoreFuture, StoreRecord, StoredState,
        facts::{
            Decision, Fact, Input, InputKind, Marker, Outcome as FactOutcome, OwnedFact, TurnStatus,
        },
    },
    events::AgentEventKind,
    input::Prompt,
    session_log::{
        ImportedTurn, LogEntry, LogPage, LoggedEvent, Replaced, Started, Submission, Submitted,
        TurnRecord, TurnRetry, TurnState, edited, now, submission_digest,
    },
    stored_event::Stored,
    thread_scope::sim as thread_scope_sim,
    turn_inbox::Envelope,
    turn_log::{EventLog, Guarded, Logged, Recording, TurnLog},
};

mod export;
mod facts;
mod import;
mod over;
pub(crate) mod shared;
mod wakes;

pub use export::Exported;
pub use facts::sim as facts_sim;
pub(crate) use facts::{Owners, Scope as FactScope, UNKNOWN_OUTCOME, in_scope, unknown_outcome};
pub use import::{Imported, sim as import_sim};
pub use over::OneLogOver;
pub use wakes::{Fire, Heard, WakeError};

pub(crate) const TOOLS_MARK: &str = "$nanocode_tools";
pub(crate) const STORED_MARK: &str = "$nanocode_stored";

#[derive(Clone, Debug)]
pub struct Policy {
    pub chain_bytes_max: Option<usize>,
    pub effects: Effects,
    pub host_call_bound: Option<std::time::Duration>,
    pub max_row_bytes: Option<usize>,
    pub max_tail_bytes: Option<usize>,
    pub restarts: u64,
    pub retain_finished: Option<usize>,
    pub retired_max: Option<usize>,
    pub snapshot_ratio: usize,
    pub snapshot_rows: Option<usize>,
    pub wakes_max: Option<usize>,
}

impl Default for Policy {
    fn default() -> Self {
        Self {
            chain_bytes_max: None,
            effects: Effects::default(),
            host_call_bound: None,
            max_row_bytes: None,
            max_tail_bytes: None,
            restarts: 0,
            retain_finished: None,
            retired_max: None,
            snapshot_ratio: nanocode_turnlog::SNAPSHOT_RATIO,
            snapshot_rows: None,
            wakes_max: None,
        }
    }
}

pub struct OneLog {
    answered: Atom<BTreeMap<(String, u64), u64>>,
    retired_max: Option<usize>,
    dispatched: Atom<BTreeMap<(String, u64), BTreeSet<u64>>>,
    deferred_settles: Atom<bool>,
    durable_settles: Atom<bool>,
    effects: Atom<Effects>,
    live: Atom<BTreeSet<(String, u64)>>,
    log: nanocode_turnlog::TurnLog,
    owed: Atom<bool>,
    owners: Atom<nanocode_sync::Map<String, OwnerToken>>,
    recovering: Atom<BTreeSet<String>>,
    remembered: Atom<BTreeMap<(String, bool), Arc<crate::durability::facts::Snapshot>>>,
    resolved: Atom<BTreeSet<String>>,
    restarts: u64,
    this: Weak<Self>,
    unsaved: Atom<BTreeMap<(String, u64), u64>>,
    wake_index: Atom<Option<Arc<dyn WakeRider>>>,
    wakes_max: Option<usize>,
}

enum Admission {
    Done(Submitted),
    Retired { same: bool },
}

#[derive(Clone)]
pub struct OneLogStore(Arc<OneLog>);

pub struct OneLogJournals(Arc<OneLog>);

#[derive(Serialize)]
pub(crate) struct Lifecycle<'a> {
    #[serde(rename = "type")]
    pub(crate) kind: &'static str,
    pub(crate) request_id: &'a str,
}

#[derive(Serialize)]
pub(crate) struct Finished<'a> {
    #[serde(rename = "type")]
    pub(crate) kind: &'static str,
    pub(crate) request_id: &'a str,
    pub(crate) status: &'static str,
    #[serde(flatten)]
    pub(crate) fields: &'a Map<String, Value>,
}

#[derive(Serialize)]
struct Replacement<'a> {
    digest: &'a str,
    edit_id: &'a str,
    #[serde(rename = "type")]
    kind: &'static str,
    request_id: &'a str,
}

#[derive(Serialize)]
pub(crate) struct Retrying<'a> {
    #[serde(rename = "type")]
    pub(crate) tag: &'static str,
    pub(crate) request_id: &'a str,
    pub(crate) error: &'a str,
    pub(crate) kind: &'a str,
    pub(crate) attempts: u64,
    pub(crate) retry_at: u64,
}

fn encoding(error: serde_json::Error) -> StoreError {
    StoreError::NotCommitted(format!("encode turn log record: {error}"))
}

pub(crate) fn raw(value: &impl Serialize) -> Result<Box<RawValue>, StoreError> {
    serde_json::value::to_raw_value(value).map_err(encoding)
}

fn restored(
    fold: &nanocode_turnlog::Fold,
    session: &str,
    whole: &BTreeSet<u64>,
    shared: &[String],
) {
    let (mut cells, mut finished, mut updates, mut finished_updates) = (0_u64, 0_u64, 0_u64, 0_u64);
    for (_, cell, held) in fold.cells_of(Some(session)) {
        let bytes = held
            .iter()
            .map(|update| update.get().len() as u64)
            .sum::<u64>();
        cells += 1;
        updates += bytes;
        if !whole.contains(&cell) {
            finished += 1;
            finished_updates += bytes;
        }
    }
    let results = shared
        .iter()
        .filter_map(|key| fold.stored(key))
        .map(|value| value.len() as u64)
        .sum::<u64>();
    nanocode_trace::event!(target: "nc_metrics", nanocode_trace::Level::INFO, {
        monotonic_counter.nc.cells.restored = cells,
        monotonic_counter.nc.cells.restored_finished = finished,
        monotonic_counter.nc.cells.restored_results = shared.len() as u64,
        histogram.nc.cells.restore_update_bytes = updates,
        histogram.nc.cells.restore_finished_update_bytes = finished_updates,
        histogram.nc.cells.restore_result_bytes = results
    });
}

fn invalid(detail: impl std::fmt::Display) -> StoreError {
    StoreError::Backend(format!("turn log state is invalid: {detail}"))
}

pub(crate) const fn outcome(state: TurnState) -> Option<Outcome> {
    match state {
        TurnState::Cancelled => Some(Outcome::Cancelled),
        TurnState::Completed => Some(Outcome::Completed),
        TurnState::Failed => Some(Outcome::Failed),
        TurnState::Queued | TurnState::Running => None,
    }
}

const fn telemetry(event: &SessionEvent) -> bool {
    let kind = match event {
        SessionEvent::Agent { event, .. } => event.kind,
        SessionEvent::Subagent(ScopedAgentUpdate {
            update: AgentUpdate::Event { event, .. },
            ..
        }) => event.kind,
        SessionEvent::Subagent(_) => return false,
    };
    matches!(
        kind,
        AgentEventKind::ApiEvent
            | AgentEventKind::ModelAttemptFailed
            | AgentEventKind::ModelAttemptRetrying
            | AgentEventKind::ModelAttemptStarted
            | AgentEventKind::ModelCallCompleted
            | AgentEventKind::ModelCallStarted
            | AgentEventKind::ModelConnectionCompleted
            | AgentEventKind::ModelConnectionFailed
            | AgentEventKind::ModelConnectionStarted
            | AgentEventKind::ModelWarmupCompleted
            | AgentEventKind::ModelWarmupFailed
            | AgentEventKind::ModelWarmupStarted
    )
}

fn retry_of(retry: Option<&nanocode_turnlog::Retry>) -> Option<TurnRetry> {
    retry.map(|retry| TurnRetry {
        attempts: retry.attempts,
        error: retry.error.clone(),
        kind: retry.kind.clone(),
        retry_at: retry.retry_at,
    })
}

const fn state_of(outcome: Outcome) -> TurnState {
    match outcome {
        Outcome::Cancelled => TurnState::Cancelled,
        Outcome::Completed => TurnState::Completed,
        Outcome::Failed => TurnState::Failed,
    }
}

fn retired_record(
    id: &str,
    retired: &Retired,
    result: Option<Box<RawValue>>,
) -> Result<TurnRecord, StoreError> {
    Ok(TurnRecord {
        request_id: id.to_owned(),
        state: state_of(retired.outcome),
        digest: format!("retired:{:032x}", retired.digest),
        prompt: None,
        hand: retired.hand.as_deref().map(str::to_owned),
        metadata: raw(&Value::Null)?,
        cancel_requested: false,
        note: None,
        first_seq: retired.first,
        last_seq: Some(retired.last),
        result,
        retry: None,
    })
}

pub(crate) fn record_of(turn: &Turn) -> Result<TurnRecord, StoreError> {
    let prompt = || {
        serde_json::from_str::<Prompt>(turn.prompt.get())
            .map(Some)
            .map_err(|error| invalid(format!("prompt of turn {}: {error}", turn.id)))
    };
    let (state, prompt, cancel_requested, note, last_seq, result, retry) = match &turn.phase {
        Phase::Queued { retry } => (
            TurnState::Queued,
            prompt()?,
            false,
            None,
            None,
            None,
            retry_of(retry.as_ref()),
        ),
        Phase::Running {
            cancelling,
            note,
            retry,
        } => (
            TurnState::Running,
            prompt()?,
            *cancelling,
            note.clone(),
            None,
            None,
            retry_of(retry.as_ref()),
        ),
        Phase::Finished {
            last,
            outcome,
            result,
        } => (
            match outcome {
                Outcome::Cancelled => TurnState::Cancelled,
                Outcome::Completed => TurnState::Completed,
                Outcome::Failed => TurnState::Failed,
            },
            None,
            false,
            None,
            Some(*last),
            Some(result.clone()),
            None,
        ),
    };
    Ok(TurnRecord {
        request_id: turn.id.clone(),
        state,
        digest: turn.digest.clone(),
        prompt,
        hand: turn.hand.clone(),
        metadata: turn.metadata.clone(),
        cancel_requested,
        note,
        first_seq: turn.first,
        last_seq,
        result,
        retry,
    })
}

pub(crate) fn logged(
    seq: u64,
    request_id: Option<&str>,
    at: u64,
    event: Box<RawValue>,
) -> LoggedEvent {
    LoggedEvent {
        seq,
        request_id: request_id.map(str::to_owned),
        created_at: at,
        event,
    }
}

async fn staging(
    writing: &mut Writing,
    state: &str,
    records: &[StoreRecord],
) -> Result<(), StoreError> {
    let owned = std::iter::once(state.to_owned())
        .chain(records.iter().map(|record| stored_key(state, &record.key)))
        .collect::<Vec<_>>();
    writing.load_owners(&owned).await?;
    writing
        .index_owners(&shared::candidates(state, records))
        .await
}

fn put(writing: &mut Writing, state: &str, records: &[StoreRecord]) -> Result<(), StoreError> {
    for record in records {
        let key = stored_key(state, &record.key);
        let value = shared::staged(writing.fold(), state, record);
        if writing.fold().stored(&key) != Some(&*value) {
            let value = value.into_owned();
            writing.push(Record::Stored { key, value })?;
        }
    }
    Ok(())
}

type RuntimeFact = OwnedFact<Box<RawValue>>;

fn put_fact(writing: &mut Writing, fact: &RuntimeFact) -> Result<u64, StoreError> {
    writing.append_owned(None, fact).map_err(StoreError::from)
}

fn put_host(writing: &mut Writing, key: &str, value: Box<RawValue>) -> Result<(), StoreError> {
    if facts::sim::mutated(facts::sim::Mutation::HostCopied) {
        writing.push(Record::Host {
            key: key.to_owned(),
            value: value.clone(),
        })?;
    }
    let fact = Fact::Decision(Decision::Host {
        key: key.to_owned(),
        value,
    });
    put_fact(writing, &fact).map(drop)
}

fn settle_stopped(writing: &mut Writing, turn: &str) -> Result<(), StoreError> {
    if facts::sim::mutated(facts::sim::Mutation::CancelLeavesIntent) {
        return Ok(());
    }
    facts::settle(writing, turn, facts::Owners::Turn).map(drop)
}

fn status_fact(turn: &str, status: TurnStatus) -> RuntimeFact {
    Fact::Marker(Marker::Status {
        turn: turn.to_owned(),
        status,
    })
}

pub(crate) const fn fact_outcome(outcome: Outcome) -> FactOutcome {
    match outcome {
        Outcome::Cancelled => FactOutcome::Cancelled,
        Outcome::Completed => FactOutcome::Completed,
        Outcome::Failed => FactOutcome::Failed,
    }
}

pub(crate) fn stored_key(state: &str, key: &str) -> String {
    format!("{state}\n{key}")
}

async fn retried(
    writing: &mut Writing,
    request_id: &str,
    error: &str,
    kind: &str,
    retry_at: u64,
) -> Result<Option<Option<(TurnRecord, Option<LoggedEvent>)>>, StoreError> {
    let Some(turn) = writing.fold().turn(request_id) else {
        return Ok(None);
    };
    let previous = match &turn.phase {
        Phase::Finished { .. } => return Ok(Some(Some((record_of(turn)?, None)))),
        Phase::Queued { retry } | Phase::Running { retry, .. } => retry.clone(),
    };
    let attempts = previous.as_ref().map_or(0, |retry| retry.attempts) + 1;
    let changed = previous
        .as_ref()
        .is_none_or(|old| old.error != error || old.kind != kind);
    let event = if changed {
        writing.secure().await?;
        let seq = writing.next_event();
        let at = now();
        let body = raw(&Retrying {
            tag: "retrying",
            request_id,
            error,
            kind,
            attempts,
            retry_at,
        })?;
        writing.push(Record::Event {
            at,
            event: body.clone(),
            seq,
            turn: Some(request_id.to_owned()),
        })?;
        Some(logged(seq, Some(request_id), at, body))
    } else {
        None
    };
    put_fact(
        writing,
        &status_fact(
            request_id,
            TurnStatus::Retrying {
                attempts,
                error: error.to_owned(),
                kind: kind.to_owned(),
                retry_at,
            },
        ),
    )?;
    let record = writing.fold().turn(request_id).map(record_of).transpose()?;
    Ok(Some(record.map(|record| (record, event))))
}

pub(crate) type Encoded = Vec<(Option<Box<RawValue>>, Box<RawValue>, Option<String>)>;

pub(crate) fn encoded(recording: &Recording<'_>) -> Result<Encoded, String> {
    let mut encoded = Vec::with_capacity(recording.events.len());
    for (event, request_id) in recording.events.iter().zip(recording.request_ids) {
        let stored = if telemetry(event) {
            None
        } else {
            Some(
                serde_json::value::to_raw_value(&Stored {
                    event,
                    compact: None,
                })
                .map_err(|error| format!("encode agent event: {error}"))?,
            )
        };
        let live = serde_json::value::to_raw_value(&Envelope {
            event,
            annotation: recording.annotation,
        })
        .map_err(|error| format!("encode agent event: {error}"))?;
        encoded.push((stored, live, request_id.clone()));
    }
    Ok(encoded)
}

fn upgraded(this: &Weak<OneLog>) -> Arc<OneLog> {
    this.upgrade()
        .expect("a turn log's writer runs a job only while a caller holds its log")
}

impl OneLog {
    pub async fn open(
        host: Arc<dyn LogHost>,
        scope: impl Into<Arc<str>>,
        policy: Policy,
    ) -> Result<Arc<Self>, StoreError> {
        Self::opened(nanocode_turnlog::TurnLog::open(host, scope).await?, policy).await
    }

    pub async fn claim(
        follower: nanocode_turnlog::Follower,
        policy: Policy,
    ) -> Result<Arc<Self>, StoreError> {
        Self::opened(follower.claim().await?, policy).await
    }

    async fn opened(
        log: nanocode_turnlog::TurnLog,
        policy: Policy,
    ) -> Result<Arc<Self>, StoreError> {
        let log = log
            .retain_finished(policy.retain_finished)
            .host_calls_within(policy.host_call_bound)
            .row_limit(policy.max_row_bytes)
            .max_tail_bytes(policy.max_tail_bytes)
            .chain_bytes_max(policy.chain_bytes_max)
            .snapshot_ratio(policy.snapshot_ratio)
            .snapshot_rows(policy.snapshot_rows);
        log.write_within(|writing| {
            writing.retire_finished();
            Box::pin(std::future::ready(Ok::<_, StoreError>(())))
        })
        .await?;
        let recovering = log
            .read(|fold| {
                fold.turns()
                    .iter()
                    .filter(|turn| !matches!(turn.phase, Phase::Finished { .. }))
                    .map(|turn| turn.id.clone())
                    .collect()
            })
            .await;
        Ok(Arc::new_cyclic(|this| Self {
            answered: Atom::default(),
            deferred_settles: Atom::new(false),
            dispatched: Atom::default(),
            durable_settles: Atom::new(false),
            effects: Atom::new(Effects::builtin().merged(policy.effects)),
            live: Atom::default(),
            log,
            owed: Atom::new(false),
            owners: Atom::default(),
            recovering: Atom::new(recovering),
            remembered: Atom::default(),
            resolved: Atom::default(),
            restarts: policy.restarts,
            retired_max: policy.retired_max,
            this: Weak::clone(this),
            unsaved: Atom::default(),
            wake_index: Atom::new(None),
            wakes_max: policy.wakes_max,
        }))
    }

    #[must_use]
    pub fn scope(&self) -> &str {
        self.log.scope()
    }

    fn weak(&self) -> Weak<Self> {
        Weak::clone(&self.this)
    }

    pub fn settle_durably(&self) {
        self.durable_settles.set(true);
    }

    #[must_use]
    pub fn barrier(self: &Arc<Self>) -> Arc<dyn RemoteBarrier> {
        self.deferred_settles.set(true);
        Arc::clone(self) as Arc<dyn RemoteBarrier>
    }

    pub async fn settle_deferred(&self) -> Result<(), StoreError> {
        let this = self.weak();
        self.log
            .write_within(move |writing| {
                Box::pin(async move {
                    let this = upgraded(&this);
                    if *this.owed.get() && writing.buffered() > 0 {
                        writing.commit(Point::Progress).await?;
                    } else {
                        writing.unride();
                    }
                    this.owed.set(false);
                    Ok(())
                })
            })
            .await
    }

    async fn carried(&self, carried: Carried) -> Result<bool, StoreError> {
        let this = self.weak();
        self.log
            .write_within(move |writing| {
                Box::pin(async move {
                    let this = upgraded(&this);
                    if !*this.owed.get() || writing.buffered() == 0 {
                        this.owed.set(false);
                        return Ok(false);
                    }
                    let statements = carried
                        .statements
                        .iter()
                        .map(|(sql, params)| SqlStatement {
                            sql,
                            params: params.iter().map(String::as_str).collect(),
                        })
                        .collect::<Vec<_>>();
                    let rider = Rider {
                        host: carried.host,
                        statements: &statements,
                    };
                    let (_, carried) = writing.commit_carrying(Point::Progress, rider).await?;
                    this.owed.set(false);
                    Ok(carried)
                })
            })
            .await
    }

    pub fn declare(&self, tool: impl Into<String>, class: EffectClass) {
        let tool = tool.into();
        self.effects
            .update(|effects| effects.insert(tool.clone(), class));
    }

    #[must_use]
    pub fn effect_of(&self, tool: &str) -> EffectClass {
        self.effects.read(|effects| effects.of(tool))
    }

    pub async fn stats(&self) -> Stats {
        self.log.stats().await
    }

    pub async fn reclaim(&self) -> Result<(), StoreError> {
        self.log.reclaim().await
    }

    #[must_use]
    pub fn remembered_bytes(&self) -> usize {
        self.remembered.read(|remembered| {
            remembered
                .values()
                .map(|snapshot| snapshot.state.get().len())
                .sum()
        })
    }

    pub async fn read<R>(&self, read: impl FnOnce(&Fold) -> R + Send + 'static) -> R
    where
        R: Send + 'static,
    {
        self.log.read(read).await
    }

    #[must_use]
    pub fn store(self: &Arc<Self>) -> OneLogStore {
        OneLogStore(Arc::clone(self))
    }

    #[must_use]
    pub fn ledger(self: &Arc<Self>) -> Arc<dyn HoldLedger> {
        Arc::clone(self) as Arc<dyn HoldLedger>
    }

    pub async fn uncertain_holds(&self) -> Vec<String> {
        let resolved = self.resolved.get();
        self.log
            .read(move |fold| {
                fold.holds()
                    .filter(|(key, hold)| {
                        matches!(hold, Hold::Running { .. }) && !resolved.contains(*key)
                    })
                    .map(|(key, _)| key.to_owned())
                    .collect()
            })
            .await
    }

    async fn written<R, E, J>(&self, failed: fn(StoreError) -> E, job: J) -> Result<R, E>
    where
        R: Send + 'static,
        E: Send + 'static,
        J: for<'a> FnOnce(&'a mut Writing) -> StoreFuture<'a, Result<R, E>> + Send + 'static,
    {
        self.log
            .write_within(move |writing| {
                let work = job(writing);
                Box::pin(async move { Ok::<_, StoreError>(work.await) })
            })
            .await
            .map_err(failed)?
    }

    async fn journal_hold<T: Send + 'static>(
        &self,
        key: &str,
        next: impl FnOnce(Option<&Hold>) -> Result<(Option<Hold>, T), LedgerError> + Send + 'static,
    ) -> Result<T, LedgerError> {
        let journal = |error: StoreError| LedgerError::Journal(error.to_string());
        let key = key.to_owned();
        self.written(journal, move |writing| {
            Box::pin(async move {
                let (hold, decided) = next(writing.fold().hold(&key))?;
                let Some(hold) = hold else {
                    return Ok(decided);
                };
                writing.push(Record::Hold { hold, key }).map_err(journal)?;
                writing.commit(Point::Call).await.map_err(journal)?;
                Ok(decided)
            })
        })
        .await
    }

    #[must_use]
    pub fn gate(self: &Arc<Self>) -> Arc<dyn EffectGate> {
        Arc::clone(self) as Arc<dyn EffectGate>
    }

    #[must_use]
    pub fn sink(self: &Arc<Self>) -> Arc<dyn CellJournalSink> {
        Arc::new(OneLogJournals(Arc::clone(self)))
    }

    pub async fn journals(self: &Arc<Self>) -> Result<CellJournals, StoreError> {
        Ok(CellJournals::with_sink(self.sink()))
    }

    async fn cells(
        &self,
        only: &str,
        cell: Option<u64>,
    ) -> Result<BTreeMap<String, Vec<CellJournal>>, StoreError> {
        let only = only.to_owned();
        self.log
            .write_within(move |writing| {
                Box::pin(async move {
                    let only = only.as_str();
                    writing.load_owners(&[only.to_owned()]).await?;
                    let mut whole = BTreeSet::new();
                    let mut shared = Vec::new();
                    let mut catalogs = BTreeSet::new();
                    for (session, id, updates) in writing.fold().cells_of(Some(only)) {
                        if cell.is_some_and(|cell| cell != id) {
                            continue;
                        }
                        for update in updates {
                            if let Some(digest) = shared::catalog(update, TOOLS_MARK)? {
                                catalogs.insert(stored_key(TOOLS_MARK, &digest));
                            }
                        }
                        let wanted = match cell {
                            Some(cell) => id == cell,
                            None => !shared::finished(updates)?,
                        };
                        if wanted {
                            whole.insert(id);
                            shared.extend(shared::result_keys(session, updates)?);
                        }
                    }
                    let mut wanted = shared.clone();
                    wanted.extend(catalogs);
                    writing.load_owners(&wanted).await?;
                    let fold = writing.fold();
                    restored(fold, only, &whole, &shared);
                    let mut sessions: BTreeMap<String, Vec<CellJournal>> = BTreeMap::new();
                    let mut catalogs: HashMap<String, Arc<[Value]>> = HashMap::new();
                    for (session, id, updates) in fold.cells_of(Some(only)) {
                        if cell.is_some_and(|cell| cell != id) {
                            continue;
                        }
                        let loaded = whole.contains(&id);
                        let mut decoded = Vec::with_capacity(updates.len());
                        for update in updates {
                            let update = if loaded {
                                shared::with_results(fold, session, update)?
                            } else {
                                shared::without_results(update)?
                            };
                            let mut update = serde_json::from_str::<CellJournalUpdate>(&update)
                                .map_err(|error| {
                                    invalid(format!("cell journal update: {error}"))
                                })?;
                            if let Some(header) = update.header.as_mut()
                                && let [Value::Object(mark)] = &*header.tools
                                && let Some(digest) = mark.get(TOOLS_MARK).and_then(Value::as_str)
                            {
                                let tools = match catalogs.get(digest) {
                                    Some(tools) => Arc::clone(tools),
                                    None => {
                                        let catalog = fold
                                            .stored(&stored_key(TOOLS_MARK, digest))
                                            .ok_or_else(|| {
                                                invalid(format!("tool catalog {digest}"))
                                            })?;
                                        let tools: Arc<[Value]> =
                                            serde_json::from_str::<Vec<Value>>(catalog)
                                                .map_err(|error| {
                                                    invalid(format!("tool catalog: {error}"))
                                                })?
                                                .into();
                                        catalogs.insert(digest.to_owned(), Arc::clone(&tools));
                                        tools
                                    }
                                };
                                header.tools = tools;
                            }
                            decoded.push(update);
                        }
                        if let Some(mut journal) = CellJournal::fold(decoded) {
                            journal.unloaded = !loaded;
                            sessions
                                .entry(session.to_owned())
                                .or_default()
                                .push(journal);
                        }
                    }
                    Ok(sessions)
                })
            })
            .await
    }

    async fn whole(
        &self,
        state: &str,
        keys: &[String],
        values: Vec<Option<String>>,
    ) -> Result<Vec<Option<String>>, StoreError> {
        let owners = shared::spliced_keys(
            state,
            keys.iter()
                .zip(&values)
                .filter_map(|(key, value)| Some((key.as_str(), value.as_deref()?))),
        )?;
        if owners.is_empty() {
            return Ok(values);
        }
        let state = state.to_owned();
        let keys = keys.to_vec();
        self.log
            .read_owners(&owners, move |fold| {
                keys.iter()
                    .zip(values)
                    .map(|(key, value)| {
                        value
                            .map(|value| shared::whole(fold, &state, key, value))
                            .transpose()
                    })
                    .collect()
            })
            .await?
    }

    pub async fn field(&self, key: &str, value: Box<RawValue>) -> Result<(), StoreError> {
        let key = key.to_owned();
        self.log
            .write_within(move |writing| {
                Box::pin(async move {
                    let key = key.as_str();
                    put_host(writing, key, value)
                })
            })
            .await
    }

    pub async fn effect(&self, key: &str, value: Box<RawValue>) -> Result<u64, StoreError> {
        let key = key.to_owned();
        self.log
            .write_within(move |writing| {
                Box::pin(async move {
                    let key = key.as_str();
                    put_host(writing, key, value)?;
                    writing.commit(Point::Call).await
                })
            })
            .await
    }

    pub async fn submit(&self, submission: &Submission) -> Result<Submitted, StoreError> {
        self.admit(submission, true).await
    }

    pub async fn submit_buffered(&self, submission: &Submission) -> Result<Submitted, StoreError> {
        self.admit(submission, false).await
    }

    async fn admit(&self, submission: &Submission, durable: bool) -> Result<Submitted, StoreError> {
        let id = submission.request_id.clone();
        let digest = submission_digest(&submission.prompt, submission.hand.as_deref())?;
        let prompt = raw(&submission.prompt)?;
        let derived = input_digest(&prompt, submission.hand.as_deref());
        let this = self.weak();
        let submission = submission.clone();
        let admitted = self
            .log
            .write_within(move |writing| {
                Box::pin(async move {
                    let this = upgraded(&this);
                    this.admitted(writing, &submission, digest, prompt, &derived, durable)
                        .await
                })
            })
            .await?;
        let same = match admitted {
            Admission::Done(submitted) => return Ok(submitted),
            Admission::Retired { same } => same,
        };
        let record = self
            .retired_turn(&id)
            .await?
            .ok_or_else(|| invalid(format!("retired turn {id} is missing")))?;
        Ok(if same {
            Submitted::Existing(record)
        } else {
            Submitted::Conflict(record)
        })
    }

    async fn admitted(
        &self,
        writing: &mut Writing,
        submission: &Submission,
        digest: String,
        prompt: Box<RawValue>,
        derived: &str,
        durable: bool,
    ) -> Result<Admission, StoreError> {
        let id = submission.request_id.as_str();
        if let Some(turn) = writing.fold().turn(id) {
            let record = record_of(turn)?;
            return Ok(Admission::Done(
                if turn.digest == digest || turn.digest == derived {
                    Submitted::Existing(record)
                } else {
                    Submitted::Conflict(record)
                },
            ));
        }
        if let Some(same) = writing
            .fold()
            .retired(id)
            .map(|old| old.digest == tag(&digest) || old.digest == tag(derived))
        {
            return Ok(Admission::Retired { same });
        }
        let retired = writing.fold().retired_turns();
        if let Some(max) = self.retired_max
            && retired >= max
        {
            return Err(StoreError::NotCommitted(format!(
                "turn {id} refused: this log already holds {retired} finished turns, the most one log keeps ({max}); start a new thread"
            )));
        }
        if !durable {
            writing.secure().await?;
        }
        let first = writing.next_event();
        let at = now();
        let body = raw(&Lifecycle {
            kind: "queued",
            request_id: id,
        })?;
        if facts::sim::mutated(facts::sim::Mutation::AdmissionCopied) {
            writing.push(Record::Admitted {
                digest: digest.clone(),
                first,
                hand: submission.hand.clone(),
                metadata: submission.metadata.clone(),
                prompt: prompt.clone(),
                turn: id.to_owned(),
            })?;
        }
        put_fact(
            writing,
            &Fact::Input(Input {
                turn: id.to_owned(),
                kind: InputKind::Prompt,
                value: prompt,
                metadata: submission.metadata.clone(),
                hand: submission.hand.clone(),
                first: Some(first),
            }),
        )?;
        writing.push(Record::Event {
            at,
            event: body.clone(),
            seq: first,
            turn: Some(id.to_owned()),
        })?;
        if durable && let Err(error) = writing.commit(Point::Admission).await {
            writing.unadmit(id, first);
            return Err(error);
        }
        let record = record_of(
            writing
                .fold()
                .turn(id)
                .ok_or_else(|| invalid(format!("admitted turn {id} is missing")))?,
        )?;
        Ok(Admission::Done(Submitted::Accepted {
            record,
            event: logged(first, Some(id), at, body),
        }))
    }

    pub async fn append(&self, entries: &[LogEntry]) -> Result<Vec<LoggedEvent>, StoreError> {
        let entries = entries.to_vec();
        self.log
            .write_within(move |writing| {
                Box::pin(async move {
                    let entries = entries.as_slice();
                    writing.secure().await?;
                    let mut events = Vec::with_capacity(entries.len());
                    for entry in entries {
                        let seq = writing.next_event();
                        let at = now();
                        writing.push(Record::Event {
                            at,
                            event: entry.event.clone(),
                            seq,
                            turn: entry.request_id.clone(),
                        })?;
                        events.push(logged(
                            seq,
                            entry.request_id.as_deref(),
                            at,
                            entry.event.clone(),
                        ));
                    }
                    Ok(events)
                })
            })
            .await
    }

    pub async fn turn(&self, request_id: &str) -> Result<Option<TurnRecord>, StoreError> {
        let id = request_id.to_owned();
        let held = self
            .log
            .read(move |fold| fold.turn(&id).map(record_of).transpose())
            .await?;
        match held {
            Some(record) => Ok(Some(record)),
            None => self.retired_turn(request_id).await,
        }
    }

    async fn retired_turn(&self, request_id: &str) -> Result<Option<TurnRecord>, StoreError> {
        let id = request_id.to_owned();
        let Some(retired) = self.log.read(move |fold| fold.retired(&id).cloned()).await else {
            return Ok(None);
        };
        let last = retired.last;
        let result = self
            .log
            .history(last.saturating_sub(1), None, move |result, event| {
                if event.seq == last {
                    *result = Some(event.event.clone());
                }
                event.seq < last
            })
            .await?;
        retired_record(request_id, &retired, result).map(Some)
    }

    pub async fn pending(&self) -> Result<Vec<TurnRecord>, StoreError> {
        self.log
            .read(|fold| {
                fold.turns()
                    .iter()
                    .filter(|turn| !matches!(turn.phase, Phase::Finished { .. }))
                    .map(record_of)
                    .collect()
            })
            .await
    }

    async fn close(
        &self,
        writing: &mut Writing,
        request_id: &str,
        state: TurnState,
        fields: &Map<String, Value>,
    ) -> Result<(TurnRecord, LoggedEvent), StoreError> {
        let Some(outcome) = outcome(state) else {
            return Err(StoreError::NotCommitted(format!(
                "a turn cannot finish as {}",
                state.as_str()
            )));
        };
        settle_stopped(writing, request_id)?;
        let body = raw(&Finished {
            kind: "result",
            request_id,
            status: state.as_str(),
            fields,
        })?;
        let mut finished = writing
            .fold()
            .turn(request_id)
            .cloned()
            .ok_or_else(|| invalid(format!("finishing turn {request_id} is missing")))?;
        let last = writing.next_event();
        let at = now();
        writing.push(Record::Event {
            at,
            event: body.clone(),
            seq: last,
            turn: Some(request_id.to_owned()),
        })?;
        put_fact(
            writing,
            &status_fact(
                request_id,
                TurnStatus::Finished {
                    last,
                    outcome: fact_outcome(outcome),
                    result: body.clone(),
                },
            ),
        )?;
        finished.phase = Phase::Finished {
            last,
            outcome,
            result: body.clone(),
        };
        writing.commit(Point::Finish).await?;
        self.recovering.update(|turns| turns.remove(request_id));
        Ok((
            record_of(&finished)?,
            logged(last, Some(request_id), at, body),
        ))
    }

    pub async fn start(&self, request_id: &str) -> Result<Option<Started>, StoreError> {
        if self.recovering.read(|turns| turns.contains(request_id)) {
            self.settle_intents(request_id).await?;
        }
        let this = self.weak();
        let request_id = request_id.to_owned();
        self.log
            .write_within(move |writing| {
                Box::pin(async move {
                    let this = upgraded(&this);
                    let request_id = request_id.as_str();
                    let Some(turn) = writing.fold().turn(request_id) else {
                        return Ok(None);
                    };
                    let previous = match turn.phase {
                        Phase::Finished { .. } => return Ok(None),
                        Phase::Queued { .. } => TurnState::Queued,
                        Phase::Running { .. } => TurnState::Running,
                    };
                    let recovering = this.recovering.read(|turns| turns.contains(request_id));
                    if recovering {
                        let attempt = writing.fold().next_attempt(request_id);
                        if attempt > this.restarts {
                            let fields = Map::from_iter([(
                                "error".to_owned(),
                                Value::String(format!(
                                    "the turn restarted {} times without progress",
                                    attempt - 1
                                )),
                            )]);
                            this.close(writing, request_id, TurnState::Failed, &fields)
                                .await?;
                            return Ok(None);
                        }
                        let fold = writing.fold();
                        let unknown = fold
                            .unknown()
                            .into_iter()
                            .filter(|call_id| fold.intent(call_id).is_none())
                            .collect::<Vec<_>>();
                        if unknown.is_empty() {
                            put_fact(
                                writing,
                                &Fact::Marker(Marker::Recovery {
                                    turn: request_id.to_owned(),
                                    attempt,
                                    at: None,
                                }),
                            )?;
                        } else {
                            writing.push(Record::Recovery {
                                at: None,
                                attempt,
                                turn: request_id.to_owned(),
                                unknown,
                            })?;
                        }
                    } else {
                        writing.secure().await?;
                    }
                    let seq = writing.next_event();
                    let at = now();
                    let body = raw(&Lifecycle {
                        kind: "running",
                        request_id,
                    })?;
                    put_fact(writing, &status_fact(request_id, TurnStatus::Running))?;
                    writing.push(Record::Event {
                        at,
                        event: body.clone(),
                        seq,
                        turn: Some(request_id.to_owned()),
                    })?;
                    if recovering {
                        writing.commit(Point::Recovery).await?;
                        this.recovering.update(|turns| turns.remove(request_id));
                    }
                    let record = record_of(writing.fold().turn(request_id).ok_or_else(|| {
                        invalid(format!("started turn {request_id} is missing"))
                    })?)?;
                    Ok(Some(Started {
                        previous,
                        record,
                        event: logged(seq, Some(request_id), at, body),
                    }))
                })
            })
            .await
    }

    pub async fn finish(
        &self,
        request_id: &str,
        state: TurnState,
        fields: &Map<String, Value>,
    ) -> Result<Option<(TurnRecord, LoggedEvent)>, StoreError> {
        let this = self.weak();
        let request_id = request_id.to_owned();
        let fields = fields.clone();
        self.log
            .write_within(move |writing| {
                Box::pin(async move {
                    let this = upgraded(&this);
                    let request_id = request_id.as_str();
                    let fields = &fields;
                    match writing.fold().turn(request_id).map(|turn| &turn.phase) {
                        None | Some(Phase::Finished { .. }) => Ok(None),
                        Some(_) => this
                            .close(writing, request_id, state, fields)
                            .await
                            .map(Some),
                    }
                })
            })
            .await
    }

    pub async fn cancel(
        &self,
        request_id: &str,
        fields: &Map<String, Value>,
    ) -> Result<Option<(TurnRecord, Option<LoggedEvent>)>, StoreError> {
        let this = self.weak();
        let id = request_id.to_owned();
        let fields = fields.clone();
        let held = self
            .log
            .write_within(move |writing| {
                Box::pin(async move {
                    let this = upgraded(&this);
                    this.cancelled(writing, &id, &fields).await
                })
            })
            .await?;
        if let Some(cancelled) = held {
            return Ok(cancelled);
        }
        let retired = self.retired_turn(request_id).await?;
        Ok(retired.map(|record| (record, None)))
    }

    async fn cancelled(
        &self,
        writing: &mut Writing,
        request_id: &str,
        fields: &Map<String, Value>,
    ) -> Result<Option<Option<(TurnRecord, Option<LoggedEvent>)>>, StoreError> {
        let Some(turn) = writing.fold().turn(request_id) else {
            return Ok(None);
        };
        match turn.phase {
            Phase::Running {
                cancelling: false, ..
            } => {
                settle_stopped(writing, request_id)?;
                put_fact(writing, &status_fact(request_id, TurnStatus::Cancelling))?;
                writing.commit(Point::Cancel).await?;
                let record = writing.fold().turn(request_id).map(record_of).transpose()?;
                Ok(Some(record.map(|record| (record, None))))
            }
            Phase::Queued { .. } => {
                let (record, event) = self
                    .close(writing, request_id, TurnState::Cancelled, fields)
                    .await?;
                Ok(Some(Some((record, Some(event)))))
            }
            _ => Ok(Some(Some((record_of(turn)?, None)))),
        }
    }

    pub async fn withdraw_steered(
        &self,
        request_id: &str,
        digest: &str,
    ) -> Result<Option<(TurnRecord, LoggedEvent)>, StoreError> {
        let this = self.weak();
        let request_id = request_id.to_owned();
        let digest = digest.to_owned();
        self.log
            .write_within(move |writing| {
                Box::pin(async move {
                    let this = upgraded(&this);
                    let request_id = request_id.as_str();
                    let digest = digest.as_str();
                    let queued = writing.fold().turn(request_id).is_some_and(|turn| {
                        matches!(turn.phase, Phase::Queued { .. }) && turn.digest == digest
                    });
                    if !queued {
                        return Ok(None);
                    }
                    let fields = Map::from_iter([
                        ("cancelled".to_owned(), Value::Bool(true)),
                        ("steered".to_owned(), Value::Bool(true)),
                    ]);
                    this.close(writing, request_id, TurnState::Cancelled, &fields)
                        .await
                        .map(Some)
                })
            })
            .await
    }

    pub async fn interrupted(&self, request_id: &str, note: &str) -> Result<(), StoreError> {
        let request_id = request_id.to_owned();
        let note = note.to_owned();
        self.log
            .write_within(move |writing| {
                Box::pin(async move {
                    let request_id = request_id.as_str();
                    let note = note.as_str();
                    let running = writing
                        .fold()
                        .turn(request_id)
                        .is_some_and(|turn| matches!(turn.phase, Phase::Running { .. }));
                    if !running {
                        return Ok(());
                    }
                    settle_stopped(writing, request_id)?;
                    put_fact(
                        writing,
                        &status_fact(
                            request_id,
                            TurnStatus::Interrupted {
                                note: note.to_owned(),
                            },
                        ),
                    )
                    .map(drop)
                })
            })
            .await
    }

    pub async fn retry(
        &self,
        request_id: &str,
        error: &str,
        kind: &str,
        retry_at: u64,
    ) -> Result<Option<(TurnRecord, Option<LoggedEvent>)>, StoreError> {
        let id = request_id.to_owned();
        let error = error.to_owned();
        let kind = kind.to_owned();
        let held = self
            .log
            .write_within(move |writing| {
                Box::pin(async move { retried(writing, &id, &error, &kind, retry_at).await })
            })
            .await?;
        if let Some(retried) = held {
            return Ok(retried);
        }
        let retired = self.retired_turn(request_id).await?;
        Ok(retired.map(|record| (record, None)))
    }

    pub async fn import(&self, turns: &[ImportedTurn]) -> Result<usize, StoreError> {
        if turns.is_empty() {
            return Ok(0);
        }
        let turns = turns.to_vec();
        self.log
            .write_within(move |writing| {
                Box::pin(async move {
                    let turns = turns.as_slice();
                    let mut imported = false;
                    let mut highest = 0;
                    for turn in turns {
                        let fold = writing.fold();
                        if fold.turn(&turn.request_id).is_some()
                            || fold.retired(&turn.request_id).is_some()
                        {
                            continue;
                        }
                        let id = turn.request_id.as_str();
                        let first = turn.first_seq.unwrap_or(0);
                        writing.push(Record::Admitted {
                            digest: submission_digest(&turn.prompt, turn.hand.as_deref())?,
                            first,
                            hand: turn.hand.clone(),
                            metadata: turn.metadata.clone(),
                            prompt: raw(&turn.prompt)?,
                            turn: id.to_owned(),
                        })?;
                        let status = |status| Record::Status {
                            status,
                            turn: id.to_owned(),
                        };
                        match outcome(turn.state) {
                            Some(outcome) => {
                                let result = raw(&Finished {
                                    kind: "result",
                                    request_id: id,
                                    status: turn.state.as_str(),
                                    fields: turn.result.as_ref().unwrap_or(&Map::new()),
                                })?;
                                let last = match turn.last_seq {
                                    Some(last) => last,
                                    None => {
                                        let last = writing.next_event();
                                        writing.push(Record::Event {
                                            at: now(),
                                            event: result.clone(),
                                            seq: last,
                                            turn: Some(id.to_owned()),
                                        })?;
                                        last
                                    }
                                };
                                highest = highest.max(last);
                                writing.push(status(Status::Finished {
                                    last,
                                    outcome,
                                    result,
                                }))?;
                            }
                            None if turn.state == TurnState::Running => {
                                writing.push(status(Status::Running))?;
                                if turn.cancel_requested {
                                    writing.push(status(Status::Cancelling))?;
                                }
                                if let Some(note) = &turn.note {
                                    writing
                                        .push(status(Status::Interrupted { note: note.clone() }))?;
                                }
                            }
                            None => {}
                        }
                        highest = highest.max(first);
                        imported = true;
                    }
                    if highest > writing.fold().last_event() {
                        writing.push(Record::Floor { seq: highest })?;
                    }
                    if imported {
                        writing.commit(Point::Admission).await?;
                    }
                    Ok(turns.len())
                })
            })
            .await
    }

    pub async fn import_events(
        &self,
        after: u64,
        events: &[LoggedEvent],
    ) -> Result<u64, StoreError> {
        let Some(last) = events.last().map(|event| event.seq) else {
            return Ok(after);
        };
        let mut previous = after;
        for event in events {
            if event.seq <= previous {
                return Err(StoreError::NotCommitted(format!(
                    "imported session log events must ascend after seq {previous}, but seq {} follows it",
                    event.seq
                )));
            }
            previous = event.seq;
        }
        let this = self.weak();
        let events = events.to_vec();
        self.log.write_within(move |writing| Box::pin(async move {
let this = upgraded(&this);
let events = events.as_slice();
        let ends = writing.fold().ends();
        if ends < after {
            return Err(StoreError::NotCommitted(format!(
                "turn log {} ends at seq {ends}, before the seq {after} the imported events follow",
                this.scope()
            )));
        }
        let mut held = Vec::new();
        writing
            .held(after, |event| {
                held.push(event.clone());
                true
            })
            .await?;
        for event in &held {
            match events.iter().find(|import| import.seq == event.seq) {
                None => {
                    return Err(StoreError::NotCommitted(format!(
                        "turn log {} holds event {}, which the imported events after seq {after} do not",
                        this.scope(),
                        event.seq
                    )));
                }
                Some(import)
                    if import.request_id != event.turn
                        || import.created_at != event.at
                        || import.event.get() != event.event.get() =>
                {
                    return Err(StoreError::NotCommitted(format!(
                        "turn log {} already holds a different event at seq {}",
                        this.scope(),
                        event.seq
                    )));
                }
                Some(_) => {}
            }
        }
        let fresh = &events[events.partition_point(|event| event.seq <= ends)..];
        if fresh.is_empty() {
            return Ok(last);
        }
        for event in fresh {
            writing.push(Record::Event {
                at: event.created_at,
                event: event.event.clone(),
                seq: event.seq,
                turn: event.request_id.clone(),
            })?;
        }
        writing.commit(Point::Admission).await?;
        Ok(last)
}))
.await
    }

    pub async fn replace_queued(
        &self,
        request_id: &str,
        edit_id: &str,
        text: &str,
    ) -> Result<Replaced, StoreError> {
        self.edit_queued(request_id, edit_id, text, false)
            .await?
            .ready(request_id)
    }

    async fn edit_queued(
        &self,
        request_id: &str,
        edit_id: &str,
        text: &str,
        steering: bool,
    ) -> Result<Guarded<Replaced>, StoreError> {
        if edit_id.is_empty() {
            return Err(StoreError::NotCommitted(
                "an edit ID must not be empty".into(),
            ));
        }
        let edit = submission_digest(&Prompt::new(text), None)?;
        let request = raw(&Prompt::new(text))?;
        let derived = input_digest(&request, None);
        let request_id = request_id.to_owned();
        let edit_id = edit_id.to_owned();
        let text = text.to_owned();
        self.log
            .write_within(move |writing| {
                Box::pin(async move {
                    let request_id = request_id.as_str();
                    let edit_id = edit_id.as_str();
                    let text = text.as_str();
                    let Some(turn) = writing.fold().turn(request_id) else {
                        return Ok(Guarded::Ready(Replaced::NotQueued));
                    };
                    if let Some(applied) = turn.edits.get(edit_id) {
                        return Ok(Guarded::Ready(if *applied == edit || *applied == derived {
                            Replaced::Applied(None)
                        } else {
                            Replaced::Conflict
                        }));
                    }
                    if steering {
                        return Ok(Guarded::Steering);
                    }
                    if !matches!(turn.phase, Phase::Queued { .. }) {
                        return Ok(Guarded::Ready(Replaced::NotQueued));
                    }
                    let prompt =
                        serde_json::from_str::<Prompt>(turn.prompt.get()).map_err(|error| {
                            invalid(format!("prompt of turn {request_id}: {error}"))
                        })?;
                    let prompt = edited(&prompt, text)?;
                    let body = raw(&Replacement {
                        digest: &edit,
                        edit_id,
                        kind: "replaced",
                        request_id,
                    })?;
                    let seq = writing.next_event();
                    let at = now();
                    writing.push(Record::Event {
                        at,
                        event: body.clone(),
                        seq,
                        turn: Some(request_id.to_owned()),
                    })?;
                    put_fact(
                        writing,
                        &Fact::Input(Input {
                            turn: request_id.to_owned(),
                            kind: InputKind::Edit {
                                edit_id: edit_id.to_owned(),
                            },
                            value: raw(&prompt)?,
                            metadata: request,
                            hand: None,
                            first: None,
                        }),
                    )?;
                    writing.commit(Point::Admission).await?;
                    Ok(Guarded::Ready(Replaced::Applied(Some(logged(
                        seq,
                        Some(request_id),
                        at,
                        body,
                    )))))
                })
            })
            .await
    }

    pub async fn page(&self, after: u64, limit: usize) -> Result<LogPage, StoreError> {
        self.paged(None, after, limit, None).await
    }

    pub async fn page_bounded(
        &self,
        after: u64,
        limit: usize,
        max_bytes: usize,
    ) -> Result<LogPage, StoreError> {
        self.paged(None, after, limit, Some(max_bytes)).await
    }

    pub async fn turn_page(
        &self,
        request_id: &str,
        after: u64,
        limit: usize,
    ) -> Result<LogPage, StoreError> {
        self.paged(Some(request_id), after, limit, None).await
    }

    pub async fn turn_page_bounded(
        &self,
        request_id: &str,
        after: u64,
        limit: usize,
        max_bytes: usize,
    ) -> Result<LogPage, StoreError> {
        self.paged(Some(request_id), after, limit, Some(max_bytes))
            .await
    }

    async fn paged(
        &self,
        request_id: Option<&str>,
        after: u64,
        limit: usize,
        max_bytes: Option<usize>,
    ) -> Result<LogPage, StoreError> {
        let start = match request_id {
            Some(id) => {
                let id = id.to_owned();
                self.log
                    .read(move |fold| {
                        fold.turn(&id)
                            .map(|turn| turn.first)
                            .or_else(|| fold.retired(&id).map(|retired| retired.first))
                            .map_or(after, |first| after.max(first.saturating_sub(1)))
                    })
                    .await
            }
            None => after,
        };
        let wanted = request_id.map(str::to_owned);
        let (events, _, has_more) = self
            .log
            .history(
                start,
                (Vec::new(), 0, false),
                move |(events, bytes, has_more): &mut (Vec<LoggedEvent>, usize, bool), event| {
                    if wanted
                        .as_deref()
                        .is_some_and(|id| event.turn.as_deref() != Some(id))
                    {
                        return true;
                    }
                    let size = event.event.get().len();
                    if events.len() == limit
                        || max_bytes.is_some_and(|max| !events.is_empty() && *bytes + size > max)
                    {
                        *has_more = true;
                        return false;
                    }
                    *bytes += size;
                    events.push(logged(
                        event.seq,
                        event.turn.as_deref(),
                        event.at,
                        event.event.clone(),
                    ));
                    true
                },
            )
            .await?;
        Ok(LogPage {
            next_cursor: events.last().map_or(after, |event| event.seq),
            has_more,
            events,
        })
    }

    async fn recorded(&self, recording: Recording<'_>) -> Result<Vec<LoggedEvent>, String> {
        let encoded = encoded(&recording)?;
        self.written(
            |error| error.to_string(),
            move |writing| {
                Box::pin(async move {
                    writing.secure().await.map_err(|error| error.to_string())?;
                    let mut logged_events = Vec::with_capacity(encoded.len());
                    for (stored, live, request_id) in encoded {
                        let Some(stored) = stored else {
                            logged_events.push(logged(0, request_id.as_deref(), now(), live));
                            continue;
                        };
                        let seq = writing.next_event();
                        let at = now();
                        writing
                            .push(Record::Event {
                                at,
                                event: stored,
                                seq,
                                turn: request_id.clone(),
                            })
                            .map_err(|error| error.to_string())?;
                        logged_events.push(logged(seq, request_id.as_deref(), at, live));
                    }
                    Ok(logged_events)
                })
            },
        )
        .await
    }
}

impl EventLog for OneLog {
    fn record<'a>(
        &'a self,
        recording: Recording<'a>,
    ) -> HostFuture<'a, Result<Vec<LoggedEvent>, String>> {
        Box::pin(self.recorded(recording))
    }

    fn flush(&self) -> Logged<'_, ()> {
        Box::pin(self.settle_deferred())
    }
}

impl TurnLog for OneLog {
    fn turn<'a>(&'a self, request_id: &'a str) -> Logged<'a, Option<TurnRecord>> {
        Box::pin(Self::turn(self, request_id))
    }

    fn pending(&self) -> Logged<'_, Vec<TurnRecord>> {
        Box::pin(Self::pending(self))
    }

    fn start<'a>(&'a self, request_id: &'a str) -> Logged<'a, Option<Started>> {
        Box::pin(Self::start(self, request_id))
    }

    fn finish<'a>(
        &'a self,
        request_id: &'a str,
        state: TurnState,
        fields: &'a Map<String, Value>,
    ) -> Logged<'a, Option<(TurnRecord, LoggedEvent)>> {
        Box::pin(Self::finish(self, request_id, state, fields))
    }

    fn cancel<'a>(
        &'a self,
        request_id: &'a str,
        fields: &'a Map<String, Value>,
    ) -> Logged<'a, Option<(TurnRecord, Option<LoggedEvent>)>> {
        Box::pin(Self::cancel(self, request_id, fields))
    }

    fn withdraw_steered<'a>(
        &'a self,
        request_id: &'a str,
        digest: &'a str,
    ) -> Logged<'a, Option<(TurnRecord, LoggedEvent)>> {
        Box::pin(Self::withdraw_steered(self, request_id, digest))
    }

    fn replace_queued<'a>(
        &'a self,
        request_id: &'a str,
        edit_id: &'a str,
        text: &'a str,
        steering: bool,
    ) -> Logged<'a, Guarded<Replaced>> {
        Box::pin(self.edit_queued(request_id, edit_id, text, steering))
    }
}

pub(crate) fn tool_input(input: &ToolInput) -> Result<Box<RawValue>, StoreError> {
    match input {
        ToolInput::Function(input) => Ok(input.clone()),
        ToolInput::Freeform(input) => raw(input),
    }
}

impl EffectGate for OneLog {
    fn effect(&self, tool: &str) -> EffectClass {
        self.effect_of(tool)
    }

    fn before<'a>(&'a self, call: EffectCall<'a>) -> HostFuture<'a, Result<(), String>> {
        Box::pin(async move {
            let input = tool_input(call.input).map_err(|error| error.to_string())?;
            let this = self.weak();
            let call_id = call.call_id.to_owned();
            let tool = call.tool.to_owned();
            let session_id = call.session_id.to_owned();
            let effect = call.effect;
            let turn = call.turn_id.map(str::to_owned);
            self.written(
                |error| error.to_string(),
                move |writing| {
                    Box::pin(async move {
                        let this = upgraded(&this);
            match facts::intended(writing.fold(), call_id.as_str()) {
                Some(intended)
                    if intended != tool.as_str()
                        && !thread_scope_sim::mutated(
                            thread_scope_sim::Mutation::GateComparesIntentToItself,
                        ) =>
                {
                    return Err(format!(
                        "call {} was intended for tool {intended}, not {}",
                        call_id.as_str(), tool.as_str()
                    ));
                }
                Some(_) => {
                    if writing.buffered() > 0 {
                        writing
                            .commit(Point::Call)
                            .await
                            .map_err(|error| error.to_string())?;
                    }
                    this.owed.set(false);
                    return Ok(());
                }
                None if writing.fold().logs_facts(session_id.as_str())
                    && !facts::sim::mutated(facts::sim::Mutation::IntentOptional) =>
                {
                    return Err(format!(
                        "call {} to {} has no durable intent, and session {} commits one before each external call",
                        call_id.as_str(), tool.as_str(), session_id.as_str()
                    ));
                }
                None => {}
            }
                        writing
                .push(Record::Call {
                    call_id: call_id.clone(),
                    effect,
                    input,
                    tool: tool.clone(),
                    turn,
                })
                .map_err(|error| error.to_string())?;
            writing
                .commit(Point::Call)
                .await
                .map_err(|error| error.to_string())?;
            this.owed.set(false);
            Ok(())
                    })
                },
            )
            .await
        })
    }

    fn settle(&self) -> HostFuture<'_, Result<(), String>> {
        Box::pin(async move {
            self.settle_deferred()
                .await
                .map_err(|error| error.to_string())
        })
    }

    fn abandon<'a>(
        &'a self,
        call_id: &'a str,
    ) -> HostFuture<'a, Result<Option<ToolOutput>, String>> {
        Box::pin(async move {
            self.abandoned(call_id)
                .await
                .map_err(|error| error.to_string())
        })
    }

    fn after<'a>(&'a self, call_id: &'a str, output: &'a ToolOutput) -> HostFuture<'a, ()> {
        Box::pin(async move {
            let Ok(output) = raw(&json!({"success": output.success})) else {
                return;
            };
            let id = call_id.to_owned();
            let recorded = self
                .log
                .write_within(move |writing| {
                    Box::pin(async move {
                        if facts::intended(writing.fold(), &id).is_some() {
                            return Ok(());
                        }
                        writing.push(Record::Result {
                            call_id: id,
                            output,
                        })
                    })
                })
                .await;
            if let Err(error) = recorded {
                nanocode_trace::warn!(%error, call_id, "turn log could not record a tool result");
            }
        })
    }
}

impl HoldLedger for OneLog {
    fn hold<'a>(
        &'a self,
        key: &'a str,
        call: HoldCall<'a>,
        card: &'a RawValue,
    ) -> HostFuture<'a, Result<(), LedgerError>> {
        let owned = key.to_owned();
        let call_id = call.call_id.to_owned();
        let card = card.to_owned();
        let input = call.input.to_owned();
        let tool = call.tool.to_owned();
        let turn = call.turn_id.map(str::to_owned);
        Box::pin(async move {
            self.journal_hold(key, move |existing| {
                let held = HeldCall {
                    call_id: &call_id,
                    card: &card,
                    input: &input,
                    tool: &tool,
                    turn: turn.as_deref(),
                };
                Hold::hold(existing, &owned, &held)
                    .map(|hold| (hold, ()))
                    .map_err(LedgerError::Hold)
            })
            .await
        })
    }

    fn resolve<'a>(
        &'a self,
        key: &'a str,
        answer: Answer,
    ) -> HostFuture<'a, Result<Resumption, LedgerError>> {
        Box::pin(async move {
            let owned = key.to_owned();
            let resumption = self
                .journal_hold(key, move |existing| {
                    let (hold, resumed) =
                        Hold::resolve(existing, &owned, answer).map_err(LedgerError::Hold)?;
                    Ok((Some(hold), resumed))
                })
                .await?;
            if matches!(resumption, Resumption::Run { .. }) {
                self.resolved.update(|resolved| {
                    resolved.insert(key.to_owned());
                });
            }
            Ok(resumption)
        })
    }

    fn running<'a>(&'a self, call_id: &'a str) -> HostFuture<'a, Option<Running>> {
        Box::pin(async move {
            let resolved = self.resolved.get();
            let call_id = call_id.to_owned();
            self.log
                .read(move |fold| {
                    let (key, hold) = fold.running_hold(&call_id)?;
                    let Hold::Running { input, .. } = hold else {
                        unreachable!("running_hold returns only running holds");
                    };
                    resolved.contains(key).then(|| Running {
                        input: input.clone(),
                        key: key.to_owned(),
                    })
                })
                .await
        })
    }

    fn claim<'a>(&'a self, key: &'a str) -> HostFuture<'a, bool> {
        // Taking the key out of `resolved` is the claim: whatever happens to the release after
        // this, no later call in this process can resume the hold again, and until the release is
        // saved `uncertain_holds` lists it.
        Box::pin(async move { self.resolved.update(|resolved| resolved.remove(key)) })
    }

    fn release<'a>(&'a self, key: &'a str) -> HostFuture<'a, Result<(), LedgerError>> {
        Box::pin(async move {
            let owned = key.to_owned();
            self.journal_hold(key, move |existing| {
                Hold::release(existing, &owned)
                    .map(|hold| (Some(hold), ()))
                    .map_err(LedgerError::Hold)
            })
            .await
        })
    }
}

impl RemoteBarrier for OneLog {
    fn settle(&self) -> HostFuture<'_, Result<(), String>> {
        Box::pin(async move {
            self.settle_deferred()
                .await
                .map_err(|error| error.to_string())
        })
    }

    fn carry(&self, carried: Carried) -> HostFuture<'_, Result<bool, String>> {
        Box::pin(async move {
            self.carried(carried)
                .await
                .map_err(|error| error.to_string())
        })
    }

    fn ride(&self, carried: Carried) -> HostFuture<'static, bool> {
        let rode = self.log.ride(carried.host, carried.statements);
        Box::pin(async move { rode.await.unwrap_or(false) })
    }
}

impl CellJournalSink for OneLogJournals {
    fn save(&self, mut update: CellJournalUpdate) -> HostFuture<'static, Result<(), String>> {
        let log = Arc::clone(&self.0);
        Box::pin(async move {
            let external = update
                .calls
                .iter()
                .filter(|call| {
                    matches!(call.state, JournaledCallState::Running)
                        && log.effect_of(&call.name).needs_row()
                })
                .map(|call| {
                    Ok(Record::Call {
                        call_id: call.call_id.clone(),
                        effect: EffectClass::External,
                        input: raw(&call.input)?,
                        tool: call.name.clone(),
                        turn: None,
                    })
                })
                .collect::<Result<Vec<_>, StoreError>>()
                .map_err(|error| error.to_string())?;
            let weak = Arc::downgrade(&log);
            log.written(
                |error| error.to_string(),
                move |writing| {
                    Box::pin(async move {
                        let log = upgraded(&weak);
                        let fresh = update.header.is_some();
                        if let Some(header) = update.header.as_mut() {
                            let catalog = serde_json::to_string(&header.tools)
                                .map_err(|error| error.to_string())?;
                            let digest = nanocode_digest::blake3(catalog.as_bytes())
                                .to_hex()
                                .to_string();
                            let key = stored_key(TOOLS_MARK, &digest);
                            writing
                                .index_owners(std::slice::from_ref(&key))
                                .await
                                .map_err(|error| error.to_string())?;
                            if !writing.fold().contains(&key) {
                                writing
                                    .push(Record::Stored {
                                        key,
                                        value: catalog,
                                    })
                                    .map_err(|error| error.to_string())?;
                            }
                            header.tools = [json!({ (TOOLS_MARK): digest })].into();
                        }
                        let shared =
                            shared::shared_results(&update).map_err(|error| error.to_string())?;
                        let encoded = match shared {
                            Some(shared) => {
                                let keys = shared
                                    .values
                                    .iter()
                                    .map(|(key, _)| stored_key(&update.session_id, key))
                                    .collect::<Vec<_>>();
                                writing
                                    .load_owners(&keys)
                                    .await
                                    .map_err(|error| error.to_string())?;
                                for ((_, value), key) in shared.values.into_iter().zip(keys) {
                                    if writing.fold().stored(&key) != Some(&*value) {
                                        writing
                                            .push(Record::Stored { key, value })
                                            .map_err(|error| error.to_string())?;
                                    }
                                }
                                shared.update
                            }
                            None => raw(&update).map_err(|error| error.to_string())?,
                        };
                        writing
                            .push(Record::Cell {
                                cell: update.cell_id,
                                fresh,
                                session: update.session_id.clone(),
                                update: encoded,
                            })
                            .map_err(|error| error.to_string())?;
                        let cell = (update.session_id.clone(), update.cell_id);
                        if update.finished {
                            log.live.update(|live| live.remove(&cell));
                        } else if fresh {
                            log.live.update(|live| live.insert(cell.clone()));
                        }
                        let seq = writing.fold().seq();
                        let timed = update
                            .entries
                            .iter()
                            .any(|entry| matches!(entry, JournalEntry::Timeout { .. }));
                        let pending = log.unsaved.read(|unsaved| unsaved.get(&cell) == Some(&seq));
                        let started = update
                            .calls
                            .iter()
                            .filter(|call| {
                                matches!(call.state, JournaledCallState::Running)
                                    && log.effect_of(&call.name).needs_row()
                            })
                            .map(|call| call.id)
                            .collect::<Vec<_>>();
                        let results = update
                            .entries
                            .iter()
                            .filter_map(|entry| match entry {
                                JournalEntry::Result { id, .. } => Some(*id),
                                _ => None,
                            })
                            .collect::<Vec<_>>();
                        let answered = log.dispatched.update(|dispatched| {
                            let calls = dispatched.entry(cell.clone()).or_default();
                            calls.extend(started.iter().copied());
                            let before = calls.len();
                            calls.retain(|id| !results.contains(id));
                            let answered = calls.len() < before;
                            if calls.is_empty() || update.finished {
                                dispatched.remove(&cell);
                            }
                            answered
                        });
                        let owed = log.answered.read(|owed| owed.get(&cell) == Some(&seq));
                        if external.is_empty()
                            && !owed
                            && !(answered && (timed || update.finished))
                            && !(timed && (fresh || pending))
                        {
                            log.unsaved.update(|unsaved| {
                                if update.finished {
                                    unsaved.remove(&cell);
                                } else if fresh {
                                    unsaved.insert(cell.clone(), seq);
                                }
                            });
                            log.answered.update(|owed| {
                                if update.finished {
                                    owed.remove(&cell);
                                } else if answered {
                                    owed.insert(cell.clone(), seq);
                                }
                            });
                            return Ok(());
                        }
                        log.unsaved.update(|unsaved| unsaved.remove(&cell));
                        log.answered.update(|owed| owed.remove(&cell));
                        for call in external {
                            writing.push(call).map_err(|error| error.to_string())?;
                        }
                        writing
                            .commit(Point::Call)
                            .await
                            .map(drop)
                            .map_err(|error| error.to_string())
                    })
                },
            )
            .await
        })
    }

    fn forget(&self, session_id: &str, cell_id: u64) -> HostFuture<'static, ()> {
        let log = Arc::clone(&self.0);
        let session = session_id.to_owned();
        log.unsaved
            .update(|unsaved| unsaved.remove(&(session.clone(), cell_id)));
        log.answered
            .update(|owed| owed.remove(&(session.clone(), cell_id)));
        log.dispatched
            .update(|dispatched| dispatched.remove(&(session.clone(), cell_id)));
        let unfinished = log
            .live
            .update(|live| live.remove(&(session.clone(), cell_id)));
        Box::pin(async move {
            let forgotten = log
                .log
                .write_within(move |writing| {
                    Box::pin(async move {
                        if let Err(error) =
                            writing.load_owners(std::slice::from_ref(&session)).await
                        {
                            nanocode_trace::warn!(%error, "turn log could not load a cell journal to forget");
                            return Ok(());
                        }
                        if let Err(error) = writing.push(Record::Forget {
                            cell: cell_id,
                            session,
                        }) {
                            nanocode_trace::warn!(%error, "turn log could not forget a cell journal");
                            return Ok(());
                        }
                        if unfinished && let Err(error) = writing.commit(Point::Call).await {
                            nanocode_trace::warn!(%error, "turn log could not commit a forgotten cell journal");
                        }
                        Ok::<_, StoreError>(())
                    })
                })
                .await;
            if let Err(error) = forgotten {
                nanocode_trace::warn!(%error, "turn log could not load a cell journal to forget");
            }
        })
    }

    fn load(&self, session_id: &str) -> HostFuture<'static, Result<Vec<CellJournal>, String>> {
        let log = Arc::clone(&self.0);
        let session = session_id.to_owned();
        Box::pin(async move {
            Ok(log
                .cells(&session, None)
                .await
                .map_err(|error| error.to_string())?
                .remove(&session)
                .unwrap_or_default())
        })
    }

    fn load_cell(
        &self,
        session_id: &str,
        cell_id: u64,
    ) -> HostFuture<'static, Result<Option<CellJournal>, String>> {
        let log = Arc::clone(&self.0);
        let session = session_id.to_owned();
        Box::pin(async move {
            Ok(log
                .cells(&session, Some(cell_id))
                .await
                .map_err(|error| error.to_string())?
                .remove(&session)
                .and_then(|journals| {
                    journals
                        .into_iter()
                        .find(|journal| journal.cell_id == cell_id)
                }))
        })
    }

    fn save_stored(
        &self,
        session_id: &str,
        stored: String,
    ) -> HostFuture<'static, Result<(), String>> {
        let log = Arc::clone(&self.0);
        let key = stored_key(session_id, STORED_MARK);
        Box::pin(async move {
            log.log
                .write_within(move |writing| {
                    Box::pin(std::future::ready(
                        writing.push(Record::Stored { key, value: stored }),
                    ))
                })
                .await
                .map_err(|error: StoreError| error.to_string())
        })
    }

    fn load_stored(&self, session_id: &str) -> HostFuture<'static, Result<Option<String>, String>> {
        let log = Arc::clone(&self.0);
        let key = stored_key(session_id, STORED_MARK);
        let legacy = stored_key(STORED_MARK, session_id);
        Box::pin(async move {
            let stored = log
                .log
                .read_owners(std::slice::from_ref(&key), {
                    let key = key.clone();
                    move |fold| fold.stored(&key).map(str::to_owned)
                })
                .await
                .map_err(|error| error.to_string())?;
            match stored {
                Some(stored) => Ok(Some(stored)),
                None => log
                    .log
                    .read_owners(std::slice::from_ref(&legacy), {
                        let legacy = legacy.clone();
                        move |fold| fold.stored(&legacy).map(str::to_owned)
                    })
                    .await
                    .map_err(|error| error.to_string()),
            }
        })
    }
}

impl OneLogStore {
    fn replaced(
        log: &OneLog,
        writing: &mut Writing,
        state_id: &str,
        owner: &OwnerToken,
        expected_revision: u64,
        payload: &str,
        records: &[StoreRecord],
    ) -> Result<u64, StoreError> {
        if log
            .owners
            .read(|owners| owners.get(state_id) != Some(owner))
        {
            return Err(StoreError::Fenced);
        }
        let actual = writing
            .fold()
            .checkpoint(state_id)
            .map_or(0, |checkpoint| checkpoint.revision);
        if actual != expected_revision {
            return Err(StoreError::Conflict {
                expected: expected_revision,
                actual,
            });
        }
        let revision = actual + 1;
        put(writing, state_id, records)?;
        writing.push(Record::Checkpoint {
            payload: payload.to_owned(),
            revision,
            state: state_id.to_owned(),
        })?;
        Ok(revision)
    }

    fn stored_state(fold: &Fold, state_id: &str) -> StoredState {
        fold.checkpoint(state_id)
            .map(|checkpoint| StoredState {
                revision: checkpoint.revision,
                payload: Some(checkpoint.payload.clone()),
            })
            .unwrap_or_default()
    }
}

impl StateStore for OneLogStore {
    fn facts(&self) -> Option<Arc<dyn crate::durability::FactLogs>> {
        Some(Arc::new(facts::Logs(Arc::clone(&self.0))))
    }

    fn read_record<'a>(
        &'a mut self,
        state_id: &'a str,
        key: &'a str,
    ) -> StoreFuture<'a, Result<Option<String>, StoreError>> {
        Box::pin(async move {
            let owned = stored_key(state_id, key);
            let value = self
                .0
                .log
                .read_owners(std::slice::from_ref(&owned), {
                    let owned = owned.clone();
                    move |fold| fold.stored(&owned).map(str::to_owned)
                })
                .await?;
            Ok(self
                .0
                .whole(state_id, &[key.to_owned()], vec![value])
                .await?
                .pop()
                .flatten())
        })
    }

    fn read_records<'a>(
        &'a mut self,
        state_id: &'a str,
        keys: &'a [String],
    ) -> StoreFuture<'a, Result<Vec<Option<String>>, StoreError>> {
        Box::pin(async move {
            let owned = keys
                .iter()
                .map(|key| stored_key(state_id, key))
                .collect::<Vec<_>>();
            let values = self
                .0
                .log
                .read_owners(&owned, {
                    let owned = owned.clone();
                    move |fold| {
                        owned
                            .iter()
                            .map(|key| fold.stored(key).map(str::to_owned))
                            .collect()
                    }
                })
                .await?;
            self.0.whole(state_id, keys, values).await
        })
    }

    fn peek<'a>(
        &'a mut self,
        state_id: &'a str,
    ) -> StoreFuture<'a, Result<StoredState, StoreError>> {
        Box::pin(async move {
            self.0
                .log
                .read_owners(&[state_id.to_owned()], {
                    let state_id = state_id.to_owned();
                    move |fold| Self::stored_state(fold, &state_id)
                })
                .await
        })
    }

    fn list_states<'a>(
        &'a mut self,
        limit: usize,
    ) -> StoreFuture<'a, Result<Vec<String>, StoreError>> {
        Box::pin(async move {
            self.0.log.load_index().await?;
            Ok(self
                .0
                .log
                .read(move |fold| {
                    fold.states()
                        .into_iter()
                        .take(limit)
                        .map(str::to_owned)
                        .collect()
                })
                .await)
        })
    }

    fn list_records<'a>(
        &'a mut self,
        state_id: &'a str,
    ) -> StoreFuture<'a, Result<Vec<String>, StoreError>> {
        Box::pin(async move {
            self.0
                .log
                .read_owner(state_id, {
                    let state_id = state_id.to_owned();
                    move |fold| {
                        export::records_of(fold, &state_id)
                            .map(|(key, _)| key.to_owned())
                            .collect()
                    }
                })
                .await
        })
    }

    fn owner<'a>(
        &'a mut self,
        state_id: &'a str,
    ) -> StoreFuture<'a, Result<Option<OwnerToken>, StoreError>> {
        let owner = self.0.owners.read(|owners| owners.get(state_id).cloned());
        Box::pin(async move { Ok(owner) })
    }

    fn acquire<'a>(
        &'a mut self,
        state_id: &'a str,
        owner_id: OwnerId,
    ) -> StoreFuture<'a, Result<OwnedState, StoreError>> {
        let log = Arc::downgrade(&self.0);
        let state_id = state_id.to_owned();
        Box::pin(self.0.log.write_within(move |writing| {
            Box::pin(async move {
                let log = upgraded(&log);
                writing.load_owners(std::slice::from_ref(&state_id)).await?;
                let owner = log.owners.update(|owners| {
                    let fence = owners.get(&state_id).map_or(0, OwnerToken::fence) + 1;
                    let token = OwnerToken::new(owner_id.clone(), fence);
                    owners.insert(state_id.clone(), token.clone());
                    token
                });
                Ok(OwnedState {
                    owner,
                    state: Self::stored_state(writing.fold(), &state_id),
                })
            })
        }))
    }

    fn replace<'a>(
        &'a mut self,
        state_id: &'a str,
        owner: &'a OwnerToken,
        expected_revision: u64,
        payload: &'a str,
        records: &'a [StoreRecord],
    ) -> StoreFuture<'a, Result<u64, StoreError>> {
        let log = Arc::downgrade(&self.0);
        let state_id = state_id.to_owned();
        let owner = owner.clone();
        let payload = payload.to_owned();
        let records = records.to_vec();
        Box::pin(self.0.log.write_within(move |writing| {
            Box::pin(async move {
                let log = upgraded(&log);
                staging(writing, &state_id, &records).await?;
                let revision = Self::replaced(
                    &log,
                    writing,
                    &state_id,
                    &owner,
                    expected_revision,
                    &payload,
                    &records,
                )?;
                if *log.durable_settles.get() {
                    writing.commit(Point::Progress).await?;
                }
                Ok(revision)
            })
        }))
    }

    fn commit<'a>(
        &'a mut self,
        state_id: &'a str,
        owner: &'a OwnerToken,
        expected_revision: u64,
        commit: StateCommit<'a>,
    ) -> StoreFuture<'a, Result<Committed, StoreError>> {
        if !commit.events.is_empty() {
            return Box::pin(std::future::ready(Err(StoreError::NotCommitted(
                "this store does not record state events".to_owned(),
            ))));
        }
        let log = Arc::downgrade(&self.0);
        let state_id = state_id.to_owned();
        let owner = owner.clone();
        let payload = commit.payload.to_owned();
        let records = commit.records.to_vec();
        let settle = commit.settle;
        Box::pin(self.0.log.write_within(move |writing| {
            Box::pin(async move {
                let log = upgraded(&log);
                staging(writing, &state_id, &records).await?;
                let revision = Self::replaced(
                    &log,
                    writing,
                    &state_id,
                    &owner,
                    expected_revision,
                    &payload,
                    &records,
                )?;
                let recovering = writing.fold().recovering();
                if settle == Settle::Durable && (recovering || *log.durable_settles.get()) {
                    if !recovering && *log.deferred_settles.get() {
                        log.owed.set(true);
                    } else {
                        writing.commit(Point::Progress).await?;
                    }
                }
                Ok(Committed {
                    revision,
                    first_event: None,
                })
            })
        }))
    }

    fn collect_unreachable<'a>(
        &'a mut self,
        state_id: &'a str,
    ) -> StoreFuture<'a, Result<(), StoreError>> {
        let state_id = state_id.to_owned();
        Box::pin(self.0.log.write_within(move |writing| {
            writing.collect_unreachable(&state_id);
            Box::pin(std::future::ready(Ok(())))
        }))
    }

    fn flush(&mut self) -> StoreFuture<'_, Result<(), StoreError>> {
        Box::pin(self.0.log.write_within(|writing| {
            Box::pin(async move {
                if writing.buffered() > 0 {
                    writing.commit(Point::Progress).await?;
                }
                Ok(())
            })
        }))
    }

    fn put_records<'a>(
        &'a mut self,
        state_id: &'a str,
        records: &'a [StoreRecord],
    ) -> StoreFuture<'a, Result<bool, StoreError>> {
        let state_id = state_id.to_owned();
        let records = records.to_vec();
        Box::pin(self.0.log.write_within(move |writing| {
            Box::pin(async move {
                staging(writing, &state_id, &records).await?;
                put(writing, &state_id, &records)?;
                Ok(true)
            })
        }))
    }
}

#[cfg(test)]
#[path = "one_log_tail_tests.rs"]
mod tail_tests;
