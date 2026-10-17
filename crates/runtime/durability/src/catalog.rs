use std::path::PathBuf;

use nanocode_agent_core::{HarnessFamily, Lineage, ModelSelection};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionRecord {
    pub session_id: String,

    pub model: ModelSelection,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workspace: Option<PathBuf>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,

    pub lineage: Lineage,

    #[serde(default)]
    pub created_at_ms: u64,

    #[serde(default)]
    pub updated_at_ms: u64,

    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub history_pruned: bool,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub branch: Option<BranchBoundary>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) start_checkpoint: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub initial: Option<InitialSettings>,

    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub inherited_transcript: Vec<nanocode_agent_core::input::PromptMessage>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct InitialSettings {
    pub thinking: nanocode_agent_core::Thinking,
    pub service_tier: nanocode_agent_core::ServiceTier,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BranchBoundary {
    pub source_session_id: String,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub through_turn: Option<String>,
}

impl SessionRecord {
    #[must_use]
    pub fn root(
        session_id: impl Into<String>,
        model: ModelSelection,
        workspace: Option<PathBuf>,
    ) -> Self {
        let session_id = session_id.into();
        let now = now_ms();
        Self {
            lineage: Lineage::root(session_id.clone()),
            session_id,
            model,
            workspace,
            title: None,
            created_at_ms: now,
            updated_at_ms: now,
            history_pruned: false,
            branch: None,
            start_checkpoint: None,
            initial: None,
            inherited_transcript: Vec::new(),
        }
    }

    #[must_use]
    pub fn with_inherited_transcript(
        mut self,
        transcript: Vec<nanocode_agent_core::input::PromptMessage>,
    ) -> Self {
        self.inherited_transcript = transcript;
        self
    }

    #[must_use]
    pub fn family(&self) -> HarnessFamily {
        self.model.family()
    }

    #[must_use]
    pub fn with_title(mut self, title: impl Into<String>) -> Self {
        self.title = Some(title.into());
        self
    }

    #[must_use]
    pub fn with_lineage(mut self, lineage: Lineage) -> Self {
        self.lineage = lineage;
        self
    }

    #[must_use]
    pub fn derive(
        &self,
        session_id: impl Into<String>,
        origin: nanocode_agent_core::Origin,
    ) -> Self {
        let session_id = session_id.into();
        let now = now_ms();
        Self {
            lineage: Lineage::child_of(&self.lineage, self.session_id.clone(), origin),
            session_id,
            model: self.model.clone(),
            workspace: self.workspace.clone(),
            title: self.title.clone(),
            created_at_ms: now,
            updated_at_ms: now,
            history_pruned: false,
            branch: None,
            start_checkpoint: None,
            initial: None,
            inherited_transcript: Vec::new(),
        }
    }

    pub(crate) fn touch(&mut self) {
        let now = now_ms();
        if now > self.updated_at_ms {
            self.updated_at_ms = now;
        }
    }

    pub(crate) fn merged(&self, next: Self) -> Self {
        let lineage = if next.lineage == Lineage::root(next.session_id.clone()) {
            self.lineage.clone()
        } else {
            next.lineage
        };
        Self {
            session_id: next.session_id,
            model: next.model,
            workspace: next.workspace.or_else(|| self.workspace.clone()),
            title: next.title.or_else(|| self.title.clone()),
            lineage,
            created_at_ms: if self.created_at_ms == 0 {
                next.created_at_ms
            } else {
                self.created_at_ms
            },
            updated_at_ms: self.updated_at_ms,
            history_pruned: self.history_pruned || next.history_pruned,

            branch: self.branch.clone().or(next.branch),
            start_checkpoint: next
                .start_checkpoint
                .or_else(|| self.start_checkpoint.clone()),
            initial: next.initial.or(self.initial),
            inherited_transcript: if next.inherited_transcript.is_empty() {
                self.inherited_transcript.clone()
            } else {
                next.inherited_transcript
            },
        }
    }
}

fn now_ms() -> u64 {
    use nanocode_platform::UNIX_EPOCH;

    nanocode_platform::system_time()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |elapsed| {
            u64::try_from(elapsed.as_millis()).unwrap_or(u64::MAX)
        })
}

pub use store::*;

mod store {
    use std::{path::PathBuf, sync::Arc};

    use nanocode_agent_core::{HarnessFamily, Lineage, ModelSelection, Origin};
    use serde::Serialize;
    use serde_json::Value;

    use super::{BranchBoundary, SessionRecord};
    use crate::{
        AdmittedTurn, CheckpointFormat, DurableSession, DurableState, EncodedPayload, Error,
        OperationStatus, OwnerId, PayloadReader, Result, StateStore, Transition,
        format::Formats,
        session::reduce_peeked,
        shared_store::{SharedStore, WeakSharedStore},
    };

    pub use nanocode_agent_core::session::TranscriptItem;

    pub const LIST_LIMIT: usize = 1000;

    #[derive(Clone, Debug, PartialEq, Eq, Serialize)]
    pub struct SessionSummary {
        pub record: SessionRecord,

        pub preview: Option<String>,
    }

    #[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
    #[serde(rename_all = "snake_case")]
    pub enum TurnStatus {
        Pending,

        Completed,

        Failed,

        Cancelled,
    }

    #[derive(Clone, Debug, PartialEq, Serialize)]
    pub struct StoredTurn {
        pub id: String,

        pub preview: Option<String>,

        pub status: TurnStatus,

        pub input: Value,
    }

    #[derive(Clone, Debug)]
    pub struct StoredSession {
        pub summary: SessionSummary,

        pub transcript: Vec<TranscriptItem>,

        pub turns: Vec<StoredTurn>,

        pub checkpoint: Option<Value>,

        formats: Formats,

        loaded: Loaded,
    }

    #[derive(Clone, Debug)]
    struct Loaded {
        store: WeakSharedStore,
        state_id: String,
        checkpoint_key: Option<String>,
    }

    impl StoredSession {
        pub fn session_checkpoint(&self) -> Result<Option<nanocode_agent_core::SessionCheckpoint>> {
            let Some(checkpoint) = &self.checkpoint else {
                return Ok(None);
            };
            let record = &self.summary.record;
            if is_portable(checkpoint) {
                let portable = portable_checkpoint(checkpoint)?;
                portable
                    .require_family(record.family())
                    .map_err(|error| Error::InvalidState(error.to_string()))?;

                let rebound = nanocode_agent_core::SessionCheckpoint::native(
                    record.session_id.clone(),
                    portable.model(),
                    portable.thinking(),
                    record.lineage.clone(),
                    portable.conversation_id(),
                    portable.turn_id().map(str::to_owned),
                    portable.has_conversation(),
                    portable.payload().clone(),
                )
                .with_service_tier(portable.service_tier());
                rebound
                    .validate()
                    .map_err(|error| Error::InvalidState(error.to_string()))?;
                return Ok(Some(rebound));
            }
            self.formats
                .family(record.family())?
                .session_checkpoint(record, checkpoint.clone())
                .map(Some)
        }
    }

    #[derive(Clone, Debug, PartialEq, Eq)]
    pub enum BranchPoint {
        Latest,

        Through(String),

        Before(String),
    }

    #[derive(Clone)]
    pub struct SessionStore {
        store: SharedStore,
        formats: Formats,
    }

    pub trait IntoSessionStore {
        fn into_session_store(self) -> Result<SessionStore>;
    }

    impl IntoSessionStore for SessionStore {
        fn into_session_store(self) -> Result<SessionStore> {
            Ok(self)
        }
    }

    impl<S: StateStore + 'static> IntoSessionStore for S {
        fn into_session_store(self) -> Result<SessionStore> {
            SessionStore::new(self)
        }
    }

    impl SessionStore {
        #[must_use]
        pub fn same_store(&self, other: &Self) -> bool {
            self.store.same_store(&other.store)
        }

        pub fn new<S: StateStore + 'static>(store: S) -> Result<Self> {
            Ok(Self {
                store: SharedStore::new(store)?,
                formats: Formats::default(),
            })
        }

        pub fn register_format(&self, format: Arc<dyn CheckpointFormat>) -> Result<()> {
            self.formats.register(format)
        }

        pub async fn flush(&self) -> Result<()> {
            self.store.clone().flush().await?;
            Ok(())
        }

        pub async fn list(&self) -> Result<Vec<SessionSummary>> {
            let ids = self.store.clone().list_states(usize::MAX).await?;
            let mut sessions = Vec::new();
            for id in ids {
                if is_auxiliary_state(&id) {
                    continue;
                }
                if let Some(summary) = self.summary(&id).await? {
                    sessions.push(summary);
                }
            }
            sessions.sort_by_key(|session| std::cmp::Reverse(session.record.updated_at_ms));
            Ok(sessions)
        }

        pub async fn summary(&self, id: &str) -> Result<Option<SessionSummary>> {
            let Some(state) = self.peek(id).await? else {
                return Ok(None);
            };

            let created_child = state.session().is_some_and(|record| {
                matches!(
                    record.lineage.origin,
                    Origin::Fork | Origin::SideConversation | Origin::Subagent
                )
            });
            if state.operations().is_empty()
                && state.latest_checkpoint().is_none()
                && !created_child
            {
                return Ok(None);
            }
            let preview = self.first_prompt(id, &state).await?;
            let record = match state.session() {
                Some(record) => record.clone(),
                None => {
                    let Some(checkpoint) = self.native_checkpoint(id, &state).await? else {
                        return Ok(None);
                    };
                    self.infer_record(id, &checkpoint)?
                }
            };
            Ok(Some(SessionSummary { record, preview }))
        }

        pub async fn load(&self, id: &str) -> Result<StoredSession> {
            let state = self.require(id).await?;
            let checkpoint = self.native_checkpoint(id, &state).await?;
            let record = match (state.session(), &checkpoint) {
                (Some(record), _) => record.clone(),
                (None, Some(checkpoint)) => self.infer_record(id, checkpoint)?,
                (None, None) => {
                    return Err(Error::SessionNotFound {
                        session_id: id.to_owned(),
                    });
                }
            };
            let turns = self.state_turns(id, &state).await?;
            let admitted = admitted_turns(&turns, &state);
            let transcript = match &checkpoint {
                Some(checkpoint) => self.transcript(checkpoint, record.family(), &admitted)?,
                None => Vec::new(),
            };
            let preview = turns
                .iter()
                .find_map(|turn| turn.preview.clone())
                .or_else(|| {
                    transcript.iter().find_map(|item| match item {
                        TranscriptItem::User(text) => Some(text.clone()),
                        _ => None,
                    })
                });
            Ok(StoredSession {
                summary: SessionSummary { record, preview },
                transcript,
                turns,
                checkpoint,
                formats: self.formats.clone(),
                loaded: Loaded {
                    store: self.store.downgrade(),
                    state_id: id.to_owned(),
                    checkpoint_key: state
                        .latest_checkpoint()
                        .map(|payload| payload.key().to_owned()),
                },
            })
        }

        pub async fn turns(&self, id: &str) -> Result<Vec<StoredTurn>> {
            let state = self.require(id).await?;
            self.state_turns(id, &state).await
        }

        pub async fn session(&self, record: SessionRecord) -> Result<DurableSession> {
            DurableSession::open_shared_described(
                self.store.clone(),
                record.session_id.clone(),
                None,
                Some(record),
            )
            .await
        }

        pub async fn prepare_session(&self, record: SessionRecord) -> Result<DurableSession> {
            DurableSession::prepare_shared_described(
                self.store.clone(),
                record.session_id.clone(),
                None,
                Some(record),
            )
            .await
        }

        pub async fn recovery_checkpoint(
            &self,
            journal: nanocode_agent_core::SessionCheckpoint,
        ) -> Result<nanocode_agent_core::SessionCheckpoint> {
            journal
                .validate()
                .map_err(|error| Error::InvalidState(error.to_string()))?;
            let stored = match self.load(journal.session_id()).await {
                Ok(stored) => stored,
                Err(Error::SessionNotFound { .. }) => return Ok(journal),
                Err(error) => return Err(error),
            };
            let record = &stored.summary.record;
            if record.family() != journal.family() || record.lineage != *journal.lineage() {
                return Err(Error::InvalidState(
                    "child journal identity differs from its durable session".into(),
                ));
            }
            Ok(stored.session_checkpoint()?.unwrap_or(journal))
        }

        pub async fn resume(&self, id: &str) -> Result<DurableSession> {
            let record = self.resume_record(id).await?;
            DurableSession::open_shared_described(self.store.clone(), id.to_owned(), None, record)
                .await
        }

        pub async fn prepare_resume(&self, id: &str) -> Result<DurableSession> {
            let record = self.resume_record(id).await?;
            DurableSession::prepare_shared_described(
                self.store.clone(),
                id.to_owned(),
                None,
                record,
            )
            .await
        }

        pub async fn prepare_resume_loaded(
            &self,
            stored: &StoredSession,
        ) -> Result<DurableSession> {
            if !stored.loaded.store.is(&self.store) {
                return Err(Error::InvalidState(
                    "the stored session was loaded from a different session store".to_owned(),
                ));
            }
            let id = &stored.summary.record.session_id;
            if *id != stored.loaded.state_id {
                return Err(Error::InvalidState(
                    "the stored session identity changed after loading".to_owned(),
                ));
            }
            let state = self.prepare_resume(id).await?;
            if let (Some(key), Some(checkpoint)) =
                (&stored.loaded.checkpoint_key, &stored.checkpoint)
            {
                state.preload_checkpoint(key.clone(), checkpoint.clone());
            }
            Ok(state)
        }

        async fn resume_record(&self, id: &str) -> Result<Option<SessionRecord>> {
            let state = self.require(id).await?;
            if state.session().is_some() {
                return Ok(None);
            }
            self.native_checkpoint(id, &state)
                .await?
                .map(|checkpoint| self.infer_record(id, &checkpoint))
                .transpose()
        }

        pub async fn branch(
            &self,
            id: &str,
            at: BranchPoint,
            workspace: Option<PathBuf>,
        ) -> Result<SessionSummary> {
            self.branch_with(id, at, workspace, |selected, _| Ok(selected))
                .await
        }

        pub async fn branch_with(
            &self,
            id: &str,
            at: BranchPoint,
            workspace: Option<PathBuf>,
            transform: impl FnOnce(Option<Value>, Option<&Value>) -> Result<Option<Value>>,
        ) -> Result<SessionSummary> {
            let state = self.require(id).await?;

            if !state.pending_operations().is_empty() {
                return Err(Error::InvalidState(
                    "conversation rewind refuses pending operations; settle or reconcile them first"
                        .into(),
                ));
            }
            let mut record = match state.session() {
                Some(record) => record.clone(),
                None => {
                    let checkpoint =
                        self.native_checkpoint(id, &state).await?.ok_or_else(|| {
                            Error::InvalidState(
                                "source session has no checkpoint to branch from".into(),
                            )
                        })?;
                    self.infer_record(id, &checkpoint)?
                }
            };
            let selected = match &at {
                BranchPoint::Latest => state
                    .latest_checkpoint()
                    .cloned()
                    .map(|payload| (id.to_owned(), payload)),
                BranchPoint::Through(turn) => {
                    let operation = state.operation(turn).ok_or_else(unknown_turn)?;
                    Some(
                        settled_checkpoint(&operation.status)
                            .cloned()
                            .map(|payload| (id.to_owned(), payload))
                            .ok_or_else(|| {
                                Error::InvalidState(
                                    "the selected turn has no settled checkpoint".into(),
                                )
                            })?,
                    )
                }
                BranchPoint::Before(turn) => {
                    let selected = state.operation(turn).ok_or_else(unknown_turn)?;
                    let prior = state
                        .operations()
                        .values()
                        .filter(|op| op.accepted_order() < selected.accepted_order())
                        .max_by_key(|op| op.accepted_order());

                    let complete_history = state
                        .session()
                        .map_or(selected.accepted_order() == 1, |record| {
                            !record.history_pruned
                        });
                    match prior {
                        None if complete_history => Self::start_checkpoint(id, &state)?,
                        Some(prior) => Some(
                            settled_checkpoint(&prior.status)
                                .cloned()
                                .map(|payload| (id.to_owned(), payload))
                                .ok_or_else(|| {
                                    Error::InvalidState(
                                        "checkpoint before the selected turn is unavailable".into(),
                                    )
                                })?,
                        ),
                        None => {
                            return Err(Error::InvalidState(
                                "retained history before the selected turn was pruned".into(),
                            ));
                        }
                    }
                }
            };
            let boundary = self.boundary(id, &state, &at).await?;
            let branch_id = nanocode_platform::uuid_v7().to_string();
            let selected = match selected {
                Some((holder, payload)) => Some(
                    self.hydrate(&holder, &payload, Some(record.family()))
                        .await?,
                ),
                None => None,
            };
            let latest = self.native_checkpoint(id, &state).await?;
            let mut native = transform(selected, latest.as_ref())?;
            if let (Some(workspace), Some(Value::Object(object))) = (&workspace, native.as_mut())
                && object.contains_key("workspace")
            {
                object.insert(
                    "workspace".into(),
                    Value::String(workspace.display().to_string()),
                );
            }
            record = record.derive(branch_id, Origin::Branch);
            record.branch = Some(boundary);
            if workspace.is_some() {
                record.workspace = workspace;
            }
            let checkpoint = native
                .map(|value| self.encode_native(record.family(), value))
                .transpose()?;

            record.start_checkpoint = checkpoint.as_ref().map(|payload| payload.key().to_owned());
            let preview = match &at {
                BranchPoint::Before(_) => None,
                _ => self.first_prompt(id, &state).await?,
            };
            self.publish(record.clone(), checkpoint).await?;
            Ok(SessionSummary { record, preview })
        }

        fn start_checkpoint(
            id: &str,
            state: &DurableState,
        ) -> Result<Option<(String, EncodedPayload)>> {
            let Some(record) = state.session() else {
                return Ok(None);
            };
            if matches!(record.lineage.origin, Origin::Root | Origin::Subagent) {
                return Ok(None);
            }
            match (&record.start_checkpoint, &record.branch) {
                (Some(key), _) => Ok(Some((id.to_owned(), EncodedPayload::from_key(key)))),
                (None, Some(_)) => Ok(None),
                (None, None) => Err(Error::InvalidState(
                    "history before this session's first turn is held by its source session, which this older session record does not pin".into(),
                )),
            }
        }

        async fn boundary(
            &self,
            id: &str,
            state: &DurableState,
            at: &BranchPoint,
        ) -> Result<BranchBoundary> {
            let order = |turn: &str| state.operation(turn).map(|op| op.accepted_order());
            let limit = match at {
                BranchPoint::Latest => state
                    .operations()
                    .values()
                    .filter(|op| settled_checkpoint(&op.status).is_some())
                    .map(|op| op.accepted_order())
                    .max()
                    .unwrap_or(0),
                BranchPoint::Through(turn) => order(turn).ok_or_else(unknown_turn)?,
                BranchPoint::Before(turn) => {
                    order(turn).ok_or_else(unknown_turn)?.saturating_sub(1)
                }
            };
            let through_turn = self
                .state_turns(id, state)
                .await?
                .into_iter()
                .rev()
                .find(|turn| order(&turn.id).is_some_and(|order| order <= limit))
                .map(|turn| turn.id);
            Ok(BranchBoundary {
                source_session_id: id.to_owned(),
                through_turn,
            })
        }

        async fn publish(
            &self,
            record: SessionRecord,
            checkpoint: Option<EncodedPayload>,
        ) -> Result<()> {
            let mut store = self.store.clone();
            let id = record.session_id.clone();
            let target = store.acquire(&id, OwnerId::new()).await?;
            if target.state.revision != 0 || target.state.payload.is_some() {
                return Err(Error::InvalidState(
                    "new branch identity unexpectedly exists".into(),
                ));
            }
            let mut state = DurableState::default();
            match checkpoint {
                Some(checkpoint) => {
                    state.apply_transition(1, Transition::CheckpointCommitted { checkpoint })?;
                }
                None => state.advance_revision(1)?,
            }
            state.set_session(Some(record));
            let records = state.stage_records();
            store
                .replace(
                    &id,
                    &target.owner,
                    0,
                    &state.checkpoint_payload()?,
                    &records,
                )
                .await?;
            Ok(())
        }

        async fn peek(&self, id: &str) -> Result<Option<DurableState>> {
            let stored = self.store.clone().peek(id).await?;
            if stored.payload.is_none() {
                return Ok(None);
            }
            reduce_peeked(stored).map(Some)
        }

        async fn require(&self, id: &str) -> Result<DurableState> {
            self.peek(id).await?.ok_or_else(|| Error::SessionNotFound {
                session_id: id.to_owned(),
            })
        }

        async fn load_payload(&self, id: &str, payload: &EncodedPayload) -> Result<EncodedPayload> {
            payload.load(&mut self.store.clone(), id).await
        }

        async fn hydrate(
            &self,
            id: &str,
            payload: &EncodedPayload,
            family: Option<HarnessFamily>,
        ) -> Result<Value> {
            let value: Value = self.load_payload(id, payload).await?.decode()?;
            if is_portable(&value) {
                return Ok(value);
            }
            let format = match family {
                Some(family) => self.formats.family(family)?,
                None => self.formats.owner(&value)?,
            };
            format
                .hydrate(PayloadReader::store(&self.store, id), value)
                .await
        }

        fn encode_native(&self, family: HarnessFamily, value: Value) -> Result<EncodedPayload> {
            if is_portable(&value) {
                portable_checkpoint(&value)?
                    .require_family(family)
                    .map_err(|error| Error::InvalidState(error.to_string()))?;
                return EncodedPayload::encode(&value);
            }
            self.formats.family(family)?.encode(value)
        }

        fn infer_record(&self, id: &str, checkpoint: &Value) -> Result<SessionRecord> {
            if is_portable(checkpoint) {
                let checkpoint = portable_checkpoint(checkpoint)?;
                let mut record = SessionRecord::root(id, checkpoint.model(), None)
                    .with_lineage(checkpoint.lineage().clone());
                record.created_at_ms = 0;
                record.updated_at_ms = 0;
                return Ok(record);
            }
            let model = checkpoint
                .get("model")
                .and_then(Value::as_str)
                .and_then(|model| model.parse::<ModelSelection>().ok())
                .ok_or_else(|| {
                    Error::InvalidState("stored checkpoint has no known model".into())
                })?;
            if !self.formats.family(model.family())?.owns(checkpoint) {
                return Err(Error::InvalidState(
                    "stored checkpoint model belongs to another family".into(),
                ));
            }
            let workspace = checkpoint
                .get("workspace")
                .and_then(Value::as_str)
                .filter(|workspace| !workspace.is_empty())
                .map(PathBuf::from);
            Ok(SessionRecord {
                session_id: id.to_owned(),
                model,
                workspace,
                title: None,
                lineage: Lineage::root(id),
                created_at_ms: 0,
                updated_at_ms: 0,
                history_pruned: false,
                branch: None,
                start_checkpoint: None,
                initial: None,
                inherited_transcript: Vec::new(),
            })
        }

        fn transcript(
            &self,
            checkpoint: &Value,
            family: HarnessFamily,
            turns: &[AdmittedTurn<'_>],
        ) -> Result<Vec<TranscriptItem>> {
            let checkpoint = if is_portable(checkpoint) {
                &checkpoint["payload"]
            } else {
                checkpoint
            };
            Ok(self.formats.family(family)?.transcript(checkpoint, turns))
        }

        async fn native_checkpoint(&self, id: &str, state: &DurableState) -> Result<Option<Value>> {
            match state.latest_checkpoint() {
                Some(payload) => self
                    .hydrate(id, payload, state.session().map(SessionRecord::family))
                    .await
                    .map(Some),
                None => Ok(None),
            }
        }

        async fn state_turns(&self, id: &str, state: &DurableState) -> Result<Vec<StoredTurn>> {
            let mut operations: Vec<_> = state.operations().iter().collect();
            operations.sort_by_key(|(_, op)| op.accepted_order());
            let inputs: Vec<_> = operations.iter().map(|(_, op)| op.input.clone()).collect();
            let inputs = EncodedPayload::load_many(&inputs, &mut self.store.clone(), id).await?;
            let mut turns = Vec::new();
            for ((turn, operation), input) in operations.into_iter().zip(inputs) {
                let input: Value = input.decode()?;
                if input
                    .get("kind")
                    .and_then(Value::as_str)
                    .is_some_and(|kind| kind != "prompt")
                {
                    continue;
                }
                turns.push(StoredTurn {
                    id: turn.clone(),
                    preview: prompt_text(&input),
                    status: match operation.status {
                        OperationStatus::Pending => TurnStatus::Pending,
                        OperationStatus::Completed { .. } => TurnStatus::Completed,
                        OperationStatus::Failed { .. } => TurnStatus::Failed,
                        OperationStatus::Cancelled { .. } => TurnStatus::Cancelled,
                    },
                    input,
                });
            }
            Ok(turns)
        }

        async fn first_prompt(&self, id: &str, state: &DurableState) -> Result<Option<String>> {
            let Some((_, first)) = state
                .operations()
                .iter()
                .min_by_key(|(_, op)| op.accepted_order())
            else {
                return Ok(None);
            };
            let input: Value = self.load_payload(id, &first.input).await?.decode()?;
            Ok(prompt_text(&input))
        }
    }

    fn unknown_turn() -> Error {
        Error::InvalidState("unknown or expired turn".into())
    }

    const fn settled_checkpoint(status: &OperationStatus) -> Option<&EncodedPayload> {
        match status {
            OperationStatus::Completed { checkpoint, .. }
            | OperationStatus::Failed { checkpoint, .. }
            | OperationStatus::Cancelled {
                checkpoint: Some(checkpoint),
            } => Some(checkpoint),
            _ => None,
        }
    }

    fn is_auxiliary_state(id: &str) -> bool {
        id.ends_with(":subagents") || id.ends_with(":process-routes")
    }

    fn is_portable(value: &Value) -> bool {
        value
            .get("format")
            .and_then(Value::as_str)
            .is_some_and(|format| format.starts_with("nanocode-session-checkpoint/"))
    }

    fn portable_checkpoint(value: &Value) -> Result<nanocode_agent_core::SessionCheckpoint> {
        let checkpoint: nanocode_agent_core::SessionCheckpoint =
            serde_json::from_value(value.clone()).map_err(Error::InvalidPayload)?;
        checkpoint
            .validate()
            .map_err(|error| Error::InvalidState(error.to_string()))?;
        Ok(checkpoint)
    }

    fn prompt_text(input: &Value) -> Option<String> {
        fn find(value: &Value, depth: usize) -> Option<&str> {
            if depth > 8 {
                return None;
            }
            match value {
                Value::Object(object) => {
                    for key in ["instruction", "text", "prompt", "message"] {
                        if let Some(text) = object.get(key).and_then(Value::as_str)
                            && !text.trim().is_empty()
                        {
                            return Some(text);
                        }
                    }
                    object.values().find_map(|value| find(value, depth + 1))
                }
                Value::Array(items) => items.iter().find_map(|value| find(value, depth + 1)),
                _ => None,
            }
        }
        find(input, 0).map(|text| text.chars().take(500).collect())
    }

    fn admitted_turns<'a>(turns: &'a [StoredTurn], state: &DurableState) -> Vec<AdmittedTurn<'a>> {
        turns
            .iter()
            .map(|turn| AdmittedTurn {
                input: &turn.input,
                steers: state
                    .operations()
                    .iter()
                    .find(|(id, _)| id.as_str() == turn.id)
                    .map_or(0, |(_, operation)| {
                        operation.retired_steers as usize + operation.steers.len()
                    }),
            })
            .collect()
    }
}
