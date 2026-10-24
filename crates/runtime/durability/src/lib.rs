#![deny(rustdoc::broken_intra_doc_links)]

mod backend;
mod blob;
mod branch;
mod catalog;
mod child_journal;
mod format;
mod memory;
mod paged;
mod session;
mod shared_store;
mod state;

pub use backend::{Branches, DurableAgentExt, DurableBackendRegistration, agent_error};
pub use blob::MemoryBlobStore;
pub use branch::{BranchTurn, CheckpointBranch};
pub use catalog::{BranchBoundary, InitialSettings, SessionRecord};
pub use catalog::{
    BranchCursor, BranchExport, BranchImport, BranchPage, BranchPoint, IntoSessionStore,
    LIST_LIMIT, SessionPage, SessionQuery, SessionReader, SessionStore, SessionSummary,
    StoredSession, StoredTurn, TranscriptItem, TurnStatus,
};
pub use child_journal::{JournalClaim, JournalSeed};
pub use format::{AdmittedTurn, CheckpointFormat, FormatFuture};
pub use memory::MemoryStore;
pub use nanocode_durability_core::{
    BlobKey, BlobStore, BlobUrl, Committed, EncodedPayload, Error, EventCursor, EventHit,
    EventHits, EventOrder, EventPage, EventQuery, EventRange, IndexedState, KeyValue,
    KeyValueStore, OwnedCommit, OwnedState, OwnerId, OwnerToken, Result, Sql, SqlDialect, SqlRow,
    SqlStatement, SqlStore, StagedStore, StagedWrites, StateCommit, StateCursor, StatePage,
    StateQuery, StateReader, StateStore, StateSummary, StoreError, StoreEvent, StoreFuture,
    StoreRecord, StoreReplacement, StoreUpdate, StoredEvent, StoredState, SummaryUpdate, Versioned,
    get_blob, put_blob,
};
pub use paged::{
    PagedWriter, PayloadReader, Prepared, Sequence, SequenceCache, load_sequence, seal_sequence,
};
pub use session::{
    Admission, AutomaticAdmission, BeginStep, DurableOwner, DurableSession, InitialAcquisition,
    MetadataExpectation, MetadataWrite, OwnerWrite, SessionReceipt, SessionWrite, StoredSteer,
    TitleChange, WeakDurableSession, WriteOutcome,
};
pub use shared_store::SharedStore;
pub use state::{
    DurableState, IdentifiedSteerReceipt, OperationState, OperationStatus, SteerState, StepState,
    StepStatus, Transition,
};
