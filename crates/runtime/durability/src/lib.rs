#![deny(rustdoc::broken_intra_doc_links)]

mod backend;
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
pub use branch::{BranchTurn, CheckpointBranch};
pub use catalog::{BranchBoundary, InitialSettings, SessionRecord};
pub use catalog::{
    BranchPoint, IntoSessionStore, LIST_LIMIT, SessionStore, SessionSummary, StoredSession,
    StoredTurn, TranscriptItem, TurnStatus,
};
pub use format::{AdmittedTurn, CheckpointFormat, FormatFuture};
pub use memory::MemoryStore;
pub use nanocode_durability_core::{
    EncodedPayload, Error, OwnedState, OwnerId, OwnerToken, Result, StateStore, StoreError,
    StoreFuture, StoreRecord, StoreReplacement, StoreUpdate, StoredState,
};
pub use paged::{PagedWriter, PayloadReader, Prepared, Sequence, load_sequence};
pub use session::{
    Admission, AutomaticAdmission, BeginStep, DurableOwner, DurableSession, InitialAcquisition,
    StoredSteer,
};
pub use shared_store::SharedStore;
pub use state::{
    DurableState, IdentifiedSteerReceipt, OperationState, OperationStatus, SteerState, StepState,
    StepStatus, Transition,
};
