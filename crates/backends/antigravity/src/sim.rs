use std::sync::atomic::{AtomicU8, Ordering};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum Mutation {
    None,
    MovedForgetsResults,
    ReplayReruns,
    FactSyncSkipped,
    RevisionMissing,
    HostContextMissing,
    SteerRevisionIgnored,
    RevisionForgotten,
    OriginMissing,
    CompactionOriginMissing,
    XhighAsHigh,
    HarnessDefaultsHigh,
}

static MUTATION: AtomicU8 = AtomicU8::new(Mutation::None as u8);

pub fn mutate(mutation: Mutation) {
    MUTATION.store(mutation as u8, Ordering::SeqCst);
}

pub fn mutated(mutation: Mutation) -> bool {
    MUTATION.load(Ordering::Relaxed) == mutation as u8
}
