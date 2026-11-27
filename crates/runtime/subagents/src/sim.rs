use std::sync::atomic::{AtomicU8, Ordering};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum Mutation {
    None,
    WaitRepeatsIds,
    WaitRepeatsUnread,
    WaitIgnoresCarrierRelease,
    WaitKeepsOriginTurn,
    WaitForgetsDiscard,
    RestoreRemembersWaits,
    RouteSkipsClaim,
    ScopeJobsOverlap,
    RestoreSharesClaim,
    ScopeJobsStopInline,
    ScopeLocksKept,
    CancelHoldKept,
}

impl Mutation {
    pub const ALL: [Self; 13] = [
        Self::None,
        Self::WaitRepeatsIds,
        Self::WaitRepeatsUnread,
        Self::WaitIgnoresCarrierRelease,
        Self::WaitKeepsOriginTurn,
        Self::WaitForgetsDiscard,
        Self::RestoreRemembersWaits,
        Self::RouteSkipsClaim,
        Self::ScopeJobsOverlap,
        Self::RestoreSharesClaim,
        Self::ScopeJobsStopInline,
        Self::ScopeLocksKept,
        Self::CancelHoldKept,
    ];

    pub const fn name(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::WaitIgnoresCarrierRelease => "subagents_wait_ignores_carrier_release",
            Self::WaitForgetsDiscard => "subagents_wait_forgets_discard",
            Self::WaitKeepsOriginTurn => "subagents_wait_keeps_origin_turn",
            Self::WaitRepeatsUnread => "subagents_wait_repeats_unread",
            Self::WaitRepeatsIds => "subagents_wait_repeats_ids",
            Self::RestoreRemembersWaits => "subagents_restore_remembers_waits",
            Self::RouteSkipsClaim => "subagents_route_skips_claim",
            Self::ScopeJobsOverlap => "subagents_scope_jobs_overlap",
            Self::RestoreSharesClaim => "subagents_restore_shares_claim",
            Self::ScopeJobsStopInline => "subagents_scope_jobs_stop_inline",
            Self::ScopeLocksKept => "subagents_scope_locks_kept",
            Self::CancelHoldKept => "subagents_cancel_hold_kept",
        }
    }
}

static MUTATION: AtomicU8 = AtomicU8::new(Mutation::None as u8);

pub fn mutate(mutation: Mutation) {
    MUTATION.store(mutation as u8, Ordering::SeqCst);
}

pub fn mutated(mutation: Mutation) -> bool {
    MUTATION.load(Ordering::Relaxed) == mutation as u8
}
