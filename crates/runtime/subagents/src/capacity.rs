use nanocode_sync::Atom;
use std::sync::Arc;
use tokio::sync::watch;

#[derive(Clone)]
pub(super) struct Capacity {
    state: Arc<Atom<CapacityState>>,
    revision: watch::Sender<u64>,
}

#[derive(Clone, Copy)]
struct CapacityState {
    active: usize,
    limit: usize,
}

pub(super) struct TurnCapacity {
    state: Arc<Atom<CapacityState>>,
    revision: watch::Sender<u64>,
}

fn exhausted(limit: usize) -> std::io::Error {
    std::io::Error::other(format!(
        "sub-agent concurrency limit of {limit} has been reached; try delegation again later"
    ))
}

impl Capacity {
    pub(super) fn new(limit: usize) -> Self {
        let (revision, _) = watch::channel(0);
        Self {
            state: Arc::new(Atom::new(CapacityState { active: 0, limit })),
            revision,
        }
    }

    pub(super) fn reserve(&self) -> std::io::Result<TurnCapacity> {
        self.state.modify(|state| {
            if state.limit != usize::MAX && state.active >= state.limit {
                return Err(exhausted(state.limit));
            }
            state.active += 1;
            Ok(())
        })?;

        Ok(TurnCapacity {
            state: Arc::clone(&self.state),
            revision: self.revision.clone(),
        })
    }

    pub(super) fn reserve_many(&self, count: usize) -> std::io::Result<Vec<TurnCapacity>> {
        self.state.modify(|state| {
            if state.limit != usize::MAX && count > state.limit.saturating_sub(state.active) {
                return Err(exhausted(state.limit));
            }
            state.active += count;
            Ok(())
        })?;

        Ok((0..count)
            .map(|_| TurnCapacity {
                state: Arc::clone(&self.state),
                revision: self.revision.clone(),
            })
            .collect())
    }

    pub(super) fn set_limit(&self, limit: usize) {
        self.state.update(|state| state.limit = limit);
        self.changed();
    }

    pub(super) fn subscribe(&self) -> watch::Receiver<u64> {
        self.revision.subscribe()
    }

    fn changed(&self) {
        self.revision.send_modify(|revision| {
            *revision = revision.wrapping_add(1);
        });
    }
}

impl Drop for TurnCapacity {
    fn drop(&mut self) {
        self.state.update(|state| state.active -= 1);
        self.revision.send_modify(|revision| {
            *revision = revision.wrapping_add(1);
        });
    }
}
