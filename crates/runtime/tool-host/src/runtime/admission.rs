use std::{
    collections::{HashMap, HashSet, VecDeque},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

use nanocode_sync::{Atom, Signal};

#[derive(Clone, Default)]
struct GateState {
    exclusive: Option<u64>,
    shared: HashMap<u64, Option<String>>,
    waiting: VecDeque<Waiter>,
    yielded: HashSet<String>,
}

#[derive(Clone)]
struct Waiter {
    token: u64,
    exclusive: bool,
    origin: Option<String>,
    admitted: Arc<Signal>,
}

#[derive(Default)]
pub(super) struct AdmissionGate {
    state: Atom<GateState>,
    tokens: AtomicU64,
}

pub(super) struct Admission<'gate> {
    gate: &'gate AdmissionGate,
    token: u64,
}

impl Drop for Admission<'_> {
    fn drop(&mut self) {
        self.gate.release(self.token);
    }
}

impl GateState {
    fn admits(&self, exclusive: bool) -> bool {
        self.exclusive.is_none() && (!exclusive || self.shared.is_empty())
    }

    fn admit(&mut self, token: u64, exclusive: bool, origin: Option<String>) {
        if exclusive {
            self.exclusive = Some(token);
        } else if origin
            .as_ref()
            .is_none_or(|origin| !self.yielded.contains(origin))
        {
            self.shared.insert(token, origin);
        }
    }

    fn advance(&mut self) -> Vec<Arc<Signal>> {
        let mut admitted = Vec::new();
        while self
            .waiting
            .front()
            .is_some_and(|next| self.admits(next.exclusive))
        {
            let Some(next) = self.waiting.pop_front() else {
                break;
            };
            self.admit(next.token, next.exclusive, next.origin);
            admitted.push(next.admitted);
        }
        admitted
    }
}

impl AdmissionGate {
    pub(super) async fn admit(&self, exclusive: bool, origin: Option<&str>) -> Admission<'_> {
        let token = self.tokens.fetch_add(1, Ordering::Relaxed);
        let admitted = Arc::new(Signal::new());
        let admission = Admission { gate: self, token };
        let immediate = self.state.update(|state| {
            let origin = origin.map(str::to_owned);
            if state.waiting.is_empty() && state.admits(exclusive) {
                state.admit(token, exclusive, origin);
                true
            } else {
                state.waiting.push_back(Waiter {
                    token,
                    exclusive,
                    origin,
                    admitted: Arc::clone(&admitted),
                });
                false
            }
        });
        if !immediate {
            admitted.wait().await;
        }
        admission
    }

    pub(super) fn observed(&self, origin: &str, observed: bool) {
        let admitted = self.state.update(|state| {
            if observed {
                state.yielded.remove(origin);
                Vec::new()
            } else {
                state.yielded.insert(origin.to_owned());
                state
                    .shared
                    .retain(|_, held| held.as_deref() != Some(origin));
                state.advance()
            }
        });
        admitted.iter().for_each(|admitted| admitted.fire());
    }

    fn release(&self, token: u64) {
        let admitted = self.state.update(|state| {
            state.waiting.retain(|waiter| waiter.token != token);
            if state.exclusive == Some(token) {
                state.exclusive = None;
            }
            state.shared.remove(&token);
            state.advance()
        });
        admitted.iter().for_each(|admitted| admitted.fire());
    }
}
