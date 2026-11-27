use std::sync::LazyLock;
use std::sync::atomic::{AtomicU8, AtomicU64, Ordering};

use nanocode_sync::watch;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum Mutation {
    None,
    AdmissionHeld,
    AdmitsStale,
    ObserverSaveHeld,
    NestedEndsFirst,
    NestedStartsFirst,
}

static MUTATION: AtomicU8 = AtomicU8::new(Mutation::None as u8);
static ADMISSION_HELD: AtomicU64 = AtomicU64::new(0);
static ADMISSION_RELEASED: LazyLock<watch::Sender<()>> = LazyLock::new(|| watch::Sender::new(()));

pub fn mutate(mutation: Mutation) {
    MUTATION.store(mutation as u8, Ordering::SeqCst);
}

pub fn mutated(mutation: Mutation) -> bool {
    MUTATION.load(Ordering::Relaxed) == mutation as u8
}

#[cfg(any(not(target_family = "wasm"), feature = "local-cells"))]
pub fn nested_overlap(calls: &[crate::embedded::NestedToolCall]) -> (u64, u64) {
    crate::code_mode::nested_overlap(calls)
}

pub fn hold_admission() -> bool {
    let held = mutated(Mutation::AdmissionHeld);
    if held {
        ADMISSION_HELD.fetch_add(1, Ordering::SeqCst);
    }
    held
}

pub fn release_admission(held: bool) {
    if held {
        ADMISSION_HELD.fetch_sub(1, Ordering::SeqCst);
        ADMISSION_RELEASED.send_replace(());
    }
}

pub async fn admission_released() {
    if !mutated(Mutation::AdmissionHeld) {
        return;
    }
    loop {
        let mut released = ADMISSION_RELEASED.subscribe();
        if ADMISSION_HELD.load(Ordering::SeqCst) == 0 {
            return;
        }
        released
            .changed()
            .await
            .expect("the admission release sender is static");
    }
}
