//! A host's admission of a hand that attached over a socket it accepted
//! itself: reading the hand's first frame as its catalog, and deciding whether
//! the runtime it advertises may take the hand.
use std::{fmt, future::Future, time::Duration};

use super::RemoteCatalog;

/// Why a hand's first frame gave no catalog.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CatalogError {
    /// The connection ended, failed or stayed silent past the deadline before
    /// its first frame, as from a hand starved for memory or CPU: transient, so
    /// the hand should reconnect rather than stop.
    NoCatalog,
    /// The first frame is not a remote Code Mode catalog (not text, or JSON
    /// without `definitions`, which is how `RunnerPool` tells protocols
    /// apart), such as one from a hand built for an older protocol.
    Outdated,
    /// A catalog frame that does not decode, with the decoder's error.
    Invalid(String),
}

impl fmt::Display for CatalogError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NoCatalog => f.write_str(super::NO_CATALOG),
            Self::Outdated => f.write_str("first frame is not a remote code mode catalog"),
            Self::Invalid(error) => f.write_str(error),
        }
    }
}

impl std::error::Error for CatalogError {}

/// Read a hand's catalog from its first frame. `first` resolves to the first
/// frame as the host reads it: `None` when the connection ended or failed
/// first, `Some(None)` for a frame that is not text (such as binary), and
/// `Some(Some(text))` for a text frame. `timeout` bounds the wait, and
/// `max_frame_bytes` is the host's frame policy for [`RemoteCatalog::decode`].
pub async fn read_catalog(
    first: impl Future<Output = Option<Option<String>>>,
    timeout: Duration,
    max_frame_bytes: usize,
) -> Result<RemoteCatalog, CatalogError> {
    let Ok(Some(frame)) = nanocode_platform::timeout(timeout, first).await else {
        return Err(CatalogError::NoCatalog);
    };
    let text = frame.ok_or(CatalogError::Outdated)?;
    if !serde_json::from_str::<serde_json::Value>(&text)
        .is_ok_and(|frame| frame.get("definitions").is_some())
    {
        return Err(CatalogError::Outdated);
    }
    RemoteCatalog::decode(&text, max_frame_bytes)
        .map_err(|error| CatalogError::Invalid(error.to_string()))
}

/// What a host does with a hand runtime that advertised itself in its catalog
/// ([`RemoteCatalog::resumes`]); see [`admit_runtime`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Admission {
    /// The runtime takes the hand. With `replaces`, it succeeds another
    /// runtime, which is retired: the host forgets that runtime's undelivered
    /// operations (their outcomes are unknown) and fences it from now on.
    Take { replaces: bool },
    /// Another runtime's socket still counts as open. The host can't tell a
    /// live competitor from a socket whose process died or whose computer
    /// slept (a restarted or updated hand reconnecting before the old socket's
    /// heartbeat expired), so the new runtime reconnects and retries until the
    /// old socket closes or is released, rather than stopping for good.
    Wait,
    /// The same runtime reconnected to this host before the host noticed its
    /// previous socket here was lost: it retries.
    Retry,
    /// A retired runtime: another runtime took the hand after it. The host
    /// fences it, even while its successor's socket is open, closing with
    /// [`super::HAND_REPLACED`] so the hand stops for good.
    Fence,
}

/// Whether the runtime `advertised` may take a hand. `previous` is the
/// runtime that last took it, `connected_before` whether `advertised` took it
/// at some time before, and `live` the runtime whose socket still counts as
/// open, with whether this host holds that socket.
///
/// A runtime that reconnects through another host takes the hand over even if
/// that host still counts its socket as open, since reconnecting shows the
/// runtime left it; that host dispatches nothing more to it.
///
/// ```ignore
/// assert_eq!(admit_runtime(Some("old"), true, None, "old"), Admission::Take { replaces: false });
/// assert_eq!(admit_runtime(Some("new"), true, None, "old"), Admission::Fence);
/// assert_eq!(admit_runtime(Some("old"), false, Some(("old", false)), "new"), Admission::Wait);
/// assert_eq!(admit_runtime(Some("new"), true, Some(("new", true)), "new"), Admission::Retry);
/// assert_eq!(admit_runtime(Some("old"), false, None, "new"), Admission::Take { replaces: true });
/// ```
#[must_use]
pub fn admit_runtime(
    previous: Option<&str>,
    connected_before: bool,
    live: Option<(&str, bool)>,
    advertised: &str,
) -> Admission {
    let replaces = previous.is_some_and(|previous| previous != advertised);
    match live {
        _ if replaces && connected_before => Admission::Fence,
        Some((current, _)) if current != advertised => Admission::Wait,
        Some((_, true)) => Admission::Retry,
        Some((_, false)) | None => Admission::Take { replaces },
    }
}
