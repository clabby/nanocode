use std::{collections::HashSet, sync::Arc};

use nanocode_durability::{
    EncodedPayload, PagedWriter, PayloadReader, Prepared, Result, Sequence, SequenceCache,
    load_sequence,
};
use serde::{Deserialize, Serialize};
use serde_json::value::RawValue;

use crate::{
    client::responses::{ResponseHistory, ResponseItem},
    execution::ExecutionContinuation,
    session::{SessionSnapshot, SessionSnapshotHead},
};

#[derive(Deserialize, Serialize)]
struct Continuation {
    state: Box<RawValue>,
    history: Sequence,
    prefix: Sequence,
}

pub(crate) fn prepare_continuation(
    value: ExecutionContinuation,
    known: &HashSet<String>,
) -> Result<Prepared> {
    let mut writer = PagedWriter::new(known);
    let history = writer.sequence(&value.history)?;
    let prefix = writer.sequence(&value.prefix)?;
    let state = RawValue::from_string(value.state_json)
        .map_err(nanocode_durability::Error::InvalidPayload)?;
    writer.finish(&Continuation {
        state,
        history,
        prefix,
    })
}

#[derive(Default)]
pub(crate) struct ContinuationCache {
    history: ResponseHistory,
    history_pages: SequenceCache,
    prefix: Arc<[ResponseItem]>,
    prefix_pages: SequenceCache,
}

impl ContinuationCache {
    pub(crate) fn committed(&mut self, history: ResponseHistory, prefix: Arc<[ResponseItem]>) {
        self.history = history;
        self.history_pages.committed();
        self.prefix = prefix;
        self.prefix_pages.committed();
    }
}

pub(crate) fn prepare_cached_continuation(
    state_json: String,
    history: &ResponseHistory,
    prefix: &Arc<[ResponseItem]>,
    cache: &mut ContinuationCache,
    known: &HashSet<String>,
) -> Result<Prepared> {
    let mut writer = PagedWriter::new(known);
    let items = history.iter().collect::<Vec<_>>();
    let unchanged = cache.history.shared_prefix_len(history);
    let history_sequence = writer.sequence_cached(&items, unchanged, &mut cache.history_pages)?;
    let unchanged = if Arc::ptr_eq(&cache.prefix, prefix) {
        prefix.len()
    } else {
        0
    };
    let prefix_sequence = writer.sequence_cached(prefix, unchanged, &mut cache.prefix_pages)?;
    let state =
        RawValue::from_string(state_json).map_err(nanocode_durability::Error::InvalidPayload)?;
    writer.finish(&Continuation {
        state,
        history: history_sequence,
        prefix: prefix_sequence,
    })
}

pub(crate) async fn load_continuation(
    reader: PayloadReader<'_>,
    payload: EncodedPayload,
) -> Result<(ExecutionContinuation, HashSet<String>)> {
    let value: Continuation = payload.decode()?;
    let mut keys = HashSet::new();
    let continuation = ExecutionContinuation {
        state_json: value.state.get().to_owned(),
        history: load_sequence(reader, value.history, &mut keys).await?,
        prefix: load_sequence(reader, value.prefix, &mut keys).await?,
    };
    Ok((continuation, keys))
}

#[derive(Deserialize)]
struct ContinuationHistory {
    history: Sequence,
}

pub(crate) async fn continuation_history(
    reader: PayloadReader<'_>,
    continuation: serde_json::Value,
) -> Result<Vec<serde_json::Value>> {
    let value: ContinuationHistory =
        serde_json::from_value(continuation).map_err(nanocode_durability::Error::InvalidPayload)?;
    load_sequence(reader, value.history, &mut HashSet::new()).await
}

#[derive(Deserialize, Serialize)]
pub(crate) struct Snapshot {
    head: SessionSnapshotHead,
    history: Sequence,
    prefix: Option<Sequence>,
}

pub(crate) fn prepare_snapshot(
    value: SessionSnapshot,
    known: &HashSet<String>,
) -> Result<Prepared> {
    let (head, history, prefix) = value.into_context_parts();
    let mut writer = PagedWriter::new(known);
    let history = writer.sequence(&history)?;
    let prefix = prefix.map(|items| writer.sequence(&items)).transpose()?;
    writer.finish(&Snapshot {
        head,
        history,
        prefix,
    })
}

pub(crate) async fn load_snapshot(
    reader: PayloadReader<'_>,
    payload: EncodedPayload,
) -> Result<SessionSnapshot> {
    restore_snapshot(reader, payload.decode()?).await
}

pub(crate) async fn restore_snapshot(
    reader: PayloadReader<'_>,
    saved: Snapshot,
) -> Result<SessionSnapshot> {
    load_snapshot_with_keys(reader, saved)
        .await
        .map(|(snapshot, _)| snapshot)
}

pub(crate) fn preloaded_snapshot(
    checkpoint: &serde_json::Value,
    key: &str,
) -> Option<(SessionSnapshot, HashSet<String>)> {
    let prepared = prepare_snapshot(
        SessionSnapshot::deserialize(checkpoint).ok()?,
        &HashSet::new(),
    )
    .ok()?;
    if !prepared.payload.matches_key(key) {
        return None;
    }
    Some((
        SessionSnapshot::deserialize(checkpoint).ok()?,
        prepared.keys,
    ))
}

pub(crate) async fn load_snapshot_with_keys(
    reader: PayloadReader<'_>,
    saved: Snapshot,
) -> Result<(SessionSnapshot, HashSet<String>)> {
    let mut keys = HashSet::new();
    let history = load_sequence(reader, saved.history, &mut keys).await?;
    let prefix = match saved.prefix {
        Some(value) => Some(load_sequence(reader, value, &mut keys).await?),
        None => None,
    };
    Ok((saved.head.with_context(history, prefix), keys))
}
