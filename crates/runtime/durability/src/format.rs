use std::{
    fmt,
    future::Future,
    pin::Pin,
    sync::{Arc, RwLock, RwLockReadGuard},
};

use nanocode_agent_core::{HarnessFamily, SessionCheckpoint, session::TranscriptItem};
use serde_json::Value;

use crate::{EncodedPayload, Error, PayloadReader, Result, SessionRecord};

#[cfg(not(target_family = "wasm"))]
pub type FormatFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;
#[cfg(target_family = "wasm")]
pub type FormatFuture<'a, T> = Pin<Box<dyn Future<Output = T> + 'a>>;

pub struct AdmittedTurn<'a> {
    pub input: &'a Value,
    pub steers: usize,
}

pub trait CheckpointFormat: Send + Sync {
    fn family(&self) -> HarnessFamily;

    fn owns(&self, checkpoint: &Value) -> bool;

    fn hydrate<'a>(
        &'a self,
        _reader: PayloadReader<'a>,
        stored: Value,
    ) -> FormatFuture<'a, Result<Value>> {
        Box::pin(async move { Ok(stored) })
    }

    fn hydrate_continuation<'a>(
        &'a self,
        _reader: PayloadReader<'a>,
        _continuation: Value,
    ) -> FormatFuture<'a, Result<Option<Value>>> {
        Box::pin(async { Ok(None) })
    }

    fn encode(&self, checkpoint: Value) -> Result<EncodedPayload> {
        EncodedPayload::encode(&checkpoint)
    }

    fn session_checkpoint(
        &self,
        record: &SessionRecord,
        checkpoint: Value,
    ) -> Result<SessionCheckpoint>;

    fn transcript(&self, checkpoint: &Value, turns: &[AdmittedTurn<'_>]) -> Vec<TranscriptItem>;
}

#[derive(Clone, Default)]
pub(crate) struct Formats(Arc<RwLock<Vec<Arc<dyn CheckpointFormat>>>>);

impl Formats {
    fn read(&self) -> Result<RwLockReadGuard<'_, Vec<Arc<dyn CheckpointFormat>>>> {
        self.0
            .read()
            .map_err(|_| Error::InvalidState("checkpoint format registry poisoned".into()))
    }

    pub(crate) fn register(&self, format: Arc<dyn CheckpointFormat>) -> Result<()> {
        let mut formats = self
            .0
            .write()
            .map_err(|_| Error::InvalidState("checkpoint format registry poisoned".into()))?;
        formats.retain(|known| known.family() != format.family());
        formats.push(format);
        Ok(())
    }

    pub(crate) fn family(&self, family: HarnessFamily) -> Result<Arc<dyn CheckpointFormat>> {
        self.read()?
            .iter()
            .find(|format| format.family() == family)
            .cloned()
            .ok_or_else(|| Error::UnsupportedFamily(family.to_string()))
    }

    pub(crate) fn owner(&self, checkpoint: &Value) -> Result<Arc<dyn CheckpointFormat>> {
        self.read()?
            .iter()
            .find(|format| format.owns(checkpoint))
            .cloned()
            .ok_or_else(|| {
                Error::InvalidState("no registered checkpoint format owns this checkpoint".into())
            })
    }
}

impl fmt::Debug for Formats {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let families: Vec<_> = self
            .0
            .read()
            .map(|formats| formats.iter().map(|format| format.family()).collect())
            .unwrap_or_default();
        formatter.debug_list().entries(families).finish()
    }
}
