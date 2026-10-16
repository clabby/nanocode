use std::future::Future;

use nanocode_platform::Instant;
use tokio::sync::{mpsc, oneshot};
use tracing::Instrument;

#[cfg(not(target_family = "wasm"))]
use crate::{Error, StoreUpdate};
use crate::{OwnedState, OwnerId, OwnerToken, StateStore, StoreError, StoreFuture};

const COMMAND_CAPACITY: usize = 64;

enum Command {
    ReadRecords {
        state_id: String,
        keys: Vec<String>,
        result: oneshot::Sender<Result<Vec<Option<String>>, StoreError>>,
    },
    ReadRecord {
        state_id: String,
        key: String,
        result: oneshot::Sender<Result<Option<String>, StoreError>>,
    },
    Acquire {
        state_id: String,
        owner_id: OwnerId,
        result: oneshot::Sender<Result<OwnedState, StoreError>>,
    },
    #[cfg(not(target_family = "wasm"))]
    AcquireWith {
        state_id: String,
        owner_id: OwnerId,
        update: StoreUpdate,
        result: oneshot::Sender<Result<OwnedState, StoreError>>,
    },
    Replace {
        state_id: String,
        owner: OwnerToken,
        expected_revision: u64,
        payload: String,
        records: Vec<crate::StoreRecord>,
        result: oneshot::Sender<Result<u64, StoreError>>,
    },
    Peek {
        state_id: String,
        result: oneshot::Sender<Result<crate::StoredState, StoreError>>,
    },
    ListStates {
        limit: usize,
        result: oneshot::Sender<Result<Vec<String>, StoreError>>,
    },
}

struct StoreRequest {
    command: Command,
    span: tracing::Span,
    queued: Instant,
}

#[derive(Clone)]
pub struct SharedStore {
    commands: mpsc::Sender<StoreRequest>,
}

impl SharedStore {
    pub(crate) fn same_store(&self, other: &Self) -> bool {
        self.commands.same_channel(&other.commands)
    }

    pub(crate) fn new<S>(mut store: S) -> crate::Result<Self>
    where
        S: StateStore + 'static,
    {
        let (commands, mut receiver) = mpsc::channel(COMMAND_CAPACITY);
        spawn_driver(async move {
            while let Some(StoreRequest {
                command,
                span,
                queued,
            }) = receiver.recv().await
            {
                span.record(
                    "queue.duration_ms",
                    queued.elapsed().as_secs_f64() * 1_000.0,
                );
                async {
                    match command {
                        Command::ReadRecords {
                            state_id,
                            keys,
                            result,
                        } => {
                            drop(result.send(store.read_records(&state_id, &keys).await));
                        }
                        Command::ReadRecord {
                            state_id,
                            key,
                            result,
                        } => {
                            drop(result.send(store.read_record(&state_id, &key).await));
                        }

                        Command::Acquire {
                            state_id,
                            owner_id,
                            result,
                        } => {
                            drop(result.send(store.acquire(&state_id, owner_id).await));
                        }
                        Command::Replace {
                            state_id,
                            owner,
                            expected_revision,
                            payload,
                            records: new_records,
                            result,
                        } => {
                            drop(
                                result.send(
                                    store
                                        .replace(
                                            &state_id,
                                            &owner,
                                            expected_revision,
                                            &payload,
                                            &new_records,
                                        )
                                        .await,
                                ),
                            );
                        }
                        #[cfg(not(target_family = "wasm"))]
                        Command::AcquireWith {
                            state_id,
                            owner_id,
                            update,
                            result,
                        } => {
                            drop(
                                result.send(store.acquire_with(&state_id, owner_id, update).await),
                            );
                        }
                        Command::Peek { state_id, result } => {
                            drop(result.send(store.peek(&state_id).await));
                        }
                        Command::ListStates { limit, result } => {
                            drop(result.send(store.list_states(limit).await));
                        }
                    }
                }
                .instrument(span)
                .await;
            }
        })?;
        Ok(Self { commands })
    }
}

impl StateStore for SharedStore {
    #[cfg(not(target_family = "wasm"))]
    fn acquire_with<'a>(
        &'a mut self,
        state_id: &'a str,
        owner_id: OwnerId,
        update: StoreUpdate,
    ) -> StoreFuture<'a, Result<OwnedState, StoreError>> {
        let span = tracing::info_span!(
            "durability.store",
            operation = "acquire_with",
            state.id = state_id,
            queue.duration_ms = tracing::field::Empty
        );
        Box::pin(async move {
            let (result, receiver) = oneshot::channel();
            self.commands
                .send(StoreRequest {
                    command: Command::AcquireWith {
                        state_id: state_id.to_owned(),
                        owner_id,
                        update,
                        result,
                    },
                    span,
                    queued: Instant::now(),
                })
                .await
                .map_err(|_| stopped())?;
            receiver.await.map_err(|_| stopped())?
        })
    }

    fn read_records<'a>(
        &'a mut self,
        state_id: &'a str,
        keys: &'a [String],
    ) -> StoreFuture<'a, Result<Vec<Option<String>>, StoreError>> {
        let span = tracing::info_span!(
            "durability.store",
            operation = "read_records",
            state.id = state_id,
            record.count = keys.len(),
            queue.duration_ms = tracing::field::Empty
        );
        Box::pin(async move {
            let (result, receiver) = oneshot::channel();
            self.commands
                .send(StoreRequest {
                    command: Command::ReadRecords {
                        state_id: state_id.to_owned(),
                        keys: keys.to_vec(),
                        result,
                    },
                    span,
                    queued: Instant::now(),
                })
                .await
                .map_err(|_| stopped())?;
            receiver.await.map_err(|_| stopped())?
        })
    }

    fn read_record<'a>(
        &'a mut self,
        state_id: &'a str,
        key: &'a str,
    ) -> StoreFuture<'a, Result<Option<String>, StoreError>> {
        let span = tracing::info_span!(
            "durability.store",
            operation = "read_record",
            state.id = state_id,
            queue.duration_ms = tracing::field::Empty
        );
        Box::pin(async move {
            let (result, receiver) = oneshot::channel();
            self.commands
                .send(StoreRequest {
                    command: Command::ReadRecord {
                        state_id: state_id.to_owned(),
                        key: key.to_owned(),
                        result,
                    },
                    span,
                    queued: Instant::now(),
                })
                .await
                .map_err(|_| stopped())?;
            receiver.await.map_err(|_| stopped())?
        })
    }

    fn acquire<'a>(
        &'a mut self,
        state_id: &'a str,
        owner_id: OwnerId,
    ) -> StoreFuture<'a, Result<OwnedState, StoreError>> {
        let span = tracing::info_span!(
            "durability.store",
            operation = "acquire",
            state.id = state_id,
            queue.duration_ms = tracing::field::Empty
        );
        Box::pin(async move {
            let (result, receiver) = oneshot::channel();
            self.commands
                .send(StoreRequest {
                    command: Command::Acquire {
                        state_id: state_id.to_owned(),
                        owner_id,
                        result,
                    },
                    span,
                    queued: Instant::now(),
                })
                .await
                .map_err(|_| stopped())?;
            receiver.await.map_err(|_| stopped())?
        })
    }

    fn replace<'a>(
        &'a mut self,
        state_id: &'a str,
        owner: &'a OwnerToken,
        expected_revision: u64,
        payload: &'a str,
        records: &'a [crate::StoreRecord],
    ) -> StoreFuture<'a, Result<u64, StoreError>> {
        let span = tracing::info_span!(
            "durability.store",
            operation = "replace",
            state.id = state_id,
            payload.bytes = payload.len(),
            record.count = records.len(),
            queue.duration_ms = tracing::field::Empty
        );
        Box::pin(async move {
            let (result, receiver) = oneshot::channel();
            self.commands
                .send(StoreRequest {
                    command: Command::Replace {
                        state_id: state_id.to_owned(),
                        owner: owner.clone(),
                        expected_revision,
                        payload: payload.to_owned(),
                        records: records.to_vec(),
                        result,
                    },
                    span,
                    queued: Instant::now(),
                })
                .await
                .map_err(|_| stopped())?;
            receiver.await.map_err(|_| stopped())?
        })
    }

    fn peek<'a>(
        &'a mut self,
        state_id: &'a str,
    ) -> StoreFuture<'a, Result<crate::StoredState, StoreError>> {
        let span = tracing::info_span!(
            "durability.store",
            operation = "peek",
            state.id = state_id,
            queue.duration_ms = tracing::field::Empty
        );
        Box::pin(async move {
            let (result, receiver) = oneshot::channel();
            self.commands
                .send(StoreRequest {
                    command: Command::Peek {
                        state_id: state_id.to_owned(),
                        result,
                    },
                    span,
                    queued: Instant::now(),
                })
                .await
                .map_err(|_| stopped())?;
            receiver.await.map_err(|_| stopped())?
        })
    }

    fn list_states<'a>(
        &'a mut self,
        limit: usize,
    ) -> StoreFuture<'a, Result<Vec<String>, StoreError>> {
        let span = tracing::info_span!(
            "durability.store",
            operation = "list_states",
            state.limit = limit,
            queue.duration_ms = tracing::field::Empty
        );
        Box::pin(async move {
            let (result, receiver) = oneshot::channel();
            self.commands
                .send(StoreRequest {
                    command: Command::ListStates { limit, result },
                    span,
                    queued: Instant::now(),
                })
                .await
                .map_err(|_| stopped())?;
            receiver.await.map_err(|_| stopped())?
        })
    }
}

fn stopped() -> StoreError {
    StoreError::Backend("shared durability store stopped".to_owned())
}

#[cfg(not(target_family = "wasm"))]
fn spawn_driver(driver: impl Future<Output = ()> + Send + 'static) -> crate::Result<()> {
    let runtime = tokio::runtime::Handle::try_current().map_err(|_| Error::RuntimeUnavailable)?;
    drop(runtime.spawn(driver));
    Ok(())
}

#[cfg(target_family = "wasm")]
fn spawn_driver(driver: impl Future<Output = ()> + 'static) -> crate::Result<()> {
    nanocode_platform::spawn_local(driver);
    Ok(())
}
