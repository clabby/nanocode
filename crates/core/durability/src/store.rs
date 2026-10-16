use std::{future::Future, pin::Pin};

#[derive(Clone, Debug, Eq, Hash, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(transparent)]
pub struct OwnerId(String);

impl OwnerId {
    pub fn new() -> Self {
        #[cfg(not(target_family = "wasm"))]
        let id = uuid::Uuid::now_v7();
        #[cfg(target_family = "wasm")]
        let id = nanocode_platform::uuid_v7();
        Self(id.to_string())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl Default for OwnerId {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct OwnerToken {
    owner_id: OwnerId,
    fence: u64,
}

impl OwnerToken {
    pub const fn new(owner_id: OwnerId, fence: u64) -> Self {
        Self { owner_id, fence }
    }

    pub const fn owner_id(&self) -> &OwnerId {
        &self.owner_id
    }

    pub const fn fence(&self) -> u64 {
        self.fence
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct StoredState {
    pub revision: u64,

    pub payload: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct OwnedState {
    pub owner: OwnerToken,

    pub state: StoredState,
}

#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
pub enum StoreError {
    #[error("durability state owner was fenced")]
    Fenced,

    #[error("durability state revision conflict: expected {expected}, found {actual}")]
    Conflict { expected: u64, actual: u64 },

    #[error("durability store operation was not committed: {0}")]
    NotCommitted(String),

    #[error("durability store failed: {0}")]
    Backend(String),
}

#[cfg(not(target_family = "wasm"))]
pub type StoreFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

#[cfg(target_family = "wasm")]
pub type StoreFuture<'a, T> = Pin<Box<dyn Future<Output = T> + 'a>>;

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct StoreRecord {
    pub key: String,

    pub value: String,
}

pub struct StoreReplacement {
    pub payload: String,
    pub records: Vec<StoreRecord>,
}

#[cfg(not(target_family = "wasm"))]
pub type StoreUpdate =
    Box<dyn FnOnce(&StoredState) -> Result<Option<StoreReplacement>, StoreError> + Send>;

#[cfg(target_family = "wasm")]
pub type StoreUpdate =
    Box<dyn FnOnce(&StoredState) -> Result<Option<StoreReplacement>, StoreError>>;

async fn acquire_updated<S: StateStore + ?Sized>(
    store: &mut S,
    state_id: &str,
    owner_id: OwnerId,
    update: StoreUpdate,
) -> Result<OwnedState, StoreError> {
    let mut owned = store.acquire(state_id, owner_id).await?;
    if let Some(replacement) = update(&owned.state).map_err(|error| {
        StoreError::Backend(format!(
            "state update failed after acquiring its owner: {error}"
        ))
    })? {
        let revision = store
            .replace(
                state_id,
                &owned.owner,
                owned.state.revision,
                &replacement.payload,
                &replacement.records,
            )
            .await
            .map_err(|error| {
                StoreError::Backend(format!(
                    "state update failed after acquiring its owner: {error}"
                ))
            })?;
        owned.state = StoredState {
            revision,
            payload: Some(replacement.payload),
        };
    }
    Ok(owned)
}

async fn acquire_companion<S: StateStore + ?Sized>(
    store: &mut S,
    state_id: &str,
    owner_id: OwnerId,
    update: StoreUpdate,
    companion_id: &str,
    companion_owner: OwnerId,
) -> Result<(OwnedState, OwnedState), StoreError> {
    if companion_id == state_id {
        return Err(StoreError::NotCommitted(
            "a companion state must differ from its state".to_owned(),
        ));
    }
    let companion = store.acquire(companion_id, companion_owner).await?;
    let owned = store
        .acquire_with(state_id, owner_id, update)
        .await
        .map_err(|error| {
            StoreError::Backend(format!(
                "state acquisition failed after acquiring its companion: {error}"
            ))
        })?;
    Ok((owned, companion))
}

fn read_only_unsupported() -> StoreError {
    StoreError::Backend("this store does not support non-fencing reads".to_owned())
}

#[cfg(not(target_family = "wasm"))]
pub trait StateStore: Send {
    fn read_record<'a>(
        &'a mut self,
        state_id: &'a str,
        key: &'a str,
    ) -> StoreFuture<'a, Result<Option<String>, StoreError>>;

    fn read_records<'a>(
        &'a mut self,
        state_id: &'a str,
        keys: &'a [String],
    ) -> StoreFuture<'a, Result<Vec<Option<String>>, StoreError>> {
        Box::pin(async move {
            let mut records = Vec::with_capacity(keys.len());
            for key in keys {
                records.push(self.read_record(state_id, key).await?);
            }
            Ok(records)
        })
    }

    fn peek<'a>(
        &'a mut self,
        _state_id: &'a str,
    ) -> StoreFuture<'a, Result<StoredState, StoreError>> {
        Box::pin(async { Err(read_only_unsupported()) })
    }

    fn list_states<'a>(
        &'a mut self,
        _limit: usize,
    ) -> StoreFuture<'a, Result<Vec<String>, StoreError>> {
        Box::pin(async { Err(read_only_unsupported()) })
    }

    fn acquire<'a>(
        &'a mut self,
        state_id: &'a str,
        owner_id: OwnerId,
    ) -> StoreFuture<'a, Result<OwnedState, StoreError>>;

    fn acquire_with<'a>(
        &'a mut self,
        state_id: &'a str,
        owner_id: OwnerId,
        update: StoreUpdate,
    ) -> StoreFuture<'a, Result<OwnedState, StoreError>> {
        Box::pin(acquire_updated(self, state_id, owner_id, update))
    }

    fn batches_companion_acquisition(&self) -> bool {
        false
    }

    fn acquire_with_companion<'a>(
        &'a mut self,
        state_id: &'a str,
        owner_id: OwnerId,
        update: StoreUpdate,
        companion_id: &'a str,
        companion_owner: OwnerId,
    ) -> StoreFuture<'a, Result<(OwnedState, OwnedState), StoreError>> {
        Box::pin(acquire_companion(
            self,
            state_id,
            owner_id,
            update,
            companion_id,
            companion_owner,
        ))
    }

    fn replace<'a>(
        &'a mut self,
        state_id: &'a str,
        owner: &'a OwnerToken,
        expected_revision: u64,
        payload: &'a str,
        records: &'a [crate::StoreRecord],
    ) -> StoreFuture<'a, Result<u64, StoreError>>;
}

#[cfg(target_family = "wasm")]
pub trait StateStore {
    fn read_record<'a>(
        &'a mut self,
        state_id: &'a str,
        key: &'a str,
    ) -> StoreFuture<'a, Result<Option<String>, StoreError>>;

    fn read_records<'a>(
        &'a mut self,
        state_id: &'a str,
        keys: &'a [String],
    ) -> StoreFuture<'a, Result<Vec<Option<String>>, StoreError>> {
        Box::pin(async move {
            let mut records = Vec::with_capacity(keys.len());
            for key in keys {
                records.push(self.read_record(state_id, key).await?);
            }
            Ok(records)
        })
    }

    fn peek<'a>(
        &'a mut self,
        _state_id: &'a str,
    ) -> StoreFuture<'a, Result<StoredState, StoreError>> {
        Box::pin(async { Err(read_only_unsupported()) })
    }

    fn list_states<'a>(
        &'a mut self,
        _limit: usize,
    ) -> StoreFuture<'a, Result<Vec<String>, StoreError>> {
        Box::pin(async { Err(read_only_unsupported()) })
    }

    fn acquire<'a>(
        &'a mut self,
        state_id: &'a str,
        owner_id: OwnerId,
    ) -> StoreFuture<'a, Result<OwnedState, StoreError>>;

    fn acquire_with<'a>(
        &'a mut self,
        state_id: &'a str,
        owner_id: OwnerId,
        update: StoreUpdate,
    ) -> StoreFuture<'a, Result<OwnedState, StoreError>> {
        Box::pin(acquire_updated(self, state_id, owner_id, update))
    }

    fn batches_companion_acquisition(&self) -> bool {
        false
    }

    fn acquire_with_companion<'a>(
        &'a mut self,
        state_id: &'a str,
        owner_id: OwnerId,
        update: StoreUpdate,
        companion_id: &'a str,
        companion_owner: OwnerId,
    ) -> StoreFuture<'a, Result<(OwnedState, OwnedState), StoreError>> {
        Box::pin(acquire_companion(
            self,
            state_id,
            owner_id,
            update,
            companion_id,
            companion_owner,
        ))
    }

    fn replace<'a>(
        &'a mut self,
        state_id: &'a str,
        owner: &'a OwnerToken,
        expected_revision: u64,
        payload: &'a str,
        records: &'a [crate::StoreRecord],
    ) -> StoreFuture<'a, Result<u64, StoreError>>;
}
