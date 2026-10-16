#![cfg(not(target_family = "wasm"))]

mod index;

use nanocode_durability_core::{
    OwnedState, OwnerId, OwnerToken, StateStore, StoreError, StoreFuture, StoreRecord, StoreUpdate,
    StoredState,
};
use serde::{Deserialize, Deserializer, Serialize, de};
use std::{
    borrow::Cow,
    collections::{HashMap, HashSet},
    fs::{File, Metadata, OpenOptions},
    io::{BufRead, BufReader, Read, Seek, SeekFrom, Write},
    ops::Range,
    path::{Path, PathBuf},
    time::Instant,
};

#[derive(Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum Entry {
    Acquire {
        state_id: String,
        owner: OwnerToken,
    },
    Replace {
        state_id: String,
        owner: OwnerToken,
        revision: u64,
        payload: String,
        records: Vec<StoreRecord>,
    },
}

#[derive(Deserialize)]
#[serde(rename_all = "snake_case")]
enum Kind {
    Acquire,
    Replace,
}

#[derive(Deserialize)]
struct Records {
    records: Vec<StoreRecord>,
}

#[derive(Deserialize)]
struct Indexed<'a> {
    #[serde(rename = "type")]
    kind: Kind,
    #[serde(borrow)]
    state_id: Cow<'a, str>,
    owner: OwnerToken,
    revision: Option<u64>,
    payload: Option<Text>,
    #[serde(borrow)]
    records: Option<Vec<IndexedRecord<'a>>>,
}

#[derive(Deserialize)]
struct IndexedRecord<'a> {
    #[serde(borrow)]
    key: Cow<'a, str>,
    #[serde(rename = "value")]
    _value: Text,
}

struct Text;

impl<'de> Deserialize<'de> for Text {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        raw_text(deserializer)?;
        Ok(Text)
    }
}

fn raw_text<'de, D: Deserializer<'de>>(
    deserializer: D,
) -> Result<&'de serde_json::value::RawValue, D::Error> {
    let raw = <&serde_json::value::RawValue>::deserialize(deserializer)?;
    if !raw.get().starts_with('"') {
        return Err(de::Error::custom("expected a string"));
    }
    if raw.get().contains("\\u") {
        let mut decoder = serde_json::Deserializer::from_str(raw.get());
        decoder
            .deserialize_str(TextVisitor)
            .map_err(de::Error::custom)?;
    }
    Ok(raw)
}

struct TextVisitor;

impl de::Visitor<'_> for TextVisitor {
    type Value = Text;

    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        formatter.write_str("a string")
    }

    fn visit_str<E: de::Error>(self, _: &str) -> Result<Text, E> {
        Ok(Text)
    }
}

#[derive(Clone, Copy, Eq, Hash, PartialEq, Serialize, Deserialize)]
struct Location {
    position: u64,
    length: usize,
}

#[derive(Serialize, Deserialize)]
struct State {
    revision: u64,
    location: Location,
    records: HashMap<String, Location>,
}

#[derive(Deserialize)]
struct StoredEntry<'a> {
    #[serde(rename = "type")]
    kind: Kind,
    #[serde(borrow)]
    state_id: Cow<'a, str>,
    #[serde(rename = "owner")]
    _owner: OwnerToken,
    revision: u64,
    #[serde(borrow, deserialize_with = "raw_text")]
    payload: &'a serde_json::value::RawValue,
    #[serde(borrow)]
    records: Vec<StoredRecord<'a>>,
}

#[derive(Deserialize)]
struct StoredRecord<'a> {
    #[serde(borrow)]
    key: Cow<'a, str>,
    #[serde(borrow, deserialize_with = "raw_text")]
    value: &'a serde_json::value::RawValue,
}

struct CachedEntry {
    location: Location,
    bytes: Vec<u8>,
    state_id: String,
    revision: u64,
    payload: Range<usize>,
    records: HashMap<String, Range<usize>>,
}

impl CachedEntry {
    fn decode(location: Location, bytes: Vec<u8>) -> Result<Self, StoreError> {
        let entry: StoredEntry = serde_json::from_slice(&bytes).map_err(backend)?;
        if !matches!(entry.kind, Kind::Replace) {
            return Err(backend("JSONL index does not reference a replacement"));
        }
        let range = |value: &serde_json::value::RawValue| {
            let start = value.get().as_ptr().addr() - bytes.as_ptr().addr();
            start..start + value.get().len()
        };
        let payload = range(entry.payload);
        let mut records = HashMap::with_capacity(entry.records.len());
        for record in entry.records {
            records
                .entry(record.key.into_owned())
                .or_insert_with(|| range(record.value));
        }
        Ok(Self {
            location,
            state_id: entry.state_id.into_owned(),
            revision: entry.revision,
            payload,
            records,
            bytes,
        })
    }

    fn text(&self, range: Range<usize>) -> Result<String, StoreError> {
        serde_json::from_slice(&self.bytes[range]).map_err(backend)
    }

    fn record(&self, key: &str) -> Result<String, StoreError> {
        self.text(
            self.records
                .get(key)
                .ok_or_else(|| backend("JSONL record index does not reference its record"))?
                .clone(),
        )
    }
}

pub struct JsonlStore {
    file: File,
    reads: File,
    path: PathBuf,
    index: index::Index,
    persisted_offset: Option<u64>,
    ready: bool,
    invalid_index: Option<String>,
    cached: Option<CachedEntry>,
}

impl JsonlStore {
    pub fn open(path: impl AsRef<Path>) -> Result<Self, StoreError> {
        let _span = tracing::info_span!("durability.jsonl.open").entered();
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(false)
            .open(path.as_ref())
            .map_err(backend)?;
        let path = std::fs::canonicalize(path.as_ref()).map_err(backend)?;
        let reads = File::open(&path).map_err(backend)?;
        let source = identity(&file.metadata().map_err(backend)?)?;
        if identity(&reads.metadata().map_err(backend)?)? != source {
            return Err(identity_changed());
        }
        let mut store = Self {
            file,
            reads,
            path,
            index: index::Index::new(source),
            persisted_offset: None,
            ready: false,
            invalid_index: None,
            cached: None,
        };
        store.restore_index();
        store.locked(|store| {
            if store.persisted_offset.is_none() {
                store.persist_index();
            }
            Ok(())
        })?;
        store.ready = true;
        Ok(store)
    }

    pub fn open_home(home: &Path) -> Result<Self, StoreError> {
        std::fs::create_dir_all(home).map_err(backend)?;
        Self::open(home.join("sessions.jsonl"))
    }

    fn locked<T>(
        &mut self,
        action: impl FnOnce(&mut Self) -> Result<T, StoreError>,
    ) -> Result<T, StoreError> {
        let _span = tracing::info_span!("durability.jsonl.transaction").entered();
        self.file.lock().map_err(backend)?;
        let result = self.refresh_index().and_then(|()| action(self));
        let unlocked = self.file.unlock().map_err(backend);
        match result {
            Ok(value) => unlocked.map(|()| value),
            Err(error) => Err(error),
        }
    }

    fn refresh(&mut self) -> Result<(), StoreError> {
        if identity(&std::fs::metadata(&self.path).map_err(backend)?)? != self.index.source {
            return Err(identity_changed());
        }
        let length = self.file.metadata().map_err(backend)?.len();
        if length < self.index.offset {
            return Err(backend(
                "JSONL durability log was truncated before its committed offset",
            ));
        }
        if length == self.index.offset {
            return Ok(());
        }
        let _span = tracing::info_span!(
            "durability.jsonl.recover",
            log.unindexed_bytes = length - self.index.offset
        )
        .entered();
        let mut reader = BufReader::new(self.file.try_clone().map_err(backend)?);
        reader
            .seek(SeekFrom::Start(self.index.offset))
            .map_err(backend)?;
        let mut line = Vec::new();
        while self.index.offset < length {
            line.clear();
            reader.read_until(b'\n', &mut line).map_err(backend)?;
            if line.last() != Some(&b'\n') {
                self.file.set_len(self.index.offset).map_err(backend)?;
                self.file.sync_all().map_err(backend)?;
                break;
            }
            self.replay(&line)?;
            self.index.last = Some(Location {
                position: self.index.offset,
                length: line.len(),
            });
            self.index.offset = self
                .index
                .offset
                .checked_add(line.len() as u64)
                .ok_or_else(|| backend("JSONL offset overflow"))?;
        }
        Ok(())
    }

    fn replay(&mut self, line: &[u8]) -> Result<(), StoreError> {
        let indexed: Indexed = serde_json::from_slice(line).map_err(backend)?;
        let location = Location {
            position: self.index.offset,
            length: line.len(),
        };
        match indexed.kind {
            Kind::Acquire => self.acquired(&indexed.state_id, indexed.owner),
            Kind::Replace => {
                let (Some(revision), Some(Text), Some(records)) =
                    (indexed.revision, indexed.payload, indexed.records)
                else {
                    return Err(backend("invalid JSONL replacement entry"));
                };
                let id = &*indexed.state_id;
                self.check_replacement(id, &indexed.owner, revision)?;
                if !records.is_empty()
                    && let Err(error) = self.index.load_records(id)
                {
                    self.invalid_index = Some(error.to_string());
                    return Err(backend(error));
                }
                let known = self.index.states.get(id).map(|state| &state.records);
                let mut keys = HashSet::with_capacity(records.len());
                let repeated = records.iter().any(|record| {
                    !keys.insert(&*record.key)
                        || known.is_some_and(|known| known.contains_key(&*record.key))
                });
                if repeated {
                    let Records { records } = serde_json::from_slice(line).map_err(backend)?;
                    self.validate_records(id, &records)?;
                }
                self.replaced(
                    id,
                    revision,
                    location,
                    records.iter().map(|record| &*record.key),
                );
                Ok(())
            }
        }
    }

    fn next_fence(&self, id: &str) -> Option<u64> {
        self.index
            .owners
            .get(id)
            .map_or(Some(1), |owner| owner.fence().checked_add(1))
    }

    fn revision(&self, id: &str) -> u64 {
        self.index.states.get(id).map_or(0, |state| state.revision)
    }

    fn acquired(&mut self, id: &str, owner: OwnerToken) -> Result<(), StoreError> {
        if self.next_fence(id) != Some(owner.fence()) {
            return Err(backend("invalid JSONL owner fence sequence"));
        }
        match self.index.owners.get_mut(id) {
            Some(current) => *current = owner,
            None => {
                self.index.owners.insert(id.into(), owner);
            }
        }
        Ok(())
    }

    fn check_replacement(
        &self,
        id: &str,
        owner: &OwnerToken,
        revision: u64,
    ) -> Result<(), StoreError> {
        if self.index.owners.get(id) != Some(owner)
            || self.revision(id).checked_add(1) != Some(revision)
        {
            return Err(backend("invalid JSONL state revision or owner"));
        }
        Ok(())
    }

    fn replaced<'k>(
        &mut self,
        id: &str,
        revision: u64,
        location: Location,
        keys: impl Iterator<Item = &'k str>,
    ) {
        if !self.index.states.contains_key(id) {
            self.index.created.push(id.into());
            self.index.states.insert(
                id.into(),
                State {
                    revision,
                    location,
                    records: HashMap::new(),
                },
            );
        }
        let state = self
            .index
            .states
            .get_mut(id)
            .expect("a replaced state is indexed");
        state.revision = revision;
        state.location = location;
        for key in keys {
            if !state.records.contains_key(key) {
                state.records.insert(key.into(), location);
            }
        }
    }

    fn entry(&mut self, location: Location) -> Result<&CachedEntry, StoreError> {
        if self
            .cached
            .as_ref()
            .is_none_or(|cached| cached.location != location)
        {
            self.reads
                .seek(SeekFrom::Start(location.position))
                .map_err(backend)?;
            let mut bytes = vec![0; location.length];
            self.reads.read_exact(&mut bytes).map_err(backend)?;
            self.cached = Some(CachedEntry::decode(location, bytes)?);
        }
        Ok(self.cached.as_ref().expect("the entry was loaded"))
    }

    fn state(&mut self, id: &str) -> Result<StoredState, StoreError> {
        let Some(location) = self.index.states.get(id).map(|state| state.location) else {
            return Ok(StoredState::default());
        };
        let entry = self.entry(location)?;
        if entry.state_id != id {
            return Err(backend("JSONL state index does not reference its state"));
        }
        Ok(StoredState {
            revision: entry.revision,
            payload: Some(entry.text(entry.payload.clone())?),
        })
    }

    fn location(&mut self, id: &str, key: &str) -> Result<Option<Location>, StoreError> {
        self.ensure_records(id)?;
        Ok(self
            .index
            .states
            .get(id)
            .and_then(|state| state.records.get(key))
            .copied())
    }

    fn record(&mut self, id: &str, key: &str) -> Result<Option<String>, StoreError> {
        let Some(location) = self.location(id, key)? else {
            return Ok(None);
        };
        let entry = self.entry(location)?;
        if entry.state_id != id {
            return Err(backend("JSONL record index does not reference its state"));
        }
        entry.record(key).map(Some)
    }

    fn validate_records(&mut self, id: &str, records: &[StoreRecord]) -> Result<(), StoreError> {
        let mut staged = HashMap::new();
        for record in records {
            let existing = self.record(id, &record.key)?;
            let retained = existing
                .as_ref()
                .or_else(|| staged.get(&record.key).copied());
            if retained.is_some_and(|value| value != &record.value) {
                return Err(StoreError::NotCommitted(format!(
                    "durability record `{}` has conflicting content",
                    record.key
                )));
            }
            staged.insert(record.key.clone(), &record.value);
        }
        Ok(())
    }

    fn append(&mut self, entry: Entry) -> Result<(), StoreError> {
        self.append_entries(vec![entry])
    }

    fn append_entries(&mut self, entries: Vec<Entry>) -> Result<(), StoreError> {
        let span = tracing::info_span!(
            "durability.jsonl.append",
            entry.count = entries.len(),
            log.bytes = tracing::field::Empty,
            encode.duration_ms = tracing::field::Empty,
            fsync.duration_ms = tracing::field::Empty,
            index.duration_ms = tracing::field::Empty
        );
        let _entered = span.enter();
        let started = Instant::now();
        let mut bytes = Vec::new();
        let mut lengths = Vec::with_capacity(entries.len());
        for entry in &entries {
            let start = bytes.len();
            serde_json::to_writer(&mut bytes, entry).map_err(backend)?;
            bytes.push(b'\n');
            lengths.push(bytes.len() - start);
        }
        span.record("log.bytes", bytes.len());
        span.record(
            "encode.duration_ms",
            started.elapsed().as_secs_f64() * 1_000.0,
        );
        self.file
            .seek(SeekFrom::Start(self.index.offset))
            .map_err(backend)?;
        self.file.write_all(&bytes).map_err(backend)?;
        let started = Instant::now();
        self.file.sync_all().map_err(backend)?;
        span.record(
            "fsync.duration_ms",
            started.elapsed().as_secs_f64() * 1_000.0,
        );
        let started = Instant::now();
        for (entry, length) in entries.into_iter().zip(lengths) {
            let location = Location {
                position: self.index.offset,
                length,
            };
            match entry {
                Entry::Acquire { state_id, owner } => self.acquired(&state_id, owner)?,
                Entry::Replace {
                    state_id,
                    owner,
                    revision,
                    records,
                    ..
                } => {
                    self.check_replacement(&state_id, &owner, revision)?;
                    self.replaced(
                        &state_id,
                        revision,
                        location,
                        records.iter().map(|record| record.key.as_str()),
                    );
                }
            }
            self.index.last = Some(location);
            self.index.offset = self
                .index
                .offset
                .checked_add(length as u64)
                .ok_or_else(|| backend("JSONL offset overflow"))?;
        }
        span.record(
            "index.duration_ms",
            started.elapsed().as_secs_f64() * 1_000.0,
        );
        Ok(())
    }
}

impl StateStore for JsonlStore {
    fn acquire_with<'a>(
        &'a mut self,
        id: &'a str,
        owner_id: OwnerId,
        update: StoreUpdate,
    ) -> StoreFuture<'a, Result<OwnedState, StoreError>> {
        let result = self.locked(|store| {
            let fence = store
                .next_fence(id)
                .ok_or_else(|| StoreError::NotCommitted("JSONL owner fence overflow".into()))?;
            let owner = OwnerToken::new(owner_id, fence);
            let mut state = store.state(id)?;
            let replacement = update(&state)?;
            let mut entries = vec![Entry::Acquire {
                state_id: id.into(),
                owner: owner.clone(),
            }];
            if let Some(replacement) = replacement {
                let revision = state
                    .revision
                    .checked_add(1)
                    .ok_or_else(|| StoreError::NotCommitted("JSONL revision overflow".into()))?;
                store.validate_records(id, &replacement.records)?;
                entries.push(Entry::Replace {
                    state_id: id.into(),
                    owner: owner.clone(),
                    revision,
                    payload: replacement.payload.clone(),
                    records: replacement.records,
                });
                state = StoredState {
                    revision,
                    payload: Some(replacement.payload),
                };
            }
            store.append_entries(entries)?;
            Ok(OwnedState { owner, state })
        });
        Box::pin(async move { result })
    }

    fn read_record<'a>(
        &'a mut self,
        id: &'a str,
        key: &'a str,
    ) -> StoreFuture<'a, Result<Option<String>, StoreError>> {
        let result = self.locked(|store| store.record(id, key));
        Box::pin(async move { result })
    }
    fn read_records<'a>(
        &'a mut self,
        id: &'a str,
        keys: &'a [String],
    ) -> StoreFuture<'a, Result<Vec<Option<String>>, StoreError>> {
        let result = self.locked(|store| {
            let mut grouped = HashMap::<Location, Vec<(usize, &String)>>::new();
            let mut values = vec![None; keys.len()];
            for (index, key) in keys.iter().enumerate() {
                if let Some(location) = store.location(id, key)? {
                    grouped.entry(location).or_default().push((index, key));
                }
            }
            for (location, requested) in grouped {
                let entry = store.entry(location)?;
                if entry.state_id != id {
                    return Err(backend("JSONL record index does not reference its state"));
                }
                for (index, key) in requested {
                    values[index] = Some(entry.record(key)?);
                }
            }
            Ok(values)
        });
        Box::pin(async move { result })
    }
    fn peek<'a>(&'a mut self, id: &'a str) -> StoreFuture<'a, Result<StoredState, StoreError>> {
        let result = self.locked(|store| store.state(id));
        Box::pin(async move { result })
    }
    fn list_states<'a>(
        &'a mut self,
        limit: usize,
    ) -> StoreFuture<'a, Result<Vec<String>, StoreError>> {
        let result = self.locked(|store| {
            Ok(store
                .index
                .created
                .iter()
                .rev()
                .take(limit)
                .cloned()
                .collect())
        });
        Box::pin(async move { result })
    }
    fn acquire<'a>(
        &'a mut self,
        id: &'a str,
        owner_id: OwnerId,
    ) -> StoreFuture<'a, Result<OwnedState, StoreError>> {
        let result = self.locked(|store| {
            let fence = store
                .next_fence(id)
                .ok_or_else(|| StoreError::NotCommitted("JSONL owner fence overflow".into()))?;
            let owner = OwnerToken::new(owner_id, fence);
            store.append(Entry::Acquire {
                state_id: id.into(),
                owner: owner.clone(),
            })?;
            Ok(OwnedState {
                owner,
                state: store.state(id)?,
            })
        });
        Box::pin(async move { result })
    }
    fn replace<'a>(
        &'a mut self,
        id: &'a str,
        owner: &'a OwnerToken,
        expected: u64,
        payload: &'a str,
        records: &'a [StoreRecord],
    ) -> StoreFuture<'a, Result<u64, StoreError>> {
        let result = self.locked(|store| {
            if store.index.owners.get(id) != Some(owner) {
                return Err(StoreError::Fenced);
            }
            let actual = store.revision(id);
            if actual != expected {
                return Err(StoreError::Conflict { expected, actual });
            }
            let revision = actual
                .checked_add(1)
                .ok_or_else(|| StoreError::NotCommitted("JSONL revision overflow".into()))?;
            store.validate_records(id, records)?;
            store.append(Entry::Replace {
                state_id: id.into(),
                owner: owner.clone(),
                revision,
                payload: payload.into(),
                records: records.into(),
            })?;
            Ok(revision)
        });
        Box::pin(async move { result })
    }
}

#[cfg(unix)]
fn identity(metadata: &Metadata) -> Result<String, StoreError> {
    use std::os::unix::fs::MetadataExt;
    Ok(format!("{}:{}", metadata.dev(), metadata.ino()))
}

#[cfg(not(unix))]
fn identity(metadata: &Metadata) -> Result<String, StoreError> {
    Ok(format!("{:?}", metadata.created().map_err(backend)?))
}

fn identity_changed() -> StoreError {
    backend("JSONL log identity changed while its store was open")
}

fn backend(error: impl std::fmt::Display) -> StoreError {
    StoreError::Backend(error.to_string())
}
