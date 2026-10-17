#![cfg(not(target_family = "wasm"))]

mod index;

use nanocode_durability_core::{
    OwnedState, OwnerId, OwnerToken, StateStore, StoreError, StoreFuture, StoreRecord, StoreUpdate,
    StoredState,
};
use serde::{Deserialize, Deserializer, Serialize, Serializer, de, ser::SerializeStruct};
use sha2::{Digest, Sha256};
use std::{
    borrow::Cow,
    cell::{Cell, RefCell},
    collections::{HashMap, HashSet},
    fs::{File, Metadata, OpenOptions},
    io::{BufRead, BufReader, Read, Seek, SeekFrom, Write},
    ops::Range,
    path::{Path, PathBuf},
    thread,
    time::Instant,
};
use tokio::sync::{mpsc, oneshot};

const JOB_CAPACITY: usize = 1;

type Job = Box<dyn FnOnce(&mut Journal) + Send>;

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

#[derive(Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum Line<'a> {
    Acquire {
        state_id: &'a str,
        owner: &'a OwnerToken,
    },
    Replace {
        state_id: &'a str,
        owner: &'a OwnerToken,
        revision: u64,
        payload: &'a str,
        records: Spanned<'a, [StoreRecord]>,
    },
}

#[derive(Default)]
struct Spans {
    written: Cell<usize>,
    values: RefCell<Vec<Range<usize>>>,
}

struct Spanned<'a, T: ?Sized> {
    value: &'a T,
    spans: &'a Spans,
}

impl Serialize for Spanned<'_, [StoreRecord]> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_seq(self.value.iter().map(|value| Spanned {
            value,
            spans: self.spans,
        }))
    }
}

impl Serialize for Spanned<'_, StoreRecord> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut record = serializer.serialize_struct("StoreRecord", 2)?;
        record.serialize_field("key", &self.value.key)?;
        record.serialize_field(
            "value",
            &Spanned {
                value: self.value.value.as_str(),
                spans: self.spans,
            },
        )?;
        record.end()
    }
}

impl Serialize for Spanned<'_, str> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let start = self.spans.written.get();
        let written = serializer.serialize_str(self.value)?;
        self.spans
            .values
            .borrow_mut()
            .push(start..self.spans.written.get());
        Ok(written)
    }
}

struct Counted<'a> {
    bytes: &'a mut Vec<u8>,
    spans: &'a Spans,
}

impl Write for Counted<'_> {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.bytes.extend_from_slice(bytes);
        self.spans.written.set(self.bytes.len());
        Ok(bytes.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
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
    #[serde(borrow, deserialize_with = "raw_text")]
    value: &'a serde_json::value::RawValue,
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

#[derive(Clone, Copy, Eq, PartialEq)]
enum Record {
    Entry(Location),
    Value {
        location: Location,
        digest: [u8; 32],
    },
}

impl Record {
    fn value(id: &str, key: &str, position: u64, bytes: &[u8]) -> Self {
        Self::Value {
            location: Location {
                position,
                length: bytes.len(),
            },
            digest: value_digest(id, key, bytes),
        }
    }

    fn located(
        id: &str,
        key: &str,
        line: &[u8],
        position: u64,
        value: &serde_json::value::RawValue,
    ) -> Self {
        let start = value.get().as_ptr().addr() - line.as_ptr().addr();
        Self::value(id, key, position + start as u64, value.get().as_bytes())
    }

    fn location(self) -> Location {
        match self {
            Self::Entry(location) | Self::Value { location, .. } => location,
        }
    }
}

fn value_digest(id: &str, key: &str, bytes: &[u8]) -> [u8; 32] {
    let mut digest = Sha256::new();
    for part in [id.as_bytes(), key.as_bytes()] {
        digest.update((part.len() as u64).to_le_bytes());
        digest.update(part);
    }
    digest.update(bytes);
    digest.finalize().into()
}

struct State {
    revision: u64,
    location: Location,
    records: HashMap<String, Record>,
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
}

struct Journal {
    file: File,
    reads: File,
    path: PathBuf,
    index: index::Index,
    persisted_offset: Option<u64>,
    ready: bool,
    invalid_index: Option<String>,
    cached: Option<CachedEntry>,
}

impl Journal {
    fn open(path: &Path) -> Result<Self, StoreError> {
        let _span = tracing::info_span!("durability.jsonl.open").entered();
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(false)
            .open(path)
            .map_err(backend)?;
        let path = std::fs::canonicalize(path).map_err(backend)?;
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

    fn home(home: &Path) -> Result<PathBuf, StoreError> {
        std::fs::create_dir_all(home).map_err(backend)?;
        Ok(home.join("sessions.jsonl"))
    }

    fn locked<T>(
        &mut self,
        action: impl FnOnce(&mut Self) -> Result<T, StoreError>,
    ) -> Result<T, StoreError> {
        let span = tracing::info_span!(
            "durability.jsonl.transaction",
            lock.wait_ms = tracing::field::Empty
        );
        let _entered = span.enter();
        let started = Instant::now();
        self.file.lock().map_err(backend)?;
        span.record("lock.wait_ms", started.elapsed().as_secs_f64() * 1_000.0);
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
                    records.iter().map(|record| {
                        (
                            &*record.key,
                            Record::located(id, &record.key, line, location.position, record.value),
                        )
                    }),
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
        records: impl Iterator<Item = (&'k str, Record)>,
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
        for (key, record) in records {
            if !state.records.contains_key(key) {
                state.records.insert(key.into(), record);
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

    fn invalidated(&mut self, error: impl std::fmt::Display) -> StoreError {
        let error = error.to_string();
        self.invalid_index = Some(error.clone());
        backend(error)
    }

    fn rebuilt<T>(
        &mut self,
        mut action: impl FnMut(&mut Self) -> Result<T, StoreError>,
    ) -> Result<T, StoreError> {
        let result = action(self);
        let Some(error) = self.invalid_index.take() else {
            return result;
        };
        self.discard_index(error);
        self.refresh_index()?;
        self.persist_index();
        action(self)
    }

    fn location(&mut self, id: &str, key: &str) -> Result<Option<Record>, StoreError> {
        if let Err(error) = self.index.load_records(id) {
            return Err(self.invalidated(error));
        }
        Ok(self
            .index
            .states
            .get(id)
            .and_then(|state| state.records.get(key))
            .copied())
    }

    fn record(&mut self, id: &str, key: &str) -> Result<Option<String>, StoreError> {
        match self.location(id, key)? {
            None => Ok(None),
            Some(Record::Value { location, digest }) => {
                self.value(id, key, location, digest).map(Some)
            }
            Some(Record::Entry(location)) => self.entry_record(id, key, location).map(Some),
        }
    }

    fn value(
        &mut self,
        id: &str,
        key: &str,
        location: Location,
        digest: [u8; 32],
    ) -> Result<String, StoreError> {
        let mut bytes = vec![0; location.length];
        if let Err(error) = self
            .reads
            .seek(SeekFrom::Start(location.position))
            .and_then(|_| self.reads.read_exact(&mut bytes))
        {
            return Err(self.invalidated(error));
        }
        if value_digest(id, key, &bytes) != digest {
            return Err(self.invalidated("JSONL record index does not match its journal"));
        }
        serde_json::from_slice(&bytes).map_err(|error| self.invalidated(error))
    }

    fn entry_record(
        &mut self,
        id: &str,
        key: &str,
        location: Location,
    ) -> Result<String, StoreError> {
        if let Err(error) = self.entry(location).map(|_| ()) {
            return Err(self.invalidated(error));
        }
        let entry = self.cached.as_ref().expect("the entry was loaded");
        let Some(range) = entry
            .records
            .get(key)
            .filter(|_| entry.state_id == id)
            .cloned()
        else {
            return Err(self.invalidated("JSONL record index does not reference its record"));
        };
        let value = match entry.text(range) {
            Ok(value) => value,
            Err(error) => return Err(self.invalidated(error)),
        };
        let state = self
            .index
            .states
            .get_mut(id)
            .expect("an indexed record has a state");
        if state.records.get(key) == Some(&Record::Entry(location)) {
            for (key, range) in &entry.records {
                if let Some(record) = state.records.get_mut(key)
                    && *record == Record::Entry(location)
                {
                    *record = Record::value(
                        id,
                        key,
                        location.position + range.start as u64,
                        &entry.bytes[range.clone()],
                    );
                }
            }
            self.index.migrated = true;
        }
        if state.location != location {
            self.cached = None;
        }
        Ok(value)
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
        let spans = Spans::default();
        for entry in &entries {
            let start = bytes.len();
            let line = match entry {
                Entry::Acquire { state_id, owner } => Line::Acquire { state_id, owner },
                Entry::Replace {
                    state_id,
                    owner,
                    revision,
                    payload,
                    records,
                } => Line::Replace {
                    state_id,
                    owner,
                    revision: *revision,
                    payload,
                    records: Spanned {
                        value: records,
                        spans: &spans,
                    },
                },
            };
            serde_json::to_writer(
                Counted {
                    bytes: &mut bytes,
                    spans: &spans,
                },
                &line,
            )
            .map_err(backend)?;
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
        let origin = self.index.offset;
        let mut values = spans.values.into_inner().into_iter();
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
                        records.iter().zip(values.by_ref()).map(|(record, span)| {
                            (
                                record.key.as_str(),
                                Record::value(
                                    &state_id,
                                    &record.key,
                                    origin + span.start as u64,
                                    &bytes[span],
                                ),
                            )
                        }),
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

impl Journal {
    fn acquire_batch(
        &mut self,
        id: &str,
        owner_id: OwnerId,
        update: StoreUpdate,
        companion: Option<(&str, OwnerId)>,
    ) -> Result<(OwnedState, Option<OwnedState>), StoreError> {
        if companion
            .as_ref()
            .is_some_and(|(companion_id, _)| *companion_id == id)
        {
            return Err(StoreError::NotCommitted(
                "a companion state must differ from its state".into(),
            ));
        }
        self.locked(|store| {
            let mut entries = Vec::new();
            let companion = match companion {
                Some((companion_id, companion_owner)) => {
                    let fence = store.next_fence(companion_id).ok_or_else(|| {
                        StoreError::NotCommitted("JSONL owner fence overflow".into())
                    })?;
                    let owner = OwnerToken::new(companion_owner, fence);
                    let state = store.state(companion_id)?;
                    entries.push(Entry::Acquire {
                        state_id: companion_id.into(),
                        owner: owner.clone(),
                    });
                    Some((companion_id, OwnedState { owner, state }))
                }
                None => None,
            };
            let fence = store
                .next_fence(id)
                .ok_or_else(|| StoreError::NotCommitted("JSONL owner fence overflow".into()))?;
            let owner = OwnerToken::new(owner_id, fence);
            let mut state = store.state(id)?;
            let current = state.revision;
            let replacement = update(&state)?;
            entries.push(Entry::Acquire {
                state_id: id.into(),
                owner: owner.clone(),
            });
            if let Some(replacement) = replacement {
                let revision = state
                    .revision
                    .checked_add(1)
                    .ok_or_else(|| StoreError::NotCommitted("JSONL revision overflow".into()))?;
                store.rebuilt(|store| store.validate_records(id, &replacement.records))?;
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
            if store.next_fence(id) != Some(fence)
                || store.revision(id) != current
                || companion.as_ref().is_some_and(|(companion_id, companion)| {
                    store.next_fence(companion_id) != Some(companion.owner.fence())
                        || store.revision(companion_id) != companion.state.revision
                })
            {
                return Err(StoreError::NotCommitted(
                    "JSONL index changed while validating records".into(),
                ));
            }
            store.append_entries(entries)?;
            Ok((
                OwnedState { owner, state },
                companion.map(|(_, companion)| companion),
            ))
        })
    }

    fn read_record(&mut self, id: &str, key: &str) -> Result<Option<String>, StoreError> {
        self.locked(|store| store.rebuilt(|store| store.record(id, key)))
    }

    fn read_records(
        &mut self,
        id: &str,
        keys: &[String],
    ) -> Result<Vec<Option<String>>, StoreError> {
        self.locked(|store| {
            store.rebuilt(|store| keys.iter().map(|key| store.record(id, key)).collect())
        })
    }

    fn peek(&mut self, id: &str) -> Result<StoredState, StoreError> {
        self.locked(|store| store.state(id))
    }

    fn list_states(&mut self, limit: usize) -> Result<Vec<String>, StoreError> {
        self.locked(|store| {
            Ok(store
                .index
                .created
                .iter()
                .rev()
                .take(limit)
                .cloned()
                .collect())
        })
    }

    fn acquire(&mut self, id: &str, owner_id: OwnerId) -> Result<OwnedState, StoreError> {
        self.locked(|store| {
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
        })
    }

    fn replace(
        &mut self,
        id: &str,
        owner: &OwnerToken,
        expected: u64,
        payload: String,
        records: Vec<StoreRecord>,
    ) -> Result<u64, StoreError> {
        self.locked(|store| {
            let revision = store.rebuilt(|store| {
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
                store.validate_records(id, &records)?;
                Ok(revision)
            })?;
            store.append(Entry::Replace {
                state_id: id.into(),
                owner: owner.clone(),
                revision,
                payload,
                records,
            })?;
            Ok(revision)
        })
    }
}

pub struct JsonlStore {
    jobs: mpsc::Sender<Job>,
    closed: oneshot::Receiver<()>,
}

impl JsonlStore {
    pub fn open(path: impl AsRef<Path>) -> Result<Self, StoreError> {
        let journal = Journal::open(path.as_ref())?;
        Self::start(move || Ok(journal)).map(|(store, _)| store)
    }

    pub fn open_home(home: &Path) -> Result<Self, StoreError> {
        Self::open(Journal::home(home)?)
    }

    pub async fn open_async(path: impl AsRef<Path>) -> Result<Self, StoreError> {
        let path = path.as_ref().to_owned();
        Self::opened(Self::start(move || Journal::open(&path))?).await
    }

    pub async fn open_home_async(home: &Path) -> Result<Self, StoreError> {
        let home = home.to_owned();
        Self::opened(Self::start(move || Journal::open(&Journal::home(&home)?))?).await
    }

    pub async fn close(self) -> Result<(), StoreError> {
        let Self { jobs, closed } = self;
        drop(jobs);
        closed.await.map_err(|_| stopped())
    }

    fn start(
        open: impl FnOnce() -> Result<Journal, StoreError> + Send + 'static,
    ) -> Result<(Self, oneshot::Receiver<Result<(), StoreError>>), StoreError> {
        let (jobs, mut receiver) = mpsc::channel::<Job>(JOB_CAPACITY);
        let (finished, closed) = oneshot::channel();
        let (ready, opened) = oneshot::channel();
        let span = tracing::Span::current();
        thread::Builder::new()
            .name("nanocode-jsonl".into())
            .spawn(move || {
                let mut journal = match span.in_scope(open) {
                    Ok(journal) => journal,
                    Err(error) => return drop(ready.send(Err(error))),
                };
                drop(ready.send(Ok(())));
                while let Some(job) = receiver.blocking_recv() {
                    job(&mut journal);
                }
                drop(journal);
                let _ = finished.send(());
            })
            .map_err(backend)?;
        Ok((Self { jobs, closed }, opened))
    }

    async fn opened(
        (store, opened): (Self, oneshot::Receiver<Result<(), StoreError>>),
    ) -> Result<Self, StoreError> {
        opened.await.map_err(|_| stopped())??;
        Ok(store)
    }

    fn write<T: Send + 'static>(
        &mut self,
        action: impl FnOnce(&mut Journal) -> Result<T, StoreError> + Send + 'static,
    ) -> StoreFuture<'_, Result<T, StoreError>> {
        self.dispatch(false, action)
    }

    fn read<T: Send + 'static>(
        &mut self,
        action: impl FnOnce(&mut Journal) -> Result<T, StoreError> + Send + 'static,
    ) -> StoreFuture<'_, Result<T, StoreError>> {
        self.dispatch(true, action)
    }

    fn dispatch<T: Send + 'static>(
        &mut self,
        abandonable: bool,
        action: impl FnOnce(&mut Journal) -> Result<T, StoreError> + Send + 'static,
    ) -> StoreFuture<'_, Result<T, StoreError>> {
        let parent = tracing::Span::current();
        let queued = Instant::now();
        let (reply, result) = oneshot::channel();
        let job: Job = Box::new(move |journal| {
            if abandonable && reply.is_closed() {
                return;
            }
            let span = tracing::info_span!(
                parent: &parent,
                "durability.jsonl.worker",
                queue.duration_ms = queued.elapsed().as_secs_f64() * 1_000.0
            );
            let _entered = span.enter();
            drop(reply.send(action(journal)));
        });
        Box::pin(async move {
            self.jobs.send(job).await.map_err(|_| stopped())?;
            result.await.map_err(|_| stopped())?
        })
    }
}

impl StateStore for JsonlStore {
    fn acquire_with<'a>(
        &'a mut self,
        id: &'a str,
        owner_id: OwnerId,
        update: StoreUpdate,
    ) -> StoreFuture<'a, Result<OwnedState, StoreError>> {
        let id = id.to_owned();
        self.write(move |journal| {
            journal
                .acquire_batch(&id, owner_id, update, None)
                .map(|(owned, _)| owned)
        })
    }

    fn batches_companion_acquisition(&self) -> bool {
        true
    }

    fn acquire_with_companion<'a>(
        &'a mut self,
        id: &'a str,
        owner_id: OwnerId,
        update: StoreUpdate,
        companion_id: &'a str,
        companion_owner: OwnerId,
    ) -> StoreFuture<'a, Result<(OwnedState, OwnedState), StoreError>> {
        let (id, companion_id) = (id.to_owned(), companion_id.to_owned());
        self.write(move |journal| {
            journal
                .acquire_batch(
                    &id,
                    owner_id,
                    update,
                    Some((&companion_id, companion_owner)),
                )
                .and_then(|(owned, companion)| {
                    companion
                        .map(|companion| (owned, companion))
                        .ok_or_else(|| backend("JSONL companion acquisition was not staged"))
                })
        })
    }

    fn read_record<'a>(
        &'a mut self,
        id: &'a str,
        key: &'a str,
    ) -> StoreFuture<'a, Result<Option<String>, StoreError>> {
        let (id, key) = (id.to_owned(), key.to_owned());
        self.read(move |journal| journal.read_record(&id, &key))
    }
    fn read_records<'a>(
        &'a mut self,
        id: &'a str,
        keys: &'a [String],
    ) -> StoreFuture<'a, Result<Vec<Option<String>>, StoreError>> {
        let (id, keys) = (id.to_owned(), keys.to_vec());
        self.read(move |journal| journal.read_records(&id, &keys))
    }
    fn peek<'a>(&'a mut self, id: &'a str) -> StoreFuture<'a, Result<StoredState, StoreError>> {
        let id = id.to_owned();
        self.read(move |journal| journal.peek(&id))
    }
    fn list_states<'a>(
        &'a mut self,
        limit: usize,
    ) -> StoreFuture<'a, Result<Vec<String>, StoreError>> {
        self.read(move |journal| journal.list_states(limit))
    }
    fn acquire<'a>(
        &'a mut self,
        id: &'a str,
        owner_id: OwnerId,
    ) -> StoreFuture<'a, Result<OwnedState, StoreError>> {
        let id = id.to_owned();
        self.write(move |journal| journal.acquire(&id, owner_id))
    }
    fn replace<'a>(
        &'a mut self,
        id: &'a str,
        owner: &'a OwnerToken,
        expected: u64,
        payload: &'a str,
        records: &'a [StoreRecord],
    ) -> StoreFuture<'a, Result<u64, StoreError>> {
        let (id, owner, payload, records) = (
            id.to_owned(),
            owner.clone(),
            payload.to_owned(),
            records.to_vec(),
        );
        self.write(move |journal| journal.replace(&id, &owner, expected, payload, records))
    }

    fn flush(&mut self) -> StoreFuture<'_, Result<(), StoreError>> {
        self.write(|journal| {
            journal.flush_index();
            Ok(())
        })
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

fn stopped() -> StoreError {
    backend("JSONL durability worker stopped")
}

fn backend(error: impl std::fmt::Display) -> StoreError {
    StoreError::Backend(error.to_string())
}
