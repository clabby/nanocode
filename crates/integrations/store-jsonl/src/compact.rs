use std::{
    collections::HashSet,
    fs::{File, OpenOptions},
    io::{BufRead, BufReader, BufWriter, Seek, SeekFrom, Write},
};

use serde::{Deserialize, Serialize};
use serde_json::value::RawValue;

use super::{Indexed, IndexedRecord, Journal, Kind, Location, OwnerToken, Record, StoreError};
use super::{backend, identity, identity_changed};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CompactionStats {
    pub before_bytes: u64,
    pub after_bytes: u64,
    pub records_removed: u64,
}

#[derive(Deserialize)]
struct Header {
    nanocode_journal: Option<String>,
}

#[derive(Deserialize)]
struct Payload<'a> {
    #[serde(borrow)]
    payload: &'a RawValue,
}

#[derive(Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum Line<'a> {
    Acquire {
        state_id: &'a str,
        owner: &'a OwnerToken,
        #[serde(skip_serializing_if = "Option::is_none")]
        nanocode_journal: Option<&'a str>,
    },
    Replace {
        state_id: &'a str,
        owner: &'a OwnerToken,
        revision: u64,
        payload: &'a RawValue,
        records: &'a [IndexedRecord<'a>],
    },
}

pub(super) fn lineage(file: &mut File, source: &str) -> Result<String, StoreError> {
    file.seek(SeekFrom::Start(0)).map_err(backend)?;
    let mut line = Vec::new();
    BufReader::new(file)
        .read_until(b'\n', &mut line)
        .map_err(backend)?;
    if line.last() != Some(&b'\n') {
        return Ok(source.into());
    }
    let header: Header = serde_json::from_slice(&line).map_err(backend)?;
    Ok(header.nanocode_journal.unwrap_or_else(|| source.into()))
}

impl Journal {
    pub(super) fn reopen_compacted(&mut self) -> Result<(), StoreError> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&self.path)
            .map_err(backend)?;
        let mut reads = File::open(&self.path).map_err(backend)?;
        let source = identity(&file.metadata().map_err(backend)?)?;
        if identity(&reads.metadata().map_err(backend)?)? != source {
            return Ok(());
        }
        if lineage(&mut reads, &source)? != self.lineage {
            return Err(identity_changed());
        }
        self.file = file;
        self.reads = reads;
        self.index = super::index::Index::new(source);
        self.persisted_offset = None;
        self.invalid_index = None;
        self.cached = None;
        self.ready = false;
        self.restore_index();
        Ok(())
    }

    pub(super) fn compact(&mut self) -> Result<CompactionStats, StoreError> {
        let stats = self.locked(|store| store.rebuilt(Self::compact_locked))?;
        self.locked(|store| {
            store.persist_index();
            Ok(stats)
        })
    }

    fn compact_locked(&mut self) -> Result<CompactionStats, StoreError> {
        let span = tracing::info_span!(
            "durability.jsonl.compact",
            before_bytes = self.index.offset,
            after_bytes = tracing::field::Empty,
            records_removed = tracing::field::Empty
        );
        let _entered = span.enter();
        let metadata = self.file.metadata().map_err(backend)?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt;
            if metadata.nlink() != 1 {
                return Err(StoreError::NotCommitted(
                    "cannot compact a journal with hard links".into(),
                ));
            }
        }
        let mut stats = CompactionStats {
            before_bytes: self.index.offset,
            after_bytes: self.index.offset,
            records_removed: 0,
        };
        if stats.before_bytes == 0 {
            return Ok(stats);
        }
        let directory = self
            .path
            .parent()
            .ok_or_else(|| backend("journal has no directory"))?;
        let directory = directory.to_owned();
        self.clear_temporaries().map_err(backend)?;
        let mut temporary = self.temporary(".compact-").map_err(backend)?;
        temporary
            .as_file()
            .set_permissions(metadata.permissions())
            .map_err(backend)?;
        let empty = RawValue::from_string("\"\"".into()).map_err(backend)?;
        let mut input = BufReader::new(File::open(&self.path).map_err(backend)?);
        let mut output = BufWriter::new(temporary.as_file_mut());
        let mut line = Vec::new();
        let mut position = 0_u64;
        while position < self.index.offset {
            line.clear();
            input.read_until(b'\n', &mut line).map_err(backend)?;
            if line.last() != Some(&b'\n') {
                return Err(backend("journal ended during compaction"));
            }
            let location = Location {
                position,
                length: line.len(),
            };
            let indexed: Indexed = serde_json::from_slice(&line).map_err(backend)?;
            let id = indexed.state_id.as_ref();
            match indexed.kind {
                Kind::Acquire => {
                    serde_json::to_writer(
                        &mut output,
                        &Line::Acquire {
                            state_id: id,
                            owner: &indexed.owner,
                            nanocode_journal: (position == 0).then_some(self.lineage.as_str()),
                        },
                    )
                    .map_err(backend)?;
                }
                Kind::Replace => {
                    if indexed.payload.is_none() {
                        return Err(backend("missing payload"));
                    }
                    let revision = indexed
                        .revision
                        .ok_or_else(|| backend("missing revision"))?;
                    let original = if self
                        .index
                        .states
                        .get(id)
                        .is_some_and(|state| state.location == location)
                    {
                        Some(serde_json::from_slice::<Payload<'_>>(&line).map_err(backend)?)
                    } else {
                        None
                    };
                    let mut keys = HashSet::new();
                    let mut records = Vec::new();
                    for record in indexed.records.ok_or_else(|| backend("missing records"))? {
                        let retained = match self.location(id, &record.key)? {
                            Some(Record::Entry(entry)) => {
                                entry == location && keys.insert(record.key.to_string())
                            }
                            Some(Record::Value {
                                location: value,
                                digest,
                            }) => {
                                let current =
                                    Record::located(id, &record.key, &line, position, record.value);
                                if current.location() == value {
                                    if current
                                        != (Record::Value {
                                            location: value,
                                            digest,
                                        })
                                    {
                                        return Err(
                                            self.invalidated("record changed during compaction")
                                        );
                                    }
                                    true
                                } else {
                                    false
                                }
                            }
                            None => {
                                return Err(self.invalidated("record missing during compaction"));
                            }
                        };
                        if retained {
                            records.push(record);
                        } else {
                            stats.records_removed += 1;
                        }
                    }
                    serde_json::to_writer(
                        &mut output,
                        &Line::Replace {
                            state_id: id,
                            owner: &indexed.owner,
                            revision,
                            payload: original
                                .as_ref()
                                .map_or(empty.as_ref(), |value| value.payload),
                            records: &records,
                        },
                    )
                    .map_err(backend)?;
                }
            }
            output.write_all(b"\n").map_err(backend)?;
            position += line.len() as u64;
        }
        output.flush().map_err(backend)?;
        drop(output);
        let length = temporary.as_file().metadata().map_err(backend)?.len();
        if length >= stats.before_bytes {
            stats.records_removed = 0;
            return Ok(stats);
        }
        temporary.as_file().sync_all().map_err(backend)?;
        temporary
            .persist(&self.path)
            .map_err(|error| backend(error.error))?;
        #[cfg(unix)]
        File::open(directory)
            .and_then(|file| file.sync_all())
            .map_err(backend)?;
        stats.after_bytes = length;
        span.record("after_bytes", length);
        span.record("records_removed", stats.records_removed);
        Ok(stats)
    }
}
