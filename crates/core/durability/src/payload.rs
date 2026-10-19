use std::sync::Arc;

use serde::{Serialize, de::DeserializeOwned};

use crate::{Error, Result};

const INLINE_BYTES: usize = 16 * 1024;

const CHUNK_MIN: usize = 2 * 1024;
const CHUNK_AVG: usize = 8 * 1024;
const CHUNK_MAX: usize = 32 * 1024;

const CHUNK_MASK_SMALL: u64 = !(u64::MAX >> 15);
const CHUNK_MASK_LARGE: u64 = !(u64::MAX >> 11);

const MANIFEST_PREFIX: char = '%';

const SHA256_MANIFEST_PREFIX: char = '#';

const TREE_PREFIX: char = '^';

const KEY_PREFIX: &str = "b3:";

const GEAR: [u64; 256] = gear_table();

const fn gear_table() -> [u64; 256] {
    let mut table = [0_u64; 256];
    let mut state: u64 = 0x6e61_6e6f_636f_6465;
    let mut index = 0;
    while index < 256 {
        state = state.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut value = state;
        value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        table[index] = value ^ (value >> 31);
        index += 1;
    }
    table
}

fn chunk_end(content: &str) -> usize {
    let bytes = content.as_bytes();
    let length = bytes.len();

    if length <= CHUNK_MIN * 2 {
        return length;
    }
    let limit = length.min(CHUNK_MAX);
    let normal = limit.min(CHUNK_AVG);
    let mut hash = 0_u64;
    let mut index = CHUNK_MIN;
    let mut cut = limit;
    while index < limit {
        hash = (hash << 1).wrapping_add(GEAR[bytes[index] as usize]);
        let mask = if index < normal {
            CHUNK_MASK_SMALL
        } else {
            CHUNK_MASK_LARGE
        };
        if hash & mask == 0 {
            cut = index + 1;
            break;
        }
        index += 1;
    }
    content.ceil_char_boundary(cut)
}

fn chunk_key(hash: &str) -> String {
    format!("c:{hash}")
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
#[serde(transparent)]
pub struct EncodedPayload {
    key: Arc<str>,
    #[serde(skip)]
    content: Option<Arc<str>>,
    #[serde(skip)]
    layout: Option<Arc<Layout>>,
    #[serde(skip)]
    pending: Vec<crate::StoreRecord>,
}

#[derive(Debug)]
struct Layout {
    hashes: String,
    ends: Vec<usize>,
    nodes: Vec<(String, String)>,
    root: String,
}

impl Layout {
    fn of(content: &str) -> Self {
        let mut hashes = String::with_capacity(content.len() / CHUNK_MIN * HASH_HEX);
        let mut ends = Vec::with_capacity(content.len() / CHUNK_AVG + 1);
        let mut offset = 0;
        while offset < content.len() {
            let end = offset + chunk_end(&content[offset..]);
            hashes.push_str(&content_hash(&content[offset..end]));
            ends.push(end);
            offset = end;
        }
        let mut nodes = Vec::new();
        let root = manifest_tree(MANIFEST_PREFIX, &hashes, &mut nodes);
        Self {
            hashes,
            ends,
            nodes,
            root,
        }
    }

    fn hash(&self, index: usize) -> &str {
        &self.hashes[index * HASH_HEX..(index + 1) * HASH_HEX]
    }
}

const HASH_HEX: usize = 64;

fn manifest_tree(prefix: char, hashes: &str, nodes: &mut Vec<(String, String)>) -> String {
    if 1 + hashes.len() < INLINE_BYTES {
        return format!("{prefix}{hashes}");
    }
    let mut parents = String::new();
    let mut group = String::from(prefix);
    let mut count = 0;
    for hash in hashes.as_bytes().chunks(HASH_HEX) {
        let hash = std::str::from_utf8(hash).unwrap_or_default();
        group.push_str(hash);
        count += 1;
        if count >= CHUNK_MAX / HASH_HEX || (count >= CHUNK_MIN / HASH_HEX && node_cut(hash)) {
            close_group(&mut group, &mut parents, nodes, prefix);
            count = 0;
        }
    }
    if count > 0 {
        close_group(&mut group, &mut parents, nodes, prefix);
    }
    manifest_tree(TREE_PREFIX, &parents, nodes)
}

fn node_cut(hash: &str) -> bool {
    u32::from_str_radix(&hash[..8], 16)
        .is_ok_and(|value| value % (CHUNK_AVG / HASH_HEX) as u32 == 0)
}

fn close_group(
    group: &mut String,
    parents: &mut String,
    nodes: &mut Vec<(String, String)>,
    prefix: char,
) {
    let text = std::mem::replace(group, String::from(prefix));
    let hash = content_hash(&text);
    parents.push_str(&hash);
    nodes.push((hash, text));
}

fn keyed(json: &str) -> (Arc<str>, Option<Arc<Layout>>) {
    if json.len() < INLINE_BYTES {
        return (record_key(json).into(), None);
    }
    let layout = Layout::of(json);
    (record_key(&layout.root).into(), Some(Arc::new(layout)))
}

impl EncodedPayload {
    pub fn key(&self) -> &str {
        &self.key
    }

    pub fn matches_key(&self, key: &str) -> bool {
        self.key.as_ref() == key
            || (!key.starts_with(KEY_PREFIX)
                && self
                    .content
                    .as_deref()
                    .is_some_and(|content| sha256_key(content) == key))
    }

    pub fn encode<T: Serialize + ?Sized>(value: &T) -> Result<Self> {
        let json = serde_json::to_string(value).map_err(Error::InvalidPayload)?;
        Ok(Self::from_json(json.into()))
    }

    pub fn from_json(json: Arc<str>) -> Self {
        let (key, layout) = keyed(&json);
        Self {
            key,
            content: Some(json),
            layout,
            pending: Vec::new(),
        }
    }

    pub fn by_key(key: &str) -> Self {
        Self {
            key: key.into(),
            content: None,
            layout: None,
            pending: Vec::new(),
        }
    }

    pub fn decode<T: DeserializeOwned>(&self) -> Result<T> {
        serde_json::from_str(self.json()?).map_err(Error::InvalidPayload)
    }

    pub fn json(&self) -> Result<&str> {
        self.content.as_deref().ok_or_else(|| {
            Error::InvalidState(
                "resolve the payload through its durable session before reading it".into(),
            )
        })
    }

    #[cfg_attr(target_family = "wasm", allow(dead_code))]
    pub fn from_key(key: &str) -> Self {
        Self::by_key(key)
    }

    pub fn reference(&self) -> Self {
        Self::by_key(&self.key)
    }

    pub fn with_records(mut self, records: Vec<crate::StoreRecord>) -> Self {
        self.pending = records;
        self
    }

    pub fn stage(&mut self, records: &mut Vec<crate::StoreRecord>) {
        self.stage_unless(records, &|_| false);
    }

    pub fn stage_unless(
        &mut self,
        records: &mut Vec<crate::StoreRecord>,
        known: &dyn Fn(&str) -> bool,
    ) {
        records.extend(self.pending.drain(..).filter(|record| !known(&record.key)));
        let Some(content) = self.content.take() else {
            return;
        };
        let layout = self.layout.take();
        if known(&self.key) {
            return;
        }
        let layout = match layout {
            Some(layout) => layout,
            None if content.len() >= INLINE_BYTES && self.key.starts_with(KEY_PREFIX) => {
                Arc::new(Layout::of(&content))
            }
            None => {
                records.push(crate::StoreRecord {
                    key: self.key.to_string(),
                    value: format!("={content}"),
                });
                return;
            }
        };
        let mut offset = 0;
        for (index, &end) in layout.ends.iter().enumerate() {
            let key = chunk_key(layout.hash(index));
            if !known(&key) {
                records.push(crate::StoreRecord {
                    key,
                    value: content[offset..end].to_owned(),
                });
            }
            offset = end;
        }
        for (hash, text) in &layout.nodes {
            let key = chunk_key(hash);
            if !known(&key) {
                records.push(crate::StoreRecord {
                    key,
                    value: text.clone(),
                });
            }
        }
        records.push(crate::StoreRecord {
            key: self.key.to_string(),
            value: layout.root.clone(),
        });
    }

    pub async fn load(&self, store: &mut dyn crate::StateStore, state_id: &str) -> Result<Self> {
        if self.content.is_some() {
            return Ok(self.clone());
        }
        let record = store
            .read_record(state_id, &self.key)
            .await?
            .ok_or_else(|| Error::InvalidState(format!("missing payload record {}", self.key)))?;
        self.load_record(store, state_id, record).await
    }

    pub async fn read(&self, reader: &mut dyn crate::StateReader, state_id: &str) -> Result<Self> {
        if self.content.is_some() {
            return Ok(self.clone());
        }
        let record = reader
            .read_records(state_id, std::slice::from_ref(&self.key.to_string()))
            .await?
            .pop()
            .flatten()
            .ok_or_else(|| Error::InvalidState(format!("missing payload record {}", self.key)))?;
        self.load_record(reader, state_id, record).await
    }

    pub async fn load_many(
        values: &[Self],
        store: &mut dyn crate::StateStore,
        state_id: &str,
    ) -> Result<Vec<Self>> {
        if values.is_empty() {
            return Ok(Vec::new());
        }
        let keys: Vec<_> = values.iter().map(|value| value.key.to_string()).collect();
        let records = store.read_records(state_id, &keys).await?;
        if records.len() != values.len() {
            return Err(Error::InvalidState("record batch length mismatch".into()));
        }
        let mut result = Vec::with_capacity(values.len());
        for (value, record) in values.iter().zip(records) {
            let record = record.ok_or_else(|| {
                Error::InvalidState(format!("missing payload record {}", value.key))
            })?;
            result.push(value.load_record(store, state_id, record).await?);
        }
        Ok(result)
    }

    async fn load_record<S: RecordSource + ?Sized>(
        &self,
        store: &mut S,
        state_id: &str,
        record: String,
    ) -> Result<Self> {
        let assembled;
        let mut verified = false;
        let content = if let Some(content) = record.strip_prefix('=') {
            content
        } else if record.starts_with(MANIFEST_PREFIX) || record.starts_with(TREE_PREFIX) {
            if record_key(&record) != self.key.as_ref() {
                return Err(Error::InvalidState(format!(
                    "payload manifest checksum mismatch {}",
                    self.key
                )));
            }
            let hashes = self.leaf_hashes(store, state_id, record.clone()).await?;
            assembled = self.assemble(store, state_id, &hashes, true).await?;
            verified = true;
            &assembled
        } else if let Some(hashes) = record.strip_prefix(SHA256_MANIFEST_PREFIX) {
            assembled = self.assemble(store, state_id, hashes, false).await?;
            &assembled
        } else {
            let count: usize = record
                .strip_prefix('+')
                .ok_or_else(|| Error::InvalidState("invalid payload record".into()))?
                .parse()
                .map_err(|_| Error::InvalidState("invalid payload record count".into()))?;
            let mut content = String::new();
            for index in 0..count {
                let chunk = store
                    .records(state_id, &[format!("{}/{index}", self.key)])
                    .await?
                    .pop()
                    .flatten()
                    .ok_or_else(|| {
                        Error::InvalidState(format!("missing payload chunk {}/{index}", self.key))
                    })?;
                content.push_str(&chunk);
            }
            assembled = content;
            &assembled
        };
        if !verified && !key_matches(&self.key, content) {
            return Err(Error::InvalidState(format!(
                "payload record checksum mismatch {}",
                self.key
            )));
        }
        Ok(Self {
            key: self.key.clone(),
            content: Some(content.into()),
            layout: None,
            pending: Vec::new(),
        })
    }

    async fn leaf_hashes<S: RecordSource + ?Sized>(
        &self,
        store: &mut S,
        state_id: &str,
        root: String,
    ) -> Result<String> {
        let mut level = vec![root];
        loop {
            let leaves = level.iter().all(|text| text.starts_with(MANIFEST_PREFIX));
            if !leaves && !level.iter().all(|text| text.starts_with(TREE_PREFIX)) {
                return Err(Error::InvalidState(format!(
                    "payload manifest tree mixes levels {}",
                    self.key
                )));
            }
            let hashes: String = level.iter().map(|text| &text[1..]).collect();
            if hashes.is_empty()
                || hashes.len() % HASH_HEX != 0
                || !hashes.bytes().all(|byte| byte.is_ascii_hexdigit())
            {
                return Err(Error::InvalidState("invalid payload chunk manifest".into()));
            }
            if leaves {
                return Ok(hashes);
            }
            let keys: Vec<String> = hashes
                .as_bytes()
                .chunks(HASH_HEX)
                .map(|hash| chunk_key(std::str::from_utf8(hash).unwrap_or_default()))
                .collect();
            let children = store.records(state_id, &keys).await?;
            if children.len() != keys.len() {
                return Err(Error::InvalidState("record batch length mismatch".into()));
            }
            level = Vec::with_capacity(keys.len());
            for (index, (key, child)) in keys.iter().zip(children).enumerate() {
                let child = child.ok_or_else(|| {
                    Error::InvalidState(format!("missing payload manifest {key} of {}", self.key))
                })?;
                if content_hash(&child) != hashes[index * HASH_HEX..(index + 1) * HASH_HEX] {
                    return Err(Error::InvalidState(format!(
                        "payload manifest checksum mismatch {key} of {}",
                        self.key
                    )));
                }
                level.push(child);
            }
        }
    }

    async fn assemble<S: RecordSource + ?Sized>(
        &self,
        store: &mut S,
        state_id: &str,
        hashes: &str,
        verify: bool,
    ) -> Result<String> {
        if hashes.is_empty()
            || hashes.len() % 64 != 0
            || !hashes.bytes().all(|byte| byte.is_ascii_hexdigit())
        {
            return Err(Error::InvalidState("invalid payload chunk manifest".into()));
        }
        let keys: Vec<String> = (0..hashes.len() / 64)
            .map(|index| chunk_key(&hashes[index * 64..(index + 1) * 64]))
            .collect();
        let chunks = store.records(state_id, &keys).await?;
        if chunks.len() != keys.len() {
            return Err(Error::InvalidState("record batch length mismatch".into()));
        }
        let mut content = String::new();
        for (index, (key, chunk)) in keys.iter().zip(chunks).enumerate() {
            let chunk = chunk.ok_or_else(|| {
                Error::InvalidState(format!("missing payload chunk {key} of {}", self.key))
            })?;
            if verify && content_hash(&chunk) != hashes[index * 64..(index + 1) * 64] {
                return Err(Error::InvalidState(format!(
                    "payload chunk checksum mismatch {key} of {}",
                    self.key
                )));
            }
            content.push_str(&chunk);
        }
        Ok(content)
    }
}

trait RecordSource {
    fn records<'a>(
        &'a mut self,
        state_id: &'a str,
        keys: &'a [String],
    ) -> crate::StoreFuture<'a, std::result::Result<Vec<Option<String>>, crate::StoreError>>;
}

impl RecordSource for dyn crate::StateStore + '_ {
    fn records<'a>(
        &'a mut self,
        state_id: &'a str,
        keys: &'a [String],
    ) -> crate::StoreFuture<'a, std::result::Result<Vec<Option<String>>, crate::StoreError>> {
        self.read_records(state_id, keys)
    }
}

impl RecordSource for dyn crate::StateReader + '_ {
    fn records<'a>(
        &'a mut self,
        state_id: &'a str,
        keys: &'a [String],
    ) -> crate::StoreFuture<'a, std::result::Result<Vec<Option<String>>, crate::StoreError>> {
        self.read_records(state_id, keys)
    }
}

fn content_hash(value: &str) -> String {
    blake3::hash(value.as_bytes()).to_hex().to_string()
}

fn record_key(value: &str) -> String {
    format!("{KEY_PREFIX}{}", content_hash(value))
}

fn key_matches(key: &str, content: &str) -> bool {
    match key.strip_prefix(KEY_PREFIX) {
        Some(hash) => content_hash(content) == hash,
        None => sha256_key(content) == key,
    }
}

fn sha256_key(value: &str) -> String {
    use sha2::{Digest, Sha256};
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = String::with_capacity(64);
    for byte in Sha256::digest(value.as_bytes()) {
        encoded.push(HEX[(byte >> 4) as usize] as char);
        encoded.push(HEX[(byte & 15) as usize] as char);
    }
    encoded
}

impl PartialEq for EncodedPayload {
    fn eq(&self, other: &Self) -> bool {
        self.key == other.key
    }
}
impl Eq for EncodedPayload {}
