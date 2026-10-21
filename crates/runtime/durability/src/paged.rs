use std::collections::HashSet;

use serde::{Deserialize, Serialize, de::DeserializeOwned};

use crate::{
    DurableSession, EncodedPayload, Result, StoreRecord, session::DurableOwner,
    shared_store::SharedStore,
};

#[derive(Clone, Copy)]
enum Source<'a> {
    Owner(&'a DurableOwner),
    Session(&'a DurableSession),
    Store(&'a SharedStore, &'a str),
}

#[derive(Clone, Copy)]
pub struct PayloadReader<'a>(Source<'a>);

impl<'a> From<&'a DurableOwner> for PayloadReader<'a> {
    fn from(value: &'a DurableOwner) -> Self {
        Self(Source::Owner(value))
    }
}

impl<'a> From<&'a DurableSession> for PayloadReader<'a> {
    fn from(value: &'a DurableSession) -> Self {
        Self(Source::Session(value))
    }
}

impl<'a> PayloadReader<'a> {
    pub(crate) const fn store(store: &'a SharedStore, state_id: &'a str) -> Self {
        Self(Source::Store(store, state_id))
    }

    pub async fn load_payloads(self, payloads: Vec<EncodedPayload>) -> Result<Vec<EncodedPayload>> {
        match self.0 {
            Source::Owner(owner) => owner.load_payloads(payloads).await,
            Source::Session(session) => session.resolve_many(payloads).await,
            Source::Store(store, state_id) => {
                EncodedPayload::load_many(&payloads, &mut store.clone(), state_id).await
            }
        }
    }

    pub async fn load_payload(self, payload: EncodedPayload) -> Result<EncodedPayload> {
        match self.0 {
            Source::Owner(owner) => owner.load_payload(payload).await,
            Source::Session(session) => session.resolve(&payload).await,
            Source::Store(store, state_id) => payload.load(&mut store.clone(), state_id).await,
        }
    }
}

const GROUP: usize = 16;

#[derive(Clone, Default, Deserialize, Serialize)]
pub struct Sequence {
    tail: Option<Tail>,
}

#[derive(Clone, Deserialize, Serialize)]
#[serde(untagged)]
enum Tail {
    Sealed(Sealed),
    Chained(EncodedPayload),
}

#[derive(Clone, Default, Deserialize, Serialize)]
struct Sealed {
    levels: Vec<Vec<EncodedPayload>>,
    items: Vec<EncodedPayload>,
}

#[derive(Deserialize)]
struct Page {
    previous: Option<EncodedPayload>,
    items: Vec<EncodedPayload>,
}

#[derive(Deserialize, Serialize)]
struct Group {
    entries: Vec<EncodedPayload>,
}

pub struct Prepared {
    pub payload: EncodedPayload,
    pub keys: HashSet<String>,
}

#[derive(Clone, Default)]
pub struct SequenceCache {
    items: Vec<EncodedPayload>,
    pages: Vec<Vec<EncodedPayload>>,
    durable_items: usize,
    durable_pages: Vec<usize>,
}

impl SequenceCache {
    pub fn committed(&mut self) {
        self.durable_items = self.items.len();
        self.durable_pages = self.pages.iter().map(Vec::len).collect();
    }

    pub fn clear(&mut self) {
        *self = Self::default();
    }
}

pub struct PagedWriter<'a> {
    known: &'a HashSet<String>,
    keys: HashSet<String>,
    records: Vec<StoreRecord>,
}

impl<'a> PagedWriter<'a> {
    #[must_use]
    pub fn new(known: &'a HashSet<String>) -> Self {
        Self {
            known,
            keys: HashSet::new(),
            records: Vec::new(),
        }
    }

    fn record<T: Serialize>(&mut self, value: &T) -> Result<EncodedPayload> {
        let mut payload = EncodedPayload::encode(value)?;
        let key = payload.key().to_owned();
        if self.keys.insert(key.clone()) && !self.known.contains(&key) {
            payload.stage(&mut self.records);
        }

        Ok(payload.reference())
    }

    pub fn sequence<T: Serialize>(&mut self, items: &[T]) -> Result<Sequence> {
        self.sequence_cached(items, 0, &mut SequenceCache::default())
    }

    pub fn sequence_cached<T: Serialize>(
        &mut self,
        items: &[T],
        unchanged: usize,
        cache: &mut SequenceCache,
    ) -> Result<Sequence> {
        let reused = unchanged
            .min(items.len())
            .min(cache.durable_items)
            .min(cache.items.len());
        cache.items.truncate(reused);
        cache.durable_items = cache.durable_items.min(reused);
        for item in &items[reused..] {
            let reference = self.record(item)?;
            cache.items.push(reference);
        }
        if cache.items.is_empty() {
            cache.pages.clear();
            cache.durable_pages.clear();
            return Ok(Sequence::default());
        }
        let mut valid = reused;
        let mut level = 0;
        loop {
            let below = if level == 0 {
                &cache.items
            } else {
                &cache.pages[level - 1]
            };
            let full = below.len() / GROUP;
            if full == 0 {
                break;
            }
            if cache.pages.len() == level {
                cache.pages.push(Vec::new());
                cache.durable_pages.push(0);
            }
            let kept = (valid / GROUP)
                .min(full)
                .min(cache.durable_pages[level])
                .min(cache.pages[level].len());
            let fresh = if level == 0 {
                cache.items[kept * GROUP..full * GROUP].to_vec()
            } else {
                cache.pages[level - 1][kept * GROUP..full * GROUP].to_vec()
            };
            cache.pages[level].truncate(kept);
            cache.durable_pages[level] = cache.durable_pages[level].min(kept);
            for entries in fresh.chunks(GROUP) {
                let page = self.record(&Group {
                    entries: entries.to_vec(),
                })?;
                cache.pages[level].push(page);
            }
            valid = kept;
            level += 1;
        }
        cache.pages.truncate(level);
        cache.durable_pages.truncate(level);
        let inline = |entries: &[EncodedPayload]| entries[entries.len() / GROUP * GROUP..].to_vec();
        Ok(Sequence {
            tail: Some(Tail::Sealed(Sealed {
                levels: cache.pages.iter().map(|pages| inline(pages)).collect(),
                items: inline(&cache.items),
            })),
        })
    }

    pub fn finish<T: Serialize>(self, value: &T) -> Result<Prepared> {
        Ok(Prepared {
            payload: EncodedPayload::encode(value)?.with_records(self.records),
            keys: self.keys,
        })
    }
}

pub fn seal_sequence(items: &[EncodedPayload]) -> Result<(Sequence, Vec<EncodedPayload>)> {
    if items.is_empty() {
        return Ok((Sequence::default(), Vec::new()));
    }
    let mut pages = Vec::new();
    let mut inline = Vec::new();
    let mut below: Vec<_> = items.iter().map(EncodedPayload::reference).collect();
    loop {
        let full = below.len() / GROUP * GROUP;
        if full == 0 {
            break;
        }
        let mut sealed = Vec::with_capacity(full / GROUP);
        for entries in below[..full].chunks(GROUP) {
            let page = EncodedPayload::encode(&Group {
                entries: entries.to_vec(),
            })?;
            sealed.push(page.reference());
            pages.push(page);
        }
        inline.push(below.split_off(full));
        below = sealed;
    }
    inline.push(below);
    let items = inline.remove(0);
    Ok((
        Sequence {
            tail: Some(Tail::Sealed(Sealed {
                levels: inline,
                items,
            })),
        },
        pages,
    ))
}

pub async fn load_sequence<T: DeserializeOwned>(
    reader: PayloadReader<'_>,
    sequence: Sequence,
    keys: &mut HashSet<String>,
) -> Result<Vec<T>> {
    let references = match sequence.tail {
        None => return Ok(Vec::new()),
        Some(Tail::Chained(tail)) => chained(reader, tail, keys).await?,
        Some(Tail::Sealed(sealed)) => unsealed(reader, sealed, keys).await?,
    };
    if references.is_empty() {
        return Ok(Vec::new());
    }
    for reference in &references {
        keys.insert(reference.key().to_owned());
    }
    let mut items = Vec::with_capacity(references.len());
    for payload in reader.load_payloads(references).await? {
        items.push(payload.decode()?);
    }
    Ok(items)
}

async fn unsealed(
    reader: PayloadReader<'_>,
    sealed: Sealed,
    keys: &mut HashSet<String>,
) -> Result<Vec<EncodedPayload>> {
    let mut levels = sealed.levels;
    let mut pending = levels.pop().unwrap_or_default();
    loop {
        for reference in &pending {
            keys.insert(reference.key().to_owned());
        }
        let mut entries = Vec::new();
        for page in reader.load_payloads(std::mem::take(&mut pending)).await? {
            entries.extend(page.decode::<Group>()?.entries);
        }
        match levels.pop() {
            Some(inline) => {
                entries.extend(inline);
                pending = entries;
            }
            None => {
                entries.extend(sealed.items);
                return Ok(entries);
            }
        }
    }
}

async fn chained(
    reader: PayloadReader<'_>,
    tail: EncodedPayload,
    keys: &mut HashSet<String>,
) -> Result<Vec<EncodedPayload>> {
    let mut pages = Vec::new();
    let mut next = Some(tail);
    while let Some(reference) = next {
        keys.insert(reference.key().to_owned());
        let page: Page = reader.load_payload(reference).await?.decode()?;
        next = page.previous;
        pages.push(page.items);
    }
    Ok(pages.into_iter().rev().flatten().collect())
}
