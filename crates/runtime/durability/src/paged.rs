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

const PAGE_ITEMS: usize = 64;

#[derive(Clone, Default, Deserialize, Serialize)]
pub struct Sequence {
    tail: Option<EncodedPayload>,
}

#[derive(Deserialize, Serialize)]
struct Page {
    previous: Option<EncodedPayload>,
    items: Vec<EncodedPayload>,
}

pub struct Prepared {
    pub payload: EncodedPayload,
    pub keys: HashSet<String>,
}

#[derive(Clone, Default)]
pub struct SequenceCache {
    items: Vec<EncodedPayload>,
    pages: Vec<EncodedPayload>,
    durable_items: usize,
    durable_pages: usize,
}

impl SequenceCache {
    pub fn committed(&mut self) {
        self.durable_items = self.items.len();
        self.durable_pages = self.pages.len();
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
        let mut previous = None;
        for chunk in items.chunks(PAGE_ITEMS) {
            let mut values = Vec::with_capacity(chunk.len());
            for item in chunk {
                values.push(self.record(item)?);
            }
            previous = Some(self.record(&Page {
                previous,
                items: values,
            })?);
        }
        Ok(Sequence { tail: previous })
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
        let pages = (reused / PAGE_ITEMS)
            .min(cache.durable_pages)
            .min(cache.pages.len());
        cache.items.truncate(reused);
        cache.pages.truncate(pages);
        cache.durable_items = cache.durable_items.min(reused);
        cache.durable_pages = cache.durable_pages.min(pages);
        for item in &items[reused..] {
            let reference = self.record(item)?;
            cache.items.push(reference);
        }
        for chunk in cache.items[pages * PAGE_ITEMS..].chunks(PAGE_ITEMS) {
            let page = self.record(&Page {
                previous: cache.pages.last().cloned(),
                items: chunk.to_vec(),
            })?;
            cache.pages.push(page);
        }
        Ok(Sequence {
            tail: cache.pages.last().cloned(),
        })
    }

    pub fn finish<T: Serialize>(self, value: &T) -> Result<Prepared> {
        Ok(Prepared {
            payload: EncodedPayload::encode(value)?.with_records(self.records),
            keys: self.keys,
        })
    }
}

pub async fn load_sequence<T: DeserializeOwned>(
    reader: PayloadReader<'_>,
    sequence: Sequence,
    keys: &mut HashSet<String>,
) -> Result<Vec<T>> {
    let mut pages = Vec::new();
    let mut next = sequence.tail;
    while let Some(reference) = next {
        keys.insert(reference.key().to_owned());
        let page: Page = reader.load_payload(reference).await?.decode()?;
        next = page.previous;
        pages.push(page.items);
    }
    let references: Vec<_> = pages.into_iter().rev().flatten().collect();
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
