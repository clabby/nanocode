use std::sync::Arc;

use serde::ser::SerializeSeq;

use crate::{
    Committed, OwnedCommit, OwnedState, OwnerId, OwnerToken, StateStore, StoreError, StoreFuture,
    StoreRecord, StoredState,
};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SqlStatement<'a> {
    pub sql: &'static str,
    pub params: Vec<&'a str>,
}

impl serde::Serialize for SqlStatement<'_> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut sequence = serializer.serialize_seq(Some(self.params.len() + 1))?;
        sequence.serialize_element(self.sql)?;
        for param in &self.params {
            sequence.serialize_element(param)?;
        }
        sequence.end()
    }
}

pub type SqlRow = Vec<Option<String>>;

#[cfg(not(target_family = "wasm"))]
pub trait Sql: Send + Sync {
    fn transaction<'a>(
        &'a self,
        operation: &'static str,
        statements: &'a [SqlStatement<'a>],
    ) -> StoreFuture<'a, Result<Vec<Vec<SqlRow>>, StoreError>>;
}

#[cfg(target_family = "wasm")]
pub trait Sql {
    fn transaction<'a>(
        &'a self,
        operation: &'static str,
        statements: &'a [SqlStatement<'a>],
    ) -> StoreFuture<'a, Result<Vec<Vec<SqlRow>>, StoreError>>;
}

const SCHEMA: [&str; 3] = [
    "CREATE TABLE IF NOT EXISTS states (id TEXT PRIMARY KEY, revision TEXT NOT NULL, payload TEXT NOT NULL)",
    "CREATE TABLE IF NOT EXISTS owners (id TEXT PRIMARY KEY, owner TEXT NOT NULL, fence TEXT NOT NULL)",
    "CREATE TABLE IF NOT EXISTS records (state_id TEXT NOT NULL, key TEXT NOT NULL, value TEXT NOT NULL, PRIMARY KEY (state_id, key)) WITHOUT ROWID",
];
const PEEK: &str = "SELECT revision, payload FROM states WHERE id = ?1";
const OWNER: &str = "SELECT owner, fence FROM owners WHERE id = ?1";
const HEAD: &str = "SELECT owners.owner, owners.fence, COALESCE(states.revision, '0') FROM owners LEFT JOIN states ON states.id = owners.id WHERE owners.id = ?1";
const RECORD: &str = "SELECT value FROM records WHERE state_id = ?1 AND key = ?2";
const RECORDS: &str = "SELECT records.value FROM json_each(?2) AS keys LEFT JOIN records ON records.state_id = ?1 AND records.key = keys.value ORDER BY keys.key";
const STATES: &str = "SELECT id FROM states ORDER BY rowid DESC LIMIT CAST(?1 AS INTEGER)";
const ACQUIRE: &str = "INSERT INTO owners VALUES (?1, ?2, '1') ON CONFLICT (id) DO UPDATE SET owner = excluded.owner, fence = CAST(CAST(fence AS INTEGER) + 1 AS TEXT) WHERE CAST(fence AS INTEGER) < 9223372036854775807 RETURNING fence";
const INSERT_RECORD: &str = "INSERT INTO records SELECT ?1, ?2, ?3 WHERE EXISTS (SELECT 1 FROM owners WHERE id = ?1 AND owner = ?4 AND fence = ?5) AND COALESCE((SELECT revision FROM states WHERE id = ?1), '0') = ?6 ON CONFLICT DO NOTHING";
const REPLACE: &str = "INSERT INTO states SELECT ?1, ?2, ?3 WHERE EXISTS (SELECT 1 FROM owners WHERE id = ?1 AND owner = ?4 AND fence = ?5) AND COALESCE((SELECT revision FROM states WHERE id = ?1), '0') = ?6 ON CONFLICT (id) DO UPDATE SET revision = excluded.revision, payload = excluded.payload RETURNING revision";

const fn statement<'a>(sql: &'static str, params: Vec<&'a str>) -> SqlStatement<'a> {
    SqlStatement { sql, params }
}

fn invalid(detail: impl std::fmt::Display) -> StoreError {
    StoreError::Backend(format!("SQL host answered an invalid row: {detail}"))
}

fn text(row: &SqlRow, column: usize) -> Result<&str, StoreError> {
    row.get(column)
        .and_then(Option::as_deref)
        .ok_or_else(|| invalid(format!("column {column} is missing")))
}

fn counter(row: &SqlRow, column: usize) -> Result<u64, StoreError> {
    let value = text(row, column)?;
    value
        .parse()
        .map_err(|error| invalid(format!("counter `{value}`: {error}")))
}

fn single(rows: &[SqlRow]) -> Result<Option<&SqlRow>, StoreError> {
    match rows {
        [] => Ok(None),
        [row] => Ok(Some(row)),
        _ => Err(invalid(format!(
            "{} rows for a single-row query",
            rows.len()
        ))),
    }
}

fn stored(rows: &[SqlRow]) -> Result<StoredState, StoreError> {
    match single(rows)? {
        None => Ok(StoredState::default()),
        Some(row) => Ok(StoredState {
            revision: counter(row, 0)?,
            payload: Some(text(row, 1)?.to_owned()),
        }),
    }
}

struct Replacement<'a> {
    state_id: &'a str,
    owner: &'a OwnerToken,
    expected_revision: u64,
    payload: &'a str,
    records: &'a [StoreRecord],
}

struct Counters {
    fence: String,
    expected: String,
    revision: String,
}

impl Replacement<'_> {
    fn counters(&self) -> Result<Counters, StoreError> {
        let revision = self.expected_revision.checked_add(1).ok_or_else(|| {
            StoreError::NotCommitted("SQL durability revision overflow".to_owned())
        })?;
        Ok(Counters {
            fence: self.owner.fence().to_string(),
            expected: self.expected_revision.to_string(),
            revision: revision.to_string(),
        })
    }

    fn statements<'a>(&'a self, counters: &'a Counters, statements: &mut Vec<SqlStatement<'a>>) {
        let owner = self.owner.owner_id().as_str();
        statements.extend(self.records.iter().map(|record| {
            statement(
                INSERT_RECORD,
                vec![
                    self.state_id,
                    &record.key,
                    &record.value,
                    owner,
                    &counters.fence,
                    &counters.expected,
                ],
            )
        }));
        statements.push(statement(
            REPLACE,
            vec![
                self.state_id,
                &counters.revision,
                self.payload,
                owner,
                &counters.fence,
                &counters.expected,
            ],
        ));
        statements.push(statement(HEAD, vec![self.state_id]));
    }

    fn outcome(&self, results: &[Vec<SqlRow>]) -> Result<u64, StoreError> {
        let [.., replaced, head] = results else {
            return Err(StoreError::Backend(
                "SQL host answered too few statements for a replacement".to_owned(),
            ));
        };
        if let Some(row) = single(replaced)? {
            return counter(row, 0);
        }
        let Some(head) = single(head)? else {
            return Err(StoreError::Fenced);
        };
        if text(head, 0)? != self.owner.owner_id().as_str()
            || counter(head, 1)? != self.owner.fence()
        {
            return Err(StoreError::Fenced);
        }
        let actual = counter(head, 2)?;
        if actual != self.expected_revision {
            return Err(StoreError::Conflict {
                expected: self.expected_revision,
                actual,
            });
        }
        Err(StoreError::Backend(format!(
            "SQL host refused to replace revision {actual} of state {} while reporting it as current",
            self.state_id
        )))
    }
}

pub struct SqlStore<H: ?Sized> {
    host: Arc<H>,
    ready: bool,
}

impl<H: Sql + ?Sized> SqlStore<H> {
    pub fn new(host: H) -> Self
    where
        H: Sized,
    {
        Self::shared(Arc::new(host))
    }

    pub const fn shared(host: Arc<H>) -> Self {
        Self { host, ready: false }
    }

    pub fn host(&self) -> &H {
        &self.host
    }

    async fn transaction(
        &mut self,
        operation: &'static str,
        statements: Vec<SqlStatement<'_>>,
    ) -> Result<Vec<Vec<SqlRow>>, StoreError> {
        let schema = if self.ready { 0 } else { SCHEMA.len() };
        let statements: Vec<_> = SCHEMA[..schema]
            .iter()
            .map(|sql| statement(sql, Vec::new()))
            .chain(statements)
            .collect();
        let mut results = self.host.transaction(operation, &statements).await?;
        if results.len() != statements.len() {
            return Err(StoreError::Backend(format!(
                "SQL host answered {} of {} statements",
                results.len(),
                statements.len()
            )));
        }
        results.drain(..schema);
        self.ready = true;
        Ok(results)
    }

    async fn query(
        &mut self,
        operation: &'static str,
        sql: &'static str,
        params: Vec<&str>,
    ) -> Result<Vec<SqlRow>, StoreError> {
        let mut results = self
            .transaction(operation, vec![statement(sql, params)])
            .await?;
        Ok(results.pop().unwrap_or_default())
    }

    async fn record(&mut self, state_id: &str, key: &str) -> Result<Option<String>, StoreError> {
        let rows = self
            .query("read_record", RECORD, vec![state_id, key])
            .await?;
        single(&rows)?
            .map(|row| text(row, 0).map(str::to_owned))
            .transpose()
    }

    async fn records(
        &mut self,
        state_id: &str,
        keys: &[String],
    ) -> Result<Vec<Option<String>>, StoreError> {
        if keys.is_empty() {
            return Ok(Vec::new());
        }
        let encoded = serde_json::to_string(keys)
            .map_err(|error| StoreError::Backend(format!("record keys: {error}")))?;
        let rows = self
            .query("read_records", RECORDS, vec![state_id, &encoded])
            .await?;
        if rows.len() != keys.len() {
            return Err(StoreError::Backend(format!(
                "SQL host answered {} of {} record reads",
                rows.len(),
                keys.len()
            )));
        }
        rows.into_iter()
            .map(|row| match <[Option<String>; 1]>::try_from(row) {
                Ok([value]) => Ok(value),
                Err(row) => Err(invalid(format!("{} columns for a record read", row.len()))),
            })
            .collect()
    }

    async fn state(&mut self, state_id: &str) -> Result<StoredState, StoreError> {
        stored(&self.query("peek", PEEK, vec![state_id]).await?)
    }

    async fn state_ids(&mut self, limit: usize) -> Result<Vec<String>, StoreError> {
        let limit = limit.to_string();
        let rows = self.query("list_states", STATES, vec![&limit]).await?;
        rows.iter()
            .map(|row| text(row, 0).map(str::to_owned))
            .collect()
    }

    async fn current_owner(&mut self, state_id: &str) -> Result<Option<OwnerToken>, StoreError> {
        let rows = self.query("owner", OWNER, vec![state_id]).await?;
        single(&rows)?
            .map(|row| {
                Ok(OwnerToken::new(
                    OwnerId::from(text(row, 0)?.to_owned()),
                    counter(row, 1)?,
                ))
            })
            .transpose()
    }

    async fn acquire_owner(
        &mut self,
        state_id: &str,
        owner_id: OwnerId,
    ) -> Result<OwnedState, StoreError> {
        let results = self
            .transaction(
                "acquire",
                vec![
                    statement(ACQUIRE, vec![state_id, owner_id.as_str()]),
                    statement(PEEK, vec![state_id]),
                ],
            )
            .await?;
        let Some(acquired) = single(&results[0])? else {
            return Err(StoreError::NotCommitted(
                "SQL durability owner fence overflow".to_owned(),
            ));
        };
        let fence = counter(acquired, 0)?;
        Ok(OwnedState {
            owner: OwnerToken::new(owner_id, fence),
            state: stored(&results[1])?,
        })
    }

    async fn replace_state(&mut self, replacement: Replacement<'_>) -> Result<u64, StoreError> {
        let counters = replacement.counters()?;
        let mut statements = Vec::with_capacity(replacement.records.len() + 2);
        replacement.statements(&counters, &mut statements);
        let results = self.transaction("replace", statements).await?;
        replacement.outcome(&results)
    }

    async fn commit_all(
        &mut self,
        commits: Vec<OwnedCommit>,
    ) -> Vec<Result<Committed, StoreError>> {
        let prepared: Vec<_> = commits
            .iter()
            .map(|commit| {
                if !commit.events.is_empty() {
                    return Err(StoreError::NotCommitted(
                        "this store does not record state events".to_owned(),
                    ));
                }
                let replacement = Replacement {
                    state_id: &commit.state_id,
                    owner: &commit.owner,
                    expected_revision: commit.expected_revision,
                    payload: &commit.payload,
                    records: &commit.records,
                };
                let counters = replacement.counters()?;
                Ok((replacement, counters))
            })
            .collect();
        let mut statements = Vec::new();
        let mut ranges = Vec::with_capacity(prepared.len());
        for entry in &prepared {
            let start = statements.len();
            if let Ok((replacement, counters)) = entry {
                replacement.statements(counters, &mut statements);
            }
            ranges.push(start..statements.len());
        }
        let results = if statements.is_empty() {
            Ok(Vec::new())
        } else {
            self.transaction("commit_many", statements).await
        };
        prepared
            .iter()
            .zip(ranges)
            .map(|(entry, range)| {
                let (replacement, _) = entry.as_ref().map_err(Clone::clone)?;
                let results = results.as_ref().map_err(Clone::clone)?;
                replacement
                    .outcome(&results[range])
                    .map(|revision| Committed {
                        revision,
                        first_event: None,
                    })
            })
            .collect()
    }
}

impl<H: Sql + ?Sized + 'static> StateStore for SqlStore<H> {
    fn read_record<'a>(
        &'a mut self,
        state_id: &'a str,
        key: &'a str,
    ) -> StoreFuture<'a, Result<Option<String>, StoreError>> {
        Box::pin(self.record(state_id, key))
    }

    fn read_records<'a>(
        &'a mut self,
        state_id: &'a str,
        keys: &'a [String],
    ) -> StoreFuture<'a, Result<Vec<Option<String>>, StoreError>> {
        Box::pin(self.records(state_id, keys))
    }

    fn peek<'a>(
        &'a mut self,
        state_id: &'a str,
    ) -> StoreFuture<'a, Result<StoredState, StoreError>> {
        Box::pin(self.state(state_id))
    }

    fn list_states<'a>(
        &'a mut self,
        limit: usize,
    ) -> StoreFuture<'a, Result<Vec<String>, StoreError>> {
        Box::pin(self.state_ids(limit))
    }

    fn owner<'a>(
        &'a mut self,
        state_id: &'a str,
    ) -> StoreFuture<'a, Result<Option<OwnerToken>, StoreError>> {
        Box::pin(self.current_owner(state_id))
    }

    fn acquire<'a>(
        &'a mut self,
        state_id: &'a str,
        owner_id: OwnerId,
    ) -> StoreFuture<'a, Result<OwnedState, StoreError>> {
        Box::pin(self.acquire_owner(state_id, owner_id))
    }

    fn replace<'a>(
        &'a mut self,
        state_id: &'a str,
        owner: &'a OwnerToken,
        expected_revision: u64,
        payload: &'a str,
        records: &'a [StoreRecord],
    ) -> StoreFuture<'a, Result<u64, StoreError>> {
        Box::pin(self.replace_state(Replacement {
            state_id,
            owner,
            expected_revision,
            payload,
            records,
        }))
    }

    fn commit_many(
        &mut self,
        commits: Vec<OwnedCommit>,
    ) -> StoreFuture<'_, Vec<Result<Committed, StoreError>>> {
        Box::pin(self.commit_all(commits))
    }
}
