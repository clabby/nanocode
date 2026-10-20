use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::UNIX_EPOCH,
};

use serde::{Deserialize, Serialize, Serializer, ser::SerializeMap};
use serde_json::{Map, Value, value::RawValue};

use crate::{
    durability::{EncodedPayload, Sql, SqlRow, SqlStatement, StoreError},
    input::Prompt,
};

const SCHEMA: [&str; 2] = [
    "CREATE TABLE IF NOT EXISTS events (seq INTEGER PRIMARY KEY, scope TEXT NOT NULL, request_id TEXT, body TEXT NOT NULL, created_at INTEGER NOT NULL)",
    "CREATE TABLE IF NOT EXISTS turns (scope TEXT NOT NULL, request_id TEXT NOT NULL, digest TEXT NOT NULL, status TEXT NOT NULL, prompt TEXT, hand TEXT, metadata TEXT NOT NULL, cancel_requested INTEGER NOT NULL DEFAULT 0, note TEXT, first_seq INTEGER NOT NULL, last_seq INTEGER, retry TEXT, PRIMARY KEY (scope, request_id)) WITHOUT ROWID",
];
const RETRY_COLUMN: &str =
    "SELECT CAST(COUNT(*) AS TEXT) FROM pragma_table_info('turns') WHERE name = 'retry'";
const ADD_RETRY_COLUMN: &str = "ALTER TABLE turns ADD COLUMN retry TEXT";
const TURN: &str = "SELECT turns.request_id, turns.status, turns.digest, turns.prompt, turns.hand, turns.metadata, CAST(turns.cancel_requested AS TEXT), turns.note, CAST(turns.first_seq AS TEXT), CAST(turns.last_seq AS TEXT), events.body, turns.retry FROM turns LEFT JOIN events ON events.seq = turns.last_seq WHERE turns.scope = ?1 AND turns.request_id = ?2";
const PENDING: &str = "SELECT request_id, status, digest, prompt, hand, metadata, CAST(cancel_requested AS TEXT), note, CAST(first_seq AS TEXT), NULL, NULL, retry FROM turns WHERE scope = ?1 AND status IN ('queued', 'running') ORDER BY first_seq";
const UNFINISHED_EVENT: &str = "INSERT INTO events (scope, request_id, body, created_at) SELECT ?1, ?2, ?3, CAST(?4 AS INTEGER) WHERE EXISTS (SELECT 1 FROM turns WHERE scope = ?1 AND request_id = ?2 AND status IN ('queued', 'running')) RETURNING CAST(seq AS TEXT), CAST(created_at AS TEXT)";
const QUEUED_EVENT: &str = "INSERT INTO events (scope, request_id, body, created_at) SELECT ?1, ?2, ?3, CAST(?4 AS INTEGER) WHERE EXISTS (SELECT 1 FROM turns WHERE scope = ?1 AND request_id = ?2 AND status = 'queued') RETURNING CAST(seq AS TEXT), CAST(created_at AS TEXT)";
const NEW_EVENT: &str = "INSERT INTO events (scope, request_id, body, created_at) SELECT ?1, ?2, ?3, CAST(?4 AS INTEGER) WHERE NOT EXISTS (SELECT 1 FROM turns WHERE scope = ?1 AND request_id = ?2) RETURNING CAST(seq AS TEXT), CAST(created_at AS TEXT)";
const NEW_TURN: &str = "INSERT INTO turns (scope, request_id, digest, status, prompt, hand, metadata, cancel_requested, first_seq) SELECT ?1, ?2, ?3, 'queued', ?4, NULL, ?5, 0, (SELECT MAX(seq) FROM events) WHERE NOT EXISTS (SELECT 1 FROM turns WHERE scope = ?1 AND request_id = ?2)";
const NEW_PLACED_TURN: &str = "INSERT INTO turns (scope, request_id, digest, status, prompt, hand, metadata, cancel_requested, first_seq) SELECT ?1, ?2, ?3, 'queued', ?4, ?6, ?5, 0, (SELECT MAX(seq) FROM events) WHERE NOT EXISTS (SELECT 1 FROM turns WHERE scope = ?1 AND request_id = ?2)";
const START: &str = "UPDATE turns SET status = 'running', note = NULL, retry = NULL WHERE scope = ?1 AND request_id = ?2 AND status IN ('queued', 'running')";
const FINISH: &str = "UPDATE turns SET status = ?3, prompt = NULL, note = NULL, retry = NULL, last_seq = (SELECT MAX(seq) FROM events) WHERE scope = ?1 AND request_id = ?2 AND status IN ('queued', 'running')";
const REQUEST_CANCEL: &str = "UPDATE turns SET cancel_requested = 1 WHERE scope = ?1 AND request_id = ?2 AND status = 'running' AND cancel_requested = 0";
const CANCEL_QUEUED: &str = "UPDATE turns SET status = 'cancelled', prompt = NULL, note = NULL, retry = NULL, last_seq = (SELECT MAX(seq) FROM events) WHERE scope = ?1 AND request_id = ?2 AND status = 'queued'";
const NOTE: &str =
    "UPDATE turns SET note = ?3 WHERE scope = ?1 AND request_id = ?2 AND status = 'running'";
const RETRY_EVENT: &str = "INSERT INTO events (scope, request_id, body, created_at) SELECT ?1, ?2, json_object('type', 'retrying', 'request_id', ?2, 'error', ?3, 'kind', ?4, 'attempts', COALESCE(json_extract(retry, '$.attempts'), 0) + 1, 'retry_at', CAST(?5 AS INTEGER)), CAST(?6 AS INTEGER) FROM turns WHERE scope = ?1 AND request_id = ?2 AND status IN ('queued', 'running') AND (retry IS NULL OR json_extract(retry, '$.error') IS NOT ?3 OR json_extract(retry, '$.kind') IS NOT ?4) RETURNING CAST(seq AS TEXT), CAST(created_at AS TEXT), body";
const RETRY: &str = "UPDATE turns SET retry = json_object('error', ?3, 'kind', ?4, 'attempts', COALESCE(json_extract(retry, '$.attempts'), 0) + 1, 'retry_at', CAST(?5 AS INTEGER)) WHERE scope = ?1 AND request_id = ?2 AND status IN ('queued', 'running')";
const APPEND: &str = "INSERT INTO events (scope, request_id, body, created_at) VALUES (?1, ?2, ?3, CAST(?4 AS INTEGER)) RETURNING CAST(seq AS TEXT)";
const APPEND_UNATTRIBUTED: &str = "INSERT INTO events (scope, request_id, body, created_at) VALUES (?1, NULL, ?2, CAST(?3 AS INTEGER)) RETURNING CAST(seq AS TEXT)";
const PAGE: &str = "SELECT CAST(seq AS TEXT), request_id, CAST(created_at AS TEXT), body FROM events WHERE scope = ?1 AND seq > CAST(?2 AS INTEGER) ORDER BY seq LIMIT CAST(?3 AS INTEGER)";
const TURN_PAGE: &str = "SELECT CAST(events.seq AS TEXT), events.request_id, CAST(events.created_at AS TEXT), events.body FROM turns JOIN events ON events.seq > MAX(CAST(?3 AS INTEGER), turns.first_seq - 1) AND events.seq <= COALESCE(turns.last_seq, 9223372036854775807) WHERE turns.scope = ?1 AND turns.request_id = ?2 AND events.scope = ?1 AND events.request_id = ?2 ORDER BY events.seq LIMIT CAST(?4 AS INTEGER)";
const TURN_START: &str =
    "SELECT CAST(first_seq AS TEXT) FROM turns WHERE scope = ?1 AND request_id = ?2";
const IMPORT: &str = "INSERT INTO turns (scope, request_id, digest, status, prompt, hand, metadata, cancel_requested, note, first_seq, last_seq) SELECT ?1, json_extract(value, '$.request_id'), json_extract(value, '$.digest'), json_extract(value, '$.status'), json_extract(value, '$.prompt'), json_extract(value, '$.hand'), json_extract(value, '$.metadata'), json_extract(value, '$.cancel_requested'), json_extract(value, '$.note'), json_extract(value, '$.first_seq'), json_extract(value, '$.last_seq') FROM json_each(?2) WHERE true ON CONFLICT DO NOTHING";
const IMPORT_RESULT: &str = "INSERT INTO events (scope, request_id, body, created_at) SELECT ?1, ?2, ?3, CAST(?4 AS INTEGER) WHERE EXISTS (SELECT 1 FROM turns WHERE scope = ?1 AND request_id = ?2 AND status IN ('completed', 'failed', 'cancelled') AND last_seq IS NULL)";
const IMPORT_FINISH: &str = "UPDATE turns SET last_seq = (SELECT MAX(seq) FROM events) WHERE scope = ?1 AND request_id = ?2 AND status IN ('completed', 'failed', 'cancelled') AND last_seq IS NULL";

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum TurnState {
    Queued,
    Running,
    Completed,
    Failed,
    Cancelled,
}

impl TurnState {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Queued => "queued",
            Self::Running => "running",
            Self::Completed => "completed",
            Self::Failed => "failed",
            Self::Cancelled => "cancelled",
        }
    }

    #[must_use]
    pub const fn is_terminal(self) -> bool {
        matches!(self, Self::Completed | Self::Failed | Self::Cancelled)
    }

    pub fn parse(value: &str) -> Result<Self, StoreError> {
        Ok(match value {
            "queued" => Self::Queued,
            "running" => Self::Running,
            "completed" => Self::Completed,
            "failed" => Self::Failed,
            "cancelled" => Self::Cancelled,
            other => return Err(invalid(format!("turn status `{other}`"))),
        })
    }
}

#[derive(Clone, Debug, Deserialize)]
pub struct TurnRetry {
    pub error: String,
    pub kind: String,
    pub attempts: u64,
    pub retry_at: u64,
}

#[derive(Clone, Debug)]
pub struct TurnRecord {
    pub request_id: String,
    pub state: TurnState,
    pub digest: String,
    pub prompt: Option<Prompt>,
    pub hand: Option<String>,
    pub metadata: Box<RawValue>,
    pub cancel_requested: bool,
    pub note: Option<String>,
    pub first_seq: u64,
    pub last_seq: Option<u64>,
    pub result: Option<Box<RawValue>>,
    pub retry: Option<TurnRetry>,
}

impl TurnRecord {
    pub fn report(&self) -> Result<Map<String, Value>, StoreError> {
        let mut report = match &self.result {
            Some(result) => match serde_json::from_str(result.get()) {
                Ok(Value::Object(fields)) => fields,
                _ => return Err(invalid("turn result is not an object")),
            },
            None => Map::new(),
        };
        report.remove("type");
        report.insert("request_id".into(), self.request_id.clone().into());
        report.insert("status".into(), self.state.as_str().into());
        if let Some(note) = &self.note {
            report.insert("error".into(), note.clone().into());
        }
        if let Some(retry) = &self.retry {
            report.insert("error".into(), retry.error.clone().into());
            report.insert("kind".into(), retry.kind.clone().into());
            report.insert("attempts".into(), retry.attempts.into());
            report.insert("retry_at".into(), retry.retry_at.into());
        }
        Ok(report)
    }
}

#[derive(Clone, Debug)]
pub struct Submission {
    pub request_id: String,
    pub prompt: Prompt,
    pub hand: Option<String>,
    pub metadata: Box<RawValue>,
}

#[derive(Clone, Debug)]
pub enum Submitted {
    Accepted {
        record: TurnRecord,
        event: LoggedEvent,
    },
    Existing(TurnRecord),
    Conflict(TurnRecord),
}

#[derive(Clone, Debug)]
pub struct ImportedTurn {
    pub request_id: String,
    pub state: TurnState,
    pub prompt: Prompt,
    pub hand: Option<String>,
    pub metadata: Box<RawValue>,
    pub cancel_requested: bool,
    pub note: Option<String>,
    pub first_seq: Option<u64>,
    pub last_seq: Option<u64>,
    pub result: Option<Map<String, Value>>,
}

#[derive(Clone, Debug)]
pub struct LoggedEvent {
    pub seq: u64,
    pub request_id: Option<String>,
    pub created_at: u64,
    pub event: Box<RawValue>,
}

impl Serialize for LoggedEvent {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut map = serializer.serialize_map(Some(4))?;
        map.serialize_entry("seq", &self.seq)?;
        map.serialize_entry("request_id", &self.request_id)?;
        map.serialize_entry("created_at", &self.created_at)?;
        map.serialize_entry("event", &self.event)?;
        map.end()
    }
}

#[derive(Clone, Debug)]
pub struct Started {
    pub previous: TurnState,
    pub record: TurnRecord,
    pub event: LoggedEvent,
}

#[derive(Clone, Debug, Serialize)]
pub struct LogPage {
    pub events: Vec<LoggedEvent>,
    pub next_cursor: u64,
    pub has_more: bool,
}

#[derive(Clone, Debug)]
pub struct LogEntry {
    pub request_id: Option<String>,
    pub event: Box<RawValue>,
}

#[derive(Serialize)]
struct Lifecycle<'a> {
    #[serde(rename = "type")]
    kind: &'static str,
    request_id: &'a str,
}

#[derive(Serialize)]
struct Outcome<'a> {
    #[serde(rename = "type")]
    kind: &'static str,
    request_id: &'a str,
    status: &'static str,
    #[serde(flatten)]
    fields: &'a Map<String, Value>,
}

#[derive(Serialize)]
struct Imported<'a> {
    request_id: &'a str,
    digest: String,
    status: &'static str,
    prompt: Option<String>,
    hand: Option<&'a str>,
    metadata: &'a str,
    cancel_requested: u8,
    note: Option<&'a str>,
    first_seq: u64,
    last_seq: Option<u64>,
}

fn invalid(detail: impl std::fmt::Display) -> StoreError {
    StoreError::Backend(format!(
        "SQL host answered an invalid session log row: {detail}"
    ))
}

fn encoding(error: serde_json::Error) -> StoreError {
    StoreError::NotCommitted(format!("encode session log entry: {error}"))
}

const fn statement<'a>(sql: &'static str, params: Vec<&'a str>) -> SqlStatement<'a> {
    SqlStatement { sql, params }
}

fn text(row: &SqlRow, column: usize) -> Result<&str, StoreError> {
    row.get(column)
        .and_then(Option::as_deref)
        .ok_or_else(|| invalid(format!("column {column} is missing")))
}

fn optional(row: &SqlRow, column: usize) -> Option<&str> {
    row.get(column).and_then(Option::as_deref)
}

fn counter(value: &str) -> Result<u64, StoreError> {
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

fn present(rows: &[SqlRow]) -> Result<bool, StoreError> {
    Ok(single(rows)?
        .map(|row| text(row, 0))
        .transpose()?
        .is_some_and(|count| count != "0"))
}

fn raw(json: String) -> Result<Box<RawValue>, StoreError> {
    RawValue::from_string(json).map_err(|error| invalid(format!("stored JSON: {error}")))
}

fn now() -> u64 {
    nanocode_platform::system_time()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |elapsed| {
            u64::try_from(elapsed.as_millis()).unwrap_or(u64::MAX)
        })
}

fn record(row: &SqlRow) -> Result<TurnRecord, StoreError> {
    Ok(TurnRecord {
        request_id: text(row, 0)?.to_owned(),
        state: TurnState::parse(text(row, 1)?)?,
        digest: text(row, 2)?.to_owned(),
        prompt: optional(row, 3)
            .map(|prompt| {
                serde_json::from_str(prompt).map_err(|error| invalid(format!("prompt: {error}")))
            })
            .transpose()?,
        hand: optional(row, 4).map(str::to_owned),
        metadata: raw(text(row, 5)?.to_owned())?,
        cancel_requested: text(row, 6)? != "0",
        note: optional(row, 7).map(str::to_owned),
        first_seq: counter(text(row, 8)?)?,
        last_seq: optional(row, 9).map(counter).transpose()?,
        result: optional(row, 10)
            .map(|body| raw(body.to_owned()))
            .transpose()?,
        retry: optional(row, 11)
            .map(|retry| {
                serde_json::from_str(retry).map_err(|error| invalid(format!("retry: {error}")))
            })
            .transpose()?,
    })
}

fn logged(row: &SqlRow, request_id: &str, event: Box<RawValue>) -> Result<LoggedEvent, StoreError> {
    Ok(LoggedEvent {
        seq: counter(text(row, 0)?)?,
        request_id: Some(request_id.to_owned()),
        created_at: counter(text(row, 1)?)?,
        event,
    })
}

fn page(rows: &[SqlRow], after: u64, limit: usize) -> Result<LogPage, StoreError> {
    let events = rows
        .iter()
        .map(|row| {
            Ok(LoggedEvent {
                seq: counter(text(row, 0)?)?,
                request_id: optional(row, 1).map(str::to_owned),
                created_at: counter(text(row, 2)?)?,
                event: raw(text(row, 3)?.to_owned())?,
            })
        })
        .collect::<Result<Vec<_>, StoreError>>()?;
    Ok(LogPage {
        next_cursor: events.last().map_or(after, |event| event.seq),
        has_more: events.len() == limit,
        events,
    })
}

pub fn submission_digest(prompt: &Prompt, hand: Option<&str>) -> Result<String, StoreError> {
    EncodedPayload::encode(&(prompt, hand))
        .map(|payload| payload.key().to_owned())
        .map_err(|error| StoreError::NotCommitted(format!("digest turn submission: {error}")))
}

pub struct SessionLog<H: ?Sized> {
    host: Arc<H>,
    scope: Arc<str>,
    ready: AtomicBool,
}

impl<H: Sql + ?Sized> SessionLog<H> {
    pub fn new(host: Arc<H>, scope: impl Into<Arc<str>>) -> Self {
        Self {
            host,
            scope: scope.into(),
            ready: AtomicBool::new(false),
        }
    }

    pub fn scope(&self) -> &str {
        &self.scope
    }

    pub fn host(&self) -> &H {
        &self.host
    }

    async fn run(
        &self,
        operation: &'static str,
        statements: &[SqlStatement<'_>],
    ) -> Result<Vec<Vec<SqlRow>>, StoreError> {
        let results = self.host.transaction(operation, statements).await?;
        if results.len() != statements.len() {
            return Err(StoreError::Backend(format!(
                "SQL host answered {} of {} session log statements",
                results.len(),
                statements.len()
            )));
        }
        Ok(results)
    }

    async fn migrate(&self) -> Result<(), StoreError> {
        let statements: Vec<_> = SCHEMA
            .iter()
            .chain([&RETRY_COLUMN])
            .map(|sql| statement(sql, Vec::new()))
            .collect();
        let results = self.run("schema", &statements).await?;
        if !present(&results[SCHEMA.len()])?
            && let Err(error) = self
                .run("schema", &[statement(ADD_RETRY_COLUMN, Vec::new())])
                .await
            && !present(
                &self
                    .run("schema", &[statement(RETRY_COLUMN, Vec::new())])
                    .await?[0],
            )?
        {
            return Err(error);
        }
        self.ready.store(true, Ordering::Release);
        Ok(())
    }

    async fn transaction(
        &self,
        operation: &'static str,
        statements: Vec<SqlStatement<'_>>,
    ) -> Result<Vec<Vec<SqlRow>>, StoreError> {
        if !self.ready.load(Ordering::Acquire) {
            self.migrate().await?;
        }
        self.run(operation, &statements).await
    }

    pub async fn submit(&self, submission: &Submission) -> Result<Submitted, StoreError> {
        let id = submission.request_id.as_str();
        let prompt = serde_json::to_string(&submission.prompt).map_err(encoding)?;
        let digest = submission_digest(&submission.prompt, submission.hand.as_deref())?;
        let body = serde_json::to_string(&Lifecycle {
            kind: "queued",
            request_id: id,
        })
        .map_err(encoding)?;
        let created = now().to_string();
        let metadata = submission.metadata.get();
        let turn = match submission.hand.as_deref() {
            Some(hand) => statement(
                NEW_PLACED_TURN,
                vec![&self.scope, id, &digest, &prompt, metadata, hand],
            ),
            None => statement(NEW_TURN, vec![&self.scope, id, &digest, &prompt, metadata]),
        };
        let results = self
            .transaction(
                "submit",
                vec![
                    statement(NEW_EVENT, vec![&self.scope, id, &body, &created]),
                    turn,
                    statement(TURN, vec![&self.scope, id]),
                ],
            )
            .await?;
        let record = single(&results[2])?
            .map(record)
            .transpose()?
            .ok_or_else(|| invalid("the submitted turn is missing"))?;
        Ok(match single(&results[0])? {
            Some(row) => Submitted::Accepted {
                event: logged(row, id, raw(body)?)?,
                record,
            },
            None if record.digest == digest => Submitted::Existing(record),
            None => Submitted::Conflict(record),
        })
    }

    pub async fn turn(&self, request_id: &str) -> Result<Option<TurnRecord>, StoreError> {
        let results = self
            .transaction("turn", vec![statement(TURN, vec![&self.scope, request_id])])
            .await?;
        single(&results[0])?.map(record).transpose()
    }

    pub async fn pending(&self) -> Result<Vec<TurnRecord>, StoreError> {
        let results = self
            .transaction("pending", vec![statement(PENDING, vec![&self.scope])])
            .await?;
        results[0].iter().map(record).collect()
    }

    pub async fn start(&self, request_id: &str) -> Result<Option<Started>, StoreError> {
        let body = serde_json::to_string(&Lifecycle {
            kind: "running",
            request_id,
        })
        .map_err(encoding)?;
        let created = now().to_string();
        let results = self
            .transaction(
                "start",
                vec![
                    statement(TURN, vec![&self.scope, request_id]),
                    statement(
                        UNFINISHED_EVENT,
                        vec![&self.scope, request_id, &body, &created],
                    ),
                    statement(START, vec![&self.scope, request_id]),
                    statement(TURN, vec![&self.scope, request_id]),
                ],
            )
            .await?;
        let (Some(before), Some(row), Some(current)) = (
            single(&results[0])?,
            single(&results[1])?,
            single(&results[3])?,
        ) else {
            return Ok(None);
        };
        Ok(Some(Started {
            previous: record(before)?.state,
            record: record(current)?,
            event: logged(row, request_id, raw(body)?)?,
        }))
    }

    pub async fn finish(
        &self,
        request_id: &str,
        state: TurnState,
        fields: &Map<String, Value>,
    ) -> Result<Option<(TurnRecord, LoggedEvent)>, StoreError> {
        if !state.is_terminal() {
            return Err(StoreError::NotCommitted(format!(
                "a turn cannot finish as {}",
                state.as_str()
            )));
        }
        let body = serde_json::to_string(&Outcome {
            kind: "result",
            request_id,
            status: state.as_str(),
            fields,
        })
        .map_err(encoding)?;
        let created = now().to_string();
        let results = self
            .transaction(
                "finish",
                vec![
                    statement(
                        UNFINISHED_EVENT,
                        vec![&self.scope, request_id, &body, &created],
                    ),
                    statement(FINISH, vec![&self.scope, request_id, state.as_str()]),
                    statement(TURN, vec![&self.scope, request_id]),
                ],
            )
            .await?;
        let (Some(row), Some(current)) = (single(&results[0])?, single(&results[2])?) else {
            return Ok(None);
        };
        Ok(Some((
            record(current)?,
            logged(row, request_id, raw(body)?)?,
        )))
    }

    pub async fn cancel(
        &self,
        request_id: &str,
        fields: &Map<String, Value>,
    ) -> Result<Option<(TurnRecord, Option<LoggedEvent>)>, StoreError> {
        let body = serde_json::to_string(&Outcome {
            kind: "result",
            request_id,
            status: TurnState::Cancelled.as_str(),
            fields,
        })
        .map_err(encoding)?;
        let created = now().to_string();
        let results = self
            .transaction(
                "cancel",
                vec![
                    statement(REQUEST_CANCEL, vec![&self.scope, request_id]),
                    statement(QUEUED_EVENT, vec![&self.scope, request_id, &body, &created]),
                    statement(CANCEL_QUEUED, vec![&self.scope, request_id]),
                    statement(TURN, vec![&self.scope, request_id]),
                ],
            )
            .await?;
        let Some(current) = single(&results[3])? else {
            return Ok(None);
        };
        let event = single(&results[1])?
            .map(|row| logged(row, request_id, raw(body)?))
            .transpose()?;
        Ok(Some((record(current)?, event)))
    }

    pub async fn retry(
        &self,
        request_id: &str,
        error: &str,
        kind: &str,
        retry_at: u64,
    ) -> Result<Option<(TurnRecord, Option<LoggedEvent>)>, StoreError> {
        let at = retry_at.to_string();
        let created = now().to_string();
        let results = self
            .transaction(
                "retry",
                vec![
                    statement(
                        RETRY_EVENT,
                        vec![&self.scope, request_id, error, kind, &at, &created],
                    ),
                    statement(RETRY, vec![&self.scope, request_id, error, kind, &at]),
                    statement(TURN, vec![&self.scope, request_id]),
                ],
            )
            .await?;
        let Some(current) = single(&results[2])? else {
            return Ok(None);
        };
        let event = single(&results[0])?
            .map(|row| {
                Ok::<_, StoreError>(LoggedEvent {
                    seq: counter(text(row, 0)?)?,
                    request_id: Some(request_id.to_owned()),
                    created_at: counter(text(row, 1)?)?,
                    event: raw(text(row, 2)?.to_owned())?,
                })
            })
            .transpose()?;
        Ok(Some((record(current)?, event)))
    }

    pub async fn interrupted(&self, request_id: &str, note: &str) -> Result<(), StoreError> {
        self.transaction(
            "interrupted",
            vec![statement(NOTE, vec![&self.scope, request_id, note])],
        )
        .await
        .map(drop)
    }

    pub async fn append(&self, entries: &[LogEntry]) -> Result<Vec<LoggedEvent>, StoreError> {
        if entries.is_empty() {
            return Ok(Vec::new());
        }
        let created = now();
        let at = created.to_string();
        let statements = entries
            .iter()
            .map(|entry| match entry.request_id.as_deref() {
                Some(request_id) => statement(
                    APPEND,
                    vec![&self.scope, request_id, entry.event.get(), &at],
                ),
                None => statement(
                    APPEND_UNATTRIBUTED,
                    vec![&self.scope, entry.event.get(), &at],
                ),
            })
            .collect();
        let results = self.transaction("append", statements).await?;
        entries
            .iter()
            .zip(&results)
            .map(|(entry, rows)| {
                let row =
                    single(rows)?.ok_or_else(|| invalid("an appended event has no sequence"))?;
                Ok(LoggedEvent {
                    seq: counter(text(row, 0)?)?,
                    request_id: entry.request_id.clone(),
                    created_at: created,
                    event: entry.event.clone(),
                })
            })
            .collect()
    }

    pub async fn page(&self, after: u64, limit: usize) -> Result<LogPage, StoreError> {
        let cursor = after.to_string();
        let count = limit.to_string();
        let results = self
            .transaction(
                "page",
                vec![statement(PAGE, vec![&self.scope, &cursor, &count])],
            )
            .await?;
        page(&results[0], after, limit)
    }

    pub async fn turn_page(
        &self,
        request_id: &str,
        after: u64,
        limit: usize,
    ) -> Result<LogPage, StoreError> {
        let cursor = after.to_string();
        let count = limit.to_string();
        let results = self
            .transaction(
                "turn_page",
                vec![
                    statement(TURN_START, vec![&self.scope, request_id]),
                    statement(TURN_PAGE, vec![&self.scope, request_id, &cursor, &count]),
                ],
            )
            .await?;
        let start = single(&results[0])?
            .map(|row| text(row, 0).and_then(counter))
            .transpose()?
            .map_or(after, |first| after.max(first.saturating_sub(1)));
        page(&results[1], start, limit)
    }

    pub async fn import(&self, turns: &[ImportedTurn]) -> Result<usize, StoreError> {
        if turns.is_empty() {
            return Ok(0);
        }
        let rows = turns
            .iter()
            .map(|turn| {
                Ok(Imported {
                    request_id: &turn.request_id,
                    digest: submission_digest(&turn.prompt, turn.hand.as_deref())?,
                    status: turn.state.as_str(),
                    prompt: (!turn.state.is_terminal())
                        .then(|| serde_json::to_string(&turn.prompt))
                        .transpose()
                        .map_err(encoding)?,
                    hand: turn.hand.as_deref(),
                    metadata: turn.metadata.get(),
                    cancel_requested: u8::from(turn.cancel_requested),
                    note: turn.note.as_deref(),
                    first_seq: turn.first_seq.unwrap_or(0),
                    last_seq: turn.last_seq,
                })
            })
            .collect::<Result<Vec<_>, StoreError>>()?;
        let encoded = serde_json::to_string(&rows).map_err(encoding)?;
        let created = now().to_string();
        let results: Vec<_> = turns
            .iter()
            .filter(|turn| turn.state.is_terminal() && turn.last_seq.is_none())
            .filter_map(|turn| {
                turn.result.as_ref().map(|fields| {
                    serde_json::to_string(&Outcome {
                        kind: "result",
                        request_id: &turn.request_id,
                        status: turn.state.as_str(),
                        fields,
                    })
                    .map(|body| (turn.request_id.as_str(), body))
                })
            })
            .collect::<Result<_, _>>()
            .map_err(encoding)?;
        let mut statements = vec![statement(IMPORT, vec![&self.scope, &encoded])];
        for (request_id, body) in &results {
            statements.push(statement(
                IMPORT_RESULT,
                vec![&self.scope, request_id, body, &created],
            ));
            statements.push(statement(IMPORT_FINISH, vec![&self.scope, request_id]));
        }
        self.transaction("import", statements).await?;
        Ok(rows.len())
    }
}
