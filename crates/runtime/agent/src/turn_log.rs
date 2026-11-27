use std::time::Duration;

use nanocode_tool_host::embedded::HostFuture;
use serde_json::{Map, Value};

use crate::{
    SessionEvent,
    durability::StoreError,
    session_log::{LoggedEvent, Replaced, Started, TurnRecord, TurnState},
};

pub type Logged<'a, T> = HostFuture<'a, Result<T, StoreError>>;

pub struct Recording<'a> {
    pub session_id: &'a str,
    pub annotation: &'a Map<String, Value>,
    pub events: &'a [SessionEvent],
    pub request_ids: &'a [Option<String>],
    pub read: &'a (dyn Fn(&str) -> bool + Send + Sync),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Verdict<T> {
    Accepted,
    Existing(T),
    Conflict(T),
    Steered(T),
}

impl<T> Verdict<T> {
    pub fn of(previous: Option<(T, bool)>, active: Option<T>, steers: bool) -> Self {
        match (previous, active) {
            (Some((previous, true)), _) => Self::Existing(previous),
            (Some((previous, false)), _) => Self::Conflict(previous),
            (None, Some(active)) if steers => Self::Steered(active),
            (None, Some(active)) => Self::Conflict(active),
            (None, None) => Self::Accepted,
        }
    }
}

pub trait EventLog: Send + Sync + 'static {
    fn record<'a>(
        &'a self,
        recording: Recording<'a>,
    ) -> HostFuture<'a, Result<Vec<LoggedEvent>, String>>;

    fn flush(&self) -> Logged<'_, ()>;

    fn tick(&self) -> Option<Duration> {
        None
    }
}

#[derive(Clone, Debug)]
pub enum Guarded<T> {
    Ready(T),
    Steering,
}

impl<T> Guarded<T> {
    pub(crate) fn ready(self, request_id: &str) -> Result<T, StoreError> {
        match self {
            Self::Ready(value) => Ok(value),
            Self::Steering => Err(StoreError::NotCommitted(format!(
                "request {request_id} is held as a steer by the running turn"
            ))),
        }
    }
}

pub trait TurnLog: EventLog {
    fn turn<'a>(&'a self, request_id: &'a str) -> Logged<'a, Option<TurnRecord>>;

    fn pending(&self) -> Logged<'_, Vec<TurnRecord>>;

    fn start<'a>(&'a self, request_id: &'a str) -> Logged<'a, Option<Started>>;

    fn finish<'a>(
        &'a self,
        request_id: &'a str,
        state: TurnState,
        fields: &'a Map<String, Value>,
    ) -> Logged<'a, Option<(TurnRecord, LoggedEvent)>>;

    fn cancel<'a>(
        &'a self,
        request_id: &'a str,
        fields: &'a Map<String, Value>,
    ) -> Logged<'a, Option<(TurnRecord, Option<LoggedEvent>)>>;

    fn withdraw_steered<'a>(
        &'a self,
        request_id: &'a str,
        digest: &'a str,
    ) -> Logged<'a, Option<(TurnRecord, LoggedEvent)>>;

    fn replace_queued<'a>(
        &'a self,
        _request_id: &'a str,
        _edit_id: &'a str,
        _text: &'a str,
        steering: bool,
    ) -> Logged<'a, Guarded<Replaced>> {
        Box::pin(async move {
            if steering {
                return Ok(Guarded::Steering);
            }
            Err(StoreError::NotCommitted(
                "this log does not edit queued requests".into(),
            ))
        })
    }
}
