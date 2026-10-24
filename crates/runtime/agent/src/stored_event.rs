use std::{borrow::Cow, collections::BTreeMap};

use nanocode_subagents::{AgentId, AgentUpdate, ScopedAgentUpdate};
use serde::{Serialize, Serializer, de::Error as _, ser::SerializeMap};
use serde_json::{
    Value,
    value::{RawValue, to_raw_value},
};

use crate::{
    SessionEvent,
    events::{AgentEvent, AgentEventKind},
    session_log::LoggedEvent,
};

const IMPLIED_PROTOCOL_VERSION: u32 = 1;
const DERIVED: &str = "derived";

type Object = BTreeMap<String, Box<RawValue>>;

#[derive(Clone, Copy)]
pub(crate) enum EventForm<'a> {
    Full,
    Compact { session_id: &'a str },
}

pub(crate) struct Stored<'a> {
    pub(crate) event: &'a SessionEvent,
    pub(crate) form: EventForm<'a>,
}

impl Serialize for Stored<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match (self.event, self.form) {
            (SessionEvent::Agent { event, .. }, EventForm::Full) => {
                let mut map = serializer.serialize_map(Some(2))?;
                map.serialize_entry("source", "agent")?;
                match without_timing(&event.payload) {
                    Some(payload) => map.serialize_entry(
                        "event",
                        &AgentEvent {
                            payload: payload.into(),
                            ..event.clone()
                        },
                    )?,
                    None => map.serialize_entry("event", event)?,
                }
                map.end()
            }
            (SessionEvent::Agent { event, .. }, EventForm::Compact { session_id }) => {
                let mut map = serializer.serialize_map(Some(2))?;
                map.serialize_entry("source", "agent")?;
                map.serialize_entry(
                    "event",
                    &Compact {
                        event,
                        root_session_id: Some(session_id),
                    },
                )?;
                map.end()
            }
            (
                SessionEvent::Subagent(ScopedAgentUpdate {
                    detach,
                    root_session_id,
                    update: AgentUpdate::Event { id, event },
                }),
                EventForm::Compact { .. },
            ) => {
                let mut map = serializer.serialize_map(Some(4))?;
                map.serialize_entry("source", "subagent")?;
                map.serialize_entry("root_session_id", root_session_id)?;
                map.serialize_entry("detach", detach)?;
                map.serialize_entry("update", &ChildEvent { id, event })?;
                map.end()
            }
            (event, _) => event.serialize(serializer),
        }
    }
}

struct ChildEvent<'a> {
    id: &'a AgentId,
    event: &'a AgentEvent,
}

impl Serialize for ChildEvent<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut map = serializer.serialize_map(Some(3))?;
        map.serialize_entry("type", "event")?;
        map.serialize_entry("id", self.id)?;
        map.serialize_entry(
            "event",
            &Compact {
                event: self.event,
                root_session_id: None,
            },
        )?;
        map.end()
    }
}

struct Compact<'a> {
    event: &'a AgentEvent,
    root_session_id: Option<&'a str>,
}

impl Serialize for Compact<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let event = self.event;
        let slim = match self.root_session_id {
            Some(_) => without_timing(&event.payload),
            None => None,
        };
        let payload = slim.as_deref().unwrap_or(&event.payload);
        let deduplicated = (event.kind == AgentEventKind::ToolResult)
            .then(|| deduplicated(payload))
            .flatten();
        let mut map = serializer.serialize_map(None)?;
        if event.protocol_version != IMPLIED_PROTOCOL_VERSION {
            map.serialize_entry("protocol_version", &event.protocol_version)?;
        }
        if self.root_session_id != Some(&*event.request_id) {
            map.serialize_entry("request_id", &event.request_id)?;
        }
        map.serialize_entry("seq", &event.seq)?;
        map.serialize_entry("type", &event.kind)?;
        map.serialize_entry("payload", deduplicated.as_deref().unwrap_or(payload))?;
        map.end()
    }
}

fn without_timing(payload: &RawValue) -> Option<Box<RawValue>> {
    if !payload.get().contains("\"timing\"") {
        return None;
    }
    let Ok(Value::Object(mut fields)) = serde_json::from_str::<Value>(payload.get()) else {
        return None;
    };
    fields.remove("timing")?;
    to_raw_value(&fields).ok()
}

enum Derived {
    StructuredResult,
    Result,
    ResultText(Object),
}

fn deduplicated(payload: &RawValue) -> Option<Box<RawValue>> {
    let mut fields: Object = serde_json::from_str(payload.get()).ok()?;
    let structured = fields.get("structured_result")?;
    let result = fields.get("result")?;
    let derived = match serde_json::from_str::<Object>(result.get()) {
        Ok(mut wrapper) => {
            let text = wrapper.get("text")?;
            if same(text, structured) {
                Derived::StructuredResult
            } else if encodes(text, structured) {
                wrapper.remove("text");
                Derived::ResultText(wrapper)
            } else {
                return None;
            }
        }
        Err(_) if same(result, structured) => Derived::StructuredResult,
        Err(_) if encodes(result, structured) => Derived::Result,
        Err(_) => return None,
    };
    let field = match derived {
        Derived::StructuredResult => {
            fields.remove("structured_result");
            "structured_result"
        }
        Derived::Result => {
            fields.remove("result");
            "result"
        }
        Derived::ResultText(wrapper) => {
            fields.insert("result".into(), to_raw_value(&wrapper).ok()?);
            "result.text"
        }
    };
    fields.insert(DERIVED.into(), to_raw_value(field).ok()?);
    to_raw_value(&fields).ok()
}

fn same(left: &RawValue, right: &RawValue) -> bool {
    if left.get() == right.get() {
        return true;
    }
    left.get().len() == right.get().len()
        && matches!(
            (
                serde_json::from_str::<Value>(left.get()),
                serde_json::from_str::<Value>(right.get()),
            ),
            (Ok(left), Ok(right)) if left == right
        )
}

fn encodes(text: &RawValue, structured: &RawValue) -> bool {
    text.get().len() >= structured.get().len() + 2
        && serde_json::from_str::<Cow<'_, str>>(text.get())
            .is_ok_and(|text| text == structured.get())
}

impl LoggedEvent {
    pub fn expanded(&self, session_id: &str) -> Result<Self, serde_json::Error> {
        Ok(match expand_body(&self.event, session_id)? {
            Some(event) => Self {
                event,
                ..self.clone()
            },
            None => self.clone(),
        })
    }
}

fn expand_body(
    body: &RawValue,
    session_id: &str,
) -> Result<Option<Box<RawValue>>, serde_json::Error> {
    let Ok(mut fields) = serde_json::from_str::<Object>(body.get()) else {
        return Ok(None);
    };
    let source = match fields.get("source") {
        Some(source) => Some(serde_json::from_str::<Cow<'_, str>>(source.get())?),
        None => None,
    };
    let (key, expanded) = match source.as_deref() {
        Some("agent") => (
            "event",
            expand_event(field(&fields, "event")?, Some(session_id))?,
        ),
        Some("subagent") => ("update", expand_update(field(&fields, "update")?)?),
        _ => return Ok(None),
    };
    let Some(expanded) = expanded else {
        return Ok(None);
    };
    fields.insert(key.into(), expanded);
    to_raw_value(&fields).map(Some)
}

fn expand_update(update: &RawValue) -> Result<Option<Box<RawValue>>, serde_json::Error> {
    let mut fields: Object = serde_json::from_str(update.get())?;
    let Some(event) = fields.get("event") else {
        return Ok(None);
    };
    let Some(event) = expand_event(event, None)? else {
        return Ok(None);
    };
    fields.insert("event".into(), event);
    to_raw_value(&fields).map(Some)
}

fn expand_event(
    event: &RawValue,
    session_id: Option<&str>,
) -> Result<Option<Box<RawValue>>, serde_json::Error> {
    let mut fields: Object = serde_json::from_str(event.get())?;
    let mut changed = false;
    if !fields.contains_key("protocol_version") {
        fields.insert(
            "protocol_version".into(),
            to_raw_value(&IMPLIED_PROTOCOL_VERSION)?,
        );
        changed = true;
    }
    if let Some(session_id) = session_id
        && !fields.contains_key("request_id")
    {
        fields.insert("request_id".into(), to_raw_value(session_id)?);
        changed = true;
    }
    let kind: Cow<'_, str> = serde_json::from_str(field(&fields, "type")?.get())?;
    if kind == "tool.result"
        && let Some(payload) = expand_tool_result(field(&fields, "payload")?)?
    {
        fields.insert("payload".into(), payload);
        changed = true;
    }
    changed.then(|| to_raw_value(&fields)).transpose()
}

fn expand_tool_result(payload: &RawValue) -> Result<Option<Box<RawValue>>, serde_json::Error> {
    let mut fields: Object = serde_json::from_str(payload.get())?;
    let Some(derived) = fields.remove(DERIVED) else {
        return Ok(None);
    };
    let derived: Cow<'_, str> = serde_json::from_str(derived.get())?;
    let (name, value) = match &*derived {
        "structured_result" => {
            let result = field(&fields, "result")?;
            let text = match serde_json::from_str::<Object>(result.get()) {
                Ok(mut wrapper) => wrapper
                    .remove("text")
                    .ok_or_else(|| missing("result.text"))?,
                Err(_) => result.to_owned(),
            };
            ("structured_result", text)
        }
        "result" => (
            "result",
            to_raw_value(field(&fields, "structured_result")?.get())?,
        ),
        "result.text" => {
            let mut wrapper: Object = serde_json::from_str(field(&fields, "result")?.get())?;
            wrapper.insert(
                "text".into(),
                to_raw_value(field(&fields, "structured_result")?.get())?,
            );
            ("result", to_raw_value(&wrapper)?)
        }
        other => {
            return Err(serde_json::Error::custom(format!(
                "unknown derived tool result field `{other}`"
            )));
        }
    };
    fields.insert(name.into(), value);
    to_raw_value(&fields).map(Some)
}

fn field<'a>(fields: &'a Object, name: &str) -> Result<&'a RawValue, serde_json::Error> {
    fields
        .get(name)
        .map(Box::as_ref)
        .ok_or_else(|| missing(name))
}

fn missing(field: &str) -> serde_json::Error {
    serde_json::Error::custom(format!("stored agent event is missing `{field}`"))
}
