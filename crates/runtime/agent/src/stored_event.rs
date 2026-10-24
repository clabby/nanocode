use serde::{Serialize, Serializer, ser::SerializeMap};
use serde_json::Value;

use crate::{SessionEvent, events::AgentEvent};

pub(crate) struct Stored<'a>(pub(crate) &'a SessionEvent);

impl Serialize for Stored<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let SessionEvent::Agent { event, .. } = self.0 else {
            return self.0.serialize(serializer);
        };
        let mut map = serializer.serialize_map(Some(2))?;
        map.serialize_entry("source", "agent")?;
        match without_timing(event) {
            Some(slim) => map.serialize_entry("event", &slim)?,
            None => map.serialize_entry("event", event)?,
        }
        map.end()
    }
}

fn without_timing(event: &AgentEvent) -> Option<AgentEvent> {
    if !event.payload.get().contains("\"timing\"") {
        return None;
    }
    let Ok(Value::Object(mut fields)) = serde_json::from_str::<Value>(event.payload.get()) else {
        return None;
    };
    fields.remove("timing")?;
    Some(AgentEvent {
        payload: serde_json::value::to_raw_value(&fields).ok()?.into(),
        ..event.clone()
    })
}
