use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

use nanocode_sync::Atom;

const MAX_TASKS: usize = 256;
const MAX_TODOS: usize = 256;
const MAX_INPUT: usize = 64 * 1024;
const MAX_OUTPUT: usize = 64 * 1024;
const MAX_TEXT: usize = 8 * 1024;
const MAX_METADATA: usize = 16 * 1024;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Task {
    id: String,
    subject: String,
    description: String,
    active_form: Option<String>,
    status: String,
    owner: Option<String>,
    metadata: Map<String, Value>,
    blocks: BTreeSet<String>,
    blocked_by: BTreeSet<String>,
}

#[derive(Clone, Default, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct State {
    next_id: u64,
    tasks: BTreeMap<String, Task>,
    todos: Vec<Value>,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Checkpoint {
    version: u32,
    state: State,
}

#[derive(Clone, Default, Debug)]
pub struct ClaudeTasks {
    state: Arc<Atom<State>>,
}

impl ClaudeTasks {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    pub fn snapshot(&self) -> Result<Value, String> {
        Ok(json!({"version":1,"state":&*self.state.get()}))
    }

    pub fn restore(&self, checkpoint: Value) -> Result<(), String> {
        let checkpoint: Checkpoint = serde_json::from_value(checkpoint)
            .map_err(|e| format!("invalid task checkpoint: {e}"))?;
        if checkpoint.version != 1 {
            return Err("unsupported task checkpoint version".into());
        }
        validate_state(&checkpoint.state)?;
        self.state.set(checkpoint.state);
        Ok(())
    }

    #[must_use]
    pub fn definitions() -> Vec<Value> {
        vec![
            crate::upstream::tool_value("TaskCreate"),
            crate::upstream::tool_value("TaskGet"),
            crate::upstream::tool_value("TaskList"),
            crate::upstream::tool_value("TaskUpdate"),
            json!({"name":"TodoWrite","description":"Replace the session-local todo list.","input_schema":{"type":"object","properties":{"todos":{"type":"array","maxItems":MAX_TODOS,"items":{"type":"object","properties":{"content":{"type":"string"},"status":{"type":"string","enum":["pending","in_progress","completed"]},"activeForm":{"type":"string"}},"required":["content","status","activeForm"],"additionalProperties":false}}},"required":["todos"],"additionalProperties":false}}),
        ]
    }

    #[must_use]
    pub fn tool_schemas() -> Vec<Value> {
        Self::definitions()
    }

    pub async fn execute(&self, name: &str, input: Value) -> Result<String, String> {
        if serde_json::to_vec(&input).map_err(|e| e.to_string())?.len() > MAX_INPUT {
            return Err("task input exceeds 64 KiB".into());
        }
        let fields = input.as_object().ok_or("task input must be an object")?;
        let allowed: &[&str] = match name {
            "TaskCreate" => &["subject", "description", "activeForm", "metadata"],
            "TaskGet" => &["taskId"],
            "TaskList" => &[],
            "TaskUpdate" => &[
                "taskId",
                "subject",
                "description",
                "activeForm",
                "status",
                "addBlocks",
                "addBlockedBy",
                "owner",
                "metadata",
            ],
            "TodoWrite" => &["todos"],
            _ => return Err(format!("unknown task tool: {name}")),
        };
        if let Some(field) = fields.keys().find(|key| !allowed.contains(&key.as_str())) {
            return Err(format!("unsupported {name} field: {field}"));
        }
        let result = match name {
            "TaskCreate" => self.create(fields)?,
            "TaskGet" => self.get(fields)?,
            "TaskList" => self.list()?,
            "TaskUpdate" => self.update(fields)?,
            "TodoWrite" => self.todo_write(fields)?,
            _ => unreachable!(),
        };
        let output = serde_json::to_string(&result).map_err(|e| e.to_string())?;
        if output.len() > MAX_OUTPUT {
            return Err("task output exceeds 64 KiB".into());
        }
        Ok(output)
    }

    fn create(&self, fields: &Map<String, Value>) -> Result<Value, String> {
        let subject = required_text(fields, "subject")?.to_owned();
        let description = required_text(fields, "description")?.to_owned();
        let active_form = optional_text(fields, "activeForm")?.map(str::to_owned);
        let metadata = metadata(fields)?.unwrap_or_default();
        self.state.modify(|state| {
            if state.tasks.len() >= MAX_TASKS {
                return Err("session task limit reached".into());
            }
            let next_id = state
                .next_id
                .checked_add(1)
                .ok_or("task ID limit reached")?;
            let id = next_id.to_string();
            let task = Task {
                id: id.clone(),
                subject: subject.clone(),
                description: description.clone(),
                active_form: active_form.clone(),
                status: "pending".into(),
                owner: None,
                metadata: metadata.clone(),
                blocks: BTreeSet::new(),
                blocked_by: BTreeSet::new(),
            };

            let result = json!({"task":{"id":id,"subject":subject}});
            ensure_output(&result)?;
            state.tasks.insert(id, task);
            ensure_readable(&state.tasks)?;
            state.next_id = next_id;
            Ok(result)
        })
    }

    fn get(&self, fields: &Map<String, Value>) -> Result<Value, String> {
        let id = required_text(fields, "taskId")?;
        Ok(self.state.read(|state| task_result(state.tasks.get(id))))
    }

    fn list(&self) -> Result<Value, String> {
        Ok(self.state.read(|state| list_result(&state.tasks)))
    }

    fn update(&self, fields: &Map<String, Value>) -> Result<Value, String> {
        let id = required_text(fields, "taskId")?.to_owned();
        let subject = optional_text(fields, "subject")?;
        let description = optional_text(fields, "description")?;
        let active_form = optional_text(fields, "activeForm")?;
        let owner = optional_text(fields, "owner")?;
        let status = optional_text(fields, "status")?;
        if let Some(value) = status
            && !matches!(value, "pending" | "in_progress" | "completed" | "deleted")
        {
            return Err("invalid status".into());
        }
        let blocks = id_list(fields, "addBlocks")?;
        let blocked_by = id_list(fields, "addBlockedBy")?;
        let changes = metadata(fields)?;
        self.state.modify(|state| {
            let original = state.tasks.get(&id).ok_or("task not found")?;
            let old_status = original.status.clone();
            if status == Some("deleted") {
                if fields.keys().any(|key| key != "taskId" && key != "status") {
                    return Err("deleted status cannot be combined with other changes".into());
                }
                let result = json!({"success":true,"taskId":id,"updatedFields":["status"],"statusChange":{"from":old_status,"to":"deleted"}});
                ensure_output(&result)?;
                state.tasks.remove(&id);
                for task in state.tasks.values_mut() {
                    task.blocks.remove(&id);
                    task.blocked_by.remove(&id);
                }
                return Ok(result);
            }

            let mut candidate = original.clone();
            let mut updated_fields = Vec::new();
            for (key, value) in [
                ("subject", subject),
                ("description", description),
                ("activeForm", active_form),
                ("owner", owner),
            ] {
                if let Some(value) = value {
                    match key {
                        "subject" => candidate.subject = value.to_owned(),
                        "description" => candidate.description = value.to_owned(),
                        "activeForm" => candidate.active_form = Some(value.to_owned()),
                        "owner" => candidate.owner = Some(value.to_owned()),
                        _ => unreachable!(),
                    }
                    updated_fields.push(key);
                }
            }
            if let Some(status) = status {
                candidate.status = status.into();
                updated_fields.push("status");
            }
            if let Some(changes) = &changes {
                for (key, value) in changes {
                    if value.is_null() {
                        candidate.metadata.remove(key);
                    } else {
                        candidate.metadata.insert(key.clone(), value.clone());
                    }
                }
                if serde_json::to_vec(&candidate.metadata)
                    .map_err(|e| e.to_string())?
                    .len()
                    > MAX_METADATA
                {
                    return Err("task metadata exceeds 16 KiB".into());
                }
                updated_fields.push("metadata");
            }
            let mut edges = Vec::new();
            if fields.contains_key("addBlocks") {
                updated_fields.push("addBlocks");
            }
            if fields.contains_key("addBlockedBy") {
                updated_fields.push("addBlockedBy");
            }
            for target in &blocks {
                edges.push((id.clone(), target.clone()));
            }
            for source in &blocked_by {
                edges.push((source.clone(), id.clone()));
            }
            for (source, target) in &edges {
                if source == target {
                    return Err("task cannot depend on itself".into());
                }
                if !state.tasks.contains_key(source) || !state.tasks.contains_key(target) {
                    return Err("dependency task not found".into());
                }
            }

            let mut adjacency: BTreeMap<String, BTreeSet<String>> = state
                .tasks
                .iter()
                .map(|(id, task)| (id.clone(), task.blocks.clone()))
                .collect();
            for (source, target) in &edges {
                adjacency
                    .get_mut(source)
                    .expect("validated source")
                    .insert(target.clone());
            }
            if has_cycle(&adjacency) {
                return Err("dependency cycle".into());
            }
            let mut result = json!({"success":true,"taskId":id,"updatedFields":updated_fields});
            if let Some(status) = status
                && status != old_status
            {
                result["statusChange"] = json!({"from":old_status,"to":status});
            }
            ensure_output(&result)?;
            state.tasks.insert(id.clone(), candidate);
            for (source, target) in edges {
                state
                    .tasks
                    .get_mut(&source)
                    .expect("validated source")
                    .blocks
                    .insert(target.clone());
                state
                    .tasks
                    .get_mut(&target)
                    .expect("validated target")
                    .blocked_by
                    .insert(source);
            }

            ensure_readable(&state.tasks)?;
            Ok(result)
        })
    }

    fn todo_write(&self, fields: &Map<String, Value>) -> Result<Value, String> {
        let todos = fields
            .get("todos")
            .and_then(Value::as_array)
            .ok_or("missing or invalid todos")?;
        validate_todos(todos)?;
        self.state.modify(|state| {
            let result = json!({"oldTodos":state.todos,"newTodos":todos});
            ensure_output(&result)?;
            state.todos.clone_from(todos);
            Ok(result)
        })
    }
}

fn validate_state(state: &State) -> Result<(), String> {
    if state.tasks.len() > MAX_TASKS {
        return Err("session task limit reached".into());
    }
    for (id, task) in &state.tasks {
        let numeric = id
            .parse::<u64>()
            .map_err(|_| "invalid checkpoint task ID")?;
        if numeric == 0 || numeric > state.next_id || numeric.to_string() != *id || task.id != *id {
            return Err("invalid checkpoint task ID watermark or identity".into());
        }
        for text in [&task.subject, &task.description]
            .into_iter()
            .chain(task.active_form.iter())
            .chain(task.owner.iter())
        {
            if text.len() > MAX_TEXT {
                return Err("checkpoint task text exceeds 8 KiB".into());
            }
        }
        if !matches!(
            task.status.as_str(),
            "pending" | "in_progress" | "completed"
        ) {
            return Err("invalid checkpoint task status".into());
        }
        metadata(&Map::from_iter([("metadata".into(), json!(task.metadata))]))?;
        for target in &task.blocks {
            if target == id
                || !state
                    .tasks
                    .get(target)
                    .is_some_and(|t| t.blocked_by.contains(id))
            {
                return Err("invalid checkpoint dependency".into());
            }
        }
        for source in &task.blocked_by {
            if source == id
                || !state
                    .tasks
                    .get(source)
                    .is_some_and(|t| t.blocks.contains(id))
            {
                return Err("invalid checkpoint dependency".into());
            }
        }
    }
    let adjacency = state
        .tasks
        .iter()
        .map(|(id, t)| (id.clone(), t.blocks.clone()))
        .collect();
    if has_cycle(&adjacency) {
        return Err("checkpoint dependency cycle".into());
    }
    validate_todos(&state.todos)?;
    ensure_output(&json!({"oldTodos":state.todos,"newTodos":[]}))?;
    ensure_readable(&state.tasks)
}

fn validate_todos(todos: &[Value]) -> Result<(), String> {
    if todos.len() > MAX_TODOS {
        return Err("session todo limit reached".into());
    }
    for todo in todos {
        let object = todo.as_object().ok_or("todo must be an object")?;
        if object.len() != 3
            || object
                .keys()
                .any(|key| !["content", "status", "activeForm"].contains(&key.as_str()))
        {
            return Err("todo must contain only content, status, activeForm".into());
        }
        required_text(object, "content")?;
        required_text(object, "activeForm")?;
        if !matches!(
            required_text(object, "status")?,
            "pending" | "in_progress" | "completed"
        ) {
            return Err("invalid todo status".into());
        }
    }
    Ok(())
}

fn task_result(task: Option<&Task>) -> Value {
    json!({"task": task.map(|t| json!({
        "id":t.id,"subject":t.subject,"description":t.description,
        "status":t.status,"blocks":t.blocks,"blockedBy":t.blocked_by
    }))})
}

fn list_result(tasks: &BTreeMap<String, Task>) -> Value {
    let tasks: Vec<Value> = tasks
        .values()
        .map(|t| {
            let mut task =
                json!({"id":t.id,"subject":t.subject,"status":t.status,"blockedBy":t.blocked_by});
            if let Some(owner) = &t.owner {
                task["owner"] = json!(owner);
            }
            task
        })
        .collect();
    json!({"tasks":tasks})
}

fn ensure_readable(tasks: &BTreeMap<String, Task>) -> Result<(), String> {
    ensure_output(&list_result(tasks))?;
    for task in tasks.values() {
        ensure_output(&task_result(Some(task)))?;
    }
    Ok(())
}

fn required_text<'a>(fields: &'a Map<String, Value>, key: &str) -> Result<&'a str, String> {
    let value = fields
        .get(key)
        .and_then(Value::as_str)
        .ok_or_else(|| format!("missing or invalid {key}"))?;
    if value.len() > MAX_TEXT {
        return Err(format!("{key} exceeds 8 KiB"));
    }
    Ok(value)
}

fn optional_text<'a>(fields: &'a Map<String, Value>, key: &str) -> Result<Option<&'a str>, String> {
    if fields.contains_key(key) {
        required_text(fields, key).map(Some)
    } else {
        Ok(None)
    }
}

fn metadata(fields: &Map<String, Value>) -> Result<Option<Map<String, Value>>, String> {
    let Some(value) = fields.get("metadata") else {
        return Ok(None);
    };
    let object = value.as_object().ok_or("metadata must be an object")?;
    if serde_json::to_vec(object).map_err(|e| e.to_string())?.len() > MAX_METADATA {
        return Err("metadata exceeds 16 KiB".into());
    }
    Ok(Some(object.clone()))
}

fn id_list(fields: &Map<String, Value>, key: &str) -> Result<Vec<String>, String> {
    let Some(value) = fields.get(key) else {
        return Ok(Vec::new());
    };
    let list = value
        .as_array()
        .ok_or_else(|| format!("{key} must be an array"))?;
    if list.len() > MAX_TASKS {
        return Err(format!("{key} exceeds task limit"));
    }
    list.iter()
        .map(|entry| {
            entry
                .as_str()
                .filter(|s| !s.is_empty() && s.len() <= MAX_TEXT)
                .map(str::to_owned)
                .ok_or_else(|| format!("{key} must contain task IDs"))
        })
        .collect()
}

fn ensure_output(value: &Value) -> Result<(), String> {
    if serde_json::to_vec(value).map_err(|e| e.to_string())?.len() > MAX_OUTPUT {
        return Err("task output exceeds 64 KiB".into());
    }
    Ok(())
}

fn has_cycle(edges: &BTreeMap<String, BTreeSet<String>>) -> bool {
    fn visit(
        id: &str,
        edges: &BTreeMap<String, BTreeSet<String>>,
        marks: &mut BTreeMap<String, u8>,
    ) -> bool {
        match marks.get(id) {
            Some(1) => return true,
            Some(2) => return false,
            _ => {}
        }
        marks.insert(id.to_owned(), 1);
        if let Some(targets) = edges.get(id) {
            for target in targets {
                if visit(target, edges, marks) {
                    return true;
                }
            }
        }
        marks.insert(id.to_owned(), 2);
        false
    }
    let mut marks = BTreeMap::new();
    edges.keys().any(|id| visit(id, edges, &mut marks))
}
