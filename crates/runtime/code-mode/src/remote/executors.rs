use std::{
    collections::{BTreeMap, HashMap},
    sync::{Arc, Mutex, MutexGuard, PoisonError},
};

use crate::{
    Tool, ToolContext, ToolDefinition, ToolOutputBody, ToolOutputContent,
    embedded::{
        CodeModeCell, CodeModeExecution, CodeModeHost, CodeModeHostError, CodeModeObserver,
        CodeModeUpdate, EmbeddedToolMode, HostFuture, NestedToolCall,
    },
};

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ExecutorSelection {
    Local,
    Executor(Arc<str>),
    Executors(Arc<[Arc<str>]>),
    All,
}

#[derive(Clone)]
pub struct HandRoute {
    pub name: Arc<str>,
    pub host: Option<Arc<dyn CodeModeHost>>,
}

#[derive(Clone)]
pub enum Placement {
    Local,
    Hands {
        hands: Vec<HandRoute>,
        broadcast: bool,
    },
}

#[derive(Clone, Default)]
pub struct CodeModeExecutors {
    state: Arc<Mutex<State>>,
}

#[derive(Default)]
struct State {
    executors: BTreeMap<Arc<str>, Arc<dyn CodeModeHost>>,
    selections: HashMap<Box<str>, ExecutorSelection>,
    roots: HashMap<Box<str>, Box<str>>,
    default: Option<Arc<str>>,
    sessions: HashMap<Box<str>, SessionPlaces>,
}

#[derive(Default)]
struct SessionPlaces {
    touched: Vec<Arc<str>>,
    cells: Vec<RunningCell>,
}

struct RunningCell {
    origin: Box<str>,
    place: Option<Arc<str>>,
    id: Option<Box<str>>,
}

const RUNNING_CELL: &str = "Script running with cell ID ";

fn cell_id(execution: &CodeModeExecution) -> Option<Box<str>> {
    let texts: Vec<&str> = match &execution.output {
        ToolOutputBody::Text(text) => vec![text.as_str()],
        ToolOutputBody::Content(items) => items
            .iter()
            .filter_map(|item| match item {
                ToolOutputContent::InputText { text } => Some(text.as_str()),
                _ => None,
            })
            .collect(),
    };
    texts.into_iter().find_map(|text| {
        let start = text.find(RUNNING_CELL)? + RUNNING_CELL.len();
        let id = text[start..]
            .split(|character: char| !character.is_ascii_digit())
            .next()?;
        (!id.is_empty()).then(|| id.into())
    })
}

impl State {
    fn resolve(&self, session_id: &str) -> ExecutorSelection {
        self.selections
            .get(session_id)
            .or_else(|| {
                self.roots
                    .get(session_id)
                    .and_then(|root| self.selections.get(root.as_ref()))
            })
            .cloned()
            .or_else(|| self.default.clone().map(ExecutorSelection::Executor))
            .unwrap_or(ExecutorSelection::Local)
    }

    fn hand(&self, name: &Arc<str>) -> HandRoute {
        HandRoute {
            name: Arc::clone(name),
            host: self.executors.get(name).cloned(),
        }
    }

    fn placement(&self, session_id: &str) -> Placement {
        match self.resolve(session_id) {
            ExecutorSelection::Local => Placement::Local,
            ExecutorSelection::Executor(name) => Placement::Hands {
                hands: vec![self.hand(&name)],
                broadcast: false,
            },
            ExecutorSelection::Executors(names) => Placement::Hands {
                hands: names.iter().map(|name| self.hand(name)).collect(),
                broadcast: true,
            },
            ExecutorSelection::All => Placement::Hands {
                hands: self
                    .executors
                    .iter()
                    .map(|(name, host)| HandRoute {
                        name: Arc::clone(name),
                        host: Some(Arc::clone(host)),
                    })
                    .collect(),
                broadcast: true,
            },
        }
    }

    fn forget_executor(&mut self, name: &str) {
        for places in self.sessions.values_mut() {
            places.touched.retain(|touched| &**touched != name);
            places
                .cells
                .retain(|cell| cell.place.as_deref() != Some(name));
        }
    }
}

fn disconnected(name: &str) -> CodeModeHostError {
    CodeModeHostError::new(format!(
        "hand `{name}` selected for this session is not connected; nothing was dispatched"
    ))
}

fn unselected() -> CodeModeHostError {
    CodeModeHostError::new("no hand is selected for this session; nothing was dispatched")
}

fn nobody() -> CodeModeHostError {
    CodeModeHostError::new(
        "none of the hands selected for this session is connected; nothing was dispatched",
    )
}

struct HandObserver<'a, 'o> {
    hand: &'a str,
    inner: &'a Mutex<&'o mut dyn CodeModeObserver>,
}

impl CodeModeObserver for HandObserver<'_, '_> {
    fn update(&mut self, update: CodeModeUpdate<'_>) {
        let mut inner = self.inner.lock().unwrap_or_else(PoisonError::into_inner);
        match update {
            CodeModeUpdate::NestedCallStarted {
                call_id,
                name,
                input,
            } => {
                let call_id = format!("{}:{call_id}", self.hand);
                inner.update(CodeModeUpdate::NestedCallStarted {
                    call_id: &call_id,
                    name,
                    input,
                });
            }
            CodeModeUpdate::NestedCallCompleted(call) => {
                inner.update(CodeModeUpdate::NestedCallCompleted(&relabel(
                    self.hand, call,
                )));
            }
        }
    }
}

fn relabel(hand: &str, call: &NestedToolCall) -> NestedToolCall {
    NestedToolCall {
        call_id: format!("{hand}:{}", call.call_id),
        name: call.name.clone(),
        input: call.input.clone(),
        output: call.output.clone(),
        structured_result: call.structured_result.clone(),
        success: call.success,
        started_after_ns: call.started_after_ns,
        duration_ns: call.duration_ns,
        metadata: call.metadata.clone(),
    }
}

fn combine(origin_call_id: &str, results: Vec<(Arc<str>, CodeModeExecution)>) -> CodeModeExecution {
    let mut content = Vec::new();
    let mut success = true;
    let mut cell = None::<bool>;
    let mut nested_calls = Vec::new();
    let mut notifications = Vec::new();
    for (name, execution) in results {
        success &= execution.success;
        if let Some(running) = execution.cell.as_ref().map(|cell| cell.running) {
            cell = Some(cell.unwrap_or(false) || running);
        }
        content.push(ToolOutputContent::InputText {
            text: format!("[hand {name}]\n"),
        });
        match execution.output {
            ToolOutputBody::Text(text) => content.push(ToolOutputContent::InputText { text }),
            ToolOutputBody::Content(items) => content.extend(items),
        }
        nested_calls.extend(
            execution
                .nested_calls
                .iter()
                .map(|call| relabel(&name, call)),
        );
        notifications.extend(execution.notifications);
    }
    CodeModeExecution {
        cell: cell.map(|running| CodeModeCell {
            origin_call_id: origin_call_id.to_owned(),
            running,
        }),
        output: ToolOutputBody::Content(content),
        success,
        nested_calls,
        notifications,
    }
}

impl CodeModeExecutors {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    fn state(&self) -> MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    pub fn insert(
        &self,
        name: impl Into<Arc<str>>,
        host: Arc<dyn CodeModeHost>,
    ) -> Option<Arc<dyn CodeModeHost>> {
        let name = name.into();
        let mut state = self.state();
        let previous = state.executors.insert(Arc::clone(&name), host);
        if previous.is_some() {
            state.forget_executor(&name);
        }
        previous
    }

    pub fn remove(&self, name: &str) -> Option<Arc<dyn CodeModeHost>> {
        let mut state = self.state();
        let removed = state.executors.remove(name);
        if removed.is_some() {
            state.forget_executor(name);
        }
        removed
    }

    pub fn remove_host(&self, name: &str, host: &Arc<dyn CodeModeHost>) -> bool {
        let mut state = self.state();
        if !state
            .executors
            .get(name)
            .is_some_and(|current| Arc::ptr_eq(current, host))
        {
            return false;
        }
        state.executors.remove(name);
        state.forget_executor(name);
        true
    }

    #[must_use]
    pub fn get(&self, name: &str) -> Option<Arc<dyn CodeModeHost>> {
        self.state().executors.get(name).cloned()
    }

    pub fn connected(&self, name: &str) -> Result<Arc<dyn CodeModeHost>, CodeModeHostError> {
        self.get(name).ok_or_else(|| disconnected(name))
    }

    #[must_use]
    pub fn names(&self) -> Vec<Arc<str>> {
        self.state().executors.keys().cloned().collect()
    }

    pub fn select(&self, session_id: &str, selection: Option<ExecutorSelection>) {
        let mut state = self.state();
        match selection {
            Some(selection) => {
                state.selections.insert(session_id.into(), selection);
            }
            None => {
                state.selections.remove(session_id);
            }
        }
    }

    #[must_use]
    pub fn selection(&self, session_id: &str) -> ExecutorSelection {
        self.state().resolve(session_id)
    }

    #[must_use]
    pub fn placement(&self, session_id: &str) -> Placement {
        self.state().placement(session_id)
    }

    pub fn set_default(&self, name: Option<Arc<str>>) {
        self.state().default = name;
    }

    #[must_use]
    pub fn default_executor(&self) -> Option<Arc<str>> {
        self.state().default.clone()
    }

    pub fn clear_selections(&self, name: &str) {
        let mut state = self.state();
        state.selections.retain(|_, selection| match selection {
            ExecutorSelection::Executor(selected) => &**selected != name,
            ExecutorSelection::Executors(selected) => {
                selected.iter().all(|selected| &**selected != name)
            }
            ExecutorSelection::Local | ExecutorSelection::All => true,
        });
        if state.default.as_deref() == Some(name) {
            state.default = None;
        }
    }

    pub fn link(&self, session_id: &str, root_session_id: &str) {
        if session_id != root_session_id {
            self.state()
                .roots
                .insert(session_id.into(), root_session_id.into());
        }
    }

    pub fn forget(&self, session_id: &str) {
        let mut state = self.state();
        state.selections.remove(session_id);
        state.roots.remove(session_id);
        state.sessions.remove(session_id);
    }

    pub fn record(
        &self,
        session_id: &str,
        executor: Option<&Arc<str>>,
        execution: &CodeModeExecution,
    ) {
        let mut state = self.state();
        let places = state.sessions.entry(session_id.into()).or_default();
        if let Some(name) = executor
            && !places.touched.contains(name)
        {
            places.touched.push(Arc::clone(name));
        }
        if let Some(cell) = &execution.cell {
            let previous = places
                .cells
                .iter()
                .position(|known| {
                    *known.origin == *cell.origin_call_id && known.place.as_ref() == executor
                })
                .map(|index| places.cells.remove(index));
            if cell.running {
                places.cells.push(RunningCell {
                    origin: cell.origin_call_id.as_str().into(),
                    place: executor.cloned(),
                    id: cell_id(execution).or_else(|| previous.and_then(|known| known.id)),
                });
            }
        }
    }

    #[must_use]
    pub fn wait_route(&self, session_id: &str) -> Vec<Option<Arc<str>>> {
        let state = self.state();
        let mut places = Vec::new();
        if let Some(session) = state.sessions.get(session_id) {
            for cell in session.cells.iter().rev() {
                if !places.contains(&cell.place) {
                    places.push(cell.place.clone());
                }
            }
        }
        if let Placement::Hands { hands, .. } = state.placement(session_id) {
            for hand in hands {
                let place = Some(hand.name);
                if !places.contains(&place) {
                    places.push(place);
                }
            }
        } else if !places.contains(&None) {
            places.push(None);
        }
        places
    }

    #[must_use]
    pub const fn missing_cell(execution: &CodeModeExecution) -> bool {
        !execution.success && execution.cell.is_none()
    }

    #[must_use]
    pub fn failed(error: &CodeModeHostError) -> CodeModeExecution {
        CodeModeExecution {
            cell: None,
            output: ToolOutputBody::Text(format!("Script failed\nOutput:\n{error}")),
            success: false,
            nested_calls: Vec::new(),
            notifications: Vec::new(),
        }
    }

    #[must_use]
    pub fn touched(&self, session_id: &str) -> Vec<Arc<dyn CodeModeHost>> {
        let state = self.state();
        state
            .sessions
            .get(session_id)
            .map(|session| {
                session
                    .touched
                    .iter()
                    .filter_map(|name| state.executors.get(name).cloned())
                    .collect()
            })
            .unwrap_or_default()
    }

    pub fn settle_cells(&self, session_id: &str) {
        if let Some(session) = self.state().sessions.get_mut(session_id) {
            session.cells.clear();
        }
    }

    pub async fn execute_on(
        &self,
        hands: Vec<(HandRoute, Vec<Arc<dyn Tool>>)>,
        broadcast: bool,
        source: &str,
        context: ToolContext<'_>,
        observer: Option<&mut dyn CodeModeObserver>,
    ) -> CodeModeExecution {
        let session_id = context.session_id();
        if !broadcast {
            let Some((hand, tools)) = hands.into_iter().next() else {
                return Self::failed(&unselected());
            };
            let Some(host) = hand.host else {
                return Self::failed(&disconnected(&hand.name));
            };
            let observer = observer.map(|observer| &mut *observer as &mut dyn CodeModeObserver);
            return match host
                .execute_with_local_tools(source, context, tools, observer)
                .await
            {
                Ok(execution) => {
                    self.record(session_id, Some(&hand.name), &execution);
                    execution
                }
                Err(error) => Self::failed(&error),
            };
        }
        if !hands.iter().any(|(hand, _)| hand.host.is_some()) {
            return Self::failed(&nobody());
        }
        let mut ignored = IgnoreUpdates;
        let shared = Mutex::new(match observer {
            Some(observer) => &mut *observer as &mut dyn CodeModeObserver,
            None => &mut ignored as &mut dyn CodeModeObserver,
        });
        let runs = hands.into_iter().map(|(hand, tools)| {
            let shared = &shared;
            async move {
                let execution = match &hand.host {
                    Some(host) => {
                        let mut observer = HandObserver {
                            hand: &hand.name,
                            inner: shared,
                        };
                        match host
                            .execute_with_local_tools(source, context, tools, Some(&mut observer))
                            .await
                        {
                            Ok(execution) => {
                                self.record(session_id, Some(&hand.name), &execution);
                                execution
                            }
                            Err(error) => Self::failed(&error),
                        }
                    }
                    None => Self::failed(&disconnected(&hand.name)),
                };
                (hand.name, execution)
            }
        });
        let results = futures_util::future::join_all(runs).await;
        combine(context.call_id(), results)
    }

    pub async fn wait_on(
        &self,
        hands: Vec<HandRoute>,
        input: &str,
        context: ToolContext<'_>,
        observer: &mut dyn CodeModeObserver,
    ) -> CodeModeExecution {
        let session_id = context.session_id();
        let requested = serde_json::from_str::<serde_json::Value>(input)
            .ok()
            .and_then(|input| {
                input
                    .get("cell_id")
                    .and_then(|id| id.as_str().map(str::to_owned))
            });
        let running = {
            let state = self.state();
            let cells = state
                .sessions
                .get(session_id)
                .map_or(&[][..], |session| &session.cells[..]);
            let origin = requested.as_deref().and_then(|requested| {
                cells
                    .iter()
                    .rev()
                    .find(|cell| cell.id.as_deref() == Some(requested))
                    .map(|cell| cell.origin.clone())
            });
            cells
                .iter()
                .filter(|cell| {
                    origin
                        .as_ref()
                        .is_none_or(|origin| *cell.origin == **origin)
                })
                .filter_map(|cell| Some((cell.place.clone()?, cell.id.clone())))
                .collect::<Vec<_>>()
        };
        let mut targets = hands
            .iter()
            .filter(|hand| hand.host.is_some())
            .filter_map(|hand| {
                running
                    .iter()
                    .find(|(place, _)| *place == hand.name)
                    .map(|(_, id)| (hand.clone(), id.clone()))
            })
            .collect::<Vec<_>>();
        if targets.is_empty() {
            targets = hands
                .into_iter()
                .filter(|hand| hand.host.is_some())
                .map(|hand| (hand, None))
                .collect();
        }
        if targets.is_empty() {
            return Self::failed(&nobody());
        }
        let shared = Mutex::new(&mut *observer as &mut dyn CodeModeObserver);
        let waits = targets.into_iter().map(|(hand, id)| {
            let shared = &shared;
            let input = match id {
                Some(id) => rewrite(input, &id),
                None => input.to_owned(),
            };
            async move {
                let mut observer = HandObserver {
                    hand: &hand.name,
                    inner: shared,
                };
                let execution = match &hand.host {
                    Some(host) => {
                        match host.wait_with_updates(&input, context, &mut observer).await {
                            Ok(execution) => execution,
                            Err(error) => Self::failed(&error),
                        }
                    }
                    None => Self::failed(&disconnected(&hand.name)),
                };
                (hand.name, execution)
            }
        });
        let results = futures_util::future::join_all(waits).await;
        let mut found = Vec::new();
        let mut missing = None;
        for (name, execution) in results {
            if Self::missing_cell(&execution) {
                missing.get_or_insert((name, execution));
            } else {
                self.record(session_id, Some(&name), &execution);
                found.push((name, execution));
            }
        }
        if found.is_empty() {
            return missing.map_or_else(|| Self::failed(&nobody()), |(_, execution)| execution);
        }
        let origin = found
            .iter()
            .find_map(|(_, execution)| execution.cell.as_ref())
            .map_or_else(
                || context.call_id().to_owned(),
                |cell| cell.origin_call_id.clone(),
            );
        combine(&origin, found)
    }

    async fn execute_routed(
        &self,
        source: &str,
        context: ToolContext<'_>,
        tools: Vec<Arc<dyn Tool>>,
        observer: Option<&mut dyn CodeModeObserver>,
    ) -> Result<CodeModeExecution, CodeModeHostError> {
        Ok(match self.placement(context.session_id()) {
            Placement::Local => Self::failed(&unselected()),
            Placement::Hands { hands, broadcast } => {
                let hands = hands
                    .into_iter()
                    .map(|hand| (hand, tools.clone()))
                    .collect();
                self.execute_on(hands, broadcast, source, context, observer)
                    .await
            }
        })
    }

    async fn wait_routed(
        &self,
        input: &str,
        context: ToolContext<'_>,
        observer: &mut dyn CodeModeObserver,
    ) -> Result<CodeModeExecution, CodeModeHostError> {
        let session_id = context.session_id();
        if let Placement::Hands {
            hands,
            broadcast: true,
        } = self.placement(session_id)
        {
            return Ok(self.wait_on(hands, input, context, observer).await);
        }
        let places = self.wait_route(session_id);
        let remaining = places.len();
        let mut failure = unselected();
        let mut missing = None;
        for (index, place) in places.into_iter().enumerate() {
            let Some(name) = place else {
                continue;
            };
            let host = match self.connected(&name) {
                Ok(host) => host,
                Err(error) => {
                    failure = error;
                    continue;
                }
            };
            match host
                .wait_with_updates(input, context, &mut *observer as &mut dyn CodeModeObserver)
                .await
            {
                Ok(execution) if Self::missing_cell(&execution) && index + 1 < remaining => {
                    missing = Some(execution);
                }
                Ok(execution) => {
                    self.record(session_id, Some(&name), &execution);
                    return Ok(execution);
                }
                Err(error) => failure = error,
            }
        }
        Ok(missing.unwrap_or_else(|| Self::failed(&failure)))
    }
}

fn rewrite(input: &str, id: &str) -> String {
    match serde_json::from_str::<serde_json::Value>(input) {
        Ok(serde_json::Value::Object(mut fields)) => {
            fields.insert("cell_id".into(), serde_json::Value::String(id.into()));
            serde_json::Value::Object(fields).to_string()
        }
        _ => input.to_owned(),
    }
}

struct IgnoreUpdates;

impl CodeModeObserver for IgnoreUpdates {
    fn update(&mut self, _update: CodeModeUpdate<'_>) {}
}

impl CodeModeHost for CodeModeExecutors {
    fn tool_mode(&self) -> EmbeddedToolMode {
        EmbeddedToolMode::CodeOnly
    }

    fn supports_cells(&self) -> bool {
        true
    }

    fn tool_definitions(&self, session_id: &str) -> Result<Vec<ToolDefinition>, CodeModeHostError> {
        let Placement::Hands { hands, .. } = self.placement(session_id) else {
            return Ok(Vec::new());
        };
        let mut definitions = Vec::<ToolDefinition>::new();
        for host in hands.into_iter().filter_map(|hand| hand.host) {
            for definition in host.tool_definitions(session_id)? {
                if !definitions
                    .iter()
                    .any(|known| known.name() == definition.name())
                {
                    definitions.push(definition);
                }
            }
        }
        Ok(definitions)
    }

    fn execute<'a>(
        &'a self,
        source: &'a str,
        context: ToolContext<'a>,
    ) -> HostFuture<'a, Result<CodeModeExecution, CodeModeHostError>> {
        Box::pin(self.execute_routed(source, context, Vec::new(), None))
    }

    fn execute_with_updates<'a>(
        &'a self,
        source: &'a str,
        context: ToolContext<'a>,
        observer: &'a mut dyn CodeModeObserver,
    ) -> HostFuture<'a, Result<CodeModeExecution, CodeModeHostError>> {
        Box::pin(self.execute_routed(source, context, Vec::new(), Some(observer)))
    }

    fn execute_with_local_tools<'a>(
        &'a self,
        source: &'a str,
        context: ToolContext<'a>,
        tools: Vec<Arc<dyn Tool>>,
        observer: Option<&'a mut dyn CodeModeObserver>,
    ) -> HostFuture<'a, Result<CodeModeExecution, CodeModeHostError>> {
        Box::pin(self.execute_routed(source, context, tools, observer))
    }

    fn wait_with_updates<'a>(
        &'a self,
        input: &'a str,
        context: ToolContext<'a>,
        observer: &'a mut dyn CodeModeObserver,
    ) -> HostFuture<'a, Result<CodeModeExecution, CodeModeHostError>> {
        Box::pin(self.wait_routed(input, context, observer))
    }

    fn link_session(&self, session_id: &str, root_session_id: &str) {
        self.link(session_id, root_session_id);
    }

    fn begin_turn(&self, session_id: &str) {
        for host in self.touched(session_id) {
            host.begin_turn(session_id);
        }
    }

    fn detach_turn(
        &self,
        session_id: &str,
        observer: &mut (dyn FnMut(&str) -> Box<dyn CodeModeObserver> + Send),
    ) {
        for host in self.touched(session_id) {
            host.detach_turn(session_id, observer);
        }
    }

    fn preempt_turn<'a>(
        &'a self,
        session_id: &'a str,
    ) -> HostFuture<'a, Result<(), CodeModeHostError>> {
        Box::pin(async move {
            let mut outcome = Ok(());
            for host in self.touched(session_id) {
                let result = host.preempt_turn(session_id).await;
                outcome = outcome.and(result);
            }
            outcome
        })
    }

    fn settle_preempt(&self, session_id: &str) {
        for host in self.touched(session_id) {
            host.settle_preempt(session_id);
        }
    }

    fn cancel_turn<'a>(
        &'a self,
        session_id: &'a str,
    ) -> HostFuture<'a, Result<(), CodeModeHostError>> {
        Box::pin(async move {
            let mut outcome = Ok(());
            for host in self.touched(session_id) {
                let result = host.cancel_turn(session_id).await;
                outcome = outcome.and(result);
            }
            self.settle_cells(session_id);
            outcome
        })
    }

    fn cancel_turn_with_updates<'a>(
        &'a self,
        session_id: &'a str,
        observer: &'a mut dyn CodeModeObserver,
    ) -> HostFuture<'a, Result<(), CodeModeHostError>> {
        Box::pin(async move {
            let mut outcome = Ok(());
            for host in self.touched(session_id) {
                let result = host.cancel_turn_with_updates(session_id, observer).await;
                outcome = outcome.and(result);
            }
            self.settle_cells(session_id);
            outcome
        })
    }

    fn cancel_with_updates<'a>(
        &'a self,
        session_id: &'a str,
        observer: &'a mut dyn CodeModeObserver,
    ) -> HostFuture<'a, Result<(), CodeModeHostError>> {
        Box::pin(async move {
            let mut outcome = Ok(());
            for host in self.touched(session_id) {
                let result = host.cancel_with_updates(session_id, observer).await;
                outcome = outcome.and(result);
            }
            self.settle_cells(session_id);
            outcome
        })
    }

    fn cancel<'a>(&'a self, session_id: &'a str) -> HostFuture<'a, Result<(), CodeModeHostError>> {
        Box::pin(async move {
            let mut outcome = Ok(());
            for host in self.touched(session_id) {
                let result = host.cancel(session_id).await;
                outcome = outcome.and(result);
            }
            self.settle_cells(session_id);
            outcome
        })
    }
}
