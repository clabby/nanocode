use std::{
    collections::{BTreeMap, HashMap},
    sync::{Arc, Mutex, MutexGuard, PoisonError},
};

use crate::{
    Tool, ToolContext, ToolDefinition, ToolOutputBody,
    embedded::{
        CodeModeExecution, CodeModeHost, CodeModeHostError, CodeModeObserver, EmbeddedToolMode,
        HostFuture,
    },
};

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ExecutorSelection {
    Local,
    Executor(Arc<str>),
}

pub type ExecutorRoute = Option<(Arc<str>, Arc<dyn CodeModeHost>)>;

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
    placements: HashMap<Box<str>, Placement>,
}

#[derive(Default)]
struct Placement {
    touched: Vec<Arc<str>>,
    cells: Vec<(Box<str>, Option<Arc<str>>)>,
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

    fn route(&self, session_id: &str) -> Result<ExecutorRoute, CodeModeHostError> {
        match self.resolve(session_id) {
            ExecutorSelection::Local => Ok(None),
            ExecutorSelection::Executor(name) => self
                .executors
                .get(&name)
                .map(|host| Some((Arc::clone(&name), Arc::clone(host))))
                .ok_or_else(|| disconnected(&name)),
        }
    }

    fn forget_executor(&mut self, name: &str) {
        for placement in self.placements.values_mut() {
            placement.touched.retain(|touched| &**touched != name);
            placement
                .cells
                .retain(|(_, place)| place.as_deref() != Some(name));
        }
    }
}

fn disconnected(name: &str) -> CodeModeHostError {
    CodeModeHostError::new(format!(
        "Code Mode executor `{name}` selected for this session is not connected; nothing was dispatched"
    ))
}

fn unselected() -> CodeModeHostError {
    CodeModeHostError::new(
        "no Code Mode executor is selected for this session; nothing was dispatched",
    )
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

    pub fn set_default(&self, name: Option<Arc<str>>) {
        self.state().default = name;
    }

    #[must_use]
    pub fn default_executor(&self) -> Option<Arc<str>> {
        self.state().default.clone()
    }

    pub fn clear_selections(&self, name: &str) {
        let mut state = self.state();
        state.selections.retain(|_, selection| {
            !matches!(selection, ExecutorSelection::Executor(selected) if &**selected == name)
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
        state.placements.remove(session_id);
    }

    pub fn route(&self, session_id: &str) -> Result<ExecutorRoute, CodeModeHostError> {
        self.state().route(session_id)
    }

    pub fn record(
        &self,
        session_id: &str,
        executor: Option<&Arc<str>>,
        execution: &CodeModeExecution,
    ) {
        let mut state = self.state();
        let placement = state.placements.entry(session_id.into()).or_default();
        if let Some(name) = executor
            && !placement.touched.contains(name)
        {
            placement.touched.push(Arc::clone(name));
        }
        if let Some(cell) = &execution.cell {
            placement
                .cells
                .retain(|(origin, _)| **origin != *cell.origin_call_id);
            if cell.running {
                placement
                    .cells
                    .push((cell.origin_call_id.as_str().into(), executor.cloned()));
            }
        }
    }

    #[must_use]
    pub fn wait_route(&self, session_id: &str) -> Vec<Option<Arc<str>>> {
        let state = self.state();
        let mut places = Vec::new();
        if let Some(placement) = state.placements.get(session_id) {
            for (_, place) in placement.cells.iter().rev() {
                if !places.contains(place) {
                    places.push(place.clone());
                }
            }
        }
        let selected = match state.resolve(session_id) {
            ExecutorSelection::Local => None,
            ExecutorSelection::Executor(name) => Some(name),
        };
        if !places.contains(&selected) {
            places.push(selected);
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
            .placements
            .get(session_id)
            .map(|placement| {
                placement
                    .touched
                    .iter()
                    .filter_map(|name| state.executors.get(name).cloned())
                    .collect()
            })
            .unwrap_or_default()
    }

    pub fn settle_cells(&self, session_id: &str) {
        if let Some(placement) = self.state().placements.get_mut(session_id) {
            placement.cells.clear();
        }
    }

    async fn execute_routed(
        &self,
        source: &str,
        context: ToolContext<'_>,
        tools: Vec<Arc<dyn Tool>>,
        observer: Option<&mut dyn CodeModeObserver>,
    ) -> Result<CodeModeExecution, CodeModeHostError> {
        let session_id = context.session_id();
        let (name, host) = match self.route(session_id) {
            Ok(Some(route)) => route,
            Ok(None) => return Ok(Self::failed(&unselected())),
            Err(error) => return Ok(Self::failed(&error)),
        };
        let observer = observer.map(|observer| &mut *observer as &mut dyn CodeModeObserver);
        match host
            .execute_with_local_tools(source, context, tools, observer)
            .await
        {
            Ok(execution) => {
                self.record(session_id, Some(&name), &execution);
                Ok(execution)
            }
            Err(error) => Ok(Self::failed(&error)),
        }
    }

    async fn wait_routed(
        &self,
        input: &str,
        context: ToolContext<'_>,
        observer: &mut dyn CodeModeObserver,
    ) -> Result<CodeModeExecution, CodeModeHostError> {
        let session_id = context.session_id();
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

impl CodeModeHost for CodeModeExecutors {
    fn tool_mode(&self) -> EmbeddedToolMode {
        EmbeddedToolMode::CodeOnly
    }

    fn supports_cells(&self) -> bool {
        true
    }

    fn tool_definitions(&self, session_id: &str) -> Result<Vec<ToolDefinition>, CodeModeHostError> {
        match self.route(session_id) {
            Ok(Some((_, host))) => host.tool_definitions(session_id),
            Ok(None) | Err(_) => Ok(Vec::new()),
        }
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
