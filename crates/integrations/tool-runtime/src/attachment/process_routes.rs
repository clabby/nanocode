use super::ProcessRouteStore;
use crate::contract::ToolError;
use serde::{Deserialize, Serialize};
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};
use tokio::sync::Mutex;

#[derive(Clone, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) struct SessionMapping {
    pub original: i64,
    pub namespace: Option<Box<str>>,
    pub runtime_id: Option<Box<str>>,
}

#[derive(Default, Serialize, Deserialize)]
struct RouteSession {
    next_id: i32,
    handles: HashMap<i32, SessionMapping>,
    #[serde(default)]
    mounts: HashSet<String>,
}

pub(crate) struct ProcessRoutes {
    sessions: Mutex<HashMap<String, RouteSession>>,
    store: Option<Arc<dyn ProcessRouteStore>>,
}

impl ProcessRoutes {
    pub fn new(store: Option<Arc<dyn ProcessRouteStore>>) -> Self {
        Self {
            sessions: Mutex::default(),
            store,
        }
    }

    async fn load(
        &self,
        session_id: &str,
        sessions: &mut HashMap<String, RouteSession>,
    ) -> Result<(), ToolError> {
        if !sessions.contains_key(session_id) {
            let state: RouteSession = if let Some(store) = &self.store {
                store
                    .load(session_id)
                    .await?
                    .map(serde_json::from_value)
                    .transpose()?
                    .unwrap_or_default()
            } else {
                RouteSession::default()
            };
            if state.next_id < 0
                || state
                    .mounts
                    .iter()
                    .any(|name| super::RemoteRunner::new(name).is_err())
                || state.handles.iter().any(|(id, mapping)| {
                    *id <= 0
                        || *id > state.next_id
                        || mapping.runtime_id.as_deref().is_none_or(str::is_empty)
                        || mapping.namespace.as_deref().is_some_and(str::is_empty)
                })
            {
                return Err("invalid persisted process route journal".into());
            }
            sessions.insert(session_id.to_owned(), state);
        }
        Ok(())
    }

    pub fn remember_mounts<'call>(
        &'call self,
        session_id: &'call str,
        mounts: &'call [String],
    ) -> futures_util::future::BoxFuture<'call, Result<(), ToolError>> {
        Box::pin(async move {
            let mut sessions = self.sessions.lock().await;
            self.load(session_id, &mut sessions).await?;
            let state = sessions
                .get_mut(session_id)
                .ok_or("process route session missing")?;
            let added = mounts
                .iter()
                .filter(|mount| !state.mounts.contains(*mount))
                .collect::<Vec<_>>();
            if !added.is_empty() {
                state
                    .mounts
                    .extend(added.iter().map(|mount| (*mount).clone()));
                if let Some(store) = &self.store {
                    if let Err(error) = store.save(session_id, serde_json::to_value(&*state)?).await
                    {
                        for mount in added {
                            state.mounts.remove(mount);
                        }
                        return Err(error);
                    }
                }
            }
            Ok(())
        })
    }

    pub fn known_mount<'call>(
        &'call self,
        session_id: &'call str,
        path: &'call std::path::Path,
    ) -> futures_util::future::BoxFuture<'call, Result<Option<String>, ToolError>> {
        Box::pin(async move {
            let mut sessions = self.sessions.lock().await;
            self.load(session_id, &mut sessions).await?;
            let state = sessions
                .get(session_id)
                .ok_or("process route session missing")?;
            Ok(state
                .mounts
                .iter()
                .map(String::as_str)
                .chain(
                    state
                        .handles
                        .values()
                        .filter_map(|mapping| mapping.namespace.as_deref()),
                )
                .find(|name| path.starts_with(format!("/{name}")))
                .map(str::to_owned))
        })
    }

    pub fn get<'call>(
        &'call self,
        session_id: &'call str,
        id: i32,
    ) -> futures_util::future::BoxFuture<'call, Result<SessionMapping, ToolError>> {
        Box::pin(async move {
            let mut sessions = self.sessions.lock().await;
            self.load(session_id, &mut sessions).await?;
            sessions
                .get(session_id)
                .and_then(|state| state.handles.get(&id))
                .cloned()
                .ok_or_else(|| "unknown logical process session".into())
        })
    }

    pub fn insert<'call>(
        &'call self,
        session_id: &'call str,
        mapping: SessionMapping,
    ) -> futures_util::future::BoxFuture<'call, Result<i32, ToolError>> {
        Box::pin(async move {
            let mut sessions = self.sessions.lock().await;
            self.load(session_id, &mut sessions).await?;
            let state = sessions
                .get_mut(session_id)
                .ok_or("process route session missing")?;
            if let Some((id, _)) = state
                .handles
                .iter()
                .find(|(_, existing)| **existing == mapping)
            {
                return Ok(*id);
            }
            let id = state
                .next_id
                .checked_add(1)
                .ok_or("process identifier space exhausted")?;
            state.handles.insert(id, mapping);
            state.next_id = id;
            if let Some(store) = &self.store {
                if let Err(error) = store.save(session_id, serde_json::to_value(&*state)?).await {
                    state.handles.remove(&id);
                    return Err(format!(
                        "process executed but its route could not be persisted: {error}"
                    )
                    .into());
                }
            }
            Ok(id)
        })
    }
}
