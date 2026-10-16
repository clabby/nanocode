use nanocode_agent_core::Result;
use std::sync::Arc;

#[cfg(not(target_family = "wasm"))]
pub(super) struct Cua {
    tools: tokio::sync::OnceCell<Option<Computer>>,
    unavailable: std::sync::OnceLock<String>,
    closed: tokio::sync::watch::Sender<bool>,
}

#[cfg(not(target_family = "wasm"))]
pub(super) type Cell = Option<Arc<Cua>>;

#[cfg(not(target_family = "wasm"))]
pub(super) type Computer = crate::ComputerTools;

#[cfg(not(target_family = "wasm"))]
pub(super) fn cell(explicit: Option<Computer>) -> Arc<Cua> {
    Arc::new(Cua {
        tools: match explicit {
            Some(computer) => tokio::sync::OnceCell::new_with(Some(Some(computer))),
            None => tokio::sync::OnceCell::new(),
        },
        unavailable: std::sync::OnceLock::new(),
        closed: tokio::sync::watch::Sender::new(false),
    })
}

#[cfg(not(target_family = "wasm"))]
pub(super) async fn connect(cell: Cell) -> Result<Option<Computer>> {
    use tracing::Instrument as _;

    let Some(cua) = cell else {
        return Ok(None);
    };
    let mut closed = cua.closed.subscribe();
    let connected = cua.tools.get_or_init(|| async {
        let connected = match crate::ComputerConfig::installed() {
            Ok(config) => {
                crate::ComputerTools::connect(config)
                    .instrument(tracing::info_span!("agent.host.computer.connect"))
                    .await
            }
            Err(error) => Err(error),
        };
        connected
            .map_err(|error| {
                let _ = cua.unavailable.set(error.to_string());
            })
            .ok()
    });
    tokio::select! {
        tools = connected => Ok(tools.clone()),
        _ = closed.wait_for(|closed| *closed) => Err(nanocode_agent_core::NanocodexError::AgentStopped),
    }
}

#[cfg(not(target_family = "wasm"))]
pub(super) fn close(cell: &Cell) {
    if let Some(cua) = cell {
        cua.closed.send_replace(true);
    }
}

#[cfg(not(target_family = "wasm"))]
pub(super) fn unavailable(cell: &Cell) -> Option<String> {
    cell.as_ref()?.unavailable.get().cloned()
}

#[cfg(not(target_family = "wasm"))]
pub(super) fn shared(computer: Option<&Computer>) -> Vec<Arc<dyn nanocode_tool_contracts::Tool>> {
    computer
        .map(|computer| {
            computer
                .tools()
                .map(|tool| Arc::new(tool) as Arc<dyn nanocode_tool_contracts::Tool>)
                .collect()
        })
        .unwrap_or_default()
}

#[cfg(target_family = "wasm")]
pub(super) type Cell = Option<Arc<Computer>>;

#[cfg(target_family = "wasm")]
pub(super) enum Computer {}

#[cfg(target_family = "wasm")]
pub(super) async fn connect(cell: Cell) -> Result<Option<Computer>> {
    Ok(cell.map(|computer| match *computer {}))
}

#[cfg(target_family = "wasm")]
pub(super) fn close(_cell: &Cell) {}

#[cfg(target_family = "wasm")]
pub(super) fn unavailable(cell: &Cell) -> Option<String> {
    cell.as_ref().map(|computer| match **computer {})
}

#[cfg(target_family = "wasm")]
pub(super) fn shared(computer: Option<&Computer>) -> Vec<Arc<dyn nanocode_tool_contracts::Tool>> {
    computer.map_or_else(Vec::new, |computer| match *computer {})
}
