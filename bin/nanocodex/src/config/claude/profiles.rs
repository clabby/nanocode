//! Host-owned immutable profile bindings, inherited by every descendant.
use super::super::*;
use nanocodex::claude::{
    ClaudeHookFuture, ClaudeToolDecision, ClaudeToolHooks, ClaudeToolInvocation,
};
use nanocodex::claude_tools::AgentProfile;
use std::collections::BTreeMap;
use std::sync::{Mutex, OnceLock};

#[derive(Clone, Default)]
pub(in crate::config::claude) struct Admission {
    pub profile: Option<AgentProfile>,
    pub isolation: bool,
}
tokio::task_local! { static ADMISSION: Admission; }
struct Binding {
    profiles: Vec<AgentProfile>,
    workspace: Arc<worktree::Workspace>,
    parent: Option<String>,
}
static BINDINGS: OnceLock<Mutex<BTreeMap<String, Binding>>> = OnceLock::new();
fn bindings() -> &'static Mutex<BTreeMap<String, Binding>> {
    BINDINGS.get_or_init(Mutex::default)
}
pub(in crate::config::claude) async fn scope<F: std::future::Future>(
    admission: Admission,
    future: F,
) -> F::Output {
    ADMISSION.scope(admission, future).await
}
fn validate_profiles(profiles: &[AgentProfile]) -> std::result::Result<(), String> {
    for profile in profiles {
        permissions::Policy {
            allow: profile.tools.clone().unwrap_or_default(),
            deny: profile.disallowed_tools.clone(),
            ..Default::default()
        }
        .validate()
        .map_err(|e| format!("invalid agent profile {}: {e}", profile.name))?;
    }
    Ok(())
}
pub(in crate::config::claude) fn restore(
    session: &str,
    workspace: Arc<worktree::Workspace>,
) -> std::result::Result<(), String> {
    let profiles = workspace.profiles().unwrap_or_default();
    validate_profiles(&profiles)?;
    let mut bindings = bindings().lock().map_err(|_| "profile bindings poisoned")?;
    bindings.entry(session.into()).or_insert_with(|| Binding {
        profiles,
        workspace,
        parent: None,
    });
    Ok(())
}
/// Invoked before the child's tools, instructions or first model request exist.
pub(in crate::config::claude) fn bind(
    parent: &str,
    child: &str,
    workspace: Arc<worktree::Workspace>,
) -> std::result::Result<(), String> {
    let request = ADMISSION.try_with(Clone::clone).unwrap_or_default();
    let mut bindings = bindings().lock().map_err(|_| "profile bindings poisoned")?;
    if bindings.contains_key(child) {
        return Ok(());
    }
    let profiles = if let Some(saved) = workspace.profiles() {
        validate_profiles(&saved)?;
        saved
    } else {
        let mut inherited = bindings
            .get(parent)
            .map(|b| b.profiles.clone())
            .unwrap_or_default();
        if let Some(profile) = request.profile {
            inherited.push(profile);
        }
        if inherited.len() > 32 {
            return Err("profile delegation depth exceeds 32".into());
        }
        validate_profiles(&inherited)?;
        workspace.bind_profiles(inherited.clone())?;
        if request.isolation {
            workspace.isolate_child(child)?;
        }
        inherited
    };
    bindings.insert(
        child.into(),
        Binding {
            profiles,
            workspace,
            parent: Some(parent.into()),
        },
    );
    Ok(())
}
/// The registry must already have confirmed closure and management authority.
fn closed(session: &str) -> Value {
    let Ok(mut bindings) = bindings().lock() else {
        return json!({"cleanup_error":"profile bindings poisoned"});
    };
    let mut subtree = vec![session.to_owned()];
    let mut cursor = 0;
    while cursor < subtree.len() {
        let children: Vec<_> = bindings
            .iter()
            .filter(|(_, b)| b.parent.as_deref() == Some(&subtree[cursor]))
            .map(|(s, _)| s.clone())
            .collect();
        subtree.extend(children);
        cursor += 1;
    }
    // Release descendant parent pins before checking any owned tree.
    for session in &subtree {
        if let Some(b) = bindings.get(session) {
            b.workspace.release_parent();
        }
    }
    let mut receipts = Vec::new();
    for session in subtree.iter().rev() {
        if let Some(b) = bindings.remove(session) {
            let receipt = b.workspace.finish_child(session);
            if !receipt.is_null() {
                receipts.push(receipt);
            }
        }
    }
    json!(receipts)
}
pub(in crate::config::claude) fn instructions(session: &str, base: String) -> String {
    let bindings = bindings().lock().expect("profile bindings poisoned");
    match bindings.get(session).and_then(|b| b.profiles.last()) {
        Some(profile) => format!(
            "{base}\n\nHost-selected subagent profile (project guidance; inherited permissions remain mandatory):\n{}",
            serde_json::json!({"name":profile.name,"source":profile.path,"instructions":profile.instructions})
        ),
        None => base,
    }
}
pub(in crate::config::claude) fn selected_name(session: &str) -> Option<String> {
    bindings().lock().ok().and_then(|bindings| {
        bindings
            .get(session)
            .and_then(|b| b.profiles.last().map(|p| p.name.clone()))
    })
}
pub(in crate::config::claude) fn allows_context(session: &str) -> bool {
    bindings()
        .lock()
        .map(|bindings| {
            bindings.get(session).is_none_or(|b| {
                b.profiles.iter().all(|p| {
                    !p.disallowed_tools.iter().any(|t| t == "Read")
                        && p.tools
                            .as_ref()
                            .is_none_or(|tools| tools.iter().any(|t| t == "Read"))
                })
            })
        })
        .unwrap_or(false)
}
pub(super) fn required_model(session: &str) -> Option<String> {
    bindings().lock().ok().and_then(|bindings| {
        bindings
            .get(session)
            .and_then(|b| b.profiles.iter().rev().find_map(|p| p.model.clone()))
    })
}
pub(super) fn model(value: &str) -> &str {
    match value {
        "sonnet" => "claude-sonnet-5-5",
        "opus" => "claude-opus-5-5",
        "fable" => "claude-fable-5-1",
        "mythos" => "claude-mythos-5-1",
        "haiku" => "claude-haiku-5-5",
        other => other,
    }
}
pub(super) fn check_isolation(session: &str, workspace: &Path) -> std::result::Result<(), String> {
    let bindings = bindings().lock().map_err(|_| "profile bindings poisoned")?;
    if let Some(binding) = bindings.get(session) {
        for profile in &binding.profiles {
            if profile
                .tools
                .as_ref()
                .is_some_and(|tools| !tools.iter().any(|tool| tool == "EnterWorktree"))
                || profile
                    .disallowed_tools
                    .iter()
                    .any(|tool| tool == "EnterWorktree")
            {
                return Err(format!(
                    "worktree creation is unavailable under inherited agent profile {}",
                    profile.name
                ));
            }
            if let Some(mode) = &profile.permission_mode {
                let policy = permissions::Policy {
                    mode: Some(mode.clone()),
                    ..Default::default()
                };
                if !matches!(
                    policy
                        .evaluate("EnterWorktree", &json!({}), workspace)
                        .map_err(|e| e.to_string())?,
                    permissions::Decision::Allow
                ) {
                    return Err("inherited profile mode refuses worktree creation".into());
                }
            }
        }
    }
    Ok(())
}
/// Shared spawn callbacks and Skill's private spawn must enforce the same chain.
pub(super) fn check_spawn(session: &str, input: &Value) -> std::result::Result<(), String> {
    let bindings = bindings().lock().map_err(|_| "profile bindings poisoned")?;
    if let Some(binding) = bindings.get(session) {
        for profile in &binding.profiles {
            if profile
                .tools
                .as_ref()
                .is_some_and(|tools| !tools.iter().any(|t| t == "spawn_agent"))
                || profile.disallowed_tools.iter().any(|t| t == "spawn_agent")
            {
                return Err(format!(
                    "spawn_agent is unavailable under inherited agent profile {}",
                    profile.name
                ));
            }
            if let Some(mode) = &profile.permission_mode {
                let policy = permissions::Policy {
                    mode: Some(mode.clone()),
                    ..Default::default()
                };
                if !matches!(
                    policy
                        .evaluate("spawn_agent", input, &binding.workspace.current())
                        .map_err(|e| e.to_string())?,
                    permissions::Decision::Allow
                ) {
                    return Err("inherited profile mode refuses spawn_agent".into());
                }
            }
            if input
                .get("harness")
                .and_then(Value::as_str)
                .is_some_and(|h| h != "claude")
                || input
                    .get("model")
                    .and_then(Value::as_str)
                    .is_some_and(|m| !model(m).starts_with("claude-"))
            {
                return Err("inherited agent profiles require a Claude child; cross-family delegation is unavailable".into());
            }
            if let Some(required) = &profile.model
                && input
                    .get("model")
                    .and_then(Value::as_str)
                    .is_some_and(|m| model(m) != model(required))
            {
                return Err("model override conflicts with an inherited agent profile".into());
            }
        }
    }
    Ok(())
}
pub(in crate::config::claude) struct Guard {
    pub workspaces: Arc<WorkspaceRegistry>,
    pub registry: Option<std::sync::Weak<nanocodex_subagents::Registry>>,
}
impl ClaudeToolHooks for Guard {
    fn before<'a>(
        &'a self,
        name: &'a str,
        input: &'a Value,
        invocation: &'a ClaudeToolInvocation,
    ) -> ClaudeHookFuture<'a, std::result::Result<ClaudeToolDecision, String>> {
        Box::pin(async move {
            let chain = bindings()
                .lock()
                .map_err(|_| "profile bindings poisoned")?
                .get(&invocation.session_id)
                .map(|b| b.profiles.clone())
                .unwrap_or_default();
            let workspace = self.workspaces.current(&invocation.session_id)?;
            if name == "Workflow" && !chain.is_empty() {
                return Ok(ClaudeToolDecision::Deny("Workflow is unavailable under agent profiles; use spawn_agent so inherited restrictions remain enforced".into()));
            }
            if matches!(name, "Skill" | "ProjectContext") && !allows_context(&invocation.session_id)
            {
                return Ok(ClaudeToolDecision::Deny(
                    "project discovery is unavailable when an inherited profile restricts Read"
                        .into(),
                ));
            }
            if name == "spawn_agent" {
                check_spawn(&invocation.session_id, input)?;
            }
            for profile in &chain {
                if name != "submit_result"
                    && (profile
                        .tools
                        .as_ref()
                        .is_some_and(|tools| !tools.iter().any(|tool| tool == name))
                        || profile.disallowed_tools.iter().any(|tool| tool == name))
                {
                    return Ok(ClaudeToolDecision::Deny(format!(
                        "{name} is unavailable under inherited agent profile {}",
                        profile.name
                    )));
                }
                if let Some(mode) = &profile.permission_mode {
                    let policy = permissions::Policy {
                        mode: Some(mode.clone()),
                        ..Default::default()
                    };
                    if name != "submit_result"
                        && !matches!(
                            policy
                                .evaluate(name, input, &workspace)
                                .map_err(|e| e.to_string())?,
                            permissions::Decision::Allow
                        )
                    {
                        return Ok(ClaudeToolDecision::Deny(format!(
                            "{} profile permissionMode {mode} refuses {name}",
                            profile.name
                        )));
                    }
                }
            }
            Ok(ClaudeToolDecision::Allow)
        })
    }
    fn after<'a>(
        &'a self,
        name: &'a str,
        input: &'a Value,
        invocation: &'a ClaudeToolInvocation,
        reply: &'a nanocodex::claude::ClaudeToolReply,
    ) -> ClaudeHookFuture<'a, std::result::Result<(), String>> {
        Box::pin(async move {
            if name == "close_agent"
                && !reply.is_error
                && let Some(registry) = self.registry.as_ref().and_then(std::sync::Weak::upgrade)
            {
                let id =
                    serde_json::from_value(input["agent_id"].clone()).map_err(|e| e.to_string())?;
                let session = registry
                    .child_session_id(&invocation.session_id, id)
                    .await
                    .map_err(|e| e.to_string())?;
                closed(&session);
            }
            Ok(())
        })
    }
}
