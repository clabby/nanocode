pub(crate) fn environment_section(workspace: &str) -> String {
    let mut lines = vec![
        "# Environment".to_owned(),
        "You have been invoked in the following environment: ".to_owned(),
    ];
    if !workspace.is_empty() {
        lines.push(format!("Primary working directory: {workspace}"));
        lines.push(format!(
            "Is a git repository: {}",
            is_git_repository(workspace)
        ));
    }
    lines.extend(host_lines());
    lines.push(format!("Today's date: {}", current_date()));
    lines.join("\n")
}

#[cfg(not(target_family = "wasm"))]
pub(crate) fn project_instructions(workspace: &str) -> Option<String> {
    if workspace.is_empty() {
        return None;
    }
    let global = nanocode_home::AgentHome::from_env()
        .ok()
        .map(|home| home.global_instructions());
    let project = nanocode_home::ProjectHome::discover(workspace).read_instructions();
    for diagnostic in global
        .iter()
        .flat_map(|global| &global.diagnostics)
        .chain(&project.diagnostics)
        .filter(|diagnostic| matches!(diagnostic.level, nanocode_home::Level::Warning))
    {
        tracing::warn!(
            path = %diagnostic.path.display(),
            message = %diagnostic.message,
            "instruction file skipped"
        );
    }
    let text = nanocode_home::combine_instructions(
        global.and_then(|global| global.combined()).as_deref(),
        project.combined().as_deref(),
    )?;
    Some(format!(
        "# AGENTS.md instructions for {workspace}\n\n<INSTRUCTIONS>\n{text}\n</INSTRUCTIONS>"
    ))
}

#[cfg(target_family = "wasm")]
pub(crate) const fn project_instructions(_workspace: &str) -> Option<String> {
    None
}

#[cfg(not(target_family = "wasm"))]
fn is_git_repository(workspace: &str) -> bool {
    std::path::Path::new(workspace)
        .ancestors()
        .any(|directory| directory.join(".git").exists())
}

#[cfg(target_family = "wasm")]
const fn is_git_repository(_workspace: &str) -> bool {
    false
}

#[cfg(not(target_family = "wasm"))]
fn host_lines() -> Vec<String> {
    let platform = match std::env::consts::OS {
        "macos" => "darwin",
        "windows" => "win32",
        other => other,
    };
    let mut lines = vec![format!("Platform: {platform}")];
    if let Some(shell) = std::env::var_os("SHELL")
        .map(std::path::PathBuf::from)
        .and_then(|shell| {
            shell
                .file_name()
                .map(|name| name.to_string_lossy().into_owned())
        })
    {
        lines.push(format!("Shell: {shell}"));
    }
    if let Some(version) = std::process::Command::new("uname")
        .arg("-sr")
        .output()
        .ok()
        .filter(|output| output.status.success())
        .and_then(|output| String::from_utf8(output.stdout).ok())
        .map(|version| version.trim().to_owned())
        .filter(|version| !version.is_empty())
    {
        lines.push(format!("OS Version: {version}"));
    }
    lines.extend(session_store_line());
    lines
}

#[cfg(not(target_family = "wasm"))]
fn session_store_line() -> Option<String> {
    let home = nanocode_home::AgentHome::from_env().ok()?;
    let store = home.nanocode_home();
    store.join("sessions.jsonl").is_file().then(|| {
        format!(
            "Nanocode sessions: {} (list them with `nc sessions --all`; print one's conversation with `nc sessions <ID>`)",
            store.display()
        )
    })
}

#[cfg(target_family = "wasm")]
const fn host_lines() -> Vec<String> {
    Vec::new()
}

#[cfg(not(target_family = "wasm"))]
fn current_date() -> String {
    chrono::Local::now().format("%Y-%m-%d").to_string()
}

#[cfg(target_family = "wasm")]
fn current_date() -> String {
    chrono::DateTime::<chrono::Utc>::from(nanocode_platform::system_time())
        .format("%Y-%m-%d")
        .to_string()
}
