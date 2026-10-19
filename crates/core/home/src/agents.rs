use std::{collections::BTreeMap, path::PathBuf};

use serde::Serialize;

use crate::{
    AgentHome, Convention, Diagnostic, Scope,
    fs_util::{PathState, path_state, sorted_visible_entries},
    project::ProjectHome,
};

const MAX_PROFILE_ENTRIES: usize = 256;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ProfileFormat {
    ClaudeMarkdown,

    CodexToml,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct AgentProfileRoot {
    pub path: PathBuf,
    pub scope: Scope,
    pub convention: Convention,
    pub format: ProfileFormat,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct AgentProfileFile {
    pub name: String,
    pub path: PathBuf,
    pub canonical_path: PathBuf,
    pub root: AgentProfileRoot,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize)]
pub struct AgentProfileSet {
    pub profiles: Vec<AgentProfileFile>,
    pub shadowed: Vec<AgentProfileFile>,
    pub diagnostics: Vec<Diagnostic>,
}

pub fn resolve_agent_profiles(roots: &[AgentProfileRoot]) -> AgentProfileSet {
    let mut set = AgentProfileSet::default();
    let mut winners: BTreeMap<(String, ProfileFormat), AgentProfileFile> = BTreeMap::new();
    let mut seen: BTreeMap<PathBuf, PathBuf> = BTreeMap::new();
    for root in roots {
        let (extension, max_depth) = match root.format {
            ProfileFormat::ClaudeMarkdown => ("md", 4),
            ProfileFormat::CodexToml => ("toml", 0),
        };
        let mut files = Vec::new();
        let mut pending = vec![(root.path.clone(), 0usize)];
        while let Some((dir, depth)) = pending.pop() {
            match path_state(&dir) {
                Ok(PathState::Dir { .. }) => {}
                Ok(PathState::Absent) => continue,
                Ok(PathState::Dangling) => {
                    set.diagnostics
                        .push(Diagnostic::warning(&dir, "dangling symlink ignored"));
                    continue;
                }
                Ok(_) => continue,
                Err(error) => {
                    set.diagnostics
                        .push(Diagnostic::warning(&dir, format!("unreadable: {error}")));
                    continue;
                }
            }
            let entries = match sorted_visible_entries(&dir, MAX_PROFILE_ENTRIES) {
                Ok((entries, truncated)) => {
                    if truncated {
                        set.diagnostics
                            .push(Diagnostic::warning(&dir, "profile discovery truncated"));
                    }
                    entries
                }
                Err(error) => {
                    set.diagnostics
                        .push(Diagnostic::warning(&dir, format!("unreadable: {error}")));
                    continue;
                }
            };
            for entry in entries {
                match path_state(&entry) {
                    Ok(PathState::Dir { .. }) if depth < max_depth => {
                        pending.push((entry, depth + 1));
                    }
                    Ok(PathState::File { canonical })
                        if entry.extension().is_some_and(|e| e == extension) =>
                    {
                        files.push((entry, canonical));
                    }
                    Ok(PathState::Dangling) => set
                        .diagnostics
                        .push(Diagnostic::warning(&entry, "dangling symlink ignored")),
                    _ => {}
                }
            }
        }
        files.sort();
        for (path, canonical_path) in files {
            let Some(name) = path.file_stem().and_then(|s| s.to_str()).map(str::to_owned) else {
                continue;
            };
            if let Some(first) = seen.get(&canonical_path) {
                set.diagnostics.push(Diagnostic::info(
                    &path,
                    format!("same file as {}; listed once", first.display()),
                ));
                continue;
            }
            seen.insert(canonical_path.clone(), path.clone());
            let profile = AgentProfileFile {
                name,
                path,
                canonical_path,
                root: root.clone(),
            };
            let key = (profile.name.clone(), root.format);
            if let Some(winner) = winners.get(&key) {
                set.diagnostics.push(Diagnostic::info(
                    &profile.path,
                    format!(
                        "profile {} shadowed by {}",
                        profile.name,
                        winner.path.display()
                    ),
                ));
                set.shadowed.push(profile);
            } else {
                winners.insert(key, profile);
            }
        }
    }
    set.profiles = winners.into_values().collect();
    set.profiles
        .sort_by(|a, b| (&a.name, a.root.format, &a.path).cmp(&(&b.name, b.root.format, &b.path)));
    set
}

impl AgentHome {
    pub fn agent_profile_roots(&self) -> Vec<AgentProfileRoot> {
        vec![
            AgentProfileRoot {
                path: self.claude_home().join("agents"),
                scope: Scope::User,
                convention: Convention::Claude,
                format: ProfileFormat::ClaudeMarkdown,
            },
            AgentProfileRoot {
                path: self.nanocode_home().join("agents"),
                scope: Scope::User,
                convention: Convention::Codex,
                format: ProfileFormat::CodexToml,
            },
        ]
    }

    pub fn agent_profile_roots_for(&self, project: &ProjectHome) -> Vec<AgentProfileRoot> {
        let mut roots = project.agent_profile_roots();
        roots.extend(self.agent_profile_roots());
        roots
    }

    pub fn agent_profiles(&self) -> AgentProfileSet {
        resolve_agent_profiles(&self.agent_profile_roots())
    }

    pub fn agent_profiles_for(&self, project: &ProjectHome) -> AgentProfileSet {
        resolve_agent_profiles(&self.agent_profile_roots_for(project))
    }
}
