use std::{collections::BTreeMap, path::PathBuf};

use serde::Serialize;

use crate::{
    AgentHome, Convention, Diagnostic, Scope,
    fs_util::{PathState, path_state, sorted_visible_entries},
    project::ProjectHome,
};

const MAX_SKILLS_PER_ROOT: usize = 512;

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct SkillRoot {
    pub path: PathBuf,
    pub scope: Scope,
    pub convention: Convention,
}

impl SkillRoot {
    pub(crate) const fn new(path: PathBuf, scope: Scope, convention: Convention) -> Self {
        Self {
            path,
            scope,
            convention,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct SkillEntry {
    pub name: String,

    pub dir: PathBuf,
    pub skill_md: PathBuf,

    pub canonical_dir: PathBuf,
    pub root: SkillRoot,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize)]
pub struct SkillSet {
    pub skills: Vec<SkillEntry>,

    pub shadowed: Vec<SkillEntry>,
    pub diagnostics: Vec<Diagnostic>,
}

impl SkillSet {
    pub fn get(&self, name: &str) -> Option<&SkillEntry> {
        self.skills.iter().find(|skill| skill.name == name)
    }
}

pub fn resolve_skills(roots: &[SkillRoot]) -> SkillSet {
    let mut set = SkillSet::default();
    let mut winners: BTreeMap<String, SkillEntry> = BTreeMap::new();
    let mut seen_dirs: BTreeMap<PathBuf, PathBuf> = BTreeMap::new();
    for root in roots {
        for entry in scan_root(root, &mut set.diagnostics) {
            if let Some(first) = seen_dirs.get(&entry.canonical_dir) {
                set.diagnostics.push(Diagnostic::info(
                    &entry.dir,
                    format!("same skill folder as {}; listed once", first.display()),
                ));
                continue;
            }
            seen_dirs.insert(entry.canonical_dir.clone(), entry.dir.clone());
            if let Some(winner) = winners.get(&entry.name) {
                set.diagnostics.push(Diagnostic::info(
                    &entry.dir,
                    format!("skill {} shadowed by {}", entry.name, winner.dir.display()),
                ));
                set.shadowed.push(entry);
            } else {
                winners.insert(entry.name.clone(), entry);
            }
        }
    }
    set.skills = winners.into_values().collect();
    set
}

pub(crate) fn scan_root(root: &SkillRoot, diagnostics: &mut Vec<Diagnostic>) -> Vec<SkillEntry> {
    match path_state(&root.path) {
        Ok(PathState::Dir { .. }) => {}
        Ok(PathState::Absent) => return Vec::new(),
        Ok(PathState::Dangling) => {
            diagnostics.push(Diagnostic::warning(
                &root.path,
                "dangling skills root ignored",
            ));
            return Vec::new();
        }
        Ok(_) => {
            diagnostics.push(Diagnostic::warning(
                &root.path,
                "skills root is not a directory",
            ));
            return Vec::new();
        }
        Err(error) => {
            diagnostics.push(Diagnostic::warning(
                &root.path,
                format!("unreadable: {error}"),
            ));
            return Vec::new();
        }
    }
    let entries = match sorted_visible_entries(&root.path, MAX_SKILLS_PER_ROOT) {
        Ok((entries, truncated)) => {
            if truncated {
                diagnostics.push(Diagnostic::warning(
                    &root.path,
                    format!("more than {MAX_SKILLS_PER_ROOT} entries; remainder ignored"),
                ));
            }
            entries
        }
        Err(error) => {
            diagnostics.push(Diagnostic::warning(
                &root.path,
                format!("unreadable: {error}"),
            ));
            return Vec::new();
        }
    };
    let mut skills = Vec::new();
    for dir in entries {
        let Some(name) = dir
            .file_name()
            .and_then(|name| name.to_str())
            .map(str::to_owned)
        else {
            diagnostics.push(Diagnostic::warning(&dir, "non-UTF-8 skill name ignored"));
            continue;
        };
        let canonical_dir = match path_state(&dir) {
            Ok(PathState::Dir { canonical }) => canonical,
            Ok(PathState::Dangling) => {
                diagnostics.push(Diagnostic::warning(&dir, "dangling skill symlink ignored"));
                continue;
            }
            Ok(_) => continue,
            Err(error) => {
                diagnostics.push(Diagnostic::warning(&dir, format!("unreadable: {error}")));
                continue;
            }
        };
        let skill_md = dir.join("SKILL.md");
        match path_state(&skill_md) {
            Ok(PathState::File { .. }) => skills.push(SkillEntry {
                name,
                dir,
                skill_md,
                canonical_dir,
                root: root.clone(),
            }),
            Ok(PathState::Dangling) => {
                diagnostics.push(Diagnostic::warning(&skill_md, "dangling SKILL.md ignored"));
            }
            _ => diagnostics.push(Diagnostic::info(&dir, "folder has no SKILL.md; ignored")),
        }
    }
    skills
}

pub(crate) fn user_skill_roots(home: &AgentHome) -> Vec<SkillRoot> {
    let mut roots = vec![
        SkillRoot::new(
            home.nanocode_home().join("skills"),
            Scope::User,
            Convention::Codex,
        ),
        SkillRoot::new(
            home.claude_home().join("skills"),
            Scope::User,
            Convention::Claude,
        ),
    ];
    if let Some(user) = home.user_home() {
        roots.push(SkillRoot::new(
            user.join(".agents").join("skills"),
            Scope::User,
            Convention::Codex,
        ));
    }
    roots
}

impl AgentHome {
    pub fn skill_roots(&self) -> Vec<SkillRoot> {
        user_skill_roots(self)
    }

    pub fn skills(&self) -> SkillSet {
        resolve_skills(&self.skill_roots())
    }

    pub fn skill_roots_for(&self, project: &ProjectHome) -> Vec<SkillRoot> {
        let mut roots = self.skill_roots();
        roots.extend(project.skill_roots());
        roots
    }

    pub fn skills_for(&self, project: &ProjectHome) -> SkillSet {
        resolve_skills(&self.skill_roots_for(project))
    }
}
