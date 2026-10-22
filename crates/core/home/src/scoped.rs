use crate::InstructionKind;

pub const PROJECT_INSTRUCTION_FILES: [(&str, InstructionKind); 2] = [
    ("AGENTS.override.md", InstructionKind::AgentsOverride),
    ("AGENTS.md", InstructionKind::Agents),
];

#[must_use]
pub fn instruction_scopes<'a>(paths: impl IntoIterator<Item = &'a str>) -> Vec<String> {
    let mut scopes = std::collections::BTreeSet::from([(0, String::new())]);
    for path in paths {
        let parts = path
            .split('/')
            .filter(|part| !part.is_empty() && *part != ".")
            .collect::<Vec<_>>();
        for depth in 1..=parts.len() {
            scopes.insert((depth, parts[..depth].join("/")));
        }
    }
    scopes
        .into_iter()
        .map(|(_, scope)| {
            if scope.is_empty() {
                ".".to_owned()
            } else {
                scope
            }
        })
        .collect()
}

#[must_use]
pub fn instruction_candidates(scope: &str) -> [(String, InstructionKind); 2] {
    PROJECT_INSTRUCTION_FILES.map(|(name, kind)| {
        let path = if scope.is_empty() || scope == "." {
            name.to_owned()
        } else {
            format!("{scope}/{name}")
        };
        (path, kind)
    })
}
