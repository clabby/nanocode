use std::{fs::OpenOptions, io::Write, path::Path};

pub fn load(path: &Path) -> Vec<String> {
    std::fs::read_to_string(path)
        .map(|contents| {
            contents
                .lines()
                .filter_map(|line| serde_json::from_str::<String>(line).ok())
                .collect()
        })
        .unwrap_or_default()
}

pub fn append(path: &Path, entry: &str) {
    if let Some(parent) = path.parent() {
        drop(std::fs::create_dir_all(parent));
    }
    let Ok(mut file) = OpenOptions::new().create(true).append(true).open(path) else {
        return;
    };
    if let Ok(line) = serde_json::to_string(entry) {
        drop(writeln!(file, "{line}"));
    }
}
