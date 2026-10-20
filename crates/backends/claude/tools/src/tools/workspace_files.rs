use crate::tools::{SessionEnvironment, ToolContent, ToolOutput, media::MediaReadOptions};
use serde_json::Value;
use std::{
    fs,
    io::{BufRead as _, BufReader, Write as _},
    path::{Path, PathBuf},
};

#[derive(Clone, Debug)]
pub struct ClaudeWorkspaceFiles {
    root: PathBuf,
    base: PathBuf,
    relative_paths: bool,
    media: MediaReadOptions,
    read_token_budget: usize,
}

impl ClaudeWorkspaceFiles {
    pub fn new(root: impl AsRef<Path>) -> Result<Self, String> {
        let given = root.as_ref();
        let root = fs::canonicalize(given).map_err(|e| format!("workspace root: {e}"))?;
        if !root.is_dir() {
            return Err("workspace root is not a directory".into());
        }
        Ok(Self {
            base: root.clone(),
            relative_paths: true,
            root,
            media: MediaReadOptions::default(),
            read_token_budget: usize::MAX,
        })
    }

    pub fn machine(directory: impl AsRef<Path>) -> Result<Self, String> {
        let base = fs::canonicalize(directory.as_ref()).map_err(|e| format!("directory: {e}"))?;
        if !base.is_dir() {
            return Err("directory is not a directory".into());
        }
        let root = base
            .ancestors()
            .last()
            .ok_or("directory has no filesystem root")?
            .to_path_buf();
        Ok(Self {
            root,
            base,
            relative_paths: false,
            media: MediaReadOptions::default(),
            read_token_budget: usize::MAX,
        })
    }

    fn shown(&self, relative: &Path) -> String {
        if self.relative_paths {
            relative.display().to_string()
        } else {
            self.root.join(relative).display().to_string()
        }
    }

    #[must_use]
    pub fn with_media_options(mut self, media: MediaReadOptions) -> Self {
        self.media = media;
        self
    }

    #[must_use]
    pub const fn with_read_token_budget(mut self, tokens: usize) -> Self {
        self.read_token_budget = tokens;
        self
    }

    #[must_use]
    pub fn definitions() -> Vec<Value> {
        ["Read", "Edit", "Write"]
            .into_iter()
            .map(crate::upstream::tool_value)
            .collect()
    }

    #[must_use]
    pub fn tool_schemas() -> Vec<Value> {
        Self::definitions()
    }

    pub async fn execute(&self, name: &str, input: Value) -> Result<String, String> {
        match self.execute_output(name, input).await?.content {
            ToolContent::Text(text) => Ok(text),
            ToolContent::Blocks(blocks) => {
                let mut text = String::new();
                for block in blocks {
                    match block {
                        crate::tools::ToolResultBlock::Text { text: part } => text.push_str(&part),
                        _ => return Err("Read returned media; use execute_output to preserve native Claude content blocks".into()),
                    }
                }
                Ok(text)
            }
        }
    }

    pub async fn execute_output(&self, name: &str, input: Value) -> Result<ToolOutput, String> {
        self.execute_output_in_session(name, input, None).await
    }

    pub async fn execute_output_in_session(
        &self,
        name: &str,
        input: Value,
        session: Option<SessionEnvironment>,
    ) -> Result<ToolOutput, String> {
        let this = self.clone();
        let name = name.to_owned();
        tokio::task::spawn_blocking(move || {
            let mut media = None;
            if name == "Read" {
                let fields = input.as_object().ok_or("Read input must be an object")?;
                if let Some(key) = fields.keys().find(|k| {
                    !["file_path", "offset", "limit", "pages", "allow_large"].contains(&k.as_str())
                }) {
                    return Err(format!("unsupported Read option: {key}"));
                }
                if input.get("allow_large").is_some_and(|v| !v.is_boolean()) {
                    return Err("allow_large must be a boolean".into());
                }
                let (_, path) = this.file(Self::field(&input, "file_path")?)?;
                media = crate::tools::media::read(&path, &input, &this.media, session.as_ref())?;
            }
            Ok(match media {
                Some(output) => output,
                None => ToolOutput::text(this.execute_sync(&name, &input)?),
            })
        })
        .await
        .map_err(|e| format!("workspace task: {e}"))?
    }

    fn execute_sync(&self, name: &str, input: &Value) -> Result<String, String> {
        let allowed: &[&str] = match name {
            "Read" => &["file_path", "offset", "limit", "pages", "allow_large"],
            "Write" => &["file_path", "content"],
            "Edit" => &["file_path", "old_string", "new_string", "replace_all"],
            _ => return Err(format!("unknown workspace tool: {name}")),
        };
        let fields = input
            .as_object()
            .ok_or("workspace input must be an object")?;
        if let Some(key) = fields.keys().find(|key| !allowed.contains(&key.as_str())) {
            return Err(format!("unsupported {name} option: {key}"));
        }
        match name {
            "Read" => self.read(input),
            "Write" => self.write(input),
            "Edit" => self.edit(input),
            _ => Err(format!("unknown workspace tool: {name}")),
        }
    }

    fn field<'a>(input: &'a Value, key: &str) -> Result<&'a str, String> {
        input
            .get(key)
            .and_then(Value::as_str)
            .ok_or_else(|| format!("missing or invalid {key}"))
    }

    fn resolve(&self, text: &str) -> Result<PathBuf, String> {
        if text.is_empty() {
            return Err("file path is empty".into());
        }
        Ok(self.base.join(text))
    }

    fn relative_path(&self, path: &Path) -> PathBuf {
        path.strip_prefix(&self.root).unwrap_or(path).to_path_buf()
    }

    fn existing(&self, text: &str) -> Result<(PathBuf, PathBuf), String> {
        let real = fs::canonicalize(self.resolve(text)?).map_err(|e| format!("path: {e}"))?;
        Ok((self.relative_path(&real), real))
    }

    fn file(&self, text: &str) -> Result<(PathBuf, PathBuf), String> {
        let (rel, real) = self.existing(text)?;
        if !real.is_file() {
            return Err("path is not a regular file".into());
        }
        Ok((rel, real))
    }

    fn read_text(path: &Path) -> Result<String, String> {
        fs::read_to_string(path).map_err(|e| format!("read: {e}"))
    }

    fn read(&self, input: &Value) -> Result<String, String> {
        if input.get("pages").is_some() {
            return Err("pages is only applicable to PDF files".into());
        }
        let (_, path) = self.file(Self::field(input, "file_path")?)?;
        let output_limit = self.read_token_budget.saturating_mul(4);
        let offset = input
            .get("offset")
            .map_or(Some(1), Value::as_u64)
            .ok_or("invalid offset")?;
        let limit = input
            .get("limit")
            .map_or(Some(u64::MAX), Value::as_u64)
            .ok_or("invalid limit")?;
        if limit == 0 {
            return Err("limit must be positive".into());
        }
        let mut out = String::new();
        let file = fs::File::open(path).map_err(|e| format!("read: {e}"))?;
        for (i, line) in BufReader::new(file)
            .lines()
            .enumerate()
            .skip(offset.saturating_sub(1).min(usize::MAX as u64) as usize)
            .take(usize::try_from(limit).unwrap_or(usize::MAX))
        {
            let line = line.map_err(|e| format!("read: {e}"))?;
            let line = format!("{}\t{}\n", i + 1, line);
            if out.len().saturating_add(line.len()) > output_limit {
                return Err(
                    "requested text exceeds the available Read budget; use offset/limit".into(),
                );
            }
            out.push_str(&line);
        }
        Ok(out)
    }

    fn write_target(&self, text: &str) -> Result<(PathBuf, PathBuf), String> {
        let mut path = self.resolve(text)?;
        loop {
            match fs::canonicalize(&path) {
                Ok(real) => {
                    if !real.is_file() {
                        return Err("target is not a regular file".into());
                    }
                    return Ok((self.relative_path(&real), real));
                }
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                    match fs::symlink_metadata(&path) {
                        Ok(meta) if meta.file_type().is_symlink() => {
                            let target =
                                fs::read_link(&path).map_err(|e| format!("read link: {e}"))?;
                            path = path.parent().ok_or("file has no parent")?.join(target);
                            continue;
                        }
                        Ok(_) => {}
                        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                        Err(e) => return Err(format!("check target: {e}")),
                    }
                    let parent = path.parent().ok_or("file has no parent")?;
                    fs::create_dir_all(parent).map_err(|e| format!("create directory: {e}"))?;
                    let parent = fs::canonicalize(parent).map_err(|e| format!("parent: {e}"))?;
                    let path = parent.join(path.file_name().ok_or("file has no name")?);
                    return Ok((self.relative_path(&path), path));
                }
                Err(e) => return Err(format!("path: {e}")),
            }
        }
    }

    fn write(&self, input: &Value) -> Result<String, String> {
        let content = Self::field(input, "content")?;
        let (rel, path) = self.write_target(Self::field(input, "file_path")?)?;
        atomic_write(&path, content)?;
        Ok(format!("Wrote {}", self.shown(&rel)))
    }

    fn edit(&self, input: &Value) -> Result<String, String> {
        let text = Self::field(input, "file_path")?;
        let old = Self::field(input, "old_string")?;
        let new = Self::field(input, "new_string")?;
        if old.is_empty() {
            return Err("old_string must not be empty".into());
        }
        let replace_all = input
            .get("replace_all")
            .map_or(Some(false), Value::as_bool)
            .ok_or("invalid replace_all")?;
        let (rel, existing) = self.file(text)?;
        let content = Self::read_text(&existing)?;
        let count = content.matches(old).count();
        if count == 0 {
            return Err("old_string not found".into());
        }
        if count != 1 && !replace_all {
            return Err(format!("old_string occurs {count} times; set replace_all"));
        }

        let updated = if replace_all {
            content.replace(old, new)
        } else {
            content.replacen(old, new, 1)
        };
        let (_, path) = self.write_target(text)?;

        if Self::read_text(&path)? != content {
            return Err("file changed during edit".into());
        }
        atomic_write(&path, &updated)?;
        Ok(format!(
            "Edited {} ({count} replacement(s))",
            self.shown(&rel)
        ))
    }
}

fn atomic_write(path: &Path, content: &str) -> Result<(), String> {
    let parent = path.parent().ok_or("file has no parent")?;
    let existing = fs::metadata(path).ok();
    let mut builder = tempfile::Builder::new();
    #[cfg(unix)]
    if existing.is_none() {
        use std::os::unix::fs::PermissionsExt;
        builder.permissions(fs::Permissions::from_mode(0o666));
    }
    let mut file = builder
        .tempfile_in(parent)
        .map_err(|e| format!("create temp file: {e}"))?;
    file.write_all(content.as_bytes())
        .map_err(|e| format!("write: {e}"))?;
    flush_to_disk(file.as_file()).map_err(|e| format!("sync: {e}"))?;
    if let Some(meta) = existing {
        file.as_file()
            .set_permissions(meta.permissions())
            .map_err(|e| format!("permissions: {e}"))?;
    }
    file.persist(path).map_err(|e| format!("rename: {e}"))?;
    Ok(())
}

#[cfg(unix)]
fn flush_to_disk(file: &fs::File) -> std::io::Result<()> {
    nix::unistd::fsync(file).map_err(std::io::Error::from)
}

#[cfg(not(unix))]
fn flush_to_disk(file: &fs::File) -> std::io::Result<()> {
    file.sync_all()
}
