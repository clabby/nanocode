use crate::tools::{SessionEnvironment, ToolContent, ToolOutput, media::MediaReadOptions};
use serde_json::Value;
use std::{
    fs,
    io::{BufRead as _, BufReader, Write as _},
    path::{Path, PathBuf},
};

const MAX_READ_BYTES: u64 = 262_144;
const MAX_READ_TOKENS: usize = 25_000;
const BYTES_PER_TOKEN: usize = 4;
const SELECTED_BYTES_PER_TOKEN: u64 = 128;
const ALLOW_LARGE_BYTES_PER_TOKEN: u64 = 3;
const TRUNCATED: &str = "[Truncated: PARTIAL view \u{2014} ";
const FITS_WITH_ALLOW_LARGE: &str = " To read it anyway, up to what still fits in your context, call Read again with allow_large: true \u{2014} only if you genuinely need all of it or the user asked for the whole file.";
const TOO_LARGE_FOR_ROOM: &str = " Even with allow_large, this is more than fits in the context you have left this turn (other large reads in the same turn share that room).";
const ALLOW_LARGE_UNAVAILABLE: &str =
    " allow_large is not available where this read runs; read it in parts with offset and limit.";

#[derive(Clone, Copy)]
struct ReadLimits {
    bytes: u64,
    tokens: usize,
}

fn size_text(bytes: u64) -> String {
    let trimmed = |value: f64| format!("{value:.1}").trim_end_matches(".0").to_owned();
    let kilobytes = bytes as f64 / 1024.0;
    if kilobytes < 1.0 {
        return format!("{bytes} bytes");
    }
    if kilobytes < 1024.0 {
        return format!("{}KB", trimmed(kilobytes));
    }
    let megabytes = kilobytes / 1024.0;
    if megabytes < 1024.0 {
        return format!("{}MB", trimmed(megabytes));
    }
    format!("{}GB", trimmed(megabytes / 1024.0))
}

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
        let file_path = Self::field(input, "file_path")?;
        let (_, path) = self.file(file_path)?;
        let offset = input
            .get("offset")
            .map_or(Some(1), Value::as_u64)
            .ok_or("invalid offset")?;
        let limit = input
            .get("limit")
            .map(|value| value.as_u64().ok_or("invalid limit"))
            .transpose()?;
        if limit == Some(0) {
            return Err("limit must be positive".into());
        }
        let opted = input
            .get("allow_large")
            .and_then(Value::as_bool)
            .unwrap_or(false);
        let defaults = ReadLimits {
            bytes: MAX_READ_BYTES,
            tokens: MAX_READ_TOKENS,
        };
        let room = (self.read_token_budget != usize::MAX).then_some(self.read_token_budget);
        let raised = room
            .filter(|room| *room > MAX_READ_TOKENS)
            .map(|room| ReadLimits {
                bytes: (room as u64)
                    .saturating_mul(BYTES_PER_TOKEN as u64 * ALLOW_LARGE_BYTES_PER_TOKEN)
                    .max(MAX_READ_BYTES),
                tokens: room,
            });
        let limits = raised.filter(|_| opted).unwrap_or(defaults);
        let hint = |bytes: Option<u64>, tokens: Option<usize>| -> &'static str {
            let Some(_) = room else {
                return if opted { ALLOW_LARGE_UNAVAILABLE } else { "" };
            };
            let Some(raised) = raised.filter(|_| !opted) else {
                return TOO_LARGE_FOR_ROOM;
            };
            if bytes.is_none_or(|bytes| bytes <= raised.bytes)
                && tokens.is_none_or(|tokens| tokens <= raised.tokens)
            {
                FITS_WITH_ALLOW_LARGE
            } else {
                TOO_LARGE_FOR_ROOM
            }
        };
        let size = fs::metadata(&path).map_err(|e| format!("read: {e}"))?.len();
        if limit.is_none() && size > limits.bytes {
            return Err(format!(
                "File content ({}) exceeds maximum allowed size ({}). Use offset and limit parameters to read specific portions of the file, or search for specific content instead of reading the whole file.{}",
                size_text(size),
                size_text(limits.bytes),
                hint(Some(size), None)
            ));
        }
        let max_selected = (MAX_READ_TOKENS as u64 * SELECTED_BYTES_PER_TOKEN)
            .max(raised.filter(|_| opted).map_or(0, |raised| raised.bytes));
        let start = offset.saturating_sub(1);
        let end = limit.map(|limit| start.saturating_add(limit));
        let mut lines = Vec::new();
        let mut selected = 0_u64;
        let mut total = 0_u64;
        let file = fs::File::open(path).map_err(|e| format!("read: {e}"))?;
        for line in BufReader::new(file).lines() {
            let mut line = line.map_err(|e| format!("read: {e}"))?;
            if total == 0 && line.starts_with('\u{feff}') {
                line.drain(..'\u{feff}'.len_utf8());
            }
            if total >= start && end.is_none_or(|end| total < end) {
                selected += line.len() as u64 + u64::from(!lines.is_empty());
                if limit.is_some() && selected > max_selected {
                    return Err(format!(
                        "The requested line range contains over {} of text, more than a read can return. Use a smaller limit \u{2014} or, if a single line is this large, no limit will fit it: search for specific content instead.{}",
                        size_text(max_selected),
                        hint(Some(selected), None)
                    ));
                }
                lines.push(line);
            }
            total += 1;
        }
        let first = offset.max(1);
        let content = lines.join("\n");
        let tokens = content.len().div_ceil(BYTES_PER_TOKEN);
        let mut banner = None;
        let mut shown = lines.len();
        let mut text = content;
        if tokens > limits.tokens {
            if offset > 1 || limit.is_some() {
                return Err(format!(
                    "File content ({tokens} tokens) exceeds maximum allowed tokens ({}). Use offset and limit parameters to read specific portions of the file, or search for specific content instead of reading the whole file.{}",
                    limits.tokens,
                    hint(None, Some(tokens))
                ));
            }
            let cap = MAX_READ_TOKENS;
            let reason = if opted && limits.tokens > cap {
                format!(
                    "allow_large had room for {} tokens this turn",
                    limits.tokens
                )
            } else {
                format!("cap {cap}")
            };
            let per_token = (text.len() as f64 / tokens.max(1) as f64).max(0.5);
            let estimate = |text: &str| text.len() as f64 / per_token;
            let mut count =
                ((lines.len() as f64 * cap as f64 / tokens.max(1) as f64 * 0.85).floor() as usize)
                    .clamp(1, lines.len().max(1));
            let mut page = lines[..count.min(lines.len())].join("\n");
            for _ in 0..6 {
                if estimate(&page) <= cap as f64 || count <= 1 {
                    break;
                }
                count = ((count as f64 * 0.7).floor() as usize).max(1);
                page = lines[..count].join("\n");
            }
            let hint = hint(None, Some(tokens));
            if estimate(&page) > cap as f64 || page.trim().is_empty() {
                let mut chars = ((cap as f64 * per_token * 0.85).floor() as usize).max(1);
                for _ in 0..6 {
                    page = text[..text.floor_char_boundary(chars)].to_owned();
                    if estimate(&page) <= cap as f64 {
                        break;
                    }
                    chars = ((chars as f64 * 0.7).floor() as usize).max(1);
                }
                banner = Some(format!(
                    "{TRUNCATED}{file_path}: showing the first {} of {} characters ({tokens} tokens, {reason}); this file has very long lines and cannot be paginated by line. Use Grep to find a specific section, or Read with offset/limit to page through it.{hint} Do NOT answer from this excerpt alone if the answer may be elsewhere in the file.]",
                    page.chars().count(),
                    text.chars().count()
                ));
                shown = page.split('\n').count();
            } else {
                shown = count;
                if (count as u64) < total {
                    banner = Some(format!(
                        "{TRUNCATED}{file_path}: showing lines 1-{count} of {total} total ({tokens} tokens, {reason}). Call Read with offset={} limit={count} for the next page, or Grep to find a specific section.{hint} Do NOT answer from this page alone if the answer may be further in the file.]",
                        count + 1
                    ));
                }
            }
            text = page;
        }
        let mut out = if !text.is_empty() || (shown >= 1 && total > 1) {
            text.split('\n')
                .enumerate()
                .map(|(index, line)| format!("{}\t{line}", first + index as u64))
                .collect::<Vec<_>>()
                .join("\n")
        } else if shown >= 1 || total == 0 {
            "<system-reminder>Warning: the file exists but the contents are empty.</system-reminder>"
                .to_owned()
        } else {
            format!(
                "<system-reminder>Warning: the file exists but is shorter than the provided offset ({first}). The file has {total} lines.</system-reminder>"
            )
        };
        if let Some(banner) = banner {
            out.push_str(&format!("\n<system-reminder>{banner}</system-reminder>"));
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
