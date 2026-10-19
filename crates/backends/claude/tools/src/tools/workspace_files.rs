use crate::tools::{SessionEnvironment, ToolContent, ToolOutput, media::MediaReadOptions};
use regex::RegexBuilder;
use serde_json::{Value, json};
use std::{
    fs::{self, OpenOptions},
    io::{Read as _, Write as _},
    path::{Component, Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
};

const MAX_FILE: usize = 1024 * 1024;
const MAX_VISITS: usize = 10_000;
const MAX_SEARCH_BYTES: u64 = 128 * 1024 * 1024;
static TEMP_ID: AtomicU64 = AtomicU64::new(0);

#[derive(Clone, Debug)]
pub struct ClaudeWorkspaceFiles {
    root: PathBuf,
    root_alias: PathBuf,
    base: PathBuf,
    confined: bool,
    media: MediaReadOptions,
    read_token_budget: usize,
}

impl ClaudeWorkspaceFiles {
    pub fn new(root: impl AsRef<Path>) -> Result<Self, String> {
        let given = root.as_ref();
        let root_alias = if given.is_absolute() {
            given.to_path_buf()
        } else {
            std::env::current_dir()
                .map_err(|e| format!("workspace current directory: {e}"))?
                .join(given)
        };
        let root = fs::canonicalize(given).map_err(|e| format!("workspace root: {e}"))?;
        if !root.is_dir() {
            return Err("workspace root is not a directory".into());
        }
        Ok(Self {
            base: root.clone(),
            confined: true,
            root,
            root_alias,
            media: MediaReadOptions::default(),
            read_token_budget: 200_000,
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
            root_alias: root.clone(),
            root,
            base,
            confined: false,
            media: MediaReadOptions::default(),
            read_token_budget: 200_000,
        })
    }

    fn shown(&self, relative: &Path) -> String {
        if self.confined {
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
    pub fn with_read_token_budget(mut self, tokens: usize) -> Self {
        self.read_token_budget = tokens;
        self
    }

    #[must_use]
    pub fn definitions() -> Vec<Value> {
        ["Read", "Edit", "Write", "Glob", "Grep"]
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
        self.execute_output_with_context(name, input, true).await
    }

    pub async fn execute_output_with_context(
        &self,
        name: &str,
        input: Value,
        include_project_context: bool,
    ) -> Result<ToolOutput, String> {
        self.execute_output_in_session(name, input, include_project_context, None)
            .await
    }

    pub async fn execute_output_in_session(
        &self,
        name: &str,
        input: Value,
        include_project_context: bool,
        session: Option<SessionEnvironment>,
    ) -> Result<ToolOutput, String> {
        let this = self.clone();
        let name = name.to_owned();
        tokio::task::spawn_blocking(move || {
            let mut media = None;
            if name == "Read" {
                let fields = input.as_object().ok_or("Read input must be an object")?;
                if let Some(key) = fields
                    .keys()
                    .find(|k| !["file_path", "offset", "limit", "pages", "allow_large"].contains(&k.as_str()))
                {
                    return Err(format!("unsupported Read option: {key}"));
                }
                if input.get("allow_large").is_some_and(|v| !v.is_boolean()) {
                    return Err("allow_large must be a boolean".into());
                }
                let (_, path) = this.file(Self::field(&input, "file_path")?)?;
                media = crate::tools::media::read(&path, &input, &this.media, session.as_ref())?;
            }
            let mut output = match media {
                Some(output) => output,
                None => ToolOutput::text(this.execute_sync(&name, &input)?),
            };
            if !include_project_context {
                return Ok(output);
            }

            let requested = if matches!(name.as_str(), "Glob" | "Grep") {
                input.get("path").and_then(Value::as_str).unwrap_or(".")
            } else {
                Self::field(&input, "file_path")?
            };
            let relative = this.relative(requested, true)?;
            let relative = if relative == Path::new(".") { Path::new("") } else { &relative };

            if let Ok(loader) = crate::tools::ClaudeProjectContext::new(&this.root) {
                let context = loader.load_for_path(relative);
                if !context.excerpts.is_empty() || !context.diagnostics.is_empty() {
                    let context = json!(context);
                    let text = format!("\nWorkspace context (guidance only; does not expand tool authority):\n{context}\n");
                    match &mut output.content {
                        ToolContent::Text(body) => body.push_str(&text),
                        ToolContent::Blocks(blocks) => blocks.push(crate::tools::ToolResultBlock::Text { text }),
                    }
                    let metadata = output.metadata.get_or_insert_with(|| json!({}));
                    metadata["project_context"] = context;
                }
            }
            Ok(output)
        })
        .await
        .map_err(|e| format!("workspace task: {e}"))?
    }

    fn execute_sync(&self, name: &str, input: &Value) -> Result<String, String> {
        let allowed: &[&str] = match name {
            "Read" => &["file_path", "offset", "limit", "pages", "allow_large"],
            "Write" => &["file_path", "content"],
            "Edit" => &["file_path", "old_string", "new_string", "replace_all"],
            "Glob" => &["pattern", "path"],

            "Grep" => return self.grep(input),
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
            "Glob" => self.glob(input),
            "Grep" => self.grep(input),
            _ => Err(format!("unknown workspace tool: {name}")),
        }
    }

    fn field<'a>(input: &'a Value, key: &str) -> Result<&'a str, String> {
        input
            .get(key)
            .and_then(Value::as_str)
            .ok_or_else(|| format!("missing or invalid {key}"))
    }

    fn relative(&self, text: &str, allow_root: bool) -> Result<PathBuf, String> {
        if text.len() > 4096 {
            return Err("path exceeds 4096-byte limit".into());
        }
        let joined;
        let path = if Path::new(text).is_absolute() || self.confined {
            Path::new(text)
        } else {
            joined = self.base.join(text);
            joined.as_path()
        };
        let relative = if path.is_absolute() {
            path.strip_prefix(&self.root)
                .or_else(|_| path.strip_prefix(&self.root_alias))
                .map_err(|_| "absolute path outside workspace".to_string())?
        } else {
            path
        };
        if relative
            .components()
            .any(|c| !matches!(c, Component::Normal(_)))
        {
            if !(allow_root && (text == "." || relative.as_os_str().is_empty())) {
                return Err("path must not contain traversal, root, or special components".into());
            }
        }
        if !allow_root && relative.as_os_str().is_empty() {
            return Err("file path is empty".into());
        }
        Ok(relative.to_path_buf())
    }

    fn existing(&self, text: &str) -> Result<(PathBuf, PathBuf), String> {
        let rel = self.relative(text, true)?;
        let target = self.root.join(&rel);
        let real = fs::canonicalize(target).map_err(|e| format!("path: {e}"))?;
        if !real.starts_with(&self.root) {
            return Err("symlink escapes workspace".into());
        }
        Ok((rel, real))
    }

    fn file(&self, text: &str) -> Result<(PathBuf, PathBuf), String> {
        let (rel, real) = self.existing(text)?;
        if !real.is_file() {
            return Err("path is not a regular file".into());
        }
        Ok((rel, real))
    }

    fn read_text(path: &Path) -> Result<String, String> {
        let f = fs::File::open(path).map_err(|e| format!("read: {e}"))?;
        let mut bytes = Vec::new();
        f.take((MAX_FILE + 1) as u64)
            .read_to_end(&mut bytes)
            .map_err(|e| format!("read: {e}"))?;
        if bytes.len() > MAX_FILE {
            return Err("file exceeds 1 MiB text limit".into());
        }
        String::from_utf8(bytes).map_err(|_| "file is not UTF-8 text".into())
    }

    fn read(&self, input: &Value) -> Result<String, String> {
        if input.get("pages").is_some() {
            return Err("pages is only applicable to PDF files".into());
        }
        let (_, path) = self.file(Self::field(input, "file_path")?)?;
        let allow_large = input
            .get("allow_large")
            .map_or(Some(false), Value::as_bool)
            .ok_or("allow_large must be a boolean")?;
        let source_limit = if allow_large {
            16 * 1024 * 1024
        } else {
            MAX_FILE
        };
        let mut bytes = Vec::new();
        fs::File::open(&path)
            .map_err(|e| format!("read: {e}"))?
            .take((source_limit + 1) as u64)
            .read_to_end(&mut bytes)
            .map_err(|e| format!("read: {e}"))?;
        if bytes.len() > source_limit {
            return Err(if allow_large {
                "text exceeds 16 MiB source limit"
            } else {
                "file exceeds 1 MiB text limit; use allow_large: true"
            }
            .into());
        }
        let content = String::from_utf8(bytes).map_err(|_| "file is not UTF-8 text")?;
        let output_limit = self
            .read_token_budget
            .min(if allow_large { usize::MAX } else { 25_000 })
            .saturating_mul(4)
            .min(16 * 1024 * 1024);
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
        for (i, line) in content
            .lines()
            .enumerate()
            .skip(offset.saturating_sub(1).min(usize::MAX as u64) as usize)
            .take(usize::try_from(limit).unwrap_or(usize::MAX))
        {
            let line = format!("{}\t{}\n", i + 1, line);
            if out.len().saturating_add(line.len()) > output_limit {
                return Err("requested text exceeds the available Read budget; use offset/limit or allow_large: true".into());
            }
            out.push_str(&line);
        }
        Ok(out)
    }

    fn write_target(&self, text: &str) -> Result<(PathBuf, PathBuf), String> {
        let rel = self.relative(text, false)?;
        let path = self.root.join(&rel);
        if !self.confined {
            let parent = path.parent().ok_or("file has no parent")?;
            fs::create_dir_all(parent).map_err(|e| format!("create directory: {e}"))?;
            let path = match fs::canonicalize(&path) {
                Ok(real) => real,
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => path,
                Err(e) => return Err(format!("check target: {e}")),
            };
            return Ok((rel, path));
        }
        let parent = path.parent().ok_or("file has no parent")?;
        let mut current = self.root.clone();
        for component in rel.parent().into_iter().flat_map(Path::components) {
            current.push(component.as_os_str());
            match fs::symlink_metadata(&current) {
                Ok(meta) if meta.file_type().is_symlink() => {
                    return Err("symlink parent rejected".into());
                }
                Ok(meta) if !meta.is_dir() => return Err("parent is not a directory".into()),
                Ok(_) => {}
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                    fs::create_dir(&current).map_err(|e| format!("create directory: {e}"))?
                }
                Err(e) => return Err(format!("check parent: {e}")),
            }
        }
        if fs::canonicalize(parent).map_err(|e| format!("parent: {e}"))? != parent {
            return Err("parent escapes workspace or uses a symlink".into());
        }
        reject_symlink_target(&path)?;
        Ok((rel, path))
    }

    fn write(&self, input: &Value) -> Result<String, String> {
        let content = Self::field(input, "content")?;
        if content.len() > MAX_FILE {
            return Err("content exceeds 1 MiB text limit".into());
        }
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

        let replacements = if replace_all { count } else { 1 };
        let updated_len = content.len() - old.len() * replacements;
        let updated_len = new
            .len()
            .checked_mul(replacements)
            .and_then(|added| updated_len.checked_add(added))
            .filter(|&len| len <= MAX_FILE)
            .ok_or("edited content exceeds 1 MiB text limit")?;
        let updated = if replace_all {
            content.replace(old, new)
        } else {
            content.replacen(old, new, 1)
        };
        debug_assert_eq!(updated.len(), updated_len);
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

    fn search_root(&self, input: &Value) -> Result<(PathBuf, PathBuf), String> {
        let text = input
            .get("path")
            .map_or(Some("."), Value::as_str)
            .ok_or("invalid path")?;
        self.existing(text)
    }

    fn walk(&self, path: &Path) -> Result<Vec<PathBuf>, String> {
        let mut stack = vec![path.to_path_buf()];
        let mut files = Vec::new();
        let mut visits = 0;
        while let Some(next) = stack.pop() {
            visits += 1;
            if visits > MAX_VISITS {
                return Err("search exceeds 10000 entries".into());
            }
            let meta = fs::symlink_metadata(&next).map_err(|e| format!("walk: {e}"))?;
            if meta.file_type().is_symlink() {
                continue;
            }
            if meta.is_file() {
                files.push(next);
            } else if meta.is_dir() {
                for entry in fs::read_dir(&next).map_err(|e| format!("walk: {e}"))? {
                    if visits + stack.len() >= MAX_VISITS {
                        return Err("search exceeds 10000 entries".into());
                    }
                    stack.push(entry.map_err(|e| format!("walk: {e}"))?.path());
                }
            }
        }
        files.sort();
        Ok(files)
    }

    fn glob(&self, input: &Value) -> Result<String, String> {
        let raw_pattern = Self::field(input, "pattern")?;
        let pattern = if Path::new(raw_pattern).is_absolute() {
            self.relative(raw_pattern, false)?
                .to_str()
                .ok_or("pattern is not UTF-8")?
                .to_owned()
        } else {
            raw_pattern.to_owned()
        };
        let matcher = glob_regex(&pattern)?;

        let literal_prefix = pattern
            .split('/')
            .take_while(|part| !part.contains(['*', '?', '[', '{', '\\']))
            .collect::<Vec<_>>()
            .join("/");
        let anchored = Path::new(raw_pattern).is_absolute() && !self.confined;
        let (_, root) = if anchored {
            (PathBuf::new(), self.root.clone())
        } else {
            self.search_root(input)?
        };
        if !literal_prefix.is_empty() {
            let candidate = root.join(&literal_prefix);
            if candidate.exists()
                && !fs::canonicalize(&candidate)
                    .map_err(|e| format!("glob path: {e}"))?
                    .starts_with(&self.root)
            {
                return Err("glob symlink escapes workspace".into());
            }
        }
        let mut out = String::new();
        let start = if anchored {
            root.join(&literal_prefix)
        } else {
            root.clone()
        };
        if anchored && !start.exists() {
            return Ok(out);
        }
        let mut files = self.walk(&start)?;
        files.sort_by_cached_key(|path| {
            (
                std::cmp::Reverse(fs::metadata(path).and_then(|m| m.modified()).ok()),
                path.clone(),
            )
        });
        for file in files {
            let rel = file
                .strip_prefix(&root)
                .map_err(|_| "search path changed")?
                .to_string_lossy();
            if matcher.is_match(rel.as_ref()) {
                let shown = self.shown(
                    file.strip_prefix(&self.root)
                        .map_err(|_| "search escaped workspace")?,
                );
                if !push_bounded(&mut out, &format!("{shown}\n")) {
                    break;
                }
            }
        }
        Ok(out)
    }

    fn grep(&self, input: &Value) -> Result<String, String> {
        let fields = input.as_object().ok_or("Grep input must be an object")?;
        for key in fields.keys() {
            if !matches!(
                key.as_str(),
                "pattern"
                    | "path"
                    | "glob"
                    | "type"
                    | "output_mode"
                    | "-B"
                    | "-A"
                    | "-C"
                    | "context"
                    | "-n"
                    | "-i"
                    | "-o"
                    | "head_limit"
                    | "offset"
                    | "multiline"
                    | "case_sensitive"
            ) {
                return Err(format!("unsupported Grep option: {key}"));
            }
        }
        let pattern = Self::field(input, "pattern")?;
        if pattern.is_empty() || pattern.len() > 4096 {
            return Err("pattern must be 1 to 4096 bytes".into());
        }
        let mode = input
            .get("output_mode")
            .map_or(Some("files_with_matches"), Value::as_str)
            .ok_or("invalid output_mode")?;
        if !matches!(mode, "content" | "files_with_matches" | "count") {
            return Err("invalid output_mode".into());
        }
        let insensitive = grep_bool(input, "-i", false)?;
        if input.get("case_sensitive").is_some() && input.get("-i").is_some() {
            return Err("case_sensitive conflicts with -i".into());
        }

        let sensitive = grep_bool(input, "case_sensitive", !insensitive)?;
        let only_matching = grep_bool(input, "-o", false)?;
        let line_numbers = grep_bool(input, "-n", true)?;
        let multiline = grep_bool(input, "multiline", false)?;
        let before = grep_number(input, "-B", 0, 2000)?;
        let after = grep_number(input, "-A", 0, 2000)?;
        if input.get("-C").is_some() && input.get("context").is_some() {
            return Err("-C conflicts with context".into());
        }
        let context = if input.get("-C").is_some() {
            grep_number(input, "-C", 0, 2000)?
        } else {
            grep_number(input, "context", 0, 2000)?
        };
        let before = if input.get("-B").is_some() {
            before
        } else {
            context
        };
        let after = if input.get("-A").is_some() {
            after
        } else {
            context
        };
        let offset = grep_number(input, "offset", 0, u64::MAX)?;
        let head_limit = grep_number(input, "head_limit", 250, u64::MAX)?;
        if only_matching && (before > 0 || after > 0 || multiline) {
            return Err("-o cannot be combined with context or multiline".into());
        }
        let glob = match input.get("glob") {
            Some(value) => Some(value.as_str().ok_or("invalid glob")?),
            None => None,
        };
        let glob_exclude = glob.is_some_and(|g| g.starts_with('!'));
        let glob = glob.map(|g| g.strip_prefix('!').unwrap_or(g));
        let glob_matcher = glob.map(glob_regex).transpose()?;
        let mut types = ignore::types::TypesBuilder::new();
        types.add_defaults();
        if let Some(kind) = input.get("type") {
            let kind = kind.as_str().ok_or("invalid type")?;
            if kind.is_empty() || kind.len() > 64 {
                return Err("invalid type".into());
            }
            types.select(kind);
        }
        let types = types
            .build()
            .map_err(|e| format!("invalid file type: {e}"))?;
        let re = RegexBuilder::new(pattern)
            .case_insensitive(!sensitive)
            .multi_line(multiline)
            .dot_matches_new_line(multiline)
            .size_limit(4 * 1024 * 1024)
            .build()
            .map_err(|e| format!("invalid or oversized regex: {e}"))?;
        let (_, root) = self.search_root(input)?;
        let mut out = String::new();
        let mut searched_bytes = 0u64;
        let mut skipped = 0u64;
        let mut yielded = 0u64;
        for file in self.walk(&root)? {
            let shown = self.shown(
                file.strip_prefix(&self.root)
                    .map_err(|_| "search escaped workspace")?,
            );
            if let (Some(glob), Some(matcher)) = (glob, &glob_matcher) {
                let relative = file
                    .strip_prefix(&root)
                    .map_err(|_| "search path changed")?;
                let target = if glob.contains('/') && !relative.as_os_str().is_empty() {
                    relative.to_string_lossy()
                } else {
                    file.file_name().unwrap_or_default().to_string_lossy()
                };
                if matcher.is_match(target.as_ref()) == glob_exclude {
                    continue;
                }
            }
            if types.matched(&file, false).is_ignore() {
                continue;
            }
            let size = fs::metadata(&file)
                .map_err(|e| format!("search metadata: {e}"))?
                .len();
            searched_bytes = searched_bytes.saturating_add(size.min((MAX_FILE + 1) as u64));
            if searched_bytes > MAX_SEARCH_BYTES {
                return Err("search exceeds 128 MiB scan limit".into());
            }
            let Ok(contents) = Self::read_text(&file) else {
                continue;
            };

            if contents.contains('\0') {
                continue;
            }
            let lines: Vec<&str> = contents.lines().collect();
            let mut hits = vec![false; lines.len()];
            if multiline {
                let starts: Vec<usize> = std::iter::once(0)
                    .chain(contents.match_indices('\n').map(|(i, _)| i + 1))
                    .collect();
                for found in re.find_iter(&contents) {
                    let first = starts
                        .partition_point(|&s| s <= found.start())
                        .saturating_sub(1);
                    let last_byte = found.end().saturating_sub(1).max(found.start());
                    let last = starts
                        .partition_point(|&s| s <= last_byte)
                        .saturating_sub(1);
                    for hit in hits.iter_mut().take(last.saturating_add(1)).skip(first) {
                        *hit = true;
                    }
                }
            } else {
                for (line, hit) in lines.iter().zip(&mut hits) {
                    *hit = re.is_match(line);
                }
            }
            if mode != "content" {
                let count = hits.iter().filter(|&&hit| hit).count();
                if count == 0 {
                    continue;
                }
                if skipped < offset {
                    skipped += 1;
                    continue;
                }
                if head_limit != 0 && yielded >= head_limit {
                    return Ok(out);
                }
                let line = if mode == "count" {
                    format!("{shown}:{count}\n")
                } else {
                    format!("{shown}\n")
                };
                if !push_bounded(&mut out, &line) {
                    return Ok(out);
                }
                yielded += 1;
                continue;
            }
            if only_matching {
                for (index, line) in lines.iter().enumerate() {
                    for found in re.find_iter(line) {
                        if skipped < offset {
                            skipped += 1;
                            continue;
                        }
                        if head_limit != 0 && yielded >= head_limit {
                            return Ok(out);
                        }
                        let prefix = if line_numbers {
                            format!("{shown}:{}:", index + 1)
                        } else {
                            format!("{shown}:")
                        };
                        if !push_bounded(&mut out, &format!("{prefix}{}\n", found.as_str())) {
                            return Ok(out);
                        }
                        yielded += 1;
                    }
                }
                continue;
            }
            let mut selected = vec![false; lines.len()];
            for (index, hit) in hits.iter().enumerate() {
                if !hit {
                    continue;
                }
                if skipped < offset {
                    skipped += 1;
                    continue;
                }
                if head_limit != 0 && yielded >= head_limit {
                    break;
                }
                selected[index] = true;
                yielded += 1;
            }
            let mut emit = vec![false; lines.len()];
            for (index, &hit) in selected.iter().enumerate() {
                if hit {
                    let start = index.saturating_sub(before as usize);
                    let end = index
                        .saturating_add(after as usize)
                        .saturating_add(1)
                        .min(lines.len());
                    emit[start..end].fill(true);
                }
            }
            let mut previous = None;
            for (index, line) in lines.iter().enumerate() {
                if !emit[index] {
                    continue;
                }
                if let Some(prev) = previous
                    && index > prev + 1
                    && !push_bounded(&mut out, "--\n")
                {
                    return Ok(out);
                }
                let separator = if selected[index] { ':' } else { '-' };
                let result = if line_numbers {
                    format!("{shown}{separator}{}{separator}{line}\n", index + 1)
                } else {
                    format!("{shown}{separator}{line}\n")
                };
                if !push_bounded(&mut out, &result) {
                    return Ok(out);
                }
                previous = Some(index);
            }
            if head_limit != 0 && yielded >= head_limit {
                return Ok(out);
            }
        }
        Ok(out)
    }
}

fn grep_bool(input: &Value, key: &str, default: bool) -> Result<bool, String> {
    input
        .get(key)
        .map_or(Some(default), Value::as_bool)
        .ok_or_else(|| format!("invalid {key}"))
}

fn grep_number(input: &Value, key: &str, default: u64, max: u64) -> Result<u64, String> {
    let value = input
        .get(key)
        .map_or(Some(default), Value::as_u64)
        .ok_or_else(|| format!("invalid {key}"))?;
    if value > max {
        return Err(format!("{key} exceeds {max}"));
    }
    Ok(value)
}

fn reject_symlink_target(path: &Path) -> Result<(), String> {
    match fs::symlink_metadata(path) {
        Ok(meta) if meta.file_type().is_symlink() => Err("symlink target rejected".into()),
        Ok(meta) if !meta.is_file() => Err("target is not a regular file".into()),
        Ok(_) => Ok(()),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(format!("check target: {e}")),
    }
}

fn atomic_write(path: &Path, content: &str) -> Result<(), String> {
    let parent = path.parent().ok_or("file has no parent")?;
    let mut temp = None;
    for _ in 0..16 {
        let name = format!(
            ".nanocode-{}-{}.tmp",
            std::process::id(),
            TEMP_ID.fetch_add(1, Ordering::Relaxed)
        );
        let candidate = parent.join(name);
        let mut options = OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt as _;
            options.mode(0o600);
        }
        match options.open(&candidate) {
            Ok(file) => {
                temp = Some((candidate, file));
                break;
            }
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {}
            Err(e) => return Err(format!("create temp file: {e}")),
        }
    }
    let (temp_path, mut file) = temp.ok_or("could not allocate temp file")?;
    let result = (|| {
        file.write_all(content.as_bytes())
            .map_err(|e| format!("write: {e}"))?;
        file.sync_all().map_err(|e| format!("sync: {e}"))?;
        drop(file);
        reject_symlink_target(path)?;
        if let Ok(meta) = fs::metadata(path) {
            fs::set_permissions(&temp_path, meta.permissions())
                .map_err(|e| format!("permissions: {e}"))?;
        }
        fs::rename(&temp_path, path).map_err(|e| format!("rename: {e}"))
    })();
    if result.is_err() {
        let _ = fs::remove_file(&temp_path);
    }
    result
}

fn push_bounded(output: &mut String, line: &str) -> bool {
    output.push_str(line);
    true
}

fn validate_pattern(pattern: &str) -> Result<(), String> {
    if pattern.is_empty()
        || pattern.len() > 512
        || pattern.starts_with('/')
        || pattern.split('/').any(|part| part == ".." || part == ".")
    {
        return Err("invalid glob pattern or traversal".into());
    }
    Ok(())
}

fn glob_regex(pattern: &str) -> Result<globset::GlobMatcher, String> {
    validate_pattern(pattern)?;
    globset::GlobBuilder::new(pattern)
        .literal_separator(true)
        .backslash_escape(true)
        .build()
        .map(|glob| glob.compile_matcher())
        .map_err(|e| format!("invalid glob: {e}"))
}
