use std::{
    fs::{self, File},
    io::Read as _,
    path::{Component, Path, PathBuf},
};

pub(crate) const FILE_BYTES: usize = 32 * 1024;

pub(crate) fn authorized_root(root: &Path) -> Result<PathBuf, String> {
    let root = root
        .canonicalize()
        .map_err(|e| format!("workspace root: {e}"))?;
    if !root.is_dir() {
        return Err("workspace root is not a directory".into());
    }
    Ok(root)
}
pub(crate) fn safe_relative(path: &Path) -> bool {
    path.components().all(|c| matches!(c, Component::Normal(_)))
}
pub(crate) fn local_directory(root: &Path, relative: &Path) -> bool {
    if !safe_relative(relative) {
        return false;
    }
    let mut path = root.to_path_buf();
    for component in relative.components() {
        path.push(component.as_os_str());
        if !fs::symlink_metadata(&path).is_ok_and(|m| m.is_dir() && !m.file_type().is_symlink()) {
            return false;
        }
    }
    true
}
pub(crate) fn read_local(
    root: &Path,
    relative: &Path,
    limit: usize,
) -> Result<(String, bool), String> {
    if !safe_relative(relative) || relative.as_os_str().is_empty() {
        return Err("path must be workspace-relative without traversal".into());
    }
    #[cfg(unix)]
    let file = {
        use nix::{
            fcntl::{OFlag, openat},
            sys::stat::Mode,
        };
        let mut file = File::open(root).map_err(|e| e.to_string())?;
        let mut parts = relative.components().peekable();
        while let Some(part) = parts.next() {
            let mut flags =
                OFlag::O_RDONLY | OFlag::O_CLOEXEC | OFlag::O_NOFOLLOW | OFlag::O_NONBLOCK;
            if parts.peek().is_some() {
                flags |= OFlag::O_DIRECTORY;
            }
            file = File::from(
                openat(&file, Path::new(part.as_os_str()), flags, Mode::empty())
                    .map_err(|e| format!("local file: {e}"))?,
            );
        }
        file
    };
    #[cfg(not(unix))]
    let file = {
        if !local_directory(root, relative.parent().unwrap_or(Path::new("")))
            || fs::symlink_metadata(root.join(relative))
                .map_err(|e| e.to_string())?
                .file_type()
                .is_symlink()
        {
            return Err("symlink path is not permitted".into());
        }
        File::open(root.join(relative)).map_err(|e| e.to_string())?
    };
    read_bounded(file, limit)
}

pub(crate) fn read_user_file(path: &Path, limit: usize) -> Result<(String, bool), String> {
    if !path.is_absolute() {
        return Err("user file path must be absolute".into());
    }
    read_bounded(
        File::open(path).map_err(|e| format!("user file: {e}"))?,
        limit,
    )
}
fn read_bounded(file: File, limit: usize) -> Result<(String, bool), String> {
    if !file.metadata().map_err(|e| e.to_string())?.is_file() {
        return Err("not a regular file".into());
    }
    let mut bytes = Vec::new();
    file.take((limit + 1) as u64)
        .read_to_end(&mut bytes)
        .map_err(|e| e.to_string())?;
    let truncated = bytes.len() > limit;
    bytes.truncate(limit);
    let mut text = String::from_utf8_lossy(&bytes).into_owned();
    let end = text.floor_char_boundary(limit);
    let truncated = truncated || end < text.len();
    text.truncate(end);
    Ok((text, truncated))
}
pub(crate) fn frontmatter(text: &str) -> Result<(serde_json::Value, &str), String> {
    let text = text.strip_prefix('\u{feff}').unwrap_or(text);
    let Some(rest) = text
        .strip_prefix("---\n")
        .or_else(|| text.strip_prefix("---\r\n"))
    else {
        return Ok((serde_json::json!({}), text));
    };
    let mut offset = 0;
    for line in rest.split_inclusive('\n') {
        if line.trim_end() == "---" {
            let meta: serde_json::Value = serde_yaml_ng::from_str(&rest[..offset])
                .map_err(|e| format!("invalid YAML frontmatter: {e}"))?;
            if !meta.is_object() && !meta.is_null() {
                return Err("frontmatter must be a mapping".into());
            }
            return Ok((meta, &rest[offset + line.len()..]));
        }
        offset += line.len();
    }
    Err("unterminated YAML frontmatter".into())
}
