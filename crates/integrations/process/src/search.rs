use std::{
    borrow::Cow,
    env,
    ffi::{OsStr, OsString},
    fs, io,
    path::{Path, PathBuf},
    process::ExitCode,
    sync::OnceLock,
};

use super::selection::Shell;

const NAMES: [&str; 3] = ["grep", "egrep", "fgrep"];
const VCS_DIRECTORIES: [&str; 6] = [".git", ".svn", ".hg", ".bzr", ".jj", ".sl"];
const REGEX_META: &str = "\\.+*?()|[]{}^$#&-~";
const CLASS_ESCAPES: &str = "dDwWsSbBnt";

static SHIMS: OnceLock<PathBuf> = OnceLock::new();

#[cfg(unix)]
pub fn install(directory: &Path) -> io::Result<()> {
    use std::hash::{Hash, Hasher};

    let executable = fs::canonicalize(env::current_exe()?)?;
    let mut hasher = std::hash::DefaultHasher::new();
    executable.hash(&mut hasher);
    let shims = directory.join(format!("{:016x}", hasher.finish()));
    fs::create_dir_all(&shims)?;
    for name in NAMES {
        let link = shims.join(name);
        if fs::read_link(&link).is_ok_and(|target| target == executable) {
            continue;
        }
        let staged = shims.join(format!(".{name}.{}", std::process::id()));
        match fs::remove_file(&staged) {
            Err(error) if error.kind() != io::ErrorKind::NotFound => return Err(error),
            _ => {}
        }
        std::os::unix::fs::symlink(&executable, &staged)?;
        fs::rename(&staged, &link)?;
    }
    SHIMS
        .set(shims)
        .map_err(|_| io::Error::other("search shims are already installed"))
}

#[cfg(not(unix))]
pub fn install(_directory: &Path) -> io::Result<()> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "search shims need symbolic links",
    ))
}

#[must_use]
pub fn shimmed(argv0: &OsStr) -> Option<&'static str> {
    let name = Path::new(argv0).file_name()?.to_str()?;
    NAMES.into_iter().find(|candidate| *candidate == name)
}

pub fn run(name: &str, arguments: Vec<OsString>) -> ExitCode {
    let ripgrep = find("rg").zip(translate(name, &arguments));
    let (program, arguments) = match ripgrep {
        Some((_, Translated::Directories(directories))) => {
            for directory in directories {
                eprintln!("{name}: {}: Is a directory", directory.to_string_lossy());
            }
            return ExitCode::from(2);
        }
        Some((rg, Translated::Arguments(arguments))) => (rg, arguments),
        None => match find(name) {
            Some(program) => (program, arguments),
            None => {
                eprintln!("{name}: neither rg nor {name} was found on PATH");
                return ExitCode::from(2);
            }
        },
    };
    let error = replace(&program, &arguments);
    eprintln!("{name}: {}: {error}", program.display());
    ExitCode::from(2)
}

pub(super) fn wrap<'script>(script: &'script str, shell: &Shell) -> Cow<'script, str> {
    match SHIMS.get() {
        Some(shims) if matches!(shell.name(), "bash" | "zsh" | "sh") => {
            let quoted = shims.to_string_lossy().replace('\'', "'\\''");
            Cow::Owned(format!("export PATH='{quoted}':\"$PATH\"; {script}"))
        }
        _ => Cow::Borrowed(script),
    }
}

#[cfg(unix)]
fn replace(program: &Path, arguments: &[OsString]) -> io::Error {
    use std::os::unix::process::CommandExt;

    std::process::Command::new(program).args(arguments).exec()
}

#[cfg(not(unix))]
fn replace(program: &Path, arguments: &[OsString]) -> io::Error {
    match std::process::Command::new(program).args(arguments).status() {
        Ok(status) => std::process::exit(status.code().unwrap_or(2)),
        Err(error) => error,
    }
}

fn find(program: &str) -> Option<PathBuf> {
    let current = env::current_exe()
        .ok()
        .and_then(|path| fs::canonicalize(path).ok());
    env::split_paths(&env::var_os("PATH")?)
        .map(|directory| directory.join(program))
        .find(|candidate| executable(candidate) && fs::canonicalize(candidate).ok() != current)
}

#[cfg(unix)]
fn executable(path: &Path) -> bool {
    use std::os::unix::fs::PermissionsExt;

    fs::metadata(path)
        .is_ok_and(|metadata| metadata.is_file() && metadata.permissions().mode() & 0o111 != 0)
}

#[cfg(not(unix))]
fn executable(path: &Path) -> bool {
    path.is_file()
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Syntax {
    Basic,
    Extended,
    Fixed,
    Perl,
}

#[derive(Clone, Copy)]
enum Arity {
    Flag,
    Required,
    Optional,
}

struct Translation {
    syntax: Syntax,
    patterns: Vec<String>,
    pattern_files: Vec<String>,
    flags: Vec<OsString>,
    operands: Vec<OsString>,
    recursive: bool,
    count: bool,
}

enum Translated {
    Arguments(Vec<OsString>),
    Directories(Vec<OsString>),
}

fn translate(name: &str, arguments: &[OsString]) -> Option<Translated> {
    let mut translation = Translation {
        syntax: match name {
            "egrep" => Syntax::Extended,
            "fgrep" => Syntax::Fixed,
            _ => Syntax::Basic,
        },
        patterns: Vec::new(),
        pattern_files: Vec::new(),
        flags: Vec::new(),
        operands: Vec::new(),
        recursive: false,
        count: false,
    };
    let mut index = 0;
    let mut operands_only = false;
    while let Some(argument) = arguments.get(index) {
        index += 1;
        let text = argument.to_str();
        if operands_only || text.is_none_or(|text| text == "-" || !text.starts_with('-')) {
            translation.operands.push(argument.clone());
            continue;
        }
        let text = text?;
        if text == "--" {
            operands_only = true;
            continue;
        }
        if let Some(long) = text.strip_prefix("--") {
            let (key, inline) = match long.split_once('=') {
                Some((key, value)) => (key, Some(value.to_owned())),
                None => (long, None),
            };
            let value = match long_arity(key)? {
                Arity::Flag if inline.is_some() => return None,
                Arity::Flag => None,
                Arity::Optional => inline,
                Arity::Required => match inline {
                    Some(value) => Some(value),
                    None => {
                        index += 1;
                        Some(arguments.get(index - 1)?.to_str()?.to_owned())
                    }
                },
            };
            translation.long(key, value)?;
            continue;
        }
        let cluster = text[1..].chars().collect::<Vec<_>>();
        let mut position = 0;
        while let Some(&flag) = cluster.get(position) {
            position += 1;
            if flag.is_ascii_digit() {
                let mut digits = String::from(flag);
                while let Some(&digit) = cluster.get(position).filter(|c| c.is_ascii_digit()) {
                    digits.push(digit);
                    position += 1;
                }
                translation
                    .flags
                    .extend(["--context".into(), digits.into()]);
            } else if "efABCmdD".contains(flag) {
                let value = if position < cluster.len() {
                    let value = cluster[position..].iter().collect::<String>();
                    position = cluster.len();
                    value
                } else {
                    index += 1;
                    arguments.get(index - 1)?.to_str()?.to_owned()
                };
                translation.short_value(flag, value)?;
            } else {
                translation.short(flag)?;
            }
        }
    }
    translation.finish()
}

fn long_arity(key: &str) -> Option<Arity> {
    Some(match key {
        "regexp" | "file" | "after-context" | "before-context" | "context" | "max-count"
        | "directories" | "devices" | "include" | "exclude" | "exclude-dir" | "binary-files" => {
            Arity::Required
        }
        "color" | "colour" => Arity::Optional,
        "extended-regexp"
        | "fixed-strings"
        | "basic-regexp"
        | "perl-regexp"
        | "ignore-case"
        | "no-ignore-case"
        | "invert-match"
        | "word-regexp"
        | "line-regexp"
        | "count"
        | "files-with-matches"
        | "files-without-match"
        | "only-matching"
        | "quiet"
        | "silent"
        | "no-messages"
        | "line-number"
        | "byte-offset"
        | "with-filename"
        | "no-filename"
        | "text"
        | "recursive"
        | "dereference-recursive"
        | "null"
        | "null-data"
        | "binary"
        | "version"
        | "line-buffered" => Arity::Flag,
        _ => return None,
    })
}

impl Translation {
    fn short(&mut self, flag: char) -> Option<()> {
        let mapped: &[&str] = match flag {
            'E' => {
                self.syntax = Syntax::Extended;
                &[]
            }
            'F' => {
                self.syntax = Syntax::Fixed;
                &[]
            }
            'G' => {
                self.syntax = Syntax::Basic;
                &[]
            }
            'P' => {
                self.syntax = Syntax::Perl;
                &[]
            }
            'r' => {
                self.recursive = true;
                &[]
            }
            'R' => {
                self.recursive = true;
                &["--follow"]
            }
            'c' => {
                self.count = true;
                &["--count"]
            }
            'i' | 'y' => &["--ignore-case"],
            'v' => &["--invert-match"],
            'w' => &["--word-regexp"],
            'x' => &["--line-regexp"],
            'l' => &["--files-with-matches"],
            'L' => &["--files-without-match"],
            'o' => &["--only-matching"],
            'q' => &["--quiet"],
            's' => &["--no-messages"],
            'n' => &["--line-number"],
            'b' => &["--byte-offset"],
            'H' => &["--with-filename"],
            'h' => &["--no-filename"],
            'a' => &["--text"],
            'Z' => &["--null"],
            'z' => &["--null-data"],
            'V' => &["--version"],
            'I' | 'U' | 'u' => &[],
            _ => return None,
        };
        self.flags.extend(mapped.iter().map(OsString::from));
        Some(())
    }

    fn short_value(&mut self, flag: char, value: String) -> Option<()> {
        let option = match flag {
            'e' => {
                self.patterns.push(value);
                return Some(());
            }
            'f' => {
                self.pattern_files.push(value);
                return Some(());
            }
            'd' => {
                match value.as_str() {
                    "recurse" => self.recursive = true,
                    "read" => {}
                    _ => return None,
                }
                return Some(());
            }
            'D' => return matches!(value.as_str(), "read" | "skip").then_some(()),
            'A' => "--after-context",
            'B' => "--before-context",
            'C' => "--context",
            'm' => "--max-count",
            _ => return None,
        };
        self.flags.extend([option.into(), value.into()]);
        Some(())
    }

    fn long(&mut self, key: &str, value: Option<String>) -> Option<()> {
        let short = match key {
            "extended-regexp" => 'E',
            "fixed-strings" => 'F',
            "basic-regexp" => 'G',
            "perl-regexp" => 'P',
            "ignore-case" => 'i',
            "invert-match" => 'v',
            "word-regexp" => 'w',
            "line-regexp" => 'x',
            "count" => 'c',
            "files-with-matches" => 'l',
            "files-without-match" => 'L',
            "only-matching" => 'o',
            "quiet" | "silent" => 'q',
            "no-messages" => 's',
            "line-number" => 'n',
            "byte-offset" => 'b',
            "with-filename" => 'H',
            "no-filename" => 'h',
            "text" => 'a',
            "recursive" => 'r',
            "dereference-recursive" => 'R',
            "null" => 'Z',
            "null-data" => 'z',
            "binary" => 'U',
            "version" => 'V',
            "regexp" => 'e',
            "file" => 'f',
            "after-context" => 'A',
            "before-context" => 'B',
            "context" => 'C',
            "max-count" => 'm',
            "directories" => 'd',
            "devices" => 'D',
            _ => {
                let flags: Vec<OsString> = match (key, value) {
                    ("no-ignore-case", None) => vec!["--case-sensitive".into()],
                    ("line-buffered", None) => vec!["--line-buffered".into()],
                    ("include", Some(glob)) => vec!["--glob".into(), glob.into()],
                    ("exclude", Some(glob)) => vec!["--glob".into(), format!("!{glob}").into()],
                    ("exclude-dir", Some(glob)) => {
                        vec!["--glob".into(), format!("!{glob}/").into()]
                    }
                    ("color" | "colour", when) => {
                        vec![format!("--color={}", when.as_deref().unwrap_or("auto")).into()]
                    }
                    ("binary-files", Some(kind)) => match kind.as_str() {
                        "text" => vec!["--text".into()],
                        "binary" | "without-match" => Vec::new(),
                        _ => return None,
                    },
                    _ => return None,
                };
                self.flags.extend(flags);
                return Some(());
            }
        };
        match value {
            Some(value) => self.short_value(short, value),
            None => self.short(short),
        }
    }

    fn finish(mut self) -> Option<Translated> {
        for file in &self.pattern_files {
            let contents = fs::read_to_string(file).ok()?;
            self.patterns.extend(contents.lines().map(str::to_owned));
        }
        if self.pattern_files.is_empty() && self.patterns.is_empty() {
            if self.operands.is_empty() {
                return None;
            }
            self.patterns
                .push(self.operands.remove(0).into_string().ok()?);
        }
        if self.patterns.is_empty() {
            return None;
        }
        if !self.recursive {
            let directories = self
                .operands
                .iter()
                .filter(|operand| Path::new(operand).is_dir())
                .cloned()
                .collect::<Vec<_>>();
            if !directories.is_empty() && directories.len() == self.operands.len() {
                return Some(Translated::Directories(directories));
            }
            for directory in directories {
                eprintln!("grep: {}: Is a directory", directory.to_string_lossy());
            }
        }
        let mut arguments: Vec<OsString> = vec!["--no-config".into(), "--hidden".into()];
        for directory in VCS_DIRECTORIES {
            arguments.extend(["--glob".into(), format!("!{directory}").into()]);
        }
        match self.syntax {
            Syntax::Fixed => arguments.push("--fixed-strings".into()),
            Syntax::Perl => arguments.push("--pcre2".into()),
            Syntax::Basic | Syntax::Extended => {}
        }
        if self.count {
            arguments.push("--include-zero".into());
        }
        if !self.recursive {
            arguments.push("--max-depth=0".into());
        }
        arguments.append(&mut self.flags);
        for pattern in self.patterns.iter().flat_map(|pattern| pattern.split('\n')) {
            let pattern = match self.syntax {
                Syntax::Basic => convert(pattern, true)?,
                Syntax::Extended => convert(pattern, false)?,
                Syntax::Fixed | Syntax::Perl => pattern.to_owned(),
            };
            arguments.extend(["--regexp".into(), pattern.into()]);
        }
        arguments.push("--".into());
        if self.operands.is_empty() && !self.recursive {
            arguments.push("-".into());
        }
        arguments.append(&mut self.operands);
        Some(Translated::Arguments(arguments))
    }
}

fn convert(pattern: &str, basic: bool) -> Option<String> {
    let chars = pattern.chars().collect::<Vec<_>>();
    let mut output = String::with_capacity(pattern.len() + 8);
    let mut index = 0;
    let mut start = true;
    while let Some(&character) = chars.get(index) {
        index += 1;
        let at_start = std::mem::replace(&mut start, false);
        match character {
            '\\' => {
                let next = *chars.get(index)?;
                index += 1;
                match next {
                    '1'..='9' => return None,
                    '(' | '|' if basic => {
                        output.push(next);
                        start = true;
                    }
                    ')' if basic => output.push(')'),
                    '{' if basic && !at_start => {
                        if !interval(&chars, &mut index, true, &mut output) {
                            return None;
                        }
                    }
                    '+' | '?' if basic && !at_start => output.push(next),
                    '<' | '>' => {
                        output.push('\\');
                        output.push(next);
                    }
                    '\x60' => output.push_str("\\A"),
                    '\'' => output.push_str("\\z"),
                    _ if CLASS_ESCAPES.contains(next) => {
                        output.push('\\');
                        output.push(next);
                    }
                    _ => literal(next, &mut output),
                }
            }
            '[' => bracket(&chars, &mut index, &mut output)?,
            '(' | '|' if !basic => {
                output.push(character);
                start = true;
            }
            '{' if !basic && !at_start => {
                if !interval(&chars, &mut index, false, &mut output) {
                    output.push_str("\\{");
                }
            }
            '*' | '+' | '?' if at_start && !basic => start = true,
            '*' | '+' | '?' | '{' if at_start => literal(character, &mut output),
            '(' | ')' | '|' | '{' | '}' | '+' | '?' if basic => literal(character, &mut output),
            '^' if at_start || !basic => {
                output.push('^');
                start = at_start;
            }
            '^' => literal('^', &mut output),
            '$' if basic && !at_end(&chars, index) => literal('$', &mut output),
            _ => output.push(character),
        }
    }
    Some(output)
}

fn at_end(chars: &[char], index: usize) -> bool {
    match chars.get(index..index + 2) {
        Some(['\\', ')' | '|']) => true,
        _ => index == chars.len(),
    }
}

fn literal(character: char, output: &mut String) {
    if REGEX_META.contains(character) {
        output.push('\\');
    }
    output.push(character);
}

fn interval(chars: &[char], index: &mut usize, basic: bool, output: &mut String) -> bool {
    let mut cursor = *index;
    let digits = |cursor: &mut usize| {
        let begin = *cursor;
        while chars.get(*cursor).is_some_and(char::is_ascii_digit) {
            *cursor += 1;
        }
        chars[begin..*cursor].iter().collect::<String>()
    };
    let low = digits(&mut cursor);
    let high = (chars.get(cursor) == Some(&',')).then(|| {
        cursor += 1;
        digits(&mut cursor)
    });
    let closing: &[char] = if basic { &['\\', '}'] } else { &['}'] };
    if chars.get(cursor..cursor + closing.len()) != Some(closing)
        || (low.is_empty() && high.as_deref().is_none_or(str::is_empty))
    {
        return false;
    }
    *index = cursor + closing.len();
    output.push('{');
    output.push_str(if low.is_empty() { "0" } else { &low });
    if let Some(high) = high {
        output.push(',');
        output.push_str(&high);
    }
    output.push('}');
    true
}

fn bracket(chars: &[char], index: &mut usize, output: &mut String) -> Option<()> {
    let mut cursor = *index;
    output.push('[');
    if chars.get(cursor) == Some(&'^') {
        output.push('^');
        cursor += 1;
    }
    if chars.get(cursor) == Some(&']') {
        output.push_str("\\]");
        cursor += 1;
    }
    loop {
        let character = *chars.get(cursor)?;
        cursor += 1;
        match character {
            ']' => {
                output.push(']');
                *index = cursor;
                return Some(());
            }
            '[' if chars.get(cursor) == Some(&':') => {
                let end = (cursor + 1..chars.len().saturating_sub(1))
                    .find(|&position| chars[position] == ':' && chars[position + 1] == ']')?;
                output.extend(&chars[cursor - 1..end + 2]);
                cursor = end + 2;
            }
            '[' if matches!(chars.get(cursor), Some('.' | '=')) => return None,
            '\\' | '[' => {
                output.push('\\');
                output.push(character);
            }
            '&' | '~' | '-' if chars.get(cursor) == Some(&character) => {
                output.push('\\');
                output.push(character);
            }
            _ => output.push(character),
        }
    }
}
