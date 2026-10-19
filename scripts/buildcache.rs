use std::{
    collections::BTreeMap,
    env,
    ffi::{OsStr, OsString},
    fs,
    io::{self, Read, Write},
    os::unix::{
        ffi::{OsStrExt, OsStringExt},
        fs::symlink,
        process::CommandExt,
    },
    path::{Path, PathBuf},
    process::{Command, ExitCode, Stdio},
    time::SystemTime,
};

const RUST_KEY: &str = "nanocode-rustc-cache-4";
const C_KEY: &str = "nanocode-c-cache-3 -ffile-prefix-map=<out>=. -MD";
const MEMO: &str = ".nanocode-sha256";

type Environment = BTreeMap<OsString, OsString>;

fn main() -> ExitCode {
    let args: Vec<OsString> = env::args_os().skip(1).collect();
    let Some(tool) = args.first() else {
        return ExitCode::from(2);
    };
    let cache = cache_dir();
    let environment: Environment = env::vars_os().collect();
    if tool.as_bytes().ends_with(b"rustc") {
        rust(&cache, args, environment)
    } else if environment.contains_key(OsStr::new("OUT_DIR")) {
        c(&cache, &args, environment)
    } else {
        exec(&args, &environment)
    }
}

fn cache_dir() -> PathBuf {
    env::var_os("XDG_CACHE_HOME")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from(env::var_os("HOME").unwrap_or_default()).join(".cache"))
        .join("nanocode/build")
}

fn exec(args: &[OsString], environment: &Environment) -> ExitCode {
    let error = Command::new(&args[0])
        .args(&args[1..])
        .env_clear()
        .envs(environment)
        .exec();
    eprintln!("buildcache: could not run {}: {error}", args[0].to_string_lossy());
    ExitCode::from(1)
}

fn exit_code(status: std::process::ExitStatus) -> ExitCode {
    ExitCode::from(status.code().map_or(1, |code| code as u8))
}

fn rust(cache: &Path, mut args: Vec<OsString>, mut environment: Environment) -> ExitCode {
    for name in ["CARGO_TARGET_DIR", "CARGO_BUILD_TARGET_DIR", "CARGO_BUILD_BUILD_DIR"] {
        environment.remove(OsStr::new(name));
    }
    if let Some(out_dir) = environment.get(OsStr::new("OUT_DIR")).cloned()
        && Path::new(&out_dir).is_dir()
        && let Some(stable) = stable(cache, Path::new(&out_dir))
    {
        environment.insert("OUT_DIR".into(), stable.into_os_string());
    }
    let registry = PathBuf::from(
        environment
            .get(OsStr::new("CARGO_HOME"))
            .cloned()
            .unwrap_or_else(|| {
                PathBuf::from(environment.get(OsStr::new("HOME")).cloned().unwrap_or_default())
                    .join(".cargo")
                    .into_os_string()
            }),
    )
    .join("registry/src/");
    let mut keyed: Vec<Vec<u8>> = Vec::with_capacity(args.len());
    let (mut out_dir, mut suffix, mut source, mut eligible) = (None, None, false, true);
    for index in 1..args.len() {
        let previous = args[index - 1].as_bytes().to_vec();
        let arg = args[index].clone();
        let bytes = arg.as_bytes();
        let mut key = bytes.to_vec();
        match previous.as_slice() {
            b"-L" => {
                if let Some(at) = bytes.iter().position(|byte| *byte == b'=') {
                    let (kind, path) = (&bytes[..at], &bytes[at + 1..]);
                    if kind == b"dependency" {
                        key = b"dependency=".to_vec();
                    } else if is_build_out(path) && Path::new(OsStr::from_bytes(path)).is_dir()
                        && let Some(stable) = stable(cache, Path::new(OsStr::from_bytes(path)))
                    {
                        let mut rewritten = kind.to_vec();
                        rewritten.push(b'=');
                        rewritten.extend_from_slice(stable.as_os_str().as_bytes());
                        key.clone_from(&rewritten);
                        args[index] = OsString::from_vec(rewritten);
                    }
                }
            }
            b"--out-dir" => {
                out_dir = Some(PathBuf::from(&arg));
                key.clear();
            }
            b"--extern" => {
                if let Some(at) = bytes.iter().position(|byte| *byte == b'=') {
                    let path = Path::new(OsStr::from_bytes(&bytes[at + 1..]));
                    match identity(path) {
                        Some(identity) => {
                            key = bytes[..at].to_vec();
                            key.push(b'=');
                            key.extend_from_slice(path.file_name().map_or(b"" as &[u8], OsStrExt::as_bytes));
                            key.push(b' ');
                            key.extend_from_slice(identity.as_bytes());
                        }
                        None => eligible = false,
                    }
                }
            }
            b"-C" => {
                if let Some(value) = bytes.strip_prefix(b"extra-filename=") {
                    suffix = Some(value.to_vec());
                }
            }
            _ => {}
        }
        if bytes.starts_with(b"@") {
            eligible = false;
        }
        if bytes.ends_with(b".rs") && bytes.starts_with(registry.as_os_str().as_bytes()) {
            source = true;
        }
        keyed.push(key);
    }
    let (Some(out_dir), Some(suffix), true, true) = (out_dir, suffix, source, eligible) else {
        return exec(&args, &environment);
    };
    let Some(version) = rustc_version(cache, Path::new(&args[0])) else {
        return exec(&args, &environment);
    };
    let mut hasher = Sha256::new();
    hasher.update(RUST_KEY.as_bytes());
    hasher.update(b"\n");
    hasher.update(version.as_bytes());
    hasher.update(b"\n");
    for key in &keyed {
        hasher.update(key);
        hasher.update(b"\n");
    }
    for (name, value) in &environment {
        let name = name.as_bytes();
        let relevant = (name.starts_with(b"CARGO_") && name != b"CARGO_MAKEFLAGS")
            || matches!(name, b"OUT_DIR" | b"RUSTC_BOOTSTRAP" | b"MACOSX_DEPLOYMENT_TARGET" | b"SDKROOT");
        if relevant {
            hasher.update(name);
            hasher.update(b"=");
            hasher.update(value.as_bytes());
            hasher.update(b"\n");
        }
    }
    let key = hasher.finish();
    let entry = cache.join("units").join(&key);
    if restore(&entry, &out_dir, &environment).is_ok() {
        return ExitCode::SUCCESS;
    }
    let mut child = match Command::new(&args[0])
        .args(&args[1..])
        .env_clear()
        .envs(&environment)
        .stderr(Stdio::piped())
        .spawn()
    {
        Ok(child) => child,
        Err(error) => {
            eprintln!("buildcache: could not run {}: {error}", args[0].to_string_lossy());
            return ExitCode::from(1);
        }
    };
    let mut captured = Vec::new();
    if let Some(mut pipe) = child.stderr.take() {
        let mut buffer = [0; 65536];
        let mut stderr = io::stderr();
        loop {
            match pipe.read(&mut buffer) {
                Ok(0) => break,
                Ok(read) => {
                    let _ = stderr.write_all(&buffer[..read]);
                    let _ = stderr.flush();
                    captured.extend_from_slice(&buffer[..read]);
                }
                Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
                Err(_) => break,
            }
        }
    }
    let status = match child.wait() {
        Ok(status) => status,
        Err(_) => return ExitCode::from(1),
    };
    if !status.success() {
        return exit_code(status);
    }
    let outputs = outputs(&out_dir, &suffix);
    let mut digests = Vec::new();
    for output in &outputs {
        let name = output.file_name().map_or(b"" as &[u8], OsStrExt::as_bytes);
        let linked = name.ends_with(b".rmeta")
            || (name.starts_with(b"lib") && (name.ends_with(b".dylib") || name.ends_with(b".so")));
        if linked && let Ok(digest) = digest_file(output) {
            write_memo(output, &digest);
            digests.extend_from_slice(name);
            digests.push(b' ');
            digests.extend_from_slice(digest.as_bytes());
            digests.push(b'\n');
        }
    }
    let _ = store(cache, &entry, &out_dir, &registry, &outputs, &digests, &captured, &environment);
    ExitCode::SUCCESS
}

fn is_build_out(path: &[u8]) -> bool {
    path.ends_with(b"/out") && path.windows(7).any(|window| window == b"/build/")
}

fn outputs(out_dir: &Path, suffix: &[u8]) -> Vec<PathBuf> {
    let mut outputs: Vec<PathBuf> = fs::read_dir(out_dir)
        .into_iter()
        .flatten()
        .flatten()
        .filter(|entry| {
            let name = entry.file_name();
            let name = name.as_bytes();
            name.windows(suffix.len()).any(|window| window == suffix)
                && !name.windows(MEMO.len()).any(|window| window == MEMO.as_bytes())
                && entry.file_type().is_ok_and(|kind| kind.is_file())
        })
        .map(|entry| entry.path())
        .collect();
    outputs.sort();
    outputs
}

fn memo_path(path: &Path) -> PathBuf {
    let mut name = path.as_os_str().to_os_string();
    name.push(MEMO);
    PathBuf::from(name)
}

fn modified(path: &Path) -> Option<SystemTime> {
    fs::metadata(path).and_then(|metadata| metadata.modified()).ok()
}

fn identity(path: &Path) -> Option<String> {
    let memo = memo_path(path);
    if let (Some(file), Some(memo_time)) = (modified(path), modified(&memo))
        && file <= memo_time
        && let Ok(text) = fs::read_to_string(&memo)
        && let Some(line) = text.lines().next()
        && line.len() == 64
    {
        return Some(line.to_owned());
    }
    let digest = digest_file(path).ok()?;
    write_memo(path, &digest);
    Some(digest)
}

fn write_memo(path: &Path, digest: &str) {
    let memo = memo_path(path);
    let mut staging = memo.clone().into_os_string();
    staging.push(format!(".{}", std::process::id()));
    if fs::write(&staging, format!("{digest}\n")).is_ok() && fs::rename(&staging, &memo).is_err() {
        let _ = fs::remove_file(&staging);
    }
}

fn digest_file(path: &Path) -> io::Result<String> {
    let mut file = fs::File::open(path)?;
    let mut hasher = Sha256::new();
    let mut buffer = vec![0; 1 << 20];
    loop {
        let read = file.read(&mut buffer)?;
        if read == 0 {
            return Ok(hasher.finish());
        }
        hasher.update(&buffer[..read]);
    }
}

fn rustc_version(cache: &Path, rustc: &Path) -> Option<String> {
    let path = rustc.as_os_str().as_bytes();
    let memo = path
        .windows(12)
        .any(|window| window == b"/toolchains/")
        .then(|| {
            let name: Vec<u8> = path
                .iter()
                .map(|byte| if matches!(byte, b'/' | b' ') { b'_' } else { *byte })
                .collect();
            cache.join("rustc").join(OsStr::from_bytes(&name))
        });
    if let Some(memo) = &memo
        && let (Some(binary), Some(memo_time)) = (modified(rustc), modified(memo))
        && binary <= memo_time
        && let Ok(text) = fs::read_to_string(memo)
        && let Some(line) = text.lines().next()
    {
        return Some(line.to_owned());
    }
    let output = Command::new(rustc).arg("-vV").output().ok()?;
    if !output.status.success() {
        return None;
    }
    let mut hasher = Sha256::new();
    hasher.update(&output.stdout);
    let version = hasher.finish();
    if let Some(memo) = memo {
        let _ = fs::create_dir_all(cache.join("rustc"));
        let mut staging = memo.clone().into_os_string();
        staging.push(format!(".{}", std::process::id()));
        if fs::write(&staging, format!("{version}\n")).is_ok() {
            let _ = fs::rename(&staging, &memo);
        }
    }
    Some(version)
}

fn stable(cache: &Path, directory: &Path) -> Option<PathBuf> {
    let mut entries = Vec::new();
    walk(directory, Path::new(""), &mut entries).ok()?;
    entries.sort();
    let mut hasher = Sha256::new();
    for (relative, is_file) in &entries {
        hasher.update(relative.as_os_str().as_bytes());
        if *is_file {
            hasher.update(b" ");
            hasher.update(digest_file(&directory.join(relative)).ok()?.as_bytes());
        }
        hasher.update(b"\n");
    }
    let stable = cache.join("out").join(hasher.finish());
    if !stable.exists() {
        fs::create_dir_all(cache.join("out")).ok()?;
        let staging = cache.join("out").join(format!(".data.{}.{}", std::process::id(), unique()));
        let copied = (|| -> io::Result<()> {
            fs::create_dir(&staging)?;
            for (relative, is_file) in &entries {
                if *is_file {
                    clone(&directory.join(relative), &staging.join(relative))?;
                } else {
                    fs::create_dir_all(staging.join(relative))?;
                }
            }
            Ok(())
        })();
        if copied.is_err() || symlink(&staging, &stable).is_err() {
            let _ = fs::remove_dir_all(&staging);
        }
    }
    stable.exists().then_some(stable)
}

fn unique() -> u128 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map_or(0, |elapsed| elapsed.as_nanos())
}

fn walk(root: &Path, relative: &Path, entries: &mut Vec<(PathBuf, bool)>) -> io::Result<()> {
    for entry in fs::read_dir(root.join(relative))? {
        let entry = entry?;
        let path = relative.join(entry.file_name());
        let kind = entry.file_type()?;
        if kind.is_dir() {
            entries.push((path.clone(), false));
            walk(root, &path, entries)?;
        } else {
            entries.push((path, true));
        }
    }
    Ok(())
}

fn clone(from: &Path, to: &Path) -> io::Result<()> {
    if fs::symlink_metadata(to).is_ok() {
        fs::remove_file(to)?;
    }
    fs::copy(from, to)?;
    Ok(())
}

fn touch(path: &Path) -> io::Result<()> {
    fs::File::options().write(true).open(path)?.set_modified(SystemTime::now())
}

fn relocate(text: &[u8], from: &[u8], to: &[u8]) -> Vec<u8> {
    if from.is_empty() {
        return text.to_vec();
    }
    let mut result = Vec::with_capacity(text.len());
    let mut index = 0;
    while index < text.len() {
        if text[index..].starts_with(from) {
            result.extend_from_slice(to);
            index += from.len();
        } else {
            result.push(text[index]);
            index += 1;
        }
    }
    result
}

fn same_environment(entry: &Path, environment: &Environment) -> bool {
    let Ok(recorded) = fs::read(entry.join("env")) else {
        return true;
    };
    recorded.split(|byte| *byte == b'\n').filter(|line| !line.is_empty()).all(|line| {
        match line.iter().position(|byte| *byte == b'=') {
            Some(at) => environment
                .get(OsStr::from_bytes(&line[..at]))
                .is_some_and(|value| value.as_bytes() == &line[at + 1..]),
            None => !environment.contains_key(OsStr::from_bytes(line)),
        }
    })
}

fn restore(entry: &Path, out_dir: &Path, environment: &Environment) -> io::Result<()> {
    let cached_out = fs::read(entry.join("out-dir"))?;
    let cached_out = cached_out.strip_suffix(b"\n").unwrap_or(&cached_out).to_vec();
    if !same_environment(entry, environment) {
        return Err(io::Error::other("environment changed"));
    }
    let current = out_dir.as_os_str().as_bytes();
    let mut files: Vec<_> = fs::read_dir(entry.join("files"))?.collect::<io::Result<_>>()?;
    files.sort_by_key(fs::DirEntry::file_name);
    for file in &files {
        let name = file.file_name();
        let destination = out_dir.join(&name);
        if name.as_bytes().ends_with(b".d") {
            fs::write(&destination, relocate(&fs::read(file.path())?, &cached_out, current))?;
        } else {
            clone(&file.path(), &destination)?;
            touch(&destination)?;
        }
    }
    if let Ok(digests) = fs::read(entry.join("digests")) {
        for line in digests.split(|byte| *byte == b'\n') {
            if let Some(at) = line.iter().position(|byte| *byte == b' ') {
                let digest = String::from_utf8_lossy(&line[at + 1..]);
                write_memo(&out_dir.join(OsStr::from_bytes(&line[..at])), &digest);
            }
        }
    }
    let stderr = fs::read(entry.join("stderr"))?;
    let mut output = io::stderr();
    output.write_all(&relocate(&stderr, &cached_out, current))?;
    output.flush()
}

#[allow(clippy::too_many_arguments)]
fn store(
    cache: &Path,
    entry: &Path,
    out_dir: &Path,
    registry: &Path,
    outputs: &[PathBuf],
    digests: &[u8],
    captured: &[u8],
    environment: &Environment,
) -> io::Result<()> {
    let Some(depinfo) = outputs.iter().find(|path| path.extension() == Some(OsStr::new("d"))) else {
        return Ok(());
    };
    let depinfo = fs::read(depinfo)?;
    let stable_root = cache.join("out");
    let mut names = Vec::new();
    for line in depinfo.split(|byte| *byte == b'\n') {
        if let Some(name) = line.strip_prefix(b"# env-dep:") {
            let end = name.iter().position(|byte| *byte == b'=').unwrap_or(name.len());
            names.push(name[..end].to_vec());
        } else if !line.starts_with(b"#") && line.ends_with(b":") {
            let path = &line[..line.len() - 1];
            let inside = path.starts_with(registry.as_os_str().as_bytes())
                || path.starts_with(stable_root.as_os_str().as_bytes());
            if !inside {
                return Ok(());
            }
        }
    }
    names.sort();
    names.dedup();
    let mut recorded = Vec::new();
    for name in &names {
        recorded.extend_from_slice(name);
        if let Some(value) = environment.get(OsStr::from_bytes(name)) {
            if value.as_bytes().contains(&b'\n') {
                return Ok(());
            }
            recorded.push(b'=');
            recorded.extend_from_slice(value.as_bytes());
        }
        recorded.push(b'\n');
    }
    fs::create_dir_all(cache.join("units"))?;
    let staging = cache.join("units").join(format!(".data.{}.{}", std::process::id(), unique()));
    let result = (|| -> io::Result<()> {
        fs::create_dir_all(staging.join("files"))?;
        for output in outputs {
            if let Some(name) = output.file_name() {
                clone(output, &staging.join("files").join(name))?;
            }
        }
        fs::write(staging.join("env"), &recorded)?;
        fs::write(staging.join("digests"), digests)?;
        fs::write(staging.join("stderr"), captured)?;
        let mut out = out_dir.as_os_str().as_bytes().to_vec();
        out.push(b'\n');
        fs::write(staging.join("out-dir"), out)?;
        symlink(&staging, entry)
    })();
    if result.is_err() {
        let _ = fs::remove_dir_all(&staging);
    }
    result
}

fn c(cache: &Path, args: &[OsString], environment: Environment) -> ExitCode {
    let (mut object, mut source, mut compile) = (None, None, false);
    for index in 1..args.len() {
        let arg = args[index].as_bytes();
        match args[index - 1].as_bytes() {
            b"-o" => object = Some(index),
            b"-c" => source = Some(index),
            _ => {}
        }
        if arg == b"-E" || arg.starts_with(b"-M") || (arg.starts_with(b"-o") && arg.len() > 2)
            || arg.starts_with(b"@") || arg == b"-"
        {
            return exec(args, &environment);
        }
        if arg == b"-c" {
            compile = true;
        }
    }
    let (Some(object), Some(source), true) = (object, source, compile) else {
        return exec(args, &environment);
    };
    if args[source].as_bytes().starts_with(b"-") {
        return exec(args, &environment);
    }
    let out_dir = environment.get(OsStr::new("OUT_DIR")).cloned().unwrap_or_default();
    let out_dir = out_dir.as_bytes().to_vec();
    let Ok(source_text) = fs::read(&args[source]) else {
        return exec(args, &environment);
    };
    if volatile(&source_text) {
        return exec(args, &environment);
    }
    let Ok(version) = Command::new(&args[0]).arg("--version").output() else {
        return exec(args, &environment);
    };
    let mut hasher = Sha256::new();
    hasher.update(C_KEY.as_bytes());
    hasher.update(b"\n");
    hasher.update(&version.stdout);
    hasher.update(&version.stderr);
    hasher.update(env::current_dir().unwrap_or_default().as_os_str().as_bytes());
    hasher.update(b"\n");
    for (index, arg) in args.iter().enumerate() {
        if index == object || index + 1 == object {
            continue;
        }
        hasher.update(&relocate(arg.as_bytes(), &out_dir, b"<out>"));
        hasher.update(b"\n");
    }
    for name in ["MACOSX_DEPLOYMENT_TARGET", "SDKROOT", "CPATH", "C_INCLUDE_PATH", "CPLUS_INCLUDE_PATH", "OBJC_INCLUDE_PATH"] {
        if let Some(value) = environment.get(OsStr::new(name)) {
            hasher.update(name.as_bytes());
            hasher.update(b"=");
            hasher.update(value.as_bytes());
            hasher.update(b"\n");
        }
    }
    let mut source_hasher = Sha256::new();
    source_hasher.update(&source_text);
    hasher.update(source_hasher.finish().as_bytes());
    let entry = cache.join("c").join(hasher.finish());
    let object = PathBuf::from(&args[object]);
    if headers_match(&entry, &out_dir)
        && clone(&entry.join("object"), &object).is_ok()
        && touch(&object).is_ok()
    {
        if let Ok(errors) = fs::read(entry.join("stderr")) {
            let _ = io::stderr().write_all(&errors);
        }
        return ExitCode::SUCCESS;
    }
    let mut depfile = object.clone().into_os_string();
    depfile.push(format!(".{}.buildcache.d", std::process::id()));
    let mut map = OsString::from("-ffile-prefix-map=");
    map.push(OsStr::from_bytes(&out_dir));
    map.push("=.");
    let output = match Command::new(&args[0])
        .args(&args[1..])
        .arg(&map)
        .arg("-MD")
        .arg("-MF")
        .arg(&depfile)
        .env_clear()
        .envs(&environment)
        .stderr(Stdio::piped())
        .output()
    {
        Ok(output) => output,
        Err(error) => {
            eprintln!("buildcache: could not run {}: {error}", args[0].to_string_lossy());
            return ExitCode::from(1);
        }
    };
    let _ = io::stdout().write_all(&output.stdout);
    let _ = io::stderr().write_all(&output.stderr);
    let dependencies = fs::read(&depfile);
    let _ = fs::remove_file(&depfile);
    if output.status.success()
        && let Ok(dependencies) = dependencies
        && let Some(manifest) = manifest(&dependencies, &out_dir)
    {
        let _ = store_object(cache, &entry, &object, &manifest, &output.stderr);
    }
    exit_code(output.status)
}

fn volatile(text: &[u8]) -> bool {
    [b"__DATE__" as &[u8], b"__TIME__", b"__TIMESTAMP__"]
        .iter()
        .any(|token| text.windows(token.len()).any(|window| window == *token))
}

fn dependency_paths(depfile: &[u8]) -> Vec<Vec<u8>> {
    let Some(colon) = depfile.windows(2).position(|window| window == b": ") else {
        return Vec::new();
    };
    let mut paths = Vec::new();
    let mut current = Vec::new();
    let mut bytes = depfile[colon + 2..].iter().peekable();
    while let Some(&byte) = bytes.next() {
        match byte {
            b'\\' if bytes.peek() == Some(&&b' ') => {
                current.push(b' ');
                bytes.next();
            }
            b'\\' if bytes.peek() == Some(&&b'\n') => {
                bytes.next();
            }
            b' ' | b'\n' | b'\t' | b'\r' => {
                if !current.is_empty() {
                    paths.push(std::mem::take(&mut current));
                }
            }
            _ => current.push(byte),
        }
    }
    if !current.is_empty() {
        paths.push(current);
    }
    paths
}

fn manifest(depfile: &[u8], out_dir: &[u8]) -> Option<Vec<u8>> {
    let mut manifest = Vec::new();
    for path in dependency_paths(depfile).into_iter().skip(1) {
        let contents = fs::read(OsStr::from_bytes(&path)).ok()?;
        if volatile(&contents) {
            return None;
        }
        let mut hasher = Sha256::new();
        hasher.update(&contents);
        manifest.extend_from_slice(hasher.finish().as_bytes());
        manifest.push(b' ');
        manifest.extend_from_slice(&relocate(&path, out_dir, b"<out>"));
        manifest.push(b'\n');
    }
    Some(manifest)
}

fn headers_match(entry: &Path, out_dir: &[u8]) -> bool {
    let Ok(manifest) = fs::read(entry.join("headers")) else {
        return false;
    };
    manifest.split(|byte| *byte == b'\n').filter(|line| !line.is_empty()).all(|line| {
        let Some(at) = line.iter().position(|byte| *byte == b' ') else {
            return false;
        };
        let path = relocate(&line[at + 1..], b"<out>", out_dir);
        digest_file(Path::new(OsStr::from_bytes(&path))).is_ok_and(|digest| digest.as_bytes() == &line[..at])
    })
}

fn store_object(cache: &Path, entry: &Path, object: &Path, manifest: &[u8], errors: &[u8]) -> io::Result<()> {
    fs::create_dir_all(cache.join("c"))?;
    let staging = cache.join("c").join(format!(".data.{}.{}", std::process::id(), unique()));
    let result = (|| -> io::Result<()> {
        fs::create_dir(&staging)?;
        clone(object, &staging.join("object"))?;
        fs::write(staging.join("headers"), manifest)?;
        fs::write(staging.join("stderr"), errors)?;
        if fs::symlink_metadata(entry).is_ok() {
            fs::remove_file(entry)?;
        }
        symlink(&staging, entry)
    })();
    if result.is_err() {
        let _ = fs::remove_dir_all(&staging);
    }
    result
}

struct Sha256 {
    state: [u32; 8],
    buffer: [u8; 64],
    filled: usize,
    length: u64,
}

const ROUND: [u32; 64] = [
    0x428a2f98, 0x71374491, 0xb5c0fbcf, 0xe9b5dba5, 0x3956c25b, 0x59f111f1, 0x923f82a4, 0xab1c5ed5,
    0xd807aa98, 0x12835b01, 0x243185be, 0x550c7dc3, 0x72be5d74, 0x80deb1fe, 0x9bdc06a7, 0xc19bf174,
    0xe49b69c1, 0xefbe4786, 0x0fc19dc6, 0x240ca1cc, 0x2de92c6f, 0x4a7484aa, 0x5cb0a9dc, 0x76f988da,
    0x983e5152, 0xa831c66d, 0xb00327c8, 0xbf597fc7, 0xc6e00bf3, 0xd5a79147, 0x06ca6351, 0x14292967,
    0x27b70a85, 0x2e1b2138, 0x4d2c6dfc, 0x53380d13, 0x650a7354, 0x766a0abb, 0x81c2c92e, 0x92722c85,
    0xa2bfe8a1, 0xa81a664b, 0xc24b8b70, 0xc76c51a3, 0xd192e819, 0xd6990624, 0xf40e3585, 0x106aa070,
    0x19a4c116, 0x1e376c08, 0x2748774c, 0x34b0bcb5, 0x391c0cb3, 0x4ed8aa4a, 0x5b9cca4f, 0x682e6ff3,
    0x748f82ee, 0x78a5636f, 0x84c87814, 0x8cc70208, 0x90befffa, 0xa4506ceb, 0xbef9a3f7, 0xc67178f2,
];

impl Sha256 {
    const fn new() -> Self {
        Self {
            state: [
                0x6a09e667, 0xbb67ae85, 0x3c6ef372, 0xa54ff53a, 0x510e527f, 0x9b05688c, 0x1f83d9ab,
                0x5be0cd19,
            ],
            buffer: [0; 64],
            filled: 0,
            length: 0,
        }
    }

    fn update(&mut self, mut data: &[u8]) {
        self.length += data.len() as u64;
        if self.filled > 0 {
            let take = (64 - self.filled).min(data.len());
            self.buffer[self.filled..self.filled + take].copy_from_slice(&data[..take]);
            self.filled += take;
            data = &data[take..];
            if self.filled < 64 {
                return;
            }
            let block = self.buffer;
            self.compress(&block);
            self.filled = 0;
        }
        let mut chunks = data.chunks_exact(64);
        for block in &mut chunks {
            self.compress(block.try_into().expect("64-byte block"));
        }
        let rest = chunks.remainder();
        self.buffer[..rest.len()].copy_from_slice(rest);
        self.filled = rest.len();
    }

    fn compress(&mut self, block: &[u8; 64]) {
        let mut words = [0u32; 64];
        for (index, word) in words.iter_mut().take(16).enumerate() {
            *word = u32::from_be_bytes(block[index * 4..index * 4 + 4].try_into().expect("word"));
        }
        for index in 16..64 {
            let small0 = words[index - 15].rotate_right(7) ^ words[index - 15].rotate_right(18) ^ (words[index - 15] >> 3);
            let small1 = words[index - 2].rotate_right(17) ^ words[index - 2].rotate_right(19) ^ (words[index - 2] >> 10);
            words[index] = words[index - 16]
                .wrapping_add(small0)
                .wrapping_add(words[index - 7])
                .wrapping_add(small1);
        }
        let [mut a, mut b, mut c, mut d, mut e, mut f, mut g, mut h] = self.state;
        for index in 0..64 {
            let big1 = e.rotate_right(6) ^ e.rotate_right(11) ^ e.rotate_right(25);
            let choose = (e & f) ^ (!e & g);
            let first = h
                .wrapping_add(big1)
                .wrapping_add(choose)
                .wrapping_add(ROUND[index])
                .wrapping_add(words[index]);
            let big0 = a.rotate_right(2) ^ a.rotate_right(13) ^ a.rotate_right(22);
            let majority = (a & b) ^ (a & c) ^ (b & c);
            let second = big0.wrapping_add(majority);
            h = g;
            g = f;
            f = e;
            e = d.wrapping_add(first);
            d = c;
            c = b;
            b = a;
            a = first.wrapping_add(second);
        }
        for (state, value) in self.state.iter_mut().zip([a, b, c, d, e, f, g, h]) {
            *state = state.wrapping_add(value);
        }
    }

    fn finish(mut self) -> String {
        let bits = self.length.wrapping_mul(8);
        let mut padding = vec![0x80u8];
        let used = (self.length % 64) as usize;
        let zeros = if used < 56 { 55 - used } else { 119 - used };
        padding.extend(std::iter::repeat_n(0, zeros));
        padding.extend_from_slice(&bits.to_be_bytes());
        let length = self.length;
        self.update(&padding);
        self.length = length;
        self.state.iter().map(|word| format!("{word:08x}")).collect()
    }
}
