use std::{
    env,
    ffi::{OsStr, OsString},
    io::{self, Read, Write},
    path::Path,
    process::Stdio,
    sync::{Arc, Mutex as StdMutex},
};

#[cfg(unix)]
use nix::{
    errno::Errno,
    sys::signal::{Signal, killpg},
    unistd::Pid,
};
use portable_pty::{CommandBuilder, PtySize, native_pty_system};
use tokio::process::{ChildStderr, ChildStdout, Command};
use tokio::task::JoinHandle;

use super::selection::Shell;

const SENSITIVE_ENV_PARTS: [&str; 11] = [
    "AUTH",
    "AUTHORIZATION",
    "COOKIE",
    "CREDENTIAL",
    "CREDENTIALS",
    "KEY",
    "PASS",
    "PASSWD",
    "PASSWORD",
    "SECRET",
    "TOKEN",
];

const NORMALIZED_ENVIRONMENT: [(&str, &str); 10] = [
    ("NO_COLOR", "1"),
    ("TERM", "dumb"),
    ("LANG", "C.UTF-8"),
    ("LC_CTYPE", "C.UTF-8"),
    ("LC_ALL", "C.UTF-8"),
    ("COLORTERM", ""),
    ("PAGER", "cat"),
    ("GIT_PAGER", "cat"),
    ("GH_PAGER", "cat"),
    ("CODEX_CI", "1"),
];

pub(super) struct SpawnedProcess {
    pub(super) child: ProcessChild,
    pub(super) stdin: Option<ProcessStdin>,
    pub(super) output: ProcessOutput,
    pub(super) process_group: SharedProcessGroup,
}

pub(super) struct ProcessChild {
    wait: Option<JoinHandle<io::Result<i32>>>,
    completed: Option<io::Result<i32>>,
    process_group: SharedProcessGroup,
}

impl ProcessChild {
    fn new(
        mut try_wait: impl FnMut() -> io::Result<Option<i32>> + Send + 'static,
        process_group: SharedProcessGroup,
        io_driver_available: bool,
    ) -> Self {
        #[cfg(not(unix))]
        let _ = io_driver_available;
        let group = process_group.clone();
        let wait = tokio::spawn(async move {
            #[cfg(unix)]
            let mut exited = if io_driver_available {
                match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::child()) {
                    Ok(signal) => Some(signal),
                    Err(error) => {
                        tracing::warn!(%error, "shell exit notifications unavailable; using polling");
                        None
                    }
                }
            } else {
                None
            };
            loop {
                {
                    let mut guard = group.0.lock().unwrap_or_else(|e| e.into_inner());

                    let ready = guard.prepare_to_reap();
                    match ready.and_then(|ready| if ready { try_wait() } else { Ok(None) }) {
                        Ok(None) => {}
                        Ok(Some(status)) => {
                            guard.disarm();
                            return Ok(status);
                        }

                        Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
                        Err(error) => {
                            guard.disarm();
                            return Err(error);
                        }
                    }
                }
                #[cfg(unix)]
                if let Some(signal) = exited.as_mut() {
                    if signal.recv().await.is_none() {
                        exited = None;
                    }
                    continue;
                }

                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        });
        Self {
            wait: Some(wait),
            completed: None,
            process_group,
        }
    }

    pub(super) async fn wait(&mut self) -> io::Result<i32> {
        if self.completed.is_none() {
            let result = self
                .wait
                .as_mut()
                .ok_or_else(|| io::Error::other("shell wait result is unavailable"))?
                .await
                .map_err(|error| io::Error::other(format!("shell wait task failed: {error}")))
                .and_then(|result| result);
            self.wait = None;
            self.completed = Some(result);
        }
        match self.completed.as_ref().expect("completed wait") {
            Ok(status) => Ok(*status),
            Err(error) => Err(io::Error::new(error.kind(), error.to_string())),
        }
    }
}

impl Drop for ProcessChild {
    fn drop(&mut self) {
        let _ = self.process_group.terminate_and_disarm();
    }
}

#[derive(Clone)]
pub(super) struct SharedProcessGroup(Arc<StdMutex<ProcessGroupGuard>>);

impl SharedProcessGroup {
    fn new(pid: u32) -> Self {
        Self(Arc::new(StdMutex::new(ProcessGroupGuard::new(pid))))
    }

    pub(super) fn interrupt(&self) -> io::Result<()> {
        self.0.lock().unwrap_or_else(|e| e.into_inner()).interrupt()
    }

    pub(super) fn terminate_and_disarm(&self) -> io::Result<()> {
        self.0
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .terminate_and_disarm()
    }
}

pub(super) enum ProcessStdin {
    Pty(Arc<StdMutex<Box<dyn Write + Send>>>),
}

impl ProcessStdin {
    pub(super) async fn write(&mut self, bytes: &[u8]) -> io::Result<()> {
        match self {
            Self::Pty(writer) => {
                let writer = Arc::clone(writer);
                let bytes = bytes.to_vec();
                tokio::task::spawn_blocking(move || {
                    let mut writer = writer
                        .lock()
                        .map_err(|_| io::Error::other("PTY writer lock poisoned"))?;
                    writer.write_all(&bytes)?;
                    writer.flush()
                })
                .await
                .map_err(|error| io::Error::other(format!("PTY write task failed: {error}")))?
            }
        }
    }
}

pub(super) enum ProcessOutput {
    Pipes {
        stdout: Option<ChildStdout>,
        stderr: Option<ChildStderr>,
    },
    Pty(Box<dyn Read + Send>),
}

pub(super) fn spawn(
    script: &str,
    workspace: &Path,
    shell: &Shell,
    login: bool,
    tty: bool,
    environment: &[(OsString, OsString)],
) -> io::Result<SpawnedProcess> {
    if tty {
        return spawn_pty(script, workspace, shell, login, environment);
    }

    spawn_pipes(script, workspace, shell, login, environment)
}

fn spawn_pipes(
    script: &str,
    workspace: &Path,
    shell: &Shell,
    login: bool,
    environment: &[(OsString, OsString)],
) -> io::Result<SpawnedProcess> {
    let mut command = Command::new(shell.path());
    command
        .args(shell.args(script, login))
        .current_dir(workspace)
        .env_clear()
        .envs(environment.iter().cloned())
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    #[cfg(unix)]
    command.process_group(0);

    let mut child = command.spawn()?;
    let pid = child
        .id()
        .ok_or_else(|| io::Error::other("spawned shell without a process identifier"))?;
    let process_group = SharedProcessGroup::new(pid);
    Ok(SpawnedProcess {
        stdin: None,
        output: ProcessOutput::Pipes {
            stdout: child.stdout.take(),
            stderr: child.stderr.take(),
        },
        child: ProcessChild::new(
            move || child.try_wait().map(|status| status.map(exit_code)),
            process_group.clone(),
            true,
        ),
        process_group,
    })
}

fn spawn_pty(
    script: &str,
    workspace: &Path,
    shell: &Shell,
    login: bool,
    environment: &[(OsString, OsString)],
) -> io::Result<SpawnedProcess> {
    let pty_system = native_pty_system();
    let pair = pty_system
        .openpty(PtySize {
            rows: 24,
            cols: 80,
            pixel_width: 0,
            pixel_height: 0,
        })
        .map_err(pty_error)?;
    let mut command = CommandBuilder::new(shell.path());
    for argument in shell.args(script, login) {
        command.arg(argument);
    }
    command.cwd(workspace);
    command.env_clear();
    for (name, value) in environment {
        command.env(name, value);
    }

    let reader = pair.master.try_clone_reader().map_err(pty_error)?;
    let writer = pair.master.take_writer().map_err(pty_error)?;
    let mut child = pair.slave.spawn_command(command).map_err(pty_error)?;
    let pid = child
        .process_id()
        .ok_or_else(|| io::Error::other("spawned PTY command without a process identifier"))?;
    let process_group = SharedProcessGroup::new(pid);
    let child = ProcessChild::new(
        move || {
            child.try_wait().map(|status| {
                status.map(|status| i32::try_from(status.exit_code()).unwrap_or(i32::MAX))
            })
        },
        process_group.clone(),
        false,
    );

    Ok(SpawnedProcess {
        child,
        stdin: Some(ProcessStdin::Pty(Arc::new(StdMutex::new(writer)))),
        output: ProcessOutput::Pty(reader),
        process_group,
    })
}

fn pty_error(error: impl std::fmt::Display) -> io::Error {
    io::Error::other(error.to_string())
}

#[cfg(unix)]
fn exit_code(status: std::process::ExitStatus) -> i32 {
    use std::os::unix::process::ExitStatusExt;

    status
        .code()
        .or_else(|| status.signal().map(|signal| 128_i32.saturating_add(signal)))
        .unwrap_or(1)
}

#[cfg(not(unix))]
fn exit_code(status: std::process::ExitStatus) -> i32 {
    status.code().unwrap_or(1)
}

pub struct ProcessGroupGuard {
    #[cfg(unix)]
    process_group: Option<Pid>,
    #[cfg(not(unix))]
    process_group: Option<u32>,
}

impl ProcessGroupGuard {
    pub fn new(pid: u32) -> Self {
        #[cfg(unix)]
        let process_group = i32::try_from(pid).ok().map(Pid::from_raw);
        #[cfg(not(unix))]
        let process_group = Some(pid);
        Self { process_group }
    }

    #[cfg(all(
        unix,
        not(any(
            target_os = "openbsd",
            target_os = "redox",
            target_os = "horizon",
            target_os = "cygwin"
        ))
    ))]
    fn prepare_to_reap(&mut self) -> io::Result<bool> {
        use rustix::process::{WaitId, WaitIdOptions, waitid};

        let Some(group) = self.process_group else {
            return Ok(true);
        };
        let pid = rustix::process::Pid::from_raw(group.as_raw())
            .ok_or_else(|| io::Error::other("invalid shell process identifier"))?;
        match waitid(
            WaitId::Pid(pid),
            WaitIdOptions::EXITED | WaitIdOptions::NOHANG | WaitIdOptions::NOWAIT,
        ) {
            Ok(None) => Ok(false),
            Ok(Some(_)) => {
                let _ = self.terminate_and_disarm();
                Ok(true)
            }
            Err(error) => Err(error.into()),
        }
    }

    #[cfg(not(all(
        unix,
        not(any(
            target_os = "openbsd",
            target_os = "redox",
            target_os = "horizon",
            target_os = "cygwin"
        ))
    )))]
    fn prepare_to_reap(&mut self) -> io::Result<bool> {
        Ok(true)
    }

    #[cfg(unix)]
    pub(super) fn interrupt(&self) -> io::Result<()> {
        let Some(process_group) = self.process_group else {
            return Ok(());
        };
        match killpg(process_group, Signal::SIGINT) {
            Ok(()) | Err(Errno::ESRCH) => Ok(()),
            Err(error) => Err(io::Error::from_raw_os_error(error as i32)),
        }
    }

    #[cfg(not(unix))]
    pub(super) fn interrupt(&self) -> io::Result<()> {
        self.terminate()
    }

    #[cfg(unix)]
    fn terminate(&self) -> io::Result<()> {
        let Some(process_group) = self.process_group else {
            return Ok(());
        };
        match killpg(process_group, Signal::SIGKILL) {
            Ok(()) | Err(Errno::ESRCH) => Ok(()),
            Err(error) => Err(io::Error::from_raw_os_error(error as i32)),
        }
    }

    #[cfg(windows)]
    fn terminate(&self) -> io::Result<()> {
        let Some(process_group) = self.process_group else {
            return Ok(());
        };

        std::process::Command::new("taskkill.exe")
            .args(["/PID", &process_group.to_string(), "/T", "/F"])
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .map(|_| ())
    }

    #[cfg(not(any(unix, windows)))]
    fn terminate(&self) -> io::Result<()> {
        Ok(())
    }

    pub(super) const fn disarm(&mut self) {
        self.process_group = None;
    }

    pub fn terminate_and_disarm(&mut self) -> io::Result<()> {
        let result = self.terminate();
        self.disarm();
        result
    }
}

impl Drop for ProcessGroupGuard {
    fn drop(&mut self) {
        let _ = self.terminate();
    }
}

pub(super) fn sanitized_environment(
    overrides: &[(OsString, OsString)],
) -> (Vec<(OsString, OsString)>, Vec<String>) {
    let mut environment = Vec::new();
    let mut secrets = Vec::new();
    for (name, value) in env::vars_os() {
        if crate::SessionEnvironment::controls(&name) {
            continue;
        }
        if is_sensitive_name(&name) {
            if let Some(value) = value.to_str().filter(|value| value.len() >= 8) {
                secrets.push(value.to_owned());
            }
        } else {
            environment.push((name, value));
        }
    }
    normalize_environment(&mut environment);
    for (name, value) in overrides {
        environment.retain(|(candidate, _)| candidate != name);
        environment.push((name.clone(), value.clone()));
        if is_sensitive_name(name)
            && let Some(value) = value.to_str().filter(|value| value.len() >= 8)
        {
            secrets.push(value.to_owned());
        }
    }
    secrets.sort_unstable_by_key(|secret| std::cmp::Reverse(secret.len()));
    secrets.dedup();
    (environment, secrets)
}

fn normalize_environment(environment: &mut Vec<(OsString, OsString)>) {
    for (name, value) in NORMALIZED_ENVIRONMENT {
        environment.retain(|(candidate, _)| candidate != name);
        environment.push((name.into(), value.into()));
    }
}

#[must_use]
pub fn ambient_sensitive_environment() -> Vec<(OsString, OsString)> {
    env::vars_os()
        .filter(|(name, _)| is_sensitive_name(name))
        .collect()
}

fn is_sensitive_name(name: &OsStr) -> bool {
    name.to_string_lossy()
        .to_ascii_uppercase()
        .split('_')
        .any(|part| SENSITIVE_ENV_PARTS.contains(&part))
}
