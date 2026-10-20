use nanocode_home::SessionEnvironment;
use serde::Serialize;
use std::{ffi::OsString, future::Future, path::Path, pin::Pin, sync::Arc, time::Duration};

#[cfg(not(target_family = "wasm"))]
pub type ProcessFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;
#[cfg(target_family = "wasm")]
pub type ProcessFuture<'a, T> = Pin<Box<dyn Future<Output = T> + 'a>>;

pub struct ExecCommand {
    pub script: String,
    pub workdir: Option<String>,
    pub shell: Option<String>,
    pub login: Option<bool>,
    pub tty: bool,
    pub yield_time_ms: Option<u64>,
    pub max_output_tokens: Option<usize>,
}

impl ExecCommand {
    pub const fn new(
        script: String,
        workdir: Option<String>,
        shell: Option<String>,
        login: Option<bool>,
        tty: bool,
        yield_time_ms: Option<u64>,
        max_output_tokens: Option<usize>,
    ) -> Self {
        Self {
            script,
            workdir,
            shell,
            login,
            tty,
            yield_time_ms,
            max_output_tokens,
        }
    }
}

pub struct WriteStdin {
    pub session_id: i32,
    pub chars: String,
    pub yield_time_ms: Option<u64>,
    pub max_output_tokens: Option<usize>,
}

impl WriteStdin {
    pub const fn new(
        session_id: i32,
        chars: String,
        yield_time_ms: Option<u64>,
        max_output_tokens: Option<usize>,
    ) -> Self {
        Self {
            session_id,
            chars,
            yield_time_ms,
            max_output_tokens,
        }
    }
}

#[derive(Serialize)]
pub struct ExecCommandResult {
    #[serde(skip)]
    pub error: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub chunk_id: Option<String>,
    pub wall_time_seconds: f64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub exit_code: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub session_id: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub original_token_count: Option<usize>,
    pub output: String,
}

pub struct ForegroundOutput {
    pub stdout: String,
    pub stderr: String,
    pub exit_code: i32,
    pub truncated: bool,
}

pub trait ShellExecutor: Send + Sync {
    fn execute<'a>(
        &'a self,
        command: ExecCommand,
        workspace: &'a Path,
    ) -> ProcessFuture<'a, ExecCommandResult>;
    fn write_stdin(&self, request: WriteStdin) -> ProcessFuture<'_, ExecCommandResult>;
    fn terminate_all(&self) -> ProcessFuture<'_, ()>;
}

pub trait OutputSink: Send + Sync {
    fn write(&self, bytes: &[u8]);
}

pub struct ForegroundCommand<'a> {
    pub workspace: &'a Path,
    pub command: &'a str,
    pub timeout: Duration,
    pub max_stdout: usize,
    pub max_stderr: usize,
    pub session: Option<&'a SessionEnvironment>,
    pub output: Option<&'a dyn OutputSink>,
}

pub trait ForegroundExecutor: Send + Sync {
    fn execute<'a>(
        &'a self,
        command: ForegroundCommand<'a>,
    ) -> ProcessFuture<'a, Result<ForegroundOutput, String>>;
}

pub trait ProcessExecutors: Send + Sync {
    fn shell(&self, environment: Vec<(OsString, OsString)>) -> Arc<dyn ShellExecutor>;
    fn foreground(&self) -> Arc<dyn ForegroundExecutor>;
}
impl ExecCommandResult {
    pub fn failed(wall_time: Duration, output: String) -> Self {
        Self {
            error: Some(output.clone()),
            chunk_id: None,
            wall_time_seconds: wall_time.as_secs_f64(),
            exit_code: Some(1),
            session_id: None,
            original_token_count: None,
            output,
        }
    }
}

pub const DEFAULT_EXEC_YIELD_MS: u64 = 10_000;
pub const DEFAULT_WRITE_YIELD_MS: u64 = 250;
pub const DEFAULT_POLL_YIELD_MS: u64 = 5_000;
pub const MAX_EXEC_YIELD_MS: u64 = 30_000;
pub const MAX_POLL_YIELD_MS: u64 = 300_000;
