use std::io::Read;

use tokio::{
    io::{AsyncRead, AsyncReadExt},
    sync::mpsc::UnboundedSender,
};

const BYTES_PER_TOKEN: usize = 4;
const READ_BUFFER_LENGTH: usize = 8 * 1024;
const REDACTION: &str = "[REDACTED]";

pub(super) fn effective_token_limit(requested: Option<usize>) -> usize {
    requested.map_or(usize::MAX, |value| value.saturating_mul(BYTES_PER_TOKEN))
}

pub(super) async fn drain(
    mut pipe: Option<impl AsyncRead + Unpin>,
    chunks: UnboundedSender<Vec<u8>>,
) {
    let Some(mut pipe) = pipe.take() else {
        return;
    };
    let mut buffer = [0_u8; READ_BUFFER_LENGTH];
    loop {
        match pipe.read(&mut buffer).await {
            Ok(0) => return,
            Ok(length) => {
                if chunks.send(buffer[..length].to_vec()).is_err() {
                    return;
                }
            }
            Err(error) => {
                let _ = chunks.send(format!("failed to read command output: {error}").into_bytes());
                return;
            }
        }
    }
}

pub(super) fn drain_blocking(
    mut pipe: Box<dyn Read + Send>,
    chunks: UnboundedSender<Vec<u8>>,
) -> tokio::task::JoinHandle<()> {
    tokio::task::spawn_blocking(move || {
        let mut buffer = [0_u8; READ_BUFFER_LENGTH];
        loop {
            match pipe.read(&mut buffer) {
                Ok(0) => return,
                Ok(length) => {
                    if chunks.send(buffer[..length].to_vec()).is_err() {
                        return;
                    }
                }
                Err(error) if error.kind() == std::io::ErrorKind::Interrupted => {}

                Err(_) => return,
            }
        }
    })
}

pub(super) fn redact_and_limit(
    mut output: String,
    secrets: &[String],
    limit: usize,
) -> (String, bool) {
    for secret in secrets {
        output = output.replace(secret, REDACTION);
    }
    if output.len() <= limit {
        return (output, false);
    }

    let original_token_count = output.len().saturating_add(3) / BYTES_PER_TOKEN;
    let total_lines = output.lines().count();
    let left_budget = limit / 2;
    let right_budget = limit.saturating_sub(left_budget);
    let prefix_end = output.floor_char_boundary(left_budget);
    let suffix_start = output
        .ceil_char_boundary(output.len().saturating_sub(right_budget))
        .max(prefix_end);
    let removed_tokens = output.len().saturating_sub(limit).saturating_add(3) / BYTES_PER_TOKEN;
    let truncated = format!(
        "Warning: truncated output (original token count: {original_token_count})\nTotal output lines: {total_lines}\n\n{}…{removed_tokens} tokens truncated…{}",
        &output[..prefix_end],
        &output[suffix_start..],
    );
    (truncated, true)
}
