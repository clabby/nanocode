use crate::{Connection, steps::Tabs};
use serde_json::{Value, json};
use std::{
    io::{BufRead, BufReader, Write},
    os::unix::net::UnixStream,
    path::Path,
    sync::mpsc,
    thread,
    time::Duration,
};

/// Lines from the host's socket held before its reader waits.
const HOST_LINES: usize = 64;

/// How one attempt to reach the host's socket ended.
#[derive(PartialEq, Eq)]
enum Relayed {
    /// The host isn't listening or isn't ready: try again after a pause.
    Retry,
    /// The connection ended; reconnect right away.
    Closed,
}

impl Connection {
    /// Serves browser steps for a host process listening on the Unix socket `socket`, reconnecting
    /// while the connection is [`live`](Self::live), then waits for the browser to disconnect, as
    /// [`finish`](Self::finish). The socket carries newline-delimited JSON: the host sends
    /// `{"type":"hello"}` first, then `{"type":"call","id","session","turn","input"}` for a step,
    /// answered with `{"type":"result","id","ok":true,"summary","result"}` or
    /// `{"type":"result","id","ok":false,"error"}`, and `{"type":"turn_ended","session","turn"}`
    /// for [`end_turn`](Self::end_turn). The step input field named `title`, when the host sends it,
    /// names the session's tab group (see [`name_session`](Self::name_session)). Turns without a
    /// step for `idle` end as [`end_idle`](Self::end_idle).
    pub fn relay(mut self, socket: &Path, idle: Duration, title: &str) {
        while self.live() {
            if self.relay_once(socket, idle, title) == Relayed::Retry {
                thread::sleep(Duration::from_secs(2));
            }
        }
        self.finish();
    }

    /// Handles one line from the host: the result frame for a step call, or `None` for a turn's
    /// end and anything that needs no answer.
    fn answer(&mut self, tabs: &mut Tabs, line: &str, title: &str) -> Option<Value> {
        let call = serde_json::from_str::<Value>(line).ok()?;
        if call["type"] == "turn_ended" {
            if let (Some(session), Some(turn)) = (call["session"].as_str(), call["turn"].as_str()) {
                let _ = self.end_turn(tabs, session, turn);
            }
            return None;
        }
        if call["type"] != "call" {
            return None;
        }
        let step = match call["session"].as_str() {
            Some(session) => {
                let turn = call["turn"].as_str();
                if let Some(title) = call["input"][title].as_str() {
                    let _ = self.name_session(tabs, session, turn, title);
                }
                self.step(tabs, session, turn, &call["input"])
            }
            None => Err("invalid browser session".into()),
        };
        Some(match step {
            Ok((summary, result)) => {
                json!({"type":"result","id":call["id"],"ok":true,"summary":summary,"result":result})
            }
            Err(error) => {
                json!({"type":"result","id":call["id"],"ok":false,"error":error.to_string()})
            }
        })
    }

    /// Connects to the host's socket and answers its steps while the connection is live. Its
    /// reader thread ends before this returns.
    fn relay_once(&mut self, socket: &Path, idle: Duration, title: &str) -> Relayed {
        let Ok(stream) = UnixStream::connect(socket) else {
            return Relayed::Retry;
        };
        let Ok(mut writer) = stream.try_clone() else {
            return Relayed::Retry;
        };
        let (tx, rx) = mpsc::sync_channel(HOST_LINES);
        let reading = thread::spawn(move || {
            for line in BufReader::new(stream).lines() {
                if tx.send(line).is_err() {
                    break;
                }
            }
        });
        let relayed = 'session: {
            let first = match rx.recv_timeout(Duration::from_secs(3)) {
                Ok(Ok(line)) => match serde_json::from_str::<Value>(&line) {
                    Ok(value) => value,
                    Err(_) => break 'session Relayed::Closed,
                },
                _ => break 'session Relayed::Closed,
            };
            if first["type"] != "hello" {
                break 'session Relayed::Retry;
            }
            let mut tabs = Tabs::default();
            while self.live() {
                match rx.recv_timeout(Duration::from_millis(250)) {
                    Ok(Ok(line)) => {
                        let Some(answer) = self.answer(&mut tabs, &line, title) else {
                            continue;
                        };
                        if writer.write_all(answer.to_string().as_bytes()).is_err()
                            || writer.write_all(b"\n").is_err()
                        {
                            break;
                        }
                    }
                    Ok(Err(_)) | Err(mpsc::RecvTimeoutError::Disconnected) => break,
                    Err(mpsc::RecvTimeoutError::Timeout) => {
                        let _ = self.end_idle(&mut tabs, idle);
                    }
                }
            }
            // Tab ownership is forgotten with the connection: never attach to old tabs after
            // reconnecting to the host.
            Relayed::Closed
        };
        // Closing the socket and the receiver ends the reader: one runs at a time.
        let _ = writer.shutdown(std::net::Shutdown::Both);
        drop(rx);
        let _ = reading.join();
        relayed
    }
}
