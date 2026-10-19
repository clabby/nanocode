use std::process::Stdio;

use nanocode::{
    claude::{ClaudeHomeLogin, subscription::ClaudeSubscriptionStatus},
    codex::auth::{ChatGptLogin, chatgpt_auth_file},
};
use nanocode_tui::transcript::NoticeLevel;
use tokio::sync::mpsc::UnboundedSender;

use crate::app::{App, AppEvent};

impl App {
    pub(crate) fn login(&mut self, argument: &str) {
        let _span = tracing::info_span!(target: "nanocode", "nc.action", action.name = "login", session.id = self.agent.as_ref().map(|agent| agent.session_id())).entered();
        let tx = self.tx.clone();
        match argument.trim() {
            "codex" | "chatgpt" | "openai" => {
                tokio::spawn(codex(tx));
            }
            "claude" | "anthropic" => {
                tokio::spawn(claude(tx));
            }
            _ => self.notice(NoticeLevel::Info, "usage: /login codex|claude"),
        }
    }
}

async fn codex(tx: UnboundedSender<AppEvent>) {
    let notice = |level, text| drop(tx.send(AppEvent::Notice(level, text)));
    let path = match chatgpt_auth_file() {
        Ok(path) => path,
        Err(error) => return notice(NoticeLevel::Error, format!("login codex: {error}")),
    };
    let login = match ChatGptLogin::start(path).await {
        Ok(login) => login,
        Err(error) => return notice(NoticeLevel::Error, format!("login codex: {error}")),
    };
    notice(
        NoticeLevel::Info,
        browse("ChatGPT", login.authorization_url()),
    );
    match login.complete().await {
        Ok(status) => notice(
            NoticeLevel::Info,
            format!(
                "signed in to ChatGPT as {}",
                status.email.unwrap_or(status.account_id)
            ),
        ),
        Err(error) => notice(NoticeLevel::Error, format!("login codex: {error}")),
    }
}

async fn claude(tx: UnboundedSender<AppEvent>) {
    let notice = |level, text| drop(tx.send(AppEvent::Notice(level, text)));
    let login = match ClaudeHomeLogin::start().await {
        Ok(login) => login,
        Err(error) => return notice(NoticeLevel::Error, format!("login claude: {error}")),
    };
    notice(
        NoticeLevel::Info,
        browse("Claude", login.authorization_url()),
    );
    match login.complete().await {
        Ok(ClaudeSubscriptionStatus::Authenticated { .. }) => {
            notice(NoticeLevel::Info, "signed in to Claude".to_owned());
        }
        Ok(status) => notice(
            NoticeLevel::Error,
            format!("login claude ended as {status:?}"),
        ),
        Err(error) => notice(NoticeLevel::Error, format!("login claude: {error}")),
    }
}

fn browse(service: &str, url: &str) -> String {
    if open(url) {
        format!("opened {service} sign-in in your browser; if it did not appear, visit {url}")
    } else {
        format!("visit {url} to sign in to {service}")
    }
}

fn open(url: &str) -> bool {
    let mut command = if cfg!(target_os = "macos") {
        tokio::process::Command::new("open")
    } else if cfg!(windows) {
        let mut command = tokio::process::Command::new("rundll32");
        command.arg("url.dll,FileProtocolHandler");
        command
    } else {
        tokio::process::Command::new("xdg-open")
    };
    command
        .arg(url)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .is_ok()
}
