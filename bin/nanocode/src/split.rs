use std::{
    env,
    ffi::{OsStr, OsString},
    path::{Path, PathBuf},
    process::{Command, Stdio},
};

pub struct Split {
    backend: Backend,
    executable: PathBuf,
}

enum Backend {
    Tmux(Option<OsString>),
    Zellij,
    WezTerm(OsString),
    WindowsTerminal(bool),
    Iterm,
    AppleTerminal,
    MacApp(&'static str, &'static [&'static str]),
    Window(&'static str, &'static [&'static str]),
}

const MAC_APPS: [(&str, &str, &[&str]); 3] = [
    ("ghostty", "Ghostty.app", &["-e"]),
    ("kitty", "kitty.app", &[]),
    ("wezterm", "WezTerm.app", &["start", "--"]),
];

const WINDOWS: [(&str, &str, &[&str]); 10] = [
    ("ghostty", "ghostty", &["-e"]),
    ("kitty", "kitty", &["--detach"]),
    ("wezterm", "wezterm", &["start", "--"]),
    ("alacritty", "alacritty", &["-e"]),
    ("gnome", "gnome-terminal", &["--"]),
    ("konsole", "konsole", &["-e"]),
    ("foot", "footclient", &[]),
    ("foot", "foot", &[]),
    ("xterm", "xterm", &["-e"]),
    ("", "x-terminal-emulator", &["-e"]),
];

const ITERM_SCRIPT: &str = r#"on run argv
tell application "iTerm2"
    tell current session of current window
        split vertically with same profile command (item 1 of argv)
    end tell
end tell
end run"#;

const TERMINAL_SCRIPT: &str = r#"on run argv
tell application "Terminal"
    activate
    do script (item 1 of argv)
end tell
end run"#;

impl Split {
    pub fn detect() -> Result<Self, String> {
        let executable = env::current_exe()
            .map_err(|error| format!("could not locate the nanocode executable: {error}"))?;
        Ok(Self {
            backend: detect()?,
            executable,
        })
    }

    pub fn launch(
        &self,
        session: &str,
        home: &Path,
        workspace: &Path,
    ) -> Result<&'static str, String> {
        let arguments = [
            self.executable.as_os_str(),
            OsStr::new("--home"),
            home.as_os_str(),
            OsStr::new("-C"),
            workspace.as_os_str(),
            OsStr::new("resume"),
            OsStr::new(session),
        ]
        .map(OsString::from);
        let shell = || shell_command(&arguments);
        let (program, mut command, wait): (OsString, Vec<OsString>, bool) = match &self.backend {
            Backend::Tmux(pane) => {
                let mut command = words(["split-window", "-d", "-h"]);
                if let Some(pane) = pane {
                    command.extend([OsString::from("-t"), pane.clone()]);
                }
                command.push(shell()?);
                ("tmux".into(), command, true)
            }
            Backend::Zellij => (
                "zellij".into(),
                words([
                    "action",
                    "new-pane",
                    "--direction",
                    "right",
                    "--close-on-exit",
                    "--",
                ]),
                true,
            ),
            Backend::WezTerm(pane) => {
                let mut command = words(["cli", "split-pane", "--right", "--pane-id"]);
                command.extend([pane.clone(), OsString::from("--")]);
                ("wezterm".into(), command, true)
            }
            Backend::WindowsTerminal(pane) => (
                "wt.exe".into(),
                if *pane {
                    words(["-w", "0", "split-pane", "-V"])
                } else {
                    words(["-w", "new", "new-tab"])
                },
                true,
            ),
            Backend::Iterm => (
                "/usr/bin/osascript".into(),
                vec!["-e".into(), ITERM_SCRIPT.into(), shell()?],
                true,
            ),
            Backend::AppleTerminal => (
                "/usr/bin/osascript".into(),
                vec!["-e".into(), TERMINAL_SCRIPT.into(), shell()?],
                true,
            ),
            Backend::MacApp(app, prefix) => {
                let mut command = words(["-na", app, "--args"]);
                command.extend(prefix.iter().map(OsString::from));
                ("open".into(), command, true)
            }
            Backend::Window(program, prefix) => (
                (*program).into(),
                prefix.iter().map(OsString::from).collect(),
                false,
            ),
        };
        if !matches!(
            self.backend,
            Backend::Tmux(_) | Backend::Iterm | Backend::AppleTerminal
        ) {
            command.extend(arguments);
        }
        run(&program, &command, wait)?;
        Ok(self.backend.destination())
    }
}

impl Backend {
    const fn destination(&self) -> &'static str {
        match self {
            Self::Tmux(_) => "a tmux pane on the right",
            Self::Zellij => "a Zellij pane on the right",
            Self::WezTerm(_) => "a WezTerm pane on the right",
            Self::WindowsTerminal(true) => "a Windows Terminal pane",
            Self::WindowsTerminal(false) => "a new Windows Terminal window",
            Self::Iterm => "an iTerm2 pane on the right",
            Self::AppleTerminal => "a new Terminal window",
            Self::MacApp(..) | Self::Window(..) => "a new terminal window",
        }
    }
}

fn detect() -> Result<Backend, String> {
    let set = |name: &str| env::var_os(name).filter(|value| !value.is_empty());
    if set("TMUX").is_some() && available("tmux") {
        return Ok(Backend::Tmux(set("TMUX_PANE")));
    }
    if (set("ZELLIJ").is_some() || set("ZELLIJ_SESSION_NAME").is_some()) && available("zellij") {
        return Ok(Backend::Zellij);
    }
    if ["SSH_TTY", "SSH_CONNECTION", "SSH_CLIENT"]
        .into_iter()
        .any(|name| set(name).is_some())
    {
        return Err("over SSH only a tmux or Zellij session can open a sibling pane".to_owned());
    }
    if let Some(pane) = set("WEZTERM_PANE")
        && available("wezterm")
    {
        return Ok(Backend::WezTerm(pane));
    }
    if cfg!(windows) {
        return if available("wt.exe") {
            Ok(Backend::WindowsTerminal(set("WT_SESSION").is_some()))
        } else {
            Err("Windows Terminal (wt.exe) was not found on PATH".to_owned())
        };
    }
    let terminal = ["TERM_PROGRAM", "TERMINAL_EMULATOR", "TERM"]
        .into_iter()
        .filter_map(set)
        .map(|value| value.to_string_lossy().to_ascii_lowercase())
        .collect::<Vec<_>>()
        .join(" ");
    if cfg!(target_os = "macos") {
        if terminal.contains("iterm") {
            return Ok(Backend::Iterm);
        }
        if let Some((_, app, prefix)) = MAC_APPS
            .into_iter()
            .find(|(needle, ..)| terminal.contains(needle))
        {
            return Ok(Backend::MacApp(app, prefix));
        }
        return Ok(Backend::AppleTerminal);
    }
    if set("DISPLAY").is_none() && set("WAYLAND_DISPLAY").is_none() {
        return Err("no graphical display or terminal multiplexer is available".to_owned());
    }
    WINDOWS
        .into_iter()
        .find(|(needle, program, _)| {
            !needle.is_empty() && terminal.contains(needle) && available(program)
        })
        .or_else(|| {
            WINDOWS
                .into_iter()
                .find(|(_, program, _)| available(program))
        })
        .map(|(_, program, prefix)| Backend::Window(program, prefix))
        .ok_or_else(|| "no supported terminal emulator was found on PATH".to_owned())
}

fn run(program: &OsString, arguments: &[OsString], wait: bool) -> Result<(), String> {
    let name = program.to_string_lossy();
    let mut command = Command::new(program);
    command
        .args(arguments)
        .stdin(Stdio::null())
        .stdout(Stdio::null());
    if !wait {
        command
            .stderr(Stdio::null())
            .spawn()
            .map_err(|error| format!("could not start {name}: {error}"))?;
        return Ok(());
    }
    let output = command
        .stderr(Stdio::piped())
        .output()
        .map_err(|error| format!("could not run {name}: {error}"))?;
    if output.status.success() {
        return Ok(());
    }
    let stderr = String::from_utf8_lossy(&output.stderr).trim().to_owned();
    Err(if stderr.is_empty() {
        format!("{name} exited with {}", output.status)
    } else {
        format!("{name} failed: {stderr}")
    })
}

fn shell_command(arguments: &[OsString]) -> Result<OsString, String> {
    let quoted = arguments
        .iter()
        .map(|argument| {
            argument
                .to_str()
                .map(|text| format!("'{}'", text.replace('\'', r"'\''")))
                .ok_or_else(|| format!("{} is not valid UTF-8", argument.to_string_lossy()))
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(format!("exec {}", quoted.join(" ")).into())
}

fn words<const N: usize>(words: [&str; N]) -> Vec<OsString> {
    words.into_iter().map(OsString::from).collect()
}

fn available(program: &str) -> bool {
    env::var_os("PATH").is_some_and(|paths| {
        env::split_paths(&paths).any(|directory| executable(&directory.join(program)))
    })
}

#[cfg(unix)]
fn executable(path: &Path) -> bool {
    use std::os::unix::fs::PermissionsExt;
    std::fs::metadata(path)
        .is_ok_and(|metadata| metadata.is_file() && metadata.permissions().mode() & 0o111 != 0)
}

#[cfg(not(unix))]
fn executable(path: &Path) -> bool {
    path.is_file()
}
