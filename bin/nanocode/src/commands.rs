pub const COMMANDS: [(&str, &str); 30] = [
    (
        "/model",
        "choose harness, model, thinking and speed for a new thread",
    ),
    ("/thinking", "open the thinking dial"),
    ("/effort", "open the thinking dial"),
    (
        "/fast",
        "cycle the service tier (standard, fast, ultrafast)",
    ),
    (
        "/btw",
        "open the side pane: a forked side thread that never interrupts the turn",
    ),
    ("/resume", "resume a saved session"),
    ("/new", "start a new session"),
    (
        "/agents",
        "toggle the tree of active subagents; /agents all lists finished ones too",
    ),
    ("/compact", "compact the conversation context"),
    ("/context", "token usage for the last turn and the session"),
    ("/copy", "copy the last reply to the clipboard"),
    ("/theme", "choose the auto, dark or light theme"),
    ("/skills", "insert a skill from this workspace"),
    ("/image", "attach an image from the clipboard"),
    ("/detach", "remove pending image attachments"),
    ("/session", "show session, model and tool details"),
    ("/id", "show the session id and copy it to the clipboard"),
    (
        "/runner",
        "list, connect, listen, select or disconnect hands",
    ),
    ("/expand", "expand or collapse tools and reasoning"),
    ("/mouse", "toggle mouse capture for native text selection"),
    ("/help", "show key bindings"),
    ("/clear", "start a new session"),
    (
        "/collapse",
        "send the side pane's conversation to its thread and close it",
    ),
    ("/split", "open the side thread in a new terminal pane"),
    ("/close", "close the side pane without touching the thread"),
    (
        "/zoom",
        "toggle the focused pane between split and full width",
    ),
    (
        "/agent",
        "focus a subagent by #id or role and message it; /agent alone returns to the main thread",
    ),
    (
        "/stop",
        "close a subagent and its children for good by #id or role; alone closes the focused one; esc esc only interrupts",
    ),
    (
        "/reload",
        "restart all nc windows with the newest binary, continuing active turns and queued prompts",
    ),
    ("/quit", "exit nanocode"),
];

#[derive(clap::Parser)]
#[command(
    name = "/runner",
    about = "Manage runner connections and listeners",
    disable_help_subcommand = true
)]
pub struct RunnerCommand {
    #[command(subcommand)]
    pub action: Option<RunnerAction>,
}

#[derive(clap::Subcommand)]
pub enum RunnerAction {
    #[command(alias = "status")]
    List,
    Connect {
        url: String,
        #[arg(long)]
        token: Option<String>,
        #[arg(long)]
        name: Option<String>,
        #[arg(long)]
        replace: bool,
    },
    Listen {
        address: std::net::SocketAddr,
        #[arg(long)]
        token: Option<String>,
    },
    #[command(
        about = "Run this session's exec cells on one hand, on several at once, on every hand with *, or on this machine without names"
    )]
    Select {
        names: Vec<String>,
    },
    Disconnect {
        name: String,
    },
    StopListening,
}

pub fn matching(prefix: &str) -> impl Iterator<Item = (&'static str, &'static str)> + '_ {
    COMMANDS
        .iter()
        .copied()
        .filter(move |(command, _)| command.starts_with(prefix))
}

pub fn complete(prefix: &str) -> Option<&'static str> {
    matching(prefix).next().map(|(command, _)| command)
}
