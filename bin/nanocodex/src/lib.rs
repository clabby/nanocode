#![cfg(not(target_family = "wasm"))]

mod actions;
mod app;
mod cli;
mod commands;
mod events;
mod extras;
mod focus;
mod history;
mod host;
mod models;
mod sessions;
mod side;
mod split;
mod stream;
mod telemetry;
mod view;

use std::{
    io::{IsTerminal, Write},
    path::PathBuf,
    process::ExitCode,
};

use clap::Parser;
use futures_util::StreamExt;
use nanocode::{
    ModelSelection, ServiceTier, Thinking,
    tools::attachment::{RunnerConfig, RunnerEndpoint, run_runner_with_tools},
};

use crate::{
    cli::{Cli, Command, HarnessArg, RunnerArgs, SessionArgs, SessionsArgs},
    host::{Host, HostConfig, Result},
};

const TELEMETRY_SHUTDOWN: std::time::Duration = std::time::Duration::from_millis(500);

fn install_telemetry(cli: &Cli) -> Result<Option<nanocode_observability::ObservabilityGuard>> {
    let args = &cli.observability;
    let explicit_home = match &cli.command {
        Some(Command::Exec(args)) => args.home.as_deref(),
        Some(Command::Sessions(args)) => args.home.as_deref(),
        _ => None,
    }
    .or(cli.session.home.as_deref());
    let home = host::home_dir(explicit_home)?;
    let interactive = !matches!(
        cli.command,
        Some(Command::Exec(_) | Command::Runner(_) | Command::Sessions(_) | Command::Models)
    );
    let output = match (args.log_filter().is_some(), interactive) {
        (false, _) => nanocode_observability::LogOutput::Disabled,
        (true, false) => nanocode_observability::LogOutput::Stderr,
        (true, true) => {
            nanocode_observability::LogOutput::File(home.join("log").join("nanocodex.log"))
        }
    };
    Ok(Some(
        args.builder("nanocodex", env!("CARGO_PKG_VERSION"), output)
            .otlp_directory(home.join("log").join("otel"))
            .shutdown_timeout(TELEMETRY_SHUTDOWN)
            .install()?,
    ))
}

pub fn main() -> ExitCode {
    let entered = std::time::Instant::now();
    let cli = Cli::parse();
    let parsed = entered.elapsed();
    let telemetry_started = std::time::Instant::now();
    let telemetry = match install_telemetry(&cli) {
        Ok(telemetry) => telemetry,
        Err(error) => {
            eprintln!("nanocodex: failed to start telemetry: {error}");
            return ExitCode::FAILURE;
        }
    };
    let startup = tracing::info_span!(
        target: "nanocodex",
        "nc.startup",
        process.pid = std::process::id(),
        cli.duration_ms = parsed.as_secs_f64() * 1_000.0,
        telemetry.duration_ms = telemetry_started.elapsed().as_secs_f64() * 1_000.0,
        main_to_span.duration_ms = entered.elapsed().as_secs_f64() * 1_000.0,
        runtime.duration_ms = tracing::field::Empty
    );
    let runtime_started = std::time::Instant::now();
    let runtime = match startup.in_scope(|| {
        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
    }) {
        Ok(runtime) => runtime,
        Err(error) => {
            eprintln!("nanocodex: failed to start the async runtime: {error}");
            return ExitCode::FAILURE;
        }
    };
    startup.record(
        "runtime.duration_ms",
        runtime_started.elapsed().as_secs_f64() * 1_000.0,
    );
    drop(startup);
    let outcome = runtime.block_on(async move {
        match cli.command {
            Some(Command::Runner(args)) => runner(args).await,
            Some(Command::Sessions(mut args)) => {
                args.home = args.home.or(cli.session.home);
                list_sessions(args).await
            }
            Some(Command::Exec(args)) => exec(args.or(cli.session)).await,
            Some(Command::Models) => {
                print_models();
                Ok(())
            }
            Some(Command::Resume { id }) => {
                let mut args = cli.session;
                args.resume = Some(id.unwrap_or_default());
                interactive(args).await
            }
            None => interactive(cli.session).await,
        }
    });
    runtime.shutdown_timeout(std::time::Duration::from_secs(1));
    drop(telemetry);
    match outcome {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("nanocodex: {error}");
            ExitCode::FAILURE
        }
    }
}

struct Prepared {
    host: Host,
    model: ModelSelection,
    thinking: Thinking,
    tier: ServiceTier,
    home: PathBuf,
}

async fn prepare(args: &SessionArgs, notify: &mut dyn FnMut(&str)) -> Result<Prepared> {
    let workspace = match &args.workspace {
        Some(path) => std::fs::canonicalize(path)
            .map_err(|error| format!("workspace {}: {error}", path.display()))?,
        None => std::env::current_dir()?,
    };
    let home = host::home_dir(args.home.as_deref())?;
    let gateway = host::gateway(&args.gateway)?;
    let model = match (&args.model, args.harness) {
        (Some(model), harness) => {
            let model = host::parse_model(model)?;
            if let Some(harness) = harness
                && family(harness) != model.family()
            {
                return Err(format!(
                    "model {model} does not belong to the {} harness",
                    family(harness)
                )
                .into());
            }
            model
        }
        (None, Some(harness)) => family(harness).default_model(),
        (None, None) => nanocode::HarnessFamily::Codex.default_model(),
    };
    if let Some(gateway) = &gateway {
        gateway.check(&model)?;
    }
    let defaulted = args.model.is_none() && args.harness.is_none();
    let capabilities = model.capabilities(nanocode::ModelTransport::Native);
    let thinking = match &args.thinking {
        Some(value) => host::parse_thinking(value)?,
        None if defaulted && capabilities.supports_thinking(Thinking::Xhigh) => Thinking::Xhigh,
        None => model.default_thinking(),
    };
    model
        .capabilities(nanocode::ModelTransport::Native)
        .check_thinking(thinking)?;
    let tier = match &args.tier {
        Some(value) => host::parse_tier(value)?,
        None if defaulted
            && gateway.is_none()
            && capabilities.supports_service_tier(ServiceTier::Fast) =>
        {
            ServiceTier::Fast
        }
        None => ServiceTier::Standard,
    };
    model
        .capabilities(nanocode::ModelTransport::Native)
        .check_service_tier(tier)?;
    let store = if args.ephemeral {
        None
    } else {
        Some(host::open_store(&home).await?)
    };
    let (runners, runner_servers) = host::runner_configs(&args.remote)?;
    let config = HostConfig {
        workspace,
        store,
        subagents: !args.no_subagents,
        runners,
        runner_servers,
        gateway,
    };
    let host = tracing::Instrument::instrument(
        Host::build(config, &model, thinking),
        tracing::info_span!(target: "nanocodex", "nc.host.build"),
    )
    .await?;
    tracing::Instrument::instrument(
        host.runtime.initialize_runners(),
        tracing::info_span!(target: "nanocodex", "nc.runners.initialize"),
    )
    .await?;
    for server in host.runtime.runner_servers() {
        notify(&format!(
            "listening {} · token {}",
            server.endpoint(),
            server.token()
        ));
    }
    Ok(Prepared {
        host,
        model,
        thinking,
        tier,
        home,
    })
}

const fn family(harness: HarnessArg) -> nanocode::HarnessFamily {
    match harness {
        HarnessArg::Codex => nanocode::HarnessFamily::Codex,
        HarnessArg::Claude => nanocode::HarnessFamily::Claude,
    }
}

async fn interactive(args: SessionArgs) -> Result<()> {
    if !std::io::stdout().is_terminal() || !std::io::stdin().is_terminal() {
        return Err(
            "the interactive interface needs a terminal; use `nanocodex exec <prompt>` for scripts"
                .into(),
        );
    }
    let terminal =
        nanocode_tui::terminal::TerminalSession::enter(nanocode_tui::terminal::TerminalOptions {
            mouse_capture: !args.no_mouse,
            keyboard_enhancement: true,
        })?;
    nanocode_tui::highlight::prewarm();
    let prepared = prepare(&args, &mut |_| {}).await;
    let prepared = match prepared {
        Ok(prepared) => prepared,
        Err(error) => {
            drop(terminal);
            return Err(error);
        }
    };
    let (resume, pick_session) = match args.resume.as_deref() {
        Some("") => (None, true),
        Some(id) => (Some(id.to_owned()), false),
        None if args.continue_last => match latest_session(&prepared.host).await {
            Ok(id) => (Some(id), false),
            Err(error) => {
                drop(terminal);
                return Err(error);
            }
        },
        None => (None, false),
    };
    if (resume.is_some() || pick_session) && prepared.host.config.store.is_none() {
        drop(terminal);
        return Err("--resume and --continue require durability; remove --ephemeral".into());
    }
    app::run(
        app::Launch {
            host: prepared.host,
            model: prepared.model,
            thinking: prepared.thinking,
            tier: prepared.tier,
            resume,
            pick_session,
            prompt: args.prompt,
            history_path: Some(prepared.home.join("history.jsonl")),
            home: prepared.home.clone(),
            thinking_override: args
                .thinking
                .as_deref()
                .map(host::parse_thinking)
                .transpose()?,
            tier_override: args.tier.as_deref().map(host::parse_tier).transpose()?,
        },
        terminal,
    )
    .await
}

async fn latest_session(host: &Host) -> Result<String> {
    let store = host
        .config
        .store
        .as_ref()
        .ok_or("--continue requires durability")?;
    store
        .list()
        .await?
        .into_iter()
        .find(|session| {
            sessions::user_facing(session.record.lineage.origin)
                && session.record.workspace.as_deref() == Some(host.config.workspace.as_path())
        })
        .map(|session| session.record.session_id)
        .ok_or_else(|| "no saved session for this workspace".into())
}

async fn exec(args: SessionArgs) -> Result<()> {
    let prompt = args
        .prompt
        .clone()
        .filter(|prompt| !prompt.trim().is_empty())
        .ok_or("exec requires a prompt")?;
    let prepared = prepare(&args, &mut |message| eprintln!("nanocodex: {message}")).await?;
    let host = prepared.host;
    let resume = match (&args.resume, args.continue_last) {
        (Some(id), _) if !id.is_empty() => Some(id.clone()),
        (_, true) => Some(latest_session(&host).await?),
        _ => None,
    };
    let (agent, mut events, pending) = match resume {
        Some(id) => {
            let ready = app::open_session(
                &host,
                &id,
                args.thinking
                    .as_deref()
                    .map(host::parse_thinking)
                    .transpose()?,
                args.tier.as_deref().map(host::parse_tier).transpose()?,
            )
            .await?;
            (ready.agent, ready.events, ready.pending)
        }
        None => {
            let (agent, events) = host.start(&prepared.model, prepared.thinking).await?;
            app::apply_tier(&agent, &prepared.model, prepared.tier).await?;
            (agent, events, Vec::new())
        }
    };
    if let Some(reason) = host.runtime.computer_use_unavailable() {
        eprintln!("nanocodex: computer use unavailable, continuing without it: {reason}");
    }
    tokio::spawn(async move { while events.recv().await.is_some() {} });
    for pending in pending {
        eprintln!("nanocodex: resuming interrupted turn {}", pending.id);
        let turn = agent
            .prompt(nanocode::PromptRequest::new(pending.prompt).request_id(&pending.id))
            .await?;
        stream_turn(turn, std::io::stderr()).await?;
    }
    stream_turn(agent.prompt(prompt.as_str()).await?, std::io::stdout()).await?;
    let mut stderr = std::io::stderr();
    drop(writeln!(stderr, "session {}", agent.session_id()));
    agent.flush().await?;
    host.runtime.shutdown().await?;
    Ok(())
}

async fn stream_turn(mut turn: nanocode::agent::Turn, answers: impl Write) -> Result<()> {
    let mut printer = stream::Printer::new(answers);
    while let Some(event) = turn.next().await {
        printer.event(&event)?;
    }
    let result = turn.await?;
    printer.finish(result.final_message())?;
    Ok(())
}

async fn list_sessions(args: SessionsArgs) -> Result<()> {
    let home = host::home_dir(args.home.as_deref())?;
    let store = host::open_store(&home).await?;
    let workspace = std::env::current_dir()?;
    for session in store.list().await? {
        let record = &session.record;
        if !sessions::user_facing(record.lineage.origin) {
            continue;
        }
        if !args.all && record.workspace.as_deref() != Some(workspace.as_path()) {
            continue;
        }
        println!(
            "{}  {:>8}  {:<20}  {}",
            record.session_id,
            sessions::age(record.updated_at_ms),
            record.model.to_string(),
            sessions::title(&session)
        );
    }
    store.flush().await?;
    Ok(())
}

fn print_models() {
    for (family, models) in models::catalog() {
        println!("{}", app::family_label(&family));
        for model in models {
            let capabilities = model.capabilities(nanocode::ModelTransport::Native);
            let thinking = capabilities
                .thinking()
                .map(Thinking::as_str)
                .collect::<Vec<_>>()
                .join("/");
            let features = models::features(&model);
            println!(
                "  {:<20} thinking {:<30} default {:<7} {}",
                model.to_string(),
                thinking,
                capabilities.default_thinking().as_str(),
                features
            );
        }
    }
}

async fn runner(args: RunnerArgs) -> Result<()> {
    let workspace = match &args.workspace {
        Some(path) => std::fs::canonicalize(path)?,
        None => std::env::current_dir()?,
    };
    let endpoint = match (args.listen, args.connect) {
        (Some(address), _) => RunnerEndpoint::Listen(address),
        (None, Some(url)) => RunnerEndpoint::Connect(url),
        (None, None) => return Err("pass --listen <ADDR> or --connect <WS_URL>".into()),
    };
    let config = RunnerConfig {
        endpoint,
        token: args.token,
        workspace: workspace.clone(),
        name: args.name.clone(),
    };
    let mut tools = nanocode::claude::runner_tools(workspace.clone())?.into_builder();
    if args.code_mode {
        tools = tools.add(nanocode::tools::WorkspaceTools::new(workspace.clone()));
    }
    if args.cua {
        let computer =
            nanocode::ComputerTools::connect(nanocode::ComputerConfig::installed()?).await?;
        tools = tools.tool(computer.js()).tool(computer.reset());
        eprintln!(
            "nanocodex runner {} serving {} with computer use",
            args.name,
            workspace.display()
        );
    } else {
        eprintln!(
            "nanocodex runner {} serving {}",
            args.name,
            workspace.display()
        );
    }
    if args.code_mode {
        nanocode::tools::remote::run_code_mode_runner(
            config,
            tools.build()?,
            nanocode::tools::remote::SocketPolicy::runner(),
        )
        .await?;
        return Ok(());
    }
    run_runner_with_tools(config, tools.build()?).await?;
    Ok(())
}
