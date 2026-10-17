#[cfg(not(target_family = "wasm"))]
#[path = "support/agents.rs"]
mod agents;
#[cfg(not(target_family = "wasm"))]
mod support;

#[cfg(not(target_family = "wasm"))]
async fn run() -> support::Result<()> {
    use nanocode::{JsonlStore, PromptRequest};

    let args: Vec<String> = std::env::args().skip(1).collect();
    if args.first().is_some_and(|arg| arg == "--help") {
        println!(
            "cargo run --locked -p nanocode --example durable_session -- BACKEND STORE SESSION|- REQUEST_ID PROMPT...\nCreate with SESSION=-; reopen using the session ID printed on stderr.\nRepeat the same request ID and prompt to recover or return its recorded result.\nBACKEND is claude or codex. STORE is a JSONL file in an existing directory.\nSet NANOCODE_EXAMPLE_API_BASE plus both API keys for a compatible local endpoint."
        );
        return Ok(());
    }
    if args.len() < 5 {
        return Err("expected BACKEND STORE SESSION|- REQUEST_ID PROMPT...; use --help".into());
    }
    let store = nanocode::session_store(JsonlStore::open_async(&args[1]).await?)?;
    let runtime = agents::runtime(support::tools()?)?
        .durability(store.clone())
        .build_async()
        .await?;
    let outcome: support::Result<()> = async {
        let (agent, mut events) = if args[2] == "-" {
            agents::spawn(&runtime, &args[0]).await?
        } else {
            let restored = runtime.open_session(&args[2]).await?;
            if restored.0.harness_family().as_str() != args[0] {
                return Err("BACKEND differs from the saved session".into());
            }
            restored
        };
        eprintln!("session {}", agent.session_id());
        let drain = tokio::spawn(async move { while events.recv().await.is_some() {} });
        let answer = agent
            .prompt(PromptRequest::new(args[4..].join(" ")).request_id(&args[3]))
            .await?
            .result()
            .await;
        drain.abort();
        println!("{}", answer?.final_message());
        agent.flush().await?;
        let saved = store.load(agent.session_id()).await?;
        eprintln!("persisted {} turns", saved.turns.len());
        Ok(())
    }
    .await;
    let shutdown = runtime.shutdown().await;
    outcome?;
    shutdown?;
    Ok(())
}

#[cfg(not(target_family = "wasm"))]
fn main() {
    support::main(run());
}

#[cfg(target_family = "wasm")]
fn main() {}
