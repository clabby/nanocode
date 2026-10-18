#[cfg(not(target_family = "wasm"))]
#[path = "support/agents.rs"]
mod agents;
#[cfg(not(target_family = "wasm"))]
#[path = "support/stream.rs"]
mod stream;
#[cfg(not(target_family = "wasm"))]
mod support;

#[cfg(not(target_family = "wasm"))]
async fn run() -> support::Result<()> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    if args.first().is_some_and(|arg| arg == "--help") {
        println!(
            "cargo run --locked -p nanocode --example dual_backends -- [PROMPT...]\nRuns Claude and Codex with the same registered Rust tools and native on-disk durability.\nSet NANOCODE_EXAMPLE_API_BASE plus both API keys to use a compatible local endpoint."
        );
        return Ok(());
    }
    let prompt = if args.is_empty() {
        "Use Code Mode to call tools.checked_sum({a:19,b:23}) and tools.quotient({a:84,b:2}). Then call tools.quotient({a:1,b:0}) and explain the reported error."
            .to_owned()
    } else {
        args.join(" ")
    };
    let runtime = agents::runtime(support::tools()?)?.build_async().await?;
    let outcome: support::Result<()> = async {
        for backend in ["claude", "codex"] {
            let (agent, mut events) = agents::spawn(&runtime, backend).await?;
            eprintln!("{backend} session {}", agent.session_id());
            stream::prompt(&agent, &mut events, prompt.clone()).await?;
            agent.flush().await?;
        }
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
