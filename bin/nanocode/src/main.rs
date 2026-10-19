#![cfg(not(target_family = "wasm"))]

fn main() -> std::process::ExitCode {
    nanocode_cli::run()
}
