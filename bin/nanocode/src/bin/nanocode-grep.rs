fn main() -> std::process::ExitCode {
    let mut arguments = std::env::args_os();
    let name = arguments
        .next()
        .as_deref()
        .and_then(nanocode::runners::search::shimmed)
        .unwrap_or("grep");
    nanocode::runners::search::run(name, arguments.collect())
}
