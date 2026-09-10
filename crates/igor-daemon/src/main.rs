use std::process::ExitCode;

use clap::Parser;

#[derive(Debug, Parser)]
#[command(name = "igor-daemon", version, about = "Igor background runtime")]
struct Cli {}

fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("igor-daemon: {error:#}");
            ExitCode::FAILURE
        }
    }
}

fn run() -> anyhow::Result<()> {
    igor_core::telemetry::init("igor_daemon=info")?;
    let _cli = Cli::parse();
    tracing::info!("daemon runtime is not implemented yet");
    Ok(())
}
