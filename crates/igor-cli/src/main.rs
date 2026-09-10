use std::process::ExitCode;

use clap::Parser;

#[derive(Debug, Parser)]
#[command(
    name = "igor",
    version,
    about = "Integrated General-purpose Orchestrator for Research"
)]
struct Cli {}

fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("igor: {error:#}");
            ExitCode::FAILURE
        }
    }
}

fn run() -> anyhow::Result<()> {
    igor_core::telemetry::init("igor=info")?;
    let _cli = Cli::parse();
    Ok(())
}
