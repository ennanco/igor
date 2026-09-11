use std::process::ExitCode;

use clap::{Parser, ValueEnum};
use igor_core::{ConfigOverrides, Environment, load_effective_config};
use igor_daemon::DaemonRole;

#[derive(Debug, Parser)]
#[command(name = "igor-daemon", version, about = "Igor background runtime")]
struct Cli {
    #[arg(value_enum)]
    role: Role,
}

#[derive(Clone, Copy, Debug, ValueEnum)]
enum Role {
    Worker,
    Supervisor,
}

impl From<Role> for DaemonRole {
    fn from(role: Role) -> Self {
        match role {
            Role::Worker => Self::Worker,
            Role::Supervisor => Self::Supervisor,
        }
    }
}

#[tokio::main]
async fn main() -> ExitCode {
    match run().await {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("igor-daemon: {error:#}");
            ExitCode::FAILURE
        }
    }
}

async fn run() -> anyhow::Result<()> {
    igor_core::telemetry::init("igor_daemon=info")?;
    let cli = Cli::parse();
    let environment = Environment::from_process()?;
    let cwd = std::env::current_dir()?;
    let effective = load_effective_config(&environment, &ConfigOverrides::default(), &cwd)?;
    igor_daemon::run(cli.role.into(), &effective.paths).await?;
    Ok(())
}
