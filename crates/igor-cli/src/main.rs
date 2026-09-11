use std::{path::PathBuf, process::ExitCode};

use clap::{Args, Parser, Subcommand};
use igor_core::{
    ConfigOverrides, EffectiveConfig, Environment, initialize_project, load_effective_config,
};
use igor_daemon::{
    Client, ClientError, DaemonRole, DatabaseStatus, Health, Request, Response, Version,
};
use serde::Serialize;

const EXIT_DAEMON_UNAVAILABLE: u8 = 3;
const EXIT_PROTOCOL_MISMATCH: u8 = 4;
const EXIT_INVALID_REQUEST: u8 = 5;
const EXIT_DATABASE_UNAVAILABLE: u8 = 6;
const EXIT_DAEMON_INTERNAL: u8 = 7;

#[derive(Debug, Parser)]
#[command(
    name = "igor",
    version,
    about = "Integrated General-purpose Orchestrator for Research"
)]
struct Cli {
    #[command(flatten)]
    paths: PathArgs,
    #[command(subcommand)]
    command: Command,
}

#[derive(Args, Debug, Default)]
struct PathArgs {
    #[arg(long, global = true, value_name = "FILE")]
    config: Option<PathBuf>,
    #[arg(long, global = true, value_name = "PATH")]
    project: Option<PathBuf>,
    #[arg(long, global = true)]
    state_dir: Option<PathBuf>,
    #[arg(long, global = true)]
    runtime_dir: Option<PathBuf>,
    #[arg(long, global = true)]
    database: Option<PathBuf>,
    #[arg(long, global = true)]
    log_dir: Option<PathBuf>,
    #[arg(long, global = true)]
    worktree_dir: Option<PathBuf>,
    #[arg(long, global = true)]
    report_dir: Option<PathBuf>,
}

impl From<PathArgs> for ConfigOverrides {
    fn from(value: PathArgs) -> Self {
        Self {
            global_config: value.config,
            project: value.project,
            state_dir: value.state_dir,
            runtime_dir: value.runtime_dir,
            database: value.database,
            log_dir: value.log_dir,
            worktree_dir: value.worktree_dir,
            report_dir: value.report_dir,
        }
    }
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Create portable project configuration.
    Init {
        #[arg(default_value = ".")]
        path: PathBuf,
        #[arg(long)]
        force: bool,
    },
    /// Inspect and validate configuration.
    Config {
        #[command(subcommand)]
        command: ConfigCommand,
    },
    /// Run the experiment worker.
    Worker,
    /// Run the background action supervisor.
    Supervisor,
    /// Inspect local daemon processes.
    Daemon {
        #[command(subcommand)]
        command: DaemonCommand,
    },
}

#[derive(Debug, Subcommand)]
enum ConfigCommand {
    /// Print selected global and project configuration paths.
    Path,
    /// Print effective configuration with secrets redacted.
    Show,
    /// Parse and validate selected configuration.
    Check,
}

#[derive(Debug, Subcommand)]
enum DaemonCommand {
    /// Check that worker and supervisor respond.
    Health {
        #[arg(long)]
        json: bool,
    },
    /// Show daemon versions and database state.
    Status {
        #[arg(long)]
        json: bool,
    },
}

#[derive(Debug, Serialize)]
struct DaemonStatus {
    role: DaemonRole,
    health: Health,
    version: Version,
    database: DatabaseStatus,
}

#[tokio::main]
async fn main() -> ExitCode {
    match run().await {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("igor: {error:#}");
            client_exit_code(&error).unwrap_or(ExitCode::FAILURE)
        }
    }
}

async fn run() -> anyhow::Result<()> {
    igor_core::telemetry::init("igor=info")?;
    let cli = Cli::parse();
    let cwd = std::env::current_dir()?;
    let overrides = ConfigOverrides::from(cli.paths);
    match cli.command {
        Command::Init { path, force } => {
            let root = if path.is_absolute() {
                path
            } else {
                cwd.join(path)
            };
            for created in initialize_project(&root, force)? {
                println!("{}", created.display());
            }
        }
        Command::Config { command } => {
            let effective = effective_config(&overrides, &cwd)?;
            match command {
                ConfigCommand::Path => {
                    println!("global: {}", effective.paths.config_file.display());
                    match effective.project {
                        Some(project) => println!("project: {}", project.config_file.display()),
                        None => println!("project: not found"),
                    }
                }
                ConfigCommand::Show => print!("{}", toml::to_string_pretty(&effective)?),
                ConfigCommand::Check => println!("configuration is valid"),
            }
        }
        Command::Worker => {
            let effective = effective_config(&overrides, &cwd)?;
            igor_daemon::run(DaemonRole::Worker, &effective.paths).await?;
        }
        Command::Supervisor => {
            let effective = effective_config(&overrides, &cwd)?;
            igor_daemon::run(DaemonRole::Supervisor, &effective.paths).await?;
        }
        Command::Daemon { command } => {
            let effective = effective_config(&overrides, &cwd)?;
            let client = Client::new(&effective.paths);
            match command {
                DaemonCommand::Health { json } => show_health(&client, json).await?,
                DaemonCommand::Status { json } => show_status(&client, json).await?,
            }
        }
    }
    Ok(())
}

fn effective_config(
    overrides: &ConfigOverrides,
    cwd: &std::path::Path,
) -> anyhow::Result<EffectiveConfig> {
    let environment = Environment::from_process()?;
    Ok(load_effective_config(&environment, overrides, cwd)?)
}

async fn show_health(client: &Client, json: bool) -> anyhow::Result<()> {
    let mut health = Vec::new();
    for role in [DaemonRole::Worker, DaemonRole::Supervisor] {
        match client.request(role, Request::Health).await? {
            Response::Health(response) => health.push(response),
            response => anyhow::bail!("unexpected {response:?} response to health request"),
        }
    }
    if json {
        println!("{}", serde_json::to_string_pretty(&health)?);
    } else {
        for response in &health {
            let state = if response.healthy {
                "healthy"
            } else {
                "unhealthy"
            };
            println!("{}: {state} (pid {})", response.role, response.pid);
        }
    }
    if health.iter().any(|response| !response.healthy) {
        anyhow::bail!("one or more daemons reported an unhealthy state");
    }
    Ok(())
}

async fn show_status(client: &Client, json: bool) -> anyhow::Result<()> {
    let mut statuses = Vec::new();
    for role in [DaemonRole::Worker, DaemonRole::Supervisor] {
        let health = match client.request(role, Request::Health).await? {
            Response::Health(response) => response,
            response => anyhow::bail!("unexpected {response:?} response to health request"),
        };
        let version = match client.request(role, Request::Version).await? {
            Response::Version(response) => response,
            response => anyhow::bail!("unexpected {response:?} response to version request"),
        };
        let database = match client.request(role, Request::DatabaseStatus).await? {
            Response::DatabaseStatus(response) => response,
            response => anyhow::bail!("unexpected {response:?} response to database request"),
        };
        statuses.push(DaemonStatus {
            role,
            health,
            version,
            database,
        });
    }
    if json {
        println!("{}", serde_json::to_string_pretty(&statuses)?);
    } else {
        for status in &statuses {
            let health = if status.health.healthy {
                "healthy"
            } else {
                "unhealthy"
            };
            println!(
                "{}: {}, Igor {}, protocol {}, database schema {}, integrity {}",
                status.role,
                health,
                status.version.igor,
                status.version.protocol,
                status.database.schema_version,
                status.database.integrity
            );
        }
    }
    if statuses.iter().any(|status| !status.health.healthy) {
        anyhow::bail!("one or more daemons reported an unhealthy state");
    }
    Ok(())
}

fn client_exit_code(error: &anyhow::Error) -> Option<ExitCode> {
    let client = error.downcast_ref::<ClientError>()?;
    let code = match client {
        ClientError::Unavailable { .. } | ClientError::Timeout { .. } => EXIT_DAEMON_UNAVAILABLE,
        ClientError::ProtocolMismatch { .. } => EXIT_PROTOCOL_MISMATCH,
        ClientError::InvalidRequest { .. } => EXIT_INVALID_REQUEST,
        ClientError::DatabaseUnavailable { .. } => EXIT_DATABASE_UNAVAILABLE,
        ClientError::Internal { .. } => EXIT_DAEMON_INTERNAL,
        ClientError::InvalidResponse(_) => return None,
    };
    Some(ExitCode::from(code))
}
