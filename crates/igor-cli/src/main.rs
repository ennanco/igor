use std::{path::PathBuf, process::ExitCode};

use clap::{Args, Parser, Subcommand};
use igor_core::{ConfigOverrides, Environment, initialize_project, load_effective_config};

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
    let cli = Cli::parse();
    let cwd = std::env::current_dir()?;
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
            let environment = Environment::from_process()?;
            let effective = load_effective_config(&environment, &cli.paths.into(), &cwd)?;
            match command {
                ConfigCommand::Path => {
                    println!("global: {}", effective.paths.config_file.display());
                    match effective.project {
                        Some(project) => println!("project: {}", project.config_file.display()),
                        None => println!("project: not found"),
                    }
                }
                ConfigCommand::Show => print!("{}", toml::to_string_pretty(&effective)?),
                ConfigCommand::Check => {
                    println!("configuration is valid");
                }
            }
        }
    }
    Ok(())
}
