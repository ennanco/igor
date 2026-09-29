mod service;

use std::{
    fs::{self, File},
    io::{self, Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
    process::{ExitCode, Stdio},
    time::Duration,
};

use clap::{Args, Parser, Subcommand};
use igor_core::{
    CommandSpec, ConfigOverrides, Database, DatabaseOptions, EffectiveConfig, Environment,
    EnvironmentPolicy, FamilyDetail, FamilyId, HostConfigUpdate, IntegrityCheck, JobDetail, JobId,
    JobLogs, JobState, Project, ProjectId, ResourceStatus, ShellPolicy, StoredEvent, StoredJob,
    SubmissionInput, TransitionState, initialize_project, load_effective_config, load_family_file,
    load_job_file, load_project_config, select_global_config_path, update_global_host_config,
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
const EXIT_NOT_FOUND: u8 = 8;
const EXIT_CONFLICT: u8 = 9;
const USER_SERVICE_NAMES: [&str; 2] = ["igor-worker.service", "igor-supervisor.service"];
const SYSTEMCTL_TIMEOUT: Duration = Duration::from_secs(30);
const JOURNALCTL_TIMEOUT: Duration = Duration::from_secs(30);

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
    /// Remove Igor's user services while preserving the binary and all data.
    Uninstall {
        /// Skip the confirmation prompt.
        #[arg(long)]
        yes: bool,
        /// Show which managed units would be removed without changing services.
        #[arg(long)]
        dry_run: bool,
    },
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
    /// Inspect and verify the local database.
    Db {
        #[command(subcommand)]
        command: DbCommand,
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
    /// Manage Igor user services.
    Service {
        #[command(subcommand)]
        command: ServiceCommand,
    },
    /// Configure and test Telegram notifications.
    Notify {
        #[command(subcommand)]
        command: NotifyCommand,
    },
    /// Show host resources and active leases.
    Resources {
        #[arg(long)]
        json: bool,
    },
    /// Register and inspect projects.
    Project {
        #[command(subcommand)]
        command: ProjectCommand,
    },
    /// Submit a command or versioned job file.
    Submit(SubmitArgs),
    /// Submit and inspect seed families.
    Family {
        #[command(subcommand)]
        command: FamilyCommand,
    },
    /// List submitted jobs.
    List {
        #[arg(long)]
        json: bool,
    },
    /// Show a job and its immutable attempts.
    Show {
        job_id: JobId,
        #[arg(long)]
        json: bool,
    },
    /// Show a job's event stream.
    Events {
        job_id: JobId,
        #[arg(long)]
        json: bool,
    },
    /// Wait until a job reaches a terminal state.
    Wait {
        job_id: JobId,
        #[arg(long)]
        json: bool,
    },
    /// Cancel a queued or running job.
    Cancel {
        job_id: JobId,
        #[arg(long, default_value_t = 5)]
        grace_seconds: u32,
        #[arg(long)]
        json: bool,
    },
    /// Queue a new attempt for a failed, cancelled, or lost job.
    Retry {
        job_id: JobId,
        #[arg(long)]
        json: bool,
    },
    /// Print the latest attempt's stdout and stderr logs.
    Logs {
        job_id: JobId,
        #[arg(long)]
        follow: bool,
    },
}

#[derive(Debug, Subcommand)]
enum ConfigCommand {
    /// Print selected global and project configuration paths.
    Path {
        #[arg(long)]
        json: bool,
    },
    /// Print effective configuration with secrets redacted.
    Show {
        #[arg(long)]
        json: bool,
    },
    /// Parse and validate selected configuration.
    Check {
        #[arg(long)]
        json: bool,
    },
    /// Set host scheduling limits in the global configuration.
    Set(ConfigSetArgs),
}

#[derive(Debug, Subcommand)]
enum DbCommand {
    /// Verify database integrity, foreign keys, and schema version without a daemon.
    Check {
        #[arg(long)]
        json: bool,
    },
    /// Migrate an existing database to the latest schema.
    Migrate {
        #[arg(long)]
        json: bool,
        /// Preview the migration without changing the database.
        #[arg(long)]
        dry_run: bool,
    },
    /// Create and validate a backup of an existing database.
    Backup {
        destination: PathBuf,
        #[arg(long)]
        json: bool,
    },
}

#[derive(Args, Debug)]
struct ConfigSetArgs {
    #[arg(long)]
    max_concurrent_jobs: Option<u32>,
    #[arg(
        long,
        value_name = "SIZE",
        value_parser = parse_memory_size,
        conflicts_with = "auto_memory"
    )]
    memory_bytes: Option<u64>,
    #[arg(long)]
    auto_memory: bool,
    #[arg(long, conflicts_with = "auto_cpu")]
    cpu_threads: Option<u32>,
    #[arg(long)]
    auto_cpu: bool,
    #[arg(long = "gpu", conflicts_with_all = ["disable_gpus", "auto_gpus"])]
    gpus: Vec<String>,
    #[arg(long, conflicts_with = "auto_gpus")]
    disable_gpus: bool,
    #[arg(long)]
    auto_gpus: bool,
}

#[derive(Debug, Subcommand)]
enum ProjectCommand {
    /// Register a configured project root.
    Add {
        path: PathBuf,
        #[arg(long)]
        name: Option<String>,
        #[arg(long)]
        json: bool,
    },
    /// List registered projects.
    List {
        #[arg(long)]
        json: bool,
    },
    /// Remove a project registration without deleting its files or history.
    Remove {
        #[arg(default_value = ".")]
        path: PathBuf,
        #[arg(long)]
        json: bool,
    },
}

#[derive(Args, Debug)]
struct SubmitArgs {
    #[arg(long, conflicts_with = "file")]
    shell: Option<String>,
    #[arg(long, conflicts_with = "shell")]
    file: Option<PathBuf>,
    #[arg(long)]
    name: Option<String>,
    #[arg(long)]
    priority: Option<i64>,
    #[arg(long)]
    allow_dirty: bool,
    #[arg(long = "configuration", value_name = "FILE")]
    scientific_configurations: Vec<PathBuf>,
    #[arg(long = "input", value_name = "FILE")]
    immutable_inputs: Vec<PathBuf>,
    #[arg(long)]
    json: bool,
    #[arg(last = true)]
    command: Vec<String>,
}

#[derive(Debug, Subcommand)]
enum FamilyCommand {
    /// Submit every seed of a family file as one atomic generation.
    Submit(FamilySubmitArgs),
    /// Show every generation and its members, with counts by job state.
    Show {
        family_id: FamilyId,
        #[arg(long)]
        json: bool,
    },
    /// Replace a terminal generation with a full new seed generation.
    Supersede {
        family_id: FamilyId,
        #[arg(long, value_name = "FILE")]
        file: PathBuf,
        #[arg(long)]
        json: bool,
    },
}

#[derive(Args, Debug)]
struct FamilySubmitArgs {
    #[arg(long, value_name = "FILE")]
    file: PathBuf,
    #[arg(long)]
    json: bool,
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

#[derive(Debug, Subcommand)]
enum ServiceCommand {
    /// Install Igor's user service units without enabling or starting them.
    Install {
        /// Explicitly install user services; system services are never modified.
        #[arg(long, required = true)]
        user: bool,
        /// Enable the installed user services at login.
        #[arg(long)]
        enable: bool,
        /// Start the installed user services immediately.
        #[arg(long)]
        start: bool,
    },
    /// Enable Igor's installed user services.
    Enable {
        #[arg(long)]
        now: bool,
    },
    /// Disable Igor's installed user services.
    Disable {
        #[arg(long)]
        now: bool,
    },
    /// Start Igor's installed user services.
    Start,
    /// Stop Igor's installed user services.
    Stop,
    /// Restart Igor's installed user services.
    Restart,
    /// Show load, active, and enablement state for Igor's user services.
    Status {
        #[arg(long)]
        json: bool,
    },
    /// Show logs from Igor's user services.
    Logs {
        #[arg(long)]
        follow: bool,
    },
    /// Stop and remove Igor's user service units.
    Uninstall {
        /// Explicitly select user services; system services are never modified.
        #[arg(long)]
        user: bool,
        /// Skip the confirmation prompt.
        #[arg(long)]
        yes: bool,
        /// Show which managed units would be removed without changing services.
        #[arg(long)]
        dry_run: bool,
    },
}

#[derive(Debug, Subcommand)]
enum NotifyCommand {
    /// Read a bot token from standard input and store it in private user configuration.
    Setup {
        #[arg(long, allow_hyphen_values = true)]
        chat_id: String,
    },
    /// Send a test message using configured credentials.
    Test,
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
        Command::Uninstall { yes, dry_run } => {
            uninstall_user_services(yes, dry_run).await?;
            if !dry_run {
                println!("preserved Igor binary, configuration, database, logs, and job history");
            }
        }
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
        Command::Config { command } => match command {
            ConfigCommand::Path { json } => {
                let effective = effective_config(&overrides, &cwd)?;
                if json {
                    println!(
                        "{}",
                        serde_json::to_string_pretty(&serde_json::json!({
                            "global": effective.paths.config_file,
                            "project": effective.project.map(|project| project.config_file),
                        }))?
                    );
                } else {
                    println!("global: {}", effective.paths.config_file.display());
                    match effective.project {
                        Some(project) => println!("project: {}", project.config_file.display()),
                        None => println!("project: not found"),
                    }
                }
            }
            ConfigCommand::Show { json } => {
                let effective = effective_config(&overrides, &cwd)?;
                if json {
                    println!("{}", serde_json::to_string_pretty(&effective)?);
                } else {
                    print!("{}", toml::to_string_pretty(&effective)?);
                }
            }
            ConfigCommand::Check { json } => {
                effective_config(&overrides, &cwd)?;
                if json {
                    println!("{{\"valid\":true}}");
                } else {
                    println!("configuration is valid");
                }
            }
            ConfigCommand::Set(arguments) => {
                set_host_config(&overrides, arguments)?;
            }
        },
        Command::Db { command } => match command {
            DbCommand::Check { json } => {
                let effective = effective_config(&overrides, &cwd)?;
                check_database(&effective.paths.database, json).await?;
            }
            DbCommand::Migrate { json, dry_run } => {
                let effective = effective_config(&overrides, &cwd)?;
                migrate_database(&effective.paths.database, json, dry_run).await?;
            }
            DbCommand::Backup { destination, json } => {
                let effective = effective_config(&overrides, &cwd)?;
                backup_database(
                    &effective.paths.database,
                    &absolute(&cwd, &destination),
                    json,
                )
                .await?;
            }
        },
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
        Command::Service { command } => match command {
            ServiceCommand::Install {
                user: _,
                enable,
                start,
            } => {
                install_user_services().await?;
                if enable {
                    manage_user_services("enable", false).await?;
                }
                if start {
                    manage_user_services("start", false).await?;
                }
            }
            ServiceCommand::Enable { now } => manage_user_services("enable", now).await?,
            ServiceCommand::Disable { now } => manage_user_services("disable", now).await?,
            ServiceCommand::Start => manage_user_services("start", false).await?,
            ServiceCommand::Stop => manage_user_services("stop", false).await?,
            ServiceCommand::Restart => manage_user_services("restart", false).await?,
            ServiceCommand::Status { json } => show_service_status(json).await?,
            ServiceCommand::Logs { follow } => show_service_logs(follow).await?,
            ServiceCommand::Uninstall {
                user: _,
                yes,
                dry_run,
            } => {
                uninstall_user_services(yes, dry_run).await?;
            }
        },
        Command::Notify { command } => match command {
            NotifyCommand::Setup { chat_id } => {
                let mut token = String::new();
                io::stdin().take(1025).read_to_string(&mut token)?;
                let path = select_global_config_path(&Environment::from_process()?, &overrides);
                igor_core::update_global_telegram_config(
                    &path,
                    token.trim_end_matches(['\r', '\n']),
                    &chat_id,
                ).map_err(|_| anyhow::anyhow!("cannot save Telegram credentials; check token, chat ID and private user config"))?;
                println!("Telegram credentials saved; restart the supervisor to apply changes");
            }
            NotifyCommand::Test => {
                let effective = effective_config(&overrides, &cwd)
                    .map_err(|_| anyhow::anyhow!("cannot load private user configuration"))?;
                let telegram = igor_daemon::TelegramClient::new(&effective.global.telegram)
                    .ok_or_else(|| {
                        anyhow::anyhow!("Telegram is not configured; run igor notify setup")
                    })?;
                match telegram.send("Igor notification test").await {
                    igor_daemon::SendResult::Delivered => println!("Telegram test delivered"),
                    igor_daemon::SendResult::Retry { reason, .. } => {
                        anyhow::bail!("Telegram test failed: {reason}")
                    }
                }
            }
        },
        Command::Resources { json } => {
            let effective = effective_config(&overrides, &cwd)?;
            let resources = match Client::new(&effective.paths)
                .request(DaemonRole::Worker, Request::Resources)
                .await?
            {
                Response::Resources { resources } => resources,
                response => anyhow::bail!("unexpected {response:?} resources response"),
            };
            print_resources(&resources, json)?;
        }
        Command::Project { command } => {
            let effective = effective_config(&overrides, &cwd)?;
            let client = Client::new(&effective.paths);
            match command {
                ProjectCommand::Add { path, name, json } => {
                    let root = absolute(&cwd, &path).canonicalize()?;
                    let loaded = load_project_config(&root.join(".igor/project.toml"))?;
                    let project = Project {
                        id: ProjectId::new(),
                        name: name.unwrap_or_else(|| {
                            root.file_name()
                                .and_then(|value| value.to_str())
                                .unwrap_or("project")
                                .to_owned()
                        }),
                        root,
                        config_path: loaded.config_file,
                    };
                    let registered = expect_project(
                        client
                            .request(DaemonRole::Worker, Request::ProjectRegister { project })
                            .await?,
                    )?;
                    print_project(&registered, json)?;
                }
                ProjectCommand::List { json } => {
                    let projects = match client
                        .request(DaemonRole::Worker, Request::ProjectList)
                        .await?
                    {
                        Response::Projects { projects } => projects,
                        response => anyhow::bail!("unexpected {response:?} project-list response"),
                    };
                    if json {
                        println!("{}", serde_json::to_string_pretty(&projects)?);
                    } else {
                        for project in projects {
                            println!(
                                "{}\t{}\t{}",
                                project.id,
                                project.name,
                                project.root.display()
                            );
                        }
                    }
                }
                ProjectCommand::Remove { path, json } => {
                    let root = absolute(&cwd, &path).canonicalize()?;
                    let removed = expect_project(
                        client
                            .request(DaemonRole::Worker, Request::ProjectRemove { root })
                            .await?,
                    )?;
                    print_project(&removed, json)?;
                }
            }
        }
        Command::Submit(arguments) => {
            let effective = effective_config(&overrides, &cwd)?;
            submit(&effective, &cwd, arguments).await?;
        }
        Command::Family { command } => match command {
            FamilyCommand::Submit(arguments) => {
                let effective = effective_config(&overrides, &cwd)?;
                family_submit(&effective, &cwd, arguments).await?;
            }
            FamilyCommand::Show { family_id, json } => {
                let effective = effective_config(&overrides, &cwd)?;
                let response = Client::new(&effective.paths)
                    .request(DaemonRole::Worker, Request::FamilyShow { family_id })
                    .await?;
                let family = match response {
                    Response::Family(family) => family,
                    response => anyhow::bail!("unexpected {response:?} family-show response"),
                };
                print_family(&family, json)?;
            }
            FamilyCommand::Supersede {
                family_id,
                file,
                json,
            } => {
                let effective = effective_config(&overrides, &cwd)?;
                family_supersede(&effective, &cwd, family_id, &file, json).await?;
            }
        },
        Command::List { json } => {
            let effective = effective_config(&overrides, &cwd)?;
            let client = Client::new(&effective.paths);
            let selected = selected_registered_project(&client, &effective).await?;
            if effective.project.is_some() && selected.is_none() {
                anyhow::bail!("current project is not registered; run `igor project add .`");
            }
            let project_id = selected.map(|project| project.id);
            let jobs = match client
                .request(DaemonRole::Worker, Request::JobList { project_id })
                .await?
            {
                Response::Jobs { jobs } => jobs,
                response => anyhow::bail!("unexpected {response:?} job-list response"),
            };
            print_jobs(&jobs, json)?;
        }
        Command::Show { job_id, json } => {
            let effective = effective_config(&overrides, &cwd)?;
            let detail = request_job(&Client::new(&effective.paths), job_id).await?;
            print_job(&detail, json)?;
        }
        Command::Events { job_id, json } => {
            let effective = effective_config(&overrides, &cwd)?;
            let events = request_events(&Client::new(&effective.paths), job_id).await?;
            print_events(&events, json)?;
        }
        Command::Wait { job_id, json } => {
            let effective = effective_config(&overrides, &cwd)?;
            let client = Client::new(&effective.paths);
            loop {
                let detail = request_job(&client, job_id).await?;
                if detail.job.state.is_terminal() {
                    print_job(&detail, json)?;
                    if detail.job.state != JobState::Succeeded {
                        anyhow::bail!(
                            "job {} finished with state {}",
                            detail.job.spec.id,
                            detail.job.state.as_str()
                        );
                    }
                    break;
                }
                tokio::time::sleep(Duration::from_millis(250)).await;
            }
        }
        Command::Cancel {
            job_id,
            grace_seconds,
            json,
        } => {
            let effective = effective_config(&overrides, &cwd)?;
            let response = Client::new(&effective.paths)
                .request(
                    DaemonRole::Worker,
                    Request::JobCancel {
                        job_id,
                        grace_seconds,
                    },
                )
                .await?;
            let detail = match response {
                Response::Cancelled(detail) => detail,
                response => anyhow::bail!("unexpected {response:?} cancellation response"),
            };
            if json {
                println!("{}", serde_json::to_string_pretty(&detail)?);
            } else if detail.job.state == JobState::Cancelled {
                println!("cancelled {}", detail.job.spec.id);
            } else {
                println!("cancellation requested for {}", detail.job.spec.id);
            }
        }
        Command::Retry { job_id, json } => {
            let effective = effective_config(&overrides, &cwd)?;
            let response = Client::new(&effective.paths)
                .request(DaemonRole::Worker, Request::JobRetry { job_id })
                .await?;
            let detail = match response {
                Response::Retried(detail) => detail,
                response => anyhow::bail!("unexpected {response:?} retry response"),
            };
            if json {
                println!("{}", serde_json::to_string_pretty(&detail)?);
            } else {
                let attempt = detail
                    .attempts
                    .last()
                    .ok_or_else(|| anyhow::anyhow!("retried job has no attempt"))?;
                println!(
                    "retried {} as attempt {} (sequence {})",
                    detail.job.spec.id,
                    attempt.spec.id(),
                    attempt.spec.sequence()
                );
            }
        }
        Command::Logs { job_id, follow } => {
            let effective = effective_config(&overrides, &cwd)?;
            show_logs(&Client::new(&effective.paths), job_id, follow).await?;
        }
    }
    Ok(())
}

async fn install_user_services() -> anyhow::Result<()> {
    let directory = user_service_directory()?;
    let binary = std::env::current_exe()?.canonicalize()?;
    let xdg = service::XdgDirectories {
        config: absolute_xdg("XDG_CONFIG_HOME"),
        state: absolute_xdg("XDG_STATE_HOME"),
        runtime: absolute_xdg("XDG_RUNTIME_DIR"),
    };
    let units = service::render_units(&binary, &xdg).map_err(anyhow::Error::msg)?;
    fs::create_dir_all(&directory)?;
    let mut changed = false;
    let mut pending = Vec::new();
    for unit in &units {
        let path = directory.join(unit.name);
        match fs::symlink_metadata(&path) {
            Ok(metadata) if !metadata.file_type().is_file() => {
                anyhow::bail!(
                    "refusing non-regular managed service unit {}",
                    path.display()
                );
            }
            Ok(_) => {
                let previous = fs::read(&path)?;
                if previous == unit.contents.as_bytes() {
                    continue;
                }
                if !managed_unit_identity(&previous, unit.name) {
                    anyhow::bail!(
                        "refusing to overwrite unrelated service unit {}",
                        path.display()
                    );
                }
                changed = true;
                pending.push((
                    path.clone(),
                    Some(previous),
                    unit.contents.as_bytes().to_vec(),
                ));
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                changed = true;
                pending.push((path, None, unit.contents.as_bytes().to_vec()));
            }
            Err(error) => return Err(error.into()),
        }
    }
    let mut staged = Vec::new();
    let staging = (|| -> anyhow::Result<()> {
        for (index, (path, original, contents)) in pending.into_iter().enumerate() {
            let temporary =
                path.with_file_name(format!(".igor-unit-{}-{index}.tmp", std::process::id()));
            let mut file = fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&temporary)?;
            staged.push((temporary.clone(), path, original));
            file.write_all(&contents)?;
            file.sync_all()?;
        }
        Ok(())
    })();
    if let Err(error) = staging {
        for (temporary, _, _) in &staged {
            let _ = fs::remove_file(temporary);
        }
        return Err(error);
    }
    let mut installed = 0;
    for (temporary, path, _) in &staged {
        if let Err(error) = fs::rename(temporary, path) {
            rollback_units(&staged, installed);
            return Err(anyhow::anyhow!(
                "failed installing {}: {error}",
                path.display()
            ));
        }
        installed += 1;
    }
    if changed {
        let reload = reload_user_units().await;
        if let Err(error) = reload {
            rollback_units(&staged, installed);
            let _ = reload_user_units().await;
            return Err(error);
        }
    }
    Ok(())
}

fn managed_unit_identity(contents: &[u8], name: &str) -> bool {
    let Ok(contents) = std::str::from_utf8(contents) else {
        return false;
    };
    let (description, role) = match name {
        "igor-worker.service" => ("Igor worker", "worker"),
        "igor-supervisor.service" => ("Igor supervisor", "supervisor"),
        _ => return false,
    };
    let mut section = "";
    let mut found_description = false;
    let mut found_type = false;
    let mut found_exec = false;
    for line in contents.lines().map(str::trim) {
        if line.starts_with('[') && line.ends_with(']') {
            section = &line[1..line.len() - 1];
        } else if let Some((key, value)) = line.split_once('=') {
            match (section, key.trim(), value.trim()) {
                ("Unit", "Description", value) if value == description => found_description = true,
                ("Service", "Type", "simple") => found_type = true,
                ("Service", "ExecStart", value) => {
                    found_exec = value
                        .strip_prefix('"')
                        .and_then(|quoted| quoted.rsplit_once('"'))
                        .is_some_and(|(binary, arguments)| {
                            Path::new(binary).is_absolute()
                                && Path::new(binary)
                                    .file_name()
                                    .is_some_and(|name| name == "igor")
                                && arguments.trim() == role
                        });
                }
                _ => {}
            }
        }
    }
    found_description && found_type && found_exec
}

fn rollback_units(staged: &[(PathBuf, PathBuf, Option<Vec<u8>>)], installed: usize) {
    for (_, path, original) in staged.iter().take(installed) {
        match original {
            Some(contents) => {
                let _ = fs::write(path, contents);
            }
            None => {
                let _ = fs::remove_file(path);
            }
        }
    }
    for (temporary, _, _) in staged {
        let _ = fs::remove_file(temporary);
    }
}

async fn reload_user_units() -> anyhow::Result<()> {
    systemctl_user(&["daemon-reload"]).await
}

async fn systemctl_user(arguments: &[&str]) -> anyhow::Result<()> {
    let status = tokio::time::timeout(
        SYSTEMCTL_TIMEOUT,
        tokio::process::Command::new("systemctl")
            .args(["--user"])
            .args(arguments)
            .stdin(Stdio::null())
            .kill_on_drop(true)
            .status(),
    )
    .await
    .map_err(|_| anyhow::anyhow!("systemctl --user {} timed out", arguments.join(" ")))??;
    if !status.success() {
        anyhow::bail!(
            "systemctl --user {} failed with {status}",
            arguments.join(" ")
        );
    }
    Ok(())
}

async fn manage_user_services(action: &str, now: bool) -> anyhow::Result<()> {
    let directory = user_service_directory()?;
    for name in USER_SERVICE_NAMES {
        let path = directory.join(name);
        let metadata = match fs::symlink_metadata(&path) {
            Ok(metadata) => metadata,
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                anyhow::bail!("managed service unit is not installed: {}", path.display())
            }
            Err(error) => return Err(error.into()),
        };
        if !metadata.file_type().is_file() || !managed_unit_identity(&fs::read(&path)?, name) {
            anyhow::bail!("managed service unit is not installed: {}", path.display());
        }
    }
    let mut arguments = vec![action];
    if now {
        arguments.push("--now");
    }
    arguments.push("--");
    arguments.extend(USER_SERVICE_NAMES);
    systemctl_user(&arguments).await
}

#[derive(Debug, Serialize)]
struct ServiceUnitStatus {
    unit: &'static str,
    load_state: String,
    active_state: String,
    unit_file_state: String,
}

async fn show_service_status(json: bool) -> anyhow::Result<()> {
    let mut statuses = Vec::with_capacity(USER_SERVICE_NAMES.len());
    for unit in USER_SERVICE_NAMES {
        let output = tokio::time::timeout(
            SYSTEMCTL_TIMEOUT,
            tokio::process::Command::new("systemctl")
                .args([
                    "--user",
                    "show",
                    "--property=LoadState,ActiveState,UnitFileState",
                    "--",
                    unit,
                ])
                .stdin(Stdio::null())
                .output(),
        )
        .await
        .map_err(|_| anyhow::anyhow!("systemctl --user show {unit} timed out"))??;
        if !output.status.success() {
            anyhow::bail!(
                "systemctl --user show {unit} failed with {}: {}",
                output.status,
                String::from_utf8_lossy(&output.stderr).trim()
            );
        }
        let values = parse_unit_properties(&String::from_utf8(output.stdout)?)?;
        statuses.push(ServiceUnitStatus {
            unit,
            load_state: values.0,
            active_state: values.1,
            unit_file_state: values.2,
        });
    }
    if json {
        println!("{}", serde_json::to_string_pretty(&statuses)?);
    } else {
        println!("UNIT\tLOAD\tACTIVE\tENABLEMENT");
        for status in statuses {
            println!(
                "{}\t{}\t{}\t{}",
                status.unit, status.load_state, status.active_state, status.unit_file_state
            );
        }
    }
    Ok(())
}

fn parse_unit_properties(output: &str) -> anyhow::Result<(String, String, String)> {
    let mut load = None;
    let mut active = None;
    let mut enabled = None;
    for line in output.lines() {
        let Some((key, value)) = line.split_once('=') else {
            continue;
        };
        let target = match key {
            "LoadState" => &mut load,
            "ActiveState" => &mut active,
            "UnitFileState" => &mut enabled,
            _ => continue,
        };
        *target = Some(value.to_owned());
    }
    Ok((
        load.ok_or_else(|| anyhow::anyhow!("systemctl output missing LoadState"))?,
        active.ok_or_else(|| anyhow::anyhow!("systemctl output missing ActiveState"))?,
        enabled.ok_or_else(|| anyhow::anyhow!("systemctl output missing UnitFileState"))?,
    ))
}

async fn show_service_logs(follow: bool) -> anyhow::Result<()> {
    let mut command = tokio::process::Command::new("journalctl");
    command.args(["--user", "--no-pager"]);
    if follow {
        command.arg("--follow");
    }
    for unit in USER_SERVICE_NAMES {
        command.args(["-u", unit]);
    }
    command.stdin(Stdio::null()).kill_on_drop(true);
    let result = if follow {
        command.status().await?
    } else {
        tokio::time::timeout(JOURNALCTL_TIMEOUT, command.status())
            .await
            .map_err(|_| anyhow::anyhow!("journalctl --user timed out"))??
    };
    if !result.success() {
        anyhow::bail!("journalctl --user failed with {result}");
    }
    Ok(())
}

fn absolute_xdg(name: &str) -> Option<PathBuf> {
    std::env::var_os(name)
        .map(PathBuf::from)
        .filter(|path| path.is_absolute())
}

fn set_host_config(overrides: &ConfigOverrides, arguments: ConfigSetArgs) -> anyhow::Result<()> {
    let has_update = arguments.max_concurrent_jobs.is_some()
        || arguments.memory_bytes.is_some()
        || arguments.auto_memory
        || arguments.cpu_threads.is_some()
        || arguments.auto_cpu
        || !arguments.gpus.is_empty()
        || arguments.disable_gpus
        || arguments.auto_gpus;
    if !has_update {
        anyhow::bail!("provide at least one host scheduling limit to update");
    }
    let (gpus, discover_gpus) = if !arguments.gpus.is_empty() {
        (Some(arguments.gpus), Some(false))
    } else if arguments.disable_gpus {
        (Some(Vec::new()), Some(false))
    } else if arguments.auto_gpus {
        (Some(Vec::new()), Some(true))
    } else {
        (None, None)
    };
    let update = HostConfigUpdate {
        cpu_threads: arguments
            .cpu_threads
            .map(Some)
            .or(arguments.auto_cpu.then_some(None)),
        memory_bytes: arguments
            .memory_bytes
            .map(Some)
            .or(arguments.auto_memory.then_some(None)),
        gpus,
        discover_gpus,
        max_concurrent_jobs: arguments.max_concurrent_jobs,
    };
    let environment = Environment::from_process()?;
    let path = select_global_config_path(&environment, overrides);
    update_global_host_config(&path, &update)?;
    println!(
        "updated {}; restart the worker to apply changes",
        path.display()
    );
    Ok(())
}

fn parse_memory_size(value: &str) -> Result<u64, String> {
    let value = value.trim();
    let split = value
        .find(|character: char| !character.is_ascii_digit())
        .unwrap_or(value.len());
    let number = value[..split]
        .parse::<u64>()
        .map_err(|_| "memory size must start with a positive integer".to_owned())?;
    if number == 0 {
        return Err("memory size must be positive".into());
    }
    let unit = value[split..].trim().to_ascii_uppercase();
    let multiplier = match unit.as_str() {
        "" | "B" => 1,
        "KB" => 1_000,
        "MB" => 1_000_000,
        "GB" => 1_000_000_000,
        "TB" => 1_000_000_000_000,
        "KIB" => 1_024,
        "MIB" => 1_048_576,
        "GIB" => 1_073_741_824,
        "TIB" => 1_099_511_627_776,
        _ => {
            return Err(
                "supported memory units are B, KB, MB, GB, TB, KiB, MiB, GiB, and TiB".into(),
            );
        }
    };
    number
        .checked_mul(multiplier)
        .ok_or_else(|| "memory size is too large".into())
}

fn effective_config(
    overrides: &ConfigOverrides,
    cwd: &std::path::Path,
) -> anyhow::Result<EffectiveConfig> {
    let environment = Environment::from_process()?;
    Ok(load_effective_config(&environment, overrides, cwd)?)
}

async fn check_database(path: &Path, json: bool) -> anyhow::Result<()> {
    let latest = Database::latest_schema_version();
    let observed = match run_database_check(path).await {
        Ok(schema_version) => schema_version,
        Err(error) => anyhow::bail!("database check failed for {}: {error:#}", path.display()),
    };
    if json {
        println!(
            "{}",
            serde_json::to_string_pretty(&serde_json::json!({
                "status": "ok",
                "schema_version": observed,
                "latest_schema_version": latest,
            }))?
        );
    } else {
        println!(
            "database check passed: integrity ok, foreign keys ok, schema version {observed} matches latest {latest}"
        );
    }
    Ok(())
}

async fn run_database_check(path: &Path) -> anyhow::Result<i64> {
    if !path.exists() {
        anyhow::bail!(
            "database file does not exist; run the worker once to initialize it or pass --database <path>"
        );
    }
    let schema_version = validate_database(path).await?;
    let latest = Database::latest_schema_version();
    if schema_version != latest {
        anyhow::bail!(
            "schema version {schema_version} does not match latest {latest}; start the worker once to migrate the database"
        );
    }
    Ok(schema_version)
}

async fn migrate_database(path: &Path, json: bool, dry_run: bool) -> anyhow::Result<()> {
    if !path.exists() {
        anyhow::bail!("database file does not exist; refusing to create it");
    }
    let before = validate_database(path).await?;
    let latest = Database::latest_schema_version();
    if before > latest {
        anyhow::bail!(
            "schema version {before} is newer than supported version {latest}; use a compatible Igor version"
        );
    }
    if dry_run {
        if json {
            println!(
                "{}",
                serde_json::to_string_pretty(&serde_json::json!({
                    "status": "dry_run",
                    "schema_version_before": before,
                    "latest_schema_version": latest,
                    "would_migrate": before != latest,
                }))?
            );
        } else if before == latest {
            println!("database schema is already current at version {before}; no migration needed");
        } else {
            println!("would migrate database from schema version {before} to {latest}");
        }
        return Ok(());
    }
    let database = Database::open_with_options(
        path,
        DatabaseOptions {
            writable: true,
            create_if_missing: false,
            ..DatabaseOptions::default()
        },
    )
    .await
    .map_err(|error| anyhow::anyhow!("database migration failed: {error}"))?;
    let after = database.schema_version().await?;
    database.integrity_check(IntegrityCheck::Full).await?;
    database.foreign_key_check().await?;
    database.pool().close().await;
    if json {
        println!(
            "{}",
            serde_json::to_string_pretty(&serde_json::json!({
                "status": if before == after { "unchanged" } else { "migrated" },
                "schema_version_before": before,
                "schema_version_after": after,
                "latest_schema_version": Database::latest_schema_version(),
            }))?
        );
    } else if before == after {
        println!("database schema is already current at version {after}");
    } else {
        println!("database migrated from schema version {before} to {after}");
    }
    Ok(())
}

async fn validate_database(path: &Path) -> anyhow::Result<i64> {
    let database = Database::open_with_options(
        path,
        DatabaseOptions {
            writable: false,
            ..DatabaseOptions::default()
        },
    )
    .await?;
    let version = database.schema_version().await?;
    database.integrity_check(IntegrityCheck::Full).await?;
    database.foreign_key_check().await?;
    database.pool().close().await;
    Ok(version)
}

async fn backup_database(source: &Path, destination: &Path, json: bool) -> anyhow::Result<()> {
    if !source.exists() {
        anyhow::bail!("database file does not exist; refusing to create it");
    }
    let database = Database::open_with_options(
        source,
        DatabaseOptions {
            writable: false,
            ..DatabaseOptions::default()
        },
    )
    .await?;
    let version = database.schema_version().await?;
    database.backup(destination).await?;
    database.pool().close().await;
    if json {
        println!(
            "{}",
            serde_json::to_string_pretty(&serde_json::json!({
                "status": "ok",
                "source": source,
                "destination": destination,
                "source_schema_version": version,
            }))?
        );
    } else {
        println!(
            "database backed up to {} (source schema version {version})",
            destination.display()
        );
    }
    Ok(())
}

async fn submit(
    effective: &EffectiveConfig,
    cwd: &std::path::Path,
    arguments: SubmitArgs,
) -> anyhow::Result<()> {
    let client = Client::new(&effective.paths);
    let loaded = effective
        .project
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("no project configuration found; run `igor init` first"))?;
    let project = selected_registered_project(&client, effective)
        .await?
        .ok_or_else(|| {
            anyhow::anyhow!(
                "project is not registered; run `igor project add {}`",
                loaded.project_root.display()
            )
        })?;
    if arguments.file.is_some() && !arguments.command.is_empty() {
        anyhow::bail!("--file cannot be combined with a direct command");
    }
    if arguments.shell.is_some() && !arguments.command.is_empty() {
        anyhow::bail!("--shell cannot be combined with a direct command");
    }
    let mut input = if let Some(file) = arguments.file {
        load_job_file(&absolute(cwd, &file))?.into_input(&project.root)?
    } else if let Some(shell) = arguments.shell {
        SubmissionInput {
            name: None,
            priority: 0,
            allow_dirty: false,
            command: CommandSpec {
                program: "/bin/sh".into(),
                args: vec!["-c".into(), shell],
                cwd: project.root.clone(),
                shell: ShellPolicy::Shell,
                environment: EnvironmentPolicy::default(),
            },
            executor: None,
            resources: None,
            scientific_configurations: Vec::new(),
            immutable_inputs: Vec::new(),
        }
    } else {
        let mut command = arguments.command.into_iter();
        let program = command.next().ok_or_else(|| {
            anyhow::anyhow!("provide a command after `--`, use --shell, or use --file")
        })?;
        SubmissionInput {
            name: None,
            priority: 0,
            allow_dirty: false,
            command: CommandSpec {
                program,
                args: command.collect(),
                cwd: project.root.clone(),
                shell: ShellPolicy::Direct,
                environment: EnvironmentPolicy::default(),
            },
            executor: None,
            resources: None,
            scientific_configurations: Vec::new(),
            immutable_inputs: Vec::new(),
        }
    };
    if arguments.name.is_some() {
        input.name = arguments.name;
    }
    if let Some(priority) = arguments.priority {
        input.priority = priority;
    }
    input.allow_dirty |= arguments.allow_dirty;
    input
        .scientific_configurations
        .extend(arguments.scientific_configurations);
    input.immutable_inputs.extend(arguments.immutable_inputs);
    let response = client
        .request(
            DaemonRole::Worker,
            Request::Submit {
                project_id: project.id,
                input: Box::new(input),
            },
        )
        .await?;
    let job = match response {
        Response::Submitted(job) => job,
        response => anyhow::bail!("unexpected {response:?} submission response"),
    };
    if arguments.json {
        println!("{}", serde_json::to_string_pretty(&job)?);
    } else {
        println!("submitted {} ({})", job.spec.id, job.spec.name);
    }
    Ok(())
}

async fn family_submit(
    effective: &EffectiveConfig,
    cwd: &std::path::Path,
    arguments: FamilySubmitArgs,
) -> anyhow::Result<()> {
    let client = Client::new(&effective.paths);
    let loaded = effective
        .project
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("no project configuration found; run `igor init` first"))?;
    let project = selected_registered_project(&client, effective)
        .await?
        .ok_or_else(|| {
            anyhow::anyhow!(
                "project is not registered; run `igor project add {}`",
                loaded.project_root.display()
            )
        })?;
    let file = load_family_file(&absolute(cwd, &arguments.file))?;
    let response = client
        .request(
            DaemonRole::Worker,
            Request::FamilySubmit {
                project_id: project.id,
                file: Box::new(file),
            },
        )
        .await?;
    let (family_id, generation_id, jobs) = match response {
        Response::FamilySubmitted {
            family_id,
            generation_id,
            jobs,
        } => (family_id, generation_id, jobs),
        response => anyhow::bail!("unexpected {response:?} family submission response"),
    };
    if arguments.json {
        let payload = serde_json::json!({
            "family_id": family_id,
            "generation_id": generation_id,
            "jobs": jobs,
        });
        println!("{}", serde_json::to_string_pretty(&payload)?);
    } else {
        println!(
            "submitted family {} generation {} ({} jobs)",
            family_id,
            generation_id,
            jobs.len()
        );
        for job in &jobs {
            println!("{} {}", job.spec.id, job.spec.name);
        }
    }
    Ok(())
}

fn print_family(family: &FamilyDetail, json: bool) -> anyhow::Result<()> {
    if json {
        println!("{}", serde_json::to_string_pretty(family)?);
        return Ok(());
    }
    println!("family {} ({})", family.family.id, family.family.name);
    for generation in &family.generations {
        println!(
            "generation {} {} revision {} protocol {} status {}{}",
            generation.identity.number,
            generation.identity.id,
            generation.identity.source_revision,
            generation.identity.protocol_digest,
            match generation.status {
                igor_core::GenerationStatus::Incomplete => "incomplete",
                igor_core::GenerationStatus::Complete => "complete",
                igor_core::GenerationStatus::Comparable => "comparable",
                igor_core::GenerationStatus::Invalid => "invalid",
            },
            if family.current_generation_id == Some(generation.identity.id) {
                " (current)"
            } else {
                ""
            }
        );
        for job in &generation.jobs {
            println!(
                "  {}\t{}\t{}",
                job.spec.id,
                job.spec.name,
                job.state.as_str()
            );
        }
        println!("  counts: {}", format_counts(&generation.counts));
    }
    println!("total: {}", format_counts(&family.counts));
    Ok(())
}

async fn family_supersede(
    effective: &EffectiveConfig,
    cwd: &Path,
    family_id: FamilyId,
    file: &Path,
    json: bool,
) -> anyhow::Result<()> {
    let client = Client::new(&effective.paths);
    let loaded = effective
        .project
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("no project configuration found; run `igor init` first"))?;
    let project = selected_registered_project(&client, effective)
        .await?
        .ok_or_else(|| {
            anyhow::anyhow!(
                "project is not registered; run `igor project add {}`",
                loaded.project_root.display()
            )
        })?;
    let file = load_family_file(&absolute(cwd, file))?;
    let response = client
        .request(
            DaemonRole::Worker,
            Request::FamilySupersede {
                project_id: project.id,
                family_id,
                file: Box::new(file),
            },
        )
        .await?;
    let (previous_generation_id, generation_id, jobs) = match response {
        Response::FamilySuperseded {
            previous_generation_id,
            generation_id,
            jobs,
            ..
        } => (previous_generation_id, generation_id, jobs),
        response => anyhow::bail!("unexpected {response:?} family supersession response"),
    };
    if json {
        println!(
            "{}",
            serde_json::to_string_pretty(&serde_json::json!({
                "family_id": family_id,
                "previous_generation_id": previous_generation_id,
                "generation_id": generation_id,
                "jobs": jobs,
            }))?
        );
    } else {
        println!(
            "superseded generation {previous_generation_id} with {generation_id} ({} jobs)",
            jobs.len()
        );
        for job in &jobs {
            println!("{} {}", job.spec.id, job.spec.name);
        }
    }
    Ok(())
}

fn format_counts(counts: &std::collections::BTreeMap<String, u64>) -> String {
    counts
        .iter()
        .map(|(state, count)| format!("{state}={count}"))
        .collect::<Vec<_>>()
        .join(" ")
}

async fn selected_registered_project(
    client: &Client,
    effective: &EffectiveConfig,
) -> anyhow::Result<Option<Project>> {
    let Some(loaded) = &effective.project else {
        return Ok(None);
    };
    match client
        .request(
            DaemonRole::Worker,
            Request::ProjectByRoot {
                root: loaded.project_root.canonicalize()?,
            },
        )
        .await?
    {
        Response::OptionalProject { project } => Ok(project),
        response => anyhow::bail!("unexpected {response:?} project lookup response"),
    }
}

async fn request_job(client: &Client, job_id: JobId) -> anyhow::Result<JobDetail> {
    match client
        .request(DaemonRole::Worker, Request::JobShow { job_id })
        .await?
    {
        Response::Job(job) => Ok(job),
        response => anyhow::bail!("unexpected {response:?} job response"),
    }
}

async fn request_events(client: &Client, job_id: JobId) -> anyhow::Result<Vec<StoredEvent>> {
    match client
        .request(DaemonRole::Worker, Request::JobEvents { job_id })
        .await?
    {
        Response::Events { events } => Ok(events),
        response => anyhow::bail!("unexpected {response:?} event response"),
    }
}

async fn request_logs(client: &Client, job_id: JobId) -> anyhow::Result<JobLogs> {
    match client
        .request(DaemonRole::Worker, Request::JobLogs { job_id })
        .await?
    {
        Response::Logs(logs) => Ok(logs),
        response => anyhow::bail!("unexpected {response:?} job-logs response"),
    }
}

async fn show_logs(client: &Client, job_id: JobId, follow: bool) -> anyhow::Result<()> {
    let logs = loop {
        let logs = request_logs(client, job_id).await?;
        if logs.stdout_path.is_some() && logs.stderr_path.is_some() {
            break logs;
        }
        if !follow || logs.state.is_terminal() {
            anyhow::bail!("logs are not available for attempt {}", logs.attempt_id);
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    };
    let (stdout_path, stderr_path) = match (logs.stdout_path, logs.stderr_path) {
        (Some(stdout), Some(stderr)) => (stdout, stderr),
        _ => anyhow::bail!("logs are not available for attempt {}", logs.attempt_id),
    };
    let mut stdout_offset = 0;
    let mut stderr_offset = 0;
    loop {
        stdout_offset += emit_log(&stdout_path, stdout_offset, false)?;
        stderr_offset += emit_log(&stderr_path, stderr_offset, true)?;
        if !follow {
            break;
        }
        let detail = request_job(client, job_id).await?;
        if detail.job.state.is_terminal() {
            emit_log(&stdout_path, stdout_offset, false)?;
            emit_log(&stderr_path, stderr_offset, true)?;
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    Ok(())
}

fn emit_log(path: &Path, offset: u64, stderr: bool) -> anyhow::Result<u64> {
    let mut file = File::open(path)?;
    file.seek(SeekFrom::Start(offset))?;
    let copied = if stderr {
        let mut output = std::io::stderr().lock();
        let copied = std::io::copy(&mut file, &mut output)?;
        output.flush()?;
        copied
    } else {
        let mut output = std::io::stdout().lock();
        let copied = std::io::copy(&mut file, &mut output)?;
        output.flush()?;
        copied
    };
    Ok(copied)
}

fn print_project(project: &Project, json: bool) -> anyhow::Result<()> {
    if json {
        println!("{}", serde_json::to_string_pretty(project)?);
    } else {
        println!(
            "{}\t{}\t{}",
            project.id,
            project.name,
            project.root.display()
        );
    }
    Ok(())
}

fn print_jobs(jobs: &[StoredJob], json: bool) -> anyhow::Result<()> {
    if json {
        println!("{}", serde_json::to_string_pretty(jobs)?);
    } else {
        for job in jobs {
            println!(
                "{}\t{}\t{}\t{}\t{}",
                job.spec.id,
                job.state.as_str(),
                job.priority,
                job.submission_order,
                job.spec.name
            );
        }
    }
    Ok(())
}

fn print_resources(resources: &[ResourceStatus], json: bool) -> anyhow::Result<()> {
    if json {
        println!("{}", serde_json::to_string_pretty(resources)?);
    } else {
        println!("KIND\tNAME\tCAPACITY\tAVAILABLE\tLEASES");
        for status in resources {
            println!(
                "{}\t{}\t{}\t{}\t{}",
                status.resource.kind,
                status.resource.name,
                status.resource.capacity,
                status.resource.metadata["available"]
                    .as_bool()
                    .unwrap_or(true),
                status.leases.len()
            );
        }
    }
    Ok(())
}

fn print_job(detail: &JobDetail, json: bool) -> anyhow::Result<()> {
    if json {
        println!("{}", serde_json::to_string_pretty(detail)?);
    } else {
        println!(
            "job {}\nname: {}\nproject: {}\nstate: {}\npriority: {}\nsubmitted: {}",
            detail.job.spec.id,
            detail.job.spec.name,
            detail.job.spec.project_id,
            detail.job.state.as_str(),
            detail.job.priority,
            detail.job.submitted_at
        );
        for attempt in &detail.attempts {
            println!(
                "attempt {}\tsequence {}\t{}",
                attempt.spec.id(),
                attempt.spec.sequence(),
                attempt.state.as_str()
            );
        }
    }
    Ok(())
}

fn print_events(events: &[StoredEvent], json: bool) -> anyhow::Result<()> {
    if json {
        println!("{}", serde_json::to_string_pretty(events)?);
    } else {
        for stored in events {
            println!(
                "{}\t{}\t{}\t{}\t{}\t{}\t{}",
                stored.sequence,
                stored.event.id,
                stored.event.kind.as_str(),
                stored.project_id,
                stored
                    .job_id
                    .map(|id| id.to_string())
                    .unwrap_or_else(|| "-".into()),
                stored
                    .attempt_id
                    .map(|id| id.to_string())
                    .unwrap_or_else(|| "-".into()),
                stored.occurred_at
            );
        }
    }
    Ok(())
}

fn expect_project(response: Response) -> anyhow::Result<Project> {
    match response {
        Response::Project(project) => Ok(project),
        response => anyhow::bail!("unexpected {response:?} project response"),
    }
}

async fn uninstall_user_services(yes: bool, dry_run: bool) -> anyhow::Result<()> {
    let unit_directory = user_service_directory()?;
    let mut installed = Vec::new();
    for name in &USER_SERVICE_NAMES {
        let path = unit_directory.join(name);
        match fs::symlink_metadata(&path) {
            Ok(metadata) if !metadata.file_type().is_file() => anyhow::bail!(
                "refusing non-regular managed service unit {}",
                path.display()
            ),
            Ok(_) if managed_unit_identity(&fs::read(&path)?, name) => installed.push((name, path)),
            Ok(_) => anyhow::bail!(
                "refusing to uninstall unrelated service unit {}",
                path.display()
            ),
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
    }
    if installed.is_empty() {
        println!("no Igor user services are installed");
        return Ok(());
    }
    let staging_paths: Vec<_> = installed
        .iter()
        .enumerate()
        .map(|(index, (_, path))| {
            path.with_file_name(format!(
                ".igor-unit-{}-uninstall-{index}.tmp",
                std::process::id()
            ))
        })
        .collect();
    for temporary in &staging_paths {
        match fs::symlink_metadata(temporary) {
            Ok(_) => anyhow::bail!(
                "refusing to reuse existing uninstall staging path {}",
                temporary.display()
            ),
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
    }
    if dry_run {
        for (_, path) in installed {
            println!("would stop and remove {}", path.display());
        }
        return Ok(());
    }
    if !yes {
        print!("stop and remove Igor's user services? [y/N] ");
        io::stdout().flush()?;
        let mut answer = String::new();
        io::stdin().read_line(&mut answer)?;
        if !matches!(answer.trim(), "y" | "Y" | "yes" | "YES") {
            anyhow::bail!("service uninstall cancelled");
        }
    }

    let mut disable = tokio::process::Command::new("systemctl");
    disable
        .args(["--user", "disable", "--now", "--"])
        .args(installed.iter().map(|(name, _)| **name))
        .stdin(Stdio::null())
        .kill_on_drop(true);
    let status = tokio::time::timeout(SYSTEMCTL_TIMEOUT, disable.status())
        .await
        .map_err(|_| anyhow::anyhow!("systemctl --user disable --now timed out"))??;
    if !status.success() {
        anyhow::bail!("systemctl --user disable --now failed with {status}");
    }
    // Stage the owned units by rename inside their own directory so their bytes
    // and permissions survive a failed reload, then delete them once the manager
    // accepted the change.
    let restore_staged = |staged: &[(PathBuf, PathBuf)]| {
        for (temporary, path) in staged {
            if let Err(error) = fs::rename(temporary, path) {
                eprintln!("could not restore staged unit {}: {error}", path.display());
            }
        }
    };
    let mut staged: Vec<(PathBuf, PathBuf)> = Vec::new();
    let mut staging_error = None;
    for ((_, path), temporary) in installed.iter().zip(&staging_paths) {
        let staged_one: anyhow::Result<()> = match fs::symlink_metadata(temporary) {
            Ok(_) => Err(anyhow::anyhow!(
                "refusing to reuse existing uninstall staging path {}",
                temporary.display()
            )),
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                match fs::rename(path, temporary) {
                    Ok(()) => {
                        staged.push((temporary.clone(), path.clone()));
                        Ok(())
                    }
                    Err(error) => Err(anyhow::anyhow!(
                        "failed staging {} for removal: {error}",
                        path.display()
                    )),
                }
            }
            Err(error) => Err(error.into()),
        };
        if let Err(error) = staged_one {
            staging_error = Some(error);
            break;
        }
    }
    if let Some(error) = staging_error {
        restore_staged(&staged);
        let _ = reload_user_units().await;
        return Err(error);
    }
    if let Err(error) = reload_user_units().await {
        restore_staged(&staged);
        let _ = reload_user_units().await;
        return Err(error);
    }
    let mut removal_errors = Vec::new();
    for (temporary, path) in &staged {
        match fs::remove_file(temporary) {
            Ok(()) => println!("removed {}", path.display()),
            Err(error) => removal_errors.push(format!(
                "could not remove staged unit {}: {error}",
                temporary.display()
            )),
        }
    }
    if !removal_errors.is_empty() {
        anyhow::bail!("{}", removal_errors.join("; "));
    }
    Ok(())
}

fn user_service_directory() -> anyhow::Result<PathBuf> {
    let home = std::env::var_os("HOME")
        .map(PathBuf::from)
        .ok_or_else(|| anyhow::anyhow!("HOME is not set"))?;
    let config = std::env::var_os("XDG_CONFIG_HOME")
        .map(PathBuf::from)
        .filter(|path| path.is_absolute())
        .unwrap_or_else(|| home.join(".config"));
    Ok(config.join("systemd/user"))
}

fn absolute(cwd: &std::path::Path, path: &std::path::Path) -> PathBuf {
    if path.is_absolute() {
        path.to_path_buf()
    } else {
        cwd.join(path)
    }
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
        ClientError::NotFound { .. } => EXIT_NOT_FOUND,
        ClientError::Conflict { .. } => EXIT_CONFLICT,
        ClientError::InvalidResponse(_) => return None,
    };
    Some(ExitCode::from(code))
}
