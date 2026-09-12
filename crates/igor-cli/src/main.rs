use std::{path::PathBuf, process::ExitCode, time::Duration};

use clap::{Args, Parser, Subcommand};
use igor_core::{
    CommandSpec, ConfigOverrides, EffectiveConfig, Environment, EnvironmentPolicy, JobDetail,
    JobId, JobState, Project, ProjectId, ShellPolicy, StoredEvent, StoredJob, SubmissionInput,
    TransitionState, initialize_project, load_effective_config, load_job_file, load_project_config,
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
    /// Register and inspect projects.
    Project {
        #[command(subcommand)]
        command: ProjectCommand,
    },
    /// Submit a command or versioned job file.
    Submit(SubmitArgs),
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
                ConfigCommand::Path { json } => {
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
                    if json {
                        println!("{}", serde_json::to_string_pretty(&effective)?);
                    } else {
                        print!("{}", toml::to_string_pretty(&effective)?);
                    }
                }
                ConfigCommand::Check { json } => {
                    if json {
                        println!("{{\"valid\":true}}");
                    } else {
                        println!("configuration is valid");
                    }
                }
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
            }
        }
        Command::Submit(arguments) => {
            let effective = effective_config(&overrides, &cwd)?;
            submit(&effective, &cwd, arguments).await?;
        }
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
