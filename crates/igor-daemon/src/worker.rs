use std::{
    collections::BTreeMap,
    fs::{self, DirBuilder, OpenOptions},
    io,
    os::unix::{
        fs::{DirBuilderExt, OpenOptionsExt, PermissionsExt},
        process::{CommandExt, ExitStatusExt},
    },
    path::{Path, PathBuf},
    process::Stdio,
    time::Duration,
};

use igor_core::{
    AttemptState, Database, EnvironmentInheritance, ExecutionClaim, ExecutionOutcome, ExecutorSpec,
    ProcessIsolation, ProcessStart, RuntimePaths, environment_variable_is_sensitive,
};
use nix::{
    sys::signal::{Signal, killpg},
    unistd::Pid,
};
use tokio::{process::Command, sync::watch, time};

const CLAIM_DURATION: Duration = Duration::from_secs(30);
const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(5);
const CANCELLATION_INTERVAL: Duration = Duration::from_millis(100);
const IDLE_INTERVAL: Duration = Duration::from_millis(100);

pub async fn run(database: Database, paths: RuntimePaths, mut shutdown: watch::Receiver<bool>) {
    let owner = format!("worker:{}", std::process::id());
    loop {
        if *shutdown.borrow() {
            return;
        }
        match database
            .jobs()
            .claim_execution(&owner, CLAIM_DURATION)
            .await
        {
            Ok(Some(claim)) => execute(&database, &paths, &claim, &mut shutdown).await,
            Ok(None) => {
                tokio::select! {
                    _ = time::sleep(IDLE_INTERVAL) => {}
                    _ = shutdown.changed() => return,
                }
            }
            Err(error) => {
                tracing::error!(%error, "worker failed to claim queued execution");
                tokio::select! {
                    _ = time::sleep(IDLE_INTERVAL) => {}
                    _ = shutdown.changed() => return,
                }
            }
        }
    }
}

async fn execute(
    database: &Database,
    paths: &RuntimePaths,
    claim: &ExecutionClaim,
    shutdown: &mut watch::Receiver<bool>,
) {
    let ExecutorSpec::Process(process) = claim.attempt.spec.executor() else {
        finish_launch_failure(database, claim, "Docker execution is not implemented").await;
        return;
    };
    if process.isolation != ProcessIsolation::ProcessGroup {
        finish_launch_failure(
            database,
            claim,
            "systemd user-unit execution is not implemented",
        )
        .await;
        return;
    }
    let command = claim.attempt.spec.command();
    let (stdout_path, stderr_path) =
        match prepare_logs(&paths.log_dir, &claim.attempt.spec.id().to_string()) {
            Ok(paths) => paths,
            Err(error) => {
                finish_launch_failure(database, claim, &format!("cannot prepare logs: {error}"))
                    .await;
                return;
            }
        };
    let stdout = match open_log(&stdout_path) {
        Ok(file) => file,
        Err(error) => {
            finish_launch_failure(database, claim, &format!("cannot open stdout log: {error}"))
                .await;
            return;
        }
    };
    let stderr = match open_log(&stderr_path) {
        Ok(file) => file,
        Err(error) => {
            finish_launch_failure(database, claim, &format!("cannot open stderr log: {error}"))
                .await;
            return;
        }
    };
    let mut child_command = Command::new(&command.program);
    child_command
        .args(&command.args)
        .current_dir(&command.cwd)
        .stdin(Stdio::null())
        .stdout(Stdio::from(stdout))
        .stderr(Stdio::from(stderr));
    child_command.as_std_mut().process_group(0);
    if let Err(error) = configure_environment(&mut child_command, &command.environment) {
        finish_launch_failure(database, claim, &error.to_string()).await;
        return;
    }
    let mut child = match child_command.spawn() {
        Ok(child) => child,
        Err(error) => {
            finish_launch_failure(database, claim, &format!("cannot launch process: {error}"))
                .await;
            return;
        }
    };
    let Some(pid) = child.id() else {
        let _ = child.kill().await;
        finish_launch_failure(database, claim, "launched process has no PID").await;
        return;
    };
    let start_ticks = match process_start_ticks(pid) {
        Ok(start_ticks) => start_ticks,
        Err(error) => {
            terminate_group(pid);
            let _ = child.wait().await;
            finish_launch_failure(
                database,
                claim,
                &format!("cannot identify launched process: {error}"),
            )
            .await;
            return;
        }
    };
    let start = ProcessStart {
        pid: i64::from(pid),
        process_group_id: i64::from(pid),
        process_start_ticks: start_ticks,
        stdout_path,
        stderr_path,
    };
    if let Err(error) = database.jobs().record_process_started(claim, &start).await {
        terminate_group(pid);
        let _ = child.wait().await;
        tracing::error!(%error, job_id = %claim.job.spec.id, "cannot persist process identity");
        finish_launch_failure(database, claim, "cannot persist launched process identity").await;
        return;
    }

    let mut heartbeat = time::interval(HEARTBEAT_INTERVAL);
    heartbeat.set_missed_tick_behavior(time::MissedTickBehavior::Delay);
    heartbeat.tick().await;
    let mut cancellation = time::interval(CANCELLATION_INTERVAL);
    cancellation.set_missed_tick_behavior(time::MissedTickBehavior::Delay);
    let timeout = async {
        match claim.attempt.spec.resources().timeout_seconds {
            Some(seconds) => time::sleep(Duration::from_secs(seconds)).await,
            None => std::future::pending().await,
        }
    };
    tokio::pin!(timeout);
    let (status, interruption) = loop {
        tokio::select! {
            result = child.wait() => break (result, None),
            _ = cancellation.tick() => {
                match database.jobs().cancellation_grace(claim).await {
                    Ok(Some(grace)) => {
                        signal_group(pid, Signal::SIGTERM);
                        let grace_elapsed = time::sleep(grace);
                        tokio::pin!(grace_elapsed);
                        let mut group_poll = time::interval(Duration::from_millis(20));
                        group_poll.set_missed_tick_behavior(time::MissedTickBehavior::Delay);
                        group_poll.tick().await;
                        let mut leader_status = None;
                        let (status, state, reason) = loop {
                            tokio::select! {
                                status = child.wait(), if leader_status.is_none() => {
                                    if process_group_exists(pid) {
                                        leader_status = Some(status);
                                    } else {
                                        break (status, AttemptState::Cancelled, "execution cancelled");
                                    }
                                }
                                _ = group_poll.tick(), if leader_status.is_some() => {
                                    if !process_group_exists(pid)
                                        && let Some(status) = leader_status.take()
                                    {
                                        break (status, AttemptState::Cancelled, "execution cancelled");
                                    }
                                }
                                _ = cancellation.tick() => {
                                    match database.jobs().cancellation_grace(claim).await {
                                        Ok(Some(remaining)) => {
                                            let deadline = time::Instant::now() + remaining;
                                            if deadline < grace_elapsed.deadline() {
                                                grace_elapsed.as_mut().reset(deadline);
                                            }
                                        }
                                        Ok(None) => {}
                                        Err(error) => {
                                            tracing::error!(%error, job_id = %claim.job.spec.id, "cannot refresh cancellation request");
                                            terminate_group(pid);
                                            let status = match leader_status.take() {
                                                Some(status) => status,
                                                None => child.wait().await,
                                            };
                                            break (status, AttemptState::Failed, "cannot refresh cancellation request");
                                        }
                                    }
                                }
                                () = &mut grace_elapsed => {
                                    terminate_group(pid);
                                    let status = match leader_status.take() {
                                        Some(status) => status,
                                        None => child.wait().await,
                                    };
                                    break (status, AttemptState::Cancelled, "execution cancelled");
                                }
                                _ = shutdown.changed() => {
                                    terminate_group(pid);
                                    let status = match leader_status.take() {
                                        Some(status) => status,
                                        None => child.wait().await,
                                    };
                                    break (status, AttemptState::Failed, "worker shutdown interrupted execution");
                                }
                                _ = heartbeat.tick() => {
                                    if let Err(error) = database.jobs().heartbeat_execution(claim, CLAIM_DURATION).await {
                                        tracing::error!(%error, job_id = %claim.job.spec.id, "execution heartbeat failed during cancellation");
                                        terminate_group(pid);
                                        let status = match leader_status.take() {
                                            Some(status) => status,
                                            None => child.wait().await,
                                        };
                                        break (status, AttemptState::Failed, "execution heartbeat failed");
                                    }
                                }
                            }
                        };
                        break (status, Some((state, reason)));
                    }
                    Ok(None) => {}
                    Err(error) => {
                        tracing::error!(%error, job_id = %claim.job.spec.id, "cannot read cancellation request");
                        terminate_group(pid);
                        break (child.wait().await, Some((AttemptState::Failed, "cannot read cancellation request")));
                    }
                }
            }
            _ = heartbeat.tick() => {
                if let Err(error) = database.jobs().heartbeat_execution(claim, CLAIM_DURATION).await {
                    tracing::error!(%error, job_id = %claim.job.spec.id, "execution heartbeat failed");
                    terminate_group(pid);
                    break (child.wait().await, Some((AttemptState::Failed, "execution heartbeat failed")));
                }
            }
            () = &mut timeout => {
                terminate_group(pid);
                break (child.wait().await, Some((AttemptState::Failed, "configured execution timeout elapsed")));
            }
            _ = shutdown.changed() => {
                terminate_group(pid);
                break (child.wait().await, Some((AttemptState::Failed, "worker shutdown interrupted execution")));
            },
        }
    };
    let outcome = match (status, interruption) {
        (Ok(status), Some((state, reason))) => ExecutionOutcome {
            state,
            exit_code: status.code(),
            term_signal: status.signal(),
            error: Some(reason.into()),
        },
        (Ok(status), None) if status.success() => ExecutionOutcome {
            state: AttemptState::Succeeded,
            exit_code: status.code(),
            term_signal: status.signal(),
            error: None,
        },
        (Ok(status), None) => ExecutionOutcome {
            state: AttemptState::Failed,
            exit_code: status.code(),
            term_signal: status.signal(),
            error: None,
        },
        (Err(error), _) => ExecutionOutcome {
            state: AttemptState::Failed,
            exit_code: None,
            term_signal: None,
            error: Some(format!("cannot wait for process: {error}")),
        },
    };
    if let Err(error) = database.jobs().finish_execution(claim, &outcome).await {
        tracing::error!(%error, job_id = %claim.job.spec.id, "cannot persist execution outcome");
    }
}

async fn finish_launch_failure(database: &Database, claim: &ExecutionClaim, reason: &str) {
    let outcome = ExecutionOutcome {
        state: AttemptState::Failed,
        exit_code: None,
        term_signal: None,
        error: Some(reason.into()),
    };
    if let Err(error) = database.jobs().finish_execution(claim, &outcome).await {
        tracing::error!(%error, job_id = %claim.job.spec.id, "cannot persist launch failure");
    }
}

fn prepare_logs(log_dir: &Path, attempt_id: &str) -> io::Result<(PathBuf, PathBuf)> {
    let mut builder = DirBuilder::new();
    builder.recursive(true).mode(0o700).create(log_dir)?;
    ensure_real_directory(log_dir)?;
    fs::set_permissions(log_dir, fs::Permissions::from_mode(0o700))?;
    let attempt_dir = log_dir.join(attempt_id);
    DirBuilder::new().mode(0o700).create(&attempt_dir)?;
    ensure_real_directory(&attempt_dir)?;
    fs::set_permissions(&attempt_dir, fs::Permissions::from_mode(0o700))?;
    Ok((
        attempt_dir.join("stdout.log"),
        attempt_dir.join("stderr.log"),
    ))
}

fn ensure_real_directory(path: &Path) -> io::Result<()> {
    let metadata = fs::symlink_metadata(path)?;
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!(
                "{} must be a directory and not a symbolic link",
                path.display()
            ),
        ));
    }
    Ok(())
}

fn open_log(path: &Path) -> io::Result<fs::File> {
    OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(path)
}

fn configure_environment(
    command: &mut Command,
    policy: &igor_core::EnvironmentPolicy,
) -> io::Result<()> {
    if let Some(name) = policy
        .set
        .keys()
        .find(|name| environment_variable_is_sensitive(name))
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("sensitive environment variable {name:?} cannot be passed to a process"),
        ));
    }
    command.env_clear();
    let inherited: BTreeMap<_, _> = match policy.inherit {
        EnvironmentInheritance::None => BTreeMap::new(),
        EnvironmentInheritance::Minimal => std::env::vars_os()
            .filter(|(name, _)| minimal_environment_name(name.to_string_lossy().as_ref()))
            .collect(),
        EnvironmentInheritance::All => std::env::vars_os()
            .filter(|(name, _)| !environment_variable_is_sensitive(&name.to_string_lossy()))
            .collect(),
    };
    command.envs(inherited);
    command.envs(&policy.set);
    for name in &policy.remove {
        command.env_remove(name);
    }
    Ok(())
}

fn minimal_environment_name(name: &str) -> bool {
    matches!(
        name,
        "HOME" | "LANG" | "LOGNAME" | "PATH" | "TERM" | "TMPDIR" | "TZ" | "USER"
    ) || name.starts_with("LC_")
}

fn process_start_ticks(pid: u32) -> io::Result<i64> {
    let stat = fs::read_to_string(format!("/proc/{pid}/stat"))?;
    let end = stat
        .rfind(')')
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "missing process name"))?;
    let ticks = stat[end + 1..]
        .split_whitespace()
        .nth(19)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "missing process start time"))?
        .parse::<i64>()
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
    Ok(ticks)
}

fn terminate_group(pid: u32) {
    signal_group(pid, Signal::SIGKILL);
}

fn signal_group(pid: u32, signal: Signal) {
    if let Ok(pid) = i32::try_from(pid) {
        let _ = killpg(Pid::from_raw(pid), signal);
    }
}

fn process_group_exists(pid: u32) -> bool {
    i32::try_from(pid)
        .ok()
        .is_some_and(|pid| killpg(Pid::from_raw(pid), None::<Signal>).is_ok())
}
