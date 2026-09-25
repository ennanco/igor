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
    AttemptState, ContainerCreate, ContainerFinish, ContainerRecord, Database, DockerCommand,
    DockerCommandPlanner, DockerContainerInspection, DockerContainerState, DockerIdentity,
    DockerRecoveryIdentity, EnvironmentInheritance, ExecutionClaim, ExecutionOutcome, ExecutorSpec,
    ProcessIsolation, ProcessRecord, ProcessStart, RecoveredExecution, RuntimePaths, UnitRecord,
    UnitReservation, environment_variable_is_sensitive, validate_docker_mount_sources,
};
use nix::{
    sys::signal::{Signal, killpg},
    unistd::Pid,
};
use tokio::{process::Command, sync::watch, time};

use crate::systemd::{self, PlannedCommand, UnitOutcome};

const CLAIM_DURATION: Duration = Duration::from_secs(30);
const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(5);
const CANCELLATION_INTERVAL: Duration = Duration::from_millis(100);
const IDLE_INTERVAL: Duration = Duration::from_millis(100);

struct RecoveredProcessState {
    leader_present: bool,
    completion: Option<(AttemptState, &'static str)>,
    timeout_remaining: Option<Duration>,
}

enum DockerSupervision {
    Completed {
        logs: std::process::Output,
        wait: std::process::Output,
    },
    Cancellation(Duration),
    Timeout,
    Shutdown,
    OwnershipLost(String),
}

enum DockerInspectFailure {
    Missing,
    Indeterminate(String),
}

struct DockerLogFiles {
    stdout: fs::File,
    stderr: fs::File,
}

pub async fn run(database: Database, paths: RuntimePaths, mut shutdown: watch::Receiver<bool>) {
    let owner = format!("worker:{}", std::process::id());
    loop {
        if *shutdown.borrow() {
            return;
        }
        cleanup_pending(&database).await;
        match database.jobs().claim_recovery(&owner, CLAIM_DURATION).await {
            Ok(Some(recovered)) => reconcile(&database, &paths, recovered, &mut shutdown).await,
            Ok(None) => match database
                .jobs()
                .claim_execution(&owner, CLAIM_DURATION)
                .await
            {
                Ok(Some(claim)) => execute(&database, &paths, &claim, &mut shutdown).await,
                Ok(None) => idle(&mut shutdown).await,
                Err(error) => {
                    tracing::error!(%error, "worker failed to claim queued execution");
                    idle(&mut shutdown).await;
                }
            },
            Err(error) => {
                tracing::error!(%error, "worker failed to claim execution recovery");
                idle(&mut shutdown).await;
            }
        }
    }
}

async fn idle(shutdown: &mut watch::Receiver<bool>) {
    tokio::select! {
        _ = time::sleep(IDLE_INTERVAL) => {}
        _ = shutdown.changed() => {}
    }
}

async fn reconcile(
    database: &Database,
    paths: &RuntimePaths,
    recovered: RecoveredExecution,
    shutdown: &mut watch::Receiver<bool>,
) {
    if matches!(
        recovered.claim.attempt.spec.executor(),
        ExecutorSpec::Docker(_)
    ) {
        supervise_recovered_docker(database, paths, recovered, shutdown).await;
        return;
    }
    if matches!(recovered.claim.attempt.spec.executor(), ExecutorSpec::Process(spec) if spec.isolation == ProcessIsolation::SystemdUserUnit)
    {
        supervise_recovered_unit(database, recovered, shutdown).await;
        return;
    }
    let validation_started = time::Instant::now();
    let Some(process) = recovered.process else {
        finish_lost(
            database,
            &recovered.claim,
            "worker restarted before process identity was persisted",
        )
        .await;
        return;
    };
    let Ok(pid) = u32::try_from(process.pid) else {
        finish_lost(
            database,
            &recovered.claim,
            "persisted process PID is invalid",
        )
        .await;
        return;
    };
    let Ok(group_id) = u32::try_from(process.process_group_id) else {
        finish_lost(
            database,
            &recovered.claim,
            "persisted process group ID is invalid",
        )
        .await;
        return;
    };
    let state = match process_identity(pid) {
        Ok(identity)
            if identity.process_group_id == process.process_group_id
                && identity.start_ticks == process.process_start_ticks =>
        {
            RecoveredProcessState {
                leader_present: true,
                completion: None,
                timeout_remaining: recovered
                    .timeout_remaining
                    .map(|remaining| remaining.saturating_sub(validation_started.elapsed())),
            }
        }
        Ok(_) => RecoveredProcessState {
            leader_present: false,
            completion: Some((
                AttemptState::Lost,
                "persisted PID belongs to a different process",
            )),
            timeout_remaining: None,
        },
        Err(_) => RecoveredProcessState {
            leader_present: false,
            completion: Some((AttemptState::Lost, "persisted process no longer exists")),
            timeout_remaining: None,
        },
    };
    supervise_recovered(
        database,
        &recovered.claim,
        &process,
        pid,
        group_id,
        state,
        shutdown,
    )
    .await;
}

// Recovery deliberately never creates a container: the persisted identity is the
// only safe authority once Docker create has been committed.
async fn supervise_recovered_docker(
    database: &Database,
    paths: &RuntimePaths,
    recovered: RecoveredExecution,
    shutdown: &mut watch::Receiver<bool>,
) {
    let timeout_remaining = recovered.timeout_remaining;
    let claim = recovered.claim;
    let planner = DockerCommandPlanner::default();
    let container = match recovered.container {
        Some(container) => container,
        None => match recover_container_identity(database, paths, &planner, &claim, shutdown).await
        {
            Some(container) => container,
            None => return,
        },
    };
    let id = container.container_id.clone();
    let inspect = docker_command(
        &planner.recovery_inspect(&id),
        database,
        &claim,
        shutdown,
        Stdio::piped(),
        Stdio::piped(),
    )
    .await;
    let output = match inspect {
        Ok(output) if output.status.success() => output,
        Ok(output) => {
            let detail = String::from_utf8_lossy(&output.stderr);
            let missing = detail.contains("No such container") || detail.contains("No such object");
            if missing {
                finish_docker_lost(
                    database,
                    &claim,
                    &container,
                    "persisted Docker container is missing",
                )
                .await;
            }
            return;
        }
        Err(_) => return,
    };
    let recovered_identity = match igor_core::parse_docker_recovery_inspect(&output.stdout) {
        Ok(identity)
            if identity.id == id
                && recovery_identity_matches(&identity, &claim, &container.image_id) =>
        {
            identity
        }
        Ok(_) => {
            tracing::error!(container_id = %id, "persisted Docker identity no longer matches its container");
            return;
        }
        Err(error) => {
            tracing::error!(%error, container_id = %id, "cannot parse persisted Docker identity");
            return;
        }
    };
    let state = recovered_identity.state;
    if state.status == "created" && !state.running {
        match database.jobs().cancellation_grace(&claim).await {
            Ok(Some(_)) => {
                finish_created_container_cancelled(database, &planner, &claim, &container).await;
                return;
            }
            Ok(None) => {}
            Err(error) => {
                tracing::error!(%error, container_id = %id, "cannot read recovered Docker cancellation");
                return;
            }
        }
        if container.state != DockerContainerState::Created {
            tracing::error!(container_id = %id, "persisted running container is only created in Docker");
            return;
        }
        let start = docker_command(
            &planner.start(&id),
            database,
            &claim,
            shutdown,
            Stdio::piped(),
            Stdio::piped(),
        )
        .await;
        if !matches!(start, Ok(ref output) if output.status.success()) {
            tracing::warn!(container_id = %id, "recovered Docker start was not confirmed; preserving ownership for reconciliation");
            return;
        }
        if let Err(error) = database.jobs().record_container_started(&claim).await {
            tracing::error!(%error, container_id = %id, "cannot persist recovered Docker start");
            return;
        }
    } else if state.running && container.state == DockerContainerState::Created {
        if let Err(error) = database.jobs().record_container_started(&claim).await {
            tracing::error!(%error, container_id = %id, "cannot persist recovered running container");
            return;
        }
    } else if !state.running {
        if capture_docker_snapshot(database, &planner, &claim, &container, shutdown)
            .await
            .is_err()
        {
            return;
        }
        finish_docker_inspection(database, &planner, &claim, &container, state, None, None).await;
        return;
    }
    let (stdout, stderr) = match reset_recovery_logs(&container) {
        Ok(logs) => logs,
        Err(error) => {
            tracing::error!(%error, container_id = %id, "cannot open recovered Docker logs");
            return;
        }
    };
    let supervision = docker_logs_and_wait(
        &planner.logs(&id),
        &planner.wait(&id),
        database,
        &claim,
        shutdown,
        DockerLogFiles { stdout, stderr },
        timeout_remaining,
    )
    .await;
    match supervision {
        DockerSupervision::Completed { logs, wait } => {
            finalize_supervised_docker(
                database, &planner, &claim, &container, &logs, &wait, shutdown,
            )
            .await;
        }
        DockerSupervision::Cancellation(grace) => {
            cancel_docker(database, &planner, &claim, &container, grace, shutdown).await;
        }
        DockerSupervision::Timeout => {
            timeout_docker(database, &planner, &claim, &container, shutdown).await;
        }
        DockerSupervision::Shutdown | DockerSupervision::OwnershipLost(_) => {}
    }
}

async fn recover_container_identity(
    database: &Database,
    paths: &RuntimePaths,
    planner: &DockerCommandPlanner,
    claim: &ExecutionClaim,
    shutdown: &mut watch::Receiver<bool>,
) -> Option<ContainerRecord> {
    let identity = docker_identity(claim);
    let candidates = match docker_command(
        &planner.recovery_candidates(identity),
        database,
        claim,
        shutdown,
        Stdio::piped(),
        Stdio::piped(),
    )
    .await
    {
        Ok(output) if output.status.success() => {
            match igor_core::parse_docker_recovery_candidates(&output.stdout) {
                Ok(candidates) => candidates,
                Err(error) => {
                    tracing::error!(%error, "cannot parse Docker recovery candidates");
                    return None;
                }
            }
        }
        Ok(output) => {
            tracing::error!(error = %String::from_utf8_lossy(&output.stderr), "cannot list Docker recovery candidates");
            return None;
        }
        Err(_) => return None,
    };
    if candidates.is_empty() {
        finish_lost(
            database,
            claim,
            "no Docker container matches the recovered attempt",
        )
        .await;
        return None;
    }

    let spec = match claim.attempt.spec.executor() {
        ExecutorSpec::Docker(spec) => spec,
        _ => return None,
    };
    let expected_image = match docker_command(
        &planner.image_inspect(&spec.image_reference()),
        database,
        claim,
        shutdown,
        Stdio::piped(),
        Stdio::piped(),
    )
    .await
    {
        Ok(output) if output.status.success() => {
            match igor_core::parse_docker_create_stdout(&output.stdout) {
                Ok(image) => image,
                Err(error) => {
                    tracing::error!(%error, "cannot parse recovered Docker image identity");
                    return None;
                }
            }
        }
        _ => return None,
    };
    let mut matches = Vec::new();
    for candidate in candidates {
        let output = match docker_command(
            &planner.recovery_inspect(&candidate),
            database,
            claim,
            shutdown,
            Stdio::piped(),
            Stdio::piped(),
        )
        .await
        {
            Ok(output) if output.status.success() => output,
            Ok(output) if docker_missing(&output.stderr) => continue,
            _ => return None,
        };
        let recovered = match igor_core::parse_docker_recovery_inspect(&output.stdout) {
            Ok(recovered) => recovered,
            Err(error) => {
                tracing::error!(%error, candidate, "cannot parse Docker recovery identity");
                return None;
            }
        };
        if recovery_identity_matches(&recovered, claim, &expected_image) {
            matches.push(recovered);
        }
    }
    if matches.len() != 1 {
        tracing::error!(
            count = matches.len(),
            "Docker recovery identity is ambiguous"
        );
        return None;
    }
    let recovered = matches.pop()?;
    let (stdout_path, stderr_path) =
        match prepare_logs(&paths.log_dir, &claim.attempt.spec.id().to_string()) {
            Ok(paths) => paths,
            Err(error) => {
                tracing::error!(%error, "cannot prepare recovered Docker logs");
                return None;
            }
        };
    let create = ContainerCreate {
        container_id: recovered.id,
        container_name: format!("igor-{}", claim.attempt.spec.id()),
        image: igor_core::DockerImageIdentity {
            reference: spec.image_reference(),
            image_id: recovered.image_id,
        },
        stdout_path,
        stderr_path,
    };
    if let Err(error) = database
        .jobs()
        .record_container_created(claim, &create)
        .await
    {
        tracing::error!(%error, "cannot persist recovered Docker identity");
        return None;
    }
    match database
        .jobs()
        .container_for_attempt(claim.attempt.spec.id())
        .await
    {
        Ok(container) => container,
        Err(error) => {
            tracing::error!(%error, "cannot reload recovered Docker identity");
            None
        }
    }
}

fn docker_identity(claim: &ExecutionClaim) -> DockerIdentity {
    DockerIdentity {
        project_id: claim.job.spec.project_id,
        job_id: claim.job.spec.id,
        attempt_id: claim.attempt.spec.id(),
        generation_id: claim
            .attempt
            .spec
            .family()
            .map(|family| family.generation.id),
    }
}

fn recovery_identity_matches(
    recovered: &DockerRecoveryIdentity,
    claim: &ExecutionClaim,
    expected_image: &str,
) -> bool {
    let identity = docker_identity(claim);
    let expected_name = format!("igor-{}", identity.attempt_id);
    let expected_labels = [
        ("igor.project_id", identity.project_id.to_string()),
        ("igor.job_id", identity.job_id.to_string()),
        ("igor.attempt_id", identity.attempt_id.to_string()),
    ];
    recovered.name.trim_start_matches('/') == expected_name
        && recovered.image_id == expected_image
        && expected_labels
            .iter()
            .all(|(key, value)| recovered.labels.get(*key) == Some(value))
        && match identity.generation_id {
            Some(generation) => {
                recovered.labels.get("igor.generation_id") == Some(&generation.to_string())
            }
            None => !recovered.labels.contains_key("igor.generation_id"),
        }
        && recovered.labels.keys().all(|key| {
            !key.starts_with("igor.")
                || matches!(
                    key.as_str(),
                    "igor.project_id" | "igor.job_id" | "igor.attempt_id" | "igor.generation_id"
                )
        })
}

async fn inspect_docker(
    database: &Database,
    planner: &DockerCommandPlanner,
    claim: &ExecutionClaim,
    container_id: &str,
    shutdown: &mut watch::Receiver<bool>,
) -> Result<DockerContainerInspection, DockerInspectFailure> {
    match docker_command(
        &planner.inspect(container_id),
        database,
        claim,
        shutdown,
        Stdio::piped(),
        Stdio::piped(),
    )
    .await
    {
        Ok(output) if output.status.success() => {
            igor_core::parse_docker_inspect_stdout(&output.stdout)
                .map_err(|error| DockerInspectFailure::Indeterminate(error.to_string()))
        }
        Ok(output) if docker_missing(&output.stderr) => Err(DockerInspectFailure::Missing),
        Ok(output) => Err(DockerInspectFailure::Indeterminate(
            String::from_utf8_lossy(&output.stderr).trim().to_owned(),
        )),
        Err(error) => Err(DockerInspectFailure::Indeterminate(error)),
    }
}

fn docker_missing(stderr: &[u8]) -> bool {
    let detail = String::from_utf8_lossy(stderr);
    detail.contains("No such container") || detail.contains("No such object")
}

async fn finish_docker_lost(
    database: &Database,
    claim: &ExecutionClaim,
    container: &ContainerRecord,
    reason: &str,
) {
    let error = format!("docker_container_lost: {reason}");
    let finish = ContainerFinish {
        state: DockerContainerState::Lost,
        docker_status: Some("missing".into()),
        exit_code: None,
        oom_killed: false,
        error: Some(error.clone()),
    };
    let outcome = ExecutionOutcome {
        state: AttemptState::Lost,
        exit_code: None,
        term_signal: None,
        error: Some(error),
    };
    if let Err(error) = database
        .jobs()
        .finish_container_execution(claim, &finish, &outcome)
        .await
    {
        tracing::error!(%error, container_id = %container.container_id, "cannot persist lost Docker container");
    }
}

async fn capture_docker_snapshot(
    database: &Database,
    planner: &DockerCommandPlanner,
    claim: &ExecutionClaim,
    container: &ContainerRecord,
    shutdown: &mut watch::Receiver<bool>,
) -> Result<(), String> {
    let (stdout, stderr) = reset_recovery_logs(container).map_err(|error| error.to_string())?;
    let output = docker_command(
        &planner.logs_snapshot(&container.container_id),
        database,
        claim,
        shutdown,
        Stdio::from(stdout),
        Stdio::from(stderr),
    )
    .await?;
    if !output.status.success() {
        return Err(format!("docker_logs_failed: {}", output.status));
    }
    sync_docker_logs(container).map_err(|error| error.to_string())
}

async fn finish_created_container_cancelled(
    database: &Database,
    planner: &DockerCommandPlanner,
    claim: &ExecutionClaim,
    container: &ContainerRecord,
) {
    let error = "docker_cancelled: cancellation requested".to_owned();
    let finish = ContainerFinish {
        state: DockerContainerState::Exited,
        docker_status: Some("created".into()),
        exit_code: None,
        oom_killed: false,
        error: Some(error.clone()),
    };
    let outcome = ExecutionOutcome {
        state: AttemptState::Cancelled,
        exit_code: None,
        term_signal: None,
        error: Some(error),
    };
    if database
        .jobs()
        .finish_container_execution(claim, &finish, &outcome)
        .await
        .is_ok()
        && matches!(claim.attempt.spec.executor(), ExecutorSpec::Docker(spec) if spec.remove_container)
    {
        docker_cleanup(planner, database, claim, &container.container_id).await;
    }
}

async fn supervise_recovered(
    database: &Database,
    claim: &ExecutionClaim,
    process: &ProcessRecord,
    pid: u32,
    group_id: u32,
    state: RecoveredProcessState,
    shutdown: &mut watch::Receiver<bool>,
) {
    let RecoveredProcessState {
        mut leader_present,
        mut completion,
        timeout_remaining,
    } = state;
    let mut heartbeat = time::interval(HEARTBEAT_INTERVAL);
    heartbeat.set_missed_tick_behavior(time::MissedTickBehavior::Delay);
    heartbeat.tick().await;
    let mut poll = time::interval(CANCELLATION_INTERVAL);
    poll.set_missed_tick_behavior(time::MissedTickBehavior::Delay);
    let mut cancel_deadline = None;
    let mut timeout_deadline = timeout_remaining.map(|remaining| time::Instant::now() + remaining);

    loop {
        tokio::select! {
            _ = poll.tick() => {
                if leader_present {
                    match process_identity(pid) {
                        Ok(identity)
                            if identity.process_group_id == process.process_group_id
                                && identity.start_ticks == process.process_start_ticks => {}
                        Ok(_) => {
                            leader_present = false;
                            completion = Some((AttemptState::Lost, "PID identity changed during recovery"));
                            cancel_deadline = None;
                            timeout_deadline = None;
                        }
                        Err(_) if completion.is_none() => {
                            leader_present = false;
                            completion = Some((AttemptState::Lost, "recovered process leader exited without an observable status"));
                            cancel_deadline = None;
                            timeout_deadline = None;
                        }
                        Err(_) => leader_present = false,
                    }
                }
                if !process_group_exists(group_id) {
                    let (state, reason) = completion.unwrap_or((
                        AttemptState::Lost,
                        "recovered process exited without an observable status",
                    ));
                    finish_recovered(database, claim, state, reason).await;
                    return;
                }
                if completion.is_none()
                    || matches!(completion, Some((AttemptState::Cancelled, _)))
                {
                    match database.jobs().cancellation_grace(claim).await {
                        Ok(Some(remaining)) => {
                            if cancel_deadline.is_none() {
                                signal_group(group_id, Signal::SIGTERM);
                            }
                            let deadline = time::Instant::now() + remaining;
                            cancel_deadline = Some(cancel_deadline.map_or(deadline, |current| current.min(deadline)));
                            completion = Some((AttemptState::Cancelled, "execution cancelled after worker recovery"));
                        }
                        Ok(None) => {}
                        Err(error) => {
                            tracing::error!(%error, job_id = %claim.job.spec.id, "cannot read recovered cancellation request");
                            terminate_group(group_id);
                            completion = Some((AttemptState::Failed, "cannot read cancellation request after recovery"));
                        }
                    }
                }
            }
            _ = heartbeat.tick() => {
                if let Err(error) = database.jobs().heartbeat_execution(claim, CLAIM_DURATION).await {
                    tracing::error!(%error, job_id = %claim.job.spec.id, "recovered execution heartbeat failed");
                    terminate_group(group_id);
                    completion = Some((AttemptState::Failed, "execution heartbeat failed after recovery"));
                }
            }
            _ = wait_for_deadline(cancel_deadline), if cancel_deadline.is_some() => {
                terminate_group(group_id);
                cancel_deadline = None;
            }
            _ = wait_for_deadline(timeout_deadline), if timeout_deadline.is_some() => {
                terminate_group(group_id);
                timeout_deadline = None;
                cancel_deadline = None;
                completion = Some((AttemptState::Failed, "configured execution timeout elapsed after recovery"));
            }
            _ = shutdown.changed() => {
                release_for_restart(database, claim).await;
                return;
            },
        }
    }
}

async fn wait_for_deadline(deadline: Option<time::Instant>) {
    match deadline {
        Some(deadline) => time::sleep_until(deadline).await,
        None => std::future::pending().await,
    }
}

async fn finish_lost(database: &Database, claim: &ExecutionClaim, reason: &str) {
    finish_recovered(database, claim, AttemptState::Lost, reason).await;
}

async fn release_for_restart(database: &Database, claim: &ExecutionClaim) {
    if let Err(error) = database.jobs().release_execution(claim).await {
        tracing::error!(%error, job_id = %claim.job.spec.id, "cannot release execution for restart");
    }
}

async fn finish_recovered(
    database: &Database,
    claim: &ExecutionClaim,
    state: AttemptState,
    reason: &str,
) {
    let outcome = ExecutionOutcome {
        state,
        exit_code: None,
        term_signal: None,
        error: Some(reason.into()),
    };
    if let Err(error) = database.jobs().finish_execution(claim, &outcome).await {
        tracing::error!(%error, job_id = %claim.job.spec.id, "cannot persist recovered execution outcome");
    }
}

async fn execute(
    database: &Database,
    paths: &RuntimePaths,
    claim: &ExecutionClaim,
    shutdown: &mut watch::Receiver<bool>,
) {
    match claim.attempt.spec.executor() {
        ExecutorSpec::Process(spec) if spec.isolation == ProcessIsolation::SystemdUserUnit => {
            execute_unit(database, paths, claim, shutdown).await;
        }
        ExecutorSpec::Process(_) => execute_process(database, paths, claim, shutdown).await,
        ExecutorSpec::Docker(_) => execute_docker(database, paths, claim, shutdown).await,
    }
}

// The reservation is the launch boundary: once it exists, neither a failed CLI
// response nor a worker restart is permission to launch this attempt again.
async fn execute_unit(
    database: &Database,
    paths: &RuntimePaths,
    claim: &ExecutionClaim,
    shutdown: &mut watch::Receiver<bool>,
) {
    let command = claim.attempt.spec.command();
    let (stdout_path, stderr_path) =
        match prepare_logs(&paths.log_dir, &claim.attempt.spec.id().to_string()) {
            Ok(paths) => paths,
            Err(error) => {
                finish_launch_failure(
                    database,
                    claim,
                    &format!("cannot prepare unit logs: {error}"),
                )
                .await;
                return;
            }
        };
    for path in [&stdout_path, &stderr_path] {
        if let Err(error) = open_log(path) {
            finish_launch_failure(database, claim, &format!("cannot create unit log: {error}"))
                .await;
            return;
        }
    }
    let mut environment_command = Command::new(&command.program);
    if let Err(error) = configure_execution_environment(
        &mut environment_command,
        &command.environment,
        &claim.assigned_gpus,
    ) {
        finish_launch_failure(database, claim, &error.to_string()).await;
        return;
    }
    let mut environment = BTreeMap::new();
    for (key, value) in environment_command.as_std().get_envs() {
        if let Some(value) = value {
            let (Some(key), Some(value)) = (key.to_str(), value.to_str()) else {
                finish_launch_failure(
                    database,
                    claim,
                    "unit environment contains non-UTF-8 values",
                )
                .await;
                return;
            };
            environment.insert(key.to_owned(), value.to_owned());
        }
    }
    let unit_name = match systemd::unit_name(&claim.attempt.spec.id().to_string()) {
        Ok(name) => name,
        Err(error) => {
            finish_launch_failure(database, claim, &error.to_string()).await;
            return;
        }
    };
    let planned = match systemd::run(
        &unit_name,
        &command.cwd,
        &stdout_path,
        &stderr_path,
        &environment,
        &command.program,
        &command.args,
    ) {
        Ok(planned) => planned,
        Err(error) => {
            finish_launch_failure(database, claim, &error.to_string()).await;
            return;
        }
    };
    let unit = match database
        .jobs()
        .reserve_unit(
            claim,
            &UnitReservation {
                stdout_path,
                stderr_path,
            },
        )
        .await
    {
        Ok(unit) => unit,
        Err(error) => {
            tracing::error!(%error, "cannot reserve unit before launch");
            return;
        }
    };
    // A cancellation arriving before the external call must not launch a job.
    match database.jobs().cancellation_grace(claim).await {
        Ok(Some(_)) => {
            finish_unit(
                database,
                claim,
                &unit,
                &unit_outcome(
                    AttemptState::Cancelled,
                    None,
                    None,
                    Some("execution cancelled before launch".into()),
                ),
                None,
            )
            .await;
            return;
        }
        Ok(None) => {}
        Err(error) => {
            tracing::error!(%error, "cannot inspect pre-launch cancellation");
            return;
        }
    }
    let launch = unit_command(&planned, database, Some(claim), shutdown).await;
    if let Ok(ref output) = launch
        && output.status.success()
        && let Some(invocation) = systemd::parse_launch(&output.stdout, &unit.unit_name)
    {
        if let Err(error) = database
            .jobs()
            .record_unit_started(claim, &invocation)
            .await
        {
            tracing::error!(%error, "cannot persist launched unit invocation");
            return;
        }
        supervise_unit(
            database,
            claim,
            &unit,
            Some(invocation),
            claim
                .attempt
                .spec
                .resources()
                .timeout_seconds
                .map(Duration::from_secs),
            shutdown,
        )
        .await;
        return;
    }
    // A systemd-run error can occur after creation: inspect the reserved name
    // instead of reporting a launch failure or starting another unit.
    tracing::warn!(unit = %unit.unit_name, "unit launch not confirmed; reconciling reserved identity");
    supervise_unit(
        database,
        claim,
        &unit,
        None,
        claim
            .attempt
            .spec
            .resources()
            .timeout_seconds
            .map(Duration::from_secs),
        shutdown,
    )
    .await;
}

async fn supervise_recovered_unit(
    database: &Database,
    recovered: RecoveredExecution,
    shutdown: &mut watch::Receiver<bool>,
) {
    let claim = recovered.claim;
    let Some(unit) = recovered.unit else {
        finish_lost(
            database,
            &claim,
            "worker restarted before unit identity was reserved",
        )
        .await;
        return;
    };
    if systemd::unit_name(&claim.attempt.spec.id().to_string()).as_deref() != Ok(&unit.unit_name) {
        tracing::error!(unit = %unit.unit_name, "recovered unit name does not match attempt");
        return;
    }
    supervise_unit(
        database,
        &claim,
        &unit,
        unit.invocation_id.clone(),
        recovered.timeout_remaining.or_else(|| {
            claim
                .attempt
                .spec
                .resources()
                .timeout_seconds
                .map(Duration::from_secs)
        }),
        shutdown,
    )
    .await;
}

async fn unit_command(
    planned: &PlannedCommand,
    database: &Database,
    claim: Option<&ExecutionClaim>,
    shutdown: &mut watch::Receiver<bool>,
) -> Result<std::process::Output, String> {
    let mut command = Command::new(planned.program);
    command
        .args(&planned.args)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    command.kill_on_drop(true);
    let child = command.spawn().map_err(|error| error.to_string())?;
    let output = child.wait_with_output();
    tokio::pin!(output);
    let mut heartbeat = time::interval(HEARTBEAT_INTERVAL);
    heartbeat.set_missed_tick_behavior(time::MissedTickBehavior::Delay);
    let deadline = time::sleep(CLAIM_DURATION);
    tokio::pin!(deadline);
    loop {
        tokio::select! {
            result = &mut output => return result.map_err(|error| error.to_string()),
            () = &mut deadline => return Err("systemd command timed out".into()),
            _ = shutdown.changed() => {
                if let Some(claim) = claim { release_for_restart(database, claim).await; }
                return Err("worker shutdown".into());
            }
            _ = heartbeat.tick(), if claim.is_some() => {
                if let Some(claim) = claim {
                    database.jobs().heartbeat_execution(claim, CLAIM_DURATION).await
                        .map_err(|error| format!("unit heartbeat failed: {error}"))?;
                }
            }
        }
    }
}

async fn inspect_unit(
    database: &Database,
    claim: &ExecutionClaim,
    unit: &UnitRecord,
    invocation: Option<&str>,
    shutdown: &mut watch::Receiver<bool>,
) -> Option<systemd::UnitStatus> {
    let planned = systemd::show(&unit.unit_name).ok()?;
    let output = match unit_command(&planned, database, Some(claim), shutdown).await {
        Ok(output) if output.status.success() => output,
        Ok(output) => {
            tracing::warn!(unit = %unit.unit_name, error = %String::from_utf8_lossy(&output.stderr), "cannot inspect unit");
            return None;
        }
        Err(error) => {
            tracing::warn!(%error, unit = %unit.unit_name, "cannot inspect unit");
            return None;
        }
    };
    Some(match invocation {
        Some(invocation) => systemd::parse_status(&output.stdout, invocation),
        None => systemd::parse_unclaimed_status(&output.stdout),
    })
}

async fn finish_unit(
    database: &Database,
    claim: &ExecutionClaim,
    unit: &UnitRecord,
    outcome: &ExecutionOutcome,
    invocation: Option<&str>,
) {
    if let Err(error) = database
        .jobs()
        .finish_unit_execution(claim, outcome, invocation)
        .await
    {
        tracing::error!(%error, unit = %unit.unit_name, "cannot persist unit result");
    }
}

fn unit_outcome(
    state: AttemptState,
    exit_code: Option<i32>,
    term_signal: Option<i32>,
    error: Option<String>,
) -> ExecutionOutcome {
    ExecutionOutcome {
        state,
        exit_code,
        term_signal,
        error,
    }
}

async fn supervise_unit(
    database: &Database,
    claim: &ExecutionClaim,
    unit: &UnitRecord,
    mut invocation: Option<String>,
    timeout_remaining: Option<Duration>,
    shutdown: &mut watch::Receiver<bool>,
) {
    let mut poll = time::interval(CANCELLATION_INTERVAL);
    poll.set_missed_tick_behavior(time::MissedTickBehavior::Delay);
    let mut heartbeat = time::interval(HEARTBEAT_INTERVAL);
    heartbeat.set_missed_tick_behavior(time::MissedTickBehavior::Delay);
    let timeout = timeout_remaining.map(|remaining| time::Instant::now() + remaining);
    let mut interruption: Option<(AttemptState, String)> = None;
    let mut stop_deadline = None;
    let mut missing_since = None;
    loop {
        tokio::select! {
            _ = poll.tick() => {
                let Some(status) = inspect_unit(database, claim, unit, invocation.as_deref(), shutdown).await else { continue };
                if invocation.is_none() && let Some(id) = status.invocation_id.clone() {
                    if let Err(error) = database.jobs().record_unit_started(claim, &id).await {
                        tracing::error!(%error, "cannot persist recovered unit invocation");
                        return;
                    }
                    invocation = Some(id);
                }
                match status.outcome {
                    UnitOutcome::Exited { status, .. } => {
                        if let Some((state, reason)) = interruption {
                            finish_unit(database, claim, unit, &unit_outcome(state, Some(status), None, Some(reason)), invocation.as_deref()).await;
                        } else {
                            let state = if status == 0 { AttemptState::Succeeded } else { AttemptState::Failed };
                            finish_unit(database, claim, unit, &unit_outcome(state, Some(status), None, None), invocation.as_deref()).await;
                        }
                        return;
                    }
                    UnitOutcome::Signaled { signal, .. } => {
                        let (state, error) = interruption.map_or((AttemptState::Failed, None), |(state, reason)| (state, Some(reason)));
                        finish_unit(database, claim, unit, &unit_outcome(state, None, Some(signal), error), invocation.as_deref()).await;
                        return;
                    }
                    UnitOutcome::Missing => {
                        if !missing_since.is_some_and(|since: time::Instant| since.elapsed() >= Duration::from_secs(1)) {
                            missing_since.get_or_insert_with(time::Instant::now);
                            continue;
                        }
                        let (state, reason) = interruption.unwrap_or((AttemptState::Lost, "reserved systemd unit is missing".into()));
                        finish_unit(database, claim, unit, &unit_outcome(state, None, None, Some(reason)), invocation.as_deref()).await;
                        return;
                    }
                    UnitOutcome::Running { .. } => {}
                    UnitOutcome::Indeterminate if interruption.is_none() || status.invocation_id.as_deref() != invocation.as_deref() || invocation.is_none() => continue,
                    UnitOutcome::Indeterminate => {}
                }
                missing_since = None;
                if interruption.is_some() && stop_deadline.is_some_and(|deadline| time::Instant::now() >= deadline) {
                    if let Ok(kill) = systemd::signal(&unit.unit_name, "SIGKILL") {
                        let _ = unit_command(&kill, database, Some(claim), shutdown).await;
                    }
                    stop_deadline = None;
                }
                if matches!(interruption, Some((AttemptState::Cancelled, _))) && stop_deadline.is_some()
                    && let Ok(Some(remaining)) = database.jobs().cancellation_grace(claim).await {
                    let deadline = time::Instant::now() + remaining;
                    stop_deadline = stop_deadline.map(|current| current.min(deadline));
                }
                if interruption.is_some() && stop_deadline.is_none() {
                    if let Ok(stop) = systemd::stop(&unit.unit_name) {
                        let _ = unit_command(&stop, database, Some(claim), shutdown).await;
                    }
                    continue;
                }
                if interruption.is_none() {
                    let cancel = database.jobs().cancellation_grace(claim).await;
                    let action = match cancel {
                        Ok(Some(grace)) => Some((AttemptState::Cancelled, "execution cancelled".to_owned(), grace)),
                        Ok(None) if timeout.is_some_and(|deadline| time::Instant::now() >= deadline) =>
                            Some((AttemptState::Failed, "configured execution timeout elapsed".to_owned(), Duration::ZERO)),
                        Ok(None) => None,
                        Err(error) => { tracing::error!(%error, "cannot read unit cancellation"); None }
                    };
                    if let Some((state, reason, grace)) = action
                        && let Ok(planned) = systemd::signal(&unit.unit_name, "SIGTERM") {
                            match unit_command(&planned, database, Some(claim), shutdown).await {
                                Ok(output) if output.status.success() => {
                                    interruption = Some((state, reason));
                                    stop_deadline = Some(time::Instant::now() + grace);
                                }
                                other => tracing::warn!(unit = %unit.unit_name, ?other, "unit signal was not confirmed"),
                            }
                    }
                }
            }
            _ = heartbeat.tick() => {
                if let Err(error) = database.jobs().heartbeat_execution(claim, CLAIM_DURATION).await {
                    tracing::error!(%error, unit = %unit.unit_name, "unit heartbeat failed");
                    return;
                }
            }
            _ = shutdown.changed() => {
                release_for_restart(database, claim).await;
                return;
            }
        }
    }
}

async fn execute_process(
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
    if let Err(error) = configure_execution_environment(
        &mut child_command,
        &command.environment,
        &claim.assigned_gpus,
    ) {
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
                                    release_for_restart(database, claim).await;
                                    return;
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
                release_for_restart(database, claim).await;
                return;
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

async fn docker_command(
    command: &DockerCommand,
    database: &Database,
    claim: &ExecutionClaim,
    shutdown: &mut watch::Receiver<bool>,
    stdout: Stdio,
    stderr: Stdio,
) -> Result<std::process::Output, String> {
    let mut child = Command::new(command.program());
    child
        .args(command.args())
        .stdin(Stdio::null())
        .stdout(stdout)
        .stderr(stderr);
    child.kill_on_drop(true);
    let child = child.spawn().map_err(|e| e.to_string())?;
    let output = child.wait_with_output();
    tokio::pin!(output);
    tokio::select! {
        result = &mut output => result.map_err(|e| e.to_string()),
        _ = shutdown.changed() => {
            release_for_restart(database, claim).await;
            Err("worker shutdown".into())
        }
        result = async {
            let mut interval = time::interval(HEARTBEAT_INTERVAL);
            loop {
                interval.tick().await;
                if let Err(error) = database.jobs().heartbeat_execution(claim, CLAIM_DURATION).await {
                    break error.to_string();
                }
            }
        } => Err(format!("docker_heartbeat_failed: {result}")),
    }
}

async fn docker_stop_with_deadline(
    command: &DockerCommand,
    database: &Database,
    claim: &ExecutionClaim,
    shutdown: &mut watch::Receiver<bool>,
    grace: Duration,
) -> Result<std::process::Output, String> {
    let mut child = Command::new(command.program());
    child
        .args(command.args())
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    child.kill_on_drop(true);
    let child = child.spawn().map_err(|error| error.to_string())?;
    let output = child.wait_with_output();
    tokio::pin!(output);
    let deadline = time::sleep(grace);
    tokio::pin!(deadline);
    let mut cancellation = time::interval(CANCELLATION_INTERVAL);
    cancellation.set_missed_tick_behavior(time::MissedTickBehavior::Delay);
    let mut heartbeat = time::interval(HEARTBEAT_INTERVAL);
    heartbeat.set_missed_tick_behavior(time::MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            result = &mut output => return result.map_err(|error| error.to_string()),
            _ = cancellation.tick() => {
                match database.jobs().cancellation_grace(claim).await {
                    Ok(Some(remaining)) => {
                        let shortened = time::Instant::now() + remaining;
                        if shortened < deadline.deadline() {
                            deadline.as_mut().reset(shortened);
                        }
                    }
                    Ok(None) => {}
                    Err(error) => return Err(error.to_string()),
                }
            }
            _ = heartbeat.tick() => {
                database
                    .jobs()
                    .heartbeat_execution(claim, CLAIM_DURATION)
                    .await
                    .map_err(|error| error.to_string())?;
            }
            () = &mut deadline => return Err("Docker stop grace period elapsed".into()),
            _ = shutdown.changed() => {
                release_for_restart(database, claim).await;
                return Err("worker shutdown".into());
            }
        }
    }
}

async fn docker_logs_and_wait(
    logs_command: &DockerCommand,
    wait_command: &DockerCommand,
    database: &Database,
    claim: &ExecutionClaim,
    shutdown: &mut watch::Receiver<bool>,
    log_files: DockerLogFiles,
    timeout_remaining: Option<Duration>,
) -> DockerSupervision {
    let mut logs = Command::new(logs_command.program());
    logs.args(logs_command.args())
        .stdin(Stdio::null())
        .stdout(Stdio::from(log_files.stdout))
        .stderr(Stdio::from(log_files.stderr));
    logs.kill_on_drop(true);
    let logs = match logs.spawn() {
        Ok(logs) => logs,
        Err(error) => return DockerSupervision::OwnershipLost(error.to_string()),
    };

    let mut wait = Command::new(wait_command.program());
    wait.args(wait_command.args())
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    wait.kill_on_drop(true);
    let wait = match wait.spawn() {
        Ok(wait) => wait,
        Err(error) => return DockerSupervision::OwnershipLost(error.to_string()),
    };

    let logs = logs.wait_with_output();
    let wait = wait.wait_with_output();
    tokio::pin!(logs);
    tokio::pin!(wait);
    let mut logs_output = None;
    let mut wait_output = None;
    let mut heartbeat = time::interval(HEARTBEAT_INTERVAL);
    heartbeat.set_missed_tick_behavior(time::MissedTickBehavior::Delay);
    let mut cancellation = time::interval(CANCELLATION_INTERVAL);
    cancellation.set_missed_tick_behavior(time::MissedTickBehavior::Delay);
    let timeout = async {
        match timeout_remaining {
            Some(remaining) => time::sleep(remaining).await,
            None => std::future::pending().await,
        }
    };
    tokio::pin!(timeout);
    loop {
        tokio::select! {
            result = &mut logs, if logs_output.is_none() => {
                match result {
                    Ok(output) => logs_output = Some(output),
                    Err(error) => return DockerSupervision::OwnershipLost(error.to_string()),
                }
            }
            result = &mut wait, if wait_output.is_none() => {
                match result {
                    Ok(output) => wait_output = Some(output),
                    Err(error) => return DockerSupervision::OwnershipLost(error.to_string()),
                }
            }
            _ = cancellation.tick() => {
                match database.jobs().cancellation_grace(claim).await {
                    Ok(Some(grace)) => return DockerSupervision::Cancellation(grace),
                    Ok(None) => {}
                    Err(error) => return DockerSupervision::OwnershipLost(error.to_string()),
                }
            }
            _ = heartbeat.tick() => {
                if let Err(error) = database.jobs().heartbeat_execution(claim, CLAIM_DURATION).await {
                    return DockerSupervision::OwnershipLost(format!("docker_heartbeat_failed: {error}"));
                }
            }
            () = &mut timeout => return DockerSupervision::Timeout,
            _ = shutdown.changed() => {
                release_for_restart(database, claim).await;
                return DockerSupervision::Shutdown;
            }
        }
        if logs_output.is_some() && wait_output.is_some() {
            let (Some(logs), Some(wait)) = (logs_output.take(), wait_output.take()) else {
                unreachable!("Docker log and wait outputs were checked")
            };
            return DockerSupervision::Completed { logs, wait };
        }
    }
}

async fn docker_cleanup(
    planner: &DockerCommandPlanner,
    database: &Database,
    claim: &ExecutionClaim,
    container_id: &str,
) {
    let command = planner.remove(container_id);
    let mut process = Command::new(command.program());
    process
        .args(command.args())
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::piped());
    process.kill_on_drop(true);
    let output = match time::timeout(CLAIM_DURATION, process.output()).await {
        Ok(output) => output,
        Err(_) => {
            tracing::error!(%container_id, "Docker container removal timed out");
            return;
        }
    };
    let removed = match output {
        Ok(output) if output.status.success() => true,
        Ok(output) => String::from_utf8_lossy(&output.stderr).contains("No such container"),
        Err(error) => {
            tracing::error!(%error, %container_id, "cannot remove terminal Docker container");
            false
        }
    };
    if removed
        && let Err(error) = database
            .jobs()
            .record_container_removed(claim.attempt.spec.id(), container_id)
            .await
    {
        tracing::error!(%error, %container_id, "cannot persist Docker container removal");
    }
}

async fn cleanup_pending(database: &Database) {
    cleanup_pending_units(database).await;
    let pending = match database.jobs().containers_pending_cleanup().await {
        Ok(pending) => pending,
        Err(error) => {
            tracing::error!(%error, "cannot list Docker containers pending cleanup");
            return;
        }
    };
    let planner = DockerCommandPlanner::default();
    for container in pending {
        let command = planner.remove(&container.container_id);
        let mut process = Command::new(command.program());
        process
            .args(command.args())
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::piped());
        process.kill_on_drop(true);
        let result = time::timeout(CLAIM_DURATION, process.output()).await;
        let removed = match result {
            Ok(Ok(output)) if output.status.success() => true,
            Ok(Ok(output)) => {
                let error = String::from_utf8_lossy(&output.stderr);
                error.contains("No such container") || error.contains("No such object")
            }
            Ok(Err(error)) => {
                tracing::warn!(%error, container_id = %container.container_id, "cleanup failed");
                false
            }
            Err(_) => {
                tracing::warn!(container_id = %container.container_id, "cleanup timed out");
                false
            }
        };
        if removed
            && let Err(error) = database
                .jobs()
                .record_container_removed(container.attempt_id, &container.container_id)
                .await
        {
            tracing::error!(%error, container_id = %container.container_id, "cannot persist Docker cleanup");
        }
    }
}

async fn cleanup_pending_units(database: &Database) {
    let pending = match database.jobs().units_pending_cleanup().await {
        Ok(pending) => pending,
        Err(error) => {
            tracing::error!(%error, "cannot list units pending cleanup");
            return;
        }
    };
    let (_sender, mut shutdown) = watch::channel(false);
    for unit in pending {
        if systemd::unit_name(&unit.attempt_id.to_string()).as_deref() != Ok(&unit.unit_name) {
            tracing::error!(unit = %unit.unit_name, "unit cleanup identity mismatch");
            continue;
        }
        let Ok(show) = systemd::show(&unit.unit_name) else {
            continue;
        };
        let Ok(output) = unit_command(&show, database, None, &mut shutdown).await else {
            continue;
        };
        if !output.status.success() {
            continue;
        }
        let status = match unit.invocation_id.as_deref() {
            Some(id) => systemd::parse_status(&output.stdout, id),
            None => systemd::parse_unclaimed_status(&output.stdout),
        };
        if matches!(
            status.outcome,
            UnitOutcome::Indeterminate | UnitOutcome::Running { .. }
        ) {
            continue;
        }
        let mut missing = matches!(status.outcome, UnitOutcome::Missing);
        if !missing {
            // A reserved unit without a recorded invocation can only be cleaned
            // after it is observed missing. Otherwise its origin is ambiguous.
            if unit.invocation_id.is_none() {
                continue;
            }
            let Ok(stop) = systemd::stop(&unit.unit_name) else {
                continue;
            };
            if !matches!(unit_command(&stop, database, None, &mut shutdown).await, Ok(output) if output.status.success())
            {
                continue;
            }
            let Ok(output) = unit_command(&show, database, None, &mut shutdown).await else {
                continue;
            };
            if !output.status.success() {
                continue;
            }
            missing = matches!(
                systemd::parse_unclaimed_status(&output.stdout).outcome,
                UnitOutcome::Missing
            );
        }
        if !missing {
            let Ok(reset) = systemd::cleanup(&unit.unit_name) else {
                continue;
            };
            if !matches!(unit_command(&reset, database, None, &mut shutdown).await, Ok(output) if output.status.success())
            {
                continue;
            }
        }
        let Ok(output) = unit_command(&show, database, None, &mut shutdown).await else {
            continue;
        };
        if !output.status.success()
            || !matches!(
                systemd::parse_unclaimed_status(&output.stdout).outcome,
                UnitOutcome::Missing
            )
        {
            continue;
        }
        if let Err(error) = database
            .jobs()
            .record_unit_removed(unit.attempt_id, &unit.unit_name)
            .await
        {
            tracing::error!(%error, unit = %unit.unit_name, "cannot persist unit cleanup");
        }
    }
}

async fn finish_created_container_failure(
    database: &Database,
    claim: &ExecutionClaim,
    prefix: &str,
    detail: &str,
) -> bool {
    let error = format!("{prefix}: {detail}");
    let finish = ContainerFinish {
        state: DockerContainerState::Exited,
        docker_status: Some("created".into()),
        exit_code: None,
        oom_killed: false,
        error: Some(error.clone()),
    };
    let outcome = ExecutionOutcome {
        state: AttemptState::Failed,
        exit_code: None,
        term_signal: None,
        error: Some(error),
    };
    match database
        .jobs()
        .finish_container_execution(claim, &finish, &outcome)
        .await
    {
        Ok(()) => true,
        Err(error) => {
            tracing::error!(%error, job_id = %claim.job.spec.id, "cannot persist Docker launch failure");
            false
        }
    }
}

async fn execute_docker(
    database: &Database,
    paths: &RuntimePaths,
    claim: &ExecutionClaim,
    shutdown: &mut watch::Receiver<bool>,
) {
    let spec = match claim.attempt.spec.executor() {
        ExecutorSpec::Docker(s) => s,
        _ => unreachable!(),
    };
    let planner = DockerCommandPlanner::default();
    async fn fail(
        database: &Database,
        claim: &ExecutionClaim,
        prefix: &str,
        detail: impl std::fmt::Display,
    ) {
        finish_launch_failure(database, claim, &format!("{prefix}: {detail}")).await;
    }
    if let Err(e) = validate_docker_mount_sources(spec) {
        fail(database, claim, "docker_create_failed", e).await;
        return;
    }
    for (command, prefix) in [
        (planner.client_version(), "docker_cli_unavailable"),
        (planner.server_version(), "docker_daemon_unavailable"),
    ] {
        match docker_command(
            &command,
            database,
            claim,
            shutdown,
            Stdio::piped(),
            Stdio::piped(),
        )
        .await
        {
            Ok(output) if output.status.success() => {}
            Ok(output) => {
                fail(
                    database,
                    claim,
                    prefix,
                    String::from_utf8_lossy(&output.stderr).trim().to_owned(),
                )
                .await;
                return;
            }
            Err(e) if e == "worker shutdown" => return,
            Err(e) => {
                let prefix = if e.starts_with("docker_heartbeat_failed:") {
                    "docker_heartbeat_failed"
                } else {
                    prefix
                };
                fail(database, claim, prefix, e).await;
                return;
            }
        }
    }
    let image_command = planner.image_inspect(&spec.image_reference());
    let image_output = match docker_command(
        &image_command,
        database,
        claim,
        shutdown,
        Stdio::piped(),
        Stdio::piped(),
    )
    .await
    {
        Ok(output) if output.status.success() => output,
        Ok(output) => {
            fail(
                database,
                claim,
                "docker_image_missing_or_invalid",
                String::from_utf8_lossy(&output.stderr).trim(),
            )
            .await;
            return;
        }
        Err(e) if e == "worker shutdown" => return,
        Err(e) => {
            fail(database, claim, "docker_image_missing_or_invalid", e).await;
            return;
        }
    };
    let image_id = match igor_core::parse_docker_create_stdout(&image_output.stdout) {
        Ok(id) => id,
        Err(error) => {
            fail(database, claim, "docker_image_missing_or_invalid", error).await;
            return;
        }
    };
    let image = igor_core::DockerImageIdentity {
        reference: spec.image_reference(),
        image_id,
    };
    if let Err(error) = image.validate() {
        fail(database, claim, "docker_image_missing_or_invalid", error).await;
        return;
    }
    let (stdout_path, stderr_path) =
        match prepare_logs(&paths.log_dir, &claim.attempt.spec.id().to_string()) {
            Ok(p) => p,
            Err(e) => {
                fail(database, claim, "docker_create_failed", e).await;
                return;
            }
        };
    let stdout = match open_log(&stdout_path) {
        Ok(file) => file,
        Err(error) => {
            fail(database, claim, "docker_logs_failed", error).await;
            return;
        }
    };
    let stderr = match open_log(&stderr_path) {
        Ok(file) => file,
        Err(error) => {
            fail(database, claim, "docker_logs_failed", error).await;
            return;
        }
    };
    let identity = DockerIdentity {
        project_id: claim.job.spec.project_id,
        job_id: claim.job.spec.id,
        attempt_id: claim.attempt.spec.id(),
        generation_id: claim
            .attempt
            .spec
            .family()
            .map(|family| family.generation.id),
    };
    let plan = match planner.create(
        spec,
        claim.attempt.spec.command(),
        identity,
        &claim.assigned_gpus,
    ) {
        Ok(p) => p,
        Err(e) => {
            fail(database, claim, "docker_create_failed", e).await;
            return;
        }
    };
    let output = match docker_command(
        &plan.command,
        database,
        claim,
        shutdown,
        Stdio::piped(),
        Stdio::piped(),
    )
    .await
    {
        Ok(o) => o,
        Err(e) if e == "worker shutdown" => return,
        Err(e) => {
            fail(database, claim, "docker_create_failed", e).await;
            return;
        }
    };
    if !output.status.success() {
        fail(
            database,
            claim,
            "docker_create_failed",
            String::from_utf8_lossy(&output.stderr).trim().to_owned(),
        )
        .await;
        return;
    }
    let id = match igor_core::parse_docker_create_stdout(&output.stdout) {
        Ok(id) => id,
        Err(e) => {
            fail(database, claim, "docker_create_failed", e).await;
            return;
        }
    };
    let container = ContainerCreate {
        container_id: id.clone(),
        container_name: plan.container_name.clone(),
        image,
        stdout_path: stdout_path.clone(),
        stderr_path: stderr_path.clone(),
    };
    if let Err(e) = database
        .jobs()
        .record_container_created(claim, &container)
        .await
    {
        let _ = docker_command(
            &planner.remove(&id),
            database,
            claim,
            shutdown,
            Stdio::null(),
            Stdio::null(),
        )
        .await;
        fail(database, claim, "docker_identity_persistence_failed", e).await;
        return;
    }
    let start = match docker_command(
        &planner.start(&id),
        database,
        claim,
        shutdown,
        Stdio::piped(),
        Stdio::piped(),
    )
    .await
    {
        Ok(o) => o,
        Err(e) if e == "worker shutdown" => return,
        Err(e) if e.starts_with("docker_heartbeat_failed:") => {
            tracing::error!(error = %e, container_id = %id, "Docker start supervision lost ownership");
            return;
        }
        Err(e) => {
            if finish_created_container_failure(database, claim, "docker_start_failed", &e).await
                && spec.remove_container
            {
                docker_cleanup(&planner, database, claim, &id).await;
            }
            return;
        }
    };
    if !start.status.success() {
        let detail = String::from_utf8_lossy(&start.stderr);
        if finish_created_container_failure(database, claim, "docker_start_failed", detail.trim())
            .await
            && spec.remove_container
        {
            docker_cleanup(&planner, database, claim, &id).await;
        }
        return;
    }
    if let Err(e) = database.jobs().record_container_started(claim).await {
        tracing::error!(%e, "cannot mark Docker container running");
        return;
    }
    let container = match database
        .jobs()
        .container_for_attempt(claim.attempt.spec.id())
        .await
    {
        Ok(Some(container)) => container,
        Ok(None) => {
            tracing::error!(container_id = %id, "started Docker identity disappeared");
            return;
        }
        Err(error) => {
            tracing::error!(%error, container_id = %id, "cannot reload started Docker identity");
            return;
        }
    };
    match docker_logs_and_wait(
        &planner.logs(&id),
        &planner.wait(&id),
        database,
        claim,
        shutdown,
        DockerLogFiles { stdout, stderr },
        claim
            .attempt
            .spec
            .resources()
            .timeout_seconds
            .map(Duration::from_secs),
    )
    .await
    {
        DockerSupervision::Completed { logs, wait } => {
            if !logs.status.success() {
                tracing::warn!(status = %logs.status, container_id = %id, "Docker logs command failed before final inspection");
            }
            finalize_supervised_docker(
                database, &planner, claim, &container, &logs, &wait, shutdown,
            )
            .await;
        }
        DockerSupervision::Cancellation(grace) => {
            cancel_docker(database, &planner, claim, &container, grace, shutdown).await;
        }
        DockerSupervision::Timeout => {
            timeout_docker(database, &planner, claim, &container, shutdown).await;
        }
        DockerSupervision::Shutdown => {}
        DockerSupervision::OwnershipLost(error) => {
            tracing::error!(%error, container_id = %id, "Docker log/wait supervision lost ownership");
        }
    }
}

async fn finalize_supervised_docker(
    database: &Database,
    planner: &DockerCommandPlanner,
    claim: &ExecutionClaim,
    container: &ContainerRecord,
    logs: &std::process::Output,
    wait: &std::process::Output,
    shutdown: &mut watch::Receiver<bool>,
) {
    let mut infrastructure_error = sync_docker_logs(container)
        .err()
        .map(|error| format!("docker_logs_failed: {error}"));
    if !logs.status.success() && infrastructure_error.is_none() {
        infrastructure_error = Some(format!("docker_logs_failed: {}", logs.status));
    }
    let wait_code = if wait.status.success() {
        match igor_core::parse_docker_wait_stdout(&wait.stdout) {
            Ok(code) => Some(code),
            Err(error) => {
                infrastructure_error = Some(format!("docker_wait_failed: {error}"));
                None
            }
        }
    } else {
        infrastructure_error = Some(format!(
            "docker_wait_failed: {}",
            String::from_utf8_lossy(&wait.stderr).trim()
        ));
        None
    };
    let inspection = match inspect_docker(
        database,
        planner,
        claim,
        &container.container_id,
        shutdown,
    )
    .await
    {
        Ok(inspection) if !inspection.running => inspection,
        Ok(_) => {
            tracing::error!(container_id = %container.container_id, "docker wait returned while container is still running");
            return;
        }
        Err(DockerInspectFailure::Missing) => {
            finish_docker_lost(
                database,
                claim,
                container,
                "container disappeared after wait",
            )
            .await;
            return;
        }
        Err(DockerInspectFailure::Indeterminate(error)) => {
            tracing::error!(%error, container_id = %container.container_id, "cannot inspect Docker container after wait");
            return;
        }
    };
    if let Some(wait_code) = wait_code
        && wait_code != inspection.exit_code
    {
        infrastructure_error = Some(format!(
            "docker_inspect_failed: wait exit code {wait_code} differs from inspect exit code {}",
            inspection.exit_code
        ));
    }
    finish_docker_inspection(
        database,
        planner,
        claim,
        container,
        inspection,
        None,
        infrastructure_error,
    )
    .await;
}

async fn finish_docker_inspection(
    database: &Database,
    planner: &DockerCommandPlanner,
    claim: &ExecutionClaim,
    container: &ContainerRecord,
    inspection: DockerContainerInspection,
    forced_state: Option<AttemptState>,
    infrastructure_error: Option<String>,
) {
    let exit_code = inspection.exit_code;
    let error = if forced_state == Some(AttemptState::Cancelled) {
        Some("docker_cancelled: cancellation requested".into())
    } else if forced_state.is_some()
        && let Some(error) = infrastructure_error
    {
        Some(error)
    } else if inspection.oom_killed {
        Some("docker_oom_killed: container was killed by the OOM killer".into())
    } else if let Some(error) = infrastructure_error {
        Some(error)
    } else if let Some(error) = inspection.error.clone() {
        Some(format!("docker_application_nonzero: {error}"))
    } else if exit_code != 0 {
        Some(format!(
            "docker_application_nonzero: container exited with code {exit_code}"
        ))
    } else {
        None
    };
    let state = forced_state.unwrap_or_else(|| {
        if error.is_some() {
            AttemptState::Failed
        } else {
            AttemptState::Succeeded
        }
    });
    let finish = ContainerFinish {
        state: DockerContainerState::Exited,
        docker_status: Some(inspection.status),
        exit_code: Some(exit_code),
        oom_killed: inspection.oom_killed,
        error: error.clone(),
    };
    let outcome = ExecutionOutcome {
        state,
        exit_code: Some(exit_code),
        term_signal: None,
        error,
    };
    if database
        .jobs()
        .finish_container_execution(claim, &finish, &outcome)
        .await
        .is_ok()
        && matches!(claim.attempt.spec.executor(), ExecutorSpec::Docker(spec) if spec.remove_container)
    {
        docker_cleanup(planner, database, claim, &container.container_id).await;
    }
}

async fn cancel_docker(
    database: &Database,
    planner: &DockerCommandPlanner,
    claim: &ExecutionClaim,
    container: &ContainerRecord,
    grace: Duration,
    shutdown: &mut watch::Receiver<bool>,
) {
    if !grace.is_zero() {
        let seconds = grace.as_secs().max(1);
        let stop_command = planner.stop(&container.container_id, seconds);
        if let Err(error) =
            docker_stop_with_deadline(&stop_command, database, claim, shutdown, grace).await
            && error != "Docker stop grace period elapsed"
        {
            tracing::error!(%error, container_id = %container.container_id, "Docker stop was not confirmed");
            return;
        }
    }
    let mut inspection = match inspect_docker(
        database,
        planner,
        claim,
        &container.container_id,
        shutdown,
    )
    .await
    {
        Ok(inspection) => inspection,
        Err(DockerInspectFailure::Missing) => {
            finish_docker_lost(
                database,
                claim,
                container,
                "container disappeared during cancellation",
            )
            .await;
            return;
        }
        Err(DockerInspectFailure::Indeterminate(error)) => {
            tracing::error!(%error, container_id = %container.container_id, "cannot inspect Docker cancellation");
            return;
        }
    };
    if inspection.running {
        match docker_command(
            &planner.kill(&container.container_id),
            database,
            claim,
            shutdown,
            Stdio::piped(),
            Stdio::piped(),
        )
        .await
        {
            Ok(output) if output.status.success() => {}
            Ok(output) => {
                tracing::error!(error = %String::from_utf8_lossy(&output.stderr), container_id = %container.container_id, "Docker kill failed");
                return;
            }
            Err(error) => {
                tracing::error!(%error, container_id = %container.container_id, "Docker kill was not confirmed");
                return;
            }
        }
        inspection = match inspect_docker(
            database,
            planner,
            claim,
            &container.container_id,
            shutdown,
        )
        .await
        {
            Ok(inspection) if !inspection.running => inspection,
            Ok(_) => return,
            Err(DockerInspectFailure::Missing) => {
                finish_docker_lost(
                    database,
                    claim,
                    container,
                    "container disappeared after kill",
                )
                .await;
                return;
            }
            Err(DockerInspectFailure::Indeterminate(error)) => {
                tracing::error!(%error, container_id = %container.container_id, "cannot confirm Docker kill");
                return;
            }
        };
    }
    if let Err(error) = capture_docker_snapshot(database, planner, claim, container, shutdown).await
    {
        tracing::error!(%error, container_id = %container.container_id, "cannot persist cancelled Docker logs");
        return;
    }
    finish_docker_inspection(
        database,
        planner,
        claim,
        container,
        inspection,
        Some(AttemptState::Cancelled),
        None,
    )
    .await;
}

async fn timeout_docker(
    database: &Database,
    planner: &DockerCommandPlanner,
    claim: &ExecutionClaim,
    container: &ContainerRecord,
    shutdown: &mut watch::Receiver<bool>,
) {
    match docker_command(
        &planner.kill(&container.container_id),
        database,
        claim,
        shutdown,
        Stdio::piped(),
        Stdio::piped(),
    )
    .await
    {
        Ok(output) if output.status.success() => {}
        Ok(output) => {
            tracing::error!(error = %String::from_utf8_lossy(&output.stderr), container_id = %container.container_id, "cannot kill timed-out Docker container");
            return;
        }
        Err(error) => {
            tracing::error!(%error, container_id = %container.container_id, "Docker timeout kill was not confirmed");
            return;
        }
    }
    let inspection = match inspect_docker(
        database,
        planner,
        claim,
        &container.container_id,
        shutdown,
    )
    .await
    {
        Ok(inspection) if !inspection.running => inspection,
        Ok(_) => return,
        Err(DockerInspectFailure::Missing) => {
            finish_docker_lost(
                database,
                claim,
                container,
                "container disappeared after timeout",
            )
            .await;
            return;
        }
        Err(DockerInspectFailure::Indeterminate(error)) => {
            tracing::error!(%error, container_id = %container.container_id, "cannot confirm Docker timeout kill");
            return;
        }
    };
    if let Err(error) = capture_docker_snapshot(database, planner, claim, container, shutdown).await
    {
        tracing::error!(%error, container_id = %container.container_id, "cannot persist timed-out Docker logs");
        return;
    }
    finish_docker_inspection(
        database,
        planner,
        claim,
        container,
        inspection,
        Some(AttemptState::Failed),
        Some("docker_timeout: configured execution timeout elapsed".into()),
    )
    .await;
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
    match DirBuilder::new().mode(0o700).create(&attempt_dir) {
        Ok(()) => {}
        Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {}
        Err(error) => return Err(error),
    }
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

fn reset_recovery_logs(container: &ContainerRecord) -> io::Result<(fs::File, fs::File)> {
    Ok((
        reset_recovery_log(&container.stdout_path)?,
        reset_recovery_log(&container.stderr_path)?,
    ))
}

fn reset_recovery_log(path: &Path) -> io::Result<fs::File> {
    OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .mode(0o600)
        .custom_flags(nix::libc::O_NOFOLLOW)
        .open(path)
}

fn sync_docker_logs(container: &ContainerRecord) -> io::Result<()> {
    for path in [&container.stdout_path, &container.stderr_path] {
        OpenOptions::new().write(true).open(path)?.sync_all()?;
    }
    Ok(())
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

fn configure_execution_environment(
    command: &mut Command,
    policy: &igor_core::EnvironmentPolicy,
    assigned_gpus: &[String],
) -> io::Result<()> {
    configure_environment(command, policy)?;
    command.env("CUDA_VISIBLE_DEVICES", assigned_gpus.join(","));
    Ok(())
}

fn minimal_environment_name(name: &str) -> bool {
    matches!(
        name,
        "HOME" | "LANG" | "LOGNAME" | "PATH" | "TERM" | "TMPDIR" | "TZ" | "USER"
    ) || name.starts_with("LC_")
}

fn process_start_ticks(pid: u32) -> io::Result<i64> {
    Ok(process_identity(pid)?.start_ticks)
}

struct ProcessIdentity {
    process_group_id: i64,
    start_ticks: i64,
}

fn process_identity(pid: u32) -> io::Result<ProcessIdentity> {
    let stat = fs::read_to_string(format!("/proc/{pid}/stat"))?;
    let end = stat
        .rfind(')')
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "missing process name"))?;
    let fields: Vec<_> = stat[end + 1..].split_whitespace().collect();
    let process_group_id = fields
        .get(2)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "missing process group"))?
        .parse::<i64>()
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
    let start_ticks = fields
        .get(19)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "missing process start time"))?
        .parse::<i64>()
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
    Ok(ProcessIdentity {
        process_group_id,
        start_ticks,
    })
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

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn assigned_gpus_override_the_requested_process_environment() -> io::Result<()> {
        let mut policy = igor_core::EnvironmentPolicy::default();
        policy
            .set
            .insert("CUDA_VISIBLE_DEVICES".into(), "unreserved".into());
        let mut command = Command::new("/bin/sh");
        command.args(["-c", "printf %s \"$CUDA_VISIBLE_DEVICES\""]);
        configure_execution_environment(
            &mut command,
            &policy,
            &["GPU-two".into(), "GPU-one".into()],
        )?;
        let output = command.output().await?;
        assert!(output.status.success());
        assert_eq!(output.stdout, b"GPU-two,GPU-one");
        Ok(())
    }

    #[tokio::test]
    async fn jobs_without_gpu_leases_cannot_inherit_gpu_visibility() -> io::Result<()> {
        let mut policy = igor_core::EnvironmentPolicy::default();
        policy
            .set
            .insert("CUDA_VISIBLE_DEVICES".into(), "unreserved".into());
        let mut command = Command::new("/bin/sh");
        command.args(["-c", "printf %s \"$CUDA_VISIBLE_DEVICES\""]);
        configure_execution_environment(&mut command, &policy, &[])?;
        let output = command.output().await?;
        assert!(output.status.success());
        assert!(output.stdout.is_empty());
        Ok(())
    }
}
