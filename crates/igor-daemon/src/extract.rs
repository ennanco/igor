use std::{fs, path::Path, process::Stdio, time::Duration};

use igor_core::{
    ActionId, ActionRetryPolicy, AttemptState, Claim, ClaimedAction, CommandSpec, Database,
    RuntimePaths, parse_metric_document_json, validate_metric_document,
};
use serde_json::{Value, json};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWriteExt},
    process::Command,
    sync::watch,
    time,
};

const LEASE: Duration = Duration::from_secs(30);
const HEARTBEAT: Duration = Duration::from_secs(5);
const POLL: Duration = Duration::from_millis(200);
const MAX_JSON_BYTES: u64 = 1024 * 1024;
const MAX_STDERR_BYTES: u64 = 64 * 1024;

pub(crate) async fn run(
    database: Database,
    paths: RuntimePaths,
    mut shutdown: watch::Receiver<bool>,
) {
    let owner = format!("extractor:{}", std::process::id());
    loop {
        if *shutdown.borrow() {
            return;
        }
        match database.actions().claim_extraction(&owner, LEASE).await {
            Ok(Some(claim)) => match database.actions().claimed(&claim).await {
                Ok(Some(work)) => {
                    if let Some(result) =
                        extract(&database, &paths, &claim, &work, &mut shutdown).await
                    {
                        match result {
                            Ok(document) => {
                                if let Some(attempt_id) = work.attempt_id
                                    && let Err(error) = database
                                        .actions()
                                        .complete_extraction(&claim, attempt_id, &document)
                                        .await
                                {
                                    tracing::error!(%error, action_id = %claim.record_id, "cannot persist extractor document and action outcome");
                                }
                            }
                            Err(reason) => {
                                if let Err(error) = database
                                    .actions()
                                    .retry(&claim, &ActionRetryPolicy::default().0, reason)
                                    .await
                                {
                                    tracing::error!(%error, action_id = %claim.record_id, "cannot persist extractor failure");
                                }
                            }
                        }
                    }
                }
                Ok(None) => {}
                Err(error) => tracing::error!(%error, "cannot read claimed extraction action"),
            },
            Ok(None) => {}
            Err(error) => tracing::error!(%error, "cannot claim metric extraction action"),
        }
        tokio::select! {
            _ = time::sleep(POLL) => {}
            _ = shutdown.changed() => return,
        }
    }
}

async fn extract(
    database: &Database,
    paths: &RuntimePaths,
    claim: &Claim<ActionId>,
    work: &ClaimedAction,
    shutdown: &mut watch::Receiver<bool>,
) -> Option<Result<Value, &'static str>> {
    let prepared = prepare(database, paths, work).await;
    let (command, input, attempt_dir, publication_root) = match prepared {
        Ok(prepared) => prepared,
        Err(reason) => return Some(Err(reason)),
    };
    let output = run_command(database, claim, &command, &attempt_dir, &input, shutdown).await?;
    let stdout = match output {
        Ok(stdout) => stdout,
        Err(reason) => return Some(Err(reason)),
    };
    let document: Value = match parse_metric_document_json(&stdout) {
        Ok(document) => document,
        Err(_) => return Some(Err("invalid_extractor_json")),
    };
    if validate_metric_document(&document).is_err() {
        return Some(Err("invalid_extractor_schema"));
    }
    if let Some(attempt_id) = work.attempt_id
        && let Some(declarations) = document.get("artifacts").and_then(Value::as_array)
    {
        let Some(publication_root) = publication_root else {
            return Some(Err("missing_publication_root"));
        };
        for declaration in declarations {
            let registered = match database
                .artifacts()
                .register_claimed(claim, attempt_id, &attempt_dir, declaration)
                .await
            {
                Ok(artifact) => artifact,
                Err(error) => {
                    tracing::warn!(%error, action_id = %claim.record_id, "could not register declared artifact");
                    return Some(Err("artifact_registration_failed"));
                }
            };
            if let Err(error) = database
                .artifacts()
                .stage_claimed(
                    claim,
                    registered.id,
                    attempt_id,
                    &attempt_dir,
                    &paths.state_dir.join("artifact-staging"),
                )
                .await
            {
                tracing::warn!(%error, action_id = %claim.record_id, "could not stage declared artifact");
                return Some(Err("artifact_staging_failed"));
            }
            if document["status"] == "success"
                && let Err(error) = database
                    .artifacts()
                    .prepare_publication_claimed(
                        claim,
                        registered.id,
                        attempt_id,
                        &paths.state_dir.join("artifact-staging"),
                        &publication_root,
                    )
                    .await
            {
                tracing::warn!(%error, action_id = %claim.record_id, "could not prepare artifact publication");
                return Some(Err("artifact_publication_failed"));
            }
        }
        if document["status"] == "success"
            && let Err(error) = database
                .artifacts()
                .finalize_publication_claimed(
                    claim,
                    attempt_id,
                    &paths.state_dir.join("artifact-staging"),
                    &publication_root,
                    &document,
                )
                .await
        {
            tracing::warn!(%error, action_id = %claim.record_id, "could not atomically expose artifact directory");
            return Some(Err("artifact_publication_failed"));
        }
    }
    Some(Ok(document))
}

async fn prepare(
    database: &Database,
    paths: &RuntimePaths,
    work: &ClaimedAction,
) -> Result<
    (
        CommandSpec,
        Vec<u8>,
        std::path::PathBuf,
        Option<std::path::PathBuf>,
    ),
    &'static str,
> {
    if work.action.kind != "extract_metrics" || work.action.spec.get("version") != Some(&json!(1)) {
        return Err("invalid_extraction_action");
    }
    let job_id = work.job_id.ok_or("missing_extraction_job")?;
    let attempt_id = work.attempt_id.ok_or("missing_extraction_attempt")?;
    let detail = database
        .jobs()
        .detail(job_id)
        .await
        .map_err(|_| "cannot_read_extraction_job")?
        .ok_or("missing_extraction_job")?;
    if detail.job.spec.project_id != work.action.project_id {
        return Err("invalid_extraction_project");
    }
    let attempt = detail
        .attempts
        .iter()
        .find(|attempt| attempt.spec.id() == attempt_id)
        .ok_or("missing_extraction_attempt")?;
    if attempt.state != AttemptState::Succeeded {
        return Err("extraction_attempt_not_successful");
    }
    let command = attempt
        .spec
        .result()
        .extractor
        .as_ref()
        .ok_or("missing_frozen_extractor")?;
    let frozen: CommandSpec = serde_json::from_value(
        work.action
            .spec
            .get("extractor")
            .cloned()
            .ok_or("missing_frozen_extractor")?,
    )
    .map_err(|_| "invalid_frozen_extractor")?;
    if frozen != *command {
        return Err("extraction_command_mismatch");
    }
    let attempt_dir = paths.log_dir.join(attempt_id.to_string());
    if !fs::symlink_metadata(&attempt_dir).is_ok_and(|metadata| metadata.file_type().is_dir()) {
        return Err("invalid_extraction_attempt_directory");
    }
    let logs = database
        .jobs()
        .logs_for_attempt(attempt_id)
        .await
        .map_err(|_| "missing_extraction_logs")?;
    let stdout = checked_log(&logs.stdout, &attempt_dir, "stdout.log")?;
    let stderr = checked_log(&logs.stderr, &attempt_dir, "stderr.log")?;
    let membership = attempt.spec.family();
    let input = json!({
        "schema_version": 1,
        "project_id": work.action.project_id,
        "job_id": job_id,
        "attempt_id": attempt_id,
        "family_id": membership.map(|member| member.family_id),
        "generation_id": membership.map(|member| member.generation.id),
        "seed": membership.map(|member| member.seed.0),
        "stdout_path": stdout,
        "stderr_path": stderr,
    });
    let mut encoded = serde_json::to_vec(&input).map_err(|_| "invalid_extraction_context")?;
    encoded.push(b'\n');
    Ok((
        frozen,
        encoded,
        attempt_dir,
        attempt.spec.result().publication_root.clone(),
    ))
}

fn checked_log(
    path: &Path,
    attempt_dir: &Path,
    expected: &str,
) -> Result<std::path::PathBuf, &'static str> {
    if path != attempt_dir.join(expected)
        || !fs::symlink_metadata(path).is_ok_and(|metadata| metadata.file_type().is_file())
    {
        return Err("invalid_extraction_log_path");
    }
    Ok(path.to_path_buf())
}

async fn run_command(
    database: &Database,
    claim: &Claim<ActionId>,
    extractor: &CommandSpec,
    attempt_dir: &Path,
    input: &[u8],
    shutdown: &mut watch::Receiver<bool>,
) -> Option<Result<Vec<u8>, &'static str>> {
    let mut command = Command::new(&extractor.program);
    command
        .args(&extractor.args)
        .current_dir(attempt_dir)
        .env_clear()
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    if let Some(path) = std::env::var_os("PATH") {
        command.env("PATH", path);
    }
    let mut child = match command.spawn() {
        Ok(child) => child,
        Err(_) => return Some(Err("extractor_spawn_failed")),
    };
    if let Some(mut stdin) = child.stdin.take() {
        if stdin.write_all(input).await.is_err() {
            return Some(Err("extractor_stdin_failed"));
        }
    } else {
        return Some(Err("extractor_stdin_unavailable"));
    }
    let stdout = match child.stdout.take() {
        Some(stdout) => stdout,
        None => return Some(Err("extractor_stdout_unavailable")),
    };
    let stderr = match child.stderr.take() {
        Some(stderr) => stderr,
        None => return Some(Err("extractor_stderr_unavailable")),
    };
    let mut output = tokio::spawn(async move {
        let (stdout, _stderr) = tokio::try_join!(
            read_limited(stdout, MAX_JSON_BYTES),
            read_limited(stderr, MAX_STDERR_BYTES),
        )?;
        if !child
            .wait()
            .await
            .map_err(|_| "extractor_wait_failed")?
            .success()
        {
            return Err("extractor_exited_nonzero");
        }
        Ok(stdout)
    });
    let mut heartbeat = time::interval(HEARTBEAT);
    heartbeat.set_missed_tick_behavior(time::MissedTickBehavior::Delay);
    heartbeat.tick().await;
    loop {
        tokio::select! {
            result = &mut output => return Some(result.unwrap_or(Err("extractor_task_failed"))),
            _ = heartbeat.tick() => {
                if let Err(error) = database.actions().heartbeat(claim, LEASE).await {
                    tracing::error!(%error, action_id = %claim.record_id, "metric extractor lost its action lease");
                    output.abort();
                    let _ = output.await;
                    return None;
                }
            }
            _ = shutdown.changed() => {
                output.abort();
                let _ = output.await;
                return None;
            }
        }
    }
}

async fn read_limited<R: AsyncRead + Unpin>(
    reader: R,
    limit: u64,
) -> Result<Vec<u8>, &'static str> {
    let mut bytes = Vec::new();
    reader
        .take(limit + 1)
        .read_to_end(&mut bytes)
        .await
        .map_err(|_| "extractor_output_read_failed")?;
    if bytes.len() as u64 > limit {
        Err("extractor_output_too_large")
    } else {
        Ok(bytes)
    }
}
