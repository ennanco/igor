use super::*;
use igor_core::{GcOperation, GcStatus, RetiredLogTail, RuntimePaths};
use serde_json::Value;

struct FailedLogs {
    _directory: TempDir,
    database: Database,
    paths: RuntimePaths,
    attempt: AttemptId,
    job: JobId,
}

struct LiveGroup(std::process::Child);
impl Drop for LiveGroup {
    fn drop(&mut self) {
        let _ = nix::sys::signal::killpg(
            nix::unistd::Pid::from_raw(self.0.id() as i32),
            nix::sys::signal::Signal::SIGKILL,
        );
        let _ = self.0.wait();
    }
}

impl FailedLogs {
    async fn new(
        policy: Value,
        stdout: &[u8],
        stderr: &[u8],
        kb: u32,
    ) -> Result<Self, Box<dyn Error>> {
        let (directory, database) = database().await?;
        let (project, job) = insert_project_job(&database).await?;
        let root = directory.path();
        let paths = RuntimePaths {
            config_file: root.join("config"),
            state_dir: root.join("state"),
            runtime_dir: root.join("runtime"),
            database: root.join("nested/igor.db"),
            log_dir: root.join("logs"),
            worktree_dir: root.join("worktrees"),
            report_dir: root.join("reports"),
            worker_socket: root.join("worker.sock"),
            supervisor_socket: root.join("supervisor.sock"),
            resource_dir: root.join("resources"),
        };
        let mut value = serde_json::to_value(attempt(&job, 1)?)?;
        value["result"]["failed_full_logs"] = policy;
        value["result"]["failed_log_tail_kb"] = json!(kb);
        let spec: AttemptSpec = serde_json::from_value(value)?;
        let attempt = spec.id();
        database
            .jobs()
            .insert_attempt_with_event(&spec, &event(EventKind::AttemptCreated, json!({}))?)
            .await?;
        let dir = paths.log_dir.join(attempt.to_string());
        fs::create_dir_all(&dir)?;
        fs::write(dir.join("stdout.log"), stdout)?;
        fs::write(dir.join("stderr.log"), stderr)?;
        sqlx::query("INSERT INTO attempt_processes(attempt_id, job_id, project_id, pid, process_group_id, process_start_ticks, stdout_path, stderr_path) VALUES (?, ?, ?, 2147483647, 2147483647, 1, ?, ?)").bind(attempt.to_string()).bind(job.id.to_string()).bind(project.id.to_string()).bind(dir.join("stdout.log").to_str()).bind(dir.join("stderr.log").to_str()).execute(database.pool()).await?;
        sqlx::query("UPDATE attempts SET state = 'failed'")
            .execute(database.pool())
            .await?;
        sqlx::query("UPDATE jobs SET state = 'failed'")
            .execute(database.pool())
            .await?;
        let tail = |bytes: &[u8]| bytes[bytes.len().saturating_sub(kb as usize * 1024)..].to_vec();
        sqlx::query("INSERT INTO failed_log_tails(attempt_id, stdout, stderr, limit_kb, complete) VALUES (?, ?, ?, ?, 1)").bind(attempt.to_string()).bind(tail(stdout)).bind(tail(stderr)).bind(kb).execute(database.pool()).await?;
        Ok(Self {
            _directory: directory,
            database,
            paths,
            attempt,
            job: job.id,
        })
    }
    fn raw(&self, stream: &str) -> PathBuf {
        self.paths
            .log_dir
            .join(self.attempt.to_string())
            .join(format!("{stream}.log"))
    }
    async fn diagnose(&self) -> TestResult {
        let id = uuid::Uuid::new_v4().to_string();
        sqlx::query("INSERT INTO recoveries(id,attempt_id,state,diagnosis_json,idempotency_key) VALUES (?,?,'proposed','{}',?)").bind(&id).bind(self.attempt.to_string()).bind(&id).execute(self.database.pool()).await?;
        Ok(())
    }
    async fn retire(&self) -> Result<usize, PersistenceError> {
        self.database
            .jobs()
            .retire_failed_logs(self.attempt, &self.paths)
            .await
    }
}

#[tokio::test]
async fn gc_failed_logs_saved_readonly_plan_removes_exact_paths_and_preserves_binary_tails()
-> TestResult {
    use igor_core::{GcEffect, GcExecutionStatus, GcTarget};

    let binary: Vec<u8> = (0..=255).cycle().take(4099).collect();
    let f = FailedLogs::new(json!("delete-after-diagnosis"), b"", &binary, 1).await?;
    f.diagnose().await?;
    let external = TempDir::new()?;
    fs::write(external.path().join("stdout.log"), b"external\0\xff")?;
    let unrelated = f
        .raw("stdout")
        .parent()
        .ok_or("parent")?
        .join("unselected.log");
    fs::write(&unrelated, b"unselected\xff\0")?;
    let external_before = gc_support::files(external.path(), &f.paths.database)?;
    let disk_before = gc_support::files(f._directory.path(), &f.paths.database)?;
    let sql_before = gc_support::sql_snapshot(&f.database).await?;
    let plan = gc_support::readonly_plan(&f.database, &f.paths, f._directory.path(), None).await?;
    assert_eq!(gc_support::sql_snapshot(&f.database).await?, sql_before);
    assert_eq!(
        gc_support::files(f._directory.path(), &f.paths.database)?,
        disk_before
    );
    assert_eq!(plan.entries.len(), 2);
    for (entry, (stream, expected)) in plan
        .entries
        .iter()
        .zip([("stdout", b"".as_slice()), ("stderr", binary.as_slice())])
    {
        assert_eq!(fs::read(f.raw(stream))?, expected);
        assert_eq!(entry.operation, GcOperation::FailedLogRetirement);
        assert_eq!(entry.subject_id, format!("{}:{stream}", f.attempt));
        assert_eq!(entry.status, GcStatus::Ready);
        assert_eq!(entry.effect, GcEffect::Remove);
        assert_eq!(
            entry.targets,
            [GcTarget {
                path: f.raw(stream),
                bytes: Some(expected.len() as u64),
                effect: GcEffect::Remove,
            }]
        );
    }
    let worklist = plan.worklist()?;
    assert_eq!(worklist.selections.len(), 2);
    for (selection, entry) in worklist.selections.iter().zip(&plan.entries) {
        assert_eq!(&selection.expected, entry);
    }
    let result = f
        .database
        .gc_execute(None, false, worklist, &f.paths)
        .await?;
    assert_eq!(result.completed, 2, "{result:?}");
    assert_eq!(
        (
            result.already_done,
            result.deferred,
            result.unsafe_count,
            result.errors
        ),
        (0, 0, 0, 0)
    );
    assert_eq!(result.entries.len(), 2);
    for (stream, expected) in [("stdout", b"".as_slice()), ("stderr", binary.as_slice())] {
        let subject = format!("{}:{stream}", f.attempt);
        let response = result
            .entries
            .iter()
            .find(|e| e.subject_id == subject)
            .ok_or("stream response")?;
        assert_eq!(response.operation, GcOperation::FailedLogRetirement);
        assert_eq!(response.status, GcExecutionStatus::Completed);
        assert!(response.cause.is_none());
        assert!(!f.raw(stream).try_exists()?);
        let tail = f
            .database
            .jobs()
            .retired_log_tail(f.attempt, stream)
            .await?
            .ok_or("tail")?;
        let start = expected.len().saturating_sub(1024);
        assert_eq!(tail.original_path, f.raw(stream));
        assert_eq!(tail.original_size, expected.len() as u64);
        assert_eq!(
            (tail.start, tail.end),
            (start as u64, expected.len() as u64)
        );
        let chunk = f.database.jobs().retired_log_chunk(&tail, 0, 65536).await?;
        assert_eq!(chunk.bytes, &expected[start..]);
        assert_eq!(
            (chunk.start, chunk.end),
            (start as u64, expected.len() as u64)
        );
        if !expected.is_empty() {
            let offset = start + 17;
            let chunk = f
                .database
                .jobs()
                .retired_log_chunk(&tail, offset as u64, 31)
                .await?;
            assert_eq!(chunk.bytes, &expected[offset..offset + 31]);
            assert_eq!(
                (chunk.start, chunk.end),
                (offset as u64, (offset + 31) as u64)
            );
        }
        let state: String = sqlx::query_scalar(
            "SELECT state FROM failed_log_retirements WHERE attempt_id = ? AND stream = ?",
        )
        .bind(f.attempt.to_string())
        .bind(stream)
        .fetch_one(f.database.pool())
        .await?;
        assert_eq!(state, "completed");
        let tombstone = f
            .database
            .cleanup_tombstone("failed_log_retirement", &subject)
            .await?
            .ok_or("tombstone")?;
        assert_eq!(tombstone["subject_id"], subject);
        assert_eq!(tombstone["outcome"], "raw_retired");
        assert_eq!(tombstone["identity"], serde_json::to_value(&tail)?);
    }
    let expected_disk: Vec<_> = disk_before
        .into_iter()
        .filter(|(path, _, _)| *path != f.raw("stdout") && *path != f.raw("stderr"))
        .collect();
    assert_eq!(
        gc_support::files(f._directory.path(), &f.paths.database)?,
        expected_disk
    );
    assert_eq!(
        gc_support::files(external.path(), &f.paths.database)?,
        external_before
    );
    Ok(())
}

#[tokio::test]
async fn failed_full_logs_zero_artifacts_durable_barriers_and_chunks() -> TestResult {
    let bytes: Vec<u8> = (0..=255).cycle().take(200_003).collect();
    let f = FailedLogs::new(json!("delete-after-diagnosis"), &bytes, b"", 1).await?;
    assert_eq!(f.retire().await?, 0);
    assert!(
        f.database
            .jobs()
            .failed_log_candidates(None)
            .await?
            .is_empty()
    );
    f.diagnose().await?;
    sqlx::query("UPDATE failed_log_tails SET complete = 0")
        .execute(f.database.pool())
        .await?;
    assert_eq!(f.retire().await?, 0);
    sqlx::query("UPDATE failed_log_tails SET complete = 1")
        .execute(f.database.pool())
        .await?;
    let plan = f.database.gc_plan_filtered(None, &f.paths, true).await?;
    let entries: Vec<_> = plan
        .entries
        .iter()
        .filter(|e| e.operation == GcOperation::FailedLogRetirement)
        .collect();
    assert_eq!(entries.len(), 2);
    assert!(entries.iter().all(|e| e.status == GcStatus::Ready));
    let work = plan.worklist()?;
    let result = f
        .database
        .gc_execute(None, true, work.clone(), &f.paths)
        .await?;
    assert_eq!(result.completed, 2, "{result:?}");
    assert!(!f.raw("stdout").exists());
    assert!(!f.raw("stderr").exists());
    let logs = f.database.jobs().logs_for_job(f.job).await?;
    let id = logs.stdout_tail.ok_or("descriptor")?;
    assert_eq!(id.start, bytes.len() as u64 - 1024);
    assert_eq!(id.end, bytes.len() as u64);
    let chunk = f.database.jobs().retired_log_chunk(&id, 0, 65536).await?;
    assert_eq!(chunk.bytes, &bytes[bytes.len() - 1024..]);
    assert_eq!(chunk.start, id.start);
    assert_eq!(chunk.end, id.end);
    assert!(
        f.database
            .jobs()
            .retired_log_chunk(&id, 0, 65537)
            .await
            .is_err()
    );
    assert!(
        f.database
            .jobs()
            .retired_log_chunk(&id, id.end + 1, 1)
            .await
            .is_err()
    );
    assert!(
        sqlx::query("UPDATE failed_log_tails SET stdout = X'00'")
            .execute(f.database.pool())
            .await
            .is_err()
    );
    assert_eq!(f.retire().await?, 0);
    let result = f.database.gc_execute(None, true, work, &f.paths).await?;
    assert_eq!(result.already_done, 2);
    let events: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM events WHERE kind = 'cleanup_state_changed'")
            .fetch_one(f.database.pool())
            .await?;
    assert_eq!(events, 2);
    let retried = f.database.jobs().retry(f.job).await?;
    assert_eq!(
        retried
            .attempts
            .last()
            .ok_or("retry")?
            .spec
            .result()
            .failed_full_logs,
        Some(igor_core::FailedRetention::DeleteAfterDiagnosis)
    );
    let pinned = f
        .database
        .jobs()
        .logs_for_job_attempt(f.job, Some(f.attempt))
        .await?;
    assert_eq!(pinned.stdout_tail, Some(id));
    assert_eq!(
        f.database
            .storage_status(None)
            .await?
            .failed_log_retirements[0]
            .original_identity_bytes,
        bytes.len() as u64
    );
    Ok(())
}

#[tokio::test]
async fn failed_full_logs_legacy_keep_and_missing_without_intent_preserved() -> TestResult {
    for policy in [json!(null), json!("keep")] {
        let f = FailedLogs::new(policy, b"stdout", b"stderr", 1).await?;
        f.diagnose().await?;
        assert_eq!(f.retire().await?, 0);
        assert_eq!(fs::read(f.raw("stdout"))?, b"stdout");
        assert!(
            f.database
                .jobs()
                .retired_log_tail(f.attempt, "stdout")
                .await?
                .is_none()
        );
        let plan = f.database.gc_plan_filtered(None, &f.paths, true).await?;
        assert!(
            plan.entries
                .iter()
                .filter(|e| e.operation == GcOperation::FailedLogRetirement)
                .all(|e| e.status == GcStatus::Retain)
        );
    }
    let f = FailedLogs::new(json!("delete-after-diagnosis"), b"stdout", b"stderr", 1).await?;
    f.diagnose().await?;
    fs::remove_file(f.raw("stdout"))?;
    assert!(f.retire().await.is_err());
    assert!(
        f.database
            .jobs()
            .retired_log_tail(f.attempt, "stdout")
            .await?
            .is_none()
    );
    Ok(())
}

#[tokio::test]
async fn failed_full_logs_replay_and_external_path_preservation() -> TestResult {
    let mut f = FailedLogs::new(json!("delete-after-diagnosis"), b"stdout", b"stderr", 1).await?;
    f.diagnose().await?;
    let tail_before: Vec<u8> =
        sqlx::query_scalar("SELECT stdout FROM failed_log_tails WHERE attempt_id = ?")
            .bind(f.attempt.to_string())
            .fetch_one(f.database.pool())
            .await?;
    assert_eq!(tail_before, b"stdout");
    let tail_hash = format!("{:x}", sha2::Sha256::digest(&tail_before));
    sqlx::raw_sql("CREATE TRIGGER fail_retirement_event BEFORE INSERT ON events WHEN NEW.kind = 'cleanup_state_changed' BEGIN SELECT RAISE(ABORT,'crash boundary'); END;").execute(f.database.pool()).await?;
    assert!(f.retire().await.is_err());
    assert!(!f.raw("stdout").exists());
    assert_eq!(fs::read(f.raw("stderr"))?, b"stderr");
    assert!(
        f.database
            .jobs()
            .retired_log_tail(f.attempt, "stdout")
            .await?
            .is_none()
    );
    // The intent, its decision time and the original coordinates are durable
    // before the completion transaction the fault aborted.
    let captured: Vec<(String, String, String, String)> = sqlx::query_as(
        "SELECT stream, identity_json, state, decided_at FROM failed_log_retirements WHERE attempt_id = ? AND stream = 'stdout'",
    )
    .bind(f.attempt.to_string())
    .fetch_all(f.database.pool())
    .await?;
    assert_eq!(captured.len(), 1);
    assert_eq!(
        (captured[0].0.as_str(), captured[0].2.as_str()),
        ("stdout", "prepared")
    );
    let decided_at = captured[0].3.clone();
    assert!(!decided_at.is_empty());
    let intent: RetiredLogTail = serde_json::from_str(&captured[0].1)?;
    assert_eq!(intent.attempt_id, f.attempt);
    assert_eq!(intent.stream, "stdout");
    assert_eq!(intent.original_path, f.raw("stdout"));
    assert_eq!((intent.original_size, intent.start, intent.end), (6, 0, 6));
    assert_eq!(intent.tail_sha256, tail_hash);
    assert_eq!(
        intent.original_sha256,
        format!("{:x}", sha2::Sha256::digest(b"stdout"))
    );
    // The durable tail still carries the original coordinates and bytes.
    let tail_after: Vec<u8> =
        sqlx::query_scalar("SELECT stdout FROM failed_log_tails WHERE attempt_id = ?")
            .bind(f.attempt.to_string())
            .fetch_one(f.database.pool())
            .await?;
    assert_eq!(tail_after, tail_before);
    assert_eq!(
        &tail_after[intent.start as usize..intent.end as usize],
        b"stdout"
    );
    assert_eq!(
        format!("{:x}", sha2::Sha256::digest(&tail_after)),
        tail_hash
    );
    assert!(
        sqlx::query("UPDATE failed_log_tails SET stdout = X'00' WHERE attempt_id = ?")
            .bind(f.attempt.to_string())
            .execute(f.database.pool())
            .await
            .is_err()
    );
    let events_after_fault: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM events WHERE kind = 'cleanup_state_changed'")
            .fetch_one(f.database.pool())
            .await?;
    assert_eq!(events_after_fault, 0);
    let sql_before = gc_support::sql_snapshot(&f.database).await?;
    let disk_before = gc_support::files(f._directory.path(), &f.paths.database)?;
    f.database.pool().close().await;
    f.database = Database::open(&f.paths.database).await?;
    assert_eq!(gc_support::sql_snapshot(&f.database).await?, sql_before);
    assert_eq!(
        gc_support::files(f._directory.path(), &f.paths.database)?,
        disk_before
    );
    let reopened: Vec<(String, String, String, String)> = sqlx::query_as(
        "SELECT stream, identity_json, state, decided_at FROM failed_log_retirements WHERE attempt_id = ? AND stream = 'stdout'",
    )
    .bind(f.attempt.to_string())
    .fetch_all(f.database.pool())
    .await?;
    assert_eq!(reopened, captured);
    let external = TempDir::new()?;
    fs::write(external.path().join("sentinel"), b"preserve")?;
    let dir = f.raw("stdout").parent().ok_or("parent")?.to_owned();
    fs::rename(&dir, dir.with_extension("saved"))?;
    std::os::unix::fs::symlink(external.path(), &dir)?;
    assert!(f.retire().await.is_err());
    assert_eq!(fs::read(external.path().join("sentinel"))?, b"preserve");
    fs::remove_file(&dir)?;
    fs::rename(dir.with_extension("saved"), &dir)?;
    sqlx::query("DROP TRIGGER fail_retirement_event")
        .execute(f.database.pool())
        .await?;
    assert_eq!(f.retire().await?, 2);
    // Exactly one event and one tombstone per stream, on the original
    // coordinates, binary tail and decision time.
    for (stream, expected) in [
        ("stdout", b"stdout".as_slice()),
        ("stderr", b"stderr".as_slice()),
    ] {
        let subject = format!("{}:{stream}", f.attempt);
        let events: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM events WHERE kind = 'cleanup_state_changed' AND json_extract(payload_json, '$.data.stream') = ?",
        )
        .bind(stream)
        .fetch_one(f.database.pool())
        .await?;
        assert_eq!(events, 1, "{stream}");
        let tombstones: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM cleanup_tombstones WHERE operation = 'failed_log_retirement' AND subject_id = ?",
        )
        .bind(&subject)
        .fetch_one(f.database.pool())
        .await?;
        assert_eq!(tombstones, 1, "{stream}");
        let state: (String, String) = sqlx::query_as(
            "SELECT state, decided_at FROM failed_log_retirements WHERE attempt_id = ? AND stream = ?",
        )
        .bind(f.attempt.to_string())
        .bind(stream)
        .fetch_one(f.database.pool())
        .await?;
        assert_eq!(state.0, "completed");
        assert!(!state.1.is_empty());
        if stream == "stdout" {
            // The timestamp captured before the fault survived the completion.
            assert_eq!(state.1, decided_at);
        }
        let tombstone = f
            .database
            .cleanup_tombstone("failed_log_retirement", &subject)
            .await?
            .ok_or("tombstone")?;
        assert_eq!(tombstone["decided_at"].as_str(), Some(state.1.as_str()));
        let tail = f
            .database
            .jobs()
            .retired_log_tail(f.attempt, stream)
            .await?
            .ok_or("tail")?;
        if stream == "stdout" {
            assert_eq!(tail, intent);
        }
        assert_eq!((tail.original_size, tail.start, tail.end), (6, 0, 6));
        assert_eq!(
            tail.tail_sha256,
            format!("{:x}", sha2::Sha256::digest(expected))
        );
        let chunk = f.database.jobs().retired_log_chunk(&tail, 0, 65536).await?;
        assert_eq!(chunk.bytes, expected);
        assert_eq!((chunk.start, chunk.end), (0, 6));
    }
    let sql_final = gc_support::sql_snapshot(&f.database).await?;
    let disk_final = gc_support::files(f._directory.path(), &f.paths.database)?;
    assert_eq!(f.retire().await?, 0);
    assert_eq!(gc_support::sql_snapshot(&f.database).await?, sql_final);
    assert_eq!(
        gc_support::files(f._directory.path(), &f.paths.database)?,
        disk_final
    );
    Ok(())
}

#[tokio::test]
async fn failed_full_logs_tail_suffix_and_live_consumers_block_retirement() -> TestResult {
    let f = FailedLogs::new(json!("delete-after-diagnosis"), b"stdout", b"stderr", 1).await?;
    f.diagnose().await?;
    fs::write(f.raw("stdout"), b"changed")?;
    assert!(f.retire().await.is_err());
    fs::write(f.raw("stdout"), b"stdout")?;
    sqlx::query("UPDATE recoveries SET state = 'applying'")
        .execute(f.database.pool())
        .await?;
    assert_eq!(f.retire().await?, 0);
    sqlx::query("UPDATE recoveries SET state = 'proposed'")
        .execute(f.database.pool())
        .await?;
    let id = uuid::Uuid::new_v4().to_string();
    sqlx::query("INSERT INTO actions(id,project_id,attempt_id,kind,state,spec_json,idempotency_key,available_at) SELECT ?,project_id,id,'extract_metrics','pending','{}',?,'2000-01-01' FROM attempts").bind(&id).bind(&id).execute(f.database.pool()).await?;
    assert_eq!(f.retire().await?, 0);
    sqlx::query("UPDATE actions SET state = 'failed'")
        .execute(f.database.pool())
        .await?;
    assert_eq!(f.retire().await?, 2);
    Ok(())
}

#[tokio::test]
async fn failed_full_logs_maximum_tail_is_read_in_single_bounded_binary_chunks() -> TestResult {
    let bytes: Vec<u8> = (0..=255).cycle().take(16 * 1024 * 1024 + 37).collect();
    let f = FailedLogs::new(json!("delete-after-diagnosis"), &bytes, b"stderr", 16384).await?;
    f.diagnose().await?;
    assert_eq!(f.retire().await?, 2);
    let id = f
        .database
        .jobs()
        .retired_log_tail(f.attempt, "stdout")
        .await?
        .ok_or("tail")?;
    assert_eq!(id.start, 37);
    let mut offset = id.start;
    while offset < id.end {
        let chunk = f
            .database
            .jobs()
            .retired_log_chunk(&id, offset, 65536)
            .await?;
        assert!(chunk.bytes.len() <= 65536);
        assert_eq!(chunk.bytes, &bytes[offset as usize..chunk.end as usize]);
        offset = chunk.end;
    }
    assert!(serde_json::to_vec(&f.database.jobs().logs_for_job(f.job).await?)?.len() < 4096);
    Ok(())
}

#[tokio::test]
async fn failed_full_logs_second_phase_revalidates_diagnosis_and_registration() -> TestResult {
    for change in [
        "UPDATE projects SET registered = 0;",
        "UPDATE recoveries SET state = 'diagnosing';",
    ] {
        let f = FailedLogs::new(json!("delete-after-diagnosis"), b"stdout", b"stderr", 1).await?;
        f.diagnose().await?;
        sqlx::raw_sql(&format!("CREATE TRIGGER change_retirement_barrier AFTER INSERT ON failed_log_retirements BEGIN {change} END;")).execute(f.database.pool()).await?;
        assert_eq!(f.retire().await?, 0);
        assert_eq!(fs::read(f.raw("stdout"))?, b"stdout");
        assert!(
            f.database
                .jobs()
                .retired_log_tail(f.attempt, "stdout")
                .await?
                .is_none()
        );
        sqlx::raw_sql("DROP TRIGGER change_retirement_barrier; UPDATE projects SET registered = 1; UPDATE recoveries SET state = 'proposed';").execute(f.database.pool()).await?;
        assert_eq!(f.retire().await?, 2);
    }
    Ok(())
}

#[tokio::test]
async fn failed_full_logs_linked_raw_and_changed_consent_are_preserved() -> TestResult {
    for hardlink in [true, false] {
        let f = FailedLogs::new(json!("delete-after-diagnosis"), b"stdout", b"stderr", 1).await?;
        f.diagnose().await?;
        let external = TempDir::new()?;
        let sentinel = external.path().join("sentinel");
        fs::write(&sentinel, b"stdout")?;
        fs::remove_file(f.raw("stdout"))?;
        if hardlink {
            fs::hard_link(&sentinel, f.raw("stdout"))?;
        } else {
            std::os::unix::fs::symlink(&sentinel, f.raw("stdout"))?;
        }
        assert!(f.retire().await.is_err());
        assert_eq!(fs::read(&sentinel)?, b"stdout");
    }
    let f = FailedLogs::new(
        json!("delete-after-diagnosis"),
        &vec![b'x'; 2048],
        b"stderr",
        1,
    )
    .await?;
    f.diagnose().await?;
    let work = f
        .database
        .gc_plan_filtered(None, &f.paths, true)
        .await?
        .worklist()?;
    let mut changed = vec![b'x'; 2048];
    changed[0] = b'y';
    fs::write(f.raw("stdout"), changed)?;
    let result = f.database.gc_execute(None, true, work, &f.paths).await?;
    assert_eq!(result.deferred, 1);
    assert_eq!(result.completed, 1);
    assert!(f.raw("stdout").is_file());
    Ok(())
}

#[tokio::test]
async fn failed_full_logs_live_process_group_preserves_complete_tail() -> TestResult {
    use std::os::unix::process::CommandExt;
    let f = FailedLogs::new(json!("delete-after-diagnosis"), b"stdout", b"stderr", 1).await?;
    f.diagnose().await?;
    let group = LiveGroup(
        std::process::Command::new("/bin/sleep")
            .arg("60")
            .process_group(0)
            .spawn()?,
    );
    sqlx::query("UPDATE attempt_processes SET process_group_id = ?")
        .bind(i64::from(group.0.id()))
        .execute(f.database.pool())
        .await?;
    assert_eq!(f.retire().await?, 0);
    assert_eq!(fs::read(f.raw("stdout"))?, b"stdout");
    drop(group);
    assert_eq!(f.retire().await?, 2);
    Ok(())
}

#[tokio::test]
async fn failed_full_logs_zero_artifact_supersession_requires_current_comparable_generation()
-> TestResult {
    let mut f = FailedLogs::new(json!("delete-after-diagnosis"), b"stdout", b"stderr", 1).await?;
    let family = family_fixture().await?;
    family.database.families().submit(&family.prepared).await?;
    f.database = family.database.clone();
    let member = &family.prepared.submissions[0];
    f.attempt = member.attempt.id();
    f.job = member.job.id;
    let dir = f.paths.log_dir.join(f.attempt.to_string());
    fs::create_dir_all(&dir)?;
    fs::write(f.raw("stdout"), b"stdout")?;
    fs::write(f.raw("stderr"), b"stderr")?;
    sqlx::query("INSERT INTO attempt_processes(attempt_id,job_id,project_id,pid,process_group_id,process_start_ticks,stdout_path,stderr_path) VALUES (?,?,?,2147483647,2147483647,1,?,?)").bind(f.attempt.to_string()).bind(f.job.to_string()).bind(family.prepared.family.project_id.to_string()).bind(f.raw("stdout").to_str()).bind(f.raw("stderr").to_str()).execute(f.database.pool()).await?;
    sqlx::raw_sql("UPDATE attempts SET state = 'failed'; UPDATE jobs SET state = 'failed';")
        .execute(f.database.pool())
        .await?;
    sqlx::query("INSERT INTO failed_log_tails(attempt_id,stdout,stderr,limit_kb,complete) VALUES (?,X'7374646f7574',X'737464657272',512,1)").bind(f.attempt.to_string()).execute(f.database.pool()).await?;
    f.diagnose().await?;
    let successor = successor_for(&family).await?;
    f.database
        .families()
        .supersede(family.prepared.generation.identity.id, &successor)
        .await?;
    assert_eq!(f.retire().await?, 0);
    for submission in &successor.submissions {
        sqlx::query("UPDATE jobs SET state = 'succeeded' WHERE id = ?")
            .bind(submission.job.id.to_string())
            .execute(f.database.pool())
            .await?;
    }
    assert_eq!(f.retire().await?, 2);
    Ok(())
}
