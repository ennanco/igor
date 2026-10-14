use super::*;
use igor_core::{GcOperation, GcStatus, RuntimePaths};

#[tokio::test]
async fn owned_outputs_acceptance_saved_readonly_selection_matches_exact_fs_and_evidence()
-> TestResult {
    use igor_core::{GcEffect, GcExecutionStatus};

    let f = Owned::new(true, false).await?;
    f.diagnose().await?;
    fs::write(f.root.join("second.bin"), b"second output")?;
    let external = f._directory.path().join("external.bin");
    fs::write(&external, b"external bytes")?;
    std::os::unix::fs::symlink(&external, f.root.join("unsafe-link"))?;
    fs::create_dir(f.root.join("nested"))?;
    fs::write(f.root.join("nested/disposable.bin"), b"not an owned leaf")?;

    // Literal fixture paths/lengths are the oracle, not production preflight.
    let expected = vec![
        (f.root.join("disposable.bin"), 7),
        (f.root.join("second.bin"), 13),
    ];
    let before_sql = gc_support::sql_snapshot(&f.db).await?;
    let before_files = gc_support::files(f._directory.path(), &f.paths.database)?;
    let plan = gc_support::readonly_plan(&f.db, &f.paths, f._directory.path(), None).await?;
    assert_eq!(gc_support::sql_snapshot(&f.db).await?, before_sql);
    assert_eq!(
        gc_support::files(f._directory.path(), &f.paths.database)?,
        before_files
    );
    let owned: Vec<_> = plan
        .entries
        .iter()
        .filter(|e| e.operation == GcOperation::OwnedFailedOutputs)
        .collect();
    assert_eq!(owned.len(), 3);
    assert_eq!(
        owned
            .iter()
            .filter(|e| e.status == GcStatus::Unsafe)
            .count(),
        1
    );
    assert!(
        owned
            .iter()
            .filter(|e| e.status == GcStatus::Unsafe)
            .all(|e| { e.effect == GcEffect::Retain && e.targets.is_empty() })
    );
    // Frozen keep/control/directory exclusions currently have no individual
    // Retain entry; the full filesystem comparison below still protects them.
    let mut removals: Vec<_> = plan
        .entries
        .iter()
        .flat_map(|e| &e.targets)
        .filter(|t| t.effect == GcEffect::Remove)
        .map(|t| (t.path.clone(), t.bytes.ok_or("unknown removal length")))
        .map(|(path, bytes)| bytes.map(|bytes| (path, bytes)))
        .collect::<Result<_, _>>()?;
    removals.sort();
    assert_eq!(removals, expected);
    let mut work = plan.worklist()?;
    work.selections
        .retain(|s| s.expected.operation == GcOperation::OwnedFailedOutputs);
    assert_eq!(work.selections.len(), 2);
    assert!(work.selections.iter().all(|s| {
        s.expected.status == GcStatus::Ready
            && s.expected.effect == GcEffect::Remove
            && s.expected.targets.len() == 1
            && expected.contains(&(
                s.expected.targets[0].path.clone(),
                s.expected.targets[0].bytes.unwrap_or(u64::MAX),
            ))
    }));
    let selected: BTreeSet<_> = work
        .selections
        .iter()
        .map(|s| s.expected.subject_id.clone())
        .collect();
    // Execute exactly the saved selections without any intervening mutation.
    let result = f.db.gc_execute(None, true, work.clone(), &f.paths).await?;
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
    assert!(result.entries.iter().all(|e| {
        e.operation == GcOperation::OwnedFailedOutputs
            && e.status == GcExecutionStatus::Completed
            && e.cause.is_none()
    }));
    assert_eq!(
        result
            .entries
            .iter()
            .map(|e| e.subject_id.clone())
            .collect::<BTreeSet<_>>(),
        selected
    );
    let expected_files: Vec<_> = before_files
        .into_iter()
        .filter(|(path, _, _)| !expected.iter().any(|(removed, _)| path == removed))
        .collect();
    assert_eq!(
        gc_support::files(f._directory.path(), &f.paths.database)?,
        expected_files
    );
    assert_eq!(fs::read(f.root.join("keep.bin"))?, b"retained");
    assert_eq!(fs::read(&external)?, b"external bytes");

    let manifest: Vec<(String, String, String, i64)> = sqlx::query_as(
        "SELECT id, state, json_extract(identity_json, '$.name'), json_extract(identity_json, '$.size') FROM owned_output_manifest ORDER BY id",
    ).fetch_all(f.db.pool()).await?;
    assert_eq!(manifest.len(), 2);
    assert_eq!(
        manifest
            .iter()
            .map(|r| r.0.clone())
            .collect::<BTreeSet<_>>(),
        selected
    );
    for (id, state, name, size) in &manifest {
        assert_eq!(state, "completed");
        assert!(expected.contains(&(f.root.join(name), u64::try_from(*size)?)));
        let tombstone =
            f.db.cleanup_tombstone("failed_owned_output_retention", id)
                .await?
                .ok_or("selected tombstone")?;
        assert_eq!(tombstone["outcome"], "removed");
        assert_eq!(tombstone["identity"]["name"], *name);
        assert_eq!(tombstone["identity"]["size"], *size);
    }
    let tombstones: Vec<String> = sqlx::query_scalar("SELECT subject_id FROM cleanup_tombstones")
        .fetch_all(f.db.pool())
        .await?;
    assert_eq!(tombstones.len(), 2);
    assert_eq!(tombstones.into_iter().collect::<BTreeSet<_>>(), selected);
    let events: Vec<String> = sqlx::query_scalar(
        "SELECT json_extract(payload_json, '$.data.subject_id') FROM events WHERE kind = 'cleanup_state_changed'",
    ).fetch_all(f.db.pool()).await?;
    assert_eq!(events.len(), 2);
    assert_eq!(events.into_iter().collect::<BTreeSet<_>>(), selected);
    let completed_sql = gc_support::sql_snapshot(&f.db).await?;
    let replay = f.db.gc_execute(None, true, work, &f.paths).await?;
    assert_eq!(replay.already_done, 2, "{replay:?}");
    assert_eq!(replay.completed, 0);
    assert_eq!(gc_support::sql_snapshot(&f.db).await?, completed_sql);
    assert_eq!(
        gc_support::files(f._directory.path(), &f.paths.database)?,
        expected_files
    );
    Ok(())
}

#[tokio::test]
async fn owned_outputs_acceptance_metadata_only_discovery_changes_no_bytes() -> TestResult {
    use igor_core::{GcEffect, GcExecutionStatus};

    let f = Owned::new(true, false).await?;
    f.diagnose().await?;
    fs::remove_file(f.root.join("disposable.bin"))?;
    let external = f._directory.path().join("external.bin");
    fs::write(&external, b"external bytes")?;
    let before_sql = gc_support::sql_snapshot(&f.db).await?;
    let before_files = gc_support::files(f._directory.path(), &f.paths.database)?;
    let plan = gc_support::readonly_plan(&f.db, &f.paths, f._directory.path(), None).await?;
    assert_eq!(gc_support::sql_snapshot(&f.db).await?, before_sql);
    let mut work = plan.worklist()?;
    work.selections
        .retain(|s| s.expected.operation == GcOperation::OwnedFailedOutputs);
    assert_eq!(work.selections.len(), 1);
    let entry = &work.selections[0].expected;
    assert_eq!(entry.subject_id, format!("{}:discovery", f.attempt));
    assert_eq!(entry.status, GcStatus::MetadataOnly);
    assert_eq!(entry.effect, GcEffect::ResumeMetadata);
    assert!(entry.targets.is_empty());
    assert_eq!(
        entry.consent_identity["sha256"]
            .as_str()
            .ok_or("consent hash")?
            .len(),
        64
    );
    let result = f.db.gc_execute(None, true, work.clone(), &f.paths).await?;
    assert_eq!(result.completed, 1, "{result:?}");
    assert_eq!(
        (
            result.already_done,
            result.deferred,
            result.unsafe_count,
            result.errors
        ),
        (0, 0, 0, 0)
    );
    assert_eq!(result.entries.len(), 1);
    assert_eq!(
        result.entries[0].subject_id,
        format!("{}:discovery", f.attempt)
    );
    assert_eq!(result.entries[0].status, GcExecutionStatus::Completed);
    assert!(result.entries[0].cause.is_none());
    assert_eq!(
        gc_support::files(f._directory.path(), &f.paths.database)?,
        before_files
    );
    let discovery: Vec<(String, i64, String)> =
        sqlx::query_as("SELECT attempt_id, complete, identity_json FROM owned_output_discovery")
            .fetch_all(f.db.pool())
            .await?;
    assert_eq!(discovery.len(), 1);
    assert_eq!(discovery[0].0, f.attempt.to_string());
    assert_eq!(discovery[0].1, 1);
    let identity: serde_json::Value = serde_json::from_str(&discovery[0].2)?;
    assert_eq!(
        identity["namespace"]["root"],
        f.root.to_str().ok_or("root")?
    );
    assert_eq!(identity["namespace"]["attempt_id"], f.attempt.to_string());
    assert_eq!(identity["namespace"]["created_empty"], true);
    assert_eq!(
        identity["children_sha256"]
            .as_str()
            .ok_or("children hash")?
            .len(),
        64
    );
    // Discovery has no per-file intent, tombstone or cleanup event today.
    let after_sql = gc_support::sql_snapshot(&f.db).await?;
    for (table, rows) in &before_sql {
        if table != "owned_output_discovery" {
            assert_eq!(
                &after_sql
                    .iter()
                    .find(|(name, _)| name == table)
                    .ok_or("table")?
                    .1,
                rows,
                "unexpected SQL change in {table}"
            );
        }
    }
    assert_eq!(
        f.retire().await?,
        0,
        "metadata completion is not a removed-file count"
    );
    let next = gc_support::readonly_plan(&f.db, &f.paths, f._directory.path(), None).await?;
    assert!(
        !next
            .worklist()?
            .selections
            .iter()
            .any(|s| s.expected.operation == GcOperation::OwnedFailedOutputs)
    );
    let replay = f.db.gc_execute(None, true, work, &f.paths).await?;
    assert_eq!(replay.already_done, 1);
    assert_eq!(replay.completed, 0);
    assert_eq!(gc_support::sql_snapshot(&f.db).await?, after_sql);
    assert_eq!(
        gc_support::files(f._directory.path(), &f.paths.database)?,
        before_files
    );
    Ok(())
}

struct Owned {
    _directory: TempDir,
    db: Database,
    paths: RuntimePaths,
    attempt: AttemptId,
    job: JobId,
    root: PathBuf,
}
impl Owned {
    async fn new(policy: bool, preexisting: bool) -> Result<Self, Box<dyn Error>> {
        Self::with_retention(policy, preexisting, "delete-after-diagnosis").await
    }
    async fn with_retention(
        policy: bool,
        preexisting: bool,
        retention: &str,
    ) -> Result<Self, Box<dyn Error>> {
        let (directory, db) = database().await?;
        let (project, job) = insert_project_job(&db).await?;
        let base = directory.path();
        let paths = RuntimePaths {
            config_file: base.join("config"),
            state_dir: base.join("state"),
            runtime_dir: base.join("runtime"),
            database: base.join("nested/igor.db"),
            log_dir: base.join("logs"),
            worktree_dir: base.join("worktrees"),
            report_dir: base.join("reports"),
            worker_socket: base.join("worker.sock"),
            supervisor_socket: base.join("supervisor.sock"),
            resource_dir: base.join("resources"),
        };
        let mut value = serde_json::to_value(attempt(&job, 1)?)?;
        value["result"]["owned_namespace"] = if policy { json!(true) } else { json!(null) };
        value["result"]["failed_artifact_retention"] =
            json!({"policy":retention,"keep_on_failure":["keep*"]});
        let spec: AttemptSpec = serde_json::from_value(value)?;
        let attempt = spec.id();
        db.jobs()
            .insert_attempt_with_event(&spec, &event(EventKind::AttemptCreated, json!({}))?)
            .await?;
        let root = paths.log_dir.join(attempt.to_string());
        if preexisting {
            fs::create_dir_all(&root)?;
            fs::write(root.join("preexisting.bin"), b"preserve")?;
        }
        let (stdout, stderr) = db.jobs().prepare_attempt_logs(attempt, &paths).await?;
        assert_eq!(
            sqlx::query_scalar::<_, i64>(
                "SELECT COUNT(*) FROM owned_output_namespaces WHERE attempt_id = ?"
            )
            .bind(attempt.to_string())
            .fetch_one(db.pool())
            .await?,
            i64::from(policy && !preexisting),
            "ownership must be durable before writing or launching"
        );
        fs::write(&stdout, b"out")?;
        fs::write(&stderr, b"err")?;
        sqlx::query("INSERT INTO attempt_processes(attempt_id,job_id,project_id,pid,process_group_id,process_start_ticks,stdout_path,stderr_path) VALUES (?,?,?,2147483647,2147483647,1,?,?)").bind(attempt.to_string()).bind(job.id.to_string()).bind(project.id.to_string()).bind(stdout.to_str()).bind(stderr.to_str()).execute(db.pool()).await?;
        sqlx::query("UPDATE attempts SET state = 'failed'")
            .execute(db.pool())
            .await?;
        sqlx::query("UPDATE jobs SET state = 'failed'")
            .execute(db.pool())
            .await?;
        sqlx::query("INSERT INTO failed_log_tails(attempt_id,stdout,stderr,limit_kb,complete) VALUES (?,X'6f7574',X'657272',512,1)").bind(attempt.to_string()).execute(db.pool()).await?;
        fs::write(root.join("disposable.bin"), b"garbage")?;
        fs::write(root.join("keep.bin"), b"retained")?;
        Ok(Self {
            _directory: directory,
            db,
            paths,
            attempt,
            job: job.id,
            root,
        })
    }
    async fn diagnose(&self) -> TestResult {
        let id = uuid::Uuid::new_v4().to_string();
        sqlx::query("INSERT INTO recoveries(id,attempt_id,state,diagnosis_json,idempotency_key) VALUES (?,?,'proposed','{}',?)").bind(&id).bind(self.attempt.to_string()).bind(&id).execute(self.db.pool()).await?;
        Ok(())
    }
    async fn retire(&self) -> Result<usize, PersistenceError> {
        self.db
            .jobs()
            .retire_owned_failed_outputs(self.attempt, &self.paths)
            .await
    }
    async fn work(&self) -> Result<igor_core::GcWorklist, Box<dyn Error>> {
        let mut work = self
            .db
            .gc_plan_filtered(None, &self.paths, true)
            .await?
            .worklist()?;
        work.selections
            .retain(|s| s.expected.operation == GcOperation::OwnedFailedOutputs);
        Ok(work)
    }
}

#[tokio::test]
async fn owned_outputs_diagnosis_tails_consumers_keep_and_read_only_inventory() -> TestResult {
    let f = Owned::new(true, false).await?;
    assert_eq!(f.retire().await?, 0);
    f.diagnose().await?;
    sqlx::query("UPDATE failed_log_tails SET complete = 0")
        .execute(f.db.pool())
        .await?;
    assert_eq!(f.retire().await?, 0);
    sqlx::query("UPDATE failed_log_tails SET complete = 1")
        .execute(f.db.pool())
        .await?;
    let action = uuid::Uuid::new_v4().to_string();
    sqlx::query("INSERT INTO actions(id,project_id,job_id,attempt_id,kind,state,spec_json,idempotency_key,available_at) SELECT ?,project_id,job_id,id,'extract_metrics','pending','{}',?,strftime('%Y-%m-%dT%H:%M:%fZ','now') FROM attempts WHERE id = ?").bind(&action).bind(&action).bind(f.attempt.to_string()).execute(f.db.pool()).await?;
    assert_eq!(f.retire().await?, 0);
    sqlx::query("UPDATE actions SET state = 'succeeded' WHERE id = ?")
        .bind(action)
        .execute(f.db.pool())
        .await?;
    let work = f.work().await?;
    assert_eq!(work.selections.len(), 1);
    assert!(
        f.db.storage_status(None)
            .await?
            .owned_failed_outputs
            .is_empty()
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM owned_output_manifest")
            .fetch_one(f.db.pool())
            .await?,
        0
    );
    fs::write(f.root.join("new-after-consent.bin"), b"new")?;
    let done = f.db.gc_execute(None, true, work.clone(), &f.paths).await?;
    assert_eq!(done.completed, 1, "{done:?}");
    assert!(!f.root.join("disposable.bin").exists());
    assert_eq!(fs::read(f.root.join("new-after-consent.bin"))?, b"new");
    assert_eq!(fs::read(f.root.join("keep.bin"))?, b"retained");
    assert!(f.root.join("stdout.log").exists() && f.root.join("stderr.log").exists());
    let repeated = f.db.gc_execute(None, true, work, &f.paths).await?;
    assert_eq!(repeated.already_done, 1);
    let storage = f.db.storage_status(None).await?;
    assert_eq!(storage.owned_failed_outputs[0].manifest_identity_bytes, 7);
    assert_eq!(storage.owned_failed_outputs[0].actual_file_bytes, None);
    assert!(
        storage
            .tombstones
            .iter()
            .any(|t| t.operation == "failed_owned_output_retention" && t.count == 1)
    );
    Ok(())
}

#[tokio::test]
async fn owned_outputs_legacy_retry_and_preexisting_directories_are_not_adopted() -> TestResult {
    for (policy, preexisting) in [(false, false), (true, true)] {
        let f = Owned::new(policy, preexisting).await?;
        f.diagnose().await?;
        assert_eq!(f.retire().await?, 0);
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM owned_output_namespaces")
                .fetch_one(f.db.pool())
                .await?,
            0
        );
        assert!(f.root.join("disposable.bin").exists());
        if !policy {
            let retry = f.db.jobs().retry(f.job).await?;
            let spec = &retry.attempts.last().ok_or("retry")?.spec;
            assert_eq!(spec.result().owned_namespace, None);
            f.db.jobs()
                .prepare_attempt_logs(spec.id(), &f.paths)
                .await?;
            assert_eq!(
                sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM owned_output_namespaces")
                    .fetch_one(f.db.pool())
                    .await?,
                0
            );
        }
    }
    Ok(())
}

#[tokio::test]
async fn owned_outputs_changed_consent_and_root_inode_preserve_bytes() -> TestResult {
    let f = Owned::new(true, false).await?;
    f.diagnose().await?;
    let work = f.work().await?;
    fs::write(f.root.join("disposable.bin"), b"changed")?;
    let done = f.db.gc_execute(None, true, work, &f.paths).await?;
    assert_eq!(done.completed, 0);
    assert_eq!(fs::read(f.root.join("disposable.bin"))?, b"changed");
    let saved = f.root.with_extension("original");
    fs::rename(&f.root, &saved)?;
    fs::create_dir(&f.root)?;
    fs::write(f.root.join("disposable.bin"), b"substituted")?;
    assert_eq!(f.retire().await?, 0);
    let plan = f.db.gc_plan_filtered(None, &f.paths, true).await?;
    assert!(plan.entries.iter().any(|e|e.operation==GcOperation::OwnedFailedOutputs && e.status==GcStatus::Unsafe));
    assert_eq!(fs::read(f.root.join("disposable.bin"))?, b"substituted");
    assert_eq!(fs::read(saved.join("disposable.bin"))?, b"changed");
    Ok(())
}

#[tokio::test]
async fn owned_outputs_symlink_fifo_hardlink_and_registered_sources_are_preserved() -> TestResult {
    let f = Owned::new(true, false).await?;
    f.diagnose().await?;
    let outside = f._directory.path().join("outside");
    fs::write(&outside, b"outside")?;
    std::os::unix::fs::symlink(&outside, f.root.join("link"))?;
    fs::hard_link(&outside, f.root.join("hard"))?;
    nix::unistd::mkfifo(
        &f.root.join("fifo"),
        nix::sys::stat::Mode::S_IRUSR | nix::sys::stat::Mode::S_IWUSR,
    )?;
    let artifact = uuid::Uuid::new_v4();
    sqlx::query("INSERT INTO artifacts(id,attempt_id,path,role,state,lifecycle_state,diagnosis_required,size_bytes,sha256,metadata_json) VALUES (?,?,'disposable.bin','metrics','registered','failed',1,7,?,'{}')").bind(artifact.to_string()).bind(f.attempt.to_string()).bind(format!("{:x}",sha2::Sha256::digest(b"garbage"))).execute(f.db.pool()).await?;
    let plan = tokio::time::timeout(
        Duration::from_secs(5),
        f.db.gc_plan_filtered(None, &f.paths, true),
    )
    .await??;
    assert_eq!(
        plan.entries
            .iter()
            .filter(
                |e| e.operation == GcOperation::OwnedFailedOutputs && e.status == GcStatus::Unsafe
            )
            .count(),
        3
    );
    assert_eq!(f.retire().await?, 0);
    assert_eq!(fs::read(&outside)?, b"outside");
    assert!(f.root.join("disposable.bin").exists());
    assert!(fs::symlink_metadata(f.root.join("fifo")).is_ok());
    Ok(())
}

#[tokio::test]
async fn owned_outputs_prepared_unlink_replay_has_one_event_and_tombstone() -> TestResult {
    let f = Owned::new(true, false).await?;
    f.diagnose().await?;
    sqlx::raw_sql("CREATE TRIGGER owned_event_crash BEFORE INSERT ON events WHEN NEW.kind = 'cleanup_state_changed' BEGIN SELECT RAISE(ABORT,'crash'); END;").execute(f.db.pool()).await?;
    assert!(f.retire().await.is_err());
    assert!(!f.root.join("disposable.bin").exists());
    assert_eq!(
        sqlx::query_scalar::<_, String>("SELECT state FROM owned_output_manifest")
            .fetch_one(f.db.pool())
            .await?,
        "prepared"
    );
    // Freeze the durable prepared identity (decision, namespace root/inode and
    // consent hash) plus the saved selection before the pool restarts.
    let prepared: (String, String, Option<String>, String) =
        sqlx::query_as("SELECT id, identity_json, decided_at, state FROM owned_output_manifest")
            .fetch_one(f.db.pool())
            .await?;
    let saved = f.work().await?;
    let mut saved_subjects: Vec<String> = saved
        .selections
        .iter()
        .map(|s| s.expected.subject_id.clone())
        .collect();
    saved_subjects.sort();
    assert!(!saved_subjects.is_empty());
    // The saved worklist may only ever target the single owned leaf.
    for target in saved
        .selections
        .iter()
        .flat_map(|s| s.expected.targets.iter())
        .map(|t| t.path.clone())
    {
        assert_eq!(target, f.root.join("disposable.bin"));
    }
    let before_sql = gc_support::sql_snapshot(&f.db).await?;
    let before_files = gc_support::files(f._directory.path(), &f.paths.database)?;

    // Restart the pool at the fault seam: SQL, filesystem and prepared
    // identity must survive, and the plan must select no new leaf or recovery.
    f.db.pool().close().await;
    let restarted = Database::open(&f.paths.database).await?;
    assert_eq!(gc_support::sql_snapshot(&restarted).await?, before_sql);
    assert_eq!(
        gc_support::files(f._directory.path(), &f.paths.database)?,
        before_files
    );
    let reopened: (String, String, Option<String>, String) =
        sqlx::query_as("SELECT id, identity_json, decided_at, state FROM owned_output_manifest")
            .fetch_one(restarted.pool())
            .await?;
    assert_eq!(reopened, prepared);
    let mut fresh = restarted
        .gc_plan_filtered(None, &f.paths, true)
        .await?
        .worklist()?;
    fresh
        .selections
        .retain(|s| s.expected.operation == GcOperation::OwnedFailedOutputs);
    let mut fresh_subjects: Vec<String> = fresh
        .selections
        .iter()
        .map(|s| s.expected.subject_id.clone())
        .collect();
    fresh_subjects.sort();
    assert_eq!(fresh_subjects, saved_subjects);
    assert_eq!(fs::read(f.root.join("keep.bin"))?, b"retained");
    assert!(f.root.join("stdout.log").is_file() && f.root.join("stderr.log").is_file());

    sqlx::query("DROP TRIGGER owned_event_crash")
        .execute(restarted.pool())
        .await?;
    assert_eq!(
        restarted
            .jobs()
            .retire_owned_failed_outputs(f.attempt, &f.paths)
            .await?,
        1
    );
    assert_eq!(
        restarted
            .jobs()
            .retire_owned_failed_outputs(f.attempt, &f.paths)
            .await?,
        0
    );
    assert_eq!(sqlx::query_scalar::<_,i64>("SELECT COUNT(*) FROM cleanup_tombstones WHERE operation = 'failed_owned_output_retention'").fetch_one(restarted.pool()).await?,1);
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM events WHERE kind = 'cleanup_state_changed'"
        )
        .fetch_one(restarted.pool())
        .await?,
        1
    );
    // Completion freezes the same prepared identity without a second intent.
    let completed: (String, String, String) =
        sqlx::query_as("SELECT id, identity_json, state FROM owned_output_manifest")
            .fetch_one(restarted.pool())
            .await?;
    assert_eq!(completed.0, prepared.0);
    assert_eq!(completed.1, prepared.1);
    assert_eq!(completed.2, "completed");
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM owned_output_manifest")
            .fetch_one(restarted.pool())
            .await?,
        1
    );

    // Replaying the saved worklist is stable for SQL and the filesystem.
    let completed_sql = gc_support::sql_snapshot(&restarted).await?;
    let completed_files = gc_support::files(f._directory.path(), &f.paths.database)?;
    assert_eq!(
        restarted
            .gc_execute(None, true, saved, &f.paths)
            .await?
            .already_done,
        1
    );
    assert_eq!(gc_support::sql_snapshot(&restarted).await?, completed_sql);
    assert_eq!(
        gc_support::files(f._directory.path(), &f.paths.database)?,
        completed_files
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM cleanup_tombstones WHERE operation = 'failed_owned_output_retention'"
        )
        .fetch_one(restarted.pool())
        .await?,
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM events WHERE kind = 'cleanup_state_changed'"
        )
        .fetch_one(restarted.pool())
        .await?,
        1
    );
    assert!(
        sqlx::query("UPDATE owned_output_manifest SET identity_json = identity_json")
            .execute(restarted.pool())
            .await
            .is_err()
    );
    assert!(
        sqlx::query("DELETE FROM owned_output_namespaces")
            .execute(restarted.pool())
            .await
            .is_err()
    );
    Ok(())
}

#[tokio::test]
async fn owned_outputs_metadata_namespace_closure_recovers_interrupted_insert() -> TestResult {
    use igor_core::{GcEffect, GcExecutionStatus};

    let f = Owned::new(true, false).await?;
    f.diagnose().await?;
    fs::remove_file(f.root.join("disposable.bin"))?;
    sqlx::raw_sql("CREATE TRIGGER owned_discovery_crash BEFORE INSERT ON owned_output_discovery BEGIN SELECT RAISE(ABORT,'crash'); END;")
        .execute(f.db.pool())
        .await?;
    let before_files = gc_support::files(f._directory.path(), &f.paths.database)?;
    let plan = gc_support::readonly_plan(&f.db, &f.paths, f._directory.path(), None).await?;
    let mut saved = plan.worklist()?;
    saved
        .selections
        .retain(|s| s.expected.operation == GcOperation::OwnedFailedOutputs);
    assert_eq!(saved.selections.len(), 1);
    assert_eq!(saved.selections[0].expected.status, GcStatus::MetadataOnly);
    assert_eq!(
        saved.selections[0].expected.effect,
        GcEffect::ResumeMetadata
    );
    let interrupted = f.db.gc_execute(None, true, saved.clone(), &f.paths).await?;
    assert_eq!(interrupted.errors, 1, "{interrupted:?}");
    assert_eq!(interrupted.completed, 0, "{interrupted:?}");
    assert_eq!(interrupted.entries.len(), 1, "{interrupted:?}");
    assert_eq!(interrupted.entries[0].status, GcExecutionStatus::Error);
    assert!(interrupted.entries[0].cause.is_some());
    // The interrupted insert is fully rolled back: no bytes, no discovery row.
    assert_eq!(
        gc_support::files(f._directory.path(), &f.paths.database)?,
        before_files
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM owned_output_discovery")
            .fetch_one(f.db.pool())
            .await?,
        0
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM owned_output_manifest")
            .fetch_one(f.db.pool())
            .await?,
        0
    );

    // Restarting the pool recovers the namespace closure: the same saved
    // worklist completes once the insert is no longer aborted.
    f.db.pool().close().await;
    let restarted = Database::open(&f.paths.database).await?;
    sqlx::query("DROP TRIGGER owned_discovery_crash")
        .execute(restarted.pool())
        .await?;
    let done = restarted.gc_execute(None, true, saved, &f.paths).await?;
    assert_eq!(done.completed, 1, "{done:?}");
    assert_eq!(done.entries.len(), 1);
    assert_eq!(done.entries[0].status, GcExecutionStatus::Completed);
    assert!(done.entries[0].cause.is_none());
    assert_eq!(
        gc_support::files(f._directory.path(), &f.paths.database)?,
        before_files
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM owned_output_discovery")
            .fetch_one(restarted.pool())
            .await?,
        1
    );
    // Metadata only: no output intent (manifest row) and no cleanup event.
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM owned_output_manifest")
            .fetch_one(restarted.pool())
            .await?,
        0
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM events WHERE kind = 'cleanup_state_changed'"
        )
        .fetch_one(restarted.pool())
        .await?,
        0
    );
    let mut next = gc_support::readonly_plan(&restarted, &f.paths, f._directory.path(), None)
        .await?
        .worklist()?;
    next.selections
        .retain(|s| s.expected.operation == GcOperation::OwnedFailedOutputs);
    assert!(next.selections.is_empty());
    Ok(())
}

#[tokio::test]
async fn owned_outputs_bounded_preview_reports_truncation() -> TestResult {
    let f = Owned::new(true, false).await?;
    f.diagnose().await?;
    for i in 0..100 {
        fs::write(f.root.join(format!("output-{i:03}")), b"x")?;
    }
    let plan = f.db.gc_plan_filtered(None, &f.paths, true).await?;
    assert!(
        plan.entries
            .iter()
            .filter(|e| e.operation == GcOperation::OwnedFailedOutputs)
            .count()
            <= 64
    );
    assert!(
        plan.pages
            .iter()
            .find(|p| p.operation == GcOperation::OwnedFailedOutputs)
            .ok_or("page")?
            .artifact_batches[0]
            .truncated
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM owned_output_manifest")
            .fetch_one(f.db.pool())
            .await?,
        0
    );
    Ok(())
}

#[tokio::test]
async fn owned_outputs_second_phase_revalidates_consumers_registration_and_bindings() -> TestResult
{
    for change in [
        "UPDATE projects SET registered = 0;",
        "UPDATE recoveries SET state = 'diagnosing';",
        "UPDATE failed_log_tails SET complete = 0;",
        "UPDATE attempt_processes SET stdout_path = '/tmp/not-owned.log';",
    ] {
        let f = Owned::new(true, false).await?;
        f.diagnose().await?;
        sqlx::raw_sql(&format!("CREATE TRIGGER owned_change_barrier AFTER INSERT ON owned_output_manifest BEGIN {change} END;")).execute(f.db.pool()).await?;
        let result = f.retire().await;
        assert!(result.is_err() || result? == 0);
        assert_eq!(fs::read(f.root.join("disposable.bin"))?, b"garbage");
        assert_eq!(
            sqlx::query_scalar::<_, i64>(
                "SELECT COUNT(*) FROM owned_output_manifest WHERE state = 'completed'"
            )
            .fetch_one(f.db.pool())
            .await?,
            0
        );
    }
    Ok(())
}

struct Writer(std::process::Child);
impl Drop for Writer {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

#[tokio::test]
async fn owned_outputs_live_writer_and_ancestor_symlinks_defer() -> TestResult {
    use std::os::unix::process::CommandExt;
    let f = Owned::new(true, false).await?;
    f.diagnose().await?;
    let writer = Writer(
        Command::new("/bin/sleep")
            .arg("60")
            .process_group(0)
            .spawn()?,
    );
    sqlx::query("UPDATE attempt_processes SET process_group_id = ?")
        .bind(i64::from(writer.0.id()))
        .execute(f.db.pool())
        .await?;
    assert_eq!(f.retire().await?, 0);
    drop(writer);
    let root = &f.paths.log_dir;
    let saved = root.with_extension("saved");
    fs::rename(root, &saved)?;
    std::os::unix::fs::symlink(&saved, root)?;
    assert_eq!(f.retire().await?, 0);
    assert!(f.root.join("disposable.bin").exists());
    fs::remove_file(root)?;
    fs::rename(saved, root)?;
    assert_eq!(f.retire().await?, 1);
    Ok(())
}

#[tokio::test]
async fn owned_outputs_supersession_requires_current_comparable_generation() -> TestResult {
    let mut f = Owned::new(true, false).await?;
    let family = family_fixture().await?;
    family.database.families().submit(&family.prepared).await?;
    f.db = family.database.clone();
    let member = &family.prepared.submissions[0];
    f.attempt = member.attempt.id();
    f.job = member.job.id;
    f.root = f.paths.log_dir.join(f.attempt.to_string());
    let (stdout, stderr) =
        f.db.jobs()
            .prepare_attempt_logs(f.attempt, &f.paths)
            .await?;
    fs::write(&stdout, b"out")?;
    fs::write(&stderr, b"err")?;
    fs::write(f.root.join("disposable.bin"), b"garbage")?;
    sqlx::query("INSERT INTO attempt_processes(attempt_id,job_id,project_id,pid,process_group_id,process_start_ticks,stdout_path,stderr_path) VALUES (?,?,?,2147483647,2147483647,1,?,?)").bind(f.attempt.to_string()).bind(f.job.to_string()).bind(family.prepared.family.project_id.to_string()).bind(stdout.to_str()).bind(stderr.to_str()).execute(f.db.pool()).await?;
    sqlx::raw_sql("UPDATE attempts SET state = 'failed'; UPDATE jobs SET state = 'failed';")
        .execute(f.db.pool())
        .await?;
    sqlx::query("INSERT INTO failed_log_tails(attempt_id,stdout,stderr,limit_kb,complete) VALUES (?,X'6f7574',X'657272',512,1)").bind(f.attempt.to_string()).execute(f.db.pool()).await?;
    f.diagnose().await?;
    let successor = successor_for(&family).await?;
    f.db.families()
        .supersede(family.prepared.generation.identity.id, &successor)
        .await?;
    assert_eq!(f.retire().await?, 0);
    for submission in &successor.submissions {
        sqlx::query("UPDATE jobs SET state = 'succeeded' WHERE id = ?")
            .bind(submission.job.id.to_string())
            .execute(f.db.pool())
            .await?;
    }
    assert_eq!(f.retire().await?, 1);
    Ok(())
}

#[tokio::test]
async fn owned_outputs_missing_without_intent_and_recreated_prepared_leaf_preserve() -> TestResult {
    let f = Owned::new(true, false).await?;
    f.diagnose().await?;
    let work = f.work().await?;
    fs::remove_file(f.root.join("disposable.bin"))?;
    let result = f.db.gc_execute(None, true, work, &f.paths).await?;
    assert_eq!(result.completed, 0);
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM owned_output_manifest")
            .fetch_one(f.db.pool())
            .await?,
        0
    );
    fs::write(f.root.join("disposable.bin"), b"garbage")?;
    sqlx::raw_sql("CREATE TRIGGER owned_event_crash BEFORE INSERT ON events WHEN NEW.kind = 'cleanup_state_changed' BEGIN SELECT RAISE(ABORT,'crash'); END;").execute(f.db.pool()).await?;
    assert!(f.retire().await.is_err());
    fs::write(f.root.join("disposable.bin"), b"new-owner")?;
    sqlx::query("DROP TRIGGER owned_event_crash")
        .execute(f.db.pool())
        .await?;
    assert_eq!(f.retire().await?, 0);
    assert_eq!(fs::read(f.root.join("disposable.bin"))?, b"new-owner");
    assert_eq!(sqlx::query_scalar::<_,i64>("SELECT COUNT(*) FROM cleanup_tombstones WHERE operation = 'failed_owned_output_retention'").fetch_one(f.db.pool()).await?,0);
    Ok(())
}

#[tokio::test]
async fn owned_outputs_frozen_keep_unknown_namespace_and_project_rebinding_preserve() -> TestResult
{
    let f = Owned::with_retention(true, false, "keep").await?;
    f.diagnose().await?;
    assert_eq!(f.retire().await?, 0);
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM owned_output_manifest")
            .fetch_one(f.db.pool())
            .await?,
        0
    );
    assert!(f.root.join("disposable.bin").exists());

    let f = Owned::new(true, false).await?;
    f.diagnose().await?;
    sqlx::query("UPDATE projects SET root_path = ?")
        .bind(f.root.to_str())
        .execute(f.db.pool())
        .await?;
    assert_eq!(f.retire().await?, 0);
    assert!(f.root.join("disposable.bin").exists());

    let (directory, db) = database().await?;
    let (_, job) = insert_project_job(&db).await?;
    let mut value = serde_json::to_value(attempt(&job, 1)?)?;
    value["result"]["owned_namespace"] = json!(true);
    let spec: AttemptSpec = serde_json::from_value(value)?;
    db.jobs()
        .insert_attempt_with_event(&spec, &event(EventKind::AttemptCreated, json!({}))?)
        .await?;
    let paths = RuntimePaths {
        config_file: directory.path().join("config"),
        state_dir: directory.path().join("state"),
        runtime_dir: directory.path().join("runtime"),
        database: directory.path().join("nested/igor.db"),
        log_dir: directory.path().join("logs"),
        worktree_dir: directory.path().join("worktrees"),
        report_dir: directory.path().join("reports"),
        worker_socket: directory.path().join("worker.sock"),
        supervisor_socket: directory.path().join("supervisor.sock"),
        resource_dir: directory.path().join("resources"),
    };
    fs::create_dir_all(paths.log_dir.join(spec.id().to_string()))?;
    db.jobs().prepare_attempt_logs(spec.id(), &paths).await?;
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM owned_output_namespaces")
            .fetch_one(db.pool())
            .await?,
        0,
        "even empty preexisting namespaces are not adopted"
    );
    Ok(())
}

#[tokio::test]
async fn owned_outputs_late_registration_protects_prepared_leaf() -> TestResult {
    let f = Owned::new(true, false).await?;
    f.diagnose().await?;
    let hash = format!("{:x}", sha2::Sha256::digest(b"garbage"));
    let id = uuid::Uuid::new_v4();
    sqlx::raw_sql(&format!("CREATE TRIGGER owned_register_late AFTER INSERT ON owned_output_manifest BEGIN INSERT INTO artifacts(id,attempt_id,path,role,state,lifecycle_state,diagnosis_required,size_bytes,sha256,metadata_json) VALUES ('{id}',NEW.attempt_id,'disposable.bin','metrics','registered','failed',1,7,'{hash}','{{}}'); END;")).execute(f.db.pool()).await?;
    assert_eq!(f.retire().await?, 0);
    assert_eq!(fs::read(f.root.join("disposable.bin"))?, b"garbage");
    assert_eq!(
        sqlx::query_scalar::<_, String>("SELECT state FROM owned_output_manifest")
            .fetch_one(f.db.pool())
            .await?,
        "prepared"
    );
    sqlx::query("DROP TRIGGER owned_register_late")
        .execute(f.db.pool())
        .await?;
    assert_eq!(f.retire().await?, 0);
    assert_eq!(sqlx::query_scalar::<_,i64>("SELECT COUNT(*) FROM cleanup_tombstones WHERE operation = 'failed_owned_output_retention'").fetch_one(f.db.pool()).await?,0);
    Ok(())
}

#[tokio::test]
async fn owned_outputs_protected_prefix_does_not_starve_bounded_pages() -> TestResult {
    let f = Owned::new(true, false).await?;
    f.diagnose().await?;
    for i in 0..100 {
        fs::write(f.root.join(format!("keep-{i:03}")), b"protected")?;
    }
    for i in 0..70 {
        fs::write(f.root.join(format!("z-disposable-{i:03}")), b"remove")?;
    }
    let work = f.work().await?;
    assert_eq!(work.selections.len(), 64);
    let result = f.db.gc_execute(None, true, work, &f.paths).await?;
    assert_eq!(result.completed, 64, "{result:?}");
    let work = f.work().await?;
    assert_eq!(work.selections.len(), 7);
    assert_eq!(
        f.db.gc_execute(None, true, work, &f.paths).await?.completed,
        7
    );
    let work = f.work().await?;
    assert_eq!(work.selections.len(), 1);
    assert_eq!(work.selections[0].expected.status, GcStatus::MetadataOnly);
    assert_eq!(
        f.db.gc_execute(None, true, work, &f.paths).await?.completed,
        1
    );
    assert!(f.work().await?.selections.is_empty());
    for i in 0..100 {
        assert_eq!(fs::read(f.root.join(format!("keep-{i:03}")))?, b"protected");
    }
    fs::write(f.root.join("future-output"), b"not authorized")?;
    assert_eq!(f.retire().await?, 0);
    assert!(f.root.join("future-output").exists());
    Ok(())
}

#[tokio::test]
async fn owned_outputs_discovery_consent_changed_overflow_and_unsafe_preserve() -> TestResult {
    let f = Owned::new(true, false).await?;
    f.diagnose().await?;
    fs::remove_file(f.root.join("disposable.bin"))?;
    let work = f.work().await?;
    assert_eq!(work.selections.len(), 1);
    fs::write(f.root.join("new-file"), b"new")?;
    let result = f.db.gc_execute(None, true, work, &f.paths).await?;
    assert_eq!(result.completed, 0);
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM owned_output_discovery")
            .fetch_one(f.db.pool())
            .await?,
        0
    );
    assert_eq!(fs::read(f.root.join("new-file"))?, b"new");
    fs::remove_file(f.root.join("new-file"))?;
    let work = f.work().await?;
    let saved = f.root.with_extension("original");
    fs::rename(&f.root, &saved)?;
    fs::create_dir(&f.root)?;
    assert_eq!(
        f.db.gc_execute(None, true, work, &f.paths).await?.completed,
        0
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM owned_output_discovery")
            .fetch_one(f.db.pool())
            .await?,
        0
    );
    fs::remove_dir(&f.root)?;
    fs::rename(saved, &f.root)?;
    for i in 0..4096 {
        fs::write(f.root.join(format!("keep-{i:04}")), b"x")?;
    }
    let plan = f.db.gc_plan_filtered(None, &f.paths, true).await?;
    assert!(
        plan.entries
            .iter()
            .any(|e| e.operation == GcOperation::OwnedFailedOutputs
                && e.reason == "namespace_children_bound"
                && e.status == GcStatus::Deferred)
    );
    assert!(f.work().await?.selections.is_empty());
    assert_eq!(f.retire().await?, 0);
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM owned_output_discovery")
            .fetch_one(f.db.pool())
            .await?,
        0
    );
    Ok(())
}

#[tokio::test]
async fn owned_outputs_worker_only_gc_reaches_33rd_namespace_after_draining_32() -> TestResult {
    let f = Owned::new(true, false).await?;
    f.diagnose().await?;
    let template = f.db.jobs().detail(f.job).await?.ok_or("job")?.job.spec;
    let mut attempts = vec![f.attempt];
    for _ in 0..32 {
        let mut job = template.clone();
        job.id = JobId::new();
        f.db.jobs()
            .insert_job_with_event(&job, 10, &event(EventKind::JobSubmitted, json!({}))?)
            .await?;
        let mut value = serde_json::to_value(attempt(&job, 1)?)?;
        value["result"]["owned_namespace"] = json!(true);
        value["result"]["failed_artifact_retention"] =
            json!({"policy":"delete-after-diagnosis","keep_on_failure":["keep*"]});
        let spec: AttemptSpec = serde_json::from_value(value)?;
        let id = spec.id();
        f.db.jobs()
            .insert_attempt_with_event(&spec, &event(EventKind::AttemptCreated, json!({}))?)
            .await?;
        let (stdout, stderr) = f.db.jobs().prepare_attempt_logs(id, &f.paths).await?;
        fs::write(&stdout, b"out")?;
        fs::write(&stderr, b"err")?;
        fs::write(
            stdout.parent().ok_or("root")?.join("disposable.bin"),
            b"garbage",
        )?;
        fs::write(stdout.parent().ok_or("root")?.join("keep.bin"), b"kept")?;
        sqlx::query("INSERT INTO attempt_processes(attempt_id,job_id,project_id,pid,process_group_id,process_start_ticks,stdout_path,stderr_path) VALUES (?,?,?,2147483647,2147483647,1,?,?)").bind(id.to_string()).bind(job.id.to_string()).bind(job.project_id.to_string()).bind(stdout.to_str()).bind(stderr.to_str()).execute(f.db.pool()).await?;
        sqlx::query("UPDATE attempts SET state = 'failed' WHERE id = ?")
            .bind(id.to_string())
            .execute(f.db.pool())
            .await?;
        sqlx::query("UPDATE jobs SET state = 'failed' WHERE id = ?")
            .bind(job.id.to_string())
            .execute(f.db.pool())
            .await?;
        sqlx::query("INSERT INTO failed_log_tails(attempt_id,stdout,stderr,limit_kb,complete) VALUES (?,X'6f7574',X'657272',512,1)").bind(id.to_string()).execute(f.db.pool()).await?;
        let recovery = uuid::Uuid::new_v4().to_string();
        sqlx::query("INSERT INTO recoveries(id,attempt_id,state,diagnosis_json,idempotency_key) VALUES (?,?,'proposed','{}',?)").bind(&recovery).bind(id.to_string()).bind(&recovery).execute(f.db.pool()).await?;
        attempts.push(id);
    }
    attempts.sort();
    let work = f.work().await?;
    assert_eq!(work.selections.len(), 32);
    assert_eq!(
        f.db.gc_execute(None, true, work, &f.paths).await?.completed,
        32
    );
    let work = f.work().await?;
    assert_eq!(work.selections.len(), 32);
    assert!(
        work.selections
            .iter()
            .all(|s| s.expected.status == GcStatus::MetadataOnly)
    );
    assert_eq!(
        f.db.gc_execute(None, true, work, &f.paths).await?.completed,
        32
    );
    let work = f.work().await?;
    assert_eq!(work.selections.len(), 1);
    assert_eq!(work.selections[0].expected.attempt_id, Some(attempts[32]));
    assert_eq!(
        f.db.gc_execute(None, true, work, &f.paths).await?.completed,
        1
    );
    assert!(
        f.root
            .parent()
            .ok_or("logs")?
            .join(attempts[32].to_string())
            .join("keep.bin")
            .exists()
    );
    Ok(())
}

#[tokio::test]
async fn owned_outputs_reserve_only_exact_controls_and_real_metadata_boundaries() -> TestResult {
    let f = Owned::new(true, false).await?;
    f.diagnose().await?;
    let reserved = f
        .root
        .join(format!(".igor-log-{}.tmp", uuid::Uuid::new_v4()));
    fs::write(&reserved, b"reserved")?;
    sqlx::query("INSERT INTO success_log_archive_preparations(attempt_id,stream,raw_path,temp_path) VALUES (?,'stdout',?,?)").bind(f.attempt.to_string()).bind(f.root.join("stdout.log").to_str()).bind(reserved.to_str()).execute(f.db.pool()).await?;
    for name in ["stdout.log.gz", "stderr.log.gz", ".git"] {
        fs::write(f.root.join(name), b"control")?;
    }
    fs::create_dir(f.root.join("metadata"))?;
    fs::write(f.root.join("metadata/nested"), b"not traversed")?;
    let unreserved = format!(".igor-log-{}.tmp", uuid::Uuid::new_v4());
    for name in [
        ".scratch",
        "checkpoint.tmp",
        "results.gz",
        "config",
        "index",
        unreserved.as_str(),
    ] {
        fs::write(f.root.join(name), b"disposable")?;
    }
    assert_eq!(f.retire().await?, 7);
    for name in [
        ".scratch",
        "checkpoint.tmp",
        "results.gz",
        "config",
        "index",
        unreserved.as_str(),
    ] {
        assert!(!f.root.join(name).exists());
    }
    assert_eq!(fs::read(reserved)?, b"reserved");
    for name in ["stdout.log.gz", "stderr.log.gz", ".git"] {
        assert_eq!(fs::read(f.root.join(name))?, b"control");
    }
    assert_eq!(fs::read(f.root.join("metadata/nested"))?, b"not traversed");
    assert_eq!(f.retire().await?, 0);
    assert!(f.work().await?.selections.is_empty());
    Ok(())
}
