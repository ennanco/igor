use std::{
    error::Error,
    os::unix::process::ExitStatusExt,
    path::Path,
    process::{Child, Command, Stdio},
    thread,
    time::{Duration, Instant},
};

use igor_core::{Database, Project, StorageStatus, initialize_project};

type TestResult = Result<(), Box<dyn Error>>;
#[path = "../../igor-core/tests/support/gc.rs"]
#[allow(dead_code)]
mod gc_support;

fn command(home: &Path, cwd: &Path) -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_igor"));
    command
        .env_clear()
        .current_dir(cwd)
        .env("HOME", home)
        .env("XDG_CONFIG_HOME", home.join("config"))
        .env("XDG_STATE_HOME", home.join("state"))
        .env("XDG_RUNTIME_DIR", home.join("runtime"));
    command
}

struct Worker(Child);

impl Drop for Worker {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

/// Health poll for the worker-only fixture: `storage status` is answered by the
/// worker daemon itself, so its success marks readiness. Bounded by `timeout`,
/// and the tracked child is checked on every iteration so a dead worker never
/// turns into an unbounded wait.
fn await_worker_ready(
    worker: &mut Worker,
    home: &Path,
    cwd: &Path,
    timeout: Duration,
) -> TestResult {
    let deadline = Instant::now() + timeout;
    loop {
        let output = command(home, cwd)
            .args(["storage", "status", "--json"])
            .output()?;
        if output.status.success() {
            return Ok(());
        }
        assert!(
            worker.0.try_wait()?.is_none(),
            "worker exited: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(
            Instant::now() < deadline,
            "worker unavailable: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        thread::sleep(Duration::from_millis(20));
    }
}

fn status(
    home: &Path,
    cwd: &Path,
    project: Option<&Path>,
) -> Result<StorageStatus, Box<dyn Error>> {
    let mut cmd = command(home, cwd);
    cmd.args(["storage", "status", "--json"]);
    if let Some(project) = project {
        cmd.arg("--project").arg(project);
    }
    let output = cmd.output()?;
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    Ok(serde_json::from_slice(&output.stdout)?)
}

#[test]
fn worker_only_storage_status_supports_empty_human_json_and_project_selection() -> TestResult {
    let temp = tempfile::tempdir()?;
    let home = temp.path().join("home");
    let outside = temp.path();
    let bare = command(&home, outside).arg("gc").output()?;
    assert_eq!(bare.status.code(), Some(3));
    let mut worker = Worker(
        command(&home, outside)
            .arg("worker")
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()?,
    );
    await_worker_ready(&mut worker, &home, outside, Duration::from_secs(15))?;
    let empty = status(&home, outside, None)?;
    assert_eq!(empty.project_id, None);
    assert!(empty.artifacts.active.is_empty());
    assert_eq!(empty.pending_log_preparations, Default::default());
    assert_eq!(empty.failed_tails, Default::default());
    let gc = command(&home, outside)
        .args(["gc", "--dry-run", "--json"])
        .output()?;
    assert!(
        gc.status.success(),
        "{}",
        String::from_utf8_lossy(&gc.stderr)
    );
    let gc: igor_core::GcPlan = serde_json::from_slice(&gc.stdout)?;
    assert!(gc.entries.is_empty());
    assert_eq!(gc.pages.len(), 6);
    let gc = command(&home, outside).args(["gc", "--dry-run"]).output()?;
    assert!(gc.status.success());
    let human_gc = String::from_utf8(gc.stdout)?;
    assert!(human_gc.contains("GC dry-run: all projects"));
    assert!(human_gc.contains("Future execution must revalidate"));
    for args in [vec!["gc"], vec!["gc", "--json"]] {
        let noop = command(&home, outside).args(args).output()?;
        assert!(noop.status.success());
        assert!(!String::from_utf8(noop.stderr)?.contains("[y/N]"));
    }
    let human = command(&home, outside)
        .args(["storage", "status"])
        .output()?;
    assert!(human.status.success());
    let human = String::from_utf8(human.stdout)?;
    for label in [
        "Storage scope: global",
        "Active artifacts: 0 rows",
        "Removed artifact history: 0 rows",
        "Successful log archives: 0 rows",
        "Pending log preparations: count=0; temporary bytes not measured",
        "Failed tails: complete=0 incomplete=0 stdout BLOB bytes=0 stderr BLOB bytes=0",
        "Repair worktrees: 0 rows; disk bytes not measured",
        "Failed cleanup actions: 0 rows",
        "Cleanup intents: 0 rows",
        "Cleanup tombstones: 0 rows",
        "Actual disk footprint: not measured",
    ] {
        assert!(human.contains(label), "missing {label}: {human}");
    }
    let first_root = temp.path().join("first");
    let second_root = temp.path().join("second");
    let unknown_root = temp.path().join("unknown");
    for root in [&first_root, &second_root, &unknown_root] {
        initialize_project(root, false)?;
    }
    let mut projects = Vec::new();
    for root in [&first_root, &second_root] {
        let added = command(&home, outside)
            .args(["project", "add"])
            .arg(root)
            .arg("--json")
            .output()?;
        assert!(
            added.status.success(),
            "{}",
            String::from_utf8_lossy(&added.stderr)
        );
        projects.push(serde_json::from_slice::<Project>(&added.stdout)?);
    }
    let runtime = tokio::runtime::Runtime::new()?;
    let database_path = home.join("state/igor/igor.sqlite3");
    let mut db = runtime.block_on(Database::open(&database_path))?;
    runtime.block_on(async {
        for (i, project) in projects.iter().enumerate() {
            for j in 0..=i {
                let id = format!("cleanup-{i}-{j}");
                sqlx::query("INSERT INTO actions(id, project_id, kind, state, spec_json, idempotency_key, available_at) VALUES (?, ?, 'cleanup_failed_artifacts', 'pending', '{}', ?, '2026-01-01')")
                    .bind(&id).bind(project.id.to_string()).bind(&id).execute(db.pool()).await?;
            }
        }
        Ok::<_, sqlx::Error>(())
    })?;
    let before: Vec<(String, String, Option<String>, i64)> = runtime.block_on(
        sqlx::query_as("SELECT id, state, claim_id, attempts FROM actions ORDER BY id")
            .fetch_all(db.pool()),
    )?;
    let events_before: i64 =
        runtime.block_on(sqlx::query_scalar("SELECT COUNT(*) FROM events").fetch_one(db.pool()))?;
    let first = status(&home, &first_root, None)?;
    for (cwd, explicit, expected) in [
        (&first_root, None, Some(projects[0].id)),
        (&first_root, Some(&second_root), Some(projects[1].id)),
        (&outside.to_path_buf(), None, None),
    ] {
        let mut cmd = command(&home, cwd);
        cmd.args(["gc", "--dry-run", "--json"]);
        if let Some(root) = explicit {
            cmd.arg("--project").arg(root);
        }
        let output = cmd.output()?;
        assert!(output.status.success());
        let gc: igor_core::GcPlan = serde_json::from_slice(&output.stdout)?;
        assert_eq!(gc.project_id, expected);
    }
    assert_eq!(first.project_id, Some(projects[0].id));
    assert_eq!(
        first.project_name.as_deref(),
        Some(projects[0].name.as_str())
    );
    assert_eq!(first.failed_cleanup_actions[0].count, 1);
    let second = status(&home, &first_root, Some(&second_root))?;
    assert_eq!(second.project_id, Some(projects[1].id));
    assert_eq!(second.failed_cleanup_actions[0].count, 2);
    assert_eq!(status(&home, outside, Some(&first_root))?, first);
    let global = status(&home, outside, None)?;
    assert_eq!(global.project_id, None);
    assert_eq!(global.failed_cleanup_actions[0].count, 3);
    let scoped_human = command(&home, &first_root)
        .args(["storage", "status"])
        .output()?;
    assert!(scoped_human.status.success());
    assert!(String::from_utf8(scoped_human.stdout)?.contains("pending: count=1"));
    for cwd in [&unknown_root, outside] {
        let mut gc = command(&home, cwd);
        gc.args(["gc", "--dry-run"]);
        if cwd == outside {
            gc.arg("--project").arg(&unknown_root);
        }
        let gc = gc.output()?;
        assert!(!gc.status.success());
        assert!(String::from_utf8(gc.stderr)?.contains("current project is not registered"));
        let mut cmd = command(&home, cwd);
        cmd.args(["storage", "status"]);
        if cwd == outside {
            cmd.arg("--project").arg(&unknown_root);
        }
        let unknown = cmd.output()?;
        assert!(!unknown.status.success());
        assert!(String::from_utf8(unknown.stderr)?.contains("current project is not registered"));
    }
    assert!(!home.join("runtime/igor/supervisor.sock").exists());
    let after: Vec<(String, String, Option<String>, i64)> = runtime.block_on(
        sqlx::query_as("SELECT id, state, claim_id, attempts FROM actions ORDER BY id")
            .fetch_all(db.pool()),
    )?;
    let events_after: i64 =
        runtime.block_on(sqlx::query_scalar("SELECT COUNT(*) FROM events").fetch_one(db.pool()))?;
    assert_eq!(after, before);
    assert_eq!(events_after, events_before);
    // A real worker-created attempt supplies the normal persisted log boundaries.
    for args in [
        vec!["init", "-q"],
        vec!["add", "."],
        vec![
            "-c",
            "user.name=Test",
            "-c",
            "user.email=test@example.invalid",
            "commit",
            "-qm",
            "fixture",
        ],
    ] {
        assert!(
            Command::new("git")
                .arg("-C")
                .arg(&first_root)
                .args(args)
                .status()?
                .success()
        );
    }
    let submitted = command(&home, &first_root)
        .args(["submit", "--json", "--", "/bin/true"])
        .output()?;
    assert!(
        submitted.status.success(),
        "{}",
        String::from_utf8_lossy(&submitted.stderr)
    );
    let submitted: serde_json::Value = serde_json::from_slice(&submitted.stdout)?;
    let job_id = submitted["spec"]["id"].as_str().ok_or("job ID")?;
    let waited = command(&home, &first_root)
        .args(["wait", job_id, "--json"])
        .output()?;
    assert!(
        waited.status.success(),
        "{}",
        String::from_utf8_lossy(&waited.stderr)
    );
    let (attempt, stdout): (String, String) = runtime.block_on(sqlx::query_as("SELECT a.id, p.stdout_path FROM attempts a JOIN attempt_processes p ON p.attempt_id = a.id WHERE a.job_id = ?").bind(job_id).fetch_one(db.pool()))?;
    let source = Path::new(&stdout).with_file_name("failed-artifact.bin");
    std::fs::write(&source, b"retained diagnostic bytes")?;
    let publication = first_root.join("publication");
    runtime.block_on(async {
        sqlx::query("DROP TRIGGER attempts_spec_immutable").execute(db.pool()).await?;
        sqlx::query("UPDATE attempts SET spec_json = json_set(spec_json, '$.result.extractor', json_extract(spec_json, '$.command'), '$.result.publication_root', ?, '$.result.failed_artifact_retention', json('{\"policy\":\"delete-after-diagnosis\",\"keep_on_failure\":[]}')) WHERE id = ?")
            .bind(publication.to_str()).bind(&attempt).execute(db.pool()).await?;
        let id = igor_core::RecoveryId::new().to_string();
        sqlx::query("INSERT INTO recoveries(id, attempt_id, state, diagnosis_json, idempotency_key) VALUES (?, ?, 'proposed', '{}', ?)").bind(&id).bind(&attempt).bind(&id).execute(db.pool()).await?;
        Ok::<_, sqlx::Error>(())
    })?;
    use sha2::Digest;
    let artifact = igor_core::ArtifactRecord {
        id: "00000000-0000-0000-0000-000000000001".parse()?,
        attempt_id: attempt.parse()?,
        path: "failed-artifact.bin".into(),
        role: igor_core::ArtifactRole::Checkpoint,
        sha256: Some(format!(
            "sha256:{:x}",
            sha2::Sha256::digest(std::fs::read(&source)?)
        )),
        size_bytes: Some(std::fs::metadata(&source)?.len()),
        state: igor_core::ArtifactPublicationState::Registered,
        lifecycle: igor_core::ArtifactLifecycleState::Failed,
        diagnosis_required: true,
        metadata: serde_json::json!({}),
    };
    runtime.block_on(db.artifacts().insert(&artifact))?;
    let state_dir = home.join("state/igor");
    let runtime_dir = home.join("runtime/igor");
    let paths = igor_core::RuntimePaths {
        config_file: home.join("config/igor/config.toml"),
        database: state_dir.join("igor.sqlite3"),
        log_dir: Path::new(&stdout)
            .parent()
            .ok_or("attempt directory")?
            .parent()
            .ok_or("log directory")?
            .to_owned(),
        worktree_dir: state_dir.join("worktrees"),
        report_dir: state_dir.join("reports"),
        worker_socket: runtime_dir.join("worker.sock"),
        supervisor_socket: runtime_dir.join("supervisor.sock"),
        resource_dir: runtime_dir.join("resources"),
        state_dir,
        runtime_dir,
    };
    let initial_plan = runtime.block_on(db.gc_plan_filtered(None, &paths, true))?;
    assert_eq!(
        initial_plan.worklist()?.selections.len(),
        1,
        "{initial_plan:?}"
    );
    let sql_before = runtime.block_on(gc_support::sql_snapshot(&db))?;
    let files_before = gc_support::files(temp.path(), &home.join("state/igor/igor.sqlite3"))?;
    for (json, answer) in [
        (false, "no\n"),
        (true, "n\n"),
        (false, ""),
        (true, ""),
        (true, "\n"),
        (true, "true\n"),
    ] {
        let mut cmd = command(&home, outside);
        cmd.args(["gc", "--failed"]);
        if json {
            cmd.arg("--json");
        }
        let mut child = cmd
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()?;
        use std::io::Write;
        child
            .stdin
            .take()
            .ok_or("stdin")?
            .write_all(answer.as_bytes())?;
        let output = child.wait_with_output()?;
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(String::from_utf8_lossy(&output.stderr).contains("[y/N]"));
        if json {
            let result: igor_core::GcExecution = serde_json::from_slice(&output.stdout)?;
            assert!(!result.confirmed && result.entries.is_empty());
        } else {
            assert!(output.stdout.is_empty());
        }
        assert_eq!(runtime.block_on(gc_support::sql_snapshot(&db))?, sql_before);
        assert_eq!(
            gc_support::files(temp.path(), &home.join("state/igor/igor.sqlite3"))?,
            files_before
        );
    }
    let dry = command(&home, outside)
        .args(["gc", "--dry-run", "--failed", "--yes", "--json"])
        .output()?;
    assert!(dry.status.success());
    assert!(!String::from_utf8_lossy(&dry.stderr).contains("[y/N]"));
    let dry: igor_core::GcPlan = serde_json::from_slice(&dry.stdout)?;
    assert_eq!(dry.pages.len(), 3);
    assert_eq!(dry.entries.len(), 1);
    assert_eq!(runtime.block_on(gc_support::sql_snapshot(&db))?, sql_before);
    runtime.block_on(sqlx::query("CREATE TRIGGER reject_gc_event BEFORE INSERT ON events WHEN NEW.kind = 'cleanup_state_changed' BEGIN SELECT RAISE(ABORT, 'injected GC event failure'); END").execute(db.pool()))?;
    let failed = command(&home, outside)
        .args(["gc", "--failed", "--yes", "--json"])
        .output()?;
    assert_eq!(failed.status.code(), Some(1));
    let result: igor_core::GcExecution = serde_json::from_slice(&failed.stdout)?;
    assert!(result.confirmed && result.errors >= 1, "{result:?}");
    assert!(
        String::from_utf8_lossy(&failed.stderr).contains("GC encountered errors or unsafe entries")
    );
    assert!(!source.exists());
    assert!(Path::new(&stdout).is_file());
    assert!(
        runtime
            .block_on(db.cleanup_tombstone("failed_artifact_retention", &artifact.id.to_string()))?
            .is_none()
    );
    let intent_before: (String, String) = runtime.block_on(
        sqlx::query_as(
            "SELECT state, decided_at FROM failed_cleanup_intents WHERE artifact_id = ?",
        )
        .bind(artifact.id.to_string())
        .fetch_one(db.pool()),
    )?;
    assert_eq!(intent_before.0, "prepared");
    let events: i64 = runtime.block_on(
        sqlx::query_scalar(
            "SELECT COUNT(*) FROM events WHERE kind = 'cleanup_state_changed' AND attempt_id = ?",
        )
        .bind(&attempt)
        .fetch_one(db.pool()),
    )?;
    assert_eq!(events, 0);
    // A13.03 crash seam. The completed failed request already persisted the
    // prepared intent above, so that SQL row (never a sleep) orders the kill.
    // The worker dies abruptly with SIGKILL while the parent connection is still
    // open; only afterwards is the parent pool closed, so the terminal error's
    // WAL frames are read back through a fresh connection without any explicit
    // checkpoint.
    worker.0.kill()?;
    let killed = worker.0.wait()?;
    assert_eq!(killed.signal(), Some(nix::libc::SIGKILL));
    runtime.block_on(db.pool().close());
    db = runtime.block_on(Database::open(&database_path))?;
    let persisted: (String, String, String) = runtime.block_on(
        sqlx::query_as(
            "SELECT artifact_id, state, decided_at FROM failed_cleanup_intents WHERE artifact_id = ?",
        )
        .bind(artifact.id.to_string())
        .fetch_one(db.pool()),
    )?;
    assert_eq!(persisted.0, artifact.id.to_string());
    assert_eq!(persisted.1, "prepared");
    assert_eq!(
        persisted.2, intent_before.1,
        "decided_at must survive the kill"
    );
    let persisted_tombstones: i64 = runtime.block_on(
        sqlx::query_scalar(
            "SELECT COUNT(*) FROM cleanup_tombstones WHERE operation = 'failed_artifact_retention' AND subject_id = ?",
        )
        .bind(artifact.id.to_string())
        .fetch_one(db.pool()),
    )?;
    assert_eq!(persisted_tombstones, 0);
    let persisted_events: i64 = runtime.block_on(
        sqlx::query_scalar(
            "SELECT COUNT(*) FROM events WHERE kind = 'cleanup_state_changed' AND attempt_id = ?",
        )
        .bind(&attempt)
        .fetch_one(db.pool()),
    )?;
    assert_eq!(persisted_events, 0);
    assert!(!source.exists());
    assert!(Path::new(&stdout).is_file());
    // The fault trigger lives in SQLite, so it is dropped through the reopened
    // database handle. No supervisor is ever (re)started for this fixture.
    runtime.block_on(sqlx::query("DROP TRIGGER reject_gc_event").execute(db.pool()))?;
    worker = Worker(
        command(&home, outside)
            .arg("worker")
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()?,
    );
    await_worker_ready(&mut worker, &home, outside, Duration::from_secs(10))?;
    assert!(!home.join("runtime/igor/supervisor.sock").exists());
    let yes = command(&home, outside)
        .args(["gc", "--failed", "--yes", "--json"])
        .output()?;
    assert!(
        yes.status.success(),
        "{}",
        String::from_utf8_lossy(&yes.stderr)
    );
    assert!(!String::from_utf8_lossy(&yes.stderr).contains("[y/N]"));
    let result: igor_core::GcExecution = serde_json::from_slice(&yes.stdout)?;
    assert_eq!(result.completed, 1, "{result:?}");
    assert!(result.confirmed);
    assert_eq!(result.errors, 0);
    assert_eq!(result.unsafe_count, 0);
    let intent_after: (String, String) = runtime.block_on(
        sqlx::query_as(
            "SELECT state, decided_at FROM failed_cleanup_intents WHERE artifact_id = ?",
        )
        .bind(artifact.id.to_string())
        .fetch_one(db.pool()),
    )?;
    assert_eq!(intent_after.0, "completed");
    assert_eq!(intent_after.1, intent_before.1);
    let tombstones: i64 = runtime.block_on(
        sqlx::query_scalar("SELECT COUNT(*) FROM cleanup_tombstones WHERE operation = 'failed_artifact_retention' AND subject_id = ?")
            .bind(artifact.id.to_string())
            .fetch_one(db.pool()),
    )?;
    assert_eq!(tombstones, 1);
    let events: i64 = runtime.block_on(
        sqlx::query_scalar(
            "SELECT COUNT(*) FROM events WHERE kind = 'cleanup_state_changed' AND attempt_id = ?",
        )
        .bind(&attempt)
        .fetch_one(db.pool()),
    )?;
    assert_eq!(events, 1);
    assert!(!source.exists());
    assert!(Path::new(&stdout).is_file());
    assert!(!home.join("runtime/igor/supervisor.sock").exists());
    // A fresh CLI preview of the same scope is now empty and leaves both the SQL
    // snapshot and the filesystem untouched.
    let completed_sql = runtime.block_on(gc_support::sql_snapshot(&db))?;
    let completed_files = gc_support::files(temp.path(), &database_path)?;
    let repeat_output = command(&home, outside)
        .args(["gc", "--dry-run", "--failed", "--yes", "--json"])
        .output()?;
    assert!(
        repeat_output.status.success(),
        "{}",
        String::from_utf8_lossy(&repeat_output.stderr)
    );
    let repeat: igor_core::GcPlan = serde_json::from_slice(&repeat_output.stdout)?;
    assert!(repeat.entries.is_empty(), "{repeat:?}");
    assert_eq!(
        runtime.block_on(gc_support::sql_snapshot(&db))?,
        completed_sql
    );
    assert_eq!(
        gc_support::files(temp.path(), &database_path)?,
        completed_files
    );
    for (i, json, answer) in [(2u128, false, "Y\n"), (3u128, true, "yes\n")] {
        let mut next = artifact.clone();
        next.id = format!("00000000-0000-0000-0000-{i:012x}").parse()?;
        next.path = format!("affirmative-{i}.bin");
        let path = source.with_file_name(&next.path);
        std::fs::write(&path, b"retained diagnostic bytes")?;
        runtime.block_on(db.artifacts().insert(&next))?;
        let mut cmd = command(&home, outside);
        cmd.args(["gc", "--failed"]);
        if json {
            cmd.arg("--json");
        }
        let mut child = cmd
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()?;
        use std::io::Write;
        child
            .stdin
            .take()
            .ok_or("stdin")?
            .write_all(answer.as_bytes())?;
        let output = child.wait_with_output()?;
        assert!(output.status.success());
        assert!(!path.exists());
        if json {
            let result: igor_core::GcExecution = serde_json::from_slice(&output.stdout)?;
            assert_eq!(result.completed, 1);
        } else {
            assert!(output.stdout.is_empty());
        }
    }
    // A13.02: preview-unsafe subjects are filtered out of the worklist, so the
    // CLI must still report them after explicit consent instead of exit 0.
    let safe_id = "00000000-0000-0000-0000-000000000010";
    let unsafe_id = "00000000-0000-0000-0000-000000000011";
    let safe_payload = b"known safe diagnostic payload";
    let safe_path = source.with_file_name("replay-safe.bin");
    std::fs::write(&safe_path, safe_payload)?;
    let mut safe_artifact = artifact.clone();
    safe_artifact.id = safe_id.parse()?;
    safe_artifact.path = "replay-safe.bin".into();
    safe_artifact.sha256 = Some(format!("sha256:{:x}", sha2::Sha256::digest(safe_payload)));
    safe_artifact.size_bytes = Some(safe_payload.len() as u64);
    runtime.block_on(db.artifacts().insert(&safe_artifact))?;
    // A second real attempt keeps the unsafe subject out of the first attempt's
    // artifact batch, which is unsafe as a whole.
    let submitted2 = command(&home, &first_root)
        .args(["submit", "--json", "--", "/bin/true"])
        .output()?;
    assert!(
        submitted2.status.success(),
        "{}",
        String::from_utf8_lossy(&submitted2.stderr)
    );
    let submitted2: serde_json::Value = serde_json::from_slice(&submitted2.stdout)?;
    let job2 = submitted2["spec"]["id"].as_str().ok_or("second job ID")?;
    let waited2 = command(&home, &first_root)
        .args(["wait", job2, "--json"])
        .output()?;
    assert!(
        waited2.status.success(),
        "{}",
        String::from_utf8_lossy(&waited2.stderr)
    );
    let (attempt2, stdout2): (String, String) = runtime.block_on(
        sqlx::query_as(
            "SELECT a.id, p.stdout_path FROM attempts a JOIN attempt_processes p ON p.attempt_id = a.id WHERE a.job_id = ?",
        )
        .bind(job2)
        .fetch_one(db.pool()),
    )?;
    let publication2 = first_root.join("publication");
    runtime.block_on(async {
        sqlx::query("DROP TRIGGER IF EXISTS attempts_spec_immutable")
            .execute(db.pool())
            .await?;
        sqlx::query("UPDATE attempts SET spec_json = json_set(spec_json, '$.result.extractor', json_extract(spec_json, '$.command'), '$.result.publication_root', ?, '$.result.failed_artifact_retention', json('{\"policy\":\"delete-after-diagnosis\",\"keep_on_failure\":[]}')) WHERE id = ?")
            .bind(publication2.to_str()).bind(&attempt2).execute(db.pool()).await?;
        let id = igor_core::RecoveryId::new().to_string();
        sqlx::query("INSERT INTO recoveries(id, attempt_id, state, diagnosis_json, idempotency_key) VALUES (?, ?, 'proposed', '{}', ?)").bind(&id).bind(&attempt2).bind(&id).execute(db.pool()).await?;
        Ok::<_, sqlx::Error>(())
    })?;
    // Registered digest differs from the bytes on disk, so the whole attempt's
    // artifact batch fails preflight and every entry becomes unsafe.
    let registered = b"registered digest payload";
    let on_disk = b"on-disk bytes that do not match the registered digest";
    let unsafe_path = Path::new(&stdout2).with_file_name("unsafe-artifact.bin");
    std::fs::write(&unsafe_path, on_disk)?;
    let mut unsafe_artifact = artifact.clone();
    unsafe_artifact.id = unsafe_id.parse()?;
    unsafe_artifact.attempt_id = attempt2.parse()?;
    unsafe_artifact.path = "unsafe-artifact.bin".into();
    unsafe_artifact.sha256 = Some(format!("sha256:{:x}", sha2::Sha256::digest(registered)));
    unsafe_artifact.size_bytes = Some(registered.len() as u64);
    runtime.block_on(db.artifacts().insert(&unsafe_artifact))?;
    let mixed_sql = runtime.block_on(gc_support::sql_snapshot(&db))?;
    let mixed_files = gc_support::files(temp.path(), &home.join("state/igor/igor.sqlite3"))?;
    let preview_output = command(&home, outside)
        .args(["gc", "--dry-run", "--failed", "--json"])
        .output()?;
    assert!(
        preview_output.status.success(),
        "{}",
        String::from_utf8_lossy(&preview_output.stderr)
    );
    let preview: igor_core::GcPlan = serde_json::from_slice(&preview_output.stdout)?;
    assert_eq!(preview.pages.len(), 3);
    assert_eq!(preview.entries.len(), 2, "{preview:?}");
    let safe_entry = preview
        .entries
        .iter()
        .find(|entry| entry.subject_id == safe_id)
        .ok_or("safe preview entry")?;
    assert_eq!(safe_entry.status, igor_core::GcStatus::Ready);
    assert_eq!(safe_entry.targets.len(), 1, "{safe_entry:?}");
    assert_eq!(safe_entry.targets[0].bytes, Some(safe_payload.len() as u64));
    assert!(preview.known_copy_bytes >= safe_payload.len() as u64);
    let preview_unsafe = preview
        .entries
        .iter()
        .find(|entry| entry.subject_id == unsafe_id)
        .ok_or("unsafe preview entry")?;
    assert_eq!(preview_unsafe.status, igor_core::GcStatus::Unsafe);
    assert!(preview_unsafe.targets.is_empty());
    // The initial unsafe subject is never authorized for the executor.
    let worklist = preview.worklist()?;
    assert_eq!(worklist.selections.len(), 1, "{worklist:?}");
    assert_eq!(worklist.selections[0].expected.subject_id, safe_id);
    assert_eq!(runtime.block_on(gc_support::sql_snapshot(&db))?, mixed_sql);
    assert_eq!(
        gc_support::files(temp.path(), &home.join("state/igor/igor.sqlite3"))?,
        mixed_files
    );
    // Decline and EOF on a mixed plan keep the unconfirmed empty response.
    for answer in ["n\n", ""] {
        let mut child = command(&home, outside)
            .args(["gc", "--failed", "--json"])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()?;
        use std::io::Write;
        child
            .stdin
            .take()
            .ok_or("stdin")?
            .write_all(answer.as_bytes())?;
        let declined = child.wait_with_output()?;
        assert!(
            declined.status.success(),
            "{}",
            String::from_utf8_lossy(&declined.stderr)
        );
        assert!(String::from_utf8_lossy(&declined.stderr).contains("[y/N]"));
        let result: igor_core::GcExecution = serde_json::from_slice(&declined.stdout)?;
        assert!(!result.confirmed && result.entries.is_empty(), "{result:?}");
        assert_eq!(result.unsafe_count, 0);
        assert_eq!(runtime.block_on(gc_support::sql_snapshot(&db))?, mixed_sql);
        assert_eq!(
            gc_support::files(temp.path(), &home.join("state/igor/igor.sqlite3"))?,
            mixed_files
        );
    }
    let mixed_output = command(&home, outside)
        .args(["gc", "--failed", "--yes", "--json"])
        .output()?;
    assert_eq!(
        mixed_output.status.code(),
        Some(1),
        "{}",
        String::from_utf8_lossy(&mixed_output.stderr)
    );
    assert!(
        String::from_utf8_lossy(&mixed_output.stderr)
            .contains("GC encountered errors or unsafe entries")
    );
    let mixed: igor_core::GcExecution = serde_json::from_slice(&mixed_output.stdout)?;
    assert!(mixed.confirmed, "{mixed:?}");
    assert_eq!(mixed.completed, 1, "{mixed:?}");
    assert_eq!(mixed.errors, 0, "{mixed:?}");
    assert_eq!(mixed.unsafe_count, 1, "{mixed:?}");
    assert_eq!(mixed.entries.len(), 2, "{mixed:?}");
    let reported = mixed
        .entries
        .iter()
        .find(|entry| entry.subject_id == unsafe_id)
        .ok_or("unsafe execution entry")?;
    assert_eq!(reported.status, igor_core::GcExecutionStatus::Unsafe);
    let cause = reported.cause.as_ref().ok_or("unsafe cause")?;
    assert_eq!(cause.code, "unsafe_preview");
    assert_eq!(
        cause.message,
        "Dry-run preflight rejected this entry; it was not selected for execution"
    );
    assert!(!safe_path.exists());
    assert!(unsafe_path.exists());
    let safe_state: String = runtime.block_on(
        sqlx::query_scalar("SELECT lifecycle_state FROM artifacts WHERE id = ?")
            .bind(safe_id)
            .fetch_one(db.pool()),
    )?;
    assert_eq!(safe_state, "removed");
    let unsafe_state: (String, String) = runtime.block_on(
        sqlx::query_as("SELECT lifecycle_state, state FROM artifacts WHERE id = ?")
            .bind(unsafe_id)
            .fetch_one(db.pool()),
    )?;
    assert_eq!(unsafe_state, ("failed".to_owned(), "registered".to_owned()));
    let authorized: i64 = runtime.block_on(
        sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM failed_cleanup_intents WHERE artifact_id = ?)",
        )
        .bind(unsafe_id)
        .fetch_one(db.pool()),
    )?;
    assert_eq!(authorized, 0, "preview-unsafe subject was authorized");
    // Unsafe-only plans report the rejection without requesting execution.
    let unsafe_only_sql = runtime.block_on(gc_support::sql_snapshot(&db))?;
    let unsafe_only_files = gc_support::files(temp.path(), &home.join("state/igor/igor.sqlite3"))?;
    let unconfirmed_output = command(&home, outside)
        .args(["gc", "--failed", "--json"])
        .stdin(Stdio::null())
        .output()?;
    assert_eq!(unconfirmed_output.status.code(), Some(1));
    assert!(!String::from_utf8_lossy(&unconfirmed_output.stderr).contains("[y/N]"));
    let unconfirmed: igor_core::GcExecution = serde_json::from_slice(&unconfirmed_output.stdout)?;
    assert!(!unconfirmed.confirmed);
    assert_eq!((unconfirmed.completed, unconfirmed.unsafe_count), (0, 1));
    assert_eq!(unconfirmed.entries.len(), 1);
    assert_eq!(unconfirmed.entries[0].subject_id, unsafe_id);
    assert_eq!(
        runtime.block_on(gc_support::sql_snapshot(&db))?,
        unsafe_only_sql
    );
    assert_eq!(
        gc_support::files(temp.path(), &home.join("state/igor/igor.sqlite3"))?,
        unsafe_only_files
    );
    let unsafe_only_output = command(&home, outside)
        .args(["gc", "--failed", "--yes", "--json"])
        .output()?;
    assert_eq!(
        unsafe_only_output.status.code(),
        Some(1),
        "{}",
        String::from_utf8_lossy(&unsafe_only_output.stderr)
    );
    assert!(!String::from_utf8_lossy(&unsafe_only_output.stderr).contains("[y/N]"));
    let unsafe_only: igor_core::GcExecution = serde_json::from_slice(&unsafe_only_output.stdout)?;
    assert!(unsafe_only.confirmed, "{unsafe_only:?}");
    assert_eq!(unsafe_only.completed, 0, "{unsafe_only:?}");
    assert_eq!(unsafe_only.unsafe_count, 1, "{unsafe_only:?}");
    assert_eq!(unsafe_only.entries.len(), 1, "{unsafe_only:?}");
    assert_eq!(unsafe_only.entries[0].subject_id, unsafe_id);
    assert_eq!(
        unsafe_only.entries[0].status,
        igor_core::GcExecutionStatus::Unsafe
    );
    assert_eq!(
        unsafe_only.entries[0]
            .cause
            .as_ref()
            .map(|cause| cause.code.as_str()),
        Some("unsafe_preview")
    );
    assert_eq!(
        runtime.block_on(gc_support::sql_snapshot(&db))?,
        unsafe_only_sql
    );
    assert_eq!(
        gc_support::files(temp.path(), &home.join("state/igor/igor.sqlite3"))?,
        unsafe_only_files
    );
    // A13.02 scope acceptance: project scope is bounded, global sees both
    // projects, and neither dry-run mutates SQL or the filesystem.
    let scope_first_id = "00000000-0000-0000-0000-000000000021";
    let scope_second_id = "00000000-0000-0000-0000-000000000022";
    let scope_payload = b"retained diagnostic bytes";
    let scope_first_path = source.with_file_name("scope-first.bin");
    std::fs::write(&scope_first_path, scope_payload)?;
    let mut scope_first = artifact.clone();
    scope_first.id = scope_first_id.parse()?;
    scope_first.path = "scope-first.bin".into();
    runtime.block_on(db.artifacts().insert(&scope_first))?;
    for args in [
        vec!["init", "-q"],
        vec!["add", "."],
        vec![
            "-c",
            "user.name=Test",
            "-c",
            "user.email=test@example.invalid",
            "commit",
            "-qm",
            "fixture",
        ],
    ] {
        assert!(
            Command::new("git")
                .arg("-C")
                .arg(&second_root)
                .args(args)
                .status()?
                .success()
        );
    }
    let submitted3 = command(&home, &second_root)
        .args(["submit", "--json", "--", "/bin/true"])
        .output()?;
    assert!(
        submitted3.status.success(),
        "{}",
        String::from_utf8_lossy(&submitted3.stderr)
    );
    let submitted3: serde_json::Value = serde_json::from_slice(&submitted3.stdout)?;
    let job3 = submitted3["spec"]["id"]
        .as_str()
        .ok_or("second project job ID")?;
    let waited3 = command(&home, &second_root)
        .args(["wait", job3, "--json"])
        .output()?;
    assert!(
        waited3.status.success(),
        "{}",
        String::from_utf8_lossy(&waited3.stderr)
    );
    let (attempt3, stdout3): (String, String) = runtime.block_on(
        sqlx::query_as(
            "SELECT a.id, p.stdout_path FROM attempts a JOIN attempt_processes p ON p.attempt_id = a.id WHERE a.job_id = ?",
        )
        .bind(job3)
        .fetch_one(db.pool()),
    )?;
    let publication3 = second_root.join("publication");
    runtime.block_on(async {
        sqlx::query("DROP TRIGGER IF EXISTS attempts_spec_immutable")
            .execute(db.pool())
            .await?;
        sqlx::query("UPDATE attempts SET spec_json = json_set(spec_json, '$.result.extractor', json_extract(spec_json, '$.command'), '$.result.publication_root', ?, '$.result.failed_artifact_retention', json('{\"policy\":\"delete-after-diagnosis\",\"keep_on_failure\":[]}')) WHERE id = ?")
            .bind(publication3.to_str()).bind(&attempt3).execute(db.pool()).await?;
        let id = igor_core::RecoveryId::new().to_string();
        sqlx::query("INSERT INTO recoveries(id, attempt_id, state, diagnosis_json, idempotency_key) VALUES (?, ?, 'proposed', '{}', ?)").bind(&id).bind(&attempt3).bind(&id).execute(db.pool()).await?;
        Ok::<_, sqlx::Error>(())
    })?;
    let scope_second_path = Path::new(&stdout3).with_file_name("scope-second.bin");
    std::fs::write(&scope_second_path, scope_payload)?;
    let mut scope_second = artifact.clone();
    scope_second.id = scope_second_id.parse()?;
    scope_second.attempt_id = attempt3.parse()?;
    scope_second.path = "scope-second.bin".into();
    runtime.block_on(db.artifacts().insert(&scope_second))?;
    let scope_sql = runtime.block_on(gc_support::sql_snapshot(&db))?;
    let scope_files = gc_support::files(temp.path(), &home.join("state/igor/igor.sqlite3"))?;
    let scoped_output = command(&home, &first_root)
        .args(["gc", "--dry-run", "--failed", "--json"])
        .output()?;
    assert!(
        scoped_output.status.success(),
        "{}",
        String::from_utf8_lossy(&scoped_output.stderr)
    );
    let scoped: igor_core::GcPlan = serde_json::from_slice(&scoped_output.stdout)?;
    assert_eq!(scoped.project_id, Some(projects[0].id));
    assert_eq!(scoped.pages.len(), 3, "{scoped:?}");
    assert_eq!(scoped.entries.len(), 2, "{scoped:?}");
    assert!(
        scoped
            .entries
            .iter()
            .all(|entry| entry.project_id == projects[0].id)
    );
    let scoped_safe = scoped
        .entries
        .iter()
        .find(|entry| entry.subject_id == scope_first_id)
        .ok_or("scoped safe entry")?;
    assert_eq!(scoped_safe.status, igor_core::GcStatus::Ready);
    assert_eq!(scoped_safe.targets.len(), 1, "{scoped_safe:?}");
    assert_eq!(scoped_safe.targets[0].path, scope_first_path);
    assert_eq!(
        scoped_safe.targets[0].bytes,
        Some(scope_payload.len() as u64)
    );
    assert_eq!(
        scoped
            .entries
            .iter()
            .find(|entry| entry.subject_id == unsafe_id)
            .map(|entry| entry.status),
        Some(igor_core::GcStatus::Unsafe),
        "scoped plan must keep the preview-unsafe subject"
    );
    let global_output = command(&home, outside)
        .args(["gc", "--dry-run", "--failed", "--json"])
        .output()?;
    assert!(
        global_output.status.success(),
        "{}",
        String::from_utf8_lossy(&global_output.stderr)
    );
    let global_plan: igor_core::GcPlan = serde_json::from_slice(&global_output.stdout)?;
    assert_eq!(global_plan.project_id, None);
    assert_eq!(global_plan.pages.len(), 3, "{global_plan:?}");
    assert_eq!(global_plan.entries.len(), 3, "{global_plan:?}");
    let mut global_projects: Vec<String> = global_plan
        .entries
        .iter()
        .map(|entry| entry.project_id.to_string())
        .collect();
    global_projects.sort();
    global_projects.dedup();
    assert_eq!(global_projects.len(), 2, "{global_plan:?}");
    assert!(global_projects.contains(&projects[0].id.to_string()));
    assert!(global_projects.contains(&projects[1].id.to_string()));
    for entry in &scoped.entries {
        let twin = global_plan
            .entries
            .iter()
            .find(|candidate| candidate.subject_id == entry.subject_id)
            .ok_or("global twin for scoped subject")?;
        assert_eq!(twin.status, entry.status, "{entry:?}");
        assert_eq!(twin.project_id, entry.project_id, "{entry:?}");
        assert_eq!(twin.targets, entry.targets, "{entry:?}");
    }
    let global_second = global_plan
        .entries
        .iter()
        .find(|entry| entry.subject_id == scope_second_id)
        .ok_or("global second-project entry")?;
    assert_eq!(global_second.status, igor_core::GcStatus::Ready);
    assert_eq!(global_second.project_id, projects[1].id);
    assert_eq!(global_second.targets.len(), 1, "{global_second:?}");
    assert_eq!(global_second.targets[0].path, scope_second_path);
    assert_eq!(
        global_second.targets[0].bytes,
        Some(scope_payload.len() as u64)
    );
    assert_eq!(runtime.block_on(gc_support::sql_snapshot(&db))?, scope_sql);
    assert_eq!(
        gc_support::files(temp.path(), &home.join("state/igor/igor.sqlite3"))?,
        scope_files
    );
    // Scoped execution cleans only this project's safe subject.
    let scope_execute = command(&home, &first_root)
        .args(["gc", "--failed", "--yes", "--json"])
        .output()?;
    assert_eq!(
        scope_execute.status.code(),
        Some(1),
        "{}",
        String::from_utf8_lossy(&scope_execute.stderr)
    );
    let scope_execute: igor_core::GcExecution = serde_json::from_slice(&scope_execute.stdout)?;
    assert!(scope_execute.confirmed, "{scope_execute:?}");
    assert_eq!(scope_execute.completed, 1, "{scope_execute:?}");
    assert_eq!(scope_execute.errors, 0, "{scope_execute:?}");
    assert_eq!(scope_execute.unsafe_count, 1, "{scope_execute:?}");
    assert!(!scope_first_path.exists());
    assert!(scope_second_path.exists());
    let second_untouched: i64 = runtime.block_on(
        sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM failed_cleanup_intents WHERE artifact_id = ?)",
        )
        .bind(scope_second_id)
        .fetch_one(db.pool()),
    )?;
    assert_eq!(second_untouched, 0, "scoped execution touched project two");
    let second_preview_output = command(&home, &second_root)
        .args(["gc", "--dry-run", "--failed", "--json"])
        .output()?;
    assert!(
        second_preview_output.status.success(),
        "{}",
        String::from_utf8_lossy(&second_preview_output.stderr)
    );
    let second_preview: igor_core::GcPlan = serde_json::from_slice(&second_preview_output.stdout)?;
    assert_eq!(second_preview.project_id, Some(projects[1].id));
    // `--failed` excludes SuccessLogCompression entirely.
    assert_eq!(second_preview.pages.len(), 3, "{second_preview:?}");
    assert_eq!(second_preview.entries.len(), 1, "{second_preview:?}");
    assert_eq!(second_preview.entries[0].subject_id, scope_second_id);
    let second_execute = command(&home, &second_root)
        .args(["gc", "--failed", "--yes", "--json"])
        .output()?;
    assert!(
        second_execute.status.success(),
        "{}",
        String::from_utf8_lossy(&second_execute.stderr)
    );
    let second_execute: igor_core::GcExecution = serde_json::from_slice(&second_execute.stdout)?;
    assert!(second_execute.confirmed, "{second_execute:?}");
    assert_eq!(second_execute.completed, 1, "{second_execute:?}");
    assert_eq!(second_execute.errors, 0, "{second_execute:?}");
    assert_eq!(second_execute.unsafe_count, 0, "{second_execute:?}");
    assert!(!scope_second_path.exists());
    // The global plan now only holds the preview-unsafe first subject.
    let remaining_sql = runtime.block_on(gc_support::sql_snapshot(&db))?;
    let remaining_files = gc_support::files(temp.path(), &home.join("state/igor/igor.sqlite3"))?;
    let global_execute = command(&home, outside)
        .args(["gc", "--failed", "--yes", "--json"])
        .output()?;
    assert_eq!(
        global_execute.status.code(),
        Some(1),
        "{}",
        String::from_utf8_lossy(&global_execute.stderr)
    );
    let global_execute: igor_core::GcExecution = serde_json::from_slice(&global_execute.stdout)?;
    assert!(global_execute.confirmed, "{global_execute:?}");
    assert_eq!(global_execute.completed, 0, "{global_execute:?}");
    assert_eq!(global_execute.unsafe_count, 1, "{global_execute:?}");
    assert_eq!(global_execute.entries.len(), 1, "{global_execute:?}");
    assert_eq!(global_execute.entries[0].subject_id, unsafe_id);
    assert_eq!(
        runtime.block_on(gc_support::sql_snapshot(&db))?,
        remaining_sql
    );
    assert_eq!(
        gc_support::files(temp.path(), &home.join("state/igor/igor.sqlite3"))?,
        remaining_files
    );
    // Cheap truncation fixture: 65 registered rows exceed the 64-row batch cap.
    for i in 100u128..=164 {
        let mut bounded = artifact.clone();
        bounded.id = format!("00000000-0000-0000-0000-{i:012x}").parse()?;
        bounded.path = format!("bounded-{i}.bin");
        std::fs::write(source.with_file_name(&bounded.path), scope_payload)?;
        runtime.block_on(db.artifacts().insert(&bounded))?;
    }
    let truncation_sql = runtime.block_on(gc_support::sql_snapshot(&db))?;
    let truncation_files = gc_support::files(temp.path(), &home.join("state/igor/igor.sqlite3"))?;
    let truncation_human = command(&home, outside)
        .args(["gc", "--dry-run", "--failed"])
        .output()?;
    assert!(
        truncation_human.status.success(),
        "{}",
        String::from_utf8_lossy(&truncation_human.stderr)
    );
    assert!(
        String::from_utf8(truncation_human.stdout)?.contains("artifact batch truncated"),
        "missing truncation flag"
    );
    let truncation_json = command(&home, outside)
        .args(["gc", "--dry-run", "--failed", "--json"])
        .output()?;
    assert!(
        truncation_json.status.success(),
        "{}",
        String::from_utf8_lossy(&truncation_json.stderr)
    );
    let truncation: igor_core::GcPlan = serde_json::from_slice(&truncation_json.stdout)?;
    let failed_page = truncation
        .pages
        .iter()
        .find(|page| page.operation == igor_core::GcOperation::FailedArtifacts)
        .ok_or("failed artifacts page")?;
    assert!(!failed_page.truncated, "{failed_page:?}");
    let truncated_batches: Vec<_> = failed_page
        .artifact_batches
        .iter()
        .filter(|batch| batch.truncated)
        .collect();
    assert_eq!(
        truncated_batches.len(),
        1,
        "{:?}",
        failed_page.artifact_batches
    );
    assert_eq!(truncated_batches[0].rows_observed, 64, "{truncation:?}");
    assert!(truncated_batches[0].continuation_after.is_some());
    let bounded_entries: Vec<_> = truncation
        .entries
        .iter()
        .filter(|entry| entry.subject_id != unsafe_id)
        .collect();
    assert_eq!(bounded_entries.len(), 64, "{truncation:?}");
    assert!(
        bounded_entries
            .iter()
            .all(|entry| entry.status == igor_core::GcStatus::Ready)
    );
    let bounded_targets: Vec<_> = truncation
        .entries
        .iter()
        .flat_map(|entry| &entry.targets)
        .collect();
    assert_eq!(bounded_targets.len(), 64, "{truncation:?}");
    assert_eq!(
        truncation.known_copy_bytes,
        64 * scope_payload.len() as u64,
        "{truncation:?}"
    );
    let literal_first = source.with_file_name("bounded-100.bin");
    assert!(truncation.entries.iter().any(|entry| {
        entry.targets.iter().any(|target| {
            target.path == literal_first && target.bytes == Some(scope_payload.len() as u64)
        })
    }));
    assert!(
        truncation
            .entries
            .iter()
            .any(|entry| entry.subject_id == unsafe_id
                && entry.status == igor_core::GcStatus::Unsafe)
    );
    assert_eq!(
        runtime.block_on(gc_support::sql_snapshot(&db))?,
        truncation_sql
    );
    assert_eq!(
        gc_support::files(temp.path(), &home.join("state/igor/igor.sqlite3"))?,
        truncation_files
    );
    runtime.block_on(db.pool().close());
    Ok(())
}
