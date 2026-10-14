use std::error::Error;

use igor_core::{AttemptId, Database, DatabaseOptions, JobId, PersistenceError, ProjectId};
use serde_json::json;

type TestResult = Result<(), Box<dyn Error>>;

async fn project(db: &Database, name: &str) -> Result<ProjectId, Box<dyn Error>> {
    let id = ProjectId::new();
    sqlx::query("INSERT INTO projects(id, name, root_path, config_path) VALUES (?, ?, ?, ?)")
        .bind(id.to_string())
        .bind(name)
        .bind(format!("/missing/{name}"))
        .bind(format!("/missing/{name}/project.toml"))
        .execute(db.pool())
        .await?;
    Ok(id)
}

async fn attempt(
    db: &Database,
    project: ProjectId,
    order: i64,
) -> Result<AttemptId, Box<dyn Error>> {
    let job = JobId::new();
    let id = AttemptId::new();
    sqlx::query("INSERT INTO jobs(id, project_id, name, state, submission_order, spec_json) VALUES (?, ?, 'inventory', 'succeeded', ?, '{}')")
        .bind(job.to_string()).bind(project.to_string()).bind(order).execute(db.pool()).await?;
    sqlx::query("INSERT INTO attempts(id, job_id, project_id, sequence, state, spec_json) VALUES (?, ?, ?, 1, 'succeeded', '{}')")
        .bind(id.to_string()).bind(job.to_string()).bind(project.to_string()).execute(db.pool()).await?;
    Ok(id)
}

async fn artifact(
    db: &Database,
    attempt: AttemptId,
    id: &str,
    state: &str,
    lifecycle: &str,
    size: Option<i64>,
) -> TestResult {
    sqlx::query("INSERT INTO artifacts(id, attempt_id, path, role, state, lifecycle_state, size_bytes, sha256, metadata_json, diagnosis_required) VALUES (?, ?, ?, 'model', ?, ?, ?, ?, ?, ?)")
        .bind(id).bind(attempt.to_string()).bind(format!("/missing/source/{id}"))
        .bind(state).bind(lifecycle).bind(size).bind("a".repeat(64))
        .bind(json!({"staged_path": format!("/missing/stage/{id}"), "published_path": format!("/missing/published/{id}")}).to_string())
        .bind(lifecycle == "failed")
        .execute(db.pool()).await?;
    Ok(())
}

async fn archive(
    db: &Database,
    attempt: AttemptId,
    stream: &str,
    state: &str,
    retired: bool,
    original: u64,
    compressed: u64,
) -> TestResult {
    let identity = json!({"path":"/missing/archive.gz", "temporary_path":"/missing/archive.tmp", "original_path":"/missing/raw", "original_size":original, "archive_size":compressed, "original_sha256":"a".repeat(64), "archive_sha256":"b".repeat(64), "codec_version":1});
    sqlx::query("INSERT INTO success_log_archives(attempt_id, stream, identity_json, state, retired) VALUES (?, ?, ?, ?, ?)")
        .bind(attempt.to_string()).bind(stream).bind(identity.to_string()).bind(state).bind(retired)
        .execute(db.pool()).await?;
    Ok(())
}

async fn tombstone(db: &Database, operation: &str, subject: &str, outcome: &str) -> TestResult {
    sqlx::query("INSERT INTO cleanup_tombstones(operation, subject_id, schema_version, reason, outcome, identity_json, decided_at) VALUES (?, ?, 1, 'inventory fixture', ?, '{}', '2026-01-01')")
        .bind(operation).bind(subject).bind(outcome).execute(db.pool()).await?;
    Ok(())
}

async fn log_preparation(db: &Database, attempt: AttemptId, stream: &str) -> TestResult {
    sqlx::query("INSERT INTO success_log_archive_preparations(attempt_id, stream, raw_path, temp_path) VALUES (?, ?, ?, ?)")
        .bind(attempt.to_string()).bind(stream)
        .bind(format!("/missing/{attempt}/{stream}"))
        .bind(format!("/missing/{attempt}/{stream}.tmp"))
        .execute(db.pool()).await?;
    Ok(())
}

// A full SQL dump compares persisted values, including leases and timestamps, rather
// than only the rows selected by the inventory implementation.
async fn snapshot(db: &Database) -> Result<Vec<(String, Vec<String>)>, Box<dyn Error>> {
    let tables: Vec<String> = sqlx::query_scalar("SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%' ORDER BY name")
        .fetch_all(db.pool()).await?;
    let mut result = Vec::new();
    for table in tables {
        let columns: Vec<String> = sqlx::query_scalar(&format!(
            "SELECT name FROM pragma_table_info('{table}') ORDER BY cid"
        ))
        .fetch_all(db.pool())
        .await?;
        let expression = columns
            .iter()
            .map(|c| format!("quote(\"{c}\")"))
            .collect::<Vec<_>>()
            .join(" || ',' || ");
        let rows = sqlx::query_scalar(&format!("SELECT {expression} FROM \"{table}\" ORDER BY 1"))
            .fetch_all(db.pool())
            .await?;
        result.push((table, rows));
    }
    Ok(result)
}

#[tokio::test]
async fn inventory_is_scoped_read_only_and_counts_identities_once() -> TestResult {
    let temporary = tempfile::tempdir()?;
    let path = temporary.path().join("inventory.db");
    let db = Database::open(&path).await?;
    let p = project(&db, "first").await?;
    let other = project(&db, "second").await?;
    let historic = project(&db, "historic").await?;
    let a = attempt(&db, p, 1).await?;
    let b = attempt(&db, p, 2).await?;
    let c = attempt(&db, other, 3).await?;
    let h = attempt(&db, historic, 4).await?;
    sqlx::query("UPDATE projects SET registered = 0 WHERE id = ?")
        .bind(historic.to_string())
        .execute(db.pool())
        .await?;

    artifact(&db, a, "known", "published", "published", Some(100)).await?;
    artifact(&db, a, "legacy", "published", "published", None).await?;
    artifact(&db, b, "staged", "staged", "failed", Some(7)).await?;
    artifact(&db, a, "removed", "removed", "removed", Some(900)).await?;
    artifact(&db, c, "second", "published", "retained", Some(200)).await?;
    artifact(&db, h, "historic", "published", "published", Some(300)).await?;
    archive(&db, a, "stdout", "prepared", false, 1000, 101).await?;
    archive(&db, a, "stderr", "published", false, 2000, 202).await?;
    archive(&db, b, "stdout", "published", true, 3000, 303).await?;
    archive(&db, c, "stdout", "published", true, 5000, 505).await?;
    for (id, stream) in [
        (a, "stdout"), // Matched prepared identity.
        (a, "stderr"), // Matched published identity.
        (b, "stdout"), // Matched retired identity.
        (b, "stderr"), // Different stream from the archived reservation.
        (c, "stderr"), // Different project, same stream as an unmatched reservation.
        (h, "stdout"), // Unregistered history still contributes globally.
        (h, "stderr"),
    ] {
        log_preparation(&db, id, stream).await?;
    }
    for (id, complete, stdout, stderr) in [
        (a, true, vec![0, 255, 0, 128], vec![255, 0]),
        (b, false, vec![0, 255, 0], vec![128]),
        (c, true, vec![0; 11], vec![255; 13]),
    ] {
        sqlx::query("INSERT INTO failed_log_tails(attempt_id, stdout, stderr, limit_kb, complete) VALUES (?, ?, ?, 1, ?)")
            .bind(id.to_string()).bind(stdout).bind(stderr).bind(complete).execute(db.pool()).await?;
    }
    for (i, state) in ["pending", "running", "succeeded", "failed", "cancelled"]
        .into_iter()
        .enumerate()
    {
        sqlx::query("INSERT INTO actions(id, project_id, kind, state, spec_json, idempotency_key, available_at, claim_id, claim_owner, claim_expires_at) VALUES (?, ?, 'cleanup_failed_artifacts', ?, '{}', ?, '2026-01-01', ?, ?, ?)")
            .bind(format!("cleanup-{i}")).bind(p.to_string()).bind(state).bind(format!("cleanup-{i}"))
            .bind((state == "running").then_some("claim"))
            .bind((state == "running").then_some("owner"))
            .bind((state == "running").then_some("2099-01-01"))
            .execute(db.pool()).await?;
    }
    sqlx::query("INSERT INTO actions(id, project_id, kind, state, spec_json, idempotency_key, available_at) VALUES ('unrelated', ?, 'generate_report', 'pending', '{}', 'unrelated', '2026-01-01')")
        .bind(p.to_string()).execute(db.pool()).await?;
    for id in ["staged", "removed"] {
        sqlx::query(
            "INSERT INTO failed_cleanup_intents(artifact_id, identity_json) VALUES (?, '{}')",
        )
        .bind(id)
        .execute(db.pool())
        .await?;
    }
    sqlx::query(
        "UPDATE failed_cleanup_intents SET state = 'completed' WHERE artifact_id = 'removed'",
    )
    .execute(db.pool())
    .await?;

    for (id, attempt_id, project_id) in [
        ("released", a, p),
        ("registered", b, p),
        ("other-repair", c, other),
    ] {
        sqlx::query("INSERT INTO recoveries(id, attempt_id, state, idempotency_key) VALUES (?, ?, 'succeeded', ?)")
            .bind(id).bind(attempt_id.to_string()).bind(id).execute(db.pool()).await?;
        sqlx::query("INSERT INTO repair_worktrees(recovery_id, project_id, path, root_path, repository_path, common_dir, admin_dir, identity_json, delete_on_release) VALUES (?, ?, ?, '/missing', '/missing/repo', '/missing/common', ?, '{}', 0)")
            .bind(id).bind(project_id.to_string()).bind(format!("/missing/{id}"))
            .bind(format!("/missing/admin/{id}")).execute(db.pool()).await?;
    }
    let evidence = json!({"schema_version":1,"diagnosis":{"schema_version":1,"content":{}},"validation_results":{"schema_version":1,"results":[]},"changed_paths":[],"candidate":null});
    sqlx::query("UPDATE repair_worktrees SET state = 'released', patch = ?, patch_sha256 = ?, records_json = ?, released_at = '2026-01-01' WHERE recovery_id = 'released'")
        .bind(vec![0_u8, 255, 0, 128, 1]).bind("a".repeat(64)).bind(evidence.to_string()).execute(db.pool()).await?;
    tombstone(&db, "failed_artifact_retention", "removed", "removed").await?;
    tombstone(&db, "repair_worktree_retention", "released", "retained").await?;
    tombstone(
        &db,
        "success_log_compression",
        &format!("{b}:stdout"),
        "raw_retired",
    )
    .await?;
    tombstone(
        &db,
        "success_log_compression",
        &format!("{c}:stdout"),
        "raw_retired",
    )
    .await?;
    // Similar prefixes and unknown operations must never acquire a scoped association.
    tombstone(
        &db,
        "success_log_compression",
        &format!("{b}:stdout-extra"),
        "raw_retired",
    )
    .await?;
    tombstone(&db, "future_operation", "unassociated", "retained").await?;

    let before = snapshot(&db).await?;
    let status = db.storage_status(Some(p)).await?;
    assert_eq!(status.project_id, Some(p));
    assert_eq!(status.project_name.as_deref(), Some("first"));
    assert_eq!(status.artifacts.active.len(), 2);
    let published = &status.artifacts.active[0];
    assert_eq!(
        (
            &published.physical_state[..],
            &published.lifecycle_state[..]
        ),
        ("published", "published")
    );
    assert_eq!(
        (
            published.count,
            published.known_size_count,
            published.unknown_size_count,
            published.logical_bytes
        ),
        (2, 1, 1, 100)
    );
    assert_eq!(status.artifacts.active[1].logical_bytes, 7);
    assert_eq!(status.artifacts.removed_history[0].logical_bytes, 900);
    assert_eq!(status.successful_log_archives.len(), 3);
    assert_eq!(status.pending_log_preparations.count, 1);
    assert_eq!(status.pending_log_preparations.temporary_bytes, None);
    assert_eq!(
        serde_json::to_value(&status)?["pending_log_preparations"],
        json!({"count": 1, "temporary_bytes": null})
    );
    let logs = &status.successful_log_archives;
    assert_eq!(
        (
            logs[0].state.as_str(),
            logs[0].original_identity_bytes,
            logs[0].archive_identity_bytes,
            logs[0].raw_outstanding_bytes
        ),
        ("prepared", 1000, 101, 1000)
    );
    assert_eq!(
        (logs[1].retired, logs[1].raw_outstanding_bytes),
        (false, 2000)
    );
    assert_eq!(
        (
            logs[2].retired,
            logs[2].original_identity_bytes,
            logs[2].archive_identity_bytes,
            logs[2].raw_outstanding_bytes
        ),
        (true, 3000, 303, 0)
    );
    assert_eq!(
        (
            status.failed_tails.complete_count,
            status.failed_tails.incomplete_count,
            status.failed_tails.stdout_blob_bytes,
            status.failed_tails.stderr_blob_bytes
        ),
        (1, 1, 7, 3)
    );
    assert_eq!(
        status
            .repair_worktrees
            .iter()
            .map(|r| (r.state.as_str(), r.count, r.patch_blob_bytes, r.disk_bytes))
            .collect::<Vec<_>>(),
        vec![("registered", 1, 0, None), ("released", 1, 5, None)]
    );
    assert_eq!(
        status
            .failed_cleanup_actions
            .iter()
            .map(|r| (r.state.as_str(), r.count))
            .collect::<Vec<_>>(),
        vec![
            ("cancelled", 1),
            ("failed", 1),
            ("pending", 1),
            ("running", 1),
            ("succeeded", 1)
        ]
    );
    assert_eq!(
        status
            .cleanup_intents
            .iter()
            .map(|r| (r.kind.as_str(), r.state.as_str(), r.count))
            .collect::<Vec<_>>(),
        vec![("failed", "completed", 1), ("failed", "prepared", 1)]
    );
    assert_eq!(
        status
            .tombstones
            .iter()
            .map(|r| (r.operation.as_str(), r.outcome.as_str(), r.count))
            .collect::<Vec<_>>(),
        vec![
            ("failed_artifact_retention", "removed", 1),
            ("repair_worktree_retention", "retained", 1),
            ("success_log_compression", "raw_retired", 1)
        ]
    );
    assert_eq!(status.actual_disk_footprint, None);
    assert!(serde_json::to_value(&status)?["actual_disk_footprint"].is_null());
    let global = db.storage_status(None).await?;
    assert_eq!(global.pending_log_preparations.count, 4);
    assert_eq!(global.pending_log_preparations.temporary_bytes, None);
    assert_eq!(
        db.storage_status(Some(other))
            .await?
            .pending_log_preparations
            .count,
        1
    );
    assert_eq!(
        db.storage_status(Some(historic))
            .await?
            .pending_log_preparations
            .count,
        2
    );
    assert_eq!((global.project_id, global.project_name), (None, None));
    assert_eq!(
        global
            .artifacts
            .active
            .iter()
            .map(|r| r.logical_bytes)
            .sum::<u64>(),
        607
    );
    assert_eq!(global.tombstones.iter().map(|r| r.count).sum::<u64>(), 6);
    assert_eq!(
        db.storage_status(Some(other)).await?.artifacts.active[0].logical_bytes,
        200
    );
    assert_eq!(
        db.storage_status(Some(historic)).await?.artifacts.active[0].logical_bytes,
        300
    );
    assert!(matches!(
        db.storage_status(Some(ProjectId::new())).await,
        Err(PersistenceError::NotFound { entity: "project" })
    ));
    let readonly = Database::open_with_options(
        &path,
        DatabaseOptions {
            writable: false,
            create_if_missing: false,
            ..Default::default()
        },
    )
    .await?;
    assert_eq!(readonly.storage_status(Some(p)).await?, status);
    assert_eq!(snapshot(&db).await?, before);
    Ok(())
}

#[tokio::test]
async fn empty_inventory_has_zero_payload_and_no_disk_estimate() -> TestResult {
    let temporary = tempfile::tempdir()?;
    let db = Database::open(temporary.path().join("empty.db")).await?;
    let p = project(&db, "empty").await?;
    for scope in [None, Some(p)] {
        let status = db.storage_status(scope).await?;
        assert!(status.artifacts.active.is_empty());
        assert!(status.artifacts.removed_history.is_empty());
        assert!(status.successful_log_archives.is_empty());
        assert_eq!(status.pending_log_preparations, Default::default());
        assert_eq!(
            serde_json::to_value(&status)?["pending_log_preparations"],
            json!({"count": 0, "temporary_bytes": null})
        );
        assert_eq!(status.failed_tails, Default::default());
        assert!(status.repair_worktrees.is_empty());
        assert!(status.failed_cleanup_actions.is_empty());
        assert!(status.cleanup_intents.is_empty());
        assert!(status.tombstones.is_empty());
        assert_eq!(status.actual_disk_footprint, None);
    }
    Ok(())
}

#[tokio::test]
async fn superseded_preparations_and_tombstones_are_evidence_not_extra_bytes() -> TestResult {
    let temporary = tempfile::tempdir()?;
    let db = Database::open(temporary.path().join("superseded.db")).await?;
    let p = project(&db, "superseded").await?;
    let other = project(&db, "unrelated").await?;
    sqlx::query("INSERT INTO families(id, project_id, name) VALUES ('family', ?, 'family')")
        .bind(p.to_string())
        .execute(db.pool())
        .await?;
    for number in [1, 2] {
        let generation = format!("generation-{number}");
        sqlx::query("INSERT INTO generations(id, family_id, project_id, generation_number, source_revision, protocol_digest, spec_json) VALUES (?, 'family', ?, ?, ?, 'protocol', ?)")
            .bind(&generation).bind(p.to_string()).bind(number).bind(format!("revision-{number}"))
            .bind(json!({"family":{"members":[{"seed":1}]}}).to_string()).execute(db.pool()).await?;
        let a = attempt(&db, p, number).await?;
        let job: String = sqlx::query_scalar("SELECT job_id FROM attempts WHERE id = ?")
            .bind(a.to_string())
            .fetch_one(db.pool())
            .await?;
        let spec = json!({"id":job,"family":{"family_id":"family","seed":1,"generation":{"id":generation,"number":number,"source_revision":format!("revision-{number}"),"protocol_digest":"protocol"}}});
        sqlx::query("UPDATE jobs SET family_id = 'family', generation_id = ?, state = ?, spec_json = ? WHERE id = ?")
            .bind(&generation).bind(if number == 1 { "superseded" } else { "succeeded" })
            .bind(spec.to_string()).bind(&job).execute(db.pool()).await?;
        if number == 1 {
            for id in ["old-prepared", "old-removed"] {
                artifact(&db, a, id, "published", "superseded", Some(42)).await?;
            }
        }
    }
    for id in ["old-prepared", "old-removed"] {
        sqlx::query("INSERT INTO superseded_cleanup_intents(artifact_id, identity_json, replacement_generation_id) VALUES (?, '{}', 'generation-2')")
            .bind(id).execute(db.pool()).await?;
    }
    sqlx::query(
        "UPDATE superseded_cleanup_intents SET state = 'removed' WHERE artifact_id = 'old-removed'",
    )
    .execute(db.pool())
    .await?;
    sqlx::query("UPDATE artifacts SET state = 'removed', lifecycle_state = 'removed' WHERE id = 'old-removed'").execute(db.pool()).await?;
    tombstone(
        &db,
        "superseded_artifact_retention",
        "old-removed",
        "removed",
    )
    .await?;
    let before = snapshot(&db).await?;
    let status = db.storage_status(Some(p)).await?;
    assert_eq!(status.artifacts.active[0].logical_bytes, 42);
    assert_eq!(status.artifacts.active[0].lifecycle_state, "superseded");
    assert_eq!(status.artifacts.removed_history[0].logical_bytes, 42);
    assert_eq!(
        status
            .cleanup_intents
            .iter()
            .map(|r| (r.kind.as_str(), r.state.as_str(), r.count))
            .collect::<Vec<_>>(),
        vec![("superseded", "prepared", 1), ("superseded", "removed", 1)]
    );
    assert_eq!(
        status.tombstones[0].operation,
        "superseded_artifact_retention"
    );
    assert_eq!(status.tombstones[0].count, 1);
    assert_eq!(
        db.storage_status(None).await?.cleanup_intents,
        status.cleanup_intents
    );
    let unrelated = db.storage_status(Some(other)).await?;
    assert!(unrelated.cleanup_intents.is_empty());
    assert!(unrelated.tombstones.is_empty());
    assert_eq!(snapshot(&db).await?, before);
    Ok(())
}
