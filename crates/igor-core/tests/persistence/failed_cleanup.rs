use super::*;
use igor_core::persistence::FailedCleanupOutcome;

async fn negative_replay_prepare_failed(f: &FailedCleanupFixture) -> TestResult {
    f.diagnose().await?;
    sqlx::raw_sql("CREATE TRIGGER reject_negative_event BEFORE INSERT ON events WHEN NEW.kind = 'cleanup_state_changed' BEGIN SELECT RAISE(ABORT, 'negative replay rollback'); END;")
        .execute(f.database.pool()).await?;
    assert!(f.cleanup().await.is_err());
    for path in [&f.source, &f.staged] {
        assert_eq!(
            fs::symlink_metadata(path)
                .err()
                .ok_or("artifact copy remains present")?
                .kind(),
            std::io::ErrorKind::NotFound
        );
    }
    sqlx::query("DROP TRIGGER reject_negative_event")
        .execute(f.database.pool())
        .await?;
    assert_eq!(
        race_diagnosis_evidence_counts(&f.database).await?,
        (1, 0, 0, 0)
    );
    let state: String = sqlx::query_scalar("SELECT state FROM failed_cleanup_intents")
        .fetch_one(f.database.pool())
        .await?;
    assert_eq!(state, "prepared");
    Ok(())
}

async fn negative_replay_assert_failed_rejected(f: &FailedCleanupFixture) -> TestResult {
    let before = gc_support::sql_snapshot(&f.database).await?;
    let disk = gc_support::files(f._directory.path(), &f.paths.database)?;
    let plan = gc_support::readonly_plan(&f.database, &f.paths, f._directory.path(), None).await?;
    let entries: Vec<_> = plan
        .entries
        .iter()
        .filter(|e| e.operation == igor_core::GcOperation::FailedArtifacts)
        .collect();
    assert_eq!(entries.len(), 1);
    assert!(
        entries
            .iter()
            .all(|e| e.status == igor_core::GcStatus::Unsafe && e.targets.is_empty()),
        "{entries:?}"
    );
    assert!(f.cleanup().await.is_err());
    assert_eq!(gc_support::sql_snapshot(&f.database).await?, before);
    assert_eq!(
        gc_support::files(f._directory.path(), &f.paths.database)?,
        disk
    );
    assert_eq!(
        race_diagnosis_evidence_counts(&f.database).await?,
        (1, 0, 0, 0)
    );
    assert!(
        f.database
            .cleanup_tombstone("failed_artifact_retention", &f.artifact.id.to_string())
            .await?
            .is_none()
    );
    assert_eq!(
        f.database
            .artifacts()
            .get(f.artifact.id)
            .await?
            .ok_or("artifact")?
            .lifecycle,
        igor_core::ArtifactLifecycleState::Failed
    );
    Ok(())
}

#[tokio::test]
async fn negative_replay_failed_absent_leaves_reject_linked_roots_and_ancestors() -> TestResult {
    use std::os::unix::fs::MetadataExt;
    for case in ["attempt", "logs", "stage_attempt", "staging", "state"] {
        let f = FailedCleanupFixture::new().await?;
        let unrelated = f.project_root.join("unrelated-registered-root-file");
        fs::write(&unrelated, b"preserve registered root")?;
        negative_replay_prepare_failed(&f).await?;
        let link = match case {
            "attempt" => f.source.parent().ok_or("attempt")?.to_path_buf(),
            "logs" => f.paths.log_dir.clone(),
            "stage_attempt" => f.staged.parent().ok_or("stage attempt")?.to_path_buf(),
            "staging" => f.paths.state_dir.join("artifact-staging"),
            "state" => f.paths.state_dir.clone(),
            _ => unreachable!(),
        };
        let external = TempDir::new()?;
        fs::write(external.path().join("sentinel"), b"external live bytes")?;
        fs::rename(&link, link.with_extension("saved"))?;
        std::os::unix::fs::symlink(external.path(), &link)?;
        let metadata = fs::symlink_metadata(&link)?;
        assert!(metadata.file_type().is_symlink());
        let external_before = gc_support::files(external.path(), &f.paths.database)?;
        assert_eq!(external_before.len(), 2, "root and one live sentinel");
        for path in [&f.source, &f.staged] {
            assert_eq!(
                fs::symlink_metadata(path)
                    .err()
                    .ok_or("artifact leaf is not absent")?
                    .kind(),
                std::io::ErrorKind::NotFound,
                "{case}: the artifact leaf must remain absent"
            );
        }
        negative_replay_assert_failed_rejected(&f).await?;
        let after = fs::symlink_metadata(&link)?;
        assert!(after.file_type().is_symlink());
        assert_eq!((after.dev(), after.ino()), (metadata.dev(), metadata.ino()));
        assert_eq!(fs::read_link(&link)?, external.path());
        assert_eq!(
            gc_support::files(external.path(), &f.paths.database)?,
            external_before
        );
        assert_eq!(fs::read(&unrelated)?, b"preserve registered root");
    }
    Ok(())
}

#[tokio::test]
async fn negative_replay_failed_absent_leaves_reject_dot_and_parent_runtime_spellings() -> TestResult
{
    for case in ["logs_dot", "logs_parent", "state_dot", "state_parent"] {
        let mut f = FailedCleanupFixture::new().await?;
        negative_replay_prepare_failed(&f).await?;
        let intents: Vec<(String, String)> = sqlx::query_as(
            "SELECT identity_json, state FROM failed_cleanup_intents ORDER BY artifact_id",
        )
        .fetch_all(f.database.pool())
        .await?;
        let root = if case.starts_with("logs") {
            &mut f.paths.log_dir
        } else {
            &mut f.paths.state_dir
        };
        *root = if case.ends_with("dot") {
            root.join(".")
        } else {
            root.join("..").join(root.file_name().ok_or("root name")?)
        };
        let attempt = f.paths.log_dir.join(f.attempt_id.to_string());
        sqlx::query(
            "UPDATE attempt_processes SET stdout_path = ?, stderr_path = ? WHERE attempt_id = ?",
        )
        .bind(attempt.join("stdout.log").to_str())
        .bind(attempt.join("stderr.log").to_str())
        .bind(f.attempt_id.to_string())
        .execute(f.database.pool())
        .await?;
        if case.starts_with("state") {
            let staged = f
                .paths
                .state_dir
                .join("artifact-staging")
                .join(f.attempt_id.to_string())
                .join(f.artifact.id.to_string());
            sqlx::query("UPDATE artifacts SET metadata_json = json_set(metadata_json, '$.staged_path', ?) WHERE id = ?")
                .bind(staged.to_str()).bind(f.artifact.id.to_string()).execute(f.database.pool()).await?;
        }
        let sentinel = f.project_root.join("sentinel");
        fs::write(&sentinel, b"live registered bytes")?;
        negative_replay_assert_failed_rejected(&f).await?;
        let after: Vec<(String, String)> = sqlx::query_as(
            "SELECT identity_json, state FROM failed_cleanup_intents ORDER BY artifact_id",
        )
        .fetch_all(f.database.pool())
        .await?;
        assert_eq!(after, intents, "{case}");
        assert_eq!(fs::read(sentinel)?, b"live registered bytes");
    }
    Ok(())
}

async fn race_diagnosis_evidence_counts(
    database: &Database,
) -> Result<(i64, i64, i64, i64), sqlx::Error> {
    sqlx::query_as(
        "SELECT (SELECT COUNT(*) FROM failed_cleanup_intents),
                (SELECT COUNT(*) FROM cleanup_tombstones),
                (SELECT COUNT(*) FROM events WHERE kind = 'cleanup_state_changed'),
                (SELECT COUNT(*) FROM actions)",
    )
    .fetch_one(database.pool())
    .await
}

// Ignore only the two cells deliberately changed by the external writer.
async fn race_diagnosis_sql_snapshot(
    database: &Database,
) -> Result<Vec<(String, Vec<String>)>, Box<dyn Error>> {
    let mut snapshot = gc_support::sql_snapshot(database).await?;
    let columns: Vec<String> = sqlx::query_scalar(
        "SELECT name FROM pragma_table_info('recoveries') WHERE name NOT IN ('state', 'diagnosis_json') ORDER BY cid",
    )
    .fetch_all(database.pool())
    .await?;
    let expression = columns
        .iter()
        .map(|column| format!("quote(\"{column}\")"))
        .collect::<Vec<_>>()
        .join(" || ',' || ");
    let rows = sqlx::query_scalar(&format!("SELECT {expression} FROM recoveries ORDER BY 1"))
        .fetch_all(database.pool())
        .await?;
    snapshot
        .iter_mut()
        .find(|(table, _)| table == "recoveries")
        .ok_or("recoveries snapshot")?
        .1 = rows;
    Ok(snapshot)
}

#[tokio::test]
async fn race_diagnosis_cleanup_waits_for_independent_writer_commit_or_rollback() -> TestResult {
    use std::future::Future;

    for commit in [true, false] {
        let f = FailedCleanupFixture::new().await?;
        f.diagnose().await?;
        // Open both independent clients before acquiring the writer lock.
        let cleanup_database = Database::open(&f.paths.database).await?;
        let mut external = SqliteConnection::connect_with(
            &sqlx::sqlite::SqliteConnectOptions::new().filename(&f.paths.database),
        )
        .await?;
        let before = race_diagnosis_sql_snapshot(&f.database).await?;
        let source = fs::read(&f.source)?;
        let staged = fs::read(&f.staged)?;
        assert_eq!(
            race_diagnosis_evidence_counts(&f.database).await?,
            (0, 0, 0, 0)
        );

        let mut writer = external.begin().await?;
        sqlx::query("UPDATE recoveries SET state = state WHERE attempt_id = ?")
            .bind(f.attempt_id.to_string())
            .execute(&mut *writer)
            .await?;
        assert_eq!(
            sqlx::query("UPDATE recoveries SET state = 'diagnosing', diagnosis_json = NULL WHERE attempt_id = ?")
                .bind(f.attempt_id.to_string()).execute(&mut *writer).await?.rows_affected(),
            1
        );

        let barrier = tokio::sync::Barrier::new(2);
        let (started, pending) = tokio::sync::oneshot::channel();
        let cleanup = async {
            barrier.wait().await;
            let artifacts = cleanup_database.artifacts();
            let mut future =
                std::pin::pin!(artifacts.cleanup_failed_artifacts(f.attempt_id, &f.paths));
            let mut started = Some(started);
            std::future::poll_fn(|cx| {
                let result = future.as_mut().poll(cx);
                if result.is_pending() {
                    if let Some(started) = started.take() {
                        let _ = started.send(());
                    }
                } else {
                    assert!(
                        started.is_none(),
                        "cleanup completed while external writer was held"
                    );
                }
                result
            })
            .await
        };
        let release = async {
            barrier.wait().await;
            pending.await?;
            assert_eq!(fs::read(&f.source)?, source);
            assert_eq!(fs::read(&f.staged)?, staged);
            // The external writer owns the lock first; this is an API-start
            // synchronization point, not a claimed interphase cleanup seam.
            if commit {
                writer.commit().await?;
            } else {
                writer.rollback().await?;
            }
            Ok::<(), Box<dyn Error>>(())
        };
        let (removed, released) = tokio::time::timeout(Duration::from_secs(20), async {
            tokio::join!(cleanup, release)
        })
        .await?;
        released?;
        assert_eq!(removed?, usize::from(!commit), "commit={commit}");

        if commit {
            assert_eq!(fs::read(&f.source)?, source);
            assert_eq!(fs::read(&f.staged)?, staged);
            assert_eq!(
                race_diagnosis_evidence_counts(&f.database).await?,
                (0, 0, 0, 0)
            );
            let recovery: (String, Option<String>) =
                sqlx::query_as("SELECT state, diagnosis_json FROM recoveries WHERE attempt_id = ?")
                    .bind(f.attempt_id.to_string())
                    .fetch_one(f.database.pool())
                    .await?;
            assert_eq!(recovery, ("diagnosing".into(), None));
            assert_eq!(race_diagnosis_sql_snapshot(&f.database).await?, before);
        } else {
            assert!(!f.source.exists() && !f.staged.exists());
            assert_eq!(
                race_diagnosis_evidence_counts(&f.database).await?,
                (1, 1, 1, 0)
            );
            let completed = gc_support::sql_snapshot(&f.database).await?;
            assert_eq!(f.cleanup().await?, 0);
            assert_eq!(
                race_diagnosis_evidence_counts(&f.database).await?,
                (1, 1, 1, 0)
            );
            assert_eq!(gc_support::sql_snapshot(&f.database).await?, completed);
        }
        cleanup_database.pool().close().await;
    }
    Ok(())
}

#[tokio::test]
async fn race_diagnosis_readonly_plan_observes_only_committed_diagnosis() -> TestResult {
    use igor_core::{DatabaseOptions, GcOperation, GcStatus};

    let f = FailedCleanupFixture::new().await?;
    f.diagnose().await?;
    let readonly = Database::open_with_options(
        &f.paths.database,
        DatabaseOptions {
            writable: false,
            create_if_missing: false,
            ..Default::default()
        },
    )
    .await?;
    let mut external = SqliteConnection::connect_with(
        &sqlx::sqlite::SqliteConnectOptions::new().filename(&f.paths.database),
    )
    .await?;
    let original = readonly.gc_plan_filtered(None, &f.paths, true).await?;
    let entry = original
        .entries
        .iter()
        .find(|entry| entry.operation == GcOperation::FailedArtifacts)
        .ok_or("failed entry")?;
    assert_eq!(entry.status, GcStatus::Ready);
    let scope = Some(entry.project_id);
    let original = readonly.gc_plan_filtered(scope, &f.paths, true).await?;
    let before = race_diagnosis_sql_snapshot(&f.database).await?;
    let disk = gc_support::files(f._directory.path(), &f.paths.database)?;
    assert_eq!(
        race_diagnosis_evidence_counts(&f.database).await?,
        (0, 0, 0, 0)
    );

    let mut writer = external.begin().await?;
    sqlx::query("UPDATE recoveries SET state = state WHERE attempt_id = ?")
        .bind(f.attempt_id.to_string())
        .execute(&mut *writer)
        .await?;
    assert_eq!(
        sqlx::query(
            "UPDATE recoveries SET state = 'diagnosing', diagnosis_json = NULL WHERE attempt_id = ?"
        )
        .bind(f.attempt_id.to_string())
        .execute(&mut *writer)
        .await?
        .rows_affected(),
        1
    );
    // Both previews must finish while the substantive diagnosis update is
    // uncommitted, without requesting a writer or seeing its private state.
    for _ in 0..2 {
        let plan = tokio::time::timeout(
            Duration::from_secs(20),
            readonly.gc_plan_filtered(scope, &f.paths, true),
        )
        .await??;
        assert_eq!(plan, original);
    }
    writer.commit().await?;
    let committed = readonly.gc_plan_filtered(scope, &f.paths, true).await?;
    let entry = committed
        .entries
        .iter()
        .find(|entry| entry.operation == GcOperation::FailedArtifacts)
        .ok_or("failed entry")?;
    assert_eq!(entry.status, GcStatus::Deferred);
    assert_eq!(entry.reason, "diagnosis_pending");
    assert!(entry.targets.is_empty());
    assert!(committed.worklist()?.selections.is_empty());
    assert_eq!(
        race_diagnosis_evidence_counts(&f.database).await?,
        (0, 0, 0, 0)
    );
    let recovery: (String, Option<String>) =
        sqlx::query_as("SELECT state, diagnosis_json FROM recoveries WHERE attempt_id = ?")
            .bind(f.attempt_id.to_string())
            .fetch_one(f.database.pool())
            .await?;
    assert_eq!(recovery, ("diagnosing".into(), None));
    assert_eq!(race_diagnosis_sql_snapshot(&f.database).await?, before);
    assert_eq!(
        gc_support::files(f._directory.path(), &f.paths.database)?,
        disk
    );
    readonly.pool().close().await;
    Ok(())
}

#[tokio::test]
async fn gc_execute_failed_second_phase_rechecks_new_extraction_barrier() -> TestResult {
    let f = FailedCleanupFixture::new().await?;
    f.diagnose().await?;
    let worklist = f
        .database
        .gc_plan_filtered(None, &f.paths, true)
        .await?
        .worklist()?;
    sqlx::raw_sql("CREATE TRIGGER extraction_after_intent AFTER INSERT ON failed_cleanup_intents BEGIN INSERT INTO actions(id, project_id, job_id, attempt_id, kind, state, spec_json, idempotency_key, available_at) SELECT 'new-extraction', project_id, job_id, id, 'extract_metrics', 'pending', '{}', 'new-extraction', '2026-01-01' FROM attempts WHERE id = (SELECT attempt_id FROM artifacts WHERE id = NEW.artifact_id); END;").execute(f.database.pool()).await?;
    let result = f
        .database
        .gc_execute(None, true, worklist, &f.paths)
        .await?;
    assert_eq!(result.completed, 0);
    assert_eq!(result.deferred, 1);
    assert!(f.source.is_file() && f.staged.is_file());
    let tombstones: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM cleanup_tombstones")
        .fetch_one(f.database.pool())
        .await?;
    assert_eq!(tombstones, 0);
    Ok(())
}

#[tokio::test]
async fn gc_execute_initial_extraction_deferred_stays_unselected_after_completion() -> TestResult {
    let f = FailedCleanupFixture::new().await?;
    f.diagnose().await?;
    sqlx::query("INSERT INTO actions(id, project_id, job_id, attempt_id, kind, state, spec_json, idempotency_key, available_at) SELECT 'extract', project_id, job_id, id, 'extract_metrics', 'pending', '{}', 'extract', '2026-01-01' FROM attempts WHERE id = ?").bind(f.attempt_id.to_string()).execute(f.database.pool()).await?;
    let worklist = f
        .database
        .gc_plan_filtered(None, &f.paths, true)
        .await?
        .worklist()?;
    assert!(worklist.selections.is_empty());
    sqlx::query("UPDATE actions SET state = 'succeeded'")
        .execute(f.database.pool())
        .await?;
    let before = gc_support::sql_snapshot(&f.database).await?;
    assert!(
        f.database
            .gc_execute(None, true, worklist, &f.paths)
            .await?
            .entries
            .is_empty()
    );
    assert_eq!(gc_support::sql_snapshot(&f.database).await?, before);
    assert!(f.source.is_file() && f.staged.is_file());
    Ok(())
}

#[tokio::test]
async fn gc_execute_failed_fixed_batch_ignores_new_sorted_rows_and_replays_once() -> TestResult {
    let f = FailedCleanupFixture::new().await?;
    f.diagnose().await?;
    sqlx::query("UPDATE artifacts SET lifecycle_state = 'retained'")
        .execute(f.database.pool())
        .await?;
    for i in 1..=65u128 {
        let mut a = f.artifact.clone();
        a.id = uuid::Uuid::from_u128(i);
        a.path = format!("selected-{i}.bin");
        a.state = igor_core::ArtifactPublicationState::Registered;
        a.metadata = json!({});
        fs::copy(&f.source, f.source.with_file_name(&a.path))?;
        f.database.artifacts().insert(&a).await?;
    }
    let plan = f.database.gc_plan_filtered(None, &f.paths, true).await?;
    assert_eq!(plan.pages.len(), 3);
    let worklist = plan.worklist()?;
    assert_eq!(worklist.selections.len(), 64);
    let mut new = f.artifact.clone();
    new.id = uuid::Uuid::nil();
    new.path = "new-before-confirmation.bin".into();
    new.state = igor_core::ArtifactPublicationState::Registered;
    new.metadata = json!({});
    let new_path = f.source.with_file_name(&new.path);
    fs::copy(&f.source, &new_path)?;
    f.database.artifacts().insert(&new).await?;
    let result = f
        .database
        .gc_execute(None, true, worklist.clone(), &f.paths)
        .await?;
    assert_eq!(result.completed, 64, "{result:?}");
    assert_eq!(result.deferred + result.errors + result.unsafe_count, 0);
    assert!(new_path.is_file());
    assert!(f.source.with_file_name("selected-65.bin").is_file());
    let replay = f
        .database
        .gc_execute(None, true, worklist, &f.paths)
        .await?;
    assert_eq!(replay.already_done, 64);
    let tombstones: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM cleanup_tombstones")
        .fetch_one(f.database.pool())
        .await?;
    assert_eq!(tombstones, 64);
    let actions: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM actions WHERE kind = 'cleanup_failed_artifacts'")
            .fetch_one(f.database.pool())
            .await?;
    assert_eq!(actions, 0);
    Ok(())
}

#[tokio::test]
async fn gc_execute_initial_deferred_is_not_future_authorization() -> TestResult {
    let f = FailedCleanupFixture::new().await?;
    let initial = f.database.gc_plan_filtered(None, &f.paths, true).await?;
    assert!(initial.worklist()?.selections.is_empty());
    f.diagnose().await?;
    let before = gc_support::sql_snapshot(&f.database).await?;
    let disk = gc_support::files(f._directory.path(), &f.paths.database)?;
    let result = f
        .database
        .gc_execute(None, true, initial.worklist()?, &f.paths)
        .await?;
    assert!(result.entries.is_empty());
    assert_eq!(gc_support::sql_snapshot(&f.database).await?, before);
    assert_eq!(
        gc_support::files(f._directory.path(), &f.paths.database)?,
        disk
    );
    Ok(())
}

#[tokio::test]
async fn gc_execute_failed_revalidates_consent_paths_policy_barriers_and_bytes() -> TestResult {
    for change in [
        "path",
        "policy",
        "extraction",
        "diagnosis",
        "bytes",
        "expected_path",
        "unregistered",
    ] {
        let f = FailedCleanupFixture::new().await?;
        f.diagnose().await?;
        let mut worklist = f
            .database
            .gc_plan_filtered(None, &f.paths, true)
            .await?
            .worklist()?;
        match change {
            "path" => {
                sqlx::query("UPDATE artifacts SET path = 'new-path.bin'")
                    .execute(f.database.pool())
                    .await?;
                fs::copy(&f.source, f.source.with_file_name("new-path.bin"))?;
            }
            "policy" => {
                sqlx::query("DROP TRIGGER attempts_spec_immutable")
                    .execute(f.database.pool())
                    .await?;
                sqlx::query("UPDATE attempts SET spec_json = json_set(spec_json, '$.result.failed_artifact_retention.policy', 'keep')").execute(f.database.pool()).await?;
            }
            "extraction" => {
                sqlx::query("INSERT INTO actions(id, project_id, job_id, attempt_id, kind, state, spec_json, idempotency_key, available_at) SELECT ?, project_id, job_id, id, 'extract_metrics', 'pending', '{}', ?, '2026-01-01' FROM attempts WHERE id = ?").bind(uuid::Uuid::new_v4().to_string()).bind(uuid::Uuid::new_v4().to_string()).bind(f.attempt_id.to_string()).execute(f.database.pool()).await?;
            }
            "diagnosis" => {
                sqlx::query("UPDATE recoveries SET state = 'pending'")
                    .execute(f.database.pool())
                    .await?;
            }
            "bytes" => fs::write(&f.source, b"changed identity")?,
            "expected_path" => {
                worklist.selections[0].expected.targets[0].path =
                    f.source.with_file_name("arbitrary-client-path")
            }
            "unregistered" => {
                sqlx::query("UPDATE projects SET registered = 0")
                    .execute(f.database.pool())
                    .await?;
            }
            _ => unreachable!(),
        }
        let before = gc_support::sql_snapshot(&f.database).await?;
        let disk = gc_support::files(f._directory.path(), &f.paths.database)?;
        let result = f
            .database
            .gc_execute(None, true, worklist, &f.paths)
            .await?;
        assert_eq!(result.completed, 0, "{change}: {result:?}");
        assert_eq!(
            gc_support::sql_snapshot(&f.database).await?,
            before,
            "{change}"
        );
        assert_eq!(
            gc_support::files(f._directory.path(), &f.paths.database)?,
            disk,
            "{change}"
        );
    }
    Ok(())
}

#[tokio::test]
async fn gc_execute_failed_event_rollback_recovers_prepared_intent_without_duplicate_actions()
-> TestResult {
    let f = FailedCleanupFixture::new().await?;
    f.diagnose().await?;
    let worklist = f
        .database
        .gc_plan_filtered(None, &f.paths, true)
        .await?
        .worklist()?;
    sqlx::raw_sql("CREATE TRIGGER reject_gc_event BEFORE INSERT ON events WHEN NEW.kind = 'cleanup_state_changed' BEGIN SELECT RAISE(ABORT, 'event failure'); END;").execute(f.database.pool()).await?;
    let result = f
        .database
        .gc_execute(None, true, worklist, &f.paths)
        .await?;
    assert_eq!(result.errors, 1, "{result:?}");
    assert!(!f.source.exists() && !f.staged.exists());
    sqlx::query("DROP TRIGGER reject_gc_event")
        .execute(f.database.pool())
        .await?;
    let recovery = f
        .database
        .gc_plan_filtered(None, &f.paths, true)
        .await?
        .worklist()?;
    let result = f
        .database
        .gc_execute(None, true, recovery.clone(), &f.paths)
        .await?;
    assert_eq!(result.completed, 1, "{result:?}");
    assert_eq!(
        f.database
            .gc_execute(None, true, recovery, &f.paths)
            .await?
            .already_done,
        1
    );
    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM cleanup_tombstones")
        .fetch_one(f.database.pool())
        .await?;
    assert_eq!(count, 1);
    Ok(())
}

// Full-row projection of the durable failed-cleanup decision, including the
// immutable decided_at/prepared_at identity columns.
async fn reopen_intent_rows_failed(database: &Database) -> Result<Vec<String>, Box<dyn Error>> {
    Ok(sqlx::query_scalar(
        "SELECT json_object('artifact_id', artifact_id, 'schema_version', schema_version, \
         'identity_json', identity_json, 'state', state, 'decided_at', decided_at, \
         'prepared_at', prepared_at) \
         FROM failed_cleanup_intents ORDER BY artifact_id",
    )
    .fetch_all(database.pool())
    .await?)
}

#[tokio::test]
async fn gc_execute_failed_event_rollback_recovers_prepared_intent_after_pool_reopen() -> TestResult
{
    let mut f = FailedCleanupFixture::new().await?;
    f.diagnose().await?;
    // A registered but retained artifact stays outside the saved selection.
    let mut keep = f.artifact.clone();
    keep.id = uuid::Uuid::new_v4();
    keep.path = "retained-outside.bin".into();
    keep.state = igor_core::ArtifactPublicationState::Registered;
    keep.lifecycle = igor_core::ArtifactLifecycleState::Retained;
    keep.metadata = json!({});
    let keep_path = f.source.with_file_name(&keep.path);
    fs::copy(&f.source, &keep_path)?;
    f.database.artifacts().insert(&keep).await?;
    let saved = f
        .database
        .gc_plan_filtered(None, &f.paths, true)
        .await?
        .worklist()?;
    assert_eq!(saved.selections.len(), 1);
    // Real unlinks happen; the event insert fails and rolls back completion.
    sqlx::raw_sql("CREATE TRIGGER reject_gc_event BEFORE INSERT ON events WHEN NEW.kind = 'cleanup_state_changed' BEGIN SELECT RAISE(ABORT, 'event failure'); END;").execute(f.database.pool()).await?;
    let faulted = f
        .database
        .gc_execute(None, true, saved.clone(), &f.paths)
        .await?;
    assert_eq!(faulted.errors, 1, "{faulted:?}");
    assert!(!f.source.exists() && !f.staged.exists());
    assert!(keep_path.is_file());
    assert_eq!(
        race_diagnosis_evidence_counts(&f.database).await?,
        (1, 0, 0, 0)
    );
    let prepared = reopen_intent_rows_failed(&f.database).await?;
    assert_eq!(prepared.len(), 1);
    let persisted = gc_support::sql_snapshot(&f.database).await?;

    // Close the pool, then reopen the same database file.
    f.database.pool().close().await;
    f.database = Database::open(&f.paths.database).await?;
    assert_eq!(
        gc_support::sql_snapshot(&f.database).await?,
        persisted,
        "prepared intent must survive the pool reopen"
    );
    assert_eq!(reopen_intent_rows_failed(&f.database).await?, prepared);
    assert_eq!(
        race_diagnosis_evidence_counts(&f.database).await?,
        (1, 0, 0, 0)
    );
    assert!(!f.source.exists() && !f.staged.exists());
    assert!(keep_path.is_file());
    // The immutable decision identity is still enforced after the reopen.
    assert!(
        sqlx::query("UPDATE failed_cleanup_intents SET decided_at = '2000-01-01T00:00:00.000Z'")
            .execute(f.database.pool())
            .await
            .is_err()
    );
    assert!(
        sqlx::query("DELETE FROM failed_cleanup_intents")
            .execute(f.database.pool())
            .await
            .is_err()
    );

    // The diagnosis barrier is rechecked on the reopened database.
    sqlx::query("DROP TRIGGER reject_gc_event")
        .execute(f.database.pool())
        .await?;
    let diagnosis: String = sqlx::query_scalar("SELECT state FROM recoveries")
        .fetch_one(f.database.pool())
        .await?;
    sqlx::query("UPDATE recoveries SET state = 'pending'")
        .execute(f.database.pool())
        .await?;
    let blocked_snapshot = gc_support::sql_snapshot(&f.database).await?;
    let blocked_disk = gc_support::files(f._directory.path(), &f.paths.database)?;
    let blocked = f
        .database
        .gc_execute(None, true, saved.clone(), &f.paths)
        .await?;
    assert_eq!(blocked.completed, 0, "{blocked:?}");
    assert_eq!(
        gc_support::sql_snapshot(&f.database).await?,
        blocked_snapshot,
        "{diagnosis}"
    );
    assert_eq!(
        gc_support::files(f._directory.path(), &f.paths.database)?,
        blocked_disk
    );
    sqlx::query("UPDATE recoveries SET state = ?")
        .bind(&diagnosis)
        .execute(f.database.pool())
        .await?;

    // A row created after the fault is not authorized by the saved worklist.
    let mut late = f.artifact.clone();
    late.id = uuid::Uuid::new_v4();
    late.path = "new-after-fault.bin".into();
    late.state = igor_core::ArtifactPublicationState::Registered;
    late.metadata = json!({});
    let late_path = f.source.with_file_name(&late.path);
    fs::copy(&keep_path, &late_path)?;
    f.database.artifacts().insert(&late).await?;

    let resumed = f
        .database
        .gc_execute(None, true, saved.clone(), &f.paths)
        .await?;
    assert_eq!(resumed.completed, 1, "{resumed:?}");
    assert_eq!(
        resumed.errors + resumed.deferred + resumed.unsafe_count,
        0,
        "{resumed:?}"
    );
    assert!(!f.source.exists() && !f.staged.exists());
    assert!(keep_path.is_file(), "unselected retained copy must survive");
    assert!(
        late_path.is_file(),
        "rows after the fault need a new preview"
    );

    let tombstones: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM cleanup_tombstones")
        .fetch_one(f.database.pool())
        .await?;
    assert_eq!(tombstones, 1);
    let events: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM events WHERE kind = 'cleanup_state_changed'")
            .fetch_one(f.database.pool())
            .await?;
    assert_eq!(events, 1);
    let actions: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM actions WHERE kind = 'cleanup_failed_artifacts'")
            .fetch_one(f.database.pool())
            .await?;
    assert_eq!(actions, 0);

    let completed = reopen_intent_rows_failed(&f.database).await?;
    assert_eq!(completed.len(), 1);
    let before_value: serde_json::Value = serde_json::from_str(&prepared[0])?;
    let completed_value: serde_json::Value = serde_json::from_str(&completed[0])?;
    assert_eq!(completed_value["state"], "completed");
    assert_eq!(
        completed_value["identity_json"],
        before_value["identity_json"]
    );
    assert_eq!(completed_value["decided_at"], before_value["decided_at"]);
    assert_eq!(completed_value["prepared_at"], before_value["prepared_at"]);
    let evidence = f
        .database
        .cleanup_tombstone("failed_artifact_retention", &f.artifact.id.to_string())
        .await?
        .ok_or("failed tombstone")?;
    assert_eq!(evidence["decided_at"], before_value["decided_at"]);

    // Replaying the saved worklist changes neither SQL nor files.
    let snapshot = gc_support::sql_snapshot(&f.database).await?;
    let disk = gc_support::files(f._directory.path(), &f.paths.database)?;
    let replay = f.database.gc_execute(None, true, saved, &f.paths).await?;
    assert_eq!(replay.already_done, 1, "{replay:?}");
    assert_eq!(gc_support::sql_snapshot(&f.database).await?, snapshot);
    assert_eq!(
        gc_support::files(f._directory.path(), &f.paths.database)?,
        disk
    );
    assert!(keep_path.is_file() && late_path.is_file());
    Ok(())
}

#[tokio::test]
async fn gc_failed_preview_attributes_copy_bytes_by_exact_path_not_basename() -> TestResult {
    use igor_core::GcOperation as O;
    let f = FailedCleanupFixture::new().await?;
    f.diagnose().await?;
    let mut other = f.artifact.clone();
    other.id = uuid::Uuid::new_v4();
    other.path = f.artifact.id.to_string();
    other.state = igor_core::ArtifactPublicationState::Registered;
    other.metadata = json!({});
    let source = f.source.parent().ok_or("parent")?.join(&other.path);
    fs::copy(&f.source, &source)?;
    f.database.artifacts().insert(&other).await?;
    let plan = gc_support::readonly_plan(&f.database, &f.paths, f._directory.path(), None).await?;
    let first = plan
        .entries
        .iter()
        .find(|e| e.operation == O::FailedArtifacts && e.subject_id == f.artifact.id.to_string())
        .ok_or("first")?;
    assert_eq!(first.targets.len(), 2);
    assert!(!first.targets.iter().any(|t| t.path == source));
    let second = plan
        .entries
        .iter()
        .find(|e| e.operation == O::FailedArtifacts && e.subject_id == other.id.to_string())
        .ok_or("second")?;
    assert_eq!(second.targets.len(), 1);
    assert_eq!(second.targets[0].path, source);
    assert_eq!(
        plan.known_copy_bytes,
        f.artifact.size_bytes.ok_or("bytes")? * 3
    );
    assert_eq!(f.cleanup().await?, 2);
    Ok(())
}

#[tokio::test]
async fn gc_failed_preview_barriers_precede_keep_and_missing_intent_replay() -> TestResult {
    use igor_core::{GcOperation as O, GcStatus as S};
    let f =
        FailedCleanupFixture::with_snapshot(json!({"policy":"keep","keep_on_failure":[]})).await?;
    f.diagnose().await?;
    sqlx::query("UPDATE attempts SET state = 'failed'")
        .execute(f.database.pool())
        .await?;
    let plan = f.database.gc_plan(None, &f.paths).await?;
    assert_eq!(
        plan.entries
            .iter()
            .find(|e| e.operation == O::FailedArtifacts)
            .ok_or("entry")?
            .reason,
        "complete_failed_tails_pending"
    );
    let g = FailedCleanupFixture::new().await?;
    g.diagnose().await?;
    sqlx::query("INSERT INTO actions(id, project_id, job_id, attempt_id, kind, state, spec_json, idempotency_key, available_at) SELECT ?, project_id, job_id, id, 'extract_metrics', 'pending', '{}', ?, '2026-01-01' FROM attempts WHERE id = ?")
        .bind(uuid::Uuid::new_v4().to_string()).bind(uuid::Uuid::new_v4().to_string()).bind(g.attempt_id.to_string()).execute(g.database.pool()).await?;
    let plan = gc_support::readonly_plan(&g.database, &g.paths, g._directory.path(), None).await?;
    assert_eq!(
        plan.entries
            .iter()
            .find(|e| e.operation == O::FailedArtifacts)
            .ok_or("entry")?
            .reason,
        "active_extraction"
    );
    sqlx::query("UPDATE actions SET state = 'failed'")
        .execute(g.database.pool())
        .await?;
    sqlx::raw_sql("CREATE TRIGGER defer_prepared AFTER INSERT ON failed_cleanup_intents BEGIN UPDATE projects SET registered = 0; END;").execute(g.database.pool()).await?;
    assert_eq!(g.cleanup().await?, 0);
    let global = g.database.gc_plan(None, &g.paths).await?;
    assert_eq!(
        global
            .entries
            .iter()
            .find(|e| e.operation == O::FailedArtifacts)
            .ok_or("entry")?
            .reason,
        "project_unregistered"
    );
    let project_id = global.entries[0].project_id;
    assert!(matches!(
        g.database.gc_plan(Some(project_id), &g.paths).await,
        Err(PersistenceError::NotFound { entity: "project" })
    ));
    fs::remove_file(&g.source)?;
    fs::remove_file(&g.staged)?;
    sqlx::raw_sql("DROP TRIGGER defer_prepared; UPDATE projects SET registered = 1;")
        .execute(g.database.pool())
        .await?;
    let plan = gc_support::readonly_plan(&g.database, &g.paths, g._directory.path(), None).await?;
    assert_eq!(
        plan.entries
            .iter()
            .find(|e| e.operation == O::FailedArtifacts)
            .ok_or("entry")?
            .status,
        S::MetadataOnly
    );
    assert_eq!(plan.known_copy_bytes, 0);
    assert!(plan.entries.iter().all(|e| e.targets.is_empty()));
    let before = gc_support::files(g._directory.path(), &g.paths.database)?;
    let result = g
        .database
        .gc_execute(None, false, plan.worklist()?, &g.paths)
        .await?;
    assert_eq!(
        gc_support::files(g._directory.path(), &g.paths.database)?,
        before
    );
    let (actual, expected) = failed_preview_execution(&g, &plan, result, true)?;
    assert_eq!(actual, expected);
    Ok(())
}

#[tokio::test]
async fn gc_failed_preview_bounds_subjects_and_artifact_batches_without_partial_removal()
-> TestResult {
    use igor_core::{GcOperation as O, GcStatus as S};
    let f = FailedCleanupFixture::with_snapshot(
        json!({"policy":"delete-after-diagnosis", "keep_on_failure":["copy-1.bin"]}),
    )
    .await?;
    f.diagnose().await?;
    // Deterministic IDs put a corrupt copy at the end of the first batch and
    // an absent required copy outside it. No other member may advertise removal.
    // Keep the fixture's staged copy outside this batch.
    sqlx::query("UPDATE artifacts SET lifecycle_state = 'retained'")
        .execute(f.database.pool())
        .await?;
    let bytes = fs::read(&f.source)?;
    for i in 1..=65u128 {
        let mut artifact = f.artifact.clone();
        artifact.id = uuid::Uuid::from_u128(i);
        artifact.path = format!("copy-{i}.bin");
        artifact.state = igor_core::ArtifactPublicationState::Registered;
        artifact.metadata = json!({});
        if i < 65 {
            fs::write(
                f.source.parent().ok_or("parent")?.join(&artifact.path),
                if i == 64 {
                    b"corrupt".as_slice()
                } else {
                    &bytes
                },
            )?;
        }
        f.database.artifacts().insert(&artifact).await?;
    }
    let plan = gc_support::readonly_plan(&f.database, &f.paths, f._directory.path(), None).await?;
    let entries: Vec<_> = plan
        .entries
        .iter()
        .filter(|e| e.operation == O::FailedArtifacts)
        .collect();
    assert_eq!(entries.len(), 64);
    assert!(
        entries
            .iter()
            .all(|e| e.status == S::Unsafe && e.targets.is_empty())
    );
    let page = plan
        .pages
        .iter()
        .find(|p| p.operation == O::FailedArtifacts)
        .ok_or("page")?;
    assert!(page.artifact_batches[0].truncated);
    assert_eq!(
        page.artifact_batches[0].continuation_after,
        Some(uuid::Uuid::from_u128(64).to_string())
    );
    assert!(f.cleanup().await.is_err());
    fs::write(
        f.source.parent().ok_or("parent")?.join("copy-64.bin"),
        &bytes,
    )?;
    let safe = f.database.gc_plan(None, &f.paths).await?;
    let safe: Vec<_> = safe
        .entries
        .iter()
        .filter(|e| e.operation == O::FailedArtifacts)
        .collect();
    assert_eq!(safe.len(), 64);
    assert_eq!(safe[0].status, S::Retain);
    assert!(safe[1..].iter().all(|e| e.status == S::Ready));
    // 33 subjects with a frozen legacy policy remain visible despite discovery filters.
    let original: (String, String) =
        sqlx::query_as("SELECT job_id, spec_json FROM attempts WHERE id = ?")
            .bind(f.attempt_id.to_string())
            .fetch_one(f.database.pool())
            .await?;
    let project = entries[0].project_id;
    for i in 1..=33u128 {
        let id = uuid::Uuid::from_u128(i + 1000).to_string();
        let mut spec: serde_json::Value = serde_json::from_str(&original.1)?;
        spec["id"] = json!(id);
        spec["result"]["failed_artifact_retention"] = serde_json::Value::Null;
        sqlx::query("INSERT INTO attempts(id, job_id, project_id, sequence, state, spec_json) VALUES (?, ?, ?, ?, 'failed', ?)").bind(&id).bind(&original.0).bind(project.to_string()).bind(i as i64 + 1).bind(spec.to_string()).execute(f.database.pool()).await?;
        sqlx::query("INSERT INTO artifacts(id, attempt_id, path, role, size_bytes, sha256, state, lifecycle_state, diagnosis_required, metadata_json) VALUES (?, ?, 'legacy.bin', 'model', 0, ?, 'registered', 'failed', 1, '{}')").bind(uuid::Uuid::new_v4().to_string()).bind(&id).bind(format!("sha256:{}", "0".repeat(64))).execute(f.database.pool()).await?;
    }
    let plan = f.database.gc_plan(Some(project), &f.paths).await?;
    let page = plan
        .pages
        .iter()
        .find(|p| p.operation == O::FailedArtifacts)
        .ok_or("page")?;
    assert_eq!(page.subjects_observed, 32);
    assert!(page.truncated);
    assert!(page.continuation_after.is_some());
    assert_eq!(
        plan.entries
            .iter()
            .filter(|e| e.operation == O::FailedArtifacts)
            .count(),
        32
    );
    Ok(())
}

#[tokio::test]
async fn gc_failed_preview_is_read_only_and_matches_executor() -> TestResult {
    use igor_core::{GcOperation as O, GcStatus as S};
    let f = FailedCleanupFixture::new().await?;
    let plan = gc_support::readonly_plan(&f.database, &f.paths, f._directory.path(), None).await?;
    let entry = plan
        .entries
        .iter()
        .find(|e| e.operation == O::FailedArtifacts)
        .ok_or("entry")?;
    assert_eq!(entry.reason, "diagnosis_pending");
    assert_eq!(entry.status, S::Deferred);
    f.diagnose().await?;
    let plan = gc_support::readonly_plan(&f.database, &f.paths, f._directory.path(), None).await?;
    let entry = plan
        .entries
        .iter()
        .find(|e| e.operation == O::FailedArtifacts)
        .ok_or("entry")?;
    assert_eq!(entry.status, S::Ready);
    let expected: std::collections::BTreeMap<_, _> = [f.source.clone(), f.staged.clone()]
        .into_iter()
        .map(|path| Ok((path.clone(), Some(fs::read(path)?.len() as u64))))
        .collect::<Result<_, std::io::Error>>()?;
    let paths: std::collections::BTreeMap<_, _> = entry
        .targets
        .iter()
        .map(|t| (t.path.clone(), t.bytes))
        .collect();
    assert_eq!(entry.targets.len(), expected.len());
    assert_eq!(paths, expected);
    assert!(
        entry
            .targets
            .iter()
            .all(|t| t.effect == igor_core::GcEffect::Remove)
    );
    assert_eq!(
        plan.known_copy_bytes,
        expected.values().flatten().sum::<u64>()
    );
    let before = gc_support::files(f._directory.path(), &f.paths.database)?;
    let result = f
        .database
        .gc_execute(None, false, plan.worklist()?, &f.paths)
        .await?;
    let after = gc_support::files(f._directory.path(), &f.paths.database)?;
    assert_eq!(
        after,
        before
            .into_iter()
            .filter(|(path, _, _)| !expected.contains_key(path))
            .collect::<Vec<_>>()
    );
    let (actual, expected) = failed_preview_execution(&f, &plan, result, true)?;
    assert_eq!(actual, expected);
    Ok(())
}

#[tokio::test]
async fn gc_failed_preview_keep_legacy_missing_corrupt_and_symlink() -> TestResult {
    use igor_core::{GcOperation as O, GcStatus as S};
    let mut actual_results = vec![];
    let mut expected_results = vec![];
    for case in ["keep", "legacy", "missing", "corrupt", "symlink"] {
        let snapshot = match case {
            "keep" => json!({"policy":"keep","keep_on_failure":[]}),
            "legacy" => serde_json::Value::Null,
            _ => json!({"policy":"delete-after-diagnosis","keep_on_failure":[]}),
        };
        let f = FailedCleanupFixture::with_snapshot(snapshot).await?;
        f.diagnose().await?;
        match case {
            "missing" => fs::remove_file(&f.source)?,
            "corrupt" => fs::write(&f.staged, b"corrupt")?,
            "symlink" => {
                fs::remove_file(&f.source)?;
                std::os::unix::fs::symlink(&f.staged, &f.source)?;
            }
            _ => (),
        }
        let plan =
            gc_support::readonly_plan(&f.database, &f.paths, f._directory.path(), None).await?;
        let entry = plan
            .entries
            .iter()
            .find(|e| e.operation == O::FailedArtifacts)
            .ok_or("entry")?;
        assert_eq!(
            entry.status,
            if matches!(case, "keep" | "legacy") {
                S::Retain
            } else {
                S::Unsafe
            },
            "{case}"
        );
        assert!(entry.targets.is_empty());
        assert_eq!(plan.known_copy_bytes, 0);
        let before = gc_support::files(f._directory.path(), &f.paths.database)?;
        let result = f
            .database
            .gc_execute(None, false, plan.worklist()?, &f.paths)
            .await?;
        // Unsafe preview subjects are not authorized; retained subjects are completed decisions.
        assert_eq!(
            gc_support::files(f._directory.path(), &f.paths.database)?,
            before,
            "{case}"
        );
        let (actual, expected) =
            failed_preview_execution(&f, &plan, result, matches!(case, "keep" | "legacy"))?;
        actual_results.push((case, actual));
        expected_results.push((case, expected));
    }
    assert_eq!(actual_results, expected_results);
    Ok(())
}

#[tokio::test]
async fn gc_failed_preview_retained_decisions_revalidate_saved_consent() -> TestResult {
    use igor_core::{GcExecutionStatus as X, GcOperation as O, GcStatus as S};
    let f = FailedCleanupFixture::with_snapshot(serde_json::Value::Null).await?;
    f.diagnose().await?;
    let plan = gc_support::readonly_plan(&f.database, &f.paths, f._directory.path(), None).await?;
    assert_eq!(plan.entries.len(), 3);
    assert!(plan.entries.iter().all(|e| e.status == S::Retain));
    let worklist = plan.worklist()?;
    let before = gc_support::sql_snapshot(&f.database).await?;
    let files = gc_support::files(f._directory.path(), &f.paths.database)?;
    let result = f
        .database
        .gc_execute(None, false, worklist.clone(), &f.paths)
        .await?;
    assert_eq!((result.completed, result.deferred), (3, 0));
    assert!(result.entries.iter().all(|e| e.status == X::Completed));
    assert_eq!(gc_support::sql_snapshot(&f.database).await?, before);
    assert_eq!(
        gc_support::files(f._directory.path(), &f.paths.database)?,
        files
    );

    // Frozen consent must not become authorization for a newly enabled cleanup.
    sqlx::query("DROP TRIGGER attempts_spec_immutable")
        .execute(f.database.pool())
        .await?;
    sqlx::query("UPDATE attempts SET spec_json = json_set(spec_json, '$.result.failed_artifact_retention', json('{\"policy\":\"delete-after-diagnosis\",\"keep_on_failure\":[]}')) WHERE id = ?")
        .bind(f.attempt_id.to_string()).execute(f.database.pool()).await?;
    let changed = gc_support::sql_snapshot(&f.database).await?;
    let result = f
        .database
        .gc_execute(None, false, worklist, &f.paths)
        .await?;
    assert_eq!(
        (result.completed, result.already_done, result.deferred),
        (0, 0, 3)
    );
    assert!(result.entries.iter().all(|e| e.status == X::Deferred));
    assert!(
        result
            .entries
            .iter()
            .any(|e| e.operation == O::FailedArtifacts)
    );
    assert_eq!(gc_support::sql_snapshot(&f.database).await?, changed);
    assert_eq!(
        gc_support::files(f._directory.path(), &f.paths.database)?,
        files
    );
    Ok(())
}

fn failed_preview_execution(
    f: &FailedCleanupFixture,
    plan: &igor_core::GcPlan,
    mut actual: igor_core::GcExecution,
    artifact_selected: bool,
) -> Result<(igor_core::GcExecution, igor_core::GcExecution), Box<dyn Error>> {
    use igor_core::{GcExecution, GcExecutionEntry, GcExecutionStatus as X, GcOperation as O};
    for stream in ["stdout", "stderr"] {
        let entry = plan
            .entries
            .iter()
            .find(|e| {
                e.operation == O::SuccessLogCompression
                    && e.subject_id == format!("{}:{stream}", f.attempt_id)
            })
            .ok_or("fixture log entry")?;
        assert_eq!(entry.status, igor_core::GcStatus::Retain);
        assert!(entry.targets.is_empty());
    }
    let mut entries = ["stdout", "stderr"]
        .into_iter()
        .map(|stream| GcExecutionEntry {
            operation: O::SuccessLogCompression,
            subject_id: format!("{}:{stream}", f.attempt_id),
            status: X::Completed,
            cause: None,
        })
        .collect::<Vec<_>>();
    if artifact_selected {
        entries.push(GcExecutionEntry {
            operation: O::FailedArtifacts,
            subject_id: f.artifact.id.to_string(),
            status: X::Completed,
            cause: None,
        });
    }
    entries.sort_by(|a, b| a.subject_id.cmp(&b.subject_id));
    actual
        .entries
        .sort_by(|a, b| a.subject_id.cmp(&b.subject_id));
    Ok((
        actual,
        GcExecution {
            confirmed: true,
            completed: entries.len(),
            entries,
            ..Default::default()
        },
    ))
}

#[tokio::test]
async fn failed_cleanup_rejects_unsupported_or_mismatched_action_spec() -> TestResult {
    for spec in [
        json!({"schema_version":2}),
        json!({"schema_version":1,"attempt_id":AttemptId::new()}),
    ] {
        let f = FailedCleanupFixture::new().await?;
        f.diagnose().await?;
        let action = f
            .database
            .actions()
            .enqueue_failed_cleanup(f.attempt_id)
            .await?;
        sqlx::query("UPDATE actions SET spec_json = ? WHERE id = ?")
            .bind(spec.to_string())
            .bind(action.to_string())
            .execute(f.database.pool())
            .await?;
        let claim = f
            .database
            .actions()
            .claim_failed_cleanup("version-test", Duration::from_secs(30))
            .await?
            .ok_or("missing claim")?;
        assert!(
            f.database
                .artifacts()
                .cleanup_failed_artifacts_claimed(&claim, &f.paths)
                .await
                .is_err()
        );
        assert!(f.source.is_file() && f.staged.is_file());
        let intents: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM failed_cleanup_intents")
            .fetch_one(f.database.pool())
            .await?;
        assert_eq!(intents, 0);
    }
    Ok(())
}

#[tokio::test]
async fn failed_cleanup_prepared_copy_still_requires_matching_bytes() -> TestResult {
    let f = FailedCleanupFixture::new().await?;
    f.diagnose().await?;
    sqlx::raw_sql("CREATE TRIGGER defer_prepared AFTER INSERT ON failed_cleanup_intents BEGIN UPDATE projects SET registered = 0; END;")
        .execute(f.database.pool()).await?;
    assert_eq!(f.cleanup().await?, 0);
    let original = fs::read(&f.source)?;
    fs::write(&f.source, b"tampered after preparation")?;
    sqlx::raw_sql("DROP TRIGGER defer_prepared; UPDATE projects SET registered = 1;")
        .execute(f.database.pool())
        .await?;
    assert!(f.cleanup().await.is_err());
    assert_eq!(fs::read(&f.source)?, b"tampered after preparation");
    assert_eq!(fs::read(&f.staged)?, original);
    assert!(
        f.database
            .cleanup_tombstone("failed_artifact_retention", &f.artifact.id.to_string())
            .await?
            .is_none()
    );
    Ok(())
}

#[tokio::test]
async fn failed_cleanup_renews_claim_after_long_preparation_and_deletion() -> TestResult {
    let f = FailedCleanupFixture::new().await?;
    f.diagnose().await?;
    f.database
        .actions()
        .enqueue_failed_cleanup(f.attempt_id)
        .await?;
    let claim = f
        .database
        .actions()
        .claim_failed_cleanup("old", Duration::from_secs(30))
        .await?
        .ok_or("claim")?;
    sqlx::raw_sql(&format!("CREATE TRIGGER expire_prepared AFTER INSERT ON failed_cleanup_intents BEGIN UPDATE actions SET claim_expires_at = '2000-01-01' WHERE id = '{}'; END;
        CREATE TRIGGER expire_tombstone AFTER INSERT ON cleanup_tombstones BEGIN UPDATE actions SET claim_expires_at = '2000-01-01' WHERE id = '{}'; END;
        CREATE TRIGGER expire_event AFTER INSERT ON events WHEN NEW.kind = 'cleanup_state_changed' BEGIN UPDATE actions SET claim_expires_at = '2000-01-01' WHERE id = '{}'; END;",
        claim.record_id, claim.record_id, claim.record_id))
        .execute(f.database.pool()).await?;
    assert_eq!(
        f.database
            .artifacts()
            .cleanup_failed_artifacts_claimed(&claim, &f.paths)
            .await?,
        FailedCleanupOutcome::Completed
    );
    assert!(!f.source.exists() && !f.staged.exists());
    assert!(
        f.database
            .cleanup_tombstone("failed_artifact_retention", &f.artifact.id.to_string())
            .await?
            .is_some()
    );
    let events: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM events WHERE kind = 'cleanup_state_changed'")
            .fetch_one(f.database.pool())
            .await?;
    assert_eq!(events, 1);
    f.database.actions().succeed(&claim).await?;
    Ok(())
}

#[tokio::test]
async fn failed_cleanup_expired_claim_at_begin_cannot_prepare_or_unlink() -> TestResult {
    let f = FailedCleanupFixture::new().await?;
    f.diagnose().await?;
    f.database
        .actions()
        .enqueue_failed_cleanup(f.attempt_id)
        .await?;
    let claim = f
        .database
        .actions()
        .claim_failed_cleanup("expired", Duration::from_secs(30))
        .await?
        .ok_or("claim")?;
    sqlx::query("UPDATE actions SET claim_expires_at = '2000-01-01' WHERE id = ?")
        .bind(claim.record_id.to_string())
        .execute(f.database.pool())
        .await?;
    assert!(matches!(
        f.database
            .artifacts()
            .cleanup_failed_artifacts_claimed(&claim, &f.paths)
            .await,
        Err(PersistenceError::Conflict { .. })
    ));
    assert!(f.source.is_file() && f.staged.is_file());
    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM failed_cleanup_intents")
        .fetch_one(f.database.pool())
        .await?;
    assert_eq!(count, 0);
    assert!(f.database.actions().claimed(&claim).await?.is_none());
    Ok(())
}

#[tokio::test]
async fn failed_cleanup_renewal_conflicts_on_changed_or_missing_claim() -> TestResult {
    for phase in ["failed_cleanup_intents", "events"] {
        for change in [
            "claim_owner = 'other-owner'",
            "claim_id = '00000000-0000-0000-0000-000000000099'",
            "state = 'pending', claim_id = NULL, claim_owner = NULL, claim_expires_at = NULL",
            "kind = 'publish_artifacts'",
            "attempt_id = NULL",
            "job_id = NULL",
            "project_id = (SELECT id FROM projects WHERE name = 'other-project')",
        ] {
            let f = FailedCleanupFixture::new().await?;
            f.diagnose().await?;
            // A distinct registered project makes a project-only identity change valid SQL.
            sqlx::query("INSERT INTO projects (id, name, root_path, config_path) VALUES (?, 'other-project', '/other-project', '/other-project/igor.toml')")
                .bind(uuid::Uuid::new_v4().to_string()).execute(f.database.pool()).await?;
            f.database
                .actions()
                .enqueue_failed_cleanup(f.attempt_id)
                .await?;
            let claim = f
                .database
                .actions()
                .claim_failed_cleanup("original-owner", Duration::from_secs(30))
                .await?
                .ok_or("claim")?;
            let condition = if phase == "events" {
                "WHEN NEW.kind = 'cleanup_state_changed'"
            } else {
                ""
            };
            sqlx::raw_sql(&format!("CREATE TRIGGER change_claim AFTER INSERT ON {phase} {condition} BEGIN UPDATE actions SET {change} WHERE id = '{}'; END;", claim.record_id))
                .execute(f.database.pool()).await?;
            assert!(
                matches!(
                    f.database
                        .artifacts()
                        .cleanup_failed_artifacts_claimed(&claim, &f.paths)
                        .await,
                    Err(PersistenceError::Conflict {
                        entity: "failed cleanup claim"
                    })
                ),
                "{phase}: {change}"
            );
            // The changed identity and all phase metadata must roll back, rather than
            // renewing another owner's lease or committing cleanup evidence.
            assert!(
                f.database.actions().claimed(&claim).await?.is_some(),
                "{phase}: {change}"
            );
            let intents: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM failed_cleanup_intents")
                .fetch_one(f.database.pool())
                .await?;
            assert_eq!(intents, i64::from(phase == "events"), "{phase}: {change}");
            assert!(
                f.database
                    .cleanup_tombstone("failed_artifact_retention", &f.artifact.id.to_string())
                    .await?
                    .is_none()
            );
            let events: i64 = sqlx::query_scalar(
                "SELECT COUNT(*) FROM events WHERE kind = 'cleanup_state_changed'",
            )
            .fetch_one(f.database.pool())
            .await?;
            assert_eq!(events, 0, "{phase}: {change}");
            if phase == "failed_cleanup_intents" {
                assert!(f.source.is_file() && f.staged.is_file());
            } else {
                // Physical deletion is recoverable using the already committed intent.
                assert!(!f.source.exists() && !f.staged.exists());
                sqlx::raw_sql("DROP TRIGGER change_claim")
                    .execute(f.database.pool())
                    .await?;
                assert_eq!(
                    f.database
                        .artifacts()
                        .cleanup_failed_artifacts_claimed(&claim, &f.paths)
                        .await?,
                    FailedCleanupOutcome::Completed
                );
                f.database.actions().succeed(&claim).await?;
            }
        }
    }
    Ok(())
}

#[tokio::test]
async fn failed_cleanup_renewal_conflicts_on_missing_action() -> TestResult {
    for change in [
        "DELETE FROM actions WHERE id =",
        "UPDATE actions SET id = '00000000-0000-0000-0000-000000000099' WHERE id =",
    ] {
        let f = FailedCleanupFixture::new().await?;
        f.diagnose().await?;
        f.database
            .actions()
            .enqueue_failed_cleanup(f.attempt_id)
            .await?;
        let claim = f
            .database
            .actions()
            .claim_failed_cleanup("original-owner", Duration::from_secs(30))
            .await?
            .ok_or("claim")?;
        sqlx::raw_sql(&format!("CREATE TRIGGER remove_claim AFTER INSERT ON failed_cleanup_intents BEGIN {change} '{}'; END;", claim.record_id))
            .execute(f.database.pool()).await?;
        assert!(
            matches!(
                f.database
                    .artifacts()
                    .cleanup_failed_artifacts_claimed(&claim, &f.paths)
                    .await,
                Err(PersistenceError::Conflict {
                    entity: "failed cleanup claim"
                })
            ),
            "{change}"
        );
        assert!(f.source.is_file() && f.staged.is_file());
        assert!(f.database.actions().claimed(&claim).await?.is_some());
        let intents: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM failed_cleanup_intents")
            .fetch_one(f.database.pool())
            .await?;
        assert_eq!(intents, 0);
    }
    Ok(())
}

#[tokio::test]
async fn cleanup_schema20_does_not_fabricate_historical_tombstones() -> TestResult {
    let f = FailedCleanupFixture::new().await?;
    f.diagnose().await?;
    // Model the terminal artifact rows left by schema19, with no evidence table.
    sqlx::raw_sql("DROP TABLE cleanup_tombstones; DROP TABLE failed_cleanup_intents; DELETE FROM _sqlx_migrations WHERE version = 20; UPDATE artifacts SET state = 'removed', lifecycle_state = 'removed';")
        .execute(f.database.pool()).await?;
    f.database.pool().close().await;
    let upgraded = Database::open(&f.paths.database).await?;
    assert_eq!(upgraded.schema_version().await?, 22);
    assert!(
        upgraded
            .cleanup_tombstone("failed_artifact_retention", &f.artifact.id.to_string())
            .await?
            .is_none()
    );
    assert_eq!(
        upgraded
            .artifacts()
            .cleanup_failed_artifacts(f.attempt_id, &f.paths)
            .await?,
        0
    );
    assert!(f.source.is_file() && f.staged.is_file());
    Ok(())
}

#[tokio::test]
async fn failed_cleanup_missing_copy_cannot_prepare_or_delete_other_bytes() -> TestResult {
    for source_missing in [true, false] {
        let f = FailedCleanupFixture::new().await?;
        f.diagnose().await?;
        fs::remove_file(if source_missing { &f.source } else { &f.staged })?;
        let remaining = if source_missing { &f.staged } else { &f.source };
        let bytes = fs::read(remaining)?;
        assert!(f.cleanup().await.is_err());
        assert_eq!(fs::read(remaining)?, bytes);
        let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM failed_cleanup_intents")
            .fetch_one(f.database.pool())
            .await?;
        assert_eq!(count, 0);
        assert!(
            f.database
                .cleanup_tombstone("failed_artifact_retention", &f.artifact.id.to_string())
                .await?
                .is_none()
        );
    }
    Ok(())
}

#[tokio::test]
async fn failed_cleanup_rechecks_barriers_after_durable_preparation() -> TestResult {
    for change in [
        "UPDATE artifacts SET metadata_json = json_set(metadata_json, '$.changed', 1) WHERE id = NEW.artifact_id;",
        "UPDATE recoveries SET state = 'diagnosing', diagnosis_json = NULL;",
        "UPDATE failed_log_tails SET complete = 0;",
        "UPDATE projects SET registered = 0;",
        "UPDATE projects SET root_path = root_path || '/different';",
        "UPDATE attempt_processes SET stdout_path = '/different/stdout.log';",
        "INSERT INTO actions (id, project_id, attempt_id, kind, state, spec_json, idempotency_key, available_at) SELECT '00000000-0000-0000-0000-000000000099', project_id, id, 'extract_metrics', 'pending', '{}', 'barrier-extraction', '2000-01-01' FROM attempts;",
    ] {
        let f = FailedCleanupFixture::new().await?;
        f.diagnose().await?;
        sqlx::query("UPDATE attempts SET state = 'failed'")
            .execute(f.database.pool())
            .await?;
        sqlx::query("INSERT INTO failed_log_tails (attempt_id, stdout, stderr, limit_kb, complete) VALUES (?, X'', X'', 1, 1)")
            .bind(f.attempt_id.to_string()).execute(f.database.pool()).await?;
        sqlx::raw_sql(&format!("CREATE TRIGGER preparation_change AFTER INSERT ON failed_cleanup_intents BEGIN {change} END;"))
            .execute(f.database.pool()).await?;
        let _ = f.cleanup().await;
        assert_eq!(fs::read(&f.source)?, fs::read(&f.staged)?, "{change}");
        assert!(
            f.database
                .cleanup_tombstone("failed_artifact_retention", &f.artifact.id.to_string())
                .await?
                .is_none()
        );
        let state: String = sqlx::query_scalar("SELECT state FROM failed_cleanup_intents")
            .fetch_one(f.database.pool())
            .await?;
        assert_eq!(state, "prepared");
    }
    Ok(())
}

#[tokio::test]
async fn failed_cleanup_outbox_deduplicates_and_defers_without_spending_retries() -> TestResult {
    let f = FailedCleanupFixture::new().await?;
    let id = f
        .database
        .actions()
        .enqueue_failed_cleanup(f.attempt_id)
        .await?;
    assert_eq!(
        id,
        f.database
            .actions()
            .enqueue_failed_cleanup(f.attempt_id)
            .await?
    );
    for _ in 0..5 {
        let claim = f
            .database
            .actions()
            .claim_failed_cleanup("cleanup", Duration::from_secs(30))
            .await?
            .ok_or("claim")?;
        let work = f.database.actions().claimed(&claim).await?.ok_or("work")?;
        let attempt = f
            .database
            .jobs()
            .get_attempt(f.attempt_id)
            .await?
            .ok_or("attempt")?;
        assert_eq!(work.attempt_id, Some(f.attempt_id));
        assert_eq!(work.job_id, Some(attempt.spec.job_id()));
        let project: String = sqlx::query_scalar("SELECT project_id FROM attempts WHERE id = ?")
            .bind(f.attempt_id.to_string())
            .fetch_one(f.database.pool())
            .await?;
        assert_eq!(work.action.project_id.to_string(), project);
        assert_eq!(
            work.action.spec,
            json!({"schema_version":1,"attempt_id":f.attempt_id})
        );
        assert_eq!(
            work.action.idempotency_key,
            format!("cleanup_failed_artifacts:v1:{}", f.attempt_id)
        );
        assert_eq!(work.attempts, 1);
        assert_eq!(
            f.database
                .artifacts()
                .cleanup_failed_artifacts_claimed(&claim, &f.paths)
                .await?,
            FailedCleanupOutcome::Deferred
        );
        f.database
            .actions()
            .postpone_failed_cleanup(&claim, Duration::ZERO)
            .await?;
    }
    assert!(f.source.is_file() && f.staged.is_file());
    f.diagnose().await?;
    let stale = f
        .database
        .actions()
        .claim_failed_cleanup("old", Duration::from_secs(30))
        .await?
        .ok_or("claim")?;
    sqlx::query("UPDATE actions SET claim_expires_at = '2000-01-01' WHERE id = ?")
        .bind(id.to_string())
        .execute(f.database.pool())
        .await?;
    let claim = f
        .database
        .actions()
        .claim_failed_cleanup("new", Duration::from_secs(30))
        .await?
        .ok_or("reclaimed")?;
    assert!(
        f.database
            .artifacts()
            .cleanup_failed_artifacts_claimed(&stale, &f.paths)
            .await
            .is_err()
    );
    assert!(f.source.is_file() && f.staged.is_file());
    assert_eq!(
        f.database
            .artifacts()
            .cleanup_failed_artifacts_claimed(&claim, &f.paths)
            .await?,
        FailedCleanupOutcome::Completed
    );
    let evidence = f
        .database
        .cleanup_tombstone("failed_artifact_retention", &f.artifact.id.to_string())
        .await?
        .ok_or("tombstone")?;
    assert_eq!(evidence["action_id"], id.to_string());
    // Crash after cleanup commit, before action ack: reclaim and replay without new evidence.
    sqlx::query("UPDATE actions SET claim_expires_at = '2000-01-01' WHERE id = ?")
        .bind(id.to_string())
        .execute(f.database.pool())
        .await?;
    let replay = f
        .database
        .actions()
        .claim_failed_cleanup("restart", Duration::from_secs(30))
        .await?
        .ok_or("replay")?;
    assert_eq!(
        f.database
            .artifacts()
            .cleanup_failed_artifacts_claimed(&replay, &f.paths)
            .await?,
        FailedCleanupOutcome::Completed
    );
    assert_eq!(
        f.database
            .cleanup_tombstone("failed_artifact_retention", &f.artifact.id.to_string())
            .await?,
        Some(evidence)
    );
    let count: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM events WHERE kind = 'cleanup_state_changed'")
            .fetch_one(f.database.pool())
            .await?;
    assert_eq!(count, 1);
    f.database.actions().succeed(&replay).await?;
    assert_eq!(
        id,
        f.database
            .actions()
            .enqueue_failed_cleanup(f.attempt_id)
            .await?
    );
    assert!(
        f.database
            .actions()
            .claim_failed_cleanup("scanner", Duration::from_secs(30))
            .await?
            .is_none()
    );
    Ok(())
}

// Durable outbox view of the maintenance action: prepared intents, evidence,
// cleanup events and the single action row.
async fn reopen_outbox_counts(database: &Database) -> Result<(i64, i64, i64, i64), sqlx::Error> {
    sqlx::query_as(
        "SELECT (SELECT COUNT(*) FROM failed_cleanup_intents WHERE state = 'prepared'), \
                (SELECT COUNT(*) FROM cleanup_tombstones), \
                (SELECT COUNT(*) FROM events WHERE kind = 'cleanup_state_changed'), \
                (SELECT COUNT(*) FROM actions WHERE kind = 'cleanup_failed_artifacts')",
    )
    .fetch_one(database.pool())
    .await
}

// Outbox status a claimed run must not change by itself; only a live lease may
// renew claim_expires_at/updated_at while the run is in flight.
async fn reopen_outbox_status(database: &Database) -> Result<Vec<String>, sqlx::Error> {
    sqlx::query_scalar(
        "SELECT json_object('state', state, 'claim_id', claim_id, 'claim_owner', claim_owner, \
                'attempts', attempts, 'available_at', available_at, \
                'idempotency_key', idempotency_key) \
         FROM actions WHERE kind = 'cleanup_failed_artifacts'",
    )
    .fetch_all(database.pool())
    .await
}

#[tokio::test]
async fn failed_cleanup_outbox_expired_claim_recovers_after_pool_reopen() -> TestResult {
    let mut f = FailedCleanupFixture::new().await?;
    f.diagnose().await?;
    let diagnosis: String = sqlx::query_scalar("SELECT state FROM recoveries")
        .fetch_one(f.database.pool())
        .await?;
    let id = f
        .database
        .actions()
        .enqueue_failed_cleanup(f.attempt_id)
        .await?;
    let old = f
        .database
        .actions()
        .claim_failed_cleanup("old", Duration::from_secs(30))
        .await?
        .ok_or("claim")?;
    // The claimed run unlinks for real; the event failure rolls the completion
    // transaction back without deferring or completing the action on its own.
    sqlx::raw_sql("CREATE TRIGGER reject_gc_event BEFORE INSERT ON events WHEN NEW.kind = 'cleanup_state_changed' BEGIN SELECT RAISE(ABORT, 'event failure'); END;").execute(f.database.pool()).await?;
    let before_error = reopen_outbox_status(&f.database).await?;
    assert!(
        f.database
            .artifacts()
            .cleanup_failed_artifacts_claimed(&old, &f.paths)
            .await
            .is_err()
    );
    assert_eq!(
        reopen_outbox_status(&f.database).await?,
        before_error,
        "a failed claimed run must not defer, complete or reassign its action"
    );
    assert!(!f.source.exists() && !f.staged.exists());
    assert_eq!(reopen_outbox_counts(&f.database).await?, (1, 0, 0, 1));
    let prepared = reopen_intent_rows_failed(&f.database).await?;
    assert_eq!(prepared.len(), 1);

    // Expire the lease, close the pool and reopen the same database file.
    sqlx::query("UPDATE actions SET claim_expires_at = '2000-01-01' WHERE id = ?")
        .bind(id.to_string())
        .execute(f.database.pool())
        .await?;
    let attempts_before: i64 = sqlx::query_scalar("SELECT attempts FROM actions WHERE id = ?")
        .bind(id.to_string())
        .fetch_one(f.database.pool())
        .await?;
    let persisted = gc_support::sql_snapshot(&f.database).await?;
    f.database.pool().close().await;
    f.database = Database::open(&f.paths.database).await?;
    assert_eq!(
        gc_support::sql_snapshot(&f.database).await?,
        persisted,
        "expired claim and prepared intent must survive the pool reopen"
    );

    // The stale lease is rejected before any filesystem or SQL work.
    let stale = reopen_outbox_status(&f.database).await?;
    assert!(
        f.database
            .artifacts()
            .cleanup_failed_artifacts_claimed(&old, &f.paths)
            .await
            .is_err()
    );
    assert_eq!(reopen_outbox_status(&f.database).await?, stale);
    assert!(!f.source.exists() && !f.staged.exists());
    assert_eq!(reopen_outbox_counts(&f.database).await?, (1, 0, 0, 1));

    // A new claim recovers the same action under the existing retry budget.
    sqlx::query("DROP TRIGGER reject_gc_event")
        .execute(f.database.pool())
        .await?;
    let reclaim = f
        .database
        .actions()
        .claim_failed_cleanup("new", Duration::from_secs(30))
        .await?
        .ok_or("reclaimed")?;
    let attempts_after: i64 = sqlx::query_scalar("SELECT attempts FROM actions WHERE id = ?")
        .bind(id.to_string())
        .fetch_one(f.database.pool())
        .await?;
    assert_eq!(
        attempts_after,
        attempts_before + 1,
        "each claim spends exactly one attempt of the existing retry budget"
    );
    let work = f
        .database
        .actions()
        .claimed(&reclaim)
        .await?
        .ok_or("work")?;
    assert_eq!(
        work.action.idempotency_key,
        format!("cleanup_failed_artifacts:v1:{}", f.attempt_id)
    );
    assert!(work.attempts >= 1);

    assert_eq!(
        f.database
            .artifacts()
            .cleanup_failed_artifacts_claimed(&reclaim, &f.paths)
            .await?,
        FailedCleanupOutcome::Completed
    );
    assert!(!f.source.exists() && !f.staged.exists());
    assert_eq!(reopen_outbox_counts(&f.database).await?, (0, 1, 1, 1));
    let completed = reopen_intent_rows_failed(&f.database).await?;
    assert_eq!(completed.len(), 1);
    let prepared_value: serde_json::Value = serde_json::from_str(&prepared[0])?;
    let completed_value: serde_json::Value = serde_json::from_str(&completed[0])?;
    assert_eq!(completed_value["state"], "completed");
    assert_eq!(
        completed_value["identity_json"],
        prepared_value["identity_json"]
    );
    assert_eq!(completed_value["decided_at"], prepared_value["decided_at"]);
    assert_eq!(
        completed_value["prepared_at"],
        prepared_value["prepared_at"]
    );
    let evidence = f
        .database
        .cleanup_tombstone("failed_artifact_retention", &f.artifact.id.to_string())
        .await?
        .ok_or("tombstone")?;
    assert_eq!(evidence["action_id"], id.to_string());
    assert_eq!(evidence["decided_at"], prepared_value["decided_at"]);

    // Replaying with the same claim produces no duplicate action or evidence.
    assert_eq!(
        f.database
            .artifacts()
            .cleanup_failed_artifacts_claimed(&reclaim, &f.paths)
            .await?,
        FailedCleanupOutcome::Completed
    );
    assert_eq!(
        f.database
            .cleanup_tombstone("failed_artifact_retention", &f.artifact.id.to_string())
            .await?,
        Some(evidence)
    );
    assert_eq!(reopen_outbox_counts(&f.database).await?, (0, 1, 1, 1));

    // Ack through the outbox: the action stays unique and no new claim appears.
    f.database.actions().succeed(&reclaim).await?;
    assert_eq!(
        id,
        f.database
            .actions()
            .enqueue_failed_cleanup(f.attempt_id)
            .await?
    );
    assert!(
        f.database
            .actions()
            .claim_failed_cleanup("scanner", Duration::from_secs(30))
            .await?
            .is_none()
    );
    assert_eq!(reopen_outbox_counts(&f.database).await?, (0, 1, 1, 1));
    // Diagnosis barrier and process logs are untouched by the maintenance run.
    let final_diagnosis: String = sqlx::query_scalar("SELECT state FROM recoveries")
        .fetch_one(f.database.pool())
        .await?;
    assert_eq!(final_diagnosis, diagnosis);
    let attempt_logs = f.paths.log_dir.join(f.attempt_id.to_string());
    assert_eq!(
        fs::read(attempt_logs.join("stdout.log"))?,
        b"stdout diagnostic"
    );
    assert_eq!(
        fs::read(attempt_logs.join("stderr.log"))?,
        b"stderr diagnostic"
    );
    Ok(())
}

#[tokio::test]
async fn failed_cleanup_outbox_batches_do_not_ack_early() -> TestResult {
    let f = FailedCleanupFixture::new().await?;
    f.diagnose().await?;
    for n in 0..65 {
        let mut artifact = f.artifact.clone();
        artifact.id = uuid::Uuid::new_v4();
        artifact.path = format!("extra-{n}.bin");
        artifact.state = igor_core::ArtifactPublicationState::Registered;
        artifact.metadata = json!({});
        fs::copy(&f.source, f.source.with_file_name(&artifact.path))?;
        f.database.artifacts().insert(&artifact).await?;
    }
    f.database
        .actions()
        .enqueue_failed_cleanup(f.attempt_id)
        .await?;
    let claim = f
        .database
        .actions()
        .claim_failed_cleanup("batch", Duration::from_secs(60))
        .await?
        .ok_or("claim")?;
    assert_eq!(
        f.database
            .artifacts()
            .cleanup_failed_artifacts_claimed(&claim, &f.paths)
            .await?,
        FailedCleanupOutcome::MoreWork
    );
    let removed: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM cleanup_tombstones")
        .fetch_one(f.database.pool())
        .await?;
    assert_eq!(removed, 64);
    f.database
        .actions()
        .postpone_failed_cleanup(&claim, Duration::ZERO)
        .await?;
    let claim = f
        .database
        .actions()
        .claim_failed_cleanup("batch", Duration::from_secs(60))
        .await?
        .ok_or("claim")?;
    assert_eq!(
        f.database
            .artifacts()
            .cleanup_failed_artifacts_claimed(&claim, &f.paths)
            .await?,
        FailedCleanupOutcome::Completed
    );
    let removed: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM cleanup_tombstones")
        .fetch_one(f.database.pool())
        .await?;
    assert_eq!(removed, 66);
    Ok(())
}

#[tokio::test]
async fn failed_cleanup_operational_failures_exhaust_default_retries_without_duplicate_actions()
-> TestResult {
    let f = FailedCleanupFixture::new().await?;
    f.diagnose().await?;
    fs::write(&f.staged, b"corrupt copy")?;
    let id = f
        .database
        .actions()
        .enqueue_failed_cleanup(f.attempt_id)
        .await?;
    for n in 1..=3 {
        let claim = f
            .database
            .actions()
            .claim_failed_cleanup("errors", Duration::from_secs(30))
            .await?
            .ok_or("claim")?;
        let error = f
            .database
            .artifacts()
            .cleanup_failed_artifacts_claimed(&claim, &f.paths)
            .await
            .err()
            .ok_or("corrupt copy was accepted")?;
        f.database
            .actions()
            .retry(
                &claim,
                &igor_core::ActionRetryPolicy::default().0,
                &error.to_string(),
            )
            .await?;
        let row = sqlx::query("SELECT state, attempts, last_error FROM actions WHERE id = ?")
            .bind(id.to_string())
            .fetch_one(f.database.pool())
            .await?;
        assert_eq!(row.get::<i64, _>("attempts"), n);
        assert_eq!(
            row.get::<String, _>("state"),
            if n == 3 { "failed" } else { "pending" }
        );
        assert!(
            row.get::<String, _>("last_error")
                .contains("cleanup artifact identity")
        );
        sqlx::query("UPDATE actions SET available_at = '2000-01-01' WHERE id = ?")
            .bind(id.to_string())
            .execute(f.database.pool())
            .await?;
    }
    assert_eq!(
        id,
        f.database
            .actions()
            .enqueue_failed_cleanup(f.attempt_id)
            .await?
    );
    assert!(
        f.database
            .actions()
            .claim_failed_cleanup("scanner", Duration::from_secs(30))
            .await?
            .is_none()
    );
    assert!(f.source.is_file() && f.staged.is_file());
    Ok(())
}

#[tokio::test]
async fn cleanup_evidence_is_immutable_even_for_replace_and_has_durable_postponement() -> TestResult
{
    let f = FailedCleanupFixture::new().await?;
    let id = f
        .database
        .actions()
        .enqueue_failed_cleanup(f.attempt_id)
        .await?;
    let claim = f
        .database
        .actions()
        .claim_failed_cleanup("wait", Duration::from_secs(30))
        .await?
        .ok_or("claim")?;
    assert_eq!(
        f.database
            .artifacts()
            .cleanup_failed_artifacts_claimed(&claim, &f.paths)
            .await?,
        FailedCleanupOutcome::Deferred
    );
    f.database
        .actions()
        .postpone_failed_cleanup(&claim, Duration::from_secs(60))
        .await?;
    assert!(
        f.database
            .actions()
            .claim_failed_cleanup("wait", Duration::from_secs(30))
            .await?
            .is_none()
    );
    let row = sqlx::query("SELECT state, attempts, last_error FROM actions WHERE id = ?")
        .bind(id.to_string())
        .fetch_one(f.database.pool())
        .await?;
    assert_eq!(row.get::<String, _>("state"), "pending");
    assert_eq!(row.get::<i64, _>("attempts"), 0);
    assert!(row.get::<Option<String>, _>("last_error").is_none());
    f.diagnose().await?;
    assert_eq!(f.cleanup().await?, 1);
    for sql in [
        "UPDATE failed_cleanup_intents SET identity_json = '{}'",
        "UPDATE failed_cleanup_intents SET prepared_at = '2000-01-01'",
        "UPDATE failed_cleanup_intents SET state = 'prepared'",
        "DELETE FROM failed_cleanup_intents",
        "INSERT OR REPLACE INTO failed_cleanup_intents SELECT * FROM failed_cleanup_intents",
        "UPDATE cleanup_tombstones SET completed_at = '2000-01-01'",
        "DELETE FROM cleanup_tombstones",
        "INSERT OR REPLACE INTO cleanup_tombstones SELECT * FROM cleanup_tombstones",
    ] {
        assert!(
            sqlx::query(sql).execute(f.database.pool()).await.is_err(),
            "{sql}"
        );
    }
    Ok(())
}
