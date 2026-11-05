use super::report_actions::{candidate, enabled, succeed};
use super::*;
use igor_core::{
    Claim, PreparedReport, ReportExecutionError, ReportPreparation, ReportPublishOutcome,
    stage_generated_report,
};
use std::future::Future;
use std::os::unix::fs::MetadataExt;

async fn setup() -> Result<(FamilyFixture, Claim<ActionId>), Box<dyn Error>> {
    let fixture = family_fixture().await?;
    fixture
        .database
        .families()
        .submit(&fixture.prepared)
        .await?;
    succeed(&fixture.database, &fixture.prepared).await?;
    let selected = candidate(&fixture).await?;
    fixture
        .database
        .actions()
        .enqueue_family_report(&selected, &enabled())
        .await?;
    let claim = fixture
        .database
        .actions()
        .claim_report("report-test", Duration::from_secs(30))
        .await?
        .ok_or("report claim")?;
    Ok((fixture, claim))
}

async fn prepare(
    database: &Database,
    claim: &Claim<ActionId>,
) -> Result<PreparedReport, Box<dyn Error>> {
    match database.actions().prepare_report(claim, &enabled()).await? {
        ReportPreparation::Ready(prepared) => Ok(*prepared),
        result => Err(format!("report was not ready: {result:?}").into()),
    }
}

async fn old_report(fixture: &FamilyFixture) -> Result<PathBuf, Box<dyn Error>> {
    let project = candidate(fixture).await?.project;
    let context = fixture
        .database
        .families()
        .report_context(fixture.prepared.family.id)
        .await?;
    Ok(stage_generated_report(&project, &enabled(), &context)?
        .publish()?
        .path)
}

async fn state(database: &Database, id: ActionId) -> Result<(String, i64), sqlx::Error> {
    sqlx::query_as("SELECT state, attempts FROM actions WHERE id = ?")
        .bind(id.to_string())
        .fetch_one(database.pool())
        .await
}

#[tokio::test]
async fn report_execution_publishes_current_ready_context_and_cannot_bypass_completion_gate()
-> TestResult {
    let (fixture, claim) = setup().await?;
    let database = &fixture.database;
    assert!(database.actions().succeed(&claim).await.is_err());
    let prepared = prepare(database, &claim).await?;
    let output = prepared.project().root.join("RESULTS.md");
    let locked = tokio::task::spawn_blocking(move || prepared.stage_and_lock()).await??;
    assert!(!output.exists());
    let result = database
        .actions()
        .publish_report(&claim, locked, &enabled())
        .await?;
    let ReportPublishOutcome::Published(receipt) = result else {
        return Err("not published".into());
    };
    let bytes = fs::read(&output)?;
    assert_eq!(receipt.path, output);
    assert_eq!(
        receipt.sha256,
        format!("sha256:{:x}", sha2::Sha256::digest(&bytes))
    );
    assert!(String::from_utf8(bytes)?.contains("Status: **Ready**"));
    assert_eq!(
        state(database, claim.record_id).await?,
        ("succeeded".into(), 1)
    );
    assert!(
        database
            .actions()
            .claim_report("again", Duration::from_secs(30))
            .await?
            .is_none()
    );
    assert!(
        database
            .jobs()
            .list(None)
            .await?
            .iter()
            .all(|job| job.state == JobState::Succeeded)
    );
    Ok(())
}

#[tokio::test]
async fn report_execution_incomplete_or_failed_after_staging_defers_without_replacing_old_file()
-> TestResult {
    for job_state in ["queued", "failed"] {
        let (fixture, claim) = setup().await?;
        let database = &fixture.database;
        let output = old_report(&fixture).await?;
        let bytes = fs::read(&output)?;
        let inode = fs::metadata(&output)?.ino();
        let prepared = prepare(database, &claim).await?;
        let locked = prepared.stage_and_lock()?;
        sqlx::query("UPDATE jobs SET state = ? WHERE id = ?")
            .bind(job_state)
            .bind(fixture.prepared.submissions[0].job.id.to_string())
            .execute(database.pool())
            .await?;
        assert_eq!(
            database
                .actions()
                .publish_report(&claim, locked, &enabled())
                .await?,
            ReportPublishOutcome::Deferred
        );
        assert_eq!(
            state(database, claim.record_id).await?,
            ("pending".into(), 0)
        );
        assert_eq!(fs::read(&output)?, bytes);
        assert_eq!(fs::metadata(&output)?.ino(), inode);
    }
    Ok(())
}

#[tokio::test]
async fn report_execution_supersession_cancels_obsolete_generation_and_preserves_previous_report()
-> TestResult {
    let (fixture, claim) = setup().await?;
    let successor = successor_for(&fixture).await?;
    let database = &fixture.database;
    let output = old_report(&fixture).await?;
    let bytes = fs::read(&output)?;
    let locked = prepare(database, &claim).await?.stage_and_lock()?;
    database
        .families()
        .supersede(fixture.prepared.generation.identity.id, &successor)
        .await?;
    assert_eq!(
        database
            .actions()
            .publish_report(&claim, locked, &enabled())
            .await?,
        ReportPublishOutcome::Cancelled
    );
    assert_eq!(state(database, claim.record_id).await?.0, "cancelled");
    assert_eq!(fs::read(&output)?, bytes);
    Ok(())
}

#[tokio::test]
async fn report_execution_live_policy_registration_and_context_changes_fence_publication()
-> TestResult {
    for change in [
        "disabled",
        "output",
        "deregistered",
        "metadata",
        "context",
        "context_float",
    ] {
        let (fixture, claim) = setup().await?;
        let database = &fixture.database;
        if change == "context_float" {
            sqlx::query("INSERT INTO recoveries (id, attempt_id, state, diagnosis_json, idempotency_key) VALUES (?, ?, 'succeeded', '{\"zero\":-0.0}', ?)")
                .bind(uuid::Uuid::new_v4().to_string()).bind(fixture.prepared.submissions[0].attempt.id().to_string())
                .bind(uuid::Uuid::new_v4().to_string()).execute(database.pool()).await?;
        }
        let output = old_report(&fixture).await?;
        let bytes = fs::read(&output)?;
        let inode = fs::metadata(&output)?.ino();
        let locked = prepare(database, &claim).await?.stage_and_lock()?;
        let mut policy = enabled();
        let project = candidate(&fixture).await?.project;
        match change {
            "disabled" => policy.enabled = false,
            "output" => policy.output = PathBuf::from("other.md"),
            "deregistered" => {
                database.projects().remove(&project.root).await?;
            }
            "metadata" => {
                let mut project = project.clone();
                project.name = "changed".into();
                database.projects().update(&project).await?;
            }
            "context" => {
                sqlx::query("INSERT INTO recoveries (id, attempt_id, state, diagnosis_json, idempotency_key) VALUES (?, ?, 'succeeded', '{}', ?)")
                    .bind(uuid::Uuid::new_v4().to_string()).bind(fixture.prepared.submissions[0].attempt.id().to_string())
                    .bind(uuid::Uuid::new_v4().to_string()).execute(database.pool()).await?;
            }
            "context_float" => {
                // Numeric PartialEq treats signed zero as equal; the serialized evidence differs.
                sqlx::query("UPDATE recoveries SET diagnosis_json = '{\"zero\":0.0}'")
                    .execute(database.pool())
                    .await?;
            }
            _ => return Err("unknown fixture change".into()),
        }
        let outcome = database
            .actions()
            .publish_report(&claim, locked, &policy)
            .await?;
        assert_eq!(
            outcome,
            if change.starts_with("context") {
                ReportPublishOutcome::Deferred
            } else {
                ReportPublishOutcome::Cancelled
            }
        );
        assert_eq!(fs::read(&output)?, bytes);
        assert_eq!(fs::metadata(&output)?.ino(), inode);
        assert!(!project.root.join("other.md").exists());
    }
    Ok(())
}

#[tokio::test]
async fn report_execution_reclaimed_lease_cannot_publish_the_old_projection() -> TestResult {
    let (fixture, old) = setup().await?;
    let database = &fixture.database;
    let locked = prepare(database, &old).await?.stage_and_lock()?;
    sqlx::query("UPDATE actions SET claim_expires_at = '2000-01-01' WHERE id = ?")
        .bind(old.record_id.to_string())
        .execute(database.pool())
        .await?;
    let new = database
        .actions()
        .claim_report("new owner", Duration::from_secs(30))
        .await?
        .ok_or("new report claim")?;
    assert_ne!(old.lease_id, new.lease_id);
    assert!(matches!(
        database
            .actions()
            .publish_report(&old, locked, &enabled())
            .await,
        Err(ReportExecutionError::Persistence(
            PersistenceError::Conflict { .. }
        ))
    ));
    let root = candidate(&fixture).await?.project.root;
    assert!(!root.join("RESULTS.md").exists());
    let locked = prepare(database, &new).await?.stage_and_lock()?;
    assert!(matches!(
        database
            .actions()
            .publish_report(&new, locked, &enabled())
            .await?,
        ReportPublishOutcome::Published(_)
    ));
    Ok(())
}

#[tokio::test]
async fn report_execution_sql_failure_precedes_rename_and_retries_without_experiment_rerun()
-> TestResult {
    let (fixture, claim) = setup().await?;
    let database = &fixture.database;
    let output = old_report(&fixture).await?;
    let bytes = fs::read(&output)?;
    let inode = fs::metadata(&output)?.ino();
    let locked = prepare(database, &claim).await?.stage_and_lock()?;
    sqlx::raw_sql("CREATE TRIGGER report_completion_fault BEFORE INSERT ON events WHEN NEW.kind = 'action_state_changed' AND json_extract(NEW.payload_json, '$.data.state') = 'succeeded' BEGIN SELECT RAISE(ABORT, 'report completion fault'); END;")
        .execute(database.pool()).await?;
    assert!(
        database
            .actions()
            .publish_report(&claim, locked, &enabled())
            .await
            .is_err()
    );
    assert_eq!(fs::read(&output)?, bytes);
    assert_eq!(fs::metadata(&output)?.ino(), inode);
    assert_eq!(state(database, claim.record_id).await?.0, "running");
    sqlx::raw_sql("DROP TRIGGER report_completion_fault")
        .execute(database.pool())
        .await?;
    let locked = prepare(database, &claim).await?.stage_and_lock()?;
    assert!(matches!(
        database
            .actions()
            .publish_report(&claim, locked, &enabled())
            .await?,
        ReportPublishOutcome::Published(_)
    ));
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM attempts")
            .fetch_one(database.pool())
            .await?,
        2
    );
    Ok(())
}

#[tokio::test]
async fn report_execution_pending_extraction_defers_and_terminal_outcomes_release_the_barrier()
-> TestResult {
    let mut fixture = family_fixture().await?;
    let command = fixture.prepared.submissions[0].job.command.clone();
    for submission in &mut fixture.prepared.submissions {
        let mut spec = serde_json::to_value(&submission.attempt)?;
        spec["result"]["extractor"] = serde_json::to_value(&command)?;
        submission.attempt = serde_json::from_value(spec)?;
    }
    let database = &fixture.database;
    database.families().submit(&fixture.prepared).await?;
    succeed(database, &fixture.prepared).await?;
    let selected = candidate(&fixture).await?;
    database
        .actions()
        .enqueue_family_report(&selected, &enabled())
        .await?;
    let claim = database
        .actions()
        .claim_report("reporter", Duration::from_secs(30))
        .await?
        .ok_or("report claim")?;
    assert!(matches!(
        database
            .actions()
            .prepare_report(&claim, &enabled())
            .await?,
        ReportPreparation::Deferred
    ));
    assert_eq!(
        state(database, claim.record_id).await?,
        ("pending".into(), 0)
    );
    assert!(!selected.project.root.join("RESULTS.md").exists());
    for (index, member) in fixture.prepared.submissions.iter().enumerate() {
        sqlx::query("INSERT INTO actions (id, project_id, job_id, attempt_id, kind, state, spec_json, idempotency_key, available_at) VALUES (?, ?, ?, ?, 'extract_metrics', ?, '{}', ?, '2000-01-01')")
            .bind(ActionId::new().to_string()).bind(member.job.project_id.to_string()).bind(member.job.id.to_string())
            .bind(member.attempt.id().to_string()).bind(if index == 0 { "failed" } else { "cancelled" })
            .bind(member.attempt.id().to_string()).execute(database.pool()).await?;
    }
    sqlx::query("UPDATE actions SET available_at = '2000-01-01' WHERE kind = 'generate_report'")
        .execute(database.pool())
        .await?;
    let new = database
        .actions()
        .claim_report("reporter", Duration::from_secs(30))
        .await?
        .ok_or("ready report")?;
    let locked = prepare(database, &new).await?.stage_and_lock()?;
    let ReportPublishOutcome::Published(receipt) = database
        .actions()
        .publish_report(&new, locked, &enabled())
        .await?
    else {
        return Err("terminal extraction failed to unblock report".into());
    };
    let markdown = fs::read_to_string(receipt.path)?;
    assert!(markdown.contains("Status: **Ready**"));
    assert!(markdown.contains("No primary scalar metrics were supplied."));
    assert!(
        database
            .jobs()
            .list(None)
            .await?
            .iter()
            .all(|job| job.state == JobState::Succeeded)
    );
    Ok(())
}

#[tokio::test]
async fn report_execution_busy_directory_does_not_hold_the_sqlite_writer_or_spend_retry_budget()
-> TestResult {
    let (fixture, claim) = setup().await?;
    let database = &fixture.database;
    let prepared = prepare(database, &claim).await?;
    let root = prepared.project().root.clone();
    let lock = nix::fcntl::Flock::lock(fs::File::open(&root)?, nix::fcntl::FlockArg::LockExclusive)
        .map_err(|(_, error)| io::Error::from_raw_os_error(error as i32))?;
    let error = tokio::task::spawn_blocking(move || prepared.stage_and_lock())
        .await?
        .err()
        .ok_or("directory lock was bypassed")?;
    assert!(matches!(
        error,
        ReportExecutionError::Generated(igor_core::GeneratedReportError::Publication(
            igor_core::ReportPublicationError::Busy { .. }
        ))
    ));
    let mut writer = tokio::time::timeout(
        Duration::from_secs(1),
        database.pool().begin_with("BEGIN IMMEDIATE"),
    )
    .await??;
    sqlx::query("UPDATE jobs SET priority = priority + 1")
        .execute(&mut *writer)
        .await?;
    writer.commit().await?;
    database
        .actions()
        .defer_report(&claim, "report_output_busy")
        .await?;
    assert_eq!(
        state(database, claim.record_id).await?,
        ("pending".into(), 0)
    );
    assert!(!root.join("RESULTS.md").exists());
    drop(lock);
    Ok(())
}

#[tokio::test]
async fn report_execution_commit_failure_is_uncertain_and_reconciles_without_terminal_failure()
-> TestResult {
    let (fixture, claim) = setup().await?;
    let database = &fixture.database;
    let output = old_report(&fixture).await?;
    let previous = fs::read(&output)?;
    sqlx::query("INSERT INTO recoveries (id, attempt_id, state, diagnosis_json, idempotency_key) VALUES (?, ?, 'succeeded', '{\"reason\":\"new evidence\"}', ?)")
        .bind(uuid::Uuid::new_v4().to_string()).bind(fixture.prepared.submissions[0].attempt.id().to_string())
        .bind(uuid::Uuid::new_v4().to_string()).execute(database.pool()).await?;
    let locked = prepare(database, &claim).await?.stage_and_lock()?;
    // Deferred FK fails COMMIT, after a successful rename, rather than SQL preparation.
    sqlx::raw_sql("CREATE TABLE report_commit_fault (project_id TEXT REFERENCES projects(id) DEFERRABLE INITIALLY DEFERRED);
        CREATE TRIGGER report_commit_failure AFTER INSERT ON events WHEN NEW.kind = 'action_state_changed' AND json_extract(NEW.payload_json, '$.data.state') = 'succeeded'
        BEGIN INSERT INTO report_commit_fault VALUES ('missing-project'); END;")
        .execute(database.pool()).await?;
    let error = database
        .actions()
        .publish_report(&claim, locked, &enabled())
        .await
        .err()
        .ok_or("commit unexpectedly succeeded")?;
    let ReportExecutionError::PublishedUncertain { report, .. } = error else {
        return Err("lost post-rename uncertainty".into());
    };
    let visible = fs::read(&output)?;
    assert_ne!(visible, previous);
    assert_eq!(
        report.sha256,
        format!("sha256:{:x}", sha2::Sha256::digest(&visible))
    );
    assert_eq!(state(database, claim.record_id).await?.0, "running");
    sqlx::raw_sql("DROP TRIGGER report_commit_failure; DROP TABLE report_commit_fault;")
        .execute(database.pool())
        .await?;
    database
        .actions()
        .defer_report(&claim, "report_publication_uncertain")
        .await?;
    assert_eq!(
        state(database, claim.record_id).await?,
        ("pending".into(), 0)
    );
    sqlx::query("UPDATE actions SET available_at = '2000-01-01' WHERE id = ?")
        .bind(claim.record_id.to_string())
        .execute(database.pool())
        .await?;
    let retry = database
        .actions()
        .claim_report("reconciler", Duration::from_secs(30))
        .await?
        .ok_or("retry claim")?;
    let locked = prepare(database, &retry).await?.stage_and_lock()?;
    assert!(matches!(
        database
            .actions()
            .publish_report(&retry, locked, &enabled())
            .await?,
        ReportPublishOutcome::Published(_)
    ));
    assert_eq!(fs::read(&output)?, visible);
    assert_eq!(state(database, claim.record_id).await?.0, "succeeded");
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM attempts")
            .fetch_one(database.pool())
            .await?,
        2
    );
    Ok(())
}

#[tokio::test]
async fn report_execution_started_publication_survives_caller_future_cancellation() -> TestResult {
    let (fixture, claim) = setup().await?;
    let database = &fixture.database;
    let prepared = prepare(database, &claim).await?;
    let output = prepared.project().root.join("RESULTS.md");
    let locked = prepared.stage_and_lock()?;
    let writer = database.pool().begin_with("BEGIN IMMEDIATE").await?;
    let db = database.clone();
    let lease = claim.clone();
    let (started, receiver) = oneshot::channel();
    let caller = tokio::spawn(async move {
        let policy = enabled();
        let actions = db.actions();
        let future = actions.publish_report(&lease, locked, &policy);
        tokio::pin!(future);
        let first =
            std::future::poll_fn(|cx| std::task::Poll::Ready(future.as_mut().poll(cx))).await;
        match first {
            std::task::Poll::Pending => {
                let _ = started.send(true);
                future.await
            }
            std::task::Poll::Ready(result) => {
                let _ = started.send(false);
                result
            }
        }
    });
    assert!(receiver.await?);
    caller.abort();
    assert!(caller.await.is_err());
    writer.commit().await?;
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if state(database, claim.record_id).await?.0 == "succeeded" {
                return Ok::<_, sqlx::Error>(());
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await??;
    assert!(fs::read_to_string(output)?.contains("Status: **Ready**"));
    Ok(())
}
