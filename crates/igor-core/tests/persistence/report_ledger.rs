use super::report_actions::{candidate, enabled, succeed};
use super::*;
use igor_core::{
    Claim, PreparedReport, ReportExecutionError, ReportId, ReportPreparation, ReportPublishOutcome,
    ReportState,
};
use std::os::unix::fs::MetadataExt;

async fn setup() -> Result<(FamilyFixture, Claim<ActionId>), Box<dyn Error>> {
    let fixture = family_fixture().await?;
    let database = &fixture.database;
    database.families().submit(&fixture.prepared).await?;
    succeed(database, &fixture.prepared).await?;
    let selected = candidate(&fixture).await?;
    database
        .actions()
        .enqueue_manual_report(&selected, &enabled(), ActionId::new())
        .await?;
    let claim = database
        .actions()
        .claim_report("ledger test", Duration::from_secs(30))
        .await?
        .ok_or("claim")?;
    Ok((fixture, claim))
}

async fn prepare(
    database: &Database,
    claim: &Claim<ActionId>,
) -> Result<PreparedReport, Box<dyn Error>> {
    match database.actions().prepare_report(claim, &enabled()).await? {
        ReportPreparation::Ready(prepared) => Ok(*prepared),
        other => Err(format!("not ready: {other:?}").into()),
    }
}

async fn expire(database: &Database, claim: &Claim<ActionId>) -> TestResult {
    sqlx::query("UPDATE actions SET claim_expires_at = '2000-01-01' WHERE id = ?")
        .bind(claim.record_id.to_string())
        .execute(database.pool())
        .await?;
    Ok(())
}

async fn commit_fault(database: &Database, enabled: bool) -> TestResult {
    if enabled {
        sqlx::raw_sql("CREATE TABLE ledger_commit_fault (project_id TEXT REFERENCES projects(id) DEFERRABLE INITIALLY DEFERRED);
            CREATE TRIGGER ledger_commit_fault AFTER INSERT ON events WHEN NEW.kind = 'action_state_changed' AND json_extract(NEW.payload_json, '$.data.state') = 'succeeded'
            BEGIN INSERT INTO ledger_commit_fault VALUES ('missing'); END;").execute(database.pool()).await?;
    } else {
        sqlx::raw_sql("DROP TRIGGER ledger_commit_fault; DROP TABLE ledger_commit_fault;")
            .execute(database.pool())
            .await?;
    }
    Ok(())
}

#[tokio::test]
async fn report_ledger_persists_exact_context_before_staging_and_hashes_before_ack() -> TestResult {
    let (fixture, claim) = setup().await?;
    let database = &fixture.database;
    let prepared = prepare(database, &claim).await?;
    let id = prepared.run_id().ok_or("run")?;
    let initial = database
        .actions()
        .report_run(id)
        .await?
        .ok_or("stored run")?;
    assert_eq!(initial.state, ReportState::Generating);
    assert_eq!(initial.phase.as_deref(), Some("context"));
    assert!(initial.output_hash.is_none());
    let context = initial.context.as_ref().ok_or("stored context")?;
    assert_eq!(
        initial.context_hash,
        format!(
            "sha256:{:x}",
            sha2::Sha256::digest(serde_json::to_vec(context)?)
        )
    );
    assert_eq!(initial.action_id, Some(claim.record_id));
    assert_eq!(initial.initial_lease_id, Some(claim.lease_id));
    assert_eq!(initial.lease_owner.as_deref(), Some(claim.owner.as_str()));
    assert_eq!(initial.provider, "igor");
    assert_eq!(initial.model, "deterministic-format-v1");
    assert_eq!(prepare(database, &claim).await?.run_id(), Some(id));
    assert_eq!(
        database.actions().report_runs(claim.record_id).await?.len(),
        1
    );
    let readonly = Database::open_with_options(
        fixture._database.path().join("nested/igor.db"),
        DatabaseOptions {
            writable: false,
            create_if_missing: false,
            ..Default::default()
        },
    )
    .await?;
    assert_eq!(
        readonly.actions().report_run(id).await?,
        Some(initial.clone())
    );
    let locked = prepared.stage_and_lock()?;
    let ReportPublishOutcome::Published(receipt) = database
        .actions()
        .publish_report(&claim, locked, &enabled())
        .await?
    else {
        return Err("not published".into());
    };
    let run = readonly
        .actions()
        .report_run(id)
        .await?
        .ok_or("published run")?;
    assert_eq!(run.context_hash, initial.context_hash);
    assert_eq!(run.context, initial.context);
    assert_eq!(run.state, ReportState::Published);
    assert_eq!(run.phase.as_deref(), Some("published"));
    assert_eq!(run.output_hash.as_deref(), Some(receipt.sha256.as_str()));
    assert_eq!(
        run.output_size_bytes,
        Some(fs::metadata(&receipt.path)?.len())
    );
    assert!(run.prepared_at.is_some() && run.published_at.is_some());
    assert_eq!(
        receipt.sha256,
        format!(
            "sha256:{:x}",
            sha2::Sha256::digest(fs::read(&receipt.path)?)
        )
    );
    for sql in [
        "UPDATE report_runs SET context_json = '{}' WHERE action_id IS NOT NULL",
        "UPDATE report_runs SET context_hash = 'tampered' WHERE action_id IS NOT NULL",
        "UPDATE report_runs SET output_hash = 'tampered' WHERE action_id IS NOT NULL",
        "DELETE FROM report_runs WHERE action_id IS NOT NULL",
    ] {
        assert!(sqlx::query(sql).execute(database.pool()).await.is_err());
    }
    Ok(())
}

#[tokio::test]
async fn report_ledger_prepared_intent_survives_sql_failure_and_reopen_before_rename() -> TestResult
{
    let (fixture, claim) = setup().await?;
    let database = &fixture.database;
    let prepared = prepare(database, &claim).await?;
    let id = prepared.run_id().ok_or("run")?;
    let output = prepared.project().root.join("RESULTS.md");
    let locked = prepared.stage_and_lock()?;
    sqlx::raw_sql("CREATE TRIGGER ledger_before_rename BEFORE INSERT ON events WHEN NEW.kind = 'action_state_changed' AND json_extract(NEW.payload_json, '$.data.state') = 'succeeded' BEGIN SELECT RAISE(ABORT, 'before rename'); END;")
        .execute(database.pool()).await?;
    assert!(
        database
            .actions()
            .publish_report(&claim, locked, &enabled())
            .await
            .is_err()
    );
    assert!(!output.exists());
    let expected = database
        .actions()
        .report_run(id)
        .await?
        .ok_or("prepared intent")?;
    assert_eq!(expected.phase.as_deref(), Some("prepared"));
    assert_eq!(expected.state, ReportState::Generating);
    assert!(expected.output_hash.is_some());
    sqlx::raw_sql("DROP TRIGGER ledger_before_rename")
        .execute(database.pool())
        .await?;
    expire(database, &claim).await?;
    database.pool().close().await;
    let reopened = Database::open(fixture._database.path().join("nested/igor.db")).await?;
    let retry = reopened
        .actions()
        .claim_report("reopened", Duration::from_secs(30))
        .await?
        .ok_or("retry")?;
    let prepared = prepare(&reopened, &retry).await?;
    assert_eq!(prepared.run_id(), Some(id));
    let resumed = reopened.actions().report_run(id).await?.ok_or("resumed")?;
    assert_eq!(resumed.initial_lease_id, Some(claim.lease_id));
    assert_eq!(resumed.lease_id, Some(retry.lease_id));
    assert_eq!(resumed.context_hash, expected.context_hash);
    assert!(matches!(
        reopened
            .actions()
            .publish_report(&retry, prepared.stage_and_lock()?, &enabled())
            .await?,
        ReportPublishOutcome::Published(_)
    ));
    assert_eq!(
        reopened.actions().report_runs(claim.record_id).await?.len(),
        1
    );
    Ok(())
}

#[tokio::test]
async fn report_ledger_after_rename_recovery_confirms_matching_output_without_rewriting_inode()
-> TestResult {
    let (fixture, claim) = setup().await?;
    let database = &fixture.database;
    let prepared = prepare(database, &claim).await?;
    let id = prepared.run_id().ok_or("run")?;
    let locked = prepared.stage_and_lock()?;
    commit_fault(database, true).await?;
    let error = database
        .actions()
        .publish_report(&claim, locked, &enabled())
        .await
        .err()
        .ok_or("commit succeeded")?;
    let ReportExecutionError::PublishedUncertain { report, .. } = error else {
        return Err("not uncertain".into());
    };
    let inode = fs::metadata(&report.path)?.ino();
    let bytes = fs::read(&report.path)?;
    let intent = database.actions().report_run(id).await?.ok_or("intent")?;
    assert_eq!(intent.phase.as_deref(), Some("prepared"));
    assert_eq!(intent.output_hash.as_deref(), Some(report.sha256.as_str()));
    commit_fault(database, false).await?;
    expire(database, &claim).await?;
    database.pool().close().await;
    let database = Database::open(fixture._database.path().join("nested/igor.db")).await?;
    let retry = database
        .actions()
        .claim_report("reconcile", Duration::from_secs(30))
        .await?
        .ok_or("claim")?;
    let prepared = prepare(&database, &retry).await?;
    assert_eq!(prepared.run_id(), Some(id));
    assert!(matches!(
        database
            .actions()
            .publish_report(&retry, prepared.stage_and_lock()?, &enabled())
            .await?,
        ReportPublishOutcome::Published(_)
    ));
    assert_eq!(fs::metadata(&report.path)?.ino(), inode);
    assert_eq!(fs::read(&report.path)?, bytes);
    assert_eq!(
        database.actions().report_run(id).await?.ok_or("run")?.state,
        ReportState::Published
    );
    Ok(())
}

#[tokio::test]
async fn report_ledger_unresolved_intent_never_overwrites_a_newer_report_even_after_deferral()
-> TestResult {
    let (fixture, old) = setup().await?;
    let database = &fixture.database;
    let selected = candidate(&fixture).await?;
    let prepared = prepare(database, &old).await?;
    let old_id = prepared.run_id().ok_or("old run")?;
    commit_fault(database, true).await?;
    assert!(matches!(
        database
            .actions()
            .publish_report(&old, prepared.stage_and_lock()?, &enabled())
            .await,
        Err(ReportExecutionError::PublishedUncertain { .. })
    ));
    commit_fault(database, false).await?;
    sqlx::query("INSERT INTO recoveries (id, attempt_id, state, diagnosis_json, idempotency_key) VALUES (?, ?, 'succeeded', '{\"reason\":\"newer evidence\"}', ?)")
        .bind(uuid::Uuid::new_v4().to_string()).bind(fixture.prepared.submissions[0].attempt.id().to_string())
        .bind(uuid::Uuid::new_v4().to_string()).execute(database.pool()).await?;
    let new_id = ActionId::new();
    database
        .actions()
        .enqueue_manual_report(&selected, &enabled(), new_id)
        .await?;
    let newer = database
        .actions()
        .claim_report("newer request", Duration::from_secs(30))
        .await?
        .ok_or("new request")?;
    assert_eq!(newer.record_id, new_id);
    let new_prepared = prepare(database, &newer).await?;
    let new_run_id = new_prepared.run_id().ok_or("new run")?;
    let ReportPublishOutcome::Published(new_file) = database
        .actions()
        .publish_report(&newer, new_prepared.stage_and_lock()?, &enabled())
        .await?
    else {
        return Err("newer not published".into());
    };
    let bytes = fs::read(&new_file.path)?;
    let inode = fs::metadata(&new_file.path)?.ino();
    expire(database, &old).await?;
    let recovery = database
        .actions()
        .claim_report("recover old", Duration::from_secs(30))
        .await?
        .ok_or("old reclaim")?;
    let interrupted = prepare(database, &recovery).await?;
    assert_ne!(interrupted.run_id(), Some(old_id));
    drop(interrupted);
    database
        .actions()
        .defer_report(&recovery, "report_output_busy")
        .await?;
    assert_eq!(
        database
            .actions()
            .report_run(old_id)
            .await?
            .ok_or("old intent")?
            .state,
        ReportState::Generating
    );
    sqlx::query("UPDATE actions SET available_at = '2000-01-01' WHERE id = ?")
        .bind(old.record_id.to_string())
        .execute(database.pool())
        .await?;
    let recovery = database
        .actions()
        .claim_report("recover again", Duration::from_secs(30))
        .await?
        .ok_or("reclaim")?;
    let prepared = prepare(database, &recovery).await?;
    assert_eq!(
        database
            .actions()
            .publish_report(&recovery, prepared.stage_and_lock()?, &enabled())
            .await?,
        ReportPublishOutcome::Cancelled
    );
    assert_eq!(fs::read(&new_file.path)?, bytes);
    assert_eq!(fs::metadata(&new_file.path)?.ino(), inode);
    assert_eq!(
        database
            .actions()
            .report_run(new_run_id)
            .await?
            .ok_or("new run")?
            .state,
        ReportState::Published
    );
    assert_eq!(
        database
            .actions()
            .report_run(old_id)
            .await?
            .ok_or("old run")?
            .state,
        ReportState::Cancelled
    );
    Ok(())
}

#[tokio::test]
async fn report_ledger_failed_execution_and_explicit_retry_keep_distinct_run_history() -> TestResult
{
    let (fixture, claim) = setup().await?;
    let database = &fixture.database;
    let selected = candidate(&fixture).await?;
    let prepared = prepare(database, &claim).await?;
    let failed_id = prepared.run_id().ok_or("run")?;
    let output = selected.project.root.join("RESULTS.md");
    fs::write(&output, "handwritten")?;
    assert!(prepared.stage_and_lock().is_err());
    let policy = igor_core::RetryPolicy {
        max_attempts: 1,
        initial_delay_seconds: 0,
        max_delay_seconds: 0,
        multiplier: 1,
    };
    database
        .actions()
        .retry(&claim, &policy, "unmanaged_report")
        .await?;
    let failed = database
        .actions()
        .report_run(failed_id)
        .await?
        .ok_or("failed run")?;
    assert_eq!(failed.state, ReportState::Failed);
    assert_eq!(failed.error.as_deref(), Some("unmanaged_report"));
    assert_eq!(failed.output_hash, None);
    database
        .actions()
        .retry_family_report(&selected, &enabled(), ActionId::new())
        .await?;
    fs::remove_file(&output)?;
    let retry = database
        .actions()
        .claim_report("retry", Duration::from_secs(30))
        .await?
        .ok_or("retry")?;
    let prepared = prepare(database, &retry).await?;
    assert_ne!(prepared.run_id(), Some(failed_id));
    database
        .actions()
        .publish_report(&retry, prepared.stage_and_lock()?, &enabled())
        .await?;
    assert_eq!(
        database.actions().report_run(failed_id).await?,
        Some(failed)
    );
    assert_eq!(
        database.actions().report_runs(claim.record_id).await?.len(),
        2
    );
    Ok(())
}

#[tokio::test]
async fn report_ledger_pending_or_cancelled_work_does_not_fabricate_output_hashes() -> TestResult {
    let (fixture, claim) = setup().await?;
    let database = &fixture.database;
    let prepared = prepare(database, &claim).await?;
    let id = prepared.run_id().ok_or("run")?;
    let locked = prepared.stage_and_lock()?;
    assert_eq!(
        database
            .actions()
            .publish_report(&claim, locked, &igor_core::ReportConfig::default())
            .await?,
        ReportPublishOutcome::Cancelled
    );
    let stored = database.actions().report_run(id).await?.ok_or("run")?;
    assert_eq!(stored.state, ReportState::Cancelled);
    assert_eq!(stored.output_hash, None);
    assert_eq!(stored.phase.as_deref(), Some("context"));
    assert!(stored.published_at.is_none());
    Ok(())
}

#[tokio::test]
async fn report_ledger_migration_keeps_legacy_provenance_unknown_and_prevents_adoption()
-> TestResult {
    let (fixture, claim) = setup().await?;
    let database = &fixture.database;
    drop_report_ledger(database).await?;
    let id = ReportId::new();
    sqlx::query("INSERT INTO report_runs (id, generation_id, state, provider, model, prompt_hash, context_hash, output_hash, idempotency_key) VALUES (?, ?, 'published', 'legacy', 'unknown', 'old-prompt', 'old-context', 'old-output', 'legacy-run')")
        .bind(id.to_string()).bind(fixture.prepared.generation.identity.id.to_string()).execute(database.pool()).await?;
    database.pool().close().await;
    let upgraded = Database::open(fixture._database.path().join("nested/igor.db")).await?;
    assert_eq!(upgraded.schema_version().await?, 23);
    let legacy = upgraded.actions().report_run(id).await?.ok_or("legacy")?;
    assert_eq!(legacy.context_hash, "old-context");
    assert_eq!(legacy.output_hash.as_deref(), Some("old-output"));
    assert!(
        legacy.context.is_none()
            && legacy.spec.is_none()
            && legacy.action_id.is_none()
            && legacy.phase.is_none()
    );
    assert!(
        sqlx::query("UPDATE report_runs SET action_id = ? WHERE id = ?")
            .bind(claim.record_id.to_string())
            .bind(id.to_string())
            .execute(upgraded.pool())
            .await
            .is_err()
    );
    Ok(())
}

#[tokio::test]
async fn report_ledger_changed_context_replaces_unpublished_intent_only_when_baseline_matches()
-> TestResult {
    let (fixture, claim) = setup().await?;
    let database = &fixture.database;
    let prepared = prepare(database, &claim).await?;
    let old_id = prepared.run_id().ok_or("run")?;
    sqlx::raw_sql("CREATE TRIGGER ledger_gate_fault BEFORE INSERT ON events WHEN NEW.kind = 'action_state_changed' AND json_extract(NEW.payload_json, '$.data.state') = 'succeeded' BEGIN SELECT RAISE(ABORT, 'no rename'); END;")
        .execute(database.pool()).await?;
    assert!(
        database
            .actions()
            .publish_report(&claim, prepared.stage_and_lock()?, &enabled())
            .await
            .is_err()
    );
    sqlx::raw_sql("DROP TRIGGER ledger_gate_fault")
        .execute(database.pool())
        .await?;
    sqlx::query("INSERT INTO recoveries (id, attempt_id, state, diagnosis_json, idempotency_key) VALUES (?, ?, 'succeeded', '{\"reason\":\"fresh context\"}', ?)")
        .bind(uuid::Uuid::new_v4().to_string()).bind(fixture.prepared.submissions[0].attempt.id().to_string())
        .bind(uuid::Uuid::new_v4().to_string()).execute(database.pool()).await?;
    expire(database, &claim).await?;
    let retry = database
        .actions()
        .claim_report("fresh context", Duration::from_secs(30))
        .await?
        .ok_or("claim")?;
    let prepared = prepare(database, &retry).await?;
    let new_id = prepared.run_id().ok_or("new run")?;
    assert_ne!(old_id, new_id);
    let ReportPublishOutcome::Published(receipt) = database
        .actions()
        .publish_report(&retry, prepared.stage_and_lock()?, &enabled())
        .await?
    else {
        return Err("fresh context not published".into());
    };
    assert!(fs::read_to_string(receipt.path)?.contains("fresh context"));
    let old = database.actions().report_run(old_id).await?.ok_or("old")?;
    let new = database.actions().report_run(new_id).await?.ok_or("new")?;
    assert_eq!(old.state, ReportState::Cancelled);
    assert_eq!(old.phase.as_deref(), Some("prepared"));
    assert_ne!(old.context_hash, new.context_hash);
    assert_eq!(new.state, ReportState::Published);
    Ok(())
}

#[tokio::test]
async fn report_ledger_new_request_supersedes_acknowledged_history_without_mutating_evidence()
-> TestResult {
    let (fixture, claim) = setup().await?;
    let database = &fixture.database;
    let prepared = prepare(database, &claim).await?;
    let old_id = prepared.run_id().ok_or("old")?;
    let ReportPublishOutcome::Published(first) = database
        .actions()
        .publish_report(&claim, prepared.stage_and_lock()?, &enabled())
        .await?
    else {
        return Err("first not published".into());
    };
    let old = database.actions().report_run(old_id).await?.ok_or("old")?;
    let inode = fs::metadata(&first.path)?.ino();
    let selected = candidate(&fixture).await?;
    database
        .actions()
        .enqueue_manual_report(&selected, &enabled(), ActionId::new())
        .await?;
    let next = database
        .actions()
        .claim_report("new request", Duration::from_secs(30))
        .await?
        .ok_or("next")?;
    let prepared = prepare(database, &next).await?;
    let new_id = prepared.run_id().ok_or("new")?;
    database
        .actions()
        .publish_report(&next, prepared.stage_and_lock()?, &enabled())
        .await?;
    let previous = database
        .actions()
        .report_run(old_id)
        .await?
        .ok_or("history")?;
    assert_eq!(previous.state, ReportState::Superseded);
    assert_eq!(previous.context, old.context);
    assert_eq!(previous.context_hash, old.context_hash);
    assert_eq!(previous.output_hash, old.output_hash);
    assert_eq!(previous.published_at, old.published_at);
    assert_eq!(
        database
            .actions()
            .report_run(new_id)
            .await?
            .ok_or("new")?
            .state,
        ReportState::Published
    );
    assert_eq!(fs::metadata(&first.path)?.ino(), inode);
    Ok(())
}
