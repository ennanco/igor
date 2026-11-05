use super::report_actions::{candidate, enabled, succeed};
use super::*;
use igor_core::{ReportActionSpec, ReportOrigin, ReportPreparation, ReportPublishOutcome};

async fn ready() -> Result<FamilyFixture, Box<dyn Error>> {
    let fixture = family_fixture().await?;
    fixture
        .database
        .families()
        .submit(&fixture.prepared)
        .await?;
    succeed(&fixture.database, &fixture.prepared).await?;
    Ok(fixture)
}

#[tokio::test]
async fn manual_report_requests_replay_without_rewriting_policy_or_retry_budget() -> TestResult {
    let fixture = ready().await?;
    let database = &fixture.database;
    let selected = candidate(&fixture).await?;
    let request_id = ActionId::new();
    let receipt = database
        .actions()
        .enqueue_manual_report(&selected, &enabled(), request_id)
        .await?;
    assert_eq!(receipt.action_id, request_id);
    assert_eq!(receipt.state, ActionState::Pending);
    assert_eq!(receipt.generation, selected.generation);
    assert_eq!(receipt.output, selected.project.root.join("RESULTS.md"));
    let spec: String = sqlx::query_scalar("SELECT spec_json FROM actions WHERE id = ?")
        .bind(request_id.to_string())
        .fetch_one(database.pool())
        .await?;
    let frozen: ReportActionSpec = serde_json::from_str(&spec)?;
    assert_eq!(frozen.origin, ReportOrigin::Manual { request_id });
    sqlx::query("UPDATE actions SET state = 'failed', attempts = 3 WHERE id = ?")
        .bind(request_id.to_string())
        .execute(database.pool())
        .await?;
    let replay = database
        .actions()
        .enqueue_manual_report(&selected, &igor_core::ReportConfig::default(), request_id)
        .await?;
    assert_eq!(replay.action_id, receipt.action_id);
    assert_eq!(replay.state, ActionState::Failed);
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT attempts FROM actions WHERE id = ?")
            .bind(request_id.to_string())
            .fetch_one(database.pool())
            .await?,
        3
    );
    let fresh_id = ActionId::new();
    assert_eq!(
        database
            .actions()
            .enqueue_manual_report(&selected, &enabled(), fresh_id)
            .await?
            .action_id,
        fresh_id
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM actions WHERE kind = 'generate_report'")
            .fetch_one(database.pool())
            .await?,
        2
    );
    let events: i64 = sqlx::query_scalar("SELECT count(*) FROM events WHERE kind = 'action_state_changed' AND json_extract(payload_json, '$.data.record_id') = ?")
        .bind(request_id.to_string()).fetch_one(database.pool()).await?;
    assert_eq!(events, 1);
    Ok(())
}

#[tokio::test]
async fn manual_report_requests_reject_disabled_incomplete_and_wrong_scope() -> TestResult {
    let fixture = family_fixture().await?;
    let database = &fixture.database;
    database.families().submit(&fixture.prepared).await?;
    let selected = candidate(&fixture).await?;
    assert!(
        database
            .actions()
            .report_candidate(selected.family_id, Some(ProjectId::new()))
            .await?
            .is_none()
    );
    assert!(
        database
            .actions()
            .report_candidate(FamilyId::new(), None)
            .await?
            .is_none()
    );
    assert!(matches!(
        database
            .actions()
            .enqueue_manual_report(
                &selected,
                &igor_core::ReportConfig::default(),
                ActionId::new()
            )
            .await,
        Err(PersistenceError::ReportDisabled)
    ));
    assert!(matches!(
        database
            .actions()
            .enqueue_manual_report(&selected, &enabled(), ActionId::new())
            .await,
        Err(PersistenceError::GenerationNotComparable { .. })
    ));
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM actions")
            .fetch_one(database.pool())
            .await?,
        0
    );
    succeed(database, &fixture.prepared).await?;
    let request_id = ActionId::new();
    database
        .actions()
        .enqueue_manual_report(&selected, &enabled(), request_id)
        .await?;
    assert!(matches!(
        database
            .actions()
            .manual_report_request(request_id, FamilyId::new(), None)
            .await,
        Err(PersistenceError::NotFound { .. })
    ));
    assert!(matches!(
        database
            .actions()
            .manual_report_request(request_id, selected.family_id, Some(ProjectId::new()))
            .await,
        Err(PersistenceError::NotFound { .. })
    ));
    Ok(())
}

#[tokio::test]
async fn manual_report_requests_replay_original_generation_after_supersession_or_deregistration()
-> TestResult {
    let fixture = ready().await?;
    let database = &fixture.database;
    let selected = candidate(&fixture).await?;
    let request_id = ActionId::new();
    let original = database
        .actions()
        .enqueue_manual_report(&selected, &enabled(), request_id)
        .await?;
    let next = successor_for(&fixture).await?;
    database
        .families()
        .supersede(selected.generation.id, &next)
        .await?;
    let current = database
        .actions()
        .report_candidate(selected.family_id, None)
        .await?
        .ok_or("current generation")?;
    assert_ne!(current.generation.id, original.generation.id);
    assert_eq!(
        database
            .actions()
            .enqueue_manual_report(&current, &enabled(), request_id)
            .await?,
        original
    );
    assert!(
        database
            .actions()
            .enqueue_manual_report(&current, &enabled(), ActionId::new())
            .await
            .is_err()
    );
    for job in database.jobs().list(Some(selected.project.id)).await? {
        if job.state == JobState::Queued {
            database
                .jobs()
                .request_cancellation(job.spec.id, Duration::ZERO)
                .await?;
        }
    }
    database.projects().remove(&selected.project.root).await?;
    assert_eq!(
        database
            .actions()
            .manual_report_request(request_id, selected.family_id, None)
            .await?,
        Some(original)
    );
    Ok(())
}

#[tokio::test]
async fn manual_report_requests_racing_connections_share_one_action_and_event() -> TestResult {
    let fixture = ready().await?;
    let database = &fixture.database;
    let other = Database::open(fixture._database.path().join("nested/igor.db")).await?;
    let selected = candidate(&fixture).await?;
    let request_id = ActionId::new();
    let config = enabled();
    let gate = Barrier::new(2);
    let (a, b) = tokio::join!(
        async {
            gate.wait().await;
            database
                .actions()
                .enqueue_manual_report(&selected, &config, request_id)
                .await
        },
        async {
            gate.wait().await;
            other
                .actions()
                .enqueue_manual_report(&selected, &config, request_id)
                .await
        }
    );
    assert_eq!(a?, b?);
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM actions WHERE kind = 'generate_report'")
            .fetch_one(database.pool())
            .await?,
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM events WHERE kind = 'action_state_changed'"
        )
        .fetch_one(database.pool())
        .await?,
        1
    );
    Ok(())
}

#[tokio::test]
async fn manual_report_requests_event_failure_rolls_back_and_nonce_collision_fails_closed()
-> TestResult {
    let fixture = ready().await?;
    let database = &fixture.database;
    let selected = candidate(&fixture).await?;
    let request_id = ActionId::new();
    sqlx::raw_sql("CREATE TRIGGER manual_report_fault BEFORE INSERT ON events WHEN NEW.kind = 'action_state_changed' BEGIN SELECT RAISE(ABORT, 'manual report event fault'); END;")
        .execute(database.pool()).await?;
    assert!(
        database
            .actions()
            .enqueue_manual_report(&selected, &enabled(), request_id)
            .await
            .is_err()
    );
    assert!(
        database
            .actions()
            .manual_report_request(request_id, selected.family_id, None)
            .await?
            .is_none()
    );
    sqlx::raw_sql("DROP TRIGGER manual_report_fault")
        .execute(database.pool())
        .await?;
    database
        .actions()
        .enqueue_manual_report(&selected, &enabled(), request_id)
        .await?;
    let collision = ActionId::new();
    database
        .actions()
        .insert(&ActionRecord {
            id: collision,
            project_id: selected.project.id,
            kind: "send_notification".into(),
            state: ActionState::Pending,
            spec: json!({}),
            idempotency_key: "non-report-collision".into(),
        })
        .await?;
    assert!(matches!(
        database
            .actions()
            .enqueue_manual_report(&selected, &enabled(), collision)
            .await,
        Err(PersistenceError::Conflict { .. })
    ));
    Ok(())
}

#[tokio::test]
async fn manual_report_requests_execute_with_the_live_gate_and_legacy_origins_still_parse()
-> TestResult {
    let fixture = ready().await?;
    let database = &fixture.database;
    let selected = candidate(&fixture).await?;
    let request_id = ActionId::new();
    database
        .actions()
        .enqueue_manual_report(&selected, &enabled(), request_id)
        .await?;
    let claim = database
        .actions()
        .claim_report("manual consumer", Duration::from_secs(30))
        .await?
        .ok_or("manual claim")?;
    let ReportPreparation::Ready(prepared) = database
        .actions()
        .prepare_report(&claim, &enabled())
        .await?
    else {
        return Err("manual request not ready".into());
    };
    let publication = prepared.stage_and_lock()?;
    assert!(matches!(
        database
            .actions()
            .publish_report(&claim, publication, &enabled())
            .await?,
        ReportPublishOutcome::Published(_)
    ));
    assert_eq!(
        database
            .actions()
            .manual_report_request(request_id, selected.family_id, None)
            .await?
            .ok_or("receipt")?
            .state,
        ActionState::Succeeded
    );
    let auto_id = database
        .actions()
        .enqueue_family_report(&selected, &enabled())
        .await?
        .ok_or("automatic intent")?;
    sqlx::query("UPDATE actions SET spec_json = json_remove(spec_json, '$.origin') WHERE id = ?")
        .bind(auto_id.to_string())
        .execute(database.pool())
        .await?;
    let auto = database
        .actions()
        .claim_report("legacy consumer", Duration::from_secs(30))
        .await?
        .ok_or("legacy claim")?;
    assert!(matches!(
        database.actions().prepare_report(&auto, &enabled()).await?,
        ReportPreparation::Ready(_)
    ));
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM attempts")
            .fetch_one(database.pool())
            .await?,
        2
    );
    Ok(())
}
