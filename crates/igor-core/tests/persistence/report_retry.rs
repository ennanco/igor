use super::report_actions::{candidate, enabled, succeed};
use super::*;
use igor_core::{ReportPreparation, ReportPublishOutcome};
use serde_json::Value;

async fn fixture(
    manual: bool,
) -> Result<(FamilyFixture, igor_core::ReportCandidate, ActionId), Box<dyn Error>> {
    let fixture = family_fixture().await?;
    let database = &fixture.database;
    database.families().submit(&fixture.prepared).await?;
    succeed(database, &fixture.prepared).await?;
    let selected = candidate(&fixture).await?;
    let id = if manual {
        database
            .actions()
            .enqueue_manual_report(&selected, &enabled(), ActionId::new())
            .await?
            .action_id
    } else {
        database
            .actions()
            .enqueue_family_report(&selected, &enabled())
            .await?
            .ok_or("automatic action")?
    };
    if !manual {
        sqlx::query(
            "UPDATE actions SET spec_json = json_remove(spec_json, '$.origin') WHERE id = ?",
        )
        .bind(id.to_string())
        .execute(database.pool())
        .await?;
    }
    fail(database, id).await?;
    Ok((fixture, selected, id))
}

async fn fail(database: &Database, id: ActionId) -> TestResult {
    sqlx::query("UPDATE actions SET state = 'failed', attempts = 3, last_error = 'report_execution_failed', claim_id = NULL, claim_owner = NULL, claim_expires_at = NULL WHERE id = ?")
        .bind(id.to_string()).execute(database.pool()).await?;
    Ok(())
}

async fn row(
    database: &Database,
    id: ActionId,
) -> Result<(String, i64, Option<String>, String, String), sqlx::Error> {
    sqlx::query_as(
        "SELECT state, attempts, last_error, spec_json, idempotency_key FROM actions WHERE id = ?",
    )
    .bind(id.to_string())
    .fetch_one(database.pool())
    .await
}

#[tokio::test]
async fn report_retry_preserves_action_spec_and_history_and_replay_never_resets_twice() -> TestResult
{
    for manual in [false, true] {
        let (fixture, selected, id) = fixture(manual).await?;
        let database = &fixture.database;
        let original = row(database, id).await?;
        let request_id = ActionId::new();
        let receipt = database
            .actions()
            .retry_family_report(&selected, &enabled(), request_id)
            .await?;
        assert_eq!(receipt.request_id, request_id);
        assert_eq!(receipt.report.action_id, id);
        assert_eq!(receipt.report.state, ActionState::Pending);
        let after = row(database, id).await?;
        assert_eq!(after.0, "pending");
        assert_eq!(after.1, 0);
        assert_eq!(after.2, None);
        assert_eq!((&after.3, &after.4), (&original.3, &original.4));
        let payload: Value = serde_json::from_str(
            &sqlx::query_scalar::<_, String>("SELECT payload_json FROM events WHERE id = ?")
                .bind(request_id.to_string())
                .fetch_one(database.pool())
                .await?,
        )?;
        assert_eq!(payload["data"]["details"]["previous_attempts"], 3);
        assert_eq!(
            payload["data"]["details"]["previous_error"],
            "report_execution_failed"
        );
        let claim = database
            .actions()
            .claim_report("reporter", Duration::from_secs(30))
            .await?
            .ok_or("claim")?;
        assert_eq!(claim.record_id, id);
        let replay = database
            .actions()
            .retry_family_report(&selected, &igor_core::ReportConfig::default(), request_id)
            .await?;
        assert_eq!(replay.report.state, ActionState::Running);
        assert_eq!(row(database, id).await?.1, 1);
        assert!(database.actions().claimed(&claim).await?.is_some());
        fail(database, id).await?;
        assert_eq!(
            database
                .actions()
                .retry_family_report(&selected, &enabled(), request_id)
                .await?
                .report
                .state,
            ActionState::Failed
        );
        assert_eq!(row(database, id).await?.1, 3);
        database
            .actions()
            .retry_family_report(&selected, &enabled(), ActionId::new())
            .await?;
        assert_eq!(row(database, id).await?.1, 0);
        assert_eq!(
            sqlx::query_scalar::<_, i64>(
                "SELECT count(*) FROM actions WHERE kind = 'generate_report'"
            )
            .fetch_one(database.pool())
            .await?,
            1
        );
    }
    Ok(())
}

#[tokio::test]
async fn report_retry_uses_latest_recorded_request_not_older_failed_uuid_or_clock_order()
-> TestResult {
    for state in ["pending", "running", "succeeded", "cancelled"] {
        let fixture = family_fixture().await?;
        let database = &fixture.database;
        database.families().submit(&fixture.prepared).await?;
        succeed(database, &fixture.prepared).await?;
        let selected = candidate(&fixture).await?;
        let old: ActionId = "ffffffff-ffff-4fff-8fff-ffffffffffff".parse()?;
        let latest: ActionId = "00000000-0000-4000-8000-000000000001".parse()?;
        database
            .actions()
            .enqueue_manual_report(&selected, &enabled(), old)
            .await?;
        fail(database, old).await?;
        database
            .actions()
            .enqueue_manual_report(&selected, &enabled(), latest)
            .await?;
        // An older request's later lifecycle event must not make it the latest request.
        let late = json!({"schema_version":1,"data":{"entity":"action","record_id":old,"state":"failed","lease_id":null,"details":null}});
        sqlx::query("INSERT INTO events (id, project_id, kind, payload_json) VALUES (?, ?, 'action_state_changed', ?)")
            .bind(EventId::new().to_string()).bind(selected.project.id.to_string()).bind(late.to_string())
            .execute(database.pool()).await?;
        sqlx::query("UPDATE actions SET created_at = CASE WHEN id = ? THEN '2099-01-01' ELSE '1900-01-01' END")
            .bind(old.to_string()).execute(database.pool()).await?;
        if state == "succeeded" {
            // This fixture synthesizes lifecycle states without executing report publication.
            sqlx::raw_sql("DROP TRIGGER report_action_success_guard")
                .execute(database.pool())
                .await?;
        }
        if state == "running" {
            database
                .actions()
                .claim_report("active", Duration::from_secs(30))
                .await?
                .ok_or("claim")?;
        } else {
            sqlx::query("UPDATE actions SET state = ? WHERE id = ?")
                .bind(state)
                .bind(latest.to_string())
                .execute(database.pool())
                .await?;
        }
        assert!(matches!(
            database
                .actions()
                .retry_family_report(&selected, &enabled(), ActionId::new())
                .await,
            Err(PersistenceError::Conflict {
                entity: "latest report action is not failed"
            })
        ));
        assert_eq!(row(database, old).await?.0, "failed");
        assert_eq!(row(database, latest).await?.0, state);
    }
    Ok(())
}

#[tokio::test]
async fn report_retry_live_configuration_and_current_generation_are_required_for_new_requests()
-> TestResult {
    let (fixture, selected, id) = fixture(true).await?;
    let database = &fixture.database;
    let before = row(database, id).await?;
    assert!(matches!(
        database
            .actions()
            .retry_family_report(
                &selected,
                &igor_core::ReportConfig::default(),
                ActionId::new()
            )
            .await,
        Err(PersistenceError::ReportDisabled)
    ));
    let changed = igor_core::ReportConfig {
        output: PathBuf::from("other.md"),
        ..enabled()
    };
    assert!(
        database
            .actions()
            .retry_family_report(&selected, &changed, ActionId::new())
            .await
            .is_err()
    );
    let mut renamed = selected.project.clone();
    renamed.name = "different metadata".into();
    database.projects().update(&renamed).await?;
    assert!(
        database
            .actions()
            .retry_family_report(&selected, &enabled(), ActionId::new())
            .await
            .is_err()
    );
    database.projects().update(&selected.project).await?;
    let next = successor_for(&fixture).await?;
    database
        .families()
        .supersede(selected.generation.id, &next)
        .await?;
    assert!(
        database
            .actions()
            .retry_family_report(&selected, &enabled(), ActionId::new())
            .await
            .is_err()
    );
    assert_eq!(row(database, id).await?, before);
    Ok(())
}

#[tokio::test]
async fn report_retry_independent_writers_deduplicate_nonce_and_fence_distinct_retries()
-> TestResult {
    for same in [false, true] {
        let (fixture, selected, id) = fixture(false).await?;
        let database = &fixture.database;
        let other = Database::open(fixture._database.path().join("nested/igor.db")).await?;
        let a = ActionId::new();
        let b = if same { a } else { ActionId::new() };
        let gate = Barrier::new(2);
        let config = enabled();
        let (first, second) = tokio::join!(
            async {
                gate.wait().await;
                database
                    .actions()
                    .retry_family_report(&selected, &config, a)
                    .await
            },
            async {
                gate.wait().await;
                other
                    .actions()
                    .retry_family_report(&selected, &config, b)
                    .await
            }
        );
        if same {
            assert_eq!(first?, second?);
        } else {
            assert_eq!(usize::from(first.is_ok()) + usize::from(second.is_ok()), 1);
        }
        assert_eq!(row(database, id).await?.1, 0);
        let events: i64 = sqlx::query_scalar("SELECT count(*) FROM events WHERE json_extract(payload_json, '$.data.details.reason') = 'report_retry_requested'")
            .fetch_one(database.pool()).await?;
        assert_eq!(events, 1);
    }
    Ok(())
}

#[tokio::test]
async fn report_retry_event_failure_and_nonce_collision_preserve_failed_action() -> TestResult {
    let (fixture, selected, id) = fixture(true).await?;
    let database = &fixture.database;
    let before = row(database, id).await?;
    let request_id = ActionId::new();
    sqlx::raw_sql("CREATE TRIGGER report_retry_fault BEFORE INSERT ON events WHEN json_extract(NEW.payload_json, '$.data.details.reason') = 'report_retry_requested' BEGIN SELECT RAISE(ABORT, 'retry event fault'); END;")
        .execute(database.pool()).await?;
    assert!(
        database
            .actions()
            .retry_family_report(&selected, &enabled(), request_id)
            .await
            .is_err()
    );
    assert_eq!(row(database, id).await?, before);
    assert!(
        database
            .actions()
            .report_retry_request(request_id, selected.family_id, None)
            .await?
            .is_none()
    );
    sqlx::raw_sql("DROP TRIGGER report_retry_fault")
        .execute(database.pool())
        .await?;
    let collision: String = sqlx::query_scalar("SELECT id FROM events ORDER BY sequence LIMIT 1")
        .fetch_one(database.pool())
        .await?;
    assert!(
        database
            .actions()
            .retry_family_report(&selected, &enabled(), collision.parse()?)
            .await
            .is_err()
    );
    assert_eq!(row(database, id).await?, before);
    database
        .actions()
        .retry_family_report(&selected, &enabled(), request_id)
        .await?;
    assert!(matches!(
        database
            .actions()
            .report_retry_request(request_id, FamilyId::new(), None)
            .await,
        Err(PersistenceError::NotFound { .. })
    ));
    assert!(matches!(
        database
            .actions()
            .report_retry_request(request_id, selected.family_id, Some(ProjectId::new()))
            .await,
        Err(PersistenceError::NotFound { .. })
    ));
    Ok(())
}

#[tokio::test]
async fn report_retry_replay_survives_reopen_and_obsolescence_and_executor_revalidates()
-> TestResult {
    let (fixture, selected, id) = fixture(false).await?;
    let database = &fixture.database;
    let request_id = ActionId::new();
    database
        .actions()
        .retry_family_report(&selected, &enabled(), request_id)
        .await?;
    let next = successor_for(&fixture).await?;
    database
        .families()
        .supersede(selected.generation.id, &next)
        .await?;
    let claim = database
        .actions()
        .claim_report("reporter", Duration::from_secs(30))
        .await?
        .ok_or("claim")?;
    assert_eq!(claim.record_id, id);
    assert!(matches!(
        database
            .actions()
            .prepare_report(&claim, &enabled())
            .await?,
        ReportPreparation::Cancelled
    ));
    database.pool().close().await;
    let reopened = Database::open(fixture._database.path().join("nested/igor.db")).await?;
    let receipt = reopened
        .actions()
        .report_retry_request(request_id, selected.family_id, None)
        .await?
        .ok_or("retry receipt")?;
    assert_eq!(receipt.report.action_id, id);
    assert_eq!(receipt.report.state, ActionState::Cancelled);
    assert_eq!(receipt.report.generation, selected.generation);
    Ok(())
}

#[tokio::test]
async fn report_retry_requeues_the_original_action_and_publishes_without_job_reruns() -> TestResult
{
    for manual in [false, true] {
        let (fixture, selected, id) = fixture(manual).await?;
        let database = &fixture.database;
        database
            .actions()
            .retry_family_report(&selected, &enabled(), ActionId::new())
            .await?;
        let claim = database
            .actions()
            .claim_report("reporter", Duration::from_secs(30))
            .await?
            .ok_or("claim")?;
        let ReportPreparation::Ready(prepared) = database
            .actions()
            .prepare_report(&claim, &enabled())
            .await?
        else {
            return Err("not ready".into());
        };
        let locked = prepared.stage_and_lock()?;
        assert!(matches!(
            database
                .actions()
                .publish_report(&claim, locked, &enabled())
                .await?,
            ReportPublishOutcome::Published(_)
        ));
        assert_eq!(claim.record_id, id);
        assert_eq!(row(database, id).await?.0, "succeeded");
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT count(*) FROM attempts")
                .fetch_one(database.pool())
                .await?,
            2
        );
        assert!(
            fs::read_to_string(selected.project.root.join("RESULTS.md"))?
                .contains("Status: **Ready**")
        );
    }
    Ok(())
}

#[tokio::test]
async fn report_retry_missing_report_or_incomplete_family_has_no_side_effects() -> TestResult {
    let fixture = family_fixture().await?;
    let database = &fixture.database;
    database.families().submit(&fixture.prepared).await?;
    let selected = candidate(&fixture).await?;
    let before = row_counts(database, &["actions", "events", "attempts"]).await?;
    assert!(matches!(
        database
            .actions()
            .retry_family_report(&selected, &enabled(), ActionId::new())
            .await,
        Err(PersistenceError::GenerationNotComparable { .. })
    ));
    succeed(database, &fixture.prepared).await?;
    assert!(matches!(
        database
            .actions()
            .retry_family_report(&selected, &enabled(), ActionId::new())
            .await,
        Err(PersistenceError::NotFound { .. })
    ));
    assert_eq!(
        row_counts(database, &["actions", "events", "attempts"]).await?,
        before
    );
    Ok(())
}
