use super::*;
use igor_core::{
    REPORT_ACTION_VERSION, ReportActionSpec, ReportCandidate, ReportConfig, ReportGenerator,
};

pub(super) fn enabled() -> ReportConfig {
    ReportConfig {
        enabled: true,
        ..Default::default()
    }
}

pub(super) async fn candidate(fixture: &FamilyFixture) -> Result<ReportCandidate, Box<dyn Error>> {
    Ok(ReportCandidate {
        family_id: fixture.prepared.family.id,
        generation: fixture.prepared.generation.identity.clone(),
        project: fixture
            .database
            .projects()
            .get(fixture.prepared.family.project_id)
            .await?
            .ok_or("project")?,
    })
}

pub(super) async fn succeed(database: &Database, prepared: &PreparedFamily) -> TestResult {
    sqlx::query("UPDATE jobs SET state = 'succeeded' WHERE generation_id = ?")
        .bind(prepared.generation.identity.id.to_string())
        .execute(database.pool())
        .await?;
    sqlx::query("UPDATE attempts SET state = 'succeeded' WHERE job_id IN (SELECT id FROM jobs WHERE generation_id = ?)")
        .bind(prepared.generation.identity.id.to_string()).execute(database.pool()).await?;
    Ok(())
}

async fn report_count(database: &Database) -> Result<i64, sqlx::Error> {
    sqlx::query_scalar("SELECT count(*) FROM actions WHERE kind = 'generate_report'")
        .fetch_one(database.pool())
        .await
}

#[tokio::test]
async fn report_actions_opt_in_freezes_policy_and_deduplicates_across_reopen() -> TestResult {
    let fixture = family_fixture().await?;
    let database = &fixture.database;
    database.families().submit(&fixture.prepared).await?;
    succeed(database, &fixture.prepared).await?;
    let candidate = candidate(&fixture).await?;
    let before = row_counts(database, &["actions", "events"]).await?;
    assert_eq!(
        database
            .actions()
            .enqueue_family_report(&candidate, &ReportConfig::default())
            .await?,
        None
    );
    assert_eq!(row_counts(database, &["actions", "events"]).await?, before);
    let config = ReportConfig {
        generator: ReportGenerator::Pi,
        ..enabled()
    };
    let id = database
        .actions()
        .enqueue_family_report(&candidate, &config)
        .await?
        .ok_or("report intent")?;
    let changed = ReportConfig {
        output: PathBuf::from("other.md"),
        ..enabled()
    };
    assert_eq!(
        database
            .actions()
            .enqueue_family_report(&candidate, &changed)
            .await?,
        Some(id)
    );
    let (spec, key, job, attempt): (String, String, Option<String>, Option<String>) =
        sqlx::query_as(
            "SELECT spec_json, idempotency_key, job_id, attempt_id FROM actions WHERE id = ?",
        )
        .bind(id.to_string())
        .fetch_one(database.pool())
        .await?;
    let frozen: ReportActionSpec = serde_json::from_str(&spec)?;
    assert_eq!(frozen.schema_version, REPORT_ACTION_VERSION);
    assert_eq!(frozen.project, candidate.project);
    assert_eq!(frozen.generation, candidate.generation);
    assert_eq!(frozen.report.generator, ReportGenerator::Pi);
    assert_eq!(
        frozen.report.output,
        candidate.project.root.join("RESULTS.md")
    );
    assert_eq!(
        frozen.report.prompt_file,
        candidate.project.root.join(".igor/report-prompt.md")
    );
    assert!(key.contains(&candidate.family_id.to_string()));
    assert!(key.contains(&candidate.generation.id.to_string()));
    assert_eq!((job, attempt), (None, None));
    sqlx::query(
        "UPDATE actions SET spec_json = json_set(spec_json, '$.schema_version', 2) WHERE id = ?",
    )
    .bind(id.to_string())
    .execute(database.pool())
    .await?;
    assert!(matches!(
        database
            .actions()
            .enqueue_family_report(&candidate, &changed)
            .await,
        Err(PersistenceError::Conflict {
            entity: "family report intent"
        })
    ));
    assert_eq!(report_count(database).await?, 1);
    sqlx::query("UPDATE actions SET spec_json = ? WHERE id = ?")
        .bind(&spec)
        .bind(id.to_string())
        .execute(database.pool())
        .await?;
    sqlx::query("UPDATE actions SET state = 'failed' WHERE id = ?")
        .bind(id.to_string())
        .execute(database.pool())
        .await?;
    database.pool().close().await;
    let reopened = Database::open(fixture._database.path().join("nested/igor.db")).await?;
    assert_eq!(
        reopened
            .actions()
            .enqueue_family_report(&candidate, &changed)
            .await?,
        Some(id)
    );
    assert_eq!(report_count(&reopened).await?, 1);
    let persisted: String = sqlx::query_scalar("SELECT spec_json FROM actions WHERE id = ?")
        .bind(id.to_string())
        .fetch_one(reopened.pool())
        .await?;
    assert_eq!(persisted, spec);
    let events: i64 = sqlx::query_scalar("SELECT count(*) FROM events WHERE kind = 'action_state_changed' AND json_extract(payload_json, '$.data.record_id') = ?")
        .bind(id.to_string()).fetch_one(reopened.pool()).await?;
    assert_eq!(events, 1);
    assert!(reopened.actions().report_candidates(None).await?.is_empty());
    Ok(())
}

#[tokio::test]
async fn report_actions_require_current_comparable_registered_identity() -> TestResult {
    let fixture = family_fixture().await?;
    let database = &fixture.database;
    database.families().submit(&fixture.prepared).await?;
    let candidate = candidate(&fixture).await?;
    for state in ["queued", "failed", "cancelled", "lost"] {
        sqlx::query("UPDATE jobs SET state = ? WHERE generation_id = ?")
            .bind(state)
            .bind(candidate.generation.id.to_string())
            .execute(database.pool())
            .await?;
        assert!(database.actions().report_candidates(None).await?.is_empty());
        assert_eq!(
            database
                .actions()
                .enqueue_family_report(&candidate, &enabled())
                .await?,
            None
        );
    }
    succeed(database, &fixture.prepared).await?;
    let member = &fixture.prepared.submissions[1].job;
    let mut invalid = serde_json::to_value(member)?;
    invalid["family"]["seed"] = json!(1);
    sqlx::query("UPDATE jobs SET spec_json = ? WHERE id = ?")
        .bind(invalid.to_string())
        .bind(member.id.to_string())
        .execute(database.pool())
        .await?;
    assert_eq!(
        database
            .actions()
            .enqueue_family_report(&candidate, &enabled())
            .await?,
        None
    );
    sqlx::query("UPDATE jobs SET spec_json = ? WHERE id = ?")
        .bind(serde_json::to_string(member)?)
        .bind(member.id.to_string())
        .execute(database.pool())
        .await?;
    let mut stale = candidate.clone();
    stale.generation.source_revision = "wrong revision".into();
    assert_eq!(
        database
            .actions()
            .enqueue_family_report(&stale, &enabled())
            .await?,
        None
    );
    stale = candidate.clone();
    stale.project.config_path = stale.project.root.join("different.toml");
    assert_eq!(
        database
            .actions()
            .enqueue_family_report(&stale, &enabled())
            .await?,
        None
    );
    let mut moved = candidate.project.clone();
    moved.root = moved.root.join("moved-after-discovery");
    database.projects().update(&moved).await?;
    assert_eq!(
        database
            .actions()
            .enqueue_family_report(&candidate, &enabled())
            .await?,
        None
    );
    database.projects().update(&candidate.project).await?;
    database.projects().remove(&candidate.project.root).await?;
    assert!(database.actions().report_candidates(None).await?.is_empty());
    assert_eq!(
        database
            .actions()
            .enqueue_family_report(&candidate, &enabled())
            .await?,
        None
    );
    assert_eq!(report_count(database).await?, 0);
    Ok(())
}

#[tokio::test]
async fn report_actions_trigger_without_waiting_for_optional_extraction() -> TestResult {
    let mut fixture = family_fixture().await?;
    let extractor = fixture.prepared.submissions[0].job.command.clone();
    for submission in &mut fixture.prepared.submissions {
        let mut attempt = serde_json::to_value(&submission.attempt)?;
        attempt["result"]["extractor"] = serde_json::to_value(&extractor)?;
        submission.attempt = serde_json::from_value(attempt)?;
    }
    let database = &fixture.database;
    database.families().submit(&fixture.prepared).await?;
    succeed(database, &fixture.prepared).await?;
    let extraction_id = ActionId::new();
    let member = &fixture.prepared.submissions[0];
    sqlx::query("INSERT INTO actions (id, project_id, job_id, attempt_id, kind, state, spec_json, idempotency_key, available_at) VALUES (?, ?, ?, ?, 'extract_metrics', 'pending', '{}', ?, '2000-01-01')")
        .bind(extraction_id.to_string()).bind(member.job.project_id.to_string()).bind(member.job.id.to_string())
        .bind(member.attempt.id().to_string()).bind(extraction_id.to_string()).execute(database.pool()).await?;
    assert!(
        !database
            .families()
            .report_context(fixture.prepared.family.id)
            .await?
            .ready
    );
    let selected = database.actions().report_candidates(None).await?;
    assert_eq!(selected.len(), 1);
    assert!(
        database
            .actions()
            .enqueue_family_report(&selected[0], &enabled())
            .await?
            .is_some()
    );
    assert_eq!(
        database
            .actions()
            .claim_extraction("extractor", Duration::from_secs(30))
            .await?
            .ok_or("extraction claim")?
            .record_id,
        extraction_id
    );
    assert!(
        database
            .actions()
            .claim_notification("notification", Duration::from_secs(30))
            .await?
            .is_none()
    );
    assert_eq!(report_count(database).await?, 1);
    Ok(())
}

#[tokio::test]
async fn report_actions_supersession_fences_stale_selection_and_keys_each_generation() -> TestResult
{
    for queue_previous in [false, true] {
        let fixture = family_fixture().await?;
        let database = &fixture.database;
        database.families().submit(&fixture.prepared).await?;
        succeed(database, &fixture.prepared).await?;
        let previous = candidate(&fixture).await?;
        let old_action = if queue_previous {
            database
                .actions()
                .enqueue_family_report(&previous, &enabled())
                .await?
        } else {
            None
        };
        let successor = successor_for(&fixture).await?;
        database
            .families()
            .supersede(previous.generation.id, &successor)
            .await?;
        assert_eq!(
            database
                .actions()
                .enqueue_family_report(&previous, &enabled())
                .await?,
            None
        );
        assert!(database.actions().report_candidates(None).await?.is_empty());
        succeed(database, &successor).await?;
        let selected = database.actions().report_candidates(None).await?;
        assert_eq!(selected.len(), 1);
        assert_eq!(selected[0].generation, successor.generation.identity);
        let new_action = database
            .actions()
            .enqueue_family_report(&selected[0], &enabled())
            .await?
            .ok_or("new report")?;
        assert_ne!(Some(new_action), old_action);
        assert_eq!(
            report_count(database).await?,
            if queue_previous { 2 } else { 1 }
        );
    }
    Ok(())
}

#[tokio::test]
async fn report_actions_atomic_event_failure_preserves_experiment_and_replays_once() -> TestResult {
    let fixture = family_fixture().await?;
    let database = &fixture.database;
    database.families().submit(&fixture.prepared).await?;
    succeed(database, &fixture.prepared).await?;
    let candidate = candidate(&fixture).await?;
    let before = row_counts(database, &["jobs", "attempts", "events"]).await?;
    sqlx::raw_sql("CREATE TRIGGER fail_report_enqueue BEFORE INSERT ON events WHEN NEW.kind = 'action_state_changed' BEGIN SELECT RAISE(ABORT, 'report event fault'); END;")
        .execute(database.pool()).await?;
    assert!(
        database
            .actions()
            .enqueue_family_report(&candidate, &enabled())
            .await
            .is_err()
    );
    assert_eq!(report_count(database).await?, 0);
    assert_eq!(
        row_counts(database, &["jobs", "attempts", "events"]).await?,
        before
    );
    assert!(
        database
            .jobs()
            .list(Some(candidate.project.id))
            .await?
            .iter()
            .all(|job| job.state == JobState::Succeeded)
    );
    sqlx::raw_sql("DROP TRIGGER fail_report_enqueue")
        .execute(database.pool())
        .await?;
    assert!(
        database
            .actions()
            .enqueue_family_report(&candidate, &enabled())
            .await?
            .is_some()
    );
    assert_eq!(report_count(database).await?, 1);
    Ok(())
}

#[tokio::test]
async fn report_actions_independent_writers_enqueue_once_and_wait_before_reading() -> TestResult {
    let fixture = family_fixture().await?;
    let database = &fixture.database;
    database.families().submit(&fixture.prepared).await?;
    succeed(database, &fixture.prepared).await?;
    let candidate = candidate(&fixture).await?;
    let independent = Database::open(fixture._database.path().join("nested/igor.db")).await?;
    let gate = Barrier::new(2);
    let config = enabled();
    let (first, second) = tokio::join!(
        async {
            gate.wait().await;
            database
                .actions()
                .enqueue_family_report(&candidate, &config)
                .await
        },
        async {
            gate.wait().await;
            independent
                .actions()
                .enqueue_family_report(&candidate, &config)
                .await
        }
    );
    assert_eq!(first?, second?);
    assert_eq!(report_count(database).await?, 1);
    let mut writer = independent.pool().begin_with("BEGIN IMMEDIATE").await?;
    sqlx::query("UPDATE jobs SET state = 'failed' WHERE generation_id = ?")
        .bind(candidate.generation.id.to_string())
        .execute(&mut *writer)
        .await?;
    let db = database.clone();
    let selected = candidate.clone();
    let mut waiting = tokio::spawn(async move {
        db.actions()
            .enqueue_family_report(&selected, &enabled())
            .await
    });
    assert!(
        tokio::time::timeout(Duration::from_millis(50), &mut waiting)
            .await
            .is_err()
    );
    writer.commit().await?;
    assert_eq!(waiting.await??, None);
    assert_eq!(report_count(database).await?, 1);
    Ok(())
}

#[tokio::test]
async fn report_actions_discovery_is_readonly_keyset_bounded_and_does_not_starve_later_families()
-> TestResult {
    let (_repository, project, config, file) = family_repository()?;
    let (directory, database) = database().await?;
    database.projects().insert(&project).await?;
    let mut expected = BTreeSet::new();
    for index in 0..65 {
        let mut file = file.clone();
        file.name = format!("report family {index}");
        let prepared = build_family_generation(&project, &config, file)?;
        database.families().submit(&prepared).await?;
        succeed(&database, &prepared).await?;
        expected.insert(prepared.generation.identity.id);
    }
    let readonly = Database::open_with_options(
        directory.path().join("nested/igor.db"),
        DatabaseOptions {
            writable: false,
            create_if_missing: false,
            ..Default::default()
        },
    )
    .await?;
    let before = row_counts(&database, &["actions", "events"]).await?;
    let first = readonly.actions().report_candidates(None).await?;
    assert_eq!(first.len(), 64);
    for candidate in &first {
        assert_eq!(
            database
                .actions()
                .enqueue_family_report(candidate, &ReportConfig::default())
                .await?,
            None
        );
    }
    let second = readonly
        .actions()
        .report_candidates(first.last().map(|candidate| candidate.generation.id))
        .await?;
    assert_eq!(second.len(), 1);
    let collected: BTreeSet<_> = first
        .iter()
        .chain(&second)
        .map(|candidate| candidate.generation.id)
        .collect();
    assert!(
        first
            .iter()
            .chain(&second)
            .all(|candidate| candidate.generation.number == 1)
    );
    assert_eq!(collected, expected);
    assert!(
        readonly
            .actions()
            .report_candidates(second.last().map(|candidate| candidate.generation.id))
            .await?
            .is_empty()
    );
    assert_eq!(readonly.actions().report_candidates(None).await?, first);
    assert_eq!(row_counts(&database, &["actions", "events"]).await?, before);
    Ok(())
}
