use super::*;

fn configure_extractor(prepared: &mut PreparedFamily) -> TestResult {
    let command = prepared
        .submissions
        .first()
        .ok_or("missing member")?
        .job
        .command
        .clone();
    for submission in &mut prepared.submissions {
        let mut value = serde_json::to_value(&submission.attempt)?;
        value["result"]["extractor"] = serde_json::to_value(&command)?;
        submission.attempt = serde_json::from_value(value)?;
    }
    Ok(())
}

async fn succeeded(database: &Database, prepared: &PreparedFamily) -> TestResult {
    sqlx::query("UPDATE jobs SET state = 'succeeded' WHERE generation_id = ?")
        .bind(prepared.generation.identity.id.to_string())
        .execute(database.pool())
        .await?;
    sqlx::query("UPDATE attempts SET state = 'succeeded' WHERE job_id IN (SELECT id FROM jobs WHERE generation_id = ?)")
        .bind(prepared.generation.identity.id.to_string()).execute(database.pool()).await?;
    Ok(())
}

async fn extraction(
    database: &Database,
    attempt_id: AttemptId,
    state: &str,
    value: Option<f64>,
) -> TestResult {
    let action = ActionId::new().to_string();
    sqlx::query("INSERT INTO actions (id, project_id, job_id, attempt_id, kind, state, spec_json, idempotency_key, available_at) SELECT ?, project_id, job_id, id, 'extract_metrics', ?, '{}', ?, '2000-01-01' FROM attempts WHERE id = ?")
        .bind(&action).bind(state).bind(&action).bind(attempt_id.to_string()).execute(database.pool()).await?;
    if let Some(value) = value {
        let document = json!({"schema_version":1,"status":"success","metrics":{"validation.score":value},
            "selection":{"split":"validation","metric":"score","value":value},
            "classes":{"class-a":{"f1":0.75}}});
        igor_core::validate_metric_document(&document)?;
        sqlx::query(
            "INSERT INTO metric_documents (attempt_id, version, document_json) VALUES (?, 1, ?)",
        )
        .bind(attempt_id.to_string())
        .bind(document.to_string())
        .execute(database.pool())
        .await?;
        sqlx::query("INSERT INTO metrics (attempt_id, name, value, document_json) VALUES (?, 'validation.score', ?, ?)")
            .bind(attempt_id.to_string()).bind(value).bind(document.to_string()).execute(database.pool()).await?;
        sqlx::query("INSERT INTO metric_selections (attempt_id, split, metric, value, selection_json) VALUES (?, 'validation', 'score', ?, ?)")
            .bind(attempt_id.to_string()).bind(value).bind(document["selection"].to_string()).execute(database.pool()).await?;
    }
    Ok(())
}

#[tokio::test]
async fn report_context_is_stable_read_only_and_supports_no_extractor() -> TestResult {
    let fixture = family_fixture().await?;
    let database = &fixture.database;
    database.families().submit(&fixture.prepared).await?;
    let family_id = fixture.prepared.family.id;
    let queued = database.families().report_context(family_id).await?;
    assert!(!queued.ready);
    assert!(queued.primary_metrics.is_none());
    succeeded(database, &fixture.prepared).await?;
    let attempt_id = fixture.prepared.submissions[0].attempt.id();
    let artifact = ArtifactRecord {
        id: uuid::Uuid::new_v4(),
        attempt_id,
        path: "weights.bin".into(),
        role: ArtifactRole::Model,
        sha256: Some("sha256:retained".into()),
        size_bytes: Some(42),
        state: igor_core::ArtifactPublicationState::Published,
        lifecycle: igor_core::ArtifactLifecycleState::Retained,
        diagnosis_required: false,
        metadata: json!({"published_path":"/fixture/weights.bin"}),
    };
    database.artifacts().insert(&artifact).await?;
    let recovery = uuid::Uuid::new_v4().to_string();
    sqlx::query("INSERT INTO recoveries (id, attempt_id, state, diagnosis_json, validation_json, idempotency_key) VALUES (?, ?, 'succeeded', '{\"reason\":\"fixture\"}', '{\"passed\":true}', ?)")
        .bind(&recovery).bind(attempt_id.to_string()).bind(&recovery).execute(database.pool()).await?;
    // Projection uses durable metadata even when a worktree is not present on disk.
    sqlx::query("INSERT INTO repair_worktrees (recovery_id, project_id, path, root_path, repository_path, common_dir, admin_dir, identity_json, delete_on_release) VALUES (?, ?, '/fixture/repair', '/fixture/worktrees', '/fixture/project', '/fixture/git', '/fixture/git/worktrees/repair', '{}', 1)")
        .bind(&recovery).bind(fixture.prepared.family.project_id.to_string()).execute(database.pool()).await?;
    let tables = [
        "jobs",
        "attempts",
        "actions",
        "metrics",
        "events",
        "artifacts",
        "recoveries",
        "report_runs",
    ];
    let before = row_counts(database, &tables).await?;
    let read_only = Database::open_with_options(
        fixture._database.path().join("nested/igor.db"),
        DatabaseOptions {
            writable: false,
            create_if_missing: false,
            ..Default::default()
        },
    )
    .await?;
    // A held independent writer must not be needed by the projection.
    let mut writer = database.pool().begin_with("BEGIN IMMEDIATE").await?;
    sqlx::query("UPDATE families SET name = 'uncommitted' WHERE id = ?")
        .bind(family_id.to_string())
        .execute(&mut *writer)
        .await?;
    let first = read_only.families().report_context(family_id).await?;
    let second = read_only.families().report_context(family_id).await?;
    assert_eq!(serde_json::to_vec(&first)?, serde_json::to_vec(&second)?);
    let markdown = igor_core::render_report_markdown(&first)?;
    assert_eq!(markdown, igor_core::render_report_markdown(&second)?);
    assert!(markdown.contains("Status: **Ready**"));
    assert_eq!(first.family, fixture.prepared.family);
    assert_eq!(first.schema_version, igor_core::REPORT_CONTEXT_VERSION);
    assert!(first.ready);
    assert_eq!(first.primary_metrics, Some(vec![]));
    let attempt = &first.members[0].attempts[0];
    assert_eq!(
        &attempt.source,
        fixture.prepared.submissions[0].attempt.source()
    );
    assert_eq!(
        &attempt.configuration,
        fixture.prepared.submissions[0].attempt.configuration()
    );
    assert_eq!(attempt.artifacts, vec![artifact]);
    assert!(attempt.extractor.is_none());
    assert!(attempt.metrics.document.is_none());
    assert_eq!(first.repairs[0].recovery_id.to_string(), recovery);
    assert_eq!(
        first.repairs[0].diagnosis,
        Some(json!({"reason":"fixture"}))
    );
    assert_eq!(first.repairs[0].validation, Some(json!({"passed":true})));
    assert_eq!(
        first.repairs[0]
            .worktree
            .as_ref()
            .ok_or("worktree metadata")?["state"],
        "registered"
    );
    assert_eq!(first.tools.igor_context_builder, env!("CARGO_PKG_VERSION"));
    assert_eq!(first.tools.extractor, None);
    assert_eq!(first.tools.provider, None);
    assert_eq!(first.tools.model, None);
    writer.rollback().await?;
    assert_eq!(row_counts(database, &tables).await?, before);
    Ok(())
}

#[tokio::test]
async fn report_context_never_falls_back_or_mixes_superseded_results() -> TestResult {
    let mut fixture = family_fixture().await?;
    configure_extractor(&mut fixture.prepared)?;
    let database = &fixture.database;
    database.families().submit(&fixture.prepared).await?;
    succeeded(database, &fixture.prepared).await?;
    for submission in &fixture.prepared.submissions {
        extraction(database, submission.attempt.id(), "succeeded", Some(9999.0)).await?;
    }
    let old = database
        .families()
        .report_context(fixture.prepared.family.id)
        .await?;
    assert!(old.ready);
    let recovery = uuid::Uuid::new_v4().to_string();
    sqlx::query("INSERT INTO recoveries (id, attempt_id, state, diagnosis_json, idempotency_key) VALUES (?, ?, 'succeeded', '{}', ?)")
        .bind(&recovery).bind(fixture.prepared.submissions[0].attempt.id().to_string()).bind(&recovery)
        .execute(database.pool()).await?;
    let mut successor = successor_for(&fixture).await?;
    configure_extractor(&mut successor)?;
    database
        .families()
        .supersede(fixture.prepared.generation.identity.id, &successor)
        .await?;
    for state in ["queued", "failed"] {
        sqlx::query("UPDATE jobs SET state = ? WHERE generation_id = ?")
            .bind(state)
            .bind(successor.generation.identity.id.to_string())
            .execute(database.pool())
            .await?;
        let current = database
            .families()
            .report_context(fixture.prepared.family.id)
            .await?;
        assert_eq!(current.generation.identity, successor.generation.identity);
        assert!(!current.ready);
        assert!(current.primary_metrics.is_none());
        assert_eq!(current.history.len(), 1);
        assert_eq!(current.history[0].counts["superseded"], 2);
    }
    succeeded(database, &successor).await?;
    for (index, submission) in successor.submissions.iter().enumerate() {
        extraction(
            database,
            submission.attempt.id(),
            "succeeded",
            Some(index as f64 + 0.25),
        )
        .await?;
    }
    let root = fixture._repository.path().join("project");
    let loaded = load_project_config(&root.join(".igor/project.toml"))?;
    let project = Project {
        id: fixture.prepared.family.project_id,
        name: "family-project".into(),
        root,
        config_path: loaded.config_file,
    };
    let mut other_file = fixture.prepared.file.clone();
    other_file.name = "unrelated-family".into();
    let mut other = build_family_generation(&project, &loaded.config, other_file)?;
    configure_extractor(&mut other)?;
    database.families().submit(&other).await?;
    succeeded(database, &other).await?;
    for submission in &other.submissions {
        extraction(
            database,
            submission.attempt.id(),
            "succeeded",
            Some(-9999.0),
        )
        .await?;
    }
    let current = database
        .families()
        .report_context(fixture.prepared.family.id)
        .await?;
    assert!(current.ready);
    assert_eq!(current.repairs.len(), 1);
    let markdown = igor_core::render_report_markdown(&current)?;
    let primary_section = markdown
        .split("## Primary metric series\n\n")
        .nth(1)
        .ok_or("primary section")?
        .split("## Current-generation attempts")
        .next()
        .ok_or("attempt section")?;
    assert!(primary_section.contains("0.25"));
    assert!(primary_section.contains("1.25"));
    assert!(!primary_section.contains("9999"));
    assert_eq!(
        current.repairs[0].generation_id,
        fixture.prepared.generation.identity.id
    );
    let primary = current.primary_metrics.ok_or("missing primary")?;
    assert_eq!(primary.len(), 1);
    assert_eq!(
        primary[0]
            .values
            .iter()
            .map(|value| value.value)
            .collect::<Vec<_>>(),
        [0.25, 1.25]
    );
    for (value, submission) in primary[0].values.iter().zip(&successor.submissions) {
        assert_eq!(value.attempt_id, submission.attempt.id());
    }
    assert_eq!(
        current.members[0].attempts[0]
            .metrics
            .document
            .as_ref()
            .ok_or("document")?["classes"]["class-a"]["f1"],
        0.75
    );
    Ok(())
}

#[tokio::test]
async fn report_context_waits_for_extraction_but_terminal_failure_is_not_job_failure() -> TestResult
{
    let mut fixture = family_fixture().await?;
    configure_extractor(&mut fixture.prepared)?;
    let database = &fixture.database;
    database.families().submit(&fixture.prepared).await?;
    succeeded(database, &fixture.prepared).await?;
    let family = fixture.prepared.family.id;
    assert!(!database.families().report_context(family).await?.ready);
    extraction(
        database,
        fixture.prepared.submissions[0].attempt.id(),
        "pending",
        None,
    )
    .await?;
    extraction(
        database,
        fixture.prepared.submissions[1].attempt.id(),
        "failed",
        None,
    )
    .await?;
    assert!(!database.families().report_context(family).await?.ready);
    sqlx::query("UPDATE actions SET state = 'succeeded' WHERE state = 'pending'")
        .execute(database.pool())
        .await?;
    // A successful extraction action without its document cannot finalize metrics.
    assert!(!database.families().report_context(family).await?.ready);
    sqlx::query("UPDATE actions SET state = 'cancelled' WHERE state = 'succeeded'")
        .execute(database.pool())
        .await?;
    let context = database.families().report_context(family).await?;
    assert!(context.ready);
    assert_eq!(context.primary_metrics, Some(vec![]));
    assert!(
        context
            .members
            .iter()
            .all(|member| member.state == JobState::Succeeded)
    );
    assert_eq!(
        context.members[1].attempts[0]
            .metrics
            .action_state
            .as_deref(),
        Some("failed")
    );
    assert!(context.members[1].attempts[0].metrics.document.is_none());
    Ok(())
}

#[tokio::test]
async fn report_context_latest_attempt_and_numeric_seeds_define_series_order() -> TestResult {
    let (_repository, project, config, mut file) = family_repository()?;
    file.members[0].seed.0 = 10;
    file.members[1].seed.0 = 2;
    let mut prepared = build_family_generation(&project, &config, file)?;
    configure_extractor(&mut prepared)?;
    let (_directory, database) = database().await?;
    database.projects().insert(&project).await?;
    database.families().submit(&prepared).await?;
    succeeded(&database, &prepared).await?;
    let previous = &prepared.submissions[0].attempt;
    extraction(&database, previous.id(), "succeeded", Some(9999.0)).await?;
    sqlx::query("UPDATE attempts SET state = 'failed' WHERE id = ?")
        .bind(previous.id().to_string())
        .execute(database.pool())
        .await?;
    let mut replacement = serde_json::to_value(previous)?;
    let latest = AttemptId::new();
    replacement["id"] = json!(latest);
    replacement["sequence"] = json!(2);
    sqlx::query("INSERT INTO attempts (id, job_id, project_id, sequence, state, spec_json) VALUES (?, ?, ?, 2, 'succeeded', ?)")
        .bind(latest.to_string()).bind(previous.job_id().to_string()).bind(project.id.to_string())
        .bind(replacement.to_string()).execute(database.pool()).await?;
    extraction(&database, latest, "succeeded", Some(10.0)).await?;
    extraction(
        &database,
        prepared.submissions[1].attempt.id(),
        "succeeded",
        Some(2.0),
    )
    .await?;
    let context = database
        .families()
        .report_context(prepared.family.id)
        .await?;
    assert_eq!(
        context
            .required_seeds
            .iter()
            .map(|seed| seed.0)
            .collect::<Vec<_>>(),
        [2, 10]
    );
    assert_eq!(
        context
            .members
            .iter()
            .map(|member| member.seed.0)
            .collect::<Vec<_>>(),
        [2, 10]
    );
    assert_eq!(context.members[1].attempts.len(), 2);
    let primary = context.primary_metrics.ok_or("missing primary")?;
    assert_eq!(
        primary[0]
            .values
            .iter()
            .map(|value| value.value)
            .collect::<Vec<_>>(),
        [2.0, 10.0]
    );
    assert_eq!(primary[0].values[1].attempt_id, latest);
    Ok(())
}

#[tokio::test]
async fn report_context_rejects_corrupt_members_and_attempts() -> TestResult {
    let fixture = family_fixture().await?;
    let database = &fixture.database;
    database.families().submit(&fixture.prepared).await?;
    succeeded(database, &fixture.prepared).await?;
    let job = &fixture.prepared.submissions[0].job;
    for (field, value) in [
        ("$.family.generation.number", json!(99)),
        ("$.command.program", json!("tampered")),
    ] {
        sqlx::query("UPDATE jobs SET spec_json = json_set(spec_json, ?, json(?)) WHERE id = ?")
            .bind(field)
            .bind(value.to_string())
            .bind(job.id.to_string())
            .execute(database.pool())
            .await?;
        assert!(
            database
                .families()
                .report_context(fixture.prepared.family.id)
                .await
                .is_err()
        );
        sqlx::query("UPDATE jobs SET spec_json = ? WHERE id = ?")
            .bind(serde_json::to_string(job)?)
            .bind(job.id.to_string())
            .execute(database.pool())
            .await?;
    }
    // Simulate damaged persisted evidence, explicitly removing its normal immutability guard.
    sqlx::raw_sql("DROP TRIGGER attempts_spec_immutable")
        .execute(database.pool())
        .await?;
    sqlx::query("UPDATE attempts SET spec_json = json_set(spec_json, '$.source.identity.revision', 'wrong') WHERE job_id = ?")
        .bind(job.id.to_string()).execute(database.pool()).await?;
    assert!(
        database
            .families()
            .report_context(fixture.prepared.family.id)
            .await
            .is_err()
    );
    Ok(())
}
