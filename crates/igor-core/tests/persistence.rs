use std::{
    collections::BTreeSet,
    error::Error,
    io,
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, Instant},
};

use igor_core::{
    ActionId, ActionRecord, ActionState, ArtifactRecord, ArtifactRole, AttemptId, AttemptSpec,
    AttemptState, CommandSpec, ConfigurationIdentity, Database, DatabaseOptions, DeliveryId,
    DeliveryRecord, DeliveryState, EnvironmentPolicy, Event, EventId, EventKind, EventPayload,
    ExecutionOutcome, Family, FamilyId, Generation, GenerationId, GenerationIdentity, HostGpu,
    HostInventory, IntegrityCheck, JobId, JobSpec, JobState, PersistenceError, ProcessStart,
    Project, ProjectId, Resource, ResourceId, ResultContract, ShellPolicy, SourceIdentity,
};
use serde_json::json;
use sqlx::{Connection, Row, SqliteConnection, SqlitePool};
use tempfile::TempDir;
use tokio::sync::{Barrier, oneshot};

type TestResult = Result<(), Box<dyn Error>>;

fn missing(message: &'static str) -> io::Error {
    io::Error::other(message)
}

async fn database() -> Result<(TempDir, Database), Box<dyn Error>> {
    let directory = tempfile::tempdir()?;
    let database = Database::open(directory.path().join("nested/igor.db")).await?;
    Ok((directory, database))
}

fn project() -> Project {
    Project {
        id: ProjectId::new(),
        name: "research".into(),
        root: PathBuf::from("/tmp/research"),
        config_path: PathBuf::from("/tmp/research/.igor/project.toml"),
    }
}

fn job(project_id: ProjectId) -> JobSpec {
    JobSpec {
        id: JobId::new(),
        project_id,
        name: "experiment".into(),
        command: CommandSpec {
            program: "true".into(),
            args: Vec::new(),
            cwd: PathBuf::from("/tmp"),
            shell: ShellPolicy::Direct,
            environment: EnvironmentPolicy::default(),
        },
        executor: Default::default(),
        resources: Default::default(),
        retry: Default::default(),
        family: None,
    }
}

fn event(kind: EventKind, data: serde_json::Value) -> Result<Event, Box<dyn Error>> {
    Ok(Event::new(
        EventId::new(),
        kind,
        EventPayload::new(kind, 1, data)?,
    )?)
}

fn attempt(job: &JobSpec, sequence: u32) -> Result<AttemptSpec, Box<dyn Error>> {
    Ok(AttemptSpec::from_job(
        AttemptId::new(),
        sequence,
        job,
        SourceIdentity::SnapshotDigest("0123456789abcdef".into()),
        ConfigurationIdentity {
            project_digest: "sha256:project".into(),
            job_digest: "sha256:job".into(),
            contents: Vec::new(),
        },
        ResultContract {
            schema_version: 1,
            extractor: None,
        },
    )?)
}

async fn insert_project_job(database: &Database) -> Result<(Project, JobSpec), Box<dyn Error>> {
    let project = project();
    database.projects().insert(&project).await?;
    let job = job(project.id);
    database
        .jobs()
        .insert_job_with_event(
            &job,
            10,
            &event(EventKind::JobSubmitted, json!({"source": "test"}))?,
        )
        .await?;
    Ok((project, job))
}

#[tokio::test]
async fn empty_database_upgrades_to_checksummed_latest_schema() -> TestResult {
    let (_directory, database) = database().await?;
    let expected_tables: BTreeSet<String> = [
        "_sqlx_migrations",
        "actions",
        "agent_sessions",
        "artifacts",
        "attempts",
        "attempt_processes",
        "deliveries",
        "events",
        "families",
        "generations",
        "jobs",
        "metrics",
        "project_aliases",
        "projects",
        "recoveries",
        "report_runs",
        "resource_leases",
        "resources",
    ]
    .into_iter()
    .map(String::from)
    .collect();
    let tables: BTreeSet<String> = sqlx::query_scalar(
        "SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%'",
    )
    .fetch_all(database.pool())
    .await?
    .into_iter()
    .collect();
    assert_eq!(tables, expected_tables);
    let expected_indexes: BTreeSet<String> = [
        "actions_lease_idx",
        "actions_pending_idx",
        "agent_sessions_cleanup_idx",
        "artifacts_attempt_idx",
        "attempts_job_lookup_idx",
        "deliveries_lease_idx",
        "deliveries_pending_idx",
        "events_attempt_stream_idx",
        "events_job_stream_idx",
        "events_project_stream_idx",
        "jobs_generation_idx",
        "jobs_queue_order_idx",
        "metrics_attempt_idx",
        "recoveries_attempt_idx",
        "report_runs_generation_idx",
        "resource_leases_expiry_idx",
        "resource_leases_job_idx",
        "resource_leases_resource_idx",
    ]
    .into_iter()
    .map(String::from)
    .collect();
    let indexes: BTreeSet<String> = sqlx::query_scalar(
        "SELECT name FROM sqlite_master WHERE type = 'index' AND name NOT LIKE 'sqlite_%'",
    )
    .fetch_all(database.pool())
    .await?
    .into_iter()
    .collect();
    assert!(expected_indexes.is_subset(&indexes));

    let ledger = sqlx::query(
        "SELECT version, success, length(checksum) AS checksum_length FROM _sqlx_migrations ORDER BY version",
    )
    .fetch_all(database.pool())
    .await?;
    assert_eq!(ledger.len(), 6);
    for (index, row) in ledger.iter().enumerate() {
        assert_eq!(row.get::<i64, _>("version"), (index + 1) as i64);
        assert!(row.get::<bool, _>("success"));
        assert_eq!(row.get::<i64, _>("checksum_length"), 48);
    }
    database.integrity_check(IntegrityCheck::Quick).await?;
    database.integrity_check(IntegrityCheck::Full).await?;
    Ok(())
}

#[tokio::test]
async fn version_one_fixture_upgrades_and_preserves_ledger_checksum() -> TestResult {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("fixture.db");
    let url = format!("sqlite://{}?mode=rwc", path.display());
    let pool = SqlitePool::connect(&url).await?;
    let fixture = sqlx::migrate::Migrator::new(
        Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/migrations-v1"),
    )
    .await?;
    fixture.run(&pool).await?;
    sqlx::query("INSERT INTO projects (id, name, root_path, config_path) VALUES ('00000000-0000-0000-0000-000000000001', 'fixture', '/fixture', '/fixture/config')")
        .execute(&pool).await?;
    sqlx::query("INSERT INTO projects (id, name, root_path, config_path) VALUES ('00000000-0000-0000-0000-000000000006', 'legacy-duplicate', '/fixture', '/fixture/config')")
        .execute(&pool).await?;
    sqlx::query("INSERT INTO families (id, project_id, name) VALUES ('00000000-0000-0000-0000-000000000002', '00000000-0000-0000-0000-000000000001', 'fixture-family')")
        .execute(&pool).await?;
    sqlx::query("INSERT INTO generations (id, family_id, project_id, generation_number, source_revision, protocol_digest, spec_json) VALUES ('00000000-0000-0000-0000-000000000003', '00000000-0000-0000-0000-000000000002', '00000000-0000-0000-0000-000000000001', 1, 'revision', 'digest', '{}')")
        .execute(&pool).await?;
    sqlx::query("INSERT INTO jobs (id, project_id, family_id, generation_id, name, state, submission_order, spec_json) VALUES ('00000000-0000-0000-0000-000000000004', '00000000-0000-0000-0000-000000000001', '00000000-0000-0000-0000-000000000002', '00000000-0000-0000-0000-000000000003', 'fixture-job', 'queued', 1, '{}')")
        .execute(&pool).await?;
    sqlx::query(
        "UPDATE jobs SET submission_order = -2 WHERE id = '00000000-0000-0000-0000-000000000004'",
    )
    .execute(&pool)
    .await?;
    sqlx::query("INSERT INTO jobs (id, project_id, name, state, submission_order, spec_json) VALUES ('00000000-0000-0000-0000-000000000008', '00000000-0000-0000-0000-000000000001', 'later-fixture-job', 'queued', -1, '{}')")
        .execute(&pool).await?;
    sqlx::query("INSERT INTO attempts (id, job_id, project_id, sequence, state, spec_json) VALUES ('00000000-0000-0000-0000-000000000005', '00000000-0000-0000-0000-000000000004', '00000000-0000-0000-0000-000000000001', 1, 'pending', '{}')")
        .execute(&pool).await?;
    pool.close().await;

    let database = Database::open(&path).await?;
    let versions: Vec<i64> =
        sqlx::query_scalar("SELECT version FROM _sqlx_migrations WHERE success ORDER BY version")
            .fetch_all(database.pool())
            .await?;
    assert_eq!(versions, vec![1, 2, 3, 4, 5, 6]);
    let checksum: Vec<u8> =
        sqlx::query_scalar("SELECT checksum FROM _sqlx_migrations WHERE version = 1")
            .fetch_one(database.pool())
            .await?;
    assert_eq!(checksum.len(), 48);
    let names: Vec<String> = sqlx::query_scalar(
        "SELECT projects.name FROM projects
         JOIN families ON families.project_id = projects.id
         JOIN generations ON generations.family_id = families.id
         JOIN jobs ON jobs.generation_id = generations.id
         JOIN attempts ON attempts.job_id = jobs.id",
    )
    .fetch_all(database.pool())
    .await?;
    assert_eq!(names, vec!["fixture"]);
    let projects: i64 = sqlx::query_scalar("SELECT count(*) FROM projects")
        .fetch_one(database.pool())
        .await?;
    assert_eq!(projects, 2);
    let paths: Vec<(String, String)> =
        sqlx::query_as("SELECT root_path, config_path FROM projects ORDER BY created_at, id")
            .fetch_all(database.pool())
            .await?;
    assert_eq!(paths[0], ("/fixture".into(), "/fixture/config".into()));
    assert_eq!(paths[1], ("/fixture".into(), "/fixture/config".into()));
    let aliases: i64 = sqlx::query_scalar("SELECT count(*) FROM project_aliases")
        .fetch_one(database.pool())
        .await?;
    assert_eq!(aliases, 1);
    let duplicate_root = sqlx::query("INSERT INTO projects (id, name, root_path, config_path) VALUES ('00000000-0000-0000-0000-000000000007', 'duplicate-root', '/fixture', '/other/config')")
        .execute(database.pool())
        .await;
    assert!(duplicate_root.is_err());
    let job_ids: Vec<String> = sqlx::query_scalar("SELECT id FROM jobs ORDER BY submission_order")
        .fetch_all(database.pool())
        .await?;
    assert_eq!(
        job_ids,
        [
            "00000000-0000-0000-0000-000000000004",
            "00000000-0000-0000-0000-000000000008"
        ]
    );
    Ok(())
}

#[tokio::test]
async fn repositories_round_trip_and_events_are_append_only() -> TestResult {
    let (_directory, database) = database().await?;
    let (project, job) = insert_project_job(&database).await?;
    assert_eq!(database.projects().get(project.id).await?, Some(project));
    let family = Family {
        id: FamilyId::new(),
        project_id: job.project_id,
        name: "baseline-comparison".into(),
    };
    database.families().insert_family(&family).await?;
    assert_eq!(
        database.families().get_family(family.id).await?,
        Some(family.clone())
    );
    let generation = Generation {
        family_id: family.id,
        identity: GenerationIdentity {
            id: GenerationId::new(),
            number: 1,
            source_revision: "0123456789abcdef".into(),
            protocol_digest: "sha256:protocol".into(),
        },
        spec: json!({"schema_version": 1}),
    };
    database.families().insert_generation(&generation).await?;
    assert_eq!(
        database
            .families()
            .get_generation(generation.identity.id)
            .await?,
        Some(generation)
    );
    let stored = database
        .jobs()
        .get_job(job.id)
        .await?
        .ok_or_else(|| missing("job missing"))?;
    assert_eq!(stored.spec, job);
    assert_eq!(stored.state, JobState::Queued);

    let attempt = attempt(&job, 1)?;
    database
        .jobs()
        .insert_attempt_with_event(
            &attempt,
            &event(EventKind::AttemptCreated, json!({"sequence": 1}))?,
        )
        .await?;
    let stored_attempt = database
        .jobs()
        .get_attempt(attempt.id())
        .await?
        .ok_or_else(|| missing("attempt missing"))?;
    assert_eq!(stored_attempt.spec, attempt);
    assert_eq!(stored_attempt.state, AttemptState::Pending);
    let attempt_event = Event::new(
        EventId::new(),
        EventKind::AttemptStateChanged,
        EventPayload::new(
            EventKind::AttemptStateChanged,
            1,
            json!({"from": "pending", "to": "starting"}),
        )?,
    )?;
    database
        .jobs()
        .transition_attempt(attempt.id(), AttemptState::Starting, &attempt_event)
        .await?;
    assert_eq!(
        database
            .jobs()
            .get_attempt(attempt.id())
            .await?
            .ok_or_else(|| missing("transitioned attempt missing"))?
            .state,
        AttemptState::Starting
    );

    for (index, role) in [
        ArtifactRole::Metrics,
        ArtifactRole::Model,
        ArtifactRole::Checkpoint,
        ArtifactRole::Figure,
        ArtifactRole::Log,
        ArtifactRole::CrashDump,
        ArtifactRole::Report,
        ArtifactRole::Other("custom-scientific-output".into()),
    ]
    .into_iter()
    .enumerate()
    {
        let artifact = ArtifactRecord {
            id: uuid::Uuid::new_v4(),
            attempt_id: attempt.id(),
            path: format!("artifact-{index}"),
            role,
            sha256: Some(format!("digest-{index}")),
            metadata: json!({"index": index}),
        };
        database.artifacts().insert(&artifact).await?;
        assert_eq!(database.artifacts().get(artifact.id).await?, Some(artifact));
    }

    let event = Event::new(
        EventId::new(),
        EventKind::JobSubmitted,
        EventPayload::new(EventKind::JobSubmitted, 1, json!({"source": "test"}))?,
    )?;
    database
        .events()
        .append(job.project_id, Some(job.id), None, &event)
        .await?;
    assert_eq!(database.events().for_job(job.id).await?.len(), 4);
    let update = sqlx::query("UPDATE events SET occurred_at = occurred_at WHERE id = ?")
        .bind(event.id.to_string())
        .execute(database.pool())
        .await;
    assert!(update.is_err());
    assert!(
        sqlx::query("DELETE FROM jobs WHERE id = ?")
            .bind(job.id.to_string())
            .execute(database.pool())
            .await
            .is_err()
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_claims_and_resource_ownership_are_exclusive() -> TestResult {
    let (_directory, database) = database().await?;
    let (project, first_job) = insert_project_job(&database).await?;
    let action = ActionRecord {
        id: ActionId::new(),
        project_id: project.id,
        kind: "extract_metrics".into(),
        state: ActionState::Pending,
        spec: json!({"version": 1}),
        idempotency_key: "action-1".into(),
    };
    database.actions().insert(&action).await?;
    let delivery = DeliveryRecord {
        id: DeliveryId::new(),
        project_id: project.id,
        channel: "local".into(),
        state: DeliveryState::Pending,
        payload: json!({"message": "ready"}),
        idempotency_key: "delivery-1".into(),
    };
    database.deliveries().insert(&delivery, None).await?;
    let resource = Resource {
        id: ResourceId::new(),
        name: "gpu-0".into(),
        kind: "gpu".into(),
        capacity: 1,
        metadata: json!({}),
    };
    database.resources().insert(&resource).await?;

    let barrier = Arc::new(Barrier::new(3));
    let mut handles = Vec::new();
    for owner in ["worker-a", "worker-b"] {
        let database = database.clone();
        let barrier = Arc::clone(&barrier);
        handles.push(tokio::spawn(async move {
            barrier.wait().await;
            database
                .jobs()
                .claim_queued(owner, Duration::from_secs(30))
                .await
        }));
    }
    barrier.wait().await;
    let mut job_claims = 0;
    for handle in handles {
        if handle.await??.is_some() {
            job_claims += 1;
        }
    }
    assert_eq!(job_claims, 1);

    let actions = database.actions();
    let (action_a, action_b) = tokio::join!(
        actions.claim_pending("a", Duration::from_secs(30)),
        actions.claim_pending("b", Duration::from_secs(30)),
    );
    assert_eq!(
        usize::from(action_a?.is_some()) + usize::from(action_b?.is_some()),
        1
    );
    let action_payload: serde_json::Value = serde_json::from_str(
        &sqlx::query_scalar::<_, String>(
            "SELECT payload_json FROM events WHERE kind = 'action_state_changed'",
        )
        .fetch_one(database.pool())
        .await?,
    )?;
    assert_eq!(action_payload["data"]["entity"], "action");
    assert_eq!(action_payload["data"]["record_id"], action.id.to_string());
    let deliveries = database.deliveries();
    let (delivery_a, delivery_b) = tokio::join!(
        deliveries.claim_pending("a", Duration::from_secs(30)),
        deliveries.claim_pending("b", Duration::from_secs(30)),
    );
    assert_eq!(
        usize::from(delivery_a?.is_some()) + usize::from(delivery_b?.is_some()),
        1
    );
    let delivery_payload: serde_json::Value = serde_json::from_str(
        &sqlx::query_scalar::<_, String>(
            "SELECT payload_json FROM events WHERE kind = 'delivery_state_changed'",
        )
        .fetch_one(database.pool())
        .await?,
    )?;
    assert_eq!(delivery_payload["data"]["entity"], "delivery");
    assert_eq!(
        delivery_payload["data"]["record_id"],
        delivery.id.to_string()
    );
    let resource_job = job(project.id);
    database
        .jobs()
        .insert_job_with_event(
            &resource_job,
            10,
            &event(EventKind::JobSubmitted, json!({"source": "test"}))?,
        )
        .await?;
    let resources = database.resources();
    let (lease_a, lease_b) = tokio::join!(
        resources.acquire(resource.id, first_job.id, "a", 1, Duration::from_secs(30)),
        resources.acquire(
            resource.id,
            resource_job.id,
            "b",
            1,
            Duration::from_secs(30)
        ),
    );
    assert_eq!(
        usize::from(lease_a?.is_some()) + usize::from(lease_b?.is_some()),
        1
    );
    Ok(())
}

#[tokio::test]
async fn stale_job_claim_cannot_transition_or_clear_replacement_lease() -> TestResult {
    let (_directory, database) = database().await?;
    let (_project, job) = insert_project_job(&database).await?;
    let old = database
        .jobs()
        .claim_queued("old-worker", Duration::from_secs(30))
        .await?
        .ok_or_else(|| missing("old claim missing"))?;
    sqlx::query("UPDATE jobs SET claim_expires_at = '2000-01-01T00:00:00.000Z' WHERE id = ?")
        .bind(job.id.to_string())
        .execute(database.pool())
        .await?;
    let replacement = database
        .jobs()
        .claim_queued("new-worker", Duration::from_secs(30))
        .await?
        .ok_or_else(|| missing("replacement claim missing"))?;
    let transition = event(
        EventKind::JobStateChanged,
        json!({"from": "running", "to": "succeeded"}),
    )?;
    let stale = database
        .jobs()
        .transition_claimed_job(
            job.id,
            old.lease_id,
            &old.owner,
            JobState::Succeeded,
            &transition,
        )
        .await;
    assert!(matches!(
        stale,
        Err(PersistenceError::Conflict {
            entity: "job claim"
        })
    ));
    let (state, claim_id, owner): (String, String, String) =
        sqlx::query_as("SELECT state, claim_id, claim_owner FROM jobs WHERE id = ?")
            .bind(job.id.to_string())
            .fetch_one(database.pool())
            .await?;
    assert_eq!(state, "running");
    assert_eq!(claim_id, replacement.lease_id.to_string());
    assert_eq!(owner, replacement.owner);
    database
        .jobs()
        .transition_claimed_job(
            job.id,
            replacement.lease_id,
            &replacement.owner,
            JobState::Succeeded,
            &transition,
        )
        .await?;
    Ok(())
}

#[tokio::test]
async fn composite_foreign_keys_reject_cross_project_and_event_mismatches() -> TestResult {
    let (_directory, database) = database().await?;
    let (first_project, first_job) = insert_project_job(&database).await?;
    let first_family = Family {
        id: FamilyId::new(),
        project_id: first_project.id,
        name: "first-family".into(),
    };
    let other_first_family = Family {
        id: FamilyId::new(),
        project_id: first_project.id,
        name: "other-first-family".into(),
    };
    database.families().insert_family(&first_family).await?;
    database
        .families()
        .insert_family(&other_first_family)
        .await?;
    let other_first_generation = Generation {
        family_id: other_first_family.id,
        identity: GenerationIdentity {
            id: GenerationId::new(),
            number: 1,
            source_revision: "other-revision".into(),
            protocol_digest: "other-digest".into(),
        },
        spec: json!({}),
    };
    database
        .families()
        .insert_generation(&other_first_generation)
        .await?;
    let mut wrong_generation_job = job(first_project.id);
    wrong_generation_job.family = Some(igor_core::FamilyMembership {
        family_id: first_family.id,
        generation: other_first_generation.identity,
        seed: igor_core::Seed(1),
    });
    assert!(
        database
            .jobs()
            .insert_job_with_event(
                &wrong_generation_job,
                0,
                &event(EventKind::JobSubmitted, json!({}))?,
            )
            .await
            .is_err()
    );
    let mut second_project = project();
    second_project.root = PathBuf::from("/tmp/research-second");
    second_project.config_path = PathBuf::from("/tmp/research-second/.igor/project.toml");
    database.projects().insert(&second_project).await?;
    let second_family = Family {
        id: FamilyId::new(),
        project_id: second_project.id,
        name: "second-family".into(),
    };
    database.families().insert_family(&second_family).await?;
    let second_generation = Generation {
        family_id: second_family.id,
        identity: GenerationIdentity {
            id: GenerationId::new(),
            number: 1,
            source_revision: "revision".into(),
            protocol_digest: "digest".into(),
        },
        spec: json!({}),
    };
    database
        .families()
        .insert_generation(&second_generation)
        .await?;

    let mut mismatched_job = job(first_project.id);
    mismatched_job.family = Some(igor_core::FamilyMembership {
        family_id: second_family.id,
        generation: second_generation.identity,
        seed: igor_core::Seed(1),
    });
    assert!(
        database
            .jobs()
            .insert_job_with_event(
                &mismatched_job,
                0,
                &event(EventKind::JobSubmitted, json!({}))?,
            )
            .await
            .is_err()
    );

    let second_job = job(second_project.id);
    database
        .jobs()
        .insert_job_with_event(&second_job, 0, &event(EventKind::JobSubmitted, json!({}))?)
        .await?;
    let second_attempt = attempt(&second_job, 1)?;
    database
        .jobs()
        .insert_attempt_with_event(
            &second_attempt,
            &event(EventKind::AttemptCreated, json!({}))?,
        )
        .await?;
    let other_first_job = job(first_project.id);
    database
        .jobs()
        .insert_job_with_event(
            &other_first_job,
            0,
            &event(EventKind::JobSubmitted, json!({}))?,
        )
        .await?;
    let other_first_attempt = attempt(&other_first_job, 1)?;
    database
        .jobs()
        .insert_attempt_with_event(
            &other_first_attempt,
            &event(EventKind::AttemptCreated, json!({}))?,
        )
        .await?;
    let arbitrary = event(EventKind::AttemptStateChanged, json!({}))?;
    assert!(
        database
            .events()
            .append(
                first_project.id,
                Some(first_job.id),
                Some(second_attempt.id()),
                &arbitrary
            )
            .await
            .is_err()
    );
    assert!(
        database
            .events()
            .append(
                first_project.id,
                Some(first_job.id),
                Some(other_first_attempt.id()),
                &arbitrary,
            )
            .await
            .is_err()
    );
    assert!(
        database
            .events()
            .append(first_project.id, Some(second_job.id), None, &arbitrary)
            .await
            .is_err()
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn shared_resource_leases_respect_total_capacity() -> TestResult {
    let (_directory, database) = database().await?;
    let (project, first_job) = insert_project_job(&database).await?;
    let second_job = job(project.id);
    database
        .jobs()
        .insert_job_with_event(
            &second_job,
            10,
            &event(EventKind::JobSubmitted, json!({"source": "test"}))?,
        )
        .await?;
    let third_job = job(project.id);
    database
        .jobs()
        .insert_job_with_event(
            &third_job,
            10,
            &event(EventKind::JobSubmitted, json!({"source": "test"}))?,
        )
        .await?;
    let resource = Resource {
        id: ResourceId::new(),
        name: "shared-capacity".into(),
        kind: "named".into(),
        capacity: 2,
        metadata: json!({}),
    };
    database.resources().insert(&resource).await?;

    let resources = database.resources();
    let (first, second) = tokio::join!(
        resources.acquire(
            resource.id,
            first_job.id,
            "worker-a",
            1,
            Duration::from_secs(30)
        ),
        resources.acquire(
            resource.id,
            second_job.id,
            "worker-b",
            1,
            Duration::from_secs(30)
        ),
    );
    assert!(first?.is_some());
    assert!(second?.is_some());
    assert!(
        resources
            .acquire(
                resource.id,
                third_job.id,
                "worker-c",
                1,
                Duration::from_secs(30)
            )
            .await?
            .is_none()
    );
    Ok(())
}

#[tokio::test]
async fn failed_event_insert_rolls_back_lifecycle_transition() -> TestResult {
    let (_directory, database) = database().await?;
    let (_project, job) = insert_project_job(&database).await?;
    sqlx::query("CREATE TRIGGER reject_test_transition BEFORE INSERT ON events WHEN NEW.kind = 'job_state_changed' BEGIN SELECT RAISE(ABORT, 'injected event failure'); END")
        .execute(database.pool()).await?;
    let event = Event::new(
        EventId::new(),
        EventKind::JobStateChanged,
        EventPayload::new(
            EventKind::JobStateChanged,
            1,
            json!({"from": "queued", "to": "cancelled"}),
        )?,
    )?;
    assert!(
        database
            .jobs()
            .transition_job(job.id, JobState::Cancelled, &event)
            .await
            .is_err()
    );
    let stored = database
        .jobs()
        .get_job(job.id)
        .await?
        .ok_or_else(|| missing("job missing"))?;
    assert_eq!(stored.state, JobState::Queued);
    let events = database.events().for_job(job.id).await?;
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].event.kind, EventKind::JobSubmitted);
    Ok(())
}

#[tokio::test]
async fn failed_creation_event_rolls_back_job_and_attempt() -> TestResult {
    let (_directory, database) = database().await?;
    let project = project();
    database.projects().insert(&project).await?;
    let job = job(project.id);
    sqlx::query("CREATE TRIGGER reject_job_creation BEFORE INSERT ON events WHEN NEW.kind = 'job_submitted' BEGIN SELECT RAISE(ABORT, 'injected job creation event failure'); END")
        .execute(database.pool()).await?;
    assert!(
        database
            .jobs()
            .insert_job_with_event(&job, 10, &event(EventKind::JobSubmitted, json!({}))?)
            .await
            .is_err()
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM jobs")
            .fetch_one(database.pool())
            .await?,
        0
    );
    sqlx::query("DROP TRIGGER reject_job_creation")
        .execute(database.pool())
        .await?;
    database
        .jobs()
        .insert_job_with_event(&job, 10, &event(EventKind::JobSubmitted, json!({}))?)
        .await?;
    let attempt = attempt(&job, 1)?;
    sqlx::query("CREATE TRIGGER reject_attempt_creation BEFORE INSERT ON events WHEN NEW.kind = 'attempt_created' BEGIN SELECT RAISE(ABORT, 'injected attempt creation event failure'); END")
        .execute(database.pool()).await?;
    assert!(
        database
            .jobs()
            .insert_attempt_with_event(&attempt, &event(EventKind::AttemptCreated, json!({}))?,)
            .await
            .is_err()
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM attempts")
            .fetch_one(database.pool())
            .await?,
        0
    );
    Ok(())
}

#[tokio::test]
async fn failed_claim_events_roll_back_all_claim_families_and_attempt_transition() -> TestResult {
    let (_directory, database) = database().await?;
    let (project, job) = insert_project_job(&database).await?;
    let attempt = attempt(&job, 1)?;
    database
        .jobs()
        .insert_attempt_with_event(&attempt, &event(EventKind::AttemptCreated, json!({}))?)
        .await?;
    let action = ActionRecord {
        id: ActionId::new(),
        project_id: project.id,
        kind: "extract_metrics".into(),
        state: ActionState::Pending,
        spec: json!({}),
        idempotency_key: "rollback-action".into(),
    };
    database.actions().insert(&action).await?;
    let delivery = DeliveryRecord {
        id: DeliveryId::new(),
        project_id: project.id,
        channel: "local".into(),
        state: DeliveryState::Pending,
        payload: json!({}),
        idempotency_key: "rollback-delivery".into(),
    };
    database.deliveries().insert(&delivery, None).await?;
    let resource = Resource {
        id: ResourceId::new(),
        name: "rollback-resource".into(),
        kind: "named".into(),
        capacity: 1,
        metadata: json!({}),
    };
    database.resources().insert(&resource).await?;
    for (name, kind) in [
        ("reject_job_claim", "job_state_changed"),
        ("reject_attempt_transition", "attempt_state_changed"),
        ("reject_action_claim", "action_state_changed"),
        ("reject_delivery_claim", "delivery_state_changed"),
        ("reject_resource_claim", "resource_reserved"),
    ] {
        sqlx::query(&format!(
            "CREATE TRIGGER {name} BEFORE INSERT ON events WHEN NEW.kind = '{kind}' BEGIN SELECT RAISE(ABORT, 'injected event failure'); END"
        ))
        .execute(database.pool())
        .await?;
    }

    assert!(
        database
            .jobs()
            .claim_queued("worker", Duration::from_secs(30))
            .await
            .is_err()
    );
    assert!(
        database
            .jobs()
            .transition_attempt(
                attempt.id(),
                AttemptState::Starting,
                &event(EventKind::AttemptStateChanged, json!({}))?,
            )
            .await
            .is_err()
    );
    assert!(
        database
            .actions()
            .claim_pending("worker", Duration::from_secs(30))
            .await
            .is_err()
    );
    assert!(
        database
            .deliveries()
            .claim_pending("worker", Duration::from_secs(30))
            .await
            .is_err()
    );
    assert!(
        database
            .resources()
            .acquire(resource.id, job.id, "worker", 1, Duration::from_secs(30))
            .await
            .is_err()
    );

    let (job_state, job_claim): (String, Option<String>) =
        sqlx::query_as("SELECT state, claim_id FROM jobs WHERE id = ?")
            .bind(job.id.to_string())
            .fetch_one(database.pool())
            .await?;
    assert_eq!((job_state.as_str(), job_claim), ("queued", None));
    assert_eq!(
        database
            .jobs()
            .get_attempt(attempt.id())
            .await?
            .ok_or_else(|| missing("attempt missing"))?
            .state,
        AttemptState::Pending
    );
    for table in ["actions", "deliveries"] {
        let (state, claim, attempts): (String, Option<String>, i64) =
            sqlx::query_as(&format!("SELECT state, claim_id, attempts FROM {table}"))
                .fetch_one(database.pool())
                .await?;
        assert_eq!(state, "pending");
        assert_eq!(claim, None);
        assert_eq!(attempts, 0);
    }
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM resource_leases")
            .fetch_one(database.pool())
            .await?,
        0
    );
    Ok(())
}

#[tokio::test]
async fn active_outbox_states_require_leases_and_cannot_be_inserted() -> TestResult {
    let (_directory, database) = database().await?;
    let project = project();
    database.projects().insert(&project).await?;
    let action = ActionRecord {
        id: ActionId::new(),
        project_id: project.id,
        kind: "extract_metrics".into(),
        state: ActionState::Running,
        spec: json!({}),
        idempotency_key: "invalid-action".into(),
    };
    assert!(database.actions().insert(&action).await.is_err());
    let delivery = DeliveryRecord {
        id: DeliveryId::new(),
        project_id: project.id,
        channel: "local".into(),
        state: DeliveryState::Delivering,
        payload: json!({}),
        idempotency_key: "invalid-delivery".into(),
    };
    assert!(database.deliveries().insert(&delivery, None).await.is_err());
    assert!(
        sqlx::query("INSERT INTO actions (id, project_id, kind, state, spec_json, idempotency_key, available_at) VALUES (?, ?, 'extract_metrics', 'running', '{}', 'direct-invalid', '2000-01-01T00:00:00.000Z')")
            .bind(ActionId::new().to_string())
            .bind(project.id.to_string())
            .execute(database.pool())
            .await
            .is_err()
    );
    assert!(
        sqlx::query("INSERT INTO jobs (id, project_id, name, state, submission_order, spec_json) VALUES (?, ?, 'invalid-running', 'running', 1, '{}')")
            .bind(JobId::new().to_string())
            .bind(project.id.to_string())
            .execute(database.pool())
            .await
            .is_err()
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn foreign_keys_apply_to_every_connection() -> TestResult {
    let (_directory, database) = database().await?;
    let barrier = Arc::new(Barrier::new(5));
    let mut handles = Vec::new();
    for _ in 0..4 {
        let pool = database.pool().clone();
        let barrier = Arc::clone(&barrier);
        handles.push(tokio::spawn(async move {
            let mut connection = pool.acquire().await?;
            barrier.wait().await;
            let foreign_keys = sqlx::query_scalar::<_, i64>("PRAGMA foreign_keys")
                .fetch_one(&mut *connection)
                .await?;
            let journal_mode = sqlx::query_scalar::<_, String>("PRAGMA journal_mode")
                .fetch_one(&mut *connection)
                .await?;
            Ok::<_, sqlx::Error>((foreign_keys, journal_mode))
        }));
    }
    barrier.wait().await;
    for handle in handles {
        let (foreign_keys, journal_mode) = handle.await??;
        assert_eq!(foreign_keys, 1);
        assert_eq!(journal_mode, "wal");
    }
    Ok(())
}

#[tokio::test]
async fn busy_timeout_is_short_and_bounded() -> TestResult {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("busy.db");
    let database = Database::open_with_options(
        &path,
        DatabaseOptions {
            busy_timeout: Duration::from_millis(100),
            ..DatabaseOptions::default()
        },
    )
    .await?;
    let mut lock = database.pool().acquire().await?;
    sqlx::query("BEGIN IMMEDIATE").execute(&mut *lock).await?;
    let started = Instant::now();
    let result = sqlx::query("INSERT INTO projects (id, name, root_path, config_path) VALUES (?, 'locked', '/', '/config')")
        .bind(ProjectId::new().to_string()).execute(database.pool()).await;
    let elapsed = started.elapsed();
    sqlx::query("ROLLBACK").execute(&mut *lock).await?;
    assert!(result.is_err());
    assert!(elapsed >= Duration::from_millis(70));
    assert!(elapsed < Duration::from_secs(1));
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn online_backup_during_writes_restores_cleanly() -> TestResult {
    let directory = tempfile::tempdir()?;
    let source = Database::open(directory.path().join("source.db")).await?;
    let destination = directory.path().join("backup/restored.db");
    let (first_write_tx, first_write_rx) = oneshot::channel();
    let (stop_tx, mut stop_rx) = oneshot::channel();
    let writer = source.clone();
    let handle = tokio::spawn(async move {
        let mut first_write_tx = Some(first_write_tx);
        let mut sequence = 0_u64;
        loop {
            tokio::select! {
                _ = &mut stop_rx => break,
                result = async {
                    sequence += 1;
                    let project = Project {
                        id: ProjectId::new(), name: format!("project-{sequence}"),
                        root: PathBuf::from(format!("/tmp/project-{sequence}")),
                        config_path: PathBuf::from(format!("/tmp/project-{sequence}/config")),
                    };
                    writer.projects().insert(&project).await
                } => {
                    result?;
                    if let Some(sender) = first_write_tx.take() { let _ = sender.send(()); }
                    tokio::time::sleep(Duration::from_millis(1)).await;
                }
            }
        }
        Ok::<(), igor_core::PersistenceError>(())
    });
    first_write_rx.await?;
    source.backup(&destination).await?;
    let _ = stop_tx.send(());
    handle.await??;
    let restored = Database::open(&destination).await?;
    restored.integrity_check(IntegrityCheck::Full).await?;
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM projects")
        .fetch_one(restored.pool())
        .await?;
    assert!(count > 0);
    assert!(source.backup(&destination).await.is_err());
    let retry_destination = directory.path().join("backup/retry.db");
    std::fs::write(&retry_destination, b"occupied")?;
    assert!(source.backup(&retry_destination).await.is_err());
    std::fs::remove_file(&retry_destination)?;
    source.backup(&retry_destination).await?;
    let retry = Database::open(&retry_destination).await?;
    retry.integrity_check(IntegrityCheck::Full).await?;
    let backup_directory = destination
        .parent()
        .ok_or_else(|| missing("backup parent missing"))?;
    assert!(
        std::fs::read_dir(backup_directory)?
            .filter_map(Result::ok)
            .all(|entry| !entry.file_name().to_string_lossy().ends_with(".tmp"))
    );
    Ok(())
}

#[tokio::test]
async fn project_registration_is_idempotent_and_listed() -> TestResult {
    let (_directory, database) = database().await?;
    let first = project();
    let registered = database.projects().register(&first).await?;
    let mut duplicate = first.clone();
    duplicate.id = ProjectId::new();
    duplicate.name = "renamed request".into();
    let repeated = database.projects().register(&duplicate).await?;
    assert_eq!(registered, repeated);
    assert_eq!(database.projects().by_root(&first.root).await?, Some(first));
    assert_eq!(database.projects().list().await?.len(), 1);
    Ok(())
}

#[tokio::test]
async fn persisted_legacy_git_attempt_remains_readable() -> TestResult {
    let (_directory, database) = database().await?;
    let (project, job) = insert_project_job(&database).await?;
    let attempt = attempt(&job, 1)?;
    let mut document = serde_json::to_value(&attempt)?;
    document["source"] = json!({
        "kind": "git_revision",
        "identity": "0123456789abcdef"
    });
    sqlx::query("INSERT INTO attempts (id, job_id, project_id, sequence, state, spec_json) VALUES (?, ?, ?, 1, 'pending', ?)")
        .bind(attempt.id().to_string())
        .bind(job.id.to_string())
        .bind(project.id.to_string())
        .bind(serde_json::to_string(&document)?)
        .execute(database.pool())
        .await?;
    let attempts = database.jobs().attempts_for_job(job.id).await?;
    assert!(matches!(
        attempts[0].spec.source(),
        SourceIdentity::GitRevision(revision) if revision == "0123456789abcdef"
    ));
    Ok(())
}

#[tokio::test]
async fn submission_is_atomic_and_allocates_global_order() -> TestResult {
    let (_directory, database) = database().await?;
    let first_project = project();
    database.projects().insert(&first_project).await?;
    let mut second_project = project();
    second_project.root = PathBuf::from("/tmp/second-project");
    second_project.config_path = second_project.root.join(".igor/project.toml");
    database.projects().insert(&second_project).await?;

    let first_job = job(first_project.id);
    let first_attempt = attempt(&first_job, 1)?;
    let first = database
        .jobs()
        .submit(
            &first_job,
            &first_attempt,
            9,
            &event(EventKind::JobSubmitted, json!({}))?,
            &event(EventKind::AttemptCreated, json!({}))?,
        )
        .await?;
    let second_job = job(second_project.id);
    let second_attempt = attempt(&second_job, 1)?;
    let second = database
        .jobs()
        .submit(
            &second_job,
            &second_attempt,
            1,
            &event(EventKind::JobSubmitted, json!({}))?,
            &event(EventKind::AttemptCreated, json!({}))?,
        )
        .await?;
    assert_eq!((first.submission_order, second.submission_order), (1, 2));
    assert_eq!(database.jobs().list(None).await?.len(), 2);
    let detail = database
        .jobs()
        .detail(first_job.id)
        .await?
        .ok_or_else(|| missing("submitted job missing"))?;
    assert_eq!(detail.attempts.len(), 1);
    let events = database.events().for_job(first_job.id).await?;
    assert_eq!(events.len(), 2);
    assert_eq!(events[0].job_id, Some(first_job.id));
    assert_eq!(events[1].attempt_id, Some(first_attempt.id()));

    let failed_job = job(first_project.id);
    let failed_attempt = attempt(&failed_job, 1)?;
    let duplicate_event = event(EventKind::JobSubmitted, json!({}))?;
    let invalid_attempt_event = Event {
        id: duplicate_event.id,
        kind: EventKind::AttemptCreated,
        payload: EventPayload::new(EventKind::AttemptCreated, 1, json!({}))?,
    };
    assert!(
        database
            .jobs()
            .submit(
                &failed_job,
                &failed_attempt,
                0,
                &duplicate_event,
                &invalid_attempt_event,
            )
            .await
            .is_err()
    );
    assert!(database.jobs().get_job(failed_job.id).await?.is_none());
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_submissions_receive_distinct_global_order() -> TestResult {
    let (_directory, database) = database().await?;
    let project = project();
    database.projects().insert(&project).await?;
    let barrier = Arc::new(Barrier::new(3));
    let mut handles = Vec::new();
    for _ in 0..2 {
        let database = database.clone();
        let barrier = barrier.clone();
        let job = job(project.id);
        let attempt = attempt(&job, 1)?;
        let job_event = event(EventKind::JobSubmitted, json!({}))?;
        let attempt_event = event(EventKind::AttemptCreated, json!({}))?;
        handles.push(tokio::spawn(async move {
            barrier.wait().await;
            database
                .jobs()
                .submit(&job, &attempt, 0, &job_event, &attempt_event)
                .await
        }));
    }
    barrier.wait().await;
    let mut orders = BTreeSet::new();
    for handle in handles {
        orders.insert(handle.await??.submission_order);
    }
    assert_eq!(orders, BTreeSet::from([1, 2]));
    Ok(())
}

#[tokio::test]
async fn execution_claims_priority_fifo_and_persists_process_outcomes() -> TestResult {
    let (_directory, database) = database().await?;
    let project = project();
    database.projects().insert(&project).await?;
    let mut jobs = Vec::new();
    for (name, priority) in [
        ("first", 0),
        ("second", 0),
        ("urgent", 10),
        ("cancel-me", -1),
    ] {
        let mut job = job(project.id);
        job.name = name.into();
        let attempt = attempt(&job, 1)?;
        database
            .jobs()
            .submit(
                &job,
                &attempt,
                priority,
                &event(EventKind::JobSubmitted, json!({}))?,
                &event(EventKind::AttemptCreated, json!({}))?,
            )
            .await?;
        jobs.push((job, attempt));
    }

    let urgent = database
        .jobs()
        .claim_execution("worker-a", Duration::from_secs(30))
        .await?
        .ok_or_else(|| missing("urgent execution was not claimed"))?;
    assert_eq!(urgent.job.spec.name, "urgent");
    assert_eq!(urgent.job.state, JobState::Running);
    assert_eq!(urgent.attempt.state, AttemptState::Starting);
    let cancel_job = jobs
        .iter()
        .find(|(job, _)| job.name == "cancel-me")
        .ok_or_else(|| missing("queued cancellation fixture"))?;
    let cancelled = database
        .jobs()
        .request_cancellation(cancel_job.0.id, Duration::from_secs(1))
        .await?;
    assert_eq!(cancelled.job.state, JobState::Cancelled);
    assert_eq!(cancelled.attempts[0].state, AttemptState::Cancelled);
    assert!(
        database
            .jobs()
            .claim_execution("worker-b", Duration::from_secs(30))
            .await?
            .is_none()
    );
    assert!(
        database
            .jobs()
            .heartbeat_execution(&urgent, Duration::from_secs(30))
            .await
            .is_err()
    );
    let process = ProcessStart {
        pid: 1234,
        process_group_id: 1234,
        process_start_ticks: 9876,
        stdout_path: PathBuf::from("/tmp/igor/stdout.log"),
        stderr_path: PathBuf::from("/tmp/igor/stderr.log"),
    };
    database
        .jobs()
        .record_process_started(&urgent, &process)
        .await?;
    database
        .jobs()
        .heartbeat_execution(&urgent, Duration::from_secs(30))
        .await?;
    let recorded = database
        .jobs()
        .process_for_attempt(urgent.attempt.spec.id())
        .await?
        .ok_or_else(|| missing("process metadata was not persisted"))?;
    assert_eq!(recorded.pid, process.pid);
    assert_eq!(recorded.process_group_id, process.process_group_id);
    assert_eq!(recorded.process_start_ticks, process.process_start_ticks);
    database
        .jobs()
        .finish_execution(
            &urgent,
            &ExecutionOutcome {
                state: AttemptState::Succeeded,
                exit_code: Some(0),
                term_signal: None,
                error: None,
            },
        )
        .await?;
    assert_eq!(
        database
            .jobs()
            .get_job(urgent.job.spec.id)
            .await?
            .ok_or_else(|| missing("finished job missing"))?
            .state,
        JobState::Succeeded
    );
    assert_eq!(
        database
            .jobs()
            .get_attempt(urgent.attempt.spec.id())
            .await?
            .ok_or_else(|| missing("finished attempt missing"))?
            .state,
        AttemptState::Succeeded
    );
    assert_eq!(
        database
            .jobs()
            .process_for_attempt(urgent.attempt.spec.id())
            .await?
            .ok_or_else(|| missing("finished process metadata missing"))?
            .exit_code,
        Some(0)
    );

    let first = database
        .jobs()
        .claim_execution("worker-a", Duration::from_secs(30))
        .await?
        .ok_or_else(|| missing("first FIFO execution was not claimed"))?;
    assert_eq!(first.job.spec.name, "first");
    database
        .jobs()
        .finish_execution(
            &first,
            &ExecutionOutcome {
                state: AttemptState::Failed,
                exit_code: Some(7),
                term_signal: None,
                error: None,
            },
        )
        .await?;
    assert_eq!(
        database
            .jobs()
            .get_job(first.job.spec.id)
            .await?
            .ok_or_else(|| missing("failed job missing"))?
            .state,
        JobState::Failed
    );
    let next = database
        .jobs()
        .claim_execution("worker-a", Duration::from_secs(30))
        .await?
        .ok_or_else(|| missing("second FIFO execution was not claimed"))?;
    assert_eq!(next.job.spec.name, "second");
    database
        .jobs()
        .record_process_started(
            &next,
            &ProcessStart {
                pid: 4321,
                process_group_id: 4321,
                process_start_ticks: 6789,
                stdout_path: PathBuf::from("/tmp/igor/retry-stdout.log"),
                stderr_path: PathBuf::from("/tmp/igor/retry-stderr.log"),
            },
        )
        .await?;
    let recovered = database
        .jobs()
        .claim_recovery("worker-b", Duration::from_secs(30))
        .await?;
    assert!(recovered.is_none());
    database.jobs().release_execution(&next).await?;
    let recovered = database
        .jobs()
        .claim_recovery("worker-b", Duration::from_secs(30))
        .await?
        .ok_or_else(|| missing("running execution was not recovered"))?;
    assert_eq!(recovered.claim.attempt.spec.id(), next.attempt.spec.id());
    assert_eq!(
        recovered.process.as_ref().map(|process| process.pid),
        Some(4321)
    );
    assert!(
        database
            .jobs()
            .heartbeat_execution(&next, Duration::from_secs(30))
            .await
            .is_err()
    );
    let cancellation = database
        .jobs()
        .request_cancellation(next.job.spec.id, Duration::from_secs(3))
        .await?;
    assert_eq!(cancellation.job.state, JobState::Running);
    let grace = database
        .jobs()
        .cancellation_grace(&recovered.claim)
        .await?
        .ok_or_else(|| missing("cancellation grace period"))?;
    assert!(grace <= Duration::from_secs(3));
    assert!(grace > Duration::from_secs(2));
    database
        .jobs()
        .finish_execution(
            &recovered.claim,
            &ExecutionOutcome {
                state: AttemptState::Cancelled,
                exit_code: None,
                term_signal: Some(15),
                error: Some("execution cancelled".into()),
            },
        )
        .await?;
    let retried = database.jobs().retry(next.job.spec.id).await?;
    assert_eq!(retried.job.state, JobState::Queued);
    assert_eq!(retried.attempts.len(), 2);
    assert_eq!(retried.attempts[0].state, AttemptState::Cancelled);
    assert_eq!(retried.attempts[1].state, AttemptState::Pending);
    assert_eq!(retried.attempts[1].spec.sequence(), 2);
    assert_eq!(
        retried.attempts[0].spec.source(),
        retried.attempts[1].spec.source()
    );
    let retry_claim = database
        .jobs()
        .claim_execution("worker-a", Duration::from_secs(30))
        .await?
        .ok_or_else(|| missing("retry execution was not claimed"))?;
    assert_eq!(retry_claim.attempt.spec.sequence(), 2);
    assert_eq!(retry_claim.attempt.spec.id(), retried.attempts[1].spec.id());
    let interrupted_start = database
        .jobs()
        .claim_recovery("worker-b", Duration::from_secs(30))
        .await?;
    assert!(interrupted_start.is_none());
    database.jobs().release_execution(&retry_claim).await?;
    let interrupted_start = database
        .jobs()
        .claim_recovery("worker-b", Duration::from_secs(30))
        .await?
        .ok_or_else(|| missing("starting execution was not recovered"))?;
    assert!(interrupted_start.process.is_none());
    database
        .jobs()
        .finish_execution(
            &interrupted_start.claim,
            &ExecutionOutcome {
                state: AttemptState::Lost,
                exit_code: None,
                term_signal: None,
                error: Some("process identity missing".into()),
            },
        )
        .await?;
    assert_eq!(
        database
            .jobs()
            .get_job(next.job.spec.id)
            .await?
            .ok_or_else(|| missing("lost retry job"))?
            .state,
        JobState::Lost
    );
    Ok(())
}

#[tokio::test]
async fn host_inventory_is_idempotent_and_removes_stale_unleased_resources() -> TestResult {
    let (_directory, database) = database().await?;
    let first = HostInventory {
        detected_cpu_threads: 16,
        cpu_threads: 8,
        detected_memory_bytes: 32_000,
        memory_bytes: 24_000,
        max_concurrent_jobs: 2,
        gpus: vec![HostGpu {
            identity: "GPU-one".into(),
            display_name: Some("Test GPU".into()),
        }],
        gpu_inventory_authoritative: true,
        named_resources: vec!["scratch".into()],
    };
    database.resources().synchronize_inventory(&first).await?;
    let first_status = database.resources().status().await?;
    assert_eq!(first_status.len(), 5);
    let host_id = first_status
        .iter()
        .find(|status| status.resource.name == "host")
        .map(|status| status.resource.id)
        .ok_or_else(|| missing("host resource was not registered"))?;
    let memory = first_status
        .iter()
        .find(|status| status.resource.name == "memory")
        .ok_or_else(|| missing("memory resource was not registered"))?;
    assert_eq!(memory.resource.capacity, 24_000);
    assert_eq!(memory.resource.metadata["detected_bytes"], 32_000);
    let gpu_id = first_status
        .iter()
        .find(|status| status.resource.name == "gpu:GPU-one")
        .map(|status| status.resource.id)
        .ok_or_else(|| missing("GPU resource was not registered"))?;
    let unavailable = HostInventory {
        gpus: Vec::new(),
        gpu_inventory_authoritative: false,
        ..first.clone()
    };
    database
        .resources()
        .synchronize_inventory(&unavailable)
        .await?;
    let unavailable_status = database.resources().status().await?;
    let unavailable_gpu = unavailable_status
        .iter()
        .find(|status| status.resource.id == gpu_id)
        .ok_or_else(|| missing("GPU identity was removed after failed discovery"))?;
    assert_eq!(unavailable_gpu.resource.metadata["available"], false);
    database.resources().synchronize_inventory(&first).await?;
    let (_project, job) = insert_project_job(&database).await?;
    let lease = database
        .resources()
        .acquire(gpu_id, job.id, "worker", 1, Duration::from_secs(30))
        .await?
        .ok_or_else(|| missing("GPU resource was not leased"))?;

    let second = HostInventory {
        gpus: Vec::new(),
        named_resources: Vec::new(),
        memory_bytes: 20_000,
        ..first
    };
    database.resources().synchronize_inventory(&second).await?;
    let second_status = database.resources().status().await?;
    assert_eq!(second_status.len(), 4);
    assert_eq!(
        second_status
            .iter()
            .find(|status| status.resource.name == "host")
            .map(|status| status.resource.id),
        Some(host_id)
    );
    assert_eq!(
        second_status
            .iter()
            .find(|status| status.resource.name == "memory")
            .map(|status| status.resource.capacity),
        Some(20_000)
    );
    assert!(
        second_status
            .iter()
            .any(|status| status.resource.id == gpu_id
                && status.resource.metadata["available"] == false
                && status.leases == [lease.clone()])
    );
    assert!(database.resources().release(lease.id, "worker").await?);
    database.resources().synchronize_inventory(&second).await?;
    assert_eq!(database.resources().status().await?.len(), 3);
    Ok(())
}

#[tokio::test]
async fn attempt_specifications_are_immutable_in_sqlite() -> TestResult {
    let (_directory, database) = database().await?;
    let (project, job) = insert_project_job(&database).await?;
    let attempt = attempt(&job, 1)?;
    database
        .jobs()
        .insert_attempt_with_event(&attempt, &event(EventKind::AttemptCreated, json!({}))?)
        .await?;
    let changed = sqlx::query("UPDATE attempts SET spec_json = '{}' WHERE id = ?")
        .bind(attempt.id().to_string())
        .execute(database.pool())
        .await;
    assert!(changed.is_err());
    assert_eq!(
        database
            .jobs()
            .get_attempt(attempt.id())
            .await?
            .ok_or_else(|| missing("attempt missing"))?
            .spec,
        attempt
    );
    assert_eq!(project.id, job.project_id);
    Ok(())
}

#[tokio::test]
async fn direct_sqlite_connection_observes_migrated_foreign_keys() -> TestResult {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("direct.db");
    let database = Database::open(&path).await?;
    database.pool().close().await;
    let mut connection = SqliteConnection::connect(&format!("sqlite://{}", path.display())).await?;
    sqlx::query("PRAGMA foreign_keys = ON")
        .execute(&mut connection)
        .await?;
    let result = sqlx::query("INSERT INTO jobs (id, project_id, name, state, priority, submission_order, spec_json) VALUES (?, ?, 'orphan', 'queued', 0, 1, '{}')")
        .bind(JobId::new().to_string()).bind(ProjectId::new().to_string()).execute(&mut connection).await;
    assert!(result.is_err());
    Ok(())
}
