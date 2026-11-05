use super::*;
use crate::report_publication::{PublicationFault, pause_publication};
use crate::{ReportCandidate, ReportId, ReportState, StoredReportRun};
use std::{
    error::Error,
    fs,
    os::unix::{fs::MetadataExt, process::ExitStatusExt},
    process::{Child, Command, Stdio},
};

type TestResult = Result<(), Box<dyn Error>>;

fn policy() -> ReportConfig {
    ReportConfig {
        enabled: true,
        ..Default::default()
    }
}

struct Fixture {
    directory: tempfile::TempDir,
    database: Database,
    candidate: ReportCandidate,
    action_id: ActionId,
    previous: Option<StoredReportRun>,
}

impl Fixture {
    fn path(&self) -> PathBuf {
        self.directory.path().join("igor.db")
    }
    fn output(&self) -> PathBuf {
        self.candidate.project.root.join("RESULTS.md")
    }

    async fn new(previous: bool) -> Result<Self, Box<dyn Error>> {
        let directory = tempfile::tempdir()?;
        let root = directory.path().join("project");
        crate::initialize_project(&root, false)?;
        fs::write(root.join(".gitignore"), "RESULTS.md\n.igor-report-*.tmp\n")?;
        fs::write(
            root.join("family.toml"),
            "schema_version = 1\nname = 'publication faults'\n[execution]\nprogram = '/usr/bin/true'\n[[members]]\nseed = 1\nargs = ['seed-one']\n[[members]]\nseed = 2\nargs = ['seed-two']\n",
        )?;
        for args in [
            vec!["init", "-q"],
            vec!["add", "."],
            vec!["commit", "-qm", "fixture"],
        ] {
            let status = Command::new("git")
                .arg("-C")
                .arg(&root)
                .args([
                    "-c",
                    "user.name=Igor Test",
                    "-c",
                    "user.email=igor@example.invalid",
                ])
                .args(args)
                .status()?;
            assert!(status.success());
        }
        let loaded = crate::load_project_config(&root.join(".igor/project.toml"))?;
        let project = Project {
            id: ProjectId::new(),
            name: "publication faults".into(),
            root,
            config_path: loaded.config_file,
        };
        let prepared = crate::build_family_generation(
            &project,
            &loaded.config,
            crate::load_family_file(&project.root.join("family.toml"))?,
        )?;
        let database = Database::open(directory.path().join("igor.db")).await?;
        database.projects().insert(&project).await?;
        database.families().submit(&prepared).await?;
        sqlx::query("UPDATE jobs SET state = 'succeeded'")
            .execute(database.pool())
            .await?;
        sqlx::query("UPDATE attempts SET state = 'succeeded'")
            .execute(database.pool())
            .await?;
        let candidate = ReportCandidate {
            family_id: prepared.family.id,
            generation: prepared.generation.identity,
            project,
        };
        let previous = if previous {
            database
                .actions()
                .enqueue_manual_report(&candidate, &policy(), ActionId::new())
                .await?;
            let claim = claim(&database).await?;
            let prepared = prepare(&database, &claim).await?;
            let id = prepared.run_id().ok_or("previous run id")?;
            assert!(matches!(
                database
                    .actions()
                    .publish_report(&claim, prepared.stage_and_lock()?, &policy())
                    .await?,
                ReportPublishOutcome::Published(_)
            ));
            Some(run(&database, id).await?)
        } else {
            None
        };
        let recovery = Uuid::new_v4().to_string();
        sqlx::query("INSERT INTO recoveries (id, attempt_id, state, diagnosis_json, idempotency_key) VALUES (?, ?, 'succeeded', '{\"reason\":\"fresh evidence\"}', ?)")
            .bind(&recovery).bind(prepared.submissions[0].attempt.id().to_string()).bind(&recovery).execute(database.pool()).await?;
        let action_id = ActionId::new();
        database
            .actions()
            .enqueue_manual_report(&candidate, &policy(), action_id)
            .await?;
        Ok(Self {
            directory,
            database,
            candidate,
            action_id,
            previous,
        })
    }

    async fn assert_previous_unchanged(&self, database: &Database) -> TestResult {
        if let Some(previous) = &self.previous {
            assert_eq!(run(database, previous.id).await?, *previous);
        }
        Ok(())
    }
}

async fn claim(database: &Database) -> Result<Claim<ActionId>, Box<dyn Error>> {
    Ok(database
        .actions()
        .claim_report("publication faults", Duration::from_secs(30))
        .await?
        .ok_or("claim")?)
}

async fn prepare(
    database: &Database,
    claim: &Claim<ActionId>,
) -> Result<PreparedReport, Box<dyn Error>> {
    match database.actions().prepare_report(claim, &policy()).await? {
        ReportPreparation::Ready(prepared) => Ok(*prepared),
        other => Err(format!("not ready: {other:?}").into()),
    }
}

async fn run(database: &Database, id: ReportId) -> Result<StoredReportRun, Box<dyn Error>> {
    Ok(database
        .actions()
        .report_run(id)
        .await?
        .ok_or("report run")?)
}

async fn assert_unacknowledged(
    database: &Database,
    action: ActionId,
    id: ReportId,
    phase: &str,
) -> TestResult {
    let state: String = sqlx::query_scalar("SELECT state FROM actions WHERE id = ?")
        .bind(action.to_string())
        .fetch_one(database.pool())
        .await?;
    assert_eq!(state, "running");
    let run = run(database, id).await?;
    assert_eq!(run.state, ReportState::Generating);
    assert_eq!(run.phase.as_deref(), Some(phase));
    assert!(run.published_at.is_none());
    assert_eq!(success_events(database, action).await?, 0);
    assert_eq!(published_events(database, id).await?, 0);
    Ok(())
}

async fn success_events(database: &Database, action: ActionId) -> Result<i64, sqlx::Error> {
    sqlx::query_scalar("SELECT count(*) FROM events WHERE kind = 'action_state_changed' AND json_extract(payload_json, '$.data.record_id') = ? AND json_extract(payload_json, '$.data.state') = 'succeeded'")
        .bind(action.to_string()).fetch_one(database.pool()).await
}

async fn published_events(database: &Database, id: ReportId) -> Result<i64, sqlx::Error> {
    sqlx::query_scalar("SELECT count(*) FROM events WHERE json_extract(payload_json, '$.data.record_id') = ? AND json_extract(payload_json, '$.data.state') = 'published'")
        .bind(id.to_string()).fetch_one(database.pool()).await
}

async fn assert_completed(fixture: &Fixture, database: &Database, id: ReportId) -> TestResult {
    let stored = run(database, id).await?;
    let bytes = fs::read(fixture.output())?;
    assert_eq!(stored.state, ReportState::Published);
    assert_eq!(stored.phase.as_deref(), Some("published"));
    assert_eq!(
        stored.output_hash,
        Some(format!("sha256:{:x}", Sha256::digest(&bytes)))
    );
    assert_eq!(stored.output_size_bytes, Some(bytes.len() as u64));
    assert_eq!(success_events(database, fixture.action_id).await?, 1);
    assert_eq!(published_events(database, id).await?, 1);
    assert_eq!(
        database
            .actions()
            .report_runs(fixture.action_id)
            .await?
            .iter()
            .filter(|run| run.state == ReportState::Published)
            .count(),
        1
    );
    if let Some(previous) = &fixture.previous {
        let mut expected = previous.clone();
        expected.state = ReportState::Superseded;
        assert_eq!(run(database, previous.id).await?, expected);
    }
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM attempts")
            .fetch_one(database.pool())
            .await?,
        2
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM jobs WHERE state != 'succeeded'")
            .fetch_one(database.pool())
            .await?,
        0
    );
    Ok(())
}

fn temporaries(root: &Path) -> Result<Vec<PathBuf>, std::io::Error> {
    let mut paths = Vec::new();
    for entry in fs::read_dir(root)? {
        let path = entry?.path();
        if path
            .file_name()
            .is_some_and(|name| name.as_encoded_bytes().starts_with(b".igor-report-"))
        {
            paths.push(path);
        }
    }
    paths.sort();
    Ok(paths)
}

#[tokio::test]
async fn rename_failure_rolls_back_ack_and_preserves_previous_report_through_terminal_retry()
-> TestResult {
    for previous in [false, true] {
        let fixture = Fixture::new(previous).await?;
        let database = &fixture.database;
        let before = previous.then(|| fs::read(fixture.output())).transpose()?;
        let inode = previous
            .then(|| fs::metadata(fixture.output()).map(|m| m.ino()))
            .transpose()?;
        let claim = claim(database).await?;
        let prepared = prepare(database, &claim).await?;
        let id = prepared.run_id().ok_or("run id")?;
        let mut locked = prepared.stage_and_lock()?;
        locked.file.inject_fault(PublicationFault::Rename);
        assert!(matches!(
            database
                .actions()
                .publish_report(&claim, locked, &policy())
                .await,
            Err(ReportExecutionError::Publication(
                ReportPublicationError::Io {
                    operation: "rename staged report",
                    ..
                }
            ))
        ));
        assert_unacknowledged(database, fixture.action_id, id, "prepared").await?;
        assert_eq!(
            previous.then(|| fs::read(fixture.output())).transpose()?,
            before
        );
        assert_eq!(
            previous
                .then(|| fs::metadata(fixture.output()).map(|m| m.ino()))
                .transpose()?,
            inode
        );
        assert_eq!(fixture.output().exists(), previous);
        assert!(temporaries(&fixture.candidate.project.root)?.is_empty());
        fixture.assert_previous_unchanged(database).await?;
        database
            .actions()
            .retry(
                &claim,
                &crate::RetryPolicy {
                    max_attempts: 1,
                    initial_delay_seconds: 0,
                    max_delay_seconds: 0,
                    multiplier: 1,
                },
                "rename_failed",
            )
            .await?;
        let failed = run(database, id).await?;
        assert_eq!(failed.state, ReportState::Failed);
        assert!(failed.published_at.is_none());
        fixture.assert_previous_unchanged(database).await?;
        database
            .actions()
            .retry_family_report(&fixture.candidate, &policy(), ActionId::new())
            .await?;
        let retry = self::claim(database).await?;
        let prepared = prepare(database, &retry).await?;
        let new_id = prepared.run_id().ok_or("retry run id")?;
        assert_ne!(id, new_id);
        assert!(matches!(
            database
                .actions()
                .publish_report(&retry, prepared.stage_and_lock()?, &policy())
                .await?,
            ReportPublishOutcome::Published(_)
        ));
        assert_eq!(run(database, id).await?, failed);
        assert_completed(&fixture, database, new_id).await?;
    }
    Ok(())
}

#[tokio::test]
async fn directory_sync_failure_is_uncertain_and_reopen_confirms_without_rewriting() -> TestResult {
    for previous in [false, true] {
        let fixture = Fixture::new(previous).await?;
        let database = &fixture.database;
        let claim = claim(database).await?;
        let prepared = prepare(database, &claim).await?;
        let id = prepared.run_id().ok_or("run id")?;
        let mut locked = prepared.stage_and_lock()?;
        let expected = locked.file.report().clone();
        locked.file.inject_fault(PublicationFault::DirectorySync);
        let Err(ReportExecutionError::PublishedUncertain { report, .. }) = database
            .actions()
            .publish_report(&claim, locked, &policy())
            .await
        else {
            return Err("lost post-rename uncertainty".into());
        };
        assert_eq!(report, expected);
        assert_unacknowledged(database, fixture.action_id, id, "prepared").await?;
        fixture.assert_previous_unchanged(database).await?;
        let bytes = fs::read(fixture.output())?;
        let inode = fs::metadata(fixture.output())?.ino();
        database
            .actions()
            .defer_report(&claim, "report_publication_uncertain")
            .await?;
        sqlx::query("UPDATE actions SET available_at = '2000-01-01' WHERE id = ?")
            .bind(fixture.action_id.to_string())
            .execute(database.pool())
            .await?;
        database.pool().close().await;
        let reopened = Database::open(fixture.path()).await?;
        let retry = self::claim(&reopened).await?;
        let prepared = prepare(&reopened, &retry).await?;
        assert_eq!(prepared.run_id(), Some(id));
        // A second sync failure during exact-file reconciliation must still not ACK.
        let mut locked = prepared.stage_and_lock()?;
        locked.file.inject_fault(PublicationFault::DirectorySync);
        assert!(matches!(
            reopened
                .actions()
                .publish_report(&retry, locked, &policy())
                .await,
            Err(ReportExecutionError::PublishedUncertain { .. })
        ));
        assert_unacknowledged(&reopened, fixture.action_id, id, "prepared").await?;
        fixture.assert_previous_unchanged(&reopened).await?;
        let prepared = prepare(&reopened, &retry).await?;
        assert!(matches!(
            reopened
                .actions()
                .publish_report(&retry, prepared.stage_and_lock()?, &policy())
                .await?,
            ReportPublishOutcome::Published(_)
        ));
        assert_eq!(fs::read(fixture.output())?, bytes);
        assert_eq!(fs::metadata(fixture.output())?.ino(), inode);
        assert_completed(&fixture, &reopened, id).await?;
        assert_eq!(
            reopened
                .actions()
                .report_runs(fixture.action_id)
                .await?
                .len(),
            1
        );
    }
    Ok(())
}

struct KillOnDrop(Child);
impl Drop for KillOnDrop {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

#[test]
#[ignore = "only launched by the isolated SIGKILL parent test"]
fn publication_crash_child() -> TestResult {
    let database = std::env::var_os("IGOR_TEST_REPORT_DB").ok_or("child database")?;
    let marker = PathBuf::from(std::env::var_os("IGOR_TEST_REPORT_MARKER").ok_or("child marker")?);
    let phase = std::env::var("IGOR_TEST_REPORT_PHASE")?;
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?
        .block_on(async {
            let database = Database::open(PathBuf::from(database)).await?;
            let claim = claim(&database).await?;
            let prepared = prepare(&database, &claim).await?;
            let mut locked = prepared.stage_and_lock()?;
            match phase.as_str() {
                "staged" => pause_publication(&marker)?,
                "before_rename" => locked
                    .file
                    .inject_fault(PublicationFault::BeforeRename(marker)),
                "after_rename" => locked
                    .file
                    .inject_fault(PublicationFault::AfterRename(marker)),
                _ => return Err("unknown child phase".into()),
            }
            database
                .actions()
                .publish_report(&claim, locked, &policy())
                .await?;
            Err("child unexpectedly finished publication".into())
        })
}

#[tokio::test]
async fn sigkill_at_publication_boundaries_recovers_once_and_preserves_unowned_temporaries()
-> TestResult {
    for phase in ["staged", "before_rename", "after_rename"] {
        let fixture = Fixture::new(true).await?;
        let before = fs::read(fixture.output())?;
        let old_inode = fs::metadata(fixture.output())?.ino();
        let marker = fixture.directory.path().join("paused");
        let mut child = KillOnDrop(
            Command::new(std::env::current_exe()?)
                .args([
                    "--ignored",
                    "--exact",
                    "persistence::report_execution::tests::publication_crash_child",
                    "--nocapture",
                ])
                .env("IGOR_TEST_REPORT_DB", fixture.path())
                .env("IGOR_TEST_REPORT_MARKER", &marker)
                .env("IGOR_TEST_REPORT_PHASE", phase)
                .stdin(Stdio::null())
                .stdout(Stdio::null())
                .stderr(Stdio::inherit())
                .spawn()?,
        );
        tokio::time::timeout(Duration::from_secs(15), async {
            while !marker.exists() {
                assert!(child.0.try_wait()?.is_none(), "child exited before {phase}");
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            Ok::<_, std::io::Error>(())
        })
        .await??;
        let runs = fixture
            .database
            .actions()
            .report_runs(fixture.action_id)
            .await?;
        let id = runs.first().ok_or("child run")?.id;
        assert_eq!(runs.len(), 1);
        assert_unacknowledged(
            &fixture.database,
            fixture.action_id,
            id,
            if phase == "staged" {
                "context"
            } else {
                "prepared"
            },
        )
        .await?;
        fixture.assert_previous_unchanged(&fixture.database).await?;
        let visible = fs::read(fixture.output())?;
        let inode = fs::metadata(fixture.output())?.ino();
        if phase == "after_rename" {
            assert_ne!(visible, before);
            assert_ne!(inode, old_inode);
            assert_eq!(
                runs[0].output_hash,
                Some(format!("sha256:{:x}", Sha256::digest(&visible)))
            );
            assert_eq!(runs[0].output_size_bytes, Some(visible.len() as u64));
        } else {
            assert_eq!(visible, before);
            assert_eq!(inode, old_inode);
        }
        let orphaned = temporaries(&fixture.candidate.project.root)?
            .into_iter()
            .map(|path| {
                let bytes = fs::read(&path)?;
                Ok((path, bytes))
            })
            .collect::<Result<Vec<_>, std::io::Error>>()?;
        assert_eq!(orphaned.len(), usize::from(phase != "after_rename"));
        child.0.kill()?;
        assert_eq!(child.0.wait()?.signal(), Some(libc::SIGKILL));
        // Expire only after death: a reader must not publish over a still-live lease.
        sqlx::query("UPDATE actions SET claim_expires_at = '2000-01-01' WHERE id = ?")
            .bind(fixture.action_id.to_string())
            .execute(fixture.database.pool())
            .await?;
        fixture.database.pool().close().await;
        let reopened = Database::open(fixture.path()).await?;
        let retry = claim(&reopened).await?;
        assert_ne!(Some(retry.lease_id), runs[0].lease_id);
        let prepared = prepare(&reopened, &retry).await?;
        assert_eq!(prepared.run_id(), Some(id));
        let resumed = run(&reopened, id).await?;
        assert_eq!(resumed.context_hash, runs[0].context_hash);
        assert_eq!(resumed.initial_lease_id, runs[0].initial_lease_id);
        assert!(matches!(
            reopened
                .actions()
                .publish_report(&retry, prepared.stage_and_lock()?, &policy())
                .await?,
            ReportPublishOutcome::Published(_)
        ));
        if phase == "after_rename" {
            assert_eq!(fs::metadata(fixture.output())?.ino(), inode);
            assert_eq!(fs::read(fixture.output())?, visible);
        }
        for (path, bytes) in orphaned {
            assert_eq!(fs::read(path)?, bytes);
        }
        assert_completed(&fixture, &reopened, id).await?;
        assert_eq!(
            reopened
                .actions()
                .report_runs(fixture.action_id)
                .await?
                .len(),
            1
        );
        assert!(
            reopened
                .actions()
                .claim_report("again", Duration::from_secs(30))
                .await?
                .is_none()
        );
    }
    Ok(())
}
