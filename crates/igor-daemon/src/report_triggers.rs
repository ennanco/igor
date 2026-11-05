use std::{
    collections::{BTreeMap, btree_map::Entry},
    time::Duration,
};

use igor_core::{Database, GenerationId, Project, ProjectId, ReportConfig, load_project_config};
use tokio::{sync::watch, time};

pub(crate) async fn run(database: Database, mut shutdown: watch::Receiver<bool>) {
    let mut cursor = None;
    loop {
        if *shutdown.borrow() {
            return;
        }
        match discover(&database, cursor, &shutdown).await {
            Ok(next) => cursor = next,
            Err(error) => tracing::warn!(%error, "cannot discover family report triggers"),
        }
        tokio::select! { _ = time::sleep(Duration::from_secs(1)) => {}, _ = shutdown.changed() => return }
    }
}

async fn discover(
    database: &Database,
    cursor: Option<GenerationId>,
    shutdown: &watch::Receiver<bool>,
) -> Result<Option<GenerationId>, igor_core::PersistenceError> {
    let candidates = database.actions().report_candidates(cursor).await?;
    let next = candidates.last().map(|candidate| candidate.generation.id);
    let mut policies = BTreeMap::<ProjectId, Option<ReportConfig>>::new();
    for candidate in candidates {
        if *shutdown.borrow() {
            break;
        }
        if let Entry::Vacant(entry) = policies.entry(candidate.project.id) {
            let project = candidate.project.clone();
            let loaded = tokio::task::spawn_blocking(move || load_policy(&project)).await;
            let policy = match loaded {
                Ok(Ok(policy)) => Some(policy),
                Ok(Err(error)) => {
                    tracing::warn!(%error, project_id = %candidate.project.id, "report trigger awaits readable project configuration");
                    None
                }
                Err(error) => {
                    tracing::warn!(%error, "report configuration loading task failed");
                    None
                }
            };
            entry.insert(policy);
        }
        if let Some(Some(policy)) = policies.get(&candidate.project.id)
            && let Err(error) = database
                .actions()
                .enqueue_family_report(&candidate, policy)
                .await
        {
            tracing::warn!(%error, family_id = %candidate.family_id, generation_id = %candidate.generation.id, "family report trigger deferred");
        }
    }
    Ok(next)
}

pub(crate) fn load_policy(project: &Project) -> Result<ReportConfig, String> {
    if project
        .root
        .canonicalize()
        .map_err(|error| error.to_string())?
        != project.root
        || project.config_path != project.root.join(igor_core::PROJECT_CONFIG_RELATIVE_PATH)
    {
        return Err("report configuration does not match the registered canonical project".into());
    }
    let directory =
        std::fs::symlink_metadata(project.root.join(".igor")).map_err(|error| error.to_string())?;
    let file =
        std::fs::symlink_metadata(&project.config_path).map_err(|error| error.to_string())?;
    if !directory.is_dir() || !file.is_file() {
        return Err("report configuration requires a real directory and regular file".into());
    }
    let loaded = load_project_config(&project.config_path).map_err(|error| error.to_string())?;
    if loaded.project_root != project.root {
        return Err("report configuration root differs from registered project".into());
    }
    Ok(loaded.config.report)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Client, DaemonError, DaemonRole, Request, Response, run_until};
    use igor_core::{
        FamilyFile, JobState, PreparedFamily, Project, ProjectId, ReportActionSpec, RuntimePaths,
        build_family_generation, initialize_project,
    };
    use std::{error::Error, fs, path::Path, process::Command};
    use tempfile::TempDir;
    use tokio::{sync::oneshot, task::JoinHandle};

    type TestResult = Result<(), Box<dyn Error>>;

    #[test]
    fn report_triggers_fifo_config_child() -> TestResult {
        let Some(root) = std::env::var_os("IGOR_REPORT_FIFO_CHILD_ROOT") else {
            return Ok(());
        };
        let root = std::path::PathBuf::from(root);
        let project = Project {
            id: ProjectId::new(),
            name: "FIFO fixture".into(),
            config_path: root.join(".igor/project.toml"),
            root,
        };
        assert!(load_policy(&project).is_err());
        Ok(())
    }

    #[tokio::test]
    async fn report_triggers_reject_fifo_configuration_in_a_killable_process() -> TestResult {
        let temporary = TempDir::new()?;
        let root = temporary.path().join("project");
        initialize_project(&root, false)?;
        let path = root.join(".igor/project.toml");
        fs::remove_file(&path)?;
        nix::unistd::mkfifo(&path, nix::sys::stat::Mode::from_bits_truncate(0o600))?;
        let mut child = tokio::process::Command::new(std::env::current_exe()?)
            .args([
                "--exact",
                "report_triggers::tests::report_triggers_fifo_config_child",
                "--test-threads=1",
            ])
            .env("IGOR_REPORT_FIFO_CHILD_ROOT", &root)
            .stdout(std::process::Stdio::null())
            .kill_on_drop(true)
            .spawn()?;
        let status = match time::timeout(Duration::from_secs(5), child.wait()).await {
            Ok(status) => status?,
            Err(error) => {
                child.kill().await?;
                return Err(error.into());
            }
        };
        assert!(status.success());
        Ok(())
    }

    fn git(root: &Path, args: &[&str]) -> TestResult {
        let status = Command::new("git")
            .arg("-C")
            .arg(root)
            .args([
                "-c",
                "user.name=Igor Test",
                "-c",
                "user.email=igor@example.invalid",
            ])
            .args(args)
            .status()?;
        if !status.success() {
            return Err(format!("fixture git exited with {status}").into());
        }
        Ok(())
    }

    fn repository(root: &Path, enabled: bool) -> Result<(Project, PreparedFamily), Box<dyn Error>> {
        initialize_project(root, false)?;
        let config_path = root.join(".igor/project.toml");
        if enabled {
            fs::write(
                &config_path,
                fs::read_to_string(&config_path)?.replace("enabled = false", "enabled = true"),
            )?;
        }
        let family_path = root.join("family.toml");
        fs::write(
            &family_path,
            r#"
schema_version = 1
name = "completed family"
[execution]
program = "/usr/bin/true"
[[members]]
seed = 1
args = ["seed-one"]
[[members]]
seed = 2
args = ["seed-two"]
"#,
        )?;
        git(root, &["init", "-q"])?;
        git(root, &["add", "."])?;
        git(root, &["commit", "-qm", "report trigger fixture"])?;
        let loaded = load_project_config(&config_path)?;
        let project = Project {
            id: ProjectId::new(),
            name: "report project".into(),
            root: root.to_path_buf(),
            config_path,
        };
        let file: FamilyFile = igor_core::load_family_file(&family_path)?;
        let prepared = build_family_generation(&project, &loaded.config, file)?;
        Ok((project, prepared))
    }

    async fn successful_fixture(
        database: &Database,
        project: &Project,
        prepared: &PreparedFamily,
    ) -> TestResult {
        database.projects().insert(project).await?;
        database.families().submit(prepared).await?;
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
    async fn report_triggers_retry_disabled_or_unreadable_policy_after_wraparound() -> TestResult {
        let temporary = TempDir::new()?;
        let (project, prepared) = repository(&temporary.path().join("project"), false)?;
        let database = Database::open(temporary.path().join("igor.db")).await?;
        successful_fixture(&database, &project, &prepared).await?;
        let (_sender, shutdown) = watch::channel(false);
        let first = discover(&database, None, &shutdown).await?;
        assert_eq!(first, Some(prepared.generation.identity.id));
        assert_eq!(report_count(&database).await?, 0);
        assert_eq!(discover(&database, first, &shutdown).await?, None);
        let config = fs::read_to_string(&project.config_path)?;
        fs::write(&project.config_path, "not valid project configuration")?;
        assert_eq!(discover(&database, None, &shutdown).await?, first);
        assert_eq!(report_count(&database).await?, 0);
        assert_eq!(discover(&database, first, &shutdown).await?, None);
        fs::write(
            &project.config_path,
            config.replace("enabled = false", "enabled = true"),
        )?;
        discover(&database, None, &shutdown).await?;
        assert_eq!(report_count(&database).await?, 1);
        discover(&database, None, &shutdown).await?;
        assert_eq!(report_count(&database).await?, 1);
        assert!(
            database
                .jobs()
                .list(Some(project.id))
                .await?
                .iter()
                .all(|job| job.state == JobState::Succeeded)
        );
        Ok(())
    }

    #[tokio::test]
    async fn report_triggers_preserve_registration_root_boundary() -> TestResult {
        let temporary = TempDir::new()?;
        let (project, prepared) = repository(&temporary.path().join("project"), true)?;
        let database = Database::open(temporary.path().join("igor.db")).await?;
        successful_fixture(&database, &project, &prepared).await?;
        let elsewhere = temporary.path().join("different-project");
        initialize_project(&elsewhere, false)?;
        let mut moved = project.clone();
        moved.config_path = elsewhere.join(".igor/project.toml");
        database.projects().update(&moved).await?;
        let (_sender, shutdown) = watch::channel(false);
        discover(&database, None, &shutdown).await?;
        assert_eq!(report_count(&database).await?, 0);
        database.projects().update(&project).await?;
        let config_dir = project.root.join(".igor");
        let backup_dir = project.root.join("config-backup");
        fs::rename(&config_dir, &backup_dir)?;
        std::os::unix::fs::symlink(&backup_dir, &config_dir)?;
        discover(&database, None, &shutdown).await?;
        assert_eq!(report_count(&database).await?, 0);
        fs::remove_file(&config_dir)?;
        fs::rename(&backup_dir, &config_dir)?;
        let backup_file = config_dir.join("saved.toml");
        fs::rename(&project.config_path, &backup_file)?;
        std::os::unix::fs::symlink(&backup_file, &project.config_path)?;
        discover(&database, None, &shutdown).await?;
        assert_eq!(report_count(&database).await?, 0);
        fs::remove_file(&project.config_path)?;
        fs::rename(&backup_file, &project.config_path)?;
        discover(&database, None, &shutdown).await?;
        assert_eq!(report_count(&database).await?, 1);
        Ok(())
    }

    fn paths(root: &Path) -> RuntimePaths {
        let state = root.join("state");
        let runtime = root.join("runtime");
        RuntimePaths {
            config_file: root.join("global.toml"),
            database: state.join("igor.db"),
            state_dir: state.clone(),
            runtime_dir: runtime.clone(),
            log_dir: state.join("logs"),
            worktree_dir: state.join("worktrees"),
            report_dir: state.join("reports"),
            worker_socket: runtime.join("worker.sock"),
            supervisor_socket: runtime.join("supervisor.sock"),
            resource_dir: runtime.join("resources"),
        }
    }

    fn spawn(
        role: DaemonRole,
        paths: RuntimePaths,
    ) -> (oneshot::Sender<()>, JoinHandle<Result<(), DaemonError>>) {
        let (stop, receiver) = oneshot::channel();
        let handle = tokio::spawn(async move {
            run_until(role, &paths, async {
                let _ = receiver.await;
            })
            .await
        });
        (stop, handle)
    }

    async fn healthy(client: &Client, role: DaemonRole) -> TestResult {
        time::timeout(Duration::from_secs(10), async {
            loop {
                if matches!(
                    client.request(role, Request::Health).await,
                    Ok(Response::Health(_))
                ) {
                    return;
                }
                time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await?;
        Ok(())
    }

    #[tokio::test]
    async fn report_triggers_recover_worker_only_completion_on_supervisor_start_and_restart()
    -> TestResult {
        let temporary = TempDir::new()?;
        let (project, prepared) = repository(&temporary.path().join("project"), true)?;
        let paths = paths(temporary.path());
        let database = Database::open(&paths.database).await?;
        database.projects().insert(&project).await?;
        database.families().submit(&prepared).await?;
        let (stop_worker, worker) = spawn(DaemonRole::Worker, paths.clone());
        let client = Client::new(&paths);
        healthy(&client, DaemonRole::Worker).await?;
        time::timeout(Duration::from_secs(15), async {
            loop {
                let jobs = database.jobs().list(Some(project.id)).await?;
                if jobs.iter().all(|job| job.state == JobState::Succeeded) {
                    return Ok::<_, igor_core::PersistenceError>(());
                }
                time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await??;
        assert_eq!(report_count(&database).await?, 0);
        assert!(!project.root.join("RESULTS.md").exists());
        let (stop_supervisor, supervisor) = spawn(DaemonRole::Supervisor, paths.clone());
        healthy(&client, DaemonRole::Supervisor).await?;
        time::timeout(Duration::from_secs(10), async {
            loop {
                let completed: i64 = sqlx::query_scalar("SELECT count(*) FROM actions WHERE kind = 'generate_report' AND state = 'succeeded'")
                    .fetch_one(database.pool()).await?;
                if completed == 1 {
                    return Ok::<_, sqlx::Error>(());
                }
                time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await??;
        let published = fs::read_to_string(project.root.join("RESULTS.md"))?;
        assert!(published.starts_with("<!-- igor:generated-file:v1"));
        assert!(published.contains("Status: **Ready**"));
        let spec: String =
            sqlx::query_scalar("SELECT spec_json FROM actions WHERE kind = 'generate_report'")
                .fetch_one(database.pool())
                .await?;
        let frozen: ReportActionSpec = serde_json::from_str(&spec)?;
        assert_eq!(frozen.family_id, prepared.family.id);
        assert_eq!(frozen.generation, prepared.generation.identity);
        assert_eq!(frozen.project, project);
        assert!(frozen.report.enabled);
        assert_eq!(frozen.report.output, project.root.join("RESULTS.md"));
        stop_supervisor
            .send(())
            .map_err(|_| "supervisor stop receiver closed")?;
        time::timeout(Duration::from_secs(5), supervisor).await???;
        let (stop_supervisor, supervisor) = spawn(DaemonRole::Supervisor, paths.clone());
        healthy(&client, DaemonRole::Supervisor).await?;
        let (_sender, shutdown) = watch::channel(false);
        discover(&database, None, &shutdown).await?;
        assert_eq!(report_count(&database).await?, 1);
        assert_eq!(
            fs::read_to_string(project.root.join("RESULTS.md"))?,
            published
        );
        let events: i64 = sqlx::query_scalar("SELECT count(*) FROM events WHERE kind = 'action_state_changed' AND json_extract(payload_json, '$.data.details.trigger') = 'family-completed'")
            .fetch_one(database.pool()).await?;
        assert_eq!(events, 1);
        let attempts: i64 = sqlx::query_scalar("SELECT count(*) FROM attempts")
            .fetch_one(database.pool())
            .await?;
        assert_eq!(attempts, 2);
        assert!(
            database
                .jobs()
                .list(Some(project.id))
                .await?
                .iter()
                .all(|job| job.state == JobState::Succeeded)
        );
        stop_supervisor
            .send(())
            .map_err(|_| "supervisor stop receiver closed")?;
        stop_worker
            .send(())
            .map_err(|_| "worker stop receiver closed")?;
        time::timeout(Duration::from_secs(5), supervisor).await???;
        time::timeout(Duration::from_secs(5), worker).await???;
        Ok(())
    }

    #[tokio::test]
    async fn report_executor_waits_for_extraction_and_honors_late_disable() -> TestResult {
        for disable in [false, true] {
            let temporary = TempDir::new()?;
            let (project, mut prepared) = repository(&temporary.path().join("project"), true)?;
            let command = prepared.submissions[0].job.command.clone();
            for member in &mut prepared.submissions {
                let mut value = serde_json::to_value(&member.attempt)?;
                value["result"]["extractor"] = serde_json::to_value(&command)?;
                member.attempt = serde_json::from_value(value)?;
            }
            let database = Database::open(temporary.path().join("igor.db")).await?;
            successful_fixture(&database, &project, &prepared).await?;
            for member in &prepared.submissions {
                let id = igor_core::ActionId::new().to_string();
                sqlx::query("INSERT INTO actions (id, project_id, job_id, attempt_id, kind, state, spec_json, idempotency_key, available_at) VALUES (?, ?, ?, ?, 'extract_metrics', 'pending', '{}', ?, '2000-01-01')")
                    .bind(&id).bind(project.id.to_string()).bind(member.job.id.to_string()).bind(member.attempt.id().to_string())
                    .bind(&id).execute(database.pool()).await?;
            }
            let (stop, shutdown) = watch::channel(false);
            discover(&database, None, &shutdown).await?;
            let executor = tokio::spawn(crate::reports::run(database.clone(), shutdown));
            time::timeout(Duration::from_secs(10), async {
                loop {
                    let waiting: i64 = sqlx::query_scalar("SELECT count(*) FROM actions WHERE kind = 'generate_report' AND state = 'pending' AND last_error = 'report_context_not_ready' AND attempts = 0")
                        .fetch_one(database.pool()).await?;
                    if waiting == 1 { return Ok::<_, sqlx::Error>(()); }
                    time::sleep(Duration::from_millis(20)).await;
                }
            }).await??;
            assert!(!project.root.join("RESULTS.md").exists());
            if disable {
                fs::write(
                    &project.config_path,
                    fs::read_to_string(&project.config_path)?
                        .replace("enabled = true", "enabled = false"),
                )?;
            } else {
                for _ in &prepared.submissions {
                    let claim = database
                        .actions()
                        .claim_extraction("fixture extractor", Duration::from_secs(30))
                        .await?
                        .ok_or("extraction")?;
                    let work = database
                        .actions()
                        .claimed(&claim)
                        .await?
                        .ok_or("extraction work")?;
                    let attempt_id = work.attempt_id.ok_or("attempt")?;
                    assert!(
                        prepared
                            .submissions
                            .iter()
                            .any(|item| item.attempt.id() == attempt_id)
                    );
                    let document = serde_json::json!({"schema_version":1,"status":"success","metrics":{"score":0.875},
                        "selection":{"split":"validation","metric":"score","value":0.875}});
                    database
                        .actions()
                        .complete_extraction(&claim, attempt_id, &document)
                        .await?;
                }
            }
            sqlx::query(
                "UPDATE actions SET available_at = '2000-01-01' WHERE kind = 'generate_report'",
            )
            .execute(database.pool())
            .await?;
            let expected = if disable { "cancelled" } else { "succeeded" };
            time::timeout(Duration::from_secs(10), async {
                loop {
                    let state: String = sqlx::query_scalar(
                        "SELECT state FROM actions WHERE kind = 'generate_report'",
                    )
                    .fetch_one(database.pool())
                    .await?;
                    if state == expected {
                        return Ok::<_, sqlx::Error>(());
                    }
                    time::sleep(Duration::from_millis(20)).await;
                }
            })
            .await??;
            if disable {
                assert!(!project.root.join("RESULTS.md").exists());
            } else {
                let markdown = fs::read_to_string(project.root.join("RESULTS.md"))?;
                assert!(markdown.contains("Status: **Ready**"));
                assert!(markdown.contains("0.875"));
            }
            assert!(
                database
                    .jobs()
                    .list(Some(project.id))
                    .await?
                    .iter()
                    .all(|job| job.state == JobState::Succeeded)
            );
            stop.send(true)?;
            time::timeout(Duration::from_secs(5), executor).await??;
        }
        Ok(())
    }

    #[tokio::test]
    async fn report_executor_manuscript_failure_exhausts_report_retries_only() -> TestResult {
        let temporary = TempDir::new()?;
        let (project, prepared) = repository(&temporary.path().join("project"), true)?;
        let database = Database::open(temporary.path().join("igor.db")).await?;
        successful_fixture(&database, &project, &prepared).await?;
        let output = project.root.join("RESULTS.md");
        fs::write(&output, "# Handwritten results\nKeep these notes.\n")?;
        let (stop, shutdown) = watch::channel(false);
        discover(&database, None, &shutdown).await?;
        let executor = tokio::spawn(crate::reports::run(database.clone(), shutdown));
        for expected_attempt in 1..=3 {
            time::timeout(Duration::from_secs(10), async {
                loop {
                    let (state, attempts): (String, i64) = sqlx::query_as(
                        "SELECT state, attempts FROM actions WHERE kind = 'generate_report'",
                    )
                    .fetch_one(database.pool())
                    .await?;
                    if attempts == expected_attempt
                        && state
                            == if expected_attempt == 3 {
                                "failed"
                            } else {
                                "pending"
                            }
                    {
                        return Ok::<_, sqlx::Error>(());
                    }
                    time::sleep(Duration::from_millis(20)).await;
                }
            })
            .await??;
            assert_eq!(
                fs::read(&output)?,
                b"# Handwritten results\nKeep these notes.\n"
            );
            sqlx::query(
                "UPDATE actions SET available_at = '2000-01-01' WHERE kind = 'generate_report'",
            )
            .execute(database.pool())
            .await?;
        }
        assert!(
            database
                .jobs()
                .list(Some(project.id))
                .await?
                .iter()
                .all(|job| job.state == JobState::Succeeded)
        );
        let attempts: i64 = sqlx::query_scalar("SELECT count(*) FROM attempts")
            .fetch_one(database.pool())
            .await?;
        assert_eq!(attempts, 2);
        stop.send(true)?;
        time::timeout(Duration::from_secs(5), executor).await??;
        Ok(())
    }

    #[tokio::test]
    async fn report_executor_busy_output_defers_without_blocking_database_writes() -> TestResult {
        let temporary = TempDir::new()?;
        let (project, prepared) = repository(&temporary.path().join("project"), true)?;
        let database = Database::open(temporary.path().join("igor.db")).await?;
        successful_fixture(&database, &project, &prepared).await?;
        let lock = nix::fcntl::Flock::lock(
            fs::File::open(&project.root)?,
            nix::fcntl::FlockArg::LockExclusive,
        )
        .map_err(|(_, error)| std::io::Error::from_raw_os_error(error as i32))?;
        let (stop, shutdown) = watch::channel(false);
        discover(&database, None, &shutdown).await?;
        let executor = tokio::spawn(crate::reports::run(database.clone(), shutdown));
        time::timeout(Duration::from_secs(10), async {
            loop {
                let waiting: i64 = sqlx::query_scalar("SELECT count(*) FROM actions WHERE kind = 'generate_report' AND state = 'pending' AND attempts = 0 AND last_error = 'report_output_changed_or_busy'")
                    .fetch_one(database.pool()).await?;
                if waiting == 1 { return Ok::<_, sqlx::Error>(()); }
                time::sleep(Duration::from_millis(20)).await;
            }
        }).await??;
        let mut writer = time::timeout(
            Duration::from_secs(1),
            database.pool().begin_with("BEGIN IMMEDIATE"),
        )
        .await??;
        sqlx::query("UPDATE jobs SET priority = priority + 1")
            .execute(&mut *writer)
            .await?;
        writer.commit().await?;
        assert!(!project.root.join("RESULTS.md").exists());
        drop(lock);
        sqlx::query(
            "UPDATE actions SET available_at = '2000-01-01' WHERE kind = 'generate_report'",
        )
        .execute(database.pool())
        .await?;
        time::timeout(Duration::from_secs(10), async {
            loop {
                let state: String =
                    sqlx::query_scalar("SELECT state FROM actions WHERE kind = 'generate_report'")
                        .fetch_one(database.pool())
                        .await?;
                if state == "succeeded" {
                    return Ok::<_, sqlx::Error>(());
                }
                time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await??;
        assert!(fs::read_to_string(project.root.join("RESULTS.md"))?.contains("Status: **Ready**"));
        stop.send(true)?;
        time::timeout(Duration::from_secs(5), executor).await??;
        Ok(())
    }
}
