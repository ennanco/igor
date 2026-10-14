use std::{
    error::Error,
    fs,
    os::unix::fs::PermissionsExt,
    path::Path,
    process::{Child, Command, Stdio},
    thread,
    time::{Duration, Instant},
};

use igor_core::{
    AttemptId, AttemptState, ContainerRecord, Database, DockerContainerState, JobId, JobState,
    ProcessRecord, TransitionState, UnitRecord,
};
use nix::{
    sys::signal::{Signal, kill, killpg},
    unistd::Pid,
};
use serde_json::{Value, json};
use sha2::Digest;
use tempfile::TempDir;

type TestResult = Result<(), Box<dyn Error>>;

struct WorkerTestGuard(std::path::PathBuf);

impl Drop for WorkerTestGuard {
    fn drop(&mut self) {
        let _ = fs::remove_dir(&self.0);
    }
}

fn worker_test_guard() -> Result<WorkerTestGuard, Box<dyn Error>> {
    let path = std::env::temp_dir().join(format!("igor-job-cli-tests-{}", std::process::id()));
    loop {
        match fs::create_dir(&path) {
            Ok(()) => return Ok(WorkerTestGuard(path)),
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                thread::sleep(Duration::from_millis(10));
            }
            Err(error) => return Err(error.into()),
        }
    }
}

fn command(home: &Path, cwd: &Path) -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_igor"));
    command
        .env_clear()
        .current_dir(cwd)
        .env("HOME", home)
        .env("XDG_CONFIG_HOME", home.join("config"))
        .env("XDG_STATE_HOME", home.join("state"))
        .env("XDG_RUNTIME_DIR", home.join("runtime"));
    command
}

fn git(root: &Path, args: &[&str]) -> TestResult {
    let status = Command::new("git")
        .arg("-C")
        .arg(root)
        .args(args)
        .status()?;
    if !status.success() {
        return Err(format!("git {args:?} failed with {status}").into());
    }
    Ok(())
}

struct Worker(Child);

impl Worker {
    fn stop(&mut self) -> TestResult {
        if self.0.try_wait()?.is_none() {
            let pid = i32::try_from(self.0.id())?;
            kill(Pid::from_raw(pid), Signal::SIGTERM)?;
            self.0.wait()?;
        }
        Ok(())
    }
}

fn spawn_worker(home: &Path, project: &Path) -> Result<Worker, Box<dyn Error>> {
    Ok(Worker(
        command(home, project)
            .env("TELEGRAM_BOT_TOKEN", "must-not-leak")
            .env("OPENCODE_TEST_SECRET", "must-not-leak")
            .arg("worker")
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()?,
    ))
}

fn spawn_systemd_worker(home: &Path, project: &Path, bus: &str) -> Result<Worker, Box<dyn Error>> {
    Ok(Worker(
        command(home, project)
            .env("DBUS_SESSION_BUS_ADDRESS", bus)
            .arg("worker")
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()?,
    ))
}

fn spawn_fake_systemd_worker(
    home: &Path,
    project: &Path,
    bin: &Path,
    state: &Path,
) -> Result<Worker, Box<dyn Error>> {
    let inherited = std::env::var_os("PATH").unwrap_or_default();
    let path = std::env::join_paths(
        std::iter::once(bin.to_path_buf()).chain(std::env::split_paths(&inherited)),
    )?;
    Ok(Worker(
        command(home, project)
            .env("PATH", path)
            .env("IGOR_FAKE_UNIT_STATE", state)
            .arg("worker")
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()?,
    ))
}

struct LiveUnitCleanup {
    bus: String,
    units: Vec<String>,
}

impl Drop for LiveUnitCleanup {
    fn drop(&mut self) {
        for unit in &self.units {
            for operation in ["stop", "reset-failed"] {
                let _ = Command::new("systemctl")
                    .env("DBUS_SESSION_BUS_ADDRESS", &self.bus)
                    .args(["--user", operation, "--", unit])
                    .output();
            }
        }
    }
}

fn spawn_docker_worker(home: &Path, project: &Path) -> Result<Worker, Box<dyn Error>> {
    let mut worker = command(home, project);
    worker
        .env("PATH", std::env::var_os("PATH").ok_or("PATH is not set")?)
        .env("TELEGRAM_BOT_TOKEN", "must-not-leak")
        .env("OPENCODE_TEST_SECRET", "must-not-leak");
    for name in [
        "DOCKER_HOST",
        "DOCKER_TLS_VERIFY",
        "DOCKER_CERT_PATH",
        "DOCKER_CONTEXT",
    ] {
        if let Some(value) = std::env::var_os(name) {
            worker.env(name, value);
        }
    }
    Ok(Worker(
        worker
            .arg("worker")
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()?,
    ))
}

struct FakeDockerOptions<'a> {
    exit_code: i32,
    oom_killed: bool,
    fail_command: Option<&'a str>,
    state_dir: Option<&'a Path>,
    crash_point: Option<&'a str>,
}

fn spawn_fake_docker_worker(
    home: &Path,
    project: &Path,
    docker_directory: &Path,
    invocation_log: &Path,
    options: FakeDockerOptions<'_>,
) -> Result<Worker, Box<dyn Error>> {
    let inherited = std::env::var_os("PATH").unwrap_or_default();
    let path = std::env::join_paths(
        std::iter::once(docker_directory.to_path_buf()).chain(std::env::split_paths(&inherited)),
    )?;
    let mut command = command(home, project);
    command
        .env("PATH", path)
        .env("IGOR_FAKE_DOCKER_LOG", invocation_log)
        .env("IGOR_FAKE_DOCKER_EXIT", options.exit_code.to_string())
        .env(
            "IGOR_FAKE_DOCKER_OOM",
            if options.oom_killed { "true" } else { "false" },
        )
        .env("IGOR_FAKE_DOCKER_FAIL", options.fail_command.unwrap_or(""))
        .env("IGOR_FAKE_DOCKER_CRASH", options.crash_point.unwrap_or(""))
        .env(
            "IGOR_FAKE_DOCKER_KILL_NONZERO_AFTER_STOP",
            if options.crash_point == Some("kill_nonzero_after_stop") {
                "true"
            } else {
                "false"
            },
        )
        .env("TELEGRAM_BOT_TOKEN", "must-not-leak")
        .env("OPENCODE_TEST_SECRET", "must-not-leak");
    if let Some(state_dir) = options.state_dir {
        command.env("IGOR_FAKE_DOCKER_STATE", state_dir);
    }
    Ok(Worker(
        command
            .arg("worker")
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()?,
    ))
}

fn write_fake_docker(directory: &Path) -> TestResult {
    fs::create_dir_all(directory)?;
    let path = directory.join("docker");
    fs::write(
        &path,
        r#"#!/bin/sh
set -eu
printf '%s\n' "$*" >> "${IGOR_FAKE_DOCKER_LOG:?}"
STATE_DIR="${IGOR_FAKE_DOCKER_STATE:-}"
CRASH_POINT="${IGOR_FAKE_DOCKER_CRASH:-}"
if [ "${IGOR_FAKE_DOCKER_FAIL:-}" = "$1" ]; then
  printf 'forced fake Docker failure for %s\n' "$1" >&2
  exit 42
fi
case "$1" in
  --version) printf 'Docker version 26.0.0\n' ;;
  version) printf '26.0.0\n' ;;
  image) printf 'sha256:%064d\n' 0 | tr '0' 'a' ;;
  create)
    if [ -n "$STATE_DIR" ]; then
      touch "$STATE_DIR/created"
      shift
      while [ "$#" -gt 0 ]; do
        case "$1" in
          --name) shift; printf '%s' "$1" > "$STATE_DIR/name" ;;
          --label)
            shift
            case "$1" in
              igor.project_id=*) printf '%s' "${1#*=}" > "$STATE_DIR/project_id" ;;
              igor.job_id=*) printf '%s' "${1#*=}" > "$STATE_DIR/job_id" ;;
              igor.attempt_id=*) printf '%s' "${1#*=}" > "$STATE_DIR/attempt_id" ;;
              igor.generation_id=*) printf '%s' "${1#*=}" > "$STATE_DIR/generation_id" ;;
            esac
            ;;
        esac
        shift
      done
    fi
    printf 'fake-container\n'
    if [ "$CRASH_POINT" = after_create ]; then kill -KILL "$PPID"; fi
    ;;
  start)
    if [ "$CRASH_POINT" = before_start ]; then kill -KILL "$PPID"; exit 1; fi
    if [ -n "$STATE_DIR" ]; then
      touch "$STATE_DIR/running"
      rm -f "$STATE_DIR/created" "$STATE_DIR/stopped"
    fi
    if [ "$CRASH_POINT" = after_start ]; then kill -KILL "$PPID"; fi
    ;;
  stop)
    if [ -n "$STATE_DIR" ]; then
      touch "$STATE_DIR/stopped"
    fi
    ;;
  kill)
    if [ -n "$STATE_DIR" ]; then touch "$STATE_DIR/stopped"; fi
    if [ "${IGOR_FAKE_DOCKER_KILL_NONZERO_AFTER_STOP:-false}" = true ]; then
      printf 'container exited before kill\n' >&2
      exit 1
    fi
    ;;
  logs) printf 'fake stdout\n'; printf 'fake stderr\n' >&2 ;;
  wait)
    if [ -n "$STATE_DIR" ]; then
      while [ ! -f "$STATE_DIR/stopped" ]; do
        sleep 0.2
      done
    fi
    printf '%s\n' "${IGOR_FAKE_DOCKER_EXIT:-0}"
    if [ "$CRASH_POINT" = after_wait ]; then kill -KILL "$PPID"; fi
    ;;
  inspect)
    if [ -n "$STATE_DIR" ] && [ -f "$STATE_DIR/stopped" ]; then
      STATUS=exited; RUNNING=false; EXIT_CODE="${IGOR_FAKE_DOCKER_EXIT:-0}"; OOM="${IGOR_FAKE_DOCKER_OOM:-false}"
    elif [ -n "$STATE_DIR" ] && [ -f "$STATE_DIR/running" ]; then
      STATUS=running; RUNNING=true; EXIT_CODE=0; OOM=false
    elif [ -n "$STATE_DIR" ] && [ -f "$STATE_DIR/created" ]; then
      STATUS=created; RUNNING=false; EXIT_CODE=0; OOM=false
    else
      STATUS=exited; RUNNING=false; EXIT_CODE="${IGOR_FAKE_DOCKER_EXIT:-0}"; OOM="${IGOR_FAKE_DOCKER_OOM:-false}"
    fi
    if [ -n "$STATE_DIR" ] && [ "${3:-}" = '{{json .}}' ]; then
      GENERATION=''
      if [ -f "$STATE_DIR/generation_id" ]; then
        GENERATION=$(printf ',"igor.generation_id":"%s"' "$(cat "$STATE_DIR/generation_id")")
      fi
      printf '{"Id":"fake-container","Name":"/%s","Image":"sha256:%s","Config":{"Labels":{"igor.project_id":"%s","igor.job_id":"%s","igor.attempt_id":"%s"%s}},"State":{"Status":"%s","Running":%s,"ExitCode":%s,"OOMKilled":%s,"Error":""}}\n' \
        "$(cat "$STATE_DIR/name")" "$(printf '%064d' 0 | tr '0' 'a')" \
        "$(cat "$STATE_DIR/project_id")" "$(cat "$STATE_DIR/job_id")" "$(cat "$STATE_DIR/attempt_id")" "$GENERATION" \
        "$STATUS" "$RUNNING" "$EXIT_CODE" "$OOM"
    else
      printf '{"Status":"%s","Running":%s,"ExitCode":%s,"OOMKilled":%s,"Error":""}\n' "$STATUS" "$RUNNING" "$EXIT_CODE" "$OOM"
    fi
    ;;
  rm)
    if [ "$CRASH_POINT" = before_rm ]; then kill -KILL "$PPID"; exit 1; fi
    if [ -n "$STATE_DIR" ]; then
      rm -f "$STATE_DIR/created" "$STATE_DIR/running" "$STATE_DIR/stopped"
    fi
    ;;
  ps)
    if [ -n "$STATE_DIR" ] && { [ -f "$STATE_DIR/created" ] || [ -f "$STATE_DIR/running" ] || [ -f "$STATE_DIR/stopped" ]; }; then
      printf 'fake-container\n'
    fi
    ;;
  *) exit 64 ;;
esac
"#,
    )?;
    fs::set_permissions(&path, fs::Permissions::from_mode(0o700))?;
    Ok(())
}

impl Drop for Worker {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

struct Fixture {
    _temporary: TempDir,
    home: std::path::PathBuf,
    project: std::path::PathBuf,
    worker: Worker,
}

impl Fixture {
    fn wait_until_worker_ready(&self, description: &str) -> Result<(), Box<dyn Error>> {
        for _ in 0..100 {
            if self.home.join("runtime/igor/worker.sock").exists()
                && self
                    .run()
                    .args(["project", "list", "--json"])
                    .output()
                    .is_ok_and(|output| output.status.success())
            {
                return Ok(());
            }
            thread::sleep(Duration::from_millis(20));
        }
        Err(format!("{description} worker did not become ready").into())
    }

    fn new() -> Result<Self, Box<dyn Error>> {
        Self::with_global_config(None)
    }

    fn with_global_config(global_config: Option<&str>) -> Result<Self, Box<dyn Error>> {
        let temporary = TempDir::new()?;
        let home = temporary.path().join("home");
        let project = temporary.path().join("project");
        let initialized = command(&home, temporary.path())
            .args(["init", project.to_str().ok_or("non-UTF-8 project")?])
            .output()?;
        assert!(initialized.status.success());
        fs::write(project.join("science.json"), b"{\"seed\":1}\n")?;
        git(&project, &["init", "-q"])?;
        git(&project, &["config", "user.email", "igor@example.invalid"])?;
        git(&project, &["config", "user.name", "Igor Test"])?;
        git(&project, &["add", "."])?;
        git(&project, &["commit", "-qm", "initial"])?;
        if let Some(global_config) = global_config {
            let config = home.join("config/igor/config.toml");
            fs::create_dir_all(config.parent().ok_or("missing config parent")?)?;
            fs::write(config, global_config)?;
        }
        let worker = spawn_worker(&home, &project)?;
        for _ in 0..100 {
            if home.join("runtime/igor/worker.sock").exists() {
                let added = command(&home, &project)
                    .args([
                        "project",
                        "add",
                        project.to_str().ok_or("non-UTF-8 project")?,
                    ])
                    .output()?;
                if added.status.success() {
                    return Ok(Self {
                        _temporary: temporary,
                        home,
                        project,
                        worker,
                    });
                }
            }
            thread::sleep(Duration::from_millis(20));
        }
        Err("worker did not accept project registration".into())
    }

    fn run(&self) -> Command {
        command(&self.home, &self.project)
    }

    fn restart_worker(&mut self) -> Result<(), Box<dyn Error>> {
        self.worker.stop()?;
        self.worker = spawn_worker(&self.home, &self.project)?;
        self.wait_until_worker_ready("restarted")
    }

    fn restart_with_system_docker(&mut self) -> Result<(), Box<dyn Error>> {
        self.worker.stop()?;
        self.worker = spawn_docker_worker(&self.home, &self.project)?;
        self.wait_until_worker_ready("Docker")
    }

    fn restart_with_fake_docker(
        &mut self,
        exit_code: i32,
        oom_killed: bool,
        fail_command: Option<&str>,
    ) -> Result<std::path::PathBuf, Box<dyn Error>> {
        self.worker.stop()?;
        let docker_directory = self._temporary.path().join("fake-docker-bin");
        let invocation_log = self._temporary.path().join("fake-docker.log");
        write_fake_docker(&docker_directory)?;
        self.worker = spawn_fake_docker_worker(
            &self.home,
            &self.project,
            &docker_directory,
            &invocation_log,
            FakeDockerOptions {
                exit_code,
                oom_killed,
                fail_command,
                state_dir: None,
                crash_point: None,
            },
        )?;
        for _ in 0..100 {
            if self.home.join("runtime/igor/worker.sock").exists()
                && self
                    .run()
                    .args(["project", "list", "--json"])
                    .output()
                    .is_ok_and(|output| output.status.success())
            {
                return Ok(invocation_log);
            }
            thread::sleep(Duration::from_millis(20));
        }
        Err("fake-Docker worker did not become ready".into())
    }

    fn restart_with_stateful_fake_docker(
        &mut self,
        exit_code: i32,
        oom_killed: bool,
        fail_command: Option<&str>,
        existing_state_dir: Option<&Path>,
        crash_point: Option<&str>,
    ) -> Result<(std::path::PathBuf, std::path::PathBuf), Box<dyn Error>> {
        self.worker.stop()?;
        let docker_directory = self._temporary.path().join("fake-docker-bin");
        let invocation_log = self._temporary.path().join("fake-docker.log");
        let state_dir = match existing_state_dir {
            Some(dir) => dir.to_path_buf(),
            None => {
                let dir = self._temporary.path().join("fake-docker-state");
                fs::create_dir_all(&dir)?;
                dir
            }
        };
        write_fake_docker(&docker_directory)?;
        self.worker = spawn_fake_docker_worker(
            &self.home,
            &self.project,
            &docker_directory,
            &invocation_log,
            FakeDockerOptions {
                exit_code,
                oom_killed,
                fail_command,
                state_dir: Some(&state_dir),
                crash_point,
            },
        )?;
        for _ in 0..100 {
            if self.home.join("runtime/igor/worker.sock").exists()
                && self
                    .run()
                    .args(["project", "list", "--json"])
                    .output()
                    .is_ok_and(|output| output.status.success())
            {
                return Ok((invocation_log, state_dir));
            }
            thread::sleep(Duration::from_millis(20));
        }
        Err("stateful fake-Docker worker did not become ready".into())
    }
}

fn output_json(output: std::process::Output) -> Result<Value, Box<dyn Error>> {
    if !output.status.success() {
        return Err(String::from_utf8_lossy(&output.stderr).into_owned().into());
    }
    Ok(serde_json::from_slice(&output.stdout)?)
}

fn write_docker_job(fixture: &Fixture) -> Result<(std::path::PathBuf, String), Box<dyn Error>> {
    let digest = format!("sha256:{}", "a".repeat(64));
    let job_file = fixture._temporary.path().join("docker-job.toml");
    fs::write(
        &job_file,
        format!(
            "schema_version = 1\nname = 'fake Docker'\n\
             [execution]\nprogram = 'python'\nargs = ['train.py']\n\
             [executor]\nkind = 'docker'\n\
             [executor.settings]\nimage = 'example/image:tag'\ndigest = '{digest}'\nremove_container = true\n\
             [resources]\nmode = 'shared'\n"
        ),
    )?;
    Ok((job_file, digest))
}

fn submit_docker_job(fixture: &Fixture, job_file: &Path) -> Result<JobId, Box<dyn Error>> {
    let submitted = output_json(
        fixture
            .run()
            .args([
                "submit",
                "--file",
                job_file.to_str().ok_or("non-UTF-8 Docker job file")?,
                "--json",
            ])
            .output()?,
    )?;
    Ok(submitted["spec"]["id"]
        .as_str()
        .ok_or("missing Docker job ID")?
        .parse()?)
}

async fn wait_for_container(
    database: &Database,
    job_id: JobId,
) -> Result<ContainerRecord, Box<dyn Error>> {
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let detail = database
                .jobs()
                .detail(job_id)
                .await?
                .ok_or("missing submitted Docker job")?;
            let attempt = detail.attempts.last().ok_or("missing Docker attempt")?;
            if let Some(container) = database
                .jobs()
                .container_for_attempt(attempt.spec.id())
                .await?
                && container.state == DockerContainerState::Running
            {
                return Ok::<_, Box<dyn Error>>(container);
            }
            if detail.job.state.is_terminal() {
                return Err(format!(
                    "Docker job became {} before its container was running",
                    detail.job.state.as_str()
                )
                .into());
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .map_err(|_| "Docker container did not start")?
}

async fn wait_for_worker_crash(fixture: &mut Fixture) -> TestResult {
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if fixture.worker.0.try_wait()?.is_some() {
                return Ok::<_, Box<dyn Error>>(());
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .map_err(|_| "fake-Docker worker did not crash")??;
    Ok(())
}

async fn expire_execution_claim(database: &Database, job_id: JobId) -> TestResult {
    sqlx::query(
        "UPDATE jobs SET claim_expires_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now', '-1 second')
         WHERE id = ? AND claim_id IS NOT NULL",
    )
    .bind(job_id.to_string())
    .execute(database.pool())
    .await?;
    sqlx::query(
        "UPDATE resource_leases
         SET expires_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now', '-1 second')
         WHERE job_id = ?",
    )
    .bind(job_id.to_string())
    .execute(database.pool())
    .await?;
    Ok(())
}

async fn wait_for_removed_container(
    database: &Database,
    attempt_id: AttemptId,
) -> Result<ContainerRecord, Box<dyn Error>> {
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if let Some(container) = database.jobs().container_for_attempt(attempt_id).await?
                && container.state == DockerContainerState::Removed
            {
                return Ok::<_, Box<dyn Error>>(container);
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .map_err(|_| "Docker container was not removed")?
}

async fn run_docker_crash_boundary(
    crash_point: &str,
    complete_before_crash: bool,
    expected_start_commands: usize,
) -> TestResult {
    let mut fixture = Fixture::new()?;
    let (invocation_log, state_dir) =
        fixture.restart_with_stateful_fake_docker(0, false, None, None, Some(crash_point))?;
    let (job_file, _) = write_docker_job(&fixture)?;
    let job_id = submit_docker_job(&fixture, &job_file)?;
    let database = Database::open(fixture.home.join("state/igor/igor.sqlite3")).await?;
    if complete_before_crash {
        let _ = wait_for_container(&database, job_id).await?;
        fs::write(state_dir.join("stopped"), b"")?;
    }
    wait_for_worker_crash(&mut fixture).await?;
    expire_execution_claim(&database, job_id).await?;
    fixture.restart_with_stateful_fake_docker(0, false, None, Some(&state_dir), None)?;

    if matches!(crash_point, "after_create" | "before_start" | "after_start") {
        let _ = wait_for_container(&database, job_id).await?;
        fs::write(state_dir.join("stopped"), b"")?;
    }
    let waited = fixture
        .run()
        .args(["wait", &job_id.to_string(), "--json"])
        .output()?;
    assert!(waited.status.success(), "crash boundary {crash_point}");
    let detail = database
        .jobs()
        .detail(job_id)
        .await?
        .ok_or("missing crash-recovered Docker job")?;
    assert_eq!(detail.job.state, JobState::Succeeded);
    let attempt = detail.attempts.last().ok_or("missing recovered attempt")?;
    let container = wait_for_removed_container(&database, attempt.spec.id()).await?;
    assert_eq!(fs::read(&container.stdout_path)?, b"fake stdout\n");
    assert_eq!(fs::read(&container.stderr_path)?, b"fake stderr\n");
    let leases: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM resource_leases WHERE job_id = ?")
        .bind(job_id.to_string())
        .fetch_one(database.pool())
        .await?;
    assert_eq!(leases, 0);
    let terminal_events: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM events WHERE job_id = ? AND kind = 'attempt_state_changed'
         AND payload_json LIKE '%\"state\":\"succeeded\"%'",
    )
    .bind(job_id.to_string())
    .fetch_one(database.pool())
    .await?;
    assert_eq!(terminal_events, 1);
    let invocations = fs::read_to_string(invocation_log)?;
    assert_eq!(
        invocations
            .lines()
            .filter(|line| line.starts_with("create "))
            .count(),
        1,
        "crash boundary {crash_point} recreated the container"
    );
    assert_eq!(
        invocations
            .lines()
            .filter(|line| *line == "start fake-container")
            .count(),
        expected_start_commands,
        "unexpected start count at {crash_point}"
    );
    Ok(())
}

async fn run_fake_docker_case(
    exit_code: i32,
    oom_killed: bool,
    expected_error: Option<&str>,
) -> TestResult {
    let _guard = worker_test_guard()?;
    let mut fixture = Fixture::new()?;
    let invocation_log = fixture.restart_with_fake_docker(exit_code, oom_killed, None)?;
    let (job_file, digest) = write_docker_job(&fixture)?;
    let submitted = output_json(
        fixture
            .run()
            .args([
                "submit",
                "--file",
                job_file.to_str().ok_or("non-UTF-8 Docker job file")?,
                "--json",
            ])
            .output()?,
    )?;
    let job_id: JobId = submitted["spec"]["id"]
        .as_str()
        .ok_or("missing Docker job ID")?
        .parse()?;
    let waited = fixture
        .run()
        .args(["wait", &job_id.to_string(), "--json"])
        .output()?;
    assert_eq!(waited.status.success(), expected_error.is_none());
    let waited: Value = serde_json::from_slice(&waited.stdout)?;
    let attempt_id: AttemptId = waited["attempts"][0]["spec"]["id"]
        .as_str()
        .ok_or("missing Docker attempt ID")?
        .parse()?;
    assert_eq!(
        waited["job"]["state"],
        if expected_error.is_some() {
            "failed"
        } else {
            "succeeded"
        }
    );

    let database = Database::open(fixture.home.join("state/igor/igor.sqlite3")).await?;
    let container = database
        .jobs()
        .container_for_attempt(attempt_id)
        .await?
        .ok_or("missing durable Docker container")?;
    assert_eq!(container.state, DockerContainerState::Removed);
    assert_eq!(
        container.image_reference,
        format!("example/image:tag@{digest}")
    );
    assert_eq!(container.exit_code, Some(exit_code));
    assert_eq!(container.oom_killed, Some(oom_killed));
    match expected_error {
        Some(prefix) => assert!(
            container
                .error
                .as_deref()
                .is_some_and(|error| error.starts_with(prefix))
        ),
        None => assert_eq!(container.error, None),
    }
    let leases: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM resource_leases WHERE job_id = ?")
        .bind(job_id.to_string())
        .fetch_one(database.pool())
        .await?;
    assert_eq!(leases, 0);
    assert_eq!(fs::read_to_string(&container.stdout_path)?, "fake stdout\n");
    assert_eq!(fs::read_to_string(&container.stderr_path)?, "fake stderr\n");

    let invocations = fs::read_to_string(invocation_log)?;
    assert!(!invocations.contains("must-not-leak"));
    let invocations: Vec<_> = invocations.lines().collect();
    let position = |prefix: &str| {
        invocations
            .iter()
            .position(|line| line.starts_with(prefix))
            .unwrap_or_else(|| panic!("missing Docker invocation {prefix:?}: {invocations:?}"))
    };
    assert!(position("--version") < position("version --format"));
    assert!(position("version --format") < position("image inspect"));
    assert!(position("image inspect") < position("create "));
    assert!(position("create ") < position("start fake-container"));
    assert!(position("start fake-container") < position("logs --follow fake-container"));
    assert!(position("start fake-container") < position("wait fake-container"));
    assert!(position("logs --follow fake-container") < position("inspect --format"));
    assert!(position("wait fake-container") < position("inspect --format"));
    assert!(position("inspect --format") < position("rm --force fake-container"));
    assert!(invocations.iter().any(|line| {
        line.starts_with("image inspect") && line.ends_with(&format!("example/image:tag@{digest}"))
    }));
    Ok(())
}

fn configure_metric_extractor(fixture: &Fixture, script: &str) -> TestResult {
    let extractor = fixture.project.join("extract.sh");
    fs::write(&extractor, script)?;
    fs::set_permissions(&extractor, fs::Permissions::from_mode(0o700))?;
    git(&fixture.project, &["add", "extract.sh"])?;
    git(&fixture.project, &["commit", "-qm", "add metric extractor"])?;
    let config = fixture.project.join(".igor/project.toml");
    fs::write(
        &config,
        "schema_version = 1\n[results]\nextractor = ['./extract.sh']\nworking_directory = 'attempt'\n",
    )?;
    git(&fixture.project, &["add", ".igor/project.toml"])?;
    git(
        &fixture.project,
        &["commit", "-qm", "configure metric extractor"],
    )?;
    Ok(())
}

fn preserve_raw_success_logs(fixture: &Fixture) -> TestResult {
    let path = fixture.project.join(".igor/project.toml");
    let mut config: toml::Value = toml::from_str(&fs::read_to_string(&path)?)?;
    let table = config.as_table_mut().ok_or("project config table")?;
    let retention = table
        .entry("retention")
        .or_insert_with(|| toml::Value::Table(Default::default()));
    retention
        .as_table_mut()
        .ok_or("retention table")?
        .insert("compress_success_logs".into(), toml::Value::Boolean(false));
    fs::write(path, toml::to_string(&config)?)?;
    git(&fixture.project, &["add", ".igor/project.toml"])?;
    git(
        &fixture.project,
        &["commit", "-qm", "preserve raw logs for fixture inspection"],
    )?;
    Ok(())
}

#[tokio::test]
async fn failed_owned_outputs_and_full_logs_real_process_acceptance() -> TestResult {
    let _guard = worker_test_guard()?;
    let fixture = Fixture::new()?;
    let config = fixture.project.join(".igor/project.toml");
    fs::write(
        &config,
        "schema_version = 1\n[retention]\nfailed_full_logs = 'delete-after-diagnosis'\nfailed_artifacts = 'delete-after-diagnosis'\nfailed_log_tail_kb = 1\n[artifacts]\nkeep_on_failure = ['keep.bin']\n",
    )?;
    git(&fixture.project, &["add", ".igor/project.toml"])?;
    git(
        &fixture.project,
        &["commit", "-qm", "freeze failed log retention"],
    )?;
    let file = fixture._temporary.path().join("failed-logs.toml");
    let outside = fixture._temporary.path().join("external-output.bin");
    fs::write(
        &file,
        format!(
            "schema_version = 1\nname = 'failed binary logs'\n[execution]\nprogram = '/bin/sh'\nargs = ['-c', 'root=$(readlink /proc/$$/fd/1); root=${{root%/*}}; dd if=/dev/zero of=\"$root/disposable.bin\" bs=1024 count=256 2>/dev/null; for name in .scratch checkpoint.tmp results.gz config index; do printf disposable > \"$root/$name\"; done; printf keep > \"$root/keep.bin\"; printf project > checkout-output.bin; printf external > \"$1\"; i=0; while [ $i -lt 2000 ]; do printf \"\\000\\377x\"; printf \"\\000\\376y\" >&2; i=$((i+1)); done; exit 7', 'igor-fixture', '{}']\n[resources]\nmode = 'shared'\n",
            outside.display()
        ),
    )?;
    let submitted = output_json(
        fixture
            .run()
            .args(["submit", "--json", "--file", file.to_str().ok_or("file")?])
            .output()?,
    )?;
    let job: JobId = submitted["spec"]["id"].as_str().ok_or("job")?.parse()?;
    let done = fixture
        .run()
        .args(["wait", &job.to_string(), "--json"])
        .output()?;
    let done: Value = serde_json::from_slice(&done.stdout)?;
    assert_eq!(done["job"]["state"], "failed");
    let database = Database::open(fixture.home.join("state/igor/igor.sqlite3")).await?;
    let logs = database.jobs().logs_for_job(job).await?;
    let stdout = logs.stdout_path.ok_or("stdout")?;
    let stderr = logs.stderr_path.ok_or("stderr")?;
    let original = fs::read(&stdout)?;
    assert_eq!(original.len(), 6000);
    let expected = &original[original.len() - 1024..];
    let original_stderr = fs::read(&stderr)?;
    assert_eq!(original_stderr.len(), 6000);
    let expected_stderr = &original_stderr[4976..];
    let root = stdout.parent().ok_or("private namespace")?;
    let disposable = root.join("disposable.bin");
    let keep = root.join("keep.bin");
    assert_eq!(fs::metadata(&disposable)?.len(), 256 * 1024);
    for name in [
        ".scratch",
        "checkpoint.tmp",
        "results.gz",
        "config",
        "index",
    ] {
        assert!(root.join(name).is_file());
    }
    assert_eq!(fs::read(&keep)?, b"keep");
    let artifacts: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM artifacts WHERE attempt_id = ?")
        .bind(logs.attempt_id.to_string())
        .fetch_one(database.pool())
        .await?;
    assert_eq!(artifacts, 0);
    let before = output_json(
        fixture
            .run()
            .args(["gc", "--failed", "--dry-run", "--json"])
            .output()?,
    )?;
    assert!(
        before["entries"]
            .as_array()
            .ok_or("entries")?
            .iter()
            .any(|e| e["operation"] == "owned_failed_outputs" && e["status"] == "deferred")
    );
    assert_eq!(fs::read(&stdout)?, original);
    assert_eq!(fs::read(&stderr)?, original_stderr);
    assert!(disposable.exists());
    let id = igor_core::RecoveryId::new().to_string();
    sqlx::query("INSERT INTO recoveries(id,attempt_id,state,diagnosis_json,idempotency_key) VALUES (?,?,'proposed','{}',?)").bind(&id).bind(logs.attempt_id.to_string()).bind(&id).execute(database.pool()).await?;
    let plan = output_json(
        fixture
            .run()
            .args(["gc", "--failed", "--dry-run", "--json"])
            .output()?,
    )?;
    let entries = plan["entries"].as_array().ok_or("GC entries")?;
    assert_eq!(
        entries
            .iter()
            .filter(|e| e["operation"] == "failed_log_retirement" && e["status"] == "ready")
            .count(),
        2
    );
    assert_eq!(fs::read(&stdout)?, original);
    assert!(
        entries
            .iter()
            .any(|e| e["operation"] == "owned_failed_outputs" && e["status"] == "ready")
    );
    let manifests: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM owned_output_manifest")
        .fetch_one(database.pool())
        .await?;
    assert_eq!(manifests, 0, "preview must not discover durably");
    let home = fixture.home.clone();
    let paths = igor_core::RuntimePaths {
        config_file: home.join("config/igor/config.toml"),
        state_dir: home.join("state/igor"),
        runtime_dir: home.join("runtime/igor"),
        database: home.join("state/igor/igor.sqlite3"),
        log_dir: home.join("state/igor/logs"),
        worktree_dir: home.join("state/igor/worktrees"),
        report_dir: home.join("state/igor/reports"),
        worker_socket: home.join("runtime/igor/worker.sock"),
        supervisor_socket: home.join("runtime/igor/supervisor.sock"),
        resource_dir: home.join("state/igor/resources"),
    };
    let mut supervisor = spawn_metric_supervisor(&fixture)?;
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM failed_log_retirements WHERE attempt_id = ? AND state = 'completed'").bind(logs.attempt_id.to_string()).fetch_one(database.pool()).await?;
            let outputs: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM owned_output_manifest WHERE attempt_id = ? AND state = 'completed'").bind(logs.attempt_id.to_string()).fetch_one(database.pool()).await?;
            if count == 2 && outputs == 6 { return Ok::<_,Box<dyn Error>>(()); }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }).await??;
    supervisor.stop()?;
    assert!(!stdout.exists() && !stderr.exists());
    assert!(!disposable.exists());
    for name in [
        ".scratch",
        "checkpoint.tmp",
        "results.gz",
        "config",
        "index",
    ] {
        assert!(!root.join(name).exists());
    }
    assert_eq!(fs::read(&keep)?, b"keep");
    assert_eq!(
        fs::read(fixture.project.join("checkout-output.bin"))?,
        b"project"
    );
    assert_eq!(fs::read(&outside)?, b"external");
    assert!(root.is_dir());
    let client = igor_daemon::Client::new(&paths);
    let identity = database
        .jobs()
        .retired_log_tail(logs.attempt_id, "stdout")
        .await?
        .ok_or("tail identity")?;
    assert!(
        client
            .request(
                igor_daemon::DaemonRole::Worker,
                igor_daemon::Request::RetiredLogChunk {
                    identity: identity.clone(),
                    offset: 0,
                    limit: 65537
                }
            )
            .await
            .is_err()
    );
    let chunk = client
        .request(
            igor_daemon::DaemonRole::Worker,
            igor_daemon::Request::RetiredLogChunk {
                identity,
                offset: 0,
                limit: 65536,
            },
        )
        .await?;
    let igor_daemon::Response::LogTailChunk(chunk) = chunk else {
        return Err("tail chunk response".into());
    };
    assert_eq!(chunk.bytes, expected);
    assert_eq!(chunk.start, 4976);
    assert_eq!(chunk.end, 6000);
    let output = fixture.run().args(["logs", &job.to_string()]).output()?;
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(output.stdout, expected);
    assert!(
        output
            .stderr
            .windows(b"log prefix omitted".len())
            .any(|w| w == b"log prefix omitted")
    );
    assert!(output.stderr.ends_with(expected_stderr));
    let followed = fixture
        .run()
        .args(["logs", &job.to_string(), "--follow"])
        .output()?;
    assert!(followed.status.success());
    assert_eq!(followed.stdout, expected);
    Ok(())
}

async fn wait_for_successful_extraction(
    database: &Database,
    action_id: &str,
    expected_attempts: i64,
    phase: &str,
) -> Result<(String, i64, Option<String>), Box<dyn Error>> {
    let result = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let row: (String, i64, Option<String>) =
                sqlx::query_as("SELECT state, attempts, last_error FROM actions WHERE id = ?")
                    .bind(action_id)
                    .fetch_one(database.pool())
                    .await?;
            if row.0 == "succeeded" && row.1 == expected_attempts {
                return Ok::<_, Box<dyn Error>>(row);
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await;
    match result {
        Ok(result) => result,
        Err(_) => {
            // Extraction errors are fixed diagnostic codes, not subprocess output or context.
            let (state, attempts, last_error): (String, i64, Option<String>) =
                sqlx::query_as("SELECT state, attempts, last_error FROM actions WHERE id = ?")
                    .bind(action_id)
                    .fetch_one(database.pool())
                    .await?;
            Err(format!(
                "{phase} extraction timed out: state={state}, attempts={attempts}, last_error={last_error:?}"
            )
            .into())
        }
    }
}

fn submit_metric_extractor_job(fixture: &Fixture) -> Result<JobId, Box<dyn Error>> {
    let job_file = fixture._temporary.path().join("metric-extractor-job.toml");
    fs::write(
        &job_file,
        "schema_version = 1\nname = 'metric extractor'\n[execution]\nprogram = '/bin/sh'\nargs = ['-c', 'printf training-output']\n[resources]\nmode = 'shared'\n",
    )?;
    let submitted = output_json(
        fixture
            .run()
            .args([
                "submit",
                "--json",
                "--file",
                job_file.to_str().ok_or("non-UTF-8 job file")?,
            ])
            .output()?,
    )?;
    Ok(submitted["spec"]["id"]
        .as_str()
        .ok_or("missing job id")?
        .parse()?)
}

fn spawn_metric_supervisor(fixture: &Fixture) -> Result<Worker, Box<dyn Error>> {
    Ok(Worker(
        command(&fixture.home, &fixture.project)
            .env("TELEGRAM_BOT_TOKEN", "must-not-leak")
            .env("OPENCODE_TEST_SECRET", "must-not-leak")
            .arg("supervisor")
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()?,
    ))
}

#[tokio::test]
async fn metric_extractor_runs_in_isolated_attempt_and_persists_document() -> TestResult {
    let _guard = worker_test_guard()?;
    let fixture = Fixture::new()?;
    let document = r#"{"schema_version":1,"status":"success","metrics":{"accuracy":0.75},"selection":{"split":"validation","metric":"accuracy","value":0.75,"epoch":3,"trained_epochs":5,"checkpoint":"checkpoints/best.bin"}}"#;
    configure_metric_extractor(
        &fixture,
        &format!(
            "#!/bin/sh\nset -eu\n[ -z \"${{TELEGRAM_BOT_TOKEN-}}\" ]\n[ -z \"${{OPENCODE_TEST_SECRET-}}\" ]\ncat > extractor-input.json\npwd > extractor-cwd\nprintf '%s\\n' '{}'\n",
            document
        ),
    )?;
    let mut supervisor = spawn_metric_supervisor(&fixture)?;
    wait_for_marker(&fixture.home.join("runtime/igor/supervisor.sock"))?;
    let job_id = submit_metric_extractor_job(&fixture)?;
    let completed = fixture
        .run()
        .args(["wait", &job_id.to_string(), "--json"])
        .output()?;
    assert!(
        completed.status.success(),
        "{}",
        String::from_utf8_lossy(&completed.stderr)
    );
    let completed: Value = serde_json::from_slice(&completed.stdout)?;
    assert_eq!(completed["job"]["state"], "succeeded");

    let database = Database::open(fixture.home.join("state/igor/igor.sqlite3")).await?;
    let detail = database.jobs().detail(job_id).await?.ok_or("missing job")?;
    assert_eq!(detail.job.state, JobState::Succeeded);
    let attempt = detail.attempts.last().ok_or("missing attempt")?;
    let attempt_id = attempt.spec.id();
    let expected: Value = serde_json::from_str(document)?;
    let completion = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if database.actions().document_for_attempt(attempt_id).await? == Some(expected.clone())
            {
                return Ok::<_, Box<dyn Error>>(());
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await;
    if completion.is_err() {
        let action: Option<(String, Option<String>, i64)> = sqlx::query_as(
            "SELECT state, last_error, attempts FROM actions WHERE attempt_id = ? AND kind = 'extract_metrics'",
        )
        .bind(attempt_id.to_string())
        .fetch_optional(database.pool())
        .await?;
        return Err(format!("metric extraction did not complete; action {action:?}").into());
    }
    completion??;
    let log_dir = fixture
        .home
        .join("state/igor/logs")
        .join(attempt_id.to_string());
    let observed = log_dir.join("extractor-input.json");
    let cwd = log_dir.join("extractor-cwd");
    let input: Value = serde_json::from_slice(&fs::read(observed)?)?;
    assert_eq!(
        input
            .as_object()
            .ok_or("extractor input is not an object")?
            .len(),
        9
    );
    assert_eq!(input["schema_version"], 1);
    assert_eq!(input["project_id"], detail.job.spec.project_id.to_string());
    assert_eq!(input["job_id"], job_id.to_string());
    assert_eq!(input["attempt_id"], attempt_id.to_string());
    assert_eq!(input["family_id"], Value::Null);
    assert_eq!(input["generation_id"], Value::Null);
    assert_eq!(input["seed"], Value::Null);
    assert_eq!(
        input["stdout_path"],
        log_dir
            .join("stdout.log")
            .to_str()
            .ok_or("non-UTF-8 stdout path")?
    );
    assert_eq!(
        input["stderr_path"],
        log_dir
            .join("stderr.log")
            .to_str()
            .ok_or("non-UTF-8 stderr path")?
    );
    assert_eq!(
        fs::read_to_string(cwd)?.trim(),
        log_dir.to_str().ok_or("non-UTF-8 attempt path")?
    );

    assert_eq!(
        database.actions().document_for_attempt(attempt_id).await?,
        Some(expected.clone())
    );
    assert_eq!(
        database.actions().selection_for_attempt(attempt_id).await?,
        Some(expected["selection"].clone())
    );
    assert_eq!(
        database.deliveries().available_metrics(attempt_id).await?,
        [("accuracy".into(), 0.75)]
    );
    let (state, last_error): (String, Option<String>) = sqlx::query_as(
        "SELECT state, last_error FROM actions WHERE job_id = ? AND kind = 'extract_metrics'",
    )
    .bind(job_id.to_string())
    .fetch_one(database.pool())
    .await?;
    assert_eq!(state, "succeeded");
    assert_eq!(last_error, None);
    supervisor.stop()?;
    Ok(())
}

#[tokio::test]
async fn metric_extractor_publishes_verified_artifacts() -> TestResult {
    let _guard = worker_test_guard()?;
    let fixture = Fixture::new()?;
    configure_metric_extractor(
        &fixture,
        "#!/bin/sh\nset -eu\ncat > extractor-input.json\nprintf result > output.bin\nprintf '%s\n' '{\"schema_version\":1,\"status\":\"success\",\"metrics\":{\"score\":1},\"artifacts\":[{\"path\":\"output.bin\",\"role\":\"other\",\"role_detail\":\"raw\"}]}'\n",
    )?;
    let mut supervisor = spawn_metric_supervisor(&fixture)?;
    wait_for_marker(&fixture.home.join("runtime/igor/supervisor.sock"))?;
    let job_id = submit_metric_extractor_job(&fixture)?;
    assert!(
        fixture
            .run()
            .args(["wait", &job_id.to_string()])
            .output()?
            .status
            .success()
    );
    let database = Database::open(fixture.home.join("state/igor/igor.sqlite3")).await?;
    let attempt_id = database
        .jobs()
        .detail(job_id)
        .await?
        .ok_or("missing job")?
        .attempts
        .last()
        .ok_or("missing attempt")?
        .spec
        .id();
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if database
                .actions()
                .document_for_attempt(attempt_id)
                .await?
                .is_some()
            {
                return Ok::<_, Box<dyn Error>>(());
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await??;
    let id: String =
        sqlx::query_scalar("SELECT id FROM artifacts WHERE attempt_id = ? AND path = 'output.bin'")
            .bind(attempt_id.to_string())
            .fetch_one(database.pool())
            .await?;
    let artifact = database
        .artifacts()
        .get(id.parse()?)
        .await?
        .ok_or("artifact not registered")?;
    assert_eq!(artifact.role, igor_core::ArtifactRole::Other("raw".into()));
    assert_eq!(artifact.size_bytes, Some(6));
    assert_eq!(
        artifact.sha256,
        Some(format!("sha256:{:x}", sha2::Sha256::digest(b"result")))
    );
    assert_eq!(
        artifact.state,
        igor_core::ArtifactPublicationState::Published
    );
    assert_eq!(
        artifact.lifecycle,
        igor_core::ArtifactLifecycleState::Published
    );
    assert!(!artifact.diagnosis_required);
    let staged = fixture
        .home
        .join("state/igor/artifact-staging")
        .join(attempt_id.to_string())
        .join(&id);
    assert_eq!(
        artifact.metadata["staged_path"],
        staged.to_str().ok_or("non-UTF-8 staged path")?
    );
    assert_eq!(fs::read(&staged)?, b"result");
    assert_eq!(fs::metadata(&staged)?.permissions().mode() & 0o777, 0o600);
    assert_eq!(
        fs::metadata(staged.parent().ok_or("missing staged parent")?)?
            .permissions()
            .mode()
            & 0o777,
        0o700
    );
    let published = fixture
        .project
        .join("artifacts")
        .join(attempt_id.to_string())
        .join(&id);
    assert_eq!(
        artifact.metadata["published_path"],
        published.to_str().ok_or("non-UTF-8 published path")?
    );
    assert_eq!(fs::read(&published)?, b"result");
    assert_eq!(
        fs::metadata(&published)?.permissions().mode() & 0o777,
        0o600
    );
    assert!(!fixture.project.join("artifacts/output.bin").exists());
    assert_eq!(
        database
            .jobs()
            .get_job(job_id)
            .await?
            .ok_or("missing job")?
            .state,
        JobState::Succeeded
    );
    supervisor.stop()?;
    Ok(())
}

#[tokio::test]
async fn repeating_successful_extraction_preserves_document_indexes_and_published_artifact()
-> TestResult {
    let _guard = worker_test_guard()?;
    let fixture = Fixture::new()?;
    configure_metric_extractor(
        &fixture,
        "#!/bin/sh\nset -eu\ncat > /dev/null\nif [ -f extractor-runs ]; then runs=$(cat extractor-runs); else runs=0; fi\nprintf '%s\n' \"$((runs+1))\" > extractor-runs\nprintf same-data > repeat.bin\nprintf '%s\n' '{\"schema_version\":1,\"status\":\"success\",\"metrics\":{\"score\":0.75},\"selection\":{\"split\":\"validation\",\"metric\":\"score\",\"value\":0.75},\"artifacts\":[{\"path\":\"repeat.bin\",\"role\":\"metrics\"}]}'\n",
    )?;
    // Intentional extractor replay requires raw source logs after the first success;
    // otherwise the concurrent archive task may compress and retire them before replay.
    preserve_raw_success_logs(&fixture)?;
    let mut supervisor = spawn_metric_supervisor(&fixture)?;
    wait_for_marker(&fixture.home.join("runtime/igor/supervisor.sock"))?;
    let job_id = submit_metric_extractor_job(&fixture)?;
    assert!(
        fixture
            .run()
            .args(["wait", &job_id.to_string()])
            .output()?
            .status
            .success()
    );
    let database = Database::open(fixture.home.join("state/igor/igor.sqlite3")).await?;
    let attempt_id = database
        .jobs()
        .detail(job_id)
        .await?
        .ok_or("missing job")?
        .attempts[0]
        .spec
        .id();
    let action_id: String = sqlx::query_scalar(
        "SELECT id FROM actions WHERE attempt_id = ? AND kind = 'extract_metrics'",
    )
    .bind(attempt_id.to_string())
    .fetch_one(database.pool())
    .await?;
    wait_for_successful_extraction(&database, &action_id, 1, "first").await?;
    let before = output_json(
        fixture
            .run()
            .args(["metrics", &job_id.to_string(), "--json"])
            .output()?,
    )?;
    assert_eq!(before["attempts"][0]["document"]["metrics"]["score"], 0.75);
    let artifact_id: String =
        sqlx::query_scalar("SELECT id FROM artifacts WHERE attempt_id = ? AND path = 'repeat.bin'")
            .bind(attempt_id.to_string())
            .fetch_one(database.pool())
            .await?;
    let published = fixture
        .project
        .join("artifacts")
        .join(attempt_id.to_string())
        .join(&artifact_id);
    assert_eq!(fs::read(&published)?, b"same-data");
    let counts_before = (
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM metric_documents WHERE attempt_id = ?")
            .bind(attempt_id.to_string())
            .fetch_one(database.pool())
            .await?,
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM metrics WHERE attempt_id = ?")
            .bind(attempt_id.to_string())
            .fetch_one(database.pool())
            .await?,
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM metric_selections WHERE attempt_id = ?")
            .bind(attempt_id.to_string())
            .fetch_one(database.pool())
            .await?,
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM artifacts WHERE attempt_id = ?")
            .bind(attempt_id.to_string())
            .fetch_one(database.pool())
            .await?,
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM events WHERE kind = 'artifact_registered' AND attempt_id = ?",
        )
        .bind(attempt_id.to_string())
        .fetch_one(database.pool())
        .await?,
    );
    assert_eq!(counts_before, (1, 1, 1, 1, 1));

    supervisor.stop()?;
    let updated = sqlx::query("UPDATE actions SET state = 'pending', available_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now', '-1 second') WHERE id = ? AND state = 'succeeded'")
        .bind(&action_id).execute(database.pool()).await?;
    assert_eq!(updated.rows_affected(), 1);
    supervisor = spawn_metric_supervisor(&fixture)?;
    wait_for_marker(&fixture.home.join("runtime/igor/supervisor.sock"))?;
    let (state, attempts, last_error) =
        wait_for_successful_extraction(&database, &action_id, 2, "second (replay)").await?;
    assert_eq!(state, "succeeded");
    assert_eq!(attempts, 2);
    assert_eq!(last_error, None);
    assert_eq!(
        fs::read_to_string(
            fixture
                .home
                .join("state/igor/logs")
                .join(attempt_id.to_string())
                .join("extractor-runs")
        )?,
        "2\n"
    );
    assert_eq!(fs::read(&published)?, b"same-data");
    let after = output_json(
        fixture
            .run()
            .args(["metrics", &job_id.to_string(), "--json"])
            .output()?,
    )?;
    assert_eq!(before, after);
    let counts_after = (
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM metric_documents WHERE attempt_id = ?")
            .bind(attempt_id.to_string())
            .fetch_one(database.pool())
            .await?,
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM metrics WHERE attempt_id = ?")
            .bind(attempt_id.to_string())
            .fetch_one(database.pool())
            .await?,
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM metric_selections WHERE attempt_id = ?")
            .bind(attempt_id.to_string())
            .fetch_one(database.pool())
            .await?,
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM artifacts WHERE attempt_id = ?")
            .bind(attempt_id.to_string())
            .fetch_one(database.pool())
            .await?,
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM events WHERE kind = 'artifact_registered' AND attempt_id = ?",
        )
        .bind(attempt_id.to_string())
        .fetch_one(database.pool())
        .await?,
    );
    assert_eq!(counts_after, counts_before);
    assert_eq!(
        database
            .artifacts()
            .get(artifact_id.parse()?)
            .await?
            .ok_or("missing artifact")?
            .state,
        igor_core::ArtifactPublicationState::Published
    );
    let detail = database.jobs().detail(job_id).await?.ok_or("missing job")?;
    assert_eq!(detail.job.state, JobState::Succeeded);
    assert_eq!(detail.attempts.len(), 1);
    supervisor.stop()?;
    Ok(())
}

#[tokio::test]
async fn metric_extractor_partial_result_keeps_artifacts_unpublished() -> TestResult {
    let _guard = worker_test_guard()?;
    let fixture = Fixture::new()?;
    configure_metric_extractor(
        &fixture,
        "#!/bin/sh\nset -eu\ncat > extractor-input.json\nprintf partial > partial.bin\nprintf '%s\n' '{\"schema_version\":1,\"status\":\"partial\",\"metrics\":{},\"artifacts\":[{\"path\":\"partial.bin\",\"role\":\"other\",\"role_detail\":\"partial\"}]}'\n",
    )?;
    let mut supervisor = spawn_metric_supervisor(&fixture)?;
    wait_for_marker(&fixture.home.join("runtime/igor/supervisor.sock"))?;
    let job_id = submit_metric_extractor_job(&fixture)?;
    assert!(
        fixture
            .run()
            .args(["wait", &job_id.to_string()])
            .output()?
            .status
            .success()
    );
    let database = Database::open(fixture.home.join("state/igor/igor.sqlite3")).await?;
    let attempt_id = database
        .jobs()
        .detail(job_id)
        .await?
        .ok_or("missing job")?
        .attempts
        .last()
        .ok_or("missing attempt")?
        .spec
        .id();
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if database
                .actions()
                .document_for_attempt(attempt_id)
                .await?
                .is_some()
            {
                return Ok::<_, Box<dyn Error>>(());
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await??;
    let id: String = sqlx::query_scalar(
        "SELECT id FROM artifacts WHERE attempt_id = ? AND path = 'partial.bin'",
    )
    .bind(attempt_id.to_string())
    .fetch_one(database.pool())
    .await?;
    let artifact = database
        .artifacts()
        .get(id.parse()?)
        .await?
        .ok_or("missing partial artifact")?;
    assert_eq!(artifact.state, igor_core::ArtifactPublicationState::Staged);
    assert_eq!(
        artifact.lifecycle,
        igor_core::ArtifactLifecycleState::Failed
    );
    assert!(artifact.diagnosis_required);
    assert!(artifact.metadata.get("published_path").is_none());
    assert!(
        !fixture
            .project
            .join("artifacts")
            .join(attempt_id.to_string())
            .exists()
    );
    assert_eq!(
        fs::read(
            fixture
                .home
                .join("state/igor/artifact-staging")
                .join(attempt_id.to_string())
                .join(&id)
        )?,
        b"partial"
    );
    supervisor.stop()?;
    Ok(())
}

#[tokio::test]
async fn supervisor_cleans_failed_artifact_only_after_diagnosis() -> TestResult {
    let _guard = worker_test_guard()?;
    let fixture = Fixture::new()?;
    configure_metric_extractor(
        &fixture,
        "#!/bin/sh\nset -eu\ncat > /dev/null\nprintf partial > partial.bin\nprintf '%s\\n' '{\"schema_version\":1,\"status\":\"partial\",\"metrics\":{},\"artifacts\":[{\"path\":\"partial.bin\",\"role\":\"metrics\"}]}'\n",
    )?;
    preserve_raw_success_logs(&fixture)?;
    let mut supervisor = spawn_metric_supervisor(&fixture)?;
    wait_for_marker(&fixture.home.join("runtime/igor/supervisor.sock"))?;
    let job_id = submit_metric_extractor_job(&fixture)?;
    assert!(
        fixture
            .run()
            .args(["wait", &job_id.to_string()])
            .output()?
            .status
            .success()
    );
    let database = Database::open(fixture.home.join("state/igor/igor.sqlite3")).await?;
    let attempt_id = database
        .jobs()
        .detail(job_id)
        .await?
        .ok_or("missing job")?
        .attempts[0]
        .spec
        .id();
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if database
                .actions()
                .document_for_attempt(attempt_id)
                .await?
                .is_some()
            {
                return Ok::<_, Box<dyn Error>>(());
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await??;
    let id: String = sqlx::query_scalar("SELECT id FROM artifacts WHERE attempt_id = ?")
        .bind(attempt_id.to_string())
        .fetch_one(database.pool())
        .await?;
    let source = fixture
        .home
        .join("state/igor/logs")
        .join(attempt_id.to_string())
        .join("partial.bin");
    let staged = fixture
        .home
        .join("state/igor/artifact-staging")
        .join(attempt_id.to_string())
        .join(&id);
    assert_eq!(fs::read(&source)?, b"partial");
    assert_eq!(fs::read(&staged)?, b"partial");
    assert_eq!(
        database
            .artifacts()
            .get(id.parse()?)
            .await?
            .ok_or("missing artifact")?
            .lifecycle,
        igor_core::ArtifactLifecycleState::Failed
    );
    let recovery = igor_core::RecoveryId::new().to_string();
    sqlx::query("INSERT INTO recoveries (id, attempt_id, state, diagnosis_json, idempotency_key) VALUES (?, ?, 'proposed', '{}', ?)")
        .bind(&recovery).bind(attempt_id.to_string()).bind(&recovery)
        .execute(database.pool()).await?;
    tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            if database
                .artifacts()
                .get(id.parse()?)
                .await?
                .ok_or("missing artifact")?
                .state
                == igor_core::ArtifactPublicationState::Removed
            {
                return Ok::<_, Box<dyn Error>>(());
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await??;
    assert!(!source.exists() && !staged.exists());
    let cleanup_id = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let row: Option<(String, String)> = sqlx::query_as("SELECT id, state FROM actions WHERE attempt_id = ? AND job_id = ? AND kind = 'cleanup_failed_artifacts'")
                .bind(attempt_id.to_string()).bind(job_id.to_string()).fetch_optional(database.pool()).await?;
            if let Some((id, state)) = row && state == "succeeded" {
                return Ok::<_, Box<dyn Error>>(id);
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }).await??;
    let tombstone = database
        .cleanup_tombstone("failed_artifact_retention", &id)
        .await?
        .ok_or("missing cleanup tombstone")?;
    assert_eq!(tombstone["action_id"], cleanup_id);
    assert_eq!(tombstone["outcome"], "removed");
    assert!(source.with_file_name("stdout.log").is_file());
    assert!(source.with_file_name("stderr.log").is_file());
    assert_eq!(
        database
            .jobs()
            .get_job(job_id)
            .await?
            .ok_or("missing job")?
            .state,
        JobState::Succeeded
    );
    supervisor.stop()?;
    Ok(())
}

#[tokio::test]
async fn metric_extractor_malformed_json_retries_without_changing_job_outcome() -> TestResult {
    let _guard = worker_test_guard()?;
    let fixture = Fixture::new()?;
    configure_metric_extractor(&fixture, "#!/bin/sh\nprintf '{not json'\n")?;
    let mut supervisor = spawn_metric_supervisor(&fixture)?;
    wait_for_marker(&fixture.home.join("runtime/igor/supervisor.sock"))?;
    let job_id = submit_metric_extractor_job(&fixture)?;
    let completed = fixture
        .run()
        .args(["wait", &job_id.to_string(), "--json"])
        .output()?;
    assert!(
        completed.status.success(),
        "{}",
        String::from_utf8_lossy(&completed.stderr)
    );
    let completed: Value = serde_json::from_slice(&completed.stdout)?;
    assert_eq!(completed["job"]["state"], "succeeded");

    let database = Database::open(fixture.home.join("state/igor/igor.sqlite3")).await?;
    let detail = database.jobs().detail(job_id).await?.ok_or("missing job")?;
    assert_eq!(detail.job.state, JobState::Succeeded);
    let attempt_id = detail.attempts.last().ok_or("missing attempt")?.spec.id();
    assert_eq!(
        database.actions().document_for_attempt(attempt_id).await?,
        None
    );
    let (state, last_error) = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let row: (String, Option<String>) = sqlx::query_as(
                "SELECT state, last_error FROM actions WHERE job_id = ? AND kind = 'extract_metrics'",
            )
            .bind(job_id.to_string())
            .fetch_one(database.pool())
            .await?;
            if row.1.as_deref() == Some("invalid_extractor_json") {
                return Ok::<_, Box<dyn Error>>(row);
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }).await??;
    assert!(
        state == "pending" || state == "failed",
        "unexpected action state {state}"
    );
    assert!(
        last_error
            .as_deref()
            .is_some_and(|error| error == "invalid_extractor_json")
    );
    assert_eq!(
        database.actions().document_for_attempt(attempt_id).await?,
        None
    );
    supervisor.stop()?;
    Ok(())
}

#[tokio::test]
async fn metric_extractor_restarts_from_an_expired_claim_without_losing_the_document() -> TestResult
{
    let _guard = worker_test_guard()?;
    let fixture = Fixture::new()?;
    configure_metric_extractor(
        &fixture,
        "#!/bin/sh\nset -eu\ncat > extractor-input.json\ntouch extractor-started\ni=0\nwhile [ ! -f extractor-release ] && [ \"$i\" -lt 100 ]; do i=$((i+1)); sleep 0.1; done\n[ -f extractor-release ]\nprintf '%s\n' '{\"schema_version\":1,\"status\":\"success\",\"metrics\":{\"score\":0.75}}'\n",
    )?;
    let mut supervisor = spawn_metric_supervisor(&fixture)?;
    wait_for_marker(&fixture.home.join("runtime/igor/supervisor.sock"))?;
    let job_id = submit_metric_extractor_job(&fixture)?;
    assert!(
        fixture
            .run()
            .args(["wait", &job_id.to_string()])
            .output()?
            .status
            .success()
    );
    let database = Database::open(fixture.home.join("state/igor/igor.sqlite3")).await?;
    let attempt_id = database
        .jobs()
        .detail(job_id)
        .await?
        .ok_or("missing extraction job")?
        .attempts
        .last()
        .ok_or("missing extraction attempt")?
        .spec
        .id();
    let attempt_dir = fixture
        .home
        .join("state/igor/logs")
        .join(attempt_id.to_string());
    wait_for_marker(&attempt_dir.join("extractor-started"))?;
    assert!(
        database
            .actions()
            .document_for_attempt(attempt_id)
            .await?
            .is_none()
    );
    supervisor.stop()?;
    let action_id: String = sqlx::query_scalar(
        "SELECT id FROM actions WHERE attempt_id = ? AND kind = 'extract_metrics'",
    )
    .bind(attempt_id.to_string())
    .fetch_one(database.pool())
    .await?;
    sqlx::query("UPDATE actions SET claim_expires_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now', '-1 second') WHERE id = ?")
        .bind(&action_id).execute(database.pool()).await?;
    fs::write(attempt_dir.join("extractor-release"), b"ready")?;
    supervisor = spawn_metric_supervisor(&fixture)?;
    wait_for_marker(&fixture.home.join("runtime/igor/supervisor.sock"))?;
    let expected: Value =
        serde_json::json!({"schema_version":1,"status":"success","metrics":{"score":0.75}});
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if database.actions().document_for_attempt(attempt_id).await? == Some(expected.clone())
            {
                return Ok::<_, Box<dyn Error>>(());
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await??;
    let (state, attempts): (String, i64) =
        sqlx::query_as("SELECT state, attempts FROM actions WHERE id = ?")
            .bind(&action_id)
            .fetch_one(database.pool())
            .await?;
    assert_eq!(state, "succeeded");
    assert_eq!(attempts, 2);
    let documents: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM metric_documents WHERE attempt_id = ?")
            .bind(attempt_id.to_string())
            .fetch_one(database.pool())
            .await?;
    assert_eq!(documents, 1);
    assert_eq!(
        database
            .jobs()
            .get_job(job_id)
            .await?
            .ok_or("missing job")?
            .state,
        JobState::Succeeded
    );
    supervisor.stop()?;
    Ok(())
}

async fn assert_metric_extractor_failure_reaches_terminal_state(
    script: &str,
    expected_error: &str,
) -> TestResult {
    let _guard = worker_test_guard()?;
    let fixture = Fixture::new()?;
    configure_metric_extractor(&fixture, script)?;
    let job_id = submit_metric_extractor_job(&fixture)?;
    let completed = fixture
        .run()
        .args(["wait", &job_id.to_string(), "--json"])
        .output()?;
    assert!(
        completed.status.success(),
        "{}",
        String::from_utf8_lossy(&completed.stderr)
    );
    let completed: Value = serde_json::from_slice(&completed.stdout)?;
    assert_eq!(completed["job"]["state"], "succeeded");

    let database = Database::open(fixture.home.join("state/igor/igor.sqlite3")).await?;
    let detail = database.jobs().detail(job_id).await?.ok_or("missing job")?;
    assert_eq!(detail.job.state, JobState::Succeeded);
    assert_eq!(detail.attempts.len(), 1);
    let attempt = detail.attempts.last().ok_or("missing attempt")?;
    assert_eq!(attempt.state, AttemptState::Succeeded);
    let attempt_id = attempt.spec.id();

    // Claim the pending action on its last allowed attempt without waiting for backoff.
    let armed = sqlx::query(
        "UPDATE actions SET attempts = 2, available_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now', '-1 second')
         WHERE attempt_id = ? AND kind = 'extract_metrics' AND state = 'pending'",
    )
    .bind(attempt_id.to_string())
    .execute(database.pool())
    .await?;
    assert_eq!(armed.rows_affected(), 1);

    let mut supervisor = spawn_metric_supervisor(&fixture)?;
    wait_for_marker(&fixture.home.join("runtime/igor/supervisor.sock"))?;
    let (state, last_error, attempts): (String, Option<String>, i64) =
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                let row: (String, Option<String>, i64) = sqlx::query_as(
                    "SELECT state, last_error, attempts FROM actions WHERE attempt_id = ? AND kind = 'extract_metrics'",
                )
                .bind(attempt_id.to_string())
                .fetch_one(database.pool())
                .await?;
                if row.0 == "failed" {
                    return Ok::<_, Box<dyn Error>>(row);
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await??;
    assert_eq!(state, "failed");
    assert_eq!(last_error.as_deref(), Some(expected_error));
    assert_eq!(attempts, 3);
    assert_eq!(
        database.actions().document_for_attempt(attempt_id).await?,
        None
    );
    let documents: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM metric_documents WHERE attempt_id = ?")
            .bind(attempt_id.to_string())
            .fetch_one(database.pool())
            .await?;
    assert_eq!(documents, 0);
    let artifacts: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM artifacts WHERE attempt_id = ?")
        .bind(attempt_id.to_string())
        .fetch_one(database.pool())
        .await?;
    assert_eq!(artifacts, 0);
    assert!(
        !fixture
            .project
            .join("artifacts")
            .join(attempt_id.to_string())
            .exists()
    );

    let detail = database.jobs().detail(job_id).await?.ok_or("missing job")?;
    assert_eq!(detail.job.state, JobState::Succeeded);
    assert_eq!(detail.attempts.len(), 1);
    assert_eq!(detail.attempts[0].state, AttemptState::Succeeded);
    let metrics = output_json(
        fixture
            .run()
            .args(["metrics", &job_id.to_string(), "--json"])
            .output()?,
    )?;
    assert_eq!(metrics["job_id"], job_id.to_string());
    assert_eq!(metrics["attempts"][0]["attempt_id"], attempt_id.to_string());
    assert_eq!(metrics["attempts"][0]["action_state"], "failed");
    assert_eq!(metrics["attempts"][0]["last_error"], expected_error);
    assert_eq!(metrics["attempts"][0]["document"], Value::Null);
    assert_eq!(metrics["attempts"][0]["metrics"], json!([]));
    supervisor.stop()?;
    Ok(())
}

#[tokio::test]
async fn metric_extractor_failure_nonzero_exit_reaches_terminal_state() -> TestResult {
    assert_metric_extractor_failure_reaches_terminal_state(
        "#!/bin/sh\ncat > /dev/null\nexit 17\n",
        "extractor_exited_nonzero",
    )
    .await
}

#[tokio::test]
async fn metric_extractor_failure_invalid_json_reaches_terminal_state() -> TestResult {
    assert_metric_extractor_failure_reaches_terminal_state(
        "#!/bin/sh\ncat > /dev/null\nprintf '{not json'\n",
        "invalid_extractor_json",
    )
    .await
}

#[tokio::test]
async fn metric_extractor_failure_artifact_registration_reaches_terminal_state() -> TestResult {
    assert_metric_extractor_failure_reaches_terminal_state(
        "#!/bin/sh\ncat > /dev/null\nprintf '%s\\n' '{\"schema_version\":1,\"status\":\"success\",\"metrics\":{\"score\":1},\"artifacts\":[{\"path\":\"missing.bin\",\"role\":\"other\",\"role_detail\":\"raw\"}]}'\n",
        "artifact_registration_failed",
    )
    .await
}

#[tokio::test]
async fn terminal_extraction_failure_tracks_staged_artifact_as_failed() -> TestResult {
    let _guard = worker_test_guard()?;
    let fixture = Fixture::new()?;
    configure_metric_extractor(
        &fixture,
        "#!/bin/sh\nset -eu\ncat > /dev/null\nprintf partial-data > partial.bin\nprintf '%s\n' '{\"schema_version\":1,\"status\":\"success\",\"metrics\":{},\"artifacts\":[{\"path\":\"partial.bin\",\"role\":\"metrics\"}]}'\n",
    )?;
    let outside = fixture._temporary.path().join("outside-artifacts");
    fs::create_dir_all(&outside)?;
    let job_id = submit_metric_extractor_job(&fixture)?;
    assert!(
        fixture
            .run()
            .args(["wait", &job_id.to_string()])
            .output()?
            .status
            .success()
    );
    std::os::unix::fs::symlink(&outside, fixture.project.join("artifacts"))?;
    let database = Database::open(fixture.home.join("state/igor/igor.sqlite3")).await?;
    let attempt_id = database
        .jobs()
        .detail(job_id)
        .await?
        .ok_or("missing job")?
        .attempts[0]
        .spec
        .id();
    let armed = sqlx::query("UPDATE actions SET attempts = 2, available_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now', '-1 second') WHERE attempt_id = ? AND kind = 'extract_metrics' AND state = 'pending'")
        .bind(attempt_id.to_string()).execute(database.pool()).await?;
    assert_eq!(armed.rows_affected(), 1);
    let mut supervisor = spawn_metric_supervisor(&fixture)?;
    wait_for_marker(&fixture.home.join("runtime/igor/supervisor.sock"))?;
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let state: String = sqlx::query_scalar(
                "SELECT state FROM actions WHERE attempt_id = ? AND kind = 'extract_metrics'",
            )
            .bind(attempt_id.to_string())
            .fetch_one(database.pool())
            .await?;
            if state == "failed" {
                return Ok::<_, Box<dyn Error>>(());
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await??;
    let (artifact_id, physical_state, lifecycle): (String, String, String) = sqlx::query_as(
        "SELECT id, state, lifecycle_state FROM artifacts WHERE attempt_id = ? AND path = 'partial.bin'",
    ).bind(attempt_id.to_string()).fetch_one(database.pool()).await?;
    assert_eq!(physical_state, "staged");
    assert_eq!(lifecycle, "failed");
    assert!(
        database
            .artifacts()
            .get(artifact_id.parse()?)
            .await?
            .ok_or("missing failed artifact")?
            .diagnosis_required
    );
    let staged = fixture
        .home
        .join("state/igor/artifact-staging")
        .join(attempt_id.to_string())
        .join(artifact_id);
    assert_eq!(fs::read(staged)?, b"partial-data");
    assert_eq!(fs::read_dir(outside)?.count(), 0);
    assert_eq!(
        database
            .jobs()
            .get_job(job_id)
            .await?
            .ok_or("missing job")?
            .state,
        JobState::Succeeded
    );
    supervisor.stop()?;
    Ok(())
}

#[tokio::test]
async fn metric_extractor_rejects_invalid_result_fixtures_without_publishing() -> TestResult {
    for (case, document, error) in [
        ("malformed", "{broken", "invalid_extractor_json"),
        (
            "missing status",
            r#"{"schema_version":1,"metrics":{}}"#,
            "invalid_extractor_schema",
        ),
        (
            "missing metrics",
            r#"{"schema_version":1,"status":"success"}"#,
            "invalid_extractor_schema",
        ),
        (
            "duplicate top-level key",
            r#"{"schema_version":1,"status":"failed","status":"success","metrics":{}}"#,
            "invalid_extractor_json",
        ),
        (
            "duplicate nested key",
            r#"{"schema_version":1,"status":"success","metrics":{"score":0.5,"score":0.75}}"#,
            "invalid_extractor_json",
        ),
        (
            "duplicate artifact property",
            r#"{"schema_version":1,"status":"success","metrics":{},"artifacts":[{"path":"bad","path":"other","role":"metrics"}]}"#,
            "invalid_extractor_json",
        ),
        (
            "nonfinite token",
            r#"{"schema_version":1,"status":"success","metrics":{"score":NaN}}"#,
            "invalid_extractor_json",
        ),
        (
            "overflowing number",
            r#"{"schema_version":1,"status":"success","metrics":{"score":1e999}}"#,
            "invalid_extractor_json",
        ),
        (
            "unsupported version",
            r#"{"schema_version":2,"status":"success","metrics":{}}"#,
            "invalid_extractor_schema",
        ),
    ] {
        let script = format!("#!/bin/sh\ncat > /dev/null\nprintf '%s\\n' '{document}'\n");
        if let Err(failure) =
            assert_metric_extractor_failure_reaches_terminal_state(&script, error).await
        {
            return Err(format!("{case}: {failure}").into());
        }
    }
    Ok(())
}

#[tokio::test]
async fn metrics_cli_shows_pending_and_published_attempt_without_losing_names() -> TestResult {
    let _guard = worker_test_guard()?;
    let fixture = Fixture::new()?;
    let document = r#"{"schema_version":1,"status":"success","metrics":{"score one":0.75,"other":-2.5},"selection":{"split":"validation","metric":"score one","value":0.75}}"#;
    configure_metric_extractor(
        &fixture,
        &format!("#!/bin/sh\ncat > /dev/null\nprintf '%s\\n' '{document}'\n"),
    )?;
    let job_id = submit_metric_extractor_job(&fixture)?;
    assert!(
        fixture
            .run()
            .args(["wait", &job_id.to_string()])
            .output()?
            .status
            .success()
    );
    let pending = output_json(
        fixture
            .run()
            .args(["metrics", &job_id.to_string(), "--json"])
            .output()?,
    )?;
    assert_eq!(pending["job_id"], job_id.to_string());
    assert_eq!(
        pending["attempts"]
            .as_array()
            .ok_or("missing attempts")?
            .len(),
        1
    );
    assert_eq!(pending["attempts"][0]["action_state"], "pending");
    assert_eq!(pending["attempts"][0]["document"], Value::Null);
    let attempt_id = pending["attempts"][0]["attempt_id"]
        .as_str()
        .ok_or("missing attempt id")?;
    let attempt_id: igor_core::AttemptId = attempt_id.parse()?;

    let mut supervisor = spawn_metric_supervisor(&fixture)?;
    wait_for_marker(&fixture.home.join("runtime/igor/supervisor.sock"))?;
    let published = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let output = output_json(
                fixture
                    .run()
                    .args(["metrics", &job_id.to_string(), "--json"])
                    .output()?,
            )?;
            if output["attempts"][0]["action_state"] == "succeeded" {
                return Ok::<_, Box<dyn Error>>(output);
            }
            tokio::time::sleep(Duration::from_millis(30)).await;
        }
    })
    .await??;
    assert_eq!(
        published["attempts"][0]["attempt_id"],
        attempt_id.to_string()
    );
    assert_eq!(
        published["attempts"][0]["document"],
        serde_json::from_str::<Value>(document)?
    );
    assert_eq!(published["attempts"][0]["selection"]["split"], "validation");
    assert_eq!(
        published["attempts"][0]["metrics"],
        json!([["other", -2.5], ["score one", 0.75]])
    );
    let human = fixture
        .run()
        .args(["metrics", &job_id.to_string()])
        .output()?;
    assert!(human.status.success());
    assert!(String::from_utf8(human.stdout)?.contains("score one: 0.75"));
    supervisor.stop()?;
    Ok(())
}

#[tokio::test]
async fn metrics_cli_distinguishes_unconfigured_job_and_missing_job() -> TestResult {
    let _guard = worker_test_guard()?;
    let fixture = Fixture::new()?;
    let job_id = submit_metric_extractor_job(&fixture)?;
    assert!(
        fixture
            .run()
            .args(["wait", &job_id.to_string()])
            .output()?
            .status
            .success()
    );
    let result = output_json(
        fixture
            .run()
            .args(["metrics", &job_id.to_string(), "--json"])
            .output()?,
    )?;
    assert_eq!(result["attempts"][0]["action_state"], Value::Null);
    assert_eq!(result["attempts"][0]["document"], Value::Null);
    let missing = fixture
        .run()
        .args(["metrics", &JobId::new().to_string(), "--json"])
        .output()?;
    assert_eq!(missing.status.code(), Some(8));
    Ok(())
}

#[tokio::test]
async fn metrics_cli_reads_large_document_beyond_request_frame_limit() -> TestResult {
    let _guard = worker_test_guard()?;
    let fixture = Fixture::new()?;
    let mut metrics = serde_json::Map::new();
    for index in 0..2_000 {
        metrics.insert(format!("{index:04}-{}", "x".repeat(250)), json!(0.5));
    }
    let document = serde_json::to_string(&json!({
        "schema_version": 1,
        "status": "success",
        "metrics": metrics,
    }))?;
    assert!(document.len() < 1024 * 1024);
    configure_metric_extractor(
        &fixture,
        &format!("#!/bin/sh\ncat > /dev/null\nprintf '%s\\n' '{document}'\n"),
    )?;
    let mut supervisor = spawn_metric_supervisor(&fixture)?;
    wait_for_marker(&fixture.home.join("runtime/igor/supervisor.sock"))?;
    let job_id = submit_metric_extractor_job(&fixture)?;
    assert!(
        fixture
            .run()
            .args(["wait", &job_id.to_string()])
            .output()?
            .status
            .success()
    );
    let result = tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            let output = output_json(
                fixture
                    .run()
                    .args(["metrics", &job_id.to_string(), "--json"])
                    .output()?,
            )?;
            if output["attempts"][0]["action_state"] == "succeeded" {
                return Ok::<_, Box<dyn Error>>(output);
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await??;
    let encoded = serde_json::to_vec(&result)?;
    assert!(encoded.len() > 1024 * 1024);
    assert_eq!(
        result["attempts"][0]["metrics"]
            .as_array()
            .ok_or("missing indexed metrics")?
            .len(),
        2_000
    );
    supervisor.stop()?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn telegram_includes_indexed_metrics_when_extraction_finishes_before_delivery() -> TestResult
{
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpListener,
    };

    const TOKEN: &str = "123456:abcdefghijklmnopqrstuvwxyzABCDEFG";
    let _guard = worker_test_guard()?;
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let config = format!(
        "schema_version = 1\n[telegram]\nbot_token = '{TOKEN}'\nchat_id = '12345'\napi_base = 'http://{}'\n",
        listener.local_addr()?
    );
    let fixture = Fixture::with_global_config(Some(&config))?;
    configure_metric_extractor(
        &fixture,
        "#!/bin/sh\nset -eu\ncat > /dev/null\nprintf '%s\n' '{\"schema_version\":1,\"status\":\"success\",\"metrics\":{\"alpha\":0.7,\"f1 score\":0.9}}'\n",
    )?;
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await?;
        let mut received = Vec::new();
        let mut chunk = [0_u8; 1024];
        let boundary = loop {
            let count = stream.read(&mut chunk).await?;
            if count == 0 || received.len() + count > 64 * 1024 {
                return Err(std::io::Error::other("invalid Telegram request"));
            }
            received.extend_from_slice(&chunk[..count]);
            if let Some(offset) = received.windows(4).position(|part| part == b"\r\n\r\n") {
                break offset + 4;
            }
        };
        let headers = String::from_utf8_lossy(&received[..boundary]);
        if !headers.contains("/sendMessage") {
            return Err(std::io::Error::other("unexpected Telegram endpoint"));
        }
        let content_length = headers
            .lines()
            .find_map(|line| {
                line.split_once(':').and_then(|(name, value)| {
                    name.eq_ignore_ascii_case("content-length")
                        .then(|| value.trim().parse::<usize>().ok())
                        .flatten()
                })
            })
            .ok_or_else(|| std::io::Error::other("missing Telegram Content-Length"))?;
        if content_length > 64 * 1024 {
            return Err(std::io::Error::other("oversized Telegram request"));
        }
        while received.len() - boundary < content_length {
            let count = stream.read(&mut chunk).await?;
            if count == 0 {
                return Err(std::io::Error::other("truncated Telegram body"));
            }
            received.extend_from_slice(&chunk[..count]);
        }
        let payload: Value = serde_json::from_slice(&received[boundary..boundary + content_length])
            .map_err(std::io::Error::other)?;
        stream.write_all(b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 11\r\nConnection: close\r\n\r\n{\"ok\":true}").await?;
        Ok::<_, std::io::Error>(payload)
    });

    let job_id = submit_metric_extractor_job(&fixture)?;
    let waited = fixture.run().args(["wait", &job_id.to_string()]).output()?;
    assert!(
        waited.status.success(),
        "{}",
        String::from_utf8_lossy(&waited.stderr)
    );
    let database = Database::open(fixture.home.join("state/igor/igor.sqlite3")).await?;
    let detail = database.jobs().detail(job_id).await?.ok_or("missing job")?;
    let attempt_id = detail.attempts[0].spec.id();
    let held = sqlx::query(
        "UPDATE actions SET available_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now', '+120 seconds')
         WHERE job_id = ? AND kind = 'send_notification' AND state = 'pending'",
    )
    .bind(job_id.to_string())
    .execute(database.pool())
    .await?;
    assert_eq!(held.rows_affected(), 1);
    let mut supervisor = spawn_metric_supervisor(&fixture)?;
    wait_for_marker(&fixture.home.join("runtime/igor/supervisor.sock"))?;
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if database
                .actions()
                .document_for_attempt(attempt_id)
                .await?
                .is_some()
            {
                return Ok::<_, Box<dyn Error>>(());
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await??;
    assert_eq!(
        database.deliveries().available_metrics(attempt_id).await?,
        [("alpha".into(), 0.7), ("f1 score".into(), 0.9)]
    );
    let released = sqlx::query(
        "UPDATE actions SET available_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now', '-1 second')
         WHERE job_id = ? AND kind = 'send_notification' AND state = 'pending'",
    )
    .bind(job_id.to_string())
    .execute(database.pool())
    .await?;
    assert_eq!(released.rows_affected(), 1);
    let payload = tokio::time::timeout(Duration::from_secs(10), server).await???;
    assert_eq!(payload["chat_id"], "12345");
    let text = payload["text"].as_str().ok_or("missing Telegram message")?;
    assert!(text.contains("alpha: 0.7"), "{text}");
    assert!(text.contains("f1 score: 0.9"), "{text}");
    assert!(text.contains(&job_id.to_string()), "{text}");
    assert!(
        text.find("f1 score").ok_or("missing metric")?
            < text.find("Job:").ok_or("missing job identity")?
    );
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let state: Option<String> = sqlx::query_scalar("SELECT state FROM deliveries WHERE idempotency_key IN (SELECT 'telegram:action:' || id FROM actions WHERE job_id = ? AND kind = 'send_notification')")
                .bind(job_id.to_string()).fetch_optional(database.pool()).await?;
            if state.as_deref() == Some("delivered") {
                return Ok::<_, Box<dyn Error>>(());
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }).await??;
    assert_eq!(
        database
            .jobs()
            .get_job(job_id)
            .await?
            .ok_or("missing job")?
            .state,
        JobState::Succeeded
    );
    supervisor.stop()?;
    Ok(())
}

fn configure_metric_fixture(fixture: &Fixture, name: &str) -> TestResult {
    let source = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/metrics")
        .join(name);
    let executable = fixture.project.join("fixture-extractor");
    if name == "native.rs" {
        let compiled = Command::new("rustc")
            .args(["--edition=2024", "-o"])
            .arg(&executable)
            .arg(&source)
            .output()?;
        assert!(
            compiled.status.success(),
            "native fixture failed to compile: {}",
            String::from_utf8_lossy(&compiled.stderr)
        );
    } else {
        fs::copy(&source, &executable)?;
    }
    fs::set_permissions(&executable, fs::Permissions::from_mode(0o700))?;
    fs::write(
        fixture.project.join(".igor/project.toml"),
        "schema_version = 1\n[results]\nextractor = ['./fixture-extractor']\nworking_directory = 'attempt'\n",
    )?;
    git(
        &fixture.project,
        &["add", "fixture-extractor", ".igor/project.toml"],
    )?;
    git(
        &fixture.project,
        &["commit", "-qm", "add extractor fixture"],
    )?;
    Ok(())
}

async fn run_metric_fixture(
    name: &str,
    expected_metrics: Value,
    expected_selection: Value,
    artifact_path: &str,
    artifact_contents: &str,
) -> TestResult {
    let _guard = worker_test_guard()?;
    let fixture = Fixture::new()?;
    configure_metric_fixture(&fixture, name)?;
    let mut supervisor = spawn_metric_supervisor(&fixture)?;
    wait_for_marker(&fixture.home.join("runtime/igor/supervisor.sock"))?;
    let job_id = submit_metric_extractor_job(&fixture)?;
    let waited = fixture.run().args(["wait", &job_id.to_string()]).output()?;
    assert!(
        waited.status.success(),
        "{}",
        String::from_utf8_lossy(&waited.stderr)
    );

    let database = Database::open(fixture.home.join("state/igor/igor.sqlite3")).await?;
    let detail = database.jobs().detail(job_id).await?.ok_or("missing job")?;
    assert_eq!(detail.job.state, JobState::Succeeded);
    assert_eq!(detail.attempts.len(), 1);
    assert_eq!(detail.attempts[0].state, AttemptState::Succeeded);
    let attempt_id = detail.attempts[0].spec.id();
    let reported = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let result = output_json(
                fixture
                    .run()
                    .args(["metrics", &job_id.to_string(), "--json"])
                    .output()?,
            )?;
            if result["attempts"][0]["action_state"] == "succeeded" {
                return Ok::<_, Box<dyn Error>>(result);
            }
            tokio::time::sleep(Duration::from_millis(30)).await;
        }
    })
    .await??;
    let reported = &reported["attempts"][0];
    assert_eq!(reported["attempt_id"], attempt_id.to_string());
    assert_eq!(reported["last_error"], Value::Null);
    assert_eq!(reported["document"]["status"], "success");
    assert_eq!(reported["document"]["metrics"], expected_metrics);
    assert_eq!(reported["selection"], expected_selection);
    assert_eq!(
        reported["document"]["artifacts"],
        json!([{
            "path": artifact_path,
            "role": "metrics"
        }])
    );
    let mut indexed = expected_metrics
        .as_object()
        .ok_or("expected scalar metrics")?
        .iter()
        .map(|(key, value)| Ok(json!([key, value.as_f64().ok_or("non-numeric metric")?])))
        .collect::<Result<Vec<Value>, Box<dyn Error>>>()?;
    indexed.sort_by(|a, b| a[0].as_str().cmp(&b[0].as_str()));
    assert_eq!(reported["metrics"], json!(indexed));

    let artifact_id: String =
        sqlx::query_scalar("SELECT id FROM artifacts WHERE attempt_id = ? AND path = ?")
            .bind(attempt_id.to_string())
            .bind(artifact_path)
            .fetch_one(database.pool())
            .await?;
    let artifact = database
        .artifacts()
        .get(artifact_id.parse()?)
        .await?
        .ok_or("missing artifact")?;
    assert_eq!(
        artifact.state,
        igor_core::ArtifactPublicationState::Published
    );
    let source = fixture
        .home
        .join("state/igor/logs")
        .join(attempt_id.to_string())
        .join(artifact_path);
    let published = fixture
        .project
        .join("artifacts")
        .join(attempt_id.to_string())
        .join(artifact_id);
    let bytes = fs::read(&published)?;
    assert_eq!(fs::read(source)?, bytes);
    let contents = String::from_utf8(bytes.clone())?;
    assert_eq!(contents, artifact_contents);
    assert_eq!(artifact.size_bytes, Some(bytes.len() as u64));
    assert_eq!(
        artifact.sha256,
        Some(format!("sha256:{:x}", sha2::Sha256::digest(&bytes)))
    );
    supervisor.stop()?;
    Ok(())
}

#[tokio::test]
async fn python_fixture_extracts_metrics_from_attempt_log() -> TestResult {
    run_metric_fixture(
        "python.py",
        json!({"python bytes": 15, "python score": 0.625}),
        json!({"split": "validation", "metric": "python score", "value": 0.625}),
        "python-summary.txt",
        "training-output",
    )
    .await
}

#[tokio::test]
async fn julia_compatible_shell_fixture_publishes_json_and_artifact() -> TestResult {
    run_metric_fixture(
        "julia-output.sh",
        json!({"Julia score": 0.875, "loss_final": 0.125}),
        json!({"split": "validation", "metric": "Julia score", "value": 0.875}),
        "julia-metrics.txt",
        "julia score: 0.875\n",
    )
    .await
}

#[tokio::test]
async fn native_binary_fixture_publishes_json_and_artifact() -> TestResult {
    run_metric_fixture(
        "native.rs",
        json!({"native bytes": 15, "native score": 0.9375}),
        Value::Null,
        "native-metrics.txt",
        "stdout bytes: 15\n",
    )
    .await
}

fn process_fixture(name: &str) -> std::path::PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/process")
        .join(name)
}

#[test]
fn docker_fake_fixture_script_is_executable() {
    // Keep the fake command contract close to the CLI integration fixtures.
    // The end-to-end Docker case is exercised by the daemon's worker tests.
    assert!(Path::new(env!("CARGO_MANIFEST_DIR")).exists());
}

fn wait_for_marker(marker: &Path) -> TestResult {
    let deadline = Instant::now() + Duration::from_secs(10);
    while !marker.exists() {
        if Instant::now() >= deadline {
            return Err(format!("marker did not appear: {}", marker.display()).into());
        }
        thread::sleep(Duration::from_millis(20));
    }
    Ok(())
}

fn submit_process(fixture: &Fixture, name: &str) -> Result<JobId, Box<dyn Error>> {
    let script = process_fixture(name);
    let submitted = output_json(
        fixture
            .run()
            .args([
                "submit",
                "--json",
                "--",
                "/bin/sh",
                script.to_str().ok_or("non-UTF-8 process fixture")?,
            ])
            .output()?,
    )?;
    Ok(submitted["spec"]["id"]
        .as_str()
        .ok_or("missing submitted job id")?
        .parse()?)
}

async fn wait_for_process(
    database: &Database,
    job_id: JobId,
) -> Result<ProcessRecord, Box<dyn Error>> {
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let detail = database
                .jobs()
                .detail(job_id)
                .await?
                .ok_or("missing submitted job")?;
            let attempt = detail.attempts.last().ok_or("missing submitted attempt")?;
            if let Some(process) = database
                .jobs()
                .process_for_attempt(attempt.spec.id())
                .await?
            {
                return Ok::<_, Box<dyn Error>>(process);
            }
            if detail.job.state.is_terminal() {
                let payload: Option<String> = sqlx::query_scalar(
                    "SELECT payload_json FROM events WHERE job_id = ? ORDER BY occurred_at DESC, id DESC LIMIT 1",
                )
                .bind(job_id.to_string())
                .fetch_optional(database.pool())
                .await?;
                return Err(format!(
                    "job became {} before its process identity was persisted (attempt {}, event {})",
                    detail.job.state.as_str(),
                    attempt.state.as_str(),
                    payload.as_deref().unwrap_or("missing")
                )
                .into());
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .map_err(|_| "process did not start")?
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shared_jobs_overlap_up_to_the_host_concurrency_limit() -> TestResult {
    let _guard = worker_test_guard()?;
    let fixture = Fixture::with_global_config(Some(
        "schema_version = 1\n[host]\nmax_concurrent_jobs = 2\n",
    ))?;
    let barrier_root = fixture._temporary.path().join("barriers");
    let script = process_fixture("barrier.sh");
    let mut jobs = Vec::new();

    for name in ["first", "second", "third"] {
        let barrier = barrier_root.join(name);
        fs::create_dir_all(&barrier)?;
        let job_file = fixture._temporary.path().join(format!("{name}.toml"));
        fs::write(
            &job_file,
            format!(
                "schema_version = 1\nname = '{name}'\n[execution]\nprogram = '/bin/sh'\nargs = ['{}', '{}']\n[resources]\nmode = 'shared'\n",
                script.display(),
                barrier.display()
            ),
        )?;
        let submitted = output_json(
            fixture
                .run()
                .args([
                    "submit",
                    "--json",
                    "--file",
                    job_file.to_str().ok_or("non-UTF-8 job file")?,
                ])
                .output()?,
        )?;
        jobs.push(
            submitted["spec"]["id"]
                .as_str()
                .ok_or("missing submitted job id")?
                .parse::<JobId>()?,
        );
    }

    wait_for_marker(&barrier_root.join("first/started"))?;
    wait_for_marker(&barrier_root.join("second/started"))?;
    let database = Database::open(fixture.home.join("state/igor/igor.sqlite3")).await?;
    let first_process = wait_for_process(&database, jobs[0]).await?;
    let second_process = wait_for_process(&database, jobs[1]).await?;
    assert!(
        kill(
            Pid::from_raw(i32::try_from(first_process.pid)?),
            None::<Signal>
        )
        .is_ok()
    );
    assert!(
        kill(
            Pid::from_raw(i32::try_from(second_process.pid)?),
            None::<Signal>
        )
        .is_ok()
    );
    assert_eq!(
        database
            .jobs()
            .get_job(jobs[2])
            .await?
            .ok_or("missing third job")?
            .state,
        JobState::Queued
    );
    assert!(!barrier_root.join("third/started").exists());

    fs::write(barrier_root.join("first/release"), b"")?;
    wait_for_marker(&barrier_root.join("third/started"))?;
    assert!(
        kill(
            Pid::from_raw(i32::try_from(second_process.pid)?),
            None::<Signal>
        )
        .is_ok()
    );

    fs::write(barrier_root.join("second/release"), b"")?;
    fs::write(barrier_root.join("third/release"), b"")?;
    for job in jobs {
        let waited = output_json(
            fixture
                .run()
                .args(["wait", &job.to_string(), "--json"])
                .output()?,
        )?;
        assert_eq!(waited["job"]["state"], "succeeded");
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancelling_one_shared_job_preserves_the_other_execution() -> TestResult {
    let _guard = worker_test_guard()?;
    let fixture = Fixture::with_global_config(Some(
        "schema_version = 1\n[host]\nmax_concurrent_jobs = 2\n",
    ))?;
    let database = Database::open(fixture.home.join("state/igor/igor.sqlite3")).await?;
    let barrier_root = fixture._temporary.path().join("cancellation-barriers");
    let script = process_fixture("barrier.sh");
    let mut jobs = Vec::new();

    for name in ["first", "second"] {
        let barrier = barrier_root.join(name);
        fs::create_dir_all(&barrier)?;
        let job_file = fixture
            ._temporary
            .path()
            .join(format!("cancellation-{name}.toml"));
        fs::write(
            &job_file,
            format!(
                "schema_version = 1\nname = 'cancellation-{name}'\n[execution]\nprogram = '/bin/sh'\nargs = ['{}', '{}']\n[resources]\nmode = 'shared'\n",
                script.display(),
                barrier.display()
            ),
        )?;
        let submitted = output_json(
            fixture
                .run()
                .args([
                    "submit",
                    "--json",
                    "--file",
                    job_file.to_str().ok_or("non-UTF-8 job file")?,
                ])
                .output()?,
        )?;
        jobs.push(
            submitted["spec"]["id"]
                .as_str()
                .ok_or("missing submitted job id")?
                .parse::<JobId>()?,
        );
    }

    wait_for_marker(&barrier_root.join("first/started"))?;
    wait_for_marker(&barrier_root.join("second/started"))?;
    let first_process = wait_for_process(&database, jobs[0]).await?;
    let second_process = wait_for_process(&database, jobs[1]).await?;
    for process in [&first_process, &second_process] {
        assert!(kill(Pid::from_raw(i32::try_from(process.pid)?), None::<Signal>).is_ok());
    }
    let second_leases: Vec<(String, String, String, i64)> = sqlx::query_as(
        "SELECT id, resource_id, owner, quantity FROM resource_leases
         WHERE job_id = ? ORDER BY id",
    )
    .bind(jobs[1].to_string())
    .fetch_all(database.pool())
    .await?;
    assert!(!second_leases.is_empty());

    let cancelled = fixture
        .run()
        .args(["cancel", &jobs[0].to_string(), "--grace-seconds", "0"])
        .output()?;
    assert!(cancelled.status.success());
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let state = database
                .jobs()
                .get_job(jobs[0])
                .await?
                .ok_or("missing cancelled job")?
                .state;
            let process_group_gone = killpg(
                Pid::from_raw(i32::try_from(first_process.process_group_id)?),
                None::<Signal>,
            )
            .is_err();
            if state == JobState::Cancelled && process_group_gone {
                return Ok::<_, Box<dyn Error>>(());
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .map_err(|_| "cancelled process group did not exit")??;

    assert_eq!(
        database
            .jobs()
            .get_job(jobs[1])
            .await?
            .ok_or("missing second job")?
            .state,
        JobState::Running
    );
    assert!(
        kill(
            Pid::from_raw(i32::try_from(second_process.pid)?),
            None::<Signal>
        )
        .is_ok()
    );
    let current_second_leases: Vec<(String, String, String, i64)> = sqlx::query_as(
        "SELECT id, resource_id, owner, quantity FROM resource_leases
         WHERE job_id = ? ORDER BY id",
    )
    .bind(jobs[1].to_string())
    .fetch_all(database.pool())
    .await?;
    assert_eq!(current_second_leases, second_leases);

    fs::write(barrier_root.join("second/release"), b"")?;
    let waited = output_json(
        fixture
            .run()
            .args(["wait", &jobs[1].to_string(), "--json"])
            .output()?,
    )?;
    assert_eq!(waited["job"]["state"], "succeeded");
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn exclusive_host_job_blocks_a_later_shared_job() -> TestResult {
    let _guard = worker_test_guard()?;
    let fixture = Fixture::with_global_config(Some(
        "schema_version = 1\n[host]\nmax_concurrent_jobs = 2\n",
    ))?;
    let barrier_root = fixture._temporary.path().join("exclusive-barriers");
    let script = process_fixture("barrier.sh");
    let mut jobs = Vec::new();

    for (name, mode) in [("exclusive", "exclusive-host"), ("shared", "shared")] {
        let barrier = barrier_root.join(name);
        fs::create_dir_all(&barrier)?;
        let job_file = fixture._temporary.path().join(format!("{name}.toml"));
        fs::write(
            &job_file,
            format!(
                "schema_version = 1\nname = '{name}'\n[execution]\nprogram = '/bin/sh'\nargs = ['{}', '{}']\n[resources]\nmode = '{mode}'\n",
                script.display(),
                barrier.display()
            ),
        )?;
        let submitted = output_json(
            fixture
                .run()
                .args([
                    "submit",
                    "--json",
                    "--file",
                    job_file.to_str().ok_or("non-UTF-8 job file")?,
                ])
                .output()?,
        )?;
        jobs.push(
            submitted["spec"]["id"]
                .as_str()
                .ok_or("missing submitted job id")?
                .parse::<JobId>()?,
        );
        if name == "exclusive" {
            wait_for_marker(&barrier.join("started"))?;
        }
    }

    let database = Database::open(fixture.home.join("state/igor/igor.sqlite3")).await?;
    let exclusive_pid = fs::read_to_string(barrier_root.join("exclusive/pid"))?
        .trim()
        .parse::<i32>()?;
    for _ in 0..10 {
        assert_eq!(
            database
                .jobs()
                .get_job(jobs[0])
                .await?
                .ok_or("missing exclusive job")?
                .state,
            JobState::Running
        );
        assert_eq!(
            database
                .jobs()
                .get_job(jobs[1])
                .await?
                .ok_or("missing shared job")?
                .state,
            JobState::Queued
        );
        assert!(!barrier_root.join("shared/started").exists());
        assert!(kill(Pid::from_raw(exclusive_pid), None::<Signal>).is_ok());
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    fs::write(barrier_root.join("exclusive/release"), b"")?;
    wait_for_marker(&barrier_root.join("shared/started"))?;
    fs::write(barrier_root.join("shared/release"), b"")?;
    for job in jobs {
        let waited = output_json(
            fixture
                .run()
                .args(["wait", &job.to_string(), "--json"])
                .output()?,
        )?;
        assert_eq!(waited["job"]["state"], "succeeded");
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn distinct_exclusive_resources_allow_shared_jobs_to_overlap() -> TestResult {
    let _guard = worker_test_guard()?;
    let fixture = Fixture::with_global_config(Some(
        "schema_version = 1\n[host]\nmax_concurrent_jobs = 2\ndiscover_gpus = false\ngpus = ['GPU-one', 'GPU-two']\nnamed_resources = ['scratch-one', 'scratch-two']\n",
    ))?;
    let barrier_root = fixture._temporary.path().join("resource-barriers");
    let script = process_fixture("barrier.sh");
    let mut jobs = Vec::new();

    for (name, named_resource) in [("first", "scratch-one"), ("second", "scratch-two")] {
        let barrier = barrier_root.join(name);
        fs::create_dir_all(&barrier)?;
        let job_file = fixture
            ._temporary
            .path()
            .join(format!("resource-{name}.toml"));
        fs::write(
            &job_file,
            format!(
                "schema_version = 1\nname = '{name}'\n[execution]\nprogram = '/bin/sh'\nargs = ['{}', '{}']\n[resources]\nmode = 'shared'\ngpu = {{ selection = 'any' }}\ngpu_count = 1\ngpu_exclusive = true\nnamed = [{{ name = '{named_resource}', mode = 'exclusive' }}]\n",
                script.display(),
                barrier.display()
            ),
        )?;
        let submitted = output_json(
            fixture
                .run()
                .args([
                    "submit",
                    "--json",
                    "--file",
                    job_file.to_str().ok_or("non-UTF-8 job file")?,
                ])
                .output()?,
        )?;
        jobs.push(
            submitted["spec"]["id"]
                .as_str()
                .ok_or("missing submitted job id")?
                .parse::<JobId>()?,
        );
    }

    wait_for_marker(&barrier_root.join("first/started"))?;
    wait_for_marker(&barrier_root.join("second/started"))?;
    let first_pid = fs::read_to_string(barrier_root.join("first/pid"))?
        .trim()
        .parse::<i32>()?;
    let second_pid = fs::read_to_string(barrier_root.join("second/pid"))?
        .trim()
        .parse::<i32>()?;
    assert!(kill(Pid::from_raw(first_pid), None::<Signal>).is_ok());
    assert!(kill(Pid::from_raw(second_pid), None::<Signal>).is_ok());
    let first_gpu = fs::read_to_string(barrier_root.join("first/cuda-visible-devices"))?;
    let second_gpu = fs::read_to_string(barrier_root.join("second/cuda-visible-devices"))?;
    let first_gpu = first_gpu.trim();
    let second_gpu = second_gpu.trim();
    assert_ne!(first_gpu, second_gpu);
    assert!(["GPU-one", "GPU-two"].contains(&first_gpu));
    assert!(["GPU-one", "GPU-two"].contains(&second_gpu));

    fs::write(barrier_root.join("first/release"), b"")?;
    fs::write(barrier_root.join("second/release"), b"")?;
    for job in jobs {
        let waited = output_json(
            fixture
                .run()
                .args(["wait", &job.to_string(), "--json"])
                .output()?,
        )?;
        assert_eq!(waited["job"]["state"], "succeeded");
    }
    Ok(())
}

#[test]
fn project_remove_deregisters_without_deleting_files_and_can_be_readded() -> TestResult {
    let _guard = worker_test_guard()?;
    let fixture = Fixture::new()?;
    let removed = output_json(
        fixture
            .run()
            .args(["project", "remove", ".", "--json"])
            .output()?,
    )?;
    assert_eq!(
        removed["root"],
        fixture.project.to_str().ok_or("non-UTF-8 project")?
    );
    assert!(fixture.project.join(".igor/project.toml").is_file());
    let projects = output_json(fixture.run().args(["project", "list", "--json"]).output()?)?;
    assert_eq!(projects, json!([]));

    let repeated = fixture.run().args(["project", "remove", "."]).output()?;
    assert_eq!(repeated.status.code(), Some(8));
    let added = fixture
        .run()
        .args(["project", "add", ".", "--json"])
        .output()?;
    assert!(added.status.success());
    let projects = output_json(fixture.run().args(["project", "list", "--json"]).output()?)?;
    assert_eq!(projects.as_array().map(Vec::len), Some(1));
    Ok(())
}

#[tokio::test]
async fn fake_docker_jobs_persist_logs_outcomes_and_cleanup() -> TestResult {
    run_fake_docker_case(0, false, None).await?;
    run_fake_docker_case(7, false, Some("docker_application_nonzero:")).await?;
    run_fake_docker_case(137, true, Some("docker_oom_killed:")).await?;
    Ok(())
}

#[tokio::test]
async fn fake_docker_start_failure_is_terminal_and_releases_resources() -> TestResult {
    let _guard = worker_test_guard()?;
    let mut fixture = Fixture::new()?;
    let invocation_log = fixture.restart_with_fake_docker(0, false, Some("start"))?;
    let (job_file, _) = write_docker_job(&fixture)?;
    let submitted = output_json(
        fixture
            .run()
            .args([
                "submit",
                "--file",
                job_file.to_str().ok_or("non-UTF-8 Docker job file")?,
                "--json",
            ])
            .output()?,
    )?;
    let job_id: JobId = submitted["spec"]["id"]
        .as_str()
        .ok_or("missing Docker job ID")?
        .parse()?;
    let waited = fixture
        .run()
        .args(["wait", &job_id.to_string(), "--json"])
        .output()?;
    assert!(!waited.status.success());
    let waited: Value = serde_json::from_slice(&waited.stdout)?;
    let attempt_id: AttemptId = waited["attempts"][0]["spec"]["id"]
        .as_str()
        .ok_or("missing Docker attempt ID")?
        .parse()?;
    let database = Database::open(fixture.home.join("state/igor/igor.sqlite3")).await?;
    let container = database
        .jobs()
        .container_for_attempt(attempt_id)
        .await?
        .ok_or("missing failed Docker container")?;
    assert_eq!(container.state, DockerContainerState::Removed);
    assert!(
        container
            .error
            .as_deref()
            .is_some_and(|error| error.starts_with("docker_start_failed:"))
    );
    let leases: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM resource_leases WHERE job_id = ?")
        .bind(job_id.to_string())
        .fetch_one(database.pool())
        .await?;
    assert_eq!(leases, 0);
    let invocations = fs::read_to_string(invocation_log)?;
    assert!(invocations.contains("start fake-container"));
    assert!(invocations.contains("rm --force fake-container"));
    assert!(!invocations.contains("logs --follow"));
    Ok(())
}

#[tokio::test]
async fn fake_docker_identity_conflict_removes_unowned_container_and_records_original_error()
-> TestResult {
    let _guard = worker_test_guard()?;
    let mut fixture = Fixture::new()?;
    let invocation_log = fixture.restart_with_fake_docker(0, false, None)?;
    let (job_file, _) = write_docker_job(&fixture)?;
    let mut job_ids = Vec::new();
    for expected_success in [true, false] {
        let submitted = output_json(
            fixture
                .run()
                .args([
                    "submit",
                    "--file",
                    job_file.to_str().ok_or("non-UTF-8 Docker job file")?,
                    "--json",
                ])
                .output()?,
        )?;
        let job_id: JobId = submitted["spec"]["id"]
            .as_str()
            .ok_or("missing Docker job ID")?
            .parse()?;
        let waited = fixture
            .run()
            .args(["wait", &job_id.to_string(), "--json"])
            .output()?;
        assert_eq!(waited.status.success(), expected_success);
        job_ids.push(job_id);
    }

    let database = Database::open(fixture.home.join("state/igor/igor.sqlite3")).await?;
    let payload: String = sqlx::query_scalar(
        "SELECT payload_json FROM events
         WHERE job_id = ? AND kind = 'attempt_state_changed'
         ORDER BY sequence DESC LIMIT 1",
    )
    .bind(job_ids[1].to_string())
    .fetch_one(database.pool())
    .await?;
    let payload: Value = serde_json::from_str(&payload)?;
    assert!(
        payload["data"]["details"]["error"]
            .as_str()
            .is_some_and(|error| error.starts_with("docker_identity_persistence_failed:"))
    );
    let leases: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM resource_leases WHERE job_id = ?")
        .bind(job_ids[1].to_string())
        .fetch_one(database.pool())
        .await?;
    assert_eq!(leases, 0);
    let invocations = fs::read_to_string(invocation_log)?;
    assert_eq!(
        invocations
            .lines()
            .filter(|line| *line == "rm --force fake-container")
            .count(),
        2
    );
    assert_eq!(
        invocations
            .lines()
            .filter(|line| line.starts_with("start fake-container"))
            .count(),
        1
    );
    Ok(())
}

#[tokio::test]
async fn fake_docker_cancellation_stops_and_finalizes_before_cleanup() -> TestResult {
    let _guard = worker_test_guard()?;
    let mut fixture = Fixture::new()?;
    let (invocation_log, _) =
        fixture.restart_with_stateful_fake_docker(143, false, None, None, None)?;
    let (job_file, _) = write_docker_job(&fixture)?;
    let job_id = submit_docker_job(&fixture, &job_file)?;
    let database = Database::open(fixture.home.join("state/igor/igor.sqlite3")).await?;
    let running = wait_for_container(&database, job_id).await?;

    let cancelled = fixture
        .run()
        .args(["cancel", &job_id.to_string(), "--grace-seconds", "2"])
        .output()?;
    assert!(
        cancelled.status.success(),
        "cancel failed: {}",
        String::from_utf8_lossy(&cancelled.stderr)
    );
    let waited = fixture
        .run()
        .args(["wait", &job_id.to_string(), "--json"])
        .output()?;
    assert!(!waited.status.success());
    let detail = database
        .jobs()
        .detail(job_id)
        .await?
        .ok_or("missing cancelled Docker job")?;
    assert_eq!(detail.job.state, JobState::Cancelled);
    assert_eq!(detail.attempts[0].state, AttemptState::Cancelled);
    let container = database
        .jobs()
        .container_for_attempt(running.attempt_id)
        .await?
        .ok_or("missing cancelled Docker container")?;
    assert_eq!(container.state, DockerContainerState::Removed);
    assert_eq!(fs::read(&container.stdout_path)?, b"fake stdout\n");
    assert_eq!(fs::read(&container.stderr_path)?, b"fake stderr\n");
    let leases: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM resource_leases WHERE job_id = ?")
        .bind(job_id.to_string())
        .fetch_one(database.pool())
        .await?;
    assert_eq!(leases, 0);

    let invocations = fs::read_to_string(invocation_log)?;
    let stop = invocations
        .find("stop --time ")
        .ok_or("missing Docker stop")?;
    let inspect = invocations[stop..]
        .find("inspect --format")
        .map(|offset| stop + offset)
        .ok_or("missing post-stop inspect")?;
    let snapshot = invocations[inspect..]
        .find("logs fake-container")
        .map(|offset| inspect + offset)
        .ok_or("missing cancellation log snapshot")?;
    let remove = invocations[snapshot..]
        .find("rm --force fake-container")
        .map(|offset| snapshot + offset)
        .ok_or("missing cancellation cleanup")?;
    assert!(stop < inspect && inspect < snapshot && snapshot < remove);
    assert!(!invocations.contains("kill fake-container"));
    Ok(())
}

#[tokio::test]
async fn fake_docker_cancellation_escalates_to_kill() -> TestResult {
    let _guard = worker_test_guard()?;
    let mut fixture = Fixture::new()?;
    let (invocation_log, _) =
        fixture.restart_with_stateful_fake_docker(137, false, Some("stop"), None, None)?;
    let (job_file, _) = write_docker_job(&fixture)?;
    let job_id = submit_docker_job(&fixture, &job_file)?;
    let database = Database::open(fixture.home.join("state/igor/igor.sqlite3")).await?;
    let running = wait_for_container(&database, job_id).await?;
    let cancelled = fixture
        .run()
        .args(["cancel", &job_id.to_string(), "--grace-seconds", "1"])
        .output()?;
    assert!(
        cancelled.status.success(),
        "cancel failed: {}",
        String::from_utf8_lossy(&cancelled.stderr)
    );
    let waited = fixture
        .run()
        .args(["wait", &job_id.to_string(), "--json"])
        .output()?;
    assert!(!waited.status.success());
    let detail = database
        .jobs()
        .detail(job_id)
        .await?
        .ok_or("missing killed Docker job")?;
    assert_eq!(detail.job.state, JobState::Cancelled);
    assert_eq!(detail.attempts[0].state, AttemptState::Cancelled);
    let container = database
        .jobs()
        .container_for_attempt(running.attempt_id)
        .await?
        .ok_or("missing killed Docker container")?;
    assert_eq!(container.state, DockerContainerState::Removed);
    assert_eq!(container.exit_code, Some(137));
    let invocations = fs::read_to_string(invocation_log)?;
    assert!(invocations.contains("stop --time "));
    assert!(invocations.contains("kill fake-container"));
    assert!(
        invocations.find("kill fake-container") < invocations.find("rm --force fake-container")
    );
    Ok(())
}

#[tokio::test]
async fn fake_docker_timeout_kills_and_persists_failure() -> TestResult {
    let _guard = worker_test_guard()?;
    let mut fixture = Fixture::new()?;
    let (invocation_log, _) =
        fixture.restart_with_stateful_fake_docker(137, false, None, None, None)?;
    let (job_file, _) = write_docker_job(&fixture)?;
    let specification = fs::read_to_string(&job_file)?.replace(
        "[resources]\nmode = 'shared'",
        "[resources]\nmode = 'shared'\ntimeout_seconds = 1",
    );
    fs::write(&job_file, specification)?;
    let job_id = submit_docker_job(&fixture, &job_file)?;
    let database = Database::open(fixture.home.join("state/igor/igor.sqlite3")).await?;
    let running = wait_for_container(&database, job_id).await?;
    let waited = fixture
        .run()
        .args(["wait", &job_id.to_string(), "--json"])
        .output()?;
    assert!(!waited.status.success());
    let detail = database
        .jobs()
        .detail(job_id)
        .await?
        .ok_or("missing timed-out Docker job")?;
    assert_eq!(detail.job.state, JobState::Failed);
    assert_eq!(detail.attempts[0].state, AttemptState::Failed);
    let container = wait_for_removed_container(&database, running.attempt_id).await?;
    assert_eq!(container.state, DockerContainerState::Removed);
    assert_eq!(container.exit_code, Some(137));
    assert_eq!(
        container.error.as_deref(),
        Some("docker_timeout: configured execution timeout elapsed")
    );
    let tails = database
        .jobs()
        .failed_log_tails(running.attempt_id)
        .await?
        .ok_or("missing failed Docker log tails")?;
    assert_eq!(tails.stdout, b"fake stdout\n");
    assert_eq!(tails.stderr, b"fake stderr\n");
    assert_eq!(tails.limit_kb, 512);
    assert!(tails.complete);
    let invocations = fs::read_to_string(invocation_log)?;
    assert!(invocations.contains("kill fake-container"));
    assert!(!invocations.contains("stop --time "));
    Ok(())
}

#[tokio::test]
async fn fake_docker_timeout_records_failure_when_kill_races_with_exit() -> TestResult {
    let _guard = worker_test_guard()?;
    let mut fixture = Fixture::new()?;
    let (invocation_log, _) = fixture.restart_with_stateful_fake_docker(
        137,
        false,
        None,
        None,
        Some("kill_nonzero_after_stop"),
    )?;
    let (job_file, _) = write_docker_job(&fixture)?;
    let specification = fs::read_to_string(&job_file)?.replace(
        "[resources]\nmode = 'shared'",
        "[resources]\nmode = 'shared'\ntimeout_seconds = 1",
    );
    fs::write(&job_file, specification)?;
    let job_id = submit_docker_job(&fixture, &job_file)?;
    let database = Database::open(fixture.home.join("state/igor/igor.sqlite3")).await?;
    let running = wait_for_container(&database, job_id).await?;
    let waited = fixture
        .run()
        .args(["wait", &job_id.to_string(), "--json"])
        .output()?;
    assert!(!waited.status.success());
    let detail = database
        .jobs()
        .detail(job_id)
        .await?
        .ok_or("missing timed-out job")?;
    assert_eq!(detail.job.state, JobState::Failed);
    assert_eq!(detail.attempts[0].state, AttemptState::Failed);
    let container = database
        .jobs()
        .container_for_attempt(running.attempt_id)
        .await?
        .ok_or("missing timed-out Docker container")?;
    assert_eq!(container.state, DockerContainerState::Removed);
    assert_eq!(
        container.error.as_deref(),
        Some("docker_timeout: configured execution timeout elapsed")
    );
    assert!(fs::read_to_string(invocation_log)?.contains("rm --force fake-container"));
    Ok(())
}

#[tokio::test]
async fn fake_docker_recovery_preserves_timeout_deadline() -> TestResult {
    let _guard = worker_test_guard()?;
    let mut fixture = Fixture::new()?;
    let (invocation_log, state_dir) =
        fixture.restart_with_stateful_fake_docker(137, false, None, None, None)?;
    let (job_file, _) = write_docker_job(&fixture)?;
    let specification = fs::read_to_string(&job_file)?.replace(
        "[resources]\nmode = 'shared'",
        "[resources]\nmode = 'shared'\ntimeout_seconds = 2",
    );
    fs::write(&job_file, specification)?;
    let job_id = submit_docker_job(&fixture, &job_file)?;
    let database = Database::open(fixture.home.join("state/igor/igor.sqlite3")).await?;
    let running = wait_for_container(&database, job_id).await?;
    tokio::time::sleep(Duration::from_millis(500)).await;
    fixture.restart_with_stateful_fake_docker(137, false, None, Some(&state_dir), None)?;

    let waited = fixture
        .run()
        .args(["wait", &job_id.to_string(), "--json"])
        .output()?;
    assert!(!waited.status.success());
    let container = database
        .jobs()
        .container_for_attempt(running.attempt_id)
        .await?
        .ok_or("missing recovered timed-out Docker container")?;
    assert_eq!(container.state, DockerContainerState::Removed);
    assert_eq!(
        container.error.as_deref(),
        Some("docker_timeout: configured execution timeout elapsed")
    );
    let invocations = fs::read_to_string(invocation_log)?;
    assert_eq!(
        invocations
            .lines()
            .filter(|line| line.starts_with("create "))
            .count(),
        1
    );
    assert_eq!(
        invocations
            .lines()
            .filter(|line| *line == "start fake-container")
            .count(),
        1
    );
    assert!(invocations.contains("kill fake-container"));
    Ok(())
}

#[tokio::test]
async fn fake_docker_restart_reattaches_without_recreating_container() -> TestResult {
    let _guard = worker_test_guard()?;
    let mut fixture = Fixture::new()?;
    let (invocation_log, state_dir) =
        fixture.restart_with_stateful_fake_docker(0, false, None, None, None)?;
    let (job_file, _) = write_docker_job(&fixture)?;
    let job_id = submit_docker_job(&fixture, &job_file)?;
    let database = Database::open(fixture.home.join("state/igor/igor.sqlite3")).await?;
    let running = wait_for_container(&database, job_id).await?;

    fixture.restart_with_stateful_fake_docker(0, false, None, Some(&state_dir), None)?;
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let invocations = fs::read_to_string(&invocation_log)?;
            if invocations
                .lines()
                .filter(|line| *line == "wait fake-container")
                .count()
                >= 2
            {
                return Ok::<_, Box<dyn Error>>(());
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .map_err(|_| "recovered Docker worker did not reattach")??;
    fs::write(state_dir.join("stopped"), b"")?;

    let waited = fixture
        .run()
        .args(["wait", &job_id.to_string(), "--json"])
        .output()?;
    assert!(waited.status.success());
    let detail = database
        .jobs()
        .detail(job_id)
        .await?
        .ok_or("missing recovered Docker job")?;
    assert_eq!(detail.job.state, JobState::Succeeded);
    assert_eq!(detail.attempts[0].state, AttemptState::Succeeded);
    let container = database
        .jobs()
        .container_for_attempt(running.attempt_id)
        .await?
        .ok_or("missing recovered Docker container")?;
    assert_eq!(container.state, DockerContainerState::Removed);
    let leases: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM resource_leases WHERE job_id = ?")
        .bind(job_id.to_string())
        .fetch_one(database.pool())
        .await?;
    assert_eq!(leases, 0);
    let terminal_events: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM events WHERE job_id = ? AND kind = 'attempt_state_changed'
         AND payload_json LIKE '%\"state\":\"succeeded\"%'",
    )
    .bind(job_id.to_string())
    .fetch_one(database.pool())
    .await?;
    assert_eq!(terminal_events, 1);

    let invocations = fs::read_to_string(invocation_log)?;
    assert_eq!(
        invocations
            .lines()
            .filter(|line| line.starts_with("create "))
            .count(),
        1
    );
    assert_eq!(
        invocations
            .lines()
            .filter(|line| *line == "start fake-container")
            .count(),
        1
    );
    assert_eq!(
        invocations
            .lines()
            .filter(|line| *line == "rm --force fake-container")
            .count(),
        1
    );
    Ok(())
}

#[tokio::test]
async fn fake_docker_recovers_every_external_sqlite_crash_boundary() -> TestResult {
    let _guard = worker_test_guard()?;
    run_docker_crash_boundary("after_create", false, 1).await?;
    run_docker_crash_boundary("before_start", false, 2).await?;
    run_docker_crash_boundary("after_start", false, 1).await?;
    run_docker_crash_boundary("after_wait", true, 1).await?;
    run_docker_crash_boundary("before_rm", true, 1).await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn opt_in_real_docker_jobs_cover_worker_lifecycle() -> TestResult {
    if std::env::var("IGOR_RUN_DOCKER_TESTS").ok().as_deref() != Some("1") {
        eprintln!("skipping real Docker integration test: IGOR_RUN_DOCKER_TESTS is not 1");
        return Ok(());
    }
    let image = match std::env::var("IGOR_TEST_DOCKER_IMAGE") {
        Ok(image) if !image.trim().is_empty() => image,
        _ => {
            eprintln!("skipping real Docker integration test: IGOR_TEST_DOCKER_IMAGE is unset");
            return Ok(());
        }
    };
    let docker = |args: &[&str]| Command::new("docker").args(args).output();
    for (args, reason) in [
        (
            &["version", "--format", "{{.Server.Version}}"] as &[&str],
            "Docker daemon is unavailable",
        ),
        (
            &["image", "inspect", &image],
            "configured Docker image is not local",
        ),
        (
            &["run", "--rm", &image, "/bin/sh", "-c", "exit 0"],
            "configured image lacks /bin/sh",
        ),
    ] {
        match docker(args) {
            Ok(output) if output.status.success() => {}
            _ => {
                eprintln!("skipping real Docker integration test: {reason}");
                return Ok(());
            }
        }
    }
    let inspected = docker(&[
        "image",
        "inspect",
        "--format",
        "{{index .RepoDigests 0}}",
        &image,
    ])?;
    let repo_digest = String::from_utf8(inspected.stdout)?.trim().to_owned();
    let Some(digest) = repo_digest
        .split_once('@')
        .map(|(_, digest)| digest)
        .filter(|digest| digest.starts_with("sha256:") && digest.len() == 71)
    else {
        eprintln!("skipping real Docker integration test: local image has no canonical RepoDigest");
        return Ok(());
    };
    if image.contains('\'') {
        return Err("IGOR_TEST_DOCKER_IMAGE cannot contain a single quote".into());
    }

    let _guard = worker_test_guard()?;
    let mut fixture = Fixture::new()?;
    fixture.restart_with_system_docker()?;
    let job_root = fixture._temporary.path().to_owned();
    let job = |name: &str, program: &str, args: &str, configured_digest: &str| {
        let path = job_root.join(format!("{name}.toml"));
        fs::write(
            &path,
            format!(
                "schema_version = 1\nname = '{name}'\n[execution]\nprogram = '{program}'\nargs = [{args}]\n[executor]\nkind = 'docker'\n[executor.settings]\nimage = '{image}'\ndigest = '{configured_digest}'\nremove_container = true\n[resources]\nmode = 'shared'\n"
            ),
        )?;
        Ok::<_, Box<dyn Error>>(path)
    };

    let success = job(
        "real-docker-success",
        "/bin/sh",
        "'-c', 'printf stdout; printf stderr >&2; test -z \"$TELEGRAM_BOT_TOKEN\" -a -z \"$OPENCODE_TEST_SECRET\"'",
        digest,
    )?;
    let success_id = submit_docker_job(&fixture, &success)?;
    let waited = fixture
        .run()
        .args(["wait", &success_id.to_string(), "--json"])
        .output()?;
    assert!(waited.status.success());
    let success: Value = serde_json::from_slice(&waited.stdout)?;
    let attempt_id: AttemptId = success["attempts"][0]["spec"]["id"]
        .as_str()
        .ok_or("missing attempt")?
        .parse()?;
    let database = Database::open(fixture.home.join("state/igor/igor.sqlite3")).await?;
    let container = database
        .jobs()
        .container_for_attempt(attempt_id)
        .await?
        .ok_or("missing container")?;
    assert_eq!(container.state, DockerContainerState::Removed);
    assert_eq!(fs::read_to_string(&container.stdout_path)?, "stdout");
    assert_eq!(fs::read_to_string(&container.stderr_path)?, "stderr");
    assert!(
        !docker(&["inspect", &container.container_id])?
            .status
            .success()
    );

    let failed = job("real-docker-failure", "/bin/sh", "'-c', 'exit 7'", digest)?;
    let failed_id = submit_docker_job(&fixture, &failed)?;
    assert!(
        !fixture
            .run()
            .args(["wait", &failed_id.to_string(), "--json"])
            .output()?
            .status
            .success()
    );

    let live = job("real-docker-live", "/bin/sh", "'-c', 'sleep 30'", digest)?;
    let live_id = submit_docker_job(&fixture, &live)?;
    let live_container = wait_for_container(&database, live_id).await?;
    fixture.restart_with_system_docker()?;
    assert!(
        fixture
            .run()
            .args(["cancel", &live_id.to_string(), "--grace-seconds", "0"])
            .output()?
            .status
            .success()
    );
    assert!(
        !fixture
            .run()
            .args(["wait", &live_id.to_string(), "--json"])
            .output()?
            .status
            .success()
    );
    assert!(
        !docker(&["inspect", &live_container.container_id])?
            .status
            .success()
    );

    let bad = job(
        "real-docker-bad-digest",
        "/bin/sh",
        "'-c', 'exit 0'",
        &format!("sha256:{}", "0".repeat(64)),
    )?;
    let bad_id = submit_docker_job(&fixture, &bad)?;
    assert!(
        !fixture
            .run()
            .args(["wait", &bad_id.to_string(), "--json"])
            .output()?
            .status
            .success()
    );
    let bad_detail = database
        .jobs()
        .detail(bad_id)
        .await?
        .ok_or("missing bad-digest job")?;
    let bad_attempt = bad_detail
        .attempts
        .last()
        .ok_or("missing bad-digest attempt")?;
    assert!(
        database
            .jobs()
            .container_for_attempt(bad_attempt.spec.id())
            .await?
            .is_none()
    );
    let bad_event: String = sqlx::query_scalar(
        "SELECT payload_json FROM events
         WHERE job_id = ? AND kind = 'attempt_state_changed'
         ORDER BY sequence DESC LIMIT 1",
    )
    .bind(bad_id.to_string())
    .fetch_one(database.pool())
    .await?;
    assert!(bad_event.contains("docker_image_missing_or_invalid"));
    Ok(())
}

#[test]
fn project_submission_and_reads_preserve_argument_contracts() -> TestResult {
    let _guard = worker_test_guard()?;
    let fixture = Fixture::new()?;
    let projects = output_json(fixture.run().args(["project", "list", "--json"]).output()?)?;
    assert_eq!(projects.as_array().map(Vec::len), Some(1));
    let repeated = fixture
        .run()
        .args([
            "project",
            "add",
            fixture.project.to_str().ok_or("non-UTF-8 project")?,
            "--json",
        ])
        .output()?;
    assert!(repeated.status.success());

    let direct = output_json(
        fixture
            .run()
            .args([
                "submit",
                "--json",
                "--name",
                "edge arguments",
                "--priority",
                "4",
                "--configuration",
                "science.json",
                "--",
                "python",
                "",
                "a b",
                "café",
                "--flag",
                "--json",
                "$(touch forbidden)",
            ])
            .output()?,
    )?;
    let job_id = direct["spec"]["id"].as_str().ok_or("missing job id")?;
    let detail = output_json(fixture.run().args(["show", job_id, "--json"]).output()?)?;
    assert_eq!(detail["job"]["spec"]["command"]["program"], "python");
    assert_eq!(
        detail["job"]["spec"]["command"]["args"],
        json!(["", "a b", "café", "--flag", "--json", "$(touch forbidden)"])
    );
    assert_eq!(detail["job"]["spec"]["command"]["shell"], "direct");
    assert_eq!(
        detail["attempts"][0]["spec"]["command"]["args"],
        detail["job"]["spec"]["command"]["args"]
    );
    assert!(!fixture.project.join("forbidden").exists());
    assert_eq!(
        detail["attempts"][0]["spec"]["configuration"]["contents"]
            .as_array()
            .map(Vec::len),
        Some(1)
    );

    let events = output_json(fixture.run().args(["events", job_id, "--json"]).output()?)?;
    assert!(events.as_array().is_some_and(|events| events.len() >= 2));
    let waited = fixture.run().args(["wait", job_id, "--json"]).output()?;
    assert!(matches!(waited.status.code(), Some(0 | 1)));
    let waited: Value = serde_json::from_slice(&waited.stdout)?;
    let human = fixture.run().args(["show", job_id]).output()?;
    assert!(human.status.success());
    let human = String::from_utf8(human.stdout)?;
    assert!(human.contains(job_id));
    assert!(
        human.contains(
            waited["job"]["state"]
                .as_str()
                .ok_or("missing terminal job state")?
        )
    );
    let listed = output_json(fixture.run().args(["list", "--json"]).output()?)?;
    assert_eq!(listed[0]["spec"]["id"], job_id);

    let shell = output_json(
        fixture
            .run()
            .args(["submit", "--shell", "printf ok | wc -c", "--json"])
            .output()?,
    )?;
    let shell_id = shell["spec"]["id"].as_str().ok_or("missing shell job id")?;
    let shell = output_json(fixture.run().args(["show", shell_id, "--json"]).output()?)?;
    assert_eq!(shell["job"]["spec"]["command"]["program"], "/bin/sh");
    assert_eq!(
        shell["job"]["spec"]["command"]["args"],
        json!(["-c", "printf ok | wc -c"])
    );
    assert_eq!(shell["job"]["spec"]["command"]["shell"], "shell");

    let rust = output_json(
        fixture
            .run()
            .args(["submit", "--json", "--", "cargo", "test", "--release"])
            .output()?,
    )?;
    let rust_id = rust["spec"]["id"].as_str().ok_or("missing Rust job id")?;
    let rust = output_json(fixture.run().args(["show", rust_id, "--json"]).output()?)?;
    assert_eq!(rust["job"]["spec"]["command"]["program"], "cargo");
    assert_eq!(
        rust["job"]["spec"]["command"]["args"],
        json!(["test", "--release"])
    );
    assert_eq!(rust["job"]["spec"]["command"]["shell"], "direct");
    Ok(())
}

#[test]
fn process_only_worker_needs_no_docker_cli_or_path() -> TestResult {
    let _guard = worker_test_guard()?;
    let fixture = Fixture::new()?;
    let submitted = output_json(
        fixture
            .run()
            .args(["submit", "--json", "--", "/bin/true"])
            .output()?,
    )?;
    let job_id = submitted["spec"]["id"]
        .as_str()
        .ok_or("missing process-only job ID")?;
    let waited = fixture.run().args(["wait", job_id, "--json"]).output()?;
    assert!(waited.status.success());
    let waited: Value = serde_json::from_slice(&waited.stdout)?;
    assert_eq!(waited["job"]["state"], "succeeded");
    Ok(())
}

#[test]
fn family_submit_queues_both_seeds_in_one_generation() -> TestResult {
    let _guard = worker_test_guard()?;
    let fixture = Fixture::new()?;
    let family_file = fixture._temporary.path().join("family.toml");
    fs::write(
        &family_file,
        "schema_version = 1\nname = 'two-seeds'\n[execution]\nprogram = '/bin/echo'\nargs = ['family']\n\
         [[members]]\nseed = 1\nargs = ['--seed', '1']\n\
         [[members]]\nseed = 2\nargs = ['--seed', '2']\n",
    )?;

    let help = fixture
        .run()
        .args(["family", "submit", "--help"])
        .output()?;
    assert!(help.status.success());
    let help = String::from_utf8(help.stdout)?;
    assert!(help.contains("--file"));

    let file = family_file.to_str().ok_or("non-UTF-8 family file")?;
    let plain = fixture
        .run()
        .args(["family", "submit", "--file", file])
        .output()?;
    assert!(
        plain.status.success(),
        "{}",
        String::from_utf8_lossy(&plain.stderr)
    );
    let plain = String::from_utf8(plain.stdout)?;
    let mut lines = plain.lines();
    let header = lines.next().ok_or("missing family submission header")?;
    assert!(header.starts_with("submitted family "));
    assert!(header.contains(" generation "));
    assert!(header.contains("(2 jobs)"));
    let mut plain_jobs = Vec::new();
    for line in lines {
        plain_jobs.push(
            line.split_whitespace()
                .next()
                .ok_or("missing job id in family output")?
                .to_owned(),
        );
    }
    assert_eq!(plain_jobs.len(), 2);

    // Atomic submission: both seeds are queued together or not at all.
    let listed = output_json(fixture.run().args(["list", "--json"]).output()?)?;
    let listed = listed.as_array().ok_or("job list is not an array")?;
    assert_eq!(listed.len(), 2);
    for job_id in &plain_jobs {
        assert!(
            listed
                .iter()
                .any(|job| job["spec"]["id"] == job_id.as_str())
        );
    }

    // Stable JSON contract: family_id, generation_id, jobs.
    fs::write(
        &family_file,
        fs::read_to_string(&family_file)?.replace("two-seeds", "json-two-seeds"),
    )?;
    let submitted = output_json(
        fixture
            .run()
            .args(["family", "submit", "--file", file, "--json"])
            .output()?,
    )?;
    let family_id = submitted["family_id"].as_str().ok_or("missing family_id")?;
    let generation_id = submitted["generation_id"]
        .as_str()
        .ok_or("missing generation_id")?;
    assert_eq!(family_id.len(), 36);
    assert_eq!(generation_id.len(), 36);
    let jobs = submitted["jobs"].as_array().ok_or("missing jobs")?;
    assert_eq!(jobs.len(), 2);
    let submitted_job_ids: Vec<&str> = jobs
        .iter()
        .map(|job| job["spec"]["id"].as_str().ok_or("missing member job id"))
        .collect::<Result<_, _>>()?;
    let detail = output_json(
        fixture
            .run()
            .args(["family", "show", family_id, "--json"])
            .output()?,
    )?;
    assert_eq!(detail["family"]["id"], family_id);
    assert_eq!(detail["family"]["name"], "json-two-seeds");
    let generations = detail["generations"]
        .as_array()
        .ok_or("missing family generations")?;
    assert_eq!(generations.len(), 1);
    assert_eq!(generations[0]["identity"]["id"], generation_id);
    let detail_jobs = generations[0]["jobs"]
        .as_array()
        .ok_or("missing generation jobs")?;
    let detail_job_ids: Vec<&str> = detail_jobs
        .iter()
        .map(|job| {
            job["spec"]["id"]
                .as_str()
                .ok_or("missing generation member job id")
        })
        .collect::<Result<_, _>>()?;
    assert_eq!(detail_job_ids, submitted_job_ids);
    assert_eq!(
        generations[0]["status"],
        if detail_jobs.iter().all(|job| job["state"] == "succeeded") {
            "comparable"
        } else {
            "incomplete"
        }
    );
    let counts = detail["counts"]
        .as_object()
        .ok_or("missing family state counts")?;
    assert_eq!(
        counts
            .values()
            .map(|count| count.as_u64().unwrap_or(0))
            .sum::<u64>(),
        2
    );
    assert_eq!(detail["generations"][0]["counts"], detail["counts"]);
    for job in jobs {
        assert!(job["spec"]["id"].as_str().is_some());
    }

    for job_id in &plain_jobs {
        let waited = fixture.run().args(["wait", job_id, "--json"]).output()?;
        assert!(
            waited.status.success(),
            "{}",
            String::from_utf8_lossy(&waited.stderr)
        );
        let waited: Value = serde_json::from_slice(&waited.stdout)?;
        assert_eq!(waited["job"]["state"], "succeeded");
    }
    let detail = fixture
        .run()
        .args([
            "family",
            "show",
            header
                .split_whitespace()
                .nth(2)
                .ok_or("missing family id")?,
        ])
        .output()?;
    assert!(detail.status.success());
    let detail = String::from_utf8(detail.stdout)?;
    assert!(!detail.trim().is_empty());
    assert!(detail.contains("succeeded=2"), "{detail}");
    assert!(detail.contains("status comparable"), "{detail}");

    let missing = fixture
        .run()
        .args(["family", "show", "00000000-0000-4000-8000-000000000000"])
        .output()?;
    assert_eq!(missing.status.code(), Some(8));
    Ok(())
}

#[test]
fn family_supersede_replaces_terminal_generation_and_preserves_history() -> TestResult {
    let _guard = worker_test_guard()?;
    let fixture = Fixture::new()?;
    let family_file = fixture._temporary.path().join("family-supersede.toml");
    fs::write(
        &family_file,
        "schema_version = 1\nname = 'supersede-seeds'\n[execution]\nprogram = '/bin/echo'\nargs = ['family']\n\
         [[members]]\nseed = 1\nargs = ['--seed', '1']\n\
         [[members]]\nseed = 2\nargs = ['--seed', '2']\n",
    )?;
    let file = family_file.to_str().ok_or("non-UTF-8 family file")?;
    let first = output_json(
        fixture
            .run()
            .args(["family", "submit", "--file", file, "--json"])
            .output()?,
    )?;
    let family_id = first["family_id"].as_str().ok_or("missing family id")?;
    let previous_generation = first["generation_id"]
        .as_str()
        .ok_or("missing generation id")?;
    let first_jobs = first["jobs"].as_array().ok_or("missing first jobs")?;
    let old_ids: Vec<String> = first_jobs
        .iter()
        .map(|job| {
            job["spec"]["id"]
                .as_str()
                .map(str::to_owned)
                .ok_or("missing job id")
        })
        .collect::<Result<_, _>>()?;
    for job_id in &old_ids {
        let waited = fixture.run().args(["wait", job_id, "--json"]).output()?;
        assert!(
            waited.status.success(),
            "{}",
            String::from_utf8_lossy(&waited.stderr)
        );
        let waited: Value = serde_json::from_slice(&waited.stdout)?;
        assert_eq!(waited["job"]["state"], "succeeded");
    }
    let first_show = output_json(
        fixture
            .run()
            .args(["show", &old_ids[0], "--json"])
            .output()?,
    )?;
    let old_revision = first_show["attempts"][0]["spec"]["source"]["identity"]["revision"]
        .as_str()
        .ok_or("missing old attempt revision")?;
    fs::write(fixture.project.join("science.json"), b"{\"seed\":2}\n")?;
    git(&fixture.project, &["add", "science.json"])?;
    git(&fixture.project, &["commit", "-qm", "change seed input"])?;
    let superseded = output_json(
        fixture
            .run()
            .args(["family", "supersede", family_id, "--file", file, "--json"])
            .output()?,
    )?;
    assert_eq!(superseded["family_id"], family_id);
    assert_eq!(superseded["previous_generation_id"], previous_generation);
    let generation = superseded["generation_id"]
        .as_str()
        .ok_or("missing new generation")?;
    assert_ne!(generation, previous_generation);
    let new_jobs = superseded["jobs"].as_array().ok_or("missing new jobs")?;
    assert_eq!(new_jobs.len(), 2);
    let new_ids: Vec<String> = new_jobs
        .iter()
        .map(|job| {
            job["spec"]["id"]
                .as_str()
                .map(str::to_owned)
                .ok_or("missing new job id")
        })
        .collect::<Result<_, _>>()?;
    assert!(new_ids.iter().all(|id| !old_ids.contains(id)));
    assert_eq!(
        new_jobs
            .iter()
            .map(|job| job["spec"]["family"]["seed"].as_u64())
            .collect::<Vec<_>>(),
        vec![Some(1), Some(2)]
    );
    let new_revision = String::from_utf8(
        Command::new("git")
            .arg("-C")
            .arg(&fixture.project)
            .args(["rev-parse", "HEAD"])
            .output()?
            .stdout,
    )?;
    let new_revision = new_revision.trim();
    assert_ne!(new_revision, old_revision);
    let detail = output_json(
        fixture
            .run()
            .args(["family", "show", family_id, "--json"])
            .output()?,
    )?;
    assert_eq!(detail["current_generation_id"], generation);
    let generations = detail["generations"]
        .as_array()
        .ok_or("missing generations")?;
    assert_eq!(generations.len(), 2);
    let old_generation = generations
        .iter()
        .find(|item| item["identity"]["id"] == previous_generation)
        .ok_or("missing old generation")?;
    assert_eq!(old_generation["counts"]["superseded"], 2);
    assert_eq!(old_generation["identity"]["source_revision"], old_revision);
    for old_id in &old_ids {
        let old = output_json(fixture.run().args(["show", old_id, "--json"]).output()?)?;
        assert_eq!(old["job"]["state"], "superseded");
        assert_eq!(old["attempts"][0]["state"], "succeeded");
        assert!(
            !fixture
                .run()
                .args(["retry", old_id])
                .output()?
                .status
                .success()
        );
    }
    let new_generation = generations
        .iter()
        .find(|item| item["identity"]["id"] == generation)
        .ok_or("missing new generation")?;
    assert_eq!(new_generation["identity"]["source_revision"], new_revision);
    assert_eq!(
        ["queued", "running", "succeeded"]
            .iter()
            .map(|state| new_generation["counts"][state].as_u64().unwrap_or(0))
            .sum::<u64>(),
        2
    );
    for job_id in &new_ids {
        let waited = fixture.run().args(["wait", job_id, "--json"]).output()?;
        assert!(
            waited.status.success(),
            "{}",
            String::from_utf8_lossy(&waited.stderr)
        );
        let waited: Value = serde_json::from_slice(&waited.stdout)?;
        assert_eq!(waited["job"]["state"], "succeeded");
    }
    Ok(())
}

#[test]
fn family_retry_recovers_a_failed_seed_without_creating_a_new_generation() -> TestResult {
    let _guard = worker_test_guard()?;
    let fixture = Fixture::new()?;
    let script = fixture._temporary.path().join("fail-once.sh");
    let marker = fixture._temporary.path().join("attempted-once");
    fs::write(
        &script,
        format!(
            "#!/bin/sh\nif [ -f '{}' ]; then exit 0; fi\ntouch '{}'\nexit 7\n",
            marker.display(),
            marker.display()
        ),
    )?;
    let family_file = fixture._temporary.path().join("family-retry.toml");
    fs::write(
        &family_file,
        format!(
            "schema_version = 1\nname = 'retry-seeds'\n[execution]\nprogram = '/bin/sh'\n\
             [[members]]\nseed = 1\nargs = ['{}']\n\
             [[members]]\nseed = 2\nargs = ['/dev/null']\n",
            script.display()
        ),
    )?;
    let submitted = output_json(
        fixture
            .run()
            .args([
                "family",
                "submit",
                "--file",
                family_file.to_str().ok_or("non-UTF-8 family file")?,
                "--json",
            ])
            .output()?,
    )?;
    let family_id = submitted["family_id"].as_str().ok_or("missing family id")?;
    let generation_id = submitted["generation_id"]
        .as_str()
        .ok_or("missing generation id")?;
    let jobs = submitted["jobs"].as_array().ok_or("missing family jobs")?;
    let failed_id = jobs[0]["spec"]["id"]
        .as_str()
        .ok_or("missing first job id")?;
    let succeeded_id = jobs[1]["spec"]["id"]
        .as_str()
        .ok_or("missing second job id")?;
    let failed = fixture.run().args(["wait", failed_id, "--json"]).output()?;
    assert_eq!(failed.status.code(), Some(1));
    let failed: Value = serde_json::from_slice(&failed.stdout)?;
    assert_eq!(failed["job"]["state"], "failed");
    let completed = fixture.run().args(["wait", succeeded_id]).output()?;
    assert!(completed.status.success());
    let before = output_json(
        fixture
            .run()
            .args(["family", "show", family_id, "--json"])
            .output()?,
    )?;
    assert_eq!(before["generations"][0]["status"], "complete");

    let retried = output_json(
        fixture
            .run()
            .args(["retry", failed_id, "--json"])
            .output()?,
    )?;
    assert_eq!(retried["attempts"].as_array().map(Vec::len), Some(2));
    assert_eq!(retried["attempts"][0]["state"], "failed");
    assert_eq!(retried["attempts"][1]["spec"]["sequence"], 2);
    assert_ne!(
        retried["attempts"][0]["spec"]["id"],
        retried["attempts"][1]["spec"]["id"]
    );
    for field in ["family", "source", "configuration", "result"] {
        assert_eq!(
            retried["attempts"][0]["spec"][field],
            retried["attempts"][1]["spec"][field]
        );
    }
    let waited = fixture.run().args(["wait", failed_id, "--json"]).output()?;
    assert!(
        waited.status.success(),
        "{}",
        String::from_utf8_lossy(&waited.stderr)
    );
    let detail = output_json(
        fixture
            .run()
            .args(["family", "show", family_id, "--json"])
            .output()?,
    )?;
    assert_eq!(detail["generations"].as_array().map(Vec::len), Some(1));
    assert_eq!(detail["generations"][0]["identity"]["id"], generation_id);
    assert_eq!(detail["generations"][0]["status"], "comparable");
    assert_eq!(detail["counts"]["succeeded"], 2);
    Ok(())
}

fn write_five_seed_family(
    fixture: &Fixture,
    script: &Path,
    name: &str,
) -> Result<std::path::PathBuf, Box<dyn Error>> {
    let file = fixture._temporary.path().join(format!("{name}.toml"));
    let members = (1..=5)
        .map(|seed| {
            format!(
                "[[members]]\nseed = {seed}\nargs = ['{}', '{seed}']\n",
                script.display()
            )
        })
        .collect::<String>();
    fs::write(
        &file,
        format!(
            "schema_version = 1\nname = '{name}'\nimmutable_inputs = ['science.json']\n[execution]\nprogram = '/bin/sh'\n{members}"
        ),
    )?;
    Ok(file)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn five_seed_family_is_partial_until_all_real_executions_succeed() -> TestResult {
    let _guard = worker_test_guard()?;
    let fixture = Fixture::new()?;
    let barrier = fixture._temporary.path().join("five-seed-barrier");
    fs::create_dir_all(&barrier)?;
    let script = fixture.project.join("five-seed-run.sh");
    fs::write(
        &script,
        format!(
            "#!/bin/sh\nprintf ready > '{}/started-'\"$1\"\ni=0\nwhile [ ! -e '{}/release' ] && [ \"$i\" -lt 150 ]; do i=$((i + 1)); sleep 0.1; done\n[ -e '{}/release' ]\n",
            barrier.display(),
            barrier.display(),
            barrier.display()
        ),
    )?;
    git(&fixture.project, &["add", "five-seed-run.sh"])?;
    git(&fixture.project, &["commit", "-qm", "five seed baseline"])?;
    let file = write_five_seed_family(&fixture, &script, "five-seed-partial")?;
    let submitted = output_json(
        fixture
            .run()
            .args([
                "family",
                "submit",
                "--file",
                file.to_str().ok_or("non-UTF-8 family file")?,
                "--json",
            ])
            .output()?,
    )?;
    let family_id = submitted["family_id"].as_str().ok_or("missing family ID")?;
    let generation_id: igor_core::GenerationId = submitted["generation_id"]
        .as_str()
        .ok_or("missing generation ID")?
        .parse()?;
    let jobs = submitted["jobs"].as_array().ok_or("missing family jobs")?;
    assert_eq!(jobs.len(), 5);
    wait_for_marker(&barrier.join("started-1"))?;
    let partial = output_json(
        fixture
            .run()
            .args(["family", "show", family_id, "--json"])
            .output()?,
    )?;
    assert_eq!(partial["current_generation_id"], generation_id.to_string());
    assert_eq!(partial["generations"][0]["status"], "incomplete");
    assert_eq!(partial["generations"][0]["counts"]["running"], 1);
    assert_eq!(partial["generations"][0]["counts"]["queued"], 4);
    let database = Database::open(fixture.home.join("state/igor/igor.sqlite3")).await?;
    let family_id: igor_core::FamilyId = family_id.parse()?;
    assert!(matches!(
        database
            .families()
            .comparable_generation(family_id, generation_id)
            .await,
        Err(igor_core::PersistenceError::GenerationNotComparable {
            status: igor_core::GenerationStatus::Incomplete,
            ..
        })
    ));
    fs::write(barrier.join("release"), b"go")?;
    for job in jobs {
        let job_id = job["spec"]["id"].as_str().ok_or("missing job ID")?;
        let waited = fixture.run().args(["wait", job_id, "--json"]).output()?;
        assert!(
            waited.status.success(),
            "{}",
            String::from_utf8_lossy(&waited.stderr)
        );
    }
    let complete = output_json(
        fixture
            .run()
            .args(["family", "show", &family_id.to_string(), "--json"])
            .output()?,
    )?;
    let generation = &complete["generations"][0];
    assert_eq!(generation["status"], "comparable");
    assert_eq!(generation["counts"]["succeeded"], 5);
    let identity = &generation["identity"];
    let actual = generation["jobs"]
        .as_array()
        .ok_or("missing generation members")?;
    for (index, job) in actual.iter().enumerate() {
        assert_eq!(job["spec"]["family"]["seed"], (index + 1) as u64);
        assert_eq!(&job["spec"]["family"]["generation"], identity);
        let detail = output_json(
            fixture
                .run()
                .args([
                    "show",
                    job["spec"]["id"].as_str().ok_or("missing member ID")?,
                    "--json",
                ])
                .output()?,
        )?;
        assert_eq!(
            detail["attempts"][0]["spec"]["source"]["identity"]["revision"],
            identity["source_revision"]
        );
    }
    let comparable = database
        .families()
        .comparable_generation(family_id, generation_id)
        .await?;
    assert_eq!(comparable.members().len(), 5);
    assert_eq!(
        comparable.identity().source_revision,
        identity["source_revision"]
            .as_str()
            .ok_or("missing revision")?
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn five_seed_failure_retry_and_repaired_generation_never_mix_members() -> TestResult {
    let _guard = worker_test_guard()?;
    let fixture = Fixture::new()?;
    let script = fixture.project.join("five-seed-repair.sh");
    fs::write(
        &script,
        "#!/bin/sh\nif [ \"$1\" = 3 ]; then exit 17; fi\nexit 0\n",
    )?;
    git(&fixture.project, &["add", "five-seed-repair.sh"])?;
    git(&fixture.project, &["commit", "-qm", "initial failing code"])?;
    let file = write_five_seed_family(&fixture, &script, "five-seed-repair")?;
    let file_path = file.to_str().ok_or("non-UTF-8 family file")?;
    let submitted = output_json(
        fixture
            .run()
            .args(["family", "submit", "--file", file_path, "--json"])
            .output()?,
    )?;
    let family_id: igor_core::FamilyId = submitted["family_id"]
        .as_str()
        .ok_or("missing family ID")?
        .parse()?;
    let old_generation: igor_core::GenerationId = submitted["generation_id"]
        .as_str()
        .ok_or("missing generation ID")?
        .parse()?;
    let jobs = submitted["jobs"].as_array().ok_or("missing family jobs")?;
    assert_eq!(jobs.len(), 5);
    let old_ids: Vec<String> = jobs
        .iter()
        .map(|job| {
            job["spec"]["id"]
                .as_str()
                .map(str::to_owned)
                .ok_or("missing job ID")
        })
        .collect::<Result<_, _>>()?;
    for (index, id) in old_ids.iter().enumerate() {
        let result = fixture.run().args(["wait", id, "--json"]).output()?;
        assert_eq!(result.status.success(), index != 2);
    }
    let database = Database::open(fixture.home.join("state/igor/igor.sqlite3")).await?;
    let failed = output_json(
        fixture
            .run()
            .args(["family", "show", &family_id.to_string(), "--json"])
            .output()?,
    )?;
    assert_eq!(failed["generations"][0]["status"], "complete");
    assert_eq!(failed["generations"][0]["counts"]["succeeded"], 4);
    assert_eq!(failed["generations"][0]["counts"]["failed"], 1);
    assert!(
        database
            .families()
            .comparable_generation(family_id, old_generation)
            .await
            .is_err()
    );
    let retried = output_json(
        fixture
            .run()
            .args(["retry", &old_ids[2], "--json"])
            .output()?,
    )?;
    assert_eq!(retried["attempts"].as_array().map(Vec::len), Some(2));
    let retry_result = fixture
        .run()
        .args(["wait", &old_ids[2], "--json"])
        .output()?;
    assert_eq!(retry_result.status.code(), Some(1));
    let failed_again = output_json(
        fixture
            .run()
            .args(["family", "show", &family_id.to_string(), "--json"])
            .output()?,
    )?;
    assert_eq!(
        failed_again["current_generation_id"],
        old_generation.to_string()
    );
    assert_eq!(failed_again["generations"][0]["status"], "complete");

    fs::write(&script, "#!/bin/sh\nexit 0\n")?;
    git(&fixture.project, &["add", "five-seed-repair.sh"])?;
    git(&fixture.project, &["commit", "-qm", "fix failing seed"])?;
    let mixed_file = fixture._temporary.path().join("mixed-science.toml");
    fs::write(
        &mixed_file,
        fs::read_to_string(&file)?.replace("seed = 5\n", "seed = 6\n"),
    )?;
    let mixed = fixture
        .run()
        .args([
            "family",
            "supersede",
            &family_id.to_string(),
            "--file",
            mixed_file.to_str().ok_or("non-UTF-8 mixed family")?,
        ])
        .output()?;
    assert_eq!(mixed.status.code(), Some(9));
    let unchanged = output_json(
        fixture
            .run()
            .args(["family", "show", &family_id.to_string(), "--json"])
            .output()?,
    )?;
    assert_eq!(
        unchanged["current_generation_id"],
        old_generation.to_string()
    );
    assert_eq!(unchanged["generations"].as_array().map(Vec::len), Some(1));

    let successor = output_json(
        fixture
            .run()
            .args([
                "family",
                "supersede",
                &family_id.to_string(),
                "--file",
                file_path,
                "--json",
            ])
            .output()?,
    )?;
    let new_generation: igor_core::GenerationId = successor["generation_id"]
        .as_str()
        .ok_or("missing successor ID")?
        .parse()?;
    assert_ne!(new_generation, old_generation);
    let new_jobs = successor["jobs"]
        .as_array()
        .ok_or("missing successor jobs")?;
    assert_eq!(new_jobs.len(), 5);
    let new_ids: Vec<String> = new_jobs
        .iter()
        .map(|job| {
            job["spec"]["id"]
                .as_str()
                .map(str::to_owned)
                .ok_or("missing new job ID")
        })
        .collect::<Result<_, _>>()?;
    assert!(new_ids.iter().all(|id| !old_ids.contains(id)));
    for id in &new_ids {
        let result = fixture.run().args(["wait", id, "--json"]).output()?;
        assert!(
            result.status.success(),
            "{}",
            String::from_utf8_lossy(&result.stderr)
        );
    }
    let repaired = output_json(
        fixture
            .run()
            .args(["family", "show", &family_id.to_string(), "--json"])
            .output()?,
    )?;
    assert_eq!(
        repaired["current_generation_id"],
        new_generation.to_string()
    );
    assert_eq!(repaired["generations"].as_array().map(Vec::len), Some(2));
    assert_eq!(repaired["generations"][0]["counts"]["superseded"], 5);
    assert_eq!(repaired["generations"][1]["status"], "comparable");
    assert_ne!(
        repaired["generations"][0]["identity"]["source_revision"],
        repaired["generations"][1]["identity"]["source_revision"]
    );
    let old_attempt = output_json(
        fixture
            .run()
            .args(["show", &old_ids[2], "--json"])
            .output()?,
    )?;
    assert_eq!(old_attempt["attempts"].as_array().map(Vec::len), Some(2));
    assert!(
        old_attempt["attempts"]
            .as_array()
            .is_some_and(|attempts| attempts.iter().all(|attempt| attempt["state"] == "failed"))
    );
    assert!(
        database
            .families()
            .comparable_generation(family_id, old_generation)
            .await
            .is_err()
    );
    let comparable = database
        .families()
        .comparable_generation(family_id, new_generation)
        .await?;
    let selected_ids: Vec<String> = comparable
        .members()
        .iter()
        .map(|job| job.spec.id.to_string())
        .collect();
    assert_eq!(selected_ids, new_ids);
    Ok(())
}

#[test]
fn job_file_dirty_policy_and_frozen_attempt_are_enforced() -> TestResult {
    let _guard = worker_test_guard()?;
    let fixture = Fixture::new()?;
    let job_file = fixture._temporary.path().join("job.toml");
    fs::write(
        &job_file,
        "schema_version = 1\nname = 'from file'\npriority = 8\nscientific_configurations = ['science.json']\n[execution]\nprogram = 'julia'\nargs = ['--project=.', 'run.jl']\n",
    )?;
    let submitted = output_json(
        fixture
            .run()
            .args([
                "submit",
                "--file",
                job_file.to_str().ok_or("non-UTF-8 file")?,
                "--json",
            ])
            .output()?,
    )?;
    let job_id = submitted["spec"]["id"].as_str().ok_or("missing job id")?;
    let before = output_json(fixture.run().args(["show", job_id, "--json"]).output()?)?;
    let frozen = before["attempts"][0]["spec"].clone();

    fs::write(fixture.project.join("science.json"), b"changed\n")?;
    let rejected = fixture.run().args(["submit", "--", "true"]).output()?;
    assert!(!rejected.status.success());
    assert!(String::from_utf8(rejected.stderr)?.contains("--allow-dirty"));
    let allowed = fixture
        .run()
        .args(["submit", "--allow-dirty", "--json", "--", "true"])
        .output()?;
    assert!(
        allowed.status.success(),
        "{}",
        String::from_utf8_lossy(&allowed.stderr)
    );

    git(&fixture.project, &["add", "science.json"])?;
    git(&fixture.project, &["commit", "-qm", "change science"])?;
    let after = output_json(fixture.run().args(["show", job_id, "--json"]).output()?)?;
    assert_eq!(after["attempts"][0]["spec"], frozen);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn wait_returns_when_job_is_terminal() -> TestResult {
    let _guard = worker_test_guard()?;
    let mut fixture = Fixture::new()?;
    let success = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/process/success.sh");
    let job_file = fixture._temporary.path().join("process-success.toml");
    fs::write(
        &job_file,
        format!(
            "schema_version = 1\n[execution]\nprogram = '/bin/sh'\nargs = ['{}']\n[environment]\ninherit = 'all'\nremove = ['HOME']\n[environment.set]\nSAFE_VALUE = 'visible'\n",
            success.display()
        ),
    )?;
    let submitted = output_json(
        fixture
            .run()
            .args([
                "submit",
                "--json",
                "--file",
                job_file.to_str().ok_or("non-UTF-8 job file")?,
            ])
            .output()?,
    )?;
    let job_id = submitted["spec"]["id"].as_str().ok_or("missing job id")?;
    let started = Instant::now();
    let waited = fixture.run().args(["wait", job_id, "--json"]).output()?;
    assert!(waited.status.success());
    assert!(started.elapsed() >= Duration::from_millis(200));
    let waited: Value = serde_json::from_slice(&waited.stdout)?;
    assert_eq!(waited["job"]["state"], "succeeded");
    let attempt_id = waited["attempts"][0]["spec"]["id"]
        .as_str()
        .ok_or("missing attempt id")?;
    let log_dir = fixture.home.join("state/igor/logs").join(attempt_id);
    let stdout = fs::read_to_string(log_dir.join("stdout.log"))?;
    assert!(stdout.starts_with("fixture stdout\nidentity "));
    let identity: Vec<&str> = stdout
        .lines()
        .nth(1)
        .ok_or("missing process identity output")?
        .split_whitespace()
        .collect();
    assert_eq!(identity.len(), 3);
    assert_eq!(identity[1], identity[2]);
    assert_eq!(
        fs::read_to_string(log_dir.join("stderr.log"))?,
        "fixture stderr\n"
    );
    assert_eq!(
        fs::metadata(log_dir.join("stdout.log"))?
            .permissions()
            .mode()
            & 0o777,
        0o600
    );
    let database = Database::open(fixture.home.join("state/igor/igor.sqlite3")).await?;
    let attempt_id: AttemptId = attempt_id.parse()?;
    let process = database
        .jobs()
        .process_for_attempt(attempt_id)
        .await?
        .ok_or("missing successful process metadata")?;
    assert_eq!(process.pid, process.process_group_id);
    assert!(process.process_start_ticks > 0);
    assert_eq!(process.exit_code, Some(0));

    let failure = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/process/failure.sh");
    let submitted = output_json(
        fixture
            .run()
            .args([
                "submit",
                "--json",
                "--",
                "/bin/sh",
                failure.to_str().ok_or("non-UTF-8 failure fixture")?,
            ])
            .output()?,
    )?;
    let failed_job_id = submitted["spec"]["id"]
        .as_str()
        .ok_or("missing failed job id")?;
    let failed = fixture
        .run()
        .args(["wait", failed_job_id, "--json"])
        .output()?;
    assert_eq!(failed.status.code(), Some(1));
    let failed: Value = serde_json::from_slice(&failed.stdout)?;
    assert_eq!(failed["job"]["state"], "failed");
    let attempt_id = failed["attempts"][0]["spec"]["id"]
        .as_str()
        .ok_or("missing failed attempt id")?;
    let log_dir = fixture.home.join("state/igor/logs").join(attempt_id);
    assert_eq!(
        fs::read_to_string(log_dir.join("stdout.log"))?,
        "failure stdout\n"
    );
    assert_eq!(
        fs::read_to_string(log_dir.join("stderr.log"))?,
        "failure stderr\n"
    );

    let signal = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/process/signal.sh");
    let submitted = output_json(
        fixture
            .run()
            .args([
                "submit",
                "--json",
                "--",
                "/bin/sh",
                signal.to_str().ok_or("non-UTF-8 signal fixture")?,
            ])
            .output()?,
    )?;
    let signal_job_id = submitted["spec"]["id"]
        .as_str()
        .ok_or("missing signal job id")?;
    let signalled = fixture
        .run()
        .args(["wait", signal_job_id, "--json"])
        .output()?;
    assert_eq!(signalled.status.code(), Some(1));
    let signalled: Value = serde_json::from_slice(&signalled.stdout)?;
    let attempt_id: AttemptId = signalled["attempts"][0]["spec"]["id"]
        .as_str()
        .ok_or("missing signalled attempt id")?
        .parse()?;
    assert_eq!(
        database
            .jobs()
            .process_for_attempt(attempt_id)
            .await?
            .ok_or("missing signalled process metadata")?
            .term_signal,
        Some(15)
    );

    let timeout = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/process/timeout.sh");
    let timeout_job = fixture._temporary.path().join("process-timeout.toml");
    fs::write(
        &timeout_job,
        format!(
            "schema_version = 1\n[execution]\nprogram = '/bin/sh'\nargs = ['{}']\n[resources]\ntimeout_seconds = 1\n",
            timeout.display()
        ),
    )?;
    let submitted = output_json(
        fixture
            .run()
            .args([
                "submit",
                "--json",
                "--file",
                timeout_job.to_str().ok_or("non-UTF-8 timeout job file")?,
            ])
            .output()?,
    )?;
    let timeout_job_id = submitted["spec"]["id"]
        .as_str()
        .ok_or("missing timeout job id")?;
    let started = Instant::now();
    let timed_out = fixture
        .run()
        .args(["wait", timeout_job_id, "--json"])
        .output()?;
    assert_eq!(timed_out.status.code(), Some(1));
    assert!(started.elapsed() < Duration::from_secs(4));
    let timed_out: Value = serde_json::from_slice(&timed_out.stdout)?;
    assert_eq!(timed_out["job"]["state"], "failed");

    // Keep the recovered process alive across the heartbeat wait and worker
    // restart even when the full integration suite is running slowly.
    let observed_script = fixture._temporary.path().join("recovered-wait.sh");
    fs::write(&observed_script, "#!/bin/sh\nsleep 60\n")?;
    let submitted = output_json(
        fixture
            .run()
            .args([
                "submit",
                "--json",
                "--",
                "/bin/sh",
                observed_script.to_str().ok_or("non-UTF-8 sleep fixture")?,
            ])
            .output()?,
    )?;
    let observed_job_id: JobId = submitted["spec"]["id"]
        .as_str()
        .ok_or("missing observed job id")?
        .parse()?;
    let (observed_attempt_id, initial_heartbeat) = loop {
        let detail = database
            .jobs()
            .detail(observed_job_id)
            .await?
            .ok_or("missing observed job")?;
        if let Some(process) = database
            .jobs()
            .process_for_attempt(detail.attempts[0].spec.id())
            .await?
        {
            break (process.attempt_id, process.heartbeat_at);
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    };
    tokio::time::sleep(Duration::from_millis(5_500)).await;
    let observed = database
        .jobs()
        .process_for_attempt(observed_attempt_id)
        .await?
        .ok_or("missing observed process")?;
    assert!(observed.heartbeat_at > initial_heartbeat);
    assert_eq!(
        database
            .jobs()
            .get_job(observed_job_id)
            .await?
            .ok_or("missing running observed job")?
            .state,
        JobState::Running
    );
    fixture.worker.stop()?;
    assert_eq!(
        database
            .jobs()
            .get_job(observed_job_id)
            .await?
            .ok_or("missing interrupted observed job")?
            .state,
        JobState::Running
    );
    assert!(kill(Pid::from_raw(i32::try_from(observed.pid)?), None::<Signal>).is_ok());
    fixture.restart_worker()?;
    assert_eq!(
        database
            .jobs()
            .get_job(observed_job_id)
            .await?
            .ok_or("missing recovered observed job")?
            .state,
        JobState::Running
    );
    let cancellation = fixture
        .run()
        .args([
            "cancel",
            &observed_job_id.to_string(),
            "--grace-seconds",
            "0",
        ])
        .output()?;
    assert!(
        cancellation.status.success(),
        "{}",
        String::from_utf8_lossy(&cancellation.stderr)
    );
    let waited = fixture
        .run()
        .args(["wait", &observed_job_id.to_string(), "--json"])
        .output()?;
    assert_eq!(waited.status.code(), Some(1));
    let waited: Value = serde_json::from_slice(&waited.stdout)?;
    assert_eq!(waited["job"]["state"], "cancelled");
    let interrupted = database
        .jobs()
        .process_for_attempt(observed_attempt_id)
        .await?
        .ok_or("missing interrupted process")?;
    assert_eq!(interrupted.term_signal, None);
    assert_eq!(
        interrupted.error.as_deref(),
        Some("execution cancelled after worker recovery")
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn restart_recovers_two_active_shared_executions_concurrently() -> TestResult {
    let _guard = worker_test_guard()?;
    let mut fixture = Fixture::with_global_config(Some(
        "schema_version = 1\n[host]\nmax_concurrent_jobs = 2\n",
    ))?;
    let database = Database::open(fixture.home.join("state/igor/igor.sqlite3")).await?;
    let barrier_root = fixture._temporary.path().join("recovery-barriers");
    let script = process_fixture("barrier.sh");
    let mut jobs = Vec::new();

    for name in ["first", "second"] {
        let barrier = barrier_root.join(name);
        fs::create_dir_all(&barrier)?;
        let job_file = fixture
            ._temporary
            .path()
            .join(format!("recovery-{name}.toml"));
        fs::write(
            &job_file,
            format!(
                "schema_version = 1\nname = 'recovery-{name}'\n[execution]\nprogram = '/bin/sh'\nargs = ['{}', '{}']\n[resources]\nmode = 'shared'\n",
                script.display(),
                barrier.display()
            ),
        )?;
        let submitted = output_json(
            fixture
                .run()
                .args([
                    "submit",
                    "--json",
                    "--file",
                    job_file.to_str().ok_or("non-UTF-8 recovery job file")?,
                ])
                .output()?,
        )?;
        jobs.push(
            submitted["spec"]["id"]
                .as_str()
                .ok_or("missing recovery job id")?
                .parse::<JobId>()?,
        );
    }

    wait_for_marker(&barrier_root.join("first/started"))?;
    wait_for_marker(&barrier_root.join("second/started"))?;
    let mut original_processes = Vec::new();
    let mut old_claim_ids = Vec::new();
    let old_owner = format!("worker:{}", fixture.worker.0.id());
    for (job, name) in jobs.iter().zip(["first", "second"]) {
        let process = wait_for_process(&database, *job).await?;
        let barrier_pid = fs::read_to_string(barrier_root.join(name).join("pid"))?
            .trim()
            .parse::<i64>()?;
        assert_eq!(process.pid, barrier_pid);
        assert!(kill(Pid::from_raw(i32::try_from(process.pid)?), None::<Signal>).is_ok());
        let (claim_id, claim_owner): (String, String) =
            sqlx::query_as("SELECT claim_id, claim_owner FROM jobs WHERE id = ?")
                .bind(job.to_string())
                .fetch_one(database.pool())
                .await?;
        assert_eq!(claim_owner, old_owner);
        old_claim_ids.push(claim_id);
        original_processes.push(process);
    }

    fixture.restart_worker()?;
    let replacement_owner = format!("worker:{}", fixture.worker.0.id());
    assert_ne!(replacement_owner, old_owner);
    let recovered_processes = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let mut processes = Vec::new();
            let mut both_recovered = true;
            for (index, job) in jobs.iter().enumerate() {
                let claim: Option<(String, String, String)> =
                    sqlx::query_as("SELECT state, claim_id, claim_owner FROM jobs WHERE id = ?")
                        .bind(job.to_string())
                        .fetch_optional(database.pool())
                        .await?;
                let replacement_leases: i64 = sqlx::query_scalar(
                    "SELECT COUNT(*) FROM resource_leases WHERE job_id = ? AND owner = ?",
                )
                .bind(job.to_string())
                .bind(&replacement_owner)
                .fetch_one(database.pool())
                .await?;
                let other_leases: i64 = sqlx::query_scalar(
                    "SELECT COUNT(*) FROM resource_leases WHERE job_id = ? AND owner != ?",
                )
                .bind(job.to_string())
                .bind(&replacement_owner)
                .fetch_one(database.pool())
                .await?;
                let process = database
                    .jobs()
                    .process_for_attempt(original_processes[index].attempt_id)
                    .await?;
                if !matches!(
                    claim,
                    Some((ref state, ref claim_id, ref owner))
                        if state == "running"
                            && claim_id != &old_claim_ids[index]
                            && owner == &replacement_owner
                ) || replacement_leases == 0
                    || other_leases != 0
                    || process.is_none()
                {
                    both_recovered = false;
                    break;
                }
                processes.push(process.ok_or("missing recovered process")?);
            }
            if both_recovered {
                return Ok::<_, Box<dyn Error>>(processes);
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .map_err(|_| "both executions were not recovered by the replacement worker")??;

    let old_job_claims: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM jobs WHERE claim_owner = ?")
        .bind(&old_owner)
        .fetch_one(database.pool())
        .await?;
    let old_resource_claims: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM resource_leases WHERE owner = ?")
            .bind(&old_owner)
            .fetch_one(database.pool())
            .await?;
    assert_eq!(old_job_claims, 0);
    assert_eq!(old_resource_claims, 0);
    for ((job, original), recovered) in jobs
        .iter()
        .zip(&original_processes)
        .zip(&recovered_processes)
    {
        let detail = database
            .jobs()
            .detail(*job)
            .await?
            .ok_or("missing recovered job")?;
        assert_eq!(detail.attempts.len(), 1);
        assert_eq!(detail.attempts[0].spec.id(), original.attempt_id);
        assert_eq!(recovered.attempt_id, original.attempt_id);
        assert_eq!(recovered.pid, original.pid);
        assert_eq!(recovered.process_group_id, original.process_group_id);
        assert_eq!(recovered.process_start_ticks, original.process_start_ticks);
        assert!(kill(Pid::from_raw(i32::try_from(recovered.pid)?), None::<Signal>).is_ok());
    }

    fs::write(barrier_root.join("first/release"), b"")?;
    fs::write(barrier_root.join("second/release"), b"")?;
    for job in jobs {
        let waited = fixture
            .run()
            .args(["wait", &job.to_string(), "--json"])
            .output()?;
        assert_eq!(waited.status.code(), Some(1));
        let waited: Value = serde_json::from_slice(&waited.stdout)?;
        assert_eq!(waited["job"]["state"], "lost");
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn recovery_classifies_missing_and_reused_process_identities_as_lost() -> TestResult {
    let _guard = worker_test_guard()?;
    let mut fixture = Fixture::new()?;
    let database = Database::open(fixture.home.join("state/igor/igor.sqlite3")).await?;

    let missing_job = submit_process(&fixture, "child-process.sh")?;
    let missing_process = wait_for_process(&database, missing_job).await?;
    fixture.worker.stop()?;
    killpg(
        Pid::from_raw(i32::try_from(missing_process.process_group_id)?),
        Signal::SIGKILL,
    )?;
    for _ in 0..100 {
        if kill(
            Pid::from_raw(i32::try_from(missing_process.pid)?),
            None::<Signal>,
        )
        .is_err()
        {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    fixture.restart_worker()?;
    let waited = fixture
        .run()
        .args(["wait", &missing_job.to_string(), "--json"])
        .output()?;
    assert_eq!(waited.status.code(), Some(1));
    let waited: Value = serde_json::from_slice(&waited.stdout)?;
    assert_eq!(waited["job"]["state"], "lost");

    let reused_job = submit_process(&fixture, "child-process.sh")?;
    let reused_process = wait_for_process(&database, reused_job).await?;
    fixture.worker.stop()?;
    sqlx::query(
        "UPDATE attempt_processes SET process_start_ticks = process_start_ticks + 1
         WHERE attempt_id = ?",
    )
    .bind(reused_process.attempt_id.to_string())
    .execute(database.pool())
    .await?;
    fixture.restart_worker()?;
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(
        database
            .jobs()
            .get_job(reused_job)
            .await?
            .ok_or("missing reused-PID job")?
            .state,
        JobState::Running
    );
    assert!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM resource_leases WHERE job_id = ?")
            .bind(reused_job.to_string())
            .fetch_one(database.pool())
            .await?
            > 0
    );
    assert!(
        kill(
            Pid::from_raw(i32::try_from(reused_process.pid)?),
            None::<Signal>
        )
        .is_ok()
    );
    killpg(
        Pid::from_raw(i32::try_from(reused_process.process_group_id)?),
        Signal::SIGKILL,
    )?;
    let waited = fixture
        .run()
        .args(["wait", &reused_job.to_string(), "--json"])
        .output()?;
    assert_eq!(waited.status.code(), Some(1));
    let waited: Value = serde_json::from_slice(&waited.stdout)?;
    assert_eq!(waited["job"]["state"], "lost");
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM resource_leases WHERE job_id = ?")
            .bind(reused_job.to_string())
            .fetch_one(database.pool())
            .await?,
        0
    );

    let timeout_job_file = fixture._temporary.path().join("recovered-timeout.toml");
    fs::write(
        &timeout_job_file,
        format!(
            "schema_version = 1\n[execution]\nprogram = '/bin/sh'\nargs = ['{}']\n[resources]\ntimeout_seconds = 2\n",
            process_fixture("timeout.sh").display()
        ),
    )?;
    let submitted = output_json(
        fixture
            .run()
            .args([
                "submit",
                "--json",
                "--file",
                timeout_job_file
                    .to_str()
                    .ok_or("non-UTF-8 recovered timeout file")?,
            ])
            .output()?,
    )?;
    let timeout_job: JobId = submitted["spec"]["id"]
        .as_str()
        .ok_or("missing recovered timeout job id")?
        .parse()?;
    let timeout_process = wait_for_process(&database, timeout_job).await?;
    tokio::time::sleep(Duration::from_millis(750)).await;
    fixture.restart_worker()?;
    let started = Instant::now();
    let waited = fixture
        .run()
        .args(["wait", &timeout_job.to_string(), "--json"])
        .output()?;
    assert_eq!(waited.status.code(), Some(1));
    assert!(started.elapsed() < Duration::from_secs(2));
    let waited: Value = serde_json::from_slice(&waited.stdout)?;
    assert_eq!(waited["job"]["state"], "failed");
    assert_eq!(
        database
            .jobs()
            .process_for_attempt(timeout_process.attempt_id)
            .await?
            .ok_or("missing recovered timeout process")?
            .error
            .as_deref(),
        Some("configured execution timeout elapsed after recovery")
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancellation_and_retry_preserve_process_and_attempt_contracts() -> TestResult {
    let _guard = worker_test_guard()?;
    let fixture = Fixture::new()?;
    let database = Database::open(fixture.home.join("state/igor/igor.sqlite3")).await?;
    let graceful_job = submit_process(&fixture, "cancel-graceful.sh")?;
    let graceful_process = wait_for_process(&database, graceful_job).await?;
    let cancelled = output_json(
        fixture
            .run()
            .args([
                "cancel",
                &graceful_job.to_string(),
                "--grace-seconds",
                "2",
                "--json",
            ])
            .output()?,
    )?;
    assert!(matches!(
        cancelled["job"]["state"].as_str(),
        Some("running" | "cancelled")
    ));
    let waited = fixture
        .run()
        .args(["wait", &graceful_job.to_string(), "--json"])
        .output()?;
    assert_eq!(waited.status.code(), Some(1));
    let waited: Value = serde_json::from_slice(&waited.stdout)?;
    assert_eq!(waited["job"]["state"], "cancelled");
    let process = database
        .jobs()
        .process_for_attempt(graceful_process.attempt_id)
        .await?
        .ok_or("missing cancelled process")?;
    assert_eq!(process.exit_code, Some(0));
    assert_eq!(process.term_signal, None);
    assert_eq!(process.error.as_deref(), Some("execution cancelled"));
    let logs = fixture
        .run()
        .args(["logs", &graceful_job.to_string()])
        .output()?;
    assert!(logs.status.success());
    assert_eq!(logs.stdout, b"graceful-cancellation\n");

    let child_job = submit_process(&fixture, "child-process.sh")?;
    let child_process = wait_for_process(&database, child_job).await?;
    let identity = loop {
        let output = fs::read_to_string(&child_process.stdout_path)?;
        if let Some(line) = output.lines().next() {
            break line.to_owned();
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    };
    let child_pid = identity
        .split_whitespace()
        .find_map(|field| field.strip_prefix("child="))
        .ok_or("missing child PID")?
        .parse::<i32>()?;
    let started = Instant::now();
    let cancel = fixture
        .run()
        .args(["cancel", &child_job.to_string(), "--grace-seconds", "5"])
        .output()?;
    assert!(cancel.status.success());
    let expedite = fixture
        .run()
        .args(["cancel", &child_job.to_string(), "--grace-seconds", "0"])
        .output()?;
    assert!(expedite.status.success());
    let waited = fixture
        .run()
        .args(["wait", &child_job.to_string(), "--json"])
        .output()?;
    assert_eq!(waited.status.code(), Some(1));
    assert!(started.elapsed() < Duration::from_secs(2));
    for _ in 0..100 {
        if kill(Pid::from_raw(child_pid), None::<Signal>).is_err() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(kill(Pid::from_raw(child_pid), None::<Signal>).is_err());

    let failed_job = submit_process(&fixture, "failure.sh")?;
    assert_eq!(
        fixture
            .run()
            .args(["wait", &failed_job.to_string()])
            .output()?
            .status
            .code(),
        Some(1)
    );
    let retried = output_json(
        fixture
            .run()
            .args(["retry", &failed_job.to_string(), "--json"])
            .output()?,
    )?;
    assert_eq!(retried["attempts"].as_array().map(Vec::len), Some(2));
    assert_eq!(retried["attempts"][0]["state"], "failed");
    assert_ne!(
        retried["attempts"][0]["spec"]["id"],
        retried["attempts"][1]["spec"]["id"]
    );
    let waited = fixture
        .run()
        .args(["wait", &failed_job.to_string(), "--json"])
        .output()?;
    assert_eq!(waited.status.code(), Some(1));
    let waited: Value = serde_json::from_slice(&waited.stdout)?;
    assert_eq!(waited["attempts"].as_array().map(Vec::len), Some(2));
    assert!(
        waited["attempts"]
            .as_array()
            .is_some_and(|attempts| attempts.iter().all(|attempt| attempt["state"] == "failed"))
    );
    Ok(())
}

#[test]
fn logs_follow_and_preserve_large_and_binary_output() -> TestResult {
    let _guard = worker_test_guard()?;
    let fixture = Fixture::new()?;
    let followed = output_json(
        fixture
            .run()
            .args([
                "submit",
                "--shell",
                "printf 'before\\n'; sleep 1; printf 'after\\n'; printf 'error\\n' >&2",
                "--json",
            ])
            .output()?,
    )?;
    let followed_job = followed["spec"]["id"]
        .as_str()
        .ok_or("missing followed job id")?;
    let logs = fixture
        .run()
        .args(["logs", "--follow", followed_job])
        .output()?;
    assert!(logs.status.success());
    assert_eq!(logs.stdout, b"before\nafter\n");
    assert_eq!(logs.stderr, b"error\n");

    let large_job = submit_process(&fixture, "large-output.sh")?;
    let waited = fixture
        .run()
        .args(["wait", &large_job.to_string()])
        .output()?;
    assert!(waited.status.success());
    let logs = fixture
        .run()
        .args(["logs", &large_job.to_string()])
        .output()?;
    assert!(logs.status.success());
    assert_eq!(logs.stdout, vec![b'A'; 262_144]);
    assert_eq!(logs.stderr, vec![b'B'; 262_144]);

    let binary_job = submit_process(&fixture, "binary-output.sh")?;
    let waited = fixture
        .run()
        .args(["wait", &binary_job.to_string()])
        .output()?;
    assert!(waited.status.success());
    let logs = fixture
        .run()
        .args(["logs", &binary_job.to_string()])
        .output()?;
    assert!(logs.status.success());
    let expected: Vec<u8> = [0, 1, 128, 255].into_iter().cycle().take(1_024).collect();
    assert_eq!(logs.stdout, expected);
    assert_eq!(logs.stderr, expected);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn logs_read_supervisor_published_binary_success_archives() -> TestResult {
    let _guard = worker_test_guard()?;
    let fixture = Fixture::new()?;
    let mut supervisor = Worker(
        fixture
            .run()
            .arg("supervisor")
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()?,
    );
    let binary_job = submit_process(&fixture, "binary-output.sh")?;
    assert!(
        fixture
            .run()
            .args(["wait", &binary_job.to_string()])
            .output()?
            .status
            .success()
    );
    let database = Database::open(fixture.home.join("state/igor/igor.sqlite3")).await?;
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let logs = database.jobs().logs_for_job(binary_job).await?;
        if let (Some(stdout), Some(stderr)) = (logs.stdout_archive, logs.stderr_archive)
            && !stdout.original_path.exists()
            && !stderr.original_path.exists()
        {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "supervisor did not retire raw binary logs"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let expected: Vec<u8> = [0, 1, 128, 255].into_iter().cycle().take(1024).collect();
    for args in [
        vec!["logs".to_owned(), binary_job.to_string()],
        vec!["logs".into(), "--follow".into(), binary_job.to_string()],
    ] {
        let logs = fixture.run().args(args).output()?;
        assert!(
            logs.status.success(),
            "{}",
            String::from_utf8_lossy(&logs.stderr)
        );
        assert_eq!(logs.stdout, expected);
        assert_eq!(logs.stderr, expected);
    }
    supervisor.stop()?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn live_transient_unit_completion_and_restart_are_opt_in() -> TestResult {
    if std::env::var("IGOR_RUN_SYSTEMD_TESTS").as_deref() != Ok("1") {
        return Ok(());
    }
    let bus = std::env::var("DBUS_SESSION_BUS_ADDRESS")
        .or_else(|_| std::env::var("XDG_RUNTIME_DIR").map(|dir| format!("unix:path={dir}/bus")))?;
    if !Command::new("systemctl")
        .env("DBUS_SESSION_BUS_ADDRESS", &bus)
        .args(["--user", "is-system-running"])
        .output()?
        .status
        .success()
    {
        return Ok(());
    }
    let _guard = worker_test_guard()?;
    let mut fixture = Fixture::new()?;
    fixture.worker.stop()?;
    fixture.worker = spawn_systemd_worker(&fixture.home, &fixture.project, &bus)?;
    fixture.wait_until_worker_ready("systemd")?;
    let database = Database::open(fixture.home.join("state/igor/igor.sqlite3")).await?;
    let mut cleanup = LiveUnitCleanup {
        bus,
        units: Vec::new(),
    };
    for (name, exit, restart, cancel, timed) in [
        ("success", 0, false, false, false),
        ("failure", 7, false, false, false),
        ("signal", 0, false, false, false),
        ("restart", 0, true, false, false),
        ("cancel", 0, false, true, false),
        ("timeout", 0, false, false, true),
    ] {
        let job_file = fixture._temporary.path().join(format!("unit-{name}.toml"));
        let script = if cancel || timed {
            "sleep 30".to_owned()
        } else if name == "signal" {
            "kill -TERM $$".to_owned()
        } else if restart {
            "sleep 2; printf 'recovered\\n'".to_owned()
        } else {
            format!("printf 'unit-{name}\\n'; exit {exit}")
        };
        fs::write(
            &job_file,
            format!(
                "schema_version = 1\nname = 'unit-{name}'\n[execution]\nprogram = '/bin/sh'\nargs = ['-c', {}]\n[executor]\nkind = 'process'\n[executor.settings]\nisolation = 'systemd_user_unit'\n[resources]\nmode = 'shared'\n{}",
                serde_json::to_string(&script)?,
                if timed { "timeout_seconds = 1\n" } else { "" }
            ),
        )?;
        let submitted = output_json(
            fixture
                .run()
                .args([
                    "submit",
                    "--json",
                    "--file",
                    job_file.to_str().ok_or("non-UTF-8 unit job")?,
                ])
                .output()?,
        )?;
        let job_id: JobId = submitted["spec"]["id"]
            .as_str()
            .ok_or("missing unit job id")?
            .parse()?;
        let attempt_id = database
            .jobs()
            .detail(job_id)
            .await?
            .ok_or("missing unit job")?
            .attempts
            .first()
            .ok_or("missing unit attempt")?
            .spec
            .id();
        cleanup.units.push(format!("igor-job-{attempt_id}.service"));
        if restart || cancel {
            tokio::time::timeout(Duration::from_secs(10), async {
                loop {
                    if database
                        .jobs()
                        .unit_for_attempt(attempt_id)
                        .await?
                        .is_some_and(|unit| unit.state == "started")
                    {
                        return Ok::<_, Box<dyn Error>>(());
                    }
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
            })
            .await??;
            if restart {
                fixture.worker.stop()?;
                fixture.worker =
                    spawn_systemd_worker(&fixture.home, &fixture.project, &cleanup.bus)?;
                fixture.wait_until_worker_ready("recovered systemd")?;
            } else {
                let output = fixture
                    .run()
                    .args(["cancel", &job_id.to_string(), "--grace-seconds", "0"])
                    .output()?;
                assert!(
                    output.status.success(),
                    "{}",
                    String::from_utf8_lossy(&output.stderr)
                );
            }
        }
        let waited = tokio::time::timeout(Duration::from_secs(15), async {
            loop {
                let detail = database
                    .jobs()
                    .detail(job_id)
                    .await?
                    .ok_or("missing systemd job")?;
                if detail.job.state.is_terminal() {
                    return Ok::<_, Box<dyn Error>>(detail);
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await;
        let detail = match waited {
            Ok(detail) => detail?,
            Err(_) => {
                let unit = database.jobs().unit_for_attempt(attempt_id).await?;
                let status = Command::new("systemctl")
                    .env("DBUS_SESSION_BUS_ADDRESS", &cleanup.bus)
                    .args([
                        "--user",
                        "show",
                        "--",
                        cleanup.units.last().ok_or("unit missing")?,
                    ])
                    .output()?;
                return Err(format!(
                    "unit {name} did not finish: persisted {unit:?}; systemd {}",
                    String::from_utf8_lossy(&status.stdout)
                )
                .into());
            }
        };
        assert_eq!(
            detail.job.state,
            if cancel {
                JobState::Cancelled
            } else if timed || exit != 0 || name == "signal" {
                JobState::Failed
            } else {
                JobState::Succeeded
            }
        );
        let unit: UnitRecord = database
            .jobs()
            .unit_for_attempt(attempt_id)
            .await?
            .ok_or("missing unit record")?;
        assert!(unit.invocation_id.is_some());
        if name == "signal" {
            assert_eq!(
                unit.term_signal,
                Some(15),
                "{unit:?}; stderr {:?}",
                fs::read_to_string(&unit.stderr_path)?
            );
            assert_eq!(unit.exit_code, None);
        } else if !cancel && !timed {
            assert_eq!(unit.exit_code, Some(exit));
            assert_eq!(
                fs::read_to_string(&unit.stdout_path)?,
                if restart {
                    "recovered\n".to_owned()
                } else {
                    format!("unit-{name}\n")
                }
            );
        }
        let leases: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM resource_leases WHERE job_id = ?")
                .bind(job_id.to_string())
                .fetch_one(database.pool())
                .await?;
        assert_eq!(leases, 0);
        let removed = tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                if database
                    .jobs()
                    .unit_for_attempt(attempt_id)
                    .await?
                    .is_some_and(|unit| unit.state == "removed")
                {
                    return Ok::<_, Box<dyn Error>>(());
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await;
        if removed.is_err() {
            let unit = database.jobs().unit_for_attempt(attempt_id).await?;
            let status = Command::new("systemctl")
                .env("DBUS_SESSION_BUS_ADDRESS", &cleanup.bus)
                .args([
                    "--user",
                    "show",
                    "--",
                    cleanup.units.last().ok_or("unit missing")?,
                ])
                .output()?;
            return Err(format!(
                "unit {name} was not cleaned: persisted {unit:?}; systemd {}",
                String::from_utf8_lossy(&status.stdout)
            )
            .into());
        }
        removed??;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ambiguous_unit_status_survives_restart_without_relaunch() -> TestResult {
    let _guard = worker_test_guard()?;
    let mut fixture = Fixture::new()?;
    let bin = fixture._temporary.path().join("fake-systemd-bin");
    let state = fixture._temporary.path().join("fake-systemd-state");
    fs::create_dir(&bin)?;
    fs::create_dir(&state)?;
    let run = bin.join("systemd-run");
    fs::write(
        &run,
        r#"#!/bin/sh
set -eu
for arg in "$@"; do case "$arg" in --unit=*) unit="${arg#--unit=}";; esac; done
printf '%s\n' "$unit" >> "$IGOR_FAKE_UNIT_STATE/launches"
printf 'Running as unit: %s; invocation ID: 0123456789abcdef0123456789abcdef\n' "$unit"
"#,
    )?;
    let ctl = bin.join("systemctl");
    fs::write(
        &ctl,
        r#"#!/bin/sh
set -eu
case "$2" in
  show)
    if [ -f "$IGOR_FAKE_UNIT_STATE/stopped" ]; then
      printf 'Result=success\nExecMainCode=0\nExecMainStatus=0\nLoadState=not-found\nActiveState=inactive\nSubState=dead\nInvocationID=\n'
    elif [ -f "$IGOR_FAKE_UNIT_STATE/finished" ]; then
      printf 'Result=success\nExecMainCode=1\nExecMainStatus=0\nLoadState=loaded\nActiveState=active\nSubState=exited\nInvocationID=0123456789abcdef0123456789abcdef\n'
    else
      printf 'LoadState=loaded\nActiveState=activating\nSubState=start\nResult=\nExecMainCode=0\nExecMainStatus=0\nInvocationID=0123456789abcdef0123456789abcdef\n'
    fi ;;
  stop) touch "$IGOR_FAKE_UNIT_STATE/stopped" ;;
  reset-failed) : ;;
  *) exit 64 ;;
esac
"#,
    )?;
    for file in [&run, &ctl] {
        fs::set_permissions(file, fs::Permissions::from_mode(0o700))?;
    }
    fixture.worker.stop()?;
    fixture.worker = spawn_fake_systemd_worker(&fixture.home, &fixture.project, &bin, &state)?;
    fixture.wait_until_worker_ready("fake systemd")?;
    let job_file = fixture._temporary.path().join("ambiguous-unit.toml");
    fs::write(
        &job_file,
        "schema_version = 1\nname = 'ambiguous unit'\n[execution]\nprogram = '/bin/true'\n[executor]\nkind = 'process'\n[executor.settings]\nisolation = 'systemd_user_unit'\n[resources]\nmode = 'shared'\n",
    )?;
    let submitted = output_json(
        fixture
            .run()
            .args([
                "submit",
                "--json",
                "--file",
                job_file.to_str().ok_or("non-UTF-8 unit fixture")?,
            ])
            .output()?,
    )?;
    let job_id: JobId = submitted["spec"]["id"]
        .as_str()
        .ok_or("missing unit job id")?
        .parse()?;
    let database = Database::open(fixture.home.join("state/igor/igor.sqlite3")).await?;
    let attempt_id = database
        .jobs()
        .detail(job_id)
        .await?
        .ok_or("missing unit job")?
        .attempts[0]
        .spec
        .id();
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if database
                .jobs()
                .unit_for_attempt(attempt_id)
                .await?
                .is_some_and(|unit| unit.state == "started")
            {
                return Ok::<_, Box<dyn Error>>(());
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await??;
    tokio::time::sleep(Duration::from_millis(250)).await;
    assert_eq!(
        database
            .jobs()
            .detail(job_id)
            .await?
            .ok_or("missing unit job")?
            .job
            .state,
        JobState::Running
    );
    fixture.worker.stop()?;
    fs::write(state.join("finished"), b"")?;
    fixture.worker = spawn_fake_systemd_worker(&fixture.home, &fixture.project, &bin, &state)?;
    fixture.wait_until_worker_ready("recovered fake systemd")?;
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if database
                .jobs()
                .unit_for_attempt(attempt_id)
                .await?
                .is_some_and(|unit| unit.state == "removed")
            {
                return Ok::<_, Box<dyn Error>>(());
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await??;
    assert_eq!(
        database
            .jobs()
            .detail(job_id)
            .await?
            .ok_or("missing recovered unit job")?
            .job
            .state,
        JobState::Succeeded
    );
    assert_eq!(
        fs::read_to_string(state.join("launches"))?.lines().count(),
        1
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn telegram_delivery_recovers_after_supervisor_restart_without_blocking_worker() -> TestResult
{
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpListener,
    };

    const TOKEN: &str = "123456:abcdefghijklmnopqrstuvwxyzABCDEFG";
    let _guard = worker_test_guard()?;
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let config = format!(
        "schema_version = 1\n[telegram]\nbot_token = '{TOKEN}'\nchat_id = '12345'\napi_base = 'http://{}'\n",
        listener.local_addr()?
    );
    let fixture = Fixture::with_global_config(Some(&config))?;
    preserve_raw_success_logs(&fixture)?;
    let server = tokio::spawn(async move {
        const MAX_HEADER_BYTES: usize = 64 * 1024;
        const MAX_BODY_BYTES: usize = 64 * 1024;
        let mut original_text: Option<String> = None;
        loop {
            let (mut stream, _) = listener.accept().await?;
            // Accumulate the full request head before touching the body so a
            // partial read can never masquerade as a complete Telegram call.
            let mut request: Vec<u8> = Vec::new();
            let header_end = loop {
                if let Some(found) = request.windows(4).position(|window| window == b"\r\n\r\n") {
                    if found + 4 > MAX_HEADER_BYTES {
                        return Err(std::io::Error::other("Telegram headers exceeded limit"));
                    }
                    break found + 4;
                }
                if request.len() >= MAX_HEADER_BYTES {
                    return Err(std::io::Error::other("Telegram headers exceeded limit"));
                }
                let mut chunk = [0u8; 4096];
                let read = stream.read(&mut chunk).await?;
                if read == 0 {
                    return Err(std::io::Error::other(
                        "Telegram request ended before complete headers",
                    ));
                }
                request.extend_from_slice(&chunk[..read]);
            };
            let headers = String::from_utf8_lossy(&request[..header_end]).to_ascii_lowercase();
            if !headers.contains("/sendmessage") {
                return Err(std::io::Error::other("unexpected Telegram endpoint"));
            }
            let content_length = headers
                .lines()
                .filter_map(|line| line.split_once(':'))
                .filter(|(name, _)| name.trim() == "content-length")
                .filter_map(|(_, value)| value.trim().parse::<usize>().ok())
                .next()
                .ok_or_else(|| std::io::Error::other("Telegram request missing Content-Length"))?;
            if content_length > MAX_BODY_BYTES {
                return Err(std::io::Error::other("Telegram body exceeded limit"));
            }
            let mut chunk = [0u8; 4096];
            while request.len() - header_end < content_length {
                let read = stream.read(&mut chunk).await?;
                if read == 0 {
                    return Err(std::io::Error::other(
                        "Telegram request body ended before Content-Length",
                    ));
                }
                request.extend_from_slice(&chunk[..read]);
            }
            let payload: Value =
                serde_json::from_slice(&request[header_end..header_end + content_length])
                    .map_err(|_| std::io::Error::other("invalid Telegram JSON body"))?;
            let text = payload["text"]
                .as_str()
                .ok_or_else(|| std::io::Error::other("Telegram message missing text"))?;
            let original_retry = original_text.as_deref() == Some(text);
            let response = if original_text.is_none() {
                original_text = Some(text.to_owned());
                "HTTP/1.1 429 Too Many Requests\r\nConnection: close\r\n\r\n{\"ok\":false,\"parameters\":{\"retry_after\":2}}"
            } else {
                "HTTP/1.1 200 OK\r\nConnection: close\r\n\r\n{\"ok\":true}"
            };
            // The fixture keeps the token out of every response: only an
            // explicit JSON content type/length header pair is added.
            let (head, body) = response
                .split_once("\r\n\r\n")
                .ok_or_else(|| std::io::Error::other("malformed Telegram fixture response"))?;
            let reply = format!(
                "{head}\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{body}",
                body.len()
            );
            stream.write_all(reply.as_bytes()).await?;
            // Unrelated notifications succeed without consuming the original
            // message's retry response or closing its endpoint before restart.
            if original_retry {
                break;
            }
        }
        Ok::<_, std::io::Error>(())
    });
    let spawn_supervisor = || -> Result<Worker, Box<dyn Error>> {
        Ok(Worker(
            command(&fixture.home, &fixture.project)
                .arg("supervisor")
                .stdin(Stdio::null())
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .spawn()?,
        ))
    };
    let mut supervisor = spawn_supervisor()?;
    wait_for_marker(&fixture.home.join("runtime/igor/supervisor.sock"))?;
    let submit = |program: &str| -> Result<JobId, Box<dyn Error>> {
        let output = output_json(
            fixture
                .run()
                .args(["submit", "--json", "--", program])
                .output()?,
        )?;
        Ok(output["spec"]["id"]
            .as_str()
            .ok_or("missing notification job")?
            .parse()?)
    };
    let job_id = submit("/bin/true")?;
    let database = Database::open(fixture.home.join("state/igor/igor.sqlite3")).await?;
    let delivery_id: String = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let row: Option<(String, String, Option<String>)> = sqlx::query_as(
                "SELECT id, state, last_error FROM deliveries WHERE idempotency_key IN
                 (SELECT 'telegram:action:' || id FROM actions WHERE job_id = ?)",
            )
            .bind(job_id.to_string())
            .fetch_optional(database.pool())
            .await?;
            if let Some((id, state, error)) = row
                && state == "pending"
                && error.as_deref() == Some("telegram_rate_limited")
            {
                return Ok::<_, Box<dyn Error>>(id);
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .map_err(|_| "Telegram delivery did not record rate limiting within 10 seconds")??;
    // Keep the original retry pending until the deliberate supervisor restart.
    sqlx::query("UPDATE deliveries SET available_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now', '+60 seconds') WHERE id = ?")
        .bind(&delivery_id).execute(database.pool()).await?;
    // A rate-limited notification endpoint cannot hold the worker queue.
    let next_job = submit("/usr/bin/env")?;
    let next = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if let Some(job) = database.jobs().get_job(next_job).await?
                && job.state == JobState::Succeeded
            {
                return Ok::<_, Box<dyn Error>>(());
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .map_err(|_| "next worker job did not finish during Telegram retry")?;
    next?;
    let logs = database.jobs().logs_for_job(next_job).await?;
    let stdout = logs.stdout_path.ok_or("missing environment job log")?;
    assert!(!fs::read_to_string(stdout)?.contains(TOKEN));
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let delivered: bool = sqlx::query_scalar(
                "SELECT EXISTS(SELECT 1 FROM deliveries WHERE state = 'delivered'
                 AND idempotency_key IN
                 (SELECT 'telegram:action:' || id FROM actions WHERE job_id = ?))",
            )
            .bind(next_job.to_string())
            .fetch_one(database.pool())
            .await?;
            if delivered {
                return Ok::<_, Box<dyn Error>>(());
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .map_err(|_| "next job notification was not delivered during the original retry")??;
    let original_state: String = sqlx::query_scalar("SELECT state FROM deliveries WHERE id = ?")
        .bind(&delivery_id)
        .fetch_one(database.pool())
        .await?;
    assert_eq!(original_state, "pending");
    assert!(
        !server.is_finished(),
        "Telegram mock closed after an unrelated job notification"
    );
    supervisor.stop()?;
    sqlx::query("UPDATE deliveries SET available_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now', '-1 second') WHERE id = ?")
        .bind(&delivery_id).execute(database.pool()).await?;
    supervisor = spawn_supervisor()?;
    let recovered = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let state: String = sqlx::query_scalar("SELECT state FROM deliveries WHERE id = ?")
                .bind(&delivery_id)
                .fetch_one(database.pool())
                .await?;
            if state == "delivered" {
                return Ok::<_, Box<dyn Error>>(());
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await;
    if recovered.is_err() {
        let row: (String, i64, String, Option<String>) = sqlx::query_as(
            "SELECT state, attempts, available_at, claim_id FROM deliveries WHERE id = ?",
        )
        .bind(&delivery_id)
        .fetch_one(database.pool())
        .await?;
        return Err(format!(
            "Telegram delivery not recovered: state={}, attempts={}, available_at={}, claimed={}, supervisor_exit={:?}, server_finished={}",
            row.0, row.1, row.2, row.3.is_some(), supervisor.0.try_wait()?, server.is_finished()
        ).into());
    }
    recovered??;
    let error: Option<String> =
        sqlx::query_scalar("SELECT last_error FROM deliveries WHERE id = ?")
            .bind(&delivery_id)
            .fetch_one(database.pool())
            .await?;
    assert!(!error.unwrap_or_default().contains(TOKEN));
    server.await??;
    supervisor.stop()?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn failed_process_persists_bounded_binary_log_tails() -> TestResult {
    let _guard = worker_test_guard()?;
    let fixture = Fixture::new()?;
    fs::write(
        fixture.project.join(".igor/project.toml"),
        "schema_version = 1\n[retention]\nfailed_log_tail_kb = 1\n",
    )?;
    git(&fixture.project, &["add", ".igor/project.toml"])?;
    git(
        &fixture.project,
        &["commit", "-qm", "retain one kilobyte of failed logs"],
    )?;

    let payload = fixture._temporary.path().join("failed-tail-input.bin");
    let mut payload_bytes: Vec<u8> = vec![b'A'; 1_600];
    payload_bytes.extend_from_slice(b"\0\xfftail-of-failed-log");
    fs::write(&payload, &payload_bytes)?;

    let job_file = fixture._temporary.path().join("failed-tail-job.toml");
    let script = format!(
        "cat '{}'; cat '{}' >&2; exit 7",
        payload.display(),
        payload.display()
    );
    fs::write(
        &job_file,
        format!(
            "schema_version = 1\nname = 'failed tail'\n[execution]\nprogram = '/bin/sh'\n\
             args = ['-c', {}]\n[resources]\nmode = 'shared'\n",
            serde_json::to_string(&script)?
        ),
    )?;
    let submitted = output_json(
        fixture
            .run()
            .args([
                "submit",
                "--json",
                "--file",
                job_file.to_str().ok_or("non-UTF-8 failed-tail job")?,
            ])
            .output()?,
    )?;
    let job_id: JobId = submitted["spec"]["id"]
        .as_str()
        .ok_or("missing failed-tail job id")?
        .parse()?;

    let waited = fixture
        .run()
        .args(["wait", &job_id.to_string(), "--json"])
        .output()?;
    assert_eq!(waited.status.code(), Some(1));
    let waited: Value = serde_json::from_slice(&waited.stdout)?;
    assert_eq!(waited["job"]["state"], "failed");
    let attempt_id: AttemptId = waited["attempts"][0]["spec"]["id"]
        .as_str()
        .ok_or("missing failed-tail attempt id")?
        .parse()?;

    let database = Database::open(fixture.home.join("state/igor/igor.sqlite3")).await?;
    let tails = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let row: Option<(Vec<u8>, Vec<u8>, i64, i64)> = sqlx::query_as(
                "SELECT stdout, stderr, limit_kb, complete FROM failed_log_tails
                 WHERE attempt_id = ?",
            )
            .bind(attempt_id.to_string())
            .fetch_optional(database.pool())
            .await?;
            if row.is_some() {
                return Ok::<_, Box<dyn Error>>(row);
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .map_err(|_| "failed log tails were not persisted")??;
    let (stdout, stderr, limit_kb, complete) = tails.ok_or("missing failed log tails row")?;

    let expected: Vec<u8> = payload_bytes[payload_bytes.len() - 1024..].to_vec();
    assert_eq!(limit_kb, 1);
    assert_eq!(complete, 1);
    assert_eq!(stdout, expected);
    assert_eq!(stderr, expected);
    assert!(stdout.contains(&0xff));

    let process = database
        .jobs()
        .process_for_attempt(attempt_id)
        .await?
        .ok_or("missing durable process record")?;
    assert_eq!(fs::read(&process.stdout_path)?, payload_bytes);
    assert_eq!(fs::read(&process.stderr_path)?, payload_bytes);
    assert!(fs::read(&process.stdout_path)?.len() > 1024);
    Ok(())
}
