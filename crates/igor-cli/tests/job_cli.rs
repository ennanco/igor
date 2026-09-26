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
    assert!(cancelled.status.success());
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
    assert!(
        fixture
            .run()
            .args(["cancel", &job_id.to_string(), "--grace-seconds", "1"])
            .output()?
            .status
            .success()
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
    let container = database
        .jobs()
        .container_for_attempt(running.attempt_id)
        .await?
        .ok_or("missing timed-out Docker container")?;
    assert_eq!(container.state, DockerContainerState::Removed);
    assert_eq!(container.exit_code, Some(137));
    assert_eq!(
        container.error.as_deref(),
        Some("docker_timeout: configured execution timeout elapsed")
    );
    let invocations = fs::read_to_string(invocation_log)?;
    assert!(invocations.contains("kill fake-container"));
    assert!(!invocations.contains("stop --time "));
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

    let submitted = output_json(
        fixture
            .run()
            .args([
                "submit",
                "--json",
                "--",
                "/bin/sh",
                timeout.to_str().ok_or("non-UTF-8 sleep fixture")?,
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
    assert!(
        fixture
            .run()
            .args([
                "cancel",
                &observed_job_id.to_string(),
                "--grace-seconds",
                "0"
            ])
            .output()?
            .status
            .success()
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
    let server = tokio::spawn(async move {
        for response in [
            "HTTP/1.1 429 Too Many Requests\r\nConnection: close\r\n\r\n{\"ok\":false,\"parameters\":{\"retry_after\":2}}",
            "HTTP/1.1 200 OK\r\nConnection: close\r\n\r\n{\"ok\":true}",
        ] {
            let (mut stream, _) = listener.accept().await?;
            let mut buffer = [0u8; 2048];
            let read = stream.read(&mut buffer).await?;
            if !String::from_utf8_lossy(&buffer[..read]).contains("/sendMessage") {
                return Err(std::io::Error::other("unexpected Telegram endpoint"));
            }
            stream.write_all(response.as_bytes()).await?;
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
    .await??;
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
    .await?;
    next?;
    let logs = database.jobs().logs_for_job(next_job).await?;
    let stdout = logs.stdout_path.ok_or("missing environment job log")?;
    assert!(!fs::read_to_string(stdout)?.contains(TOKEN));
    supervisor.stop()?;
    sqlx::query("UPDATE deliveries SET available_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now', '-1 second') WHERE id = ?")
        .bind(&delivery_id).execute(database.pool()).await?;
    supervisor = spawn_supervisor()?;
    tokio::time::timeout(Duration::from_secs(10), async {
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
    .await??;
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
