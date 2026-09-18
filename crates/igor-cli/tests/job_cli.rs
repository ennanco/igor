use std::{
    error::Error,
    fs,
    os::unix::fs::PermissionsExt,
    path::Path,
    process::{Child, Command, Stdio},
    thread,
    time::{Duration, Instant},
};

use igor_core::{AttemptId, Database, JobId, JobState, ProcessRecord, TransitionState};
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
        Err("restarted worker did not become ready".into())
    }
}

fn output_json(output: std::process::Output) -> Result<Value, Box<dyn Error>> {
    if !output.status.success() {
        return Err(String::from_utf8_lossy(&output.stderr).into_owned().into());
    }
    Ok(serde_json::from_slice(&output.stdout)?)
}

fn process_fixture(name: &str) -> std::path::PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/process")
        .join(name)
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
