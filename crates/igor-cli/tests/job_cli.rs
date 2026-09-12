use std::{
    error::Error,
    fs,
    os::unix::fs::PermissionsExt,
    path::Path,
    process::{Child, Command, Stdio},
    thread,
    time::{Duration, Instant},
};

use igor_core::{AttemptId, Database, JobId, JobState};
use nix::{
    sys::signal::{Signal, kill},
    unistd::Pid,
};
use serde_json::{Value, json};
use tempfile::TempDir;

type TestResult = Result<(), Box<dyn Error>>;

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
        let worker = Worker(
            command(&home, &project)
                .env("TELEGRAM_BOT_TOKEN", "must-not-leak")
                .env("OPENCODE_TEST_SECRET", "must-not-leak")
                .arg("worker")
                .stdin(Stdio::null())
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .spawn()?,
        );
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
}

fn output_json(output: std::process::Output) -> Result<Value, Box<dyn Error>> {
    if !output.status.success() {
        return Err(String::from_utf8_lossy(&output.stderr).into_owned().into());
    }
    Ok(serde_json::from_slice(&output.stdout)?)
}

#[test]
fn project_submission_and_reads_preserve_argument_contracts() -> TestResult {
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
        JobState::Failed
    );
    let interrupted = database
        .jobs()
        .process_for_attempt(observed_attempt_id)
        .await?
        .ok_or("missing interrupted process")?;
    assert_eq!(interrupted.term_signal, Some(9));
    assert_eq!(
        interrupted.error.as_deref(),
        Some("worker shutdown interrupted execution")
    );
    Ok(())
}
