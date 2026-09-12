use std::{
    error::Error,
    fs,
    path::Path,
    process::{Child, Command, Stdio},
    thread,
    time::Duration,
};

use igor_core::{Database, Event, EventId, EventKind, EventPayload, JobId, JobState};
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
    _worker: Worker,
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
                        _worker: worker,
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
    assert_eq!(events.as_array().map(Vec::len), Some(2));
    let human = fixture.run().args(["show", job_id]).output()?;
    assert!(human.status.success());
    let human = String::from_utf8(human.stdout)?;
    assert!(human.contains(job_id));
    assert!(human.contains("queued"));
    let listed = output_json(fixture.run().args(["list", "--json"]).output()?)?;
    assert_eq!(listed[0]["spec"]["id"], job_id);

    let shell = output_json(
        fixture
            .run()
            .args(["submit", "--shell", "printf ok | tee result", "--json"])
            .output()?,
    )?;
    let shell_id = shell["spec"]["id"].as_str().ok_or("missing shell job id")?;
    let shell = output_json(fixture.run().args(["show", shell_id, "--json"]).output()?)?;
    assert_eq!(shell["job"]["spec"]["command"]["program"], "/bin/sh");
    assert_eq!(
        shell["job"]["spec"]["command"]["args"],
        json!(["-c", "printf ok | tee result"])
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
    let fixture = Fixture::new()?;
    let submitted = output_json(
        fixture
            .run()
            .args(["submit", "--json", "--", "true"])
            .output()?,
    )?;
    let job_id: JobId = submitted["spec"]["id"]
        .as_str()
        .ok_or("missing job id")?
        .parse()?;
    let database = Database::open(fixture.home.join("state/igor/igor.sqlite3")).await?;
    let event = Event::new(
        EventId::new(),
        EventKind::JobStateChanged,
        EventPayload::new(EventKind::JobStateChanged, 1, json!({"state": "cancelled"}))?,
    )?;
    database
        .jobs()
        .transition_job(job_id, JobState::Cancelled, &event)
        .await?;
    let waited = fixture
        .run()
        .args(["wait", &job_id.to_string(), "--json"])
        .output()?;
    assert_eq!(waited.status.code(), Some(1));
    let waited: Value = serde_json::from_slice(&waited.stdout)?;
    assert_eq!(waited["job"]["state"], "cancelled");
    Ok(())
}
