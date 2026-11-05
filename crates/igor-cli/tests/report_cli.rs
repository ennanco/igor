use std::{
    error::Error,
    fs,
    path::{Path, PathBuf},
    process::{Child, Command, Output, Stdio},
    thread,
    time::{Duration, Instant},
};

use igor_core::{
    Database, ReportActionSpec, ReportOrigin, ReportRequestReceipt, ReportRetryReceipt,
};
use serde_json::Value;

type TestResult = Result<(), Box<dyn Error>>;

#[path = "report/acceptance.rs"]
mod acceptance;

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

struct Daemon(Child);
impl Daemon {
    fn stop(&mut self) -> TestResult {
        if self.0.try_wait()?.is_none() {
            let _ = nix::sys::signal::kill(
                nix::unistd::Pid::from_raw(i32::try_from(self.0.id())?),
                nix::sys::signal::Signal::SIGTERM,
            );
            let deadline = Instant::now() + Duration::from_secs(5);
            while self.0.try_wait()?.is_none() && Instant::now() < deadline {
                thread::sleep(Duration::from_millis(20));
            }
            if self.0.try_wait()?.is_none() {
                self.0.kill()?;
            }
            self.0.wait()?;
        }
        Ok(())
    }
}
impl Drop for Daemon {
    fn drop(&mut self) {
        let _ = self.stop();
    }
}
struct Gate(PathBuf);
impl Drop for Gate {
    fn drop(&mut self) {
        let _ = fs::write(&self.0, b"ready");
    }
}

fn successful(output: Output) -> Result<Value, Box<dyn Error>> {
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    Ok(serde_json::from_slice(&output.stdout)?)
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
    assert!(status.success());
    Ok(())
}

fn project(home: &Path, root: &Path, gated: bool) -> TestResult {
    let output = command(home, root.parent().ok_or("project parent")?)
        .arg("init")
        .arg(root)
        .output()?;
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let path = root.join(".igor/project.toml");
    fs::write(
        &path,
        fs::read_to_string(&path)?.replace("enabled = false", "enabled = true"),
    )?;
    fs::write(root.join(".gitignore"), "RESULTS.md\nallow\n")?;
    let execution = if gated {
        "program = '/bin/sh'\nargs = ['-c', 'i=0; while [ ! -f allow ] && [ \"$i\" -lt 1200 ]; do i=$((i+1)); /usr/bin/sleep 0.05; done; test -f allow']"
    } else {
        "program = '/usr/bin/true'"
    };
    fs::write(
        root.join("family.toml"),
        format!(
            "schema_version = 1\nname = 'report family'\n[execution]\n{execution}\n[[members]]\nseed = 1\nargs = ['seed-one']\n[[members]]\nseed = 2\nargs = ['seed-two']\n"
        ),
    )?;
    git(root, &["init", "-q"])?;
    git(root, &["add", "."])?;
    git(root, &["commit", "-qm", "report CLI fixture"])?;
    Ok(())
}

fn spawn(home: &Path, cwd: &Path, role: &str) -> Result<Daemon, Box<dyn Error>> {
    Ok(Daemon(
        command(home, cwd)
            .arg(role)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()?,
    ))
}

fn wait(mut condition: impl FnMut() -> Result<bool, Box<dyn Error>>) -> TestResult {
    let deadline = Instant::now() + Duration::from_secs(20);
    while !condition()? {
        if Instant::now() >= deadline {
            return Err("report CLI fixture timed out".into());
        }
        thread::sleep(Duration::from_millis(20));
    }
    Ok(())
}

#[test]
fn report_generate_and_retry_cli_keep_requests_and_experiments_independent() -> TestResult {
    let temp = tempfile::tempdir()?;
    let home = temp.path().join("home");
    let root = temp.path().join("project");
    let missing = command(&home, temp.path())
        .args([
            "report",
            "generate",
            &igor_core::FamilyId::new().to_string(),
            "--json",
        ])
        .output()?;
    assert_eq!(missing.status.code(), Some(3));
    let malformed = command(&home, temp.path())
        .args(["report", "generate", "not-an-id"])
        .output()?;
    assert_eq!(malformed.status.code(), Some(2));
    let help = command(&home, temp.path())
        .args(["report", "generate", "--help"])
        .output()?;
    assert!(help.status.success());
    let help = String::from_utf8(help.stdout)?;
    assert!(help.contains("supervisor"));
    assert!(help.contains("--project"));
    let retry_help = command(&home, temp.path())
        .args(["report", "retry", "--help"])
        .output()?;
    assert!(retry_help.status.success());
    assert!(String::from_utf8(retry_help.stdout)?.contains("--project"));
    project(&home, &root, true)?;
    let mut worker = spawn(&home, temp.path(), "worker")?;
    let _gate = Gate(root.join("allow"));
    wait(|| {
        assert!(worker.0.try_wait()?.is_none(), "worker exited");
        Ok(command(&home, temp.path())
            .args(["project", "list", "--json"])
            .output()?
            .status
            .success())
    })?;
    let registered = successful(
        command(&home, temp.path())
            .args(["project", "add"])
            .arg(&root)
            .arg("--json")
            .output()?,
    )?;
    let family = successful(
        command(&home, &root)
            .args(["family", "submit", "--file", "family.toml", "--json"])
            .output()?,
    )?;
    let family_id = family["family_id"].as_str().ok_or("family id")?;
    let incomplete = command(&home, &root)
        .args(["report", "generate", family_id, "--json"])
        .output()?;
    assert_eq!(incomplete.status.code(), Some(9));
    assert!(!root.join("RESULTS.md").exists());
    assert_eq!(
        command(&home, &root)
            .args(["report", "retry", family_id, "--json"])
            .output()?
            .status
            .code(),
        Some(9)
    );
    fs::write(root.join("allow"), "ready")?;
    wait(|| {
        let detail = successful(
            command(&home, &root)
                .args(["family", "show", family_id, "--json"])
                .output()?,
        )?;
        Ok(detail["generations"]
            .as_array()
            .and_then(|generations| generations.last())
            .is_some_and(|generation| generation["status"] == "comparable"))
    })?;
    let config_path = root.join(".igor/project.toml");
    let enabled = fs::read_to_string(&config_path)?;
    fs::write(
        &config_path,
        enabled.replace("enabled = true", "enabled = false"),
    )?;
    let disabled = command(&home, &root)
        .args(["report", "generate", family_id, "--json"])
        .output()?;
    assert_eq!(disabled.status.code(), Some(9));
    assert!(String::from_utf8_lossy(&disabled.stderr).contains("disabled"));
    fs::write(&config_path, &enabled)?;
    assert_eq!(
        command(&home, &root)
            .args(["report", "retry", family_id, "--json"])
            .output()?
            .status
            .code(),
        Some(8)
    );
    let unknown = command(&home, temp.path())
        .args([
            "report",
            "generate",
            &igor_core::FamilyId::new().to_string(),
            "--json",
        ])
        .output()?;
    assert_eq!(unknown.status.code(), Some(8));
    let receipt_value = successful(
        command(&home, &root)
            .args(["report", "generate", family_id, "--json"])
            .output()?,
    )?;
    let receipt: ReportRequestReceipt = serde_json::from_value(receipt_value)?;
    assert_eq!(receipt.state, igor_core::ActionState::Pending);
    assert_eq!(
        receipt.project_id.to_string(),
        registered["id"].as_str().ok_or("project id")?
    );
    assert_eq!(
        receipt.generation.id.to_string(),
        family["generation_id"].as_str().ok_or("generation id")?
    );
    assert_eq!(receipt.output, root.join("RESULTS.md"));
    assert!(!receipt.output.exists());
    let human = command(&home, temp.path())
        .args(["report", "generate", family_id])
        .output()?;
    assert!(
        human.status.success(),
        "{}",
        String::from_utf8_lossy(&human.stderr)
    );
    assert!(String::from_utf8(human.stdout)?.contains("Execution: supervisor"));
    let other = temp.path().join("other-project");
    project(&home, &other, false)?;
    successful(
        command(&home, temp.path())
            .args(["project", "add"])
            .arg(&other)
            .arg("--json")
            .output()?,
    )?;
    for cwd in [other.as_path(), temp.path()] {
        let mut scoped = command(&home, cwd);
        scoped.args(["report", "generate", family_id, "--json"]);
        if cwd == temp.path() {
            scoped.arg("--project").arg(&other);
        }
        assert_eq!(scoped.output()?.status.code(), Some(8));
    }
    let explicit = successful(
        command(&home, temp.path())
            .args(["report", "generate", family_id, "--json", "--project"])
            .arg(&root)
            .output()?,
    )?;
    assert_eq!(explicit["project_id"], registered["id"]);
    assert!(!other.join("RESULTS.md").exists());
    let runtime = tokio::runtime::Runtime::new()?;
    let database = runtime.block_on(Database::open(home.join("state/igor/igor.sqlite3")))?;
    let spec: String = runtime.block_on(
        sqlx::query_scalar("SELECT spec_json FROM actions WHERE id = ?")
            .bind(receipt.action_id.to_string())
            .fetch_one(database.pool()),
    )?;
    assert_eq!(
        serde_json::from_str::<ReportActionSpec>(&spec)?.origin,
        ReportOrigin::Manual {
            request_id: receipt.action_id
        }
    );
    let state_dir = home.join("state/igor");
    let runtime_dir = home.join("runtime/igor");
    let paths = igor_core::RuntimePaths {
        config_file: home.join("config/igor/config.toml"),
        database: state_dir.join("igor.sqlite3"),
        log_dir: state_dir.join("logs"),
        worktree_dir: state_dir.join("worktrees"),
        report_dir: state_dir.join("reports"),
        worker_socket: runtime_dir.join("worker.sock"),
        supervisor_socket: runtime_dir.join("supervisor.sock"),
        resource_dir: runtime_dir.join("resources"),
        state_dir,
        runtime_dir,
    };
    fs::write(&config_path, "temporarily invalid policy")?;
    let replay = runtime.block_on(igor_daemon::Client::new(&paths).request(
        igor_daemon::DaemonRole::Worker,
        igor_daemon::Request::ReportGenerate {
            family_id: receipt.family_id,
            project_id: Some(receipt.project_id),
            request_id: receipt.action_id,
        },
    ))?;
    assert_eq!(
        replay,
        igor_daemon::Response::ReportRequested(receipt.clone())
    );
    fs::write(&config_path, &enabled)?;
    let mut supervisor = spawn(&home, temp.path(), "supervisor")?;
    wait(|| {
        assert!(supervisor.0.try_wait()?.is_none(), "supervisor exited");
        let state: String = runtime.block_on(
            sqlx::query_scalar("SELECT state FROM actions WHERE id = ?")
                .bind(receipt.action_id.to_string())
                .fetch_one(database.pool()),
        )?;
        Ok(state == "succeeded")
    })?;
    let managed = fs::read_to_string(&receipt.output)?;
    assert!(managed.contains("Status: **Ready**"));
    wait(|| {
        Ok(runtime.block_on(sqlx::query_scalar::<_, i64>("SELECT count(*) FROM actions WHERE kind = 'generate_report' AND idempotency_key LIKE 'generate_report:v1:family-completed:%'")
            .fetch_one(database.pool()))? == 1)
    })?;
    supervisor.stop()?;
    // A new explicit request must preserve manual edits and cannot change experiment outcome.
    let mut notes = fs::read_to_string(&receipt.output)?;
    notes.push_str("\nMy handwritten notes\n");
    fs::write(&receipt.output, &notes)?;
    let edited: ReportRequestReceipt = serde_json::from_value(successful(
        command(&home, temp.path())
            .args(["report", "generate", family_id, "--json"])
            .output()?,
    )?)?;
    runtime.block_on(
        sqlx::query("UPDATE actions SET attempts = 2 WHERE id = ?")
            .bind(edited.action_id.to_string())
            .execute(database.pool()),
    )?;
    let mut supervisor = spawn(&home, temp.path(), "supervisor")?;
    wait(|| {
        assert!(supervisor.0.try_wait()?.is_none());
        let state: String = runtime.block_on(
            sqlx::query_scalar("SELECT state FROM actions WHERE id = ?")
                .bind(edited.action_id.to_string())
                .fetch_one(database.pool()),
        )?;
        Ok(state == "failed")
    })?;
    assert_eq!(fs::read_to_string(&receipt.output)?, notes);
    supervisor.stop()?;
    fs::write(
        &config_path,
        enabled.replace("enabled = true", "enabled = false"),
    )?;
    assert_eq!(
        command(&home, temp.path())
            .args(["report", "retry", family_id, "--json"])
            .output()?
            .status
            .code(),
        Some(9)
    );
    fs::write(&config_path, &enabled)?;
    assert_eq!(
        command(&home, &other)
            .args(["report", "retry", family_id, "--json"])
            .output()?
            .status
            .code(),
        Some(8)
    );
    let original: (String, String) = runtime.block_on(
        sqlx::query_as("SELECT spec_json, idempotency_key FROM actions WHERE id = ?")
            .bind(edited.action_id.to_string())
            .fetch_one(database.pool()),
    )?;
    let human = command(&home, temp.path())
        .args(["report", "retry", family_id])
        .output()?;
    assert!(
        human.status.success(),
        "{}",
        String::from_utf8_lossy(&human.stderr)
    );
    assert!(String::from_utf8(human.stdout)?.contains("Report retry request"));
    let pending: (String, i64) = runtime.block_on(
        sqlx::query_as("SELECT state, attempts FROM actions WHERE id = ?")
            .bind(edited.action_id.to_string())
            .fetch_one(database.pool()),
    )?;
    assert_eq!(pending, ("pending".into(), 0));
    // Retrying never bypasses manuscript protection.
    runtime.block_on(
        sqlx::query("UPDATE actions SET attempts = 2 WHERE id = ?")
            .bind(edited.action_id.to_string())
            .execute(database.pool()),
    )?;
    let mut supervisor = spawn(&home, temp.path(), "supervisor")?;
    wait(|| {
        let state: String = runtime.block_on(
            sqlx::query_scalar("SELECT state FROM actions WHERE id = ?")
                .bind(edited.action_id.to_string())
                .fetch_one(database.pool()),
        )?;
        Ok(state == "failed")
    })?;
    supervisor.stop()?;
    assert_eq!(fs::read_to_string(&receipt.output)?, notes);
    // User restores the original generated bytes; retry reuses the same action and policy.
    fs::write(&receipt.output, &managed)?;
    let retried: ReportRetryReceipt = serde_json::from_value(successful(
        command(&home, temp.path())
            .args(["report", "retry", family_id, "--json", "--project"])
            .arg(&root)
            .output()?,
    )?)?;
    assert_eq!(retried.report.action_id, edited.action_id);
    assert_eq!(retried.report.generation, edited.generation);
    assert_eq!(retried.report.state, igor_core::ActionState::Pending);
    assert_eq!(
        command(&home, temp.path())
            .args(["report", "retry", family_id, "--json"])
            .output()?
            .status
            .code(),
        Some(9)
    );
    fs::write(&config_path, "temporarily invalid policy")?;
    let replay = runtime.block_on(igor_daemon::Client::new(&paths).request(
        igor_daemon::DaemonRole::Worker,
        igor_daemon::Request::ReportRetry {
            family_id: receipt.family_id,
            project_id: Some(receipt.project_id),
            request_id: retried.request_id,
        },
    ))?;
    assert_eq!(
        replay,
        igor_daemon::Response::ReportRetried(retried.clone())
    );
    fs::write(&config_path, &enabled)?;
    let unchanged: (String, String) = runtime.block_on(
        sqlx::query_as("SELECT spec_json, idempotency_key FROM actions WHERE id = ?")
            .bind(edited.action_id.to_string())
            .fetch_one(database.pool()),
    )?;
    assert_eq!(unchanged, original);
    let mut supervisor = spawn(&home, temp.path(), "supervisor")?;
    wait(|| {
        let state: String = runtime.block_on(
            sqlx::query_scalar("SELECT state FROM actions WHERE id = ?")
                .bind(edited.action_id.to_string())
                .fetch_one(database.pool()),
        )?;
        Ok(state == "succeeded")
    })?;
    assert_eq!(fs::read_to_string(&receipt.output)?, managed);
    assert_eq!(
        command(&home, temp.path())
            .args(["report", "retry", family_id, "--json"])
            .output()?
            .status
            .code(),
        Some(9)
    );
    assert_eq!(
        runtime.block_on(
            sqlx::query_scalar::<_, i64>("SELECT count(*) FROM attempts")
                .fetch_one(database.pool())
        )?,
        2
    );
    assert_eq!(
        runtime.block_on(
            sqlx::query_scalar::<_, i64>("SELECT count(*) FROM jobs WHERE state != 'succeeded'")
                .fetch_one(database.pool())
        )?,
        0
    );
    supervisor.stop()?;
    worker.stop()?;
    Ok(())
}
