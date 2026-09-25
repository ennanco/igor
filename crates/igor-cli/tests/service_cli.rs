use std::{
    error::Error,
    fs,
    os::unix::{fs::PermissionsExt, net::UnixStream},
    path::{Path, PathBuf},
    process::Command,
    thread,
    time::{Duration, Instant},
};

type TestResult = Result<(), Box<dyn Error>>;

const UNITS: [&str; 2] = ["igor-worker.service", "igor-supervisor.service"];

struct LiveServiceCleanup {
    config_home: PathBuf,
    runtime_dir: PathBuf,
    state_home: PathBuf,
    release_marker: Option<PathBuf>,
}

impl Drop for LiveServiceCleanup {
    fn drop(&mut self) {
        if let Some(marker) = &self.release_marker
            && let Err(error) = fs::write(marker, b"release")
        {
            eprintln!("live systemd test cleanup could not release job: {error}");
        }
        let result = Command::new(env!("CARGO_BIN_EXE_igor"))
            .env_clear()
            .env("HOME", std::env::var_os("HOME").unwrap_or_default())
            .env("XDG_CONFIG_HOME", &self.config_home)
            .env("XDG_STATE_HOME", &self.state_home)
            .env("XDG_RUNTIME_DIR", &self.runtime_dir)
            .args(["service", "uninstall", "--user", "--yes"])
            .status();
        match result {
            Ok(status) if status.success() => {}
            Ok(status) => eprintln!("live systemd test cleanup uninstall failed: {status}"),
            Err(error) => eprintln!("live systemd test cleanup could not uninstall units: {error}"),
        }
    }
}

fn live_igor_command(config_home: &Path, state_home: &Path, runtime_dir: &Path) -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_igor"));
    command
        .env_clear()
        .env("HOME", std::env::var_os("HOME").unwrap_or_default())
        .env("PATH", std::env::var_os("PATH").unwrap_or_default())
        .env("XDG_CONFIG_HOME", config_home)
        .env("XDG_STATE_HOME", state_home)
        .env("XDG_RUNTIME_DIR", runtime_dir);
    command
}

fn systemctl_user(runtime_dir: &Path, args: &[&str]) -> std::io::Result<std::process::Output> {
    Command::new("systemctl")
        .env_clear()
        .env("HOME", std::env::var_os("HOME").unwrap_or_default())
        .env("PATH", std::env::var_os("PATH").unwrap_or_default())
        .env("XDG_RUNTIME_DIR", runtime_dir)
        .args(["--user"])
        .args(args)
        .output()
}

fn socket_is_active(path: &Path) -> bool {
    match UnixStream::connect(path) {
        Ok(_) => true,
        Err(error) => !matches!(
            error.kind(),
            std::io::ErrorKind::ConnectionRefused | std::io::ErrorKind::NotFound
        ),
    }
}

fn wait_for_states(
    config_home: &Path,
    state_home: &Path,
    runtime_dir: &Path,
    wanted: &str,
) -> TestResult {
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        let output = live_igor_command(config_home, state_home, runtime_dir)
            .args(["service", "status", "--json"])
            .output()?;
        if output.status.success() {
            let status: serde_json::Value = serde_json::from_slice(&output.stdout)?;
            if UNITS.iter().all(|unit| {
                status.as_array().is_some_and(|entries| {
                    entries
                        .iter()
                        .any(|entry| entry["unit"] == *unit && entry["active_state"] == wanted)
                })
            }) {
                return Ok(());
            }
        }
        if Instant::now() >= deadline {
            return Err(format!("user units did not reach {wanted} before timeout").into());
        }
        thread::sleep(Duration::from_millis(200));
    }
}

fn wait_for_job_state(
    config_home: &Path,
    state_home: &Path,
    runtime_dir: &Path,
    job_id: &str,
    wanted: &str,
) -> Result<serde_json::Value, Box<dyn Error>> {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let output = live_igor_command(config_home, state_home, runtime_dir)
            .args(["show", job_id, "--json"])
            .output()?;
        let output_status = output.status;
        let stdout = String::from_utf8_lossy(&output.stdout).into_owned();
        let stderr = String::from_utf8_lossy(&output.stderr).into_owned();
        if output.status.success() {
            let job: serde_json::Value = serde_json::from_slice(stdout.as_bytes())?;
            if job["job"]["state"] == wanted {
                return Ok(job);
            }
        }
        if Instant::now() >= deadline {
            let (status, stdout, stderr) = (output_status, stdout, stderr);
            let job: serde_json::Value = serde_json::from_str(&stdout).unwrap_or_default();
            let attempt = job["attempts"].get(0).unwrap_or(&serde_json::Value::Null);
            return Err(format!(
                "job {job_id} did not reach {wanted} before timeout; output status: {status}; last observed job.state: {}; attempts[0].state: {}; attempts[0].error: {}; stderr: {stderr}",
                job["job"]["state"],
                attempt["state"],
                attempt["error"],
            )
            .into());
        }
        thread::sleep(Duration::from_millis(200));
    }
}

#[test]
fn live_user_systemd_lifecycle_is_opt_in() -> TestResult {
    if std::env::var("IGOR_RUN_SYSTEMD_TESTS").as_deref() != Ok("1") {
        eprintln!("SKIP: set IGOR_RUN_SYSTEMD_TESTS=1 to run live user-systemd lifecycle test");
        return Ok(());
    }
    let Some(config_home) = std::env::var_os("XDG_CONFIG_HOME")
        .map(PathBuf::from)
        .or_else(|| std::env::var_os("HOME").map(|home| PathBuf::from(home).join(".config")))
    else {
        eprintln!("SKIP: HOME and XDG_CONFIG_HOME are unavailable");
        return Ok(());
    };
    let Some(runtime_dir) = std::env::var_os("XDG_RUNTIME_DIR").map(PathBuf::from) else {
        eprintln!("SKIP: XDG_RUNTIME_DIR is unavailable");
        return Ok(());
    };
    let unit_directory = config_home.join("systemd/user");
    if UNITS.iter().any(|unit| unit_directory.join(unit).exists()) {
        eprintln!("SKIP: preexisting Igor user unit file");
        return Ok(());
    }
    let manager = systemctl_user(&runtime_dir, &["show"]);
    if !matches!(manager, Ok(ref output) if output.status.success()) {
        eprintln!("SKIP: user systemd manager unavailable");
        return Ok(());
    }
    for unit in UNITS {
        let output = systemctl_user(
            &runtime_dir,
            &[
                "show",
                "--property=LoadState,ActiveState,UnitFileState",
                "--",
                unit,
            ],
        );
        let Ok(output) = output else {
            eprintln!("SKIP: could not inspect user unit state");
            return Ok(());
        };
        let state = String::from_utf8_lossy(&output.stdout);
        if !output.status.success()
            || !state.contains("LoadState=not-found")
            || state.contains("ActiveState=active")
            || state.contains("UnitFileState=enabled")
        {
            eprintln!("SKIP: preexisting Igor user systemd state for {unit}");
            return Ok(());
        }
    }
    let sockets = runtime_dir.join("igor");
    if socket_is_active(&sockets.join("worker.sock"))
        || socket_is_active(&sockets.join("supervisor.sock"))
    {
        eprintln!("SKIP: Igor worker or supervisor socket is active");
        return Ok(());
    }

    let temporary = tempfile::tempdir()?;
    let state_home = temporary.path().join("state");
    let mut cleanup = LiveServiceCleanup {
        config_home: config_home.clone(),
        runtime_dir: runtime_dir.clone(),
        state_home: state_home.clone(),
        release_marker: None,
    };
    let output = live_igor_command(&config_home, &state_home, &runtime_dir)
        .args(["service", "install", "--user"])
        .output()?;
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(UNITS.iter().all(|unit| unit_directory.join(unit).is_file()));

    let output = live_igor_command(&config_home, &state_home, &runtime_dir)
        .args(["service", "start"])
        .output()?;
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    wait_for_states(&config_home, &state_home, &runtime_dir, "active")?;

    let project = temporary.path().join("project");
    fs::create_dir_all(&project)?;
    let output = live_igor_command(&config_home, &state_home, &runtime_dir)
        .current_dir(&project)
        .args(["init"])
        .output()?;
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let output = Command::new("git")
        .arg("-C")
        .arg(&project)
        .args(["init", "-q"])
        .output()?;
    assert!(
        output.status.success(),
        "git init failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    for args in [
        vec![
            "config",
            "--local",
            "user.email",
            "igor-live-test@example.invalid",
        ],
        vec!["config", "--local", "user.name", "Igor live test"],
        vec!["add", "--all"],
        vec![
            "commit",
            "--allow-empty",
            "-m",
            "live systemd recovery test",
        ],
    ] {
        let output = Command::new("git")
            .arg("-C")
            .arg(&project)
            .args(args)
            .output()?;
        assert!(
            output.status.success(),
            "git failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
    let registration_deadline = Instant::now() + Duration::from_secs(10);
    let mut registration_stderr;
    loop {
        let output = live_igor_command(&config_home, &state_home, &runtime_dir)
            .current_dir(&project)
            .args(["project", "add", "."])
            .output()?;
        if output.status.success() {
            break;
        }
        registration_stderr = String::from_utf8_lossy(&output.stderr).into_owned();
        if Instant::now() >= registration_deadline {
            return Err(format!(
                "project registration did not succeed within 10 seconds; last stderr: {registration_stderr}"
            )
            .into());
        }
        thread::sleep(Duration::from_millis(50));
    }

    let started = temporary.path().join("started");
    let release = temporary.path().join("release");
    let finished = temporary.path().join("finished");
    cleanup.release_marker = Some(release.clone());
    let script = format!(
        "printf started > '{}'\nwhile [ ! -f '{}' ]; do /bin/sleep 0.1; done\nprintf finished > '{}'",
        started.display(),
        release.display(),
        finished.display()
    );
    let output = live_igor_command(&config_home, &state_home, &runtime_dir)
        .current_dir(&project)
        .args(["submit", "--json", "--", "/bin/sh", "-c", &script])
        .output()?;
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let submission: serde_json::Value = serde_json::from_slice(&output.stdout)?;
    let job_id = submission["spec"]["id"]
        .as_str()
        .ok_or("submit JSON did not include job id")?
        .to_owned();
    let running = wait_for_job_state(&config_home, &state_home, &runtime_dir, &job_id, "running")?;
    assert!(started.is_file(), "job did not write its started marker");
    assert_eq!(running["attempts"].as_array().map(Vec::len), Some(1));
    let attempt_id = running["attempts"][0]["spec"]["id"]
        .as_str()
        .ok_or("running job JSON did not include attempt spec id")?
        .to_owned();

    let output = live_igor_command(&config_home, &state_home, &runtime_dir)
        .args(["service", "restart"])
        .output()?;
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    wait_for_states(&config_home, &state_home, &runtime_dir, "active")?;
    thread::sleep(Duration::from_millis(500));
    assert!(
        !finished.exists(),
        "job finished before release after restart"
    );
    let after_restart =
        wait_for_job_state(&config_home, &state_home, &runtime_dir, &job_id, "running")?;
    assert_eq!(after_restart["attempts"][0]["spec"]["id"], attempt_id);
    assert_eq!(after_restart["attempts"].as_array().map(Vec::len), Some(1));

    fs::write(&release, b"release")?;
    // A recovered process's exit status is unobservable to the supervisor.
    let deadline = Instant::now() + Duration::from_secs(30);
    let terminal = loop {
        let output = live_igor_command(&config_home, &state_home, &runtime_dir)
            .args(["show", &job_id, "--json"])
            .output()?;
        let status = output.status;
        let stdout = String::from_utf8_lossy(&output.stdout).into_owned();
        let stderr = String::from_utf8_lossy(&output.stderr).into_owned();
        if output.status.success() {
            let job: serde_json::Value = serde_json::from_slice(stdout.as_bytes())?;
            let state = job["job"]["state"].as_str().unwrap_or_default();
            if matches!(state, "succeeded" | "lost") {
                break job;
            }
            if matches!(state, "cancelled" | "failed") {
                let attempt = job["attempts"].get(0).unwrap_or(&serde_json::Value::Null);
                return Err(format!(
                    "job {job_id} reached unexpected terminal state {state}; attempt state: {}; error: {}",
                    attempt["state"], attempt["error"],
                )
                .into());
            }
        }
        if Instant::now() >= deadline {
            let job: serde_json::Value = serde_json::from_str(&stdout).unwrap_or_default();
            let attempt = job["attempts"].get(0).unwrap_or(&serde_json::Value::Null);
            return Err(format!(
                "job {job_id} did not reach succeeded or lost before timeout; output status: {status}; last observed job.state: {}; attempts[0].state: {}; attempts[0].error: {}; stderr: {stderr}",
                job["job"]["state"],
                attempt["state"],
                attempt["error"],
            )
            .into());
        }
        thread::sleep(Duration::from_millis(200));
    };
    assert!(finished.is_file(), "job did not write its finished marker");
    assert_eq!(terminal["attempts"].as_array().map(Vec::len), Some(1));
    assert_eq!(terminal["attempts"][0]["spec"]["id"], attempt_id);
    assert!(matches!(
        terminal["job"]["state"].as_str(),
        Some("succeeded" | "lost")
    ));
    assert_ne!(terminal["job"]["state"], "cancelled");
    if terminal["job"]["state"] == "lost" {
        assert_eq!(terminal["attempts"][0]["state"], "lost");
    }
    let output = live_igor_command(&config_home, &state_home, &runtime_dir)
        .args(["service", "enable", "--now"])
        .output()?;
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let output = live_igor_command(&config_home, &state_home, &runtime_dir)
        .args(["service", "status", "--json"])
        .output()?;
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let enabled: serde_json::Value = serde_json::from_slice(&output.stdout)?;
    assert!(
        UNITS.iter().all(|unit| {
            enabled.as_array().is_some_and(|entries| {
                entries
                    .iter()
                    .any(|entry| entry["unit"] == *unit && entry["unit_file_state"] == "enabled")
            })
        }),
        "both units should be enabled: {enabled}"
    );

    let output = live_igor_command(&config_home, &state_home, &runtime_dir)
        .args(["service", "disable", "--now"])
        .output()?;
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let output = live_igor_command(&config_home, &state_home, &runtime_dir)
        .args(["service", "status", "--json"])
        .output()?;
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let disabled: serde_json::Value = serde_json::from_slice(&output.stdout)?;
    assert!(
        UNITS.iter().all(|unit| {
            disabled.as_array().is_some_and(|entries| {
                entries.iter().any(|entry| {
                    entry["unit"] == *unit
                        && entry["unit_file_state"] == "disabled"
                        && entry["active_state"] == "inactive"
                })
            })
        }),
        "both units should be disabled and inactive: {disabled}"
    );

    let output = live_igor_command(&config_home, &state_home, &runtime_dir)
        .args(["service", "uninstall", "--user", "--yes"])
        .output()?;
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(UNITS.iter().all(|unit| !unit_directory.join(unit).exists()));
    std::mem::forget(cleanup);
    Ok(())
}

fn command(home: &Path, fake_bin: &Path, log: &Path) -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_igor"));
    command
        .env_clear()
        .env("HOME", home)
        .env("XDG_CONFIG_HOME", home.join("config"))
        .env("PATH", fake_bin)
        .env("SYSTEMCTL_LOG", log);
    command
}

fn fake_journalctl(root: &Path, body: &str) -> Result<PathBuf, Box<dyn Error>> {
    let directory = root.join("bin");
    fs::create_dir_all(&directory)?;
    let path = directory.join("journalctl");
    fs::write(&path, format!("#!/bin/sh\n{body}\n"))?;
    fs::set_permissions(&path, fs::Permissions::from_mode(0o700))?;
    Ok(directory)
}

fn install_units(home: &Path) -> Result<PathBuf, Box<dyn Error>> {
    let directory = home.join("config/systemd/user");
    fs::create_dir_all(&directory)?;
    for name in UNITS {
        fs::write(directory.join(name), b"[Unit]\nDescription=Igor test\n")?;
    }
    Ok(directory)
}

fn fake_systemctl(root: &Path, body: &str) -> Result<PathBuf, Box<dyn Error>> {
    let directory = root.join("bin");
    fs::create_dir_all(&directory)?;
    let path = directory.join("systemctl");
    fs::write(&path, format!("#!/bin/sh\n{body}\n"))?;
    fs::set_permissions(&path, fs::Permissions::from_mode(0o700))?;
    Ok(directory)
}

#[test]
fn service_and_top_level_uninstall_remove_only_managed_user_units() -> TestResult {
    let temporary = tempfile::tempdir()?;
    let home = temporary.path().join("home");
    let log = temporary.path().join("systemctl.log");
    let fake_bin = fake_systemctl(
        temporary.path(),
        "printf '%s\\n' \"$*\" >> \"$SYSTEMCTL_LOG\"",
    )?;
    let unit_directory = install_units(&home)?;
    let marker = home.join("config/igor/config.toml");
    fs::create_dir_all(marker.parent().ok_or("missing marker parent")?)?;
    fs::write(&marker, b"schema_version = 1\n")?;

    let output = command(&home, &fake_bin, &log)
        .args(["service", "uninstall", "--user", "--yes"])
        .output()?;
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(UNITS.iter().all(|name| !unit_directory.join(name).exists()));
    assert!(marker.is_file());
    assert_eq!(
        fs::read_to_string(&log)?,
        "--user disable --now -- igor-worker.service igor-supervisor.service\n--user daemon-reload\n"
    );

    fs::write(unit_directory.join(UNITS[0]), b"[Unit]\n")?;
    fs::write(&log, b"")?;
    let output = command(&home, &fake_bin, &log)
        .args(["uninstall", "--yes"])
        .output()?;
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(!unit_directory.join(UNITS[0]).exists());
    assert!(marker.is_file());
    assert!(String::from_utf8(output.stdout)?.contains("preserved Igor binary"));
    assert_eq!(
        fs::read_to_string(&log)?,
        "--user disable --now -- igor-worker.service\n--user daemon-reload\n"
    );

    fs::write(&log, b"")?;
    let output = command(&home, &fake_bin, &log)
        .args(["uninstall", "--yes"])
        .output()?;
    assert!(output.status.success());
    assert!(fs::read_to_string(&log)?.is_empty());
    Ok(())
}

#[test]
fn failed_service_stop_preserves_unit_files() -> TestResult {
    let temporary = tempfile::tempdir()?;
    let home = temporary.path().join("home");
    let log = temporary.path().join("systemctl.log");
    let fake_bin = fake_systemctl(
        temporary.path(),
        "printf '%s\\n' \"$*\" >> \"$SYSTEMCTL_LOG\"; exit 7",
    )?;
    let unit_directory = install_units(&home)?;
    let output = command(&home, &fake_bin, &log)
        .args(["service", "uninstall", "--yes"])
        .output()?;
    assert!(!output.status.success());
    assert!(UNITS.iter().all(|name| unit_directory.join(name).is_file()));
    assert_eq!(
        fs::read_to_string(log)?,
        "--user disable --now -- igor-worker.service igor-supervisor.service\n"
    );
    Ok(())
}

#[test]
fn install_writes_deterministic_units_and_skips_unchanged_reload() -> TestResult {
    let temporary = tempfile::tempdir()?;
    let home = temporary.path().join("home");
    fs::create_dir_all(&home)?;
    let log = temporary.path().join("systemctl.log");
    let fake_bin = fake_systemctl(
        temporary.path(),
        "printf '%s\\n' \"$*\" >> \"$SYSTEMCTL_LOG\"",
    )?;
    let output = command(&home, &fake_bin, &log)
        .args(["service", "install", "--user"])
        .output()?;
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let directory = home.join("config/systemd/user");
    for name in UNITS {
        let contents = fs::read_to_string(directory.join(name))?;
        assert!(contents.contains("ExecStart=\""));
        if name == UNITS[0] {
            assert!(contents.contains("KillMode=process"));
        } else {
            assert!(!contents.contains("KillMode="));
        }
    }
    assert_eq!(fs::read_to_string(&log)?, "--user daemon-reload\n");
    fs::write(&log, b"")?;
    let output = command(&home, &fake_bin, &log)
        .args(["service", "install", "--user"])
        .output()?;
    assert!(output.status.success());
    assert!(fs::read(&log)?.is_empty());
    Ok(())
}

#[test]
fn installed_units_pass_systemd_verify_when_available() -> TestResult {
    let temporary = tempfile::tempdir()?;
    let home = temporary.path().join("home");
    fs::create_dir_all(&home)?;
    let log = temporary.path().join("systemctl.log");
    let fake_bin = fake_systemctl(
        temporary.path(),
        "printf '%s\\n' \"$*\" >> \"$SYSTEMCTL_LOG\"",
    )?;

    let output = command(&home, &fake_bin, &log)
        .args(["service", "install", "--user"])
        .output()?;
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let directory = home.join("config/systemd/user");
    let worker = fs::read_to_string(directory.join(UNITS[0]))?;
    let supervisor = fs::read_to_string(directory.join(UNITS[1]))?;
    let binary = env!("CARGO_BIN_EXE_igor");
    assert!(worker.contains(&format!("ExecStart=\"{binary}\" worker")));
    assert!(worker.contains("KillMode=process"));
    assert!(supervisor.contains(&format!("ExecStart=\"{binary}\" supervisor")));
    assert!(supervisor.contains("Nice=10"));
    assert!(supervisor.contains("IOSchedulingClass=idle"));

    let analyzer = Command::new("systemd-analyze").arg("--version").output();
    if analyzer.is_err() {
        eprintln!("SKIP: systemd-analyze unavailable; installed unit verification not run");
        return Ok(());
    }
    let output = Command::new("systemd-analyze")
        .arg("--user")
        .arg("verify")
        .arg(directory.join(UNITS[0]))
        .arg(directory.join(UNITS[1]))
        .output()?;
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    eprintln!("systemd-analyze --user verify output:\n{stdout}{stderr}");
    assert!(
        output.status.success(),
        "systemd-analyze verify failed: {stdout}{stderr}"
    );
    Ok(())
}

#[test]
fn install_enable_start_runs_in_order_and_flags_are_independent() -> TestResult {
    let temporary = tempfile::tempdir()?;
    let home = temporary.path().join("home");
    fs::create_dir_all(&home)?;
    let log = temporary.path().join("systemctl.log");
    let fake_bin = fake_systemctl(
        temporary.path(),
        "printf '%s\\n' \"$*\" >> \"$SYSTEMCTL_LOG\"",
    )?;

    let output = command(&home, &fake_bin, &log)
        .args(["service", "install", "--user", "--enable", "--start"])
        .output()?;
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(
        fs::read_to_string(&log)?,
        "--user daemon-reload\n--user enable -- igor-worker.service igor-supervisor.service\n--user start -- igor-worker.service igor-supervisor.service\n"
    );

    fs::write(&log, b"")?;
    let output = command(&home, &fake_bin, &log)
        .args(["service", "install", "--user", "--start"])
        .output()?;
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(
        fs::read_to_string(&log)?,
        "--user start -- igor-worker.service igor-supervisor.service\n"
    );
    Ok(())
}

#[test]
fn service_operations_use_managed_units_and_propagate_failures() -> TestResult {
    let temporary = tempfile::tempdir()?;
    let home = temporary.path().join("home");
    let log = temporary.path().join("systemctl.log");
    let fake_bin = fake_systemctl(
        temporary.path(),
        "printf '%s\\n' \"$*\" >> \"$SYSTEMCTL_LOG\"",
    )?;
    install_units(&home)?;

    for (arguments, expected) in [
        (
            vec!["service", "enable"],
            "--user enable -- igor-worker.service igor-supervisor.service\n",
        ),
        (
            vec!["service", "enable", "--now"],
            "--user enable --now -- igor-worker.service igor-supervisor.service\n",
        ),
        (
            vec!["service", "disable"],
            "--user disable -- igor-worker.service igor-supervisor.service\n",
        ),
        (
            vec!["service", "disable", "--now"],
            "--user disable --now -- igor-worker.service igor-supervisor.service\n",
        ),
        (
            vec!["service", "start"],
            "--user start -- igor-worker.service igor-supervisor.service\n",
        ),
        (
            vec!["service", "stop"],
            "--user stop -- igor-worker.service igor-supervisor.service\n",
        ),
        (
            vec!["service", "restart"],
            "--user restart -- igor-worker.service igor-supervisor.service\n",
        ),
    ] {
        fs::write(&log, b"")?;
        let output = command(&home, &fake_bin, &log).args(arguments).output()?;
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert_eq!(fs::read_to_string(&log)?, expected);
    }

    let empty_home = temporary.path().join("empty-home");
    let output = command(&empty_home, &fake_bin, &log)
        .args(["service", "start"])
        .output()?;
    assert!(!output.status.success());
    Ok(())
}

#[test]
fn service_operation_failure_is_nonzero_and_never_uses_sudo() -> TestResult {
    let temporary = tempfile::tempdir()?;
    let home = temporary.path().join("home");
    let log = temporary.path().join("systemctl.log");
    let fake_bin = fake_systemctl(
        temporary.path(),
        "printf '%s\\n' \"$*\" >> \"$SYSTEMCTL_LOG\"; exit 7",
    )?;
    install_units(&home)?;
    let output = command(&home, &fake_bin, &log)
        .args(["service", "restart"])
        .output()?;
    assert_eq!(output.status.code(), Some(1));
    let invocation = fs::read_to_string(&log)?;
    assert_eq!(
        invocation,
        "--user restart -- igor-worker.service igor-supervisor.service\n"
    );
    assert!(!invocation.contains("sudo"));
    Ok(())
}

#[test]
fn failed_install_reload_restores_existing_unit_bytes() -> TestResult {
    let temporary = tempfile::tempdir()?;
    let home = temporary.path().join("home");
    let directory = install_units(&home)?;
    let before: Vec<_> = UNITS
        .iter()
        .map(|name| fs::read(directory.join(name)))
        .collect::<Result<_, _>>()?;
    let log = temporary.path().join("systemctl.log");
    let fake_bin = fake_systemctl(
        temporary.path(),
        "printf '%s\\n' \"$*\" >> \"$SYSTEMCTL_LOG\"; exit 7",
    )?;
    let output = command(&home, &fake_bin, &log)
        .args(["service", "install", "--user"])
        .output()?;
    assert!(!output.status.success());
    for (name, original) in UNITS.iter().zip(before) {
        assert_eq!(fs::read(directory.join(name))?, original);
    }
    assert_eq!(
        fs::read_to_string(&log)?,
        "--user daemon-reload\n--user daemon-reload\n"
    );
    Ok(())
}

#[test]
fn install_rejects_symlinked_unit_without_touching_any_unit() -> TestResult {
    let temporary = tempfile::tempdir()?;
    let home = temporary.path().join("home");
    fs::create_dir_all(&home)?;
    let log = temporary.path().join("systemctl.log");
    let fake_bin = fake_systemctl(
        temporary.path(),
        "printf '%s\\n' \"$*\" >> \"$SYSTEMCTL_LOG\"",
    )?;
    let directory = home.join("config/systemd/user");
    fs::create_dir_all(&directory)?;
    let external = temporary.path().join("external");
    fs::write(&external, b"preserve")?;
    std::os::unix::fs::symlink(external, directory.join(UNITS[0]))?;
    let output = command(&home, &fake_bin, &log)
        .args(["service", "install", "--user"])
        .output()?;
    assert!(!output.status.success());
    assert_eq!(fs::read(directory.join(UNITS[0]))?, b"preserve");
    assert!(!directory.join(UNITS[1]).exists());
    assert!(!log.exists());
    Ok(())
}

#[test]
fn service_status_reports_both_units_and_stable_json_without_installation() -> TestResult {
    let temporary = tempfile::tempdir()?;
    let home = temporary.path().join("home");
    let log = temporary.path().join("systemctl.log");
    let fake_bin = fake_systemctl(
        temporary.path(),
        "printf '%s\\n' \"$*\" >> \"$SYSTEMCTL_LOG\"; case \"$*\" in *igor-worker*) printf 'UnitFileState=enabled\\nActiveState=active\\nLoadState=loaded\\n' ;; *) printf 'LoadState=not-found\\nActiveState=inactive\\nUnitFileState=disabled\\n' ;; esac",
    )?;
    let output = command(&home, &fake_bin, &log)
        .args(["service", "status", "--json"])
        .output()?;
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let json: serde_json::Value = serde_json::from_slice(&output.stdout)?;
    assert_eq!(
        json[0],
        serde_json::json!({"unit":"igor-worker.service","load_state":"loaded","active_state":"active","unit_file_state":"enabled"})
    );
    assert_eq!(
        json[1],
        serde_json::json!({"unit":"igor-supervisor.service","load_state":"not-found","active_state":"inactive","unit_file_state":"disabled"})
    );
    assert_eq!(
        fs::read_to_string(log)?,
        "--user show --property=LoadState,ActiveState,UnitFileState -- igor-worker.service\n--user show --property=LoadState,ActiveState,UnitFileState -- igor-supervisor.service\n"
    );
    Ok(())
}

#[test]
fn service_status_and_logs_propagate_user_manager_and_journal_failures() -> TestResult {
    let temporary = tempfile::tempdir()?;
    let home = temporary.path().join("home");
    let log = temporary.path().join("calls.log");
    let system_bin = fake_systemctl(
        temporary.path(),
        "printf '%s\\n' \"$*\" >> \"$SYSTEMCTL_LOG\"; exit 1",
    )?;
    let status = command(&home, &system_bin, &log)
        .args(["service", "status"])
        .output()?;
    assert!(!status.status.success());
    assert!(String::from_utf8_lossy(&status.stderr).contains("systemctl --user show"));

    let journal_bin = fake_journalctl(
        temporary.path(),
        "printf '%s\\n' \"$*\" >> \"$SYSTEMCTL_LOG\"; exit 7",
    )?;
    let logs = command(&home, &journal_bin, &log)
        .args(["service", "logs"])
        .output()?;
    assert!(!logs.status.success());
    assert!(String::from_utf8_lossy(&logs.stderr).contains("journalctl --user failed"));
    assert_eq!(
        fs::read_to_string(log)?,
        "--user show --property=LoadState,ActiveState,UnitFileState -- igor-worker.service\n--user --no-pager -u igor-worker.service -u igor-supervisor.service\n"
    );
    Ok(())
}
