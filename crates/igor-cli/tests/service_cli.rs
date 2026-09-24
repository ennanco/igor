use std::{
    error::Error,
    fs,
    os::unix::fs::PermissionsExt,
    path::{Path, PathBuf},
    process::Command,
};

type TestResult = Result<(), Box<dyn Error>>;

const UNITS: [&str; 2] = ["igor-worker.service", "igor-supervisor.service"];

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
