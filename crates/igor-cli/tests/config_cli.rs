use std::{error::Error, fs, path::Path, process::Command};

use tempfile::TempDir;

fn igor() -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_igor"));
    command.env_clear();
    command
}

fn disposable_command(home: &Path, cwd: &Path) -> Command {
    let mut command = igor();
    command
        .current_dir(cwd)
        .env("HOME", home)
        .env("XDG_CONFIG_HOME", home.join("config"))
        .env("XDG_STATE_HOME", home.join("state"))
        .env("XDG_RUNTIME_DIR", home.join("runtime"));
    command
}

fn text(bytes: Vec<u8>) -> Result<String, Box<dyn Error>> {
    Ok(String::from_utf8(bytes)?)
}

#[test]
fn version_is_preserved() -> Result<(), Box<dyn Error>> {
    let output = igor().arg("--version").output()?;
    assert!(output.status.success());
    assert!(text(output.stdout)?.contains(env!("CARGO_PKG_VERSION")));
    Ok(())
}

#[test]
fn init_does_not_overwrite_without_force() -> Result<(), Box<dyn Error>> {
    let temporary = TempDir::new()?;
    let home = temporary.path().join("home");
    let project = temporary.path().join("project");
    let output = disposable_command(&home, temporary.path())
        .args(["init", project.to_str().ok_or("non-UTF-8 project path")?])
        .output()?;
    assert!(output.status.success(), "{}", text(output.stderr)?);
    let config = project.join(".igor/project.toml");
    let prompt = project.join(".igor/report-prompt.md");
    assert!(config.is_file());
    assert!(prompt.is_file());
    assert!(!project.join("igor.sqlite3").exists());
    assert!(!project.join("logs").exists());

    fs::write(&config, "sentinel")?;
    let output = disposable_command(&home, temporary.path())
        .args(["init", project.to_str().ok_or("non-UTF-8 project path")?])
        .output()?;
    assert!(!output.status.success());
    assert_eq!(fs::read_to_string(&config)?, "sentinel");

    let output = disposable_command(&home, temporary.path())
        .args([
            "init",
            project.to_str().ok_or("non-UTF-8 project path")?,
            "--force",
        ])
        .output()?;
    assert!(output.status.success(), "{}", text(output.stderr)?);
    assert!(fs::read_to_string(config)?.contains("schema_version = 1"));
    Ok(())
}

#[test]
fn config_commands_discover_validate_and_redact() -> Result<(), Box<dyn Error>> {
    const SENTINEL: &str = "CLI-SENTINEL-SECRET";
    let temporary = TempDir::new()?;
    let home = temporary.path().join("home");
    let project = temporary.path().join("project");
    let nested = project.join("nested/directory");
    fs::create_dir_all(&nested)?;
    fs::create_dir_all(home.join("config/igor"))?;
    fs::create_dir_all(project.join(".igor"))?;
    fs::write(
        home.join("config/igor/config.toml"),
        format!("schema_version = 1\n[telegram]\nbot_token = '{SENTINEL}'\n"),
    )?;
    fs::write(project.join(".igor/project.toml"), "schema_version = 1\n")?;

    let output = disposable_command(&home, &nested)
        .args(["config", "path"])
        .output()?;
    assert!(output.status.success(), "{}", text(output.stderr)?);
    let stdout = text(output.stdout)?;
    assert!(stdout.contains(&project.join(".igor/project.toml").display().to_string()));

    let output = disposable_command(&home, &nested)
        .args(["config", "show"])
        .output()?;
    assert!(output.status.success(), "{}", text(output.stderr)?);
    let stdout = text(output.stdout)?;
    assert!(!stdout.contains(SENTINEL));
    assert!(stdout.contains("<redacted>"));

    let output = disposable_command(&home, &nested)
        .args(["config", "check"])
        .output()?;
    assert!(output.status.success(), "{}", text(output.stderr)?);
    assert!(text(output.stdout)?.contains("configuration is valid"));
    Ok(())
}

#[test]
fn config_check_reports_exact_invalid_field_without_secret() -> Result<(), Box<dyn Error>> {
    const SENTINEL: &str = "ERROR-SENTINEL-SECRET";
    let temporary = TempDir::new()?;
    let home = temporary.path().join("home");
    let project = temporary.path().join("project");
    fs::create_dir_all(home.join("config/igor"))?;
    fs::create_dir_all(project.join(".igor"))?;
    fs::write(
        home.join("config/igor/config.toml"),
        format!("schema_version = 1\n[telegram]\nbot_token = '{SENTINEL}'\n"),
    )?;
    let project_config = project.join(".igor/project.toml");
    fs::write(
        &project_config,
        "schema_version = 1\n[resources]\nmode = 'invalid'\n",
    )?;
    let output = disposable_command(&home, &project)
        .args(["config", "check"])
        .output()?;
    assert!(!output.status.success());
    let stderr = text(output.stderr)?;
    assert!(stderr.contains(&project_config.display().to_string()));
    assert!(stderr.contains("resources.mode"));
    assert!(!stderr.contains(SENTINEL));
    Ok(())
}

#[test]
fn config_set_updates_host_scheduling_limits_and_preserves_secrets() -> Result<(), Box<dyn Error>> {
    const SENTINEL: &str = "SET-SENTINEL-SECRET";
    let temporary = TempDir::new()?;
    let home = temporary.path().join("home");
    let config = home.join("config/igor/config.toml");
    fs::create_dir_all(config.parent().ok_or("missing config parent")?)?;
    fs::write(
        &config,
        format!("schema_version = 1\n[telegram]\nbot_token = '{SENTINEL}'\n"),
    )?;
    let output = disposable_command(&home, temporary.path())
        .args([
            "config",
            "set",
            "--max-concurrent-jobs",
            "2",
            "--memory-bytes",
            "8GB",
            "--gpu",
            "GPU-one",
            "--gpu",
            "GPU-two",
        ])
        .output()?;
    assert!(output.status.success(), "{}", text(output.stderr)?);
    let raw = fs::read_to_string(&config)?;
    assert!(raw.contains(SENTINEL));

    let shown = disposable_command(&home, temporary.path())
        .args(["config", "show", "--json"])
        .output()?;
    assert!(shown.status.success(), "{}", text(shown.stderr)?);
    let shown_output = String::from_utf8(shown.stdout)?;
    let shown: serde_json::Value = serde_json::from_str(&shown_output)?;
    assert_eq!(shown["global"]["host"]["max_concurrent_jobs"], 2);
    assert_eq!(shown["global"]["host"]["memory_bytes"], 8_000_000_000_u64);
    assert_eq!(shown["global"]["host"]["gpus"][0], "GPU-one");
    assert_eq!(shown["global"]["host"]["discover_gpus"], false);
    assert!(!shown_output.contains(SENTINEL));

    let reset = disposable_command(&home, temporary.path())
        .args([
            "config",
            "set",
            "--auto-memory",
            "--auto-cpu",
            "--auto-gpus",
        ])
        .output()?;
    assert!(reset.status.success(), "{}", text(reset.stderr)?);
    let reset = disposable_command(&home, temporary.path())
        .args(["config", "show", "--json"])
        .output()?;
    let reset: serde_json::Value = serde_json::from_slice(&reset.stdout)?;
    assert!(reset["global"]["host"]["memory_bytes"].is_null());
    assert!(reset["global"]["host"]["cpu_threads"].is_null());
    assert_eq!(reset["global"]["host"]["gpus"], serde_json::json!([]));
    assert_eq!(reset["global"]["host"]["discover_gpus"], true);
    Ok(())
}
