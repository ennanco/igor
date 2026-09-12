use std::{error::Error, ffi::OsString, fs, path::Path};

use igor_core::{
    ConfigOverrides, Environment, REDACTED, ResourceMode, ResourceRequest, discover_project_config,
    initialize_project, load_effective_config, load_global_config, load_project_config,
};
use tempfile::TempDir;

fn environment(home: &Path, values: &[(&str, &Path)]) -> Environment {
    Environment::new(
        home,
        1234,
        values
            .iter()
            .map(|(name, value)| (OsString::from(name), value.as_os_str().to_os_string())),
    )
}

fn write(path: &Path, contents: &str) -> Result<(), Box<dyn Error>> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    fs::write(path, contents)?;
    Ok(())
}

#[test]
fn xdg_paths_and_fallbacks_are_deterministic() -> Result<(), Box<dyn Error>> {
    let temporary = TempDir::new()?;
    let home = temporary.path().join("home");
    let xdg_config = temporary.path().join("config");
    let xdg_state = temporary.path().join("state");
    let xdg_runtime = temporary.path().join("runtime");
    let configured = environment(
        &home,
        &[
            ("XDG_CONFIG_HOME", &xdg_config),
            ("XDG_STATE_HOME", &xdg_state),
            ("XDG_RUNTIME_DIR", &xdg_runtime),
        ],
    );
    let effective =
        load_effective_config(&configured, &ConfigOverrides::default(), temporary.path())?;
    assert_eq!(
        effective.paths.config_file,
        xdg_config.join("igor/config.toml")
    );
    assert_eq!(
        effective.paths.database,
        xdg_state.join("igor/igor.sqlite3")
    );
    assert_eq!(effective.paths.log_dir, xdg_state.join("igor/logs"));
    assert_eq!(
        effective.paths.worktree_dir,
        xdg_state.join("igor/worktrees")
    );
    assert_eq!(effective.paths.report_dir, xdg_state.join("igor/reports"));
    assert_eq!(
        effective.paths.worker_socket,
        xdg_runtime.join("igor/worker.sock")
    );
    assert_eq!(
        effective.paths.supervisor_socket,
        xdg_runtime.join("igor/supervisor.sock")
    );
    assert_eq!(
        effective.paths.resource_dir,
        xdg_runtime.join("igor/resources")
    );

    let fallback = load_effective_config(
        &environment(&home, &[]),
        &ConfigOverrides::default(),
        temporary.path(),
    )?;
    assert_eq!(
        fallback.paths.config_file,
        home.join(".config/igor/config.toml")
    );
    assert_eq!(fallback.paths.state_dir, home.join(".local/state/igor"));
    assert_eq!(fallback.paths.runtime_dir, Path::new("/run/user/1234/igor"));
    Ok(())
}

#[test]
fn precedence_is_cli_then_igor_environment_then_file_then_default() -> Result<(), Box<dyn Error>> {
    let temporary = TempDir::new()?;
    let config = temporary.path().join("config.toml");
    write(
        &config,
        "schema_version = 1\n[paths]\nstate_dir = 'from-file'\nlog_dir = 'file-logs'\n",
    )?;
    let env_state = temporary.path().join("from-env");
    let env = environment(temporary.path(), &[("IGOR_STATE_DIR", &env_state)]);
    let cli_state = temporary.path().join("from-cli");
    let effective = load_effective_config(
        &env,
        &ConfigOverrides {
            global_config: Some(config.clone()),
            state_dir: Some(cli_state.clone()),
            ..ConfigOverrides::default()
        },
        temporary.path(),
    )?;
    assert_eq!(effective.paths.state_dir, cli_state);
    assert_eq!(effective.paths.log_dir, temporary.path().join("file-logs"));

    let effective = load_effective_config(
        &env,
        &ConfigOverrides {
            global_config: Some(config),
            ..ConfigOverrides::default()
        },
        temporary.path(),
    )?;
    assert_eq!(effective.paths.state_dir, env_state);
    Ok(())
}

#[test]
fn project_discovery_walks_upward() -> Result<(), Box<dyn Error>> {
    let temporary = TempDir::new()?;
    let config = temporary.path().join(".igor/project.toml");
    let nested = temporary.path().join("one/two");
    write(&config, "schema_version = 1\n")?;
    fs::create_dir_all(&nested)?;
    assert_eq!(
        discover_project_config(&nested).as_deref(),
        Some(config.as_path())
    );
    Ok(())
}

#[test]
fn minimal_project_has_unrestricted_exclusive_defaults() -> Result<(), Box<dyn Error>> {
    let temporary = TempDir::new()?;
    let config = temporary.path().join(".igor/project.toml");
    write(&config, "schema_version = 1\n")?;
    let loaded = load_project_config(&config)?;
    assert_eq!(loaded.config.resources, ResourceRequest::default());
    assert_eq!(loaded.config.resources.mode, ResourceMode::ExclusiveHost);
    assert_eq!(loaded.config.resources.cpu_threads, None);
    assert_eq!(loaded.config.resources.memory_bytes, None);
    assert_eq!(loaded.config.resources.timeout_seconds, None);
    assert!(loaded.config.resources.gpu_exclusive);
    Ok(())
}

#[test]
fn relative_paths_are_normalized_and_cannot_escape() -> Result<(), Box<dyn Error>> {
    let temporary = TempDir::new()?;
    let config = temporary.path().join(".igor/project.toml");
    write(
        &config,
        "schema_version = 1\n[paths]\nartifact_root = 'output/../artifacts'\ncleanup_roots = ['artifacts/tmp']\n",
    )?;
    let loaded = load_project_config(&config)?;
    assert_eq!(
        loaded.config.paths.artifact_root,
        temporary.path().join("artifacts")
    );

    write(
        &config,
        "schema_version = 1\n[paths]\nartifact_root = '../../escape'\n",
    )?;
    let error = load_project_config(&config)
        .err()
        .ok_or("escaping path was accepted")?;
    let diagnostic = error.to_string();
    assert!(diagnostic.contains(&config.display().to_string()));
    assert!(diagnostic.contains("paths.artifact_root"));
    assert!(diagnostic.contains("escapes project root"));
    Ok(())
}

#[test]
fn provenance_paths_are_resolved_against_the_project() -> Result<(), Box<dyn Error>> {
    let temporary = TempDir::new()?;
    let config = temporary.path().join(".igor/project.toml");
    write(
        &config,
        "schema_version = 1\n[provenance]\nscientific_configurations = ['configs/science.json']\nimmutable_inputs = ['inputs/split.json']\n",
    )?;
    let loaded = load_project_config(&config)?;
    assert_eq!(
        loaded.config.provenance.scientific_configurations,
        [temporary.path().join("configs/science.json")]
    );
    assert_eq!(
        loaded.config.provenance.immutable_inputs,
        [temporary.path().join("inputs/split.json")]
    );
    Ok(())
}

#[test]
fn diagnostics_identify_file_field_version_and_enum() -> Result<(), Box<dyn Error>> {
    let temporary = TempDir::new()?;
    let config = temporary.path().join(".igor/project.toml");
    write(&config, "schema_version = 9\n")?;
    let version = load_project_config(&config)
        .err()
        .ok_or("version was accepted")?
        .to_string();
    assert!(version.contains(&config.display().to_string()));
    assert!(version.contains("schema_version"));
    assert!(version.contains("found 9"));

    write(
        &config,
        "schema_version = 1\n[resources]\nmode = 'invalid-mode'\n",
    )?;
    let value = load_project_config(&config)
        .err()
        .ok_or("enum was accepted")?
        .to_string();
    assert!(value.contains(&config.display().to_string()));
    assert!(value.contains("resources.mode"));
    assert!(value.contains("exclusive-host"));
    Ok(())
}

#[test]
fn secrets_are_redacted_from_debug_display_and_errors() -> Result<(), Box<dyn Error>> {
    const SENTINEL: &str = "SENTINEL-SECRET-DO-NOT-LEAK";
    let temporary = TempDir::new()?;
    let config = temporary.path().join("config.toml");
    write(
        &config,
        &format!("schema_version = 1\n[telegram]\nbot_token = '{SENTINEL}'\nchat_id = 'chat'\n"),
    )?;
    let loaded = load_global_config(&config)?;
    let debug = format!("{loaded:?}");
    let shown = toml::to_string_pretty(&loaded)?;
    assert!(!debug.contains(SENTINEL));
    assert!(!shown.contains(SENTINEL));
    assert!(shown.contains(REDACTED));

    write(
        &config,
        &format!("schema_version = 1\n[telegram]\nbot_token = '{SENTINEL}'\nunknown = true\n"),
    )?;
    let error = load_global_config(&config)
        .err()
        .ok_or("unknown key was accepted")?
        .to_string();
    assert!(!error.contains(SENTINEL));
    assert!(error.contains("telegram.unknown"));
    Ok(())
}

#[test]
fn init_preflights_overwrites_and_creates_only_portable_files() -> Result<(), Box<dyn Error>> {
    let temporary = TempDir::new()?;
    let project = temporary.path().join("project");
    let created = initialize_project(&project, false)?;
    assert_eq!(created.len(), 2);
    assert!(project.join(".igor/project.toml").is_file());
    assert!(project.join(".igor/report-prompt.md").is_file());
    let generated = fs::read_to_string(project.join(".igor/project.toml"))?;
    assert!(generated.contains("cpu_threads = \"unlimited\""));
    assert!(generated.contains("memory = \"unlimited\""));
    assert!(generated.contains("timeout = \"none\""));
    assert!(!project.join("igor.sqlite3").exists());
    assert!(!project.join("logs").exists());
    assert!(initialize_project(&project, false).is_err());
    initialize_project(&project, true)?;
    Ok(())
}

#[test]
fn shipped_fixtures_cover_valid_minimal_complete_and_invalid_documents()
-> Result<(), Box<dyn Error>> {
    let repository = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let fixtures = repository.join("tests/fixtures/config");
    load_global_config(&fixtures.join("global-valid.toml"))?;
    load_global_config(&fixtures.join("global-minimal.toml"))?;
    load_global_config(&fixtures.join("global-complete.toml"))?;
    assert!(load_global_config(&fixtures.join("global-invalid.toml")).is_err());

    let temporary = TempDir::new()?;
    let project_directory = temporary.path().join(".igor");
    fs::create_dir_all(&project_directory)?;
    for fixture in [
        "project-valid.toml",
        "project-minimal.toml",
        "project-complete.toml",
    ] {
        let destination = project_directory.join("project.toml");
        fs::copy(fixtures.join(fixture), &destination)?;
        load_project_config(&destination)?;
    }
    for fixture in [
        "project-invalid-version.toml",
        "project-invalid-enum.toml",
        "project-invalid-path.toml",
    ] {
        let destination = project_directory.join("project.toml");
        fs::copy(fixtures.join(fixture), &destination)?;
        assert!(
            load_project_config(&destination).is_err(),
            "accepted {fixture}"
        );
    }
    Ok(())
}
