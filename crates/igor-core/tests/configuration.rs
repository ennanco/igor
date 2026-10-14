use std::{error::Error, ffi::OsString, fs, os::unix::fs::PermissionsExt, path::Path};

use igor_core::{
    ArtifactRetentionConfig, ConfigOverrides, Environment, FailedRetention, HostConfigUpdate,
    REDACTED, ReportConfig, ReportGenerator, ResourceMode, ResourceRequest, RetentionConfig,
    SupersededRetention, discover_project_config, initialize_project, load_effective_config,
    load_global_config, load_project_config, update_global_host_config,
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
    assert!(!loaded.config.report.enabled);
    assert_eq!(
        loaded.config.report.generator,
        ReportGenerator::Deterministic
    );
    assert!(loaded.config.results.is_none());
    Ok(())
}

#[test]
fn optional_report_configuration_retains_explicit_values_and_defaults_to_deterministic()
-> Result<(), Box<dyn Error>> {
    let temporary = TempDir::new()?;
    let config = temporary.path().join(".igor/project.toml");
    write(
        &config,
        "schema_version = 1\n[report]\nenabled = true\ngenerator = 'opencode'\n",
    )?;
    let legacy = load_project_config(&config)?.config;
    assert!(legacy.report.enabled);
    assert_eq!(legacy.report.generator, ReportGenerator::Opencode);

    write(&config, "schema_version = 1\n[report]\nenabled = false\n")?;
    let disabled = load_project_config(&config)?.config;
    assert!(!disabled.report.enabled);
    assert_eq!(disabled.report.generator, ReportGenerator::Deterministic);
    let expected = ReportConfig {
        output: temporary.path().join("RESULTS.md"),
        prompt_file: temporary.path().join(".igor/report-prompt.md"),
        ..ReportConfig::default()
    };
    assert_eq!(disabled.report, expected);
    Ok(())
}

#[test]
fn results_configuration_is_optional_and_strict() -> Result<(), Box<dyn Error>> {
    let temporary = TempDir::new()?;
    let config = temporary.path().join(".igor/project.toml");
    write(&config, "schema_version = 1\n")?;
    let omitted = load_project_config(&config)?;
    assert!(omitted.config.results.is_none());
    let serialized = serde_json::to_value(&omitted.config)?;
    assert!(serialized.get("results").is_none());

    write(
        &config,
        "schema_version = 1\n[results]\nextractor = ['python3', 'script.py']\nworking_directory = 'attempt'\n",
    )?;
    let loaded = load_project_config(&config)?;
    assert_eq!(
        loaded
            .config
            .results
            .as_ref()
            .ok_or("results missing")?
            .extractor,
        ["python3", "script.py"]
    );
    for results in [
        "extractor = []",
        "extractor = ['', 'arg']",
        "extractor = ['   ']",
        "extractor = ['python3']\nworking_directory = 'project'",
        "extractor = ['python3']\nunknown = true",
    ] {
        write(
            &config,
            &format!("schema_version = 1\n[results]\n{results}\n"),
        )?;
        assert!(
            load_project_config(&config).is_err(),
            "accepted results config: {results}"
        );
    }
    let mut configured = loaded.config.results.ok_or("results missing")?;
    configured.extractor.push("bad\0arg".into());
    assert!(configured.validate().is_err());
    Ok(())
}

#[test]
fn retention_defaults_and_project_exceptions_are_effective_and_backward_compatible()
-> Result<(), Box<dyn Error>> {
    let temporary = TempDir::new()?;
    let path = temporary.path().join(".igor/project.toml");
    write(&path, "schema_version = 1\n")?;
    let omitted = load_project_config(&path)?.config;
    assert_eq!(omitted.retention, RetentionConfig::default());
    assert_eq!(omitted.artifacts, ArtifactRetentionConfig::default());
    let legacy: serde_json::Value = serde_json::to_value(&omitted)?;
    assert!(legacy.get("retention").is_none());
    assert!(legacy.get("artifacts").is_none());

    write(
        &path,
        concat!(
            "schema_version = 1\n[retention]\n",
            "failed_artifacts = 'delete-after-diagnosis'\n",
            "failed_full_logs = 'delete-after-diagnosis'\n",
            "failed_log_tail_kb = 512\ncompress_success_logs = true\n",
            "delete_repair_worktrees = true\n",
            "superseded_artifacts = 'delete-after-replacement'\n",
            "[artifacts]\nkeep_on_success = []\nkeep_on_failure = []\n",
        ),
    )?;
    let explicit_defaults = load_project_config(&path)?.config;
    assert_eq!(omitted, explicit_defaults);
    assert_eq!(serde_json::to_value(&explicit_defaults)?, legacy);

    write(
        &path,
        concat!(
            "schema_version = 1\n[retention]\n",
            "failed_artifacts = 'keep'\nfailed_log_tail_kb = 0\n",
            "compress_success_logs = false\n",
            "superseded_artifacts = 'keep'\n",
            "[artifacts]\n",
            "keep_on_success = ['metrics.json', 'figures/**']\n",
            "keep_on_failure = ['crash-dump.json']\n",
        ),
    )?;
    let configured = load_project_config(&path)?.config;
    assert_eq!(configured.retention.failed_artifacts, FailedRetention::Keep);
    assert_eq!(
        configured.retention.failed_full_logs,
        FailedRetention::DeleteAfterDiagnosis
    );
    assert_eq!(configured.retention.failed_log_tail_kb, 0);
    assert!(!configured.retention.compress_success_logs);
    assert!(configured.retention.delete_repair_worktrees);
    assert_eq!(
        configured.retention.superseded_artifacts,
        SupersededRetention::Keep
    );
    assert_eq!(
        configured.artifacts.keep_on_success,
        ["metrics.json", "figures/**"]
    );
    assert_eq!(configured.artifacts.keep_on_failure, ["crash-dump.json"]);
    let serialized = serde_json::to_value(&configured)?;
    assert_eq!(serialized["retention"]["failed_artifacts"], "keep");
    assert_eq!(
        serialized["artifacts"]["keep_on_failure"][0],
        "crash-dump.json"
    );
    let restored: igor_core::ProjectConfig = serde_json::from_value(serialized)?;
    assert_eq!(restored, configured);
    Ok(())
}

#[test]
fn retention_rejects_unknown_values_and_escaping_or_duplicate_patterns()
-> Result<(), Box<dyn Error>> {
    let temporary = TempDir::new()?;
    let path = temporary.path().join(".igor/project.toml");
    for (body, field) in [
        (
            "[retention]\nfailed_artifacts = 'delete-now'",
            "retention.failed_artifacts",
        ),
        (
            "[retention]\nfailed_full_logs = 'delete-after-replacement'",
            "retention.failed_full_logs",
        ),
        (
            "[retention]\nsuperseded_artifacts = 'delete-after-diagnosis'",
            "retention.superseded_artifacts",
        ),
        (
            "[retention]\nfailed_log_tail_kb = -1",
            "retention.failed_log_tail_kb",
        ),
        (
            "[retention]\nfailed_log_tail_kb = 1.5",
            "retention.failed_log_tail_kb",
        ),
        (
            "[retention]\nfailed_log_tail_kb = 16385",
            "retention.failed_log_tail_kb",
        ),
        ("[retention]\nunknown = true", "retention.unknown"),
        (
            "[artifacts]\nkeep_on_success = 'file'",
            "artifacts.keep_on_success",
        ),
        ("[artifacts]\nunknown = true", "artifacts.unknown"),
        (
            "[artifacts]\nkeep_on_success = ['../outside']",
            "artifacts.keep_on_success[0]",
        ),
        (
            "[artifacts]\nkeep_on_failure = ['/absolute']",
            "artifacts.keep_on_failure[0]",
        ),
        (
            "[artifacts]\nkeep_on_failure = ['figures//**']",
            "artifacts.keep_on_failure[0]",
        ),
        (
            "[artifacts]\nkeep_on_success = ['x', 'x']",
            "artifacts.keep_on_success[1]",
        ),
    ] {
        write(&path, &format!("schema_version = 1\n{body}\n"))?;
        let error = load_project_config(&path)
            .err()
            .ok_or("invalid retention accepted")?;
        assert!(error.to_string().contains(field), "{body}: {error}");
    }
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
    let initialized = load_project_config(&project.join(".igor/project.toml"))?.config;
    assert!(!initialized.report.enabled);
    assert_eq!(initialized.report.generator, ReportGenerator::Deterministic);
    let expected_report = ReportConfig {
        output: project.join("RESULTS.md"),
        prompt_file: project.join(".igor/report-prompt.md"),
        ..ReportConfig::default()
    };
    assert_eq!(initialized.report, expected_report);
    assert!(!project.join("igor.sqlite3").exists());
    assert!(!project.join("logs").exists());
    assert!(initialize_project(&project, false).is_err());
    initialize_project(&project, true)?;
    Ok(())
}

#[cfg(unix)]
#[test]
fn init_force_rejects_symlink_targets_without_changing_either_target() -> Result<(), Box<dyn Error>>
{
    use std::os::unix::fs::symlink;

    for (managed, dangling) in [
        ("project.toml", false),
        ("report-prompt.md", false),
        ("project.toml", true),
    ] {
        let temporary = TempDir::new()?;
        let project = temporary.path().join("project");
        let directory = project.join(".igor");
        fs::create_dir_all(&directory)?;
        let config = directory.join("project.toml");
        let prompt = directory.join("report-prompt.md");
        let external = temporary.path().join("external");
        let other = if managed == "project.toml" {
            &prompt
        } else {
            &config
        };
        write(other, "preserve managed file")?;
        if dangling {
            symlink(
                temporary.path().join("missing-target"),
                if managed == "project.toml" {
                    &config
                } else {
                    &prompt
                },
            )?;
        } else {
            write(&external, "preserve external file")?;
            symlink(
                &external,
                if managed == "project.toml" {
                    &config
                } else {
                    &prompt
                },
            )?;
        }

        assert!(initialize_project(&project, true).is_err());
        assert_eq!(fs::read_to_string(other)?, "preserve managed file");
        if !dangling {
            assert_eq!(fs::read_to_string(external)?, "preserve external file");
        }
    }
    Ok(())
}

#[cfg(unix)]
#[test]
fn init_rejects_symlinked_config_directory_without_writing_outside_project()
-> Result<(), Box<dyn Error>> {
    use std::os::unix::fs::symlink;

    let temporary = TempDir::new()?;
    let project = temporary.path().join("project");
    let external = temporary.path().join("external");
    fs::create_dir_all(&project)?;
    fs::create_dir_all(&external)?;
    write(&external.join("project.toml"), "preserve external config")?;
    symlink(&external, project.join(".igor"))?;
    assert!(initialize_project(&project, true).is_err());
    assert_eq!(
        fs::read_to_string(external.join("project.toml"))?,
        "preserve external config"
    );
    assert!(!external.join("report-prompt.md").exists());
    Ok(())
}

#[test]
fn init_force_overwrites_regular_files() -> Result<(), Box<dyn Error>> {
    let temporary = TempDir::new()?;
    let project = temporary.path().join("project");
    let config = project.join(".igor/project.toml");
    let prompt = project.join(".igor/report-prompt.md");
    write(&config, "old config")?;
    write(&prompt, "old prompt")?;
    fs::set_permissions(&config, fs::Permissions::from_mode(0o600))?;

    initialize_project(&project, true)?;

    assert_ne!(fs::read_to_string(&config)?, "old config");
    assert_eq!(fs::metadata(config)?.permissions().mode() & 0o777, 0o600);
    assert_ne!(fs::read_to_string(prompt)?, "old prompt");
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

#[test]
fn host_limits_update_atomically_without_exposing_secrets() -> Result<(), Box<dyn Error>> {
    const SECRET: &str = "preserve-this-token";
    let temporary = TempDir::new()?;
    let path = temporary.path().join("config/igor/config.toml");
    write(
        &path,
        &format!("schema_version = 1\n[telegram]\nbot_token = '{SECRET}'\n"),
    )?;
    let updated = update_global_host_config(
        &path,
        &HostConfigUpdate {
            memory_bytes: Some(Some(8_000_000_000)),
            gpus: Some(vec!["GPU-one".into()]),
            discover_gpus: Some(false),
            max_concurrent_jobs: Some(2),
            ..HostConfigUpdate::default()
        },
    )?;
    assert_eq!(updated.host.memory_bytes, Some(8_000_000_000));
    assert_eq!(updated.host.gpus, ["GPU-one"]);
    assert!(!updated.host.discover_gpus);
    assert_eq!(updated.host.max_concurrent_jobs, 2);
    let raw = fs::read_to_string(&path)?;
    assert!(raw.contains(SECRET));
    assert_eq!(fs::metadata(&path)?.permissions().mode() & 0o777, 0o600);

    let before = raw;
    assert!(
        update_global_host_config(
            &path,
            &HostConfigUpdate {
                max_concurrent_jobs: Some(0),
                ..HostConfigUpdate::default()
            }
        )
        .is_err()
    );
    assert_eq!(fs::read_to_string(path)?, before);

    assert!(
        update_global_host_config(
            &temporary.path().join("config/igor/config.toml"),
            &HostConfigUpdate {
                memory_bytes: Some(Some(u64::MAX)),
                ..HostConfigUpdate::default()
            }
        )
        .is_err()
    );
    Ok(())
}
