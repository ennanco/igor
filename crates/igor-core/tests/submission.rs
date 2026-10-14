use std::{collections::BTreeMap, error::Error, fs, path::Path, process::Command};

use igor_core::{
    CommandSpec, EnvironmentInheritance, EnvironmentPolicy, Project, ProjectId, ShellPolicy,
    SourceIdentity, SubmissionInput, build_submission, initialize_project, load_job_file,
    load_project_config,
};
use tempfile::TempDir;

type TestResult = Result<(), Box<dyn Error>>;

#[test]
fn shipped_docker_job_example_is_valid() -> TestResult {
    let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../config/example-docker-job.toml");
    let project = TempDir::new()?;
    let input = load_job_file(&path)?.into_input(project.path())?;
    input.command.validate()?;
    input
        .executor
        .ok_or("missing Docker executor")?
        .validate()?;
    input
        .resources
        .ok_or("missing Docker resources")?
        .validate()?;
    Ok(())
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

fn repository() -> Result<(TempDir, Project, igor_core::ProjectConfig), Box<dyn Error>> {
    let temporary = TempDir::new()?;
    let root = temporary.path().join("project");
    initialize_project(&root, false)?;
    fs::write(root.join("science.json"), b"{\"seed\":1}\n")?;
    git(&root, &["init", "-q"])?;
    git(&root, &["config", "user.email", "igor@example.invalid"])?;
    git(&root, &["config", "user.name", "Igor Test"])?;
    git(
        &root,
        &[
            "remote",
            "add",
            "origin",
            "https://user:secret@example.invalid/research.git",
        ],
    )?;
    git(&root, &["add", "."])?;
    git(&root, &["commit", "-qm", "initial"])?;
    let loaded = load_project_config(&root.join(".igor/project.toml"))?;
    let project = Project {
        id: ProjectId::new(),
        name: "project".into(),
        root,
        config_path: loaded.config_file,
    };
    Ok((temporary, project, loaded.config))
}

fn input(project: &Project) -> SubmissionInput {
    SubmissionInput {
        name: Some("experiment".into()),
        priority: 7,
        allow_dirty: false,
        command: CommandSpec {
            program: "python".into(),
            args: vec!["-m".into(), "experiment".into(), "".into()],
            cwd: project.root.clone(),
            shell: ShellPolicy::Direct,
            environment: EnvironmentPolicy {
                set: BTreeMap::from([("THREADS".into(), "4".into())]),
                ..EnvironmentPolicy::default()
            },
        },
        executor: None,
        resources: None,
        scientific_configurations: vec![Path::new("science.json").to_path_buf()],
        immutable_inputs: Vec::new(),
    }
}

#[test]
fn submission_freezes_git_command_defaults_and_file_hashes() -> TestResult {
    let (_temporary, project, config) = repository()?;
    let submission = build_submission(&project, &config, input(&project))?;
    assert_eq!(submission.priority, 7);
    assert_eq!(
        submission.attempt.result().failed_full_logs,
        Some(igor_core::FailedRetention::DeleteAfterDiagnosis)
    );
    assert_eq!(submission.attempt.command().args[2], "");
    assert_eq!(submission.attempt.resources(), &config.resources);
    assert_eq!(
        submission.attempt.result().failed_artifact_retention,
        Some(igor_core::FailedArtifactRetention {
            policy: igor_core::FailedRetention::DeleteAfterDiagnosis,
            keep_on_failure: vec![],
        })
    );
    assert_eq!(submission.attempt.configuration().contents.len(), 1);
    assert!(
        submission.attempt.configuration().contents[0]
            .sha256
            .starts_with("sha256:")
    );
    assert!(matches!(
        submission.attempt.source(),
        SourceIdentity::Git(identity)
            if !identity.dirty
                && identity.revision.len() == 40
                && identity.repository == "https://example.invalid/research.git"
    ));
    Ok(())
}

#[test]
fn results_extractor_is_frozen_directly_without_inheriting_environment() -> TestResult {
    let (_temporary, project, mut config) = repository()?;
    config.retention.failed_log_tail_kb = 1;
    config.retention.failed_artifacts = igor_core::FailedRetention::Keep;
    config.retention.failed_full_logs = igor_core::FailedRetention::Keep;
    config.artifacts.keep_on_failure = vec!["checkpoints/**".into()];
    fs::write(project.root.join("exporter"), "not executed")?;
    git(&project.root, &["add", "exporter"])?;
    git(&project.root, &["commit", "-qm", "add extractor"])?;
    config.results = Some(igor_core::ResultsConfig {
        extractor: vec!["./exporter".into(), "a b".into(), "".into()],
        working_directory: "attempt".into(),
    });
    let submission = build_submission(&project, &config, input(&project))?;
    let extractor = submission
        .attempt
        .result()
        .extractor
        .as_ref()
        .ok_or("extractor missing")?;
    assert_eq!(
        extractor.program,
        project
            .root
            .join("exporter")
            .canonicalize()?
            .to_string_lossy()
    );
    assert_eq!(extractor.args, ["a b", ""]);
    assert_eq!(extractor.shell, ShellPolicy::Direct);
    assert!(extractor.environment.set.is_empty());
    assert_eq!(extractor.environment.inherit, EnvironmentInheritance::None);
    assert_eq!(submission.attempt.result().failed_log_tail_kb(), 1);
    assert_eq!(
        submission.attempt.result().publication_root.as_deref(),
        Some(project.root.join("artifacts").as_path())
    );
    let frozen = submission.attempt.result().publication_root.clone();
    config.paths.artifact_root = project.root.join("custom-results");
    config.retention.failed_log_tail_kb = 2;
    config.retention.failed_full_logs = igor_core::FailedRetention::DeleteAfterDiagnosis;
    config.retention.failed_artifacts = igor_core::FailedRetention::DeleteAfterDiagnosis;
    config.artifacts.keep_on_failure.clear();
    config.retention.superseded_artifacts = igor_core::SupersededRetention::Keep;
    config.artifacts.keep_on_success = vec!["models/**".into()];
    let later = build_submission(&project, &config, input(&project))?;
    assert_eq!(
        submission.attempt.result().failed_full_logs,
        Some(igor_core::FailedRetention::Keep)
    );
    assert_eq!(
        later.attempt.result().failed_full_logs,
        Some(igor_core::FailedRetention::DeleteAfterDiagnosis)
    );
    assert_eq!(
        submission.attempt.result().superseded_artifact_retention,
        Some(igor_core::SupersededArtifactRetention {
            policy: igor_core::SupersededRetention::DeleteAfterReplacement,
            keep_on_success: vec![],
        })
    );
    assert_eq!(
        later.attempt.result().superseded_artifact_retention,
        Some(igor_core::SupersededArtifactRetention {
            policy: igor_core::SupersededRetention::Keep,
            keep_on_success: vec!["models/**".into()],
        })
    );
    assert_eq!(later.attempt.result().failed_log_tail_kb(), 2);
    assert_eq!(submission.attempt.result().failed_log_tail_kb(), 1);
    assert_eq!(
        submission.attempt.result().failed_artifact_retention,
        Some(igor_core::FailedArtifactRetention {
            policy: igor_core::FailedRetention::Keep,
            keep_on_failure: vec!["checkpoints/**".into()],
        })
    );
    assert_eq!(
        later
            .attempt
            .result()
            .failed_artifact_retention
            .as_ref()
            .ok_or("snapshot")?
            .policy,
        igor_core::FailedRetention::DeleteAfterDiagnosis
    );
    assert_eq!(
        later.attempt.result().publication_root.as_deref(),
        Some(config.paths.artifact_root.as_path())
    );
    assert_eq!(submission.attempt.result().publication_root, frozen);
    let mut legacy = serde_json::to_value(&submission.attempt)?;
    legacy["result"]
        .as_object_mut()
        .ok_or("result")?
        .remove("failed_full_logs");
    legacy["result"]
        .as_object_mut()
        .ok_or("missing result object")?
        .remove("publication_root");
    legacy["result"]
        .as_object_mut()
        .ok_or("missing result object")?
        .remove("failed_log_tail_kb");
    legacy["result"]
        .as_object_mut()
        .ok_or("missing result object")?
        .remove("failed_artifact_retention");
    legacy["result"]
        .as_object_mut()
        .ok_or("result")?
        .remove("superseded_artifact_retention");
    let legacy: igor_core::AttemptSpec = serde_json::from_value(legacy)?;
    assert!(legacy.result().publication_root.is_none());
    assert!(legacy.result().failed_full_logs.is_none());
    assert_eq!(legacy.result().failed_log_tail_kb(), 512);
    assert!(legacy.result().failed_artifact_retention.is_none());
    assert!(legacy.result().superseded_artifact_retention.is_none());
    config.paths.artifact_root = project.root.join("../escape");
    assert!(build_submission(&project, &config, input(&project)).is_err());
    Ok(())
}

#[cfg(unix)]
#[test]
fn results_extractor_symlink_cannot_escape_project() -> TestResult {
    use std::os::unix::fs::symlink;
    let (_temporary, project, mut config) = repository()?;
    let outside = project
        .root
        .parent()
        .ok_or("no parent")?
        .join("outside-extractor");
    fs::write(&outside, "not executed")?;
    symlink(&outside, project.root.join("linked-extractor"))?;
    git(&project.root, &["add", "linked-extractor"])?;
    git(&project.root, &["commit", "-qm", "add linked extractor"])?;
    config.results = Some(igor_core::ResultsConfig {
        extractor: vec!["./linked-extractor".into()],
        working_directory: "attempt".into(),
    });
    let error = build_submission(&project, &config, input(&project))
        .err()
        .ok_or("extractor symlink escape accepted")?;
    assert!(error.to_string().contains("escapes the project root"));
    Ok(())
}

#[test]
fn superseded_retention_snapshot_validates_lexical_paths_and_globs() -> TestResult {
    for pattern in [
        "/absolute",
        "../outside",
        "nested/../outside",
        "./model",
        "nested/./model",
        "[unclosed",
    ] {
        let snapshot = igor_core::SupersededArtifactRetention {
            policy: igor_core::SupersededRetention::DeleteAfterReplacement,
            keep_on_success: vec![pattern.into()],
        };
        assert!(snapshot.validate().is_err(), "{pattern}");
    }
    igor_core::SupersededArtifactRetention {
        policy: igor_core::SupersededRetention::Keep,
        keep_on_success: vec!["models/**".into(), "**/*.bin".into()],
    }
    .validate()?;
    Ok(())
}

#[test]
fn dirty_worktree_requires_explicit_policy_and_records_digest() -> TestResult {
    let (_temporary, project, config) = repository()?;
    fs::write(project.root.join("science.json"), b"changed")?;
    let error = build_submission(&project, &config, input(&project))
        .err()
        .ok_or("dirty worktree was accepted")?;
    assert!(error.to_string().contains("--allow-dirty"));

    let mut allowed = input(&project);
    allowed.allow_dirty = true;
    let submission = build_submission(&project, &config, allowed)?;
    assert!(matches!(
        submission.attempt.source(),
        SourceIdentity::Git(identity) if identity.dirty && identity.dirty_digest.is_some()
    ));
    Ok(())
}

#[test]
fn versioned_job_file_preserves_direct_and_shell_boundaries() -> TestResult {
    let (temporary, project, _config) = repository()?;
    let direct = temporary.path().join("direct.toml");
    fs::write(
        &direct,
        "schema_version = 1\n[execution]\nprogram = 'python'\nargs = ['', 'a b', '--flag']\n",
    )?;
    let direct = load_job_file(&direct)?.into_input(&project.root)?;
    assert_eq!(direct.command.args, ["", "a b", "--flag"]);
    assert_eq!(direct.command.shell, ShellPolicy::Direct);

    let shell = temporary.path().join("shell.toml");
    fs::write(
        &shell,
        "schema_version = 1\n[execution]\nshell_command = 'printf ok | tee result'\n",
    )?;
    let shell = load_job_file(&shell)?.into_input(&project.root)?;
    assert_eq!(shell.command.program, "/bin/sh");
    assert_eq!(shell.command.args, ["-c", "printf ok | tee result"]);
    assert_eq!(shell.command.shell, ShellPolicy::Shell);
    Ok(())
}

#[test]
fn secret_like_environment_values_are_rejected_before_persistence() -> TestResult {
    let (_temporary, project, config) = repository()?;
    for name in [
        "TELEGRAM_BOT_TOKEN",
        "API_KEY",
        "PRIVATE_KEY",
        "GITHUB_PAT",
        "DATABASE_URL",
        "AWS_ACCESS_KEY_ID",
        "SERVICE_CREDENTIAL",
    ] {
        let mut submission = input(&project);
        submission
            .command
            .environment
            .set
            .insert(name.into(), "do-not-store".into());
        let error = build_submission(&project, &config, submission)
            .err()
            .ok_or_else(|| format!("secret-like environment variable {name} was accepted"))?;
        let diagnostic = error.to_string();
        assert!(diagnostic.contains("cannot be persisted"));
        assert!(!diagnostic.contains("do-not-store"));
    }
    Ok(())
}

#[test]
fn command_working_directory_must_remain_inside_project() -> TestResult {
    let (temporary, project, config) = repository()?;
    let mut submission = input(&project);
    submission.command.cwd = temporary.path().to_path_buf();
    let error = build_submission(&project, &config, submission)
        .err()
        .ok_or("working directory outside the project was accepted")?;
    assert!(error.to_string().contains("escapes the project root"));
    Ok(())
}

#[test]
fn scp_style_remote_credentials_are_removed() -> TestResult {
    let (_temporary, project, config) = repository()?;
    git(
        &project.root,
        &[
            "remote",
            "set-url",
            "origin",
            "user@example.invalid:research.git",
        ],
    )?;
    let submission = build_submission(&project, &config, input(&project))?;
    assert!(matches!(
        submission.attempt.source(),
        SourceIdentity::Git(identity) if identity.repository == "example.invalid:research.git"
    ));
    Ok(())
}

#[test]
fn declared_symlinks_are_rejected_without_reading_the_target() -> TestResult {
    use std::os::unix::fs::symlink;

    let (_temporary, project, config) = repository()?;
    symlink("science.json", project.root.join("linked.json"))?;
    let mut submission = input(&project);
    submission.allow_dirty = true;
    submission.scientific_configurations = vec![Path::new("linked.json").to_path_buf()];
    let error = build_submission(&project, &config, submission)
        .err()
        .ok_or("declared symlink was accepted")?;
    assert!(error.to_string().contains("symbolic link"));
    Ok(())
}
