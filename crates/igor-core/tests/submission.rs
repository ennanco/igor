use std::{collections::BTreeMap, error::Error, fs, path::Path, process::Command};

use igor_core::{
    CommandSpec, EnvironmentPolicy, Project, ProjectId, ShellPolicy, SourceIdentity,
    SubmissionInput, build_submission, initialize_project, load_job_file, load_project_config,
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
    assert_eq!(submission.attempt.command().args[2], "");
    assert_eq!(submission.attempt.resources(), &config.resources);
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
