use std::{
    collections::BTreeSet,
    ffi::OsStr,
    fs::{self, OpenOptions},
    io::Read,
    os::fd::AsRawFd,
    os::unix::{
        ffi::OsStrExt,
        fs::{MetadataExt, OpenOptionsExt, PermissionsExt},
    },
    path::{Path, PathBuf},
    process::Command,
};

use serde::{Deserialize, Serialize};
use serde_json::json;
use sha2::{Digest, Sha256};
use thiserror::Error;

use crate::{
    AttemptId, AttemptSpec, CommandSpec, ConfigurationIdentity, ContentIdentity, ContentRole,
    DomainError, EnvironmentPolicy, Event, EventId, EventKind, EventPayload, ExecutorSpec,
    GitIdentity, JobId, JobSpec, Project, ProjectConfig, ResourceRequest, ResultContract,
    ShellPolicy, SourceIdentity,
};

pub const JOB_FILE_VERSION: u32 = 1;

#[derive(Debug, Error)]
pub enum SubmissionError {
    #[error("invalid submission: {0}")]
    Domain(#[from] DomainError),
    #[error("cannot read {path}: {source}")]
    Io {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("invalid job file {path}: {reason}")]
    JobFile { path: PathBuf, reason: String },
    #[error("Git provenance failed for {path}: {reason}")]
    Git { path: PathBuf, reason: String },
    #[error("declared {kind} {path} is invalid: {reason}")]
    Content {
        kind: &'static str,
        path: PathBuf,
        reason: String,
    },
    #[error("cannot serialize {contract}: {source}")]
    Serialization {
        contract: &'static str,
        #[source]
        source: serde_json::Error,
    },
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SubmissionInput {
    pub name: Option<String>,
    pub priority: i64,
    pub allow_dirty: bool,
    pub command: CommandSpec,
    pub executor: Option<ExecutorSpec>,
    pub resources: Option<ResourceRequest>,
    pub scientific_configurations: Vec<PathBuf>,
    pub immutable_inputs: Vec<PathBuf>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct JobFile {
    pub schema_version: u32,
    #[serde(default)]
    pub name: Option<String>,
    #[serde(default)]
    pub priority: i64,
    #[serde(default)]
    pub allow_dirty: bool,
    pub execution: JobExecution,
    #[serde(default)]
    pub environment: EnvironmentPolicy,
    #[serde(default)]
    pub executor: Option<ExecutorSpec>,
    #[serde(default)]
    pub resources: Option<ResourceRequest>,
    #[serde(default)]
    pub scientific_configurations: Vec<PathBuf>,
    #[serde(default)]
    pub immutable_inputs: Vec<PathBuf>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct JobExecution {
    #[serde(default)]
    pub program: Option<String>,
    #[serde(default)]
    pub args: Vec<String>,
    #[serde(default)]
    pub cwd: Option<PathBuf>,
    #[serde(default)]
    pub shell_command: Option<String>,
}

impl JobFile {
    pub fn into_input(self, project_root: &Path) -> Result<SubmissionInput, SubmissionError> {
        if self.schema_version != JOB_FILE_VERSION {
            return Err(SubmissionError::JobFile {
                path: PathBuf::from("schema_version"),
                reason: format!(
                    "unsupported version {}; supported version is {JOB_FILE_VERSION}",
                    self.schema_version
                ),
            });
        }
        let cwd = resolve_cwd(project_root, self.execution.cwd.as_deref())?;
        let command = match (self.execution.program, self.execution.shell_command) {
            (Some(program), None) => CommandSpec {
                program,
                args: self.execution.args,
                cwd,
                shell: ShellPolicy::Direct,
                environment: self.environment,
            },
            (None, Some(shell_command)) if self.execution.args.is_empty() => CommandSpec {
                program: "/bin/sh".into(),
                args: vec!["-c".into(), shell_command],
                cwd,
                shell: ShellPolicy::Shell,
                environment: self.environment,
            },
            _ => {
                return Err(SubmissionError::JobFile {
                    path: PathBuf::from("execution"),
                    reason: "specify either program with args or shell_command without args".into(),
                });
            }
        };
        Ok(SubmissionInput {
            name: self.name,
            priority: self.priority,
            allow_dirty: self.allow_dirty,
            command,
            executor: self.executor,
            resources: self.resources,
            scientific_configurations: self.scientific_configurations,
            immutable_inputs: self.immutable_inputs,
        })
    }
}

pub fn load_job_file(path: &Path) -> Result<JobFile, SubmissionError> {
    let contents = fs::read_to_string(path).map_err(|source| SubmissionError::Io {
        path: path.to_path_buf(),
        source,
    })?;
    toml::from_str(&contents).map_err(|error| SubmissionError::JobFile {
        path: path.to_path_buf(),
        reason: error.to_string(),
    })
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Submission {
    pub job: JobSpec,
    pub attempt: AttemptSpec,
    pub priority: i64,
    pub job_event: Event,
    pub attempt_event: Event,
}

pub fn build_submission(
    project: &Project,
    config: &ProjectConfig,
    mut input: SubmissionInput,
) -> Result<Submission, SubmissionError> {
    project.validate()?;
    config.validate()?;
    input.command.cwd = resolve_cwd(&project.root, Some(&input.command.cwd))?;
    input.command.validate()?;
    reject_secret_environment(&input.command.environment)?;
    let before = capture_git(&project.root, input.allow_dirty)?;

    let mut declarations = Vec::new();
    declarations.extend(
        config
            .provenance
            .scientific_configurations
            .iter()
            .cloned()
            .map(|path| (path, ContentRole::ScientificConfiguration)),
    );
    declarations.extend(
        input
            .scientific_configurations
            .drain(..)
            .map(|path| (path, ContentRole::ScientificConfiguration)),
    );
    declarations.extend(
        config
            .provenance
            .immutable_inputs
            .iter()
            .cloned()
            .map(|path| (path, ContentRole::ImmutableInput)),
    );
    declarations.extend(
        input
            .immutable_inputs
            .drain(..)
            .map(|path| (path, ContentRole::ImmutableInput)),
    );
    let contents = hash_declared_contents(&project.root, declarations)?;
    let executor = input.executor.unwrap_or_default();
    let resources = input.resources.unwrap_or_else(|| config.resources.clone());
    let name = input.name.unwrap_or_else(|| default_name(&input.command));
    let project_digest = digest_json(config, "project configuration")?;
    let job_id = JobId::new();
    let job = JobSpec {
        id: job_id,
        project_id: project.id,
        name,
        command: input.command,
        executor,
        resources,
        retry: config.attempt_retry.clone(),
        family: None,
    };
    job.validate()?;
    let job_digest = digest_json(&job, "effective job")?;
    let after = capture_git(&project.root, input.allow_dirty)?;
    if before != after {
        return Err(SubmissionError::Git {
            path: project.root.clone(),
            reason: "worktree changed while the submission was being frozen; retry".into(),
        });
    }
    let attempt_id = AttemptId::new();
    let attempt = AttemptSpec::from_job(
        attempt_id,
        1,
        &job,
        SourceIdentity::Git(before),
        ConfigurationIdentity {
            project_digest,
            job_digest,
            contents,
        },
        ResultContract {
            schema_version: 1,
            extractor: None,
        },
    )?;
    let job_event = event(
        EventKind::JobSubmitted,
        json!({"job_id": job_id, "project_id": project.id}),
    )?;
    let attempt_event = event(
        EventKind::AttemptCreated,
        json!({"job_id": job_id, "attempt_id": attempt_id, "sequence": 1}),
    )?;
    Ok(Submission {
        job,
        attempt,
        priority: input.priority,
        job_event,
        attempt_event,
    })
}

fn resolve_cwd(project_root: &Path, cwd: Option<&Path>) -> Result<PathBuf, SubmissionError> {
    let selected = cwd.unwrap_or_else(|| Path::new("."));
    let selected = if selected.is_absolute() {
        selected.to_path_buf()
    } else {
        project_root.join(selected)
    };
    let canonical = selected
        .canonicalize()
        .map_err(|source| SubmissionError::Io {
            path: selected.clone(),
            source,
        })?;
    let root = project_root
        .canonicalize()
        .map_err(|source| SubmissionError::Io {
            path: project_root.to_path_buf(),
            source,
        })?;
    if !canonical.starts_with(&root) {
        return Err(SubmissionError::Content {
            kind: "working directory",
            path: canonical,
            reason: "escapes the project root".into(),
        });
    }
    Ok(canonical)
}

fn capture_git(root: &Path, allow_dirty: bool) -> Result<GitIdentity, SubmissionError> {
    let repository_root = git_text(root, &["rev-parse", "--show-toplevel"])?;
    let repository_root = PathBuf::from(repository_root)
        .canonicalize()
        .map_err(|source| SubmissionError::Io {
            path: root.to_path_buf(),
            source,
        })?;
    let revision = git_text(&repository_root, &["rev-parse", "HEAD"])?;
    let branch = git_optional_text(
        &repository_root,
        &["symbolic-ref", "--quiet", "--short", "HEAD"],
    )?;
    let remote = git_optional_text(&repository_root, &["config", "--get", "remote.origin.url"])?;
    let status = git_bytes(
        &repository_root,
        &["status", "--porcelain=v1", "-z", "--untracked-files=all"],
    )?;
    let dirty = !status.is_empty();
    if dirty && !allow_dirty {
        return Err(SubmissionError::Git {
            path: root.to_path_buf(),
            reason: "worktree is dirty; commit the changes or pass --allow-dirty".into(),
        });
    }
    let dirty_digest = dirty.then(|| {
        let mut hasher = Sha256::new();
        hasher.update(&status);
        let diff = git_bytes(&repository_root, &["diff", "--binary", "HEAD"])?;
        hasher.update(diff);
        for entry in status.split(|byte| *byte == 0) {
            if let Some(path) = entry.strip_prefix(b"?? ") {
                let path = repository_root.join(Path::new(OsStr::from_bytes(path)));
                let mut file = OpenOptions::new()
                    .read(true)
                    .custom_flags(libc::O_NOFOLLOW)
                    .open(&path)
                    .map_err(|source| SubmissionError::Io {
                        path: path.clone(),
                        source,
                    })?;
                let metadata = file.metadata().map_err(|source| SubmissionError::Io {
                    path: path.clone(),
                    source,
                })?;
                if !metadata.is_file() {
                    return Err(SubmissionError::Git {
                        path,
                        reason: "untracked content must be a regular file".into(),
                    });
                }
                hasher.update((entry.len() as u64).to_le_bytes());
                hasher.update(entry);
                hasher.update(metadata.len().to_le_bytes());
                hasher.update((metadata.permissions().mode() & 0o111).to_le_bytes());
                hash_reader(&mut file, &mut hasher, &path)?;
                let after = file.metadata().map_err(|source| SubmissionError::Io {
                    path: path.clone(),
                    source,
                })?;
                if !same_file_state(&metadata, &after) {
                    return Err(SubmissionError::Git {
                        path,
                        reason: "untracked content changed while it was being hashed; retry".into(),
                    });
                }
            }
        }
        Ok::<_, SubmissionError>(format!("sha256:{:x}", hasher.finalize()))
    });
    let dirty_digest = dirty_digest.transpose()?;
    let repository = remote
        .map(|value| sanitize_remote(&value))
        .unwrap_or_else(|| format!("file://{}", repository_root.display()));
    Ok(GitIdentity {
        repository,
        root: repository_root,
        branch,
        revision,
        dirty,
        dirty_digest,
    })
}

fn git_text(root: &Path, args: &[&str]) -> Result<String, SubmissionError> {
    let bytes = git_bytes(root, args)?;
    String::from_utf8(bytes)
        .map(|value| value.trim().to_owned())
        .map_err(|error| SubmissionError::Git {
            path: root.to_path_buf(),
            reason: format!("Git returned non-UTF-8 output: {error}"),
        })
}

fn git_optional_text(root: &Path, args: &[&str]) -> Result<Option<String>, SubmissionError> {
    let output = Command::new("git")
        .arg("-C")
        .arg(root)
        .args(args)
        .output()
        .map_err(|source| SubmissionError::Git {
            path: root.to_path_buf(),
            reason: source.to_string(),
        })?;
    if output.status.success() {
        String::from_utf8(output.stdout)
            .map(|value| Some(value.trim().to_owned()))
            .map_err(|error| SubmissionError::Git {
                path: root.to_path_buf(),
                reason: format!("Git returned non-UTF-8 output: {error}"),
            })
    } else {
        Ok(None)
    }
}

fn git_bytes(root: &Path, args: &[&str]) -> Result<Vec<u8>, SubmissionError> {
    let output = Command::new("git")
        .arg("-C")
        .arg(root)
        .args(args)
        .output()
        .map_err(|source| SubmissionError::Git {
            path: root.to_path_buf(),
            reason: source.to_string(),
        })?;
    if output.status.success() {
        Ok(output.stdout)
    } else {
        let reason = String::from_utf8_lossy(&output.stderr).trim().to_owned();
        Err(SubmissionError::Git {
            path: root.to_path_buf(),
            reason: if reason.is_empty() {
                format!("git exited with {}", output.status)
            } else {
                reason
            },
        })
    }
}

fn sanitize_remote(remote: &str) -> String {
    if let Some(scheme) = remote.find("://") {
        let authority = scheme + 3;
        return match remote[authority..].find('@') {
            Some(at) => format!("{}{}", &remote[..authority], &remote[authority + at + 1..]),
            None => remote.to_owned(),
        };
    }
    remote
        .find('@')
        .map_or_else(|| remote.to_owned(), |at| remote[at + 1..].to_owned())
}

fn reject_secret_environment(environment: &EnvironmentPolicy) -> Result<(), SubmissionError> {
    for name in environment.set.keys() {
        let uppercase = name.to_ascii_uppercase();
        if uppercase.starts_with("IGOR_")
            || uppercase.contains("TOKEN")
            || uppercase.contains("SECRET")
            || uppercase.contains("PASSWORD")
            || uppercase.contains("CREDENTIAL")
            || uppercase.ends_with("_KEY")
            || uppercase.ends_with("_PAT")
            || uppercase == "DATABASE_URL"
            || uppercase.starts_with("AWS_ACCESS_KEY")
        {
            return Err(SubmissionError::JobFile {
                path: PathBuf::from("environment.set").join(name),
                reason:
                    "secret-like environment variables cannot be persisted in job specifications"
                        .into(),
            });
        }
    }
    Ok(())
}

fn hash_declared_contents(
    project_root: &Path,
    declarations: Vec<(PathBuf, ContentRole)>,
) -> Result<Vec<ContentIdentity>, SubmissionError> {
    let root = project_root
        .canonicalize()
        .map_err(|source| SubmissionError::Io {
            path: project_root.to_path_buf(),
            source,
        })?;
    let mut seen = BTreeSet::new();
    let mut identities = Vec::new();
    for (declared, role) in declarations {
        let candidate = if declared.is_absolute() {
            declared
        } else {
            root.join(declared)
        };
        let metadata =
            fs::symlink_metadata(&candidate).map_err(|source| SubmissionError::Content {
                kind: role_name(role),
                path: candidate.clone(),
                reason: source.to_string(),
            })?;
        if metadata.file_type().is_symlink() || !metadata.is_file() {
            return Err(SubmissionError::Content {
                kind: role_name(role),
                path: candidate,
                reason: "must be a regular file and not a symbolic link".into(),
            });
        }
        let mut file = OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_NOFOLLOW)
            .open(&candidate)
            .map_err(|source| SubmissionError::Content {
                kind: role_name(role),
                path: candidate.clone(),
                reason: source.to_string(),
            })?;
        let opened_metadata = file.metadata().map_err(|source| SubmissionError::Content {
            kind: role_name(role),
            path: candidate.clone(),
            reason: source.to_string(),
        })?;
        if !opened_metadata.is_file() {
            return Err(SubmissionError::Content {
                kind: role_name(role),
                path: candidate,
                reason: "opened descriptor is not a regular file".into(),
            });
        }
        let canonical =
            fs::read_link(format!("/proc/self/fd/{}", file.as_raw_fd())).map_err(|source| {
                SubmissionError::Content {
                    kind: role_name(role),
                    path: candidate.clone(),
                    reason: source.to_string(),
                }
            })?;
        if !canonical.starts_with(&root) {
            return Err(SubmissionError::Content {
                kind: role_name(role),
                path: canonical,
                reason: "escapes the project root".into(),
            });
        }
        if !seen.insert((canonical.clone(), role as u8)) {
            continue;
        }
        let mut digest = Sha256::new();
        hash_reader(&mut file, &mut digest, &canonical)?;
        let after = file.metadata().map_err(|source| SubmissionError::Content {
            kind: role_name(role),
            path: canonical.clone(),
            reason: source.to_string(),
        })?;
        if !same_file_state(&opened_metadata, &after) {
            return Err(SubmissionError::Content {
                kind: role_name(role),
                path: canonical,
                reason: "changed while it was being hashed; retry".into(),
            });
        }
        identities.push(ContentIdentity {
            path: canonical,
            role,
            sha256: format!("sha256:{:x}", digest.finalize()),
            size_bytes: opened_metadata.len(),
        });
    }
    identities.sort_by(|left, right| left.path.cmp(&right.path).then(left.role.cmp(&right.role)));
    Ok(identities)
}

fn same_file_state(before: &fs::Metadata, after: &fs::Metadata) -> bool {
    before.dev() == after.dev()
        && before.ino() == after.ino()
        && before.len() == after.len()
        && before.mtime() == after.mtime()
        && before.mtime_nsec() == after.mtime_nsec()
        && before.ctime() == after.ctime()
        && before.ctime_nsec() == after.ctime_nsec()
}

fn hash_reader(
    reader: &mut impl Read,
    hasher: &mut Sha256,
    path: &Path,
) -> Result<(), SubmissionError> {
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        let read = reader
            .read(&mut buffer)
            .map_err(|source| SubmissionError::Io {
                path: path.to_path_buf(),
                source,
            })?;
        if read == 0 {
            return Ok(());
        }
        hasher.update(&buffer[..read]);
    }
}

fn role_name(role: ContentRole) -> &'static str {
    match role {
        ContentRole::ScientificConfiguration => "scientific configuration",
        ContentRole::ImmutableInput => "immutable input",
    }
}

fn digest_json<T: Serialize>(value: &T, contract: &'static str) -> Result<String, SubmissionError> {
    let bytes = serde_json::to_vec(value)
        .map_err(|source| SubmissionError::Serialization { contract, source })?;
    Ok(format!("sha256:{:x}", Sha256::digest(bytes)))
}

fn default_name(command: &CommandSpec) -> String {
    if command.shell == ShellPolicy::Shell {
        let text = command.args.get(1).map(String::as_str).unwrap_or("command");
        let prefix: String = text.chars().take(48).collect();
        format!("shell: {prefix}")
    } else {
        Path::new(&command.program)
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or(&command.program)
            .to_owned()
    }
}

fn event(kind: EventKind, data: serde_json::Value) -> Result<Event, SubmissionError> {
    Ok(Event::new(
        EventId::new(),
        kind,
        EventPayload::new(kind, 1, data)?,
    )?)
}
