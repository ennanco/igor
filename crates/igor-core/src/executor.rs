use std::{
    collections::BTreeSet,
    ffi::OsString,
    path::{Component, Path, PathBuf},
    process::Command,
};

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{
    AttemptId, CommandSpec, DomainError, ErrorCode, GenerationId, JobId, ProjectId, Result,
    environment_variable_is_sensitive,
};

const DOCKER_PROGRAM: &str = "docker";
const DOCKER_IMAGE_ID_FORMAT: &str = "{{.Id}}";
const DOCKER_SERVER_VERSION_FORMAT: &str = "{{.Server.Version}}";
const DOCKER_STATE_FORMAT: &str = "{{json .State}}";
const DOCKER_RECOVERY_FORMAT: &str = "{{json .}}";

#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ProcessIsolation {
    #[default]
    ProcessGroup,
    SystemdUserUnit,
}

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct ProcessExecutorSpec {
    pub isolation: ProcessIsolation,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum MountAccess {
    ReadOnly,
    ReadWrite,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum DockerContainerState {
    Created,
    Running,
    Exited,
    Removed,
    Lost,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DockerContainerInspection {
    pub status: String,
    pub running: bool,
    pub exit_code: i32,
    pub oom_killed: bool,
    pub error: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DockerRecoveryIdentity {
    pub id: String,
    pub name: String,
    pub image_id: String,
    pub labels: std::collections::BTreeMap<String, String>,
    pub state: DockerContainerInspection,
}

#[derive(Debug, Error, Eq, PartialEq)]
pub enum DockerOutputParseError {
    #[error("Docker {operation} output is not valid UTF-8")]
    NonUtf8 { operation: &'static str },
    #[error("Docker {operation} output must contain exactly one {expected}")]
    InvalidShape {
        operation: &'static str,
        expected: &'static str,
    },
    #[error("Docker wait output is not a signed 32-bit exit code: {0:?}")]
    InvalidExitCode(String),
    #[error("Docker inspect output is invalid JSON state: {0}")]
    InvalidState(String),
    #[error("Docker recovery output is invalid JSON: {0}")]
    InvalidRecovery(String),
    #[error("Docker recovery candidate ID is invalid: {0:?}")]
    InvalidCandidateId(String),
}

pub fn parse_docker_create_stdout(
    stdout: &[u8],
) -> std::result::Result<String, DockerOutputParseError> {
    let text = std::str::from_utf8(stdout).map_err(|_| DockerOutputParseError::NonUtf8 {
        operation: "create",
    })?;
    let mut tokens = text.split_whitespace();
    let id = tokens.next().ok_or(DockerOutputParseError::InvalidShape {
        operation: "create",
        expected: "container ID token",
    })?;
    if tokens.next().is_some() {
        return Err(DockerOutputParseError::InvalidShape {
            operation: "create",
            expected: "container ID token",
        });
    }
    Ok(id.to_owned())
}

pub fn parse_docker_wait_stdout(stdout: &[u8]) -> std::result::Result<i32, DockerOutputParseError> {
    let text = std::str::from_utf8(stdout)
        .map_err(|_| DockerOutputParseError::NonUtf8 { operation: "wait" })?
        .trim();
    text.parse::<i32>()
        .map_err(|_| DockerOutputParseError::InvalidExitCode(text.to_owned()))
}

pub fn parse_docker_inspect_stdout(
    stdout: &[u8],
) -> std::result::Result<DockerContainerInspection, DockerOutputParseError> {
    let text = std::str::from_utf8(stdout).map_err(|_| DockerOutputParseError::NonUtf8 {
        operation: "inspect",
    })?;
    #[derive(Deserialize)]
    #[serde(deny_unknown_fields)]
    struct State {
        #[serde(rename = "Status")]
        status: String,
        #[serde(rename = "Running")]
        running: bool,
        #[serde(rename = "ExitCode")]
        exit_code: i32,
        #[serde(rename = "OOMKilled")]
        oom_killed: bool,
        #[serde(rename = "Error")]
        error: Option<String>,
    }
    let state: State = serde_json::from_str(text)
        .map_err(|error| DockerOutputParseError::InvalidState(error.to_string()))?;
    Ok(DockerContainerInspection {
        status: state.status,
        running: state.running,
        exit_code: state.exit_code,
        oom_killed: state.oom_killed,
        error: state.error.filter(|error| !error.is_empty()),
    })
}

pub fn parse_docker_recovery_candidates(
    stdout: &[u8],
) -> std::result::Result<Vec<String>, DockerOutputParseError> {
    let text = std::str::from_utf8(stdout)
        .map_err(|_| DockerOutputParseError::NonUtf8 { operation: "ps" })?;
    text.lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .map(|line| {
            if line.chars().any(char::is_whitespace) {
                Err(DockerOutputParseError::InvalidCandidateId(line.to_owned()))
            } else {
                Ok(line.to_owned())
            }
        })
        .collect()
}

pub fn parse_docker_recovery_inspect(
    stdout: &[u8],
) -> std::result::Result<DockerRecoveryIdentity, DockerOutputParseError> {
    let text = std::str::from_utf8(stdout).map_err(|_| DockerOutputParseError::NonUtf8 {
        operation: "recovery inspect",
    })?;
    #[derive(Deserialize)]
    struct Raw {
        #[serde(rename = "Id")]
        id: String,
        #[serde(rename = "Name")]
        name: String,
        #[serde(rename = "Image")]
        image_id: String,
        #[serde(rename = "Config")]
        config: RecoveryConfig,
        #[serde(rename = "State")]
        state: State,
    }
    #[derive(Deserialize)]
    struct RecoveryConfig {
        #[serde(rename = "Labels")]
        labels: Option<std::collections::BTreeMap<String, String>>,
    }
    #[derive(Deserialize)]
    struct State {
        #[serde(rename = "Status")]
        status: String,
        #[serde(rename = "Running")]
        running: bool,
        #[serde(rename = "ExitCode")]
        exit_code: i32,
        #[serde(rename = "OOMKilled")]
        oom_killed: bool,
        #[serde(rename = "Error")]
        error: Option<String>,
    }
    let raw: Raw = serde_json::from_str(text)
        .map_err(|error| DockerOutputParseError::InvalidRecovery(error.to_string()))?;
    Ok(DockerRecoveryIdentity {
        id: raw.id,
        name: raw.name,
        image_id: raw.image_id,
        labels: raw.config.labels.unwrap_or_default(),
        state: DockerContainerInspection {
            status: raw.state.status,
            running: raw.state.running,
            exit_code: raw.state.exit_code,
            oom_killed: raw.state.oom_killed,
            error: raw.state.error.filter(|error| !error.is_empty()),
        },
    })
}

impl DockerContainerState {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Created => "created",
            Self::Running => "running",
            Self::Exited => "exited",
            Self::Removed => "removed",
            Self::Lost => "lost",
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct DockerMount {
    pub source: PathBuf,
    pub target: PathBuf,
    pub access: MountAccess,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct DockerExecutorSpec {
    pub image: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub digest: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workdir: Option<PathBuf>,
    #[serde(default)]
    pub mounts: Vec<DockerMount>,
    #[serde(default = "default_true")]
    pub remove_container: bool,
}

const fn default_true() -> bool {
    true
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "kind", content = "settings", rename_all = "snake_case")]
pub enum ExecutorSpec {
    Process(ProcessExecutorSpec),
    Docker(DockerExecutorSpec),
}

impl ExecutorSpec {
    pub fn validate(&self) -> Result<()> {
        match self {
            Self::Process(_) => Ok(()),
            Self::Docker(spec) => spec.validate(),
        }
    }
}

impl DockerExecutorSpec {
    pub fn validate(&self) -> Result<()> {
        if self.image.trim().is_empty()
            || self.image.chars().any(char::is_whitespace)
            || self.image.chars().any(char::is_control)
            || self.image.contains('@')
        {
            return Err(executor_validation(
                "executor.settings.image",
                "must be a non-empty tag or image name without whitespace or an embedded digest",
            ));
        }
        if let Some(digest) = &self.digest
            && !is_sha256_digest(digest)
        {
            return Err(executor_validation(
                "executor.settings.digest",
                "must be sha256 followed by 64 lowercase hexadecimal characters",
            ));
        }
        if let Some(workdir) = &self.workdir {
            validate_absolute_container_path(workdir, "executor.settings.workdir")?;
        }
        let mut targets = BTreeSet::new();
        for mount in &self.mounts {
            validate_absolute_mount_path(&mount.source, "executor.settings.mounts.source")?;
            validate_absolute_container_path(&mount.target, "executor.settings.mounts.target")?;
            if !targets.insert(&mount.target) {
                return Err(executor_validation(
                    "executor.settings.mounts.target",
                    "mount targets must be unique",
                ));
            }
        }
        Ok(())
    }

    #[must_use]
    pub fn image_reference(&self) -> String {
        self.digest.as_ref().map_or_else(
            || self.image.clone(),
            |digest| format!("{}@{digest}", self.image),
        )
    }
}

fn is_sha256_digest(value: &str) -> bool {
    value.strip_prefix("sha256:").is_some_and(|digest| {
        digest.len() == 64
            && digest
                .bytes()
                .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
    })
}

fn validate_absolute_mount_path(path: &Path, field: &'static str) -> Result<()> {
    validate_absolute_path(path, field)?;
    if path.as_os_str().as_encoded_bytes().contains(&b',') {
        return Err(executor_validation(
            field,
            "must not contain a comma because Docker uses it as a mount separator",
        ));
    }
    Ok(())
}

fn validate_absolute_container_path(path: &Path, field: &'static str) -> Result<()> {
    validate_absolute_mount_path(path, field)?;
    if path.to_str().is_none() {
        return Err(executor_validation(field, "must be valid UTF-8"));
    }
    Ok(())
}

fn validate_absolute_path(path: &Path, field: &'static str) -> Result<()> {
    if !path.is_absolute() {
        return Err(executor_validation(field, "must be absolute"));
    }
    if path
        .components()
        .any(|component| matches!(component, Component::CurDir | Component::ParentDir))
    {
        return Err(executor_validation(
            field,
            "must not contain current-directory or parent-directory components",
        ));
    }
    Ok(())
}

fn executor_validation(field: &'static str, reason: &str) -> DomainError {
    DomainError::validation(ErrorCode::InvalidExecutor, field, reason)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DockerIdentity {
    pub project_id: ProjectId,
    pub job_id: JobId,
    pub attempt_id: AttemptId,
    pub generation_id: Option<GenerationId>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DockerCommand {
    program: PathBuf,
    args: Vec<OsString>,
}

impl DockerCommand {
    #[must_use]
    pub fn program(&self) -> &Path {
        &self.program
    }

    #[must_use]
    pub fn args(&self) -> &[OsString] {
        &self.args
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DockerCreatePlan {
    pub container_name: String,
    pub image_reference: String,
    pub remove_container: bool,
    pub command: DockerCommand,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DockerImageIdentity {
    pub reference: String,
    pub image_id: String,
}

impl DockerImageIdentity {
    pub fn validate(&self) -> Result<()> {
        if self.reference.trim().is_empty() {
            return Err(executor_validation(
                "executor.settings.image",
                "resolved image reference must not be empty",
            ));
        }
        if !is_sha256_digest(&self.image_id) {
            return Err(executor_validation(
                "executor.settings.image",
                "resolved image ID must be a canonical sha256 digest",
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Error)]
pub enum DockerImageError {
    #[error("invalid Docker image specification: {0}")]
    InvalidSpec(#[from] DomainError),
    #[error("cannot inspect local Docker image {reference}: {reason}")]
    Inspect { reference: String, reason: String },
    #[error("Docker returned an invalid image ID {image_id:?} for {reference}")]
    InvalidImageId { reference: String, image_id: String },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DockerCommandPlanner {
    program: PathBuf,
}

impl Default for DockerCommandPlanner {
    fn default() -> Self {
        Self::new(DOCKER_PROGRAM)
    }
}

impl DockerCommandPlanner {
    pub fn new(program: impl Into<PathBuf>) -> Self {
        Self {
            program: program.into(),
        }
    }

    #[must_use]
    pub fn client_version(&self) -> DockerCommand {
        self.command([OsString::from("--version")])
    }

    #[must_use]
    pub fn server_version(&self) -> DockerCommand {
        self.command([
            OsString::from("version"),
            OsString::from("--format"),
            OsString::from(DOCKER_SERVER_VERSION_FORMAT),
        ])
    }

    #[must_use]
    pub fn image_inspect(&self, image: &str) -> DockerCommand {
        self.command([
            OsString::from("image"),
            OsString::from("inspect"),
            OsString::from("--format"),
            OsString::from(DOCKER_IMAGE_ID_FORMAT),
            OsString::from(image),
        ])
    }

    pub fn create(
        &self,
        spec: &DockerExecutorSpec,
        command: &CommandSpec,
        identity: DockerIdentity,
        assigned_gpus: &[String],
    ) -> Result<DockerCreatePlan> {
        spec.validate()?;
        command.validate()?;
        if let Some(name) =
            command.environment.set.keys().find(|name| {
                invalid_environment_name(name) || environment_variable_is_sensitive(name)
            })
        {
            return Err(executor_validation(
                "command.environment.set",
                &format!("variable {name:?} is invalid or sensitive"),
            ));
        }
        if let Some(device) = assigned_gpus
            .iter()
            .find(|device| !valid_gpu_identity(device))
        {
            return Err(executor_validation(
                "resources.gpu",
                &format!("assigned GPU identity {device:?} is invalid"),
            ));
        }

        let container_name = format!("igor-{}", identity.attempt_id);
        let image_reference = spec.image_reference();
        let mut args = vec![
            OsString::from("create"),
            OsString::from("--name"),
            OsString::from(&container_name),
        ];
        push_label(&mut args, "igor.project_id", identity.project_id);
        push_label(&mut args, "igor.job_id", identity.job_id);
        push_label(&mut args, "igor.attempt_id", identity.attempt_id);
        if let Some(generation_id) = identity.generation_id {
            push_label(&mut args, "igor.generation_id", generation_id);
        }
        if let Some(workdir) = &spec.workdir {
            args.push(OsString::from("--workdir"));
            args.push(workdir.as_os_str().to_owned());
        }
        for mount in &spec.mounts {
            args.push(OsString::from("--mount"));
            args.push(mount_argument(mount));
        }
        for (name, value) in &command.environment.set {
            args.push(OsString::from("--env"));
            args.push(OsString::from(format!("{name}={value}")));
        }
        if !assigned_gpus.is_empty() {
            args.push(OsString::from("--gpus"));
            args.push(OsString::from(format!(
                "device={}",
                assigned_gpus.join(",")
            )));
        }
        args.push(OsString::from(&image_reference));
        args.push(OsString::from(&command.program));
        args.extend(command.args.iter().map(OsString::from));
        Ok(DockerCreatePlan {
            container_name,
            image_reference,
            remove_container: spec.remove_container,
            command: self.command(args),
        })
    }

    #[must_use]
    pub fn start(&self, container_id: &str) -> DockerCommand {
        self.container_command("start", container_id)
    }

    #[must_use]
    pub fn wait(&self, container_id: &str) -> DockerCommand {
        self.container_command("wait", container_id)
    }

    #[must_use]
    pub fn logs(&self, container_id: &str) -> DockerCommand {
        self.command([
            OsString::from("logs"),
            OsString::from("--follow"),
            OsString::from(container_id),
        ])
    }

    #[must_use]
    pub fn logs_snapshot(&self, container_id: &str) -> DockerCommand {
        self.command([OsString::from("logs"), OsString::from(container_id)])
    }

    #[must_use]
    pub fn recovery_candidates(&self, identity: DockerIdentity) -> DockerCommand {
        let mut args = vec![
            OsString::from("ps"),
            OsString::from("--all"),
            OsString::from("--quiet"),
            OsString::from("--filter"),
        ];
        args.push(OsString::from(format!(
            "label=igor.project_id={}",
            identity.project_id
        )));
        args.push(OsString::from("--filter"));
        args.push(OsString::from(format!(
            "label=igor.job_id={}",
            identity.job_id
        )));
        args.push(OsString::from("--filter"));
        args.push(OsString::from(format!(
            "label=igor.attempt_id={}",
            identity.attempt_id
        )));
        if let Some(generation_id) = identity.generation_id {
            args.push(OsString::from("--filter"));
            args.push(OsString::from(format!(
                "label=igor.generation_id={generation_id}"
            )));
        }
        self.command(args)
    }

    #[must_use]
    pub fn recovery_inspect(&self, container_id: &str) -> DockerCommand {
        self.command([
            OsString::from("inspect"),
            OsString::from("--format"),
            OsString::from(DOCKER_RECOVERY_FORMAT),
            OsString::from(container_id),
        ])
    }

    #[must_use]
    pub fn stop(&self, container_id: &str, grace_seconds: u64) -> DockerCommand {
        self.command([
            OsString::from("stop"),
            OsString::from("--time"),
            OsString::from(grace_seconds.to_string()),
            OsString::from(container_id),
        ])
    }

    #[must_use]
    pub fn kill(&self, container_id: &str) -> DockerCommand {
        self.container_command("kill", container_id)
    }

    #[must_use]
    pub fn inspect(&self, container_id: &str) -> DockerCommand {
        self.command([
            OsString::from("inspect"),
            OsString::from("--format"),
            OsString::from(DOCKER_STATE_FORMAT),
            OsString::from(container_id),
        ])
    }

    #[must_use]
    pub fn remove(&self, container_id: &str) -> DockerCommand {
        self.command([
            OsString::from("rm"),
            OsString::from("--force"),
            OsString::from(container_id),
        ])
    }

    fn container_command(&self, operation: &str, container_id: &str) -> DockerCommand {
        self.command([OsString::from(operation), OsString::from(container_id)])
    }

    fn command(&self, args: impl IntoIterator<Item = OsString>) -> DockerCommand {
        DockerCommand {
            program: self.program.clone(),
            args: args.into_iter().collect(),
        }
    }
}

fn invalid_environment_name(name: &str) -> bool {
    name.is_empty() || name.contains('=') || name.chars().any(char::is_control)
}

fn valid_gpu_identity(device: &str) -> bool {
    !device.is_empty()
        && device.bytes().all(|byte| {
            byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b':' | b'/')
        })
}

fn push_label(args: &mut Vec<OsString>, name: &str, value: impl std::fmt::Display) {
    args.push(OsString::from("--label"));
    args.push(OsString::from(format!("{name}={value}")));
}

fn mount_argument(mount: &DockerMount) -> OsString {
    let mut argument = OsString::from("type=bind,src=");
    argument.push(&mount.source);
    argument.push(",dst=");
    argument.push(&mount.target);
    if mount.access == MountAccess::ReadOnly {
        argument.push(",readonly");
    }
    argument
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DockerCapability {
    Unavailable {
        error: String,
    },
    CliOnly {
        client_version: String,
        daemon_error: String,
    },
    Available {
        client_version: String,
        server_version: String,
    },
}

#[must_use]
pub fn detect_docker(program: impl AsRef<Path>) -> DockerCapability {
    let planner = DockerCommandPlanner::new(program.as_ref());
    let client = match run_probe(&planner.client_version()) {
        Ok(version) => version,
        Err(error) => return DockerCapability::Unavailable { error },
    };
    match run_probe(&planner.server_version()) {
        Ok(server_version) => DockerCapability::Available {
            client_version: client,
            server_version,
        },
        Err(daemon_error) => DockerCapability::CliOnly {
            client_version: client,
            daemon_error,
        },
    }
}

pub fn inspect_local_docker_image(
    program: impl AsRef<Path>,
    spec: &DockerExecutorSpec,
) -> std::result::Result<DockerImageIdentity, DockerImageError> {
    spec.validate()?;
    let reference = spec.image_reference();
    let planner = DockerCommandPlanner::new(program.as_ref());
    let image_id = run_probe(&planner.image_inspect(&reference)).map_err(|reason| {
        DockerImageError::Inspect {
            reference: reference.clone(),
            reason,
        }
    })?;
    if !is_sha256_digest(&image_id) {
        return Err(DockerImageError::InvalidImageId {
            reference,
            image_id,
        });
    }
    Ok(DockerImageIdentity {
        reference,
        image_id,
    })
}

pub fn validate_docker_mount_sources(spec: &DockerExecutorSpec) -> Result<()> {
    spec.validate()?;
    for mount in &spec.mounts {
        if !mount.source.try_exists().map_err(|error| {
            executor_validation(
                "executor.settings.mounts.source",
                &format!("cannot inspect {:?}: {error}", mount.source),
            )
        })? {
            return Err(executor_validation(
                "executor.settings.mounts.source",
                &format!("does not exist: {:?}", mount.source),
            ));
        }
    }
    Ok(())
}

fn run_probe(command: &DockerCommand) -> std::result::Result<String, String> {
    let output = Command::new(command.program())
        .args(command.args())
        .output()
        .map_err(|error| error.to_string())?;
    if !output.status.success() {
        let stderr = String::from_utf8(output.stderr)
            .map_err(|_| "Docker returned non-UTF-8 error output".to_owned())?
            .trim()
            .to_owned();
        return Err(if stderr.is_empty() {
            format!("Docker exited with {}", output.status)
        } else {
            stderr
        });
    }
    let stdout = String::from_utf8(output.stdout)
        .map_err(|_| "Docker returned non-UTF-8 output".to_owned())?
        .trim()
        .to_owned();
    if stdout.is_empty() {
        Err("Docker returned an empty version".into())
    } else {
        Ok(stdout)
    }
}

impl Default for ExecutorSpec {
    fn default() -> Self {
        Self::Process(ProcessExecutorSpec::default())
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::BTreeMap,
        ffi::OsString,
        fs,
        os::unix::{ffi::OsStringExt, fs::PermissionsExt},
    };

    use tempfile::TempDir;

    use super::*;
    use crate::{EnvironmentInheritance, EnvironmentPolicy, ShellPolicy};

    fn docker_spec(source: PathBuf) -> DockerExecutorSpec {
        DockerExecutorSpec {
            image: "registry.example/research/image:stable".into(),
            digest: Some(format!("sha256:{}", "a".repeat(64))),
            workdir: Some(PathBuf::from("/workspace")),
            mounts: vec![
                DockerMount {
                    source: source.join("input data"),
                    target: PathBuf::from("/workspace/input data"),
                    access: MountAccess::ReadOnly,
                },
                DockerMount {
                    source: source.join("output"),
                    target: PathBuf::from("/workspace/output"),
                    access: MountAccess::ReadWrite,
                },
            ],
            remove_container: true,
        }
    }

    fn command() -> CommandSpec {
        CommandSpec {
            program: "python".into(),
            args: vec!["train.py".into(), "--label=with spaces".into()],
            cwd: PathBuf::from("/host/project"),
            shell: ShellPolicy::Direct,
            environment: EnvironmentPolicy {
                set: BTreeMap::from([
                    ("LANG".into(), "C.UTF-8".into()),
                    ("SAFE_VALUE".into(), "visible".into()),
                ]),
                remove: vec!["HOME".into()],
                inherit: EnvironmentInheritance::All,
            },
        }
    }

    fn identity() -> DockerIdentity {
        DockerIdentity {
            project_id: ProjectId::new(),
            job_id: JobId::new(),
            attempt_id: AttemptId::new(),
            generation_id: Some(GenerationId::new()),
        }
    }

    fn args(command: &DockerCommand) -> Vec<String> {
        command
            .args()
            .iter()
            .map(|argument| argument.to_string_lossy().into_owned())
            .collect()
    }

    #[test]
    fn docker_contract_rejects_ambiguous_images_paths_and_targets() {
        let mut spec = docker_spec(PathBuf::from("/host"));
        spec.image = "image@sha256:embedded".into();
        assert!(spec.validate().is_err());

        spec.image = "image:tag".into();
        spec.digest = Some(format!("sha256:{}", "A".repeat(64)));
        assert!(spec.validate().is_err());

        spec.digest = None;
        spec.workdir = Some(PathBuf::from("relative"));
        assert!(spec.validate().is_err());

        spec.workdir = Some(PathBuf::from("/workspace"));
        spec.mounts[0].target = PathBuf::from("/workspace/../secret");
        assert!(spec.validate().is_err());

        spec.mounts[0].target = spec.mounts[1].target.clone();
        assert!(spec.validate().is_err());

        spec.mounts[0].target = PathBuf::from("/workspace,target");
        assert!(spec.validate().is_err());

        spec.mounts[0].target =
            PathBuf::from(OsString::from_vec(b"/workspace/non-utf8-\xff".to_vec()));
        assert!(spec.validate().is_err());
    }

    #[test]
    fn docker_create_plan_is_structured_deterministic_and_secret_free() -> Result<()> {
        let temporary = TempDir::new()
            .map_err(|error| executor_validation("test.temporary", &error.to_string()))?;
        fs::create_dir(temporary.path().join("input data"))
            .map_err(|error| executor_validation("test.input", &error.to_string()))?;
        fs::create_dir(temporary.path().join("output"))
            .map_err(|error| executor_validation("test.output", &error.to_string()))?;
        let spec = docker_spec(temporary.path().to_path_buf());
        let identity = identity();
        let generation_id = identity
            .generation_id
            .ok_or_else(|| executor_validation("test.identity", "generation ID is missing"))?;
        let planner = DockerCommandPlanner::default();
        let plan = planner.create(
            &spec,
            &command(),
            identity,
            &["GPU-two".into(), "GPU-one".into()],
        )?;

        assert_eq!(plan.container_name, format!("igor-{}", identity.attempt_id));
        assert!(plan.remove_container);
        assert_eq!(
            plan.image_reference,
            format!(
                "{}@{}",
                spec.image,
                spec.digest.as_deref().unwrap_or_default()
            )
        );
        assert_eq!(plan.command.program(), Path::new("docker"));
        assert_eq!(
            args(&plan.command),
            vec![
                "create".into(),
                "--name".into(),
                plan.container_name,
                "--label".into(),
                format!("igor.project_id={}", identity.project_id),
                "--label".into(),
                format!("igor.job_id={}", identity.job_id),
                "--label".into(),
                format!("igor.attempt_id={}", identity.attempt_id),
                "--label".into(),
                format!("igor.generation_id={generation_id}"),
                "--workdir".into(),
                "/workspace".into(),
                "--mount".into(),
                format!(
                    "type=bind,src={},dst=/workspace/input data,readonly",
                    temporary.path().join("input data").display()
                ),
                "--mount".into(),
                format!(
                    "type=bind,src={},dst=/workspace/output",
                    temporary.path().join("output").display()
                ),
                "--env".into(),
                "LANG=C.UTF-8".into(),
                "--env".into(),
                "SAFE_VALUE=visible".into(),
                "--gpus".into(),
                "device=GPU-two,GPU-one".into(),
                plan.image_reference,
                "python".into(),
                "train.py".into(),
                "--label=with spaces".into(),
            ]
        );
        Ok(())
    }

    #[test]
    fn docker_create_rejects_missing_mounts_sensitive_environment_and_bad_gpus() -> Result<()> {
        let planner = DockerCommandPlanner::default();
        let mut spec = docker_spec(PathBuf::from("/missing"));
        spec.mounts.truncate(1);
        assert!(planner.create(&spec, &command(), identity(), &[]).is_ok());
        assert!(validate_docker_mount_sources(&spec).is_err());

        let temporary = TempDir::new()
            .map_err(|error| executor_validation("test.temporary", &error.to_string()))?;
        let source = temporary.path().join("input data");
        fs::create_dir(&source)
            .map_err(|error| executor_validation("test.input", &error.to_string()))?;
        spec.mounts[0].source = source;
        validate_docker_mount_sources(&spec)?;
        let mut sensitive = command();
        sensitive
            .environment
            .set
            .insert("TELEGRAM_BOT_TOKEN".into(), "secret".into());
        assert!(planner.create(&spec, &sensitive, identity(), &[]).is_err());
        assert!(
            planner
                .create(&spec, &command(), identity(), &["GPU-one,two".into()])
                .is_err()
        );
        assert!(
            planner
                .create(&spec, &command(), identity(), &["GPU one".into()])
                .is_err()
        );

        let mut non_utf8_name = OsString::from("input-");
        non_utf8_name.push(OsString::from_vec(vec![0xff]));
        let non_utf8_source = temporary.path().join(non_utf8_name);
        fs::create_dir(&non_utf8_source)
            .map_err(|error| executor_validation("test.non_utf8", &error.to_string()))?;
        spec.mounts[0].source = non_utf8_source;
        validate_docker_mount_sources(&spec)?;
        assert!(planner.create(&spec, &command(), identity(), &[]).is_ok());
        Ok(())
    }

    #[test]
    fn docker_lifecycle_commands_never_use_a_shell() {
        let planner = DockerCommandPlanner::new("/usr/bin/docker");
        let commands = [
            (planner.start("container"), vec!["start", "container"]),
            (planner.wait("container"), vec!["wait", "container"]),
            (
                planner.logs("container"),
                vec!["logs", "--follow", "container"],
            ),
            (
                planner.logs_snapshot("container"),
                vec!["logs", "container"],
            ),
            (
                planner.stop("container", 12),
                vec!["stop", "--time", "12", "container"],
            ),
            (planner.kill("container"), vec!["kill", "container"]),
            (
                planner.inspect("container"),
                vec!["inspect", "--format", "{{json .State}}", "container"],
            ),
            (
                planner.remove("container"),
                vec!["rm", "--force", "container"],
            ),
        ];
        for (command, expected) in commands {
            assert_eq!(command.program(), Path::new("/usr/bin/docker"));
            assert_eq!(args(&command), expected);
        }
    }

    #[test]
    fn docker_recovery_commands_and_parsers_are_non_selecting_and_strict()
    -> std::result::Result<(), DockerOutputParseError> {
        let planner = DockerCommandPlanner::default();
        let command = planner.recovery_candidates(identity());
        assert_eq!(args(&command)[..3], ["ps", "--all", "--quiet"]);
        assert!(
            args(&command)
                .iter()
                .any(|arg| arg.starts_with("label=igor.project_id="))
        );
        assert_eq!(
            parse_docker_recovery_candidates(b"one\ntwo\n")?,
            vec!["one", "two"]
        );
        assert!(parse_docker_recovery_candidates(b"one two\n").is_err());
        let state = parse_docker_recovery_inspect(
            br#"{"Id":"cid","Name":"/igor-c","Image":"sha256:abc","Config":{"Labels":{"igor.attempt_id":"a"}},"State":{"Status":"exited","Running":false,"ExitCode":2,"OOMKilled":false,"Error":""}}"#,
        )?;
        assert_eq!(state.id, "cid");
        assert_eq!(state.labels["igor.attempt_id"], "a");
        assert!(args(&planner.logs("container")).contains(&"--follow".into()));
        assert!(!args(&planner.logs_snapshot("container")).contains(&"--follow".into()));
        Ok(())
    }

    #[test]
    fn docker_lifecycle_output_parsing_is_strict() -> std::result::Result<(), DockerOutputParseError>
    {
        assert_eq!(parse_docker_create_stdout(b"abc\n")?, "abc");
        assert!(parse_docker_create_stdout(b"abc def").is_err());
        assert!(parse_docker_create_stdout(b"\xff").is_err());
        assert_eq!(parse_docker_wait_stdout(b"  -2147483648\n")?, i32::MIN);
        assert_eq!(parse_docker_wait_stdout(b"2147483647")?, i32::MAX);
        assert!(parse_docker_wait_stdout(b"2147483648").is_err());
        assert!(parse_docker_wait_stdout(b"1 2").is_err());
        assert!(parse_docker_wait_stdout(b"\xff").is_err());

        let state = parse_docker_inspect_stdout(
            br#"{"Status":"exited","Running":false,"ExitCode":17,"OOMKilled":true,"Error":""}"#,
        )?;
        assert_eq!(state.status, "exited");
        assert!(!state.running);
        assert_eq!(state.exit_code, 17);
        assert!(state.oom_killed);
        assert_eq!(state.error, None);
        assert!(parse_docker_inspect_stdout(b"{} trailing").is_err());
        assert!(parse_docker_inspect_stdout(
            br#"{"Status":"exited","Running":false,"ExitCode":0,"OOMKilled":false,"Error":"x","Extra":1}"#,
        )
        .is_err());
        assert!(parse_docker_inspect_stdout(b"\xff").is_err());
        Ok(())
    }

    #[test]
    fn docker_capability_distinguishes_cli_daemon_and_absence() -> std::io::Result<()> {
        let temporary = TempDir::new()?;
        let available = temporary.path().join("docker-available");
        executable(
            &available,
            "#!/bin/sh\nif [ \"$1\" = '--version' ]; then echo 'Docker version test'; else echo '27.0'; fi\n",
        )?;
        assert_eq!(
            detect_docker(&available),
            DockerCapability::Available {
                client_version: "Docker version test".into(),
                server_version: "27.0".into(),
            }
        );

        let cli_only = temporary.path().join("docker-cli-only");
        executable(
            &cli_only,
            "#!/bin/sh\nif [ \"$1\" = '--version' ]; then echo 'Docker version test'; else echo 'daemon unavailable' >&2; exit 1; fi\n",
        )?;
        assert_eq!(
            detect_docker(&cli_only),
            DockerCapability::CliOnly {
                client_version: "Docker version test".into(),
                daemon_error: "daemon unavailable".into(),
            }
        );
        assert!(matches!(
            detect_docker(temporary.path().join("missing")),
            DockerCapability::Unavailable { .. }
        ));
        Ok(())
    }

    #[test]
    fn docker_image_inspection_uses_the_exact_local_digest() -> std::io::Result<()> {
        let temporary = TempDir::new()?;
        let docker = temporary.path().join("docker-image-inspect");
        let expected_digest = format!("sha256:{}", "a".repeat(64));
        let expected_reference = format!("example/image:tag@{expected_digest}");
        executable(
            &docker,
            &format!(
                "#!/bin/sh\nif [ \"$1\" = image ] && [ \"$5\" = '{expected_reference}' ]; then echo 'sha256:{}'; else echo 'image not found' >&2; exit 1; fi\n",
                "b".repeat(64)
            ),
        )?;
        let spec = DockerExecutorSpec {
            image: "example/image:tag".into(),
            digest: Some(expected_digest),
            workdir: None,
            mounts: Vec::new(),
            remove_container: true,
        };
        assert_eq!(
            inspect_local_docker_image(&docker, &spec).map_err(std::io::Error::other)?,
            DockerImageIdentity {
                reference: expected_reference,
                image_id: format!("sha256:{}", "b".repeat(64)),
            }
        );

        let mut missing = spec;
        missing.digest = Some(format!("sha256:{}", "c".repeat(64)));
        assert!(matches!(
            inspect_local_docker_image(&docker, &missing),
            Err(DockerImageError::Inspect { .. })
        ));
        Ok(())
    }

    fn executable(path: &Path, contents: &str) -> std::io::Result<()> {
        let mut file = fs::File::create(path)?;
        std::io::Write::write_all(&mut file, contents.as_bytes())?;
        file.sync_all()?;
        drop(file);
        let mut permissions = fs::metadata(path)?.permissions();
        permissions.set_mode(0o700);
        fs::set_permissions(path, permissions)
    }
}
