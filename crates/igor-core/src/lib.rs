//! Igor's dependency-light domain contracts and shared application support.

mod artifact;
mod command;
mod config;
mod error;
mod event;
mod executor;
mod id;
mod inventory;
mod job;
pub mod persistence;
mod project;
mod resource;
mod retry;
mod state;
mod submission;
pub mod telemetry;

pub use artifact::{ArtifactRole, RetentionDecision};
pub use command::{
    CommandSpec, EnvironmentInheritance, EnvironmentPolicy, ShellPolicy,
    environment_variable_is_sensitive,
};
pub use config::{
    ConfigError, ConfigOverrides, DEFAULT_MAX_CONCURRENT_JOBS, EffectiveConfig, Environment,
    GLOBAL_CONFIG_VERSION, GlobalConfig, GlobalPathConfig, HostConfig, HostConfigUpdate,
    LoadedProjectConfig, MAX_CONCURRENT_JOBS, PROJECT_CONFIG_RELATIVE_PATH, REDACTED,
    REPORT_PROMPT_RELATIVE_PATH, RuntimePaths, SecretString, TelegramConfig,
    discover_project_config, initialize_project, load_effective_config, load_global_config,
    load_project_config, select_global_config_path, update_global_host_config,
};
pub use error::{DomainError, ErrorCategory, ErrorCode, Result};
pub use event::{Event, EventKind, EventPayload};
pub use executor::{
    DockerCapability, DockerCommand, DockerCommandPlanner, DockerContainerInspection,
    DockerContainerState, DockerCreatePlan, DockerExecutorSpec, DockerIdentity, DockerImageError,
    DockerImageIdentity, DockerMount, DockerOutputParseError, DockerRecoveryIdentity, ExecutorSpec,
    MountAccess, ProcessExecutorSpec, ProcessIsolation, detect_docker, inspect_local_docker_image,
    parse_docker_create_stdout, parse_docker_inspect_stdout, parse_docker_recovery_candidates,
    parse_docker_recovery_inspect, parse_docker_wait_stdout, validate_docker_mount_sources,
};
pub use id::{
    ActionId, AgentSessionId, AttemptId, DeliveryId, EventId, FamilyId, GenerationId, JobId,
    ProjectId, RecoveryId, ReportId, ResourceId,
};
pub use inventory::{HostGpu, HostInventory, InventoryError, discover_host_inventory};
pub use job::{
    AttemptSpec, ConfigurationIdentity, ContentIdentity, ContentRole, FamilyMembership,
    GenerationIdentity, GitIdentity, JobSpec, ResultContract, Seed, SourceIdentity,
};
pub use persistence::{
    ActionRecord, ActionRepository, ArtifactRecord, ArtifactRepository, Claim, ContainerCreate,
    ContainerFinish, ContainerRecord, Database, DatabaseOptions, DeliveryRecord,
    DeliveryRepository, EventRepository, ExecutionClaim, ExecutionOutcome, Family,
    FamilyGenerationRepository, Generation, IntegrityCheck, JobAttemptRepository, JobDetail,
    JobLogs, PersistenceError, ProcessRecord, ProcessStart, ProjectRepository, RecoveredExecution,
    Resource, ResourceLease, ResourceRepository, ResourceStatus, StoredAttempt, StoredEvent,
    StoredJob, UnitRecord, UnitReservation,
};
pub use project::{
    PROJECT_CONFIG_VERSION, Project, ProjectConfig, ProjectPaths, ProvenanceConfig, ReportConfig,
    ReportGenerator, ReportMode, ReportTrigger,
};
pub use resource::{
    GpuRequest, NamedResourceMode, NamedResourceRequest, ResourceMode, ResourceRequest,
};
pub use retry::{ActionRetryPolicy, AttemptRetryPolicy, DeliveryRetryPolicy, RetryPolicy};
pub use state::{
    ActionState, AttemptState, CleanupState, DeliveryState, JobState, RecoveryState, ReportState,
    TransitionState,
};
pub use submission::{
    JOB_FILE_VERSION, JobExecution, JobFile, Submission, SubmissionError, SubmissionInput,
    build_submission, load_job_file,
};
