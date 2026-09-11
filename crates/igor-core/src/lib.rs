//! Igor's dependency-light domain contracts and shared application support.

mod artifact;
mod command;
mod config;
mod error;
mod event;
mod executor;
mod id;
mod job;
pub mod persistence;
mod project;
mod resource;
mod retry;
mod state;
pub mod telemetry;

pub use artifact::{ArtifactRole, RetentionDecision};
pub use command::{CommandSpec, EnvironmentInheritance, EnvironmentPolicy, ShellPolicy};
pub use config::{
    ConfigError, ConfigOverrides, EffectiveConfig, Environment, GLOBAL_CONFIG_VERSION,
    GlobalConfig, GlobalPathConfig, HostConfig, LoadedProjectConfig, PROJECT_CONFIG_RELATIVE_PATH,
    REDACTED, REPORT_PROMPT_RELATIVE_PATH, RuntimePaths, SecretString, TelegramConfig,
    discover_project_config, initialize_project, load_effective_config, load_global_config,
    load_project_config, select_global_config_path,
};
pub use error::{DomainError, ErrorCategory, ErrorCode, Result};
pub use event::{Event, EventKind, EventPayload};
pub use executor::{
    DockerExecutorSpec, DockerMount, ExecutorSpec, MountAccess, ProcessExecutorSpec,
    ProcessIsolation,
};
pub use id::{
    ActionId, AgentSessionId, AttemptId, DeliveryId, EventId, FamilyId, GenerationId, JobId,
    ProjectId, RecoveryId, ReportId, ResourceId,
};
pub use job::{
    AttemptSpec, ConfigurationIdentity, FamilyMembership, GenerationIdentity, JobSpec,
    ResultContract, Seed, SourceIdentity,
};
pub use persistence::{
    ActionRecord, ActionRepository, ArtifactRecord, ArtifactRepository, Claim, Database,
    DatabaseOptions, DeliveryRecord, DeliveryRepository, EventRepository, Family,
    FamilyGenerationRepository, Generation, IntegrityCheck, JobAttemptRepository, PersistenceError,
    ProjectRepository, Resource, ResourceLease, ResourceRepository, StoredAttempt, StoredEvent,
    StoredJob,
};
pub use project::{
    PROJECT_CONFIG_VERSION, Project, ProjectConfig, ProjectPaths, ReportConfig, ReportGenerator,
    ReportMode, ReportTrigger,
};
pub use resource::{
    GpuRequest, NamedResourceMode, NamedResourceRequest, ResourceMode, ResourceRequest,
};
pub use retry::{ActionRetryPolicy, AttemptRetryPolicy, DeliveryRetryPolicy, RetryPolicy};
pub use state::{
    ActionState, AttemptState, CleanupState, DeliveryState, JobState, RecoveryState, ReportState,
    TransitionState,
};
