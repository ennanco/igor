//! Igor's dependency-light domain contracts and shared application support.

mod artifact;
mod command;
mod config;
mod error;
mod event;
mod executor;
mod family;
mod generated_report;
mod id;
mod inventory;
mod job;
mod metric;
pub mod persistence;
mod project;
mod report;
mod report_publication;
mod resource;
mod retry;
mod state;
mod submission;
pub mod telemetry;

pub use artifact::{ArtifactPublicationState, ArtifactRole, RetentionDecision};
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
    update_global_telegram_config,
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
pub use family::{
    FAMILY_FILE_VERSION, FamilyFile, FamilyMember, PreparedFamily, build_family_generation,
    load_family_file,
};
pub use generated_report::{GeneratedReportError, StagedGeneratedReport, stage_generated_report};
pub use id::{
    ActionId, AgentSessionId, AttemptId, DeliveryId, EventId, FamilyId, GenerationId, JobId,
    ProjectId, RecoveryId, ReportId, ResourceId,
};
pub use inventory::{HostGpu, HostInventory, InventoryError, discover_host_inventory};
pub use job::{
    AttemptSpec, ConfigurationIdentity, ContentIdentity, ContentRole, FailedArtifactRetention,
    FamilyMembership, GenerationIdentity, GitIdentity, JobSpec, ResultContract, Seed,
    SourceIdentity, SupersededArtifactRetention,
};
pub use metric::{MetricDocumentError, parse_metric_document_json, validate_metric_document};
pub use persistence::ReportRetryReceipt;
pub use persistence::StoredReportRun;
pub use persistence::{
    ActionRecord, ActionRepository, ArtifactRecord, ArtifactRepository, ArtifactStorage,
    ArtifactStorageGroup, AttemptLogPaths, AttemptMetrics, Claim, ClaimedAction, ClaimedDelivery,
    CleanupIntentStorage, ComparableGeneration, ContainerCreate, ContainerFinish, ContainerRecord,
    Database, DatabaseOptions, DeliveryRecord, DeliveryRepository, EventRepository, ExecutionClaim,
    ExecutionOutcome, FailedLogRetirementStorage, FailedLogTails, FailedTailStorage, Family,
    FamilyDetail, FamilyGenerationDetail, FamilyGenerationRepository, GcArtifactBatch, GcEffect,
    GcEntry, GcExecution, GcExecutionCause, GcExecutionEntry, GcExecutionStatus, GcLogStream,
    GcOperation, GcPage, GcPlan, GcSelection, GcStatus, GcSubject, GcTarget, GcWorklist,
    Generation, GenerationStatus, IntegrityCheck, JobAttemptRepository, JobDetail, JobLogs,
    LogArchive, LogArchiveStorage, LogTailChunk, OwnedOutputStorage, PendingLogPreparationsStorage,
    PersistenceError, ProcessRecord, ProcessStart, ProjectRepository, REPORT_ACTION_VERSION,
    REPORT_CONTEXT_VERSION, RecoveredExecution, RepairWorktreeEvidence, RepairWorktreeRepository,
    RepairWorktreeStorage, ReportActionSpec, ReportAttempt, ReportCandidate, ReportContext,
    ReportExtractor, ReportGeneration, ReportMember, ReportMetricSeries, ReportMetricValue,
    ReportRepair, ReportToolVersions, Resource, ResourceLease, ResourceRepository, ResourceStatus,
    RetiredLogTail, StorageStateCount, StorageStatus, StoredAttempt, StoredEvent, StoredJob,
    TombstoneStorage, UnitRecord, UnitReservation, copy_log,
};
pub use persistence::{
    LockedReportPublication, PreparedReport, ReportExecutionError, ReportPreparation,
    ReportPublishOutcome,
};
pub use persistence::{ReportOrigin, ReportRequestReceipt};
pub use project::{
    ArtifactRetentionConfig, FailedRetention, PROJECT_CONFIG_VERSION, Project, ProjectConfig,
    ProjectPaths, ProvenanceConfig, ReportConfig, ReportGenerator, ReportMode, ReportTrigger,
    ResultsConfig, RetentionConfig, SupersededRetention,
};
pub use report::{ReportRenderError, render_report_markdown};
pub use report_publication::{PublishedReport, ReportPublicationError, StagedReport, stage_report};
pub use resource::{
    GpuRequest, NamedResourceMode, NamedResourceRequest, ResourceMode, ResourceRequest,
};
pub use retry::{ActionRetryPolicy, AttemptRetryPolicy, DeliveryRetryPolicy, RetryPolicy};
pub use state::{
    ActionState, ArtifactLifecycleState, AttemptState, CleanupState, DeliveryState, JobState,
    RecoveryState, ReportState, TransitionState,
};
pub use submission::{
    JOB_FILE_VERSION, JobExecution, JobFile, Submission, SubmissionError, SubmissionInput,
    build_submission, load_job_file,
};
