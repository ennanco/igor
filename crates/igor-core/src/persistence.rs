use std::{
    fs::OpenOptions,
    path::{Path, PathBuf},
    str::FromStr,
    time::Duration,
};

use rusqlite::backup::Backup;
use serde_json::Value;
use sqlx::{
    Row, Sqlite, SqlitePool, Transaction,
    migrate::{MigrateError, Migrator},
    sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions},
};
use thiserror::Error;
use uuid::Uuid;

use crate::{
    ActionId, ActionState, ArtifactRole, AttemptId, AttemptSpec, AttemptState, DeliveryId,
    DeliveryState, DockerContainerState, DockerExecutorSpec, DockerImageIdentity, DomainError,
    Event, EventId, EventKind, EventPayload, ExecutorSpec, FamilyId, GenerationId,
    GenerationIdentity, GpuRequest, JobId, JobSpec, JobState, Project, ProjectId, ResourceId,
    ResourceMode, ResourceRequest, TransitionState,
};

static MIGRATOR: Migrator = sqlx::migrate!();
const DEFAULT_BUSY_TIMEOUT: Duration = Duration::from_secs(2);
const MAX_LEASE_SECONDS: u64 = 24 * 60 * 60;
const PRIORITY_AGING_INTERVAL_SECONDS: i64 = 60 * 60;

pub type PersistenceResult<T> = std::result::Result<T, PersistenceError>;

#[derive(Debug, Error)]
pub enum PersistenceError {
    #[error("database operation {operation} failed: {source}")]
    Database {
        operation: &'static str,
        #[source]
        source: sqlx::Error,
    },
    #[error("database schema upgrade failed: {0}")]
    Migration(#[from] MigrateError),
    #[error("database backup failed: {0}")]
    Backup(#[from] rusqlite::Error),
    #[error("database filesystem operation failed: {0}")]
    Io(#[from] std::io::Error),
    #[error("invalid persisted JSON for {contract}: {source}")]
    Serialization {
        contract: &'static str,
        #[source]
        source: serde_json::Error,
    },
    #[error("invalid persisted {entity} value {value:?}")]
    InvalidValue { entity: &'static str, value: String },
    #[error("invalid identifier in {entity}: {source}")]
    InvalidId {
        entity: &'static str,
        #[source]
        source: uuid::Error,
    },
    #[error("{entity} was not found")]
    NotFound { entity: &'static str },
    #[error("{entity} changed concurrently")]
    Conflict { entity: &'static str },
    #[error("invalid lease: {0}")]
    InvalidLease(&'static str),
    #[error("backup destination must differ from the source database")]
    BackupAliasesSource,
    #[error("backup destination already exists")]
    BackupDestinationExists,
    #[error("online backup is unavailable for an in-memory database")]
    InMemoryBackup,
    #[error(transparent)]
    Domain(#[from] DomainError),
    #[error("blocking database task failed: {0}")]
    BlockingTask(#[from] tokio::task::JoinError),
}

fn db(operation: &'static str, source: sqlx::Error) -> PersistenceError {
    PersistenceError::Database { operation, source }
}

fn json<T: serde::Serialize>(value: &T, contract: &'static str) -> PersistenceResult<String> {
    serde_json::to_string(value)
        .map_err(|source| PersistenceError::Serialization { contract, source })
}

fn from_json<T: serde::de::DeserializeOwned>(
    value: &str,
    contract: &'static str,
) -> PersistenceResult<T> {
    serde_json::from_str(value)
        .map_err(|source| PersistenceError::Serialization { contract, source })
}

fn parse_id<T: FromStr<Err = uuid::Error>>(
    value: &str,
    entity: &'static str,
) -> PersistenceResult<T> {
    value
        .parse()
        .map_err(|source| PersistenceError::InvalidId { entity, source })
}

fn validate_lease(owner: &str, duration: Duration) -> PersistenceResult<i64> {
    let seconds = duration.as_secs();
    if owner.trim().is_empty() {
        return Err(PersistenceError::InvalidLease("owner must not be empty"));
    }
    if seconds == 0 || seconds > MAX_LEASE_SECONDS {
        return Err(PersistenceError::InvalidLease(
            "duration must be between 1 second and 24 hours",
        ));
    }
    i64::try_from(seconds).map_err(|_| PersistenceError::InvalidLease("duration is too large"))
}

fn require_event_kind(
    event: &Event,
    expected: EventKind,
    entity: &'static str,
) -> PersistenceResult<()> {
    if event.kind == expected {
        Ok(())
    } else {
        Err(PersistenceError::InvalidValue {
            entity,
            value: event.kind.as_str().into(),
        })
    }
}

#[derive(Clone, Debug)]
pub struct DatabaseOptions {
    pub max_connections: u32,
    pub busy_timeout: Duration,
    pub writable: bool,
}

impl Default for DatabaseOptions {
    fn default() -> Self {
        Self {
            max_connections: 8,
            busy_timeout: DEFAULT_BUSY_TIMEOUT,
            writable: true,
        }
    }
}

#[derive(Clone, Debug)]
pub struct Database {
    pool: SqlitePool,
    path: Option<PathBuf>,
}

impl Database {
    pub async fn open(path: impl AsRef<Path>) -> PersistenceResult<Self> {
        Self::open_with_options(path, DatabaseOptions::default()).await
    }

    pub async fn open_with_options(
        path: impl AsRef<Path>,
        options: DatabaseOptions,
    ) -> PersistenceResult<Self> {
        let path = path.as_ref();
        if options.writable
            && let Some(parent) = path.parent()
            && !parent.as_os_str().is_empty()
        {
            std::fs::create_dir_all(parent)?;
        }
        let canonical_parent = path
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
            .unwrap_or_else(|| Path::new("."))
            .canonicalize()?;
        let file_name = path.file_name().ok_or_else(|| {
            PersistenceError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "database path has no filename",
            ))
        })?;
        let canonical_path = canonical_parent.join(file_name);
        let connect = configured_options(&canonical_path, &options);
        let pool = SqlitePoolOptions::new()
            .max_connections(options.max_connections.max(1))
            .connect_with(connect)
            .await
            .map_err(|source| db("open", source))?;
        let database = Self {
            pool,
            path: Some(canonical_path),
        };
        if options.writable {
            database.migrate().await?;
        }
        Ok(database)
    }

    pub async fn in_memory() -> PersistenceResult<Self> {
        let options = DatabaseOptions {
            max_connections: 1,
            ..DatabaseOptions::default()
        };
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect_with(configured_options(Path::new(":memory:"), &options))
            .await
            .map_err(|source| db("open in-memory database", source))?;
        let database = Self { pool, path: None };
        database.migrate().await?;
        Ok(database)
    }

    pub fn pool(&self) -> &SqlitePool {
        &self.pool
    }

    pub async fn migrate(&self) -> PersistenceResult<()> {
        MIGRATOR.run(&self.pool).await?;
        Ok(())
    }

    pub async fn schema_version(&self) -> PersistenceResult<i64> {
        sqlx::query_scalar(
            "SELECT COALESCE(MAX(version), 0) FROM _sqlx_migrations WHERE success = 1",
        )
        .fetch_one(&self.pool)
        .await
        .map_err(|source| db("read schema version", source))
    }

    pub fn projects(&self) -> ProjectRepository<'_> {
        ProjectRepository { database: self }
    }

    pub fn families(&self) -> FamilyGenerationRepository<'_> {
        FamilyGenerationRepository { database: self }
    }

    pub fn jobs(&self) -> JobAttemptRepository<'_> {
        JobAttemptRepository { database: self }
    }

    pub fn events(&self) -> EventRepository<'_> {
        EventRepository { database: self }
    }

    pub fn actions(&self) -> ActionRepository<'_> {
        ActionRepository { database: self }
    }

    pub fn deliveries(&self) -> DeliveryRepository<'_> {
        DeliveryRepository { database: self }
    }

    pub fn resources(&self) -> ResourceRepository<'_> {
        ResourceRepository { database: self }
    }

    pub fn artifacts(&self) -> ArtifactRepository<'_> {
        ArtifactRepository { database: self }
    }

    pub async fn integrity_check(&self, check: IntegrityCheck) -> PersistenceResult<()> {
        let pragma = match check {
            IntegrityCheck::Quick => "PRAGMA quick_check",
            IntegrityCheck::Full => "PRAGMA integrity_check",
        };
        let rows: Vec<String> = sqlx::query_scalar(pragma)
            .fetch_all(&self.pool)
            .await
            .map_err(|source| db("run integrity check", source))?;
        if rows.len() == 1 && rows[0] == "ok" {
            Ok(())
        } else {
            Err(PersistenceError::InvalidValue {
                entity: "integrity check result",
                value: rows.join("; "),
            })
        }
    }

    pub async fn backup(&self, destination: impl AsRef<Path>) -> PersistenceResult<()> {
        let source = self.path.clone().ok_or(PersistenceError::InMemoryBackup)?;
        let destination = destination.as_ref();
        if destination.exists() {
            return if destination.canonicalize()? == source {
                Err(PersistenceError::BackupAliasesSource)
            } else {
                Err(PersistenceError::BackupDestinationExists)
            };
        }
        if let Some(parent) = destination.parent()
            && !parent.as_os_str().is_empty()
        {
            std::fs::create_dir_all(parent)?;
        }
        let parent = destination
            .parent()
            .filter(|value| !value.as_os_str().is_empty())
            .unwrap_or_else(|| Path::new("."))
            .canonicalize()?;
        let destination = parent.join(destination.file_name().ok_or_else(|| {
            PersistenceError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "backup path has no filename",
            ))
        })?);
        if destination == source {
            return Err(PersistenceError::BackupAliasesSource);
        }
        tokio::task::spawn_blocking(move || -> PersistenceResult<()> {
            let temporary = destination.with_file_name(format!(
                ".{}.{}.tmp",
                destination
                    .file_name()
                    .and_then(|value| value.to_str())
                    .unwrap_or("igor-backup"),
                Uuid::new_v4()
            ));
            OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&temporary)?;
            let cleanup = TemporaryBackup::new(temporary.clone());
            let source = rusqlite::Connection::open_with_flags(
                source,
                rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
            )?;
            {
                let mut temporary_database = rusqlite::Connection::open_with_flags(
                    &temporary,
                    rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE,
                )?;
                let backup = Backup::new(&source, &mut temporary_database)?;
                backup.run_to_completion(64, Duration::from_millis(2), None)?;
                drop(backup);
                let integrity: String =
                    temporary_database.query_row("PRAGMA integrity_check", [], |row| row.get(0))?;
                if integrity != "ok" {
                    return Err(PersistenceError::InvalidValue {
                        entity: "backup integrity check result",
                        value: integrity,
                    });
                }
            }
            std::fs::hard_link(&temporary, &destination).map_err(|source| {
                if source.kind() == std::io::ErrorKind::AlreadyExists {
                    PersistenceError::BackupDestinationExists
                } else {
                    PersistenceError::Io(source)
                }
            })?;
            cleanup.remove()?;
            Ok(())
        })
        .await??;
        Ok(())
    }
}

struct TemporaryBackup {
    path: PathBuf,
}

impl TemporaryBackup {
    fn new(path: PathBuf) -> Self {
        Self { path }
    }

    fn remove(&self) -> std::io::Result<()> {
        std::fs::remove_file(&self.path)
    }
}

impl Drop for TemporaryBackup {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
    }
}

fn configured_options(path: &Path, options: &DatabaseOptions) -> SqliteConnectOptions {
    let connect = SqliteConnectOptions::new()
        .filename(path)
        .create_if_missing(options.writable)
        .read_only(!options.writable)
        .foreign_keys(true)
        .busy_timeout(options.busy_timeout);
    if options.writable && path != Path::new(":memory:") {
        connect.journal_mode(SqliteJournalMode::Wal)
    } else {
        connect
    }
}

#[derive(Clone, Copy, Debug)]
pub enum IntegrityCheck {
    Quick,
    Full,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Claim<T> {
    pub record_id: T,
    pub lease_id: Uuid,
    pub owner: String,
    pub expires_at: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Family {
    pub id: FamilyId,
    pub project_id: ProjectId,
    pub name: String,
}

#[derive(Clone, Debug, PartialEq)]
pub struct Generation {
    pub family_id: FamilyId,
    pub identity: GenerationIdentity,
    pub spec: Value,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct StoredJob {
    pub spec: JobSpec,
    pub state: JobState,
    pub priority: i64,
    pub submission_order: i64,
    pub submitted_at: String,
    pub updated_at: String,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct StoredAttempt {
    pub spec: AttemptSpec,
    pub state: AttemptState,
    pub created_at: String,
    pub started_at: Option<String>,
    pub finished_at: Option<String>,
    pub updated_at: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExecutionClaim {
    pub job: StoredJob,
    pub attempt: StoredAttempt,
    pub lease_id: Uuid,
    pub owner: String,
    pub expires_at: String,
    pub resource_leases: Vec<ResourceLease>,
    pub assigned_gpus: Vec<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RecoveredExecution {
    pub claim: ExecutionClaim,
    pub process: Option<ProcessRecord>,
    pub container: Option<ContainerRecord>,
    pub timeout_remaining: Option<Duration>,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct ProcessRecord {
    pub attempt_id: AttemptId,
    pub job_id: JobId,
    pub project_id: ProjectId,
    pub pid: i64,
    pub process_group_id: i64,
    pub process_start_ticks: i64,
    pub stdout_path: PathBuf,
    pub stderr_path: PathBuf,
    pub heartbeat_at: String,
    pub exit_code: Option<i32>,
    pub term_signal: Option<i32>,
    pub error: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProcessStart {
    pub pid: i64,
    pub process_group_id: i64,
    pub process_start_ticks: i64,
    pub stdout_path: PathBuf,
    pub stderr_path: PathBuf,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ContainerCreate {
    pub container_id: String,
    pub container_name: String,
    pub image: DockerImageIdentity,
    pub stdout_path: PathBuf,
    pub stderr_path: PathBuf,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ContainerFinish {
    pub state: DockerContainerState,
    pub docker_status: Option<String>,
    pub exit_code: Option<i32>,
    pub oom_killed: bool,
    pub error: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct ContainerRecord {
    pub attempt_id: AttemptId,
    pub job_id: JobId,
    pub project_id: ProjectId,
    pub container_id: String,
    pub container_name: String,
    pub image_reference: String,
    pub image_id: String,
    pub state: DockerContainerState,
    pub docker_status: Option<String>,
    pub stdout_path: PathBuf,
    pub stderr_path: PathBuf,
    pub exit_code: Option<i32>,
    pub oom_killed: Option<bool>,
    pub error: Option<String>,
    pub started_at: Option<String>,
    pub finished_at: Option<String>,
    pub removed_at: Option<String>,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExecutionOutcome {
    pub state: AttemptState,
    pub exit_code: Option<i32>,
    pub term_signal: Option<i32>,
    pub error: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct JobLogs {
    pub job_id: JobId,
    pub attempt_id: AttemptId,
    pub state: JobState,
    pub stdout_path: Option<PathBuf>,
    pub stderr_path: Option<PathBuf>,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct StoredEvent {
    pub sequence: i64,
    pub project_id: ProjectId,
    pub job_id: Option<JobId>,
    pub attempt_id: Option<AttemptId>,
    pub event: Event,
    pub occurred_at: String,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct JobDetail {
    pub job: StoredJob,
    pub attempts: Vec<StoredAttempt>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ActionRecord {
    pub id: ActionId,
    pub project_id: ProjectId,
    pub kind: String,
    pub state: ActionState,
    pub spec: Value,
    pub idempotency_key: String,
}

#[derive(Clone, Debug, PartialEq)]
pub struct DeliveryRecord {
    pub id: DeliveryId,
    pub project_id: ProjectId,
    pub channel: String,
    pub state: DeliveryState,
    pub payload: Value,
    pub idempotency_key: String,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct Resource {
    pub id: ResourceId,
    pub name: String,
    pub kind: String,
    pub capacity: i64,
    pub metadata: Value,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct ResourceLease {
    pub id: Uuid,
    pub resource_id: ResourceId,
    pub job_id: JobId,
    pub owner: String,
    pub quantity: i64,
    pub heartbeat_at: String,
    pub expires_at: String,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct ResourceStatus {
    pub resource: Resource,
    pub leases: Vec<ResourceLease>,
}

#[derive(Clone)]
struct SchedulingResource {
    resource: Resource,
    used: i64,
    available: bool,
}

struct PlannedLease {
    resource_id: ResourceId,
    quantity: i64,
    assigned_gpu: Option<String>,
}

struct LeasedResource {
    lease: ResourceLease,
    resource: Resource,
}

fn execution_resource_conflict() -> PersistenceError {
    PersistenceError::Conflict {
        entity: "execution resource leases",
    }
}

fn assigned_gpus(plan: &[PlannedLease]) -> Vec<String> {
    plan.iter()
        .filter_map(|lease| lease.assigned_gpu.clone())
        .collect()
}

async fn lock_execution_scheduler(
    transaction: &mut Transaction<'_, Sqlite>,
    operation: &'static str,
) -> PersistenceResult<bool> {
    let locked = sqlx::query("UPDATE resources SET capacity = capacity WHERE name = 'host'")
        .execute(&mut **transaction)
        .await
        .map_err(|source| db(operation, source))?;
    Ok(locked.rows_affected() == 1)
}

async fn validate_execution_claim_resources(
    transaction: &mut Transaction<'_, Sqlite>,
    claim: &ExecutionClaim,
) -> PersistenceResult<()> {
    let resource_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM resource_leases
         WHERE job_id = ? AND owner = ?",
    )
    .bind(claim.job.spec.id.to_string())
    .bind(&claim.owner)
    .fetch_one(&mut **transaction)
    .await
    .map_err(|source| db("validate execution resources", source))?;
    if usize::try_from(resource_count).ok() != Some(claim.resource_leases.len()) {
        return Err(execution_resource_conflict());
    }
    Ok(())
}

async fn scheduling_resources(
    transaction: &mut Transaction<'_, Sqlite>,
) -> PersistenceResult<Vec<SchedulingResource>> {
    let rows = sqlx::query(
        "SELECT resources.id, resources.name, resources.kind, resources.capacity,
                resources.metadata_json, COALESCE(SUM(resource_leases.quantity), 0) AS used
         FROM resources
         LEFT JOIN resource_leases ON resource_leases.resource_id = resources.id
         GROUP BY resources.id
         ORDER BY resources.kind, resources.name",
    )
    .fetch_all(&mut **transaction)
    .await
    .map_err(|source| db("read scheduling resources", source))?;
    rows.into_iter()
        .map(|row| {
            let metadata: Value = from_json(row.get("metadata_json"), "resource metadata")?;
            Ok(SchedulingResource {
                available: metadata["available"].as_bool().unwrap_or(true),
                resource: Resource {
                    id: parse_id(row.get("id"), "scheduling resource")?,
                    name: row.get("name"),
                    kind: row.get("kind"),
                    capacity: row.get("capacity"),
                    metadata,
                },
                used: row.get("used"),
            })
        })
        .collect()
}

fn plan_resource_leases(
    request: &ResourceRequest,
    inventory: &[SchedulingResource],
) -> PersistenceResult<Option<Vec<PlannedLease>>> {
    request.validate()?;
    let mut inventory = inventory.to_vec();
    let host_capacity = inventory
        .iter()
        .find(|resource| resource.resource.name == "host" && resource.available)
        .map(|resource| resource.resource.capacity);
    let Some(host_capacity) = host_capacity else {
        return Ok(None);
    };
    let host_quantity = match request.mode {
        ResourceMode::ExclusiveHost => host_capacity,
        ResourceMode::Shared => 1,
    };
    let mut plan = Vec::new();
    if !plan_named_resource(&mut inventory, &mut plan, "host", host_quantity, None) {
        return Ok(None);
    }
    if let Some(cpu_threads) = request.cpu_threads
        && !plan_named_resource(
            &mut inventory,
            &mut plan,
            "cpu",
            i64::from(cpu_threads),
            None,
        )
    {
        return Ok(None);
    }
    if let Some(memory_bytes) = request.memory_bytes {
        let quantity = i64::try_from(memory_bytes).map_err(|_| PersistenceError::InvalidValue {
            entity: "execution memory request",
            value: memory_bytes.to_string(),
        })?;
        if !plan_named_resource(&mut inventory, &mut plan, "memory", quantity, None) {
            return Ok(None);
        }
    }
    match &request.gpu {
        GpuRequest::None => {}
        GpuRequest::Specific(device) => {
            if !plan_named_resource(
                &mut inventory,
                &mut plan,
                &format!("gpu:{device}"),
                1,
                Some(device.clone()),
            ) {
                return Ok(None);
            }
        }
        GpuRequest::Any => {
            for _ in 0..request.gpu_count {
                let Some(index) = inventory.iter().position(|resource| {
                    resource.resource.kind == "gpu"
                        && resource.available
                        && resource.used < resource.resource.capacity
                }) else {
                    return Ok(None);
                };
                let device = inventory[index].resource.metadata["device"]
                    .as_str()
                    .ok_or_else(|| PersistenceError::InvalidValue {
                        entity: "GPU resource metadata",
                        value: inventory[index].resource.metadata.to_string(),
                    })?
                    .to_owned();
                inventory[index].used += 1;
                plan.push(PlannedLease {
                    resource_id: inventory[index].resource.id,
                    quantity: 1,
                    assigned_gpu: Some(device),
                });
            }
        }
    }
    for resource in &request.named {
        if !plan_named_resource(
            &mut inventory,
            &mut plan,
            &format!("named:{}", resource.name),
            1,
            None,
        ) {
            return Ok(None);
        }
    }
    Ok(Some(plan))
}

fn plan_named_resource(
    inventory: &mut [SchedulingResource],
    plan: &mut Vec<PlannedLease>,
    name: &str,
    quantity: i64,
    assigned_gpu: Option<String>,
) -> bool {
    let Some(resource) = inventory.iter_mut().find(|resource| {
        resource.resource.name == name
            && resource.available
            && resource.resource.capacity - resource.used >= quantity
    }) else {
        return false;
    };
    resource.used += quantity;
    plan.push(PlannedLease {
        resource_id: resource.resource.id,
        quantity,
        assigned_gpu,
    });
    true
}

async fn reserve_execution_resources(
    transaction: &mut Transaction<'_, Sqlite>,
    project_id: ProjectId,
    job_id: JobId,
    owner: &str,
    seconds: i64,
    plan: &[PlannedLease],
) -> PersistenceResult<Vec<ResourceLease>> {
    let mut leases = Vec::with_capacity(plan.len());
    for planned in plan {
        let lease_id = Uuid::new_v4();
        let row = sqlx::query(
            "INSERT INTO resource_leases
                 (id, resource_id, job_id, owner, quantity, expires_at)
             VALUES (?, ?, ?, ?, ?,
                 strftime('%Y-%m-%dT%H:%M:%fZ', 'now', '+' || ? || ' seconds'))
             RETURNING heartbeat_at, expires_at",
        )
        .bind(lease_id.to_string())
        .bind(planned.resource_id.to_string())
        .bind(job_id.to_string())
        .bind(owner)
        .bind(planned.quantity)
        .bind(seconds)
        .fetch_one(&mut **transaction)
        .await
        .map_err(|source| db("reserve execution resource", source))?;
        let event = claim_event(
            EventKind::ResourceReserved,
            lease_id,
            owner,
            "reserved",
            "resource_lease",
            &lease_id.to_string(),
        )?;
        insert_event(transaction, project_id, Some(job_id), None, &event).await?;
        leases.push(ResourceLease {
            id: lease_id,
            resource_id: planned.resource_id,
            job_id,
            owner: owner.into(),
            quantity: planned.quantity,
            heartbeat_at: row.get("heartbeat_at"),
            expires_at: row.get("expires_at"),
        });
    }
    Ok(leases)
}

async fn execution_resource_leases(
    transaction: &mut Transaction<'_, Sqlite>,
    job_id: JobId,
) -> PersistenceResult<Vec<LeasedResource>> {
    let rows = sqlx::query(
        "SELECT resource_leases.id AS lease_id, resource_leases.resource_id,
                resource_leases.job_id, resource_leases.owner, resource_leases.quantity,
                resource_leases.heartbeat_at, resource_leases.expires_at,
                resources.name, resources.kind, resources.capacity, resources.metadata_json
         FROM resource_leases
         JOIN resources ON resources.id = resource_leases.resource_id
         WHERE resource_leases.job_id = ?
         ORDER BY resources.kind, resources.name",
    )
    .bind(job_id.to_string())
    .fetch_all(&mut **transaction)
    .await
    .map_err(|source| db("read execution resource leases", source))?;
    rows.into_iter()
        .map(|row| {
            let resource_id = parse_id(row.get("resource_id"), "execution lease resource")?;
            Ok(LeasedResource {
                lease: ResourceLease {
                    id: parse_id(row.get("lease_id"), "execution resource lease")?,
                    resource_id,
                    job_id: parse_id(row.get("job_id"), "execution resource lease job")?,
                    owner: row.get("owner"),
                    quantity: row.get("quantity"),
                    heartbeat_at: row.get("heartbeat_at"),
                    expires_at: row.get("expires_at"),
                },
                resource: Resource {
                    id: resource_id,
                    name: row.get("name"),
                    kind: row.get("kind"),
                    capacity: row.get("capacity"),
                    metadata: from_json(row.get("metadata_json"), "resource metadata")?,
                },
            })
        })
        .collect()
}

fn validate_execution_resource_leases(
    request: &ResourceRequest,
    leased: &[LeasedResource],
) -> PersistenceResult<Vec<String>> {
    let host = leased
        .iter()
        .find(|leased| leased.resource.name == "host")
        .ok_or_else(execution_resource_conflict)?;
    let expected_host = match request.mode {
        ResourceMode::ExclusiveHost => host.resource.capacity,
        ResourceMode::Shared => 1,
    };
    if host.lease.quantity != expected_host {
        return Err(execution_resource_conflict());
    }
    let exact_quantity = |name: &str, expected: Option<i64>| {
        let actual = leased
            .iter()
            .find(|leased| leased.resource.name == name)
            .map(|leased| leased.lease.quantity);
        (actual == expected).then_some(())
    };
    if exact_quantity("cpu", request.cpu_threads.map(i64::from)).is_none()
        || exact_quantity(
            "memory",
            request
                .memory_bytes
                .and_then(|value| i64::try_from(value).ok()),
        )
        .is_none()
    {
        return Err(execution_resource_conflict());
    }
    let gpu_leases: Vec<_> = leased
        .iter()
        .filter(|leased| leased.resource.kind == "gpu")
        .collect();
    let assigned_gpus = match &request.gpu {
        GpuRequest::None if gpu_leases.is_empty() => Vec::new(),
        GpuRequest::Specific(device)
            if gpu_leases.len() == 1
                && gpu_leases[0].resource.metadata["device"].as_str() == Some(device) =>
        {
            vec![device.clone()]
        }
        GpuRequest::Any
            if gpu_leases.len()
                == usize::try_from(request.gpu_count).map_err(|_| {
                    PersistenceError::InvalidValue {
                        entity: "GPU count",
                        value: request.gpu_count.to_string(),
                    }
                })? =>
        {
            gpu_leases
                .iter()
                .map(|leased| {
                    leased.resource.metadata["device"]
                        .as_str()
                        .map(str::to_owned)
                        .ok_or_else(|| PersistenceError::InvalidValue {
                            entity: "GPU resource metadata",
                            value: leased.resource.metadata.to_string(),
                        })
                })
                .collect::<PersistenceResult<Vec<_>>>()?
        }
        _ => return Err(execution_resource_conflict()),
    };
    let named: std::collections::BTreeSet<_> = leased
        .iter()
        .filter(|leased| leased.resource.kind == "named")
        .filter_map(|leased| leased.resource.name.strip_prefix("named:"))
        .collect();
    let expected_named: std::collections::BTreeSet<_> = request
        .named
        .iter()
        .map(|resource| resource.name.as_str())
        .collect();
    if named != expected_named {
        return Err(execution_resource_conflict());
    }
    let expected_count = 1
        + usize::from(request.cpu_threads.is_some())
        + usize::from(request.memory_bytes.is_some())
        + gpu_leases.len()
        + expected_named.len();
    if leased.len() != expected_count {
        return Err(execution_resource_conflict());
    }
    Ok(assigned_gpus)
}

async fn release_claim_resources(
    transaction: &mut Transaction<'_, Sqlite>,
    claim: &ExecutionClaim,
) -> PersistenceResult<()> {
    let rows = sqlx::query(
        "DELETE FROM resource_leases
         WHERE job_id = ? AND owner = ?
         RETURNING id",
    )
    .bind(claim.job.spec.id.to_string())
    .bind(&claim.owner)
    .fetch_all(&mut **transaction)
    .await
    .map_err(|source| db("release execution resources", source))?;
    if rows.len() != claim.resource_leases.len() {
        return Err(execution_resource_conflict());
    }
    for row in rows {
        let lease_id: Uuid = parse_id(row.get("id"), "released execution resource lease")?;
        let event = claim_event(
            EventKind::ResourceReleased,
            lease_id,
            &claim.owner,
            "released",
            "resource_lease",
            &lease_id.to_string(),
        )?;
        insert_event(
            transaction,
            claim.job.spec.project_id,
            Some(claim.job.spec.id),
            None,
            &event,
        )
        .await?;
    }
    Ok(())
}

async fn release_reclaimable_resource_leases(
    transaction: &mut Transaction<'_, Sqlite>,
) -> PersistenceResult<()> {
    let rows = sqlx::query(
        "DELETE FROM resource_leases
         WHERE expires_at <= strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
             AND NOT EXISTS (
                 SELECT 1 FROM jobs
                 WHERE jobs.id = resource_leases.job_id AND jobs.state = 'running'
             )
         RETURNING id, job_id, owner",
    )
    .fetch_all(&mut **transaction)
    .await
    .map_err(|source| db("release reclaimable resource leases", source))?;
    for row in rows {
        let lease_id: Uuid = parse_id(row.get("id"), "reclaimed resource lease")?;
        let job_id: JobId = parse_id(row.get("job_id"), "reclaimed resource lease job")?;
        let owner: String = row.get("owner");
        let project_id: String = sqlx::query_scalar("SELECT project_id FROM jobs WHERE id = ?")
            .bind(job_id.to_string())
            .fetch_one(&mut **transaction)
            .await
            .map_err(|source| db("read reclaimed resource lease project", source))?;
        let event = claim_event(
            EventKind::ResourceReleased,
            lease_id,
            &owner,
            "expired",
            "resource_lease",
            &lease_id.to_string(),
        )?;
        insert_event(
            transaction,
            parse_id(&project_id, "reclaimed resource lease project")?,
            Some(job_id),
            None,
            &event,
        )
        .await?;
    }
    Ok(())
}

#[derive(Clone, Debug, PartialEq)]
pub struct ArtifactRecord {
    pub id: Uuid,
    pub attempt_id: AttemptId,
    pub path: String,
    pub role: ArtifactRole,
    pub sha256: Option<String>,
    pub metadata: Value,
}

#[derive(Clone, Copy)]
pub struct ProjectRepository<'a> {
    database: &'a Database,
}

impl ProjectRepository<'_> {
    pub async fn register(&self, project: &Project) -> PersistenceResult<Project> {
        project.validate()?;
        let mut transaction = self
            .database
            .pool
            .begin()
            .await
            .map_err(|source| db("begin project registration", source))?;
        let conflict: Option<String> = sqlx::query_scalar(
            "SELECT root_path FROM projects WHERE config_path = ? AND root_path != ? AND is_alias = 0 LIMIT 1",
        )
        .bind(project.config_path.to_string_lossy().as_ref())
        .bind(project.root.to_string_lossy().as_ref())
        .fetch_optional(&mut *transaction)
        .await
        .map_err(|source| db("check project registration", source))?;
        if conflict.is_some() {
            return Err(PersistenceError::Conflict { entity: "project" });
        }
        sqlx::query(
            "INSERT INTO projects (id, name, root_path, config_path)
             SELECT ?, ?, ?, ? WHERE NOT EXISTS (SELECT 1 FROM projects WHERE root_path = ? AND is_alias = 0)",
        )
        .bind(project.id.to_string())
        .bind(&project.name)
        .bind(project.root.to_string_lossy().as_ref())
        .bind(project.config_path.to_string_lossy().as_ref())
        .bind(project.root.to_string_lossy().as_ref())
        .execute(&mut *transaction)
        .await
        .map_err(|source| db("register project", source))?;
        let row = sqlx::query(
            "SELECT id, name, root_path, config_path FROM projects WHERE root_path = ? AND is_alias = 0",
        )
        .bind(project.root.to_string_lossy().as_ref())
        .fetch_one(&mut *transaction)
        .await
        .map_err(|source| db("read registered project", source))?;
        let registered = decode_project(row)?;
        if registered.config_path != project.config_path {
            return Err(PersistenceError::Conflict { entity: "project" });
        }
        sqlx::query(
            "UPDATE projects SET registered = 1,
                 updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
             WHERE id = ? AND is_alias = 0",
        )
        .bind(registered.id.to_string())
        .execute(&mut *transaction)
        .await
        .map_err(|source| db("reactivate project registration", source))?;
        transaction
            .commit()
            .await
            .map_err(|source| db("commit project registration", source))?;
        Ok(registered)
    }

    pub async fn by_root(&self, root: &Path) -> PersistenceResult<Option<Project>> {
        let row = sqlx::query(
            "SELECT id, name, root_path, config_path FROM projects
             WHERE root_path = ? AND is_alias = 0 AND registered = 1",
        )
        .bind(root.to_string_lossy().as_ref())
        .fetch_optional(&self.database.pool)
        .await
        .map_err(|source| db("get project by root", source))?;
        row.map(decode_project).transpose()
    }

    pub async fn list(&self) -> PersistenceResult<Vec<Project>> {
        let rows = sqlx::query(
            "SELECT id, name, root_path, config_path FROM projects
             WHERE is_alias = 0 AND registered = 1
             ORDER BY name, id",
        )
        .fetch_all(&self.database.pool)
        .await
        .map_err(|source| db("list projects", source))?;
        rows.into_iter().map(decode_project).collect()
    }

    pub async fn insert(&self, project: &Project) -> PersistenceResult<()> {
        project.validate()?;
        sqlx::query("INSERT INTO projects (id, name, root_path, config_path) VALUES (?, ?, ?, ?)")
            .bind(project.id.to_string())
            .bind(&project.name)
            .bind(project.root.to_string_lossy().as_ref())
            .bind(project.config_path.to_string_lossy().as_ref())
            .execute(&self.database.pool)
            .await
            .map_err(|source| db("insert project", source))?;
        Ok(())
    }

    pub async fn get(&self, id: ProjectId) -> PersistenceResult<Option<Project>> {
        let row = sqlx::query(
            "SELECT id, name, root_path, config_path FROM projects
             WHERE id = ? AND registered = 1",
        )
        .bind(id.to_string())
        .fetch_optional(&self.database.pool)
        .await
        .map_err(|source| db("get project", source))?;
        row.map(decode_project).transpose()
    }

    pub async fn remove(&self, root: &Path) -> PersistenceResult<Project> {
        let mut transaction = self
            .database
            .pool
            .begin()
            .await
            .map_err(|source| db("begin project removal", source))?;
        let row = sqlx::query(
            "SELECT id, name, root_path, config_path FROM projects
             WHERE root_path = ? AND is_alias = 0 AND registered = 1",
        )
        .bind(root.to_string_lossy().as_ref())
        .fetch_optional(&mut *transaction)
        .await
        .map_err(|source| db("read project removal", source))?
        .ok_or(PersistenceError::NotFound { entity: "project" })?;
        let project = decode_project(row)?;
        let active: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM jobs
             WHERE project_id = ? AND state IN ('queued', 'running')",
        )
        .bind(project.id.to_string())
        .fetch_one(&mut *transaction)
        .await
        .map_err(|source| db("check active project jobs", source))?;
        if active != 0 {
            return Err(PersistenceError::Conflict { entity: "project" });
        }
        let removed = sqlx::query(
            "UPDATE projects SET registered = 0,
                 updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
             WHERE id = ? AND registered = 1 AND NOT EXISTS (
                 SELECT 1 FROM jobs
                 WHERE jobs.project_id = projects.id
                     AND jobs.state IN ('queued', 'running')
             )",
        )
        .bind(project.id.to_string())
        .execute(&mut *transaction)
        .await
        .map_err(|source| db("remove project registration", source))?;
        if removed.rows_affected() != 1 {
            return Err(PersistenceError::Conflict { entity: "project" });
        }
        transaction
            .commit()
            .await
            .map_err(|source| db("commit project removal", source))?;
        Ok(project)
    }

    pub async fn update(&self, project: &Project) -> PersistenceResult<()> {
        project.validate()?;
        let result = sqlx::query(
            "UPDATE projects SET name = ?, root_path = ?, config_path = ?, updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now') WHERE id = ?",
        )
        .bind(&project.name)
        .bind(project.root.to_string_lossy().as_ref())
        .bind(project.config_path.to_string_lossy().as_ref())
        .bind(project.id.to_string())
        .execute(&self.database.pool)
        .await
        .map_err(|source| db("update project", source))?;
        if result.rows_affected() == 0 {
            return Err(PersistenceError::NotFound { entity: "project" });
        }
        Ok(())
    }
}

#[derive(Clone, Copy)]
pub struct FamilyGenerationRepository<'a> {
    database: &'a Database,
}

impl FamilyGenerationRepository<'_> {
    pub async fn insert_family(&self, family: &Family) -> PersistenceResult<()> {
        sqlx::query("INSERT INTO families (id, project_id, name) VALUES (?, ?, ?)")
            .bind(family.id.to_string())
            .bind(family.project_id.to_string())
            .bind(&family.name)
            .execute(&self.database.pool)
            .await
            .map_err(|source| db("insert family", source))?;
        Ok(())
    }

    pub async fn get_family(&self, id: FamilyId) -> PersistenceResult<Option<Family>> {
        let row = sqlx::query("SELECT id, project_id, name FROM families WHERE id = ?")
            .bind(id.to_string())
            .fetch_optional(&self.database.pool)
            .await
            .map_err(|source| db("get family", source))?;
        row.map(|row| {
            Ok(Family {
                id: parse_id(row.get("id"), "family")?,
                project_id: parse_id(row.get("project_id"), "family project")?,
                name: row.get("name"),
            })
        })
        .transpose()
    }

    pub async fn insert_generation(&self, generation: &Generation) -> PersistenceResult<()> {
        let spec = json(&generation.spec, "generation spec")?;
        let result = sqlx::query(
            "INSERT INTO generations (id, family_id, project_id, generation_number, source_revision, protocol_digest, spec_json)
             SELECT ?, id, project_id, ?, ?, ?, ? FROM families WHERE id = ?",
        )
        .bind(generation.identity.id.to_string())
        .bind(i64::from(generation.identity.number))
        .bind(&generation.identity.source_revision)
        .bind(&generation.identity.protocol_digest)
        .bind(spec)
        .bind(generation.family_id.to_string())
        .execute(&self.database.pool)
        .await
        .map_err(|source| db("insert generation", source))?;
        if result.rows_affected() == 0 {
            return Err(PersistenceError::NotFound { entity: "family" });
        }
        Ok(())
    }

    pub async fn get_generation(&self, id: GenerationId) -> PersistenceResult<Option<Generation>> {
        let row = sqlx::query("SELECT id, family_id, generation_number, source_revision, protocol_digest, spec_json FROM generations WHERE id = ?")
            .bind(id.to_string())
            .fetch_optional(&self.database.pool)
            .await
            .map_err(|source| db("get generation", source))?;
        row.map(|row| {
            let number = u32::try_from(row.get::<i64, _>("generation_number")).map_err(|_| {
                PersistenceError::InvalidValue {
                    entity: "generation number",
                    value: row.get::<i64, _>("generation_number").to_string(),
                }
            })?;
            Ok(Generation {
                family_id: parse_id(row.get("family_id"), "generation family")?,
                identity: GenerationIdentity {
                    id: parse_id(row.get("id"), "generation")?,
                    number,
                    source_revision: row.get("source_revision"),
                    protocol_digest: row.get("protocol_digest"),
                },
                spec: from_json(row.get("spec_json"), "generation spec")?,
            })
        })
        .transpose()
    }
}

#[derive(Clone, Copy)]
pub struct JobAttemptRepository<'a> {
    database: &'a Database,
}

impl JobAttemptRepository<'_> {
    pub async fn submit(
        &self,
        job: &JobSpec,
        attempt: &AttemptSpec,
        priority: i64,
        job_event: &Event,
        attempt_event: &Event,
    ) -> PersistenceResult<StoredJob> {
        job.validate()?;
        attempt.validate()?;
        if attempt.job_id() != job.id
            || attempt.sequence() != 1
            || attempt.command() != &job.command
            || attempt.executor() != &job.executor
            || attempt.resources() != &job.resources
            || attempt.family() != job.family.as_ref()
        {
            return Err(PersistenceError::InvalidValue {
                entity: "initial attempt",
                value: format!("job {}, sequence {}", attempt.job_id(), attempt.sequence()),
            });
        }
        require_event_kind(
            job_event,
            EventKind::JobSubmitted,
            "job creation event kind",
        )?;
        require_event_kind(
            attempt_event,
            EventKind::AttemptCreated,
            "attempt creation event kind",
        )?;
        let (family_id, generation_id) = job.family.as_ref().map_or((None, None), |family| {
            (
                Some(family.family_id.to_string()),
                Some(family.generation.id.to_string()),
            )
        });
        let mut transaction = self
            .database
            .pool
            .begin()
            .await
            .map_err(|source| db("begin submission", source))?;
        sqlx::query(
            "INSERT INTO jobs (id, project_id, family_id, generation_id, name, state, priority, submission_order, spec_json)
             VALUES (?, ?, ?, ?, ?, 'queued', ?, (SELECT COALESCE(MAX(submission_order), 0) + 1 FROM jobs), ?)",
        )
        .bind(job.id.to_string())
        .bind(job.project_id.to_string())
        .bind(family_id)
        .bind(generation_id)
        .bind(&job.name)
        .bind(priority)
        .bind(json(job, "job spec")?)
        .execute(&mut *transaction)
        .await
        .map_err(|source| db("insert submitted job", source))?;
        insert_event(
            &mut transaction,
            job.project_id,
            Some(job.id),
            None,
            job_event,
        )
        .await?;
        sqlx::query(
            "INSERT INTO attempts (id, job_id, project_id, sequence, state, spec_json)
             VALUES (?, ?, ?, 1, 'pending', ?)",
        )
        .bind(attempt.id().to_string())
        .bind(job.id.to_string())
        .bind(job.project_id.to_string())
        .bind(json(attempt, "attempt spec")?)
        .execute(&mut *transaction)
        .await
        .map_err(|source| db("insert initial attempt", source))?;
        insert_event(
            &mut transaction,
            job.project_id,
            Some(job.id),
            Some(attempt.id()),
            attempt_event,
        )
        .await?;
        transaction
            .commit()
            .await
            .map_err(|source| db("commit submission", source))?;
        self.get_job(job.id)
            .await?
            .ok_or(PersistenceError::NotFound { entity: "job" })
    }

    pub async fn insert_job_with_event(
        &self,
        job: &JobSpec,
        priority: i64,
        event: &Event,
    ) -> PersistenceResult<()> {
        job.validate()?;
        require_event_kind(event, EventKind::JobSubmitted, "job creation event kind")?;
        let spec = json(job, "job spec")?;
        let (family_id, generation_id) = job.family.as_ref().map_or((None, None), |family| {
            (
                Some(family.family_id.to_string()),
                Some(family.generation.id.to_string()),
            )
        });
        let mut transaction = self
            .database
            .pool
            .begin()
            .await
            .map_err(|source| db("begin job creation", source))?;
        sqlx::query("INSERT INTO jobs (id, project_id, family_id, generation_id, name, state, priority, submission_order, spec_json) VALUES (?, ?, ?, ?, ?, 'queued', ?, (SELECT COALESCE(MAX(submission_order), 0) + 1 FROM jobs), ?)")
            .bind(job.id.to_string())
            .bind(job.project_id.to_string())
            .bind(family_id)
            .bind(generation_id)
            .bind(&job.name)
            .bind(priority)
            .bind(spec)
            .execute(&mut *transaction)
            .await
            .map_err(|source| db("insert job", source))?;
        insert_event(&mut transaction, job.project_id, Some(job.id), None, event).await?;
        transaction
            .commit()
            .await
            .map_err(|source| db("commit job creation", source))?;
        Ok(())
    }

    pub async fn get_job(&self, id: JobId) -> PersistenceResult<Option<StoredJob>> {
        let row = sqlx::query(
            "SELECT spec_json, state, priority, submission_order, submitted_at, updated_at FROM jobs WHERE id = ?",
        )
        .bind(id.to_string())
        .fetch_optional(&self.database.pool)
        .await
        .map_err(|source| db("get job", source))?;
        row.map(decode_job).transpose()
    }

    pub async fn list(&self, project_id: Option<ProjectId>) -> PersistenceResult<Vec<StoredJob>> {
        let rows = match project_id {
            Some(project_id) => {
                sqlx::query("SELECT spec_json, state, priority, submission_order, submitted_at, updated_at FROM jobs WHERE project_id = ? OR project_id IN (SELECT project_id FROM project_aliases WHERE canonical_project_id = ?) ORDER BY submission_order DESC")
                    .bind(project_id.to_string())
                    .bind(project_id.to_string())
                    .fetch_all(&self.database.pool)
                    .await
            }
            None => sqlx::query("SELECT spec_json, state, priority, submission_order, submitted_at, updated_at FROM jobs ORDER BY submission_order DESC")
                .fetch_all(&self.database.pool)
                .await,
        }
        .map_err(|source| db("list jobs", source))?;
        rows.into_iter().map(decode_job).collect()
    }

    pub async fn attempts_for_job(&self, job_id: JobId) -> PersistenceResult<Vec<StoredAttempt>> {
        let rows = sqlx::query("SELECT spec_json, state, created_at, started_at, finished_at, updated_at FROM attempts WHERE job_id = ? ORDER BY sequence")
            .bind(job_id.to_string())
            .fetch_all(&self.database.pool)
            .await
            .map_err(|source| db("list job attempts", source))?;
        rows.into_iter().map(decode_attempt).collect()
    }

    pub async fn detail(&self, job_id: JobId) -> PersistenceResult<Option<JobDetail>> {
        let Some(job) = self.get_job(job_id).await? else {
            return Ok(None);
        };
        Ok(Some(JobDetail {
            job,
            attempts: self.attempts_for_job(job_id).await?,
        }))
    }

    pub async fn claim_queued(
        &self,
        owner: &str,
        duration: Duration,
    ) -> PersistenceResult<Option<Claim<JobId>>> {
        let seconds = validate_lease(owner, duration)?;
        let lease_id = Uuid::new_v4();
        let mut transaction = self
            .database
            .pool
            .begin()
            .await
            .map_err(|source| db("begin queued job claim", source))?;
        let row = sqlx::query(
            "WITH candidate AS (
                SELECT id FROM jobs
                WHERE state = 'queued' OR (state = 'running' AND claim_expires_at <= strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
                ORDER BY priority + MAX(0,
                    CAST(strftime('%s', 'now') AS INTEGER) - CAST(strftime('%s', submitted_at) AS INTEGER)
                ) / ? DESC,
                         submission_order ASC LIMIT 1
             )
             UPDATE jobs SET state = 'running', claim_id = ?, claim_owner = ?,
                 claim_expires_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now', '+' || ? || ' seconds'),
                 updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
             WHERE id = (SELECT id FROM candidate)
             RETURNING id, project_id, claim_expires_at",
        )
        .bind(PRIORITY_AGING_INTERVAL_SECONDS)
        .bind(lease_id.to_string())
        .bind(owner)
        .bind(seconds)
        .fetch_optional(&mut *transaction)
        .await
        .map_err(|source| db("claim queued job", source))?;
        let claim = if let Some(row) = row {
            let record_id = parse_id(row.get("id"), "claimed job")?;
            let event = claim_event(
                EventKind::JobStateChanged,
                lease_id,
                owner,
                "running",
                "job",
                row.get("id"),
            )?;
            insert_event(
                &mut transaction,
                parse_id(row.get("project_id"), "claimed job project")?,
                Some(record_id),
                None,
                &event,
            )
            .await?;
            Some(Claim {
                record_id,
                lease_id,
                owner: owner.into(),
                expires_at: row.get("claim_expires_at"),
            })
        } else {
            None
        };
        transaction
            .commit()
            .await
            .map_err(|source| db("commit queued job claim", source))?;
        Ok(claim)
    }

    pub async fn claim_execution(
        &self,
        owner: &str,
        duration: Duration,
    ) -> PersistenceResult<Option<ExecutionClaim>> {
        let seconds = validate_lease(owner, duration)?;
        let lease_id = Uuid::new_v4();
        let mut transaction = self
            .database
            .pool
            .begin()
            .await
            .map_err(|source| db("begin execution claim", source))?;
        // Acquire SQLite's write lock before reading candidates so concurrent
        // schedulers cannot plan from the same lease snapshot.
        if !lock_execution_scheduler(&mut transaction, "lock execution scheduler").await? {
            transaction
                .commit()
                .await
                .map_err(|source| db("commit execution claim without inventory", source))?;
            return Ok(None);
        }
        release_reclaimable_resource_leases(&mut transaction).await?;
        let inventory = scheduling_resources(&mut transaction).await?;
        let candidates = sqlx::query(
            "SELECT jobs.id, jobs.project_id, jobs.spec_json, jobs.state, jobs.priority,
                    jobs.submission_order, jobs.submitted_at, jobs.updated_at
             FROM jobs
             JOIN attempts ON attempts.id = (
                 SELECT id FROM attempts AS latest
                 WHERE latest.job_id = jobs.id ORDER BY sequence DESC LIMIT 1
             )
             WHERE jobs.state = 'queued' AND attempts.state = 'pending'
             ORDER BY jobs.priority + MAX(0,
                 CAST(strftime('%s', 'now') AS INTEGER) - CAST(strftime('%s', jobs.submitted_at) AS INTEGER)
             ) / ? DESC,
                      jobs.submission_order ASC",
        )
        .bind(PRIORITY_AGING_INTERVAL_SECONDS)
        .fetch_all(&mut *transaction)
        .await
        .map_err(|source| db("list execution candidates", source))?;
        let mut selected = None;
        for job_row in candidates {
            let job = decode_job(job_row)?;
            let attempt_row = sqlx::query(
                "SELECT id, spec_json, state, created_at, started_at, finished_at, updated_at
                 FROM attempts WHERE id = (
                     SELECT id FROM attempts AS latest
                     WHERE latest.job_id = ? ORDER BY sequence DESC LIMIT 1
                 ) AND state = 'pending'",
            )
            .bind(job.spec.id.to_string())
            .fetch_optional(&mut *transaction)
            .await
            .map_err(|source| db("read execution candidate attempt", source))?
            .ok_or(PersistenceError::Conflict {
                entity: "execution attempt",
            })?;
            let attempt = decode_attempt(attempt_row)?;
            if let Some(plan) = plan_resource_leases(attempt.spec.resources(), &inventory)? {
                selected = Some((job, attempt, plan));
                break;
            }
        }
        let Some((candidate_job, candidate_attempt, plan)) = selected else {
            transaction
                .commit()
                .await
                .map_err(|source| db("commit empty execution claim", source))?;
            return Ok(None);
        };
        let job_id = candidate_job.spec.id;
        let project_id = candidate_job.spec.project_id;
        let resource_leases = reserve_execution_resources(
            &mut transaction,
            project_id,
            job_id,
            owner,
            seconds,
            &plan,
        )
        .await?;
        let job_row = sqlx::query(
            "UPDATE jobs SET state = 'running', claim_id = ?, claim_owner = ?,
                 claim_expires_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now', '+' || ? || ' seconds'),
                 updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
             WHERE id = ? AND state = 'queued'
             RETURNING id, project_id, spec_json, state, priority, submission_order,
                       submitted_at, updated_at, claim_expires_at",
        )
        .bind(lease_id.to_string())
        .bind(owner)
        .bind(seconds)
        .bind(job_id.to_string())
        .fetch_optional(&mut *transaction)
        .await
        .map_err(|source| db("claim execution job", source))?
        .ok_or(PersistenceError::Conflict {
            entity: "execution job",
        })?;
        let expires_at = job_row.get("claim_expires_at");
        let job = decode_job(job_row)?;
        let attempt_row = sqlx::query(
            "UPDATE attempts SET state = 'starting',
                 updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
             WHERE id = ? AND state = 'pending'
             RETURNING id, spec_json, state, created_at, started_at, finished_at, updated_at",
        )
        .bind(candidate_attempt.spec.id().to_string())
        .fetch_optional(&mut *transaction)
        .await
        .map_err(|source| db("claim execution attempt", source))?
        .ok_or(PersistenceError::Conflict {
            entity: "execution attempt",
        })?;
        let attempt_id: AttemptId = parse_id(attempt_row.get("id"), "claimed execution attempt")?;
        let attempt = decode_attempt(attempt_row)?;
        let job_event = state_event(
            EventKind::JobStateChanged,
            "job",
            &job_id.to_string(),
            "running",
            Some(lease_id),
            None,
        )?;
        insert_event(&mut transaction, project_id, Some(job_id), None, &job_event).await?;
        let attempt_event = state_event(
            EventKind::AttemptStateChanged,
            "attempt",
            &attempt_id.to_string(),
            "starting",
            Some(lease_id),
            None,
        )?;
        insert_event(
            &mut transaction,
            project_id,
            Some(job_id),
            Some(attempt_id),
            &attempt_event,
        )
        .await?;
        transaction
            .commit()
            .await
            .map_err(|source| db("commit execution claim", source))?;
        Ok(Some(ExecutionClaim {
            job,
            attempt,
            lease_id,
            owner: owner.into(),
            expires_at,
            assigned_gpus: assigned_gpus(&plan),
            resource_leases,
        }))
    }

    pub async fn claim_recovery(
        &self,
        owner: &str,
        duration: Duration,
    ) -> PersistenceResult<Option<RecoveredExecution>> {
        let seconds = validate_lease(owner, duration)?;
        let lease_id = Uuid::new_v4();
        let mut transaction = self
            .database
            .pool
            .begin()
            .await
            .map_err(|source| db("begin execution recovery claim", source))?;
        lock_execution_scheduler(&mut transaction, "lock execution recovery scheduler").await?;
        release_reclaimable_resource_leases(&mut transaction).await?;
        let job_row = sqlx::query(
            "WITH candidate AS (
                 SELECT jobs.id FROM jobs
                 JOIN attempts ON attempts.id = (
                     SELECT id FROM attempts AS latest
                     WHERE latest.job_id = jobs.id ORDER BY sequence DESC LIMIT 1
                 )
                 WHERE jobs.state = 'running' AND attempts.state IN ('starting', 'running')
                     AND jobs.claim_expires_at <= strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
                 ORDER BY jobs.updated_at, jobs.submission_order LIMIT 1
             )
             UPDATE jobs SET claim_id = ?, claim_owner = ?,
                 claim_expires_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now', '+' || ? || ' seconds'),
                 updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
             WHERE id = (SELECT id FROM candidate)
             RETURNING id, project_id, spec_json, state, priority, submission_order,
                       submitted_at, updated_at, claim_expires_at",
        )
        .bind(lease_id.to_string())
        .bind(owner)
        .bind(seconds)
        .fetch_optional(&mut *transaction)
        .await
        .map_err(|source| db("claim execution recovery", source))?;
        let Some(job_row) = job_row else {
            transaction
                .commit()
                .await
                .map_err(|source| db("commit empty execution recovery claim", source))?;
            return Ok(None);
        };
        let job_id: JobId = parse_id(job_row.get("id"), "recovered job")?;
        let expires_at = job_row.get("claim_expires_at");
        let job = decode_job(job_row)?;
        let project_id = job.spec.project_id;
        let attempt_row = sqlx::query(
            "SELECT id, spec_json, state, created_at, started_at, finished_at, updated_at
             FROM attempts WHERE id = (
                 SELECT id FROM attempts AS latest
                 WHERE latest.job_id = ? ORDER BY sequence DESC LIMIT 1
             ) AND state IN ('starting', 'running')",
        )
        .bind(job_id.to_string())
        .fetch_optional(&mut *transaction)
        .await
        .map_err(|source| db("read recovered attempt", source))?
        .ok_or(PersistenceError::Conflict {
            entity: "recovered attempt",
        })?;
        let attempt = decode_attempt(attempt_row)?;
        let existing_leases = execution_resource_leases(&mut transaction, job_id).await?;
        let (resource_leases, assigned_gpus) = if existing_leases.is_empty()
            && matches!(attempt.spec.executor(), ExecutorSpec::Docker(_))
        {
            return Err(PersistenceError::Conflict {
                entity: "recovered Docker execution resources",
            });
        } else if existing_leases.is_empty() {
            let inventory = scheduling_resources(&mut transaction).await?;
            let plan = plan_resource_leases(attempt.spec.resources(), &inventory)?.ok_or(
                PersistenceError::Conflict {
                    entity: "recovered execution resources",
                },
            )?;
            let assigned_gpus = assigned_gpus(&plan);
            let leases = reserve_execution_resources(
                &mut transaction,
                project_id,
                job_id,
                owner,
                seconds,
                &plan,
            )
            .await?;
            (leases, assigned_gpus)
        } else {
            let assigned_gpus =
                validate_execution_resource_leases(attempt.spec.resources(), &existing_leases)?;
            let updated = sqlx::query(
                "UPDATE resource_leases SET owner = ?,
                     expires_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now', '+' || ? || ' seconds'),
                     heartbeat_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
                 WHERE job_id = ?",
            )
            .bind(owner)
            .bind(seconds)
            .bind(job_id.to_string())
            .execute(&mut *transaction)
            .await
            .map_err(|source| db("transfer recovered resource leases", source))?;
            if usize::try_from(updated.rows_affected()).ok() != Some(existing_leases.len()) {
                return Err(PersistenceError::Conflict {
                    entity: "recovered execution resources",
                });
            }
            let transferred = execution_resource_leases(&mut transaction, job_id).await?;
            (
                transferred.into_iter().map(|leased| leased.lease).collect(),
                assigned_gpus,
            )
        };
        let process_row = sqlx::query(
            "SELECT attempt_id, job_id, project_id, pid, process_group_id,
                    process_start_ticks, stdout_path, stderr_path, heartbeat_at,
                    exit_code, term_signal, error
             FROM attempt_processes WHERE attempt_id = ?",
        )
        .bind(attempt.spec.id().to_string())
        .fetch_optional(&mut *transaction)
        .await
        .map_err(|source| db("read recovered process", source))?;
        let process = process_row.map(decode_process).transpose()?;
        let container_row = sqlx::query(
            "SELECT attempt_id, job_id, project_id, container_id, container_name,
                    image_reference, image_id, state, docker_status, stdout_path, stderr_path,
                    exit_code, oom_killed, error, started_at, finished_at, removed_at,
                    created_at, updated_at
             FROM attempt_containers WHERE attempt_id = ?",
        )
        .bind(attempt.spec.id().to_string())
        .fetch_optional(&mut *transaction)
        .await
        .map_err(|source| db("read recovered container", source))?;
        let container = container_row.map(decode_container).transpose()?;
        if matches!(attempt.spec.executor(), ExecutorSpec::Docker(_)) && process.is_some() {
            return Err(PersistenceError::Conflict {
                entity: "recovered executor identity",
            });
        }
        if !matches!(attempt.spec.executor(), ExecutorSpec::Docker(_)) && container.is_some() {
            return Err(PersistenceError::Conflict {
                entity: "recovered executor identity",
            });
        }
        if process.is_some() {
            let heartbeat = sqlx::query(
                "UPDATE attempt_processes SET
                     heartbeat_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now'),
                     updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
                 WHERE attempt_id = ?",
            )
            .bind(attempt.spec.id().to_string())
            .execute(&mut *transaction)
            .await
            .map_err(|source| db("heartbeat recovered process", source))?;
            if heartbeat.rows_affected() != 1 {
                return Err(PersistenceError::Conflict {
                    entity: "recovered process",
                });
            }
        }
        let timeout_remaining =
            if let Some(timeout_seconds) = attempt.spec.resources().timeout_seconds {
                sqlx::query_scalar::<_, Option<i64>>(
                    "SELECT CAST(MAX(0, ROUND(
                         (julianday(started_at, '+' || ? || ' seconds')
                          - julianday('now')) * 86400000
                     )) AS INTEGER)
                 FROM attempts
                 WHERE id = ?",
                )
                .bind(i64::try_from(timeout_seconds).map_err(|_| {
                    PersistenceError::InvalidValue {
                        entity: "execution timeout",
                        value: timeout_seconds.to_string(),
                    }
                })?)
                .bind(attempt.spec.id().to_string())
                .fetch_optional(&mut *transaction)
                .await
                .map_err(|source| db("read recovered execution timeout", source))?
                .flatten()
                .map(|milliseconds| {
                    u64::try_from(milliseconds)
                        .map(Duration::from_millis)
                        .map_err(|_| PersistenceError::InvalidValue {
                            entity: "recovered execution timeout",
                            value: milliseconds.to_string(),
                        })
                })
                .transpose()?
            } else {
                None
            };
        transaction
            .commit()
            .await
            .map_err(|source| db("commit execution recovery claim", source))?;
        Ok(Some(RecoveredExecution {
            claim: ExecutionClaim {
                job,
                attempt,
                lease_id,
                owner: owner.into(),
                expires_at,
                resource_leases,
                assigned_gpus,
            },
            process,
            container,
            timeout_remaining,
        }))
    }

    pub async fn release_execution(&self, claim: &ExecutionClaim) -> PersistenceResult<()> {
        let mut transaction = self
            .database
            .pool
            .begin()
            .await
            .map_err(|source| db("begin execution claim release", source))?;
        let updated = sqlx::query(
            "UPDATE jobs SET
                 claim_expires_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now'),
                 updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
             WHERE id = ? AND state = 'running' AND claim_id = ? AND claim_owner = ?",
        )
        .bind(claim.job.spec.id.to_string())
        .bind(claim.lease_id.to_string())
        .bind(&claim.owner)
        .execute(&mut *transaction)
        .await
        .map_err(|source| db("release execution claim", source))?;
        if updated.rows_affected() != 1 {
            return Err(PersistenceError::Conflict {
                entity: "execution claim",
            });
        }
        let resources = sqlx::query(
            "UPDATE resource_leases SET
                 expires_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now'),
                 heartbeat_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
             WHERE job_id = ? AND owner = ?",
        )
        .bind(claim.job.spec.id.to_string())
        .bind(&claim.owner)
        .execute(&mut *transaction)
        .await
        .map_err(|source| db("release execution resource claims", source))?;
        if usize::try_from(resources.rows_affected()).ok() != Some(claim.resource_leases.len()) {
            return Err(execution_resource_conflict());
        }
        transaction
            .commit()
            .await
            .map_err(|source| db("commit execution claim release", source))?;
        Ok(())
    }

    pub async fn record_container_created(
        &self,
        claim: &ExecutionClaim,
        container: &ContainerCreate,
    ) -> PersistenceResult<()> {
        if !matches!(claim.attempt.spec.executor(), ExecutorSpec::Docker(_)) {
            return Err(PersistenceError::InvalidValue {
                entity: "container execution",
                value: "attempt does not use the Docker executor".into(),
            });
        }
        container.image.validate()?;
        let stdout_path = container.stdout_path.to_str();
        let stderr_path = container.stderr_path.to_str();
        if container.container_id.trim().is_empty()
            || container.container_name != format!("igor-{}", claim.attempt.spec.id())
            || stdout_path.is_none_or(str::is_empty)
            || stderr_path.is_none_or(str::is_empty)
        {
            return Err(PersistenceError::InvalidValue {
                entity: "container identity",
                value: format!(
                    "id {:?}, name {:?}, stdout {:?}, stderr {:?}",
                    container.container_id,
                    container.container_name,
                    container.stdout_path,
                    container.stderr_path
                ),
            });
        }
        let mut transaction = self
            .database
            .pool
            .begin()
            .await
            .map_err(|source| db("begin container creation record", source))?;
        if !lock_execution_scheduler(&mut transaction, "lock container creation").await? {
            return Err(execution_resource_conflict());
        }
        validate_execution_claim_resources(&mut transaction, claim).await?;
        let inserted = sqlx::query(
            "INSERT INTO attempt_containers
                 (attempt_id, job_id, project_id, container_id, container_name,
                  image_reference, image_id, state, docker_status, stdout_path, stderr_path)
             SELECT ?, ?, ?, ?, ?, ?, ?, 'created', 'created', ?, ?
             WHERE EXISTS (
                 SELECT 1 FROM jobs
                 JOIN attempts ON attempts.job_id = jobs.id
                 WHERE jobs.id = ? AND jobs.state = 'running'
                     AND jobs.claim_id = ? AND jobs.claim_owner = ?
                     AND jobs.claim_expires_at > strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
                     AND attempts.id = ? AND attempts.state = 'starting'
             )",
        )
        .bind(claim.attempt.spec.id().to_string())
        .bind(claim.job.spec.id.to_string())
        .bind(claim.job.spec.project_id.to_string())
        .bind(&container.container_id)
        .bind(&container.container_name)
        .bind(&container.image.reference)
        .bind(&container.image.image_id)
        .bind(stdout_path)
        .bind(stderr_path)
        .bind(claim.job.spec.id.to_string())
        .bind(claim.lease_id.to_string())
        .bind(&claim.owner)
        .bind(claim.attempt.spec.id().to_string())
        .execute(&mut *transaction)
        .await
        .map_err(|source| db("record created container", source))?;
        if inserted.rows_affected() != 1 {
            return Err(PersistenceError::Conflict {
                entity: "container creation",
            });
        }
        transaction
            .commit()
            .await
            .map_err(|source| db("commit container creation record", source))?;
        Ok(())
    }

    pub async fn record_container_started(&self, claim: &ExecutionClaim) -> PersistenceResult<()> {
        let mut transaction = self
            .database
            .pool
            .begin()
            .await
            .map_err(|source| db("begin container start", source))?;
        if !lock_execution_scheduler(&mut transaction, "lock container start").await? {
            return Err(execution_resource_conflict());
        }
        validate_execution_claim_resources(&mut transaction, claim).await?;
        let attempt_id = claim.attempt.spec.id();
        let attempt = sqlx::query(
            "UPDATE attempts SET state = 'running',
                 started_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now'),
                 updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
             WHERE id = ? AND state = 'starting' AND EXISTS (
                 SELECT 1 FROM jobs
                 WHERE jobs.id = ? AND jobs.state = 'running'
                     AND jobs.claim_id = ? AND jobs.claim_owner = ?
                     AND jobs.claim_expires_at > strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
             )",
        )
        .bind(attempt_id.to_string())
        .bind(claim.job.spec.id.to_string())
        .bind(claim.lease_id.to_string())
        .bind(&claim.owner)
        .execute(&mut *transaction)
        .await
        .map_err(|source| db("mark container attempt running", source))?;
        if attempt.rows_affected() != 1 {
            return Err(PersistenceError::Conflict {
                entity: "container attempt",
            });
        }
        let container = sqlx::query(
            "UPDATE attempt_containers SET state = 'running', docker_status = 'running',
                 started_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now'),
                 updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
             WHERE attempt_id = ? AND state = 'created' AND EXISTS (
                 SELECT 1 FROM jobs
                 WHERE jobs.id = ? AND jobs.state = 'running'
                     AND jobs.claim_id = ? AND jobs.claim_owner = ?
                     AND jobs.claim_expires_at > strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
             )",
        )
        .bind(attempt_id.to_string())
        .bind(claim.job.spec.id.to_string())
        .bind(claim.lease_id.to_string())
        .bind(&claim.owner)
        .execute(&mut *transaction)
        .await
        .map_err(|source| db("mark container running", source))?;
        if container.rows_affected() != 1 {
            return Err(PersistenceError::Conflict {
                entity: "container",
            });
        }
        let event = state_event(
            EventKind::AttemptStateChanged,
            "attempt",
            &attempt_id.to_string(),
            "running",
            Some(claim.lease_id),
            None,
        )?;
        insert_event(
            &mut transaction,
            claim.job.spec.project_id,
            Some(claim.job.spec.id),
            Some(attempt_id),
            &event,
        )
        .await?;
        transaction
            .commit()
            .await
            .map_err(|source| db("commit container start", source))?;
        Ok(())
    }

    pub async fn container_for_attempt(
        &self,
        attempt_id: AttemptId,
    ) -> PersistenceResult<Option<ContainerRecord>> {
        let row = sqlx::query(
            "SELECT attempt_id, job_id, project_id, container_id, container_name,
                    image_reference, image_id, state, docker_status, stdout_path, stderr_path,
                    exit_code, oom_killed, error, started_at, finished_at, removed_at,
                    created_at, updated_at
             FROM attempt_containers WHERE attempt_id = ?",
        )
        .bind(attempt_id.to_string())
        .fetch_optional(&self.database.pool)
        .await
        .map_err(|source| db("read attempt container", source))?;
        row.map(decode_container).transpose()
    }

    pub async fn containers_pending_cleanup(&self) -> PersistenceResult<Vec<ContainerRecord>> {
        let rows = sqlx::query(
            "SELECT attempt_id, job_id, project_id, container_id, container_name,
                    image_reference, image_id, state, docker_status, stdout_path, stderr_path,
                    exit_code, oom_killed, error, started_at, finished_at, removed_at,
                    created_at, updated_at
             FROM attempt_containers
             WHERE state IN ('exited', 'lost') AND removed_at IS NULL
             ORDER BY updated_at, attempt_id",
        )
        .fetch_all(&self.database.pool)
        .await
        .map_err(|source| db("list containers pending cleanup", source))?;
        let mut pending = Vec::new();
        for row in rows {
            let container = decode_container(row)?;
            let spec_json: String =
                sqlx::query_scalar("SELECT spec_json FROM attempts WHERE id = ?")
                    .bind(container.attempt_id.to_string())
                    .fetch_one(&self.database.pool)
                    .await
                    .map_err(|source| db("read container cleanup attempt", source))?;
            let attempt: AttemptSpec = from_json(&spec_json, "attempt spec")?;
            if matches!(
                attempt.executor(),
                ExecutorSpec::Docker(DockerExecutorSpec {
                    remove_container: true,
                    ..
                })
            ) {
                pending.push(container);
            }
        }
        Ok(pending)
    }

    pub async fn record_process_started(
        &self,
        claim: &ExecutionClaim,
        process: &ProcessStart,
    ) -> PersistenceResult<()> {
        if process.pid <= 0 || process.process_group_id <= 0 || process.process_start_ticks < 0 {
            return Err(PersistenceError::InvalidValue {
                entity: "process identity",
                value: format!(
                    "pid {}, process group {}, start ticks {}",
                    process.pid, process.process_group_id, process.process_start_ticks
                ),
            });
        }
        let mut transaction = self
            .database
            .pool
            .begin()
            .await
            .map_err(|source| db("begin process start", source))?;
        lock_execution_scheduler(&mut transaction, "lock process start").await?;
        let attempt_id = claim.attempt.spec.id();
        let job_id = claim.job.spec.id;
        let project_id = claim.job.spec.project_id;
        validate_execution_claim_resources(&mut transaction, claim).await?;
        let inserted = sqlx::query(
            "INSERT INTO attempt_processes
                 (attempt_id, job_id, project_id, pid, process_group_id, process_start_ticks,
                  stdout_path, stderr_path)
             SELECT ?, ?, ?, ?, ?, ?, ?, ?
             WHERE EXISTS (
                  SELECT 1 FROM jobs WHERE id = ? AND state = 'running'
                      AND claim_id = ? AND claim_owner = ?
                      AND claim_expires_at > strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
             )",
        )
        .bind(attempt_id.to_string())
        .bind(job_id.to_string())
        .bind(project_id.to_string())
        .bind(process.pid)
        .bind(process.process_group_id)
        .bind(process.process_start_ticks)
        .bind(process.stdout_path.to_string_lossy().as_ref())
        .bind(process.stderr_path.to_string_lossy().as_ref())
        .bind(job_id.to_string())
        .bind(claim.lease_id.to_string())
        .bind(&claim.owner)
        .execute(&mut *transaction)
        .await
        .map_err(|source| db("insert process identity", source))?;
        if inserted.rows_affected() != 1 {
            return Err(PersistenceError::Conflict {
                entity: "execution claim",
            });
        }
        let updated = sqlx::query(
            "UPDATE attempts SET state = 'running',
                 started_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now'),
                 updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
             WHERE id = ? AND state = 'starting'",
        )
        .bind(attempt_id.to_string())
        .execute(&mut *transaction)
        .await
        .map_err(|source| db("mark process attempt running", source))?;
        if updated.rows_affected() != 1 {
            return Err(PersistenceError::Conflict {
                entity: "execution attempt",
            });
        }
        let event = state_event(
            EventKind::AttemptStateChanged,
            "attempt",
            &attempt_id.to_string(),
            "running",
            Some(claim.lease_id),
            None,
        )?;
        insert_event(
            &mut transaction,
            project_id,
            Some(job_id),
            Some(attempt_id),
            &event,
        )
        .await?;
        transaction
            .commit()
            .await
            .map_err(|source| db("commit process start", source))?;
        Ok(())
    }

    pub async fn heartbeat_execution(
        &self,
        claim: &ExecutionClaim,
        duration: Duration,
    ) -> PersistenceResult<()> {
        let seconds = validate_lease(&claim.owner, duration)?;
        let mut transaction = self
            .database
            .pool
            .begin()
            .await
            .map_err(|source| db("begin execution heartbeat", source))?;
        let updated = sqlx::query(
            "UPDATE jobs SET
                 claim_expires_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now', '+' || ? || ' seconds'),
                 updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
             WHERE id = ? AND state = 'running' AND claim_id = ? AND claim_owner = ?
                 AND claim_expires_at > strftime('%Y-%m-%dT%H:%M:%fZ', 'now')",
        )
        .bind(seconds)
        .bind(claim.job.spec.id.to_string())
        .bind(claim.lease_id.to_string())
        .bind(&claim.owner)
        .execute(&mut *transaction)
        .await
        .map_err(|source| db("heartbeat execution claim", source))?;
        if updated.rows_affected() != 1 {
            return Err(PersistenceError::Conflict {
                entity: "execution claim",
            });
        }
        let resources = sqlx::query(
            "UPDATE resource_leases SET
                 expires_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now', '+' || ? || ' seconds'),
                 heartbeat_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
             WHERE job_id = ? AND owner = ?",
        )
        .bind(seconds)
        .bind(claim.job.spec.id.to_string())
        .bind(&claim.owner)
        .execute(&mut *transaction)
        .await
        .map_err(|source| db("heartbeat execution resources", source))?;
        if usize::try_from(resources.rows_affected()).ok() != Some(claim.resource_leases.len()) {
            return Err(execution_resource_conflict());
        }
        let (identity_rows, identity_entity) = match claim.attempt.spec.executor() {
            ExecutorSpec::Process(_) => {
                let result = sqlx::query(
                    "UPDATE attempt_processes SET
                         heartbeat_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now'),
                         updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
                     WHERE attempt_id = ? AND job_id = ? AND project_id = ?",
                )
                .bind(claim.attempt.spec.id().to_string())
                .bind(claim.job.spec.id.to_string())
                .bind(claim.job.spec.project_id.to_string())
                .execute(&mut *transaction)
                .await
                .map_err(|source| db("heartbeat attempt process", source))?;
                (result.rows_affected(), "attempt process")
            }
            ExecutorSpec::Docker(_) => {
                let result = sqlx::query(
                    "UPDATE attempt_containers SET
                         updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
                     WHERE attempt_id = ? AND job_id = ? AND project_id = ?
                         AND state IN ('created', 'running')",
                )
                .bind(claim.attempt.spec.id().to_string())
                .bind(claim.job.spec.id.to_string())
                .bind(claim.job.spec.project_id.to_string())
                .execute(&mut *transaction)
                .await
                .map_err(|source| db("heartbeat attempt container", source))?;
                let rows = if result.rows_affected() == 1 {
                    1
                } else {
                    let pre_identity: bool = sqlx::query_scalar(
                        "SELECT EXISTS(
                                 SELECT 1 FROM attempts
                                 WHERE id = ? AND job_id = ? AND project_id = ?
                                     AND state = 'starting' AND NOT EXISTS (
                                         SELECT 1 FROM attempt_containers
                                         WHERE attempt_id = attempts.id
                                     )
                             )",
                    )
                    .bind(claim.attempt.spec.id().to_string())
                    .bind(claim.job.spec.id.to_string())
                    .bind(claim.job.spec.project_id.to_string())
                    .fetch_one(&mut *transaction)
                    .await
                    .map_err(|source| db("check pre-identity Docker heartbeat", source))?;
                    if pre_identity { 1 } else { 0 }
                };
                (rows, "attempt container")
            }
        };
        if identity_rows != 1 {
            return Err(PersistenceError::Conflict {
                entity: identity_entity,
            });
        }
        transaction
            .commit()
            .await
            .map_err(|source| db("commit execution heartbeat", source))?;
        Ok(())
    }

    pub async fn cancellation_grace(
        &self,
        claim: &ExecutionClaim,
    ) -> PersistenceResult<Option<Duration>> {
        let milliseconds = sqlx::query_scalar::<_, Option<i64>>(
            "SELECT CAST(MAX(0, ROUND(
                     (julianday(cancel_requested_at, '+' || cancel_grace_seconds || ' seconds')
                      - julianday('now')) * 86400000
                 )) AS INTEGER)
             FROM jobs
             WHERE id = ? AND state = 'running' AND claim_id = ? AND claim_owner = ?
                 AND claim_expires_at > strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
                 AND cancel_requested_at IS NOT NULL",
        )
        .bind(claim.job.spec.id.to_string())
        .bind(claim.lease_id.to_string())
        .bind(&claim.owner)
        .fetch_optional(&self.database.pool)
        .await
        .map_err(|source| db("read execution cancellation", source))?
        .flatten();
        milliseconds
            .map(|milliseconds| {
                u64::try_from(milliseconds)
                    .map(Duration::from_millis)
                    .map_err(|_| PersistenceError::InvalidValue {
                        entity: "cancellation grace period",
                        value: milliseconds.to_string(),
                    })
            })
            .transpose()
    }

    pub async fn finish_execution(
        &self,
        claim: &ExecutionClaim,
        outcome: &ExecutionOutcome,
    ) -> PersistenceResult<()> {
        validate_execution_outcome(outcome)?;
        let attempt_id = claim.attempt.spec.id();
        let mut transaction = self
            .database
            .pool
            .begin()
            .await
            .map_err(|source| db("begin execution finish", source))?;
        sqlx::query(
            "UPDATE attempt_processes SET exit_code = ?, term_signal = ?, error = ?,
                 updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
             WHERE attempt_id = ?",
        )
        .bind(outcome.exit_code)
        .bind(outcome.term_signal)
        .bind(&outcome.error)
        .bind(attempt_id.to_string())
        .execute(&mut *transaction)
        .await
        .map_err(|source| db("record process outcome", source))?;
        let details = serde_json::json!({
            "exit_code": outcome.exit_code,
            "term_signal": outcome.term_signal,
            "error": outcome.error,
        });
        finish_execution_transaction(&mut transaction, claim, outcome, details).await?;
        transaction
            .commit()
            .await
            .map_err(|source| db("commit execution finish", source))?;
        Ok(())
    }

    pub async fn finish_container_execution(
        &self,
        claim: &ExecutionClaim,
        finish: &ContainerFinish,
        outcome: &ExecutionOutcome,
    ) -> PersistenceResult<()> {
        if !matches!(claim.attempt.spec.executor(), ExecutorSpec::Docker(_)) {
            return Err(PersistenceError::InvalidValue {
                entity: "container execution",
                value: "attempt does not use the Docker executor".into(),
            });
        }
        if !matches!(
            finish.state,
            DockerContainerState::Exited | DockerContainerState::Lost
        ) {
            return Err(PersistenceError::InvalidValue {
                entity: "container finish state",
                value: finish.state.as_str().into(),
            });
        }
        validate_execution_outcome(outcome)?;
        if finish.exit_code != outcome.exit_code
            || finish.error != outcome.error
            || outcome.term_signal.is_some()
            || (finish.state == DockerContainerState::Lost && outcome.state != AttemptState::Lost)
            || (finish.oom_killed
                && !matches!(
                    outcome.state,
                    AttemptState::Failed | AttemptState::Cancelled
                ))
        {
            return Err(PersistenceError::InvalidValue {
                entity: "container execution outcome",
                value: "container and attempt outcomes are inconsistent".into(),
            });
        }
        let attempt_id = claim.attempt.spec.id();
        let mut transaction = self
            .database
            .pool
            .begin()
            .await
            .map_err(|source| db("begin container execution finish", source))?;
        let container = sqlx::query(
            "UPDATE attempt_containers SET state = ?, docker_status = ?, exit_code = ?,
                 oom_killed = ?, error = ?,
                 finished_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now'),
                 updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
             WHERE attempt_id = ? AND job_id = ? AND project_id = ?
                 AND state IN ('created', 'running') AND EXISTS (
                 SELECT 1 FROM jobs
                 WHERE jobs.id = ? AND jobs.state = 'running'
                     AND jobs.claim_id = ? AND jobs.claim_owner = ?
                     AND jobs.claim_expires_at > strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
             )",
        )
        .bind(finish.state.as_str())
        .bind(&finish.docker_status)
        .bind(finish.exit_code)
        .bind(finish.oom_killed)
        .bind(&finish.error)
        .bind(attempt_id.to_string())
        .bind(claim.job.spec.id.to_string())
        .bind(claim.job.spec.project_id.to_string())
        .bind(claim.job.spec.id.to_string())
        .bind(claim.lease_id.to_string())
        .bind(&claim.owner)
        .execute(&mut *transaction)
        .await
        .map_err(|source| db("record container outcome", source))?;
        if container.rows_affected() != 1 {
            return Err(PersistenceError::Conflict {
                entity: "container execution",
            });
        }
        let details = serde_json::json!({
            "exit_code": outcome.exit_code,
            "term_signal": outcome.term_signal,
            "error": outcome.error,
            "container_state": finish.state.as_str(),
            "docker_status": finish.docker_status,
            "container_exit_code": finish.exit_code,
            "oom_killed": finish.oom_killed,
            "container_error": finish.error,
        });
        finish_execution_transaction(&mut transaction, claim, outcome, details).await?;
        transaction
            .commit()
            .await
            .map_err(|source| db("commit container execution finish", source))?;
        Ok(())
    }

    pub async fn record_container_removed(
        &self,
        attempt_id: AttemptId,
        container_id: &str,
    ) -> PersistenceResult<()> {
        let mut transaction = self
            .database
            .pool
            .begin()
            .await
            .map_err(|source| db("begin container removal record", source))?;
        let removed = sqlx::query(
            "UPDATE attempt_containers SET state = 'removed',
                 removed_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now'),
                 updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
             WHERE attempt_id = ? AND container_id = ? AND state IN ('exited', 'lost')",
        )
        .bind(attempt_id.to_string())
        .bind(container_id)
        .execute(&mut *transaction)
        .await
        .map_err(|source| db("record container removal", source))?;
        if removed.rows_affected() == 0 {
            let state: Option<String> = sqlx::query_scalar(
                "SELECT state FROM attempt_containers
                 WHERE attempt_id = ? AND container_id = ?",
            )
            .bind(attempt_id.to_string())
            .bind(container_id)
            .fetch_optional(&mut *transaction)
            .await
            .map_err(|source| db("read container removal state", source))?;
            if state.as_deref() != Some("removed") {
                return Err(PersistenceError::Conflict {
                    entity: "container removal",
                });
            }
        }
        transaction
            .commit()
            .await
            .map_err(|source| db("commit container removal record", source))?;
        Ok(())
    }

    pub async fn process_for_attempt(
        &self,
        attempt_id: AttemptId,
    ) -> PersistenceResult<Option<ProcessRecord>> {
        let row = sqlx::query(
            "SELECT attempt_id, job_id, project_id, pid, process_group_id,
                    process_start_ticks, stdout_path, stderr_path, heartbeat_at,
                    exit_code, term_signal, error
             FROM attempt_processes WHERE attempt_id = ?",
        )
        .bind(attempt_id.to_string())
        .fetch_optional(&self.database.pool)
        .await
        .map_err(|source| db("get attempt process", source))?;
        row.map(decode_process).transpose()
    }

    pub async fn request_cancellation(
        &self,
        job_id: JobId,
        grace: Duration,
    ) -> PersistenceResult<JobDetail> {
        let seconds =
            i64::try_from(grace.as_secs()).map_err(|_| PersistenceError::InvalidValue {
                entity: "cancellation grace period",
                value: grace.as_secs().to_string(),
            })?;
        let mut transaction = self
            .database
            .pool
            .begin()
            .await
            .map_err(|source| db("begin cancellation request", source))?;
        let row = sqlx::query("SELECT project_id, state FROM jobs WHERE id = ?")
            .bind(job_id.to_string())
            .fetch_optional(&mut *transaction)
            .await
            .map_err(|source| db("read cancellation job", source))?
            .ok_or(PersistenceError::NotFound { entity: "job" })?;
        let project_id = parse_id(row.get("project_id"), "cancellation project")?;
        match parse_job_state(row.get("state"))? {
            JobState::Running => {
                let updated = sqlx::query(
                    "UPDATE jobs SET
                         cancel_requested_at = COALESCE(cancel_requested_at, strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
                         cancel_grace_seconds = CASE
                             WHEN cancel_grace_seconds IS NULL THEN ?
                             ELSE MIN(cancel_grace_seconds, ?)
                         END,
                         updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
                     WHERE id = ? AND state = 'running'",
                )
                .bind(seconds)
                .bind(seconds)
                .bind(job_id.to_string())
                .execute(&mut *transaction)
                .await
                .map_err(|source| db("request running job cancellation", source))?;
                if updated.rows_affected() != 1 {
                    return Err(PersistenceError::Conflict { entity: "job" });
                }
            }
            JobState::Queued => {
                let attempt = sqlx::query(
                    "UPDATE attempts SET state = 'cancelled',
                         finished_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now'),
                         updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
                     WHERE id = (SELECT id FROM attempts WHERE job_id = ? ORDER BY sequence DESC LIMIT 1)
                         AND state = 'pending'
                     RETURNING id",
                )
                .bind(job_id.to_string())
                .fetch_optional(&mut *transaction)
                .await
                .map_err(|source| db("cancel pending attempt", source))?
                .ok_or(PersistenceError::Conflict { entity: "attempt" })?;
                let attempt_id: AttemptId = parse_id(attempt.get("id"), "cancelled attempt")?;
                let updated = sqlx::query(
                    "UPDATE jobs SET state = 'cancelled',
                         updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
                     WHERE id = ? AND state = 'queued' AND claim_id IS NULL",
                )
                .bind(job_id.to_string())
                .execute(&mut *transaction)
                .await
                .map_err(|source| db("cancel queued job", source))?;
                if updated.rows_affected() != 1 {
                    return Err(PersistenceError::Conflict { entity: "job" });
                }
                let attempt_event = state_event(
                    EventKind::AttemptStateChanged,
                    "attempt",
                    &attempt_id.to_string(),
                    "cancelled",
                    None,
                    None,
                )?;
                insert_event(
                    &mut transaction,
                    project_id,
                    Some(job_id),
                    Some(attempt_id),
                    &attempt_event,
                )
                .await?;
                let job_event = state_event(
                    EventKind::JobStateChanged,
                    "job",
                    &job_id.to_string(),
                    "cancelled",
                    None,
                    None,
                )?;
                insert_event(&mut transaction, project_id, Some(job_id), None, &job_event).await?;
            }
            _ => return Err(PersistenceError::Conflict { entity: "job" }),
        }
        transaction
            .commit()
            .await
            .map_err(|source| db("commit cancellation request", source))?;
        self.detail(job_id)
            .await?
            .ok_or(PersistenceError::NotFound { entity: "job" })
    }

    pub async fn retry(&self, job_id: JobId) -> PersistenceResult<JobDetail> {
        let mut transaction = self
            .database
            .pool
            .begin()
            .await
            .map_err(|source| db("begin job retry", source))?;
        let row = sqlx::query("SELECT project_id, spec_json, state FROM jobs WHERE id = ?")
            .bind(job_id.to_string())
            .fetch_optional(&mut *transaction)
            .await
            .map_err(|source| db("read retry job", source))?
            .ok_or(PersistenceError::NotFound { entity: "job" })?;
        let state = parse_job_state(row.get("state"))?;
        if !matches!(
            state,
            JobState::Failed | JobState::Cancelled | JobState::Lost
        ) {
            return Err(PersistenceError::Conflict { entity: "job" });
        }
        let project_id: ProjectId = parse_id(row.get("project_id"), "retry project")?;
        let job: JobSpec = from_json(row.get("spec_json"), "retry job spec")?;
        let previous_row = sqlx::query(
            "SELECT spec_json FROM attempts WHERE job_id = ? ORDER BY sequence DESC LIMIT 1",
        )
        .bind(job_id.to_string())
        .fetch_one(&mut *transaction)
        .await
        .map_err(|source| db("read previous retry attempt", source))?;
        let previous: AttemptSpec =
            from_json(previous_row.get("spec_json"), "previous attempt spec")?;
        let sequence =
            previous
                .sequence()
                .checked_add(1)
                .ok_or_else(|| PersistenceError::InvalidValue {
                    entity: "attempt sequence",
                    value: previous.sequence().to_string(),
                })?;
        let attempt = AttemptSpec::from_job(
            AttemptId::new(),
            sequence,
            &job,
            previous.source().clone(),
            previous.configuration().clone(),
            previous.result().clone(),
        )?;
        sqlx::query(
            "INSERT INTO attempts (id, job_id, project_id, sequence, state, spec_json)
             VALUES (?, ?, ?, ?, 'pending', ?)",
        )
        .bind(attempt.id().to_string())
        .bind(job_id.to_string())
        .bind(project_id.to_string())
        .bind(i64::from(sequence))
        .bind(json(&attempt, "retry attempt spec")?)
        .execute(&mut *transaction)
        .await
        .map_err(|source| db("insert retry attempt", source))?;
        let updated = sqlx::query(
            "UPDATE jobs SET state = 'queued', cancel_requested_at = NULL,
                 cancel_grace_seconds = NULL,
                 updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
             WHERE id = ? AND state = ? AND claim_id IS NULL",
        )
        .bind(job_id.to_string())
        .bind(state.as_str())
        .execute(&mut *transaction)
        .await
        .map_err(|source| db("queue retried job", source))?;
        if updated.rows_affected() != 1 {
            return Err(PersistenceError::Conflict { entity: "job" });
        }
        let attempt_event = Event::new(
            EventId::new(),
            EventKind::AttemptCreated,
            EventPayload::new(
                EventKind::AttemptCreated,
                1,
                serde_json::json!({
                    "job_id": job_id,
                    "attempt_id": attempt.id(),
                    "sequence": sequence,
                }),
            )?,
        )?;
        insert_event(
            &mut transaction,
            project_id,
            Some(job_id),
            Some(attempt.id()),
            &attempt_event,
        )
        .await?;
        let job_event = state_event(
            EventKind::JobStateChanged,
            "job",
            &job_id.to_string(),
            "queued",
            None,
            Some(serde_json::json!({"retry_attempt_id": attempt.id()})),
        )?;
        insert_event(&mut transaction, project_id, Some(job_id), None, &job_event).await?;
        transaction
            .commit()
            .await
            .map_err(|source| db("commit job retry", source))?;
        self.detail(job_id)
            .await?
            .ok_or(PersistenceError::NotFound { entity: "job" })
    }

    pub async fn logs_for_job(&self, job_id: JobId) -> PersistenceResult<JobLogs> {
        let row = sqlx::query(
            "SELECT jobs.state, attempts.id AS attempt_id,
                    attempt_processes.stdout_path, attempt_processes.stderr_path
             FROM jobs
             JOIN attempts ON attempts.id = (
                 SELECT id FROM attempts AS latest
                 WHERE latest.job_id = jobs.id ORDER BY sequence DESC LIMIT 1
             )
             LEFT JOIN attempt_processes ON attempt_processes.attempt_id = attempts.id
             WHERE jobs.id = ?",
        )
        .bind(job_id.to_string())
        .fetch_optional(&self.database.pool)
        .await
        .map_err(|source| db("read job log paths", source))?
        .ok_or(PersistenceError::NotFound { entity: "job" })?;
        Ok(JobLogs {
            job_id,
            attempt_id: parse_id(row.get("attempt_id"), "log attempt")?,
            state: parse_job_state(row.get("state"))?,
            stdout_path: row
                .get::<Option<String>, _>("stdout_path")
                .map(PathBuf::from),
            stderr_path: row
                .get::<Option<String>, _>("stderr_path")
                .map(PathBuf::from),
        })
    }

    pub async fn insert_attempt_with_event(
        &self,
        attempt: &AttemptSpec,
        event: &Event,
    ) -> PersistenceResult<()> {
        attempt.validate()?;
        require_event_kind(
            event,
            EventKind::AttemptCreated,
            "attempt creation event kind",
        )?;
        let mut transaction = self
            .database
            .pool
            .begin()
            .await
            .map_err(|source| db("begin attempt creation", source))?;
        let row = sqlx::query(
            "INSERT INTO attempts (id, job_id, project_id, sequence, state, spec_json)
             SELECT ?, id, project_id, ?, 'pending', ? FROM jobs WHERE id = ?
             RETURNING project_id",
        )
        .bind(attempt.id().to_string())
        .bind(i64::from(attempt.sequence()))
        .bind(json(attempt, "attempt spec")?)
        .bind(attempt.job_id().to_string())
        .fetch_optional(&mut *transaction)
        .await
        .map_err(|source| db("insert attempt", source))?;
        let row = row.ok_or(PersistenceError::NotFound { entity: "job" })?;
        insert_event(
            &mut transaction,
            parse_id(row.get("project_id"), "attempt project")?,
            Some(attempt.job_id()),
            Some(attempt.id()),
            event,
        )
        .await?;
        transaction
            .commit()
            .await
            .map_err(|source| db("commit attempt creation", source))?;
        Ok(())
    }

    pub async fn get_attempt(&self, id: AttemptId) -> PersistenceResult<Option<StoredAttempt>> {
        let row = sqlx::query("SELECT spec_json, state, created_at, started_at, finished_at, updated_at FROM attempts WHERE id = ?")
            .bind(id.to_string())
            .fetch_optional(&self.database.pool)
            .await
            .map_err(|source| db("get attempt", source))?;
        row.map(decode_attempt).transpose()
    }

    pub async fn transition_job(
        &self,
        job_id: JobId,
        next: JobState,
        event: &Event,
    ) -> PersistenceResult<()> {
        require_event_kind(
            event,
            EventKind::JobStateChanged,
            "job transition event kind",
        )?;
        if next == JobState::Running {
            return Err(PersistenceError::InvalidLease(
                "running jobs must be entered through claim_queued",
            ));
        }
        let mut transaction = self
            .database
            .pool
            .begin()
            .await
            .map_err(|source| db("begin job transition", source))?;
        let row = sqlx::query("SELECT project_id, state FROM jobs WHERE id = ?")
            .bind(job_id.to_string())
            .fetch_optional(&mut *transaction)
            .await
            .map_err(|source| db("read job transition state", source))?
            .ok_or(PersistenceError::NotFound { entity: "job" })?;
        let current = parse_job_state(row.get("state"))?;
        if current == JobState::Running {
            return Err(PersistenceError::InvalidLease(
                "running jobs require a matching claim lease transition",
            ));
        }
        current.transition_to(next)?;
        let updated = sqlx::query("UPDATE jobs SET state = ?, updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now') WHERE id = ? AND state = ? AND claim_id IS NULL")
            .bind(next.as_str())
            .bind(job_id.to_string())
            .bind(current.as_str())
            .execute(&mut *transaction)
            .await
            .map_err(|source| db("update job transition state", source))?;
        if updated.rows_affected() != 1 {
            return Err(PersistenceError::Conflict { entity: "job" });
        }
        insert_event(
            &mut transaction,
            parse_id(row.get("project_id"), "job project")?,
            Some(job_id),
            None,
            event,
        )
        .await?;
        transaction
            .commit()
            .await
            .map_err(|source| db("commit job transition", source))?;
        Ok(())
    }

    pub async fn transition_claimed_job(
        &self,
        job_id: JobId,
        lease_id: Uuid,
        owner: &str,
        next: JobState,
        event: &Event,
    ) -> PersistenceResult<()> {
        require_event_kind(
            event,
            EventKind::JobStateChanged,
            "job transition event kind",
        )?;
        if owner.trim().is_empty() {
            return Err(PersistenceError::InvalidLease("owner must not be empty"));
        }
        JobState::Running.transition_to(next)?;
        let mut transaction = self
            .database
            .pool
            .begin()
            .await
            .map_err(|source| db("begin claimed job transition", source))?;
        let row = sqlx::query(
            "UPDATE jobs SET state = ?, claim_id = NULL, claim_owner = NULL,
                 claim_expires_at = NULL, updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
             WHERE id = ? AND state = 'running' AND claim_id = ? AND claim_owner = ?
                 AND claim_expires_at > strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
             RETURNING project_id",
        )
        .bind(next.as_str())
        .bind(job_id.to_string())
        .bind(lease_id.to_string())
        .bind(owner)
        .fetch_optional(&mut *transaction)
        .await
        .map_err(|source| db("update claimed job transition state", source))?
        .ok_or(PersistenceError::Conflict {
            entity: "job claim",
        })?;
        insert_event(
            &mut transaction,
            parse_id(row.get("project_id"), "job project")?,
            Some(job_id),
            None,
            event,
        )
        .await?;
        transaction
            .commit()
            .await
            .map_err(|source| db("commit claimed job transition", source))?;
        Ok(())
    }

    pub async fn transition_attempt(
        &self,
        attempt_id: AttemptId,
        next: AttemptState,
        event: &Event,
    ) -> PersistenceResult<()> {
        require_event_kind(
            event,
            EventKind::AttemptStateChanged,
            "attempt transition event kind",
        )?;
        let mut transaction = self
            .database
            .pool
            .begin()
            .await
            .map_err(|source| db("begin attempt transition", source))?;
        let row = sqlx::query(
            "SELECT jobs.project_id, attempts.job_id, attempts.state
             FROM attempts JOIN jobs ON jobs.id = attempts.job_id
             WHERE attempts.id = ?",
        )
        .bind(attempt_id.to_string())
        .fetch_optional(&mut *transaction)
        .await
        .map_err(|source| db("read attempt transition state", source))?
        .ok_or(PersistenceError::NotFound { entity: "attempt" })?;
        let current = parse_attempt_state(row.get("state"))?;
        current.transition_to(next)?;
        let updated = sqlx::query(
            "UPDATE attempts SET state = ?, updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
             WHERE id = ? AND state = ?",
        )
        .bind(next.as_str())
        .bind(attempt_id.to_string())
        .bind(current.as_str())
        .execute(&mut *transaction)
        .await
        .map_err(|source| db("update attempt transition state", source))?;
        if updated.rows_affected() != 1 {
            return Err(PersistenceError::Conflict { entity: "attempt" });
        }
        insert_event(
            &mut transaction,
            parse_id(row.get("project_id"), "attempt project")?,
            Some(parse_id(row.get("job_id"), "attempt job")?),
            Some(attempt_id),
            event,
        )
        .await?;
        transaction
            .commit()
            .await
            .map_err(|source| db("commit attempt transition", source))?;
        Ok(())
    }
}

fn validate_execution_outcome(outcome: &ExecutionOutcome) -> PersistenceResult<()> {
    if !matches!(
        outcome.state,
        AttemptState::Succeeded
            | AttemptState::Failed
            | AttemptState::Cancelled
            | AttemptState::Lost
    ) {
        return Err(PersistenceError::InvalidValue {
            entity: "execution outcome",
            value: outcome.state.as_str().into(),
        });
    }
    if outcome.exit_code.is_some() && outcome.term_signal.is_some() {
        return Err(PersistenceError::InvalidValue {
            entity: "execution outcome",
            value: "exit code and terminating signal are mutually exclusive".into(),
        });
    }
    if outcome.state == AttemptState::Succeeded
        && (outcome.exit_code != Some(0)
            || outcome.term_signal.is_some()
            || outcome.error.is_some())
    {
        return Err(PersistenceError::InvalidValue {
            entity: "execution outcome",
            value: "successful execution must have exit code zero and no signal or error".into(),
        });
    }
    Ok(())
}

async fn finish_execution_transaction(
    transaction: &mut Transaction<'_, Sqlite>,
    claim: &ExecutionClaim,
    outcome: &ExecutionOutcome,
    details: Value,
) -> PersistenceResult<()> {
    let job_state = match outcome.state {
        AttemptState::Succeeded => JobState::Succeeded,
        AttemptState::Cancelled => JobState::Cancelled,
        AttemptState::Lost => JobState::Lost,
        _ => JobState::Failed,
    };
    let attempt_id = claim.attempt.spec.id();
    let job_id = claim.job.spec.id;
    let project_id = claim.job.spec.project_id;
    let attempt = sqlx::query(
        "UPDATE attempts SET state = ?,
             finished_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now'),
             updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
         WHERE id = ? AND state IN ('starting', 'running')",
    )
    .bind(outcome.state.as_str())
    .bind(attempt_id.to_string())
    .execute(&mut **transaction)
    .await
    .map_err(|source| db("finish execution attempt", source))?;
    if attempt.rows_affected() != 1 {
        return Err(PersistenceError::Conflict {
            entity: "execution attempt",
        });
    }
    let job = sqlx::query(
        "UPDATE jobs SET state = ?, claim_id = NULL, claim_owner = NULL,
             claim_expires_at = NULL, cancel_requested_at = NULL,
             cancel_grace_seconds = NULL,
             updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
         WHERE id = ? AND state = 'running' AND claim_id = ? AND claim_owner = ?",
    )
    .bind(job_state.as_str())
    .bind(job_id.to_string())
    .bind(claim.lease_id.to_string())
    .bind(&claim.owner)
    .execute(&mut **transaction)
    .await
    .map_err(|source| db("finish execution job", source))?;
    if job.rows_affected() != 1 {
        return Err(PersistenceError::Conflict {
            entity: "execution claim",
        });
    }
    release_claim_resources(transaction, claim).await?;
    let attempt_event = state_event(
        EventKind::AttemptStateChanged,
        "attempt",
        &attempt_id.to_string(),
        outcome.state.as_str(),
        Some(claim.lease_id),
        Some(details.clone()),
    )?;
    insert_event(
        transaction,
        project_id,
        Some(job_id),
        Some(attempt_id),
        &attempt_event,
    )
    .await?;
    let job_event = state_event(
        EventKind::JobStateChanged,
        "job",
        &job_id.to_string(),
        job_state.as_str(),
        Some(claim.lease_id),
        Some(details),
    )?;
    insert_event(transaction, project_id, Some(job_id), None, &job_event).await?;
    Ok(())
}

fn decode_job(row: sqlx::sqlite::SqliteRow) -> PersistenceResult<StoredJob> {
    Ok(StoredJob {
        spec: from_json(row.get("spec_json"), "job spec")?,
        state: parse_job_state(row.get("state"))?,
        priority: row.get("priority"),
        submission_order: row.get("submission_order"),
        submitted_at: row.get("submitted_at"),
        updated_at: row.get("updated_at"),
    })
}

fn decode_attempt(row: sqlx::sqlite::SqliteRow) -> PersistenceResult<StoredAttempt> {
    Ok(StoredAttempt {
        spec: from_json(row.get("spec_json"), "attempt spec")?,
        state: parse_attempt_state(row.get("state"))?,
        created_at: row.get("created_at"),
        started_at: row.get("started_at"),
        finished_at: row.get("finished_at"),
        updated_at: row.get("updated_at"),
    })
}

fn decode_process(row: sqlx::sqlite::SqliteRow) -> PersistenceResult<ProcessRecord> {
    Ok(ProcessRecord {
        attempt_id: parse_id(row.get("attempt_id"), "process attempt")?,
        job_id: parse_id(row.get("job_id"), "process job")?,
        project_id: parse_id(row.get("project_id"), "process project")?,
        pid: row.get("pid"),
        process_group_id: row.get("process_group_id"),
        process_start_ticks: row.get("process_start_ticks"),
        stdout_path: PathBuf::from(row.get::<String, _>("stdout_path")),
        stderr_path: PathBuf::from(row.get::<String, _>("stderr_path")),
        heartbeat_at: row.get("heartbeat_at"),
        exit_code: row.get("exit_code"),
        term_signal: row.get("term_signal"),
        error: row.get("error"),
    })
}

fn decode_container(row: sqlx::sqlite::SqliteRow) -> PersistenceResult<ContainerRecord> {
    Ok(ContainerRecord {
        attempt_id: parse_id(row.get("attempt_id"), "container attempt")?,
        job_id: parse_id(row.get("job_id"), "container job")?,
        project_id: parse_id(row.get("project_id"), "container project")?,
        container_id: row.get("container_id"),
        container_name: row.get("container_name"),
        image_reference: row.get("image_reference"),
        image_id: row.get("image_id"),
        state: parse_container_state(row.get("state"))?,
        docker_status: row.get("docker_status"),
        stdout_path: PathBuf::from(row.get::<String, _>("stdout_path")),
        stderr_path: PathBuf::from(row.get::<String, _>("stderr_path")),
        exit_code: row.get("exit_code"),
        oom_killed: row.get("oom_killed"),
        error: row.get("error"),
        started_at: row.get("started_at"),
        finished_at: row.get("finished_at"),
        removed_at: row.get("removed_at"),
        created_at: row.get("created_at"),
        updated_at: row.get("updated_at"),
    })
}

fn parse_container_state(value: &str) -> PersistenceResult<DockerContainerState> {
    match value {
        "created" => Ok(DockerContainerState::Created),
        "running" => Ok(DockerContainerState::Running),
        "exited" => Ok(DockerContainerState::Exited),
        "removed" => Ok(DockerContainerState::Removed),
        "lost" => Ok(DockerContainerState::Lost),
        value => Err(PersistenceError::InvalidValue {
            entity: "container state",
            value: value.into(),
        }),
    }
}

fn decode_project(row: sqlx::sqlite::SqliteRow) -> PersistenceResult<Project> {
    Ok(Project {
        id: parse_id(row.get("id"), "project")?,
        name: row.get("name"),
        root: PathBuf::from(row.get::<String, _>("root_path")),
        config_path: PathBuf::from(row.get::<String, _>("config_path")),
    })
}

#[derive(Clone, Copy)]
pub struct EventRepository<'a> {
    database: &'a Database,
}

impl EventRepository<'_> {
    pub async fn append(
        &self,
        project_id: ProjectId,
        job_id: Option<JobId>,
        attempt_id: Option<AttemptId>,
        event: &Event,
    ) -> PersistenceResult<i64> {
        let result = sqlx::query("INSERT INTO events (id, project_id, job_id, attempt_id, kind, payload_json) VALUES (?, ?, ?, ?, ?, ?)")
            .bind(event.id.to_string())
            .bind(project_id.to_string())
            .bind(job_id.map(|id| id.to_string()))
            .bind(attempt_id.map(|id| id.to_string()))
            .bind(event.kind.as_str())
            .bind(json(&event.payload, "event payload")?)
            .execute(&self.database.pool)
            .await
            .map_err(|source| db("append event", source))?;
        Ok(result.last_insert_rowid())
    }

    pub async fn for_job(&self, job_id: JobId) -> PersistenceResult<Vec<StoredEvent>> {
        let rows = sqlx::query("SELECT sequence, id, project_id, job_id, attempt_id, kind, payload_json, occurred_at FROM events WHERE job_id = ? ORDER BY sequence")
            .bind(job_id.to_string())
            .fetch_all(&self.database.pool)
            .await
            .map_err(|source| db("read job event stream", source))?;
        rows.into_iter().map(decode_event).collect()
    }
}

async fn insert_event(
    transaction: &mut Transaction<'_, Sqlite>,
    project_id: ProjectId,
    job_id: Option<JobId>,
    attempt_id: Option<AttemptId>,
    event: &Event,
) -> PersistenceResult<()> {
    sqlx::query("INSERT INTO events (id, project_id, job_id, attempt_id, kind, payload_json) VALUES (?, ?, ?, ?, ?, ?)")
        .bind(event.id.to_string())
        .bind(project_id.to_string())
        .bind(job_id.map(|id| id.to_string()))
        .bind(attempt_id.map(|id| id.to_string()))
        .bind(event.kind.as_str())
        .bind(json(&event.payload, "event payload")?)
        .execute(&mut **transaction)
        .await
        .map_err(|source| db("append transition event", source))?;
    Ok(())
}

fn decode_event(row: sqlx::sqlite::SqliteRow) -> PersistenceResult<StoredEvent> {
    let kind = parse_event_kind(row.get("kind"))?;
    let payload: EventPayload = from_json(row.get("payload_json"), "event payload")?;
    Ok(StoredEvent {
        sequence: row.get("sequence"),
        project_id: parse_id(row.get("project_id"), "event project")?,
        job_id: row
            .get::<Option<String>, _>("job_id")
            .map(|value| parse_id(&value, "event job"))
            .transpose()?,
        attempt_id: row
            .get::<Option<String>, _>("attempt_id")
            .map(|value| parse_id(&value, "event attempt"))
            .transpose()?,
        event: Event::new(parse_id(row.get("id"), "event")?, kind, payload)?,
        occurred_at: row.get("occurred_at"),
    })
}

#[derive(Clone, Copy)]
pub struct ActionRepository<'a> {
    database: &'a Database,
}

impl ActionRepository<'_> {
    pub async fn insert(&self, action: &ActionRecord) -> PersistenceResult<()> {
        if action.state != ActionState::Pending {
            return Err(PersistenceError::InvalidValue {
                entity: "new action state",
                value: action.state.as_str().into(),
            });
        }
        sqlx::query("INSERT INTO actions (id, project_id, kind, state, spec_json, idempotency_key, available_at) VALUES (?, ?, ?, ?, ?, ?, strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))")
            .bind(action.id.to_string()).bind(action.project_id.to_string()).bind(&action.kind)
            .bind(action.state.as_str()).bind(json(&action.spec, "action spec")?).bind(&action.idempotency_key)
            .execute(&self.database.pool).await.map_err(|source| db("insert action", source))?;
        Ok(())
    }

    pub async fn claim_pending(
        &self,
        owner: &str,
        duration: Duration,
    ) -> PersistenceResult<Option<Claim<ActionId>>> {
        claim_outbox(
            &self.database.pool,
            "actions",
            "running",
            EventKind::ActionStateChanged,
            owner,
            duration,
        )
        .await?
        .map(|claim| {
            Ok(Claim {
                record_id: parse_id(&claim.record_id, "claimed action")?,
                lease_id: claim.lease_id,
                owner: claim.owner,
                expires_at: claim.expires_at,
            })
        })
        .transpose()
    }
}

#[derive(Clone, Copy)]
pub struct DeliveryRepository<'a> {
    database: &'a Database,
}

impl DeliveryRepository<'_> {
    pub async fn insert(
        &self,
        delivery: &DeliveryRecord,
        event_id: Option<EventId>,
    ) -> PersistenceResult<()> {
        if delivery.state != DeliveryState::Pending {
            return Err(PersistenceError::InvalidValue {
                entity: "new delivery state",
                value: delivery.state.as_str().into(),
            });
        }
        sqlx::query("INSERT INTO deliveries (id, project_id, event_id, channel, state, payload_json, idempotency_key, available_at) VALUES (?, ?, ?, ?, ?, ?, ?, strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))")
            .bind(delivery.id.to_string()).bind(delivery.project_id.to_string()).bind(event_id.map(|id| id.to_string()))
            .bind(&delivery.channel).bind(delivery.state.as_str()).bind(json(&delivery.payload, "delivery payload")?)
            .bind(&delivery.idempotency_key).execute(&self.database.pool).await.map_err(|source| db("insert delivery", source))?;
        Ok(())
    }

    pub async fn claim_pending(
        &self,
        owner: &str,
        duration: Duration,
    ) -> PersistenceResult<Option<Claim<DeliveryId>>> {
        claim_outbox(
            &self.database.pool,
            "deliveries",
            "delivering",
            EventKind::DeliveryStateChanged,
            owner,
            duration,
        )
        .await?
        .map(|claim| {
            Ok(Claim {
                record_id: parse_id(&claim.record_id, "claimed delivery")?,
                lease_id: claim.lease_id,
                owner: claim.owner,
                expires_at: claim.expires_at,
            })
        })
        .transpose()
    }
}

#[derive(Debug)]
struct StringClaim {
    record_id: String,
    lease_id: Uuid,
    owner: String,
    expires_at: String,
}

async fn claim_outbox(
    pool: &SqlitePool,
    table: &'static str,
    claimed_state: &'static str,
    event_kind: EventKind,
    owner: &str,
    duration: Duration,
) -> PersistenceResult<Option<StringClaim>> {
    let seconds = validate_lease(owner, duration)?;
    let lease_id = Uuid::new_v4();
    let mut transaction = pool
        .begin()
        .await
        .map_err(|source| db("begin outbox claim", source))?;
    let query = format!(
        "WITH candidate AS (
            SELECT id FROM {table}
            WHERE (state = 'pending' AND available_at <= strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
               OR (state = '{claimed_state}' AND claim_expires_at <= strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
            ORDER BY available_at, created_at LIMIT 1
         )
         UPDATE {table} SET state = '{claimed_state}', claim_id = ?, claim_owner = ?,
             claim_expires_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now', '+' || ? || ' seconds'),
             attempts = attempts + 1, updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
         WHERE id = (SELECT id FROM candidate)
         RETURNING id, project_id, claim_expires_at"
    );
    let row = sqlx::query(&query)
        .bind(lease_id.to_string())
        .bind(owner)
        .bind(seconds)
        .fetch_optional(&mut *transaction)
        .await
        .map_err(|source| db("claim pending outbox record", source))?;
    let claim = if let Some(row) = row {
        let record_id: String = row.get("id");
        let event = claim_event(
            event_kind,
            lease_id,
            owner,
            claimed_state,
            match table {
                "actions" => "action",
                "deliveries" => "delivery",
                _ => table,
            },
            &record_id,
        )?;
        insert_event(
            &mut transaction,
            parse_id(row.get("project_id"), "outbox project")?,
            None,
            None,
            &event,
        )
        .await?;
        Some(StringClaim {
            record_id,
            lease_id,
            owner: owner.into(),
            expires_at: row.get("claim_expires_at"),
        })
    } else {
        None
    };
    transaction
        .commit()
        .await
        .map_err(|source| db("commit outbox claim", source))?;
    Ok(claim)
}

fn claim_event(
    kind: EventKind,
    lease_id: Uuid,
    owner: &str,
    state: &str,
    entity: &str,
    record_id: &str,
) -> PersistenceResult<Event> {
    let payload = EventPayload::new(
        kind,
        1,
        serde_json::json!({
            "entity": entity,
            "record_id": record_id,
            "state": state,
            "lease_id": lease_id,
            "owner": owner,
        }),
    )?;
    Ok(Event::new(EventId::new(), kind, payload)?)
}

fn state_event(
    kind: EventKind,
    entity: &str,
    record_id: &str,
    state: &str,
    lease_id: Option<Uuid>,
    details: Option<Value>,
) -> PersistenceResult<Event> {
    let payload = EventPayload::new(
        kind,
        1,
        serde_json::json!({
            "entity": entity,
            "record_id": record_id,
            "state": state,
            "lease_id": lease_id,
            "details": details,
        }),
    )?;
    Ok(Event::new(EventId::new(), kind, payload)?)
}

#[derive(Clone, Copy)]
pub struct ArtifactRepository<'a> {
    database: &'a Database,
}

impl ArtifactRepository<'_> {
    pub async fn insert(&self, artifact: &ArtifactRecord) -> PersistenceResult<()> {
        let (role, role_detail) = encode_artifact_role(&artifact.role);
        sqlx::query(
            "INSERT INTO artifacts
                 (id, attempt_id, path, role, role_detail, sha256, metadata_json)
             VALUES (?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(artifact.id.to_string())
        .bind(artifact.attempt_id.to_string())
        .bind(&artifact.path)
        .bind(role)
        .bind(role_detail)
        .bind(&artifact.sha256)
        .bind(json(&artifact.metadata, "artifact metadata")?)
        .execute(&self.database.pool)
        .await
        .map_err(|source| db("insert artifact", source))?;
        Ok(())
    }

    pub async fn get(&self, id: Uuid) -> PersistenceResult<Option<ArtifactRecord>> {
        let row = sqlx::query(
            "SELECT id, attempt_id, path, role, role_detail, sha256, metadata_json
             FROM artifacts WHERE id = ?",
        )
        .bind(id.to_string())
        .fetch_optional(&self.database.pool)
        .await
        .map_err(|source| db("get artifact", source))?;
        row.map(|row| {
            Ok(ArtifactRecord {
                id: parse_id(row.get("id"), "artifact")?,
                attempt_id: parse_id(row.get("attempt_id"), "artifact attempt")?,
                path: row.get("path"),
                role: decode_artifact_role(row.get("role"), row.get("role_detail"))?,
                sha256: row.get("sha256"),
                metadata: from_json(row.get("metadata_json"), "artifact metadata")?,
            })
        })
        .transpose()
    }
}

fn encode_artifact_role(role: &ArtifactRole) -> (&'static str, Option<&str>) {
    match role {
        ArtifactRole::Metrics => ("metrics", None),
        ArtifactRole::Model => ("model", None),
        ArtifactRole::Checkpoint => ("checkpoint", None),
        ArtifactRole::Figure => ("figure", None),
        ArtifactRole::Log => ("log", None),
        ArtifactRole::CrashDump => ("crash_dump", None),
        ArtifactRole::Report => ("report", None),
        ArtifactRole::Other(detail) => ("other", Some(detail)),
    }
}

fn decode_artifact_role(role: &str, detail: Option<String>) -> PersistenceResult<ArtifactRole> {
    match (role, detail) {
        ("metrics", None) => Ok(ArtifactRole::Metrics),
        ("model", None) => Ok(ArtifactRole::Model),
        ("checkpoint", None) => Ok(ArtifactRole::Checkpoint),
        ("figure", None) => Ok(ArtifactRole::Figure),
        ("log", None) => Ok(ArtifactRole::Log),
        ("crash_dump", None) => Ok(ArtifactRole::CrashDump),
        ("report", None) => Ok(ArtifactRole::Report),
        ("other", Some(detail)) => Ok(ArtifactRole::Other(detail)),
        (role, detail) => Err(PersistenceError::InvalidValue {
            entity: "artifact role",
            value: format!("{role} ({detail:?})"),
        }),
    }
}

#[derive(Clone, Copy)]
pub struct ResourceRepository<'a> {
    database: &'a Database,
}

impl ResourceRepository<'_> {
    pub async fn synchronize_inventory(
        &self,
        inventory: &crate::HostInventory,
    ) -> PersistenceResult<()> {
        let cpu_capacity = i64::from(inventory.cpu_threads);
        let memory_capacity =
            i64::try_from(inventory.memory_bytes).map_err(|_| PersistenceError::InvalidValue {
                entity: "host memory capacity",
                value: inventory.memory_bytes.to_string(),
            })?;
        let mut resources = vec![
            Resource {
                id: ResourceId::new(),
                name: "host".into(),
                kind: "host".into(),
                capacity: i64::from(inventory.max_concurrent_jobs),
                metadata: serde_json::json!({
                    "managed_by": "igor",
                    "available": true,
                    "max_concurrent_jobs": inventory.max_concurrent_jobs,
                }),
            },
            Resource {
                id: ResourceId::new(),
                name: "cpu".into(),
                kind: "cpu".into(),
                capacity: cpu_capacity,
                metadata: serde_json::json!({
                    "managed_by": "igor",
                    "available": true,
                    "detected_threads": inventory.detected_cpu_threads,
                }),
            },
            Resource {
                id: ResourceId::new(),
                name: "memory".into(),
                kind: "memory".into(),
                capacity: memory_capacity,
                metadata: serde_json::json!({
                    "managed_by": "igor",
                    "available": true,
                    "detected_bytes": inventory.detected_memory_bytes,
                }),
            },
        ];
        resources.extend(inventory.gpus.iter().map(|gpu| Resource {
            id: ResourceId::new(),
            name: format!("gpu:{}", gpu.identity),
            kind: "gpu".into(),
            capacity: 1,
            metadata: serde_json::json!({
                "managed_by": "igor",
                "available": true,
                "device": gpu.identity,
                "display_name": gpu.display_name,
            }),
        }));
        resources.extend(inventory.named_resources.iter().map(|name| Resource {
            id: ResourceId::new(),
            name: format!("named:{name}"),
            kind: "named".into(),
            capacity: 1,
            metadata: serde_json::json!({
                "managed_by": "igor",
                "available": true,
                "resource_name": name,
            }),
        }));

        let mut transaction = self
            .database
            .pool
            .begin()
            .await
            .map_err(|source| db("begin resource inventory synchronization", source))?;
        for resource in &resources {
            sqlx::query(
                "INSERT INTO resources (id, name, kind, capacity, metadata_json)
                 VALUES (?, ?, ?, ?, ?)
                 ON CONFLICT(name) DO UPDATE SET kind = excluded.kind,
                     capacity = CASE WHEN EXISTS (
                         SELECT 1 FROM resource_leases
                         WHERE resource_id = resources.id
                     ) THEN resources.capacity ELSE excluded.capacity END,
                     metadata_json = excluded.metadata_json",
            )
            .bind(resource.id.to_string())
            .bind(&resource.name)
            .bind(&resource.kind)
            .bind(resource.capacity)
            .bind(json(&resource.metadata, "resource metadata")?)
            .execute(&mut *transaction)
            .await
            .map_err(|source| db("upsert discovered resource", source))?;
        }
        let managed: Vec<(String, String)> = sqlx::query_as(
            "SELECT name, kind FROM resources
             WHERE json_extract(metadata_json, '$.managed_by') = 'igor'",
        )
        .fetch_all(&mut *transaction)
        .await
        .map_err(|source| db("list managed resources", source))?;
        for (name, kind) in managed {
            if resources.iter().any(|resource| resource.name == name) {
                continue;
            }
            if kind == "gpu" && !inventory.gpu_inventory_authoritative {
                mark_resource_unavailable(&mut transaction, &name).await?;
                continue;
            }
            let deleted = sqlx::query(
                "DELETE FROM resources WHERE name = ? AND NOT EXISTS (
                     SELECT 1 FROM resource_leases WHERE resource_id = resources.id
                 )",
            )
            .bind(&name)
            .execute(&mut *transaction)
            .await
            .map_err(|source| db("remove stale managed resource", source))?;
            if deleted.rows_affected() == 0 {
                mark_resource_unavailable(&mut transaction, &name).await?;
            }
        }
        transaction
            .commit()
            .await
            .map_err(|source| db("commit resource inventory synchronization", source))?;
        Ok(())
    }

    pub async fn status(&self) -> PersistenceResult<Vec<ResourceStatus>> {
        let rows = sqlx::query(
            "SELECT id, name, kind, capacity, metadata_json
             FROM resources ORDER BY kind, name",
        )
        .fetch_all(&self.database.pool)
        .await
        .map_err(|source| db("list resources", source))?;
        let mut status = rows
            .into_iter()
            .map(|row| {
                Ok(ResourceStatus {
                    resource: Resource {
                        id: parse_id(row.get("id"), "resource")?,
                        name: row.get("name"),
                        kind: row.get("kind"),
                        capacity: row.get("capacity"),
                        metadata: from_json(row.get("metadata_json"), "resource metadata")?,
                    },
                    leases: Vec::new(),
                })
            })
            .collect::<PersistenceResult<Vec<_>>>()?;
        let leases = sqlx::query(
            "SELECT id, resource_id, job_id, owner, quantity, heartbeat_at, expires_at
             FROM resource_leases ORDER BY created_at, id",
        )
        .fetch_all(&self.database.pool)
        .await
        .map_err(|source| db("list resource leases", source))?;
        for row in leases {
            let resource_id = parse_id(row.get("resource_id"), "resource lease resource")?;
            let lease = ResourceLease {
                id: parse_id(row.get("id"), "resource lease")?,
                resource_id,
                job_id: parse_id(row.get("job_id"), "resource lease job")?,
                owner: row.get("owner"),
                quantity: row.get("quantity"),
                heartbeat_at: row.get("heartbeat_at"),
                expires_at: row.get("expires_at"),
            };
            if let Some(resource) = status
                .iter_mut()
                .find(|entry| entry.resource.id == resource_id)
            {
                resource.leases.push(lease);
            }
        }
        Ok(status)
    }

    pub async fn insert(&self, resource: &Resource) -> PersistenceResult<()> {
        sqlx::query("INSERT INTO resources (id, name, kind, capacity, metadata_json) VALUES (?, ?, ?, ?, ?)")
            .bind(resource.id.to_string()).bind(&resource.name).bind(&resource.kind).bind(resource.capacity)
            .bind(json(&resource.metadata, "resource metadata")?).execute(&self.database.pool)
            .await.map_err(|source| db("insert resource", source))?;
        Ok(())
    }

    pub async fn get(&self, id: ResourceId) -> PersistenceResult<Option<Resource>> {
        let row = sqlx::query(
            "SELECT id, name, kind, capacity, metadata_json FROM resources WHERE id = ?",
        )
        .bind(id.to_string())
        .fetch_optional(&self.database.pool)
        .await
        .map_err(|source| db("get resource", source))?;
        row.map(|row| {
            Ok(Resource {
                id: parse_id(row.get("id"), "resource")?,
                name: row.get("name"),
                kind: row.get("kind"),
                capacity: row.get("capacity"),
                metadata: from_json(row.get("metadata_json"), "resource metadata")?,
            })
        })
        .transpose()
    }

    pub async fn acquire(
        &self,
        resource_id: ResourceId,
        job_id: JobId,
        owner: &str,
        quantity: i64,
        duration: Duration,
    ) -> PersistenceResult<Option<ResourceLease>> {
        let seconds = validate_lease(owner, duration)?;
        if quantity <= 0 {
            return Err(PersistenceError::InvalidLease("quantity must be positive"));
        }
        let lease_id = Uuid::new_v4();
        let mut transaction = self
            .database
            .pool
            .begin()
            .await
            .map_err(|source| db("begin resource lease claim", source))?;
        release_reclaimable_resource_leases(&mut transaction).await?;
        let row = sqlx::query(
            "INSERT INTO resource_leases (id, resource_id, job_id, owner, quantity, expires_at)
             SELECT ?, id, ?, ?, ?, strftime('%Y-%m-%dT%H:%M:%fZ', 'now', '+' || ? || ' seconds')
             FROM resources
             WHERE id = ? AND capacity >= ? + COALESCE(
                 (SELECT SUM(quantity) FROM resource_leases WHERE resource_id = ?), 0
             ) AND NOT EXISTS (
                 SELECT 1 FROM resource_leases WHERE resource_id = ? AND job_id = ?
             )
             RETURNING heartbeat_at, expires_at",
        )
        .bind(lease_id.to_string())
        .bind(job_id.to_string())
        .bind(owner)
        .bind(quantity)
        .bind(seconds)
        .bind(resource_id.to_string())
        .bind(quantity)
        .bind(resource_id.to_string())
        .bind(resource_id.to_string())
        .bind(job_id.to_string())
        .fetch_optional(&mut *transaction)
        .await
        .map_err(|source| db("acquire resource lease", source))?;
        let lease = if let Some(row) = row {
            let project_id: String = sqlx::query_scalar("SELECT project_id FROM jobs WHERE id = ?")
                .bind(job_id.to_string())
                .fetch_one(&mut *transaction)
                .await
                .map_err(|source| db("read resource lease project", source))?;
            let event = claim_event(
                EventKind::ResourceReserved,
                lease_id,
                owner,
                "reserved",
                "resource_lease",
                &lease_id.to_string(),
            )?;
            insert_event(
                &mut transaction,
                parse_id(&project_id, "resource lease project")?,
                Some(job_id),
                None,
                &event,
            )
            .await?;
            Some(ResourceLease {
                id: lease_id,
                resource_id,
                job_id,
                owner: owner.into(),
                quantity,
                heartbeat_at: row.get("heartbeat_at"),
                expires_at: row.get("expires_at"),
            })
        } else {
            None
        };
        transaction
            .commit()
            .await
            .map_err(|source| db("commit resource lease claim", source))?;
        Ok(lease)
    }

    pub async fn release(&self, lease_id: Uuid, owner: &str) -> PersistenceResult<bool> {
        let mut transaction = self
            .database
            .pool
            .begin()
            .await
            .map_err(|source| db("begin resource lease release", source))?;
        let row = sqlx::query(
            "DELETE FROM resource_leases WHERE id = ? AND owner = ? RETURNING resource_id, job_id",
        )
        .bind(lease_id.to_string())
        .bind(owner)
        .fetch_optional(&mut *transaction)
        .await
        .map_err(|source| db("release resource lease", source))?;
        if let Some(row) = &row {
            let job_id: JobId = parse_id(row.get("job_id"), "released resource lease job")?;
            let project_id: String = sqlx::query_scalar("SELECT project_id FROM jobs WHERE id = ?")
                .bind(job_id.to_string())
                .fetch_one(&mut *transaction)
                .await
                .map_err(|source| db("read released resource lease project", source))?;
            let event = claim_event(
                EventKind::ResourceReleased,
                lease_id,
                owner,
                "released",
                "resource_lease",
                &lease_id.to_string(),
            )?;
            insert_event(
                &mut transaction,
                parse_id(&project_id, "released resource lease project")?,
                Some(job_id),
                None,
                &event,
            )
            .await?;
        }
        transaction
            .commit()
            .await
            .map_err(|source| db("commit resource lease release", source))?;
        Ok(row.is_some())
    }
}

async fn mark_resource_unavailable(
    transaction: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    name: &str,
) -> PersistenceResult<()> {
    sqlx::query(
        "UPDATE resources SET metadata_json = json_set(
             metadata_json, '$.available', json('false')
         ) WHERE name = ?",
    )
    .bind(name)
    .execute(&mut **transaction)
    .await
    .map_err(|source| db("mark stale managed resource unavailable", source))?;
    Ok(())
}

fn parse_job_state(value: &str) -> PersistenceResult<JobState> {
    match value {
        "queued" => Ok(JobState::Queued),
        "running" => Ok(JobState::Running),
        "succeeded" => Ok(JobState::Succeeded),
        "failed" => Ok(JobState::Failed),
        "cancelled" => Ok(JobState::Cancelled),
        "lost" => Ok(JobState::Lost),
        "superseded" => Ok(JobState::Superseded),
        value => Err(PersistenceError::InvalidValue {
            entity: "job state",
            value: value.into(),
        }),
    }
}

fn parse_attempt_state(value: &str) -> PersistenceResult<AttemptState> {
    match value {
        "pending" => Ok(AttemptState::Pending),
        "starting" => Ok(AttemptState::Starting),
        "running" => Ok(AttemptState::Running),
        "succeeded" => Ok(AttemptState::Succeeded),
        "failed" => Ok(AttemptState::Failed),
        "cancelled" => Ok(AttemptState::Cancelled),
        "lost" => Ok(AttemptState::Lost),
        value => Err(PersistenceError::InvalidValue {
            entity: "attempt state",
            value: value.into(),
        }),
    }
}

fn parse_event_kind(value: &str) -> PersistenceResult<EventKind> {
    match value {
        "job_submitted" => Ok(EventKind::JobSubmitted),
        "job_state_changed" => Ok(EventKind::JobStateChanged),
        "attempt_created" => Ok(EventKind::AttemptCreated),
        "attempt_state_changed" => Ok(EventKind::AttemptStateChanged),
        "action_state_changed" => Ok(EventKind::ActionStateChanged),
        "delivery_state_changed" => Ok(EventKind::DeliveryStateChanged),
        "recovery_state_changed" => Ok(EventKind::RecoveryStateChanged),
        "report_state_changed" => Ok(EventKind::ReportStateChanged),
        "cleanup_state_changed" => Ok(EventKind::CleanupStateChanged),
        "resource_reserved" => Ok(EventKind::ResourceReserved),
        "resource_released" => Ok(EventKind::ResourceReleased),
        "artifact_registered" => Ok(EventKind::ArtifactRegistered),
        value => Err(PersistenceError::InvalidValue {
            entity: "event kind",
            value: value.into(),
        }),
    }
}
