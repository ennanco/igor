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
    DeliveryState, DomainError, Event, EventId, EventKind, EventPayload, FamilyId, GenerationId,
    GenerationIdentity, JobId, JobSpec, JobState, Project, ProjectId, ResourceId, TransitionState,
};

static MIGRATOR: Migrator = sqlx::migrate!();
const DEFAULT_BUSY_TIMEOUT: Duration = Duration::from_secs(2);
const MAX_LEASE_SECONDS: u64 = 24 * 60 * 60;

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
pub struct ExecutionOutcome {
    pub state: AttemptState,
    pub exit_code: Option<i32>,
    pub term_signal: Option<i32>,
    pub error: Option<String>,
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

#[derive(Clone, Debug, PartialEq)]
pub struct Resource {
    pub id: ResourceId,
    pub name: String,
    pub kind: String,
    pub capacity: i64,
    pub metadata: Value,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ResourceLease {
    pub id: Uuid,
    pub resource_id: ResourceId,
    pub job_id: JobId,
    pub owner: String,
    pub quantity: i64,
    pub expires_at: String,
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
        transaction
            .commit()
            .await
            .map_err(|source| db("commit project registration", source))?;
        Ok(registered)
    }

    pub async fn by_root(&self, root: &Path) -> PersistenceResult<Option<Project>> {
        let row = sqlx::query(
            "SELECT id, name, root_path, config_path FROM projects WHERE root_path = ? AND is_alias = 0",
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
             WHERE is_alias = 0
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
        let row = sqlx::query("SELECT id, name, root_path, config_path FROM projects WHERE id = ?")
            .bind(id.to_string())
            .fetch_optional(&self.database.pool)
            .await
            .map_err(|source| db("get project", source))?;
        row.map(decode_project).transpose()
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
                ORDER BY priority DESC, submission_order ASC LIMIT 1
             )
             UPDATE jobs SET state = 'running', claim_id = ?, claim_owner = ?,
                 claim_expires_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now', '+' || ? || ' seconds'),
                 updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
             WHERE id = (SELECT id FROM candidate)
             RETURNING id, project_id, claim_expires_at",
        )
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
        let job_row = sqlx::query(
            "WITH candidate AS (
                SELECT jobs.id FROM jobs
                JOIN attempts ON attempts.job_id = jobs.id AND attempts.sequence = 1
                WHERE jobs.state = 'queued' AND attempts.state = 'pending'
                    AND NOT EXISTS (SELECT 1 FROM jobs AS active WHERE active.state = 'running')
                ORDER BY jobs.priority DESC, jobs.submission_order ASC LIMIT 1
             )
             UPDATE jobs SET state = 'running', claim_id = ?, claim_owner = ?,
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
        .map_err(|source| db("claim execution job", source))?;
        let Some(job_row) = job_row else {
            transaction
                .commit()
                .await
                .map_err(|source| db("commit empty execution claim", source))?;
            return Ok(None);
        };
        let job_id: JobId = parse_id(job_row.get("id"), "claimed execution job")?;
        let project_id: ProjectId =
            parse_id(job_row.get("project_id"), "claimed execution project")?;
        let expires_at = job_row.get("claim_expires_at");
        let job = decode_job(job_row)?;
        let attempt_row = sqlx::query(
            "UPDATE attempts SET state = 'starting',
                 updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
             WHERE job_id = ? AND sequence = 1 AND state = 'pending'
             RETURNING id, spec_json, state, created_at, started_at, finished_at, updated_at",
        )
        .bind(job_id.to_string())
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
        }))
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
        let attempt_id = claim.attempt.spec.id();
        let job_id = claim.job.spec.id;
        let project_id = claim.job.spec.project_id;
        let inserted = sqlx::query(
            "INSERT INTO attempt_processes
                 (attempt_id, job_id, project_id, pid, process_group_id, process_start_ticks,
                  stdout_path, stderr_path)
             SELECT ?, ?, ?, ?, ?, ?, ?, ?
             WHERE EXISTS (
                 SELECT 1 FROM jobs WHERE id = ? AND state = 'running'
                     AND claim_id = ? AND claim_owner = ?
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
             WHERE id = ? AND state = 'running' AND claim_id = ? AND claim_owner = ?",
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
        let process = sqlx::query(
            "UPDATE attempt_processes SET
                 heartbeat_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now'),
                 updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
             WHERE attempt_id = ?",
        )
        .bind(claim.attempt.spec.id().to_string())
        .execute(&mut *transaction)
        .await
        .map_err(|source| db("heartbeat attempt process", source))?;
        if process.rows_affected() != 1 {
            return Err(PersistenceError::Conflict {
                entity: "attempt process",
            });
        }
        transaction
            .commit()
            .await
            .map_err(|source| db("commit execution heartbeat", source))?;
        Ok(())
    }

    pub async fn finish_execution(
        &self,
        claim: &ExecutionClaim,
        outcome: &ExecutionOutcome,
    ) -> PersistenceResult<()> {
        if !matches!(
            outcome.state,
            AttemptState::Succeeded | AttemptState::Failed
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
                value: "successful execution must have exit code zero and no signal or error"
                    .into(),
            });
        }
        let job_state = if outcome.state == AttemptState::Succeeded {
            JobState::Succeeded
        } else {
            JobState::Failed
        };
        let attempt_id = claim.attempt.spec.id();
        let job_id = claim.job.spec.id;
        let project_id = claim.job.spec.project_id;
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
        let attempt = sqlx::query(
            "UPDATE attempts SET state = ?,
                 finished_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now'),
                 updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
             WHERE id = ? AND state IN ('starting', 'running')",
        )
        .bind(outcome.state.as_str())
        .bind(attempt_id.to_string())
        .execute(&mut *transaction)
        .await
        .map_err(|source| db("finish execution attempt", source))?;
        if attempt.rows_affected() != 1 {
            return Err(PersistenceError::Conflict {
                entity: "execution attempt",
            });
        }
        let job = sqlx::query(
            "UPDATE jobs SET state = ?, claim_id = NULL, claim_owner = NULL,
                 claim_expires_at = NULL,
                 updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
             WHERE id = ? AND state = 'running' AND claim_id = ? AND claim_owner = ?",
        )
        .bind(job_state.as_str())
        .bind(job_id.to_string())
        .bind(claim.lease_id.to_string())
        .bind(&claim.owner)
        .execute(&mut *transaction)
        .await
        .map_err(|source| db("finish execution job", source))?;
        if job.rows_affected() != 1 {
            return Err(PersistenceError::Conflict {
                entity: "execution claim",
            });
        }
        let details = serde_json::json!({
            "exit_code": outcome.exit_code,
            "term_signal": outcome.term_signal,
            "error": outcome.error,
        });
        let attempt_event = state_event(
            EventKind::AttemptStateChanged,
            "attempt",
            &attempt_id.to_string(),
            outcome.state.as_str(),
            Some(claim.lease_id),
            Some(details.clone()),
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
            job_state.as_str(),
            Some(claim.lease_id),
            Some(details),
        )?;
        insert_event(&mut transaction, project_id, Some(job_id), None, &job_event).await?;
        transaction
            .commit()
            .await
            .map_err(|source| db("commit execution finish", source))?;
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
        sqlx::query(
            "DELETE FROM resource_leases
             WHERE resource_id = ? AND expires_at <= strftime('%Y-%m-%dT%H:%M:%fZ', 'now')",
        )
        .bind(resource_id.to_string())
        .execute(&mut *transaction)
        .await
        .map_err(|source| db("remove expired resource leases", source))?;
        let row = sqlx::query(
            "INSERT INTO resource_leases (id, resource_id, job_id, owner, quantity, expires_at)
             SELECT ?, id, ?, ?, ?, strftime('%Y-%m-%dT%H:%M:%fZ', 'now', '+' || ? || ' seconds')
             FROM resources
             WHERE id = ? AND capacity >= ? + COALESCE(
                 (SELECT SUM(quantity) FROM resource_leases WHERE resource_id = ?), 0
             ) AND NOT EXISTS (
                 SELECT 1 FROM resource_leases WHERE resource_id = ? AND job_id = ?
             )
             RETURNING expires_at",
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
