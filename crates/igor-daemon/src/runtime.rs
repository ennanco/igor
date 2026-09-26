use std::{
    fs::{self, File, OpenOptions},
    future::Future,
    io,
    os::unix::{
        fs::{FileTypeExt, MetadataExt, OpenOptionsExt, PermissionsExt},
        net::{UnixListener as StdUnixListener, UnixStream as StdUnixStream},
    },
    path::{Path, PathBuf},
    time::Duration,
};

use fs2::FileExt;
use igor_core::{
    ConfigError, Database, DatabaseOptions, GlobalConfig, HostConfig, IntegrityCheck,
    InventoryError, PersistenceError, RuntimePaths, build_submission, discover_host_inventory,
    load_global_config, load_project_config,
};
use serde_json::Value;
use thiserror::Error;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{UnixListener, UnixStream},
    sync::watch,
    task::JoinSet,
    time::timeout,
};

use crate::protocol::{
    DaemonRole, DatabaseStatus, Health, PROTOCOL_VERSION, ProtocolError, ProtocolErrorKind,
    Request, RequestEnvelope, Response, ResponseEnvelope, Version,
};

const MAX_FRAME_BYTES: usize = 1024 * 1024;
const IO_TIMEOUT: Duration = Duration::from_secs(60);
const SHUTDOWN_GRACE: Duration = Duration::from_secs(2);
const MAX_DATABASE_CONNECTIONS: u32 = 32;

#[derive(Debug, Error)]
pub enum DaemonError {
    #[error("{role} daemon is already running at {path}")]
    AlreadyRunning { role: DaemonRole, path: PathBuf },
    #[error("insecure runtime path {path}: {reason}")]
    InsecureRuntime { path: PathBuf, reason: String },
    #[error("cannot {operation} {path}: {source}")]
    Io {
        operation: &'static str,
        path: PathBuf,
        #[source]
        source: io::Error,
    },
    #[error("database startup failed: {0}")]
    Database(#[from] igor_core::PersistenceError),
    #[error("database initialization task failed: {0}")]
    DatabaseInitialization(String),
    #[error("resource inventory configuration failed: {0}")]
    Config(#[from] ConfigError),
    #[error("resource discovery failed: {0}")]
    Inventory(#[from] InventoryError),
    #[error("resource discovery task failed: {0}")]
    InventoryTask(String),
}

#[derive(Debug, Error)]
pub enum ClientError {
    #[error("{role} daemon is unavailable at {path}: {source}")]
    Unavailable {
        role: DaemonRole,
        path: PathBuf,
        #[source]
        source: io::Error,
    },
    #[error("{role} daemon timed out at {path}")]
    Timeout { role: DaemonRole, path: PathBuf },
    #[error("daemon protocol mismatch: found version {found}, supported version is {supported}")]
    ProtocolMismatch { found: u32, supported: u32 },
    #[error("daemon rejected request ({code}): {message}")]
    InvalidRequest { code: String, message: String },
    #[error("daemon database is unavailable ({code}): {message}")]
    DatabaseUnavailable { code: String, message: String },
    #[error("daemon request failed ({code}): {message}")]
    Internal { code: String, message: String },
    #[error("daemon object was not found ({code}): {message}")]
    NotFound { code: String, message: String },
    #[error("daemon operation conflicts with existing state ({code}): {message}")]
    Conflict { code: String, message: String },
    #[error("invalid daemon response: {0}")]
    InvalidResponse(String),
}

#[derive(Clone, Debug)]
pub struct Client {
    worker_socket: PathBuf,
    supervisor_socket: PathBuf,
}

impl Client {
    #[must_use]
    pub fn new(paths: &RuntimePaths) -> Self {
        Self {
            worker_socket: paths.worker_socket.clone(),
            supervisor_socket: paths.supervisor_socket.clone(),
        }
    }

    pub async fn request(
        &self,
        role: DaemonRole,
        request: Request,
    ) -> Result<Response, ClientError> {
        let path = match role {
            DaemonRole::Worker => &self.worker_socket,
            DaemonRole::Supervisor => &self.supervisor_socket,
        };
        let exchange = async {
            let mut stream =
                UnixStream::connect(path)
                    .await
                    .map_err(|source| ClientError::Unavailable {
                        role,
                        path: path.clone(),
                        source,
                    })?;
            let mut encoded = serde_json::to_vec(&RequestEnvelope::new(request.clone()))
                .map_err(|error| ClientError::InvalidResponse(error.to_string()))?;
            encoded.push(b'\n');
            stream
                .write_all(&encoded)
                .await
                .map_err(|source| ClientError::Unavailable {
                    role,
                    path: path.clone(),
                    source,
                })?;
            let frame =
                read_frame(&mut stream)
                    .await
                    .map_err(|source| ClientError::Unavailable {
                        role,
                        path: path.clone(),
                        source,
                    })?;
            let response = decode_response(&frame)?;
            validate_response(role, request, response)
        };
        timeout(IO_TIMEOUT, exchange)
            .await
            .map_err(|_| ClientError::Timeout {
                role,
                path: path.clone(),
            })?
    }
}

pub async fn run(role: DaemonRole, paths: &RuntimePaths) -> Result<(), DaemonError> {
    run_until(role, paths, shutdown_signal()).await
}

pub async fn run_until<F>(
    role: DaemonRole,
    paths: &RuntimePaths,
    shutdown: F,
) -> Result<(), DaemonError>
where
    F: Future<Output = ()>,
{
    let bound = BoundSocket::bind(role, paths)?;
    let listener = bound.listener;
    let _cleanup = bound.cleanup;
    let host = if role == DaemonRole::Worker {
        Some(if paths.config_file.is_file() {
            load_global_config(&paths.config_file)?.host
        } else {
            GlobalConfig::default().host
        })
    } else {
        None
    };
    let worker_count = host.as_ref().map_or(0, |host| host.max_concurrent_jobs);
    let database = open_database(paths, worker_count).await?;
    if let Some(host) = &host {
        synchronize_host_inventory(&database, host).await?;
    }
    let mut requests = JoinSet::new();
    let (worker_shutdown, worker_receiver) = watch::channel(false);
    let mut workers = JoinSet::new();
    for _ in 0..worker_count {
        let receiver = worker_receiver.clone();
        workers.spawn(crate::worker::run(
            database.clone(),
            paths.clone(),
            receiver,
        ));
    }
    if role == DaemonRole::Supervisor {
        workers.spawn(crate::supervisor::run(
            database.clone(),
            paths.clone(),
            worker_receiver.clone(),
        ));
    }
    drop(worker_receiver);
    tokio::pin!(shutdown);

    loop {
        tokio::select! {
            biased;
            () = &mut shutdown => break,
            completed = requests.join_next(), if !requests.is_empty() => {
                if let Some(Err(error)) = completed {
                    tracing::warn!(%role, %error, "daemon request task failed");
                }
            }
            accepted = listener.accept() => {
                let (stream, _) = accepted.map_err(|source| daemon_io(
                    "accept connection on",
                    role.socket_path(paths),
                    source,
                ))?;
                if peer_is_current_user(&stream)? {
                    let database = database.clone();
                    requests.spawn(async move {
                        match timeout(IO_TIMEOUT, serve_connection(stream, role, database)).await {
                            Ok(Ok(())) => {}
                            Ok(Err(error)) => {
                                tracing::warn!(%role, %error, "daemon request failed");
                            }
                            Err(error) => {
                                tracing::warn!(%role, %error, "daemon request timed out");
                            }
                        }
                    });
                } else {
                    tracing::warn!(%role, "rejected Unix socket peer with a different user ID");
                }
            }
        }
    }
    drop(listener);

    if timeout(SHUTDOWN_GRACE, drain_requests(&mut requests))
        .await
        .is_err()
    {
        requests.abort_all();
        while requests.join_next().await.is_some() {}
    }
    let _ = worker_shutdown.send(true);
    if timeout(SHUTDOWN_GRACE, drain_workers(&mut workers))
        .await
        .is_err()
    {
        tracing::warn!("worker execution loops did not stop before shutdown");
        workers.abort_all();
        while workers.join_next().await.is_some() {}
    }
    database.pool().close().await;
    drop(_cleanup);
    Ok(())
}

async fn synchronize_host_inventory(
    database: &Database,
    host: &HostConfig,
) -> Result<(), DaemonError> {
    let inventory = tokio::task::spawn_blocking({
        let host = host.clone();
        move || discover_host_inventory(&host)
    })
    .await
    .map_err(|error| DaemonError::InventoryTask(error.to_string()))??;
    database
        .resources()
        .synchronize_inventory(&inventory)
        .await?;
    Ok(())
}

async fn open_database(paths: &RuntimePaths, worker_count: u32) -> Result<Database, DaemonError> {
    let database_path = paths.database.clone();
    let file_name = database_path
        .file_name()
        .map(|name| name.to_string_lossy().into_owned())
        .ok_or_else(|| insecure(&database_path, "database path has no filename"))?;
    let lock_path = database_path.with_file_name(format!(".{file_name}.init.lock"));
    let lock = tokio::task::spawn_blocking(move || -> Result<File, DaemonError> {
        if let Some(parent) = lock_path.parent() {
            fs::create_dir_all(parent)
                .map_err(|source| daemon_io("create database directory", parent, source))?;
        }
        let lock = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .mode(0o600)
            .open(&lock_path)
            .map_err(|source| daemon_io("open database initialization lock", &lock_path, source))?;
        FileExt::lock_exclusive(&lock).map_err(|source| {
            daemon_io("acquire database initialization lock", &lock_path, source)
        })?;
        Ok(lock)
    })
    .await
    .map_err(|error| DaemonError::DatabaseInitialization(error.to_string()))??;
    let max_connections = worker_count
        .saturating_add(3)
        .clamp(4, MAX_DATABASE_CONNECTIONS);
    let database = Database::open_with_options(
        &paths.database,
        DatabaseOptions {
            max_connections,
            ..DatabaseOptions::default()
        },
    )
    .await?;
    drop(lock);
    Ok(database)
}

async fn drain_requests(requests: &mut JoinSet<()>) {
    while let Some(result) = requests.join_next().await {
        if let Err(error) = result {
            tracing::warn!(%error, "daemon request task failed during shutdown");
        }
    }
}

async fn drain_workers(workers: &mut JoinSet<()>) {
    while let Some(result) = workers.join_next().await {
        if let Err(error) = result {
            tracing::warn!(%error, "worker execution loop failed during shutdown");
        }
    }
}

async fn shutdown_signal() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};

        let terminate = signal(SignalKind::terminate());
        match terminate {
            Ok(mut terminate) => {
                tokio::select! {
                    result = tokio::signal::ctrl_c() => {
                        if let Err(error) = result {
                            tracing::error!(%error, "failed to listen for Ctrl-C");
                        }
                    }
                    _ = terminate.recv() => {}
                }
            }
            Err(error) => {
                tracing::error!(%error, "failed to listen for SIGTERM");
                let _ = tokio::signal::ctrl_c().await;
            }
        }
    }
}

async fn serve_connection(
    mut stream: UnixStream,
    role: DaemonRole,
    database: Database,
) -> io::Result<()> {
    let response = match read_frame(&mut stream).await {
        Ok(frame) => match decode_request(&frame) {
            Ok(request) => handle_request(role, &database, request).await,
            Err(error) => ResponseEnvelope::failure(error),
        },
        Err(error) if error.kind() == io::ErrorKind::FileTooLarge => {
            ResponseEnvelope::failure(ProtocolError::frame_too_large())
        }
        Err(error) => ResponseEnvelope::failure(ProtocolError::invalid_request(error.to_string())),
    };
    let mut encoded = serde_json::to_vec(&response).map_err(io::Error::other)?;
    encoded.push(b'\n');
    stream.write_all(&encoded).await
}

async fn handle_request(
    role: DaemonRole,
    database: &Database,
    request: Request,
) -> ResponseEnvelope {
    match request {
        Request::Health => ResponseEnvelope::success(Response::Health(Health {
            role,
            healthy: true,
            pid: std::process::id(),
        })),
        Request::Version => ResponseEnvelope::success(Response::Version(Version {
            role,
            igor: env!("CARGO_PKG_VERSION").into(),
            protocol: PROTOCOL_VERSION,
        })),
        Request::DatabaseStatus => {
            let status = async {
                let schema_version = database.schema_version().await?;
                database.integrity_check(IntegrityCheck::Quick).await?;
                Ok::<_, igor_core::PersistenceError>(schema_version)
            }
            .await;
            match status {
                Ok(schema_version) => {
                    ResponseEnvelope::success(Response::DatabaseStatus(DatabaseStatus {
                        role,
                        schema_version,
                        integrity: "ok".into(),
                    }))
                }
                Err(error) => {
                    tracing::error!(%role, %error, "database status request failed");
                    ResponseEnvelope::failure(ProtocolError::database_unavailable())
                }
            }
        }
        _request if role != DaemonRole::Worker => ResponseEnvelope::failure(
            ProtocolError::invalid_request("operational requests must be sent to the worker"),
        ),
        Request::Resources => match database.resources().status().await {
            Ok(resources) => ResponseEnvelope::success(Response::Resources { resources }),
            Err(error) => persistence_response(error),
        },
        Request::ProjectRegister { project } => match normalize_project(project) {
            Ok(project) => match database.projects().register(&project).await {
                Ok(project) => ResponseEnvelope::success(Response::Project(project)),
                Err(error) => persistence_response(error),
            },
            Err(error) => ResponseEnvelope::failure(ProtocolError::invalid_request(error)),
        },
        Request::ProjectList => match database.projects().list().await {
            Ok(projects) => ResponseEnvelope::success(Response::Projects { projects }),
            Err(error) => persistence_response(error),
        },
        Request::ProjectRemove { root } => match normalize_existing_path(&root) {
            Ok(root) => match database.projects().remove(&root).await {
                Ok(project) => ResponseEnvelope::success(Response::Project(project)),
                Err(error) => persistence_response(error),
            },
            Err(error) => ResponseEnvelope::failure(ProtocolError::invalid_request(error)),
        },
        Request::ProjectByRoot { root } => match database.projects().by_root(&root).await {
            Ok(project) => ResponseEnvelope::success(Response::OptionalProject { project }),
            Err(error) => persistence_response(error),
        },
        Request::Submit { project_id, input } => {
            let project = match database.projects().get(project_id).await {
                Ok(Some(project)) => project,
                Ok(None) => return ResponseEnvelope::failure(ProtocolError::not_found("project")),
                Err(error) => return persistence_response(error),
            };
            let built = tokio::task::spawn_blocking(move || {
                let config =
                    load_project_config(&project.config_path).map_err(|error| error.to_string())?;
                build_submission(&project, &config.config, *input)
                    .map_err(|error| error.to_string())
            })
            .await;
            let submission = match built {
                Ok(Ok(submission)) => submission,
                Ok(Err(error)) => {
                    return ResponseEnvelope::failure(ProtocolError::invalid_request(error));
                }
                Err(error) => {
                    tracing::error!(%error, "submission builder task failed");
                    return ResponseEnvelope::failure(ProtocolError::internal());
                }
            };
            match database
                .jobs()
                .submit(
                    &submission.job,
                    &submission.attempt,
                    submission.priority,
                    &submission.job_event,
                    &submission.attempt_event,
                )
                .await
            {
                Ok(job) => ResponseEnvelope::success(Response::Submitted(job)),
                Err(error) => persistence_response(error),
            }
        }
        Request::JobList { project_id } => match database.jobs().list(project_id).await {
            Ok(jobs) => ResponseEnvelope::success(Response::Jobs { jobs }),
            Err(error) => persistence_response(error),
        },
        Request::JobShow { job_id } => match database.jobs().detail(job_id).await {
            Ok(Some(job)) => ResponseEnvelope::success(Response::Job(job)),
            Ok(None) => ResponseEnvelope::failure(ProtocolError::not_found("job")),
            Err(error) => persistence_response(error),
        },
        Request::JobEvents { job_id } => match database.jobs().get_job(job_id).await {
            Ok(Some(_)) => match database.events().for_job(job_id).await {
                Ok(events) => ResponseEnvelope::success(Response::Events { events }),
                Err(error) => persistence_response(error),
            },
            Ok(None) => ResponseEnvelope::failure(ProtocolError::not_found("job")),
            Err(error) => persistence_response(error),
        },
        Request::JobCancel {
            job_id,
            grace_seconds,
        } => match database
            .jobs()
            .request_cancellation(job_id, Duration::from_secs(u64::from(grace_seconds)))
            .await
        {
            Ok(job) => ResponseEnvelope::success(Response::Cancelled(job)),
            Err(error) => persistence_response(error),
        },
        Request::JobRetry { job_id } => match database.jobs().retry(job_id).await {
            Ok(job) => ResponseEnvelope::success(Response::Retried(job)),
            Err(error) => persistence_response(error),
        },
        Request::JobLogs { job_id } => match database.jobs().logs_for_job(job_id).await {
            Ok(logs) => ResponseEnvelope::success(Response::Logs(logs)),
            Err(error) => persistence_response(error),
        },
    }
}

fn normalize_project(mut project: igor_core::Project) -> Result<igor_core::Project, String> {
    project.root = project
        .root
        .canonicalize()
        .map_err(|error| format!("cannot canonicalize project root: {error}"))?;
    let expected = project.root.join(".igor/project.toml");
    for path in [project.root.join(".igor"), expected.clone()] {
        let metadata = fs::symlink_metadata(&path)
            .map_err(|error| format!("cannot inspect {}: {error}", path.display()))?;
        if metadata.file_type().is_symlink() {
            return Err(format!("{} must not be a symbolic link", path.display()));
        }
    }
    project.config_path = project
        .config_path
        .canonicalize()
        .map_err(|error| format!("cannot canonicalize project configuration: {error}"))?;
    let expected = expected
        .canonicalize()
        .map_err(|error| format!("cannot canonicalize expected project configuration: {error}"))?;
    if project.config_path != expected {
        return Err("project configuration must be PROJECT_ROOT/.igor/project.toml".into());
    }
    load_project_config(&project.config_path).map_err(|error| error.to_string())?;
    Ok(project)
}

fn normalize_existing_path(path: &Path) -> Result<PathBuf, String> {
    path.canonicalize()
        .map_err(|error| format!("cannot canonicalize project root: {error}"))
}

fn persistence_response(error: PersistenceError) -> ResponseEnvelope {
    let protocol = match error {
        PersistenceError::NotFound { entity } => ProtocolError::not_found(entity),
        PersistenceError::Conflict { entity } => ProtocolError::conflict(entity),
        PersistenceError::Domain(error) => ProtocolError::invalid_request(error.to_string()),
        PersistenceError::InvalidValue { entity, value } => {
            ProtocolError::invalid_request(format!("invalid {entity}: {value}"))
        }
        error => {
            tracing::error!(%error, "daemon persistence request failed");
            ProtocolError::internal()
        }
    };
    ResponseEnvelope::failure(protocol)
}

fn decode_request(frame: &[u8]) -> Result<Request, ProtocolError> {
    let value: Value = serde_json::from_slice(frame)
        .map_err(|error| ProtocolError::invalid_request(error.to_string()))?;
    let version = value
        .get("protocol_version")
        .and_then(Value::as_u64)
        .and_then(|version| u32::try_from(version).ok())
        .ok_or_else(|| {
            ProtocolError::invalid_request("protocol_version must be an unsigned integer")
        })?;
    if version != PROTOCOL_VERSION {
        return Err(ProtocolError::incompatible(version));
    }
    let envelope: RequestEnvelope = serde_json::from_value(value)
        .map_err(|error| ProtocolError::invalid_request(error.to_string()))?;
    Ok(envelope.request)
}

fn decode_response(frame: &[u8]) -> Result<Response, ClientError> {
    let value: Value = serde_json::from_slice(frame)
        .map_err(|error| ClientError::InvalidResponse(error.to_string()))?;
    let version = value
        .get("protocol_version")
        .and_then(Value::as_u64)
        .and_then(|version| u32::try_from(version).ok())
        .ok_or_else(|| ClientError::InvalidResponse("missing protocol_version".into()))?;
    if version != PROTOCOL_VERSION {
        return Err(ClientError::ProtocolMismatch {
            found: version,
            supported: PROTOCOL_VERSION,
        });
    }
    let envelope: ResponseEnvelope = serde_json::from_value(value)
        .map_err(|error| ClientError::InvalidResponse(error.to_string()))?;
    match (envelope.response, envelope.error) {
        (Some(response), None) => Ok(response),
        (None, Some(error)) => Err(client_protocol_error(error)),
        _ => Err(ClientError::InvalidResponse(
            "response must contain exactly one of response or error".into(),
        )),
    }
}

fn client_protocol_error(error: ProtocolError) -> ClientError {
    match error.kind {
        ProtocolErrorKind::IncompatibleProtocol => ClientError::ProtocolMismatch {
            found: error.found_version.unwrap_or_default(),
            supported: error.supported_version.unwrap_or(PROTOCOL_VERSION),
        },
        ProtocolErrorKind::DatabaseUnavailable => ClientError::DatabaseUnavailable {
            code: error.code,
            message: error.message,
        },
        ProtocolErrorKind::InvalidRequest | ProtocolErrorKind::FrameTooLarge => {
            ClientError::InvalidRequest {
                code: error.code,
                message: error.message,
            }
        }
        ProtocolErrorKind::Internal => ClientError::Internal {
            code: error.code,
            message: error.message,
        },
        ProtocolErrorKind::NotFound => ClientError::NotFound {
            code: error.code,
            message: error.message,
        },
        ProtocolErrorKind::Conflict => ClientError::Conflict {
            code: error.code,
            message: error.message,
        },
    }
}

fn validate_response(
    role: DaemonRole,
    request: Request,
    response: Response,
) -> Result<Response, ClientError> {
    let valid = match (&request, &response) {
        (Request::Health, Response::Health(value)) => value.role == role,
        (Request::Version, Response::Version(value)) => value.role == role,
        (Request::DatabaseStatus, Response::DatabaseStatus(value)) => value.role == role,
        (Request::Resources, Response::Resources { .. }) => role == DaemonRole::Worker,
        (Request::ProjectRegister { .. }, Response::Project(_))
        | (Request::ProjectRemove { .. }, Response::Project(_))
        | (Request::ProjectList, Response::Projects { .. })
        | (Request::ProjectByRoot { .. }, Response::OptionalProject { .. })
        | (Request::Submit { .. }, Response::Submitted(_))
        | (Request::JobList { .. }, Response::Jobs { .. })
        | (Request::JobShow { .. }, Response::Job(_))
        | (Request::JobEvents { .. }, Response::Events { .. })
        | (Request::JobCancel { .. }, Response::Cancelled(_))
        | (Request::JobRetry { .. }, Response::Retried(_))
        | (Request::JobLogs { .. }, Response::Logs(_)) => role == DaemonRole::Worker,
        _ => false,
    };
    if valid {
        Ok(response)
    } else {
        Err(ClientError::InvalidResponse(format!(
            "{role} returned {response:?} for {request:?}"
        )))
    }
}

async fn read_frame(stream: &mut UnixStream) -> io::Result<Vec<u8>> {
    let mut frame = Vec::new();
    let mut chunk = [0_u8; 1024];
    loop {
        let read = stream.read(&mut chunk).await?;
        if read == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "connection closed before a complete frame",
            ));
        }
        frame.extend_from_slice(&chunk[..read]);
        if frame.len() > MAX_FRAME_BYTES {
            return Err(io::Error::new(
                io::ErrorKind::FileTooLarge,
                "protocol frame is too large",
            ));
        }
        if let Some(end) = frame.iter().position(|byte| *byte == b'\n') {
            if frame[end + 1..]
                .iter()
                .any(|byte| !byte.is_ascii_whitespace())
            {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "multiple protocol frames are not allowed",
                ));
            }
            frame.truncate(end);
            return Ok(frame);
        }
    }
}

struct BoundSocket {
    listener: UnixListener,
    cleanup: SocketCleanup,
}

impl BoundSocket {
    fn bind(role: DaemonRole, paths: &RuntimePaths) -> Result<Self, DaemonError> {
        let uid = current_uid()?;
        secure_runtime_directory(&paths.runtime_dir, uid)?;
        let socket_path = role.socket_path(paths);
        let lock_path = paths.runtime_dir.join(format!("{}.lock", role.as_str()));
        let lock = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .mode(0o600)
            .open(&lock_path)
            .map_err(|source| daemon_io("open lock file", &lock_path, source))?;
        let lock_metadata = fs::symlink_metadata(&lock_path)
            .map_err(|source| daemon_io("inspect lock file", &lock_path, source))?;
        if !lock_metadata.file_type().is_file()
            || lock_metadata.file_type().is_symlink()
            || lock_metadata.uid() != uid
        {
            return Err(insecure(
                &lock_path,
                "lock must be a regular file owned by the current user",
            ));
        }
        fs::set_permissions(&lock_path, fs::Permissions::from_mode(0o600))
            .map_err(|source| daemon_io("secure lock file", &lock_path, source))?;
        FileExt::try_lock_exclusive(&lock).map_err(|source| {
            if source.kind() == io::ErrorKind::WouldBlock {
                DaemonError::AlreadyRunning {
                    role,
                    path: socket_path.to_path_buf(),
                }
            } else {
                daemon_io("lock", &lock_path, source)
            }
        })?;
        remove_stale_socket(role, socket_path, uid)?;
        let listener = StdUnixListener::bind(socket_path)
            .map_err(|source| daemon_io("bind socket", socket_path, source))?;
        fs::set_permissions(socket_path, fs::Permissions::from_mode(0o600))
            .map_err(|source| daemon_io("secure socket", socket_path, source))?;
        let metadata = fs::symlink_metadata(socket_path)
            .map_err(|source| daemon_io("inspect socket", socket_path, source))?;
        listener
            .set_nonblocking(true)
            .map_err(|source| daemon_io("configure socket", socket_path, source))?;
        let listener = UnixListener::from_std(listener)
            .map_err(|source| daemon_io("register socket", socket_path, source))?;
        Ok(Self {
            listener,
            cleanup: SocketCleanup {
                path: socket_path.to_path_buf(),
                device: metadata.dev(),
                inode: metadata.ino(),
                _lock: lock,
            },
        })
    }
}

struct SocketCleanup {
    path: PathBuf,
    device: u64,
    inode: u64,
    _lock: File,
}

impl Drop for SocketCleanup {
    fn drop(&mut self) {
        if let Ok(metadata) = fs::symlink_metadata(&self.path)
            && metadata.dev() == self.device
            && metadata.ino() == self.inode
        {
            let _ = fs::remove_file(&self.path);
        }
    }
}

fn current_uid() -> Result<u32, DaemonError> {
    fs::metadata("/proc/self")
        .map(|metadata| metadata.uid())
        .map_err(|source| daemon_io("inspect", Path::new("/proc/self"), source))
}

fn secure_runtime_directory(path: &Path, uid: u32) -> Result<(), DaemonError> {
    fs::create_dir_all(path)
        .map_err(|source| daemon_io("create runtime directory", path, source))?;
    let metadata = fs::symlink_metadata(path)
        .map_err(|source| daemon_io("inspect runtime directory", path, source))?;
    if !metadata.file_type().is_dir() || metadata.file_type().is_symlink() {
        return Err(insecure(path, "must be a directory, not a symlink"));
    }
    if metadata.uid() != uid {
        return Err(insecure(path, "is not owned by the current user"));
    }
    fs::set_permissions(path, fs::Permissions::from_mode(0o700))
        .map_err(|source| daemon_io("secure runtime directory", path, source))
}

fn remove_stale_socket(role: DaemonRole, path: &Path, uid: u32) -> Result<(), DaemonError> {
    let metadata = match fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(()),
        Err(source) => return Err(daemon_io("inspect socket", path, source)),
    };
    if !metadata.file_type().is_socket() || metadata.uid() != uid {
        return Err(insecure(
            path,
            "existing path is not a Unix socket owned by the current user",
        ));
    }
    match StdUnixStream::connect(path) {
        Ok(_) => {
            return Err(DaemonError::AlreadyRunning {
                role,
                path: path.to_path_buf(),
            });
        }
        Err(error) if error.kind() == io::ErrorKind::ConnectionRefused => {}
        Err(source) => return Err(daemon_io("verify stale socket", path, source)),
    }
    let current = fs::symlink_metadata(path)
        .map_err(|source| daemon_io("recheck stale socket", path, source))?;
    if current.dev() != metadata.dev() || current.ino() != metadata.ino() {
        return Err(insecure(path, "socket changed while checking its owner"));
    }
    fs::remove_file(path).map_err(|source| daemon_io("remove stale socket", path, source))
}

fn peer_is_current_user(stream: &UnixStream) -> Result<bool, DaemonError> {
    let peer = stream.peer_cred().map_err(|source| {
        daemon_io(
            "inspect socket peer for",
            Path::new("accepted connection"),
            source,
        )
    })?;
    Ok(peer.uid() == current_uid()?)
}

fn daemon_io(operation: &'static str, path: &Path, source: io::Error) -> DaemonError {
    DaemonError::Io {
        operation,
        path: path.to_path_buf(),
        source,
    }
}

fn insecure(path: &Path, reason: impl Into<String>) -> DaemonError {
    DaemonError::InsecureRuntime {
        path: path.to_path_buf(),
        reason: reason.into(),
    }
}

#[cfg(test)]
mod tests {
    use std::os::unix::fs::symlink;

    use igor_core::{Project, ProjectId, initialize_project};

    use super::*;

    #[test]
    fn project_registration_rejects_symlinked_configuration()
    -> Result<(), Box<dyn std::error::Error>> {
        let temporary = tempfile::tempdir()?;
        let root = temporary.path().join("project");
        initialize_project(&root, false)?;
        let external = temporary.path().join("external.toml");
        fs::write(&external, "schema_version = 1\n")?;
        let config_path = root.join(".igor/project.toml");
        fs::remove_file(&config_path)?;
        symlink(&external, &config_path)?;
        let result = normalize_project(Project {
            id: ProjectId::new(),
            name: "project".into(),
            root,
            config_path,
        });
        let Err(error) = result else {
            return Err(std::io::Error::other("symlinked configuration was accepted").into());
        };
        assert!(error.contains("symbolic link"));
        Ok(())
    }
}
