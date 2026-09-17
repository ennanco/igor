use std::{
    error::Error,
    fs, future,
    os::unix::fs::{MetadataExt, PermissionsExt},
    os::unix::net::UnixListener,
    path::{Path, PathBuf},
    time::Duration,
};

use igor_core::RuntimePaths;
use igor_daemon::{
    Client, ClientError, DaemonError, DaemonRole, ProtocolErrorKind, Request, Response,
    ResponseEnvelope, run_until,
};
use tempfile::TempDir;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::UnixStream,
    sync::oneshot,
    task::JoinHandle,
    time::sleep,
};

fn paths(root: &Path) -> RuntimePaths {
    let runtime_dir = root.join("runtime/igor");
    let state_dir = root.join("state/igor");
    RuntimePaths {
        config_file: root.join("config/igor/config.toml"),
        state_dir: state_dir.clone(),
        runtime_dir: runtime_dir.clone(),
        database: state_dir.join("igor.sqlite3"),
        log_dir: state_dir.join("logs"),
        worktree_dir: state_dir.join("worktrees"),
        report_dir: state_dir.join("reports"),
        worker_socket: runtime_dir.join("worker.sock"),
        supervisor_socket: runtime_dir.join("supervisor.sock"),
        resource_dir: runtime_dir.join("resources"),
    }
}

async fn wait_for(path: &Path) -> Result<(), Box<dyn Error>> {
    for _ in 0..100 {
        if path.exists() {
            return Ok(());
        }
        sleep(Duration::from_millis(10)).await;
    }
    Err(format!("socket did not appear at {}", path.display()).into())
}

async fn wait_for_health(client: &Client, role: DaemonRole) -> Result<(), Box<dyn Error>> {
    for _ in 0..100 {
        if matches!(
            client.request(role, Request::Health).await,
            Ok(Response::Health(_))
        ) {
            return Ok(());
        }
        sleep(Duration::from_millis(10)).await;
    }
    Err(format!("{role} did not become healthy").into())
}

fn spawn_daemon(
    role: DaemonRole,
    paths: RuntimePaths,
) -> (oneshot::Sender<()>, JoinHandle<Result<(), DaemonError>>) {
    let (shutdown, receiver) = oneshot::channel();
    let task = tokio::spawn(async move {
        run_until(role, &paths, async {
            let _ = receiver.await;
        })
        .await
    });
    (shutdown, task)
}

#[tokio::test]
async fn both_roles_serve_health_version_and_database_status() -> Result<(), Box<dyn Error>> {
    let temporary = TempDir::new()?;
    let paths = paths(temporary.path());
    let (stop_worker, worker) = spawn_daemon(DaemonRole::Worker, paths.clone());
    let (stop_supervisor, supervisor) = spawn_daemon(DaemonRole::Supervisor, paths.clone());
    wait_for(&paths.worker_socket).await?;
    wait_for(&paths.supervisor_socket).await?;

    let client = Client::new(&paths);
    for role in [DaemonRole::Worker, DaemonRole::Supervisor] {
        assert!(matches!(
            client.request(role, Request::Health).await?,
            Response::Health(health) if health.role == role && health.healthy
        ));
        assert!(matches!(
            client.request(role, Request::Version).await?,
            Response::Version(version) if version.role == role && version.protocol == 4
        ));
        assert!(matches!(
            client.request(role, Request::DatabaseStatus).await?,
            Response::DatabaseStatus(status)
                if status.role == role && status.schema_version == 6 && status.integrity == "ok"
        ));
    }
    assert!(matches!(
        client.request(DaemonRole::Worker, Request::Resources).await?,
        Response::Resources { resources }
            if resources.iter().any(|status| status.resource.name == "host")
                && resources.iter().any(|status| status.resource.name == "cpu")
                && resources.iter().any(|status| status.resource.name == "memory")
    ));
    assert_eq!(
        fs::metadata(&paths.runtime_dir)?.permissions().mode() & 0o777,
        0o700
    );
    assert_eq!(
        fs::metadata(&paths.worker_socket)?.permissions().mode() & 0o777,
        0o600
    );
    assert_eq!(
        fs::metadata(&paths.supervisor_socket)?.permissions().mode() & 0o777,
        0o600
    );

    let _ = stop_worker.send(());
    let _ = stop_supervisor.send(());
    worker.await??;
    supervisor.await??;
    assert!(!paths.worker_socket.exists());
    assert!(!paths.supervisor_socket.exists());
    Ok(())
}

#[tokio::test]
async fn duplicate_role_cannot_replace_a_live_socket() -> Result<(), Box<dyn Error>> {
    let temporary = TempDir::new()?;
    let paths = paths(temporary.path());
    let (stop, worker) = spawn_daemon(DaemonRole::Worker, paths.clone());
    wait_for(&paths.worker_socket).await?;
    let inode = fs::metadata(&paths.worker_socket)?.ino();

    let error = run_until(DaemonRole::Worker, &paths, future::pending())
        .await
        .err()
        .ok_or("second worker unexpectedly started")?;
    assert!(matches!(error, DaemonError::AlreadyRunning { .. }));
    assert_eq!(fs::metadata(&paths.worker_socket)?.ino(), inode);

    let _ = stop.send(());
    worker.await??;
    Ok(())
}

#[tokio::test]
async fn stale_socket_is_removed_but_regular_file_is_preserved() -> Result<(), Box<dyn Error>> {
    let temporary = TempDir::new()?;
    let paths = paths(temporary.path());
    fs::create_dir_all(&paths.runtime_dir)?;
    let stale = std::os::unix::net::UnixListener::bind(&paths.worker_socket)?;
    drop(stale);
    let (stop, worker) = spawn_daemon(DaemonRole::Worker, paths.clone());
    let client = Client::new(&paths);
    wait_for_health(&client, DaemonRole::Worker).await?;
    assert!(matches!(
        client.request(DaemonRole::Worker, Request::Health).await?,
        Response::Health(_)
    ));
    let _ = stop.send(());
    worker.await??;

    fs::write(&paths.worker_socket, "do not remove")?;
    let error = run_until(DaemonRole::Worker, &paths, future::pending())
        .await
        .err()
        .ok_or("worker replaced a regular file")?;
    assert!(
        matches!(error, DaemonError::InsecureRuntime { .. }),
        "unexpected error: {error:?}"
    );
    assert_eq!(fs::read_to_string(&paths.worker_socket)?, "do not remove");
    Ok(())
}

#[tokio::test]
async fn protocol_mismatch_and_invalid_request_are_distinct() -> Result<(), Box<dyn Error>> {
    let temporary = TempDir::new()?;
    let paths = paths(temporary.path());
    let (stop, worker) = spawn_daemon(DaemonRole::Worker, paths.clone());
    wait_for(&paths.worker_socket).await?;

    let mismatch = raw_request(
        &paths.worker_socket,
        b"{\"protocol_version\":99,\"request\":{\"type\":\"health\"}}\n",
    )
    .await?;
    let mismatch: ResponseEnvelope = serde_json::from_slice(&mismatch)?;
    assert!(matches!(
        mismatch.error,
        Some(error) if error.kind == ProtocolErrorKind::IncompatibleProtocol
    ));

    let invalid = raw_request(&paths.worker_socket, b"{not-json}\n").await?;
    let invalid: ResponseEnvelope = serde_json::from_slice(&invalid)?;
    assert!(matches!(
        invalid.error,
        Some(error) if error.kind == ProtocolErrorKind::InvalidRequest
    ));

    let _ = stop.send(());
    worker.await??;
    Ok(())
}

#[tokio::test]
async fn missing_daemon_is_reported_as_unavailable() -> Result<(), Box<dyn Error>> {
    let temporary = TempDir::new()?;
    let paths = paths(temporary.path());
    let error = Client::new(&paths)
        .request(DaemonRole::Worker, Request::Health)
        .await
        .err()
        .ok_or("missing daemon responded")?;
    assert!(matches!(error, ClientError::Unavailable { .. }));
    Ok(())
}

#[tokio::test]
async fn client_rejects_response_from_the_wrong_role() -> Result<(), Box<dyn Error>> {
    let temporary = TempDir::new()?;
    let paths = paths(temporary.path());
    fs::create_dir_all(&paths.runtime_dir)?;
    let listener = UnixListener::bind(&paths.worker_socket)?;
    let server = std::thread::spawn(move || {
        if let Ok((mut stream, _)) = listener.accept() {
            use std::io::{BufRead, BufReader, Write};

            let mut request = String::new();
            let _ = BufReader::new(&stream).read_line(&mut request);
            let _ = stream.write_all(
                b"{\"protocol_version\":4,\"response\":{\"type\":\"health\",\"role\":\"supervisor\",\"healthy\":true,\"pid\":1}}\n",
            );
        }
    });
    let error = Client::new(&paths)
        .request(DaemonRole::Worker, Request::Health)
        .await
        .err()
        .ok_or("client accepted the wrong daemon role")?;
    assert!(matches!(error, ClientError::InvalidResponse(_)));
    server.join().map_err(|_| "fake server thread panicked")?;
    Ok(())
}

#[tokio::test]
async fn supervisor_rejects_worker_operations() -> Result<(), Box<dyn Error>> {
    let temporary = TempDir::new()?;
    let paths = paths(temporary.path());
    let (stop, supervisor) = spawn_daemon(DaemonRole::Supervisor, paths.clone());
    wait_for_health(&Client::new(&paths), DaemonRole::Supervisor).await?;
    let error = Client::new(&paths)
        .request(DaemonRole::Supervisor, Request::ProjectList)
        .await
        .err()
        .ok_or("supervisor accepted a worker operation")?;
    assert!(matches!(error, ClientError::InvalidRequest { .. }));
    let error = Client::new(&paths)
        .request(DaemonRole::Supervisor, Request::Resources)
        .await
        .err()
        .ok_or("supervisor accepted a resources request")?;
    assert!(matches!(error, ClientError::InvalidRequest { .. }));
    let _ = stop.send(());
    supervisor.await??;
    Ok(())
}

async fn raw_request(path: &PathBuf, request: &[u8]) -> Result<Vec<u8>, Box<dyn Error>> {
    let mut stream = UnixStream::connect(path).await?;
    stream.write_all(request).await?;
    let mut response = Vec::new();
    stream.read_to_end(&mut response).await?;
    Ok(response)
}
