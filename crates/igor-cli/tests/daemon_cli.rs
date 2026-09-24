use std::{
    error::Error,
    fs,
    io::{BufRead, BufReader, Write},
    os::unix::net::UnixListener,
    path::Path,
    process::{Child, Command, Stdio},
    thread,
    time::Duration,
};

use tempfile::TempDir;

fn command(home: &Path, cwd: &Path) -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_igor"));
    command
        .env_clear()
        .current_dir(cwd)
        .env("HOME", home)
        .env("XDG_CONFIG_HOME", home.join("config"))
        .env("XDG_STATE_HOME", home.join("state"))
        .env("XDG_RUNTIME_DIR", home.join("runtime"));
    command
}

struct ChildGuard(Child);

impl Drop for ChildGuard {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn spawn_daemon(home: &Path, cwd: &Path, role: &str) -> Result<ChildGuard, Box<dyn Error>> {
    let child = command(home, cwd)
        .arg(role)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()?;
    Ok(ChildGuard(child))
}

fn wait_for_health(home: &Path, cwd: &Path) -> Result<(), Box<dyn Error>> {
    for _ in 0..100 {
        let output = command(home, cwd).args(["daemon", "health"]).output()?;
        if output.status.success() {
            return Ok(());
        }
        thread::sleep(Duration::from_millis(20));
    }
    Err("daemons did not become healthy".into())
}

#[test]
fn worker_and_supervisor_support_health_and_status() -> Result<(), Box<dyn Error>> {
    let temporary = TempDir::new()?;
    let home = temporary.path().join("home");
    let mut worker = spawn_daemon(&home, temporary.path(), "worker")?;
    let mut supervisor = spawn_daemon(&home, temporary.path(), "supervisor")?;
    wait_for_health(&home, temporary.path())?;

    let health = command(&home, temporary.path())
        .args(["daemon", "health", "--json"])
        .output()?;
    assert!(health.status.success());
    let health: serde_json::Value = serde_json::from_slice(&health.stdout)?;
    assert_eq!(health.as_array().map(Vec::len), Some(2));
    assert_eq!(health[0]["role"], "worker");
    assert_eq!(health[1]["role"], "supervisor");

    let status = command(&home, temporary.path())
        .args(["daemon", "status", "--json"])
        .output()?;
    assert!(status.status.success());
    let status: serde_json::Value = serde_json::from_slice(&status.stdout)?;
    assert_eq!(status[0]["database"]["schema_version"], 8);
    assert_eq!(status[1]["database"]["integrity"], "ok");

    let resources = command(&home, temporary.path())
        .args(["resources", "--json"])
        .output()?;
    assert!(resources.status.success());
    let resources: serde_json::Value = serde_json::from_slice(&resources.stdout)?;
    assert!(resources.as_array().is_some_and(|resources| {
        resources
            .iter()
            .any(|resource| resource["resource"]["name"] == "host")
    }));

    terminate(&mut worker)?;
    terminate(&mut supervisor)?;
    assert!(!home.join("runtime/igor/worker.sock").exists());
    assert!(!home.join("runtime/igor/supervisor.sock").exists());
    Ok(())
}

#[test]
fn abrupt_termination_allows_clean_restart() -> Result<(), Box<dyn Error>> {
    let temporary = TempDir::new()?;
    let home = temporary.path().join("home");
    for role in ["worker", "supervisor"] {
        let socket = home.join(format!("runtime/igor/{role}.sock"));
        let mut daemon = spawn_daemon(&home, temporary.path(), role)?;
        wait_for_socket(&socket)?;
        daemon.0.kill()?;
        daemon.0.wait()?;
        assert!(socket.exists());

        let restarted = spawn_daemon(&home, temporary.path(), role)?;
        wait_for_protocol(&socket)?;
        drop(restarted);
    }
    Ok(())
}

#[test]
fn daemon_exit_codes_distinguish_transport_and_protocol_errors() -> Result<(), Box<dyn Error>> {
    let temporary = TempDir::new()?;
    let home = temporary.path().join("home");
    let unavailable = command(&home, temporary.path())
        .args(["daemon", "health"])
        .output()?;
    assert_eq!(unavailable.status.code(), Some(3));

    let socket = home.join("runtime/igor/worker.sock");
    serve_once(
        &socket,
        b"{\"protocol_version\":99,\"response\":{\"type\":\"health\",\"role\":\"worker\",\"healthy\":true,\"pid\":1}}\n",
    )?;
    let mismatch = command(&home, temporary.path())
        .args(["daemon", "health"])
        .output()?;
    assert_eq!(mismatch.status.code(), Some(4));

    serve_once(
        &socket,
        b"{\"protocol_version\":5,\"error\":{\"code\":\"IGOR-PROTO-002\",\"kind\":\"invalid_request\",\"message\":\"bad request\"}}\n",
    )?;
    let invalid = command(&home, temporary.path())
        .args(["daemon", "health"])
        .output()?;
    assert_eq!(invalid.status.code(), Some(5));

    serve_once(
        &socket,
        b"{\"protocol_version\":5,\"error\":{\"code\":\"IGOR-DAEMON-002\",\"kind\":\"internal\",\"message\":\"request failed\"}}\n",
    )?;
    let internal = command(&home, temporary.path())
        .args(["daemon", "health"])
        .output()?;
    assert_eq!(internal.status.code(), Some(7));
    Ok(())
}

fn wait_for_socket(path: &Path) -> Result<(), Box<dyn Error>> {
    for _ in 0..100 {
        if path.exists() {
            return Ok(());
        }
        thread::sleep(Duration::from_millis(20));
    }
    Err(format!("socket did not appear at {}", path.display()).into())
}

fn wait_for_protocol(path: &Path) -> Result<(), Box<dyn Error>> {
    for _ in 0..100 {
        if let Ok(mut stream) = std::os::unix::net::UnixStream::connect(path) {
            stream.write_all(b"{\"protocol_version\":5,\"request\":{\"type\":\"health\"}}\n")?;
            let mut response = String::new();
            BufReader::new(stream).read_line(&mut response)?;
            if response.contains("\"healthy\":true") {
                return Ok(());
            }
        }
        thread::sleep(Duration::from_millis(20));
    }
    Err("restarted worker did not answer the protocol".into())
}

fn serve_once(path: &Path, response: &'static [u8]) -> Result<(), Box<dyn Error>> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    if path.exists() {
        fs::remove_file(path)?;
    }
    let listener = UnixListener::bind(path)?;
    thread::spawn(move || {
        if let Ok((mut stream, _)) = listener.accept() {
            let mut request = String::new();
            let _ = BufReader::new(&stream).read_line(&mut request);
            let _ = stream.write_all(response);
        }
    });
    Ok(())
}

fn terminate(child: &mut ChildGuard) -> Result<(), Box<dyn Error>> {
    let status = Command::new("kill")
        .args(["-TERM", &child.0.id().to_string()])
        .status()?;
    if !status.success() {
        return Err("could not send SIGTERM to daemon".into());
    }
    for _ in 0..100 {
        if let Some(status) = child.0.try_wait()? {
            if !status.success() {
                return Err(format!("daemon exited unsuccessfully after SIGTERM: {status}").into());
            }
            return Ok(());
        }
        thread::sleep(Duration::from_millis(20));
    }
    child.0.kill()?;
    Err("daemon did not exit within two seconds of SIGTERM".into())
}
