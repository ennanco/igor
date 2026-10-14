use super::*;
use igor_core::{RuntimePaths, copy_log};

struct DisposableGroup(i32);

async fn reserve_stdout_temp(f: &LogsFixture, temporary: &std::path::Path) -> TestResult {
    sqlx::query("INSERT INTO success_log_archive_preparations (attempt_id, stream, raw_path, temp_path) VALUES (?, 'stdout', ?, ?)")
        .bind(f.attempt.to_string())
        .bind(f.raw("stdout").to_str())
        .bind(temporary.to_str())
        .execute(f.database.pool()).await?;
    Ok(())
}

async fn reserved_stdout_temp(f: &LogsFixture) -> Result<String, Box<dyn Error>> {
    Ok(sqlx::query_scalar(
        "SELECT temp_path FROM success_log_archive_preparations WHERE attempt_id = ? AND stream = 'stdout'",
    )
    .bind(f.attempt.to_string())
    .fetch_one(f.database.pool())
    .await?)
}

// Durable counters plus the on-disk gzip of the stdout stream: the exact state
// a restarted handle must observe after each interrupted phase.
async fn durable_phase(
    f: &LogsFixture,
) -> Result<(i64, i64, i64, i64, i64, i64, bool), Box<dyn Error>> {
    let archives: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM success_log_archives WHERE attempt_id = ? AND stream = 'stdout'",
    )
    .bind(f.attempt.to_string())
    .fetch_one(f.database.pool())
    .await?;
    let published: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM success_log_archives WHERE attempt_id = ? AND stream = 'stdout' AND state = 'published'",
    )
    .bind(f.attempt.to_string())
    .fetch_one(f.database.pool())
    .await?;
    let retired: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM success_log_archives WHERE attempt_id = ? AND stream = 'stdout' AND retired = 1",
    )
    .bind(f.attempt.to_string())
    .fetch_one(f.database.pool())
    .await?;
    let reservations: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM success_log_archive_preparations WHERE attempt_id = ? AND stream = 'stdout'",
    )
    .bind(f.attempt.to_string())
    .fetch_one(f.database.pool())
    .await?;
    let tombstones: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM cleanup_tombstones WHERE operation = 'success_log_compression'",
    )
    .fetch_one(f.database.pool())
    .await?;
    let events: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM events WHERE kind = 'cleanup_state_changed'")
            .fetch_one(f.database.pool())
            .await?;
    Ok((
        archives,
        published,
        retired,
        reservations,
        tombstones,
        events,
        f.raw("stdout").with_extension("log.gz").exists(),
    ))
}

async fn assert_reserved_preflight_rejected(f: &LogsFixture) -> TestResult {
    let before = gc_support::sql_snapshot(&f.database).await?;
    let disk = gc_support::files(f.directory.path(), &f.paths.database)?;
    let plan = gc_support::readonly_plan(&f.database, &f.paths, f.directory.path(), None).await?;
    assert_eq!(plan.entries.len(), 2);
    assert_eq!(plan.entries[0].status, igor_core::GcStatus::Unsafe);
    assert_eq!(plan.entries[1].status, igor_core::GcStatus::Deferred);
    assert!(plan.entries.iter().all(|entry| entry.targets.is_empty()));
    assert!(f.compress().await.is_err());
    assert_eq!(gc_support::sql_snapshot(&f.database).await?, before);
    assert_eq!(
        gc_support::files(f.directory.path(), &f.paths.database)?,
        disk
    );
    for stream in ["stdout", "stderr"] {
        assert!(!f.raw(stream).with_extension("log.gz").exists());
        assert!(
            f.database
                .jobs()
                .log_archive(f.attempt, stream)
                .await?
                .is_none()
        );
        assert!(
            f.database
                .cleanup_tombstone(
                    "success_log_compression",
                    &format!("{}:{stream}", f.attempt)
                )
                .await?
                .is_none()
        );
    }
    Ok(())
}

#[tokio::test]
async fn path_escape_reserved_temp_links_do_not_authorize_reclamation() -> TestResult {
    use std::os::unix::fs::MetadataExt;

    for hardlink in [false, true] {
        let f = LogsFixture::new(Some(true), b"stdout", b"stderr").await?;
        let external = TempDir::new()?;
        let sentinel = external.path().join("sentinel");
        fs::write(&sentinel, b"external sentinel\0\xff")?;
        fs::set_permissions(&sentinel, fs::Permissions::from_mode(0o600))?;
        let temporary = f
            .raw("stdout")
            .parent()
            .ok_or("parent")?
            .join(format!(".igor-log-{}.tmp", uuid::Uuid::new_v4()));
        reserve_stdout_temp(&f, &temporary).await?;
        if hardlink {
            fs::hard_link(&sentinel, &temporary)?;
            let metadata = fs::metadata(&temporary)?;
            assert!(metadata.nlink() > 1);
            assert_eq!(metadata.uid(), fs::metadata("/proc/self")?.uid());
            assert_eq!(metadata.mode() & 0o777, 0o600);
        } else {
            std::os::unix::fs::symlink(&sentinel, &temporary)?;
        }
        let external_before = gc_support::files(external.path(), &f.paths.database)?;
        let leaf_before = fs::symlink_metadata(&temporary)?;
        assert_reserved_preflight_rejected(&f).await?;
        let leaf_after = fs::symlink_metadata(&temporary)?;
        assert_eq!(
            (leaf_after.dev(), leaf_after.ino(), leaf_after.nlink()),
            (leaf_before.dev(), leaf_before.ino(), leaf_before.nlink())
        );
        if !hardlink {
            assert_eq!(fs::read_link(&temporary)?, sentinel);
        }
        assert_eq!(fs::read(&temporary)?, b"external sentinel\0\xff");
        assert_eq!(fs::read(&sentinel)?, b"external sentinel\0\xff");
        assert_eq!(
            gc_support::files(external.path(), &f.paths.database)?,
            external_before
        );
        assert_eq!(fs::read(f.raw("stdout"))?, b"stdout");
        assert_eq!(fs::read(f.raw("stderr"))?, b"stderr");
    }
    Ok(())
}

#[tokio::test]
async fn path_escape_reserved_temp_parent_and_literal_root_rejected_without_mutation() -> TestResult
{
    for case in ["outside_parent", "dot_root", "linked_parent"] {
        let mut f = LogsFixture::new(Some(true), b"stdout", b"stderr").await?;
        let external = TempDir::new()?;
        let sentinel = external.path().join("sentinel");
        fs::write(&sentinel, b"sentinel")?;
        if case == "dot_root" {
            // Preserve the literal spelling in both runtime roots and persisted
            // metadata, so registration matches and lexical preflight must reject it.
            f.paths.log_dir = PathBuf::from(format!("{}/./logs", f.directory.path().display()));
            assert!(f.paths.log_dir.to_str().ok_or("root")?.contains("/./"));
            sqlx::query("UPDATE attempt_processes SET stdout_path = ?, stderr_path = ? WHERE attempt_id = ?")
                .bind(f.raw("stdout").to_str()).bind(f.raw("stderr").to_str()).bind(f.attempt.to_string())
                .execute(f.database.pool()).await?;
        }
        let parent = f.raw("stdout").parent().ok_or("parent")?.to_owned();
        if case == "linked_parent" {
            let moved = external.path().join("attempt");
            fs::rename(&parent, &moved)?;
            std::os::unix::fs::symlink(&moved, &parent)?;
        }
        let temporary = if case == "outside_parent" {
            external.path()
        } else {
            &parent
        }
        .join(format!(".igor-log-{}.tmp", uuid::Uuid::new_v4()));
        reserve_stdout_temp(&f, &temporary).await?;
        assert!(!temporary.try_exists()?);
        let external_before = gc_support::files(external.path(), &f.paths.database)?;
        assert_reserved_preflight_rejected(&f).await?;
        assert!(!temporary.try_exists()?);
        assert_eq!(fs::read(&sentinel)?, b"sentinel");
        assert_eq!(
            gc_support::files(external.path(), &f.paths.database)?,
            external_before
        );
        assert_eq!(fs::read(f.raw("stdout"))?, b"stdout");
        assert_eq!(fs::read(f.raw("stderr"))?, b"stderr");
    }
    Ok(())
}

#[tokio::test]
async fn fifo_reserved_preflight_child() -> TestResult {
    use std::os::unix::fs::{FileTypeExt, MetadataExt};

    let Ok(case) = std::env::var("IGOR_M13_16_FIFO_CASE") else {
        return Ok(());
    };
    assert!(matches!(case.as_str(), "raw" | "reserved_temp"));
    let f = LogsFixture::new(Some(true), b"stdout", b"stderr").await?;
    let temporary = f
        .raw("stdout")
        .parent()
        .ok_or("parent")?
        .join(format!(".igor-log-{}.tmp", uuid::Uuid::new_v4()));
    reserve_stdout_temp(&f, &temporary).await?;
    let fifo = if case == "raw" {
        fs::remove_file(f.raw("stdout"))?;
        f.raw("stdout")
    } else {
        temporary.clone()
    };
    nix::unistd::mkfifo(
        &fifo,
        nix::sys::stat::Mode::S_IRUSR | nix::sys::stat::Mode::S_IWUSR,
    )?;
    let sentinel = f.directory.path().join("sentinel");
    fs::write(&sentinel, b"sentinel")?;
    let before = fs::symlink_metadata(&fifo)?;
    assert!(before.file_type().is_fifo());
    // No writer is ever opened: either vulnerable operation would block here.
    assert_reserved_preflight_rejected(&f).await?;
    let after = fs::symlink_metadata(&fifo)?;
    assert!(after.file_type().is_fifo());
    assert_eq!((after.dev(), after.ino()), (before.dev(), before.ino()));
    assert_eq!(fs::read(f.raw("stderr"))?, b"stderr");
    assert_eq!(fs::read(&sentinel)?, b"sentinel");
    if case == "reserved_temp" {
        assert_eq!(fs::read(f.raw("stdout"))?, b"stdout");
    } else {
        assert!(!temporary.try_exists()?);
    }
    Ok(())
}

#[test]
fn fifo_preflight_rejection_is_process_bounded() -> TestResult {
    use std::io::Read;

    struct ChildGuard(std::process::Child);
    impl Drop for ChildGuard {
        fn drop(&mut self) {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }
    for case in ["raw", "reserved_temp"] {
        // Fixtures created by the child remain owned by this parent even if a
        // blocked spawn_blocking worker requires SIGKILL.
        let directory = TempDir::new()?;
        let stdout = directory.path().join("child.stdout");
        let stderr = directory.path().join("child.stderr");
        let mut child = ChildGuard(
            Command::new(std::env::current_exe()?)
                .args([
                    "--exact",
                    "success_log_archive_cases::fifo_reserved_preflight_child",
                    "--nocapture",
                ])
                .env("IGOR_M13_16_FIFO_CASE", case)
                .env("TMPDIR", directory.path())
                .stdin(std::process::Stdio::null())
                .stdout(fs::File::create(&stdout)?)
                .stderr(fs::File::create(&stderr)?)
                .spawn()?,
        );
        let deadline = std::time::Instant::now() + Duration::from_secs(20);
        let status = loop {
            if let Some(status) = child.0.try_wait()? {
                break Some(status);
            }
            if std::time::Instant::now() >= deadline {
                child.0.kill()?;
                child.0.wait()?;
                break None;
            }
            std::thread::sleep(Duration::from_millis(20));
        };
        let mut output = String::new();
        let mut errors = String::new();
        fs::File::open(stdout)?
            .take(8192)
            .read_to_string(&mut output)?;
        fs::File::open(stderr)?
            .take(8192)
            .read_to_string(&mut errors)?;
        assert!(
            status.is_some_and(|status| status.success()),
            "{case}: child failed or exceeded watchdog; stdout: {output}; stderr: {errors}"
        );
        assert!(
            output.contains("1 passed"),
            "exact FIFO worker did not execute: {output}"
        );
    }
    Ok(())
}

#[tokio::test]
async fn race_success_log_compressors_retire_each_stream_once() -> TestResult {
    async fn pending<F: std::future::Future>(
        barrier: &tokio::sync::Barrier,
        started: tokio::sync::oneshot::Sender<()>,
        future: F,
    ) -> F::Output {
        barrier.wait().await;
        let mut future = std::pin::pin!(future);
        std::future::poll_fn(|cx| {
            assert!(
                future.as_mut().poll(cx).is_pending(),
                "compressor finished before writer handoff"
            );
            std::task::Poll::Ready(())
        })
        .await;
        let _ = started.send(());
        future.await
    }

    let stdout = b"binary\0\xffstdout";
    let stderr = b"stderr\xff\0bytes";
    let f = LogsFixture::new(Some(true), stdout, stderr).await?;
    let unowned = f
        .raw("stdout")
        .parent()
        .ok_or("parent")?
        .join(format!(".igor-log-{}.tmp", uuid::Uuid::new_v4()));
    fs::write(&unowned, b"unowned sentinel")?;

    // Open all independent pools before taking the writer; only the watchdog
    // uses time, while the barrier and Pending notifications drive the race.
    let first = Database::open(&f.paths.database).await?;
    let second = Database::open(&f.paths.database).await?;
    let external = Database::open(&f.paths.database).await?;
    let mut writer = external.pool().begin().await?;
    sqlx::query("UPDATE projects SET name = name")
        .execute(&mut *writer)
        .await?;
    let barrier = tokio::sync::Barrier::new(3);
    let (first_started, first_pending) = tokio::sync::oneshot::channel();
    let (second_started, second_pending) = tokio::sync::oneshot::channel();
    let first_jobs = first.jobs();
    let second_jobs = second.jobs();
    let first_compression = pending(
        &barrier,
        first_started,
        first_jobs.compress_success_logs(f.attempt, &f.paths),
    );
    let second_compression = pending(
        &barrier,
        second_started,
        second_jobs.compress_success_logs(f.attempt, &f.paths),
    );
    let release = async {
        barrier.wait().await;
        first_pending.await?;
        second_pending.await?;
        writer.commit().await?;
        Ok::<_, Box<dyn Error>>(())
    };
    let (first_result, second_result, released) =
        tokio::time::timeout(Duration::from_secs(30), async {
            tokio::join!(biased; first_compression, second_compression, release)
        })
        .await?;
    released?;
    first_result?;
    second_result?;

    let archives: Vec<(String, String, i64)> = sqlx::query_as(
        "SELECT stream, state, retired FROM success_log_archives WHERE attempt_id = ? ORDER BY stream",
    )
    .bind(f.attempt.to_string())
    .fetch_all(f.database.pool())
    .await?;
    assert_eq!(
        archives,
        [
            ("stderr".into(), "published".into(), 1),
            ("stdout".into(), "published".into(), 1)
        ]
    );
    for (stream, expected) in [("stdout", stdout.as_slice()), ("stderr", stderr.as_slice())] {
        let archive = f
            .database
            .jobs()
            .log_archive(f.attempt, stream)
            .await?
            .ok_or("archive")?;
        assert!(!f.raw(stream).exists());
        assert!(!archive.temporary_path.exists());
        let mut decoded = Vec::new();
        copy_log(&f.raw(stream), Some(&archive), 0, &mut decoded)?;
        assert_eq!(decoded, expected);
    }
    let preparations: Vec<String> = sqlx::query_scalar(
        "SELECT temp_path FROM success_log_archive_preparations WHERE attempt_id = ? ORDER BY stream",
    )
    .bind(f.attempt.to_string())
    .fetch_all(f.database.pool())
    .await?;
    assert_eq!(preparations.len(), 2);
    assert!(
        preparations
            .iter()
            .all(|path| !std::path::Path::new(path).exists())
    );
    let temporaries: Vec<_> = fs::read_dir(f.raw("stdout").parent().ok_or("parent")?)?
        .collect::<Result<Vec<_>, _>>()?
        .into_iter()
        .filter(|entry| {
            entry
                .file_name()
                .to_string_lossy()
                .starts_with(".igor-log-")
        })
        .map(|entry| entry.path())
        .collect();
    assert_eq!(temporaries.as_slice(), std::slice::from_ref(&unowned));
    assert_eq!(fs::read(&unowned)?, b"unowned sentinel");

    let tombstones: Vec<(String, String, String, String)> = sqlx::query_as(
        "SELECT subject_id, outcome, decided_at, completed_at FROM cleanup_tombstones WHERE operation = 'success_log_compression' ORDER BY subject_id",
    )
    .fetch_all(f.database.pool())
    .await?;
    assert_eq!(tombstones.len(), 2);
    for (row, stream) in tombstones.iter().zip(["stderr", "stdout"]) {
        assert_eq!(row.0, format!("{}:{stream}", f.attempt));
        assert_eq!(row.1, "raw_retired");
    }
    let events: Vec<(String, String)> = sqlx::query_as(
        "SELECT json_extract(payload_json, '$.data.stream'), json_extract(payload_json, '$.data.attempt_id') FROM events WHERE kind = 'cleanup_state_changed' AND json_extract(payload_json, '$.data.reason') = 'success_log_compression' ORDER BY json_extract(payload_json, '$.data.stream')",
    )
    .fetch_all(f.database.pool())
    .await?;
    assert_eq!(
        events,
        [
            ("stderr".into(), f.attempt.to_string()),
            ("stdout".into(), f.attempt.to_string())
        ]
    );
    assert_eq!(f.compress().await?, 0);
    let replay_tombstones: Vec<(String, String, String, String)> = sqlx::query_as(
        "SELECT subject_id, outcome, decided_at, completed_at FROM cleanup_tombstones WHERE operation = 'success_log_compression' ORDER BY subject_id",
    )
    .fetch_all(f.database.pool())
    .await?;
    assert_eq!(replay_tombstones, tombstones);
    let replay_events: Vec<(String, String)> = sqlx::query_as(
        "SELECT json_extract(payload_json, '$.data.stream'), json_extract(payload_json, '$.data.attempt_id') FROM events WHERE kind = 'cleanup_state_changed' AND json_extract(payload_json, '$.data.reason') = 'success_log_compression' ORDER BY json_extract(payload_json, '$.data.stream')",
    )
    .fetch_all(f.database.pool())
    .await?;
    assert_eq!(replay_events, events);
    assert_eq!(fs::read(unowned)?, b"unowned sentinel");
    Ok(())
}

#[tokio::test]
async fn gc_logs_unregistered_project_defers_executor_and_each_archive_phase() -> TestResult {
    use igor_core::GcStatus as S;
    for phase in ["initial", "reservation", "prepared", "published"] {
        let f = LogsFixture::new(Some(true), b"stdout", b"stderr").await?;
        match phase {
            "initial" => {
                sqlx::query("UPDATE projects SET registered = 0")
                    .execute(f.database.pool())
                    .await?;
            }
            "reservation" => {
                sqlx::raw_sql("CREATE TRIGGER deregister AFTER INSERT ON success_log_archive_preparations BEGIN UPDATE projects SET registered = 0; END;").execute(f.database.pool()).await?;
            }
            "prepared" => {
                sqlx::raw_sql("CREATE TRIGGER deregister AFTER INSERT ON success_log_archives BEGIN UPDATE projects SET registered = 0; END;").execute(f.database.pool()).await?;
            }
            "published" => {
                sqlx::raw_sql("CREATE TRIGGER deregister AFTER UPDATE OF state ON success_log_archives WHEN NEW.state = 'published' BEGIN UPDATE projects SET registered = 0; END;").execute(f.database.pool()).await?;
            }
            _ => unreachable!(),
        }
        if phase != "initial" {
            assert_eq!(f.compress().await?, 0, "{phase}");
        }
        let plan =
            gc_support::readonly_plan(&f.database, &f.paths, f.directory.path(), None).await?;
        assert_eq!(plan.entries.len(), 2);
        assert!(
            plan.entries
                .iter()
                .all(|e| e.status == S::Deferred && e.reason == "project_unregistered")
        );
        let dir = f.raw("stdout").parent().ok_or("parent")?.to_owned();
        let before: std::collections::BTreeMap<_, _> = fs::read_dir(&dir)?
            .map(|entry| {
                let path = entry?.path();
                Ok((path.clone(), fs::read(path)?))
            })
            .collect::<Result<_, io::Error>>()?;
        assert_eq!(f.compress().await?, 0);
        let after: std::collections::BTreeMap<_, _> = fs::read_dir(&dir)?
            .map(|entry| {
                let path = entry?.path();
                Ok((path.clone(), fs::read(path)?))
            })
            .collect::<Result<_, io::Error>>()?;
        assert_eq!(before, after, "{phase}");
        assert_eq!(fs::read(f.raw("stdout"))?, b"stdout");
        assert_eq!(fs::read(f.raw("stderr"))?, b"stderr");
        if phase == "initial" || phase == "reservation" {
            assert!(!f.raw("stdout").with_extension("log.gz").exists());
            assert_eq!(before.len(), 2);
        }
    }
    Ok(())
}

#[tokio::test]
async fn gc_logs_preview_read_only_fresh_and_reserved_partial_match_executor() -> TestResult {
    use igor_core::{GcEffect as E, GcStatus as S, GcTarget};
    let binary = b"binary\0\xffstdout";
    for reserved in [false, true] {
        let f = LogsFixture::new(Some(true), binary, b"").await?;
        let temp = f
            .raw("stdout")
            .parent()
            .ok_or("parent")?
            .join(format!(".igor-log-{}.tmp", uuid::Uuid::new_v4()));
        if reserved {
            sqlx::query("INSERT INTO success_log_archive_preparations (attempt_id, stream, raw_path, temp_path) VALUES (?, 'stdout', ?, ?)").bind(f.attempt.to_string()).bind(f.raw("stdout").to_str()).bind(temp.to_str()).execute(f.database.pool()).await?;
            fs::write(&temp, b"partial")?;
            fs::set_permissions(&temp, fs::Permissions::from_mode(0o600))?;
        }
        let plan =
            gc_support::readonly_plan(&f.database, &f.paths, f.directory.path(), None).await?;
        assert_eq!(plan.entries.len(), 2);
        assert!(plan.entries.iter().all(|e| e.status == S::Ready));
        assert_eq!(
            plan.entries[0].effect,
            if reserved {
                E::ResumeCompression
            } else {
                E::CompressPublishRetireRaw
            }
        );
        for (entry, (stream, bytes)) in plan
            .entries
            .iter()
            .zip([("stdout", binary.as_slice()), ("stderr", b"".as_slice())])
        {
            assert_eq!(entry.subject_id, format!("{}:{stream}", f.attempt));
            let mut expected = vec![GcTarget {
                path: f.raw(stream),
                bytes: Some(bytes.len() as u64),
                effect: E::CompressPublishRetireRaw,
            }];
            if reserved && stream == "stdout" {
                expected.push(GcTarget {
                    path: temp.clone(),
                    bytes: Some(b"partial".len() as u64),
                    effect: E::ResumeCompression,
                });
            }
            assert_eq!(entry.targets, expected);
        }
        let worklist = plan.worklist()?;
        assert_eq!(worklist.selections.len(), 2);
        for (selection, entry) in worklist.selections.iter().zip(&plan.entries) {
            assert_eq!(&selection.expected, entry);
        }
        let result = f
            .database
            .gc_execute(None, false, worklist, &f.paths)
            .await?;
        assert_success_gc_effects(&f, &plan, &result, binary, b"").await?;
        if reserved {
            assert!(!temp.exists());
            let archive = f
                .database
                .jobs()
                .log_archive(f.attempt, "stdout")
                .await?
                .ok_or("archive")?;
            assert_eq!(archive.temporary_path, temp);
            assert_ne!(fs::read(&archive.path)?, b"partial");
            let mut bytes = Vec::new();
            copy_log(&f.raw("stdout"), Some(&archive), 0, &mut bytes)?;
            assert_eq!(bytes, binary);
        }
    }
    Ok(())
}

// Compound compression targets name the raw input, not the destination. Derive
// the destination independently and decode it without the production log reader.
async fn assert_success_gc_effects(
    f: &LogsFixture,
    plan: &igor_core::GcPlan,
    result: &igor_core::GcExecution,
    stdout: &[u8],
    stderr: &[u8],
) -> TestResult {
    use igor_core::{GcExecutionStatus, GcOperation};
    use std::io::Read;

    assert_eq!(result.completed, 2, "{result:?}");
    assert_eq!(result.entries.len(), 2);
    assert_eq!(
        (
            result.already_done,
            result.deferred,
            result.unsafe_count,
            result.errors
        ),
        (0, 0, 0, 0)
    );
    for (stream, expected) in [("stdout", stdout), ("stderr", stderr)] {
        let subject = format!("{}:{stream}", f.attempt);
        let saved = plan
            .entries
            .iter()
            .find(|e| e.subject_id == subject)
            .ok_or("saved stream")?;
        let response = result
            .entries
            .iter()
            .find(|e| e.subject_id == subject)
            .ok_or("response stream")?;
        assert_eq!(response.operation, saved.operation);
        assert_eq!(response.operation, GcOperation::SuccessLogCompression);
        assert_eq!(response.status, GcExecutionStatus::Completed);
        assert!(response.cause.is_none());
        let raw = f.raw(stream);
        let destination = raw.with_extension("log.gz");
        assert!(!raw.try_exists()?);
        let compressed = fs::read(&destination)?;
        let mut decoded = Vec::new();
        flate2::read::GzDecoder::new(compressed.as_slice()).read_to_end(&mut decoded)?;
        assert_eq!(decoded, expected);
        let archive = f
            .database
            .jobs()
            .log_archive(f.attempt, stream)
            .await?
            .ok_or("archive")?;
        assert_eq!(archive.original_path, raw);
        assert_eq!(archive.path, destination);
        assert_eq!(archive.original_size, expected.len() as u64);
        assert_eq!(
            archive.original_sha256,
            format!("{:x}", sha2::Sha256::digest(expected))
        );
        assert_eq!(archive.archive_size, compressed.len() as u64);
        assert_eq!(
            archive.archive_sha256,
            format!("{:x}", sha2::Sha256::digest(&compressed))
        );
        assert!(!archive.temporary_path.try_exists()?);
        let reservation: (String, String) = sqlx::query_as("SELECT raw_path, temp_path FROM success_log_archive_preparations WHERE attempt_id = ? AND stream = ?")
            .bind(f.attempt.to_string()).bind(stream).fetch_one(f.database.pool()).await?;
        assert_eq!(PathBuf::from(reservation.0), raw);
        assert_eq!(PathBuf::from(reservation.1), archive.temporary_path);
        let state: (String, i64) = sqlx::query_as(
            "SELECT state, retired FROM success_log_archives WHERE attempt_id = ? AND stream = ?",
        )
        .bind(f.attempt.to_string())
        .bind(stream)
        .fetch_one(f.database.pool())
        .await?;
        assert_eq!(state, ("published".into(), 1));
        let tombstone = f
            .database
            .cleanup_tombstone("success_log_compression", &subject)
            .await?
            .ok_or("tombstone")?;
        assert_eq!(tombstone["outcome"], "raw_retired");
        assert_eq!(tombstone["subject_id"], subject);
        assert_eq!(
            tombstone["identity"]["archive"],
            serde_json::to_value(&archive)?
        );
    }
    let mut leaves = fs::read_dir(f.raw("stdout").parent().ok_or("parent")?)?
        .map(|entry| Ok(entry?.path()))
        .collect::<Result<Vec<_>, io::Error>>()?;
    leaves.sort();
    assert_eq!(
        leaves,
        [
            f.raw("stderr").with_extension("log.gz"),
            f.raw("stdout").with_extension("log.gz")
        ]
    );
    Ok(())
}

#[tokio::test]
async fn gc_logs_saved_plan_prepared_published_and_metadata_only_match_full_effect() -> TestResult {
    use igor_core::{GcEffect as E, GcStatus as S, GcTarget};
    let stdout = b"prepared\0\xffbinary";
    let stderr = b"stderr\xff\0";
    for phase in ["prepared", "published", "metadata_only"] {
        let f = LogsFixture::new(Some(true), stdout, stderr).await?;
        let fault = if phase == "metadata_only" {
            "CREATE TRIGGER archive_fault BEFORE UPDATE OF retired ON success_log_archives BEGIN SELECT RAISE(ABORT, 'fault'); END;"
        } else {
            "CREATE TRIGGER archive_fault BEFORE UPDATE OF state ON success_log_archives BEGIN SELECT RAISE(ABORT, 'fault'); END;"
        };
        sqlx::raw_sql(fault).execute(f.database.pool()).await?;
        assert!(f.compress().await.is_err());
        sqlx::query("DROP TRIGGER archive_fault")
            .execute(f.database.pool())
            .await?;
        let identity: String = sqlx::query_scalar("SELECT identity_json FROM success_log_archives WHERE attempt_id = ? AND stream = 'stdout'")
            .bind(f.attempt.to_string()).fetch_one(f.database.pool()).await?;
        let archive: igor_core::LogArchive = serde_json::from_str(&identity)?;
        let destination = f.raw("stdout").with_extension("log.gz");
        let copy = if phase == "prepared" {
            fs::rename(&destination, &archive.temporary_path)?;
            archive.temporary_path.clone()
        } else {
            if phase == "published" {
                sqlx::query("UPDATE success_log_archives SET state = 'published' WHERE attempt_id = ? AND stream = 'stdout'")
                    .bind(f.attempt.to_string()).execute(f.database.pool()).await?;
            }
            destination
        };
        let compressed_before = fs::read(&copy)?;
        assert_eq!(f.raw("stdout").try_exists()?, phase != "metadata_only");
        let plan =
            gc_support::readonly_plan(&f.database, &f.paths, f.directory.path(), None).await?;
        assert_eq!(plan.entries.len(), 2);
        let first = &plan.entries[0];
        assert_eq!(first.subject_id, format!("{}:stdout", f.attempt));
        assert_eq!(
            first.status,
            if phase == "metadata_only" {
                S::MetadataOnly
            } else {
                S::Ready
            }
        );
        assert_eq!(
            first.effect,
            if phase == "metadata_only" {
                E::ResumeMetadata
            } else {
                E::ResumePublishRetireRaw
            }
        );
        let mut targets = vec![GcTarget {
            path: copy,
            bytes: Some(compressed_before.len() as u64),
            effect: if phase == "prepared" {
                E::ResumePublishRetireRaw
            } else {
                E::Retain
            },
        }];
        if phase != "metadata_only" {
            targets.push(GcTarget {
                path: f.raw("stdout"),
                bytes: Some(stdout.len() as u64),
                effect: E::Remove,
            });
        }
        assert_eq!(first.targets, targets);
        assert_eq!(plan.entries[1].status, S::Ready);
        assert_eq!(plan.entries[1].effect, E::CompressPublishRetireRaw);
        assert_eq!(
            plan.entries[1].targets,
            [GcTarget {
                path: f.raw("stderr"),
                bytes: Some(stderr.len() as u64),
                effect: E::CompressPublishRetireRaw
            }]
        );
        let worklist = plan.worklist()?;
        assert_eq!(worklist.selections.len(), 2);
        for (selection, entry) in worklist.selections.iter().zip(&plan.entries) {
            assert_eq!(&selection.expected, entry);
        }
        let result = f
            .database
            .gc_execute(None, false, worklist, &f.paths)
            .await?;
        assert_success_gc_effects(&f, &plan, &result, stdout, stderr).await?;
        assert_eq!(
            fs::read(f.raw("stdout").with_extension("log.gz"))?,
            compressed_before
        );
    }
    Ok(())
}

#[tokio::test]
async fn gc_logs_preview_corruption_stops_later_stream_and_never_reclaims_temp() -> TestResult {
    use igor_core::GcStatus as S;
    for case in [
        "foreign",
        "missing",
        "symlink",
        "partial_permissions",
        "prepared_corrupt",
    ] {
        let f = LogsFixture::new(Some(true), b"stdout", b"stderr").await?;
        match case {
            "foreign" => fs::write(f.raw("stdout").with_extension("log.gz"), b"foreign")?,
            "missing" => fs::remove_file(f.raw("stdout"))?,
            "symlink" => {
                fs::remove_file(f.raw("stdout"))?;
                std::os::unix::fs::symlink(f.raw("stderr"), f.raw("stdout"))?;
            }
            "partial_permissions" => {
                let temp = f
                    .raw("stdout")
                    .parent()
                    .ok_or("parent")?
                    .join(format!(".igor-log-{}.tmp", uuid::Uuid::new_v4()));
                sqlx::query("INSERT INTO success_log_archive_preparations (attempt_id, stream, raw_path, temp_path) VALUES (?, 'stdout', ?, ?)").bind(f.attempt.to_string()).bind(f.raw("stdout").to_str()).bind(temp.to_str()).execute(f.database.pool()).await?;
                fs::write(&temp, b"partial")?;
                fs::set_permissions(temp, fs::Permissions::from_mode(0o644))?;
            }
            "prepared_corrupt" => {
                sqlx::raw_sql("CREATE TRIGGER archive_fault BEFORE UPDATE OF state ON success_log_archives BEGIN SELECT RAISE(ABORT, 'fault'); END;").execute(f.database.pool()).await?;
                assert!(f.compress().await.is_err());
                sqlx::query("DROP TRIGGER archive_fault")
                    .execute(f.database.pool())
                    .await?;
                fs::write(f.raw("stdout").with_extension("log.gz"), b"corrupt")?;
            }
            _ => unreachable!(),
        }
        let plan =
            gc_support::readonly_plan(&f.database, &f.paths, f.directory.path(), None).await?;
        assert_eq!(plan.entries[0].status, S::Unsafe, "{case}");
        assert_eq!(plan.entries[1].status, S::Deferred, "{case}");
        assert_eq!(plan.entries[1].reason, "preceding_stream_unsafe");
        assert!(plan.entries.iter().all(|e| e.targets.is_empty()));
        assert!(f.compress().await.is_err());
        assert_eq!(fs::read(f.raw("stderr"))?, b"stderr");
    }
    Ok(())
}

#[tokio::test]
async fn success_logs_preserve_raw_for_active_diagnosis_without_artifacts() -> TestResult {
    let f = LogsFixture::new(Some(true), b"diagnosis stdout", b"diagnosis stderr").await?;
    let recovery = uuid::Uuid::new_v4().to_string();
    sqlx::query("INSERT INTO recoveries (id, attempt_id, state, idempotency_key) VALUES (?, ?, 'diagnosing', ?)")
        .bind(&recovery).bind(f.attempt.to_string()).bind(&recovery)
        .execute(f.database.pool()).await?;
    assert_eq!(f.compress().await?, 0);
    assert_eq!(fs::read(f.raw("stdout"))?, b"diagnosis stdout");
    sqlx::query("UPDATE recoveries SET state = 'failed', diagnosis_json = '{}' WHERE id = ?")
        .bind(&recovery)
        .execute(f.database.pool())
        .await?;
    assert_eq!(f.compress().await?, 2);
    Ok(())
}

impl Drop for DisposableGroup {
    fn drop(&mut self) {
        let _ = nix::sys::signal::killpg(
            nix::unistd::Pid::from_raw(self.0),
            nix::sys::signal::Signal::SIGKILL,
        );
    }
}

#[tokio::test]
async fn success_logs_group_child_outlives_successful_leader() -> TestResult {
    use std::os::unix::process::CommandExt;

    for prepared in [false, true] {
        let f = LogsFixture::new(Some(true), b"early", b"").await?;
        if prepared {
            sqlx::raw_sql("CREATE TRIGGER archive_fault BEFORE UPDATE OF state ON success_log_archives BEGIN SELECT RAISE(ABORT, 'fault'); END;")
                .execute(f.database.pool()).await?;
            assert!(f.compress().await.is_err());
            sqlx::raw_sql(
                "DROP TRIGGER archive_fault; UPDATE success_log_archives SET state = 'published';",
            )
            .execute(f.database.pool())
            .await?;
        }
        let release = f.directory.path().join("release-child");
        let mut leader = Command::new("sh")
            .args([
                "-c",
                "(while [ ! -e \"$1\" ]; do sleep 0.02; done; printf late) & printf early",
                "sh",
            ])
            .arg(&release)
            .process_group(0)
            .stdout(fs::File::create(f.raw("stdout"))?)
            .stderr(fs::File::create(f.raw("stderr"))?)
            .spawn()?;
        let group = DisposableGroup(leader.id() as i32);
        assert!(leader.wait()?.success());
        sqlx::query(
            "UPDATE attempt_processes SET pid = ?, process_group_id = ? WHERE attempt_id = ?",
        )
        .bind(i64::from(group.0))
        .bind(i64::from(group.0))
        .bind(f.attempt.to_string())
        .execute(f.database.pool())
        .await?;
        assert_eq!(f.compress().await?, 0);
        assert_eq!(fs::read(f.raw("stdout"))?, b"early");
        let retired: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM success_log_archives WHERE retired = 1")
                .fetch_one(f.database.pool())
                .await?;
        assert_eq!(retired, 0);
        if !prepared {
            assert!(!f.raw("stdout").with_extension("log.gz").exists());
        }
        fs::write(&release, b"")?;
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if fs::read(f.raw("stdout"))? == b"earlylate" {
                    break Ok::<_, io::Error>(());
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await??;
        if prepared {
            // An already prepared archive no longer matches late bytes: raw
            // must survive even after the child has finished.
            let _ = f.compress().await;
            assert_eq!(fs::read(f.raw("stdout"))?, b"earlylate");
        } else {
            tokio::time::timeout(Duration::from_secs(5), async {
                loop {
                    if f.compress().await? == 2 {
                        break Ok::<_, Box<dyn Error>>(());
                    }
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
            })
            .await??;
            let archive = f
                .database
                .jobs()
                .log_archive(f.attempt, "stdout")
                .await?
                .ok_or("archive")?;
            let mut bytes = Vec::new();
            copy_log(&f.raw("stdout"), Some(&archive), 0, &mut bytes)?;
            assert_eq!(bytes, b"earlylate");
        }
        let state: String = sqlx::query_scalar("SELECT state FROM attempts WHERE id = ?")
            .bind(f.attempt.to_string())
            .fetch_one(f.database.pool())
            .await?;
        assert_eq!(state, "succeeded");
    }
    Ok(())
}

#[tokio::test]
async fn success_logs_reserved_partial_temp_recovered_exactly_after_restart() -> TestResult {
    for unsafe_temp in [false, true] {
        let mut f = LogsFixture::new(Some(true), b"raw\0\xff", b"error").await?;
        let dir = f.raw("stdout").parent().ok_or("parent")?.to_owned();
        let temporary = dir.join(format!(".igor-log-{}.tmp", uuid::Uuid::new_v4()));
        reserve_stdout_temp(&f, &temporary).await?;
        // The reservation is durable before a single byte of the temporary exists.
        assert_eq!(
            reserved_stdout_temp(&f).await?,
            temporary.to_str().ok_or("temp")?
        );
        fs::write(&temporary, b"partial gzip before crash")?;
        fs::set_permissions(
            &temporary,
            fs::Permissions::from_mode(if unsafe_temp { 0o644 } else { 0o600 }),
        )?;
        let unowned = dir.join(format!(".igor-log-{}.tmp", uuid::Uuid::new_v4()));
        fs::write(&unowned, b"unowned")?;
        assert!(
            sqlx::query("UPDATE success_log_archive_preparations SET temp_path = 'other'")
                .execute(f.database.pool())
                .await
                .is_err()
        );
        let reserved = reserved_stdout_temp(&f).await?;
        let disk = gc_support::files(f.directory.path(), &f.paths.database)?;
        f.database.pool().close().await;
        f.database = Database::open(&f.paths.database).await?;
        // Durability without any checkpoint: the row, the partial bytes and the
        // foreign temporary are exactly as they were before the close.
        assert_eq!(reserved_stdout_temp(&f).await?, reserved);
        assert_eq!(fs::read(&temporary)?, b"partial gzip before crash");
        assert_eq!(fs::read(&unowned)?, b"unowned");
        assert_eq!(
            gc_support::files(f.directory.path(), &f.paths.database)?,
            disk
        );
        let result = f.compress().await;
        if unsafe_temp {
            assert!(result.is_err());
            assert_eq!(fs::read(&temporary)?, b"partial gzip before crash");
            assert_eq!(fs::read(f.raw("stdout"))?, b"raw\0\xff");
        } else {
            assert_eq!(result?, 2);
            assert!(!temporary.exists());
            let archive = f
                .database
                .jobs()
                .log_archive(f.attempt, "stdout")
                .await?
                .ok_or("archive")?;
            assert_eq!(archive.temporary_path, temporary);
            let mut bytes = Vec::new();
            copy_log(&f.raw("stdout"), Some(&archive), 0, &mut bytes)?;
            assert_eq!(bytes, b"raw\0\xff");
        }
        assert_eq!(fs::read(unowned)?, b"unowned");
    }
    // A reservation committed before the interrupted writer produced any byte:
    // nothing exists to reclaim, yet recovery owns exactly that path.
    let mut f = LogsFixture::new(Some(true), b"binary\0\xff tail", b"error").await?;
    let dir = f.raw("stdout").parent().ok_or("parent")?.to_owned();
    let temporary = dir.join(format!(".igor-log-{}.tmp", uuid::Uuid::new_v4()));
    reserve_stdout_temp(&f, &temporary).await?;
    assert!(!temporary.try_exists()?);
    let unowned = dir.join(format!(".igor-log-{}.tmp", uuid::Uuid::new_v4()));
    fs::write(&unowned, b"foreign")?;
    let reserved = reserved_stdout_temp(&f).await?;
    assert_eq!(reserved, temporary.to_str().ok_or("temp")?);
    f.database.pool().close().await;
    f.database = Database::open(&f.paths.database).await?;
    assert_eq!(reserved_stdout_temp(&f).await?, reserved);
    assert!(!temporary.try_exists()?);
    assert_eq!(f.compress().await?, 2);
    assert!(!temporary.exists());
    assert_eq!(fs::read(&unowned)?, b"foreign");
    let archive = f
        .database
        .jobs()
        .log_archive(f.attempt, "stdout")
        .await?
        .ok_or("archive")?;
    assert_eq!(archive.temporary_path, temporary);
    let mut bytes = Vec::new();
    copy_log(&f.raw("stdout"), Some(&archive), 0, &mut bytes)?;
    assert_eq!(bytes, b"binary\0\xff tail");
    Ok(())
}

struct LogsFixture {
    directory: TempDir,
    database: Database,
    paths: RuntimePaths,
    job: JobSpec,
    attempt: AttemptId,
}

#[tokio::test]
async fn gc_execute_logs_reports_completed_stream_when_later_selected_stream_becomes_unsafe()
-> TestResult {
    let f = LogsFixture::new(Some(true), b"stdout", b"stderr").await?;
    let worklist = f.database.gc_plan(None, &f.paths).await?.worklist()?;
    sqlx::raw_sql("CREATE TRIGGER invalid_second_stream AFTER UPDATE OF retired ON success_log_archives WHEN NEW.stream = 'stdout' AND NEW.retired = 1 BEGIN INSERT INTO success_log_archives(attempt_id, stream, identity_json, state) VALUES (NEW.attempt_id, 'stderr', NEW.identity_json, 'prepared'); END;").execute(f.database.pool()).await?;
    let result = f
        .database
        .gc_execute(None, false, worklist, &f.paths)
        .await?;
    assert_eq!(result.completed, 1, "{result:?}");
    assert_eq!(result.unsafe_count, 1, "{result:?}");
    assert!(!f.raw("stdout").exists());
    assert_eq!(fs::read(f.raw("stderr"))?, b"stderr");
    Ok(())
}

#[tokio::test]
async fn gc_execute_logs_fixed_stream_selection_and_failed_filter() -> TestResult {
    let f = LogsFixture::new(Some(true), b"stdout", b"stderr").await?;
    let failed = f.database.gc_plan_filtered(None, &f.paths, true).await?;
    assert_eq!(failed.pages.len(), 3);
    assert!(failed.entries.is_empty());
    fs::remove_file(f.raw("stderr"))?;
    let plan = f.database.gc_plan(None, &f.paths).await?;
    let worklist = plan.worklist()?;
    assert_eq!(worklist.selections.len(), 1);
    assert!(
        worklist.selections[0]
            .expected
            .subject_id
            .ends_with(":stdout")
    );
    fs::write(f.raw("stderr"), b"newly-ready-stderr")?;
    assert!(
        f.database
            .gc_execute(None, true, worklist.clone(), &f.paths)
            .await
            .is_err()
    );
    assert!(f.raw("stdout").is_file());
    let result = f
        .database
        .gc_execute(None, false, worklist, &f.paths)
        .await?;
    assert_eq!(result.completed, 1, "{result:?}");
    assert!(!f.raw("stdout").exists());
    assert_eq!(fs::read(f.raw("stderr"))?, b"newly-ready-stderr");
    assert!(
        f.database
            .jobs()
            .log_archive(f.attempt, "stderr")
            .await?
            .is_none()
    );
    Ok(())
}

#[tokio::test]
async fn gc_execute_logs_changed_raw_identity_is_deferred_before_reservation() -> TestResult {
    let f = LogsFixture::new(Some(true), b"before", b"stderr").await?;
    let worklist = f.database.gc_plan(None, &f.paths).await?.worklist()?;
    fs::write(f.raw("stdout"), b"after!")?;
    let result = f
        .database
        .gc_execute(None, false, worklist, &f.paths)
        .await?;
    assert_eq!(result.completed, 1, "{result:?}");
    assert_eq!(result.deferred, 1);
    let reservations: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM success_log_archive_preparations")
            .fetch_one(f.database.pool())
            .await?;
    // stderr is independently selected: a changed stdout cannot authorize a new stdout.
    assert_eq!(reservations, 1);
    assert_eq!(fs::read(f.raw("stdout"))?, b"after!");
    Ok(())
}

impl LogsFixture {
    async fn new(
        policy: Option<bool>,
        stdout: &[u8],
        stderr: &[u8],
    ) -> Result<Self, Box<dyn Error>> {
        let (directory, database) = database().await?;
        let (project, job) = insert_project_job(&database).await?;
        let root = directory.path();
        let paths = RuntimePaths {
            config_file: root.join("config"),
            state_dir: root.join("state"),
            runtime_dir: root.join("runtime"),
            database: root.join("nested/igor.db"),
            log_dir: root.join("logs"),
            worktree_dir: root.join("worktrees"),
            report_dir: root.join("reports"),
            worker_socket: root.join("worker.sock"),
            supervisor_socket: root.join("supervisor.sock"),
            resource_dir: root.join("resources"),
        };
        let mut value = serde_json::to_value(attempt(&job, 1)?)?;
        value["result"]["compress_success_logs"] = json!(policy);
        let spec: AttemptSpec = serde_json::from_value(value)?;
        let attempt = spec.id();
        database
            .jobs()
            .insert_attempt_with_event(&spec, &event(EventKind::AttemptCreated, json!({}))?)
            .await?;
        let dir = paths.log_dir.join(attempt.to_string());
        fs::create_dir_all(&dir)?;
        fs::write(dir.join("stdout.log"), stdout)?;
        fs::write(dir.join("stderr.log"), stderr)?;
        sqlx::query("INSERT INTO attempt_processes (attempt_id, job_id, project_id, pid, process_group_id, process_start_ticks, stdout_path, stderr_path) VALUES (?, ?, ?, 2147483647, 2147483647, 1, ?, ?)")
            .bind(attempt.to_string()).bind(job.id.to_string()).bind(project.id.to_string())
            .bind(dir.join("stdout.log").to_str()).bind(dir.join("stderr.log").to_str()).execute(database.pool()).await?;
        sqlx::query("UPDATE attempts SET state = 'succeeded'")
            .execute(database.pool())
            .await?;
        sqlx::query("UPDATE jobs SET state = 'succeeded'")
            .execute(database.pool())
            .await?;
        Ok(Self {
            directory,
            database,
            paths,
            job,
            attempt,
        })
    }
    fn raw(&self, stream: &str) -> PathBuf {
        self.paths
            .log_dir
            .join(self.attempt.to_string())
            .join(format!("{stream}.log"))
    }
    async fn compress(&self) -> Result<usize, Box<dyn Error>> {
        Ok(self
            .database
            .jobs()
            .compress_success_logs(self.attempt, &self.paths)
            .await?)
    }
}

#[tokio::test]
async fn success_logs_binary_large_empty_roundtrip_and_uncompressed_offsets() -> TestResult {
    let binary: Vec<u8> = (0..=255).cycle().take(2_000_003).collect();
    let f = LogsFixture::new(Some(true), &binary, b"").await?;
    assert_eq!(
        f.database.jobs().success_log_candidates(None).await?,
        [f.attempt]
    );
    assert!(
        f.database
            .jobs()
            .success_log_candidates(Some(f.attempt))
            .await?
            .is_empty()
    );
    assert_eq!(f.compress().await?, 2);
    let logs = f.database.jobs().logs_for_job(f.job.id).await?;
    for (stream, archive, expected) in [
        ("stdout", logs.stdout_archive, binary.as_slice()),
        ("stderr", logs.stderr_archive, b"".as_slice()),
    ] {
        let archive = archive.ok_or("missing archive")?;
        assert!(!f.raw(stream).exists());
        assert_eq!(
            fs::metadata(&archive.path)?.permissions().mode() & 0o777,
            0o600
        );
        let mut output = Vec::new();
        assert_eq!(
            copy_log(&f.raw(stream), Some(&archive), 0, &mut output)?,
            expected.len() as u64
        );
        assert_eq!(output, expected);
        if !expected.is_empty() {
            output.clear();
            copy_log(&f.raw(stream), Some(&archive), 65539, &mut output)?;
            assert_eq!(output, expected[65539..]);
        }
        // Descriptors, never suffix inference.
        assert!(copy_log(&f.raw(stream), None, 0, &mut io::sink()).is_err());
    }
    assert_eq!(f.compress().await?, 0);
    assert!(
        f.database
            .jobs()
            .success_log_candidates(None)
            .await?
            .is_empty()
    );
    // Executor/extractor metadata is deliberately still the literal raw path.
    assert_eq!(
        f.database.jobs().logs_for_attempt(f.attempt).await?.stdout,
        f.raw("stdout")
    );
    let _ = &f.directory;
    Ok(())
}

#[tokio::test]
async fn success_logs_false_legacy_and_active_attempt_preserve_bytes() -> TestResult {
    for policy in [None, Some(false), Some(true)] {
        let f = LogsFixture::new(policy, b"raw\0\xff", b"stderr").await?;
        if policy == Some(true) {
            sqlx::query("UPDATE attempts SET state = 'running'")
                .execute(f.database.pool())
                .await?;
        }
        assert!(
            f.database
                .jobs()
                .success_log_candidates(None)
                .await?
                .is_empty()
        );
        assert_eq!(f.compress().await?, 0);
        assert_eq!(fs::read(f.raw("stdout"))?, b"raw\0\xff");
        assert!(!f.raw("stdout").with_extension("log.gz").exists());
    }
    Ok(())
}

#[tokio::test]
async fn success_logs_extraction_and_diagnosis_barriers() -> TestResult {
    let f = LogsFixture::new(Some(true), b"raw", b"error").await?;
    let action = ActionRecord {
        id: ActionId::new(),
        project_id: f.job.project_id,
        kind: "extract_metrics".into(),
        state: ActionState::Pending,
        spec: json!({}),
        idempotency_key: "archive-extract".into(),
    };
    f.database.actions().insert(&action).await?;
    sqlx::query("UPDATE actions SET attempt_id = ?")
        .bind(f.attempt.to_string())
        .execute(f.database.pool())
        .await?;
    for state in ["pending", "running"] {
        if state == "running" {
            f.database
                .actions()
                .claim_pending("archive-test", Duration::from_secs(30))
                .await?
                .ok_or("claim")?;
        }
        assert_eq!(f.compress().await?, 0);
        assert!(
            f.database
                .jobs()
                .success_log_candidates(None)
                .await?
                .is_empty()
        );
    }
    sqlx::query("UPDATE actions SET state = 'succeeded', claim_id = NULL, claim_owner = NULL, claim_expires_at = NULL").execute(f.database.pool()).await?;
    let artifact = ArtifactRecord {
        id: uuid::Uuid::new_v4(),
        attempt_id: f.attempt,
        path: "model.bin".into(),
        role: ArtifactRole::Model,
        sha256: Some(format!("sha256:{:x}", sha2::Sha256::digest(b"model"))),
        size_bytes: Some(5),
        state: igor_core::ArtifactPublicationState::Registered,
        lifecycle: igor_core::ArtifactLifecycleState::Failed,
        diagnosis_required: true,
        metadata: json!({}),
    };
    f.database.artifacts().insert(&artifact).await?;
    assert_eq!(f.compress().await?, 0);
    assert_eq!(fs::read(f.raw("stdout"))?, b"raw");
    assert!(
        f.database
            .jobs()
            .success_log_candidates(None)
            .await?
            .is_empty()
    );
    sqlx::query("INSERT INTO recoveries (id, attempt_id, state, diagnosis_json, idempotency_key) VALUES (?, ?, 'proposed', '{}', 'archive-diagnosis')")
        .bind(uuid::Uuid::new_v4().to_string()).bind(f.attempt.to_string()).execute(f.database.pool()).await?;
    assert_eq!(f.compress().await?, 2);
    Ok(())
}

#[tokio::test]
async fn success_logs_missing_symlink_and_incompatible_archive_preserve_raw() -> TestResult {
    for case in ["missing", "symlink", "ancestor", "corrupt", "conflict"] {
        let f = LogsFixture::new(Some(true), b"raw", b"error").await?;
        let raw = f.raw("stdout");
        match case {
            "missing" => fs::remove_file(&raw)?,
            "symlink" => {
                fs::remove_file(&raw)?;
                std::os::unix::fs::symlink(f.raw("stderr"), &raw)?;
            }
            "ancestor" => {
                let dir = raw.parent().ok_or("parent")?;
                let moved = f.paths.log_dir.join("moved");
                fs::rename(dir, &moved)?;
                std::os::unix::fs::symlink(&moved, dir)?;
            }
            "corrupt" => fs::write(raw.with_extension("log.gz"), b"not gzip")?,
            "conflict" => {
                let a = ArtifactRecord {
                    id: uuid::Uuid::new_v4(),
                    attempt_id: f.attempt,
                    path: "stdout.log.gz".into(),
                    role: ArtifactRole::Model,
                    sha256: Some(format!("sha256:{:x}", sha2::Sha256::digest(b"model"))),
                    size_bytes: Some(5),
                    state: igor_core::ArtifactPublicationState::Registered,
                    lifecycle: igor_core::ArtifactLifecycleState::Running,
                    diagnosis_required: false,
                    metadata: json!({}),
                };
                f.database.artifacts().insert(&a).await?;
            }
            _ => unreachable!(),
        }
        assert!(f.compress().await.is_err(), "{case}");
        assert_eq!(fs::read(f.raw("stderr"))?, b"error");
        if matches!(case, "corrupt" | "conflict" | "ancestor") {
            assert_eq!(fs::read(raw)?, b"raw");
        }
    }
    Ok(())
}

#[tokio::test]
async fn success_logs_restart_metadata_publication_and_event_rollback() -> TestResult {
    for phase in ["metadata", "publication", "retirement", "event"] {
        let mut f = LogsFixture::new(Some(true), b"raw\0\xff", b"error").await?;
        let dir = f.raw("stdout").parent().ok_or("parent")?.to_owned();
        let foreign = dir.join(format!(".igor-log-{}.tmp", uuid::Uuid::new_v4()));
        fs::write(&foreign, b"foreign")?;
        let sql = match phase {
            "metadata" => {
                "CREATE TRIGGER archive_fault BEFORE INSERT ON success_log_archives BEGIN SELECT RAISE(ABORT, 'fault'); END;"
            }
            "publication" => {
                "CREATE TRIGGER archive_fault BEFORE UPDATE OF state ON success_log_archives BEGIN SELECT RAISE(ABORT, 'fault'); END;"
            }
            "retirement" => {
                "CREATE TRIGGER archive_fault BEFORE UPDATE OF retired ON success_log_archives BEGIN SELECT RAISE(ABORT, 'fault'); END;"
            }
            _ => {
                "CREATE TRIGGER archive_fault BEFORE INSERT ON events WHEN NEW.kind = 'cleanup_state_changed' BEGIN SELECT RAISE(ABORT, 'fault'); END;"
            }
        };
        sqlx::raw_sql(sql).execute(f.database.pool()).await?;
        assert!(f.compress().await.is_err());
        if phase == "metadata" {
            for _ in 0..3 {
                assert!(f.compress().await.is_err());
                let temporaries: Vec<_> = fs::read_dir(f.raw("stdout").parent().ok_or("parent")?)?
                    .collect::<Result<Vec<_>, _>>()?
                    .into_iter()
                    .filter(|e| {
                        e.path() != foreign
                            && e.file_name().to_string_lossy().starts_with(".igor-log-")
                    })
                    .collect();
                assert!(temporaries.is_empty());
                let reservations: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM success_log_archive_preparations WHERE attempt_id = ? AND stream = 'stdout'")
                    .bind(f.attempt.to_string()).fetch_one(f.database.pool()).await?;
                assert_eq!(reservations, 1);
                assert_eq!(fs::read(f.raw("stdout"))?, b"raw\0\xff");
            }
        }
        assert_eq!(
            f.raw("stdout").exists(),
            !matches!(phase, "retirement" | "event")
        );
        // Published metadata, retirement and evidence are all still pending:
        // the failed transaction left only its durable phase behind.
        let durable = durable_phase(&f).await?;
        assert_eq!(
            durable,
            match phase {
                "metadata" => (0, 0, 0, 1, 0, 0, false),
                "publication" => (1, 0, 0, 1, 0, 0, true),
                _ => (1, 1, 0, 1, 0, 0, true),
            },
            "{phase}"
        );
        assert_eq!(fs::read(&foreign)?, b"foreign");
        let plan =
            gc_support::readonly_plan(&f.database, &f.paths, f.directory.path(), None).await?;
        assert_eq!(
            plan.entries[0].status,
            if matches!(phase, "retirement" | "event") {
                igor_core::GcStatus::MetadataOnly
            } else {
                igor_core::GcStatus::Ready
            }
        );
        assert_eq!(plan.entries[1].status, igor_core::GcStatus::Ready);
        // Close before dropping the faulting trigger: only the restarted handle
        // may observe the durable phase and then resume it.
        f.database.pool().close().await;
        f.database = Database::open(&f.paths.database).await?;
        assert_eq!(durable_phase(&f).await?, durable, "{phase}");
        assert_eq!(fs::read(&foreign)?, b"foreign");
        sqlx::query("DROP TRIGGER archive_fault")
            .execute(f.database.pool())
            .await?;
        assert_eq!(f.compress().await?, 2);
        let logs = f.database.jobs().logs_for_job(f.job.id).await?;
        let archive = logs.stdout_archive.ok_or("archive")?;
        let mut output = Vec::new();
        copy_log(&f.raw("stdout"), Some(&archive), 0, &mut output)?;
        assert_eq!(output, b"raw\0\xff");
        let subject = format!("{}:stdout", f.attempt);
        let tombstone = f
            .database
            .cleanup_tombstone("success_log_compression", &subject)
            .await?
            .ok_or("archive tombstone")?;
        assert_eq!(tombstone["outcome"], "raw_retired");
        assert_eq!(f.compress().await?, 0);
        assert_eq!(
            f.database
                .cleanup_tombstone("success_log_compression", &subject)
                .await?,
            Some(tombstone)
        );
        assert_eq!(
            durable_phase(&f).await?,
            (1, 1, 1, 1, 2, 2, true),
            "{phase}"
        );
        assert_eq!(fs::read(&foreign)?, b"foreign");
        assert!(
            sqlx::query(
                "UPDATE success_log_archives SET identity_json = '{}' WHERE attempt_id = ?"
            )
            .bind(f.attempt.to_string())
            .execute(f.database.pool())
            .await
            .is_err()
        );
    }
    Ok(())
}

#[tokio::test]
async fn success_logs_published_missing_or_corrupt_archive_blocks_retirement_and_read() -> TestResult
{
    for corrupt in [false, true] {
        let f = LogsFixture::new(Some(true), b"raw", b"error").await?;
        sqlx::raw_sql("CREATE TRIGGER archive_fault BEFORE UPDATE OF state ON success_log_archives BEGIN SELECT RAISE(ABORT, 'fault'); END;").execute(f.database.pool()).await?;
        assert!(f.compress().await.is_err());
        sqlx::query("DROP TRIGGER archive_fault")
            .execute(f.database.pool())
            .await?;
        let archive = f.raw("stdout").with_extension("log.gz");
        if corrupt {
            fs::write(&archive, b"corrupt")?;
        } else {
            fs::remove_file(&archive)?;
        }
        assert!(f.compress().await.is_err());
        assert_eq!(fs::read(f.raw("stdout"))?, b"raw");
    }
    let f = LogsFixture::new(Some(true), b"raw", b"error").await?;
    f.compress().await?;
    let archive = f
        .database
        .jobs()
        .logs_for_job(f.job.id)
        .await?
        .stdout_archive
        .ok_or("archive")?;
    fs::write(&archive.path, b"corrupt")?;
    assert!(copy_log(&f.raw("stdout"), Some(&archive), 0, &mut io::sink()).is_err());
    // Even a descriptor matching the compressed file must reject a broken gzip
    // trailer, rather than silently returning the bytes emitted before corruption.
    let f = LogsFixture::new(Some(true), b"raw", b"error").await?;
    f.compress().await?;
    let mut archive = f
        .database
        .jobs()
        .logs_for_job(f.job.id)
        .await?
        .stdout_archive
        .ok_or("archive")?;
    let file = fs::OpenOptions::new().write(true).open(&archive.path)?;
    file.set_len(archive.archive_size - 1)?;
    archive.archive_size -= 1;
    archive.archive_sha256 = format!("{:x}", sha2::Sha256::digest(fs::read(&archive.path)?));
    assert!(copy_log(&f.raw("stdout"), Some(&archive), 0, &mut io::sink()).is_err());
    Ok(())
}

#[tokio::test]
async fn success_logs_follow_transition_and_bounded_stream_writes() -> TestResult {
    struct BoundedWriter {
        count: u64,
    }
    impl io::Write for BoundedWriter {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            assert!(bytes.len() <= 65536, "unbounded output buffer");
            self.count += bytes.len() as u64;
            Ok(bytes.len())
        }
        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }
    let binary: Vec<u8> = (0..=255).cycle().take(3_000_007).collect();
    let prefix = 1013;
    let f = LogsFixture::new(Some(true), &binary[..prefix], b"").await?;
    let mut output = Vec::new();
    let offset = copy_log(&f.raw("stdout"), None, 0, &mut output)?;
    fs::write(f.raw("stdout"), &binary)?;
    f.compress().await?;
    let archive = f
        .database
        .jobs()
        .logs_for_job(f.job.id)
        .await?
        .stdout_archive
        .ok_or("archive")?;
    copy_log(&f.raw("stdout"), Some(&archive), offset, &mut output)?;
    assert_eq!(output, binary);
    let mut bounded = BoundedWriter { count: 0 };
    copy_log(&f.raw("stdout"), Some(&archive), 0, &mut bounded)?;
    assert_eq!(bounded.count, binary.len() as u64);
    let mut undersized = archive.clone();
    undersized.original_size = 10;
    assert!(copy_log(&f.raw("stdout"), Some(&undersized), 0, &mut io::sink()).is_err());
    Ok(())
}

#[tokio::test]
async fn success_logs_candidate_keyset_is_bounded_and_does_not_starve_later_attempts() -> TestResult
{
    let f = LogsFixture::new(Some(true), b"raw", b"error").await?;
    for _ in 0..39 {
        let job = job(f.job.project_id);
        f.database
            .jobs()
            .insert_job_with_event(&job, 10, &event(EventKind::JobSubmitted, json!({}))?)
            .await?;
        let mut value = serde_json::to_value(attempt(&job, 1)?)?;
        value["result"]["compress_success_logs"] = json!(true);
        let spec: AttemptSpec = serde_json::from_value(value)?;
        f.database
            .jobs()
            .insert_attempt_with_event(&spec, &event(EventKind::AttemptCreated, json!({}))?)
            .await?;
    }
    sqlx::query("UPDATE attempts SET state = 'succeeded'")
        .execute(f.database.pool())
        .await?;
    let first = f.database.jobs().success_log_candidates(None).await?;
    assert_eq!(first.len(), 32);
    let second = f
        .database
        .jobs()
        .success_log_candidates(first.last().copied())
        .await?;
    assert_eq!(second.len(), 8);
    let mut all = first;
    all.extend(second);
    assert!(
        all.windows(2)
            .all(|pair| pair[0].to_string() < pair[1].to_string())
    );
    Ok(())
}

#[tokio::test]
async fn success_logs_prepared_missing_raw_is_not_accepted() -> TestResult {
    let f = LogsFixture::new(Some(true), b"raw", b"error").await?;
    sqlx::raw_sql("CREATE TRIGGER archive_fault BEFORE UPDATE OF state ON success_log_archives BEGIN SELECT RAISE(ABORT, 'fault'); END;").execute(f.database.pool()).await?;
    assert!(f.compress().await.is_err());
    sqlx::query("DROP TRIGGER archive_fault")
        .execute(f.database.pool())
        .await?;
    fs::remove_file(f.raw("stdout"))?;
    assert!(f.compress().await.is_err());
    assert!(
        f.database
            .jobs()
            .logs_for_job(f.job.id)
            .await?
            .stdout_archive
            .is_none()
    );
    assert_eq!(fs::read(f.raw("stderr"))?, b"error");
    Ok(())
}

#[tokio::test]
async fn success_logs_container_lookup_and_reserved_extractor_names() -> TestResult {
    let f = LogsFixture::new(Some(true), b"container\0", b"stderr\xff").await?;
    sqlx::query("INSERT INTO attempt_containers (attempt_id, job_id, project_id, container_id, container_name, image_reference, image_id, state, removed_at, stdout_path, stderr_path)
        SELECT attempt_id, job_id, project_id, 'fixture-container', 'igor-' || attempt_id, 'fixture:latest', 'image', 'removed', strftime('%Y-%m-%dT%H:%M:%fZ', 'now'), stdout_path, stderr_path FROM attempt_processes")
        .execute(f.database.pool()).await?;
    sqlx::query("DELETE FROM attempt_processes")
        .execute(f.database.pool())
        .await?;
    let logs = f.database.jobs().logs_for_job(f.job.id).await?;
    assert_eq!(logs.stdout_path.as_ref(), Some(&f.raw("stdout")));
    let action = ActionRecord {
        id: ActionId::new(),
        project_id: f.job.project_id,
        kind: "extract_metrics".into(),
        state: ActionState::Pending,
        spec: json!({}),
        idempotency_key: "reserved-names".into(),
    };
    f.database.actions().insert(&action).await?;
    let claim = f
        .database
        .actions()
        .claim_pending("reserved-test", Duration::from_secs(30))
        .await?
        .ok_or("claim")?;
    for name in [
        "stdout.log",
        "stderr.log",
        "stdout.log.gz",
        "stderr.log.gz",
        "nested/stdout.log.gz",
    ] {
        assert!(matches!(
            f.database
                .artifacts()
                .register_claimed(
                    &claim,
                    f.attempt,
                    f.raw("stdout").parent().ok_or("parent")?,
                    &json!({"path":name,"role":"model"})
                )
                .await,
            Err(PersistenceError::Conflict {
                entity: "reserved log artifact path"
            })
        ));
    }
    assert_eq!(f.compress().await?, 2);
    let archive = f
        .database
        .jobs()
        .logs_for_job(f.job.id)
        .await?
        .stdout_archive
        .ok_or("container archive")?;
    let mut output = Vec::new();
    copy_log(&f.raw("stdout"), Some(&archive), 0, &mut output)?;
    assert_eq!(output, b"container\0");
    Ok(())
}
