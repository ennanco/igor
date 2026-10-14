use std::{
    fs::{File, OpenOptions},
    io::{self, Read, Seek, SeekFrom, Write},
    os::unix::fs::{MetadataExt, OpenOptionsExt},
    path::{Component, Path, PathBuf},
};

use flate2::{Compression, read::MultiGzDecoder, write::GzEncoder};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use sqlx::{Row, Sqlite, Transaction};

use super::{
    JobAttemptRepository, PersistenceError, PersistenceResult, db, from_json, insert_event,
    parse_id,
};
use crate::{AttemptId, AttemptSpec, Event, EventId, EventKind, EventPayload, RuntimePaths};

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct LogArchive {
    pub path: PathBuf,
    pub temporary_path: PathBuf,
    pub original_path: PathBuf,
    pub original_size: u64,
    pub original_sha256: String,
    pub archive_size: u64,
    pub archive_sha256: String,
    pub codec_version: u32,
}

fn conflict() -> PersistenceError {
    PersistenceError::Conflict {
        entity: "success log archive",
    }
}

// Walk all ancestors from /, including the trusted root. Missing is distinct from unsafe.
pub(super) fn existing(path: &Path) -> PersistenceResult<bool> {
    if !path.is_absolute()
        || path
            .to_string_lossy()
            .split('/')
            .any(|c| c == "." || c == "..")
    {
        return Err(conflict());
    }
    let mut walked = PathBuf::new();
    for part in path.components() {
        if !matches!(part, Component::RootDir | Component::Normal(_)) {
            return Err(conflict());
        }
        walked.push(part);
        let m = match std::fs::symlink_metadata(&walked) {
            Ok(m) => m,
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(false),
            Err(e) => return Err(e.into()),
        };
        if m.file_type().is_symlink()
            || (walked == path && !m.is_file())
            || (walked != path && !m.is_dir())
        {
            return Err(conflict());
        }
    }
    Ok(true)
}

pub(super) fn digest(mut input: impl Read) -> io::Result<(u64, String)> {
    let mut hash = Sha256::new();
    let mut size = 0;
    let mut buffer = [0; 65536];
    loop {
        let n = input.read(&mut buffer)?;
        if n == 0 {
            break;
        }
        size += n as u64;
        hash.update(&buffer[..n]);
    }
    Ok((size, format!("{:x}", hash.finalize())))
}

fn verify(archive: &LogArchive, require_raw: bool) -> PersistenceResult<bool> {
    if archive.codec_version != 1 || !existing(&archive.path)? {
        return Err(conflict());
    }
    if digest(File::open(&archive.path)?)? != (archive.archive_size, archive.archive_sha256.clone())
    {
        return Err(conflict());
    }
    let decoded = MultiGzDecoder::new(File::open(&archive.path)?)
        .take(archive.original_size.checked_add(1).ok_or_else(conflict)?);
    if digest(decoded)? != (archive.original_size, archive.original_sha256.clone()) {
        return Err(conflict());
    }
    let raw = existing(&archive.original_path)?;
    if raw {
        if digest(File::open(&archive.original_path)?)?
            != (archive.original_size, archive.original_sha256.clone())
        {
            return Err(conflict());
        }
    } else if require_raw {
        return Err(conflict());
    }
    Ok(raw)
}

// A leader's exit does not close descriptors inherited by its group children.
// Ignore dead entries (including unreaped zombies), but fail closed on proc errors.
pub(super) fn group_quiescent(group: i64) -> PersistenceResult<bool> {
    if group <= 0 {
        return Err(conflict());
    }
    for entry in std::fs::read_dir("/proc")? {
        let entry = entry?;
        if entry.file_name().to_string_lossy().parse::<u32>().is_err() {
            continue;
        }
        let stat = match std::fs::read_to_string(entry.path().join("stat")) {
            Ok(stat) => stat,
            Err(e) if e.kind() == io::ErrorKind::NotFound => continue,
            Err(e) => return Err(e.into()),
        };
        let (_, fields) = stat.rsplit_once(')').ok_or_else(conflict)?;
        let mut fields = fields.split_whitespace();
        let state = fields.next().ok_or_else(conflict)?;
        fields.next().ok_or_else(conflict)?; // ppid
        let pgid = fields
            .next()
            .ok_or_else(conflict)?
            .parse::<i64>()
            .map_err(|_| conflict())?;
        if pgid == group && !matches!(state, "Z" | "X" | "x") {
            return Ok(false);
        }
    }
    Ok(true)
}

fn validate_temporary(original: &Path, temporary: &Path) -> PersistenceResult<()> {
    if temporary.parent() != original.parent()
        || !temporary
            .file_name()
            .and_then(|n| n.to_str())
            .and_then(|n| n.strip_prefix(".igor-log-"))
            .and_then(|n| n.strip_suffix(".tmp"))
            .is_some_and(|id| uuid::Uuid::parse_str(id).is_ok())
    {
        return Err(conflict());
    }
    Ok(())
}

fn validate_reserved_file(temporary: &Path) -> PersistenceResult<()> {
    if existing(temporary)? {
        let metadata = std::fs::symlink_metadata(temporary)?;
        if metadata.uid() != std::fs::metadata("/proc/self")?.uid()
            || metadata.mode() & 0o777 != 0o600
            || metadata.nlink() != 1
        {
            return Err(conflict());
        }
    }
    Ok(())
}

fn prepare(original_path: PathBuf, temporary: PathBuf) -> PersistenceResult<LogArchive> {
    if !existing(&original_path)? {
        return Err(conflict());
    }
    let path = original_path.with_extension("log.gz");
    let (original_size, original_sha256) = digest(File::open(&original_path)?)?;
    // An unowned pre-existing final name is a declaration conflict, even if its
    // bytes happen to match. Only durable prepared metadata can authorize replay.
    if existing(&path)? {
        return Err(conflict());
    }
    let parent = original_path.parent().ok_or_else(conflict)?;
    validate_temporary(&original_path, &temporary)?;
    // This exact path was committed before creation, and no prepared identity
    // exists. Reclaim only that reservation's partial file after a crash.
    if existing(&temporary)? {
        validate_reserved_file(&temporary)?;
        std::fs::remove_file(&temporary)?;
        File::open(parent)?.sync_all()?;
    }
    let mut created = false;
    let result = (|| -> PersistenceResult<()> {
        let file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(&temporary)?;
        created = true;
        let mut encoder = GzEncoder::new(file, Compression::default());
        io::copy(&mut File::open(&original_path)?, &mut encoder)?;
        encoder.finish()?.sync_all()?;
        File::open(parent)?.sync_all()?;
        Ok(())
    })();
    // Only this invocation's private temporary can be removed.
    if result.is_err() && created {
        let _ = std::fs::remove_file(&temporary);
    }
    result?;
    let (archive_size, archive_sha256) = digest(File::open(&temporary)?)?;
    let archive = LogArchive {
        path,
        temporary_path: temporary.clone(),
        original_path,
        original_size,
        original_sha256,
        archive_size,
        archive_sha256,
        codec_version: 1,
    };
    let mut temporary_archive = archive.clone();
    temporary_archive.path = temporary;
    verify(&temporary_archive, true)?;
    Ok(archive)
}

fn publish(archive: &LogArchive) -> PersistenceResult<()> {
    if existing(&archive.path)? {
        verify(archive, true)?;
    } else {
        let mut temporary_archive = archive.clone();
        temporary_archive.path = archive.temporary_path.clone();
        verify(&temporary_archive, true)?;
        nix::fcntl::renameat2(
            nix::fcntl::AT_FDCWD,
            &archive.temporary_path,
            nix::fcntl::AT_FDCWD,
            &archive.path,
            nix::fcntl::RenameFlags::RENAME_NOREPLACE,
        )
        .map_err(|e| io::Error::from_raw_os_error(e as i32))?;
    }
    File::open(archive.path.parent().ok_or_else(conflict)?)?.sync_all()?;
    Ok(())
}

async fn archive_state(
    tx: &mut Transaction<'_, Sqlite>,
    attempt_id: AttemptId,
    stream: &str,
    expected: &LogArchive,
) -> PersistenceResult<(bool, bool)> {
    let row = sqlx::query("SELECT identity_json, state, retired FROM success_log_archives WHERE attempt_id = ? AND stream = ?")
        .bind(attempt_id.to_string()).bind(stream).fetch_optional(&mut **tx).await.map_err(|e| db("recheck durable log archive", e))?.ok_or_else(conflict)?;
    let identity: LogArchive = from_json(row.get("identity_json"), "durable archive identity")?;
    if &identity != expected {
        return Err(conflict());
    }
    Ok((
        row.get::<&str, _>("state") == "published",
        row.get::<i64, _>("retired") == 1,
    ))
}

/// Raw logs remain preferred. Archives are selected only by persisted metadata.
/// Offsets and the return value always count uncompressed bytes.
pub fn copy_log(
    raw: &Path,
    archive: Option<&LogArchive>,
    offset: u64,
    output: &mut impl Write,
) -> io::Result<u64> {
    match File::open(raw) {
        Ok(mut file) => {
            file.seek(SeekFrom::Start(offset))?;
            io::copy(&mut file, output)
        }
        Err(e) if e.kind() == io::ErrorKind::NotFound => {
            let archive = archive.ok_or(e)?;
            if archive.original_path != raw || archive.codec_version != 1 {
                return Err(io::Error::other("invalid log archive descriptor"));
            }
            verify(archive, false).map_err(io::Error::other)?;
            let mut decoder = MultiGzDecoder::new(File::open(&archive.path)?);
            let skipped = io::copy(&mut (&mut decoder).take(offset), &mut io::sink())?;
            if skipped != offset {
                return Err(io::Error::other("log offset exceeds archive"));
            }
            io::copy(&mut decoder, output)
        }
        Err(e) => Err(e),
    }
}

impl JobAttemptRepository<'_> {
    pub async fn log_archive(
        &self,
        attempt: AttemptId,
        stream: &str,
    ) -> PersistenceResult<Option<LogArchive>> {
        let value: Option<String> = sqlx::query_scalar("SELECT identity_json FROM success_log_archives WHERE attempt_id = ? AND stream = ? AND state = 'published'")
            .bind(attempt.to_string()).bind(stream).fetch_optional(&self.database.pool).await.map_err(|e| db("read log archive", e))?;
        value.map(|v| from_json(&v, "log archive")).transpose()
    }

    pub async fn success_log_candidates(
        &self,
        after: Option<AttemptId>,
    ) -> PersistenceResult<Vec<AttemptId>> {
        let values: Vec<String> = sqlx::query_scalar("SELECT a.id FROM attempts a WHERE a.id > COALESCE(?, '') AND a.state = 'succeeded'
            AND json_extract(a.spec_json, '$.result.compress_success_logs') = 1
            AND NOT EXISTS (SELECT 1 FROM actions WHERE attempt_id = a.id AND kind = 'extract_metrics' AND state IN ('pending', 'running'))
            AND (NOT EXISTS (SELECT 1 FROM artifacts WHERE attempt_id = a.id AND diagnosis_required = 1) OR EXISTS (SELECT 1 FROM diagnosed_attempts WHERE attempt_id = a.id))
            AND (SELECT COUNT(*) FROM success_log_archives WHERE attempt_id = a.id AND retired = 1) < 2 ORDER BY a.id LIMIT 32")
            .bind(after.map(|v| v.to_string())).fetch_all(&self.database.pool).await.map_err(|e| db("find success log candidates", e))?;
        values
            .into_iter()
            .map(|v| parse_id(&v, "log attempt"))
            .collect()
    }

    async fn archive_writer(
        &self,
        attempt_id: AttemptId,
        paths: &RuntimePaths,
    ) -> PersistenceResult<Option<Transaction<'static, Sqlite>>> {
        let mut tx = self
            .database
            .pool
            .begin()
            .await
            .map_err(|e| db("begin log archive", e))?;
        sqlx::query("UPDATE attempts SET state = state WHERE id = ?")
            .bind(attempt_id.to_string())
            .execute(&mut *tx)
            .await
            .map_err(|e| db("reserve log archive writer", e))?;
        let row = archive_eligibility(&mut tx, attempt_id).await?;
        let spec: AttemptSpec = from_json(row.get("spec_json"), "archive attempt")?;
        if row.get::<&str, _>("state") != "succeeded"
            || row.get::<i64, _>("registered") != 1
            || spec.result().compress_success_logs != Some(true)
            || row.get::<i64, _>("extracted") == 0
            || row.get::<i64, _>("diagnosed") == 0
        {
            return Ok(None);
        }
        if raw_consumers(&mut tx, attempt_id).await? {
            return Ok(None);
        }
        check_archive_registration(&mut tx, attempt_id, paths, &row).await?;
        let group: Option<i64> = row.get("process_group_id");
        // Keep the writer reservation owned by the blocking worker throughout the lifetime check.
        let (tx, quiescent) = tokio::task::spawn_blocking(move || {
            let result = group.map(group_quiescent).transpose();
            (tx, result)
        })
        .await?;
        if quiescent? == Some(false) {
            return Ok(None);
        }
        Ok(Some(tx))
    }

    pub async fn compress_success_logs(
        &self,
        attempt_id: AttemptId,
        paths: &RuntimePaths,
    ) -> PersistenceResult<usize> {
        self.compress_success_logs_selected(attempt_id, paths, None)
            .await
    }

    pub(super) async fn compress_success_logs_selected(
        &self,
        attempt_id: AttemptId,
        paths: &RuntimePaths,
        selected: Option<&[super::GcEntry]>,
    ) -> PersistenceResult<usize> {
        let mut retired = 0;
        for stream in ["stdout", "stderr"] {
            let stream_selection = selected.map(|s| {
                s.iter()
                    .filter(|e| e.subject_id == format!("{attempt_id}:{stream}"))
                    .cloned()
                    .collect::<Vec<_>>()
            });
            if stream_selection.as_ref().is_some_and(Vec::is_empty) {
                continue;
            }
            let selected = stream_selection.as_deref();
            let Some(mut tx) = self.archive_writer(attempt_id, paths).await? else {
                return Ok(retired);
            };
            if !super::gc::validate_selection(&mut tx, paths, selected, true).await? {
                continue;
            }
            let original = paths
                .log_dir
                .join(attempt_id.to_string())
                .join(format!("{stream}.log"));
            let mut row = sqlx::query("SELECT identity_json, state, retired FROM success_log_archives WHERE attempt_id = ? AND stream = ?").bind(attempt_id.to_string()).bind(stream).fetch_optional(&mut *tx).await.map_err(|e| db("read archive preparation", e))?;
            let mut temporary = None;
            if row.is_none() {
                let candidate = original
                    .parent()
                    .ok_or_else(conflict)?
                    .join(format!(".igor-log-{}.tmp", uuid::Uuid::new_v4()));
                sqlx::query("INSERT INTO success_log_archive_preparations (attempt_id, stream, raw_path, temp_path) VALUES (?, ?, ?, ?) ON CONFLICT (attempt_id, stream) DO NOTHING")
                    .bind(attempt_id.to_string()).bind(stream).bind(original.to_str().ok_or_else(conflict)?).bind(candidate.to_str().ok_or_else(conflict)?)
                    .execute(&mut *tx).await.map_err(|e| db("reserve archive temporary", e))?;
                tx.commit()
                    .await
                    .map_err(|e| db("commit archive reservation", e))?;
                let Some(writer) = self.archive_writer(attempt_id, paths).await? else {
                    return Ok(retired);
                };
                tx = writer;
                if !super::gc::validate_selection(&mut tx, paths, selected, false).await? {
                    continue;
                }
                row = sqlx::query("SELECT identity_json, state, retired FROM success_log_archives WHERE attempt_id = ? AND stream = ?").bind(attempt_id.to_string()).bind(stream).fetch_optional(&mut *tx).await.map_err(|e| db("recheck archive preparation", e))?;
                let reservation = sqlx::query("SELECT raw_path, temp_path FROM success_log_archive_preparations WHERE attempt_id = ? AND stream = ?")
                    .bind(attempt_id.to_string()).bind(stream).fetch_one(&mut *tx).await.map_err(|e| db("read archive reservation", e))?;
                if Path::new(reservation.get::<&str, _>("raw_path")) != original {
                    return Err(conflict());
                }
                let reserved = PathBuf::from(reservation.get::<String, _>("temp_path"));
                validate_temporary(&original, &reserved)?;
                temporary = Some(reserved);
            }
            if row
                .as_ref()
                .is_some_and(|r| r.get::<i64, _>("retired") == 1)
            {
                continue;
            }
            let prior: Option<LogArchive> = row
                .as_ref()
                .map(|r| from_json(r.get("identity_json"), "archive identity"))
                .transpose()?;
            if prior.as_ref().is_some_and(|a| {
                a.original_path != original
                    || a.path != original.with_extension("log.gz")
                    || validate_temporary(&original, &a.temporary_path).is_err()
            }) {
                return Err(conflict());
            }
            let published = row
                .as_ref()
                .is_some_and(|r| r.get::<&str, _>("state") == "published");
            let (worker_tx, result) = tokio::task::spawn_blocking(move || {
                let result = (|| -> PersistenceResult<LogArchive> {
                    match prior {
                        Some(a) => {
                            let mut copy = a.clone();
                            if !published && !existing(&a.path)? {
                                copy.path = a.temporary_path.clone();
                            }
                            verify(&copy, !published).map(|_| a)
                        }
                        None => prepare(original, temporary.ok_or_else(conflict)?),
                    }
                })();
                (tx, result)
            })
            .await?;
            tx = worker_tx;
            let archive = result?;
            if row.is_none() {
                let inserted = sqlx::query("INSERT INTO success_log_archives (attempt_id, stream, identity_json, state) VALUES (?, ?, ?, 'prepared')").bind(attempt_id.to_string()).bind(stream).bind(serde_json::to_string(&archive).map_err(|_| conflict())?).execute(&mut *tx).await;
                if let Err(error) = inserted {
                    let mut private = archive.clone();
                    private.path = archive.temporary_path.clone();
                    // No archive identity was inserted. Remove only the verified
                    // file produced here, retaining the durable reservation for
                    // cancellation/crash or cleanup failure recovery.
                    tokio::task::spawn_blocking(move || {
                        let _writer = tx;
                        if verify(&private, true).is_ok() {
                            let _ = std::fs::remove_file(&private.path);
                            if let Some(parent) = private.path.parent() {
                                let _ = File::open(parent).and_then(|f| f.sync_all());
                            }
                        }
                    })
                    .await?;
                    return Err(db("prepare log archive", error));
                }
            }
            tx.commit()
                .await
                .map_err(|e| db("commit archive identity", e))?;
            let Some(mut tx) = self.archive_writer(attempt_id, paths).await? else {
                return Ok(retired);
            };
            let (published, already_retired) =
                archive_state(&mut tx, attempt_id, stream, &archive).await?;
            if !super::gc::validate_selection(&mut tx, paths, selected, false).await? {
                continue;
            }
            if already_retired {
                continue;
            }
            let publication = archive.clone();
            let (mut tx, result) = tokio::task::spawn_blocking(move || {
                let result = if published {
                    verify(&publication, false).map(|_| ())
                } else {
                    publish(&publication)
                };
                (tx, result)
            })
            .await?;
            result?;
            sqlx::query("UPDATE success_log_archives SET state = 'published' WHERE attempt_id = ? AND stream = ?").bind(attempt_id.to_string()).bind(stream).execute(&mut *tx).await.map_err(|e| db("publish log archive", e))?;
            tx.commit()
                .await
                .map_err(|e| db("commit archive publication", e))?;
            let Some(mut tx) = self.archive_writer(attempt_id, paths).await? else {
                return Ok(retired);
            };
            let (published, already_retired) =
                archive_state(&mut tx, attempt_id, stream, &archive).await?;
            if !super::gc::validate_selection(&mut tx, paths, selected, false).await? {
                continue;
            }
            if !published {
                return Err(conflict());
            }
            if already_retired {
                continue;
            }
            // The blocking worker owns the reserved writer. Cancelling its async
            // caller cannot release the eligibility fence while filesystem I/O runs.
            let tombstone_identity = serde_json::json!({"archive":archive});
            let (mut tx, result) = tokio::task::spawn_blocking(move || {
                let result = (|| -> PersistenceResult<()> {
                    if verify(&archive, false)? {
                        std::fs::remove_file(&archive.original_path)?;
                    }
                    File::open(archive.path.parent().ok_or_else(conflict)?)?.sync_all()?;
                    Ok(())
                })();
                (tx, result)
            })
            .await?;
            result?;
            super::cleanup_evidence::record(
                &mut tx,
                "success_log_compression",
                &format!("{attempt_id}:{stream}"),
                "raw_retired",
                &tombstone_identity,
                None,
                None,
            )
            .await?;
            sqlx::query(
                "UPDATE success_log_archives SET retired = 1 WHERE attempt_id = ? AND stream = ?",
            )
            .bind(attempt_id.to_string())
            .bind(stream)
            .execute(&mut *tx)
            .await
            .map_err(|e| db("retire raw log", e))?;
            let (project, job): (String, String) =
                sqlx::query_as("SELECT project_id, job_id FROM attempts WHERE id = ?")
                    .bind(attempt_id.to_string())
                    .fetch_one(&mut *tx)
                    .await
                    .map_err(|e| db("read archive event context", e))?;
            let event = Event::new(
                EventId::new(),
                EventKind::CleanupStateChanged,
                EventPayload::new(
                    EventKind::CleanupStateChanged,
                    1,
                    serde_json::json!({"attempt_id":attempt_id,"stream":stream,"state":"raw_retired","reason":"success_log_compression"}),
                )?,
            )?;
            insert_event(
                &mut tx,
                parse_id(&project, "archive project")?,
                Some(parse_id(&job, "archive job")?),
                Some(attempt_id),
                &event,
            )
            .await?;
            tx.commit()
                .await
                .map_err(|e| db("commit raw log retirement", e))?;
            retired += 1;
        }
        Ok(retired)
    }
}

pub(super) fn reserved_log_name(path: &Path) -> bool {
    path.file_name().is_some_and(|n| {
        matches!(
            n.to_str(),
            Some("stdout.log" | "stderr.log" | "stdout.log.gz" | "stderr.log.gz")
        )
    })
}

pub(super) async fn archive_eligibility(
    tx: &mut Transaction<'_, Sqlite>,
    attempt_id: AttemptId,
) -> PersistenceResult<sqlx::sqlite::SqliteRow> {
    sqlx::query("SELECT a.spec_json, a.state, a.project_id, registry.registered,
        p.process_group_id, u.attempt_id AS unit_id, c.attempt_id AS container_id,
        COALESCE(u.stdout_path, p.stdout_path, c.stdout_path) AS stdout,
        COALESCE(u.stderr_path, p.stderr_path, c.stderr_path) AS stderr,
        NOT EXISTS (SELECT 1 FROM actions WHERE attempt_id = a.id AND kind = 'extract_metrics' AND state IN ('pending', 'running')) AS extracted,
        (NOT EXISTS (SELECT 1 FROM artifacts WHERE attempt_id = a.id AND diagnosis_required = 1) OR EXISTS (SELECT 1 FROM diagnosed_attempts WHERE attempt_id = a.id)) AS diagnosed
        FROM attempts a JOIN projects registry ON registry.id = a.project_id LEFT JOIN attempt_units u ON u.attempt_id = a.id LEFT JOIN attempt_processes p ON p.attempt_id = a.id LEFT JOIN attempt_containers c ON c.attempt_id = a.id WHERE a.id = ?")
        .bind(attempt_id.to_string()).fetch_optional(&mut **tx).await.map_err(|e| db("check log archive eligibility", e))?.ok_or_else(conflict)
}

pub(super) async fn raw_consumers(
    tx: &mut Transaction<'_, Sqlite>,
    attempt_id: AttemptId,
) -> PersistenceResult<bool> {
    let needed: i64 = sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM recoveries r WHERE r.attempt_id = ? AND r.state IN ('pending', 'diagnosing', 'applying'))
        OR EXISTS (SELECT 1 FROM actions a JOIN attempts t ON t.id = ? WHERE a.kind = 'diagnose_or_repair' AND a.state IN ('pending', 'running') AND (a.attempt_id = t.id OR (a.attempt_id IS NULL AND (a.job_id = t.job_id OR (a.job_id IS NULL AND a.project_id = t.project_id)))))
        OR EXISTS (SELECT 1 FROM agent_sessions s JOIN recoveries r ON r.id = s.recovery_id WHERE r.attempt_id = ? AND (s.cleanup_state IN ('pending', 'running') OR s.lease_id IS NOT NULL OR s.lease_owner IS NOT NULL OR s.lease_expires_at IS NOT NULL))")
        .bind(attempt_id.to_string()).bind(attempt_id.to_string()).bind(attempt_id.to_string()).fetch_one(&mut **tx).await.map_err(|e| db("check raw log consumers", e))?;
    Ok(needed != 0)
}

pub(super) async fn check_archive_registration(
    tx: &mut Transaction<'_, Sqlite>,
    attempt_id: AttemptId,
    paths: &RuntimePaths,
    row: &sqlx::sqlite::SqliteRow,
) -> PersistenceResult<()> {
    let dir = paths.log_dir.join(attempt_id.to_string());
    for stream in ["stdout", "stderr"] {
        if row.get::<Option<String>, _>(stream).as_deref()
            != dir.join(format!("{stream}.log")).to_str()
        {
            return Err(conflict());
        }
    }
    let collision: i64 = sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM artifacts WHERE attempt_id = ? AND (path IN ('stdout.log', 'stderr.log', 'stdout.log.gz', 'stderr.log.gz') OR path LIKE '%/stdout.log' OR path LIKE '%/stderr.log' OR path LIKE '%/stdout.log.gz' OR path LIKE '%/stderr.log.gz'))")
        .bind(attempt_id.to_string()).fetch_one(&mut **tx).await.map_err(|e| db("check archive artifact conflicts", e))?;
    if collision != 0
        || (row.get::<Option<i64>, _>("process_group_id").is_none()
            && row.get::<Option<String>, _>("unit_id").is_none()
            && row.get::<Option<String>, _>("container_id").is_none())
    {
        return Err(conflict());
    }
    Ok(())
}

pub(super) async fn preview(
    tx: &mut Transaction<'_, Sqlite>,
    attempt_id: AttemptId,
    paths: &RuntimePaths,
    output: &mut Vec<super::GcEntry>,
) -> PersistenceResult<()> {
    use super::{GcEffect as E, GcEntry, GcOperation as O, GcStatus as S, GcTarget};
    let row = archive_eligibility(tx, attempt_id).await?;
    let spec: AttemptSpec = from_json(row.get("spec_json"), "archive GC spec")?;
    let project_id = parse_id(row.get("project_id"), "GC project")?;
    let mut entries = vec![];
    let mut retired = vec![];
    for stream in ["stdout", "stderr"] {
        let done: i64 = sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM success_log_archives WHERE attempt_id = ? AND stream = ? AND retired = 1)").bind(attempt_id.to_string()).bind(stream).fetch_one(&mut **tx).await.map_err(|e| db("read GC retired stream", e))?;
        if done != 0 {
            retired.push(stream);
            continue;
        }
        entries.push(GcEntry::new(
            O::SuccessLogCompression,
            format!("{attempt_id}:{stream}"),
            project_id,
            Some(attempt_id),
        ));
    }
    let result: PersistenceResult<()> = async {
        let barrier = if spec.result().compress_success_logs != Some(true) { Some((S::Retain, "compression_not_enabled")) }
            else if row.get::<&str,_>("state") != "succeeded" { Some((S::Deferred, "attempt_not_successful")) }
            else if row.get::<i64,_>("registered") != 1 { Some((S::Deferred, "project_unregistered")) }
            else if row.get::<i64,_>("extracted") == 0 { Some((S::Deferred, "active_extraction")) }
            else if row.get::<i64,_>("diagnosed") == 0 { Some((S::Deferred, "diagnosis_pending")) }
            else if raw_consumers(tx, attempt_id).await? { Some((S::Deferred, "raw_log_consumers")) } else { None };
        if let Some((status,reason)) = barrier { for e in &mut entries { e.decide(status, reason, E::Retain); } return Ok(()); }
        check_archive_registration(tx, attempt_id, paths, &row).await?;
        let group: Option<i64> = row.get("process_group_id");
        if tokio::task::spawn_blocking(move || group.map(group_quiescent).transpose()).await?? == Some(false) { for e in &mut entries { e.decide(S::Deferred, "live_process_group", E::Retain); } return Ok(()); }
        let mut stopped = false;
        for stream in ["stdout", "stderr"] {
            if retired.contains(&stream) { continue; }
            let entry = entries.iter_mut().find(|e| e.subject_id == format!("{attempt_id}:{stream}")).ok_or_else(conflict)?;
            if stopped { entry.decide(S::Deferred, "preceding_stream_unsafe", E::Retain); continue; }
            let original = paths.log_dir.join(attempt_id.to_string()).join(format!("{stream}.log"));
            let archive_row = sqlx::query("SELECT identity_json, state FROM success_log_archives WHERE attempt_id = ? AND stream = ?").bind(attempt_id.to_string()).bind(stream).fetch_optional(&mut **tx).await.map_err(|e| db("read GC archive", e))?;
            let prior: Option<LogArchive> = match archive_row.as_ref().map(|r| from_json(r.get("identity_json"), "GC archive identity")).transpose() {
                Ok(prior) => prior,
                Err(error) => {
                    GcEntry::unsafe_batch(std::slice::from_mut(entry), &error);
                    stopped = true;
                    continue;
                }
            };
            let published = archive_row.as_ref().is_some_and(|r| r.get::<&str,_>("state") == "published");
            let reservation: Option<(String,String)> = sqlx::query_as("SELECT raw_path, temp_path FROM success_log_archive_preparations WHERE attempt_id = ? AND stream = ?").bind(attempt_id.to_string()).bind(stream).fetch_optional(&mut **tx).await.map_err(|e| db("read GC archive reservation", e))?;
            let checked = tokio::task::spawn_blocking(move || -> PersistenceResult<(S, E, Vec<GcTarget>)> {
                if let Some(a) = prior {
                    if a.original_path != original || a.path != original.with_extension("log.gz") { return Err(conflict()); }
                    validate_temporary(&original, &a.temporary_path)?;
                    let mut copy = a.clone();
                    if !published && !existing(&a.path)? { copy.path = a.temporary_path.clone(); }
                    let raw = verify(&copy, !published)?;
                    let copy_effect = if copy.path == a.path { E::Retain } else { E::ResumePublishRetireRaw };
                    let mut targets = vec![GcTarget { path: copy.path, bytes: Some(a.archive_size), effect: copy_effect }];
                    if raw { targets.push(GcTarget { path: original, bytes: Some(a.original_size), effect: E::Remove }); }
                    Ok((if raw { S::Ready } else { S::MetadataOnly }, if raw { E::ResumePublishRetireRaw } else { E::ResumeMetadata }, targets))
                } else {
                    if !existing(&original)? || existing(&original.with_extension("log.gz"))? { return Err(conflict()); }
                    let (size,_) = digest(File::open(&original)?)?;
                    let mut targets = vec![GcTarget { path: original.clone(), bytes: Some(size), effect: E::CompressPublishRetireRaw }];
                    let effect = if let Some((raw,temp)) = reservation {
                        if Path::new(&raw) != original { return Err(conflict()); }
                        let temp = PathBuf::from(temp);
                        validate_temporary(&original, &temp)?; validate_reserved_file(&temp)?;
                        if existing(&temp)? { targets.push(GcTarget { bytes: Some(std::fs::metadata(&temp)?.len()), path: temp, effect: E::ResumeCompression }); }
                        E::ResumeCompression
                    } else { E::CompressPublishRetireRaw };
                    Ok((S::Ready, effect, targets))
                }
            }).await?;
            match checked {
                Ok((status,effect,targets)) => { entry.decide(status, "verified_success_log_compression", effect); entry.targets = targets; }
                Err(error) => { GcEntry::unsafe_batch(std::slice::from_mut(entry), &error); stopped = true; }
            }
        }
        Ok(())
    }.await;
    if let Err(error) = result {
        GcEntry::unsafe_batch(&mut entries, &error);
    }
    output.extend(entries);
    Ok(())
}
