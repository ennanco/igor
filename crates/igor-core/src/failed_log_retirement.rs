use super::success_log_archives::{
    archive_eligibility, check_archive_registration, digest, existing, group_quiescent,
    raw_consumers,
};
use super::*;
use crate::RuntimePaths;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use sqlx::{Row, Sqlite, Transaction};
use std::{
    fs::File,
    io::{Read, Seek, SeekFrom},
    os::unix::fs::MetadataExt,
    path::PathBuf,
};

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct RetiredLogTail {
    pub attempt_id: AttemptId,
    pub stream: String,
    pub original_path: PathBuf,
    pub original_size: u64,
    pub original_sha256: String,
    pub start: u64,
    pub end: u64,
    pub tail_sha256: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct LogTailChunk {
    pub identity: RetiredLogTail,
    pub start: u64,
    pub end: u64,
    pub bytes: Vec<u8>,
}

fn conflict() -> PersistenceError {
    PersistenceError::Conflict {
        entity: "failed log retirement identity",
    }
}
pub(super) async fn consent(
    tx: &mut Transaction<'_, Sqlite>,
    attempt: AttemptId,
    stream: &str,
    paths: &RuntimePaths,
) -> PersistenceResult<RetiredLogTail> {
    Ok(identity(tx, attempt, stream, paths).await?.0)
}
fn stream_column(stream: &str) -> PersistenceResult<&'static str> {
    match stream {
        "stdout" => Ok("stdout"),
        "stderr" => Ok("stderr"),
        _ => Err(conflict()),
    }
}

async fn barrier(
    tx: &mut Transaction<'_, Sqlite>,
    attempt: AttemptId,
    paths: &RuntimePaths,
) -> PersistenceResult<Option<&'static str>> {
    let row = archive_eligibility(tx, attempt).await?;
    let spec: AttemptSpec = from_json(row.get("spec_json"), "failed log spec")?;
    if spec.result().failed_full_logs != Some(crate::FailedRetention::DeleteAfterDiagnosis) {
        return Ok(Some("full_logs_retained"));
    }
    if !matches!(row.get::<&str, _>("state"), "failed" | "cancelled" | "lost") {
        return Ok(Some("attempt_not_failed_terminal"));
    }
    if row.get::<i64, _>("registered") != 1 {
        return Ok(Some("project_unregistered"));
    }
    let diagnosed: i64 =
        sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM diagnosed_attempts WHERE attempt_id = ?)")
            .bind(attempt.to_string())
            .fetch_one(&mut **tx)
            .await
            .map_err(|e| db("check failed log diagnosis", e))?;
    if diagnosed == 0 {
        return Ok(Some("diagnosis_pending"));
    }
    let complete: Option<i64> =
        sqlx::query_scalar("SELECT complete FROM failed_log_tails WHERE attempt_id = ?")
            .bind(attempt.to_string())
            .fetch_optional(&mut **tx)
            .await
            .map_err(|e| db("check failed log tail", e))?;
    if complete != Some(1) {
        return Ok(Some("failed_tail_incomplete"));
    }
    if row.get::<i64, _>("extracted") == 0 || raw_consumers(tx, attempt).await? {
        return Ok(Some("raw_log_consumers"));
    }
    let superseded: i64 = sqlx::query_scalar("SELECT j.state = 'superseded' FROM attempts a JOIN jobs j ON j.id = a.job_id WHERE a.id = ?").bind(attempt.to_string()).fetch_one(&mut **tx).await.map_err(|e| db("check failed log generation",e))?;
    if superseded != 0 && !replacement_attempt_succeeded(tx, attempt).await? {
        return Ok(Some("current_replacement_pending"));
    }
    check_archive_registration(tx, attempt, paths, &row).await?;
    let group: Option<i64> = row.get("process_group_id");
    if tokio::task::spawn_blocking(move || group.map(group_quiescent).transpose()).await??
        == Some(false)
    {
        return Ok(Some("live_process_group"));
    }
    Ok(None)
}

async fn identity(
    tx: &mut Transaction<'_, Sqlite>,
    attempt: AttemptId,
    stream: &str,
    paths: &RuntimePaths,
) -> PersistenceResult<(RetiredLogTail, bool)> {
    let column = stream_column(stream)?;
    let row = sqlx::query(&format!("SELECT {column} AS bytes, limit_kb FROM failed_log_tails WHERE attempt_id = ? AND complete = 1"))
        .bind(attempt.to_string()).fetch_one(&mut **tx).await.map_err(|e| db("read retirement tail",e))?;
    let tail: Vec<u8> = row.get("bytes");
    let limit: i64 = row.get("limit_kb");
    let spec: String = sqlx::query_scalar("SELECT spec_json FROM attempts WHERE id = ?")
        .bind(attempt.to_string())
        .fetch_one(&mut **tx)
        .await
        .map_err(|e| db("read frozen tail limit", e))?;
    let spec: AttemptSpec = from_json(&spec, "frozen tail spec")?;
    if limit != i64::from(spec.result().failed_log_tail_kb()) {
        return Err(conflict());
    }
    if !(0..=16384).contains(&limit) || tail.len() > limit as usize * 1024 {
        return Err(conflict());
    }
    let prior: Option<String> = sqlx::query_scalar(
        "SELECT identity_json FROM failed_log_retirements WHERE attempt_id = ? AND stream = ?",
    )
    .bind(attempt.to_string())
    .bind(stream)
    .fetch_optional(&mut **tx)
    .await
    .map_err(|e| db("read retirement intent", e))?;
    let prior: Option<RetiredLogTail> = prior
        .map(|v| from_json(&v, "retirement identity"))
        .transpose()?;
    let path = paths
        .log_dir
        .join(attempt.to_string())
        .join(format!("{stream}.log"));
    let stream = stream.to_owned();
    tokio::task::spawn_blocking(move || {
        let tail_hash = format!("{:x}", Sha256::digest(&tail));
        if let Some(ref p) = prior
            && (p.attempt_id != attempt
                || p.stream != stream
                || p.original_path != path
                || p.end != p.original_size
                || p.start.checked_add(tail.len() as u64) != Some(p.end)
                || p.tail_sha256 != tail_hash)
        {
            return Err(conflict());
        }
        if !existing(&path)? {
            return prior.map(|p| (p, false)).ok_or_else(conflict);
        }
        if std::fs::symlink_metadata(&path)?.nlink() != 1 {
            return Err(conflict());
        }
        let mut file = File::open(&path)?;
        let (size, hash) = digest(&mut file)?;
        if tail.len() as u64 != size.min(limit as u64 * 1024) {
            return Err(conflict());
        }
        let start = size.checked_sub(tail.len() as u64).ok_or_else(conflict)?;
        file.seek(SeekFrom::Start(start))?;
        let mut suffix = Vec::with_capacity(tail.len());
        file.take(tail.len() as u64 + 1).read_to_end(&mut suffix)?;
        if suffix != tail {
            return Err(conflict());
        }
        let current = RetiredLogTail {
            attempt_id: attempt,
            stream,
            original_path: path,
            original_size: size,
            original_sha256: hash,
            start,
            end: size,
            tail_sha256: tail_hash,
        };
        if prior.is_some_and(|p| p != current) {
            return Err(conflict());
        }
        Ok((current, true))
    })
    .await?
}

pub(super) async fn preview(
    tx: &mut Transaction<'_, Sqlite>,
    attempt: AttemptId,
    paths: &RuntimePaths,
    output: &mut Vec<GcEntry>,
) -> PersistenceResult<()> {
    let project: String = sqlx::query_scalar("SELECT project_id FROM attempts WHERE id = ?")
        .bind(attempt.to_string())
        .fetch_one(&mut **tx)
        .await
        .map_err(|e| db("read failed log project", e))?;
    let barrier = barrier(tx, attempt, paths).await;
    for stream in ["stdout", "stderr"] {
        let done: i64 = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM failed_log_retirements WHERE attempt_id = ? AND stream = ? AND state = 'completed')").bind(attempt.to_string()).bind(stream).fetch_one(&mut **tx).await.map_err(|e| db("read retired log state",e))?;
        if done != 0 {
            continue;
        }
        let mut entry = GcEntry::new(
            GcOperation::FailedLogRetirement,
            format!("{attempt}:{stream}"),
            parse_id(&project, "log project")?,
            Some(attempt),
        );
        match &barrier {
            Err(error) => GcEntry::unsafe_batch(std::slice::from_mut(&mut entry), error),
            Ok(Some(reason)) => entry.decide(
                if *reason == "full_logs_retained" {
                    GcStatus::Retain
                } else {
                    GcStatus::Deferred
                },
                reason,
                GcEffect::Retain,
            ),
            Ok(None) => match identity(tx, attempt, stream, paths).await {
                Err(error) => GcEntry::unsafe_batch(std::slice::from_mut(&mut entry), &error),
                Ok((id, raw)) => {
                    entry.decide(
                        if raw {
                            GcStatus::Ready
                        } else {
                            GcStatus::MetadataOnly
                        },
                        "verified_failed_log_retirement",
                        if raw {
                            GcEffect::Remove
                        } else {
                            GcEffect::ResumeMetadata
                        },
                    );
                    if raw {
                        entry.targets.push(GcTarget {
                            path: id.original_path,
                            bytes: Some(id.original_size),
                            effect: GcEffect::Remove,
                        });
                    }
                }
            },
        }
        output.push(entry);
    }
    Ok(())
}

impl JobAttemptRepository<'_> {
    pub async fn failed_log_candidates(
        &self,
        after: Option<AttemptId>,
    ) -> PersistenceResult<Vec<AttemptId>> {
        let ids: Vec<String> = sqlx::query_scalar("SELECT a.id FROM attempts a JOIN diagnosed_attempts d ON d.attempt_id = a.id WHERE a.id > COALESCE(?, '') AND json_extract(a.spec_json, '$.result.failed_full_logs') = 'delete-after-diagnosis' AND (SELECT COUNT(*) FROM failed_log_retirements r WHERE r.attempt_id = a.id AND state = 'completed') < 2 ORDER BY a.id LIMIT 32")
            .bind(after.map(|a|a.to_string())).fetch_all(&self.database.pool).await.map_err(|e| db("find failed logs",e))?;
        ids.iter()
            .map(|a| parse_id(a, "failed log attempt"))
            .collect()
    }

    pub async fn retire_failed_logs(
        &self,
        attempt: AttemptId,
        paths: &RuntimePaths,
    ) -> PersistenceResult<usize> {
        self.retire_failed_logs_selected(attempt, paths, None).await
    }

    pub(super) async fn retire_failed_logs_selected(
        &self,
        attempt: AttemptId,
        paths: &RuntimePaths,
        selected: Option<&[GcEntry]>,
    ) -> PersistenceResult<usize> {
        let mut count = 0;
        for stream in ["stdout", "stderr"] {
            let selection = selected.map(|s| {
                s.iter()
                    .filter(|e| e.subject_id == format!("{attempt}:{stream}"))
                    .cloned()
                    .collect::<Vec<_>>()
            });
            if selection.as_ref().is_some_and(Vec::is_empty) {
                continue;
            }
            let mut tx = self
                .database
                .pool
                .begin()
                .await
                .map_err(|e| db("begin failed log preparation", e))?;
            sqlx::query("UPDATE attempts SET state = state WHERE id = ?")
                .bind(attempt.to_string())
                .execute(&mut *tx)
                .await
                .map_err(|e| db("reserve failed log writer", e))?;
            if barrier(&mut tx, attempt, paths).await?.is_some() {
                continue;
            }
            let done: i64 = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM failed_log_retirements WHERE attempt_id = ? AND stream = ? AND state = 'completed')").bind(attempt.to_string()).bind(stream).fetch_one(&mut *tx).await.map_err(|e|db("check completed retirement",e))?;
            if done != 0 {
                continue;
            }
            if !gc::validate_selection(&mut tx, paths, selection.as_deref(), true).await? {
                continue;
            }
            let (prepared, _) = identity(&mut tx, attempt, stream, paths).await?;
            sqlx::query("INSERT INTO failed_log_retirements(attempt_id, stream, identity_json, state) VALUES (?, ?, ?, 'prepared') ON CONFLICT DO NOTHING")
                .bind(attempt.to_string()).bind(stream).bind(json(&prepared,"failed log identity")?).execute(&mut *tx).await.map_err(|e|db("prepare failed log retirement",e))?;
            tx.commit()
                .await
                .map_err(|e| db("commit failed log preparation", e))?;
            let mut tx = self
                .database
                .pool
                .begin()
                .await
                .map_err(|e| db("begin failed log retirement", e))?;
            sqlx::query("UPDATE attempts SET state = state WHERE id = ?")
                .bind(attempt.to_string())
                .execute(&mut *tx)
                .await
                .map_err(|e| db("reserve failed log retirement", e))?;
            if barrier(&mut tx, attempt, paths).await?.is_some()
                || !gc::validate_selection(&mut tx, paths, selection.as_deref(), false).await?
            {
                continue;
            }
            let done: i64 = sqlx::query_scalar("SELECT state = 'completed' FROM failed_log_retirements WHERE attempt_id = ? AND stream = ?").bind(attempt.to_string()).bind(stream).fetch_one(&mut *tx).await.map_err(|e|db("recheck completed retirement",e))?;
            if done != 0 {
                continue;
            }
            let (current, raw) = identity(&mut tx, attempt, stream, paths).await?;
            if current != prepared {
                return Err(conflict());
            }
            let path = current.original_path.clone();
            let (mut tx, result) = tokio::task::spawn_blocking(move || {
                let result = (|| -> PersistenceResult<()> {
                    if raw {
                        std::fs::remove_file(&path)?;
                    }
                    let mut parent = path.parent().ok_or_else(conflict)?.to_path_buf();
                    while !parent.exists() {
                        parent = parent.parent().ok_or_else(conflict)?.to_path_buf();
                    }
                    File::open(parent)?.sync_all()?;
                    Ok(())
                })();
                (tx, result)
            })
            .await?;
            result?;
            let decided_at: String = sqlx::query_scalar(
                "SELECT decided_at FROM failed_log_retirements WHERE attempt_id = ? AND stream = ?",
            )
            .bind(attempt.to_string())
            .bind(stream)
            .fetch_one(&mut *tx)
            .await
            .map_err(|e| db("read retirement decision time", e))?;
            cleanup_evidence::record(
                &mut tx,
                "failed_log_retirement",
                &format!("{attempt}:{stream}"),
                "raw_retired",
                &serde_json::to_value(&current).map_err(|_| conflict())?,
                Some(&decided_at),
                None,
            )
            .await?;
            sqlx::query("UPDATE failed_log_retirements SET state = 'completed' WHERE attempt_id = ? AND stream = ?").bind(attempt.to_string()).bind(stream).execute(&mut *tx).await.map_err(|e|db("complete failed log retirement",e))?;
            let (project, job): (String, String) =
                sqlx::query_as("SELECT project_id, job_id FROM attempts WHERE id = ?")
                    .bind(attempt.to_string())
                    .fetch_one(&mut *tx)
                    .await
                    .map_err(|e| db("read retirement event context", e))?;
            let event = crate::Event::new(
                crate::EventId::new(),
                crate::EventKind::CleanupStateChanged,
                crate::EventPayload::new(
                    crate::EventKind::CleanupStateChanged,
                    1,
                    serde_json::json!({"attempt_id":attempt,"stream":stream,"state":"raw_retired","reason":"failed_log_retirement"}),
                )?,
            )?;
            insert_event(
                &mut tx,
                parse_id(&project, "log project")?,
                Some(parse_id(&job, "log job")?),
                Some(attempt),
                &event,
            )
            .await?;
            tx.commit()
                .await
                .map_err(|e| db("commit failed log retirement", e))?;
            count += 1;
        }
        Ok(count)
    }

    pub async fn retired_log_tail(
        &self,
        attempt: AttemptId,
        stream: &str,
    ) -> PersistenceResult<Option<RetiredLogTail>> {
        stream_column(stream)?;
        let value: Option<String> = sqlx::query_scalar("SELECT identity_json FROM failed_log_retirements WHERE attempt_id = ? AND stream = ? AND state = 'completed'").bind(attempt.to_string()).bind(stream).fetch_optional(&self.database.pool).await.map_err(|e|db("read retired tail descriptor",e))?;
        value.map(|v| from_json(&v, "tail descriptor")).transpose()
    }

    pub async fn retired_log_chunk(
        &self,
        expected: &RetiredLogTail,
        offset: u64,
        limit: u32,
    ) -> PersistenceResult<LogTailChunk> {
        let column = stream_column(&expected.stream)?;
        if limit == 0 || limit > 65536 {
            return Err(conflict());
        }
        let mut tx = self
            .database
            .pool
            .begin()
            .await
            .map_err(|e| db("begin tail chunk", e))?;
        let row = sqlx::query(&format!("SELECT r.identity_json, length(t.{column}) AS len, substr(t.{column}, ?, ?) AS bytes FROM failed_log_retirements r JOIN failed_log_tails t ON t.attempt_id = r.attempt_id WHERE r.attempt_id = ? AND r.stream = ? AND r.state = 'completed' AND t.complete = 1"))
            .bind(i64::try_from(offset.max(expected.start).saturating_sub(expected.start)).map_err(|_|conflict())?.checked_add(1).ok_or_else(conflict)?)
            .bind(i64::from(limit)).bind(expected.attempt_id.to_string()).bind(&expected.stream).fetch_optional(&mut *tx).await.map_err(|e|db("read bounded tail chunk",e))?.ok_or_else(conflict)?;
        let actual: RetiredLogTail = from_json(row.get("identity_json"), "tail chunk identity")?;
        let len: i64 = row.get("len");
        if actual != *expected
            || !(0..=16 * 1024 * 1024).contains(&len)
            || actual.end != actual.original_size
            || actual.start.checked_add(len as u64) != Some(actual.end)
            || offset > actual.end
        {
            return Err(conflict());
        }
        let bytes: Vec<u8> = row.get("bytes");
        let start = offset.max(actual.start);
        let end = start.checked_add(bytes.len() as u64).ok_or_else(conflict)?;
        if end > actual.end {
            return Err(conflict());
        }
        tx.commit().await.map_err(|e| db("finish tail chunk", e))?;
        Ok(LogTailChunk {
            identity: actual,
            start,
            end,
            bytes,
        })
    }
}
