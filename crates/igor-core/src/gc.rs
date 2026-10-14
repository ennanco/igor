use super::*;
use crate::RuntimePaths;
use serde::{Deserialize, Serialize};
use std::{collections::BTreeMap, path::PathBuf};

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum GcOperation {
    FailedArtifacts,
    SupersededArtifacts,
    RepairWorktrees,
    SuccessLogCompression,
    FailedLogRetirement,
    OwnedFailedOutputs,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum GcStatus {
    Ready,
    Retain,
    Deferred,
    Unsafe,
    MetadataOnly,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum GcEffect {
    Remove,
    Retain,
    CompressPublishRetireRaw,
    ResumePublishRetireRaw,
    ResumeCompression,
    ResumeMetadata,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct GcTarget {
    pub path: PathBuf,
    pub effect: GcEffect,
    pub bytes: Option<u64>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct GcEntry {
    pub operation: GcOperation,
    pub subject_id: String,
    pub project_id: ProjectId,
    pub attempt_id: Option<AttemptId>,
    pub status: GcStatus,
    pub reason: String,
    pub effect: GcEffect,
    pub targets: Vec<GcTarget>,
    #[serde(default)]
    pub consent_identity: serde_json::Value,
}

impl GcEntry {
    pub(super) fn new(
        operation: GcOperation,
        subject_id: String,
        project_id: ProjectId,
        attempt_id: Option<AttemptId>,
    ) -> Self {
        Self {
            operation,
            subject_id,
            project_id,
            attempt_id,
            status: GcStatus::Deferred,
            reason: "eligibility_pending".into(),
            effect: GcEffect::Retain,
            targets: vec![],
            consent_identity: serde_json::Value::Null,
        }
    }
    pub(super) fn decide(&mut self, status: GcStatus, reason: &str, effect: GcEffect) {
        self.status = status;
        self.reason = reason.into();
        self.effect = effect;
    }
    pub(super) fn unsafe_batch(entries: &mut [Self], error: &PersistenceError) {
        let reason = match error {
            PersistenceError::Conflict { entity } => entity.replace(' ', "_"),
            PersistenceError::Io(_) => "filesystem_error".into(),
            PersistenceError::Serialization { .. } => "invalid_persisted_identity".into(),
            _ => "invalid_preflight".into(),
        };
        for entry in entries {
            entry.decide(GcStatus::Unsafe, &reason, GcEffect::Retain);
            entry.targets.clear();
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct GcPage {
    pub operation: GcOperation,
    pub subjects_observed: usize,
    pub truncated: bool,
    pub continuation_after: Option<String>,
    pub artifact_batches: Vec<GcArtifactBatch>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct GcArtifactBatch {
    pub attempt_id: AttemptId,
    pub rows_observed: usize,
    pub truncated: bool,
    pub continuation_after: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct GcPlan {
    pub project_id: Option<ProjectId>,
    pub project_name: Option<String>,
    pub advisory: bool,
    pub eligibility_basis: String,
    pub filesystem_basis: String,
    pub requires_revalidation: bool,
    pub entries: Vec<GcEntry>,
    pub pages: Vec<GcPage>,
    pub known_copy_bytes: u64,
    pub estimated_gzip_savings: Option<u64>,
    pub worktree_bytes: Option<u64>,
}

pub(super) async fn artifact_batch(
    tx: &mut Transaction<'_, Sqlite>,
    attempt_id: AttemptId,
    operation: GcOperation,
    selected: Option<&[GcEntry]>,
) -> PersistenceResult<(Vec<ArtifactRecord>, GcArtifactBatch)> {
    let predicate = match operation {
        GcOperation::FailedArtifacts => {
            "lifecycle_state = 'failed' AND state IN ('registered', 'staged')"
        }
        _ => "lifecycle_state = 'superseded' AND state = 'published'",
    };
    let sql = format!(
        "SELECT id, attempt_id, path, role, role_detail, sha256, size_bytes, state, lifecycle_state, diagnosis_required, metadata_json FROM artifacts WHERE attempt_id = ? AND {predicate} AND (?2 IS NULL OR id IN (SELECT value FROM json_each(?2))) ORDER BY id LIMIT 65"
    );
    let mut rows = sqlx::query(&sql)
        .bind(attempt_id.to_string())
        .bind(selected_ids(selected))
        .fetch_all(&mut **tx)
        .await
        .map_err(|e| db("read GC artifact batch", e))?;
    let truncated = rows.len() > 64;
    rows.truncate(64);
    let artifacts = rows
        .into_iter()
        .map(decode_artifact)
        .collect::<PersistenceResult<Vec<_>>>()?;
    let batch = GcArtifactBatch {
        attempt_id,
        rows_observed: artifacts.len(),
        truncated,
        continuation_after: artifacts
            .last()
            .filter(|_| truncated)
            .map(|a| a.id.to_string()),
    };
    Ok((artifacts, batch))
}

impl Database {
    pub async fn gc_plan(
        &self,
        project_id: Option<ProjectId>,
        paths: &RuntimePaths,
    ) -> PersistenceResult<GcPlan> {
        self.gc_plan_filtered(project_id, paths, false).await
    }

    pub async fn gc_plan_filtered(
        &self,
        project_id: Option<ProjectId>,
        paths: &RuntimePaths,
        failed_only: bool,
    ) -> PersistenceResult<GcPlan> {
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| db("begin GC preview", e))?;
        let scope = project_id.map(|id| id.to_string());
        let project_name = if let Some(id) = &scope {
            Some(
                sqlx::query_scalar("SELECT name FROM projects WHERE id = ? AND registered = 1")
                    .bind(id)
                    .fetch_optional(&mut *tx)
                    .await
                    .map_err(|e| db("read GC scope", e))?
                    .ok_or(PersistenceError::NotFound { entity: "project" })?,
            )
        } else {
            None
        };
        let mut plan = GcPlan {
            project_id,
            project_name,
            advisory: true,
            eligibility_basis: "database_snapshot".into(),
            filesystem_basis: "observed_read_only".into(),
            requires_revalidation: true,
            entries: vec![],
            pages: vec![],
            known_copy_bytes: 0,
            estimated_gzip_savings: None,
            worktree_bytes: None,
        };
        for (operation, sql) in [
            (
                GcOperation::OwnedFailedOutputs,
                "SELECT a.id FROM attempts a JOIN owned_output_namespaces n ON n.attempt_id = a.id WHERE (?1 IS NULL OR a.project_id = ?1) AND a.state IN ('failed','cancelled','lost') AND (NOT EXISTS(SELECT 1 FROM owned_output_discovery d WHERE d.attempt_id = a.id) OR EXISTS(SELECT 1 FROM owned_output_manifest m WHERE m.attempt_id = a.id AND state = 'prepared')) ORDER BY a.id LIMIT 33",
            ),
            (
                GcOperation::FailedLogRetirement,
                "SELECT a.id FROM attempts a WHERE (?1 IS NULL OR a.project_id = ?1) AND a.state IN ('failed', 'cancelled', 'lost') AND (json_type(a.spec_json, '$.result.failed_full_logs') = 'text' OR EXISTS(SELECT 1 FROM failed_log_retirements r WHERE r.attempt_id = a.id)) AND (SELECT COUNT(*) FROM failed_log_retirements r WHERE r.attempt_id = a.id AND state = 'completed') < 2 ORDER BY a.id LIMIT 33",
            ),
            (
                GcOperation::FailedArtifacts,
                "SELECT DISTINCT a.id FROM attempts a JOIN artifacts r ON r.attempt_id = a.id WHERE (?1 IS NULL OR a.project_id = ?1) AND r.lifecycle_state = 'failed' AND r.state IN ('registered', 'staged') ORDER BY a.id LIMIT 33",
            ),
            (
                GcOperation::SupersededArtifacts,
                "SELECT DISTINCT a.id FROM attempts a JOIN artifacts r ON r.attempt_id = a.id WHERE (?1 IS NULL OR a.project_id = ?1) AND r.lifecycle_state = 'superseded' AND r.state = 'published' ORDER BY a.id LIMIT 33",
            ),
            (
                GcOperation::RepairWorktrees,
                "SELECT recovery_id FROM repair_worktrees WHERE (?1 IS NULL OR project_id = ?1) AND state IN ('registered', 'released', 'cleaning') ORDER BY recovery_id LIMIT 33",
            ),
            (
                GcOperation::SuccessLogCompression,
                "SELECT a.id FROM attempts a WHERE (?1 IS NULL OR a.project_id = ?1) AND (a.state = 'succeeded' OR EXISTS (SELECT 1 FROM success_log_archive_preparations p WHERE p.attempt_id = a.id) OR EXISTS (SELECT 1 FROM success_log_archives l WHERE l.attempt_id = a.id AND retired = 0)) AND (SELECT COUNT(*) FROM success_log_archives l WHERE l.attempt_id = a.id AND retired = 1) < 2 ORDER BY a.id LIMIT 33",
            ),
        ] {
            if failed_only
                && !matches!(
                    operation,
                    GcOperation::FailedArtifacts
                        | GcOperation::FailedLogRetirement
                        | GcOperation::OwnedFailedOutputs
                )
            {
                continue;
            }
            let mut ids: Vec<String> = sqlx::query_scalar(sql)
                .bind(&scope)
                .fetch_all(&mut *tx)
                .await
                .map_err(|e| db("discover GC subjects", e))?;
            let truncated = ids.len() > 32;
            ids.truncate(32);
            let mut page = GcPage {
                operation,
                subjects_observed: ids.len(),
                truncated,
                continuation_after: ids.last().filter(|_| truncated).cloned(),
                artifact_batches: vec![],
            };
            for id in ids {
                match operation {
                    GcOperation::OwnedFailedOutputs => {
                        owned_failed_outputs::preview(
                            &mut tx,
                            parse_id(&id, "GC owned attempt")?,
                            paths,
                            &mut plan.entries,
                            &mut page.artifact_batches,
                        )
                        .await?
                    }
                    GcOperation::FailedLogRetirement => {
                        failed_log_retirement::preview(
                            &mut tx,
                            parse_id(&id, "GC log attempt")?,
                            paths,
                            &mut plan.entries,
                        )
                        .await?
                    }
                    GcOperation::FailedArtifacts => {
                        failed_cleanup::preview(
                            &mut tx,
                            parse_id(&id, "GC attempt")?,
                            paths,
                            &mut plan.entries,
                            &mut page.artifact_batches,
                            None,
                        )
                        .await?
                    }
                    GcOperation::SupersededArtifacts => {
                        superseded_cleanup::preview(
                            &mut tx,
                            parse_id(&id, "GC attempt")?,
                            paths,
                            &mut plan.entries,
                            &mut page.artifact_batches,
                            None,
                        )
                        .await?
                    }
                    GcOperation::RepairWorktrees => {
                        repair_worktrees::preview(
                            &mut tx,
                            parse_id(&id, "GC recovery")?,
                            paths,
                            &mut plan.entries,
                        )
                        .await?
                    }
                    GcOperation::SuccessLogCompression => {
                        success_log_archives::preview(
                            &mut tx,
                            parse_id(&id, "GC attempt")?,
                            paths,
                            &mut plan.entries,
                        )
                        .await?
                    }
                }
            }
            plan.pages.push(page);
        }
        for entry in &mut plan.entries {
            if eligible(entry.status) {
                match consent_identity(&mut tx, entry, paths).await {
                    Ok(identity) => entry.consent_identity = identity,
                    Err(error) => GcEntry::unsafe_batch(std::slice::from_mut(entry), &error),
                }
            }
        }
        tx.commit().await.map_err(|e| db("finish GC preview", e))?;
        let mut copies = BTreeMap::new();
        for target in plan.entries.iter().flat_map(|e| &e.targets) {
            if let Some(bytes) = target.bytes {
                copies.entry(&target.path).or_insert(bytes);
            }
        }
        plan.known_copy_bytes = copies.values().copied().sum();
        Ok(plan)
    }
}

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct GcWorklist {
    pub selections: Vec<GcSelection>,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum GcLogStream {
    Stdout,
    Stderr,
}

impl GcLogStream {
    fn as_str(self) -> &'static str {
        match self {
            Self::Stdout => "stdout",
            Self::Stderr => "stderr",
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum GcSubject {
    OwnedOutput {
        id: String,
    },
    Artifact {
        id: uuid::Uuid,
    },
    Recovery {
        id: crate::RecoveryId,
    },
    Log {
        attempt_id: AttemptId,
        stream: GcLogStream,
    },
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct GcSelection {
    pub subject: GcSubject,
    pub expected: GcEntry,
}

fn subject(entry: &GcEntry) -> PersistenceResult<GcSubject> {
    Ok(match entry.operation {
        GcOperation::OwnedFailedOutputs => GcSubject::OwnedOutput {
            id: entry.subject_id.clone(),
        },
        GcOperation::FailedArtifacts | GcOperation::SupersededArtifacts => GcSubject::Artifact {
            id: parse_id(&entry.subject_id, "GC artifact")?,
        },
        GcOperation::RepairWorktrees => GcSubject::Recovery {
            id: parse_id(&entry.subject_id, "GC recovery")?,
        },
        GcOperation::SuccessLogCompression | GcOperation::FailedLogRetirement => {
            let attempt_id = entry.attempt_id.ok_or(PersistenceError::Conflict {
                entity: "GC log attempt",
            })?;
            let stream = if entry.subject_id == format!("{attempt_id}:stdout") {
                GcLogStream::Stdout
            } else if entry.subject_id == format!("{attempt_id}:stderr") {
                GcLogStream::Stderr
            } else {
                return Err(PersistenceError::Conflict {
                    entity: "GC log stream",
                });
            };
            GcSubject::Log { attempt_id, stream }
        }
    })
}

impl GcPlan {
    pub fn worklist(&self) -> PersistenceResult<GcWorklist> {
        Ok(GcWorklist {
            selections: self
                .entries
                .iter()
                .filter(|e| eligible(e.status))
                .map(|e| {
                    Ok(GcSelection {
                        subject: subject(e)?,
                        expected: e.clone(),
                    })
                })
                .collect::<PersistenceResult<Vec<_>>>()?,
        })
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum GcExecutionStatus {
    Completed,
    AlreadyDone,
    Deferred,
    Unsafe,
    Error,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct GcExecutionEntry {
    pub operation: GcOperation,
    pub subject_id: String,
    pub status: GcExecutionStatus,
    pub cause: Option<GcExecutionCause>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct GcExecutionCause {
    pub code: String,
    pub message: String,
}

impl GcExecutionCause {
    pub fn new(code: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            code: code.into(),
            message: message.into(),
        }
    }
}

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct GcExecution {
    pub project_id: Option<ProjectId>,
    pub confirmed: bool,
    pub completed: usize,
    pub already_done: usize,
    pub deferred: usize,
    pub unsafe_count: usize,
    pub errors: usize,
    pub entries: Vec<GcExecutionEntry>,
}

fn eligible(status: GcStatus) -> bool {
    matches!(
        status,
        GcStatus::Ready | GcStatus::Retain | GcStatus::MetadataOnly
    )
}

pub(super) fn selected_ids(selected: Option<&[GcEntry]>) -> Option<String> {
    selected
        .map(|s| serde_json::json!(s.iter().map(|e| &e.subject_id).collect::<Vec<_>>()).to_string())
}

async fn consent_identity(
    tx: &mut Transaction<'_, Sqlite>,
    entry: &GcEntry,
    paths: &RuntimePaths,
) -> PersistenceResult<serde_json::Value> {
    let attempt = entry.attempt_id.ok_or(PersistenceError::Conflict {
        entity: "GC attempt identity",
    })?;
    let context: Option<(String, String, String, i64)> = sqlx::query_as("SELECT a.project_id, a.spec_json, p.root_path, p.registered FROM attempts a JOIN projects p ON p.id = a.project_id WHERE a.id = ?")
        .bind(attempt.to_string()).fetch_optional(&mut **tx).await.map_err(|e| db("read GC consent context", e))?;
    let Some((project, spec, root, registered)) = context else {
        return Err(PersistenceError::NotFound {
            entity: "GC attempt",
        });
    };
    if project != entry.project_id.to_string() || registered != 1 {
        return Ok(serde_json::Value::Null);
    }
    let identity: Option<String> = match entry.operation {
        GcOperation::OwnedFailedOutputs => {
            if entry.subject_id == attempt.to_string() && entry.status == GcStatus::Retain {
                Some(entry.reason.clone())
            } else {
                Some(
                    owned_failed_outputs::consent(tx, entry, paths)
                        .await?
                        .to_string(),
                )
            }
        }
        GcOperation::FailedLogRetirement => {
            let stream = entry
                .subject_id
                .rsplit(':')
                .next()
                .ok_or(PersistenceError::Conflict {
                    entity: "GC log stream",
                })?;
            if entry.status == GcStatus::Retain {
                Some(stream.to_owned())
            } else {
                Some(
                    serde_json::to_string(
                        &failed_log_retirement::consent(tx, attempt, stream, paths).await?,
                    )
                    .map_err(|_| PersistenceError::Conflict {
                        entity: "GC log identity",
                    })?,
                )
            }
        }
        GcOperation::FailedArtifacts | GcOperation::SupersededArtifacts => {
            let id: uuid::Uuid = parse_id(&entry.subject_id, "GC artifact")?;
            sqlx::query_scalar("SELECT json_object('id', id, 'path', path, 'sha256', sha256, 'size', size_bytes, 'role', role, 'detail', role_detail, 'metadata', metadata_json) FROM artifacts WHERE id = ? AND attempt_id = ?")
                .bind(id.to_string()).bind(attempt.to_string()).fetch_optional(&mut **tx).await.map_err(|e| db("read GC artifact consent", e))?
        }
        GcOperation::RepairWorktrees => {
            let id: crate::RecoveryId = parse_id(&entry.subject_id, "GC recovery")?;
            sqlx::query_scalar("SELECT json_object('path', path, 'repository', repository_path, 'root', root_path, 'patch', patch_sha256, 'records', records_json, 'delete', delete_on_release) FROM repair_worktrees WHERE recovery_id = ? AND project_id = ?")
                .bind(id.to_string()).bind(project.clone()).fetch_optional(&mut **tx).await.map_err(|e| db("read GC worktree consent", e))?
        }
        GcOperation::SuccessLogCompression => {
            let GcSubject::Log { stream, .. } = subject(entry)? else {
                unreachable!()
            };
            let raw = paths
                .log_dir
                .join(attempt.to_string())
                .join(format!("{}.log", stream.as_str()));
            let log_root = paths.log_dir.clone();
            let relative =
                PathBuf::from(attempt.to_string()).join(format!("{}.log", stream.as_str()));
            let digest =
                tokio::task::spawn_blocking(move || -> PersistenceResult<Option<(u64, String)>> {
                    match std::fs::symlink_metadata(&raw) {
                        Ok(metadata)
                            if metadata.is_file() && !metadata.file_type().is_symlink() =>
                        {
                            Ok(Some(hash_artifact_file(&log_root, &relative)?))
                        }
                        Ok(_) => Ok(None),
                        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
                        Err(error) => Err(error.into()),
                    }
                })
                .await??;
            Some(serde_json::json!({"stream": stream, "raw": digest}).to_string())
        }
    };
    let identity =
        serde_json::json!({"project": project, "spec": spec, "root": root, "subject": identity});
    Ok(
        serde_json::json!({"sha256": format!("{:x}", Sha256::digest(identity.to_string().as_bytes()))}),
    )
}

pub(super) async fn validate_selection(
    tx: &mut Transaction<'_, Sqlite>,
    paths: &RuntimePaths,
    selected: Option<&[GcEntry]>,
    first: bool,
) -> PersistenceResult<bool> {
    let Some(selected) = selected else {
        return Ok(true);
    };
    if selected.is_empty() {
        return Ok(false);
    }
    let mut current = vec![];
    let initial = &selected[0];
    let attempt = initial.attempt_id.ok_or(PersistenceError::Conflict {
        entity: "GC selected attempt",
    })?;
    match initial.operation {
        GcOperation::OwnedFailedOutputs => {
            owned_failed_outputs::preview(tx, attempt, paths, &mut current, &mut vec![]).await?
        }
        GcOperation::FailedLogRetirement => {
            failed_log_retirement::preview(tx, attempt, paths, &mut current).await?
        }
        GcOperation::FailedArtifacts => {
            failed_cleanup::preview(
                tx,
                attempt,
                paths,
                &mut current,
                &mut vec![],
                Some(selected),
            )
            .await?
        }
        GcOperation::SupersededArtifacts => {
            superseded_cleanup::preview(
                tx,
                attempt,
                paths,
                &mut current,
                &mut vec![],
                Some(selected),
            )
            .await?
        }
        GcOperation::RepairWorktrees => {
            repair_worktrees::preview(
                tx,
                parse_id(&initial.subject_id, "GC recovery")?,
                paths,
                &mut current,
            )
            .await?
        }
        GcOperation::SuccessLogCompression => {
            success_log_archives::preview(tx, attempt, paths, &mut current).await?
        }
    }
    for expected in selected {
        if !eligible(expected.status)
            || expected.consent_identity.is_null()
            || consent_identity(tx, expected, paths).await? != expected.consent_identity
        {
            return Ok(false);
        }
        let Some(actual) = current.iter().find(|e| {
            e.operation == expected.operation
                && e.subject_id == expected.subject_id
                && e.project_id == expected.project_id
                && e.attempt_id == expected.attempt_id
        }) else {
            if !first && terminal(tx, expected).await? {
                continue;
            }
            return Ok(false);
        };
        if actual.status == GcStatus::Unsafe {
            return Err(PersistenceError::Conflict {
                entity: "GC current preflight unsafe",
            });
        }
        if !eligible(actual.status)
            || (first
                && (actual.targets != expected.targets
                    || actual.effect != expected.effect
                    || actual.reason != expected.reason))
        {
            return Ok(false);
        }
    }
    Ok(true)
}

impl Database {
    pub async fn gc_execute(
        &self,
        project_id: Option<ProjectId>,
        failed_only: bool,
        worklist: GcWorklist,
        paths: &RuntimePaths,
    ) -> PersistenceResult<GcExecution> {
        if worklist.selections.len() > 32 * (64 * 3 + 5) {
            return Err(PersistenceError::Conflict {
                entity: "GC worklist bound",
            });
        }
        let mut groups: BTreeMap<(u8, String), Vec<GcEntry>> = BTreeMap::new();
        let mut seen = std::collections::BTreeSet::new();
        for selection in worklist.selections {
            let entry = selection.expected;
            if subject(&entry)? != selection.subject {
                return Err(PersistenceError::Conflict {
                    entity: "GC typed subject",
                });
            }
            if project_id.is_some_and(|id| id != entry.project_id)
                || (failed_only
                    && !matches!(
                        entry.operation,
                        GcOperation::FailedArtifacts
                            | GcOperation::FailedLogRetirement
                            | GcOperation::OwnedFailedOutputs
                    ))
                || !eligible(entry.status)
                || !seen.insert((entry.operation as u8, entry.subject_id.clone()))
            {
                return Err(PersistenceError::Conflict {
                    entity: "GC selection scope",
                });
            }
            let key = if entry.operation == GcOperation::RepairWorktrees {
                entry.subject_id.clone()
            } else {
                entry
                    .attempt_id
                    .ok_or(PersistenceError::Conflict {
                        entity: "GC attempt",
                    })?
                    .to_string()
            };
            groups
                .entry((entry.operation as u8, key))
                .or_default()
                .push(entry);
        }
        let mut result = GcExecution {
            project_id,
            confirmed: true,
            ..GcExecution::default()
        };
        let mut subjects = [0usize; 6];
        for ((operation, _), selected) in &groups {
            subjects[*operation as usize] += 1;
            let bound = if matches!(
                selected[0].operation,
                GcOperation::SuccessLogCompression | GcOperation::FailedLogRetirement
            ) {
                2
            } else if selected[0].operation == GcOperation::RepairWorktrees {
                1
            } else {
                64
            };
            if selected.len() > bound || subjects[*operation as usize] > 32 {
                return Err(PersistenceError::Conflict {
                    entity: "GC subject bound",
                });
            }
        }
        for selected in groups.values() {
            let entry = &selected[0];
            let attempt = entry.attempt_id.ok_or(PersistenceError::Conflict {
                entity: "GC attempt",
            })?;
            let mut tx = self
                .pool
                .begin()
                .await
                .map_err(|e| db("read GC initial states", e))?;
            let mut previously_done = std::collections::BTreeSet::new();
            for entry in selected {
                if terminal(&mut tx, entry).await? {
                    previously_done.insert(entry.subject_id.clone());
                }
            }
            tx.commit()
                .await
                .map_err(|e| db("finish GC initial states", e))?;
            let step = match entry.operation {
                GcOperation::OwnedFailedOutputs => {
                    self.jobs()
                        .retire_owned_failed_outputs_selected(attempt, paths, Some(selected))
                        .await
                }
                GcOperation::FailedLogRetirement => {
                    self.jobs()
                        .retire_failed_logs_selected(attempt, paths, Some(selected))
                        .await
                }
                GcOperation::FailedArtifacts => {
                    self.artifacts()
                        .cleanup_failed_selected(attempt, paths, selected)
                        .await
                }
                GcOperation::SupersededArtifacts => {
                    self.artifacts()
                        .cleanup_superseded_selected(attempt, paths, Some(selected))
                        .await
                }
                GcOperation::RepairWorktrees => self
                    .repair_worktrees()
                    .cleanup_repair_selected(
                        parse_id(&entry.subject_id, "GC recovery")?,
                        paths,
                        Some(selected),
                    )
                    .await
                    .map(usize::from),
                GcOperation::SuccessLogCompression => {
                    self.jobs()
                        .compress_success_logs_selected(attempt, paths, Some(selected))
                        .await
                }
            };
            let mut post = self
                .pool
                .begin()
                .await
                .map_err(|e| db("read selected GC outcomes", e))?;
            for entry in selected {
                let done = terminal(&mut post, entry).await?;
                // Retained decisions keep legacy subjects without any persisted
                // lifecycle marker, so a successful step alone cannot prove
                // completion: revalidate the saved selection under the post
                // transaction instead of inventing a terminal write.
                let retention_check: PersistenceResult<bool> = if step.is_ok()
                    && !done
                    && entry.status == GcStatus::Retain
                {
                    validate_selection(&mut post, paths, Some(std::slice::from_ref(entry)), true)
                        .await
                } else {
                    Ok(false)
                };
                let (status, cause) = match (&step, &retention_check) {
                    _ if previously_done.contains(&entry.subject_id) => {
                        (GcExecutionStatus::AlreadyDone, None)
                    }
                    _ if done || matches!(&retention_check, Ok(true)) => {
                        (GcExecutionStatus::Completed, None)
                    }
                    (Err(PersistenceError::Conflict { entity }), _)
                    | (_, Err(PersistenceError::Conflict { entity })) => (
                        GcExecutionStatus::Unsafe,
                        Some(GcExecutionCause::new("unsafe_preflight", *entity)),
                    ),
                    (Err(error), _) | (_, Err(error)) => (
                        GcExecutionStatus::Error,
                        Some(GcExecutionCause::new("execution_error", error.to_string())),
                    ),
                    _ => (
                        GcExecutionStatus::Deferred,
                        Some(GcExecutionCause::new(
                            "current_barrier_or_identity_changed",
                            "Current eligibility or consent identity prevented completion",
                        )),
                    ),
                };
                match status {
                    GcExecutionStatus::Completed => result.completed += 1,
                    GcExecutionStatus::AlreadyDone => result.already_done += 1,
                    GcExecutionStatus::Deferred => result.deferred += 1,
                    GcExecutionStatus::Unsafe => result.unsafe_count += 1,
                    GcExecutionStatus::Error => result.errors += 1,
                }
                result.entries.push(GcExecutionEntry {
                    operation: entry.operation,
                    subject_id: entry.subject_id.clone(),
                    status,
                    cause,
                });
            }
            post.commit()
                .await
                .map_err(|e| db("finish selected GC outcomes", e))?;
        }
        Ok(result)
    }
}

async fn terminal(tx: &mut Transaction<'_, Sqlite>, entry: &GcEntry) -> PersistenceResult<bool> {
    let attempt = entry.attempt_id.ok_or(PersistenceError::Conflict {
        entity: "GC terminal attempt",
    })?;
    let done: i64 = match entry.operation {
        GcOperation::OwnedFailedOutputs => sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM attempts a WHERE a.id = ? AND a.project_id = ? AND (EXISTS(SELECT 1 FROM owned_output_manifest r WHERE r.attempt_id = a.id AND r.id = ?3 AND r.state = 'completed') OR (?3 = a.id || ':discovery' AND EXISTS(SELECT 1 FROM owned_output_discovery d WHERE d.attempt_id = a.id))))").bind(attempt.to_string()).bind(entry.project_id.to_string()).bind(&entry.subject_id).fetch_one(&mut **tx).await,
        GcOperation::FailedLogRetirement => sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM failed_log_retirements r JOIN attempts a ON a.id = r.attempt_id WHERE a.id = ? AND a.project_id = ? AND r.stream = ? AND r.state = 'completed')").bind(attempt.to_string()).bind(entry.project_id.to_string()).bind(entry.subject_id.rsplit(':').next().unwrap_or("")).fetch_one(&mut **tx).await,
        GcOperation::FailedArtifacts | GcOperation::SupersededArtifacts => sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM artifacts r JOIN attempts a ON a.id = r.attempt_id WHERE r.id = ? AND a.id = ? AND a.project_id = ? AND r.lifecycle_state IN ('removed', 'retained'))").bind(&entry.subject_id).bind(attempt.to_string()).bind(entry.project_id.to_string()).fetch_one(&mut **tx).await,
        GcOperation::RepairWorktrees => sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM repair_worktrees WHERE recovery_id = ? AND project_id = ? AND state IN ('removed', 'retained'))").bind(&entry.subject_id).bind(entry.project_id.to_string()).fetch_one(&mut **tx).await,
        GcOperation::SuccessLogCompression => sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM success_log_archives l JOIN attempts a ON a.id = l.attempt_id WHERE a.id = ? AND a.project_id = ? AND l.stream = ? AND l.retired = 1)").bind(attempt.to_string()).bind(entry.project_id.to_string()).bind(entry.subject_id.rsplit(':').next().unwrap_or("")).fetch_one(&mut **tx).await,
    }.map_err(|e| db("read GC terminal state", e))?;
    Ok(done != 0)
}
