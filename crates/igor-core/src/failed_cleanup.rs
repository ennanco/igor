use std::{
    collections::{BTreeMap, BTreeSet},
    fs::File,
    path::{Component, Path, PathBuf},
};

use serde_json::Value;
use sqlx::Row;

use super::{
    ArtifactRepository, PersistenceError, PersistenceResult, db, decode_artifact, from_json,
    hash_artifact_file, insert_event, parse_id, replacement_generation_succeeded,
    valid_artifact_relative_path,
};
use crate::{
    ActionId, ArtifactPublicationState, AttemptId, AttemptSpec, Event, EventId, EventKind,
    EventPayload, FailedRetention, RuntimePaths,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FailedCleanupOutcome {
    Completed,
    Deferred,
    MoreWork,
}

impl ArtifactRepository<'_> {
    /// Candidate discovery is advisory; cleanup rechecks eligibility under a write lock.
    pub async fn failed_cleanup_candidates(
        &self,
        after: Option<AttemptId>,
    ) -> PersistenceResult<Vec<AttemptId>> {
        let ids: Vec<String> = sqlx::query_scalar(
            "SELECT DISTINCT a.id FROM attempts a JOIN artifacts r ON r.attempt_id = a.id
             JOIN diagnosed_attempts d ON d.attempt_id = a.id
             WHERE r.lifecycle_state = 'failed' AND r.state IN ('registered', 'staged')
               AND json_type(a.spec_json, '$.result.failed_artifact_retention') = 'object'
               AND a.id > COALESCE(?, '')
             ORDER BY a.id LIMIT 32",
        )
        .bind(after.map(|id| id.to_string()))
        .fetch_all(&self.database.pool)
        .await
        .map_err(|source| db("find failed artifact cleanup candidates", source))?;
        ids.into_iter()
            .map(|id| parse_id(&id, "cleanup attempt"))
            .collect()
    }

    /// Removes a bounded batch; missing copies require durable prepared evidence.
    pub async fn cleanup_failed_artifacts(
        &self,
        attempt_id: AttemptId,
        paths: &RuntimePaths,
    ) -> PersistenceResult<usize> {
        self.cleanup_failed_pass(attempt_id, paths, None, true, None)
            .await?;
        Ok(self
            .cleanup_failed_pass(attempt_id, paths, None, false, None)
            .await?
            .0)
    }

    pub async fn cleanup_failed_artifacts_claimed(
        &self,
        claim: &super::Claim<ActionId>,
        paths: &RuntimePaths,
    ) -> PersistenceResult<FailedCleanupOutcome> {
        let work =
            self.database
                .actions()
                .claimed(claim)
                .await?
                .ok_or(PersistenceError::Conflict {
                    entity: "failed cleanup claim",
                })?;
        let attempt_id = work.attempt_id.ok_or(PersistenceError::Conflict {
            entity: "failed cleanup attempt",
        })?;
        if work.action.kind != "cleanup_failed_artifacts"
            || work.action.spec.get("schema_version") != Some(&Value::from(1))
            || work.action.spec.get("attempt_id") != Some(&Value::from(attempt_id.to_string()))
        {
            return Err(PersistenceError::Conflict {
                entity: "failed cleanup action spec",
            });
        }
        self.cleanup_failed_pass(attempt_id, paths, Some(claim), true, None)
            .await?;
        Ok(self
            .cleanup_failed_pass(attempt_id, paths, Some(claim), false, None)
            .await?
            .1)
    }

    pub(super) async fn cleanup_failed_selected(
        &self,
        attempt_id: AttemptId,
        paths: &RuntimePaths,
        selected: &[super::GcEntry],
    ) -> PersistenceResult<usize> {
        self.cleanup_failed_pass(attempt_id, paths, None, true, Some(selected))
            .await?;
        Ok(self
            .cleanup_failed_pass(attempt_id, paths, None, false, Some(selected))
            .await?
            .0)
    }

    async fn cleanup_failed_pass(
        &self,
        attempt_id: AttemptId,
        paths: &RuntimePaths,
        claim: Option<&super::Claim<ActionId>>,
        prepare: bool,
        selected: Option<&[super::GcEntry]>,
    ) -> PersistenceResult<(usize, FailedCleanupOutcome)> {
        let mut transaction = self
            .database
            .pool
            .begin()
            .await
            .map_err(|source| db("begin failed artifact cleanup", source))?;
        // Reserve SQLite's writer before eligibility reads. Claims and diagnosis writers
        // cannot change the decision during the trusted, same-user filesystem operation.
        sqlx::query("UPDATE artifacts SET updated_at = updated_at WHERE attempt_id = ? AND lifecycle_state = 'failed' AND state IN ('registered', 'staged')")
            .bind(attempt_id.to_string()).execute(&mut *transaction).await
            .map_err(|source| db("reserve failed artifact cleanup", source))?;
        if !super::gc::validate_selection(&mut transaction, paths, selected, prepare).await? {
            return Ok((0, FailedCleanupOutcome::Deferred));
        }
        if let Some(claim) = claim {
            let valid = sqlx::query("UPDATE actions SET updated_at = updated_at WHERE id = ? AND kind = 'cleanup_failed_artifacts' AND attempt_id = ? AND state = 'running' AND claim_id = ? AND claim_owner = ? AND claim_expires_at > strftime('%Y-%m-%dT%H:%M:%fZ', 'now') AND project_id = (SELECT project_id FROM attempts WHERE id = ?) AND job_id = (SELECT job_id FROM attempts WHERE id = ?)")
                .bind(claim.record_id.to_string()).bind(attempt_id.to_string()).bind(claim.lease_id.to_string()).bind(&claim.owner)
                .bind(attempt_id.to_string()).bind(attempt_id.to_string()).execute(&mut *transaction).await.map_err(|e| db("fence failed cleanup claim", e))?;
            if valid.rows_affected() != 1 {
                return Err(PersistenceError::Conflict {
                    entity: "failed cleanup claim",
                });
            }
        }
        let row = read_attempt(&mut transaction, attempt_id).await?;
        let attempt: AttemptSpec = from_json(row.get("spec_json"), "cleanup attempt spec")?;
        let Some(policy) = attempt.result().failed_artifact_retention.as_ref() else {
            return Ok((0, FailedCleanupOutcome::Completed));
        };
        policy.validate()?;
        if attempt.result().publication_root.is_none() {
            return Ok((0, FailedCleanupOutcome::Completed));
        }
        if eligibility_barrier(&row).is_some() {
            return Ok((0, FailedCleanupOutcome::Deferred));
        }
        let artifacts = sqlx::query(
            "SELECT id, attempt_id, path, role, role_detail, sha256, size_bytes, state, lifecycle_state, diagnosis_required, metadata_json
             FROM artifacts WHERE attempt_id = ? AND lifecycle_state = 'failed'
              AND state IN ('registered', 'staged') AND (?2 IS NULL OR id IN (SELECT value FROM json_each(?2))) ORDER BY id LIMIT 64",
        ).bind(attempt_id.to_string()).bind(super::gc::selected_ids(selected)).fetch_all(&mut *transaction).await
            .map_err(|source| db("read failed cleanup artifacts", source))?
            .into_iter().map(decode_artifact).collect::<PersistenceResult<Vec<_>>>()?;
        for artifact in &artifacts {
            if !valid_artifact_relative_path(&artifact.path) {
                return Err(PersistenceError::Conflict {
                    entity: "cleanup artifact path",
                });
            }
        }
        if artifacts.is_empty() {
            return Ok((0, FailedCleanupOutcome::Completed));
        }
        if row.get::<i64, _>("registered") != 1 {
            return Ok((0, FailedCleanupOutcome::Deferred));
        }
        let Some(publication_root) = attempt.result().publication_root.clone() else {
            return Ok((0, FailedCleanupOutcome::Completed));
        };
        let project_root = PathBuf::from(row.get::<String, _>("root_path"));
        if !valid_absolute_path(&project_root)
            || !valid_absolute_path(&publication_root)
            || publication_root == project_root
            || !publication_root.starts_with(&project_root)
            || attempt
                .result()
                .extractor
                .as_ref()
                .is_some_and(|extractor| {
                    !valid_absolute_path(&extractor.cwd)
                        || !extractor.cwd.starts_with(&project_root)
                })
        {
            return Err(PersistenceError::Conflict {
                entity: "cleanup project publication root",
            });
        }
        if row.get::<&str, _>("job_state") == "superseded" {
            for artifact in &artifacts {
                if !replacement_generation_succeeded(&mut transaction, artifact.id).await? {
                    return Ok((0, FailedCleanupOutcome::Deferred));
                }
            }
        }
        let attempt_dir = paths.log_dir.join(attempt_id.to_string());
        if row
            .get::<Option<String>, _>("stdout_path")
            .map(PathBuf::from)
            .as_ref()
            != Some(&attempt_dir.join("stdout.log"))
            || row
                .get::<Option<String>, _>("stderr_path")
                .map(PathBuf::from)
                .as_ref()
                != Some(&attempt_dir.join("stderr.log"))
        {
            return Err(PersistenceError::Conflict {
                entity: "cleanup log paths",
            });
        }
        let mut patterns = globset::GlobSetBuilder::new();
        for pattern in &policy.keep_on_failure {
            patterns.add(globset::Glob::new(pattern).map_err(|error| {
                PersistenceError::InvalidValue {
                    entity: "failed artifact keep pattern",
                    value: error.to_string(),
                }
            })?);
        }
        let patterns = patterns
            .build()
            .map_err(|error| PersistenceError::InvalidValue {
                entity: "failed artifact keep patterns",
                value: error.to_string(),
            })?;
        let (kept, removed): (Vec<_>, Vec<_>) = artifacts.into_iter().partition(|artifact| {
            policy.policy == FailedRetention::Keep || patterns.is_match(&artifact.path)
        });
        let mut evidence = BTreeMap::new();
        let mut prepared = BTreeSet::new();
        for artifact in &removed {
            let identity = cleanup_identity(artifact, &attempt, paths, &row);
            let prior: Option<String> = sqlx::query_scalar("SELECT identity_json FROM failed_cleanup_intents WHERE artifact_id = ? AND state = 'prepared'")
                .bind(artifact.id.to_string()).fetch_optional(&mut *transaction).await.map_err(|e| db("read failed cleanup intent", e))?;
            if let Some(prior) = prior {
                if from_json::<Value>(&prior, "failed cleanup identity")? != identity {
                    return Err(PersistenceError::Conflict {
                        entity: "failed cleanup immutable identity",
                    });
                }
                prepared.insert(artifact.id);
            } else if !prepare {
                return Err(PersistenceError::Conflict {
                    entity: "failed cleanup unprepared identity",
                });
            }
            evidence.insert(artifact.id, identity);
        }
        let paths = paths.clone();
        let removal_batch = removed.clone();
        // The blocking worker owns the transaction as well: aborting its async caller must
        // not release the writer while a detached filesystem operation is still running.
        let (mut transaction, removal_result) = tokio::task::spawn_blocking(move || {
            let result = (|| -> PersistenceResult<()> {
                if surviving_directory(&project_root)? != project_root {
                    return Err(PersistenceError::Conflict {
                        entity: "cleanup project root",
                    });
                }
                surviving_directory(&publication_root)?;
                let (copies, directories) = preflight(
                    &removal_batch,
                    attempt_id,
                    &paths,
                    &publication_root,
                    &prepared,
                )?;
                if !prepare {
                    for copy in copies {
                        std::fs::remove_file(copy)?;
                    }
                    for directory in directories {
                        File::open(directory)?.sync_all()?;
                    }
                }
                Ok(())
            })();
            (transaction, result)
        })
        .await?;
        removal_result?;
        if prepare {
            for (id, identity) in &evidence {
                sqlx::query("INSERT INTO failed_cleanup_intents (artifact_id, identity_json) SELECT ?, ? WHERE NOT EXISTS (SELECT 1 FROM failed_cleanup_intents WHERE artifact_id = ?)")
                    .bind(id.to_string()).bind(identity.to_string()).bind(id.to_string()).execute(&mut *transaction).await.map_err(|e| db("prepare failed cleanup intent", e))?;
            }
            renew_cleanup_claim(
                &mut transaction,
                claim,
                attempt_id,
                row.get("project_id"),
                row.get("job_id"),
            )
            .await?;
            transaction
                .commit()
                .await
                .map_err(|e| db("commit failed cleanup intents", e))?;
            return Ok((0, FailedCleanupOutcome::MoreWork));
        }
        let project_id = parse_id(row.get("project_id"), "cleanup project")?;
        let job_id = parse_id(row.get("job_id"), "cleanup job")?;
        for (artifacts, state) in [(&kept, "retained"), (&removed, "removed")] {
            for artifact in artifacts {
                let identity = evidence.get(&artifact.id).cloned().unwrap_or_else(|| serde_json::json!({"artifact_id":artifact.id, "attempt_id":attempt_id, "path":artifact.path,"role":artifact.role,"sha256":artifact.sha256,"size_bytes":artifact.size_bytes,"state":artifact.state,"metadata":artifact.metadata,"policy":policy,"project_root":row.get::<String, _>("root_path")}));
                let decided: Option<String> = sqlx::query_scalar(
                    "SELECT decided_at FROM failed_cleanup_intents WHERE artifact_id = ?",
                )
                .bind(artifact.id.to_string())
                .fetch_optional(&mut *transaction)
                .await
                .map_err(|e| db("read failed cleanup decision", e))?;
                if !super::cleanup_evidence::record(
                    &mut transaction,
                    "failed_artifact_retention",
                    &artifact.id.to_string(),
                    state,
                    &identity,
                    decided.as_deref(),
                    claim.map(|c| c.record_id),
                )
                .await?
                {
                    continue;
                }
                sqlx::query("UPDATE artifacts SET lifecycle_state = ?, state = CASE WHEN ? = 'removed' THEN 'removed' ELSE state END, updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now') WHERE id = ?")
                    .bind(state).bind(state).bind(artifact.id.to_string())
                    .execute(&mut *transaction).await.map_err(|source| db("record failed artifact cleanup", source))?;
                if state == "removed" {
                    sqlx::query("UPDATE failed_cleanup_intents SET state = 'completed' WHERE artifact_id = ?")
                        .bind(artifact.id.to_string()).execute(&mut *transaction).await.map_err(|e| db("complete failed cleanup intent", e))?;
                }
                let event = Event::new(
                    EventId::new(),
                    EventKind::CleanupStateChanged,
                    EventPayload::new(
                        EventKind::CleanupStateChanged,
                        1,
                        serde_json::json!({
                            "artifact_id": artifact.id, "attempt_id": attempt_id,
                            "from": "failed", "state": state, "reason": "failed_artifact_retention",
                        }),
                    )?,
                )?;
                insert_event(
                    &mut transaction,
                    project_id,
                    Some(job_id),
                    Some(attempt_id),
                    &event,
                )
                .await?;
            }
        }
        let remaining: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM artifacts WHERE attempt_id = ? AND lifecycle_state = 'failed' AND state IN ('registered', 'staged')")
            .bind(attempt_id.to_string()).fetch_one(&mut *transaction).await.map_err(|e| db("read remaining failed cleanup", e))?;
        renew_cleanup_claim(
            &mut transaction,
            claim,
            attempt_id,
            row.get("project_id"),
            row.get("job_id"),
        )
        .await?;
        transaction
            .commit()
            .await
            .map_err(|source| db("commit failed artifact cleanup", source))?;
        Ok((
            removed.len(),
            if remaining == 0 {
                FailedCleanupOutcome::Completed
            } else {
                FailedCleanupOutcome::MoreWork
            },
        ))
    }
}

async fn renew_cleanup_claim(
    transaction: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    claim: Option<&super::Claim<ActionId>>,
    attempt_id: AttemptId,
    project_id: &str,
    job_id: &str,
) -> PersistenceResult<()> {
    let Some(claim) = claim else {
        return Ok(());
    };
    // Expiry was fenced before blocking I/O while reserving this writer. No competing
    // claimant can write until commit, so elapsed time alone must not prevent renewal.
    let renewed = sqlx::query(
        "UPDATE actions SET claim_expires_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now', '+30 seconds'),
         updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
         WHERE id = ? AND kind = 'cleanup_failed_artifacts' AND attempt_id = ?
           AND project_id = ? AND job_id = ? AND state = 'running'
           AND claim_id = ? AND claim_owner = ?",
    )
    .bind(claim.record_id.to_string())
    .bind(attempt_id.to_string())
    .bind(project_id)
    .bind(job_id)
    .bind(claim.lease_id.to_string())
    .bind(&claim.owner)
    .execute(&mut **transaction)
    .await
    .map_err(|source| db("renew failed cleanup claim", source))?;
    if renewed.rows_affected() != 1 {
        return Err(PersistenceError::Conflict {
            entity: "failed cleanup claim",
        });
    }
    Ok(())
}

fn valid_absolute_path(path: &Path) -> bool {
    path.is_absolute()
        && path.to_str().is_some_and(|raw| {
            !raw.contains('\0') && !raw.split('/').any(|part| matches!(part, "." | ".."))
        })
}

fn preflight(
    artifacts: &[super::ArtifactRecord],
    attempt_id: AttemptId,
    paths: &RuntimePaths,
    publication_root: &Path,
    prepared: &BTreeSet<uuid::Uuid>,
) -> PersistenceResult<(BTreeSet<PathBuf>, BTreeSet<PathBuf>)> {
    let mut copies = BTreeSet::new();
    let mut directories = BTreeSet::new();
    for artifact in artifacts {
        let relative = Path::new(&artifact.path);
        if !valid_artifact_relative_path(&artifact.path)
            || super::success_log_archives::reserved_log_name(relative)
        {
            return Err(PersistenceError::Conflict {
                entity: "cleanup artifact path",
            });
        }
        if artifact.size_bytes.is_none() || artifact.sha256.is_none() {
            return Err(PersistenceError::Conflict {
                entity: "cleanup artifact identity",
            });
        }
        let staged_relative = PathBuf::from("artifact-staging")
            .join(attempt_id.to_string())
            .join(artifact.id.to_string());
        let staged = paths.state_dir.join(&staged_relative);
        let metadata_staged = artifact.metadata.get("staged_path").and_then(Value::as_str);
        if (artifact.state == ArtifactPublicationState::Staged && metadata_staged.is_none())
            || metadata_staged.is_some_and(|value| Path::new(value) != staged)
            || artifact.metadata.get("published_path").is_some()
        {
            return Err(PersistenceError::Conflict {
                entity: "cleanup artifact staging path",
            });
        }
        for (relative, entity) in [
            (
                PathBuf::from(".igor-publication-staging")
                    .join(attempt_id.to_string())
                    .join(artifact.id.to_string()),
                "cleanup publication preparation",
            ),
            (
                PathBuf::from(attempt_id.to_string()).join(artifact.id.to_string()),
                "cleanup final publication copy",
            ),
        ] {
            if existing_file(publication_root, &relative)?.is_some() {
                return Err(PersistenceError::Conflict { entity });
            }
        }
        for (root, relative) in [
            (
                &paths.log_dir,
                PathBuf::from(attempt_id.to_string()).join(relative),
            ),
            (&paths.state_dir, staged_relative),
        ] {
            if root
                .join(&relative)
                .starts_with(publication_root.join(attempt_id.to_string()))
            {
                return Err(PersistenceError::Conflict {
                    entity: "cleanup final publication path",
                });
            }
            let existing = existing_file(root, &relative)?;
            if existing.is_none()
                && !prepared.contains(&artifact.id)
                && (root == &paths.log_dir || artifact.state == ArtifactPublicationState::Staged)
            {
                return Err(PersistenceError::Conflict {
                    entity: "failed cleanup missing required copy",
                });
            }
            if let Some(file) = existing {
                if root == &paths.state_dir && metadata_staged.is_none() {
                    return Err(PersistenceError::Conflict {
                        entity: "cleanup artifact staging path",
                    });
                }
                let (size, digest) = hash_artifact_file(root, &relative)?;
                if artifact.size_bytes != Some(size) || artifact.sha256.as_deref() != Some(&digest)
                {
                    return Err(PersistenceError::Conflict {
                        entity: "cleanup artifact identity",
                    });
                }
                copies.insert(file);
            }
            if let Some(parent) = root.join(&relative).parent() {
                directories.insert(surviving_directory(parent)?);
            }
        }
    }
    Ok((copies, directories))
}

fn cleanup_identity(
    artifact: &super::ArtifactRecord,
    attempt: &AttemptSpec,
    paths: &RuntimePaths,
    row: &sqlx::sqlite::SqliteRow,
) -> Value {
    serde_json::json!({
        "schema_version": 1, "artifact_id": artifact.id, "attempt_id": artifact.attempt_id,
        "path": artifact.path, "role": artifact.role, "sha256": artifact.sha256,
        "size_bytes": artifact.size_bytes, "metadata": artifact.metadata,
        "state": artifact.state, "project_root": PathBuf::from(row.get::<String, _>("root_path")),
        "lifecycle": artifact.lifecycle, "diagnosis_required": artifact.diagnosis_required,
        "project_id": row.get::<String, _>("project_id"), "job_id": row.get::<String, _>("job_id"),
        "publication_root": attempt.result().publication_root, "policy": attempt.result().failed_artifact_retention,
        "attempt_spec": attempt, "stdout_path": row.get::<Option<String>, _>("stdout_path"),
        "stderr_path": row.get::<Option<String>, _>("stderr_path"),
        "source": paths.log_dir.join(artifact.attempt_id.to_string()).join(&artifact.path),
        "staged": paths.state_dir.join("artifact-staging").join(artifact.attempt_id.to_string()).join(artifact.id.to_string()),
    })
}

pub(super) async fn preview(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    attempt_id: AttemptId,
    paths: &RuntimePaths,
    output: &mut Vec<super::GcEntry>,
    batches: &mut Vec<super::gc::GcArtifactBatch>,
    selected: Option<&[super::GcEntry]>,
) -> PersistenceResult<()> {
    use super::{GcEffect as E, GcEntry, GcOperation as O, GcStatus as S, GcTarget};
    let row = read_attempt(tx, attempt_id).await?;
    let attempt: AttemptSpec = from_json(row.get("spec_json"), "failed GC spec")?;
    let (artifacts, batch) =
        super::gc::artifact_batch(tx, attempt_id, O::FailedArtifacts, selected).await?;
    batches.push(batch);
    let project_id = parse_id(row.get("project_id"), "GC project")?;
    let mut entries: Vec<_> = artifacts
        .iter()
        .map(|a| {
            GcEntry::new(
                O::FailedArtifacts,
                a.id.to_string(),
                project_id,
                Some(attempt_id),
            )
        })
        .collect();
    let result: PersistenceResult<()> = async {
        let Some(policy) = &attempt.result().failed_artifact_retention else {
            for e in &mut entries { e.decide(S::Retain, "legacy_policy_absent", E::Retain); }
            return Ok(());
        };
        policy.validate()?;
        let Some(publication) = &attempt.result().publication_root else {
            for e in &mut entries { e.decide(S::Retain, "publication_root_absent", E::Retain); }
            return Ok(());
        };
        let barrier = eligibility_barrier(&row).or_else(|| (row.get::<i64,_>("registered") != 1).then_some("project_unregistered"));
        if let Some(reason) = barrier { for e in &mut entries { e.decide(S::Deferred, reason, E::Retain); } return Ok(()); }
        for a in &artifacts { if !valid_artifact_relative_path(&a.path) { return Err(PersistenceError::Conflict { entity: "cleanup artifact path" }); } }
        let project_root = PathBuf::from(row.get::<String,_>("root_path"));
        if !valid_absolute_path(&project_root) || !valid_absolute_path(publication) || publication == &project_root || !publication.starts_with(&project_root)
            || attempt.result().extractor.as_ref().is_some_and(|x| !valid_absolute_path(&x.cwd) || !x.cwd.starts_with(&project_root)) {
            return Err(PersistenceError::Conflict { entity: "cleanup project publication root" });
        }
        if row.get::<&str,_>("job_state") == "superseded" {
            for a in &artifacts { if !replacement_generation_succeeded(tx, a.id).await? { for e in &mut entries { e.decide(S::Deferred, "current_replacement_pending", E::Retain); } return Ok(()); } }
        }
        let dir = paths.log_dir.join(attempt_id.to_string());
        if row.get::<Option<String>,_>("stdout_path").as_deref() != dir.join("stdout.log").to_str() || row.get::<Option<String>,_>("stderr_path").as_deref() != dir.join("stderr.log").to_str() { return Err(PersistenceError::Conflict { entity: "cleanup log paths" }); }
        let mut builder = globset::GlobSetBuilder::new();
        for p in &policy.keep_on_failure { builder.add(globset::Glob::new(p).map_err(|_| PersistenceError::Conflict { entity: "cleanup pattern" })?); }
        let patterns = builder.build().map_err(|_| PersistenceError::Conflict { entity: "cleanup patterns" })?;
        let removed: Vec<_> = artifacts.iter().filter(|a| policy.policy != FailedRetention::Keep && !patterns.is_match(&a.path)).cloned().collect();
        let mut prepared = BTreeSet::new();
        for a in &removed {
            let prior: Option<String> = sqlx::query_scalar("SELECT identity_json FROM failed_cleanup_intents WHERE artifact_id = ? AND state = 'prepared'").bind(a.id.to_string()).fetch_optional(&mut **tx).await.map_err(|e| db("read failed GC intent", e))?;
            if let Some(prior) = prior { if from_json::<Value>(&prior, "failed GC intent")? != cleanup_identity(a, &attempt, paths, &row) { return Err(PersistenceError::Conflict { entity: "failed cleanup immutable identity" }); } prepared.insert(a.id); }
        }
        let worker_paths = paths.clone(); let publication = publication.clone(); let checked = removed.clone();
        let files = tokio::task::spawn_blocking(move || {
            if surviving_directory(&project_root)? != project_root { return Err(PersistenceError::Conflict { entity: "cleanup project root" }); }
            surviving_directory(&publication)?;
            preflight(&checked, attempt_id, &worker_paths, &publication, &prepared).map(|x| x.0)
        }).await??;
        for (a, e) in artifacts.iter().zip(&mut entries) {
            if removed.iter().any(|r| r.id == a.id) {
                let staged = paths.state_dir.join("artifact-staging").join(attempt_id.to_string()).join(a.id.to_string());
                let targets: Vec<_> = files.iter().filter(|p| **p == dir.join(&a.path) || **p == staged).cloned().map(|path| GcTarget { path, bytes: a.size_bytes, effect: E::Remove }).collect();
                e.decide(if targets.is_empty() { S::MetadataOnly } else { S::Ready }, "verified_failed_retention", if targets.is_empty() { E::ResumeMetadata } else { E::Remove });
                e.targets = targets;
            } else { e.decide(S::Retain, "keep_policy", E::Retain); }
        }
        Ok(())
    }.await;
    if let Err(error) = result {
        GcEntry::unsafe_batch(&mut entries, &error);
    }
    output.extend(entries);
    Ok(())
}

async fn read_attempt(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    attempt_id: AttemptId,
) -> PersistenceResult<sqlx::sqlite::SqliteRow> {
    sqlx::query("SELECT a.spec_json, a.state, a.project_id, a.job_id, j.state AS job_state, registry.root_path, registry.registered,
        COALESCE(u.stdout_path, p.stdout_path, c.stdout_path) AS stdout_path, COALESCE(u.stderr_path, p.stderr_path, c.stderr_path) AS stderr_path,
        EXISTS (SELECT 1 FROM diagnosed_attempts WHERE attempt_id = a.id) AS diagnosed,
        EXISTS (SELECT 1 FROM actions WHERE attempt_id = a.id AND kind = 'extract_metrics' AND state IN ('pending', 'running')) AS extracting,
        EXISTS (SELECT 1 FROM failed_log_tails WHERE attempt_id = a.id AND complete = 1) AS tails
        FROM attempts a JOIN jobs j ON j.id = a.job_id JOIN projects registry ON registry.id = a.project_id
        LEFT JOIN attempt_units u ON u.attempt_id = a.id LEFT JOIN attempt_processes p ON p.attempt_id = a.id LEFT JOIN attempt_containers c ON c.attempt_id = a.id WHERE a.id = ?")
        .bind(attempt_id.to_string()).fetch_optional(&mut **tx).await.map_err(|e| db("read failed cleanup attempt", e))?.ok_or(PersistenceError::NotFound { entity: "cleanup attempt" })
}

fn eligibility_barrier(row: &sqlx::sqlite::SqliteRow) -> Option<&'static str> {
    let state: &str = row.get("state");
    if !matches!(state, "succeeded" | "failed" | "lost" | "cancelled") {
        Some("attempt_nonterminal")
    } else if row.get::<i64, _>("diagnosed") == 0 {
        Some("diagnosis_pending")
    } else if row.get::<i64, _>("extracting") != 0 {
        Some("active_extraction")
    } else if state != "succeeded" && row.get::<i64, _>("tails") == 0 {
        Some("complete_failed_tails_pending")
    } else {
        None
    }
}

/// Return the nearest surviving directory, validating every ancestor from `/`.
fn surviving_directory(path: &Path) -> PersistenceResult<PathBuf> {
    if !valid_absolute_path(path) {
        return Err(PersistenceError::Conflict {
            entity: "cleanup directory path",
        });
    }
    let mut walked = PathBuf::new();
    let mut surviving = PathBuf::new();
    for part in path.components() {
        walked.push(part);
        match std::fs::symlink_metadata(&walked) {
            Ok(metadata) if metadata.file_type().is_dir() => surviving = walked.clone(),
            Ok(_) => {
                return Err(PersistenceError::Conflict {
                    entity: "cleanup directory type",
                });
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => break,
            Err(error) => return Err(error.into()),
        }
    }
    Ok(surviving)
}

/// Walk the full absolute lexical path without following links, even for absent leaves.
fn existing_file(root: &Path, relative: &Path) -> PersistenceResult<Option<PathBuf>> {
    if !valid_absolute_path(root) || !relative.to_str().is_some_and(valid_artifact_relative_path) {
        return Err(PersistenceError::Conflict {
            entity: "cleanup copy path",
        });
    }
    let target = root.join(relative);
    let mut candidate = PathBuf::new();
    for part in target.components() {
        if !matches!(part, Component::RootDir | Component::Normal(_)) {
            return Err(PersistenceError::Conflict {
                entity: "cleanup copy path",
            });
        }
        candidate.push(part);
        let metadata = match std::fs::symlink_metadata(&candidate) {
            Ok(metadata) => metadata,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(error.into()),
        };
        let valid = if candidate == target {
            metadata.file_type().is_file()
        } else {
            metadata.file_type().is_dir()
        };
        if !valid {
            return Err(PersistenceError::Conflict {
                entity: "cleanup copy type",
            });
        }
    }
    Ok(Some(candidate))
}
