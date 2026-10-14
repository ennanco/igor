use std::{
    collections::BTreeSet,
    fs::File,
    path::{Component, Path, PathBuf},
};

use serde_json::{Value, json};
use sqlx::{Row, Sqlite, Transaction};

use super::{
    ArtifactRecord, ArtifactRepository, PersistenceError, PersistenceResult, db, decode_artifact,
    from_json, hash_artifact_file, insert_event, parse_id, replacement_generation_succeeded,
};
use crate::{
    AttemptId, AttemptSpec, Event, EventId, EventKind, EventPayload, RuntimePaths,
    SupersededRetention,
};

fn conflict(entity: &'static str) -> PersistenceError {
    PersistenceError::Conflict { entity }
}

#[derive(Clone)]
struct CopyIdentity {
    artifact: ArtifactRecord,
    roots: Vec<(PathBuf, PathBuf)>,
    hidden: (PathBuf, PathBuf),
    evidence: Value,
    prepared: bool,
}

/// Check every ancestor, including the registered roots, without following links.
fn existing(root: &Path, relative: &Path) -> PersistenceResult<Option<PathBuf>> {
    if !root.is_absolute()
        || root
            .to_string_lossy()
            .split('/')
            .any(|c| c == "." || c == "..")
        || !relative
            .components()
            .all(|c| matches!(c, Component::Normal(_)))
    {
        return Err(conflict("superseded cleanup copy path"));
    }
    let target = root.join(relative);
    let mut walked = PathBuf::new();
    for part in target.components() {
        if !matches!(part, Component::RootDir | Component::Normal(_)) {
            return Err(conflict("superseded cleanup copy path"));
        }
        walked.push(part);
        let metadata = match std::fs::symlink_metadata(&walked) {
            Ok(metadata) => metadata,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(error.into()),
        };
        if (walked == target && !metadata.is_file())
            || (walked != target && !metadata.is_dir())
            || metadata.file_type().is_symlink()
        {
            return Err(conflict("superseded cleanup copy type"));
        }
    }
    Ok(Some(target))
}

fn identity(
    artifact: ArtifactRecord,
    attempt: &AttemptSpec,
    paths: &RuntimePaths,
    project_root: &Path,
) -> PersistenceResult<CopyIdentity> {
    let logical = Path::new(&artifact.path);
    // Path::components normalizes interior dots, so inspect the original spelling too.
    if artifact.path.is_empty()
        || artifact
            .path
            .split('/')
            .any(|c| c.is_empty() || c == "." || c == "..")
        || !logical
            .components()
            .all(|c| matches!(c, Component::Normal(_)))
        || logical.file_name().is_some_and(|n| {
            matches!(
                n.to_str(),
                Some("stdout.log" | "stderr.log" | "stdout.log.gz" | "stderr.log.gz")
            )
        })
    {
        return Err(conflict("superseded cleanup logical path"));
    }
    if artifact.size_bytes.is_none() || artifact.sha256.is_none() {
        return Err(conflict("superseded cleanup identity"));
    }
    let publication = attempt
        .result()
        .publication_root
        .as_ref()
        .ok_or_else(|| conflict("superseded cleanup publication root"))?;
    if !publication.is_absolute()
        || publication == project_root
        || !publication.starts_with(project_root)
        || publication
            .to_string_lossy()
            .split('/')
            .any(|c| c == "." || c == "..")
        || !publication
            .components()
            .all(|c| matches!(c, Component::RootDir | Component::Normal(_)))
    {
        return Err(conflict("superseded cleanup publication root"));
    }
    let id = artifact.attempt_id.to_string();
    let staged = PathBuf::from("artifact-staging")
        .join(&id)
        .join(artifact.id.to_string());
    let final_path = PathBuf::from(&id).join(artifact.id.to_string());
    if artifact
        .metadata
        .get("published_path")
        .and_then(Value::as_str)
        .map(|s| Path::new(s).as_os_str())
        != Some(publication.join(&final_path).as_os_str())
    {
        return Err(conflict("superseded cleanup published metadata"));
    }
    if let Some(value) = artifact.metadata.get("staged_path")
        && value.as_str().map(|s| Path::new(s).as_os_str())
            != Some(paths.state_dir.join(&staged).as_os_str())
    {
        return Err(conflict("superseded cleanup staged metadata"));
    }
    let roots = vec![
        (paths.log_dir.clone(), PathBuf::from(&id).join(logical)),
        (paths.state_dir.clone(), staged),
        (publication.clone(), final_path),
    ];
    let hidden = (
        publication.clone(),
        PathBuf::from(".igor-publication-staging")
            .join(id)
            .join(artifact.id.to_string()),
    );
    let evidence = json!({"artifact_id":artifact.id,"attempt_id":artifact.attempt_id,
        "path":artifact.path,"role":artifact.role,"size_bytes":artifact.size_bytes,
        "sha256":artifact.sha256,"metadata":artifact.metadata,"copies":roots,
        "hidden":hidden,"project_root":project_root,"retention":attempt.result().superseded_artifact_retention});
    Ok(CopyIdentity {
        artifact,
        roots,
        hidden,
        evidence,
        prepared: false,
    })
}

/// Returns verified existing copies and directories to fsync. No unlink occurs here.
fn preflight(batch: &[CopyIdentity]) -> PersistenceResult<(BTreeSet<PathBuf>, BTreeSet<PathBuf>)> {
    let mut files = BTreeSet::new();
    let mut directories = BTreeSet::new();
    for item in batch {
        if existing(&item.hidden.0, &item.hidden.1)?.is_some() {
            return Err(conflict("superseded cleanup publication preparation"));
        }
        for (index, (root, relative)) in item.roots.iter().enumerate() {
            match existing(root, relative)? {
                Some(file) => {
                    if index == 1 && item.artifact.metadata.get("staged_path").is_none() {
                        return Err(conflict("superseded cleanup undeclared staging copy"));
                    }
                    let (size, digest) = hash_artifact_file(root, relative)?;
                    if item.artifact.size_bytes != Some(size)
                        || item.artifact.sha256.as_deref() != Some(&digest)
                    {
                        return Err(conflict("superseded cleanup copy identity"));
                    }
                    files.insert(file);
                }
                None if index == 1 && item.artifact.metadata.get("staged_path").is_none() => {}
                None if !item.prepared => {
                    return Err(conflict("superseded cleanup missing copy without intent"));
                }
                None => {}
            }
            // Sync surviving ancestors on replay as well as immediate unlink parents.
            let target = root.join(relative);
            let mut parent = target.parent();
            while let Some(path) = parent {
                if std::fs::symlink_metadata(path)
                    .is_ok_and(|m| m.is_dir() && !m.file_type().is_symlink())
                {
                    directories.insert(path.to_path_buf());
                    break;
                }
                parent = path.parent();
            }
        }
    }
    Ok((files, directories))
}

async fn reserve(
    transaction: &mut Transaction<'_, Sqlite>,
    attempt_id: AttemptId,
) -> PersistenceResult<()> {
    sqlx::query("UPDATE artifacts SET updated_at = updated_at WHERE attempt_id = ?")
        .bind(attempt_id.to_string())
        .execute(&mut **transaction)
        .await
        .map_err(|e| db("reserve superseded cleanup writer", e))?;
    Ok(())
}

async fn check_log_paths(
    transaction: &mut Transaction<'_, Sqlite>,
    attempt_id: AttemptId,
    paths: &RuntimePaths,
) -> PersistenceResult<()> {
    let (stdout, stderr): (Option<String>, Option<String>) = sqlx::query_as(
        "SELECT COALESCE(u.stdout_path, p.stdout_path, c.stdout_path),
                COALESCE(u.stderr_path, p.stderr_path, c.stderr_path)
         FROM attempts a
         LEFT JOIN attempt_units u ON u.attempt_id = a.id
         LEFT JOIN attempt_processes p ON p.attempt_id = a.id
         LEFT JOIN attempt_containers c ON c.attempt_id = a.id WHERE a.id = ?",
    )
    .bind(attempt_id.to_string())
    .fetch_one(&mut **transaction)
    .await
    .map_err(|e| db("read superseded cleanup log paths", e))?;
    let attempt_dir = paths.log_dir.join(attempt_id.to_string());
    if stdout.as_deref() != attempt_dir.join("stdout.log").to_str()
        || stderr.as_deref() != attempt_dir.join("stderr.log").to_str()
        || stdout.is_none()
        || stderr.is_none()
    {
        return Err(conflict("superseded cleanup log paths"));
    }
    Ok(())
}

impl ArtifactRepository<'_> {
    pub async fn superseded_cleanup_candidates(
        &self,
        after: Option<AttemptId>,
    ) -> PersistenceResult<Vec<AttemptId>> {
        let ids: Vec<String> = sqlx::query_scalar("SELECT DISTINCT a.id FROM attempts a JOIN jobs j ON j.id = a.job_id JOIN artifacts r ON r.attempt_id = a.id
            WHERE a.state = 'succeeded' AND j.state = 'superseded' AND r.state = 'published' AND r.lifecycle_state = 'superseded'
            AND json_type(a.spec_json, '$.result.superseded_artifact_retention') = 'object'
            AND a.id > COALESCE(?, '') ORDER BY a.id LIMIT 32")
            .bind(after.map(|id| id.to_string())).fetch_all(&self.database.pool).await
            .map_err(|e| db("find superseded cleanup candidates", e))?;
        ids.into_iter()
            .map(|id| parse_id(&id, "superseded cleanup attempt"))
            .collect()
    }

    pub async fn cleanup_superseded_artifacts(
        &self,
        attempt_id: AttemptId,
        paths: &RuntimePaths,
    ) -> PersistenceResult<usize> {
        self.cleanup_superseded_selected(attempt_id, paths, None)
            .await
    }

    pub(super) async fn cleanup_superseded_selected(
        &self,
        attempt_id: AttemptId,
        paths: &RuntimePaths,
        selected: Option<&[super::GcEntry]>,
    ) -> PersistenceResult<usize> {
        let mut transaction = self
            .database
            .pool
            .begin()
            .await
            .map_err(|e| db("begin superseded cleanup preparation", e))?;
        reserve(&mut transaction, attempt_id).await?;
        if !super::gc::validate_selection(&mut transaction, paths, selected, true).await? {
            return Ok(0);
        }
        let row = read_attempt(&mut transaction, attempt_id).await?;
        let Some(row) = row else { return Ok(0) };
        if eligibility_barrier(&row).is_some() {
            return Ok(0);
        }
        let attempt: AttemptSpec = from_json(row.get("spec_json"), "superseded cleanup spec")?;
        let Some(policy) = &attempt.result().superseded_artifact_retention else {
            return Ok(0);
        };
        policy.validate()?;
        let mut builder = globset::GlobSetBuilder::new();
        for pattern in &policy.keep_on_success {
            builder.add(
                globset::Glob::new(pattern).map_err(|_| conflict("superseded cleanup pattern"))?,
            );
        }
        let patterns = builder
            .build()
            .map_err(|_| conflict("superseded cleanup patterns"))?;
        let artifacts = sqlx::query("SELECT id, attempt_id, path, role, role_detail, sha256, size_bytes, state, lifecycle_state, diagnosis_required, metadata_json
            FROM artifacts WHERE attempt_id = ? AND state = 'published' AND lifecycle_state = 'superseded' AND (?2 IS NULL OR id IN (SELECT value FROM json_each(?2))) ORDER BY id LIMIT 64")
            .bind(attempt_id.to_string()).bind(super::gc::selected_ids(selected)).fetch_all(&mut *transaction).await.map_err(|e| db("read superseded cleanup artifacts", e))?
            .into_iter().map(decode_artifact).collect::<PersistenceResult<Vec<_>>>()?;
        let (kept, removed): (Vec<_>, Vec<_>) = artifacts.into_iter().partition(|a| {
            policy.policy == SupersededRetention::Keep || patterns.is_match(&a.path)
        });
        if !removed.is_empty() {
            check_log_paths(&mut transaction, attempt_id, paths).await?;
        }
        let project_id = parse_id(row.get("project_id"), "cleanup project")?;
        let job_id = parse_id(row.get("job_id"), "cleanup job")?;
        for artifact in &kept {
            if !super::cleanup_evidence::record(&mut transaction, "superseded_artifact_retention", &artifact.id.to_string(), "retained", &json!({"artifact_id":artifact.id,"attempt_id":attempt_id,"path":artifact.path,"role":artifact.role,"sha256":artifact.sha256,"size_bytes":artifact.size_bytes,"state":artifact.state,"metadata":artifact.metadata,"policy":policy}), None, None).await? {
                continue;
            }
            sqlx::query("UPDATE artifacts SET lifecycle_state = 'retained' WHERE id = ? AND state = 'published' AND lifecycle_state = 'superseded'")
                .bind(artifact.id.to_string()).execute(&mut *transaction).await.map_err(|e| db("retain superseded artifact", e))?;
            let event = Event::new(
                EventId::new(),
                EventKind::CleanupStateChanged,
                EventPayload::new(
                    EventKind::CleanupStateChanged,
                    1,
                    json!({"artifact_id":artifact.id,"attempt_id":attempt_id,"from":"superseded","state":"retained","reason":"superseded_artifact_retention"}),
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
        let project_root = PathBuf::from(row.get::<String, _>("root_path"));
        let mut batch = Vec::new();
        for artifact in removed {
            let mut item = identity(artifact, &attempt, paths, &project_root)?;
            let intent: Option<String> = sqlx::query_scalar("SELECT identity_json FROM superseded_cleanup_intents WHERE artifact_id = ? AND state = 'prepared'")
                .bind(item.artifact.id.to_string()).fetch_optional(&mut *transaction).await.map_err(|e| db("read superseded cleanup intent", e))?;
            if let Some(intent) = intent {
                let evidence: Value = from_json(&intent, "superseded cleanup evidence")?;
                if evidence != item.evidence {
                    return Err(conflict("superseded cleanup immutable identity"));
                }
                item.prepared = true;
            } else if !replacement_generation_succeeded(&mut transaction, item.artifact.id).await? {
                // Retention does not require a replacement; it never removes bytes.
                transaction
                    .commit()
                    .await
                    .map_err(|e| db("commit superseded artifact retention", e))?;
                return Ok(0);
            }
            batch.push(item);
        }
        let checked = batch.clone();
        let (returned, check) = tokio::task::spawn_blocking(move || {
            let check = preflight(&checked);
            (transaction, check)
        })
        .await?;
        transaction = returned;
        check?;
        for item in &batch {
            if !item.prepared {
                let replacement: String = sqlx::query_scalar("SELECT g.id FROM generations g JOIN jobs j ON j.family_id = g.family_id WHERE j.id = ? ORDER BY g.generation_number DESC LIMIT 1")
                    .bind(row.get::<String, _>("job_id")).fetch_one(&mut *transaction).await.map_err(|e| db("read authorized replacement", e))?;
                sqlx::query("INSERT INTO superseded_cleanup_intents (artifact_id, identity_json, replacement_generation_id) VALUES (?, ?, ?)")
                    .bind(item.artifact.id.to_string()).bind(item.evidence.to_string()).bind(replacement)
                    .execute(&mut *transaction).await.map_err(|e| db("prepare superseded cleanup intent", e))?;
            }
        }
        transaction
            .commit()
            .await
            .map_err(|e| db("commit superseded cleanup intent", e))?;
        if batch.is_empty() {
            return Ok(0);
        }

        // New writer transaction: prepared evidence is durable before ANY unlink.
        let mut transaction = self
            .database
            .pool
            .begin()
            .await
            .map_err(|e| db("begin superseded cleanup removal", e))?;
        reserve(&mut transaction, attempt_id).await?;
        if !super::gc::validate_selection(&mut transaction, paths, selected, false).await? {
            return Ok(0);
        }
        let eligible: i64 = sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM attempts a JOIN jobs j ON j.id = a.job_id WHERE a.id = ? AND a.state = 'succeeded' AND j.state = 'superseded' AND a.spec_json = ?)
            AND NOT EXISTS (SELECT 1 FROM actions WHERE attempt_id = ? AND kind = 'extract_metrics' AND state IN ('pending', 'running'))")
            .bind(attempt_id.to_string()).bind(row.get::<String, _>("spec_json")).bind(attempt_id.to_string())
            .fetch_one(&mut *transaction).await.map_err(|e| db("recheck superseded cleanup attempt", e))?;
        if eligible == 0 {
            return Ok(0);
        }
        check_log_paths(&mut transaction, attempt_id, paths).await?;
        let current_root: String =
            sqlx::query_scalar("SELECT root_path FROM projects WHERE id = ? AND registered = 1")
                .bind(row.get::<String, _>("project_id"))
                .fetch_one(&mut *transaction)
                .await
                .map_err(|e| db("recheck cleanup project", e))?;
        if Path::new(&current_root) != project_root {
            return Err(conflict("superseded cleanup project changed"));
        }
        let mut replacement_ok = true;
        for item in &mut batch {
            let current = sqlx::query("SELECT id, attempt_id, path, role, role_detail, sha256, size_bytes, state, lifecycle_state, diagnosis_required, metadata_json FROM artifacts WHERE id = ?")
                .bind(item.artifact.id.to_string()).fetch_one(&mut *transaction).await.map_err(|e| db("recheck cleanup artifact", e))?;
            if decode_artifact(current)? != item.artifact {
                return Err(conflict("superseded cleanup artifact changed"));
            }
            replacement_ok &=
                replacement_generation_succeeded(&mut transaction, item.artifact.id).await?;
            item.prepared = true;
        }
        // A cancelled caller cannot release the writer while its detached worker unlinks.
        let checked = batch.clone();
        let (returned, removal) = tokio::task::spawn_blocking(move || {
            let result = (|| -> PersistenceResult<BTreeSet<uuid::Uuid>> {
                let (files, directories) = preflight(&checked)?;
                let completed = checked
                    .iter()
                    .filter(|item| {
                        replacement_ok
                            || !item
                                .roots
                                .iter()
                                .any(|(root, relative)| files.contains(&root.join(relative)))
                    })
                    .map(|item| item.artifact.id)
                    .collect();
                if replacement_ok {
                    for file in files {
                        std::fs::remove_file(file)?;
                    }
                }
                for directory in directories {
                    File::open(directory)?.sync_all()?;
                }
                Ok(completed)
            })();
            (transaction, result)
        })
        .await?;
        transaction = returned;
        let completed = removal?;
        if completed.is_empty() {
            return Ok(0);
        }
        for artifact in batch
            .iter()
            .filter(|i| completed.contains(&i.artifact.id))
            .map(|i| &i.artifact)
        {
            let identity: String = sqlx::query_scalar(
                "SELECT identity_json FROM superseded_cleanup_intents WHERE artifact_id = ?",
            )
            .bind(artifact.id.to_string())
            .fetch_one(&mut *transaction)
            .await
            .map_err(|e| db("read superseded tombstone identity", e))?;
            if !super::cleanup_evidence::record(
                &mut transaction,
                "superseded_artifact_retention",
                &artifact.id.to_string(),
                "removed",
                &from_json::<Value>(&identity, "superseded identity")?,
                None,
                None,
            )
            .await?
            {
                continue;
            }
            if !replacement_ok {
                // Verified all absent in the worker while this transaction owned the writer.
                sqlx::query("UPDATE superseded_cleanup_intents SET completion_only = 1 WHERE artifact_id = ? AND state = 'prepared'")
                    .bind(artifact.id.to_string()).execute(&mut *transaction).await.map_err(|e| db("mark cleanup completion only", e))?;
            }
            sqlx::query("UPDATE artifacts SET lifecycle_state = 'removed', state = 'removed' WHERE id = ? AND state = 'published' AND lifecycle_state = 'superseded'")
                .bind(artifact.id.to_string()).execute(&mut *transaction).await.map_err(|e| db("record superseded cleanup", e))?;
            sqlx::query("UPDATE superseded_cleanup_intents SET state = 'removed', completion_only = 0 WHERE artifact_id = ?")
                    .bind(artifact.id.to_string()).execute(&mut *transaction).await.map_err(|e| db("complete superseded cleanup intent", e))?;
            let event = Event::new(
                EventId::new(),
                EventKind::CleanupStateChanged,
                EventPayload::new(
                    EventKind::CleanupStateChanged,
                    1,
                    json!({"artifact_id":artifact.id,"attempt_id":attempt_id,"from":"superseded","state":"removed","reason":"superseded_artifact_retention"}),
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
        transaction
            .commit()
            .await
            .map_err(|e| db("commit superseded cleanup removal", e))?;
        Ok(completed.len())
    }
}

pub(super) async fn preview(
    tx: &mut Transaction<'_, Sqlite>,
    attempt_id: AttemptId,
    paths: &RuntimePaths,
    output: &mut Vec<super::GcEntry>,
    batches: &mut Vec<super::gc::GcArtifactBatch>,
    selected: Option<&[super::GcEntry]>,
) -> PersistenceResult<()> {
    use super::{GcEffect as E, GcEntry, GcOperation as O, GcStatus as S, GcTarget};
    let row = read_attempt(tx, attempt_id)
        .await?
        .ok_or(PersistenceError::NotFound {
            entity: "cleanup attempt",
        })?;
    let attempt: AttemptSpec = from_json(row.get("spec_json"), "superseded GC spec")?;
    let project_id = parse_id(row.get("project_id"), "GC project")?;
    let (artifacts, batch) =
        super::gc::artifact_batch(tx, attempt_id, O::SupersededArtifacts, selected).await?;
    batches.push(batch);
    let mut entries: Vec<_> = artifacts
        .iter()
        .map(|a| {
            GcEntry::new(
                O::SupersededArtifacts,
                a.id.to_string(),
                project_id,
                Some(attempt_id),
            )
        })
        .collect();
    let result: PersistenceResult<()> = async {
        let barrier = eligibility_barrier(&row);
        if let Some(reason) = barrier { for e in &mut entries { e.decide(S::Deferred, reason, E::Retain); } return Ok(()); }
        let Some(policy) = &attempt.result().superseded_artifact_retention else { for e in &mut entries { e.decide(S::Retain, "legacy_policy_absent", E::Retain); } return Ok(()); };
        policy.validate()?;
        let mut builder = globset::GlobSetBuilder::new();
        for p in &policy.keep_on_success { builder.add(globset::Glob::new(p).map_err(|_| conflict("superseded cleanup pattern"))?); }
        let patterns = builder.build().map_err(|_| conflict("superseded cleanup patterns"))?;
        let removed: Vec<_> = artifacts.iter().filter(|a| policy.policy != SupersededRetention::Keep && !patterns.is_match(&a.path)).cloned().collect();
        for (a,e) in artifacts.iter().zip(&mut entries) { if !removed.iter().any(|r| r.id == a.id) { e.decide(S::Retain, "keep_policy", E::Retain); } }
        if !removed.is_empty() { check_log_paths(tx, attempt_id, paths).await?; }
        let project_root = PathBuf::from(row.get::<String,_>("root_path"));
        let mut batch = vec![];
        let mut replacement_ok = true;
        for a in removed {
            let mut item = identity(a, &attempt, paths, &project_root)?;
            let prior: Option<String> = sqlx::query_scalar("SELECT identity_json FROM superseded_cleanup_intents WHERE artifact_id = ? AND state = 'prepared'").bind(item.artifact.id.to_string()).fetch_optional(&mut **tx).await.map_err(|e| db("read superseded GC intent", e))?;
            let replacement = replacement_generation_succeeded(tx, item.artifact.id).await?;
            replacement_ok &= replacement;
            if let Some(prior) = prior {
                if from_json::<Value>(&prior, "superseded GC intent")? != item.evidence { return Err(conflict("superseded cleanup immutable identity")); }
                item.prepared = true;
            } else if !replacement {
                // The actual first transaction commits Keeps here, before filesystem preflight.
                for e in &mut entries { if e.status != S::Retain { e.decide(S::Deferred, "current_replacement_pending", E::Retain); } }
                return Ok(());
            }
            batch.push(item);
        }
        let checked = batch.clone();
        let files = tokio::task::spawn_blocking(move || preflight(&checked).map(|r| r.0)).await??;
        for item in batch {
            let e = entries.iter_mut().find(|e| e.subject_id == item.artifact.id.to_string()).ok_or_else(|| conflict("GC artifact entry"))?;
            let targets: Vec<_> = item.roots.iter().map(|(root, relative)| root.join(relative)).filter(|p| files.contains(p)).map(|path| GcTarget { path, bytes: item.artifact.size_bytes, effect: E::Remove }).collect();
            if !replacement_ok && !targets.is_empty() { e.decide(S::Deferred, "current_replacement_pending", E::Retain); }
            else { e.decide(if targets.is_empty() { S::MetadataOnly } else { S::Ready }, "verified_superseded_retention", if targets.is_empty() { E::ResumeMetadata } else { E::Remove }); e.targets = targets; }
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
    tx: &mut Transaction<'_, Sqlite>,
    attempt_id: AttemptId,
) -> PersistenceResult<Option<sqlx::sqlite::SqliteRow>> {
    sqlx::query("SELECT a.spec_json, a.state, a.project_id, a.job_id, j.state AS job_state, p.root_path, p.registered,
        EXISTS (SELECT 1 FROM actions WHERE attempt_id = a.id AND kind = 'extract_metrics' AND state IN ('pending', 'running')) AS extracting
        FROM attempts a JOIN jobs j ON j.id = a.job_id JOIN projects p ON p.id = a.project_id WHERE a.id = ?")
        .bind(attempt_id.to_string()).fetch_optional(&mut **tx).await.map_err(|e| db("read superseded cleanup attempt", e))
}

fn eligibility_barrier(row: &sqlx::sqlite::SqliteRow) -> Option<&'static str> {
    if row.get::<&str, _>("state") != "succeeded" || row.get::<&str, _>("job_state") != "superseded"
    {
        Some("attempt_not_superseded_success")
    } else if row.get::<i64, _>("registered") != 1 {
        Some("project_unregistered")
    } else if row.get::<i64, _>("extracting") != 0 {
        Some("active_extraction")
    } else {
        None
    }
}
