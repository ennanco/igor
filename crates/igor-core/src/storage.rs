use super::*;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct StorageStatus {
    pub project_id: Option<ProjectId>,
    pub project_name: Option<String>,
    pub artifacts: ArtifactStorage,
    pub successful_log_archives: Vec<LogArchiveStorage>,
    pub pending_log_preparations: PendingLogPreparationsStorage,
    pub failed_tails: FailedTailStorage,
    pub failed_log_retirements: Vec<FailedLogRetirementStorage>,
    pub owned_failed_outputs: Vec<OwnedOutputStorage>,
    pub repair_worktrees: Vec<RepairWorktreeStorage>,
    pub failed_cleanup_actions: Vec<StorageStateCount>,
    pub cleanup_intents: Vec<CleanupIntentStorage>,
    pub tombstones: Vec<TombstoneStorage>,
    pub actual_disk_footprint: Option<u64>,
}

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct ArtifactStorage {
    pub active: Vec<ArtifactStorageGroup>,
    pub removed_history: Vec<ArtifactStorageGroup>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ArtifactStorageGroup {
    pub physical_state: String,
    pub lifecycle_state: String,
    pub count: u64,
    pub known_size_count: u64,
    pub unknown_size_count: u64,
    pub logical_bytes: u64,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct LogArchiveStorage {
    pub state: String,
    pub retired: bool,
    pub count: u64,
    pub original_identity_bytes: u64,
    pub archive_identity_bytes: u64,
    pub raw_outstanding_bytes: u64,
}

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct PendingLogPreparationsStorage {
    pub count: u64,
    pub temporary_bytes: Option<u64>,
}

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct FailedTailStorage {
    pub complete_count: u64,
    pub incomplete_count: u64,
    pub stdout_blob_bytes: u64,
    pub stderr_blob_bytes: u64,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct FailedLogRetirementStorage {
    pub state: String,
    pub count: u64,
    pub original_identity_bytes: u64,
    pub tail_identity_bytes: u64,
    pub actual_raw_bytes: Option<u64>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct OwnedOutputStorage {
    pub state: String,
    pub count: u64,
    pub manifest_identity_bytes: u64,
    pub actual_file_bytes: Option<u64>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct RepairWorktreeStorage {
    pub state: String,
    pub count: u64,
    pub patch_blob_bytes: u64,
    pub disk_bytes: Option<u64>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct StorageStateCount {
    pub state: String,
    pub count: u64,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct CleanupIntentStorage {
    pub kind: String,
    pub state: String,
    pub count: u64,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct TombstoneStorage {
    pub operation: String,
    pub outcome: String,
    pub count: u64,
}

async fn groups<T: serde::de::DeserializeOwned>(
    tx: &mut Transaction<'_, Sqlite>,
    sql: &str,
    scope: &Option<String>,
) -> PersistenceResult<Vec<T>> {
    let rows: Vec<String> = sqlx::query_scalar(sql)
        .bind(scope)
        .fetch_all(&mut **tx)
        .await
        .map_err(|e| db("read storage inventory", e))?;
    rows.iter()
        .map(|row| from_json(row, "storage inventory"))
        .collect()
}

impl Database {
    pub async fn storage_status(
        &self,
        project_id: Option<ProjectId>,
    ) -> PersistenceResult<StorageStatus> {
        // SQLite BEGIN is deferred: every section shares the first SELECT's snapshot,
        // without reserving a writer or changing any tracked state.
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| db("begin storage inventory", e))?;
        let scope = project_id.map(|id| id.to_string());
        let project_name = if let Some(id) = &scope {
            Some(
                sqlx::query_scalar::<_, String>("SELECT name FROM projects WHERE id = ?")
                    .bind(id)
                    .fetch_optional(&mut *tx)
                    .await
                    .map_err(|e| db("read storage project", e))?
                    .ok_or(PersistenceError::NotFound { entity: "project" })?,
            )
        } else {
            None
        };
        let artifact_groups: Vec<ArtifactStorageGroup> = groups(
            &mut tx,
            "SELECT json_object(
            'physical_state', r.state, 'lifecycle_state', r.lifecycle_state,
            'count', COUNT(*), 'known_size_count', COUNT(r.size_bytes),
            'unknown_size_count', COUNT(*) - COUNT(r.size_bytes),
            'logical_bytes', COALESCE(SUM(r.size_bytes), 0))
            FROM artifacts r JOIN attempts a ON a.id = r.attempt_id
            WHERE (?1 IS NULL OR a.project_id = ?1)
            GROUP BY r.state, r.lifecycle_state ORDER BY r.state, r.lifecycle_state",
            &scope,
        )
        .await?;
        let (removed_history, active) = artifact_groups
            .into_iter()
            .partition(|r| r.physical_state == "removed");
        let successful_log_archives = groups(&mut tx, "SELECT json_object(
            'state', l.state, 'retired', json(CASE l.retired WHEN 1 THEN 'true' ELSE 'false' END),
            'count', COUNT(*),
            'original_identity_bytes', SUM(json_extract(l.identity_json, '$.original_size')),
            'archive_identity_bytes', SUM(json_extract(l.identity_json, '$.archive_size')),
            'raw_outstanding_bytes', SUM(CASE WHEN l.retired = 0 THEN json_extract(l.identity_json, '$.original_size') ELSE 0 END))
            FROM success_log_archives l JOIN attempts a ON a.id = l.attempt_id
            WHERE (?1 IS NULL OR a.project_id = ?1)
            GROUP BY l.state, l.retired ORDER BY l.state, l.retired", &scope).await?;
        let mut preparations = groups(
            &mut tx,
            "SELECT json_object('count', COUNT(*), 'temporary_bytes', NULL)
            FROM success_log_archive_preparations p JOIN attempts a ON a.id = p.attempt_id
            WHERE (?1 IS NULL OR a.project_id = ?1)
            AND NOT EXISTS (SELECT 1 FROM success_log_archives l
                WHERE l.attempt_id = p.attempt_id AND l.stream = p.stream)",
            &scope,
        )
        .await?;
        let pending_log_preparations = preparations.pop().unwrap_or_default();
        let mut tails = groups(
            &mut tx,
            "SELECT json_object(
            'complete_count', COALESCE(SUM(l.complete), 0),
            'incomplete_count', COUNT(*) - COALESCE(SUM(l.complete), 0),
            'stdout_blob_bytes', COALESCE(SUM(length(l.stdout)), 0),
            'stderr_blob_bytes', COALESCE(SUM(length(l.stderr)), 0))
            FROM failed_log_tails l JOIN attempts a ON a.id = l.attempt_id
            WHERE (?1 IS NULL OR a.project_id = ?1)",
            &scope,
        )
        .await?;
        let failed_tails = tails.pop().unwrap_or_default();
        let owned_failed_outputs = groups(&mut tx,"SELECT json_object('state', r.state, 'count', COUNT(*), 'manifest_identity_bytes', SUM(json_extract(r.identity_json, '$.size')), 'actual_file_bytes', NULL) FROM owned_output_manifest r JOIN attempts a ON a.id = r.attempt_id WHERE (?1 IS NULL OR a.project_id = ?1) GROUP BY r.state ORDER BY r.state",&scope).await?;
        let failed_log_retirements = groups(&mut tx, "SELECT json_object('state', r.state, 'count', COUNT(*), 'original_identity_bytes', SUM(json_extract(identity_json, '$.original_size')), 'tail_identity_bytes', SUM(json_extract(identity_json, '$.end') - json_extract(identity_json, '$.start')), 'actual_raw_bytes', NULL) FROM failed_log_retirements r JOIN attempts a ON a.id = r.attempt_id WHERE (?1 IS NULL OR a.project_id = ?1) GROUP BY r.state ORDER BY r.state", &scope).await?;
        let repair_worktrees = groups(&mut tx, "SELECT json_object(
            'state', state, 'count', COUNT(*), 'patch_blob_bytes', COALESCE(SUM(length(patch)), 0), 'disk_bytes', NULL)
            FROM repair_worktrees WHERE (?1 IS NULL OR project_id = ?1)
            GROUP BY state ORDER BY state", &scope).await?;
        let failed_cleanup_actions = groups(
            &mut tx,
            "SELECT json_object('state', state, 'count', COUNT(*))
            FROM actions WHERE kind = 'cleanup_failed_artifacts' AND (?1 IS NULL OR project_id = ?1)
            GROUP BY state ORDER BY state",
            &scope,
        )
        .await?;
        let mut cleanup_intents = groups(&mut tx, "SELECT json_object('kind', i.kind, 'state', i.state, 'count', COUNT(*))
            FROM (SELECT 'failed' AS kind, artifact_id, state FROM failed_cleanup_intents
                  UNION ALL SELECT 'superseded', artifact_id, state FROM superseded_cleanup_intents) i
            JOIN artifacts r ON r.id = i.artifact_id JOIN attempts a ON a.id = r.attempt_id
            WHERE (?1 IS NULL OR a.project_id = ?1)
            GROUP BY i.kind, i.state ORDER BY i.kind, i.state", &scope).await?;
        cleanup_intents.extend(groups::<CleanupIntentStorage>(&mut tx,"SELECT json_object('kind', 'owned_outputs', 'state', r.state, 'count', COUNT(*)) FROM owned_output_manifest r JOIN attempts a ON a.id = r.attempt_id WHERE (?1 IS NULL OR a.project_id = ?1) GROUP BY r.state ORDER BY r.state",&scope).await?);
        cleanup_intents.extend(groups::<CleanupIntentStorage>(&mut tx,"SELECT json_object('kind', 'owned_discovery', 'state', CASE WHEN d.complete = 1 THEN 'completed' ELSE 'pending' END, 'count', COUNT(*)) FROM owned_output_namespaces n JOIN attempts a ON a.id = n.attempt_id LEFT JOIN owned_output_discovery d ON d.attempt_id = n.attempt_id WHERE (?1 IS NULL OR a.project_id = ?1) GROUP BY d.complete ORDER BY d.complete",&scope).await?);
        let tombstones = groups(&mut tx, "SELECT json_object('operation', t.operation, 'outcome', t.outcome, 'count', COUNT(*))
            FROM cleanup_tombstones t WHERE ?1 IS NULL OR
            (t.operation IN ('failed_artifact_retention', 'superseded_artifact_retention') AND EXISTS (
                SELECT 1 FROM artifacts r JOIN attempts a ON a.id = r.attempt_id
                WHERE r.id = t.subject_id AND a.project_id = ?1)) OR
            (t.operation = 'repair_worktree_retention' AND EXISTS (
                SELECT 1 FROM recoveries r JOIN attempts a ON a.id = r.attempt_id
                WHERE r.id = t.subject_id AND a.project_id = ?1)) OR
             (t.operation IN ('success_log_compression', 'failed_log_retirement') AND EXISTS (
                SELECT 1 FROM attempts a WHERE a.project_id = ?1
                AND t.subject_id IN (a.id || ':stdout', a.id || ':stderr'))) OR
             (t.operation = 'failed_owned_output_retention' AND EXISTS (
                SELECT 1 FROM owned_output_manifest r JOIN attempts a ON a.id = r.attempt_id
                WHERE r.id = t.subject_id AND a.project_id = ?1))
            GROUP BY t.operation, t.outcome ORDER BY t.operation, t.outcome", &scope).await?;
        tx.commit()
            .await
            .map_err(|e| db("finish storage inventory", e))?;
        Ok(StorageStatus {
            project_id,
            project_name,
            artifacts: ArtifactStorage {
                active,
                removed_history,
            },
            successful_log_archives,
            pending_log_preparations,
            failed_tails,
            owned_failed_outputs,
            failed_log_retirements,
            repair_worktrees,
            failed_cleanup_actions,
            cleanup_intents,
            tombstones,
            actual_disk_footprint: None,
        })
    }
}
