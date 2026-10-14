use super::*;

impl Database {
    pub async fn cleanup_tombstone(
        &self,
        operation: &str,
        subject: &str,
    ) -> PersistenceResult<Option<Value>> {
        let value: Option<String> = sqlx::query_scalar("SELECT json_object('operation', operation, 'subject_id', subject_id, 'schema_version', schema_version, 'reason', reason, 'outcome', outcome, 'identity', json(identity_json), 'decided_at', decided_at, 'completed_at', completed_at, 'action_id', action_id) FROM cleanup_tombstones WHERE operation = ? AND subject_id = ?")
            .bind(operation).bind(subject).fetch_optional(&self.pool).await
            .map_err(|e| db("read cleanup tombstone", e))?;
        value
            .map(|v| from_json(&v, "cleanup tombstone"))
            .transpose()
    }
}

// Called only in the transaction publishing the terminal state and its event.
// A replay may read the evidence, but must never rewrite its timestamps.
pub(super) async fn record(
    tx: &mut Transaction<'_, Sqlite>,
    operation: &str,
    subject: &str,
    outcome: &str,
    identity: &Value,
    decided_at: Option<&str>,
    action_id: Option<ActionId>,
) -> PersistenceResult<bool> {
    let inserted = sqlx::query("INSERT INTO cleanup_tombstones (operation, subject_id, schema_version, reason, outcome, identity_json, decided_at, action_id) SELECT ?, ?, 1, ?, ?, ?, COALESCE(?, strftime('%Y-%m-%dT%H:%M:%fZ', 'now')), ? WHERE NOT EXISTS (SELECT 1 FROM cleanup_tombstones WHERE operation = ? AND subject_id = ?)")
        .bind(operation).bind(subject).bind(operation).bind(outcome).bind(identity.to_string())
        .bind(decided_at).bind(action_id.map(|id| id.to_string())).bind(operation).bind(subject).execute(&mut **tx).await
        .map_err(|e| db("record cleanup tombstone", e))?;
    if inserted.rows_affected() == 0 {
        let row = sqlx::query("SELECT outcome, identity_json FROM cleanup_tombstones WHERE operation = ? AND subject_id = ?")
            .bind(operation).bind(subject).fetch_one(&mut **tx).await.map_err(|e| db("validate cleanup replay", e))?;
        if row.get::<String, _>("outcome") != outcome
            || from_json::<Value>(row.get("identity_json"), "cleanup identity")? != *identity
        {
            return Err(PersistenceError::Conflict {
                entity: "cleanup tombstone identity",
            });
        }
    }
    Ok(inserted.rows_affected() == 1)
}
