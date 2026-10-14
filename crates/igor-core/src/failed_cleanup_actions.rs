use super::*;

impl ActionRepository<'_> {
    pub async fn enqueue_failed_cleanup(
        &self,
        attempt_id: AttemptId,
    ) -> PersistenceResult<ActionId> {
        let key = format!("cleanup_failed_artifacts:v1:{attempt_id}");
        let id = ActionId::new();
        let spec = serde_json::json!({"schema_version":1,"attempt_id":attempt_id});
        sqlx::query("INSERT INTO actions (id, project_id, job_id, attempt_id, kind, state, spec_json, idempotency_key, available_at) SELECT ?, project_id, job_id, id, 'cleanup_failed_artifacts', 'pending', ?, ?, strftime('%Y-%m-%dT%H:%M:%fZ', 'now') FROM attempts WHERE id = ? ON CONFLICT(idempotency_key) DO NOTHING")
            .bind(id.to_string()).bind(spec.to_string()).bind(&key).bind(attempt_id.to_string())
            .execute(&self.database.pool).await.map_err(|e| db("enqueue failed cleanup", e))?;
        let id: Option<String> =
            sqlx::query_scalar("SELECT id FROM actions WHERE idempotency_key = ?")
                .bind(key)
                .fetch_optional(&self.database.pool)
                .await
                .map_err(|e| db("read failed cleanup action", e))?;
        parse_id(
            &id.ok_or(PersistenceError::NotFound {
                entity: "cleanup attempt",
            })?,
            "cleanup action",
        )
    }

    pub async fn claim_failed_cleanup(
        &self,
        owner: &str,
        duration: Duration,
    ) -> PersistenceResult<Option<Claim<ActionId>>> {
        claim_outbox(
            &self.database.pool,
            "actions",
            "running",
            EventKind::ActionStateChanged,
            owner,
            duration,
            " AND kind = 'cleanup_failed_artifacts'",
        )
        .await?
        .map(|c| {
            Ok(Claim {
                record_id: parse_id(&c.record_id, "cleanup action")?,
                lease_id: c.lease_id,
                owner: c.owner,
                expires_at: c.expires_at,
            })
        })
        .transpose()
    }

    /// Barrier waits and batch continuation do not consume the operational retry budget.
    pub async fn postpone_failed_cleanup(
        &self,
        claim: &Claim<ActionId>,
        delay: Duration,
    ) -> PersistenceResult<()> {
        let seconds = i64::try_from(delay.as_secs())
            .map_err(|_| PersistenceError::InvalidLease("cleanup delay too large"))?;
        let mut tx = self
            .database
            .pool
            .begin()
            .await
            .map_err(|e| db("begin cleanup postponement", e))?;
        let project: Option<String> = sqlx::query_scalar("UPDATE actions SET state = 'pending', attempts = MAX(0, attempts - 1), claim_id = NULL, claim_owner = NULL, claim_expires_at = NULL, available_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now', '+' || ? || ' seconds'), updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now') WHERE id = ? AND kind = 'cleanup_failed_artifacts' AND state = 'running' AND claim_id = ? AND claim_owner = ? AND claim_expires_at > strftime('%Y-%m-%dT%H:%M:%fZ', 'now') RETURNING project_id")
            .bind(seconds).bind(claim.record_id.to_string()).bind(claim.lease_id.to_string()).bind(&claim.owner).fetch_optional(&mut *tx).await.map_err(|e| db("postpone failed cleanup", e))?;
        let project = project.ok_or(PersistenceError::Conflict {
            entity: "failed cleanup claim",
        })?;
        let event = state_event(
            EventKind::ActionStateChanged,
            "action",
            &claim.record_id.to_string(),
            "pending",
            Some(claim.lease_id),
            Some(serde_json::json!({"reason":"cleanup_postponed"})),
        )?;
        insert_event(
            &mut tx,
            parse_id(&project, "cleanup project")?,
            None,
            None,
            &event,
        )
        .await?;
        tx.commit()
            .await
            .map_err(|e| db("commit cleanup postponement", e))
    }
}
