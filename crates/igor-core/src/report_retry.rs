use super::report_actions::{action_receipt, normalized_path, validate_report_candidate};
use super::*;
use crate::{EventPayload, ReportConfig};

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct ReportRetryReceipt {
    pub request_id: ActionId,
    #[serde(flatten)]
    pub report: ReportRequestReceipt,
}

impl ActionRepository<'_> {
    pub async fn report_retry_request(
        &self,
        request_id: ActionId,
        family_id: FamilyId,
        scope: Option<ProjectId>,
    ) -> PersistenceResult<Option<ReportRetryReceipt>> {
        let mut tx = self
            .database
            .pool
            .begin()
            .await
            .map_err(|source| db("begin report retry receipt", source))?;
        retry_receipt(&mut tx, request_id, family_id, scope).await
    }

    pub async fn retry_family_report(
        &self,
        candidate: &ReportCandidate,
        live_policy: &ReportConfig,
        request_id: ActionId,
    ) -> PersistenceResult<ReportRetryReceipt> {
        let mut tx = self
            .database
            .pool
            .begin_with("BEGIN IMMEDIATE")
            .await
            .map_err(|source| db("reserve report retry writer", source))?;
        if let Some(receipt) = retry_receipt(
            &mut tx,
            request_id,
            candidate.family_id,
            Some(candidate.project.id),
        )
        .await?
        {
            return Ok(receipt);
        }
        if !live_policy.enabled {
            return Err(PersistenceError::ReportDisabled);
        }
        if !validate_report_candidate(&mut tx, candidate).await? {
            return Err(PersistenceError::Conflict {
                entity: "report retry project or generation",
            });
        }
        let row = sqlx::query("SELECT a.id, a.spec_json, a.attempts, a.last_error FROM events e
            JOIN actions a ON a.id = json_extract(e.payload_json, '$.data.record_id') AND a.project_id = e.project_id
            WHERE e.project_id = ? AND e.kind = 'action_state_changed'
              AND json_extract(e.payload_json, '$.data.entity') = 'action'
              AND json_extract(e.payload_json, '$.data.state') = 'pending'
              AND json_extract(e.payload_json, '$.data.details.trigger') IN ('family-completed', 'manual')
              AND a.kind = 'generate_report' AND json_extract(a.spec_json, '$.family_id') = ?
              AND json_extract(a.spec_json, '$.generation.id') = ? ORDER BY e.sequence DESC LIMIT 1")
            .bind(candidate.project.id.to_string()).bind(candidate.family_id.to_string()).bind(candidate.generation.id.to_string())
            .fetch_optional(&mut *tx).await.map_err(|source| db("select latest report retry target", source))?
            .ok_or(PersistenceError::NotFound { entity: "report action for current generation" })?;
        let id: ActionId = parse_id(row.get("id"), "report retry action")?;
        let receipt = action_receipt(
            &mut tx,
            id,
            candidate.family_id,
            Some(candidate.project.id),
            false,
        )
        .await?
        .ok_or(PersistenceError::NotFound {
            entity: "report retry action",
        })?;
        if receipt.state != ActionState::Failed {
            return Err(PersistenceError::Conflict {
                entity: "latest report action is not failed",
            });
        }
        let spec: ReportActionSpec = from_json(row.get("spec_json"), "report retry action")?;
        let mut policy = live_policy.clone();
        policy.output = normalized_path(&candidate.project.root, &policy.output)?;
        policy.prompt_file = normalized_path(&candidate.project.root, &policy.prompt_file)?;
        if spec.project != candidate.project
            || spec.generation != candidate.generation
            || spec.report != policy
        {
            return Err(PersistenceError::Conflict {
                entity: "frozen report retry configuration",
            });
        }
        let previous_attempts: i64 = row.get("attempts");
        let previous_error: Option<String> = row.get("last_error");
        let updated = sqlx::query("UPDATE actions SET state = 'pending', attempts = 0, last_error = NULL,
            claim_id = NULL, claim_owner = NULL, claim_expires_at = NULL,
            available_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now'), updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
            WHERE id = ? AND kind = 'generate_report' AND state = 'failed'")
            .bind(id.to_string()).execute(&mut *tx).await.map_err(|source| db("requeue failed report", source))?;
        if updated.rows_affected() != 1 {
            return Err(PersistenceError::Conflict {
                entity: "failed report action",
            });
        }
        let mut event = state_event(
            EventKind::ActionStateChanged,
            "action",
            &id.to_string(),
            "pending",
            None,
            Some(
                serde_json::json!({"reason":"report_retry_requested","request_id":request_id,
                "family_id":candidate.family_id,"generation_id":candidate.generation.id,
                "previous_attempts":previous_attempts,"previous_error":previous_error}),
            ),
        )?;
        event.id = parse_id(&request_id.to_string(), "report retry request")?;
        insert_event(&mut tx, candidate.project.id, None, None, &event).await?;
        tx.commit()
            .await
            .map_err(|source| db("commit explicit report retry", source))?;
        Ok(ReportRetryReceipt {
            request_id,
            report: ReportRequestReceipt {
                state: ActionState::Pending,
                ..receipt
            },
        })
    }
}

async fn retry_receipt(
    tx: &mut Transaction<'_, Sqlite>,
    request_id: ActionId,
    family_id: FamilyId,
    scope: Option<ProjectId>,
) -> PersistenceResult<Option<ReportRetryReceipt>> {
    let row = sqlx::query(
        "SELECT project_id, job_id, attempt_id, kind, payload_json FROM events WHERE id = ?",
    )
    .bind(request_id.to_string())
    .fetch_optional(&mut **tx)
    .await
    .map_err(|source| db("read report retry request", source))?;
    let Some(row) = row else {
        return Ok(None);
    };
    if row.get::<String, _>("kind") != "action_state_changed"
        || row.get::<Option<String>, _>("job_id").is_some()
        || row.get::<Option<String>, _>("attempt_id").is_some()
    {
        return Err(PersistenceError::Conflict {
            entity: "report retry request identity",
        });
    }
    let payload: EventPayload = from_json(row.get("payload_json"), "report retry event")?;
    let data = &payload.data;
    if data["entity"] != "action"
        || data["state"] != "pending"
        || data["details"]["reason"] != "report_retry_requested"
        || data["details"]["request_id"].as_str() != Some(request_id.to_string().as_str())
    {
        return Err(PersistenceError::Conflict {
            entity: "report retry request identity",
        });
    }
    if data["details"]["family_id"].as_str() != Some(family_id.to_string().as_str()) {
        return Err(PersistenceError::NotFound {
            entity: "report retry request",
        });
    }
    let id: ActionId = parse_id(
        data["record_id"]
            .as_str()
            .ok_or(PersistenceError::Conflict {
                entity: "report retry action identity",
            })?,
        "report retry action",
    )?;
    let report = action_receipt(tx, id, family_id, scope, false)
        .await?
        .ok_or(PersistenceError::NotFound {
            entity: "report retry action",
        })?;
    if row.get::<String, _>("project_id") != report.project_id.to_string()
        || data["details"]["generation_id"].as_str()
            != Some(report.generation.id.to_string().as_str())
    {
        return Err(PersistenceError::Conflict {
            entity: "report retry event scope",
        });
    }
    Ok(Some(ReportRetryReceipt { request_id, report }))
}
