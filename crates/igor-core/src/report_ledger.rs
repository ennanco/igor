use super::report_execution::PreparedReport;
use super::*;
use crate::{ReportContext, ReportId, ReportState};

#[derive(Clone, Debug, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct StoredReportRun {
    pub id: ReportId,
    pub action_id: Option<ActionId>,
    pub generation_id: GenerationId,
    pub project_id: Option<ProjectId>,
    pub family_id: Option<FamilyId>,
    pub state: ReportState,
    pub phase: Option<String>,
    pub context: Option<ReportContext>,
    pub context_hash: String,
    pub spec: Option<ReportActionSpec>,
    pub output_path: Option<PathBuf>,
    pub output_hash: Option<String>,
    pub output_size_bytes: Option<u64>,
    pub previous_output_hash: Option<String>,
    pub provider: String,
    pub model: String,
    pub renderer_version: Option<u32>,
    pub prompt_hash: String,
    pub initial_lease_id: Option<Uuid>,
    pub lease_id: Option<Uuid>,
    pub lease_owner: Option<String>,
    pub error: Option<String>,
    pub created_at: String,
    pub prepared_at: Option<String>,
    pub published_at: Option<String>,
}

impl ActionRepository<'_> {
    pub async fn report_runs(
        &self,
        action_id: ActionId,
    ) -> PersistenceResult<Vec<StoredReportRun>> {
        let rows = sqlx::query("SELECT * FROM report_runs WHERE action_id = ? ORDER BY id")
            .bind(action_id.to_string())
            .fetch_all(&self.database.pool)
            .await
            .map_err(|e| db("read report run history", e))?;
        rows.into_iter().map(decode_run).collect()
    }

    pub async fn report_run(&self, id: ReportId) -> PersistenceResult<Option<StoredReportRun>> {
        sqlx::query("SELECT * FROM report_runs WHERE id = ?")
            .bind(id.to_string())
            .fetch_optional(&self.database.pool)
            .await
            .map_err(|e| db("read report run", e))?
            .map(decode_run)
            .transpose()
    }
}

fn decode_run(row: sqlx::sqlite::SqliteRow) -> PersistenceResult<StoredReportRun> {
    let id = |column: &str| -> PersistenceResult<Option<String>> { Ok(row.get(column)) };
    let context_json: Option<String> = row.get("context_json");
    let context_hash: String = row.get("context_hash");
    if context_json.as_ref().is_some_and(|value| {
        format!("sha256:{:x}", Sha256::digest(value.as_bytes())) != context_hash
    }) {
        return Err(PersistenceError::InvalidValue {
            entity: "report context checksum",
            value: "stored bytes do not match hash".into(),
        });
    }
    let state: String = row.get("state");
    let state = ReportState::ALL
        .iter()
        .copied()
        .find(|value| value.as_str() == state)
        .ok_or(PersistenceError::InvalidValue {
            entity: "report run state",
            value: state,
        })?;
    let size: Option<i64> = row.get("output_size_bytes");
    let renderer: Option<i64> = row.get("renderer_version");
    Ok(StoredReportRun {
        id: parse_id(row.get("id"), "report run")?,
        action_id: id("action_id")?
            .map(|id| parse_id(&id, "report action"))
            .transpose()?,
        generation_id: parse_id(row.get("generation_id"), "report generation")?,
        project_id: id("project_id")?
            .map(|id| parse_id(&id, "report project"))
            .transpose()?,
        family_id: id("family_id")?
            .map(|id| parse_id(&id, "report family"))
            .transpose()?,
        state,
        phase: row.get("publication_phase"),
        context: context_json
            .map(|value| from_json(&value, "report context"))
            .transpose()?,
        context_hash,
        spec: row
            .get::<Option<String>, _>("spec_json")
            .map(|value| from_json(&value, "report run spec"))
            .transpose()?,
        output_path: row
            .get::<Option<String>, _>("output_path")
            .map(PathBuf::from),
        output_hash: row.get("output_hash"),
        output_size_bytes: size
            .map(|value| {
                u64::try_from(value).map_err(|_| PersistenceError::InvalidValue {
                    entity: "report output size",
                    value: value.to_string(),
                })
            })
            .transpose()?,
        previous_output_hash: row.get("previous_output_hash"),
        provider: row.get("provider"),
        model: row.get("model"),
        renderer_version: renderer
            .map(|version| {
                u32::try_from(version).map_err(|_| PersistenceError::InvalidValue {
                    entity: "report renderer version",
                    value: version.to_string(),
                })
            })
            .transpose()?,
        prompt_hash: row.get("prompt_hash"),
        initial_lease_id: id("initial_lease_id")?
            .map(|id| parse_id(&id, "report initial lease"))
            .transpose()?,
        lease_id: id("lease_id")?
            .map(|id| parse_id(&id, "report lease"))
            .transpose()?,
        lease_owner: row.get("lease_owner"),
        error: row.get("error"),
        created_at: row.get("created_at"),
        prepared_at: row.get("prepared_at"),
        published_at: row.get("published_at"),
    })
}

pub(super) async fn prepare_run(
    tx: &mut Transaction<'_, Sqlite>,
    claim: &Claim<ActionId>,
    prepared: &PreparedReport,
) -> PersistenceResult<ReportId> {
    let spec = json(&prepared.spec, "report run spec")?;
    let rows = sqlx::query("SELECT id, context_hash, context_json, spec_json, output_hash, lease_id FROM report_runs WHERE action_id = ? AND state = 'generating' ORDER BY output_hash IS NOT NULL DESC, id")
        .bind(claim.record_id.to_string()).fetch_all(&mut **tx).await.map_err(|e| db("find resumable report run", e))?;
    let mut selected = None;
    for row in &rows {
        if row.get::<String, _>("context_hash") == prepared.context_hash
            && row.get::<String, _>("spec_json") == spec
        {
            if row.get::<String, _>("context_json") != json(&prepared.context, "report context")? {
                return Err(PersistenceError::InvalidValue {
                    entity: "report context checksum",
                    value: "resumable context bytes changed".into(),
                });
            }
            selected = Some(parse_id::<ReportId>(row.get("id"), "resumable report")?);
            break;
        }
    }
    for row in rows {
        let id: ReportId = parse_id(row.get("id"), "report run")?;
        if Some(id) != selected && row.get::<Option<String>, _>("output_hash").is_none() {
            close_run(
                tx,
                id,
                prepared.spec.project.id,
                "cancelled",
                "report_context_replaced",
            )
            .await?;
        }
    }
    if let Some(id) = selected {
        let row = sqlx::query("SELECT lease_id FROM report_runs WHERE id = ?")
            .bind(id.to_string())
            .fetch_one(&mut **tx)
            .await
            .map_err(|e| db("read report run owner", e))?;
        let previous: Option<String> = row.get("lease_id");
        if previous.as_deref() != Some(claim.lease_id.to_string().as_str()) {
            sqlx::query("UPDATE report_runs SET lease_id = ?, lease_owner = ?, lease_expires_at = (SELECT claim_expires_at FROM actions WHERE id = ?), error = NULL, updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now') WHERE id = ? AND state = 'generating'")
                .bind(claim.lease_id.to_string()).bind(&claim.owner).bind(claim.record_id.to_string()).bind(id.to_string())
                .execute(&mut **tx).await.map_err(|e| db("resume report run claim", e))?;
            run_event(
                tx,
                prepared.spec.project.id,
                id,
                "generating",
                Some(claim.lease_id),
                serde_json::json!({"reason":"report_run_resumed","previous_lease_id":previous}),
            )
            .await?;
        }
        return Ok(id);
    }
    let id = ReportId::new();
    let key = format!("report_run:v1:{}:{id}", claim.record_id);
    let inserted = sqlx::query("INSERT INTO report_runs (id, generation_id, state, provider, model, prompt_hash, context_hash, idempotency_key, lease_id, lease_owner, lease_expires_at, action_id, project_id, family_id, context_json, spec_json, output_path, renderer_version, initial_lease_id, publication_phase) VALUES (?, ?, 'generating', 'igor', 'deterministic-format-v1', ?, ?, ?, ?, ?, (SELECT claim_expires_at FROM actions WHERE id = ?), ?, ?, ?, ?, ?, ?, 1, ?, 'context')")
        .bind(id.to_string()).bind(prepared.spec.generation.id.to_string()).bind(format!("sha256:{:x}", Sha256::digest([])))
        .bind(&prepared.context_hash).bind(key).bind(claim.lease_id.to_string()).bind(&claim.owner).bind(claim.record_id.to_string())
        .bind(claim.record_id.to_string()).bind(prepared.spec.project.id.to_string()).bind(prepared.spec.family_id.to_string())
        .bind(json(&prepared.context, "report context")?).bind(spec).bind(prepared.spec.report.output.to_string_lossy().as_ref())
        .bind(claim.lease_id.to_string()).execute(&mut **tx).await.map_err(|e| db("persist report context", e))?;
    if inserted.rows_affected() != 1 {
        return Err(PersistenceError::Conflict {
            entity: "durable report context",
        });
    }
    run_event(tx, prepared.spec.project.id, id, "generating", Some(claim.lease_id), serde_json::json!({"reason":"report_context_recorded","action_id":claim.record_id,"context_hash":prepared.context_hash})).await?;
    Ok(id)
}

/// Commits expected bytes before rename. False means an unresolved old intent saw different output.
pub(super) async fn prepare_output(
    tx: &mut Transaction<'_, Sqlite>,
    claim: &Claim<ActionId>,
    prepared: &PreparedReport,
    output: &crate::PublishedReport,
    previous: Option<&str>,
) -> PersistenceResult<bool> {
    let id = prepared.run_id.ok_or(PersistenceError::Conflict {
        entity: "durable report preparation",
    })?;
    let own = sqlx::query("SELECT state, lease_id, context_hash, spec_json, output_path FROM report_runs WHERE id = ? AND action_id = ?")
        .bind(id.to_string()).bind(claim.record_id.to_string()).fetch_one(&mut **tx).await.map_err(|e| db("revalidate report run", e))?;
    if own.get::<String, _>("state") != "generating"
        || own.get::<Option<String>, _>("lease_id").as_deref()
            != Some(claim.lease_id.to_string().as_str())
        || own.get::<String, _>("context_hash") != prepared.context_hash
        || own.get::<String, _>("spec_json") != json(&prepared.spec, "report run spec")?
        || Path::new(own.get::<&str, _>("output_path")) != output.path
    {
        return Err(PersistenceError::Conflict {
            entity: "report run identity",
        });
    }
    let pending = sqlx::query("SELECT id, output_hash, output_size_bytes, previous_output_hash FROM report_runs WHERE action_id = ? AND state = 'generating' AND output_hash IS NOT NULL")
        .bind(claim.record_id.to_string()).fetch_all(&mut **tx).await.map_err(|e| db("read unresolved report intent", e))?;
    let mut recorded = false;
    for row in pending {
        let old: ReportId = parse_id(row.get("id"), "unresolved report run")?;
        let expected: String = row.get("output_hash");
        let baseline: Option<String> = row.get("previous_output_hash");
        if previous != Some(expected.as_str()) && previous != baseline.as_deref() {
            return Ok(false);
        }
        if old == id {
            if expected != output.sha256
                || row.get::<Option<i64>, _>("output_size_bytes") != Some(output.size_bytes as i64)
            {
                return Ok(false);
            }
            recorded = true;
        } else {
            close_run(
                tx,
                old,
                prepared.spec.project.id,
                "cancelled",
                "report_intent_reconciled",
            )
            .await?;
        }
    }
    if !recorded {
        let updated = sqlx::query("UPDATE report_runs SET output_hash = ?, output_size_bytes = ?, previous_output_hash = ?, publication_phase = 'prepared', prepared_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now'), updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now') WHERE id = ? AND state = 'generating' AND output_hash IS NULL")
            .bind(&output.sha256).bind(output.size_bytes as i64).bind(previous).bind(id.to_string()).execute(&mut **tx).await
            .map_err(|e| db("persist report output intent", e))?;
        if updated.rows_affected() != 1 {
            return Err(PersistenceError::Conflict {
                entity: "durable report output intent",
            });
        }
        run_event(
            tx,
            prepared.spec.project.id,
            id,
            "generating",
            Some(claim.lease_id),
            serde_json::json!({"reason":"report_output_prepared","report":output}),
        )
        .await?;
    }
    Ok(true)
}

pub(super) async fn published(
    tx: &mut Transaction<'_, Sqlite>,
    claim: &Claim<ActionId>,
    prepared: &PreparedReport,
) -> PersistenceResult<()> {
    let id = prepared.run_id.ok_or(PersistenceError::Conflict {
        entity: "report run",
    })?;
    let older = sqlx::query("UPDATE report_runs SET state = 'superseded', updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now') WHERE project_id = ? AND output_path = ? AND state = 'published' AND id != ? RETURNING id")
        .bind(prepared.spec.project.id.to_string()).bind(prepared.spec.report.output.to_string_lossy().as_ref()).bind(id.to_string())
        .fetch_all(&mut **tx).await.map_err(|e| db("supersede acknowledged report output", e))?;
    for row in older {
        run_event(
            tx,
            prepared.spec.project.id,
            parse_id(row.get("id"), "previous report")?,
            "superseded",
            None,
            serde_json::json!({"reason":"report_output_replaced","replacement_run_id":id}),
        )
        .await?;
    }
    let result = sqlx::query("UPDATE report_runs SET state = 'published', publication_phase = 'published', published_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now'), lease_expires_at = (SELECT claim_expires_at FROM actions WHERE id = ?), error = NULL, updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now') WHERE id = ? AND action_id = ? AND state = 'generating' AND publication_phase = 'prepared' AND lease_id = ? AND lease_owner = ?")
        .bind(claim.record_id.to_string()).bind(id.to_string()).bind(claim.record_id.to_string()).bind(claim.lease_id.to_string()).bind(&claim.owner)
        .execute(&mut **tx).await.map_err(|e| db("prepare report run acknowledgement", e))?;
    if result.rows_affected() != 1 {
        return Err(PersistenceError::Conflict {
            entity: "report run publication",
        });
    }
    run_event(
        tx,
        prepared.spec.project.id,
        id,
        "published",
        Some(claim.lease_id),
        serde_json::json!({"reason":"report_run_published","action_id":claim.record_id}),
    )
    .await
}

#[derive(Clone, Copy)]
pub(super) enum RunFinish {
    Defer,
    Cancel,
    Retry,
    Failed,
}

pub(super) async fn finish_runs(
    tx: &mut Transaction<'_, Sqlite>,
    action: ActionId,
    lease: Uuid,
    finish: RunFinish,
    reason: &str,
) -> PersistenceResult<()> {
    let rows = sqlx::query("SELECT id, project_id, output_hash FROM report_runs WHERE action_id = ? AND state = 'generating' AND (? OR lease_id = ?)")
        .bind(action.to_string()).bind(matches!(finish, RunFinish::Cancel | RunFinish::Failed)).bind(lease.to_string()).fetch_all(&mut **tx).await.map_err(|e| db("read interrupted report runs", e))?;
    for row in rows {
        let id: ReportId = parse_id(row.get("id"), "interrupted report")?;
        let project = parse_id(row.get("project_id"), "report project")?;
        if matches!(finish, RunFinish::Defer | RunFinish::Retry)
            && row.get::<Option<String>, _>("output_hash").is_some()
        {
            sqlx::query("UPDATE report_runs SET error = ?, updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now') WHERE id = ? AND state = 'generating'")
                .bind(reason).bind(id.to_string()).execute(&mut **tx).await.map_err(|e| db("retain uncertain report intent", e))?;
            run_event(tx, project, id, "generating", Some(lease),
                serde_json::json!({"reason":"report_run_deferred","cause":reason,"action_id":action})).await?;
        } else {
            close_run(
                tx,
                id,
                project,
                if matches!(finish, RunFinish::Retry | RunFinish::Failed) {
                    "failed"
                } else {
                    "cancelled"
                },
                reason,
            )
            .await?;
        }
    }
    Ok(())
}

async fn close_run(
    tx: &mut Transaction<'_, Sqlite>,
    id: ReportId,
    project: ProjectId,
    state: &str,
    reason: &str,
) -> PersistenceResult<()> {
    sqlx::query("UPDATE report_runs SET state = ?, error = ?, updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now') WHERE id = ? AND state = 'generating'")
        .bind(state).bind(reason).bind(id.to_string()).execute(&mut **tx).await.map_err(|e| db("close report execution", e))?;
    run_event(
        tx,
        project,
        id,
        state,
        None,
        serde_json::json!({"reason":reason}),
    )
    .await
}

async fn run_event(
    tx: &mut Transaction<'_, Sqlite>,
    project: ProjectId,
    id: ReportId,
    state: &str,
    lease: Option<Uuid>,
    details: Value,
) -> PersistenceResult<()> {
    let event = state_event(
        EventKind::ReportStateChanged,
        "report",
        &id.to_string(),
        state,
        lease,
        Some(details),
    )?;
    insert_event(tx, project, None, None, &event).await
}
