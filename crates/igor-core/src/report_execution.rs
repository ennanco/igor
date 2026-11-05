use super::*;
use crate::generated_report::LockedGeneratedReport;
use crate::{
    GeneratedReportError, PublishedReport, ReportConfig, ReportContext, ReportPublicationError,
};

const LEASE_SECONDS: i64 = 30;
const DEFER_SECONDS: i64 = 5;

#[cfg(test)]
#[path = "report_execution_tests.rs"]
mod tests;

#[derive(Debug, Error)]
pub enum ReportExecutionError {
    #[error(transparent)]
    Persistence(#[from] PersistenceError),
    #[error(transparent)]
    Generated(#[from] GeneratedReportError),
    #[error(transparent)]
    Publication(#[from] ReportPublicationError),
    #[error("report publication outcome needs reconciliation: {reason}")]
    PublishedUncertain {
        report: PublishedReport,
        reason: String,
    },
}

#[derive(Debug)]
pub enum ReportPreparation {
    Ready(Box<PreparedReport>),
    Deferred,
    Cancelled,
}

#[derive(Debug)]
pub struct PreparedReport {
    pub(super) action_id: ActionId,
    pub(super) lease_id: Uuid,
    pub(super) spec: ReportActionSpec,
    pub(super) context: ReportContext,
    pub(super) context_hash: String,
    pub(super) run_id: Option<crate::ReportId>,
}

#[derive(Debug)]
pub struct LockedReportPublication {
    prepared: PreparedReport,
    file: LockedGeneratedReport,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ReportPublishOutcome {
    Published(PublishedReport),
    Deferred,
    Cancelled,
}

impl PreparedReport {
    #[must_use]
    pub fn run_id(&self) -> Option<crate::ReportId> {
        self.run_id
    }
    #[must_use]
    pub fn project(&self) -> &Project {
        &self.spec.project
    }

    /// Blocking rendering/staging/hash/flock work; run outside the SQLite writer.
    pub fn stage_and_lock(self) -> Result<LockedReportPublication, ReportExecutionError> {
        if self.run_id.is_none() {
            return Err(PersistenceError::Conflict {
                entity: "durable report context",
            }
            .into());
        }
        let file =
            crate::stage_generated_report(&self.spec.project, &self.spec.report, &self.context)?
                .lock_verified()?;
        Ok(LockedReportPublication {
            prepared: self,
            file,
        })
    }
}

enum LiveReport {
    Ready(Box<PreparedReport>),
    Defer(&'static str),
    Cancel(&'static str),
}

impl ActionRepository<'_> {
    pub async fn claim_report(
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
            " AND kind = 'generate_report'",
        )
        .await?
        .map(|claim| {
            Ok(Claim {
                record_id: parse_id(&claim.record_id, "claimed report")?,
                lease_id: claim.lease_id,
                owner: claim.owner,
                expires_at: claim.expires_at,
            })
        })
        .transpose()
    }

    pub async fn prepare_report(
        &self,
        claim: &Claim<ActionId>,
        live_policy: &ReportConfig,
    ) -> Result<ReportPreparation, ReportExecutionError> {
        let mut tx = self
            .database
            .pool
            .begin_with("BEGIN IMMEDIATE")
            .await
            .map_err(|source| db("reserve report preparation writer", source))?;
        match live_report(&mut tx, claim, live_policy).await? {
            LiveReport::Ready(mut prepared) => {
                if prepared.spec.report.generator != crate::ReportGenerator::Deterministic {
                    return Err(GeneratedReportError::UnsupportedGenerator {
                        generator: prepared.spec.report.generator,
                    }
                    .into());
                }
                prepared.run_id =
                    Some(super::report_ledger::prepare_run(&mut tx, claim, &prepared).await?);
                tx.commit()
                    .await
                    .map_err(|source| db("finish report preparation", source))?;
                Ok(ReportPreparation::Ready(prepared))
            }
            LiveReport::Defer(reason) => {
                finish_report_wait(&mut tx, claim, false, reason).await?;
                tx.commit()
                    .await
                    .map_err(|source| db("defer report preparation", source))?;
                Ok(ReportPreparation::Deferred)
            }
            LiveReport::Cancel(reason) => {
                finish_report_wait(&mut tx, claim, true, reason).await?;
                tx.commit()
                    .await
                    .map_err(|source| db("cancel report preparation", source))?;
                Ok(ReportPreparation::Cancelled)
            }
        }
    }

    /// Barriers/unreadable policy do not consume the operational retry budget.
    pub async fn defer_report(
        &self,
        claim: &Claim<ActionId>,
        reason: &'static str,
    ) -> PersistenceResult<()> {
        let mut tx = self
            .database
            .pool
            .begin_with("BEGIN IMMEDIATE")
            .await
            .map_err(|source| db("reserve report deferral writer", source))?;
        finish_report_wait(&mut tx, claim, false, reason).await?;
        tx.commit()
            .await
            .map_err(|source| db("commit report deferral", source))
    }

    pub async fn publish_report(
        &self,
        claim: &Claim<ActionId>,
        publication: LockedReportPublication,
        live_policy: &ReportConfig,
    ) -> Result<ReportPublishOutcome, ReportExecutionError> {
        let database = self.database.clone();
        let claim = claim.clone();
        let live_policy = live_policy.clone();
        let expected = publication.file.report().clone();
        // Cancellation of a client future must not drop a writer while its rename is in flight.
        tokio::spawn(async move {
            database
                .actions()
                .publish_report_inner(&claim, publication, &live_policy)
                .await
        })
        .await
        .map_err(|source| ReportExecutionError::PublishedUncertain {
            report: expected,
            reason: source.to_string(),
        })?
    }

    async fn publish_report_inner(
        &self,
        claim: &Claim<ActionId>,
        publication: LockedReportPublication,
        live_policy: &ReportConfig,
    ) -> Result<ReportPublishOutcome, ReportExecutionError> {
        if publication.prepared.action_id != claim.record_id
            || publication.prepared.lease_id != claim.lease_id
        {
            return Err(PersistenceError::Conflict {
                entity: "prepared report claim",
            }
            .into());
        }
        let expected = publication.file.report().clone();
        let mut intent = self
            .database
            .pool
            .begin_with("BEGIN IMMEDIATE")
            .await
            .map_err(|source| db("reserve report output intent writer", source))?;
        if let Some(outcome) =
            publication_barrier(&mut intent, claim, &publication.prepared, live_policy).await?
        {
            intent
                .commit()
                .await
                .map_err(|source| db("commit report intent barrier", source))?;
            return Ok(outcome);
        }
        if !super::report_ledger::prepare_output(
            &mut intent,
            claim,
            &publication.prepared,
            &expected,
            publication.file.previous_hash(),
        )
        .await?
        {
            finish_report_wait(
                &mut intent,
                claim,
                true,
                "report_recovery_destination_changed",
            )
            .await?;
            intent
                .commit()
                .await
                .map_err(|source| db("cancel changed report recovery", source))?;
            return Ok(ReportPublishOutcome::Cancelled);
        }
        intent
            .commit()
            .await
            .map_err(|source| db("commit durable report output intent", source))?;
        let adopt = publication.file.previous_hash() == Some(expected.sha256.as_str());
        // The caller already holds the directory flock; never wait for it under this writer.
        let mut tx = self
            .database
            .pool
            .begin_with("BEGIN IMMEDIATE")
            .await
            .map_err(|source| db("reserve report publication writer", source))?;
        if let Some(outcome) =
            publication_barrier(&mut tx, claim, &publication.prepared, live_policy).await?
        {
            tx.commit()
                .await
                .map_err(|source| db("commit report publication barrier", source))?;
            return Ok(outcome);
        }
        // Renew under the writer; no competitor can reclaim until commit/rollback.
        let epoch = std::time::Instant::now();
        let ttl: i64 = sqlx::query_scalar("UPDATE actions SET claim_expires_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now', '+' || ? || ' seconds') WHERE id = ? AND kind = 'generate_report' AND state = 'running' AND claim_id = ? AND claim_owner = ? AND claim_expires_at > strftime('%Y-%m-%dT%H:%M:%fZ', 'now') RETURNING ? * 1000")
            .bind(LEASE_SECONDS).bind(claim.record_id.to_string()).bind(claim.lease_id.to_string()).bind(&claim.owner)
            .bind(LEASE_SECONDS).fetch_optional(&mut *tx).await.map_err(|source| db("renew report publication claim", source))?
            .ok_or(PersistenceError::Conflict { entity: "report claim" })?;
        let deadline = epoch
            + Duration::from_millis(
                u64::try_from(ttl)
                    .map_err(|_| PersistenceError::InvalidLease("invalid report lease"))?,
            );
        let expected = publication.file.report().clone();
        super::report_ledger::published(&mut tx, claim, &publication.prepared).await?;
        // Prepare all SQL before rename: a completion/event fault must preserve the old file.
        let changed = sqlx::query("UPDATE actions SET state = 'succeeded', claim_id = NULL, claim_owner = NULL, claim_expires_at = NULL, last_error = NULL, updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now') WHERE id = ? AND kind = 'generate_report' AND state = 'running' AND claim_id = ? AND claim_owner = ?")
            .bind(claim.record_id.to_string()).bind(claim.lease_id.to_string()).bind(&claim.owner)
            .execute(&mut *tx).await.map_err(|source| db("prepare report action completion", source))?;
        if changed.rows_affected() != 1 {
            return Err(PersistenceError::Conflict {
                entity: "report claim",
            }
            .into());
        }
        let event = state_event(
            EventKind::ActionStateChanged,
            "action",
            &claim.record_id.to_string(),
            "succeeded",
            Some(claim.lease_id),
            Some(
                serde_json::json!({"reason":"report_published","report":expected,"report_run_id":publication.prepared.run_id,"reconciled_existing":adopt}),
            ),
        )?;
        insert_event(
            &mut tx,
            publication.prepared.spec.project.id,
            None,
            None,
            &event,
        )
        .await?;
        let mut file = publication.file;
        let publication_task = tokio::task::spawn_blocking(move || {
            let guard = || {
                if std::time::Instant::now() >= deadline {
                    Err(ReportPublicationError::Aborted {
                        reason: "report_lease_expired",
                    })
                } else {
                    Ok(())
                }
            };
            let result = if adopt {
                file.confirm_existing_guarded(guard)
            } else {
                file.publish_guarded(guard)
            };
            (file, result)
        });
        let (_directory_lock, result) =
            publication_task
                .await
                .map_err(|source| ReportExecutionError::PublishedUncertain {
                    report: expected,
                    reason: source.to_string(),
                })?;
        let published = match result {
            Ok(report) => report,
            Err(ReportPublicationError::PublishedButUnsynced { report, source }) => {
                return Err(ReportExecutionError::PublishedUncertain {
                    report,
                    reason: source.to_string(),
                });
            }
            Err(error) => return Err(error.into()),
        };
        if let Err(source) = tx.commit().await {
            return Err(ReportExecutionError::PublishedUncertain {
                report: published,
                reason: source.to_string(),
            });
        }
        Ok(ReportPublishOutcome::Published(published))
    }
}

async fn live_report(
    tx: &mut Transaction<'_, Sqlite>,
    claim: &Claim<ActionId>,
    live: &ReportConfig,
) -> PersistenceResult<LiveReport> {
    let row = sqlx::query("SELECT project_id, job_id, attempt_id, spec_json, idempotency_key FROM actions WHERE id = ? AND kind = 'generate_report' AND state = 'running' AND claim_id = ? AND claim_owner = ? AND claim_expires_at > strftime('%Y-%m-%dT%H:%M:%fZ', 'now')")
        .bind(claim.record_id.to_string()).bind(claim.lease_id.to_string()).bind(&claim.owner)
        .fetch_optional(&mut **tx).await.map_err(|source| db("read live report claim", source))?
        .ok_or(PersistenceError::Conflict { entity: "report claim" })?;
    let spec: ReportActionSpec = from_json(row.get("spec_json"), "report action")?;
    let key = super::report_actions::action_key(&spec);
    let origin_valid = match spec.origin {
        super::ReportOrigin::FamilyCompleted => true,
        super::ReportOrigin::Manual { request_id } => request_id == claim.record_id,
    };
    if spec.schema_version != REPORT_ACTION_VERSION
        || spec.project.id.to_string() != row.get::<String, _>("project_id")
        || row.get::<Option<String>, _>("job_id").is_some()
        || row.get::<Option<String>, _>("attempt_id").is_some()
        || row.get::<String, _>("idempotency_key") != key
        || !spec.report.enabled
        || !origin_valid
    {
        return Err(PersistenceError::InvalidValue {
            entity: "report action",
            value: "invalid frozen report scope/version".into(),
        });
    }
    let row = sqlx::query("SELECT id, name, root_path, config_path FROM projects WHERE id = ? AND registered = 1 AND is_alias = 0")
        .bind(spec.project.id.to_string()).fetch_optional(&mut **tx).await.map_err(|source| db("read live report project", source))?;
    if row.map(decode_project).transpose()?.as_ref() != Some(&spec.project) {
        return Ok(LiveReport::Cancel("report_project_changed"));
    }
    let mut normalized = live.clone();
    if !normalized.enabled {
        return Ok(LiveReport::Cancel("report_disabled"));
    }
    normalized.output =
        super::report_actions::normalized_path(&spec.project.root, &normalized.output)?;
    normalized.prompt_file =
        super::report_actions::normalized_path(&spec.project.root, &normalized.prompt_file)?;
    if normalized != spec.report {
        return Ok(LiveReport::Cancel("report_policy_changed"));
    }
    let family_project: Option<String> =
        sqlx::query_scalar("SELECT project_id FROM families WHERE id = ?")
            .bind(spec.family_id.to_string())
            .fetch_optional(&mut **tx)
            .await
            .map_err(|source| db("read report family project", source))?;
    if family_project.as_deref() != Some(spec.project.id.to_string().as_str()) {
        return Err(PersistenceError::InvalidValue {
            entity: "report family",
            value: "family/project mismatch".into(),
        });
    }
    let current: Option<String> = sqlx::query_scalar(
        "SELECT id FROM generations WHERE family_id = ? ORDER BY generation_number DESC LIMIT 1",
    )
    .bind(spec.family_id.to_string())
    .fetch_optional(&mut **tx)
    .await
    .map_err(|source| db("read current publication generation", source))?;
    if current.as_deref() != Some(spec.generation.id.to_string().as_str()) {
        return Ok(LiveReport::Cancel("report_generation_superseded"));
    }
    let context = super::report_context::read_context(tx, spec.family_id).await?;
    if context.generation.identity != spec.generation {
        return Ok(LiveReport::Cancel("report_generation_superseded"));
    }
    if !crate::report::context_ready(&context) {
        return Ok(LiveReport::Defer("report_context_not_ready"));
    }
    let context_hash = format!(
        "sha256:{:x}",
        Sha256::digest(json(&context, "report context fingerprint")?.as_bytes())
    );
    Ok(LiveReport::Ready(Box::new(PreparedReport {
        action_id: claim.record_id,
        lease_id: claim.lease_id,
        spec,
        context,
        context_hash,
        run_id: None,
    })))
}

async fn finish_report_wait(
    tx: &mut Transaction<'_, Sqlite>,
    claim: &Claim<ActionId>,
    cancel: bool,
    reason: &'static str,
) -> PersistenceResult<()> {
    let state = if cancel { "cancelled" } else { "pending" };
    let project: Option<String> = sqlx::query_scalar("UPDATE actions SET state = ?, attempts = MAX(0, attempts - 1), claim_id = NULL, claim_owner = NULL, claim_expires_at = NULL, available_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now', '+' || ? || ' seconds'), last_error = ?, updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now') WHERE id = ? AND kind = 'generate_report' AND state = 'running' AND claim_id = ? AND claim_owner = ? AND claim_expires_at > strftime('%Y-%m-%dT%H:%M:%fZ', 'now') RETURNING project_id")
        .bind(state).bind(if cancel { 0 } else { DEFER_SECONDS }).bind(reason)
        .bind(claim.record_id.to_string()).bind(claim.lease_id.to_string()).bind(&claim.owner)
        .fetch_optional(&mut **tx).await.map_err(|source| db("transition waiting report", source))?;
    let project = project.ok_or(PersistenceError::Conflict {
        entity: "report claim",
    })?;
    super::report_ledger::finish_runs(
        tx,
        claim.record_id,
        claim.lease_id,
        if cancel {
            super::report_ledger::RunFinish::Cancel
        } else {
            super::report_ledger::RunFinish::Defer
        },
        reason,
    )
    .await?;
    let event = state_event(
        EventKind::ActionStateChanged,
        "action",
        &claim.record_id.to_string(),
        state,
        Some(claim.lease_id),
        Some(serde_json::json!({"reason":reason})),
    )?;
    insert_event(
        tx,
        parse_id(&project, "report project")?,
        None,
        None,
        &event,
    )
    .await
}

async fn publication_barrier(
    tx: &mut Transaction<'_, Sqlite>,
    claim: &Claim<ActionId>,
    prepared: &PreparedReport,
    live_policy: &ReportConfig,
) -> PersistenceResult<Option<ReportPublishOutcome>> {
    let (cancel, reason) = match live_report(tx, claim, live_policy).await? {
        LiveReport::Ready(current)
            if current.spec == prepared.spec && current.context_hash == prepared.context_hash =>
        {
            return Ok(None);
        }
        LiveReport::Ready(_) => (false, "report_context_changed"),
        LiveReport::Defer(reason) => (false, reason),
        LiveReport::Cancel(reason) => (true, reason),
    };
    finish_report_wait(tx, claim, cancel, reason).await?;
    Ok(Some(if cancel {
        ReportPublishOutcome::Cancelled
    } else {
        ReportPublishOutcome::Deferred
    }))
}
