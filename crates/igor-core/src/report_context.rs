use super::*;
use crate::{ConfigurationIdentity, RecoveryId, Seed, SourceIdentity};
use serde::{Deserialize, Serialize};

pub const REPORT_CONTEXT_VERSION: u32 = 1;

/// Read-only projection; publication must revalidate this snapshot's identity and readiness.
#[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
pub struct ReportContext {
    pub schema_version: u32,
    pub family: Family,
    pub generation: ReportGeneration,
    pub required_seeds: Vec<Seed>,
    pub members: Vec<ReportMember>,
    pub history: Vec<ReportGeneration>,
    pub repairs: Vec<ReportRepair>,
    pub tools: ReportToolVersions,
    pub ready: bool,
    pub primary_metrics: Option<Vec<ReportMetricSeries>>,
}

#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
pub struct ReportGeneration {
    pub identity: GenerationIdentity,
    pub status: GenerationStatus,
    pub counts: BTreeMap<String, u64>,
}

#[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
pub struct ReportMember {
    pub job_id: JobId,
    pub name: String,
    pub seed: Seed,
    pub state: JobState,
    pub attempts: Vec<ReportAttempt>,
}

#[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
pub struct ReportAttempt {
    pub attempt_id: AttemptId,
    pub sequence: u32,
    pub state: AttemptState,
    pub source: SourceIdentity,
    pub configuration: ConfigurationIdentity,
    pub metric_schema_version: u32,
    pub extractor: Option<ReportExtractor>,
    pub metrics: AttemptMetrics,
    pub artifacts: Vec<ArtifactRecord>,
}

#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
pub struct ReportExtractor {
    pub program: String,
    pub command_sha256: String,
}

#[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
pub struct ReportMetricSeries {
    pub name: String,
    pub values: Vec<ReportMetricValue>,
}

#[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
pub struct ReportMetricValue {
    pub seed: Seed,
    pub attempt_id: AttemptId,
    pub value: f64,
}

#[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
pub struct ReportRepair {
    pub recovery_id: RecoveryId,
    pub generation_id: GenerationId,
    pub attempt_id: AttemptId,
    pub state: String,
    pub diagnosis: Option<Value>,
    pub patch: Option<Value>,
    pub validation: Option<Value>,
    pub decision: Option<Value>,
    pub worktree: Option<Value>,
}

#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
pub struct ReportToolVersions {
    pub igor_context_builder: String,
    pub extractor: Option<String>,
    pub prompt: Option<String>,
    pub provider: Option<String>,
    pub model: Option<String>,
}

impl FamilyGenerationRepository<'_> {
    pub async fn report_context(&self, family_id: FamilyId) -> PersistenceResult<ReportContext> {
        let mut transaction = self
            .database
            .pool
            .begin()
            .await
            .map_err(|source| db("begin report context read", source))?;
        read_context(&mut transaction, family_id).await
    }
}

fn invalid_report(reason: &'static str) -> PersistenceError {
    PersistenceError::InvalidValue {
        entity: "report context",
        value: reason.into(),
    }
}

async fn generation_members(
    transaction: &mut Transaction<'_, Sqlite>,
    family: &Family,
    generation: &Generation,
) -> PersistenceResult<Vec<StoredJob>> {
    let rows = sqlx::query("SELECT id, project_id, spec_json, state, priority, submission_order, submitted_at, updated_at FROM jobs WHERE family_id = ? AND generation_id = ? ORDER BY submission_order")
        .bind(family.id.to_string()).bind(generation.identity.id.to_string())
        .fetch_all(&mut **transaction).await.map_err(|source| db("read report members", source))?;
    let mut jobs = Vec::with_capacity(rows.len());
    for row in rows {
        let id: String = row.get("id");
        let project: String = row.get("project_id");
        let job = decode_job(row)?;
        if job.spec.id.to_string() != id
            || job.spec.project_id != family.project_id
            || project != family.project_id.to_string()
            || job.spec.family.as_ref().is_none_or(|member| {
                member.family_id != family.id || member.generation != generation.identity
            })
        {
            return Err(invalid_report(
                "member identity differs from its stored generation",
            ));
        }
        jobs.push(job);
    }
    Ok(jobs)
}

pub(super) async fn read_context(
    transaction: &mut Transaction<'_, Sqlite>,
    family_id: FamilyId,
) -> PersistenceResult<ReportContext> {
    let row = sqlx::query("SELECT id, project_id, name FROM families WHERE id = ?")
        .bind(family_id.to_string())
        .fetch_optional(&mut **transaction)
        .await
        .map_err(|source| db("read report family", source))?
        .ok_or(PersistenceError::NotFound { entity: "family" })?;
    let family = Family {
        id: parse_id(row.get("id"), "report family")?,
        project_id: parse_id(row.get("project_id"), "report project")?,
        name: row.get("name"),
    };
    let rows = sqlx::query("SELECT id, family_id, generation_number, source_revision, protocol_digest, spec_json FROM generations WHERE family_id = ? ORDER BY generation_number")
        .bind(family_id.to_string()).fetch_all(&mut **transaction).await
        .map_err(|source| db("read report generations", source))?;
    let mut generations = rows
        .into_iter()
        .map(decode_generation)
        .collect::<PersistenceResult<Vec<_>>>()?;
    let current = generations.pop().ok_or(PersistenceError::NotFound {
        entity: "family generation",
    })?;
    let mut jobs = generation_members(transaction, &family, &current).await?;
    let status = current.assess(&jobs)?;
    if status == GenerationStatus::Invalid {
        return Err(invalid_report("invalid seed membership"));
    }
    if status == GenerationStatus::Comparable {
        // Reuse M11's exact selection boundary inside this same read snapshot.
        jobs =
            FamilyGenerationRepository::comparable_in(transaction, family_id, current.identity.id)
                .await?
                .members;
    }
    jobs.sort_by_key(|job| job.spec.family.as_ref().map(|member| member.seed.0));
    let file: FamilyFile =
        serde_json::from_value(current.spec["family"].clone()).map_err(|source| {
            PersistenceError::Serialization {
                contract: "report family file",
                source,
            }
        })?;
    let mut required_seeds: Vec<_> = file.members.iter().map(|member| member.seed).collect();
    required_seeds.sort_by_key(|seed| seed.0);
    let mut history = Vec::with_capacity(generations.len());
    for generation in generations {
        let members = generation_members(transaction, &family, &generation).await?;
        history.push(summarize(
            &generation,
            generation.assess(&members)?,
            &members,
        ));
    }
    let mut members = Vec::with_capacity(jobs.len());
    let mut ready = status == GenerationStatus::Comparable;
    let mut series = BTreeMap::<String, Vec<ReportMetricValue>>::new();
    for job in &jobs {
        let seed = job
            .spec
            .family
            .as_ref()
            .ok_or_else(|| invalid_report("missing seed"))?
            .seed;
        let attempts = read_attempts(transaction, job, &current).await?;
        if let Some(latest) = attempts.last() {
            if status == GenerationStatus::Comparable && latest.state != AttemptState::Succeeded {
                return Err(invalid_report(
                    "successful job has no successful latest attempt",
                ));
            }
            ready &= extraction_ready(latest);
            if latest.metrics.action_state.as_deref() == Some("succeeded")
                && matches!(
                    latest
                        .metrics
                        .document
                        .as_ref()
                        .and_then(|doc| doc["status"].as_str()),
                    Some("success" | "partial")
                )
            {
                for (name, value) in &latest.metrics.metrics {
                    series
                        .entry(name.clone())
                        .or_default()
                        .push(ReportMetricValue {
                            seed,
                            attempt_id: latest.attempt_id,
                            value: *value,
                        });
                }
            }
        } else if status == GenerationStatus::Comparable {
            return Err(invalid_report("successful job has no attempt"));
        } else {
            ready = false;
        }
        members.push(ReportMember {
            job_id: job.spec.id,
            name: job.spec.name.clone(),
            seed,
            state: job.state,
            attempts,
        });
    }
    let primary_metrics = ready.then(|| {
        series
            .into_iter()
            .map(|(name, values)| ReportMetricSeries { name, values })
            .collect()
    });
    Ok(ReportContext {
        schema_version: REPORT_CONTEXT_VERSION,
        family,
        generation: summarize(&current, status, &jobs),
        required_seeds,
        members,
        history,
        repairs: read_repairs(transaction, family_id).await?,
        tools: ReportToolVersions {
            igor_context_builder: env!("CARGO_PKG_VERSION").into(),
            extractor: None,
            prompt: None,
            provider: None,
            model: None,
        },
        ready,
        primary_metrics,
    })
}

fn summarize(
    generation: &Generation,
    status: GenerationStatus,
    members: &[StoredJob],
) -> ReportGeneration {
    let mut counts = job_state_counts();
    for job in members {
        *counts.entry(job.state.as_str().into()).or_default() += 1;
    }
    ReportGeneration {
        identity: generation.identity.clone(),
        status,
        counts,
    }
}

fn extraction_ready(attempt: &ReportAttempt) -> bool {
    match attempt.metrics.action_state.as_deref() {
        Some("failed" | "cancelled") => true,
        Some("succeeded") => attempt.metrics.document.is_some(),
        Some(_) => false,
        None => attempt.extractor.is_none(),
    }
}

async fn read_attempts(
    transaction: &mut Transaction<'_, Sqlite>,
    job: &StoredJob,
    generation: &Generation,
) -> PersistenceResult<Vec<ReportAttempt>> {
    let rows = sqlx::query("SELECT id, job_id, project_id, sequence, spec_json, state, created_at, started_at, finished_at, updated_at FROM attempts WHERE job_id = ? ORDER BY sequence")
        .bind(job.spec.id.to_string()).fetch_all(&mut **transaction).await
        .map_err(|source| db("read report attempts", source))?;
    let mut attempts = Vec::with_capacity(rows.len());
    for row in rows {
        let id: String = row.get("id");
        let job_id: String = row.get("job_id");
        let project_id: String = row.get("project_id");
        let sequence: i64 = row.get("sequence");
        let stored = decode_attempt(row)?;
        let attempt = &stored.spec;
        let source_valid = match attempt.source() {
            SourceIdentity::Git(source) => {
                source.revision == generation.identity.source_revision
                    && serde_json::to_value(&source.dirty_digest).map_err(|source| {
                        PersistenceError::Serialization {
                            contract: "report Git identity",
                            source,
                        }
                    })? == generation.spec["dirty_digest"]
            }
            _ => false,
        };
        if attempt.id().to_string() != id
            || attempt.job_id() != job.spec.id
            || job_id != job.spec.id.to_string()
            || project_id != job.spec.project_id.to_string()
            || i64::from(attempt.sequence()) != sequence
            || attempt.family() != job.spec.family.as_ref()
            || !source_valid
            || attempt.command() != &job.spec.command
            || attempt.executor() != &job.spec.executor
            || attempt.resources() != &job.spec.resources
            || Some(attempt.configuration().project_digest.as_str())
                != generation.spec["project_digest"].as_str()
            || attempt.configuration().job_digest
                != crate::submission::digest_json(&job.spec, "report member job")?
        {
            return Err(invalid_report(
                "attempt differs from its frozen member identity",
            ));
        }
        let metrics = ActionRepository::metrics_in(transaction, attempt.id()).await?;
        if let Some(document) = &metrics.document {
            crate::validate_metric_document(document)
                .map_err(|_| invalid_report("invalid metric document"))?;
        }
        let rows = sqlx::query("SELECT id, attempt_id, path, role, role_detail, sha256, size_bytes, state, lifecycle_state, diagnosis_required, metadata_json FROM artifacts WHERE attempt_id = ? ORDER BY path, id")
            .bind(id).fetch_all(&mut **transaction).await.map_err(|source| db("read report artifacts", source))?;
        let artifacts = rows
            .into_iter()
            .map(decode_artifact)
            .collect::<PersistenceResult<_>>()?;
        let extractor = attempt
            .result()
            .extractor
            .as_ref()
            .map(|command| {
                Ok::<_, PersistenceError>(ReportExtractor {
                    program: command.program.clone(),
                    command_sha256: crate::submission::digest_json(
                        command,
                        "report extractor identity",
                    )?,
                })
            })
            .transpose()?;
        attempts.push(ReportAttempt {
            attempt_id: attempt.id(),
            sequence: attempt.sequence(),
            state: stored.state,
            source: attempt.source().clone(),
            configuration: attempt.configuration().clone(),
            metric_schema_version: attempt.result().schema_version,
            extractor,
            metrics,
            artifacts,
        });
    }
    Ok(attempts)
}

async fn read_repairs(
    transaction: &mut Transaction<'_, Sqlite>,
    family_id: FamilyId,
) -> PersistenceResult<Vec<ReportRepair>> {
    let rows = sqlx::query("SELECT r.id, r.attempt_id, r.state, r.diagnosis_json, r.patch_json, r.validation_json, r.decision_json, j.generation_id, w.path AS worktree_path, w.state AS worktree_state, w.patch_sha256, w.records_json FROM recoveries r JOIN attempts a ON a.id = r.attempt_id JOIN jobs j ON j.id = a.job_id LEFT JOIN repair_worktrees w ON w.recovery_id = r.id WHERE j.family_id = ? ORDER BY j.generation_id, r.id")
        .bind(family_id.to_string()).fetch_all(&mut **transaction).await
        .map_err(|source| db("read report repairs", source))?;
    rows.into_iter().map(|row| {
        let field = |name: &str| -> PersistenceResult<Option<Value>> {
            row.get::<Option<String>, _>(name).map(|value| from_json(&value, "report repair evidence")).transpose()
        };
        let worktree = row.get::<Option<String>, _>("worktree_path").map(|path| {
            Ok::<_, PersistenceError>(serde_json::json!({"path": path, "state": row.get::<String, _>("worktree_state"),
                "patch_sha256": row.get::<Option<String>, _>("patch_sha256"), "records": field("records_json")?}))
        }).transpose()?;
        Ok(ReportRepair { recovery_id: parse_id(row.get("id"), "report recovery")?,
            generation_id: parse_id(row.get("generation_id"), "report repair generation")?,
            attempt_id: parse_id(row.get("attempt_id"), "report repair attempt")?, state: row.get("state"),
            diagnosis: field("diagnosis_json")?, patch: field("patch_json")?, validation: field("validation_json")?,
            decision: field("decision_json")?, worktree })
    }).collect()
}
