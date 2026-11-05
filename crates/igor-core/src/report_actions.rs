use super::*;
use crate::{ReportConfig, ReportTrigger};

pub const REPORT_ACTION_VERSION: u32 = 1;
const PAGE_SIZE: i64 = 64;

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct ReportCandidate {
    pub family_id: FamilyId,
    pub generation: GenerationIdentity,
    pub project: Project,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct ReportActionSpec {
    pub schema_version: u32,
    pub family_id: FamilyId,
    pub generation: GenerationIdentity,
    pub project: Project,
    pub report: ReportConfig,
    #[serde(default)]
    pub origin: ReportOrigin,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ReportOrigin {
    #[default]
    FamilyCompleted,
    Manual {
        request_id: ActionId,
    },
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct ReportRequestReceipt {
    pub action_id: ActionId,
    pub project_id: ProjectId,
    pub family_id: FamilyId,
    pub generation: GenerationIdentity,
    pub output: PathBuf,
    pub state: ActionState,
}

fn report_key(family: FamilyId, generation: GenerationId) -> String {
    format!("generate_report:v1:family-completed:{family}:{generation}")
}

pub(super) fn action_key(spec: &ReportActionSpec) -> String {
    match spec.origin {
        ReportOrigin::FamilyCompleted => report_key(spec.family_id, spec.generation.id),
        ReportOrigin::Manual { request_id } => format!(
            "generate_report:v1:manual:{}:{}:{request_id}",
            spec.family_id, spec.generation.id
        ),
    }
}

impl ActionRepository<'_> {
    pub async fn report_candidate(
        &self,
        family_id: FamilyId,
        scope: Option<ProjectId>,
    ) -> PersistenceResult<Option<ReportCandidate>> {
        let row = sqlx::query("SELECT p.id, p.name, p.root_path, p.config_path, g.id AS generation_id, g.family_id, g.generation_number, g.source_revision, g.protocol_digest FROM families f JOIN projects p ON p.id = f.project_id JOIN generations g ON g.family_id = f.id WHERE f.id = ? AND p.registered = 1 AND p.is_alias = 0 AND p.id = COALESCE(?, p.id) ORDER BY g.generation_number DESC LIMIT 1")
            .bind(family_id.to_string()).bind(scope.map(|id| id.to_string())).fetch_optional(&self.database.pool).await
            .map_err(|source| db("select requested report generation", source))?;
        row.map(decode_candidate).transpose()
    }

    pub async fn manual_report_request(
        &self,
        request_id: ActionId,
        family_id: FamilyId,
        scope: Option<ProjectId>,
    ) -> PersistenceResult<Option<ReportRequestReceipt>> {
        let mut tx = self
            .database
            .pool
            .begin()
            .await
            .map_err(|source| db("read manual report request", source))?;
        manual_receipt(&mut tx, request_id, family_id, scope).await
    }

    pub async fn enqueue_manual_report(
        &self,
        candidate: &ReportCandidate,
        config: &ReportConfig,
        request_id: ActionId,
    ) -> PersistenceResult<ReportRequestReceipt> {
        self.enqueue_report(
            candidate,
            config,
            ReportOrigin::Manual { request_id },
            request_id,
        )
        .await?
        .ok_or(PersistenceError::Conflict {
            entity: "report project or current generation",
        })?;
        self.manual_report_request(request_id, candidate.family_id, Some(candidate.project.id))
            .await?
            .ok_or(PersistenceError::NotFound {
                entity: "manual report request",
            })
    }
    /// Cheap read-only discovery; enqueue revalidates the frozen domain contract.
    /// The cursor follows UUID order, not family-local generation numbers.
    pub async fn report_candidates(
        &self,
        after: Option<GenerationId>,
    ) -> PersistenceResult<Vec<ReportCandidate>> {
        let rows = sqlx::query(
            "SELECT p.id, p.name, p.root_path, p.config_path, g.id AS generation_id,
                    g.family_id, g.generation_number, g.source_revision, g.protocol_digest
             FROM successful_current_generations eligible JOIN generations g ON g.id = eligible.id
             JOIN families f ON f.id = g.family_id JOIN projects p ON p.id = f.project_id
             WHERE p.registered = 1 AND p.is_alias = 0 AND g.id > COALESCE(?, '')
               AND NOT EXISTS (SELECT 1 FROM actions a WHERE a.idempotency_key =
                   'generate_report:v1:family-completed:' || g.family_id || ':' || g.id)
             ORDER BY g.id LIMIT ?",
        )
        .bind(after.map(|id| id.to_string()))
        .bind(PAGE_SIZE)
        .fetch_all(&self.database.pool)
        .await
        .map_err(|source| db("discover report generations", source))?;
        rows.into_iter().map(decode_candidate).collect()
    }

    /// Enqueues one family-level intent, independently of extraction readiness.
    pub async fn enqueue_family_report(
        &self,
        candidate: &ReportCandidate,
        config: &ReportConfig,
    ) -> PersistenceResult<Option<ActionId>> {
        self.enqueue_report(
            candidate,
            config,
            ReportOrigin::FamilyCompleted,
            ActionId::new(),
        )
        .await
    }

    async fn enqueue_report(
        &self,
        candidate: &ReportCandidate,
        config: &ReportConfig,
        origin: ReportOrigin,
        id: ActionId,
    ) -> PersistenceResult<Option<ActionId>> {
        if !config.enabled && origin == ReportOrigin::FamilyCompleted {
            return Ok(None);
        }
        match config.trigger {
            ReportTrigger::FamilyCompleted => {}
        }
        let mut transaction = self
            .database
            .pool
            .begin_with("BEGIN IMMEDIATE")
            .await
            .map_err(|source| db("reserve report enqueue writer", source))?;
        if let ReportOrigin::Manual { request_id } = origin {
            if let Some(receipt) = manual_receipt(
                &mut transaction,
                request_id,
                candidate.family_id,
                Some(candidate.project.id),
            )
            .await?
            {
                return Ok(Some(receipt.action_id));
            }
            if !config.enabled {
                return Err(PersistenceError::ReportDisabled);
            }
        }
        let mut config = config.clone();
        config.output = normalized_path(&candidate.project.root, &config.output)?;
        config.prompt_file = normalized_path(&candidate.project.root, &config.prompt_file)?;
        match validate_report_candidate(&mut transaction, candidate).await {
            Ok(true) => {}
            Ok(false) => return Ok(None),
            Err(PersistenceError::GenerationNotComparable { .. })
                if origin == ReportOrigin::FamilyCompleted =>
            {
                return Ok(None);
            }
            Err(error) => return Err(error),
        }
        let spec = ReportActionSpec {
            schema_version: REPORT_ACTION_VERSION,
            family_id: candidate.family_id,
            generation: candidate.generation.clone(),
            project: candidate.project.clone(),
            report: config,
            origin,
        };
        let key = action_key(&spec);
        let existing = sqlx::query("SELECT id, project_id, kind, job_id, attempt_id, spec_json FROM actions WHERE idempotency_key = ?")
            .bind(&key).fetch_optional(&mut *transaction).await.map_err(|source| db("read existing family report intent", source))?;
        if let Some(row) = existing {
            let frozen: ReportActionSpec = from_json(row.get("spec_json"), "report action")?;
            if row.get::<String, _>("kind") != "generate_report"
                || row.get::<String, _>("project_id") != candidate.project.id.to_string()
                || row.get::<Option<String>, _>("job_id").is_some()
                || row.get::<Option<String>, _>("attempt_id").is_some()
                || frozen.schema_version != REPORT_ACTION_VERSION
                || frozen.family_id != candidate.family_id
                || frozen.generation != candidate.generation
                || frozen.project.id != candidate.project.id
                || !frozen.report.enabled
                || frozen.origin != origin
            {
                return Err(PersistenceError::Conflict {
                    entity: "family report intent",
                });
            }
            return Ok(Some(parse_id(
                row.get("id"),
                "existing family report action",
            )?));
        }
        sqlx::query("INSERT INTO actions (id, project_id, kind, state, spec_json, idempotency_key, available_at) VALUES (?, ?, 'generate_report', 'pending', ?, ?, strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))")
            .bind(id.to_string()).bind(candidate.project.id.to_string()).bind(json(&spec, "report action")?).bind(&key)
            .execute(&mut *transaction).await.map_err(|source| db("enqueue family report", source))?;
        let event = state_event(
            EventKind::ActionStateChanged,
            "action",
            &id.to_string(),
            "pending",
            None,
            Some(
                serde_json::json!({"trigger":if origin == ReportOrigin::FamilyCompleted { "family-completed" } else { "manual" },"family_id":candidate.family_id,"generation_id":candidate.generation.id}),
            ),
        )?;
        insert_event(&mut transaction, candidate.project.id, None, None, &event).await?;
        transaction
            .commit()
            .await
            .map_err(|source| db("commit family report enqueue", source))?;
        Ok(Some(id))
    }
}

fn decode_candidate(row: sqlx::sqlite::SqliteRow) -> PersistenceResult<ReportCandidate> {
    let number: i64 = row.get("generation_number");
    Ok(ReportCandidate {
        family_id: parse_id(row.get("family_id"), "report family")?,
        generation: GenerationIdentity {
            id: parse_id(row.get("generation_id"), "report generation")?,
            number: u32::try_from(number).map_err(|_| PersistenceError::InvalidValue {
                entity: "report generation number",
                value: number.to_string(),
            })?,
            source_revision: row.get("source_revision"),
            protocol_digest: row.get("protocol_digest"),
        },
        project: decode_project(row)?,
    })
}

async fn manual_receipt(
    tx: &mut Transaction<'_, Sqlite>,
    request_id: ActionId,
    family_id: FamilyId,
    scope: Option<ProjectId>,
) -> PersistenceResult<Option<ReportRequestReceipt>> {
    action_receipt(tx, request_id, family_id, scope, true).await
}

pub(super) async fn action_receipt(
    tx: &mut Transaction<'_, Sqlite>,
    request_id: ActionId,
    family_id: FamilyId,
    scope: Option<ProjectId>,
    manual: bool,
) -> PersistenceResult<Option<ReportRequestReceipt>> {
    let row = sqlx::query("SELECT id, project_id, kind, job_id, attempt_id, state, spec_json, idempotency_key FROM actions WHERE id = ?")
        .bind(request_id.to_string()).fetch_optional(&mut **tx).await.map_err(|source| db("read existing manual report", source))?;
    let Some(row) = row else {
        return Ok(None);
    };
    if row.get::<String, _>("kind") != "generate_report" {
        return Err(PersistenceError::Conflict {
            entity: "report request identity",
        });
    }
    let spec: ReportActionSpec = from_json(row.get("spec_json"), "manual report action")?;
    if spec.family_id != family_id || scope.is_some_and(|id| id != spec.project.id) {
        return Err(PersistenceError::NotFound {
            entity: "report request",
        });
    }
    if spec.schema_version != REPORT_ACTION_VERSION
        || (manual && spec.origin != (ReportOrigin::Manual { request_id }))
        || matches!(spec.origin, ReportOrigin::Manual { request_id: original } if original != request_id)
        || row.get::<String, _>("project_id") != spec.project.id.to_string()
        || row.get::<String, _>("idempotency_key") != action_key(&spec)
        || row.get::<Option<String>, _>("job_id").is_some()
        || row.get::<Option<String>, _>("attempt_id").is_some()
        || !spec.report.enabled
    {
        return Err(PersistenceError::Conflict {
            entity: "report request identity",
        });
    }
    let value: String = row.get("state");
    let state = ActionState::ALL
        .iter()
        .copied()
        .find(|state| state.as_str() == value)
        .ok_or(PersistenceError::InvalidValue {
            entity: "report action state",
            value,
        })?;
    Ok(Some(ReportRequestReceipt {
        action_id: request_id,
        project_id: spec.project.id,
        family_id,
        generation: spec.generation,
        output: spec.report.output,
        state,
    }))
}

pub(super) async fn validate_report_candidate(
    tx: &mut Transaction<'_, Sqlite>,
    candidate: &ReportCandidate,
) -> PersistenceResult<bool> {
    let project = sqlx::query("SELECT id, name, root_path, config_path FROM projects WHERE id = ? AND registered = 1 AND is_alias = 0")
        .bind(candidate.project.id.to_string()).fetch_optional(&mut **tx).await
        .map_err(|source| db("revalidate report project", source))?;
    if project.map(decode_project).transpose()?.as_ref() != Some(&candidate.project) {
        return Ok(false);
    }
    let current: Option<String> = sqlx::query_scalar("SELECT g.id FROM generations g JOIN families f ON f.id = g.family_id WHERE f.id = ? AND f.project_id = ? ORDER BY g.generation_number DESC LIMIT 1")
        .bind(candidate.family_id.to_string()).bind(candidate.project.id.to_string()).fetch_optional(&mut **tx).await
        .map_err(|source| db("revalidate report current generation", source))?;
    if current.as_deref() != Some(candidate.generation.id.to_string().as_str()) {
        return Ok(false);
    }
    let selection =
        FamilyGenerationRepository::comparable_in(tx, candidate.family_id, candidate.generation.id)
            .await?;
    if selection.identity() != &candidate.generation {
        return Ok(false);
    }
    if selection
        .members()
        .iter()
        .any(|job| job.spec.project_id != candidate.project.id)
    {
        return Err(PersistenceError::InvalidValue {
            entity: "report member project",
            value: "mixed project membership".into(),
        });
    }
    Ok(true)
}

pub(super) fn normalized_path(root: &Path, path: &Path) -> PersistenceResult<PathBuf> {
    crate::report_publication::resolve_report_output(root, path).map_err(|error| {
        PersistenceError::InvalidValue {
            entity: "report policy path",
            value: error.to_string(),
        }
    })
}
