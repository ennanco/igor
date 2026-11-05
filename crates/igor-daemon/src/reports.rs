use std::time::Duration;

use igor_core::{
    ActionId, ActionRetryPolicy, Claim, Database, PersistenceError, REPORT_ACTION_VERSION,
    ReportActionSpec, ReportConfig, ReportExecutionError, ReportPreparation,
    ReportPublicationError,
};
use tokio::{sync::watch, task::JoinHandle, time};

const LEASE: Duration = Duration::from_secs(30);
const HEARTBEAT: Duration = Duration::from_secs(5);
const POLL: Duration = Duration::from_millis(200);

pub(crate) async fn run(database: Database, mut shutdown: watch::Receiver<bool>) {
    let owner = format!("reporter:{}", std::process::id());
    loop {
        if *shutdown.borrow() {
            return;
        }
        match database.actions().claim_report(&owner, LEASE).await {
            Ok(Some(claim)) => execute(&database, &claim, &mut shutdown).await,
            Ok(None) => {}
            Err(error) => tracing::warn!(%error, "cannot claim report action"),
        }
        tokio::select! { _ = time::sleep(POLL) => {}, _ = shutdown.changed() => return }
    }
}

async fn blocking<T: Send + 'static>(
    mut task: JoinHandle<T>,
    database: &Database,
    claim: &Claim<ActionId>,
    shutdown: &mut watch::Receiver<bool>,
) -> Option<Result<T, tokio::task::JoinError>> {
    loop {
        if *shutdown.borrow() {
            task.abort();
            return None;
        }
        tokio::select! {
            result = &mut task => return Some(result),
            _ = shutdown.changed() => { task.abort(); return None; },
            _ = time::sleep(HEARTBEAT) => {
                if database.actions().heartbeat(claim, LEASE).await.is_err() { task.abort(); return None; }
            }
        }
    }
}

async fn policy(
    database: &Database,
    claim: &Claim<ActionId>,
    spec: &ReportActionSpec,
    shutdown: &mut watch::Receiver<bool>,
) -> Option<ReportConfig> {
    match database.projects().get(spec.project.id).await {
        Ok(Some(project)) if project == spec.project => {}
        Ok(_) => return Some(spec.report.clone()), // Core cancels changed/deregistered project, without reading its files.
        Err(error) => {
            tracing::warn!(%error, "cannot read report project");
            return None;
        }
    }
    let project = spec.project.clone();
    match blocking(
        tokio::task::spawn_blocking(move || crate::report_triggers::load_policy(&project)),
        database,
        claim,
        shutdown,
    )
    .await
    {
        Some(Ok(Ok(config))) => Some(config),
        Some(Ok(Err(error))) => {
            tracing::warn!(%error, "report awaits current policy");
            None
        }
        Some(Err(error)) => {
            tracing::warn!(%error, "report policy task failed");
            None
        }
        None => None,
    }
}

async fn defer(database: &Database, claim: &Claim<ActionId>, reason: &'static str) {
    if let Err(error) = database.actions().defer_report(claim, reason).await {
        tracing::debug!(%error, action_id = %claim.record_id, "report deferral lost its claim");
    }
}

async fn failure(database: &Database, claim: &Claim<ActionId>, error: ReportExecutionError) {
    tracing::warn!(%error, action_id = %claim.record_id, "report action did not complete");
    let publication = match &error {
        ReportExecutionError::Publication(error) => Some(error),
        ReportExecutionError::Generated(igor_core::GeneratedReportError::Publication(error)) => {
            Some(error)
        }
        _ => None,
    };
    if matches!(error, ReportExecutionError::PublishedUncertain { .. }) {
        defer(database, claim, "report_publication_uncertain").await;
    } else if matches!(
        publication,
        Some(ReportPublicationError::Busy { .. } | ReportPublicationError::Changed { .. })
    ) {
        defer(database, claim, "report_output_changed_or_busy").await;
    } else if matches!(
        error,
        ReportExecutionError::Persistence(PersistenceError::Conflict { .. })
    ) {
        // Expired or replaced lease: its new owner decides what happens next.
    } else if let Err(error) = database
        .actions()
        .retry(
            claim,
            &ActionRetryPolicy::default().0,
            "report_execution_failed",
        )
        .await
    {
        tracing::warn!(%error, "cannot persist report operational retry");
    }
}

async fn execute(
    database: &Database,
    claim: &Claim<ActionId>,
    shutdown: &mut watch::Receiver<bool>,
) {
    let work = match database.actions().claimed(claim).await {
        Ok(Some(work)) => work,
        Ok(None) => return,
        Err(error) => {
            failure(database, claim, error.into()).await;
            return;
        }
    };
    let spec: ReportActionSpec = match serde_json::from_value(work.action.spec) {
        Ok(spec) => spec,
        Err(_) => {
            let error = PersistenceError::InvalidValue {
                entity: "report action",
                value: "invalid report specification".into(),
            };
            failure(database, claim, error.into()).await;
            return;
        }
    };
    if spec.schema_version != REPORT_ACTION_VERSION || spec.project.id != work.action.project_id {
        failure(
            database,
            claim,
            PersistenceError::InvalidValue {
                entity: "report action",
                value: "invalid version/project".into(),
            }
            .into(),
        )
        .await;
        return;
    }
    let Some(live) = policy(database, claim, &spec, shutdown).await else {
        defer(database, claim, "report_policy_unavailable").await;
        return;
    };
    let prepared = match database.actions().prepare_report(claim, &live).await {
        Ok(ReportPreparation::Ready(prepared)) => *prepared,
        Ok(_) => return,
        Err(error) => {
            failure(database, claim, error).await;
            return;
        }
    };
    let publication = match blocking(
        tokio::task::spawn_blocking(move || prepared.stage_and_lock()),
        database,
        claim,
        shutdown,
    )
    .await
    {
        Some(Ok(Ok(publication))) => publication,
        Some(Ok(Err(error))) => {
            failure(database, claim, error).await;
            return;
        }
        Some(Err(error)) => {
            failure(
                database,
                claim,
                PersistenceError::BlockingTask(error).into(),
            )
            .await;
            return;
        }
        None => {
            defer(database, claim, "report_staging_interrupted").await;
            return;
        }
    };
    let Some(live) = policy(database, claim, &spec, shutdown).await else {
        defer(database, claim, "report_policy_unavailable").await;
        return;
    };
    if *shutdown.borrow() {
        defer(database, claim, "report_shutdown").await;
        return;
    }
    // Do not cancel/drop this future once publication starts: the writer fences the rename.
    if let Err(error) = database
        .actions()
        .publish_report(claim, publication, &live)
        .await
    {
        failure(database, claim, error).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use igor_core::{ActionRecord, ActionState, Project, ProjectId};

    #[tokio::test]
    async fn report_staging_wait_renews_lease_and_shutdown_abandons_only_unpublished_work()
    -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempfile::tempdir()?;
        let database = Database::open(directory.path().join("igor.db")).await?;
        let project = Project {
            id: ProjectId::new(),
            name: "lease fixture".into(),
            root: directory.path().to_path_buf(),
            config_path: directory.path().join(".igor/project.toml"),
        };
        database.projects().insert(&project).await?;
        database
            .actions()
            .insert(&ActionRecord {
                id: ActionId::new(),
                project_id: project.id,
                kind: "generate_report".into(),
                state: ActionState::Pending,
                spec: serde_json::json!({}),
                idempotency_key: "heartbeat-fixture".into(),
            })
            .await?;
        let claim = database
            .actions()
            .claim_report("heartbeat test", LEASE)
            .await?
            .ok_or("claim")?;
        sqlx::query("UPDATE actions SET claim_expires_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now', '+20 seconds') WHERE id = ?")
            .bind(claim.record_id.to_string()).execute(database.pool()).await?;
        let previous: String =
            sqlx::query_scalar("SELECT claim_expires_at FROM actions WHERE id = ?")
                .bind(claim.record_id.to_string())
                .fetch_one(database.pool())
                .await?;
        let (release, wait) = std::sync::mpsc::channel::<()>();
        let work = tokio::task::spawn_blocking(move || {
            let _ = wait.recv();
        });
        let (stop, mut shutdown) = watch::channel(false);
        let db = database.clone();
        let lease = claim.clone();
        let helper = tokio::spawn(async move { blocking(work, &db, &lease, &mut shutdown).await });
        time::timeout(Duration::from_secs(15), async {
            loop {
                let renewed: String =
                    sqlx::query_scalar("SELECT claim_expires_at FROM actions WHERE id = ?")
                        .bind(claim.record_id.to_string())
                        .fetch_one(database.pool())
                        .await?;
                if renewed != previous {
                    return Ok::<_, sqlx::Error>(());
                }
                time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await??;
        stop.send(true)?;
        assert!(
            time::timeout(Duration::from_secs(2), helper)
                .await??
                .is_none()
        );
        let _ = release.send(());
        assert!(!project.root.join("RESULTS.md").exists());
        let state: String = sqlx::query_scalar("SELECT state FROM actions WHERE id = ?")
            .bind(claim.record_id.to_string())
            .fetch_one(database.pool())
            .await?;
        assert_eq!(state, "running");
        Ok(())
    }
}
