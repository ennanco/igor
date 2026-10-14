use std::time::Duration;

use igor_core::{
    ActionRetryPolicy, Database, DeliveryId, DeliveryRecord, DeliveryRetryPolicy, DeliveryState,
    EventId, RuntimePaths,
};
use tokio::{sync::watch, time};

use crate::telegram::{SendResult, TelegramClient, format_notification_with_metrics};

const LEASE: Duration = Duration::from_secs(30);
const POLL: Duration = Duration::from_millis(200);

pub async fn run(database: Database, paths: RuntimePaths, mut shutdown: watch::Receiver<bool>) {
    let owned_outputs = tokio::spawn(run_owned_failed_outputs(
        database.clone(),
        paths.clone(),
        shutdown.clone(),
    ));
    let failed_logs = tokio::spawn(run_failed_logs(
        database.clone(),
        paths.clone(),
        shutdown.clone(),
    ));
    let archives = tokio::spawn(run_success_log_archives(
        database.clone(),
        paths.clone(),
        shutdown.clone(),
    ));
    let extraction = tokio::spawn(crate::extract::run(
        database.clone(),
        paths.clone(),
        shutdown.clone(),
    ));
    let cleanup = tokio::spawn(run_failed_cleanup(
        database.clone(),
        paths.clone(),
        shutdown.clone(),
    ));
    let superseded_cleanup = tokio::spawn(run_superseded_cleanup(
        database.clone(),
        paths.clone(),
        shutdown.clone(),
    ));
    let worktree_cleanup = tokio::spawn(run_repair_worktree_cleanup(
        database.clone(),
        paths.clone(),
        shutdown.clone(),
    ));
    run_notifications(database, paths, &mut shutdown).await;
    if let Err(error) = owned_outputs.await {
        tracing::error!(%error,"owned failed output retirement task failed");
    }
    if let Err(error) = failed_logs.await {
        tracing::error!(%error, "failed log retirement task failed");
    }
    if let Err(error) = archives.await {
        tracing::error!(%error, "success log archive supervisor task failed");
    }
    if let Err(error) = extraction.await {
        tracing::error!(%error, "metric extraction supervisor task failed");
    }
    if let Err(error) = cleanup.await {
        tracing::error!(%error, "failed artifact cleanup supervisor task failed");
    }
    if let Err(error) = superseded_cleanup.await {
        tracing::error!(%error, "superseded artifact cleanup supervisor task failed");
    }
    if let Err(error) = worktree_cleanup.await {
        tracing::error!(%error, "repair worktree cleanup supervisor task failed");
    }
}

async fn run_success_log_archives(
    database: Database,
    paths: RuntimePaths,
    mut shutdown: watch::Receiver<bool>,
) {
    let mut cursor = None;
    loop {
        if *shutdown.borrow() {
            return;
        }
        match database.jobs().success_log_candidates(cursor).await {
            Ok(attempts) => {
                cursor = attempts.last().copied();
                for attempt_id in attempts {
                    if *shutdown.borrow() {
                        return;
                    }
                    if let Err(error) = database
                        .jobs()
                        .compress_success_logs(attempt_id, &paths)
                        .await
                    {
                        tracing::warn!(%error, %attempt_id, "success log compression deferred");
                    }
                }
            }
            Err(error) => tracing::warn!(%error, "cannot find success log archive candidates"),
        }
        tokio::select! { _ = time::sleep(POLL) => {}, _ = shutdown.changed() => return }
    }
}

async fn run_failed_logs(
    database: Database,
    paths: RuntimePaths,
    mut shutdown: watch::Receiver<bool>,
) {
    let mut cursor = None;
    loop {
        if *shutdown.borrow() {
            return;
        }
        match database.jobs().failed_log_candidates(cursor).await {
            Ok(attempts) => {
                cursor = attempts.last().copied();
                for attempt in attempts {
                    if *shutdown.borrow() {
                        return;
                    }
                    if let Err(error) = database.jobs().retire_failed_logs(attempt, &paths).await {
                        tracing::warn!(%error, %attempt, "failed log retirement deferred");
                    }
                }
            }
            Err(error) => tracing::warn!(%error, "cannot discover failed logs"),
        }
        tokio::select! { _ = time::sleep(Duration::from_secs(5)) => {}, _ = shutdown.changed() => return }
    }
}

async fn run_owned_failed_outputs(
    database: Database,
    paths: RuntimePaths,
    mut shutdown: watch::Receiver<bool>,
) {
    let mut cursor = None;
    loop {
        if *shutdown.borrow() {
            return;
        }
        match database.jobs().owned_failed_output_candidates(cursor).await {
            Ok(attempts) => {
                cursor = attempts.last().copied();
                for attempt in attempts {
                    if *shutdown.borrow() {
                        return;
                    }
                    if let Err(error) = database
                        .jobs()
                        .retire_owned_failed_outputs(attempt, &paths)
                        .await
                    {
                        tracing::warn!(%error,%attempt,"owned failed output retirement deferred");
                    }
                }
            }
            Err(error) => tracing::warn!(%error,"cannot discover owned failed outputs"),
        }
        tokio::select! { _ = time::sleep(Duration::from_secs(5)) => {}, _ = shutdown.changed() => return }
    }
}

async fn run_repair_worktree_cleanup(
    database: Database,
    paths: RuntimePaths,
    mut shutdown: watch::Receiver<bool>,
) {
    let mut cursor = None;
    loop {
        if *shutdown.borrow() {
            return;
        }
        match database.repair_worktrees().cleanup_candidates(cursor).await {
            Ok(recoveries) => {
                cursor = recoveries.last().copied();
                for recovery_id in recoveries {
                    if *shutdown.borrow() {
                        return;
                    }
                    if let Err(error) = database
                        .repair_worktrees()
                        .cleanup_repair_worktree(recovery_id, &paths)
                        .await
                    {
                        tracing::warn!(%error, %recovery_id, "repair worktree cleanup deferred");
                    }
                }
            }
            Err(error) => tracing::warn!(%error, "cannot find repair worktree cleanup candidates"),
        }
        tokio::select! { _ = time::sleep(Duration::from_secs(5)) => {}, _ = shutdown.changed() => return }
    }
}

async fn run_superseded_cleanup(
    database: Database,
    paths: RuntimePaths,
    mut shutdown: watch::Receiver<bool>,
) {
    let mut cursor = None;
    loop {
        if *shutdown.borrow() {
            return;
        }
        match database
            .artifacts()
            .superseded_cleanup_candidates(cursor)
            .await
        {
            Ok(attempts) => {
                cursor = attempts.last().copied();
                for attempt_id in attempts {
                    if *shutdown.borrow() {
                        return;
                    }
                    if let Err(error) = database
                        .artifacts()
                        .cleanup_superseded_artifacts(attempt_id, &paths)
                        .await
                    {
                        tracing::warn!(%error, %attempt_id, "superseded artifact cleanup deferred");
                    }
                }
            }
            Err(error) => {
                tracing::warn!(%error, "cannot find superseded artifact cleanup candidates")
            }
        }
        tokio::select! { _ = time::sleep(Duration::from_secs(5)) => {}, _ = shutdown.changed() => return }
    }
}

async fn run_failed_cleanup(
    database: Database,
    paths: RuntimePaths,
    mut shutdown: watch::Receiver<bool>,
) {
    let mut cursor = None;
    loop {
        if *shutdown.borrow() {
            return;
        }
        match database.artifacts().failed_cleanup_candidates(cursor).await {
            Ok(attempts) => {
                // Advance past deferred attempts so a bad path cannot starve later batches.
                cursor = attempts.last().copied();
                for attempt_id in attempts {
                    if *shutdown.borrow() {
                        return;
                    }
                    if let Err(error) = database.actions().enqueue_failed_cleanup(attempt_id).await
                    {
                        tracing::warn!(%error, %attempt_id, "cannot enqueue failed artifact cleanup");
                    }
                }
            }
            Err(error) => tracing::warn!(%error, "cannot find failed artifact cleanup candidates"),
        }
        let owner = format!("failed-cleanup:{}", std::process::id());
        match database.actions().claim_failed_cleanup(&owner, LEASE).await {
            Ok(Some(claim)) => {
                let artifacts = database.artifacts();
                let work = artifacts.cleanup_failed_artifacts_claimed(&claim, &paths);
                tokio::pin!(work);
                let result = loop {
                    tokio::select! {
                        result = &mut work => break result,
                        _ = time::sleep(LEASE / 3) => {
                            // A blocking worker owns the writer during I/O. A busy
                            // heartbeat cannot permit a competing claim to take that fence;
                            // an expired ack is recovered by durable intent replay.
                            if let Err(error) = database.actions().heartbeat(&claim, LEASE).await {
                                tracing::warn!(%error, "failed cleanup heartbeat deferred");
                            }
                        }
                    }
                };
                let ack = match result {
                    Ok(igor_core::persistence::FailedCleanupOutcome::Completed) => {
                        database.actions().succeed(&claim).await
                    }
                    Ok(
                        igor_core::persistence::FailedCleanupOutcome::Deferred
                        | igor_core::persistence::FailedCleanupOutcome::MoreWork,
                    ) => {
                        database
                            .actions()
                            .postpone_failed_cleanup(&claim, Duration::from_secs(5))
                            .await
                    }
                    Err(error) => {
                        database
                            .actions()
                            .retry(&claim, &ActionRetryPolicy::default().0, &error.to_string())
                            .await
                    }
                };
                if let Err(error) = ack {
                    tracing::warn!(%error, "cannot acknowledge failed artifact cleanup");
                }
            }
            Ok(None) => {}
            Err(error) => tracing::warn!(%error, "cannot claim failed artifact cleanup"),
        }
        tokio::select! { _ = time::sleep(Duration::from_secs(5)) => {}, _ = shutdown.changed() => return }
    }
}

async fn run_notifications(
    database: Database,
    paths: RuntimePaths,
    shutdown: &mut watch::Receiver<bool>,
) {
    let owner = format!("supervisor:{}", std::process::id());
    let telegram = if paths.config_file.is_file() {
        match igor_core::load_global_config(&paths.config_file) {
            Ok(config) => TelegramClient::new(&config.telegram),
            Err(_) => {
                tracing::error!("cannot load supervisor configuration; check private user config");
                None
            }
        }
    } else {
        None
    };
    loop {
        if *shutdown.borrow() {
            return;
        }
        match database.actions().claim_notification(&owner, LEASE).await {
            Ok(Some(claim)) => {
                let work = database.actions().claimed(&claim).await;
                match work {
                    Ok(Some(work)) if work.action.kind == "send_notification" => {
                        let event_id = work
                            .action
                            .spec
                            .get("event_id")
                            .and_then(serde_json::Value::as_str)
                            .and_then(|value| value.parse::<EventId>().ok());
                        if let Some(event_id) = event_id {
                            let delivery = DeliveryRecord {
                                id: DeliveryId::new(),
                                project_id: work.action.project_id,
                                channel: "telegram".into(),
                                state: DeliveryState::Pending,
                                payload: work.action.spec,
                                idempotency_key: format!("telegram:action:{}", claim.record_id),
                            };
                            if let Err(error) = database
                                .actions()
                                .enqueue_notification(&claim, &delivery, event_id)
                                .await
                            {
                                tracing::error!(%error, "cannot enqueue notification atomically");
                            }
                        } else if let Err(error) = database
                            .actions()
                            .retry(
                                &claim,
                                &ActionRetryPolicy::default().0,
                                "invalid_notification_event",
                            )
                            .await
                        {
                            tracing::error!(%error, "cannot record invalid notification action");
                        }
                    }
                    Ok(Some(_)) => tracing::error!("unexpected non-notification action claim"),
                    Ok(None) => {}
                    Err(error) => tracing::error!(%error, "cannot load supervisor action"),
                }
            }
            Ok(None) => {}
            Err(error) => tracing::error!(%error, "cannot claim supervisor action"),
        }
        if let Some(telegram) = &telegram {
            match database.deliveries().claim_telegram(&owner, LEASE).await {
                Ok(Some(claim)) => match database.deliveries().claimed(&claim).await {
                    Ok(Some(work)) if work.delivery.channel == "telegram" => {
                        let attempt = work
                            .delivery
                            .payload
                            .get("attempt_id")
                            .and_then(serde_json::Value::as_str)
                            .and_then(|id| id.parse::<igor_core::AttemptId>().ok());
                        let metrics = match attempt {
                            Some(attempt) => database
                                .deliveries()
                                .available_metrics(attempt)
                                .await
                                .unwrap_or_default(),
                            None => Vec::new(),
                        };
                        let text =
                            format_notification_with_metrics(&work.delivery.payload, &metrics);
                        let sending = telegram.send(&text);
                        tokio::pin!(sending);
                        let result = tokio::select! {
                            result = &mut sending => result,
                            _ = shutdown.changed() => return,
                        };
                        let finished = match result {
                            SendResult::Delivered => database.deliveries().delivered(&claim).await,
                            SendResult::Retry { reason, after } => {
                                database
                                    .deliveries()
                                    .retry(&claim, &DeliveryRetryPolicy::default().0, reason, after)
                                    .await
                            }
                        };
                        if let Err(error) = finished {
                            tracing::error!(%error, "cannot persist Telegram delivery state");
                        }
                    }
                    Ok(Some(_)) => tracing::error!("unexpected non-Telegram delivery claim"),
                    Ok(None) => {}
                    Err(error) => tracing::error!(%error, "cannot load supervisor delivery"),
                },
                Ok(None) => {}
                Err(error) => tracing::error!(%error, "cannot claim pending delivery"),
            }
        }
        tokio::select! { _ = time::sleep(POLL) => {}, _ = shutdown.changed() => return }
    }
}
