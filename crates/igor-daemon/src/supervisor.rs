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
