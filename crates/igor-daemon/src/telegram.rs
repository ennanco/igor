use std::time::Duration;

use igor_core::TelegramConfig;
use serde::Deserialize;

const TIMEOUT: Duration = Duration::from_secs(10);

pub struct TelegramClient {
    client: reqwest::Client,
    endpoint: String,
    chat_id: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SendResult {
    Delivered,
    Retry {
        reason: &'static str,
        after: Option<Duration>,
    },
}

#[derive(Deserialize)]
struct TelegramResponse {
    ok: bool,
    parameters: Option<ResponseParameters>,
}

#[derive(Deserialize)]
struct ResponseParameters {
    retry_after: Option<u64>,
}

impl TelegramClient {
    pub fn new(config: &TelegramConfig) -> Option<Self> {
        Self::with_base_url(
            config,
            config
                .api_base
                .as_deref()
                .unwrap_or("https://api.telegram.org"),
        )
    }

    pub(crate) fn with_base_url(config: &TelegramConfig, base: &str) -> Option<Self> {
        let (token, chat_id) = config.credentials()?;
        let base_url = reqwest::Url::parse(base).ok()?;
        let loopback = matches!(
            base_url.host_str(),
            Some("127.0.0.1" | "localhost" | "[::1]")
        );
        if token.is_empty()
            || chat_id.is_empty()
            || !base_url.username().is_empty()
            || base_url.password().is_some()
            || base_url.query().is_some()
            || base_url.fragment().is_some()
            || !(base_url.scheme() == "https" || (base_url.scheme() == "http" && loopback))
        {
            return None;
        }
        let client = reqwest::Client::builder()
            .timeout(TIMEOUT)
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .ok()?;
        Some(Self {
            client,
            endpoint: format!("{}/bot{token}/sendMessage", base.trim_end_matches('/')),
            chat_id: chat_id.into(),
        })
    }

    pub async fn send(&self, text: &str) -> SendResult {
        let response = self
            .client
            .post(&self.endpoint)
            .json(&serde_json::json!({"chat_id": self.chat_id, "text": text}))
            .send()
            .await;
        let Ok(response) = response else {
            return SendResult::Retry {
                reason: "telegram_transport_unavailable",
                after: None,
            };
        };
        let status = response.status();
        let body = response.json::<TelegramResponse>().await;
        if status.is_success() && body.as_ref().is_ok_and(|body| body.ok) {
            return SendResult::Delivered;
        }
        let after = if status.as_u16() == 429 {
            body.ok()
                .and_then(|body| body.parameters?.retry_after)
                .map(Duration::from_secs)
        } else {
            None
        };
        SendResult::Retry {
            reason: if status.as_u16() == 429 {
                "telegram_rate_limited"
            } else if status.is_server_error() {
                "telegram_server_unavailable"
            } else {
                "telegram_delivery_rejected"
            },
            after,
        }
    }
}

pub fn format_notification(payload: &serde_json::Value) -> String {
    format_notification_with_metrics(payload, &[])
}

pub(crate) fn format_notification_with_metrics(
    payload: &serde_json::Value,
    metrics: &[(String, f64)],
) -> String {
    let name = payload
        .get("job_name")
        .and_then(serde_json::Value::as_str)
        .unwrap_or("Job");
    let status = payload
        .get("state")
        .and_then(serde_json::Value::as_str)
        .unwrap_or("unknown");
    let mut message = format!("{name}: {status}");
    if let Some(duration) = payload
        .get("duration_seconds")
        .and_then(serde_json::Value::as_f64)
    {
        message.push_str(&format!("\nDuration: {duration:.1}s"));
    }
    if let Some(code) = payload.get("exit_code").and_then(serde_json::Value::as_i64) {
        message.push_str(&format!("\nExit code: {code}"));
    }
    if let Some(signal) = payload
        .get("term_signal")
        .and_then(serde_json::Value::as_i64)
    {
        message.push_str(&format!("\nSignal: {signal}"));
    }
    if let Some(progress) = payload
        .get("family_progress")
        .and_then(serde_json::Value::as_str)
    {
        message.push_str(&format!("\nFamily: {progress}"));
    }
    for (name, value) in metrics {
        message.push_str(&format!("\n{name}: {value}"));
    }
    if let Some(job_id) = payload.get("job_id").and_then(serde_json::Value::as_str) {
        message.push_str(&format!("\nJob: {job_id}"));
    }
    message
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpListener,
    };

    fn config() -> TelegramConfig {
        serde_json::from_value(serde_json::json!({
            "bot_token": "123456:abcdefghijklmnopqrstuvwxyzABCDEFG",
            "chat_id": "123456"
        }))
        .unwrap_or_default()
    }

    async fn mock(
        response: &'static str,
    ) -> std::io::Result<(String, tokio::task::JoinHandle<std::io::Result<String>>)> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let url = format!("http://{}", listener.local_addr()?);
        let handle = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await?;
            let mut received = Vec::new();
            let mut buffer = [0; 1024];
            loop {
                let size = stream.read(&mut buffer).await?;
                if size == 0 {
                    break;
                }
                received.extend_from_slice(&buffer[..size]);
                if received.windows(4).any(|bytes| bytes == b"\r\n\r\n") {
                    break;
                }
            }
            stream.write_all(response.as_bytes()).await?;
            Ok(String::from_utf8_lossy(&received).into_owned())
        });
        Ok((url, handle))
    }

    #[tokio::test]
    async fn mocked_delivery_recognizes_success_and_bounded_rate_limit()
    -> Result<(), Box<dyn std::error::Error>> {
        let (url, handle) =
            mock("HTTP/1.1 200 OK\r\nContent-Length: 11\r\nConnection: close\r\n\r\n{\"ok\":true}")
                .await?;
        let client = TelegramClient::with_base_url(&config(), &url).ok_or("missing credentials")?;
        assert_eq!(
            client.send("research complete").await,
            SendResult::Delivered
        );
        assert!(
            handle
                .await??
                .contains("/bot123456:abcdefghijklmnopqrstuvwxyzABCDEFG/sendMessage")
        );
        let (url, handle) = mock("HTTP/1.1 429 Too Many Requests\r\nConnection: close\r\n\r\n{\"ok\":false,\"parameters\":{\"retry_after\":120000}}").await?;
        let client = TelegramClient::with_base_url(&config(), &url).ok_or("missing credentials")?;
        assert_eq!(
            client.send("retry").await,
            SendResult::Retry {
                reason: "telegram_rate_limited",
                after: Some(Duration::from_secs(120000))
            }
        );
        handle.await??;
        Ok(())
    }

    #[tokio::test]
    async fn network_timeout_does_not_expose_token() -> Result<(), Box<dyn std::error::Error>> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let url = format!("http://{}", listener.local_addr()?);
        let mut client =
            TelegramClient::with_base_url(&config(), &url).ok_or("missing credentials")?;
        client.client = reqwest::Client::builder()
            .timeout(Duration::from_millis(25))
            .build()?;
        let server = tokio::spawn(async move {
            let (_stream, _) = listener.accept().await?;
            tokio::time::sleep(Duration::from_millis(100)).await;
            Ok::<_, std::io::Error>(())
        });
        assert_eq!(
            client.send("timeout").await,
            SendResult::Retry {
                reason: "telegram_transport_unavailable",
                after: None
            }
        );
        server.await??;
        Ok(())
    }

    #[test]
    fn notification_presents_results_before_technical_identity() {
        let text = format_notification_with_metrics(
            &serde_json::json!({
                "job_name": "experiment", "state": "succeeded", "duration_seconds": 21.5,
                "family_progress": "3/5 finished", "job_id": "technical-id"
            }),
            &[("accuracy".into(), 0.91)],
        );
        assert!(text.contains("experiment: succeeded"));
        assert!(text.contains("Duration: 21.5s"));
        assert!(text.contains("Family: 3/5 finished"));
        assert!(text.contains("accuracy: 0.91"));
        assert!(
            text.find("accuracy").unwrap_or(usize::MAX) < text.find("technical-id").unwrap_or(0)
        );
    }
}
