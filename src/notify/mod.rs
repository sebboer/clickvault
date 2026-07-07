pub mod slack;
pub mod webhook;

use std::time::Duration;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::Serialize;

use crate::backup::BackupKind;
use crate::config::{NotificationConfig, NotificationProvider};
use crate::error::ClickVaultError;
use crate::retry::{self, RetryPolicy};

#[derive(Debug, Clone, Serialize)]
#[serde(tag = "event", rename_all = "snake_case")]
pub enum BackupEvent {
    BackupCompleted {
        kind: BackupKind,
        timestamp: DateTime<Utc>,
        duration_secs: u64,
        total_size: u64,
        database: String,
    },
    BackupFailed {
        kind: BackupKind,
        timestamp: DateTime<Utc>,
        error: String,
        database: String,
    },
    CleanupCompleted {
        chains_deleted: usize,
        objects_deleted: u64,
    },
}

impl BackupEvent {
    pub fn is_success(&self) -> bool {
        matches!(
            self,
            BackupEvent::BackupCompleted { .. } | BackupEvent::CleanupCompleted { .. }
        )
    }

    pub fn is_failure(&self) -> bool {
        matches!(self, BackupEvent::BackupFailed { .. })
    }

    pub fn backup_completed(
        kind: BackupKind,
        duration: Duration,
        total_size: u64,
        database: String,
    ) -> Self {
        Self::BackupCompleted {
            kind,
            timestamp: Utc::now(),
            duration_secs: duration.as_secs(),
            total_size,
            database,
        }
    }

    pub fn backup_failed(kind: BackupKind, error: String, database: String) -> Self {
        Self::BackupFailed {
            kind,
            timestamp: Utc::now(),
            error,
            database,
        }
    }
}

#[async_trait]
pub trait Notifier: Send + Sync {
    async fn send(&self, event: &BackupEvent) -> Result<(), ClickVaultError>;
}

/// One notification send attempt, classified so the retry layer can tell
/// transient failures from definitive ones.
enum SendError {
    Transport(reqwest::Error),
    Status(reqwest::StatusCode, String),
    NotCloneable,
}

impl std::fmt::Display for SendError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SendError::Transport(e) => write!(f, "request failed: {e}"),
            SendError::Status(status, body) => write!(f, "returned {status}: {body}"),
            SendError::NotCloneable => write!(f, "request body cannot be cloned for retry"),
        }
    }
}

fn is_transient_send(e: &SendError) -> bool {
    match e {
        SendError::Transport(e) => e.is_connect() || e.is_timeout(),
        SendError::Status(status, _) => {
            matches!(status.as_u16(), 408 | 429) || status.is_server_error()
        }
        SendError::NotCloneable => false,
    }
}

/// Sends an HTTP request, retrying transient failures (connect/timeout
/// errors and 408/429/5xx responses) per the policy. Other non-2xx
/// responses fail immediately — a misconfigured endpoint won't heal.
pub(crate) async fn send_with_retry(
    policy: &RetryPolicy,
    request: reqwest::RequestBuilder,
    provider: &str,
) -> Result<(), ClickVaultError> {
    retry::with_retry(policy, provider, is_transient_send, || {
        let attempt = request.try_clone();
        async move {
            let request = attempt.ok_or(SendError::NotCloneable)?;
            let response = request.send().await.map_err(SendError::Transport)?;
            let status = response.status();
            if status.is_success() {
                Ok(())
            } else {
                let body = response.text().await.unwrap_or_default();
                Err(SendError::Status(status, body))
            }
        }
    })
    .await
    .map_err(|e| ClickVaultError::Notification(format!("{provider} {e}")))
}

pub fn build_notifiers(config: &NotificationConfig, retry: RetryPolicy) -> Vec<Box<dyn Notifier>> {
    let client = reqwest::Client::new();
    let mut notifiers: Vec<Box<dyn Notifier>> = Vec::new();

    for provider in &config.providers {
        match provider {
            NotificationProvider::Slack { webhook_url } => {
                notifiers.push(Box::new(slack::SlackNotifier::new(
                    webhook_url.clone(),
                    client.clone(),
                    retry.clone(),
                )));
            }
            NotificationProvider::Webhook {
                url,
                method,
                headers,
            } => {
                notifiers.push(Box::new(webhook::WebhookNotifier::new(
                    url.clone(),
                    method.clone(),
                    headers.clone(),
                    client.clone(),
                    retry.clone(),
                )));
            }
        }
    }

    notifiers
}

/// Whether an event should be sent given the on_success/on_failure config.
fn should_send(config: &NotificationConfig, event: &BackupEvent) -> bool {
    (event.is_success() && config.on_success) || (event.is_failure() && config.on_failure)
}

/// Dispatches a backup event to all configured notifiers, respecting on_success/on_failure config.
pub async fn dispatch(
    config: &NotificationConfig,
    notifiers: &[Box<dyn Notifier>],
    event: &BackupEvent,
) {
    if !should_send(config, event) {
        return;
    }

    for notifier in notifiers {
        if let Err(e) = notifier.send(event).await {
            tracing::warn!(error = %e, "Failed to send notification");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn completed() -> BackupEvent {
        BackupEvent::backup_completed(BackupKind::Full, Duration::from_secs(12), 100, "db".into())
    }

    fn failed() -> BackupEvent {
        BackupEvent::backup_failed(BackupKind::Incremental, "boom".into(), "db".into())
    }

    fn cfg(on_success: bool, on_failure: bool) -> NotificationConfig {
        NotificationConfig {
            on_success,
            on_failure,
            providers: vec![],
        }
    }

    #[test]
    fn success_and_failure_classification() {
        assert!(completed().is_success());
        assert!(!completed().is_failure());
        assert!(failed().is_failure());
        assert!(!failed().is_success());

        let cleanup = BackupEvent::CleanupCompleted {
            chains_deleted: 1,
            objects_deleted: 2,
        };
        assert!(cleanup.is_success());
    }

    #[test]
    fn should_send_respects_config() {
        assert!(should_send(&cfg(true, true), &completed()));
        assert!(!should_send(&cfg(false, true), &completed()));
        assert!(should_send(&cfg(true, true), &failed()));
        assert!(!should_send(&cfg(true, false), &failed()));
    }

    #[test]
    fn send_errors_classify_transient_vs_definitive() {
        let status = |code: u16| {
            SendError::Status(reqwest::StatusCode::from_u16(code).unwrap(), String::new())
        };
        assert!(is_transient_send(&status(500)));
        assert!(is_transient_send(&status(503)));
        assert!(is_transient_send(&status(429)));
        assert!(is_transient_send(&status(408)));
        assert!(!is_transient_send(&status(404)));
        assert!(!is_transient_send(&status(400)));
        assert!(!is_transient_send(&SendError::NotCloneable));
    }

    #[test]
    fn event_serializes_with_tag_and_kind() {
        let json = serde_json::to_value(completed()).unwrap();
        assert_eq!(json["event"], "backup_completed");
        assert_eq!(json["kind"], "full");
        assert_eq!(json["duration_secs"], 12);
        assert_eq!(json["total_size"], 100);

        let json = serde_json::to_value(failed()).unwrap();
        assert_eq!(json["event"], "backup_failed");
        assert_eq!(json["kind"], "incremental");
        assert_eq!(json["error"], "boom");
    }
}
