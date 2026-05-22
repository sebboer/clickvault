pub mod slack;
pub mod webhook;

use std::time::Duration;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::Serialize;

use crate::backup::BackupKind;
use crate::config::{NotificationConfig, NotificationProvider};
use crate::error::ClickVaultError;

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

pub fn build_notifiers(config: &NotificationConfig) -> Vec<Box<dyn Notifier>> {
    let client = reqwest::Client::new();
    let mut notifiers: Vec<Box<dyn Notifier>> = Vec::new();

    for provider in &config.providers {
        match provider {
            NotificationProvider::Slack { webhook_url } => {
                notifiers.push(Box::new(slack::SlackNotifier::new(
                    webhook_url.clone(),
                    client.clone(),
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
                )));
            }
        }
    }

    notifiers
}

/// Dispatches a backup event to all configured notifiers, respecting on_success/on_failure config.
pub async fn dispatch(
    config: &NotificationConfig,
    notifiers: &[Box<dyn Notifier>],
    event: &BackupEvent,
) {
    let should_send = (event.is_success() && config.on_success)
        || (event.is_failure() && config.on_failure);

    if !should_send {
        return;
    }

    for notifier in notifiers {
        if let Err(e) = notifier.send(event).await {
            tracing::warn!(error = %e, "Failed to send notification");
        }
    }
}
