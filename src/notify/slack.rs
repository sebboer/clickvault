use async_trait::async_trait;
use serde_json::json;

use super::{BackupEvent, Notifier};
use crate::error::ClickVaultError;
use crate::retry::RetryPolicy;

pub struct SlackNotifier {
    webhook_url: String,
    client: reqwest::Client,
    retry: RetryPolicy,
}

impl SlackNotifier {
    pub fn new(webhook_url: String, client: reqwest::Client, retry: RetryPolicy) -> Self {
        Self {
            webhook_url,
            client,
            retry,
        }
    }

    fn format_message(&self, event: &BackupEvent) -> serde_json::Value {
        match event {
            BackupEvent::BackupCompleted {
                kind,
                timestamp,
                duration_secs,
                total_size,
                database,
            } => {
                let size_mb = *total_size as f64 / (1024.0 * 1024.0);
                json!({
                    "text": format!(
                        ":white_check_mark: *ClickVault backup completed*\n\
                         Database: `{database}`\n\
                         Type: {kind}\n\
                         Size: {size_mb:.1} MB\n\
                         Duration: {duration_secs}s\n\
                         Time: {timestamp}"
                    )
                })
            }
            BackupEvent::BackupFailed {
                kind,
                timestamp,
                error,
                database,
            } => {
                json!({
                    "text": format!(
                        ":x: *ClickVault backup failed*\n\
                         Database: `{database}`\n\
                         Type: {kind}\n\
                         Error: {error}\n\
                         Time: {timestamp}"
                    )
                })
            }
            BackupEvent::CleanupCompleted {
                chains_deleted,
                objects_deleted,
            } => {
                json!({
                    "text": format!(
                        ":broom: *ClickVault cleanup completed*\n\
                         Chains deleted: {chains_deleted}\n\
                         Objects deleted: {objects_deleted}"
                    )
                })
            }
        }
    }
}

#[async_trait]
impl Notifier for SlackNotifier {
    async fn send(&self, event: &BackupEvent) -> Result<(), ClickVaultError> {
        let payload = self.format_message(event);
        let request = self.client.post(&self.webhook_url).json(&payload);
        super::send_with_retry(&self.retry, request, "Slack webhook").await
    }
}
