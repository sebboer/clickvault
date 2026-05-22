use async_trait::async_trait;
use serde_json::json;

use super::{BackupEvent, Notifier};
use crate::error::ClickVaultError;

pub struct SlackNotifier {
    webhook_url: String,
    client: reqwest::Client,
}

impl SlackNotifier {
    pub fn new(webhook_url: String, client: reqwest::Client) -> Self {
        Self {
            webhook_url,
            client,
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

        let response = self
            .client
            .post(&self.webhook_url)
            .json(&payload)
            .send()
            .await
            .map_err(|e| ClickVaultError::Notification(format!("Slack request failed: {e}")))?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            return Err(ClickVaultError::Notification(format!(
                "Slack webhook returned {status}: {body}"
            )));
        }

        Ok(())
    }
}
