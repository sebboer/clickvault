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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backup::BackupKind;
    use std::time::Duration;

    fn notifier() -> SlackNotifier {
        SlackNotifier::new(
            "https://hooks.slack.example/x".into(),
            reqwest::Client::new(),
            RetryPolicy::default(),
        )
    }

    fn text(value: &serde_json::Value) -> &str {
        value["text"].as_str().expect("text field")
    }

    #[test]
    fn formats_completed_with_size_in_mb() {
        let event = BackupEvent::backup_completed(
            BackupKind::Full,
            Duration::from_secs(42),
            5 * 1024 * 1024 + 512 * 1024, // 5.5 MB
            "mydb".into(),
        );
        let msg = notifier().format_message(&event);
        let text = text(&msg);
        assert!(text.contains("backup completed"));
        assert!(text.contains("Database: `mydb`"));
        assert!(text.contains("Type: full"));
        assert!(text.contains("Size: 5.5 MB"));
        assert!(text.contains("Duration: 42s"));
    }

    #[test]
    fn formats_failed_with_error() {
        let event = BackupEvent::backup_failed(
            BackupKind::Incremental,
            "disk exploded".into(),
            "mydb".into(),
        );
        let msg = notifier().format_message(&event);
        let text = text(&msg);
        assert!(text.contains("backup failed"));
        assert!(text.contains("Type: incremental"));
        assert!(text.contains("Error: disk exploded"));
    }

    #[test]
    fn formats_cleanup_summary() {
        let event = BackupEvent::CleanupCompleted {
            chains_deleted: 2,
            objects_deleted: 78,
        };
        let msg = notifier().format_message(&event);
        let text = text(&msg);
        assert!(text.contains("cleanup completed"));
        assert!(text.contains("Chains deleted: 2"));
        assert!(text.contains("Objects deleted: 78"));
    }
}
