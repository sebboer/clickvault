use std::time::Duration;

use clickhouse::Client;
use serde::Deserialize;
use tracing::{info, warn};

use crate::error::ClickVaultError;

#[derive(Debug, Clone, Deserialize, clickhouse::Row)]
pub struct BackupStatus {
    pub id: String,
    pub status: String,
    pub start_time: String,
    pub end_time: String,
    pub total_size: u64,
    pub error: String,
}

/// Polls system.backups until the given backup completes or fails.
pub async fn poll_until_complete(
    client: &Client,
    backup_id: &str,
    poll_interval: Duration,
    timeout: Duration,
) -> Result<BackupStatus, ClickVaultError> {
    let start = std::time::Instant::now();

    loop {
        if start.elapsed() > timeout {
            return Err(ClickVaultError::BackupFailed {
                status: "TIMEOUT".into(),
                message: format!(
                    "Backup {backup_id} did not complete within {}s",
                    timeout.as_secs()
                ),
            });
        }

        let status = get_backup_status(client, backup_id).await?;

        match status.status.as_str() {
            "CREATING_BACKUP" => {
                info!(
                    backup_id,
                    total_size = status.total_size,
                    "Backup in progress..."
                );
                tokio::time::sleep(poll_interval).await;
            }
            "BACKUP_CREATED" => {
                info!(
                    backup_id,
                    total_size = status.total_size,
                    "Backup completed successfully"
                );
                return Ok(status);
            }
            "BACKUP_FAILED" => {
                return Err(ClickVaultError::BackupFailed {
                    status: status.status,
                    message: status.error,
                });
            }
            other => {
                warn!(backup_id, status = other, "Unexpected backup status");
                tokio::time::sleep(poll_interval).await;
            }
        }
    }
}

async fn get_backup_status(
    client: &Client,
    backup_id: &str,
) -> Result<BackupStatus, ClickVaultError> {
    let status = client
        .query(
            "SELECT id, status, toString(start_time) as start_time, \
             toString(end_time) as end_time, total_size, \
             ifNull(error, '') as error \
             FROM system.backups WHERE id = ?",
        )
        .bind(backup_id)
        .fetch_one::<BackupStatus>()
        .await?;

    Ok(status)
}

/// Gets recent backup statuses for the `status` subcommand.
pub async fn get_recent_backups(
    client: &Client,
    limit: u32,
) -> Result<Vec<BackupStatus>, ClickVaultError> {
    let statuses = client
        .query(
            "SELECT id, status, toString(start_time) as start_time, \
             toString(end_time) as end_time, total_size, \
             ifNull(error, '') as error \
             FROM system.backups \
             ORDER BY start_time DESC LIMIT ?",
        )
        .bind(limit)
        .fetch_all::<BackupStatus>()
        .await?;

    Ok(statuses)
}
