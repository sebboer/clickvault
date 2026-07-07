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

/// How many consecutive polls may find no row in system.backups before the
/// backup's state is considered lost. system.backups is in-memory: its rows
/// vanish when the ClickHouse server restarts.
const MAX_MISSING_POLLS: u32 = 3;

/// How many consecutive polls may return an unrecognized status before
/// giving up, rather than spinning until the overall timeout.
const MAX_UNKNOWN_POLLS: u32 = 5;

/// What the poll loop should do after classifying an observed status.
#[derive(Debug, PartialEq)]
enum PollAction {
    InProgress,
    Completed,
    Failed { status: String, message: String },
    Unknown,
}

/// Pure classification of a system.backups status string, so the poll
/// decision logic can be tested without a live ClickHouse.
fn classify_status(status: &BackupStatus) -> PollAction {
    match status.status.as_str() {
        "CREATING_BACKUP" => PollAction::InProgress,
        "BACKUP_CREATED" => PollAction::Completed,
        "BACKUP_FAILED" | "BACKUP_CANCELLED" => PollAction::Failed {
            status: status.status.clone(),
            message: if status.error.is_empty() {
                format!("backup ended with status {}", status.status)
            } else {
                status.error.clone()
            },
        },
        _ => PollAction::Unknown,
    }
}

/// Polls system.backups until the given backup completes or fails.
pub async fn poll_until_complete(
    client: &Client,
    backup_id: &str,
    poll_interval: Duration,
    timeout: Duration,
) -> Result<BackupStatus, ClickVaultError> {
    let start = std::time::Instant::now();
    let mut missing_polls = 0u32;
    let mut unknown_polls = 0u32;

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

        let Some(status) = get_backup_status(client, backup_id).await? else {
            missing_polls += 1;
            warn!(
                backup_id,
                attempt = missing_polls,
                max_attempts = MAX_MISSING_POLLS,
                "Backup not found in system.backups"
            );
            if missing_polls >= MAX_MISSING_POLLS {
                return Err(ClickVaultError::BackupStateLost {
                    id: backup_id.to_string(),
                    polls: missing_polls,
                });
            }
            tokio::time::sleep(poll_interval).await;
            continue;
        };
        missing_polls = 0;

        match classify_status(&status) {
            PollAction::InProgress => {
                unknown_polls = 0;
                info!(
                    backup_id,
                    total_size = status.total_size,
                    "Backup in progress..."
                );
                tokio::time::sleep(poll_interval).await;
            }
            PollAction::Completed => {
                info!(
                    backup_id,
                    total_size = status.total_size,
                    "Backup completed successfully"
                );
                return Ok(status);
            }
            PollAction::Failed { status, message } => {
                return Err(ClickVaultError::BackupFailed { status, message });
            }
            PollAction::Unknown => {
                unknown_polls += 1;
                warn!(
                    backup_id,
                    status = %status.status,
                    attempt = unknown_polls,
                    max_attempts = MAX_UNKNOWN_POLLS,
                    "Unexpected backup status"
                );
                if unknown_polls >= MAX_UNKNOWN_POLLS {
                    return Err(ClickVaultError::BackupFailed {
                        status: status.status.clone(),
                        message: format!(
                            "backup did not reach a known state after {unknown_polls} \
                             consecutive polls with unrecognized status '{}'",
                            status.status
                        ),
                    });
                }
                tokio::time::sleep(poll_interval).await;
            }
        }
    }
}

async fn get_backup_status(
    client: &Client,
    backup_id: &str,
) -> Result<Option<BackupStatus>, ClickVaultError> {
    let status = client
        .query(
            "SELECT id, toString(status) as status, toString(start_time) as start_time, \
             toString(end_time) as end_time, total_size, \
             ifNull(error, '') as error \
             FROM system.backups WHERE id = ?",
        )
        .bind(backup_id)
        .fetch_optional::<BackupStatus>()
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
            "SELECT id, toString(status) as status, toString(start_time) as start_time, \
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

#[cfg(test)]
mod tests {
    use super::*;

    fn status(s: &str, error: &str) -> BackupStatus {
        BackupStatus {
            id: "id".into(),
            status: s.into(),
            start_time: String::new(),
            end_time: String::new(),
            total_size: 0,
            error: error.into(),
        }
    }

    #[test]
    fn classify_known_statuses() {
        assert_eq!(
            classify_status(&status("CREATING_BACKUP", "")),
            PollAction::InProgress
        );
        assert_eq!(
            classify_status(&status("BACKUP_CREATED", "")),
            PollAction::Completed
        );
    }

    #[test]
    fn classify_failed_carries_error_message() {
        assert_eq!(
            classify_status(&status("BACKUP_FAILED", "disk full")),
            PollAction::Failed {
                status: "BACKUP_FAILED".into(),
                message: "disk full".into(),
            }
        );
    }

    #[test]
    fn classify_cancelled_is_terminal_with_fallback_message() {
        assert_eq!(
            classify_status(&status("BACKUP_CANCELLED", "")),
            PollAction::Failed {
                status: "BACKUP_CANCELLED".into(),
                message: "backup ended with status BACKUP_CANCELLED".into(),
            }
        );
    }

    #[test]
    fn classify_unrecognized_status_is_unknown() {
        assert_eq!(
            classify_status(&status("RESTORING", "")),
            PollAction::Unknown
        );
        assert_eq!(classify_status(&status("", "")), PollAction::Unknown);
    }
}
