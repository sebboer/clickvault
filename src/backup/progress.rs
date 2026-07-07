use std::time::Duration;

use chrono::{DateTime, Utc};
use clickhouse::Client;
use serde::Deserialize;
use tracing::{info, warn};

use crate::error::ClickVaultError;
use crate::retry::{self, RetryPolicy};

/// Column list shared by every SELECT that reads system.backups into a
/// [`BackupStatus`]. Order must match the struct's field order (RowBinary
/// is positional).
///
/// The `start_ts`/`end_ts` expressions use table-qualified column names:
/// `toString(start_time) AS start_time` shadows the column with a String
/// alias, and a bare `toUnixTimestamp(start_time)` would resolve to that
/// alias and fail to parse.
pub const BACKUP_STATUS_COLUMNS: &str = "id, toString(status) AS status, \
     toString(start_time) AS start_time, toString(end_time) AS end_time, \
     toUnixTimestamp(system.backups.start_time) AS start_ts, \
     toUnixTimestamp(system.backups.end_time) AS end_ts, \
     total_size, ifNull(error, '') AS error";

/// Whether a ClickHouse error is a transport-level blip worth retrying.
/// Everything else (server exceptions, parse errors, RowNotFound) is a
/// definitive answer.
pub(crate) fn is_transient_clickhouse(e: &clickhouse::error::Error) -> bool {
    matches!(
        e,
        clickhouse::error::Error::Network(_) | clickhouse::error::Error::TimedOut
    )
}

#[derive(Debug, Clone, Deserialize, clickhouse::Row)]
pub struct BackupStatus {
    pub id: String,
    pub status: String,
    pub start_time: String,
    pub end_time: String,
    start_ts: u32,
    end_ts: u32,
    pub total_size: u64,
    pub error: String,
}

impl BackupStatus {
    /// Actual start of the backup as recorded by ClickHouse (UTC).
    pub fn started_at(&self) -> Option<DateTime<Utc>> {
        epoch_secs(self.start_ts)
    }

    /// Actual end of the backup as recorded by ClickHouse (UTC); `None`
    /// while the backup is still running.
    pub fn finished_at(&self) -> Option<DateTime<Utc>> {
        epoch_secs(self.end_ts)
    }
}

fn epoch_secs(secs: u32) -> Option<DateTime<Utc>> {
    (secs != 0)
        .then(|| DateTime::from_timestamp(secs as i64, 0))
        .flatten()
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
    retry: &RetryPolicy,
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

        let Some(status) = get_backup_status(client, backup_id, retry).await? else {
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
    retry: &RetryPolicy,
) -> Result<Option<BackupStatus>, ClickVaultError> {
    let sql = format!("SELECT {BACKUP_STATUS_COLUMNS} FROM system.backups WHERE id = ?");
    let status = retry::with_retry(
        retry,
        "system.backups poll",
        is_transient_clickhouse,
        || {
            client
                .query(&sql)
                .bind(backup_id)
                .fetch_optional::<BackupStatus>()
        },
    )
    .await?;

    Ok(status)
}

/// Gets recent backup statuses for the `status` subcommand.
pub async fn get_recent_backups(
    client: &Client,
    limit: u32,
    retry: &RetryPolicy,
) -> Result<Vec<BackupStatus>, ClickVaultError> {
    let sql = format!(
        "SELECT {BACKUP_STATUS_COLUMNS} FROM system.backups \
         ORDER BY start_time DESC LIMIT ?"
    );
    let statuses = retry::with_retry(
        retry,
        "system.backups status",
        is_transient_clickhouse,
        || client.query(&sql).bind(limit).fetch_all::<BackupStatus>(),
    )
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
            start_ts: 0,
            end_ts: 0,
            total_size: 0,
            error: error.into(),
        }
    }

    #[test]
    fn backup_window_accessors_treat_epoch_zero_as_unset() {
        let mut s = status("BACKUP_CREATED", "");
        assert_eq!(s.started_at(), None);
        assert_eq!(s.finished_at(), None);

        s.start_ts = 1_760_000_000;
        s.end_ts = 1_760_000_120;
        assert_eq!(
            s.started_at().unwrap(),
            DateTime::from_timestamp(1_760_000_000, 0).unwrap()
        );
        assert_eq!(
            (s.finished_at().unwrap() - s.started_at().unwrap()).num_seconds(),
            120
        );
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
