use std::time::{Duration, Instant};

use chrono::Utc;
use clickhouse::Client;
use s3::Bucket;
use tracing::{error, info};

use super::discovery;
use super::progress;
use super::{BackupChain, BackupKind, BackupMetadata, METADATA_SCHEMA_VERSION};
use crate::config::Config;
use crate::error::ClickVaultError;
use crate::retry::{self, RetryPolicy};
use crate::s3 as s3_helpers;

/// Columns returned by a `BACKUP ... ASYNC` statement.
#[derive(clickhouse::Row, serde::Deserialize)]
struct BackupSubmit {
    id: String,
    // `status` is an Enum8 (Int8 on the wire); we must read it to consume the
    // row even though the initial status is always CREATING_BACKUP.
    #[allow(dead_code)]
    status: i8,
}

/// Quotes a ClickHouse identifier with backticks. ClickHouse processes
/// backslash escapes inside backticks (verified live: a trailing backslash
/// swallows the closing backtick and breaks the statement), so backslashes
/// must be escaped before backticks — mirroring `escape_sql_str` for string
/// literals.
fn quote_ident(ident: &str) -> String {
    format!("`{}`", ident.replace('\\', "\\\\").replace('`', "\\`"))
}

pub struct BackupResult {
    pub metadata: BackupMetadata,
    pub duration: Duration,
}

/// A failed backup run, carrying the kind the run decided on (if it got far
/// enough to decide) so failure notifications report what was actually
/// attempted rather than what the CLI flag implied.
#[derive(Debug, thiserror::Error)]
#[error("{source}")]
pub struct BackupRunError {
    pub kind: Option<BackupKind>,
    #[source]
    pub source: ClickVaultError,
}

/// Decides whether this run performs a full or an incremental backup. A run
/// is promoted to full when forced via `--full` or when the configured
/// interval since the last full backup has elapsed.
fn decide_kind(
    force_full: bool,
    latest_full: Option<&BackupMetadata>,
    interval_days: u32,
) -> BackupKind {
    if force_full || discovery::should_do_full_backup(latest_full, interval_days) {
        BackupKind::Full
    } else {
        BackupKind::Incremental
    }
}

pub async fn run_backup(
    client: &Client,
    bucket: &Bucket,
    config: &Config,
    force_full: bool,
    skip_in_progress_check: bool,
) -> Result<BackupResult, BackupRunError> {
    let undecided = |source| BackupRunError { kind: None, source };
    let retry = config.retry.policy();

    if !skip_in_progress_check {
        check_no_backup_in_progress(client, &retry)
            .await
            .map_err(undecided)?;
    }

    // Discover existing backups to decide full vs incremental
    let chains = discovery::discover_chains(bucket, &config.s3.prefix, &retry)
        .await
        .map_err(undecided)?;
    let latest_full = chains.first().map(|c| &c.full);

    let kind = decide_kind(
        force_full,
        latest_full,
        config.schedule.full_backup_interval_days,
    );

    execute_backup(client, bucket, config, kind, &chains)
        .await
        .map_err(|source| BackupRunError {
            kind: Some(kind),
            source,
        })
}

async fn execute_backup(
    client: &Client,
    bucket: &Bucket,
    config: &Config,
    kind: BackupKind,
    chains: &[BackupChain],
) -> Result<BackupResult, ClickVaultError> {
    let prefix = &config.s3.prefix;
    let db = quote_ident(&config.clickhouse.database);
    let retry = config.retry.policy();

    let now = Utc::now();
    let start = Instant::now();

    // Deep chaining: incrementals base on the latest backup (full or
    // incremental) of the newest chain. Computed once so the SQL and the
    // metadata sidecar can never disagree about the base.
    let base_path = if kind == BackupKind::Incremental {
        Some(
            chains
                .first()
                .map(|chain| chain.latest().0.to_string())
                .ok_or(ClickVaultError::NoBaseBackup)?,
        )
    } else {
        None
    };

    let (backup_path, sql) = match &base_path {
        None => {
            let path = s3_helpers::full_backup_path(prefix, &now);
            let dest = s3_helpers::s3_sql_fragment(&config.s3, &path);
            let sql = format!("BACKUP DATABASE {db} TO {dest} ASYNC");
            (path, sql)
        }
        Some(latest) => {
            let path = s3_helpers::incremental_backup_path(prefix, &now);
            let dest = s3_helpers::s3_sql_fragment(&config.s3, &path);
            let base = s3_helpers::s3_sql_fragment(&config.s3, latest);
            let sql = format!("BACKUP DATABASE {db} TO {dest} SETTINGS base_backup = {base} ASYNC");
            (path, sql)
        }
    };

    info!(kind = %kind, path = %backup_path, "Starting backup");

    // Execute the BACKUP command. `ASYNC` returns the backup id (and initial
    // status) directly, so we read it from the result rather than guessing the
    // most recent row in system.backups (which races with concurrent activity).
    // Never retried: submitting BACKUP is not idempotent -- a retry after an
    // ambiguous failure could start a second backup.
    let submitted = client.query(&sql).fetch_one::<BackupSubmit>().await?;
    let backup_id = submitted.id;

    if backup_id.is_empty() {
        return Err(ClickVaultError::BackupFailed {
            status: "UNKNOWN".into(),
            message: "BACKUP was submitted but returned an empty id".into(),
        });
    }

    info!(backup_id = %backup_id, "Backup started, polling for progress");

    // Poll until complete. If the backup's row vanished from system.backups
    // (server restart), the backup is no longer running and its data — which
    // has no metadata sidecar yet — would be a permanently invisible orphan,
    // so it is rolled back like a metadata-write failure. On TIMEOUT the
    // backup may still be running server-side, so its data is left alone.
    let status = match progress::poll_until_complete(
        client,
        &backup_id,
        config.backup.poll_interval(),
        config.backup.timeout(),
        &retry,
    )
    .await
    {
        Ok(status) => status,
        Err(e @ ClickVaultError::BackupStateLost { .. }) => {
            error!(
                backup_id = %backup_id,
                error = %e,
                "Backup state lost; rolling back orphaned backup data"
            );
            rollback_orphaned_backup(bucket, &backup_path, &retry).await;
            return Err(e);
        }
        Err(e) => return Err(e),
    };

    let duration = start.elapsed();

    let metadata = BackupMetadata {
        version: METADATA_SCHEMA_VERSION,
        backup_id: backup_id.clone(),
        kind,
        timestamp: now,
        base_backup_path: base_path,
        started_at: status.started_at(),
        finished_at: status.finished_at(),
        status: status.status,
        total_size: status.total_size,
        database: config.clickhouse.database.clone(),
    };

    // Persist metadata (retried internally on transient S3 errors). Without
    // the sidecar the backup is invisible to discovery (an orphan that can
    // never be listed, cleaned up, or chained off), so on definitive failure
    // we roll back the orphaned data.
    if let Err(e) = s3_helpers::write_metadata(bucket, &backup_path, &metadata, &retry).await {
        error!(
            path = %backup_path,
            error = %e,
            "Failed to write backup metadata after retries; rolling back orphaned backup data"
        );
        rollback_orphaned_backup(bucket, &backup_path, &retry).await;
        return Err(e);
    }

    info!(
        backup_id = %backup_id,
        kind = %metadata.kind,
        size = metadata.total_size,
        duration_secs = duration.as_secs(),
        "Backup completed and metadata written"
    );

    Ok(BackupResult { metadata, duration })
}

/// Deletes backup data that will never get a metadata sidecar: without one
/// the backup is invisible to discovery, so it could never be listed,
/// cleaned up, or chained off.
async fn rollback_orphaned_backup(bucket: &Bucket, backup_path: &str, retry: &RetryPolicy) {
    match s3_helpers::delete_prefix(bucket, backup_path, retry).await {
        Ok(outcome) if outcome.is_complete() => {
            info!(path = %backup_path, objects = outcome.deleted, "Rolled back orphaned backup data")
        }
        Ok(outcome) => error!(
            path = %backup_path,
            deleted = outcome.deleted,
            failed = outcome.failed,
            "Partially rolled back orphaned backup data; manual cleanup required"
        ),
        Err(ce) => error!(
            path = %backup_path,
            error = %ce,
            "Failed to roll back orphaned backup data; manual cleanup required"
        ),
    }
}

async fn check_no_backup_in_progress(
    client: &Client,
    retry: &RetryPolicy,
) -> Result<(), ClickVaultError> {
    let sql = format!(
        "SELECT {} FROM system.backups WHERE status = 'CREATING_BACKUP'",
        progress::BACKUP_STATUS_COLUMNS
    );
    let in_progress: Vec<progress::BackupStatus> = retry::with_retry(
        retry,
        "in-progress check",
        progress::is_transient_clickhouse,
        || client.query(&sql).fetch_all(),
    )
    .await?;

    if let Some(bp) = in_progress.first() {
        return Err(ClickVaultError::BackupInProgress(bp.id.clone()));
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Duration as ChronoDuration;

    fn full_backup(age_days: i64) -> BackupMetadata {
        BackupMetadata {
            version: METADATA_SCHEMA_VERSION,
            backup_id: "id".into(),
            kind: BackupKind::Full,
            timestamp: Utc::now() - ChronoDuration::days(age_days),
            base_backup_path: None,
            status: "BACKUP_CREATED".into(),
            total_size: 0,
            database: "db".into(),
            started_at: None,
            finished_at: None,
        }
    }

    #[test]
    fn quote_ident_escapes_backticks_and_backslashes() {
        assert_eq!(quote_ident("mydb"), "`mydb`");
        assert_eq!(quote_ident("my`db"), r"`my\`db`");
        assert_eq!(quote_ident(r"my\db"), r"`my\\db`");
        // The dangerous case: a trailing backslash must not swallow the
        // closing backtick.
        assert_eq!(quote_ident(r"db\"), r"`db\\`");
        assert_eq!(quote_ident(r"a\`b"), r"`a\\\`b`");
    }

    #[test]
    fn decide_kind_full_when_forced_or_no_backups() {
        assert_eq!(
            decide_kind(true, Some(&full_backup(1)), 7),
            BackupKind::Full
        );
        assert_eq!(decide_kind(false, None, 7), BackupKind::Full);
    }

    #[test]
    fn decide_kind_incremental_within_interval() {
        assert_eq!(
            decide_kind(false, Some(&full_backup(1)), 7),
            BackupKind::Incremental
        );
    }

    #[test]
    fn decide_kind_promotes_to_full_after_interval() {
        // The scenario behind the notification-kind bug: no --full flag, but
        // the elapsed interval promotes the run to a full backup.
        assert_eq!(
            decide_kind(false, Some(&full_backup(8)), 7),
            BackupKind::Full
        );
    }

    #[test]
    fn backup_run_error_displays_as_its_source() {
        let err = BackupRunError {
            kind: Some(BackupKind::Full),
            source: ClickVaultError::NoBaseBackup,
        };
        assert_eq!(err.to_string(), ClickVaultError::NoBaseBackup.to_string());
    }
}
