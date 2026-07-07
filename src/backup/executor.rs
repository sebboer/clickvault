use std::time::{Duration, Instant};

use chrono::Utc;
use clickhouse::Client;
use s3::Bucket;
use tracing::{error, info, warn};

use super::discovery;
use super::progress;
use super::{BackupKind, BackupMetadata};
use crate::config::Config;
use crate::error::ClickVaultError;
use crate::s3 as s3_helpers;

const POLL_INTERVAL: Duration = Duration::from_secs(5);
const BACKUP_TIMEOUT: Duration = Duration::from_secs(86400); // 24 hours
const METADATA_WRITE_ATTEMPTS: usize = 3;
const METADATA_WRITE_RETRY_DELAY: Duration = Duration::from_secs(2);

/// Columns returned by a `BACKUP ... ASYNC` statement.
#[derive(clickhouse::Row, serde::Deserialize)]
struct BackupSubmit {
    id: String,
    // `status` is an Enum8 (Int8 on the wire); we must read it to consume the
    // row even though the initial status is always CREATING_BACKUP.
    #[allow(dead_code)]
    status: i8,
}

/// Quotes a ClickHouse identifier with backticks, escaping any embedded backticks.
fn quote_ident(ident: &str) -> String {
    format!("`{}`", ident.replace('`', "``"))
}

pub struct BackupResult {
    pub metadata: BackupMetadata,
    pub duration: Duration,
}

pub async fn run_backup(
    client: &Client,
    bucket: &Bucket,
    config: &Config,
    force_full: bool,
    skip_in_progress_check: bool,
) -> Result<BackupResult, ClickVaultError> {
    if !skip_in_progress_check {
        check_no_backup_in_progress(client).await?;
    }

    let prefix = &config.s3.prefix;
    let db = quote_ident(&config.clickhouse.database);

    // Discover existing backups to decide full vs incremental
    let chains = discovery::discover_chains(bucket, prefix).await?;
    let latest_full = chains.first().map(|c| &c.full);

    let do_full = force_full
        || discovery::should_do_full_backup(latest_full, config.schedule.full_backup_interval_days);

    let now = Utc::now();
    let start = Instant::now();

    let (kind, backup_path, sql) = if do_full {
        let path = s3_helpers::full_backup_path(prefix, &now);
        let dest = s3_helpers::s3_sql_fragment(&config.s3, &path);
        let sql = format!("BACKUP DATABASE {db} TO {dest} ASYNC");
        (BackupKind::Full, path, sql)
    } else {
        // Deep chaining: base on the latest backup (full or incremental)
        let latest = chains
            .first()
            .map(|chain| {
                let (path, _meta) = chain.latest();
                path.to_string()
            })
            .ok_or(ClickVaultError::NoBaseBackup)?;

        let path = s3_helpers::incremental_backup_path(prefix, &now);
        let dest = s3_helpers::s3_sql_fragment(&config.s3, &path);
        let base = s3_helpers::s3_sql_fragment(&config.s3, &latest);
        let sql = format!("BACKUP DATABASE {db} TO {dest} SETTINGS base_backup = {base} ASYNC");
        (BackupKind::Incremental, path, sql)
    };

    info!(kind = %kind, path = %backup_path, "Starting backup");

    // Execute the BACKUP command. `ASYNC` returns the backup id (and initial
    // status) directly, so we read it from the result rather than guessing the
    // most recent row in system.backups (which races with concurrent activity).
    let submitted = client.query(&sql).fetch_one::<BackupSubmit>().await?;
    let backup_id = submitted.id;

    if backup_id.is_empty() {
        return Err(ClickVaultError::BackupFailed {
            status: "UNKNOWN".into(),
            message: "BACKUP was submitted but returned an empty id".into(),
        });
    }

    info!(backup_id = %backup_id, "Backup started, polling for progress");

    // Poll until complete
    let status =
        progress::poll_until_complete(client, &backup_id, POLL_INTERVAL, BACKUP_TIMEOUT).await?;

    let duration = start.elapsed();

    // Write metadata to S3
    let base_path = if kind == BackupKind::Incremental {
        chains.first().map(|chain| {
            let (path, _) = chain.latest();
            path.to_string()
        })
    } else {
        None
    };

    let metadata = BackupMetadata {
        backup_id: backup_id.clone(),
        kind,
        timestamp: now,
        base_backup_path: base_path,
        status: status.status,
        total_size: status.total_size,
        database: config.clickhouse.database.clone(),
    };

    // Persist metadata. Without the sidecar the backup is invisible to
    // discovery (an orphan that can never be listed, cleaned up, or chained
    // off), so on definitive failure we roll back the orphaned data.
    if let Err(e) = write_metadata_with_retry(bucket, &backup_path, &metadata).await {
        error!(
            path = %backup_path,
            error = %e,
            "Failed to write backup metadata after retries; rolling back orphaned backup data"
        );
        match s3_helpers::delete_prefix(bucket, &backup_path).await {
            Ok(n) => info!(path = %backup_path, objects = n, "Rolled back orphaned backup data"),
            Err(ce) => error!(
                path = %backup_path,
                error = %ce,
                "Failed to roll back orphaned backup data; manual cleanup required"
            ),
        }
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

async fn check_no_backup_in_progress(client: &Client) -> Result<(), ClickVaultError> {
    let in_progress: Vec<progress::BackupStatus> = client
        .query(
            "SELECT id, toString(status) as status, toString(start_time) as start_time, \
             toString(end_time) as end_time, total_size, \
             ifNull(error, '') as error \
             FROM system.backups WHERE status = 'CREATING_BACKUP'",
        )
        .fetch_all()
        .await?;

    if let Some(bp) = in_progress.first() {
        return Err(ClickVaultError::BackupInProgress(bp.id.clone()));
    }

    Ok(())
}

/// Writes the metadata sidecar, retrying a few times on transient S3 errors.
async fn write_metadata_with_retry(
    bucket: &Bucket,
    backup_path: &str,
    metadata: &BackupMetadata,
) -> Result<(), ClickVaultError> {
    let mut last_err = None;

    for attempt in 1..=METADATA_WRITE_ATTEMPTS {
        match s3_helpers::write_metadata(bucket, backup_path, metadata).await {
            Ok(()) => return Ok(()),
            Err(e) => {
                warn!(
                    attempt,
                    max_attempts = METADATA_WRITE_ATTEMPTS,
                    error = %e,
                    "Failed to write backup metadata"
                );
                last_err = Some(e);
                if attempt < METADATA_WRITE_ATTEMPTS {
                    tokio::time::sleep(METADATA_WRITE_RETRY_DELAY).await;
                }
            }
        }
    }

    Err(last_err.expect("at least one attempt was made"))
}
