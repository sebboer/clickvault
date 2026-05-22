use std::time::{Duration, Instant};

use chrono::Utc;
use clickhouse::Client;
use s3::Bucket;
use tracing::{info, warn};

use super::discovery;
use super::progress;
use super::{BackupKind, BackupMetadata};
use crate::config::Config;
use crate::error::ClickVaultError;
use crate::s3 as s3_helpers;

const POLL_INTERVAL: Duration = Duration::from_secs(5);
const BACKUP_TIMEOUT: Duration = Duration::from_secs(86400); // 24 hours

pub struct BackupResult {
    pub metadata: BackupMetadata,
    pub duration: Duration,
}

pub async fn run_backup(
    client: &Client,
    bucket: &Bucket,
    config: &Config,
    force_full: bool,
) -> Result<BackupResult, ClickVaultError> {
    // Check if a backup is already running
    check_no_backup_in_progress(client).await?;

    let prefix = &config.s3.prefix;

    // Discover existing backups to decide full vs incremental
    let chains = discovery::discover_chains(bucket, prefix).await?;
    let latest_full = chains.first().map(|c| &c.full);

    let do_full =
        force_full || discovery::should_do_full_backup(latest_full, config.schedule.full_backup_interval_days);

    let now = Utc::now();
    let start = Instant::now();

    let (kind, backup_path, sql) = if do_full {
        let path = s3_helpers::full_backup_path(prefix, &now);
        let dest = s3_helpers::s3_sql_fragment(&config.s3, &path);
        let sql = format!(
            "BACKUP DATABASE {} TO {} ASYNC",
            config.clickhouse.database, dest
        );
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
        let sql = format!(
            "BACKUP DATABASE {} TO {} SETTINGS base_backup = {} ASYNC",
            config.clickhouse.database, dest, base
        );
        (BackupKind::Incremental, path, sql)
    };

    info!(kind = %kind, path = %backup_path, "Starting backup");

    // Execute the BACKUP command
    client.query(&sql).execute().await?;

    // Find the backup ID from system.backups
    let backup_id = find_latest_backup_id(client).await?;
    info!(backup_id = %backup_id, "Backup started, polling for progress");

    // Poll until complete
    let status = progress::poll_until_complete(client, &backup_id, POLL_INTERVAL, BACKUP_TIMEOUT).await?;

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

    s3_helpers::write_metadata(bucket, &backup_path, &metadata).await?;

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
            "SELECT id, status, toString(start_time) as start_time, \
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

async fn find_latest_backup_id(client: &Client) -> Result<String, ClickVaultError> {
    #[derive(clickhouse::Row, serde::Deserialize)]
    struct IdRow {
        id: String,
    }

    let row = client
        .query("SELECT id FROM system.backups ORDER BY start_time DESC LIMIT 1")
        .fetch_one::<IdRow>()
        .await?;

    if row.id.is_empty() {
        warn!("Could not find backup ID in system.backups");
        return Err(ClickVaultError::BackupFailed {
            status: "UNKNOWN".into(),
            message: "Backup was submitted but no entry found in system.backups".into(),
        });
    }

    Ok(row.id)
}
