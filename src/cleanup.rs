use s3::Bucket;
use tracing::{info, warn};

use crate::backup::discovery;
use crate::config::Config;
use crate::error::ClickVaultError;
use crate::s3 as s3_helpers;

#[derive(Debug)]
pub struct CleanupReport {
    pub chains_deleted: usize,
    pub objects_deleted: u64,
}

pub async fn cleanup(
    bucket: &Bucket,
    config: &Config,
    dry_run: bool,
) -> Result<CleanupReport, ClickVaultError> {
    let chains = discovery::discover_chains(bucket, &config.s3.prefix).await?;
    let keep = config.retention.keep_full_backups as usize;

    if chains.len() <= keep {
        info!(
            total_chains = chains.len(),
            keep,
            "No backup chains to clean up"
        );
        return Ok(CleanupReport {
            chains_deleted: 0,
            objects_deleted: 0,
        });
    }

    // Chains are sorted newest-first. Keep the first `keep` chains, delete the rest.
    let to_delete = &chains[keep..];

    let mut total_chains_deleted = 0usize;
    let mut total_objects_deleted = 0u64;

    for chain in to_delete {
        info!(
            full_backup = %chain.full_path,
            incrementals = chain.incrementals.len(),
            timestamp = %chain.full.timestamp,
            "{}",
            if dry_run { "Would delete chain" } else { "Deleting chain" }
        );

        if dry_run {
            total_chains_deleted += 1;
            continue;
        }

        // Delete incrementals first (newest to oldest)
        for (incr_path, incr_meta) in chain.incrementals.iter().rev() {
            info!(path = %incr_path, timestamp = %incr_meta.timestamp, "Deleting incremental backup");
            match s3_helpers::delete_prefix(bucket, incr_path).await {
                Ok(count) => total_objects_deleted += count,
                Err(e) => warn!(path = %incr_path, error = %e, "Failed to delete incremental backup"),
            }
        }

        // Delete the full backup
        info!(path = %chain.full_path, timestamp = %chain.full.timestamp, "Deleting full backup");
        match s3_helpers::delete_prefix(bucket, &chain.full_path).await {
            Ok(count) => total_objects_deleted += count,
            Err(e) => warn!(path = %chain.full_path, error = %e, "Failed to delete full backup"),
        }

        total_chains_deleted += 1;
    }

    let report = CleanupReport {
        chains_deleted: total_chains_deleted,
        objects_deleted: total_objects_deleted,
    };

    info!(
        chains_deleted = report.chains_deleted,
        objects_deleted = report.objects_deleted,
        "Cleanup complete"
    );

    Ok(report)
}
