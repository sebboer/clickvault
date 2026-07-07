use s3::Bucket;
use tracing::{info, warn};

use crate::backup::BackupChain;
use crate::backup::discovery;
use crate::config::Config;
use crate::error::ClickVaultError;
use crate::s3 as s3_helpers;

#[derive(Debug, Default)]
pub struct CleanupReport {
    pub chains_deleted: usize,
    /// Chains selected for deletion that could not be fully removed.
    pub chains_failed: usize,
    pub objects_deleted: u64,
    /// Objects that could not be deleted. When listing a prefix fails, the
    /// object count is unknown and counted as one failure.
    pub objects_failed: u64,
}

impl CleanupReport {
    pub fn has_failures(&self) -> bool {
        self.chains_failed > 0 || self.objects_failed > 0
    }
}

/// Selects which chains should be deleted, keeping the `keep` newest.
///
/// `chains` is expected to be sorted newest-first (as returned by
/// `discover_chains`). Pure function so the retention math can be tested.
fn select_chains_for_deletion(chains: &[BackupChain], keep: usize) -> &[BackupChain] {
    if chains.len() <= keep {
        &[]
    } else {
        &chains[keep..]
    }
}

pub async fn cleanup(
    bucket: &Bucket,
    config: &Config,
    dry_run: bool,
) -> Result<CleanupReport, ClickVaultError> {
    // Strict discovery: a backup whose metadata exists but cannot be read
    // would silently shift the retention window, so cleanup refuses to run
    // on an incomplete view.
    let chains = discovery::discover_chains_strict(bucket, &config.s3.prefix).await?;
    let keep = config.retention.keep_full_backups as usize;

    // Chains are sorted newest-first. Keep the first `keep` chains, delete the rest.
    let to_delete = select_chains_for_deletion(&chains, keep);

    if to_delete.is_empty() {
        info!(
            total_chains = chains.len(),
            keep, "No backup chains to clean up"
        );
        return Ok(CleanupReport::default());
    }

    let mut report = CleanupReport::default();

    for chain in to_delete {
        info!(
            full_backup = %chain.full_path,
            incrementals = chain.incrementals.len(),
            timestamp = %chain.full.timestamp,
            "{}",
            if dry_run { "Would delete chain" } else { "Deleting chain" }
        );

        if dry_run {
            report.chains_deleted += 1;
            continue;
        }

        // Delete incrementals first (newest to oldest). The full backup is
        // only removed once every incremental is gone, so an interrupted run
        // leaves an intact, discoverable chain that the next cleanup retries.
        let mut chain_failures = 0u64;
        for (incr_path, incr_meta) in chain.incrementals.iter().rev() {
            info!(path = %incr_path, timestamp = %incr_meta.timestamp, "Deleting incremental backup");
            match s3_helpers::delete_prefix(bucket, incr_path).await {
                Ok(outcome) => {
                    report.objects_deleted += outcome.deleted;
                    chain_failures += outcome.failed;
                }
                Err(e) => {
                    warn!(path = %incr_path, error = %e, "Failed to delete incremental backup");
                    chain_failures += 1;
                }
            }
        }

        if chain_failures > 0 {
            warn!(
                full_backup = %chain.full_path,
                failed_objects = chain_failures,
                "Keeping full backup: its incrementals could not be fully deleted; rerun cleanup to retry"
            );
            report.objects_failed += chain_failures;
            report.chains_failed += 1;
            continue;
        }

        info!(path = %chain.full_path, timestamp = %chain.full.timestamp, "Deleting full backup");
        match s3_helpers::delete_prefix(bucket, &chain.full_path).await {
            Ok(outcome) => {
                report.objects_deleted += outcome.deleted;
                if outcome.is_complete() {
                    report.chains_deleted += 1;
                } else {
                    report.objects_failed += outcome.failed;
                    report.chains_failed += 1;
                }
            }
            Err(e) => {
                warn!(path = %chain.full_path, error = %e, "Failed to delete full backup");
                report.objects_failed += 1;
                report.chains_failed += 1;
            }
        }
    }

    info!(
        chains_deleted = report.chains_deleted,
        chains_failed = report.chains_failed,
        objects_deleted = report.objects_deleted,
        objects_failed = report.objects_failed,
        "Cleanup complete"
    );

    Ok(report)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backup::{BackupKind, BackupMetadata};
    use chrono::Utc;

    fn chain(path: &str) -> BackupChain {
        BackupChain {
            full_path: path.into(),
            full: BackupMetadata {
                version: crate::backup::METADATA_SCHEMA_VERSION,
                backup_id: "id".into(),
                kind: BackupKind::Full,
                timestamp: Utc::now(),
                base_backup_path: None,
                status: "BACKUP_CREATED".into(),
                total_size: 0,
                database: "db".into(),
                started_at: None,
                finished_at: None,
            },
            incrementals: vec![],
        }
    }

    #[test]
    fn keeps_all_when_fewer_than_or_equal_to_keep() {
        let chains = vec![chain("full/a/"), chain("full/b/"), chain("full/c/")];
        assert!(select_chains_for_deletion(&chains, 3).is_empty());
        assert!(select_chains_for_deletion(&chains[..2], 3).is_empty());
    }

    #[test]
    fn deletes_oldest_beyond_keep() {
        // Chains are newest-first, so deletions come from the tail.
        let chains = vec![
            chain("full/newest/"),
            chain("full/mid/"),
            chain("full/old/"),
            chain("full/oldest/"),
        ];
        let to_delete = select_chains_for_deletion(&chains, 2);
        let paths: Vec<&str> = to_delete.iter().map(|c| c.full_path.as_str()).collect();
        assert_eq!(paths, vec!["full/old/", "full/oldest/"]);
    }

    #[test]
    fn empty_input_is_safe() {
        assert!(select_chains_for_deletion(&[], 3).is_empty());
    }

    #[test]
    fn report_has_failures_when_chains_or_objects_failed() {
        assert!(!CleanupReport::default().has_failures());
        assert!(
            CleanupReport {
                chains_failed: 1,
                ..Default::default()
            }
            .has_failures()
        );
        assert!(
            CleanupReport {
                objects_failed: 2,
                ..Default::default()
            }
            .has_failures()
        );
    }
}
