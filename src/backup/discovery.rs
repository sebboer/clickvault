use chrono::{Duration, Utc};
use s3::Bucket;
use tracing::{debug, warn};

use super::{BackupChain, BackupMetadata};
use crate::error::ClickVaultError;
use crate::s3 as s3_helpers;

/// Lists all full backups found in S3. Returns (path, metadata) pairs sorted by timestamp.
pub async fn list_full_backups(
    bucket: &Bucket,
    prefix: &str,
) -> Result<Vec<(String, BackupMetadata)>, ClickVaultError> {
    let full_prefix = if prefix.is_empty() {
        "full/".to_string()
    } else {
        format!("{prefix}/full/")
    };

    let dirs = s3_helpers::list_prefixes(bucket, &full_prefix).await?;
    let mut backups = Vec::new();

    for dir in dirs {
        match s3_helpers::read_metadata(bucket, &dir).await {
            Ok(meta) => backups.push((dir, meta)),
            Err(e) => {
                warn!("Skipping full backup at {dir} (unreadable metadata, possible orphan): {e}");
            }
        }
    }

    backups.sort_by_key(|(_, meta)| meta.timestamp);
    Ok(backups)
}

/// Lists all incremental backups found in S3. Returns (path, metadata) pairs sorted by timestamp.
pub async fn list_incremental_backups(
    bucket: &Bucket,
    prefix: &str,
) -> Result<Vec<(String, BackupMetadata)>, ClickVaultError> {
    let incr_prefix = if prefix.is_empty() {
        "incremental/".to_string()
    } else {
        format!("{prefix}/incremental/")
    };

    let dirs = s3_helpers::list_prefixes(bucket, &incr_prefix).await?;
    let mut backups = Vec::new();

    for dir in dirs {
        match s3_helpers::read_metadata(bucket, &dir).await {
            Ok(meta) => backups.push((dir, meta)),
            Err(e) => {
                warn!("Skipping incremental at {dir} (unreadable metadata, possible orphan): {e}");
            }
        }
    }

    backups.sort_by_key(|(_, meta)| meta.timestamp);
    Ok(backups)
}

/// Discovers all backup chains by grouping incrementals under their full backup.
/// Chains are sorted newest-first.
pub async fn discover_chains(
    bucket: &Bucket,
    prefix: &str,
) -> Result<Vec<BackupChain>, ClickVaultError> {
    let fulls = list_full_backups(bucket, prefix).await?;
    let incrementals = list_incremental_backups(bucket, prefix).await?;

    let mut chains: Vec<BackupChain> = fulls
        .into_iter()
        .map(|(path, meta)| BackupChain {
            full_path: path,
            full: meta,
            incrementals: Vec::new(),
        })
        .collect();

    // For deep chaining, we need to walk the chain links.
    // Each incremental's base_backup_path points to the previous backup in the chain.
    // We trace back to find which full backup each incremental belongs to.
    for (incr_path, incr_meta) in incrementals {
        if let Some(chain) = find_chain_for_incremental(&chains, &incr_path, &incr_meta) {
            if let Some(c) = chains.iter_mut().find(|c| c.full_path == chain) {
                c.incrementals.push((incr_path, incr_meta));
            }
        } else {
            debug!("Orphaned incremental backup: {incr_path}");
        }
    }

    // Sort incrementals within each chain by timestamp
    for chain in &mut chains {
        chain.incrementals.sort_by_key(|(_, meta)| meta.timestamp);
    }

    // Sort chains newest-first
    chains.sort_by_key(|c| std::cmp::Reverse(c.full.timestamp));

    Ok(chains)
}

/// Traces an incremental backup's chain back to find which full backup it belongs to.
fn find_chain_for_incremental(
    chains: &[BackupChain],
    _incr_path: &str,
    incr_meta: &BackupMetadata,
) -> Option<String> {
    // Walk back through base_backup_path links.
    // In the simplest case, the base points directly to a full backup.
    // In deep chaining, it may point to another incremental,
    // so we need to check all known backups.

    let mut current_base = incr_meta.base_backup_path.as_deref()?;

    // Check if the base is a full backup
    for chain in chains {
        if chain.full_path.trim_end_matches('/') == current_base.trim_end_matches('/') {
            return Some(chain.full_path.clone());
        }
    }

    // Check if the base is another incremental in a chain (deep chaining).
    // We need to trace the chain of incrementals back to a full.
    // For safety, limit the depth to avoid infinite loops.
    for _ in 0..100 {
        let mut found = false;

        for chain in chains {
            for (path, meta) in &chain.incrementals {
                if path.trim_end_matches('/') == current_base.trim_end_matches('/') {
                    if let Some(next_base) = &meta.base_backup_path {
                        // Check if next_base is the full backup
                        if chain.full_path.trim_end_matches('/')
                            == next_base.trim_end_matches('/')
                        {
                            return Some(chain.full_path.clone());
                        }
                        current_base = next_base;
                        found = true;
                    } else {
                        return Some(chain.full_path.clone());
                    }
                }
            }
        }

        if !found {
            break;
        }
    }

    None
}

/// Finds the latest backup across all chains (could be full or incremental).
/// This is the backup that a new incremental should chain off of.
#[allow(dead_code)]
pub async fn latest_backup(
    bucket: &Bucket,
    prefix: &str,
) -> Result<Option<(String, BackupMetadata)>, ClickVaultError> {
    let chains = discover_chains(bucket, prefix).await?;

    // Chains are sorted newest-first, so the first chain's latest backup is the overall latest.
    Ok(chains.first().map(|chain| {
        let (path, meta) = chain.latest();
        (path.to_string(), meta.clone())
    }))
}

/// Determines if a full backup should be performed based on the configured interval.
pub fn should_do_full_backup(
    latest_full: Option<&BackupMetadata>,
    interval_days: u32,
) -> bool {
    match latest_full {
        None => true,
        Some(meta) => {
            let age = Utc::now() - meta.timestamp;
            age >= Duration::days(interval_days as i64)
        }
    }
}
