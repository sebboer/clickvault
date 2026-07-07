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

    Ok(group_chains(fulls, incrementals))
}

/// Groups incrementals under their full backups by tracing chain links.
///
/// Pure function (no I/O) so the grouping/sorting logic can be tested directly.
/// Returned chains are sorted newest-first; incrementals within each chain are
/// sorted oldest-first.
fn group_chains(
    fulls: Vec<(String, BackupMetadata)>,
    incrementals: Vec<(String, BackupMetadata)>,
) -> Vec<BackupChain> {
    let mut chains: Vec<BackupChain> = fulls
        .into_iter()
        .map(|(path, meta)| BackupChain {
            full_path: path,
            full: meta,
            incrementals: Vec::new(),
        })
        .collect();

    // Process incrementals oldest-first so a parent is always attached before
    // any child that chains off it (deep-chain tracing only looks at already
    // attached incrementals).
    let mut incrementals = incrementals;
    incrementals.sort_by_key(|(_, meta)| meta.timestamp);

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

    chains
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
                        if chain.full_path.trim_end_matches('/') == next_base.trim_end_matches('/')
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
pub fn should_do_full_backup(latest_full: Option<&BackupMetadata>, interval_days: u32) -> bool {
    match latest_full {
        None => true,
        Some(meta) => {
            let age = Utc::now() - meta.timestamp;
            age >= Duration::days(interval_days as i64)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backup::BackupKind;
    use chrono::{DateTime, TimeZone};

    fn t(secs: i64) -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap() + Duration::seconds(secs)
    }

    fn md(kind: BackupKind, ts: DateTime<Utc>, base: Option<&str>) -> BackupMetadata {
        BackupMetadata {
            backup_id: "id".into(),
            kind,
            timestamp: ts,
            base_backup_path: base.map(|s| s.to_string()),
            status: "BACKUP_CREATED".into(),
            total_size: 0,
            database: "db".into(),
        }
    }

    #[test]
    fn should_do_full_backup_when_none_exists() {
        assert!(should_do_full_backup(None, 7));
    }

    #[test]
    fn should_do_full_backup_respects_interval() {
        let recent = md(BackupKind::Full, Utc::now() - Duration::days(1), None);
        assert!(!should_do_full_backup(Some(&recent), 7));

        let old = md(BackupKind::Full, Utc::now() - Duration::days(8), None);
        assert!(should_do_full_backup(Some(&old), 7));

        // Exactly at the interval boundary triggers a full backup.
        let boundary = md(BackupKind::Full, Utc::now() - Duration::days(7), None);
        assert!(should_do_full_backup(Some(&boundary), 7));
    }

    #[test]
    fn group_chains_single_full_no_incrementals() {
        let chains = group_chains(
            vec![("full/f1/".into(), md(BackupKind::Full, t(0), None))],
            vec![],
        );
        assert_eq!(chains.len(), 1);
        assert_eq!(chains[0].full_path, "full/f1/");
        assert!(chains[0].incrementals.is_empty());
    }

    #[test]
    fn group_chains_nests_direct_incremental() {
        let fulls = vec![("full/f1/".into(), md(BackupKind::Full, t(0), None))];
        let incrs = vec![(
            "incremental/i1/".into(),
            md(BackupKind::Incremental, t(10), Some("full/f1/")),
        )];
        let chains = group_chains(fulls, incrs);
        assert_eq!(chains.len(), 1);
        assert_eq!(chains[0].incrementals.len(), 1);
        assert_eq!(chains[0].incrementals[0].0, "incremental/i1/");
    }

    #[test]
    fn group_chains_traces_deep_chain_and_sorts_incrementals() {
        let fulls = vec![("full/f1/".into(), md(BackupKind::Full, t(0), None))];
        // Provided out of order; i2 chains off i1 which chains off the full.
        let incrs = vec![
            (
                "incremental/i2/".into(),
                md(BackupKind::Incremental, t(20), Some("incremental/i1/")),
            ),
            (
                "incremental/i1/".into(),
                md(BackupKind::Incremental, t(10), Some("full/f1/")),
            ),
        ];
        let chains = group_chains(fulls, incrs);
        assert_eq!(chains.len(), 1);
        let paths: Vec<&str> = chains[0]
            .incrementals
            .iter()
            .map(|(p, _)| p.as_str())
            .collect();
        assert_eq!(paths, vec!["incremental/i1/", "incremental/i2/"]);
        assert_eq!(chains[0].latest().0, "incremental/i2/");
    }

    #[test]
    fn group_chains_drops_orphan_incremental() {
        let fulls = vec![("full/f1/".into(), md(BackupKind::Full, t(0), None))];
        let incrs = vec![(
            "incremental/orphan/".into(),
            md(BackupKind::Incremental, t(10), Some("full/does-not-exist/")),
        )];
        let chains = group_chains(fulls, incrs);
        assert_eq!(chains.len(), 1);
        assert!(chains[0].incrementals.is_empty());
    }

    #[test]
    fn group_chains_sorts_chains_newest_first_and_routes_incrementals() {
        let fulls = vec![
            ("full/old/".into(), md(BackupKind::Full, t(0), None)),
            ("full/new/".into(), md(BackupKind::Full, t(100), None)),
        ];
        let incrs = vec![(
            "incremental/i1/".into(),
            md(BackupKind::Incremental, t(10), Some("full/old/")),
        )];
        let chains = group_chains(fulls, incrs);
        assert_eq!(chains.len(), 2);
        // Newest chain first.
        assert_eq!(chains[0].full_path, "full/new/");
        assert_eq!(chains[1].full_path, "full/old/");
        // Incremental routed to the correct (older) full.
        assert!(chains[0].incrementals.is_empty());
        assert_eq!(chains[1].incrementals.len(), 1);
    }
}
