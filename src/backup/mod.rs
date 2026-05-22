pub mod discovery;
pub mod executor;
pub mod progress;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum BackupKind {
    Full,
    Incremental,
}

impl std::fmt::Display for BackupKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BackupKind::Full => write!(f, "full"),
            BackupKind::Incremental => write!(f, "incremental"),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BackupMetadata {
    pub backup_id: String,
    pub kind: BackupKind,
    pub timestamp: DateTime<Utc>,
    /// S3 path of the backup this one is based on (for incremental backups).
    pub base_backup_path: Option<String>,
    pub status: String,
    pub total_size: u64,
    pub database: String,
}

/// A full backup and all its chained incrementals.
#[derive(Debug)]
pub struct BackupChain {
    pub full_path: String,
    pub full: BackupMetadata,
    /// Incrementals ordered by timestamp (oldest first).
    pub incrementals: Vec<(String, BackupMetadata)>,
}

impl BackupChain {
    /// Returns the most recent backup in this chain (the latest incremental, or the full if none).
    pub fn latest(&self) -> (&str, &BackupMetadata) {
        self.incrementals
            .last()
            .map(|(path, meta)| (path.as_str(), meta))
            .unwrap_or((&self.full_path, &self.full))
    }
}
