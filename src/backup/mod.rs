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

#[cfg(test)]
mod tests {
    use super::*;

    fn md(kind: BackupKind) -> BackupMetadata {
        BackupMetadata {
            backup_id: "id".into(),
            kind,
            timestamp: Utc::now(),
            base_backup_path: None,
            status: "BACKUP_CREATED".into(),
            total_size: 42,
            database: "db".into(),
        }
    }

    #[test]
    fn backup_kind_display() {
        assert_eq!(BackupKind::Full.to_string(), "full");
        assert_eq!(BackupKind::Incremental.to_string(), "incremental");
    }

    #[test]
    fn backup_kind_serde_roundtrip() {
        assert_eq!(
            serde_json::to_string(&BackupKind::Full).unwrap(),
            "\"full\""
        );
        let kind: BackupKind = serde_json::from_str("\"incremental\"").unwrap();
        assert_eq!(kind, BackupKind::Incremental);
    }

    #[test]
    fn chain_latest_returns_full_when_no_incrementals() {
        let chain = BackupChain {
            full_path: "full/f1/".into(),
            full: md(BackupKind::Full),
            incrementals: vec![],
        };
        assert_eq!(chain.latest().0, "full/f1/");
    }

    #[test]
    fn chain_latest_returns_last_incremental() {
        let chain = BackupChain {
            full_path: "full/f1/".into(),
            full: md(BackupKind::Full),
            incrementals: vec![
                ("incremental/i1/".into(), md(BackupKind::Incremental)),
                ("incremental/i2/".into(), md(BackupKind::Incremental)),
            ],
        };
        assert_eq!(chain.latest().0, "incremental/i2/");
    }

    #[test]
    fn backup_metadata_json_roundtrip() {
        let meta = md(BackupKind::Incremental);
        let json = serde_json::to_string(&meta).unwrap();
        let back: BackupMetadata = serde_json::from_str(&json).unwrap();
        assert_eq!(back.kind, BackupKind::Incremental);
        assert_eq!(back.total_size, 42);
        assert_eq!(back.database, "db");
    }
}
