pub mod discovery;
pub mod executor;
pub mod progress;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq)]
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

/// Schema version this build writes into `.clickvault_meta.json` sidecars.
/// Bump when the metadata shape changes. Sidecars written before versioning
/// deserialize as version 0.
pub const METADATA_SCHEMA_VERSION: u32 = 1;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BackupMetadata {
    /// Sidecar schema version (see [`METADATA_SCHEMA_VERSION`]).
    #[serde(default)]
    pub version: u32,
    pub backup_id: String,
    pub kind: BackupKind,
    /// Submission time (also encoded in the S3 path); chain ordering and the
    /// full-backup interval key off this.
    pub timestamp: DateTime<Utc>,
    /// S3 path of the backup this one is based on (for incremental backups).
    pub base_backup_path: Option<String>,
    pub status: String,
    pub total_size: u64,
    pub database: String,
    /// Actual start of the backup as recorded by ClickHouse in system.backups.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub started_at: Option<DateTime<Utc>>,
    /// Actual end of the backup as recorded by ClickHouse in system.backups.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub finished_at: Option<DateTime<Utc>>,
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
            version: METADATA_SCHEMA_VERSION,
            backup_id: "id".into(),
            kind,
            timestamp: Utc::now(),
            base_backup_path: None,
            status: "BACKUP_CREATED".into(),
            total_size: 42,
            database: "db".into(),
            started_at: None,
            finished_at: None,
        }
    }

    #[test]
    fn pre_versioning_sidecar_still_deserializes() {
        // Exact shape written by clickvault before the version field existed.
        let json = r#"{
            "backup_id": "abc",
            "kind": "incremental",
            "timestamp": "2026-05-01T02:03:04.567Z",
            "base_backup_path": "backups/full/20260430T000000000Z/",
            "status": "BACKUP_CREATED",
            "total_size": 123,
            "database": "mydb"
        }"#;
        let meta: BackupMetadata = serde_json::from_str(json).unwrap();
        assert_eq!(meta.version, 0);
        assert_eq!(meta.started_at, None);
        assert_eq!(meta.finished_at, None);
        assert_eq!(meta.kind, BackupKind::Incremental);
    }

    #[test]
    fn versioned_sidecar_roundtrips_backup_window() {
        let mut meta = md(BackupKind::Full);
        meta.started_at = chrono::DateTime::from_timestamp(1_760_000_000, 0);
        meta.finished_at = chrono::DateTime::from_timestamp(1_760_000_120, 0);

        let json = serde_json::to_value(&meta).unwrap();
        assert_eq!(json["version"], METADATA_SCHEMA_VERSION);
        assert!(json["started_at"].is_string());

        let back: BackupMetadata = serde_json::from_value(json).unwrap();
        assert_eq!(back.started_at, meta.started_at);
        assert_eq!(back.finished_at, meta.finished_at);
    }

    #[test]
    fn unset_backup_window_is_omitted_from_json() {
        let json = serde_json::to_value(md(BackupKind::Full)).unwrap();
        assert!(json.get("started_at").is_none());
        assert!(json.get("finished_at").is_none());
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
