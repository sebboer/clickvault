use std::io;

#[derive(Debug, thiserror::Error)]
pub enum ClickVaultError {
    #[error("ClickHouse error: {0}")]
    ClickHouse(#[from] clickhouse::error::Error),

    #[error("S3 error: {0}")]
    S3(#[from] s3::error::S3Error),

    #[error("Configuration error: {0}")]
    Config(String),

    #[error("Backup failed with status {status}: {message}")]
    BackupFailed { status: String, message: String },

    #[error("No base backup found for incremental backup")]
    NoBaseBackup,

    #[error("A backup is already in progress (id: {0})")]
    BackupInProgress(String),

    #[error("Notification error: {0}")]
    Notification(String),

    #[error("IO error: {0}")]
    Io(#[from] io::Error),

    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),

    #[error(
        "backup {id} disappeared from system.backups after {polls} consecutive polls \
         (was the ClickHouse server restarted?)"
    )]
    BackupStateLost { id: String, polls: u32 },

    #[error("cannot verify backup at {path}: {source}")]
    MetadataUnavailable {
        path: String,
        #[source]
        source: MetadataReadError,
    },
}

/// Why a `.clickvault_meta.json` sidecar could not be turned into metadata.
///
/// The distinction matters for cleanup: a `Missing` sidecar is a true orphan
/// (the backup was never visible to discovery), while `Invalid`/`Unreadable`
/// mean a real backup exists that we temporarily cannot account for.
#[derive(Debug, thiserror::Error)]
pub enum MetadataReadError {
    #[error("metadata sidecar is missing")]
    Missing,

    #[error("metadata sidecar could not be parsed: {0}")]
    Invalid(#[from] serde_json::Error),

    #[error("metadata sidecar could not be read: {0}")]
    Unreadable(#[from] s3::error::S3Error),
}
