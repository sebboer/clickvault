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
}
