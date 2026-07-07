use chrono::{DateTime, Utc};
use s3::creds::Credentials;
use s3::error::S3Error;
use s3::{Bucket, Region};
use tracing::warn;

use crate::backup::BackupMetadata;
use crate::config::S3Config;
use crate::error::{ClickVaultError, MetadataReadError};

const METADATA_FILENAME: &str = ".clickvault_meta.json";

pub fn build_bucket(config: &S3Config) -> Result<Box<Bucket>, ClickVaultError> {
    let region = Region::Custom {
        region: config.region.clone(),
        endpoint: config.endpoint.clone(),
    };

    let credentials = Credentials::new(
        config.access_key.as_deref(),
        config.secret_key.as_deref(),
        None,
        None,
        None,
    )
    .map_err(|e| ClickVaultError::Config(format!("Invalid S3 credentials: {e}")))?;

    let mut bucket = Bucket::new(&config.bucket, region, credentials)
        .map_err(|e| ClickVaultError::Config(format!("Failed to create S3 bucket handle: {e}")))?;

    if config.path_style {
        bucket = bucket.with_path_style();
    }

    Ok(bucket)
}

/// Timestamp format for backup paths. Millisecond precision keeps two backups
/// started in the same second (e.g. a forced run racing a scheduled one) from
/// colliding on the same S3 prefix.
const PATH_TIMESTAMP_FORMAT: &str = "%Y%m%dT%H%M%S%3fZ";

pub fn full_backup_path(prefix: &str, timestamp: &DateTime<Utc>) -> String {
    let ts = timestamp.format(PATH_TIMESTAMP_FORMAT);
    if prefix.is_empty() {
        format!("full/{ts}/")
    } else {
        format!("{prefix}/full/{ts}/")
    }
}

pub fn incremental_backup_path(prefix: &str, timestamp: &DateTime<Utc>) -> String {
    let ts = timestamp.format(PATH_TIMESTAMP_FORMAT);
    if prefix.is_empty() {
        format!("incremental/{ts}/")
    } else {
        format!("{prefix}/incremental/{ts}/")
    }
}

/// Escapes a value for inclusion in a single-quoted ClickHouse string literal.
fn escape_sql_str(s: &str) -> String {
    s.replace('\\', "\\\\").replace('\'', "\\'")
}

/// Builds the S3() SQL fragment for use in ClickHouse BACKUP/RESTORE commands.
/// Returns: S3('https://endpoint/bucket/path', 'access_key', 'secret_key')
pub fn s3_sql_fragment(config: &S3Config, path: &str) -> String {
    let url = format!(
        "{}/{}/{}",
        config.clickhouse_endpoint(),
        config.bucket,
        path
    );
    let access_key = config.access_key.as_deref().unwrap_or("");
    let secret_key = config.secret_key.as_deref().unwrap_or("");
    format!(
        "S3('{}', '{}', '{}')",
        escape_sql_str(&url),
        escape_sql_str(access_key),
        escape_sql_str(secret_key)
    )
}

pub fn metadata_path(backup_path: &str) -> String {
    format!("{}{}", backup_path, METADATA_FILENAME)
}

pub async fn write_metadata(
    bucket: &Bucket,
    backup_path: &str,
    meta: &BackupMetadata,
) -> Result<(), ClickVaultError> {
    let json = serde_json::to_string_pretty(meta)?;
    let path = metadata_path(backup_path);
    bucket
        .put_object(&path, json.as_bytes())
        .await
        .map_err(ClickVaultError::S3)?;
    Ok(())
}

/// Classifies a `get_object` failure on a metadata sidecar: a 404 means the
/// sidecar does not exist (orphan), anything else is a possibly-transient
/// read failure.
fn classify_read_error(e: S3Error) -> MetadataReadError {
    match e {
        S3Error::HttpFailWithBody(404, _) => MetadataReadError::Missing,
        other => MetadataReadError::Unreadable(other),
    }
}

pub async fn read_metadata(
    bucket: &Bucket,
    backup_path: &str,
) -> Result<BackupMetadata, MetadataReadError> {
    let path = metadata_path(backup_path);
    let response = bucket
        .get_object(&path)
        .await
        .map_err(classify_read_error)?;
    let meta: BackupMetadata = serde_json::from_slice(response.as_slice())?;

    if meta.version > crate::backup::METADATA_SCHEMA_VERSION {
        warn!(
            path = %path,
            version = meta.version,
            supported = crate::backup::METADATA_SCHEMA_VERSION,
            "Metadata sidecar was written by a newer clickvault; unknown fields are ignored"
        );
    }

    Ok(meta)
}

/// Lists "directories" under a given prefix by using S3 list with a delimiter.
/// Returns the common prefixes (directory-like entries).
pub async fn list_prefixes(bucket: &Bucket, prefix: &str) -> Result<Vec<String>, ClickVaultError> {
    let mut prefixes = Vec::new();
    let mut continuation_token = None;

    loop {
        let results = bucket
            .list(prefix.to_string(), Some("/".to_string()))
            .await
            .map_err(ClickVaultError::S3)?;

        for result in &results {
            if let Some(cps) = &result.common_prefixes {
                for cp in cps {
                    prefixes.push(cp.prefix.clone());
                }
            }

            continuation_token = result.next_continuation_token.clone();
        }

        if continuation_token.is_none() {
            break;
        }
    }

    prefixes.sort();
    Ok(prefixes)
}

/// Result of deleting the objects under a prefix.
#[derive(Debug, Default)]
pub struct DeleteOutcome {
    pub deleted: u64,
    /// Objects that could not be deleted, plus the metadata sidecar when it
    /// was deliberately kept because data objects failed to delete.
    pub failed: u64,
}

impl DeleteOutcome {
    pub fn is_complete(&self) -> bool {
        self.failed == 0
    }
}

/// Splits keys into (data objects, metadata sidecar) so the sidecar can be
/// deleted last: if deletion is interrupted, the backup stays visible to
/// discovery and the next cleanup run can retry it.
fn split_sidecar_last(keys: Vec<String>, sidecar: &str) -> (Vec<String>, Vec<String>) {
    keys.into_iter().partition(|key| key != sidecar)
}

async fn delete_keys(bucket: &Bucket, keys: &[String], outcome: &mut DeleteOutcome) {
    for key in keys {
        match bucket.delete_object(key).await {
            Ok(_) => outcome.deleted += 1,
            Err(e) => {
                warn!(key = %key, error = %e, "Failed to delete object");
                outcome.failed += 1;
            }
        }
    }
}

/// Deletes all objects under a given prefix, continuing past individual
/// failures. The metadata sidecar is deleted last, and only if every data
/// object was deleted, so a partially-deleted backup remains discoverable.
pub async fn delete_prefix(
    bucket: &Bucket,
    prefix: &str,
) -> Result<DeleteOutcome, ClickVaultError> {
    let results = bucket
        .list(prefix.to_string(), None)
        .await
        .map_err(ClickVaultError::S3)?;

    let keys: Vec<String> = results
        .iter()
        .flat_map(|result| result.contents.iter().map(|object| object.key.clone()))
        .collect();
    let (data_keys, sidecar_keys) = split_sidecar_last(keys, &metadata_path(prefix));

    let mut outcome = DeleteOutcome::default();
    delete_keys(bucket, &data_keys, &mut outcome).await;

    if outcome.failed == 0 {
        delete_keys(bucket, &sidecar_keys, &mut outcome).await;
    } else if !sidecar_keys.is_empty() {
        warn!(
            prefix = %prefix,
            failed = outcome.failed,
            "Keeping metadata sidecar so the backup stays discoverable; rerun cleanup to retry"
        );
        outcome.failed += sidecar_keys.len() as u64;
    }

    Ok(outcome)
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    fn s3_config() -> S3Config {
        S3Config {
            endpoint: "https://s3.example.com".into(),
            clickhouse_endpoint: None,
            bucket: "my-bucket".into(),
            prefix: "backups".into(),
            region: "eu-central-1".into(),
            access_key: Some("AKIA".into()),
            secret_key: Some("secret".into()),
            path_style: false,
        }
    }

    #[test]
    fn full_backup_path_with_and_without_prefix() {
        let ts = Utc.with_ymd_and_hms(2026, 1, 2, 3, 4, 5).unwrap();
        assert_eq!(
            full_backup_path("backups", &ts),
            "backups/full/20260102T030405000Z/"
        );
        assert_eq!(full_backup_path("", &ts), "full/20260102T030405000Z/");
    }

    #[test]
    fn incremental_backup_path_with_and_without_prefix() {
        let ts = Utc.with_ymd_and_hms(2026, 1, 2, 3, 4, 5).unwrap();
        assert_eq!(
            incremental_backup_path("backups", &ts),
            "backups/incremental/20260102T030405000Z/"
        );
        assert_eq!(
            incremental_backup_path("", &ts),
            "incremental/20260102T030405000Z/"
        );
    }

    #[test]
    fn paths_are_millisecond_unique_within_the_same_second() {
        let a = Utc.with_ymd_and_hms(2026, 1, 2, 3, 4, 5).unwrap();
        let b = a + chrono::Duration::milliseconds(123);
        assert_ne!(full_backup_path("p", &a), full_backup_path("p", &b));
        assert_eq!(full_backup_path("p", &b), "p/full/20260102T030405123Z/");
    }

    #[test]
    fn metadata_path_appends_sidecar_filename() {
        assert_eq!(
            metadata_path("backups/full/20260102T030405000Z/"),
            "backups/full/20260102T030405000Z/.clickvault_meta.json"
        );
    }

    #[test]
    fn escape_sql_str_escapes_quotes_and_backslashes() {
        assert_eq!(escape_sql_str("plain"), "plain");
        assert_eq!(escape_sql_str("O'Brien"), "O\\'Brien");
        assert_eq!(escape_sql_str("a\\b"), "a\\\\b");
        // Backslash is escaped before the quote so the result is unambiguous.
        assert_eq!(escape_sql_str("\\'"), "\\\\\\'");
    }

    #[test]
    fn s3_sql_fragment_builds_expected_call() {
        let cfg = s3_config();
        assert_eq!(
            s3_sql_fragment(&cfg, "backups/full/x/"),
            "S3('https://s3.example.com/my-bucket/backups/full/x/', 'AKIA', 'secret')"
        );
    }

    #[test]
    fn s3_sql_fragment_escapes_credentials() {
        let mut cfg = s3_config();
        cfg.secret_key = Some("se'cret".into());
        let frag = s3_sql_fragment(&cfg, "p/");
        assert!(frag.ends_with("'se\\'cret')"), "got: {frag}");
    }

    #[test]
    fn s3_sql_fragment_uses_clickhouse_endpoint_override() {
        let mut cfg = s3_config();
        cfg.clickhouse_endpoint = Some("http://rustfs:9000".into());
        let frag = s3_sql_fragment(&cfg, "p/");
        assert!(
            frag.contains("http://rustfs:9000/my-bucket/p/"),
            "got: {frag}"
        );
    }

    #[test]
    fn classify_read_error_treats_404_as_missing() {
        let missing = classify_read_error(S3Error::HttpFailWithBody(404, String::new()));
        assert!(matches!(missing, MetadataReadError::Missing));

        let denied = classify_read_error(S3Error::HttpFailWithBody(403, String::new()));
        assert!(matches!(denied, MetadataReadError::Unreadable(_)));

        let server_error = classify_read_error(S3Error::HttpFailWithBody(500, String::new()));
        assert!(matches!(server_error, MetadataReadError::Unreadable(_)));
    }

    #[test]
    fn split_sidecar_last_separates_metadata_from_data() {
        let prefix = "backups/full/20260102T030405000Z/";
        let sidecar = metadata_path(prefix);
        let keys = vec![
            format!("{prefix}data/part1"),
            sidecar.clone(),
            format!("{prefix}data/part2"),
        ];
        let (data, sidecars) = split_sidecar_last(keys, &sidecar);
        assert_eq!(
            data,
            vec![format!("{prefix}data/part1"), format!("{prefix}data/part2")]
        );
        assert_eq!(sidecars, vec![sidecar]);
    }

    #[test]
    fn delete_outcome_is_complete_only_without_failures() {
        assert!(
            DeleteOutcome {
                deleted: 3,
                failed: 0
            }
            .is_complete()
        );
        assert!(
            !DeleteOutcome {
                deleted: 3,
                failed: 1
            }
            .is_complete()
        );
    }

    #[test]
    fn s3_sql_fragment_handles_missing_credentials() {
        let mut cfg = s3_config();
        cfg.access_key = None;
        cfg.secret_key = None;
        assert_eq!(
            s3_sql_fragment(&cfg, "p/"),
            "S3('https://s3.example.com/my-bucket/p/', '', '')"
        );
    }
}
