use chrono::{DateTime, Utc};
use s3::creds::Credentials;
use s3::{Bucket, Region};

use crate::backup::BackupMetadata;
use crate::config::S3Config;
use crate::error::ClickVaultError;

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

pub async fn read_metadata(
    bucket: &Bucket,
    backup_path: &str,
) -> Result<BackupMetadata, ClickVaultError> {
    let path = metadata_path(backup_path);
    let response = bucket
        .get_object(&path)
        .await
        .map_err(ClickVaultError::S3)?;
    let meta: BackupMetadata = serde_json::from_slice(response.as_slice())?;
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

/// Deletes all objects under a given prefix. Returns the count of deleted objects.
pub async fn delete_prefix(bucket: &Bucket, prefix: &str) -> Result<u64, ClickVaultError> {
    let mut deleted = 0u64;

    let results = bucket
        .list(prefix.to_string(), None)
        .await
        .map_err(ClickVaultError::S3)?;

    for result in &results {
        for object in &result.contents {
            bucket
                .delete_object(&object.key)
                .await
                .map_err(ClickVaultError::S3)?;
            deleted += 1;
        }
    }

    Ok(deleted)
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
