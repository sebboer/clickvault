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

pub fn full_backup_path(prefix: &str, timestamp: &DateTime<Utc>) -> String {
    let ts = timestamp.format("%Y%m%dT%H%M%SZ");
    if prefix.is_empty() {
        format!("full/{ts}/")
    } else {
        format!("{prefix}/full/{ts}/")
    }
}

pub fn incremental_backup_path(prefix: &str, timestamp: &DateTime<Utc>) -> String {
    let ts = timestamp.format("%Y%m%dT%H%M%SZ");
    if prefix.is_empty() {
        format!("incremental/{ts}/")
    } else {
        format!("{prefix}/incremental/{ts}/")
    }
}

/// Builds the S3() SQL fragment for use in ClickHouse BACKUP/RESTORE commands.
/// Returns: S3('https://endpoint/bucket/path', 'access_key', 'secret_key')
pub fn s3_sql_fragment(config: &S3Config, path: &str) -> String {
    let url = format!("{}/{}/{}", config.clickhouse_endpoint(), config.bucket, path);
    let access_key = config.access_key.as_deref().unwrap_or("");
    let secret_key = config.secret_key.as_deref().unwrap_or("");
    format!("S3('{url}', '{access_key}', '{secret_key}')")
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
pub async fn list_prefixes(
    bucket: &Bucket,
    prefix: &str,
) -> Result<Vec<String>, ClickVaultError> {
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
