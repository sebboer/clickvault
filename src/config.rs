use std::collections::HashMap;
use std::path::Path;

use serde::Deserialize;

use crate::error::ClickVaultError;

#[derive(Debug, Deserialize)]
pub struct Config {
    pub clickhouse: ClickHouseConfig,
    pub s3: S3Config,
    pub schedule: ScheduleConfig,
    pub retention: RetentionConfig,
    pub notifications: Option<NotificationConfig>,
}

#[derive(Debug, Deserialize)]
pub struct ClickHouseConfig {
    pub url: String,
    #[serde(default = "default_user")]
    pub user: Option<String>,
    #[serde(default)]
    pub password: Option<String>,
    pub database: String,
}

fn default_user() -> Option<String> {
    Some("default".to_string())
}

#[derive(Debug, Deserialize)]
pub struct S3Config {
    pub endpoint: String,
    /// S3 endpoint as seen by ClickHouse (e.g., Docker-internal URL).
    /// Falls back to `endpoint` if not set.
    pub clickhouse_endpoint: Option<String>,
    pub bucket: String,
    #[serde(default)]
    pub prefix: String,
    pub region: String,
    pub access_key: Option<String>,
    pub secret_key: Option<String>,
    #[serde(default)]
    pub path_style: bool,
}

impl S3Config {
    /// Returns the endpoint that ClickHouse should use in BACKUP/RESTORE SQL.
    pub fn clickhouse_endpoint(&self) -> &str {
        self.clickhouse_endpoint
            .as_deref()
            .unwrap_or(&self.endpoint)
    }
}

#[derive(Debug, Deserialize)]
pub struct ScheduleConfig {
    pub full_backup_interval_days: u32,
}

#[derive(Debug, Deserialize)]
pub struct RetentionConfig {
    pub keep_full_backups: u32,
}

#[derive(Debug, Deserialize)]
pub struct NotificationConfig {
    #[serde(default = "default_true")]
    pub on_success: bool,
    #[serde(default = "default_true")]
    pub on_failure: bool,
    #[serde(default)]
    pub providers: Vec<NotificationProvider>,
}

fn default_true() -> bool {
    true
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum NotificationProvider {
    Slack {
        webhook_url: String,
    },
    Webhook {
        url: String,
        #[serde(default = "default_method")]
        method: String,
        #[serde(default)]
        headers: HashMap<String, String>,
    },
}

fn default_method() -> String {
    "POST".to_string()
}

impl Config {
    pub fn load(path: &Path) -> Result<Self, ClickVaultError> {
        let contents = std::fs::read_to_string(path).map_err(|e| {
            ClickVaultError::Config(format!(
                "Failed to read config file {}: {e}",
                path.display()
            ))
        })?;

        let mut config: Config = toml::from_str(&contents)
            .map_err(|e| ClickVaultError::Config(format!("Invalid TOML config: {e}")))?;

        config.apply_env_overrides();
        config.validate()?;

        Ok(config)
    }

    fn apply_env_overrides(&mut self) {
        if let Ok(val) = std::env::var("CLICKVAULT_CLICKHOUSE_USER") {
            self.clickhouse.user = Some(val);
        }
        if let Ok(val) = std::env::var("CLICKVAULT_CLICKHOUSE_PASSWORD") {
            self.clickhouse.password = Some(val);
        }
        if let Ok(val) = std::env::var("CLICKVAULT_S3_ACCESS_KEY") {
            self.s3.access_key = Some(val);
        }
        if let Ok(val) = std::env::var("CLICKVAULT_S3_SECRET_KEY") {
            self.s3.secret_key = Some(val);
        }
    }

    fn validate(&self) -> Result<(), ClickVaultError> {
        if self.clickhouse.url.is_empty() {
            return Err(ClickVaultError::Config(
                "clickhouse.url must not be empty".into(),
            ));
        }
        if self.clickhouse.database.is_empty() {
            return Err(ClickVaultError::Config(
                "clickhouse.database must not be empty".into(),
            ));
        }
        if self.s3.bucket.is_empty() {
            return Err(ClickVaultError::Config(
                "s3.bucket must not be empty".into(),
            ));
        }
        if self.s3.region.is_empty() {
            return Err(ClickVaultError::Config(
                "s3.region must not be empty".into(),
            ));
        }
        if self.schedule.full_backup_interval_days == 0 {
            return Err(ClickVaultError::Config(
                "schedule.full_backup_interval_days must be > 0".into(),
            ));
        }
        if self.retention.keep_full_backups == 0 {
            return Err(ClickVaultError::Config(
                "retention.keep_full_backups must be > 0".into(),
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const VALID: &str = r#"
        [clickhouse]
        url = "http://localhost:8123"
        database = "mydb"

        [s3]
        endpoint = "https://s3.example.com"
        bucket = "bucket"
        region = "eu-central-1"

        [schedule]
        full_backup_interval_days = 7

        [retention]
        keep_full_backups = 4
    "#;

    fn parse(toml_str: &str) -> Config {
        toml::from_str(toml_str).expect("valid TOML")
    }

    #[test]
    fn valid_config_passes_validation() {
        let cfg = parse(VALID);
        assert!(cfg.validate().is_ok());
    }

    #[test]
    fn defaults_are_applied() {
        let cfg = parse(VALID);
        assert_eq!(cfg.clickhouse.user.as_deref(), Some("default"));
        assert_eq!(cfg.clickhouse.password, None);
        assert_eq!(cfg.s3.prefix, "");
        assert!(!cfg.s3.path_style);
        assert!(cfg.notifications.is_none());
    }

    #[test]
    fn clickhouse_endpoint_falls_back_to_endpoint() {
        let mut cfg = parse(VALID);
        assert_eq!(cfg.s3.clickhouse_endpoint(), "https://s3.example.com");
        cfg.s3.clickhouse_endpoint = Some("http://rustfs:9000".into());
        assert_eq!(cfg.s3.clickhouse_endpoint(), "http://rustfs:9000");
    }

    #[test]
    fn validation_rejects_empty_and_zero_fields() {
        let mut cfg = parse(VALID);
        cfg.s3.bucket = String::new();
        assert!(cfg.validate().is_err());

        let mut cfg = parse(VALID);
        cfg.clickhouse.database = String::new();
        assert!(cfg.validate().is_err());

        let mut cfg = parse(VALID);
        cfg.s3.region = String::new();
        assert!(cfg.validate().is_err());

        let mut cfg = parse(VALID);
        cfg.schedule.full_backup_interval_days = 0;
        assert!(cfg.validate().is_err());

        let mut cfg = parse(VALID);
        cfg.retention.keep_full_backups = 0;
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn notification_provider_defaults_parse() {
        let toml_str = format!(
            "{VALID}\n[notifications]\n[[notifications.providers]]\ntype = \"webhook\"\nurl = \"https://example.com/hook\"\n"
        );
        let cfg = parse(&toml_str);
        let notifications = cfg.notifications.expect("notifications present");
        assert!(notifications.on_success);
        assert!(notifications.on_failure);
        match &notifications.providers[0] {
            NotificationProvider::Webhook { method, url, .. } => {
                assert_eq!(method, "POST");
                assert_eq!(url, "https://example.com/hook");
            }
            _ => panic!("expected webhook provider"),
        }
    }
}
