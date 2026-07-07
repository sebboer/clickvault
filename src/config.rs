use std::collections::HashMap;
use std::path::Path;
use std::time::Duration;

use serde::Deserialize;

use crate::error::ClickVaultError;

#[derive(Debug, Deserialize)]
pub struct Config {
    pub clickhouse: ClickHouseConfig,
    pub s3: S3Config,
    #[serde(default)]
    pub backup: BackupConfig,
    pub schedule: ScheduleConfig,
    pub retention: RetentionConfig,
    pub notifications: Option<NotificationConfig>,
}

/// Tuning for backup execution. The whole section and every key are optional.
#[derive(Debug, Deserialize)]
#[serde(default)]
pub struct BackupConfig {
    /// Seconds between polls of system.backups while a backup is running.
    pub poll_interval_secs: u64,
    /// Overall time budget for a single backup before failing with TIMEOUT.
    /// Large databases can legitimately need more than the 24h default.
    pub timeout_secs: u64,
}

impl Default for BackupConfig {
    fn default() -> Self {
        Self {
            poll_interval_secs: 5,
            timeout_secs: 86_400, // 24 hours
        }
    }
}

impl BackupConfig {
    pub fn poll_interval(&self) -> Duration {
        Duration::from_secs(self.poll_interval_secs)
    }

    pub fn timeout(&self) -> Duration {
        Duration::from_secs(self.timeout_secs)
    }
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
    /// Run cleanup automatically after each successful backup, so retention
    /// is enforced without a separate cleanup cron entry.
    #[serde(default)]
    pub auto_cleanup: bool,
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

    /// Values that must never appear in logs, stderr, or notification
    /// payloads. ClickHouse error messages can echo the BACKUP statement —
    /// which carries inline S3 credentials — e.g. syntax errors quote the
    /// query text around the failure position.
    fn secrets(&self) -> impl Iterator<Item = &str> {
        [
            self.s3.access_key.as_deref(),
            self.s3.secret_key.as_deref(),
            self.clickhouse.password.as_deref(),
        ]
        .into_iter()
        .flatten()
        .filter(|s| !s.is_empty())
    }

    /// Replaces every occurrence of a configured secret in `text` with `***`.
    pub fn redact_secrets(&self, text: &str) -> String {
        let mut out = text.to_string();
        for secret in self.secrets() {
            if out.contains(secret) {
                out = out.replace(secret, "***");
            }
        }
        out
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
        if self.backup.poll_interval_secs == 0 {
            return Err(ClickVaultError::Config(
                "backup.poll_interval_secs must be > 0".into(),
            ));
        }
        if self.backup.timeout_secs < self.backup.poll_interval_secs {
            return Err(ClickVaultError::Config(
                "backup.timeout_secs must be >= backup.poll_interval_secs".into(),
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
    fn example_config_parses_and_validates() {
        let cfg: Config =
            toml::from_str(include_str!("../config.example.toml")).expect("example parses");
        assert!(cfg.validate().is_ok());
    }

    #[test]
    fn auto_cleanup_defaults_off_and_parses() {
        let cfg = parse(VALID);
        assert!(!cfg.retention.auto_cleanup);

        let toml_str = VALID.replace(
            "keep_full_backups = 4",
            "keep_full_backups = 4\nauto_cleanup = true",
        );
        let cfg = parse(&toml_str);
        assert!(cfg.retention.auto_cleanup);
    }

    #[test]
    fn backup_section_defaults_when_absent() {
        let cfg = parse(VALID);
        assert_eq!(cfg.backup.poll_interval_secs, 5);
        assert_eq!(cfg.backup.timeout_secs, 86_400);
        assert_eq!(cfg.backup.poll_interval(), Duration::from_secs(5));
        assert_eq!(cfg.backup.timeout(), Duration::from_secs(86_400));
    }

    #[test]
    fn backup_section_overrides_and_partial_defaults() {
        let toml_str = format!("{VALID}\n[backup]\npoll_interval_secs = 2\n");
        let cfg = parse(&toml_str);
        assert_eq!(cfg.backup.poll_interval_secs, 2);
        // Unspecified key keeps its default.
        assert_eq!(cfg.backup.timeout_secs, 86_400);
    }

    #[test]
    fn validation_rejects_bad_backup_tuning() {
        let mut cfg = parse(VALID);
        cfg.backup.poll_interval_secs = 0;
        assert!(cfg.validate().is_err());

        let mut cfg = parse(VALID);
        cfg.backup.poll_interval_secs = 10;
        cfg.backup.timeout_secs = 5;
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn redact_secrets_masks_all_configured_secrets() {
        let mut cfg = parse(VALID);
        cfg.s3.access_key = Some("AKIAKEY".into());
        cfg.s3.secret_key = Some("supersecret".into());
        cfg.clickhouse.password = Some("chpass".into());

        let text = "S3('http://e/b/p/', 'AKIAKEY', 'supersecret') pw=chpass again: supersecret";
        assert_eq!(
            cfg.redact_secrets(text),
            "S3('http://e/b/p/', '***', '***') pw=*** again: ***"
        );
    }

    #[test]
    fn redact_secrets_handles_missing_and_empty_secrets() {
        let mut cfg = parse(VALID);
        cfg.s3.secret_key = Some(String::new());

        let text = "nothing to hide";
        assert_eq!(cfg.redact_secrets(text), text);
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
