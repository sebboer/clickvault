use std::path::PathBuf;

use clap::{Parser, Subcommand};

#[derive(Debug, Parser)]
#[command(name = "clickvault", about = "ClickHouse backup manager for S3")]
pub struct Cli {
    /// Path to the TOML configuration file
    #[arg(
        short,
        long,
        global = true,
        default_value = "/etc/clickvault/config.toml"
    )]
    pub config: PathBuf,

    /// Log level (trace, debug, info, warn, error)
    #[arg(short, long, global = true, default_value = "info")]
    pub log_level: String,

    #[command(subcommand)]
    pub command: Command,
}

#[derive(Debug, Subcommand)]
pub enum Command {
    /// Run a backup (auto-detects full vs incremental)
    Backup {
        /// Force a full backup regardless of schedule
        #[arg(long)]
        full: bool,
        /// Skip the in-progress backup check (use when a previous backup is stuck)
        #[arg(long)]
        force: bool,
    },

    /// List known backups in S3
    List {
        /// Show only full backups
        #[arg(long)]
        full_only: bool,
    },

    /// Show the status of running and recent backups
    Status,

    /// Check that the newest backup is fresh enough (exit code for monitoring)
    Check {
        /// Maximum acceptable age of the newest backup (e.g. 90s, 30m, 26h, 2d)
        #[arg(long, value_parser = parse_max_age)]
        max_age: std::time::Duration,
        /// Print the summary as JSON
        #[arg(long)]
        json: bool,
    },

    /// Clean up expired backup chains
    Cleanup {
        /// Show what would be deleted without actually deleting
        #[arg(long)]
        dry_run: bool,
    },
}

/// Parses a duration like "90s", "30m", "26h", "2d"; a bare number is seconds.
fn parse_max_age(s: &str) -> Result<std::time::Duration, String> {
    let s = s.trim();
    let Some(unit) = s.chars().last() else {
        return Err("empty duration".into());
    };

    let (value, multiplier) = match unit {
        's' => (&s[..s.len() - 1], 1),
        'm' => (&s[..s.len() - 1], 60),
        'h' => (&s[..s.len() - 1], 3600),
        'd' => (&s[..s.len() - 1], 86_400),
        '0'..='9' => (s, 1),
        _ => return Err(format!("unknown duration unit '{unit}' (use s, m, h or d)")),
    };

    let value: u64 = value
        .parse()
        .map_err(|_| format!("invalid duration '{s}' (e.g. 90s, 30m, 26h, 2d)"))?;
    if value == 0 {
        return Err("duration must be greater than zero".into());
    }

    Ok(std::time::Duration::from_secs(value * multiplier))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn parse_max_age_accepts_suffixed_and_bare_values() {
        assert_eq!(parse_max_age("90s").unwrap(), Duration::from_secs(90));
        assert_eq!(parse_max_age("30m").unwrap(), Duration::from_secs(1800));
        assert_eq!(parse_max_age("26h").unwrap(), Duration::from_secs(93_600));
        assert_eq!(parse_max_age("2d").unwrap(), Duration::from_secs(172_800));
        assert_eq!(parse_max_age("45").unwrap(), Duration::from_secs(45));
        assert_eq!(parse_max_age(" 26h ").unwrap(), Duration::from_secs(93_600));
    }

    #[test]
    fn parse_max_age_rejects_bad_input() {
        assert!(parse_max_age("").is_err());
        assert!(parse_max_age("h").is_err());
        assert!(parse_max_age("26x").is_err());
        assert!(parse_max_age("-5h").is_err());
        assert!(parse_max_age("0h").is_err());
        assert!(parse_max_age("1.5h").is_err());
    }
}
