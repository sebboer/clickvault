use std::path::PathBuf;

use clap::{Parser, Subcommand};

#[derive(Debug, Parser)]
#[command(name = "clickvault", about = "ClickHouse backup manager for S3")]
pub struct Cli {
    /// Path to the TOML configuration file
    #[arg(short, long, default_value = "/etc/clickvault/config.toml")]
    pub config: PathBuf,

    /// Log level (trace, debug, info, warn, error)
    #[arg(short, long, default_value = "info")]
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
    },

    /// List known backups in S3
    List {
        /// Show only full backups
        #[arg(long)]
        full_only: bool,
    },

    /// Show the status of running and recent backups
    Status,

    /// Clean up expired backup chains
    Cleanup {
        /// Show what would be deleted without actually deleting
        #[arg(long)]
        dry_run: bool,
    },
}
