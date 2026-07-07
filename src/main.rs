mod backup;
mod check;
mod cleanup;
mod cli;
mod config;
mod error;
mod notify;
mod retry;
mod s3;

use clap::Parser;
use tracing::{error, info};
use tracing_subscriber::EnvFilter;

use crate::backup::BackupKind;
use crate::cli::{Cli, Command};
use crate::config::Config;
use crate::notify::BackupEvent;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(&cli.log_level)),
        )
        .init();

    let config = Config::load(&cli.config)?;
    let bucket = s3::build_bucket(&config.s3)?;

    let ch_client = build_clickhouse_client(&config);

    let retry_policy = config.retry.policy();

    // Build notifiers (if configured)
    let notifiers = config
        .notifications
        .as_ref()
        .map(|nc| notify::build_notifiers(nc, retry_policy.clone()))
        .unwrap_or_default();

    match cli.command {
        Command::Backup { full, force } => {
            match backup::executor::run_backup(&ch_client, &bucket, &config, full, force).await {
                Ok(result) => {
                    info!(
                        kind = %result.metadata.kind,
                        size = result.metadata.total_size,
                        duration_secs = result.duration.as_secs(),
                        "Backup completed"
                    );

                    println!(
                        "Backup completed: {} | size: {} bytes | duration: {}s",
                        result.metadata.kind,
                        result.metadata.total_size,
                        result.duration.as_secs()
                    );

                    if let Some(notif_config) = &config.notifications {
                        let event = BackupEvent::backup_completed(
                            result.metadata.kind,
                            result.duration,
                            result.metadata.total_size,
                            config.clickhouse.database.clone(),
                        );
                        notify::dispatch(notif_config, &notifiers, &event).await;
                    }

                    // Enforce retention in the same run when enabled. The
                    // backup itself succeeded, so cleanup problems are logged
                    // but never fail this run.
                    if config.retention.auto_cleanup {
                        match cleanup::cleanup(&bucket, &config, false).await {
                            Ok(report) => {
                                println!(
                                    "Auto-cleanup: deleted {} chain(s), {} object(s)",
                                    report.chains_deleted, report.objects_deleted
                                );

                                if report.has_failures() {
                                    error!(
                                        chains_failed = report.chains_failed,
                                        objects_failed = report.objects_failed,
                                        "Auto-cleanup could not fully delete some chains; will retry on the next run"
                                    );
                                }

                                if let Some(notif_config) = &config.notifications {
                                    let event = BackupEvent::CleanupCompleted {
                                        chains_deleted: report.chains_deleted,
                                        objects_deleted: report.objects_deleted,
                                    };
                                    notify::dispatch(notif_config, &notifiers, &event).await;
                                }
                            }
                            Err(e) => {
                                error!(error = %e, "Auto-cleanup failed (the backup itself succeeded)");
                            }
                        }
                    }
                }
                Err(e) => {
                    // ClickHouse errors can echo the BACKUP statement with
                    // its inline S3 credentials, so the error text is
                    // redacted before it reaches logs, stderr, or
                    // notification payloads.
                    let error_text = config.redact_secrets(&e.to_string());
                    error!(error = %error_text, "Backup failed");

                    // Prefer the kind the run actually decided on (a scheduled
                    // run may auto-promote to full); fall back to the
                    // flag-implied kind when the run failed before deciding.
                    let kind = e.kind.unwrap_or(if full {
                        BackupKind::Full
                    } else {
                        BackupKind::Incremental
                    });

                    if let Some(notif_config) = &config.notifications {
                        let event = BackupEvent::backup_failed(
                            kind,
                            error_text.clone(),
                            config.clickhouse.database.clone(),
                        );
                        notify::dispatch(notif_config, &notifiers, &event).await;
                    }

                    return Err(anyhow::anyhow!(error_text));
                }
            }
        }

        Command::List { full_only } => {
            let chains =
                backup::discovery::discover_chains(&bucket, &config.s3.prefix, &retry_policy)
                    .await?;

            if chains.is_empty() {
                println!("No backups found.");
                return Ok(());
            }

            for chain in &chains {
                println!(
                    "FULL  {} | {} | {} bytes | {}",
                    chain.full_path, chain.full.timestamp, chain.full.total_size, chain.full.status
                );

                if !full_only {
                    for (path, meta) in &chain.incrementals {
                        println!(
                            "  INCR  {} | {} | {} bytes | {}",
                            path, meta.timestamp, meta.total_size, meta.status
                        );
                    }
                }
            }
        }

        Command::Status => {
            let statuses =
                backup::progress::get_recent_backups(&ch_client, 10, &retry_policy).await?;

            if statuses.is_empty() {
                println!("No backup records found in system.backups.");
                return Ok(());
            }

            println!(
                "{:<38} {:<18} {:<22} {:<22} {:>12}  ERROR",
                "ID", "STATUS", "START", "END", "SIZE"
            );
            println!("{}", "-".repeat(130));

            for s in &statuses {
                println!(
                    "{:<38} {:<18} {:<22} {:<22} {:>12}  {}",
                    s.id, s.status, s.start_time, s.end_time, s.total_size, s.error
                );
            }
        }

        Command::Check { max_age, json } => {
            let chains =
                backup::discovery::discover_chains(&bucket, &config.s3.prefix, &retry_policy)
                    .await?;
            let report = check::evaluate(&chains, chrono::Utc::now(), max_age);

            if json {
                println!("{}", serde_json::to_string(&report)?);
            } else {
                println!("{}", report.human_line());
            }

            if !report.is_healthy() {
                std::process::exit(1);
            }
        }

        Command::Cleanup { dry_run } => {
            let report = cleanup::cleanup(&bucket, &config, dry_run).await?;

            if dry_run {
                println!(
                    "Dry run: would delete {} backup chain(s)",
                    report.chains_deleted
                );
            } else {
                println!(
                    "Cleanup complete: deleted {} chain(s), {} object(s)",
                    report.chains_deleted, report.objects_deleted
                );

                if let Some(notif_config) = &config.notifications {
                    let event = BackupEvent::CleanupCompleted {
                        chains_deleted: report.chains_deleted,
                        objects_deleted: report.objects_deleted,
                    };
                    notify::dispatch(notif_config, &notifiers, &event).await;
                }

                if report.has_failures() {
                    anyhow::bail!(
                        "cleanup incomplete: {} chain(s) could not be fully deleted \
                         ({} object(s) failed); rerun cleanup to retry",
                        report.chains_failed,
                        report.objects_failed
                    );
                }
            }
        }
    }

    Ok(())
}

fn build_clickhouse_client(config: &Config) -> clickhouse::Client {
    let mut client = clickhouse::Client::default().with_url(&config.clickhouse.url);

    if let Some(user) = &config.clickhouse.user {
        client = client.with_user(user);
    }
    if let Some(password) = &config.clickhouse.password {
        client = client.with_password(password);
    }

    client
}
