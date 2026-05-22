# CLAUDE.md

## Project Overview

ClickVault is a Rust CLI tool that manages ClickHouse database backups to S3 using native ClickHouse `BACKUP` SQL commands. It is cron-driven (no daemon mode) and designed for low RPO with deep incremental chaining.

## Build & Run

```bash
cargo build                           # debug build
cargo build --release                 # release build
cargo check                           # type-check without building
cargo clippy                          # lint
cargo fmt                             # format code
```

Run with a config file:
```bash
cargo run -- backup --config config.example.toml
cargo run -- list --config config.example.toml
cargo run -- status --config config.example.toml
cargo run -- cleanup --dry-run --config config.example.toml
```

## Architecture

### Module Dependency Graph

```
main.rs -> cli.rs, config.rs, s3.rs, backup/*, cleanup.rs, notify/*
backup/executor.rs -> backup/discovery.rs, backup/progress.rs, s3.rs, config.rs
backup/discovery.rs -> s3.rs, backup/mod.rs (types)
cleanup.rs -> backup/discovery.rs, s3.rs
notify/* -> config.rs (provider config)
```

### Key Design Decisions

- **Deep incremental chaining**: each incremental references the previous backup (not the full). This minimizes incremental size for frequent backups. Chain tracing logic is in `src/backup/discovery.rs:find_chain_for_incremental()`.
- **Dual S3 usage**: ClickHouse writes backup data via its native `BACKUP ... TO S3()` SQL. The `rust-s3` crate is used separately for management operations (listing, metadata, deletion). They operate on the same bucket but don't interfere.
- **ASYNC backups**: all backups use the `ASYNC` keyword so ClickHouse runs them in the background. The tool polls `system.backups` for progress (`src/backup/progress.rs`).
- **Metadata sidecar files**: `.clickvault_meta.json` is written alongside each backup in S3 to track chain linkage, kind, timestamp, size. The dot prefix prevents ClickHouse from treating it as backup data.
- **Notifications are non-fatal**: notification dispatch failures are logged as warnings but never cause the tool to exit with an error.
- **Credentials via env vars**: `CLICKVAULT_CLICKHOUSE_USER`, `CLICKVAULT_CLICKHOUSE_PASSWORD`, `CLICKVAULT_S3_ACCESS_KEY`, `CLICKVAULT_S3_SECRET_KEY` override TOML values.

### Error Handling Pattern

- `src/error.rs` defines `ClickVaultError` with `thiserror` for typed errors inside modules.
- `main.rs` uses `anyhow::Result` at the top level to propagate any error.
- `#[from]` on error variants enables `?` auto-conversion from ClickHouse, S3, IO, and JSON errors.

### S3 Path Convention

```
{prefix}/full/{YYYYMMDD}T{HHMMSS}Z/                        # ClickHouse data
{prefix}/full/{YYYYMMDD}T{HHMMSS}Z/.clickvault_meta.json   # our metadata
{prefix}/incremental/{YYYYMMDD}T{HHMMSS}Z/
{prefix}/incremental/{YYYYMMDD}T{HHMMSS}Z/.clickvault_meta.json
```

### Core Flow for `backup` Command

1. Check no backup is already running (`system.backups WHERE status = 'CREATING_BACKUP'`)
2. Discover existing chains from S3 metadata
3. Decide full vs incremental based on interval and `--full` flag
4. Build `BACKUP DATABASE ... TO S3(...) ASYNC` SQL (with `SETTINGS base_backup` for incremental)
5. Execute SQL, poll `system.backups` until complete
6. Write `.clickvault_meta.json` to S3
7. Send notifications

## Code Conventions

- Rust edition 2024
- Async runtime: tokio
- Logging: `tracing` crate with `tracing-subscriber` (env-filter). Use `info!`, `warn!`, `error!` macros.
- Config: serde `Deserialize` with `#[serde(default)]` for optional fields
- CLI: clap derive macros (`#[derive(Parser)]`, `#[derive(Subcommand)]`)
- Notification providers implement the `Notifier` trait (`src/notify/mod.rs`)

## Testing

No test suite exists yet. To test manually, you need:
- A running ClickHouse instance with a database
- S3-compatible storage (MinIO works for local testing)
- A `config.toml` pointing to both

## Areas Not Yet Implemented

- Restore subcommand (out of scope for v1)
- Unit/integration tests
- S3 batch delete (currently deletes objects one by one)
- Backup of multiple databases or individual tables (currently single database only)
