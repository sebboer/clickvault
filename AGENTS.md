# AGENTS.md

## Project Overview

ClickVault is a Rust CLI tool that manages ClickHouse database backups to S3 using native ClickHouse `BACKUP` SQL commands. It is cron-driven (no daemon mode) and designed for low RPO with deep incremental chaining.

## Publishing

The crate is published to [crates.io](https://crates.io/crates/clickvault) as an alpha release.

- **Current version**: see `version` in `Cargo.toml` (do not duplicate it here — it drifts)
- **Versioning**: semver with pre-release tags (`alpha.N` -> `beta.N` -> stable)
- **Release**: use the `/release` skill (`.claude/skills/release/`) — it updates CHANGELOG.md, bumps Cargo.toml/Cargo.lock and the README install pin, commits, tags, and pushes; the `v*` tag triggers `release.yml`
- **Dry-run**: `cargo publish --dry-run` to validate before publishing
- **Excluded from package**: `hack/`, `.claude/` (via `exclude` in Cargo.toml)

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
cargo run -- check --max-age 26h --config config.example.toml
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
- **Metadata sidecar files**: `.clickvault_meta.json` is written alongside each backup in S3 to track chain linkage, kind, timestamps (submission plus the actual `started_at`/`finished_at` window from `system.backups`), size, and a schema `version` (`METADATA_SCHEMA_VERSION` in `src/backup/mod.rs` — bump it when the shape changes; pre-versioning sidecars deserialize as version 0). The dot prefix prevents ClickHouse from treating it as backup data.
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

1. Check no backup is already running into this bucket/prefix (`system.backups WHERE status = 'CREATING_BACKUP' AND name LIKE '%/{bucket}/{prefix}/%'` — scoped so unrelated backups on a shared server don't block; check-then-act, see the guard's doc comment)
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

Unit tests cover the pure logic (no ClickHouse/S3 required): chain grouping and
deep-chain tracing (`backup/discovery.rs`), retention selection (`cleanup.rs`),
S3 path/SQL-fragment building and escaping (`s3.rs`), notification
filtering/serialization (`notify/mod.rs`), config validation and secret
redaction (`config.rs`), backup-kind decision (`backup/executor.rs`), poll
status classification (`backup/progress.rs`), staleness evaluation
(`check.rs`), and duration parsing (`cli.rs`).

```bash
cargo test              # run the unit test suite
cargo fmt --all --check # formatting
cargo clippy --all-targets -- -D warnings
```

CI (`.github/workflows/ci.yml`) runs fmt, clippy, and tests, plus an MSRV
check (`cargo check` with Rust 1.89 — keep `rust-version` in Cargo.toml, the
README requirement, and the CI job in sync) and `cargo deny check`
(RustSec advisories, license allowlist, registry sources; config in
`deny.toml`) on every push to `main` and every pull request, and weekly on a
schedule so new advisories surface without code changes.

Integration tests (`tests/integration.rs`) drive the real binary against
dockerized ClickHouse + RustFS via testcontainers (dynamic ports, automatic
cleanup). They are `#[ignore]`d in the plain test run; with Docker available:

```bash
cargo test --test integration -- --ignored --nocapture
```

CI runs them in the `integration` job. For *interactive* end-to-end work,
`hack/docker-compose.yml` (ClickHouse + RustFS) remains, with the CLI run
against `hack/config.toml`.

## Areas Not Yet Implemented

- Restore subcommand (out of scope for v1)
- Backup of multiple databases or individual tables (currently single database only)
