# ClickVault

A Rust CLI tool for managing ClickHouse database backups to S3 using native ClickHouse `BACKUP` SQL commands.

ClickVault automates full and incremental backups with deep chaining, progress monitoring, retention-based cleanup, and notifications.

## Features

- **Full & incremental backups** via ClickHouse's native `BACKUP DATABASE ... TO S3(...)` SQL
- **Deep incremental chaining** — each incremental references the previous backup, keeping deltas small for low RPO
- **Automatic full/incremental decision** — based on a configurable interval (e.g., full every 7 days)
- **Progress monitoring** — polls `system.backups` during async backups and reports size/status
- **Retention cleanup** — deletes expired backup chains from S3 based on configurable retention
- **Notifications** — Slack webhooks and generic HTTP webhooks on success/failure
- **TOML configuration** with environment variable overrides for credentials
- **Cron-friendly** — runs once per invocation, safe to call from cron or systemd timers

## Requirements

- Rust 1.89+ (edition 2024; enforced in CI and via `rust-version` in Cargo.toml)
- ClickHouse server with `BACKUP`/`RESTORE` support (v22.8+)
- S3-compatible storage (AWS S3, RustFS, etc.)

## Installation

### From crates.io

Pre-releases must be installed with an explicit version — check
[crates.io/crates/clickvault](https://crates.io/crates/clickvault) for the latest:

```bash
cargo install clickvault --version 0.1.0-alpha.4
```

### Docker

```bash
docker pull ghcr.io/sebboer/clickvault:latest
```

Multi-arch image available for `linux/amd64` and `linux/arm64`.

### From source

```bash
git clone https://github.com/sebboer/clickvault.git
cd clickvault
cargo build --release
```

The binary will be at `target/release/clickvault`.

## Quick Start

1. Copy and edit the example config:

```bash
cp config.example.toml config.toml
# Edit config.toml with your ClickHouse and S3 settings
```

2. Run a full backup:

```bash
clickvault backup --full --config config.toml
```

3. Run an incremental backup (auto-detected if a full exists):

```bash
clickvault backup --config config.toml
```

4. List all backups:

```bash
clickvault list --config config.toml
```

5. Check backup status in ClickHouse:

```bash
clickvault status --config config.toml
```

6. Clean up old backups (dry run first):

```bash
clickvault cleanup --dry-run --config config.toml
clickvault cleanup --config config.toml
```

## Configuration

ClickVault uses a TOML configuration file. See [config.example.toml](config.example.toml) for a fully commented example.

### Sections

#### `[clickhouse]`

| Key        | Required | Description                   |
| ---------- | -------- | ----------------------------- |
| `url`      | yes      | ClickHouse HTTP interface URL |
| `user`     | no       | Username (default: `default`) |
| `password` | no       | Password                      |
| `database` | yes      | Database to back up           |

#### `[s3]`

| Key                   | Required | Description                                                                                         |
| --------------------- | -------- | --------------------------------------------------------------------------------------------------- |
| `endpoint`            | yes      | S3 endpoint URL (as reachable from where clickvault runs)                                            |
| `clickhouse_endpoint` | no       | S3 endpoint as reachable from the ClickHouse server, e.g. a Docker-internal URL (default: `endpoint`) |
| `bucket`              | yes      | Bucket name                                                                                          |
| `prefix`              | no       | Key prefix for all backups (default: empty)                                                          |
| `region`              | yes      | S3 region                                                                                            |
| `access_key`          | no       | S3 access key                                                                                        |
| `secret_key`          | no       | S3 secret key                                                                                        |
| `path_style`          | no       | Use path-style S3 URLs — required for MinIO/RustFS (default: `false`)                                 |

#### `[backup]`

| Key                  | Required | Description                                                        |
| -------------------- | -------- | ------------------------------------------------------------------ |
| `poll_interval_secs` | no       | Seconds between backup progress polls (default: `5`)               |
| `timeout_secs`       | no       | Give up on a backup after this many seconds (default: `86400`/24h) |

#### `[retry]`

| Key             | Required | Description                                                                  |
| --------------- | -------- | ---------------------------------------------------------------------------- |
| `attempts`      | no       | Total attempts per S3/ClickHouse-read/notification operation (default: `3`)  |
| `base_delay_ms` | no       | First backoff, doubling per attempt with jitter (default: `200`)              |
| `max_delay_ms`  | no       | Ceiling for a single backoff sleep (default: `5000`)                          |

#### `[schedule]`

| Key                         | Required | Description                           |
| --------------------------- | -------- | ------------------------------------- |
| `full_backup_interval_days` | yes      | Days between full backups (e.g., `7`) |

#### `[retention]`

| Key                 | Required | Description                                                                             |
| ------------------- | -------- | ---------------------------------------------------------------------------------------- |
| `keep_full_backups` | yes      | Number of full backup chains to retain (minimum `1`)                                     |
| `keep_days`         | no       | Never delete a chain whose newest backup is younger than this many days (default: unset) |
| `auto_cleanup`      | no       | Run cleanup after each successful backup (default: `false`)                              |

#### `[notifications]`

| Key          | Required | Description                                   |
| ------------ | -------- | --------------------------------------------- |
| `on_success` | no       | Send notifications on success (default: true) |
| `on_failure` | no       | Send notifications on failure (default: true) |
| `providers`  | no       | List of notification providers                |

#### `[[notifications.providers]]`

Slack:

```toml
[[notifications.providers]]
type = "slack"
webhook_url = "https://hooks.slack.com/services/T.../B.../xxx"
```

Generic webhook:

```toml
[[notifications.providers]]
type = "webhook"
url = "https://monitoring.example.com/api/alerts"
method = "POST"
headers = { Authorization = "Bearer token123" }
```

### Environment Variable Overrides

Credentials can be provided or overridden via environment variables. These take precedence over values in the TOML config:

| Environment Variable             | Overrides             |
| -------------------------------- | --------------------- |
| `CLICKVAULT_CLICKHOUSE_USER`     | `clickhouse.user`     |
| `CLICKVAULT_CLICKHOUSE_PASSWORD` | `clickhouse.password` |
| `CLICKVAULT_S3_ACCESS_KEY`       | `s3.access_key`       |
| `CLICKVAULT_S3_SECRET_KEY`       | `s3.secret_key`       |

## CLI Reference

```
clickvault [OPTIONS] <COMMAND>

Options:
  -c, --config <PATH>       Path to config file [default: /etc/clickvault/config.toml]
  -l, --log-level <LEVEL>   Log level: trace, debug, info, warn, error [default: info]
  -h, --help                Print help

Commands:
  backup    Run a backup (auto-detects full vs incremental)
  list      List known backups in S3
  status    Show status of running and recent backups
  check     Check that the newest backup is fresh enough (exit code for monitoring)
  cleanup   Clean up expired backup chains
```

### `clickvault backup`

```
Options:
  --full     Force a full backup regardless of schedule
  --force    Skip the in-progress backup check (use when a previous backup is stuck)
```

Without `--full`, the tool checks S3 for the latest full backup. If it's older than `full_backup_interval_days`, a full backup is created. Otherwise, an incremental backup is created chaining off the most recent backup.

A run refuses to start while another backup is in `CREATING_BACKUP` state on the server; `--force` bypasses that guard, e.g. after a crashed run left a stuck entry in `system.backups`.

### `clickvault list`

```
Options:
  --full-only    Show only full backups
```

### `clickvault check`

```
Options:
  --max-age <DURATION>    Maximum acceptable age of the newest backup (e.g. 90s, 30m, 26h, 2d)
  --json                  Print the summary as JSON
```

Exits `0` when the newest backup is younger than `--max-age`, non-zero when it is older or no backups exist. Designed for pull-based monitoring: cron silently dying is otherwise invisible until a restore is needed.

```console
$ clickvault check --max-age 26h
OK: last backup incremental at 2026-07-07T22:36:32Z (age 2h13m, max 26h), 2 chain(s)

$ clickvault check --max-age 26h --json
{"status":"ok","kind":"incremental","timestamp":"2026-07-07T22:36:32Z","age_secs":8012,"max_age_secs":93600,"chains":2}
```

### `clickvault cleanup`

```
Options:
  --dry-run    Show what would be deleted without actually deleting
```

## How It Works

### Backup Strategy

ClickVault uses **deep incremental chaining**. Each incremental backup references the previous backup (not the full):

```
Full (Day 0)
  -> Incremental (Day 0, 12:00) base: Full
       -> Incremental (Day 0, 18:00) base: Day 0 12:00
            -> Incremental (Day 1, 06:00) base: Day 0 18:00
                 -> ...
```

This keeps each incremental as small as possible (only the delta since the last backup), which is ideal when running backups multiple times per day for a low RPO.

When the configured interval elapses, a new full backup starts a fresh chain.

### S3 Path Layout

```
{prefix}/full/{YYYYMMDD}T{HHMMSS}Z/                        # ClickHouse backup data
{prefix}/full/{YYYYMMDD}T{HHMMSS}Z/.clickvault_meta.json   # ClickVault metadata
{prefix}/incremental/{YYYYMMDD}T{HHMMSS}Z/
{prefix}/incremental/{YYYYMMDD}T{HHMMSS}Z/.clickvault_meta.json
```

The metadata sidecar file tracks a schema version, backup kind, submission timestamp, the actual backup window (`started_at`/`finished_at` as recorded by ClickHouse), chain linkage (`base_backup_path`), size, and status.

### Retention

Cleanup operates on entire chains. With `keep_full_backups = 4`, the 4 most recent full backups and all their incrementals are kept. Older chains are deleted (incrementals first, then the full).

With `keep_days` set, a chain is only deleted when it exceeds **both** bounds: beyond the `keep_full_backups` newest chains *and* its newest backup (the latest restore point the chain provides, full or incremental) older than `keep_days`. This protects the covered time window when extra full backups are created in a burst — without it, three `backup --full` runs would push a month of history out of a `keep_full_backups = 4` window.

### Cron Setup Example

```cron
# Incremental backup every 4 hours (auto-promotes to full when interval elapses)
0 */4 * * * /usr/local/bin/clickvault backup --config /etc/clickvault/config.toml

# Cleanup once daily at 03:00
0 3 * * * /usr/local/bin/clickvault cleanup --config /etc/clickvault/config.toml

# Alert when backups silently stop (pair with healthchecks.io, Nagios, etc.)
*/30 * * * * /usr/local/bin/clickvault check --max-age 26h --config /etc/clickvault/config.toml || notify-oncall
```

Retention is only enforced when `cleanup` runs — without the cleanup cron entry, backups grow unbounded. Alternatively, set `auto_cleanup = true` under `[retention]` to run cleanup after each successful backup and skip the separate cron entry (auto-cleanup problems are logged but never fail the backup run).

### Docker Usage

Mount your config file and run any command:

```bash
# Run a full backup
docker run --rm \
  -v /path/to/config.toml:/etc/clickvault/config.toml:ro \
  ghcr.io/sebboer/clickvault:latest \
  backup --full

# Run an incremental backup
docker run --rm \
  -v /path/to/config.toml:/etc/clickvault/config.toml:ro \
  ghcr.io/sebboer/clickvault:latest \
  backup

# List backups
docker run --rm \
  -v /path/to/config.toml:/etc/clickvault/config.toml:ro \
  ghcr.io/sebboer/clickvault:latest \
  list

# Cleanup with dry run
docker run --rm \
  -v /path/to/config.toml:/etc/clickvault/config.toml:ro \
  ghcr.io/sebboer/clickvault:latest \
  cleanup --dry-run
```

Credentials can be passed via environment variables instead of the config file:

```bash
docker run --rm \
  -e CLICKVAULT_CLICKHOUSE_PASSWORD=secret \
  -e CLICKVAULT_S3_ACCESS_KEY=AKIA... \
  -e CLICKVAULT_S3_SECRET_KEY=... \
  -v /path/to/config.toml:/etc/clickvault/config.toml:ro \
  ghcr.io/sebboer/clickvault:latest \
  backup
```

## Architecture

```
src/
  main.rs              Entry point, CLI dispatch, notification wiring
  cli.rs               Clap argument definitions
  config.rs            TOML deserialization, env overrides, validation
  error.rs             Typed error enum (thiserror)
  s3.rs                S3 bucket construction, path conventions, metadata I/O
  cleanup.rs           Retention enforcement
  backup/
    mod.rs             Shared types: BackupKind, BackupMetadata, BackupChain
    discovery.rs       S3-based backup chain discovery
    executor.rs        Core backup logic: SQL generation, execution, polling
    progress.rs        system.backups polling for async backup progress
  notify/
    mod.rs             Notifier trait, BackupEvent enum, dispatch logic
    slack.rs           Slack webhook implementation
    webhook.rs         Generic HTTP webhook implementation
```

## License

MIT
