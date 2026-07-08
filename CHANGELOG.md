# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- Bounded retry with exponential backoff and jitter, tunable via an optional
  `[retry]` config section (default 3 attempts): S3 operations, idempotent
  ClickHouse reads (progress polling, in-progress check, status listing), and
  notification sends. Notification retries cover connect/timeout errors and
  408/429/5xx responses; other 4xx fail immediately. The `BACKUP` submit is
  never retried (not idempotent). rust-s3's built-in blind retry — which
  retried every error including 404s — is disabled in favor of this
  selective layer.
- Metadata sidecars now carry a schema `version` field for forward
  compatibility (older sidecars deserialize as version 0; sidecars from a
  newer clickvault log a warning instead of being skipped) and record the
  actual backup window (`started_at`/`finished_at` as recorded by ClickHouse
  in `system.backups`) alongside the submission timestamp.
- `check` command for backup-staleness monitoring: exits non-zero when the
  newest backup is older than `--max-age` (or none exists), with a one-line
  human summary or `--json` output for healthchecks.io/Nagios-style probes.
- Optional `[backup]` config section: `poll_interval_secs` (default 5) and
  `timeout_secs` (default 86400) replace the previously hardcoded progress
  polling constants.
- `retention.keep_days` (optional): age-based retention alongside the count
  bound — a chain is deleted only when it is beyond `keep_full_backups` *and*
  its newest backup (latest restore point, full or incremental) is older than
  `keep_days`. Protects the covered time window against bursts of forced full
  backups.
- `retention.auto_cleanup` (default `false`): run cleanup automatically after
  each successful backup instead of a separate cron entry; auto-cleanup
  problems are logged but never fail the backup run.
- Unit test suite covering chain discovery/grouping and deep-chain tracing,
  retention selection, S3 path/SQL-fragment building and escaping, notification
  filtering/serialization, config validation and secret redaction, backup-kind
  decision, poll status classification, staleness evaluation, and duration
  parsing.
- CI workflow (`cargo fmt --check`, `cargo clippy -D warnings`, `cargo test`)
  running on every push to `main` and every pull request.

### Changed

- Cleanup deletes S3 objects via the batch `DeleteObjects` API (up to 1000
  keys per request) instead of one request per object, with per-key errors
  from the batch response feeding the existing partial-failure accounting.
- Removed the misleading manual pagination loop in `list_prefixes`; rust-s3's
  `list()` already returns all result pages.
- CI no longer runs a release build; release-mode compilation stays covered by
  the release workflow at tag time, keeping PR feedback fast.

### Fixed

- Cleanup no longer acts on an incomplete view: a metadata sidecar that exists
  but cannot be read or parsed aborts the run instead of silently shifting the
  retention window (missing sidecars are still skipped as orphans).
- Cleanup handles partial deletion failures: per-object failures no longer
  abort a prefix, the metadata sidecar is deleted last (so interrupted
  deletions stay discoverable and retryable), a chain's full backup is only
  removed once all its incrementals are gone, and partial failures surface in
  the report and a non-zero exit code.
- Backup polling survives the `system.backups` row disappearing (e.g. a
  ClickHouse restart): tolerated for a few polls, then failed with a dedicated
  error and the orphaned backup data rolled back.
- `BACKUP_CANCELLED` is treated as a terminal failure, and unrecognized backup
  statuses fail fast instead of spinning until the overall timeout.
- Failure notifications report the backup kind the run actually decided on;
  an interval-promoted full backup is no longer mislabeled as incremental.
- Backup id is now read directly from the `BACKUP ... ASYNC` result instead of a
  racy `system.backups` lookup that could attach to the wrong backup.
- The database identifier is backtick-quoted and S3 credentials/paths are
  escaped in generated SQL, so special characters can no longer break the query.
- Orphaned backup data is rolled back (after retries) if the metadata sidecar
  write fails, and metadata-less backup directories are surfaced as warnings.
- Backup S3 paths use millisecond precision to avoid two backups colliding on
  the same prefix within a single second.

### Security

- Backup error text is redacted before reaching logs, stderr, or notification
  payloads: ClickHouse can echo the `BACKUP` statement with its inline S3
  credentials (verified for syntax errors), so configured secrets are masked
  with `***`.

## [0.1.0-alpha.4] - 2026-05-23

- Latest published alpha release. See the
  [GitHub releases](https://github.com/sebboer/clickvault/releases) for the
  history of the `0.1.0-alpha.1` through `0.1.0-alpha.4` pre-releases.

[Unreleased]: https://github.com/sebboer/clickvault/compare/v0.1.0-alpha.4...HEAD
[0.1.0-alpha.4]: https://github.com/sebboer/clickvault/releases/tag/v0.1.0-alpha.4
