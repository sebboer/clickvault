# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- Unit test suite covering chain discovery/grouping and deep-chain tracing,
  retention selection, S3 path/SQL-fragment building and escaping, notification
  filtering/serialization, and config validation.
- CI workflow (`cargo fmt --check`, `cargo clippy -D warnings`, `cargo test`)
  running on every push to `main` and every pull request.

### Changed

- CI no longer runs a release build; release-mode compilation stays covered by
  the release workflow at tag time, keeping PR feedback fast.

### Fixed

- Backup id is now read directly from the `BACKUP ... ASYNC` result instead of a
  racy `system.backups` lookup that could attach to the wrong backup.
- The database identifier is backtick-quoted and S3 credentials/paths are
  escaped in generated SQL, so special characters can no longer break the query.
- Orphaned backup data is rolled back (after retries) if the metadata sidecar
  write fails, and metadata-less backup directories are surfaced as warnings.
- Backup S3 paths use millisecond precision to avoid two backups colliding on
  the same prefix within a single second.

## [0.1.0-alpha.4] - 2026-05-23

- Latest published alpha release. See the
  [GitHub releases](https://github.com/sebboer/clickvault/releases) for the
  history of the `0.1.0-alpha.1` through `0.1.0-alpha.4` pre-releases.

[Unreleased]: https://github.com/sebboer/clickvault/compare/v0.1.0-alpha.4...HEAD
[0.1.0-alpha.4]: https://github.com/sebboer/clickvault/releases/tag/v0.1.0-alpha.4
