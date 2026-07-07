---
name: release
description: Cut a new release — updates CHANGELOG.md, sets the version in Cargo.toml and Cargo.lock, commits, tags, and pushes. The v* tag triggers release.yml (GitHub release, crates.io publish, multi-arch Docker image).
disable-model-invocation: true
allowed-tools: Bash(git *) Bash(cargo *) Bash(grep *) Read Edit Glob Grep
---

## Context

- Current Cargo.toml version: !`grep -m1 '^version' Cargo.toml`
- Latest git tags: !`git tag --sort=-version:refname -l 'v*' | head`
- Current branch: !`git branch --show-current`
- Git status: !`git status --short`

## Release v$ARGUMENTS

You are cutting release **v$ARGUMENTS** of the `clickvault` crate. `$ARGUMENTS` is the semver version *without* the leading `v` (e.g. `0.1.0-alpha.5`). Follow these steps in order. Stop and ask the user if anything looks wrong.

### 1. Pre-flight checks

- Confirm `$ARGUMENTS` is a valid semver version and is newer than the current Cargo.toml version. Pre-release versions use `-alpha.N` / `-beta.N` / `-rc.N` (these are published as GitHub pre-releases automatically).
- Verify the working tree is clean (aside from what this skill will create). If dirty, stop and ask.
- Verify you are on `main` and up to date with `origin/main` — releases are cut from `main`. If not, stop and ask.
- Verify the `v$ARGUMENTS` tag does not already exist locally or on origin. If it does, stop and tell the user.
- Run the same gates CI enforces and stop if any fail:
  - `cargo fmt --all --check`
  - `cargo clippy --all-targets -- -D warnings`
  - `cargo test`
- Validate packaging with `cargo publish --dry-run`. Stop if it fails — the tag push publishes to crates.io for real, and that cannot be undone.

### 2. Update CHANGELOG.md

- Read `CHANGELOG.md`.
- Move the content under `## [Unreleased]` into a new `## [$ARGUMENTS] - YYYY-MM-DD` section (use today's date).
- Leave an empty `## [Unreleased]` section at the top for future changes.
- Update the link references at the bottom: point `[Unreleased]` at `compare/v$ARGUMENTS...HEAD` and add a `[$ARGUMENTS]` release link.
- Do NOT touch older version sections.

### 3. Set the release version

- In `Cargo.toml`, change the package `version` (the first `^version =` line) to `$ARGUMENTS`.
- Run `cargo check` — this updates the `clickvault` entry in `Cargo.lock` to match and confirms the bumped manifest still builds.
- Confirm both `Cargo.toml` and `Cargo.lock` now show `$ARGUMENTS`. `release.yml` fails the release if the tag and the Cargo.toml version disagree.

### 4. Commit and tag

- Stage `CHANGELOG.md`, `Cargo.toml`, and `Cargo.lock`.
- Commit with message: `chore(release): v$ARGUMENTS`
- Create an annotated tag: `git tag -a v$ARGUMENTS -m "Release v$ARGUMENTS"`

### 5. Push

- Show the user exactly what will be pushed (the release commit and the `v$ARGUMENTS` tag) and ask for confirmation before pushing.
- Push: `git push origin main && git push origin v$ARGUMENTS`
- The tag push triggers `.github/workflows/release.yml`, which verifies the version, creates the GitHub release (pre-release for alpha/beta/rc), publishes to crates.io, and builds/pushes the multi-arch Docker image. Point the user at the Actions tab to watch it.

### Rules

- Never force-push.
- Never skip git hooks.
- There is no post-release "next dev version" bump — unlike Maven SNAPSHOTs, the Cargo.toml version stays at the released version until the next release.
- If any step fails, stop and report — do not continue.
