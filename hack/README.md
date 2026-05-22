# Local Development Setup

## Services

| Service         | Port | Description                 |
| --------------- | ---- | --------------------------- |
| ClickHouse HTTP | 8123 | ClickHouse HTTP interface   |
| ClickHouse TCP  | 9000 | ClickHouse native interface |
| RustFS S3 API   | 9002 | S3-compatible API           |
| RustFS Console  | 9001 | RustFS web UI               |

[RustFS](https://github.com/rustfs/rustfs) is an open-source, S3-compatible object storage written in Rust (Apache 2.0).

## Quick Start

Start the services:

```bash
cd hack
docker compose up -d
```

Wait for healthchecks to pass:

```bash
docker compose ps
```

Run clickvault against the local stack:

```bash
# From the repo root
cargo run -- backup --full --config hack/config.toml
cargo run -- list --config hack/config.toml
cargo run -- status --config hack/config.toml
cargo run -- cleanup --dry-run --config hack/config.toml
```

## Accessing Services

**ClickHouse** — query directly:

```bash
curl "http://localhost:8123/?user=default&password=clickvault" -d "SELECT count() FROM testdb.events"
```

**RustFS Console** — browse backups at [http://localhost:9001](http://localhost:9001) (login: `clickvault-access` / `clickvault-secret-key`).

## Test Data

The `clickhouse/init.sql` script creates a `testdb` database with two tables:

- `testdb.events` — 5 sample click/view/signup events
- `testdb.users` — 3 sample users

To add more data between backups (to see incremental deltas):

```bash
curl "http://localhost:8123/?user=default&password=clickvault" -d \
  "INSERT INTO testdb.events (id, event_type, payload) VALUES (100, 'test', '{\"source\": \"manual\"}')"
```

## Teardown

```bash
cd hack
docker compose down -v   # -v removes volumes (data)
```
