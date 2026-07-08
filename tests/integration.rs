//! End-to-end tests driving the real `clickvault` binary against dockerized
//! ClickHouse + RustFS, started per test via testcontainers (dynamic host
//! ports, automatic cleanup). Excluded from plain `cargo test`; run with:
//!
//! ```console
//! cargo test --test integration -- --ignored --nocapture
//! ```

use std::path::PathBuf;
use std::process::Output;
use std::time::{Duration, Instant};

use s3::creds::Credentials;
use s3::{Bucket, BucketConfiguration, Region};
use testcontainers::core::{ContainerPort, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, GenericImage, ImageExt};

const CLICKHOUSE_IMAGE: (&str, &str) = ("clickhouse/clickhouse-server", "26.4");
const RUSTFS_IMAGE: (&str, &str) = ("rustfs/rustfs", "latest");
const ACCESS_KEY: &str = "clickvault-access";
const SECRET_KEY: &str = "clickvault-secret-key";
const CH_PASSWORD: &str = "clickvault";
const BUCKET: &str = "clickvault-backups";
const PREFIX: &str = "it";
const READY_DEADLINE: Duration = Duration::from_secs(90);

/// Unique per-stack identifier so parallel tests never collide on docker
/// networks or container names.
fn unique(tag: &str) -> String {
    use std::sync::atomic::{AtomicU32, Ordering};
    static N: AtomicU32 = AtomicU32::new(0);
    format!(
        "cv-it-{tag}-{}-{}",
        std::process::id(),
        N.fetch_add(1, Ordering::Relaxed)
    )
}

struct Stack {
    // Containers stop when dropped; keep them alive for the test's duration.
    _clickhouse: ContainerAsync<GenericImage>,
    _rustfs: ContainerAsync<GenericImage>,
    _dir: tempfile::TempDir,
    ch_port: u16,
    s3_port: u16,
    config: PathBuf,
    config_keep1: PathBuf,
}

impl Stack {
    async fn start() -> Stack {
        let network = unique("net");
        let rustfs_name = unique("rustfs");

        let rustfs = GenericImage::new(RUSTFS_IMAGE.0, RUSTFS_IMAGE.1)
            .with_exposed_port(ContainerPort::Tcp(9000))
            .with_wait_for(WaitFor::seconds(1))
            .with_cmd(["/data"])
            .with_env_var("RUSTFS_ACCESS_KEY", ACCESS_KEY)
            .with_env_var("RUSTFS_SECRET_KEY", SECRET_KEY)
            .with_network(&network)
            .with_container_name(&rustfs_name)
            .with_startup_timeout(READY_DEADLINE)
            .start()
            .await
            .expect("start rustfs");

        let clickhouse = GenericImage::new(CLICKHOUSE_IMAGE.0, CLICKHOUSE_IMAGE.1)
            .with_exposed_port(ContainerPort::Tcp(8123))
            .with_wait_for(WaitFor::seconds(1))
            .with_env_var("CLICKHOUSE_USER", "default")
            .with_env_var("CLICKHOUSE_PASSWORD", CH_PASSWORD)
            .with_env_var("CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT", "1")
            .with_copy_to(
                "/etc/clickhouse-server/config.d/backup.xml",
                include_str!("../hack/clickhouse/config.xml")
                    .as_bytes()
                    .to_vec(),
            )
            .with_network(&network)
            .with_startup_timeout(READY_DEADLINE)
            .start()
            .await
            .expect("start clickhouse");

        let s3_port = rustfs
            .get_host_port_ipv4(9000)
            .await
            .expect("rustfs mapped port");
        let ch_port = clickhouse
            .get_host_port_ipv4(8123)
            .await
            .expect("clickhouse mapped port");

        create_bucket_when_ready(s3_port).await;
        wait_for_clickhouse(ch_port).await;

        // Seed a database with data worth backing up.
        ch_query(ch_port, "CREATE DATABASE IF NOT EXISTS testdb").await;
        ch_query(
            ch_port,
            "CREATE TABLE testdb.events (id UInt64, msg String) ENGINE = MergeTree ORDER BY id",
        )
        .await;
        ch_query(
            ch_port,
            "INSERT INTO testdb.events SELECT number, toString(number) FROM numbers(1000)",
        )
        .await;

        let dir = tempfile::tempdir().expect("tempdir");
        let base_config = |keep: u32| {
            format!(
                r#"
[clickhouse]
url = "http://127.0.0.1:{ch_port}"
user = "default"
password = "{CH_PASSWORD}"
database = "testdb"

[s3]
endpoint = "http://127.0.0.1:{s3_port}"
clickhouse_endpoint = "http://{rustfs_name}:9000"
bucket = "{BUCKET}"
prefix = "{PREFIX}"
region = "us-east-1"
access_key = "{ACCESS_KEY}"
secret_key = "{SECRET_KEY}"
path_style = true

[backup]
poll_interval_secs = 1

[schedule]
full_backup_interval_days = 7

[retention]
keep_full_backups = {keep}
"#
            )
        };

        let config = dir.path().join("config.toml");
        std::fs::write(&config, base_config(3)).expect("write config");
        let config_keep1 = dir.path().join("config-keep1.toml");
        std::fs::write(&config_keep1, base_config(1)).expect("write keep1 config");

        Stack {
            _clickhouse: clickhouse,
            _rustfs: rustfs,
            _dir: dir,
            ch_port,
            s3_port,
            config,
            config_keep1,
        }
    }

    fn bucket(&self) -> Box<Bucket> {
        bucket_handle(self.s3_port)
    }

    /// All object keys under `prefix` (relative to the bucket).
    async fn keys_under(&self, prefix: &str) -> Vec<String> {
        let results = self
            .bucket()
            .list(prefix.to_string(), None)
            .await
            .expect("list bucket");
        results
            .iter()
            .flat_map(|r| r.contents.iter().map(|o| o.key.clone()))
            .collect()
    }

    /// Directory names (timestamps) under e.g. "it/full/".
    async fn backup_dirs(&self, segment: &str) -> Vec<String> {
        let results = self
            .bucket()
            .list(format!("{PREFIX}/{segment}/"), Some("/".to_string()))
            .await
            .expect("list prefixes");
        results
            .iter()
            .flat_map(|r| r.common_prefixes.iter().flatten())
            .map(|cp| cp.prefix.clone())
            .collect()
    }

    async fn sidecar(&self, backup_dir: &str) -> serde_json::Value {
        let response = self
            .bucket()
            .get_object(format!("{backup_dir}.clickvault_meta.json"))
            .await
            .expect("read sidecar");
        serde_json::from_slice(response.as_slice()).expect("parse sidecar")
    }
}

fn bucket_handle(s3_port: u16) -> Box<Bucket> {
    let region = Region::Custom {
        region: "us-east-1".into(),
        endpoint: format!("http://127.0.0.1:{s3_port}"),
    };
    let credentials = Credentials::new(Some(ACCESS_KEY), Some(SECRET_KEY), None, None, None)
        .expect("credentials");
    Bucket::new(BUCKET, region, credentials)
        .expect("bucket handle")
        .with_path_style()
}

async fn create_bucket_when_ready(s3_port: u16) {
    let region = Region::Custom {
        region: "us-east-1".into(),
        endpoint: format!("http://127.0.0.1:{s3_port}"),
    };
    let credentials = Credentials::new(Some(ACCESS_KEY), Some(SECRET_KEY), None, None, None)
        .expect("credentials");

    let deadline = Instant::now() + READY_DEADLINE;
    loop {
        match Bucket::create_with_path_style(
            BUCKET,
            region.clone(),
            credentials.clone(),
            BucketConfiguration::default(),
        )
        .await
        {
            Ok(_) => return,
            Err(e) if Instant::now() > deadline => panic!("rustfs never became ready: {e}"),
            Err(_) => tokio::time::sleep(Duration::from_millis(500)).await,
        }
    }
}

async fn wait_for_clickhouse(ch_port: u16) {
    let client = reqwest::Client::new();
    let deadline = Instant::now() + READY_DEADLINE;
    loop {
        let ok = client
            .post(format!(
                "http://127.0.0.1:{ch_port}/?password={CH_PASSWORD}"
            ))
            .body("SELECT 1")
            .send()
            .await
            .is_ok_and(|r| r.status().is_success());
        if ok {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "clickhouse never became ready on port {ch_port}"
        );
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

async fn ch_query(ch_port: u16, sql: &str) -> String {
    let response = reqwest::Client::new()
        .post(format!(
            "http://127.0.0.1:{ch_port}/?password={CH_PASSWORD}"
        ))
        .body(sql.to_string())
        .send()
        .await
        .expect("clickhouse request");
    let status = response.status();
    let body = response.text().await.expect("clickhouse body");
    assert!(
        status.is_success(),
        "query failed ({status}): {body}\n{sql}"
    );
    body
}

/// Runs the real clickvault binary (built by cargo for this test run).
fn clickvault(config: &PathBuf, args: &[&str]) -> Output {
    std::process::Command::new(env!("CARGO_BIN_EXE_clickvault"))
        .arg("--config")
        .arg(config)
        .args(args)
        .output()
        .expect("spawn clickvault")
}

fn stdout(output: &Output) -> String {
    String::from_utf8_lossy(&output.stdout).into_owned()
}

fn assert_success(output: &Output, context: &str) {
    assert!(
        output.status.success(),
        "{context} failed (status {:?})\nstdout: {}\nstderr: {}",
        output.status,
        stdout(output),
        String::from_utf8_lossy(&output.stderr)
    );
}

#[tokio::test]
#[ignore = "requires docker"]
async fn backup_chain_lifecycle() {
    let stack = Stack::start().await;

    // Full backup.
    let out = clickvault(&stack.config, &["backup", "--full"]);
    assert_success(&out, "full backup");
    assert!(stdout(&out).contains("Backup completed: full"));

    let fulls = stack.backup_dirs("full").await;
    assert_eq!(fulls.len(), 1, "expected one full backup dir: {fulls:?}");
    let sidecar = stack.sidecar(&fulls[0]).await;
    assert_eq!(sidecar["version"], 1);
    assert_eq!(sidecar["kind"], "full");
    assert_eq!(sidecar["status"], "BACKUP_CREATED");
    assert!(sidecar["started_at"].is_string(), "started_at recorded");
    assert!(sidecar["base_backup_path"].is_null());

    // Incremental chains off the full.
    ch_query(
        stack.ch_port,
        "INSERT INTO testdb.events SELECT number + 1000, toString(number) FROM numbers(500)",
    )
    .await;
    let out = clickvault(&stack.config, &["backup"]);
    assert_success(&out, "incremental backup");
    assert!(stdout(&out).contains("Backup completed: incremental"));

    let incrs = stack.backup_dirs("incremental").await;
    assert_eq!(incrs.len(), 1, "expected one incremental dir: {incrs:?}");
    let sidecar = stack.sidecar(&incrs[0]).await;
    assert_eq!(sidecar["kind"], "incremental");
    assert_eq!(
        sidecar["base_backup_path"].as_str(),
        Some(fulls[0].as_str()),
        "incremental must chain off the full backup"
    );

    // list shows the chain.
    let out = clickvault(&stack.config, &["list"]);
    assert_success(&out, "list");
    let listing = stdout(&out);
    assert_eq!(listing.matches("FULL ").count(), 1, "{listing}");
    assert_eq!(listing.matches("INCR ").count(), 1, "{listing}");

    // check reports a fresh chain.
    let out = clickvault(&stack.config, &["check", "--max-age", "1h", "--json"]);
    assert_success(&out, "check");
    let report: serde_json::Value = serde_json::from_str(stdout(&out).trim()).expect("check json");
    assert_eq!(report["status"], "ok");
    assert_eq!(report["chains"], 1);

    // A forced second full starts a new chain; keep=1 cleanup removes the old.
    let out = clickvault(&stack.config, &["backup", "--full"]);
    assert_success(&out, "second full backup");

    let out = clickvault(&stack.config_keep1, &["cleanup", "--dry-run"]);
    assert_success(&out, "cleanup dry-run");
    assert!(
        stdout(&out).contains("would delete 1 backup chain(s)"),
        "{}",
        stdout(&out)
    );

    let before = stack.keys_under(PREFIX).await.len();
    let out = clickvault(&stack.config_keep1, &["cleanup"]);
    assert_success(&out, "cleanup");
    let after = stack.keys_under(PREFIX).await.len();

    let cleanup_line = stdout(&out);
    let reported: usize = cleanup_line
        .split("chain(s), ")
        .nth(1)
        .and_then(|s| s.split(" object(s)").next())
        .and_then(|s| s.parse().ok())
        .unwrap_or_else(|| panic!("unparseable cleanup report: {cleanup_line}"));
    assert_eq!(
        before - after,
        reported,
        "reported deletions must match the actual bucket delta"
    );

    // Old chain fully gone; only the new full remains.
    assert!(stack.keys_under(&fulls[0]).await.is_empty());
    assert!(stack.keys_under(&incrs[0]).await.is_empty());
    let out = clickvault(&stack.config, &["list"]);
    assert_success(&out, "list after cleanup");
    let listing = stdout(&out);
    assert_eq!(listing.matches("FULL ").count(), 1, "{listing}");
    assert_eq!(listing.matches("INCR ").count(), 0, "{listing}");
}

#[tokio::test]
#[ignore = "requires docker"]
async fn check_reports_missing_and_stale() {
    let stack = Stack::start().await;

    // No backups yet.
    let out = clickvault(&stack.config, &["check", "--max-age", "1h", "--json"]);
    assert!(!out.status.success(), "check must fail with no backups");
    let report: serde_json::Value = serde_json::from_str(stdout(&out).trim()).expect("check json");
    assert_eq!(report["status"], "missing");

    // A backup older than max-age is stale.
    let out = clickvault(&stack.config, &["backup", "--full"]);
    assert_success(&out, "full backup");
    tokio::time::sleep(Duration::from_secs(2)).await;

    let out = clickvault(&stack.config, &["check", "--max-age", "1s", "--json"]);
    assert!(!out.status.success(), "check must fail when stale");
    let report: serde_json::Value = serde_json::from_str(stdout(&out).trim()).expect("check json");
    assert_eq!(report["status"], "stale");

    // And within max-age it is healthy again.
    let out = clickvault(&stack.config, &["check", "--max-age", "1h", "--json"]);
    assert_success(&out, "check fresh");
    let report: serde_json::Value = serde_json::from_str(stdout(&out).trim()).expect("check json");
    assert_eq!(report["status"], "ok");
}
