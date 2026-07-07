use std::time::Duration;

use chrono::{DateTime, Utc};
use serde::Serialize;

use crate::backup::{BackupChain, BackupKind};

#[derive(Debug, Clone, Copy, PartialEq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum CheckStatus {
    Ok,
    Stale,
    Missing,
}

/// Summary of a staleness check, printable as one line or as JSON.
#[derive(Debug, Serialize)]
pub struct CheckReport {
    pub status: CheckStatus,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub kind: Option<BackupKind>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub timestamp: Option<DateTime<Utc>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub age_secs: Option<u64>,
    pub max_age_secs: u64,
    pub chains: usize,
}

impl CheckReport {
    pub fn is_healthy(&self) -> bool {
        self.status == CheckStatus::Ok
    }

    /// One-line human-readable summary.
    pub fn human_line(&self) -> String {
        match (self.kind, self.timestamp, self.age_secs) {
            (Some(kind), Some(ts), Some(age)) => {
                let verdict = if self.is_healthy() { "OK" } else { "STALE" };
                format!(
                    "{verdict}: last backup {kind} at {} (age {}, max {}), {} chain(s)",
                    ts.format("%Y-%m-%dT%H:%M:%SZ"),
                    format_age(age),
                    format_age(self.max_age_secs),
                    self.chains
                )
            }
            _ => format!(
                "MISSING: no backups found (max {})",
                format_age(self.max_age_secs)
            ),
        }
    }
}

/// Evaluates backup freshness against `max_age`.
///
/// `chains` must be sorted newest-first (as returned by `discover_chains`);
/// the newest chain's latest backup is the most recent backup overall, since
/// new incrementals always chain off the newest chain. Discovery only sees
/// backups whose metadata sidecar was written, i.e. successful ones.
pub fn evaluate(chains: &[BackupChain], now: DateTime<Utc>, max_age: Duration) -> CheckReport {
    let max_age_secs = max_age.as_secs();

    let Some((_, latest)) = chains.first().map(|chain| chain.latest()) else {
        return CheckReport {
            status: CheckStatus::Missing,
            kind: None,
            timestamp: None,
            age_secs: None,
            max_age_secs,
            chains: 0,
        };
    };

    // Clock skew between this host and the backup writer can make the age
    // marginally negative; clamp to zero.
    let age_secs = (now - latest.timestamp).num_seconds().max(0) as u64;
    let status = if age_secs <= max_age_secs {
        CheckStatus::Ok
    } else {
        CheckStatus::Stale
    };

    CheckReport {
        status,
        kind: Some(latest.kind),
        timestamp: Some(latest.timestamp),
        age_secs: Some(age_secs),
        max_age_secs,
        chains: chains.len(),
    }
}

/// Formats seconds as a compact duration with at most two units,
/// e.g. "50s", "3m20s", "3h42m", "1d2h".
fn format_age(secs: u64) -> String {
    if secs >= 86_400 {
        let d = secs / 86_400;
        let h = (secs % 86_400) / 3600;
        if h > 0 {
            format!("{d}d{h}h")
        } else {
            format!("{d}d")
        }
    } else if secs >= 3600 {
        let h = secs / 3600;
        let m = (secs % 3600) / 60;
        if m > 0 {
            format!("{h}h{m}m")
        } else {
            format!("{h}h")
        }
    } else if secs >= 60 {
        let m = secs / 60;
        let s = secs % 60;
        if s > 0 {
            format!("{m}m{s}s")
        } else {
            format!("{m}m")
        }
    } else {
        format!("{secs}s")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backup::BackupMetadata;
    use chrono::TimeZone;

    fn t0() -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 1, 10, 12, 0, 0).unwrap()
    }

    fn md(kind: BackupKind, ts: DateTime<Utc>) -> BackupMetadata {
        BackupMetadata {
            backup_id: "id".into(),
            kind,
            timestamp: ts,
            base_backup_path: None,
            status: "BACKUP_CREATED".into(),
            total_size: 0,
            database: "db".into(),
        }
    }

    fn chain(full_ts: DateTime<Utc>, incr_ts: Option<DateTime<Utc>>) -> BackupChain {
        BackupChain {
            full_path: format!("full/{full_ts}/"),
            full: md(BackupKind::Full, full_ts),
            incrementals: incr_ts
                .map(|ts| {
                    vec![(
                        format!("incremental/{ts}/"),
                        md(BackupKind::Incremental, ts),
                    )]
                })
                .unwrap_or_default(),
        }
    }

    #[test]
    fn missing_when_no_chains() {
        let report = evaluate(&[], t0(), Duration::from_secs(3600));
        assert_eq!(report.status, CheckStatus::Missing);
        assert!(!report.is_healthy());
        assert!(report.human_line().starts_with("MISSING"));
    }

    #[test]
    fn ok_within_max_age_uses_latest_incremental() {
        let full_ts = t0() - chrono::Duration::hours(10);
        let incr_ts = t0() - chrono::Duration::hours(2);
        let chains = vec![chain(full_ts, Some(incr_ts))];

        let report = evaluate(&chains, t0(), Duration::from_secs(26 * 3600));
        assert_eq!(report.status, CheckStatus::Ok);
        assert_eq!(report.kind, Some(BackupKind::Incremental));
        assert_eq!(report.age_secs, Some(2 * 3600));
        assert_eq!(report.chains, 1);
        assert!(
            report
                .human_line()
                .starts_with("OK: last backup incremental")
        );
    }

    #[test]
    fn stale_beyond_max_age() {
        let chains = vec![chain(t0() - chrono::Duration::days(3), None)];
        let report = evaluate(&chains, t0(), Duration::from_secs(26 * 3600));
        assert_eq!(report.status, CheckStatus::Stale);
        assert!(!report.is_healthy());
        assert!(report.human_line().starts_with("STALE"));
    }

    #[test]
    fn boundary_age_equal_to_max_is_ok() {
        let chains = vec![chain(t0() - chrono::Duration::hours(1), None)];
        let report = evaluate(&chains, t0(), Duration::from_secs(3600));
        assert_eq!(report.status, CheckStatus::Ok);
    }

    #[test]
    fn negative_age_from_clock_skew_clamps_to_zero() {
        let chains = vec![chain(t0() + chrono::Duration::seconds(30), None)];
        let report = evaluate(&chains, t0(), Duration::from_secs(3600));
        assert_eq!(report.age_secs, Some(0));
        assert_eq!(report.status, CheckStatus::Ok);
    }

    #[test]
    fn json_shape_omits_absent_fields() {
        let report = evaluate(&[], t0(), Duration::from_secs(60));
        let json = serde_json::to_value(&report).unwrap();
        assert_eq!(json["status"], "missing");
        assert_eq!(json["chains"], 0);
        assert_eq!(json["max_age_secs"], 60);
        assert!(json.get("kind").is_none());

        let chains = vec![chain(t0() - chrono::Duration::minutes(5), None)];
        let json =
            serde_json::to_value(evaluate(&chains, t0(), Duration::from_secs(3600))).unwrap();
        assert_eq!(json["status"], "ok");
        assert_eq!(json["kind"], "full");
        assert_eq!(json["age_secs"], 300);
    }

    #[test]
    fn format_age_uses_at_most_two_units() {
        assert_eq!(format_age(50), "50s");
        assert_eq!(format_age(200), "3m20s");
        assert_eq!(format_age(3600), "1h");
        assert_eq!(format_age(13_320), "3h42m");
        assert_eq!(format_age(93_600), "1d2h");
        assert_eq!(format_age(86_400), "1d");
    }
}
