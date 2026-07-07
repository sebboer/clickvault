use std::fmt::Display;
use std::future::Future;
use std::time::{Duration, SystemTime};

use tracing::warn;

/// Bounded retry with exponential backoff and full jitter.
#[derive(Debug, Clone)]
pub struct RetryPolicy {
    /// Total attempts per operation (1 = no retry).
    pub attempts: u32,
    pub base_delay: Duration,
    pub max_delay: Duration,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            attempts: 3,
            base_delay: Duration::from_millis(200),
            max_delay: Duration::from_secs(5),
        }
    }
}

/// Upper bound for the sleep before retry number `attempt` (0-based):
/// `min(max_delay, base_delay * 2^attempt)`, overflow-safe.
fn backoff_cap(policy: &RetryPolicy, attempt: u32) -> Duration {
    let factor = 1u32.checked_shl(attempt).unwrap_or(u32::MAX);
    policy
        .base_delay
        .checked_mul(factor)
        .unwrap_or(policy.max_delay)
        .min(policy.max_delay)
}

/// Full jitter: a pseudo-random duration in `[0, cap]`. Entropy comes from
/// the subsecond clock — this is a single-process CLI run from cron, not a
/// thundering herd, so a real RNG dependency isn't warranted.
fn jittered(cap: Duration) -> Duration {
    let nanos = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map(|d| d.subsec_nanos() as u64)
        .unwrap_or(0);
    let cap_ms = cap.as_millis() as u64;
    Duration::from_millis(nanos % (cap_ms + 1))
}

/// The jittered sleep before retry number `attempt` (0-based). Exposed for
/// call sites that need a custom retry loop (e.g. partial batch deletes).
pub fn backoff_delay(policy: &RetryPolicy, attempt: u32) -> Duration {
    jittered(backoff_cap(policy, attempt))
}

/// Runs `op` up to `policy.attempts` times, sleeping with exponential
/// backoff + full jitter between attempts. Only errors classified as
/// transient by `is_transient` are retried; the final error is returned
/// unchanged either way.
pub async fn with_retry<T, E, Fut, Op, P>(
    policy: &RetryPolicy,
    what: &str,
    is_transient: P,
    mut op: Op,
) -> Result<T, E>
where
    E: Display,
    Fut: Future<Output = Result<T, E>>,
    Op: FnMut() -> Fut,
    P: Fn(&E) -> bool,
{
    let mut attempt = 0u32;
    loop {
        match op().await {
            Ok(v) => return Ok(v),
            Err(e) if attempt + 1 < policy.attempts && is_transient(&e) => {
                let delay = backoff_delay(policy, attempt);
                warn!(
                    operation = what,
                    attempt = attempt + 1,
                    max_attempts = policy.attempts,
                    delay_ms = delay.as_millis() as u64,
                    error = %e,
                    "Transient failure; retrying"
                );
                tokio::time::sleep(delay).await;
                attempt += 1;
            }
            Err(e) => return Err(e),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::Cell;

    fn fast_policy(attempts: u32) -> RetryPolicy {
        RetryPolicy {
            attempts,
            base_delay: Duration::from_millis(1),
            max_delay: Duration::from_millis(2),
        }
    }

    #[test]
    fn backoff_cap_grows_exponentially_and_clamps() {
        let policy = RetryPolicy {
            attempts: 10,
            base_delay: Duration::from_millis(100),
            max_delay: Duration::from_secs(1),
        };
        assert_eq!(backoff_cap(&policy, 0), Duration::from_millis(100));
        assert_eq!(backoff_cap(&policy, 1), Duration::from_millis(200));
        assert_eq!(backoff_cap(&policy, 2), Duration::from_millis(400));
        assert_eq!(backoff_cap(&policy, 3), Duration::from_millis(800));
        // Clamped to max_delay from here on, even at absurd attempt counts.
        assert_eq!(backoff_cap(&policy, 4), Duration::from_secs(1));
        assert_eq!(backoff_cap(&policy, 63), Duration::from_secs(1));
    }

    #[test]
    fn jittered_stays_within_cap() {
        let cap = Duration::from_millis(50);
        for _ in 0..100 {
            assert!(jittered(cap) <= cap);
        }
        assert_eq!(jittered(Duration::ZERO), Duration::ZERO);
    }

    #[tokio::test]
    async fn retries_transient_errors_until_success() {
        let calls = Cell::new(0u32);
        let result: Result<u32, String> = with_retry(
            &fast_policy(3),
            "test",
            |_| true,
            || {
                calls.set(calls.get() + 1);
                let n = calls.get();
                async move {
                    if n < 3 {
                        Err("boom".to_string())
                    } else {
                        Ok(n)
                    }
                }
            },
        )
        .await;
        assert_eq!(result, Ok(3));
        assert_eq!(calls.get(), 3);
    }

    #[tokio::test]
    async fn gives_up_after_max_attempts() {
        let calls = Cell::new(0u32);
        let result: Result<(), String> = with_retry(
            &fast_policy(3),
            "test",
            |_| true,
            || {
                calls.set(calls.get() + 1);
                async { Err("always".to_string()) }
            },
        )
        .await;
        assert_eq!(result, Err("always".to_string()));
        assert_eq!(calls.get(), 3);
    }

    #[tokio::test]
    async fn non_transient_errors_fail_immediately() {
        let calls = Cell::new(0u32);
        let result: Result<(), String> = with_retry(
            &fast_policy(5),
            "test",
            |_| false,
            || {
                calls.set(calls.get() + 1);
                async { Err("fatal".to_string()) }
            },
        )
        .await;
        assert_eq!(result, Err("fatal".to_string()));
        assert_eq!(calls.get(), 1);
    }

    #[tokio::test]
    async fn single_attempt_policy_never_retries() {
        let calls = Cell::new(0u32);
        let result: Result<(), String> = with_retry(
            &fast_policy(1),
            "test",
            |_| true,
            || {
                calls.set(calls.get() + 1);
                async { Err("boom".to_string()) }
            },
        )
        .await;
        assert!(result.is_err());
        assert_eq!(calls.get(), 1);
    }
}
