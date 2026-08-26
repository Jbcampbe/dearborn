//! Bounded retry with short linear backoff for transient network operations
//! (incident hardening, Recommendation 4).
//!
//! ## Why this module exists
//!
//! The finalize sequence performs two network operations that can fail for
//! reasons that are **transient** — a later attempt against an unchanged
//! request succeeds: `git push` over HTTPS (a flaky HTTP/2 send-pack path
//! produced "RPC failed; HTTP 400 curl 22 ... unexpected disconnect" in a
//! real run) and GitHub's PR-open REST API (a mid-run 429 rate limit). Both
//! failures used to surface as terminal errors even though the operation
//! would very likely succeed on a second try seconds later. This module is
//! the one shared bounded-retry loop both call sites run through.
//!
//! ## Shape
//!
//! [`retry_transient`] takes the number of total attempts, a base delay, a
//! caller-supplied *transience predicate*, the fallible operation itself,
//! and a caller-supplied *sleep* function. Linear backoff means attempt N is
//! followed by sleeping `base_delay * N` before the next try (500 ms after
//! the first failure, then 1 s). The sleep seam keeps every test hermetic
//! and instant: tests pass a closure that records the requested durations
//! and returns immediately instead of actually waiting.
//!
//! Every failed attempt is logged via `tracing` with the error's Display —
//! safe because both current callers (`crate::git::push_branch`,
//! `crate::git_host::GithubHost::open_pr`) produce already-redacted errors
//! (see `crate::git::redact` and `git_host`'s "redaction discipline" module
//! doc section); no secret can reach the log through here.
//!

use std::future::Future;
use std::time::Duration;

/// Total attempts made per wrapped operation
pub(crate) const MAX_ATTEMPTS: u32 = 100;

/// The delay after attempt *N* is `BASE_DELAY * N`
pub(crate) const BASE_DELAY: Duration = Duration::from_millis(5000);

/// Run `op` up to `attempts` times, retrying only when `is_transient`
/// classifies the failure as likely to succeed on a later try.
///
/// * Between attempts, sleeps `base_delay * attempt_number` via the
///   caller-supplied `sleep` (linear backoff — see [`BASE_DELAY`]).
/// * Every failure — retried or not — is logged at `warn` level with the
///   (already-redacted) error so triage can see how many attempts a push or
///   PR-open burned before giving up.
/// * Returns the last error once attempts are exhausted, or immediately
///   when `is_transient` says the failure cannot be fixed by another try
///   (e.g. a 4xx validation error from the GitHub API).
///
/// `what` names the operation in log lines ("push", "open_pr"); it is a
/// human label only and is never treated as sensitive.
pub(crate) async fn retry_transient<T, E, OpFut, Op, SleepFut, Sleep, Pred>(
    what: &str,
    attempts: u32,
    base_delay: Duration,
    is_transient: Pred,
    mut op: Op,
    mut sleep: Sleep,
) -> Result<T, E>
where
    E: std::fmt::Display,
    Op: FnMut() -> OpFut,
    OpFut: Future<Output = Result<T, E>>,
    Sleep: FnMut(Duration) -> SleepFut,
    SleepFut: Future<Output = ()>,
    Pred: Fn(&E) -> bool,
{
    // Clamp to at least one attempt so a misconfigured `0` still tries once
    // rather than silently reporting success-by-vacuity.
    let attempts = attempts.max(1);
    for attempt in 1..=attempts {
        match op().await {
            Ok(value) => return Ok(value),
            Err(err) => {
                let transient = is_transient(&err);
                tracing::warn!(
                    operation = %what,
                    attempt,
                    max_attempts = attempts,
                    transient,
                    error = %err,
                    "transient-network operation failed"
                );
                if !transient || attempt == attempts {
                    return Err(err);
                }
                sleep(base_delay * attempt).await;
            }
        }
    }
    unreachable!("the loop returns on its first iteration at the latest")
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::RefCell;
    use std::rc::Rc;

    /// Record the requested delays without actually waiting, so the test
    /// asserts on the backoff schedule itself rather than on wall-clock time.
    fn instant_sleep(
        delays: Rc<RefCell<Vec<Duration>>>,
    ) -> impl FnMut(Duration) -> std::future::Ready<()> {
        move |delay| {
            delays.borrow_mut().push(delay);
            std::future::ready(())
        }
    }

    #[tokio::test]
    async fn permanent_failure_returns_immediately_without_retrying() {
        let calls = Rc::new(RefCell::new(0u32));
        let delays = Rc::new(RefCell::new(Vec::new()));
        let result: Result<(), String> = retry_transient(
            "test-op",
            MAX_ATTEMPTS,
            BASE_DELAY,
            // A validation-style error: retrying cannot change the outcome.
            |err: &String| !err.contains("permanent"),
            || {
                *calls.borrow_mut() += 1;
                std::future::ready(Err("permanent rejection".to_string()))
            },
            instant_sleep(delays.clone()),
        )
        .await;

        assert_eq!(result.unwrap_err(), "permanent rejection");
        assert_eq!(
            *calls.borrow(),
            1,
            "a non-transient failure must not be retried"
        );
        assert!(
            delays.borrow().is_empty(),
            "no backoff sleep before bailing out"
        );
    }

    #[tokio::test]
    async fn exhausted_transient_failures_err_after_max_attempts() {
        let calls = Rc::new(RefCell::new(0u32));
        let delays = Rc::new(RefCell::new(Vec::new()));
        let result: Result<(), String> = retry_transient(
            "test-op",
            MAX_ATTEMPTS,
            BASE_DELAY,
            |_: &String| true,
            || {
                *calls.borrow_mut() += 1;
                std::future::ready(Err("still down".to_string()))
            },
            instant_sleep(delays.clone()),
        )
        .await;

        assert_eq!(result.unwrap_err(), "still down");
        assert_eq!(*calls.borrow(), MAX_ATTEMPTS);
        assert_eq!(delays.borrow().len(), (MAX_ATTEMPTS - 1) as usize);
    }
}
