//! The executor worker pool: N long-lived loops that claim leased epics and
//! drive them to completion (T-510, Milestone 2 §2.4/§6, decisions D2/D4).
//!
//! ## The shape (D2)
//!
//! Milestone 1's model was "the lane handler spawns a worker per epic." That
//! doesn't survive a restart (an in-flight epic's `tokio::spawn`'d task dies
//! with the process) and doesn't bound concurrency (every `Ready → InProgress`
//! move spawned its own task, unbounded). [`spawn_pool`] replaces it with
//! `config.executor.worker_concurrency` long-lived loops ([`worker_loop`]),
//! each with a stable identity (`worker_id`) used as the row's `lease_owner`.
//! The lane handler ([`crate::lanes::set_epic_lane`]) now only enqueues
//! (`status='InProgress'`, lease cleared) and calls
//! `state.notify.notify_waiters()` — it never spawns anything. A worker loop
//! survives any single epic's failure and keeps serving the queue for the
//! life of the process; restart-safety comes from the DB being the only
//! source of truth (§13) plus the boot-time lease clear ([`clear_all_leases`]).
//!
//! ## Notify-or-poll (idle loop)
//!
//! An idle worker waits on `tokio::time::timeout(poll_interval,
//! notify.notified())`. `notify_waiters()` is the fast path (near-instant
//! wake on enqueue); the `poll_interval_ms` timeout is the safety net for a
//! missed wakeup — `Notify::notify_waiters()` only wakes futures that are
//! *already* registered as waiting, so a notify that lands in the small
//! window between a worker finishing its claim attempt and re-entering the
//! wait is otherwise lost. Neither path busy-waits: the loop is parked on
//! `.await` the entire time.
//!
//! After a successful claim, a worker skips the wait entirely and tries to
//! claim again immediately ([`worker_loop`]'s inner loop) — otherwise a burst
//! of enqueues would only drain one epic per `poll_interval_ms`, however many
//! workers are idle.
//!
//! ## The claim (§2.4)
//!
//! [`claim_epic`] is exactly the §2.4 statement: an `UPDATE ... WHERE id =
//! (SELECT ... ORDER BY updated_at ASC LIMIT 1) RETURNING id, project_id`.
//! SQLite/libSQL serialize writers against one connection — that serialization
//! **is** the mutual-exclusion lock (§6); no application-level mutex sits on
//! top of it. Two workers racing this statement concurrently: the subquery
//! picks at most one row, so at most one UPDATE can match it; the loser's
//! `WHERE` clause (now failing the `lease_owner IS NULL OR lease_expires_at <
//! now` predicate, or simply finding no matching id if it was the only
//! candidate) affects zero rows and returns `Ok(None)`.
//!
//! This uses libSQL's `RETURNING` clause through the ordinary `query()` path
//! (libsql 0.9's bundled SQLite supports `UPDATE ... RETURNING` since SQLite
//! 3.35) rather than the UPDATE-then-SELECT fallback the task allows — one
//! round trip, and no need to reason about a follow-up read racing another
//! worker's claim. If a libSQL version ever regresses `RETURNING` support, the
//! safe fallback is an `UPDATE` using `changes()`/a `RETURNING`-free affected
//! check followed by `SELECT ... WHERE id = ?1 AND lease_owner = ?2` (by id
//! **and** the worker's own `lease_owner`, never a bare re-SELECT — a bare
//! `SELECT ... WHERE status='InProgress' ORDER BY updated_at LIMIT 1` after a
//! blind `UPDATE` could read a different worker's freshly-claimed row).
//!
//! ## Orphaned tasks (part of the claim path)
//!
//! A dead worker's lease eventually expires, but any task it left
//! `InProgress` did not finish — that work was abandoned mid-flight.
//! [`reset_orphaned_tasks`] resets those back to `Todo` as part of the same
//! claim (called immediately after a successful [`claim_epic`]), so the new
//! owner's DAG walk sees them as pending again rather than permanently stuck.
//!
//! ## Heartbeat with fencing (D4)
//!
//! [`spawn_heartbeat`] renews the claimed epic's `lease_expires_at` every
//! `heartbeat_secs` via the fencing update: `UPDATE epic SET
//! lease_expires_at = ? WHERE id = ? AND lease_owner = ?`. The `WHERE
//! lease_owner = ?` clause is the fence — if another worker's claim already
//! stole the row (because this worker's lease expired and nobody renewed it
//! in time), the predicate matches nothing and the UPDATE affects zero rows.
//! Zero rows is the **only** signal needed: there is no separate "am I still
//! the owner?" read to race against, because the write's own affected-row
//! count is authoritative. On zero rows the heartbeat flips the shared
//! [`LeaseHandle`] to lost and stops renewing; the claimed-epic body
//! ([`run_stub_worker`]) checks the handle every loop iteration and abandons
//! the item — no further writes — the moment it observes the loss.
//!
//! ## No reaper (D4)
//!
//! Lease expiry is **implicit**: the claim predicate itself
//! (`lease_expires_at < now`) is what makes an expired lease reclaimable.
//! There is no background task scanning for expired leases to clear them —
//! nothing needs to; the next claim attempt against that epic simply
//! succeeds. This trades a small, bounded delay (up to `lease_ttl_secs`)
//! after a genuine worker death for one less moving part.
//!
//! ## Boot-time lease clear (D4, §13)
//!
//! [`clear_all_leases`] NULLs every lease column on `epic` and `task` at
//! startup (`main`, before [`spawn_pool`]). Dearborn assumes a single server
//! process (§13): nothing else could legitimately hold a lease across a
//! restart, so waiting out the TTL would only delay resumption for no
//! benefit. Clearing immediately means a restart resumes in-flight work on
//! the very first poll/notify rather than after however much of the TTL
//! happened to elapse.
//!
//! ## The pipeline body is still the M1 stub (T-513 replaces it)
//!
//! [`run_stub_worker`] is the same DB-only DAG walk from Milestone 1 — no
//! agent, no git, no shell-out — now made **lease-aware**: it checks the
//! [`LeaseHandle`] at the top of every loop iteration and returns immediately
//! if the lease was lost, so a fenced-out worker never writes another task
//! transition after another worker has taken over the epic. See the original
//! module docs (preserved below in spirit) for the DAG-walk contract itself.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use libsql::{params, Connection};
use tokio::task::JoinHandle;

use crate::board;
use crate::epics::{fetch_epic, get_epic_project_id};
use crate::mcp;
use crate::tasks::compute_dag;
use crate::AppState;

/// Test-only pipeline hook (T-510): an async closure the claimed-epic body
/// awaits once, immediately after a claim, before doing any DB work. See
/// [`crate::AppState::test_pipeline_hook`].
#[cfg(test)]
pub type PipelineHook = Arc<
    dyn Fn() -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>> + Send + Sync,
>;

/// The row identity returned by a successful [`claim_epic`] — just enough to
/// drive the pipeline body and resolve the project for board publishes.
#[derive(Debug, Clone)]
pub struct ClaimedEpic {
    pub id: String,
    #[allow(dead_code)] // not yet read; T-511+ uses it for workspace paths
    pub project_id: String,
}

/// Shared flag signalling whether a claimed lease is still held (D4).
///
/// Cloned into the heartbeat task and the claimed-epic body. The heartbeat is
/// the only writer: it flips this to "lost" the instant its fencing UPDATE
/// affects zero rows. The body only reads it, once per loop iteration, to
/// decide whether to keep writing or abandon the item. A plain
/// `Arc<AtomicBool>` is enough — there is nothing to wake (the body is
/// already polling its own DB reads every iteration; it just needs to check
/// one more flag on each pass), so a `Notify`/watch-channel would add
/// complexity with no benefit here.
#[derive(Clone)]
pub struct LeaseHandle(Arc<AtomicBool>);

impl LeaseHandle {
    /// A fresh handle, valid until [`mark_lost`](Self::mark_lost) is called.
    fn new() -> LeaseHandle {
        LeaseHandle(Arc::new(AtomicBool::new(true)))
    }

    /// Whether the lease has been fenced out (a heartbeat renewal affected
    /// zero rows). Checked by the pipeline body at the top of every iteration.
    pub fn is_lost(&self) -> bool {
        !self.0.load(Ordering::SeqCst)
    }

    /// Record that the lease was lost. Idempotent; called by the heartbeat
    /// task only.
    fn mark_lost(&self) {
        self.0.store(false, Ordering::SeqCst);
    }
}

/// §2.4 epic claim (tried first — standalone-task claim is T-550). See the
/// module docs for the full race/RETURNING rationale. `lease_ttl_secs` sets
/// how far in the future `lease_expires_at` is written; `worker_id` becomes
/// `lease_owner`.
pub async fn claim_epic(
    conn: &Connection,
    worker_id: &str,
    lease_ttl_secs: u64,
) -> Result<Option<ClaimedEpic>, libsql::Error> {
    let now = now_ms();
    let expires_at = now + (lease_ttl_secs as i64) * 1000;
    let mut rows = conn
        .query(
            "UPDATE epic SET lease_owner = ?1, lease_expires_at = ?2, updated_at = ?3 \
             WHERE id = (SELECT id FROM epic \
                         WHERE status = 'InProgress' \
                           AND (lease_owner IS NULL OR lease_expires_at < ?3) \
                         ORDER BY updated_at ASC LIMIT 1) \
             RETURNING id, project_id",
            params![worker_id, expires_at, now],
        )
        .await?;
    match rows.next().await? {
        Some(row) => Ok(Some(ClaimedEpic {
            id: row.get::<String>(0)?,
            project_id: row.get::<String>(1)?,
        })),
        None => Ok(None),
    }
}

/// Part of the claim path (see module docs): reset any task of `epic_id` left
/// `InProgress` by a previous (now-dead or fenced-out) owner back to `Todo`,
/// so the new owner's DAG walk treats that abandoned work as pending again.
/// Returns the number of tasks reset (`0` is the common case — a fresh claim
/// with no orphans).
async fn reset_orphaned_tasks(conn: &Connection, epic_id: &str) -> Result<u64, libsql::Error> {
    let now = now_ms();
    conn.execute(
        "UPDATE task SET status = 'Todo', updated_at = ?1 WHERE epic_id = ?2 AND status = 'InProgress'",
        params![now, epic_id],
    )
    .await
}

/// A single heartbeat renewal attempt (D4's fencing update), factored out of
/// [`spawn_heartbeat`] so it is directly unit-testable without waiting on a
/// real timer: returns `Ok(true)` if the lease is still ours (the UPDATE
/// affected a row), `Ok(false)` if it was fenced out (zero rows — someone
/// else's claim now owns this id).
async fn renew_lease_once(
    conn: &Connection,
    epic_id: &str,
    worker_id: &str,
    lease_ttl_secs: u64,
) -> Result<bool, libsql::Error> {
    let now = now_ms();
    let expires_at = now + (lease_ttl_secs as i64) * 1000;
    let affected = conn
        .execute(
            "UPDATE epic SET lease_expires_at = ?1 WHERE id = ?2 AND lease_owner = ?3",
            params![expires_at, epic_id, worker_id],
        )
        .await?;
    Ok(affected > 0)
}

/// Spawn the per-claimed-item heartbeat task (D4). Renews `epic_id`'s lease
/// every `period` using [`renew_lease_once`]; the first renewal it observes
/// fail flips `lease` to lost and the task exits (no further renewals are
/// meaningful once fenced out). The caller (`try_claim_and_run`) aborts this
/// handle when the item is released, on every exit path — see the module
/// docs' "no reaper" note for why there is nothing else watching leases.
fn spawn_heartbeat(
    conn: Connection,
    epic_id: String,
    worker_id: String,
    period: Duration,
    lease_ttl_secs: u64,
    lease: LeaseHandle,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(period).await;
            match renew_lease_once(&conn, &epic_id, &worker_id, lease_ttl_secs).await {
                Ok(true) => continue,
                Ok(false) => {
                    tracing::warn!(
                        epic = %epic_id,
                        worker = %worker_id,
                        "heartbeat: lease fenced out (0 rows affected); abandoning"
                    );
                    lease.mark_lost();
                    return;
                }
                Err(err) => {
                    tracing::warn!(
                        epic = %epic_id,
                        worker = %worker_id,
                        error = %err,
                        "heartbeat: renewal query failed; will retry next tick"
                    );
                }
            }
        }
    })
}

/// Release a held lease: clear `lease_owner`/`lease_expires_at`, fenced by
/// `lease_owner = ?` so a lease already stolen by another worker (this one
/// was fenced out mid-run) is never clobbered — releasing is a no-op in that
/// case, which is correct: the new owner's lease must survive.
async fn release_lease(conn: &Connection, epic_id: &str, worker_id: &str) {
    let result = conn
        .execute(
            "UPDATE epic SET lease_owner = NULL, lease_expires_at = NULL \
             WHERE id = ?1 AND lease_owner = ?2",
            params![epic_id, worker_id],
        )
        .await;
    if let Err(err) = result {
        tracing::warn!(epic = %epic_id, worker = %worker_id, error = %err, "failed to release lease");
    }
}

/// Boot-time lease clear (D4, §13). NULLs every `lease_owner`/
/// `lease_expires_at` on `epic` **and** `task` (task carries the same columns
/// since T-500, for the standalone-task claim T-550 adds). Single-server
/// assumption: nothing else could legitimately hold a lease across a
/// restart, so this makes every previously-claimed row immediately
/// claimable rather than making the pool wait out the TTL. Call once at boot,
/// before [`spawn_pool`].
pub async fn clear_all_leases(db: &crate::Db) -> Result<(), libsql::Error> {
    let conn = db.conn();
    let epics = conn
        .execute(
            "UPDATE epic SET lease_owner = NULL, lease_expires_at = NULL \
             WHERE lease_owner IS NOT NULL OR lease_expires_at IS NOT NULL",
            (),
        )
        .await?;
    let tasks = conn
        .execute(
            "UPDATE task SET lease_owner = NULL, lease_expires_at = NULL \
             WHERE lease_owner IS NOT NULL OR lease_expires_at IS NOT NULL",
            (),
        )
        .await?;
    if epics > 0 || tasks > 0 {
        tracing::info!(epics, tasks, "boot: cleared stale leases");
    }
    Ok(())
}

/// Start the worker pool: `config.executor.worker_concurrency` long-lived
/// loops ([`worker_loop`]), each with a stable `worker_id`. Returns the
/// handles so the caller can hold/await/abort them (production drops them —
/// the pool runs for the life of the process; tests hold them so the runtime
/// keeps polling for the test's duration and they're cleaned up when the
/// test's own runtime shuts down).
pub fn spawn_pool(state: AppState) -> Vec<JoinHandle<()>> {
    let n = state.config.executor.worker_concurrency.max(1);
    (0..n)
        .map(|i| {
            let worker_id = format!("worker-{i}-{}", ulid::Ulid::new());
            let state = state.clone();
            tokio::spawn(worker_loop(state, worker_id))
        })
        .collect()
}

/// One long-lived worker loop (D2). Idles on notify-or-poll, then drains the
/// queue (claim → run → release, repeating immediately on every successful
/// claim) until nothing is left to claim, then idles again. Never returns —
/// the pool's `JoinHandle`s only resolve if the process is torn down.
async fn worker_loop(state: AppState, worker_id: String) {
    let poll_interval = Duration::from_millis(state.config.executor.poll_interval_ms.max(1));
    loop {
        // Idle path: wait for the fast-path wake or the poll fallback,
        // whichever comes first. Never busy-waits — this `.await` parks the
        // task until one of the two futures resolves.
        let _ = tokio::time::timeout(poll_interval, state.notify.notified()).await;

        // Drain: keep claiming (and running) without waiting in between, so a
        // burst of enqueues drains at claim speed, not poll-interval speed.
        loop {
            match try_claim_and_run(&state, &worker_id).await {
                ClaimOutcome::Claimed => continue,
                ClaimOutcome::EmptyOrError => break,
            }
        }
    }
}

enum ClaimOutcome {
    Claimed,
    EmptyOrError,
}

/// One claim attempt and, if it succeeds, the full claimed-item lifecycle:
/// reset orphaned tasks → start the heartbeat → run the pipeline body → stop
/// the heartbeat → release the lease. The release happens on **every** exit
/// path, including a panic in the body, because the body runs in its own
/// `tokio::spawn`'d task — a panic there resolves the `JoinHandle` as `Err`
/// rather than unwinding into this long-lived loop, so the release/heartbeat-
/// abort below always runs.
async fn try_claim_and_run(state: &AppState, worker_id: &str) -> ClaimOutcome {
    let conn = state.db.conn();

    let claimed = match claim_epic(conn, worker_id, state.config.executor.lease_ttl_secs).await {
        Ok(Some(claimed)) => claimed,
        Ok(None) => return ClaimOutcome::EmptyOrError,
        Err(err) => {
            tracing::warn!(worker = %worker_id, error = %err, "claim query failed");
            return ClaimOutcome::EmptyOrError;
        }
    };

    if let Err(err) = reset_orphaned_tasks(conn, &claimed.id).await {
        tracing::warn!(
            epic = %claimed.id,
            error = %err,
            "failed to reset orphaned InProgress tasks after claim"
        );
    }

    let lease = LeaseHandle::new();
    let heartbeat = spawn_heartbeat(
        conn.clone(),
        claimed.id.clone(),
        worker_id.to_string(),
        Duration::from_secs(state.config.executor.heartbeat_secs.max(1)),
        state.config.executor.lease_ttl_secs,
        lease.clone(),
    );

    // Run the body in its own task: isolates a panic from this long-lived
    // loop (a panicking claimed-epic body must not take the whole worker
    // down — the epic just stays InProgress with a soon-to-expire lease and
    // gets picked up again). Still awaited immediately: this worker handles
    // one item at a time; concurrency comes from having N worker loops, not
    // from overlapping bodies within one.
    let body = tokio::spawn(run_stub_worker_inner(
        state.clone(),
        claimed.id.clone(),
        lease,
    ));
    let result = body.await;

    heartbeat.abort();
    release_lease(conn, &claimed.id, worker_id).await;

    if let Err(join_err) = result {
        tracing::error!(
            epic = %claimed.id,
            worker = %worker_id,
            error = %join_err,
            "claimed-epic body panicked; lease released for re-claim"
        );
    }

    ClaimOutcome::Claimed
}

/// Run the stub pipeline body to completion on `epic_id`, lease-unaware
/// (always treats the lease as held). Kept as the direct-call seam Milestone
/// 1's tests used and still use; the pool calls the lease-aware
/// [`run_stub_worker_inner`] instead. See the module docs for why this is
/// still the M1 stub (T-513 replaces it).
pub async fn run_stub_worker(state: AppState, epic_id: String) {
    run_stub_worker_inner(state, epic_id, LeaseHandle::new()).await;
}

/// The claimed-epic pipeline body (still the M1 stub DAG walk — T-513
/// replaces this). Walks **ready** tasks one at a time (dependency order: a
/// task is ready only when `status='Todo'` and every blocker is `Done`, per
/// §2.3), flips each `Todo → InProgress → Done`, and when the DAG is fully
/// `Done` sets `epic.status='Completed'`.
///
/// Lease-aware (T-510): checks `lease.is_lost()` at the top of every loop
/// iteration and returns immediately, with no further writes, the moment the
/// heartbeat has flagged the lease as fenced out. Also awaits the T-510
/// test-only pipeline hook exactly once, before the first check, so a test
/// can gate/observe the body without sleeps (see
/// [`crate::AppState::test_pipeline_hook`]); superseded whenever T-513 lands.
///
/// ## The "no sibling InProgress" invariant (§2.3)
///
/// The claim predicate requires that no sibling task in the epic is
/// `InProgress`. This body honors it by serializing: it claims at most one
/// ready task at a time, fully completing it (`Done`) before looking for the
/// next, so there is never a moment with two `InProgress` siblings.
///
/// ## Ownership of `InProgress → Completed`
///
/// This body owns the `InProgress → Completed` transition. Manual lane moves
/// to `Completed` are rejected by [`crate::lanes`] (`409 conflict`) — only the
/// pipeline sets it, once the DAG is fully `Done`.
///
/// ## Live publishing
///
/// Every task transition publishes a `dag_updated` frame on `epic:<id>`.
/// When the epic reaches `Completed`, an `epic_updated` frame on `epic:<id>`
/// and a `board_updated` frame on `project:<id>` are published.
async fn run_stub_worker_inner(state: AppState, epic_id: String, lease: LeaseHandle) {
    #[cfg(test)]
    if let Some(hook) = state.test_pipeline_hook.clone() {
        hook().await;
    }

    loop {
        // Lease-aware bail: a heartbeat renewal failure means another worker
        // now owns this epic. Stop writing immediately — any further mutation
        // here could race the new owner's own walk.
        if lease.is_lost() {
            tracing::warn!(
                epic = %epic_id,
                "pipeline: lease lost (fenced out); abandoning without further writes"
            );
            return;
        }

        let conn = state.db.conn();

        // 1. Guard: only act on an InProgress epic. A Cancel/Block during the
        //    walk makes this a clean no-op.
        let Some(epic) = fetch_epic(conn, &epic_id).await.unwrap_or(None) else {
            tracing::debug!(epic = %epic_id, "pipeline: epic vanished; stopping");
            return;
        };
        if epic.status != "InProgress" {
            tracing::debug!(
                epic = %epic_id,
                status = %epic.status,
                "pipeline: epic no longer InProgress; stopping"
            );
            return;
        }

        // 2. Compute the DAG with readiness.
        let dag = match compute_dag(conn, &epic_id).await {
            Ok(dag) => dag,
            Err(err) => {
                tracing::warn!(
                    epic = %epic_id,
                    error = %err,
                    "pipeline: failed to compute DAG; stopping"
                );
                return;
            }
        };

        // 3. Defensive: if any task is already InProgress (shouldn't happen —
        //    we serialize), wait for it to settle and retry.
        if dag.nodes.iter().any(|n| n.task.status == "InProgress") {
            let delay = state.config.stub_worker_delay_ms;
            if delay > 0 {
                tokio::time::sleep(std::time::Duration::from_millis(delay)).await;
            }
            continue;
        }

        // 4. Find a ready task (Todo + all blockers Done).
        let Some(ready) = dag.nodes.iter().find(|n| n.ready) else {
            // 5. No ready task.
            let all_done = dag.nodes.iter().all(|n| n.task.status == "Done");
            if all_done {
                // The DAG is complete (or empty): mark the epic Completed.
                // Fenced by lease_owner too — belt-and-suspenders alongside
                // the loop-top check, since the lease could be lost between
                // the check above and this write.
                let now = now_ms();
                let _ = conn
                    .execute(
                        "UPDATE epic SET status = 'Completed', updated_at = ?1 \
                         WHERE id = ?2 AND status = 'InProgress'",
                        params![now, epic_id.clone()],
                    )
                    .await;

                // Publish the final DAG state + the updated epic + the board.
                mcp::publish_dag(&state, &epic_id).await;
                if let Ok(Some(updated)) = fetch_epic(conn, &epic_id).await {
                    let payload =
                        serde_json::to_value(&updated).unwrap_or(serde_json::Value::Null);
                    state
                        .hub
                        .publish(&format!("epic:{epic_id}"), "epic_updated", payload);
                    board::publish_board(&state, &updated.project_id).await;
                }
                tracing::info!(epic = %epic_id, "pipeline: DAG complete; epic → Completed");
                return;
            } else {
                // Some Todo tasks remain but none are ready (all blocked) and
                // none InProgress — the DAG cannot progress. A valid acyclic
                // DAG walked in dependency order never hits this (cycles are
                // rejected at link time). Log and stop; do NOT infinite-loop.
                tracing::warn!(
                    epic = %epic_id,
                    "pipeline: no ready task but not all Done; DAG is stuck; stopping"
                );
                return;
            }
        };

        let task_id = &ready.task.id;
        let now = now_ms();

        // Claim: Todo → InProgress.
        let _ = conn
            .execute(
                "UPDATE task SET status = 'InProgress', updated_at = ?1 WHERE id = ?2",
                params![now, task_id.clone()],
            )
            .await;
        mcp::publish_dag(&state, &epic_id).await;

        // Sleep so a browser can watch the walk (0 in tests).
        let delay = state.config.stub_worker_delay_ms;
        if delay > 0 {
            tokio::time::sleep(std::time::Duration::from_millis(delay)).await;
        }

        // Complete: InProgress → Done.
        let now = now_ms();
        let _ = conn
            .execute(
                "UPDATE task SET status = 'Done', updated_at = ?1 WHERE id = ?2",
                params![now, task_id.clone()],
            )
            .await;
        mcp::publish_dag(&state, &epic_id).await;

        // Sleep once more so the Done state is visible before the next claim.
        if delay > 0 {
            tokio::time::sleep(std::time::Duration::from_millis(delay)).await;
        }

        // Continue the loop — re-fetch the epic and look for the next ready task.
    }
}

/// Resolve the project id for an epic (best-effort, for the board publish).
/// Re-fetches the epic to read `.project_id` directly. Kept for completeness;
/// the pipeline body uses `fetch_epic` + `.project_id` instead.
#[allow(dead_code)]
async fn resolve_project_id(state: &AppState, epic_id: &str) -> Option<String> {
    get_epic_project_id(state.db.conn(), epic_id)
        .await
        .ok()
        .flatten()
}

/// Current unix time in milliseconds (matches the `*_at` columns).
fn now_ms() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::breakdown::testing::SilentBreakdownAgent;
    use crate::planning::testing::SilentPlanningAgent;
    use crate::{app, Config, Db};
    use axum::body::Body;
    use axum::http::{
        header::{AUTHORIZATION, CONTENT_TYPE},
        Request, StatusCode,
    };
    use libsql::params;
    use serde_json::{json, Value};
    use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};
    use std::sync::Arc;
    use std::time::Duration;
    use tower::ServiceExt;

    const TOKEN: &str = "s3cret-token";

    fn req(method: &str, uri: &str, body: Option<Value>) -> Request<Body> {
        let builder = Request::builder()
            .method(method)
            .uri(uri)
            .header(AUTHORIZATION, format!("Bearer {TOKEN}"));
        match body {
            Some(v) => builder
                .header(CONTENT_TYPE, "application/json")
                .body(Body::from(v.to_string()))
                .unwrap(),
            None => builder.body(Body::empty()).unwrap(),
        }
    }

    async fn body_json(response: axum::response::Response) -> Value {
        let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        if bytes.is_empty() {
            return Value::Null;
        }
        serde_json::from_slice(&bytes).unwrap()
    }

    /// Boot an app over an in-memory db with silent agents (delay 0 via
    /// `Config::for_test`). Returns (state, app).
    async fn test_app() -> (AppState, axum::Router) {
        let db = Db::connect(":memory:").await.unwrap();
        db.run_migrations().await.unwrap();
        let state = AppState::with_agents(
            Config::for_test(TOKEN),
            db,
            Arc::new(SilentPlanningAgent),
            Arc::new(SilentBreakdownAgent),
        );
        let app = app(state.clone());
        (state, app)
    }

    async fn seed_project(state: &AppState) -> String {
        let conn = state.db.conn();
        let id = ulid::Ulid::new().to_string();
        let now = now_ms();
        conn.execute(
            "INSERT INTO project (id, name, repo_url, clone_status, created_at, updated_at) \
             VALUES (?1, 'P', 'https://example.com/p.git', 'ready', ?2, ?2)",
            params![id.clone(), now],
        )
        .await
        .unwrap();
        id
    }

    async fn seed_epic(state: &AppState, project_id: &str, status: &str) -> String {
        let conn = state.db.conn();
        let id = ulid::Ulid::new().to_string();
        let now = now_ms();
        conn.execute(
            "INSERT INTO epic (id, project_id, title, status, created_at, updated_at) \
             VALUES (?1, ?2, 'E', ?3, ?4, ?4)",
            params![id.clone(), project_id, status, now],
        )
        .await
        .unwrap();
        id
    }

    /// Create a task under `epic_id` with `status='Todo'` via direct SQL (mirrors
    /// `tasks::create_task` but keeps the test self-contained).
    async fn seed_task(state: &AppState, epic_id: &str, project_id: &str, title: &str) -> String {
        let conn = state.db.conn();
        let id = ulid::Ulid::new().to_string();
        let now = now_ms();
        conn.execute(
            "INSERT INTO task \
             (id, epic_id, project_id, title, status, position, created_at, updated_at) \
             VALUES (?1, ?2, ?3, ?4, 'Todo', \
             (SELECT COALESCE(MAX(position), 0) + 1 FROM task WHERE epic_id = ?2), \
             ?5, ?5)",
            params![id.clone(), epic_id, project_id, title, now],
        )
        .await
        .unwrap();
        id
    }

    /// Set a task's status directly (used to seed an "orphaned InProgress"
    /// task left by a dead owner).
    async fn set_task_status(state: &AppState, task_id: &str, status: &str) {
        let conn = state.db.conn();
        conn.execute(
            "UPDATE task SET status = ?1 WHERE id = ?2",
            params![status, task_id],
        )
        .await
        .unwrap();
    }

    /// Link `blocker_id → blocked_id` via direct SQL (no cycle guard needed —
    /// tests build valid acyclic DAGs).
    async fn link(state: &AppState, blocker_id: &str, blocked_id: &str) {
        let conn = state.db.conn();
        conn.execute(
            "INSERT OR IGNORE INTO task_dependency (blocker_id, blocked_id) VALUES (?1, ?2)",
            params![blocker_id, blocked_id],
        )
        .await
        .unwrap();
    }

    /// Fetch all task statuses for an epic, keyed by title.
    async fn task_statuses(
        state: &AppState,
        epic_id: &str,
    ) -> std::collections::HashMap<String, String> {
        let conn = state.db.conn();
        let mut rows = conn
            .query(
                "SELECT title, status FROM task WHERE epic_id = ?1",
                params![epic_id],
            )
            .await
            .unwrap();
        let mut map = std::collections::HashMap::new();
        while let Some(row) = rows.next().await.unwrap() {
            map.insert(row.get::<String>(0).unwrap(), row.get::<String>(1).unwrap());
        }
        map
    }

    async fn epic_status(state: &AppState, epic_id: &str) -> String {
        fetch_epic(state.db.conn(), epic_id)
            .await
            .unwrap()
            .unwrap()
            .status
    }

    async fn epic_lease(state: &AppState, epic_id: &str) -> (Option<String>, Option<i64>) {
        let conn = state.db.conn();
        let mut rows = conn
            .query(
                "SELECT lease_owner, lease_expires_at FROM epic WHERE id = ?1",
                params![epic_id],
            )
            .await
            .unwrap();
        let row = rows.next().await.unwrap().unwrap();
        (row.get(0).unwrap(), row.get(1).unwrap())
    }

    // ---- run_stub_worker direct tests (unchanged from M1) ----

    /// Linear DAG (A → B → C): after the worker, all Done + epic Completed.
    ///
    /// The dependency ORDER is respected implicitly: B can only become ready
    /// after A is Done (its only blocker), and C after B. So asserting the
    /// final state (all Done) IS the order assertion — a reversed walk could
    /// never reach all-Done.
    #[tokio::test]
    async fn linear_dag_walks_to_completion() {
        let (state, _app) = test_app().await;
        let project_id = seed_project(&state).await;
        let epic_id = seed_epic(&state, &project_id, "InProgress").await;

        let a = seed_task(&state, &epic_id, &project_id, "A").await;
        let b = seed_task(&state, &epic_id, &project_id, "B").await;
        let c = seed_task(&state, &epic_id, &project_id, "C").await;
        // A blocks B, B blocks C (A → B → C).
        link(&state, &a, &b).await;
        link(&state, &b, &c).await;

        run_stub_worker(state.clone(), epic_id.clone()).await;

        let statuses = task_statuses(&state, &epic_id).await;
        assert_eq!(statuses["A"], "Done");
        assert_eq!(statuses["B"], "Done");
        assert_eq!(statuses["C"], "Done");
        assert_eq!(epic_status(&state, &epic_id).await, "Completed");
    }

    /// Branching DAG (A blocks B and C; B and C both block D): all Done.
    #[tokio::test]
    async fn branching_dag_walks_to_completion() {
        let (state, _app) = test_app().await;
        let project_id = seed_project(&state).await;
        let epic_id = seed_epic(&state, &project_id, "InProgress").await;

        let a = seed_task(&state, &epic_id, &project_id, "A").await;
        let b = seed_task(&state, &epic_id, &project_id, "B").await;
        let c = seed_task(&state, &epic_id, &project_id, "C").await;
        let d = seed_task(&state, &epic_id, &project_id, "D").await;
        // A → B, A → C, B → D, C → D.
        link(&state, &a, &b).await;
        link(&state, &a, &c).await;
        link(&state, &b, &d).await;
        link(&state, &c, &d).await;

        run_stub_worker(state.clone(), epic_id.clone()).await;

        let statuses = task_statuses(&state, &epic_id).await;
        assert_eq!(statuses["A"], "Done");
        assert_eq!(statuses["B"], "Done");
        assert_eq!(statuses["C"], "Done");
        assert_eq!(statuses["D"], "Done");
        assert_eq!(epic_status(&state, &epic_id).await, "Completed");
    }

    /// Empty epic (no tasks): worker sets the epic Completed.
    #[tokio::test]
    async fn empty_epic_completes() {
        let (state, _app) = test_app().await;
        let project_id = seed_project(&state).await;
        let epic_id = seed_epic(&state, &project_id, "InProgress").await;

        run_stub_worker(state.clone(), epic_id.clone()).await;

        assert_eq!(epic_status(&state, &epic_id).await, "Completed");
    }

    /// Non-InProgress epic is a no-op: no task or epic status changes.
    #[tokio::test]
    async fn non_in_progress_epic_is_no_op() {
        let (state, _app) = test_app().await;
        let project_id = seed_project(&state).await;
        let epic_id = seed_epic(&state, &project_id, "Ready").await;
        seed_task(&state, &epic_id, &project_id, "A").await;

        run_stub_worker(state.clone(), epic_id.clone()).await;

        let statuses = task_statuses(&state, &epic_id).await;
        assert_eq!(statuses["A"], "Todo", "task untouched");
        assert_eq!(epic_status(&state, &epic_id).await, "Ready", "epic untouched");
    }

    /// No sibling InProgress invariant: after a full run, the final state is
    /// consistent — all Done, none InProgress. The worker serializes by
    /// construction (one ready task at a time); this final-state assertion
    /// confirms it.
    #[tokio::test]
    async fn no_sibling_in_progress_after_run() {
        let (state, _app) = test_app().await;
        let project_id = seed_project(&state).await;
        let epic_id = seed_epic(&state, &project_id, "InProgress").await;
        let a = seed_task(&state, &epic_id, &project_id, "A").await;
        let b = seed_task(&state, &epic_id, &project_id, "B").await;
        // A and B are independent (no edge between them) — both are ready from
        // the start. The worker still claims one at a time.
        link(&state, &a, &b).await; // A → B: only A is ready initially.

        run_stub_worker(state.clone(), epic_id.clone()).await;

        let statuses = task_statuses(&state, &epic_id).await;
        assert_eq!(statuses["A"], "Done");
        assert_eq!(statuses["B"], "Done");
        assert!(statuses.values().all(|s| s != "InProgress"));
        assert_eq!(epic_status(&state, &epic_id).await, "Completed");
    }

    // ---- T-510: claim semantics ----

    /// Two (many) workers racing the claim SQL against one enqueued epic:
    /// exactly one succeeds. Hammers `claim_epic` concurrently on the same
    /// underlying connection — SQLite/libSQL's write serialization is what
    /// makes this deterministic (§6), not any application-level mutex.
    #[tokio::test]
    async fn concurrent_claims_on_one_epic_yield_exactly_one_success() {
        let (state, _app) = test_app().await;
        let project_id = seed_project(&state).await;
        let epic_id = seed_epic(&state, &project_id, "InProgress").await;

        let mut handles = Vec::new();
        for i in 0..25 {
            let db = state.db.clone();
            handles.push(tokio::spawn(async move {
                claim_epic(db.conn(), &format!("racer-{i}"), 30).await
            }));
        }

        let mut successes = 0;
        let mut winner = None;
        for h in handles {
            if let Ok(Ok(Some(claimed))) = h.await {
                successes += 1;
                winner = Some(claimed.id);
            }
        }
        assert_eq!(successes, 1, "exactly one racer must claim the epic");
        assert_eq!(winner.as_deref(), Some(epic_id.as_str()));
    }

    /// An expired lease is re-claimable by a new owner, and the new owner's
    /// claim path resets the previous owner's abandoned `InProgress` task
    /// back to `Todo`.
    #[tokio::test]
    async fn expired_lease_is_reclaimable_and_resets_orphaned_tasks() {
        let (state, _app) = test_app().await;
        let project_id = seed_project(&state).await;
        let epic_id = seed_epic(&state, &project_id, "InProgress").await;
        let a = seed_task(&state, &epic_id, &project_id, "A").await;
        set_task_status(&state, &a, "InProgress").await; // abandoned mid-flight

        let conn = state.db.conn();
        let past = now_ms() - 60_000;
        conn.execute(
            "UPDATE epic SET lease_owner = 'dead-worker', lease_expires_at = ?1 WHERE id = ?2",
            params![past, epic_id.clone()],
        )
        .await
        .unwrap();

        let claimed = claim_epic(conn, "new-worker", 30).await.unwrap();
        let claimed = claimed.expect("expired lease must be reclaimable");
        assert_eq!(claimed.id, epic_id);

        let (owner, _expires) = epic_lease(&state, &epic_id).await;
        assert_eq!(owner.as_deref(), Some("new-worker"));

        reset_orphaned_tasks(conn, &epic_id).await.unwrap();
        let statuses = task_statuses(&state, &epic_id).await;
        assert_eq!(statuses["A"], "Todo", "orphaned InProgress task reset to Todo");
    }

    /// A lease that is still live (not expired) is NOT re-claimable — the
    /// negative case alongside the expired-lease test above.
    #[tokio::test]
    async fn live_lease_is_not_reclaimable() {
        let (state, _app) = test_app().await;
        let project_id = seed_project(&state).await;
        let epic_id = seed_epic(&state, &project_id, "InProgress").await;

        let conn = state.db.conn();
        let future = now_ms() + 60_000;
        conn.execute(
            "UPDATE epic SET lease_owner = 'alive-worker', lease_expires_at = ?1 WHERE id = ?2",
            params![future, epic_id.clone()],
        )
        .await
        .unwrap();

        let claimed = claim_epic(conn, "other-worker", 30).await.unwrap();
        assert!(claimed.is_none(), "a live lease must not be reclaimable");
    }

    // ---- T-510: heartbeat + fencing ----

    /// A heartbeat renewal against a lease already stolen by another worker
    /// (lease_owner changed out from under us) reports the loss (0 rows
    /// affected) directly — the pure fencing check, no timers involved.
    #[tokio::test]
    async fn heartbeat_against_stolen_lease_reports_lost() {
        let (state, _app) = test_app().await;
        let project_id = seed_project(&state).await;
        let epic_id = seed_epic(&state, &project_id, "InProgress").await;
        let conn = state.db.conn();

        // We hold the lease as "me"...
        conn.execute(
            "UPDATE epic SET lease_owner = 'me', lease_expires_at = ?1 WHERE id = ?2",
            params![now_ms() + 60_000, epic_id.clone()],
        )
        .await
        .unwrap();

        // ...then someone else's claim steals it (simulating our lease having
        // expired and a second worker claiming in the meantime).
        conn.execute(
            "UPDATE epic SET lease_owner = 'thief', lease_expires_at = ?1 WHERE id = ?2",
            params![now_ms() + 60_000, epic_id.clone()],
        )
        .await
        .unwrap();

        let still_held = renew_lease_once(conn, &epic_id, "me", 30).await.unwrap();
        assert!(!still_held, "renewal against a stolen lease must report 0 rows / lost");

        // The row still belongs to the thief — our renewal must not have
        // clobbered it.
        let (owner, _) = epic_lease(&state, &epic_id).await;
        assert_eq!(owner.as_deref(), Some("thief"));
    }

    /// A live lease renews successfully (the positive case).
    #[tokio::test]
    async fn heartbeat_against_live_lease_succeeds() {
        let (state, _app) = test_app().await;
        let project_id = seed_project(&state).await;
        let epic_id = seed_epic(&state, &project_id, "InProgress").await;
        let conn = state.db.conn();
        conn.execute(
            "UPDATE epic SET lease_owner = 'me', lease_expires_at = ?1 WHERE id = ?2",
            params![now_ms() + 60_000, epic_id.clone()],
        )
        .await
        .unwrap();

        let still_held = renew_lease_once(conn, &epic_id, "me", 30).await.unwrap();
        assert!(still_held);
    }

    /// End-to-end wiring of `spawn_heartbeat` + [`LeaseHandle`]: a stolen
    /// lease flips the shared handle to lost within one heartbeat period.
    /// Uses a short `Duration` directly (not the config-parsed
    /// `heartbeat_secs`, which rejects sub-second values) and a bounded
    /// deadline poll rather than a fixed sleep, matching the rest of the
    /// suite's polling convention.
    #[tokio::test]
    async fn spawn_heartbeat_flags_lease_handle_lost_on_theft() {
        let (state, _app) = test_app().await;
        let project_id = seed_project(&state).await;
        let epic_id = seed_epic(&state, &project_id, "InProgress").await;
        let conn = state.db.conn();
        conn.execute(
            "UPDATE epic SET lease_owner = 'me', lease_expires_at = ?1 WHERE id = ?2",
            params![now_ms() + 60_000, epic_id.clone()],
        )
        .await
        .unwrap();

        let lease = LeaseHandle::new();
        let handle = spawn_heartbeat(
            state.db.conn().clone(),
            epic_id.clone(),
            "me".to_string(),
            Duration::from_millis(15),
            30,
            lease.clone(),
        );

        // Steal the lease.
        conn.execute(
            "UPDATE epic SET lease_owner = 'thief' WHERE id = ?1",
            params![epic_id.clone()],
        )
        .await
        .unwrap();

        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        loop {
            if lease.is_lost() {
                break;
            }
            if tokio::time::Instant::now() >= deadline {
                panic!("heartbeat never observed the stolen lease");
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }

        handle.abort();
    }

    // ---- T-510: boot-time lease clear ----

    #[tokio::test]
    async fn boot_clears_all_leases_on_epic_and_task() {
        let (state, _app) = test_app().await;
        let project_id = seed_project(&state).await;
        let epic_id = seed_epic(&state, &project_id, "InProgress").await;
        let task_id = seed_task(&state, &epic_id, &project_id, "A").await;
        let conn = state.db.conn();
        conn.execute(
            "UPDATE epic SET lease_owner = 'w', lease_expires_at = 99999999999 WHERE id = ?1",
            params![epic_id.clone()],
        )
        .await
        .unwrap();
        conn.execute(
            "UPDATE task SET lease_owner = 'w', lease_expires_at = 99999999999 WHERE id = ?1",
            params![task_id.clone()],
        )
        .await
        .unwrap();

        clear_all_leases(&state.db).await.unwrap();

        let (owner, expires) = epic_lease(&state, &epic_id).await;
        assert!(owner.is_none());
        assert!(expires.is_none());

        let mut rows = conn
            .query(
                "SELECT lease_owner, lease_expires_at FROM task WHERE id = ?1",
                params![task_id],
            )
            .await
            .unwrap();
        let row = rows.next().await.unwrap().unwrap();
        let t_owner: Option<String> = row.get(0).unwrap();
        let t_expires: Option<i64> = row.get(1).unwrap();
        assert!(t_owner.is_none());
        assert!(t_expires.is_none());
    }

    /// Clearing is a no-op (touches nothing, errors on nothing) when there is
    /// nothing to clear.
    #[tokio::test]
    async fn boot_clear_is_a_noop_with_no_leases_held() {
        let (state, _app) = test_app().await;
        let project_id = seed_project(&state).await;
        seed_epic(&state, &project_id, "InProgress").await;

        clear_all_leases(&state.db).await.unwrap();
    }

    // ---- T-510: pool concurrency ----

    /// A tiny async-friendly gate a test can hold N pipeline-body calls behind
    /// until it has observed the concurrency it wants, then release them all.
    /// Mirrors `planning::testing::Gate`'s one-shot-release shape but async
    /// (the pipeline body runs on the tokio runtime, not a blocking thread),
    /// using the standard check-register-check `Notify` pattern to avoid a
    /// missed-wakeup race between the `released` check and `notified().await`.
    struct ConcurrencyGate {
        active: AtomicUsize,
        released: std::sync::atomic::AtomicBool,
        notify: tokio::sync::Notify,
    }

    impl ConcurrencyGate {
        fn new() -> Arc<ConcurrencyGate> {
            Arc::new(ConcurrencyGate {
                active: AtomicUsize::new(0),
                released: std::sync::atomic::AtomicBool::new(false),
                notify: tokio::sync::Notify::new(),
            })
        }

        fn active(&self) -> usize {
            self.active.load(AtomicOrdering::SeqCst)
        }

        async fn enter(&self) {
            self.active.fetch_add(1, AtomicOrdering::SeqCst);
            loop {
                if self.released.load(AtomicOrdering::SeqCst) {
                    break;
                }
                let notified = self.notify.notified();
                if self.released.load(AtomicOrdering::SeqCst) {
                    break;
                }
                notified.await;
            }
            self.active.fetch_sub(1, AtomicOrdering::SeqCst);
        }

        fn release(&self) {
            self.released.store(true, AtomicOrdering::SeqCst);
            self.notify.notify_waiters();
        }
    }

    /// With `worker_concurrency = 2` and 3 enqueued (InProgress, unleased)
    /// epics, exactly 2 run concurrently: the pool only ever has 2 worker
    /// loops, so at most 2 claims can be outstanding at once. Proven
    /// deterministically (no sleeps) via the T-510 test-only pipeline hook:
    /// each claimed epic's body blocks in `ConcurrencyGate::enter` until the
    /// test releases it, so the test can poll (bounded) until exactly 2 are
    /// blocked, assert the 3rd epic is still unclaimed, then release and
    /// confirm all 3 eventually complete.
    #[tokio::test]
    async fn pool_runs_exactly_worker_concurrency_epics_at_once() {
        let db = Db::connect(":memory:").await.unwrap();
        db.run_migrations().await.unwrap();
        let mut config = Config::for_test(TOKEN);
        config.executor.worker_concurrency = 2;
        let state = AppState::with_agents(
            config,
            db,
            Arc::new(SilentPlanningAgent),
            Arc::new(SilentBreakdownAgent),
        );

        let project_id = seed_project(&state).await;
        let epic_a = seed_epic(&state, &project_id, "InProgress").await;
        let epic_b = seed_epic(&state, &project_id, "InProgress").await;
        let epic_c = seed_epic(&state, &project_id, "InProgress").await;
        for epic_id in [&epic_a, &epic_b, &epic_c] {
            seed_task(&state, epic_id, &project_id, "A").await;
        }

        let gate = ConcurrencyGate::new();
        let hook_gate = gate.clone();
        let state = state.with_pipeline_hook(Arc::new(move || {
            let gate = hook_gate.clone();
            Box::pin(async move { gate.enter().await })
        }));

        let _handles = spawn_pool(state.clone());
        state.notify.notify_waiters();

        // Bounded poll: wait until exactly 2 claimed-epic bodies are blocked
        // in the gate. With only 2 worker loops this is a ceiling, not a
        // race — the 3rd loop simply doesn't exist.
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        loop {
            if gate.active() == 2 {
                break;
            }
            if tokio::time::Instant::now() >= deadline {
                panic!("pool never reached 2 concurrently-claimed epics (active={})", gate.active());
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        assert_eq!(gate.active(), 2, "must not exceed worker_concurrency");

        // The 3rd epic must still be unclaimed — no 3rd worker loop exists to
        // claim it while the other two are held in the gate.
        let (c_owner, _) = epic_lease(&state, &epic_c).await;
        assert!(
            c_owner.is_none(),
            "a 3rd epic must remain unclaimed while worker_concurrency=2 workers are busy"
        );

        gate.release();

        // All 3 epics eventually complete (bounded poll; the released bodies
        // run to completion and the freed workers pick up the 3rd).
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        loop {
            let statuses: Vec<String> = futures_util::future::join_all(
                [&epic_a, &epic_b, &epic_c].map(|id| epic_status(&state, id)),
            )
            .await;
            if statuses.iter().all(|s| s == "Completed") {
                break;
            }
            if tokio::time::Instant::now() >= deadline {
                panic!("not all epics completed in time: {statuses:?}");
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }

    // ---- end-to-end AC test via the lane endpoint + pool ----

    /// Enqueue writes the contract shape: hitting `POST /epics/:id/lane
    /// { status: "InProgress" }` on a Ready epic with a task, with a worker
    /// pool running, drives the DAG to Completed. Since T-510 the lane
    /// handler itself spawns nothing — the pool (started here by the test,
    /// mirroring `main`) is what consumes the enqueue + notify.
    #[tokio::test]
    async fn enqueue_via_lane_drives_dag_to_completed() {
        let (state, app) = test_app().await;
        let project_id = seed_project(&state).await;
        let epic_id = seed_epic(&state, &project_id, "Ready").await;
        let a = seed_task(&state, &epic_id, &project_id, "A").await;
        let b = seed_task(&state, &epic_id, &project_id, "B").await;
        link(&state, &a, &b).await; // A → B.

        // Start the pool (T-510): the lane handler no longer spawns anything
        // itself, so a pool must be running to consume the enqueue+notify.
        let _handles = spawn_pool(state.clone());

        // Hit the lane endpoint — enqueues + notifies; the pool claims it.
        let response = app
            .clone()
            .oneshot(req(
                "POST",
                &format!("/epics/{epic_id}/lane"),
                Some(json!({ "status": "InProgress" })),
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(body_json(response).await["status"], "InProgress");

        // Poll the DB (bounded) until the epic is Completed.
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        loop {
            if epic_status(&state, &epic_id).await == "Completed" {
                break;
            }
            if tokio::time::Instant::now() >= deadline {
                panic!("worker pool did not complete the epic in time");
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        // Assert the §2.3 contract shape: lease NULL, epic Completed, all Done.
        let (lease_owner, lease_expires_at) = epic_lease(&state, &epic_id).await;
        assert!(lease_owner.is_none(), "lease_owner must be NULL");
        assert!(lease_expires_at.is_none(), "lease_expires_at must be NULL");
        assert_eq!(epic_status(&state, &epic_id).await, "Completed");

        let statuses = task_statuses(&state, &epic_id).await;
        assert_eq!(statuses["A"], "Done");
        assert_eq!(statuses["B"], "Done");
    }
}
