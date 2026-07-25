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
//! ([`run_epic_pipeline_inner`]) checks the handle at the top of the loop and
//! again immediately before each task's finalizing writes, abandoning the
//! item — no further writes — the moment it observes the loss.
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
//! ## The real implement walk (T-513)
//!
//! [`run_epic_pipeline_inner`] is the real DAG walk that replaced Milestone
//! 1's DB-only stub walk (that stub's pipeline functions are deleted
//! outright, not kept around behind a flag; see MILESTONE_2 §10's definition
//! of done). After [`workspace::provision_epic_workspace`] gates
//! entry exactly as before (see the section above), the walk processes
//! **ready** tasks (per [`compute_dag`]'s §2.3 readiness) one at a time, in
//! full, before ever looking for the next one:
//!
//! 1. **`base_sha`** — the workspace's current `HEAD` (`git rev-parse HEAD`),
//!    recorded on the task *before* anything else touches the tree. This has
//!    to happen now, not after the implement stage runs, because the
//!    implement stage's own commit (step 5) moves `HEAD` — capturing it any
//!    later would record the *wrong* base, and the whole reason `base_sha`
//!    exists (T-530's cumulative-diff review) is to diff against exactly the
//!    tree this task started from, not the tree some other step happened to
//!    leave behind.
//! 2. **`Todo → InProgress`**, publishing `dag_updated` — identical in shape
//!    to the M1 stub's transition, just earlier in a much longer step.
//! 3. **The D8 prompt**: [`crate::spec::build_context`] assembled from the
//!    task's own rendered spec, the epic's background (title/description/
//!    product & technical context), and a sibling manifest built from every
//!    *other* task in the epic, partitioned `Done` vs. not — this is what
//!    stops an autonomous implement agent from building the whole epic in
//!    one task (D7 gives it no other way to learn the epic's scope), so it is
//!    wired from the real DAG state on every run, never a bare spec string.
//! 4. **`Stage::Implement`** through the [`crate::task_agent::TaskAgent`]
//!    seam (`RunMode::Edit`, `cwd` = the provisioned workspace), evidence
//!    recorded by [`crate::task_agent::run_agent_stage`] exactly as T-512
//!    built it. A stage that does not come back `ok`
//!    ([`crate::task_agent::AgentStageOutcome::is_ok`]) — or fails to even
//!    start — routes the *epic* to `Blocked(agent_error)` via
//!    [`block_epic_on_agent_error`] and stops the walk. This is deliberately
//!    coarse: MILESTONE_2 §4 calls Phase 1 a tracer bullet and says anything
//!    that fails here blocks with `agent_error` and gets a real, structured
//!    failure taxonomy later (T-540/T-541); this slice does not attempt to
//!    distinguish *why* the stage failed.
//! 5. **`git add -A`**, then a commit **only if there is something to
//!    commit** ([`git::status_porcelain`] after staging) — an agent that made
//!    no changes (it judged the task already satisfied by earlier work) is
//!    committed as *nothing*, per MILESTONE_2 §4's explicit tracer-bullet AC;
//!    verifying that "no diff" genuinely means "already done" is
//!    [`crate::task_agent::Stage::VerifyComplete`]'s job, landing in T-532,
//!    not this one. A real commit uses the frozen §2.8 subject
//!    (`impl(<short task id>): <task title>`, [`crate::spec::short_id`]
//!    reused rather than re-derived) and a deterministic committer identity
//!    (`-c user.name=`/`-c user.email=`, never written to the workspace's own
//!    `.git/config` — see [`git::commit_all`]'s doc for why) so a commit
//!    succeeds even on a host with no configured global git identity. The
//!    resulting SHA is recorded in a `Stage::Commit` `agent_run` row's `log`
//!    (§2.2: "records the SHA in `log`") — opened only when a commit actually
//!    happens, matching D13's "every stage that runs gets a row", not "every
//!    stage that could have run".
//! 6. **`Done`**, publishing `dag_updated`. The loop then returns to its top:
//!    re-fetch the epic (still `InProgress`?), re-check the lease, recompute
//!    the DAG, and only *then* look for the next ready task — this is the
//!    same "one ready task at a time, no sibling ever `InProgress`
//!    concurrently" (§2.3) discipline the M1 stub already had, just now
//!    guarding a much more expensive step.
//!
//! ### Why the epic never reaches `Completed` here
//!
//! Unlike the M1 stub, this walk **never** sets `epic.status = 'Completed'`
//! when the DAG finishes. T-514 owns that transition — it only happens after
//! the epic's branch has been pushed and a PR has actually opened. An epic
//! whose DAG is fully `Done` but has not yet been pushed/PR'd is not "done"
//! in any sense a human watching the board should trust; T-513 stops the
//! walk and leaves the epic sitting `InProgress` (still holding its lease)
//! for whatever T-514 wiring runs next.
//!
//! ### Failure and cancellation both stop the walk the same way
//!
//! Every exit path out of the loop below — DAG fully done, DAG stuck, epic no
//! longer `InProgress`, lease lost, an implement/commit failure routed to
//! `Blocked(agent_error)` — is a plain `return`: no further writes, ever,
//! after the decision to stop. In particular, cancelling an epic mid-walk (a
//! lane move away from `InProgress`, or another worker stealing the lease)
//! is checked **both** at the top of the loop (the "between tasks" moment)
//! **and** again immediately after the implement stage returns but before
//! the commit/`Done` writes (a slow agent run racing an external cancel must
//! not finalize a task after the cancel landed) — mirroring the same
//! belt-and-suspenders re-check [`block_epic_on_provision_failure`]'s call
//! site already used for the provisioning-failure path. The full "kill the
//! in-flight agent process" mechanism is T-542's job, out of scope here; this
//! walk only guarantees that once a cancel/lease-loss is *observed*, nothing
//! further gets written.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use libsql::{params, Connection};
use tokio::task::JoinHandle;

use crate::board;
use crate::epics::{fetch_epic, get_epic_project_id};
use crate::evidence::{self, CloseStage, OpenStage};
use crate::git;
use crate::mcp;
use crate::spec::{self, EpicContext, SiblingTask, SpecFields, TaskContext};
use crate::task_agent::{self, AgentStageParams, Stage, TaskRunRequest};
use crate::tasks::compute_dag;
use crate::workspace::{self, ProvisionedWorkspace, ProvisionFailure};
use crate::AppState;

/// The deterministic git identity every T-513 commit is attributed to (§2.8's
/// "Commits" naming section fixes the *subject* format but not an identity —
/// this fills that gap). Passed as `-c user.name=`/`-c user.email=` on the
/// commit invocation itself ([`git::commit_all`]), never written to the
/// workspace's `.git/config`, so a commit succeeds even on a host with no
/// configured global git identity, and every Dearborn-authored commit is
/// attributable to the tool rather than to whatever OS user happens to run
/// the server process.
const COMMITTER_NAME: &str = "Dearborn";
const COMMITTER_EMAIL: &str = "dearborn@noreply.localhost";

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
    let body = tokio::spawn(run_epic_pipeline_inner(
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

/// Run the claimed-epic pipeline body to completion on `epic_id`,
/// lease-unaware (always treats the lease as held). Kept as the direct-call
/// seam tests use to drive the walk hermetically without going through the
/// claim/heartbeat machinery at all; the pool calls the lease-aware
/// [`run_epic_pipeline_inner`] instead (see [`try_claim_and_run`]).
pub async fn run_epic_pipeline(state: AppState, epic_id: String) {
    run_epic_pipeline_inner(state, epic_id, LeaseHandle::new()).await;
}

/// The claimed-epic pipeline body: workspace provisioning (T-511) followed by
/// the real per-task implement walk (T-513). See the module doc's "The real
/// implement walk" section for the full per-task sequence and the rationale
/// behind each step (`base_sha` timing, why the epic never reaches
/// `Completed` here, how failure and cancellation both stop the walk the
/// same way). This function is the orchestration shell around that sequence:
/// the provisioning gate, then a loop that re-validates the epic/lease before
/// every single task, processes exactly one task per iteration
/// ([`process_one_task`]), and returns the moment there is nothing left to do
/// or something says to stop.
///
/// Lease-aware (T-510): checks `lease.is_lost()` at the top of every loop
/// iteration and returns immediately, with no further writes, the moment the
/// heartbeat has flagged the lease as fenced out. Also awaits the T-510
/// test-only pipeline hook exactly once, before the first check, so a test
/// can gate/observe the body without sleeps (see
/// [`crate::AppState::test_pipeline_hook`]).
async fn run_epic_pipeline_inner(state: AppState, epic_id: String, lease: LeaseHandle) {
    #[cfg(test)]
    if let Some(hook) = state.test_pipeline_hook.clone() {
        hook().await;
    }

    // T-511: provision the workspace once per claim, before the walk below
    // ever runs. Only when the epic is actually InProgress — a claim racing a
    // Cancel/Block, or (defensively) any other status, must leave the epic
    // untouched here exactly as the walk's own status guard below would.
    let workspace = {
        if lease.is_lost() {
            return;
        }
        let conn = state.db.conn();
        let Ok(Some(epic)) = fetch_epic(conn, &epic_id).await else {
            return;
        };
        if epic.status != "InProgress" {
            return;
        }
        match workspace::provision_epic_workspace(&state, &epic_id, &epic.project_id).await {
            Ok(ws) => ws,
            Err(failure) => {
                // Re-check the lease right before writing: a slow
                // provisioning failure racing a fenced-out lease must not
                // stomp on the new owner's epic (mirrors the same
                // belt-and-suspenders fencing the walk's own writes use).
                if !lease.is_lost() {
                    block_epic_on_provision_failure(&state, &epic_id, failure).await;
                }
                return;
            }
        }
    };

    // ---- the real DAG walk (T-513) ----
    loop {
        // Lease-aware bail: a heartbeat renewal failure means another worker
        // now owns this epic. Stop writing immediately — any further mutation
        // here could race the new owner's own walk. Checked first thing on
        // every iteration — this is the "between tasks" re-check the module
        // doc describes.
        if lease.is_lost() {
            tracing::warn!(
                epic = %epic_id,
                "pipeline: lease lost (fenced out); abandoning without further writes"
            );
            return;
        }

        let conn = state.db.conn();

        // 1. Guard: only act on an InProgress epic. A Cancel/Block during the
        //    walk makes this a clean no-op — the other half of the "between
        //    tasks" re-check.
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

        // 3. Defensive: no task should ever be InProgress at loop-top — this
        //    walk fully serializes (one task claimed, run to a terminal
        //    state, before the next is even looked up), and any orphan left
        //    by a previous owner was already reset to Todo as part of the
        //    claim (`reset_orphaned_tasks`, called before this body ever
        //    runs). Seeing one here means the DAG cannot be trusted; stop
        //    rather than spin.
        if dag.nodes.iter().any(|n| n.task.status == "InProgress") {
            tracing::warn!(
                epic = %epic_id,
                "pipeline: found an InProgress task at loop-top (unexpected); stopping"
            );
            return;
        }

        // 4. Find a ready task (Todo + all blockers Done).
        let Some(ready) = dag.nodes.iter().find(|n| n.ready) else {
            // 5. No ready task.
            let all_done = dag.nodes.iter().all(|n| n.task.status == "Done");
            if all_done {
                // The DAG is fully Done (or the epic has no tasks at all).
                // T-514 — not this walk — is the only place that ever sets
                // `epic.status = 'Completed'`, and only after a PR opens; see
                // the module doc's "why the epic never reaches Completed
                // here" section. Publish the final DAG state and stop; the
                // epic stays InProgress, lease intact, until T-514's push+PR
                // step (not yet wired) runs.
                mcp::publish_dag(&state, &epic_id).await;
                tracing::info!(
                    epic = %epic_id,
                    "pipeline: DAG fully Done; stopping (T-514 owns the Completed transition)"
                );
            } else {
                // Some Todo tasks remain but none are ready (all blocked) and
                // none InProgress — the DAG cannot progress. A valid acyclic
                // DAG walked in dependency order never hits this (cycles are
                // rejected at link time). Log and stop; do NOT infinite-loop.
                tracing::warn!(
                    epic = %epic_id,
                    "pipeline: no ready task but not all Done; DAG is stuck; stopping"
                );
            }
            return;
        };

        match process_one_task(&state, &epic_id, &epic, &dag, ready, &workspace, &lease).await {
            TaskStepOutcome::Continue => continue,
            TaskStepOutcome::Stop => return,
        }
    }
}

/// What [`process_one_task`] tells the walk's loop to do next.
enum TaskStepOutcome {
    /// The task reached a terminal state (`Done`, committed or not); loop
    /// back to the top and look for the next ready task.
    Continue,
    /// Something said to stop: a failure (routed to `Blocked(agent_error)`),
    /// a cancelled/fenced-out epic observed mid-task, or a git-level error.
    /// The caller returns immediately — no further writes.
    Stop,
}

/// Process exactly one ready task through the full T-513 sequence: record
/// `base_sha`, `Todo → InProgress`, assemble the D8 prompt, run
/// `Stage::Implement`, `git add -A` + commit-if-dirty, `Done`. See the module
/// doc's "The real implement walk" section for the rationale behind each
/// step; this function is the literal implementation of that sequence.
async fn process_one_task(
    state: &AppState,
    epic_id: &str,
    epic: &crate::epics::Epic,
    dag: &crate::tasks::Dag,
    ready: &crate::tasks::DagNode,
    workspace: &ProvisionedWorkspace,
    lease: &LeaseHandle,
) -> TaskStepOutcome {
    let conn = state.db.conn();
    let task_id = ready.task.id.clone();
    let task_title = ready.task.title.clone();
    let task_description = ready.task.description.clone();
    let task_acceptance = ready.task.acceptance.clone();

    // The sibling manifest (D8): every *other* task in the epic, partitioned
    // Done vs. not by `build_context` below. Built from the DAG we already
    // hold (fresher than any separate query could be, and avoids a second
    // round trip) rather than re-querying the tasks table.
    let siblings: Vec<(String, String, bool)> = dag
        .nodes
        .iter()
        .filter(|n| n.task.id != task_id)
        .map(|n| (n.task.id.clone(), n.task.title.clone(), n.task.status == "Done"))
        .collect();

    // 1. base_sha: the workspace's HEAD *before* this task's work — recorded
    //    now, before the implement stage (or its eventual commit) can move
    //    HEAD out from under us. See the module doc for why this ordering is
    //    load-bearing, not incidental.
    let base_sha = match git::current_commit(&workspace.workspace_path).await {
        Ok(sha) => sha,
        Err(err) => {
            if !lease.is_lost() {
                block_epic_on_agent_error(
                    state,
                    epic_id,
                    &task_id,
                    &format!("failed to read base_sha: {err}"),
                )
                .await;
            }
            return TaskStepOutcome::Stop;
        }
    };

    let now = now_ms();
    let _ = conn
        .execute(
            "UPDATE task SET status = 'InProgress', base_sha = ?1, updated_at = ?2 WHERE id = ?3",
            params![base_sha, now, task_id.clone()],
        )
        .await;
    mcp::publish_dag(state, epic_id).await;

    // 2. The D8 prompt: rendered spec + epic background + sibling manifest.
    let sibling_refs: Vec<SiblingTask> = siblings
        .iter()
        .map(|(id, title, done)| SiblingTask {
            id,
            title,
            done: *done,
        })
        .collect();
    let epic_ctx = EpicContext {
        title: &epic.title,
        description: epic.description.as_deref(),
        product_context: epic.product_context.as_deref(),
        technical_context: epic.technical_context.as_deref(),
    };
    let task_ctx = TaskContext {
        spec: SpecFields {
            title: &task_title,
            description: task_description.as_deref(),
            acceptance: task_acceptance.as_deref(),
        },
        epic: Some(epic_ctx),
        siblings: &sibling_refs,
    };
    let prompt = task_agent::assemble_prompt(Stage::Implement, &task_ctx)
        .expect("Stage::Implement always has a prompt (spec::prompt_for)");

    // 3. Run the implement stage through the TaskAgent seam.
    let run_id = ulid::Ulid::new().to_string();
    let req = TaskRunRequest {
        run_id,
        stage: Stage::Implement,
        prompt,
        cwd: workspace.workspace_path.clone(),
    };
    let outcome = task_agent::run_agent_stage(
        state,
        &*state.task_agent,
        AgentStageParams {
            task_id: &task_id,
            epic_id: Some(epic_id),
            attempt: 1,
        },
        req,
    )
    .await;

    let outcome = match outcome {
        Ok(outcome) => outcome,
        Err(err) => {
            if !lease.is_lost() {
                block_epic_on_agent_error(
                    state,
                    epic_id,
                    &task_id,
                    &format!("implement stage failed to start: {err}"),
                )
                .await;
            }
            return TaskStepOutcome::Stop;
        }
    };

    // MILESTONE_2 §4 (tracer bullet): anything that fails here Blocks the
    // epic with `agent_error`; a real, structured failure taxonomy is T-540's
    // job, not this slice's.
    if !outcome.is_ok() {
        if !lease.is_lost() {
            block_epic_on_agent_error(
                state,
                epic_id,
                &task_id,
                "implement stage did not complete successfully",
            )
            .await;
        }
        return TaskStepOutcome::Stop;
    }

    // Re-check the epic's status *and* the lease immediately before the
    // commit/Done writes below — a slow implement run racing an external
    // cancel (a lane move away from InProgress) or a lease theft must not
    // finalize this task after either happened. This is the "cancelling
    // mid-walk stops cleanly" AC; the full kill-the-in-flight-agent path is
    // T-542's job.
    let still_in_progress =
        matches!(fetch_epic(conn, epic_id).await, Ok(Some(e)) if e.status == "InProgress");
    if lease.is_lost() || !still_in_progress {
        tracing::warn!(
            epic = %epic_id,
            task = %task_id,
            "pipeline: epic cancelled or lease lost mid-task; stopping without finalizing"
        );
        return TaskStepOutcome::Stop;
    }

    // 4. git add -A, then commit iff there is something to commit. An agent
    //    that made no changes is committed as *nothing* — see the module doc
    //    for why (T-532 owns verifying that "no diff" really means done).
    if let Err(err) = git::add_all(&workspace.workspace_path).await {
        if !lease.is_lost() {
            block_epic_on_agent_error(
                state,
                epic_id,
                &task_id,
                &format!("git add -A failed: {err}"),
            )
            .await;
        }
        return TaskStepOutcome::Stop;
    }
    let status = match git::status_porcelain(&workspace.workspace_path).await {
        Ok(status) => status,
        Err(err) => {
            if !lease.is_lost() {
                block_epic_on_agent_error(
                    state,
                    epic_id,
                    &task_id,
                    &format!("git status failed: {err}"),
                )
                .await;
            }
            return TaskStepOutcome::Stop;
        }
    };

    if !status.trim().is_empty() {
        // §2.8's frozen commit subject, reusing spec::short_id rather than
        // re-deriving the "last 6 of id" convention.
        let subject = format!("impl({}): {}", spec::short_id(&task_id), task_title);
        match git::commit_all(
            &workspace.workspace_path,
            &subject,
            COMMITTER_NAME,
            COMMITTER_EMAIL,
        )
        .await
        {
            Ok(sha) => {
                // §2.2: the Commit stage "records the SHA in log". Opened
                // only now that a commit actually happened (D13: every stage
                // that *runs* gets a row, not every stage that could have).
                let open = OpenStage {
                    task_id: Some(&task_id),
                    epic_id: Some(epic_id),
                    stage: Stage::Commit.as_str(),
                    attempt: 1,
                };
                if let Ok(handle) = evidence::open_stage(conn, open).await {
                    let _ = evidence::close_stage(
                        conn,
                        &handle,
                        CloseStage {
                            status: "ok",
                            session_id: None,
                            verdict: None,
                            exit_code: Some(0),
                            log: format!("commit {sha}: {subject}"),
                        },
                    )
                    .await;
                }
            }
            Err(err) => {
                if !lease.is_lost() {
                    block_epic_on_agent_error(
                        state,
                        epic_id,
                        &task_id,
                        &format!("git commit failed: {err}"),
                    )
                    .await;
                }
                return TaskStepOutcome::Stop;
            }
        }
    }

    // 5. Done.
    if lease.is_lost() {
        return TaskStepOutcome::Stop;
    }
    let now = now_ms();
    let _ = conn
        .execute(
            "UPDATE task SET status = 'Done', updated_at = ?1 WHERE id = ?2",
            params![now, task_id.clone()],
        )
        .await;
    mcp::publish_dag(state, epic_id).await;

    TaskStepOutcome::Continue
}

/// The shared "flip the epic to Blocked" write + publish, factored out so
/// [`block_epic_on_provision_failure`] (T-511) and [`block_epic_on_agent_error`]
/// (T-513) share one implementation instead of two copies of the same
/// UPDATE/publish sequence. `status='Blocked'`, `blocked_reason = reason`,
/// publish `epic_updated` + `board_updated`. The workspace is **retained** —
/// this never deletes anything; deletion only ever happens after a PR opens
/// (T-514). Fenced by `status = 'InProgress'` so a transition that already
/// happened out from under us (a Cancel racing this same moment) is a no-op
/// rather than an overwrite.
async fn set_epic_blocked(state: &AppState, epic_id: &str, reason: &str) {
    let conn = state.db.conn();
    let now = now_ms();
    let _ = conn
        .execute(
            "UPDATE epic SET status = 'Blocked', blocked_reason = ?1, updated_at = ?2 \
             WHERE id = ?3 AND status = 'InProgress'",
            params![reason, now, epic_id],
        )
        .await;

    if let Ok(Some(updated)) = fetch_epic(conn, epic_id).await {
        let payload = serde_json::to_value(&updated).unwrap_or(serde_json::Value::Null);
        state
            .hub
            .publish(&format!("epic:{epic_id}"), "epic_updated", payload);
        board::publish_board(state, &updated.project_id).await;
    }
}

/// Route a [`ProvisionFailure`] (T-511) to the epic `Blocked` transition via
/// [`set_epic_blocked`]: `blocked_reason` per §2.3 (`workspace_error` |
/// `setup_failed`).
async fn block_epic_on_provision_failure(state: &AppState, epic_id: &str, failure: ProvisionFailure) {
    let (reason, log_message) = match failure {
        ProvisionFailure::Workspace(message) => ("workspace_error", message),
        ProvisionFailure::Setup { message, exit_code } => {
            ("setup_failed", format!("exit_code={exit_code:?}: {message}"))
        }
    };
    tracing::warn!(
        epic = %epic_id,
        reason,
        error = %log_message,
        "workspace provisioning failed; epic -> Blocked"
    );
    set_epic_blocked(state, epic_id, reason).await;
}

/// Route a T-513 implement-walk failure (a failed/erroring implement stage,
/// or a git-level failure reading `base_sha`/staging/committing) to the epic
/// `Blocked` transition via [`set_epic_blocked`], with `blocked_reason =
/// 'agent_error'` — the single, coarse tracer-bullet reason MILESTONE_2 §4
/// specifies for Phase 1 ("anything that fails here Blocks the epic with
/// `agent_error` and gets thickened in later phases": T-540's structured
/// failure taxonomy, T-541's retry).
async fn block_epic_on_agent_error(state: &AppState, epic_id: &str, task_id: &str, message: &str) {
    tracing::warn!(
        epic = %epic_id,
        task = %task_id,
        error = %message,
        "task pipeline step failed; epic -> Blocked(agent_error)"
    );
    set_epic_blocked(state, epic_id, "agent_error").await;
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
    use crate::planning::testing::{Gate, SilentPlanningAgent};
    use crate::task_agent::testing::{ScriptedRun, ScriptedTaskAgent};
    use crate::{app, Config, Db, TaskAgent};
    use axum::body::Body;
    use axum::http::{
        header::{AUTHORIZATION, CONTENT_TYPE},
        Request, StatusCode,
    };
    use harness::{HarnessError, RunEvent, RunHandle};
    use libsql::params;
    use serde_json::{json, Value};
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};
    use std::sync::mpsc::Receiver;
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

    /// Boot an app over an in-memory db with silent planning/breakdown agents
    /// and a bare [`ScriptedTaskAgent`] (no scripted runs — every stage falls
    /// back to [`crate::task_agent::testing::ScriptedRun::default`]: exit 0,
    /// no files written, i.e. a no-op success). Fine for every test that
    /// doesn't care what the implement stage does, just that it succeeds
    /// (fast, via `Config::for_test`). Returns (state, app).
    async fn test_app() -> (AppState, axum::Router) {
        test_app_with_task_agent(Arc::new(ScriptedTaskAgent::new())).await
    }

    /// Like [`test_app`] but with an explicit [`TaskAgent`] — the seam T-513's
    /// tests use to script the implement stage's behavior (write files,
    /// fail, or gate in-flight) instead of accepting the bare no-op default.
    async fn test_app_with_task_agent(task_agent: Arc<dyn TaskAgent>) -> (AppState, axum::Router) {
        let db = Db::connect(":memory:").await.unwrap();
        db.run_migrations().await.unwrap();
        let state = AppState::with_all_agents(
            Config::for_test(TOKEN),
            db,
            Arc::new(SilentPlanningAgent),
            Arc::new(SilentBreakdownAgent),
            task_agent,
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

    // ---- T-511: a real (local, hermetic) git fixture for pipeline-body tests ----
    //
    // Since T-511, the claimed-epic body provisions a workspace (a real
    // `git clone`/`git fetch`) before the DAG walk. Any test that drives the
    // body to completion (`run_epic_pipeline`, or the pool via `spawn_pool`)
    // needs a project whose `clone_path`/`repo_url` point at something git can
    // actually clone from — the plain `seed_project` above (no `clone_path`,
    // a fake `repo_url`) is intentionally kept for the claim/heartbeat/lease
    // tests that never reach provisioning (they call `claim_epic`/
    // `renew_lease_once` directly, or seed a non-`InProgress` epic).

    /// A local git fixture: `git init`'s a source repo with one commit in a
    /// fresh temp dir, entirely offline. Cleans itself up on drop.
    struct GitFixture {
        dir: std::path::PathBuf,
    }

    impl GitFixture {
        async fn new() -> GitFixture {
            let dir = std::env::temp_dir().join(format!(
                "dearborn-worker-fixture-{}-{}",
                std::process::id(),
                ulid::Ulid::new()
            ));
            std::fs::create_dir_all(&dir).unwrap();
            for args in [
                &["init", "-b", "main"][..],
                &["config", "user.email", "test@example.com"],
                &["config", "user.name", "Test"],
            ] {
                git_ok(&dir, args).await;
            }
            std::fs::write(dir.join("README.md"), "hello\n").unwrap();
            git_ok(&dir, &["add", "."]).await;
            git_ok(&dir, &["commit", "-m", "init"]).await;
            GitFixture { dir }
        }

        fn path_str(&self) -> String {
            self.dir.to_string_lossy().to_string()
        }
    }

    impl Drop for GitFixture {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.dir);
        }
    }

    async fn git_ok(dir: &std::path::Path, args: &[&str]) {
        let status = tokio::process::Command::new("git")
            .args(args)
            .current_dir(dir)
            .status()
            .await
            .unwrap();
        assert!(status.success(), "git {args:?} failed in {dir:?}");
    }

    /// Like [`seed_project`] but with a real `clone_path`/`repo_url` pointing
    /// at `fixture`, so a claimed epic under this project can actually
    /// provision a workspace (T-511) instead of failing `workspace_error`.
    async fn seed_project_with_workspace(state: &AppState, fixture: &GitFixture) -> String {
        let conn = state.db.conn();
        let id = ulid::Ulid::new().to_string();
        let now = now_ms();
        let clone_path = std::path::Path::new(&state.config.clone_root).join(&id);
        conn.execute(
            "INSERT INTO project (id, name, repo_url, clone_path, clone_status, created_at, updated_at) \
             VALUES (?1, 'P', ?2, ?3, 'ready', ?4, ?4)",
            params![
                id.clone(),
                fixture.path_str(),
                clone_path.to_string_lossy().to_string(),
                now
            ],
        )
        .await
        .unwrap();
        id
    }

    /// Remove the on-disk clone directories a `seed_project_with_workspace`
    /// test created, so repeated local runs don't accumulate temp dirs.
    fn cleanup_clone_root(state: &AppState, project_id: &str, epic_ids: &[&str]) {
        let root = std::path::Path::new(&state.config.clone_root);
        let _ = std::fs::remove_dir_all(root.join(project_id));
        for epic_id in epic_ids {
            let _ = std::fs::remove_dir_all(root.join("epics").join(epic_id));
        }
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

    // ---- run_epic_pipeline direct tests: real DAG walk (T-513) ----
    //
    // These use the bare `test_app()` (a `ScriptedTaskAgent` with no scripted
    // runs, i.e. every implement stage is a no-op success — see `test_app`'s
    // doc). A no-op implement stage produces no diff, so no commit ever
    // lands for these tests; that's fine, they're only asserting the DAG
    // walk's task-status/epic-status contract, not the commit machinery
    // (covered separately below). Note the epic is asserted to stay
    // `InProgress`, never `Completed` — T-513 never sets that transition;
    // only T-514 does, after a PR opens (see the module doc).

    /// Linear DAG (A → B → C): after the walk, all Done, epic still
    /// InProgress.
    ///
    /// The dependency ORDER is respected implicitly: B can only become ready
    /// after A is Done (its only blocker), and C after B. So asserting the
    /// final state (all Done) IS the order assertion — a reversed walk could
    /// never reach all-Done. See `implement_stage_runs_respect_dependency_order`
    /// below for a stronger, order-observing proof.
    #[tokio::test]
    async fn linear_dag_walks_every_task_to_done_epic_stays_in_progress() {
        let (state, _app) = test_app().await;
        let fixture = GitFixture::new().await;
        let project_id = seed_project_with_workspace(&state, &fixture).await;
        let epic_id = seed_epic(&state, &project_id, "InProgress").await;

        let a = seed_task(&state, &epic_id, &project_id, "A").await;
        let b = seed_task(&state, &epic_id, &project_id, "B").await;
        let c = seed_task(&state, &epic_id, &project_id, "C").await;
        // A blocks B, B blocks C (A → B → C).
        link(&state, &a, &b).await;
        link(&state, &b, &c).await;

        run_epic_pipeline(state.clone(), epic_id.clone()).await;

        let statuses = task_statuses(&state, &epic_id).await;
        assert_eq!(statuses["A"], "Done");
        assert_eq!(statuses["B"], "Done");
        assert_eq!(statuses["C"], "Done");
        assert_eq!(
            epic_status(&state, &epic_id).await,
            "InProgress",
            "T-513 never sets Completed — that's T-514's job, after a PR opens"
        );
        cleanup_clone_root(&state, &project_id, &[&epic_id]);
    }

    /// Branching DAG (A blocks B and C; B and C both block D): all Done,
    /// epic still InProgress.
    #[tokio::test]
    async fn branching_dag_walks_every_task_to_done_epic_stays_in_progress() {
        let (state, _app) = test_app().await;
        let fixture = GitFixture::new().await;
        let project_id = seed_project_with_workspace(&state, &fixture).await;
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

        run_epic_pipeline(state.clone(), epic_id.clone()).await;

        let statuses = task_statuses(&state, &epic_id).await;
        assert_eq!(statuses["A"], "Done");
        assert_eq!(statuses["B"], "Done");
        assert_eq!(statuses["C"], "Done");
        assert_eq!(statuses["D"], "Done");
        assert_eq!(epic_status(&state, &epic_id).await, "InProgress");
        cleanup_clone_root(&state, &project_id, &[&epic_id]);
    }

    /// Empty epic (no tasks): the walk finds the (vacuously) fully-Done DAG
    /// immediately and stops; the epic stays InProgress (never Completed —
    /// T-514's job).
    #[tokio::test]
    async fn empty_epic_leaves_epic_in_progress() {
        let (state, _app) = test_app().await;
        let fixture = GitFixture::new().await;
        let project_id = seed_project_with_workspace(&state, &fixture).await;
        let epic_id = seed_epic(&state, &project_id, "InProgress").await;

        run_epic_pipeline(state.clone(), epic_id.clone()).await;

        assert_eq!(epic_status(&state, &epic_id).await, "InProgress");
        cleanup_clone_root(&state, &project_id, &[&epic_id]);
    }

    /// Non-InProgress epic is a no-op: no task or epic status changes (the
    /// walk never even reaches provisioning).
    #[tokio::test]
    async fn non_in_progress_epic_is_no_op() {
        let (state, _app) = test_app().await;
        let project_id = seed_project(&state).await;
        let epic_id = seed_epic(&state, &project_id, "Ready").await;
        seed_task(&state, &epic_id, &project_id, "A").await;

        run_epic_pipeline(state.clone(), epic_id.clone()).await;

        let statuses = task_statuses(&state, &epic_id).await;
        assert_eq!(statuses["A"], "Todo", "task untouched");
        assert_eq!(epic_status(&state, &epic_id).await, "Ready", "epic untouched");
    }

    /// No sibling InProgress invariant: after a full run, the final state is
    /// consistent — all Done, none InProgress, epic still InProgress. The
    /// walk serializes by construction (one ready task at a time); this
    /// final-state assertion confirms it. See
    /// `implement_stage_never_observes_a_sibling_in_progress` below for a
    /// stronger, moment-by-moment proof via the DB itself.
    #[tokio::test]
    async fn no_sibling_in_progress_after_run() {
        let (state, _app) = test_app().await;
        let fixture = GitFixture::new().await;
        let project_id = seed_project_with_workspace(&state, &fixture).await;
        let epic_id = seed_epic(&state, &project_id, "InProgress").await;
        let a = seed_task(&state, &epic_id, &project_id, "A").await;
        let b = seed_task(&state, &epic_id, &project_id, "B").await;
        // A and B are independent (no edge between them) — both are ready from
        // the start. The walk still claims one at a time.
        link(&state, &a, &b).await; // A → B: only A is ready initially.

        run_epic_pipeline(state.clone(), epic_id.clone()).await;

        let statuses = task_statuses(&state, &epic_id).await;
        assert_eq!(statuses["A"], "Done");
        assert_eq!(statuses["B"], "Done");
        assert!(statuses.values().all(|s| s != "InProgress"));
        assert_eq!(epic_status(&state, &epic_id).await, "InProgress");
        cleanup_clone_root(&state, &project_id, &[&epic_id]);
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
        let state = AppState::with_all_agents(
            config,
            db,
            Arc::new(SilentPlanningAgent),
            Arc::new(SilentBreakdownAgent),
            Arc::new(ScriptedTaskAgent::new()),
        );

        let fixture = GitFixture::new().await;
        let project_id = seed_project_with_workspace(&state, &fixture).await;
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

        // All 3 epics' tasks eventually reach Done (bounded poll; the
        // released bodies run to completion and the freed workers pick up
        // the 3rd). The epics themselves stay InProgress — T-513 never sets
        // Completed (T-514's job, after a PR opens).
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        loop {
            let all_a = task_statuses(&state, &epic_a).await;
            let all_b = task_statuses(&state, &epic_b).await;
            let all_c = task_statuses(&state, &epic_c).await;
            let done = |m: &std::collections::HashMap<String, String>| m.get("A").map(|s| s == "Done").unwrap_or(false);
            if done(&all_a) && done(&all_b) && done(&all_c) {
                break;
            }
            if tokio::time::Instant::now() >= deadline {
                panic!("not all epics' tasks reached Done in time: {all_a:?} {all_b:?} {all_c:?}");
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }

        for epic_id in [&epic_a, &epic_b, &epic_c] {
            assert_eq!(epic_status(&state, epic_id).await, "InProgress");
        }

        cleanup_clone_root(&state, &project_id, &[&epic_a, &epic_b, &epic_c]);
    }

    // ---- end-to-end AC test via the lane endpoint + pool ----

    /// Enqueue writes the contract shape: hitting `POST /epics/:id/lane
    /// { status: "InProgress" }` on a Ready epic with a task, with a worker
    /// pool running, drives the DAG to Done. Since T-510 the lane handler
    /// itself spawns nothing — the pool (started here by the test, mirroring
    /// `main`) is what consumes the enqueue + notify. The epic itself stays
    /// InProgress with its lease released — T-513 never sets Completed.
    #[tokio::test]
    async fn enqueue_via_lane_drives_dag_to_done() {
        let (state, app) = test_app().await;
        let fixture = GitFixture::new().await;
        let project_id = seed_project_with_workspace(&state, &fixture).await;
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

        // Poll the DB (bounded) until both tasks are Done.
        //
        // Deliberately NOT also asserting the lease is released at some
        // stable snapshot: since T-513 leaves the epic InProgress once its
        // DAG is fully Done (T-514 is the only thing that ever moves it to
        // Completed, removing it from the claim predicate's `status =
        // 'InProgress'` match), a fully-Done-but-still-InProgress epic
        // remains claimable — the pool's worker loop immediately re-claims
        // it, re-attaches the workspace, finds nothing left to do, and
        // releases again, repeatedly, until either the process stops or
        // T-514 lands. That makes "lease is None" a constantly-flickering,
        // not a stable, snapshot here; asserting on it would be flaky by
        // construction rather than by bug. §2.3's "lease NULL once claimable
        // work is exhausted" contract shape is exercised properly by
        // `pool_runs_exactly_worker_concurrency_epics_at_once` and the
        // claim/heartbeat-specific tests above, against epics that actually
        // reach a terminal, non-claimable status.
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        loop {
            let statuses = task_statuses(&state, &epic_id).await;
            if statuses.get("A").map(|s| s == "Done").unwrap_or(false)
                && statuses.get("B").map(|s| s == "Done").unwrap_or(false)
            {
                break;
            }
            if tokio::time::Instant::now() >= deadline {
                panic!("worker pool did not walk the DAG to Done in time: {statuses:?}");
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        assert_eq!(
            epic_status(&state, &epic_id).await,
            "InProgress",
            "T-513 never sets Completed — that's T-514's job, after a PR opens"
        );

        let statuses = task_statuses(&state, &epic_id).await;
        assert_eq!(statuses["A"], "Done");
        assert_eq!(statuses["B"], "Done");
        cleanup_clone_root(&state, &project_id, &[&epic_id]);
    }

    // ---- T-511: provisioning-failure wiring (workspace_error / setup_failed) ----

    /// A project whose repo is unreachable (mirrors `git.rs`'s own bad-url
    /// fixture): the canonical refresh inside provisioning fails fast
    /// (`GIT_TERMINAL_PROMPT=0`), forcing `ProvisionFailure::Workspace`.
    async fn seed_project_bad_repo(state: &AppState) -> String {
        let conn = state.db.conn();
        let id = ulid::Ulid::new().to_string();
        let now = now_ms();
        let clone_path = std::path::Path::new(&state.config.clone_root).join(&id);
        conn.execute(
            "INSERT INTO project (id, name, repo_url, clone_path, clone_status, created_at, updated_at) \
             VALUES (?1, 'P', 'https://dearborn.invalid/nope/nope.git', ?2, 'ready', ?3, ?3)",
            params![id.clone(), clone_path.to_string_lossy().to_string(), now],
        )
        .await
        .unwrap();
        id
    }

    /// Like [`seed_project_with_workspace`] but with a `setup_cmd`, so a
    /// provisioned workspace's setup step can be made to fail on demand.
    async fn seed_project_with_setup_cmd(
        state: &AppState,
        fixture: &GitFixture,
        setup_cmd: &str,
    ) -> String {
        let conn = state.db.conn();
        let id = ulid::Ulid::new().to_string();
        let now = now_ms();
        let clone_path = std::path::Path::new(&state.config.clone_root).join(&id);
        conn.execute(
            "INSERT INTO project (id, name, repo_url, setup_cmd, clone_path, clone_status, created_at, updated_at) \
             VALUES (?1, 'P', ?2, ?3, ?4, 'ready', ?5, ?5)",
            params![
                id.clone(),
                fixture.path_str(),
                setup_cmd,
                clone_path.to_string_lossy().to_string(),
                now
            ],
        )
        .await
        .unwrap();
        id
    }

    /// Drain `sub` (bounded) until an `epic_updated` frame carrying
    /// `status` matches, or panic after 5s. Draining rather than asserting a
    /// fixed frame position keeps this robust against the lane handler's own
    /// `Ready → InProgress` `epic_updated`/`board_updated` publishes landing
    /// on the same subscriber ahead of the provisioning-failure ones.
    async fn recv_epic_updated_with_status(
        sub: &mut tokio::sync::broadcast::Receiver<crate::hub::Envelope>,
        status: &str,
    ) -> Value {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        loop {
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            let frame = tokio::time::timeout(remaining, sub.recv())
                .await
                .unwrap_or_else(|_| panic!("never saw epic_updated(status={status})"))
                .unwrap();
            let v: Value = serde_json::from_str(&frame).unwrap();
            if v["type"] == "epic_updated" && v["payload"]["status"] == status {
                return v;
            }
        }
    }

    /// A workspace-provisioning failure (unreachable repo) drives the epic to
    /// `Blocked(workspace_error)`: the lease is released, the seeded task
    /// never leaves `Todo` (the stub DAG walk never runs), and both the
    /// `epic_updated` and `board_updated` frames land.
    #[tokio::test]
    async fn workspace_error_blocks_epic_releases_lease_and_publishes() {
        let (state, app) = test_app().await;
        let project_id = seed_project_bad_repo(&state).await;
        let epic_id = seed_epic(&state, &project_id, "Ready").await;
        seed_task(&state, &epic_id, &project_id, "A").await;

        let mut epic_sub = state.hub.subscribe(&format!("epic:{epic_id}"));
        let mut proj_sub = state.hub.subscribe(&format!("project:{project_id}"));

        let _handles = spawn_pool(state.clone());
        let response = app
            .oneshot(req(
                "POST",
                &format!("/epics/{epic_id}/lane"),
                Some(json!({ "status": "InProgress" })),
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let blocked_frame = recv_epic_updated_with_status(&mut epic_sub, "Blocked").await;
        assert_eq!(blocked_frame["payload"]["blocked_reason"], "workspace_error");

        // board_updated must have landed too (either for the InProgress
        // enqueue, the Blocked transition, or both) — drain for one.
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        loop {
            let frame = tokio::time::timeout(Duration::from_secs(5), proj_sub.recv())
                .await
                .expect("never saw a board_updated frame")
                .unwrap();
            let v: Value = serde_json::from_str(&frame).unwrap();
            if v["type"] == "board_updated" {
                break;
            }
            if tokio::time::Instant::now() >= deadline {
                panic!("never saw board_updated");
            }
        }

        let epic = fetch_epic(state.db.conn(), &epic_id).await.unwrap().unwrap();
        assert_eq!(epic.status, "Blocked");
        assert_eq!(epic.blocked_reason.as_deref(), Some("workspace_error"));

        let (lease_owner, lease_expires_at) = epic_lease(&state, &epic_id).await;
        assert!(lease_owner.is_none(), "lease must be released on Blocked");
        assert!(lease_expires_at.is_none());

        // The DAG walk never ran: the seeded task is still Todo.
        let statuses = task_statuses(&state, &epic_id).await;
        assert_eq!(statuses["A"], "Todo");
    }

    /// A failing `setup_cmd` drives the epic to `Blocked(setup_failed)` with
    /// the workspace retained on disk (never deleted) and the captured
    /// output landed in an `agent_run` row.
    #[tokio::test]
    async fn setup_cmd_failure_blocks_epic_and_retains_workspace() {
        let (state, app) = test_app().await;
        let fixture = GitFixture::new().await;
        let project_id =
            seed_project_with_setup_cmd(&state, &fixture, "echo setup-boom && exit 5").await;
        let epic_id = seed_epic(&state, &project_id, "Ready").await;
        seed_task(&state, &epic_id, &project_id, "A").await;

        let _handles = spawn_pool(state.clone());
        let response = app
            .oneshot(req(
                "POST",
                &format!("/epics/{epic_id}/lane"),
                Some(json!({ "status": "InProgress" })),
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        loop {
            if epic_status(&state, &epic_id).await == "Blocked" {
                break;
            }
            if tokio::time::Instant::now() >= deadline {
                panic!("epic never reached Blocked");
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        let epic = fetch_epic(state.db.conn(), &epic_id).await.unwrap().unwrap();
        assert_eq!(epic.blocked_reason.as_deref(), Some("setup_failed"));

        // Workspace retained: the provisioned directory (and its .git) is
        // still on disk, not deleted on this failure path.
        let workspace_path = crate::workspace::epic_workspace_path(&state.config.clone_root, &epic_id);
        assert!(
            workspace_path.join(".git").exists(),
            "workspace must be retained on setup_failed"
        );

        // Evidence: the captured setup_cmd output landed in agent_run.
        let mut rows = state
            .db
            .conn()
            .query(
                "SELECT status, exit_code, log FROM agent_run WHERE epic_id = ?1 AND stage = 'setup'",
                params![epic_id.clone()],
            )
            .await
            .unwrap();
        let row = rows.next().await.unwrap().expect("a setup agent_run row");
        assert_eq!(row.get::<String>(0).unwrap(), "error");
        assert_eq!(row.get::<Option<i64>>(1).unwrap(), Some(5));
        let log: String = row.get(2).unwrap();
        assert!(log.contains("setup-boom"));

        cleanup_clone_root(&state, &project_id, &[&epic_id]);
    }

    // ---- T-513: the real implement walk's commit machinery -------------------

    /// Read `git log`'s subjects, oldest first, in `dir`. Used to prove the
    /// walk's commit *order* and *subjects* directly from git itself rather
    /// than trusting the DB's task-status transitions alone.
    async fn git_log_subjects(dir: &std::path::Path) -> Vec<String> {
        let output = tokio::process::Command::new("git")
            .args(["log", "--reverse", "--format=%s"])
            .current_dir(dir)
            .output()
            .await
            .unwrap();
        assert!(output.status.success(), "git log failed: {output:?}");
        String::from_utf8_lossy(&output.stdout)
            .lines()
            .map(|s| s.to_string())
            .collect()
    }

    fn writes_file(path: &str, content: &str) -> ScriptedRun {
        ScriptedRun {
            files: vec![(PathBuf::from(path), content.to_string())],
            ..ScriptedRun::default()
        }
    }

    /// A linear DAG (A → B → C) with a `ScriptedTaskAgent` that writes a
    /// distinct file per task: exactly one commit lands per task, each with
    /// the §2.8 subject `impl(<short task id>): <title>`, in dependency
    /// order — read directly out of `git log`, not just inferred from task
    /// statuses.
    #[tokio::test]
    async fn implement_writes_produce_one_commit_per_task_with_section_2_8_subject() {
        let agent = Arc::new(
            ScriptedTaskAgent::new()
                .script(Stage::Implement, writes_file("a.txt", "a"))
                .script(Stage::Implement, writes_file("b.txt", "b"))
                .script(Stage::Implement, writes_file("c.txt", "c")),
        );
        let (state, _app) = test_app_with_task_agent(agent).await;
        let fixture = GitFixture::new().await;
        let project_id = seed_project_with_workspace(&state, &fixture).await;
        let epic_id = seed_epic(&state, &project_id, "InProgress").await;

        let a = seed_task(&state, &epic_id, &project_id, "A").await;
        let b = seed_task(&state, &epic_id, &project_id, "B").await;
        let c = seed_task(&state, &epic_id, &project_id, "C").await;
        link(&state, &a, &b).await;
        link(&state, &b, &c).await;

        run_epic_pipeline(state.clone(), epic_id.clone()).await;

        let statuses = task_statuses(&state, &epic_id).await;
        assert_eq!(statuses["A"], "Done");
        assert_eq!(statuses["B"], "Done");
        assert_eq!(statuses["C"], "Done");

        let workspace_path = workspace::epic_workspace_path(&state.config.clone_root, &epic_id);
        let subjects = git_log_subjects(&workspace_path).await;
        assert_eq!(
            subjects,
            vec![
                "init".to_string(),
                format!("impl({}): A", spec::short_id(&a)),
                format!("impl({}): B", spec::short_id(&b)),
                format!("impl({}): C", spec::short_id(&c)),
            ],
        );

        cleanup_clone_root(&state, &project_id, &[&epic_id]);
    }

    /// A branching (diamond) DAG: A blocks B and C; B and C both block D.
    /// Every task's commit lands, in an order that is a valid topological
    /// order of the DAG — checked both as an exact sequence (this walk always
    /// picks the lowest-`position` ready task, and B/C were created in that
    /// order, so the sequence is fully deterministic) and generically (every
    /// blocker's commit index precedes every task it blocks).
    #[tokio::test]
    async fn branching_dag_commits_land_in_a_valid_topological_order() {
        let agent = Arc::new(
            ScriptedTaskAgent::new()
                .script(Stage::Implement, writes_file("a.txt", "a"))
                .script(Stage::Implement, writes_file("b.txt", "b"))
                .script(Stage::Implement, writes_file("c.txt", "c"))
                .script(Stage::Implement, writes_file("d.txt", "d")),
        );
        let (state, _app) = test_app_with_task_agent(agent).await;
        let fixture = GitFixture::new().await;
        let project_id = seed_project_with_workspace(&state, &fixture).await;
        let epic_id = seed_epic(&state, &project_id, "InProgress").await;

        let a = seed_task(&state, &epic_id, &project_id, "A").await;
        let b = seed_task(&state, &epic_id, &project_id, "B").await;
        let c = seed_task(&state, &epic_id, &project_id, "C").await;
        let d = seed_task(&state, &epic_id, &project_id, "D").await;
        link(&state, &a, &b).await;
        link(&state, &a, &c).await;
        link(&state, &b, &d).await;
        link(&state, &c, &d).await;

        run_epic_pipeline(state.clone(), epic_id.clone()).await;

        let workspace_path = workspace::epic_workspace_path(&state.config.clone_root, &epic_id);
        let subjects = git_log_subjects(&workspace_path).await;
        assert_eq!(
            subjects,
            vec![
                "init".to_string(),
                format!("impl({}): A", spec::short_id(&a)),
                format!("impl({}): B", spec::short_id(&b)),
                format!("impl({}): C", spec::short_id(&c)),
                format!("impl({}): D", spec::short_id(&d)),
            ],
        );

        // Generic topological check, independent of this walk's specific
        // tie-break: every blocker's commit index precedes its blocked task's.
        let index_of = |short: &str| {
            subjects
                .iter()
                .position(|s| s.contains(short))
                .unwrap_or_else(|| panic!("no commit found for short id {short}"))
        };
        for (blocker, blocked) in [(&a, &b), (&a, &c), (&b, &d), (&c, &d)] {
            assert!(
                index_of(spec::short_id(blocker)) < index_of(spec::short_id(blocked)),
                "{blocker} must commit before {blocked}: {subjects:?}"
            );
        }

        cleanup_clone_root(&state, &project_id, &[&epic_id]);
    }

    /// A `TaskAgent` wrapper that, on every `run()` call, synchronously
    /// records how many tasks are `InProgress` **at the exact moment** the
    /// stage starts (before delegating to `inner`) — the deterministic,
    /// no-sleep proof that the walk never runs two tasks concurrently (§2.3's
    /// "no sibling InProgress" invariant), preferred per MILESTONE_2 T-513's
    /// AC over a sleep-based probe. The probe query runs on its own
    /// single-thread tokio runtime inside a plain `std::thread` — `run()`
    /// itself is synchronous, so a fresh runtime gives the query somewhere
    /// to `.await` without needing the caller's own async context here.
    struct ConcurrencyProbeAgent {
        inner: ScriptedTaskAgent,
        conn: libsql::Connection,
        observed: Arc<std::sync::Mutex<Vec<i64>>>,
    }

    impl TaskAgent for ConcurrencyProbeAgent {
        fn run(&self, req: TaskRunRequest) -> Result<(RunHandle, Receiver<RunEvent>), HarnessError> {
            let conn = self.conn.clone();
            let count = std::thread::spawn(move || {
                let rt = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .unwrap();
                rt.block_on(async move {
                    let mut rows = conn
                        .query(
                            "SELECT COUNT(*) FROM task WHERE status = 'InProgress'",
                            (),
                        )
                        .await
                        .unwrap();
                    let row = rows.next().await.unwrap().unwrap();
                    row.get::<i64>(0).unwrap()
                })
            })
            .join()
            .unwrap();
            self.observed.lock().unwrap().push(count);
            self.inner.run(req)
        }
    }

    /// Two independent, simultaneously-ready tasks (A, B — no edge between
    /// them) plus a third (C) that depends on both: nothing here *forces*
    /// sequential ordering by dependency alone, so this is the strongest
    /// exercise of the "no sibling InProgress" invariant — only the walk's
    /// own serialization keeps A and B from ever running together.
    #[tokio::test]
    async fn implement_stage_never_observes_a_sibling_in_progress() {
        let db = Db::connect(":memory:").await.unwrap();
        db.run_migrations().await.unwrap();
        let probe_conn = db.conn().clone();
        let observed = Arc::new(std::sync::Mutex::new(Vec::new()));
        let agent = Arc::new(ConcurrencyProbeAgent {
            inner: ScriptedTaskAgent::new(),
            conn: probe_conn,
            observed: observed.clone(),
        });
        let state = AppState::with_all_agents(
            Config::for_test(TOKEN),
            db,
            Arc::new(SilentPlanningAgent),
            Arc::new(SilentBreakdownAgent),
            agent,
        );

        let fixture = GitFixture::new().await;
        let project_id = seed_project_with_workspace(&state, &fixture).await;
        let epic_id = seed_epic(&state, &project_id, "InProgress").await;
        let a = seed_task(&state, &epic_id, &project_id, "A").await;
        let b = seed_task(&state, &epic_id, &project_id, "B").await;
        let c = seed_task(&state, &epic_id, &project_id, "C").await;
        link(&state, &a, &c).await;
        link(&state, &b, &c).await;

        run_epic_pipeline(state.clone(), epic_id.clone()).await;

        let statuses = task_statuses(&state, &epic_id).await;
        assert_eq!(statuses["A"], "Done");
        assert_eq!(statuses["B"], "Done");
        assert_eq!(statuses["C"], "Done");

        let counts = observed.lock().unwrap().clone();
        assert_eq!(counts.len(), 3, "one probe reading per task's implement call");
        assert!(
            counts.iter().all(|&n| n == 1),
            "exactly one InProgress task at every implement call: {counts:?}"
        );

        cleanup_clone_root(&state, &project_id, &[&epic_id]);
    }

    /// An implement stage that makes no changes: no commit lands, no `commit`
    /// stage `agent_run` row is written, and the task is still left `Done` —
    /// the tracer-bullet AC ("committed as nothing and left Done"); the real
    /// already-complete verification is T-532's job.
    #[tokio::test]
    async fn no_diff_implement_stage_creates_no_commit_and_leaves_task_done() {
        // Bare ScriptedTaskAgent (test_app's default): its ScriptedRun::default
        // writes no files, so the implement stage produces no diff.
        let (state, _app) = test_app().await;
        let fixture = GitFixture::new().await;
        let project_id = seed_project_with_workspace(&state, &fixture).await;
        let epic_id = seed_epic(&state, &project_id, "InProgress").await;
        let task_id = seed_task(&state, &epic_id, &project_id, "A").await;

        run_epic_pipeline(state.clone(), epic_id.clone()).await;

        let statuses = task_statuses(&state, &epic_id).await;
        assert_eq!(statuses["A"], "Done", "a no-diff task is still left Done");

        let workspace_path = workspace::epic_workspace_path(&state.config.clone_root, &epic_id);
        let subjects = git_log_subjects(&workspace_path).await;
        assert_eq!(
            subjects,
            vec!["init".to_string()],
            "no commit must land when the implement stage made no changes"
        );

        let mut rows = state
            .db
            .conn()
            .query(
                "SELECT COUNT(*) FROM agent_run WHERE task_id = ?1 AND stage = 'commit'",
                params![task_id],
            )
            .await
            .unwrap();
        let count: i64 = rows.next().await.unwrap().unwrap().get(0).unwrap();
        assert_eq!(count, 0, "the commit stage never runs when there is nothing to commit");

        cleanup_clone_root(&state, &project_id, &[&epic_id]);
    }

    /// The `implement` and `commit` `agent_run` rows are both written, with
    /// the right stages/status, and the commit row's `log` carries the
    /// resulting SHA (§2.2: the Commit stage "records the SHA in log").
    #[tokio::test]
    async fn implement_and_commit_agent_run_rows_are_written_with_sha_in_commit_log() {
        let agent = Arc::new(
            ScriptedTaskAgent::new().script(Stage::Implement, writes_file("out.txt", "hello")),
        );
        let (state, _app) = test_app_with_task_agent(agent).await;
        let fixture = GitFixture::new().await;
        let project_id = seed_project_with_workspace(&state, &fixture).await;
        let epic_id = seed_epic(&state, &project_id, "InProgress").await;
        let task_id = seed_task(&state, &epic_id, &project_id, "A").await;

        run_epic_pipeline(state.clone(), epic_id.clone()).await;

        let workspace_path = workspace::epic_workspace_path(&state.config.clone_root, &epic_id);
        let head_sha = git::current_commit(&workspace_path).await.unwrap();

        let mut rows = state
            .db
            .conn()
            .query(
                "SELECT status FROM agent_run WHERE task_id = ?1 AND stage = 'implement'",
                params![task_id.clone()],
            )
            .await
            .unwrap();
        let row = rows.next().await.unwrap().expect("an implement agent_run row");
        assert_eq!(row.get::<String>(0).unwrap(), "ok");

        let mut rows = state
            .db
            .conn()
            .query(
                "SELECT status, log FROM agent_run WHERE task_id = ?1 AND stage = 'commit'",
                params![task_id],
            )
            .await
            .unwrap();
        let row = rows.next().await.unwrap().expect("a commit agent_run row");
        assert_eq!(row.get::<String>(0).unwrap(), "ok");
        let log: String = row.get(1).unwrap();
        assert!(log.contains(&head_sha), "commit row's log must carry the SHA: {log:?}");

        cleanup_clone_root(&state, &project_id, &[&epic_id]);
    }

    /// The D8 prompt actually carries the epic's background and the sibling
    /// manifest, not a bare spec: A's prompt lists B under "Owned by later
    /// tasks" (with the epic's description/product/technical context all
    /// present); once A is Done, B's prompt lists A under "Already built".
    #[tokio::test]
    async fn implement_prompt_includes_epic_context_and_sibling_manifest() {
        let agent = Arc::new(ScriptedTaskAgent::new());
        let recorded = agent.recorded();
        let (state, _app) = test_app_with_task_agent(agent.clone()).await;
        let fixture = GitFixture::new().await;
        let project_id = seed_project_with_workspace(&state, &fixture).await;
        let epic_id = seed_epic(&state, &project_id, "InProgress").await;

        state
            .db
            .conn()
            .execute(
                "UPDATE epic SET description = ?1, product_context = ?2, technical_context = ?3 \
                 WHERE id = ?4",
                params![
                    "Let users manage their profile.",
                    "Users abandon onboarding at the profile step.",
                    "REST endpoints backed by the existing user table.",
                    epic_id.clone(),
                ],
            )
            .await
            .unwrap();

        let a = seed_task(&state, &epic_id, &project_id, "Add the profile form").await;
        let b = seed_task(&state, &epic_id, &project_id, "Wire the profile API").await;
        link(&state, &a, &b).await; // A runs first, B second.

        run_epic_pipeline(state.clone(), epic_id.clone()).await;

        let runs = recorded.lock().unwrap();
        assert_eq!(runs.len(), 2, "one implement call per task");

        // A's prompt: epic context present; B listed as owned by a later task.
        assert!(runs[0].prompt.contains("Epic Context"));
        assert!(runs[0].prompt.contains("Let users manage their profile."));
        assert!(runs[0]
            .prompt
            .contains("Users abandon onboarding at the profile step."));
        assert!(runs[0]
            .prompt
            .contains("REST endpoints backed by the existing user table."));
        assert!(runs[0].prompt.contains("Owned by later tasks"));
        assert!(runs[0].prompt.contains("Wire the profile API"));

        // B's prompt: A now shows up under "Already built".
        assert!(runs[1].prompt.contains("Already built"));
        assert!(runs[1].prompt.contains("Add the profile form"));

        drop(runs);
        cleanup_clone_root(&state, &project_id, &[&epic_id]);
    }

    /// A `TaskAgent` wrapper that gates only the *Nth* call's `Exited` event
    /// (0-indexed) behind a [`Gate`], letting every other call through
    /// untouched — unlike `ScriptedTaskAgent::with_gate`, which gates *every*
    /// call uniformly. Needed so an earlier task can finish completely while
    /// a later one is deliberately held in flight (the "cancel mid-walk"
    /// test below).
    struct SelectiveGateAgent {
        inner: ScriptedTaskAgent,
        call_index: AtomicUsize,
        gate_at_index: usize,
        gate: Arc<Gate>,
    }

    impl TaskAgent for SelectiveGateAgent {
        fn run(&self, req: TaskRunRequest) -> Result<(RunHandle, Receiver<RunEvent>), HarnessError> {
            let idx = self.call_index.fetch_add(1, AtomicOrdering::SeqCst);
            let (handle, inner_rx) = self.inner.run(req)?;
            if idx != self.gate_at_index {
                return Ok((handle, inner_rx));
            }
            let gate = self.gate.clone();
            let (tx, rx) = std::sync::mpsc::channel();
            std::thread::spawn(move || {
                for event in inner_rx {
                    if matches!(event, RunEvent::Exited { .. }) {
                        gate.wait();
                    }
                    if tx.send(event).is_err() {
                        break;
                    }
                }
            });
            Ok((handle, rx))
        }
    }

    /// Cancelling mid-walk stops cleanly: while task B's implement stage is
    /// deliberately held in flight (gated before its terminal `Exited`), an
    /// external cancel (a lane move away from `InProgress`, simulated by
    /// writing the epic's status directly) lands. Releasing the gate lets
    /// B's implement stage *finish*, but the walk's mid-task recheck must
    /// catch the cancel before finalizing B — so B is never committed or
    /// marked Done, C (never even reached) stays Todo, and no further
    /// commits land beyond A's.
    #[tokio::test]
    async fn cancel_mid_walk_stops_cleanly_without_further_writes() {
        let gate = Arc::new(Gate::default());
        let agent = Arc::new(SelectiveGateAgent {
            inner: ScriptedTaskAgent::new()
                .script(Stage::Implement, writes_file("a.txt", "a"))
                .script(Stage::Implement, writes_file("b.txt", "b"))
                .script(Stage::Implement, writes_file("c.txt", "c")),
            call_index: AtomicUsize::new(0),
            gate_at_index: 1, // gate task B's implement call
            gate: gate.clone(),
        });

        let (state, _app) = test_app_with_task_agent(agent).await;
        let fixture = GitFixture::new().await;
        let project_id = seed_project_with_workspace(&state, &fixture).await;
        let epic_id = seed_epic(&state, &project_id, "InProgress").await;
        let a = seed_task(&state, &epic_id, &project_id, "A").await;
        let b = seed_task(&state, &epic_id, &project_id, "B").await;
        let c = seed_task(&state, &epic_id, &project_id, "C").await;
        link(&state, &a, &b).await;
        link(&state, &b, &c).await;

        let walk_state = state.clone();
        let walk_epic = epic_id.clone();
        let handle = tokio::spawn(async move {
            run_epic_pipeline(walk_state, walk_epic).await;
        });

        // Bounded, no-sleep-as-the-proof readiness poll: wait until task B is
        // InProgress — proves A already finished and B's implement call is
        // now gated in flight.
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        loop {
            let statuses = task_statuses(&state, &epic_id).await;
            if statuses.get("B").map(|s| s == "InProgress").unwrap_or(false) {
                break;
            }
            if tokio::time::Instant::now() >= deadline {
                panic!("task B never reached InProgress");
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }

        // Simulate an external cancel while B's implement stage is gated.
        state
            .db
            .conn()
            .execute(
                "UPDATE epic SET status = 'Cancelled' WHERE id = ?1",
                params![epic_id.clone()],
            )
            .await
            .unwrap();

        gate.release();
        handle.await.unwrap();

        let statuses = task_statuses(&state, &epic_id).await;
        assert_eq!(statuses["C"], "Todo", "the walk must never have reached C");
        assert_ne!(
            statuses["B"], "Done",
            "B must not be finalized once the cancel was observed mid-task"
        );

        let workspace_path = workspace::epic_workspace_path(&state.config.clone_root, &epic_id);
        let subjects = git_log_subjects(&workspace_path).await;
        assert_eq!(
            subjects,
            vec!["init".to_string(), format!("impl({}): A", spec::short_id(&a))],
            "only A's commit may have landed before the cancel stopped the walk"
        );

        cleanup_clone_root(&state, &project_id, &[&epic_id]);
    }
}
