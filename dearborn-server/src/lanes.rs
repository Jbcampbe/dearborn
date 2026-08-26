//! Epic lane transitions (T-401).
//!
//! Epics move between lanes (`Planning | Ready | InProgress | InReview |
//! Completed | Cancelled | Blocked`) via `POST /epics/:id/lane`. Not every
//! transition is permitted: breakdown owns `Planning → Ready`, and the
//! executor worker pool ([`crate::worker`], T-510) owns `InProgress →
//! InReview` (finalize opens the PR). This module
//! encodes the permitted transition table and rejects everything else as
//! `409 conflict`, so the kanban's lane-move control can never put an epic in
//! an illegal state.
//!
//! On a successful transition the updated epic is published as `epic_updated`
//! on `epic:<id>` (so a subscribed planning/DAG view re-renders) and the board
//! is published as `board_updated` on `project:<id>` (so the kanban re-renders).
//!
//! ## Enqueue, don't spawn (D2, T-510)
//!
//! Before T-510 this handler spawned the stub worker directly on `Ready →
//! InProgress`, one `tokio::spawn` per enqueue. That model doesn't survive a
//! restart and doesn't bound concurrency. Since T-510 this handler only
//! **enqueues**: it sets `status='InProgress'`, explicitly clears the lease
//! columns (idle by construction on a fresh enqueue, but explicit is the
//! contract — see the inline comment below), and calls
//! `state.notify.notify_waiters()` to wake an idle worker loop immediately.
//! The long-lived worker pool ([`worker::spawn_pool`], started once in `main`)
//! is what claims and drives the epic; this handler never touches it again.
//!
//! ## `InProgress → Cancelled` is a kill, not just a status write (T-542, D12)
//!
//! Every other transition in this module is a plain `UPDATE ... status`.
//! `InProgress → Cancelled` does one thing more: after the `status` write
//! above has committed, it looks the epic's id up in
//! [`AppState::cancel_registry`] and, if an agent stage is currently running
//! for it, calls `RunControl::cancel()` — the actual kill MILESTONE_2 D12
//! promises ("Cancel is a kill"). The order matters: the DB write commits
//! *first*, so by the time the worker (in another task, possibly another
//! thread) observes the resulting `RunEvent::Exited { cancelled: true }` and
//! decides what to do next, `epic.status` already reads `Cancelled` — it
//! never has to guess whether the transition landed.
//!
//! **A cancel for an item with nothing in flight is a clean no-op**: the
//! registry lookup simply finds no entry (nothing to call `cancel()` on),
//! and [`crate::worker`]'s own stage-boundary DB checks
//! (`epic_still_in_progress`, sprinkled throughout `run_epic_pipeline_inner`
//! and its callees) are the backstop D12 requires for a cancel issued
//! *between* stages — the worker's next check of the epic's status simply
//! sees it is no longer `InProgress` and stops. This handler never needs to
//! know which case it's in; the lookup is unconditional and harmless either
//! way.
//!
//! **Never blocks on the kill completing.** `RunControl::cancel()` (the
//! `agent-harness` crate) is fire-and-forget: it signals the child process
//! (SIGTERM, with a delayed SIGKILL fallback on its own background thread if
//! the process hasn't exited within ~1.5s) and returns immediately — it does
//! not wait for the process to actually exit. This handler calls it
//! synchronously, still inside the request, and returns `200` right after;
//! the worker observes the eventual `Exited` event on its own time, tens to
//! low hundreds of milliseconds later in the common case, well within this
//! task's "terminates in seconds, not at the next stage boundary" AC.
//!
//! See [`AppState::cancel_registry`]'s own doc for the registry's shape and
//! [`crate::worker`]'s "T-542: cancellation as a kill" module-doc section for
//! what the worker does once it observes the cancelled outcome.

use axum::extract::{Path, State};
use axum::Json;
use libsql::params;
use serde::Deserialize;

use crate::board;
use crate::epics::{fetch_epic, Epic};
use crate::{AppError, AppResult, AppState};

/// The epic lane set (§2.2 stored values — no spaces: `InProgress`/`Completed`/`InReview`).
/// `InReview` is the "factory done, waiting on the human reviewer" lane the
/// post-PR review-poller owns (epic §4) — it sits between code-writing
/// (`InProgress`) and the human-driven exits `Completed`/`Cancelled`.
const VALID_LANES: &[&str] = &[
    "Planning",
    "Ready",
    "InProgress",
    "InReview",
    "Completed",
    "Cancelled",
    "Blocked",
];

/// Validate a lane string against the epic lane set, or `400 bad_request`.
fn validate_lane(lane: &str) -> AppResult<()> {
    if VALID_LANES.contains(&lane) {
        Ok(())
    } else {
        Err(AppError::BadRequest(format!(
            "`status` must be one of Planning|Ready|InProgress|InReview|Completed|Cancelled|Blocked, got `{lane}`"
        )))
    }
}

/// Whether `current → target` is a permitted lane transition. The table:
///
/// - `Planning → Cancelled`
/// - `Ready → InProgress, Cancelled`
/// - `InProgress → Cancelled, Blocked`
/// - `Blocked → Ready, Cancelled`
/// - `InReview → Cancelled` (manual "human abandon", also poller-owned on close)
/// - `Completed → (none)` — terminal
/// - `Cancelled → (none)` — terminal
///
/// `Planning → Ready` is owned by breakdown; `InProgress → InReview` is
/// owned by the executor worker pool ([`crate::worker`], T-510) — the
/// manual endpoint never moves an epic into `InReview` itself. Both are
/// rejected here. `InReview` is the "factory done, waiting on the human"
/// lane (epic §4): `InProgress → InReview` (worker finalize), `InReview →
/// InProgress` (poller when feedback spawns work) and `InReview → Completed`
/// (poller on merge) are all worker/poller-owned and so rejected from this
/// manual endpoint; only `InReview → Cancelled` is also allowed manually, as
/// the user-facing "abandon" action.
///
/// This table governs the lane-move endpoint only; the worker pool and the
/// post-PR review-poller perform their own direct status writes outside it.
fn transition_permitted(current: &str, target: &str) -> bool {
    match current {
        "Planning" => target == "Cancelled",
        "Ready" => target == "InProgress" || target == "Cancelled",
        "InProgress" => target == "Cancelled" || target == "Blocked",
        "InReview" => target == "Cancelled",
        "Blocked" => target == "Ready" || target == "Cancelled",
        "Completed" | "Cancelled" => false, // terminal
        _ => false,
    }
}

/// `POST /epics/:id/lane` body. `status` is the target lane.
#[derive(Deserialize)]
pub struct SetLaneBody {
    #[serde(default)]
    status: Option<String>,
}

/// `POST /epics/:id/lane` — move an epic between lanes. Validates the target
/// lane (`400` on unknown), `404` if the epic is missing, `409` if the
/// `current → target` transition is not permitted. On success: `UPDATE` the
/// epic's `status`, publish `epic_updated` on `epic:<id>` (payload = the updated
/// epic) and `board_updated` on `project:<id>`, and return `200` with the
/// updated epic.
pub async fn set_epic_lane(
    State(state): State<AppState>,
    Path(id): Path<String>,
    Json(req): Json<SetLaneBody>,
) -> AppResult<Json<Epic>> {
    let conn = state.db.conn();
    let epic = fetch_epic(conn, &id)
        .await?
        .ok_or_else(|| AppError::NotFound(format!("epic {id} not found")))?;

    let target = req
        .status
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .ok_or_else(|| AppError::BadRequest("`status` is required".to_string()))?;
    validate_lane(target)?;

    if !transition_permitted(&epic.status, target) {
        return Err(AppError::Conflict(format!(
            "lane transition `{}` → `{}` is not permitted",
            epic.status, target
        )));
    }

    let now = now_ms();
    conn.execute(
        "UPDATE epic SET status = ?1, updated_at = ?2 WHERE id = ?3",
        params![target, now, id.clone()],
    )
    .await?;

    // T-542/D12: `InProgress → Cancelled` is a kill, not just a status
    // write. The status write above has already committed, so by the time
    // any in-flight agent stage's worker observes the resulting
    // `RunEvent::Exited { cancelled: true }`, `epic.status` already reads
    // `Cancelled` — see this module's own doc for the full rationale. The
    // registry lookup is unconditional and cheap; finding nothing is the
    // correct, silent no-op for a cancel with no agent stage in flight
    // (caught instead by the worker's own stage-boundary DB checks, D12's
    // backstop). `RunControl::cancel()` is fire-and-forget (signals the
    // process and returns immediately — see the module doc), so this never
    // makes the response wait on the kill actually completing.
    if epic.status == "InProgress" && target == "Cancelled" {
        let in_flight = {
            let registry = state
                .cancel_registry
                .lock()
                .expect("cancel_registry mutex poisoned");
            match registry.get(&id) {
                Some(handle) => {
                    if let Err(err) = handle.cancel() {
                        tracing::warn!(
                            epic = %id,
                            error = %err,
                            "T-542: RunControl::cancel() failed (best-effort; the worker's own \
                             stage-boundary check remains the backstop)"
                        );
                    }
                    true
                }
                None => false,
            }
        };
        tracing::info!(epic = %id, in_flight, "T-542: epic cancelled");
    }

    let updated = fetch_epic(conn, &id)
        .await?
        .ok_or_else(|| AppError::Internal(format!("epic {id} vanished after lane update")))?;

    // Publish the updated epic on epic:<id> ...
    let payload = serde_json::to_value(&updated).unwrap_or(serde_json::Value::Null);
    state
        .hub
        .publish(&format!("epic:{id}"), "epic_updated", payload);
    // ... and the board on project:<id>.
    board::publish_board(&state, &updated.project_id).await;

    // T-510: the Ready → InProgress enqueue. Explicitly write the queue/lease
    // shape from §2.3 (lease columns are NULL from creation, but this makes
    // the enqueue explicit — "the enqueue sets epic.status='InProgress' and
    // leaves lease_owner NULL"), then wake an idle worker. This handler never
    // spawns anything itself (D2) — a long-lived worker loop in the pool
    // claims the epic (§2.4) and drives it to InReview; progress streams
    // over WS via dag_updated / epic_updated / board_updated. The HTTP
    // response is still the updated epic — the claim/run happens in the pool.
    if epic.status == "Ready" && target == "InProgress" {
        conn.execute(
            "UPDATE epic SET lease_owner = NULL, lease_expires_at = NULL WHERE id = ?1",
            params![id.clone()],
        )
        .await?;
        state.notify.notify_waiters();
    }

    Ok(Json(updated))
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
    use crate::planning::testing::SilentPlanningAgent;
    use crate::{app, Config, Db};
    use axum::body::Body;
    use axum::http::{
        header::{AUTHORIZATION, CONTENT_TYPE},
        Request, StatusCode,
    };
    use serde_json::{json, Value};
    use std::sync::Arc;
    use tower::ServiceExt;

    async fn test_app() -> (AppState, axum::Router) {
        let db = Db::connect(":memory:").await.unwrap();
        db.run_migrations().await.unwrap();
        let state = AppState::with_planner(Config::for_test(), db, Arc::new(SilentPlanningAgent));
        let app = app(state.clone());
        (state, app)
    }

    /// The bearer credential HTTP tests present, minted **once per process**
    /// from a seeded active admin (`crate::users::testing::seed_user` +
    /// `crate::sessions::testing::login_as`) — the replacement for the deleted
    /// static `TOKEN` constant. Access-token verification is stateless (one
    /// HMAC check against the fixed test master key, no database read), so a
    /// token minted here authenticates against every in-memory instance these
    /// tests boot.
    fn auth_bearer() -> &'static str {
        static BEARER: std::sync::OnceLock<String> = std::sync::OnceLock::new();
        BEARER.get_or_init(|| {
            // Seeding and login are async store calls, and `req` below is
            // synchronous. Mint on a dedicated OS thread: `Runtime::block_on`
            // panics if called from inside a test's own async context, but a
            // plain thread has none, so a throwaway current-thread runtime is
            // legal there.
            let (tx, rx) = std::sync::mpsc::channel();
            std::thread::spawn(move || {
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .expect("test runtime");
                let token = runtime.block_on(async {
                    let db = crate::Db::connect(":memory:").await.unwrap();
                    db.run_migrations().await.unwrap();
                    let state = crate::AppState::new(crate::Config::for_test(), db);
                    let user = crate::users::testing::seed_user(
                        &state,
                        "tester",
                        crate::users::Role::Admin,
                        true,
                    )
                    .await;
                    crate::sessions::testing::login_as(&state, &user).await
                });
                tx.send(token).expect("bearer receiver dropped");
            });
            rx.recv().expect("bearer minter panicked")
        })
    }

    fn req(method: &str, uri: &str, body: Option<Value>) -> Request<Body> {
        let builder = Request::builder()
            .method(method)
            .uri(uri)
            .header(AUTHORIZATION, format!("Bearer {}", auth_bearer()));
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

    /// Seed a single `Todo` task under `epic_id` (mirrors `worker.rs`'s test
    /// helper of the same shape, kept local so this module's tests stay
    /// self-contained).
    async fn seed_task(state: &AppState, epic_id: &str, project_id: &str) -> String {
        let conn = state.db.conn();
        let id = ulid::Ulid::new().to_string();
        let now = now_ms();
        conn.execute(
            "INSERT INTO task \
             (id, epic_id, project_id, title, status, position, created_at, updated_at) \
             VALUES (?1, ?2, ?3, 'A', 'Todo', 1, ?4, ?4)",
            params![id.clone(), epic_id, project_id, now],
        )
        .await
        .unwrap();
        id
    }

    #[tokio::test]
    async fn planning_to_cancelled_is_permitted_and_publishes() {
        let (state, app) = test_app().await;
        let project_id = seed_project(&state).await;
        let epic_id = seed_epic(&state, &project_id, "Planning").await;

        let mut epic_sub = state.hub.subscribe(&format!("epic:{epic_id}"));
        let mut proj_sub = state.hub.subscribe(&format!("project:{project_id}"));

        let response = app
            .clone()
            .oneshot(req(
                "POST",
                &format!("/epics/{epic_id}/lane"),
                Some(json!({ "status": "Cancelled" })),
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let epic = body_json(response).await;
        assert_eq!(epic["status"], "Cancelled");

        // epic_updated frame on epic:<id>.
        let frame = epic_sub.recv().await.unwrap();
        let v: Value = serde_json::from_str(&frame).unwrap();
        assert_eq!(v["type"], "epic_updated");
        assert_eq!(v["payload"]["status"], "Cancelled");

        // board_updated frame on project:<id>.
        let frame = proj_sub.recv().await.unwrap();
        let v: Value = serde_json::from_str(&frame).unwrap();
        assert_eq!(v["type"], "board_updated");
        assert_eq!(v["payload"]["epics"][0]["status"], "Cancelled");
    }

    #[tokio::test]
    async fn ready_to_in_progress_is_permitted() {
        let (state, app) = test_app().await;
        let project_id = seed_project(&state).await;
        let epic_id = seed_epic(&state, &project_id, "Ready").await;

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
    }

    /// T-510: `Ready → InProgress` clears the lease, wakes a waiter on
    /// `state.notify` (proving `notify_waiters()` is actually called — a
    /// waiter registered *before* the request resolves promptly instead of
    /// timing out), and — with no worker pool running in this test — spawns
    /// nothing itself: a seeded task never leaves `Todo`.
    #[tokio::test]
    async fn ready_to_in_progress_clears_lease_notifies_and_spawns_nothing() {
        let (state, app) = test_app().await;
        let project_id = seed_project(&state).await;
        let epic_id = seed_epic(&state, &project_id, "Ready").await;
        let task_id = seed_task(&state, &epic_id, &project_id).await;

        // Register the waiter BEFORE the request so we can prove the handler
        // itself calls `notify_waiters()` (a notify with no registered waiter
        // is not queued — this is the standard tokio::sync::Notify pattern).
        let notified = state.notify.notified();

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

        tokio::time::timeout(std::time::Duration::from_millis(500), notified)
            .await
            .expect("lane transition must call state.notify.notify_waiters()");

        // Lease columns explicitly cleared (contract shape).
        let conn = state.db.conn();
        let mut rows = conn
            .query(
                "SELECT lease_owner, lease_expires_at FROM epic WHERE id = ?1",
                libsql::params![epic_id.clone()],
            )
            .await
            .unwrap();
        let row = rows.next().await.unwrap().unwrap();
        let lease_owner: Option<String> = row.get(0).unwrap();
        let lease_expires_at: Option<i64> = row.get(1).unwrap();
        assert!(lease_owner.is_none());
        assert!(lease_expires_at.is_none());

        // No worker pool is running in this test, so nothing can move the
        // task off Todo — proving the handler itself spawns nothing. A few
        // `yield_now`s give any (incorrectly) spawned task a chance to run
        // without relying on wall-clock timing.
        for _ in 0..10 {
            tokio::task::yield_now().await;
        }
        let mut rows = conn
            .query(
                "SELECT status FROM task WHERE id = ?1",
                libsql::params![task_id],
            )
            .await
            .unwrap();
        let status: String = rows.next().await.unwrap().unwrap().get(0).unwrap();
        assert_eq!(
            status, "Todo",
            "lane handler must not spawn a worker itself"
        );
    }

    #[tokio::test]
    async fn blocked_to_ready_is_permitted() {
        let (state, app) = test_app().await;
        let project_id = seed_project(&state).await;
        let epic_id = seed_epic(&state, &project_id, "Blocked").await;

        let response = app
            .clone()
            .oneshot(req(
                "POST",
                &format!("/epics/{epic_id}/lane"),
                Some(json!({ "status": "Ready" })),
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(body_json(response).await["status"], "Ready");
    }

    /// Post-PR review loop (§4): `InReview` is the "factory done, waiting on
    /// the human reviewer" lane. The only **manual** route out of it is
    /// `InReview → Cancelled` (human abandon) — and it must publish like any
    /// other successful transition.
    #[tokio::test]
    async fn in_review_to_cancelled_is_permitted() {
        let (state, app) = test_app().await;
        let project_id = seed_project(&state).await;
        let epic_id = seed_epic(&state, &project_id, "InReview").await;

        let mut epic_sub = state.hub.subscribe(&format!("epic:{epic_id}"));
        let mut proj_sub = state.hub.subscribe(&format!("project:{project_id}"));

        let response = app
            .clone()
            .oneshot(req(
                "POST",
                &format!("/epics/{epic_id}/lane"),
                Some(json!({ "status": "Cancelled" })),
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(body_json(response).await["status"], "Cancelled");

        // epic_updated frame on epic:<id>.
        let frame = epic_sub.recv().await.unwrap();
        let v: Value = serde_json::from_str(&frame).unwrap();
        assert_eq!(v["type"], "epic_updated");
        assert_eq!(v["payload"]["status"], "Cancelled");

        // board_updated frame on project:<id>.
        let frame = proj_sub.recv().await.unwrap();
        let v: Value = serde_json::from_str(&frame).unwrap();
        assert_eq!(v["type"], "board_updated");
    }

    /// §4: `InReview → InProgress` is poller-owned (feedback spawned work) and
    /// must be rejected from the manual lane endpoint.
    #[tokio::test]
    async fn in_review_to_in_progress_is_rejected_409() {
        let (state, app) = test_app().await;
        let project_id = seed_project(&state).await;
        let epic_id = seed_epic(&state, &project_id, "InReview").await;

        let response = app
            .clone()
            .oneshot(req(
                "POST",
                &format!("/epics/{epic_id}/lane"),
                Some(json!({ "status": "InProgress" })),
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::CONFLICT);
    }

    /// §4: `InReview → Completed` is poller-owned (human merged the PR) — it
    /// must NOT be reachable from the manual lane endpoint.
    #[tokio::test]
    async fn in_review_to_completed_is_rejected_409() {
        let (state, app) = test_app().await;
        let project_id = seed_project(&state).await;
        let epic_id = seed_epic(&state, &project_id, "InReview").await;

        let response = app
            .clone()
            .oneshot(req(
                "POST",
                &format!("/epics/{epic_id}/lane"),
                Some(json!({ "status": "Completed" })),
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::CONFLICT);
    }

    /// §4: `InProgress → InReview` is worker-owned (finalize opens the PR) —
    /// it must NOT be reachable from the manual lane endpoint.
    #[tokio::test]
    async fn in_progress_to_in_review_is_rejected_409() {
        let (state, app) = test_app().await;
        let project_id = seed_project(&state).await;
        let epic_id = seed_epic(&state, &project_id, "InProgress").await;

        let response = app
            .clone()
            .oneshot(req(
                "POST",
                &format!("/epics/{epic_id}/lane"),
                Some(json!({ "status": "InReview" })),
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::CONFLICT);
    }

    #[tokio::test]
    async fn in_progress_to_blocked_is_permitted() {
        let (state, app) = test_app().await;
        let project_id = seed_project(&state).await;
        let epic_id = seed_epic(&state, &project_id, "InProgress").await;

        let response = app
            .clone()
            .oneshot(req(
                "POST",
                &format!("/epics/{epic_id}/lane"),
                Some(json!({ "status": "Blocked" })),
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(body_json(response).await["status"], "Blocked");
    }

    /// T-542/D12: `InProgress → Cancelled` looks the epic up in
    /// `state.cancel_registry` and calls `RunControl::cancel()` on whatever
    /// it finds. This test stands in for `run_agent_stage`'s real
    /// registration (that wiring is `task_agent.rs`'s own job, proven there;
    /// the full pipeline-level proof — a real gated `Stage::Implement`
    /// killed mid-flight through this exact endpoint — lives in
    /// `worker.rs`): it inserts a gated `ScriptedTaskAgent` run's handle
    /// directly under the epic's id, exactly as a `CancelGuard` would while
    /// that stage was in flight, then asserts the lane transition's own
    /// cancel step reaches that live handle.
    #[tokio::test]
    async fn in_progress_to_cancelled_kills_a_registered_in_flight_handle() {
        use crate::planning::testing::Gate;
        use crate::task_agent::testing::ScriptedTaskAgent;
        use crate::task_agent::{Stage, TaskAgent, TaskRunRequest};
        use harness::RunEvent;

        let (state, app) = test_app().await;
        let project_id = seed_project(&state).await;
        let epic_id = seed_epic(&state, &project_id, "InProgress").await;

        let gate = Arc::new(Gate::default());
        let agent = ScriptedTaskAgent::new().with_gate(gate.clone());
        let (handle, rx) = agent
            .run(TaskRunRequest {
                run_id: "run-lane-cancel".to_string(),
                stage: Stage::Implement,
                prompt: "go".to_string(),
                cwd: std::env::temp_dir(),
                harness: "claude".to_string(),
                model: None,
                prompt_hash: "test-prompt-hash".to_string(),
            })
            .unwrap();
        // Mirrors what `task_agent::CancelGuard::new` does while a real
        // stage is in flight — insert under the claimed item's id.
        state
            .cancel_registry
            .lock()
            .unwrap()
            .insert(epic_id.clone(), handle);

        let response = app
            .clone()
            .oneshot(req(
                "POST",
                &format!("/epics/{epic_id}/lane"),
                Some(json!({ "status": "Cancelled" })),
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(body_json(response).await["status"], "Cancelled");

        // The registered handle was actually cancelled — proven both via the
        // handle's own `was_cancelled()` (still in the registry; this
        // handler never removes an entry, only `CancelGuard`'s drop does)
        // and via the scripted run's terminal event once released.
        assert!(
            state
                .cancel_registry
                .lock()
                .unwrap()
                .get(&epic_id)
                .expect("still registered — only CancelGuard::drop removes it")
                .was_cancelled(),
            "the lane transition must call RunControl::cancel() on the registered handle"
        );

        gate.release();
        let exited = rx
            .into_iter()
            .find(|e| matches!(e, RunEvent::Exited { .. }));
        match exited {
            Some(RunEvent::Exited { cancelled, .. }) => {
                assert!(
                    cancelled,
                    "the scripted run's own Exited must report cancelled: true"
                )
            }
            other => panic!("expected an Exited event, got {other:?}"),
        }
    }

    /// D12's explicit backstop clause: a cancel with **nothing** registered
    /// (no agent stage in flight — e.g. between tasks, or during a non-agent
    /// stage like `test_gate`) is a clean no-op at this layer. The lane
    /// transition itself still succeeds; the worker's own stage-boundary DB
    /// check is what actually stops the walk (proven in `worker.rs`).
    #[tokio::test]
    async fn in_progress_to_cancelled_with_nothing_in_flight_is_a_clean_no_op() {
        let (state, app) = test_app().await;
        let project_id = seed_project(&state).await;
        let epic_id = seed_epic(&state, &project_id, "InProgress").await;

        assert!(state.cancel_registry.lock().unwrap().is_empty());

        let response = app
            .clone()
            .oneshot(req(
                "POST",
                &format!("/epics/{epic_id}/lane"),
                Some(json!({ "status": "Cancelled" })),
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(body_json(response).await["status"], "Cancelled");
        assert!(
            state.cancel_registry.lock().unwrap().is_empty(),
            "a cancel with nothing in flight must not touch the registry"
        );
    }

    #[tokio::test]
    async fn planning_to_ready_is_rejected_409() {
        let (state, app) = test_app().await;
        let project_id = seed_project(&state).await;
        let epic_id = seed_epic(&state, &project_id, "Planning").await;

        let response = app
            .clone()
            .oneshot(req(
                "POST",
                &format!("/epics/{epic_id}/lane"),
                Some(json!({ "status": "Ready" })),
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::CONFLICT);
        assert_eq!(body_json(response).await["error"]["code"], "conflict");
    }

    #[tokio::test]
    async fn in_progress_to_completed_is_rejected_409() {
        let (state, app) = test_app().await;
        let project_id = seed_project(&state).await;
        let epic_id = seed_epic(&state, &project_id, "InProgress").await;

        let response = app
            .clone()
            .oneshot(req(
                "POST",
                &format!("/epics/{epic_id}/lane"),
                Some(json!({ "status": "Completed" })),
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::CONFLICT);
    }

    #[tokio::test]
    async fn completed_is_terminal() {
        let (state, app) = test_app().await;
        let project_id = seed_project(&state).await;
        let epic_id = seed_epic(&state, &project_id, "Completed").await;

        let response = app
            .clone()
            .oneshot(req(
                "POST",
                &format!("/epics/{epic_id}/lane"),
                Some(json!({ "status": "Ready" })),
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::CONFLICT);
    }

    #[tokio::test]
    async fn cancelled_is_terminal() {
        let (state, app) = test_app().await;
        let project_id = seed_project(&state).await;
        let epic_id = seed_epic(&state, &project_id, "Cancelled").await;

        let response = app
            .clone()
            .oneshot(req(
                "POST",
                &format!("/epics/{epic_id}/lane"),
                Some(json!({ "status": "Ready" })),
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::CONFLICT);
    }

    #[tokio::test]
    async fn unknown_target_lane_is_400() {
        let (state, app) = test_app().await;
        let project_id = seed_project(&state).await;
        let epic_id = seed_epic(&state, &project_id, "Ready").await;

        let response = app
            .clone()
            .oneshot(req(
                "POST",
                &format!("/epics/{epic_id}/lane"),
                Some(json!({ "status": "Weird" })),
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        assert_eq!(body_json(response).await["error"]["code"], "bad_request");
    }

    #[tokio::test]
    async fn missing_status_is_400() {
        let (state, app) = test_app().await;
        let project_id = seed_project(&state).await;
        let epic_id = seed_epic(&state, &project_id, "Ready").await;

        let response = app
            .clone()
            .oneshot(req(
                "POST",
                &format!("/epics/{epic_id}/lane"),
                Some(json!({})),
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn unknown_epic_is_404() {
        let (_state, app) = test_app().await;
        let response = app
            .oneshot(req(
                "POST",
                "/epics/nope/lane",
                Some(json!({ "status": "Cancelled" })),
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }
}
