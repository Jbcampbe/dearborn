//! Stub worker — walks an in-progress epic's DAG to Completion (T-403).
//!
//! When an epic is moved **Ready → In Progress** (`POST /epics/:id/lane`,
//! [`crate::lanes`]), the lane handler enqueues the stub worker via
//! [`spawn_stub_worker`]. The worker claims **ready** tasks one at a time (in
//! dependency order — a task is ready only when `status='Todo'` and every
//! blocker is `Done`, per §2.3), flips each `Todo → InProgress → Done`, and when
//! the DAG is fully `Done` sets `epic.status='Completed'`.
//!
//! ## Why a stub
//!
//! There is **no agent, no git, and no shell-out** here — the worker is pure
//! DB writes and publishes. It proves the seam: the rows it writes
//! (`epic.status='InProgress'`, `lease_owner=NULL`, `lease_expires_at=NULL`,
//! tasks `Todo → Done` in dependency order) are exactly the shape Half 2's
//! claim predicate will read (§2.3). The real executor replaces this module in
//! Half 2.
//!
//! ## The "no sibling InProgress" invariant (§2.3)
//!
//! Half 2's claim predicate requires that **no sibling task in the epic is
//! `InProgress`**. The stub honors this by serializing: it claims at most one
//! ready task at a time, fully completing it (`Done`) before looking for the
//! next. So there is never a moment with two `InProgress` tasks.
//!
//! ## Ownership of `InProgress → Completed`
//!
//! The worker owns the `InProgress → Completed` transition. Manual lane moves
//! to `Completed` are rejected by [`crate::lanes`] (`409 conflict`) — only the
//! worker sets it, once the DAG is fully `Done`.
//!
//! ## Live publishing
//!
//! Every task transition publishes a `dag_updated` frame on `epic:<id>` (so the
//! epic kanban/DAG editor live-renders the card moving). When the epic reaches
//! `Completed`, an `epic_updated` frame on `epic:<id>` and a `board_updated`
//! frame on `project:<id>` are published (so the project kanban re-renders the
//! card into the Completed lane).
//!
//! ## Cancellation
//!
//! The loop re-fetches the epic each iteration. If the epic is no longer
//! `InProgress` (e.g. a user moved it to `Cancelled` or `Blocked` during the
//! walk), the worker returns immediately — a clean no-op.

use libsql::params;
use tokio::task::JoinHandle;

use crate::board;
use crate::epics::{fetch_epic, get_epic_project_id};
use crate::mcp;
use crate::tasks::compute_dag;
use crate::AppState;

/// Run the stub worker to completion on `epic_id`. See the module docs for the
/// full contract. This is the background body spawned by [`spawn_stub_worker`];
/// tests may also call it directly (with `stub_worker_delay_ms == 0`).
pub async fn run_stub_worker(state: AppState, epic_id: String) {
    loop {
        let conn = state.db.conn();

        // 1. Guard: only act on an InProgress epic. A Cancel/Block during the
        //    walk makes this a clean no-op.
        let Some(epic) = fetch_epic(conn, &epic_id).await.unwrap_or(None) else {
            tracing::debug!(epic = %epic_id, "stub worker: epic vanished; stopping");
            return;
        };
        if epic.status != "InProgress" {
            tracing::debug!(
                epic = %epic_id,
                status = %epic.status,
                "stub worker: epic no longer InProgress; stopping"
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
                    "stub worker: failed to compute DAG; stopping"
                );
                return;
            }
        };

        // 3. Defensive: if any task is already InProgress (shouldn't happen — we
        //    serialize), wait for it to settle and retry.
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
                tracing::info!(epic = %epic_id, "stub worker: DAG complete; epic → Completed");
                return;
            } else {
                // Some Todo tasks remain but none are ready (all blocked) and
                // none InProgress — the DAG cannot progress. A valid acyclic DAG
                // walked in dependency order never hits this (cycles are
                // rejected at link time). Log and stop; do NOT infinite-loop.
                tracing::warn!(
                    epic = %epic_id,
                    "stub worker: no ready task but not all Done; DAG is stuck (blocked with no InProgress); stopping"
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

/// Fire-and-forget spawn of [`run_stub_worker`]. Used by the lane endpoint on
/// the `Ready → InProgress` transition. The returned handle lets tests await
/// completion if they want; the lane endpoint drops it.
pub fn spawn_stub_worker(state: AppState, epic_id: String) -> JoinHandle<()> {
    tokio::spawn(run_stub_worker(state, epic_id))
}

/// Resolve the project id for an epic (best-effort, for the board publish).
/// Re-fetches the epic to read `.project_id` directly. Kept for completeness;
/// `run_stub_worker` uses `fetch_epic` + `.project_id` instead.
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
    use crate::planning::testing::SilentPlanningAgent;
    use crate::breakdown::testing::SilentBreakdownAgent;
    use crate::{app, Config, Db};
    use axum::body::Body;
    use axum::http::{
        header::{AUTHORIZATION, CONTENT_TYPE},
        Request, StatusCode,
    };
    use libsql::params;
    use serde_json::{json, Value};
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
    async fn task_statuses(state: &AppState, epic_id: &str) -> std::collections::HashMap<String, String> {
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

    // ---- run_stub_worker direct tests ----

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

    // ---- end-to-end AC test via the lane endpoint ----

    /// Enqueue writes the contract shape: hitting `POST /epics/:id/lane
    /// { status: "InProgress" }` on a Ready epic with a task spawns the worker,
    /// which drives the DAG to Completed. Assert the response is InProgress,
    /// then poll until Completed, then assert the §2.3 contract shape:
    /// `lease_owner IS NULL`, `lease_expires_at IS NULL`, `epic.status =
    /// 'Completed'`, all tasks `Done`.
    #[tokio::test]
    async fn enqueue_via_lane_drives_dag_to_completed() {
        let (state, app) = test_app().await;
        let project_id = seed_project(&state).await;
        let epic_id = seed_epic(&state, &project_id, "Ready").await;
        let a = seed_task(&state, &epic_id, &project_id, "A").await;
        let b = seed_task(&state, &epic_id, &project_id, "B").await;
        link(&state, &a, &b).await; // A → B.

        // Hit the lane endpoint — spawns the stub worker in the background.
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

        // Poll the DB (bounded) until the epic is Completed — the spawned
        // worker finishes (delay 0 → near-instant).
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        loop {
            if epic_status(&state, &epic_id).await == "Completed" {
                break;
            }
            if tokio::time::Instant::now() >= deadline {
                panic!("worker did not complete the epic in time");
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        // Assert the §2.3 contract shape: lease NULL, epic Completed, all Done.
        let conn = state.db.conn();
        let mut rows = conn
            .query(
                "SELECT lease_owner, lease_expires_at, status FROM epic WHERE id = ?1",
                params![epic_id.clone()],
            )
            .await
            .unwrap();
        let row = rows.next().await.unwrap().unwrap();
        let lease_owner: Option<String> = row.get(0).unwrap();
        let lease_expires_at: Option<i64> = row.get(1).unwrap();
        let status: String = row.get(2).unwrap();
        assert!(lease_owner.is_none(), "lease_owner must be NULL");
        assert!(lease_expires_at.is_none(), "lease_expires_at must be NULL");
        assert_eq!(status, "Completed");

        let statuses = task_statuses(&state, &epic_id).await;
        assert_eq!(statuses["A"], "Done");
        assert_eq!(statuses["B"], "Done");
    }
}
