//! The review-poller subsystem (epic plan §5, §7): a **single sequential,
//! long-lived** task that periodically scans items sitting in `InReview` with
//! an open PR and acts on their PR's lifecycle state.
//!
//! ## Why a separate poller, not a worker-pool loop
//!
//! [`crate::worker`]'s pool is lease-based and claims only `InProgress`
//! items; an `InReview` item is factory-`done` waiting on the human and must
//! never be re-claimed by the code-writing pipeline (the epic owner of the
//! post-PR-review loop — see `worker`'s module doc). So the poller is a
//! distinct, single-sequential task (concurrency 1, **no lease** — it relies
//! on the documented single-server assumption). When a later task's feedback
//! flow needs to hand work back to the leased pool it will flip the item to
//! `InProgress` and notify [`crate::worker`]; this task's step never does.
//!
//! ## This task's scope: step 1 — merge / close detection only (§7)
//!
//! This module currently implements the *first* step of per-PR processing,
//! and nothing more. For each candidate PR it runs `get_pull` and reacts to
//! **merge/close** state:
//!
//! - `merged` → the epic moves to `Completed` (a standalone task to `Done`),
//!   the board is republished, and the workspace is **deleted now** (the
//!   exit from `InReview` — §7 makes merge/close the only place a retained
//!   workspace is torn down). No further work.
//! - `state == "closed" && !merged` → the item moves to `Cancelled` and the
//!   workspace is deleted.
//! - `state == "open"` → the item is **left untouched** by this step; it
//!   stays `InReview` so the feedback steps (later tasks) own it next.
//! - **Never merged**: no code path in this module (or anywhere in the
//!   crate) calls a merge API. [`crate::git_host::GitHost`] has no merge
//!   method, and this module only ever *reads* pull state via `get_pull` —
//!   which is exactly what AC #8 asserts.
//!
//! The feedback fetch → triage → act pipeline (§6) is owned by *later*
//! tasks; on an open PR this module deliberately does nothing so it stays
//! out of their territory.
//!
//! ## Per-item error boundary
//!
//! Each candidate is processed in its own error-isolating scope: a failure
//! to load a project, decrypt a PAT, or query GitHub for one PR is logged
//! and skipped, letting the remaining candidates proceed. One failing PR can
//! never stall the rest of the poll.
//!
//! ## Wiring
//!
//! [`spawn_review_poller`] is called from `crate::main` immediately after
//! [`crate::worker::spawn_pool`], with the poll interval read from
//! `crate::config::ExecutorConfig::review_poll_interval_secs`.

use std::time::Duration;

use libsql::params;

use crate::epics::fetch_epic;
use crate::git_host::{GetPullRequest, GitHostError, PullState};
use crate::projects::load_decrypted_pat;
use crate::tasks::fetch_task;
use crate::workspace::{epic_workspace_path, task_workspace_path};
use crate::AppState;

/// Start the single review-poller task. Concurrency 1 by construction: this
/// function spawns exactly one [`review_loop`], which performs one tick's
/// work at a time — there is never more than one candidate being processed at
/// once. Returns the `JoinHandle` so the caller can hold it (production drops
/// it — the poller runs for the life of the process, exactly like the worker
/// pool; tests hold it so the runtime keeps ticking).
pub fn spawn_review_poller(state: AppState) -> tokio::task::JoinHandle<()> {
    tokio::spawn(review_loop(state))
}

/// One long-lived loop. Idles on `sleep`, then runs one [`review_tick`] and
/// repeats. Never returns — the `JoinHandle` only resolves when the process /
/// its runtime is torn down.
async fn review_loop(state: AppState) {
    let interval_secs = state.config.executor.review_poll_interval_secs.max(1);
    let interval = Duration::from_secs(interval_secs);
    loop {
        tokio::time::sleep(interval).await;
        review_tick(&state).await;
    }
}

/// Query candidates and process every one. Sequential: candidates (epics
/// first, then standalone tasks) are handled one at a time in this call, and
/// no two `review_tick` calls ever overlap (the single `review_loop`
/// serializes them).
pub async fn review_tick(state: &AppState) {
    let epic_candidates = load_epic_candidates(state).await;
    for candidate in epic_candidates {
        // Per-item error boundary: process_*_candidate swallows its own
        // failures, so one bad PR can never stall the rest.
        process_epic_candidate(state, &candidate).await;
    }

    let task_candidates = load_task_candidates(state).await;
    for candidate in task_candidates {
        process_task_candidate(state, &candidate).await;
    }
}

/// A candidate row's identity — everything the poller needs to reach its PR
/// lifecycle state and act on it, read in a single query. Kept deliberately
/// lean: the poller re-fetches the full row (`fetch_epic`/`fetch_task`) only
/// **after** a state transition lands, purely to build a publish payload.
struct Candidate {
    id: String,
    project_id: String,
    pr_number: i64,
}

/// The candidate rows: `epic WHERE status='InReview' AND pr_number IS NOT
/// NULL` (plan §5). An `InReview` epic has a recorded PR by construction
/// (finalize sets `pr_number` when it lands there), but the `IS NOT NULL`
/// guard makes the query self-documenting and robust against any hand-rolled
/// `InReview` row.
async fn load_epic_candidates(state: &AppState) -> Vec<Candidate> {
    let mut out = Vec::new();
    let mut rows = match state
        .db
        .conn()
        .query(
            "SELECT id, project_id, pr_number FROM epic \
             WHERE status = 'InReview' AND pr_number IS NOT NULL",
            (),
        )
        .await
    {
        Ok(rows) => rows,
        Err(err) => {
            tracing::warn!(error = %err, "review poll: failed to query epic candidates");
            return out;
        }
    };
    while let Some(row) = rows.next().await.transpose() {
        let row = match row {
            Ok(row) => row,
            Err(err) => {
                tracing::warn!(error = %err, "review poll: failed to read an epic candidate row");
                continue;
            }
        };
        let id: String = match row.get(0) {
            Ok(id) => id,
            Err(_) => continue,
        };
        let project_id: String = match row.get(1) {
            Ok(p) => p,
            Err(_) => continue,
        };
        let pr_number: i64 = match row.get(2) {
            Ok(n) => n,
            Err(_) => continue,
        };
        out.push(Candidate {
            id,
            project_id,
            pr_number,
        });
    }
    out
}

/// Standalone-task candidate rows (`task WHERE status='InReview' AND epic_id
/// IS NULL AND pr_number IS NOT NULL`) — the task-table mirror of
/// [`load_epic_candidates`]. Epic-owned tasks never carry the terminal
/// `InReview` (the epic row does); only standalone tasks, whose PR lifecycle
/// lives on the task row itself, are polled here.
async fn load_task_candidates(state: &AppState) -> Vec<Candidate> {
    let mut out = Vec::new();
    let mut rows = match state
        .db
        .conn()
        .query(
            "SELECT id, project_id, pr_number FROM task \
             WHERE status = 'InReview' AND epic_id IS NULL AND pr_number IS NOT NULL",
            (),
        )
        .await
    {
        Ok(rows) => rows,
        Err(err) => {
            tracing::warn!(error = %err, "review poll: failed to query task candidates");
            return out;
        }
    };
    while let Some(row) = rows.next().await.transpose() {
        let row = match row {
            Ok(row) => row,
            Err(err) => {
                tracing::warn!(error = %err, "review poll: failed to read a task candidate row");
                continue;
            }
        };
        let id: String = match row.get(0) {
            Ok(id) => id,
            Err(_) => continue,
        };
        let project_id: String = match row.get(1) {
            Ok(p) => p,
            Err(_) => continue,
        };
        let pr_number: i64 = match row.get(2) {
            Ok(n) => n,
            Err(_) => continue,
        };
        out.push(Candidate {
            id,
            project_id,
            pr_number,
        });
    }
    out
}

/// Process one epic [`Candidate`]'s merge/close state: `get_pull` the PR and
/// act on [`PullState`] (§7). Fully error-isolated — an outward failure is
/// logged and swallowed so the poll's remaining candidates still run. Never
/// calls a merge API.
async fn process_epic_candidate(state: &AppState, candidate: &Candidate) {
    let pull = match get_pull_state(state, candidate).await {
        Ok(pull) => pull,
        Err(err) => {
            tracing::warn!(
                epic = %candidate.id,
                error = %err,
                "review poll: could not read PR state; leaving epic untouched"
            );
            return;
        }
    };
    apply_epic_pull_state(state, candidate, &pull).await;
}

/// The standalone-task mirror of [`process_epic_candidate`].
async fn process_task_candidate(state: &AppState, candidate: &Candidate) {
    let pull = match get_pull_state(state, candidate).await {
        Ok(pull) => pull,
        Err(err) => {
            tracing::warn!(
                task = %candidate.id,
                error = %err,
                "review poll: could not read PR state; leaving task untouched"
            );
            return;
        }
    };
    apply_task_pull_state(state, candidate, &pull).await;
}

/// The shared `get_pull` read: load the project's `repo_url` + PAT and query
/// the hosted PR's lifecycle state. A failure to load project/PAT or to reach
/// GitHub is returned as-is (the caller isolates it). No merge path exists.
async fn get_pull_state(
    state: &AppState,
    candidate: &Candidate,
) -> Result<PullState, GitHostError> {
    let repo_url = load_repo_url(state, &candidate.project_id).await?;
    let pat = load_decrypted_pat(state, &candidate.project_id)
        .await
        .map_err(|err| GitHostError::new(format!("failed to load project PAT: {err}")))?;
    state
        .git_host
        .get_pull(GetPullRequest {
            repo_url: &repo_url,
            pat: pat.as_deref(),
            number: candidate.pr_number,
        })
        .await
}

/// Apply an epic's [`PullState`] (§7):
/// - `merged` → `Completed` + board publish + delete workspace;
/// - `state == "closed" && !merged` → `Cancelled` + delete workspace;
/// - `state == "open"` → untouched (feedback steps, a later task, own it).
async fn apply_epic_pull_state(state: &AppState, candidate: &Candidate, pull: &PullState) {
    let status = match (pull.merged, pull.state.as_str()) {
        (true, _) => "Completed",
        (false, "closed") => "Cancelled",
        (false, "open") => {
            tracing::debug!(
                epic = %candidate.id,
                "review poll: PR still open; leaving epic untouched for feedback"
            );
            return;
        }
        (false, other) => {
            tracing::warn!(
                epic = %candidate.id,
                pr_state = other,
                "review poll: unrecognized PR state; leaving epic untouched"
            );
            return;
        }
    };

    let conn = state.db.conn();
    let now = now_ms();
    let affected = conn
        .execute(
            "UPDATE epic SET status = ?1, updated_at = ?2 WHERE id = ?3 AND status = 'InReview'",
            params![status, now, candidate.id.clone()],
        )
        .await;

    match affected {
        Ok(n) if n > 0 => {
            tracing::info!(
                epic = %candidate.id,
                pr = %candidate.pr_number,
                to = status,
                "review poll: epic moved out of InReview"
            );
            publish_updated_epic(state, &candidate.id).await;
            let workspace_path = epic_workspace_path(&state.config.clone_root, &candidate.id);
            if let Err(err) = crate::workspace::delete_workspace(&workspace_path).await {
                tracing::warn!(
                    epic = %candidate.id,
                    error = %err,
                    "review poll: failed to delete epic workspace"
                );
            }
        }
        Ok(_) => {
            tracing::debug!(
                epic = %candidate.id,
                "review poll: epic no longer InReview (something else moved it); leaving as-is"
            );
        }
        Err(err) => {
            tracing::error!(
                epic = %candidate.id,
                error = %err,
                "review poll: failed to update epic status"
            );
        }
    }
}

/// The standalone-task mirror of [`apply_epic_pull_state`]. `merged` →
/// `Done`; `closed && !merged` → `Cancelled`; `open` → untouched.
async fn apply_task_pull_state(state: &AppState, candidate: &Candidate, pull: &PullState) {
    let status = match (pull.merged, pull.state.as_str()) {
        (true, _) => "Done",
        (false, "closed") => "Cancelled",
        (false, "open") => {
            tracing::debug!(
                task = %candidate.id,
                "review poll: PR still open; leaving task untouched for feedback"
            );
            return;
        }
        (false, other) => {
            tracing::warn!(
                task = %candidate.id,
                pr_state = other,
                "review poll: unrecognized PR state; leaving task untouched"
            );
            return;
        }
    };

    let conn = state.db.conn();
    let now = now_ms();
    let affected = conn
        .execute(
            "UPDATE task SET status = ?1, updated_at = ?2 WHERE id = ?3 AND status = 'InReview'",
            params![status, now, candidate.id.clone()],
        )
        .await;

    match affected {
        Ok(n) if n > 0 => {
            tracing::info!(
                task = %candidate.id,
                pr = %candidate.pr_number,
                to = status,
                "review poll: standalone task moved out of InReview"
            );
            publish_updated_task(state, &candidate.id).await;
            let workspace_path = task_workspace_path(&state.config.clone_root, &candidate.id);
            if let Err(err) = crate::workspace::delete_workspace(&workspace_path).await {
                tracing::warn!(
                    task = %candidate.id,
                    error = %err,
                    "review poll: failed to delete task workspace"
                );
            }
        }
        Ok(_) => {
            tracing::debug!(
                task = %candidate.id,
                "review poll: task no longer InReview (something else moved it); leaving as-is"
            );
        }
        Err(err) => {
            tracing::error!(
                task = %candidate.id,
                error = %err,
                "review poll: failed to update task status"
            );
        }
    }
}

/// Best-effort re-fetch + publish of an epic after a merge/close transition,
/// mirroring `worker::finalize_epic`'s publish shape (`epic_updated` on
/// `epic:<id>` + the board on `project:<id>`).
async fn publish_updated_epic(state: &AppState, epic_id: &str) {
    let Ok(Some(updated)) = fetch_epic(state.db.conn(), epic_id).await else {
        return;
    };
    let payload = serde_json::to_value(&updated).unwrap_or(serde_json::Value::Null);
    state
        .hub
        .publish(&format!("epic:{epic_id}"), "epic_updated", payload);
    crate::board::publish_board(state, &updated.project_id).await;
}

/// Best-effort publish of a standalone task after a merge/close transition,
/// mirroring `worker::finalize_task`'s publish shape (a board publish on
/// `project:<id>`).
async fn publish_updated_task(state: &AppState, task_id: &str) {
    let Ok(Some(updated)) = fetch_task(state.db.conn(), task_id).await else {
        return;
    };
    crate::board::publish_board(state, &updated.project_id).await;
}

/// Load a candidate project's `repo_url` for the `get_pull` read.
async fn load_repo_url(state: &AppState, project_id: &str) -> Result<String, GitHostError> {
    let mut rows = state
        .db
        .conn()
        .query(
            "SELECT repo_url FROM project WHERE id = ?1",
            params![project_id],
        )
        .await
        .map_err(|err| GitHostError::new(format!("failed to load project {project_id}: {err}")))?;
    match rows.next().await {
        Ok(Some(row)) => row
            .get(0)
            .map_err(|err| GitHostError::new(format!("failed to read project repo_url: {err}"))),
        Ok(None) => Err(GitHostError::new(format!("project {project_id} not found"))),
        Err(err) => Err(GitHostError::new(format!(
            "failed to read project repo_url: {err}"
        ))),
    }
}

/// Current unix time in milliseconds (matches the `*_at` columns).
fn now_ms() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|dir| dir.as_millis() as i64)
        .unwrap_or(0)
}
#[cfg(test)]
mod tests {
    use super::*;
    use std::path::{Path, PathBuf};
    use std::sync::Arc;

    use crate::breakdown::testing::SilentBreakdownAgent;
    use crate::config::Config;
    use crate::git_host::testing::FakeHost;
    use crate::git_host::{self, GitHost};
    use crate::planning::testing::SilentPlanningAgent;
    use crate::task_agent::testing::ScriptedTaskAgent;
    use crate::Db;

    struct TestEnv {
        state: AppState,
        fake: Arc<FakeHost>,
        clone_root: PathBuf,
    }

    /// Build an [`AppState`] with an injected [`GitHost`] and a temp
    /// `clone_root` (so a test can seed fake workspace directories the poller
    /// deletes on merge/close).
    async fn make_env_with_host(host: Arc<dyn GitHost>, clone_root: PathBuf) -> AppState {
        let db = Db::connect(":memory:").await.unwrap();
        db.run_migrations().await.unwrap();
        std::fs::create_dir_all(&clone_root).unwrap();

        let mut config = Config::for_test();
        config.clone_root = clone_root.to_string_lossy().into_owned();

        AppState::with_all_agents_and_host(
            config,
            db,
            Arc::new(SilentPlanningAgent),
            Arc::new(SilentBreakdownAgent),
            Arc::new(ScriptedTaskAgent::new()),
            host,
        )
    }

    async fn make_env_with(fake: FakeHost) -> TestEnv {
        let clone_root =
            std::env::temp_dir().join(format!("dearborn-review-{}", ulid::Ulid::new()));
        let fake = Arc::new(fake);
        let state = make_env_with_host(fake.clone(), clone_root.clone()).await;
        TestEnv {
            state,
            fake,
            clone_root,
        }
    }

    async fn seed_project(state: &AppState, repo_url: &str) -> String {
        let id = ulid::Ulid::new().to_string();
        let now = now_ms();
        state
            .db
            .conn()
            .execute(
                "INSERT INTO project (id, name, repo_url, clone_status, created_at, updated_at) \
                 VALUES (?1, 'P', ?2, 'ready', ?3, ?3)",
                params![id.clone(), repo_url, now],
            )
            .await
            .unwrap();
        id
    }

    async fn seed_in_review_epic(
        state: &AppState,
        project_id: &str,
        pr_number: i64,
        title: &str,
    ) -> String {
        let id = ulid::Ulid::new().to_string();
        let now = now_ms();
        state
            .db
            .conn()
            .execute(
                "INSERT INTO epic (id, project_id, title, status, pr_number, pr_url, created_at, updated_at) \
                 VALUES (?1, ?2, ?3, 'InReview', ?4, ?5, ?6, ?6)",
                params![
                    id.clone(),
                    project_id,
                    title,
                    pr_number,
                    format!("https://github.com/o/r/pull/{pr_number}"),
                    now
                ],
            )
            .await
            .unwrap();
        id
    }

    async fn seed_in_review_task(
        state: &AppState,
        project_id: &str,
        pr_number: i64,
        title: &str,
    ) -> String {
        let id = ulid::Ulid::new().to_string();
        let now = now_ms();
        state
            .db
            .conn()
            .execute(
                "INSERT INTO task (id, epic_id, project_id, title, status, pr_number, pr_url, created_at, updated_at) \
                 VALUES (?1, NULL, ?2, ?3, 'InReview', ?4, ?5, ?6, ?6)",
                params![
                    id.clone(),
                    project_id,
                    title,
                    pr_number,
                    format!("https://github.com/o/r/pull/{pr_number}"),
                    now
                ],
            )
            .await
            .unwrap();
        id
    }

    async fn epic_status(state: &AppState, epic_id: &str) -> String {
        let mut rows = state
            .db
            .conn()
            .query("SELECT status FROM epic WHERE id = ?1", params![epic_id])
            .await
            .unwrap();
        rows.next().await.unwrap().unwrap().get(0).unwrap()
    }

    async fn task_status(state: &AppState, task_id: &str) -> String {
        let mut rows = state
            .db
            .conn()
            .query("SELECT status FROM task WHERE id = ?1", params![task_id])
            .await
            .unwrap();
        rows.next().await.unwrap().unwrap().get(0).unwrap()
    }

    /// Seed a fake retained workspace at `<clone_root>/<kind>/<id>`.
    fn seed_workspace(root: &Path, kind: &str, id: &str) -> PathBuf {
        let p = root.join(kind).join(id);
        std::fs::create_dir_all(&p).unwrap();
        std::fs::write(p.join("retained.txt"), "x").unwrap();
        p
    }

    fn merged() -> git_host::PullState {
        PullState {
            merged: true,
            state: "closed".to_string(),
            head_sha: "deadbeef".to_string(),
        }
    }

    fn closed_unmerged() -> git_host::PullState {
        PullState {
            merged: false,
            state: "closed".to_string(),
            head_sha: "deadbeef".to_string(),
        }
    }

    fn open() -> git_host::PullState {
        PullState {
            merged: false,
            state: "open".to_string(),
            head_sha: "deadbeef".to_string(),
        }
    }

    // ---- merged → Completed, publishes, deletes workspace; never merges ----

    #[tokio::test]
    async fn merged_epic_becomes_completed_publishes_and_deletes_workspace() {
        let env = make_env_with(FakeHost::new().with_pull_state(merged())).await;
        let project = seed_project(&env.state, "https://github.com/o/r").await;
        let epic = seed_in_review_epic(&env.state, &project, 7, "I merged PR").await;
        let ws = seed_workspace(&env.clone_root, "epics", &epic);

        // Subscribe before the tick so we can assert an `epic_updated` arrives.
        let mut rx = env.state.hub.subscribe(&format!("epic:{epic}"));

        review_tick(&env.state).await;

        assert_eq!(epic_status(&env.state, &epic).await, "Completed");
        assert!(!ws.exists(), "a merged PR deletes the epic's workspace");
        assert_eq!(env.fake.get_pull_calls(), vec![7]);

        // The poller only ever *reads* the PR (`get_pull`); it never asks Git
        // host to mutate anything — and never to merge (AC #8).
        assert!(env.fake.open_pr_calls().is_empty());
        assert!(env.fake.post_issue_comment_calls().is_empty());
        assert!(env.fake.reply_review_comment_calls().is_empty());
        assert!(env.fake.resolve_thread_calls().is_empty());

        let envelope: serde_json::Value = serde_json::from_str(&rx.try_recv().unwrap()).unwrap();
        assert_eq!(envelope["type"], "epic_updated");
        assert_eq!(envelope["payload"]["status"], "Completed");
    }

    #[tokio::test]
    async fn merged_standalone_task_becomes_done_and_deletes_workspace() {
        let env = make_env_with(FakeHost::new().with_pull_state(merged())).await;
        let project = seed_project(&env.state, "https://github.com/o/r").await;
        let task = seed_in_review_task(&env.state, &project, 9, "task title").await;
        let ws = seed_workspace(&env.clone_root, "tasks", &task);

        let mut rx = env.state.hub.subscribe(&format!("project:{project}"));

        review_tick(&env.state).await;

        assert_eq!(task_status(&env.state, &task).await, "Done");
        assert!(!ws.exists(), "a merged task's workspace is deleted");
        assert!(env.fake.open_pr_calls().is_empty());
        assert!(env.fake.post_issue_comment_calls().is_empty());
        assert!(env.fake.reply_review_comment_calls().is_empty());
        assert!(env.fake.resolve_thread_calls().is_empty());

        let envelope: serde_json::Value = serde_json::from_str(&rx.try_recv().unwrap()).unwrap();
        assert_eq!(envelope["type"], "board_updated");
        let tasks = envelope["payload"]["tasks"].as_array().unwrap();
        assert_eq!(tasks[0]["status"], "Done");
    }

    // ---- closed-unmerged → Cancelled + workspace deleted -------------------

    #[tokio::test]
    async fn closed_unmerged_epic_becomes_cancelled_and_deletes_workspace() {
        let env = make_env_with(FakeHost::new().with_pull_state(closed_unmerged())).await;
        let project = seed_project(&env.state, "https://github.com/o/r").await;
        let epic = seed_in_review_epic(&env.state, &project, 11, "closed unmerged").await;
        let ws = seed_workspace(&env.clone_root, "epics", &epic);

        review_tick(&env.state).await;

        assert_eq!(epic_status(&env.state, &epic).await, "Cancelled");
        assert!(
            !ws.exists(),
            "a closed-unmerged epic's workspace is deleted"
        );
        assert!(env.fake.open_pr_calls().is_empty());
        assert!(env.fake.resolve_thread_calls().is_empty());
    }

    #[tokio::test]
    async fn closed_unmerged_task_becomes_cancelled_and_deletes_workspace() {
        let env = make_env_with(FakeHost::new().with_pull_state(closed_unmerged())).await;
        let project = seed_project(&env.state, "https://github.com/o/r").await;
        let task = seed_in_review_task(&env.state, &project, 13, "closed task").await;
        let ws = seed_workspace(&env.clone_root, "tasks", &task);

        review_tick(&env.state).await;

        assert_eq!(task_status(&env.state, &task).await, "Cancelled");
        assert!(
            !ws.exists(),
            "a closed-unmerged task's workspace is deleted"
        );
        assert!(env.fake.post_issue_comment_calls().is_empty());
        assert!(env.fake.reply_review_comment_calls().is_empty());
    }

    // ---- open PR → left untouched by this step ----------------------------

    #[tokio::test]
    async fn open_pr_epic_and_task_are_left_untouched() {
        let env = make_env_with(FakeHost::new().with_pull_state(open())).await;
        let project = seed_project(&env.state, "https://github.com/o/r").await;
        let epic = seed_in_review_epic(&env.state, &project, 15, "open epic").await;
        let epic_ws = seed_workspace(&env.clone_root, "epics", &epic);
        let task = seed_in_review_task(&env.state, &project, 16, "open task").await;
        let task_ws = seed_workspace(&env.clone_root, "tasks", &task);

        review_tick(&env.state).await;

        assert_eq!(epic_status(&env.state, &epic).await, "InReview");
        assert_eq!(task_status(&env.state, &task).await, "InReview");
        assert!(epic_ws.exists(), "open PR → workspace retained");
        assert!(task_ws.exists(), "open PR → workspace retained");
        // No mutations at all — feedback/fetch is a later task.
        assert!(env.fake.open_pr_calls().is_empty());
        assert!(env.fake.post_issue_comment_calls().is_empty());
        assert!(env.fake.reply_review_comment_calls().is_empty());
        assert!(env.fake.resolve_thread_calls().is_empty());
    }

    // ---- per-item error isolation ------------------------------------------

    /// A [`GitHost`] decorator over a [`FakeHost`] that makes `get_pull`
    /// fail for exactly one PR number, delegating everything else (including
    /// every other `get_pull`) to the inner fake — the minimal surface needed
    /// to prove one failing PR doesn't stall its neighbours.
    struct FailOnPull {
        inner: FakeHost,
        fail_pr: i64,
    }

    impl GitHost for FailOnPull {
        fn get_pull<'a>(
            &'a self,
            req: git_host::GetPullRequest<'a>,
        ) -> git_host::BoxFuture<'a, Result<PullState, git_host::GitHostError>> {
            Box::pin(async move {
                if req.number == self.fail_pr {
                    return Err(git_host::GitHostError::new("simulated get_pull failure"));
                }
                self.inner.get_pull(req).await
            })
        }

        fn push<'a>(
            &'a self,
            req: git_host::PushRequest<'a>,
        ) -> git_host::BoxFuture<'a, Result<(), git_host::GitHostError>> {
            self.inner.push(req)
        }

        fn open_pr<'a>(
            &'a self,
            req: git_host::OpenPrRequest<'a>,
        ) -> git_host::BoxFuture<'a, Result<git_host::OpenedPr, git_host::GitHostError>> {
            self.inner.open_pr(req)
        }

        fn check_auth<'a>(
            &'a self,
            req: git_host::CheckAuthRequest<'a>,
        ) -> git_host::BoxFuture<'a, Result<(), git_host::GitHostError>> {
            self.inner.check_auth(req)
        }

        fn list_reviews<'a>(
            &'a self,
            req: git_host::ListReviewsRequest<'a>,
        ) -> git_host::BoxFuture<'a, Result<Vec<git_host::Review>, git_host::GitHostError>>
        {
            self.inner.list_reviews(req)
        }

        fn list_review_comments<'a>(
            &'a self,
            req: git_host::ListReviewCommentsRequest<'a>,
        ) -> git_host::BoxFuture<'a, Result<Vec<git_host::InlineComment>, git_host::GitHostError>>
        {
            self.inner.list_review_comments(req)
        }

        fn list_issue_comments<'a>(
            &'a self,
            req: git_host::ListIssueCommentsRequest<'a>,
        ) -> git_host::BoxFuture<'a, Result<Vec<git_host::IssueComment>, git_host::GitHostError>>
        {
            self.inner.list_issue_comments(req)
        }

        fn post_issue_comment<'a>(
            &'a self,
            req: git_host::PostIssueCommentRequest<'a>,
        ) -> git_host::BoxFuture<'a, Result<i64, git_host::GitHostError>> {
            self.inner.post_issue_comment(req)
        }

        fn reply_review_comment<'a>(
            &'a self,
            req: git_host::ReplyReviewCommentRequest<'a>,
        ) -> git_host::BoxFuture<'a, Result<i64, git_host::GitHostError>> {
            self.inner.reply_review_comment(req)
        }

        fn list_review_threads<'a>(
            &'a self,
            req: git_host::ListReviewThreadsRequest<'a>,
        ) -> git_host::BoxFuture<'a, Result<Vec<git_host::Thread>, git_host::GitHostError>>
        {
            self.inner.list_review_threads(req)
        }

        fn resolve_thread<'a>(
            &'a self,
            req: git_host::ResolveThreadRequest<'a>,
        ) -> git_host::BoxFuture<'a, Result<(), git_host::GitHostError>> {
            self.inner.resolve_thread(req)
        }
    }

    #[tokio::test]
    async fn a_failing_pr_does_not_stall_the_rest() {
        // PR #7's `get_pull` fails; #8's succeeds and merges. The failing item
        // must be left untouched while the healthy neighbour is still processed.
        let host = Arc::new(FailOnPull {
            inner: FakeHost::new().with_pull_state(merged()),
            fail_pr: 7,
        });
        let clone_root =
            std::env::temp_dir().join(format!("dearborn-review-iso-{}", ulid::Ulid::new()));
        let state = make_env_with_host(host, clone_root.clone()).await;

        let project = seed_project(&state, "https://github.com/o/r").await;
        let epic_failing = seed_in_review_epic(&state, &project, 7, "failing epic").await;
        let epic_ok = seed_in_review_epic(&state, &project, 8, "ok epic").await;

        review_tick(&state).await;

        assert_eq!(
            epic_status(&state, &epic_failing).await,
            "InReview",
            "the failing PR's item must be left untouched"
        );
        assert_eq!(
            epic_status(&state, &epic_ok).await,
            "Completed",
            "the other PR must still be processed despite the failing neighbour"
        );
    }
}
