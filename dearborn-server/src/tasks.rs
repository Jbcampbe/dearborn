//! Task store helpers and the task-DAG primitives (T-301).
//!
//! A **task** is a vertical slice — the executor's unit of work (MILESTONE_1
//! §2.2). Breakdown (T-301) creates a graph of them under an epic, wired by
//! `task_dependency` edges (`blocker` blocks `blocked`, matching to-tasks'
//! `blocks:`). This module is the shared, framework-free store layer both the
//! breakdown MCP tools (`create_task` / `link_dependency`, see [`crate::mcp`])
//! and later the REST DAG API (T-302) build on. It follows the `epics.rs` store
//! style: crate-visible helpers, ULID ids, unix-ms timestamps, and atomic
//! `MAX(..)+1` ordinals assigned inside the single `INSERT`.
//!
//! ## Dependency direction & cycles
//!
//! An edge `(blocker_id, blocked_id)` reads "**blocker** blocks **blocked**".
//! Following edges forward (`blocker_id → blocked_id`) is the execution order.
//! Adding `(blocker, blocked)` creates a cycle iff `blocked` can *already reach*
//! `blocker` — then `blocker → blocked → … → blocker` closes a loop. Cycles are
//! rejected in [`link_dependency`] via [`would_create_cycle`] (a forward DFS from
//! `blocked`). T-302 formalizes readiness on top of this acyclic invariant.

use libsql::{params, params_from_iter, Connection, Row};
use serde::Serialize;
use std::collections::{HashMap, HashSet};

use crate::{AppError, AppResult};

/// Columns projected into a [`Task`] DTO, in schema (§2.2) order. The Half-2
/// executor columns added by `0004_executor.sql` (`lease_owner`,
/// `lease_expires_at`, `base_sha`) are deliberately **not** projected here —
/// they are internal claim/review-diff state, read by the worker via direct
/// SQL (T-510+), never through this DTO. `branch_name` / `pr_url` / `pr_number`
/// *are* projected: they are user-facing identity for a standalone task's run.
/// `failure_detail` (Rec 5) is projected right after `failure_reason`: it is
/// the redacted, length-capped failure message that pairs with the reason.
const TASK_COLUMNS: &str = "id, epic_id, project_id, title, description, acceptance, status, \
     failure_reason, failure_detail, agent_session_id, position, branch_name, pr_url, pr_number, created_at, updated_at";

/// A task as returned by the store / API (`task`, §2.2).
///
/// `epic_id` is `Option` because the schema permits standalone (parentless)
/// tasks (`NULL => standalone`); breakdown always sets it. `failure_reason`
/// (§2.3) is populated by the executor when a task lands in `Failed`, and
/// `failure_detail` (Rec 5) alongside it: the same event's redacted,
/// length-capped message (`worker::fail_item`), cleared on retry.
/// `branch_name` / `pr_url` / `pr_number` (M2 §2.1) are populated once a
/// standalone task provisions its workspace and opens its own PR; they stay
/// `null` for epic-scoped tasks (the epic carries the PR identity instead).
/// The lease columns are deliberately **not** on this struct — see
/// [`TASK_COLUMNS`].
#[derive(Debug, Clone, Serialize)]
pub struct Task {
    pub id: String,
    pub epic_id: Option<String>,
    pub project_id: String,
    pub title: String,
    pub description: Option<String>,
    pub acceptance: Option<String>,
    /// `Todo | InProgress | Done | Failed | Cancelled` (readiness is computed).
    pub status: String,
    pub failure_reason: Option<String>,
    /// The failed attempt's human-readable error text (Rec 5): redacted and
    /// length-capped by `worker::fail_item` before it ever lands here, so a
    /// triager can see *why* without cloning the branch or querying the DB.
    pub failure_detail: Option<String>,
    pub agent_session_id: Option<String>,
    pub position: Option<i64>,
    pub branch_name: Option<String>,
    pub pr_url: Option<String>,
    pub pr_number: Option<i64>,
    pub created_at: i64,
    pub updated_at: i64,
}

/// A dependency edge `(blocker_id, blocked_id)` — "blocker blocks blocked".
#[derive(Debug, Clone, Serialize)]
pub struct Dependency {
    pub blocker_id: String,
    pub blocked_id: String,
}

/// Insert a new task under `epic_id` / `project_id`, landing it in
/// `status='Todo'` with the next `position` for the epic.
///
/// The ordinal is `MAX(position)+1` for the epic computed **inside the single
/// INSERT**, so libSQL's single writer assigns it atomically (mirrors
/// `append_message`'s `seq`). `title` is required (validated); `description` /
/// `acceptance` are optional. Returns the stored task.
pub async fn create_task(
    conn: &Connection,
    epic_id: &str,
    project_id: &str,
    title: &str,
    description: Option<&str>,
    acceptance: Option<&str>,
) -> AppResult<Task> {
    let title = title.trim();
    if title.is_empty() {
        return Err(AppError::BadRequest(
            "`title` must not be empty".to_string(),
        ));
    }

    let id = ulid::Ulid::new().to_string();
    let now = now_ms();

    conn.execute(
        "INSERT INTO task \
             (id, epic_id, project_id, title, description, acceptance, status, position, created_at, updated_at) \
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, 'Todo', \
             (SELECT COALESCE(MAX(position), 0) + 1 FROM task WHERE epic_id = ?2), \
             ?7, ?7)",
        params![
            id.clone(),
            epic_id,
            project_id,
            title,
            description,
            acceptance,
            now
        ],
    )
    .await?;

    fetch_task(conn, &id)
        .await?
        .ok_or_else(|| AppError::Internal(format!("task {id} vanished after insert")))
}

/// Insert a **standalone** (parentless, `epic_id = NULL`) task directly under
/// a project, landing it in `status='Todo'`. Standalone tasks are small,
/// self-contained units of work that don't warrant an epic's planning /
/// breakdown / DAG ceremony; they surface on the project board (T-401) via
/// [`list_standalone_tasks`]. `position` is left `NULL` — the board orders
/// standalone tasks by `created_at DESC`, so an ordinal is meaningless here.
/// `title` is required (validated); `description` / `acceptance` are optional.
/// Returns the stored task.
pub async fn create_standalone_task(
    conn: &Connection,
    project_id: &str,
    title: &str,
    description: Option<&str>,
    acceptance: Option<&str>,
) -> AppResult<Task> {
    let title = title.trim();
    if title.is_empty() {
        return Err(AppError::BadRequest(
            "`title` must not be empty".to_string(),
        ));
    }

    let id = ulid::Ulid::new().to_string();
    let now = now_ms();

    conn.execute(
        "INSERT INTO task \
             (id, epic_id, project_id, title, description, acceptance, status, position, created_at, updated_at) \
         VALUES (?1, NULL, ?2, ?3, ?4, ?5, 'Todo', NULL, ?6, ?6)",
        params![
            id.clone(),
            project_id,
            title,
            description,
            acceptance,
            now
        ],
    )
    .await?;

    fetch_task(conn, &id)
        .await?
        .ok_or_else(|| AppError::Internal(format!("task {id} vanished after insert")))
}

/// Link a dependency edge `(blocker_id, blocked_id)` ("blocker blocks blocked").
///
/// Validates that both tasks exist and share the **same epic**, rejects a
/// self-edge (`400`), and rejects any edge that would introduce a cycle (`409`,
/// via [`would_create_cycle`]). A duplicate edge is a no-op (the PK makes the
/// INSERT idempotent under `OR IGNORE`).
pub async fn link_dependency(
    conn: &Connection,
    blocker_id: &str,
    blocked_id: &str,
) -> AppResult<()> {
    if blocker_id == blocked_id {
        return Err(AppError::BadRequest(
            "a task cannot depend on itself".to_string(),
        ));
    }

    // Both tasks must exist and belong to the same epic.
    let blocker_epic = task_epic(conn, blocker_id)
        .await?
        .ok_or_else(|| AppError::NotFound(format!("task {blocker_id} not found")))?;
    let blocked_epic = task_epic(conn, blocked_id)
        .await?
        .ok_or_else(|| AppError::NotFound(format!("task {blocked_id} not found")))?;

    // Standalone tasks carry no dependencies: they are meant to be small,
    // self-contained units of work. (This also closes the cross-project hole
    // where two standalone tasks would pass the same-epic check as NULL ==
    // NULL despite living under different projects.)
    if blocker_epic.is_none() || blocked_epic.is_none() {
        return Err(AppError::BadRequest(
            "standalone tasks cannot have dependencies".to_string(),
        ));
    }
    if blocker_epic != blocked_epic {
        return Err(AppError::BadRequest(
            "both tasks must belong to the same epic to be linked".to_string(),
        ));
    }

    if would_create_cycle(conn, blocker_id, blocked_id).await? {
        return Err(AppError::Conflict(format!(
            "linking {blocker_id} → {blocked_id} would create a dependency cycle"
        )));
    }

    conn.execute(
        "INSERT OR IGNORE INTO task_dependency (blocker_id, blocked_id) VALUES (?1, ?2)",
        params![blocker_id, blocked_id],
    )
    .await?;
    Ok(())
}

/// Remove a dependency edge `(blocker_id, blocked_id)` if present. Idempotent.
pub async fn unlink_dependency(
    conn: &Connection,
    blocker_id: &str,
    blocked_id: &str,
) -> AppResult<()> {
    conn.execute(
        "DELETE FROM task_dependency WHERE blocker_id = ?1 AND blocked_id = ?2",
        params![blocker_id, blocked_id],
    )
    .await?;
    Ok(())
}

/// Whether adding edge `(blocker_id, blocked_id)` would create a cycle.
///
/// A cycle appears iff `blocked_id` can already reach `blocker_id` by following
/// existing edges forward (`blocker → blocked`); the new edge would then close
/// the loop. Implemented as an iterative forward DFS from `blocked_id` looking
/// for `blocker_id`.
pub async fn would_create_cycle(
    conn: &Connection,
    blocker_id: &str,
    blocked_id: &str,
) -> AppResult<bool> {
    let mut stack = vec![blocked_id.to_string()];
    let mut seen: HashSet<String> = HashSet::new();
    while let Some(node) = stack.pop() {
        if node == blocker_id {
            return Ok(true);
        }
        if !seen.insert(node.clone()) {
            continue;
        }
        let mut rows = conn
            .query(
                "SELECT blocked_id FROM task_dependency WHERE blocker_id = ?1",
                params![node],
            )
            .await?;
        while let Some(row) = rows.next().await? {
            stack.push(row.get::<String>(0)?);
        }
    }
    Ok(false)
}

/// All tasks under `epic_id`, ordered by `position` (then id for stability).
pub async fn list_tasks_for_epic(conn: &Connection, epic_id: &str) -> AppResult<Vec<Task>> {
    let sql = format!(
        "SELECT {TASK_COLUMNS} FROM task WHERE epic_id = ?1 \
         ORDER BY position ASC, id ASC"
    );
    let mut rows = conn.query(&sql, params![epic_id]).await?;
    let mut items = Vec::new();
    while let Some(row) = rows.next().await? {
        items.push(row_to_task(&row)?);
    }
    Ok(items)
}

/// All dependency edges among the tasks of `epic_id`, as [`Dependency`] pairs.
pub async fn list_dependencies_for_epic(
    conn: &Connection,
    epic_id: &str,
) -> AppResult<Vec<Dependency>> {
    // Join both endpoints back to `task` so an edge is only surfaced when both of
    // its tasks live under this epic (edges are always same-epic by construction,
    // but this keeps the read robust).
    let mut rows = conn
        .query(
            "SELECT d.blocker_id, d.blocked_id FROM task_dependency d \
             JOIN task b ON b.id = d.blocker_id \
             JOIN task k ON k.id = d.blocked_id \
             WHERE b.epic_id = ?1 AND k.epic_id = ?1 \
             ORDER BY d.blocker_id ASC, d.blocked_id ASC",
            params![epic_id],
        )
        .await?;
    let mut items = Vec::new();
    while let Some(row) = rows.next().await? {
        items.push(Dependency {
            blocker_id: row.get(0)?,
            blocked_id: row.get(1)?,
        });
    }
    Ok(items)
}

/// Whether `task_id` exists and belongs to `epic_id`.
pub async fn task_belongs_to_epic(
    conn: &Connection,
    task_id: &str,
    epic_id: &str,
) -> AppResult<bool> {
    Ok(matches!(
        task_epic(conn, task_id).await?,
        Some(Some(e)) if e == epic_id
    ))
}

/// The permitted task lifecycle statuses (§2.2). Readiness is *computed* from
/// deps, so `Todo` is the only status a not-yet-ready task holds.
///
/// `InReview` (epic §4) is the "factory done, waiting on the human reviewer"
/// status, needed for **standalone** tasks whose PR lifecycle lives on the task
/// row itself. Epic-owned tasks keep `Done` as their terminal status — the epic
/// row carries `InReview` for those.
const VALID_STATUSES: &[&str] = &[
    "Todo",
    "InProgress",
    "InReview",
    "Done",
    "Failed",
    "Cancelled",
];

/// Validate a status string against the §2.2 set, or `400 bad_request`.
fn validate_status(status: &str) -> AppResult<()> {
    if VALID_STATUSES.contains(&status) {
        Ok(())
    } else {
        Err(AppError::BadRequest(format!(
            "`status` must be one of Todo|InProgress|InReview|Done|Failed|Cancelled, got `{status}`"
        )))
    }
}

/// Partially update a task. Each field is optional: a plain `Option<String>`
/// for `title`/`status` (absent → untouched); a *double option* for the nullable
/// `description`/`acceptance` (absent → untouched, `null` → clear to `NULL`,
/// value → set). `updated_at` always bumps. `404` if the task does not exist.
pub async fn update_task(
    conn: &Connection,
    id: &str,
    title: Option<String>,
    description: Option<Option<String>>,
    acceptance: Option<Option<String>>,
    status: Option<String>,
) -> AppResult<Task> {
    let mut assignments: Vec<&str> = Vec::new();
    let mut values: Vec<libsql::Value> = Vec::new();

    if let Some(title) = title {
        let title = title.trim().to_string();
        if title.is_empty() {
            return Err(AppError::BadRequest(
                "`title` must not be empty".to_string(),
            ));
        }
        assignments.push("title = ?");
        values.push(libsql::Value::Text(title));
    }
    if let Some(status) = status {
        validate_status(&status)?;
        assignments.push("status = ?");
        values.push(libsql::Value::Text(status));
    }
    for (column, field) in [
        ("description = ?", description),
        ("acceptance = ?", acceptance),
    ] {
        if let Some(value) = field {
            assignments.push(column);
            values.push(match value {
                Some(text) => libsql::Value::Text(text),
                None => libsql::Value::Null,
            });
        }
    }

    // Always bump updated_at, even for an otherwise-empty patch.
    assignments.push("updated_at = ?");
    values.push(libsql::Value::Integer(now_ms()));
    values.push(libsql::Value::Text(id.to_string()));

    let sql = format!("UPDATE task SET {} WHERE id = ?", assignments.join(", "));
    let affected = conn.execute(&sql, params_from_iter(values)).await?;
    if affected == 0 {
        return Err(AppError::NotFound(format!("task {id} not found")));
    }

    fetch_task(conn, id)
        .await?
        .ok_or_else(|| AppError::Internal(format!("task {id} vanished after update")))
}

/// Delete a task and its dependency edges (both directions). The `task_dependency`
/// table has no `ON DELETE CASCADE`, so edges are removed explicitly first.
/// Idempotent on edges; `404` if the task itself does not exist.
pub async fn delete_task(conn: &Connection, id: &str) -> AppResult<()> {
    conn.execute(
        "DELETE FROM task_dependency WHERE blocker_id = ?1 OR blocked_id = ?2",
        params![id, id],
    )
    .await?;
    let affected = conn
        .execute("DELETE FROM task WHERE id = ?1", params![id])
        .await?;
    if affected == 0 {
        return Err(AppError::NotFound(format!("task {id} not found")));
    }
    Ok(())
}

// ---- readiness & the DAG (T-302) ----------------------------------------

/// A task node in the DAG, carrying its computed readiness (§2.3). `ready` is
/// true iff `status == "Todo"` AND every task blocking it is `Done`.
/// `blocked_by` lists the blocker ids that are not yet `Done` (non-empty only
/// when the task is `Todo` and not ready); it is `[]` for non-`Todo` tasks.
#[derive(Debug, Clone, Serialize)]
pub struct DagNode {
    #[serde(flatten)]
    pub task: Task,
    /// Whether this task is claimable: `status='Todo'` with all blockers `Done`.
    pub ready: bool,
    /// Blocker ids that are not `Done` (empty unless `Todo` and not ready).
    pub blocked_by: Vec<String>,
}

/// The epic's task DAG: nodes (tasks + readiness) and edges (dependency pairs).
#[derive(Debug, Clone, Serialize)]
pub struct Dag {
    pub epic_id: String,
    pub nodes: Vec<DagNode>,
    pub edges: Vec<Dependency>,
}

/// Compute the epic's DAG with per-task readiness (§2.3). A task is **ready**
/// when its `status` is `Todo` and every blocker (a task with an edge into it)
/// is `Done`; otherwise it is blocked (or not `Todo`). `404` if the epic does not
/// exist — callers should check the epic first.
pub async fn compute_dag(conn: &Connection, epic_id: &str) -> AppResult<Dag> {
    let tasks = list_tasks_for_epic(conn, epic_id).await?;
    let edges = list_dependencies_for_epic(conn, epic_id).await?;

    // Index task status by id, and collect each task's incoming blockers. Both
    // built up-front so `tasks` can be consumed by the node-building map below.
    let mut status_by_id: HashMap<String, String> = HashMap::new();
    for t in &tasks {
        status_by_id.insert(t.id.clone(), t.status.clone());
    }
    let mut blockers: HashMap<String, Vec<String>> = HashMap::new();
    for edge in &edges {
        blockers
            .entry(edge.blocked_id.clone())
            .or_default()
            .push(edge.blocker_id.clone());
    }
    let is_done = |id: &str| status_by_id.get(id).map(|s| s == "Done").unwrap_or(false);

    let nodes = tasks
        .into_iter()
        .map(|task| {
            let incoming = blockers.get(&task.id).cloned().unwrap_or_default();
            let ready = task.status == "Todo" && incoming.iter().all(|b| is_done(b));
            let blocked_by = if task.status == "Todo" && !ready {
                incoming.iter().filter(|b| !is_done(b)).cloned().collect()
            } else {
                Vec::new()
            };
            DagNode {
                task,
                ready,
                blocked_by,
            }
        })
        .collect();

    Ok(Dag {
        epic_id: epic_id.to_string(),
        nodes,
        edges,
    })
}

// ---- REST handlers (T-302) ----------------------------------------------

use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::Json;
use serde::Deserialize;

use crate::epics::{fetch_epic, project_exists};
use crate::AppState;

/// `GET /epics/{id}/dag` — the epic's task DAG with per-task readiness. `404` if
/// the epic does not exist.
pub async fn get_dag(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> AppResult<Json<Dag>> {
    let conn = state.db.conn();
    if !epic_exists(conn, &id).await? {
        return Err(AppError::NotFound(format!("epic {id} not found")));
    }
    let dag = compute_dag(conn, &id).await?;
    Ok(Json(dag))
}

/// `GET /tasks/{id}` — fetch one task. `404` if it does not exist.
pub async fn get_task_by_id(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> AppResult<Json<Task>> {
    let task = fetch_task(state.db.conn(), &id)
        .await?
        .ok_or_else(|| AppError::NotFound(format!("task {id} not found")))?;
    Ok(Json(task))
}

/// `POST /epics/{id}/tasks` — create a task under the epic (manual or agentless
/// create, for the Ready-lane editor). Body: `{ title, description?,
/// acceptance?, blocks?: [ids] }`. `201` with the created task; `404` if the epic
/// does not exist. Publishes `dag_updated` on `epic:<id>`.
#[derive(Debug, Deserialize)]
pub struct CreateTaskBody {
    title: Option<String>,
    #[serde(default)]
    description: Option<String>,
    #[serde(default)]
    acceptance: Option<String>,
    /// Ids of existing tasks this new task blocks (optional).
    #[serde(default)]
    blocks: Vec<String>,
}

pub async fn create_epic_task(
    State(state): State<AppState>,
    Path(id): Path<String>,
    Json(req): Json<CreateTaskBody>,
) -> AppResult<(StatusCode, Json<Task>)> {
    let conn = state.db.conn();
    let epic = fetch_epic(conn, &id)
        .await?
        .ok_or_else(|| AppError::NotFound(format!("epic {id} not found")))?;
    let title = req
        .title
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .ok_or_else(|| {
            AppError::BadRequest("`title` is required and must not be empty".to_string())
        })?;

    let task = create_task(
        conn,
        &id,
        &epic.project_id,
        title,
        req.description.as_deref(),
        req.acceptance.as_deref(),
    )
    .await?;

    for blocked_id in &req.blocks {
        link_dependency(conn, &task.id, blocked_id).await?; // 404/400/409 propagate
    }

    crate::mcp::publish_dag(&state, &id).await;
    // A new task changes the epic's total on the project board's progress badge.
    crate::board::publish_board(&state, &epic.project_id).await;
    Ok((StatusCode::CREATED, Json(task)))
}

/// `POST /projects/{id}/tasks` — create a **standalone** (parentless) task
/// directly under the project, for work too small to warrant an epic. Body:
/// `{ title, description?, acceptance? }` (no `blocks` — standalone tasks
/// carry no dependencies). `201` with the created task; `404` if the project
/// does not exist. Publishes `board_updated` on `project:<id>`.
#[derive(Debug, Deserialize)]
pub struct CreateStandaloneTaskBody {
    title: Option<String>,
    #[serde(default)]
    description: Option<String>,
    #[serde(default)]
    acceptance: Option<String>,
}

pub async fn create_project_task(
    State(state): State<AppState>,
    Path(id): Path<String>,
    Json(req): Json<CreateStandaloneTaskBody>,
) -> AppResult<(StatusCode, Json<Task>)> {
    let conn = state.db.conn();
    if !project_exists(conn, &id).await? {
        return Err(AppError::NotFound(format!("project {id} not found")));
    }
    let title = req
        .title
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .ok_or_else(|| {
            AppError::BadRequest("`title` is required and must not be empty".to_string())
        })?;

    let task = create_standalone_task(
        conn,
        &id,
        title,
        req.description.as_deref(),
        req.acceptance.as_deref(),
    )
    .await?;

    crate::board::publish_board(&state, &id).await;
    Ok((StatusCode::CREATED, Json(task)))
}

/// `PATCH /tasks/{id}` — partial update (double-option for nullable fields).
/// `200` with the updated task; `404` if it does not exist. Publishes
/// `dag_updated` on the task's epic (or `board_updated` on its project when
/// the task is standalone). Epic-scoped tasks also publish `board_updated` — a
/// status change moves the epic's done/total progress badge on the project
/// kanban.
#[derive(Debug, Deserialize)]
pub struct UpdateTaskBody {
    #[serde(default)]
    title: Option<String>,
    #[serde(default, deserialize_with = "double_option")]
    description: Option<Option<String>>,
    #[serde(default, deserialize_with = "double_option")]
    acceptance: Option<Option<String>>,
    #[serde(default)]
    status: Option<String>,
}

pub async fn patch_task(
    State(state): State<AppState>,
    Path(id): Path<String>,
    Json(req): Json<UpdateTaskBody>,
) -> AppResult<Json<Task>> {
    let conn = state.db.conn();
    let task = update_task(
        conn,
        &id,
        req.title,
        req.description,
        req.acceptance,
        req.status,
    )
    .await?;
    if let Some(epic_id) = task.epic_id.as_ref() {
        crate::mcp::publish_dag(&state, epic_id).await;
    }
    // Always refresh the project board: standalone tasks live on it directly,
    // and an epic-scoped status change moves the epic's progress badge.
    crate::board::publish_board(&state, &task.project_id).await;
    Ok(Json(task))
}

/// `POST /tasks/{id}/retry` — D11's human-in-the-loop recovery transition
/// (MILESTONE_2 §1 D11, §7 T-541, revised for standalone tasks by §8 T-551):
/// for an **epic-scoped** task, `Failed → Todo` (clearing `failure_reason`
/// and its `failure_detail`),
/// and — iff the parent epic is currently `Blocked` — `Blocked → InProgress`
/// too (clearing `blocked_reason` and the lease), so the worker pool's claim
/// query (`worker::claim_epic`, §2.4) picks the epic back up and re-attaches
/// its retained workspace: T-511's `git reset --hard HEAD` + `git clean -fd`
/// drop exactly the failed attempt's dirty tree, and the DAG walk re-enters
/// at the now-`Todo` task. `404` if the task does not exist; `409 conflict`
/// unless it is currently `Failed` (§2.5's endpoint table). Editing the
/// task's spec via `PATCH /tasks/{id}` before calling this endpoint needs no
/// special handling here — the next `implement`/`fix` stage simply
/// re-renders whatever `description`/`acceptance` are on the row at claim
/// time (T-502), so an edited spec is what the re-run sees "for free".
///
/// ## A standalone task (`epic_id IS NULL`) retries to `InProgress`, not `Todo`
///
/// T-541 originally sent every retried task to `Todo`, its own doc noting
/// that resuming a standalone task was left for T-551. Taken literally that
/// is a dead end: `worker::claim_task`'s predicate only ever selects
/// `status = 'InProgress' AND epic_id IS NULL` (§2.4) — a task sitting in
/// `Todo` is never claimed by any worker, so retry would silently *not*
/// resume anything; a human would additionally have to call
/// `POST /tasks/{id}/run` and get no signal that "retried" didn't mean
/// "resumed."
///
/// The fix follows from what a standalone task actually *is*: unlike an
/// epic-scoped task, where the claimable item (the epic) and the unit of
/// work (the task) are two different rows with two different statuses, a
/// standalone task is both at once. The epic branch above restores the
/// *claimable item* (the epic) to `InProgress` while resetting the *unit of
/// work* (the task) to `Todo` — two writes, because they're two rows. A
/// standalone task has one row playing both roles, so restoring its
/// claimability *is* resetting its work: this endpoint moves it straight to
/// `InProgress` (via a `CASE WHEN epic_id IS NULL` in the single fenced
/// `UPDATE` below), clearing `failure_reason`/`failure_detail` and the lease
/// columns
/// (defensive — a `Failed` task's lease was already released by
/// `worker::run_claimed_standalone` on every exit path, so these are
/// normally already `NULL`; clearing them here costs nothing and mirrors the
/// epic branch's own lease clear exactly). `state.notify.notify_waiters()`
/// below (already called unconditionally) is what actually wakes a worker to
/// reclaim it — see `worker_tests::retried_standalone_task_is_reclaimed_and_rerun`
/// for proof the whole loop resumes, not just that the HTTP response looks
/// right.
///
/// ## Making the transition atomic (D11) without a new transaction idiom
///
/// "One atomic transition" here does not mean wrapping these writes in a
/// `BEGIN`/`COMMIT` — nothing else in this codebase does that; libSQL's
/// single shared writer connection (`db.rs`'s module doc) is the concurrency
/// primitive every other multi-statement flow in this crate already relies
/// on (`lanes::set_epic_lane`, `worker::fail_item`). What actually needs
/// guaranteeing is that **no concurrent observer — specifically a worker's
/// claim query — can ever see a state this handler produced that is unsafe
/// to act on**, even though the task and epic rows are necessarily two
/// separate `UPDATE`s. Two choices make that true:
///
/// 1. **The task write is itself a single fenced `UPDATE ... RETURNING`**,
///    the identical idiom `worker::claim_epic` uses for its own atomic
///    check-and-claim: the `WHERE status = 'Failed'` is evaluated by SQLite
///    at the instant of the write, not against a stale value read moments
///    earlier by a separate `SELECT`. Two concurrent retries of the same
///    task can therefore never both "win" — the loser's `UPDATE` affects
///    zero rows and reports `409`, exactly as if it had arrived a moment
///    later and observed the already-`Todo` task directly.
/// 2. **The task `UPDATE` runs strictly before the epic `UPDATE`.** A
///    worker's claim query only ever selects epics with
///    `status = 'InProgress'` (§2.4) — so the epic cannot become claimable
///    until *after* this function has already committed the task's
///    `Todo` write. There is consequently no window in which a worker could
///    claim the epic and find the DAG's just-retried task still `Failed`
///    (which would leave it stuck — not ready, and no longer being retried).
///    Ordering it the other way (epic first) would open exactly that
///    window. This mirrors `worker::fail_item`'s own ordering discipline
///    (task `Failed` is written, then the epic is fenced to `Blocked`) in
///    the opposite direction.
///
/// The epic `UPDATE` is itself fenced on `status = 'Blocked'` (matching
/// D11's "iff Blocked" — an epic in any other state, e.g. manually
/// `Cancelled` mid-triage, is left untouched) so it is a no-op rather than a
/// clobber if something else already moved the epic on.
///
/// On success: `dag_updated` + `epic_updated` on `epic:<id>` (only when the
/// task has an epic) and `board_updated` on `project:<id>` — mirroring what
/// `lanes::set_epic_lane` publishes for a lane move — then
/// `state.notify.notify_waiters()` so an idle worker loop wakes immediately
/// instead of waiting out the poll interval.
pub async fn retry_task(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> AppResult<Json<Task>> {
    let conn = state.db.conn();
    let now = now_ms();

    // T-551: `status` is `Todo` for an epic-scoped task (unit-of-work reset;
    // the epic UPDATE below is what restores *its* claimability) but
    // `InProgress` for a standalone task (there is no separate row to
    // restore claimability on — see this function's own doc, "A standalone
    // task retries to InProgress, not Todo"). `lease_owner`/`lease_expires_at`
    // are cleared defensively for the same standalone case (normally already
    // `NULL` by the time a task reaches `Failed` — `run_claimed_standalone`
    // releases the lease on every exit path).
    let sql = format!(
        "UPDATE task SET \
             status = CASE WHEN epic_id IS NULL THEN 'InProgress' ELSE 'Todo' END, \
             failure_reason = NULL, \
             failure_detail = NULL, \
             lease_owner = NULL, \
             lease_expires_at = NULL, \
             updated_at = ?1 \
         WHERE id = ?2 AND status = 'Failed' \
         RETURNING {TASK_COLUMNS}"
    );
    let mut rows = conn.query(&sql, params![now, id.clone()]).await?;
    let task = match rows.next().await? {
        Some(row) => row_to_task(&row)?,
        None => {
            // The fenced UPDATE affected nothing: either the task doesn't
            // exist (404) or it exists but isn't Failed (409) — a cheap
            // extra lookup only on this (already-erroring) path tells them
            // apart, same precision `update_task`/`delete_task` give every
            // other task endpoint.
            return Err(match fetch_task(conn, &id).await? {
                Some(_) => AppError::Conflict(format!("task {id} is not Failed")),
                None => AppError::NotFound(format!("task {id} not found")),
            });
        }
    };

    if let Some(epic_id) = task.epic_id.as_ref() {
        conn.execute(
            "UPDATE epic SET status = 'InProgress', blocked_reason = NULL, \
                 failure_detail = NULL, lease_owner = NULL, lease_expires_at = NULL, updated_at = ?1 \
             WHERE id = ?2 AND status = 'Blocked'",
            params![now, epic_id.clone()],
        )
        .await?;

        crate::mcp::publish_dag(&state, epic_id).await;
        if let Some(updated_epic) = fetch_epic(conn, epic_id).await? {
            let payload = serde_json::to_value(&updated_epic).unwrap_or(serde_json::Value::Null);
            state
                .hub
                .publish(&format!("epic:{epic_id}"), "epic_updated", payload);
        }
    }
    // Always refresh the project board: a standalone task's retry is the
    // only visible change for it; an epic-scoped retry also moves the
    // epic's card back into the In Progress lane.
    crate::board::publish_board(&state, &task.project_id).await;

    state.notify.notify_waiters();

    Ok(Json(task))
}

/// `POST /tasks/{id}/run` — T-551 (MILESTONE_2 §8, §2.5): the standalone-task
/// counterpart to an epic's `Ready → InProgress` lane move
/// (`lanes::set_epic_lane`) — `Todo → InProgress` so the worker pool's claim
/// query (`worker::claim_task`, §2.4) picks the task up on its own leased run:
/// its own workspace (`<clone_root>/tasks/{id}`), its own branch (§2.8), the
/// full pipeline (preflight → implement → gate → review → PR), its own PR.
/// `404` if the task does not exist; `409` unless the task is `Todo` **and**
/// `epic_id IS NULL` (§2.5's endpoint table) — an epic-scoped task is only
/// ever run as part of its epic's own `InProgress` transition, never through
/// this endpoint directly.
///
/// The fenced `UPDATE` folds both conditions (`status = 'Todo' AND epic_id IS
/// NULL`) into the write itself rather than checking them with a separate
/// `SELECT` first — the identical atomic check-and-write idiom
/// `retry_task`/`worker::claim_epic` already use for the same reason: two
/// concurrent calls can never both "win" a race that reads as `200` — the
/// loser's `UPDATE` affects zero rows and reports `409`, exactly as if it had
/// arrived a moment later and observed the already-`InProgress` task
/// directly. An epic-scoped task fails the same `WHERE` clause (`epic_id IS
/// NULL` excludes it) regardless of its own `status`, so it always reports
/// `409` here rather than accidentally becoming independently runnable.
///
/// On success: `board_updated` on `project:<id>` — a standalone task's own
/// status change is the entire board-visible effect of this transition (no
/// `dag_updated`; a standalone task has no DAG), mirroring every other
/// standalone-task mutation in this file — then
/// `state.notify.notify_waiters()` so an idle worker loop wakes immediately
/// instead of waiting out the poll interval.
pub async fn run_task(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> AppResult<Json<Task>> {
    let conn = state.db.conn();
    let now = now_ms();

    let sql = format!(
        "UPDATE task SET status = 'InProgress', updated_at = ?1 \
         WHERE id = ?2 AND status = 'Todo' AND epic_id IS NULL \
         RETURNING {TASK_COLUMNS}"
    );
    let mut rows = conn.query(&sql, params![now, id.clone()]).await?;
    let task = match rows.next().await? {
        Some(row) => row_to_task(&row)?,
        None => {
            // The fenced UPDATE affected nothing: either the task doesn't
            // exist (404) or it exists but isn't runnable — not `Todo`, or
            // epic-scoped, or both (409) — a cheap extra lookup only on this
            // (already-erroring) path tells them apart, same precision every
            // other task endpoint in this file gives.
            return Err(match fetch_task(conn, &id).await? {
                Some(_) => AppError::Conflict(format!(
                    "task {id} is not runnable (must be Todo with no epic)"
                )),
                None => AppError::NotFound(format!("task {id} not found")),
            });
        }
    };

    crate::board::publish_board(&state, &task.project_id).await;

    state.notify.notify_waiters();

    Ok(Json(task))
}

/// `DELETE /tasks/{id}` — remove a task and its dependency edges. `204`;
/// `404` if it does not exist. Publishes `dag_updated` on the task's epic
/// (or `board_updated` on its project when the task is standalone — a NULL
/// epic must not be misread as "missing"). Epic-scoped deletes also publish
/// `board_updated` (the epic's progress total changes).
pub async fn remove_task(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> AppResult<StatusCode> {
    let conn = state.db.conn();
    let task = fetch_task(conn, &id)
        .await?
        .ok_or_else(|| AppError::NotFound(format!("task {id} not found")))?;
    delete_task(conn, &id).await?;
    if let Some(epic_id) = task.epic_id.as_ref() {
        crate::mcp::publish_dag(&state, epic_id).await;
    }
    crate::board::publish_board(&state, &task.project_id).await;
    Ok(StatusCode::NO_CONTENT)
}

/// `POST /epics/{id}/dependencies` — link `blocker_id` → `blocked_id`. Both
/// tasks must belong to the path epic. `201` with the edge; `400` on self/cross-
/// epic; `409` on a cycle; `404` if the epic or a task is missing. Publishes
/// `dag_updated`.
#[derive(Debug, Deserialize)]
pub struct LinkDependencyBody {
    blocker_id: Option<String>,
    blocked_id: Option<String>,
}

pub async fn post_dependency(
    State(state): State<AppState>,
    Path(id): Path<String>,
    Json(req): Json<LinkDependencyBody>,
) -> AppResult<(StatusCode, Json<Dependency>)> {
    let conn = state.db.conn();
    if !epic_exists(conn, &id).await? {
        return Err(AppError::NotFound(format!("epic {id} not found")));
    }
    let blocker_id = req
        .blocker_id
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .ok_or_else(|| AppError::BadRequest("`blocker_id` is required".to_string()))?;
    let blocked_id = req
        .blocked_id
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .ok_or_else(|| AppError::BadRequest("`blocked_id` is required".to_string()))?;

    // Both endpoints must belong to the path epic (not merely the same epic).
    for tid in [blocker_id, blocked_id] {
        if !task_belongs_to_epic(conn, tid, &id).await? {
            return Err(AppError::BadRequest(format!(
                "task {tid} is not part of epic {id}"
            )));
        }
    }

    link_dependency(conn, blocker_id, blocked_id).await?; // 400 self/cross, 409 cycle
    crate::mcp::publish_dag(&state, &id).await;
    Ok((
        StatusCode::CREATED,
        Json(Dependency {
            blocker_id: blocker_id.to_string(),
            blocked_id: blocked_id.to_string(),
        }),
    ))
}

/// `DELETE /epics/{id}/dependencies?blocker_id=X&blocked_id=Y` — remove an edge.
/// Idempotent: `204` whether or not the edge existed. `404` if the epic does not
/// exist. Publishes `dag_updated`.
#[derive(Debug, Deserialize)]
pub struct UnlinkQuery {
    blocker_id: String,
    blocked_id: String,
}

pub async fn remove_dependency(
    State(state): State<AppState>,
    Path(id): Path<String>,
    Query(q): Query<UnlinkQuery>,
) -> AppResult<StatusCode> {
    let conn = state.db.conn();
    if !epic_exists(conn, &id).await? {
        return Err(AppError::NotFound(format!("epic {id} not found")));
    }
    unlink_dependency(conn, &q.blocker_id, &q.blocked_id).await?;
    crate::mcp::publish_dag(&state, &id).await;
    Ok(StatusCode::NO_CONTENT)
}

/// Deserialize a present-but-maybe-null field into `Some(_)`, leaving an absent
/// field as `None` (mirrors `projects.rs`'s double-option for nullable PATCH
/// fields).
fn double_option<'de, D, T>(deserializer: D) -> Result<Option<Option<T>>, D::Error>
where
    D: serde::Deserializer<'de>,
    T: Deserialize<'de>,
{
    Deserialize::deserialize(deserializer).map(Some)
}

/// Whether an epic exists (lightweight existence check for route guards).
async fn epic_exists(conn: &Connection, epic_id: &str) -> AppResult<bool> {
    let mut rows = conn
        .query("SELECT 1 FROM epic WHERE id = ?1", params![epic_id])
        .await?;
    Ok(rows.next().await?.is_some())
}

// ---- row / value plumbing ----------------------------------------------

/// Fetch one task by id, or `None`.
pub async fn fetch_task(conn: &Connection, id: &str) -> AppResult<Option<Task>> {
    let sql = format!("SELECT {TASK_COLUMNS} FROM task WHERE id = ?1");
    let mut rows = conn.query(&sql, params![id]).await?;
    match rows.next().await? {
        Some(row) => Ok(Some(row_to_task(&row)?)),
        None => Ok(None),
    }
}

/// The `epic_id` of a task as a **double option**: outer `None` = the task
/// does not exist; inner `None` = the task exists but is standalone (NULL
/// epic). Keeping the two cases distinct lets callers reject standalone tasks
/// with a clear `400` instead of a misleading `404`. Also used by the MCP
/// breakdown tools to give the agent a *self-correctable* rejection: "no such
/// task" vs "belongs to another epic".
pub(crate) async fn task_epic(
    conn: &Connection,
    task_id: &str,
) -> AppResult<Option<Option<String>>> {
    let mut rows = conn
        .query("SELECT epic_id FROM task WHERE id = ?1", params![task_id])
        .await?;
    match rows.next().await? {
        Some(row) => Ok(Some(row.get::<Option<String>>(0)?)),
        None => Ok(None),
    }
}

/// Standalone (parentless, `epic_id IS NULL`) tasks for a project, newest
/// first. Reused by the board loader (T-401) so the kanban shows tasks that are
/// not part of any epic's DAG.
pub(crate) async fn list_standalone_tasks(
    conn: &Connection,
    project_id: &str,
) -> AppResult<Vec<Task>> {
    let sql = format!(
        "SELECT {TASK_COLUMNS} FROM task WHERE project_id = ?1 AND epic_id IS NULL \
         ORDER BY created_at DESC, id DESC"
    );
    let mut rows = conn.query(&sql, params![project_id]).await?;
    let mut items = Vec::new();
    while let Some(row) = rows.next().await? {
        items.push(row_to_task(&row)?);
    }
    Ok(items)
}

fn row_to_task(row: &Row) -> Result<Task, libsql::Error> {
    Ok(Task {
        id: row.get(0)?,
        epic_id: row.get(1)?,
        project_id: row.get(2)?,
        title: row.get(3)?,
        description: row.get(4)?,
        acceptance: row.get(5)?,
        status: row.get(6)?,
        failure_reason: row.get(7)?,
        failure_detail: row.get(8)?,
        agent_session_id: row.get(9)?,
        position: row.get(10)?,
        branch_name: row.get(11)?,
        pr_url: row.get(12)?,
        pr_number: row.get(13)?,
        created_at: row.get(14)?,
        updated_at: row.get(15)?,
    })
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
    use crate::{Config, Db};

    /// Boot an in-memory db + seed a project and epic; return (conn-holder, ids).
    async fn seed() -> (Db, String, String) {
        let db = Db::connect(":memory:").await.unwrap();
        db.run_migrations().await.unwrap();
        let conn = db.conn();
        let now = now_ms();
        let project_id = ulid::Ulid::new().to_string();
        conn.execute(
            "INSERT INTO project (id, name, repo_url, clone_status, created_at, updated_at) \
             VALUES (?1, 'P', 'https://example.com/p.git', 'ready', ?2, ?2)",
            params![project_id.clone(), now],
        )
        .await
        .unwrap();
        let epic_id = ulid::Ulid::new().to_string();
        conn.execute(
            "INSERT INTO epic (id, project_id, title, status, created_at, updated_at) \
             VALUES (?1, ?2, 'E', 'Planning', ?3, ?3)",
            params![epic_id.clone(), project_id.clone(), now],
        )
        .await
        .unwrap();
        // Silence unused Config import churn: not needed here.
        let _ = Config::for_test;
        (db, project_id, epic_id)
    }

    #[tokio::test]
    async fn create_task_round_trips_and_assigns_position() {
        let (db, project_id, epic_id) = seed().await;
        let conn = db.conn();

        let a = create_task(
            conn,
            &epic_id,
            &project_id,
            "First",
            Some("does X"),
            Some("X works"),
        )
        .await
        .unwrap();
        assert_eq!(a.title, "First");
        assert_eq!(a.description.as_deref(), Some("does X"));
        assert_eq!(a.acceptance.as_deref(), Some("X works"));
        assert_eq!(a.status, "Todo");
        assert_eq!(a.position, Some(1));
        assert_eq!(a.epic_id.as_deref(), Some(epic_id.as_str()));

        let b = create_task(conn, &epic_id, &project_id, "Second", None, None)
            .await
            .unwrap();
        assert_eq!(b.position, Some(2), "position increments per epic");
        assert_eq!(b.description, None);

        // Round-trip via fetch + list.
        let fetched = fetch_task(conn, &a.id).await.unwrap().unwrap();
        assert_eq!(fetched.id, a.id);
        let listed = list_tasks_for_epic(conn, &epic_id).await.unwrap();
        assert_eq!(listed.len(), 2);
        assert_eq!(listed[0].id, a.id);
        assert_eq!(listed[1].id, b.id);
    }

    #[tokio::test]
    async fn create_task_rejects_empty_title() {
        let (db, project_id, epic_id) = seed().await;
        let err = create_task(db.conn(), &epic_id, &project_id, "   ", None, None)
            .await
            .unwrap_err();
        assert!(matches!(err, AppError::BadRequest(_)));
    }

    #[tokio::test]
    async fn link_dependency_stores_edge_and_lists_it() {
        let (db, project_id, epic_id) = seed().await;
        let conn = db.conn();
        let a = create_task(conn, &epic_id, &project_id, "A", None, None)
            .await
            .unwrap();
        let b = create_task(conn, &epic_id, &project_id, "B", None, None)
            .await
            .unwrap();

        link_dependency(conn, &a.id, &b.id).await.unwrap();
        // Duplicate link is a no-op (idempotent).
        link_dependency(conn, &a.id, &b.id).await.unwrap();

        let edges = list_dependencies_for_epic(conn, &epic_id).await.unwrap();
        assert_eq!(edges.len(), 1);
        assert_eq!(edges[0].blocker_id, a.id);
        assert_eq!(edges[0].blocked_id, b.id);

        // Unlink removes it.
        unlink_dependency(conn, &a.id, &b.id).await.unwrap();
        assert!(list_dependencies_for_epic(conn, &epic_id)
            .await
            .unwrap()
            .is_empty());
    }

    #[tokio::test]
    async fn link_dependency_rejects_self_edge() {
        let (db, project_id, epic_id) = seed().await;
        let conn = db.conn();
        let a = create_task(conn, &epic_id, &project_id, "A", None, None)
            .await
            .unwrap();
        let err = link_dependency(conn, &a.id, &a.id).await.unwrap_err();
        assert!(matches!(err, AppError::BadRequest(_)));
    }

    #[tokio::test]
    async fn link_dependency_rejects_cross_epic() {
        let (db, project_id, epic_id) = seed().await;
        let conn = db.conn();
        // A second epic in the same project with its own task.
        let other_epic = ulid::Ulid::new().to_string();
        conn.execute(
            "INSERT INTO epic (id, project_id, title, status, created_at, updated_at) \
             VALUES (?1, ?2, 'E2', 'Planning', ?3, ?3)",
            params![other_epic.clone(), project_id.clone(), now_ms()],
        )
        .await
        .unwrap();
        let a = create_task(conn, &epic_id, &project_id, "A", None, None)
            .await
            .unwrap();
        let x = create_task(conn, &other_epic, &project_id, "X", None, None)
            .await
            .unwrap();

        let err = link_dependency(conn, &a.id, &x.id).await.unwrap_err();
        assert!(
            matches!(err, AppError::BadRequest(_)),
            "cross-epic link rejected"
        );
    }

    #[tokio::test]
    async fn link_dependency_rejects_missing_task() {
        let (db, project_id, epic_id) = seed().await;
        let conn = db.conn();
        let a = create_task(conn, &epic_id, &project_id, "A", None, None)
            .await
            .unwrap();
        let err = link_dependency(conn, &a.id, "does-not-exist")
            .await
            .unwrap_err();
        assert!(matches!(err, AppError::NotFound(_)));
    }

    #[tokio::test]
    async fn link_dependency_rejects_cycles() {
        let (db, project_id, epic_id) = seed().await;
        let conn = db.conn();
        let a = create_task(conn, &epic_id, &project_id, "A", None, None)
            .await
            .unwrap();
        let b = create_task(conn, &epic_id, &project_id, "B", None, None)
            .await
            .unwrap();
        let c = create_task(conn, &epic_id, &project_id, "C", None, None)
            .await
            .unwrap();

        // A -> B -> C is a valid chain.
        link_dependency(conn, &a.id, &b.id).await.unwrap();
        link_dependency(conn, &b.id, &c.id).await.unwrap();

        // C -> A would close the loop A->B->C->A: rejected as a conflict.
        let err = link_dependency(conn, &c.id, &a.id).await.unwrap_err();
        assert!(
            matches!(err, AppError::Conflict(_)),
            "cycle must be rejected, got {err:?}"
        );

        // The rejected edge was not persisted.
        let edges = list_dependencies_for_epic(conn, &epic_id).await.unwrap();
        assert_eq!(edges.len(), 2);
    }

    #[tokio::test]
    async fn task_belongs_to_epic_is_accurate() {
        let (db, project_id, epic_id) = seed().await;
        let conn = db.conn();
        let a = create_task(conn, &epic_id, &project_id, "A", None, None)
            .await
            .unwrap();
        assert!(task_belongs_to_epic(conn, &a.id, &epic_id).await.unwrap());
        assert!(!task_belongs_to_epic(conn, &a.id, "other-epic")
            .await
            .unwrap());
        assert!(!task_belongs_to_epic(conn, "nope", &epic_id).await.unwrap());
    }

    // ---- T-302: readiness, DAG API, REST CRUD ----

    use crate::breakdown::testing::SilentBreakdownAgent;
    use crate::planning::testing::SilentPlanningAgent;
    use crate::{app, AppState};
    use axum::body::Body;
    use axum::http::{
        header::{AUTHORIZATION, CONTENT_TYPE},
        Request, StatusCode,
    };
    use serde_json::{json, Value};
    use std::sync::Arc;
    use tower::ServiceExt;

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

    /// Boot an app over a freshly-seeded project + epic; return (state, app, ids).
    async fn seed_app() -> (AppState, axum::Router, String, String) {
        let db = Db::connect(":memory:").await.unwrap();
        db.run_migrations().await.unwrap();
        let state = AppState::with_agents(
            Config::for_test(),
            db,
            Arc::new(SilentPlanningAgent),
            Arc::new(SilentBreakdownAgent),
        );
        let app = app(state.clone());
        let conn = state.db.conn();
        let now = now_ms();
        let project_id = ulid::Ulid::new().to_string();
        conn.execute(
            "INSERT INTO project (id, name, repo_url, clone_status, created_at, updated_at) \
             VALUES (?1, 'P', 'https://example.com/p.git', 'ready', ?2, ?2)",
            params![project_id.clone(), now],
        )
        .await
        .unwrap();
        let epic_id = ulid::Ulid::new().to_string();
        conn.execute(
            "INSERT INTO epic (id, project_id, title, status, created_at, updated_at) \
             VALUES (?1, ?2, 'E', 'Ready', ?3, ?3)",
            params![epic_id.clone(), project_id.clone(), now],
        )
        .await
        .unwrap();
        (state, app, project_id, epic_id)
    }

    #[tokio::test]
    async fn compute_dag_readiness_follows_the_contract() {
        let (db, project_id, epic_id) = seed().await;
        let conn = db.conn();

        // A (Todo, no blockers) -> B (Todo, blocked by A) -> C (Todo, blocked by B).
        let a = create_task(conn, &epic_id, &project_id, "A", None, None)
            .await
            .unwrap();
        let b = create_task(conn, &epic_id, &project_id, "B", None, None)
            .await
            .unwrap();
        let c = create_task(conn, &epic_id, &project_id, "C", None, None)
            .await
            .unwrap();
        link_dependency(conn, &a.id, &b.id).await.unwrap();
        link_dependency(conn, &b.id, &c.id).await.unwrap();

        let dag = compute_dag(conn, &epic_id).await.unwrap();
        assert_eq!(dag.epic_id, epic_id);
        assert_eq!(dag.nodes.len(), 3);
        assert_eq!(dag.edges.len(), 2);
        let node = |id: &str| dag.nodes.iter().find(|n| n.task.id == id).unwrap();
        assert!(node(&a.id).ready, "A: Todo, no blockers -> ready");
        assert!(!node(&b.id).ready, "B: blocked by A (not Done)");
        assert_eq!(node(&b.id).blocked_by, vec![a.id.clone()]);
        assert!(!node(&c.id).ready, "C: blocked by B (not Done)");
        assert_eq!(node(&c.id).blocked_by, vec![b.id.clone()]);

        // Mark A Done -> B becomes ready (its only blocker is Done); C still
        // blocked by B.
        update_task(conn, &a.id, None, None, None, Some("Done".to_string()))
            .await
            .unwrap();
        let dag = compute_dag(conn, &epic_id).await.unwrap();
        let node = |id: &str| dag.nodes.iter().find(|n| n.task.id == id).unwrap();
        assert!(!node(&a.id).ready, "A is Done -> not ready");
        assert!(node(&a.id).blocked_by.is_empty());
        assert!(
            node(&b.id).ready,
            "B: Todo + only blocker A is Done -> ready"
        );
        assert!(node(&b.id).blocked_by.is_empty());
        assert!(!node(&c.id).ready, "C still blocked by B (Todo)");
        assert_eq!(node(&c.id).blocked_by, vec![b.id.clone()]);

        // Mark B InProgress -> C stays blocked (B not Done), and B is not ready.
        update_task(
            conn,
            &b.id,
            None,
            None,
            None,
            Some("InProgress".to_string()),
        )
        .await
        .unwrap();
        let dag = compute_dag(conn, &epic_id).await.unwrap();
        let node = |id: &str| dag.nodes.iter().find(|n| n.task.id == id).unwrap();
        assert!(!node(&b.id).ready, "B InProgress -> not ready");
        assert!(!node(&c.id).ready, "C blocked by B (InProgress, not Done)");
        assert_eq!(node(&c.id).blocked_by, vec![b.id.clone()]);
    }

    #[tokio::test]
    async fn get_dag_endpoint_returns_readiness_and_404s_for_unknown_epic() {
        let (state, app, _p, epic_id) = seed_app().await;
        let conn = state.db.conn();
        create_task(conn, &epic_id, &_p, "A", None, None)
            .await
            .unwrap();

        let response = app
            .clone()
            .oneshot(req("GET", &format!("/epics/{epic_id}/dag"), None))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let dag = body_json(response).await;
        assert_eq!(dag["epic_id"], epic_id);
        assert_eq!(dag["nodes"].as_array().unwrap().len(), 1);
        assert_eq!(dag["nodes"][0]["ready"], true);
        assert_eq!(dag["nodes"][0]["title"], "A");
        assert!(dag["edges"].as_array().unwrap().is_empty());

        // Unknown epic -> 404.
        let response = app
            .oneshot(req("GET", "/epics/nope/dag", None))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn create_epic_task_endpoint_creates_publishes_and_links_blocks() {
        let (state, app, _p, epic_id) = seed_app().await;
        let conn = state.db.conn();
        // A pre-existing task the new one will block.
        let b = create_task(conn, &epic_id, &_p, "B", None, None)
            .await
            .unwrap();

        let mut sub = state.hub.subscribe(&format!("epic:{epic_id}"));

        let response = app
            .clone()
            .oneshot(req(
                "POST",
                &format!("/epics/{epic_id}/tasks"),
                Some(
                    json!({"title":"A","description":"slice","acceptance":"works","blocks":[b.id]}),
                ),
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::CREATED);
        let task = body_json(response).await;
        assert_eq!(task["title"], "A");
        assert_eq!(task["status"], "Todo");

        // Edge A -> B was wired.
        let edges = list_dependencies_for_epic(conn, &epic_id).await.unwrap();
        assert_eq!(edges.len(), 1);
        assert_eq!(edges[0].blocked_id, b.id);

        // A dag_updated frame fired.
        let frame = sub.recv().await.unwrap();
        let v: Value = serde_json::from_str(&frame).unwrap();
        assert_eq!(v["type"], "dag_updated");

        // Missing title -> 400; unknown epic -> 404.
        let bad = app
            .clone()
            .oneshot(req(
                "POST",
                &format!("/epics/{epic_id}/tasks"),
                Some(json!({"title":"  "})),
            ))
            .await
            .unwrap();
        assert_eq!(bad.status(), StatusCode::BAD_REQUEST);
        let missing = app
            .oneshot(req("POST", "/epics/nope/tasks", Some(json!({"title":"X"}))))
            .await
            .unwrap();
        assert_eq!(missing.status(), StatusCode::NOT_FOUND);
    }

    /// §4: `InReview` is a valid stored task status (standalone-task PR
    /// lifecycle sits on the task row). `PATCH` to it persists like any other
    /// valid status, and `update_task`'s `validate_status` gate accepts it.
    #[tokio::test]
    async fn patch_task_accepts_in_review_status() {
        let (state, app, project_id, epic_id) = seed_app().await;
        let conn = state.db.conn();
        let a = create_task(conn, &epic_id, &project_id, "A", None, None)
            .await
            .unwrap();

        let response = app
            .oneshot(req(
                "PATCH",
                &format!("/tasks/{}", a.id),
                Some(json!({ "status": "InReview" })),
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(body_json(response).await["status"], "InReview");
    }

    #[tokio::test]
    async fn patch_task_updates_clears_and_rejects_bad_status() {
        let (state, app, _p, epic_id) = seed_app().await;
        let conn = state.db.conn();
        let a = create_task(conn, &epic_id, &_p, "A", Some("d"), Some("acc"))
            .await
            .unwrap();

        // Patch title + clear description (null) + set status.
        let response = app
            .clone()
            .oneshot(req(
                "PATCH",
                &format!("/tasks/{}", a.id),
                Some(json!({"title":"A2","description":null,"status":"Done"})),
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let task = body_json(response).await;
        assert_eq!(task["title"], "A2");
        assert_eq!(task["description"], Value::Null, "null clears the field");
        assert_eq!(task["acceptance"], "acc", "absent field untouched");
        assert_eq!(task["status"], "Done");

        // Invalid status -> 400.
        let bad = app
            .clone()
            .oneshot(req(
                "PATCH",
                &format!("/tasks/{}", a.id),
                Some(json!({"status":"Weird"})),
            ))
            .await
            .unwrap();
        assert_eq!(bad.status(), StatusCode::BAD_REQUEST);

        // Unknown task -> 404.
        let missing = app
            .oneshot(req("PATCH", "/tasks/nope", Some(json!({"title":"x"}))))
            .await
            .unwrap();
        assert_eq!(missing.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn patch_epic_scoped_task_also_publishes_board_updated() {
        let (state, app, project_id, epic_id) = seed_app().await;
        let conn = state.db.conn();
        let a = create_task(conn, &epic_id, &project_id, "A", None, None)
            .await
            .unwrap();

        let mut epic_sub = state.hub.subscribe(&format!("epic:{epic_id}"));
        let mut proj_sub = state.hub.subscribe(&format!("project:{project_id}"));

        let response = app
            .oneshot(req(
                "PATCH",
                &format!("/tasks/{}", a.id),
                Some(json!({"status":"Done"})),
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        // dag_updated on the epic topic (existing behaviour).
        let frame = epic_sub.recv().await.unwrap();
        let v: Value = serde_json::from_str(&frame).unwrap();
        assert_eq!(v["type"], "dag_updated");

        // ... and board_updated on the project topic, so the epic card's
        // done/total progress badge refreshes live (done=1, total=1).
        let frame = proj_sub.recv().await.unwrap();
        let v: Value = serde_json::from_str(&frame).unwrap();
        assert_eq!(v["type"], "board_updated");
        assert_eq!(v["payload"]["epic_progress"][0]["epic_id"], epic_id);
        assert_eq!(v["payload"]["epic_progress"][0]["done"], 1);
        assert_eq!(v["payload"]["epic_progress"][0]["total"], 1);
    }

    #[tokio::test]
    async fn remove_task_deletes_and_cleans_its_edges() {
        let (state, app, _p, epic_id) = seed_app().await;
        let conn = state.db.conn();
        let a = create_task(conn, &epic_id, &_p, "A", None, None)
            .await
            .unwrap();
        let b = create_task(conn, &epic_id, &_p, "B", None, None)
            .await
            .unwrap();
        link_dependency(conn, &a.id, &b.id).await.unwrap();

        let mut sub = state.hub.subscribe(&format!("epic:{epic_id}"));

        let response = app
            .clone()
            .oneshot(req("DELETE", &format!("/tasks/{}", a.id), None))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NO_CONTENT);

        // The task and its edge are both gone.
        assert!(fetch_task(conn, &a.id).await.unwrap().is_none());
        assert!(list_dependencies_for_epic(conn, &epic_id)
            .await
            .unwrap()
            .is_empty());

        // A dag_updated frame fired.
        let frame = sub.recv().await.unwrap();
        let v: Value = serde_json::from_str(&frame).unwrap();
        assert_eq!(v["type"], "dag_updated");

        // Unknown task -> 404.
        let missing = app
            .oneshot(req("DELETE", "/tasks/nope", None))
            .await
            .unwrap();
        assert_eq!(missing.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn post_dependency_links_and_rejects_cycles_and_cross_epic() {
        let (state, app, _p, epic_id) = seed_app().await;
        let conn = state.db.conn();
        let a = create_task(conn, &epic_id, &_p, "A", None, None)
            .await
            .unwrap();
        let b = create_task(conn, &epic_id, &_p, "B", None, None)
            .await
            .unwrap();
        let c = create_task(conn, &epic_id, &_p, "C", None, None)
            .await
            .unwrap();

        // A -> B and B -> C are valid.
        for (blocker, blocked) in [(a.id.clone(), b.id.clone()), (b.id.clone(), c.id.clone())] {
            let r = app
                .clone()
                .oneshot(req(
                    "POST",
                    &format!("/epics/{epic_id}/dependencies"),
                    Some(json!({"blocker_id": blocker, "blocked_id": blocked})),
                ))
                .await
                .unwrap();
            assert_eq!(r.status(), StatusCode::CREATED);
        }

        // C -> A closes the cycle -> 409 conflict.
        let cycle = app
            .clone()
            .oneshot(req(
                "POST",
                &format!("/epics/{epic_id}/dependencies"),
                Some(json!({"blocker_id": c.id, "blocked_id": a.id})),
            ))
            .await
            .unwrap();
        assert_eq!(cycle.status(), StatusCode::CONFLICT);
        assert_eq!(body_json(cycle).await["error"]["code"], "conflict");

        // Cross-epic: a task from another epic can't be linked via this epic's path.
        let other_epic = ulid::Ulid::new().to_string();
        state
            .db
            .conn()
            .execute(
                "INSERT INTO epic (id, project_id, title, status, created_at, updated_at) \
                 VALUES (?1, ?2, 'E2', 'Ready', ?3, ?3)",
                params![other_epic.clone(), _p.clone(), now_ms()],
            )
            .await
            .unwrap();
        let x = create_task(state.db.conn(), &other_epic, &_p, "X", None, None)
            .await
            .unwrap();
        let cross = app
            .clone()
            .oneshot(req(
                "POST",
                &format!("/epics/{epic_id}/dependencies"),
                Some(json!({"blocker_id": a.id, "blocked_id": x.id})),
            ))
            .await
            .unwrap();
        assert_eq!(cross.status(), StatusCode::BAD_REQUEST);

        // Unknown epic -> 404.
        let missing = app
            .oneshot(req(
                "POST",
                "/epics/nope/dependencies",
                Some(json!({"blocker_id": a.id, "blocked_id": b.id})),
            ))
            .await
            .unwrap();
        assert_eq!(missing.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn remove_dependency_unlinks_and_404s_for_unknown_epic() {
        let (state, app, _p, epic_id) = seed_app().await;
        let conn = state.db.conn();
        let a = create_task(conn, &epic_id, &_p, "A", None, None)
            .await
            .unwrap();
        let b = create_task(conn, &epic_id, &_p, "B", None, None)
            .await
            .unwrap();
        link_dependency(conn, &a.id, &b.id).await.unwrap();

        let response = app
            .clone()
            .oneshot(req(
                "DELETE",
                &format!(
                    "/epics/{epic_id}/dependencies?blocker_id={}&blocked_id={}",
                    a.id, b.id
                ),
                None,
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NO_CONTENT);
        assert!(list_dependencies_for_epic(conn, &epic_id)
            .await
            .unwrap()
            .is_empty());

        // Unknown epic -> 404.
        let missing = app
            .oneshot(req(
                "DELETE",
                "/epics/nope/dependencies?blocker_id=x&blocked_id=y",
                None,
            ))
            .await
            .unwrap();
        assert_eq!(missing.status(), StatusCode::NOT_FOUND);
    }

    // ---- standalone (parentless) tasks ----

    /// T-500 AC: the Half-2 executor columns (`branch_name`, `pr_url`,
    /// `pr_number`) round-trip through `fetch_task` and its JSON alongside the
    /// pre-existing `failure_reason`, while the lease columns and `base_sha`
    /// (internal executor state, written directly by the worker) never appear
    /// on the DTO or in the API response.
    #[tokio::test]
    async fn task_executor_columns_round_trip_but_lease_and_base_sha_stay_internal() {
        let (state, app, project_id, _epic_id) = seed_app().await;
        let conn = state.db.conn();
        let t = create_standalone_task(conn, &project_id, "Small fix", None, None)
            .await
            .unwrap();

        // Write the new columns directly via SQL, the way the executor will
        // (T-551 persists branch_name/pr_url/pr_number; task.rs's own
        // failure_reason already existed pre-M2).
        conn.execute(
            "UPDATE task SET branch_name = ?1, pr_url = ?2, pr_number = ?3, \
                 failure_reason = ?4, lease_owner = ?5, lease_expires_at = ?6, \
                 base_sha = ?7 WHERE id = ?8",
            params![
                "dearborn/task-small-fix-abc123",
                "https://github.com/acme/demo/pull/7",
                7i64,
                "test_gate_exhausted",
                "worker-1",
                9_999_999_999i64,
                "deadbeef",
                t.id.clone()
            ],
        )
        .await
        .unwrap();

        let fetched = fetch_task(conn, &t.id).await.unwrap().expect("task exists");
        assert_eq!(
            fetched.branch_name.as_deref(),
            Some("dearborn/task-small-fix-abc123")
        );
        assert_eq!(
            fetched.pr_url.as_deref(),
            Some("https://github.com/acme/demo/pull/7")
        );
        assert_eq!(fetched.pr_number, Some(7));
        assert_eq!(
            fetched.failure_reason.as_deref(),
            Some("test_gate_exhausted")
        );

        // Same story through the HTTP response the client actually sees.
        let response = app
            .oneshot(req("GET", &format!("/tasks/{}", t.id), None))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = body_json(response).await;
        assert_eq!(body["branch_name"], "dearborn/task-small-fix-abc123");
        assert_eq!(body["pr_url"], "https://github.com/acme/demo/pull/7");
        assert_eq!(body["pr_number"], 7);
        assert_eq!(body["failure_reason"], "test_gate_exhausted");
        assert!(
            body.get("lease_owner").is_none(),
            "lease_owner must not be exposed"
        );
        assert!(
            body.get("lease_expires_at").is_none(),
            "lease_expires_at must not be exposed"
        );
        assert!(
            body.get("base_sha").is_none(),
            "base_sha must not be exposed"
        );
    }

    #[tokio::test]
    async fn create_standalone_task_round_trips_with_null_epic() {
        let (db, project_id, _e) = seed().await;
        let conn = db.conn();

        let t = create_standalone_task(conn, &project_id, "Small fix", Some("desc"), None)
            .await
            .unwrap();
        assert_eq!(t.title, "Small fix");
        assert_eq!(t.epic_id, None, "standalone => NULL epic");
        assert_eq!(t.project_id, project_id);
        assert_eq!(t.status, "Todo");
        assert_eq!(t.position, None, "no ordinal for standalone tasks");

        // Surfaces via list_standalone_tasks; excluded from any epic's list.
        let standalone = list_standalone_tasks(conn, &project_id).await.unwrap();
        assert_eq!(standalone.len(), 1);
        assert_eq!(standalone[0].id, t.id);
        assert!(list_tasks_for_epic(conn, &_e).await.unwrap().is_empty());

        // Empty title -> 400.
        let err = create_standalone_task(conn, &project_id, "  ", None, None)
            .await
            .unwrap_err();
        assert!(matches!(err, AppError::BadRequest(_)));
    }

    #[tokio::test]
    async fn link_dependency_rejects_standalone_tasks() {
        let (db, project_id, epic_id) = seed().await;
        let conn = db.conn();
        let a = create_task(conn, &epic_id, &project_id, "A", None, None)
            .await
            .unwrap();
        let s1 = create_standalone_task(conn, &project_id, "S1", None, None)
            .await
            .unwrap();
        let s2 = create_standalone_task(conn, &project_id, "S2", None, None)
            .await
            .unwrap();

        // standalone <-> epic-scoped: rejected 400 (not a misleading 404).
        let err = link_dependency(conn, &s1.id, &a.id).await.unwrap_err();
        assert!(
            matches!(err, AppError::BadRequest(_)),
            "standalone blocker rejected"
        );
        let err = link_dependency(conn, &a.id, &s1.id).await.unwrap_err();
        assert!(
            matches!(err, AppError::BadRequest(_)),
            "standalone blocked rejected"
        );

        // standalone <-> standalone: also rejected (and never links across
        // projects via the NULL == NULL hole).
        let err = link_dependency(conn, &s1.id, &s2.id).await.unwrap_err();
        assert!(
            matches!(err, AppError::BadRequest(_)),
            "standalone pair rejected"
        );
    }

    #[tokio::test]
    async fn create_project_task_endpoint_creates_and_publishes_board() {
        let (state, app, project_id, _e) = seed_app().await;

        let mut sub = state.hub.subscribe(&format!("project:{project_id}"));

        let response = app
            .clone()
            .oneshot(req(
                "POST",
                &format!("/projects/{project_id}/tasks"),
                Some(json!({"title":"Small fix","description":"d","acceptance":"a"})),
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::CREATED);
        let task = body_json(response).await;
        assert_eq!(task["title"], "Small fix");
        assert_eq!(task["epic_id"], Value::Null);
        assert_eq!(task["status"], "Todo");

        // A board_updated frame fired, carrying the new standalone task.
        let frame = sub.recv().await.unwrap();
        let v: Value = serde_json::from_str(&frame).unwrap();
        assert_eq!(v["type"], "board_updated");
        assert_eq!(v["payload"]["tasks"].as_array().unwrap().len(), 1);
        assert_eq!(v["payload"]["tasks"][0]["id"], task["id"]);

        // Missing title -> 400; unknown project -> 404.
        let bad = app
            .clone()
            .oneshot(req(
                "POST",
                &format!("/projects/{project_id}/tasks"),
                Some(json!({"title":"  "})),
            ))
            .await
            .unwrap();
        assert_eq!(bad.status(), StatusCode::BAD_REQUEST);
        let missing = app
            .oneshot(req(
                "POST",
                "/projects/nope/tasks",
                Some(json!({"title":"X"})),
            ))
            .await
            .unwrap();
        assert_eq!(missing.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn patch_and_delete_standalone_task_publish_board() {
        let (state, app, project_id, _e) = seed_app().await;
        let conn = state.db.conn();
        let t = create_standalone_task(conn, &project_id, "S", None, None)
            .await
            .unwrap();

        let mut sub = state.hub.subscribe(&format!("project:{project_id}"));

        // Patch a standalone task -> 200 + board_updated (not dag_updated).
        let response = app
            .clone()
            .oneshot(req(
                "PATCH",
                &format!("/tasks/{}", t.id),
                Some(json!({"title":"S2","status":"Done"})),
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let task = body_json(response).await;
        assert_eq!(task["title"], "S2");
        assert_eq!(task["status"], "Done");
        let frame = sub.recv().await.unwrap();
        let v: Value = serde_json::from_str(&frame).unwrap();
        assert_eq!(v["type"], "board_updated");
        assert_eq!(v["payload"]["tasks"][0]["title"], "S2");

        // Delete a standalone task -> 204 (regression: a NULL epic must not
        // be misread as "task not found") + board_updated.
        let response = app
            .clone()
            .oneshot(req("DELETE", &format!("/tasks/{}", t.id), None))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NO_CONTENT);
        assert!(fetch_task(conn, &t.id).await.unwrap().is_none());
        let frame = sub.recv().await.unwrap();
        let v: Value = serde_json::from_str(&frame).unwrap();
        assert_eq!(v["type"], "board_updated");
        assert!(v["payload"]["tasks"].as_array().unwrap().is_empty());
    }

    // ---- T-541: POST /tasks/{id}/retry ----
    //
    // These exercise the HTTP-level contract (status codes, response shape,
    // WS frames, notify) against directly-seeded rows — the full worker
    // recovery loop (a real failure via the test-gate, `retry`, a worker
    // re-claiming and re-attaching, editing the spec first) is covered in
    // `worker.rs`'s own `mod tests`, which has the fixture repos and scripted
    // agent this module intentionally does not depend on.

    /// Set a task's `status`/`failure_reason` directly (bypassing `PATCH`,
    /// which doesn't allow clearing `failure_reason` on its own) — mirrors
    /// `worker.rs`'s own `set_task_status` test helper.
    async fn set_task_status_and_reason(
        state: &AppState,
        task_id: &str,
        status: &str,
        reason: Option<&str>,
    ) {
        state
            .db
            .conn()
            .execute(
                "UPDATE task SET status = ?1, failure_reason = ?2 WHERE id = ?3",
                params![status, reason, task_id],
            )
            .await
            .unwrap();
    }

    /// Seed the epic `Blocked` with `reason` and a held (stale) lease, so a
    /// retry test can assert the unblock clears both the lane and the lease.
    async fn set_epic_blocked_with_stale_lease(state: &AppState, epic_id: &str, reason: &str) {
        state
            .db
            .conn()
            .execute(
                "UPDATE epic SET status = 'Blocked', blocked_reason = ?1, \
                     lease_owner = 'stale-worker', lease_expires_at = 9999999999999 \
                 WHERE id = ?2",
                params![reason, epic_id],
            )
            .await
            .unwrap();
    }

    /// `(status, blocked_reason, lease_owner, lease_expires_at)` read directly
    /// off the epic row (the lease columns are deliberately not on the `Epic`
    /// DTO — see `epics::EPIC_COLUMNS` — so a retry test needs raw SQL here).
    async fn epic_row(
        state: &AppState,
        epic_id: &str,
    ) -> (String, Option<String>, Option<String>, Option<i64>) {
        let mut rows = state
            .db
            .conn()
            .query(
                "SELECT status, blocked_reason, lease_owner, lease_expires_at FROM epic WHERE id = ?1",
                params![epic_id],
            )
            .await
            .unwrap();
        let row = rows.next().await.unwrap().unwrap();
        (
            row.get(0).unwrap(),
            row.get(1).unwrap(),
            row.get(2).unwrap(),
            row.get(3).unwrap(),
        )
    }

    #[tokio::test]
    async fn retry_task_endpoint_404s_for_unknown_task() {
        let (_state, app, _p, _e) = seed_app().await;
        let response = app
            .oneshot(req("POST", "/tasks/nope/retry", None))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
        assert_eq!(body_json(response).await["error"]["code"], "not_found");
    }

    /// AC: `409` for every status other than `Failed`.
    #[tokio::test]
    async fn retry_task_endpoint_409s_for_every_non_failed_status() {
        let (state, app, project_id, epic_id) = seed_app().await;
        let conn = state.db.conn();

        for status in ["Todo", "InProgress", "InReview", "Done", "Cancelled"] {
            let t = create_task(conn, &epic_id, &project_id, status, None, None)
                .await
                .unwrap();
            set_task_status_and_reason(&state, &t.id, status, None).await;

            let response = app
                .clone()
                .oneshot(req("POST", &format!("/tasks/{}/retry", t.id), None))
                .await
                .unwrap();
            assert_eq!(
                response.status(),
                StatusCode::CONFLICT,
                "status {status} must not be retryable"
            );
            assert_eq!(body_json(response).await["error"]["code"], "conflict");

            // Untouched — a rejected retry must not itself write anything.
            let fetched = fetch_task(conn, &t.id).await.unwrap().unwrap();
            assert_eq!(fetched.status, status);
        }
    }

    /// The headline AC: a `Failed` task under a `Blocked` epic — `200` +
    /// `Todo` + cleared `failure_reason`; the epic unblocks to `InProgress`
    /// with `blocked_reason` and the lease both cleared; `dag_updated` +
    /// `epic_updated` fire on `epic:<id>`, `board_updated` on `project:<id>`,
    /// and an idle worker's `notify` wakes.
    #[tokio::test]
    async fn retry_task_endpoint_moves_failed_task_to_todo_and_unblocks_blocked_epic() {
        let (state, app, project_id, epic_id) = seed_app().await;
        let conn = state.db.conn();
        let t = create_task(conn, &epic_id, &project_id, "A", None, None)
            .await
            .unwrap();
        set_task_status_and_reason(&state, &t.id, "Failed", Some("test_gate_exhausted")).await;
        set_epic_blocked_with_stale_lease(&state, &epic_id, "test_gate_exhausted").await;

        let mut epic_sub = state.hub.subscribe(&format!("epic:{epic_id}"));
        let mut proj_sub = state.hub.subscribe(&format!("project:{project_id}"));
        // Registered before the request, per the standard `Notify` proof
        // pattern (`lanes.rs`'s `ready_to_in_progress_clears_lease_...`):
        // a `notify_waiters()` call with no waiter registered yet is not
        // queued, so this only resolves if the handler itself calls it.
        let notified = state.notify.notified();

        let response = app
            .clone()
            .oneshot(req("POST", &format!("/tasks/{}/retry", t.id), None))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let task = body_json(response).await;
        assert_eq!(task["status"], "Todo");
        assert_eq!(task["failure_reason"], Value::Null);
        assert_eq!(task["id"], t.id);

        tokio::time::timeout(std::time::Duration::from_millis(500), notified)
            .await
            .expect("retry must call state.notify.notify_waiters()");

        let (status, blocked_reason, lease_owner, lease_expires_at) =
            epic_row(&state, &epic_id).await;
        assert_eq!(
            status, "InProgress",
            "the epic must return to the In Progress lane"
        );
        assert!(blocked_reason.is_none());
        assert!(lease_owner.is_none(), "the stale lease must be cleared");
        assert!(lease_expires_at.is_none());

        // dag_updated then epic_updated on epic:<id> ...
        let frame = epic_sub.recv().await.unwrap();
        assert_eq!(
            serde_json::from_str::<Value>(&frame).unwrap()["type"],
            "dag_updated"
        );
        let frame = epic_sub.recv().await.unwrap();
        let v: Value = serde_json::from_str(&frame).unwrap();
        assert_eq!(v["type"], "epic_updated");
        assert_eq!(v["payload"]["status"], "InProgress");
        assert_eq!(v["payload"]["blocked_reason"], Value::Null);

        // ... and board_updated on project:<id>, reflecting the lane move.
        let frame = proj_sub.recv().await.unwrap();
        let v: Value = serde_json::from_str(&frame).unwrap();
        assert_eq!(v["type"], "board_updated");
        assert_eq!(v["payload"]["epics"][0]["status"], "InProgress");
    }

    /// A standalone (`epic_id IS NULL`) task has no epic to unblock — and,
    /// per T-551's contract revision (this function's own doc, "A standalone
    /// task retries to InProgress, not Todo"), retrying it goes straight back
    /// to `InProgress` (not `Todo`) since the task itself is both the
    /// claimable item and the unit of work. Publishes `board_updated` (never
    /// `dag_updated`, matching every other standalone-task mutation).
    #[tokio::test]
    async fn retry_task_endpoint_standalone_task_returns_directly_to_in_progress() {
        let (state, app, project_id, _e) = seed_app().await;
        let conn = state.db.conn();
        let t = create_standalone_task(conn, &project_id, "Small fix", None, None)
            .await
            .unwrap();
        set_task_status_and_reason(&state, &t.id, "Failed", Some("agent_error")).await;

        let mut proj_sub = state.hub.subscribe(&format!("project:{project_id}"));

        let response = app
            .oneshot(req("POST", &format!("/tasks/{}/retry", t.id), None))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let task = body_json(response).await;
        assert_eq!(task["status"], "InProgress");
        assert_eq!(task["failure_reason"], Value::Null);
        assert_eq!(task["epic_id"], Value::Null);

        let frame = proj_sub.recv().await.unwrap();
        let v: Value = serde_json::from_str(&frame).unwrap();
        assert_eq!(v["type"], "board_updated");
        assert_eq!(v["payload"]["tasks"][0]["status"], "InProgress");
    }

    /// D11's "iff Blocked": a `Failed` task whose epic is in some other
    /// state (here, `Cancelled` mid-triage) still returns to `Todo`, but the
    /// epic itself is left completely untouched — retry must never resurrect
    /// an epic the user deliberately moved on from.
    #[tokio::test]
    async fn retry_task_endpoint_leaves_a_non_blocked_epic_untouched() {
        let (state, app, project_id, epic_id) = seed_app().await;
        let conn = state.db.conn();
        let t = create_task(conn, &epic_id, &project_id, "A", None, None)
            .await
            .unwrap();
        set_task_status_and_reason(&state, &t.id, "Failed", Some("cancelled")).await;
        state
            .db
            .conn()
            .execute(
                "UPDATE epic SET status = 'Cancelled' WHERE id = ?1",
                params![epic_id.clone()],
            )
            .await
            .unwrap();

        let response = app
            .oneshot(req("POST", &format!("/tasks/{}/retry", t.id), None))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(body_json(response).await["status"], "Todo");

        let (status, _, _, _) = epic_row(&state, &epic_id).await;
        assert_eq!(
            status, "Cancelled",
            "a non-Blocked epic must not be resurrected by retry"
        );
    }
}
