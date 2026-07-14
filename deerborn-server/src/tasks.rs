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

/// Columns projected into a [`Task`] DTO, in schema (§2.2) order.
const TASK_COLUMNS: &str = "id, epic_id, project_id, title, description, acceptance, status, \
     failure_reason, agent_session_id, position, created_at, updated_at";

/// A task as returned by the store / API (`task`, §2.2).
///
/// `epic_id` is `Option` because the schema permits standalone (parentless)
/// tasks (`NULL => standalone`); breakdown always sets it. The Half-2 columns
/// (`failure_reason`, `agent_session_id`) round-trip as `null` in Half 1.
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
    pub agent_session_id: Option<String>,
    pub position: Option<i64>,
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
        return Err(AppError::BadRequest("`title` must not be empty".to_string()));
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
    Ok(task_epic(conn, task_id).await? == Some(epic_id.to_string()))
}

/// The permitted task lifecycle statuses (§2.2). Readiness is *computed* from
/// deps, so `Todo` is the only status a not-yet-ready task holds.
const VALID_STATUSES: &[&str] = &["Todo", "InProgress", "Done", "Failed", "Cancelled"];

/// Validate a status string against the §2.2 set, or `400 bad_request`.
fn validate_status(status: &str) -> AppResult<()> {
    if VALID_STATUSES.contains(&status) {
        Ok(())
    } else {
        Err(AppError::BadRequest(format!(
            "`status` must be one of Todo|InProgress|Done|Failed|Cancelled, got `{status}`"
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
            return Err(AppError::BadRequest("`title` must not be empty".to_string()));
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

use crate::epics::fetch_epic;
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
    let title = req.title.as_deref().map(str::trim).filter(|s| !s.is_empty()).ok_or_else(|| {
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
    Ok((StatusCode::CREATED, Json(task)))
}

/// `PATCH /tasks/{id}` — partial update (double-option for nullable fields).
/// `200` with the updated task; `404` if it does not exist. Publishes
/// `dag_updated` on the task's epic.
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
    Ok(Json(task))
}

/// `DELETE /tasks/{id}` — remove a task and its dependency edges. `204`;
/// `404` if it does not exist. Publishes `dag_updated` on the task's epic.
pub async fn remove_task(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> AppResult<StatusCode> {
    let conn = state.db.conn();
    let epic_id = task_epic(conn, &id)
        .await?
        .ok_or_else(|| AppError::NotFound(format!("task {id} not found")))?;
    delete_task(conn, &id).await?;
    crate::mcp::publish_dag(&state, &epic_id).await;
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
    let blocker_id = req.blocker_id.as_deref().map(str::trim).filter(|s| !s.is_empty()).ok_or_else(|| {
        AppError::BadRequest("`blocker_id` is required".to_string())
    })?;
    let blocked_id = req.blocked_id.as_deref().map(str::trim).filter(|s| !s.is_empty()).ok_or_else(|| {
        AppError::BadRequest("`blocked_id` is required".to_string())
    })?;

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

/// The `epic_id` of a task, or `None` if the task does not exist (or is
/// standalone with a NULL epic — treated as "no epic" for linking purposes).
async fn task_epic(conn: &Connection, task_id: &str) -> AppResult<Option<String>> {
    let mut rows = conn
        .query("SELECT epic_id FROM task WHERE id = ?1", params![task_id])
        .await?;
    match rows.next().await? {
        Some(row) => Ok(row.get::<Option<String>>(0)?),
        None => Ok(None),
    }
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
        agent_session_id: row.get(8)?,
        position: row.get(9)?,
        created_at: row.get(10)?,
        updated_at: row.get(11)?,
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

        let a = create_task(conn, &epic_id, &project_id, "First", Some("does X"), Some("X works"))
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
        let a = create_task(conn, &epic_id, &project_id, "A", None, None).await.unwrap();
        let b = create_task(conn, &epic_id, &project_id, "B", None, None).await.unwrap();

        link_dependency(conn, &a.id, &b.id).await.unwrap();
        // Duplicate link is a no-op (idempotent).
        link_dependency(conn, &a.id, &b.id).await.unwrap();

        let edges = list_dependencies_for_epic(conn, &epic_id).await.unwrap();
        assert_eq!(edges.len(), 1);
        assert_eq!(edges[0].blocker_id, a.id);
        assert_eq!(edges[0].blocked_id, b.id);

        // Unlink removes it.
        unlink_dependency(conn, &a.id, &b.id).await.unwrap();
        assert!(list_dependencies_for_epic(conn, &epic_id).await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn link_dependency_rejects_self_edge() {
        let (db, project_id, epic_id) = seed().await;
        let conn = db.conn();
        let a = create_task(conn, &epic_id, &project_id, "A", None, None).await.unwrap();
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
        let a = create_task(conn, &epic_id, &project_id, "A", None, None).await.unwrap();
        let x = create_task(conn, &other_epic, &project_id, "X", None, None).await.unwrap();

        let err = link_dependency(conn, &a.id, &x.id).await.unwrap_err();
        assert!(matches!(err, AppError::BadRequest(_)), "cross-epic link rejected");
    }

    #[tokio::test]
    async fn link_dependency_rejects_missing_task() {
        let (db, project_id, epic_id) = seed().await;
        let conn = db.conn();
        let a = create_task(conn, &epic_id, &project_id, "A", None, None).await.unwrap();
        let err = link_dependency(conn, &a.id, "does-not-exist").await.unwrap_err();
        assert!(matches!(err, AppError::NotFound(_)));
    }

    #[tokio::test]
    async fn link_dependency_rejects_cycles() {
        let (db, project_id, epic_id) = seed().await;
        let conn = db.conn();
        let a = create_task(conn, &epic_id, &project_id, "A", None, None).await.unwrap();
        let b = create_task(conn, &epic_id, &project_id, "B", None, None).await.unwrap();
        let c = create_task(conn, &epic_id, &project_id, "C", None, None).await.unwrap();

        // A -> B -> C is a valid chain.
        link_dependency(conn, &a.id, &b.id).await.unwrap();
        link_dependency(conn, &b.id, &c.id).await.unwrap();

        // C -> A would close the loop A->B->C->A: rejected as a conflict.
        let err = link_dependency(conn, &c.id, &a.id).await.unwrap_err();
        assert!(matches!(err, AppError::Conflict(_)), "cycle must be rejected, got {err:?}");

        // The rejected edge was not persisted.
        let edges = list_dependencies_for_epic(conn, &epic_id).await.unwrap();
        assert_eq!(edges.len(), 2);
    }

    #[tokio::test]
    async fn task_belongs_to_epic_is_accurate() {
        let (db, project_id, epic_id) = seed().await;
        let conn = db.conn();
        let a = create_task(conn, &epic_id, &project_id, "A", None, None).await.unwrap();
        assert!(task_belongs_to_epic(conn, &a.id, &epic_id).await.unwrap());
        assert!(!task_belongs_to_epic(conn, &a.id, "other-epic").await.unwrap());
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

    /// Boot an app over a freshly-seeded project + epic; return (state, app, ids).
    async fn seed_app() -> (AppState, axum::Router, String, String) {
        let db = Db::connect(":memory:").await.unwrap();
        db.run_migrations().await.unwrap();
        let state = AppState::with_agents(
            Config::for_test(TOKEN),
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
        let a = create_task(conn, &epic_id, &project_id, "A", None, None).await.unwrap();
        let b = create_task(conn, &epic_id, &project_id, "B", None, None).await.unwrap();
        let c = create_task(conn, &epic_id, &project_id, "C", None, None).await.unwrap();
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
        assert!(node(&b.id).ready, "B: Todo + only blocker A is Done -> ready");
        assert!(node(&b.id).blocked_by.is_empty());
        assert!(!node(&c.id).ready, "C still blocked by B (Todo)");
        assert_eq!(node(&c.id).blocked_by, vec![b.id.clone()]);

        // Mark B InProgress -> C stays blocked (B not Done), and B is not ready.
        update_task(conn, &b.id, None, None, None, Some("InProgress".to_string()))
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
        create_task(conn, &epic_id, &_p, "A", None, None).await.unwrap();

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
        let b = create_task(conn, &epic_id, &_p, "B", None, None).await.unwrap();

        let mut sub = state.hub.subscribe(&format!("epic:{epic_id}"));

        let response = app
            .clone()
            .oneshot(req(
                "POST",
                &format!("/epics/{epic_id}/tasks"),
                Some(json!({"title":"A","description":"slice","acceptance":"works","blocks":[b.id]})),
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
            .oneshot(req("POST", &format!("/epics/{epic_id}/tasks"), Some(json!({"title":"  "}))))
            .await
            .unwrap();
        assert_eq!(bad.status(), StatusCode::BAD_REQUEST);
        let missing = app
            .oneshot(req("POST", "/epics/nope/tasks", Some(json!({"title":"X"}))))
            .await
            .unwrap();
        assert_eq!(missing.status(), StatusCode::NOT_FOUND);
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
            .oneshot(req("PATCH", &format!("/tasks/{}", a.id), Some(json!({"status":"Weird"}))))
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
    async fn remove_task_deletes_and_cleans_its_edges() {
        let (state, app, _p, epic_id) = seed_app().await;
        let conn = state.db.conn();
        let a = create_task(conn, &epic_id, &_p, "A", None, None).await.unwrap();
        let b = create_task(conn, &epic_id, &_p, "B", None, None).await.unwrap();
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
        assert!(list_dependencies_for_epic(conn, &epic_id).await.unwrap().is_empty());

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
        let a = create_task(conn, &epic_id, &_p, "A", None, None).await.unwrap();
        let b = create_task(conn, &epic_id, &_p, "B", None, None).await.unwrap();
        let c = create_task(conn, &epic_id, &_p, "C", None, None).await.unwrap();

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
        let a = create_task(conn, &epic_id, &_p, "A", None, None).await.unwrap();
        let b = create_task(conn, &epic_id, &_p, "B", None, None).await.unwrap();
        link_dependency(conn, &a.id, &b.id).await.unwrap();

        let response = app
            .clone()
            .oneshot(req(
                "DELETE",
                &format!("/epics/{epic_id}/dependencies?blocker_id={}&blocked_id={}", a.id, b.id),
                None,
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NO_CONTENT);
        assert!(list_dependencies_for_epic(conn, &epic_id).await.unwrap().is_empty());

        // Unknown epic -> 404.
        let missing = app
            .oneshot(req("DELETE", "/epics/nope/dependencies?blocker_id=x&blocked_id=y", None))
            .await
            .unwrap();
        assert_eq!(missing.status(), StatusCode::NOT_FOUND);
    }
}
