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

use libsql::{params, Connection, Row};
use serde::Serialize;
use std::collections::HashSet;

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
}
