//! Project board (kanban) — epics + standalone tasks (T-401).
//!
//! The project board is the kanban view at the project level: the project's
//! epics (each in its lane: `Planning | Ready | InProgress | Completed |
//! Cancelled | Blocked`) plus its **standalone** (parentless, `epic_id IS NULL`)
//! tasks. It is read via `GET /projects/:id/board` and published live as a
//! `board_updated` frame on `project:<id>` whenever an epic lane changes
//! (T-401's `POST /epics/:id/lane`) or breakdown lands an epic in Ready.
//!
//! Follows the `epics.rs` / `tasks.rs` store style: the query helpers live in
//! those modules (`list_epics_by_project`, `list_standalone_tasks`) so the SQL
//! projection is not duplicated; this module is the thin board aggregation +
//! the REST/WS publish surface.

use axum::extract::{Path, State};
use axum::Json;
use libsql::Connection;
use serde::Serialize;
use serde_json::json;

use crate::epics::{list_epics_by_project, project_exists, Epic};
use crate::tasks::{list_standalone_tasks, Task};
use crate::{AppError, AppResult, AppState};

/// The project board: its epics (in lane order) and its standalone
/// (parentless) tasks. Epics go in the lane of their `status`; standalone tasks
/// are mapped to lanes client-side.
#[derive(Debug, Serialize)]
pub struct Board {
    pub epics: Vec<Epic>,
    pub tasks: Vec<Task>,
}

/// Load the board for `project_id`: its epics (newest first, same ordering as
/// `list_epics`) and its standalone tasks (`epic_id IS NULL`, newest first).
/// Does **not** check project existence — callers guard with [`project_exists`]
/// for a clean `404`.
pub(crate) async fn load_board(conn: &Connection, project_id: &str) -> AppResult<Board> {
    let epics = list_epics_by_project(conn, project_id).await?;
    let tasks = list_standalone_tasks(conn, project_id).await?;
    Ok(Board { epics, tasks })
}

/// Best-effort publish of the board on `project:<id>` as a `board_updated`
/// frame (payload `{ epics, tasks }`). A read error is logged and the publish
/// is skipped — mirrors `mcp::publish_dag`.
pub async fn publish_board(state: &AppState, project_id: &str) {
    let board = match load_board(state.db.conn(), project_id).await {
        Ok(board) => board,
        Err(err) => {
            tracing::warn!(
                project = %project_id,
                error = %err,
                "board publish: failed to load board"
            );
            return;
        }
    };
    state.hub.publish(
        &format!("project:{project_id}"),
        "board_updated",
        json!({ "epics": board.epics, "tasks": board.tasks }),
    );
}

/// `GET /projects/:id/board` — the project's kanban board. `404` if the project
/// does not exist.
pub async fn get_board(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> AppResult<Json<Board>> {
    let conn = state.db.conn();
    if !project_exists(conn, &id).await? {
        return Err(AppError::NotFound(format!("project {id} not found")));
    }
    let board = load_board(conn, &id).await?;
    Ok(Json(board))
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
    use serde_json::Value as Json;
    use tower::ServiceExt;

    const TOKEN: &str = "s3cret-token";

    async fn test_app() -> (AppState, axum::Router) {
        let db = Db::connect(":memory:").await.unwrap();
        db.run_migrations().await.unwrap();
        let state = AppState::with_planner(
            Config::for_test(TOKEN),
            db,
            std::sync::Arc::new(SilentPlanningAgent),
        );
        let app = app(state.clone());
        (state, app)
    }

    fn req(method: &str, uri: &str, body: Option<Json>) -> Request<Body> {
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

    async fn body_json(response: axum::response::Response) -> Json {
        let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        if bytes.is_empty() {
            return Json::Null;
        }
        serde_json::from_slice(&bytes).unwrap()
    }

    /// Seed a project directly and return its id.
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

    /// Seed an epic directly with a given status.
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

    /// Seed a task directly with a given epic_id (or NULL for standalone).
    async fn seed_task(
        state: &AppState,
        epic_id: Option<&str>,
        project_id: &str,
        title: &str,
        status: &str,
    ) -> String {
        let conn = state.db.conn();
        let id = ulid::Ulid::new().to_string();
        let now = now_ms();
        conn.execute(
            "INSERT INTO task (id, epic_id, project_id, title, status, created_at, updated_at) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?6)",
            params![id.clone(), epic_id, project_id, title, status, now],
        )
        .await
        .unwrap();
        id
    }

    use libsql::params;

    fn now_ms() -> i64 {
        use std::time::{SystemTime, UNIX_EPOCH};
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as i64)
            .unwrap_or(0)
    }

    #[tokio::test]
    async fn get_board_returns_epics_and_standalone_tasks() {
        let (state, app) = test_app().await;
        let project_id = seed_project(&state).await;
        let epic_id = seed_epic(&state, &project_id, "Ready").await;

        // An epic-scoped task (should NOT appear on the board).
        let epic_task = seed_task(&state, Some(&epic_id), &project_id, "Epic task", "Todo").await;
        // A standalone task (should appear on the board).
        let standalone = seed_task(&state, None, &project_id, "Standalone task", "Todo").await;

        let response = app
            .clone()
            .oneshot(req("GET", &format!("/projects/{project_id}/board"), None))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let board = body_json(response).await;

        // One epic.
        let epics = board["epics"].as_array().unwrap();
        assert_eq!(epics.len(), 1);
        assert_eq!(epics[0]["id"], epic_id);
        assert_eq!(epics[0]["status"], "Ready");

        // One standalone task — the epic-scoped task is excluded.
        let tasks = board["tasks"].as_array().unwrap();
        assert_eq!(tasks.len(), 1);
        assert_eq!(tasks[0]["id"], standalone);
        assert_eq!(tasks[0]["title"], "Standalone task");
        assert_eq!(tasks[0]["epic_id"], Json::Null);

        // The epic-scoped task is NOT in the board's tasks.
        let task_ids: Vec<&str> = tasks.iter().map(|t| t["id"].as_str().unwrap()).collect();
        assert!(!task_ids.contains(&epic_task.as_str()));
    }

    #[tokio::test]
    async fn get_board_empty_project_returns_empty_arrays() {
        let (state, app) = test_app().await;
        let project_id = seed_project(&state).await;

        let response = app
            .oneshot(req("GET", &format!("/projects/{project_id}/board"), None))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let board = body_json(response).await;
        assert!(board["epics"].as_array().unwrap().is_empty());
        assert!(board["tasks"].as_array().unwrap().is_empty());
    }

    #[tokio::test]
    async fn get_board_unknown_project_is_404() {
        let (_state, app) = test_app().await;
        let response = app
            .oneshot(req("GET", "/projects/does-not-exist/board", None))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
        assert_eq!(body_json(response).await["error"]["code"], "not_found");
    }

    #[tokio::test]
    async fn publish_board_fans_out_to_project_topic_subscribers() {
        let (state, _app) = test_app().await;
        let project_id = seed_project(&state).await;
        seed_epic(&state, &project_id, "Ready").await;
        seed_task(&state, None, &project_id, "Standalone", "Todo").await;

        let mut sub = state.hub.subscribe(&format!("project:{project_id}"));

        publish_board(&state, &project_id).await;

        let frame = sub.recv().await.unwrap();
        let v: Json = serde_json::from_str(&frame).unwrap();
        assert_eq!(v["topic"], format!("project:{project_id}"));
        assert_eq!(v["type"], "board_updated");
        assert_eq!(v["payload"]["epics"].as_array().unwrap().len(), 1);
        assert_eq!(v["payload"]["tasks"].as_array().unwrap().len(), 1);
    }
}
