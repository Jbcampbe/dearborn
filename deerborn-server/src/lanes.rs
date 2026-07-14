//! Epic lane transitions (T-401).
//!
//! Epics move between lanes (`Planning | Ready | InProgress | Completed |
//! Cancelled | Blocked`) via `POST /epics/:id/lane`. Not every transition is
//! permitted: breakdown owns `Planning → Ready`, and the stub worker (T-403)
//! will own `InProgress → Completed`. This module encodes the permitted
//! transition table and rejects everything else as `409 conflict`, so the
//! kanban's lane-move control can never put an epic in an illegal state.
//!
//! On a successful transition the updated epic is published as `epic_updated`
//! on `epic:<id>` (so a subscribed planning/DAG view re-renders) and the board
//! is published as `board_updated` on `project:<id>` (so the kanban re-renders).

use axum::extract::{Path, State};
use axum::Json;
use libsql::params;
use serde::Deserialize;

use crate::board;
use crate::epics::{fetch_epic, Epic};
use crate::{AppError, AppResult, AppState};

/// The epic lane set (§2.2 stored values — no spaces: `InProgress`/`Completed`).
const VALID_LANES: &[&str] = &[
    "Planning",
    "Ready",
    "InProgress",
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
            "`status` must be one of Planning|Ready|InProgress|Completed|Cancelled|Blocked, got `{lane}`"
        )))
    }
}

/// Whether `current → target` is a permitted lane transition. The table:
///
/// - `Planning → Cancelled`
/// - `Ready → InProgress, Cancelled`
/// - `InProgress → Cancelled, Blocked`
/// - `Blocked → Ready, Cancelled`
/// - `Completed → (none)` — terminal
/// - `Cancelled → (none)` — terminal
///
/// `Planning → Ready` is owned by breakdown; `InProgress → Completed` will be
/// owned by the stub worker (T-403). Both are rejected here.
fn transition_permitted(current: &str, target: &str) -> bool {
    match current {
        "Planning" => target == "Cancelled",
        "Ready" => target == "InProgress" || target == "Cancelled",
        "InProgress" => target == "Cancelled" || target == "Blocked",
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

    const TOKEN: &str = "s3cret-token";

    async fn test_app() -> (AppState, axum::Router) {
        let db = Db::connect(":memory:").await.unwrap();
        db.run_migrations().await.unwrap();
        let state = AppState::with_planner(
            Config::for_test(TOKEN),
            db,
            Arc::new(SilentPlanningAgent),
        );
        let app = app(state.clone());
        (state, app)
    }

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
