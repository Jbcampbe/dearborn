//! Epics, planning-session lifecycle, and the durable transcript store (T-201).
//!
//! An **epic** is the unit of planning (MILESTONE_1 §2.2). Creating one lands it
//! in `status='Planning'` and *starts a planning session*: the epic row, its
//! `planning_session` row(s), and its `transcript_message` history all live in
//! libSQL, so a session is durable and resumable across a server restart with no
//! in-memory state.
//!
//! ## Transcript store
//!
//! Every user / agent / tool message is appended to `transcript_message` with a
//! **monotonic `seq` per epic**. [`append_message`] assigns the next seq inside
//! the single `INSERT` statement (`MAX(seq)+1` as a correlated subquery), which
//! libSQL executes atomically under its single writer — two concurrent appends
//! to one epic can never collide on `seq`. [`load_transcript`] reads an epic's
//! messages back in `seq` order. T-202 reuses [`append_message`] to persist the
//! agent's streamed reply and any tool calls on the same monotonic sequence.
//!
//! ## Planning-session resume
//!
//! `planning_session` holds the native harness `session_id` (nullable until
//! T-202's first run) keyed by `(epic_id, phase)`. [`set_harness_session_id`]
//! lets T-202 persist the resume handle; because it is durable, a restarted
//! server resumes the same agent session rather than starting over. Following
//! the wire contract in `CONVENTIONS.md`: single resources render directly,
//! collections wrap an `items` array, IDs are server-generated ULIDs, and all
//! `*_at` timestamps are unix milliseconds.

use axum::{
    extract::{Path, State},
    http::StatusCode,
    Json,
};
use libsql::{params, Connection, Row};
use serde::{Deserialize, Serialize};
use serde_json::json;

use crate::planning::config_for_phase;
use crate::{AppError, AppResult, AppState};

/// Columns projected into an [`Epic`] DTO. The Half-2 lease columns
/// (`lease_owner`, `lease_expires_at`, `branch_name`) are omitted — they are not
/// part of the planning-facing shape.
const EPIC_COLUMNS: &str = "id, project_id, title, product_context, technical_context, \
     status, created_at, updated_at";

/// Columns projected into a [`TranscriptMessage`] DTO, in schema (§2.2) order.
const MESSAGE_COLUMNS: &str = "id, epic_id, phase, role, content, seq, created_at";

/// The phase planning starts in; its `planning_session` row is created with the
/// epic. T-205 adds the `technical` phase when the user advances the epic.
const INITIAL_PHASE: &str = "product";

/// An epic as returned by the API. Lands in `status='Planning'` on create.
///
/// `product_context` / `technical_context` are `Option<String>` so a `NULL`
/// column round-trips as JSON `null` (maintained live by the planning agent in
/// T-203).
#[derive(Debug, Serialize)]
pub struct Epic {
    pub id: String,
    pub project_id: String,
    pub title: String,
    pub product_context: Option<String>,
    pub technical_context: Option<String>,
    pub status: String,
    pub created_at: i64,
    pub updated_at: i64,
}

/// A durable planning-transcript message (`transcript_message`, §2.2).
#[derive(Debug, Serialize)]
pub struct TranscriptMessage {
    pub id: String,
    pub epic_id: String,
    /// `product` | `technical`.
    pub phase: String,
    /// `user` | `agent` | `tool` | `system`.
    pub role: String,
    /// Text, or a serialized `RunEvent` (T-202).
    pub content: String,
    /// Monotonic per epic, starting at 1.
    pub seq: i64,
    pub created_at: i64,
}

/// `POST /projects/{id}/epics` body. `title` is required (validated in the
/// handler so a missing/empty field yields the standard `bad_request` envelope).
#[derive(Debug, Deserialize)]
pub struct CreateEpic {
    title: Option<String>,
}

/// `POST /epics/{id}/messages` body — append a `user` message to the transcript.
#[derive(Debug, Deserialize)]
pub struct AppendMessage {
    /// `product` | `technical` (validated).
    phase: Option<String>,
    content: Option<String>,
}

/// `POST /projects/{id}/epics` — create an epic and start its planning session.
///
/// Lands the epic in `status='Planning'` and creates the `product`-phase
/// `planning_session` row. `404` if the project does not exist; `400` if
/// `title` is missing/empty.
pub async fn create_epic(
    State(state): State<AppState>,
    Path(project_id): Path<String>,
    Json(req): Json<CreateEpic>,
) -> AppResult<(StatusCode, Json<Epic>)> {
    let title = require_field(req.title, "title")?;
    let conn = state.db.conn();

    // The project must exist (FK is declared but not enforced without
    // `PRAGMA foreign_keys`; check explicitly for a clean 404).
    if !project_exists(conn, &project_id).await? {
        return Err(AppError::NotFound(format!("project {project_id} not found")));
    }

    let id = ulid::Ulid::new().to_string();
    let now = now_ms();

    // `status` takes its schema default of 'Planning' by omission; the context
    // columns and Half-2 lease columns stay NULL.
    conn.execute(
        "INSERT INTO epic (id, project_id, title, created_at, updated_at) \
         VALUES (?1, ?2, ?3, ?4, ?5)",
        params![id.clone(), project_id, title, now, now],
    )
    .await?;

    // Starting planning = a session row for the initial (product) phase.
    conn.execute(
        "INSERT INTO planning_session (epic_id, phase, status, created_at, updated_at) \
         VALUES (?1, ?2, 'active', ?3, ?4)",
        params![id.clone(), INITIAL_PHASE, now, now],
    )
    .await?;

    let epic = fetch_epic(conn, &id)
        .await?
        .ok_or_else(|| AppError::Internal(format!("epic {id} vanished after insert")))?;
    Ok((StatusCode::CREATED, Json(epic)))
}

/// `GET /projects/{id}/epics` — list a project's epics, newest first. `404` if
/// the project does not exist.
pub async fn list_epics(
    State(state): State<AppState>,
    Path(project_id): Path<String>,
) -> AppResult<Json<serde_json::Value>> {
    let conn = state.db.conn();
    if !project_exists(conn, &project_id).await? {
        return Err(AppError::NotFound(format!("project {project_id} not found")));
    }

    let sql = format!(
        "SELECT {EPIC_COLUMNS} FROM epic WHERE project_id = ?1 \
         ORDER BY created_at DESC, id DESC"
    );
    let mut rows = conn.query(&sql, params![project_id]).await?;

    let mut items = Vec::new();
    while let Some(row) = rows.next().await? {
        items.push(row_to_epic(&row)?);
    }
    Ok(Json(json!({ "items": items })))
}

/// `GET /epics/{id}` — fetch one epic or `404`.
pub async fn get_epic(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> AppResult<Json<Epic>> {
    let epic = fetch_epic(state.db.conn(), &id)
        .await?
        .ok_or_else(|| epic_not_found(&id))?;
    Ok(Json(epic))
}

/// `POST /epics/{id}/messages` — append a `user` message to the transcript.
///
/// `201` with the stored message (including its assigned `seq`). `404` if the
/// epic does not exist; `400` on a missing/empty `content` or an invalid
/// `phase`. Agent/tool messages are appended by T-202 via [`append_message`].
pub async fn post_message(
    State(state): State<AppState>,
    Path(id): Path<String>,
    Json(req): Json<AppendMessage>,
) -> AppResult<(StatusCode, Json<TranscriptMessage>)> {
    let phase = require_field(req.phase, "phase")?;
    let content = require_field(req.content, "content")?;
    let conn = state.db.conn();

    if !epic_exists(conn, &id).await? {
        return Err(epic_not_found(&id));
    }

    let message = append_message(conn, &id, &phase, "user", &content).await?;

    // Trigger a planning agent run for this turn (T-202). The reply streams over
    // WS on `epic:<id>` and is persisted on completion — not returned inline. A
    // second trigger while a run is in flight for this epic is ignored: the user
    // message above is still stored, but no overlapping run starts (so `seq` and
    // native resume never interleave).
    if config_for_phase(&phase).is_some() {
        if let Some(guard) = state.try_acquire_run(&id) {
            crate::planning::spawn_run(state.clone(), id, phase, guard, content);
        }
    }

    Ok((StatusCode::CREATED, Json(message)))
}

/// `GET /epics/{id}/transcript` — the epic's messages in `seq` order. `404` if
/// the epic does not exist.
pub async fn get_transcript(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> AppResult<Json<serde_json::Value>> {
    let conn = state.db.conn();
    if !epic_exists(conn, &id).await? {
        return Err(epic_not_found(&id));
    }
    let items = load_transcript(conn, &id).await?;
    Ok(Json(json!({ "items": items })))
}

// ---- reusable store helpers (T-202 consumes these) ----------------------

/// Append one message to `transcript_message`, assigning the next monotonic
/// `seq` for `epic_id`.
///
/// The seq is computed as `MAX(seq)+1` for the epic **inside the single INSERT
/// statement**, so libSQL's single writer assigns it atomically: two concurrent
/// appends to the same epic serialize and can never produce a duplicate or a gap
/// in `seq`. Validates `phase ∈ {product, technical}` and
/// `role ∈ {user, agent, tool, system}`. Returns the stored message.
///
/// This is the shared write path — T-202 calls it to persist the agent's reply
/// and any tool-call events onto the same sequence.
pub async fn append_message(
    conn: &Connection,
    epic_id: &str,
    phase: &str,
    role: &str,
    content: &str,
) -> AppResult<TranscriptMessage> {
    validate_phase(phase)?;
    validate_role(role)?;

    let id = ulid::Ulid::new().to_string();
    let now = now_ms();

    conn.execute(
        "INSERT INTO transcript_message (id, epic_id, phase, role, content, seq, created_at) \
         VALUES (?1, ?2, ?3, ?4, ?5, \
             (SELECT COALESCE(MAX(seq), 0) + 1 FROM transcript_message WHERE epic_id = ?2), \
             ?6)",
        params![id.clone(), epic_id, phase, role, content, now],
    )
    .await?;

    // Read the row back so the caller gets the DB-assigned seq.
    fetch_message(conn, &id)
        .await?
        .ok_or_else(|| AppError::Internal(format!("message {id} vanished after insert")))
}

/// Load an epic's full transcript, ordered by `seq` ascending.
pub async fn load_transcript(
    conn: &Connection,
    epic_id: &str,
) -> AppResult<Vec<TranscriptMessage>> {
    let sql = format!(
        "SELECT {MESSAGE_COLUMNS} FROM transcript_message WHERE epic_id = ?1 ORDER BY seq ASC"
    );
    let mut rows = conn.query(&sql, params![epic_id]).await?;
    let mut items = Vec::new();
    while let Some(row) = rows.next().await? {
        items.push(row_to_message(&row)?);
    }
    Ok(items)
}

/// Persist the native harness `session_id` for an epic's planning phase (T-202).
///
/// Durable resume handle: on the next turn a restarted server resumes this
/// session instead of starting a new conversation. `404` if no session row
/// exists for `(epic_id, phase)`.
pub async fn set_harness_session_id(
    conn: &Connection,
    epic_id: &str,
    phase: &str,
    harness_session_id: &str,
) -> AppResult<()> {
    validate_phase(phase)?;
    let affected = conn
        .execute(
            "UPDATE planning_session SET harness_session_id = ?1, updated_at = ?2 \
             WHERE epic_id = ?3 AND phase = ?4",
            params![harness_session_id, now_ms(), epic_id, phase],
        )
        .await?;
    if affected == 0 {
        return Err(AppError::NotFound(format!(
            "planning session ({epic_id}, {phase}) not found"
        )));
    }
    Ok(())
}

/// Read the stored native harness `session_id` for an epic's planning phase, if
/// one has been captured yet (T-202 passes it back as `RunRequest.resume`).
///
/// `Ok(None)` when the session exists but has no id yet, or when no session row
/// exists for `(epic_id, phase)` — callers treat "no resume id" uniformly.
pub async fn get_harness_session_id(
    conn: &Connection,
    epic_id: &str,
    phase: &str,
) -> AppResult<Option<String>> {
    let mut rows = conn
        .query(
            "SELECT harness_session_id FROM planning_session \
             WHERE epic_id = ?1 AND phase = ?2",
            params![epic_id, phase],
        )
        .await?;
    match rows.next().await? {
        Some(row) => Ok(row.get::<Option<String>>(0)?),
        None => Ok(None),
    }
}

// ---- row / value plumbing ----------------------------------------------

async fn fetch_epic(conn: &Connection, id: &str) -> AppResult<Option<Epic>> {
    let sql = format!("SELECT {EPIC_COLUMNS} FROM epic WHERE id = ?1");
    let mut rows = conn.query(&sql, params![id]).await?;
    match rows.next().await? {
        Some(row) => Ok(Some(row_to_epic(&row)?)),
        None => Ok(None),
    }
}

async fn fetch_message(conn: &Connection, id: &str) -> AppResult<Option<TranscriptMessage>> {
    let sql = format!("SELECT {MESSAGE_COLUMNS} FROM transcript_message WHERE id = ?1");
    let mut rows = conn.query(&sql, params![id]).await?;
    match rows.next().await? {
        Some(row) => Ok(Some(row_to_message(&row)?)),
        None => Ok(None),
    }
}

fn row_to_epic(row: &Row) -> Result<Epic, libsql::Error> {
    Ok(Epic {
        id: row.get(0)?,
        project_id: row.get(1)?,
        title: row.get(2)?,
        product_context: row.get(3)?,
        technical_context: row.get(4)?,
        status: row.get(5)?,
        created_at: row.get(6)?,
        updated_at: row.get(7)?,
    })
}

fn row_to_message(row: &Row) -> Result<TranscriptMessage, libsql::Error> {
    Ok(TranscriptMessage {
        id: row.get(0)?,
        epic_id: row.get(1)?,
        phase: row.get(2)?,
        role: row.get(3)?,
        content: row.get(4)?,
        seq: row.get(5)?,
        created_at: row.get(6)?,
    })
}

async fn project_exists(conn: &Connection, project_id: &str) -> AppResult<bool> {
    let mut rows = conn
        .query("SELECT 1 FROM project WHERE id = ?1", params![project_id])
        .await?;
    Ok(rows.next().await?.is_some())
}

async fn epic_exists(conn: &Connection, epic_id: &str) -> AppResult<bool> {
    let mut rows = conn
        .query("SELECT 1 FROM epic WHERE id = ?1", params![epic_id])
        .await?;
    Ok(rows.next().await?.is_some())
}

/// `product` | `technical` — the two planning phases (§2.2).
fn validate_phase(phase: &str) -> AppResult<()> {
    match phase {
        "product" | "technical" => Ok(()),
        other => Err(AppError::BadRequest(format!(
            "`phase` must be `product` or `technical`, got `{other}`"
        ))),
    }
}

/// `user` | `agent` | `tool` | `system` — the transcript roles (§2.2).
fn validate_role(role: &str) -> AppResult<()> {
    match role {
        "user" | "agent" | "tool" | "system" => Ok(()),
        other => Err(AppError::BadRequest(format!(
            "`role` must be one of user|agent|tool|system, got `{other}`"
        ))),
    }
}

/// Require a present, non-empty (after trim) string field, or `400 bad_request`.
fn require_field(value: Option<String>, field: &str) -> AppResult<String> {
    match value {
        Some(v) if !v.trim().is_empty() => Ok(v.trim().to_string()),
        Some(_) => Err(AppError::BadRequest(format!("`{field}` must not be empty"))),
        None => Err(AppError::BadRequest(format!("`{field}` is required"))),
    }
}

fn epic_not_found(id: &str) -> AppError {
    AppError::NotFound(format!("epic {id} not found"))
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
    use crate::{app, Config, Db};
    use axum::body::Body;
    use axum::http::{
        header::{AUTHORIZATION, CONTENT_TYPE},
        Request, StatusCode,
    };
    use serde_json::Value as Json;
    use tower::ServiceExt; // for `oneshot`

    const TOKEN: &str = "s3cret-token";

    async fn test_app() -> axum::Router {
        let db = Db::connect(":memory:").await.unwrap();
        db.run_migrations().await.unwrap();
        // These tests exercise the pure transcript store, so inject the silent
        // planner: message triggers still fire a run, but it streams and persists
        // nothing (T-202's run behaviour is tested in `crate::planning`).
        app(AppState::with_planner(
            Config::for_test(TOKEN),
            db,
            std::sync::Arc::new(crate::planning::testing::SilentPlanningAgent),
        ))
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

    /// Create a project directly in the db and return its id.
    async fn seed_project(app: &axum::Router) -> String {
        let created = app
            .clone()
            .oneshot(req(
                "POST",
                "/projects",
                Some(json!({ "name": "P", "repo_url": "https://example.com/p.git" })),
            ))
            .await
            .unwrap();
        assert_eq!(created.status(), StatusCode::CREATED);
        body_json(created).await["id"].as_str().unwrap().to_string()
    }

    async fn create_epic_via_api(app: &axum::Router, project_id: &str, title: &str) -> Json {
        let created = app
            .clone()
            .oneshot(req(
                "POST",
                &format!("/projects/{project_id}/epics"),
                Some(json!({ "title": title })),
            ))
            .await
            .unwrap();
        assert_eq!(created.status(), StatusCode::CREATED);
        body_json(created).await
    }

    #[tokio::test]
    async fn create_epic_lands_in_planning_and_round_trips() {
        let app = test_app().await;
        let project_id = seed_project(&app).await;

        let created = create_epic_via_api(&app, &project_id, "Ship it").await;
        let id = created["id"].as_str().unwrap().to_string();
        assert_eq!(created["status"], "Planning");
        assert_eq!(created["title"], "Ship it");
        assert_eq!(created["project_id"], project_id);
        assert_eq!(created["product_context"], Json::Null);

        // GET one -> equal to the created resource.
        let got = app
            .clone()
            .oneshot(req("GET", &format!("/epics/{id}"), None))
            .await
            .unwrap();
        assert_eq!(got.status(), StatusCode::OK);
        assert_eq!(body_json(got).await, created);

        // LIST by project contains it.
        let listed = app
            .clone()
            .oneshot(req("GET", &format!("/projects/{project_id}/epics"), None))
            .await
            .unwrap();
        assert_eq!(listed.status(), StatusCode::OK);
        let listed = body_json(listed).await;
        let items = listed["items"].as_array().unwrap();
        assert_eq!(items.len(), 1);
        assert_eq!(items[0]["id"], id);
    }

    #[tokio::test]
    async fn create_epic_starts_a_product_planning_session() {
        let db = Db::connect(":memory:").await.unwrap();
        db.run_migrations().await.unwrap();
        let state = AppState::new(Config::for_test(TOKEN), db);
        let app = app(state.clone());
        let project_id = seed_project(&app).await;
        let epic = create_epic_via_api(&app, &project_id, "E").await;
        let id = epic["id"].as_str().unwrap().to_string();

        // The starting (product) planning session exists, active, with no resume
        // id yet (T-202 populates it).
        let mut rows = state
            .db
            .conn()
            .query(
                "SELECT status, harness_session_id FROM planning_session \
                 WHERE epic_id = ?1 AND phase = 'product'",
                params![id],
            )
            .await
            .unwrap();
        let row = rows.next().await.unwrap().expect("product session row");
        assert_eq!(row.get::<String>(0).unwrap(), "active");
        assert_eq!(row.get::<Option<String>>(1).unwrap(), None);
    }

    #[tokio::test]
    async fn create_epic_missing_title_is_structured_bad_request() {
        let app = test_app().await;
        let project_id = seed_project(&app).await;
        let response = app
            .oneshot(req(
                "POST",
                &format!("/projects/{project_id}/epics"),
                Some(json!({ "title": "   " })),
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        assert_eq!(body_json(response).await["error"]["code"], "bad_request");
    }

    #[tokio::test]
    async fn create_epic_on_unknown_project_is_404() {
        let app = test_app().await;
        let response = app
            .oneshot(req(
                "POST",
                "/projects/does-not-exist/epics",
                Some(json!({ "title": "E" })),
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
        assert_eq!(body_json(response).await["error"]["code"], "not_found");
    }

    #[tokio::test]
    async fn messages_persist_and_reload_in_seq_order() {
        let app = test_app().await;
        let project_id = seed_project(&app).await;
        let id = create_epic_via_api(&app, &project_id, "E").await["id"]
            .as_str()
            .unwrap()
            .to_string();

        // Append N user messages; each returns the next seq with no gaps/dupes.
        const N: i64 = 12;
        for i in 1..=N {
            let posted = app
                .clone()
                .oneshot(req(
                    "POST",
                    &format!("/epics/{id}/messages"),
                    Some(json!({ "phase": "product", "content": format!("msg {i}") })),
                ))
                .await
                .unwrap();
            assert_eq!(posted.status(), StatusCode::CREATED);
            let posted = body_json(posted).await;
            assert_eq!(posted["seq"].as_i64().unwrap(), i, "seq must be monotonic");
            assert_eq!(posted["role"], "user");
        }

        // Transcript reloads in seq order, 1..N, no gaps or dupes.
        let transcript = app
            .clone()
            .oneshot(req("GET", &format!("/epics/{id}/transcript"), None))
            .await
            .unwrap();
        assert_eq!(transcript.status(), StatusCode::OK);
        let items = body_json(transcript).await["items"].as_array().unwrap().clone();
        assert_eq!(items.len(), N as usize);
        for (idx, item) in items.iter().enumerate() {
            assert_eq!(item["seq"].as_i64().unwrap(), idx as i64 + 1);
            assert_eq!(item["content"], format!("msg {}", idx + 1));
        }
    }

    #[tokio::test]
    async fn concurrent_appends_to_one_epic_get_unique_seqs() {
        let db = Db::connect(":memory:").await.unwrap();
        db.run_migrations().await.unwrap();
        let state = AppState::new(Config::for_test(TOKEN), db);
        let app = app(state.clone());
        let project_id = seed_project(&app).await;
        let id = create_epic_via_api(&app, &project_id, "E").await["id"]
            .as_str()
            .unwrap()
            .to_string();

        // Fire many appends concurrently against the shared connection; the
        // single-INSERT MAX(seq)+1 must still hand out a contiguous 1..N set.
        const N: usize = 25;
        let mut handles = Vec::new();
        for i in 0..N {
            let conn = state.db.conn().clone();
            let epic_id = id.clone();
            handles.push(tokio::spawn(async move {
                append_message(&conn, &epic_id, "product", "agent", &format!("c{i}"))
                    .await
                    .unwrap()
                    .seq
            }));
        }
        let mut seqs = Vec::new();
        for h in handles {
            seqs.push(h.await.unwrap());
        }
        seqs.sort_unstable();
        let expected: Vec<i64> = (1..=N as i64).collect();
        assert_eq!(seqs, expected, "seqs must be a contiguous 1..N with no dupes");
    }

    #[tokio::test]
    async fn post_message_validates_phase_and_content() {
        let app = test_app().await;
        let project_id = seed_project(&app).await;
        let id = create_epic_via_api(&app, &project_id, "E").await["id"]
            .as_str()
            .unwrap()
            .to_string();

        // Bad phase -> 400.
        let bad_phase = app
            .clone()
            .oneshot(req(
                "POST",
                &format!("/epics/{id}/messages"),
                Some(json!({ "phase": "marketing", "content": "hi" })),
            ))
            .await
            .unwrap();
        assert_eq!(bad_phase.status(), StatusCode::BAD_REQUEST);

        // Empty content -> 400.
        let empty = app
            .clone()
            .oneshot(req(
                "POST",
                &format!("/epics/{id}/messages"),
                Some(json!({ "phase": "product", "content": "" })),
            ))
            .await
            .unwrap();
        assert_eq!(empty.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn message_on_unknown_epic_is_404() {
        let app = test_app().await;
        let response = app
            .oneshot(req(
                "POST",
                "/epics/nope/messages",
                Some(json!({ "phase": "product", "content": "hi" })),
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn transcript_on_unknown_epic_is_404() {
        let app = test_app().await;
        let response = app
            .oneshot(req("GET", "/epics/nope/transcript", None))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    /// Resumable after a server restart: append messages, then open a *fresh*
    /// `Db` against the *same file* (simulating a process restart) and read the
    /// transcript back in full, in order. Uses a temp file DB, since `:memory:`
    /// does not persist across connections.
    #[tokio::test]
    async fn transcript_survives_a_server_restart() {
        let path = std::env::temp_dir().join(format!(
            "deerborn-t201-restart-{}-{}.db",
            std::process::id(),
            now_ms()
        ));
        let path_str = path.to_str().unwrap().to_string();

        let epic_id;
        // ---- first "process": create the epic and append 5 messages. ----
        {
            let db = Db::connect(&path_str).await.unwrap();
            db.run_migrations().await.unwrap();
            let state = AppState::new(Config::for_test(TOKEN), db);
            let app = app(state.clone());
            let project_id = seed_project(&app).await;
            epic_id = create_epic_via_api(&app, &project_id, "Durable").await["id"]
                .as_str()
                .unwrap()
                .to_string();
            for i in 1..=5 {
                append_message(
                    state.db.conn(),
                    &epic_id,
                    "product",
                    if i % 2 == 0 { "agent" } else { "user" },
                    &format!("turn {i}"),
                )
                .await
                .unwrap();
            }
            // Also stash a harness resume id, as T-202 would.
            set_harness_session_id(state.db.conn(), &epic_id, "product", "sess-abc")
                .await
                .unwrap();
        }

        // ---- second "process": a fresh Db on the same file reads it all back. ----
        {
            let db = Db::connect(&path_str).await.unwrap();
            // Re-running migrations is a no-op on the existing file.
            assert_eq!(db.run_migrations().await.unwrap(), 0);
            let conn = db.conn();

            let transcript = load_transcript(conn, &epic_id).await.unwrap();
            assert_eq!(transcript.len(), 5, "all messages persisted across restart");
            for (idx, m) in transcript.iter().enumerate() {
                assert_eq!(m.seq, idx as i64 + 1, "seq order preserved");
                assert_eq!(m.content, format!("turn {}", idx + 1));
            }

            // The durable resume handle survived, so a restart resumes the session.
            let mut rows = conn
                .query(
                    "SELECT harness_session_id FROM planning_session \
                     WHERE epic_id = ?1 AND phase = 'product'",
                    params![epic_id.clone()],
                )
                .await
                .unwrap();
            let row = rows.next().await.unwrap().unwrap();
            assert_eq!(row.get::<Option<String>>(0).unwrap().as_deref(), Some("sess-abc"));
        }

        for suffix in ["", "-shm", "-wal"] {
            let _ = std::fs::remove_file(format!("{path_str}{suffix}"));
        }
    }
}
