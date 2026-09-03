//! Threaded, anchored comments (wayfinder epic §4.8/§9, Phase 5).
//!
//! A **comment** hangs off an **anchor** — a map node or a living-Document
//! section — and lives in a **thread** (`thread_id`): the first post of a
//! thread chooses the anchor, every reply (human *or* agent) joins by
//! `thread_id` and inherits it. Attribution follows the flat-permission
//! model: a signed-in human's comment carries `author_user_id`; an agent
//! run posting through its capability token carries `author_user_id = NULL`
//! and `is_agent = 1`. The `resolved` flag is **thread-wide**: resolving any
//! comment in a thread resolves the conversation (the per-row column keeps
//! the store dumb; the handler applies the flag to the whole thread).
//!
//! Promotion of a thread into a frontier node is a separate surface (it
//! stamps `promoted_node_id`) and is NOT implemented here.
//!
//! The REST surface is exactly what the `dearborn` CLI's `comment
//! post|list|resolve` verbs call, so it is on the capability-token
//! allow-list (`crate::capability::authorize_cap_request`) and accepts either
//! a browser session token or a per-run capability token. Every mutation
//! publishes a `comments_updated` frame on `epic:<id>` carrying the epic's
//! full comment list, so a subscribed client re-renders — the same
//! best-effort pattern as `map_updated` / `document_updated`.

use std::collections::HashMap;

use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    Json,
};
use libsql::{params, Value};
use serde::{Deserialize, Serialize};

use crate::{map::Actor, AppError, AppResult, AppState};

/// The anchor vocabulary: a comment hangs off a map node or a Document
/// section (`document_section.section_id`).
pub(crate) const VALID_ANCHOR_KINDS: &[&str] = &["node", "section"];

const COMMENT_COLUMNS: &str = "id, epic_id, thread_id, anchor_kind, anchor_id, \
     author_user_id, is_agent, body, resolved, promoted_node_id, created_at";

// ---- DTOs ------------------------------------------------------------------

/// A comment as stored. `author_user_id` is `NULL` and `is_agent` is `true`
/// when the author was an agent run (a capability token).
#[derive(Debug, Clone, Serialize)]
pub struct Comment {
    pub id: String,
    pub epic_id: String,
    pub thread_id: String,
    pub anchor_kind: String,
    pub anchor_id: String,
    pub author_user_id: Option<String>,
    pub is_agent: bool,
    pub body: String,
    pub resolved: bool,
    pub promoted_node_id: Option<String>,
    pub created_at: i64,
}

/// `POST /epics/{id}/comments` body: `{ anchor_kind, anchor_id, body,
/// thread_id? }`. When `thread_id` is present the comment JOINS that thread
/// (an agent reply's shape) and the anchor fields are optional — absent, they
/// are inherited from the thread; present, they must match the thread's
/// anchor. Without `thread_id` the comment starts a new thread under the
/// required anchor.
#[derive(Debug, Deserialize)]
pub struct PostCommentBody {
    anchor_kind: Option<String>,
    anchor_id: Option<String>,
    body: Option<String>,
    thread_id: Option<String>,
}

/// `GET /epics/{id}/comments` query filters. All optional; every filter
/// supplied must match.
#[derive(Debug, Default, Deserialize)]
pub struct ListCommentsQuery {
    anchor_kind: Option<String>,
    anchor_id: Option<String>,
    thread_id: Option<String>,
}

fn row_to_comment(row: &libsql::Row) -> AppResult<Comment> {
    Ok(Comment {
        id: row.get(0)?,
        epic_id: row.get(1)?,
        thread_id: row.get(2)?,
        anchor_kind: row.get(3)?,
        anchor_id: row.get(4)?,
        author_user_id: row.get(5)?,
        is_agent: row.get::<i64>(6)? != 0,
        body: row.get(7)?,
        resolved: row.get::<i64>(8)? != 0,
        promoted_node_id: row.get(9)?,
        created_at: row.get(10)?,
    })
}

// ---- store -----------------------------------------------------------------

/// Insert one comment with a pre-resolved thread id; the pure write. All
/// validation (anchor existence, thread membership, non-empty body) is the
/// caller's job.
pub async fn insert_comment(
    conn: &libsql::Connection,
    epic_id: &str,
    thread_id: &str,
    anchor_kind: &str,
    anchor_id: &str,
    author_user_id: Option<&str>,
    is_agent: bool,
    body: &str,
) -> AppResult<Comment> {
    let id = ulid::Ulid::new().to_string();
    let now = crate::capability::now_ms();

    conn.execute(
        "INSERT INTO comment \
             (id, epic_id, thread_id, anchor_kind, anchor_id, author_user_id, \
              is_agent, body, resolved, promoted_node_id, created_at) \
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, 0, NULL, ?9)",
        params![
            id.clone(),
            epic_id,
            thread_id,
            anchor_kind,
            anchor_id,
            author_user_id,
            is_agent as i64,
            body,
            now
        ],
    )
    .await?;

    fetch_comment(conn, &id)
        .await?
        .ok_or_else(|| AppError::Internal(format!("comment {id} vanished after insert")))
}

/// Fetch one comment by id, or `None`.
pub async fn fetch_comment(conn: &libsql::Connection, id: &str) -> AppResult<Option<Comment>> {
    let sql = format!("SELECT {COMMENT_COLUMNS} FROM comment WHERE id = ?1");
    let mut rows = conn.query(&sql, params![id]).await?;
    match rows.next().await? {
        Some(row) => Ok(Some(row_to_comment(&row)?)),
        None => Ok(None),
    }
}

/// A comment's thread members, oldest first (then id for stability).
pub async fn list_thread(
    conn: &libsql::Connection,
    epic_id: &str,
    thread_id: &str,
) -> AppResult<Vec<Comment>> {
    let sql = format!(
        "SELECT {COMMENT_COLUMNS} FROM comment \
         WHERE epic_id = ?1 AND thread_id = ?2 ORDER BY created_at ASC, id ASC"
    );
    let mut rows = conn.query(&sql, params![epic_id, thread_id]).await?;
    let mut items = Vec::new();
    while let Some(row) = rows.next().await? {
        items.push(row_to_comment(&row)?);
    }
    Ok(items)
}

/// The epic's comments, oldest first (then id for stability), optionally
/// narrowed by anchor or thread. Every filter must match.
pub async fn list_comments(
    conn: &libsql::Connection,
    epic_id: &str,
    filter: &ListCommentsQuery,
) -> AppResult<Vec<Comment>> {
    let mut sql = format!(
        "SELECT {COMMENT_COLUMNS} FROM comment WHERE epic_id = ?1"
    );
    let mut values: Vec<Value> = vec![Value::Text(epic_id.to_string())];
    for (column, field) in [
        ("anchor_kind", &filter.anchor_kind),
        ("anchor_id", &filter.anchor_id),
        ("thread_id", &filter.thread_id),
    ] {
        if let Some(value) = field {
            sql.push_str(&format!(" AND {column} = ?"));
            values.push(Value::Text(value.clone()));
        }
    }
    sql.push_str(" ORDER BY created_at ASC, id ASC");

    let mut rows = conn.query(&sql, values).await?;
    let mut items = Vec::new();
    while let Some(row) = rows.next().await? {
        items.push(row_to_comment(&row)?);
    }
    Ok(items)
}

/// Set `resolved` on every comment of `comment`'s thread (thread-level
/// resolution, §9). Returns the thread as it now stands. `404` if the
/// comment does not exist or belongs to another epic.
pub async fn set_thread_resolved(
    conn: &libsql::Connection,
    epic_id: &str,
    comment_id: &str,
    resolved: bool,
) -> AppResult<Vec<Comment>> {
    let comment = fetch_comment(conn, comment_id)
        .await?
        .filter(|c| c.epic_id == epic_id)
        .ok_or_else(|| AppError::NotFound(format!("comment {comment_id} not found")))?;
    let thread_id = comment.thread_id.clone();
    conn.execute(
        "UPDATE comment SET resolved = ?1 WHERE epic_id = ?2 AND thread_id = ?3",
        params![resolved as i64, epic_id, thread_id.clone()],
    )
    .await?;
    list_thread(conn, epic_id, &thread_id).await
}

// ---- anchor validation -----------------------------------------------------

/// Validate the anchor of a NEW thread: `anchor_kind` must be in the
/// vocabulary and the anchor must exist under this epic (a map node for
/// `node`, a `document_section` row for `section`). Unknown or cross-epic
/// anchors are a `400`, matching the map's edge-linking guard style.
pub(crate) async fn validate_anchor(
    conn: &libsql::Connection,
    epic_id: &str,
    anchor_kind: &str,
    anchor_id: &str,
) -> AppResult<()> {
    if !VALID_ANCHOR_KINDS.contains(&anchor_kind) {
        return Err(AppError::BadRequest(format!(
            "`anchor_kind` must be one of node|section, got `{anchor_kind}`"
        )));
    }
    match anchor_kind {
        "node" => {
            if !crate::map::node_belongs_to_epic(conn, anchor_id, epic_id).await? {
                return Err(AppError::BadRequest(format!(
                    "map node {anchor_id} is not part of epic {epic_id}"
                )));
            }
        }
        "section" => {
            let mut rows = conn
                .query(
                    "SELECT 1 FROM document_section WHERE epic_id = ?1 AND section_id = ?2",
                    params![epic_id, anchor_id],
                )
                .await?;
            if rows.next().await?.is_none() {
                return Err(AppError::BadRequest(format!(
                    "document section {anchor_id} is not part of epic {epic_id} \
                     (sections appear as a document is synced)"
                )));
            }
        }
        _ => unreachable!("VALID_ANCHOR_KINDS covers every arm"),
    }
    Ok(())
}

// ---- REST handlers ---------------------------------------------------------

/// `POST /epics/{id}/comments` — post a comment: either start a new thread
/// under an anchor (`anchor_kind` + `anchor_id` + `body`) or reply into an
/// existing thread (`thread_id` [+ matching anchor fields]). Any
/// authenticated user may post; a capability-token (agent) post is
/// attributed `is_agent = 1` with `author_user_id = NULL`. `201` with the
/// comment; `400` blank body, unknown anchor vocabulary, anchor/thread
/// mismatch; `404` unknown epic. Publishes `comments_updated` on `epic:<id>`.
pub async fn post_comment(
    State(state): State<AppState>,
    Path(id): Path<String>,
    actor: Actor,
    Json(req): Json<PostCommentBody>,
) -> AppResult<(StatusCode, Json<Comment>)> {
    let conn = state.db.conn();
    if !crate::map::epic_exists(conn, &id).await? {
        return Err(AppError::NotFound(format!("epic {id} not found")));
    }
    let body = req
        .body
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .ok_or_else(|| AppError::BadRequest("`body` is required and must not be empty".to_string()))?;

    // Resolve the thread + anchor: join an existing thread (inheriting or
    // re-confirming its anchor) or start a new one under a required anchor.
    let (thread_id, anchor_kind, anchor_id) = match req.thread_id.as_deref().map(str::trim) {
        Some(thread_id) if !thread_id.is_empty() => {
            // The thread must exist under THIS epic; its head comment fixes
            // the anchor every member shares.
            let head = list_thread(conn, &id, thread_id)
                .await?
                .into_iter()
                .next()
                .ok_or_else(|| {
                    AppError::BadRequest(format!(
                        "thread {thread_id} does not exist in epic {id}"
                    ))
                })?;
            if let Some(anchor_kind) = req.anchor_kind.as_deref().map(str::trim) {
                if anchor_kind != head.anchor_kind {
                    return Err(AppError::BadRequest(format!(
                        "thread {thread_id} is anchored to `{}`; a reply may not re-anchor it to `{anchor_kind}`",
                        head.anchor_kind
                    )));
                }
            }
            if let Some(anchor_id) = req.anchor_id.as_deref().map(str::trim) {
                if anchor_id != head.anchor_id {
                    return Err(AppError::BadRequest(format!(
                        "thread {thread_id} is anchored to {}; a reply may not re-anchor it",
                        head.anchor_id
                    )));
                }
            }
            (
                thread_id.to_string(),
                head.anchor_kind,
                head.anchor_id,
            )
        }
        _ => {
            let anchor_kind = req
                .anchor_kind
                .as_deref()
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .ok_or_else(|| {
                    AppError::BadRequest(
                        "`anchor_kind` is required to start a thread (node|section)".to_string(),
                    )
                })?;
            let anchor_id = req
                .anchor_id
                .as_deref()
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .ok_or_else(|| {
                    AppError::BadRequest("`anchor_id` is required to start a thread".to_string())
                })?;
            validate_anchor(conn, &id, anchor_kind, anchor_id).await?;
            (ulid::Ulid::new().to_string(), anchor_kind.to_string(), anchor_id.to_string())
        }
    };

    // Attribution: a signed-in human carries its user id; an agent run (a
    // capability token — no user behind `Actor`) is `is_agent = 1`.
    let is_agent = actor.user_id.is_none();
    let comment = insert_comment(
        conn,
        &id,
        &thread_id,
        &anchor_kind,
        &anchor_id,
        actor.user_id.as_deref(),
        is_agent,
        body,
    )
    .await?;

    publish_comments(&state, &id).await;
    Ok((StatusCode::CREATED, Json(comment)))
}

/// `GET /epics/{id}/comments` — the epic's comments, optionally narrowed by
/// `?anchor_kind=&anchor_id=` or `?thread_id=`. `404` unknown epic.
pub async fn list_comments_handler(
    State(state): State<AppState>,
    Path(id): Path<String>,
    Query(filter): Query<ListCommentsQuery>,
) -> AppResult<Json<HashMap<&'static str, Vec<Comment>>>> {
    let conn = state.db.conn();
    if !crate::map::epic_exists(conn, &id).await? {
        return Err(AppError::NotFound(format!("epic {id} not found")));
    }
    if let Some(anchor_kind) = filter.anchor_kind.as_deref() {
        if !VALID_ANCHOR_KINDS.contains(&anchor_kind) {
            return Err(AppError::BadRequest(format!(
                "`anchor_kind` must be one of node|section, got `{anchor_kind}`"
            )));
        }
    }
    let items = list_comments(conn, &id, &filter).await?;
    Ok(Json(HashMap::from([("items", items)])))
}

/// `POST /epics/{id}/comments/:commentId/resolve` — resolve the comment's
/// whole thread (§9's resolved flag is a conversation-level state). `200`
/// with the thread as it now stands; `404` unknown epic/comment. Publishes
/// `comments_updated` on `epic:<id>`.
pub async fn resolve_comment(
    State(state): State<AppState>,
    Path((id, comment_id)): Path<(String, String)>,
) -> AppResult<Json<HashMap<&'static str, Vec<Comment>>>> {
    let conn = state.db.conn();
    if !crate::map::epic_exists(conn, &id).await? {
        return Err(AppError::NotFound(format!("epic {id} not found")));
    }
    let thread = set_thread_resolved(conn, &id, &comment_id, true).await?;
    publish_comments(&state, &id).await;
    Ok(Json(HashMap::from([("items", thread)])))
}

// ---- live updates ----------------------------------------------------------

/// Publish a `comments_updated` frame on `epic:<id>` carrying the epic's
/// full comment list, so a subscribed client re-renders. Best-effort: a read
/// error is logged and the publish is skipped (the DB write already
/// committed).
async fn publish_comments(state: &AppState, epic_id: &str) {
    match list_comments(state.db.conn(), epic_id, &ListCommentsQuery::default()).await {
        Ok(items) => {
            let payload =
                serde_json::to_value(&items).unwrap_or(serde_json::Value::Null);
            state
                .hub
                .publish(&format!("epic:{epic_id}"), "comments_updated", payload);
        }
        Err(err) => {
            tracing::warn!(epic = %epic_id, error = %err, "comment publish: failed to load comments");
        }
    }
}

// ---- tests ------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::users::{self, Role};
    use crate::{app, Config, Db};
    use axum::body::Body;
    use axum::http::{header::AUTHORIZATION, Request, StatusCode};
    use serde_json::{json, Value};
    use std::path::PathBuf;
    use tower::ServiceExt;

    async fn boot() -> (AppState, axum::Router) {
        let db = Db::connect(":memory:").await.unwrap();
        db.run_migrations().await.unwrap();
        let state = AppState::new(Config::for_test(), db);
        let router = app(state.clone());
        (state, router)
    }

    /// Insert a project + epic; return ids.
    async fn seed_epic(state: &AppState) -> (String, String) {
        let conn = state.db.conn();
        let now = crate::capability::now_ms();
        let project_id = ulid::Ulid::new().to_string();
        conn.execute(
            "INSERT INTO project (id, name, repo_url, clone_path, clone_status, created_at, updated_at) \
             VALUES (?1, 'P', 'https://example.com/p.git', NULL, 'ready', ?2, ?2)",
            libsql::params![project_id.clone(), now],
        )
        .await
        .unwrap();
        let epic_id = ulid::Ulid::new().to_string();
        conn.execute(
            "INSERT INTO epic (id, project_id, title, status, destination, created_at, updated_at) \
             VALUES (?1, ?2, 'E', 'Planning', 'It works end to end', ?3, ?3)",
            libsql::params![epic_id.clone(), project_id.clone(), now],
        )
        .await
        .unwrap();
        (project_id, epic_id)
    }

    /// Seed one map node to anchor comments to.
    async fn seed_node(state: &AppState, epic_id: &str, kind: &str) -> String {
        crate::map::create_node(
            state.db.conn(),
            epic_id,
            kind,
            None,
            "Which store?",
            Some("Pick the blob store"),
            None,
            None,
            None,
        )
        .await
        .unwrap()
        .id
    }

    /// Seed one document-section anchor row directly (sections are indexed
    /// from a synced document; the anchor validation only reads the index).
    async fn seed_section(state: &AppState, epic_id: &str, section_id: &str) {
        state
            .db
            .conn()
            .execute(
                "INSERT INTO document_section (epic_id, section_id, title, provenance, last_edited_by, version) \
                 VALUES (?1, ?2, 'Decisions', NULL, NULL, 1)",
                libsql::params![epic_id, section_id],
            )
            .await
            .unwrap();
    }

    fn get_bearer(uri: &str, token: &str) -> Request<Body> {
        Request::builder()
            .uri(uri)
            .header(AUTHORIZATION, format!("Bearer {token}"))
            .body(Body::empty())
            .unwrap()
    }

    fn json_bearer(method: &str, uri: &str, token: &str, body: Value) -> Request<Body> {
        Request::builder()
            .method(method)
            .uri(uri)
            .header(AUTHORIZATION, format!("Bearer {token}"))
            .header("content-type", "application/json")
            .body(Body::from(body.to_string()))
            .unwrap()
    }

    async fn body_json(response: axum::response::Response) -> Value {
        let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        serde_json::from_slice(&bytes).unwrap()
    }

    async fn post_comment(app: &axum::Router, token: &str, epic_id: &str, body: Value) -> axum::response::Response {
        app.clone()
            .oneshot(json_bearer(
                "POST",
                &format!("/epics/{epic_id}/comments"),
                token,
                body,
            ))
            .await
            .unwrap()
    }

    async fn get_comments(app: &axum::Router, token: &str, epic_id: &str, query: &str) -> (StatusCode, Value) {
        let response = app
            .clone()
            .oneshot(get_bearer(
                &format!("/epics/{epic_id}/comments{query}"),
                token,
            ))
            .await
            .unwrap();
        let status = response.status();
        (status, body_json(response).await)
    }

    /// AC: two distinct users post into the same node's comment thread and
    /// both appear with correct attribution — and the agent (a capability
    /// token) can reply into the same thread, attributed `is_agent = 1`.
    #[tokio::test]
    async fn two_users_and_the_agent_share_one_node_thread_with_correct_attribution() {
        let (state, app) = boot().await;
        let (project_id, epic_id) = seed_epic(&state).await;
        let node_id = seed_node(&state, &epic_id, "grilling").await;
        let alice = users::testing::seed_user(&state, "alice", Role::Admin, true).await;
        let bob = users::testing::seed_user(&state, "bob", Role::User, true).await;
        let alice_token = crate::sessions::testing::login_as(&state, &alice).await;
        let bob_token = crate::sessions::testing::login_as(&state, &bob).await;
        let agent_guard = state.caps.mint(
            epic_id.clone(),
            project_id.clone(),
            "grilling".into(),
            PathBuf::from("/tmp"),
        );
        let agent_token = agent_guard.token().to_string();

        // Alice opens the thread on the node.
        let response = post_comment(
            &app,
            &alice_token,
            &epic_id,
            json!({
                "anchor_kind": "node",
                "anchor_id": node_id,
                "body": "Which store are we picking?",
            }),
        )
        .await;
        assert_eq!(response.status(), StatusCode::CREATED);
        let head = body_json(response).await;
        assert_eq!(head["anchor_kind"], "node");
        assert_eq!(head["anchor_id"], node_id.as_str());
        assert_eq!(head["author_user_id"], alice.id.as_str());
        assert_eq!(head["is_agent"], false);
        assert_eq!(head["resolved"], false);
        let thread_id = head["thread_id"].as_str().unwrap().to_string();

        // Bob replies into the SAME thread — anchor inherited.
        let response = post_comment(
            &app,
            &bob_token,
            &epic_id,
            json!({ "thread_id": thread_id, "body": "The evidence store fits." }),
        )
        .await;
        assert_eq!(response.status(), StatusCode::CREATED);
        let reply = body_json(response).await;
        assert_eq!(reply["thread_id"], thread_id.as_str());
        assert_eq!(reply["anchor_id"], node_id.as_str());
        assert_eq!(reply["author_user_id"], bob.id.as_str());
        assert_eq!(reply["is_agent"], false);

        // The agent replies in turn, through its capability token: still the
        // same thread, but attributed to the agent (no human author).
        let response = post_comment(
            &app,
            &agent_token,
            &epic_id,
            json!({ "thread_id": thread_id, "body": "Leaning evidence store too." }),
        )
        .await;
        assert_eq!(response.status(), StatusCode::CREATED);
        let agent_reply = body_json(response).await;
        assert_eq!(agent_reply["thread_id"], thread_id.as_str());
        assert_eq!(agent_reply["author_user_id"], Value::Null);
        assert_eq!(agent_reply["is_agent"], true);

        // All three appear in the epic's list, oldest first, correctly
        // attributed.
        let (status, list) = get_comments(&app, &alice_token, &epic_id, "").await;
        assert_eq!(status, StatusCode::OK);
        let items = list["items"].as_array().unwrap();
        assert_eq!(items.len(), 3);
        assert_eq!(items[0]["author_user_id"], alice.id.as_str());
        assert_eq!(items[1]["author_user_id"], bob.id.as_str());
        assert_eq!(items[2]["author_user_id"], Value::Null);
        assert_eq!(items[2]["is_agent"], true);
        for item in items {
            assert_eq!(item["thread_id"], thread_id.as_str());
        }

        // Narrowing to the node's anchor returns exactly this thread.
        let (status, list) = get_comments(
            &app,
            &bob_token,
            &epic_id,
            &format!("?anchor_kind=node&anchor_id={node_id}"),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(list["items"].as_array().unwrap().len(), 3);

        // Resolving any member resolves the WHOLE thread.
        let reply_id = reply["id"].as_str().unwrap().to_string();
        let response = app
            .clone()
            .oneshot(json_bearer(
                "POST",
                &format!("/epics/{epic_id}/comments/{reply_id}/resolve"),
                &alice_token,
                serde_json::json!({}),
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let thread = body_json(response).await;
        let items = thread["items"].as_array().unwrap();
        assert_eq!(items.len(), 3);
        for item in items {
            assert_eq!(item["resolved"], true);
        }

        // And the list reflects it.
        let (_, list) = get_comments(&app, &alice_token, &epic_id, "").await;
        for item in list["items"].as_array().unwrap() {
            assert_eq!(item["resolved"], true);
        }
    }

    /// A section-anchored comment requires an existing section anchor row.
    #[tokio::test]
    async fn section_anchors_validate_against_the_document_section_index() {
        let (state, app) = boot().await;
        let (_project_id, epic_id) = seed_epic(&state).await;
        let user = users::testing::seed_user(&state, "planner", Role::Admin, true).await;
        let token = crate::sessions::testing::login_as(&state, &user).await;

        // Unknown anchor id → 400, naming the epic (matches the map's
        // edge-link guard style).
        let response = post_comment(
            &app,
            &token,
            &epic_id,
            json!({
                "anchor_kind": "section",
                "anchor_id": "no-such-section",
                "body": "Hello?",
            }),
        )
        .await;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);

        // Once the section exists (a synced document indexed it), the same
        // comment posts fine.
        seed_section(&state, &epic_id, "decisions").await;
        let response = post_comment(
            &app,
            &token,
            &epic_id,
            json!({
                "anchor_kind": "section",
                "anchor_id": "decisions",
                "body": "This section reads well.",
            }),
        )
        .await;
        assert_eq!(response.status(), StatusCode::CREATED);
        let comment = body_json(response).await;
        assert_eq!(comment["anchor_kind"], "section");
        assert_eq!(comment["anchor_id"], "decisions");

        // The two anchor vocabularies stay distinct: node anchors validate
        // against the map, section anchors against the document.
        let response = post_comment(
            &app,
            &token,
            &epic_id,
            json!({
                "anchor_kind": "node",
                "anchor_id": "decisions",
                "body": "wrong vocabulary",
            }),
        )
        .await;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn malformed_comment_posts_are_rejected() {
        let (state, app) = boot().await;
        let (_project_id, epic_id) = seed_epic(&state).await;
        let node_id = seed_node(&state, &epic_id, "grilling").await;
        let user = users::testing::seed_user(&state, "planner", Role::Admin, true).await;
        let token = crate::sessions::testing::login_as(&state, &user).await;

        // Unknown epic → 404.
        let response = post_comment(
            &app,
            &token,
            "not-an-epic",
            json!({ "anchor_kind": "node", "anchor_id": node_id, "body": "hi" }),
        )
        .await;
        assert_eq!(response.status(), StatusCode::NOT_FOUND);

        // Unknown anchor vocabulary.
        let response = post_comment(
            &app,
            &token,
            &epic_id,
            json!({ "anchor_kind": "task", "anchor_id": node_id, "body": "hi" }),
        )
        .await;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);

        // Blank body.
        let response = post_comment(
            &app,
            &token,
            &epic_id,
            json!({ "anchor_kind": "node", "anchor_id": node_id, "body": "   " }),
        )
        .await;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);

        // A new thread without an anchor.
        let response = post_comment(&app, &token, &epic_id, json!({ "body": "hi" })).await;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);

        // An unknown thread id.
        let response = post_comment(
            &app,
            &token,
            &epic_id,
            json!({ "thread_id": "01NOPE", "body": "hi" }),
        )
        .await;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);

        // A reply may not re-anchor its thread.
        let response = post_comment(
            &app,
            &token,
            &epic_id,
            json!({
                "anchor_kind": "node",
                "anchor_id": node_id,
                "body": "head",
            }),
        )
        .await;
        assert_eq!(response.status(), StatusCode::CREATED);
        let thread_id = body_json(response).await["thread_id"]
            .as_str()
            .unwrap()
            .to_string();
        let response = post_comment(
            &app,
            &token,
            &epic_id,
            json!({
                "thread_id": thread_id,
                "anchor_id": "not-the-anchor",
                "body": "reply",
            }),
        )
        .await;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn resolving_an_unknown_comment_is_a_404() {
        let (state, app) = boot().await;
        let (_project_id, epic_id) = seed_epic(&state).await;
        let user = users::testing::seed_user(&state, "planner", Role::Admin, true).await;
        let token = crate::sessions::testing::login_as(&state, &user).await;

        let response = app
            .clone()
            .oneshot(json_bearer(
                "POST",
                &format!("/epics/{epic_id}/comments/01NOPE/resolve"),
                &token,
                serde_json::json!({}),
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NOT_FOUND);

        // Cross-epic lookups stay scoped: a comment from another epic is
        // equally invisible here.
        let (_other_project, other_epic) = seed_epic(&state).await;
        let node_id = seed_node(&state, &other_epic, "grilling").await;
        let response = post_comment(
            &app,
            &token,
            &other_epic,
            json!({ "anchor_kind": "node", "anchor_id": node_id, "body": "elsewhere" }),
        )
        .await;
        let comment = body_json(response).await;
        let response = app
            .oneshot(json_bearer(
                "POST",
                &format!("/epics/{epic_id}/comments/{}/resolve", comment["id"].as_str().unwrap()),
                &token,
                serde_json::json!({}),
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }
}
