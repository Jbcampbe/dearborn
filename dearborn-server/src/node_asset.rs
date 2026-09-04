//! The prototype artifact store (wayfinder epic §4.7) — `node_asset`.
//!
//! A prototype node's whole point is a **throwaway artifact** the agent builds
//! in its scratch workspace (never the target-repo clone — see
//! [`crate::node_engine`]) so a human can react to it. When the node resolves,
//! the resolution bundle ships that artifact here: stored **linked, not
//! inlined** (§4.7) — a `node_asset` row keyed to the node, its bytes in the
//! BLOB column, and the client fetches it separately to render in a
//! **sandboxed iframe** (`sandbox="allow-scripts"`, no `allow-same-origin`, so
//! the artifact runs on an opaque origin and cannot touch the app or its
//! storage).
//!
//! ## Why not [`crate::evidence`]'s store
//!
//! `evidence.rs` is the per-stage `agent_run` evidence table: it stores UTF-8
//! *transcripts* under a head+tail elision cap ([`crate::evidence::cap_log`]),
//! which is exactly wrong for an artifact (silently truncating a prototype's
//! HTML would corrupt it). `node_asset` already has its own table in the §4
//! schema, so the store here is that table's thin CRUD surface — insert on
//! resolve, list/read over REST.
//!
//! ## The REST surface (the `dearborn` CLI's reads)
//!
//! - `GET /epics/{id}/map-nodes/{nodeId}/assets` — the node's stored assets
//!   (metadata only: mime, label, byte size — linked, not inlined).
//! - `GET /epics/{id}/map-nodes/{nodeId}/assets/{assetId}` — the raw bytes
//!   (`Content-Type: mime`), which the client fetches with its bearer token
//!   and feeds to the iframe as `srcdoc`.
//!
//! Both are **reads**, so the capability-token allow-list
//! ([`crate::capability::authorize_cap_request`]) opens them to every phase.
//! The only WRITE — the resolution bundle's `artifact` part — rides the
//! already-HITL-gated `POST …/resolve` (see [`crate::resolve`]).

use axum::{
    extract::{Path, State},
    http::{header, HeaderName, HeaderValue, StatusCode},
    response::{IntoResponse, Response},
    Json,
};
use base64::{engine::general_purpose, Engine as _};
use libsql::{params, Connection, Row};
use serde::Serialize;
use serde_json::{json, Value};

use crate::{map, activity, AppError, AppResult, AppState};

/// Ceiling on one stored artifact: 8 MiB. A prototype is a throwaway single-
/// file HTML app, not a build output; this bounds the DB blob (the same
/// spirit as [`crate::evidence::LOG_CAP_BYTES`], without the elision — an
/// artifact is either stored whole or not at all).
pub const MAX_ASSET_BYTES: usize = 8 * 1024 * 1024;

// ---- DTOs ------------------------------------------------------------------

/// A stored artifact's metadata — everything except the bytes (linked, not
/// inlined; the bytes live behind the per-asset read endpoint).
#[derive(Debug, Clone, Serialize)]
pub struct NodeAssetMeta {
    pub id: String,
    pub node_id: String,
    pub mime: String,
    pub label: Option<String>,
    /// The artifact's size in bytes (`LENGTH(bytes)` — computed, not stored).
    pub byte_size: i64,
    pub created_at: i64,
}

const ASSET_COLUMNS: &str =
    "id, node_id, mime, label, LENGTH(bytes), created_at";

fn row_to_meta(row: &Row) -> Result<NodeAssetMeta, libsql::Error> {
    Ok(NodeAssetMeta {
        id: row.get(0)?,
        node_id: row.get(1)?,
        mime: row.get(2)?,
        label: row.get(3)?,
        byte_size: row.get(4)?,
        created_at: row.get(5)?,
    })
}

// ---- validation -------------------------------------------------------------

/// Validate and normalize a client-supplied `mime` type. It becomes the
/// `Content-Type` response header on the raw read, so it must never carry
/// header-breaking characters: after lowering, only `type/subtype` plus
/// `; key=value` parameters of plain token characters are accepted. An
/// absent mime defaults to `text/html` (the prototype artifact's shape — a
/// standalone HTML app, wayfinder epic §11).
pub(crate) fn validate_mime(raw: Option<&str>) -> AppResult<String> {
    let mime = raw
        .unwrap_or("text/html")
        .trim()
        .trim_end_matches(';')
        .trim()
        .to_ascii_lowercase();
    if mime.is_empty() {
        return Err(AppError::BadRequest("`artifact.mime` must not be empty".to_string()));
    }
    let is_token = |s: &str| {
        !s.is_empty()
            && s.bytes().all(|b| {
                b.is_ascii_alphanumeric()
                    || matches!(b, b'-' | b'+' | b'.' | b'_' | b'/' | b'=' | b' ')
            })
    };
    for part in mime.split(';') {
        if !is_token(part.trim()) {
            return Err(AppError::BadRequest(format!(
                "`artifact.mime` must be a plain type/subtype (with optional ; key=value \
                 parameters), got `{mime}`"
            )));
        }
    }
    Ok(mime)
}

/// Decode the resolution bundle's base64 artifact payload. The CLI encodes
/// with the standard alphabet, but a padded standard or URL-safe (padded or
/// not) encoding is accepted — a base64 payload identifies itself.
pub(crate) fn decode_artifact_bytes(data_base64: &str) -> AppResult<Vec<u8>> {
    let decoded = general_purpose::STANDARD
        .decode(data_base64.trim())
        .or_else(|_| general_purpose::URL_SAFE.decode(data_base64.trim()))
        .or_else(|_| general_purpose::URL_SAFE_NO_PAD.decode(data_base64.trim()))
        .map_err(|_| {
            AppError::BadRequest("`artifact.data_base64` is not valid base64".to_string())
        })?;
    if decoded.is_empty() {
        return Err(AppError::BadRequest(
            "`artifact.data_base64` decodes to an empty artifact".to_string(),
        ));
    }
    if decoded.len() > MAX_ASSET_BYTES {
        return Err(AppError::BadRequest(format!(
            "`artifact` exceeds the {} MiB store limit (got {} bytes)",
            MAX_ASSET_BYTES / (1024 * 1024),
            decoded.len(),
        )));
    }
    Ok(decoded)
}

// ---- store ------------------------------------------------------------------

/// Insert a `node_asset` linked from `node_id` and record the
/// [`activity::NODE_ASSET_STORED`] feed row (the node's epic supplies the
/// attribution context; the node itself carries no epic id, so it is joined
/// here rather than threaded through every caller).
pub async fn insert_asset(
    conn: &Connection,
    node_id: &str,
    mime: &str,
    bytes: Vec<u8>,
    label: Option<&str>,
    actor_user_id: Option<&str>,
) -> AppResult<NodeAssetMeta> {
    let node = map::fetch_node(conn, node_id)
        .await?
        .ok_or_else(|| AppError::NotFound(format!("map node {node_id} not found")))?;

    let id = ulid::Ulid::new().to_string();
    conn.execute(
        "INSERT INTO node_asset (id, node_id, mime, bytes, label, created_at) \
         VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
        params![
            id.clone(),
            node_id,
            mime,
            libsql::Value::Blob(bytes),
            label,
            crate::capability::now_ms(),
        ],
    )
    .await?;

    activity::record(
        conn,
        &node.epic_id,
        Some(node_id),
        actor_user_id,
        activity::NODE_ASSET_STORED,
        label.or(Some(mime)),
    )
    .await?;

    fetch_meta(conn, &id)
        .await?
        .ok_or_else(|| AppError::Internal(format!("node asset {id} vanished after insert")))
}

/// Fetch one asset's metadata by id, or `None`.
pub async fn fetch_meta(conn: &Connection, asset_id: &str) -> AppResult<Option<NodeAssetMeta>> {
    let sql = format!("SELECT {ASSET_COLUMNS} FROM node_asset WHERE id = ?1");
    let mut rows = conn.query(&sql, params![asset_id]).await?;
    match rows.next().await? {
        Some(row) => Ok(Some(row_to_meta(&row)?)),
        None => Ok(None),
    }
}

/// A node's stored assets, oldest first.
pub async fn list_assets(conn: &Connection, node_id: &str) -> AppResult<Vec<NodeAssetMeta>> {
    let sql = format!(
        "SELECT {ASSET_COLUMNS} FROM node_asset WHERE node_id = ?1 ORDER BY created_at ASC, id ASC"
    );
    let mut rows = conn.query(&sql, params![node_id]).await?;
    let mut items = Vec::new();
    while let Some(row) = rows.next().await? {
        items.push(row_to_meta(&row)?);
    }
    Ok(items)
}

/// Fetch one asset **with its bytes**, scoped through its node: the asset
/// must exist AND belong to `node_id` (itself already verified to belong to
/// the requesting epic), so an asset id from a sibling epic's node resolves
/// to `None` rather than leaking another map's artifact.
pub async fn fetch_asset_bytes(
    conn: &Connection,
    node_id: &str,
    asset_id: &str,
) -> AppResult<Option<(NodeAssetMeta, Vec<u8>)>> {
    let sql = format!(
        "SELECT {ASSET_COLUMNS}, bytes FROM node_asset WHERE id = ?1 AND node_id = ?2"
    );
    let mut rows = conn.query(&sql, params![asset_id, node_id]).await?;
    match rows.next().await? {
        Some(row) => {
            let meta = row_to_meta(&row)?;
            let bytes: Vec<u8> = row.get(6)?;
            Ok(Some((meta, bytes)))
        }
        None => Ok(None),
    }
}

// ---- REST handlers ----------------------------------------------------------

/// Load a node and guard it belongs to `epic_id`. `404` otherwise (mirrors
/// the map-node surface's scoping).
async fn require_node_in_epic(
    conn: &Connection,
    epic_id: &str,
    node_id: &str,
) -> AppResult<map::MapNode> {
    map::fetch_node(conn, node_id)
        .await?
        .filter(|node| node.epic_id == epic_id)
        .ok_or_else(|| AppError::NotFound(format!("map node {node_id} not found")))
}

/// `GET /epics/{id}/map-nodes/{nodeId}/assets` — a node's stored artifacts
/// (metadata only; bytes are linked, not inlined). `404` unknown epic/node.
pub async fn list_node_assets(
    State(state): State<AppState>,
    Path((epic_id, node_id)): Path<(String, String)>,
) -> AppResult<Json<Value>> {
    let conn = state.db.conn();
    require_node_in_epic(&conn, &epic_id, &node_id).await?;
    let items = list_assets(&conn, &node_id).await?;
    Ok(Json(json!({ "items": items })))
}

/// `GET /epics/{id}/map-nodes/{nodeId}/assets/{assetId}` — the artifact's raw
/// bytes as `Content-Type: mime`. The client fetches this with its bearer
/// token and renders it in the sandboxed iframe as `srcdoc`; `X-Content-Type-
/// Options: nosniff` and an `inline` disposition keep a direct navigation from
/// becoming anything other than a passive render. `404` unknown epic/node, or
/// an asset that is not this node's.
pub async fn get_node_asset(
    State(state): State<AppState>,
    Path((epic_id, node_id, asset_id)): Path<(String, String, String)>,
) -> AppResult<Response> {
    let conn = state.db.conn();
    require_node_in_epic(&conn, &epic_id, &node_id).await?;
    let (meta, bytes) = fetch_asset_bytes(&conn, &node_id, &asset_id)
        .await?
        .ok_or_else(|| {
            AppError::NotFound(format!("node asset {asset_id} not found on node {node_id}"))
        })?;

    let mut response = (StatusCode::OK, bytes).into_response();
    if let Ok(value) = HeaderValue::from_str(&meta.mime) {
        response.headers_mut().insert(header::CONTENT_TYPE, value);
    }
    response.headers_mut().insert(
        header::CONTENT_DISPOSITION,
        HeaderValue::from_static("inline"),
    );
    response.headers_mut().insert(
        HeaderName::from_static("x-content-type-options"),
        HeaderValue::from_static("nosniff"),
    );
    Ok(response)
}

// ---- tests ------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*; // brings `base64::Engine` (used by the module) into scope
    use crate::capability::now_ms;
    use crate::users::{self, Role};
    use crate::{app, AppState, Config, Db};
    use axum::body::Body;
    use axum::http::{header::AUTHORIZATION, Request, StatusCode};
    use serde_json::Value;
    use tower::ServiceExt; // for `oneshot`

    async fn boot() -> (AppState, axum::Router) {
        let db = Db::connect(":memory:").await.unwrap();
        db.run_migrations().await.unwrap();
        let state = AppState::new(Config::for_test(), db);
        let router = app(state.clone());
        (state, router)
    }

    async fn seed_epic(state: &AppState) -> String {
        let conn = state.db.conn();
        let now = now_ms();
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
            libsql::params![epic_id.clone(), project_id, now],
        )
        .await
        .unwrap();
        epic_id
    }

    async fn seed_node(state: &AppState, epic_id: &str, kind: &str) -> String {
        map::create_node(
            state.db.conn(),
            epic_id,
            kind,
            None,
            "Which shape?",
            Some("Pick the prototype's shape"),
            None,
            None,
            None,
        )
        .await
        .unwrap()
        .id
    }

    async fn get_json(app: &axum::Router, token: &str, uri: &str) -> (StatusCode, Value) {
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(uri)
                    .header(AUTHORIZATION, format!("Bearer {token}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let status = response.status();
        let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        (status, serde_json::from_slice(&bytes).unwrap())
    }

    // ---- AC: a stored artifact is linked from its node and readable back ----

    #[tokio::test]
    async fn an_inserted_asset_is_listed_and_read_back_with_its_bytes() {
        let (state, app) = boot().await;
        let user = users::testing::seed_user(&state, "planner", Role::Admin, true).await;
        let token = crate::sessions::testing::login_as(&state, &user).await;
        let epic_id = seed_epic(&state).await;
        let node_id = seed_node(&state, &epic_id, "prototype").await;

        let meta = insert_asset(
            state.db.conn(),
            &node_id,
            "text/html",
            b"<h1>It works</h1>".to_vec(),
            Some("index.html"),
            None,
        )
        .await
        .unwrap();
        assert_eq!(meta.node_id, node_id);
        assert_eq!(meta.mime, "text/html");
        assert_eq!(meta.byte_size, 17);

        // Listed (metadata only) under the node, scoped to the epic's URI.
        let (status, listed) = get_json(
            &app,
            &token,
            &format!("/epics/{epic_id}/map-nodes/{node_id}/assets"),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        let items = listed["items"].as_array().unwrap();
        assert_eq!(items.len(), 1);
        assert_eq!(items[0]["id"], meta.id.as_str());
        assert_eq!(items[0]["byte_size"], 17);
        assert!(items[0].get("bytes").is_none(), "linked, not inlined");

        // Read back raw with the stored content type.
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(&format!(
                        "/epics/{epic_id}/map-nodes/{node_id}/assets/{}",
                        meta.id
                    ))
                    .header(AUTHORIZATION, format!("Bearer {token}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers()[header::CONTENT_TYPE],
            "text/html",
            "the stored mime becomes the response's content type"
        );
        let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        assert_eq!(&bytes[..], b"<h1>It works</h1>");

        // The attribution feed recorded the store.
        let (_, activity) =
            get_json(&app, &token, &format!("/epics/{epic_id}/activity")).await;
        assert!(activity["items"]
            .as_array()
            .unwrap()
            .iter()
            .any(|row| row["action"] == "node_asset_stored"));
    }

    // ---- AC: assets are scoped to their own node's epic ---------------------

    #[tokio::test]
    async fn an_asset_is_not_reachable_through_another_epic_or_node() {
        let (state, app) = boot().await;
        let user = users::testing::seed_user(&state, "planner", Role::Admin, true).await;
        let token = crate::sessions::testing::login_as(&state, &user).await;
        let epic_a = seed_epic(&state).await;
        let node_a = seed_node(&state, &epic_a, "prototype").await;
        let epic_b = seed_epic(&state).await;
        let node_b = seed_node(&state, &epic_b, "prototype").await;

        let meta = insert_asset(
            state.db.conn(),
            &node_a,
            "text/html",
            b"<p>a's artifact</p>".to_vec(),
            None,
            None,
        )
        .await
        .unwrap();

        // Through the wrong epic's URI → 404.
        let (status, _) = get_json(
            &app,
            &token,
            &format!("/epics/{epic_b}/map-nodes/{node_a}/assets"),
        )
        .await;
        assert_eq!(status, StatusCode::NOT_FOUND);

        // Through a sibling node of the same epic → 404 (asset is node-scoped).
        let other_node = seed_node(&state, &epic_a, "prototype").await;
        let (status, _) = get_json(
            &app,
            &token,
            &format!("/epics/{epic_a}/map-nodes/{other_node}/assets/{}", meta.id),
        )
        .await;
        assert_eq!(status, StatusCode::NOT_FOUND);

        // …while node_b's own list stays empty.
        let (_, listed) = get_json(
            &app,
            &token,
            &format!("/epics/{epic_b}/map-nodes/{node_b}/assets"),
        )
        .await;
        assert_eq!(listed["items"].as_array().unwrap().len(), 0);
    }

    // ---- validation: mime + base64 + size cap --------------------------------

    #[test]
    fn mime_validation_accepts_plain_types_and_rejects_header_breakers() {
        assert_eq!(validate_mime(None).unwrap(), "text/html");
        assert_eq!(validate_mime(Some("Text/HTML")).unwrap(), "text/html");
        assert_eq!(
            validate_mime(Some("text/html; charset=utf-8")).unwrap(),
            "text/html; charset=utf-8"
        );
        // Surrounding whitespace (a trailing newline included) is trimmed away,
        // so it never reaches the Content-Type header.
        assert_eq!(validate_mime(Some(" text/html\n")).unwrap(), "text/html");
        assert!(validate_mime(Some("")).is_err());
        assert!(validate_mime(Some("text/html\r\nX-Evil: 1")).is_err());
        assert!(validate_mime(Some("text/ht\rml")).is_err());
    }

    #[test]
    fn artifact_decoding_rejects_garbage_and_oversize_but_not_the_cap_edge() {
        assert!(decode_artifact_bytes("not base64!!").is_err());
        assert!(decode_artifact_bytes("").is_err());
        assert_eq!(
            decode_artifact_bytes(&general_purpose::STANDARD.encode(b"hello")).unwrap(),
            b"hello".to_vec()
        );
        // URL-safe (padded or not) also decodes — a base64 payload identifies itself.
        assert_eq!(
            decode_artifact_bytes(&general_purpose::URL_SAFE_NO_PAD.encode(b"bytes")).unwrap(),
            b"bytes".to_vec()
        );

        // Just over the cap → rejected; exactly at the cap → accepted.
        let oversize = vec![0u8; MAX_ASSET_BYTES + 1];
        let err = decode_artifact_bytes(&general_purpose::STANDARD.encode(&oversize)).unwrap_err();
        assert!(err.to_string().contains("store limit"));
        let at_cap = vec![0u8; MAX_ASSET_BYTES];
        assert!(decode_artifact_bytes(&general_purpose::STANDARD.encode(&at_cap)).is_ok());
    }
}
