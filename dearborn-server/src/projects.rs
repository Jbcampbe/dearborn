//! Project CRUD API (T-101).
//!
//! Backs the `project` table (MILESTONE_1 §2.2) with the five standard CRUD
//! endpoints, all mounted behind the bearer-auth layer (see `lib.rs`). Follows
//! the wire contract in `CONVENTIONS.md`: single resources render directly,
//! collections wrap an `items` array, IDs are server-generated ULIDs, and all
//! `*_at` timestamps are unix milliseconds.
//!
//! ## PAT handling (T-102)
//!
//! `POST`/`PATCH` accept an optional `pat`, encrypted with AES-256-GCM (see
//! [`crate::crypto`]) before it is written to the `pat_encrypted` BLOB. The
//! plaintext PAT lives only inside the request handler's stack frame; the
//! internal decrypt path is [`load_decrypted_pat`] (used by T-103's cloning —
//! never exposed via a route).
//!
//! ## Cloning (T-103)
//!
//! On create (when `config.auto_clone`), `clone_path` is recorded and a
//! background task shells out to `git clone` (see [`crate::git`]), flipping
//! `clone_status` from `'pending'` to `'ready'`/`'error'` and publishing a
//! `project:<id>` WS event. `POST /projects/{id}/refresh` re-syncs the checkout.
//!
//! ## Secrets
//!
//! The serialized [`Project`] response omits `pat_encrypted` entirely — the
//! column is never selected into the DTO, so it can never leak through the API.
//! The incoming `pat` is wrapped in [`Secret`], whose `Debug` is redacted, so it
//! cannot leak through a log line either.

use axum::{
    extract::{Path, State},
    http::StatusCode,
    Json,
};
use libsql::{params, params_from_iter, Connection, Row, Value};
use serde::{Deserialize, Deserializer, Serialize};
use serde_json::json;

use std::path::{Path as FsPath, PathBuf};

use crate::crypto::Secret;
use crate::git::{self, GitError};
use crate::{AppError, AppResult, AppState};

/// The columns projected into a [`Project`] DTO. Note the conspicuous absence of
/// `pat_encrypted`: it is never read back into an API-facing shape.
const PROJECT_COLUMNS: &str = "id, name, repo_url, setup_cmd, test_cmd, run_cmd, \
     clone_path, clone_status, clone_error, created_at, updated_at";

/// A project as returned by the API. **Never** carries `pat_encrypted`.
///
/// Optional command fields are `Option<String>` so a `NULL` column round-trips
/// as JSON `null` (not `""`): `NULL` in → `null` out.
#[derive(Debug, Serialize)]
pub struct Project {
    pub id: String,
    pub name: String,
    pub repo_url: String,
    pub setup_cmd: Option<String>,
    pub test_cmd: Option<String>,
    pub run_cmd: Option<String>,
    pub clone_path: Option<String>,
    pub clone_status: String,
    pub clone_error: Option<String>,
    pub created_at: i64,
    pub updated_at: i64,
}

/// `POST /projects` body. `name` and `repo_url` are required (validated in the
/// handler so a missing field yields the standard `bad_request` envelope rather
/// than a raw deserialize rejection); the command fields are optional.
#[derive(Debug, Deserialize)]
pub struct CreateProject {
    name: Option<String>,
    repo_url: Option<String>,
    #[serde(default)]
    setup_cmd: Option<String>,
    #[serde(default)]
    test_cmd: Option<String>,
    #[serde(default)]
    run_cmd: Option<String>,
    /// Optional GitHub PAT. Encrypted (AES-256-GCM) before insert into
    /// `pat_encrypted`; never echoed back. Wrapped in [`Secret`] so it can never
    /// leak through a `Debug` log line. An empty/whitespace value is treated as
    /// "no PAT" (column left `NULL`).
    #[serde(default)]
    pat: Option<Secret>,
}

/// `PATCH /projects/{id}` body — partial update. Every field is optional:
///
/// * `name` / `repo_url`: present (and non-empty) → updated; absent → untouched.
/// * command fields use a *double option* so the three states are distinct:
///   absent (`None`) → untouched, `null` (`Some(None)`) → set to `NULL`,
///   value (`Some(Some(v))`) → set to that value.
#[derive(Debug, Deserialize)]
pub struct UpdateProject {
    #[serde(default)]
    name: Option<String>,
    #[serde(default)]
    repo_url: Option<String>,
    #[serde(default, deserialize_with = "double_option")]
    setup_cmd: Option<Option<String>>,
    #[serde(default, deserialize_with = "double_option")]
    test_cmd: Option<Option<String>>,
    #[serde(default, deserialize_with = "double_option")]
    run_cmd: Option<Option<String>>,
    /// Set / clear the stored PAT (double-option): absent → untouched, `null` (or
    /// an empty string) → clear to `NULL`, value → re-encrypt and replace.
    #[serde(default, deserialize_with = "double_option")]
    pat: Option<Option<Secret>>,
}

/// Deserialize a present-but-maybe-null field into `Some(_)`, leaving an absent
/// field as `None` (via `#[serde(default)]`). This distinguishes "set to null"
/// from "not provided" for partial updates.
fn double_option<'de, D, T>(deserializer: D) -> Result<Option<Option<T>>, D::Error>
where
    D: Deserializer<'de>,
    T: Deserialize<'de>,
{
    Deserialize::deserialize(deserializer).map(Some)
}

/// `POST /projects` — create a project. `201` with the created resource.
pub async fn create_project(
    State(state): State<AppState>,
    Json(req): Json<CreateProject>,
) -> AppResult<(StatusCode, Json<Project>)> {
    let name = require_field(req.name, "name")?;
    let repo_url = require_field(req.repo_url, "repo_url")?;

    let id = ulid::Ulid::new().to_string();
    let now = now_ms();
    let conn = state.db.conn();

    // Encrypt the PAT (if any) before it touches the row. The plaintext lives only
    // in this stack frame; only ciphertext is persisted.
    let pat_encrypted = encrypt_pat(&state, req.pat.as_ref())?;

    // `clone_path`, `clone_error` are left NULL; `clone_status` takes its schema
    // default of 'pending' by omission. T-103 populates the clone_* columns.
    conn.execute(
        "INSERT INTO project \
             (id, name, repo_url, setup_cmd, test_cmd, run_cmd, pat_encrypted, created_at, updated_at) \
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
        params![
            id.clone(),
            name,
            repo_url.clone(),
            req.setup_cmd,
            req.test_cmd,
            req.run_cmd,
            blob_or_null(pat_encrypted),
            now,
            now,
        ],
    )
    .await?;

    // T-103: kick off the canonical read-only clone in the background. Record the
    // intended `clone_path` up front; the row stays `clone_status='pending'` until
    // the spawned task flips it to `ready`/`error`.
    if state.config.auto_clone {
        let dest = clone_dest(&state.config.clone_root, &id);
        conn.execute(
            "UPDATE project SET clone_path = ?1 WHERE id = ?2",
            params![dest.to_string_lossy().to_string(), id.clone()],
        )
        .await?;
        let pat_plain = plaintext_pat(req.pat.as_ref());
        spawn_clone(state.clone(), id.clone(), repo_url, pat_plain, dest);
    }

    let project = fetch_project(conn, &id)
        .await?
        .ok_or_else(|| AppError::Internal(format!("project {id} vanished after insert")))?;
    Ok((StatusCode::CREATED, Json(project)))
}

/// `GET /projects` — list all projects, newest first.
pub async fn list_projects(State(state): State<AppState>) -> AppResult<Json<serde_json::Value>> {
    let sql = format!("SELECT {PROJECT_COLUMNS} FROM project ORDER BY created_at DESC, id DESC");
    let mut rows = state.db.conn().query(&sql, ()).await?;

    let mut items = Vec::new();
    while let Some(row) = rows.next().await? {
        items.push(row_to_project(&row)?);
    }
    Ok(Json(json!({ "items": items })))
}

/// `GET /projects/{id}` — fetch one project or `404`.
pub async fn get_project(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> AppResult<Json<Project>> {
    let project = fetch_project(state.db.conn(), &id)
        .await?
        .ok_or_else(|| not_found(&id))?;
    Ok(Json(project))
}

/// `PATCH /projects/{id}` — partial update; bumps `updated_at`. `404` if absent.
pub async fn update_project(
    State(state): State<AppState>,
    Path(id): Path<String>,
    Json(req): Json<UpdateProject>,
) -> AppResult<Json<Project>> {
    let conn = state.db.conn();

    // Build the SET list dynamically so absent fields are left untouched.
    let mut assignments: Vec<&str> = Vec::new();
    let mut values: Vec<Value> = Vec::new();

    if let Some(name) = req.name {
        assignments.push("name = ?");
        values.push(Value::Text(non_empty(name, "name")?));
    }
    if let Some(repo_url) = req.repo_url {
        assignments.push("repo_url = ?");
        values.push(Value::Text(non_empty(repo_url, "repo_url")?));
    }
    for (column, field) in [
        ("setup_cmd = ?", req.setup_cmd),
        ("test_cmd = ?", req.test_cmd),
        ("run_cmd = ?", req.run_cmd),
    ] {
        if let Some(value) = field {
            assignments.push(column);
            values.push(match value {
                Some(text) => Value::Text(text),
                None => Value::Null,
            });
        }
    }

    // PAT gets the same double-option treatment, but the value is encrypted (or
    // cleared to NULL on an explicit null / empty string) rather than stored raw.
    if let Some(pat) = req.pat {
        assignments.push("pat_encrypted = ?");
        values.push(blob_or_null(encrypt_pat(&state, pat.as_ref())?));
    }

    // Always bump updated_at, even for an otherwise-empty patch.
    assignments.push("updated_at = ?");
    values.push(Value::Integer(now_ms()));
    // Bind the id last, matching the trailing `WHERE id = ?`.
    values.push(Value::Text(id.clone()));

    let sql = format!("UPDATE project SET {} WHERE id = ?", assignments.join(", "));
    let affected = conn.execute(&sql, params_from_iter(values)).await?;
    if affected == 0 {
        return Err(not_found(&id));
    }

    let project = fetch_project(conn, &id)
        .await?
        .ok_or_else(|| not_found(&id))?;
    Ok(Json(project))
}

/// `DELETE /projects/{id}` — remove a project. `204` on success, `404` if absent.
pub async fn delete_project(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> AppResult<StatusCode> {
    let affected = state
        .db
        .conn()
        .execute("DELETE FROM project WHERE id = ?1", params![id.clone()])
        .await?;
    if affected == 0 {
        return Err(not_found(&id));
    }
    Ok(StatusCode::NO_CONTENT)
}

/// `POST /projects/{id}/refresh` — re-sync the canonical read-only checkout.
///
/// Sets `clone_status='pending'` and (in production) spawns a background
/// `git fetch` + hard-reset to origin's default branch, updating the row to
/// `ready`/`error` on completion. Returns the (now-`pending`) project. `404` if
/// the project does not exist.
pub async fn refresh_project(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> AppResult<Json<Project>> {
    let project = fetch_project(state.db.conn(), &id)
        .await?
        .ok_or_else(|| not_found(&id))?;

    // Decrypt the stored PAT (crate-internal path) for the git operation.
    let pat = load_decrypted_pat(&state, &id).await?;
    let dest = project
        .clone_path
        .clone()
        .map(PathBuf::from)
        .unwrap_or_else(|| clone_dest(&state.config.clone_root, &id));

    state
        .db
        .conn()
        .execute(
            "UPDATE project SET clone_status = 'pending', clone_error = NULL, \
                 clone_path = ?1, updated_at = ?2 WHERE id = ?3",
            params![dest.to_string_lossy().to_string(), now_ms(), id.clone()],
        )
        .await?;

    if state.config.auto_clone {
        let state = state.clone();
        let id = id.clone();
        let repo_url = project.repo_url.clone();
        let dest = dest.clone();
        tokio::spawn(async move {
            let result = git::refresh_repo(&repo_url, pat.as_deref(), &dest).await;
            record_clone_outcome(&state, &id, &dest, result).await;
        });
    }

    let refreshed = fetch_project(state.db.conn(), &id)
        .await?
        .ok_or_else(|| not_found(&id))?;
    Ok(Json(refreshed))
}

/// The per-project clone directory: `<clone_root>/<project id>`.
fn clone_dest(clone_root: &str, id: &str) -> PathBuf {
    FsPath::new(clone_root).join(id)
}

/// Extract a trimmed, non-empty plaintext PAT from the request `Secret`.
fn plaintext_pat(pat: Option<&Secret>) -> Option<String> {
    pat.map(|s| s.expose().trim().to_string())
        .filter(|s| !s.is_empty())
}

/// Spawn the background clone task and record its outcome on completion.
fn spawn_clone(state: AppState, id: String, repo_url: String, pat: Option<String>, dest: PathBuf) {
    tokio::spawn(async move {
        let result = git::clone_repo(&repo_url, pat.as_deref(), &dest).await;
        record_clone_outcome(&state, &id, &dest, result).await;
    });
}

/// Write the terminal clone/refresh state to the row and publish a
/// `project:<id>` `clone_status` WS event. `GitError` messages are already
/// redacted of any token, so they are safe to store and log.
async fn record_clone_outcome(state: &AppState, id: &str, dest: &FsPath, result: Result<(), GitError>) {
    let (status, clone_error) = match &result {
        Ok(()) => ("ready", None),
        Err(err) => ("error", Some(err.message.clone())),
    };

    let write = state
        .db
        .conn()
        .execute(
            "UPDATE project SET clone_status = ?1, clone_error = ?2, updated_at = ?3 WHERE id = ?4",
            params![status, clone_error.clone(), now_ms(), id.to_string()],
        )
        .await;
    if let Err(err) = write {
        tracing::error!(project = %id, error = %err, "failed to record clone outcome");
        return;
    }

    state.hub.publish(
        &format!("project:{id}"),
        "clone_status",
        json!({
            "id": id,
            "clone_status": status,
            "clone_error": clone_error,
            "clone_path": dest.to_string_lossy(),
        }),
    );

    match &result {
        Ok(()) => tracing::info!(project = %id, path = %dest.display(), "clone ready"),
        Err(err) => tracing::warn!(project = %id, reason = %err.message, "clone failed"),
    }
}

/// Load a single project by id, mapping the row (sans `pat_encrypted`) to the DTO.
async fn fetch_project(conn: &Connection, id: &str) -> AppResult<Option<Project>> {
    let sql = format!("SELECT {PROJECT_COLUMNS} FROM project WHERE id = ?1");
    let mut rows = conn.query(&sql, params![id]).await?;
    match rows.next().await? {
        Some(row) => Ok(Some(row_to_project(&row)?)),
        None => Ok(None),
    }
}

/// Map a `PROJECT_COLUMNS`-ordered row into a [`Project`]. Nullable columns land
/// as `Option<String>`, so a `NULL` stays `null` on the way out.
fn row_to_project(row: &Row) -> Result<Project, libsql::Error> {
    Ok(Project {
        id: row.get(0)?,
        name: row.get(1)?,
        repo_url: row.get(2)?,
        setup_cmd: row.get(3)?,
        test_cmd: row.get(4)?,
        run_cmd: row.get(5)?,
        clone_path: row.get(6)?,
        clone_status: row.get(7)?,
        clone_error: row.get(8)?,
        created_at: row.get(9)?,
        updated_at: row.get(10)?,
    })
}

/// Encrypt an optional PAT for storage. An absent or empty/whitespace-only PAT
/// yields `None` (column set to `NULL`); otherwise the trimmed token is encrypted
/// with the master key. Encryption failure surfaces as a generic `500` (the PAT
/// is never included in the error).
fn encrypt_pat(state: &AppState, pat: Option<&Secret>) -> AppResult<Option<Vec<u8>>> {
    let Some(pat) = pat else { return Ok(None) };
    let trimmed = pat.expose().trim();
    if trimmed.is_empty() {
        return Ok(None);
    }
    let blob = state
        .crypto
        .encrypt_pat(trimmed)
        .map_err(|_| AppError::Internal("failed to encrypt PAT".to_string()))?;
    Ok(Some(blob))
}

/// Map optional ciphertext into a bindable `Value` (`Blob` or `Null`).
fn blob_or_null(blob: Option<Vec<u8>>) -> Value {
    match blob {
        Some(bytes) => Value::Blob(bytes),
        None => Value::Null,
    }
}

/// **Internal decrypt path** (T-103 calls this to get the plaintext PAT for
/// `git clone`/`git fetch`). Deliberately NOT exposed via any route and never
/// serialised: it reads the `pat_encrypted` BLOB for `id` and decrypts it.
///
/// Returns `Ok(None)` when the project exists but has no stored PAT, and a
/// `NotFound` error when the project id is unknown. Consumed by T-103's
/// clone/refresh; kept crate-internal by design.
pub(crate) async fn load_decrypted_pat(state: &AppState, id: &str) -> AppResult<Option<String>> {
    let mut rows = state
        .db
        .conn()
        .query("SELECT pat_encrypted FROM project WHERE id = ?1", params![id])
        .await?;
    let row = rows.next().await?.ok_or_else(|| not_found(id))?;
    let blob: Option<Vec<u8>> = row.get(0)?;
    match blob {
        Some(bytes) => {
            let pat = state
                .crypto
                .decrypt_pat(&bytes)
                .map_err(|_| AppError::Internal("failed to decrypt PAT".to_string()))?;
            Ok(Some(pat))
        }
        None => Ok(None),
    }
}

/// Require a present, non-empty (after trim) string field, or `400 bad_request`.
fn require_field(value: Option<String>, field: &str) -> AppResult<String> {
    match value {
        Some(v) => non_empty(v, field),
        None => Err(AppError::BadRequest(format!("`{field}` is required"))),
    }
}

/// Reject an empty/whitespace-only value; return the trimmed string otherwise.
fn non_empty(value: String, field: &str) -> AppResult<String> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Err(AppError::BadRequest(format!("`{field}` must not be empty")));
    }
    Ok(trimmed.to_string())
}

/// The standard not-found error for a project id.
fn not_found(id: &str) -> AppError {
    AppError::NotFound(format!("project {id} not found"))
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
        let (app, _state) = test_app_with_state().await;
        app
    }

    /// Like [`test_app`] but also hands back the [`AppState`] so a test can reach
    /// the raw db (to inspect `pat_encrypted`) and the internal decrypt path.
    async fn test_app_with_state() -> (axum::Router, AppState) {
        let db = Db::connect(":memory:").await.unwrap();
        db.run_migrations().await.unwrap();
        let state = AppState::new(Config::for_test(TOKEN), db);
        (app(state.clone()), state)
    }

    /// Build an authenticated request; `body` sets `Content-Type: application/json`.
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

    #[tokio::test]
    async fn full_crud_round_trip() {
        let app = test_app().await;

        // CREATE -> 201
        let created = app
            .clone()
            .oneshot(req(
                "POST",
                "/projects",
                Some(json!({
                    "name": "Demo",
                    "repo_url": "https://example.com/demo.git",
                    "setup_cmd": "make setup",
                    "run_cmd": "make run"
                })),
            ))
            .await
            .unwrap();
        assert_eq!(created.status(), StatusCode::CREATED);
        let created = body_json(created).await;
        let id = created["id"].as_str().unwrap().to_string();
        assert_eq!(created["name"], "Demo");
        assert_eq!(created["repo_url"], "https://example.com/demo.git");
        assert_eq!(created["setup_cmd"], "make setup");
        assert_eq!(created["clone_status"], "pending");
        assert_eq!(created["clone_path"], Json::Null);
        // Secret column is never serialized.
        assert!(created.get("pat_encrypted").is_none());
        let created_at = created["created_at"].as_i64().unwrap();

        // GET -> 200, equal to the created resource
        let got = app
            .clone()
            .oneshot(req("GET", &format!("/projects/{id}"), None))
            .await
            .unwrap();
        assert_eq!(got.status(), StatusCode::OK);
        assert_eq!(body_json(got).await, created);

        // LIST -> 200, contains our project
        let listed = app
            .clone()
            .oneshot(req("GET", "/projects", None))
            .await
            .unwrap();
        assert_eq!(listed.status(), StatusCode::OK);
        let listed = body_json(listed).await;
        let items = listed["items"].as_array().unwrap();
        assert_eq!(items.len(), 1);
        assert_eq!(items[0]["id"], id);
        assert!(items[0].get("pat_encrypted").is_none());

        // UPDATE -> 200, name changed, updated_at not older than created_at
        let updated = app
            .clone()
            .oneshot(req(
                "PATCH",
                &format!("/projects/{id}"),
                Some(json!({ "name": "Renamed" })),
            ))
            .await
            .unwrap();
        assert_eq!(updated.status(), StatusCode::OK);
        let updated = body_json(updated).await;
        assert_eq!(updated["name"], "Renamed");
        assert_eq!(updated["setup_cmd"], "make setup"); // untouched
        assert!(updated["updated_at"].as_i64().unwrap() >= created_at);

        // DELETE -> 204 (empty body)
        let deleted = app
            .clone()
            .oneshot(req("DELETE", &format!("/projects/{id}"), None))
            .await
            .unwrap();
        assert_eq!(deleted.status(), StatusCode::NO_CONTENT);

        // GET after delete -> 404 with the structured envelope
        let missing = app
            .clone()
            .oneshot(req("GET", &format!("/projects/{id}"), None))
            .await
            .unwrap();
        assert_eq!(missing.status(), StatusCode::NOT_FOUND);
        assert_eq!(body_json(missing).await["error"]["code"], "not_found");
    }

    #[tokio::test]
    async fn create_missing_name_is_structured_bad_request() {
        let response = test_app()
            .await
            .oneshot(req(
                "POST",
                "/projects",
                Some(json!({ "repo_url": "https://example.com/demo.git" })),
            ))
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        assert_eq!(body_json(response).await["error"]["code"], "bad_request");
    }

    #[tokio::test]
    async fn create_empty_repo_url_is_structured_bad_request() {
        let response = test_app()
            .await
            .oneshot(req(
                "POST",
                "/projects",
                Some(json!({ "name": "Demo", "repo_url": "   " })),
            ))
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        assert_eq!(body_json(response).await["error"]["code"], "bad_request");
    }

    #[tokio::test]
    async fn null_test_cmd_is_allowed_and_preserved_through_create_and_get() {
        let app = test_app().await;

        // Create WITHOUT test_cmd/run_cmd (they are NULL), with setup_cmd set.
        let created = app
            .clone()
            .oneshot(req(
                "POST",
                "/projects",
                Some(json!({
                    "name": "NullCmds",
                    "repo_url": "https://example.com/n.git",
                    "setup_cmd": "make setup"
                })),
            ))
            .await
            .unwrap();
        let created = body_json(created).await;
        let id = created["id"].as_str().unwrap().to_string();

        // NULL in -> null out (NOT empty string) on the create response...
        assert_eq!(
            created["test_cmd"],
            Json::Null,
            "test_cmd NULL must serialize as null"
        );
        assert_eq!(created["run_cmd"], Json::Null);
        assert_eq!(created["setup_cmd"], "make setup");

        // ...and it round-trips as null through a fresh GET.
        let got = app
            .clone()
            .oneshot(req("GET", &format!("/projects/{id}"), None))
            .await
            .unwrap();
        let got = body_json(got).await;
        assert_eq!(
            got["test_cmd"],
            Json::Null,
            "test_cmd NULL must be preserved as null (not empty string)"
        );
        assert!(got["test_cmd"].is_null());
    }

    #[tokio::test]
    async fn patch_can_set_and_clear_test_cmd() {
        let app = test_app().await;

        let created = app
            .clone()
            .oneshot(req(
                "POST",
                "/projects",
                Some(json!({
                    "name": "P",
                    "repo_url": "https://example.com/p.git",
                    "test_cmd": "cargo test"
                })),
            ))
            .await
            .unwrap();
        let id = body_json(created).await["id"].as_str().unwrap().to_string();

        // Explicit null clears it back to SQL NULL (double-option semantics).
        let cleared = app
            .clone()
            .oneshot(req(
                "PATCH",
                &format!("/projects/{id}"),
                Some(json!({ "test_cmd": null })),
            ))
            .await
            .unwrap();
        assert_eq!(body_json(cleared).await["test_cmd"], Json::Null);

        // Omitting test_cmd leaves the (now-null) value untouched.
        let untouched = app
            .clone()
            .oneshot(req(
                "PATCH",
                &format!("/projects/{id}"),
                Some(json!({ "name": "P2" })),
            ))
            .await
            .unwrap();
        let untouched = body_json(untouched).await;
        assert_eq!(untouched["name"], "P2");
        assert_eq!(untouched["test_cmd"], Json::Null);
    }

    // ---- T-102: PAT encryption at rest ----------------------------------

    const PAT: &str = "ghp_exampleSecretToken_ABC123";

    /// (a) A `pat` supplied on create is accepted but never echoed back — no
    /// `pat` or `pat_encrypted` field appears in any response.
    #[tokio::test]
    async fn create_with_pat_never_returns_it() {
        let app = test_app().await;
        let created = app
            .clone()
            .oneshot(req(
                "POST",
                "/projects",
                Some(json!({
                    "name": "Secret",
                    "repo_url": "https://example.com/s.git",
                    "pat": PAT
                })),
            ))
            .await
            .unwrap();
        assert_eq!(created.status(), StatusCode::CREATED);
        let created = body_json(created).await;
        let id = created["id"].as_str().unwrap().to_string();

        // Neither the plaintext PAT nor a pat/pat_encrypted field is present, and
        // the serialized body contains the token nowhere.
        assert!(created.get("pat").is_none());
        assert!(created.get("pat_encrypted").is_none());
        assert!(!created.to_string().contains("ghp_"));

        // ...nor on GET, nor in the list.
        let got = app
            .clone()
            .oneshot(req("GET", &format!("/projects/{id}"), None))
            .await
            .unwrap();
        let got = body_json(got).await;
        assert!(got.get("pat_encrypted").is_none());
        assert!(!got.to_string().contains("ghp_"));

        let listed = app
            .clone()
            .oneshot(req("GET", "/projects", None))
            .await
            .unwrap();
        assert!(!body_json(listed).await.to_string().contains("ghp_"));
    }

    /// (b) The bytes actually stored in `pat_encrypted` are ciphertext: non-empty
    /// and never containing the plaintext token. (c) The internal decrypt path
    /// round-trips them back to the original PAT.
    #[tokio::test]
    async fn stored_pat_is_ciphertext_and_decrypts_via_internal_path() {
        let (app, state) = test_app_with_state().await;
        let created = app
            .oneshot(req(
                "POST",
                "/projects",
                Some(json!({
                    "name": "Secret",
                    "repo_url": "https://example.com/s.git",
                    "pat": PAT
                })),
            ))
            .await
            .unwrap();
        let id = body_json(created).await["id"].as_str().unwrap().to_string();

        // Read the raw stored bytes straight from the db.
        let mut rows = state
            .db
            .conn()
            .query("SELECT pat_encrypted FROM project WHERE id = ?1", params![id.clone()])
            .await
            .unwrap();
        let row = rows.next().await.unwrap().unwrap();
        let stored: Vec<u8> = row.get(0).unwrap();

        // Ciphertext: non-empty, and the plaintext never appears in it.
        assert!(!stored.is_empty());
        assert_ne!(stored, PAT.as_bytes());
        assert!(
            !stored.windows(PAT.len()).any(|w| w == PAT.as_bytes()),
            "plaintext PAT must not appear in stored bytes"
        );

        // Internal decrypt path recovers the original PAT.
        let decrypted = load_decrypted_pat(&state, &id).await.unwrap();
        assert_eq!(decrypted.as_deref(), Some(PAT));
    }

    /// A project created without a PAT stores `NULL` and decrypts to `None`.
    #[tokio::test]
    async fn no_pat_stores_null_and_decrypts_none() {
        let (app, state) = test_app_with_state().await;
        let created = app
            .oneshot(req(
                "POST",
                "/projects",
                Some(json!({ "name": "NoPat", "repo_url": "https://example.com/n.git" })),
            ))
            .await
            .unwrap();
        let id = body_json(created).await["id"].as_str().unwrap().to_string();
        assert_eq!(load_decrypted_pat(&state, &id).await.unwrap(), None);
    }

    /// PATCH can set a PAT, then clear it back to `NULL` with an explicit null.
    #[tokio::test]
    async fn patch_can_set_and_clear_pat() {
        let (app, state) = test_app_with_state().await;
        let created = app
            .clone()
            .oneshot(req(
                "POST",
                "/projects",
                Some(json!({ "name": "P", "repo_url": "https://example.com/p.git" })),
            ))
            .await
            .unwrap();
        let id = body_json(created).await["id"].as_str().unwrap().to_string();

        // Set a PAT via PATCH...
        let set = app
            .clone()
            .oneshot(req(
                "PATCH",
                &format!("/projects/{id}"),
                Some(json!({ "pat": PAT })),
            ))
            .await
            .unwrap();
        assert_eq!(set.status(), StatusCode::OK);
        assert!(!body_json(set).await.to_string().contains("ghp_"));
        assert_eq!(
            load_decrypted_pat(&state, &id).await.unwrap().as_deref(),
            Some(PAT)
        );

        // ...then clear it with an explicit null (double-option semantics).
        let cleared = app
            .clone()
            .oneshot(req(
                "PATCH",
                &format!("/projects/{id}"),
                Some(json!({ "pat": null })),
            ))
            .await
            .unwrap();
        assert_eq!(cleared.status(), StatusCode::OK);
        assert_eq!(load_decrypted_pat(&state, &id).await.unwrap(), None);
    }

    // ---- T-103: clone lifecycle -----------------------------------------

    /// Build an app whose state has real cloning enabled and a private temp
    /// clone root, plus the temp dir path (caller cleans it up).
    async fn clone_test_app() -> (axum::Router, AppState, PathBuf) {
        let db = Db::connect(":memory:").await.unwrap();
        db.run_migrations().await.unwrap();
        let root = std::env::temp_dir().join(format!(
            "dearborn-clone-test-{}-{}",
            std::process::id(),
            now_ms()
        ));
        let mut config = Config::for_test(TOKEN);
        config.auto_clone = true;
        config.clone_root = root.to_string_lossy().to_string();
        let state = AppState::new(config, db);
        (app(state.clone()), state, root)
    }

    /// Poll `clone_status` until it leaves `pending` (or time out).
    async fn wait_until_settled(state: &AppState, id: &str) -> (String, Option<String>) {
        for _ in 0..100 {
            let mut rows = state
                .db
                .conn()
                .query(
                    "SELECT clone_status, clone_error FROM project WHERE id = ?1",
                    params![id.to_string()],
                )
                .await
                .unwrap();
            let row = rows.next().await.unwrap().unwrap();
            let status: String = row.get(0).unwrap();
            if status != "pending" {
                return (status, row.get(1).unwrap());
            }
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
        panic!("clone_status stayed 'pending' too long");
    }

    /// Creating a project against a bad URL drives the background clone to
    /// `error` with a readable, token-free reason; the response is `pending`.
    #[tokio::test]
    async fn create_with_bad_url_settles_to_error() {
        let (app, state, root) = clone_test_app().await;
        let created = app
            .oneshot(req(
                "POST",
                "/projects",
                Some(json!({
                    "name": "Bad",
                    "repo_url": "https://dearborn.invalid/nope/nope.git",
                    "pat": "ghp_secretTokenXYZ"
                })),
            ))
            .await
            .unwrap();
        let created = body_json(created).await;
        let id = created["id"].as_str().unwrap().to_string();
        // Response is immediate: still pending, clone_path recorded up front.
        assert_eq!(created["clone_status"], "pending");
        assert!(created["clone_path"].as_str().unwrap().ends_with(&id));

        let (status, error) = wait_until_settled(&state, &id).await;
        assert_eq!(status, "error");
        let error = error.expect("error status must carry a reason");
        assert!(!error.is_empty());
        assert!(!error.contains("ghp_"), "token must not leak into clone_error: {error}");

        let _ = std::fs::remove_dir_all(&root);
    }

    /// The refresh route on a project that never cloned settles to `error` for a
    /// bad URL (refresh falls back to an initial clone when none exists).
    #[tokio::test]
    async fn refresh_bad_url_settles_to_error() {
        let (app, state, root) = clone_test_app().await;

        let created = app
            .clone()
            .oneshot(req(
                "POST",
                "/projects",
                Some(json!({
                    "name": "Bad",
                    "repo_url": "https://dearborn.invalid/nope/nope.git"
                })),
            ))
            .await
            .unwrap();
        let id = body_json(created).await["id"].as_str().unwrap().to_string();
        // Let the create-time clone settle first.
        wait_until_settled(&state, &id).await;

        let refreshed = app
            .oneshot(req("POST", &format!("/projects/{id}/refresh"), None))
            .await
            .unwrap();
        assert_eq!(refreshed.status(), StatusCode::OK);
        assert_eq!(body_json(refreshed).await["clone_status"], "pending");

        let (status, error) = wait_until_settled(&state, &id).await;
        assert_eq!(status, "error");
        assert!(!error.unwrap().is_empty());

        let _ = std::fs::remove_dir_all(&root);
    }

    /// Refresh of an unknown project id is a `404`.
    #[tokio::test]
    async fn refresh_missing_project_is_404() {
        let response = test_app()
            .await
            .oneshot(req("POST", "/projects/nope/refresh", None))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn update_missing_project_is_404() {
        let response = test_app()
            .await
            .oneshot(req(
                "PATCH",
                "/projects/does-not-exist",
                Some(json!({ "name": "x" })),
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }
}
