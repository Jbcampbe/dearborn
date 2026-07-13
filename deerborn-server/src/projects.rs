//! Project CRUD API (T-101).
//!
//! Backs the `project` table (MILESTONE_1 §2.2) with the five standard CRUD
//! endpoints, all mounted behind the bearer-auth layer (see `lib.rs`). Follows
//! the wire contract in `CONVENTIONS.md`: single resources render directly,
//! collections wrap an `items` array, IDs are server-generated ULIDs, and all
//! `*_at` timestamps are unix milliseconds.
//!
//! ## What this task deliberately does NOT do
//!
//! * **PAT handling is T-102.** No `pat` is accepted, stored, or returned here;
//!   `pat_encrypted` is left `NULL` on insert. The request DTOs mark the exact
//!   seam where T-102 slots the field in (search for `T-102`).
//! * **Cloning is T-103.** `clone_status` starts at its schema default
//!   `'pending'`; `clone_path`/`clone_error` stay `NULL`. No git runs here.
//!
//! ## Secrets
//!
//! The serialized [`Project`] response omits `pat_encrypted` entirely — the
//! column is never selected into the DTO, so it can never leak through the API.

use axum::{
    extract::{Path, State},
    http::StatusCode,
    Json,
};
use libsql::{params, params_from_iter, Connection, Row, Value};
use serde::{Deserialize, Deserializer, Serialize};
use serde_json::json;

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
    // T-102 seam: add `pat: Option<String>` here; encrypt it and bind the
    // ciphertext into the `pat_encrypted` column in `create_project` below.
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
    // T-102 seam: add `pat: Option<Option<String>>` here (same double-option
    // shape) to allow setting / clearing the stored PAT on update.
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

    // `pat_encrypted`, `clone_path`, `clone_error` are left NULL; `clone_status`
    // takes its schema default of 'pending' by omission. T-102 adds the PAT bind
    // to this INSERT; T-103 populates the clone_* columns out of band.
    conn.execute(
        "INSERT INTO project \
             (id, name, repo_url, setup_cmd, test_cmd, run_cmd, created_at, updated_at) \
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
        params![
            id.clone(),
            name,
            repo_url,
            req.setup_cmd,
            req.test_cmd,
            req.run_cmd,
            now,
            now,
        ],
    )
    .await?;

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
        let db = Db::connect(":memory:").await.unwrap();
        db.run_migrations().await.unwrap();
        app(AppState::new(Config::for_test(TOKEN), db))
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
