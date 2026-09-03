//! Epics and the wayfinder prose they carry (epic "Wayfinder-Inspired
//! Planning" §3/§4.10).
//!
//! An **epic** is the unit of planning. Creating one requires a **destination**
//! — what the finished plan looks like — and lands it in `status='Planning'`;
//! the map workflow (decision nodes, the living Document) grows out from there
//! in later tasks. The old linear product/technical planning-session flow was
//! removed in the clean cutover: there is no epic-level transcript, no phase
//! sessions, and no advance-phase step — planning history lives on map nodes.
//!
//! Following the wire contract in `CONVENTIONS.md`: single resources render
//! directly, collections wrap an `items` array, IDs are server-generated
//! ULIDs, and all `*_at` timestamps are unix milliseconds.

use axum::{
    extract::{Path, State},
    http::StatusCode,
    Json,
};
use libsql::{params, params_from_iter, Connection, Row, Value};
use serde::{Deserialize, Deserializer, Serialize};
use serde_json::json;

use crate::{git, projects, AppError, AppResult, AppState};

/// Columns projected into an [`Epic`] DTO. The lease columns (`lease_owner`,
/// `lease_expires_at`, `branch_name`) are internal executor state and are
/// deliberately omitted here — they are read by the worker via direct SQL
/// (T-510+), never through this DTO. `pr_url`/`pr_number`/`blocked_reason`
/// (M2 §2.1) *are* part of the API-facing shape: they tell the user where the
/// epic's PR landed and, if it stalled, why.
const EPIC_COLUMNS: &str =
    "id, project_id, title, description, destination, notes, \
     base_branch, status, pr_url, pr_number, blocked_reason, failure_detail, created_at, updated_at";

/// An epic as returned by the API. Lands in `status='Planning'` on create.
///
/// `description` is `Option<String>` so a `NULL` column round-trips as JSON
/// `null` (it is a user-facing short blurb shown on kanban cards).
/// `destination` is the required, human-typed statement of what the finished
/// plan looks like (wayfinder plan §3); `notes` is its optional companion
/// prose. The remaining wayfinder prose (`not_yet_specified` / `out_of_scope`)
/// is not projected here — it belongs to the map workflow's own surfaces.
/// `pr_url` / `pr_number` / `blocked_reason` (M2 §2.1) are populated by the
/// executor: the PR identity once one opens, and the structured reason (§2.3)
/// if the epic lands in `Blocked` — with `failure_detail` (Rec 5) alongside
/// it: the same event's redacted, length-capped message. The lease columns
/// are deliberately **not** on this struct — see [`EPIC_COLUMNS`].
#[derive(Debug, Serialize)]
pub struct Epic {
    pub id: String,
    pub project_id: String,
    pub title: String,
    pub description: Option<String>,
    /// What the finished plan looks like — fixes scope (wayfinder plan §3).
    /// Required at creation; never `None` for epics created after the cutover
    /// (legacy rows keep `None` until re-created under the new flow).
    pub destination: Option<String>,
    /// Optional freeform prose alongside the destination (wayfinder plan §3).
    pub notes: Option<String>,
    /// The epic's §5 base-branch override (`None` = project default / repo
    /// default). Set at creation only; immutable afterwards.
    pub base_branch: Option<String>,
    pub status: String,
    pub pr_url: Option<String>,
    pub pr_number: Option<i64>,
    pub blocked_reason: Option<String>,
    /// The failed attempt's human-readable error text (Rec 5), redacted and
    /// length-capped by `worker::fail_item` before it ever lands here.
    pub failure_detail: Option<String>,
    pub created_at: i64,
    pub updated_at: i64,
}

/// `POST /projects/{id}/epics` body. `title` and `destination` are required
/// (validated in the handler so a missing/empty field yields the standard
/// `bad_request` envelope).
#[derive(Debug, Deserialize)]
pub struct CreateEpic {
    title: Option<String>,
    /// Required: what the finished plan looks like (wayfinder plan §3). The
    /// seed for the whole map workflow — an epic cannot be created without one.
    destination: Option<String>,
    /// Optional short description (kanban card blurb). An empty/whitespace
    /// string is stored as `NULL`.
    #[serde(default)]
    description: Option<String>,
    /// Optional freeform prose alongside the destination (wayfinder plan §3).
    /// An empty/whitespace string is stored as `NULL`, like `description`.
    #[serde(default)]
    notes: Option<String>,
    /// Optional base-branch override (design doc §5): this epic provisions
    /// from and PRs into this branch instead of the project default / repo
    /// default. Validated against the remote at creation time (`ls-remote`
    /// with the project PAT; unknown branch → 400) and **immutable
    /// afterwards** — no PATCH surface exists by design.
    #[serde(default)]
    base_branch: Option<String>,
}

/// `PATCH /epics/{id}` body — manual edits to the epic's user-facing fields
/// (the Details tab). Every field is optional; absent fields are left
/// untouched. `description` is a double-option: absent → untouched,
/// `null` → clear to `NULL`, value → set. `title` must be non-empty when
/// present (validated in the handler).
#[derive(Debug, Deserialize)]
pub struct UpdateEpic {
    #[serde(default)]
    title: Option<String>,
    #[serde(default, deserialize_with = "double_option")]
    description: Option<Option<String>>,
}

/// Deserialize a present-but-maybe-null field into `Some(_)`, leaving an absent
/// field as `None` (via `#[serde(default)]`). This distinguishes "set to null"
/// from "not provided" for partial updates (mirrors `projects.rs`).
fn double_option<'de, D, T>(deserializer: D) -> Result<Option<Option<T>>, D::Error>
where
    D: Deserializer<'de>,
    T: Deserialize<'de>,
{
    Deserialize::deserialize(deserializer).map(Some)
}

/// `POST /projects/{id}/epics` — create an epic with its destination.
///
/// Lands the epic in `status='Planning'`. `404` if the project does not exist;
/// `400` if `title` or `destination` is missing/empty. The map workflow grows
/// from the destination (the seed grilling node is added in a later task; the
/// old linear planning session is gone with the cutover).
pub async fn create_epic(
    State(state): State<AppState>,
    Path(project_id): Path<String>,
    Json(req): Json<CreateEpic>,
) -> AppResult<(StatusCode, Json<Epic>)> {
    let title = require_field(req.title, "title")?;
    let destination = require_field(req.destination, "destination")?;
    let conn = state.db.conn();

    // The project must exist (FK is declared but not enforced without
    // `PRAGMA foreign_keys`; check explicitly for a clean 404).
    if !project_exists(conn, &project_id).await? {
        return Err(AppError::NotFound(format!(
            "project {project_id} not found"
        )));
    }

    // Validate an explicit base branch against the remote now (design doc
    // §5): a typo caught here never burns a provisioning run. Blank/whitespace
    // is treated as "not provided" (stored NULL), mirroring `description`.
    let base_branch = req
        .base_branch
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty());
    if let Some(branch) = base_branch {
        let pat = projects::load_decrypted_pat(&state, &project_id).await?;
        let exists = git::remote_branch_exists(
            &project_repo_url(conn, &project_id).await?.as_str(),
            pat.as_deref(),
            branch,
        )
        .await;
        match exists {
            Ok(true) => {}
            Ok(false) => {
                return Err(AppError::BadRequest(format!(
                    "base branch `{branch}` does not exist on the remote"
                )));
            }
            Err(err) => {
                return Err(AppError::BadRequest(format!(
                    "could not verify base branch `{branch}` against the remote: {}",
                    err.message
                )));
            }
        }
    }

    let id = ulid::Ulid::new().to_string();
    let now = now_ms();

    // `status` takes its schema default of 'Planning' by omission; the
    // wayfinder fog/out-of-scope prose and the Half-2 lease columns stay NULL.
    // An empty/whitespace description or notes is stored as NULL (unset).
    let description = req
        .description
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty());
    let notes = req.notes.as_deref().map(str::trim).filter(|s| !s.is_empty());
    conn.execute(
        "INSERT INTO epic (id, project_id, title, description, destination, notes, \
         base_branch, created_at, updated_at) \
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?8)",
        params![
            id.clone(),
            project_id,
            title,
            description,
            destination,
            notes,
            base_branch,
            now
        ],
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
        return Err(AppError::NotFound(format!(
            "project {project_id} not found"
        )));
    }

    let items = list_epics_by_project(conn, &project_id).await?;
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

/// `PATCH /epics/{id}` — manually edit an epic's title and/or context fields.
///
/// The write path mirrors `projects::update_project`: a dynamic SET list so
/// absent fields stay untouched, `updated_at` always bumped, `404` if the epic
/// does not exist, `400` on an empty `title`. `200` with the updated epic.
///
/// On success the updated epic is published as `epic_updated` on `epic:<id>` —
/// the same frame any epic edit emits — so any
/// subscribed view (planning record, DAG editor, kanban, or a second Details
/// tab) re-renders live with the manual edit.
pub async fn update_epic(
    State(state): State<AppState>,
    Path(id): Path<String>,
    Json(req): Json<UpdateEpic>,
) -> AppResult<Json<Epic>> {
    let conn = state.db.conn();

    // Build the SET list dynamically so absent fields are left untouched.
    let mut assignments: Vec<&str> = Vec::new();
    let mut values: Vec<Value> = Vec::new();

    if let Some(title) = req.title {
        assignments.push("title = ?");
        values.push(Value::Text(require_field(Some(title), "title")?));
    }
    for (column, field) in [("description = ?", req.description)] {
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

    let sql = format!("UPDATE epic SET {} WHERE id = ?", assignments.join(", "));
    let affected = conn.execute(&sql, params_from_iter(values)).await?;
    if affected == 0 {
        return Err(epic_not_found(&id));
    }

    let epic = fetch_epic(conn, &id)
        .await?
        .ok_or_else(|| AppError::Internal(format!("epic {id} vanished after update")))?;

    // Live-publish the manual edit on epic:<id> (payload = the updated epic).
    let payload = serde_json::to_value(&epic).unwrap_or(serde_json::Value::Null);
    state
        .hub
        .publish(&format!("epic:{id}"), "epic_updated", payload);

    Ok(Json(epic))
}

/// The `project_id` an epic belongs to, or `None` if the epic is unknown. Used
/// when minting a capability that needs the project (breakdown's `create_task`,
/// and planning's scope) without projecting a whole [`Epic`].
pub(crate) async fn get_epic_project_id(
    conn: &Connection,
    epic_id: &str,
) -> AppResult<Option<String>> {
    let mut rows = conn
        .query(
            "SELECT project_id FROM epic WHERE id = ?1",
            params![epic_id],
        )
        .await?;
    match rows.next().await? {
        Some(row) => Ok(Some(row.get::<String>(0)?)),
        None => Ok(None),
    }
}

/// The project's canonical clone path for an epic, if the clone is on disk.
/// Used by T-203 to point a tools-enabled planning run's `cwd` (and the
/// `read_codebase_context` root) at the read-only checkout. `Ok(None)` when the
/// epic is unknown or its project has not been cloned yet.
pub(crate) async fn get_epic_clone_path(
    conn: &Connection,
    epic_id: &str,
) -> AppResult<Option<String>> {
    let mut rows = conn
        .query(
            "SELECT p.clone_path FROM epic e JOIN project p ON p.id = e.project_id \
             WHERE e.id = ?1",
            params![epic_id],
        )
        .await?;
    match rows.next().await? {
        Some(row) => Ok(row.get::<Option<String>>(0)?),
        None => Ok(None),
    }
}

// ---- row / value plumbing ----------------------------------------------

/// List a project's epics, newest first (same ordering as `list_epics`).
/// Reused by the board loader (T-401) so the board and the epics list agree.
pub(crate) async fn list_epics_by_project(
    conn: &Connection,
    project_id: &str,
) -> AppResult<Vec<Epic>> {
    let sql = format!(
        "SELECT {EPIC_COLUMNS} FROM epic WHERE project_id = ?1 \
         ORDER BY created_at DESC, id DESC"
    );
    let mut rows = conn.query(&sql, params![project_id]).await?;
    let mut items = Vec::new();
    while let Some(row) = rows.next().await? {
        items.push(row_to_epic(&row)?);
    }
    Ok(items)
}

pub(crate) async fn fetch_epic(conn: &Connection, id: &str) -> AppResult<Option<Epic>> {
    let sql = format!("SELECT {EPIC_COLUMNS} FROM epic WHERE id = ?1");
    let mut rows = conn.query(&sql, params![id]).await?;
    match rows.next().await? {
        Some(row) => Ok(Some(row_to_epic(&row)?)),
        None => Ok(None),
    }
}

fn row_to_epic(row: &Row) -> Result<Epic, libsql::Error> {
    Ok(Epic {
        id: row.get(0)?,
        project_id: row.get(1)?,
        title: row.get(2)?,
        description: row.get(3)?,
        destination: row.get(4)?,
        notes: row.get(5)?,
        base_branch: row.get(6)?,
        status: row.get(7)?,
        pr_url: row.get(8)?,
        pr_number: row.get(9)?,
        blocked_reason: row.get(10)?,
        failure_detail: row.get(11)?,
        created_at: row.get(12)?,
        updated_at: row.get(13)?,
    })
}

pub(crate) async fn project_exists(conn: &Connection, project_id: &str) -> AppResult<bool> {
    let mut rows = conn
        .query("SELECT 1 FROM project WHERE id = ?1", params![project_id])
        .await?;
    Ok(rows.next().await?.is_some())
}

/// The project's `repo_url`, for callers that need it before any epic row
/// exists (epic-create's §5 remote validation probe). `404` if unknown.
async fn project_repo_url(conn: &Connection, project_id: &str) -> AppResult<String> {
    let mut rows = conn
        .query(
            "SELECT repo_url FROM project WHERE id = ?1",
            params![project_id],
        )
        .await?;
    match rows.next().await? {
        Some(row) => Ok(row.get(0)?),
        None => Err(AppError::NotFound(format!(
            "project {project_id} not found"
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

    async fn test_app() -> axum::Router {
        let db = Db::connect(":memory:").await.unwrap();
        db.run_migrations().await.unwrap();
        app(AppState::new(Config::for_test(), db))
    }

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

    fn req(method: &str, uri: &str, body: Option<Json>) -> Request<Body> {
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
                Some(json!({ "title": title, "destination": "It works end to end" })),
            ))
            .await
            .unwrap();
        assert_eq!(created.status(), StatusCode::CREATED);
        body_json(created).await
    }

    #[tokio::test]
    async fn epic_pr_and_blocked_reason_round_trip_but_lease_columns_stay_internal() {
        let db = Db::connect(":memory:").await.unwrap();
        db.run_migrations().await.unwrap();
        let state = AppState::new(Config::for_test(), db);
        let app = app(state.clone());
        let project_id = seed_project(&app).await;
        let id = create_epic_via_api(&app, &project_id, "Ship it").await["id"]
            .as_str()
            .unwrap()
            .to_string();

        let conn = state.db.conn();
        // Write the new columns directly via SQL, the way the executor will
        // (T-514 persists pr_url/pr_number; T-540 persists blocked_reason). Also
        // set the lease columns, mirroring a claimed epic, to prove they never
        // leak through the DTO.
        conn.execute(
            "UPDATE epic SET pr_url = ?1, pr_number = ?2, blocked_reason = ?3, \
                 lease_owner = ?4, lease_expires_at = ?5 WHERE id = ?6",
            params![
                "https://github.com/acme/demo/pull/42",
                42i64,
                "test_gate_exhausted",
                "worker-1",
                9_999_999_999i64,
                id.clone()
            ],
        )
        .await
        .unwrap();

        let epic = fetch_epic(conn, &id).await.unwrap().expect("epic exists");
        assert_eq!(
            epic.pr_url.as_deref(),
            Some("https://github.com/acme/demo/pull/42")
        );
        assert_eq!(epic.pr_number, Some(42));
        assert_eq!(epic.blocked_reason.as_deref(), Some("test_gate_exhausted"));

        // Same story through the HTTP response the client actually sees.
        let response = app
            .oneshot(req("GET", &format!("/epics/{id}"), None))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = body_json(response).await;
        assert_eq!(body["pr_url"], "https://github.com/acme/demo/pull/42");
        assert_eq!(body["pr_number"], 42);
        assert_eq!(body["blocked_reason"], "test_gate_exhausted");
        assert!(
            body.get("lease_owner").is_none(),
            "lease_owner must not be exposed"
        );
        assert!(
            body.get("lease_expires_at").is_none(),
            "lease_expires_at must not be exposed"
        );
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
    async fn create_epic_requires_a_destination() {
        let app = test_app().await;
        let project_id = seed_project(&app).await;

        // Missing destination -> 400.
        let missing = app
            .clone()
            .oneshot(req(
                "POST",
                &format!("/projects/{project_id}/epics"),
                Some(json!({ "title": "No destination" })),
            ))
            .await
            .unwrap();
        assert_eq!(missing.status(), StatusCode::BAD_REQUEST);
        assert_eq!(body_json(missing).await["error"]["code"], "bad_request");

        // Blank (whitespace-only) destination -> 400 too.
        let blank = app
            .oneshot(req(
                "POST",
                &format!("/projects/{project_id}/epics"),
                Some(json!({ "title": "Blank", "destination": "   " })),
            ))
            .await
            .unwrap();
        assert_eq!(blank.status(), StatusCode::BAD_REQUEST);
        assert_eq!(body_json(blank).await["error"]["code"], "bad_request");
    }

    #[tokio::test]
    async fn create_epic_persists_destination_and_notes() {
        let db = Db::connect(":memory:").await.unwrap();
        db.run_migrations().await.unwrap();
        let state = AppState::new(Config::for_test(), db);
        let app = app(state.clone());
        let project_id = seed_project(&app).await;

        // Destination (required) and notes (optional) are trimmed and stored.
        let created = app
            .clone()
            .oneshot(req(
                "POST",
                &format!("/projects/{project_id}/epics"),
                Some(json!({
                    "title": "Ship it",
                    "destination": "  A working exporter, end to end.  ",
                    "notes": "  Keep the executor untouched.  ",
                })),
            ))
            .await
            .unwrap();
        assert_eq!(created.status(), StatusCode::CREATED);
        let epic = body_json(created).await;
        assert_eq!(epic["destination"], "A working exporter, end to end.");
        assert_eq!(epic["notes"], "Keep the executor untouched.");

        // Round-trips through GET.
        let id = epic["id"].as_str().unwrap().to_string();
        let got = app
            .clone()
            .oneshot(req("GET", &format!("/epics/{id}"), None))
            .await
            .unwrap();
        assert_eq!(got.status(), StatusCode::OK);
        let got = body_json(got).await;
        assert_eq!(got["destination"], "A working exporter, end to end.");
        assert_eq!(got["notes"], "Keep the executor untouched.");

        // Blank or omitted notes store NULL (the destination is never null).
        let blank_notes = app
            .clone()
            .oneshot(req(
                "POST",
                &format!("/projects/{project_id}/epics"),
                Some(json!({
                    "title": "Blank notes",
                    "destination": "Done",
                    "notes": "   ",
                })),
            ))
            .await
            .unwrap();
        assert_eq!(blank_notes.status(), StatusCode::CREATED);
        let epic = body_json(blank_notes).await;
        assert_eq!(epic["destination"], "Done");
        assert_eq!(epic["notes"], Json::Null);

        let omitted_notes = app
            .oneshot(req(
                "POST",
                &format!("/projects/{project_id}/epics"),
                Some(json!({ "title": "Omitted notes", "destination": "Done" })),
            ))
            .await
            .unwrap();
        assert_eq!(omitted_notes.status(), StatusCode::CREATED);
        assert_eq!(body_json(omitted_notes).await["notes"], Json::Null);
    }

    #[tokio::test]
    async fn create_epic_on_unknown_project_is_404() {
        let app = test_app().await;
        let response = app
            .oneshot(req(
                "POST",
                "/projects/does-not-exist/epics",
                Some(json!({ "title": "E", "destination": "Done" })),
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
        assert_eq!(body_json(response).await["error"]["code"], "not_found");
    }

    #[tokio::test]
    async fn update_epic_patches_title_and_description_and_publishes() {
        let db = Db::connect(":memory:").await.unwrap();
        db.run_migrations().await.unwrap();
        let state = AppState::new(Config::for_test(), db);
        let app = app(state.clone());
        let project_id = seed_project(&app).await;
        let created = create_epic_via_api(&app, &project_id, "Old title").await;
        let id = created["id"].as_str().unwrap().to_string();
        let created_updated_at = created["updated_at"].as_i64().unwrap();

        let mut sub = state.hub.subscribe(&format!("epic:{id}"));

        let patched = app
            .oneshot(req(
                "PATCH",
                &format!("/epics/{id}"),
                Some(json!({
                    "title": "New title",
                    "description": "Short blurb.",
                })),
            ))
            .await
            .unwrap();
        assert_eq!(patched.status(), StatusCode::OK);
        let epic = body_json(patched).await;
        assert_eq!(epic["title"], "New title");
        assert_eq!(epic["description"], "Short blurb.");
        assert!(epic["updated_at"].as_i64().unwrap() >= created_updated_at);

        // The manual edit is live-published on epic:<id> (the epic_updated frame
        // any epic edit emits).
        let frame = sub.recv().await.unwrap();
        let v: Json = serde_json::from_str(&frame).unwrap();
        assert_eq!(v["type"], "epic_updated");
        assert_eq!(v["payload"]["title"], "New title");
    }

    #[tokio::test]
    async fn update_epic_partial_patch_leaves_absent_fields_untouched() {
        let app = test_app().await;
        let project_id = seed_project(&app).await;
        let id = create_epic_via_api(&app, &project_id, "Keep me").await["id"]
            .as_str()
            .unwrap()
            .to_string();

        let patched = app
            .clone()
            .oneshot(req(
                "PATCH",
                &format!("/epics/{id}"),
                Some(json!({ "description": "ctx" })),
            ))
            .await
            .unwrap();
        assert_eq!(patched.status(), StatusCode::OK);
        let epic = body_json(patched).await;
        assert_eq!(epic["title"], "Keep me", "absent title untouched");
        assert_eq!(epic["description"], "ctx");

        // An explicit null clears the description back to NULL.
        let cleared = app
            .oneshot(req(
                "PATCH",
                &format!("/epics/{id}"),
                Some(json!({ "description": null })),
            ))
            .await
            .unwrap();
        assert_eq!(cleared.status(), StatusCode::OK);
        assert_eq!(body_json(cleared).await["description"], Json::Null);
    }

    /// A local bare git fixture with `main` + `release/1` heads — the
    /// `ls-remote` probe target for the §5 epic-create validation tests
    /// (offline, no PAT). The project row is inserted directly because its
    /// repo_url is a local path, which `/projects`' https-only validation
    /// would reject.
    async fn seed_local_bare_project(state: &crate::AppState) -> (String, std::path::PathBuf) {
        let dir = std::env::temp_dir().join(format!(
            "dearborn-t13-bare-{}-{}",
            std::process::id(),
            ulid::Ulid::new()
        ));
        let run = |args: &[&str]| {
            std::process::Command::new("git")
                .args(args)
                .current_dir(&dir)
                .status()
                .unwrap()
                .success()
        };
        std::fs::create_dir_all(&dir).unwrap();
        assert!(run(&["init", "--bare", "-b", "main"]));

        // Seed commits via a throwaway clone, then push both branches.
        let work = dir.with_extension("work");
        std::fs::create_dir_all(&work).unwrap();
        let wrun = |args: &[&str]| {
            std::process::Command::new("git")
                .args(args)
                .current_dir(&work)
                .status()
                .unwrap()
                .success()
        };
        assert!(wrun(&[
            "clone",
            dir.to_str().unwrap(),
            work.to_str().unwrap()
        ]));
        std::fs::write(work.join("README.md"), "hi\n").unwrap();
        assert!(wrun(&["config", "user.email", "t@example.com"]));
        assert!(wrun(&["config", "user.name", "T"]));
        assert!(wrun(&["add", "."]));
        assert!(wrun(&["commit", "-m", "init"]));
        assert!(wrun(&["branch", "release/1"]));
        assert!(wrun(&["push", "origin", "main", "release/1"]));

        let conn = state.db.conn();
        let id = ulid::Ulid::new().to_string();
        let now = now_ms();
        conn.execute(
            "INSERT INTO project (id, name, repo_url, created_at, updated_at) \
             VALUES (?1, 'Bare', ?2, ?3, ?3)",
            params![id.clone(), dir.to_string_lossy(), now],
        )
        .await
        .unwrap();
        (id, dir)
    }

    #[tokio::test]
    async fn create_epic_with_existing_base_branch_stores_it_and_returns_it() {
        use crate::{Config, Db};
        let db = Db::connect(":memory:").await.unwrap();
        db.run_migrations().await.unwrap();
        let state = crate::AppState::new(Config::for_test(), db);
        let app = app(state.clone());
        let (project_id, _bare) = seed_local_bare_project(&state).await;

        let created = app
            .oneshot(req(
                "POST",
                &format!("/projects/{project_id}/epics"),
                Some(json!({
                    "title": "Stacked",
                    "destination": "Done",
                    "base_branch": " release/1 "
                })),
            ))
            .await
            .unwrap();
        assert_eq!(created.status(), StatusCode::CREATED);
        let epic = body_json(created).await;
        // Trimmed on the way in, round-tripped on the way out.
        assert_eq!(epic["base_branch"], "release/1");
    }

    #[tokio::test]
    async fn create_epic_with_unknown_base_branch_is_structured_bad_request() {
        use crate::{Config, Db};
        let db = Db::connect(":memory:").await.unwrap();
        db.run_migrations().await.unwrap();
        let state = crate::AppState::new(Config::for_test(), db);
        let app = app(state.clone());
        let (project_id, _bare) = seed_local_bare_project(&state).await;

        let response = app
            .oneshot(req(
                "POST",
                &format!("/projects/{project_id}/epics"),
                Some(json!({
                    "title": "Typo",
                    "destination": "Done",
                    "base_branch": "no-such-branch"
                })),
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        assert_eq!(body_json(response).await["error"]["code"], "bad_request");
    }

    #[tokio::test]
    async fn create_epic_blank_base_branch_stores_null_like_omitted() {
        use crate::{Config, Db};
        let db = Db::connect(":memory:").await.unwrap();
        db.run_migrations().await.unwrap();
        let state = crate::AppState::new(Config::for_test(), db);
        let app = app(state.clone());
        let project_id = seed_project(&app).await;

        let blank = app
            .clone()
            .oneshot(req(
                "POST",
                &format!("/projects/{project_id}/epics"),
                Some(json!({
                    "title": "Blank base",
                    "destination": "Done",
                    "base_branch": "   "
                })),
            ))
            .await
            .unwrap();
        assert_eq!(blank.status(), StatusCode::CREATED);
        assert_eq!(body_json(blank).await["base_branch"], Json::Null);

        let omitted = app
            .oneshot(req(
                "POST",
                &format!("/projects/{project_id}/epics"),
                Some(json!({ "title": "Omitted base", "destination": "Done" })),
            ))
            .await
            .unwrap();
        assert_eq!(omitted.status(), StatusCode::CREATED);
        assert_eq!(body_json(omitted).await["base_branch"], Json::Null);
    }

    #[tokio::test]
    async fn create_epic_with_description_round_trips_and_blank_is_null() {
        let app = test_app().await;
        let project_id = seed_project(&app).await;

        // A provided description is stored and returned.
        let created = app
            .clone()
            .oneshot(req(
                "POST",
                &format!("/projects/{project_id}/epics"),
                Some(json!({
                    "title": "Ship it",
                    "description": "  Short blurb.  ",
                    "destination": "Done"
                })),
            ))
            .await
            .unwrap();
        assert_eq!(created.status(), StatusCode::CREATED);
        let epic = body_json(created).await;
        assert_eq!(
            epic["description"], "Short blurb.",
            "description is trimmed"
        );

        // Omitted or blank descriptions store NULL.
        let blank = app
            .clone()
            .oneshot(req(
                "POST",
                &format!("/projects/{project_id}/epics"),
                Some(json!({
                    "title": "Blank",
                    "description": "   ",
                    "destination": "Done"
                })),
            ))
            .await
            .unwrap();
        assert_eq!(blank.status(), StatusCode::CREATED);
        assert_eq!(body_json(blank).await["description"], Json::Null);

        let omitted = create_epic_via_api(&app, &project_id, "Omitted").await;
        assert_eq!(omitted["description"], Json::Null);
    }

    #[tokio::test]
    async fn patch_epic_sets_and_clears_description() {
        let app = test_app().await;
        let project_id = seed_project(&app).await;
        let id = create_epic_via_api(&app, &project_id, "Keep me").await["id"]
            .as_str()
            .unwrap()
            .to_string();

        // Set the description; absent fields stay untouched.
        let patched = app
            .clone()
            .oneshot(req(
                "PATCH",
                &format!("/epics/{id}"),
                Some(json!({ "description": "Now with a blurb" })),
            ))
            .await
            .unwrap();
        assert_eq!(patched.status(), StatusCode::OK);
        let epic = body_json(patched).await;
        assert_eq!(epic["description"], "Now with a blurb");
        assert_eq!(epic["title"], "Keep me", "absent title untouched");

        // An explicit null clears it back to NULL.
        let cleared = app
            .oneshot(req(
                "PATCH",
                &format!("/epics/{id}"),
                Some(json!({ "description": null })),
            ))
            .await
            .unwrap();
        assert_eq!(cleared.status(), StatusCode::OK);
        assert_eq!(body_json(cleared).await["description"], Json::Null);
    }

    #[tokio::test]
    async fn update_epic_validates_title_and_unknown_epic() {
        let app = test_app().await;
        let project_id = seed_project(&app).await;
        let id = create_epic_via_api(&app, &project_id, "E").await["id"]
            .as_str()
            .unwrap()
            .to_string();

        // Empty title -> 400.
        let empty = app
            .clone()
            .oneshot(req(
                "PATCH",
                &format!("/epics/{id}"),
                Some(json!({ "title": "   " })),
            ))
            .await
            .unwrap();
        assert_eq!(empty.status(), StatusCode::BAD_REQUEST);
        assert_eq!(body_json(empty).await["error"]["code"], "bad_request");

        // Unknown epic -> 404.
        let missing = app
            .oneshot(req("PATCH", "/epics/nope", Some(json!({ "title": "x" }))))
            .await
            .unwrap();
        assert_eq!(missing.status(), StatusCode::NOT_FOUND);
}
}
