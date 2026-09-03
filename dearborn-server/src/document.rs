//! The living Document (wayfinder epic §4.5/§4.6, §10 — Phase 3): the
//! settled-decisions HTML spec a planning map evolves, its version lineage,
//! and its section anchor/provenance index.
//!
//! One HTML blob per epic (`document`) is the source of truth for the plan's
//! prose; every accepted edit lands a `document_version` row (the `vNN`
//! lineage) and rebuilds `document_section`, the anchor/provenance index over
//! the HTML (section ids are the HTML's `id=` attributes; provenance names the
//! map node that last wrote each section). Agents *evolve* the document with
//! surgical edits — they do not regenerate-and-clobber — so **last-writer-wins
//! is safe**, and LWW is exactly what a sync performs.
//!
//! ## The scratch-file round trip
//!
//! Big HTML goes through file tools, not tool-args (epic §10): the agent pulls
//! the document to a **scratch workspace file** (`dearborn document pull` /
//! [`crate::cli::CliClient::document_pull`]), edits it with its harness's
//! native Edit/Write, then commits it (`dearborn document sync` /
//! [`crate::cli::CliClient::document_sync`]) carrying the `base_version` it
//! read. The sync handler:
//!
//! 1. takes the **per-epic write semaphore** ([`AppState::document_write_lock`]
//!    — an in-process `tokio::Mutex` keyed by `epic_id`, sufficient because
//!    Dearborn is a single server process and SQLite already serializes
//!    writers),
//! 2. checks the base version — a stale base is a clean `409` for a re-read
//!    and retry, never a bad write (a moved anchor the same way),
//! 3. persists the new version + section index,
//! 4. publishes `document_updated` on `epic:<id>` so subscribed clients
//!     re-render.
//!
//! The write is confined to this bounded read→check→commit step (epic §7:
//! "only its *resolution edit* takes the semaphore"), so sibling node sessions
//! never stall behind it. The grilling resolution bundle ([`crate::resolve`])
//! folds this same critical section into `node resolve` (via
//! [`sync_under_semaphore`]); this module is the standalone store + REST
//! surface it builds on.
//!
//! The REST surface is exactly what the `dearborn` CLI's `document pull|sync`
//! verbs call, so it is on the capability-token allow-list
//! ([`crate::capability::authorize_cap_request`]) and accepts either a browser
//! session token or a per-run capability token.

use std::collections::{HashMap, HashSet};

use axum::{
    extract::{Path, State},
    Json,
};
use libsql::{params, Connection};
use serde::{Deserialize, Serialize};
use serde_json::json;

use crate::map::Actor;
use crate::{AppError, AppResult, AppState};

// ---- DTOs ------------------------------------------------------------------

/// The epic's living document (`document`, §4.5) as stored.
#[derive(Debug, Clone, Serialize)]
pub struct Document {
    pub epic_id: String,
    pub html: String,
    /// Monotonic lineage number; starts at 1 with the first sync.
    pub version: i64,
    /// Which human last edited (`NULL` when the editor acted through an agent
    /// run's capability token — node provenance covers those).
    pub last_edited_by: Option<String>,
    pub updated_at: i64,
}

/// One entry in the section anchor/provenance index (`document_section`,
/// §4.6): an `id=` attribute in the document's HTML, the heading text it
/// carries (when the anchor sits on an `h1`–`h6`), the map node(s) that
/// wrote/touched it, and the version at which it was last indexed.
#[derive(Debug, Clone, Serialize)]
pub struct DocumentSection {
    pub epic_id: String,
    /// Matches an `id=` attribute in `document.html`.
    pub section_id: String,
    /// The anchor's heading text, when it sits on an `h1`–`h6`.
    pub title: Option<String>,
    /// The node id that last wrote this section (`NULL` when a human edited
    /// it and no node ever has).
    pub provenance: Option<String>,
    pub last_edited_by: Option<String>,
    pub version: Option<i64>,
}

/// The document endpoints' view: the current document (or the not-yet-synced
/// empty state — `html: null, version: 0`) plus its section index, so one
/// response re-renders a client's Document view completely.
#[derive(Debug, Clone, Serialize)]
pub struct DocumentView {
    pub epic_id: String,
    /// `None` until the first sync creates version 1.
    pub html: Option<String>,
    /// `0` before the first sync; the base version a sync must carry.
    pub version: i64,
    pub last_edited_by: Option<String>,
    pub updated_at: Option<i64>,
    /// The section anchor/provenance index, in document order.
    pub sections: Vec<DocumentSection>,
}

/// `POST /epics/{id}/document/sync` body: the edited HTML plus the
/// `base_version` the caller read. `base_version` must equal the document's
/// current version (0 when never synced) or the sync is a clean `409` —
/// re-read and retry. `node_id`, when given, must be a map node of this epic
/// and becomes the sections' write provenance.
#[derive(Debug, Default, Deserialize)]
pub struct SyncDocumentBody {
    html: Option<String>,
    base_version: Option<i64>,
    node_id: Option<String>,
}

// ---- HTML section extraction -----------------------------------------------

/// One `id=` anchor found in a document's HTML, with its heading title.
#[derive(Debug, PartialEq)]
pub(crate) struct ExtractedSection {
    pub section_id: String,
    pub title: Option<String>,
}

/// Extract the section anchors of a document's HTML in document order: every
/// element's `id` attribute (quoted or unquoted, case-insensitive), deduped to
/// the first occurrence. When the anchor sits on an `h1`–`h6` element, its
/// heading text (markup stripped) becomes the section's title.
///
/// A hand-rolled scan rather than an HTML-parser dependency: agent-authored
/// planning documents are well-formed enough that tag/attribute-shape scanning
/// finds every anchor, and a mis-scanned doc degrades to a sparser index, not
/// a wrong write. Comments are skipped so an `id=` in one is not indexed.
pub(crate) fn extract_sections(html: &str) -> Vec<ExtractedSection> {
    // `to_ascii_lowercase` preserves byte offsets, so every index into `lower`
    // names the same slice of `html` (titles are taken from `html` for casing).
    let lower = html.to_ascii_lowercase();
    let bytes = lower.as_bytes();
    let mut sections: Vec<ExtractedSection> = Vec::new();
    let mut seen: HashSet<String> = HashSet::new();
    let mut i = 0;

    while i < bytes.len() {
        if bytes[i] != b'<' {
            i += 1;
            continue;
        }
        let name_start = i + 1;
        if lower[name_start..].starts_with("!--") {
            // A comment: skip past its `-->` so `id=` inside is never indexed.
            match lower[name_start..].find("-->") {
                Some(p) => {
                    i = name_start + p + 3;
                    continue;
                }
                None => break,
            }
        }
        // Tag name: the alphanumeric run after `<`.
        let mut j = name_start;
        while j < bytes.len() && bytes[j].is_ascii_alphanumeric() {
            j += 1;
        }
        let tag = &lower[name_start..j];
        if tag.is_empty() {
            // Comment/doctype/processing-instruction remnant: skip past `>`.
            match lower[j..].find('>') {
                Some(p) => {
                    i = j + p + 1;
                    continue;
                }
                None => break,
            }
        }
        let tag_end = match lower[j..].find('>') {
            Some(p) => j + p,
            None => break,
        };
        let id = extract_id_attr(&lower[j..tag_end]);
        let title = if matches!(tag, "h1" | "h2" | "h3" | "h4" | "h5" | "h6") {
            let after = tag_end + 1;
            let close = format!("</{tag}>");
            lower[after..]
                .find(&close)
                .map(|p| strip_tags(html[after..after + p].trim()))
                .filter(|t| !t.is_empty())
        } else {
            None
        };
        i = tag_end + 1;

        if let Some(section_id) = id {
            if !section_id.is_empty() && seen.insert(section_id.clone()) {
                sections.push(ExtractedSection {
                    section_id,
                    title,
                });
            }
        }
    }
    sections
}

/// Read the `id` attribute out of one tag's attribute text (already
/// lowercased), or `None`. Quoted attribute values are skipped whole, so an
/// `id=` inside a value (e.g. `href="?id=x"`) is not mistaken for the
/// attribute.
fn extract_id_attr(attrs: &str) -> Option<String> {
    let b = attrs.as_bytes();
    let mut i = 0;
    while i < b.len() {
        match b[i] {
            q @ (b'"' | b'\'') => {
                i += 1;
                while i < b.len() && b[i] != q {
                    i += 1;
                }
                i += 1;
            }
            c if c.is_ascii_alphabetic() => {
                let start = i;
                while i < b.len()
                    && (b[i].is_ascii_alphanumeric() || b[i] == b'-' || b[i] == b'_')
                {
                    i += 1;
                }
                let name = &attrs[start..i];
                let mut j = i;
                while j < b.len() && (b[j] == b' ' || b[j] == b'\n' || b[j] == b'\t') {
                    j += 1;
                }
                if name == "id" && j < b.len() && b[j] == b'=' {
                    j += 1;
                    while j < b.len() && (b[j] == b' ' || b[j] == b'\n' || b[j] == b'\t') {
                        j += 1;
                    }
                    if j >= b.len() {
                        return None;
                    }
                    return Some(match b[j] {
                        q @ (b'"' | b'\'') => {
                            let val_start = j + 1;
                            let mut k = val_start;
                            while k < b.len() && b[k] != q {
                                k += 1;
                            }
                            attrs[val_start..k.min(b.len())].to_string()
                        }
                        _ => {
                            let mut k = j;
                            while k < b.len() && b[k] != b' ' && b[k] != b'\n' && b[k] != b'\t' {
                                k += 1;
                            }
                            attrs[j..k].to_string()
                        }
                    });
                }
                // Another attribute: keep scanning from just past its name —
                // its `=`/value is consumed by the quote-skip arm above or as
                // ordinary characters.
            }
            _ => i += 1,
        }
    }
    None
}

/// Remove `<...>` markup from a heading's text (best-effort, for titles only).
fn strip_tags(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    let mut in_tag = false;
    for ch in text.chars() {
        match ch {
            '<' => in_tag = true,
            '>' => in_tag = false,
            ch if !in_tag => out.push(ch),
            _ => {}
        }
    }
    out.split_whitespace().collect::<Vec<_>>().join(" ")
}

// ---- store -----------------------------------------------------------------

const DOCUMENT_COLUMNS: &str = "epic_id, html, version, last_edited_by, updated_at";
const SECTION_COLUMNS: &str = "epic_id, section_id, title, provenance, last_edited_by, version";

fn row_to_document(row: &libsql::Row) -> Result<Document, libsql::Error> {
    Ok(Document {
        epic_id: row.get(0)?,
        html: row.get(1)?,
        version: row.get(2)?,
        last_edited_by: row.get(3)?,
        updated_at: row.get(4)?,
    })
}

fn row_to_section(row: &libsql::Row) -> Result<DocumentSection, libsql::Error> {
    Ok(DocumentSection {
        epic_id: row.get(0)?,
        section_id: row.get(1)?,
        title: row.get(2)?,
        provenance: row.get(3)?,
        last_edited_by: row.get(4)?,
        version: row.get(5)?,
    })
}

/// Fetch the epic's current document row, or `None` before the first sync.
pub async fn fetch_document(conn: &Connection, epic_id: &str) -> AppResult<Option<Document>> {
    let sql = format!("SELECT {DOCUMENT_COLUMNS} FROM document WHERE epic_id = ?1");
    let mut rows = conn.query(&sql, params![epic_id]).await?;
    match rows.next().await? {
        Some(row) => Ok(Some(row_to_document(&row)?)),
        None => Ok(None),
    }
}

/// The document's current version — `0` before the first sync (so a sync
/// carrying `base_version: 0` starts the lineage at version 1).
pub async fn current_version(conn: &Connection, epic_id: &str) -> AppResult<i64> {
    Ok(fetch_document(conn, epic_id).await?.map(|d| d.version).unwrap_or(0))
}

/// The section index in document order (insertion order — the index is
/// rebuilt wholesale on each sync).
pub async fn list_sections(conn: &Connection, epic_id: &str) -> AppResult<Vec<DocumentSection>> {
    let sql = format!(
        "SELECT {SECTION_COLUMNS} FROM document_section WHERE epic_id = ?1 ORDER BY rowid ASC"
    );
    let mut rows = conn.query(&sql, params![epic_id]).await?;
    let mut items = Vec::new();
    while let Some(row) = rows.next().await? {
        items.push(row_to_section(&row)?);
    }
    Ok(items)
}

/// The epic's full document view: current document (or the empty `version: 0`
/// state) plus its section index.
pub async fn document_view(conn: &Connection, epic_id: &str) -> AppResult<DocumentView> {
    let document = fetch_document(conn, epic_id).await?;
    let sections = list_sections(conn, epic_id).await?;
    Ok(match document {
        Some(document) => DocumentView {
            epic_id: epic_id.to_string(),
            html: Some(document.html),
            version: document.version,
            last_edited_by: document.last_edited_by,
            updated_at: Some(document.updated_at),
            sections,
        },
        None => DocumentView {
            epic_id: epic_id.to_string(),
            html: None,
            version: 0,
            last_edited_by: None,
            updated_at: None,
            sections,
        },
    })
}

/// Commit a new document version: land the `document_version` lineage row,
/// then move the `document` pointer to it. The caller has already taken the
/// per-epic write semaphore and checked `base_version` — this is the pure
/// write, and `new_version` must be `current + 1`.
pub async fn commit_version(
    conn: &Connection,
    epic_id: &str,
    html: &str,
    new_version: i64,
    editor_user_id: Option<&str>,
    node_id: Option<&str>,
) -> AppResult<Document> {
    let now = now_ms();
    conn.execute(
        "INSERT INTO document_version (epic_id, version, html, editor_user_id, node_id, created_at) \
         VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
        params![epic_id, new_version, html, editor_user_id, node_id, now],
    )
    .await?;
    conn.execute(
        "INSERT INTO document (epic_id, html, version, last_edited_by, updated_at) \
         VALUES (?1, ?2, ?3, ?4, ?5) \
         ON CONFLICT(epic_id) DO UPDATE \
             SET html = ?2, version = ?3, last_edited_by = ?4, updated_at = ?5",
        params![epic_id, html, new_version, editor_user_id, now],
    )
    .await?;

    fetch_document(conn, epic_id)
        .await?
        .ok_or_else(|| AppError::Internal(format!("document {epic_id} vanished after sync")))
}

/// Rebuild the section anchor/provenance index from the freshly-committed
/// HTML: every anchor present is (re-)inserted stamped with this sync's
/// writer/version; anchors that disappeared from the HTML are dropped.
///
/// Provenance is last-writer-wins: when the sync names a `node_id` it becomes
/// every present section's provenance; when it doesn't (a human edit), a
/// surviving section keeps its prior node provenance.
pub async fn rebuild_sections(
    conn: &Connection,
    epic_id: &str,
    html: &str,
    version: i64,
    editor_user_id: Option<&str>,
    node_id: Option<&str>,
) -> AppResult<Vec<DocumentSection>> {
    let mut prior_provenance: HashMap<String, Option<String>> = HashMap::new();
    for section in list_sections(conn, epic_id).await? {
        prior_provenance.insert(section.section_id, section.provenance);
    }

    conn.execute(
        "DELETE FROM document_section WHERE epic_id = ?1",
        params![epic_id],
    )
    .await?;

    let mut sections = Vec::new();
    for extracted in extract_sections(html) {
        let provenance = match node_id {
            Some(node_id) => Some(node_id.to_string()),
            None => prior_provenance
                .get(&extracted.section_id)
                .cloned()
                .flatten(),
        };
        conn.execute(
            "INSERT INTO document_section \
                 (epic_id, section_id, title, provenance, last_edited_by, version) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            params![
                epic_id,
                extracted.section_id.clone(),
                extracted.title.clone(),
                provenance.clone(),
                editor_user_id,
                version
            ],
        )
        .await?;
        sections.push(DocumentSection {
            epic_id: epic_id.to_string(),
            section_id: extracted.section_id,
            title: extracted.title,
            provenance,
            last_edited_by: editor_user_id.map(str::to_string),
            version: Some(version),
        });
    }
    Ok(sections)
}

// ---- REST handlers ---------------------------------------------------------

/// `GET /epics/{id}/document` — the epic's living document plus its section
/// index (`html: null, version: 0` before the first sync). `404` if the epic
/// does not exist. This is the `document pull` verb's read.
pub async fn get_document(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> AppResult<Json<DocumentView>> {
    let conn = state.db.conn();
    if !epic_exists(&conn, &id).await? {
        return Err(AppError::NotFound(format!("epic {id} not found")));
    }
    Ok(Json(document_view(&conn, &id).await?))
}

/// `POST /epics/{id}/document/sync` — commit an edited scratch file as a new
/// document version (the `document sync` verb's write). Takes the per-epic
/// write semaphore, checks the `base_version` (a stale base is a `409` for a
/// clean re-read/retry), persists the new `document_version` lineage row +
/// section index, and publishes `document_updated` on `epic:<id>`.
///
/// `400` on missing `html`/`base_version`, a negative `base_version`, or a
/// `node_id` outside this epic; `404` unknown epic; `409` stale base version.
pub async fn sync_document(
    State(state): State<AppState>,
    Path(id): Path<String>,
    actor: Actor,
    Json(req): Json<SyncDocumentBody>,
) -> AppResult<Json<DocumentView>> {
    let conn = state.db.conn();
    if !epic_exists(&conn, &id).await? {
        return Err(AppError::NotFound(format!("epic {id} not found")));
    }
    let html = req
        .html
        .ok_or_else(|| AppError::BadRequest("`html` is required".to_string()))?;
    let base_version = req.base_version.ok_or_else(|| {
        AppError::BadRequest("`base_version` is required (the version you read)".to_string())
    })?;
    if base_version < 0 {
        return Err(AppError::BadRequest(
            "`base_version` must not be negative".to_string(),
        ));
    }
    let node_id = req.node_id.as_deref().map(str::trim).filter(|s| !s.is_empty());
    if let Some(node_id) = node_id {
        if !crate::map::node_belongs_to_epic(&conn, node_id, &id).await? {
            return Err(AppError::BadRequest(format!(
                "map node {node_id} is not part of epic {id}"
            )));
        }
    }

    let view = sync_under_semaphore(
        &state,
        &id,
        &html,
        base_version,
        actor.user_id.as_deref(),
        node_id,
    )
    .await?;
    Ok(Json(view))
}

/// Commit an edited document as a new version under the per-epic write
/// semaphore — the whole bounded read→check→commit step of [`sync_document`],
/// factored out so the grilling resolution bundle ([`crate::resolve`]) can
/// fold the very same critical section into `node resolve` (epic §10: the
/// sync is "folded into `node resolve`", not duplicated beside it).
/// `409` on a stale `base_version` — a clean re-read/retry, never a bad write
/// — with nothing applied.
pub async fn sync_under_semaphore(
    state: &AppState,
    epic_id: &str,
    html: &str,
    base_version: i64,
    editor_user_id: Option<&str>,
    node_id: Option<&str>,
) -> AppResult<DocumentView> {
    let conn = state.db.conn();

    // The per-epic write semaphore (epic §7): the whole read→check→commit is
    // bounded, so a sibling session's resolution edit never stalls behind
    // anything but this critical section.
    let lock = state.document_write_lock(epic_id);
    let _guard = lock.lock().await;

    let current = current_version(&conn, epic_id).await?;
    if base_version != current {
        return Err(AppError::Conflict(format!(
            "document sync: base version {base_version} is stale (current version is {current}); \
             re-read the document and retry"
        )));
    }
    let new_version = current + 1;
    let document = commit_version(
        &conn,
        epic_id,
        html,
        new_version,
        editor_user_id,
        node_id,
    )
    .await?;
    let sections = rebuild_sections(
        &conn,
        epic_id,
        html,
        new_version,
        editor_user_id,
        node_id,
    )
    .await?;
    drop(_guard);

    let view = DocumentView {
        epic_id: epic_id.to_string(),
        html: Some(document.html),
        version: document.version,
        last_edited_by: document.last_edited_by,
        updated_at: Some(document.updated_at),
        sections,
    };
    publish_document_updated(state, epic_id, &view);
    Ok(view)
}

// ---- helpers ---------------------------------------------------------------

/// Publish a `document_updated` frame on `epic:<id>` — version, timestamp,
/// and the section index (not the HTML; a re-rendering client re-reads the
/// document). Best-effort by construction: publishing to a topic with no
/// subscribers is a no-op.
fn publish_document_updated(state: &AppState, epic_id: &str, view: &DocumentView) {
    let payload = json!({
        "epic_id": view.epic_id,
        "version": view.version,
        "updated_at": view.updated_at,
        "sections": view.sections,
    });
    state
        .hub
        .publish(&format!("epic:{epic_id}"), "document_updated", payload);
}

/// Whether an epic exists (lightweight existence check for route guards).
async fn epic_exists(conn: &Connection, epic_id: &str) -> AppResult<bool> {
    let mut rows = conn
        .query("SELECT 1 FROM epic WHERE id = ?1", params![epic_id])
        .await?;
    Ok(rows.next().await?.is_some())
}

/// Current unix time in milliseconds (matches the `*_at` columns).
fn now_ms() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

// ---- tests -----------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::capability::now_ms;
    use crate::users::{self, Role};
    use crate::{app, AppState, Config, Db};
    use axum::body::Body;
    use axum::http::{header::AUTHORIZATION, Request, StatusCode};
    use serde_json::{json, Value};
    use std::path::PathBuf;
    use tower::ServiceExt; // for `oneshot`

    /// Boot state + router, so tests exercise handlers over the real router.
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
            "INSERT INTO epic (id, project_id, title, status, created_at, updated_at) \
             VALUES (?1, ?2, 'E', 'Planning', ?3, ?3)",
            libsql::params![epic_id.clone(), project_id.clone(), now],
        )
        .await
        .unwrap();
        (project_id, epic_id)
    }

    fn get_bearer(uri: &str, token: &str) -> Request<Body> {
        Request::builder()
            .uri(uri)
            .header(AUTHORIZATION, format!("Bearer {token}"))
            .body(Body::empty())
            .unwrap()
    }

    fn post_json_bearer(uri: &str, token: &str, body: Value) -> Request<Body> {
        Request::builder()
            .method("POST")
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

    async fn get_document(app: &axum::Router, token: &str, epic_id: &str) -> (StatusCode, Value) {
        let response = app
            .clone()
            .oneshot(get_bearer(&format!("/epics/{epic_id}/document"), token))
            .await
            .unwrap();
        let status = response.status();
        (status, body_json(response).await)
    }

    async fn sync(
        app: &axum::Router,
        token: &str,
        epic_id: &str,
        body: Value,
    ) -> (StatusCode, Value) {
        let response = app
            .clone()
            .oneshot(post_json_bearer(
                &format!("/epics/{epic_id}/document/sync"),
                token,
                body,
            ))
            .await
            .unwrap();
        let status = response.status();
        (status, body_json(response).await)
    }

    // ---- AC: round trip — pull the empty doc, sync version 1 ---------------

    #[tokio::test]
    async fn the_document_round_trips_from_empty_to_version_one_and_back() {
        let (state, app) = boot().await;
        let user = users::testing::seed_user(&state, "planner", Role::Admin, true).await;
        let token = crate::sessions::testing::login_as(&state, &user).await;
        let (_project_id, epic_id) = seed_epic(&state).await;

        // Before the first sync: html null, version 0 — the base version a
        // first sync must carry.
        let (status, doc) = get_document(&app, &token, &epic_id).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(doc["html"], Value::Null);
        assert_eq!(doc["version"], 0);
        assert_eq!(doc["sections"].as_array().unwrap().len(), 0);

        // Sync (the agent's edited scratch file, posted by `document sync`).
        let html = "<h1 id=\"decisions\">Decisions</h1><p>Use the evidence blob store.</p>";
        let (status, view) = sync(
            &app,
            &token,
            &epic_id,
            json!({ "html": html, "base_version": 0 }),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(view["version"], 1);
        assert_eq!(view["html"], html);
        assert_eq!(view["last_edited_by"], user.id.as_str());
        assert_eq!(view["updated_at"].as_i64().is_some(), true);

        // The round trip closes: the same content reads back.
        let (status, doc) = get_document(&app, &token, &epic_id).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(doc["html"], html);
        assert_eq!(doc["version"], 1);

        // The version lineage row landed.
        let mut rows = state
            .db
            .conn()
            .query(
                "SELECT version, html FROM document_version WHERE epic_id = ?1",
                libsql::params![epic_id.clone()],
            )
            .await
            .unwrap();
        let mut lineage = Vec::new();
        while let Some(row) = rows.next().await.unwrap() {
            lineage.push((row.get::<i64>(0).unwrap(), row.get::<String>(1).unwrap()));
        }
        assert_eq!(lineage, vec![(1, html.to_string())]);
    }

    // ---- AC: version lineage grows; stale base is rejected for retry -------

    #[tokio::test]
    async fn consecutive_syncs_build_lineage_and_a_stale_base_is_rejected_for_retry() {
        let (state, app) = boot().await;
        let user = users::testing::seed_user(&state, "planner", Role::Admin, true).await;
        let token = crate::sessions::testing::login_as(&state, &user).await;
        let (_project_id, epic_id) = seed_epic(&state).await;

        let v1 = "<h1 id=\"a\">One</h1>";
        let (status, _) = sync(&app, &token, &epic_id, json!({ "html": v1, "base_version": 0 })).await;
        assert_eq!(status, StatusCode::OK);

        // A stale base (0, but the document is at 1) → 409 naming the current
        // version — the clean re-read/retry signal, never a bad write.
        let (status, err) = sync(
            &app,
            &token,
            &epic_id,
            json!({ "html": "<p>clobber</p>", "base_version": 0 }),
        )
        .await;
        assert_eq!(status, StatusCode::CONFLICT);
        assert!(err["error"]["message"].as_str().unwrap().contains("stale"));
        assert!(err["error"]["message"]
            .as_str()
            .unwrap()
            .contains("current version is 1"));

        // Nothing was written: still version 1, html untouched.
        let (_, doc) = get_document(&app, &token, &epic_id).await;
        assert_eq!(doc["version"], 1);
        assert_eq!(doc["html"], v1);

        // Re-read, retry with the correct base → version 2.
        let v2 = "<h1 id=\"a\">One</h1><h2 id=\"b\">Two</h2>";
        let (status, view) = sync(&app, &token, &epic_id, json!({ "html": v2, "base_version": 1 })).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(view["version"], 2);

        // The lineage holds both snapshots in order.
        let mut rows = state
            .db
            .conn()
            .query(
                "SELECT version, html FROM document_version WHERE epic_id = ?1 ORDER BY version ASC",
                libsql::params![epic_id.clone()],
            )
            .await
            .unwrap();
        let mut lineage = Vec::new();
        while let Some(row) = rows.next().await.unwrap() {
            lineage.push((row.get::<i64>(0).unwrap(), row.get::<String>(1).unwrap()));
        }
        assert_eq!(
            lineage,
            vec![(1, v1.to_string()), (2, v2.to_string())]
        );

        // A future version never seen → also stale (base must be current).
        let (status, err) = sync(
            &app,
            &token,
            &epic_id,
            json!({ "html": "x", "base_version": 99 }),
        )
        .await;
        assert_eq!(status, StatusCode::CONFLICT);
        assert!(err["error"]["message"]
            .as_str()
            .unwrap()
            .contains("current version is 2"));
    }

    // ---- AC: concurrent syncs serialize on the per-epic semaphore ----------

    #[tokio::test]
    async fn concurrent_syncs_serialize_and_exactly_one_wins() {
        let (state, app) = boot().await;
        let user = users::testing::seed_user(&state, "planner", Role::Admin, true).await;
        let token = crate::sessions::testing::login_as(&state, &user).await;
        let (_project_id, epic_id) = seed_epic(&state).await;

        let (status, _) = sync(&app, &token, &epic_id, json!({ "html": "v1", "base_version": 0 })).await;
        assert_eq!(status, StatusCode::OK);

        // Two syncs race on the same base version. The per-epic semaphore
        // serializes the read→check→commit: the first commits version 2, the
        // second re-reads under the lock, finds itself stale, and gets 409.
        let (a, b) = tokio::join!(
            sync(&app, &token, &epic_id, json!({ "html": "from a", "base_version": 1 })),
            sync(&app, &token, &epic_id, json!({ "html": "from b", "base_version": 1 })),
        );
        let mut statuses = [a.0, b.0];
        statuses.sort();
        assert_eq!(statuses, [StatusCode::OK, StatusCode::CONFLICT]);

        // A consistent outcome: exactly version 2, exactly two lineage rows
        // (no interleaved duplicate), html = whichever sync won.
        let (_, doc) = get_document(&app, &token, &epic_id).await;
        assert_eq!(doc["version"], 2);
        assert!(doc["html"] == "from a" || doc["html"] == "from b");
        let mut rows = state
            .db
            .conn()
            .query(
                "SELECT version FROM document_version WHERE epic_id = ?1 ORDER BY version ASC",
                libsql::params![epic_id],
            )
            .await
            .unwrap();
        let mut versions = Vec::new();
        while let Some(row) = rows.next().await.unwrap() {
            versions.push(row.get::<i64>(0).unwrap());
        }
        assert_eq!(versions, vec![1, 2]);
    }

    #[tokio::test]
    async fn document_write_lock_is_keyed_by_epic() {
        let db = Db::connect(":memory:").await.unwrap();
        let state = AppState::new(Config::for_test(), db);
        let a1 = state.document_write_lock("epic-a");
        let a2 = state.document_write_lock("epic-a");
        let b = state.document_write_lock("epic-b");
        assert!(std::sync::Arc::ptr_eq(&a1, &a2), "the same epic yields the same mutex");
        assert!(!std::sync::Arc::ptr_eq(&a1, &b), "different epics never contend");
    }

    // ---- AC: section anchor/provenance index updates -----------------------

    #[tokio::test]
    async fn the_section_index_tracks_anchors_titles_and_node_provenance() {
        let (state, app) = boot().await;
        let user = users::testing::seed_user(&state, "planner", Role::Admin, true).await;
        let token = crate::sessions::testing::login_as(&state, &user).await;
        let (_project_id, epic_id) = seed_epic(&state).await;
        let conn = state.db.conn();

        let node_a = crate::map::create_node(conn, &epic_id, "grilling", None, "A", None, None, None, None)
            .await
            .unwrap();
        let node_b = crate::map::create_node(conn, &epic_id, "grilling", None, "B", None, None, None, None)
            .await
            .unwrap();

        // First sync, by node A: sections land with A's provenance + version 1.
        let v1 = "<h1 id=\"decisions\">Decisions</h1>\
                  <h2 id=\"store-choice\">The <code>store</code></h2><p>libsql blobs.</p>\
                  <div id=\"risks\"></div>";
        let (status, view) = sync(
            &app,
            &token,
            &epic_id,
            json!({ "html": v1, "base_version": 0, "node_id": node_a.id }),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        let sections = view["sections"].as_array().unwrap();
        assert_eq!(sections.len(), 3);
        assert_eq!(sections[0]["section_id"], "decisions");
        assert_eq!(sections[0]["title"], "Decisions");
        assert_eq!(sections[0]["provenance"], node_a.id.as_str());
        assert_eq!(sections[0]["version"], 1);
        // Heading markup is stripped from the title.
        assert_eq!(sections[1]["section_id"], "store-choice");
        assert_eq!(sections[1]["title"], "The store");
        // Anchors off headings have no title.
        assert_eq!(sections[2]["section_id"], "risks");
        assert_eq!(sections[2]["title"], Value::Null);

        // Second sync, by node B: provenance follows the last writer (LWW),
        // a dropped anchor disappears, and the version stamps advance.
        let v2 = "<h2 id=\"store-choice\">The store</h2><p>libsql blobs.</p>";
        let (status, view) = sync(
            &app,
            &token,
            &epic_id,
            json!({ "html": v2, "base_version": 1, "node_id": node_b.id }),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(view["version"], 2);
        let sections = view["sections"].as_array().unwrap();
        assert_eq!(sections.len(), 1, "`decisions` and `risks` left the HTML");
        assert_eq!(sections[0]["provenance"], node_b.id.as_str());
        assert_eq!(sections[0]["version"], 2);

        // A human edit (no node id) keeps the surviving section's node
        // provenance rather than erasing it.
        let v3 = "<h2 id=\"store-choice\">The store, revised</h2>";
        let (status, view) = sync(&app, &token, &epic_id, json!({ "html": v3, "base_version": 2 })).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(view["sections"][0]["provenance"], node_b.id.as_str());
        assert_eq!(view["sections"][0]["version"], 3);

        // A sync naming a node outside the epic → 400.
        let (status, err) = sync(
            &app,
            &token,
            &epic_id,
            json!({ "html": "x", "base_version": 3, "node_id": "01JZZNOPE" }),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert!(err["error"]["message"].as_str().unwrap().contains("not part of epic"));
    }

    // ---- AC: document_updated streams to epic:<id> --------------------------

    #[tokio::test]
    async fn sync_publishes_document_updated_on_the_epic_topic() {
        let (state, app) = boot().await;
        let user = users::testing::seed_user(&state, "planner", Role::Admin, true).await;
        let token = crate::sessions::testing::login_as(&state, &user).await;
        let (_project_id, epic_id) = seed_epic(&state).await;
        let mut rx = state.hub.subscribe(&format!("epic:{epic_id}"));
        let mut other = state.hub.subscribe("epic:unrelated");

        let (status, _) = sync(
            &app,
            &token,
            &epic_id,
            json!({ "html": "<h1 id=\"a\">A</h1>", "base_version": 0 }),
        )
        .await;
        assert_eq!(status, StatusCode::OK);

        let frame: Value =
            serde_json::from_str(&rx.recv().await.unwrap()).unwrap();
        assert_eq!(frame["topic"], format!("epic:{epic_id}"));
        assert_eq!(frame["type"], "document_updated");
        assert_eq!(frame["payload"]["version"], 1);
        assert_eq!(frame["payload"]["sections"][0]["section_id"], "a");

        // The unrelated topic saw nothing.
        assert!(other.try_recv().is_err());
    }

    // ---- guards: unknown epic, missing fields -------------------------------

    #[tokio::test]
    async fn sync_and_get_validate_the_epic_and_the_body() {
        let (state, app) = boot().await;
        let user = users::testing::seed_user(&state, "planner", Role::Admin, true).await;
        let token = crate::sessions::testing::login_as(&state, &user).await;
        let (_project_id, epic_id) = seed_epic(&state).await;

        // Unknown epic → 404 on both verbs.
        for uri in ["/epics/01JZZNOPE/document", "/epics/01JZZNOPE/document/sync"] {
            let request = if uri.ends_with("sync") {
                post_json_bearer(uri, &token, json!({ "html": "x", "base_version": 0 }))
            } else {
                get_bearer(uri, &token)
            };
            let response = app.clone().oneshot(request).await.unwrap();
            assert_eq!(response.status(), StatusCode::NOT_FOUND);
        }

        // Missing html / base_version → 400; negative base_version → 400.
        for body in [
            json!({ "base_version": 0 }),
            json!({ "html": "x" }),
            json!({ "html": "x", "base_version": -1 }),
        ] {
            let (status, _) = sync(&app, &token, &epic_id, body).await;
            assert_eq!(status, StatusCode::BAD_REQUEST);
        }
    }

    // ---- AC: the scoped capability token round-trips its own epic only ------

    #[tokio::test]
    async fn a_scoped_capability_token_round_trips_its_own_epics_document() {
        let (state, app) = boot().await;
        let (project_id, epic_id) = seed_epic(&state).await;
        let (_other_project, other_epic) = seed_epic(&state).await;
        let guard = state.caps.mint(
            epic_id.clone(),
            project_id.clone(),
            "grilling".into(),
            PathBuf::from("/tmp"),
        );
        let token = guard.token().to_string();

        // Pull (GET) + sync on its own epic.
        let (status, doc) = get_document(&app, &token, &epic_id).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(doc["version"], 0);

        let (status, view) = sync(
            &app,
            &token,
            &epic_id,
            json!({ "html": "<h1 id=\"d\">D</h1>", "base_version": 0 }),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(view["version"], 1);
        // An agent run has no user id: attribution is NULL, provenance carries
        // the node.
        assert_eq!(view["last_edited_by"], Value::Null);

        // The other epic is out of reach (403 from the allow-list).
        let response = app
            .clone()
            .oneshot(get_bearer(&format!("/epics/{other_epic}/document"), &token))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::FORBIDDEN);
        let (status, _) = sync(
            &app,
            &token,
            &other_epic,
            json!({ "html": "hostile", "base_version": 0 }),
        )
        .await;
        assert_eq!(status, StatusCode::FORBIDDEN);
    }

    // ---- section extraction --------------------------------------------------

    #[test]
    fn extracts_anchors_in_document_order_with_heading_titles() {
        let html = "<!doctype html><!-- id=\"ghost\" -->\
                    <h1 id=\"top\">The Plan</h1>\
                    <H2 ID='store'>Which store</H2>\
                    <p>text <a href=\"?id=x\">link</a></p>\
                    <div id=risks class=\"chip\">r</div>\
                    <h3 id=\"top\">dup ignored</h3>\
                    <section id=\"empty-title\"><h4></h4></section>";
        let sections = extract_sections(html);
        let ids: Vec<&str> = sections.iter().map(|s| s.section_id.as_str()).collect();
        assert_eq!(ids, vec!["top", "store", "risks", "empty-title"]);
        assert_eq!(sections[0].title.as_deref(), Some("The Plan"));
        assert_eq!(sections[1].title.as_deref(), Some("Which store"));
        assert_eq!(sections[2].title, None);
        // A heading whose text is empty/only markup has no title.
        assert_eq!(sections[3].title, None);
    }
}
