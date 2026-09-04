//! The grilling resolution bundle — one call that resolves a decision AND
//! reshapes the map (wayfinder epic §6).
//!
//! A grilling (or prototype) session ends by resolving its decision node. That
//! resolution is not just a state flip: it is the map-building act, doing up
//! to five things **in one session**:
//!
//! 1. **Record the decision** — set `gist`, mark the node `resolved`.
//! 2. **Edit the living Document** — the agent pulls the document to a scratch
//!    file (`dearborn document pull`), makes surgical edits with its native
//!    file tools, and the resolution **folds the sync in** (`document.sync`
//!    under the per-epic write semaphore, base-version checked — siblings
//!    never stall behind anything but the bounded critical section, §7).
//! 2½. **Ship the prototype artifact** (prototype nodes only) — the throwaway
//!    artifact the session built in its scratch workspace (never a
//!    target-repo clone) is stored as a `node_asset` **linked from the node**
//!    ([`crate::node_asset`], §4.7) and rendered client-side in a sandboxed
//!    iframe.
//! 3. **Graduate fog into new frontier nodes** — each graduated node is
//!    created open and **blocked by this node** (the graduation shape); since
//!    a resolved blocker is settled, the new layer lands directly on the
//!    frontier. `trim_fog` rewrites `epic.not_yet_specified` in the same call.
//! 4. **Rule things out of scope** — create + immediately close an
//!    `out_of_scope` node (with its reason) and append the one-line prose to
//!    `epic.out_of_scope`.
//! 5. **Invalidate/update affected nodes** — partial updates to other nodes
//!    the decision changed (re-question, rule out, settle a dependent).
//!
//! ## AFK kinds are barred
//!
//! Map reshaping is **HITL-only** (epic §6): only `grilling` and `prototype`
//! nodes may resolve through this surface. Research/AFK-task nodes report
//! facts and never redraw the map — they are rejected here with `409`, and
//! the capability-token allow-list ([`crate::capability`]) independently
//! refuses map-mutating routes to any token not minted for a HITL phase, so
//! an unattended run cannot reach this endpoint even if it somehow gained a
//! token.
//!
//! ## Atomicity ("atomically enough that siblings don't stall", §7)
//!
//! Every part of the bundle is validated **before** anything is written; the
//! document sync (the only step that takes the per-epic semaphore) runs
//! first, so a stale base version is a clean `409` with the map untouched —
//! the agent re-pulls, re-edits, and retries the whole resolution. The map
//! mutations themselves are small bounded writes. Both `map_updated` and
//! `document_updated` fan out on `epic:<id>` so subscribed clients re-render
//! the whole resolved state.
//!
//! The REST surface is the `dearborn` CLI's (upgraded) `node resolve` verb.

use axum::{
    extract::{Path, State},
    Json,
};
use serde::Deserialize;
use serde_json::{json, Value};

use crate::map::Actor;
use crate::{map, node_engine, AppError, AppResult, AppState};

/// `POST /epics/{id}/map-nodes/{nodeId}/resolve` body — the resolution
/// bundle. Every part is optional; absent parts are simply not performed.
/// Field semantics live on the sub-structs below.
#[derive(Debug, Default, Deserialize)]
pub struct ResolveNodeBody {
    /// The one-line decision (stored on the node's `gist`).
    #[serde(default)]
    gist: Option<String>,
    /// The folded document edit: the edited HTML plus the version it was
    /// read at (exactly what `document sync` carries).
    #[serde(default)]
    document: Option<ResolveDocumentBody>,
    /// The shipped prototype artifact (`data_base64` + `mime` + optional
    /// `label`) — stored as a `node_asset` linked from the node. Prototype
    /// nodes only; a grilling resolution carries no artifact.
    #[serde(default)]
    artifact: Option<ResolveArtifactBody>,
    /// New frontier nodes to graduate out of the fog, each blocked by this
    /// node.
    #[serde(default)]
    graduations: Vec<GraduateBody>,
    /// The replacement `epic.not_yet_specified` prose (empty clears it).
    #[serde(default)]
    trim_fog: Option<String>,
    /// Things this decision rules beyond the destination: each becomes a
    /// created-and-closed `out_of_scope` node plus a prose line.
    #[serde(default)]
    out_of_scope: Vec<OutOfScopeBody>,
    /// Partial updates to other nodes affected by this decision.
    #[serde(default)]
    updates: Vec<UpdateBody>,
}

/// The folded document edit: `html` (required) committed as a new version on
/// top of `base_version` (required, `0` before the first sync) — a stale base
/// is a clean `409` for re-read/retry with the map untouched.
#[derive(Debug, Deserialize)]
pub struct ResolveDocumentBody {
    html: Option<String>,
    base_version: Option<i64>,
}

/// The shipped prototype artifact (wayfinder epic §4.7): the scratch
/// workspace file, base64-encoded by the CLI (`--artifact PATH`), with its
/// `mime` (default `text/html`) and an optional human `label` (the file name
/// works well). Stored as a `node_asset` **linked from the node** — never
/// inlined into the map or the transcript.
#[derive(Debug, Deserialize)]
pub struct ResolveArtifactBody {
    data_base64: Option<String>,
    mime: Option<String>,
    #[serde(default)]
    label: Option<String>,
}

/// One fog graduation: a new node, created open and blocked by the resolving
/// node, landing directly on the frontier. `task_mode` follows the usual
/// create rules (required for `kind = "task"`, rejected elsewhere).
#[derive(Debug, Deserialize)]
pub struct GraduateBody {
    kind: Option<String>,
    title: Option<String>,
    #[serde(default)]
    question: Option<String>,
    #[serde(default)]
    task_mode: Option<String>,
}

/// One out-of-scope ruling: a node (kind defaults to `grilling`) created and
/// immediately closed with its `reason`, plus a one-line append to
/// `epic.out_of_scope`.
#[derive(Debug, Deserialize)]
pub struct OutOfScopeBody {
    title: Option<String>,
    #[serde(default)]
    kind: Option<String>,
    reason: Option<String>,
}

/// One partial update to another node affected by this decision (the
/// invalidate/update act). `id` is required; every other field follows the
/// `PATCH /map-nodes/{id}` semantics (state must be in the §4.1 vocabulary;
/// prose fields trim, empty → `NULL`).
#[derive(Debug, Deserialize)]
pub struct UpdateBody {
    id: Option<String>,
    #[serde(default)]
    state: Option<String>,
    #[serde(default)]
    title: Option<String>,
    #[serde(default)]
    question: Option<String>,
    #[serde(default)]
    gist: Option<String>,
    #[serde(default)]
    out_of_scope_reason: Option<String>,
}

/// `POST /epics/{id}/map-nodes/{nodeId}/resolve` — resolve a grilling/prototype
/// node and reshape the map in one call. `200` with the resolution outcome
/// (the resolved node, the new document version, the created/ruled-out/updated
/// nodes, and the full recomputed map). `404` unknown epic/node; `409` for a
/// non-HITL kind (research/task never reshape the map) or a stale document
/// base version — the latter with nothing applied, a clean re-read/retry.
pub async fn resolve_node(
    State(state): State<AppState>,
    Path((id, node_id)): Path<(String, String)>,
    actor: Actor,
    Json(req): Json<ResolveNodeBody>,
) -> AppResult<Json<Value>> {
    let conn = state.db.conn();
    let node = map::fetch_node(&conn, &node_id)
        .await?
        .filter(|node| node.epic_id == id)
        .ok_or_else(|| AppError::NotFound(format!("map node {node_id} not found")))?;

    // Map reshaping is HITL-only (§6): research and task nodes report facts /
    // record manual work; they never resolve through this surface, and their
    // runs are never granted a CLI that could.
    if !map::MAP_RESHAPING_KINDS.contains(&node.kind.as_str()) {
        return Err(AppError::Conflict(format!(
            "map node {node_id} is a `{}` node, which cannot reshape the map — \
             only {} nodes resolve (AFK kinds report facts only)",
            node.kind,
            map::MAP_RESHAPING_KINDS.join(" / ")
        )));
    }

    let ResolveNodeBody {
        gist,
        document,
        artifact,
        graduations,
        trim_fog,
        out_of_scope,
        updates,
    } = req;

    // ---- validate EVERYTHING before writing anything -----------------------
    // The bundle must not strand half an application: a rejection here leaves
    // both the map and the Document exactly as they were.
    let document = match document {
        Some(doc) => {
            let html = doc
                .html
                .ok_or_else(|| AppError::BadRequest("`document.html` is required".to_string()))?;
            let base_version = doc.base_version.ok_or_else(|| {
                AppError::BadRequest(
                    "`document.base_version` is required (the version you read)".to_string(),
                )
            })?;
            if base_version < 0 {
                return Err(AppError::BadRequest(
                    "`document.base_version` must not be negative".to_string(),
                ));
            }
            Some((html, base_version))
        }
        None => None,
    };
    let mut validated_graduations = Vec::with_capacity(graduations.len());
    for graduation in graduations {
        let kind = graduation
            .kind
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .ok_or_else(|| AppError::BadRequest("`graduations[].kind` is required".to_string()))?;
        map::validate_kind(kind)?;
        let title = graduation
            .title
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .ok_or_else(|| {
                AppError::BadRequest(
                    "`graduations[].title` is required and must not be empty".to_string(),
                )
            })?;
        let task_mode = map::validate_task_mode(kind, graduation.task_mode.as_deref())?;
        let question = graduation
            .question
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(str::to_string);
        validated_graduations.push((kind.to_string(), title.to_string(), question, task_mode));
    }
    let mut validated_out_of_scope = Vec::with_capacity(out_of_scope.len());
    for ruling in out_of_scope {
        let title = ruling
            .title
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .ok_or_else(|| {
                AppError::BadRequest(
                    "`out_of_scope[].title` is required and must not be empty".to_string(),
                )
            })?;
        let reason = ruling
            .reason
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .ok_or_else(|| {
                AppError::BadRequest(
                    "`out_of_scope[].reason` is required — an out-of-scope ruling states why"
                        .to_string(),
                )
            })?;
        let kind = ruling
            .kind
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .unwrap_or("grilling");
        map::validate_kind(kind)?;
        let task_mode = map::validate_task_mode(kind, None)?;
        validated_out_of_scope.push((kind.to_string(), task_mode, title.to_string(), reason.to_string()));
    }
    let mut validated_updates: Vec<(String, map::UpdateMapNodeBody)> =
        Vec::with_capacity(updates.len());
    for update in updates {
        let update_id = update
            .id
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .ok_or_else(|| AppError::BadRequest("`updates[].id` is required".to_string()))?;
        if !map::node_belongs_to_epic(&conn, update_id, &id).await? {
            return Err(AppError::BadRequest(format!(
                "map node {update_id} is not part of epic {id}"
            )));
        }
        if let Some(state_name) = update.state.as_deref() {
            if !map::VALID_STATES.contains(&state_name) {
                return Err(AppError::BadRequest(format!(
                    "`state` must be one of open|in_progress|resolved|out_of_scope, got `{state_name}`"
                )));
            }
        }
        validated_updates.push((
            update_id.to_string(),
            map::UpdateMapNodeBody {
                state: update.state,
                title: update.title,
                question: update.question,
                gist: update.gist,
                out_of_scope_reason: update.out_of_scope_reason,
                position_x: None,
                position_y: None,
            },
        ));
    }
    // The shipped prototype artifact (§4.7): validated like everything else
    // BEFORE any write, and prototype-only — a grilling resolution carries no
    // artifact (its deliverable is a decision, not a build).
    let artifact = match artifact {
        Some(art) => {
            if node.kind != "prototype" {
                return Err(AppError::BadRequest(format!(
                    "map node {node_id} is a `{}` node: only prototype resolutions ship an \\
                     artifact (a grilling decision is a decision, not a build)",
                    node.kind
                )));
            }
            let data_base64 = art
                .data_base64
                .as_deref()
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .ok_or_else(|| {
                    AppError::BadRequest(
                        "`artifact.data_base64` is required (the scratch file, base64-encoded)"
                            .to_string(),
                    )
                })?;
            let bytes = crate::node_asset::decode_artifact_bytes(data_base64)?;
            let mime = crate::node_asset::validate_mime(art.mime.as_deref())?;
            let label = art
                .label
                .as_deref()
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .map(str::to_string);
            Some((mime, bytes, label))
        }
        None => None,
    };

    // ---- apply: document first (the only semaphore-taking step) ------------
    // A stale base version fails here as a bare 409 with the map untouched —
    // the agent re-pulls, re-edits, and retries the whole resolution.
    let document_payload = match document {
        Some((html, base_version)) => {
            let view = crate::document::sync_under_semaphore(
                &state,
                &id,
                &html,
                base_version,
                actor.user_id.as_deref(),
                Some(&node_id),
            )
            .await?;
            Some(json!({
                "version": view.version,
                "updated_at": view.updated_at,
                "sections": view.sections,
            }))
        }
        None => None,
    };

    // 1. Record the decision (state flip + one-line gist, attributed).
    let node = map::update_node(
        &conn,
        &node_id,
        map::UpdateMapNodeBody {
            state: Some("resolved".to_string()),
            gist,
            ..Default::default()
        },
        actor.user_id.as_deref(),
    )
    .await?;
    // The node's interactive session (if any) is done: mark it complete.
    node_engine::mark_session_complete(&conn, &node_id).await?;

    // 1½. Ship the prototype artifact: stored as a `node_asset` linked from
    //     the node (wayfinder epic §4.7), from where the client renders it in
    //     a sandboxed iframe. (The scratch workspace file itself stays
    //     throwaway — the store is the durable copy.)
    let asset_payload = match artifact {
        Some((mime, bytes, label)) => {
            let meta = crate::node_asset::insert_asset(
                &conn,
                &node_id,
                &mime,
                bytes,
                label.as_deref(),
                actor.user_id.as_deref(),
            )
            .await?;
            Some(serde_json::to_value(&meta).unwrap_or(Value::Null))
        }
        None => None,
    };

    // 2. Graduate fog into the next frontier layer: each new node is open and
    //    blocked by THIS node — a settled blocker, so the layer is on the
    //    frontier immediately (§6: "this is how the map grows").
    let mut created = Vec::with_capacity(validated_graduations.len());
    for (kind, title, question, task_mode) in validated_graduations {
        let new_node = map::create_node(
            &conn,
            &id,
            &kind,
            task_mode.as_deref(),
            &title,
            question.as_deref(),
            actor.user_id.as_deref(),
            None,
            None,
        )
        .await?;
        map::link_nodes(&conn, &node_id, &new_node.id, actor.user_id.as_deref()).await?;
        created.push(new_node);
    }

    // 3. Rule things out of scope: create + immediately close an out_of_scope
    //    node carrying its reason, and append the one-line prose.
    let mut ruled_out = Vec::with_capacity(validated_out_of_scope.len());
    for (kind, task_mode, title, reason) in validated_out_of_scope {
        let oos_node = map::create_node(
            &conn,
            &id,
            &kind,
            task_mode.as_deref(),
            &title,
            None,
            actor.user_id.as_deref(),
            None,
            None,
        )
        .await?;
        let oos_node = map::update_node(
            &conn,
            &oos_node.id,
            map::UpdateMapNodeBody {
                state: Some("out_of_scope".to_string()),
                out_of_scope_reason: Some(reason.clone()),
                ..Default::default()
            },
            actor.user_id.as_deref(),
        )
        .await?;
        map::append_out_of_scope_prose(&conn, &id, &reason, actor.user_id.as_deref()).await?;
        ruled_out.push(oos_node);
    }

    // 4. Invalidate/update the other nodes this decision changed.
    let mut updated = Vec::with_capacity(validated_updates.len());
    for (update_id, patch) in validated_updates {
        updated.push(map::update_node(&conn, &update_id, patch, actor.user_id.as_deref()).await?);
    }

    // 5. Trim the fog: the graduated decisions leave `not_yet_specified`.
    if let Some(fog) = trim_fog {
        map::update_prose(
            &conn,
            &id,
            map::UpdateMapProseBody {
                not_yet_specified: Some(fog),
                ..Default::default()
            },
            actor.user_id.as_deref(),
        )
        .await?;
    }

    // Fan the whole resolved state out: subscribed clients re-render the map
    // (the document sync already published its own `document_updated` frame).
    map::publish_map(&state, &id).await;
    let computed = map::compute_map(&conn, &id).await?;

    Ok(Json(json!({
        "node": node,
        "document": document_payload,
        "asset": asset_payload,
        "created": created,
        "out_of_scope": ruled_out,
        "updated": updated,
        "map": computed,
    })))
}

// ---- tests ------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use base64::Engine as _;
    use crate::capability::now_ms;
    use crate::users::{self, Role};
    use crate::{app, AppState, Config, Db};
    use axum::body::Body;
    use axum::http::{header::AUTHORIZATION, Request, StatusCode};
    use serde_json::Value;
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
            "INSERT INTO epic (id, project_id, title, status, destination, not_yet_specified, created_at, updated_at) \
             VALUES (?1, ?2, 'E', 'Planning', 'It works end to end', 'Which events export; retention', ?3, ?3)",
            libsql::params![epic_id.clone(), project_id.clone(), now],
        )
        .await
        .unwrap();
        (project_id, epic_id)
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

    /// Seed a node directly through the map store; return its id.
    async fn seed_node(state: &AppState, epic_id: &str, kind: &str, task_mode: Option<&str>) -> String {
        map::create_node(
            state.db.conn(),
            epic_id,
            kind,
            task_mode,
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

    async fn resolve(
        app: &axum::Router,
        token: &str,
        epic_id: &str,
        node_id: &str,
        body: Value,
    ) -> (StatusCode, Value) {
        let response = app
            .clone()
            .oneshot(post_json_bearer(
                &format!("/epics/{epic_id}/map-nodes/{node_id}/resolve"),
                token,
                body,
            ))
            .await
            .unwrap();
        let status = response.status();
        (status, body_json(response).await)
    }

    async fn get_map(app: &axum::Router, token: &str, epic_id: &str) -> Value {
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(&format!("/epics/{epic_id}/map"))
                    .header(AUTHORIZATION, format!("Bearer {token}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        body_json(response).await
    }

    async fn get_document(app: &axum::Router, token: &str, epic_id: &str) -> Value {
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(&format!("/epics/{epic_id}/document"))
                    .header(AUTHORIZATION, format!("Bearer {token}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        body_json(response).await
    }

    fn node_of<'v>(value: &'v Value, id: &str) -> &'v Value {
        value["nodes"]
            .as_array()
            .unwrap()
            .iter()
            .find(|n| n["id"].as_str() == Some(id))
            .unwrap()
    }

    // ---- AC: one resolution records the decision, edits the Document,
    //          graduates the next frontier, and rules something out ---------

    #[tokio::test]
    async fn one_resolution_records_edits_graduates_and_rules_out() {
        let (state, app) = boot().await;
        let user = users::testing::seed_user(&state, "planner", Role::Admin, true).await;
        let token = crate::sessions::testing::login_as(&state, &user).await;
        let (_project_id, epic_id) = seed_epic(&state).await;
        let node_id = seed_node(&state, &epic_id, "grilling", None).await;

        // The agent's folded document edit: pulled at version 0, edited
        // surgically, committed with the resolution.
        let html = "<h1 id=\"blob-store\">Blob store</h1><p>Use the evidence blob store.</p>";

        let (status, outcome) = resolve(
            &app,
            &token,
            &epic_id,
            &node_id,
            json!({
                "gist": "Use the evidence blob store",
                "document": { "html": html, "base_version": 0 },
                "graduations": [
                    { "kind": "grilling", "title": "Which events export?", "question": "Scope the export surface" },
                    { "kind": "research", "title": "Survey libsql blob support" }
                ],
                "trim_fog": "Retention policy",
                "out_of_scope": [
                    { "title": "Multi-region replication", "reason": "Beyond the destination: single-region only" }
                ]
            }),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "outcome: {outcome}");

        // 1. The decision is recorded, attributed to the human who resolved it.
        assert_eq!(outcome["node"]["state"], "resolved");
        assert_eq!(outcome["node"]["gist"], "Use the evidence blob store");
        assert_eq!(outcome["node"]["resolved_by"], user.id.as_str());

        // 2. The Document gained a version whose sections carry this node's
        //    provenance.
        assert_eq!(outcome["document"]["version"], 1);
        let doc = get_document(&app, &token, &epic_id).await;
        assert_eq!(doc["html"], html);
        assert_eq!(doc["version"], 1);
        assert_eq!(doc["sections"][0]["section_id"], "blob-store");
        assert_eq!(doc["sections"][0]["provenance"], node_id.as_str());

        // 3. The graduated nodes exist, are blocked by the resolved node, and
        //    are on the frontier immediately (their only blocker is settled).
        let created = outcome["created"].as_array().unwrap();
        assert_eq!(created.len(), 2);
        let map = get_map(&app, &token, &epic_id).await;
        for graduated in created {
            let id = graduated["id"].as_str().unwrap();
            assert_eq!(node_of(&map, id)["state"], "open");
            // The graduation edge exists; because the resolved blocker is
            // settled, the node is ON the frontier (blocked_by only lists
            // *unsettled* blockers).
            assert_eq!(node_of(&map, id)["frontier"], true);
            assert_eq!(node_of(&map, id)["blocked_by"], json!([]));
        }
        let edges = map["edges"].as_array().unwrap();
        assert_eq!(edges.len(), 2);
        assert!(edges
            .iter()
            .all(|e| e["blocker_id"] == node_id.as_str()));

        // The fog was trimmed: only what was never graduated remains.
        assert_eq!(map["not_yet_specified"], "Retention policy");

        // 4. The out-of-scope ruling created a closed node + a prose line.
        let ruled_out = outcome["out_of_scope"].as_array().unwrap();
        assert_eq!(ruled_out.len(), 1);
        let oos_id = ruled_out[0]["id"].as_str().unwrap();
        assert_eq!(node_of(&map, oos_id)["state"], "out_of_scope");
        assert_eq!(
            node_of(&map, oos_id)["out_of_scope_reason"],
            "Beyond the destination: single-region only"
        );
        assert_eq!(
            map["out_of_scope"],
            "Beyond the destination: single-region only"
        );

        // The full recomputed map came back with the response.
        assert_eq!(outcome["map"]["nodes"].as_array().unwrap().len(), 4);
    }

    // ---- AC: AFK kinds are barred from map reshaping ------------------------

    #[tokio::test]
    async fn research_and_task_nodes_cannot_resolve_or_reshape() {
        let (state, app) = boot().await;
        let user = users::testing::seed_user(&state, "planner", Role::Admin, true).await;
        let token = crate::sessions::testing::login_as(&state, &user).await;
        let (_project_id, epic_id) = seed_epic(&state).await;
        let research_id = seed_node(&state, &epic_id, "research", None).await;
        let task_id = seed_node(&state, &epic_id, "task", Some("afk")).await;

        for node_id in [&research_id, &task_id] {
            let (status, outcome) = resolve(
                &app,
                &token,
                &epic_id,
                node_id,
                json!({ "gist": "fact", "graduations": [ { "kind": "grilling", "title": "X" } ] }),
            )
            .await;
            assert_eq!(status, StatusCode::CONFLICT);
            assert!(
                outcome["error"]["message"]
                    .as_str()
                    .unwrap()
                    .contains("cannot reshape the map"),
                "rejection names the rule: {outcome}"
            );

            // Nothing was written: no node was resolved or created.
            let map = get_map(&app, &token, &epic_id).await;
            assert_eq!(map["nodes"].as_array().unwrap().len(), 2);
            assert!(map["nodes"]
                .as_array()
                .unwrap()
                .iter()
                .all(|n| n["state"] == "open"));
        }
    }

    // ---- AC: a stale document base fails cleanly with the map untouched ----

    #[tokio::test]
    async fn a_stale_document_base_version_is_a_clean_409_with_nothing_applied() {
        let (state, app) = boot().await;
        let user = users::testing::seed_user(&state, "planner", Role::Admin, true).await;
        let token = crate::sessions::testing::login_as(&state, &user).await;
        let (_project_id, epic_id) = seed_epic(&state).await;
        let node_id = seed_node(&state, &epic_id, "grilling", None).await;

        // Land version 1 out from under the resolution — via the standalone
        // document sync, so the node itself is untouched — making the
        // resolution's base of 0 stale by the time it lands.
        let v1 = app
            .clone()
            .oneshot(post_json_bearer(
                &format!("/epics/{epic_id}/document/sync"),
                &token,
                json!({ "html": "<p>v1</p>", "base_version": 0 }),
            ))
            .await
            .unwrap();
        assert_eq!(v1.status(), StatusCode::OK);

        let (status, outcome) = resolve(
            &app,
            &token,
            &epic_id,
            &node_id,
            json!({
                "gist": "Use the evidence blob store",
                "document": { "html": "<p>v2</p>", "base_version": 0 }
            }),
        )
        .await;
        assert_eq!(status, StatusCode::CONFLICT);
        assert!(
            outcome["error"]["message"]
                .as_str()
                .unwrap()
                .contains("current version is 1"),
            "the stale-base message names the current version: {outcome}"
        );

        // Nothing was applied: the node is still open, no version 2 exists.
        let node = map::fetch_node(state.db.conn(), &node_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(node.state, "open");
        assert_eq!(node.gist, None);
        let doc = get_document(&app, &token, &epic_id).await;
        assert_eq!(doc["version"], 1);
        assert_eq!(doc["html"], "<p>v1</p>");

        // The retry with the fresh base closes the round trip.
        let (status, outcome) = resolve(
            &app,
            &token,
            &epic_id,
            &node_id,
            json!({
                "gist": "Use the evidence blob store",
                "document": { "html": "<p id=\"dec\">v2</p>", "base_version": 1 }
            }),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(outcome["document"]["version"], 2);
        assert_eq!(outcome["node"]["state"], "resolved");
    }

    // ---- AC: invalidating/updating affected nodes --------------------------

    #[tokio::test]
    async fn a_resolution_can_invalidate_and_update_other_nodes() {
        let (state, app) = boot().await;
        let user = users::testing::seed_user(&state, "planner", Role::Admin, true).await;
        let token = crate::sessions::testing::login_as(&state, &user).await;
        let (_project_id, epic_id) = seed_epic(&state).await;
        let node_id = seed_node(&state, &epic_id, "grilling", None).await;
        let other_id = seed_node(&state, &epic_id, "grilling", None).await;

        let (status, outcome) = resolve(
            &app,
            &token,
            &epic_id,
            &node_id,
            json!({
                "gist": "Single-region only",
                "updates": [
                    { "id": other_id, "question": "What does single-region retention look like?" },
                    { "id": other_id, "state": "out_of_scope", "out_of_scope_reason": "Superseded by the storage decision" }
                ]
            }),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "outcome: {outcome}");
        let updated = outcome["updated"].as_array().unwrap();
        assert_eq!(updated.len(), 2);
        assert_eq!(
            updated[0]["question"],
            "What does single-region retention look like?"
        );
        assert_eq!(updated[1]["state"], "out_of_scope");

        let map = get_map(&app, &token, &epic_id).await;
        assert_eq!(
            node_of(&map, &other_id)["out_of_scope_reason"],
            "Superseded by the storage decision"
        );
    }

    // ---- validation: nothing is written when any part is malformed ---------

    #[tokio::test]
    async fn an_invalid_bundle_writes_nothing() {
        let (state, app) = boot().await;
        let user = users::testing::seed_user(&state, "planner", Role::Admin, true).await;
        let token = crate::sessions::testing::login_as(&state, &user).await;
        let (_project_id, epic_id) = seed_epic(&state).await;
        let node_id = seed_node(&state, &epic_id, "grilling", None).await;

        let cases: Vec<(&str, Value)> = vec![
            (
                "bad graduation kind",
                json!({ "graduations": [ { "kind": "charting", "title": "X" } ] }),
            ),
            (
                "task graduation without task_mode",
                json!({ "graduations": [ { "kind": "task", "title": "X" } ] }),
            ),
            (
                "blank graduation title",
                json!({ "graduations": [ { "kind": "grilling", "title": "  " } ] }),
            ),
            (
                "out-of-scope without a reason",
                json!({ "out_of_scope": [ { "title": "Multi-region" } ] }),
            ),
            (
                "update naming a node outside the epic",
                json!({ "updates": [ { "id": "01JZZNOPE" } ] }),
            ),
            (
                "update with an invalid state",
                json!({ "updates": [ { "id": node_id, "state": "finished" } ] }),
            ),
            (
                "negative document base version",
                json!({ "document": { "html": "<p>x</p>", "base_version": -1 } }),
            ),
        ];
        for (name, body) in cases {
            let (status, outcome) = resolve(&app, &token, &epic_id, &node_id, body).await;
            assert_eq!(status, StatusCode::BAD_REQUEST, "{name}: {outcome}");
            let node = map::fetch_node(state.db.conn(), &node_id)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(node.state, "open", "{name}: nothing may be written");
        }
    }

    // ---- guards: unknown epic/node -----------------------------------------

    #[tokio::test]
    async fn unknown_nodes_are_404() {
        let (state, app) = boot().await;
        let user = users::testing::seed_user(&state, "planner", Role::Admin, true).await;
        let token = crate::sessions::testing::login_as(&state, &user).await;
        let (_project_id, epic_id) = seed_epic(&state).await;
        let node_id = seed_node(&state, &epic_id, "grilling", None).await;

        // Unknown node → 404.
        let (status, _) = resolve(&app, &token, &epic_id, "01JZZNOPE", json!({})).await;
        assert_eq!(status, StatusCode::NOT_FOUND);

        // This node under another epic's path → 404 (no leak).
        let (_other_project, other_epic) = seed_epic(&state).await;
        let (status, _) = resolve(&app, &token, &other_epic, &node_id, json!({})).await;
        assert_eq!(status, StatusCode::NOT_FOUND);
    }

    // ---- AC: the resolution completes the node's interactive session -------

    #[tokio::test]
    async fn resolving_completes_the_nodes_session() {
        let (state, app) = boot().await;
        let user = users::testing::seed_user(&state, "planner", Role::Admin, true).await;
        let token = crate::sessions::testing::login_as(&state, &user).await;
        let (_project_id, epic_id) = seed_epic(&state).await;
        let node_id = seed_node(&state, &epic_id, "grilling", None).await;

        // Open the node's session first (the engine's soft in_progress signal).
        let open = app
            .clone()
            .oneshot(post_json_bearer(
                &format!("/epics/{epic_id}/map-nodes/{node_id}/session"),
                &token,
                json!({}),
            ))
            .await
            .unwrap();
        assert_eq!(open.status(), StatusCode::OK);

        let (status, _) = resolve(
            &app,
            &token,
            &epic_id,
            &node_id,
            json!({ "gist": "done" }),
        )
        .await;
        assert_eq!(status, StatusCode::OK);

        let session = node_engine::fetch_session(state.db.conn(), &node_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(session.status, "complete");
        let node = map::fetch_node(state.db.conn(), &node_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(node.state, "resolved");
    }

    // ---- AC: a capability token minted for an AFK phase cannot reach it ----

    #[tokio::test]
    async fn an_afk_phase_capability_token_is_403_on_the_resolution_surface() {
        let (state, app) = boot().await;
        let user = users::testing::seed_user(&state, "planner", Role::Admin, true).await;
        let token = crate::sessions::testing::login_as(&state, &user).await;
        let (project_id, epic_id) = seed_epic(&state).await;
        let node_id = seed_node(&state, &epic_id, "grilling", None).await;

        // A research run's token (hypothetically leaked) must not reshape the
        // map: every map-mutating route is 403 for a non-HITL phase.
        let cap = state.caps.mint(
            epic_id.clone(),
            project_id.clone(),
            "research".into(),
            std::path::PathBuf::from("/tmp"),
        );
        let (status, _) = resolve(
            &app,
            cap.token(),
            &epic_id,
            &node_id,
            json!({ "gist": "x" }),
        )
        .await;
        assert_eq!(status, StatusCode::FORBIDDEN);

        // The map is untouched.
        let map = get_map(&app, &token, &epic_id).await;
        assert_eq!(node_of(&map, &node_id)["state"], "open");
    }

    // ---- AC: resolving a prototype node stores its artifact as a
    //         node_asset linked from the node ----------------------------

    #[tokio::test]
    async fn resolving_a_prototype_node_stores_a_linked_node_asset() {
        let (state, app) = boot().await;
        let user = users::testing::seed_user(&state, "planner", Role::Admin, true).await;
        let token = crate::sessions::testing::login_as(&state, &user).await;
        let (_project_id, epic_id) = seed_epic(&state).await;
        let node_id = seed_node(&state, &epic_id, "prototype", None).await;

        let artifact_html = "<h1>State machine probe</h1><button>next</button>";
        let encoded = base64::engine::general_purpose::STANDARD.encode(artifact_html);
        let (status, outcome) = resolve(
            &app,
            &token,
            &epic_id,
            &node_id,
            json!({
                "gist": "The list-based state model feels right",
                "artifact": { "data_base64": encoded, "label": "index.html" }
            }),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "outcome: {outcome}");
        assert_eq!(outcome["node"]["state"], "resolved");

        // The response names the stored asset; the store lists it under the
        // node (metadata only — linked, not inlined).
        assert_eq!(outcome["asset"]["label"], "index.html");
        assert_eq!(outcome["asset"]["mime"], "text/html");
        let asset_id = outcome["asset"]["id"].as_str().unwrap().to_string();
        let listed = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(&format!(
                        "/epics/{epic_id}/map-nodes/{node_id}/assets"
                    ))
                    .header(AUTHORIZATION, format!("Bearer {token}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(listed.status(), StatusCode::OK);
        let listed = body_json(listed).await;
        let items = listed["items"].as_array().unwrap();
        assert_eq!(items.len(), 1);
        assert_eq!(items[0]["id"], asset_id.as_str());
        assert_eq!(items[0]["node_id"], node_id.as_str());

        // And the bytes read back raw, with the (default) text/html type —
        // exactly what the client feeds its sandboxed iframe.
        let raw = app
            .oneshot(
                Request::builder()
                    .uri(&format!(
                        "/epics/{epic_id}/map-nodes/{node_id}/assets/{asset_id}"
                    ))
                    .header(AUTHORIZATION, format!("Bearer {token}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(raw.status(), StatusCode::OK);
        assert_eq!(raw.headers()["content-type"], "text/html");
        let bytes = axum::body::to_bytes(raw.into_body(), usize::MAX)
            .await
            .unwrap();
        assert_eq!(&bytes[..], artifact_html.as_bytes());
    }

    #[tokio::test]
    async fn a_grilling_resolution_cannot_ship_an_artifact() {
        let (state, app) = boot().await;
        let user = users::testing::seed_user(&state, "planner", Role::Admin, true).await;
        let token = crate::sessions::testing::login_as(&state, &user).await;
        let (_project_id, epic_id) = seed_epic(&state).await;
        let node_id = seed_node(&state, &epic_id, "grilling", None).await;

        let encoded = base64::engine::general_purpose::STANDARD.encode("<h1>x</h1>");
        let (status, outcome) = resolve(
            &app,
            &token,
            &epic_id,
            &node_id,
            json!({ "gist": "decided", "artifact": { "data_base64": encoded } }),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "outcome: {outcome}");
        assert!(outcome["error"]["message"]
            .as_str()
            .unwrap()
            .contains("only prototype resolutions"));

        // Nothing was applied: the node is still open and nothing stored.
        let node = map::fetch_node(state.db.conn(), &node_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(node.state, "open");
        assert_eq!(
            crate::node_asset::list_assets(state.db.conn(), &node_id)
                .await
                .unwrap()
                .len(),
            0
        );
    }
}
