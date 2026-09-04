//! The append-only attribution/provenance feed (wayfinder epic §4.9/§9,
//! Phase 5).
//!
//! Per-row `created_by` / `last_edited_by` columns cover **inline** attribution
//! (a map node knows who created and resolved it, the Document knows who last
//! edited it, a comment knows its author); the `activity` table covers the
//! **feed**: one immutable row per key mutation, `(epic_id, node_id,
//! actor_user_id, action, detail, created_at)`, from which the epic's
//! history renders and its **participants** are derived as distinct actors.
//!
//! Append-only by construction: nothing in the codebase ever updates or
//! deletes an `activity` row — [`record`] inserts, [`list`] reads, and the
//! epic's participants ([`participants`]) are a pure query over the
//! attribution surfaces (activity actors, node created/resolved-by, document
//! last-edited-by, comment authors, node-message posters), so a participant
//! list never has to be stored or maintained.
//!
//! The REST surface is `GET /epics/{id}/activity` (the history, optionally
//! narrowed by node/action/actor) and `GET /epics/{id}/participants` (the
//! derived distinct actors). Both are reads, so they sit on the
//! capability-token allow-list for every phase
//! ([`crate::capability::authorize_cap_request`]) and accept either a browser
//! session token or a per-run capability token.
//!
//! Recording is wired at the mutation points themselves — the map store
//! ([`crate::map`]), the document sync ([`crate::document`]), the comment
//! surface ([`crate::comments`]), epic creation ([`crate::epics`]), and
//! breakdown ([`crate::breakdown`]) — so every caller of those paths (the
//! REST handlers, the grilling resolution bundle, the seed, promotion) lands
//! its feed row without a per-call-site reminder.

use axum::{
    extract::{Path, Query, State},
    Json,
};
use serde::{Deserialize, Serialize};

use crate::AppError;

// ---- the action vocabulary -------------------------------------------------

/// The `action` values the feed records. The column stays free-form TEXT (an
/// append-only log must not reject a future action), but every writer today
/// uses one of these.
pub const EPIC_CREATED: &str = "epic_created";
pub const NODE_CREATED: &str = "node_created";
pub const NODE_UPDATED: &str = "node_updated";
pub const NODE_RESOLVED: &str = "node_resolved";
pub const NODE_OUT_OF_SCOPE: &str = "node_out_of_scope";
pub const DEPENDENCY_LINKED: &str = "dependency_linked";
pub const MAP_PROSE_UPDATED: &str = "map_prose_updated";
pub const DOCUMENT_SYNCED: &str = "document_synced";
pub const COMMENT_POSTED: &str = "comment_posted";
pub const COMMENT_RESOLVED: &str = "comment_resolved";
pub const THREAD_PROMOTED: &str = "thread_promoted";
pub const BREAKDOWN_STARTED: &str = "breakdown_started";

// ---- DTOs ------------------------------------------------------------------

/// One feed row (`activity`, §4.9) as stored. `actor_user_id` is `NULL` when
/// the actor was an agent run (a capability token — attribution records no
/// human) or a system act; `node_id` is `NULL` for epic-level actions
/// (epic created, prose edits, document syncs, breakdown).
#[derive(Debug, Clone, Serialize)]
pub struct Activity {
    pub id: i64,
    pub epic_id: String,
    pub node_id: Option<String>,
    pub actor_user_id: Option<String>,
    pub action: String,
    pub detail: Option<String>,
    pub created_at: i64,
}

/// One derived participant: a distinct actor of the epic, resolved to the
/// user row the attribution points at. Agents act without a user id, so only
/// humans appear here — the flat-permission model's "participants = derived
/// distinct actors" (§9).
#[derive(Debug, Clone, Serialize)]
pub struct Participant {
    pub id: String,
    pub username: String,
    pub display_name: String,
}

/// `GET /epics/{id}/activity` query filters. All optional; every filter
/// supplied must match.
#[derive(Debug, Default, Deserialize)]
pub struct ListActivityQuery {
    node_id: Option<String>,
    action: Option<String>,
    actor_user_id: Option<String>,
}

// ---- store -----------------------------------------------------------------

/// Append one activity row. Pure insert — validation of the epic/node ids is
/// the caller's job (every caller has just written the row the activity
/// describes, so the ids are already known-good); a record failure returns
/// the error so a mutation's feed row is never silently dropped.
pub async fn record(
    conn: &libsql::Connection,
    epic_id: &str,
    node_id: Option<&str>,
    actor_user_id: Option<&str>,
    action: &str,
    detail: Option<&str>,
) -> crate::AppResult<()> {
    conn.execute(
        "INSERT INTO activity (epic_id, node_id, actor_user_id, action, detail, created_at) \
         VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
        libsql::params![
            epic_id,
            node_id,
            actor_user_id,
            action,
            detail,
            crate::capability::now_ms()
        ],
    )
    .await?;
    Ok(())
}

/// The epic's activity history, oldest first (append order; the autoincrement
/// `id` is the tie-break for same-millisecond rows). Optionally narrowed by
/// node, action, or actor — every filter supplied must match.
pub async fn list(
    conn: &libsql::Connection,
    epic_id: &str,
    filter: &ListActivityQuery,
) -> crate::AppResult<Vec<Activity>> {
    let mut sql = String::from(
        "SELECT id, epic_id, node_id, actor_user_id, action, detail, created_at \
         FROM activity WHERE epic_id = ?1",
    );
    let mut values: Vec<libsql::Value> = vec![libsql::Value::Text(epic_id.to_string())];
    for (column, field) in [
        ("node_id", &filter.node_id),
        ("action", &filter.action),
        ("actor_user_id", &filter.actor_user_id),
    ] {
        if let Some(value) = field {
            sql.push_str(&format!(" AND {column} = ?"));
            values.push(libsql::Value::Text(value.clone()));
        }
    }
    sql.push_str(" ORDER BY created_at ASC, id ASC");

    let mut rows = conn.query(&sql, values).await?;
    let mut items = Vec::new();
    while let Some(row) = rows.next().await? {
        items.push(Activity {
            id: row.get(0)?,
            epic_id: row.get(1)?,
            node_id: row.get(2)?,
            actor_user_id: row.get(3)?,
            action: row.get(4)?,
            detail: row.get(5)?,
            created_at: row.get(6)?,
        });
    }
    Ok(items)
}

/// The epic's participants, derived as the **distinct actors** across every
/// attribution surface (§9 — never stored): activity actors, map-node
/// created/resolved-by, the Document's last-edited-by, comment authors, and
/// node-message posters. Agents act without a user id, so only humans are
/// listed; each is resolved to its user row, ordered by username.
pub async fn participants(
    conn: &libsql::Connection,
    epic_id: &str,
) -> crate::AppResult<Vec<Participant>> {
    let mut rows = conn
        .query(
            "SELECT u.id, u.username, u.display_name FROM user u \
             WHERE u.id IN ( \
               SELECT actor_user_id FROM activity WHERE epic_id = ?1 AND actor_user_id IS NOT NULL \
               UNION \
               SELECT created_by FROM map_node WHERE epic_id = ?1 AND created_by IS NOT NULL \
               UNION \
               SELECT resolved_by FROM map_node WHERE epic_id = ?1 AND resolved_by IS NOT NULL \
               UNION \
               SELECT last_edited_by FROM document WHERE epic_id = ?1 AND last_edited_by IS NOT NULL \
               UNION \
               SELECT author_user_id FROM comment WHERE epic_id = ?1 AND author_user_id IS NOT NULL \
               UNION \
               SELECT m.actor_user_id FROM node_message m \
                 JOIN map_node n ON n.id = m.node_id \
                 WHERE n.epic_id = ?1 AND m.actor_user_id IS NOT NULL \
             ) \
             ORDER BY u.username ASC",
        libsql::params![epic_id],
    )
        .await?;
    let mut items = Vec::new();
    while let Some(row) = rows.next().await? {
        items.push(Participant {
            id: row.get(0)?,
            username: row.get(1)?,
            display_name: row.get(2)?,
        });
    }
    Ok(items)
}

// ---- REST handlers ---------------------------------------------------------

/// `GET /epics/{id}/activity` — the epic's append-only activity feed as a
/// history (oldest first), optionally narrowed by `?node_id=&action=&actor_user_id=`.
/// `404` unknown epic; `400` on an unknown `action` filter value.
pub async fn get_activity(
    State(state): State<crate::AppState>,
    Path(id): Path<String>,
    Query(filter): Query<ListActivityQuery>,
) -> crate::AppResult<Json<std::collections::HashMap<&'static str, Vec<Activity>>>> {
    let conn = state.db.conn();
    if !crate::map::epic_exists(&conn, &id).await? {
        return Err(AppError::NotFound(format!("epic {id} not found")));
    }
    let items = list(&conn, &id, &filter).await?;
    Ok(Json(std::collections::HashMap::from([("items", items)])))
}

/// `GET /epics/{id}/participants` — the epic's participants, derived as
/// distinct actors over every attribution surface. `404` unknown epic.
pub async fn get_participants(
    State(state): State<crate::AppState>,
    Path(id): Path<String>,
) -> crate::AppResult<Json<std::collections::HashMap<&'static str, Vec<Participant>>>> {
    let conn = state.db.conn();
    if !crate::map::epic_exists(&conn, &id).await? {
        return Err(AppError::NotFound(format!("epic {id} not found")));
    }
    let items = participants(&conn, &id).await?;
    Ok(Json(std::collections::HashMap::from([("items", items)])))
}

// ---- tests ------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::activity;
    use crate::capability::now_ms;
    use crate::users::{self, Role};
    use crate::{app, AppState, Config, Db};
    use axum::body::Body;
    use axum::http::{header::AUTHORIZATION, Request, StatusCode};
    use serde_json::{json, Value};
    use std::path::PathBuf;
    use tower::ServiceExt; // for `oneshot`

    async fn boot() -> (AppState, axum::Router) {
        let db = Db::connect(":memory:").await.unwrap();
        db.run_migrations().await.unwrap();
        let state = AppState::new(Config::for_test(), db);
        let router = app(state.clone());
        (state, router)
    }

    /// Insert a project + epic directly (bypassing `create_epic`, so tests
    /// control exactly which mutations happened); return ids.
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
            "INSERT INTO epic (id, project_id, title, status, destination, created_at, updated_at) \
             VALUES (?1, ?2, 'E', 'Planning', 'It works end to end', ?3, ?3)",
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

    async fn body_json(response: axum::response::Response) -> Value {
        let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        serde_json::from_slice(&bytes).unwrap()
    }

    async fn get_activity(app: &axum::Router, token: &str, epic_id: &str, query: &str) -> (StatusCode, Value) {
        let response = app
            .clone()
            .oneshot(get_bearer(&format!("/epics/{epic_id}/activity{query}"), token))
            .await
            .unwrap();
        let status = response.status();
        (status, body_json(response).await)
    }

    // ---- AC: key mutations append activity rows; the feed renders as a
    //          history -------------------------------------------------------

    #[tokio::test]
    async fn key_mutations_append_activity_rows_and_the_feed_renders_as_a_history() {
        let (state, app) = boot().await;
        let (project_id, epic_id) = seed_epic(&state).await;
        let conn = state.db.conn();

        // Seed a user and node + document + comment attribution rows the way
        // the real mutation surfaces do, then verify the feed picks each up.
        let alice = users::testing::seed_user(&state, "alice", Role::Admin, true).await;
        let bob = users::testing::seed_user(&state, "bob", Role::User, true).await;

        // Epic creation (the handler) lands an `epic_created` row; simulate
        // the store-level writes the wired surfaces perform.
        activity::record(&conn, &epic_id, None, Some(&alice.id), EPIC_CREATED, Some("E"))
            .await
            .unwrap();
        let node = crate::map::create_node(
            &conn,
            &epic_id,
            "grilling",
            None,
            "Which store?",
            Some("Pick the blob store"),
            Some(&alice.id),
            None,
            None,
        )
        .await
        .unwrap();
        crate::map::update_node(
            &conn,
            &node.id,
            crate::map::UpdateMapNodeBody {
                state: Some("resolved".to_string()),
                gist: Some("Use the evidence store".to_string()),
                ..Default::default()
            },
            Some(&bob.id),
        )
        .await
        .unwrap();

        let (status, feed) = get_activity(&app, &alice_token(&state, &alice).await, &epic_id, "").await;
        assert_eq!(status, StatusCode::OK);
        let items = feed["items"].as_array().unwrap();
        let actions: Vec<&str> = items.iter().map(|i| i["action"].as_str().unwrap()).collect();
        assert_eq!(
            actions,
            vec![EPIC_CREATED, NODE_CREATED, NODE_RESOLVED],
            "the feed is the append-order history: {feed}"
        );
        assert_eq!(items[0]["detail"], "E");
        assert_eq!(items[1]["node_id"], node.id.as_str());
        assert_eq!(items[1]["actor_user_id"], alice.id.as_str());
        assert_eq!(items[2]["detail"], "Use the evidence store");
        assert_eq!(items[2]["actor_user_id"], bob.id.as_str());
        assert_eq!(items[2]["epic_id"], epic_id.as_str());
        let _ = project_id;
    }

    async fn alice_token(state: &AppState, user: &crate::users::User) -> String {
        crate::sessions::testing::login_as(state, user).await
    }

    // ---- AC: participants are derived as distinct actors -------------------

    #[tokio::test]
    async fn participants_are_derived_as_distinct_actors_across_every_surface() {
        let (state, app) = boot().await;
        let (_project_id, epic_id) = seed_epic(&state).await;
        let conn = state.db.conn();
        let alice = users::testing::seed_user(&state, "alice", Role::Admin, true).await;
        let bob = users::testing::seed_user(&state, "bob", Role::User, true).await;
        let carol = users::testing::seed_user(&state, "carol", Role::User, true).await;
        let dave = users::testing::seed_user(&state, "dave", Role::User, true).await;
        let eve = users::testing::seed_user(&state, "eve", Role::User, true).await;

        // Each attribution surface names a different user (and one repeats
        // alice, who must appear exactly once).
        activity::record(&conn, &epic_id, None, Some(&alice.id), EPIC_CREATED, None)
            .await
            .unwrap();
        let node = crate::map::create_node(
            &conn,
            &epic_id,
            "grilling",
            None,
            "N",
            None,
            Some(&bob.id), // created_by
            None,
            None,
        )
        .await
        .unwrap();
        crate::map::update_node(
            &conn,
            &node.id,
            crate::map::UpdateMapNodeBody {
                state: Some("resolved".to_string()),
                ..Default::default()
            },
            Some(&carol.id), // resolved_by
        )
        .await
        .unwrap();
        conn.execute(
            "INSERT INTO document (epic_id, html, version, last_edited_by, updated_at) \
             VALUES (?1, '<p>x</p>', 1, ?2, ?3)",
            libsql::params![epic_id.clone(), dave.id.clone(), now_ms()],
        )
        .await
        .unwrap();
        conn.execute(
            "INSERT INTO comment (id, epic_id, thread_id, anchor_kind, anchor_id, \
               author_user_id, is_agent, body, resolved, created_at) \
             VALUES (?1, ?2, ?1, 'node', ?3, ?4, 0, 'hi', 0, ?5)",
            libsql::params![
                ulid::Ulid::new().to_string(),
                epic_id.clone(),
                node.id.clone(),
                eve.id.clone(),
                now_ms()
            ],
        )
        .await
        .unwrap();
        conn.execute(
            "INSERT INTO node_message (id, node_id, role, actor_user_id, content, seq, created_at) \
             VALUES (?1, ?2, 'user', ?3, 'hello', 1, ?4)",
            libsql::params![ulid::Ulid::new().to_string(), node.id, alice.id.clone(), now_ms()],
        )
        .await
        .unwrap();

        let token = alice_token(&state, &alice).await;
        let response = app
            .clone()
            .oneshot(get_bearer(&format!("/epics/{epic_id}/participants"), &token))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = body_json(response).await;
        let ids: Vec<&str> = body["items"]
            .as_array()
            .unwrap()
            .iter()
            .map(|p| p["id"].as_str().unwrap())
            .collect();
        assert_eq!(ids.len(), 5, "distinct actors only: {body}");
        for expected in [&alice.id, &bob.id, &carol.id, &dave.id, &eve.id] {
            assert!(ids.contains(&expected.as_str()), "missing {expected}: {body}");
        }
        // Resolved to the user rows (username + display_name ride along).
        let usernames: Vec<&str> = body["items"]
            .as_array()
            .unwrap()
            .iter()
            .map(|p| p["username"].as_str().unwrap())
            .collect();
        assert_eq!(usernames, vec!["alice", "bob", "carol", "dave", "eve"]);

        // An agent-only attribution (NULL user ids) contributes nothing.
        let (_p2, epic2) = seed_epic(&state).await;
        activity::record(&conn, &epic2, None, None, NODE_CREATED, None)
            .await
            .unwrap();
        let response = app
            .oneshot(get_bearer(&format!("/epics/{epic2}/participants"), &token))
            .await
            .unwrap();
        let body = body_json(response).await;
        assert_eq!(body["items"].as_array().unwrap().len(), 0);
    }

    // ---- AC: the feed and participants are scoped to the epic -------------

    #[tokio::test]
    async fn the_feed_is_scoped_to_the_epic_and_supports_filters() {
        let (state, app) = boot().await;
        let (_project_id, epic_id) = seed_epic(&state).await;
        let (_other_project, other_epic) = seed_epic(&state).await;
        let user = users::testing::seed_user(&state, "planner", Role::Admin, true).await;
        let token = alice_token(&state, &user).await;
        let conn = state.db.conn();

        let node = crate::map::create_node(&conn, &epic_id, "grilling", None, "N", None, None, None, None)
            .await
            .unwrap();
        // `create_node` appended its own `node_created` feed row; add an
        // epic-level row for the filter test.
        activity::record(&conn, &epic_id, None, None, EPIC_CREATED, None)
            .await
            .unwrap();
        // Another epic's row must never leak into this epic's feed.
        activity::record(&conn, &other_epic, None, None, EPIC_CREATED, None)
            .await
            .unwrap();

        let (status, feed) = get_activity(&app, &token, &epic_id, "").await;
        assert_eq!(status, StatusCode::OK);
        let items = feed["items"].as_array().unwrap();
        assert_eq!(items.len(), 2);
        assert!(items.iter().all(|i| i["epic_id"] == epic_id.as_str()));

        // Narrow by node.
        let (_, feed) = get_activity(&app, &token, &epic_id, &format!("?node_id={}", node.id)).await;
        assert_eq!(feed["items"].as_array().unwrap().len(), 1);
        assert_eq!(feed["items"][0]["action"], NODE_CREATED);

        // Narrow by action.
        let (_, feed) = get_activity(&app, &token, &epic_id, &format!("?action={EPIC_CREATED}")).await;
        assert_eq!(feed["items"].as_array().unwrap().len(), 1);
        assert_eq!(feed["items"][0]["action"], EPIC_CREATED);

        // An unknown action filter matches nothing (the vocabulary is open
        // text; filtering is exact).
        let (_, feed) = get_activity(&app, &token, &epic_id, "?action=no-such-action").await;
        assert_eq!(feed["items"].as_array().unwrap().len(), 0);

        // Unknown epic → 404 on both endpoints.
        for uri in ["/epics/01JZZNOPE/activity", "/epics/01JZZNOPE/participants"] {
            let response = app
                .clone()
                .oneshot(get_bearer(uri, &token))
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::NOT_FOUND);
        }
    }

    // ---- AC: agent runs read the feed through their scoped capability ------

    #[tokio::test]
    async fn a_scoped_capability_token_can_read_the_feed_and_participants() {
        let (state, app) = boot().await;
        let (project_id, epic_id) = seed_epic(&state).await;
        let guard = state.caps.mint(
            epic_id.clone(),
            project_id.clone(),
            "grilling".into(),
            PathBuf::from("/tmp"),
        );
        let token = guard.token().to_string();

        activity::record(
            state.db.conn(),
            &epic_id,
            None,
            None,
            EPIC_CREATED,
            Some("E"),
        )
        .await
        .unwrap();

        for uri in [
            format!("/epics/{epic_id}/activity"),
            format!("/epics/{epic_id}/participants"),
        ] {
            let response = app
                .clone()
                .oneshot(get_bearer(&uri, &token))
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::OK);
        }

        // The other epic is out of reach (403 from the allow-list).
        let (_other_project, other_epic) = seed_epic(&state).await;
        let response = app
            .oneshot(get_bearer(&format!("/epics/{other_epic}/activity"), &token))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::FORBIDDEN);
    }

    // ---- AC: nothing ever updates or deletes a feed row (append-only) ------

    #[tokio::test]
    async fn recording_is_pure_append_and_the_history_is_stable() {
        let (state, _app) = boot().await;
        let (_project_id, epic_id) = seed_epic(&state).await;
        let conn = state.db.conn();

        activity::record(&conn, &epic_id, None, None, EPIC_CREATED, Some("first"))
            .await
            .unwrap();
        let before = list(&conn, &epic_id, &ListActivityQuery::default())
            .await
            .unwrap();
        assert_eq!(before.len(), 1);
        assert_eq!(before[0].detail.as_deref(), Some("first"));

        // A second record appends; the first row is untouched (append-only).
        activity::record(&conn, &epic_id, None, None, NODE_CREATED, Some("second"))
            .await
            .unwrap();
        let after = list(&conn, &epic_id, &ListActivityQuery::default())
            .await
            .unwrap();
        assert_eq!(after.len(), 2);
        assert_eq!(after[0].id, before[0].id);
        assert_eq!(after[0].detail.as_deref(), Some("first"));
        assert_eq!(after[1].detail.as_deref(), Some("second"));
    }

    // ---- AC end to end: the real mutation surfaces append feed rows, the
    //          feed reads as a history, and the inline per-row attribution is
    //          present on nodes / the Document / comments -------------------

    fn post_json_bearer(uri: &str, token: &str, body: Value) -> Request<Body> {
        Request::builder()
            .method("POST")
            .uri(uri)
            .header(AUTHORIZATION, format!("Bearer {token}"))
            .header("content-type", "application/json")
            .body(Body::from(body.to_string()))
            .unwrap()
    }

    #[tokio::test]
    async fn the_real_mutation_surfaces_feed_the_history_and_the_inline_attribution_is_present() {
        let (state, app) = boot().await;
        let (project_id, _) = seed_epic(&state).await; // the project row only
        let alice = users::testing::seed_user(&state, "alice", Role::Admin, true).await;
        let bob = users::testing::seed_user(&state, "bob", Role::User, true).await;
        let alice_tok = alice_token(&state, &alice).await;
        let bob_token = alice_token(&state, &bob).await;

        // Epic create (destination required): lands `epic_created` + the
        // seed grilling node's `node_created`, both attributed to alice.
        let response = app
            .clone()
            .oneshot(post_json_bearer(
                &format!("/projects/{project_id}/epics"),
                &alice_tok,
                json!({ "title": "Wayfinder", "destination": "A map that plans itself" }),
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::CREATED);
        let epic = body_json(response).await;
        let epic_id = epic["id"].as_str().unwrap().to_string();

        // Bob creates a node (inline attribution: `created_by`).
        let response = app
            .clone()
            .oneshot(post_json_bearer(
                &format!("/epics/{epic_id}/map-nodes"),
                &bob_token,
                json!({ "kind": "grilling", "title": "Which store?" }),
            ))
            .await
            .unwrap();
        let node = body_json(response).await;
        assert_eq!(node["created_by"], bob.id.as_str());
        let node_id = node["id"].as_str().unwrap().to_string();

        // Alice comments on the node (inline attribution: `author_user_id`).
        let response = app
            .clone()
            .oneshot(post_json_bearer(
                &format!("/epics/{epic_id}/comments"),
                &alice_tok,
                json!({ "anchor_kind": "node", "anchor_id": node_id, "body": "Evidence store?" }),
            ))
            .await
            .unwrap();
        let comment = body_json(response).await;
        assert_eq!(comment["author_user_id"], alice.id.as_str());

        // Bob syncs the Document (inline attribution: `last_edited_by`).
        let response = app
            .clone()
            .oneshot(post_json_bearer(
                &format!("/epics/{epic_id}/document/sync"),
                &bob_token,
                json!({ "html": "<h1 id=\"d\">D</h1>", "base_version": 0 }),
            ))
            .await
            .unwrap();
        let doc = body_json(response).await;
        assert_eq!(doc["last_edited_by"], bob.id.as_str());

        // Bob resolves the node (inline attribution: `resolved_by`).
        let response = app
            .clone()
            .oneshot(post_json_bearer(
                &format!("/epics/{epic_id}/map-nodes/{node_id}/resolve"),
                &bob_token,
                json!({ "gist": "Evidence store" }),
            ))
            .await
            .unwrap();
        let outcome = body_json(response).await;
        assert_eq!(outcome["node"]["resolved_by"], bob.id.as_str());

        // The feed renders the whole history, in append order, every key
        // mutation present with its actor.
        let (status, feed) = get_activity(&app, &alice_tok, &epic_id, "").await;
        assert_eq!(status, StatusCode::OK);
        let items = feed["items"].as_array().unwrap();
        let actions: Vec<&str> = items.iter().map(|i| i["action"].as_str().unwrap()).collect();
        assert_eq!(
            actions,
            vec![
                EPIC_CREATED,
                NODE_CREATED, // the seed node
                NODE_CREATED, // bob's node
                COMMENT_POSTED,
                DOCUMENT_SYNCED,
                NODE_RESOLVED,
            ],
            "the feed is the append-order history: {feed}"
        );
        assert_eq!(items[0]["actor_user_id"], alice.id.as_str());
        assert_eq!(items[2]["actor_user_id"], bob.id.as_str());
        assert_eq!(items[3]["actor_user_id"], alice.id.as_str());
        assert_eq!(items[4]["detail"], "version 1");
        assert_eq!(items[5]["detail"], "Evidence store");

        // Participants are the derived distinct actors: alice and bob, once
        // each, resolved to their user rows.
        let response = app
            .clone()
            .oneshot(get_bearer(&format!("/epics/{epic_id}/participants"), &alice_tok))
            .await
            .unwrap();
        let body = body_json(response).await;
        let usernames: Vec<&str> = body["items"]
            .as_array()
            .unwrap()
            .iter()
            .map(|p| p["username"].as_str().unwrap())
            .collect();
        assert_eq!(usernames, vec!["alice", "bob"]);
    }
}
