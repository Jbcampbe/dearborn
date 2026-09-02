//! Capability tokens — short-lived, per-run, epic-scoped credentials for the
//! agent-facing [`crate::cli`] and the REST routes it is allowed to call.
//!
//! ## Auth & scoping
//!
//! Agent runs (breakdown today; the per-node planning engines as they land)
//! mint one capability token via [`CapabilityStore::mint`] and hand it to the
//! agent as the `--token` of the `dearborn` CLI. The token rides the normal
//! `Authorization: Bearer` header, but it is **not** a browser session token:
//! [`crate::auth::require_auth`] falls back to resolving it against the
//! [`CapabilityStore`], and on success checks [`authorize_cap_request`] — a
//! fixed method+path allow-list whose epic segment must equal the token's
//! scope. A token minted for epic A can therefore act only on epic A, and only
//! through the CLI's verbs; everything else is a `403`.
//!
//! Tokens are opaque ULIDs, held server-side in a plain in-memory map with a
//! TTL backstop. The run holds a [`CapabilityGuard`] that revokes the token the
//! instant the run ends (completion, error, or panic).
//!
//! This module replaces the retired in-process MCP server (`mcp.rs`), whose
//! JSON-RPC endpoint handed the same scope to two hand-rolled tools; the REST
//! routes the CLI calls now play that role.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use axum::{
    async_trait,
    extract::FromRequestParts,
    http::request::Parts,
    response::IntoResponse,
    Json,
};
use serde_json::json;

use crate::{AppError, AppState};

/// Default lifetime of a minted capability token. An agent run is far shorter;
/// the [`CapabilityGuard`] revokes the token the instant the run ends
/// regardless, so this TTL is only a backstop against a leaked/never-dropped
/// guard.
const CAPABILITY_TTL: Duration = Duration::from_secs(6 * 60 * 60);

// ---- capability tokens ---------------------------------------------------

/// The fixed scope a capability token grants. Set at mint time from the run;
/// never influenced by agent-supplied CLI arguments.
#[derive(Clone, Debug)]
pub struct CapabilityScope {
    /// The one epic this token may act on.
    pub epic_id: String,
    /// The one project this token acts under (used by task creation to set
    /// `task.project_id`; the agent never supplies it).
    pub project_id: String,
    /// The phase (engine run) whose surface this token grants, e.g.
    /// `breakdown`. Informational — carried for attribution and `/auth/capability`.
    pub phase: String,
    /// The project's canonical read-only clone; the run's working directory.
    /// Never exposed through the REST surface.
    pub clone_path: PathBuf,
    /// Unix-ms expiry; a token resolved at/after this instant is rejected.
    expires_at: i64,
}

/// Per-run capability registry shared on [`AppState`]. Maps opaque tokens to the
/// scope they authorize.
#[derive(Default)]
pub struct CapabilityStore {
    tokens: Mutex<HashMap<String, CapabilityScope>>,
}

impl CapabilityStore {
    /// Create an empty store.
    pub fn new() -> CapabilityStore {
        CapabilityStore::default()
    }

    /// Mint a token scoped to `(epic_id, project_id, phase, clone_path)` with the
    /// default TTL. The returned [`CapabilityGuard`] revokes the token on drop.
    pub fn mint(
        self: &Arc<Self>,
        epic_id: String,
        project_id: String,
        phase: String,
        clone_path: PathBuf,
    ) -> CapabilityGuard {
        self.mint_with_expiry(
            epic_id,
            project_id,
            phase,
            clone_path,
            now_ms() + CAPABILITY_TTL.as_millis() as i64,
        )
    }

    /// Mint with an explicit unix-ms expiry (used by tests to forge an expired
    /// token). Otherwise identical to [`mint`](Self::mint).
    pub(crate) fn mint_with_expiry(
        self: &Arc<Self>,
        epic_id: String,
        project_id: String,
        phase: String,
        clone_path: PathBuf,
        expires_at: i64,
    ) -> CapabilityGuard {
        let token = ulid::Ulid::new().to_string();
        let scope = CapabilityScope {
            epic_id,
            project_id,
            phase,
            clone_path,
            expires_at,
        };
        self.tokens
            .lock()
            .expect("capability mutex poisoned")
            .insert(token.clone(), scope);
        CapabilityGuard {
            token,
            store: Arc::clone(self),
        }
    }

    /// Resolve a token to its scope, or `None` if unknown or expired. Expired
    /// tokens are pruned as a side effect.
    pub fn resolve(&self, token: &str) -> Option<CapabilityScope> {
        let mut tokens = self.tokens.lock().expect("capability mutex poisoned");
        match tokens.get(token) {
            Some(scope) if scope.expires_at > now_ms() => Some(scope.clone()),
            Some(_) => {
                tokens.remove(token);
                None
            }
            None => None,
        }
    }

    fn revoke(&self, token: &str) {
        self.tokens
            .lock()
            .expect("capability mutex poisoned")
            .remove(token);
    }
}

/// RAII handle to a minted capability token. Dropping it revokes the token, so
/// an agent run's CLI access dies with the run (completion, error, or panic).
pub struct CapabilityGuard {
    token: String,
    store: Arc<CapabilityStore>,
}

impl CapabilityGuard {
    /// The opaque token handed to the `dearborn` CLI as `--token`.
    pub fn token(&self) -> &str {
        &self.token
    }
}

impl Drop for CapabilityGuard {
    fn drop(&mut self) {
        self.store.revoke(&self.token);
    }
}

// ---- what a capability token may do over REST -----------------------------

/// Whether a request carrying a capability token is authorized.
///
/// The allow-list is exactly the REST surface the `dearborn` CLI exposes to
/// agents — reads of the scoped epic and the two task-DAG writes breakdown
/// performs — plus `GET /auth/capability`, which names the token's own scope.
/// Every epic-addressed pattern requires the path's epic id to equal the
/// scope's: **a scoped token can only act on its epic.** Anything else is a
/// `403` from [`crate::auth::require_auth`], before any handler runs.
///
/// Session tokens bypass this table entirely (full access, as before).
pub fn authorize_cap_request(
    scope: &CapabilityScope,
    method: &axum::http::Method,
    path: &str,
) -> bool {
    let segs: Vec<&str> = path.split('/').filter(|s| !s.is_empty()).collect();
    let epic = scope.epic_id.as_str();
    match (method.as_str(), segs.as_slice()) {
        ("GET", ["auth", "capability"]) => true,
        ("GET", ["epics", e]) => *e == epic,
        ("GET", ["epics", e, "dag"]) => *e == epic,
        ("POST", ["epics", e, "tasks"]) => *e == epic,
        ("POST", ["epics", e, "dependencies"]) => *e == epic,
        _ => false,
    }
}

/// Extractor for the capability-token identity on protected routes.
///
/// Populated from the scope [`crate::auth::require_auth`] already resolved and
/// inserted into the request extensions. Rejects — with `403`, never `401` —
/// any request that was authenticated another way (e.g. a browser session
/// token), so the endpoint it guards is unambiguously the CLI's.
pub struct CapabilityActor(pub CapabilityScope);

#[async_trait]
impl FromRequestParts<AppState> for CapabilityActor {
    type Rejection = AppError;

    async fn from_request_parts(parts: &mut Parts, _state: &AppState) -> Result<Self, Self::Rejection> {
        parts
            .extensions
            .get::<CapabilityScope>()
            .cloned()
            .map(CapabilityActor)
            .ok_or_else(|| {
                AppError::Forbidden("this endpoint requires a capability token".to_string())
            })
    }
}

/// `GET /auth/capability` — what the `dearborn scope` verb calls. Returns the
/// bearer token's capability scope (epic, project, phase, expiry), so an agent
/// can learn which epic its token acts on without the scope being baked into
/// any single verb's arguments.
pub async fn whoami(actor: CapabilityActor) -> Result<impl IntoResponse, AppError> {
    let scope = actor.0;
    Ok(Json(json!({
        "kind": "capability",
        "epic_id": scope.epic_id,
        "project_id": scope.project_id,
        "phase": scope.phase,
        "expires_at": scope.expires_at,
    })))
}

// ---- DAG live-update publish ----------------------------------------------

/// Build the epic's task DAG (`{ nodes, edges }`) — nodes carry computed
/// readiness (`DagNode`, same shape as `GET /epics/:id/dag`) — and publish it
/// on `epic:<id>` under the `dag_updated` type, so a subscribed client re-renders
/// with correct ready/blocked state. Best-effort: a read error is logged and the
/// publish is skipped.
pub async fn publish_dag(state: &AppState, epic_id: &str) {
    let dag = match crate::tasks::compute_dag(state.db.conn(), epic_id).await {
        Ok(dag) => dag,
        Err(err) => {
            tracing::warn!(epic = %epic_id, error = %err, "dag publish: failed to load DAG");
            return;
        }
    };
    let payload = json!({ "nodes": dag.nodes, "edges": dag.edges });
    state
        .hub
        .publish(&format!("epic:{epic_id}"), "dag_updated", payload);
}

/// Current unix time in milliseconds (matches the `*_at` columns).
pub fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::users::{self, Role};
    use serde_json::Value;
    use crate::{app, Config, Db};
    use axum::body::Body;
    use axum::http::{header::AUTHORIZATION, Request, StatusCode};
    use tower::ServiceExt;

    /// Boot state + router (no agent runs are started by these tests).
    async fn boot() -> (AppState, axum::Router) {
        let db = Db::connect(":memory:").await.unwrap();
        db.run_migrations().await.unwrap();
        let state = AppState::new(Config::for_test(), db);
        let router = app(state.clone());
        (state, router)
    }

    /// Insert a project and an epic; return ids.
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

    fn delete(uri: &str, token: &str) -> Request<Body> {
        Request::builder()
            .method("DELETE")
            .uri(uri)
            .header(AUTHORIZATION, format!("Bearer {token}"))
            .body(Body::empty())
            .unwrap()
    }

    #[tokio::test]
    async fn unknown_or_expired_capability_is_rejected() {
        let (state, app) = boot().await;
        let (_p, epic_id) = seed_epic(&state).await;

        // Unknown token → 401 (never opened, nothing to act on).
        let response = app
            .clone()
            .oneshot(get_bearer("/epics/not-a-real-epic", "no-such-token"))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);

        // Expired token → 401 (forge an expiry in the past).
        let guard = state.caps.mint_with_expiry(
            epic_id.clone(),
            "proj".into(),
            "breakdown".into(),
            PathBuf::from("/tmp"),
            now_ms() - 1,
        );
        let response = app
            .oneshot(get_bearer(
                &format!("/epics/{epic_id}"),
                guard.token(),
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn a_scoped_token_can_act_on_its_own_epic() {
        let (state, app) = boot().await;
        let (project_id, epic_id) = seed_epic(&state).await;
        let guard =
            state
                .caps
                .mint(epic_id.clone(), project_id.clone(), "breakdown".into(), PathBuf::from("/tmp"));
        let token = guard.token();

        // Read the scoped epic.
        let response = app
            .clone()
            .oneshot(get_bearer(&format!("/epics/{epic_id}"), token))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        // Read its DAG.
        let response = app
            .clone()
            .oneshot(get_bearer(&format!("/epics/{epic_id}/dag"), token))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        // Create a task under it.
        let response = app
            .clone()
            .oneshot(post_json_bearer(
                &format!("/epics/{epic_id}/tasks"),
                token,
                json!({"title": "Slice one"}),
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::CREATED);

        // Name its own scope.
        let response = app
            .clone()
            .oneshot(get_bearer("/auth/capability", token))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body: Value = serde_json::from_slice(
            &axum::body::to_bytes(response.into_body(), usize::MAX)
                .await
                .unwrap(),
        )
        .unwrap();
        assert_eq!(body["epic_id"], epic_id.as_str());
        assert_eq!(body["project_id"], project_id.as_str());
        assert_eq!(body["phase"], "breakdown");
    }

    #[tokio::test]
    async fn a_scoped_token_cannot_act_on_another_epic() {
        let (state, app) = boot().await;
        let (project_id, epic_id) = seed_epic(&state).await;
        let (_other_project, other_epic) = seed_epic(&state).await;
        let guard = state
            .caps
            .mint(epic_id.clone(), project_id, "breakdown".into(), PathBuf::from("/tmp"));
        let token = guard.token();

        // Every verb shape against the OTHER epic is a 403, not a 401 or 404.
        let response = app
            .clone()
            .oneshot(get_bearer(&format!("/epics/{other_epic}"), token))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::FORBIDDEN);

        let response = app
            .clone()
            .oneshot(post_json_bearer(
                &format!("/epics/{other_epic}/tasks"),
                token,
                json!({"title": "hostile"}),
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::FORBIDDEN);

        let response = app
            .clone()
            .oneshot(post_json_bearer(
                &format!("/epics/{other_epic}/dependencies"),
                token,
                json!({"blocker_id": "a", "blocked_id": "b"}),
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::FORBIDDEN);

        // ...and nothing was written anywhere.
        assert!(
            crate::tasks::list_tasks_for_epic(state.db.conn(), &other_epic)
                .await
                .unwrap()
                .is_empty()
        );
    }

    #[tokio::test]
    async fn a_scoped_token_cannot_leave_the_cli_allow_list() {
        let (state, app) = boot().await;
        let (project_id, epic_id) = seed_epic(&state).await;
        let guard =
            state
                .caps
                .mint(epic_id.clone(), project_id, "breakdown".into(), PathBuf::from("/tmp"));
        let token = guard.token();

        // Admin/user management — browser-session territory.
        let response = app
            .clone()
            .oneshot(get_bearer("/users", token))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::FORBIDDEN);

        // A scoped-epic path in a method the CLI does not expose.
        let response = app
            .clone()
            .oneshot(delete(&format!("/epics/{epic_id}"), token))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::FORBIDDEN);

        // Authenticated-but-not-a-capability-token caller on the scope verb.
        let user = users::testing::seed_user(&state, "tester", Role::Admin, true).await;
        let session_token = crate::sessions::testing::login_as(&state, &user).await;
        let response = app
            .oneshot(get_bearer("/auth/capability", &session_token))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::FORBIDDEN);
    }

    #[tokio::test]
    async fn capability_guard_drop_revokes_the_token() {
        let (state, app) = boot().await;
        let (_p, epic_id) = seed_epic(&state).await;
        let token = {
            let guard = state.caps.mint(
                epic_id,
                "proj".into(),
                "breakdown".into(),
                PathBuf::from("/tmp"),
            );
            guard.token().to_string()
            // guard dropped here → token revoked
        };
        let response = app
            .oneshot(get_bearer("/auth/capability", &token))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }
}
