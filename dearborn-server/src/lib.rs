//! Dearborn server library.
//!
//! Keeps app-construction (router, handlers, shared state) separate from the
//! binary entrypoint so later tasks can add modules and integration tests
//! cleanly.

pub mod agent_settings;
pub mod agent_slot;
pub mod auth;
pub mod board;
pub mod breakdown;
pub mod cmd;
pub mod config;
pub mod crypto;
pub mod db;
pub mod epics;
pub mod error;
pub mod evidence;
pub mod git;
pub mod git_host;
pub mod hub;
pub mod lanes;
pub mod mcp;
pub mod planning;
pub mod pr;
pub mod projects;
pub mod spec;
pub mod task_agent;
pub mod tasks;
pub mod worker;
pub mod workspace;
pub mod ws;

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};

use std::path::Path;

use axum::{middleware, routing::get, Json, Router};
use serde_json::{json, Value};
use tower_http::{
    services::{ServeDir, ServeFile},
    trace::TraceLayer,
};

pub use breakdown::BreakdownAgent;
pub use config::{Config, ConfigError, ExecutorConfig};
pub use crypto::MasterKey;
pub use db::{Db, DbError};
pub use error::{AppError, AppResult};
pub use git_host::GitHost;
pub use hub::Hub;
pub use mcp::CapabilityStore;
pub use planning::PlanningAgent;
pub use task_agent::TaskAgent;

/// Initialise the global `tracing` subscriber. Idempotent; safe to skip in tests.
/// Honours `RUST_LOG`, defaulting to `info`.
pub fn init_tracing() {
    use tracing_subscriber::{fmt, EnvFilter};
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("info,dearborn_server=debug"));
    // `try_init` returns Err if a subscriber is already set — ignore it.
    let _ = fmt().with_env_filter(filter).try_init();
}

/// Shared application state handed to handlers and middleware.
///
/// `Clone` is cheap: everything inside is reference-counted.
#[derive(Clone)]
pub struct AppState {
    pub config: Arc<Config>,
    pub db: Db,
    /// Topic pub/sub broadcaster for live WebSocket subscriptions. Server-side
    /// code publishes events via `state.hub.publish(topic, type, payload)`.
    pub hub: Arc<Hub>,
    /// AES-256 key (derived from `DEARBORN_MASTER_KEY`) used to encrypt/decrypt
    /// per-project PATs. Never serialised or logged.
    pub crypto: Arc<MasterKey>,
    /// The planning agent that drives interactive epic-planning runs (T-202).
    /// Production is [`planning::ClaudePlanningAgent`]; tests inject a fake.
    pub planner: Arc<dyn PlanningAgent>,
    /// The one-shot breakdown agent that turns an approved epic into a task DAG
    /// (T-301). Production is [`breakdown::ClaudeBreakdownAgent`]; tests inject
    /// a fake. Shares the planning in-flight slot so the two never overlap on
    /// one epic.
    pub breakdown: Arc<dyn BreakdownAgent>,
    /// The task-stage agent that drives `implement`/`fix`/`review`/
    /// `verify_complete`/`summarize` (T-512). Production is
    /// [`task_agent::ClaudeTaskAgent`]; tests inject a scripted fake. Unlike
    /// `planner`/`breakdown`, a task-stage run is one-shot with no `resume`
    /// (D19) and has no in-flight slot of its own here — concurrency for
    /// task stages is the worker pool's job (T-510+: at most one stage per
    /// claimed task at a time, by construction of the DAG walk), not a
    /// per-epic guard like planning's.
    pub task_agent: Arc<dyn TaskAgent>,
    /// The git-hosting seam (T-514): push the epic branch and open its PR.
    /// Production is [`git_host::GithubHost`]; tests inject
    /// [`git_host::testing::FakeHost`] so `just test` never talks to a real
    /// GitHub API (MILESTONE_2 §10). Unlike `planner`/`breakdown`/
    /// `task_agent`, this seam is pure network I/O with no in-flight-run
    /// bookkeeping of its own — [`crate::worker`]'s finalize step is its only
    /// caller, once per epic, at the very end of a successful DAG walk.
    pub git_host: Arc<dyn GitHost>,
    /// Epics with a planning run currently in flight. A second trigger for an
    /// epic already in this set is ignored (its user message is still stored),
    /// so runs never interleave on `seq`/resume. See [`AppState::try_acquire_run`].
    pub inflight: Arc<Mutex<HashSet<String>>>,
    /// Per-run MCP capability tokens (T-203). A planning run mints a token scoped
    /// to one `(epic, phase, clone_path)`; the shelled-out agent authenticates its
    /// `POST /mcp/:cap` calls with it. See [`crate::mcp`].
    pub caps: Arc<CapabilityStore>,
    /// Dearborn's own loopback origin (e.g. `http://127.0.0.1:8787`), used to
    /// build the MCP config URL handed to the agent. Set once after the listener
    /// binds (`main`, or the live test); `None` in unit tests that never spawn a
    /// real agent, which disables MCP wiring for the run.
    pub advertised_base: Arc<Mutex<Option<String>>>,
    /// The worker pool's wake signal (D2, T-510). Anything that enqueues work —
    /// today, the `Ready → InProgress` lane transition — calls
    /// `notify.notify_waiters()` after committing the enqueue so idle worker
    /// loops wake immediately instead of waiting out their poll interval. See
    /// [`worker::spawn_pool`] for the notify-or-poll idle loop this drives.
    pub notify: Arc<tokio::sync::Notify>,
    /// Per-project async lock guarding the canonical checkout's refresh step
    /// (T-511, MILESTONE_2 §11 risk 3). Every epic provision in a project
    /// calls `git::refresh_repo` against that project's single shared
    /// canonical checkout; without serializing, two workers provisioning
    /// epics in the same project concurrently could interleave one's `git
    /// reset --hard` with another's in-flight `git fetch`, corrupting the
    /// checkout both epics clone from. Keyed by project id so provisions in
    /// *different* projects never block each other. See
    /// [`AppState::project_refresh_lock`]; consumed by
    /// [`crate::workspace::provision_epic_workspace`]. The outer
    /// `std::sync::Mutex` guards only the map itself (never held across an
    /// `.await`); the per-project `tokio::sync::Mutex` it hands out is the
    /// actual (long-held, across-await) exclusion.
    pub refresh_locks: Arc<Mutex<HashMap<String, Arc<tokio::sync::Mutex<()>>>>>,
    /// Live `RunHandle`s for whatever agent stage is currently running,
    /// keyed by the claimed item's id (T-542, MILESTONE_2 D12/§7). Populated
    /// by [`task_agent::run_agent_stage`] for the duration of exactly one
    /// agent stage via a `task_agent::CancelGuard` (private to that module —
    /// this field is the only thing outside it that ever sees the map), and
    /// removed on **every** exit path out of that function — normal
    /// completion, an ordinary (non-cancel) failure, a harness spawn error, a
    /// panicked drain thread, or a cancel itself — by that guard's `Drop`,
    /// never by a caller remembering to clean up. See
    /// [`task_agent::CancelRegistry`] for the concrete type and
    /// [`task_agent::run_agent_stage`]'s own doc for exactly when an entry
    /// exists.
    ///
    /// [`lanes::set_epic_lane`]'s `InProgress → Cancelled` transition is the
    /// only thing that ever *reads* this map: it looks up the epic's id
    /// (already committed `Cancelled` in the DB by that point) and, if an
    /// entry exists, calls `RunControl::cancel()` on it — best-effort,
    /// fire-and-forget (the harness's own `cancel()` sends a signal and
    /// returns immediately; it does not wait for the process to actually
    /// exit), so the HTTP handler never blocks on the kill completing. **A
    /// cancel for an item with nothing in flight is a clean no-op** — the
    /// lookup simply finds no entry — because the stage-boundary DB checks
    /// already sprinkled through `worker::run_epic_pipeline_inner` and its
    /// callees are the backstop D12 requires for a cancel issued *between*
    /// stages (non-agent stages — `setup`/`preflight`/`test_gate`/`commit`/
    /// `push` — never register a handle here at all; a cancel that lands
    /// while one of those is running is caught by that same backstop, not a
    /// kill).
    ///
    /// **1:1, not 1:many.** MILESTONE_2 §2.3's DAG walk fully serializes: at
    /// most one task per claimed epic is ever `InProgress`, and every call
    /// site in `worker.rs` awaits one `run_agent_stage` call to completion
    /// before starting the next, so at most one agent stage per claimed item
    /// is ever in flight. The map is keyed accordingly — a plain
    /// insert-on-start/remove-on-end, never a `Vec` or a ref-count. See
    /// `task_agent::CancelGuard`'s own doc for what breaks if that
    /// assumption is ever violated.
    ///
    /// **Shaped for T-550/T-551.** The key is "whatever id the claimed item
    /// has" — the epic id for an epic's DAG walk, the task id for a
    /// standalone claim (`epic_id: None`). T-550 landed
    /// `worker::WorkItem::Epic(id) | WorkItem::Standalone(task_id)` as
    /// exactly this id (`WorkItem::id()`), confirming the bet this doc made
    /// before that unification existed: neither this field nor
    /// `task_agent::cancel_registry_key` needed to change shape when it did.
    /// T-551 landed the rest of the bet: `worker::run_standalone_pipeline_inner`
    /// now drives a standalone task through the identical `process_one_task`
    /// sequence an epic-owned task runs, so a `Standalone`-keyed entry *does*
    /// populate here for the duration of each of its agent stages, exactly
    /// like an `Epic`-keyed one. What's still missing is any HTTP surface
    /// that would ever look one up to call `RunControl::cancel()` on it — T-551
    /// deliberately did not add a `POST /tasks/{id}/lane`-style cancel
    /// endpoint (MILESTONE_2 §8 names no such AC, and T-561's own client AC
    /// only lists "Cancel on in-flight **epics**") — so a standalone task's
    /// entry here is populated and correctly shaped, just never read by
    /// anything today.
    pub cancel_registry: Arc<task_agent::CancelRegistry>,
    /// Test-only seam (T-510) letting a test observe/gate the claimed-epic
    /// pipeline body without sleeps: if set, [`worker::run_epic_pipeline`]
    /// awaits it once, immediately after claiming an epic and before doing
    /// any work. A concurrency test uses this to hold N claims in flight
    /// simultaneously and assert the pool never exceeds `worker_concurrency`.
    /// `None` (the default) is a no-op — production code never sets it.
    #[cfg(test)]
    pub test_pipeline_hook: Option<worker::PipelineHook>,
}

impl AppState {
    /// Construct shared state from a resolved [`Config`] and open [`Db`], using
    /// the production planning agent ([`planning::ClaudePlanningAgent`]).
    ///
    /// The master key is derived here; `config.master_key` is guaranteed
    /// non-empty by config loading, so derivation cannot fail. Boot code should
    /// nevertheless call [`MasterKey::derive`] first to fail fast (see `main`).
    pub fn new(config: Config, db: Db) -> AppState {
        AppState::with_agents(
            config,
            db,
            Arc::new(planning::ClaudePlanningAgent::new()),
            Arc::new(breakdown::ClaudeBreakdownAgent::new()),
        )
    }

    /// Like [`AppState::new`] but with an injected [`PlanningAgent`] — the seam
    /// that lets tests drive planning runs hermetically with a scripted fake.
    /// The breakdown agent defaults to the production
    /// [`breakdown::ClaudeBreakdownAgent`] (override it via [`with_agents`]).
    pub fn with_planner(config: Config, db: Db, planner: Arc<dyn PlanningAgent>) -> AppState {
        AppState::with_agents(
            config,
            db,
            planner,
            Arc::new(breakdown::ClaudeBreakdownAgent::new()),
        )
    }

    /// Like [`with_planner`](Self::with_planner) but also injecting the
    /// [`BreakdownAgent`] — the seam tests use to drive breakdown runs
    /// hermetically (T-301). Production wiring ([`AppState::new`] /
    /// [`with_planner`](Self::with_planner)) defaults the breakdown agent to
    /// [`breakdown::ClaudeBreakdownAgent`]; the task agent defaults to
    /// [`task_agent::ClaudeTaskAgent`] (override it via
    /// [`with_all_agents`](Self::with_all_agents)).
    pub fn with_agents(
        config: Config,
        db: Db,
        planner: Arc<dyn PlanningAgent>,
        breakdown: Arc<dyn BreakdownAgent>,
    ) -> AppState {
        AppState::with_all_agents(
            config,
            db,
            planner,
            breakdown,
            Arc::new(task_agent::ClaudeTaskAgent::new()),
        )
    }

    /// Like [`with_agents`](Self::with_agents) but also injecting the
    /// [`TaskAgent`] — the seam tests use to drive task-stage runs
    /// hermetically (T-512) without spawning `claude`. Defaults `git_host` to
    /// the production [`git_host::GithubHost`] (override it via
    /// [`with_all_agents_and_host`](Self::with_all_agents_and_host)).
    pub fn with_all_agents(
        config: Config,
        db: Db,
        planner: Arc<dyn PlanningAgent>,
        breakdown: Arc<dyn BreakdownAgent>,
        task_agent: Arc<dyn TaskAgent>,
    ) -> AppState {
        AppState::with_all_agents_and_host(
            config,
            db,
            planner,
            breakdown,
            task_agent,
            Arc::new(git_host::GithubHost::new()),
        )
    }

    /// Like [`with_all_agents`](Self::with_all_agents) but also injecting the
    /// [`GitHost`] — the seam T-514's tests use to drive the finalize
    /// (push + open PR) step hermetically via
    /// [`git_host::testing::FakeHost`] instead of the real
    /// [`git_host::GithubHost`].
    pub fn with_all_agents_and_host(
        config: Config,
        db: Db,
        planner: Arc<dyn PlanningAgent>,
        breakdown: Arc<dyn BreakdownAgent>,
        task_agent: Arc<dyn TaskAgent>,
        git_host: Arc<dyn GitHost>,
    ) -> AppState {
        let crypto = MasterKey::derive(&config.master_key)
            .expect("master key material validated non-empty at config load");
        AppState {
            config: Arc::new(config),
            db,
            hub: Arc::new(Hub::new()),
            crypto: Arc::new(crypto),
            planner,
            breakdown,
            task_agent,
            git_host,
            inflight: Arc::new(Mutex::new(HashSet::new())),
            caps: Arc::new(CapabilityStore::new()),
            advertised_base: Arc::new(Mutex::new(None)),
            notify: Arc::new(tokio::sync::Notify::new()),
            refresh_locks: Arc::new(Mutex::new(HashMap::new())),
            cancel_registry: Arc::new(std::sync::Mutex::new(HashMap::new())),
            #[cfg(test)]
            test_pipeline_hook: None,
        }
    }

    /// Record Dearborn's loopback origin (`http://host:port`) once the listener
    /// is bound, so planning runs can build the agent's MCP config URL. Idempotent
    /// last-write-wins.
    pub fn set_advertised_base(&self, base: impl Into<String>) {
        *self.advertised_base.lock().expect("base mutex poisoned") = Some(base.into());
    }

    /// The advertised loopback origin, if set (see [`set_advertised_base`](Self::set_advertised_base)).
    pub fn advertised_base(&self) -> Option<String> {
        self.advertised_base
            .lock()
            .expect("base mutex poisoned")
            .clone()
    }

    /// Hand out `project_id`'s canonical-checkout refresh lock (T-511),
    /// creating it on first use. The same project id always yields the same
    /// underlying `tokio::sync::Mutex` (checked via `Arc::ptr_eq` in tests),
    /// so every caller provisioning against that project actually excludes
    /// every other. See [`AppState::refresh_locks`] for why this exists.
    pub fn project_refresh_lock(&self, project_id: &str) -> Arc<tokio::sync::Mutex<()>> {
        let mut map = self
            .refresh_locks
            .lock()
            .expect("refresh_locks mutex poisoned");
        map.entry(project_id.to_string())
            .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(())))
            .clone()
    }

    /// Claim the in-flight slot for `epic_id` for a planning run.
    ///
    /// Returns `Some(guard)` if no run was already active for the epic — the
    /// caller spawns the run and holds the guard for its lifetime; dropping it
    /// frees the slot. Returns `None` if a run is already in flight (the caller
    /// then ignores the trigger).
    pub fn try_acquire_run(&self, epic_id: &str) -> Option<InflightGuard> {
        let mut set = self.inflight.lock().expect("inflight mutex poisoned");
        if set.contains(epic_id) {
            return None;
        }
        set.insert(epic_id.to_string());
        Some(InflightGuard {
            set: self.inflight.clone(),
            epic_id: epic_id.to_string(),
        })
    }
}

#[cfg(test)]
impl AppState {
    /// Attach the T-510 test-only pipeline hook (see
    /// [`AppState::test_pipeline_hook`]) for a concurrency test to gate the
    /// claimed-epic body deterministically.
    pub fn with_pipeline_hook(mut self, hook: worker::PipelineHook) -> AppState {
        self.test_pipeline_hook = Some(hook);
        self
    }
}

/// RAII claim on an epic's planning in-flight slot. Frees the slot on drop, so
/// the slot is released however the run ends (completion, error, or panic).
pub struct InflightGuard {
    set: Arc<Mutex<HashSet<String>>>,
    epic_id: String,
}

impl Drop for InflightGuard {
    fn drop(&mut self) {
        if let Ok(mut set) = self.set.lock() {
            set.remove(&self.epic_id);
        }
    }
}

/// Build the application router.
///
/// `/health` is public; every other API route sits behind the bearer-token
/// layer. Any request that matches **no** API route falls through to the SPA
/// static handler (the built Vite assets), so the HTML/JS load without auth and
/// the user can then enter their token — auth is enforced on the API calls the
/// SPA makes, not on serving the static shell.
pub fn app(state: AppState) -> Router {
    // `/health` is public; `/ws` authenticates the handshake in-handler (the
    // header-only bearer middleware would reject browser WS handshakes, which
    // carry the token in the query string instead).
    let public = Router::new()
        .route("/health", get(health))
        .route("/ws", get(ws::ws_handler))
        // Dearborn's local MCP server for planning runs (T-203). Authed by the
        // per-run capability token in the `:cap` path segment, NOT the browser
        // bearer token — so it lives outside the bearer layer, like `/ws`.
        .route("/mcp/:cap", axum::routing::post(mcp::mcp_endpoint));

    let protected = Router::new()
        .route("/whoami", get(whoami))
        .route(
            "/settings",
            get(agent_settings::get_settings).put(agent_settings::put_settings),
        )
        .route(
            "/projects",
            get(projects::list_projects).post(projects::create_project),
        )
        .route(
            "/projects/:id",
            get(projects::get_project)
                .patch(projects::update_project)
                .delete(projects::delete_project),
        )
        .route(
            "/projects/:id/refresh",
            axum::routing::post(projects::refresh_project),
        )
        .route("/projects/:id/board", get(board::get_board))
        .route(
            "/projects/:id/tasks",
            axum::routing::post(tasks::create_project_task),
        )
        .route(
            "/projects/:id/epics",
            get(epics::list_epics).post(epics::create_epic),
        )
        .route(
            "/projects/:id/agent-settings",
            get(agent_settings::get_project_agent_settings),
        )
        .route(
            "/projects/:id/agent-settings/:slot",
            axum::routing::put(agent_settings::put_agent_setting),
        )
        .route("/epics/:id", get(epics::get_epic).patch(epics::update_epic))
        .route(
            "/epics/:id/messages",
            axum::routing::post(epics::post_message),
        )
        .route("/epics/:id/transcript", get(epics::get_transcript))
        .route("/epics/:id/sessions", get(epics::list_sessions))
        .route(
            "/epics/:id/advance-phase",
            axum::routing::post(epics::advance_phase),
        )
        .route(
            "/epics/:id/breakdown",
            axum::routing::post(breakdown::trigger_breakdown),
        )
        .route("/epics/:id/dag", get(tasks::get_dag))
        .route("/epics/:id/lane", axum::routing::post(lanes::set_epic_lane))
        .route(
            "/epics/:id/tasks",
            axum::routing::post(tasks::create_epic_task),
        )
        .route(
            "/epics/:id/dependencies",
            axum::routing::post(tasks::post_dependency).delete(tasks::remove_dependency),
        )
        .route(
            "/tasks/:id",
            get(tasks::get_task_by_id)
                .patch(tasks::patch_task)
                .delete(tasks::remove_task),
        )
        .route("/tasks/:id/retry", axum::routing::post(tasks::retry_task))
        .route("/tasks/:id/run", axum::routing::post(tasks::run_task))
        .route("/tasks/:id/runs", get(evidence::list_task_runs))
        .route("/runs/:id", get(evidence::get_run))
        .route_layer(middleware::from_fn_with_state(
            state.clone(),
            auth::require_bearer,
        ));

    let mut router = public.merge(protected);

    // Serve the built SPA (and its client-side-routing fallback) for everything
    // the API routes above don't claim. Degrade gracefully if it isn't built.
    if let Some(spa) = spa_service(&state.config.static_dir) {
        router = router.fallback_service(spa);
    }

    router.layer(TraceLayer::new_for_http()).with_state(state)
}

/// Build the static-file service for the built SPA at `dir`, or `None` if `dir`
/// doesn't exist (dev without a client build). `ServeDir` serves real asset
/// files; any unknown path (a client-side route like `/projects/123`) falls
/// back to `index.html` so the Vue router can take over — an SPA fallback.
///
/// Returning `None` (rather than crashing) lets `cargo run` still serve the API
/// when the client hasn't been built; a warning tells the operator how to fix it.
fn spa_service(static_dir: &str) -> Option<ServeDir<ServeFile>> {
    let dir = Path::new(static_dir);
    let index = dir.join("index.html");
    if !index.is_file() {
        tracing::warn!(
            static_dir = %static_dir,
            "no built SPA found (missing {}); serving API only — run `npm run build` in ./client",
            index.display()
        );
        return None;
    }
    tracing::info!(static_dir = %static_dir, "serving built SPA with client-side-routing fallback");
    Some(ServeDir::new(dir).fallback(ServeFile::new(index)))
}

/// Liveness probe. Public — returns `200 OK` with a small JSON body.
async fn health() -> Json<Value> {
    Json(json!({ "status": "ok" }))
}

/// Authenticated token check. Useful for the client's token-entry screen.
async fn whoami() -> Json<Value> {
    Json(json!({ "status": "authenticated" }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::{header::AUTHORIZATION, Request, StatusCode};
    use tower::ServiceExt; // for `oneshot`

    const TOKEN: &str = "s3cret-token";

    async fn test_app() -> Router {
        let db = Db::connect(":memory:").await.unwrap();
        db.run_migrations().await.unwrap();
        app(AppState::new(Config::for_test(TOKEN), db))
    }

    /// Build an app whose SPA static dir is a freshly-created temp dir holding a
    /// sentinel `index.html`, so the static/SPA-fallback path is exercised.
    async fn test_app_with_spa(marker: &str) -> (Router, std::path::PathBuf) {
        let dir = std::env::temp_dir().join(format!("dearborn-spa-test-{}", ulid::Ulid::new()));
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("index.html"), marker).unwrap();
        let db = Db::connect(":memory:").await.unwrap();
        db.run_migrations().await.unwrap();
        let mut config = Config::for_test(TOKEN);
        config.static_dir = dir.to_string_lossy().into_owned();
        (app(AppState::new(config, db)), dir)
    }

    async fn body_text(response: axum::response::Response) -> String {
        let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        String::from_utf8(bytes.to_vec()).unwrap()
    }

    async fn body_json(response: axum::response::Response) -> Value {
        let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        serde_json::from_slice(&bytes).unwrap()
    }

    #[tokio::test]
    async fn health_is_public_and_returns_200_ok() {
        let response = test_app()
            .await
            .oneshot(
                Request::builder()
                    .uri("/health")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(body_json(response).await, json!({ "status": "ok" }));
    }

    #[tokio::test]
    async fn protected_route_without_token_is_401() {
        let response = test_app()
            .await
            .oneshot(
                Request::builder()
                    .uri("/whoami")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn protected_route_with_wrong_token_is_401() {
        let response = test_app()
            .await
            .oneshot(
                Request::builder()
                    .uri("/whoami")
                    .header(AUTHORIZATION, "Bearer not-the-token")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn protected_route_with_correct_token_is_200() {
        let response = test_app()
            .await
            .oneshot(
                Request::builder()
                    .uri("/whoami")
                    .header(AUTHORIZATION, format!("Bearer {TOKEN}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            body_json(response).await,
            json!({ "status": "authenticated" })
        );
    }

    #[test]
    fn default_bind_is_well_formed() {
        assert!(config::DEFAULT_BIND.parse::<std::net::SocketAddr>().is_ok());
    }

    #[tokio::test]
    async fn spa_served_at_root_when_built() {
        let marker = "<!doctype html><title>dearborn-spa-marker</title>";
        let (app, dir) = test_app_with_spa(marker).await;
        let response = app
            .oneshot(Request::builder().uri("/").body(Body::empty()).unwrap())
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(body_text(response).await, marker);
        std::fs::remove_dir_all(dir).ok();
    }

    #[tokio::test]
    async fn unknown_client_route_falls_back_to_index_html() {
        let marker = "<!doctype html><title>dearborn-spa-marker</title>";
        let (app, dir) = test_app_with_spa(marker).await;
        // A client-side-routing path (not an API route, not a real file) must
        // return index.html so the Vue router can take over.
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/foo/bar")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(body_text(response).await, marker);
        std::fs::remove_dir_all(dir).ok();
    }

    #[tokio::test]
    async fn api_routes_win_over_spa_fallback() {
        let (app, dir) = test_app_with_spa("spa").await;
        // `/projects` is a real API route: it must still enforce auth (401),
        // never be shadowed by the static/SPA fallback.
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/projects")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
        assert_eq!(body_json(response).await["error"]["code"], "unauthorized");
        std::fs::remove_dir_all(dir).ok();
    }
}
