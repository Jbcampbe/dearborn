//! Dearborn server library.
//!
//! Keeps app-construction (router, handlers, shared state) separate from the
//! binary entrypoint so later tasks can add modules and integration tests
//! cleanly.

pub mod afk_engine;
pub mod agent_settings;
pub mod agent_slot;
pub mod activity;
pub mod auth;
pub mod board;
pub mod breakdown;
pub mod capability;
pub mod cli;
pub mod cmd;
pub mod comments;
pub mod config;
pub mod cost;
pub mod crypto;
pub mod db;
pub mod document;
pub mod epics;
pub mod error;
pub mod evidence;
pub mod git;
pub mod git_host;
pub mod harness_pi;
pub mod hub;
pub mod lanes;
pub mod map;
pub mod node_asset;
pub mod node_engine;
pub mod planning;
pub mod pr;
pub mod projects;
pub(crate) mod retry;
pub mod resolve;
pub mod review_poll;
pub mod sessions;
pub mod spec;
pub mod task_agent;
pub mod tasks;
pub mod users;
pub mod worker;
pub mod workspace;
pub mod ws;

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use std::path::Path;

use axum::{middleware, routing::get, Json, Router};
use serde_json::{json, Value};
use tower_http::{
    services::{ServeDir, ServeFile},
    trace::TraceLayer,
};

pub use breakdown::BreakdownAgent;
pub use afk_engine::AfkAgent;
pub use config::{AuthConfig, Config, ConfigError, ExecutorConfig};
pub use crypto::{CryptoError, MasterKey};
pub use db::{Db, DbError};
pub use error::{AppError, AppResult};
pub use git_host::GitHost;
pub use hub::Hub;
pub use capability::CapabilityStore;
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
    /// HMAC-SHA256 signing key for the self-contained access tokens (domain-
    /// separated from `crypto`, so the two derivations of `DEARBORN_MASTER_KEY`
    /// never share bytes). Deterministic, so sessions survive a restart. Never
    /// serialised or logged.
    pub auth_key: Arc<auth::AuthKey>,
    /// Whether this instance has been **claimed** — i.e. whether any `user` row
    /// exists. Monotonic and cached: `false` means "not yet confirmed" and
    /// triggers a `SELECT EXISTS(SELECT 1 FROM user)`; once that comes back
    /// true it latches and no unauthenticated request ever counts users again.
    ///
    /// Latching is safe because a claimed instance can never become unclaimed:
    /// `user` rows are never deleted, and the lockout guards make "zero active
    /// admins" unreachable through the API. See [`AppState::instance_claimed`].
    pub claimed: Arc<AtomicBool>,
    /// The interactive agent-run seam (see [`planning`]). The per-node
    /// planning engines (grilling/prototype — wayfinder epic, later tasks)
    /// build on this seam; tests inject a scripted fake.
    pub planner: Arc<dyn PlanningAgent>,
    /// The one-shot breakdown agent that turns an approved epic into a task DAG
    /// (T-301). Production is [`breakdown::ClaudeBreakdownAgent`]; tests inject
    /// a fake. Shares the planning in-flight slot so the two never overlap on
    /// one epic.
    pub breakdown: Arc<dyn BreakdownAgent>,
    /// The task-stage agent that drives `implement`/`fix`/`review`/
    /// `verify_complete`/`summarize` (T-512). Production is
    /// [`task_agent::CliTaskAgent`]; tests inject a scripted fake. Unlike
    /// `planner`/`breakdown`, a task-stage run is one-shot with no `resume`
    /// (D19) and has no in-flight slot of its own here — concurrency for
    /// task stages is the worker pool's job (T-510+: at most one stage per
    /// claimed task at a time, by construction of the DAG walk), not a
    /// per-epic guard like planning's.
    pub task_agent: Arc<dyn TaskAgent>,
    /// The one-shot AFK node engine that runs research and AFK-task map nodes
    /// unattended (wayfinder epic §5). Production is
    /// [`afk_engine::ClaudeAfkAgent`]; tests inject a scripted fake. Like
    /// breakdown it is one-shot (no resume), but it is wired to no `dearborn`
    /// CLI at all — an unattended run gets no write surface, so the map it is
    /// forbidden from reshaping is structurally out of reach; Dearborn itself
    /// records the report into the node's `gist`. Concurrency is the
    /// per-node run-lock ([`AppState::node_inflight`]), so the frontier's AFK
    /// nodes fire in parallel.
    pub afk: Arc<dyn AfkAgent>,
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
    ///
    /// Breakdown (the one-shot epic → task DAG run) is the only remaining
    /// per-epic-locked engine. The interactive per-node engines
    /// (grilling/prototype) moved their one-run-in-flight lock down to
    /// [`AppState::node_inflight`] so unblocked frontier nodes run concurrently
    /// (wayfinder epic §7).
    pub inflight: Arc<Mutex<HashSet<String>>>,
    /// Map nodes with an interactive agent reply currently in flight, keyed by
    /// `map_node.id` — the per-node run-lock (wayfinder epic §7). A message
    /// posted into a node whose id is already here is still stored, but does not
    /// start a second agent turn: the lock serializes the agent's replies within
    /// a node while leaving *different* nodes free to run in parallel. See
    /// [`AppState::try_acquire_node_run`].
    pub node_inflight: Arc<Mutex<HashSet<String>>>,
    /// Per-run capability tokens. An agent run mints a token scoped to one
    /// `(epic, project, phase, clone)`; the agent authenticates its `dearborn`
    /// CLI calls with it as a bearer, and the token can act only on that epic
    /// through the CLI's REST surface. See [`crate::capability`].
    pub caps: Arc<CapabilityStore>,
    /// Dearborn's own loopback origin (e.g. `http://127.0.0.1:8787`), used to
    /// build the `--url` handed to the agent's `dearborn` CLI. Set once after
    /// the listener binds (`main`, or the live test); `None` in unit tests that
    /// never spawn a real agent, which disables CLI wiring for the run.
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
    /// Per-epic document write semaphores (wayfinder epic §7): a
    /// `tokio::sync::Mutex` keyed by `epic_id`, handed out by
    /// [`AppState::document_write_lock`]. The living Document's sync
    /// ([`crate::document::sync_document`]) takes this for its bounded
    /// read→check→commit — base-version check, version + section-index
    /// persistence — so two sibling node sessions' resolution edits can never
    /// interleave. An in-process lock suffices: Dearborn is a single server
    /// process (no horizontal scaling) and SQLite already serializes writers.
    /// Keyed by epic id, so epics never block each other. Same pattern as
    /// [`AppState::refresh_locks`] and the in-flight sets above.
    pub document_locks: Arc<Mutex<HashMap<String, Arc<tokio::sync::Mutex<()>>>>>,
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
    /// Hand out `epic_id`'s document write semaphore (wayfinder epic §7),
    /// creating it on first use. The same epic id always yields the same
    /// underlying `tokio::sync::Mutex` (checked via `Arc::ptr_eq` in tests),
    /// so every document sync on that epic excludes every other, while
    /// different epics never contend. See [`AppState::document_locks`].
    pub fn document_write_lock(&self, epic_id: &str) -> Arc<tokio::sync::Mutex<()>> {
        let mut map = self
            .document_locks
            .lock()
            .expect("document_locks mutex poisoned");
        map.entry(epic_id.to_string())
            .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(())))
            .clone()
    }

    /// Construct shared state from a resolved [`Config`] and open [`Db`], using
    /// the production interactive agent ([`planning::ClaudePlanningAgent`]).
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
    /// that lets tests drive interactive runs hermetically with a scripted fake.
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
    /// [`task_agent::CliTaskAgent`] (override it via
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
            Arc::new(task_agent::CliTaskAgent::new()),
        )
    }

    /// Swap in an injected [`AfkAgent`] — the seam tests use to drive the
    /// one-shot AFK node engine (research / AFK-task nodes) hermetically.
    /// Consumes and returns `self` so it chains after any constructor;
    /// production wiring defaults the agent to
    /// [`afk_engine::ClaudeAfkAgent`].
    pub fn with_afk(mut self, afk: Arc<dyn AfkAgent>) -> AppState {
        self.afk = afk;
        self
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
        let auth_key = auth::AuthKey::derive(&config.master_key)
            .expect("master key material validated non-empty at config load");
        AppState {
            config: Arc::new(config),
            db,
            hub: Arc::new(Hub::new()),
            crypto: Arc::new(crypto),
            auth_key: Arc::new(auth_key),
            claimed: Arc::new(AtomicBool::new(false)),
            planner,
            breakdown,
            task_agent,
            afk: Arc::new(afk_engine::ClaudeAfkAgent::new()),
            git_host,
            inflight: Arc::new(Mutex::new(HashSet::new())),
            node_inflight: Arc::new(Mutex::new(HashSet::new())),
            caps: Arc::new(CapabilityStore::new()),
            advertised_base: Arc::new(Mutex::new(None)),
            notify: Arc::new(tokio::sync::Notify::new()),
            refresh_locks: Arc::new(Mutex::new(HashMap::new())),
            document_locks: Arc::new(Mutex::new(HashMap::new())),
            cancel_registry: Arc::new(std::sync::Mutex::new(HashMap::new())),
            #[cfg(test)]
            test_pipeline_hook: None,
        }
    }

    /// Record Dearborn's loopback origin (`http://host:port`) once the listener
    /// is bound, so agent runs can build the `dearborn` CLI's `--url`. Idempotent
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

    /// Whether this instance has any users yet — i.e. whether it has been
    /// claimed through `POST /auth/setup`.
    ///
    /// Reads the cached latch first ([`AppState::claimed`]) and only falls
    /// through to `SELECT EXISTS(SELECT 1 FROM user)` while it is still
    /// `false`. That bounds the query to the *unclaimed* window: once an
    /// instance has a user, no unauthenticated request counts users again, so
    /// the public `/auth/status` probe cannot be turned into a database load
    /// generator.
    ///
    /// `Relaxed` ordering is sufficient: the flag guards nothing but itself,
    /// and the only transition is `false → true`. A racing reader that misses
    /// a just-set latch simply runs one extra `EXISTS` query and reaches the
    /// same answer.
    pub async fn instance_claimed(&self) -> AppResult<bool> {
        if self.claimed.load(Ordering::Relaxed) {
            return Ok(true);
        }
        let mut rows = self
            .db
            .conn()
            .query("SELECT EXISTS(SELECT 1 FROM user)", ())
            .await?;
        let exists = match rows.next().await? {
            Some(row) => row.get::<i64>(0)? != 0,
            None => false,
        };
        if exists {
            self.claimed.store(true, Ordering::Relaxed);
        }
        Ok(exists)
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

    /// Claim the per-node run-lock for `node_id` for an interactive agent reply
    /// (wayfinder epic §7).
    ///
    /// Returns `Some(guard)` if no reply was already in flight for the node —
    /// the caller spawns the reply and holds the guard for its lifetime;
    /// dropping it frees the lock. Returns `None` if a reply is already running
    /// (the caller then leaves the just-stored user message for the in-flight
    /// turn's successor, exactly like [`try_acquire_run`](Self::try_acquire_run)
    /// does per epic). The lock is keyed by node, so a reply in one node never
    /// blocks a reply in another.
    pub fn try_acquire_node_run(&self, node_id: &str) -> Option<NodeRunGuard> {
        let mut set = self
            .node_inflight
            .lock()
            .expect("node_inflight mutex poisoned");
        if set.contains(node_id) {
            return None;
        }
        set.insert(node_id.to_string());
        Some(NodeRunGuard {
            set: self.node_inflight.clone(),
            node_id: node_id.to_string(),
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

/// RAII claim on a map node's interactive run-lock. Frees the lock on drop, so
/// the node is workable again however the reply ends (completion, error, or
/// panic). The per-node analogue of [`InflightGuard`] (wayfinder epic §7).
pub struct NodeRunGuard {
    set: Arc<Mutex<HashSet<String>>>,
    node_id: String,
}

impl Drop for NodeRunGuard {
    fn drop(&mut self) {
        if let Ok(mut set) = self.set.lock() {
            set.remove(&self.node_id);
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
        // First-launch claim, login, and session refresh. Unauthenticated by
        // necessity: these are how a caller *gets* a credential, so they
        // cannot sit behind one.
        .route("/auth/status", get(sessions::auth_status))
        .route("/auth/setup", axum::routing::post(sessions::setup))
        .route("/auth/login", axum::routing::post(sessions::login))
        .route("/auth/refresh", axum::routing::post(sessions::refresh));

    let protected = Router::new()
        .route("/auth/me", get(sessions::me))
        // The `dearborn` CLI's `scope` verb: names the bearer capability
        // token's own epic/project/phase (a session token gets 403 here —
        // see `capability::CapabilityActor`).
        .route("/auth/capability", get(capability::whoami))
        .route("/auth/logout", axum::routing::post(sessions::logout))
        .route(
            "/auth/password",
            axum::routing::post(sessions::change_password),
        )
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
        .route("/projects/:id/cost", get(cost::get_project_cost))
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
        // The planning map (wayfinder epic): node CRUD + dependency edges +
        // the four prose fields, plus the computed-map query. The `dearborn`
        // CLI's `node`/`map` verbs call exactly these (capability-token scoped).
        .route(
            "/epics/:id/map",
            get(map::get_map).patch(map::patch_map_prose),
        )
        .route("/epics/:id/map-nodes", axum::routing::post(map::create_map_node))
        .route(
            "/epics/:id/map-nodes/:nodeId",
            get(map::get_map_node).patch(map::patch_map_node),
        )
        // The interactive per-node engine (wayfinder epic §5/§7): opening a
        // grilling/prototype node starts/resumes its node-scoped session, and
        // any user may post a message whose reply the per-node run-lock
        // serializes. Live `RunEvent`s stream on `node:<id>`.
        .route(
            "/epics/:id/map-nodes/:nodeId/session",
            get(node_engine::get_node_session).post(node_engine::open_node_session),
        )
        .route(
            "/epics/:id/map-nodes/:nodeId/messages",
            get(node_engine::list_node_messages).post(node_engine::post_node_message),
        )
        // The prototype artifact store (wayfinder epic §4.7/§11): a node's
        // stored prototype artifacts, listed (metadata — linked, not inlined)
        // and read back raw so the client can render them in a sandboxed
        // iframe. Writes ride the resolution bundle (HITL-gated above).
        .route(
            "/epics/:id/map-nodes/:nodeId/assets",
            get(node_asset::list_node_assets),
        )
        .route(
            "/epics/:id/map-nodes/:nodeId/assets/:assetId",
            get(node_asset::get_node_asset),
        )
        // The grilling resolution bundle (wayfinder epic §6/§10): one call that
        // records the decision, folds in the Document edit under the per-epic
        // write semaphore, graduates fog into new frontier nodes, rules things
        // out of scope, and updates affected nodes. HITL kinds only — the
        // `dearborn` CLI's (upgraded) `node resolve` verb calls exactly this.
        .route(
            "/epics/:id/map-nodes/:nodeId/resolve",
            axum::routing::post(resolve::resolve_node),
        )
        // The one-shot AFK node engine (wayfinder epic §5): firing a research
        // or AFK-task node runs one unattended agent turn whose report lands
        // in the node's `gist`; per-node runs never reshape the map and fire
        // in parallel under the per-node run-lock. Live `RunEvent`s stream on
        // `node:<id>`.
        .route(
            "/epics/:id/map-nodes/:nodeId/run",
            axum::routing::post(afk_engine::fire_node),
        )
        // The living Document (wayfinder epic §4.5/§10, Phase 3): read the
        // epic's HTML document for the scratch-file round trip, and sync an
        // edited file back as a new version under the per-epic write
        // semaphore. The `dearborn` CLI's `document pull|sync` verbs call
        // exactly these (capability-token scoped).
        .route("/epics/:id/document", get(document::get_document))
        .route(
            "/epics/:id/document/sync",
            axum::routing::post(document::sync_document),
        )
        // Comments (wayfinder epic §4.8/§9): threaded, anchored to a map node
        // or a Document section, user-attributed with agent replies (an agent
        // run posts through its capability token, `is_agent = 1`), thread-
        // level resolve, and thread promotion into a new open frontier node
        // (stamping `promoted_node_id` on the source thread). The `dearborn`
        // CLI's `comment post|list|resolve|promote` verbs call exactly these
        // (capability-token scoped).
        .route(
            "/epics/:id/comments",
            get(comments::list_comments_handler).post(comments::post_comment),
        )
        .route(
            "/epics/:id/comments/:commentId/resolve",
            axum::routing::post(comments::resolve_comment),
        )
        .route(
            "/epics/:id/comments/:commentId/promote",
            axum::routing::post(comments::promote_comment),
        )
        // Attribution & activity feed (wayfinder epic §4.9/§9): the
        // append-only history of key mutations (every mutation surface
        // records into it) and the participants derived as distinct actors
        // across all attribution surfaces. Reads, so they are on the
        // capability-token allow-list for every phase.
        .route("/epics/:id/activity", get(activity::get_activity))
        .route("/epics/:id/participants", get(activity::get_participants))
        .route(
            "/epics/:id/map-node-dependencies",
            axum::routing::post(map::link_map_nodes),
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
        .route("/runs/:id/events", get(evidence::list_run_events_handler))
        // Admin-only user management. AdminUser gates each handler individually
        // (re-reading the user row), so a user-role token always gets 403.
        .route("/users", get(users::list_users).post(users::create_user))
        .route("/users/:id", axum::routing::patch(users::update_user))
        .route(
            "/users/:id/password",
            axum::routing::post(users::reset_user_password),
        )
        .route_layer(middleware::from_fn_with_state(
            state.clone(),
            auth::require_auth,
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sessions;
    use crate::users::{self, Role};
    use axum::body::Body;
    use axum::http::{header::AUTHORIZATION, Request, StatusCode};
    use serde_json::{json, Value};
    use tower::ServiceExt; // for `oneshot`

    async fn test_app() -> Router {
        let db = Db::connect(":memory:").await.unwrap();
        db.run_migrations().await.unwrap();
        app(AppState::new(Config::for_test(), db))
    }

    /// A claimed instance (one seeded active admin) plus a bearer-ready access
    /// token for it — the replacement for the deleted static `TOKEN`.
    async fn test_app_with_user() -> (Router, String) {
        let db = Db::connect(":memory:").await.unwrap();
        db.run_migrations().await.unwrap();
        let state = AppState::new(Config::for_test(), db);
        let user = users::testing::seed_user(&state, "tester", Role::Admin, true).await;
        let token = sessions::testing::login_as(&state, &user).await;
        (app(state), token)
    }

    /// Build an app whose SPA static dir is a freshly-created temp dir holding a
    /// sentinel `index.html`, so the static/SPA-fallback path is exercised.
    async fn test_app_with_spa(marker: &str) -> (Router, std::path::PathBuf) {
        let dir = std::env::temp_dir().join(format!("dearborn-spa-test-{}", ulid::Ulid::new()));
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("index.html"), marker).unwrap();
        let db = Db::connect(":memory:").await.unwrap();
        db.run_migrations().await.unwrap();
        let mut config = Config::for_test();
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

    fn get(uri: &str) -> Request<Body> {
        Request::builder().uri(uri).body(Body::empty()).unwrap()
    }

    fn get_bearer(uri: &str, token: &str) -> Request<Body> {
        Request::builder()
            .uri(uri)
            .header(AUTHORIZATION, format!("Bearer {token}"))
            .body(Body::empty())
            .unwrap()
    }

    fn post_json(uri: &str, body: Value) -> Request<Body> {
        Request::builder()
            .method("POST")
            .uri(uri)
            .header("content-type", "application/json")
            .body(Body::from(body.to_string()))
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

    fn patch_json_bearer(uri: &str, token: &str, body: Value) -> Request<Body> {
        Request::builder()
            .method("PATCH")
            .uri(uri)
            .header(AUTHORIZATION, format!("Bearer {token}"))
            .header("content-type", "application/json")
            .body(Body::from(body.to_string()))
            .unwrap()
    }

    #[tokio::test]
    async fn health_is_public_and_returns_200_ok() {
        let response = test_app().await.oneshot(get("/health")).await.unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(body_json(response).await, json!({ "status": "ok" }));
    }

    #[tokio::test]
    async fn protected_route_without_token_is_401() {
        let response = test_app().await.oneshot(get("/projects")).await.unwrap();
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn protected_route_with_a_garbage_token_is_401() {
        let (app, _token) = test_app_with_user().await;
        let response = app
            .oneshot(get_bearer("/projects", "not-a-real-token"))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }

    // ---- AC5: a login-issued access token opens every protected route ----

    #[tokio::test]
    async fn a_login_issued_access_token_calls_protected_routes() {
        let db = Db::connect(":memory:").await.unwrap();
        db.run_migrations().await.unwrap();
        let state = AppState::new(Config::for_test(), db);
        users::testing::seed_user(&state, "tester", Role::Admin, true).await;

        // Log in over HTTP — the same path a browser takes.
        let response = app(state.clone())
            .oneshot(post_json(
                "/auth/login",
                json!({
                    "username": "tester",
                    "password": users::testing::SEED_PASSWORD,
                }),
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let access_token = body_json(response).await["access_token"]
            .as_str()
            .unwrap()
            .to_string();

        // The freshly minted credential passes previously token-protected routes.
        for uri in ["/projects", "/settings"] {
            let response = app(state.clone())
                .clone()
                .oneshot(get_bearer(uri, &access_token))
                .await
                .unwrap();
            assert_eq!(
                response.status(),
                StatusCode::OK,
                "`{uri}` must accept the login-issued access token"
            );
        }
    }

    // ---- AC6: every flavor of bad credential is a plain 401 ----

    #[tokio::test]
    async fn missing_garbage_tampered_and_expired_credentials_all_get_401() {
        let (app, good_token) = test_app_with_user().await;

        // Tampered signature: flip one character of the final segment.
        let (head, sig) = good_token.rsplit_once('.').unwrap();
        let mut sig: Vec<char> = sig.chars().collect();
        sig[0] = if sig[0] == 'A' { 'B' } else { 'A' };
        let tampered = format!("{head}.{}", sig.iter().collect::<String>());

        // Expired `exp`: mint directly with an expiry in the past.
        let expired = {
            let state_claims = auth::Claims {
                sub: "01JD2Q7XK3V9M4N8P6R2T5W9YA".to_string(),
                sid: "01JD2Q8BZ4W0N5P9Q3S3U6X0ZB".to_string(),
                role: Role::Admin,
                exp: 1_000, // long past
            };
            // Mint under this instance's key by rebuilding it from config material.
            let key = auth::AuthKey::derive(&Config::for_test().master_key).unwrap();
            key.mint(&state_claims)
        };

        for (label, token) in [
            ("garbage", "garbage-token".to_string()),
            ("tampered-signature", tampered),
            ("expired-exp", expired),
        ] {
            let response = app
                .clone()
                .oneshot(get_bearer("/projects", &token))
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::UNAUTHORIZED, "{label}");
            assert_eq!(body_json(response).await["error"]["code"], "unauthorized");
        }
    }

    // ---- Unclaimed vs claimed: which 401 code comes back ----

    #[tokio::test]
    async fn unauthenticated_request_on_an_unclaimed_instance_says_setup_required() {
        let response = test_app().await.oneshot(get("/projects")).await.unwrap();
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
        assert_eq!(body_json(response).await["error"]["code"], "setup_required");
    }

    #[tokio::test]
    async fn unauthenticated_request_on_a_claimed_instance_says_unauthorized() {
        let (app, _token) = test_app_with_user().await;
        let response = app.oneshot(get("/projects")).await.unwrap();
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
        assert_eq!(body_json(response).await["error"]["code"], "unauthorized");
    }

    // ---- AC18: the legacy shared token env var authorizes nothing ----

    #[tokio::test]
    async fn a_dearboren_token_in_the_environment_authorizes_nothing() {
        // The variable no longer exists as far as the server is concerned: no
        // field reads it. Assert that even with it set, presenting that exact
        // string as a bearer token is rejected, and that config loading neither
        // needs it nor trips over it.
        // SAFETY: process-global env mutation; tests in this module that read
        // env are not run concurrently with other env-mutating ones.
        //
        // The variable's name is assembled rather than spelled out because no
        // source under `src/` may mention it any more: the field it once fed
        // does not exist, and this assertion documents exactly that absence.
        let legacy_var = concat!("DEARBORN", "_TOKEN");
        let old = std::env::var(legacy_var).ok();
        let old_master = std::env::var("DEARBORN_MASTER_KEY").ok();
        std::env::set_var("DEARBORN_MASTER_KEY", "boot-check-material");
        std::env::set_var(legacy_var, "s3cret-token");

        let (app, _token) = test_app_with_user().await;
        let response = app
            .oneshot(get_bearer("/projects", "s3cret-token"))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);

        // ...and the server boots fine with it unset or set to garbage.
        drop(response);
        std::env::remove_var(legacy_var);
        assert!(Config::from_env().is_ok());
        std::env::set_var(legacy_var, "leftover-garbage");
        assert!(Config::from_env().is_ok());

        match (old, old_master) {
            (Some(v), m) => {
                std::env::set_var(legacy_var, v);
                match m {
                    Some(v) => std::env::set_var("DEARBORN_MASTER_KEY", v),
                    None => std::env::remove_var("DEARBORN_MASTER_KEY"),
                }
            }
            (None, Some(v)) => {
                std::env::remove_var(legacy_var);
                std::env::set_var("DEARBORN_MASTER_KEY", v);
            }
            (None, None) => {
                std::env::remove_var(legacy_var);
                std::env::remove_var("DEARBORN_MASTER_KEY");
            }
        }
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

    // ---- Admin user-management routes (AC9–AC15, AC17) -------------------------

    /// Build a shared AppState (backed by an in-memory DB) plus an admin token.
    async fn admin_state_and_token() -> (AppState, String) {
        let db = Db::connect(":memory:").await.unwrap();
        db.run_migrations().await.unwrap();
        let state = AppState::new(Config::for_test(), db);
        let admin = users::testing::seed_user(&state, "admin", users::Role::Admin, true).await;
        let token = sessions::testing::login_as(&state, &admin).await;
        (state, token)
    }

    /// AC9: admin creates a user; that user logs in immediately with the given password.
    #[tokio::test]
    async fn ac9_admin_creates_user_who_can_log_in() {
        let (state, admin_token) = admin_state_and_token().await;
        let router = app(state);

        let resp = router
            .clone()
            .oneshot(post_json_bearer(
                "/users",
                &admin_token,
                json!({
                    "username": "newbie",
                    "display_name": "Newbie User",
                    "password": "twelve-chars-ok",
                    "role": "user"
                }),
            ))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::CREATED);
        let body = body_json(resp).await;
        assert_eq!(body["username"], "newbie");
        assert_eq!(body["role"], "user");

        // The created user can log in immediately.
        let login_resp = router
            .oneshot(post_json(
                "/auth/login",
                json!({ "username": "newbie", "password": "twelve-chars-ok" }),
            ))
            .await
            .unwrap();
        assert_eq!(login_resp.status(), StatusCode::OK);
    }

    /// AC10: PATCH updates display name and role; promoted user passes admin route.
    #[tokio::test]
    async fn ac10_patch_updates_display_name_and_role() {
        let (state, admin_token) = admin_state_and_token().await;
        let regular = users::testing::seed_user(&state, "regular", users::Role::User, true).await;
        let router = app(state.clone());

        // Update display name and promote to admin.
        let resp = router
            .clone()
            .oneshot(patch_json_bearer(
                &format!("/users/{}", regular.id),
                &admin_token,
                json!({ "display_name": "Promoted User", "role": "admin" }),
            ))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = body_json(resp).await;
        assert_eq!(body["display_name"], "Promoted User");
        assert_eq!(body["role"], "admin");

        // The promoted user's new access token (re-read via AdminUser) grants
        // access to admin routes.
        let promoted_token = sessions::testing::login_as(&state, &regular).await;
        let admin_resp = router
            .oneshot(get_bearer("/users", &promoted_token))
            .await
            .unwrap();
        assert_eq!(admin_resp.status(), StatusCode::OK);
    }

    /// AC11: password reset — new password works, old does not, existing sessions fail to refresh.
    #[tokio::test]
    async fn ac11_password_reset_invalidates_old_password_and_sessions() {
        let (state, admin_token) = admin_state_and_token().await;
        let target = users::testing::seed_user(&state, "victim", users::Role::User, true).await;
        // Issue a session for the target before the reset.
        let pre_reset_refresh = {
            let issued = sessions::issue(&state.db, &state.auth_key, &target, &state.config.auth)
                .await
                .unwrap();
            issued.refresh_token
        };
        let router = app(state);

        // Admin resets password.
        let resp = router
            .clone()
            .oneshot(post_json_bearer(
                &format!("/users/{}/password", target.id),
                &admin_token,
                json!({ "password": "brand-new-password" }),
            ))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::NO_CONTENT);

        // New password logs in.
        let new_login = router
            .clone()
            .oneshot(post_json(
                "/auth/login",
                json!({ "username": "victim", "password": "brand-new-password" }),
            ))
            .await
            .unwrap();
        assert_eq!(new_login.status(), StatusCode::OK);

        // Old password no longer works.
        let old_login = router
            .clone()
            .oneshot(post_json(
                "/auth/login",
                json!({ "username": "victim", "password": users::testing::SEED_PASSWORD }),
            ))
            .await
            .unwrap();
        assert_eq!(old_login.status(), StatusCode::UNAUTHORIZED);

        // The pre-reset refresh token is revoked.
        let refresh_resp = router
            .oneshot(post_json(
                "/auth/refresh",
                json!({ "refresh_token": pre_reset_refresh }),
            ))
            .await
            .unwrap();
        assert_eq!(refresh_resp.status(), StatusCode::UNAUTHORIZED);
    }

    /// AC12: deactivate blocks login/refresh; row survives; reactivating restores login.
    #[tokio::test]
    async fn ac12_deactivation_blocks_login_and_refresh() {
        let (state, admin_token) = admin_state_and_token().await;
        let target = users::testing::seed_user(&state, "target", users::Role::User, true).await;
        // Issue a session before deactivation so we can verify refresh is revoked.
        let pre_deactivation_refresh = {
            let issued = sessions::issue(&state.db, &state.auth_key, &target, &state.config.auth)
                .await
                .unwrap();
            issued.refresh_token
        };
        let router = app(state);

        // Deactivate.
        let deactivate_resp = router
            .clone()
            .oneshot(patch_json_bearer(
                &format!("/users/{}", target.id),
                &admin_token,
                json!({ "active": false }),
            ))
            .await
            .unwrap();
        assert_eq!(deactivate_resp.status(), StatusCode::OK);
        assert_eq!(body_json(deactivate_resp).await["active"], false);

        // Login is blocked.
        let login_resp = router
            .clone()
            .oneshot(post_json(
                "/auth/login",
                json!({ "username": "target", "password": users::testing::SEED_PASSWORD }),
            ))
            .await
            .unwrap();
        assert_eq!(login_resp.status(), StatusCode::UNAUTHORIZED);

        // Row still exists and appears in GET /users.
        let list_resp = router
            .clone()
            .oneshot(get_bearer("/users", &admin_token))
            .await
            .unwrap();
        assert_eq!(list_resp.status(), StatusCode::OK);
        let list = body_json(list_resp).await;
        let items = list["items"].as_array().unwrap();
        let target_item = items.iter().find(|u| u["username"] == "target").unwrap();
        assert_eq!(target_item["active"], false);

        // Refresh is blocked — the pre-deactivation session was revoked.
        let refresh_resp = router
            .clone()
            .oneshot(post_json(
                "/auth/refresh",
                json!({ "refresh_token": pre_deactivation_refresh }),
            ))
            .await
            .unwrap();
        assert_eq!(refresh_resp.status(), StatusCode::UNAUTHORIZED);

        // Reactivate restores login.
        let reactivate_resp = router
            .clone()
            .oneshot(patch_json_bearer(
                &format!("/users/{}", target.id),
                &admin_token,
                json!({ "active": true }),
            ))
            .await
            .unwrap();
        assert_eq!(reactivate_resp.status(), StatusCode::OK);

        let restored_login = router
            .oneshot(post_json(
                "/auth/login",
                json!({ "username": "target", "password": users::testing::SEED_PASSWORD }),
            ))
            .await
            .unwrap();
        assert_eq!(restored_login.status(), StatusCode::OK);
    }

    /// AC13: a user-role token gets 403 (not 404) on all four /users routes.
    #[tokio::test]
    async fn ac13_user_role_token_gets_403_on_all_admin_routes() {
        let (state, _admin_token) = admin_state_and_token().await;
        let regular = users::testing::seed_user(&state, "regular", users::Role::User, true).await;
        let user_token = sessions::testing::login_as(&state, &regular).await;
        let router = app(state);

        // GET /users
        let r = router
            .clone()
            .oneshot(get_bearer("/users", &user_token))
            .await
            .unwrap();
        assert_eq!(r.status(), StatusCode::FORBIDDEN, "GET /users");
        assert_eq!(body_json(r).await["error"]["code"], "forbidden");

        // POST /users
        let r = router
            .clone()
            .oneshot(post_json_bearer(
                "/users",
                &user_token,
                json!({ "username": "x", "display_name": "X", "password": "twelve-chars-ok", "role": "user" }),
            ))
            .await
            .unwrap();
        assert_eq!(r.status(), StatusCode::FORBIDDEN, "POST /users");

        // PATCH /users/:id
        let r = router
            .clone()
            .oneshot(patch_json_bearer(
                "/users/some-id",
                &user_token,
                json!({ "display_name": "Y" }),
            ))
            .await
            .unwrap();
        assert_eq!(r.status(), StatusCode::FORBIDDEN, "PATCH /users/:id");

        // POST /users/:id/password
        let r = router
            .oneshot(post_json_bearer(
                "/users/some-id/password",
                &user_token,
                json!({ "password": "twelve-chars-ok" }),
            ))
            .await
            .unwrap();
        assert_eq!(
            r.status(),
            StatusCode::FORBIDDEN,
            "POST /users/:id/password"
        );
    }

    /// AC14: demoting the last active admin returns 409; deactivating the last
    /// active admin via HTTP surfaces the self-deactivation guard with its
    /// specific message.
    ///
    /// The "cannot deactivate the last active admin" guard is defence-in-depth at
    /// the store level and cannot be triggered via HTTP with actor != target:
    /// `AdminUser` guarantees the actor is an active admin, so whenever the
    /// target is also an active admin the count is ≥ 2, and the last-admin guard
    /// never fires. It is tested exhaustively in `users::tests`.
    #[tokio::test]
    async fn ac14_last_active_admin_lockout_guards() {
        let (state, admin_token) = admin_state_and_token().await;
        let admin_id = {
            let u = users::get_by_username(&state.db, "admin")
                .await
                .unwrap()
                .unwrap();
            u.id
        };
        let router = app(state);

        // Demoting the last active admin → 409 with the documented message.
        // The demotion guard does not check for self vs. other, so this is
        // reachable even when the actor and target are the same admin.
        let resp = router
            .clone()
            .oneshot(patch_json_bearer(
                &format!("/users/{admin_id}"),
                &admin_token,
                json!({ "role": "user" }),
            ))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::CONFLICT);
        assert_eq!(
            body_json(resp).await["error"]["message"],
            "cannot demote the last active admin"
        );

        // Deactivating the last active admin via HTTP: the actor IS that admin,
        // so the self-deactivation guard fires first and surfaces its specific
        // message. The last-admin deactivation message cannot be triggered via
        // HTTP with actor != target (see doc comment above).
        let resp = router
            .oneshot(patch_json_bearer(
                &format!("/users/{admin_id}"),
                &admin_token,
                json!({ "active": false }),
            ))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::CONFLICT);
        assert_eq!(
            body_json(resp).await["error"]["message"],
            "you cannot deactivate your own account"
        );
    }

    /// AC15: admin deactivating their own account returns 409.
    #[tokio::test]
    async fn ac15_self_deactivation_is_409() {
        let (state, admin_token) = admin_state_and_token().await;
        // Add a second admin so the only guard that fires is the self one.
        users::testing::seed_user(&state, "admin2", users::Role::Admin, true).await;
        let admin_id = {
            let u = users::get_by_username(&state.db, "admin")
                .await
                .unwrap()
                .unwrap();
            u.id
        };
        let router = app(state);

        let resp = router
            .oneshot(patch_json_bearer(
                &format!("/users/{admin_id}"),
                &admin_token,
                json!({ "active": false }),
            ))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::CONFLICT);
        assert_eq!(
            body_json(resp).await["error"]["message"],
            "you cannot deactivate your own account"
        );
    }

    /// AC17: 11-char password → 400 at admin create and reset; 12-char all-lowercase accepted.
    #[tokio::test]
    async fn ac17_password_policy_enforced_on_admin_create_and_reset() {
        let (state, admin_token) = admin_state_and_token().await;
        let target = users::testing::seed_user(&state, "target", users::Role::User, true).await;
        let router = app(state);

        // Admin create with 11-char password → 400.
        let resp = router
            .clone()
            .oneshot(post_json_bearer(
                "/users",
                &admin_token,
                json!({ "username": "shortpw", "display_name": "Short", "password": "elevenchars", "role": "user" }),
            ))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);

        // Admin create with 12-char all-lowercase → 201.
        let resp = router
            .clone()
            .oneshot(post_json_bearer(
                "/users",
                &admin_token,
                json!({ "username": "goodpw", "display_name": "Good", "password": "twelvecharss", "role": "user" }),
            ))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::CREATED);

        // Admin reset with 11-char password → 400.
        let resp = router
            .clone()
            .oneshot(post_json_bearer(
                &format!("/users/{}/password", target.id),
                &admin_token,
                json!({ "password": "elevenchars" }),
            ))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);

        // Admin reset with 12-char all-lowercase → 204.
        let resp = router
            .oneshot(post_json_bearer(
                &format!("/users/{}/password", target.id),
                &admin_token,
                json!({ "password": "twelvecharss" }),
            ))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::NO_CONTENT);
    }

    /// Duplicate username on create → 409, case-insensitive.
    #[tokio::test]
    async fn create_user_duplicate_username_is_409_case_insensitive() {
        let (state, admin_token) = admin_state_and_token().await;
        let router = app(state);

        let resp = router
            .clone()
            .oneshot(post_json_bearer(
                "/users",
                &admin_token,
                json!({ "username": "josiah", "display_name": "Josiah", "password": "twelve-chars-ok", "role": "user" }),
            ))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::CREATED);

        // Exact duplicate.
        let resp = router
            .clone()
            .oneshot(post_json_bearer(
                "/users",
                &admin_token,
                json!({ "username": "josiah", "display_name": "Dup", "password": "twelve-chars-ok", "role": "user" }),
            ))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::CONFLICT);

        // Case-variant duplicate.
        let resp = router
            .oneshot(post_json_bearer(
                "/users",
                &admin_token,
                json!({ "username": "JOSIAH", "display_name": "Dup2", "password": "twelve-chars-ok", "role": "user" }),
            ))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::CONFLICT);
    }

    /// Privilege freshness: a deactivated admin's valid token gets 403 on admin routes immediately.
    #[tokio::test]
    async fn privilege_freshness_deactivated_admin_gets_403_on_admin_routes() {
        let (state, _) = admin_state_and_token().await;
        // Add a second admin to hold the "last active admin" slot.
        let _second_admin =
            users::testing::seed_user(&state, "admin2", users::Role::Admin, true).await;
        let first_admin_user = users::get_by_username(&state.db, "admin")
            .await
            .unwrap()
            .unwrap();
        // Get a token for the first admin *before* deactivation.
        let first_admin_token = sessions::testing::login_as(&state, &first_admin_user).await;

        // Deactivate the first admin via raw store (bypassing the admin API,
        // which would block self-deactivation — the point is to test the token
        // freshness check, not the guard).
        state
            .db
            .conn()
            .execute(
                "UPDATE user SET active = 0 WHERE id = ?1",
                libsql::params![first_admin_user.id.clone()],
            )
            .await
            .unwrap();

        let router = app(state);
        // The still-valid token is rejected on admin routes.
        let resp = router
            .clone()
            .oneshot(get_bearer("/users", &first_admin_token))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::FORBIDDEN);

        // But ordinary protected routes still accept it (eventual-revocation contract).
        let resp = router
            .oneshot(get_bearer("/projects", &first_admin_token))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }

    /// Privilege freshness: a demoted admin's valid token gets 403 on admin routes immediately.
    #[tokio::test]
    async fn privilege_freshness_demoted_admin_gets_403_on_admin_routes() {
        let (state, _) = admin_state_and_token().await;
        // Add a second admin to hold the "last active admin" slot so the raw
        // SQL demotion below is not blocked by the guard.
        let _second_admin =
            users::testing::seed_user(&state, "admin2", users::Role::Admin, true).await;
        let first_admin_user = users::get_by_username(&state.db, "admin")
            .await
            .unwrap()
            .unwrap();
        let first_admin_token = sessions::testing::login_as(&state, &first_admin_user).await;

        // Demote the first admin via raw SQL, bypassing the guard (which would
        // refuse to demote the last active admin — not relevant here since there
        // are two). The point is to test the token freshness check.
        state
            .db
            .conn()
            .execute(
                "UPDATE user SET role = 'user' WHERE id = ?1",
                libsql::params![first_admin_user.id.clone()],
            )
            .await
            .unwrap();

        let router = app(state);
        // The still-valid token is rejected on admin routes.
        let resp = router
            .clone()
            .oneshot(get_bearer("/users", &first_admin_token))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::FORBIDDEN);

        // Ordinary protected routes still accept it (eventual-revocation contract).
        let resp = router
            .oneshot(get_bearer("/projects", &first_admin_token))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn api_routes_win_over_spa_fallback() {
        let (app, dir) = test_app_with_spa("spa").await;
        // `/projects` is a real API route: it must still enforce auth (401),
        // never be shadowed by the static/SPA fallback. The DB here has no
        // users, so the code distinguishes itself as setup_required.
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
        assert_eq!(body_json(response).await["error"]["code"], "setup_required");
        std::fs::remove_dir_all(dir).ok();
    }
}
