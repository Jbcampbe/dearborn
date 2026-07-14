//! One-shot breakdown agent — epic → task DAG (T-301).
//!
//! After planning (product + technical) an epic is *approved* and ready to be
//! broken into work. `POST /epics/:id/breakdown` runs a **single, non-interactive**
//! agent that reads the epic's product + technical context and creates a graph of
//! tasks (thin vertical slices / tracer bullets, per `references/prompts/to-tasks.md`)
//! via the breakdown-phase MCP tools `create_task` / `link_dependency`
//! ([`crate::mcp`]). When the run finishes, Deerborn moves the epic
//! **Planning → Ready** and records the run in `agent_run`.
//!
//! ## Relation to planning
//!
//! This mirrors [`crate::planning`]'s run machinery — the agent sits behind a
//! [`BreakdownAgent`] trait (production [`ClaudeBreakdownAgent`]; tests inject a
//! scripted fake), the blocking `RunEvent` receiver is drained on
//! `spawn_blocking` and every event is relayed live to `epic:<id>` (reusing
//! [`crate::planning::ws_type`]) — but the run is **one-shot**: no `resume`, no
//! multi-turn, and it does **not** write to `transcript_message`. Its durable
//! output is the task rows + edges the MCP tools persist, plus the `agent_run`
//! evidence row and the `epic.status='Ready'` transition.
//!
//! ## Determinism boundary
//!
//! The agent only ever creates tasks and links dependencies (its allow-list is
//! [`crate::mcp::BREAKDOWN_ALLOWED_TOOLS`], scoped to this one epic by the
//! capability token). Deerborn — not the agent — owns the `Planning → Ready`
//! lane transition, exactly as ARCHITECTURE §11 requires.

use std::path::PathBuf;
use std::sync::mpsc::Receiver;

use axum::{
    extract::{Path, State},
    http::StatusCode,
    Json,
};
use harness::{Claude, Harness, RunEvent, RunMode, RunRequest, RunTuning};
use libsql::params;
use serde_json::{json, Value};

use crate::epics::{fetch_epic, get_epic_clone_path, get_epic_project_id};
use crate::{AppError, AppResult, AppState, InflightGuard};

/// The system prompt that encodes the `to-tasks` vertical-slice breakdown logic.
/// The epic's product + technical context is appended separately (as the "PRD").
const BREAKDOWN_PROMPT: &str = "\
You are Deerborn's breakdown agent. You run ONCE (non-interactively) to convert an \
approved epic into an executable task DAG. The epic's product and technical context \
are provided to you as the plan.

Break the plan into TRACER-BULLET tasks: each task is a thin vertical slice that cuts \
through ALL integration layers end-to-end (schema, API, UI, tests), NOT a horizontal \
slice of a single layer. A completed slice must be demoable or verifiable on its own. \
Prefer many thin slices over few thick ones.

Create the tasks using your tools:
- `create_task`: create ONE task with a `title`, a `description` of the end-to-end \
behavior (not layer-by-layer), and `acceptance` criteria. Create blockers BEFORE the \
tasks that depend on them; when creating a task, you may pass `blocks` (ids of already- \
created tasks this new task blocks) or wire edges afterward with `link_dependency`.
- `link_dependency`: add a `blocker_id → blocked_id` edge (the blocker must finish first). \
Both tasks must belong to this epic; cycles are rejected.

Work in dependency order. Do not modify the codebase, run commands, or change the epic's \
status — creating tasks and linking dependencies is your entire surface. When the DAG is \
complete, stop.";

// ---- the agent seam ------------------------------------------------------

/// A one-shot breakdown run, decoupled from the harness so tests inject a
/// scripted agent. Built by [`spawn_breakdown`] from the epic's context.
pub struct BreakdownRunRequest {
    /// Unique id for this run (a ULID); echoed on every `RunEvent`.
    pub run_id: String,
    /// The breakdown instruction (the user-visible "go" prompt).
    pub prompt: String,
    /// The epic's product + technical context, appended as a system prompt (PRD).
    pub plan: String,
    /// Working directory: the project's read-only clone (code grounding). `None`
    /// when the clone isn't ready — the run proceeds without code context.
    pub cwd: Option<PathBuf>,
    /// MCP wiring (the breakdown tool surface). `None` disables tools (used only
    /// when the clone/base URL is unavailable — the run then no-ops usefully).
    pub mcp: Option<BreakdownMcp>,
}

/// The MCP knobs [`spawn_breakdown`] hands the agent for the run.
pub struct BreakdownMcp {
    /// Path to the temp `--mcp-config` JSON naming Deerborn's http server.
    pub config_path: PathBuf,
    /// Value for `--allowedTools` — [`crate::mcp::BREAKDOWN_ALLOWED_TOOLS`].
    pub allowed_tools: String,
}

/// The seam that makes T-301 hermetically testable (mirrors
/// [`crate::planning::PlanningAgent`]).
pub trait BreakdownAgent: Send + Sync {
    /// Start a one-shot run and hand back its blocking `RunEvent` receiver.
    fn run(&self, req: BreakdownRunRequest) -> Receiver<RunEvent>;
}

/// Production [`BreakdownAgent`]: drives Claude Code through the harness, one shot.
#[derive(Default)]
pub struct ClaudeBreakdownAgent;

impl ClaudeBreakdownAgent {
    /// Construct the production agent.
    pub fn new() -> ClaudeBreakdownAgent {
        ClaudeBreakdownAgent
    }
}

impl BreakdownAgent for ClaudeBreakdownAgent {
    fn run(&self, req: BreakdownRunRequest) -> Receiver<RunEvent> {
        let run_id = req.run_id.clone();

        let mut extra_args = vec![
            "--append-system-prompt".to_string(),
            BREAKDOWN_PROMPT.to_string(),
            // The epic's product + technical context, as the plan to break down.
            "--append-system-prompt".to_string(),
            req.plan.clone(),
        ];
        if let Some(mcp) = &req.mcp {
            extra_args.push("--mcp-config".to_string());
            extra_args.push(mcp.config_path.to_string_lossy().into_owned());
            extra_args.push("--allowedTools".to_string());
            extra_args.push(mcp.allowed_tools.clone());
            extra_args.push("--permission-mode".to_string());
            extra_args.push("bypassPermissions".to_string());
        }

        let request = RunRequest {
            run_id: req.run_id,
            prompt: req.prompt,
            cwd: req.cwd,
            // Read-only w.r.t. the codebase: enforced by the tool allow-list +
            // the read-only clone, not by the mode (per the T-200 spike).
            mode: RunMode::Ask,
            tuning: RunTuning {
                extra_args,
                ..RunTuning::default()
            },
            // One-shot: never resume a prior session.
            resume: None,
        };

        match Claude::new().run_channel(request) {
            Ok((_handle, rx)) => rx,
            Err(err) => {
                let (tx, rx) = std::sync::mpsc::channel();
                let _ = tx.send(RunEvent::Error {
                    run_id: run_id.clone(),
                    message: format!("failed to start breakdown run: {err}"),
                });
                let _ = tx.send(RunEvent::Exited {
                    run_id,
                    exit_code: None,
                    cancelled: false,
                });
                rx
            }
        }
    }
}

// ---- route: trigger breakdown --------------------------------------------

/// `POST /epics/{id}/breakdown` — run the one-shot breakdown agent on an approved
/// epic, then move it Planning → Ready.
///
/// * `404` if the epic does not exist.
/// * `409` if the epic is not in `Planning`, if it has not advanced to technical
///   planning (no `technical` session), or if a run is already in flight for it.
/// * `202 Accepted` once the background run is spawned (its events stream over
///   WS on `epic:<id>`; the DAG + lane change land when the run completes).
pub async fn trigger_breakdown(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> AppResult<(StatusCode, Json<Value>)> {
    let conn = state.db.conn();

    let epic = fetch_epic(conn, &id)
        .await?
        .ok_or_else(|| AppError::NotFound(format!("epic {id} not found")))?;

    if epic.status != "Planning" {
        return Err(AppError::Conflict(format!(
            "epic {id} is `{}`, not `Planning`; only a planning epic can be broken down",
            epic.status
        )));
    }

    // Breakdown runs on the *approved* epic — after technical planning has begun.
    if !technical_session_exists(&state, &id).await? {
        return Err(AppError::Conflict(format!(
            "epic {id} has not advanced to technical planning; complete planning before breakdown"
        )));
    }

    // One run at a time per epic (shares the planning in-flight slot so a
    // planning run and a breakdown run never overlap on the same epic).
    let Some(guard) = state.try_acquire_run(&id) else {
        return Err(AppError::Conflict(format!(
            "a run is already in flight for epic {id}"
        )));
    };

    spawn_breakdown(state.clone(), id, guard);

    Ok((
        StatusCode::ACCEPTED,
        Json(json!({ "status": "breakdown_started" })),
    ))
}

/// Whether the epic has a `technical` planning session (i.e. it advanced past
/// product planning) — the marker that planning is far enough along to break down.
async fn technical_session_exists(state: &AppState, epic_id: &str) -> AppResult<bool> {
    let mut rows = state
        .db
        .conn()
        .query(
            "SELECT 1 FROM planning_session WHERE epic_id = ?1 AND phase = 'technical'",
            params![epic_id],
        )
        .await?;
    Ok(rows.next().await?.is_some())
}

// ---- run orchestration ---------------------------------------------------

/// What a drained breakdown run leaves behind, persisted after the stream ends.
#[derive(Default)]
struct BreakdownOutcome {
    /// Assembled assistant text (all `Text` deltas), stored as `agent_run.log`.
    log: String,
    /// The harness session id captured from `RunEvent::Session` (evidence).
    session_id: Option<String>,
}

impl BreakdownOutcome {
    fn absorb(&mut self, event: &RunEvent) {
        match event {
            RunEvent::Text { delta, .. } => self.log.push_str(delta),
            RunEvent::Session {
                session_id: Some(id),
                ..
            } => self.session_id = Some(id.clone()),
            _ => {}
        }
    }
}

/// Kick off the one-shot breakdown run in the background and return immediately.
///
/// Holds `guard` (releasing the epic's in-flight slot when the run finishes),
/// mints a breakdown capability scoped to the epic, drains the blocking
/// `RunEvent` receiver on `spawn_blocking` while relaying every event to
/// `epic:<id>`, then moves the epic to `Ready`, records an `agent_run` row, and
/// publishes the final DAG + updated epic.
pub fn spawn_breakdown(state: AppState, epic_id: String, guard: InflightGuard) {
    tokio::spawn(async move {
        let _guard = guard;
        let conn = state.db.conn();

        // Build the plan (PRD) from the epic's product + technical context.
        let plan = match build_plan(&state, &epic_id).await {
            Some(plan) => plan,
            None => {
                tracing::warn!(epic = %epic_id, "breakdown: epic vanished before run");
                return;
            }
        };

        // Mint an MCP capability for the breakdown tool surface, scoped to this
        // (epic, project, clone). Held for the whole run; the temp config file is
        // removed on completion. Falls back to a tool-less run if the clone/base
        // URL is unavailable.
        let mut cwd: Option<PathBuf> = None;
        let mut mcp: Option<BreakdownMcp> = None;
        let mut _cap_guard: Option<crate::mcp::CapabilityGuard> = None;
        let mut mcp_config_path: Option<PathBuf> = None;

        let clone_path = get_epic_clone_path(conn, &epic_id).await.ok().flatten();
        let project_id = get_epic_project_id(conn, &epic_id)
            .await
            .ok()
            .flatten()
            .unwrap_or_default();
        match (clone_path, state.advertised_base()) {
            (Some(clone_path), Some(base)) => {
                let clone_pb = PathBuf::from(&clone_path);
                let cap = state.caps.mint(
                    epic_id.clone(),
                    project_id.clone(),
                    "breakdown".to_string(),
                    clone_pb.clone(),
                );
                match crate::mcp::write_mcp_config(&base, cap.token()) {
                    Ok(path) => {
                        cwd = Some(clone_pb);
                        mcp = Some(BreakdownMcp {
                            config_path: path.clone(),
                            allowed_tools: crate::mcp::BREAKDOWN_ALLOWED_TOOLS.to_string(),
                        });
                        mcp_config_path = Some(path);
                        _cap_guard = Some(cap);
                    }
                    Err(err) => {
                        tracing::warn!(epic = %epic_id, error = %err, "breakdown: MCP config write failed; running without tools");
                    }
                }
            }
            _ => {
                tracing::debug!(epic = %epic_id, "breakdown: no ready clone or base URL; running without MCP");
            }
        }

        let req = BreakdownRunRequest {
            run_id: ulid::Ulid::new().to_string(),
            prompt: "Break this epic down into a task DAG using your tools.".to_string(),
            plan,
            cwd,
            mcp,
        };

        let rx = state.breakdown.run(req);
        let hub = state.hub.clone();
        let topic = format!("epic:{epic_id}");

        // Drain the BLOCKING receiver off the async runtime, relaying live.
        let drained = tokio::task::spawn_blocking(move || {
            let mut outcome = BreakdownOutcome::default();
            for event in rx {
                let payload = serde_json::to_value(&event).unwrap_or(Value::Null);
                hub.publish(&topic, crate::planning::ws_type(&event), payload);
                outcome.absorb(&event);
            }
            outcome
        })
        .await;

        // Agent has exited; the temp MCP config file is no longer needed. The
        // capability token is revoked when `_cap_guard` drops at task end.
        if let Some(path) = &mcp_config_path {
            let _ = tokio::fs::remove_file(path).await;
        }

        let outcome = match drained {
            Ok(outcome) => outcome,
            Err(_) => return, // blocking task panicked; nothing reliable to persist
        };

        // Record per-run evidence (the tasks/edges were persisted live by the
        // MCP tools during the run).
        let run_id = ulid::Ulid::new().to_string();
        let _ = conn
            .execute(
                "INSERT INTO agent_run (id, task_id, epic_id, stage, session_id, log, created_at) \
                 VALUES (?1, NULL, ?2, 'breakdown', ?3, ?4, ?5)",
                params![
                    run_id,
                    epic_id.clone(),
                    outcome.session_id,
                    outcome.log,
                    now_ms()
                ],
            )
            .await;

        // Deerborn owns the lane transition: Planning → Ready.
        let _ = conn
            .execute(
                "UPDATE epic SET status = 'Ready', updated_at = ?1 WHERE id = ?2 AND status = 'Planning'",
                params![now_ms(), epic_id.clone()],
            )
            .await;

        // Publish the final DAG and the updated epic so the client re-renders.
        crate::mcp::publish_dag(&state, &epic_id).await;
        if let Ok(Some(epic)) = fetch_epic(conn, &epic_id).await {
            let payload = serde_json::to_value(&epic).unwrap_or(Value::Null);
            state
                .hub
                .publish(&format!("epic:{epic_id}"), "epic_updated", payload);

            // Also publish the project board so a subscribed kanban re-renders
            // when breakdown lands an epic in Ready (T-401).
            crate::board::publish_board(&state, &epic.project_id).await;
        }
    });
}

/// Assemble the plan (PRD) an epic hands the breakdown agent from its title +
/// product/technical context. `None` if the epic no longer exists.
async fn build_plan(state: &AppState, epic_id: &str) -> Option<String> {
    let epic = fetch_epic(state.db.conn(), epic_id).await.ok().flatten()?;
    let product = epic
        .product_context
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .unwrap_or("(no product context recorded)");
    let technical = epic
        .technical_context
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .unwrap_or("(no technical context recorded)");
    Some(format!(
        "The epic to break down is titled \"{title}\".\n\n\
         --- PRODUCT CONTEXT ---\n{product}\n--- END PRODUCT CONTEXT ---\n\n\
         --- TECHNICAL CONTEXT ---\n{technical}\n--- END TECHNICAL CONTEXT ---",
        title = epic.title,
    ))
}

/// Current unix time in milliseconds (matches the `*_at` columns).
fn now_ms() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

// ---- test doubles --------------------------------------------------------

#[cfg(test)]
pub(crate) mod testing {
    use super::*;
    use std::sync::{Arc, Mutex};

    /// A recorded breakdown run (what the engine passed the fake).
    #[derive(Clone, Debug)]
    #[allow(dead_code)]
    pub struct RecordedBreakdown {
        pub run_id: String,
        pub prompt: String,
        pub plan: String,
        pub had_mcp: bool,
    }

    /// A scripted [`BreakdownAgent`] that, per run, invokes a caller-supplied
    /// closure (to drive the MCP tools like a real agent would) and then emits
    /// Started → Session → Text* → Exited. Records each request.
    pub struct ScriptedBreakdownAgent {
        session_id: String,
        chunks: Vec<String>,
        recorded: Arc<Mutex<Vec<RecordedBreakdown>>>,
    }

    impl ScriptedBreakdownAgent {
        pub fn new(session_id: &str, chunks: &[&str]) -> ScriptedBreakdownAgent {
            ScriptedBreakdownAgent {
                session_id: session_id.to_string(),
                chunks: chunks.iter().map(|s| s.to_string()).collect(),
                recorded: Arc::new(Mutex::new(Vec::new())),
            }
        }

        pub fn recorded(&self) -> Arc<Mutex<Vec<RecordedBreakdown>>> {
            self.recorded.clone()
        }
    }

    impl BreakdownAgent for ScriptedBreakdownAgent {
        fn run(&self, req: BreakdownRunRequest) -> Receiver<RunEvent> {
            self.recorded.lock().unwrap().push(RecordedBreakdown {
                run_id: req.run_id.clone(),
                prompt: req.prompt.clone(),
                plan: req.plan.clone(),
                had_mcp: req.mcp.is_some(),
            });

            let (tx, rx) = std::sync::mpsc::channel();
            let run_id = req.run_id;
            let session_id = self.session_id.clone();
            let chunks = self.chunks.clone();

            std::thread::spawn(move || {
                let _ = tx.send(RunEvent::Started {
                    run_id: run_id.clone(),
                });
                let _ = tx.send(RunEvent::Session {
                    run_id: run_id.clone(),
                    session_id: Some(session_id),
                    model: Some("fake-model".to_string()),
                });
                for chunk in chunks {
                    let _ = tx.send(RunEvent::Text {
                        run_id: run_id.clone(),
                        delta: chunk,
                    });
                }
                let _ = tx.send(RunEvent::Exited {
                    run_id,
                    exit_code: Some(0),
                    cancelled: false,
                });
            });
            rx
        }
    }

    /// A [`BreakdownAgent`] that creates a small fixed DAG by calling Deerborn's
    /// own MCP endpoint (proving the end-to-end seam), driven by a callback the
    /// test supplies. Kept minimal: it just emits a terminal stream; DAG creation
    /// is done by the test directly against the store/endpoint so the engine's
    /// completion path (Ready transition, agent_run, publishes) is exercised.
    pub struct SilentBreakdownAgent;

    impl BreakdownAgent for SilentBreakdownAgent {
        fn run(&self, req: BreakdownRunRequest) -> Receiver<RunEvent> {
            let (tx, rx) = std::sync::mpsc::channel();
            let run_id = req.run_id;
            std::thread::spawn(move || {
                let _ = tx.send(RunEvent::Started {
                    run_id: run_id.clone(),
                });
                let _ = tx.send(RunEvent::Exited {
                    run_id,
                    exit_code: Some(0),
                    cancelled: false,
                });
            });
            rx
        }
    }
}

#[cfg(test)]
mod tests {
    use super::testing::*;
    use super::*;
    use crate::planning::testing::SilentPlanningAgent;
    use crate::{app, AppState, Config, Db};
    use axum::body::Body;
    use axum::http::{
        header::{AUTHORIZATION, CONTENT_TYPE},
        Request, StatusCode,
    };
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::sync::broadcast;
    use tower::ServiceExt;

    const TOKEN: &str = "s3cret-token";

    fn req(method: &str, uri: &str, body: Option<Value>) -> Request<Body> {
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

    async fn body_json(response: axum::response::Response) -> Value {
        let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        if bytes.is_empty() {
            return Value::Null;
        }
        serde_json::from_slice(&bytes).unwrap()
    }

    /// Boot state with an injected breakdown agent (silent planner).
    async fn app_with_breakdown(
        breakdown: Arc<dyn BreakdownAgent>,
    ) -> (AppState, axum::Router) {
        let db = Db::connect(":memory:").await.unwrap();
        db.run_migrations().await.unwrap();
        let state = AppState::with_agents(
            Config::for_test(TOKEN),
            db,
            Arc::new(SilentPlanningAgent),
            breakdown,
        );
        let app = app(state.clone());
        (state, app)
    }

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

    async fn create_epic(app: &axum::Router, project_id: &str) -> String {
        let created = app
            .clone()
            .oneshot(req(
                "POST",
                &format!("/projects/{project_id}/epics"),
                Some(json!({ "title": "E" })),
            ))
            .await
            .unwrap();
        assert_eq!(created.status(), StatusCode::CREATED);
        body_json(created).await["id"].as_str().unwrap().to_string()
    }

    async fn advance(app: &axum::Router, epic_id: &str) {
        let r = app
            .clone()
            .oneshot(req("POST", &format!("/epics/{epic_id}/advance-phase"), None))
            .await
            .unwrap();
        assert_eq!(r.status(), StatusCode::CREATED);
    }

    async fn trigger(app: &axum::Router, epic_id: &str) -> axum::response::Response {
        app.clone()
            .oneshot(req("POST", &format!("/epics/{epic_id}/breakdown"), None))
            .await
            .unwrap()
    }

    /// Poll until the epic reaches `status` (or timeout); returns the status seen.
    async fn wait_for_status(state: &AppState, epic_id: &str, status: &str) -> String {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        loop {
            let epic = fetch_epic(state.db.conn(), epic_id).await.unwrap().unwrap();
            if epic.status == status || tokio::time::Instant::now() >= deadline {
                return epic.status;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    #[tokio::test]
    async fn breakdown_streams_moves_epic_to_ready_and_records_a_run() {
        let agent = Arc::new(ScriptedBreakdownAgent::new("bd-sess", &["Breaking ", "down…"]));
        let recorded = agent.recorded();
        let (state, app) = app_with_breakdown(agent).await;
        let project_id = seed_project(&app).await;
        let epic_id = create_epic(&app, &project_id).await;
        advance(&app, &epic_id).await;

        // Seed some context so the plan is non-trivial.
        state
            .db
            .conn()
            .execute(
                "UPDATE epic SET product_context = 'export feature', technical_context = 'axum route' WHERE id = ?1",
                params![epic_id.clone()],
            )
            .await
            .unwrap();

        let sub = state.hub.subscribe(&format!("epic:{epic_id}"));

        let response = trigger(&app, &epic_id).await;
        assert_eq!(response.status(), StatusCode::ACCEPTED);
        assert_eq!(body_json(response).await["status"], "breakdown_started");

        // Events relay over WS in order (ending in exited).
        let frames = collect_until_exited(sub).await;
        let types: Vec<&str> = frames.iter().map(|f| f["type"].as_str().unwrap()).collect();
        assert!(types.contains(&"started"));
        assert!(types.contains(&"text"));
        assert!(types.last() == Some(&"exited"));

        // The epic moved Planning → Ready.
        assert_eq!(wait_for_status(&state, &epic_id, "Ready").await, "Ready");

        // An agent_run row records the breakdown stage + session id.
        let mut rows = state
            .db
            .conn()
            .query(
                "SELECT stage, session_id FROM agent_run WHERE epic_id = ?1",
                params![epic_id.clone()],
            )
            .await
            .unwrap();
        let row = rows.next().await.unwrap().expect("agent_run row");
        assert_eq!(row.get::<String>(0).unwrap(), "breakdown");
        assert_eq!(row.get::<Option<String>>(1).unwrap().as_deref(), Some("bd-sess"));

        // The engine received the plan built from the epic's context.
        let runs = recorded.lock().unwrap();
        assert_eq!(runs.len(), 1);
        assert!(runs[0].plan.contains("export feature"));
        assert!(runs[0].plan.contains("axum route"));
    }

    #[tokio::test]
    async fn breakdown_rejected_when_not_planning() {
        let (state, app) = app_with_breakdown(Arc::new(SilentBreakdownAgent)).await;
        let project_id = seed_project(&app).await;
        let epic_id = create_epic(&app, &project_id).await;
        advance(&app, &epic_id).await;
        // Force the epic out of Planning.
        state
            .db
            .conn()
            .execute(
                "UPDATE epic SET status = 'Ready' WHERE id = ?1",
                params![epic_id.clone()],
            )
            .await
            .unwrap();

        let response = trigger(&app, &epic_id).await;
        assert_eq!(response.status(), StatusCode::CONFLICT);
        assert_eq!(body_json(response).await["error"]["code"], "conflict");
    }

    #[tokio::test]
    async fn breakdown_rejected_before_technical_planning() {
        let (_state, app) = app_with_breakdown(Arc::new(SilentBreakdownAgent)).await;
        let project_id = seed_project(&app).await;
        let epic_id = create_epic(&app, &project_id).await;
        // No advance → no technical session.
        let response = trigger(&app, &epic_id).await;
        assert_eq!(response.status(), StatusCode::CONFLICT);
    }

    #[tokio::test]
    async fn breakdown_on_unknown_epic_is_404() {
        let (_state, app) = app_with_breakdown(Arc::new(SilentBreakdownAgent)).await;
        let response = trigger(&app, "nope").await;
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    async fn collect_until_exited(mut rx: broadcast::Receiver<Arc<str>>) -> Vec<Value> {
        let mut frames = Vec::new();
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        loop {
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            match tokio::time::timeout(remaining, rx.recv()).await {
                Ok(Ok(env)) => {
                    let value: Value = serde_json::from_str(&env).unwrap();
                    let is_exit = value["type"] == "exited";
                    frames.push(value);
                    if is_exit {
                        return frames;
                    }
                }
                Ok(Err(broadcast::error::RecvError::Lagged(_))) => continue,
                _ => return frames,
            }
        }
    }

    /// Ignore-marked live smoke test: drives the REAL `claude` CLI to break an
    /// epic into tasks. Excluded from the hermetic gate (needs auth + network).
    #[tokio::test]
    #[ignore]
    async fn live_claude_breakdown_run() {
        let (state, app) = {
            let db = Db::connect(":memory:").await.unwrap();
            db.run_migrations().await.unwrap();
            let state = AppState::new(Config::for_test(TOKEN), db);
            let app = app(state.clone());
            (state, app)
        };
        let project_id = seed_project(&app).await;
        let epic_id = create_epic(&app, &project_id).await;
        advance(&app, &epic_id).await;
        state
            .db
            .conn()
            .execute(
                "UPDATE epic SET product_context = 'A todo app', technical_context = 'Rust + libSQL' WHERE id = ?1",
                params![epic_id.clone()],
            )
            .await
            .unwrap();
        let response = trigger(&app, &epic_id).await;
        assert_eq!(response.status(), StatusCode::ACCEPTED);
        assert_eq!(wait_for_status(&state, &epic_id, "Ready").await, "Ready");
    }
}
