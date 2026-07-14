//! Planning agent runs and live `RunEvent` → WebSocket streaming (T-202).
//!
//! A user message on an epic (`POST /epics/:id/messages`, in [`crate::epics`])
//! triggers a **planning agent run**. The agent is driven through the
//! [`agent-harness`](https://github.com/getlatentic/agent-harness) crate, which
//! shells out to Claude Code and emits a normalized stream of [`RunEvent`]s. This
//! module owns the run lifecycle:
//!
//! 1. build a run request (native `resume` from the stored harness session id,
//!    plus the phase's product-planning system prompt);
//! 2. drain the harness's **blocking** `std::sync::mpsc::Receiver<RunEvent>` on a
//!    dedicated `spawn_blocking` thread (never on the async runtime);
//! 3. **relay every event live** to the epic's WS topic (`epic:<id>`) so the
//!    browser streams the reply token-by-token;
//! 4. on completion, **persist** the assembled agent reply (and any tool events)
//!    to the durable transcript and stash the harness session id for resume.
//!
//! ## The [`PlanningAgent`] seam
//!
//! The real `claude` CLI cannot run deterministically under `cargo test` (it
//! needs auth + network), so the harness sits behind the [`PlanningAgent`] trait.
//! [`ClaudePlanningAgent`] is the production implementation (used by
//! [`AppState::new`](crate::AppState::new)); tests inject a scripted fake that
//! emits a deterministic `RunEvent` sequence. The trait is intentionally tiny:
//! given a [`PlanningRunRequest`], hand back the same blocking receiver the
//! harness would, so the orchestration in [`spawn_run`] is identical for both.
//!
//! ## Phase configs
//!
//! [`PlanningConfig`] describes a planning *role* (its phase + system prompt).
//! T-202 ships only [`PRODUCT_PLANNING`]; the [`config_for_phase`] switch and the
//! `&'static PlanningConfig` shape are structured so T-205 can add a `technical`
//! config against this same engine without touching the run machinery.
//!
//! ## Concurrency
//!
//! At most one run may be in flight per epic (so `seq`/resume never interleave).
//! The in-flight set lives on [`AppState`](crate::AppState); a second trigger that
//! arrives while a run is active is **ignored** — the user message is still
//! persisted, but no overlapping run is started (the live run continues intact).

use std::path::PathBuf;
use std::sync::mpsc::Receiver;

use harness::{Claude, Harness, RunEvent, RunMode, RunRequest, RunTuning};
use serde_json::Value;

use libsql::Connection;

use crate::epics::{
    append_message, fetch_epic, get_epic_clone_path, get_epic_project_id, get_harness_session_id,
    set_harness_session_id,
};
use crate::{AppState, InflightGuard};

// ---- phase configuration -------------------------------------------------

/// A planning *role*: the phase it records under and the system prompt that
/// steers the agent. One engine, swappable config — T-205 adds a `technical`
/// config alongside [`PRODUCT_PLANNING`] without changing the run machinery.
pub struct PlanningConfig {
    /// Transcript phase these runs record under (`product` | `technical`).
    pub phase: &'static str,
    /// Appended to the agent via `--append-system-prompt`; stable across turns.
    pub system_prompt: &'static str,
    /// Whether this phase exposes Deerborn's planning MCP tools (`update_epic`,
    /// `read_codebase_context`) to the agent. When `true`, [`spawn_run`] mints a
    /// capability token, wires `--mcp-config`/`--allowedTools`, and points `cwd`
    /// at the project's read-only clone (T-203).
    pub tools_enabled: bool,
}

/// Product-planning role (T-202/T-203). A conversational planner that also has
/// the phase-scoped MCP tools: it maintains the epic's `product_context` via
/// `update_epic` and may inspect the project's canonical clone (read-only) via
/// `read_codebase_context`.
pub const PRODUCT_PLANNING: PlanningConfig = PlanningConfig {
    phase: "product",
    system_prompt: PRODUCT_PLANNING_PROMPT,
    tools_enabled: true,
};

const PRODUCT_PLANNING_PROMPT: &str = "\
You are Deerborn's product-planning partner. You help a single engineer turn a \
rough idea for an epic into a crisp product definition through conversation.

Focus on the PRODUCT: the user problem, who it is for, the desired outcome, \
scope boundaries, and concrete acceptance criteria. Draw out ambiguity by asking \
one or two sharp questions at a time rather than interrogating. Do not design the \
technical implementation — a separate technical-planning phase handles that later.

You have two tools:
- `update_epic`: keep the epic's product context current. Whenever the shared \
understanding advances, call it with the FULL up-to-date product context (markdown); \
the value you pass REPLACES the stored context. Do this proactively as the plan \
firms up, not only when asked.
- `read_codebase_context`: read-only access to the project's code (list \
directories, read files) to ground the plan in what already exists.

You must NOT modify the codebase, run commands, or change the epic's status, lane, \
or anything beyond its product context — those tools are the entire surface you have. \
Be concise and conversational.";

/// Technical-planning role (T-205). The second half of planning: given a
/// product definition already agreed in the product phase, this planner works
/// out *how* to build it. It has the same tool surface as product planning
/// (`update_epic` + `read_codebase_context`) but maintains the epic's
/// `technical_context` and is steered to ground every decision in the real code.
///
/// The product outcome does not carry over automatically — each phase is its own
/// harness session — so [`spawn_run`] seeds the prior product context into this
/// run's continuity preamble (see [`PlanningRunRequest::continuity`]).
pub const TECHNICAL_PLANNING: PlanningConfig = PlanningConfig {
    phase: "technical",
    system_prompt: TECHNICAL_PLANNING_PROMPT,
    tools_enabled: true,
};

const TECHNICAL_PLANNING_PROMPT: &str = "\
You are Deerborn's technical-planning partner. The product-planning phase has \
already agreed WHAT to build (its outcome is provided to you as the product \
context). Your job is to work out HOW: the technical approach, architecture, the \
concrete files and modules to touch, data model / API changes, sequencing, and \
technical risks.

Ground every decision in the ACTUAL code, not assumptions:
- `read_codebase_context`: read-only access to the project's canonical clone \
(list directories, read files). Inspect the real structure, existing patterns, \
and the specific files your plan would change BEFORE proposing an approach. Quote \
what you find.
- `update_epic`: keep the epic's technical context current. Whenever the approach \
firms up, call it with the FULL up-to-date technical plan (markdown); the value you \
pass REPLACES the stored technical context. Do this proactively, not only when asked.

Build on the product context — do not re-open settled product decisions. You must \
NOT modify the codebase, run commands, or change the epic's status, lane, or \
anything beyond its technical context — those two tools are your entire surface. \
Be concise and conversational, asking one or two sharp questions at a time.";

/// Resolve the [`PlanningConfig`] for a transcript phase, or `None` if the phase
/// has no planning role.
pub fn config_for_phase(phase: &str) -> Option<&'static PlanningConfig> {
    match phase {
        "product" => Some(&PRODUCT_PLANNING),
        "technical" => Some(&TECHNICAL_PLANNING),
        _ => None,
    }
}

/// Build the technical phase's continuity preamble from the epic's title and the
/// `product_context` the product phase produced. Returned as a system-prompt
/// block so the technical planner builds on the settled product decisions rather
/// than re-deriving them. Best-effort: on a DB error / missing epic it returns
/// `None` and the run proceeds without the seed (the tools still work).
async fn technical_continuity(conn: &Connection, epic_id: &str) -> Option<String> {
    let epic = fetch_epic(conn, epic_id).await.ok().flatten()?;
    let product = epic
        .product_context
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .unwrap_or("(the product phase recorded no product context)");
    Some(format!(
        "You are continuing planning for the epic titled \"{title}\".\n\n\
         The product-planning phase already produced the product context below. \
         Treat it as settled input and build your technical plan on top of it:\n\n\
         --- PRODUCT CONTEXT ---\n{product}\n--- END PRODUCT CONTEXT ---",
        title = epic.title,
    ))
}

// ---- the agent seam ------------------------------------------------------

/// A single planning turn to run, decoupled from the harness so tests can inject
/// a scripted agent. Built by [`spawn_run`] from the epic's session + phase.
pub struct PlanningRunRequest {
    /// Unique id for this run (a ULID); echoed back on every `RunEvent`.
    pub run_id: String,
    /// The user's message for this turn.
    pub prompt: String,
    /// Working directory for the run (the project's read-only clone). `None`
    /// today; T-203 points this at the canonical checkout for code context.
    pub cwd: Option<PathBuf>,
    /// Native harness resume id, if this epic/phase already has a session.
    pub resume: Option<String>,
    /// System prompt for the phase (stable across turns).
    pub system_prompt: &'static str,
    /// Cross-phase continuity, appended as a *second* system prompt (T-205).
    /// Because each phase is its own harness session, a later phase cannot see an
    /// earlier phase's conversation; [`spawn_run`] seeds the prior phase's outcome
    /// here (e.g. the technical run receives the epic's `product_context` + title)
    /// so the run builds on it. `None` for the first phase, which has no prior.
    pub continuity: Option<String>,
    /// MCP wiring for a tools-enabled phase (T-203). `None` for a plain
    /// conversational run; `Some` adds `--mcp-config`/`--allowedTools`/
    /// `--permission-mode bypassPermissions` so the agent can reach Deerborn's
    /// local MCP server.
    pub mcp: Option<McpRun>,
}

/// The MCP knobs [`spawn_run`] hands the agent for a tools-enabled planning turn.
pub struct McpRun {
    /// Path to the temp `--mcp-config` JSON file naming Deerborn's http server.
    pub config_path: PathBuf,
    /// Value for `--allowedTools` — the phase-scoped allow-list.
    pub allowed_tools: String,
}

/// The seam that makes T-202 hermetically testable.
///
/// Production wraps [`harness::Claude`] ([`ClaudePlanningAgent`]); tests inject a
/// fake. Implementations return the harness's own **blocking**
/// `std::sync::mpsc::Receiver<RunEvent>`, which [`spawn_run`] drains off-runtime.
pub trait PlanningAgent: Send + Sync {
    /// Start a run and hand back its `RunEvent` receiver. Must not block: the
    /// events are produced on the harness's / fake's own thread and the receiver
    /// hangs up on its own when the run ends.
    fn run(&self, req: PlanningRunRequest) -> Receiver<RunEvent>;
}

/// Production [`PlanningAgent`]: drives Claude Code through the harness exactly as
/// the T-200 spike proved — `RunMode::Ask`, the phase system prompt via
/// `--append-system-prompt`, native `resume`.
#[derive(Default)]
pub struct ClaudePlanningAgent;

impl ClaudePlanningAgent {
    /// Construct the production agent.
    pub fn new() -> ClaudePlanningAgent {
        ClaudePlanningAgent
    }
}

impl PlanningAgent for ClaudePlanningAgent {
    fn run(&self, req: PlanningRunRequest) -> Receiver<RunEvent> {
        let run_id = req.run_id.clone();

        // Base args: the phase system prompt, appended verbatim.
        let mut extra_args = vec![
            "--append-system-prompt".to_string(),
            req.system_prompt.to_string(),
        ];
        // Cross-phase continuity (T-205): the prior phase's outcome, appended as a
        // second system prompt so a later phase (e.g. technical) builds on it.
        if let Some(continuity) = &req.continuity {
            extra_args.push("--append-system-prompt".to_string());
            extra_args.push(continuity.clone());
        }
        // Tools-enabled phase (T-203): wire Deerborn's local MCP server exactly as
        // the T-200 spike proved. Read-only is enforced by the allow-list (only the
        // two planning tools) + the read-only clone as `cwd`, NOT by `RunMode`.
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
            // Planning is a read-only discussion. NB: per the spike, `Ask` is not a
            // read-only *guarantee* — that comes from tool-scoping + the read-only
            // clone; the mode itself does not block edit tools.
            mode: RunMode::Ask,
            tuning: RunTuning {
                extra_args,
                ..RunTuning::default()
            },
            resume: req.resume,
        };

        match Claude::new().run_channel(request) {
            // Drop the handle: per the spike, dropping does NOT cancel; the run
            // proceeds to completion and the receiver hangs up on its own.
            Ok((_handle, rx)) => rx,
            // Surface a spawn failure as a terminal Error+Exited stream so the
            // orchestration drains uniformly instead of branching on Result.
            Err(err) => {
                let (tx, rx) = std::sync::mpsc::channel();
                let _ = tx.send(RunEvent::Error {
                    run_id: run_id.clone(),
                    message: format!("failed to start planning run: {err}"),
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

// ---- RunEvent → WS type mapping ------------------------------------------

/// Map a [`RunEvent`] to the WS `type` published on `epic:<id>`. The serialized
/// event (camelCase, `kind`-tagged) is relayed verbatim as the frame `payload`.
/// Documented in `CONVENTIONS.md` §WebSocket.
pub fn ws_type(event: &RunEvent) -> &'static str {
    match event {
        RunEvent::Started { .. } => "started",
        RunEvent::Session { .. } => "session",
        RunEvent::Text { .. } => "text",
        RunEvent::Thinking { .. } => "thinking",
        RunEvent::ToolStart { .. } => "tool_start",
        RunEvent::ToolEnd { .. } => "tool_end",
        RunEvent::SuggestedEdits { .. } => "suggested_edits",
        RunEvent::Activity { .. } => "activity",
        RunEvent::Usage { .. } => "usage",
        RunEvent::AskQuestion { .. } => "ask_question",
        RunEvent::Error { .. } => "error",
        RunEvent::Exited { .. } => "exited",
        // `RunEvent` is `#[non_exhaustive]`: a future kind relays under a
        // generic type rather than being dropped.
        _ => "event",
    }
}

// ---- run orchestration ---------------------------------------------------

/// What a drained run leaves behind, to be persisted after the stream ends.
#[derive(Default)]
struct RunOutcome {
    /// The assembled assistant reply (all `Text` deltas concatenated).
    text: String,
    /// The harness session id captured from `RunEvent::Session`, for resume.
    session_id: Option<String>,
    /// Tool events, serialized, to store as `role='tool'` messages (none in
    /// T-202 — the planner has no tools — but handled for T-203 forward-compat).
    tool_events: Vec<String>,
}

impl RunOutcome {
    /// Fold one event into the accumulator (called for every relayed event).
    fn absorb(&mut self, event: &RunEvent) {
        match event {
            RunEvent::Text { delta, .. } => self.text.push_str(delta),
            RunEvent::Session {
                session_id: Some(id),
                ..
            } => self.session_id = Some(id.clone()),
            RunEvent::ToolStart { .. } | RunEvent::ToolEnd { .. } => {
                if let Ok(json) = serde_json::to_string(event) {
                    self.tool_events.push(json);
                }
            }
            _ => {}
        }
    }
}

/// Kick off a planning run in the background and return immediately.
///
/// Holds `guard` (releasing the epic's in-flight slot when the run finishes),
/// drains the blocking `RunEvent` receiver on a `spawn_blocking` thread while
/// relaying every event live to `epic:<id>`, then persists the assembled agent
/// reply, any tool events, and the harness session id.
pub fn spawn_run(
    state: AppState,
    epic_id: String,
    phase: String,
    guard: InflightGuard,
    user_content: String,
) {
    tokio::spawn(async move {
        // Held for the whole run; dropping it frees the epic's in-flight slot.
        let _guard = guard;

        let Some(config) = config_for_phase(&phase) else {
            // Phase was validated before the trigger; treat as a no-op if not.
            return;
        };

        let resume = get_harness_session_id(state.db.conn(), &epic_id, &phase)
            .await
            .ok()
            .flatten();

        // For a tools-enabled phase, mint a per-run capability scoped to this
        // (epic, phase, clone) and wire the MCP config. `_cap_guard` is held for
        // the whole run so the token is revoked when the run ends; `mcp_config`
        // is the temp file we remove on completion. Both stay `None` (plain
        // conversational run) if the clone isn't ready or the base URL is unset.
        let mut cwd: Option<PathBuf> = None;
        let mut mcp: Option<McpRun> = None;
        let mut _cap_guard: Option<crate::mcp::CapabilityGuard> = None;
        let mut mcp_config_path: Option<PathBuf> = None;

        if config.tools_enabled {
            let clone_path = get_epic_clone_path(state.db.conn(), &epic_id)
                .await
                .ok()
                .flatten();
            match (clone_path, state.advertised_base()) {
                (Some(clone_path), Some(base)) => {
                    let clone_pb = PathBuf::from(&clone_path);
                    // The scope needs the project (unused by planning's tools, but
                    // part of the shared scope shape); fall back to empty if the
                    // epic vanished, which only disables tools harmlessly.
                    let project_id = get_epic_project_id(state.db.conn(), &epic_id)
                        .await
                        .ok()
                        .flatten()
                        .unwrap_or_default();
                    let guard = state.caps.mint(
                        epic_id.clone(),
                        project_id,
                        phase.clone(),
                        clone_pb.clone(),
                    );
                    match crate::mcp::write_mcp_config(&base, guard.token()) {
                        Ok(path) => {
                            cwd = Some(clone_pb);
                            mcp = Some(McpRun {
                                config_path: path.clone(),
                                allowed_tools: crate::mcp::PLANNING_ALLOWED_TOOLS.to_string(),
                            });
                            mcp_config_path = Some(path);
                            _cap_guard = Some(guard);
                        }
                        Err(err) => {
                            tracing::warn!(epic = %epic_id, error = %err, "MCP config write failed; running without tools");
                        }
                    }
                }
                _ => {
                    tracing::debug!(epic = %epic_id, "tools-enabled phase without a ready clone or base URL; running without MCP");
                }
            }
        }

        // Cross-phase continuity (T-205): a later phase is a separate harness
        // session, so it cannot see the earlier phase's conversation. Seed the
        // technical phase with the epic's title + product context so it builds on
        // the product outcome (and, together with the read-only clone + MCP tools
        // below, "has code-inspection context" per the T-205 AC).
        let continuity = if config.phase == "technical" {
            technical_continuity(state.db.conn(), &epic_id).await
        } else {
            None
        };

        let req = PlanningRunRequest {
            run_id: ulid::Ulid::new().to_string(),
            prompt: user_content,
            cwd,
            resume,
            system_prompt: config.system_prompt,
            continuity,
            mcp,
        };

        let rx = state.planner.run(req);
        let hub = state.hub.clone();
        let topic = format!("epic:{epic_id}");

        // Drain the BLOCKING receiver off the async runtime. Relay each event
        // live, and accumulate what must be persisted after the stream ends.
        let drained = tokio::task::spawn_blocking(move || {
            let mut outcome = RunOutcome::default();
            for event in rx {
                let payload = serde_json::to_value(&event).unwrap_or(Value::Null);
                hub.publish(&topic, ws_type(&event), payload);
                outcome.absorb(&event);
            }
            outcome
        })
        .await;

        // The agent process has exited (the receiver hung up), so the MCP config
        // temp file is no longer needed; the capability token is revoked when
        // `_cap_guard` drops at the end of this task.
        if let Some(path) = &mcp_config_path {
            let _ = tokio::fs::remove_file(path).await;
        }

        let outcome = match drained {
            Ok(outcome) => outcome,
            // The blocking task panicked; nothing reliable to persist.
            Err(_) => return,
        };

        // Persist to the durable transcript (source of truth). Tool events first,
        // in arrival order, then the assembled agent reply — all on the epic's
        // monotonic seq via the shared T-201 store helper.
        let conn = state.db.conn();
        for tool_event in &outcome.tool_events {
            let _ = append_message(conn, &epic_id, &phase, "tool", tool_event).await;
        }
        if !outcome.text.is_empty() {
            let _ = append_message(conn, &epic_id, &phase, "agent", &outcome.text).await;
        }
        // Stash the resume handle so the next turn (and a restarted server)
        // continues this session instead of starting a new one.
        if let Some(session_id) = &outcome.session_id {
            let _ = set_harness_session_id(conn, &epic_id, &phase, session_id).await;
        }
    });
}

// ---- test doubles (crate-visible so `epics` tests can inject them) --------

#[cfg(test)]
pub(crate) mod testing {
    use super::*;
    use std::sync::{Arc, Condvar, Mutex};

    /// A run the fake was asked to perform — lets tests assert what the engine
    /// passed (notably the `resume` id on a follow-up turn). `run_id`/`prompt`
    /// are captured for diagnostics even when a given test only checks `resume`.
    #[derive(Clone, Debug)]
    #[allow(dead_code)]
    pub struct RecordedRun {
        pub run_id: String,
        pub prompt: String,
        pub resume: Option<String>,
        /// The cross-phase continuity preamble the engine passed (T-205): `Some`
        /// for a technical run seeded with product context, `None` otherwise.
        pub continuity: Option<String>,
    }

    /// A one-shot gate: the fake's run thread blocks before its terminal
    /// `Exited` until [`Gate::release`] is called, so a test can hold a run
    /// in-flight deterministically (no sleeps).
    #[derive(Default)]
    pub struct Gate {
        released: Mutex<bool>,
        cv: Condvar,
    }

    impl Gate {
        pub fn wait(&self) {
            let mut released = self.released.lock().unwrap();
            while !*released {
                released = self.cv.wait(released).unwrap();
            }
        }

        pub fn release(&self) {
            *self.released.lock().unwrap() = true;
            self.cv.notify_all();
        }
    }

    /// Scripted [`PlanningAgent`]: emits Started → Session{session_id} → one
    /// `Text` per chunk → Exited, records each request, and (optionally) blocks
    /// on a [`Gate`] before Exited so a run can be pinned in-flight.
    pub struct ScriptedPlanningAgent {
        session_id: String,
        chunks: Vec<String>,
        recorded: Arc<Mutex<Vec<RecordedRun>>>,
        gate: Option<Arc<Gate>>,
    }

    impl ScriptedPlanningAgent {
        pub fn new(session_id: &str, chunks: &[&str]) -> ScriptedPlanningAgent {
            ScriptedPlanningAgent {
                session_id: session_id.to_string(),
                chunks: chunks.iter().map(|s| s.to_string()).collect(),
                recorded: Arc::new(Mutex::new(Vec::new())),
                gate: None,
            }
        }

        /// Attach a gate that pins each run in-flight until released.
        pub fn with_gate(mut self, gate: Arc<Gate>) -> ScriptedPlanningAgent {
            self.gate = Some(gate);
            self
        }

        /// Handle to the recorded runs (for assertions on resume/prompt).
        pub fn recorded(&self) -> Arc<Mutex<Vec<RecordedRun>>> {
            self.recorded.clone()
        }
    }

    impl PlanningAgent for ScriptedPlanningAgent {
        fn run(&self, req: PlanningRunRequest) -> Receiver<RunEvent> {
            self.recorded.lock().unwrap().push(RecordedRun {
                run_id: req.run_id.clone(),
                prompt: req.prompt.clone(),
                resume: req.resume.clone(),
                continuity: req.continuity.clone(),
            });

            let (tx, rx) = std::sync::mpsc::channel();
            let run_id = req.run_id;
            let session_id = self.session_id.clone();
            let chunks = self.chunks.clone();
            let gate = self.gate.clone();

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
                if let Some(gate) = gate {
                    gate.wait();
                }
                let _ = tx.send(RunEvent::Exited {
                    run_id,
                    exit_code: Some(0),
                    cancelled: false,
                });
                // tx drops here → receiver hangs up, matching the real harness.
            });

            rx
        }
    }

    /// A [`PlanningAgent`] that emits only Started → Exited: it streams nothing
    /// and persists nothing. Injected by the T-201 store tests so that
    /// message-triggered runs stay side-effect-free while those tests assert
    /// pure transcript-store behavior.
    pub struct SilentPlanningAgent;

    impl PlanningAgent for SilentPlanningAgent {
        fn run(&self, req: PlanningRunRequest) -> Receiver<RunEvent> {
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
    use crate::epics::{append_message, load_transcript};
    use crate::{app, AppState, Config, Db};
    use axum::body::Body;
    use axum::http::{
        header::{AUTHORIZATION, CONTENT_TYPE},
        Request, StatusCode,
    };
    use serde_json::{json, Value};
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

    /// Boot a state whose planner is `planner`, plus the router over it.
    async fn app_with(planner: Arc<dyn PlanningAgent>) -> (AppState, axum::Router) {
        let db = Db::connect(":memory:").await.unwrap();
        db.run_migrations().await.unwrap();
        let state = AppState::with_planner(Config::for_test(TOKEN), db, planner);
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

    async fn post_message(app: &axum::Router, epic_id: &str, content: &str) -> StatusCode {
        post_message_phase(app, epic_id, "product", content).await
    }

    async fn post_message_phase(
        app: &axum::Router,
        epic_id: &str,
        phase: &str,
        content: &str,
    ) -> StatusCode {
        let posted = app
            .clone()
            .oneshot(req(
                "POST",
                &format!("/epics/{epic_id}/messages"),
                Some(json!({ "phase": phase, "content": content })),
            ))
            .await
            .unwrap();
        posted.status()
    }

    /// Advance an epic product → technical; return the response for assertions.
    async fn advance_phase(app: &axum::Router, epic_id: &str) -> axum::response::Response {
        app.clone()
            .oneshot(req("POST", &format!("/epics/{epic_id}/advance-phase"), None))
            .await
            .unwrap()
    }

    /// Collect published frames from a hub subscription until an `exited` frame
    /// arrives (or a timeout). Returns the `type` of each frame in order.
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

    /// Poll the transcript until it has at least `n` messages (or timeout), so a
    /// test can wait for the run's post-stream DB writes to land.
    async fn wait_for_transcript(state: &AppState, epic_id: &str, n: usize) -> Vec<Value> {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        loop {
            let items = load_transcript(state.db.conn(), epic_id).await.unwrap();
            if items.len() >= n || tokio::time::Instant::now() >= deadline {
                return items
                    .into_iter()
                    .map(|m| serde_json::to_value(m).unwrap())
                    .collect();
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    #[tokio::test]
    async fn message_triggers_run_streams_in_order_and_persists() {
        let agent = Arc::new(ScriptedPlanningAgent::new("sess-1", &["Hello", ", ", "world"]));
        let (state, app) = app_with(agent).await;
        let project_id = seed_project(&app).await;
        let epic_id = create_epic(&app, &project_id).await;

        // Subscribe to the epic topic BEFORE posting so no frame is missed.
        let sub = state.hub.subscribe(&format!("epic:{epic_id}"));

        assert_eq!(post_message(&app, &epic_id, "hi there").await, StatusCode::CREATED);

        // The relayed events arrive in order: started, session, text*, exited.
        let frames = collect_until_exited(sub).await;
        let types: Vec<&str> = frames.iter().map(|f| f["type"].as_str().unwrap()).collect();
        assert_eq!(
            types,
            vec!["started", "session", "text", "text", "text", "exited"],
            "RunEvents must relay in order"
        );
        // Payload is the serialized RunEvent (camelCase kind-tagged).
        assert_eq!(frames[0]["payload"]["kind"], "started");
        assert_eq!(frames[1]["payload"]["sessionId"], "sess-1");
        assert_eq!(frames[2]["payload"]["delta"], "Hello");

        // Durable store: user message + assembled agent reply, in seq order.
        let items = wait_for_transcript(&state, &epic_id, 2).await;
        assert_eq!(items.len(), 2, "user + agent message persisted");
        assert_eq!(items[0]["role"], "user");
        assert_eq!(items[0]["content"], "hi there");
        assert_eq!(items[0]["seq"], 1);
        assert_eq!(items[1]["role"], "agent");
        assert_eq!(items[1]["content"], "Hello, world", "text deltas assembled");
        assert_eq!(items[1]["seq"], 2);
        assert_eq!(items[1]["phase"], "product");
    }

    #[tokio::test]
    async fn session_id_is_persisted_and_resumed_on_the_next_turn() {
        let agent = Arc::new(ScriptedPlanningAgent::new("sess-xyz", &["ok"]));
        let recorded = agent.recorded();
        let (state, app) = app_with(agent).await;
        let project_id = seed_project(&app).await;
        let epic_id = create_epic(&app, &project_id).await;

        // Turn 1: run captures + persists the session id.
        assert_eq!(post_message(&app, &epic_id, "first").await, StatusCode::CREATED);
        wait_for_transcript(&state, &epic_id, 2).await; // user + agent

        // The planning session now carries the harness resume id.
        let mut rows = state
            .db
            .conn()
            .query(
                "SELECT harness_session_id FROM planning_session \
                 WHERE epic_id = ?1 AND phase = 'product'",
                libsql::params![epic_id.clone()],
            )
            .await
            .unwrap();
        let row = rows.next().await.unwrap().unwrap();
        assert_eq!(
            row.get::<Option<String>>(0).unwrap().as_deref(),
            Some("sess-xyz")
        );

        // Turn 2: the run must receive that id as `resume`.
        assert_eq!(post_message(&app, &epic_id, "second").await, StatusCode::CREATED);
        wait_for_transcript(&state, &epic_id, 4).await; // + user + agent

        let runs = recorded.lock().unwrap();
        assert_eq!(runs.len(), 2, "two runs happened");
        assert_eq!(runs[0].resume, None, "first turn opens a fresh session");
        assert_eq!(
            runs[1].resume.as_deref(),
            Some("sess-xyz"),
            "second turn resumes the captured session id"
        );
    }

    #[tokio::test]
    async fn concurrent_trigger_is_ignored_and_transcript_is_uncorrupted() {
        let gate = Arc::new(Gate::default());
        let agent = Arc::new(ScriptedPlanningAgent::new("sess-c", &["reply"]).with_gate(gate.clone()));
        let recorded = agent.recorded();
        let (state, app) = app_with(agent).await;
        let project_id = seed_project(&app).await;
        let epic_id = create_epic(&app, &project_id).await;

        // Trigger 1: the gated run pins the epic in-flight. The handler returns
        // as soon as the run is spawned, so once this POST responds the in-flight
        // slot is held.
        assert_eq!(post_message(&app, &epic_id, "one").await, StatusCode::CREATED);

        // Trigger 2 while run 1 is still in flight: the user message is stored,
        // but no overlapping run starts.
        assert_eq!(post_message(&app, &epic_id, "two").await, StatusCode::CREATED);

        // Let run 1 finish, then wait for its agent reply to land.
        gate.release();
        let items = wait_for_transcript(&state, &epic_id, 3).await;

        // Exactly one run was started (the second trigger was ignored).
        assert_eq!(recorded.lock().unwrap().len(), 1, "second run not started");

        // seqs are a contiguous 1..N with no gaps/dupes: two user messages plus
        // the single agent reply.
        assert_eq!(items.len(), 3);
        let seqs: Vec<i64> = items.iter().map(|m| m["seq"].as_i64().unwrap()).collect();
        assert_eq!(seqs, vec![1, 2, 3], "no seq gaps or dupes");
        let roles: Vec<&str> = items.iter().map(|m| m["role"].as_str().unwrap()).collect();
        assert_eq!(roles, vec!["user", "user", "agent"]);
    }

    #[test]
    fn ws_type_maps_every_common_event() {
        assert_eq!(
            ws_type(&RunEvent::Started {
                run_id: "r".into()
            }),
            "started"
        );
        assert_eq!(
            ws_type(&RunEvent::Text {
                run_id: "r".into(),
                delta: "x".into()
            }),
            "text"
        );
        assert_eq!(
            ws_type(&RunEvent::Exited {
                run_id: "r".into(),
                exit_code: Some(0),
                cancelled: false
            }),
            "exited"
        );
    }

    // Guard against the silent test double drifting: it must persist nothing so
    // the T-201 store tests stay pure.
    #[tokio::test]
    async fn silent_agent_adds_nothing_to_the_transcript() {
        let (state, app) = app_with(Arc::new(SilentPlanningAgent)).await;
        let project_id = seed_project(&app).await;
        let epic_id = create_epic(&app, &project_id).await;

        // Seed a user message directly, then trigger a run via HTTP.
        append_message(state.db.conn(), &epic_id, "product", "user", "seed")
            .await
            .unwrap();
        assert_eq!(post_message(&app, &epic_id, "again").await, StatusCode::CREATED);

        // Give any run time to complete; the silent agent writes nothing, so only
        // the two user messages remain.
        tokio::time::sleep(Duration::from_millis(100)).await;
        let items = load_transcript(state.db.conn(), &epic_id).await.unwrap();
        assert_eq!(items.len(), 2);
        assert!(items.iter().all(|m| m.role == "user"));
    }

    // ---- T-205: two-phase planning on one transcript ----

    /// Both phases run against the same epic on ONE continuous transcript: a
    /// product turn, then advance, then a technical turn — `phase` recorded per
    /// message, `seq` globally monotonic across both phases. The technical run is
    /// seeded with the product context (continuity), proving it builds on the
    /// product outcome.
    #[tokio::test]
    async fn advance_then_technical_shares_transcript_with_phase_and_monotonic_seq() {
        let agent = Arc::new(ScriptedPlanningAgent::new("sess-1", &["reply"]));
        let recorded = agent.recorded();
        let (state, app) = app_with(agent).await;
        let project_id = seed_project(&app).await;
        let epic_id = create_epic(&app, &project_id).await;

        // Product turn.
        assert_eq!(post_message(&app, &epic_id, "product idea").await, StatusCode::CREATED);
        wait_for_transcript(&state, &epic_id, 2).await; // user + agent

        // Record a product context, so the technical continuity has real content.
        state
            .db
            .conn()
            .execute(
                "UPDATE epic SET product_context = ?1 WHERE id = ?2",
                libsql::params!["Users need one-click export.".to_string(), epic_id.clone()],
            )
            .await
            .unwrap();

        // Advance product → technical.
        let advanced = advance_phase(&app, &epic_id).await;
        assert_eq!(advanced.status(), StatusCode::CREATED);
        let sessions = body_json(advanced).await;
        let items = sessions["items"].as_array().unwrap();
        let by_phase = |p: &str| items.iter().find(|s| s["phase"] == p).unwrap().clone();
        assert_eq!(by_phase("product")["status"], "complete");
        assert_eq!(by_phase("technical")["status"], "active");
        // The internal resume handle is never exposed.
        assert!(items.iter().all(|s| s.get("harness_session_id").is_none()));

        // Technical turn on the SAME transcript.
        assert_eq!(
            post_message_phase(&app, &epic_id, "technical", "how should we build it?").await,
            StatusCode::CREATED
        );
        let items = wait_for_transcript(&state, &epic_id, 4).await;

        // One continuous transcript: monotonic seq 1..4, phase recorded per msg.
        assert_eq!(items.len(), 4);
        let seqs: Vec<i64> = items.iter().map(|m| m["seq"].as_i64().unwrap()).collect();
        assert_eq!(seqs, vec![1, 2, 3, 4], "seq stays globally monotonic across phases");
        let phases: Vec<&str> = items.iter().map(|m| m["phase"].as_str().unwrap()).collect();
        assert_eq!(phases, vec!["product", "product", "technical", "technical"]);
        let roles: Vec<&str> = items.iter().map(|m| m["role"].as_str().unwrap()).collect();
        assert_eq!(roles, vec!["user", "agent", "user", "agent"]);

        // The technical run was seeded with the product context (continuity).
        let runs = recorded.lock().unwrap();
        assert_eq!(runs.len(), 2, "one product run + one technical run");
        let tech = &runs[1];
        let continuity = tech.continuity.as_deref().expect("technical run carries continuity");
        assert!(
            continuity.contains("Users need one-click export."),
            "continuity must seed the product context, got: {continuity}"
        );
    }

    /// Per-phase native resume: the technical session resumes ITS OWN
    /// `harness_session_id`, never the product session's. The first technical run
    /// opens fresh (resume `None`) even though the product session already has a
    /// captured id; a second technical turn resumes the technical id.
    #[tokio::test]
    async fn technical_run_resumes_the_technical_session_not_the_product_one() {
        let agent = Arc::new(ScriptedPlanningAgent::new("sess-1", &["ok"]));
        let recorded = agent.recorded();
        let (state, app) = app_with(agent).await;
        let project_id = seed_project(&app).await;
        let epic_id = create_epic(&app, &project_id).await;

        // Product turn captures the product session's harness id.
        assert_eq!(post_message(&app, &epic_id, "p").await, StatusCode::CREATED);
        wait_for_transcript(&state, &epic_id, 2).await;

        assert_eq!(advance_phase(&app, &epic_id).await.status(), StatusCode::CREATED);

        // Technical turn 1, then turn 2.
        assert_eq!(post_message_phase(&app, &epic_id, "technical", "t1").await, StatusCode::CREATED);
        wait_for_transcript(&state, &epic_id, 4).await;
        assert_eq!(post_message_phase(&app, &epic_id, "technical", "t2").await, StatusCode::CREATED);
        wait_for_transcript(&state, &epic_id, 6).await;

        let resumes: Vec<Option<String>> = {
            let runs = recorded.lock().unwrap();
            assert_eq!(runs.len(), 3, "product + two technical runs");
            runs.iter().map(|r| r.resume.clone()).collect()
        };
        assert_eq!(resumes[0], None, "product turn 1 opens fresh");
        assert_eq!(
            resumes[1], None,
            "technical turn 1 must open a FRESH session, not resume the product one"
        );
        assert_eq!(
            resumes[2].as_deref(),
            Some("sess-1"),
            "technical turn 2 resumes the technical session id"
        );

        // Both sessions carry their own (here identical-valued) captured id.
        let harness_id = |phase: &'static str| {
            let conn = state.db.conn().clone();
            let epic_id = epic_id.clone();
            async move {
                let mut rows = conn
                    .query(
                        "SELECT harness_session_id FROM planning_session \
                         WHERE epic_id = ?1 AND phase = ?2",
                        libsql::params![epic_id, phase],
                    )
                    .await
                    .unwrap();
                rows.next().await.unwrap().unwrap().get::<Option<String>>(0).unwrap()
            }
        };
        assert_eq!(harness_id("product").await.as_deref(), Some("sess-1"));
        assert_eq!(harness_id("technical").await.as_deref(), Some("sess-1"));
    }

    /// A `technical`-phase message before advancing is rejected with the standard
    /// error envelope (409 conflict) and appends nothing to the transcript.
    #[tokio::test]
    async fn technical_message_before_advancing_is_rejected() {
        let agent = Arc::new(ScriptedPlanningAgent::new("sess-1", &["ok"]));
        let (state, app) = app_with(agent).await;
        let project_id = seed_project(&app).await;
        let epic_id = create_epic(&app, &project_id).await;

        let response = app
            .clone()
            .oneshot(req(
                "POST",
                &format!("/epics/{epic_id}/messages"),
                Some(json!({ "phase": "technical", "content": "too early" })),
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::CONFLICT);
        assert_eq!(body_json(response).await["error"]["code"], "conflict");

        // Nothing was persisted for the rejected technical message.
        let items = load_transcript(state.db.conn(), &epic_id).await.unwrap();
        assert!(items.is_empty(), "rejected message must not persist");
    }

    /// The technical run's `update_epic` (via the MCP handler) writes
    /// `technical_context` and publishes `epic_updated` on `epic:<id>` — the epic
    /// fills in live during the technical phase exactly as it does for product.
    #[tokio::test]
    async fn technical_phase_update_epic_writes_technical_context_and_publishes() {
        let (state, app) = app_with(Arc::new(SilentPlanningAgent)).await;
        let project_id = seed_project(&app).await;
        let epic_id = create_epic(&app, &project_id).await;
        assert_eq!(advance_phase(&app, &epic_id).await.status(), StatusCode::CREATED);

        // A technical-phase run mints a capability scoped to (epic, technical).
        let guard = state.caps.mint(
            epic_id.clone(),
            project_id.clone(),
            "technical".into(),
            std::path::PathBuf::from("/tmp"),
        );

        let mut sub = state.hub.subscribe(&format!("epic:{epic_id}"));

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(format!("/mcp/{}", guard.token()))
                    .header(CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        json!({"jsonrpc":"2.0","id":1,"method":"tools/call",
                               "params":{"name":"update_epic",
                                         "arguments":{"content":"Add an axum route + libSQL migration."}}})
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        // The technical column filled; product stayed NULL.
        let epic = crate::epics::fetch_epic(state.db.conn(), &epic_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            epic.technical_context.as_deref(),
            Some("Add an axum route + libSQL migration.")
        );
        assert_eq!(epic.product_context, None);

        // Live epic_updated frame carried the technical context.
        let frame = sub.recv().await.unwrap();
        let value: Value = serde_json::from_str(&frame).unwrap();
        assert_eq!(value["type"], "epic_updated");
        assert_eq!(
            value["payload"]["technical_context"],
            "Add an axum route + libSQL migration."
        );
    }

    /// Ignore-marked live smoke test: drives the REAL `claude` CLI end to end.
    /// Excluded from the hermetic gate (needs auth + network); run with
    /// `cargo test -p deerborn-server -- --ignored`.
    #[tokio::test]
    #[ignore]
    async fn live_claude_planning_run() {
        let (state, app) = app_with(Arc::new(ClaudePlanningAgent::new())).await;
        let project_id = seed_project(&app).await;
        let epic_id = create_epic(&app, &project_id).await;
        let sub = state.hub.subscribe(&format!("epic:{epic_id}"));
        assert_eq!(
            post_message(&app, &epic_id, "In one sentence, what makes a good acceptance criterion?")
                .await,
            StatusCode::CREATED
        );
        let frames = collect_until_exited(sub).await;
        assert!(frames.iter().any(|f| f["type"] == "text"), "got streamed text");
        let items = wait_for_transcript(&state, &epic_id, 2).await;
        assert!(items.iter().any(|m| m["role"] == "agent"));
    }
}
