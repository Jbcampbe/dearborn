//! The one-shot AFK node engine — research & AFK-task map nodes (wayfinder
//! epic §5).
//!
//! Research nodes (and `task` nodes created with `task_mode = "afk"`) run
//! **unattended**: a single, non-interactive agent turn fired at a node that
//! investigates its question and reports facts. There is no resume, no
//! multi-turn transcript, and no human on the other end — the mirror of the
//! interactive engine ([`crate::node_engine`]) on the one-shot shape of
//! [`crate::breakdown`]. Every fired node runs under the **per-node run-lock**
//! ([`crate::AppState::node_inflight`]), so the frontier's AFK nodes fire and
//! run in parallel.
//!
//! ## The contract: report facts, never reshape the map
//!
//! An unattended agent must not be able to silently redraw the frontier
//! overnight (epic §6): **AFK kinds never reshape the map** — no
//! `node create`, `node link`, `node resolve`, no map-prose edits. This is
//! enforced structurally, not by prompt discipline alone:
//!
//! * the run is wired to **no `dearborn` CLI at all** — there is no capability
//!   token, so the CLI's map-mutating verbs (and every other authenticated
//!   route) are simply not in the AFK run's allow-list; and
//! * the run's only write is performed by **Dearborn itself** after the agent
//!   exits: the agent's final message is recorded verbatim as the node's
//!   `gist` and the node settles to `resolved` (a fenced update that only
//!   lands while the node is still `open`/`in_progress`).
//!
//! The run's read surface is the project's checkout as `cwd` (native read
//! tools) plus a context header in the prompt (the epic's destination and the
//! node's title/question) — the same grounding the interactive engine gives a
//! first turn.
//!
//! ## The loop
//!
//! 1. `POST /epics/:id/map-nodes/:nodeId/run` fires the node: the per-node
//!    run-lock is claimed, the node flips to the soft `in_progress` signal,
//!    and a background one-shot agent run is spawned (`202 Accepted`).
//! 2. The run drives the [`AfkAgent`] seam (production [`ClaudeAfkAgent`];
//!    tests inject a scripted fake — the [`crate::BreakdownAgent`]-style
//!    double), relaying every `RunEvent` live to `node:<id>`.
//! 3. On a clean exit the agent's report lands in `gist`, the node settles to
//!    `resolved` (its dependents unblock), a `map_updated` frame fans out on
//!    `epic:<id>`, and an `agent_run` evidence row records the run. On a
//!    failed run the node stays `in_progress` (re-firable) and the evidence
//!    row closes as `error`.
//!
//! AFK nodes never create a `node_session` row (migration 0015: "AFK/no-engine
//! nodes may never create a row") — there is nothing to resume.

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

use crate::agent_slot::AgentSlot;
use crate::map::MapNode;
use crate::{AppError, AppResult, AppState, NodeRunGuard};

// ---- per-kind system prompts (adapted from matt-pocock-skills) --------------

/// The research engine's methodology (wayfinder epic §5), adapted from
/// `matt-pocock-skills`' `research` skill as source material. A research node
/// is unattended fact-finding: it reports primary-source facts and never
/// touches the map.
pub(crate) const RESEARCH_PROMPT: &str = "\
You are Dearborn's research agent, working ONE research node of a planning map \
(the \"Wayfinder\" model: charting a route through fog toward a destination). \
You run UNATTENDED — no human is available, so never ask questions: investigate, \
then report. This is planning, not building: your job is FACTS a human needs to \
settle the node's decision, not a slice of work.

Method:
- Investigate the node's question against PRIMARY sources — official docs, source \
code, specs, first-party APIs — not a secondary write-up of them. Follow every claim \
back to the source that owns it.
- You have read-only access to the project's checkout as your working directory; when \
the question touches this codebase, answer from the code itself.
- Cite each claim's source (file + line, doc URL, spec section).
- Separate facts from judgement: report what IS, flag what you could not determine, \
and never guess silently.

You have NO write access to the planning map: you cannot create, link, resolve, or \
edit nodes, and you cannot edit any map prose — the map does not change while you \
run. Your ONLY output is your final message: Dearborn records it verbatim as the \
node's gist. Make it a compact, factual report — the settled answer to the node's \
question first, then the supporting facts with their sources.";

/// The AFK-task engine's methodology (wayfinder epic §5). A `task` node created
/// with `task_mode = "afk"` is small manual work that unblocks a decision,
/// done unattended; its outcome is recorded as the node's gist.
pub(crate) const AFK_TASK_PROMPT: &str = "\
You are Dearborn's AFK task agent, working ONE task node of a planning map (the \
\"Wayfinder\" model). The node is a small piece of manual work that unblocks a \
decision, and you run UNATTENDED — no human is available, so never ask questions: \
do the work, then report. This is planning support, not product building: finish \
the node's exact piece of work and stop.

- Do ONLY what the node's question asks — the smallest piece of work that settles \
it. No scope creep, no refactors beyond the node.
- You have read-only access to the project's checkout as your working directory.
- You have NO write access to the planning map: you cannot create, link, resolve, \
or edit nodes, and you cannot edit any map prose — the map does not change while \
you run.
- Your ONLY output is your final message: Dearborn records it verbatim as the \
node's gist. State the outcome in one factual line first (what was done, or what \
was found, or what is still blocked), then the supporting detail.";

// ---- kind → slot ------------------------------------------------------------

/// The agent slot an AFK node kind resolves its live settings under, or `None`
/// for a kind/mode this engine does not drive: research always, `task` only
/// when it was created `afk` (a HITL task is a human checklist with no engine),
/// and never the interactive kinds (grilling/prototype run on
/// [`crate::node_engine`]).
pub fn slot_for_node(kind: &str, task_mode: Option<&str>) -> Option<AgentSlot> {
    match (kind, task_mode) {
        ("research", _) => Some(AgentSlot::Research),
        ("task", Some("afk")) => Some(AgentSlot::AfkTask),
        _ => None,
    }
}

/// The compiled default system prompt for an AFK slot.
fn default_prompt_for_slot(slot: AgentSlot) -> &'static str {
    match slot {
        AgentSlot::Research => RESEARCH_PROMPT,
        AgentSlot::AfkTask => AFK_TASK_PROMPT,
        // The AFK engine only ever resolves its own two slots; any other slot
        // reaching this function is a wiring bug, not a runtime condition.
        other => unreachable!("afk engine asked for the default prompt of non-AFK slot {other}"),
    }
}

// ---- the agent seam ------------------------------------------------------

/// A one-shot AFK node run, decoupled from the harness so tests inject a
/// scripted agent (the [`crate::BreakdownAgent`]-style double). Built by
/// [`spawn_afk_run`] from the node's context.
pub struct AfkRunRequest {
    /// Unique id for this run (a ULID); echoed on every `RunEvent`.
    pub run_id: String,
    /// The fire instruction: a context header (epic destination, node
    /// title/question) followed by the instruction to report.
    pub prompt: String,
    /// Working directory: the project's read-only clone (code grounding).
    /// `None` when the clone isn't ready — the run proceeds without it.
    pub cwd: Option<PathBuf>,
    /// The slot's live-resolved effective system prompt (the project's
    /// override when set, else the compiled per-kind default).
    pub system_prompt: String,
    /// The slot this run was resolved under (Research or AfkTask).
    pub slot: AgentSlot,
    /// The harness key this run was resolved to. Validated with
    /// [`crate::agent_settings::harness_supports_slot`] by
    /// [`ClaudeAfkAgent::run`]; a harness this slot cannot run surfaces as an
    /// `Error` + `Exited` event stream (the trait has no `Result`).
    pub harness: String,
    /// The resolved model passed verbatim to the CLI; `None` → CLI default.
    pub model: Option<String>,
}

/// The seam that makes the AFK node engine hermetically testable (mirrors
/// [`crate::BreakdownAgent`]).
pub trait AfkAgent: Send + Sync {
    /// Start a one-shot run and hand back its blocking `RunEvent` receiver.
    fn run(&self, req: AfkRunRequest) -> Receiver<RunEvent>;
}

/// Production [`AfkAgent`]: drives Claude Code through the harness, one shot.
///
/// Deliberately narrower than breakdown's run: no `dearborn` CLI access block,
/// no capability token, no `bypassPermissions` — an unattended run gets no
/// write surface at all, so the map it is forbidden from reshaping is
/// structurally out of reach (see the module docs).
#[derive(Default)]
pub struct ClaudeAfkAgent;

impl ClaudeAfkAgent {
    /// Construct the production agent.
    pub fn new() -> ClaudeAfkAgent {
        ClaudeAfkAgent
    }
}

impl AfkAgent for ClaudeAfkAgent {
    fn run(&self, req: AfkRunRequest) -> Receiver<RunEvent> {
        let run_id = req.run_id.clone();

        // T7 spawn-validation, mirroring the planning/breakdown agents: a
        // harness this slot cannot run surfaces loudly through the same
        // synthetic Error+Exited stream a spawn failure uses.
        if !crate::agent_settings::harness_supports_slot(&req.harness, req.slot) {
            let (tx, rx) = std::sync::mpsc::channel();
            let _ = tx.send(RunEvent::Error {
                run_id: run_id.clone(),
                message: crate::agent_settings::unsupported_harness_message(&req.harness, req.slot),
            });
            let _ = tx.send(RunEvent::Exited {
                run_id,
                exit_code: None,
                cancelled: false,
            });
            return rx;
        }

        let request = RunRequest {
            run_id: req.run_id,
            prompt: req.prompt,
            cwd: req.cwd,
            // Read-only w.r.t. the codebase: enforced by the prompt steering
            // and the read-only clone as `cwd`, not by the mode. There is no
            // CLI access block and no permission bypass: the AFK run's only
            // output is its final message.
            mode: RunMode::Ask,
            tuning: RunTuning {
                extra_args: vec![
                    "--append-system-prompt".to_string(),
                    req.system_prompt.clone(),
                ],
                model: req.model.clone(),
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
                    message: format!("failed to start afk node run: {err}"),
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

// ---- route: fire a node -----------------------------------------------------

/// `POST /epics/{id}/map-nodes/{nodeId}/run` — fire a research or AFK-task
/// node: claim the per-node run-lock, flip the node to the soft `in_progress`
/// signal, and spawn the one-shot agent run in the background (`202`, events
/// stream on `node:<id>`; the gist + settle land when the run completes).
///
/// * `404` if the epic/node is unknown.
/// * `409` if the node is not an AFK kind (grilling/prototype run on the
///   interactive engine; a HITL task has no engine), if it is already settled
///   (`resolved`/`out_of_scope`), or if a run is already in flight for it.
pub async fn fire_node(
    State(state): State<AppState>,
    Path((epic_id, node_id)): Path<(String, String)>,
) -> AppResult<(StatusCode, Json<Value>)> {
    let conn = state.db.conn();

    let node = crate::map::fetch_node(conn, &node_id)
        .await?
        .filter(|node| node.epic_id == epic_id)
        .ok_or_else(|| AppError::NotFound(format!("map node {node_id} not found")))?;

    let slot = require_afk_node(&node)?;
    if matches!(node.state.as_str(), "resolved" | "out_of_scope") {
        return Err(AppError::Conflict(format!(
            "map node {node_id} is already settled (`{}`); an AFK node runs once",
            node.state
        )));
    }

    // One run at a time per node (the same per-node lock the interactive
    // engine holds across an agent reply), so different nodes run in parallel
    // while a node's own runs never interleave.
    let Some(guard) = state.try_acquire_node_run(&node_id) else {
        return Err(AppError::Conflict(format!(
            "a run is already in flight for map node {node_id}"
        )));
    };

    // Soft signal only (never a lock): flip open → in_progress so the map
    // shows the node being worked. A re-fire of an already in_progress node
    // (a failed run left it there) just runs again.
    if node.state == "open" {
        conn.execute(
            "UPDATE map_node SET state = 'in_progress', updated_at = ?2 WHERE id = ?1 AND state = 'open'",
            params![node_id.clone(), now_ms()],
        )
        .await?;
        crate::map::publish_map(&state, &epic_id).await;
    }

    spawn_afk_run(state.clone(), epic_id, node_id.clone(), slot, guard);

    Ok((
        StatusCode::ACCEPTED,
        Json(json!({ "status": "afk_run_started", "node_id": node_id })),
    ))
}

/// Guard that `node` is a kind/mode this engine drives, or `409` naming where
/// the node actually belongs.
fn require_afk_node(node: &MapNode) -> AppResult<AgentSlot> {
    slot_for_node(&node.kind, node.task_mode.as_deref()).ok_or_else(|| {
        if matches!(node.kind.as_str(), "grilling" | "prototype") {
            AppError::Conflict(format!(
                "map node {} is a `{}` node, which runs on the interactive engine — open its session instead",
                node.id, node.kind
            ))
        } else {
            AppError::Conflict(format!(
                "map node {} is a HITL `task` node, which has no engine — a human works its checklist",
                node.id
            ))
        }
    })
}

// ---- run orchestration ------------------------------------------------------

/// What a drained AFK run leaves behind, persisted after the stream ends.
#[derive(Default)]
struct AfkOutcome {
    /// Assembled assistant text (all `Text` deltas) — the agent's report.
    text: String,
    /// The harness session id captured from `RunEvent::Session` (evidence).
    session_id: Option<String>,
    /// The terminal `RunEvent::Error` message, if the run failed harness-side.
    error: Option<String>,
    exit_code: Option<i32>,
    cancelled: bool,
}

impl AfkOutcome {
    fn absorb(&mut self, event: &RunEvent) {
        match event {
            RunEvent::Text { delta, .. } => self.text.push_str(delta),
            RunEvent::Session {
                session_id: Some(id),
                ..
            } => self.session_id = Some(id.clone()),
            RunEvent::Error { message, .. } => self.error = Some(message.clone()),
            RunEvent::Exited {
                exit_code,
                cancelled,
                ..
            } => {
                self.exit_code = *exit_code;
                self.cancelled = *cancelled;
            }
            _ => {}
        }
    }

    /// Whether the run ended badly: a harness error, a cancel, a non-zero
    /// exit, or no report at all. A failed run records `error` evidence and
    /// leaves the node `in_progress` (re-firable) rather than settling it.
    fn failed(&self) -> Option<String> {
        if let Some(message) = &self.error {
            return Some(message.clone());
        }
        if self.cancelled {
            return Some("the run was cancelled".to_string());
        }
        if matches!(self.exit_code, Some(code) if code != 0) {
            return Some(format!("the run exited with {:?}", self.exit_code));
        }
        if self.text.trim().is_empty() {
            return Some("the run produced no report".to_string());
        }
        None
    }
}

/// Spawn the one-shot AFK run in the background and return immediately.
///
/// Holds `guard` (releasing the node's run-lock when the run finishes),
/// resolves the slot's live settings, drains the blocking `RunEvent` receiver
/// on `spawn_blocking` while relaying every event to `node:<id>`, then records
/// the report into the node's `gist`, settles the node, and writes the
/// `agent_run` evidence row.
pub fn spawn_afk_run(
    state: AppState,
    epic_id: String,
    node_id: String,
    slot: AgentSlot,
    guard: NodeRunGuard,
) {
    tokio::spawn(async move {
        let _guard = guard;
        let conn = state.db.conn();

        // The node may have been edited between fire and spawn; re-read for
        // the context header. Vanished mid-flight → nothing to run against.
        let Ok(Some(node)) = crate::map::fetch_node(conn, &node_id).await else {
            tracing::warn!(node = %node_id, "afk run: node vanished before run");
            return;
        };

        let project_id = crate::epics::get_epic_project_id(conn, &epic_id)
            .await
            .ok()
            .flatten()
            .unwrap_or_default();

        // Live-resolve the slot's settings (harness/model/system prompt),
        // exactly like breakdown and the interactive engine — a mid-flight
        // settings edit is picked up here.
        let spawn_cfg = match crate::agent_settings::spawn_config(
            &state.db,
            &project_id,
            slot,
            default_prompt_for_slot(slot),
        )
        .await
        {
            Ok(cfg) => cfg,
            Err(err) => {
                tracing::warn!(node = %node_id, error = %err, "afk run: failed to resolve agent settings; aborting");
                return;
            }
        };

        // Read-only grounding: the project's checkout when it is on disk.
        let cwd = crate::epics::get_epic_clone_path(conn, &epic_id)
            .await
            .ok()
            .flatten()
            .map(PathBuf::from);

        let req = AfkRunRequest {
            run_id: ulid::Ulid::new().to_string(),
            prompt: run_prompt(conn, &epic_id, &node).await,
            cwd,
            system_prompt: spawn_cfg.prompt,
            slot,
            harness: spawn_cfg.harness.clone(),
            model: spawn_cfg.model.clone(),
        };

        let rx = state.afk.run(req);
        let hub = state.hub.clone();
        let topic = format!("node:{node_id}");

        // Drain the BLOCKING receiver off the async runtime, relaying live.
        let drained = tokio::task::spawn_blocking(move || {
            let mut outcome = AfkOutcome::default();
            for event in rx {
                let payload = serde_json::to_value(&event).unwrap_or(Value::Null);
                hub.publish(&topic, crate::planning::ws_type(&event), payload);
                outcome.absorb(&event);
            }
            outcome
        })
        .await;

        let outcome = match drained {
            Ok(outcome) => outcome,
            Err(_) => return, // blocking task panicked; nothing reliable to persist
        };

        // A failed run leaves the node `in_progress` (re-firable via
        // POST …/run again) and records error evidence — mirroring
        // breakdown's refusal to settle on a failed write.
        let failure = outcome.failed();

        let mut log = outcome.text.trim().to_string();
        if let Some(reason) = &failure {
            tracing::warn!(node = %node_id, reason = %reason, "afk run: failed; node stays in_progress");
            log.push_str(&format!(
                "\n\n[dearborn] afk run failed: {reason} — the node stays `in_progress` \
                 and can be re-fired."
            ));
        }

        let run_id = ulid::Ulid::new().to_string();
        let status = if failure.is_some() { "error" } else { "ok" };
        let _ = conn
            .execute(
                "INSERT INTO agent_run (id, task_id, epic_id, stage, session_id, log, created_at, \
                 attempt, status, verdict, started_at, ended_at, exit_code, \
                 harness, model, prompt_hash) \
                 VALUES (?1, NULL, ?2, ?3, ?4, ?5, ?6, 1, ?7, NULL, ?6, ?6, NULL, \
                 ?8, ?9, ?10)",
                params![
                    run_id,
                    epic_id.clone(),
                    slot.as_str(),
                    outcome.session_id,
                    log,
                    now_ms(),
                    status,
                    spawn_cfg.harness,
                    spawn_cfg.model,
                    spawn_cfg.prompt_hash,
                ],
            )
            .await;

        // Dearborn — not the agent — performs the run's only map write: the
        // report becomes the gist and the node settles. Fenced on the node
        // still being unsettled, so a human ruling it out_of_scope mid-run
        // is never clobbered by a late report.
        if failure.is_none() {
            let report = outcome.text.trim();
            let settled = conn
                .execute(
                    "UPDATE map_node SET state = 'resolved', gist = ?2, updated_at = ?3 \
                     WHERE id = ?1 AND state IN ('open', 'in_progress')",
                    params![node_id.clone(), report, now_ms()],
                )
                .await;
            match settled {
                Ok(affected) => {
                    // The attribution feed row for the run's only map write
                    // (wayfinder epic §4.9 — see [`crate::activity`]): an
                    // unattended agent run, so no human actor. Recorded only
                    // when the fenced update actually settled the node (the
                    // same guard the report itself rode on).
                    if affected > 0 {
                        if let Err(err) = crate::activity::record(
                            &conn,
                            &epic_id,
                            Some(&node_id),
                            None,
                            crate::activity::NODE_RESOLVED,
                            Some(report),
                        )
                        .await
                        {
                            tracing::warn!(node = %node_id, error = %err, "afk run: failed to append the activity row")
                        }
                    }
                    crate::map::publish_map(&state, &epic_id).await
                }
                Err(err) => {
                    tracing::warn!(node = %node_id, error = %err, "afk run: failed to record the report as gist")
                }
            }
        }
    });
}

// ---- helpers ----------------------------------------------------------------

/// The fire prompt: a compact context header (node title/question, epic
/// destination — the same grounding the interactive engine's first turn gets)
/// followed by the instruction to work and report. One-shot, so the header
/// rides on every run.
async fn run_prompt(conn: &libsql::Connection, epic_id: &str, node: &MapNode) -> String {
    let mut lines = Vec::new();
    lines.push(format!(
        "You are working the \"{}\" node (kind: {}).",
        node.title, node.kind
    ));
    if let Some(question) = node.question.as_deref().filter(|q| !q.trim().is_empty()) {
        lines.push(format!("The work this node resolves: {question}"));
    }
    if let Ok(mut rows) = conn
        .query(
            "SELECT destination FROM epic WHERE id = ?1",
            params![epic_id],
        )
        .await
    {
        if let Ok(Some(row)) = rows.next().await {
            if let Ok(Some(destination)) = row.get::<Option<String>>(0) {
                if !destination.trim().is_empty() {
                    lines.push(format!("The epic's destination: {destination}"));
                }
            }
        }
    }
    lines.push(
        "Work this node unattended now and report as your system prompt instructs; \
         your final message is the report Dearborn records."
            .to_string(),
    );
    lines.join("\n")
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
    use crate::agent_slot::AgentSlot;
    use std::sync::{Arc, Mutex};

    /// One AFK run as the fake saw it — enough for a test to assert the engine
    /// passed the right prompt and system prompt (and, by the request shape's
    /// very absence of CLI wiring, that the run carries no write surface).
    #[derive(Clone, Debug)]
    #[allow(dead_code)]
    pub struct RecordedAfkRun {
        pub run_id: String,
        pub prompt: String,
        pub system_prompt: String,
        pub slot: AgentSlot,
        pub harness: String,
    }

    /// A [`crate::planning::testing::Gate`]-style one-shot gate: the fake's run
    /// thread blocks before its terminal `Exited` until released, so a test can
    /// hold AFK runs in flight deterministically (no sleeps).
    #[derive(Default)]
    pub struct AfkGate {
        released: Mutex<bool>,
        cv: std::sync::Condvar,
    }

    impl AfkGate {
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

    /// A scripted [`AfkAgent`]: per run it records the request, then emits
    /// Started → Session (a fixed session id) → Text* → Exited. An optional
    /// [`AfkGate`] pins every run in flight before its terminal `Exited`.
    pub struct ScriptedAfkAgent {
        session_id: String,
        chunks: Vec<String>,
        recorded: Arc<Mutex<Vec<RecordedAfkRun>>>,
        gate: Option<Arc<AfkGate>>,
    }

    impl ScriptedAfkAgent {
        pub fn new(session_id: &str, chunks: &[&str]) -> ScriptedAfkAgent {
            ScriptedAfkAgent {
                session_id: session_id.to_string(),
                chunks: chunks.iter().map(|s| s.to_string()).collect(),
                recorded: Arc::new(Mutex::new(Vec::new())),
                gate: None,
            }
        }

        /// Pin every run in flight on `gate` until [`AfkGate::release`].
        pub fn with_gate(mut self, gate: Arc<AfkGate>) -> ScriptedAfkAgent {
            self.gate = Some(gate);
            self
        }

        pub fn recorded(&self) -> Arc<Mutex<Vec<RecordedAfkRun>>> {
            self.recorded.clone()
        }
    }

    impl AfkAgent for ScriptedAfkAgent {
        fn run(&self, req: AfkRunRequest) -> Receiver<RunEvent> {
            self.recorded.lock().unwrap().push(RecordedAfkRun {
                run_id: req.run_id.clone(),
                prompt: req.prompt.clone(),
                system_prompt: req.system_prompt.clone(),
                slot: req.slot,
                harness: req.harness.clone(),
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
            });
            rx
        }
    }

    /// An [`AfkAgent`] whose run fails harness-side: Started → Error → Exited.
    /// The node must stay `in_progress` with error evidence, never settle.
    pub struct FailingAfkAgent;

    impl AfkAgent for FailingAfkAgent {
        fn run(&self, req: AfkRunRequest) -> Receiver<RunEvent> {
            let (tx, rx) = std::sync::mpsc::channel();
            let run_id = req.run_id;
            std::thread::spawn(move || {
                let _ = tx.send(RunEvent::Started {
                    run_id: run_id.clone(),
                });
                let _ = tx.send(RunEvent::Error {
                    run_id: run_id.clone(),
                    message: "the harness could not spawn".to_string(),
                });
                let _ = tx.send(RunEvent::Exited {
                    run_id,
                    exit_code: None,
                    cancelled: false,
                });
            });
            rx
        }
    }
}

// ---- tests ------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::testing::{AfkGate, FailingAfkAgent, ScriptedAfkAgent};
    use super::*;
    use crate::users::{self, Role};
    use crate::{app, AppState, Config, Db};
    use axum::body::Body;
    use axum::http::{header::AUTHORIZATION, Request, StatusCode};
    use serde_json::Value;
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::sync::broadcast;
    use tower::ServiceExt; // for `oneshot`

    /// Boot state (with an injected scripted AFK agent) + router.
    async fn boot(afk: Arc<dyn AfkAgent>) -> (AppState, axum::Router) {
        let db = Db::connect(":memory:").await.unwrap();
        db.run_migrations().await.unwrap();
        let state = AppState::new(Config::for_test(), db).with_afk(afk);
        let router = app(state.clone());
        (state, router)
    }

    /// Insert a project + epic (with a destination); return the epic id.
    async fn seed_epic(state: &AppState) -> String {
        let conn = state.db.conn();
        let now = now_ms();
        let project_id = ulid::Ulid::new().to_string();
        conn.execute(
            "INSERT INTO project (id, name, repo_url, clone_path, clone_status, created_at, updated_at) \
             VALUES (?1, 'P', 'https://example.com/p.git', NULL, 'ready', ?2, ?2)",
            params![project_id.clone(), now],
        )
        .await
        .unwrap();
        let epic_id = ulid::Ulid::new().to_string();
        conn.execute(
            "INSERT INTO epic (id, project_id, title, status, destination, created_at, updated_at) \
             VALUES (?1, ?2, 'E', 'Planning', 'It works end to end', ?3, ?3)",
            params![epic_id.clone(), project_id, now],
        )
        .await
        .unwrap();
        epic_id
    }

    /// Insert a node directly through the map store; return its id.
    async fn seed_node(
        state: &AppState,
        epic_id: &str,
        kind: &str,
        task_mode: Option<&str>,
    ) -> String {
        crate::map::create_node(
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

    fn post_bearer(uri: &str, token: &str) -> Request<Body> {
        Request::builder()
            .method("POST")
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

    async fn login(state: &AppState) -> String {
        let user = users::testing::seed_user(state, "planner", Role::Admin, true).await;
        crate::sessions::testing::login_as(state, &user).await
    }

    async fn fire(
        app: &axum::Router,
        token: &str,
        epic_id: &str,
        node_id: &str,
    ) -> axum::response::Response {
        app.clone()
            .oneshot(post_bearer(
                &format!("/epics/{epic_id}/map-nodes/{node_id}/run"),
                token,
            ))
            .await
            .unwrap()
    }

    /// Poll until the node reaches `state` (or timeout); returns the node.
    async fn wait_for_node_state(
        state: &AppState,
        node_id: &str,
        target: &str,
    ) -> crate::map::MapNode {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        loop {
            let node = crate::map::fetch_node(state.db.conn(), node_id)
                .await
                .unwrap()
                .unwrap();
            if node.state == target || tokio::time::Instant::now() >= deadline {
                return node;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    /// Whether the per-node run-lock currently holds `node_id`.
    fn node_locked(state: &AppState, node_id: &str) -> bool {
        state.node_inflight.lock().unwrap().contains(node_id)
    }

    async fn wait_until_unlocked(state: &AppState, node_id: &str) {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        while node_locked(state, node_id) && tokio::time::Instant::now() < deadline {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    async fn agent_run_rows(
        state: &AppState,
        epic_id: &str,
    ) -> Vec<(String, String, Option<String>)> {
        let mut rows = state
            .db
            .conn()
            .query(
                "SELECT stage, status, session_id FROM agent_run WHERE epic_id = ?1 ORDER BY created_at ASC",
                params![epic_id],
            )
            .await
            .unwrap();
        let mut items = Vec::new();
        while let Some(row) = rows.next().await.unwrap() {
            items.push((
                row.get(0).unwrap(),
                row.get(1).unwrap(),
                row.get(2).unwrap(),
            ));
        }
        items
    }

    // ---- AC: a research node runs unattended, records a factual gist, and
    // ---- mutates nothing else on the map ------------------------------------

    #[tokio::test]
    async fn a_research_node_runs_unattended_records_a_gist_and_settles() {
        let agent = Arc::new(ScriptedAfkAgent::new(
            "afk-sess",
            &["libsql supports BLOB. ", "Source: evidence.rs."],
        ));
        let recorded = agent.recorded();
        let (state, app) = boot(agent).await;
        let token = login(&state).await;
        let epic_id = seed_epic(&state).await;
        let node_id = seed_node(&state, &epic_id, "research", None).await;
        let sub = state.hub.subscribe(&format!("node:{node_id}"));

        let response = fire(&app, &token, &epic_id, &node_id).await;
        assert_eq!(response.status(), StatusCode::ACCEPTED);
        let body = body_json(response).await;
        assert_eq!(body["status"], "afk_run_started");
        assert_eq!(body["node_id"], node_id.as_str());

        // The run flips the node to the soft in_progress signal immediately…
        let node = crate::map::fetch_node(state.db.conn(), &node_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(node.state, "in_progress");

        // …and when the unattended run finishes, Dearborn records the report
        // as the node's gist and settles it. `resolved_by` stays NULL: an
        // agent, not a human, resolved it.
        let node = wait_for_node_state(&state, &node_id, "resolved").await;
        assert_eq!(
            node.gist.as_deref(),
            Some("libsql supports BLOB. Source: evidence.rs.")
        );
        assert_eq!(node.resolved_by, None);

        // No map mutation beyond the node's own resolution: same node count,
        // still zero edges.
        let map = crate::map::compute_map(state.db.conn(), &epic_id)
            .await
            .unwrap();
        assert_eq!(map.nodes.len(), 1);
        assert!(map.edges.is_empty());

        // AFK nodes never create a node_session row (migration 0015) — there
        // is nothing to resume.
        assert!(crate::node_engine::fetch_session(state.db.conn(), &node_id)
            .await
            .unwrap()
            .is_none());

        // Evidence: an agent_run row for the research stage with the session id.
        wait_until_unlocked(&state, &node_id).await;
        let runs = agent_run_rows(&state, &epic_id).await;
        assert_eq!(
            runs,
            vec![(
                "research".to_string(),
                "ok".to_string(),
                Some("afk-sess".to_string())
            )]
        );

        // The engine passed the research system prompt and a context-headed
        // prompt; the request shape carries no CLI wiring at all. (The guard
        // is dropped before the WS await below.)
        let (run_count, run_slot, run_prompt, run_system_prompt) = {
            let runs = recorded.lock().unwrap();
            (
                runs.len(),
                runs[0].slot,
                runs[0].prompt.clone(),
                runs[0].system_prompt.clone(),
            )
        };
        drop(recorded.lock().unwrap());
        assert_eq!(run_count, 1);
        assert_eq!(run_slot, AgentSlot::Research);
        assert_eq!(run_system_prompt, RESEARCH_PROMPT);
        assert!(run_prompt.contains("Which store?"));
        assert!(run_prompt.contains("Pick the blob store"));
        assert!(run_prompt.contains("It works end to end"));

        // The live run streamed to node:<id>, ending in exited.
        let frames = collect_until_exited(sub).await;
        let types: Vec<&str> = frames.iter().map(|f| f["type"].as_str().unwrap()).collect();
        assert!(types.contains(&"started"), "types: {types:?}");
        assert!(types.contains(&"text"), "types: {types:?}");
        assert!(types.last() == Some(&"exited"), "types: {types:?}");
    }

    // ---- AC: an AFK-task node fires on its own slot --------------------------

    #[tokio::test]
    async fn an_afk_task_node_fires_on_the_afk_task_slot() {
        let agent = Arc::new(ScriptedAfkAgent::new(
            "afk-task-sess",
            &["Bucket provisioned."],
        ));
        let recorded = agent.recorded();
        let (state, app) = boot(agent).await;
        let token = login(&state).await;
        let epic_id = seed_epic(&state).await;
        let node_id = seed_node(&state, &epic_id, "task", Some("afk")).await;

        let response = fire(&app, &token, &epic_id, &node_id).await;
        assert_eq!(response.status(), StatusCode::ACCEPTED);

        let node = wait_for_node_state(&state, &node_id, "resolved").await;
        assert_eq!(node.gist.as_deref(), Some("Bucket provisioned."));

        let runs = recorded.lock().unwrap();
        assert_eq!(runs.len(), 1);
        assert_eq!(runs[0].slot, AgentSlot::AfkTask);
        assert_eq!(runs[0].system_prompt, AFK_TASK_PROMPT);
    }

    // ---- AC: multiple research nodes fire concurrently ----------------------

    #[tokio::test]
    async fn multiple_research_nodes_fire_concurrently() {
        let gate = Arc::new(AfkGate::default());
        let agent = Arc::new(ScriptedAfkAgent::new("afk-par", &["fact"]).with_gate(gate.clone()));
        let (state, app) = boot(agent).await;
        let token = login(&state).await;
        let epic_id = seed_epic(&state).await;
        let node_a = seed_node(&state, &epic_id, "research", None).await;
        let node_b = seed_node(&state, &epic_id, "research", None).await;
        let node_c = seed_node(&state, &epic_id, "research", None).await;

        // Fire three research nodes; each claims its own per-node lock and
        // holds its run in flight (gated).
        for node in [&node_a, &node_b, &node_c] {
            let response = fire(&app, &token, &epic_id, node).await;
            assert_eq!(response.status(), StatusCode::ACCEPTED);
        }
        assert!(node_locked(&state, &node_a));
        assert!(node_locked(&state, &node_b));
        assert!(node_locked(&state, &node_c));

        // A fire for an already-running node is refused (per-node lock).
        let response = fire(&app, &token, &epic_id, &node_a).await;
        assert_eq!(response.status(), StatusCode::CONFLICT);

        // Release everything; all three settle independently and in parallel.
        gate.release();
        for node in [&node_a, &node_b, &node_c] {
            wait_until_unlocked(&state, node).await;
            let node_row = wait_for_node_state(&state, node, "resolved").await;
            assert_eq!(node_row.gist.as_deref(), Some("fact"));
        }
        assert_eq!(agent_run_rows(&state, &epic_id).await.len(), 3);
    }

    // ---- AC: AFK surface is fire-only — the other kinds get 409 -------------

    #[tokio::test]
    async fn non_afk_kinds_and_hitl_tasks_have_no_afk_run() {
        let agent = Arc::new(ScriptedAfkAgent::new("s", &["x"]));
        let (state, app) = boot(agent).await;
        let token = login(&state).await;
        let epic_id = seed_epic(&state).await;
        let grilling = seed_node(&state, &epic_id, "grilling", None).await;
        let prototype = seed_node(&state, &epic_id, "prototype", None).await;
        let hitl_task = seed_node(&state, &epic_id, "task", Some("hitl")).await;

        // Interactive kinds point at the session engine; a HITL task has no
        // engine at all.
        for node in [&grilling, &prototype, &hitl_task] {
            let response = fire(&app, &token, &epic_id, node).await;
            assert_eq!(response.status(), StatusCode::CONFLICT);
        }

        // Unknown node → 404.
        let response = fire(&app, &token, &epic_id, "nope").await;
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    // ---- AC: a settled node cannot be re-fired ------------------------------

    #[tokio::test]
    async fn a_settled_node_cannot_be_refired() {
        let agent = Arc::new(ScriptedAfkAgent::new("s", &["fact"]));
        let (state, app) = boot(agent).await;
        let token = login(&state).await;
        let epic_id = seed_epic(&state).await;
        let node_id = seed_node(&state, &epic_id, "research", None).await;

        let response = fire(&app, &token, &epic_id, &node_id).await;
        assert_eq!(response.status(), StatusCode::ACCEPTED);
        wait_for_node_state(&state, &node_id, "resolved").await;

        let response = fire(&app, &token, &epic_id, &node_id).await;
        assert_eq!(response.status(), StatusCode::CONFLICT);
    }

    // ---- AC: a failed run records error evidence and stays re-firable -------

    #[tokio::test]
    async fn a_failed_afk_run_leaves_the_node_in_progress_and_refirable() {
        let (state, app) = boot(Arc::new(FailingAfkAgent)).await;
        let token = login(&state).await;
        let epic_id = seed_epic(&state).await;
        let node_id = seed_node(&state, &epic_id, "research", None).await;

        let response = fire(&app, &token, &epic_id, &node_id).await;
        assert_eq!(response.status(), StatusCode::ACCEPTED);

        wait_until_unlocked(&state, &node_id).await;
        let node = crate::map::fetch_node(state.db.conn(), &node_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(node.state, "in_progress");
        assert_eq!(node.gist, None);

        let runs = agent_run_rows(&state, &epic_id).await;
        assert_eq!(runs.len(), 1);
        assert_eq!(runs[0].1, "error");

        // The failed run can simply be re-fired: boot a router whose agent
        // seam is a scripted success, sharing the same in-memory DB (the
        // bearer token is stateless, so it still authenticates).
        let retry_app = crate::app(
            AppState::new(Config::for_test(), state.db.clone())
                .with_afk(Arc::new(ScriptedAfkAgent::new("afk-retry", &["fact"]))),
        );
        let response = fire(&retry_app, &token, &epic_id, &node_id).await;
        assert_eq!(response.status(), StatusCode::ACCEPTED);
        let node = wait_for_node_state(&state, &node_id, "resolved").await;
        assert_eq!(node.gist.as_deref(), Some("fact"));
    }

    // ---- AC: the engine's write surface is Dearborn's, not the agent's ------

    #[test]
    fn the_afk_request_shape_carries_no_cli_or_capability_surface() {
        // Compile-time statement of the allow-list AC: an AFK run request has
        // no `cli` field and mints no capability token — the map-mutating
        // `dearborn` verbs are not in the run's allow-list because the run is
        // never granted the CLI. (Breakdown's request, by contrast, carries
        // `cli: Option<DearbornCli>`; the AFK request structurally cannot.)
        fn assert_no_cli_field(_req: &AfkRunRequest) {}
        let req = AfkRunRequest {
            run_id: "r".to_string(),
            prompt: "p".to_string(),
            cwd: None,
            system_prompt: "s".to_string(),
            slot: AgentSlot::Research,
            harness: "claude".to_string(),
            model: None,
        };
        assert_no_cli_field(&req);
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
}
