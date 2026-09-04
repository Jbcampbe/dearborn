//! One-shot breakdown agent — epic → task DAG (T-301).
//!
//! A **human-approved** epic that is ready to be broken into work gets
//! `POST /epics/:id/breakdown`, which runs a **single, non-interactive** agent
//! that reads the epic's plan and creates a graph of tasks (thin vertical
//! slices / tracer bullets, per `references/prompts/to-tasks.md`) via the
//! harness-agnostic `dearborn` CLI's `task create` / `task link` verbs,
//! authenticated by a per-run capability token ([`crate::capability`]). When the
//! run finishes, Dearborn moves the epic **Planning → Ready** and records the
//! run in `agent_run`.
//!
//! ## What the agent reads
//!
//! In the wayfinder epic the plan source is the **living Document** the map
//! produces — the settled-decisions spec, never any chat transcript. The
//! context-fed plan assembly ([`build_plan`]) folds in the Document's current
//! HTML alongside the epic's title and destination.
//!
//! ## The completion gate
//!
//! Breakdown is offered only when **the way is clear** (wayfinder epic §8):
//! no open (or in-progress) map nodes remain AND the fog
//! (`epic.not_yet_specified`) is empty — the same computed eligibility the map
//! itself exposes ([`crate::map::MapCompletion`]). The trigger is **human-only**:
//! the route requires a signed-in user (a capability token is `403`ed by the
//! allow-list before the handler runs, and the handler re-checks), preserving
//! today's approve gate.
//!
//! ## Relation to the node engines
//!
//! This mirrors [`crate::planning`]'s interactive-agent machinery — the agent
//! sits behind a [`BreakdownAgent`] trait (production [`ClaudeBreakdownAgent`];
//! tests inject a scripted fake), the blocking `RunEvent` receiver is drained on
//! `spawn_blocking` and every event is relayed live to `epic:<id>` (reusing
//! [`crate::planning::ws_type`]) — but the run is **one-shot**: no `resume`, no
//! multi-turn. Its durable output is the task rows + edges the CLI's REST verbs
//! persist, plus the `agent_run` evidence row and the `epic.status='Ready'`
//! transition.
//!
//! ## Determinism boundary
//!
//! The agent only ever creates tasks and links dependencies (its surface is
//! the scoped `dearborn` CLI, whose token grants exactly the two REST routes
//! those verbs call). Dearborn — not the agent — owns the `Planning → Ready`
//! lane transition, exactly as ARCHITECTURE §11 requires.

use std::collections::HashMap;
use std::collections::HashSet;
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
use crate::epics::{fetch_epic, get_epic_clone_path, get_epic_project_id};
use crate::{AppError, AppResult, AppState, InflightGuard};

/// The system prompt that encodes the `to-tasks` vertical-slice breakdown logic.
/// The epic's plan is appended separately (see the module doc for what feeds it).
pub(crate) const BREAKDOWN_PROMPT: &str = "\
You are Dearborn's breakdown agent. You run ONCE (non-interactively) to convert an \
approved epic into an executable task DAG. The epic's plan is provided to you as the PRD.

Break the plan into TRACER-BULLET tasks: each task is a thin vertical slice that cuts \
through ALL integration layers end-to-end (schema, API, UI, tests), NOT a horizontal \
slice of a single layer. A completed slice must be demoable or verifiable on its own. \
Prefer many thin slices over few thick ones.

Create the tasks with the Dearborn CLI (access block below; run it through your \
shell tool):
- `task create --title \"...\" --description \"...\" --acceptance \"...\"`: create ONE \
task with a `title`, a `description` of the end-to-end behavior (not layer-by-layer), \
and `acceptance` criteria. Create blockers BEFORE the tasks that depend on them; \
when creating a task, you may pass `--blocks id1,id2` (ids of already-created tasks \
this new task blocks) or wire edges afterward with `task link`.
- `task link BLOCKER BLOCKED`: add a `BLOCKER → BLOCKED` edge (the blocker must \
finish first). Both tasks must belong to this epic; cycles are rejected. Use the \
EXACT task id strings that `task create` printed — copy them verbatim; never \
substitute your own numbering or labels, which will be rejected. If a link is \
rejected, read the error's list of valid task ids and retry with one of those.
- `dag`: print the current task DAG (use it to verify your work).

Every verb prints JSON on success and `dearborn: <error>` on failure.

Work in dependency order. Do not modify the codebase or change the epic's status — \
the ONLY shell commands you run are `dearborn` CLI calls; creating tasks and \
linking dependencies is your entire surface. When the DAG is complete, stop.";

// ---- the agent seam ------------------------------------------------------

/// A one-shot breakdown run, decoupled from the harness so tests inject a
/// scripted agent. Built by [`spawn_breakdown`] from the epic's context.
pub struct BreakdownRunRequest {
    /// Unique id for this run (a ULID); echoed on every `RunEvent`.
    pub run_id: String,
    /// The breakdown instruction (the user-visible "go" prompt).
    pub prompt: String,
    /// The epic's plan, appended as a system prompt (PRD).
    pub plan: String,
    /// Working directory: the project's read-only clone (code grounding). `None`
    /// when the clone isn't ready — the run proceeds without code context.
    pub cwd: Option<PathBuf>,
    /// Dearborn CLI wiring: the loopback base URL and the per-run capability
    /// token, injected as a system-prompt access block. `None` disables the
    /// CLI surface (used only when the clone/base URL is unavailable — the run
    /// then no-ops usefully).
    pub cli: Option<DearbornCli>,
    /// The breakdown instruction prompt — the slot's live-resolved effective
    /// text (T6): the project's override when set, else `BREAKDOWN_PROMPT`.
    pub system_prompt: String,
    /// The harness key this run was resolved to (T7). Validated with
    /// [`crate::agent_settings::harness_supports_slot`] by
    /// [`ClaudeBreakdownAgent::run`]; a harness this slot cannot run surfaces as an
    /// `Error` + `Exited` event stream (the trait has no `Result`).
    pub harness: String,
    /// The resolved model passed verbatim to the CLI; `None` → CLI default (T7).
    pub model: Option<String>,
}

/// The Dearborn CLI knobs [`spawn_breakdown`] hands the agent for the run.
pub struct DearbornCli {
    /// Dearborn's loopback origin (e.g. `http://127.0.0.1:8787`) — the CLI's `--url`.
    pub base_url: String,
    /// The per-run capability token — the CLI's `--token`.
    pub token: String,
}

/// The system-prompt access block injected when a run is wired to the
/// `dearborn` CLI: how to authenticate (the per-run `--url`/`--token` pair,
/// pre-scoped to this epic) and which verbs exist. The flags travel with every
/// command — each shell invocation is a fresh process, so an `export` would not
/// survive between the agent's tool calls.
fn cli_access_block(cli: &DearbornCli) -> String {
    format!(
        "\nDearborn CLI access — call it through your shell tool exactly as shaped below \
         (the `--url`/`--token` pair is already issued and scoped to THIS run; never \
         modify or omit either):\n\
         dearborn --url {url} --token {token} <verb>\n\
         where <verb> is one of:\n\
         - task create --title \"...\" [--description \"...\"] [--acceptance \"...\"] [--blocks id1,id2]\n\
         - task link BLOCKER BLOCKED\n\
         - dag\n\
         Each verb prints JSON on success and `dearborn: <error>` on failure. `task create`'s \
         JSON includes the new task's `id` — copy it verbatim into later edges.\n",
        url = cli.base_url,
        token = cli.token,
    )
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

        // T7 spawn-validation, mirroring the planning agent's check: a harness
        // this slot cannot run surfaces loudly through the same synthetic
        // Error+Exited stream a spawn failure uses. Breakdown builds the task
        // DAG through the scoped `dearborn` CLI, and its runs are driven by
        // the Claude Code adapter (see [`crate::agent_settings`]) — so a
        // non-Claude harness is refused here just like an unknown one.
        const SLOT: AgentSlot = AgentSlot::Breakdown;
        if !crate::agent_settings::harness_supports_slot(&req.harness, SLOT) {
            let (tx, rx) = std::sync::mpsc::channel();
            let _ = tx.send(RunEvent::Error {
                run_id: run_id.clone(),
                message: crate::agent_settings::unsupported_harness_message(&req.harness, SLOT),
            });
            let _ = tx.send(RunEvent::Exited {
                run_id,
                exit_code: None,
                cancelled: false,
            });
            return rx;
        }

        let mut extra_args = vec![
            "--append-system-prompt".to_string(),
            req.system_prompt.clone(),
            // The epic's plan, as the PRD to break down.
            "--append-system-prompt".to_string(),
            req.plan.clone(),
        ];
        if let Some(cli) = &req.cli {
            // The scoped `dearborn` CLI is this run's only write surface: the
            // access block tells the agent how to call it, and
            // `bypassPermissions` keeps the shell invocations from stalling on
            // approval prompts. Read-only w.r.t. the codebase is enforced by
            // the prompt steering + the read-only clone as `cwd`, not by the
            // mode.
            extra_args.push("--append-system-prompt".to_string());
            extra_args.push(cli_access_block(cli));
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
                // T7: the slot's resolved model rides through to the CLI.
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

/// `POST /epics/{id}/breakdown` — run the one-shot breakdown agent on a
/// completed planning epic, then move it Planning → Ready.
///
/// * `404` if the epic does not exist.
/// * `409` if the epic is not in `Planning`, or if a run is already in flight
///   for it, or if **the way is not yet clear** — open map nodes remain or the
///   fog (`not_yet_specified`) is non-empty. Breakdown consumes the settled
///   Document, so it is offered only on a completed map (wayfinder epic §8).
/// * `202 Accepted` once the background run is spawned (its events stream over
///   WS on `epic:<id>`; the DAG + lane change land when the run completes).
///
/// Human-only: a capability token never reaches this handler (the capability
/// allow-list `403`s the route), and [`crate::auth::CurrentUser`] re-checks
/// that the caller is a signed-in user — an unattended agent cannot trigger
/// breakdown.
pub async fn trigger_breakdown(
    State(state): State<AppState>,
    Path(id): Path<String>,
    _human: crate::auth::CurrentUser,
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

    // The completion gate (wayfinder epic §8): breakdown reads the settled
    // Document, so it is offered only when the map is complete — no open
    // (or in-progress) nodes remain and the fog is empty. The map computes
    // this on every read; the same eligibility the client surfaces as "the
    // way is clear — ready to break down" is what gates the trigger here.
    let map = crate::map::compute_map(conn, &id).await?;
    if !map.completion.eligible {
        return Err(AppError::Conflict(format!(
            "epic {id} is not ready to break down: the way is not yet clear — {} open map node(s) remain{}",
            map.completion.open_nodes,
            if map.completion.fog_remaining {
                " and the fog (not_yet_specified) is not empty"
            } else {
                ""
            }
        )));
    }

    // One run at a time per epic (shares the in-flight slot so two breakdown
    // runs never overlap on the same epic).
    let Some(guard) = state.try_acquire_run(&id) else {
        return Err(AppError::Conflict(format!(
            "a run is already in flight for epic {id}"
        )));
    };

    // The attribution feed row: a human pulled the trigger (wayfinder epic
    // §4.9 — see [`crate::activity`]).
    crate::activity::record(
        &conn,
        &id,
        None,
        Some(_human.0.sub.as_str()),
        crate::activity::BREAKDOWN_STARTED,
        None,
    )
    .await?;

    spawn_breakdown(state.clone(), id, guard);

    Ok((
        StatusCode::ACCEPTED,
        Json(json!({ "status": "breakdown_started" })),
    ))
}

// ---- run orchestration ---------------------------------------------------

/// Substring every `dearborn` CLI failure prints to stderr (`dearborn: <error>`;
/// see [`crate::cli`]). A failed tool call whose output carries it is a failed
/// DAG write, not harness-side noise: those are the calls the Planning → Ready
/// guard exists for. (Until the CLI retirement these were `mcp__dearborn__*`
/// tool names; shell-invoked CLI calls have no such name, so the *output*
/// marker is the signature instead.) Aliased from the CLI module's
/// [`crate::cli::ERROR_PREFIX`] so the two can never drift apart.
pub(crate) const DEARBORN_CLI_ERROR_MARKER: &str = crate::cli::ERROR_PREFIX;

/// What a drained breakdown run leaves behind, persisted after the stream ends.
#[derive(Default)]
struct BreakdownOutcome {
    /// Assembled assistant text (all `Text` deltas), stored as `agent_run.log`.
    log: String,
    /// The harness session id captured from `RunEvent::Session` (evidence).
    session_id: Option<String>,
    /// Tool calls started but not yet ended, by id — so a failed `ToolEnd` can
    /// be attributed to the tool that made it (`ToolEnd` carries only the id).
    pending_tools: HashMap<String, String>,
    /// Dearborn CLI calls whose run ended not-ok, in failure order, paired
    /// with whatever failure output the harness reported.
    failed_tool_calls: Vec<(String, String)>,
}

impl BreakdownOutcome {
    fn absorb(&mut self, event: &RunEvent) {
        match event {
            RunEvent::Text { delta, .. } => self.log.push_str(delta),
            RunEvent::Session {
                session_id: Some(id),
                ..
            } => self.session_id = Some(id.clone()),
            RunEvent::ToolStart {
                tool_call_id, name, ..
            } => {
                self.pending_tools
                    .insert(tool_call_id.clone(), name.clone());
            }
            RunEvent::ToolEnd {
                tool_call_id,
                ok: false,
                output,
                ..
            } => {
                // Only `dearborn` CLI failures count: those are the writes
                // behind the task DAG (identified by the CLI's stderr marker —
                // shell-invoked calls carry no Dearborn tool *name*). A failed
                // harness-side read (Read/Grep/…) can't leave the DAG
                // half-written, and treating it as fatal would block runs over
                // noise.
                if let Some(name) = self.pending_tools.remove(tool_call_id) {
                    let out = output.clone().unwrap_or_default();
                    if out.contains(DEARBORN_CLI_ERROR_MARKER) {
                        self.failed_tool_calls.push((name, out));
                    }
                }
            }
            RunEvent::ToolEnd { tool_call_id, .. } => {
                self.pending_tools.remove(tool_call_id);
            }
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

        // Build the plan (PRD) the breakdown agent reads.
        let plan = match build_plan(&state, &epic_id).await {
            Some(plan) => plan,
            None => {
                tracing::warn!(epic = %epic_id, "breakdown: epic vanished before run");
                return;
            }
        };

        // Mint a capability scoped to this (epic, project, clone) — the token
        // the agent's `dearborn` CLI calls authenticate with, and the only
        // write surface the run has. Held for the whole run; revoked on drop.
        // Falls back to a CLI-less run if the clone/base URL is unavailable.
        let mut cwd: Option<PathBuf> = None;
        let mut cli: Option<DearbornCli> = None;
        let mut _cap_guard: Option<crate::capability::CapabilityGuard> = None;

        let clone_path = get_epic_clone_path(conn, &epic_id).await.ok().flatten();
        let project_id = get_epic_project_id(conn, &epic_id)
            .await
            .ok()
            .flatten()
            .unwrap_or_default();

        // T6/T7: resolve the Breakdown slot's live config at spawn time — the
        // project's system-prompt override when set, else `BREAKDOWN_PROMPT`,
        // plus the effective harness/model (design §9: read fresh per run).
        let spawn_cfg = match crate::agent_settings::spawn_config(
            &state.db,
            &project_id,
            AgentSlot::Breakdown,
            BREAKDOWN_PROMPT,
        )
        .await
        {
            Ok(cfg) => cfg,
            Err(err) => {
                tracing::warn!(epic = %epic_id, error = %err, "failed to resolve breakdown agent settings; aborting run");
                return;
            }
        };
        match (clone_path, state.advertised_base()) {
            (Some(clone_path), Some(base)) => {
                let cap = state.caps.mint(
                    epic_id.clone(),
                    project_id.clone(),
                    "breakdown".to_string(),
                    PathBuf::from(&clone_path),
                );
                cwd = Some(PathBuf::from(&clone_path));
                cli = Some(DearbornCli {
                    base_url: base,
                    token: cap.token().to_string(),
                });
                _cap_guard = Some(cap);
            }
            _ => {
                tracing::debug!(epic = %epic_id, "breakdown: no ready clone or base URL; running without the dearborn CLI");
            }
        }

        let req = BreakdownRunRequest {
            run_id: ulid::Ulid::new().to_string(),
            prompt: "Break this epic down into a task DAG using the dearborn CLI.".to_string(),
            plan,
            cwd,
            cli,
            system_prompt: spawn_cfg.prompt,
            harness: spawn_cfg.harness.clone(),
            model: spawn_cfg.model.clone(),
        };

        let outcome = {
            // Snapshot the epic's pre-existing task ids so a failed run can
            // roll back exactly what IT created (see below) without touching
            // tasks from earlier runs or manual edits.
            let pre_existing: HashSet<String> = crate::tasks::list_tasks_for_epic(conn, &epic_id)
                .await
                .unwrap_or_default()
                .into_iter()
                .map(|t| t.id)
                .collect();

            let rx = state.breakdown.run(req);
            let hub = state.hub.clone();
            let topic = format!("epic:{epic_id}");
            let epic_for_drain = epic_id.clone();
            let conn_for_rollback = conn.clone();

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

            let outcome = match drained {
                Ok(outcome) => outcome,
                Err(_) => return, // blocking task panicked; nothing reliable to persist
            };

            // Roll back the failed run's PARTIAL DAG writes (see
            // `rollback_partial_dag`).
            if !outcome.failed_tool_calls.is_empty() {
                rollback_partial_dag(&conn_for_rollback, &epic_for_drain, &pre_existing).await;
            }

            outcome
        };

        // Agent has exited; the capability token is revoked when `_cap_guard`
        // drops at the end of this task.

        // A `dearborn` CLI call failing means part of the task DAG may be
        // missing or half-wired — even though the model's closing summary will
        // happily claim success. (This exact shape happened for real: an
        // external DB lock made every task-create fail mid-run, the model
        // declared "All 12 tasks created" anyway, and only the run's *final*
        // writes — this agent_run row plus the Planning → Ready transition —
        // landed after the lock cleared. The result was a Ready epic with zero
        // tasks that nothing would ever retry.) So: when a scoped CLI call
        // failed, record the run as `error`, append why to its log, and leave
        // the epic in `Planning` — where POST /epics/{id}/breakdown can simply
        // re-run it.
        let dag_write_failed = !outcome.failed_tool_calls.is_empty();
        if dag_write_failed {
            tracing::error!(
                epic = %epic_id,
                failed_tools = ?outcome.failed_tool_calls,
                "breakdown: dearborn CLI call(s) failed; refusing Planning → Ready"
            );
        }

        // Record per-run evidence (the tasks/edges were persisted live by the
        // CLI's REST verbs during the run), including which agent settings produced
        // it (T8).
        let mut log = outcome.log;
        if dag_write_failed {
            let failed = outcome
                .failed_tool_calls
                .iter()
                .map(|(tool, output)| {
                    if output.is_empty() {
                        tool.clone()
                    } else {
                        format!("{tool} ({output})")
                    }
                })
                .collect::<Vec<_>>()
                .join(", ");
            log.push_str(&format!(
                "\n\n[dearborn] breakdown aborted: {} dearborn CLI call(s) failed ({}) — \
                 partial tasks created by this run were rolled back and the epic stays in \
                 `Planning` so breakdown can be re-run.",
                outcome.failed_tool_calls.len(),
                failed
            ));
        }

        let run_id = ulid::Ulid::new().to_string();
        let status = if dag_write_failed { "error" } else { "ok" };
        let _ = conn
            .execute(
                "INSERT INTO agent_run (id, task_id, epic_id, stage, session_id, log, created_at, \
                 attempt, status, verdict, started_at, ended_at, exit_code, \
                 harness, model, prompt_hash) \
                 VALUES (?1, NULL, ?2, 'breakdown', ?3, ?4, ?5, 1, ?6, NULL, ?5, ?5, NULL, \
                 ?7, ?8, ?9)",
                params![
                    run_id,
                    epic_id.clone(),
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

        // Dearborn owns the lane transition: Planning → Ready — but only when
        // the whole tool surface came back clean (see above). The publish block
        // below still runs either way so a subscribed client re-renders.
        if !dag_write_failed {
            let _ = conn
                .execute(
                    "UPDATE epic SET status = 'Ready', updated_at = ?1 WHERE id = ?2 AND status = 'Planning'",
                    params![now_ms(), epic_id.clone()],
                )
                .await;
        }

        // Publish the final DAG and the updated epic so the client re-renders.
        crate::capability::publish_dag(&state, &epic_id).await;
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

/// Roll back a failed breakdown run's partial DAG writes. A failed scoped tool
/// call means the task graph is incomplete, and a re-run would recreate every
/// slice — leaving duplicates if the partial tasks stayed. Deleting exactly the
/// ids this run added (`pre_existing` minus current) makes breakdown
/// idempotently re-runnable while preserving pre-existing tasks and edges.
async fn rollback_partial_dag(
    conn: &libsql::Connection,
    epic_id: &str,
    pre_existing: &HashSet<String>,
) {
    match crate::tasks::list_tasks_for_epic(conn, epic_id).await {
        Ok(current) => {
            for task in current {
                if !pre_existing.contains(&task.id) {
                    if let Err(err) = crate::tasks::delete_task(conn, &task.id).await {
                        tracing::warn!(epic = %epic_id, task = %task.id, error = %err,
                            "breakdown rollback: failed to delete partial task");
                    }
                }
            }
        }
        Err(err) => tracing::warn!(epic = %epic_id, error = %err,
            "breakdown rollback: could not list tasks; partial DAG may remain"),
    }
}

/// Assemble the plan (PRD) an epic hands the breakdown agent: the epic's
/// title + destination, then the **settled living Document** — the map's
/// source of truth for its prose (wayfinder epic §8: breakdown reads the
/// Document, *not* any node transcript or other session artifact).
/// `None` if the epic no longer exists.
async fn build_plan(state: &AppState, epic_id: &str) -> Option<String> {
    let conn = state.db.conn();
    let epic = fetch_epic(conn, epic_id).await.ok().flatten()?;
    let document = crate::document::fetch_document(conn, epic_id)
        .await
        .ok()
        .flatten();

    let mut plan = format!(
        "The epic to break down is titled \"{title}\".",
        title = epic.title,
    );
    if let Some(destination) = epic.destination.as_deref().filter(|d| !d.trim().is_empty()) {
        plan.push_str(&format!(
            "\nIts destination — what the finished plan looks like — is: {destination}"
        ));
    }
    match document {
        Some(document) => plan.push_str(&format!(
            "\n\nThe settled plan follows as HTML (version v{:0>2} of the epic's living \
             Document — the decisions the planning map resolved). Break the plan it \
             describes into tasks:\n{}",
            document.version, document.html,
        )),
        None => plan.push_str(
            "\n\nThe epic's living Document is empty — no sections were written \
             during planning. Plan from the destination alone.",
        ),
    }
    Some(plan)
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
        pub had_cli: bool,
    }

    /// A [`BreakdownAgent`] that emits a failed harness-side read AND a failed
    /// `dearborn` CLI call, then a confident closing summary and a clean exit —
    /// the exact "model claims success, writes failed" shape the Planning →
    /// Ready guard exists for.
    pub struct FailedToolCallBreakdownAgent;

    impl BreakdownAgent for FailedToolCallBreakdownAgent {
        fn run(&self, req: BreakdownRunRequest) -> Receiver<RunEvent> {
            let (tx, rx) = std::sync::mpsc::channel();
            let run_id = req.run_id;
            std::thread::spawn(move || {
                let _ = tx.send(RunEvent::Started {
                    run_id: run_id.clone(),
                });
                let _ = tx.send(RunEvent::Session {
                    run_id: run_id.clone(),
                    session_id: Some("bd-fail".to_string()),
                    model: Some("fake-model".to_string()),
                });
                // A failed harness-side read: must NOT trip the guard.
                let _ = tx.send(RunEvent::ToolStart {
                    run_id: run_id.clone(),
                    tool_call_id: "t1".to_string(),
                    name: "Read".to_string(),
                    input: None,
                    tool_kind: harness::ToolKind::Read,
                });
                let _ = tx.send(RunEvent::ToolEnd {
                    run_id: run_id.clone(),
                    tool_call_id: "t1".to_string(),
                    ok: false,
                    output: None,
                });
                // The failed DAG write: must trip the guard. Shell-invoked
                // CLI calls surface as Bash with the CLI's stderr marker.
                let _ = tx.send(RunEvent::ToolStart {
                    run_id: run_id.clone(),
                    tool_call_id: "t2".to_string(),
                    name: "Bash".to_string(),
                    input: None,
                    tool_kind: harness::ToolKind::Other,
                });
                let _ = tx.send(RunEvent::ToolEnd {
                    run_id: run_id.clone(),
                    tool_call_id: "t2".to_string(),
                    ok: false,
                    output: Some("dearborn: failed to create task: database is locked".to_string()),
                });
                // The model's (untrustworthy) closing summary.
                let _ = tx.send(RunEvent::Text {
                    run_id: run_id.clone(),
                    delta: "All 12 tasks created.".to_string(),
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

    /// A scripted [`BreakdownAgent`] that, per run, invokes a caller-supplied
    /// closure (to drive the `dearborn` CLI like a real agent would) and then emits
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
                had_cli: req.cli.is_some(),
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

    /// A [`BreakdownAgent`] that creates a small fixed DAG by calling Dearborn
    /// through the `dearborn` CLI surface (proving the end-to-end seam), driven
    /// by a callback the test supplies. Kept minimal: it just emits a terminal
    /// stream; DAG creation is done by the test directly against the
    /// store/endpoint so the engine's completion path (Ready transition,
    /// agent_run, publishes) is exercised.
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

    /// The bearer credential HTTP tests present, minted **once per process**
    /// for a fixed user id ([`TEST_USER_ID`]) — the replacement for the deleted
    /// static `TOKEN` constant. Access-token verification is stateless (one
    /// HMAC check against the fixed test master key, no database read), so a
    /// token minted here authenticates against every in-memory instance these
    /// tests boot; each instance seeds the named user row (see
    /// [`seed_bearer_user`]) so rows attributed to the actor — like the map
    /// node that epic creation seeds — satisfy their foreign keys.
    fn auth_bearer() -> &'static str {
        static BEARER: std::sync::OnceLock<String> = std::sync::OnceLock::new();
        BEARER.get_or_init(|| {
            let key = crate::auth::AuthKey::derive(&Config::for_test().master_key).unwrap();
            key.mint(&crate::auth::Claims {
                sub: TEST_USER_ID.to_string(),
                sid: TEST_SESSION_ID.to_string(),
                role: crate::users::Role::Admin,
                exp: now_ms() + 3_600_000,
            })
        })
    }

    /// The fixed user id the shared bearer token names (seeded into every test
    /// app by [`seed_bearer_user`]).
    const TEST_USER_ID: &str = "01TESTUSER0000000000000000";
    /// The fixed session id inside the shared bearer token (verification is
    /// stateless — it is never looked up).
    const TEST_SESSION_ID: &str = "01TESTSESSION00000000000000";

    /// Seed the shared bearer user directly (raw SQL: `users::create` mints a
    /// fresh ULID, and the token's `sub` must match the row it attributes).
    async fn seed_bearer_user(state: &AppState) {
        state
            .db
            .conn()
            .execute(
                "INSERT INTO user (id, username, display_name, password_hash, role, active, \
                 created_at, updated_at) \
                 VALUES (?1, 'tester', 'Tester', 'test-fixture-not-a-login', 'admin', 1, ?2, ?2)",
                libsql::params![TEST_USER_ID, now_ms()],
            )
            .await
            .unwrap();
    }

    fn req(method: &str, uri: &str, body: Option<Value>) -> Request<Body> {
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
    async fn app_with_breakdown(breakdown: Arc<dyn BreakdownAgent>) -> (AppState, axum::Router) {
        let db = Db::connect(":memory:").await.unwrap();
        db.run_migrations().await.unwrap();
        let state = AppState::with_agents(
            Config::for_test(),
            db,
            Arc::new(SilentPlanningAgent),
            breakdown,
        );
        seed_bearer_user(&state).await;
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
                Some(json!({ "title": "E", "destination": "It works end to end" })),
            ))
            .await
            .unwrap();
        assert_eq!(created.status(), StatusCode::CREATED);
        body_json(created).await["id"].as_str().unwrap().to_string()
    }

    async fn trigger(app: &axum::Router, epic_id: &str) -> axum::response::Response {
        app.clone()
            .oneshot(req("POST", &format!("/epics/{epic_id}/breakdown"), None))
            .await
            .unwrap()
    }

    /// Drive the epic's map to completion eligibility the same way the
    /// planning flow does: resolve every open/in-progress node (settling the
    /// seed grilling node the create flow seeds) and clear the fog. Both a
    /// 1-node epic and a large multi-layer epic complete through this
    /// identical ceremony — there is no shortcut.
    async fn complete_map(app: &axum::Router, epic_id: &str) {
        let map = body_json(
            app.clone()
                .oneshot(req("GET", &format!("/epics/{epic_id}/map"), None))
                .await
                .unwrap(),
        )
        .await;
        for node in map["nodes"].as_array().unwrap() {
            if node["state"] == "open" || node["state"] == "in_progress" {
                let r = app
                    .clone()
                    .oneshot(req(
                        "PATCH",
                        &format!(
                            "/epics/{epic_id}/map-nodes/{}",
                            node["id"].as_str().unwrap()
                        ),
                        Some(json!({"state": "resolved"})),
                    ))
                    .await
                    .unwrap();
                assert_eq!(r.status(), StatusCode::OK);
            }
        }
        let r = app
            .clone()
            .oneshot(req(
                "PATCH",
                &format!("/epics/{epic_id}/map"),
                Some(json!({"not_yet_specified": ""})),
            ))
            .await
            .unwrap();
        assert_eq!(r.status(), StatusCode::OK);
    }

    /// Sync an HTML document onto the epic (the scratch round-trip's write),
    /// as a grilling resolution would. Returns the new version.
    async fn sync_document(app: &axum::Router, epic_id: &str, html: &str) -> i64 {
        let r = app
            .clone()
            .oneshot(req(
                "POST",
                &format!("/epics/{epic_id}/document/sync"),
                Some(json!({"html": html, "base_version": 0})),
            ))
            .await
            .unwrap();
        assert_eq!(r.status(), StatusCode::OK);
        body_json(r).await["version"].as_i64().unwrap()
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
        let agent = Arc::new(ScriptedBreakdownAgent::new(
            "bd-sess",
            &["Breaking ", "down…"],
        ));
        let recorded = agent.recorded();
        let (state, app) = app_with_breakdown(agent).await;
        let project_id = seed_project(&app).await;
        let epic_id = create_epic(&app, &project_id).await;
        complete_map(&app, &epic_id).await;
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
        assert_eq!(
            row.get::<Option<String>>(1).unwrap().as_deref(),
            Some("bd-sess")
        );

        // The engine received the plan built from the settled Document: the
        // epic's title + destination, then the living Document's HTML (an
        // empty-Document note here — nothing was synced in this test).
        let runs = recorded.lock().unwrap();
        assert_eq!(runs.len(), 1);
        assert!(runs[0].plan.contains("titled \"E\""));
        assert!(runs[0].plan.contains("It works end to end"));
        assert!(runs[0].plan.contains("living Document is empty"));
    }

    // ---- the completion gate (wayfinder epic §8) --------------------------

    /// Breakdown is offered only when the way is clear: an open map node or
    /// non-empty fog is a `409` naming what remains; settling every node and
    /// clearing the fog lets the identical trigger through.
    #[tokio::test]
    async fn breakdown_blocked_until_no_open_nodes_remain_and_the_fog_is_empty() {
        let (state, app) = app_with_breakdown(Arc::new(SilentBreakdownAgent)).await;
        let project_id = seed_project(&app).await;
        let epic_id = create_epic(&app, &project_id).await;

        // The seed grilling node is still open → 409, naming the blocker.
        let response = trigger(&app, &epic_id).await;
        assert_eq!(response.status(), StatusCode::CONFLICT);
        let message = body_json(response).await["error"]["message"]
            .as_str()
            .unwrap()
            .to_string();
        assert!(message.contains("1 open map node"), "{message}");

        // Fog blocks too, even before the node settles.
        let r = app
            .clone()
            .oneshot(req(
                "PATCH",
                &format!("/epics/{epic_id}/map"),
                Some(json!({"not_yet_specified": "which blob store?"})),
            ))
            .await
            .unwrap();
        assert_eq!(r.status(), StatusCode::OK);
        let response = trigger(&app, &epic_id).await;
        assert_eq!(response.status(), StatusCode::CONFLICT);
        let message = body_json(response).await["error"]["message"]
            .as_str()
            .unwrap()
            .to_string();
        assert!(message.contains("not_yet_specified"), "{message}");

        // Resolving the seed node alone is not enough while fog remains.
        let map = body_json(
            app.clone()
                .oneshot(req("GET", &format!("/epics/{epic_id}/map"), None))
                .await
                .unwrap(),
        )
        .await;
        let seed_id = map["nodes"][0]["id"].as_str().unwrap().to_string();
        let r = app
            .clone()
            .oneshot(req(
                "PATCH",
                &format!("/epics/{epic_id}/map-nodes/{seed_id}"),
                Some(json!({"state": "resolved"})),
            ))
            .await
            .unwrap();
        assert_eq!(r.status(), StatusCode::OK);
        assert_eq!(trigger(&app, &epic_id).await.status(), StatusCode::CONFLICT);

        // Clearing the fog completes the map; the same trigger goes through.
        let r = app
            .clone()
            .oneshot(req(
                "PATCH",
                &format!("/epics/{epic_id}/map"),
                Some(json!({"not_yet_specified": ""})),
            ))
            .await
            .unwrap();
        assert_eq!(r.status(), StatusCode::OK);
        let response = trigger(&app, &epic_id).await;
        assert_eq!(response.status(), StatusCode::ACCEPTED);
        assert_eq!(wait_for_status(&state, &epic_id, "Ready").await, "Ready");
    }

    /// A 1-node epic and a large multi-layer epic complete through the
    /// identical flow — the same trigger, the same gate, the same Planning →
    /// Ready transition — with no special-casing for either size.
    #[tokio::test]
    async fn one_node_and_multi_layer_epics_complete_through_the_identical_flow() {
        let (state, app) = app_with_breakdown(Arc::new(SilentBreakdownAgent)).await;
        let project_id = seed_project(&app).await;

        // 1-node epic: the seeded grilling node, resolved, nothing else.
        let small = create_epic(&app, &project_id).await;
        complete_map(&app, &small).await;
        assert_eq!(trigger(&app, &small).await.status(), StatusCode::ACCEPTED);
        assert_eq!(wait_for_status(&state, &small, "Ready").await, "Ready");

        // Multi-layer epic: seed → two children → a grandchild, all settled
        // in dependency order through the same node PATCH surface.
        let big = create_epic(&app, &project_id).await;
        let map = body_json(
            app.clone()
                .oneshot(req("GET", &format!("/epics/{big}/map"), None))
                .await
                .unwrap(),
        )
        .await;
        let seed = map["nodes"][0]["id"].as_str().unwrap().to_string();
        // Two children of the seed, then a grandchild blocked by both —
        // created sequentially so each `blocked_by` names existing ids.
        let mut ids = vec![seed.clone()];
        for i in 0..3 {
            let parents: Vec<String> = match i {
                0 | 1 => vec![seed.clone()],
                _ => vec![ids[1].clone(), ids[2].clone()],
            };
            let r = app
                .clone()
                .oneshot(req(
                    "POST",
                    &format!("/epics/{big}/map-nodes"),
                    Some(json!({
                        "kind": "grilling",
                        "title": format!("Layer node {i}"),
                        "blocked_by": parents,
                    })),
                ))
                .await
                .unwrap();
            assert_eq!(r.status(), StatusCode::CREATED);
            ids.push(body_json(r).await["id"].as_str().unwrap().to_string());
        }
        // The grandchild is blocked until its parents settle.
        let r = app
            .clone()
            .oneshot(req(
                "PATCH",
                &format!("/epics/{big}/map-nodes/{}", ids[3]),
                Some(json!({"state": "resolved"})),
            ))
            .await
            .unwrap();
        assert_eq!(r.status(), StatusCode::OK);
        complete_map(&app, &big).await;
        assert_eq!(trigger(&app, &big).await.status(), StatusCode::ACCEPTED);
        assert_eq!(wait_for_status(&state, &big, "Ready").await, "Ready");
    }

    /// Breakdown consumes the settled Document, not any transcript: the plan
    /// handed to the engine carries the Document's current HTML (plus the
    /// title + destination framing).
    #[tokio::test]
    async fn breakdown_plan_carries_the_document() {
        let agent = Arc::new(ScriptedBreakdownAgent::new("bd-doc", &[]));
        let recorded = agent.recorded();
        let (state, app) = app_with_breakdown(agent).await;
        let project_id = seed_project(&app).await;
        let epic_id = create_epic(&app, &project_id).await;

        sync_document(
            &app,
            &epic_id,
            "<h1 id='decisions'>Decisions</h1><p id='store'>Use the evidence blob store.</p>",
        )
        .await;
        complete_map(&app, &epic_id).await;

        assert_eq!(trigger(&app, &epic_id).await.status(), StatusCode::ACCEPTED);
        assert_eq!(wait_for_status(&state, &epic_id, "Ready").await, "Ready");

        let runs = recorded.lock().unwrap();
        assert_eq!(runs.len(), 1);
        assert!(runs[0].plan.contains("titled \"E\""));
        assert!(runs[0].plan.contains("It works end to end"));
        assert!(runs[0].plan.contains("version v01"));
        assert!(runs[0].plan.contains("Use the evidence blob store."));
    }

    /// Only a human can trigger breakdown: a capability token — the agent
    /// runs' credential — is `403`ed by the allow-list before the handler.
    #[tokio::test]
    async fn breakdown_cannot_be_triggered_by_a_capability_token() {
        let (state, app) = app_with_breakdown(Arc::new(SilentBreakdownAgent)).await;
        let project_id = seed_project(&app).await;
        let epic_id = create_epic(&app, &project_id).await;
        complete_map(&app, &epic_id).await;

        let cap = state.caps.mint(
            epic_id.clone(),
            project_id.clone(),
            "grilling".to_string(),
            std::path::PathBuf::from("/tmp"),
        );
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(format!("/epics/{epic_id}/breakdown"))
                    .header(AUTHORIZATION, format!("Bearer {}", cap.token()))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::FORBIDDEN);

        // The epic never moved, and no breakdown run is on record.
        let epic = fetch_epic(state.db.conn(), &epic_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(epic.status, "Planning");
    }

    /// A dearborn CLI call failing must abort the run: the epic
    /// stays in `Planning` (re-triggerable), the evidence row closes as
    /// `error`, and the failure note names the failed tool. A failed
    /// harness-side read alone does NOT abort (the first scripted tool).
    #[tokio::test]
    async fn breakdown_stays_in_planning_when_a_dearborn_tool_call_fails() {
        let (state, app) = app_with_breakdown(Arc::new(FailedToolCallBreakdownAgent)).await;
        let project_id = seed_project(&app).await;
        let epic_id = create_epic(&app, &project_id).await;
        complete_map(&app, &epic_id).await;
        let response = trigger(&app, &epic_id).await;
        assert_eq!(response.status(), StatusCode::ACCEPTED);

        // Wait until the evidence row lands (the run is backgrounded).
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        loop {
            let mut rows = state
                .db
                .conn()
                .query(
                    "SELECT status, log FROM agent_run WHERE epic_id = ?1",
                    params![epic_id.clone()],
                )
                .await
                .unwrap();
            if let Some(row) = rows.next().await.unwrap() {
                // The run is recorded as an error, with the failed tool named
                // in its log.
                assert_eq!(row.get::<String>(0).unwrap(), "error");
                let log: String = row.get(1).unwrap();
                assert!(log.contains("dearborn: failed to create task"));
                assert!(log.contains("database is locked"));
                break;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "agent_run never landed"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        // The epic never left Planning — breakdown is simply re-runnable.
        let epic = fetch_epic(state.db.conn(), &epic_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(epic.status, "Planning");
    }

    /// A failed run's partial writes are rolled back: tasks created DURING the
    /// run (ids absent from the pre-run snapshot) disappear along with their
    /// edges, while pre-existing tasks — and the edges between them — survive.
    #[tokio::test]
    async fn rollback_partial_dag_deletes_only_tasks_created_by_the_failed_run() {
        let (state, app) = app_with_breakdown(Arc::new(SilentBreakdownAgent)).await;
        let project_id = seed_project(&app).await;
        let epic_id = create_epic(&app, &project_id).await;
        let conn = state.db.conn();

        // Pre-existing state: one task from "before" plus an edge between two
        // such tasks.
        let old_a = crate::tasks::create_task(conn, &epic_id, &project_id, "old A", None, None)
            .await
            .unwrap();
        let old_b = crate::tasks::create_task(conn, &epic_id, &project_id, "old B", None, None)
            .await
            .unwrap();
        crate::tasks::link_dependency(conn, &old_a.id, &old_b.id)
            .await
            .unwrap();

        // What a failed run added before tripping the guard.
        let partial = crate::tasks::create_task(conn, &epic_id, &project_id, "partial", None, None)
            .await
            .unwrap();
        crate::tasks::link_dependency(conn, &partial.id, &old_b.id)
            .await
            .unwrap();

        let pre_existing: HashSet<String> =
            [old_a.id.clone(), old_b.id.clone()].into_iter().collect();
        rollback_partial_dag(conn, &epic_id, &pre_existing).await;

        let remaining = crate::tasks::list_tasks_for_epic(conn, &epic_id)
            .await
            .unwrap();
        let ids: Vec<&str> = remaining.iter().map(|t| t.id.as_str()).collect();
        assert_eq!(ids, vec![old_a.id.as_str(), old_b.id.as_str()]);

        // The partial task's edge is gone; the old edge survived.
        let edges = crate::tasks::list_dependencies_for_epic(conn, &epic_id)
            .await
            .unwrap();
        assert_eq!(edges.len(), 1);
        assert_eq!(edges[0].blocker_id, old_a.id);
    }

    #[tokio::test]
    async fn breakdown_rejected_when_not_planning() {
        let (state, app) = app_with_breakdown(Arc::new(SilentBreakdownAgent)).await;
        let project_id = seed_project(&app).await;
        let epic_id = create_epic(&app, &project_id).await;
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
            let state = AppState::new(Config::for_test(), db);
            let app = app(state.clone());
            (state, app)
        };
        let project_id = seed_project(&app).await;
        let epic_id = create_epic(&app, &project_id).await;
        complete_map(&app, &epic_id).await;
        let response = trigger(&app, &epic_id).await;
        assert_eq!(response.status(), StatusCode::ACCEPTED);
        assert_eq!(wait_for_status(&state, &epic_id, "Ready").await, "Ready");
    }

    // ---- T7: unsupported-harness spawn validation -------------------------

    #[test]
    fn breakdown_agent_rejects_an_unsupported_harness_loudly() {
        let agent = ClaudeBreakdownAgent::new();
        let rx = agent.run(BreakdownRunRequest {
            run_id: "run-codex".to_string(),
            prompt: "break it down".to_string(),
            plan: "plan".to_string(),
            cwd: None,
            cli: None,
            system_prompt: BREAKDOWN_PROMPT.to_string(),
            harness: "codex".to_string(),
            model: None,
        });

        let first = rx.iter().next().expect("an Error event must arrive");
        match first {
            RunEvent::Error { message, .. } => {
                assert!(
                    message.contains("codex"),
                    "error names the harness: {message}"
                );
                assert!(message.contains("unsupported"), "error says why: {message}");
            }
            other => panic!("expected RunEvent::Error, got {other:?}"),
        }
        let exited = rx.iter().find(|e| matches!(e, RunEvent::Exited { .. }));
        assert!(exited.is_some(), "stream must end with Exited");
    }

    #[test]
    fn breakdown_agent_rejects_a_spawnable_but_non_claude_harness() {
        // pi runs task stages fine, but the breakdown engine is currently
        // wired to the Claude Code adapter only.
        let agent = ClaudeBreakdownAgent::new();
        let rx = agent.run(BreakdownRunRequest {
            run_id: "run-pi".to_string(),
            prompt: "break it down".to_string(),
            plan: "plan".to_string(),
            cwd: None,
            cli: None,
            system_prompt: BREAKDOWN_PROMPT.to_string(),
            harness: crate::harness_pi::PI_HARNESS_ID.to_string(),
            model: None,
        });

        match rx.iter().next().expect("an Error event must arrive") {
            RunEvent::Error { message, .. } => {
                assert!(message.contains("pi"), "error names the harness: {message}");
                assert!(
                    message.contains("Claude Code"),
                    "error says why: {message}"
                );
                assert!(
                    message.contains("breakdown"),
                    "error names the slot: {message}"
                );
                assert!(
                    !message.contains("unsupported"),
                    "pi is supported, just not here: {message}"
                );
            }
            other => panic!("expected RunEvent::Error, got {other:?}"),
        }
        assert!(rx.iter().any(|e| matches!(e, RunEvent::Exited { .. })));
    }
}
