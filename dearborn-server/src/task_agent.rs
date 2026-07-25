//! The `TaskAgent` seam + the §2.2 `Stage` vocabulary (T-512).
//!
//! This is the executor's counterpart to [`crate::planning::PlanningAgent`] /
//! [`crate::breakdown::BreakdownAgent`]: the same "put the harness behind a
//! trait so tests never shell out to `claude`" shape, applied to the five
//! agent stages a task's pipeline drives (`implement`, `fix`, `review`,
//! `verify_complete`, `summarize` — §2.2). The non-agent stages (`setup`,
//! `preflight`, `test_gate`, `commit`, `push`) share the same [`Stage`]
//! vocabulary and the same `agent_run` evidence table ([`crate::evidence`])
//! but never touch this trait — they run a shell command or plain git, not a
//! harness.
//!
//! ## Where `Stage` lives, and what happened to `spec::PromptStage`
//!
//! [`crate::spec`] shipped (T-502) with a deliberately minimal
//! `PromptStage` — five variants, one job: pick which `include_str!`
//! prompt [`crate::spec::prompt_for`] hands back. Its doc comment said the
//! real enum belonged to "T-512, not this module" — this is that enum.
//! [`Stage`] lives *here*, next to the [`TaskAgent`] trait it drives and the
//! `RunMode`/tool-flag mapping it decides, exactly how [`crate::planning`]
//! keeps `PlanningConfig` beside `PlanningAgent` rather than off in a
//! separate "config" module. `spec.rs` stays a pure, dependency-light leaf
//! (render/context/verdict, no I/O) — it now depends on this module only for
//! the plain `Stage` *type* (`crate::spec::prompt_for` takes a `Stage` and
//! returns `Option<&'static str>`, `None` for the five non-agent stages that
//! have no prompt), which costs it nothing I/O-shaped. `PromptStage` itself
//! is deleted: every variant it had is a `Stage` variant, and `Stage` covers
//! the other five besides.
//!
//! ## D19: no `resume` field, anywhere
//!
//! [`PlanningRunRequest`](crate::planning::PlanningRunRequest) and
//! [`BreakdownRunRequest`](crate::breakdown::BreakdownRunRequest) both carry
//! (or could carry) a native harness `resume` id, because those agents hold
//! a multi-turn conversation. A task stage never does: **D19 mandates a
//! fresh agent context for every stage**, even consecutive ones on the same
//! task (`implement` → `fix` → `review` → …). The entire point is that every
//! byte of cross-stage information flows through Dearborn's own state — the
//! rendered spec, the D8 context, the previous stage's test output or review
//! findings baked into the *next* stage's prompt — never through an agent's
//! memory of an earlier turn. [`TaskRunRequest`] simply has no `resume`
//! field. A field that was always set to `None` would be a loaded gun for a
//! future author to "helpfully" wire up during, say, the fix loop (T-522) or
//! the review/fix/re-review loop (T-531) — both of which *feel* like they
//! want continuity. Leaving the field out entirely makes that mistake a
//! compile error instead of a silent contract violation. [`ClaudeTaskAgent`]
//! hard-codes `resume: None` on the underlying [`RunRequest`] to close the
//! loop at the harness boundary too.
//!
//! ## Why the handle is returned, not dropped
//!
//! [`ClaudePlanningAgent::run`](crate::planning::ClaudePlanningAgent::run)
//! deliberately does `Ok((_handle, rx)) => rx` — T-202 had no cancel path,
//! so the handle was dead weight and the comment says so ("dropping the
//! handle does NOT cancel; the run proceeds to completion"). [`TaskAgent`]
//! does the opposite on purpose: `run` returns `(RunHandle, Receiver<RunEvent>)`
//! and every implementation (below) hands **both** back rather than
//! collapsing to just the receiver. The reason is T-542 (out of scope here,
//! but the reason this matters *now*): cancelling a running task stage is a
//! `RunControl::cancel()` call against the exact handle the stage's run
//! produced, held in a registry keyed by task/epic id. A `TaskAgent` that
//! discarded its handle the way `ClaudePlanningAgent` does would make that
//! feature structurally impossible to add without changing this trait's
//! signature later — so the contract is right the first time. Every caller
//! of [`TaskAgent::run`] (today: [`run_agent_stage`] in this module) must
//! keep the handle alive for the run's whole lifetime, not just long enough
//! to hand the receiver off.
//!
//! ## Soft read-only enforcement for `Review`/`VerifyComplete`/`Summarize`
//!
//! These three stages run in `RunMode::Ask` (no edits expected) *and*
//! additionally deny the edit-shaped tools via `--disallowedTools` (see
//! [`build_extra_args`]) — belt as well as suspenders. MILESTONE_2 §11 risk 2
//! is explicit that this is **soft**: `Bash` remains available to these
//! stages, and a determined reviewer could still write through it. The real
//! backstop is structural, not permission-based — the test gate and the
//! cumulative-diff review (T-522/T-530) — exactly as `references/ralph-v2.sh`
//! accepts the same property. `--disallowedTools` narrows the *accidental*
//! surface; it is not a sandbox.
//!
//! ## T-515's empirical finding: headless write-mode "just works" as built
//!
//! MILESTONE_2 §11 risk 1 flagged `RunMode::Edit` + `--permission-mode` +
//! tool flags as unproven — M1 never ran an agent read-write. `tests/
//! worker_live.rs` (T-515) is the live proof: a real `claude` subprocess,
//! `RunMode::Edit`, driven through the exact [`ClaudeTaskAgent::run`] below
//! with **no changes**, wrote a file, and the write landed without any
//! approval prompt blocking the run. The reason this worked with zero
//! plumbing changes here: `agent-harness`'s Claude adapter
//! (`build_claude_args`) already injects a *default* `--permission-mode
//! acceptEdits` for `RunMode::Edit` whenever the caller hasn't set
//! `--permission-mode` itself via `extra_args` — and [`build_extra_args`]
//! never does for `Implement`/`Fix` (only the `Review`/`VerifyComplete`/
//! `Summarize` trio gets `extra_args` at all, and that's `--disallowedTools`,
//! a different flag). `acceptEdits` auto-approves `Edit`/`Write`/`MultiEdit`
//! while leaving `Bash` gated; a task whose spec is satisfiable with the
//! editor tools alone (the common case — the T-515 fixture asked for exactly
//! one new file) never even hits the gated surface. The empirical run: one
//! `claude -p` turn, no `--model`/`--max-turns` override, no `--resume`,
//! cold-CLI-start-to-exit in well under 30s, one commit landing on the
//! branch with the §2.8 subject, pushed and read back from the bare-origin
//! fixture. **Caveat, not yet retired**: a task whose only path to
//! satisfying its acceptance criteria requires `Bash` (e.g. running a
//! generator script) will hit `acceptEdits`' gate and has not been
//! empirically exercised — if that turns out to block real epics, the fix is
//! a caller-supplied `--permission-mode` override via `extra_args` (the
//! adapter already supports last-wins), not a change to this reasoning.

use std::path::PathBuf;
use std::sync::mpsc::Receiver;
use std::time::Duration;

use harness::{Claude, Harness, HarnessError, RunEvent, RunHandle, RunMode, RunRequest, RunTuning};
use serde_json::Value;

use crate::evidence::{self, CloseStage, OpenStage, StageHandle};
use crate::AppState;

// ---- §2.2 stage vocabulary (D6) -------------------------------------------

/// The full `agent_run.stage` vocabulary (MILESTONE_2 §2.2), covering both
/// agent stages (driven through [`TaskAgent`]) and non-agent stages (a shell
/// command or plain git, run by other modules but sharing this same
/// vocabulary + the same evidence table).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Stage {
    /// `setup_cmd` in the fresh workspace. Non-agent.
    Setup,
    /// `test_cmd` on the untouched tree; red ⇒ `Blocked`. Non-agent.
    Preflight,
    /// Make the code changes for a task. Agent, `RunMode::Edit`.
    Implement,
    /// `test_cmd`; `attempt` = 0..N. Non-agent.
    TestGate,
    /// Address one round of test or review feedback. Agent, `RunMode::Edit`.
    Fix,
    /// Confirm a no-diff task is genuinely done. Agent, `RunMode::Ask` (+
    /// denied edit tools).
    VerifyComplete,
    /// Findings + the D9 `VERDICT:` line. Agent, `RunMode::Ask` (+ denied
    /// edit tools).
    Review,
    /// Records the commit SHA in `log`. Non-agent.
    Commit,
    /// Non-agent.
    Push,
    /// The PR body's "Summary of changes" section; failure is non-fatal.
    /// Agent, `RunMode::Ask` (+ denied edit tools).
    Summarize,
}

impl Stage {
    /// The exact `agent_run.stage` string (§2.2's table), frozen — every
    /// other stage-facing thing (the evidence rows, the WS `stage_changed`
    /// payload, §2.3's failure reasons) reads this same value back out of
    /// the database, so it must never drift from the milestone doc's table.
    pub fn as_str(self) -> &'static str {
        match self {
            Stage::Setup => "setup",
            Stage::Preflight => "preflight",
            Stage::Implement => "implement",
            Stage::TestGate => "test_gate",
            Stage::Fix => "fix",
            Stage::VerifyComplete => "verify_complete",
            Stage::Review => "review",
            Stage::Commit => "commit",
            Stage::Push => "push",
            Stage::Summarize => "summarize",
        }
    }

    /// Whether this stage is driven through [`TaskAgent`] at all (§2.2's
    /// "Agent?" column). `false` stages run a shell command or plain git —
    /// they still open/close an `agent_run` row (D13: the table covers
    /// every stage), just never through this trait.
    pub fn is_agent_stage(self) -> bool {
        self.run_mode().is_some()
    }

    /// The harness `RunMode` for an agent stage (§2.2), `None` for the five
    /// non-agent stages. `Implement`/`Fix` propose edits; the read-only-by-
    /// contract trio (`Review`/`VerifyComplete`/`Summarize`) only discuss.
    pub fn run_mode(self) -> Option<RunMode> {
        match self {
            Stage::Implement | Stage::Fix => Some(RunMode::Edit),
            Stage::Review | Stage::VerifyComplete | Stage::Summarize => Some(RunMode::Ask),
            Stage::Setup | Stage::Preflight | Stage::TestGate | Stage::Commit | Stage::Push => {
                None
            }
        }
    }

    /// Whether [`ClaudeTaskAgent`] additionally denies edit-shaped tools for
    /// this stage (see the module doc's "soft read-only enforcement"
    /// section). Exactly the `Ask`-mode agent trio — `Implement`/`Fix` need
    /// their edit tools; non-agent stages never reach this at all.
    pub fn denies_edit_tools(self) -> bool {
        matches!(self, Stage::Review | Stage::VerifyComplete | Stage::Summarize)
    }
}

// ---- the agent seam (D6) ---------------------------------------------------

/// One task-stage agent run to perform, decoupled from the harness so tests
/// inject a scripted fake (mirrors
/// [`PlanningRunRequest`](crate::planning::PlanningRunRequest) /
/// [`BreakdownRunRequest`](crate::breakdown::BreakdownRunRequest)). See the
/// module doc for why there is deliberately no `resume` field (D19).
pub struct TaskRunRequest {
    /// Unique id for this run (a ULID); echoed back on every `RunEvent`.
    pub run_id: String,
    /// Which stage this is — decides `RunMode` and the denied-tools flag in
    /// [`ClaudeTaskAgent`]. Must be [`Stage::is_agent_stage`].
    pub stage: Stage,
    /// The fully assembled prompt: the stage's static instructions
    /// ([`crate::spec::prompt_for`]) followed by the D8 context block
    /// ([`crate::spec::build_context`]) — see [`assemble_prompt`]. Unlike
    /// planning/breakdown, a task stage has no separate stable "system
    /// prompt" that persists across turns (there are no turns, per D19), so
    /// the whole thing rides as one prompt string.
    pub prompt: String,
    /// The workspace to run in — always a real, already-provisioned
    /// directory (unlike planning's `Option<PathBuf>`, which may run
    /// tool-less): every task stage acts on a checked-out branch.
    pub cwd: PathBuf,
}

/// Assemble the full prompt for `stage`: its static instructions
/// ([`crate::spec::prompt_for`]) followed by the D8 context block
/// ([`crate::spec::build_context`]). `None` for a non-agent stage (there is
/// no prompt to assemble; callers must not construct a [`TaskRunRequest`]
/// for one). Centralizing the "prompt, then context" concatenation here
/// means a future caller (T-513's DAG walk) never has to re-derive the
/// ordering — it just builds a [`crate::spec::TaskContext`] from the DB and
/// calls this.
pub fn assemble_prompt(stage: Stage, context: &crate::spec::TaskContext) -> Option<String> {
    let base = crate::spec::prompt_for(stage)?;
    let context_block = crate::spec::build_context(context);
    Some(format!("{base}\n\n---\n\n{context_block}"))
}

/// Assemble the `Fix` stage's prompt for T-522's test-driven fix loop (and,
/// later, T-531's review-findings fix loop — the same function serves both
/// kinds of "one round of feedback"): `prompts/fix.md` followed by **only**
/// `feedback` — the failing test output today, a reviewer's findings once
/// T-531 lands. Deliberately **not** [`assemble_prompt`] +
/// [`crate::spec::TaskContext`] — that pairing is what `Stage::Implement`
/// gets (the rendered spec, the epic's background, the sibling manifest),
/// and this is the literal reading of T-522's AC: "the fix agent receives
/// the test output and no other stage's context." A test in `worker.rs`'s
/// module asserts this directly against what a `ScriptedTaskAgent` recorded:
/// the fix prompt contains the feedback and does **not** contain the spec
/// block, the epic-context heading, or the sibling manifest.
///
/// **Open concern, flagged rather than quietly resolved:** `prompts/fix.md`
/// asks the fix agent to address "only what the feedback raises" and to
/// avoid "anything the sibling manifest marks as owned by a later task" —
/// but with no spec, no acceptance criteria, and no sibling manifest in its
/// prompt at all, the fix agent has no way to independently judge whether a
/// change satisfies the *task's* intent versus merely making the immediate
/// symptom go away, and no way to know which files belong to a sibling task
/// it must leave alone. It can still often do the job well (many test
/// failures are self-describing — a stack trace, an assertion diff, a type
/// error — and `git diff`/reading the surrounding code fills in a lot), but
/// this is a real gap relative to what `references/prompts/fix-task-v2.md`
/// (ralph's own fix prompt, whose bash caller passed only a feedback file
/// path too, but ralph never had a sibling-manifest concept to omit) assumed
/// its agent would have. Worth revisiting — most naturally when T-531 wires
/// this same function up for review findings, since a NEEDS_CHANGES verdict
/// is even more likely to hinge on acceptance criteria the fix agent can't
/// see under the current contract.
pub fn assemble_fix_prompt(feedback: &str) -> String {
    let base = crate::spec::prompt_for(Stage::Fix).expect("Stage::Fix always has a prompt");
    format!("{base}\n\n---\n\n## Feedback\n\n{feedback}")
}

/// The seam that makes task-stage runs hermetically testable (mirrors
/// [`crate::planning::PlanningAgent`] / [`crate::breakdown::BreakdownAgent`]).
///
/// Unlike those two, `run` returns a `Result`: a task stage's caller
/// ([`run_agent_stage`]) needs to distinguish "the harness never started"
/// (close the row immediately, no draining) from "it started and streamed" —
/// planning/breakdown fold that distinction into a synthetic `Error`+`Exited`
/// stream instead, which would cost this seam the clean failure path it
/// needs to close an `agent_run` row correctly on a spawn failure.
pub trait TaskAgent: Send + Sync {
    /// Start a run and hand back its **handle and** its `RunEvent` receiver.
    /// See the module doc for why both, not just the receiver.
    fn run(&self, req: TaskRunRequest) -> Result<(RunHandle, Receiver<RunEvent>), HarnessError>;
}

/// Production [`TaskAgent`]: drives Claude Code through the harness exactly
/// as [`crate::planning::ClaudePlanningAgent`] does, with the task-stage
/// specifics — `RunMode` from [`Stage::run_mode`], denied edit tools for the
/// read-only-by-contract trio, always-fresh context (D19).
#[derive(Default)]
pub struct ClaudeTaskAgent;

impl ClaudeTaskAgent {
    /// Construct the production agent.
    pub fn new() -> ClaudeTaskAgent {
        ClaudeTaskAgent
    }
}

impl TaskAgent for ClaudeTaskAgent {
    fn run(&self, req: TaskRunRequest) -> Result<(RunHandle, Receiver<RunEvent>), HarnessError> {
        // A non-agent stage has no `RunMode`; a caller that reaches this with
        // one is a bug upstream (T-513's job), not something to spawn a
        // harness for. Default to `Ask` defensively rather than panic — the
        // stage's own worker-side dispatch is the real guard.
        let mode = req.stage.run_mode().unwrap_or(RunMode::Ask);

        let request = RunRequest {
            run_id: req.run_id,
            prompt: req.prompt,
            cwd: Some(req.cwd),
            mode,
            tuning: RunTuning {
                extra_args: build_extra_args(req.stage),
                ..RunTuning::default()
            },
            // D19: every stage is a brand new agent context, always.
            resume: None,
        };

        // Returned straight through — see the module doc's "why the handle
        // is returned, not dropped" section. This is the whole point of the
        // divergence from `ClaudePlanningAgent::run`.
        Claude::new().run_channel(request)
    }
}

/// The exact `--disallowedTools` value for the read-only-by-contract trio
/// (module doc: "soft read-only enforcement"). Named tools, comma-separated,
/// matching the CLI's own flag shape (mirrors how `ClaudePlanningAgent` /
/// `ClaudeBreakdownAgent` pass `--allowedTools`).
const DENIED_EDIT_TOOLS: &str = "Edit,Write,MultiEdit,NotebookEdit";

/// Build the harness `extra_args` for `stage`. Factored out of
/// [`ClaudeTaskAgent::run`] as a pure function (no harness, no spawn) so the
/// stage → flags mapping is unit-tested directly instead of only through a
/// live `claude` run.
fn build_extra_args(stage: Stage) -> Vec<String> {
    let mut args = Vec::new();
    if stage.denies_edit_tools() {
        args.push("--disallowedTools".to_string());
        args.push(DENIED_EDIT_TOOLS.to_string());
    }
    args
}

// ---- driving one agent stage to completion (D14) ---------------------------

/// How often a streaming agent stage flushes its accumulated log to the
/// `agent_run` row (D14) — named so the interval is documented in one place
/// rather than a bare literal at the call site. ~2s is short enough that a
/// browser opening a task mid-run sees output within a couple of seconds of
/// hydrating over REST, and long enough that a chatty run (many small `Text`
/// deltas) doesn't turn into a write-per-token hammering on libSQL's single
/// writer.
pub const PARTIAL_FLUSH_INTERVAL: Duration = Duration::from_secs(2);

/// Identifies the stage row to open + the `task:<id>` topic to relay on.
pub struct AgentStageParams<'a> {
    pub task_id: &'a str,
    /// `None` for a standalone task (D17).
    pub epic_id: Option<&'a str>,
    /// `agent_run.attempt` — 1 for the first try at this stage, bumped by
    /// the caller on a retry/fix round.
    pub attempt: i64,
}

/// What a completed (or failed) agent stage leaves behind for the caller
/// (T-513's DAG walk) to act on — e.g. deciding whether `Fix` produced a
/// diff, or handing `text` to [`crate::spec::parse_verdict`] for a review
/// stage (T-530).
#[derive(Debug, Clone, Default)]
pub struct AgentStageOutcome {
    /// All `Text` deltas, concatenated — the agent's assembled reply.
    pub text: String,
    /// The harness session id, if the CLI reported one.
    pub session_id: Option<String>,
    pub exit_code: Option<i32>,
    pub cancelled: bool,
    /// Whether an `Error` event was seen anywhere in the stream.
    pub errored: bool,
}

impl AgentStageOutcome {
    fn absorb(&mut self, event: &RunEvent) {
        match event {
            RunEvent::Text { delta, .. } => self.text.push_str(delta),
            RunEvent::Session {
                session_id: Some(id),
                ..
            } => self.session_id = Some(id.clone()),
            RunEvent::Error { message, .. } => {
                self.errored = true;
                self.text.push_str(&format!("\n[error] {message}\n"));
            }
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

    /// The §2.1 terminal `agent_run.status` this outcome implies.
    fn status(&self) -> &'static str {
        if self.cancelled {
            "cancelled"
        } else if self.errored {
            "error"
        } else if self.exit_code == Some(0) {
            "ok"
        } else {
            "error"
        }
    }

    /// Whether this stage completed cleanly (`status() == "ok"`) — the
    /// question a caller driving a multi-stage pipeline actually needs to
    /// ask (T-513's DAG walk: did `Implement` succeed enough to proceed to
    /// `git add`/commit, or must the task/epic be routed to a failure path
    /// instead?). Exposed as its own method rather than making callers derive
    /// it from the public `cancelled`/`errored`/`exit_code` fields themselves
    /// — those fields stay public for diagnostics/logging, but the pass/fail
    /// *decision* is made once, here.
    pub fn is_ok(&self) -> bool {
        self.status() == "ok"
    }
}

/// Failure modes of [`run_agent_stage`] itself (as opposed to the stage's
/// own outcome, which is never an `Err` here — a red review or a failed
/// implement attempt is still a *successful* run of the agent, just one
/// whose `AgentStageOutcome` says so; T-513/T-530 decide what that means for
/// the task).
#[derive(Debug)]
pub enum AgentStageError {
    /// Opening or closing the `agent_run` row failed.
    Db(libsql::Error),
    /// The harness never started (e.g. `claude` not on `PATH`). The row is
    /// still closed (`status = "error"`) before this is returned.
    Harness(HarnessError),
    /// The drain thread itself panicked or was aborted. The row is still
    /// closed (`status = "error"`) before this is returned — see
    /// [`run_agent_stage`]'s handling of a failed `JoinHandle`.
    DrainFailed(String),
}

impl std::fmt::Display for AgentStageError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AgentStageError::Db(e) => write!(f, "agent_run row error: {e}"),
            AgentStageError::Harness(e) => write!(f, "agent stage failed to start: {e}"),
            AgentStageError::DrainFailed(msg) => write!(f, "{msg}"),
        }
    }
}

impl std::error::Error for AgentStageError {}

/// Drive one agent stage to completion: open its `agent_run` row, start the
/// run through `agent`, relay every `RunEvent` live on `task:<task_id>`
/// (D14, reusing [`crate::planning::ws_type`]), flush the accumulating log
/// to the row roughly every [`PARTIAL_FLUSH_INTERVAL`] while it streams, then
/// close the row with the terminal status/session id/exit code/log (D13
/// capped). Returns the assembled [`AgentStageOutcome`] so a caller decides
/// what happens next — T-512 does not call this from anywhere in production
/// yet (the DAG walk is T-513's slice); it exists so the evidence + streaming
/// contract can be built and tested now, ahead of the walk that will drive it.
///
/// The [`RunHandle`] `agent.run` returns is held for the **entire** drain,
/// even though nothing here calls `cancel()` on it yet (T-542) — see the
/// module doc.
pub async fn run_agent_stage(
    state: &AppState,
    agent: &dyn TaskAgent,
    params: AgentStageParams<'_>,
    req: TaskRunRequest,
) -> Result<AgentStageOutcome, AgentStageError> {
    let conn = state.db.conn();
    let open = OpenStage {
        task_id: Some(params.task_id),
        epic_id: params.epic_id,
        stage: req.stage.as_str(),
        attempt: params.attempt,
    };
    let stage_row = evidence::open_stage(conn, open)
        .await
        .map_err(AgentStageError::Db)?;

    let (run_handle, rx) = match agent.run(req) {
        Ok(pair) => pair,
        Err(err) => {
            let _ = evidence::close_stage(
                conn,
                &stage_row,
                CloseStage {
                    status: "error",
                    session_id: None,
                    verdict: None,
                    exit_code: None,
                    log: format!("failed to start agent stage: {err}"),
                },
            )
            .await;
            return Err(AgentStageError::Harness(err));
        }
    };
    // Held across the whole drain below — see the module doc.
    let _run_handle = run_handle;

    let hub = state.hub.clone();
    let topic = format!("task:{}", params.task_id);

    // Shared with the periodic-flush task below: the blocking drain thread
    // writes the latest accumulated log text here; the async flush loop reads
    // a snapshot every tick. A plain `std::sync::Mutex` is fine — both sides
    // only ever hold it for a `String` clone/replace, never across an `.await`.
    let shared_log = std::sync::Arc::new(std::sync::Mutex::new(String::new()));
    let shared_log_writer = shared_log.clone();

    let drain_task = tokio::task::spawn_blocking(move || {
        let mut outcome = AgentStageOutcome::default();
        for event in rx {
            let payload = serde_json::to_value(&event).unwrap_or(Value::Null);
            hub.publish(&topic, crate::planning::ws_type(&event), payload);
            outcome.absorb(&event);
            *shared_log_writer.lock().expect("shared_log mutex poisoned") = outcome.text.clone();
        }
        outcome
    });

    // The D14 partial-flush loop: while the drain above is in flight, copy
    // the shared accumulator into the `agent_run` row every
    // `PARTIAL_FLUSH_INTERVAL`. Runs as its own task (not inside the blocking
    // drain) because flushing is an async DB write and the drain thread is a
    // plain blocking one — this avoids reaching for `Handle::block_on` from
    // inside `spawn_blocking`, which works but is a subtler pattern than two
    // independent tasks sharing a `Mutex<String>`. Stopped via `abort()` the
    // instant the drain finishes; the *final* close below writes the
    // complete, un-raced log regardless of this loop's last tick.
    let flush_conn = conn.clone();
    let flush_log = shared_log.clone();
    let flush_row_id = stage_row.id.clone();
    let flush_handle = tokio::spawn(async move {
        let mut interval = tokio::time::interval(PARTIAL_FLUSH_INTERVAL);
        interval.tick().await; // first tick fires immediately; skip so the
                                // first *real* flush is ~PARTIAL_FLUSH_INTERVAL in.
        loop {
            interval.tick().await;
            let snapshot = flush_log.lock().expect("shared_log mutex poisoned").clone();
            let handle = StageHandle {
                id: flush_row_id.clone(),
                started_at: stage_row.started_at,
            };
            let _ = evidence::flush_stage_log(&flush_conn, &handle, &snapshot).await;
        }
    });

    let drained = drain_task.await;
    flush_handle.abort();

    let outcome = match drained {
        Ok(outcome) => outcome,
        Err(join_err) => {
            // The blocking drain thread panicked. Close the row so it never
            // sticks `running`, then surface the panic message as the
            // outcome's own error text (there is no `AgentStageOutcome` to
            // return — the drain never finished).
            let message = if join_err.is_panic() {
                "agent stage's drain thread panicked".to_string()
            } else {
                format!("agent stage's drain thread was cancelled: {join_err}")
            };
            let _ = evidence::close_stage(
                conn,
                &stage_row,
                CloseStage {
                    status: "error",
                    session_id: None,
                    verdict: None,
                    exit_code: None,
                    log: message.clone(),
                },
            )
            .await;
            return Err(AgentStageError::DrainFailed(message));
        }
    };

    evidence::close_stage(
        conn,
        &stage_row,
        CloseStage {
            status: outcome.status(),
            session_id: outcome.session_id.clone(),
            // T-530's job to populate on a review/verify_complete stage; T-512
            // only guarantees the column can be written, not that it is here.
            verdict: None,
            exit_code: outcome.exit_code,
            log: outcome.text.clone(),
        },
    )
    .await
    .map_err(AgentStageError::Db)?;

    Ok(outcome)
}

// ---- test doubles (crate-visible so worker/DAG-walk tests can inject them,
// T-513+) --------------------------------------------------------------------
//
// Gated `#[cfg(test)]` + `pub(crate)`, mirroring `planning::testing` /
// `breakdown::testing` exactly: every existing agent seam's fake lives this
// way, reachable from any unit test in this crate (including a future
// `worker.rs` test module), but invisible to the separate `tests/*.rs`
// integration-test crate — those drive the real `ClaudeTaskAgent` instead
// (see `tests/worker_live.rs`, T-515), the same way `tests/mcp_live.rs` /
// `tests/ws.rs` already do for planning. If a later phase's integration test
// genuinely needs the scripted fake, promoting this module to a plain `pub
// mod` (dropping the `#[cfg(test)]`) is a one-line change; nothing in this
// module's shape depends on staying test-only.
#[cfg(test)]
pub(crate) mod testing {
    use super::*;
    use crate::planning::testing::Gate;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::{Arc, Mutex};

    /// One task-stage run the fake was asked to perform, recorded so a test
    /// can assert on prompt content, stage, and working directory.
    #[derive(Clone, Debug)]
    #[allow(dead_code)]
    pub struct RecordedTaskRun {
        pub run_id: String,
        pub stage: Stage,
        pub prompt: String,
        pub cwd: PathBuf,
    }

    /// One scripted response for a stage: the text to stream (as `Text`
    /// chunks), the session id to report, files to write into `cwd` before
    /// exiting (later phases need "the agent writes a file" — T-513's tracer
    /// bullet, T-522's fix loop), and the exit code to report.
    #[derive(Clone, Debug)]
    pub struct ScriptedRun {
        pub session_id: String,
        pub text: Vec<String>,
        /// `(path relative to cwd, content)`. Parent directories are created
        /// as needed.
        pub files: Vec<(PathBuf, String)>,
        pub exit_code: Option<i32>,
    }

    impl Default for ScriptedRun {
        fn default() -> ScriptedRun {
            ScriptedRun {
                session_id: "scripted-session".to_string(),
                text: vec!["ok".to_string()],
                files: Vec::new(),
                exit_code: Some(0),
            }
        }
    }

    /// A [`RunControl`] whose `cancel()` is actually observable — unlike a
    /// no-op stub, `was_cancelled()` reflects a real `cancel()` call, so a
    /// test can prove the handle [`ScriptedTaskAgent::run`] hands back is a
    /// live, usable [`RunHandle`] and not a decoration. The flag is an `Arc`
    /// so both the returned `RunHandle` and the scripted run's own thread
    /// (which checks it before emitting `Exited { cancelled }`) share the
    /// same underlying bool.
    struct ScriptedControl {
        cancelled: Arc<AtomicBool>,
    }

    impl harness::RunControl for ScriptedControl {
        fn cancel(&self) -> Result<(), HarnessError> {
            self.cancelled.store(true, Ordering::SeqCst);
            Ok(())
        }
        fn was_cancelled(&self) -> bool {
            self.cancelled.load(Ordering::SeqCst)
        }
    }

    /// Scripted [`TaskAgent`]: per stage, a queue of [`ScriptedRun`]s (so a
    /// test can script a fix loop's several attempts at the same stage
    /// differently); a stage with an empty/exhausted queue gets
    /// [`ScriptedRun::default`]. Emits Started → Session → Text* → (writes
    /// any scripted files) → \[optional gate\] → Exited, and records every
    /// request it received.
    pub struct ScriptedTaskAgent {
        scripts: Mutex<std::collections::HashMap<&'static str, std::collections::VecDeque<ScriptedRun>>>,
        recorded: Arc<Mutex<Vec<RecordedTaskRun>>>,
        gate: Option<Arc<Gate>>,
    }

    impl Default for ScriptedTaskAgent {
        fn default() -> ScriptedTaskAgent {
            ScriptedTaskAgent {
                scripts: Mutex::new(std::collections::HashMap::new()),
                recorded: Arc::new(Mutex::new(Vec::new())),
                gate: None,
            }
        }
    }

    impl ScriptedTaskAgent {
        pub fn new() -> ScriptedTaskAgent {
            ScriptedTaskAgent::default()
        }

        /// Queue `run` as the next scripted response for `stage`.
        pub fn script(self, stage: Stage, run: ScriptedRun) -> ScriptedTaskAgent {
            self.scripts
                .lock()
                .unwrap()
                .entry(stage.as_str())
                .or_default()
                .push_back(run);
            self
        }

        /// Attach a gate that pins each run in-flight (before its terminal
        /// `Exited`) until released — lets a test hold a stage in flight
        /// deterministically to exercise cancellation or a mid-run WS join.
        pub fn with_gate(mut self, gate: Arc<Gate>) -> ScriptedTaskAgent {
            self.gate = Some(gate);
            self
        }

        /// Handle to the recorded runs (for assertions on stage/prompt/cwd).
        pub fn recorded(&self) -> Arc<Mutex<Vec<RecordedTaskRun>>> {
            self.recorded.clone()
        }
    }

    impl TaskAgent for ScriptedTaskAgent {
        fn run(&self, req: TaskRunRequest) -> Result<(RunHandle, Receiver<RunEvent>), HarnessError> {
            self.recorded.lock().unwrap().push(RecordedTaskRun {
                run_id: req.run_id.clone(),
                stage: req.stage,
                prompt: req.prompt.clone(),
                cwd: req.cwd.clone(),
            });

            let script = self
                .scripts
                .lock()
                .unwrap()
                .get_mut(req.stage.as_str())
                .and_then(|q| q.pop_front())
                .unwrap_or_default();

            let (tx, rx) = std::sync::mpsc::channel();
            let run_id = req.run_id;
            let cwd = req.cwd;
            let gate = self.gate.clone();
            let cancelled = Arc::new(AtomicBool::new(false));
            let cancelled_for_thread = cancelled.clone();

            std::thread::spawn(move || {
                let _ = tx.send(RunEvent::Started {
                    run_id: run_id.clone(),
                });
                let _ = tx.send(RunEvent::Session {
                    run_id: run_id.clone(),
                    session_id: Some(script.session_id.clone()),
                    model: Some("scripted-model".to_string()),
                });
                for chunk in &script.text {
                    let _ = tx.send(RunEvent::Text {
                        run_id: run_id.clone(),
                        delta: chunk.clone(),
                    });
                }
                for (rel_path, content) in &script.files {
                    let target = cwd.join(rel_path);
                    if let Some(parent) = target.parent() {
                        let _ = std::fs::create_dir_all(parent);
                    }
                    let _ = std::fs::write(&target, content);
                }
                if let Some(gate) = gate {
                    gate.wait();
                }
                let cancelled = cancelled_for_thread.load(Ordering::SeqCst);
                let _ = tx.send(RunEvent::Exited {
                    run_id,
                    exit_code: if cancelled { None } else { script.exit_code },
                    cancelled,
                });
            });

            Ok((Box::new(ScriptedControl { cancelled }) as RunHandle, rx))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::testing::*;
    use super::*;
    use crate::planning::testing::Gate;
    use crate::{Config, Db};
    use std::sync::Arc;

    // ---- Stage vocabulary ---------------------------------------------

    #[test]
    fn as_str_matches_the_section_2_2_table() {
        assert_eq!(Stage::Setup.as_str(), "setup");
        assert_eq!(Stage::Preflight.as_str(), "preflight");
        assert_eq!(Stage::Implement.as_str(), "implement");
        assert_eq!(Stage::TestGate.as_str(), "test_gate");
        assert_eq!(Stage::Fix.as_str(), "fix");
        assert_eq!(Stage::VerifyComplete.as_str(), "verify_complete");
        assert_eq!(Stage::Review.as_str(), "review");
        assert_eq!(Stage::Commit.as_str(), "commit");
        assert_eq!(Stage::Push.as_str(), "push");
        assert_eq!(Stage::Summarize.as_str(), "summarize");
    }

    #[test]
    fn implement_and_fix_are_edit_mode() {
        assert_eq!(Stage::Implement.run_mode(), Some(RunMode::Edit));
        assert_eq!(Stage::Fix.run_mode(), Some(RunMode::Edit));
        assert!(!Stage::Implement.denies_edit_tools());
        assert!(!Stage::Fix.denies_edit_tools());
    }

    #[test]
    fn review_verify_complete_and_summarize_are_ask_mode_and_deny_edit_tools() {
        for stage in [Stage::Review, Stage::VerifyComplete, Stage::Summarize] {
            assert_eq!(stage.run_mode(), Some(RunMode::Ask), "{stage:?}");
            assert!(stage.denies_edit_tools(), "{stage:?}");
        }
    }

    #[test]
    fn non_agent_stages_have_no_run_mode_and_are_not_agent_stages() {
        for stage in [
            Stage::Setup,
            Stage::Preflight,
            Stage::TestGate,
            Stage::Commit,
            Stage::Push,
        ] {
            assert_eq!(stage.run_mode(), None, "{stage:?}");
            assert!(!stage.is_agent_stage(), "{stage:?}");
            assert!(!stage.denies_edit_tools(), "{stage:?}");
        }
        for stage in [
            Stage::Implement,
            Stage::Fix,
            Stage::Review,
            Stage::VerifyComplete,
            Stage::Summarize,
        ] {
            assert!(stage.is_agent_stage(), "{stage:?}");
        }
    }

    // ---- build_extra_args (pure, no spawn) -----------------------------

    #[test]
    fn implement_and_fix_get_no_disallowed_tools_flag() {
        for stage in [Stage::Implement, Stage::Fix] {
            let args = build_extra_args(stage);
            assert!(!args.iter().any(|a| a == "--disallowedTools"), "{stage:?}: {args:?}");
        }
    }

    #[test]
    fn review_family_gets_the_disallowed_edit_tools_flag() {
        for stage in [Stage::Review, Stage::VerifyComplete, Stage::Summarize] {
            let args = build_extra_args(stage);
            let idx = args
                .iter()
                .position(|a| a == "--disallowedTools")
                .unwrap_or_else(|| panic!("{stage:?} missing --disallowedTools: {args:?}"));
            assert_eq!(args[idx + 1], DENIED_EDIT_TOOLS);
        }
    }

    // ---- assemble_prompt -------------------------------------------------

    #[test]
    fn assemble_prompt_appends_context_after_the_stage_prompt_and_is_none_for_non_agent_stages() {
        let ctx = crate::spec::TaskContext {
            spec: crate::spec::SpecFields {
                title: "Do the thing",
                description: Some("desc"),
                acceptance: Some("acc"),
            },
            epic: None,
            siblings: &[],
        };
        let assembled = assemble_prompt(Stage::Implement, &ctx).expect("implement has a prompt");
        let prompt_only = crate::spec::prompt_for(Stage::Implement).unwrap();
        assert!(assembled.starts_with(prompt_only));
        assert!(assembled.contains("Do the thing"));
        let prompt_pos = assembled.find(prompt_only).unwrap();
        let context_pos = assembled.find("Do the thing").unwrap();
        assert!(prompt_pos < context_pos, "context must follow the prompt");

        assert!(assemble_prompt(Stage::Setup, &ctx).is_none());
        assert!(assemble_prompt(Stage::Commit, &ctx).is_none());
    }

    // ---- the handle is returned, not dropped ---------------------------

    #[test]
    fn scripted_agent_hands_back_a_live_cancellable_handle() {
        let agent = ScriptedTaskAgent::new();
        let dir = std::env::temp_dir().join(format!("dearborn-task-agent-{}", ulid::Ulid::new()));
        std::fs::create_dir_all(&dir).unwrap();
        let (handle, rx) = agent
            .run(TaskRunRequest {
                run_id: "r1".to_string(),
                stage: Stage::Implement,
                prompt: "do it".to_string(),
                cwd: dir.clone(),
            })
            .unwrap();

        assert!(!handle.was_cancelled(), "not cancelled yet");
        handle.cancel().unwrap();
        assert!(handle.was_cancelled(), "cancel() must be observable on the returned handle");

        // Drain to completion so the thread doesn't outlive the test.
        for _ in rx {}
        std::fs::remove_dir_all(&dir).ok();
    }

    /// A gated run pins its `Exited` behind the gate; cancelling the handle
    /// while gated is observable, and the eventual `Exited` reports
    /// `cancelled: true` — the deterministic (no-sleep) shape a future
    /// cancel-registry test (T-542) will want.
    #[test]
    fn gated_run_can_be_cancelled_before_it_exits() {
        let gate = Arc::new(Gate::default());
        let agent = ScriptedTaskAgent::new().with_gate(gate.clone());
        let dir = std::env::temp_dir().join(format!("dearborn-task-agent-{}", ulid::Ulid::new()));
        std::fs::create_dir_all(&dir).unwrap();

        let (handle, rx) = agent
            .run(TaskRunRequest {
                run_id: "r-gated".to_string(),
                stage: Stage::Implement,
                prompt: "do it".to_string(),
                cwd: dir.clone(),
            })
            .unwrap();

        // Drain up through the scripted Text event; the run is now blocked on
        // the gate, before its terminal Exited.
        let mut saw_text = false;
        for event in &rx {
            if matches!(event, RunEvent::Text { .. }) {
                saw_text = true;
                break;
            }
        }
        assert!(saw_text, "must observe streamed output before the gate blocks completion");

        handle.cancel().unwrap();
        assert!(handle.was_cancelled());
        gate.release();

        let exited = rx.into_iter().find(|e| matches!(e, RunEvent::Exited { .. }));
        match exited {
            Some(RunEvent::Exited { cancelled, .. }) => {
                assert!(cancelled, "Exited must report cancelled: true after handle.cancel()")
            }
            other => panic!("expected an Exited event, got {other:?}"),
        }
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn recorded_run_captures_stage_prompt_and_cwd() {
        let agent = ScriptedTaskAgent::new();
        let recorded = agent.recorded();
        let dir = std::env::temp_dir().join(format!("dearborn-task-agent-{}", ulid::Ulid::new()));
        std::fs::create_dir_all(&dir).unwrap();

        let (_handle, rx) = agent
            .run(TaskRunRequest {
                run_id: "r2".to_string(),
                stage: Stage::Review,
                prompt: "review this".to_string(),
                cwd: dir.clone(),
            })
            .unwrap();
        for _ in rx {}

        let runs = recorded.lock().unwrap();
        assert_eq!(runs.len(), 1);
        assert_eq!(runs[0].stage, Stage::Review);
        assert_eq!(runs[0].prompt, "review this");
        assert_eq!(runs[0].cwd, dir);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn scripted_agent_writes_scripted_files_into_cwd() {
        let agent = ScriptedTaskAgent::new().script(
            Stage::Implement,
            ScriptedRun {
                session_id: "s1".to_string(),
                text: vec!["done".to_string()],
                files: vec![(PathBuf::from("out.txt"), "hello".to_string())],
                exit_code: Some(0),
            },
        );
        let dir = std::env::temp_dir().join(format!("dearborn-task-agent-{}", ulid::Ulid::new()));
        std::fs::create_dir_all(&dir).unwrap();

        let (_handle, rx) = agent
            .run(TaskRunRequest {
                run_id: "r3".to_string(),
                stage: Stage::Implement,
                prompt: "write a file".to_string(),
                cwd: dir.clone(),
            })
            .unwrap();
        for _ in rx {}

        let written = std::fs::read_to_string(dir.join("out.txt")).unwrap();
        assert_eq!(written, "hello");
        std::fs::remove_dir_all(&dir).ok();
    }

    // ---- run_agent_stage: evidence + WS streaming ----------------------

    async fn test_state() -> AppState {
        let db = Db::connect(":memory:").await.unwrap();
        db.run_migrations().await.unwrap();
        db.conn()
            .execute(
                "INSERT INTO project (id, name, repo_url, clone_status, created_at, updated_at) \
                 VALUES ('proj-1', 'P', 'https://example.com/p.git', 'ready', 0, 0)",
                (),
            )
            .await
            .unwrap();
        db.conn()
            .execute(
                "INSERT INTO epic (id, project_id, title, status, created_at, updated_at) \
                 VALUES ('epic-1', 'proj-1', 'E', 'InProgress', 0, 0)",
                (),
            )
            .await
            .unwrap();
        db.conn()
            .execute(
                "INSERT INTO task (id, epic_id, project_id, title, status, position, created_at, updated_at) \
                 VALUES ('task-1', 'epic-1', 'proj-1', 'T', 'InProgress', 1, 0, 0)",
                (),
            )
            .await
            .unwrap();
        AppState::new(Config::for_test("tok"), db)
    }

    #[tokio::test]
    async fn run_agent_stage_opens_streams_and_closes_the_row() {
        let state = test_state().await;
        let agent = ScriptedTaskAgent::new().script(
            Stage::Implement,
            ScriptedRun {
                session_id: "sess-42".to_string(),
                text: vec!["Hello".to_string(), ", world".to_string()],
                files: Vec::new(),
                exit_code: Some(0),
            },
        );

        let sub = state.hub.subscribe("task:task-1");

        let dir = std::env::temp_dir().join(format!("dearborn-run-agent-stage-{}", ulid::Ulid::new()));
        std::fs::create_dir_all(&dir).unwrap();

        let outcome = run_agent_stage(
            &state,
            &agent,
            AgentStageParams {
                task_id: "task-1",
                epic_id: Some("epic-1"),
                attempt: 1,
            },
            TaskRunRequest {
                run_id: "run-1".to_string(),
                stage: Stage::Implement,
                prompt: "go".to_string(),
                cwd: dir.clone(),
            },
        )
        .await
        .expect("scripted stage must succeed");

        assert_eq!(outcome.text, "Hello, world");
        assert_eq!(outcome.session_id.as_deref(), Some("sess-42"));
        assert_eq!(outcome.exit_code, Some(0));
        assert!(!outcome.cancelled);

        // The row closed `ok` with the full log + session id + exit code.
        let mut rows = state
            .db
            .conn()
            .query(
                "SELECT stage, status, session_id, log, exit_code, attempt, task_id, epic_id \
                 FROM agent_run WHERE task_id = 'task-1'",
                (),
            )
            .await
            .unwrap();
        let row = rows.next().await.unwrap().expect("a row was written");
        assert_eq!(row.get::<String>(0).unwrap(), "implement");
        assert_eq!(row.get::<String>(1).unwrap(), "ok");
        assert_eq!(row.get::<Option<String>>(2).unwrap().as_deref(), Some("sess-42"));
        assert_eq!(row.get::<String>(3).unwrap(), "Hello, world");
        assert_eq!(row.get::<Option<i64>>(4).unwrap(), Some(0));
        assert_eq!(row.get::<i64>(5).unwrap(), 1);
        assert_eq!(row.get::<String>(6).unwrap(), "task-1");
        assert_eq!(row.get::<Option<String>>(7).unwrap().as_deref(), Some("epic-1"));

        // Every RunEvent relayed live on `task:<id>` using the planning
        // ws_type mapping, ending in `exited`.
        let frames = collect_frames(sub).await;
        let types: Vec<&str> = frames.iter().map(|f| f["type"].as_str().unwrap()).collect();
        assert_eq!(types, vec!["started", "session", "text", "text", "exited"]);
        assert_eq!(frames[2]["payload"]["delta"], "Hello");

        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn run_agent_stage_harness_spawn_failure_still_closes_the_row() {
        // A TaskAgent whose `run` always fails to start (mimics `claude` not
        // being on PATH) — the row must still close instead of sticking
        // `running` forever.
        struct FailingAgent;
        impl TaskAgent for FailingAgent {
            fn run(
                &self,
                _req: TaskRunRequest,
            ) -> Result<(RunHandle, Receiver<RunEvent>), HarnessError> {
                Err(HarnessError::Other("boom: no claude on PATH".to_string()))
            }
        }

        let state = test_state().await;
        let err = run_agent_stage(
            &state,
            &FailingAgent,
            AgentStageParams {
                task_id: "task-1",
                epic_id: Some("epic-1"),
                attempt: 1,
            },
            TaskRunRequest {
                run_id: "run-2".to_string(),
                stage: Stage::Implement,
                prompt: "go".to_string(),
                cwd: std::env::temp_dir(),
            },
        )
        .await
        .expect_err("a spawn failure must surface as an error");
        assert!(matches!(err, AgentStageError::Harness(_)));

        let mut rows = state
            .db
            .conn()
            .query("SELECT status, log FROM agent_run WHERE task_id = 'task-1'", ())
            .await
            .unwrap();
        let row = rows.next().await.unwrap().expect("row still written");
        assert_eq!(row.get::<String>(0).unwrap(), "error");
        let log: String = row.get(1).unwrap();
        assert!(log.contains("no claude on PATH"));
    }

    /// Collect published frames from a hub subscription until an `exited`
    /// frame arrives (or a short timeout) — mirrors `planning`'s test helper.
    async fn collect_frames(
        mut rx: tokio::sync::broadcast::Receiver<std::sync::Arc<str>>,
    ) -> Vec<Value> {
        let mut frames = Vec::new();
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(5);
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
                Ok(Err(tokio::sync::broadcast::error::RecvError::Lagged(_))) => continue,
                _ => return frames,
            }
        }
    }
}
