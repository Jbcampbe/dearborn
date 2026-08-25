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
//! compile error instead of a silent contract violation. [`CliTaskAgent`]
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
//! collapsing to just the receiver. The reason is T-542: cancelling a running
//! task stage is a `RunControl::cancel()` call against the exact handle the
//! stage's run produced, held in [`AppState::cancel_registry`] keyed by the
//! claimed item's id. A `TaskAgent` that discarded its handle the way
//! `ClaudePlanningAgent` does would have made that feature structurally
//! impossible to add without changing this trait's signature later — so the
//! contract was right the first time. Every caller of [`TaskAgent::run`]
//! (today: [`run_agent_stage`] in this module) keeps the handle alive for the
//! run's whole lifetime, not just long enough to hand the receiver off — see
//! [`CancelGuard`] for exactly how [`run_agent_stage`] does that now.
//!
//! ## T-542: registering the handle for the kill (D12)
//!
//! [`run_agent_stage`] is the **one** place every agent stage's `RunHandle`
//! passes through (implement/fix/review/verify_complete/summarize alike — see
//! the module doc's "the agent seam" section), so it is also the one place
//! that needs to register that handle for a possible cancel. Immediately
//! after `agent.run(req)` succeeds, [`run_agent_stage`] builds a
//! [`CancelGuard`], which inserts the handle into
//! [`AppState::cancel_registry`] under [`cancel_registry_key`] and removes it
//! again on `Drop` — i.e. the instant the stage's evidence row closes,
//! however it closes (`ok`/`error`/`cancelled`, or a panicked drain thread).
//! This is what makes "every agent stage is cancellable without each call
//! site opting in" (this task's AC) true: `worker::process_one_task` and its
//! siblings never touch the registry themselves — they just call
//! [`run_agent_stage`] exactly as they always have.
//!
//! [`crate::lanes::set_epic_lane`] is the only thing that ever calls
//! `RunControl::cancel()` on a registered handle (an `InProgress → Cancelled`
//! lane move); see that function's own doc for why the lookup happens
//! *after* the epic's `status` column is already committed `Cancelled`, and
//! [`crate::worker`]'s own "T-542: cancellation as a kill" module-doc section
//! for what the worker does once it observes the resulting
//! `RunEvent::Exited { cancelled: true }`.
//!
//! ## T-543: agent stage timeouts (D18)
//!
//! T-542 gave every agent stage a *reachable* kill switch — something has to
//! actually call `RunControl::cancel()`. Until this task, the only caller was
//! a human, through `lanes::set_epic_lane`'s `InProgress → Cancelled` lane
//! move. D18 ("per-stage wall-clock timeouts") asks for a second caller: a
//! stage that has simply run too long, with nobody watching, has to cancel
//! *itself*. [`run_agent_stage`] is the single place to put that — it is
//! already the one choke point every agent stage's `RunHandle` passes
//! through (the section above), so wrapping the whole function's drain in a
//! deadline means no call site (`process_one_task`, `run_test_gate_loop`,
//! `run_verdict_stage`, `run_review_fix_converge`, `run_verify_complete`)
//! has to opt in, exactly the same "for free" property T-542 established for
//! the human-initiated kill.
//!
//! ### The same kill, a different caller, a different reason
//!
//! On a `DEARBORN_AGENT_STAGE_TIMEOUT_SECS` deadline, [`run_agent_stage`]
//! does not build a second cancellation mechanism — it looks the handle up
//! in [`AppState::cancel_registry`] (the exact entry its own [`CancelGuard`]
//! just inserted) and calls `.cancel()` on it, the identical call
//! `lanes::set_epic_lane` makes for a human-initiated cancel. This is D12's
//! "cancel is a kill" contract satisfied by construction: there is only ever
//! one way an agent stage's process dies before it would have exited on its
//! own, whether a human clicked Cancel or the clock ran out. What has to
//! differ is what happens *after* — a human cancel means "stop, the task is
//! resumable, put it back in `Todo`" (T-542's `handle_cancelled_task`); a
//! timeout means "this attempt failed, same as any other failed attempt"
//! (this task's AC: "an implement timeout follows the ordinary failure route,
//! not a special one"). [`AgentStageOutcome::timed_out`] is the bit that
//! carries that distinction from this function through to
//! `worker::route_stage_failure`, which checks it *before* `cancelled` for
//! exactly this reason — see that field's own doc for the full contract.
//!
//! ### Waiting for the kill to actually land, bounded
//!
//! A deadline does not simply abandon the drain and move on — that would
//! leave the killed process's output mid-flush and, worse, leave nothing
//! actually confirming the process died (the AC: "a timed-out stage must not
//! leave an orphaned `claude` process behind"). Instead [`run_agent_stage`]
//! keeps awaiting the same drain task, now racing it against
//! [`AGENT_TIMEOUT_KILL_GRACE_PERIOD`] instead of the (already-expired)
//! stage deadline — the drain finishing means the row closes with the real
//! terminal `Exited` event (a real, reaped exit, a complete log). Only if
//! *that* also elapses — `cancel()` issued, and still no `Exited` — does this
//! function give up waiting and close the row itself from whatever partial
//! `AgentStageOutcome` the drain thread had accumulated as of that moment
//! (shared via `shared_outcome`, updated after every event exactly like the
//! log-only accumulator T-512/D14 already had). This is the "decide and
//! document" case MILESTONE_2 asks for explicitly: `spawn_blocking`'s
//! `JoinHandle::abort()` cannot actually preempt a thread already inside its
//! blocking closure (draining `rx`), so there is no way to *force* the drain
//! to stop — it is left running, detached, and will finish whenever the
//! underlying channel eventually closes (whenever the process, or the
//! harness's own reader threads, eventually exit), writing to a
//! `shared_outcome` nothing downstream reads anymore.
//! [`AGENT_TIMEOUT_KILL_GRACE_PERIOD`]'s own doc has the timing rationale
//! (comfortably past `cli-stream`'s own SIGTERM→SIGKILL escalation) for why
//! this should never actually fire against a real subprocess — the true
//! backstop is `RunControl::cancel()` itself reliably killing within seconds
//! (T-542's own AC), not this grace period; this is the belt-and-suspenders
//! "must terminate regardless" guarantee for the case where that reliability
//! assumption is ever wrong (or, in this crate's own tests, deliberately
//! never satisfied — a gated `ScriptedTaskAgent` run whose gate the test
//! never releases is exactly what exercises this path).
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
//! `RunMode::Edit`, driven through the exact [`CliTaskAgent::run`] below
//! with **no changes**, wrote a file, and the write landed without any
//! approval prompt blocking the run. The reason this worked with zero
//! plumbing changes here: `agent-harness`'s Claude adapter
//! (`build_claude_args`) already injects a *default* `--permission-mode
//! acceptEdits` for `RunMode::Edit` whenever the caller hasn't set
//! `--permission-mode` itself via `extra_args` — which, at the time,
//! [`build_extra_args`] never did for `Implement`/`Fix`. `acceptEdits`
//! auto-approves `Edit`/`Write`/`MultiEdit` while leaving `Bash` gated; a
//! task whose spec is satisfiable with the editor tools alone (the T-515
//! fixture asked for exactly one new file) never even hits the gated
//! surface. The empirical run: one `claude -p` turn, no `--model`/
//! `--max-turns` override, no `--resume`, cold-CLI-start-to-exit in well
//! under 30s, one commit landing on the branch with the §2.8 subject, pushed
//! and read back from the bare-origin fixture.
//!
//! ## The `acceptEdits` caveat, retired
//!
//! T-515 left a caveat open: a task whose only path to its acceptance
//! criteria runs through `Bash` would hit `acceptEdits`' gate, and nothing
//! had exercised that. It does happen, and on the most ordinary task there
//! is — an implement stage that wants to run the project's own build/test
//! command (`cargo test`, say) to check its work before handing it to the
//! gate. Headless, there is nobody to approve it, so the command simply
//! fails.
//!
//! The fix is the one that caveat prescribed — a caller-supplied
//! `--permission-mode` override via `extra_args`, not a change to the
//! reasoning above. [`build_extra_args`] now passes `--permission-mode
//! bypassPermissions` for `Implement`/`Fix` under Claude, exactly as
//! [`crate::planning::ClaudePlanningAgent`] and
//! [`crate::breakdown::ClaudeBreakdownAgent`] already do for their own
//! headless runs; those two were the only agent seams that had it, and the
//! task stages were the odd ones out. The adapter skips its `acceptEdits`
//! default whenever the host names a mode, so this replaces that default
//! rather than duplicating the flag.
//!
//! This widens what a write stage may do unattended — `Bash` included — but
//! not what reaches `main`, because the backstops for these two stages were
//! never the permission flag. They are the structural ones the "soft
//! read-only enforcement" section already names: the `TestGate` stage and
//! the cumulative-diff review (T-522/T-530), both of which run *outside* the
//! agent and are untouched by any flag it runs under. `Implement` and `Fix`
//! are expected to write to the workspace by definition; a gate that let
//! them write but not verify what they wrote bought no safety, only a
//! blinder.
//!
//! **Left deliberately alone**: the `Review`/`VerifyComplete`/`Summarize`
//! trio runs `RunMode::Ask`, for which the adapter emits no
//! `--permission-mode` at all, so those stages meet the CLI's default mode
//! and their `Bash` stays gated the same way. If a reviewer is ever meant to
//! *run* something rather than read it, that is the same fix applied to the
//! trio — but it is a real loosening of the read-only contract above, not a
//! typo to patch, and nothing needs it today.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::mpsc::Receiver;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use harness::{Claude, Harness, HarnessError, RunEvent, RunHandle, RunMode, RunRequest, RunTuning};

use crate::agent_slot::AgentSlot;
use crate::harness_pi::{Pi, PI_EDIT_TOOLS, PI_HARNESS_ID};
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
            Stage::Setup | Stage::Preflight | Stage::TestGate | Stage::Commit | Stage::Push => None,
        }
    }

    /// Whether [`CliTaskAgent`] additionally denies edit-shaped tools for
    /// this stage (see the module doc's "soft read-only enforcement"
    /// section). Exactly the `Ask`-mode agent trio — `Implement`/`Fix` need
    /// their edit tools; non-agent stages never reach this at all.
    pub fn denies_edit_tools(self) -> bool {
        matches!(
            self,
            Stage::Review | Stage::VerifyComplete | Stage::Summarize
        )
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
    /// [`CliTaskAgent`]. Must be [`Stage::is_agent_stage`].
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
    /// The harness key this run was resolved to (T7). Validated with
    /// [`crate::agent_settings::harness_supports_slot`] by [`run_agent_stage`]
    /// before any spawn — a key this slot cannot run fails loudly there
    /// instead of silently running one CLI under another's name — and then
    /// used by [`CliTaskAgent::run`] to pick the adapter and the flag dialect.
    pub harness: String,
    /// The resolved model passed verbatim to the CLI (`RunTuning.model`);
    /// `None` → the CLI's own configured default (T7).
    pub model: Option<String>,
    /// SHA-256 hex of the **instruction portion** of `prompt` — the stage's
    /// effective prompt text *before* the context/feedback blocks are
    /// appended — written to the `agent_run` evidence row (T8). See
    /// [`crate::agent_settings::prompt_hash`].
    pub prompt_hash: String,
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
    Some(assemble_prompt_text(
        crate::spec::prompt_for(stage)?,
        context,
    ))
}

/// Assemble a full stage prompt from an **explicit** instruction text plus
/// the D8 context block — the override-aware variant of [`assemble_prompt`]
/// (T6). The caller resolves the effective instruction text via
/// [`crate::agent_settings::spawn_config`] and passes it here; composition
/// order is unchanged (instruction → `---` separator → context block), so a
/// default-resolved base produces byte-for-byte today's output.
pub fn assemble_prompt_text(base: &str, context: &crate::spec::TaskContext) -> String {
    let context_block = crate::spec::build_context(context);
    format!("{base}\n\n---\n\n{context_block}")
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
/// its agent would have. **Still unresolved as of T-531** (`worker.rs`'s
/// `run_review_fix_converge`), which wires this same function up for review
/// findings exactly as anticipated here, unchanged — a `NEEDS_CHANGES`
/// verdict is, if anything, more likely than a failing test to hinge on
/// acceptance criteria the fix agent still can't see under this contract.
/// Worth revisiting, but out of scope for the task that confirmed the gap.
pub fn assemble_fix_prompt(feedback: &str) -> String {
    let base = crate::spec::prompt_for(Stage::Fix).expect("Stage::Fix always has a prompt");
    assemble_fix_prompt_text(base, feedback)
}

/// The override-aware variant of [`assemble_fix_prompt`] (T6): an explicit
/// instruction text plus one round of feedback. Composition order unchanged
/// (instruction → `---` separator → Feedback heading), so a default-resolved
/// base produces byte-for-byte today's output.
pub fn assemble_fix_prompt_text(base: &str, feedback: &str) -> String {
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

/// Production [`TaskAgent`]: drives a coding-agent CLI through the harness
/// with the task-stage specifics — `RunMode` from [`Stage::run_mode`], denied
/// edit tools for the read-only-by-contract trio, always-fresh context (D19).
///
/// Unlike [`crate::planning::ClaudePlanningAgent`] and
/// [`crate::breakdown::ClaudeBreakdownAgent`], this one is **not** tied to a
/// single CLI: the five task-stage slots reach nothing outside their
/// workspace, so any harness Dearborn has an adapter for can run them. Which
/// one is decided per run by `req.harness` — the slot's live-resolved
/// effective harness (T7) — so a project can point `implement` at pi and
/// leave `review` on Claude without any state here changing.
#[derive(Default)]
pub struct CliTaskAgent;

impl CliTaskAgent {
    /// Construct the production agent.
    pub fn new() -> CliTaskAgent {
        CliTaskAgent
    }
}

impl TaskAgent for CliTaskAgent {
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
                extra_args: build_extra_args(req.stage, &req.harness),
                // T7: the slot's resolved model rides through to the CLI;
                // `None` keeps the CLI's own configured default exactly as
                // before configurable agents existed.
                model: req.model.clone(),
                ..RunTuning::default()
            },
            // D19: every stage is a brand new agent context, always.
            resume: None,
        };

        // The harness key is validated by `run_agent_stage` before this is
        // ever called, so an unknown key cannot reach here; Claude is the
        // defensive fallback rather than a panic, matching how an unknown
        // `RunMode` above degrades instead of aborting.
        //
        // Returned straight through — see the module doc's "why the handle
        // is returned, not dropped" section. This is the whole point of the
        // divergence from `ClaudePlanningAgent::run`.
        match req.harness.as_str() {
            PI_HARNESS_ID => Pi::new().run_channel(request),
            _ => Claude::new().run_channel(request),
        }
    }
}

/// The exact `--disallowedTools` value for the read-only-by-contract trio
/// (module doc: "soft read-only enforcement"), in Claude Code's dialect.
/// Named tools, comma-separated, matching the CLI's own flag shape (mirrors
/// how `ClaudePlanningAgent` / `ClaudeBreakdownAgent` pass `--allowedTools`).
const DENIED_EDIT_TOOLS: &str = "Edit,Write,MultiEdit,NotebookEdit";

/// The `--permission-mode` the write stages run under, in Claude Code's
/// dialect. The adapter's own default when the host names no mode is
/// `acceptEdits`, which auto-approves the editor tools but leaves `Bash`
/// gated — an approval nobody can grant in a headless `claude -p` run. This
/// is the same value `ClaudePlanningAgent` / `ClaudeBreakdownAgent` already
/// pass for the same headless reason.
const BYPASS_PERMISSIONS: &str = "bypassPermissions";

/// Build the harness `extra_args` for `stage` under `harness`. Factored out
/// of [`CliTaskAgent::run`] as a pure function (no harness, no spawn) so the
/// (stage, harness) → flags mapping is unit-tested directly instead of only
/// through a live CLI run.
///
/// Two stage-driven flags, on opposite sides of the write/read-only line.
///
/// The read-only trio's edit-tool denial, which each CLI spells differently —
/// `--disallowedTools Edit,Write,…` for Claude Code, `--exclude-tools
/// edit,write` for pi (whose built-in tool set is `read`/`bash`/`edit`/
/// `write`, so the list is two names, not four). The *contract* is identical
/// in both dialects, including its limit: `Bash` is deliberately not denied
/// under either, so this stays the same "soft read-only enforcement" the
/// module doc describes rather than a guarantee.
///
/// The write pair's `--permission-mode bypassPermissions`, Claude-only. It
/// displaces the adapter's `acceptEdits` default (last-wins, and the adapter
/// skips its own default entirely once the host names a mode, so the flag is
/// never duplicated) so an `Implement`/`Fix` run can reach `Bash` and check
/// its own work — the module doc's "acceptEdits caveat, retired" section has
/// the full reasoning. pi needs no counterpart: `build_pi_args` already
/// passes `--no-approve` and pi auto-runs every tool in print mode, so its
/// write stages never meet an approval gate and there is no
/// `--permission-mode` in its dialect to set.
fn build_extra_args(stage: Stage, harness: &str) -> Vec<String> {
    let is_pi = harness == PI_HARNESS_ID;
    if stage.denies_edit_tools() {
        return if is_pi {
            vec!["--exclude-tools".to_string(), PI_EDIT_TOOLS.to_string()]
        } else {
            vec![
                "--disallowedTools".to_string(),
                DENIED_EDIT_TOOLS.to_string(),
            ]
        };
    }
    // Matched on the two write stages by name rather than on
    // `!denies_edit_tools()`, which is also true of the five non-agent
    // stages — those never reach a CLI at all, and must not pick up a flag
    // on the way past.
    if matches!(stage, Stage::Implement | Stage::Fix) && !is_pi {
        return vec![
            "--permission-mode".to_string(),
            BYPASS_PERMISSIONS.to_string(),
        ];
    }
    Vec::new()
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

/// Identifies the stage row to open + the WS topic to relay on.
pub struct AgentStageParams<'a> {
    /// `None` for a stage that has no task to attach to — as of T-560, this
    /// is exactly one case: an epic-scoped `Stage::Summarize` run over the
    /// epic's whole cumulative diff (`worker::finalize_epic`), which
    /// describes the epic as a whole rather than any single task. Every
    /// other agent stage (`implement`/`fix`/`review`/`verify_complete`, and a
    /// *standalone* task's own `Stage::Summarize` run — see
    /// `worker::finalize_task`) is `Some(_)`, exactly as this field always
    /// was before T-560 widened it from a bare `&'a str`. See
    /// [`run_agent_stage`]'s own doc for how a `None` here changes the
    /// `agent_run.task_id` column and the streamed WS topic.
    pub task_id: Option<&'a str>,
    /// `None` for a standalone task (D17), or for an epic-scoped
    /// `Stage::Summarize` run's task-less counterpart above — `epic_id` and
    /// `task_id` are never *both* `None` (see [`cancel_registry_key`]'s own
    /// doc, which leans on that same invariant).
    pub epic_id: Option<&'a str>,
    /// `agent_run.attempt` — 1 for the first try at this stage, bumped by
    /// the caller on a retry/fix round.
    pub attempt: i64,
}

impl AgentStageParams<'_> {
    /// The WS topic [`run_agent_stage`] relays this stage's `RunEvent`s on
    /// (D14): `task:<task_id>` when there is a task, the same as every stage
    /// before T-560. An epic-scoped `Stage::Summarize` run has no task, so it
    /// falls back to `epic:<epic_id>` — the same topic
    /// [`crate::planning::ClaudePlanningAgent`]'s own epic-chat `RunEvent`
    /// stream already uses (T-202) for the identical reason: a run that
    /// belongs to the epic as a whole, not one task, streams on the epic's
    /// own coarse topic rather than inventing a task-shaped one for a
    /// non-existent task. Panics if both ids are `None` — see
    /// [`cancel_registry_key`]'s doc for why that combination is a caller bug,
    /// not a case to handle quietly.
    fn stream_topic(&self) -> String {
        match self.task_id {
            Some(task_id) => format!("task:{task_id}"),
            None => format!(
                "epic:{}",
                self.epic_id
                    .expect("AgentStageParams must set at least one of epic_id/task_id")
            ),
        }
    }
}

// ---- T-542: the cancel registry (D12) --------------------------------------

/// The type [`AppState::cancel_registry`] wraps in an `Arc` — a plain
/// `Mutex<HashMap<..>>`, the exact shape [`AppState::inflight`] and
/// [`AppState::refresh_locks`] already use for a short-held, never-held-
/// across-`.await` map guard (see those fields' own docs for the idiom this
/// follows). Named here, next to [`RunHandle`]'s own import, rather than
/// spelled out inline on the `AppState` field, so the concrete type is
/// stated once.
pub type CancelRegistry = Mutex<HashMap<String, RunHandle>>;

/// The id [`AppState::cancel_registry`] keys a stage's entry under: the epic
/// id when the stage belongs to one (an epic's DAG walk), the task id
/// otherwise (`epic_id: None` — a standalone claim, T-551). This is
/// deliberately "whatever id the claimed item has," not "task id"
/// specifically or "epic id" specifically — it is the same id
/// `worker::WorkItem::Epic(id) | WorkItem::Standalone(task_id)` carries via
/// `WorkItem::id()` (T-550). T-551 confirmed the bet this doc made before a
/// standalone claim ever drove an agent stage: `worker::process_one_task`
/// passes `AgentStageParams { epic_id: task.epic_id.as_deref(), .. }`
/// unchanged for both an epic-owned and a standalone task, and this function
/// needed no adaptation at all to key a standalone stage's registry entry by
/// its task id.
///
/// T-560 widened `task_id` itself to `Option<&str>` (an epic-scoped
/// `Stage::Summarize` run has no task at all — see [`AgentStageParams::task_id`]'s
/// own doc) but changed nothing about the *key* this function picks: an
/// epic-scoped summarize run still has `epic_id: Some(_)`, so it keys under
/// the epic id exactly like every other stage in that epic's walk, and a
/// standalone task's own summarize run still has `epic_id: None, task_id:
/// Some(_)`, keying under the task id exactly like every other standalone
/// stage. The `.expect` below is the one new thing: it encodes "at least one
/// of the two is always `Some`" as a panic on the bug it would otherwise
/// paper over, rather than silently keying on an empty string.
pub(crate) fn cancel_registry_key<'a>(params: &AgentStageParams<'a>) -> &'a str {
    params
        .epic_id
        .or(params.task_id)
        .expect("AgentStageParams must set at least one of epic_id/task_id")
}

/// RAII guard for one entry in [`AppState::cancel_registry`] (T-542, D12).
/// Constructing a guard inserts `key -> handle`; `Drop` removes exactly that
/// key. This is the structural guarantee behind this task's AC "the registry
/// entry is removed on every exit path": [`run_agent_stage`] already has
/// several early `return`s (a harness spawn failure, a panicked drain
/// thread) plus its own two ordinary completion paths (an outcome that is
/// `ok`, one that isn't) — a guard makes every one of them, including any a
/// future author adds, correct by construction instead of by remembering to
/// clean up at each return site.
///
/// ## The 1:1 assumption
///
/// At most one agent stage per claimed item is ever in flight: the DAG walk
/// fully serializes (MILESTONE_2 §2.3 — one task `InProgress` at a time, one
/// stage awaited to completion before the next begins), so a second entry
/// for the same key can never legitimately exist while the first is still
/// live. This guard leans on that invariant directly — it unconditionally
/// `insert`s on construction (silently overwriting anything already at that
/// key, which should never happen) and unconditionally `remove`s the same
/// key on drop (which would incorrectly remove a *different* guard's live
/// entry if the invariant were ever violated). Flagged here, not defended
/// against, because defending against it would mean ref-counting entries
/// that are never supposed to collide in the first place; a future author
/// changing the concurrency model (e.g. running more than one stage per
/// claimed item concurrently) needs to see this assumption before breaking
/// it, not have it silently paper over the bug.
struct CancelGuard {
    registry: Arc<CancelRegistry>,
    key: String,
}

impl CancelGuard {
    /// Insert `handle` under `key` and hand back the guard that will remove
    /// it again on drop. The handle lives inside the registry's map for the
    /// guard's whole lifetime — this is also what satisfies "the handle is
    /// held for the entire drain" (the module doc's "why the handle is
    /// returned, not dropped" section): [`run_agent_stage`] no longer needs
    /// a separate local binding just to keep the handle alive, because the
    /// registry itself now owns it until this guard drops.
    fn new(registry: Arc<CancelRegistry>, key: String, handle: RunHandle) -> CancelGuard {
        registry
            .lock()
            .expect("cancel_registry mutex poisoned")
            .insert(key.clone(), handle);
        CancelGuard { registry, key }
    }
}

impl Drop for CancelGuard {
    fn drop(&mut self) {
        if let Ok(mut map) = self.registry.lock() {
            map.remove(&self.key);
        }
    }
}

/// One tool call observed during a stage's `RunEvent` stream, accumulated
/// by [`AgentStageOutcome::absorb`] so the stage's `agent_run_events` rows
/// can be persisted after the drain finishes (best-effort — a persistence
/// failure must never fail the stage itself). Mirrors the wire shape of
/// [`RunEvent::ToolStart`] / [`RunEvent::ToolEnd`] minus run bookkeeping
/// (`run_id`) and payloads we don't store (`input` / `output`).
#[derive(Debug, Clone)]
pub struct ToolEventRecord {
    /// `"tool_start"` or `"tool_end"`, matching the WS frame type the
    /// planning view already speaks.
    pub kind: &'static str,
    /// Harness id pairing a `tool_start` with its `tool_end`.
    pub tool_call_id: String,
    /// The tool's name (`"tool_start"` only; empty string for
    /// `"tool_end"`, which carries `ok` instead).
    pub name: String,
    /// `None` for `tool_start`; `Some(ok)` for `tool_end`.
    pub ok: Option<bool>,
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
    /// T-543: `true` when this stage's `cancelled` came from
    /// `DEARBORN_AGENT_STAGE_TIMEOUT_SECS` firing, not a user-initiated
    /// `POST /epics/{id}/lane` cancel (T-542). Both paths kill the stage
    /// through the identical `RunControl::cancel()` call (D12), so
    /// `cancelled` alone can't tell them apart — which is exactly why
    /// `agent_run.status` needs a value distinct from `cancelled`
    /// (§2.1/§2.2's `timeout`), and why `worker::route_stage_failure`
    /// checks this field *before* `cancelled` to decide whether a not-`ok`
    /// outcome takes T-540's ordinary failure route
    /// (`Failed(FailureReason::Timeout)`, this task's AC — "an implement
    /// timeout follows the ordinary failure route, not a special one") or
    /// T-542's `Todo`-resetting cancel route. Set by [`run_agent_stage`]
    /// itself (see its own "T-543" doc section) — never by
    /// [`AgentStageOutcome::absorb`], which only sees the harness's own
    /// `RunEvent` stream and has no notion of *why* a cancel happened.
    pub timed_out: bool,
    /// Whether an `Error` event was seen anywhere in the stream.
    ///
    /// Diagnostic only: a stream can carry an `Error` event and still finish
    /// cleanly (a provider 429 mid-run that pi's harness retried through,
    /// followed by a normal completion), so this flag deliberately does
    /// *not* force [`Self::status`] to `"error"` on its own — see that
    /// method's doc for the final-state rule that actually decides the
    /// stage. Kept public so callers logging/routing a failure can tell
    /// "the agent saw a provider error" apart from a bare non-zero exit,
    /// and so [`Self::last_error_message`] has a cheap companion check.
    pub errored: bool,
    /// The message carried by the most recently absorbed `Error` event, if
    /// any (`None` when none was seen). Only the *last* one is kept because
    /// the classification question is always about the run's final state:
    /// earlier errors were survived (they are all still present verbatim in
    /// [`Self::text`] as `[error] …` lines for the log trail); what a caller
    /// diagnosing a failed stage wants is the error closest to whatever
    /// terminal condition made the stage fail. Recorded by
    /// [`AgentStageOutcome::absorb`] alongside `errored`, which only sees
    /// the harness's own event stream and therefore cannot know whether an
    /// individual `Error` turned out to be transient — the final-state call
    /// belongs to [`Self::status`].
    pub last_error_message: Option<String>,
    /// The `agent_run.id` this outcome's row was opened/closed under (T-530).
    /// [`run_agent_stage`] has already closed the row (with `verdict: None`)
    /// by the time it returns this outcome — a review/verify-complete
    /// caller that parses [`Self::text`] for a `VERDICT:` line only learns
    /// the verdict *after* the row is closed, so it needs this id to go back
    /// and set the column via [`crate::evidence::set_verdict`] rather than
    /// threading the answer through `CloseStage` (there is no open
    /// `StageHandle` left by then). Empty for the zero-value
    /// [`Default::default`] used only inside the drain closure before this
    /// field is populated.
    pub agent_run_id: String,
    /// Every `ToolStart`/`ToolEnd` event seen in the stream, in arrival
    /// order — recorded by [`AgentStageOutcome::absorb`] alongside `text`
    /// so [`run_agent_stage`] can persist them into `agent_run_events`
    /// once the drain completes. Empty when the stage never touched a
    /// tool, in which case zero rows are written.
    pub tool_events: Vec<ToolEventRecord>,
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
                // The log trail keeps every error verbatim (the row's `log`
                // column and any live WS subscriber both render from
                // `text`), while `last_error_message` holds just the latest
                // one for programmatic diagnosis — see its field doc.
                self.last_error_message = Some(message.clone());
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
            // Tool calls are kept out of `text` (the log trail stays pure
            // prose); they accumulate here instead so they can land in
            // `agent_run_events` after the drain — see `tool_events`.
            RunEvent::ToolStart {
                tool_call_id,
                name,
                ..
            } => self.tool_events.push(ToolEventRecord {
                kind: "tool_start",
                tool_call_id: tool_call_id.clone(),
                name: name.clone(),
                ok: None,
            }),
            RunEvent::ToolEnd {
                tool_call_id,
                ok,
                ..
            } => self.tool_events.push(ToolEventRecord {
                kind: "tool_end",
                tool_call_id: tool_call_id.clone(),
                name: String::new(),
                ok: Some(*ok),
            }),
            _ => {}
        }
    }

    /// The §2.1 terminal `agent_run.status` this outcome implies, judged by
    /// the run's **final** state rather than by any single event seen along
    /// the way. `timed_out` is checked *before* `cancelled` (T-543): both are
    /// `true` for a deadline-killed stage (see [`Self::timed_out`]'s own doc
    /// — the kill mechanism is identical, so `cancelled` is always set
    /// alongside it), but `"timeout"` is the more precise, more actionable
    /// status for a human reading `agent_run.status` — `"cancelled"` reads
    /// as "a human asked to stop," which is specifically *not* what happened
    /// here.
    ///
    /// An `Error` event in the stream is likewise not automatically terminal:
    /// harnesses recover from provider transients (e.g. a mid-run HTTP 429
    /// rate limit that the agent CLI retries through and then completes
    /// normally), so a run that ends with a clean exit **and** non-empty
    /// output text is `"ok"` even though [`Self::errored`] is set — failing
    /// such a stage would discard a fix the agent fully completed. An error
    /// only condemns the stage when the run did *not* demonstrably recover:
    /// either the exit was non-zero/cancelled, or it exited 0 without having
    /// produced any text at all (a silent failure must still fail, since
    /// downstream stages would have nothing to act on). Runs that never saw
    /// an error keep exactly the old rule — exit 0 is `"ok"`, anything else
    /// is `"error"`. Note that review/verify-complete verdict parsing reads
    /// [`Self::text`] after close, which this method never touches, so that
    /// flow is unaffected either way.
    fn status(&self) -> &'static str {
        if self.timed_out {
            "timeout"
        } else if self.cancelled {
            "cancelled"
        } else if self.errored {
            if self.exit_code == Some(0) && !self.text.is_empty() {
                "ok"
            } else {
                "error"
            }
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
    /// *decision* is made once, here, under [`Self::status`]'s final-state
    /// rule (so e.g. `errored == true` alone does not make this `false`: a
    /// run that recovered from a transient provider error and exited 0 with
    /// real output counts as complete, and the DAG walk proceeds to commit
    /// the work the agent actually finished).
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
    /// The run's resolved harness cannot run this stage's slot (T7): either
    /// Dearborn has no adapter for it, or the slot needs a capability the
    /// harness lacks. Checked **before** any row is opened — such a request is
    /// a configuration bug the settings API should have made unreachable (only
    /// enabled, slot-compatible harnesses are selectable), so there is no
    /// stage attempt to record evidence for. Carries the slot as well as the
    /// key so [`crate::agent_settings::unsupported_harness_message`] can say
    /// *which* of the two reasons applies.
    UnsupportedHarness {
        harness: String,
        slot: Option<AgentSlot>,
    },
}

impl std::fmt::Display for AgentStageError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AgentStageError::Db(e) => write!(f, "agent_run row error: {e}"),
            AgentStageError::Harness(e) => write!(f, "agent stage failed to start: {e}"),
            AgentStageError::DrainFailed(msg) => write!(f, "{msg}"),
            AgentStageError::UnsupportedHarness { harness, slot } => match slot {
                Some(slot) => f.write_str(&crate::agent_settings::unsupported_harness_message(
                    harness, *slot,
                )),
                // No slot means a non-agent stage reached an agent-only path —
                // a caller bug, phrased as one rather than blamed on the key.
                None => write!(
                    f,
                    "harness `{harness}` was resolved for a stage that has no agent slot"
                ),
            },
        }
    }
}

impl std::error::Error for AgentStageError {}

/// Drive one agent stage to completion: open its `agent_run` row, start the
/// run through `agent`, relay every `RunEvent` live on `task:<task_id>` — or,
/// T-560, `epic:<epic_id>` when `params.task_id` is `None` (see
/// [`Self::stream_topic`]'s own doc for why) — (D14, reusing
/// [`crate::planning::ws_type`]), flush the accumulating log to the row
/// roughly every [`PARTIAL_FLUSH_INTERVAL`] while it streams, then close the
/// row with the terminal status/session id/exit code/log (D13 capped).
/// Returns the assembled [`AgentStageOutcome`] so a caller decides what
/// happens next.
///
/// The [`RunHandle`] `agent.run` returns is held for the **entire** drain via
/// a [`CancelGuard`] (T-542, D12): it lives inside
/// [`AppState::cancel_registry`], keyed by [`cancel_registry_key`], from
/// immediately after `agent.run` succeeds until this function returns —
/// every exit path drops the guard, which removes the entry, so a stage that
/// finishes (`ok` or otherwise), errors, or panics its drain thread always
/// leaves the registry exactly as it found it. [`crate::lanes::set_epic_lane`]
/// is the only external caller that ever reads this registry.
pub async fn run_agent_stage(
    state: &AppState,
    agent: &dyn TaskAgent,
    params: AgentStageParams<'_>,
    req: TaskRunRequest,
) -> Result<AgentStageOutcome, AgentStageError> {
    let conn = state.db.conn();
    // T7 spawn-validation, before anything else: a harness key this stage's
    // slot cannot run means settings were hand-edited into a state the API
    // refuses to create. Fail loudly; no `agent_run` row is opened because no
    // run was attempted. A non-agent stage has no slot, and cannot legally
    // reach here at all — `AgentSlot::from_stage` returning `None` is the
    // caller bug `run_mode()` already guards, so it is treated as unrunnable
    // rather than waved through.
    let slot = AgentSlot::from_stage(req.stage);
    if !slot.is_some_and(|slot| crate::agent_settings::harness_supports_slot(&req.harness, slot)) {
        return Err(AgentStageError::UnsupportedHarness {
            harness: req.harness.clone(),
            slot,
        });
    }
    let stage_str = req.stage.as_str();
    let open = OpenStage {
        task_id: params.task_id,
        epic_id: params.epic_id,
        stage: stage_str,
        attempt: params.attempt,
        // T8 evidence: which harness/model produced this run, and the hash of
        // the instruction prompt it was given (before context/feedback).
        harness: Some(req.harness.as_str()),
        model: req.model.as_deref(),
        prompt_hash: Some(req.prompt_hash.as_str()),
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
    // T-542: register the handle for a possible external cancel (D12) — held
    // in `state.cancel_registry` across the whole drain below, removed on
    // every exit path when `_cancel_guard` drops. See `CancelGuard`'s own
    // doc and the module doc's "T-542: registering the handle for the kill"
    // section.
    let _cancel_guard = CancelGuard::new(
        state.cancel_registry.clone(),
        cancel_registry_key(&params).to_string(),
        run_handle,
    );

    let hub = state.hub.clone();
    let topic = params.stream_topic();

    // Shared with the periodic-flush task below AND (T-543) with the
    // deadline-timeout path a few lines down: the blocking drain thread
    // writes its latest accumulated *outcome* here after absorbing every
    // event, not just the log text — [`AgentStageOutcome`] is a cheap
    // `#[derive(Clone)]` (a `String` plus a handful of small fields), so
    // sharing the whole thing costs nothing extra and means a deadline that
    // gives up waiting on the drain (below) has the same session id/exit
    // state a normal close would, not just the text. A plain
    // `std::sync::Mutex` is fine — every side only ever holds it for a
    // clone/replace, never across an `.await`.
    let shared_outcome = std::sync::Arc::new(std::sync::Mutex::new(AgentStageOutcome::default()));
    let shared_outcome_writer = shared_outcome.clone();

    let mut drain_task = tokio::task::spawn_blocking(move || {
        let mut outcome = AgentStageOutcome::default();
        for event in rx {
            let payload = serde_json::to_value(&event).unwrap_or(Value::Null);
            hub.publish(&topic, crate::planning::ws_type(&event), payload);
            outcome.absorb(&event);
            *shared_outcome_writer
                .lock()
                .expect("shared_outcome mutex poisoned") = outcome.clone();
        }
        outcome
    });

    // The D14 partial-flush loop: while the drain above is in flight, copy
    // the shared accumulator into the `agent_run` row every
    // `PARTIAL_FLUSH_INTERVAL`. Runs as its own task (not inside the blocking
    // drain) because flushing is an async DB write and the drain thread is a
    // plain blocking one — this avoids reaching for `Handle::block_on` from
    // inside `spawn_blocking`, which works but is a subtler pattern than two
    // independent tasks sharing a `Mutex`. Stopped via `abort()` the instant
    // the drain finishes (or T-543's deadline gives up on it); the *final*
    // close below writes the complete, un-raced log regardless of this
    // loop's last tick.
    let flush_conn = conn.clone();
    let flush_shared = shared_outcome.clone();
    let flush_row_id = stage_row.id.clone();
    let flush_handle = tokio::spawn(async move {
        let mut interval = tokio::time::interval(PARTIAL_FLUSH_INTERVAL);
        interval.tick().await; // first tick fires immediately; skip so the
                               // first *real* flush is ~PARTIAL_FLUSH_INTERVAL in.
        loop {
            interval.tick().await;
            let snapshot = flush_shared
                .lock()
                .expect("shared_outcome mutex poisoned")
                .text
                .clone();
            let handle = StageHandle {
                id: flush_row_id.clone(),
                started_at: stage_row.started_at,
            };
            let _ = evidence::flush_stage_log(&flush_conn, &handle, &snapshot).await;
        }
    });

    // ---- T-543: DEARBORN_AGENT_STAGE_TIMEOUT_SECS, enforced through the
    // exact same RunControl::cancel() T-542 wired up (D12) — see this
    // module's "T-543: agent stage timeouts" doc section (top of file) for
    // the full design. In short: race the drain against the deadline; on a
    // deadline, look the handle up in the registry `_cancel_guard` above
    // just populated and call `cancel()` on it (the identical call
    // `lanes::set_epic_lane`'s user-initiated cancel makes), then keep
    // waiting — bounded by `AGENT_TIMEOUT_KILL_GRACE_PERIOD` — for the drain
    // to actually finish, so the row closes with a complete partial log and
    // the process is genuinely reaped rather than abandoned mid-flight.
    let stage_timeout = Duration::from_secs(state.config.executor.agent_stage_timeout_secs);
    let mut deadline_fired = false;

    let drained = match tokio::time::timeout(stage_timeout, &mut drain_task).await {
        Ok(res) => res,
        Err(_elapsed) => {
            deadline_fired = true;
            tracing::warn!(
                epic = ?params.epic_id,
                task = ?params.task_id,
                stage = stage_str,
                timeout_secs = state.config.executor.agent_stage_timeout_secs,
                "agent stage exceeded DEARBORN_AGENT_STAGE_TIMEOUT_SECS; cancelling"
            );
            let cancel_result = state
                .cancel_registry
                .lock()
                .expect("cancel_registry mutex poisoned")
                .get(cancel_registry_key(&params))
                .map(|h| h.cancel());
            if let Some(Err(err)) = cancel_result {
                tracing::warn!(
                    epic = ?params.epic_id,
                    task = ?params.task_id,
                    stage = stage_str,
                    error = %err,
                    "agent stage timeout: cancel() itself returned an error"
                );
            }

            match tokio::time::timeout(AGENT_TIMEOUT_KILL_GRACE_PERIOD, &mut drain_task).await {
                Ok(res) => res,
                Err(_) => {
                    // `cancel()` did not produce an `Exited` event within the
                    // grace period. Documented, not a bug: `spawn_blocking`
                    // gives no way to preempt an in-progress blocking
                    // closure, so there is no way to *force* the drain
                    // thread to stop here — it is left running, detached,
                    // and will finish (writing nothing further anyone reads)
                    // whenever the underlying process/channel eventually
                    // does exit. This call stops waiting and closes the row
                    // itself from the last outcome snapshot the drain
                    // thread wrote — the "do not hang forever" requirement
                    // wins over holding out for a terminal event that may
                    // never come from a stuck handle.
                    tracing::warn!(
                        epic = ?params.epic_id,
                        task = ?params.task_id,
                        stage = stage_str,
                        grace_period_secs = AGENT_TIMEOUT_KILL_GRACE_PERIOD.as_secs(),
                        "agent stage timeout: cancel() did not produce Exited within the grace \
                         period; closing the row from the last known partial outcome and moving on"
                    );
                    Ok(shared_outcome
                        .lock()
                        .expect("shared_outcome mutex poisoned")
                        .clone())
                }
            }
        }
    };

    flush_handle.abort();

    let mut outcome = match drained {
        Ok(mut outcome) => {
            // Stamp the row id now that we have it — see the field's own doc
            // for why this can't ride through `CloseStage` instead (T-530).
            outcome.agent_run_id = stage_row.id.clone();
            outcome
        }
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

    // T-543: a stage that did not finish within its deadline is a *timeout*,
    // full stop — even in the (vanishingly unlikely, given `cancel()`'s own
    // sub-two-second kill latency) case where the grace-period wait above
    // caught a real `Exited` whose own `cancelled` flag was already `true`
    // for the ordinary reason (it *is* true: `cancel()` is what produced
    // it). Setting both fields unconditionally here — rather than trying to
    // distinguish "genuinely timed out" from "would have exited a moment
    // later anyway" — is what makes `AgentStageOutcome::status` and
    // `worker::route_stage_failure` able to treat "the deadline fired" as
    // the single source of truth for `timeout` vs `cancelled`, instead of
    // reconstructing it from a race that has no meaningfully "correct" other
    // answer (see the module doc section for the full rationale).
    if deadline_fired {
        outcome.timed_out = true;
        outcome.cancelled = true;
    }

    // TODO(tool-events epic): once `evidence::save_run_events` exists
    // (sibling task "Add save_run_events and list_run_events to evidence.rs",
    // alongside migration 0009's `agent_run_events` table), persist this
    // stage's accumulated tool events here, best-effort, BEFORE close_stage:
    //
    //     evidence::save_run_events(conn, &stage_row.id, &outcome.tool_events).await;
    //
    // Best-effort per the spec: ignore/log any error rather than failing
    // the stage — the row close below must always run. Note the existing
    // `evidence` fns take `&Connection`, not `&DbConn`. It cannot be called
    // today because that function does not exist yet in this branch, and
    // referencing it would break `cargo check`.

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

/// T-543: how long [`run_agent_stage`] keeps waiting for the drain to
/// actually finish *after* it has already called `RunControl::cancel()` on a
/// deadline. Not zero — the whole point of routing the deadline through the
/// same kill T-542 built (rather than just abandoning the drain outright) is
/// to get a complete partial log and a reaped process, and both need the
/// drain to actually observe the terminal `Exited` event. Not unbounded
/// either — this task's own "do not hang forever" requirement. `claude`
/// (and, in the harness's own `cli-stream` engine, every adapter's process
/// handle) escalates `SIGTERM` to `SIGKILL` after 1.5s if the process hasn't
/// exited on its own; this grace period is comfortably longer than that
/// escalation plus reader-thread teardown and the event's trip back through
/// this stage's channel, so a real subprocess should essentially never hit
/// it. It exists as the documented backstop for a handle that, for whatever
/// reason, never reports `Exited` at all (e.g. a stuck harness, or — in this
/// crate's own tests — a `testing::ScriptedTaskAgent` gated run whose gate
/// the test deliberately never releases).
const AGENT_TIMEOUT_KILL_GRACE_PERIOD: Duration = Duration::from_secs(5);

// ---- test doubles (crate-visible so worker/DAG-walk tests can inject them,
// T-513+) --------------------------------------------------------------------
//
// Gated `#[cfg(test)]` + `pub(crate)`, mirroring `planning::testing` /
// `breakdown::testing` exactly: every existing agent seam's fake lives this
// way, reachable from any unit test in this crate (including a future
// `worker.rs` test module), but invisible to the separate `tests/*.rs`
// integration-test crate — those drive the real `CliTaskAgent` instead
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

    /// The unscripted fallback for `stage` — [`ScriptedRun::default`] for
    /// every stage except the two verdict-emitting ones,
    /// [`Stage::Review`] and [`Stage::VerifyComplete`] (T-530): a bare "ok"
    /// text has no `VERDICT:` line, so a `worker.rs` test that doesn't care
    /// about a verdict stage's own behavior (the overwhelming majority —
    /// every T-513/T-522 test predates T-530 and asserts nothing about
    /// review) would otherwise hit a contract-miss retry and then
    /// `Failed(agent_error)` on every single walk that reaches that stage,
    /// purely as a side effect of the stage existing. Defaulting an
    /// unscripted verdict-stage run to a clean `VERDICT: PASS` keeps "a
    /// stage nobody scripted is a no-op success" true in the sense that
    /// actually matters for these stages — the walk proceeds — while a test
    /// that *does* care (this module's own T-530 tests, or T-532's
    /// `VerifyComplete` tests once they land) still overrides it with
    /// `.script(stage, ...)` exactly like any other stage.
    fn default_script_for(stage: Stage) -> ScriptedRun {
        match stage {
            Stage::Review | Stage::VerifyComplete => ScriptedRun {
                text: vec!["Reviewed; nothing outstanding.\n\nVERDICT: PASS".to_string()],
                ..ScriptedRun::default()
            },
            _ => ScriptedRun::default(),
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
        scripts:
            Mutex<std::collections::HashMap<&'static str, std::collections::VecDeque<ScriptedRun>>>,
        recorded: Arc<Mutex<Vec<RecordedTaskRun>>>,
        gate: Option<Arc<Gate>>,
        /// T-543: when `Some`, `gate` (above) only pins a run whose
        /// `req.stage` matches — every other stage runs ungated. `None`
        /// (the default, and [`with_gate`](ScriptedTaskAgent::with_gate)'s
        /// behavior, unchanged) means `gate` pins **every** stage's run, as
        /// it always has. Exists so a test can drive a multi-stage walk
        /// (e.g. `Implement` writes a file and commits normally, then
        /// `Review` hangs) instead of every gated test necessarily being
        /// about the *first* stage a walk reaches.
        gate_stage: Option<Stage>,
    }

    impl Default for ScriptedTaskAgent {
        fn default() -> ScriptedTaskAgent {
            ScriptedTaskAgent {
                scripts: Mutex::new(std::collections::HashMap::new()),
                recorded: Arc::new(Mutex::new(Vec::new())),
                gate: None,
                gate_stage: None,
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

        /// Attach a gate that pins **every** run in-flight (before its
        /// terminal `Exited`) until released — lets a test hold a stage in
        /// flight deterministically to exercise cancellation or a mid-run WS
        /// join. See [`with_gate_on`](ScriptedTaskAgent::with_gate_on) for
        /// the stage-scoped variant.
        pub fn with_gate(mut self, gate: Arc<Gate>) -> ScriptedTaskAgent {
            self.gate = Some(gate);
            self.gate_stage = None;
            self
        }

        /// Like [`with_gate`](ScriptedTaskAgent::with_gate), but only pins
        /// runs at `stage` — every other stage's run proceeds to `Exited`
        /// normally (per its own script/default). T-543's timeout tests use
        /// this to prove a *later* stage's deadline (e.g. `Fix` inside the
        /// test-gate loop, or `Review`) is handled exactly like `Implement`'s
        /// without also gating every earlier stage the walk has to pass
        /// through first.
        pub fn with_gate_on(mut self, stage: Stage, gate: Arc<Gate>) -> ScriptedTaskAgent {
            self.gate = Some(gate);
            self.gate_stage = Some(stage);
            self
        }

        /// Handle to the recorded runs (for assertions on stage/prompt/cwd).
        pub fn recorded(&self) -> Arc<Mutex<Vec<RecordedTaskRun>>> {
            self.recorded.clone()
        }
    }

    impl TaskAgent for ScriptedTaskAgent {
        fn run(
            &self,
            req: TaskRunRequest,
        ) -> Result<(RunHandle, Receiver<RunEvent>), HarnessError> {
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
                .unwrap_or_else(|| default_script_for(req.stage));

            let (tx, rx) = std::sync::mpsc::channel();
            let run_id = req.run_id;
            let cwd = req.cwd;
            // T-543: honor `gate_stage` — a gate applies to this run only
            // when no stage filter was set (`with_gate`) or the filter names
            // this exact stage (`with_gate_on`).
            let gate = match self.gate_stage {
                Some(only) if only != req.stage => None,
                _ => self.gate.clone(),
            };
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
    fn implement_and_fix_bypass_permissions_on_claude_and_need_no_flag_on_pi() {
        for stage in [Stage::Implement, Stage::Fix] {
            // Claude: the mode is named explicitly, displacing the adapter's
            // `acceptEdits` default so the stage can reach `Bash` and run
            // the project's own build/test command.
            let claude = build_extra_args(stage, "claude");
            let idx = claude
                .iter()
                .position(|a| a == "--permission-mode")
                .unwrap_or_else(|| panic!("{stage:?} missing --permission-mode: {claude:?}"));
            assert_eq!(claude[idx + 1], BYPASS_PERMISSIONS);
            // Never the read-only trio's denial: a write stage keeps its
            // editor tools.
            assert!(
                !claude.iter().any(|a| a == "--disallowedTools"),
                "{stage:?}: {claude:?}"
            );

            // pi: `build_pi_args` already passes `--no-approve` and pi
            // auto-runs every tool in print mode, so there is no gate to
            // open and no `--permission-mode` in its dialect to open it
            // with. Claude's flag must not leak into pi's argv.
            let pi = build_extra_args(stage, PI_HARNESS_ID);
            assert!(pi.is_empty(), "{stage:?} on pi: {pi:?}");
        }
    }

    #[test]
    fn non_agent_stages_get_no_extra_args_on_any_harness() {
        // `build_extra_args` keys the write-stage flag on `Implement`/`Fix`
        // by name, not on `!denies_edit_tools()` — which is also true of
        // these five. They never reach a CLI, so they must stay bare.
        for stage in [
            Stage::Setup,
            Stage::Preflight,
            Stage::TestGate,
            Stage::Commit,
            Stage::Push,
        ] {
            for harness in ["claude", PI_HARNESS_ID] {
                let args = build_extra_args(stage, harness);
                assert!(args.is_empty(), "{stage:?} on {harness}: {args:?}");
            }
        }
    }

    #[test]
    fn review_family_gets_the_denied_edit_tools_in_each_harness_dialect() {
        for stage in [Stage::Review, Stage::VerifyComplete, Stage::Summarize] {
            let claude = build_extra_args(stage, "claude");
            let idx = claude
                .iter()
                .position(|a| a == "--disallowedTools")
                .unwrap_or_else(|| panic!("{stage:?} missing --disallowedTools: {claude:?}"));
            assert_eq!(claude[idx + 1], DENIED_EDIT_TOOLS);

            // Same contract, pi's dialect: a different flag and a different
            // tool vocabulary, never Claude's names leaking into pi's argv.
            let pi = build_extra_args(stage, PI_HARNESS_ID);
            let idx = pi
                .iter()
                .position(|a| a == "--exclude-tools")
                .unwrap_or_else(|| panic!("{stage:?} missing --exclude-tools: {pi:?}"));
            assert_eq!(pi[idx + 1], PI_EDIT_TOOLS);
            assert!(!pi.iter().any(|a| a == "--disallowedTools"), "{pi:?}");
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
            base_sha: None,
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
                harness: "claude".to_string(),
                model: None,
                prompt_hash: "test-prompt-hash".to_string(),
            })
            .unwrap();

        assert!(!handle.was_cancelled(), "not cancelled yet");
        handle.cancel().unwrap();
        assert!(
            handle.was_cancelled(),
            "cancel() must be observable on the returned handle"
        );

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
                harness: "claude".to_string(),
                model: None,
                prompt_hash: "test-prompt-hash".to_string(),
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
        assert!(
            saw_text,
            "must observe streamed output before the gate blocks completion"
        );

        handle.cancel().unwrap();
        assert!(handle.was_cancelled());
        gate.release();

        let exited = rx
            .into_iter()
            .find(|e| matches!(e, RunEvent::Exited { .. }));
        match exited {
            Some(RunEvent::Exited { cancelled, .. }) => {
                assert!(
                    cancelled,
                    "Exited must report cancelled: true after handle.cancel()"
                )
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
                harness: "claude".to_string(),
                model: None,
                prompt_hash: "test-prompt-hash".to_string(),
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
                harness: "claude".to_string(),
                model: None,
                prompt_hash: "test-prompt-hash".to_string(),
            })
            .unwrap();
        for _ in rx {}

        let written = std::fs::read_to_string(dir.join("out.txt")).unwrap();
        assert_eq!(written, "hello");
        std::fs::remove_dir_all(&dir).ok();
    }

    // ---- AgentStageOutcome: final-state classification -----------------
    //
    // Pure unit tests over [`AgentStageOutcome::absorb`] + [`status`]: an
    // `Error` event mid-stream is not automatically terminal — a harness can
    // recover from a provider transient (e.g. a retried HTTP 429) and still
    // finish the whole stage cleanly, so the verdict is made from the run's
    // *final* state (exit code + whether any output text was produced), not
    // from the mere presence of an error event. Built by driving real
    // `RunEvent`s through `absorb` so the same path `run_agent_stage`'s drain
    // thread takes is what these assertions cover; no spawn, no network.

    /// Feed `events` through a fresh outcome exactly the way the drain
    /// thread does, returning it for status assertions.
    fn absorbed(events: &[RunEvent]) -> AgentStageOutcome {
        let mut outcome = AgentStageOutcome::default();
        for event in events {
            outcome.absorb(event);
        }
        outcome
    }

    #[test]
    fn error_event_recovered_by_clean_exit_and_output_is_ok() {
        let outcome = absorbed(&[
            RunEvent::Started {
                run_id: "r".to_string(),
            },
            RunEvent::Text {
                run_id: "r".to_string(),
                delta: "fixed the thing".to_string(),
            },
            // The incident shape: a transient provider error mid-run…
            RunEvent::Error {
                run_id: "r".to_string(),
                message: "429 rate limited".to_string(),
            },
            RunEvent::Text {
                run_id: "r".to_string(),
                delta: "; retry succeeded".to_string(),
            },
            // …and a normal completion afterwards.
            RunEvent::Exited {
                run_id: "r".to_string(),
                exit_code: Some(0),
                cancelled: false,
            },
        ]);

        assert!(outcome.errored, "the diagnostic flag must stay set");
        assert_eq!(
            outcome.last_error_message.as_deref(),
            Some("429 rate limited")
        );
        // The log trail keeps every error verbatim inside `text`.
        assert!(outcome.text.contains("[error] 429 rate limited"));
        assert!(outcome.text.contains("fixed the thing"));

        // …but the recovered transient must not fail the stage: clean exit
        // plus non-empty output is judged `ok`, so the DAG walk proceeds to
        // commit the work the agent actually completed.
        assert_eq!(outcome.status(), "ok");
        assert!(outcome.is_ok());
    }

    #[test]
    fn error_event_with_non_zero_exit_is_error() {
        let outcome = absorbed(&[
            RunEvent::Error {
                run_id: "r".to_string(),
                message: "provider exploded".to_string(),
            },
            RunEvent::Exited {
                run_id: "r".to_string(),
                exit_code: Some(1),
                cancelled: false,
            },
        ]);
        assert_eq!(outcome.status(), "error");
        assert!(!outcome.is_ok());
    }

    #[test]
    fn timeout_and_cancelled_still_take_precedence_over_the_final_state_rule() {
        // A deadline-killed stage sets BOTH flags (same cancel mechanism,
        // see `timed_out`'s doc) — `"timeout"` must win even when some text
        // was streamed and an error was seen before the kill.
        let mut timed_out = absorbed(&[
            RunEvent::Text {
                run_id: "r".to_string(),
                delta: "partial output".to_string(),
            },
            RunEvent::Error {
                run_id: "r".to_string(),
                message: "mid-run hiccup".to_string(),
            },
            RunEvent::Exited {
                run_id: "r".to_string(),
                exit_code: None,
                cancelled: true,
            },
        ]);
        timed_out.timed_out = true;
        assert_eq!(timed_out.status(), "timeout");
        assert!(!timed_out.is_ok());

        // A user-initiated cancel without a deadline stays `"cancelled"`.
        let cancelled = absorbed(&[RunEvent::Exited {
            run_id: "r".to_string(),
            exit_code: None,
            cancelled: true,
        }]);
        assert_eq!(cancelled.status(), "cancelled");
        assert!(!cancelled.is_ok());
    }

    #[test]
    fn error_event_with_clean_exit_but_no_output_is_error() {
        // Silent failure must still fail: exiting 0 having produced nothing
        // leaves downstream stages nothing to act on, so the recovered-
        // transient allowance does not apply. (`absorb` appends an
        // `[error] …` line to `text` for every Error event, so reaching this
        // arm in production means text was cleared or hand-built empty —
        // which is exactly why the guard lives in `status` itself rather
        // than trusting the log trail.)
        let outcome = AgentStageOutcome {
            errored: true,
            exit_code: Some(0),
            ..AgentStageOutcome::default()
        };
        assert_eq!(outcome.status(), "error");
        assert!(!outcome.is_ok());
    }

    #[test]
    fn absorb_keeps_only_the_last_error_message_but_every_error_line_in_text() {
        let outcome = absorbed(&[
            RunEvent::Error {
                run_id: "r".to_string(),
                message: "first failure".to_string(),
            },
            RunEvent::Error {
                run_id: "r".to_string(),
                message: "second failure".to_string(),
            },
            RunEvent::Exited {
                run_id: "r".to_string(),
                exit_code: Some(2),
                cancelled: false,
            },
        ]);
        assert_eq!(
            outcome.last_error_message.as_deref(),
            Some("second failure")
        );
        assert!(outcome.text.contains("[error] first failure"));
        assert!(outcome.text.contains("[error] second failure"));
        assert_eq!(outcome.status(), "error");
    }

    // ---- run_agent_stage: evidence + WS streaming ----------------------

    async fn test_state() -> AppState {
        test_state_with_config(Config::for_test()).await
    }

    /// Like [`test_state`] but with a caller-supplied [`Config`] — T-543's
    /// timeout tests need `agent_stage_timeout_secs` far shorter than
    /// `Config::for_test`'s own 10s default (still too slow for a gated-run
    /// test to wait out) so the deadline fires in well under a second.
    async fn test_state_with_config(config: Config) -> AppState {
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
        AppState::new(config, db)
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

        let dir =
            std::env::temp_dir().join(format!("dearborn-run-agent-stage-{}", ulid::Ulid::new()));
        std::fs::create_dir_all(&dir).unwrap();

        let outcome = run_agent_stage(
            &state,
            &agent,
            AgentStageParams {
                task_id: Some("task-1"),
                epic_id: Some("epic-1"),
                attempt: 1,
            },
            TaskRunRequest {
                run_id: "run-1".to_string(),
                stage: Stage::Implement,
                prompt: "go".to_string(),
                cwd: dir.clone(),
                harness: "claude".to_string(),
                model: None,
                prompt_hash: "test-prompt-hash".to_string(),
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
        assert_eq!(
            row.get::<Option<String>>(2).unwrap().as_deref(),
            Some("sess-42")
        );
        assert_eq!(row.get::<String>(3).unwrap(), "Hello, world");
        assert_eq!(row.get::<Option<i64>>(4).unwrap(), Some(0));
        assert_eq!(row.get::<i64>(5).unwrap(), 1);
        assert_eq!(row.get::<String>(6).unwrap(), "task-1");
        assert_eq!(
            row.get::<Option<String>>(7).unwrap().as_deref(),
            Some("epic-1")
        );

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
                task_id: Some("task-1"),
                epic_id: Some("epic-1"),
                attempt: 1,
            },
            TaskRunRequest {
                run_id: "run-2".to_string(),
                stage: Stage::Implement,
                prompt: "go".to_string(),
                cwd: std::env::temp_dir(),
                harness: "claude".to_string(),
                model: None,
                prompt_hash: "test-prompt-hash".to_string(),
            },
        )
        .await
        .expect_err("a spawn failure must surface as an error");
        assert!(matches!(err, AgentStageError::Harness(_)));

        let mut rows = state
            .db
            .conn()
            .query(
                "SELECT status, log FROM agent_run WHERE task_id = 'task-1'",
                (),
            )
            .await
            .unwrap();
        let row = rows.next().await.unwrap().expect("row still written");
        assert_eq!(row.get::<String>(0).unwrap(), "error");
        let log: String = row.get(1).unwrap();
        assert!(log.contains("no claude on PATH"));

        // T-542: a spawn failure never obtains a `RunHandle` at all, so
        // nothing was ever inserted — the registry stays exactly as empty as
        // it started, not merely "cleaned up".
        assert!(
            state.cancel_registry.lock().unwrap().is_empty(),
            "a harness spawn failure must never populate the cancel registry"
        );
    }

    // ---- T8 evidence columns ---------------------------------------------

    #[tokio::test]
    async fn run_agent_stage_writes_harness_model_and_prompt_hash_evidence() {
        let state = test_state().await;
        let agent = ScriptedTaskAgent::new().script(Stage::Review, ScriptedRun::default());
        let dir = std::env::temp_dir().join(format!("dearborn-evidence-{}", ulid::Ulid::new()));
        std::fs::create_dir_all(&dir).unwrap();

        let outcome = run_agent_stage(
            &state,
            &agent,
            AgentStageParams {
                task_id: Some("task-1"),
                epic_id: Some("epic-1"),
                attempt: 3,
            },
            TaskRunRequest {
                run_id: "run-ev".to_string(),
                stage: Stage::Review,
                prompt: "review prompt".to_string(),
                cwd: dir.clone(),
                harness: "claude".to_string(),
                model: Some("test-model".to_string()),
                prompt_hash: crate::agent_settings::prompt_hash("review prompt"),
            },
        )
        .await
        .expect("scripted stage must succeed");
        assert!(outcome.is_ok());

        let mut rows = state
            .db
            .conn()
            .query(
                "SELECT harness, model, prompt_hash FROM agent_run \
                 WHERE task_id = 'task-1' AND stage = 'review'",
                (),
            )
            .await
            .unwrap();
        let row = rows.next().await.unwrap().expect("a row was written");
        assert_eq!(row.get::<String>(0).unwrap(), "claude");
        assert_eq!(
            row.get::<Option<String>>(1).unwrap().as_deref(),
            Some("test-model")
        );
        assert_eq!(
            row.get::<Option<String>>(2).unwrap().as_deref(),
            Some(crate::agent_settings::prompt_hash("review prompt").as_str())
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn unsupported_harness_fails_loudly_before_any_row_is_opened() {
        let state = test_state().await;
        let agent = ScriptedTaskAgent::new();
        let err = run_agent_stage(
            &state,
            &agent,
            AgentStageParams {
                task_id: Some("task-1"),
                epic_id: None,
                attempt: 1,
            },
            TaskRunRequest {
                run_id: "run-codex".to_string(),
                stage: Stage::Implement,
                prompt: "go".to_string(),
                cwd: std::env::temp_dir(),
                // Only reachable by hand-editing settings rows — the API only
                // hands out enabled harnesses, and only "claude" is enabled.
                harness: "codex".to_string(),
                model: None,
                prompt_hash: "x".to_string(),
            },
        )
        .await
        .expect_err("an unsupported harness must fail loudly at spawn-validation");
        match &err {
            AgentStageError::UnsupportedHarness { harness, slot } => {
                assert_eq!(harness, "codex");
                assert_eq!(*slot, Some(AgentSlot::Implement));
            }
            other => panic!("expected UnsupportedHarness, got {other:?}"),
        }
        assert!(err.to_string().contains("codex"));

        // No run was attempted, so no evidence row exists to misread later.
        let mut rows = state
            .db
            .conn()
            .query("SELECT COUNT(*) FROM agent_run", ())
            .await
            .unwrap();
        let count: i64 = rows.next().await.unwrap().unwrap().get(0).unwrap();
        assert_eq!(count, 0);
    }

    // ---- T9: live-read across stage runs ----------------------------------

    /// The exact resolution a worker spawn site performs (see worker.rs's
    /// `stage_spawn_config`): fold globals + the project's override around the
    /// stage's compiled default, fresh on every call.
    async fn resolve_like_worker(state: &AppState) -> crate::agent_settings::SpawnConfig {
        crate::agent_settings::spawn_config(
            &state.db,
            "proj-1",
            crate::agent_slot::AgentSlot::Implement,
            crate::spec::prompt_for(Stage::Implement)
                .expect("Stage::Implement always has a prompt"),
        )
        .await
        .expect("settings resolution must succeed")
    }

    #[tokio::test]
    async fn live_read_a_mid_flight_settings_change_reaches_the_next_stage_run() {
        // `test_state` already seeds the `proj-1` FK target.
        let state = test_state().await;

        let agent = ScriptedTaskAgent::new().script(Stage::Implement, ScriptedRun::default());
        let dir = std::env::temp_dir().join(format!("dearborn-live-read-{}", ulid::Ulid::new()));
        std::fs::create_dir_all(&dir).unwrap();

        // Stage run 1: everything at defaults.
        let cfg1 = resolve_like_worker(&state).await;
        let outcome1 = run_agent_stage(
            &state,
            &agent,
            AgentStageParams {
                task_id: Some("task-1"),
                epic_id: None,
                attempt: 1,
            },
            TaskRunRequest {
                run_id: "run-1".to_string(),
                stage: Stage::Implement,
                prompt: cfg1.prompt.clone(),
                cwd: dir.clone(),
                harness: cfg1.harness.clone(),
                model: cfg1.model.clone(),
                prompt_hash: cfg1.prompt_hash.clone(),
            },
        )
        .await
        .expect("first stage must succeed");
        assert!(outcome1.is_ok());

        // Mid-flight edit: override implement's prompt + model *after* run 1
        // has already started-and-finished on the old values.
        crate::agent_settings::upsert_agent_setting(
            &state.db,
            "proj-1",
            &crate::agent_settings::AgentSetting {
                slot: crate::agent_slot::AgentSlot::Implement,
                harness: None,
                model: Some("haiku".to_string()),
                system_prompt: Some("revised implement instructions".to_string()),
            },
        )
        .await
        .unwrap();

        // Stage run 2 resolves fresh — no caching — and picks up the edit.
        let cfg2 = resolve_like_worker(&state).await;
        assert_eq!(cfg2.prompt, "revised implement instructions");
        assert_eq!(cfg2.model.as_deref(), Some("haiku"));
        assert_ne!(cfg1.prompt_hash, cfg2.prompt_hash);
        let outcome2 = run_agent_stage(
            &state,
            &agent,
            AgentStageParams {
                task_id: Some("task-1"),
                epic_id: None,
                attempt: 2,
            },
            TaskRunRequest {
                run_id: "run-2".to_string(),
                stage: Stage::Implement,
                prompt: cfg2.prompt.clone(),
                cwd: dir.clone(),
                harness: cfg2.harness.clone(),
                model: cfg2.model.clone(),
                prompt_hash: cfg2.prompt_hash.clone(),
            },
        )
        .await
        .expect("second stage must succeed");
        assert!(outcome2.is_ok());

        // Evidence keeps both runs auditable against their own configs:
        // attempt 1 carries the default hash + NULL model it actually ran
        // with; attempt 2 carries the override's hash + model.
        let mut rows = state
            .db
            .conn()
            .query(
                "SELECT attempt, harness, model, prompt_hash FROM agent_run \
                 WHERE task_id = 'task-1' ORDER BY attempt",
                (),
            )
            .await
            .unwrap();
        let row1 = rows.next().await.unwrap().unwrap();
        assert_eq!(row1.get::<i64>(0).unwrap(), 1);
        assert_eq!(row1.get::<String>(1).unwrap(), "claude");
        assert_eq!(row1.get::<Option<String>>(2).unwrap(), None);
        assert_eq!(row1.get::<String>(3).unwrap(), cfg1.prompt_hash);
        let row2 = rows.next().await.unwrap().unwrap();
        assert_eq!(row2.get::<i64>(0).unwrap(), 2);
        assert_eq!(row2.get::<String>(1).unwrap(), "claude");
        assert_eq!(
            row2.get::<Option<String>>(2).unwrap().as_deref(),
            Some("haiku")
        );
        assert_eq!(row2.get::<String>(3).unwrap(), cfg2.prompt_hash);
        std::fs::remove_dir_all(&dir).ok();
    }

    // ---- T-542: the cancel registry -------------------------------------

    /// While an agent stage is gated in flight, [`AppState::cancel_registry`]
    /// holds exactly one live entry keyed by [`cancel_registry_key`]
    /// (`epic_id` here); calling `.cancel()` through that entry is
    /// observable on the handle (`was_cancelled()`) immediately, and the
    /// stage's eventual `Exited` event (once the gate releases) reports
    /// `cancelled: true`. Once the drain finishes, the entry is gone —
    /// proving [`CancelGuard`]'s insert-on-construct/remove-on-drop shape end
    /// to end at this layer (the full pipeline-level proof, going through
    /// the real `POST /epics/{id}/lane` cancel, lives in `worker.rs`).
    #[tokio::test]
    async fn run_agent_stage_registers_a_live_handle_and_removes_it_on_drop() {
        let state = test_state().await;
        let gate = Arc::new(Gate::default());
        let agent: Arc<dyn TaskAgent> = Arc::new(ScriptedTaskAgent::new().with_gate(gate.clone()));

        let dir =
            std::env::temp_dir().join(format!("dearborn-cancel-registry-{}", ulid::Ulid::new()));
        std::fs::create_dir_all(&dir).unwrap();

        let run = {
            let state = state.clone();
            let agent = agent.clone();
            let dir = dir.clone();
            tokio::spawn(async move {
                run_agent_stage(
                    &state,
                    agent.as_ref(),
                    AgentStageParams {
                        task_id: Some("task-1"),
                        epic_id: Some("epic-1"),
                        attempt: 1,
                    },
                    TaskRunRequest {
                        run_id: "run-cancel".to_string(),
                        stage: Stage::Implement,
                        prompt: "go".to_string(),
                        cwd: dir,
                        harness: "claude".to_string(),
                        model: None,
                        prompt_hash: "test-prompt-hash".to_string(),
                    },
                )
                .await
            })
        };

        // Bounded, no-sleep-as-the-proof readiness poll: wait until the
        // registry actually has the entry (the drain thread races this test
        // for a few scheduler ticks after `agent.run` returns).
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(5);
        loop {
            if state.cancel_registry.lock().unwrap().contains_key("epic-1") {
                break;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "the cancel registry never gained an entry for the gated stage"
            );
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }

        {
            let registry = state.cancel_registry.lock().unwrap();
            assert_eq!(
                registry.len(),
                1,
                "exactly one entry for the one in-flight stage"
            );
            let handle = registry
                .get("epic-1")
                .expect("keyed by epic_id, not task_id");
            assert!(!handle.was_cancelled(), "not cancelled yet");
            handle.cancel().unwrap();
            assert!(
                handle.was_cancelled(),
                "cancel() through the registry must be observable on the live handle"
            );
        }

        gate.release();
        let outcome = run
            .await
            .unwrap()
            .expect("scripted stage must still close its row");
        assert!(
            outcome.cancelled,
            "Exited must report cancelled: true after the registry cancel()"
        );
        assert_eq!(outcome.status(), "cancelled");

        assert!(
            state.cancel_registry.lock().unwrap().is_empty(),
            "the entry must be removed once run_agent_stage returns (CancelGuard::drop)"
        );

        std::fs::remove_dir_all(&dir).ok();
    }

    /// T-551: a standalone-task stage (`epic_id: None`) keys its registry
    /// entry by `task_id` instead — [`cancel_registry_key`]'s whole reason to
    /// exist rather than a bare `params.epic_id.unwrap()`. This is the shape
    /// `worker::process_one_task` now exercises for real for every stage of a
    /// standalone claim, not just a forward-looking guess.
    #[tokio::test]
    async fn run_agent_stage_keys_the_registry_by_task_id_when_standalone() {
        let state = test_state().await;
        let gate = Arc::new(Gate::default());
        let agent: Arc<dyn TaskAgent> = Arc::new(ScriptedTaskAgent::new().with_gate(gate.clone()));

        let dir =
            std::env::temp_dir().join(format!("dearborn-cancel-registry-{}", ulid::Ulid::new()));
        std::fs::create_dir_all(&dir).unwrap();

        let run = {
            let state = state.clone();
            let agent = agent.clone();
            let dir = dir.clone();
            tokio::spawn(async move {
                run_agent_stage(
                    &state,
                    agent.as_ref(),
                    AgentStageParams {
                        task_id: Some("task-1"),
                        epic_id: None,
                        attempt: 1,
                    },
                    TaskRunRequest {
                        run_id: "run-standalone".to_string(),
                        stage: Stage::Implement,
                        prompt: "go".to_string(),
                        cwd: dir,
                        harness: "claude".to_string(),
                        model: None,
                        prompt_hash: "test-prompt-hash".to_string(),
                    },
                )
                .await
            })
        };

        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(5);
        loop {
            if state.cancel_registry.lock().unwrap().contains_key("task-1") {
                break;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "the cancel registry never gained an entry keyed by task_id"
            );
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }

        gate.release();
        run.await
            .unwrap()
            .expect("scripted stage must still close its row");
        assert!(state.cancel_registry.lock().unwrap().is_empty());
        std::fs::remove_dir_all(&dir).ok();
    }

    // ---- T-543: agent stage timeouts ------------------------------------

    /// The headline AC: an agent that never exits (a gated run whose gate
    /// this test deliberately never releases) is cancelled at
    /// `DEARBORN_AGENT_STAGE_TIMEOUT_SECS`'s deadline, and `run_agent_stage`
    /// itself returns anyway — proving the "must terminate" grace-period
    /// backstop this module's own doc documents, not just the ordinary
    /// drain-finishes-normally path every other test here exercises. Bounded
    /// throughout: the outer `tokio::time::timeout` is a hard ceiling well
    /// above the deadline (1s) plus the fixed grace period, so a broken
    /// implementation (one that actually hangs) fails this test fast rather
    /// than hanging the suite.
    #[tokio::test]
    async fn run_agent_stage_cancels_a_never_exiting_stage_at_the_deadline_and_still_returns() {
        let mut config = Config::for_test();
        config.executor.agent_stage_timeout_secs = 1;
        let state = test_state_with_config(config).await;

        let gate = Arc::new(Gate::default());
        let agent = ScriptedTaskAgent::new().with_gate(gate.clone()).script(
            Stage::Implement,
            ScriptedRun {
                text: vec!["partial output before the deadline kill".to_string()],
                ..ScriptedRun::default()
            },
        );

        let dir = std::env::temp_dir().join(format!("dearborn-timeout-{}", ulid::Ulid::new()));
        std::fs::create_dir_all(&dir).unwrap();

        let outcome = tokio::time::timeout(
            std::time::Duration::from_secs(20),
            run_agent_stage(
                &state,
                &agent,
                AgentStageParams {
                    task_id: Some("task-1"),
                    epic_id: Some("epic-1"),
                    attempt: 1,
                },
                TaskRunRequest {
                    run_id: "run-timeout".to_string(),
                    stage: Stage::Implement,
                    prompt: "go".to_string(),
                    cwd: dir.clone(),
                    harness: "claude".to_string(),
                    model: None,
                    prompt_hash: "test-prompt-hash".to_string(),
                },
            ),
        )
        .await
        .expect(
            "run_agent_stage must return well within the test's own bound — the gate is \
             NEVER released, so only the deadline + grace-period backstop can end this",
        )
        .expect("a deadline-killed stage still returns Ok with a terminal outcome");

        assert!(
            outcome.timed_out,
            "AgentStageOutcome::timed_out must be set"
        );
        assert!(
            outcome.cancelled,
            "the deadline kill is a cancel too — same RunControl::cancel()"
        );
        assert_eq!(outcome.status(), "timeout");

        // The row closed status='timeout' (not 'cancelled', not 'error') with
        // whatever partial log had accumulated by the time the deadline fired.
        let mut rows = state
            .db
            .conn()
            .query(
                "SELECT status, log FROM agent_run WHERE task_id = 'task-1'",
                (),
            )
            .await
            .unwrap();
        let row = rows.next().await.unwrap().expect("a row was written");
        assert_eq!(row.get::<String>(0).unwrap(), "timeout");
        let log: String = row.get(1).unwrap();
        assert!(
            log.contains("partial output before the deadline kill"),
            "the flushed partial log must be preserved: {log:?}"
        );

        // T-542's guard still covers a timeout: removed once run_agent_stage
        // returns, even though the underlying scripted thread is still
        // parked on the gate forever.
        assert!(
            state.cancel_registry.lock().unwrap().is_empty(),
            "the registry entry must be removed once run_agent_stage returns, timeout or not"
        );

        std::fs::remove_dir_all(&dir).ok();

        // Release the gate now, purely as teardown hygiene — this does not
        // weaken the scenario above. By this line, the deadline has already
        // fired, `RunControl::cancel()` has already been issued, the full
        // `AGENT_TIMEOUT_KILL_GRACE_PERIOD` has already elapsed with no
        // `Exited` observed, and `run_agent_stage` has already returned its
        // `timed_out` outcome and had every assertion above pass against it
        // — so the gate was genuinely never released *during* the window
        // this test is about. What it's for: a `#[tokio::test]`'s runtime is
        // dropped when the test body returns, and dropping it drains the
        // Tokio blocking pool (`BlockingPool::shutdown`), which blocks
        // **indefinitely** for every outstanding `spawn_blocking` task to
        // finish — including the drain task started inside `run_agent_stage`
        // above, which `run_agent_stage` itself gave up waiting on (per the
        // grace-period backstop this test proves) but did not, and cannot,
        // abort (`spawn_blocking` has no preemption). That drain thread is
        // still blocked reading `rx`, which stays open only because the
        // `ScriptedTaskAgent`'s own `std::thread` is still parked in
        // `Gate::wait` — so without this release, the thread never finishes,
        // `tx` never drops, `rx` never closes, the drain task never returns,
        // and the runtime drop hangs forever at teardown, after the test has
        // already passed. Releasing here lets that thread finish, closing
        // the channel and letting the drain task (and thus the runtime) exit
        // cleanly — the same teardown pattern `worker.rs`'s T-542
        // cancellation tests use (`gate.release()` only after every
        // assertion about the in-flight window has already been made).
        gate.release();
    }

    /// A stage that finishes comfortably inside its deadline is unaffected —
    /// `timed_out` stays `false` and `status()` reads the ordinary `"ok"`,
    /// not `"timeout"`. Guards against a timer implementation that fires on
    /// every stage regardless of whether the deadline was actually exceeded.
    #[tokio::test]
    async fn a_stage_that_finishes_within_the_deadline_is_not_marked_timed_out() {
        let mut config = Config::for_test();
        config.executor.agent_stage_timeout_secs = 30; // generous; must not fire
        let state = test_state_with_config(config).await;
        let agent = ScriptedTaskAgent::new();

        let dir = std::env::temp_dir().join(format!("dearborn-no-timeout-{}", ulid::Ulid::new()));
        std::fs::create_dir_all(&dir).unwrap();

        let outcome = run_agent_stage(
            &state,
            &agent,
            AgentStageParams {
                task_id: Some("task-1"),
                epic_id: Some("epic-1"),
                attempt: 1,
            },
            TaskRunRequest {
                run_id: "run-fast".to_string(),
                stage: Stage::Implement,
                prompt: "go".to_string(),
                cwd: dir.clone(),
                harness: "claude".to_string(),
                model: None,
                prompt_hash: "test-prompt-hash".to_string(),
            },
        )
        .await
        .expect("an unscripted, ungated stage finishes immediately");

        assert!(!outcome.timed_out);
        assert!(!outcome.cancelled);
        assert_eq!(outcome.status(), "ok");
        std::fs::remove_dir_all(&dir).ok();
    }

    /// The deadline/grace-period mechanics are a property of
    /// [`run_agent_stage`] itself, not of `Stage::Implement` specifically —
    /// worker.rs's own module doc is explicit that `route_stage_failure`
    /// makes "no new judgment call... at *this particular* stage" (T-543).
    /// Proves it two ways at once, using
    /// [`with_gate_on`](ScriptedTaskAgent::with_gate_on) for the first time
    /// since it was added: a `Stage::Review` run gated via `with_gate_on`
    /// times out exactly like `Stage::Implement` did above (same
    /// `timed_out`/`status()` outcome), while a `Stage::Implement` run on
    /// the *same* agent instance — which `with_gate_on`'s stage filter does
    /// not pin — finishes immediately beforehand, proving the filter itself
    /// (only the named stage gates) as well as the genericity.
    #[tokio::test]
    async fn run_agent_stage_times_out_a_non_implement_stage_the_same_way() {
        let mut config = Config::for_test();
        config.executor.agent_stage_timeout_secs = 1;
        let state = test_state_with_config(config).await;

        let gate = Arc::new(Gate::default());
        let agent = ScriptedTaskAgent::new().with_gate_on(Stage::Review, gate.clone());

        let dir =
            std::env::temp_dir().join(format!("dearborn-timeout-review-{}", ulid::Ulid::new()));
        std::fs::create_dir_all(&dir).unwrap();

        // Stage::Implement is not pinned by this agent's gate (`gate_stage`
        // names only `Review`) — it must finish immediately, proving the
        // filter itself works before the gated stage is ever reached.
        let implement_outcome = run_agent_stage(
            &state,
            &agent,
            AgentStageParams {
                task_id: Some("task-1"),
                epic_id: Some("epic-1"),
                attempt: 1,
            },
            TaskRunRequest {
                run_id: "run-implement".to_string(),
                stage: Stage::Implement,
                prompt: "go".to_string(),
                cwd: dir.clone(),
                harness: "claude".to_string(),
                model: None,
                prompt_hash: "test-prompt-hash".to_string(),
            },
        )
        .await
        .expect("Stage::Implement is not gated by this agent and must finish immediately");
        assert!(!implement_outcome.timed_out);
        assert!(!implement_outcome.cancelled);

        // Stage::Review *is* pinned — it never exits, so the same
        // deadline + grace-period backstop from the headline test above
        // must fire here too, for a stage other than Implement.
        let review_outcome = tokio::time::timeout(
            std::time::Duration::from_secs(20),
            run_agent_stage(
                &state,
                &agent,
                AgentStageParams {
                    task_id: Some("task-1"),
                    epic_id: Some("epic-1"),
                    attempt: 1,
                },
                TaskRunRequest {
                    run_id: "run-review-timeout".to_string(),
                    stage: Stage::Review,
                    prompt: "go".to_string(),
                    cwd: dir.clone(),
                    harness: "claude".to_string(),
                    model: None,
                    prompt_hash: "test-prompt-hash".to_string(),
                },
            ),
        )
        .await
        .expect("must return well within the test's own bound — Review's gate is never released")
        .expect("a deadline-killed stage still returns Ok with a terminal outcome");

        assert!(
            review_outcome.timed_out,
            "Stage::Review must time out exactly like Stage::Implement does"
        );
        assert!(review_outcome.cancelled);
        assert_eq!(review_outcome.status(), "timeout");

        std::fs::remove_dir_all(&dir).ok();

        // Teardown hygiene only — see the headline timeout test's own doc
        // above for why this release doesn't weaken the scenario (the
        // deadline and grace period have both already fired and
        // `run_agent_stage` has already returned).
        gate.release();
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
