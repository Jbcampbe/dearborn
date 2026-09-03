//! The interactive agent-run seam (the per-node planning engines' foundation).
//!
//! Dearborn's planning is a **map of decision nodes** (epic "Wayfinder-Inspired
//! Planning"): each grilling/prototype node gets one interactive agent session,
//! while research/AFK-task nodes run one-shot and breakdown stays one-shot. The
//! product/technical linear planning flow — `PlanningConfig` phases, the
//! epic-level transcript, `advance-phase` — was removed in the clean cutover;
//! this module now carries only the engine-agnostic pieces those later node
//! engines build on:
//!
//! * the [`PlanningAgent`] seam (production [`ClaudePlanningAgent`]; tests
//!   inject a scripted fake) — given a [`PlanningRunRequest`], hand back the
//!   harness's blocking `Receiver<RunEvent>`;
//! * the [`ws_type`] `RunEvent` → WebSocket frame-type mapping every live
//!   relay (breakdown, task stages, and the future per-node engines) shares;
//! * the `testing` doubles (`SilentPlanningAgent` for booting test state,
//!   `Gate` for pinning runs in flight).
//!
//! ## The seam
//!
//! The real `claude` CLI cannot run deterministically under `cargo test` (it
//! needs auth + network), so the harness sits behind the [`PlanningAgent`]
//! trait. [`ClaudePlanningAgent`] is the production implementation; the trait
//! is intentionally tiny so the per-node engines can adopt it unchanged.

use std::path::PathBuf;
use std::sync::mpsc::Receiver;

use harness::{Claude, Harness, RunEvent, RunMode, RunRequest, RunTuning};

// ---- the agent seam ------------------------------------------------------

/// A single interactive agent turn to run, decoupled from the harness so tests
/// can inject a scripted agent.
pub struct PlanningRunRequest {
    /// Unique id for this run (a ULID); echoed back on every `RunEvent`.
    pub run_id: String,
    /// The user's message for this turn.
    pub prompt: String,
    /// Working directory for the run (e.g. the project's read-only clone).
    pub cwd: Option<PathBuf>,
    /// Native harness resume id, if the session already exists.
    pub resume: Option<String>,
    /// System prompt for the run (stable across turns). A `String`, not
    /// `&'static str`: it is the slot's live-resolved effective prompt —
    /// the project's override when set, else the compiled default.
    pub system_prompt: String,
    /// The slot this run's settings were resolved under. Carried on the request
    /// so [`ClaudePlanningAgent::run`] can name the right slot when it refuses
    /// a harness that cannot run it.
    pub slot: crate::agent_slot::AgentSlot,
    /// The harness key this run was resolved to. Validated with
    /// [`crate::agent_settings::harness_supports_slot`] by
    /// [`ClaudePlanningAgent::run`]; a harness this slot cannot run surfaces as
    /// an `Error` + `Exited` event stream (the trait has no `Result`).
    pub harness: String,
    /// The resolved model passed verbatim to the CLI; `None` → CLI default.
    pub model: Option<String>,
}

/// The seam that makes interactive agent runs hermetically testable.
///
/// Production wraps [`harness::Claude`] ([`ClaudePlanningAgent`]); tests inject a
/// fake. Implementations return the harness's own **blocking**
/// `std::sync::mpsc::Receiver<RunEvent>`, which the caller drains off-runtime.
pub trait PlanningAgent: Send + Sync {
    /// Start a run and hand back its `RunEvent` receiver. Must not block: the
    /// events are produced on the harness's / fake's own thread and the receiver
    /// hangs up on its own when the run ends.
    fn run(&self, req: PlanningRunRequest) -> Receiver<RunEvent>;
}

/// Production [`PlanningAgent`]: drives Claude Code through the harness —
/// `RunMode::Ask`, the system prompt via `--append-system-prompt`, native
/// `resume`.
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

        // T7 spawn-validation, mirroring `run_agent_stage`'s task-stage check:
        // a harness key this slot cannot run means settings were hand-edited
        // into a state the API refuses to create. Surface loudly through the
        // same synthetic Error+Exited stream a spawn failure uses — the trait's
        // return type has no room for a `Result`.
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
            // Interactive runs discuss and read; the agent reads its cwd with
            // its own file tools. NB: `Ask` is not a read-only *guarantee* —
            // read-only behavior comes from the prompt steering and a read-only
            // cwd, not from the mode.
            mode: RunMode::Ask,
            tuning: RunTuning {
                extra_args: vec![
                    "--append-system-prompt".to_string(),
                    req.system_prompt.clone(),
                ],
                // The slot's resolved model rides through to the CLI.
                model: req.model.clone(),
                ..RunTuning::default()
            },
            resume: req.resume,
        };

        match Claude::new().run_channel(request) {
            // Drop the handle: dropping does NOT cancel; the run proceeds to
            // completion and the receiver hangs up on its own.
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

/// Map a [`RunEvent`] to the WS `type` published on a topic (e.g. `epic:<id>`,
/// later `node:<id>`). The serialized event (camelCase, `kind`-tagged) is
/// relayed verbatim as the frame `payload`. Documented in `CONVENTIONS.md`
/// §WebSocket.
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

// ---- test doubles (crate-visible so other modules' tests can use them) ----

#[cfg(test)]
pub(crate) mod testing {
    use super::*;
    use std::sync::{Condvar, Mutex};

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

    /// A [`PlanningAgent`] that emits only Started → Exited: it streams nothing
    /// and persists nothing. Injected by tests that only need an `AppState` to
    /// boot — nothing in production drives this agent while the per-node
    /// planning engines are being built on the seam they will own.
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
    use super::*;

    /// Build a request for `harness` under the breakdown slot — the one
    /// Claude-Code-bound slot still in the vocabulary (the per-node planning
    /// engines pick their own slots when they land).
    fn run_req(run_id: &str, harness: &str) -> PlanningRunRequest {
        PlanningRunRequest {
            run_id: run_id.to_string(),
            prompt: "hello".to_string(),
            cwd: None,
            resume: None,
            system_prompt: "system".to_string(),
            slot: crate::agent_slot::AgentSlot::Breakdown,
            harness: harness.to_string(),
            model: None,
        }
    }

    #[test]
    fn ws_type_maps_every_common_event() {
        assert_eq!(
            ws_type(&RunEvent::Started { run_id: "r".into() }),
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

    #[test]
    fn agent_rejects_an_unsupported_harness_loudly() {
        let agent = ClaudePlanningAgent::new();
        let rx = agent.run(run_req("run-codex", "codex"));

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
        // The synthetic stream still terminates so every drainer exits.
        let exited = rx.iter().find(|e| matches!(e, RunEvent::Exited { .. }));
        assert!(exited.is_some(), "stream must end with Exited");
    }

    #[test]
    fn agent_rejects_a_spawnable_but_non_claude_harness() {
        // pi is a fully supported harness for task stages, but the
        // Claude-Code-bound slots are wired to the Claude Code adapter only —
        // so a pi-configured run is refused here, and the message says which
        // of the two reasons applies.
        let agent = ClaudePlanningAgent::new();
        let rx = agent.run(run_req("run-pi", crate::harness_pi::PI_HARNESS_ID));

        match rx.iter().next().expect("an Error event must arrive") {
            RunEvent::Error { message, .. } => {
                assert!(message.contains("pi"), "error names the harness: {message}");
                assert!(message.contains("Claude Code"), "error says why: {message}");
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

    #[test]
    fn agent_accepts_the_supported_harness_request_shape() {
        // No live spawn here — a real `claude` binary would start. Instead:
        // build the request exactly as a spawn site does for the supported key
        // and confirm it passes validation by checking that a supported
        // request reaches the spawn path (which, without `claude` on PATH in
        // CI, surfaces as an Error stream too — so we only assert it did NOT
        // fail with "unsupported").
        let agent = ClaudePlanningAgent::new();
        let rx = agent.run(run_req("run-claude", "claude"));
        let unsupported = rx.iter().any(
            |e| matches!(&e, RunEvent::Error { message, .. } if message.contains("unsupported")),
        );
        assert!(
            !unsupported,
            "a supported-harness request must pass spawn validation"
        );
    }
}
