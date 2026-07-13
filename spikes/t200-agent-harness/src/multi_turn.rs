//! T-200 SPIKE — interactive multi-turn PoC over `agent-harness` (v0.3.5).
//!
//! Drives the shelled-out Claude Code CLI across TWO conversational turns where
//! turn 2 depends on turn 1's context, using the harness's NATIVE session
//! resume (`RunRequest.resume` → `claude --resume <id>`). For each turn it
//! consumes the normalized `RunEvent` stream and prints every event, so the
//! transcript shows exactly how text / session / tool / lifecycle events
//! surface.
//!
//! Turn 1:  "My name is Deerborn..."  -> capture `session_id` from RunEvent::Session
//! Turn 2:  (resume that session) "What is my name?" -> proves context carried
//!
//! Run:  cargo run --bin multi_turn
//! If the `claude` CLI is missing/unauthenticated the run still exercises the
//! real API surface; it will just print an Error/Exited event instead of text.

use std::io::Write as _;

use harness::{Claude, Harness, RunEvent, RunMode, RunRequest, RunTuning};

/// Drive one turn. Prints every normalized RunEvent and returns
/// (assistant_text, session_id_seen, exit_code).
fn run_turn(
    label: &str,
    prompt: &str,
    resume: Option<String>,
) -> Result<(String, Option<String>, Option<i32>), harness::HarnessError> {
    println!("\n=================== {label} ===================");
    println!("PROMPT: {prompt}");
    if let Some(id) = &resume {
        println!("RESUME: {id}");
    }
    println!("---------------- RunEvent stream ----------------");

    let (_handle, rx) = Claude::new().run_channel(RunRequest {
        run_id: format!("t200-{label}"),
        prompt: prompt.to_owned(),
        // Run in this crate's dir; a real Deerborn planning run would point cwd
        // at the project's read-only canonical clone.
        cwd: std::env::current_dir().ok(),
        mode: RunMode::Ask, // planning is read-only discussion, no edits
        tuning: RunTuning::default(),
        resume,
    })?;

    let mut text = String::new();
    let mut session_id = None;
    let mut exit_code = None;

    // `run_channel` hangs up the receiver on its own when the run ends.
    for event in rx {
        match &event {
            RunEvent::Started { run_id } => println!("[Started] run_id={run_id}"),
            RunEvent::Session { session_id: sid, model, .. } => {
                println!("[Session] session_id={sid:?} model={model:?}");
                if let Some(s) = sid {
                    session_id = Some(s.clone());
                }
            }
            RunEvent::Text { delta, .. } => {
                text.push_str(delta);
                // Stream the assistant text inline as it arrives.
                print!("{delta}");
                let _ = std::io::stdout().flush();
            }
            RunEvent::Thinking { delta, .. } => println!("[Thinking] {delta}"),
            RunEvent::ToolStart { name, tool_call_id, input, tool_kind, .. } => println!(
                "[ToolStart] name={name} id={tool_call_id} kind={tool_kind:?} input={input:?}"
            ),
            RunEvent::ToolEnd { tool_call_id, ok, output, .. } => {
                println!("[ToolEnd] id={tool_call_id} ok={ok} output={output:?}")
            }
            RunEvent::SuggestedEdits { edits, .. } => println!("[SuggestedEdits] {} edit(s)", edits.len()),
            RunEvent::Activity { message, .. } => println!("[Activity] {message}"),
            RunEvent::Usage { input_tokens, output_tokens, total_tokens, .. } => println!(
                "\n[Usage] in={input_tokens:?} out={output_tokens:?} total={total_tokens:?}"
            ),
            RunEvent::AskQuestion { questions, .. } => {
                println!("[AskQuestion] {} question(s)", questions.len())
            }
            RunEvent::Error { message, .. } => println!("\n[Error] {message}"),
            RunEvent::Exited { exit_code: code, cancelled, .. } => {
                exit_code = *code;
                println!("\n[Exited] exit_code={code:?} cancelled={cancelled}");
            }
            // RunEvent is #[non_exhaustive].
            _ => println!("[other event] {event:?}"),
        }
    }

    Ok((text, session_id, exit_code))
}

fn main() -> Result<(), harness::HarnessError> {
    println!("T-200 agent-harness multi-turn PoC (native session-resume)");

    // Report readiness up front so a no-auth environment is obvious in the log.
    let readiness = Claude::new().readiness();
    println!(
        "claude readiness: ready={} installed={} version={:?} auth={}",
        readiness.ready, readiness.installed, readiness.version, readiness.auth_configured
    );

    // --- Turn 1: establish a fact the model must recall in turn 2. ----------
    let (_t1_text, session_id, _c1) = run_turn(
        "turn-1",
        "My name is Deerborn. Please acknowledge in one short sentence and remember it.",
        None,
    )?;

    // --- Turn 2: resume the SAME session and ask it to recall the fact. -----
    // If native resume works, the session_id captured above carries the
    // history; the prompt below contains NO restatement of the name.
    let Some(session_id) = session_id else {
        eprintln!(
            "\nNo session_id captured from turn 1 (likely no `claude` auth in this env). \
             Native resume needs it — see the findings report for the transcript-replay fallback."
        );
        return Ok(());
    };

    let (t2_text, _s2, _c2) = run_turn(
        "turn-2",
        "What is my name? Reply with only the name, nothing else.",
        Some(session_id),
    )?;

    println!("\n=================== VERDICT ===================");
    if t2_text.to_lowercase().contains("deerborn") {
        println!("PASS: turn 2 recalled the name from turn 1 via native session-resume.");
    } else {
        println!(
            "INCONCLUSIVE: turn 2 did not echo the name. Response was: {:?}",
            t2_text.trim()
        );
    }
    Ok(())
}
