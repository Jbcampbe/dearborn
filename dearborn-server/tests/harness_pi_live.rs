//! LIVE proof for the pi harness adapter (`dearborn_server::harness_pi`).
//!
//! The adapter's unit tests pin the flag mapping and the NDJSON → `RunEvent`
//! decode against captured wire samples. What they cannot prove is that those
//! samples still describe the CLI: pi is fast-moving, and a renamed event type
//! or a changed flag would leave every unit test green while every real run
//! produced an empty stream. This test is the tripwire — it drives a genuine
//! `pi --mode json -p` subprocess through [`Pi::run_channel`] and asserts the
//! events the task-stage pipeline actually depends on come out the other side.
//!
//! ## How to run
//!
//! ```sh
//! # from the repo root; `pi` must be on PATH with a provider configured —
//! # this spends real tokens.
//! cargo test -p dearborn-server --test harness_pi_live -- --ignored --nocapture
//! ```
//!
//! ## Why `#[ignore]` (mandatory, MILESTONE_2 §10)
//!
//! `just test` stays hermetic: no network, no CLIs. Same stance as
//! `worker_live.rs` — the live proof is opt-in and run by hand.

use std::time::{Duration, Instant};

use dearborn_server::harness_pi::Pi;
use harness::{Harness, RunEvent, RunMode, RunRequest, RunTuning};

/// Bound on the whole run, so a hung CLI fails the test instead of hanging a
/// terminal. Generous: a cold pi start plus one trivial turn.
const LIVE_RUN_TIMEOUT: Duration = Duration::from_secs(180);

/// Everything the task-stage pipeline reads off the stream, tallied from one
/// real run so a failure says *which* part of the contract broke.
#[derive(Default)]
struct Seen {
    session_id: Option<String>,
    model: Option<String>,
    text: String,
    tool_names: Vec<String>,
    tool_ended_ok: bool,
    usage_total: Option<u64>,
    errors: Vec<String>,
    exit_code: Option<i32>,
}

#[test]
#[ignore = "drives the live `pi` CLI; run with --ignored"]
fn live_pi_run_streams_session_text_tools_and_usage() {
    // A workspace with one file, so the run has something real to read: the
    // task stages all operate on a checked-out tree, and a tool call is the
    // half of the wire format a text-only prompt would never exercise.
    let dir = std::env::temp_dir().join(format!("dearborn-pi-live-{}", ulid::Ulid::new()));
    std::fs::create_dir_all(&dir).expect("create workspace");
    std::fs::write(dir.join("note.txt"), "dearborn-pi-live-marker\n").expect("seed file");

    let (_handle, rx) = Pi::new()
        .run_channel(RunRequest {
            run_id: "pi-live-1".to_string(),
            prompt: "Use the read tool to read note.txt, then reply with its contents verbatim \
                     and nothing else."
                .to_string(),
            cwd: Some(dir.clone()),
            // Read-only: also proves the adapter's `Ask` default reaches the
            // CLI as a flag it accepts rather than a usage error.
            mode: RunMode::Ask,
            tuning: RunTuning::default(),
            resume: None,
        })
        .expect("pi must spawn — is it on PATH?");

    let started = Instant::now();
    let mut seen = Seen::default();
    for event in rx {
        assert!(
            started.elapsed() < LIVE_RUN_TIMEOUT,
            "live pi run exceeded {LIVE_RUN_TIMEOUT:?}"
        );
        match event {
            RunEvent::Session {
                session_id, model, ..
            } => {
                // Two Session events arrive: the header's id, then the first
                // assistant message's model. Neither may clobber the other.
                if session_id.is_some() {
                    seen.session_id = session_id;
                }
                if model.is_some() {
                    seen.model = model;
                }
            }
            RunEvent::Text { delta, .. } => seen.text.push_str(&delta),
            RunEvent::ToolStart { name, .. } => seen.tool_names.push(name),
            RunEvent::ToolEnd { ok, .. } => seen.tool_ended_ok = ok,
            RunEvent::Usage { total_tokens, .. } => seen.usage_total = total_tokens,
            RunEvent::Error { message, .. } => seen.errors.push(message),
            RunEvent::Exited { exit_code, .. } => seen.exit_code = exit_code,
            _ => {}
        }
    }

    let _ = std::fs::remove_dir_all(&dir);

    assert!(
        seen.errors.is_empty(),
        "run reported errors: {:?}",
        seen.errors
    );
    assert_eq!(seen.exit_code, Some(0), "pi exited non-zero");

    // `agent_run.session_id` evidence: the header line still carries an id.
    let session_id = seen
        .session_id
        .expect("no Session event carried a session id");
    assert!(!session_id.is_empty());

    // `agent_run.model` evidence: the assistant message still names a model.
    let model = seen.model.expect("no Session event carried a model");
    assert!(!model.is_empty(), "model must not be blank");

    // The answer text — what `AgentStageOutcome.text` is built from, and what
    // the review stage's `VERDICT:` line is parsed out of.
    assert!(
        seen.text.contains("dearborn-pi-live-marker"),
        "streamed text did not carry the file contents: {:?}",
        seen.text
    );

    // Tool events — the half of the wire format a text-only run never proves.
    assert!(
        seen.tool_names.iter().any(|n| n == "read"),
        "expected a `read` tool call, saw {:?}",
        seen.tool_names
    );
    assert!(seen.tool_ended_ok, "the read tool call reported failure");

    // Usage accounting still decodes (its field names are pi's, not ours).
    assert!(
        seen.usage_total.is_some_and(|t| t > 0),
        "no usage totals decoded: {:?}",
        seen.usage_total
    );
}
