//! [pi](https://pi.dev) (`pi`) as an out-of-tree [`Harness`].
//!
//! `agent-harness` ships Claude/Codex/bob adapters; pi is not among them, and
//! the pinned `=0.3.5` is deliberate (see `Cargo.toml`). The crate anticipates
//! exactly this: [`Harness`] is public, and [`normalize_process_event`] +
//! `run_events_from_parsed` are exported so a consumer can supply its own
//! line parser without forking the crate (its `examples/custom_harness.rs`
//! documents the pattern). So the adapter lives here, in Dearborn, alongside
//! the flag dialect the spawn sites need.
//!
//! ## Wire format
//!
//! `pi --mode json -p` writes NDJSON on stdout: a `session` header line, then
//! one line per `AgentSessionEvent`. The shapes this parser decodes (captured
//! from a real 0.84.2 run, not inferred from docs):
//!
//! ```text
//! {"type":"session","version":3,"id":"<uuid>","cwd":"…"}
//! {"type":"message_start","message":{"role":"assistant","provider":"openrouter","model":"…"}}
//! {"type":"message_update","usage":{…},"assistantMessageEvent":{"type":"text_delta","delta":"…"}}
//! {"type":"message_update","usage":{…},"assistantMessageEvent":{"type":"thinking_delta","delta":"…"}}
//! {"type":"tool_execution_start","toolCallId":"…","toolName":"read","args":{…}}
//! {"type":"tool_execution_end","toolCallId":"…","toolName":"read","result":{…},"isError":false}
//! {"type":"message_end","message":{…,"usage":{…},"stopReason":"stop"}}
//! ```
//!
//! ## What pi cannot do
//!
//! pi has **no MCP client** (checked against the installed 0.84.2 bundle: no
//! `--mcp-config`, nothing MCP-shaped in `dist/`). Dearborn's planning and
//! breakdown slots call *back* into the server over MCP ([`crate::mcp`]), so
//! those three slots are Claude-only — enforced by
//! [`crate::agent_settings::harness_supports_slot`], not by this module. The
//! five task-stage slots use no MCP and run on pi unchanged.
//!
//! ## Auth
//!
//! pi owns its own credentials (`~/.pi/agent/auth.json`, or any of the many
//! provider API-key env vars it documents), so — like the Claude adapter —
//! Dearborn stores and injects nothing: `credential().required` is `false`.

use std::path::PathBuf;
use std::process::Command;

use harness::{
    augmented_node_path, normalize_process_event, spawn_streaming, CredentialSpec, Harness,
    HarnessCapabilities, HarnessError, HarnessInfo, HarnessModel, HarnessReadiness,
    InstallCallback, InstallEvent, ParsedLine, RunCallback, RunHandle, RunMode, RunRequest,
    RunTuning, SessionInfo, ToolCallEnd, ToolCallStart, ToolKind, UsageInfo,
};
use serde_json::Value;

/// Settings/registry key for the pi harness. The string that appears in
/// `global_settings.enabled_harnesses`, `agent_setting.harness`, and the
/// `agent_run.harness` evidence column.
pub const PI_HARNESS_ID: &str = "pi";

/// The pi CLI as a [`Harness`].
#[derive(Debug, Default, Clone)]
pub struct Pi;

impl Pi {
    /// Construct the adapter. Cheap — it holds no state.
    pub fn new() -> Pi {
        Pi
    }
}

impl Harness for Pi {
    fn info(&self) -> HarnessInfo {
        HarnessInfo {
            id: PI_HARNESS_ID.to_owned(),
            display_name: "pi".to_owned(),
            description: "The pi coding agent (pi.dev). Uses your existing pi login.".to_owned(),
            requires_install: true,
            capabilities: HarnessCapabilities {
                // pi manages its own auth and edits files on disk directly.
                credential_required: false,
                previews_edits: false,
                // pi is provider-agnostic — its model catalog is whatever the
                // user has configured — so there is no useful curated list and
                // free-text `provider/id` is the norm.
                models: Vec::<HarnessModel>::new(),
                allows_custom_model: true,
                // `--thinking <level>`; the neutral levels map 1:1.
                supports_effort: true,
                // pi has no turn cap flag.
                supports_max_turns: false,
                // Sign-in is `pi auth`, which is a credential *printer*, not an
                // interactive OAuth flow we can drive. Leave `login` unsupported.
                supports_login: false,
            },
        }
    }

    fn readiness(&self) -> HarnessReadiness {
        let Some(version) = probe_version("pi") else {
            return HarnessReadiness {
                harness_id: PI_HARNESS_ID.to_owned(),
                ready: false,
                installed: false,
                version: None,
                auth_configured: false,
                error: Some("pi (`pi`) is not installed or not on PATH.".to_owned()),
                details: Value::Null,
            };
        };
        // Auth is reported as configured whenever pi is installed, and this is
        // a deliberate limitation rather than an oversight: pi resolves
        // credentials *per provider* (its own auth file plus ~30 provider env
        // vars) and `pi auth check` demands a `--provider`/`--model` to answer
        // at all — there is no provider-agnostic probe. Guessing one here
        // would report "not signed in" for a perfectly working setup pointed
        // at a different provider. A genuinely missing credential surfaces as
        // the run's own error instead, which is the same contract
        // `credential_required: false` already implies.
        HarnessReadiness {
            harness_id: PI_HARNESS_ID.to_owned(),
            ready: true,
            installed: true,
            version: Some(version),
            auth_configured: true,
            error: None,
            details: Value::Null,
        }
    }

    fn install(&self, on_event: InstallCallback) -> Result<(), HarnessError> {
        // npm global install, same blocking shape as the Claude adapter's.
        (*on_event)(InstallEvent::Step {
            text: "Installing pi via npm…".to_owned(),
        });
        let output = Command::new("npm")
            .args(["install", "-g", "@earendil-works/pi-coding-agent"])
            .env("PATH", augmented_node_path())
            .output()
            .map_err(|e| HarnessError::install(format!("failed to run npm: {e}")))?;
        for line in String::from_utf8_lossy(&output.stdout).lines() {
            (*on_event)(InstallEvent::Stdout {
                text: line.to_owned(),
            });
        }
        for line in String::from_utf8_lossy(&output.stderr).lines() {
            (*on_event)(InstallEvent::Stderr {
                text: line.to_owned(),
            });
        }
        (*on_event)(InstallEvent::Done {
            exit_code: output.status.code(),
            ok: output.status.success(),
        });
        Ok(())
    }

    fn run(&self, request: RunRequest, on_event: RunCallback) -> Result<RunHandle, HarnessError> {
        let RunRequest {
            run_id,
            prompt,
            cwd,
            mode,
            tuning,
            resume,
        } = request;
        let args = build_pi_args(prompt, mode, &tuning, resume.as_deref());
        let cwd = cwd.unwrap_or_else(|| std::env::current_dir().unwrap_or_default());

        // No env injected — pi uses its own auth. `spawn_streaming` augments
        // PATH so `node` is found under a minimal service environment.
        let handle = spawn_streaming(
            PathBuf::from("pi"),
            args,
            Vec::new(),
            cwd,
            run_id,
            move |event| {
                for normalized in normalize_process_event(event, parse_pi_line) {
                    (*on_event)(normalized);
                }
            },
        )
        .map_err(HarnessError::spawn)?;
        Ok(Box::new(handle))
    }

    fn credential(&self) -> CredentialSpec {
        CredentialSpec {
            label: "pi login (managed by the pi CLI)".to_owned(),
            keychain_service: "pi".to_owned(),
            keychain_account: "PI_API_KEY".to_owned(),
            // pi authenticates itself; Dearborn stores no key for it.
            required: false,
        }
    }
}

/// Build the argv for a `pi --mode json -p` headless run. Kept pure (no
/// spawn) so the flag mapping is unit-tested directly.
///
/// Mapping, and why each choice:
/// - `--mode json` + `-p`: NDJSON on stdout, process and exit. The whole
///   contract [`parse_pi_line`] decodes.
/// - `--no-approve`: never trust project-local `.pi/` extension code from the
///   checked-out repo. In non-interactive mode pi's trust prompts already
///   resolve to "no" (its `project-trust` context returns `undefined` without
///   a UI), so this pins today's behavior rather than changing it — an
///   autonomous run must not execute code that arrived in the workspace.
/// - `tuning.model` → `--model` (accepts `provider/id` and `model:thinking`).
/// - `tuning.effort` → `--thinking`; the neutral levels are pi's own tokens.
/// - `tuning.max_turns` is intentionally dropped: pi has no turn-cap flag
///   (`info().capabilities.supports_max_turns` says so).
/// - `resume` → `--session-id` (pi's "use this exact project session id,
///   creating it if missing"), the closest analogue of Claude's `--resume`.
/// - `RunMode::Ask` → a conservative `--exclude-tools edit,write` default,
///   emitted only when the host has not set a tool flag itself. Mirrors the
///   Claude adapter's `acceptEdits` default: a sensible floor the host fully
///   overrides. Unlike Claude, pi auto-runs every tool in print mode, so
///   without this `Ask` would not be read-only at all.
///
/// The prompt is positional and goes **last**, after every flag, so a
/// host-supplied `extra_args` flag/value pair can never swallow it.
fn build_pi_args(
    prompt: String,
    mode: RunMode,
    tuning: &RunTuning,
    resume: Option<&str>,
) -> Vec<String> {
    let mut args = vec![
        "--mode".to_owned(),
        "json".to_owned(),
        "-p".to_owned(),
        "--no-approve".to_owned(),
    ];
    if let Some(session_id) = resume {
        args.push("--session-id".to_owned());
        args.push(session_id.to_owned());
    }
    if let Some(model) = tuning
        .model
        .as_deref()
        .map(str::trim)
        .filter(|m| !m.is_empty())
    {
        args.push("--model".to_owned());
        args.push(model.to_owned());
    }
    if let Some(effort) = tuning.effort {
        args.push("--thinking".to_owned());
        args.push(effort.as_cli_value().to_owned());
    }
    if matches!(mode, RunMode::Ask) && !sets_a_tool_flag(&tuning.extra_args) {
        args.push("--exclude-tools".to_owned());
        args.push(PI_EDIT_TOOLS.to_owned());
    }
    // Host passthrough/overrides, appended verbatim after the adapter's own.
    args.extend(tuning.extra_args.iter().cloned());
    // Positional prompt, last.
    args.push(prompt);
    args
}

/// pi's file-mutating built-in tools, as the comma-separated value its
/// `--tools` / `--exclude-tools` flags take. The pi-dialect counterpart of
/// [`crate::task_agent::DENIED_EDIT_TOOLS`]: pi's built-ins are `read`,
/// `bash`, `edit`, `write` (plus `grep`/`find`/`ls`, off by default), so the
/// list is these two rather than Claude's four.
pub const PI_EDIT_TOOLS: &str = "edit,write";

/// Whether the host already decided this run's tool surface, in which case
/// the adapter must not also emit its own `Ask` default. Any of pi's four
/// tool-scoping flags counts — mixing an adapter denylist into a host
/// allowlist would silently narrow what the host asked for.
fn sets_a_tool_flag(extra_args: &[String]) -> bool {
    const TOOL_FLAGS: &[&str] = &[
        "--tools",
        "-t",
        "--exclude-tools",
        "-xt",
        "--no-tools",
        "-nt",
        "--no-builtin-tools",
        "-nbt",
    ];
    extra_args.iter().any(|arg| {
        TOOL_FLAGS
            .iter()
            .any(|flag| arg == flag || arg.starts_with(&format!("{flag}=")))
    })
}

/// Decode one line of `pi --mode json` stdout into the neutral
/// [`ParsedLine`]. Unknown/uninteresting line types decode to an empty
/// `ParsedLine`, which yields no events — pi's stream carries plenty of
/// lifecycle chatter (`agent_start`, `turn_start`, `agent_settled`, the
/// `*_start`/`*_end` bracket events around each content block) that has no
/// neutral counterpart and is deliberately dropped rather than turned into
/// noisy `Activity`.
pub fn parse_pi_line(line: &str) -> ParsedLine {
    let Ok(value) = serde_json::from_str::<Value>(line.trim()) else {
        // pi writes only NDJSON on stdout in `--mode json`; a non-JSON line is
        // stray output (a warning from node, say), not something to surface as
        // an error. Dropping it matches how the other adapters treat garbage.
        return ParsedLine::default();
    };
    let mut parsed = ParsedLine::default();
    match value.get("type").and_then(Value::as_str) {
        // The header line: pi's session id, which Dearborn stores as the
        // `agent_run.session_id` evidence handle.
        Some("session") => {
            parsed.session = Some(SessionInfo {
                session_id: value
                    .get("id")
                    .and_then(Value::as_str)
                    .map(str::to_owned),
                model: None,
            });
        }
        // The first assistant message names the provider + model actually
        // used, which the header line does not carry. Emitted as a second
        // `Session` (id-less) rather than invented onto the header: consumers
        // record the id only when present, so this cannot clobber it.
        Some("message_start") => {
            if let Some(model) = assistant_model(value.get("message")) {
                parsed.session = Some(SessionInfo {
                    session_id: None,
                    model: Some(model),
                });
            }
        }
        Some("message_update") => {
            let event = value.get("assistantMessageEvent");
            match event.and_then(|e| e.get("type")).and_then(Value::as_str) {
                Some("text_delta") => parsed.text = string_field(event, "delta"),
                Some("thinking_delta") => parsed.thinking = string_field(event, "delta"),
                _ => {}
            }
        }
        Some("tool_execution_start") => {
            let name = string_field(Some(&value), "toolName").unwrap_or_default();
            parsed.tool_start = Some(ToolCallStart {
                tool_call_id: string_field(Some(&value), "toolCallId").unwrap_or_default(),
                tool_kind: pi_tool_kind(&name),
                // pi delivers the full arguments inline at the start, so the
                // card can render them immediately (unlike Claude's streamed
                // `input_json_delta`).
                input: value.get("args").map(ToString::to_string),
                name,
            });
        }
        Some("tool_execution_end") => {
            parsed.tool_end = Some(ToolCallEnd {
                tool_call_id: string_field(Some(&value), "toolCallId").unwrap_or_default(),
                ok: !value
                    .get("isError")
                    .and_then(Value::as_bool)
                    .unwrap_or(false),
                output: value.get("result").map(flatten_tool_result),
            });
        }
        // End of an assistant message: the authoritative usage totals, and the
        // one place an in-band failure is reported. pi's `usage` is cumulative
        // for the run, so a later `message_end` supersedes an earlier one
        // rather than adding to it.
        Some("message_end") => {
            let message = value.get("message");
            parsed.usage = message.and_then(|m| m.get("usage")).map(parse_usage);
            parsed.error = message.and_then(assistant_error);
        }
        _ => {}
    }
    parsed
}

/// `"<provider>/<model>"` for an assistant message, or just the model when pi
/// reports no provider. Gives the evidence log the fully-qualified id the user
/// would type into `--model`, not a bare alias that is ambiguous across
/// providers.
fn assistant_model(message: Option<&Value>) -> Option<String> {
    let message = message?;
    if message.get("role").and_then(Value::as_str) != Some("assistant") {
        return None;
    }
    let model = message.get("model").and_then(Value::as_str)?;
    Some(match message.get("provider").and_then(Value::as_str) {
        Some(provider) if !provider.is_empty() => format!("{provider}/{model}"),
        _ => model.to_owned(),
    })
}

/// The in-band failure an assistant message carries, if any. pi ends a failed
/// turn with `stopReason: "error"` (or `"aborted"`) plus an `errorMessage`,
/// and exits 0 — so without this a failed run would look like the agent
/// silently produced nothing. Mirrors why `ParsedLine::error` exists at all.
fn assistant_error(message: &Value) -> Option<String> {
    let stop_reason = message.get("stopReason").and_then(Value::as_str)?;
    if !matches!(stop_reason, "error" | "aborted") {
        return None;
    }
    Some(
        message
            .get("errorMessage")
            .and_then(Value::as_str)
            .filter(|m| !m.trim().is_empty())
            .map(str::to_owned)
            .unwrap_or_else(|| format!("pi run {stop_reason}")),
    )
}

/// Neutral token accounting from pi's cumulative `usage` object. pi splits
/// cache reads/writes out of `input`, and carries a `cost` block Dearborn
/// deliberately drops — [`UsageInfo`] is tokens only.
fn parse_usage(usage: &Value) -> UsageInfo {
    let field = |key: &str| usage.get(key).and_then(Value::as_u64);
    UsageInfo {
        input_tokens: field("input"),
        output_tokens: field("output"),
        total_tokens: field("totalTokens"),
    }
}

/// Flatten a pi tool result into display text. pi returns
/// `{"content":[{"type":"text","text":"…"}, …]}`; anything else (an image
/// part, a bare value) falls back to its JSON so the card is never blank.
fn flatten_tool_result(result: &Value) -> String {
    let Some(parts) = result.get("content").and_then(Value::as_array) else {
        return result.to_string();
    };
    let text: Vec<&str> = parts
        .iter()
        .filter_map(|part| part.get("text").and_then(Value::as_str))
        .collect();
    if text.is_empty() {
        result.to_string()
    } else {
        text.join("")
    }
}

/// Map a pi built-in tool name onto the neutral behaviour class. pi's
/// built-ins are lowercase (`read`, `bash`, `edit`, `write`, `grep`, `find`,
/// `ls`); extension tools land in `Other`, which is exactly what `Other` is
/// for.
fn pi_tool_kind(name: &str) -> ToolKind {
    match name {
        "read" => ToolKind::Read,
        "write" => ToolKind::Write,
        "edit" => ToolKind::Edit,
        "grep" | "find" | "ls" => ToolKind::Search,
        "bash" => ToolKind::Execute,
        _ => ToolKind::Other,
    }
}

/// A non-empty string field, or `None`.
fn string_field(value: Option<&Value>, key: &str) -> Option<String> {
    value
        .and_then(|v| v.get(key))
        .and_then(Value::as_str)
        .map(str::to_owned)
}

/// Run `pi --version`, returning the trimmed stdout on success. PATH is
/// augmented for the same reason the Claude adapter does it: a service started
/// with a minimal environment must still find an npm-installed CLI.
fn probe_version(program: &str) -> Option<String> {
    let output = Command::new(program)
        .arg("--version")
        .env("PATH", augmented_node_path())
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    let text = String::from_utf8_lossy(&output.stdout).trim().to_owned();
    if text.is_empty() {
        None
    } else {
        Some(text)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use harness::ReasoningEffort;

    /// Value of the arg immediately following `flag`, if present.
    fn flag_value<'a>(args: &'a [String], flag: &str) -> Option<&'a str> {
        args.iter()
            .position(|a| a == flag)
            .and_then(|i| args.get(i + 1))
            .map(String::as_str)
    }

    #[test]
    fn pi_info_and_credential() {
        let h = Pi::new();
        assert_eq!(h.info().id, PI_HARNESS_ID);
        assert!(h.info().requires_install);
        // pi manages its own auth — Dearborn requires no key.
        assert!(!h.credential().required);
        // pi has no turn cap; it does have `--thinking`.
        assert!(!h.info().capabilities.supports_max_turns);
        assert!(h.info().capabilities.supports_effort);
    }

    #[test]
    fn pi_args_are_headless_json_with_the_prompt_last() {
        let args = build_pi_args("hi".to_owned(), RunMode::Edit, &RunTuning::default(), None);
        assert_eq!(flag_value(&args, "--mode"), Some("json"));
        assert!(args.iter().any(|a| a == "-p"));
        assert!(args.iter().any(|a| a == "--no-approve"));
        // The prompt is positional and last, so no flag/value pair can eat it.
        assert_eq!(args.last().map(String::as_str), Some("hi"));
        // Defaults carry no model / thinking level.
        assert!(!args.iter().any(|a| a == "--model"));
        assert!(!args.iter().any(|a| a == "--thinking"));
    }

    #[test]
    fn pi_ask_mode_defaults_to_excluding_edit_tools() {
        // pi auto-runs every tool in print mode, so `Ask` is only read-only if
        // the adapter says so.
        let args = build_pi_args("hi".to_owned(), RunMode::Ask, &RunTuning::default(), None);
        assert_eq!(flag_value(&args, "--exclude-tools"), Some(PI_EDIT_TOOLS));
        // `Edit` leaves the tool surface alone.
        let edit = build_pi_args("hi".to_owned(), RunMode::Edit, &RunTuning::default(), None);
        assert!(!edit.iter().any(|a| a == "--exclude-tools"));
    }

    #[test]
    fn a_host_tool_flag_replaces_the_ask_default_cleanly() {
        for host_flag in ["--tools", "-t", "--exclude-tools", "--no-tools"] {
            let tuning = RunTuning {
                extra_args: vec![host_flag.to_owned(), "read,bash".to_owned()],
                ..RunTuning::default()
            };
            let args = build_pi_args("hi".to_owned(), RunMode::Ask, &tuning, None);
            let adapter_defaults = args.iter().filter(|a| *a == PI_EDIT_TOOLS).count();
            assert_eq!(
                adapter_defaults, 0,
                "{host_flag} must suppress the adapter's own denylist"
            );
        }
        // `--flag=value` form counts too.
        let tuning = RunTuning {
            extra_args: vec!["--exclude-tools=write".to_owned()],
            ..RunTuning::default()
        };
        let args = build_pi_args("hi".to_owned(), RunMode::Ask, &tuning, None);
        assert!(!args.iter().any(|a| a == PI_EDIT_TOOLS));
    }

    #[test]
    fn pi_args_carry_model_effort_and_resume_and_ignore_max_turns() {
        let tuning = RunTuning {
            model: Some("anthropic/claude-opus-4.8".to_owned()),
            effort: Some(ReasoningEffort::High),
            // pi has no turn cap — this must not leak into argv.
            max_turns: Some(5),
            ..RunTuning::default()
        };
        let args = build_pi_args("hi".to_owned(), RunMode::Edit, &tuning, Some("sess-1"));
        assert_eq!(
            flag_value(&args, "--model"),
            Some("anthropic/claude-opus-4.8")
        );
        assert_eq!(flag_value(&args, "--thinking"), Some("high"));
        assert_eq!(flag_value(&args, "--session-id"), Some("sess-1"));
        assert!(!args.iter().any(|a| a.contains("max-turns")));
    }

    #[test]
    fn pi_blank_model_is_treated_as_unset() {
        let tuning = RunTuning {
            model: Some("   ".to_owned()),
            ..RunTuning::default()
        };
        let args = build_pi_args("hi".to_owned(), RunMode::Edit, &tuning, None);
        assert!(!args.iter().any(|a| a == "--model"));
    }

    #[test]
    fn host_extra_args_land_before_the_prompt() {
        let tuning = RunTuning {
            extra_args: vec!["--session-dir".to_owned(), "/tmp/s".to_owned()],
            ..RunTuning::default()
        };
        let args = build_pi_args("do it".to_owned(), RunMode::Edit, &tuning, None);
        let dir = args.iter().position(|a| a == "--session-dir").unwrap();
        assert_eq!(args[dir + 1], "/tmp/s");
        assert_eq!(args.last().map(String::as_str), Some("do it"));
    }

    // ---- parser ------------------------------------------------------------

    #[test]
    fn session_header_yields_the_session_id() {
        let parsed = parse_pi_line(
            r#"{"type":"session","version":3,"id":"01a03037-0fee-7d9c-ba44-794463850f16","cwd":"/w"}"#,
        );
        assert_eq!(
            parsed.session.unwrap().session_id.as_deref(),
            Some("01a03037-0fee-7d9c-ba44-794463850f16")
        );
    }

    #[test]
    fn assistant_message_start_reports_a_qualified_model() {
        let parsed = parse_pi_line(
            r#"{"type":"message_start","message":{"role":"assistant","provider":"openrouter","model":"stealth/ox-alpha"}}"#,
        );
        let session = parsed.session.unwrap();
        // Id-less, so it can never clobber the header's session id.
        assert!(session.session_id.is_none());
        assert_eq!(session.model.as_deref(), Some("openrouter/stealth/ox-alpha"));
        // A *user* message_start carries no model and yields nothing.
        assert!(parse_pi_line(
            r#"{"type":"message_start","message":{"role":"user","content":[]}}"#
        )
        .is_empty());
    }

    #[test]
    fn text_and_thinking_deltas_split() {
        let text = parse_pi_line(
            r#"{"type":"message_update","assistantMessageEvent":{"type":"text_delta","contentIndex":1,"delta":"hel"}}"#,
        );
        assert_eq!(text.text.as_deref(), Some("hel"));
        assert!(text.thinking.is_none());

        let thinking = parse_pi_line(
            r#"{"type":"message_update","assistantMessageEvent":{"type":"thinking_delta","contentIndex":0,"delta":"hmm"}}"#,
        );
        assert_eq!(thinking.thinking.as_deref(), Some("hmm"));
        assert!(thinking.text.is_none());

        // The bracket events around a content block carry no delta.
        assert!(parse_pi_line(
            r#"{"type":"message_update","assistantMessageEvent":{"type":"text_start","contentIndex":1}}"#
        )
        .is_empty());
    }

    #[test]
    fn tool_execution_start_and_end_round_trip() {
        let start = parse_pi_line(
            r#"{"type":"tool_execution_start","toolCallId":"t-1","toolName":"read","args":{"path":"note.txt"}}"#,
        );
        let start = start.tool_start.unwrap();
        assert_eq!(start.tool_call_id, "t-1");
        assert_eq!(start.name, "read");
        assert_eq!(start.tool_kind, ToolKind::Read);
        // Arguments arrive inline, so the card renders them immediately.
        assert_eq!(start.input.as_deref(), Some(r#"{"path":"note.txt"}"#));

        let end = parse_pi_line(
            r#"{"type":"tool_execution_end","toolCallId":"t-1","toolName":"read","result":{"content":[{"type":"text","text":"hello world\n"}]},"isError":false}"#,
        );
        let end = end.tool_end.unwrap();
        assert_eq!(end.tool_call_id, "t-1");
        assert!(end.ok);
        assert_eq!(end.output.as_deref(), Some("hello world\n"));
    }

    #[test]
    fn a_failed_tool_call_is_not_ok() {
        let end = parse_pi_line(
            r#"{"type":"tool_execution_end","toolCallId":"t-2","toolName":"bash","result":{"content":[{"type":"text","text":"boom"}]},"isError":true}"#,
        );
        assert!(!end.tool_end.unwrap().ok);
    }

    #[test]
    fn tool_kinds_cover_pi_builtins_and_fall_back_to_other() {
        assert_eq!(pi_tool_kind("write"), ToolKind::Write);
        assert_eq!(pi_tool_kind("edit"), ToolKind::Edit);
        assert_eq!(pi_tool_kind("bash"), ToolKind::Execute);
        assert_eq!(pi_tool_kind("grep"), ToolKind::Search);
        assert_eq!(pi_tool_kind("find"), ToolKind::Search);
        assert_eq!(pi_tool_kind("ls"), ToolKind::Search);
        // An extension tool is `Other`, not a guess.
        assert_eq!(pi_tool_kind("subagent"), ToolKind::Other);
    }

    #[test]
    fn message_end_yields_usage_and_no_error_on_a_clean_stop() {
        let parsed = parse_pi_line(
            r#"{"type":"message_end","message":{"role":"assistant","usage":{"input":5717,"output":18,"cacheRead":704,"cacheWrite":0,"totalTokens":6439,"cost":{"total":0}},"stopReason":"stop"}}"#,
        );
        let usage = parsed.usage.unwrap();
        assert_eq!(usage.input_tokens, Some(5717));
        assert_eq!(usage.output_tokens, Some(18));
        assert_eq!(usage.total_tokens, Some(6439));
        assert!(parsed.error.is_none());
    }

    #[test]
    fn a_failed_turn_surfaces_as_an_error() {
        // pi exits 0 on an in-band failure, so this is the only signal that a
        // run produced no answer for a reason.
        let parsed = parse_pi_line(
            r#"{"type":"message_end","message":{"role":"assistant","usage":{"input":1,"output":0,"totalTokens":1},"stopReason":"error","errorMessage":"rate limited"}}"#,
        );
        assert_eq!(parsed.error.as_deref(), Some("rate limited"));
        // An error with no message still surfaces, naming the stop reason.
        let bare = parse_pi_line(
            r#"{"type":"message_end","message":{"role":"assistant","stopReason":"aborted"}}"#,
        );
        assert_eq!(bare.error.as_deref(), Some("pi run aborted"));
    }

    #[test]
    fn lifecycle_chatter_and_garbage_decode_to_nothing() {
        for line in [
            r#"{"type":"agent_start"}"#,
            r#"{"type":"turn_start"}"#,
            r#"{"type":"turn_end","message":{},"toolResults":[]}"#,
            r#"{"type":"agent_end","messages":[],"willRetry":false}"#,
            r#"{"type":"agent_settled"}"#,
            r#"{"type":"some_future_event"}"#,
            "not json at all",
            "",
        ] {
            assert!(
                parse_pi_line(line).is_empty(),
                "expected no events from: {line}"
            );
        }
    }
}
