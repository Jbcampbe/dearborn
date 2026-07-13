# T-200 — [SPIKE] agent-harness interactive multi-turn PoC

**Status:** complete · **Date:** 2026-07-13 · **Gate for:** Phase 2 (T-201–T-205)

> **GATE VERDICT: PROCEED-WITH-CAVEATS.** Native multi-turn resume works
> end-to-end against the live `claude` CLI; MCP is feasible through the harness
> via `RunTuning.extra_args`; tool calls surface cleanly as `RunEvent`s. The
> caveats are non-blocking but must be designed for: (1) `RunMode::Ask` does NOT
> enforce read-only — file mutations still ran, so the planning read-only
> guarantee must come from tool-scoping + a read-only checkout, not the mode;
> (2) for Claude, `ToolStart.input` is always `None` (args aren't reconstructed
> from the stream), so Deerborn reads MCP tool args server-side, not from the
> event; (3) the crate has NO first-class MCP field — wiring is a raw-args
> passthrough Deerborn owns.

The PoC crate lives at `spikes/t200-agent-harness/` (a detached one-crate
workspace; the root `deerborn-server` build is untouched and still green).

---

## 0. Ground truth vs. the brief

| Brief said | Reality |
|---|---|
| crate `agent-harness` ~v0.4 | Latest on crates.io is **v0.3.5** (there is no 0.4). Pulls `cli-stream`/`bob-rs` 0.3.6. |
| imported as `harness` | Correct — `[lib] name = "harness"`. |
| `run_channel(RunRequest{…}) -> (handle, events)` | Correct, and it's a **provided** trait method over `run()`. Returns `(RunHandle, std::sync::mpsc::Receiver<RunEvent>)`. |
| RunEvent has `ToolStart{name}` + implied ToolEnd | Correct — both exist, richer than the README implies (ids, kind, output). |
| License unknown | **MIT OR Apache-2.0.** |

Source inspected verbatim at
`~/.cargo/registry/src/index.crates.io-*/agent-harness-0.3.5/`.

---

## 1. Exact API (verbatim key types)

### `RunRequest` / `RunTuning` / `RunMode` (`src/harness.rs`)

```rust
pub struct RunRequest {
    pub run_id: String,
    pub prompt: String,
    pub cwd: Option<PathBuf>,        // run dir; point at the read-only clone
    pub mode: RunMode,               // Ask | Edit
    pub tuning: RunTuning,
    pub resume: Option<String>,      // session id to resume; None => new session
}

pub struct RunTuning {
    pub model: Option<String>,       // -> --model
    pub effort: Option<ReasoningEffort>, // IGNORED by Claude adapter
    pub max_turns: Option<u32>,      // -> --max-turns
    pub extra_args: Vec<String>,     // appended VERBATIM after adapter's own argv
}

pub enum RunMode { Ask, Edit }       // Edit adds `--permission-mode acceptEdits` by default
```

The `Harness` trait method used:

```rust
fn run_channel(&self, request: RunRequest)
    -> Result<(RunHandle, mpsc::Receiver<RunEvent>), HarnessError>;
// The receiver hangs up on its own when the run ends (no need to drop the handle).
// Dropping the handle does NOT cancel; hold it to call handle.cancel() if needed.
```

### `RunEvent` — the normalized stream (`src/events.rs`)

`#[serde(tag = "kind", rename_all = "camelCase", rename_all_fields = "camelCase")]`,
`#[non_exhaustive]` (match arms MUST carry a `_`). Every variant carries `run_id`.

```rust
pub enum RunEvent {
    Started   { run_id },
    Session   { run_id, session_id: Option<String>, model: Option<String> }, // <-- session id here
    Text      { run_id, delta },
    Thinking  { run_id, delta },
    ToolStart { run_id, tool_call_id, name, input: Option<String>, tool_kind: ToolKind },
    ToolEnd   { run_id, tool_call_id, ok: bool, output: Option<String> },
    SuggestedEdits { run_id, edits: Vec<SuggestedEdit> },
    Activity  { run_id, message },   // also carries truncated stderr
    Usage     { run_id, input_tokens, output_tokens, total_tokens }, // all Option
    AskQuestion { run_id, request_id, questions: Vec<Question> },
    Error     { run_id, message },   // terminal, followed by Exited
    Exited    { run_id, exit_code: Option<i32>, cancelled: bool }, // exactly once
}

pub enum ToolKind { Read, Write, Edit, Search, Execute, Other } // #[non_exhaustive]
```

The wire JSON is camelCase (`kind`, `runId`, `sessionId`, `toolCallId`,
`toolKind`, …) and derives `Serialize` only — designed to be relayed straight
over Deerborn's WebSocket to the client, matching T-202's plan.

### Claude adapter argv (`src/claude/mod.rs::build_claude_args`)

Every headless run is:

```
claude -p "<prompt>" --output-format stream-json --verbose --include-partial-messages
        [--resume <session_id>] [--model <m>] [--max-turns <n>]
        [--permission-mode acceptEdits  # only in Edit mode, if host didn't set one]
        <tuning.extra_args…>            # appended verbatim, last-wins
```

No env is injected; Claude Code uses its own auth. `cwd` becomes the child's
working dir.

---

## 2. The real multi-turn transcript (live run, NOT faked)

Environment: `claude` **2.1.207**, installed + authenticated
(`readiness.ready = true`). Command: `cargo run --bin multi_turn`. This is a
verbatim capture.

```
=================== turn-1 ===================
PROMPT: My name is Deerborn. Please acknowledge in one short sentence and remember it.
[Started] run_id=t200-turn-1
[Session] session_id=Some("371d5c96-0ca0-4a84-8e12-9f4bcf74dd3f") model=Some("claude-opus-4-8")
Understood, Deerborn — I'll remember your name.
[ToolStart] name=Write id=toolu_01GhV8… kind=Write input=None
[ToolEnd]   id=toolu_01GhV8… ok=true output=Some("File created successfully at: …/memory/user-name.md …")
[ToolStart] name=Read  id=toolu_01Y9fx… kind=Read  input=None
[ToolEnd]   id=toolu_01Y9fx… ok=true output=Some("1\t- [Deerborn v1 Architecture]…")
[ToolStart] name=Edit  id=toolu_01Bdqn… kind=Edit  input=None
[ToolEnd]   id=toolu_01Bdqn… ok=true output=Some("The file …/memory/MEMORY.md has been updated successfully…")
Got it, Deerborn — noted and saved to memory.
[Usage] in=Some(8) out=Some(602) total=Some(610)
[Exited] exit_code=Some(0) cancelled=false

=================== turn-2 ===================
PROMPT: What is my name? Reply with only the name, nothing else.
RESUME: 371d5c96-0ca0-4a84-8e12-9f4bcf74dd3f
[Started] run_id=t200-turn-2
[Session] session_id=Some("371d5c96-0ca0-4a84-8e12-9f4bcf74dd3f") model=Some("claude-opus-4-8")
Deerborn
[Usage] in=Some(2) out=Some(7) total=Some(9)
[Exited] exit_code=Some(0) cancelled=false

=================== VERDICT ===================
PASS: turn 2 recalled the name from turn 1 via native session-resume.
```

Turn 2's prompt never restated the name; resuming the session id from turn 1
was sufficient. Note `input_tokens = 2` on turn 2 — the CLI supplied the history
itself; there was no transcript replay in the prompt.

---

## 3. Gap 1 — Multi-turn: native resume vs. transcript replay

**Native session-resume WORKS and is the recommendation.**

- `session_id` surfaces in **`RunEvent::Session`**, emitted as the first stdout
  line (Claude's `system/init`). Capture it there.
- Feed it back as `RunRequest.resume` on the next turn → adapter runs
  `claude --resume <id>` → the CLI replays the conversation from its own
  transcript store. Proven above.
- The resumed run **keeps the same `session_id`** (stable across turns), so
  Deerborn can store one id per planning session.

**Recommendation for T-201/T-202:**
- Drive multi-turn with **native resume as the primary path.** Persist
  `session_id` on the epic's planning session the first time `RunEvent::Session`
  arrives; pass it as `resume` on every subsequent user message.
- **But keep Deerborn's own `transcript_message` table as the source of truth**
  (already the plan — MILESTONE §2.2, ARCHITECTURE §10). Native resume is a
  *cost/context optimization*, not the durable record. Reasons to keep replay as
  a fallback:
  - Claude's session store is local to the machine + CLI version; it can be
    GC'd, and a resume against a stale/missing id will fail. Deerborn must be
    able to reconstruct context by replaying the stored transcript as a single
    prompt.
  - T-201 explicitly requires "resumable after a server restart" — the durable
    transcript satisfies that independently of the CLI's session store.
- **Fallback shape (transcript replay):** on a resume failure (or after a config
  change), send `resume: None` with a prompt that prepends the rendered
  transcript (`User: …\nAssistant: …\n…\nUser: <new message>`). Costs full input
  tokens each turn but is backend-portable. Implement resume-with-replay-fallback
  behind one function so T-202 callers don't branch.

---

## 4. Gap 2 — Tool calls mid-conversation

Tool invocations surface as a **`ToolStart` … `ToolEnd` pair matched by
`tool_call_id`**, interleaved with `Text` in stream order (see transcript §2):

- **`ToolStart { tool_call_id, name, input, tool_kind }`** — from Claude's
  `content_block_start` / `tool_use`. `name` is the raw tool name
  (`Write`, `Read`, `Edit`, and for MCP tools `mcp__<server>__<tool>`).
  `tool_kind` is a neutral class for UI routing.
  **Caveat: `input` is ALWAYS `None` for Claude.** Args stream as
  `input_json_delta` fragments and the adapter deliberately does not reconstruct
  them (to avoid delaying the card). So the event stream shows *that* a tool ran
  and its result, but **not its arguments**.
- **`ToolEnd { tool_call_id, ok, output }`** — from the `tool_result` block;
  `ok = !is_error`, `output` = the tool's result text.
- `AskUserQuestion` is special-cased: suppressed as a tool card and re-emitted as
  `RunEvent::AskQuestion` (chips). Headless Claude denies it, so it's largely
  moot for autonomous planning.

**Impact on T-202/T-203:** the planning agent's `update_epic` / `read_codebase_context`
calls will appear as `ToolStart{name:"mcp__deerborn__update_epic"}` → `ToolEnd`.
Because `input` is `None`, **Deerborn must not rely on the event stream for the
tool arguments.** It doesn't need to: the args are delivered to Deerborn's own
MCP server (it *is* the tool), so it has them first-hand and applies the epic
mutation there. The event stream is for the live transcript/UI only. This is
fine and actually cleaner (single source of the args).

---

## 5. Gap 3 — MCP (gates T-203)

**The harness has NO first-class MCP support** — no `mcp` field on `RunRequest`
or `RunTuning`, no `McpServer` type (grep of the whole crate: the only `mcp`
hits are tool-kind *classification* of already-running MCP calls).

**But it is fully feasible via `RunTuning.extra_args`,** which the Claude adapter
appends verbatim to the argv. Claude Code natively supports `--mcp-config` and
tool-allow flags, so Deerborn wires its local MCP server like this (see
`src/mcp_config_demo.rs`, which builds the real `RunRequest`):

```rust
tuning.extra_args = vec![
    "--mcp-config".into(),  <json-or-path>,        // Deerborn's local MCP server
    "--allowedTools".into(),
        "mcp__deerborn__update_epic,mcp__deerborn__read_codebase_context".into(),
    "--permission-mode".into(), "bypassPermissions".into(), // headless auto-approve
];
```

The MCP config (inline JSON or a temp file Deerborn writes into `cwd`) names an
http- or stdio-transport server pointing back at Deerborn, scoped per planning
session. Tools then appear to the agent as `mcp__deerborn__<tool>`.

**Recommendation for T-203:**
- Wire MCP through `extra_args` exactly as above. No fork, no adapter change.
- **Enforce the phase-scoped tool surface with `--allowedTools`** (allow-list
  only the planning tools) — do NOT rely on `RunMode::Ask` for this (see §6).
- Prefer writing a `.mcp.json` / config file into the run `cwd` over a giant
  inline JSON arg (cleaner, avoids shell/arg-length quirks; either works).
- One risk to validate in T-203: confirm `--mcp-config` + `--allowedTools`
  interplay with `bypassPermissions` actually auto-runs the MCP tools headlessly
  on the installed CLI version. The mechanism is standard Claude Code, but it's
  worth a smoke test since this spike didn't stand up a live MCP endpoint.

---

## 6. CAVEAT: `RunMode::Ask` is not a read-only guarantee

Observed live in §2: the turn-1 run was `RunMode::Ask`, yet the agent
successfully ran **Write** and **Edit** tools (`ok=true`) against the filesystem
(it wrote to `~/.claude/.../memory/`). `Ask` mode only means the adapter doesn't
*add* `--permission-mode acceptEdits`; it does not disallow edit tools, and
headless `-p` mode did not block them.

**Consequence for T-203's AC** ("the agent cannot mutate lane/status", reads the
clone "read-only"): the read-only guarantee must be enforced by
(a) pointing `cwd` at a genuinely read-only checkout (filesystem perms), and
(b) `--allowedTools` restricted to the MCP planning tools (+ read-only builtins
if desired) with edit/bash tools omitted — **not** by `RunMode::Ask`.

---

## 7. Crate maturity & license

- **Version:** 0.3.5 (deps `cli-stream` / `bob-rs` at 0.3.6 — versions not fully
  locked in step). Pre-1.0; API can churn.
- **License:** MIT OR Apache-2.0 (permissive, fine for Deerborn).
- **Quality signals (good):** well-documented, `#[non_exhaustive]` on the public
  enums (so new event kinds won't break Deerborn if it carries `_` arms),
  thorough unit tests in-crate, clean object-safe trait, `run_channel`
  convenience matches Deerborn's consume-from-a-channel need.
- **Rough edges:** (a) no MCP abstraction — Deerborn owns the raw args;
  (b) `ToolStart.input` always `None` for Claude; (c) `effort` silently ignored
  by the Claude adapter; (d) README under-documents resume/tool/MCP (this spike's
  reason for existing). None are blockers.
- **Pinning advice:** pin `agent-harness = "=0.3.5"` initially and treat minor
  bumps as reviewed changes, given pre-1.0 churn and the `#[non_exhaustive]`
  surface.

---

## 8. GATE VERDICT

**PROCEED-WITH-CAVEATS.**

Interactive multi-turn is real and works via native session-resume; tool calls
and MCP wiring are both achievable through the documented-here mechanisms. Phase
2 is buildable as designed. Required adaptations, none architecture-breaking:

- **T-201/T-202:** keep the durable `transcript_message` store as source of
  truth; use native `resume` as an optimization with a transcript-replay
  fallback on resume failure/restart. Capture `session_id` from
  `RunEvent::Session`.
- **T-202:** relay `RunEvent`s straight to the client (already camelCase JSON).
  Don't expect tool *arguments* in the stream (`ToolStart.input` is `None`).
- **T-203:** pass Deerborn's local MCP server via `RunTuning.extra_args`
  (`--mcp-config` + `--allowedTools` + `--permission-mode`). Enforce the
  read-only / phase-scoped guarantee with `--allowedTools` + a read-only
  checkout, **not** `RunMode::Ask`. Smoke-test the headless MCP auto-approval
  path early.
- **T-204/T-205:** unaffected; both ride the same engine.
```

## PoC location
- `spikes/t200-agent-harness/` — `cargo build` green; `cargo run --bin multi_turn`
  (live multi-turn) and `cargo run --bin mcp_config_demo` (MCP wiring).
