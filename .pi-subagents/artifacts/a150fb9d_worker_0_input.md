# Task for worker

You are a delegated subagent running from a fork of the parent session. Treat the inherited conversation as reference-only context, not a live thread to continue. Do not continue or answer prior messages as if they are waiting for a reply. Your sole job is to execute the task below and return a focused result for that task using your tools.

Task:
Implement **T-301 — One-shot breakdown agent (`to-tasks`)** for the Deerborn project (a Rust/axum + libSQL + Vue coding-agent orchestration server). This is MILESTONE_1.md §6 Phase 3, task T-301. Work in the main worktree (no subagents of your own).

## Context to read first (in this order)
1. `MILESTONE_1.md` §2 (frozen contract), §6 (T-301 AC), and the T-205 "Done" note for the pattern.
2. `deerborn-server/CONVENTIONS.md` — wire contract, route conventions, MCP server section, WS section.
3. `deerborn-server/src/planning.rs` — the planning-agent run pattern you MUST mirror (PlanningAgent trait, ClaudePlanningAgent, scripted fake, spawn_run draining blocking mpsc on spawn_blocking, WS relay, capability minting, MCP config wiring). This is your template.
4. `deerborn-server/src/mcp.rs` — the in-process MCP server; you will ADD two tools here (`create_task`, `link_dependency`) and extend `CapabilityScope`.
5. `deerborn-server/src/epics.rs` — epic store helpers, `fetch_epic`, `get_epic_clone_path`, `update_epic_context`, transcript store.
6. `deerborn-server/src/lib.rs` — `AppState`, `app()` router, `try_acquire_run`/`InflightGuard`.
7. `deerborn-server/migrations/0001_baseline.sql` — the `task` and `task_dependency` tables already exist (§2.2). Do NOT add a migration unless you must; the schema is already there.
8. `references/prompts/to-tasks.md` — the vertical-slice / tracer-bullet breakdown prompt logic you are encoding.

## What to build

### A. New module `deerborn-server/src/tasks.rs` — task store helpers
Reusable, crate-visible store helpers (mirror the style of `epics.rs` store helpers):
- `Task` DTO (Serialize): `id, epic_id, project_id, title, description: Option<String>, acceptance: Option<String>, status, failure_reason: Option<String>, agent_session_id: Option<String>, position: Option<i64>, created_at, updated_at`. (Matches §2.2 `task` columns. Omit nothing except you may leave Half-2-only fields nullable in the DTO.)
- `create_task(conn, epic_id, project_id, title, description, acceptance) -> AppResult<Task>`: INSERT with a ULID id, `status='Todo'`, `position` = `(SELECT COALESCE(MAX(position),0)+1 FROM task WHERE epic_id=?)` (atomic, like `append_message`'s seq), now-ms timestamps. Return the row.
- `link_dependency(conn, blocker_id, blocked_id) -> AppResult<()>`: INSERT INTO task_dependency. Validate both tasks exist and belong to the same epic (else `BadRequest`). **Reject self-edges** (blocker==blocked → `BadRequest`). **Reject cycles**: before insert, run a DFS/BFS from `blocked_id` following outgoing edges (blocked→its blockers) would-be... actually check: adding edge (blocker→blocked) creates a cycle iff `blocked` can already reach `blocker` via existing `blocker_id→blocked_id` edges (i.e. `blocker` is reachable from `blocked` going *forward* along "is blocked by"? — be careful with direction). Define: edge (blocker_id, blocked_id) means "blocker blocks blocked". A cycle exists iff after adding, following `blocker_id → blocked_id` edges you return to start. So adding (B, X) creates a cycle iff X can already reach B by following blocked_id→blocker_id... write a clear helper `would_create_cycle(conn, blocker_id, blocked_id) -> bool` that DFSs the existing graph and reject with `AppError::Conflict("linking ... would create a cycle")`. This cycle guard is also formalized in T-302 but include it now so the breakdown agent can't corrupt the DAG.
- `list_tasks_for_epic(conn, epic_id) -> AppResult<Vec<Task>>` ordered by position.
- `list_dependencies_for_epic(conn, epic_id) -> AppResult<Vec<(String,String)>>` (blocker_id, blocked_id pairs).
- `task_belongs_to_epic(conn, task_id, epic_id) -> AppResult<bool>` helper.

### B. Extend `deerborn-server/src/mcp.rs` — two breakdown-phase MCP tools
- Extend `CapabilityScope` with a new field `project_id: String` (required). Update the existing planning `mint` call site in `planning.rs` to supply the epic's `project_id` (fetch via the epic row you already load for clone_path, or a quick query). Keep planning behavior identical.
- Add a new constant `BREAKDOWN_ALLOWED_TOOLS: &str = "mcp__deerborn__create_task,mcp__deerborn__link_dependency"`.
- Add two tools to `tools_call` dispatch, gated by scope phase: when `scope.phase == "breakdown"`, `tools/list` returns `create_task` + `link_dependency` (NOT the planning tools); when phase is `product`/`technical`, it returns the existing planning tools (unchanged). Implement this by branching `tools_list_result` on `scope.phase`.
- `create_task` tool: args `{ title, description, acceptance, blocks?: [string] }`. Uses `scope.epic_id` + `scope.project_id` (NEVER from args — same scoping discipline as `update_epic`). Calls `tasks::create_task`, then for each id in `blocks` calls `tasks::link_dependency(new_task_id, that_id)` (new task blocks those). Returns a text content with the created task id. Validate title non-empty (tool-level `isError`).
- `link_dependency` tool: args `{ blocker_id, blocked_id }`. Verify both belong to `scope.epic_id` (else `isError`). Calls `tasks::link_dependency`. Return ok / isError on conflict (include the message).
- Publish a `dag_updated` event on `epic:<id>` (payload = the DAG, see below) after any successful create/link so the UI updates live. Provide a helper `publish_dag(state, epic_id)` that builds `{ nodes: [Task], edges: [{blocker_id, blocked_id}] }` and publishes on `epic:<id>` with type `dag_updated`. (T-302 reuses this.)
- Update `tools_list_result` doc/descriptions for the two new tools (clear, agent-facing).

### C. New module `deerborn-server/src/breakdown.rs` — the one-shot breakdown agent
Mirror `planning.rs` structure but for a ONE-SHOT run (no resume, no multi-turn):
- `BreakdownAgent` trait: `fn run(&self, req: BreakdownRunRequest) -> Receiver<RunEvent>` (same seam shape as `PlanningAgent`).
- `ClaudeBreakdownAgent` (production): builds a `RunRequest` with `RunMode::Ask`, the breakdown system prompt (encode the `to-tasks.md` vertical-slice logic — see prompt below), `--append-system-prompt` carrying the epic's title + product_context + technical_context as the "PRD", `--mcp-config` + `--allowedTools BREAKDOWN_ALLOWED_TOOLS` + `--permission-mode bypassPermissions`, `cwd` = read-only clone. No `resume` (one-shot). On spawn failure, emit terminal Error+Exited (same pattern as planning).
- A scripted fake `ScriptedBreakdownAgent` for hermetic tests: it records the request and emits Started→Session→(optional Text)→Exited. Also a `SilentBreakdownAgent` if useful.
- `spawn_breakdown(state, epic_id, guard)`: the background run. Mint a breakdown capability (`scope.phase="breakdown"`, epic_id, project_id, clone_path), write MCP config, build the prompt, run the agent, drain the blocking receiver on `spawn_blocking` relaying every RunEvent live to `epic:<id>` (reuse `planning::ws_type` for the type mapping — make it pub if it isn't). On completion: set `epic.status='Ready'` (UPDATE epic SET status='Ready', updated_at=? WHERE id=?), record an `agent_run` row (stage='breakdown', epic_id, session_id from RunEvent::Session, log = assembled text or empty), publish `epic_updated` (the updated epic) and `dag_updated` on `epic:<id>`. Hold the `InflightGuard` for the whole run; hold the `CapabilityGuard` for the whole run; remove the temp mcp config file on completion. The breakdown run does NOT write to `transcript_message` (transcript is planning-only; breakdown's artifact is the task DAG + an agent_run row).
- The breakdown system prompt (encode `to-tasks.md`): instruct the agent to break the epic into tracer-bullet vertical slices, create each via `create_task(title, description, acceptance, blocks)`, create blockers first, link deps via `link_dependency`, and stop when done. Include the rule that each slice cuts through ALL integration layers end-to-end.

### D. Route + state transition
- In `lib.rs`, add route `POST /epics/:id/breakdown` → `breakdown::trigger_breakdown` (protected, behind bearer layer).
- `trigger_breakdown`: load the epic; `404` if missing. Require `epic.status == "Planning"` else `409 conflict` ("epic must be in Planning to break down"). Require a `technical` planning_session to exist (else `409` "advance to technical planning before breakdown") — this is the "approved epic" gate. Acquire `state.try_acquire_run(&id)`; if `None` (a run already in flight) return `409 conflict` ("a run is already in flight for this epic"). Spawn `breakdown::spawn_breakdown`. Return `202 Accepted` with `{ "status": "breakdown_started" }` (the result streams over WS on `epic:<id>`).
- Add `breakdown` module to `lib.rs` `pub mod` list and wire `BreakdownAgent` into `AppState` (add `pub breakdown: Arc<dyn BreakdownAgent>` field; `AppState::new` uses `ClaudeBreakdownAgent::new()`; `with_planner` → add a `with_agents` or extend `with_planner` to also take the breakdown agent, OR add `with_breakdown`/`set_breakdown`. Keep it ergonomic for tests — tests inject both a scripted planner and scripted breakdown. Simplest: add a `with_agents(config, db, planner, breakdown)` constructor and have `with_planner` default the breakdown to `ClaudeBreakdownAgent::new()`; update existing test call sites of `with_planner` if the signature changes. Minimize churn to existing tests — prefer adding a new constructor and keeping `with_planner` as-is by giving `AppState` a `breakdown` field defaulted in `with_planner`.)

### E. Tests (hermetic, in `breakdown.rs` and `tasks.rs` and `mcp.rs`)
- `tasks.rs` unit tests: create_task round-trips; link_dependency rejects self-edge and cross-epic; cycle rejection (A blocks B, B blocks C, linking C→A rejected); list_tasks/list_dependencies correct.
- `mcp.rs` tests: breakdown-scope `tools/list` returns the two breakdown tools; `create_task` via the endpoint creates a task (assert row) and publishes `dag_updated`; `link_dependency` via endpoint links and rejects a cycle with `isError`; scope prevents addressing another epic (epic_id from scope, not args).
- `breakdown.rs` tests (scripted agent): `POST /epics/:id/breakdown` on an approved epic returns 202, the run streams events on `epic:<id>`, and on completion the epic is `Ready` + an `agent_run` row exists + `dag_updated`/`epic_updated` published. Reject breakdown when not in Planning (409), when no technical session (409), and when a run is in flight (409). Add one `#[ignore]`d live smoke test driving real `claude` (mirrors the planning live test).
- Keep `cargo test -p deerborn-server` fully green. Do NOT break existing tests.

### F. Docs (same change)
- Update `deerborn-server/CONVENTIONS.md`: add the `POST /epics/{id}/breakdown` row to the epics route table; add a "Breakdown phase" subsection under the MCP section describing the `create_task`/`link_dependency` tools and the `breakdown` capability scope; add `dag_updated` to the server→client WS frame table.
- Update `MILESTONE_1.md`: check the T-301 box (`- [x]`) and add a `**Done:**` note under it describing what landed (modules, routes, tools, scoping), matching the style of the T-205 Done note.

## Acceptance criteria (must all be true)
1. An approved epic (Planning + technical session) yields a persisted task DAG with dependencies when breakdown is triggered; tasks carry `title`/`description`/`acceptance`; epic is `Ready` after the run.
2. The breakdown agent creates tasks + edges via the `create_task`/`link_dependency` MCP tools only (scoped to the epic; cannot address another epic or change status beyond the Planning→Ready transition Deerborn owns).
3. `cargo test -p deerborn-server` is green (existing tests untouched + new tests pass).
4. `cd client && npm test` stays green (no client changes in this task).
5. CONVENTIONS.md + MILESTONE_1.md updated in the same change.

## Discipline
- Follow existing code style exactly (module doc comment, `now_ms()`, `AppResult`, `params!`, `json!({ "items": ... })`, CONVENTIONS wire shapes).
- IDs are ULIDs; timestamps unix-ms.
- Never log or return secrets. The breakdown run uses the read-only clone as cwd (same as planning).
- Do NOT run `git commit` — leave the change staged/unstaged for the team lead to review and commit. Do NOT start subagents.
- If you hit a genuine ambiguity, pick the simplest option consistent with CONVENTIONS.md and note it in your final summary.

---
**Output:**
Write your findings to exactly this path: /Users/josiahcampbell/projects/personal/deerborn/.pi-subagents/artifacts/outputs/a150fb9d/inline
This path is authoritative for this run.
Ignore any other output filename or output path mentioned elsewhere, including output destinations in the base agent prompt, system prompt, or task instructions.

## Acceptance Contract
Acceptance level: reviewed
Completion is not accepted from prose alone. End with a structured acceptance report.

Criteria:
- criterion-1: Implement the requested change without widening scope
- criterion-2: Return evidence sufficient for an independent acceptance review

Required evidence: changed-files, tests-added, commands-run, validation-output, residual-risks, no-staged-files

Review gate: required by reviewer.

Finish with a fenced JSON block tagged `acceptance-report` in this shape:
Use empty arrays when no items apply; array fields contain strings unless object entries are shown.
```acceptance-report
{
  "criteriaSatisfied": [
    {
      "id": "criterion-1",
      "status": "satisfied",
      "evidence": "specific proof"
    }
  ],
  "changedFiles": [
    "src/file.ts"
  ],
  "testsAddedOrUpdated": [
    "test/file.test.ts"
  ],
  "commandsRun": [
    {
      "command": "command",
      "result": "passed",
      "summary": "short result"
    }
  ],
  "validationOutput": [
    "validation output or concise summary"
  ],
  "residualRisks": [
    "none"
  ],
  "noStagedFiles": true,
  "diffSummary": "short description of the diff",
  "reviewFindings": [
    "blocker: file.ts:12 - issue found, or no blockers"
  ],
  "manualNotes": "anything else the parent should know"
}
```