# Dearborn — v1 Architecture Decisions

Derived from a design interview over [VISION.md](./VISION.md). This captures the
**resolved** decisions and the deliberate **v1 → v2** line. The `references/`
directory (ralph-v2 + prompt contracts) is the proven blueprint for the
orchestrator; Dearborn reimplements it in Rust with a libSQL job queue.

---

## v1 scope in one paragraph

A self-hosted Rust server (plus a manually-run agentmemory sidecar) that turns an
approved epic into a PR autonomously. The user plans an epic via interactive
chat agents, reviews an agent-generated task DAG in the `Ready` lane, then hits
`In Progress`; a background worker walks the DAG **serially within the epic**,
running each task through `implement → test-gate → commit → review+verdict →
fix-loop → close`, and opens **one PR per epic**. Multiple epics run in parallel.
Clients are a shared Vue/Tauri codebase, web-first.

---

## 1. Agent runtime
- **Shell out to CLI coding agents** (Claude Code default), via the Rust crate
  **[agent-harness](https://github.com/getlatentic/agent-harness)** — one
  `Harness` trait, normalized `RunEvent` stream (text, reasoning, tool calls,
  tokens, lifecycle).
- **Prompts, harness, and model are runtime-configurable per project agent
  slot** (global defaults + per-project overrides; evidence recorded per run).
  See [docs/design/agent-and-project-settings.md](./docs/design/agent-and-project-settings.md).
- Dearborn does **not** implement its own agent loop.

## 2. Concurrency & isolation
- **Concurrency unit = the epic.** Serial *within* an epic (topological DAG
  order), parallel *across* epics (same or different project).
- Bounded by a **worker-pool size** (default **2**, configurable).
- **Isolation = a full `git clone` per epic** in v1 (simple, zero worktree edge
  cases). Worktrees are a later disk/speed optimization.

## 3. Job model (the worker)
- **Task-as-job:** the whole per-task pipeline is one atomic claim; a terminal
  **`finalize-epic`** step opens the PR.
- **Lock/lease unit = the epic; work/checkpoint unit = the task.** A worker
  claims an epic with actionable state + a free lease, does **one** unit of work
  (`prepare` → each `run_task` → `finalize`), checkpoints, releases.
- **Claim predicate:** task is dependency-ready **AND** no sibling task in the
  epic is in progress. This one rule yields serial-within / parallel-across.

## 4. Per-task pipeline
```
implement → [test gate: Dearborn runs tests, fix-loop ≤N] → commit
          → review+verdict (single agent) → fix-loop ≤N (re-test, re-commit) → close
```
- Merged **review+judge into one agent** (ralph-v2 was over-split). Mitigation:
  **strict output contract** — `VERDICT: PASS|NEEDS_CHANGES|BLOCKED` on the first
  line, one reparse retry.
- **Dearborn owns all determinism** (ralph-v2 discipline): the test gate, git,
  commits, status transitions, close, PR. Agents are pure functions.

## 5. Project build/test config
- Per-project **user-specified, optional** `{ setup_cmd, test_cmd, run_cmd }`.
- **No `test_cmd` → the test gate is skipped** (task relies on review+verdict).
- **Host VPS is assumed to have the toolchains**; `setup_cmd` installs deps into
  the fresh clone. Containerization is v2.

## 6. Queue mechanics (libSQL)
- **Atomic claim** via SQLite single-writer:
  `UPDATE epics SET lease_owner, lease_expires_at WHERE actionable AND lease expired RETURNING …`.
  SQLite's write serialization *is* the lock.
- **Heartbeat lease** (task runs are long) + a **reaper** that requeues expired
  leases.
- **Crash recovery = re-attach, not restart:** the epic's on-disk clone keeps its
  branch + per-task commits; `git reset --hard` to last commit, re-run the
  interrupted task (idempotent — incomplete tasks never committed).
- **Wakeups:** in-process notify on enqueue + slow fallback poll (~1–2s).

## 7. Failure & human-in-the-loop
- Failure is **epic-scoped, not fatal.** Task → **`Failed`** with a structured
  reason; epic → **`Blocked`** (new status) carrying the same reason; worker
  continues on other epics. No PR. The reason set (MILESTONE_2 §2.3), as
  actually shipped, is ten values: `preflight_red | setup_failed |
  workspace_error | test_gate_exhausted | review_not_converged | blocked |
  agent_error | timeout | cancelled | pr_failed`. This section's original four
  — `test_gate_exhausted | review_not_converged | blocked | agent_error` — are
  **preserved unchanged**; the other six are the conditions ralph-v2 handled by
  `die`ing, which Dearborn instead names and routes through the identical
  `Blocked`/`Failed` path rather than aborting on: `preflight_red` (a red tree
  before any task ran), `setup_failed`/`workspace_error` (provisioning never
  got far enough to blame a task), `timeout` (T-543's per-stage wall clock,
  routed as an ordinary agent-stage failure — a stage that ran too long is a
  failure of that attempt, not a signal to stop), `cancelled` (named in the
  vocabulary for a human-initiated stop, though in practice no path routes a
  cancellation through this same failure router — see the Cancel bullet
  below), and `pr_failed` (push or PR-open itself failed after everything else
  succeeded).
- A failed task **halts its epic immediately** rather than continuing on
  independent DAG branches: a half-built epic with a hole in it yields a
  PR-shaped branch nobody can review, and later slices would be reviewed against
  a dependency they were told exists.
- **On `Blocked`, push the epic branch to the remote** so the user clones &
  triages locally (no VPS spelunking). Optional per-task push as an off-box
  durability flag.
- Preserve evidence: **per-stage logs + the agent session id per task** (also
  satisfies VISION §2 "tasks track the agent session that implemented them").
- **v1 recovery actions: Retry task + Cancel epic.** Retry is one atomic
  transition (`POST /tasks/{id}/retry`), but the transition it performs
  depends on what kind of task it is: an **epic-scoped** task retries
  `Failed → Todo` + epic `Blocked → InProgress` + lease clear + notify, exactly
  as originally designed. A **standalone** task (no epic) instead retries
  `Failed → InProgress` directly — not `Todo` — because the worker's claim
  query only ever selects `InProgress` standalone tasks, so a standalone task
  is both the claimable item and the unit of work at once; restoring its
  claimability *is* resetting its work. Edit-spec-then-retry needs no new
  surface either way — `PATCH /tasks/{id}` already edits the spec, so the
  client issues the two calls back-to-back.
- **Cancel is a kill, not a stage-boundary flag:** an in-process registry holds
  the live `RunHandle`, so `InProgress → Cancelled` terminates the agent in
  seconds instead of after a 30-minute stage. This exists only at the **epic**
  level (`POST /epics/{id}/lane` with `status: "Cancelled"`) — there is no
  standalone-task-level cancel endpoint; it was deliberately left out when
  standalone tasks landed, not an oversight.

## 8. Epic → task DAG breakdown
- **One-shot breakdown agent** (the `to-tasks` vertical-slice / tracer-bullet
  logic) generates tasks + `blocks:` dependencies.
- **Human checkpoint in the `Ready` lane:** the DAG lands there visible and
  **editable by hand** (task CRUD + dep rewiring) before the user hits
  `In Progress`. Highest-ROI human review in the system.
- Lane flow: `Planning → (breakdown) → Ready → In Progress → Completed`, plus
  `Blocked` and `Cancelled`.

## 9. Knowledge base (agentmemory) — mostly deferred
- **v1: agentmemory is an environment assumption, not Dearborn code.** The user
  manually runs agentmemory and pre-configures each coding agent (plugins / MCP /
  lifecycle hooks) so memory capture happens automatically when Dearborn shells
  out to them.
- Dearborn ships **zero agentmemory-aware code** in v1 — no supervision, no
  namespacing, no `API → Memory` reads. Only the agents touch memory.
- agentmemory is a self-contained Node server (SQLite + vectors; REST :3111,
  MCP with 53 tools, viewer :3113) with hook-based auto-capture + MCP save/recall.
- **v2:** Dearborn supervises the process, handles per-project namespacing, and —
  enabled by the libSQL choice below — can absorb agentmemory's store into its own
  DB without swapping engines.

## 10. Planning agents (interactive)
- **Same machine, two prompt configs:** product-planning (business scope) and
  technical-planning (code-focused) share one chat/transcript mechanism.
- **Dearborn owns the durable transcript** per epic (source of truth, resumable,
  portable across backends); may ride native session-resume purely as a cost
  optimization. Each user message → an agent run → `RunEvent`s streamed to the
  client over WS.
- **Read-only checkout** of the per-project canonical clone for code inspection
  (no epic branch yet).
- Output artifact = the **Epic record**, maintained live by the agent via an
  `update_epic` tool.

## 11. Agent ↔ Dearborn boundary (MCP)
- Dearborn exposes a **local MCP server**; shelled-out agents connect back to it.
- **Phase-scoped tool surface**, holding the ralph determinism line:
  | Phase | Agent may call | Dearborn keeps |
  |---|---|---|
  | Planning (interactive) | `update_epic`, `search_memory`, `read_codebase_context` | lane transitions |
  | Breakdown (one-shot, human-gated) | `create_task`, `link_dependency` | — |
  | Implement/review (autonomous) | read-only spec context; progress **comments** only | **status, commits, git, close, PR** |
- **Inside the autonomous loop, agents never mutate lifecycle/git/status.**

## 12. Data store
- **Embedded libSQL** (local file, single-writer server), used as plain
  relational storage in v1.
- Chosen for **forward-compat**: absorb agentmemory's SQLite-based store in v2
  without changing engines (libSQL = SQLite superset with vectors).
- Clients are **thin/online** against the server; server is the single source of
  truth. (Offline-first synced replicas = possible later, not a v1 driver.)

## 13. Auth & tenancy
- **Single-user, token-based auth; single-tenant schema (no `user_id`).**
- **TLS via the user's reverse proxy** (Caddy/nginx/Traefik), documented not built.
- Multi-user/teams/RBAC = explicit v2 (deliberate migration then).

## 14. Git-host integration
- **`GitHost` trait** (`clone / push / open_pr / check_auth`).
- **v1: GitHub only.** Gitea = v2 (slots into the trait).
- **Per-project PAT, encrypted at rest**, used for git-over-HTTPS + the host API.
- **One PR per epic**, branch `dearborn/<project key>-<id>`. The PR base is the
  epic's **recorded base branch** (snapshotted at provision from an optional
  epic/project setting; repo default when unset) — `open_pr` no longer does a
  live default-branch lookup.

## 15. Transport & clients
- **REST for commands/queries; WebSocket for live subscriptions** (kanban/status
  changes, relayed agent `RunEvent`s, planning-chat streaming).
- **One Vue/TS codebase, three shells.** Web = the SPA **served by the Dearborn
  binary**; desktop/mobile = **Tauri shells** of the same app pointed at the
  server. **Web-first in v1.**
- **Mobile push (APNs/FCM) = v2.** v1 uses in-app WS notifications while connected.

---

## Data-model sketch (entities)
`Project` (repo URL, encrypted PAT, `setup/test/run` cmds) · `Epic` (status,
product/technical context, lease fields) · `Task` (status, spec, agent session id,
epic FK) · `TaskDependency` (`blocks`) · `AgentRun` / `TaskLog` (per-stage logs,
session id, artifacts) · `Comment` · planning `Transcript`. No `user_id` in v1.

## Pre-build spikes (verify before committing)
1. Interactive multi-turn via agent-harness — native session-resume support vs.
   transcript-replay fallback.

## Deferred to v2 (explicit)
Parallel-within-epic worktrees · containerized build envs · inferred project
config · Dearborn-managed agentmemory (supervision, namespacing, DB
consolidation) · Gitea + other hosts · multi-user/teams/RBAC · offline-first
synced clients · mobile push notifications · issue-tracker integrations
(Linear/GitHub Issues) · multi-repo projects.
