# Milestone 1 — Half 1: Planning & Task Creation

> **Goal of this milestone:** stand up the entire *human-facing* half of Deerborn —
> projects, interactive epic planning, agent-driven task-DAG breakdown, the
> Ready-lane DAG editor, and the kanban — such that hitting **In Progress** lands
> claimable jobs in the queue. Execution of those jobs (the ralph loop) is
> **Milestone 2 / Half 2** and is deliberately out of scope here.
>
> This milestone stops at a clean seam: **an epic in `In Progress` with a valid
> task DAG and claimable rows in the queue.** A *stub worker* proves the seam;
> the real executor replaces it later.

See [ARCHITECTURE.md](./ARCHITECTURE.md) for the resolved v1 decisions and
[VISION.md](./VISION.md) for product intent. The [`references/`](./references)
dir (`ralph-v2.sh` + prompts) is the proven blueprint for Half 2 and the source
of truth for the **shared contract** frozen in §2 below.

---

## How to use this document

- Tasks are grouped into **phases**. Within the constraints of the dependency
  lines, implement them **one at a time, top to bottom**.
- Each task is a **vertical slice** with explicit **acceptance criteria (AC)**.
  A task is done when every AC is demonstrably true and the test suite is green.
- Every task must leave the tree **green and committed** before the next starts
  (ralph discipline — Deerborn will eventually run this way on itself).
- Check the box (`- [x]`) when a task is merged. If you deviate from a decision
  recorded here (stack choice, schema, contract), **update this document in the
  same change** — it is the single source of truth for Half 1.

---

## 1. Stack & conventions (locked by the first task)

These are proposals that **T-001 ratifies**. An implementing agent may swap a
choice only by updating this section and justifying it.

- **Server:** Rust, `tokio` + `axum` (REST + WebSocket), `serde`/`serde_json`.
- **DB:** embedded **libSQL** via the `libsql` crate (local file, single-writer).
  Migrations = ordered `.sql` files applied idempotently at boot.
- **Agent runtime:** [`agent-harness`](https://github.com/getlatentic/agent-harness)
  shelling out to Claude Code (default). **No custom agent loop.**
- **Git:** shell out to the `git` CLI (matches the reference; simplest for clone/fetch).
- **Client:** Vue 3 + TypeScript + Vite, Pinia for state. **Web-first**, SPA
  **served by the Deerborn binary**; Tauri shells are later.
- **Auth:** single-user static **bearer token** from config; every route except
  `/health` requires it.
- **Secrets:** PATs encrypted at rest with **AES-256-GCM**, key from
  `DEERBORN_MASTER_KEY` (env). Never returned by any API.
- **Testing:** each task ships tests. Server = `cargo test`; a `just test`
  target runs the whole gate (this becomes Deerborn's own `test_cmd`).
- **Layout (suggested, T-001 finalizes):** Cargo workspace with a `deerborn-server`
  crate and a `client/` Vite app; a `justfile` with `dev`/`test`/`build`.

---

## 2. The frozen shared contract (settle FIRST — do not casually change)

Half 1 and Half 2 are **not** independent; they share a data contract defined by
the **executor's** needs, not by what's convenient to display. This is copied
from what `ralph-v2.sh` / the prompt contracts actually consume. Freeze it before
building any UI on top of it. The queue/lease columns are written by Half 1 even
though the reaper/heartbeat/worker that read them are Half 2.

### 2.1 Rendered-spec format (what an implement/review agent ever sees)

A task renders to exactly this markdown (per `render_spec` in `ralph-v2.sh`):

```
# <title>

## Description
<description | "(none provided)">

## Acceptance Criteria
<acceptance_criteria | "(none provided)">
```

So the **only** task fields the executor consumes are `title`, `description`,
`acceptance_criteria`, and `blocks:` dependencies. The Task schema must carry
these first-class.

### 2.2 Baseline schema (libSQL / SQLite superset)

```sql
-- Projects: one repo, one KB (KB deferred to v2), optional build/test/run cmds.
CREATE TABLE project (
  id            TEXT PRIMARY KEY,          -- ulid/uuid
  name          TEXT NOT NULL,
  repo_url      TEXT NOT NULL,
  pat_encrypted BLOB,                      -- AES-256-GCM; never returned by API
  setup_cmd     TEXT,                      -- optional
  test_cmd      TEXT,                      -- optional; NULL => Half 2 skips test gate
  run_cmd       TEXT,                      -- optional
  clone_path    TEXT,                      -- canonical read-only checkout on disk
  clone_status  TEXT NOT NULL DEFAULT 'pending', -- pending|ready|error
  clone_error   TEXT,
  created_at    INTEGER NOT NULL,
  updated_at    INTEGER NOT NULL
);

-- Epic: unit of planning AND of concurrency/leasing (Half 2).
CREATE TABLE epic (
  id                TEXT PRIMARY KEY,
  project_id        TEXT NOT NULL REFERENCES project(id),
  title             TEXT NOT NULL,
  product_context   TEXT,                  -- maintained live by product-planning agent
  technical_context TEXT,                  -- maintained live by technical-planning agent
  status            TEXT NOT NULL DEFAULT 'Planning',
                    -- Planning|Ready|InProgress|Completed|Blocked|Cancelled
  branch_name       TEXT,                  -- deerborn/<project key>-<id> (Half 2)
  -- queue/lease columns: written by Half 1's enqueue, read by Half 2's claim
  lease_owner       TEXT,
  lease_expires_at  INTEGER,               -- unix ms
  created_at        INTEGER NOT NULL,
  updated_at        INTEGER NOT NULL
);

-- Task: a vertical slice; the executor's unit of work.
CREATE TABLE task (
  id             TEXT PRIMARY KEY,
  epic_id        TEXT REFERENCES epic(id), -- NULL => standalone task
  project_id     TEXT NOT NULL REFERENCES project(id),
  title          TEXT NOT NULL,
  description    TEXT,                      -- end-to-end behavior, not layer-by-layer
  acceptance     TEXT,                      -- acceptance_criteria
  status         TEXT NOT NULL DEFAULT 'Todo',
                 -- Todo|InProgress|Done|Failed|Cancelled  (readiness is COMPUTED from deps)
  failure_reason TEXT,                      -- Half 2: test_gate_exhausted|review_not_converged|blocked|agent_error
  agent_session_id TEXT,                    -- Half 2: the session that implemented it
  position       INTEGER,                   -- ordering hint within an epic/lane
  created_at     INTEGER NOT NULL,
  updated_at     INTEGER NOT NULL
);

-- Dependency edge: `blocker` blocks `blocked` (matches to-tasks `blocks:`).
CREATE TABLE task_dependency (
  blocker_id TEXT NOT NULL REFERENCES task(id),
  blocked_id TEXT NOT NULL REFERENCES task(id),
  PRIMARY KEY (blocker_id, blocked_id)
);

-- Durable planning transcript (source of truth, resumable, backend-portable).
CREATE TABLE transcript_message (
  id         TEXT PRIMARY KEY,
  epic_id    TEXT NOT NULL REFERENCES epic(id),
  phase      TEXT NOT NULL,                 -- product|technical
  role       TEXT NOT NULL,                 -- user|agent|tool|system
  content    TEXT NOT NULL,                 -- text or serialized RunEvent
  seq        INTEGER NOT NULL,              -- monotonic per epic
  created_at INTEGER NOT NULL
);

-- Per-run/per-stage evidence (mostly written by Half 2; table exists now).
CREATE TABLE agent_run (
  id         TEXT PRIMARY KEY,
  task_id    TEXT REFERENCES task(id),
  epic_id    TEXT REFERENCES epic(id),
  stage      TEXT NOT NULL,                 -- planning|breakdown|implement|review|judge|fix|...
  session_id TEXT,
  log        TEXT,
  created_at INTEGER NOT NULL
);

CREATE TABLE comment (
  id         TEXT PRIMARY KEY,
  task_id    TEXT REFERENCES task(id),
  epic_id    TEXT REFERENCES epic(id),
  author     TEXT NOT NULL,                 -- user|agent
  body       TEXT NOT NULL,
  created_at INTEGER NOT NULL
);
```

### 2.3 Claim semantics (Half 2 reads these; Half 1 must write the right shape)

- **Readiness** is computed, not stored: a task is *ready* when `status='Todo'`
  and every `blocker_id` pointing at it is `Done`.
- **Half 2 claim predicate** (documented now so Half 1 writes compatible rows):
  an epic is claimable when it has an actionable state, a free/expired lease, a
  ready task, **and no sibling task in the epic is `InProgress`**. Half 1's
  enqueue (T-403) sets `epic.status='InProgress'` and leaves `lease_owner` NULL.

### 2.4 Agent ↔ Deerborn tool surface for Half 1 (phase-scoped)

Only the **planning** and **breakdown** phases exist in Half 1:

| Phase | Agent may call (via local MCP) | Deerborn keeps |
|---|---|---|
| Planning (interactive) | `update_epic`, `read_codebase_context`, (`search_memory` = env assumption) | lane transitions |
| Breakdown (one-shot, human-gated) | `create_task`, `link_dependency` | — |

---

## 3. Phase 0 — Foundations

- [x] **T-001 — Workspace scaffolding & run story.** *deps: none*
  Cargo workspace (`deerborn-server` + `client/`), `justfile` (`dev`/`test`/`build`),
  README run instructions. **AC:** `cargo run` boots a server answering `200` on
  `GET /health`; `just dev` runs server + Vite together; `just test` is wired
  (may be near-empty). Ratifies §1 stack choices.

- [x] **T-002 — Config & single-user token auth.** *deps: T-001*
  Load config from env/file: bind addr, `DEERBORN_TOKEN`, `DEERBORN_MASTER_KEY`,
  db path, clone root. **AC:** a middleware rejects any request lacking
  `Authorization: Bearer <token>` except `/health`; env vars documented in README;
  missing `MASTER_KEY` fails fast at boot.

- [x] **T-003 — libSQL connection, migrations & baseline schema.** *deps: T-001*
  Connection pool + ordered-`.sql` migration runner applied idempotently at boot;
  land the full §2.2 schema as migration `0001`. **AC:** fresh boot creates all
  tables; re-boot is a no-op; a test inserts and reads back a `project` row.

- [x] **T-004 — REST scaffolding: error model & JSON envelope.** *deps: T-002, T-003*
  Consistent error type → HTTP status mapping; a JSON response/error envelope;
  request logging. **AC:** a deliberately failing handler returns the structured
  error shape; a documented list of route conventions exists.

- [x] **T-005 — WebSocket scaffolding & subscription protocol.** *deps: T-004*
  Authenticated WS endpoint; clients subscribe to topics (e.g. `epic:<id>`,
  `project:<id>`); define the message envelope (`{topic, type, payload}`). **AC:**
  a client connects with the token, subscribes, and receives a server-pushed test
  event; unauthenticated connect is rejected.

- [x] **T-006 — Vue/TS client shell served by the binary.** *deps: T-004*
  Vite SPA; the built assets are served at `/` by the Rust binary (dev proxies to
  Vite). A token entry screen persists the bearer token. **AC:** `cargo run` (prod
  build) serves the SPA; an authenticated call (e.g. `GET /projects`) succeeds and
  renders; wrong token shows an auth error.

---

## 4. Phase 1 — Projects

- [x] **T-101 — Project CRUD API.** *deps: T-004*
  Create/list/get/update/delete projects. `name` + `repo_url` required;
  `setup/test/run_cmd` optional. **AC:** full CRUD round-trips; validation errors
  are structured; `test_cmd` NULL is allowed and preserved.

- [x] **T-102 — PAT encryption at rest.** *deps: T-101, T-002*
  Encrypt the PAT with AES-256-GCM (key from `DEERBORN_MASTER_KEY`) before insert;
  provide an internal decrypt path. **AC:** PAT never appears in any API response
  or log; stored bytes are ciphertext; a round-trip test encrypts→decrypts.

- [x] **T-103 — Canonical read-only clone lifecycle.** *deps: T-101, T-102*
  On project create, `git clone` (using the decrypted PAT over HTTPS) into a
  per-project dir under the clone root; record `clone_path`/`clone_status`; expose
  a refresh (`git fetch`) action; clone failure → `clone_status='error'` + message.
  **AC:** creating a project against a real GitHub repo yields a `ready` clone on
  disk; a bad URL/PAT yields `error` with a readable reason; refresh updates it.

- [x] **T-104 — Projects UI (list, create, detail shell).** *deps: T-006, T-101*
  Create-project form (name, repo URL, PAT, optional cmds); projects list;
  clicking a project opens an (empty) detail/kanban page. **AC:** a user creates a
  project end-to-end in the browser and sees it listed; detail page loads.

---

## 5. Phase 2 — Interactive planning  *(the spike lives here)*

- [x] **T-200 — [SPIKE] agent-harness interactive multi-turn PoC.** *deps: T-001*
  Standalone example driving Claude Code via `agent-harness` across **≥2 turns**,
  streaming normalized `RunEvent`s. Document: native session-resume vs.
  transcript-replay fallback; how tool-calls surface mid-conversation; crate
  maturity/license. **AC:** a working multi-turn transcript is produced and the
  findings are written up. **GATE:** if interactive support is inadequate, stop
  and revisit the architecture before building T-201+.
  **Done:** verdict PROCEED-WITH-CAVEATS — see [`docs/spikes/T-200-agent-harness.md`](./docs/spikes/T-200-agent-harness.md).
  Crate is `agent-harness = "=0.3.5"` (imported as `harness`, MIT/Apache-2.0).
  Native resume works (`session_id` from `RunEvent::Session`); keep the durable
  `transcript_message` store as source of truth with a replay fallback. MCP is
  wired via `RunTuning.extra_args` (`--mcp-config` + `--allowedTools` +
  `--permission-mode bypassPermissions`); read-only is enforced by tool-scoping +
  a read-only checkout, **not** `RunMode::Ask`. Enums are `#[non_exhaustive]`.

- [ ] **T-201 — Transcript store & planning-session lifecycle.** *deps: T-003, T-200*
  Create a planning session bound to a (new) `Planning`-status epic; persist every
  user + agent + tool message to `transcript_message` with monotonic `seq`;
  resumable after a server restart. **AC:** messages persist and reload in order;
  a restarted server can resume an in-flight session.

- [ ] **T-202 — Planning agent run + WS streaming.** *deps: T-201, T-005*
  A user message triggers an agent run via the harness; `RunEvent`s (text,
  reasoning, tool calls, lifecycle) stream over WS to subscribers on `epic:<id>`;
  the transcript updates live. **AC:** a browser sends a message and watches the
  agent's response stream token-by-token; the exchange is durably stored.

- [ ] **T-203 — Local MCP server: `update_epic` + `read_codebase_context`.** *deps: T-202, T-103*
  Expose Deerborn's local MCP server to the shelled-out agent, scoped to the
  planning tool surface (§2.4). `update_epic` maintains `epic.product_context` /
  `technical_context`; `read_codebase_context` reads the project's canonical clone
  **read-only**. **AC:** during a chat the agent calls `update_epic` and the Epic
  record changes live on the client; the agent can quote real code from the clone;
  the agent cannot mutate lane/status.

- [ ] **T-204 — Planning chat UI + Epic record view.** *deps: T-104, T-202, T-203*
  A chat panel (streaming) beside a live-updating Epic record; a "start planning"
  flow that lands a new Epic in the **Planning** lane. **AC:** a user plans an epic
  entirely in the browser and sees the Epic record fill in as they talk.

- [ ] **T-205 — Second planning config (technical planning).** *deps: T-204*
  Two prompt configs (product + technical) share the one chat/transcript engine;
  the user advances an epic from product → technical planning on the same
  transcript. **AC:** both phases run against the same epic; `phase` is recorded on
  each message; technical planning has code-inspection context.

---

## 6. Phase 3 — Breakdown & the DAG

- [ ] **T-301 — One-shot breakdown agent (`to-tasks`).** *deps: T-205*
  On request, run the breakdown agent (vertical-slice / tracer-bullet logic from
  `references/prompts/to-tasks.md`) against the approved epic; it creates `task`
  rows + `blocks:` edges via the `create_task` / `link_dependency` MCP tools; the
  epic moves **Planning → Ready**. **AC:** an approved epic yields a persisted task
  DAG with dependencies; tasks carry `title`/`description`/`acceptance`; epic is
  `Ready`.

- [ ] **T-302 — DAG validation & readiness API.** *deps: T-301*
  Reject cycles; compute per-task readiness (§2.3); expose the DAG (nodes + edges)
  and each task's ready/blocked state via API. **AC:** a cyclic edit is rejected
  with a clear error; the API returns a correct topological readiness set.

- [ ] **T-303 — Ready-lane DAG editor UI.** *deps: T-104, T-302*
  Visualize the task DAG; task CRUD; rewire dependencies by hand; surface
  validation errors (cycles, orphaned deps). This is **the highest-ROI human
  checkpoint** before execution. **AC:** a user edits tasks and dependencies of a
  `Ready` epic and the persisted DAG reflects it; invalid edits are blocked.

---

## 7. Phase 4 — Kanban & the enqueue seam

- [ ] **T-401 — Project-detail kanban (epics + parentless tasks).** *deps: T-104, T-005*
  Lanes: `Planning | Ready | In Progress | Completed | Cancelled` (+ `Blocked`
  surfaced). Show epics and standalone tasks; update live over WS; allow the
  permitted lane transitions. **AC:** epics appear in the right lanes and move
  between them; two browsers see the same board update in real time.

- [ ] **T-402 — Epic-detail kanban (tasks within an epic).** *deps: T-401, T-302*
  Drill into an epic to a task kanban reflecting DAG status. **AC:** task statuses
  render correctly; drilling in/out preserves state.

- [ ] **T-403 — Enqueue on In Progress + stub worker (the seam).** *deps: T-401, T-303*
  Moving an epic **Ready → In Progress** writes the queue/lease shape from §2.2/§2.3
  (`epic.status='InProgress'`, lease NULL, tasks claimable). Ship a **stub worker**
  that claims ready tasks in dependency order and marks them `Done` (no real
  agent, no git). **AC:** hitting In Progress drives the board through the DAG to
  `Completed` via the stub, end-to-end; the rows written are exactly what Half 2's
  claim predicate will read. **This is the Milestone-1 finish line.**

---

## 8. Definition of done for Milestone 1

A user can, entirely in the browser: create a project (repo cloned read-only) →
plan an epic by chatting with the product **and** technical planning agents (Epic
record filled live) → trigger a breakdown into a task DAG → hand-edit that DAG in
the Ready lane → hit **In Progress** and watch the stub worker walk the DAG to
Completed. Every transcript, epic, task, and dependency is durably stored; the
queue rows match the frozen contract so Half 2 can drop in.

**Validation bonus (no new code):** export a generated DAG to `beads` and run the
existing `references/ralph-v2.sh` by hand against a scratch repo to sanity-check
breakdown **granularity** before Half 2 exists.

---

## 9. Explicitly out of scope (Half 2 / later)

Real agent execution (implement/test-gate/commit/review/judge/fix/close) ·
epic leases/heartbeat/reaper/crash-recovery · one-PR-per-epic + `GitHost` ·
`Blocked` failure handling + branch push · per-run timeouts/budget caps ·
Deerborn-managed agentmemory · Gitea/other hosts · Tauri desktop/mobile shells ·
mobile push · multi-user/RBAC · worktrees · containerized build envs.
