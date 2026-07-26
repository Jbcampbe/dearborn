# Milestone 2 — Half 2: The Executor

> **Goal of this milestone:** replace the stub worker with the real thing — a
> leased worker pool that claims an `In Progress` epic, provisions an isolated
> clone, walks its task DAG through `implement → test-gate → commit →
> review+verdict → fix-loop → close`, and opens **one PR per epic**. Plus the
> same pipeline for standalone tasks, the human-in-the-loop recovery path when
> it fails, and enough client surface to watch and drive it.
>
> Milestone 1 stopped at a clean seam: an epic in `InProgress` with a valid task
> DAG and claimable rows in the queue. This milestone consumes that seam and
> deletes the stub.

See [ARCHITECTURE.md](./ARCHITECTURE.md) for the resolved v1 decisions,
[MILESTONE_1.md](./MILESTONE_1.md) for the frozen shared contract (§2), and
[`references/ralph-v2.sh`](./references/ralph-v2.sh) for the proven blueprint
this milestone reimplements in Rust.

---

## How to use this document

- Same discipline as Milestone 1: phases, implemented **one at a time, top to
  bottom**, each task a **vertical slice** with explicit **acceptance criteria**.
- Every task leaves the tree **green and committed** (`just test`) before the
  next starts.
- Every task that changes the HTTP/WS contract updates
  [`dearborn-server/CONVENTIONS.md`](./dearborn-server/CONVENTIONS.md) **in the
  same change**. Deviating from a decision recorded here means updating this
  document in the same change.
- **Progress is tracked in this file.** Each task carries a checkbox and a
  `deps:` line naming the tasks that must be done first. To pick up work: find
  the topmost `- [ ]` task whose deps are all `- [x]`, and check its box (`- [x]`)
  **in the same commit** that lands the task. A task is checkable only when every
  AC is demonstrably true and `just test` is green — a box is a claim about the
  tree, not about effort spent.
- If a task is abandoned or superseded, say so inline (`- [~] … *superseded by
  T-xxx*`) rather than deleting it; the trail is what makes this file usable as
  a status report.

---

## 1. Decisions ratified for this milestone

Resolved in design review; each extends or sharpens an ARCHITECTURE section.

| # | Decision | Source |
|---|----------|--------|
| D1 | Milestone ends at a **real opened PR**, not a pushed branch. | §14 |
| D2 | **N long-lived worker loops** (default 2, `DEARBORN_WORKER_CONCURRENCY`); the lane handler enqueues + notifies, it never spawns. | §3 |
| D3 | Epic workspace = **local clone off the canonical checkout** (refresh canonical → `git clone <canonical>` → origin rewritten to the real remote → branch). Persists across re-claims; deleted after the PR opens; retained on `Blocked`/`Cancelled`. | §2 |
| D4 | **Implicit lease expiry** (no reaper task) + heartbeat **fencing** + boot-time lease clear. | §6 |
| D5 | Keep ralph's **preflight green-tree gate** and **already-complete verification**. Drop the cheap commit-message agent — commit subjects are deterministic. | §4 |
| D6 | **One `TaskAgent` trait + `Stage` enum**, mirroring `PlanningAgent`/`BreakdownAgent`. Prompts are `include_str!` markdown compiled into the binary. | §1 |
| D7 | Autonomous MCP surface = **`add_comment` only**. Everything readable is pre-baked into the prompt. | §11 |
| D8 | Prompt context = rendered spec (§2.1) + epic context + a **sibling manifest** split into "already built" and "owned by later tasks". | §2.1 |
| D9 | Verdict = **last** line matching `^VERDICT:\s*(PASS\|NEEDS_CHANGES\|BLOCKED)\s*$`, one re-run on a miss. Each review round sees the **cumulative** diff from the task's base SHA. | §4 |
| D10 | A failed task **halts its epic immediately** (`Blocked`); the failed task's uncommitted diff stays in the retained workspace and is never pushed. | §7 |
| D11 | Recovery = **`POST /tasks/{id}/retry`**, one atomic transition (task `Failed → Todo` + epic `Blocked → InProgress` + lease clear + notify). Edit-spec-then-retry falls out of the existing `PATCH /tasks/{id}`. | §7 |
| D12 | Cancel is a **kill**: an `AppState` cancel registry holds the live `RunHandle`; `InProgress → Cancelled` calls `RunControl::cancel()`. Stage-boundary DB check as backstop. | §7 |
| D13 | Evidence is **DB-only, capped** (~256 KB head+tail per log). `agent_run` gains `attempt`/`status`/`verdict`/`started_at`/`ended_at`/`exit_code`. | §7 |
| D14 | Fine-grained `RunEvent`s stream on a new **`task:<id>`** topic; `epic:<id>` keeps coarse frames. `agent_run.log` flushes every ~2s so mid-run joiners hydrate over REST. | §15 |
| D15 | **`reqwest` (rustls-tls) + a `GitHost` trait**; `GithubHost` for real, `FakeHost` for tests. | §14 |
| D16 | PR body = **deterministic template + an agent-written summary section**, with hard fallback to template-only. | §14 |
| D17 | **Standalone tasks execute in M2** — their own lease, branch, and PR — via a unified `WorkItem` enum, *not* a parallel code path. | extends §3 |
| D18 | **Per-stage wall-clock timeouts** (agent stages and shell commands). No epic-level budget: the fix-round caps already bound a task, and task count bounds the epic. | new |
| D19 | **Fresh agent context every stage** (`resume: None`). Data flows between stages only through Dearborn. | §4 |
| D20 | All new tunables are **global env vars**; per-project config stays `{setup,test,run}_cmd`. | §5 |
| D21 | Build order is **tracer-bullet first**, then thicken each stage. | new |

---

## 2. Contract additions (settle FIRST)

### 2.1 Schema migration — `0004_executor.sql`

```sql
-- Epic: PR identity + why it blocked.
ALTER TABLE epic ADD COLUMN pr_url         TEXT;
ALTER TABLE epic ADD COLUMN pr_number      INTEGER;
ALTER TABLE epic ADD COLUMN blocked_reason TEXT;

-- Task: standalone tasks are leasable work items with their own branch + PR.
ALTER TABLE task ADD COLUMN lease_owner      TEXT;
ALTER TABLE task ADD COLUMN lease_expires_at INTEGER;   -- unix ms
ALTER TABLE task ADD COLUMN branch_name      TEXT;
ALTER TABLE task ADD COLUMN pr_url           TEXT;
ALTER TABLE task ADD COLUMN pr_number        INTEGER;
ALTER TABLE task ADD COLUMN base_sha         TEXT;      -- set at claim; review diffs against it

-- agent_run becomes the per-stage evidence table (agent AND non-agent stages).
ALTER TABLE agent_run ADD COLUMN attempt    INTEGER NOT NULL DEFAULT 1;
ALTER TABLE agent_run ADD COLUMN status     TEXT NOT NULL DEFAULT 'running';
                     -- running|ok|error|timeout|cancelled
ALTER TABLE agent_run ADD COLUMN verdict    TEXT;       -- PASS|NEEDS_CHANGES|BLOCKED (review only)
ALTER TABLE agent_run ADD COLUMN started_at INTEGER;
ALTER TABLE agent_run ADD COLUMN ended_at   INTEGER;
ALTER TABLE agent_run ADD COLUMN exit_code  INTEGER;

-- Claim-path indexes.
CREATE INDEX IF NOT EXISTS idx_epic_claim ON epic(status, lease_expires_at);
CREATE INDEX IF NOT EXISTS idx_task_claim ON task(status, epic_id, lease_expires_at);
CREATE INDEX IF NOT EXISTS idx_agent_run_task ON agent_run(task_id, created_at);
```

`session_id` stays NULL for non-agent stages. `log` is capped at
`LOG_CAP_BYTES` (256 KB) keeping head + tail with an elision marker — the
informative parts of a transcript are its beginning and its end.

### 2.2 Stage vocabulary (`agent_run.stage`)

| Stage | Agent? | `RunMode` | Notes |
|---|:--:|---|---|
| `setup` | no | — | `setup_cmd` in the fresh workspace |
| `preflight` | no | — | `test_cmd` on the untouched tree; red ⇒ `Blocked` |
| `implement` | yes | `Edit` | one per task |
| `test_gate` | no | — | `test_cmd`; `attempt` = 0..N |
| `fix` | yes | `Edit` | driven by test output **or** review findings |
| `verify_complete` | yes | `Ask` | only when implement produced no diff |
| `review` | yes | `Ask` | emits the `VERDICT:` line; `attempt` = review round |
| `commit` | no | — | records the SHA in `log` |
| `push` | no | — | |
| `summarize` | yes | `Ask` | PR-body summary; failure is non-fatal |

### 2.3 Failure reasons (`task.failure_reason`, `epic.blocked_reason`)

`preflight_red` · `setup_failed` · `workspace_error` · `test_gate_exhausted` ·
`review_not_converged` · `blocked` (agent returned `BLOCKED`) · `agent_error` ·
`timeout` · `cancelled` · `pr_failed`

§7's original four are preserved; the rest name conditions the reference script
handled by `die`ing.

### 2.4 Claim semantics

```sql
-- Epic claim (tried first).
UPDATE epic SET lease_owner = ?worker, lease_expires_at = ?now + ?ttl, updated_at = ?now
WHERE id = (SELECT id FROM epic
            WHERE status = 'InProgress'
              AND (lease_owner IS NULL OR lease_expires_at < ?now)
            ORDER BY updated_at ASC LIMIT 1)
RETURNING id, project_id;

-- Standalone-task claim (fallback), same shape with
-- status='InProgress' AND epic_id IS NULL.
```

SQLite's write serialization **is** the lock (§6). Heartbeat is
`UPDATE … SET lease_expires_at = ? WHERE id = ? AND lease_owner = ?` — **0 rows
affected means the lease was lost**: cancel the agent, stop writing, abandon the
item. At boot, all leases are cleared (single-server, §13) so a restart resumes
immediately instead of waiting out a TTL.

### 2.5 New HTTP endpoints

| Action | Method + path | Success |
|---|---|---|
| retry a failed task | `POST /tasks/{id}/retry` | `200` (task); `409` unless `Failed` |
| run a standalone task | `POST /tasks/{id}/run` | `200` (task); `409` unless `Todo` + `epic_id IS NULL` |
| read a task's stage history | `GET /tasks/{id}/runs` | `200` (`items` of `agent_run`, oldest first) |
| read one stage's log | `GET /runs/{id}` | `200` (full capped log) |

### 2.6 New WebSocket frames

| Topic | `type` | Payload |
|---|---|---|
| `task:<id>` | *(RunEvent mapping — reuses `planning::ws_type`)* | serialized `RunEvent` verbatim |
| `task:<id>` | `stage_changed` | `{ task_id, stage, attempt, status, verdict? }` |
| `epic:<id>` | `stage_changed` | same, coarse — drives the task card's sub-label |
| `epic:<id>` | `dag_updated`, `epic_updated` | unchanged from M1 |
| `project:<id>` | `board_updated` | unchanged from M1 |

### 2.7 Configuration

| Variable | Default | Purpose |
|---|---|---|
| `DEARBORN_WORKER_CONCURRENCY` | `2` | worker loops |
| `DEARBORN_LEASE_TTL_SECS` | `300` | lease lifetime |
| `DEARBORN_HEARTBEAT_SECS` | `30` | renewal interval |
| `DEARBORN_AGENT_STAGE_TIMEOUT_SECS` | `1800` | per agent stage |
| `DEARBORN_CMD_TIMEOUT_SECS` | `900` | per `setup_cmd` / `test_cmd` run |
| `DEARBORN_MAX_TEST_FIX_ATTEMPTS` | `3` | ralph parity |
| `DEARBORN_MAX_FIX_ROUNDS` | `3` | ralph parity |
| `DEARBORN_VERDICT_RETRIES` | `1` | ralph parity |
| `DEARBORN_POLL_INTERVAL_MS` | `1500` | fallback poll behind the notify |

`DEARBORN_STUB_WORKER_DELAY_MS` is **removed** when the stub dies (T-513).

### 2.8 Naming

- Epic branch: `dearborn/<slug(epic.title)>-<last 6 of epic id, lowercased>`
- Standalone branch: `dearborn/task-<slug(task.title)>-<last 6 of task id>`
- Epic workspace: `<clone_root>/epics/<epic id>` · standalone:
  `<clone_root>/tasks/<task id>` · canonical stays `<clone_root>/<project id>`
- Commits: `impl(<short task id>): <task title>` and
  `fix(<short task id>) review round <N>: <task title>`

---

## 3. Phase 0 — Contract & foundations

- [x] **T-500 — Executor schema migration.** *deps: none*
  Apply §2.1 as `migrations/0004_executor.sql`. **AC:** migration applies
  idempotently at boot on a fresh **and** an existing `dearborn.db`; every new
  column round-trips through the relevant row structs; existing M1 tests stay
  green; `Task`/`Epic` API responses expose `pr_url`, `failure_reason`,
  `blocked_reason` (never lease columns).

- [x] **T-501 — Executor config surface.** *deps: none*
  Add §2.7 to `config.rs` as an `ExecutorConfig` nested in `Config`. **AC:**
  every var parses from env and the `KEY=VALUE` file with the documented
  default; invalid values fall back to the default with a `warn` (never a boot
  failure); `Config::for_test` yields test-fast values (concurrency 1, poll
  10 ms, timeouts short); README's configuration table updated.

- [x] **T-502 — Spec rendering & prompt assembly (pure).** *deps: none*
  `spec.rs`: the §2.1 renderer plus the D8 context builder, and the D9 verdict
  parser. Prompts live in `dearborn-server/prompts/*.md`, pulled in with
  `include_str!`. **AC:** renderer output matches §2.1 byte-for-byte including
  the `(none provided)` fallbacks; context builder emits epic context + a
  sibling manifest partitioned Done / not-yet with explicit "do not implement"
  framing; verdict parser unit-tested against preamble-before-verdict, multiple
  `VERDICT:` mentions (last wins), trailing whitespace, lowercase (rejected),
  and absent (returns `None`); no I/O in the module.

---

## 4. Phase 1 — The tracer bullet

The thinnest end-to-end path: claim → workspace → implement → commit → push →
PR. **Deliberately missing:** test gate, review loop, fix loops, structured
failure handling, standalone tasks. Anything that fails here Blocks the epic
with `agent_error` and gets thickened in later phases.

- [x] **T-510 — Worker pool, lease, heartbeat.** *deps: T-500, T-501*
  Replace `spawn_stub_worker` with N loops started in `main`: notify-or-poll →
  claim (§2.4) → run → release. Heartbeat task per claimed item with fencing;
  `tokio::sync::Notify` on `AppState`; boot-time lease clear. The pipeline body
  is still the M1 stub walk, so the tree stays green. **AC:** `lanes.rs` no
  longer spawns anything — it sets status, clears the lease, and calls
  `notify_waiters()`; two workers racing one epic yield exactly one claim (test
  hammers the claim SQL concurrently); an expired lease is re-claimable and the
  new owner resets `InProgress` tasks → `Todo`; a heartbeat against a stolen
  lease reports lost and aborts; boot clears all leases; with concurrency 2 and
  3 enqueued epics, exactly 2 run concurrently.

- [x] **T-511 — Epic workspace provisioning & re-attach.** *deps: T-510*
  `workspace.rs`: per-project refresh lock → `refresh_repo(canonical)` →
  `git clone <canonical> <ws>` → `git remote set-url origin <token-free url>` →
  `git checkout -b <branch>` → `setup_cmd`. Re-claim of an existing workspace
  re-attaches: `git reset --hard HEAD` + `git clean -fd`. **AC:** two workers
  claiming epics in the same project serialize their canonical refresh (no
  concurrent `reset --hard` on the shared checkout); provisioning is idempotent
  — a second call on an existing workspace re-attaches rather than re-clones;
  `branch_name` persisted on the epic; the token never appears in `.git/config`,
  any log line, or a stored error; `setup_cmd` failure ⇒ epic
  `Blocked(setup_failed)` with the captured output in an `agent_run` row;
  workspace deleted after a successful PR, retained on `Blocked`/`Cancelled`.

- [x] **T-512 — `TaskAgent` seam + evidence.** *deps: T-500, T-502*
  `trait TaskAgent { fn run(&self, req: TaskRunRequest) -> Result<(RunHandle, Receiver<RunEvent>)> }`
  with `Stage`; `ClaudeTaskAgent` maps stage → prompt + `RunMode` + tool flags;
  `ScriptedTaskAgent` fake. Every stage opens an `agent_run` row (`running`) and
  closes it (`ok|error|timeout|cancelled`) with the capped log, `session_id`,
  and `exit_code`. `GET /tasks/{id}/runs` + `GET /runs/{id}`. **AC:** the handle
  is **returned, not dropped** (M1's `Ok((_handle, rx)) => rx` is not repeated);
  `Stage::Implement`/`Fix` use `RunMode::Edit`,
  `Review`/`VerifyComplete`/`Summarize` use `Ask` and are additionally denied
  edit tools; a 300 KB transcript stores as ≤256 KB with head, tail, and an
  elision marker; a stage that panics or errors still closes its row; runs list
  in `created_at` order with stage/attempt/status/verdict.

- [x] **T-513 — Implement stage in the walk (stub deleted).** *deps: T-511, T-512*
  Walk the DAG in dependency order; per task: record `base_sha`, `Todo →
  InProgress`, render spec + context, run `Stage::Implement`, `git add -A`,
  commit (deterministic subject), `Done`. Delete `run_stub_worker` and
  `DEARBORN_STUB_WORKER_DELAY_MS`. **AC:** a scripted agent that writes a file
  produces exactly one commit per task on the epic branch with the §2.8 subject;
  tasks run in dependency order and never two at once; a task producing no diff
  is committed as nothing and left `Done` (the real handling lands in T-532);
  cancelling mid-walk stops cleanly; the epic reaches `Completed` only via T-514.

- [x] **T-514 — `GitHost`, push, and the PR.** *deps: T-513*
  Add `reqwest` (rustls-tls, json). `trait GitHost { push, open_pr, check_auth }`;
  `GithubHost` (owner/repo parsed from `repo_url`, PAT from the project) and
  `FakeHost`. Finalize: push the branch, `POST /repos/{owner}/{repo}/pulls`
  against the default branch, persist `pr_url`/`pr_number`, epic → `Completed`,
  delete the workspace, publish `epic_updated` + `board_updated`. **AC:** push
  against a bare-repo origin fixture lands every commit; `open_pr` sends the
  right title/head/base and the PR URL is persisted and returned by the epic
  API; `Completed` is set **only after** the PR opens — a failed push or PR
  leaves the epic `Blocked(pr_failed)` with the workspace retained; a 4xx from
  GitHub surfaces a readable, token-redacted error; no PAT in any log line.

- [x] **T-515 — Live end-to-end proof.** *deps: T-514*
  `tests/worker_live.rs`, `#[ignore]`d per the T-203 convention: a temp fixture
  repo + bare origin, one epic, one real `claude` run in `RunMode::Edit`.
  **AC:** documented run command in the module header; the real agent modifies
  the working tree in the workspace, Dearborn commits it, pushes to the bare
  origin, and calls `FakeHost::open_pr`; the run is excluded from `just test`.
  **This is the milestone's spike** — it retires the "does headless write-mode
  work" unknown in Phase 1 rather than Phase 6.

---

## 5. Phase 2 — Test gate & fix loop

- [x] **T-520 — Shell command runner.** *deps: T-501*
  `cmd.rs`: run `sh -c <cmd>` in the workspace with combined output capture,
  `DEARBORN_CMD_TIMEOUT_SECS`, and an `agent_run` row. **AC:** exit code +
  combined stdout/stderr captured and capped; a command exceeding the timeout is
  killed (process group) and recorded `status='timeout'`; `test_cmd IS NULL` ⇒
  the caller skips the gate entirely (§5) and records nothing.

- [x] **T-521 — Preflight gate.** *deps: T-511, T-520*
  Run `test_cmd` once after `setup_cmd` on the untouched tree. **AC:** red
  preflight ⇒ epic `Blocked(preflight_red)` with the output stored and **no
  agent ever spawned**; green preflight proceeds; absent `test_cmd` skips
  silently; the epic card shows the reason.

- [x] **T-522 — Test gate + test-driven fix loop.** *deps: T-513, T-520*
  After implement: `test_cmd` → red routes to `Stage::Fix` with the failing
  output as the sole feedback → re-test, up to `MAX_TEST_FIX_ATTEMPTS`. Commit
  only at known-green. **AC:** red-then-green (fixture `test_cmd` flipped by the
  scripted agent) commits once, after green; exhausting attempts ⇒ task
  `Failed(test_gate_exhausted)`, epic `Blocked`, **nothing committed**; each
  attempt writes its own `test_gate` and `fix` rows with increasing `attempt`;
  the fix agent receives the test output and no other stage's context (D19).

---

## 6. Phase 3 — Review, verdict, and convergence

- [x] **T-530 — Review stage + verdict contract.** *deps: T-502, T-512*
  `Stage::Review` sees the cumulative diff from `base_sha` plus the D8 context
  and must emit the `VERDICT:` line. Parse per D9; on a miss, one re-run with a
  terse contract reminder. **AC:** all three verdicts parse from realistic
  preamble-laden output; a contract miss triggers exactly one re-run and then
  `Failed(agent_error)` with both raw outputs retained; the verdict is stored on
  the `agent_run` row and published as `stage_changed`; the reviewer cannot edit
  files (denied edit tools).

- [x] **T-531 — Review → fix → re-test → re-commit loop.** *deps: T-522, T-530*
  `NEEDS_CHANGES` ⇒ `Stage::Fix` on the findings → re-run the test gate → commit
  `fix(...) review round N` → re-review against the **same** `base_sha`. `PASS`
  ⇒ task `Done`. `BLOCKED` ⇒ `Failed(blocked)`. **AC:** a scripted
  NEEDS_CHANGES → PASS sequence produces two commits and closes the task;
  exceeding `MAX_FIX_ROUNDS` ⇒ `Failed(review_not_converged)` + epic `Blocked`
  with every round's findings retained; a fix that breaks the tests fails the
  task rather than committing red; each round re-reviews the cumulative diff.

- [x] **T-532 — Already-complete verification.** *deps: T-513, T-530*
  Implement produced no diff ⇒ `Stage::VerifyComplete` against the spec, verdict
  parsed by the same D9 parser. `PASS` ⇒ close with zero commits;
  `NEEDS_CHANGES` ⇒ route findings to `Fix` and re-enter the normal pipeline;
  `BLOCKED` ⇒ `Failed(blocked)`. **AC:** all three branches covered by scripted
  tests; the PASS path leaves the branch's commit count unchanged and the task
  `Done`; the verification verdict is visible in the task's run history so a
  human can see *why* nothing was built.

---

## 7. Phase 4 — Failure, recovery, cancellation

- [x] **T-540 — Structured failure & Blocked.** *deps: T-522, T-531*
  Centralize: task → `Failed(reason)`, epic → `Blocked(reason)`, **push the epic
  branch** (§7) so the user can clone and triage locally, release the lease,
  retain the workspace, publish `dag_updated` + `epic_updated` +
  `board_updated`, and move the worker on to other work. **AC:** every §2.3
  reason reaches this path; the branch is pushed with the committed work only —
  the failed task's dirty tree is never committed or pushed; the worker
  immediately claims a different epic (a failure is epic-scoped, not fatal); a
  second epic in the same project is unaffected.

- [x] **T-541 — `POST /tasks/{id}/retry`.** *deps: T-540*
  One atomic transition: `Failed → Todo`, clear `failure_reason`, and if the
  parent epic is `Blocked` → `InProgress` + clear `blocked_reason` + clear lease
  + notify. **AC:** `409` unless the task is `Failed`; after retry a worker
  re-claims, re-attaches (`reset --hard` + `clean -fd` drops the failed
  attempt), and re-runs the task; the epic returns to the In Progress lane;
  editing the spec with `PATCH /tasks/{id}` before retrying feeds the new spec
  to the re-run.

- [ ] **T-542 — Cancellation as a kill.** *deps: T-512, T-540*
  Cancel registry on `AppState` (`epic/task id → RunHandle`) populated for each
  agent stage. `InProgress → Cancelled` calls `cancel()`; the worker observes
  `Exited { cancelled: true }`, resets the in-flight task to `Todo`, sets the
  item `Cancelled`, releases the lease, retains the workspace. **AC:** a cancel
  during a stage terminates it in seconds (not at the next stage boundary); the
  `agent_run` row closes `status='cancelled'` with its partial log; the
  stage-boundary DB check still catches a cancel issued between stages; no PR is
  opened; the registry entry is removed on every exit path.

- [ ] **T-543 — Agent stage timeouts.** *deps: T-542*
  `DEARBORN_AGENT_STAGE_TIMEOUT_SECS` around every agent stage, enforced through
  the same cancel path. **AC:** an agent that never exits is cancelled at the
  deadline, its row closes `status='timeout'` with the flushed partial log, and
  the stage counts as that stage's failure (an implement timeout follows the
  ordinary failure route, not a special one); the worker slot is released.

---

## 8. Phase 5 — Standalone tasks

- [ ] **T-550 — `WorkItem` unification.** *deps: T-513, T-540*
  Refactor the worker to claim
  `WorkItem::Epic(id) | WorkItem::Standalone(task_id)` — one loop, one shared
  `prepare → run_task → finalize`. Only the claim query, the next-task selector
  (DAG walk vs. the single task), and branch/PR naming differ. **AC:** every
  Phase 1–4 epic test passes unchanged after the refactor; the standalone claim
  honors the same lease/heartbeat/fencing rules; epic claims are tried first so
  standalone work never starves an epic; no duplicated
  lease/heartbeat/workspace/finalize code.

- [ ] **T-551 — Run a standalone task end-to-end.** *deps: T-514, T-550*
  `POST /tasks/{id}/run` (`Todo → InProgress`, enqueue, notify), workspace at
  `<clone_root>/tasks/<id>`, branch per §2.8, full pipeline, its own PR. **AC:**
  `409` unless the task is `Todo` **and** `epic_id IS NULL`; the whole pipeline
  (preflight → implement → gate → review → PR) runs for one task; failure leaves
  the task `Failed` with its branch pushed and `retry` available — there is no
  epic to Block; `pr_url` is persisted on the task and shown on the board;
  `board_updated` published on every transition.

---

## 9. Phase 6 — PR polish & client

- [ ] **T-560 — PR body: template + agent summary.** *deps: T-512, T-514*
  Deterministic scaffold (epic description, task checklist with commit SHAs,
  review-round counts, verified-already-complete slices, Dearborn footer) plus a
  `Stage::Summarize` "Summary of changes" section over the epic diff. **AC:**
  the template renders correctly with zero agent involvement; a failed, empty,
  or timed-out summary run still opens the PR with the template alone (the PR is
  **never** blocked on the summary); the summary is stored as an `agent_run` row.

- [ ] **T-561 — Client: control surface.** *deps: T-541, T-551*
  Retry button on `Failed` cards, Run button on standalone `Todo` cards, Cancel
  on in-flight epics, failure/blocked reasons on cards, PR link on `Completed`
  epics and finished standalone tasks. **AC:** each control calls its endpoint
  and reflects the resulting WS frame without a manual refresh; a `409` surfaces
  a readable message rather than a silent no-op; the existing `Blocked` (project
  kanban) and `Failed` (task kanban) lanes render the new metadata; tests follow
  the existing `client/test` pattern.

- [ ] **T-562 — Client: task detail pipeline view.** *deps: T-512*
  Stage timeline for a task (implement → test ×N → commit → review round N →
  verdict), each row expandable to its `agent_run` log, hydrated from
  `GET /tasks/{id}/runs`. **AC:** stages render in order with attempt numbers,
  status, duration, and verdict; logs are readable including the elision marker;
  a task with no runs renders an empty state, not an error.

- [ ] **T-563 — Client: live tail.** *deps: T-562*
  Subscribe `task:<id>` on detail open, unsubscribe on close; append streamed
  `RunEvent` text to the running stage; `stage_changed` advances the timeline.
  **AC:** opening a task mid-run hydrates from REST and then follows live with
  no gap or duplication (the ~2 s partial-log flush is the hydration boundary);
  closing the view unsubscribes; the project board does **not** receive the
  token firehose.

- [ ] **T-564 — Documentation.** *deps: T-561, T-562, T-563*
  **AC:** `CONVENTIONS.md` documents §2.5/§2.6; README documents §2.7, the
  executor's operational model (leases, workspaces, recovery), and the `git` +
  `claude` host prerequisites; `ARCHITECTURE.md` §7 amended with the extended
  failure-reason set; this file's boxes checked.

---

## 10. Definition of done

- An epic moved to **In Progress** with a valid DAG is claimed, executed task by
  task, and lands as **one PR** on GitHub — with no human intervention.
- A standalone task moved to **Run** does the same, as its own PR.
- A failing task **Blocks** its epic with a structured reason, pushes the branch,
  preserves per-stage evidence, and leaves other epics running; **Retry**
  resumes it; **Cancel** kills the running agent within seconds.
- Killing the server mid-epic and restarting resumes the epic from its last
  commit without duplicating work.
- `just test` is green and fully hermetic — no network, no `claude`, no GitHub.
  `tests/worker_live.rs` proves the real path on demand.
- The stub worker, `DEARBORN_STUB_WORKER_DELAY_MS`, and every reference to them
  are gone.

---

## 11. Known risks

1. **Headless write-mode behavior is unproven.** M1 only ever ran agents
   read-only. `RunMode::Edit` + `--permission-mode` + tool flags need empirical
   settling — which is exactly why T-515 lands in Phase 1.
2. **Review read-only enforcement is soft.** Denying edit tools still leaves
   `Bash`, through which a determined reviewer could write. ralph has the same
   property; the test gate and the cumulative-diff review are the real backstop.
3. **Canonical-clone contention.** Every epic provision refreshes the shared
   project checkout; the per-project lock (T-511) is load-bearing and is the
   first thing to suspect if workspaces come out inconsistent.
4. **Log volume.** A busy epic writes tens of MB of capped transcripts. Fine for
   v1's single-file libSQL; a retention policy is a v2 concern.

---

## 12. Explicitly out of scope (v2 / later)

Parallel-within-epic worktrees · containerized build envs · per-project pipeline
settings (`max_fix_rounds`, `base_branch`, model/harness overrides) · Gitea and
other hosts · agentmemory integration of any kind · multi-user/RBAC · mobile
push · PR iteration (responding to review comments on an open PR) · auto-merging
base-branch changes · cost/token budgets · a worker/activity operator view.
