# Dearborn HTTP/REST conventions

The contract every handler (T-101+) must follow. Established in T-004.

## Transport & auth

- **Base:** all endpoints are served by the Dearborn binary over HTTP. TLS is
  terminated by the operator's reverse proxy (not by Dearborn).
- **Auth:** Dearborn has named users with username + password login. There is
  no shared static credential. A successful `POST /auth/login` (or first-launch
  `POST /auth/setup`) returns a **session envelope**: a short-lived signed
  **access token** (`DEARBORN_ACCESS_TTL_SECS`, default 86400 s = 24 h) plus a
  long-lived opaque **refresh token** (`DEARBORN_REFRESH_TTL_SECS`, default
  15552000 s = 180 days).
  - The **access token** is self-contained (`v1.<payload>.<HMAC-SHA256>`,
    HMAC key derived from `DEARBORN_MASTER_KEY`), so authenticating a request
    costs no database read. Every route **except `GET /health`, `/auth/status`,
    `/auth/setup`, `/auth/login`, `/auth/refresh`, and `/ws`/`/mcp/:cap`
    (which authenticate themselves)** requires it:
    `Authorization: Bearer <access token>` on HTTP, `?token=` on the `/ws`
    handshake. A missing/expired/garbage token yields `401`; if the instance
    has no users yet, the same routes yield `401 setup_required` instead.
  - The **refresh token** is stateful — its SHA-256 hash is a `session` row.
    `POST /auth/refresh` re-reads the user's `active` flag and role, so user
    deactivation, demotion, and password resets take effect at refresh time at
    the latest (bounded by the access-token lifetime). Revocation of a live
    access token mid-flight is therefore *eventual* by design.
  - Admin-only routes additionally re-read the caller's user row per request,
    so a stale token can never keep exercising admin privilege.
- **Content type:** requests and responses are `application/json` (UTF-8).

### Authentication routes

Public (outside the auth layer — these are how a caller *gets* a credential):

| Action | Method + path | Body | Success status |
| ------ | ------------- | ---- | -------------- |
| boot probe: is setup needed? | `GET /auth/status` | — | `200` (`{"setup_required": bool}`) |
| claim a fresh instance (creates the first admin) | `POST /auth/setup` | `{username, display_name, password}` | `201` (session envelope); `409` once any user exists |
| log in | `POST /auth/login` | `{username, password}` | `200` (session envelope); `401 invalid_credentials` on unknown username / wrong password / deactivated account — always byte-identical |
| exchange the refresh token for a new access token | `POST /auth/refresh` | `{refresh_token}` | `200` (`{access_token, expires_at, user}`); `401` on unknown/expired/revoked/inactive |

Authenticated (behind the access-token layer):

| Action | Method + path | Body | Success status |
| ------ | ------------- | ---- | -------------- |
| who am I (fresh user row for the UI) | `GET /auth/me` | — | `200` (user) |
| end this device's session | `POST /auth/logout` | — | `204` (revokes the presented token's session id) |
| change own password | `POST /auth/password` | `{current_password, new_password}` | `204`; `400` on a wrong current password or policy violation; revokes all other sessions |

The session envelope is `{ "access_token", "expires_at", "refresh_token",
"refresh_expires_at", "user": {id, username, display_name, role, active} }`.
`password_hash` is never serialized.

### User management (admin)

Admin-only; every route re-reads the caller's row and requires an **active
admin**, so a regular user gets `403 forbidden` (never `404`) even while their
stale token is still live:

| Action | Method + path | Body | Success status |
| ------ | ------------- | ---- | -------------- |
| list users | `GET /users` | — | `200` (`items`) |
| create a user | `POST /users` | `{username, display_name, password, role}` | `201` (user); `409` on duplicate username |
| edit a user (display name / role / active) | `PATCH /users/{id}` | `{display_name?, role?, active?}` | `200` (user) |
| reset another user's password | `POST /users/{id}/password` | `{password}` | `204`; revokes all that user's sessions |

Lockout guards (all store-level, so no handler can forget them): deactivating
or demoting the **last active admin** fails `409` with a specific message, and
an admin cannot deactivate their own account (`409`). Passwords are minimum
12 characters (counted in characters, not bytes), no composition rules;
deactivated users are rows kept forever with `active = 0`, never deleted.

## Route naming

- Nouns, plural, kebab-free lower-case: `/projects`, `/epics`, `/tasks`.
- Nested collections under their parent: `/epics/{epic_id}/tasks`.
- Standard CRUD verbs map to HTTP methods:
  | Action        | Method + path                | Success status |
  | ------------- | ---------------------------- | -------------- |
  | list          | `GET /projects`              | `200`          |
  | create        | `POST /projects`             | `201`          |
  | get           | `GET /projects/{id}`         | `200`          |
  | update        | `PATCH /projects/{id}`       | `200`          |
  | delete        | `DELETE /projects/{id}`      | `204` (no body)|
- **Actions** on a resource are a sub-path verb-noun: `POST /projects/{id}/refresh`
  re-syncs the canonical clone (returns the `200` project, now `clone_status='pending'`).

### Epics & planning transcript (T-201)

Epics are nested under their project; the durable planning transcript is nested
under its epic:

| Action                              | Method + path                     | Success status |
| ----------------------------------- | --------------------------------- | -------------- |
| create an epic (starts planning)    | `POST /projects/{id}/epics`       | `201` (epic, `status='Planning'`) |
| list a project's epics              | `GET /projects/{id}/epics`        | `200` (`items`) |
| get one epic                        | `GET /epics/{id}`                 | `200`          |
| append a `user` transcript message  | `POST /epics/{id}/messages`       | `201` (the stored message, with its assigned `seq`) |
| load the transcript in `seq` order  | `GET /epics/{id}/transcript`      | `200` (`items`) |
| list the planning sessions          | `GET /epics/{id}/sessions`        | `200` (`items`) |
| advance product → technical planning | `POST /epics/{id}/advance-phase` | `201` (`items` = the epic's sessions) |

`POST /epics/{id}/messages` takes `{ phase, content }` where `phase ∈
product|technical`; it stores one `role='user'` message. Transcript messages
carry a **monotonic `seq` per epic** (1, 2, 3, …); agent/tool messages are
appended by the same store path in T-202.

#### Two-phase planning lifecycle (T-205)

Planning runs in two phases — **product** then **technical** — on **one
continuous transcript** (`seq` stays globally monotonic across both; only `phase`
differs per message). The `product` planning session is created with the epic
(§2.2). The user advances via `POST /epics/{id}/advance-phase`, which marks the
`product` session `complete` and creates the `technical` session (`active`); it
returns the epic's sessions and is `409 conflict` if the epic has already
advanced. A message is accepted only for a phase whose planning session exists,
so a `technical` message **before** advancing is rejected `409 conflict`.

`GET /epics/{id}/sessions` returns `{ items: [{ epic_id, phase, status,
created_at, updated_at }] }` so the client knows the active phase and whether it
may advance. The `planning_session.harness_session_id` (the internal harness
resume handle) is **never** exposed by the API. Native resume is keyed per
`(epic, phase)`: the technical run resumes the technical session, never the
product one, and the technical planner is seeded with the epic's
`product_context` (continuity) plus the read-only clone + `read_codebase_context`
so it has code-inspection context. Its `update_epic` writes `technical_context`.

#### Breakdown (T-301)

`POST /epics/{id}/breakdown` runs a **one-shot, non-interactive** breakdown
agent on an *approved* epic and moves it **Planning → Ready**. It is `409
conflict` unless the epic is in `Planning` **and** has advanced to technical
planning (a `technical` session exists), or if a run is already in flight for
the epic; `404` if the epic does not exist. The run's normalized `RunEvent`s
stream live over WS on `epic:<id>` (same mapping as planning); it does **not**
write to `transcript_message` — its durable output is the `task` rows +
`task_dependency` edges the agent creates via its MCP tools, plus one
`agent_run` row (`stage='breakdown'`) and the `epic.status='Ready'` transition
Dearborn owns. Breakdown shares the planning in-flight slot, so a planning run
and a breakdown run never overlap on one epic.

#### Task DAG & readiness API (T-302)

The task DAG under an epic is read with readiness and edited by hand in the
Ready lane (T-303). Readiness is **computed**, not stored: a task is `ready`
iff `status='Todo'` and every blocker (a task with an edge into it) is `Done`.

| Action | Method + path | Success status |
| ------ | ------------- | -------------- |
| read the DAG (nodes + readiness + edges) | `GET /epics/{id}/dag` | `200` (`{ epic_id, nodes: [DagNode], edges: [{blocker_id, blocked_id}] }`) |
| get one task | `GET /tasks/{id}` | `200` |
| create a task under the epic | `POST /epics/{id}/tasks` | `201` (task; body `{ title, description?, acceptance?, blocks?: [ids] }`) |
| create a standalone task under a project | `POST /projects/{id}/tasks` | `201` (task with `epic_id: null`; body `{ title, description?, acceptance? }` — no `blocks`: standalone tasks carry no dependencies) |
| partially update a task | `PATCH /tasks/{id}` | `200` (double-option for `description`/`acceptance`: absent=untouched, `null`=clear, value=set; `status` validated) |
| delete a task (and its edges) | `DELETE /tasks/{id}` | `204` |
| link a dependency | `POST /epics/{id}/dependencies` | `201` (`{ blocker_id, blocked_id }`); `409` on a cycle, `400` on self/cross-epic |
| unlink a dependency | `DELETE /epics/{id}/dependencies?blocker_id=X&blocked_id=Y` | `204` (idempotent) |

A `DagNode` is the `Task` object (flattened) plus `ready: bool` and `blocked_by:
[string]` (blocker ids not yet `Done`; non-empty only when `Todo` and not
ready). Every mutating endpoint publishes a `dag_updated` frame on `epic:<id>`
so a subscribed editor re-renders. Cycle rejection uses a forward DFS over the
existing edges (adding `blocker → blocked` is rejected iff `blocked` already
reaches `blocker`) — the same guard the breakdown `link_dependency` MCP tool
uses (T-301).

#### Planning map API (wayfinder epic: map nodes, frontier, prose)

The planning map is the epic's graph of decision **nodes** (`map_node`; kinds
`grilling | research | prototype | task`; states `open | in_progress |
resolved | out_of_scope`) plus its dependency edges (`map_node_dependency`) and
the four wayfinder prose fields on the epic row (`destination`, `notes`,
`not_yet_specified`, `out_of_scope`). Readiness is **computed**, never stored
(mirrors the task DAG): `frontier` = open (or `in_progress`) with every blocker
settled (`resolved` **or** `out_of_scope` — ruling work out of scope unblocks
its dependents); `blocked_by` = the unsettled blocker ids otherwise. Fog is
never a node state — it is the epic-level `not_yet_specified` prose.

| Action | Method + path | Success status |
| ------ | ------------- | -------------- |
| read the full map | `GET /epics/{id}/map` | `200` (`{ epic_id, destination, notes, not_yet_specified, out_of_scope, nodes: [MapNodeView], edges: [{blocker_id, blocked_id}] }`) |
| create a map node | `POST /epics/{id}/map-nodes` | `201` (node; body `{ kind, title, question?, task_mode?, blocked_by?: [ids], blocks?: [ids], position_x?, position_y? }` — `blocked_by` = the graduation shape (each listed node blocks the new node); `blocks` mirrors the task DAG's `--blocks` (the new node blocks each listed id); `task_mode` (`afk\|hitl`) is required for `kind="task"` and rejected for every other kind — fixed at creation) |
| get one node | `GET /epics/{id}/map-nodes/{nodeId}` | `200` (MapNodeView) |
| partially update a node | `PATCH /epics/{id}/map-nodes/{nodeId}` | `200` (node; body `{ state?, title?, question?, gist?, out_of_scope_reason?, position_x?, position_y? }`; `state = "resolved"` stamps `resolved_by` with the acting user — `null` for capability-token actors) |
| link a dependency | `POST /epics/{id}/map-node-dependencies` | `201` (`{ blocker_id, blocked_id }`); `409` on a cycle, `400` on self/cross-epic |
| set the four prose fields | `PATCH /epics/{id}/map` | `200` (the updated full map; body `{ destination?, notes?, not_yet_specified?, out_of_scope? }` — absent fields untouched, `destination` must be non-empty, the other three clear to `NULL` on an empty string) |

A `MapNodeView` is the `map_node` row (flattened) plus `frontier: bool` and
`blocked_by: [string]` computed from the graph. Cycle rejection uses the same
forward-DFS guard as the task DAG. Every mutating endpoint publishes a
`map_updated` frame on `epic:<id>` whose payload is the full computed map
(the `GET /epics/{id}/map` shape — prose included — so one frame re-renders a
client's whole map view). These are the routes the `dearborn` CLI's `node
create|link|resolve` and `map` / `map set-destination|set-notes|set-fog|
set-out-of-scope` verbs call; they accept either a session token or a
capability token (the allow-list in `capability::authorize_cap_request`).

#### Project board & epic lanes (T-401)

The project board is the kanban view at the project level — the project's epics
(each in its lane) plus its standalone (parentless, `epic_id IS NULL`) tasks. Standalone tasks are small, self-contained units of
tracked work: they have no dependencies (linking one is a `400`), no DAG, and
no lane-move control — they are created (`POST /projects/{id}/tasks`), edited,
and deleted through the plain task endpoints (`PATCH` / `DELETE /tasks/{id}`).
Mutating a standalone task publishes `board_updated` on `project:<id>` (not
`dag_updated` — there is no epic to publish it on).

| Action | Method + path | Success status |
| ------ | ------------- | -------------- |
| read the board | `GET /projects/{id}/board` | `200` (`{ epics: [Epic], tasks: [Task] }` where `tasks` are standalone) |
| move an epic between lanes | `POST /epics/{id}/lane` | `200` (the updated epic) |

`POST /epics/{id}/lane` takes `{ status }` where `status` is one of
`Planning | Ready | InProgress | Completed | Cancelled | Blocked`. Not every
transition is permitted — the server validates the table below and rejects a
disallowed move as `409 conflict`; an unknown lane value is `400 bad_request`;
`404` if the epic does not exist. On success the updated epic is published as
`epic_updated` on `epic:<id>` and the board as `board_updated` on `project:<id>`.

**Permitted epic lane transitions:**

| From | To |
| ---- | -- |
| `Planning`    | `Cancelled` |
| `Ready`       | `InProgress`, `Cancelled` |
| `InProgress`  | `Cancelled`, `Blocked` |
| `Blocked`     | `Ready`, `Cancelled` |
| `Completed`   | *(terminal)* |
| `Cancelled`   | *(terminal)* |

`Planning → Ready` is owned by breakdown; `InProgress → Completed` is owned by
the executor's finalize step (T-514) — set only after the epic's branch is
pushed **and** its PR has actually opened, never on the DAG going fully `Done`
alone. Both transitions are rejected by `POST /epics/{id}/lane`.

An `Epic` additionally carries `pr_url` (`string | null`) and `pr_number`
(`integer | null`), populated together, exactly once, by that same finalize
step, and `blocked_reason` (`string | null`, one of the MILESTONE_2 §2.3
values — e.g. `workspace_error`, `setup_failed`, `agent_error`, `pr_failed`),
set whenever `status = 'Blocked'` and cleared on every other transition. A
failed push or failed PR sets `blocked_reason = 'pr_failed'` and leaves
`pr_url`/`pr_number` `null` — `Completed` and a populated `pr_url` always
appear together.

#### Structured failure & `Blocked` (T-540)

Every failure path in the executor — preflight/provisioning failures with no
task at fault, and per-task failures (a failed agent stage, an exhausted test
gate, a review that never converged, an agent-reported `BLOCKED`) — funnels
through one centralized router (`worker::fail_item`). It sets the epic
`Blocked` with `blocked_reason` set to the exact MILESTONE_2 §2.3 reason
(`preflight_red | setup_failed | workspace_error | test_gate_exhausted |
review_not_converged | blocked | agent_error | timeout | cancelled |
pr_failed`) plus one executor-level refinement (Rec 5):
`provider_rate_limited`, routed when an **implement**-stage failure's error
text matches the transient-provider predicate (`is_transient_provider_error`)
after the bounded retry loop was exhausted — everything else stays
`agent_error`. When the failure has a specific task at fault, that `Task` is
also set `Failed` with the identical string in its own `failure_reason`
column, so `POST /tasks/{id}/retry` (T-541) can find it. Both writes always
carry the same reason string.

Both writes also carry `failure_detail` (Rec 5, migration `0008_failure_detail`):
the failure's human-readable message, redacted against the project PAT and
URL userinfo, capped to head + tail around an elision marker at 2000 chars
(the `evidence::cap_log` discipline). It is surfaced on the task/epic DTOs —
so API responses and board frames show *why* next to *what* without DB
spelunking — and cleared by retry alongside `failure_reason`, so a fresh
attempt never inherits stale detail.

On every `Blocked` transition the executor also attempts to **push the
epic's branch** to the project's remote — whatever is already committed, so
a human can `git clone`/`fetch` the branch and triage locally without VPS
access (§7). This push only ever sends committed work: a failing task's
uncommitted, in-progress changes are never staged or pushed, and the
retained workspace still has them on disk for inspection. The push is
best-effort — its outcome (success or failure) is recorded as a `push`
`agent_run` row, but a push failure never changes `blocked_reason` or
prevents the `Blocked` transition. The push is skipped entirely (no `push`
row at all) when the failure predates any provisioned workspace
(`workspace_error`/`setup_failed`) or when the finalize step's own push/PR
sequence already handled it (`pr_failed`).

A failure is epic-scoped, not fatal to the worker: the same worker loop that
just blocked one epic claims its next item (a different epic, or the same
project's next one) immediately, with no extra delay.

`cancelled` is listed above as part of the router's generic vocabulary (it is
a valid `blocked_reason`/`failure_reason` string per MILESTONE_2 §2.3, and
the router can express it), but in practice **no path ever routes a
cancellation through `fail_item`**: `Blocked` and `Cancelled` are distinct
epic statuses, and `fail_item`'s task write is unconditionally `Failed` —
exactly wrong for a cancelled task, which returns to `Todo` instead (see
"Cancellation as a kill (T-542)" below). `timeout` **is** constructed, and
routes through this same router unmodified (T-543, see "Agent stage
timeouts (T-543)" below): a stage that exceeds its deadline is, from
`fail_item`'s point of view, just another agent-stage failure with a more
precise reason string — the task fails and the epic blocks exactly as they
do for `agent_error`.

#### Recovery: retry a failed task (T-541, standalone contract revised by T-551)

| Action | Method + path | Success status |
| ------ | ------------- | --------------- |
| retry a failed task | `POST /tasks/{id}/retry` | `200` (the updated task); `409` unless `Failed` |

D11's one-shot recovery transition. For an **epic-scoped** task:
`Failed → Todo` (clearing `failure_reason`/`failure_detail`), and — **iff**
the task's parent epic is currently `Blocked` — that epic also moves `Blocked → InProgress`,
clearing `blocked_reason` and its lease (`lease_owner`/`lease_expires_at`), so
the worker pool's claim query (§2.4) can pick it up again. `404` if the task
does not exist; `409 conflict` if it exists but is not currently `Failed` (no
body is required).

For a **standalone** task (`epic_id IS NULL`), the transition is
`Failed → InProgress` directly — **not** `Todo`. T-541 originally sent every
retried task to `Todo` and left resuming a standalone one for T-551; taken
literally that's a dead end, since the worker's claim query (§2.4) only ever
selects `status = 'InProgress' AND epic_id IS NULL` — a task sitting in
`Todo` is never picked up by anything, so "retry" would silently not resume
work. The fix follows from what a standalone task actually is: unlike an
epic-scoped task, where the claimable item (the epic) and the unit of work
(the task) are two different rows, a standalone task is both at once, so
restoring its claimability *is* resetting its work — one write, not two. The
lease columns are cleared defensively (normally already `NULL` — a task's
lease is released on every pipeline exit path before it ever reaches
`Failed`). `POST /tasks/{id}/run` is the endpoint that puts a task into this
loop the *first* time (see below); retry is what puts it back in after a
failure.

Editing the task's spec via `PATCH /tasks/{id}` before calling `retry` needs
no special support here: the next `implement`/`fix` stage simply re-renders
whatever `description`/`acceptance` are on the row at claim time, so an
edited spec reaches the re-run for free. This applies identically to both the
epic-scoped and standalone cases.

On success: `dag_updated` + `epic_updated` on `epic:<id>` (only when the task
has an epic) and `board_updated` on `project:<id>` (always) — the same frames
`POST /epics/{id}/lane` publishes for a lane move — followed by
`state.notify.notify_waiters()` so an idle worker loop wakes immediately
instead of waiting out `DEARBORN_POLL_INTERVAL_MS`.

Once a worker re-claims an unblocked epic, provisioning re-attaches its
retained workspace (T-511: `git reset --hard HEAD` + `git clean -fd`), which
is what actually drops the failed attempt's dirty tree before the walk
re-enters at the now-`Todo` task. A retried standalone task re-attaches its
own workspace (`workspace::provision_task_workspace`) the identical way.

#### Run a standalone task end-to-end (T-551, §2.5, §8)

| Action | Method + path | Success status |
| ------ | ------------- | --------------- |
| run a standalone task | `POST /tasks/{id}/run` | `200` (the updated task); `409` unless `Todo` **and** `epic_id IS NULL` |

The standalone-task counterpart to an epic's `Ready → InProgress` lane move:
`Todo → InProgress`, so the worker pool's claim query (`epic` claims tried
first, standalone `task` claims as the fallback, §2.4) picks the task up on
its own leased run. `404` if the task does not exist; `409` unless the task
is currently `Todo` **and** `epic_id IS NULL` — an epic-scoped task always
`409`s here regardless of its own status; it is only ever run as part of its
epic's own `InProgress` transition.

Once claimed, the task runs the **identical** pipeline an epic-owned task's
DAG walk runs for one of its own tasks — preflight (if `test_cmd` is
configured) → implement → test gate/fix loop → commit → review/fix-converge
(or T-532's already-complete verification, if implement produced no diff) →
`Done` — against its own workspace (`<clone_root>/tasks/{id}`, §2.8) on its
own branch (`dearborn/task-<slug(title)>-<last 6 of id>`, §2.8). On success,
finalize pushes the branch and opens the task's own PR, persisting
`pr_url`/`pr_number` directly on the `Task` row (there is no epic to carry
them instead) — the task's terminal status stays `Done` (the `task` table has
no `Completed` value; opening the PR doesn't change what "done" means, only
where to find the PR). A failure routes through the same structured-failure
router every epic-scoped failure uses (`worker::fail_item`), with one
difference: **there is no epic to `Block`** — every §2.3 reason, including
`preflight_red`/`setup_failed`/`workspace_error` (which for an epic have no
task at fault yet), names the task itself, which lands `Failed` with its
branch pushed (the identical D10/§7 triage push) and its workspace retained.
`POST /tasks/{id}/retry` (above) is how it resumes.

`board_updated` on `project:<id>` publishes on every standalone-task
transition this endpoint and the pipeline body cause: `Todo → InProgress`
(this endpoint), `→ Failed` (the failure router), `→ Done` (the pipeline's
own close-out) and once more when `pr_url`/`pr_number` land (finalize) —
`Task` already serializes `pr_url`/`failure_reason`/`branch_name` (T-500), so
the project board (`GET /projects/{id}/board`) shows a standalone task's PR
link and failure reason with no board-side change required.

There is **no cancellation surface for a standalone task** — see
"Cancellation as a kill (T-542)" below, unchanged by this task.

#### Cancellation as a kill (T-542)

`POST /epics/{id}/lane` with `{ status: "Cancelled" }` against an `InProgress`
epic does more than the plain status write every other lane move does
(§ "Project board & epic lanes" above): it also **kills** whatever agent
stage is currently running for that epic (D12: "Cancel is a kill", not just a
status flip a slow worker eventually notices). The server holds a live
`RunHandle` for the in-flight stage, keyed by epic id, in an in-process
registry (`AppState.cancel_registry`) populated for the duration of exactly
one agent stage (`implement`/`fix`/`review`/`verify_complete`/`summarize` —
never a non-agent stage like `setup`/`preflight`/`test_gate`/`commit`/`push`,
which have no process handle to kill). The lane endpoint looks the epic up in
that registry *after* its own `status = 'Cancelled'` write has committed, and
calls the harness's cancel on whatever it finds — best-effort and
fire-and-forget (it signals the process and returns immediately; the HTTP
response never waits on the process actually exiting). A cancel for an epic
with nothing currently in the registry (nothing in flight — e.g. between
tasks, or while a non-agent stage is running) is a silent no-op at this
layer: the worker's own stage-boundary check (unchanged, pre-existing since
T-513) is the backstop that stops the walk the next time it looks.

Once the worker observes the killed stage's terminal event
(`agent_run.status = 'cancelled'`, its partial log already flushed per D14),
it resets the in-flight **task** back to `Todo` — not `Failed` — because a
cancellation is not a failure; the work is resumable, a human just asked to
stop. The epic needs no further write at that point (already `Cancelled`,
set synchronously by this same endpoint before the kill was even issued).
No PR is ever opened on a cancelled epic; the workspace is retained on disk
exactly as it is on every other stop path (`Blocked`, a lost lease). There is
still no equivalent for a standalone task after T-551 landed — see that
task's own section above ("no cancellation surface for a standalone task")
— or a task-scoped cancel independent of its epic; cancellation today is
epic-scoped, issued only through this one endpoint.

#### Agent stage timeouts (T-543)

Every agent stage (`implement`/`fix`/`review`/`verify_complete`/`summarize`)
carries a wall-clock deadline, `DEARBORN_AGENT_STAGE_TIMEOUT_SECS` (default
`9000`, 2.5 hours — no separate per-stage override; D18 explicitly rejects
an epic-level budget instead). Exceeding it kills the stage through the
identical mechanism T-542 built for a human-initiated cancel (D12: one
`RunControl::cancel()`, looked up in the same `AppState.cancel_registry`) —
there is no second kill path to keep in sync with the first. What differs is
what happens *after*: a human cancel resets the task to `Todo` (resumable); a
timeout instead takes the **ordinary failure route** — `fail_item` with
`failure_reason`/`blocked_reason = 'timeout'` — exactly like any other
agent-stage failure (`agent_error`), because a stage that ran too long is a
failure of that attempt, not a request to stop. A non-agent stage
(`setup`/`preflight`/`test_gate`/`commit`/`push`) has its own, separate
timeout (`DEARBORN_CMD_TIMEOUT_SECS`, T-520) and is never subject to this
one.

The killed stage's `agent_run` row closes `status='timeout'` (not
`'cancelled'`, even though the kill itself set `cancelled` too internally —
see `AgentStageOutcome::timed_out`'s own doc in `task_agent.rs`) with
whatever partial log had already flushed (D14). The server waits, bounded by
a short fixed grace period past the deadline, for the killed process to
actually be reaped before giving up and closing the row from the last known
partial state regardless — a stage is never left hanging indefinitely even
if the underlying kill is unexpectedly slow to land. The worker slot itself
is released exactly as it is for any other task-scoped failure: no special
handling, the same lease-release/workspace-retention/no-PR/next-claim
behavior D10 already gives `agent_error`.

#### Enqueue on In Progress + the executor (T-403, superseded by T-510–T-514)

> The rest of this subsection describes Milestone 1's **stub** worker,
> replaced end to end by the real worker pool and pipeline (MILESTONE_2 Phase
> 0/1: T-510 the lease/claim pool, T-511 workspace provisioning, T-512/T-513
> the real per-task implement walk, T-514 push + PR + `Completed`). The
> WS/HTTP contract shapes named below (`dag_updated`, `epic_updated`,
> `board_updated`, the lane `POST` itself) are unchanged; only what drives
> them changed. The full write-up of the executor's operational model
> (leases, workspaces, recovery) lives in the [README](../README.md#executor-operational-model)
> (T-564); this file stays scoped to the HTTP/WS contract.

Moving an epic **Ready → In Progress** via `POST /epics/{id}/lane` does more
than set `epic.status='InProgress'`: it writes the queue/lease shape from §2.3
— `lease_owner = NULL`, `lease_expires_at = NULL` (explicit even though they
are NULL from creation) — and, since T-510, notifies the executor's worker
pool rather than spawning anything itself (see the callout above).

The stub worker is a **stub**: no real agent, no git, no shell-out — pure DB
writes and WS publishes. It claims **ready** tasks one at a time (a task is
ready when `status='Todo'` and every blocker is `Done`, per §2.3), flips each
`Todo → InProgress → Done`, and when the DAG is fully `Done` sets
`epic.status='Completed'`. Because it serializes (one task at a time), there is
never a sibling `InProgress` task — exactly the invariant Half 2's claim
predicate requires (§2.3).

The stub worker **owns `InProgress → Completed`**: manual lane moves to
`Completed` stay `409 conflict` (only the worker sets it). If the epic is moved
to `Cancelled` or `Blocked` during the walk, the worker's next iteration sees
the epic is no longer `InProgress` and stops cleanly (a no-op).

The worker publishes live over WS so the browser watches the walk:

- `dag_updated` on `epic:<id>` per task transition (`Todo → InProgress` and
  `InProgress → Done`) — so the epic kanban (`/epic/:id/board`) and DAG editor
  re-render cards moving in real time.
- `epic_updated` on `epic:<id>` + `board_updated` on `project:<id>` when the
  epic reaches `Completed` — so the project kanban re-renders the card into the
  Completed lane.

The HTTP response to the lane `POST` is still `200` with the updated epic
(`status='InProgress'`); the worker runs in the background. The real executor
replaces the stub in Half 2.

#### Epic-detail task kanban (T-402)

The client route `/epic/:id/board` renders a task-lane kanban (Todo / In
Progress / Done / Failed / Cancelled) for a single epic. It reuses `GET
/epics/{id}/dag` + the `dag_updated`/`epic_updated` frames on `epic:<id>` —
no new server route.

#### Task stage evidence: `agent_run` history + logs (T-512)

Every pipeline stage — agent-driven (`implement`/`fix`/`review`/
`verify_complete`/`summarize`) and plain (`setup`/`preflight`/`test_gate`/
`commit`/`push`) alike — writes one `agent_run` row per attempt (Milestone 2
§2.1/§2.2): opened `running` when the stage starts, closed `ok|error|
timeout|cancelled` when it ends, with a capped (~256 KB head+tail, D13) log.

| Action | Method + path | Success status |
| ------ | ------------- | -------------- |
| a task's stage history | `GET /tasks/{id}/runs` | `200` (`{ items: [AgentRunSummary] }`, **oldest first** by `created_at`); `404` if the task does not exist |
| one stage's full log | `GET /runs/{id}` | `200` (`AgentRunSummary` fields + `log: string`); `404` if unknown |

An `AgentRunSummary` is `{ id, task_id, epic_id, stage, attempt, status,
verdict, session_id, started_at, ended_at, exit_code, created_at }`.
**The list endpoint deliberately omits `log`** — a busy task can accumulate
several capped-256KB logs, and a stage-timeline view only needs to know what
happened (stage/attempt/status/verdict/timing), not download every stage's
full transcript just to render a list; `GET /runs/{id}` fetches one stage's
full log on demand. `verdict` is only ever non-null for a `review` (T-530) or
`verify_complete` (T-532) stage — the two stages whose prompt ends with a D9
`VERDICT:` line; `session_id` is `null` for every non-agent stage. `task_id`
is `null` for a `push` row opened during an epic's finalize (there is no
single task at fault) and, since T-560, for an epic's own `summarize` row —
the one *agent* stage that can be epic-scoped rather than task-scoped; see
"Task-stage `RunEvent` stream" below for the WS-topic consequence of that.

An agent stage additionally streams its `RunEvent`s live — see the new
`task:<id>` WS topic below — and flushes its accumulated log to the row
roughly every 2 seconds while it runs (D14), so a client that opens a task
mid-run can hydrate the log-so-far from `GET /runs/{id}` and then follow the
rest live over WS with no gap.

## Identifiers & timestamps

- **IDs** are opaque strings (ULID/UUID) generated server-side.
- **Timestamps** are integers: unix **milliseconds** (matches the `*_at`,
  `expires_at`, and `lease_expires_at` columns in the §2.2 schema).

## Success responses

- **Single resource** → the resource object rendered directly as JSON:
  ```json
  { "id": "01J...", "name": "Demo", "repo_url": "https://...", "created_at": 1720000000000 }
  ```
- **Collection** → an object with an `items` array (leaves room for pagination
  metadata later without a breaking change):
  ```json
  { "items": [ { "id": "..." }, { "id": "..." } ] }
  ```
- **No content** (e.g. delete) → `204` with an empty body.
- **Secrets are never returned.** `pat_encrypted` and any decrypted PAT never
  appear in a response or a log line. A per-project PAT may be **supplied** on
  `POST`/`PATCH /projects` as a `pat` field, but is only ever stored encrypted
  (see [PAT encryption](#pat-encryption-at-rest)) and never read back.

## Error responses

All errors — from handlers, extractors, and middleware — render as a single
envelope:

```json
{ "error": { "code": "not_found", "message": "project 01J... not found" } }
```

- `code` is a **stable, machine-readable** slug; clients branch on it.
- `message` is human-readable. For `5xx` it is deliberately generic
  (`"internal server error"`); the real cause is logged server-side only.

### Status ↔ code mapping (`AppError`)

| `AppError` variant | HTTP status | `code`         | When                                            |
| ------------------ | ----------- | -------------- | ----------------------------------------------- |
| `BadRequest`       | `400`       | `bad_request`  | Malformed body / failed validation.             |
| `Unauthorized`     | `401`       | `unauthorized` | Missing, invalid, or expired bearer token.      |
| `InvalidCredentials` | `401`     | `invalid_credentials` | Login failed — identical body for unknown username, wrong password, and deactivated account (no information leaked). |
| `SetupRequired`    | `401`       | `setup_required` | The instance has zero users; claim it via `POST /auth/setup`. Returned in place of plain `401 unauthorized` from protected routes while unclaimed. |
| `Forbidden`        | `403`       | `forbidden`    | Authenticated but lacking the required privilege (admin-only `/users` routes, including a deactivated/demoted former admin with a live token). |
| `NotFound`         | `404`       | `not_found`    | Addressed resource does not exist.              |
| `Conflict`         | `409`       | `conflict`     | Conflicts with current state (dup, DAG cycle…). |
| `Internal`         | `500`       | `internal`     | Unexpected server-side failure (detail hidden). |
| `Db`               | `500`       | `internal`     | Database error (logged in full, hidden).        |

Handlers return `AppResult<T>` (`Result<T, AppError>`) and `?`-propagate;
`AppError` implements `IntoResponse`, so returning `Err(...)` produces the
envelope automatically.

## WebSocket & live subscriptions (`GET /ws`)

REST carries commands/queries; the WebSocket carries **live subscriptions**
(planning `RunEvent` streaming, kanban/status updates). Established in T-005.
Server-side code publishes through the shared `Hub` on `AppState`.

### Handshake auth

A browser cannot set an `Authorization` header on a WebSocket handshake, so `/ws`
accepts an **access token** from **either**:

- the query string — `GET /ws?token=<access token>` (browsers), **or**
- an `Authorization: Bearer <access token>` header (native clients / tests).

The presented token is verified against the instance's signing key — the same
stateless HMAC check the HTTP auth middleware performs; there is no shared
static credential. The token is validated **before** the upgrade. An
absent/invalid token is rejected with a `401` and the standard error envelope —
the socket is never opened. Validation is **handshake-only**: a socket that has
already opened keeps working even after its access token expires; only
reconnects need a fresh one (clients call `POST /auth/refresh` first). Because
of the query-param path, `/ws` is registered **outside** the header-only auth
middleware (which would reject every browser handshake); it does its own token
check in the handler.

### Message envelope

Every frame — both directions — is a JSON object:

```json
{ "topic": "<string>", "type": "<string>", "payload": { } }
```

`topic` is an **opaque string**. Conventions (string-matched; not validated for
existence at the transport layer):

- `epic:<id>` — planning-chat stream + epic-scoped updates (T-202).
- `project:<id>` — project kanban / board updates (T-401), and the
  `clone_status` event (T-103) published when a background clone/refresh reaches
  `ready`/`error` (`payload`: `{ id, clone_status, clone_error, clone_path }`).
- `epic:<id>` also carries `dag_updated` (T-301), published whenever a task or
  dependency is created/changed under the epic (`payload`: `{ nodes: [DagNode],
  edges: [{ blocker_id, blocked_id }] }` — the same shape as `GET
  /epics/{id}/dag`, so nodes carry computed `ready`/`blocked_by`),
  `epic_updated` (payload = the updated epic) on the `Planning → Ready`
  breakdown transition, and `map_updated` (planning map) whenever a map node,
  map dependency edge, or one of the four wayfinder prose fields is mutated
  (`payload` = the full computed map — the same shape as `GET /epics/{id}/map`,
  prose included).
- `task:<id>` — a task-stage agent run's `RunEvent` firehose (T-512), the same
  mapping as the planning stream below (reusing `planning::ws_type`). Kept on
  its own topic, separate from the coarse `epic:<id>` frames, precisely so a
  project kanban subscribed only to `epic:<id>`/`project:<id>` never receives
  the token-by-token stream of every task in the epic — only a client that has
  opened that specific task's detail view subscribes here.
- `task:<id>` and `epic:<id>` both also carry `stage_changed` (T-530, T-532): a
  task-stage transition with a known outcome (today, a `review` or
  `verify_complete` verdict). `payload`: `{ task_id, stage, attempt, status,
  verdict? }` — see below.

### Client → server (control frames)

| Frame | Effect |
| ----- | ------ |
| `{ "type": "subscribe",   "topic": "epic:<id>" }`   | Start receiving events for the topic. Idempotent. |
| `{ "type": "unsubscribe", "topic": "epic:<id>" }`   | Stop receiving events for the topic. |

`payload` may be present on control frames but is ignored. Unknown types and
malformed frames get an `error` frame back (the connection stays open).

### Server → client frames

| `type` | Meaning |
| ------ | ------- |
| `subscribed`   | Ack of a `subscribe`. Sent **after** the subscription is live, so a client may wait for it before triggering a publish (avoids a subscribe/publish race). |
| `unsubscribed` | Ack of an `unsubscribe`. |
| `error`        | Protocol error; `payload.message` explains it. `topic` is `""`. |
| `epic_updated` | An epic's record changed (planning `update_epic`, the breakdown `Planning → Ready` transition, or a lane transition via `POST /epics/{id}/lane`). `payload` = the updated epic. |
| `dag_updated`  | A task or dependency changed under the epic (T-301). `payload` = `{ nodes: [DagNode], edges: [{ blocker_id, blocked_id }] }` (same shape as `GET /epics/{id}/dag`; nodes carry `ready` + `blocked_by`). |
| `map_updated` | The planning map changed (map node create/patch, map dependency link, or one of the four wayfinder prose fields via `PATCH /epics/{id}/map`). `payload` = the full computed map (same shape as `GET /epics/{id}/map` — `{ epic_id, destination, notes, not_yet_specified, out_of_scope, nodes: [MapNodeView], edges }`; nodes carry computed `frontier` + `blocked_by`). Published on `epic:<id>`. |
| `board_updated` | The project board changed (epic lane transition via `POST /epics/{id}/lane`, breakdown's `Planning → Ready`, or a standalone task create/patch/delete). `payload` = `{ epics: [Epic], tasks: [Task] }` (same shape as `GET /projects/{id}/board`; `tasks` are standalone). Published on `project:<id>`. |
| `stage_changed` | A task-stage transition with a known outcome (T-530, T-532: a `review` or `verify_complete` verdict). `payload` = `{ task_id, stage, attempt, status, verdict? }` (`verdict` present only for those two verdict-emitting stages; `status` is the `agent_run.status` vocabulary — `ok`\|`error`\|`timeout`\|`cancelled`). Published on **both** `task:<id>` and, coarse, `epic:<id>` (identical payload) — see below. |
| *(any other)*  | A published event, delivered only to connections subscribed to its `topic`. |

### Planning `RunEvent` stream (T-202)

A user message on an epic (`POST /epics/:id/messages`) triggers a planning agent
run whose normalized `RunEvent`s are relayed live to the epic's topic,
`epic:<id>`. Each event is published as one frame: the `type` is the mapping
below and the `payload` is the **serialized `RunEvent` verbatim** (camelCase,
`kind`-tagged — e.g. `runId`, `sessionId`, `toolCallId`, `delta`).

| `RunEvent` | frame `type` | notes |
| ---------- | ------------ | ----- |
| `Started`        | `started`         | run began |
| `Session`        | `session`         | carries `sessionId` (captured for native resume) |
| `Text`           | `text`            | assistant reply chunk (`delta`); concatenated into the stored `agent` message |
| `Thinking`       | `thinking`        | reasoning chunk (`delta`) |
| `ToolStart`      | `tool_start`      | T-203+ (`input` is always absent for Claude) |
| `ToolEnd`        | `tool_end`        | T-203+ |
| `SuggestedEdits` | `suggested_edits` | |
| `Activity`       | `activity`        | |
| `Usage`          | `usage`           | token accounting |
| `AskQuestion`    | `ask_question`    | |
| `Error`          | `error`           | terminal, followed by `exited` |
| `Exited`         | `exited`          | sent exactly once at run end |

`RunEvent` is `#[non_exhaustive]`; any future kind relays under the generic type
`event` rather than being dropped. The events stream over WS only — the HTTP
`POST` returns the stored **user** message immediately (`201`). The assembled
`agent` reply (and any `tool` events) are written to `transcript_message` when
the run completes; the durable transcript is the source of truth. At most one run
is in flight per epic; a trigger arriving during a run is **ignored** (its user
message is still stored, but no overlapping run starts).

### Task-stage `RunEvent` stream (T-512)

A task-stage agent run (`implement`/`fix`/`review`/`verify_complete`/
`summarize`) relays its `RunEvent`s live the same way, on `task:<id>` instead
of `epic:<id>` — same frame shape, same `type` mapping table above (reusing
`planning::ws_type`). Unlike planning, a task stage is **one-shot** (D19: a
fresh agent context every stage, never resumed) and does not write to
`transcript_message` — its durable record is the `agent_run` row (see the
new `GET /tasks/{id}/runs` / `GET /runs/{id}` endpoints above), which also
receives the accumulated log every ~2 seconds while the stage streams (D14),
so a client that opens a task mid-run can hydrate from REST and then follow
the rest live with no gap.

**Exception (T-560):** an epic's `Stage::Summarize` run — the PR-body
"Summary of changes" section, generated once over the epic's whole
cumulative diff during finalize, not any one task's — has no task to attach
to (`agent_run.task_id` is `NULL`; only `epic_id` is set). It streams on
`epic:<id>` instead, the same topic planning's own epic-chat `RunEvent`
stream already uses, for the identical reason: a run describing the epic as
a whole belongs on the epic's topic, not a task-shaped one invented for a
task that doesn't exist. A **standalone** task's own `Stage::Summarize` run
is unaffected by this exception — it has a real task, so it streams on
`task:<id>` exactly like every other stage. Because this row's `task_id` is
`NULL`, it is also not reachable via `GET /tasks/{id}/runs` (which lists by
`task_id`) — there is no `GET /epics/{id}/runs` today, so an epic-scoped
summarize run is visible only live (the WS stream above, while it runs) or
via `GET /runs/{id}` to a caller that already has its id. Client-surface work
to close that gap is out of scope here (T-561+'s territory).

### `stage_changed` (T-530, T-532)

A task-stage transition whose *outcome* a client cares about — today, a
`review` or `verify_complete` stage's D9 verdict (the two verdict-emitting
stages, sharing one retry/publish implementation,
`worker::run_verdict_stage`) — publishes `stage_changed` on `task:<id>`
**and**, coarse (identical payload), on `epic:<id>`: `{ task_id, stage,
attempt, status, verdict? }`. The two-topic fan-out mirrors why
`dag_updated`/`epic_updated` already live on `epic:<id>`: a project board or
epic detail view watching `epic:<id>` can drive a task card's sub-label
("reviewing", "verifying already-complete", verdict) without subscribing to
that task's own `RunEvent` firehose (which stays `task:<id>`-only, per the
section above), while a task detail view already on `task:<id>` gets the same
summary alongside the token stream it's already receiving. Published once,
after the verdict is parsed and written to the `agent_run` row (`verdict` is
only ever non-null for a `review` or `verify_complete` stage, matching `GET
/tasks/{id}/runs`' own `AgentRunSummary.verdict`) — a contract-miss retry
does **not** publish until a parseable verdict is finally recorded (or the
stage gives up, at which point the failure surfaces through
`dag_updated`/`epic_updated`/`board_updated` instead, the same as any other
task failure).

### Publishing from server code

The `Hub` on `AppState` is the API future tasks (T-202, T-401) call:

```rust
// -> number of connections it was delivered to (0 = no subscribers, a no-op)
state.hub.publish("epic:123", "message", json!({ "text": "hello" }));
```

`publish(topic: &str, event_type: &str, payload: serde_json::Value) -> usize`
serialises the envelope once and fans it out to every current subscriber of the
topic. It never blocks and never fails; a slow client that overflows its buffer
drops the **oldest** frames (bounded per-connection queue).

## Local MCP server (`POST /mcp/:cap`, T-203)

During an interactive planning run the shelled-out Claude Code agent connects
**back** to Dearborn over MCP to maintain the epic record and read the project's
code. Dearborn hosts the MCP server **in-process** (a stdio subprocess couldn't
reach the in-memory `Hub` or the shared libSQL writer), speaking the minimal
**streamable-http** transport: JSON-RPC 2.0 over HTTP at `POST /mcp/:cap`.

- **Why in-process / hand-rolled:** `update_epic` must mutate the shared DB and
  publish a WS event on the live `Hub`; only two tools are exposed, so a
  hand-rolled JSON-RPC endpoint keeps deps lean (no `rmcp`).
- **Transport contract:** a JSON-RPC **request** (has `id`) gets a single
  `application/json` JSON-RPC response (the spec permits this instead of an SSE
  stream); a **notification** (no `id`, e.g. `notifications/initialized`) gets
  `202 Accepted` with no body. Methods handled: `initialize`, `tools/list`,
  `tools/call`, `ping`. A `GET` returns `405`.

### Capability-token auth & scoping

`/mcp/:cap` sits **outside** the browser bearer layer (like `/ws`). The `:cap`
path segment is a **per-run capability token**, minted when a planning run starts
and mapped server-side to a fixed **scope** `{ epic_id, phase, clone_path }`. The
run holds an RAII guard that **revokes the token when the run ends** (a TTL is a
backstop). An unknown/expired token is rejected with `401` before any method runs.

The agent **never supplies the target epic or phase** — they come from the token's
scope. So a token minted for epic A + `product` can only write A's
`product_context` and read A's clone; it cannot address another epic or change
`status`/lane/`branch_name`/leases. The MCP config URL Dearborn generates:

```json
{ "mcpServers": { "dearborn": {
  "type": "http",
  "url": "http://127.0.0.1:<port>/mcp/<cap-token>",
  "headers": { "Authorization": "Bearer <cap-token>" }
} } }
```

### The two phase-scoped tools (§2.4)

| Tool | Effect |
| ---- | ------ |
| `update_epic` | Writes the scope's phase context column (`product`→`product_context`, `technical`→`technical_context`) from the agent's `content` arg, bumps `updated_at`, and publishes an `epic_updated` frame on `epic:<id>` (payload = the updated epic). Target epic+phase are the token's, not the args'. |
| `read_codebase_context` | Read-only listing/reading of the project's canonical clone. A repo-relative `path` (default = repo root); a dir lists, a file reads (capped). **Confinement is enforced in code:** every path is canonicalized and `../`, absolute, and symlink escapes are rejected — this does not rely on `RunMode`. |

Tool-level failures (bad path, missing arg) come back as a JSON-RPC *result* with
`isError: true` (so the model sees them); an unknown tool name is a JSON-RPC
`-32601` error. The tools are exposed to the agent via
`--allowedTools mcp__dearborn__update_epic,mcp__dearborn__read_codebase_context`;
the run's `cwd` is the read-only clone and `--permission-mode bypassPermissions`
is set for headless auto-approval (read-only is guaranteed by the tool allow-list
+ the clone, per the T-200 spike, **not** by the run mode).

### Breakdown phase tools (T-301, §2.4)

A breakdown run mints a capability scoped to `{ epic_id, project_id, phase:
"breakdown", clone_path }`. Its `tools/list` returns **only** the two breakdown
tools (the planning tools are hidden for this scope):

| Tool | Effect |
| ---- | ------ |
| `create_task` | Create ONE task under the **scope's** epic + project (`title` required; optional `description`, `acceptance`, and `blocks`: ids of existing tasks this new task blocks). The epic + project come from the token, never the args — the agent cannot target another epic. Returns the new task's id; publishes a `dag_updated` frame on `epic:<id>`. |
| `link_dependency` | Add a `blocker_id → blocked_id` edge (blocker must finish first). Both endpoints must belong to the scope's epic. A self-edge or cross-epic link is rejected; a cycle is rejected (`isError` with a clear message). Publishes `dag_updated`. |

The tool surface is `--allowedTools
mcp__dearborn__create_task,mcp__dearborn__link_dependency`; the run's `cwd` is
the read-only clone (the breakdown agent may inspect the code to ground its
slices). The agent never changes the epic's status — Dearborn owns the
`Planning → Ready` transition when the run completes. Cycle rejection uses a
forward DFS over the existing edges (adding `blocker → blocked` is rejected iff
`blocked` can already reach `blocker`); T-302 reuses the same guard for the REST
DAG API.

## PAT encryption at rest

Per-project GitHub PATs (T-102) are encrypted with **AES-256-GCM** before insert
into `project.pat_encrypted` and never leave the server in plaintext:

- **Key:** `SHA-256(DEARBORN_MASTER_KEY)` gives the 256-bit AES key. Any
  non-empty master-key material is accepted; derivation is validated at boot
  (empty material fails fast).
- **Nonce/layout:** a fresh random 96-bit nonce per encryption; the stored BLOB
  is `nonce || ciphertext` (nonce prepended; ciphertext carries its GCM tag).
- **Set/clear:** `POST` accepts an optional `pat`; `PATCH` uses the double-option
  shape (`null`/empty clears to `NULL`, a value re-encrypts). An empty/whitespace
  `pat` is treated as "no PAT".
- **Decrypt:** a crate-internal path only (used by cloning, T-103); there is no
  route that returns a PAT.

## Logging

Every request is traced via `tower_http::trace::TraceLayer` on top of the
`tracing` subscriber initialised at boot (`init_tracing`). Verbosity honours
`RUST_LOG` (default `info,dearborn_server=debug`). `5xx` errors are logged at
`error` level with their real cause; secrets are never logged.
