# Deerborn HTTP/REST conventions

The contract every handler (T-101+) must follow. Established in T-004.

## Transport & auth

- **Base:** all endpoints are served by the Deerborn binary over HTTP. TLS is
  terminated by the operator's reverse proxy (not by Deerborn).
- **Auth:** every route **except `GET /health`** requires
  `Authorization: Bearer <DEERBORN_TOKEN>`. A missing/invalid token yields `401`
  with the standard error envelope (see below). `/health` is public.
- **Content type:** requests and responses are `application/json` (UTF-8).

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
Deerborn owns. Breakdown shares the planning in-flight slot, so a planning run
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

## Identifiers & timestamps

- **IDs** are opaque strings (ULID/UUID) generated server-side.
- **Timestamps** are integers: unix **milliseconds** (matches the `*_at` and
  `lease_expires_at` columns in the §2.2 schema).

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
| `Unauthorized`     | `401`       | `unauthorized` | Missing or invalid bearer token.                |
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
accepts the bearer token from **either**:

- the query string — `GET /ws?token=<DEERBORN_TOKEN>` (browsers), **or**
- an `Authorization: Bearer <DEERBORN_TOKEN>` header (native clients / tests).

The token is validated **before** the upgrade. An absent/invalid token is
rejected with a `401` and the standard error envelope — the socket is never
opened. Because of the query-param path, `/ws` is registered **outside** the
header-only bearer middleware (which would reject every browser handshake);
it does its own token check in the handler.

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
  /epics/{id}/dag`, so nodes carry computed `ready`/`blocked_by`), and
  `epic_updated` (payload = the updated epic) on the `Planning → Ready`
  breakdown transition.

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
| `epic_updated` | An epic's record changed (planning `update_epic`, or the breakdown `Planning → Ready` transition). `payload` = the updated epic. |
| `dag_updated`  | A task or dependency changed under the epic (T-301). `payload` = `{ nodes: [DagNode], edges: [{ blocker_id, blocked_id }] }` (same shape as `GET /epics/{id}/dag`; nodes carry `ready` + `blocked_by`). |
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
**back** to Deerborn over MCP to maintain the epic record and read the project's
code. Deerborn hosts the MCP server **in-process** (a stdio subprocess couldn't
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
`status`/lane/`branch_name`/leases. The MCP config URL Deerborn generates:

```json
{ "mcpServers": { "deerborn": {
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
`--allowedTools mcp__deerborn__update_epic,mcp__deerborn__read_codebase_context`;
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
mcp__deerborn__create_task,mcp__deerborn__link_dependency`; the run's `cwd` is
the read-only clone (the breakdown agent may inspect the code to ground its
slices). The agent never changes the epic's status — Deerborn owns the
`Planning → Ready` transition when the run completes. Cycle rejection uses a
forward DFS over the existing edges (adding `blocker → blocked` is rejected iff
`blocked` can already reach `blocker`); T-302 reuses the same guard for the REST
DAG API.

## PAT encryption at rest

Per-project GitHub PATs (T-102) are encrypted with **AES-256-GCM** before insert
into `project.pat_encrypted` and never leave the server in plaintext:

- **Key:** `SHA-256(DEERBORN_MASTER_KEY)` gives the 256-bit AES key. Any
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
`RUST_LOG` (default `info,deerborn_server=debug`). `5xx` errors are logged at
`error` level with their real cause; secrets are never logged.
