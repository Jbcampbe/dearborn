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

`POST /epics/{id}/messages` takes `{ phase, content }` where `phase ∈
product|technical`; it stores one `role='user'` message. Transcript messages
carry a **monotonic `seq` per epic** (1, 2, 3, …); agent/tool messages are
appended by the same store path in T-202.

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
