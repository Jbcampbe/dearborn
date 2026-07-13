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
  appear in a response or a log line.

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
- `project:<id>` — project kanban / board updates (T-401).

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

## Logging

Every request is traced via `tower_http::trace::TraceLayer` on top of the
`tracing` subscriber initialised at boot (`init_tracing`). Verbosity honours
`RUST_LOG` (default `info,deerborn_server=debug`). `5xx` errors are logged at
`error` level with their real cause; secrets are never logged.
