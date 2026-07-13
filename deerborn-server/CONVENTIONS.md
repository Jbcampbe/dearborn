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

## Logging

Every request is traced via `tower_http::trace::TraceLayer` on top of the
`tracing` subscriber initialised at boot (`init_tracing`). Verbosity honours
`RUST_LOG` (default `info,deerborn_server=debug`). `5xx` errors are logged at
`error` level with their real cause; secrets are never logged.
