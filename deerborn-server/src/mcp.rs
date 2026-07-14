//! Deerborn's in-process **local MCP server** for the planning agent (T-203).
//!
//! During an interactive planning run, the shelled-out Claude Code agent connects
//! *back* to Deerborn over MCP to maintain the epic record and inspect the
//! project's code. Per MILESTONE_1 §2.4 / ARCHITECTURE §11 the planning tool
//! surface is exactly two tools:
//!
//! * [`update_epic`](tool_update_epic) — write the epic's planning context for
//!   the run's phase (`product`→`product_context`, `technical`→`technical_context`).
//! * [`read_codebase_context`](tool_read_codebase_context) — **read-only** access
//!   to the project's canonical clone (list dirs / read files), confined to the
//!   clone root.
//!
//! ## Transport: in-process HTTP (streamable-http), not stdio
//!
//! Deerborn is already a live axum server and `update_epic` must mutate the
//! **shared libSQL DB** and publish a WS event on the **in-memory [`Hub`]** — a
//! stdio subprocess could reach neither. So Deerborn hosts the MCP server itself
//! as a route ([`mcp_endpoint`], `POST /mcp/:cap`) speaking minimal JSON-RPC 2.0
//! over HTTP (the MCP "streamable HTTP" transport). Only two tools are exposed,
//! so a hand-rolled JSON-RPC endpoint keeps deps lean — no `rmcp`. Requests get a
//! single `application/json` JSON-RPC response (the spec permits this in lieu of
//! an SSE stream); notifications get `202 Accepted`.
//!
//! ## Auth & scoping: the per-run capability token
//!
//! The `/mcp/:cap` route sits **outside** the browser bearer layer. Auth is a
//! **capability token** minted per planning run ([`CapabilityStore::mint`]) and
//! carried as the `:cap` path segment of the MCP URL. Each token maps server-side
//! to a fixed [`CapabilityScope`] — `{ epic_id, phase, clone_path }`. The agent
//! never supplies the target epic/phase: they come from the token's scope, so a
//! token minted for epic A + `product` can only ever write epic A's
//! `product_context` and read A's clone. It cannot address another epic, change
//! `status`/lane/`branch_name`/leases, or escape the clone directory. The token
//! is short-lived (TTL) and the run holds a [`CapabilityGuard`] that revokes it
//! when the run ends.

use std::collections::HashMap;
use std::path::{Component, Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use axum::{
    extract::{Path as AxPath, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use serde_json::{json, Value};

use crate::epics::update_epic_context;
use crate::tasks;
use crate::{AppError, AppState};

/// The MCP server name the agent addresses tools under (`mcp__deerborn__<tool>`).
pub const MCP_SERVER_NAME: &str = "deerborn";

/// The phase-scoped `--allowedTools` allow-list value for a planning run
/// (MILESTONE_1 §2.4). These are the *only* tools the agent may call.
pub const PLANNING_ALLOWED_TOOLS: &str =
    "mcp__deerborn__update_epic,mcp__deerborn__read_codebase_context";

/// The phase-scoped `--allowedTools` allow-list value for a breakdown run
/// (MILESTONE_1 §2.4). These are the *only* tools the breakdown agent may call.
pub const BREAKDOWN_ALLOWED_TOOLS: &str =
    "mcp__deerborn__create_task,mcp__deerborn__link_dependency";

/// Default lifetime of a minted capability token. A planning run is far shorter;
/// the [`CapabilityGuard`] revokes the token the instant the run ends regardless,
/// so this TTL is only a backstop against a leaked/never-dropped guard.
const CAPABILITY_TTL: Duration = Duration::from_secs(6 * 60 * 60);

/// Cap on bytes returned by a single `read_codebase_context` file read, so a
/// huge file can't blow up the agent's context (or Deerborn's memory).
const MAX_READ_BYTES: usize = 200_000;

// ---- capability tokens ---------------------------------------------------

/// The fixed scope a capability token grants. Set at mint time from the run;
/// never influenced by agent-supplied tool arguments.
#[derive(Clone, Debug)]
pub struct CapabilityScope {
    /// The one epic this token may act on.
    pub epic_id: String,
    /// The one project this token acts under (used by breakdown's `create_task`
    /// to set `task.project_id`; the agent never supplies it).
    pub project_id: String,
    /// The phase whose tool surface + scope this token grants:
    /// `product` | `technical` (planning: `update_epic` + `read_codebase_context`)
    /// or `breakdown` (`create_task` + `link_dependency`).
    pub phase: String,
    /// The project's canonical read-only clone; the confinement root for reads.
    pub clone_path: PathBuf,
    /// Unix-ms expiry; a token resolved at/after this instant is rejected.
    expires_at: i64,
}

/// Per-run capability registry shared on [`AppState`]. Maps opaque tokens to the
/// scope they authorize.
#[derive(Default)]
pub struct CapabilityStore {
    tokens: Mutex<HashMap<String, CapabilityScope>>,
}

impl CapabilityStore {
    /// Create an empty store.
    pub fn new() -> CapabilityStore {
        CapabilityStore::default()
    }

    /// Mint a token scoped to `(epic_id, project_id, phase, clone_path)` with the
    /// default TTL. The returned [`CapabilityGuard`] revokes the token on drop.
    pub fn mint(
        self: &Arc<Self>,
        epic_id: String,
        project_id: String,
        phase: String,
        clone_path: PathBuf,
    ) -> CapabilityGuard {
        self.mint_with_expiry(
            epic_id,
            project_id,
            phase,
            clone_path,
            now_ms() + CAPABILITY_TTL.as_millis() as i64,
        )
    }

    /// Mint with an explicit unix-ms expiry (used by tests to forge an expired
    /// token). Otherwise identical to [`mint`](Self::mint).
    pub(crate) fn mint_with_expiry(
        self: &Arc<Self>,
        epic_id: String,
        project_id: String,
        phase: String,
        clone_path: PathBuf,
        expires_at: i64,
    ) -> CapabilityGuard {
        let token = ulid::Ulid::new().to_string();
        let scope = CapabilityScope {
            epic_id,
            project_id,
            phase,
            clone_path,
            expires_at,
        };
        self.tokens
            .lock()
            .expect("capability mutex poisoned")
            .insert(token.clone(), scope);
        CapabilityGuard {
            token,
            store: Arc::clone(self),
        }
    }

    /// Resolve a token to its scope, or `None` if unknown or expired. Expired
    /// tokens are pruned as a side effect.
    fn resolve(&self, token: &str) -> Option<CapabilityScope> {
        let mut tokens = self.tokens.lock().expect("capability mutex poisoned");
        match tokens.get(token) {
            Some(scope) if scope.expires_at > now_ms() => Some(scope.clone()),
            Some(_) => {
                tokens.remove(token);
                None
            }
            None => None,
        }
    }

    fn revoke(&self, token: &str) {
        self.tokens
            .lock()
            .expect("capability mutex poisoned")
            .remove(token);
    }
}

/// RAII handle to a minted capability token. Dropping it revokes the token, so a
/// planning run's MCP access dies with the run (completion, error, or panic).
pub struct CapabilityGuard {
    token: String,
    store: Arc<CapabilityStore>,
}

impl CapabilityGuard {
    /// The opaque token to embed in the agent's MCP config URL.
    pub fn token(&self) -> &str {
        &self.token
    }
}

impl Drop for CapabilityGuard {
    fn drop(&mut self) {
        self.store.revoke(&self.token);
    }
}

// ---- MCP config the planning run hands to the agent ----------------------

/// Build the `--mcp-config` JSON naming Deerborn's local http MCP server, scoped
/// by `token`. Claude Code accepts this inline or as a file path
/// ([`write_mcp_config`] writes it to a temp file). `base_url` is Deerborn's own
/// loopback origin (e.g. `http://127.0.0.1:8787`).
pub fn mcp_config_json(base_url: &str, token: &str) -> String {
    let url = format!("{}/mcp/{}", base_url.trim_end_matches('/'), token);
    json!({
        "mcpServers": {
            MCP_SERVER_NAME: {
                "type": "http",
                "url": url,
                // The token is the URL path segment (Deerborn's auth); the header
                // is belt-and-suspenders and matches the streamable-http idiom.
                "headers": { "Authorization": format!("Bearer {token}") }
            }
        }
    })
    .to_string()
}

/// Write the MCP config JSON to a temp file and return its path. Written to the
/// system temp dir (never into the read-only clone), keyed by `token`; the caller
/// removes it when the run ends.
pub fn write_mcp_config(base_url: &str, token: &str) -> std::io::Result<PathBuf> {
    let path = std::env::temp_dir().join(format!("deerborn-mcp-{token}.json"));
    std::fs::write(&path, mcp_config_json(base_url, token))?;
    Ok(path)
}

// ---- the JSON-RPC endpoint -----------------------------------------------

/// `POST /mcp/:cap` — Deerborn's local MCP server for one planning run.
///
/// Minimal JSON-RPC 2.0 over HTTP (streamable-http): drives `initialize`,
/// `tools/list`, `tools/call`, and `ping`; `notifications/*` get `202 Accepted`.
/// The `:cap` path segment is the capability token; an unknown/expired token is
/// rejected with `401` before any method runs.
pub async fn mcp_endpoint(
    State(state): State<AppState>,
    AxPath(cap): AxPath<String>,
    Json(message): Json<Value>,
) -> Response {
    let Some(scope) = state.caps.resolve(&cap) else {
        // Unknown/expired capability: never opened, nothing to act on.
        return AppError::Unauthorized.into_response();
    };

    // A JSON-RPC *notification* (no `id`) is acknowledged with 202 and no body.
    let Some(id) = message.get("id").cloned() else {
        return StatusCode::ACCEPTED.into_response();
    };

    let method = message.get("method").and_then(Value::as_str).unwrap_or("");
    let params = message.get("params").cloned().unwrap_or(Value::Null);

    let result = match method {
        "initialize" => Ok(initialize_result(&params)),
        "tools/list" => Ok(tools_list_result(&scope.phase)),
        "tools/call" => tools_call(&state, &scope, &params).await,
        "ping" => Ok(json!({})),
        other => Err(JsonRpcError::method_not_found(other)),
    };

    let body = match result {
        Ok(result) => json!({ "jsonrpc": "2.0", "id": id, "result": result }),
        Err(err) => json!({ "jsonrpc": "2.0", "id": id, "error": err.to_value() }),
    };
    Json(body).into_response()
}

/// The `initialize` result: echo the client's protocol version (or default),
/// advertise the `tools` capability, and identify the server.
fn initialize_result(params: &Value) -> Value {
    let protocol_version = params
        .get("protocolVersion")
        .and_then(Value::as_str)
        .unwrap_or("2025-06-18");
    json!({
        "protocolVersion": protocol_version,
        "capabilities": { "tools": { "listChanged": false } },
        "serverInfo": {
            "name": MCP_SERVER_NAME,
            "title": "Deerborn planning",
            "version": env!("CARGO_PKG_VERSION"),
        }
    })
}

/// The `tools/list` result, scoped to `phase`: the two planning tools for
/// `product`/`technical`, or the two breakdown tools for `breakdown`.
fn tools_list_result(phase: &str) -> Value {
    if phase == "breakdown" {
        return breakdown_tools_list_result();
    }
    json!({
        "tools": [
            {
                "name": "update_epic",
                "description": "Maintain THIS epic's planning context for the current \
                    planning phase. Call it whenever the shared understanding advances; \
                    the `content` you pass REPLACES the stored context (send the full \
                    up-to-date context, in markdown). The target epic and phase are fixed \
                    by the planning session — you cannot address another epic or change \
                    the epic's status, lane, or branch.",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "content": {
                            "type": "string",
                            "description": "The full, current planning context (markdown) \
                                to store for this epic + phase."
                        }
                    },
                    "required": ["content"]
                }
            },
            {
                "name": "read_codebase_context",
                "description": "Read-only access to the project's canonical clone. With no \
                    `path` (or `path` = \".\") it lists the repository root; a directory \
                    path lists that directory; a file path returns the file's contents. \
                    Paths are relative to the repo root; you cannot read outside it.",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "path": {
                            "type": "string",
                            "description": "Repo-relative path to a file or directory. \
                                Defaults to the repository root."
                        }
                    }
                }
            }
        ]
    })
}

/// The `tools/list` result for a **breakdown** run: exactly the two task-DAG
/// tools (MILESTONE_1 §2.4). The target epic + project are fixed by the token
/// scope, never by tool arguments.
fn breakdown_tools_list_result() -> Value {
    json!({
        "tools": [
            {
                "name": "create_task",
                "description": "Create ONE task (a thin, end-to-end vertical slice) under \
                    THIS epic. Provide a `title`, a `description` of the end-to-end behavior \
                    (not layer-by-layer), and `acceptance` criteria. Optionally pass `blocks`: \
                    a list of EXISTING task ids that this new task blocks (i.e. this task must \
                    complete before them). The epic and project are fixed by the breakdown \
                    session — you cannot target another epic. Returns the new task's id so you \
                    can wire dependencies. Create blockers before the tasks they block.",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "title": { "type": "string", "description": "Short task title." },
                        "description": {
                            "type": "string",
                            "description": "The end-to-end behavior this slice delivers."
                        },
                        "acceptance": {
                            "type": "string",
                            "description": "Acceptance criteria: how to verify the slice is done."
                        },
                        "blocks": {
                            "type": "array",
                            "items": { "type": "string" },
                            "description": "Ids of existing tasks this new task blocks (optional)."
                        }
                    },
                    "required": ["title"]
                }
            },
            {
                "name": "link_dependency",
                "description": "Add a dependency edge: `blocker_id` blocks `blocked_id` (the \
                    blocker must finish before the blocked task can start). Both must be tasks \
                    in THIS epic. Rejected if it would create a cycle.",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "blocker_id": { "type": "string", "description": "Task that must finish first." },
                        "blocked_id": { "type": "string", "description": "Task that waits on the blocker." }
                    },
                    "required": ["blocker_id", "blocked_id"]
                }
            }
        ]
    })
}

/// Dispatch a `tools/call` to the named tool. Tool-level failures come back as a
/// successful JSON-RPC response with `isError: true` (so the model sees them),
/// not as a protocol error; an unknown tool name is a protocol error.
async fn tools_call(
    state: &AppState,
    scope: &CapabilityScope,
    params: &Value,
) -> Result<Value, JsonRpcError> {
    let name = params.get("name").and_then(Value::as_str).unwrap_or("");
    let args = params.get("arguments").cloned().unwrap_or(Value::Null);

    let outcome = match name {
        "update_epic" => tool_update_epic(state, scope, &args).await,
        "read_codebase_context" => tool_read_codebase_context(scope, &args).await,
        "create_task" => tool_create_task(state, scope, &args).await,
        "link_dependency" => tool_link_dependency(state, scope, &args).await,
        other => {
            return Err(JsonRpcError::method_not_found(&format!(
                "tools/call {other}"
            )))
        }
    };

    Ok(match outcome {
        Ok(text) => json!({ "content": [{ "type": "text", "text": text }], "isError": false }),
        Err(message) => {
            json!({ "content": [{ "type": "text", "text": message }], "isError": true })
        }
    })
}

// ---- tool: update_epic ---------------------------------------------------

/// Write the scoped epic+phase context column, bump `updated_at`, and publish an
/// `epic_updated` event on `epic:<id>` so the client updates live. The epic and
/// phase come from `scope`, never from `args`, so the agent cannot retarget.
async fn tool_update_epic(
    state: &AppState,
    scope: &CapabilityScope,
    args: &Value,
) -> Result<String, String> {
    let content = extract_content(args)
        .ok_or_else(|| "update_epic requires a non-empty `content` string".to_string())?;

    let epic = update_epic_context(state.db.conn(), &scope.epic_id, &scope.phase, &content)
        .await
        .map_err(|e| format!("failed to update epic: {e}"))?;

    // Live epic record update for subscribers (client Epic view).
    let payload = serde_json::to_value(&epic).unwrap_or(Value::Null);
    state
        .hub
        .publish(&format!("epic:{}", scope.epic_id), "epic_updated", payload);

    Ok(format!(
        "Updated {} context for this epic ({} chars).",
        scope.phase,
        content.len()
    ))
}

/// Pull the context text from a tool-call `arguments` object. Accepts `content`
/// (canonical) or the phase-named / generic aliases an agent might use; always
/// writes to the *scoped* column regardless of which key it arrived under.
fn extract_content(args: &Value) -> Option<String> {
    for key in ["content", "product_context", "technical_context", "context"] {
        if let Some(s) = args.get(key).and_then(Value::as_str) {
            if !s.trim().is_empty() {
                return Some(s.to_string());
            }
        }
    }
    None
}

// ---- tool: create_task (breakdown) ---------------------------------------

/// Create one task under the scoped epic/project, optionally wiring it as a
/// blocker of existing tasks (`blocks`). The epic + project come from `scope`,
/// never from `args`, so the breakdown agent cannot target another epic. On
/// success, publishes a `dag_updated` event so the client's DAG updates live.
async fn tool_create_task(
    state: &AppState,
    scope: &CapabilityScope,
    args: &Value,
) -> Result<String, String> {
    let title = args
        .get("title")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .ok_or_else(|| "create_task requires a non-empty `title` string".to_string())?;
    let description = args.get("description").and_then(Value::as_str);
    let acceptance = args.get("acceptance").and_then(Value::as_str);

    let task = tasks::create_task(
        state.db.conn(),
        &scope.epic_id,
        &scope.project_id,
        title,
        description,
        acceptance,
    )
    .await
    .map_err(|e| format!("failed to create task: {e}"))?;

    // Optional `blocks`: existing task ids this new task blocks. Same-epic /
    // cycle validation happens in `tasks::link_dependency`.
    let mut linked = 0usize;
    if let Some(blocks) = args.get("blocks").and_then(Value::as_array) {
        for entry in blocks {
            if let Some(blocked_id) = entry.as_str() {
                tasks::link_dependency(state.db.conn(), &task.id, blocked_id)
                    .await
                    .map_err(|e| format!("task created ({}), but linking to {blocked_id} failed: {e}", task.id))?;
                linked += 1;
            }
        }
    }

    publish_dag(state, &scope.epic_id).await;

    Ok(format!(
        "Created task {} (\"{}\"){}.",
        task.id,
        task.title,
        if linked > 0 {
            format!(", blocking {linked} task(s)")
        } else {
            String::new()
        }
    ))
}

// ---- tool: link_dependency (breakdown) -----------------------------------

/// Link `blocker_id` → `blocked_id` within the scoped epic. Both tasks must
/// belong to the scope's epic (rejected otherwise); a cycle is rejected. On
/// success, publishes a `dag_updated` event.
async fn tool_link_dependency(
    state: &AppState,
    scope: &CapabilityScope,
    args: &Value,
) -> Result<String, String> {
    let blocker_id = args
        .get("blocker_id")
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
        .ok_or_else(|| "link_dependency requires `blocker_id`".to_string())?;
    let blocked_id = args
        .get("blocked_id")
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
        .ok_or_else(|| "link_dependency requires `blocked_id`".to_string())?;

    // Both endpoints must belong to the scoped epic (the agent cannot reach
    // across epics). `link_dependency` also re-checks same-epic + cycles.
    let conn = state.db.conn();
    for id in [blocker_id, blocked_id] {
        let belongs = tasks::task_belongs_to_epic(conn, id, &scope.epic_id)
            .await
            .map_err(|e| format!("failed to validate task {id}: {e}"))?;
        if !belongs {
            return Err(format!("task {id} is not part of this epic"));
        }
    }

    tasks::link_dependency(conn, blocker_id, blocked_id)
        .await
        .map_err(|e| format!("failed to link dependency: {e}"))?;

    publish_dag(state, &scope.epic_id).await;
    Ok(format!("Linked {blocker_id} → {blocked_id}."))
}

/// Build the epic's task DAG (`{ nodes, edges }`) and publish it on `epic:<id>`
/// under the `dag_updated` type, so a subscribed client re-renders the graph.
/// Best-effort: a read error is logged and the publish is skipped.
pub async fn publish_dag(state: &AppState, epic_id: &str) {
    let conn = state.db.conn();
    let nodes = match tasks::list_tasks_for_epic(conn, epic_id).await {
        Ok(n) => n,
        Err(err) => {
            tracing::warn!(epic = %epic_id, error = %err, "dag publish: failed to load tasks");
            return;
        }
    };
    let edges = match tasks::list_dependencies_for_epic(conn, epic_id).await {
        Ok(e) => e,
        Err(err) => {
            tracing::warn!(epic = %epic_id, error = %err, "dag publish: failed to load edges");
            return;
        }
    };
    let payload = json!({ "nodes": nodes, "edges": edges });
    state
        .hub
        .publish(&format!("epic:{epic_id}"), "dag_updated", payload);
}

// ---- tool: read_codebase_context -----------------------------------------

/// List a directory or read a file from the project's clone, confined to the
/// clone root. Read-only by construction: no write/delete path exists here.
async fn tool_read_codebase_context(
    scope: &CapabilityScope,
    args: &Value,
) -> Result<String, String> {
    let requested = args.get("path").and_then(Value::as_str).unwrap_or(".");
    let path = resolve_within_root(&scope.clone_path, requested)?;

    let meta = tokio::fs::metadata(&path)
        .await
        .map_err(|e| format!("cannot stat `{requested}`: {e}"))?;

    if meta.is_dir() {
        list_dir(&path, requested).await
    } else {
        read_file(&path, requested).await
    }
}

/// Resolve `requested` (a repo-relative path) against `root`, **rejecting any
/// escape**: absolute paths, `..` traversal, and symlinks that resolve outside
/// the root. Both sides are canonicalized (which resolves symlinks and `..`), so
/// the final path is guaranteed to live inside the real clone directory.
fn resolve_within_root(root: &Path, requested: &str) -> Result<PathBuf, String> {
    // Canonicalize the root first: on macOS temp dirs are themselves symlinks
    // (e.g. /var -> /private/var), so we must compare canonical-to-canonical.
    let canonical_root = root
        .canonicalize()
        .map_err(|e| format!("clone path is unavailable: {e}"))?;

    let requested_path = Path::new(requested);

    // Reject absolute inputs outright (defense-in-depth; also caught below).
    if requested_path.is_absolute() {
        return Err(format!("`{requested}` must be a repo-relative path"));
    }
    // Reject any parent-dir component before we ever touch the filesystem.
    if requested_path
        .components()
        .any(|c| matches!(c, Component::ParentDir))
    {
        return Err(format!("`{requested}` escapes the repository root"));
    }

    let candidate = canonical_root.join(requested_path);
    let canonical = candidate
        .canonicalize()
        .map_err(|e| format!("`{requested}` not found in the repository: {e}"))?;

    // Final guard: catch symlinks inside the tree that point outside it.
    if !canonical.starts_with(&canonical_root) {
        return Err(format!("`{requested}` escapes the repository root"));
    }
    Ok(canonical)
}

/// Render a directory listing (entries sorted, dirs suffixed with `/`).
async fn list_dir(path: &Path, requested: &str) -> Result<String, String> {
    let mut entries = tokio::fs::read_dir(path)
        .await
        .map_err(|e| format!("cannot list `{requested}`: {e}"))?;

    let mut names = Vec::new();
    while let Some(entry) = entries
        .next_entry()
        .await
        .map_err(|e| format!("cannot list `{requested}`: {e}"))?
    {
        let name = entry.file_name().to_string_lossy().into_owned();
        let is_dir = entry.file_type().await.map(|t| t.is_dir()).unwrap_or(false);
        names.push(if is_dir { format!("{name}/") } else { name });
    }
    names.sort();

    let header = format!("Directory `{requested}` ({} entries):\n", names.len());
    Ok(header + &names.join("\n"))
}

/// Read a file's contents (UTF-8 lossy), capped at [`MAX_READ_BYTES`].
async fn read_file(path: &Path, requested: &str) -> Result<String, String> {
    let bytes = tokio::fs::read(path)
        .await
        .map_err(|e| format!("cannot read `{requested}`: {e}"))?;
    let truncated = bytes.len() > MAX_READ_BYTES;
    let slice = &bytes[..bytes.len().min(MAX_READ_BYTES)];
    let mut text = String::from_utf8_lossy(slice).into_owned();
    if truncated {
        text.push_str(&format!(
            "\n\n[truncated: showing first {MAX_READ_BYTES} of {} bytes]",
            bytes.len()
        ));
    }
    Ok(text)
}

// ---- minimal JSON-RPC error ----------------------------------------------

/// A JSON-RPC error object (only the codes this endpoint emits).
struct JsonRpcError {
    code: i64,
    message: String,
}

impl JsonRpcError {
    fn method_not_found(method: &str) -> JsonRpcError {
        JsonRpcError {
            code: -32601,
            message: format!("method not found: {method}"),
        }
    }

    fn to_value(&self) -> Value {
        json!({ "code": self.code, "message": self.message })
    }
}

/// Current unix time in milliseconds (matches the `*_at` columns).
fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{app, AppState, Config, Db};
    use axum::body::Body;
    use axum::http::{header::CONTENT_TYPE, Request, StatusCode};
    use serde_json::Value;
    use std::sync::Arc;
    use tower::ServiceExt;

    const TOKEN: &str = "s3cret-token";

    /// Boot state + router with the silent planner (these tests exercise the MCP
    /// endpoint/tools directly and never start a real agent run).
    async fn boot() -> (AppState, axum::Router) {
        let db = Db::connect(":memory:").await.unwrap();
        db.run_migrations().await.unwrap();
        let state = AppState::with_planner(
            Config::for_test(TOKEN),
            db,
            Arc::new(crate::planning::testing::SilentPlanningAgent),
        );
        let app = app(state.clone());
        (state, app)
    }

    /// Insert a project (optionally with a clone_path) and an epic; return ids.
    async fn seed_epic(state: &AppState, clone_path: Option<&str>) -> (String, String) {
        let conn = state.db.conn();
        let now = now_ms();
        let project_id = ulid::Ulid::new().to_string();
        conn.execute(
            "INSERT INTO project (id, name, repo_url, clone_path, clone_status, created_at, updated_at) \
             VALUES (?1, 'P', 'https://example.com/p.git', ?2, 'ready', ?3, ?3)",
            libsql::params![project_id.clone(), clone_path, now],
        )
        .await
        .unwrap();
        let epic_id = ulid::Ulid::new().to_string();
        conn.execute(
            "INSERT INTO epic (id, project_id, title, status, created_at, updated_at) \
             VALUES (?1, ?2, 'E', 'Planning', ?3, ?3)",
            libsql::params![epic_id.clone(), project_id.clone(), now],
        )
        .await
        .unwrap();
        (project_id, epic_id)
    }

    /// POST a JSON-RPC message to `/mcp/:cap` and return (status, body json).
    async fn rpc(app: &axum::Router, cap: &str, message: Value) -> (StatusCode, Value) {
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(format!("/mcp/{cap}"))
                    .header(CONTENT_TYPE, "application/json")
                    .body(Body::from(message.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        let status = response.status();
        let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let value = if bytes.is_empty() {
            Value::Null
        } else {
            serde_json::from_slice(&bytes).unwrap()
        };
        (status, value)
    }

    async fn read_epic_column(state: &AppState, epic_id: &str, column: &str) -> Option<String> {
        let sql = format!("SELECT {column} FROM epic WHERE id = ?1");
        let mut rows = state
            .db
            .conn()
            .query(&sql, libsql::params![epic_id])
            .await
            .unwrap();
        let row = rows.next().await.unwrap().unwrap();
        row.get::<Option<String>>(0).unwrap()
    }

    // ---- capability scoping / auth ----

    #[tokio::test]
    async fn unknown_or_expired_capability_is_rejected() {
        let (state, app) = boot().await;
        let (_p, epic_id) = seed_epic(&state, None).await;

        // Unknown token → 401.
        let (status, _) = rpc(
            &app,
            "not-a-real-token",
            json!({"jsonrpc":"2.0","id":1,"method":"tools/list"}),
        )
        .await;
        assert_eq!(status, StatusCode::UNAUTHORIZED);

        // Expired token → 401 (forge an expiry in the past).
        let guard = state.caps.mint_with_expiry(
            epic_id,
            "proj".into(),
            "product".into(),
            PathBuf::from("/tmp"),
            now_ms() - 1,
        );
        let (status, _) = rpc(
            &app,
            guard.token(),
            json!({"jsonrpc":"2.0","id":1,"method":"tools/list"}),
        )
        .await;
        assert_eq!(status, StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn initialize_then_tools_list_returns_the_two_scoped_tools() {
        let (state, app) = boot().await;
        let (_p, epic_id) = seed_epic(&state, None).await;
        let guard = state
            .caps
            .mint(epic_id, "proj".into(), "product".into(), PathBuf::from("/tmp"));

        // initialize handshake.
        let (status, init) = rpc(
            &app,
            guard.token(),
            json!({"jsonrpc":"2.0","id":1,"method":"initialize",
                   "params":{"protocolVersion":"2025-06-18","capabilities":{}}}),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(init["result"]["protocolVersion"], "2025-06-18");
        assert_eq!(init["result"]["serverInfo"]["name"], "deerborn");

        // notifications/initialized → 202, empty body.
        let (status, body) = rpc(
            &app,
            guard.token(),
            json!({"jsonrpc":"2.0","method":"notifications/initialized"}),
        )
        .await;
        assert_eq!(status, StatusCode::ACCEPTED);
        assert_eq!(body, Value::Null);

        // tools/list → exactly the two planning tools.
        let (_status, listed) = rpc(
            &app,
            guard.token(),
            json!({"jsonrpc":"2.0","id":2,"method":"tools/list"}),
        )
        .await;
        let tools = listed["result"]["tools"].as_array().unwrap();
        let names: Vec<&str> = tools.iter().map(|t| t["name"].as_str().unwrap()).collect();
        assert_eq!(names, vec!["update_epic", "read_codebase_context"]);
    }

    // ---- update_epic ----

    #[tokio::test]
    async fn update_epic_writes_scoped_column_and_publishes_ws_event() {
        let (state, app) = boot().await;
        let (_p, epic_id) = seed_epic(&state, None).await;
        let guard = state
            .caps
            .mint(epic_id.clone(), "proj".into(), "product".into(), PathBuf::from("/tmp"));

        // Subscribe BEFORE the call so we catch the epic_updated frame.
        let mut sub = state.hub.subscribe(&format!("epic:{epic_id}"));

        let (status, body) = rpc(
            &app,
            guard.token(),
            json!({"jsonrpc":"2.0","id":1,"method":"tools/call",
                   "params":{"name":"update_epic","arguments":{"content":"The product is X."}}}),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["result"]["isError"], false);

        // The scoped column changed; the other phase's column stayed NULL.
        assert_eq!(
            read_epic_column(&state, &epic_id, "product_context")
                .await
                .as_deref(),
            Some("The product is X.")
        );
        assert_eq!(
            read_epic_column(&state, &epic_id, "technical_context").await,
            None
        );

        // A live epic_updated event carried the updated record.
        let frame = sub.recv().await.unwrap();
        let value: Value = serde_json::from_str(&frame).unwrap();
        assert_eq!(value["type"], "epic_updated");
        assert_eq!(value["topic"], format!("epic:{epic_id}"));
        assert_eq!(value["payload"]["product_context"], "The product is X.");
    }

    #[tokio::test]
    async fn technical_scope_writes_technical_column() {
        let (state, app) = boot().await;
        let (_p, epic_id) = seed_epic(&state, None).await;
        let guard = state
            .caps
            .mint(epic_id.clone(), "proj".into(), "technical".into(), PathBuf::from("/tmp"));

        rpc(
            &app,
            guard.token(),
            json!({"jsonrpc":"2.0","id":1,"method":"tools/call",
                   "params":{"name":"update_epic","arguments":{"content":"Use libSQL."}}}),
        )
        .await;
        assert_eq!(
            read_epic_column(&state, &epic_id, "technical_context")
                .await
                .as_deref(),
            Some("Use libSQL.")
        );
        assert_eq!(
            read_epic_column(&state, &epic_id, "product_context").await,
            None
        );
    }

    #[tokio::test]
    async fn update_epic_cannot_touch_status_or_another_epic() {
        let (state, app) = boot().await;
        let (_p, epic_a) = seed_epic(&state, None).await;
        // A second epic B in the same project.
        let (project_id, _) = seed_epic(&state, None).await;
        let epic_b = ulid::Ulid::new().to_string();
        state
            .db
            .conn()
            .execute(
                "INSERT INTO epic (id, project_id, title, status, created_at, updated_at) \
                 VALUES (?1, ?2, 'B', 'Planning', ?3, ?3)",
                libsql::params![epic_b.clone(), project_id, now_ms()],
            )
            .await
            .unwrap();

        // A token for epic A. The agent can't name epic B or a status field — the
        // scope fixes the target — but we also pass hostile args to prove they're
        // ignored.
        let guard = state
            .caps
            .mint(epic_a.clone(), "proj".into(), "product".into(), PathBuf::from("/tmp"));
        rpc(
            &app,
            guard.token(),
            json!({"jsonrpc":"2.0","id":1,"method":"tools/call",
            "params":{"name":"update_epic","arguments":{
                "content":"ctx",
                "epic_id": epic_b,           // ignored
                "status": "InProgress"       // ignored
            }}}),
        )
        .await;

        // Epic A got the context; its status is unchanged; epic B is untouched.
        assert_eq!(
            read_epic_column(&state, &epic_a, "product_context")
                .await
                .as_deref(),
            Some("ctx")
        );
        assert_eq!(
            read_epic_column(&state, &epic_a, "status").await.as_deref(),
            Some("Planning")
        );
        assert_eq!(
            read_epic_column(&state, &epic_b, "product_context").await,
            None
        );
        assert_eq!(
            read_epic_column(&state, &epic_b, "status").await.as_deref(),
            Some("Planning")
        );
    }

    #[tokio::test]
    async fn capability_guard_drop_revokes_the_token() {
        let (state, app) = boot().await;
        let (_p, epic_id) = seed_epic(&state, None).await;
        let token = {
            let guard = state
                .caps
                .mint(epic_id, "proj".into(), "product".into(), PathBuf::from("/tmp"));
            guard.token().to_string()
            // guard dropped here → token revoked
        };
        let (status, _) = rpc(
            &app,
            &token,
            json!({"jsonrpc":"2.0","id":1,"method":"tools/list"}),
        )
        .await;
        assert_eq!(status, StatusCode::UNAUTHORIZED);
    }

    // ---- read_codebase_context: read + confinement ----

    /// Build a temp "clone" with a known file; return (dir, canonical dir).
    fn temp_clone() -> PathBuf {
        let dir = std::env::temp_dir().join(format!("deerborn-mcp-clone-{}", ulid::Ulid::new()));
        std::fs::create_dir_all(dir.join("src")).unwrap();
        std::fs::write(dir.join("src/lib.rs"), "pub fn deerborn() -> u32 { 42 }\n").unwrap();
        std::fs::write(dir.join("README.md"), "# Demo\n").unwrap();
        dir
    }

    #[tokio::test]
    async fn read_codebase_context_reads_a_file_and_lists_dirs() {
        let clone = temp_clone();
        let scope = CapabilityScope {
            epic_id: "e".into(),
            project_id: "proj".into(),
            phase: "product".into(),
            clone_path: clone.clone(),
            expires_at: now_ms() + 10_000,
        };

        // Read a file.
        let text = tool_read_codebase_context(&scope, &json!({"path":"src/lib.rs"}))
            .await
            .unwrap();
        assert!(text.contains("deerborn() -> u32 { 42 }"));

        // List the root (default path).
        let listing = tool_read_codebase_context(&scope, &json!({}))
            .await
            .unwrap();
        assert!(listing.contains("README.md"));
        assert!(listing.contains("src/"));

        std::fs::remove_dir_all(&clone).ok();
    }

    #[tokio::test]
    async fn read_codebase_context_rejects_parent_traversal() {
        let clone = temp_clone();
        // A secret sibling OUTSIDE the clone root.
        let secret = clone
            .parent()
            .unwrap()
            .join(format!("deerborn-secret-{}.txt", ulid::Ulid::new()));
        std::fs::write(&secret, "TOP SECRET").unwrap();
        let scope = CapabilityScope {
            epic_id: "e".into(),
            project_id: "proj".into(),
            phase: "product".into(),
            clone_path: clone.clone(),
            expires_at: now_ms() + 10_000,
        };

        let rel = format!("../{}", secret.file_name().unwrap().to_string_lossy());
        let err = tool_read_codebase_context(&scope, &json!({"path": rel}))
            .await
            .unwrap_err();
        assert!(err.contains("escapes"), "got: {err}");

        std::fs::remove_dir_all(&clone).ok();
        std::fs::remove_file(&secret).ok();
    }

    #[tokio::test]
    async fn read_codebase_context_rejects_absolute_outside_root() {
        let clone = temp_clone();
        let scope = CapabilityScope {
            epic_id: "e".into(),
            project_id: "proj".into(),
            phase: "product".into(),
            clone_path: clone.clone(),
            expires_at: now_ms() + 10_000,
        };
        let err = tool_read_codebase_context(&scope, &json!({"path":"/etc/hosts"}))
            .await
            .unwrap_err();
        assert!(
            err.contains("repo-relative") || err.contains("escapes"),
            "got: {err}"
        );
        std::fs::remove_dir_all(&clone).ok();
    }

    #[tokio::test]
    async fn read_codebase_context_rejects_symlink_escape() {
        let clone = temp_clone();
        // A secret OUTSIDE the clone, and a symlink INSIDE the clone pointing at it.
        let secret = clone
            .parent()
            .unwrap()
            .join(format!("deerborn-symlink-secret-{}.txt", ulid::Ulid::new()));
        std::fs::write(&secret, "SECRET VIA SYMLINK").unwrap();
        #[cfg(unix)]
        std::os::unix::fs::symlink(&secret, clone.join("escape")).unwrap();

        let scope = CapabilityScope {
            epic_id: "e".into(),
            project_id: "proj".into(),
            phase: "product".into(),
            clone_path: clone.clone(),
            expires_at: now_ms() + 10_000,
        };

        #[cfg(unix)]
        {
            let err = tool_read_codebase_context(&scope, &json!({"path":"escape"}))
                .await
                .unwrap_err();
            assert!(
                err.contains("escapes"),
                "symlink escape must be denied, got: {err}"
            );
        }

        std::fs::remove_dir_all(&clone).ok();
        std::fs::remove_file(&secret).ok();
    }

    #[tokio::test]
    async fn read_codebase_context_over_the_endpoint() {
        let (state, app) = boot().await;
        let clone = temp_clone();
        let (_p, epic_id) = seed_epic(&state, Some(&clone.to_string_lossy())).await;
        let guard = state.caps.mint(epic_id, "proj".into(), "product".into(), clone.clone());

        let (status, body) = rpc(
            &app,
            guard.token(),
            json!({"jsonrpc":"2.0","id":1,"method":"tools/call",
                   "params":{"name":"read_codebase_context","arguments":{"path":"src/lib.rs"}}}),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["result"]["isError"], false);
        let text = body["result"]["content"][0]["text"].as_str().unwrap();
        assert!(text.contains("deerborn() -> u32 { 42 }"));

        std::fs::remove_dir_all(&clone).ok();
    }

    #[tokio::test]
    async fn unknown_tool_name_is_a_method_error() {
        let (state, app) = boot().await;
        let (_p, epic_id) = seed_epic(&state, None).await;
        let guard = state
            .caps
            .mint(epic_id, "proj".into(), "product".into(), PathBuf::from("/tmp"));
        let (status, body) = rpc(
            &app,
            guard.token(),
            json!({"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"delete_everything","arguments":{}}}),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["error"]["code"], -32601);
    }

    #[test]
    fn mcp_config_json_names_the_http_server_and_token() {
        let cfg = mcp_config_json("http://127.0.0.1:8787", "TOK123");
        let value: Value = serde_json::from_str(&cfg).unwrap();
        assert_eq!(value["mcpServers"]["deerborn"]["type"], "http");
        assert_eq!(
            value["mcpServers"]["deerborn"]["url"],
            "http://127.0.0.1:8787/mcp/TOK123"
        );
        assert_eq!(
            value["mcpServers"]["deerborn"]["headers"]["Authorization"],
            "Bearer TOK123"
        );
    }

    // ---- breakdown tools (T-301) ----

    /// Mint a breakdown-scope capability for `epic_id` (clone_path unused by the
    /// breakdown tools, so `/tmp` is fine).
    fn breakdown_cap(state: &AppState, epic_id: &str, project_id: &str) -> CapabilityGuard {
        state.caps.mint(
            epic_id.to_string(),
            project_id.to_string(),
            "breakdown".to_string(),
            PathBuf::from("/tmp"),
        )
    }

    #[tokio::test]
    async fn breakdown_tools_list_returns_create_task_and_link_dependency() {
        let (state, app) = boot().await;
        let (project_id, epic_id) = seed_epic(&state, None).await;
        let guard = breakdown_cap(&state, &epic_id, &project_id);

        let (status, body) = rpc(
            &app,
            guard.token(),
            json!({"jsonrpc":"2.0","id":1,"method":"tools/list"}),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        let tools = body["result"]["tools"].as_array().unwrap();
        let names: Vec<&str> = tools.iter().map(|t| t["name"].as_str().unwrap()).collect();
        assert_eq!(names, vec!["create_task", "link_dependency"]);
    }

    #[tokio::test]
    async fn create_task_via_endpoint_persists_row_and_publishes_dag_updated() {
        let (state, app) = boot().await;
        let (project_id, epic_id) = seed_epic(&state, None).await;
        let guard = breakdown_cap(&state, &epic_id, &project_id);

        let mut sub = state.hub.subscribe(&format!("epic:{epic_id}"));

        let (status, body) = rpc(
            &app,
            guard.token(),
            json!({"jsonrpc":"2.0","id":1,"method":"tools/call",
                   "params":{"name":"create_task","arguments":{
                       "title":"Slice one",
                       "description":"end-to-end behavior",
                       "acceptance":"it works"
                   }}}),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["result"]["isError"], false);
        let text = body["result"]["content"][0]["text"].as_str().unwrap();
        assert!(text.contains("Created task"));

        // The task row exists with the scoped epic + project and the spec fields.
        let tasks = crate::tasks::list_tasks_for_epic(state.db.conn(), &epic_id)
            .await
            .unwrap();
        assert_eq!(tasks.len(), 1);
        assert_eq!(tasks[0].title, "Slice one");
        assert_eq!(tasks[0].description.as_deref(), Some("end-to-end behavior"));
        assert_eq!(tasks[0].acceptance.as_deref(), Some("it works"));
        assert_eq!(tasks[0].project_id, project_id);
        assert_eq!(tasks[0].epic_id.as_deref(), Some(epic_id.as_str()));

        // A dag_updated frame carried the new DAG.
        let frame = sub.recv().await.unwrap();
        let value: Value = serde_json::from_str(&frame).unwrap();
        assert_eq!(value["type"], "dag_updated");
        assert_eq!(value["topic"], format!("epic:{epic_id}"));
        assert_eq!(value["payload"]["nodes"].as_array().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn create_task_ignores_hostile_epic_and_project_args() {
        let (state, app) = boot().await;
        let (project_id, epic_id) = seed_epic(&state, None).await;
        // A second epic the token must NOT be able to target.
        let (_, other_epic) = seed_epic(&state, None).await;
        let guard = breakdown_cap(&state, &epic_id, &project_id);

        rpc(
            &app,
            guard.token(),
            json!({"jsonrpc":"2.0","id":1,"method":"tools/call",
                   "params":{"name":"create_task","arguments":{
                       "title":"hostile",
                       "epic_id": other_epic,
                       "project_id": "some-other-project"
                   }}}),
        )
        .await;

        // The task landed under the SCOPE's epic, not the argued one.
        let tasks = crate::tasks::list_tasks_for_epic(state.db.conn(), &epic_id)
            .await
            .unwrap();
        assert_eq!(tasks.len(), 1);
        assert_eq!(tasks[0].epic_id.as_deref(), Some(epic_id.as_str()));
        assert_eq!(tasks[0].project_id, project_id);
        // The other epic got nothing.
        assert!(crate::tasks::list_tasks_for_epic(state.db.conn(), &other_epic)
            .await
            .unwrap()
            .is_empty());
    }

    #[tokio::test]
    async fn create_task_with_blocks_links_edges() {
        let (state, app) = boot().await;
        let (project_id, epic_id) = seed_epic(&state, None).await;
        let guard = breakdown_cap(&state, &epic_id, &project_id);

        // Create the blocked task first (so its id is known), then a blocker that
        // blocks it via the `blocks` arg.
        let blocked = crate::tasks::create_task(
            state.db.conn(),
            &epic_id,
            &project_id,
            "B",
            None,
            None,
        )
        .await
        .unwrap();

        let (_status, body) = rpc(
            &app,
            guard.token(),
            json!({"jsonrpc":"2.0","id":1,"method":"tools/call",
                   "params":{"name":"create_task","arguments":{
                       "title":"A",
                       "blocks": [blocked.id]
                   }}}),
        )
        .await;
        assert_eq!(body["result"]["isError"], false);

        let edges = crate::tasks::list_dependencies_for_epic(state.db.conn(), &epic_id)
            .await
            .unwrap();
        assert_eq!(edges.len(), 1);
        // The new task (A) blocks `blocked` (B).
        assert_eq!(edges[0].blocked_id, blocked.id);
    }

    #[tokio::test]
    async fn link_dependency_via_endpoint_links_and_rejects_a_cycle() {
        let (state, app) = boot().await;
        let (project_id, epic_id) = seed_epic(&state, None).await;
        let guard = breakdown_cap(&state, &epic_id, &project_id);
        let conn = state.db.conn();

        let a = crate::tasks::create_task(conn, &epic_id, &project_id, "A", None, None)
            .await
            .unwrap();
        let b = crate::tasks::create_task(conn, &epic_id, &project_id, "B", None, None)
            .await
            .unwrap();
        let c = crate::tasks::create_task(conn, &epic_id, &project_id, "C", None, None)
            .await
            .unwrap();
        // A -> B -> C is valid.
        for (blocker, blocked) in [(a.id.clone(), b.id.clone()), (b.id.clone(), c.id.clone())] {
            let (_status, body) = rpc(
                &app,
                guard.token(),
                json!({"jsonrpc":"2.0","id":1,"method":"tools/call",
                       "params":{"name":"link_dependency",
                                  "arguments":{"blocker_id": blocker, "blocked_id": blocked}}}),
            )
            .await;
            assert_eq!(body["result"]["isError"], false);
        }

        // C -> A closes the cycle: the tool returns isError with a clear message.
        let (_status, body) = rpc(
            &app,
            guard.token(),
            json!({"jsonrpc":"2.0","id":1,"method":"tools/call",
                   "params":{"name":"link_dependency",
                              "arguments":{"blocker_id": c.id, "blocked_id": a.id}}}),
        )
        .await;
        assert_eq!(body["result"]["isError"], true);
        let text = body["result"]["content"][0]["text"].as_str().unwrap();
        assert!(text.contains("cycle"), "got: {text}");

        // The rejected edge was not persisted.
        let edges = crate::tasks::list_dependencies_for_epic(conn, &epic_id)
            .await
            .unwrap();
        assert_eq!(edges.len(), 2);
    }

    #[tokio::test]
    async fn link_dependency_rejects_tasks_outside_the_scoped_epic() {
        let (state, app) = boot().await;
        let (project_id, epic_id) = seed_epic(&state, None).await;
        let (other_project, other_epic) = seed_epic(&state, None).await;
        let guard = breakdown_cap(&state, &epic_id, &project_id);

        // A task under THIS epic, and one under a DIFFERENT epic.
        let a = crate::tasks::create_task(state.db.conn(), &epic_id, &project_id, "A", None, None)
            .await
            .unwrap();
        let x = crate::tasks::create_task(state.db.conn(), &other_epic, &other_project, "X", None, None)
            .await
            .unwrap();

        let (_status, body) = rpc(
            &app,
            guard.token(),
            json!({"jsonrpc":"2.0","id":1,"method":"tools/call",
                   "params":{"name":"link_dependency",
                              "arguments":{"blocker_id": a.id, "blocked_id": x.id}}}),
        )
        .await;
        assert_eq!(body["result"]["isError"], true);
        let text = body["result"]["content"][0]["text"].as_str().unwrap();
        assert!(text.contains("not part of this epic"), "got: {text}");
    }

    #[tokio::test]
    async fn create_task_rejects_missing_title() {
        let (state, app) = boot().await;
        let (project_id, epic_id) = seed_epic(&state, None).await;
        let guard = breakdown_cap(&state, &epic_id, &project_id);

        let (_status, body) = rpc(
            &app,
            guard.token(),
            json!({"jsonrpc":"2.0","id":1,"method":"tools/call",
                   "params":{"name":"create_task","arguments":{"title":"   "}}}),
        )
        .await;
        assert_eq!(body["result"]["isError"], true);
        let text = body["result"]["content"][0]["text"].as_str().unwrap();
        assert!(text.contains("title"));
    }
}
