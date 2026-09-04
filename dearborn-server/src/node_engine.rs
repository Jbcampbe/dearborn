//! The interactive per-node planning engine (wayfinder epic §5/§7).
//!
//! Grilling and prototype nodes are **HITL** decisions worked one agent session
//! at a time. This module re-scopes the old epic-level linear planning engine
//! down to the node: each such node owns a **node-scoped session**
//! ([`node_session`], the native resume handle), a **multi-party transcript**
//! ([`node_message`], attributed to the human who posted via `actor_user_id`),
//! and a **WebSocket topic** `node:<id>` its live `RunEvent`s stream to. The
//! one-run-in-flight lock moved with it: from per-epic
//! ([`crate::AppState::inflight`]) to **per-node**
//! ([`crate::AppState::node_inflight`]), so unblocked frontier nodes run
//! concurrently while replies *within* a node stay serialized.
//!
//! ## The loop
//!
//! 1. `POST /epics/:id/map-nodes/:nodeId/session` opens (or resumes) the node's
//!    session and flips the node to the soft `in_progress` "being worked"
//!    signal.
//! 2. `POST /epics/:id/map-nodes/:nodeId/messages` — **any** authenticated user
//!    posts a turn. The message is always stored; if no reply is already in
//!    flight for this node, the per-node run-lock is claimed and a background
//!    agent reply is spawned. A message that lands while a reply is running is
//!    stored but does not start a second turn (the lock serializes replies).
//! 3. The reply drives the [`crate::planning::PlanningAgent`] seam
//!    (production Claude Code; tests inject a scripted fake), relaying every
//!    `RunEvent` live to `node:<id>`, persisting the assembled agent turn as a
//!    `node_message`, and capturing the harness session id back onto
//!    `node_session` so the next turn resumes natively.
//!
//! ## Methodology is a prompt, not a Skill-tool call
//!
//! Each kind carries its method in its **system prompt** ([`GRILLING_PROMPT`],
//! [`PROTOTYPE_PROMPT`], adapted from `matt-pocock-skills` as source material),
//! resolved live per run through the same per-slot settings machinery breakdown
//! uses ([`crate::agent_settings::spawn_config`] over [`AgentSlot::Grilling`] /
//! [`AgentSlot::Prototype`]). This keeps behaviour harness-agnostic and
//! Dearborn-owned/versioned.
//!
//! ## Scope
//!
//! This module is the conversational engine **plus the resolution surface**:
//! each grilling/prototype reply runs with a per-run capability token
//! ([`crate::capability`]) scoped to its epic and minted for the node's own
//! kind (the phase), and the system prompt carries a `dearborn` CLI access
//! block so the session can resolve itself — the rich grilling resolution
//! bundle ([`crate::resolve`]: record the decision, fold in document edits,
//! graduate fog, rule things out of scope, update affected nodes) is the
//! CLI's `node resolve` verb. AFK kinds (research/AFK-task) never reach this
//! engine, so they never gain a token or a map-reshaping surface.

use std::path::PathBuf;

use axum::{
    extract::{Path, State},
    http::StatusCode,
    Json,
};
use harness::RunEvent;
use libsql::{params, Connection};
use serde::Serialize;
use serde_json::{json, Value};

use crate::agent_slot::AgentSlot;
use crate::map::Actor;
use crate::planning::{ws_type, PlanningRunRequest};
use crate::{AppError, AppResult, AppState, NodeRunGuard};

// ---- the Dearborn CLI access block (the session's resolution surface) -------

/// The `dearborn` CLI knobs handed to an interactive node reply for its run.
/// Mirrors [`crate::breakdown::DearbornCli`] — the loopback base URL and the
/// per-run capability token, injected as a system-prompt access block.
struct NodeCli {
    /// Dearborn's loopback origin (e.g. `http://127.0.0.1:8787`) — the CLI's `--url`.
    pub base_url: String,
    /// The per-run capability token — the CLI's `--token`.
    pub token: String,
}

/// The system-prompt access block injected when a node reply is wired to the
/// `dearborn` CLI: how to authenticate (the per-run `--url`/`--token` pair,
/// pre-scoped to this epic) and which verbs exist. The flags travel with
/// every command — each shell invocation is a fresh process, so an `export`
/// would not survive between the agent's tool calls.
fn cli_access_block(cli: &NodeCli) -> String {
    format!(
        "\nDearborn CLI access — call it through your shell tool exactly as shaped below \
         (the `--url`/`--token` pair is already issued and scoped to THIS epic; never \
         modify or omit either):\n\
         dearborn --url {url} --token {token} <verb>\n\
         where <verb> is one of:\n\
         - map — print the planning map (destination, fog, out-of-scope prose, every node \
         with its state and frontier position)\n\
         - document pull [PATH] — write the epic's living HTML document to a scratch file \
         (default `./document.html`) for editing with your native file tools; prints its \
         base `version`\n\
         - node resolve NODE [--gist \"...\"] [--document PATH --base-version N] \
         [--graduate \"kind=grilling; title=...; question=...\"]... \
         [--out-of-scope \"title=...; reason=...\"]... \
         [--update \"id=NODE_ID; state=out_of_scope; out_of_scope_reason=...\"]... \
         [--trim-fog \"...\"] — resolve THIS node's decision in ONE call: record the \
         one-line gist, fold your edited document in as a new version, graduate fog into \
         new frontier nodes (blocked by this node), rule things out of scope (with a \
         reason), and update or invalidate other nodes this decision affected. A stale \
         `--base-version` fails cleanly — re-pull the document, re-edit, and retry. \
         Prototype sessions also ship the artifact here: `--artifact PATH` (the file you \
         built in this scratch workspace; the CLI base64-uploads it — big files through \
         paths, not tool-args) with optional `--artifact-mime MIME` (default `text/html`) \
         and `--artifact-label \"...\"` (its file name works well); it is stored as a \
         `node_asset` linked from the node and rendered for the humans in a sandboxed \
         iframe.\n\
         Each verb prints JSON on success and `dearborn: <error>` on failure. When the \
         decision this node poses is settled, resolve it with ONE `node resolve` call \
         carrying everything the decision decided.\n",
        url = cli.base_url,
        token = cli.token,
    )
}

// ---- per-kind system prompts (adapted from matt-pocock-skills) --------------

/// The grilling engine's methodology (wayfinder epic §5/§6), adapted from
/// `matt-pocock-skills`' `wayfinder` + `domain-modeling` skills as source
/// material. A grilling node is the primary map-builder: a relentless,
/// one-decision-at-a-time interview with a human who speaks for themselves.
pub(crate) const GRILLING_PROMPT: &str = "\
You are Dearborn's grilling agent, working ONE decision node of a planning map \
(the \"Wayfinder\" model: charting a route through fog toward a destination). \
You resolve the node through a live conversation with a human who speaks for \
themselves — you NEVER answer your own questions or stand in for the human's side \
of the exchange. This is planning, not building: the node resolves to a DECISION, \
not a slice of work to execute.

How to grill:
- Interview relentlessly. Ask one sharp question at a time and follow the answer \
where it leads; resolve each branch of the decision before opening the next.
- Sharpen fuzzy language. When the human uses a vague or overloaded term, propose a \
precise canonical term and confirm it. Challenge terms that conflict with earlier \
answers.
- Stress-test with concrete scenarios. Invent edge cases that force the human to be \
precise about boundaries between concepts.
- Cross-reference the code. You have read-only access to the project's checkout as \
your working directory; when the human states how something works, check whether the \
code agrees and surface any contradiction.
- Know when you're done. When the decision this node poses is settled — with nothing \
left to decide before someone could act on it — say so plainly and summarize the \
decision in one line. Do not charge at the destination; the pull to just start doing \
the work is the signal the node is resolved.

Stay on THIS node's question. Surfacing a new, separable decision is fine — name it \
so a human can add it to the map — but don't try to resolve it here.";

/// The prototype engine's methodology (wayfinder epic §5), adapted from
/// `matt-pocock-skills`' `prototype` skill. A prototype node raises the fidelity
/// of a decision by building a cheap, throwaway artifact to react to.
pub(crate) const PROTOTYPE_PROMPT: &str = "\
You are Dearborn's prototype agent, working ONE decision node of a planning map \
(the \"Wayfinder\" model). A prototype is THROWAWAY code that answers a question — the \
question decides the shape. You work WITH a human who speaks for themselves; never \
answer your own questions.

First, pin down which question is being answered, from the node and the human:
- \"Does this logic / state model feel right?\" → build a single, shareable, \
self-contained artifact that pushes the state machine through cases that are hard to \
reason about on paper, and that a non-developer could drive.
- \"What should this look like?\" → generate a few radically different variations to \
react to.

Rules for either branch:
- Your working directory is a throwaway SCRATCH WORKSPACE — not the project's code, \
not a checkout. Build the artifact here (a single self-contained HTML app is the \
default shape); nothing here touches the target repo.
- Throwaway from day one, and clearly marked as such. Skip the polish: no tests, no \
error handling beyond what makes it runnable, no abstractions. The point is to learn \
something fast.
- No persistence by default — state lives in memory unless the question is explicitly \
about a database.
- Surface the state. After every action or variation switch, render the full relevant \
state so the human can see what changed.
- Capture the verdict. When the artifact has answered the question, state the decision \
it settled in one line — and ship the artifact: resolve the node with \
`node resolve NODE --artifact <file>` so it is stored under the node for the humans \
to open.

Stay on THIS node's question. This is planning: produce a decision informed by the \
artifact, not a production feature.";

// ---- kind → slot ------------------------------------------------------------

/// The node kinds this interactive engine drives (wayfinder epic §5): grilling
/// and prototype are HITL, multi-turn, node-scoped. Research/task nodes run on
/// the one-shot AFK engine ([`crate::afk_engine`]) and never open an
/// interactive session.
pub const INTERACTIVE_KINDS: &[&str] = &["grilling", "prototype"];

/// The agent slot a node kind resolves its live settings under, or `None` for a
/// kind this engine does not drive.
pub fn slot_for_kind(kind: &str) -> Option<AgentSlot> {
    match kind {
        "grilling" => Some(AgentSlot::Grilling),
        "prototype" => Some(AgentSlot::Prototype),
        _ => None,
    }
}

/// The compiled default system prompt for an interactive node kind.
fn default_prompt_for_kind(kind: &str) -> Option<&'static str> {
    match kind {
        "grilling" => Some(GRILLING_PROMPT),
        "prototype" => Some(PROTOTYPE_PROMPT),
        _ => None,
    }
}

// ---- DTOs -------------------------------------------------------------------

/// A node's durable resume handle (`node_session`, wayfinder epic §4.3).
#[derive(Debug, Clone, Serialize)]
pub struct NodeSession {
    pub node_id: String,
    /// Native harness resume id; `None` until the node's first agent turn runs.
    pub harness_session_id: Option<String>,
    /// `active` | `complete`.
    pub status: String,
    pub created_at: i64,
    pub updated_at: i64,
}

/// One entry in a node's multi-party transcript (`node_message`, §4.4).
#[derive(Debug, Clone, Serialize)]
pub struct NodeMessage {
    pub id: String,
    pub node_id: String,
    /// `user` | `agent` | `tool` | `system`.
    pub role: String,
    /// Which human posted it (`None` for agent/tool/system turns).
    pub actor_user_id: Option<String>,
    pub content: String,
    /// Monotonic per node.
    pub seq: i64,
    pub created_at: i64,
}

/// The `session` endpoints' response: the resume handle plus the transcript so
/// far, so a client opening a node renders the whole conversation in one call.
#[derive(Debug, Serialize)]
pub struct NodeSessionView {
    #[serde(flatten)]
    pub session: NodeSession,
    pub messages: Vec<NodeMessage>,
}

// ---- store: node_session ----------------------------------------------------

const SESSION_COLUMNS: &str = "node_id, harness_session_id, status, created_at, updated_at";

fn row_to_session(row: &libsql::Row) -> Result<NodeSession, libsql::Error> {
    Ok(NodeSession {
        node_id: row.get(0)?,
        harness_session_id: row.get(1)?,
        status: row.get(2)?,
        created_at: row.get(3)?,
        updated_at: row.get(4)?,
    })
}

/// Fetch a node's session row, or `None` if it has never been opened.
pub async fn fetch_session(conn: &Connection, node_id: &str) -> AppResult<Option<NodeSession>> {
    let sql = format!("SELECT {SESSION_COLUMNS} FROM node_session WHERE node_id = ?1");
    let mut rows = conn.query(&sql, params![node_id]).await?;
    match rows.next().await? {
        Some(row) => Ok(Some(row_to_session(&row)?)),
        None => Ok(None),
    }
}

/// Get the node's session, creating an `active` row on first open (idempotent).
pub async fn ensure_session(conn: &Connection, node_id: &str) -> AppResult<NodeSession> {
    let now = now_ms();
    conn.execute(
        "INSERT INTO node_session (node_id, harness_session_id, status, created_at, updated_at) \
         VALUES (?1, NULL, 'active', ?2, ?2) \
         ON CONFLICT(node_id) DO NOTHING",
        params![node_id, now],
    )
    .await?;
    fetch_session(conn, node_id)
        .await?
        .ok_or_else(|| AppError::Internal(format!("node session {node_id} vanished after insert")))
}

/// Record the harness's native resume id (and touch `updated_at`) after a turn.
pub async fn set_session_resume(
    conn: &Connection,
    node_id: &str,
    harness_session_id: &str,
) -> AppResult<()> {
    conn.execute(
        "UPDATE node_session SET harness_session_id = ?2, updated_at = ?3 WHERE node_id = ?1",
        params![node_id, harness_session_id, now_ms()],
    )
    .await?;
    Ok(())
}

/// Mark a node's session `complete` (the resolution side of the loop: a
/// resolved node's session has nothing left to resume). A node with no
/// session row (never opened, or an AFK kind that may never create one) is a
/// no-op.
pub async fn mark_session_complete(conn: &Connection, node_id: &str) -> AppResult<()> {
    conn.execute(
        "UPDATE node_session SET status = 'complete', updated_at = ?2 WHERE node_id = ?1",
        params![node_id, now_ms()],
    )
    .await?;
    Ok(())
}

// ---- store: node_message ----------------------------------------------------

const MESSAGE_COLUMNS: &str = "id, node_id, role, actor_user_id, content, seq, created_at";

fn row_to_message(row: &libsql::Row) -> Result<NodeMessage, libsql::Error> {
    Ok(NodeMessage {
        id: row.get(0)?,
        node_id: row.get(1)?,
        role: row.get(2)?,
        actor_user_id: row.get(3)?,
        content: row.get(4)?,
        seq: row.get(5)?,
        created_at: row.get(6)?,
    })
}

/// Append a message to a node's transcript. `seq` is computed atomically as
/// `MAX(seq)+1` inside the INSERT so concurrent posters (multi-party, §4.4)
/// never collide — SQLite serializes the writers.
pub async fn append_message(
    conn: &Connection,
    node_id: &str,
    role: &str,
    actor_user_id: Option<&str>,
    content: &str,
) -> AppResult<NodeMessage> {
    let id = ulid::Ulid::new().to_string();
    conn.execute(
        "INSERT INTO node_message (id, node_id, role, actor_user_id, content, seq, created_at) \
         VALUES (?1, ?2, ?3, ?4, ?5, \
                 (SELECT COALESCE(MAX(seq), 0) + 1 FROM node_message WHERE node_id = ?2), ?6)",
        params![id.clone(), node_id, role, actor_user_id, content, now_ms()],
    )
    .await?;
    let sql = format!("SELECT {MESSAGE_COLUMNS} FROM node_message WHERE id = ?1");
    let mut rows = conn.query(&sql, params![id.clone()]).await?;
    match rows.next().await? {
        Some(row) => Ok(row_to_message(&row)?),
        None => Err(AppError::Internal(format!(
            "node message {id} vanished after insert"
        ))),
    }
}

/// A node's transcript, in `seq` order.
pub async fn list_messages(conn: &Connection, node_id: &str) -> AppResult<Vec<NodeMessage>> {
    let sql = format!(
        "SELECT {MESSAGE_COLUMNS} FROM node_message WHERE node_id = ?1 ORDER BY seq ASC, id ASC"
    );
    let mut rows = conn.query(&sql, params![node_id]).await?;
    let mut items = Vec::new();
    while let Some(row) = rows.next().await? {
        items.push(row_to_message(&row)?);
    }
    Ok(items)
}

// ---- REST handlers ----------------------------------------------------------

/// `POST /epics/:id/map-nodes/:nodeId/session` — open or resume a node's
/// interactive session. Ensures the `node_session` row exists and flips the
/// node to the soft `in_progress` "being worked" signal. `200` with the session
/// + transcript. `404` unknown epic/node; `409` for a non-interactive kind
/// (research/task nodes have no interactive engine).
pub async fn open_node_session(
    State(state): State<AppState>,
    Path((epic_id, node_id)): Path<(String, String)>,
) -> AppResult<Json<NodeSessionView>> {
    let conn = state.db.conn();
    let node = require_interactive_node(conn, &epic_id, &node_id).await?;

    let session = ensure_session(conn, &node_id).await?;
    // Soft signal only (never a lock): mark it worked if still just open.
    if node.state == "open" {
        conn.execute(
            "UPDATE map_node SET state = 'in_progress', updated_at = ?2 WHERE id = ?1 AND state = 'open'",
            params![node_id.clone(), now_ms()],
        )
        .await?;
        crate::map::publish_map(&state, &epic_id).await;
    }

    let messages = list_messages(conn, &node_id).await?;
    Ok(Json(NodeSessionView { session, messages }))
}

/// `GET /epics/:id/map-nodes/:nodeId/session` — the node's session + transcript.
/// `404` if the epic/node is unknown or the session has not been opened.
pub async fn get_node_session(
    State(state): State<AppState>,
    Path((epic_id, node_id)): Path<(String, String)>,
) -> AppResult<Json<NodeSessionView>> {
    let conn = state.db.conn();
    require_interactive_node(conn, &epic_id, &node_id).await?;
    let session = fetch_session(conn, &node_id)
        .await?
        .ok_or_else(|| AppError::NotFound(format!("node {node_id} has no open session")))?;
    let messages = list_messages(conn, &node_id).await?;
    Ok(Json(NodeSessionView { session, messages }))
}

/// `GET /epics/:id/map-nodes/:nodeId/messages` — the node's transcript.
pub async fn list_node_messages(
    State(state): State<AppState>,
    Path((epic_id, node_id)): Path<(String, String)>,
) -> AppResult<Json<Value>> {
    let conn = state.db.conn();
    require_interactive_node(conn, &epic_id, &node_id).await?;
    let messages = list_messages(conn, &node_id).await?;
    Ok(Json(json!({ "items": messages })))
}

/// `POST /epics/:id/map-nodes/:nodeId/messages` body: `{ "content": "…" }`.
#[derive(Debug, serde::Deserialize)]
pub struct PostMessageBody {
    content: Option<String>,
}

/// `POST /epics/:id/map-nodes/:nodeId/messages` — post a turn into a node's
/// conversation. **Any** authenticated user may post (attributed via
/// `actor_user_id`); the message is always stored, and the per-node run-lock
/// decides whether it also starts an agent reply:
/// * lock free → claim it and spawn the reply (events stream on `node:<id>`);
/// * lock held (a reply already in flight for this node) → store only, so the
///   reply stays serialized within the node.
///
/// `202 Accepted` with the stored message and a `reply_started` flag. `404`
/// unknown epic/node; `400` blank content; `409` non-interactive kind.
pub async fn post_node_message(
    State(state): State<AppState>,
    Path((epic_id, node_id)): Path<(String, String)>,
    actor: Actor,
    Json(req): Json<PostMessageBody>,
) -> AppResult<(StatusCode, Json<Value>)> {
    let conn = state.db.conn();
    let node = require_interactive_node(conn, &epic_id, &node_id).await?;

    let content = req
        .content
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .ok_or_else(|| AppError::BadRequest("`content` is required and must not be empty".to_string()))?;

    // The session must exist before a turn is stored against it.
    ensure_session(conn, &node_id).await?;

    let message = append_message(conn, &node_id, "user", actor.user_id.as_deref(), content).await?;
    // Fan the human's turn out to every participant subscribed to the node.
    publish_node(&state, &node_id, "message", &message);

    // Claim the per-node run-lock; if a reply is already in flight, the message
    // is stored but no second turn starts (the lock serializes replies).
    let reply_started = match state.try_acquire_node_run(&node_id) {
        Some(guard) => {
            spawn_node_reply(
                state.clone(),
                epic_id.clone(),
                node_id.clone(),
                node.kind.clone(),
                content.to_string(),
                guard,
            );
            true
        }
        None => false,
    };

    Ok((
        StatusCode::ACCEPTED,
        Json(json!({ "message": message, "reply_started": reply_started })),
    ))
}

// ---- run orchestration ------------------------------------------------------

/// What a drained node reply leaves behind, persisted after the stream ends.
#[derive(Default)]
struct ReplyOutcome {
    /// Assembled assistant text (all `Text` deltas) — the agent's turn.
    text: String,
    /// The harness session id captured from `RunEvent::Session` (resume handle).
    session_id: Option<String>,
}

impl ReplyOutcome {
    fn absorb(&mut self, event: &RunEvent) {
        match event {
            RunEvent::Text { delta, .. } => self.text.push_str(delta),
            RunEvent::Session {
                session_id: Some(id),
                ..
            } => self.session_id = Some(id.clone()),
            _ => {}
        }
    }
}

/// Spawn the interactive agent reply in the background and return immediately.
///
/// Holds `guard` (releasing the node's run-lock when the reply finishes),
/// resolves the kind's live settings, resumes the node's native session, drains
/// the blocking `RunEvent` receiver on `spawn_blocking` while relaying every
/// event to `node:<id>`, then persists the assembled agent turn as a
/// `node_message` and records the harness session id for the next turn.
pub fn spawn_node_reply(
    state: AppState,
    epic_id: String,
    node_id: String,
    kind: String,
    user_prompt: String,
    guard: NodeRunGuard,
) {
    tokio::spawn(async move {
        let _guard = guard;
        let conn = state.db.conn();

        let Some(slot) = slot_for_kind(&kind) else {
            tracing::warn!(node = %node_id, kind = %kind, "node reply: non-interactive kind; skipping");
            return;
        };
        let default_prompt = default_prompt_for_kind(&kind).unwrap_or(GRILLING_PROMPT);

        // Resume handle + first-turn detection: with no native session id yet,
        // this is the node's first turn, so we prepend a compact context header.
        let session = fetch_session(conn, &node_id).await.ok().flatten();
        let resume = session.and_then(|s| s.harness_session_id);

        let project_id = crate::epics::get_epic_project_id(conn, &epic_id)
            .await
            .ok()
            .flatten()
            .unwrap_or_default();

        // Live-resolve the kind's slot settings (harness/model/system prompt),
        // exactly like breakdown — a mid-flight settings edit is picked up here.
        let spawn_cfg = match crate::agent_settings::spawn_config(
            &state.db,
            &project_id,
            slot,
            default_prompt,
        )
        .await
        {
            Ok(cfg) => cfg,
            Err(err) => {
                tracing::warn!(node = %node_id, error = %err, "node reply: failed to resolve agent settings; aborting");
                return;
            }
        };

        // Grounding + workspace: a GRILLING node's run works in the project's
        // read-only checkout (its prompt cross-references the code). A
        // PROTOTYPE node's run instead gets a dedicated throwaway SCRATCH
        // WORKSPACE (wayfinder epic §10) — deliberately NOT the target-repo
        // clone — where it builds the artifact its resolution later ships as
        // a `node_asset`. The same directory doubles as the scratch file for
        // the document round trip (`document pull` / the resolution's folded
        // sync).
        let cwd: Option<PathBuf> = if kind == "prototype" {
            match ensure_prototype_scratch(&state, &node_id) {
                Ok(dir) => Some(dir),
                Err(err) => {
                    tracing::warn!(node = %node_id, error = %err, "node reply: failed to create the prototype scratch workspace; aborting");
                    return;
                }
            }
        } else {
            crate::epics::get_epic_clone_path(conn, &epic_id)
                .await
                .ok()
                .flatten()
                .map(PathBuf::from)
        };

        // Wire the resolution surface: mint a per-run capability token scoped
        // to this epic with the node's kind as the phase (the HITL marker —
        // only grilling/prototype phases are allowed to reshape the map, see
        // `crate::capability`), and append the CLI access block to the system
        // prompt. Held for the whole turn; revoked on drop. Without a ready
        // clone or base URL the turn proceeds conversation-only (no CLI).
        let mut _cap_guard: Option<crate::capability::CapabilityGuard> = None;
        let mut system_prompt = spawn_cfg.prompt;
        if let (Some(cwd), Some(base_url)) = (cwd.clone(), state.advertised_base()) {
            let cap = state.caps.mint(
                epic_id.clone(),
                project_id.clone(),
                kind.clone(),
                cwd,
            );
            system_prompt.push_str(&cli_access_block(&NodeCli {
                base_url,
                token: cap.token().to_string(),
            }));
            _cap_guard = Some(cap);
        }

        let prompt = if resume.is_none() {
            format!("{}\n\n{}", first_turn_context(conn, &epic_id, &node_id).await, user_prompt)
        } else {
            user_prompt
        };

        let req = PlanningRunRequest {
            run_id: ulid::Ulid::new().to_string(),
            prompt,
            cwd,
            resume,
            system_prompt,
            slot,
            harness: spawn_cfg.harness,
            model: spawn_cfg.model,
        };

        let rx = state.planner.run(req);
        let hub = state.hub.clone();
        let topic = format!("node:{node_id}");

        // Drain the BLOCKING receiver off the async runtime, relaying live.
        let drained = tokio::task::spawn_blocking(move || {
            let mut outcome = ReplyOutcome::default();
            for event in rx {
                let payload = serde_json::to_value(&event).unwrap_or(Value::Null);
                hub.publish(&topic, ws_type(&event), payload);
                outcome.absorb(&event);
            }
            outcome
        })
        .await;

        let outcome = match drained {
            Ok(outcome) => outcome,
            Err(_) => return, // blocking task panicked; nothing reliable to persist
        };

        // Record the native resume handle so the next turn continues the session.
        if let Some(session_id) = &outcome.session_id {
            if let Err(err) = set_session_resume(conn, &node_id, session_id).await {
                tracing::warn!(node = %node_id, error = %err, "node reply: failed to record resume handle");
            }
        }

        // Persist the agent's turn and fan it out to the node's subscribers.
        let text = outcome.text.trim();
        if !text.is_empty() {
            match append_message(conn, &node_id, "agent", None, text).await {
                Ok(message) => publish_node(&state, &node_id, "message", &message),
                Err(err) => {
                    tracing::warn!(node = %node_id, error = %err, "node reply: failed to persist agent turn")
                }
            }
        }
    });
}

// ---- helpers ----------------------------------------------------------------

/// The scratch workspace a PROTOTYPE node's run works in (wayfinder epic
/// §10): `<scratch_root>/prototype/<node_id>/`, created on first use and
/// reused across the node's turns (a session resumes where it left off).
/// Deliberately **not** a target-repo clone — it lives under
/// `Config::scratch_root` (`DEARBORN_SCRATCH_ROOT`), a tree separate from the
/// per-project clones, because prototype work is throwaway planning material
/// that must never be mistaken for (or leak into) project code.
fn ensure_prototype_scratch(state: &AppState, node_id: &str) -> AppResult<PathBuf> {
    let dir = PathBuf::from(&state.config.scratch_root)
        .join("prototype")
        .join(node_id);
    std::fs::create_dir_all(&dir).map_err(|err| {
        AppError::Internal(format!(
            "failed to create the prototype scratch workspace {}: {err}",
            dir.display()
        ))
    })?;
    Ok(dir)
}

/// Load a node, guard it belongs to `epic_id`, and confirm its kind has an
/// interactive engine. `404` unknown epic/node; `409` for research/task kinds.
async fn require_interactive_node(
    conn: &Connection,
    epic_id: &str,
    node_id: &str,
) -> AppResult<crate::map::MapNode> {
    let node = crate::map::fetch_node(conn, node_id)
        .await?
        .filter(|node| node.epic_id == epic_id)
        .ok_or_else(|| AppError::NotFound(format!("map node {node_id} not found")))?;
    if slot_for_kind(&node.kind).is_none() {
        return Err(AppError::Conflict(format!(
            "map node {node_id} is a `{}` node, which has no interactive session \
             (only {} nodes do)",
            node.kind,
            INTERACTIVE_KINDS.join(" / ")
        )));
    }
    Ok(node)
}

/// A compact context header prepended to a node's first agent turn: the node's
/// id (what `node resolve` addresses), the epic's destination, and the node's
/// own question, so the agent orients before the human's opening message
/// (wayfinder epic §8: it infers "I'm first" from an empty transcript). A
/// prototype node is additionally told its working directory is the
/// throwaway scratch workspace, not the project checkout.
async fn first_turn_context(conn: &Connection, epic_id: &str, node_id: &str) -> String {
    let mut lines = Vec::new();
    lines.push(format!(
        "You are working the node with id {node_id} — pass that exact id to \
         `node resolve` when the decision is settled."
    ));
    if let Ok(Some(node)) = crate::map::fetch_node(conn, node_id).await {
        lines.push(format!("The node: {}.", node.title));
        if let Some(question) = node.question.filter(|q| !q.trim().is_empty()) {
            lines.push(format!("The decision this node resolves: {question}"));
        }
        if node.kind == "prototype" {
            lines.push(
                "Your working directory is a throwaway scratch workspace — build the \\
                 artifact there, and ship it with `node resolve <id> --artifact <file>`."
                    .to_string(),
            );
        }
    }
    if let Ok(mut rows) = conn
        .query("SELECT destination FROM epic WHERE id = ?1", params![epic_id])
        .await
    {
        if let Ok(Some(row)) = rows.next().await {
            if let Ok(Some(destination)) = row.get::<Option<String>>(0) {
                if !destination.trim().is_empty() {
                    lines.push(format!("The epic's destination: {destination}"));
                }
            }
        }
    }
    lines.join("\n")
}

/// Publish a serializable payload as a typed frame on `node:<id>`.
fn publish_node(state: &AppState, node_id: &str, frame_type: &str, payload: &impl Serialize) {
    let payload = serde_json::to_value(payload).unwrap_or(Value::Null);
    state
        .hub
        .publish(&format!("node:{node_id}"), frame_type, payload);
}

/// Current unix time in milliseconds (matches the `*_at` columns).
fn now_ms() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

// ---- tests ------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::planning::testing::{Gate, ScriptedPlanningAgent};
    use crate::users::{self, Role};
    use crate::{app, AppState, Config, Db};
    use axum::body::Body;
    use axum::http::{header::AUTHORIZATION, Request, StatusCode};
    use serde_json::Value;
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::sync::broadcast;
    use tower::ServiceExt; // for `oneshot`

    /// Boot state (with an injected scripted planner) + router.
    async fn boot(planner: Arc<dyn crate::PlanningAgent>) -> (AppState, axum::Router) {
        let db = Db::connect(":memory:").await.unwrap();
        db.run_migrations().await.unwrap();
        let state = AppState::with_planner(Config::for_test(), db, planner);
        let router = app(state.clone());
        (state, router)
    }

    /// Insert a project + epic (with a destination); return (epic_id).
    async fn seed_epic(state: &AppState) -> String {
        let conn = state.db.conn();
        let now = now_ms();
        let project_id = ulid::Ulid::new().to_string();
        conn.execute(
            "INSERT INTO project (id, name, repo_url, clone_path, clone_status, created_at, updated_at) \
             VALUES (?1, 'P', 'https://example.com/p.git', NULL, 'ready', ?2, ?2)",
            params![project_id.clone(), now],
        )
        .await
        .unwrap();
        let epic_id = ulid::Ulid::new().to_string();
        conn.execute(
            "INSERT INTO epic (id, project_id, title, status, destination, created_at, updated_at) \
             VALUES (?1, ?2, 'E', 'Planning', 'It works end to end', ?3, ?3)",
            params![epic_id.clone(), project_id, now],
        )
        .await
        .unwrap();
        epic_id
    }

    /// Insert a node directly through the map store; return its id.
    async fn seed_node(state: &AppState, epic_id: &str, kind: &str, task_mode: Option<&str>) -> String {
        let node = crate::map::create_node(
            state.db.conn(),
            epic_id,
            kind,
            task_mode,
            "Which store?",
            Some("Pick the blob store"),
            None,
            None,
            None,
        )
        .await
        .unwrap();
        node.id
    }

    fn post_json_bearer(uri: &str, token: &str, body: Value) -> Request<Body> {
        Request::builder()
            .method("POST")
            .uri(uri)
            .header(AUTHORIZATION, format!("Bearer {token}"))
            .header("content-type", "application/json")
            .body(Body::from(body.to_string()))
            .unwrap()
    }

    async fn body_json(response: axum::response::Response) -> Value {
        let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        serde_json::from_slice(&bytes).unwrap()
    }

    /// Poll a node's transcript until it has at least `n` messages (or timeout).
    async fn wait_for_messages(state: &AppState, node_id: &str, n: usize) -> Vec<NodeMessage> {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        loop {
            let msgs = list_messages(state.db.conn(), node_id).await.unwrap();
            if msgs.len() >= n || tokio::time::Instant::now() >= deadline {
                return msgs;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    /// Whether the per-node run-lock currently holds `node_id`.
    fn node_locked(state: &AppState, node_id: &str) -> bool {
        state.node_inflight.lock().unwrap().contains(node_id)
    }

    async fn wait_until_unlocked(state: &AppState, node_id: &str) {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        while node_locked(state, node_id) && tokio::time::Instant::now() < deadline {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    // ---- AC: open a node → node-scoped session; posting drives a reply -------

    #[tokio::test]
    async fn opening_a_node_starts_a_session_and_posting_drives_an_attributed_reply() {
        let agent = Arc::new(ScriptedPlanningAgent::new(
            "sess-abc",
            &["Let me grill you. ", "What store?"],
        ));
        let recorded = agent.recorded();
        let (state, app) = boot(agent).await;
        let user = users::testing::seed_user(&state, "planner", Role::Admin, true).await;
        let token = crate::sessions::testing::login_as(&state, &user).await;
        let epic_id = seed_epic(&state).await;
        let node_id = seed_node(&state, &epic_id, "grilling", None).await;
        let session_uri = format!("/epics/{epic_id}/map-nodes/{node_id}/session");
        let messages_uri = format!("/epics/{epic_id}/map-nodes/{node_id}/messages");

        // Opening the node starts a node-scoped session (no resume yet) and
        // flips the node to the soft in_progress signal.
        let opened = app
            .clone()
            .oneshot(post_json_bearer(&session_uri, &token, json!({})))
            .await
            .unwrap();
        assert_eq!(opened.status(), StatusCode::OK);
        let opened = body_json(opened).await;
        assert_eq!(opened["node_id"], node_id.as_str());
        assert_eq!(opened["status"], "active");
        assert_eq!(opened["harness_session_id"], Value::Null);
        assert_eq!(opened["messages"].as_array().unwrap().len(), 0);
        let node = crate::map::fetch_node(state.db.conn(), &node_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(node.state, "in_progress");

        // Any user posts a turn; the per-node lock lets exactly this reply run.
        let posted = app
            .clone()
            .oneshot(post_json_bearer(
                &messages_uri,
                &token,
                json!({ "content": "I think the evidence store fits." }),
            ))
            .await
            .unwrap();
        assert_eq!(posted.status(), StatusCode::ACCEPTED);
        let posted = body_json(posted).await;
        assert_eq!(posted["reply_started"], true);
        assert_eq!(posted["message"]["role"], "user");
        assert_eq!(posted["message"]["actor_user_id"], user.id.as_str());

        // The agent turn lands: user + agent messages, correctly attributed.
        let msgs = wait_for_messages(&state, &node_id, 2).await;
        assert_eq!(msgs.len(), 2);
        assert_eq!(msgs[0].role, "user");
        assert_eq!(msgs[0].actor_user_id.as_deref(), Some(user.id.as_str()));
        assert_eq!(msgs[1].role, "agent");
        assert_eq!(msgs[1].actor_user_id, None);
        assert_eq!(msgs[1].content, "Let me grill you. What store?");

        // The native resume handle was captured onto the session.
        wait_until_unlocked(&state, &node_id).await;
        let session = fetch_session(state.db.conn(), &node_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(session.harness_session_id.as_deref(), Some("sess-abc"));

        // The first run got the grilling system prompt, no resume, and a prompt
        // carrying the first-turn context header + the human's message.
        let runs = recorded.lock().unwrap();
        assert_eq!(runs.len(), 1);
        assert_eq!(runs[0].resume, None);
        assert_eq!(runs[0].system_prompt, GRILLING_PROMPT);
        assert!(runs[0].prompt.contains("It works end to end"));
        assert!(runs[0].prompt.contains("I think the evidence store fits."));
    }

    // ---- AC: a second turn resumes the node's native session ----------------

    #[tokio::test]
    async fn a_second_turn_resumes_the_native_session() {
        let agent = Arc::new(ScriptedPlanningAgent::new("sess-xyz", &["ok"]));
        let recorded = agent.recorded();
        let (state, app) = boot(agent).await;
        let user = users::testing::seed_user(&state, "planner", Role::Admin, true).await;
        let token = crate::sessions::testing::login_as(&state, &user).await;
        let epic_id = seed_epic(&state).await;
        let node_id = seed_node(&state, &epic_id, "grilling", None).await;
        let messages_uri = format!("/epics/{epic_id}/map-nodes/{node_id}/messages");

        for turn in ["first", "second"] {
            let posted = app
                .clone()
                .oneshot(post_json_bearer(
                    &messages_uri,
                    &token,
                    json!({ "content": turn }),
                ))
                .await
                .unwrap();
            assert_eq!(posted.status(), StatusCode::ACCEPTED);
            assert_eq!(body_json(posted).await["reply_started"], true);
            wait_until_unlocked(&state, &node_id).await;
        }

        let runs = recorded.lock().unwrap();
        assert_eq!(runs.len(), 2);
        // First turn: no resume, context-headed prompt.
        assert_eq!(runs[0].resume, None);
        assert!(runs[0].prompt.contains("first"));
        // Second turn: resumes the captured session id; prompt is just the turn.
        assert_eq!(runs[1].resume.as_deref(), Some("sess-xyz"));
        assert_eq!(runs[1].prompt, "second");
    }

    // ---- AC: live RunEvents stream to node:<id> -----------------------------

    #[tokio::test]
    async fn live_run_events_stream_to_the_node_topic() {
        let agent = Arc::new(ScriptedPlanningAgent::new("sess-1", &["hello ", "world"]));
        let (state, app) = boot(agent).await;
        let user = users::testing::seed_user(&state, "planner", Role::Admin, true).await;
        let token = crate::sessions::testing::login_as(&state, &user).await;
        let epic_id = seed_epic(&state).await;
        let node_id = seed_node(&state, &epic_id, "prototype", None).await;
        let sub = state.hub.subscribe(&format!("node:{node_id}"));

        let posted = app
            .oneshot(post_json_bearer(
                &format!("/epics/{epic_id}/map-nodes/{node_id}/messages"),
                &token,
                json!({ "content": "prototype it" }),
            ))
            .await
            .unwrap();
        assert_eq!(posted.status(), StatusCode::ACCEPTED);

        let frames = collect_until_exited(sub).await;
        let types: Vec<&str> = frames.iter().map(|f| f["type"].as_str().unwrap()).collect();
        // The human's turn is fanned out, then the live agent run streams.
        assert!(types.contains(&"message"), "types: {types:?}");
        assert!(types.contains(&"started"), "types: {types:?}");
        assert!(types.contains(&"session"), "types: {types:?}");
        assert!(types.contains(&"text"), "types: {types:?}");
        assert!(types.last() == Some(&"exited"), "types: {types:?}");
    }

    // ---- AC: the per-node run-lock serializes agent replies -----------------

    #[tokio::test]
    async fn the_per_node_run_lock_serializes_replies() {
        let gate = Arc::new(Gate::default());
        let agent =
            Arc::new(ScriptedPlanningAgent::new("sess-g", &["reply"]).with_gate(gate.clone()));
        let (state, app) = boot(agent).await;
        let user = users::testing::seed_user(&state, "planner", Role::Admin, true).await;
        let token = crate::sessions::testing::login_as(&state, &user).await;
        let epic_id = seed_epic(&state).await;
        let node_id = seed_node(&state, &epic_id, "grilling", None).await;
        let messages_uri = format!("/epics/{epic_id}/map-nodes/{node_id}/messages");

        // First post claims the lock and holds the reply in flight (gated).
        let first = app
            .clone()
            .oneshot(post_json_bearer(&messages_uri, &token, json!({ "content": "a" })))
            .await
            .unwrap();
        assert_eq!(body_json(first).await["reply_started"], true);
        assert!(node_locked(&state, &node_id), "the node's reply is in flight");

        // A second post while the reply runs is stored but starts no run.
        let second = app
            .clone()
            .oneshot(post_json_bearer(&messages_uri, &token, json!({ "content": "b" })))
            .await
            .unwrap();
        assert_eq!(body_json(second).await["reply_started"], false);

        // Release the held reply; the lock frees and the run completes.
        gate.release();
        wait_until_unlocked(&state, &node_id).await;
        assert!(!node_locked(&state, &node_id));

        // Both user turns were stored; exactly one agent turn ran (the second
        // post did not spawn its own).
        let msgs = wait_for_messages(&state, &node_id, 3).await;
        let roles: Vec<&str> = msgs.iter().map(|m| m.role.as_str()).collect();
        assert_eq!(roles, vec!["user", "user", "agent"]);
    }

    // ---- AC: two nodes run in parallel without contending -------------------

    #[tokio::test]
    async fn two_nodes_run_in_parallel_without_contending() {
        let gate = Arc::new(Gate::default());
        let agent =
            Arc::new(ScriptedPlanningAgent::new("sess-p", &["reply"]).with_gate(gate.clone()));
        let (state, app) = boot(agent).await;
        let user = users::testing::seed_user(&state, "planner", Role::Admin, true).await;
        let token = crate::sessions::testing::login_as(&state, &user).await;
        let epic_id = seed_epic(&state).await;
        let node_a = seed_node(&state, &epic_id, "grilling", None).await;
        let node_b = seed_node(&state, &epic_id, "prototype", None).await;

        // Post into both nodes; each claims its own per-node lock.
        for node in [&node_a, &node_b] {
            let posted = app
                .clone()
                .oneshot(post_json_bearer(
                    &format!("/epics/{epic_id}/map-nodes/{node}/messages"),
                    &token,
                    json!({ "content": "go" }),
                ))
                .await
                .unwrap();
            assert_eq!(body_json(posted).await["reply_started"], true);
        }

        // Both replies are in flight simultaneously — the locks don't contend.
        assert!(node_locked(&state, &node_a));
        assert!(node_locked(&state, &node_b));

        gate.release();
        wait_until_unlocked(&state, &node_a).await;
        wait_until_unlocked(&state, &node_b).await;
        assert_eq!(wait_for_messages(&state, &node_a, 2).await.len(), 2);
        assert_eq!(wait_for_messages(&state, &node_b, 2).await.len(), 2);
    }

    // ---- AC: a reply runs with a live HITL capability token + CLI surface ---

    #[tokio::test]
    async fn a_reply_is_wired_with_a_capability_token_and_the_cli_access_block() {
        use std::time::Instant;

        let gate = Arc::new(Gate::default());
        let agent =
            Arc::new(ScriptedPlanningAgent::new("sess-cli", &["grilling you"]).with_gate(gate.clone()));
        let recorded = agent.recorded();
        let (state, app) = boot(agent).await;
        // Advertise the loopback base so the turn is wired to the CLI, and
        // give the project a ready clone so the run has a cwd/scratch space.
        *state
            .advertised_base
            .lock()
            .unwrap() = Some("http://127.0.0.1:8787".to_string());
        let user = users::testing::seed_user(&state, "planner", Role::Admin, true).await;
        let token = crate::sessions::testing::login_as(&state, &user).await;
        let epic_id = seed_epic(&state).await;
        state
            .db
            .conn()
            .execute(
                "UPDATE project SET clone_path = '/tmp/dearborn-clone-x' \
                 WHERE id = (SELECT project_id FROM epic WHERE id = ?1)",
                params![epic_id.clone()],
            )
            .await
            .unwrap();
        let node_id = seed_node(&state, &epic_id, "grilling", None).await;

        let posted = app
            .clone()
            .oneshot(post_json_bearer(
                &format!("/epics/{epic_id}/map-nodes/{node_id}/messages"),
                &token,
                json!({ "content": "start grilling" }),
            ))
            .await
            .unwrap();
        assert_eq!(body_json(posted).await["reply_started"], true);

        // Wait for the run to be recorded (the gate holds it in flight).
        let deadline = Instant::now() + Duration::from_secs(5);
        while recorded.lock().unwrap().is_empty() && Instant::now() < deadline {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }

        // The system prompt carries the CLI access block: the base URL, the
        // resolution verb, and the per-run token — which is LIVE in the store
        // while the run is in flight.
        let runs = recorded.lock().unwrap();
        assert_eq!(runs.len(), 1);
        let system_prompt = &runs[0].system_prompt;
        assert!(system_prompt.starts_with(GRILLING_PROMPT));
        assert!(system_prompt.contains("dearborn --url http://127.0.0.1:8787 --token "));
        assert!(system_prompt.contains("node resolve NODE"));
        let token_start = system_prompt
            .find("--token ")
            .map(|i| i + "--token ".len())
            .unwrap();
        let cap_token = system_prompt[token_start..]
            .split_whitespace()
            .next()
            .unwrap()
            .to_string();
        assert!(
            state.caps.resolve(&cap_token).is_some(),
            "the run's capability token must be live while the run is in flight"
        );
        drop(runs);

        // First-turn context names the node id (what `node resolve` addresses).
        assert!(
            recorded.lock().unwrap()[0].prompt.contains(&node_id),
            "the prompt carries the node's id"
        );

        // When the run ends, the guard drops and the token is revoked.
        gate.release();
        wait_until_unlocked(&state, &node_id).await;
        assert!(
            state.caps.resolve(&cap_token).is_none(),
            "the run's capability token must be revoked when the run ends"
        );
    }

    // ---- AC: a prototype run works in a scratch workspace, never the clone
    //         (grilling keeps the project checkout) -----------------------

    #[tokio::test]
    async fn a_prototype_run_gets_a_scratch_workspace_and_grilling_keeps_the_clone() {
        let gate = Arc::new(Gate::default());
        let agent =
            Arc::new(ScriptedPlanningAgent::new("sess-scratch", &["building"]).with_gate(gate.clone()));
        let recorded = agent.recorded();
        let (state, app) = boot(agent).await;
        // Advertise the base so the turn is CLI-wired, and give the project a
        // ready clone — which the PROTOTYPE run must NOT work in.
        *state.advertised_base.lock().unwrap() = Some("http://127.0.0.1:8787".to_string());
        let user = users::testing::seed_user(&state, "planner", Role::Admin, true).await;
        let token = crate::sessions::testing::login_as(&state, &user).await;
        let epic_id = seed_epic(&state).await;
        state
            .db
            .conn()
            .execute(
                "UPDATE project SET clone_path = '/tmp/dearborn-clone-x' \
                 WHERE id = (SELECT project_id FROM epic WHERE id = ?1)",
                params![epic_id.clone()],
            )
            .await
            .unwrap();

        async fn run_turn(
            app: &axum::Router,
            token: &str,
            epic_id: &str,
            node_id: &str,
        ) {
            let posted = app
                .clone()
                .oneshot(post_json_bearer(
                    &format!("/epics/{epic_id}/map-nodes/{node_id}/messages"),
                    token,
                    json!({ "content": "go" }),
                ))
                .await
                .unwrap();
            assert_eq!(body_json(posted).await["reply_started"], true);
        }

        // The prototype node's turn runs in a scratch workspace under the
        // configured scratch root — a fresh directory, NOT the clone.
        let prototype_id = seed_node(&state, &epic_id, "prototype", None).await;
        run_turn(&app, &token, &epic_id, &prototype_id).await;
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        while recorded.lock().unwrap().len() < 1
            && tokio::time::Instant::now() < deadline
        {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        {
            let runs = recorded.lock().unwrap();
            let cwd = runs[0].cwd.as_deref().expect("prototype run gets a cwd");
            let expected = std::path::Path::new(&state.config.scratch_root)
                .join("prototype")
                .join(&prototype_id);
            assert_eq!(cwd, expected.as_path());
            assert!(cwd.is_dir(), "the scratch workspace is created on first use");
            assert_ne!(cwd, std::path::Path::new("/tmp/dearborn-clone-x"));
            // The prototype prompt tells the agent the same thing.
            assert!(runs[0].system_prompt.starts_with(PROTOTYPE_PROMPT));
            assert!(runs[0].system_prompt.contains("--artifact PATH"));
            assert!(runs[0].prompt.contains("throwaway scratch workspace"));
        }
        gate.release();
        wait_until_unlocked(&state, &prototype_id).await;

        // A grilling node's turn on the SAME epic still works in the project's
        // read-only checkout.
        let grilling_id = seed_node(&state, &epic_id, "grilling", None).await;
        run_turn(&app, &token, &epic_id, &grilling_id).await;
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        while recorded.lock().unwrap().len() < 2
            && tokio::time::Instant::now() < deadline
        {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        {
            let runs = recorded.lock().unwrap();
            assert_eq!(
                runs[1].cwd.as_deref(),
                Some(std::path::Path::new("/tmp/dearborn-clone-x"))
            );
        }
        gate.release();
        wait_until_unlocked(&state, &grilling_id).await;
    }

    // ---- non-interactive kinds have no interactive session ------------------

    #[tokio::test]
    async fn research_and_task_nodes_have_no_interactive_session() {
        let agent = Arc::new(ScriptedPlanningAgent::new("s", &["x"]));
        let (state, app) = boot(agent).await;
        let user = users::testing::seed_user(&state, "planner", Role::Admin, true).await;
        let token = crate::sessions::testing::login_as(&state, &user).await;
        let epic_id = seed_epic(&state).await;
        let research = seed_node(&state, &epic_id, "research", None).await;
        let task = seed_node(&state, &epic_id, "task", Some("afk")).await;

        for node in [&research, &task] {
            let open = app
                .clone()
                .oneshot(post_json_bearer(
                    &format!("/epics/{epic_id}/map-nodes/{node}/session"),
                    &token,
                    json!({}),
                ))
                .await
                .unwrap();
            assert_eq!(open.status(), StatusCode::CONFLICT);
            let post = app
                .clone()
                .oneshot(post_json_bearer(
                    &format!("/epics/{epic_id}/map-nodes/{node}/messages"),
                    &token,
                    json!({ "content": "hi" }),
                ))
                .await
                .unwrap();
            assert_eq!(post.status(), StatusCode::CONFLICT);
        }
    }

    // ---- guards: unknown node, blank content --------------------------------

    #[tokio::test]
    async fn posting_validates_node_and_content() {
        let agent = Arc::new(ScriptedPlanningAgent::new("s", &["x"]));
        let (state, app) = boot(agent).await;
        let user = users::testing::seed_user(&state, "planner", Role::Admin, true).await;
        let token = crate::sessions::testing::login_as(&state, &user).await;
        let epic_id = seed_epic(&state).await;
        let node_id = seed_node(&state, &epic_id, "grilling", None).await;

        // Unknown node → 404.
        let missing = app
            .clone()
            .oneshot(post_json_bearer(
                &format!("/epics/{epic_id}/map-nodes/nope/messages"),
                &token,
                json!({ "content": "hi" }),
            ))
            .await
            .unwrap();
        assert_eq!(missing.status(), StatusCode::NOT_FOUND);

        // Blank content → 400.
        let blank = app
            .oneshot(post_json_bearer(
                &format!("/epics/{epic_id}/map-nodes/{node_id}/messages"),
                &token,
                json!({ "content": "   " }),
            ))
            .await
            .unwrap();
        assert_eq!(blank.status(), StatusCode::BAD_REQUEST);
    }

    async fn collect_until_exited(mut rx: broadcast::Receiver<Arc<str>>) -> Vec<Value> {
        let mut frames = Vec::new();
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        loop {
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            match tokio::time::timeout(remaining, rx.recv()).await {
                Ok(Ok(env)) => {
                    let value: Value = serde_json::from_str(&env).unwrap();
                    let is_exit = value["type"] == "exited";
                    frames.push(value);
                    if is_exit {
                        return frames;
                    }
                }
                Ok(Err(broadcast::error::RecvError::Lagged(_))) => continue,
                _ => return frames,
            }
        }
    }
}
