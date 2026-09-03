//! The planning map: decision nodes, their dependency edges, and the four
//! wayfinder prose fields (epic "Wayfinder-Inspired Planning" §3, §4.1–4.2).
//!
//! A **map node** is one decision/investigation sized to a single agent
//! session (`kind`: grilling | research | prototype | task); the **map** is the
//! graph of those nodes for one epic. Readiness is COMPUTED, never stored —
//! `frontier` = open (or in-progress) with every blocker settled
//! (`resolved` or `out_of_scope`), `blocked` = open with some blocker not yet
//! settled — mirroring the executor DAG's computed readiness. Fog is never a
//! node state: it is the epic-level `not_yet_specified` prose, edited
//! alongside `destination` / `notes` / `out_of_scope` through this module's
//! prose surface (`PATCH /epics/{id}/map`).
//!
//! Every map mutation publishes a `map_updated` frame on `epic:<id>` carrying
//! the full computed map (nodes + edges + prose), so a subscribed client
//! re-renders with correct frontier/blocked state — the same pattern as
//! `publish_dag`'s `dag_updated` frames.
//!
//! The engines that *drive* nodes — grilling/prototype interactive runs
//! ([`crate::node_engine`]) and research/AFK-task one-shots
//! ([`crate::afk_engine`]) — build on this module; the rich grilling
//! resolution bundle (document edits, fog graduation, map reshaping) lives in
//! [`crate::resolve`], layered on top of this node/edge/prose CRUD +
//! computation layer.
//! `PATCH /epics/{id}/map-nodes/{id}` with `state = "resolved"` is the minimal
//! state transition the frontier computation needs to be observable end to
//! end; the resolution flow builds on top of it.
//!
//! The map also carries its computed **completion eligibility** (plan §8):
//! the way is clear — ready to break down — when no open (or in-progress)
//! nodes remain AND the fog (`not_yet_specified`) is empty. Like readiness it
//! is computed on every read, never stored, and it is what gates breakdown
//! ([`crate::breakdown::trigger_breakdown`]: only a human may pull that
//! trigger, and only once this says the way is clear).
//!
//! The REST surface is exactly what the `dearborn` CLI's map verbs call
//! (`node create|link|resolve`, `map` query + `map set-destination|set-notes|
//! set-fog|set-out-of-scope`), so it is on the capability-token allow-list
//! (`crate::capability::authorize_cap_request`) and accepts either a browser
//! session token or a per-run capability token.

use std::collections::{HashMap, HashSet};

use axum::{
    async_trait,
    extract::{Path, State},
    http::{request::Parts, StatusCode},
    Json,
};
use libsql::{params, params_from_iter, Connection, Value};
use serde::{Deserialize, Deserializer, Serialize};

use crate::{AppError, AppResult, AppState};

/// The node kinds (plan §5): grilling / prototype are HITL, research is AFK,
/// task is AFK or HITL (fixed at creation via `task_mode`).
pub(crate) const VALID_KINDS: &[&str] = &["grilling", "research", "prototype", "task"];

/// The `task_mode` vocabulary — only meaningful for `kind = "task"`, fixed at
/// creation.
const VALID_TASK_MODES: &[&str] = &["afk", "hitl"];

/// The kinds allowed to RESHAPE the map (wayfinder epic §6): the HITL
/// grilling/prototype sessions are the primary map-builders; research and
/// task nodes report facts / record manual work and never redraw the map.
pub(crate) const MAP_RESHAPING_KINDS: &[&str] = &["grilling", "prototype"];

/// Validate a node `kind` (the §5 vocabulary).
pub(crate) fn validate_kind(kind: &str) -> Result<(), AppError> {
    if !VALID_KINDS.contains(&kind) {
        return Err(AppError::BadRequest(format!(
            "`kind` must be one of grilling|research|prototype|task, got `{kind}`"
        )));
    }
    Ok(())
}

/// Validate a `task_mode` for `kind` and normalize it (trimmed, empty →
/// `None`). The shared rules of every node-creation surface (the
/// `POST /map-nodes` handler, the grilling resolution bundle's graduations,
/// out-of-scope rulings): `task_mode` is required for `kind = "task"` and
/// rejected for every other kind (it is fixed at creation, plan §4.1).
pub(crate) fn validate_task_mode(
    kind: &str,
    task_mode: Option<&str>,
) -> Result<Option<String>, AppError> {
    let task_mode = task_mode.map(str::trim).filter(|s| !s.is_empty());
    match (kind, task_mode) {
        ("task", Some(mode)) if VALID_TASK_MODES.contains(&mode) => Ok(Some(mode.to_string())),
        ("task", Some(mode)) => Err(AppError::BadRequest(format!(
            "`task_mode` must be one of afk|hitl, got `{mode}`"
        ))),
        ("task", None) => Err(AppError::BadRequest(
            "`task_mode` is required for kind `task` (afk|hitl), fixed at creation".to_string(),
        )),
        (_, Some(_)) => Err(AppError::BadRequest(
            "`task_mode` is only valid for kind `task`".to_string(),
        )),
        (_, None) => Ok(None),
    }
}

/// The node-state vocabulary (plan §4.1).
pub(crate) const VALID_STATES: &[&str] = &["open", "in_progress", "resolved", "out_of_scope"];

/// States still considered "open" for frontier purposes (not settled).
const OPEN_STATES: &[&str] = &["open", "in_progress"];

/// States that settle a node for dependency purposes: a `resolved` decision
/// and an `out_of_scope` ruling both unblock dependents (plan §6 — "rule out
/// of scope: create+close an out_of_scope node").
const SETTLED_STATES: &[&str] = &["resolved", "out_of_scope"];

const NODE_COLUMNS: &str = "id, epic_id, kind, task_mode, state, title, question, gist, \
     out_of_scope_reason, created_by, resolved_by, position_x, position_y, created_at, updated_at";

// ---- DTOs ------------------------------------------------------------------

/// A map node as stored. Readiness is deliberately **not** a column — it is
/// computed from the dependency graph ([`readiness_index`]) and exposed on
/// [`MapNodeView`].
#[derive(Debug, Clone, Serialize)]
pub struct MapNode {
    pub id: String,
    pub epic_id: String,
    pub kind: String,
    /// For `kind = "task"` only: `afk | hitl`, fixed at creation.
    pub task_mode: Option<String>,
    pub state: String,
    pub title: String,
    /// The decision/investigation this node resolves.
    pub question: Option<String>,
    /// One-line resolution answer (set on resolve).
    pub gist: Option<String>,
    pub out_of_scope_reason: Option<String>,
    /// Which human created/resolved the node (NULL when the actor was an
    /// agent run's capability token).
    pub created_by: Option<String>,
    pub resolved_by: Option<String>,
    /// Graph layout (nullable — auto-layout may own this).
    pub position_x: Option<f64>,
    pub position_y: Option<f64>,
    pub created_at: i64,
    pub updated_at: i64,
}

/// A map node plus its computed readiness: `frontier` (open with all
/// dependencies resolved) and `blocked_by` (the unsettled blocker ids).
/// Computed from the graph on every read, never stored (plan §3).
#[derive(Debug, Clone, Serialize)]
pub struct MapNodeView {
    #[serde(flatten)]
    pub node: MapNode,
    /// Whether this node is on the frontier: open (or in-progress) with every
    /// blocker settled. Computed from dependencies, never stored.
    pub frontier: bool,
    /// Blocker ids not yet settled (empty unless open and not on the frontier).
    pub blocked_by: Vec<String>,
}

/// A dependency edge `(blocker_id, blocked_id)` — "blocker blocks blocked".
#[derive(Debug, Clone, Serialize)]
pub struct MapEdge {
    pub blocker_id: String,
    pub blocked_id: String,
}

/// The epic's computed completion eligibility (wayfinder plan §8): whether
/// "the way is clear — ready to break down". Derived from the live node
/// graph + the fog prose on every read, never stored.
#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct MapCompletion {
    /// `true` when no open (or in-progress) map nodes remain AND the fog
    /// (`epic.not_yet_specified`) is empty — breakdown may be offered. A
    /// human still pulls the trigger ([`crate::breakdown`]).
    pub eligible: bool,
    /// How many nodes are still open or in progress (0 when eligible).
    pub open_nodes: usize,
    /// Whether fog prose remains (`not_yet_specified` non-empty).
    pub fog_remaining: bool,
}

/// The epic's planning map: the four wayfinder prose fields plus the node
/// graph with per-node computed readiness. This is the whole `map_updated`
/// WS payload, so a single frame re-renders a client's map view completely.
#[derive(Debug, Clone, Serialize)]
pub struct Map {
    pub epic_id: String,
    /// What the finished plan looks like — fixes scope (plan §3).
    pub destination: Option<String>,
    /// Optional freeform prose alongside the destination.
    pub notes: Option<String>,
    /// In-scope decisions not yet sharp enough to be nodes — fog is prose,
    /// never nodes (plan §3).
    pub not_yet_specified: Option<String>,
    /// Work explicitly ruled beyond the destination (the prose line; the
    /// terminal node state is `map_node.state = 'out_of_scope'`).
    pub out_of_scope: Option<String>,
    pub nodes: Vec<MapNodeView>,
    pub edges: Vec<MapEdge>,
    /// Computed completion eligibility: "the way is clear — ready to break
    /// down" once no open nodes remain and the fog is empty (plan §8).
    pub completion: MapCompletion,
}

/// `POST /epics/{id}/map-nodes` body. `title` and `kind` are required;
/// `task_mode` is required for `kind = "task"` and rejected for every other
/// kind (it is fixed at creation, plan §4.1). `blocked_by` lists ids of
/// existing nodes that block the new node (the graduation shape: a resolving
/// node's children depend on it); `blocks` — matching the task DAG's
/// `--blocks` convention — lists ids of existing nodes the new node blocks.
/// An empty/whitespace `question` stores `NULL`.
#[derive(Debug, Deserialize)]
pub struct CreateMapNodeBody {
    kind: Option<String>,
    title: Option<String>,
    #[serde(default)]
    question: Option<String>,
    #[serde(default)]
    task_mode: Option<String>,
    /// Ids of existing map nodes that block this new node (optional).
    #[serde(default)]
    blocked_by: Vec<String>,
    /// Ids of existing map nodes this new node blocks (optional; the
    /// task-create convention).
    #[serde(default)]
    blocks: Vec<String>,
    #[serde(default)]
    position_x: Option<f64>,
    #[serde(default)]
    position_y: Option<f64>,
}

/// `PATCH /epics/{id}/map-nodes/{nodeId}` body. Every field is optional;
/// absent fields stay untouched. `state` must be in the §4.1 vocabulary.
/// `question` / `gist` / `out_of_scope_reason` are trimmed, with an empty
/// string stored as `NULL`; positions take `null` to clear.
#[derive(Debug, Default, Deserialize)]
pub struct UpdateMapNodeBody {
    #[serde(default)]
    pub(crate) state: Option<String>,
    #[serde(default)]
    pub(crate) title: Option<String>,
    #[serde(default)]
    pub(crate) question: Option<String>,
    #[serde(default)]
    pub(crate) gist: Option<String>,
    #[serde(default)]
    pub(crate) out_of_scope_reason: Option<String>,
    #[serde(default, deserialize_with = "double_option")]
    pub(crate) position_x: Option<Option<f64>>,
    #[serde(default, deserialize_with = "double_option")]
    pub(crate) position_y: Option<Option<f64>>,
}

/// `POST /epics/{id}/map-node-dependencies` body: `blocker` blocks `blocked`
/// (the blocker must settle first).
#[derive(Debug, Deserialize)]
pub struct LinkMapNodesBody {
    blocker_id: Option<String>,
    blocked_id: Option<String>,
}

/// `PATCH /epics/{id}/map` body — the four wayfinder prose fields. Every
/// field is optional; absent fields stay untouched. `destination` must be
/// non-empty when present (it fixes the map's scope); the other three accept
/// an empty string to clear to `NULL`.
#[derive(Debug, Default, Deserialize)]
pub struct UpdateMapProseBody {
    #[serde(default)]
    pub(crate) destination: Option<String>,
    #[serde(default)]
    pub(crate) notes: Option<String>,
    #[serde(default)]
    pub(crate) not_yet_specified: Option<String>,
    #[serde(default)]
    pub(crate) out_of_scope: Option<String>,
}

/// Deserialize a present-but-maybe-null field into `Some(_)`, leaving an
/// absent field as `None` (via `#[serde(default)]`). This distinguishes "set
/// to null" from "not provided" for partial updates (mirrors `epics.rs`).
fn double_option<'de, D, T>(deserializer: D) -> Result<Option<Option<T>>, D::Error>
where
    D: Deserializer<'de>,
    T: Deserialize<'de>,
{
    Deserialize::deserialize(deserializer).map(Some)
}

// ---- the actor: who is mutating the map ------------------------------------

/// The actor behind a map mutation: a signed-in human (its user id) or an
/// agent run (a capability token — no user id, so attribution stays `NULL`).
/// [`crate::auth::require_auth`] has already resolved exactly one of the two
/// into the request extensions by the time any map handler runs.
pub struct Actor {
    pub user_id: Option<String>,
}

#[async_trait]
impl axum::extract::FromRequestParts<AppState> for Actor {
    type Rejection = AppError;

    async fn from_request_parts(
        parts: &mut Parts,
        _state: &AppState,
    ) -> Result<Self, Self::Rejection> {
        if let Some(claims) = parts.extensions.get::<crate::auth::Claims>() {
            return Ok(Actor {
                user_id: Some(claims.sub.clone()),
            });
        }
        if parts
            .extensions
            .get::<crate::capability::CapabilityScope>()
            .is_some()
        {
            // An agent run acting through its capability token: attribution
            // records no human.
            return Ok(Actor { user_id: None });
        }
        Err(AppError::Unauthorized)
    }
}

// ---- store: nodes ----------------------------------------------------------

/// Insert a map node, landing it in `state='open'`. Validation of `kind` /
/// `task_mode` / `title` is the caller's job (the handlers do it with
/// route-specific error messages); this is the pure write.
pub async fn create_node(
    conn: &Connection,
    epic_id: &str,
    kind: &str,
    task_mode: Option<&str>,
    title: &str,
    question: Option<&str>,
    created_by: Option<&str>,
    position_x: Option<f64>,
    position_y: Option<f64>,
) -> AppResult<MapNode> {
    let id = ulid::Ulid::new().to_string();
    let now = now_ms();

    conn.execute(
        "INSERT INTO map_node \
             (id, epic_id, kind, task_mode, state, title, question, gist, \
              out_of_scope_reason, created_by, resolved_by, position_x, position_y, created_at, updated_at) \
         VALUES (?1, ?2, ?3, ?4, 'open', ?5, ?6, NULL, NULL, ?7, NULL, ?8, ?9, ?10, ?10)",
        params![
            id.clone(),
            epic_id,
            kind,
            task_mode,
            title,
            question,
            created_by,
            position_x,
            position_y,
            now
        ],
    )
    .await?;

    fetch_node(conn, &id)
        .await?
        .ok_or_else(|| AppError::Internal(format!("map node {id} vanished after insert")))
}

/// Fetch one map node by id, or `None`.
pub async fn fetch_node(conn: &Connection, id: &str) -> AppResult<Option<MapNode>> {
    let sql = format!("SELECT {NODE_COLUMNS} FROM map_node WHERE id = ?1");
    let mut rows = conn.query(&sql, params![id]).await?;
    match rows.next().await? {
        Some(row) => Ok(Some(row_to_node(&row)?)),
        None => Ok(None),
    }
}

/// All map nodes under `epic_id`, oldest first (then id for stability).
pub async fn list_nodes_for_epic(
    conn: &Connection,
    epic_id: &str,
) -> AppResult<Vec<MapNode>> {
    let sql = format!(
        "SELECT {NODE_COLUMNS} FROM map_node WHERE epic_id = ?1 \
         ORDER BY created_at ASC, id ASC"
    );
    let mut rows = conn.query(&sql, params![epic_id]).await?;
    let mut items = Vec::new();
    while let Some(row) = rows.next().await? {
        items.push(row_to_node(&row)?);
    }
    Ok(items)
}

/// Whether `node_id` exists and belongs to `epic_id`.
pub async fn node_belongs_to_epic(
    conn: &Connection,
    node_id: &str,
    epic_id: &str,
) -> AppResult<bool> {
    Ok(matches!(
        fetch_node(conn, node_id).await?,
        Some(node) if node.epic_id == epic_id
    ))
}

/// Partially update a map node. Each field is optional: absent → untouched.
/// `state = "resolved"` also stamps `resolved_by` with the acting user (NULL
/// when the actor was an agent run). `question` / `gist` /
/// `out_of_scope_reason` trim, storing an empty string as `NULL`. `updated_at`
/// always bumps. `404` if the node does not exist.
///
/// This is the minimal state-transition surface the frontier computation
/// needs; the rich grilling resolution bundle (document edits, fog
/// graduation, map reshaping) in [`crate::resolve`] builds on it.
pub async fn update_node(
    conn: &Connection,
    node_id: &str,
    patch: UpdateMapNodeBody,
    actor_user_id: Option<&str>,
) -> AppResult<MapNode> {
    let mut assignments: Vec<&str> = Vec::new();
    let mut values: Vec<Value> = Vec::new();

    if let Some(state) = patch.state {
        if !VALID_STATES.contains(&state.as_str()) {
            return Err(AppError::BadRequest(format!(
                "`state` must be one of open|in_progress|resolved|out_of_scope, got `{state}`"
            )));
        }
        assignments.push("state = ?");
        values.push(Value::Text(state.clone()));
        if state == "resolved" {
            assignments.push("resolved_by = ?");
            values.push(match actor_user_id {
                Some(user_id) => Value::Text(user_id.to_string()),
                None => Value::Null,
            });
        }
    }

    if let Some(title) = patch.title {
        assignments.push("title = ?");
        values.push(Value::Text(
            require_non_empty(&title, "title")?.to_string(),
        ));
    }

    let UpdateMapNodeBody {
        state: _,
        title: _,
        question,
        gist,
        out_of_scope_reason,
        position_x,
        position_y,
    } = patch;
    for (column, field) in [
        ("question = ?", question),
        ("gist = ?", gist),
        ("out_of_scope_reason = ?", out_of_scope_reason),
    ] {
        if let Some(value) = field {
            assignments.push(column);
            values.push(trimmed_or_null(&value));
        }
    }
    for (column, field) in [
        ("position_x = ?", position_x),
        ("position_y = ?", position_y),
    ] {
        if let Some(value) = field {
            assignments.push(column);
            values.push(match value {
                Some(coordinate) => Value::Real(coordinate),
                None => Value::Null,
            });
        }
    }

    // Always bump updated_at, even for an otherwise-empty patch.
    assignments.push("updated_at = ?");
    values.push(Value::Integer(now_ms()));
    // Bind the id last, matching the trailing `WHERE id = ?`.
    values.push(Value::Text(node_id.to_string()));

    let sql = format!("UPDATE map_node SET {} WHERE id = ?", assignments.join(", "));
    let affected = conn.execute(&sql, params_from_iter(values)).await?;
    if affected == 0 {
        return Err(AppError::NotFound(format!("map node {node_id} not found")));
    }

    fetch_node(conn, node_id)
        .await?
        .ok_or_else(|| AppError::Internal(format!("map node {node_id} vanished after update")))
}

// ---- store: dependency edges ----------------------------------------------

/// Link a dependency edge `(blocker_id, blocked_id)` ("blocker blocks
/// blocked"). Self-links are rejected; both nodes must exist and belong to the
/// same epic; a cycle is rejected with `409` — the check mirrors the task
/// DAG's ([`crate::tasks::would_create_cycle`]): a cycle appears iff
/// `blocked_id` can already reach `blocker_id` by following existing edges
/// forward, so the new edge would close the loop.
pub async fn link_nodes(
    conn: &Connection,
    blocker_id: &str,
    blocked_id: &str,
) -> AppResult<()> {
    if blocker_id == blocked_id {
        return Err(AppError::BadRequest(
            "a map node cannot depend on itself".to_string(),
        ));
    }

    let blocker = fetch_node(conn, blocker_id)
        .await?
        .ok_or_else(|| AppError::NotFound(format!("map node {blocker_id} not found")))?;
    let blocked = fetch_node(conn, blocked_id)
        .await?
        .ok_or_else(|| AppError::NotFound(format!("map node {blocked_id} not found")))?;

    if blocker.epic_id != blocked.epic_id {
        return Err(AppError::BadRequest(
            "both map nodes must belong to the same epic to be linked".to_string(),
        ));
    }

    if would_create_cycle(conn, blocker_id, blocked_id).await? {
        return Err(AppError::Conflict(format!(
            "linking {blocker_id} → {blocked_id} would create a dependency cycle"
        )));
    }

    conn.execute(
        "INSERT OR IGNORE INTO map_node_dependency (blocker_id, blocked_id) VALUES (?1, ?2)",
        params![blocker_id, blocked_id],
    )
    .await?;
    Ok(())
}

/// All dependency edges among the nodes of `epic_id`, joined back to
/// `map_node` so an edge is only surfaced when both of its nodes live under
/// this epic (same robustness as `tasks::list_dependencies_for_epic`).
pub async fn list_edges_for_epic(
    conn: &Connection,
    epic_id: &str,
) -> AppResult<Vec<MapEdge>> {
    let mut rows = conn
        .query(
            "SELECT d.blocker_id, d.blocked_id FROM map_node_dependency d \
             JOIN map_node b ON b.id = d.blocker_id \
             JOIN map_node k ON k.id = d.blocked_id \
             WHERE b.epic_id = ?1 AND k.epic_id = ?1 \
             ORDER BY d.blocker_id ASC, d.blocked_id ASC",
            params![epic_id],
        )
        .await?;
    let mut items = Vec::new();
    while let Some(row) = rows.next().await? {
        items.push(MapEdge {
            blocker_id: row.get(0)?,
            blocked_id: row.get(1)?,
        });
    }
    Ok(items)
}

/// Whether adding edge `(blocker_id, blocked_id)` would create a cycle:
/// an iterative forward DFS from `blocked_id` looking for `blocker_id`
/// (mirrors `tasks::would_create_cycle` on the task tables).
pub async fn would_create_cycle(
    conn: &Connection,
    blocker_id: &str,
    blocked_id: &str,
) -> AppResult<bool> {
    let mut stack = vec![blocked_id.to_string()];
    let mut seen: HashSet<String> = HashSet::new();
    while let Some(node) = stack.pop() {
        if node == blocker_id {
            return Ok(true);
        }
        if !seen.insert(node.clone()) {
            continue;
        }
        let mut rows = conn
            .query(
                "SELECT blocked_id FROM map_node_dependency WHERE blocker_id = ?1",
                params![node],
            )
            .await?;
        while let Some(row) = rows.next().await? {
            stack.push(row.get::<String>(0)?);
        }
    }
    Ok(false)
}

// ---- computed map ----------------------------------------------------------

/// Per-node computed readiness: `(frontier, blocked_by)` by node id.
///
/// `frontier` = state is open (or in-progress) AND every blocker is settled;
/// `blocked_by` = the unsettled blocker ids when open and not on the frontier.
/// Settled means `resolved` **or** `out_of_scope` — ruling work out of scope
/// unblocks its dependents just like resolving it does (plan §6).
fn readiness_index(
    nodes: &[MapNode],
    edges: &[MapEdge],
) -> HashMap<String, (bool, Vec<String>)> {
    let settled: HashSet<&str> = nodes
        .iter()
        .filter(|n| SETTLED_STATES.contains(&n.state.as_str()))
        .map(|n| n.id.as_str())
        .collect();
    let mut blockers: HashMap<&str, Vec<&str>> = HashMap::new();
    for edge in edges {
        blockers
            .entry(edge.blocked_id.as_str())
            .or_default()
            .push(edge.blocker_id.as_str());
    }

    let mut index = HashMap::new();
    for node in nodes {
        let open = OPEN_STATES.contains(&node.state.as_str());
        let incoming = blockers.get(node.id.as_str()).cloned().unwrap_or_default();
        let frontier = open && incoming.iter().all(|b| settled.contains(b));
        let blocked_by = if open && !frontier {
            incoming
                .iter()
                .filter(|b| !settled.contains(**b))
                .map(|b| b.to_string())
                .collect()
        } else {
            Vec::new()
        };
        index.insert(node.id.clone(), (frontier, blocked_by));
    }
    index
}

/// The epic's four prose fields, or `None` if the epic does not exist.
async fn epic_prose(
    conn: &Connection,
    epic_id: &str,
) -> AppResult<Option<(Option<String>, Option<String>, Option<String>, Option<String>)>> {
    let mut rows = conn
        .query(
            "SELECT destination, notes, not_yet_specified, out_of_scope \
             FROM epic WHERE id = ?1",
            params![epic_id],
        )
        .await?;
    match rows.next().await? {
        Some(row) => Ok(Some((
            row.get(0)?,
            row.get(1)?,
            row.get(2)?,
            row.get(3)?,
        ))),
        None => Ok(None),
    }
}

/// Compute the epic's full map: nodes with per-node computed readiness, the
/// dependency edges, and the four prose fields. `404` if the epic does not
/// exist — every caller gets that guard from here.
pub async fn compute_map(conn: &Connection, epic_id: &str) -> AppResult<Map> {
    let (destination, notes, not_yet_specified, out_of_scope) = epic_prose(conn, epic_id)
        .await?
        .ok_or_else(|| AppError::NotFound(format!("epic {epic_id} not found")))?;
    let nodes = list_nodes_for_epic(conn, epic_id).await?;
    let edges = list_edges_for_epic(conn, epic_id).await?;

    let completion = compute_completion(&nodes, not_yet_specified.as_deref());
    let readiness = readiness_index(&nodes, &edges);
    let nodes = nodes
        .into_iter()
        .map(|node| {
            let (frontier, blocked_by) = readiness
                .get(&node.id)
                .cloned()
                .unwrap_or((false, Vec::new()));
            MapNodeView {
                node,
                frontier,
                blocked_by,
            }
        })
        .collect();

    Ok(Map {
        epic_id: epic_id.to_string(),
        destination,
        notes,
        not_yet_specified,
        out_of_scope,
        nodes,
        edges,
        completion,
    })
}

/// Compute completion eligibility (wayfinder plan §8): the way is clear when
/// no node is still open (or in progress — being worked, not settled) AND the
/// fog prose is empty (`NULL` or blank). Every node kind counts — a leftover
/// open task node blocks completion exactly like an open grilling node — and
/// `resolved` / `out_of_scope` nodes never do (settled, like dependencies).
pub(crate) fn compute_completion(
    nodes: &[MapNode],
    not_yet_specified: Option<&str>,
) -> MapCompletion {
    let open_nodes = nodes
        .iter()
        .filter(|n| OPEN_STATES.contains(&n.state.as_str()))
        .count();
    let fog_remaining = not_yet_specified
        .map(|fog| !fog.trim().is_empty())
        .unwrap_or(false);
    MapCompletion {
        eligible: open_nodes == 0 && !fog_remaining,
        open_nodes,
        fog_remaining,
    }
}

// ---- prose -----------------------------------------------------------------

/// Apply the four wayfinder prose fields to the epic (the `map set-*` CLI
/// verbs' write path). `destination` must be non-empty when present; the other
/// three trim, storing an empty string as `NULL`. `updated_at` always bumps.
/// `404` if the epic does not exist.
pub async fn update_prose(
    conn: &Connection,
    epic_id: &str,
    patch: UpdateMapProseBody,
) -> AppResult<()> {
    let mut assignments: Vec<&str> = Vec::new();
    let mut values: Vec<Value> = Vec::new();

    if let Some(destination) = patch.destination {
        assignments.push("destination = ?");
        values.push(Value::Text(
            require_non_empty(&destination, "destination")?.to_string(),
        ));
    }

    let UpdateMapProseBody {
        destination: _,
        notes,
        not_yet_specified,
        out_of_scope,
    } = patch;
    for (column, field) in [
        ("notes = ?", notes),
        ("not_yet_specified = ?", not_yet_specified),
        ("out_of_scope = ?", out_of_scope),
    ] {
        if let Some(value) = field {
            assignments.push(column);
            values.push(trimmed_or_null(&value));
        }
    }

    assignments.push("updated_at = ?");
    values.push(Value::Integer(now_ms()));
    values.push(Value::Text(epic_id.to_string()));

    let sql = format!("UPDATE epic SET {} WHERE id = ?", assignments.join(", "));
    let affected = conn.execute(&sql, params_from_iter(values)).await?;
    if affected == 0 {
        return Err(AppError::NotFound(format!("epic {epic_id} not found")));
    }
    Ok(())
}

/// Append one line to the epic's out-of-scope prose (the wayfinder §6 "rule
/// things out of scope: create+close an out_of_scope node + prose line"
/// write path). The line is appended on its own line after any existing prose
/// (or becomes the first line); blank input is rejected. `404` unknown epic.
pub async fn append_out_of_scope_prose(
    conn: &Connection,
    epic_id: &str,
    line: &str,
) -> AppResult<()> {
    let line = line.trim();
    if line.is_empty() {
        return Err(AppError::BadRequest(
            "the out-of-scope prose line must not be empty".to_string(),
        ));
    }
    let mut rows = conn
        .query("SELECT out_of_scope FROM epic WHERE id = ?1", params![epic_id])
        .await?;
    let existing: Option<String> = match rows.next().await? {
        Some(row) => row.get(0)?,
        None => return Err(AppError::NotFound(format!("epic {epic_id} not found"))),
    };
    let combined = match existing {
        Some(existing) if !existing.trim().is_empty() => format!("{}\n{line}", existing.trim()),
        _ => line.to_string(),
    };
    update_prose(
        conn,
        epic_id,
        UpdateMapProseBody {
            out_of_scope: Some(combined),
            ..Default::default()
        },
    )
    .await
}

// ---- REST handlers ---------------------------------------------------------

/// `POST /epics/{id}/map-nodes` — create a map node (`201` with the node).
/// `400` on an unknown `kind`, a blank `title`, or a `task_mode` mismatch
/// (required for `kind = "task"`, rejected for every other kind); `404` if
/// the epic does not exist or a dependency id names a node outside the epic.
/// Publishes `map_updated` on `epic:<id>`.
pub async fn create_map_node(
    State(state): State<AppState>,
    Path(id): Path<String>,
    actor: Actor,
    Json(req): Json<CreateMapNodeBody>,
) -> AppResult<(StatusCode, Json<MapNode>)> {
    let conn = state.db.conn();
    if !epic_exists(conn, &id).await? {
        return Err(AppError::NotFound(format!("epic {id} not found")));
    }
    let kind = req
        .kind
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .ok_or_else(|| AppError::BadRequest("`kind` is required".to_string()))?;
    validate_kind(kind)?;
    let title = req
        .title
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .ok_or_else(|| {
            AppError::BadRequest("`title` is required and must not be empty".to_string())
        })?;

    // `task_mode` is fixed at creation and belongs to task nodes alone
    // (plan §4.1): required for `kind = "task"`, rejected everywhere else.
    let task_mode = validate_task_mode(kind, req.task_mode.as_deref())?;

    let question = req.question.as_deref().map(str::trim).filter(|s| !s.is_empty());

    let node = create_node(
        conn,
        &id,
        kind,
        task_mode.as_deref(),
        title,
        question,
        actor.user_id.as_deref(),
        req.position_x,
        req.position_y,
    )
    .await?;

    // Wire the create-time edges. Each listed id must be an existing node of
    // this epic; the freshly-inserted node has no edges of its own yet, so no
    // cycle is possible — but the endpoint guards still apply (they
    // propagate 400/404).
    //
    // `blocked_by` is the graduation shape: each listed node blocks the new
    // node, so a resolving node can create its next frontier layer in one
    // call. `blocks` mirrors the task DAG's `--blocks` convention: the new
    // node blocks each listed id.
    for blocker_id in &req.blocked_by {
        link_epic_guard(conn, &id, blocker_id).await?;
        link_nodes(conn, blocker_id, &node.id).await?;
    }
    for blocked_id in &req.blocks {
        link_epic_guard(conn, &id, blocked_id).await?;
        link_nodes(conn, &node.id, blocked_id).await?;
    }

    publish_map(&state, &id).await;
    Ok((StatusCode::CREATED, Json(node)))
}

/// `GET /epics/{id}/map` — the epic's full map with computed per-node
/// readiness. `404` if the epic does not exist.
pub async fn get_map(State(state): State<AppState>, Path(id): Path<String>) -> AppResult<Json<Map>> {
    let map = compute_map(state.db.conn(), &id).await?;
    Ok(Json(map))
}

/// `GET /epics/{id}/map-nodes/{nodeId}` — one node with its computed
/// readiness. `404` if the epic or node does not exist, or the node belongs
/// to a different epic.
pub async fn get_map_node(
    State(state): State<AppState>,
    Path((id, node_id)): Path<(String, String)>,
) -> AppResult<Json<MapNodeView>> {
    let conn = state.db.conn();
    let node = fetch_node(conn, &node_id)
        .await?
        .filter(|node| node.epic_id == id)
        .ok_or_else(|| AppError::NotFound(format!("map node {node_id} not found")))?;

    let nodes = list_nodes_for_epic(conn, &id).await?;
    let edges = list_edges_for_epic(conn, &id).await?;
    let (frontier, blocked_by) = readiness_index(&nodes, &edges)
        .get(&node.id)
        .cloned()
        .unwrap_or((false, Vec::new()));
    Ok(Json(MapNodeView {
        node,
        frontier,
        blocked_by,
    }))
}

/// `PATCH /epics/{id}/map-nodes/{nodeId}` — partially update a node
/// (`200` with the node). See [`update_node`] for field semantics. Publishes
/// `map_updated` on `epic:<id>`.
pub async fn patch_map_node(
    State(state): State<AppState>,
    Path((id, node_id)): Path<(String, String)>,
    actor: Actor,
    Json(req): Json<UpdateMapNodeBody>,
) -> AppResult<Json<MapNode>> {
    let conn = state.db.conn();
    if !node_belongs_to_epic(conn, &node_id, &id).await? {
        return Err(AppError::NotFound(format!(
            "map node {node_id} not found"
        )));
    }
    let node = update_node(conn, &node_id, req, actor.user_id.as_deref()).await?;
    publish_map(&state, &id).await;
    Ok(Json(node))
}

/// `POST /epics/{id}/map-node-dependencies` — wire `blocker → blocked` (the
/// blocker must settle first). `201` with the edge; `400` self-link or
/// cross-epic node; `404` unknown epic/node; `409` on a cycle. Publishes
/// `map_updated` on `epic:<id>`.
pub async fn link_map_nodes(
    State(state): State<AppState>,
    Path(id): Path<String>,
    Json(req): Json<LinkMapNodesBody>,
) -> AppResult<(StatusCode, Json<MapEdge>)> {
    let conn = state.db.conn();
    if !epic_exists(conn, &id).await? {
        return Err(AppError::NotFound(format!("epic {id} not found")));
    }
    let blocker_id = req
        .blocker_id
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .ok_or_else(|| AppError::BadRequest("`blocker_id` is required".to_string()))?;
    let blocked_id = req
        .blocked_id
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .ok_or_else(|| AppError::BadRequest("`blocked_id` is required".to_string()))?;

    // Both endpoints must belong to the path epic (not merely the same epic).
    for nid in [blocker_id, blocked_id] {
        link_epic_guard(conn, &id, nid).await?;
    }

    link_nodes(conn, blocker_id, blocked_id).await?; // 400 self/cross, 409 cycle
    publish_map(&state, &id).await;
    Ok((
        StatusCode::CREATED,
        Json(MapEdge {
            blocker_id: blocker_id.to_string(),
            blocked_id: blocked_id.to_string(),
        }),
    ))
}

/// `PATCH /epics/{id}/map` — set any of the four wayfinder prose fields (the
/// `map set-destination|set-notes|set-fog|set-out-of-scope` CLI verbs' REST
/// surface). `200` with the updated full map; `400` on a blank `destination`;
/// `404` if the epic does not exist. Publishes `map_updated` on `epic:<id>`
/// — the prose is part of the map view, so the same frame re-renders it.
pub async fn patch_map_prose(
    State(state): State<AppState>,
    Path(id): Path<String>,
    Json(req): Json<UpdateMapProseBody>,
) -> AppResult<Json<Map>> {
    let conn = state.db.conn();
    update_prose(conn, &id, req).await?;
    let map = compute_map(conn, &id).await?;
    publish_map(&state, &id).await;
    Ok(Json(map))
}

// ---- helpers ---------------------------------------------------------------

/// Shared same-epic guard for a node id addressed via an epic path.
async fn link_epic_guard(conn: &Connection, epic_id: &str, node_id: &str) -> AppResult<()> {
    if !node_belongs_to_epic(conn, node_id, epic_id).await? {
        return Err(AppError::BadRequest(format!(
            "map node {node_id} is not part of epic {epic_id}"
        )));
    }
    Ok(())
}

/// Build the computed map and publish it as a `map_updated` frame on
/// `epic:<id>`, so every subscribed client re-renders with correct
/// frontier/blocked state. Best-effort: a read error is logged and the
/// publish is skipped (the DB write already committed).
pub async fn publish_map(state: &AppState, epic_id: &str) {
    match compute_map(state.db.conn(), epic_id).await {
        Ok(map) => {
            let payload = serde_json::to_value(&map).unwrap_or(serde_json::Value::Null);
            state
                .hub
                .publish(&format!("epic:{epic_id}"), "map_updated", payload);
        }
        Err(err) => {
            tracing::warn!(epic = %epic_id, error = %err, "map publish: failed to load map");
        }
    }
}

/// Whether an epic exists (lightweight existence check for route guards).
pub(crate) async fn epic_exists(conn: &Connection, epic_id: &str) -> AppResult<bool> {
    let mut rows = conn
        .query("SELECT 1 FROM epic WHERE id = ?1", params![epic_id])
        .await?;
    Ok(rows.next().await?.is_some())
}

/// Require a present, non-empty (after trim) string field, or `400 bad_request`.
fn require_non_empty<'a>(value: &'a str, field: &str) -> AppResult<&'a str> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Err(AppError::BadRequest(format!(
            "`{field}` must not be empty"
        )));
    }
    Ok(trimmed)
}

/// Trim a prose value, storing an empty string as `NULL` (matches
/// `epics::create_epic`'s handling of optional prose).
fn trimmed_or_null(value: &str) -> Value {
    match value.trim() {
        "" => Value::Null,
        trimmed => Value::Text(trimmed.to_string()),
    }
}

fn row_to_node(row: &libsql::Row) -> Result<MapNode, libsql::Error> {
    Ok(MapNode {
        id: row.get(0)?,
        epic_id: row.get(1)?,
        kind: row.get(2)?,
        task_mode: row.get(3)?,
        state: row.get(4)?,
        title: row.get(5)?,
        question: row.get(6)?,
        gist: row.get(7)?,
        out_of_scope_reason: row.get(8)?,
        created_by: row.get(9)?,
        resolved_by: row.get(10)?,
        position_x: row.get(11)?,
        position_y: row.get(12)?,
        created_at: row.get(13)?,
        updated_at: row.get(14)?,
    })
}

/// Current unix time in milliseconds (matches the `*_at` columns).
fn now_ms() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

// ---- tests -----------------------------------------------------------------

#[cfg(test)]
mod tests {
    use crate::capability::now_ms;
    use crate::users::{self, Role};
    use crate::{app, AppState, Config, Db};
    use axum::body::Body;
    use axum::http::{header::AUTHORIZATION, Request, StatusCode};
    use serde_json::{json, Value};
    use std::path::PathBuf;
    use tower::ServiceExt; // for `oneshot`

    /// Boot state + router, so tests exercise handlers over the real router.
    async fn boot() -> (AppState, axum::Router) {
        let db = Db::connect(":memory:").await.unwrap();
        db.run_migrations().await.unwrap();
        let state = AppState::new(Config::for_test(), db);
        let router = app(state.clone());
        (state, router)
    }

    /// Insert a project + epic; return ids.
    async fn seed_epic(state: &AppState) -> (String, String) {
        let conn = state.db.conn();
        let now = now_ms();
        let project_id = ulid::Ulid::new().to_string();
        conn.execute(
            "INSERT INTO project (id, name, repo_url, clone_path, clone_status, created_at, updated_at) \
             VALUES (?1, 'P', 'https://example.com/p.git', NULL, 'ready', ?2, ?2)",
            libsql::params![project_id.clone(), now],
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

    fn get_bearer(uri: &str, token: &str) -> Request<Body> {
        Request::builder()
            .uri(uri)
            .header(AUTHORIZATION, format!("Bearer {token}"))
            .body(Body::empty())
            .unwrap()
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

    fn patch_json_bearer(uri: &str, token: &str, body: Value) -> Request<Body> {
        Request::builder()
            .method("PATCH")
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

    /// Create one node via the API and return its JSON.
    async fn create_node(
        app: &axum::Router,
        token: &str,
        epic_id: &str,
        body: Value,
    ) -> Value {
        let response = app
            .clone()
            .oneshot(post_json_bearer(
                &format!("/epics/{epic_id}/map-nodes"),
                token,
                body,
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::CREATED);
        body_json(response).await
    }

    // ---- AC: create nodes of each kind ------------------------------------

    #[tokio::test]
    async fn creates_nodes_of_every_kind_and_queries_the_map() {
        let (state, app) = boot().await;
        let user = users::testing::seed_user(&state, "planner", Role::Admin, true).await;
        let token = crate::sessions::testing::login_as(&state, &user).await;
        let (_project_id, epic_id) = seed_epic(&state).await;

        let grilling = create_node(
            &app,
            &token,
            &epic_id,
            json!({"kind": "grilling", "title": "Which store?", "question": "Pick the blob store"}),
        )
        .await;
        let research = create_node(
            &app,
            &token,
            &epic_id,
            json!({"kind": "research", "title": "Survey libsql blob support"}),
        )
        .await;
        let prototype = create_node(
            &app,
            &token,
            &epic_id,
            json!({"kind": "prototype", "title": "Spike the reader UI"}),
        )
        .await;
        let task = create_node(
            &app,
            &token,
            &epic_id,
            json!({"kind": "task", "title": "Provision the bucket", "task_mode": "afk"}),
        )
        .await;

        assert_eq!(grilling["kind"], "grilling");
        assert_eq!(grilling["state"], "open");
        assert_eq!(grilling["question"], "Pick the blob store");
        assert_eq!(grilling["created_by"], user.id.as_str());
        assert_eq!(task["kind"], "task");
        assert_eq!(task["task_mode"], "afk");
        assert_eq!(research["task_mode"], Value::Null);
        assert_eq!(prototype["kind"], "prototype");

        // Querying the map: every node is on the frontier (no edges yet).
        let map = app
            .clone()
            .oneshot(get_bearer(&format!("/epics/{epic_id}/map"), &token))
            .await
            .unwrap();
        assert_eq!(map.status(), StatusCode::OK);
        let map = body_json(map).await;
        assert_eq!(map["epic_id"], epic_id.as_str());
        let nodes = map["nodes"].as_array().unwrap();
        assert_eq!(nodes.len(), 4);
        assert!(nodes.iter().all(|n| n["frontier"] == true));
        assert!(nodes.iter().all(|n| n["blocked_by"].as_array().unwrap().is_empty()));
        assert_eq!(map["edges"].as_array().unwrap().len(), 0);
    }

    #[tokio::test]
    async fn node_create_validates_kind_title_and_task_mode() {
        let (state, app) = boot().await;
        let user = users::testing::seed_user(&state, "planner", Role::Admin, true).await;
        let token = crate::sessions::testing::login_as(&state, &user).await;
        let (project_id, epic_id) = seed_epic(&state).await;

        // Unknown kind → 400.
        let r = app
            .clone()
            .oneshot(post_json_bearer(
                &format!("/epics/{epic_id}/map-nodes"),
                &token,
                json!({"kind": "charting", "title": "T"}),
            ))
            .await
            .unwrap();
        assert_eq!(r.status(), StatusCode::BAD_REQUEST);

        // Blank title → 400; missing kind → 400.
        for body in [json!({"kind": "grilling", "title": "   "}), json!({"title": "T"})] {
            let r = app
                .clone()
                .oneshot(post_json_bearer(&format!("/epics/{epic_id}/map-nodes"), &token, body))
                .await
                .unwrap();
            assert_eq!(r.status(), StatusCode::BAD_REQUEST);
        }

        // kind=task requires a task_mode; non-task kinds reject it.
        let r = app
            .clone()
            .oneshot(post_json_bearer(
                &format!("/epics/{epic_id}/map-nodes"),
                &token,
                json!({"kind": "task", "title": "T"}),
            ))
            .await
            .unwrap();
        assert_eq!(r.status(), StatusCode::BAD_REQUEST);
        let r = app
            .clone()
            .oneshot(post_json_bearer(
                &format!("/epics/{epic_id}/map-nodes"),
                &token,
                json!({"kind": "task", "title": "T", "task_mode": "telepathy"}),
            ))
            .await
            .unwrap();
        assert_eq!(r.status(), StatusCode::BAD_REQUEST);
        let r = app
            .clone()
            .oneshot(post_json_bearer(
                &format!("/epics/{epic_id}/map-nodes"),
                &token,
                json!({"kind": "grilling", "title": "T", "task_mode": "afk"}),
            ))
            .await
            .unwrap();
        assert_eq!(r.status(), StatusCode::BAD_REQUEST);

        // Unknown epic → 404.
        let r = app
            .oneshot(post_json_bearer(
                "/epics/01JZZNOPE/map-nodes",
                &token,
                json!({"kind": "grilling", "title": "T"}),
            ))
            .await
            .unwrap();
        assert_eq!(r.status(), StatusCode::NOT_FOUND);
        let _ = project_id;
    }

    // ---- AC: link dependencies; frontier/blocked computed -----------------

    #[tokio::test]
    async fn links_edges_and_computes_frontier_and_blocked_from_dependency_resolution() {
        let (state, app) = boot().await;
        let user = users::testing::seed_user(&state, "planner", Role::Admin, true).await;
        let token = crate::sessions::testing::login_as(&state, &user).await;
        let (_project_id, epic_id) = seed_epic(&state).await;
        let nodes_path = format!("/epics/{epic_id}/map-nodes");

        // a blocks b and c; b blocks d — the graduation shape: the new
        // nodes arrive via `blocked_by` naming their parent(s).
        let a = create_node(&app, &token, &epic_id, json!({"kind": "grilling", "title": "A"})).await;
        let b = create_node(
            &app,
            &token,
            &epic_id,
            json!({"kind": "grilling", "title": "B", "blocked_by": [a["id"]]}),
        )
        .await;
        let c = create_node(
            &app,
            &token,
            &epic_id,
            json!({"kind": "grilling", "title": "C", "blocked_by": [a["id"]]}),
        )
        .await;
        let d = create_node(
            &app,
            &token,
            &epic_id,
            json!({"kind": "grilling", "title": "D", "blocked_by": [b["id"]]}),
        )
        .await;

        let map = body_json(
            app.clone()
                .oneshot(get_bearer(&format!("/epics/{epic_id}/map"), &token))
                .await
                .unwrap(),
        )
        .await;
        assert_eq!(map["edges"].as_array().unwrap().len(), 3);
        let frontier_of = |map: &Value, id: &Value| -> (Value, Value) {
            let n = map["nodes"]
                .as_array()
                .unwrap()
                .iter()
                .find(|n| n["id"] == *id)
                .unwrap();
            (n["frontier"].clone(), n["blocked_by"].clone())
        };
        assert_eq!(frontier_of(&map, &a["id"]), (json!(true), json!([])));
        assert_eq!(frontier_of(&map, &b["id"]), (json!(false), json!([a["id"]])));
        assert_eq!(frontier_of(&map, &c["id"]), (json!(false), json!([a["id"]])));
        assert_eq!(frontier_of(&map, &d["id"]), (json!(false), json!([b["id"]])));

        // Single-node read carries the same computed readiness.
        let view = body_json(
            app.clone()
                .oneshot(get_bearer(&format!("{nodes_path}/{}", b["id"].as_str().unwrap()), &token))
                .await
                .unwrap(),
        )
        .await;
        assert_eq!(view["frontier"], false);
        assert_eq!(view["blocked_by"], json!([a["id"]]));

        // Resolving a (the minimal state transition) releases b and c; d is
        // still blocked by the unresolved b.
        let r = app
            .clone()
            .oneshot(patch_json_bearer(
                &format!("{nodes_path}/{}", a["id"].as_str().unwrap()),
                &token,
                json!({"state": "resolved", "gist": "Use the evidence blob store"}),
            ))
            .await
            .unwrap();
        assert_eq!(r.status(), StatusCode::OK);
        let resolved = body_json(r).await;
        assert_eq!(resolved["state"], "resolved");
        assert_eq!(resolved["gist"], "Use the evidence blob store");
        assert_eq!(resolved["resolved_by"], user.id.as_str());

        let map = body_json(
            app.clone()
                .oneshot(get_bearer(&format!("/epics/{epic_id}/map"), &token))
                .await
                .unwrap(),
        )
        .await;
        assert_eq!(frontier_of(&map, &a["id"]), (json!(false), json!([])));
        assert_eq!(frontier_of(&map, &b["id"]), (json!(true), json!([])));
        assert_eq!(frontier_of(&map, &c["id"]), (json!(true), json!([])));
        assert_eq!(frontier_of(&map, &d["id"]), (json!(false), json!([b["id"]])));

        // in_progress is still open (worked, unsettled): b in progress stays
        // on the frontier, and d remains blocked until b settles.
        let r = app
            .clone()
            .oneshot(patch_json_bearer(
                &format!("{nodes_path}/{}", b["id"].as_str().unwrap()),
                &token,
                json!({"state": "in_progress"}),
            ))
            .await
            .unwrap();
        assert_eq!(body_json(r).await["state"], "in_progress");
        let map = body_json(
            app.clone()
                .oneshot(get_bearer(&format!("/epics/{epic_id}/map"), &token))
                .await
                .unwrap(),
        )
        .await;
        assert_eq!(frontier_of(&map, &b["id"]).0, json!(true));
        assert_eq!(frontier_of(&map, &d["id"]).0, json!(false));

        // out_of_scope settles too: ruling b out of scope releases d.
        let r = app
            .clone()
            .oneshot(patch_json_bearer(
                &format!("{nodes_path}/{}", b["id"].as_str().unwrap()),
                &token,
                json!({"state": "out_of_scope", "out_of_scope_reason": "Solved by the destination"}),
            ))
            .await
            .unwrap();
        assert_eq!(body_json(r).await["out_of_scope_reason"], "Solved by the destination");
        let map = body_json(
            app.clone()
                .oneshot(get_bearer(&format!("/epics/{epic_id}/map"), &token))
                .await
                .unwrap(),
        )
        .await;
        assert_eq!(frontier_of(&map, &d["id"]), (json!(true), json!([])));
        let _ = c;
    }

    // ---- AC: completion eligibility (the way is clear) --------------------

    #[tokio::test]
    async fn completion_is_eligible_only_when_no_open_nodes_and_no_fog_remain() {
        let (state, app) = boot().await;
        let user = users::testing::seed_user(&state, "planner", Role::Admin, true).await;
        let token = crate::sessions::testing::login_as(&state, &user).await;
        let (_project_id, epic_id) = seed_epic(&state).await;
        let nodes_path = format!("/epics/{epic_id}/map-nodes");

        let a = create_node(&app, &token, &epic_id, json!({"kind": "grilling", "title": "A"})).await;
        let b = create_node(&app, &token, &epic_id, json!({"kind": "task", "title": "B", "task_mode": "hitl", "blocked_by": [a["id"]]})).await;

        // Open nodes → not eligible (every node kind counts, incl. task).
        let map = body_json(
            app.clone()
                .oneshot(get_bearer(&format!("/epics/{epic_id}/map"), &token))
                .await
                .unwrap(),
        )
        .await;
        assert_eq!(map["completion"]["eligible"], false);
        assert_eq!(map["completion"]["open_nodes"], 2);
        assert_eq!(map["completion"]["fog_remaining"], false);

        // Fog blocks completion even with no open nodes.
        app.clone()
            .oneshot(patch_json_bearer(
                &format!("/epics/{epic_id}/map"),
                &token,
                json!({"not_yet_specified": "  Retention policy undecided  "}),
            ))
            .await
            .unwrap();
        let map = body_json(
            app.clone()
                .oneshot(get_bearer(&format!("/epics/{epic_id}/map"), &token))
                .await
                .unwrap(),
        )
        .await;
        assert_eq!(map["completion"]["fog_remaining"], true);

        // Settle both nodes (out_of_scope settles like resolved); fog still blocks.
        for node in [&a, &b] {
            let r = app
                .clone()
                .oneshot(patch_json_bearer(
                    &format!("{nodes_path}/{}", node["id"].as_str().unwrap()),
                    &token,
                    json!({"state": "resolved"}),
                ))
                .await
                .unwrap();
            assert_eq!(r.status(), StatusCode::OK);
        }
        let map = body_json(
            app.clone()
                .oneshot(get_bearer(&format!("/epics/{epic_id}/map"), &token))
                .await
                .unwrap(),
        )
        .await;
        assert_eq!(map["completion"]["eligible"], false, "fog still blocks");

        // Clearing the fog (blank prose counts as empty) makes the way clear.
        app.clone()
            .oneshot(patch_json_bearer(
                &format!("/epics/{epic_id}/map"),
                &token,
                json!({"not_yet_specified": "   "}),
            ))
            .await
            .unwrap();
        let map = body_json(
            app.oneshot(get_bearer(&format!("/epics/{epic_id}/map"), &token))
                .await
                .unwrap(),
        )
        .await;
        assert_eq!(map["completion"]["eligible"], true);
        assert_eq!(map["completion"]["open_nodes"], 0);
        assert_eq!(map["completion"]["fog_remaining"], false);
    }

    #[tokio::test]
    async fn link_validates_and_rejects_cycles() {
        let (state, app) = boot().await;
        let user = users::testing::seed_user(&state, "planner", Role::Admin, true).await;
        let token = crate::sessions::testing::login_as(&state, &user).await;
        let (_other_project, other_epic) = seed_epic(&state).await;
        let (_project_id, epic_id) = seed_epic(&state).await;
        let deps_path = format!("/epics/{epic_id}/map-node-dependencies");

        let a = create_node(&app, &token, &epic_id, json!({"kind": "grilling", "title": "A"})).await;
        let b = create_node(&app, &token, &epic_id, json!({"kind": "grilling", "title": "B"})).await;
        let c = create_node(&app, &token, &epic_id, json!({"kind": "grilling", "title": "C"})).await;
        let (a_id, b_id, c_id) = (
            a["id"].as_str().unwrap().to_string(),
            b["id"].as_str().unwrap().to_string(),
            c["id"].as_str().unwrap().to_string(),
        );

        // Self-link → 400.
        let r = app
            .clone()
            .oneshot(post_json_bearer(&deps_path, &token, json!({"blocker_id": a_id, "blocked_id": a_id})))
            .await
            .unwrap();
        assert_eq!(r.status(), StatusCode::BAD_REQUEST);

        // A node of another epic → 400 (same-epic, not merely well-formed).
        let foreign = create_node(&app, &token, &other_epic, json!({"kind": "grilling", "title": "F"})).await;
        let r = app
            .clone()
            .oneshot(post_json_bearer(
                &deps_path,
                &token,
                json!({"blocker_id": a_id, "blocked_id": foreign["id"]}),
            ))
            .await
            .unwrap();
        assert_eq!(r.status(), StatusCode::BAD_REQUEST);
        assert!(
            body_json(r).await["error"]["message"]
                .as_str()
                .unwrap()
                .contains("not part of epic"),
            "cross-epic link names the offending node"
        );

        // Unknown node → 400 (the "not part of epic" guard covers it).
        let r = app
            .clone()
            .oneshot(post_json_bearer(
                &deps_path,
                &token,
                json!({"blocker_id": a_id, "blocked_id": "01JZZZNOPE"}),
            ))
            .await
            .unwrap();
        assert_eq!(r.status(), StatusCode::BAD_REQUEST);

        // a → b is fine; b → a closes a two-cycle → 409.
        let r = app
            .clone()
            .oneshot(post_json_bearer(&deps_path, &token, json!({"blocker_id": a_id, "blocked_id": b_id})))
            .await
            .unwrap();
        assert_eq!(r.status(), StatusCode::CREATED);
        let r = app
            .clone()
            .oneshot(post_json_bearer(&deps_path, &token, json!({"blocker_id": b_id, "blocked_id": a_id})))
            .await
            .unwrap();
        assert_eq!(r.status(), StatusCode::CONFLICT);

        // Longer cycle: a → b → c then c → a → 409.
        let r = app
            .clone()
            .oneshot(post_json_bearer(&deps_path, &token, json!({"blocker_id": b_id, "blocked_id": c_id})))
            .await
            .unwrap();
        assert_eq!(r.status(), StatusCode::CREATED);
        let r = app
            .oneshot(post_json_bearer(&deps_path, &token, json!({"blocker_id": c_id, "blocked_id": a_id})))
            .await
            .unwrap();
        assert_eq!(r.status(), StatusCode::CONFLICT);
        assert!(
            body_json(r).await["error"]["message"]
                .as_str()
                .unwrap()
                .contains("cycle"),
            "cycle rejection names the problem"
        );
    }

    #[tokio::test]
    async fn patching_a_node_validates_state_and_cannot_leak_across_epics() {
        let (state, app) = boot().await;
        let user = users::testing::seed_user(&state, "planner", Role::Admin, true).await;
        let token = crate::sessions::testing::login_as(&state, &user).await;
        let (_other_project, other_epic) = seed_epic(&state).await;
        let (_project_id, epic_id) = seed_epic(&state).await;
        let node = create_node(&app, &token, &epic_id, json!({"kind": "grilling", "title": "A"})).await;
        let node_path = format!("/epics/{epic_id}/map-nodes/{}", node["id"].as_str().unwrap());

        // Invalid state vocabulary → 400.
        let r = app
            .clone()
            .oneshot(patch_json_bearer(&node_path, &token, json!({"state": "finished"})))
            .await
            .unwrap();
        assert_eq!(r.status(), StatusCode::BAD_REQUEST);

        // Blank title → 400.
        let r = app
            .clone()
            .oneshot(patch_json_bearer(&node_path, &token, json!({"title": "  "})))
            .await
            .unwrap();
        assert_eq!(r.status(), StatusCode::BAD_REQUEST);

        // The same node id under the other epic's path → 404 (no leak).
        let r = app
            .oneshot(patch_json_bearer(
                &format!("/epics/{other_epic}/map-nodes/{}", node["id"].as_str().unwrap()),
                &token,
                json!({"state": "resolved"}),
            ))
            .await
            .unwrap();
        assert_eq!(r.status(), StatusCode::NOT_FOUND);
    }

    // ---- AC: set the four prose fields ------------------------------------

    #[tokio::test]
    async fn map_prose_surface_sets_all_four_fields() {
        let (state, app) = boot().await;
        let user = users::testing::seed_user(&state, "planner", Role::Admin, true).await;
        let token = crate::sessions::testing::login_as(&state, &user).await;
        let (_project_id, epic_id) = seed_epic(&state).await;
        let map_path = format!("/epics/{epic_id}/map");

        // Set all four fields one at a time (the CLI verbs' one-field-per-call shape).
        let r = app
            .clone()
            .oneshot(patch_json_bearer(&map_path, &token, json!({"destination": "An exporter that works end to end"})))
            .await
            .unwrap();
        assert_eq!(r.status(), StatusCode::OK);
        assert_eq!(body_json(r).await["destination"], "An exporter that works end to end");

        let r = app
            .clone()
            .oneshot(patch_json_bearer(&map_path, &token, json!({"notes": "Executor stays untouched"})))
            .await
            .unwrap();
        assert_eq!(body_json(r).await["notes"], "Executor stays untouched");

        let r = app
            .clone()
            .oneshot(patch_json_bearer(&map_path, &token, json!({"not_yet_specified": "Which events export; retention window"})))
            .await
            .unwrap();
        assert_eq!(
            body_json(r).await["not_yet_specified"],
            "Which events export; retention window"
        );

        let r = app
            .clone()
            .oneshot(patch_json_bearer(&map_path, &token, json!({"out_of_scope": "Multi-region replication"})))
            .await
            .unwrap();
        assert_eq!(body_json(r).await["out_of_scope"], "Multi-region replication");

        // The map query carries all four (the client's single re-render unit).
        let map = body_json(
            app.clone()
                .oneshot(get_bearer(&map_path, &token))
                .await
                .unwrap(),
        )
        .await;
        assert_eq!(map["destination"], "An exporter that works end to end");
        assert_eq!(map["notes"], "Executor stays untouched");
        assert_eq!(map["not_yet_specified"], "Which events export; retention window");
        assert_eq!(map["out_of_scope"], "Multi-region replication");

        // Clearing fog with an empty string stores NULL; blank destination → 400.
        let r = app
            .clone()
            .oneshot(patch_json_bearer(&map_path, &token, json!({"not_yet_specified": "  "})))
            .await
            .unwrap();
        assert_eq!(body_json(r).await["not_yet_specified"], Value::Null);
        let r = app
            .clone()
            .oneshot(patch_json_bearer(&map_path, &token, json!({"destination": "   "})))
            .await
            .unwrap();
        assert_eq!(r.status(), StatusCode::BAD_REQUEST);

        // Unknown epic → 404.
        let r = app
            .oneshot(patch_json_bearer("/epics/01JZZNOPE/map", &token, json!({"notes": "x"})))
            .await
            .unwrap();
        assert_eq!(r.status(), StatusCode::NOT_FOUND);
    }

    // ---- AC: map_updated events stream ------------------------------------

    /// The hub is the WS transport's source (tests/ws.rs covers the socket
    /// handshake/subscription path itself); a subscriber to `epic:<id>`
    /// must receive a `map_updated` frame on every map mutation, with the
    /// full computed map as its payload.
    #[tokio::test]
    async fn every_map_mutation_publishes_a_map_updated_frame_on_the_epic_topic() {
        let (state, app) = boot().await;
        let user = users::testing::seed_user(&state, "planner", Role::Admin, true).await;
        let token = crate::sessions::testing::login_as(&state, &user).await;
        let (_project_id, epic_id) = seed_epic(&state).await;
        let topic = format!("epic:{epic_id}");

        // Subscribe before mutating so nothing can race past us.
        let mut subscriber = state.hub.subscribe(&topic);

        let node = create_node(
            &app,
            &token,
            &epic_id,
            json!({"kind": "grilling", "title": "A", "blocked_by": [], "blocks": []}),
        )
        .await;

        let frame: Value = serde_json::from_str(
            &tokio::time::timeout(std::time::Duration::from_secs(5), subscriber.recv())
                .await
                .expect("timed out waiting for map_updated")
                .expect("hub channel closed"),
        )
        .unwrap();
        assert_eq!(frame["topic"], topic.as_str());
        assert_eq!(frame["type"], "map_updated");
        assert_eq!(frame["payload"]["epic_id"], epic_id.as_str());
        assert_eq!(frame["payload"]["nodes"].as_array().unwrap().len(), 1);
        assert_eq!(frame["payload"]["nodes"][0]["frontier"], true);

        // A dependency link also publishes.
        let b = create_node(&app, &token, &epic_id, json!({"kind": "grilling", "title": "B"})).await;
        let _ = tokio::time::timeout(std::time::Duration::from_secs(5), subscriber.recv())
            .await
            .unwrap()
            .unwrap(); // the create-B frame
        app.clone()
            .oneshot(post_json_bearer(
                &format!("/epics/{epic_id}/map-node-dependencies"),
                &token,
                json!({"blocker_id": b["id"], "blocked_id": node["id"]}),
            ))
            .await
            .unwrap();
        let frame: Value = serde_json::from_str(
            &tokio::time::timeout(std::time::Duration::from_secs(5), subscriber.recv())
                .await
                .unwrap()
                .unwrap(),
        )
        .unwrap();
        assert_eq!(frame["type"], "map_updated");
        assert_eq!(frame["payload"]["edges"].as_array().unwrap().len(), 1);

        // And so does a prose edit.
        app.clone()
            .oneshot(patch_json_bearer(
                &format!("/epics/{epic_id}/map"),
                &token,
                json!({"not_yet_specified": "Which events export"}),
            ))
            .await
            .unwrap();
        let frame: Value = serde_json::from_str(
            &tokio::time::timeout(std::time::Duration::from_secs(5), subscriber.recv())
                .await
                .unwrap()
                .unwrap(),
        )
        .unwrap();
        assert_eq!(frame["type"], "map_updated");
        assert_eq!(
            frame["payload"]["not_yet_specified"],
            "Which events export"
        );
    }

    // ---- capability-token surface (the CLI's bearer) -----------------------

    #[tokio::test]
    async fn a_capability_token_drives_the_map_surface_on_its_own_epic_only() {
        let (state, app) = boot().await;
        let (project_id, epic_id) = seed_epic(&state).await;
        let (_other_project, other_epic) = seed_epic(&state).await;
        let guard = state.caps.mint(
            epic_id.clone(),
            project_id.clone(),
            "grilling".into(),
            PathBuf::from("/tmp"),
        );
        let token = guard.token().to_string();

        // Create a node: agent attribution records no human.
        let r = app
            .clone()
            .oneshot(post_json_bearer(
                &format!("/epics/{epic_id}/map-nodes"),
                &token,
                json!({"kind": "grilling", "title": "From the agent"}),
            ))
            .await
            .unwrap();
        assert_eq!(r.status(), StatusCode::CREATED);
        let node = body_json(r).await;
        assert_eq!(node["created_by"], Value::Null);

        // Link + resolve + prose on its own epic.
        let b = body_json(
            app.clone()
                .oneshot(post_json_bearer(
                    &format!("/epics/{epic_id}/map-nodes"),
                    &token,
                    json!({"kind": "grilling", "title": "B"}),
                ))
                .await
                .unwrap(),
        )
        .await;
        let r = app
            .clone()
            .oneshot(post_json_bearer(
                &format!("/epics/{epic_id}/map-node-dependencies"),
                &token,
                json!({"blocker_id": node["id"], "blocked_id": b["id"]}),
            ))
            .await
            .unwrap();
        assert_eq!(r.status(), StatusCode::CREATED);
        let r = app
            .clone()
            .oneshot(patch_json_bearer(
                &format!("/epics/{epic_id}/map-nodes/{}", node["id"].as_str().unwrap()),
                &token,
                json!({"state": "resolved"}),
            ))
            .await
            .unwrap();
        assert_eq!(r.status(), StatusCode::OK);
        assert_eq!(body_json(r).await["resolved_by"], Value::Null);
        let r = app
            .clone()
            .oneshot(patch_json_bearer(
                &format!("/epics/{epic_id}/map"),
                &token,
                json!({"notes": "set by the agent"}),
            ))
            .await
            .unwrap();
        assert_eq!(r.status(), StatusCode::OK);

        // The other epic is untouched and unreachable.
        let r = app
            .clone()
            .oneshot(post_json_bearer(
                &format!("/epics/{other_epic}/map-nodes"),
                &token,
                json!({"kind": "grilling", "title": "hostile"}),
            ))
            .await
            .unwrap();
        assert_eq!(r.status(), StatusCode::FORBIDDEN);
        let r = app
            .clone()
            .oneshot(patch_json_bearer(
                &format!("/epics/{other_epic}/map"),
                &token,
                json!({"notes": "hostile"}),
            ))
            .await
            .unwrap();
        assert_eq!(r.status(), StatusCode::FORBIDDEN);
        let map = body_json(
            app.oneshot(get_bearer(&format!("/epics/{other_epic}/map"), &token))
                .await
                .unwrap(),
        )
        .await;
        // 403 envelope, not a map — but assert via the error shape explicitly.
        assert_eq!(map["error"]["code"], "forbidden");
    }

    #[tokio::test]
    async fn a_session_token_that_is_not_on_the_map_is_unauthenticated() {
        let (state, app) = boot().await;
        let (_project_id, epic_id) = seed_epic(&state).await;
        let r = app
            .oneshot(post_json_bearer(
                &format!("/epics/{epic_id}/map-nodes"),
                "not-a-token",
                json!({"kind": "grilling", "title": "T"}),
            ))
            .await
            .unwrap();
        assert_eq!(r.status(), StatusCode::UNAUTHORIZED);
    }
}
