// Node-session REST surface (wayfinder epic) consumed by the node session
// view (`NodeSessionView.vue`). Mirrors `map.ts`/`tasks.ts`: typed DTOs
// matching the server's shapes (see `dearborn-server/src/node_engine.rs`,
// `resolve.rs`, `activity.rs`) wrapped around the generic `apiFetch`.
//
// A grilling/prototype node owns a node-scoped session (`node_session` — the
// native harness resume handle) and a multi-party transcript (`node_message`,
// every human turn attributed via `actor_user_id`). Opening a session flips
// the node to the soft `in_progress` signal; posting a message may also start
// an agent reply (the per-node run-lock decides — `reply_started` says which).
// Live `RunEvent`s stream over the WebSocket on `node:<id>` (see
// `src/node/`); this module is REST only.
//
// Resolution (`POST …/resolve`) is the grilling resolution bundle (§6): one
// call that records the decision, optionally graduates fog into new frontier
// nodes, rules things out of scope, trims the fog prose, and updates affected
// nodes. The document fold-in is deliberately absent here — the human resolve
// affordance never carries HTML; agents edit the Document through the
// `dearborn` CLI during the session.

import { apiFetch, apiFetchText } from "./client";
import type { Map, MapNode } from "./map";

// ---- session + transcript ---------------------------------------------------

/** A node's durable resume handle (`node_session`, plan §4.3). */
export interface NodeSession {
  node_id: string;
  /** Native harness resume id; `null` until the node's first agent turn ran. */
  harness_session_id: string | null;
  /** `active` | `complete` (a resolved node's session has nothing left). */
  status: "active" | "complete" | string;
  created_at: number;
  updated_at: number;
}

/** One entry in a node's multi-party transcript (`node_message`, plan §4.4). */
export interface NodeMessage {
  id: string;
  node_id: string;
  /** `user` | `agent` | `tool` | `system`. */
  role: "user" | "agent" | "tool" | "system" | string;
  /** Which human posted it (`null` for agent/tool/system turns). */
  actor_user_id: string | null;
  content: string;
  /** Monotonic per node. */
  seq: number;
  created_at: number;
}

/**
 * The `session` endpoints' response: the resume handle plus the transcript so
 * far, so opening a node renders the whole conversation in one call.
 */
export interface NodeSessionView extends NodeSession {
  messages: NodeMessage[];
}

/**
 * `POST /epics/{id}/map-nodes/{nodeId}/session` — open (or resume) the node's
 * interactive session; flips an `open` node to the soft `in_progress` signal.
 * `409` for research/task kinds (no interactive engine — `afk_engine` owns
 * them); `404` unknown epic/node.
 */
export function openNodeSession(
  token: string,
  epicId: string,
  nodeId: string,
): Promise<NodeSessionView> {
  return apiFetch<NodeSessionView>(
    `/epics/${encodeURIComponent(epicId)}/map-nodes/${encodeURIComponent(nodeId)}/session`,
    token,
    { method: "POST" },
  );
}

/**
 * `GET /epics/{id}/map-nodes/{nodeId}/session` — the session + transcript.
 * `404` when the node has never been opened (no row exists yet).
 */
export function getNodeSession(
  token: string,
  epicId: string,
  nodeId: string,
): Promise<NodeSessionView> {
  return apiFetch<NodeSessionView>(
    `/epics/${encodeURIComponent(epicId)}/map-nodes/${encodeURIComponent(nodeId)}/session`,
    token,
  );
}

/** `POST …/messages` response: the stored turn plus whether an agent reply run started. */
export interface PostMessageResult {
  message: NodeMessage;
  /** False when a reply was already in flight — the turn is stored, no second run. */
  reply_started: boolean;
}

/**
 * `POST /epics/{id}/map-nodes/{nodeId}/messages` — post a turn into the
 * node's conversation. **Any** authenticated user may post (flat
 * permissions); the message is attributed to the caller. `202` with the
 * stored message; `400` blank content; `409` non-interactive kind.
 */
export function postNodeMessage(
  token: string,
  epicId: string,
  nodeId: string,
  content: string,
): Promise<PostMessageResult> {
  return apiFetch<PostMessageResult>(
    `/epics/${encodeURIComponent(epicId)}/map-nodes/${encodeURIComponent(nodeId)}/messages`,
    token,
    { method: "POST", body: JSON.stringify({ content }) },
  );
}

// ---- resolution (the grilling resolution bundle, plan §6) --------------------

/** One fog graduation: a new node created open and blocked by the resolving node. */
export interface GraduateInput {
  /** `grilling` | `research` | `prototype` (task nodes arrive via breakdown). */
  kind: string;
  title: string;
  question?: string;
}

/** One out-of-scope ruling: a created-and-closed node plus a one-line prose append. */
export interface OutOfScopeInput {
  title: string;
  reason: string;
}

/**
 * `POST …/resolve` body. Every part optional server-side; absent parts are
 * simply not performed. The client resolve affordance sends the decision gist
 * plus whatever map-reshaping the human recorded.
 */
export interface ResolveNodeInput {
  /** The one-line decision — stored on the node's `gist`. */
  gist?: string;
  /** New frontier nodes to graduate out of the fog, each blocked by this node. */
  graduations?: GraduateInput[];
  /** Things this decision rules beyond the destination. */
  out_of_scope?: OutOfScopeInput[];
  /** The replacement `not_yet_specified` prose (empty clears the fog). */
  trim_fog?: string;
}

/** A document version as folded in by the resolution (absent when no edit was sent). */
export interface ResolvedDocument {
  version: number;
  updated_at: number;
}

/** The resolution outcome: everything the bundle did, plus the recomputed map. */
export interface ResolveNodeResult {
  node: MapNode;
  document: ResolvedDocument | null;
  created: MapNode[];
  out_of_scope: MapNode[];
  updated: MapNode[];
  map: Map;
}

/**
 * `POST /epics/{id}/map-nodes/{nodeId}/resolve` — resolve a grilling/prototype
 * node and reshape the map in one call. `200` with the outcome; `409` for a
 * non-HITL kind (research/task never reshape the map). The recomputed map
 * fans out on `epic:<id>` as `map_updated`, so any open Map view re-renders.
 */
export function resolveNode(
  token: string,
  epicId: string,
  nodeId: string,
  input: ResolveNodeInput,
): Promise<ResolveNodeResult> {
  return apiFetch<ResolveNodeResult>(
    `/epics/${encodeURIComponent(epicId)}/map-nodes/${encodeURIComponent(nodeId)}/resolve`,
    token,
    { method: "POST", body: JSON.stringify(input) },
  );
}

// ---- prototype artifact store (plan §4.7) ------------------------------------

/**
 * One stored prototype artifact (`node_asset`, plan §4.7) — **linked, not
 * inlined**: the node session view lists these (metadata only) and fetches
 * the bytes separately to render them in a sandboxed iframe.
 */
export interface NodeAsset {
  id: string;
  node_id: string;
  mime: string;
  label: string | null;
  /** Computed server-side (`LENGTH(bytes)`), never a stored column. */
  byte_size: number;
  created_at: number;
}

/**
 * `GET /epics/{id}/map-nodes/{nodeId}/assets` — the node's stored artifacts,
 * metadata only. `404` unknown epic/node. Reads are open to every capability
 * phase, so a live session's artifact is visible the moment it is stored.
 */
export async function listNodeAssets(
  token: string,
  epicId: string,
  nodeId: string,
): Promise<NodeAsset[]> {
  const data = await apiFetch<{ items: NodeAsset[] }>(
    `/epics/${encodeURIComponent(epicId)}/map-nodes/${encodeURIComponent(nodeId)}/assets`,
    token,
  );
  return data.items;
}

/**
 * `GET /epics/{id}/map-nodes/{nodeId}/assets/{assetId}` — the artifact's raw
 * bytes (text; prototype artifacts are standalone HTML apps). Rendered in a
 * **sandboxed iframe** (`sandbox="allow-scripts"`, no `allow-same-origin`,
 * so the artifact runs on an opaque origin and cannot touch the app).
 */
export function getNodeAssetText(
  token: string,
  epicId: string,
  nodeId: string,
  assetId: string,
): Promise<string> {
  return apiFetchText(
    `/epics/${encodeURIComponent(epicId)}/map-nodes/${encodeURIComponent(nodeId)}/assets/${encodeURIComponent(assetId)}`,
    token,
  );
}

// ---- attribution ------------------------------------------------------------

/** One derived participant: a distinct human actor of the epic (activity.rs). */
export interface Participant {
  id: string;
  username: string;
  display_name: string;
}

/**
 * `GET /epics/{id}/participants` — the epic's participants, derived as the
 * distinct actors across every attribution surface (node-message posters
 * included). Agents act without a user id, so only humans appear. The node
 * session view uses it to resolve `actor_user_id`s to display names.
 */
export async function getParticipants(token: string, epicId: string): Promise<Participant[]> {
  const data = await apiFetch<{ items: Participant[] }>(
    `/epics/${encodeURIComponent(epicId)}/participants`,
    token,
  );
  return data.items;
}
