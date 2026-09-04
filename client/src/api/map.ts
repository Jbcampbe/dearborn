// Planning-map REST surface (wayfinder epic) consumed by the Map graph view.
// Mirrors `tasks.ts`/`epics.ts`: typed DTOs matching the server's shapes (see
// `dearborn-server/src/map.rs`) wrapped around the generic `apiFetch`.
//
// The live map does NOT come through here on mutations — every map mutation
// publishes a `map_updated` frame over the WebSocket (`epic:<id>`) carrying
// the full computed map, which the Map view's reducer (`src/map/stream.ts`)
// folds into its view model. This module covers only the initial REST hydrate
// (`GET /epics/{id}/map`); the WS side lives in `src/map/`.
//
// Readiness is computed server-side from the dependency graph on every read
// (never stored): `frontier` = open with every blocker settled, `blocked_by`
// = the unsettled blocker ids. Node sessions / resolution / message surfaces
// are separate endpoints (`/map-nodes/:id/...`) — the node session view's
// REST client lives in `src/api/nodes.ts`.

import { apiFetch } from "./client";

/** A node kind (plan §5). `task_mode` only ever accompanies `task`. */
export type MapNodeKind = "grilling" | "research" | "prototype" | "task";

/**
 * Node lifecycle state (plan §4.1). `open`/`in_progress` are live states;
 * `resolved`/`out_of_scope` are the settled terminal states — both unblock
 * dependents.
 */
export type MapNodeState = "open" | "in_progress" | "resolved" | "out_of_scope";

/** A map node as stored (`map.rs` `MapNode`). Readiness is not a column. */
export interface MapNode {
  id: string;
  epic_id: string;
  kind: MapNodeKind | string;
  /** For `kind = "task"` only: `afk | hitl`, fixed at creation. */
  task_mode: string | null;
  state: MapNodeState | string;
  title: string;
  /** The decision/investigation this node resolves. */
  question: string | null;
  /** One-line resolution answer (set on resolve). */
  gist: string | null;
  out_of_scope_reason: string | null;
  /** User id of the human who created/resolved the node (null = agent actor). */
  created_by: string | null;
  resolved_by: string | null;
  /** Graph layout (nullable — the client auto-lays-out unset positions). */
  position_x: number | null;
  position_y: number | null;
  created_at: number;
  updated_at: number;
}

/**
 * A map node plus its computed readiness (`map.rs` `MapNodeView` — the node
 * fields are `#[serde(flatten)]`-ed in alongside `frontier`/`blocked_by`).
 */
export interface MapNodeView extends MapNode {
  /** Open (or in-progress) with every blocker settled — workable now. */
  frontier: boolean;
  /** Blocker ids not yet settled (empty unless open and not on the frontier). */
  blocked_by: string[];
}

/** A dependency edge: `blocker_id` blocks `blocked_id` (must settle first). */
export interface MapEdge {
  blocker_id: string;
  blocked_id: string;
}

/** The epic's computed completion eligibility (plan §8). */
export interface MapCompletion {
  /** No open/in-progress nodes remain AND the fog prose is empty. */
  eligible: boolean;
  /** How many nodes are still open or in progress (0 when eligible). */
  open_nodes: number;
  /** Whether fog prose remains (`not_yet_specified` non-empty). */
  fog_remaining: boolean;
}

/**
 * The epic's full planning map (`GET /epics/{id}/map` → `map.rs` `Map`).
 * This is also the whole `map_updated` WS payload, so a single frame
 * re-renders the map view completely.
 */
export interface Map {
  epic_id: string;
  /** What the finished plan looks like — fixes scope. */
  destination: string | null;
  /** Optional freeform prose alongside the destination. */
  notes: string | null;
  /** In-scope decisions not yet sharp enough to be nodes — fog is prose. */
  not_yet_specified: string | null;
  /** Work explicitly ruled beyond the destination (the prose line). */
  out_of_scope: string | null;
  nodes: MapNodeView[];
  edges: MapEdge[];
  completion: MapCompletion;
}

/** `GET /epics/{id}/map` → the epic's computed map. 404 if the epic is gone. */
export function getMap(token: string, epicId: string): Promise<Map> {
  return apiFetch<Map>(`/epics/${encodeURIComponent(epicId)}/map`, token);
}

/**
 * `GET /epics/{id}/map-nodes/{nodeId}` → one node with its computed readiness.
 * 404 if the epic or node is unknown, or the node belongs to a different epic.
 */
export function getMapNode(
  token: string,
  epicId: string,
  nodeId: string,
): Promise<MapNodeView> {
  return apiFetch<MapNodeView>(
    `/epics/${encodeURIComponent(epicId)}/map-nodes/${encodeURIComponent(nodeId)}`,
    token,
  );
}
