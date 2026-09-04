// Pure WS-event → view-state reducer for the planning Map graph view.
//
// Framework-free and dependency-free (no Vue, no fetch) so it can be
// unit-tested without a browser — the same shape as `dag/stream.ts`, which
// this module deliberately mirrors: fold the ordered stream of WebSocket
// frames published on `epic:<id>` into the view model. The server publishes a
// `map_updated` frame (payload = the full computed `{ Map }` — prose, nodes
// with per-node readiness, edges, completion) on every map mutation, and an
// `epic_updated` frame (payload = the epic) on epic record changes; this
// reducer simply replaces the corresponding slice.
//
// The state is mutated in place (and returned for convenience). Callers wrap
// it in Vue reactivity; the reducer never touches a framework.

import type { Epic } from "../api/epics";
import type { Map, MapNodeView } from "../api/map";

/** A WS frame as delivered on `epic:<id>` (same envelope as the DAG stream). */
export interface MapFrame {
  topic: string;
  type: string;
  payload: unknown;
}

/** The Map graph view model. */
export interface MapState {
  /** The epic this map belongs to (live-updated by `epic_updated` frames). */
  epic: Epic | null;
  /** The epic id this state is bound to (set on hydrate; unchanged after). */
  epicId: string | null;
  /** The computed map (live-replaced by `map_updated`). */
  map: Map | null;
}

/** A fresh, empty view model. */
export function initialMapState(): MapState {
  return { epic: null, epicId: null, map: null };
}

/**
 * Hydrate the state from a REST load (`GET /epics/:id/map` + the epic).
 * Replaces any prior map. Stamps `epicId` so the view knows which epic it is
 * bound to even before an `epic_updated` frame arrives.
 */
export function hydrateMap(state: MapState, epic: Epic, map: Map): MapState {
  state.epic = epic;
  state.epicId = epic.id;
  state.map = map;
  return state;
}

/**
 * Fold one WS frame into the state. `map_updated` replaces the whole map from
 * the payload (guarded to the bound epic); `epic_updated` replaces the epic.
 * Other frame types (node-session `RunEvent` relays, `subscribed`, …) are
 * ignored — the graph view only cares about map/epic mutations. Returns the
 * state for convenience.
 */
export function applyMapFrame(state: MapState, frame: MapFrame): MapState {
  if (frame.type === "map_updated") {
    const map = frame.payload as Partial<Map> | null;
    if (
      map &&
      map.epic_id === state.epicId &&
      Array.isArray(map.nodes) &&
      Array.isArray(map.edges)
    ) {
      // Defensively normalize nodes: the server publishes `MapNodeView`s (with
      // `frontier`/`blocked_by`), but the WS is a trust boundary, so a node
      // missing those fields degrades to a safe default rather than crashing
      // the render.
      const nodes: MapNodeView[] = map.nodes.map((n) => ({
        ...n,
        frontier: (n as Partial<MapNodeView>).frontier ?? false,
        blocked_by: Array.isArray((n as Partial<MapNodeView>).blocked_by)
          ? (n as Partial<MapNodeView>).blocked_by!
          : [],
      }));
      state.map = { ...(map as Map), nodes };
    }
  } else if (frame.type === "epic_updated") {
    const epic = frame.payload as Epic | null;
    if (epic && typeof epic.id === "string") {
      state.epic = epic;
    }
  }
  return state;
}

/**
 * Visual readiness of a node, derived from its state + computed frontier flag.
 * Drives the node card's color/badge: `frontier` (open, unblocked — workable
 * now) is deliberately distinct from `blocked` (open, waiting on an unsettled
 * blocker). `in_progress` wins over `frontier` (an in-progress node is on the
 * frontier by construction, but "being worked" is the sharper signal).
 */
export type Readiness =
  | "frontier"
  | "blocked"
  | "in_progress"
  | "resolved"
  | "out_of_scope";

export function readinessOf(node: MapNodeView): Readiness {
  if (node.state === "resolved") return "resolved";
  if (node.state === "out_of_scope") return "out_of_scope";
  if (node.state === "in_progress") return "in_progress";
  return node.frontier ? "frontier" : "blocked";
}

/**
 * The ids of the nodes that block `nodeId` (have an edge into it) — prefer the
 * server's computed `blocked_by` (unsettled only); this derives ALL upstream
 * blockers from the edges, settled or not.
 */
export function blockersOf(state: MapState, nodeId: string): string[] {
  return (state.map?.edges ?? [])
    .filter((e) => e.blocked_id === nodeId)
    .map((e) => e.blocker_id);
}

/** The ids of the nodes that `nodeId` blocks (edges out of it). */
export function blocksOf(state: MapState, nodeId: string): string[] {
  return (state.map?.edges ?? [])
    .filter((e) => e.blocker_id === nodeId)
    .map((e) => e.blocked_id);
}

/** Look up a node view by id in the current map. */
export function nodeById(state: MapState, nodeId: string): MapNodeView | undefined {
  return state.map?.nodes.find((n) => n.id === nodeId);
}
