// Pure WS-event → view-state reducer for the Ready-lane DAG editor (T-303).
//
// Framework-free and dependency-free (no Vue, no fetch) so it can be unit-tested
// without a browser — mirrors `planning/stream.ts`. It folds the ordered stream
// of WebSocket frames published on `epic:<id>` into the editor's view model:
// the task nodes (with readiness), the dependency edges, and the live Epic
// record. The server publishes a `dag_updated` frame (payload = the full
// `{ nodes, edges }` DAG) on every task/dependency mutation and an
// `epic_updated` frame (payload = the epic) on the breakdown `Planning → Ready`
// transition; this reducer simply replaces the corresponding slice.
//
// The state is mutated in place (and returned for convenience). Callers wrap it
// in Vue reactivity; the reducer never touches a framework.

import type { Epic } from "../api/epics";
import type { Dag, DagNode, Dependency } from "../api/tasks";

/** A WS frame as delivered on `epic:<id>` (same envelope as the planning stream). */
export interface DagFrame {
  topic: string;
  type: string;
  payload: unknown;
}

/** The DAG editor view model. */
export interface DagState {
  /** The epic this DAG belongs to (live-updated by `epic_updated` frames). */
  epic: Epic | null;
  /** The epic id this state is bound to (set on hydrate; unchanged after). */
  epicId: string | null;
  /** Task nodes with computed readiness (live-replaced by `dag_updated`). */
  nodes: DagNode[];
  /** Dependency edges (live-replaced by `dag_updated`). */
  edges: Dependency[];
}

/** A fresh, empty view model. */
export function initialDagState(): DagState {
  return { epic: null, epicId: null, nodes: [], edges: [] };
}

/**
 * Hydrate the state from a REST load: the current Epic record and its DAG
 * (`GET /epics/:id/dag`). Replaces any prior nodes/edges. Stamps `epicId` so
 * the editor knows which epic it is bound to even before an `epic_updated`
 * frame arrives.
 */
export function hydrateDag(state: DagState, epic: Epic, dag: Dag): DagState {
  state.epic = epic;
  state.epicId = epic.id;
  state.nodes = dag.nodes;
  state.edges = dag.edges;
  return state;
}

/**
 * Fold one WS frame into the state. `dag_updated` replaces nodes + edges from
 * the payload; `epic_updated` replaces the epic. Other frame types (planning
 * `RunEvent` relays, `subscribed`, …) are ignored — the DAG editor only cares
 * about DAG/epic mutations. Returns the state for convenience.
 */
export function applyDagFrame(state: DagState, frame: DagFrame): DagState {
  if (frame.type === "dag_updated") {
    const dag = frame.payload as Partial<Dag> | null;
    if (dag && Array.isArray(dag.nodes) && Array.isArray(dag.edges)) {
      // Defensively normalize nodes: the server publishes `DagNode`s (with
      // `ready`/`blocked_by`), but the WS is a trust boundary, so a node missing
      // those fields degrades to a safe default rather than crashing the render.
      state.nodes = dag.nodes.map((n) => ({
        ...n,
        ready: n.ready ?? false,
        blocked_by: Array.isArray(n.blocked_by) ? n.blocked_by : [],
      }));
      state.edges = dag.edges;
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
 * The ids of the tasks that block `taskId` (have an edge into it), derived from
 * the current edges. Useful for rendering a task's upstream dependencies.
 */
export function blockersOf(state: DagState, taskId: string): string[] {
  return state.edges.filter((e) => e.blocked_id === taskId).map((e) => e.blocker_id);
}

/**
 * The ids of the tasks that `taskId` blocks (edges out of it). Useful for
 * rendering a task's downstream dependents.
 */
export function blocksOf(state: DagState, taskId: string): string[] {
  return state.edges.filter((e) => e.blocker_id === taskId).map((e) => e.blocked_id);
}

/** Look up a node by id in the current state. */
export function nodeById(state: DagState, taskId: string): DagNode | undefined {
  return state.nodes.find((n) => n.id === taskId);
}
