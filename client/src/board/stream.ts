// Pure WS-event → view-state reducer for the project kanban (T-401).
//
// Framework-free and dependency-free (no Vue, no fetch) so it can be unit-tested
// without a browser — mirrors `dag/stream.ts`. It folds the ordered stream of
// WebSocket frames published on `project:<id>` into the kanban's view model:
// the project's epics and its standalone tasks. The server publishes a
// `board_updated` frame (payload = the full `{ epics, tasks }` board) on every
// epic lane transition and on breakdown's `Planning → Ready`; this reducer
// simply replaces both slices.
//
// The state is mutated in place (and returned for convenience). Callers wrap it
// in Vue reactivity; the reducer never touches a framework.

import type { Epic } from "../api/epics";
import type { Task } from "../api/tasks";
import type { Board } from "../api/board";

/** A WS frame as delivered on `project:<id>` (same envelope as the planning/DAG streams). */
export interface BoardFrame {
  topic: string;
  type: string;
  payload: unknown;
}

/** The project kanban view model. */
export interface BoardState {
  /** The project id this board is bound to (stamped on hydrate). */
  projectId: string | null;
  /** The project's epics, live-replaced by `board_updated`. */
  epics: Epic[];
  /** The project's standalone (parentless) tasks, live-replaced by `board_updated`. */
  tasks: Task[];
}

/** A fresh, empty view model. */
export function initialBoardState(): BoardState {
  return { projectId: null, epics: [], tasks: [] };
}

/**
 * Hydrate the state from a REST load (`GET /projects/:id/board`). Replaces any
 * prior epics/tasks. Stamps `projectId` from the first epic's `project_id` (or
 * leaves `null` when the board is empty — the caller knows the id from the
 * route).
 */
export function hydrateBoard(state: BoardState, board: Board): BoardState {
  state.epics = board.epics;
  state.tasks = board.tasks;
  state.projectId = board.epics[0]?.project_id ?? state.projectId;
  return state;
}

/**
 * Fold one WS frame into the state. `board_updated` replaces epics + tasks from
 * the payload (defensively: if the payload is missing arrays, the frame is
 * ignored). Other frame types (`clone_status`, `subscribed`, …) are ignored —
 * the kanban only cares about board mutations. Returns the state for
 * convenience.
 */
export function applyBoardFrame(state: BoardState, frame: BoardFrame): BoardState {
  if (frame.type === "board_updated") {
    const board = frame.payload as Partial<Board> | null;
    if (board && Array.isArray(board.epics) && Array.isArray(board.tasks)) {
      state.epics = board.epics;
      state.tasks = board.tasks;
    }
  }
  return state;
}
