// Pure drag-and-drop rules for the kanban boards (project kanban + epic task
// kanban). Framework-free and dependency-free (no Vue, no DOM) so the drop
// validation can be unit-tested in isolation — mirrors `epicLanes.ts`. The
// components own the DOM events (`dragstart`/`dragover`/`drop`); this module
// answers the two questions they need: "may this card drop on that lane?" and
// "what task status does that lane mean?"

import type { EpicLane } from "../api/board";
import type { TaskStatus } from "../api/tasks";

/** The kind of kanban card being dragged. */
export type DragKind = "epic" | "task";

/**
 * Permitted epic `current → target` lane transitions. Must match the server
 * table in `deerborn-server/src/lanes.rs`: `Planning → Ready` is owned by
 * breakdown and `InProgress → Completed` by the worker, so neither is manual.
 * (Extracted from `ProjectKanbanView.vue` so the drag-and-drop rules and the
 * lane-move select share one source of truth.)
 */
export const EPIC_LANE_TRANSITIONS: Record<string, EpicLane[]> = {
  Planning: ["Cancelled"],
  Ready: ["InProgress", "Cancelled"],
  InProgress: ["Cancelled", "Blocked"],
  Blocked: ["Ready", "Cancelled"],
  Completed: [],
  Cancelled: [],
};

/** The lane keys an epic may move to from `currentStatus` (possibly empty). */
export function permittedEpicTargets(currentStatus: string): EpicLane[] {
  return EPIC_LANE_TRANSITIONS[currentStatus] ?? [];
}

/**
 * The task status a project-board lane represents — the inverse of the view's
 * `taskLane()` mapping (Ready→Todo, InProgress→InProgress, Completed→Done,
 * Blocked→Failed, Cancelled→Cancelled). `Planning` has no task-status
 * equivalent (only epics plan), so it returns `null` and rejects task drops.
 */
export function taskStatusForLane(lane: EpicLane): TaskStatus | null {
  switch (lane) {
    case "Ready":
      return "Todo";
    case "InProgress":
      return "InProgress";
    case "Completed":
      return "Done";
    case "Blocked":
      return "Failed";
    case "Cancelled":
      return "Cancelled";
    default:
      return null; // Planning
  }
}

/**
 * Whether a dragged card may drop on `targetLane` of the **project** board.
 * Epics follow the transition table; standalone tasks may drop on any lane
 * that maps to a task status (i.e. everywhere except `Planning`). Dropping a
 * card back on its own lane is allowed but a no-op the caller may skip.
 */
export function canDropOnProjectLane(
  kind: DragKind,
  currentStatus: string,
  targetLane: EpicLane,
): boolean {
  if (kind === "epic") {
    return permittedEpicTargets(currentStatus).includes(targetLane);
  }
  return taskStatusForLane(targetLane) !== null;
}

/**
 * Whether a dragged task may drop on `targetStatus` of the **epic** task
 * kanban. Any lane-to-lane move is permitted *except* dragging a task out of
 * `InProgress` — the worker owns those transitions (a running task must finish
 * or fail on its own).
 */
export function canDropOnTaskLane(currentStatus: TaskStatus, targetStatus: TaskStatus): boolean {
  if (currentStatus === targetStatus) {
    return true; // no-op drop
  }
  return currentStatus !== "InProgress";
}
