// Pure lane-grouping helper for the epic-detail task kanban (T-402).
//
// The task kanban is a different *view* of the same `DagState` the DAG editor
// (T-303) uses — it reuses `dag/stream.ts` (the reducer) + `dag/useDagStream.ts`
// (the WS composable) unchanged. The only board-specific logic is grouping the
// DAG's task nodes into lanes by `status`; this module is that one pure helper,
// extracted so it can be unit-tested in isolation (mirroring `dag.test.ts`).

import type { DagNode, TaskStatus } from "../api/tasks";

/**
 * Group task nodes by their `status`, keyed by every `TaskStatus` lane (always
 * present, even when empty, so a kanban can render a stable column order). The
 * node objects are **not** cloned — the caller's reactive `DagState.nodes` array
 * backs them, so readiness (`ready`/`blocked_by`) is preserved on each node and
 * the view re-renders live when the DAG stream replaces `nodes`.
 */
export function tasksByStatus(nodes: DagNode[]): Record<TaskStatus, DagNode[]> {
  const lanes: Record<TaskStatus, DagNode[]> = {
    Todo: [],
    InProgress: [],
    Done: [],
    Failed: [],
    Cancelled: [],
  };
  for (const node of nodes) {
    const lane = lanes[node.status];
    if (lane) {
      lane.push(node);
    }
  }
  return lanes;
}

/** Lane definitions for the epic task kanban: stored key → display label. */
export const TASK_LANES: { key: TaskStatus; label: string }[] = [
  { key: "Todo", label: "Todo" },
  { key: "InProgress", label: "In Progress" },
  { key: "Done", label: "Done" },
  { key: "Failed", label: "Failed" },
  { key: "Cancelled", label: "Cancelled" },
];

/**
 * A short readiness badge label for a task card, derived from its DAG node.
 * `Todo` tasks show `ready` / `blocked (N)` (N = blocker count); other statuses
 * show the status itself. This is the AC: "task statuses render correctly;
 * reflecting DAG status."
 */
export function readinessLabel(node: DagNode): string {
  if (node.status !== "Todo") return node.status;
  return node.ready ? "ready" : `blocked (${node.blocked_by.length})`;
}
