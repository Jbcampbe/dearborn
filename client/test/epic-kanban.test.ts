// Unit tests for the epic-detail task kanban lane helper (T-402). The view
// reuses the DAG stream reducer (`dag/stream.ts`, already tested in
// `dag.test.ts`) + WS composable unchanged; the only board-specific logic is
// the pure `tasksByStatus` grouping helper + `readinessLabel`, tested here.

import { describe, expect, it } from "vitest";

import type { DagNode } from "../src/api/tasks";
import { readinessLabel, tasksByStatus, TASK_LANES } from "../src/board/epicLanes";

function node(overrides: Partial<DagNode> = {}): DagNode {
  return {
    id: "T1",
    epic_id: "E1",
    project_id: "P1",
    title: "Slice",
    description: null,
    acceptance: null,
    status: "Todo",
    failure_reason: null,
    agent_session_id: null,
    position: 1,
    created_at: 1,
    updated_at: 1,
    ready: true,
    blocked_by: [],
    ...overrides,
  };
}

describe("tasksByStatus", () => {
  it("groups nodes into lanes by their status", () => {
    const lanes = tasksByStatus([
      node({ id: "A", status: "Todo" }),
      node({ id: "B", status: "InProgress" }),
      node({ id: "C", status: "Done" }),
    ]);

    expect(lanes.Todo.map((n) => n.id)).toEqual(["A"]);
    expect(lanes.InProgress.map((n) => n.id)).toEqual(["B"]);
    expect(lanes.Done.map((n) => n.id)).toEqual(["C"]);
    expect(lanes.Failed).toEqual([]);
    expect(lanes.Cancelled).toEqual([]);
  });

  it("returns all five lanes (even when empty) so the kanban has a stable column order", () => {
    const lanes = tasksByStatus([]);
    for (const lane of TASK_LANES) {
      expect(lanes[lane.key]).toEqual([]);
    }
    expect(Object.keys(lanes).sort()).toEqual(
      ["Todo", "InProgress", "Done", "Failed", "Cancelled"].sort(),
    );
  });

  it("distributes nodes across every status", () => {
    const lanes = tasksByStatus([
      node({ id: "A", status: "Todo" }),
      node({ id: "B", status: "Todo" }),
      node({ id: "C", status: "InProgress" }),
      node({ id: "D", status: "Done" }),
      node({ id: "E", status: "Failed" }),
      node({ id: "F", status: "Cancelled" }),
    ]);

    expect(lanes.Todo.map((n) => n.id)).toEqual(["A", "B"]);
    expect(lanes.InProgress.map((n) => n.id)).toEqual(["C"]);
    expect(lanes.Done.map((n) => n.id)).toEqual(["D"]);
    expect(lanes.Failed.map((n) => n.id)).toEqual(["E"]);
    expect(lanes.Cancelled.map((n) => n.id)).toEqual(["F"]);
  });

  it("preserves readiness (ready + blocked_by) on each grouped node", () => {
    const ready = node({ id: "A", status: "Todo", ready: true, blocked_by: [] });
    const blocked = node({
      id: "B",
      status: "Todo",
      ready: false,
      blocked_by: ["A"],
    });
    const lanes = tasksByStatus([ready, blocked]);

    expect(lanes.Todo[0].ready).toBe(true);
    expect(lanes.Todo[0].blocked_by).toEqual([]);
    expect(lanes.Todo[1].ready).toBe(false);
    expect(lanes.Todo[1].blocked_by).toEqual(["A"]);
  });
});

describe("readinessLabel", () => {
  it("shows 'ready' for a ready Todo task", () => {
    expect(readinessLabel(node({ status: "Todo", ready: true, blocked_by: [] }))).toBe("ready");
  });

  it("shows 'blocked (N)' for a blocked Todo task with N blockers", () => {
    expect(
      readinessLabel(node({ status: "Todo", ready: false, blocked_by: ["A", "B"] })),
    ).toBe("blocked (2)");
  });

  it("shows the status itself for non-Todo tasks", () => {
    expect(readinessLabel(node({ status: "InProgress" }))).toBe("InProgress");
    expect(readinessLabel(node({ status: "Done" }))).toBe("Done");
    expect(readinessLabel(node({ status: "Failed" }))).toBe("Failed");
    expect(readinessLabel(node({ status: "Cancelled" }))).toBe("Cancelled");
  });
});
