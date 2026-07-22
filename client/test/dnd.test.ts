// Unit tests for the kanban drag-and-drop rules (`src/board/dnd.ts`). The
// components own the DOM events; these pure functions decide which drops are
// legal — the epic lane transition table (mirroring the server's `lanes.rs`)
// and the worker's ownership of moves out of In Progress.

import { describe, expect, it } from "vitest";

import {
  canDropOnProjectLane,
  canDropOnTaskLane,
  permittedEpicTargets,
  taskStatusForLane,
} from "../src/board/dnd";

describe("permittedEpicTargets", () => {
  it("matches the server transition table", () => {
    expect(permittedEpicTargets("Planning")).toEqual(["Cancelled"]);
    expect(permittedEpicTargets("Ready")).toEqual(["InProgress", "Cancelled"]);
    expect(permittedEpicTargets("InProgress")).toEqual(["Cancelled", "Blocked"]);
    expect(permittedEpicTargets("Blocked")).toEqual(["Ready", "Cancelled"]);
  });

  it("treats Completed and Cancelled as terminal", () => {
    expect(permittedEpicTargets("Completed")).toEqual([]);
    expect(permittedEpicTargets("Cancelled")).toEqual([]);
  });

  it("returns no targets for an unknown status", () => {
    expect(permittedEpicTargets("Weird")).toEqual([]);
  });
});

describe("canDropOnProjectLane", () => {
  it("lets an epic drop only on its permitted transition targets", () => {
    expect(canDropOnProjectLane("epic", "Ready", "InProgress")).toBe(true);
    expect(canDropOnProjectLane("epic", "Ready", "Cancelled")).toBe(true);
    // Planning → Ready is owned by breakdown; InProgress → Completed by the worker.
    expect(canDropOnProjectLane("epic", "Planning", "Ready")).toBe(false);
    expect(canDropOnProjectLane("epic", "InProgress", "Completed")).toBe(false);
    expect(canDropOnProjectLane("epic", "Completed", "Ready")).toBe(false);
  });

  it("lets a task drop on any lane that maps to a task status", () => {
    expect(canDropOnProjectLane("task", "Todo", "InProgress")).toBe(true);
    expect(canDropOnProjectLane("task", "Todo", "Completed")).toBe(true);
    expect(canDropOnProjectLane("task", "Done", "Ready")).toBe(true);
  });

  it("rejects task drops on the Planning lane (no task status maps there)", () => {
    expect(canDropOnProjectLane("task", "Todo", "Planning")).toBe(false);
    expect(taskStatusForLane("Planning")).toBeNull();
  });
});

describe("taskStatusForLane", () => {
  it("inverts the board's taskLane mapping", () => {
    expect(taskStatusForLane("Ready")).toBe("Todo");
    expect(taskStatusForLane("InProgress")).toBe("InProgress");
    expect(taskStatusForLane("Completed")).toBe("Done");
    expect(taskStatusForLane("Blocked")).toBe("Failed");
    expect(taskStatusForLane("Cancelled")).toBe("Cancelled");
  });
});

describe("canDropOnTaskLane (epic task kanban)", () => {
  it("permits moves between ordinary lanes", () => {
    expect(canDropOnTaskLane("Todo", "InProgress")).toBe(true);
    expect(canDropOnTaskLane("Todo", "Done")).toBe(true);
    expect(canDropOnTaskLane("Failed", "Todo")).toBe(true);
    expect(canDropOnTaskLane("Done", "Todo")).toBe(true);
  });

  it("forbids dragging a task out of InProgress (the worker owns it)", () => {
    expect(canDropOnTaskLane("InProgress", "Done")).toBe(false);
    expect(canDropOnTaskLane("InProgress", "Todo")).toBe(false);
    expect(canDropOnTaskLane("InProgress", "Failed")).toBe(false);
  });

  it("allows dropping back on the same lane (a no-op the caller skips)", () => {
    expect(canDropOnTaskLane("InProgress", "InProgress")).toBe(true);
    expect(canDropOnTaskLane("Todo", "Todo")).toBe(true);
  });
});
