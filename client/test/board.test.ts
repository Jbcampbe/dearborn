// Unit tests for the pure board stream reducer (T-401). No browser, no WS —
// just fold hand-built frames and assert the resulting view model. Mirrors the
// DAG stream reducer tests.

import { describe, expect, it } from "vitest";

import type { Epic } from "../src/api/epics";
import type { Task } from "../src/api/tasks";
import type { Board, EpicProgress } from "../src/api/board";
import {
  applyBoardFrame,
  hydrateBoard,
  initialBoardState,
  type BoardFrame,
} from "../src/board/stream";

const TOPIC = "project:P1";

function frame(type: string, payload: unknown): BoardFrame {
  return { topic: TOPIC, type, payload };
}

function makeEpic(overrides: Partial<Epic> = {}): Epic {
  return {
    id: "E1",
    project_id: "P1",
    title: "Ship it",
    description: null,
    product_context: null,
    technical_context: null,
    status: "Planning",
    created_at: 1,
    updated_at: 1,
    ...overrides,
  };
}

function makeTask(overrides: Partial<Task> = {}): Task {
  return {
    id: "T1",
    epic_id: null,
    project_id: "P1",
    title: "Slice",
    description: null,
    acceptance: null,
    status: "Todo",
    failure_reason: null,
    agent_session_id: null,
    position: null,
    created_at: 1,
    updated_at: 1,
    ...overrides,
  };
}

function board(epics: Epic[], tasks: Task[], epic_progress: EpicProgress[] = []): Board {
  return { epics, tasks, epic_progress };
}

describe("Board stream reducer", () => {
  it("hydrate stamps projectId from the first epic and sets epics + tasks", () => {
    const state = initialBoardState();
    hydrateBoard(state, board(
      [makeEpic({ id: "E1", status: "Ready" })],
      [makeTask({ id: "T1", title: "Standalone" })],
    ));

    expect(state.projectId).toBe("P1");
    expect(state.epics).toHaveLength(1);
    expect(state.tasks).toHaveLength(1);
  });

  it("hydrate leaves projectId null when the board is empty", () => {
    const state = initialBoardState();
    hydrateBoard(state, board([], []));

    expect(state.projectId).toBeNull();
    expect(state.epics).toEqual([]);
    expect(state.tasks).toEqual([]);
  });

  it("hydrate and board_updated carry the epic progress counts", () => {
    const state = initialBoardState();
    const progress: EpicProgress[] = [{ epic_id: "E1", done: 2, total: 4 }];
    hydrateBoard(state, board([makeEpic({ id: "E1" })], [], progress));
    expect(state.epicProgress).toEqual(progress);

    const updated: EpicProgress[] = [{ epic_id: "E1", done: 3, total: 4 }];
    applyBoardFrame(
      state,
      frame("board_updated", board([makeEpic({ id: "E1" })], [], updated)),
    );
    expect(state.epicProgress).toEqual(updated);
  });

  it("a board_updated frame without epic_progress clears the counts", () => {
    const state = initialBoardState();
    hydrateBoard(state, board([makeEpic({ id: "E1" })], [], [{ epic_id: "E1", done: 1, total: 2 }]));

    applyBoardFrame(state, frame("board_updated", { epics: [], tasks: [] }));

    expect(state.epicProgress).toEqual([]);
  });

  it("board_updated replaces both epics and tasks", () => {
    const state = initialBoardState();
    hydrateBoard(state, board([makeEpic({ id: "E1" })], [makeTask({ id: "T1" })]));

    applyBoardFrame(state, frame("board_updated", board(
      [makeEpic({ id: "E2", status: "Ready" }), makeEpic({ id: "E3", status: "InProgress" })],
      [makeTask({ id: "T2", status: "Done" }), makeTask({ id: "T3", status: "Failed" })],
    )));

    expect(state.epics.map((e) => e.id)).toEqual(["E2", "E3"]);
    expect(state.tasks.map((t) => t.id)).toEqual(["T2", "T3"]);
  });

  it("ignores malformed board_updated payloads missing arrays", () => {
    const state = initialBoardState();
    hydrateBoard(state, board([makeEpic({ id: "E1" })], [makeTask({ id: "T1" })]));
    const before = JSON.stringify(state);

    applyBoardFrame(state, frame("board_updated", null));
    applyBoardFrame(state, frame("board_updated", { epics: "not-an-array" }));
    applyBoardFrame(state, frame("board_updated", { epics: [], tasks: 42 }));

    expect(JSON.stringify(state)).toBe(before);
  });

  it("ignores unrelated frame types", () => {
    const state = initialBoardState();
    hydrateBoard(state, board([makeEpic({ id: "E1" })], []));
    const before = JSON.stringify(state);

    applyBoardFrame(state, frame("subscribed", {}));
    applyBoardFrame(state, frame("clone_status", { id: "P1", clone_status: "ready" }));
    applyBoardFrame(state, frame("text", { delta: "hi" }));

    expect(JSON.stringify(state)).toBe(before);
  });
});
