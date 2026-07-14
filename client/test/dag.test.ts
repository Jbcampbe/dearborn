// Unit tests for the pure DAG stream reducer (T-303). No browser, no WS — just
// fold hand-built frames and assert the resulting view model. Mirrors the
// planning stream reducer tests.

import { describe, expect, it } from "vitest";

import type { Epic } from "../src/api/epics";
import type { Dag, DagNode } from "../src/api/tasks";
import {
  applyDagFrame,
  blockersOf,
  blocksOf,
  hydrateDag,
  initialDagState,
  nodeById,
  type DagFrame,
} from "../src/dag/stream";

const TOPIC = "epic:E1";

function frame(type: string, payload: unknown): DagFrame {
  return { topic: TOPIC, type, payload };
}

function makeEpic(overrides: Partial<Epic> = {}): Epic {
  return {
    id: "E1",
    project_id: "P1",
    title: "Ship it",
    product_context: null,
    technical_context: null,
    status: "Planning",
    created_at: 1,
    updated_at: 1,
    ...overrides,
  };
}

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

function dag(nodes: DagNode[], edges: { blocker_id: string; blocked_id: string }[]): Dag {
  return { epic_id: "E1", nodes, edges };
}

describe("DAG stream reducer", () => {
  it("hydrate stamps the epic id and sets nodes + edges", () => {
    const state = initialDagState();
    const a = node({ id: "A" });
    const b = node({ id: "B", ready: false, blocked_by: ["A"] });
    hydrateDag(state, makeEpic({ status: "Ready" }), dag([a, b], [{ blocker_id: "A", blocked_id: "B" }]));

    expect(state.epicId).toBe("E1");
    expect(state.epic?.status).toBe("Ready");
    expect(state.nodes).toHaveLength(2);
    expect(state.edges).toHaveLength(1);
  });

  it("dag_updated replaces the whole DAG (nodes + edges)", () => {
    const state = initialDagState();
    hydrateDag(state, makeEpic(), dag([node({ id: "A" })], []));

    // A mutation publishes a fresh full DAG.
    applyDagFrame(state, frame("dag_updated", dag(
      [node({ id: "A" }), node({ id: "B", ready: false, blocked_by: ["A"] })],
      [{ blocker_id: "A", blocked_id: "B" }],
    )));

    expect(state.nodes.map((n) => n.id)).toEqual(["A", "B"]);
    expect(state.edges).toEqual([{ blocker_id: "A", blocked_id: "B" }]);
  });

  it("epic_updated replaces the epic record (e.g. the Planning -> Ready transition)", () => {
    const state = initialDagState();
    hydrateDag(state, makeEpic({ status: "Planning" }), dag([], []));

    applyDagFrame(state, frame("epic_updated", makeEpic({ status: "Ready" })));

    expect(state.epic?.status).toBe("Ready");
  });

  it("ignores unrelated frame types and malformed payloads", () => {
    const state = initialDagState();
    const a = node({ id: "A" });
    hydrateDag(state, makeEpic(), dag([a], []));
    const before = JSON.stringify(state);

    applyDagFrame(state, frame("subscribed", {}));
    applyDagFrame(state, frame("text", { runId: "r1", delta: "hi" }));
    applyDagFrame(state, frame("dag_updated", null));
    applyDagFrame(state, frame("dag_updated", { nodes: "not-an-array" }));
    applyDagFrame(state, frame("epic_updated", null));

    expect(JSON.stringify(state)).toBe(before);
  });

  it("blockersOf / blocksOf derive upstream and downstream edges", () => {
    const state = initialDagState();
    hydrateDag(state, makeEpic(), dag(
      [node({ id: "A" }), node({ id: "B" }), node({ id: "C" })],
      [
        { blocker_id: "A", blocked_id: "B" },
        { blocker_id: "B", blocked_id: "C" },
      ],
    ));

    expect(blockersOf(state, "B")).toEqual(["A"]);
    expect(blocksOf(state, "B")).toEqual(["C"]);
    expect(blockersOf(state, "A")).toEqual([]);
    expect(blocksOf(state, "C")).toEqual([]);
  });

  it("nodeById looks up a node", () => {
    const state = initialDagState();
    hydrateDag(state, makeEpic(), dag([node({ id: "A", title: "First" })], []));

    expect(nodeById(state, "A")?.title).toBe("First");
    expect(nodeById(state, "nope")).toBeUndefined();
  });

  it("defensively normalizes dag_updated nodes missing ready/blocked_by", () => {
    // The server publishes DagNodes, but the WS is a trust boundary: a node
    // missing `ready`/`blocked_by` must degrade safely rather than crash the
    // render (the template reads `n.blocked_by.length`).
    const state = initialDagState();
    hydrateDag(state, makeEpic(), dag([], []));

    const plainTask = {
      id: "A",
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
      // no `ready`, no `blocked_by`
    };
    applyDagFrame(state, frame("dag_updated", { epic_id: "E1", nodes: [plainTask], edges: [] }));

    expect(state.nodes).toHaveLength(1);
    expect(state.nodes[0].ready).toBe(false);
    expect(state.nodes[0].blocked_by).toEqual([]);
    // The renderability invariant: `blocked_by` is always an array.
    expect(Array.isArray(state.nodes[0].blocked_by)).toBe(true);
  });
});
