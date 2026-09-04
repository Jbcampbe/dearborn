// Unit tests for the planning-map stream reducer and graph layout. No browser,
// no WS — fold hand-built frames and assert the resulting view model. Mirrors
// the DAG stream reducer tests (`dag.test.ts`).

import { describe, expect, it } from "vitest";

import type { Epic } from "../src/api/epics";
import type { Map, MapEdge, MapNodeView } from "../src/api/map";
import {
  applyMapFrame,
  blockersOf,
  blocksOf,
  hydrateMap,
  initialMapState,
  nodeById,
  readinessOf,
  type MapFrame,
  type MapState,
} from "../src/map/stream";
import {
  layoutGraph,
  NODE_HEIGHT,
  NODE_WIDTH,
  type LayoutNode,
} from "../src/map/layout";

const TOPIC = "epic:E1";

function frame(type: string, payload: unknown): MapFrame {
  return { topic: TOPIC, type, payload };
}

function makeEpic(overrides: Partial<Epic> = {}): Epic {
  return {
    id: "E1",
    project_id: "P1",
    title: "Plan the thing",
    destination: "A settled spec",
    notes: null,
    status: "Planning",
    created_at: 1,
    updated_at: 1,
    ...overrides,
  };
}

function node(overrides: Partial<MapNodeView> = {}): MapNodeView {
  return {
    id: "N1",
    epic_id: "E1",
    kind: "grilling",
    task_mode: null,
    state: "open",
    title: "Which approach?",
    question: "Do we X or Y?",
    gist: null,
    out_of_scope_reason: null,
    created_by: null,
    resolved_by: null,
    position_x: null,
    position_y: null,
    created_at: 1,
    updated_at: 1,
    frontier: true,
    blocked_by: [],
    ...overrides,
  };
}

function map(nodes: MapNodeView[], edges: MapEdge[]): Map {
  return {
    epic_id: "E1",
    destination: "A settled spec",
    notes: null,
    not_yet_specified: "some fog",
    out_of_scope: null,
    nodes,
    edges,
    completion: { eligible: false, open_nodes: nodes.length, fog_remaining: true },
  };
}

describe("map stream reducer", () => {
  it("hydrate stamps the epic id and stores the map", () => {
    const state = initialMapState();
    const a = node({ id: "A" });
    const b = node({ id: "B", frontier: false, blocked_by: ["A"] });
    hydrateMap(state, makeEpic(), map([a, b], [{ blocker_id: "A", blocked_id: "B" }]));

    expect(state.epicId).toBe("E1");
    expect(state.epic?.status).toBe("Planning");
    expect(state.map?.nodes).toHaveLength(2);
    expect(state.map?.edges).toHaveLength(1);
    expect(state.map?.not_yet_specified).toBe("some fog");
  });

  it("map_updated replaces the whole map (prose, nodes, edges, completion)", () => {
    const state = initialMapState();
    hydrateMap(state, makeEpic(), map([node({ id: "A" })], []));

    applyMapFrame(
      state,
      frame("map_updated", map(
        [
          node({ id: "A", state: "resolved", gist: "We chose X", frontier: false }),
          node({ id: "B", frontier: true }),
        ],
        [{ blocker_id: "A", blocked_id: "B" }],
      )),
    );

    expect(state.map?.nodes.map((n) => n.id)).toEqual(["A", "B"]);
    expect(state.map?.nodes[0].gist).toBe("We chose X");
    expect(state.map?.edges).toEqual([{ blocker_id: "A", blocked_id: "B" }]);
  });

  it("map_updated for a different epic is ignored", () => {
    const state = initialMapState();
    hydrateMap(state, makeEpic(), map([node({ id: "A" })], []));
    const before = JSON.stringify(state);

    const otherMap = { ...map([node({ id: "Z" })], []), epic_id: "E2" };
    applyMapFrame(state, frame("map_updated", otherMap));

    expect(JSON.stringify(state)).toBe(before);
  });

  it("epic_updated replaces the epic record (e.g. the Planning -> Ready transition)", () => {
    const state: MapState = initialMapState();
    hydrateMap(state, makeEpic(), map([], []));

    applyMapFrame(state, frame("epic_updated", makeEpic({ status: "Ready" })));

    expect(state.epic?.status).toBe("Ready");
  });

  it("normalizes nodes missing computed readiness fields instead of crashing", () => {
    const state = initialMapState();
    hydrateMap(state, makeEpic(), map([], []));

    applyMapFrame(state, frame("map_updated", {
      epic_id: "E1",
      nodes: [{ id: "A", state: "open" }],
      edges: [],
    }));

    expect(state.map?.nodes).toEqual([
      { id: "A", state: "open", frontier: false, blocked_by: [] },
    ]);
  });

  it("ignores unrelated frame types and malformed payloads", () => {
    const state = initialMapState();
    hydrateMap(state, makeEpic(), map([node({ id: "A" })], []));
    const before = JSON.stringify(state);

    applyMapFrame(state, frame("subscribed", {}));
    applyMapFrame(state, frame("text", { runId: "r1", delta: "hi" }));
    applyMapFrame(state, frame("map_updated", null));
    applyMapFrame(state, frame("map_updated", { epic_id: "E1", nodes: "nope", edges: [] }));
    applyMapFrame(state, frame("map_updated", { epic_id: "E1", nodes: [], edges: "nope" }));
    applyMapFrame(state, frame("epic_updated", null));

    expect(JSON.stringify(state)).toBe(before);
  });

  it("blockersOf / blocksOf derive upstream and downstream edges", () => {
    const state = initialMapState();
    hydrateMap(
      state,
      makeEpic(),
      map(
        [node({ id: "A" }), node({ id: "B" }), node({ id: "C" })],
        [
          { blocker_id: "A", blocked_id: "B" },
          { blocker_id: "B", blocked_id: "C" },
        ],
      ),
    );

    expect(blockersOf(state, "C")).toEqual(["B"]);
    expect(blocksOf(state, "A")).toEqual(["B"]);
    expect(nodeById(state, "B")?.title).toBe("Which approach?");
  });
});

describe("readinessOf", () => {
  it("distinguishes frontier from blocked for open nodes", () => {
    expect(readinessOf(node({ frontier: true }))).toBe("frontier");
    expect(readinessOf(node({ frontier: false, blocked_by: ["A"] }))).toBe("blocked");
  });

  it("maps settled and being-worked states through", () => {
    expect(readinessOf(node({ state: "resolved" }))).toBe("resolved");
    expect(readinessOf(node({ state: "out_of_scope" }))).toBe("out_of_scope");
    // in_progress wins over its implied frontier flag.
    expect(readinessOf(node({ state: "in_progress", frontier: true }))).toBe("in_progress");
  });
});

// The layout accepts any node shape with id/position/created_at; the map view
// feeds it full MapNodeViews, so widen the fixture type accordingly.
function layoutNode(overrides: Partial<LayoutNode> = {}): LayoutNode {
  return {
    id: "A",
    position_x: null,
    position_y: null,
    created_at: 1,
    ...overrides,
  };
}

describe("graph layout", () => {
  it("chains flow left to right by longest-path depth", () => {
    const nodes = [layoutNode({ id: "A", created_at: 3 }), layoutNode({ id: "B", created_at: 2 }), layoutNode({ id: "C", created_at: 1 })];
    const { placed, width, height } = layoutGraph(nodes, [
      { blocker_id: "A", blocked_id: "B" },
      { blocker_id: "B", blocked_id: "C" },
    ]);

    const xOf = Object.fromEntries(placed.map((p) => [p.node.id, p.x]));
    expect(xOf["A"]).toBeLessThan(xOf["B"]);
    expect(xOf["B"]).toBeLessThan(xOf["C"]);
    expect(width).toBeGreaterThan(NODE_WIDTH);
    expect(height).toBeGreaterThan(NODE_HEIGHT);
  });

  it("stacks independent siblings vertically within a layer, in stable order", () => {
    const nodes = [
      layoutNode({ id: "root", created_at: 1 }),
      layoutNode({ id: "s2", created_at: 2 }),
      layoutNode({ id: "s1", created_at: 3 }),
    ];
    const { placed } = layoutGraph(nodes, [
      { blocker_id: "root", blocked_id: "s1" },
      { blocker_id: "root", blocked_id: "s2" },
    ]);

    const yOf = Object.fromEntries(placed.map((p) => [p.node.id, p.y]));
    // Stable (created_at, id) order: s2 (created_at 2) above s1 (created_at 3).
    expect(yOf["s2"]).toBeLessThan(yOf["s1"]);
    expect(yOf["root"]).toBe(yOf["s2"]); // same layer column top
  });

  it("honors stored coordinates instead of auto-layout", () => {
    const nodes = [
      layoutNode({ id: "A" }),
      layoutNode({ id: "B", position_x: 500, position_y: 300 }),
    ];
    const { placed } = layoutGraph(nodes, [{ blocker_id: "A", blocked_id: "B" }]);

    const b = placed.find((p) => p.node.id === "B");
    expect(b).toEqual({ node: nodes[1], x: 500, y: 300 });
  });

  it("breaks defensive cycles instead of hanging", () => {
    const nodes = [layoutNode({ id: "A" }), layoutNode({ id: "B" })];
    const { placed } = layoutGraph(nodes, [
      { blocker_id: "A", blocked_id: "B" },
      { blocker_id: "B", blocked_id: "A" },
    ]);
    expect(placed).toHaveLength(2);
  });

  it("lays out an empty map without NaNs", () => {
    const { placed, width, height } = layoutGraph([], []);
    expect(placed).toEqual([]);
    expect(Number.isFinite(width)).toBe(true);
    expect(Number.isFinite(height)).toBe(true);
  });
});
