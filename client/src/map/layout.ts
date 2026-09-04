// Pure graph-layout for the Map graph view — framework-free so it can be
// unit-tested without a browser, and deliberately node-shape-agnostic (only
// `id`, optional stored coordinates, and edges) so the executor task DAG can
// adopt the same graph renderer later (intended follow-up).
//
// Nodes that carry stored coordinates (`position_x`/`position_y`, set via the
// map CLI / create body) are honored as-is. Everything else is auto-laid-out
// as a left→right layered DAG: depth = longest path from a root (a node with
// no blockers), columns by depth, rows stacked top-down within a column in
// stable (created_at, id) order. Cycles can't be created by the server (the
// link endpoint guards them) but the depth walk is defensive against them
// anyway so a bad payload can never hang the render.

/** A node the layout can position: an id plus optional stored coordinates. */
export interface LayoutNode {
  id: string;
  position_x: number | null;
  position_y: number | null;
  created_at?: number;
}

/** A dependency edge: `blocker_id` blocks `blocked_id` (must settle first). */
export interface LayoutEdge {
  blocker_id: string;
  blocked_id: string;
}

/** A node placed at canvas coordinates (top-left corner of its card). */
export interface PlacedNode<N> {
  node: N;
  x: number;
  y: number;
}

/** The laid-out graph: placed nodes plus the canvas bounding box. */
export interface GraphLayout<N> {
  placed: PlacedNode<N>[];
  width: number;
  height: number;
}

/** Node card geometry (fixed so edge anchors are computable without DOM). */
export const NODE_WIDTH = 216;
export const NODE_HEIGHT = 84;
/** Gaps between columns (layers) and rows within a layer, plus the margin. */
export const GAP_X = 72;
export const GAP_Y = 36;
export const MARGIN = 32;

/**
 * Lay out `nodes`/`edges` into a left→right layered DAG.
 *
 * - Stored coordinates win: a node with both `position_x`/`position_y` set is
 *   placed exactly there and excluded from auto-layout.
 * - Auto-laid-out nodes get `x` from their layer depth (longest path from a
 *   root) and `y` from their stable row order within the layer.
 * - The bounding box covers every placed node plus one margin of padding.
 */
export function layoutGraph<N extends LayoutNode>(
  nodes: N[],
  edges: LayoutEdge[],
): GraphLayout<N> {
  const anchored = nodes.filter(
    (n) => n.position_x !== null && n.position_y !== null,
  );
  const floating = nodes.filter(
    (n) => n.position_x === null || n.position_y === null,
  );

  const depths = layerDepths(floating, nodes, edges);

  // Group floating nodes by depth, in stable order within each layer.
  const layers = new Map<number, N[]>();
  for (const node of floating) {
    const depth = depths.get(node.id) ?? 0;
    const layer = layers.get(depth);
    if (layer) {
      layer.push(node);
    } else {
      layers.set(depth, [node]);
    }
  }
  for (const layer of layers.values()) {
    layer.sort((a, b) => (a.created_at ?? 0) - (b.created_at ?? 0) || a.id.localeCompare(b.id));
  }

  const placed: PlacedNode<N>[] = anchored.map((n) => ({
    node: n,
    x: n.position_x as number,
    y: n.position_y as number,
  }));
  for (const [depth, layer] of layers) {
    layer.forEach((node, row) => {
      placed.push({
        node,
        x: MARGIN + depth * (NODE_WIDTH + GAP_X),
        y: MARGIN + row * (NODE_HEIGHT + GAP_Y),
      });
    });
  }

  let width = 0;
  let height = 0;
  for (const p of placed) {
    width = Math.max(width, p.x + NODE_WIDTH);
    height = Math.max(height, p.y + NODE_HEIGHT);
  }
  return {
    placed,
    width: width + MARGIN,
    height: height + MARGIN,
  };
}

/**
 * Longest-path depth from a root for each node in `ids`, over `edges`.
 * Roots (no incoming edges) land at depth 0; every other node sits one past
 * its deepest blocker. Cycles are broken defensively (a back-edge contributes
 * depth 0) so a malformed graph cannot loop forever; nodes absent from `ids`
 * (e.g. anchored siblings) still count as blockers for depth purposes.
 */
function layerDepths<N extends LayoutNode>(
  ids: N[],
  allNodes: N[],
  edges: LayoutEdge[],
): Map<string, number> {
  const known = new Set(allNodes.map((n) => n.id));
  const wanted = new Set(ids.map((n) => n.id));

  const blockers = new Map<string, string[]>();
  for (const e of edges) {
    if (!known.has(e.blocked_id) || !known.has(e.blocker_id)) continue;
    const list = blockers.get(e.blocked_id);
    if (list) {
      list.push(e.blocker_id);
    } else {
      blockers.set(e.blocked_id, [e.blocker_id]);
    }
  }

  const depths = new Map<string, number>();
  const visiting = new Set<string>();

  const depthOf = (id: string): number => {
    const cached = depths.get(id);
    if (cached !== undefined) return cached;
    if (visiting.has(id)) return 0; // defensive: break cycles at 0
    visiting.add(id);
    const deps = blockers.get(id) ?? [];
    let depth = deps.length === 0 ? 0 : -1;
    for (const dep of deps) {
      if (!known.has(dep)) continue;
      depth = Math.max(depth, depthOf(dep) + 1);
    }
    visiting.delete(id);
    depths.set(id, depth < 0 ? 0 : depth);
    return depths.get(id)!;
  };

  for (const id of wanted) {
    depthOf(id);
  }
  return depths;
}
