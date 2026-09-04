<script setup lang="ts" generic="T">
// Generic graph canvas: SVG dependency edges behind absolutely-positioned
// node cards, with the node rendering delegated to a typed scoped slot so the
// component stays shape-agnostic. The planning Map view feeds it map nodes
// today; the executor task DAG is the intended adopter later (its nodes carry
// the same blocker/blocked edge shape, so only the slot content differs).
//
// Edge anchors are computed from the fixed node geometry passed in as props —
// no DOM measurement — which keeps the canvas render pure and deterministic.
const props = defineProps<{
  /** Placed nodes (canvas coordinates of each card's top-left corner). */
  nodes: { id: string; x: number; y: number; node: T }[];
  /** Dependency edges: `blocker_id` blocks `blocked_id`. */
  edges: { blocker_id: string; blocked_id: string }[];
  /** Node card geometry — must match what the layout computed. */
  nodeWidth: number;
  nodeHeight: number;
  /** Canvas bounding box (from the layout). */
  width: number;
  height: number;
  /** Id of the currently-open node, if any (highlighted). */
  selectedId?: string | null;
}>();

const emit = defineEmits<{
  /** A node card was clicked (click-to-open). */
  (e: "node-click", id: string): void;
}>();

/** Edge path from the blocker's right edge midpoint to the blocked's left edge midpoint. */
function edgePath(edge: { blocker_id: string; blocked_id: string }): string | null {
  const from = props.nodes.find((n) => n.id === edge.blocker_id);
  const to = props.nodes.find((n) => n.id === edge.blocked_id);
  if (!from || !to) return null;
  const x1 = from.x + props.nodeWidth;
  const y1 = from.y + props.nodeHeight / 2;
  const x2 = to.x;
  const y2 = to.y + props.nodeHeight / 2;
  // A gentle S-curve reads better than a hard elbow between columns.
  const mx = (x1 + x2) / 2;
  return `M ${x1} ${y1} C ${mx} ${y1}, ${mx} ${y2}, ${x2} ${y2}`;
}
</script>

<template>
  <div class="graph-scroll">
    <div class="graph-canvas" :style="{ width: `${props.width}px`, height: `${props.height}px` }">
      <svg
        class="graph-edges"
        :width="props.width"
        :height="props.height"
        aria-hidden="true"
      >
        <defs>
          <marker
            id="graph-arrow"
            viewBox="0 0 8 8"
            refX="7"
            refY="4"
            markerWidth="7"
            markerHeight="7"
            orient="auto-start-reverse"
          >
            <path d="M1 1 L7 4 L1 7" fill="none" stroke="currentColor" stroke-width="1.4" />
          </marker>
        </defs>
        <path
          v-for="e in props.edges"
          :key="`${e.blocker_id}-${e.blocked_id}`"
          class="graph-edge"
          :class="{ 'graph-edge-dim': edgePath(e) === null }"
          :d="edgePath(e) ?? undefined"
          marker-end="url(#graph-arrow)"
        />
      </svg>

      <button
        v-for="n in props.nodes"
        :key="n.id"
        type="button"
        class="graph-node"
        :class="{ 'graph-node-selected': n.id === props.selectedId }"
        :style="{
          left: `${n.x}px`,
          top: `${n.y}px`,
          width: `${props.nodeWidth}px`,
          height: `${props.nodeHeight}px`,
        }"
        :data-node-id="n.id"
        @click="emit('node-click', n.id)"
      >
        <!--
          The consumer owns node presentation (kind/state coloring, badges);
          the canvas only owns placement, edges, and click plumbing.
        -->
        <slot name="node" :node="n.node" />
      </button>
    </div>
  </div>
</template>

<style scoped>
.graph-scroll {
  overflow: auto;
  border: 1px solid var(--border-hairline);
  border-radius: var(--radius-cards);
  background:
    radial-gradient(circle, var(--color-graphite) 1px, transparent 1px) 0 0 / 24px 24px,
    var(--surface-carbon);
}

.graph-canvas {
  position: relative;
  min-width: 100%;
}

.graph-edges {
  position: absolute;
  inset: 0;
  color: var(--color-smoke);
  pointer-events: none;
}

.graph-edge {
  fill: none;
  stroke: currentColor;
  stroke-width: 1.4;
}

.graph-node {
  position: absolute;
  display: block;
  padding: 0;
  border: 1px solid var(--border-hairline);
  border-radius: var(--radius-cards);
  background: var(--surface-obsidian);
  color: inherit;
  font: inherit;
  text-align: left;
  cursor: pointer;
  transition:
    border-color var(--duration-fast) var(--ease-out),
    box-shadow var(--duration-fast) var(--ease-out);
}

.graph-node:hover {
  border-color: var(--border-strong);
}

.graph-node-selected {
  border-color: var(--color-acid-lime);
}

.graph-edge-dim {
  stroke-dasharray: 3 3;
}
</style>
