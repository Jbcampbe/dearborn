<script setup lang="ts">
import { computed, onBeforeUnmount, onMounted, reactive, ref } from "vue";
import { RouterLink } from "vue-router";

import { useAuthStore } from "../stores/auth";
import { ApiError } from "../api/client";
import { getEpic } from "../api/epics";
import { getProject } from "../api/projects";
import {
  getMap,
  type MapNodeView,
} from "../api/map";
import {
  hydrateMap,
  initialMapState,
  nodeById,
  readinessOf,
  type MapState,
  type Readiness,
} from "../map/stream";
import { useMapStream, type StreamStatus } from "../map/useMapStream";
import {
  layoutGraph,
  NODE_HEIGHT,
  NODE_WIDTH,
} from "../map/layout";
import AppIcon from "./AppIcon.vue";
import EpicTabs from "./EpicTabs.vue";
import MapGraph from "./MapGraph.vue";
import StatusIcon from "./StatusIcon.vue";

// The planning Map graph view (wayfinder epic, client phase 6). A *fresh*
// graph — not the DagEditorView layout — rendering the epic's decision nodes
// colored by kind + computed readiness, with dependency edges and
// click-to-open details. Live via `map_updated` frames on `epic:<id>` folded
// through the pure reducer (`map/stream.ts`), exactly like the DAG editor's
// plumbing. The graph canvas (`MapGraph.vue`) is deliberately generic so the
// executor task DAG can adopt it later.
//
// Click-to-open shows a node detail panel; the node session view (multi-party
// chat + resolve) is a later client task and will take over the open affordance.
const props = defineProps<{ id: string }>();

const auth = useAuthStore();
const state = reactive<MapState>(initialMapState());
const loading = ref(true);
const error = ref<string | null>(null);
const streamStatus = ref<StreamStatus>("connecting");
// The breadcrumb's project name (the epic only carries `project_id`); fills in
// after load and falls back to "…" if the fetch fails.
const projectName = ref<string | null>(null);

// The currently-open node (click-to-open); null = panel closed.
const selectedId = ref<string | null>(null);

let stream: ReturnType<typeof useMapStream> | null = null;
onBeforeUnmount(() => stream?.close());

const epic = computed(() => state.epic);
const map = computed(() => state.map);
const nodes = computed(() => state.map?.nodes ?? []);
const edges = computed(() => state.map?.edges ?? []);
const completion = computed(() => state.map?.completion ?? null);

/** Fog / out-of-scope prose, empty-string-normalized for the template. */
const fog = computed(() => state.map?.not_yet_specified?.trim() ?? "");
const outOfScopeProse = computed(() => state.map?.out_of_scope?.trim() ?? "");
const notes = computed(() => state.map?.notes?.trim() ?? "");

const layout = computed(() => layoutGraph(nodes.value, edges.value));

/** The canvas wants `{ id, x, y, node }`; the layout returns `{ node, x, y }`. */
const placedNodes = computed(() =>
  layout.value.placed.map((p) => ({ id: p.node.id, x: p.x, y: p.y, node: p.node })),
);

const selectedNode = computed(() =>
  selectedId.value === null ? undefined : nodeById(state, selectedId.value),
);

const KIND_LABELS: Record<string, string> = {
  grilling: "Grilling",
  research: "Research",
  prototype: "Prototype",
  task: "Task",
};

const READINESS_LABELS: Record<Readiness, string> = {
  frontier: "frontier",
  blocked: "blocked",
  in_progress: "in progress",
  resolved: "resolved",
  out_of_scope: "out of scope",
};

const READINESS_TONES: Record<Readiness, string> = {
  frontier: "green",
  blocked: "red",
  in_progress: "teal",
  resolved: "green",
  out_of_scope: "neutral",
};

function kindLabel(kind: string): string {
  return KIND_LABELS[kind] ?? kind;
}

function readiness(n: MapNodeView): Readiness {
  return readinessOf(n);
}

function titleOf(id: string): string {
  return nodeById(state, id)?.title ?? id.slice(0, 6);
}

/** Snippet under the title: a resolution gist, the question, or nothing. */
function nodeSnippet(n: MapNodeView): string {
  return n.gist ?? n.question ?? "";
}

function bounceIfAuth(err: unknown): boolean {
  if (err instanceof ApiError && err.isAuth) {
    auth.logout(`Token rejected (401): ${err.message}. Please re-enter it.`);
    return true;
  }
  return false;
}

function openNode(id: string): void {
  selectedId.value = id;
}

function closeNode(): void {
  selectedId.value = null;
}

async function load() {
  const token = auth.accessToken;
  if (token === null) {
    return;
  }
  loading.value = true;
  error.value = null;
  try {
    const [epicObj, mapObj] = await Promise.all([
      getEpic(token, props.id),
      getMap(token, props.id),
    ]);
    hydrateMap(state, epicObj, mapObj);
    stream = useMapStream(props.id, () => auth.ensureFresh(), state, streamStatus);
    // Non-blocking + non-fatal: the breadcrumb falls back to "…" without it.
    void getProject(token, epicObj.project_id)
      .then((p) => (projectName.value = p.name))
      .catch((err) => bounceIfAuth(err));
  } catch (err) {
    if (bounceIfAuth(err)) {
      return;
    }
    error.value = err instanceof Error ? err.message : "failed to load the map";
  } finally {
    loading.value = false;
  }
}

onMounted(load);
</script>

<template>
  <main class="page page-wide">
    <nav class="crumbs">
      <RouterLink :to="{ name: 'projects' }">Projects</RouterLink>
      <template v-if="epic">
        <span class="sep">/</span>
        <RouterLink :to="{ name: 'project-detail', params: { id: epic.project_id } }">
          {{ projectName ?? "…" }}
        </RouterLink>
      </template>
    </nav>

    <div v-if="loading" class="loading-stack" aria-label="Loading map">
      <div class="skeleton sk-title" />
      <div class="skeleton sk-block" />
    </div>
    <p v-else-if="error && !epic" class="banner banner-error" role="alert">{{ error }}</p>

    <template v-else-if="epic">
      <header class="head fade-in">
        <div class="head-main">
          <h1 class="page-title">{{ epic.title }}</h1>
          <div class="head-badges">
            <span class="badge">
              <StatusIcon :status="epic.status" :size="11" />
              {{ epic.status }}
            </span>
            <span v-if="map?.destination" class="destination">{{ map.destination }}</span>
          </div>
        </div>
        <span class="conn" :data-status="streamStatus">{{ streamStatus === "open" ? "live" : streamStatus }}</span>
      </header>

      <EpicTabs :id="props.id" tab="map" />

      <p v-if="error" class="banner banner-error" role="alert">{{ error }}</p>

      <!-- Completion gate (computed, never stored): the way is clear. -->
      <div v-if="completion?.eligible" class="banner banner-clear" role="status">
        <AppIcon name="check" :size="13" />
        The way is clear — every node is settled and the fog is gone. Ready to break down.
      </div>
      <div
        v-else-if="completion"
        class="banner banner-fog"
        role="status"
      >
        <AppIcon name="warning" :size="13" />
        {{ completion.open_nodes }} open node{{ completion.open_nodes === 1 ? "" : "s" }}<template v-if="completion.fog_remaining"> and fog remaining</template>.
      </div>

      <div class="map-columns" :class="{ 'has-panel': selectedNode }">
        <section class="map-area">
          <div v-if="nodes.length === 0" class="empty-state">
            <AppIcon name="map" :size="20" />
            <p>The map is empty. The seed grilling node appears here once created.</p>
          </div>

          <MapGraph
            v-else
            :nodes="placedNodes"
            :edges="edges"
            :node-width="NODE_WIDTH"
            :node-height="NODE_HEIGHT"
            :width="layout.width"
            :height="layout.height"
            :selected-id="selectedId"
            @node-click="openNode"
          >
            <template #node="{ node }">
              <div
                class="map-card"
                :data-kind="node.kind"
                :data-readiness="readiness(node)"
              >
                <div class="map-card-top">
                  <span class="kind">{{ kindLabel(node.kind) }}</span>
                  <span class="badge" :data-tone="READINESS_TONES[readiness(node)]">
                    {{ READINESS_LABELS[readiness(node)] }}
                  </span>
                </div>
                <span class="map-card-title" :data-out-of-scope="node.state === 'out_of_scope'">
                  {{ node.title }}
                </span>
                <span v-if="nodeSnippet(node)" class="map-card-snippet">{{ nodeSnippet(node) }}</span>
              </div>
            </template>
          </MapGraph>

          <!-- Legend: kind colors + readiness readings. -->
          <div class="legend">
            <span class="legend-group">
              <span v-for="(label, kind) in KIND_LABELS" :key="kind" class="legend-item">
                <span class="kind-dot" :data-kind="kind" />{{ label }}
              </span>
            </span>
            <span class="legend-group">
              <span class="legend-item"><span class="readiness-dot" data-readiness="frontier" />frontier</span>
              <span class="legend-item"><span class="readiness-dot" data-readiness="blocked" />blocked</span>
              <span class="legend-item"><span class="readiness-dot" data-readiness="in_progress" />in progress</span>
              <span class="legend-item"><span class="readiness-dot" data-readiness="resolved" />resolved</span>
              <span class="legend-item"><span class="readiness-dot" data-readiness="out_of_scope" />out of scope</span>
            </span>
          </div>
        </section>

        <!-- Click-to-open node details ------------------------------------- -->
        <aside v-if="selectedNode" class="node-panel">
          <div class="section-head">
            <h2>Node</h2>
            <button class="btn btn-icon" aria-label="Close node details" @click="closeNode">
              <AppIcon name="x" :size="12" />
            </button>
          </div>

          <div class="node-panel-body" :data-kind="selectedNode.kind">
            <div class="panel-badges">
              <span class="kind-pill" :data-kind="selectedNode.kind">{{ kindLabel(selectedNode.kind) }}</span>
              <span class="badge" :data-tone="READINESS_TONES[readiness(selectedNode)]">
                {{ READINESS_LABELS[readiness(selectedNode)] }}
              </span>
              <span v-if="selectedNode.task_mode" class="badge">{{ selectedNode.task_mode }}</span>
            </div>

            <h3 class="panel-title" :data-out-of-scope="selectedNode.state === 'out_of_scope'">
              {{ selectedNode.title }}
            </h3>

            <div v-if="selectedNode.question" class="panel-field">
              <span class="label">Question</span>
              <p>{{ selectedNode.question }}</p>
            </div>

            <div v-if="selectedNode.gist" class="panel-field">
              <span class="label">Decision</span>
              <p>{{ selectedNode.gist }}</p>
            </div>

            <div v-if="selectedNode.out_of_scope_reason" class="panel-field">
              <span class="label">Why out of scope</span>
              <p>{{ selectedNode.out_of_scope_reason }}</p>
            </div>

            <div v-if="selectedNode.blocked_by.length" class="panel-field">
              <span class="label">Blocked by</span>
              <div class="chip-row">
                <button
                  v-for="b in selectedNode.blocked_by"
                  :key="b"
                  class="chip chip-link"
                  type="button"
                  @click="openNode(b)"
                >
                  {{ titleOf(b) }}
                </button>
              </div>
            </div>
          </div>
        </aside>
      </div>

      <!-- Fog / out-of-scope prose (never nodes — plan §3). -->
      <div v-if="fog || outOfScopeProse || notes" class="prose-row">
        <section v-if="fog" class="prose-card">
          <h2><AppIcon name="warning" :size="12" /> Fog</h2>
          <p>{{ fog }}</p>
        </section>
        <section v-if="outOfScopeProse" class="prose-card">
          <h2><AppIcon name="x" :size="12" /> Out of scope</h2>
          <p>{{ outOfScopeProse }}</p>
        </section>
        <section v-if="notes" class="prose-card">
          <h2><AppIcon name="pencil" :size="12" /> Notes</h2>
          <p>{{ notes }}</p>
        </section>
      </div>
    </template>
  </main>
</template>

<style scoped>
.head {
  display: flex;
  align-items: flex-start;
  justify-content: space-between;
  gap: var(--spacing-16);
  margin-bottom: var(--spacing-20);
}

.head-main {
  display: flex;
  flex-direction: column;
  gap: var(--spacing-8);
  min-width: 0;
}

.head-badges {
  display: flex;
  align-items: center;
  gap: var(--spacing-12);
  flex-wrap: wrap;
}

.destination {
  font-size: var(--text-caption);
  color: var(--text-muted);
  line-height: 1.4;
}

.banner-clear {
  display: flex;
  align-items: center;
  gap: var(--spacing-8);
  border-color: rgba(39, 166, 68, 0.35);
  color: #4ec96b;
}

.banner-fog {
  display: flex;
  align-items: center;
  gap: var(--spacing-8);
  color: var(--text-muted);
}

.map-columns {
  display: grid;
  grid-template-columns: minmax(0, 1fr);
  gap: var(--spacing-24);
  margin-top: var(--spacing-16);
}

.map-columns.has-panel {
  grid-template-columns: minmax(0, 2fr) minmax(0, 1fr);
}

@media (max-width: 60rem) {
  .map-columns.has-panel {
    grid-template-columns: 1fr;
  }
}

.section-head {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: var(--spacing-8);
  margin-bottom: var(--spacing-12);
}

.section-head h2 {
  font-size: var(--text-body-sm);
  font-weight: var(--weight-medium);
}

/* Node cards (slotted into the generic canvas) --------------------------- */
.map-card {
  display: flex;
  flex-direction: column;
  gap: 4px;
  height: 100%;
  padding: 10px 12px;
  border-left: 3px solid var(--color-ash);
  border-radius: var(--radius-cards);
  box-sizing: border-box;
  overflow: hidden;
}

.map-card[data-kind="grilling"] {
  border-left-color: var(--color-iris-violet);
}

.map-card[data-kind="prototype"] {
  border-left-color: var(--color-lavender);
}

.map-card[data-kind="research"] {
  border-left-color: var(--color-signal-teal);
}

.map-card[data-kind="task"] {
  border-left-color: var(--color-fog);
}

.map-card[data-readiness="frontier"] {
  border-color: rgba(39, 166, 68, 0.55);
  box-shadow: 0 0 0 1px rgba(39, 166, 68, 0.25);
  background: rgba(39, 166, 68, 0.05);
}

.map-card[data-readiness="in_progress"] {
  border-color: rgba(2, 184, 204, 0.55);
  box-shadow: 0 0 0 1px rgba(2, 184, 204, 0.25);
  background: rgba(2, 184, 204, 0.05);
}

.map-card[data-readiness="blocked"] {
  background: var(--surface-carbon);
}

.map-card[data-readiness="blocked"] .map-card-title {
  color: var(--text-faint);
}

.map-card[data-readiness="resolved"],
.map-card[data-readiness="out_of_scope"] {
  background: var(--surface-carbon);
}

.map-card-top {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: var(--spacing-8);
}

.kind {
  font-size: var(--text-micro);
  font-weight: var(--weight-medium);
  text-transform: uppercase;
  letter-spacing: 0.06em;
  color: var(--text-faint);
}

.map-card-title {
  font-size: var(--text-caption);
  font-weight: var(--weight-medium);
  color: var(--text-primary);
  line-height: 1.25;
  display: -webkit-box;
  -webkit-line-clamp: 2;
  -webkit-box-orient: vertical;
  overflow: hidden;
}

.map-card-title[data-out-of-scope="true"] {
  text-decoration: line-through;
  color: var(--text-faint);
}

.map-card-snippet {
  font-size: var(--text-micro);
  color: var(--text-faint);
  line-height: 1.3;
  display: -webkit-box;
  -webkit-line-clamp: 1;
  -webkit-box-orient: vertical;
  overflow: hidden;
}

/* Legend ------------------------------------------------------------------ */
.legend {
  display: flex;
  align-items: center;
  justify-content: space-between;
  flex-wrap: wrap;
  gap: var(--spacing-12);
  margin-top: var(--spacing-12);
  font-size: var(--text-label);
  color: var(--text-muted);
}

.legend-group {
  display: flex;
  align-items: center;
  flex-wrap: wrap;
  gap: var(--spacing-12);
}

.legend-item {
  display: inline-flex;
  align-items: center;
  gap: 5px;
}

.kind-dot {
  width: 8px;
  height: 8px;
  border-radius: 2px;
  background: var(--color-ash);
}

.kind-dot[data-kind="grilling"] {
  background: var(--color-iris-violet);
}

.kind-dot[data-kind="prototype"] {
  background: var(--color-lavender);
}

.kind-dot[data-kind="research"] {
  background: var(--color-signal-teal);
}

.kind-dot[data-kind="task"] {
  background: var(--color-fog);
}

.readiness-dot {
  width: 8px;
  height: 8px;
  border-radius: var(--radius-pills);
  background: var(--color-ash);
}

.readiness-dot[data-readiness="frontier"] {
  background: var(--color-pulse-green);
}

.readiness-dot[data-readiness="blocked"] {
  background: var(--color-coral-red);
}

.readiness-dot[data-readiness="in_progress"] {
  background: var(--color-signal-teal);
}

.readiness-dot[data-readiness="resolved"] {
  background: var(--color-fog);
}

.readiness-dot[data-readiness="out_of_scope"] {
  background: var(--color-graphite);
  box-shadow: 0 0 0 1px var(--color-smoke);
}

/* Node detail panel -------------------------------------------------------- */
.node-panel {
  min-width: 0;
}

.node-panel-body {
  padding: var(--spacing-16);
  border: 1px solid var(--border-hairline);
  border-left-width: 3px;
  border-left-color: var(--color-ash);
  border-radius: var(--radius-cards);
  background: var(--surface-obsidian);
  display: flex;
  flex-direction: column;
  gap: var(--spacing-12);
}

.node-panel-body[data-kind="grilling"] {
  border-left-color: var(--color-iris-violet);
}

.node-panel-body[data-kind="prototype"] {
  border-left-color: var(--color-lavender);
}

.node-panel-body[data-kind="research"] {
  border-left-color: var(--color-signal-teal);
}

.node-panel-body[data-kind="task"] {
  border-left-color: var(--color-fog);
}

.panel-badges {
  display: flex;
  align-items: center;
  flex-wrap: wrap;
  gap: var(--spacing-8);
}

.kind-pill {
  font-size: var(--text-micro);
  font-weight: var(--weight-medium);
  text-transform: uppercase;
  letter-spacing: 0.06em;
  color: var(--text-muted);
  border: 1px solid var(--border-hairline);
  border-radius: var(--radius-pills);
  padding: 2px 8px;
}

.panel-title {
  margin: 0;
  font-size: var(--text-body-sm);
  font-weight: var(--weight-medium);
  color: var(--text-primary);
  line-height: 1.3;
}

.panel-title[data-out-of-scope="true"] {
  text-decoration: line-through;
  color: var(--text-faint);
}

.panel-field {
  display: flex;
  flex-direction: column;
  gap: 4px;
}

.panel-field p {
  margin: 0;
  font-size: var(--text-caption);
  color: var(--text-body);
  line-height: 1.5;
  white-space: pre-wrap;
}

.chip-row {
  display: flex;
  align-items: center;
  flex-wrap: wrap;
  gap: 6px;
}

.chip-link {
  cursor: pointer;
}

.chip-link:hover {
  color: var(--text-primary);
  border-color: var(--border-strong);
}

/* Prose cards -------------------------------------------------------------- */
.prose-row {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(240px, 1fr));
  gap: var(--spacing-16);
  margin-top: var(--spacing-24);
}

.prose-card {
  padding: var(--spacing-12) var(--spacing-16);
  border: 1px solid var(--border-hairline);
  border-radius: var(--radius-cards);
  background: var(--surface-carbon);
}

.prose-card h2 {
  display: flex;
  align-items: center;
  gap: 6px;
  margin: 0 0 var(--spacing-8);
  font-size: var(--text-label);
  font-weight: var(--weight-medium);
  text-transform: uppercase;
  letter-spacing: 0.04em;
  color: var(--text-faint);
}

.prose-card p {
  margin: 0;
  font-size: var(--text-caption);
  color: var(--text-body);
  line-height: 1.5;
  white-space: pre-wrap;
}

.loading-stack {
  display: flex;
  flex-direction: column;
  gap: var(--spacing-16);
}

.sk-title {
  height: 28px;
  width: 280px;
}

.sk-block {
  height: 360px;
}
</style>
