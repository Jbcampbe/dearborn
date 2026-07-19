<script setup lang="ts">
import { computed, onBeforeUnmount, onMounted, reactive, ref } from "vue";
import { RouterLink } from "vue-router";

import { useAuthStore } from "../stores/auth";
import { ApiError } from "../api/client";
import { getEpic } from "../api/epics";
import type { DagNode } from "../api/tasks";
import { getDag } from "../api/tasks";
import {
  hydrateDag,
  initialDagState,
  nodeById,
  type DagState,
} from "../dag/stream";
import { useDagStream, type StreamStatus } from "../dag/useDagStream";
import {
  readinessLabel,
  tasksByStatus,
  TASK_LANES,
} from "../board/epicLanes";
import AppIcon from "./AppIcon.vue";
import StatusIcon from "./StatusIcon.vue";

// Epic-detail task kanban (T-402). A different *view* of the same `DagState` the
// DAG editor (T-303) uses: it loads an epic's task DAG via `GET /epics/:id/dag`
// and subscribes to `epic:<id>` for live `dag_updated`/`epic_updated` frames, so
// the lanes re-render live as tasks change (e.g. the T-403 stub worker moves
// cards Todo→InProgress→Done in real time). It reuses the existing reducer
// (`dag/stream.ts`) + WS composable (`dag/useDagStream.ts`) unchanged — the only
// board-specific logic is the pure `tasksByStatus` grouping helper. No manual
// status edits here; hand-edits happen in the DAG editor (`/epic/:id/tasks`).
const props = defineProps<{ id: string }>();

const auth = useAuthStore();
const state = reactive<DagState>(initialDagState());
const loading = ref(true);
const error = ref<string | null>(null);
const streamStatus = ref<StreamStatus>("connecting");

let stream: ReturnType<typeof useDagStream> | null = null;
onBeforeUnmount(() => stream?.close());

const epic = computed(() => state.epic);
const nodes = computed(() => state.nodes);

/** Task nodes grouped by status lane (always all five lanes present). */
const byLane = computed<Record<string, DagNode[]>>(() => tasksByStatus(nodes.value));

function titleOf(id: string): string {
  return nodeById(state, id)?.title ?? id.slice(0, 6);
}

function blockerTitles(node: DagNode): string[] {
  return node.blocked_by.map((b) => titleOf(b));
}

function snippet(text: string | null, max = 80): string | null {
  if (!text) return null;
  return text.length > max ? `${text.slice(0, max)}…` : text;
}

function bounceIfAuth(err: unknown): boolean {
  if (err instanceof ApiError && err.isAuth) {
    auth.logout(`Token rejected (401): ${err.message}. Please re-enter it.`);
    return true;
  }
  return false;
}

async function load() {
  const token = auth.token;
  if (token === null) {
    return;
  }
  loading.value = true;
  error.value = null;
  try {
    const [epicObj, dag] = await Promise.all([
      getEpic(token, props.id),
      getDag(token, props.id),
    ]);
    hydrateDag(state, epicObj, dag);
    stream = useDagStream(props.id, token, state, streamStatus);
  } catch (err) {
    if (bounceIfAuth(err)) {
      return;
    }
    error.value = err instanceof Error ? err.message : "failed to load the board";
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
        <RouterLink :to="{ name: 'epic-planning', params: { id: props.id } }">Planning</RouterLink>
        <span class="sep">/</span>
        <RouterLink :to="{ name: 'epic-dag', params: { id: props.id } }">DAG</RouterLink>
      </template>
      <span class="sep">/</span>
      <span class="current">Board</span>
    </nav>

    <div v-if="loading" class="lanes-skeleton" aria-label="Loading board">
      <div v-for="i in 5" :key="i" class="skeleton sk-lane" />
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
            <span v-if="epic.status === 'InProgress'" class="worker-hint">
              <span class="worker-dot" />
              worker running
            </span>
          </div>
        </div>
        <div class="head-side">
          <span class="conn" :data-status="streamStatus">{{ streamStatus === "open" ? "live" : streamStatus }}</span>
          <RouterLink class="btn btn-ghost" :to="{ name: 'epic-dag', params: { id: props.id } }">
            <AppIcon name="diagram" :size="13" />
            Edit DAG
          </RouterLink>
        </div>
      </header>

      <p v-if="error" class="banner banner-error" role="alert">{{ error }}</p>

      <div v-if="nodes.length === 0" class="empty-state">
        <AppIcon name="board" :size="20" />
        <p>No tasks yet. Break the epic down from the planning view.</p>
      </div>

      <div v-else class="lanes">
        <div v-for="lane in TASK_LANES" :key="lane.key" class="lane" :data-lane="lane.key">
          <header class="lane-head">
            <StatusIcon :status="lane.key" :size="13" />
            <h3>{{ lane.label }}</h3>
            <span class="lane-count">{{ byLane[lane.key]?.length ?? 0 }}</span>
          </header>

          <div class="lane-body">
            <div v-for="n in byLane[lane.key]" :key="n.id" class="card card-interactive task-card" :data-status="n.status">
              <div class="card-head">
                <span class="card-title">{{ n.title }}</span>
                <span class="badge" :data-tone="n.status === 'Todo' && n.ready ? 'green' : 'neutral'">
                  {{ readinessLabel(n) }}
                </span>
              </div>
              <p v-if="snippet(n.acceptance)" class="card-acc">{{ snippet(n.acceptance) }}</p>
              <div v-if="n.status === 'Todo' && n.blocked_by.length" class="card-blockers">
                <span class="blockers-label">Blocked by</span>
                <span v-for="title in blockerTitles(n)" :key="title" class="chip">{{ title }}</span>
              </div>
            </div>
            <p v-if="!byLane[lane.key]?.length" class="empty-lane">No tasks</p>
          </div>
        </div>
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

.worker-hint {
  display: inline-flex;
  align-items: center;
  gap: 6px;
  font-size: var(--text-label);
  color: var(--color-signal-teal);
}

.worker-dot {
  width: 6px;
  height: 6px;
  border-radius: var(--radius-pills);
  background: var(--color-signal-teal);
  animation: pulse-dot 1.2s ease-in-out infinite;
}

.head-side {
  display: flex;
  align-items: center;
  gap: var(--spacing-16);
  flex-shrink: 0;
}

.lanes {
  display: flex;
  gap: var(--spacing-12);
  overflow-x: auto;
  padding-bottom: var(--spacing-8);
  align-items: flex-start;
}

.lane {
  flex: 0 0 264px;
  display: flex;
  flex-direction: column;
  max-height: 72vh;
  border-radius: var(--radius-cards);
  background: rgba(255, 255, 255, 0.015);
  border: 1px solid var(--border-hairline);
}

.lane-head {
  display: flex;
  align-items: center;
  gap: var(--spacing-8);
  padding: 10px var(--spacing-12);
  border-bottom: 1px solid var(--border-hairline);
}

.lane-head h3 {
  font-size: var(--text-caption);
  font-weight: var(--weight-medium);
  color: var(--text-body);
}

.lane-count {
  margin-left: auto;
  font-size: var(--text-label);
  color: var(--text-faint);
}

.lane-body {
  display: flex;
  flex-direction: column;
  gap: var(--spacing-8);
  padding: var(--spacing-8);
  overflow-y: auto;
  min-height: 72px;
}

.task-card {
  display: flex;
  flex-direction: column;
  gap: 6px;
  padding: 10px var(--spacing-12);
}

.card-head {
  display: flex;
  align-items: flex-start;
  justify-content: space-between;
  gap: var(--spacing-8);
}

.card-title {
  font-size: var(--text-caption);
  font-weight: var(--weight-regular);
  color: var(--text-primary);
  line-height: 1.4;
}

.card-acc {
  font-size: var(--text-label);
  color: var(--text-faint);
  line-height: 1.45;
}

.card-blockers {
  display: flex;
  align-items: center;
  flex-wrap: wrap;
  gap: 6px;
}

.blockers-label {
  font-size: 10px;
  font-weight: var(--weight-medium);
  color: var(--text-faint);
  text-transform: uppercase;
  letter-spacing: 0.04em;
}

.empty-lane {
  padding: var(--spacing-16) 0;
  text-align: center;
  font-size: var(--text-label);
  color: var(--text-faint);
}

.lanes-skeleton {
  display: flex;
  gap: var(--spacing-12);
  overflow: hidden;
}

.sk-lane {
  flex: 0 0 264px;
  height: 280px;
}
</style>
