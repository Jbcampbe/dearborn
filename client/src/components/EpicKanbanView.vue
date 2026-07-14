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
  <main>
    <p class="crumb">
      <RouterLink :to="{ name: 'projects' }">← Projects</RouterLink>
      <template v-if="epic">
        <span class="sep">/</span>
        <RouterLink :to="{ name: 'epic-planning', params: { id: props.id } }">Planning</RouterLink>
        <span class="sep">/</span>
        <RouterLink :to="{ name: 'epic-dag', params: { id: props.id } }">DAG</RouterLink>
      </template>
    </p>

    <p v-if="loading">Loading…</p>
    <p v-else-if="error && !epic" class="error" role="alert">{{ error }}</p>

    <template v-else-if="epic">
      <header>
        <div>
          <h1>{{ epic.title }}</h1>
          <span class="status" :data-status="epic.status">{{ epic.status }}</span>
          <span v-if="epic.status === 'InProgress'" class="worker-hint">worker running…</span>
          <span class="conn" :data-status="streamStatus">{{ streamStatus === "open" ? "live" : streamStatus }}</span>
        </div>
        <RouterLink class="edit-link" :to="{ name: 'epic-dag', params: { id: props.id } }">
          Edit DAG
        </RouterLink>
      </header>

      <p v-if="error" class="error inline" role="alert">{{ error }}</p>

      <p v-if="nodes.length === 0" class="empty">
        No tasks yet. Break the epic down from the planning view.
      </p>

      <div v-else class="lanes">
        <div v-for="lane in TASK_LANES" :key="lane.key" class="lane" :data-lane="lane.key">
          <h3>{{ lane.label }}</h3>

          <template v-if="byLane[lane.key]?.length">
            <div v-for="n in byLane[lane.key]" :key="n.id" class="card" :data-ready="n.ready" :data-status="n.status">
              <div class="card-head">
                <span class="card-title">{{ n.title }}</span>
                <span class="badge" :data-ready="n.ready">{{ readinessLabel(n) }}</span>
              </div>
              <p v-if="snippet(n.acceptance)" class="card-acc">{{ snippet(n.acceptance) }}</p>
              <p v-if="n.status === 'Todo' && n.blocked_by.length" class="card-blockers">
                <strong>Blocked by:</strong>
                <span v-for="title in blockerTitles(n)" :key="title" class="chip">{{ title }}</span>
              </p>
            </div>
          </template>

          <p v-else class="empty-lane">—</p>
        </div>
      </div>
    </template>
  </main>
</template>

<style scoped>
main {
  max-width: 96rem;
  margin: 2rem auto;
  padding: 0 1rem;
}
.crumb { margin: 0 0 1rem; }
.crumb a { color: #2563eb; text-decoration: none; }
.crumb .sep { margin: 0 0.5rem; color: #9ca3af; }
header {
  display: flex;
  align-items: flex-start;
  justify-content: space-between;
  gap: 1rem;
}
header h1 { margin: 0 0 0.3rem; }
.status {
  font-size: 0.8rem;
  padding: 0.1rem 0.5rem;
  border-radius: 999px;
  background: #eef2ff;
  color: #3730a3;
}
.status[data-status="Ready"] { background: #ecfdf5; color: #065f46; }
.status[data-status="InProgress"] { background: #fef3c7; color: #92400e; }
.conn {
  font-size: 0.75rem;
  margin-left: 0.5rem;
  color: #6b7280;
}
.conn[data-status="open"] { color: #059669; }
.worker-hint {
  font-size: 0.75rem;
  margin-left: 0.5rem;
  color: #92400e;
  font-style: italic;
}
.edit-link {
  font-size: 0.85rem;
  color: #2563eb;
  text-decoration: none;
}
.edit-link:hover { text-decoration: underline; }
.error { color: #b91c1c; }
.error.inline { margin: 1rem 0; }
.empty {
  color: #6b7280;
  padding: 2rem 1rem;
  text-align: center;
  border: 2px dashed #d1d5db;
  border-radius: 10px;
}

.lanes {
  display: flex;
  gap: 0.75rem;
  overflow-x: auto;
  padding-bottom: 0.5rem;
  margin-top: 1rem;
}
.lane {
  flex: 0 0 16rem;
  min-height: 8rem;
  border: 1px solid #e5e7eb;
  border-radius: 10px;
  background: #f9fafb;
  padding: 0.5rem 0.6rem;
}
.lane h3 {
  margin: 0 0 0.5rem;
  font-size: 0.9rem;
  color: #374151;
  border-bottom: 1px solid #e5e7eb;
  padding-bottom: 0.3rem;
}
.empty-lane {
  color: #d1d5db;
  text-align: center;
  margin: 1rem 0;
}
.card {
  border: 1px solid #e5e7eb;
  border-radius: 8px;
  background: white;
  padding: 0.5rem 0.6rem;
  margin-bottom: 0.5rem;
}
.card[data-ready="true"] { border-left: 3px solid #a7f3d0; }
.card[data-status="Done"] { border-left: 3px solid #a7f3d0; }
.card[data-status="InProgress"] { border-left: 3px solid #fbbf24; }
.card[data-status="Failed"] { border-left: 3px solid #fca5a5; }
.card[data-status="Cancelled"] { border-left: 3px solid #d1d5db; }
.card-head {
  display: flex;
  align-items: flex-start;
  justify-content: space-between;
  gap: 0.5rem;
}
.card-title {
  font-weight: 600;
  font-size: 0.9rem;
  color: #1f2937;
}
.badge {
  font-size: 0.7rem;
  padding: 0.1rem 0.5rem;
  border-radius: 999px;
  background: #f3f4f6;
  color: #374151;
  white-space: nowrap;
}
.badge[data-ready="true"] { background: #ecfdf5; color: #065f46; }
.card-acc {
  margin: 0.3rem 0 0;
  font-size: 0.8rem;
  color: #6b7280;
}
.card-blockers {
  margin: 0.3rem 0 0;
  font-size: 0.78rem;
  color: #6b7280;
}
.chip {
  display: inline-block;
  margin-left: 0.3rem;
  padding: 0.05rem 0.4rem;
  border-radius: 6px;
  background: #fef3c7;
  color: #92400e;
  font-size: 0.72rem;
}
</style>
