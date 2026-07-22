<script setup lang="ts">
import { computed, onBeforeUnmount, onMounted, reactive, ref } from "vue";
import { RouterLink } from "vue-router";

import { useAuthStore } from "../stores/auth";
import { ApiError } from "../api/client";
import { getEpic } from "../api/epics";
import { getProject } from "../api/projects";
import type { DagNode, Task, TaskStatus } from "../api/tasks";
import { getDag, patchTask } from "../api/tasks";
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
import { canDropOnTaskLane } from "../board/dnd";
import AppIcon from "./AppIcon.vue";
import EpicTabs from "./EpicTabs.vue";
import StatusIcon from "./StatusIcon.vue";
import TaskModal from "./TaskModal.vue";

// Epic-detail task kanban (T-402). A different *view* of the same `DagState` the
// DAG editor (T-303) uses: it loads an epic's task DAG via `GET /epics/:id/dag`
// and subscribes to `epic:<id>` for live `dag_updated`/`epic_updated` frames, so
// the lanes re-render live as tasks change (e.g. the T-403 stub worker moves
// cards Todo→InProgress→Done in real time). It reuses the existing reducer
// (`dag/stream.ts`) + WS composable (`dag/useDagStream.ts`) unchanged — the only
// board-specific logic is the pure `tasksByStatus` grouping helper. Cards are
// clickable (opening the shared TaskModal editor, like the project board's
// standalone tasks) and draggable between lanes to change status — except
// cards in In Progress, whose transitions the worker owns (`board/dnd.ts`).
const props = defineProps<{ id: string }>();

const auth = useAuthStore();
const state = reactive<DagState>(initialDagState());
const loading = ref(true);
const error = ref<string | null>(null);
const streamStatus = ref<StreamStatus>("connecting");
// The breadcrumb's project name (the epic only carries `project_id`); fills in
// after load and falls back to "…" if the fetch fails.
const projectName = ref<string | null>(null);

/** Task-modal state: open flag + the task being edited (a DagNode is a Task). */
const taskModalOpen = ref(false);
const editingTask = ref<Task | null>(null);

function openEditTask(node: DagNode) {
  editingTask.value = node;
  taskModalOpen.value = true;
}

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

/* --- Drag and drop --------------------------------------------------------
 * Native HTML5 DnD between the status lanes; the rules live in
 * `board/dnd.ts` (any move except dragging a task out of In Progress — the
 * worker owns those). A drop PATCHes the task's status; the `dag_updated`
 * frame drives the re-render, so there is no optimistic state to roll back.
 */
const dragPayload = ref<{ id: string; status: TaskStatus } | null>(null);
const dropLane = ref<TaskStatus | null>(null);

function onDragStart(node: DagNode, event: DragEvent) {
  dragPayload.value = { id: node.id, status: node.status };
  event.dataTransfer?.setData("text/plain", node.id);
  if (event.dataTransfer) {
    event.dataTransfer.effectAllowed = "move";
  }
}

function onDragEnd() {
  dragPayload.value = null;
  dropLane.value = null;
}

function laneAccepts(lane: TaskStatus): boolean {
  const drag = dragPayload.value;
  return drag !== null && canDropOnTaskLane(drag.status, lane);
}

function onLaneDragOver(lane: TaskStatus, event: DragEvent) {
  if (!laneAccepts(lane)) {
    return; // no preventDefault -> the lane rejects the drop (not-allowed cursor)
  }
  event.preventDefault();
  if (event.dataTransfer) {
    event.dataTransfer.dropEffect = "move";
  }
  dropLane.value = lane;
}

function onLaneDragLeave(event: DragEvent) {
  // Only clear the highlight when the pointer truly leaves the lane (entering
  // a child card fires dragleave on the lane too).
  const related = event.relatedTarget as Node | null;
  if (!related || !(event.currentTarget as HTMLElement).contains(related)) {
    dropLane.value = null;
  }
}

async function onLaneDrop(lane: TaskStatus, event: DragEvent) {
  event.preventDefault();
  const drag = dragPayload.value;
  onDragEnd();
  const token = auth.token;
  if (drag === null || token === null || !canDropOnTaskLane(drag.status, lane)) {
    return;
  }
  if (drag.status === lane) {
    return; // dropped back on its own lane — no-op
  }
  error.value = null;
  try {
    await patchTask(token, drag.id, { status: lane });
    // The dag_updated WS frame drives the re-render.
  } catch (err) {
    if (bounceIfAuth(err)) {
      return;
    }
    error.value = err instanceof Error ? err.message : "failed to move the task";
  }
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
    // Non-blocking + non-fatal: the breadcrumb falls back to "…" without it.
    void getProject(token, epicObj.project_id)
      .then((p) => (projectName.value = p.name))
      .catch((err) => bounceIfAuth(err));
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
        <RouterLink :to="{ name: 'project-detail', params: { id: epic.project_id } }">
          {{ projectName ?? "…" }}
        </RouterLink>
      </template>
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
        <span class="conn" :data-status="streamStatus">{{ streamStatus === "open" ? "live" : streamStatus }}</span>
      </header>

      <EpicTabs :id="props.id" tab="board" />

      <p v-if="error" class="banner banner-error" role="alert">{{ error }}</p>

      <div v-if="nodes.length === 0" class="empty-state">
        <AppIcon name="board" :size="20" />
        <p>No tasks yet. Break the epic down from the planning view.</p>
      </div>

      <div v-else class="lanes">
        <div
          v-for="lane in TASK_LANES"
          :key="lane.key"
          class="lane"
          :class="{ 'drop-target': dropLane === lane.key }"
          :data-lane="lane.key"
          @dragover="onLaneDragOver(lane.key, $event)"
          @dragleave="onLaneDragLeave"
          @drop="onLaneDrop(lane.key, $event)"
        >
          <header class="lane-head">
            <StatusIcon :status="lane.key" :size="13" />
            <h3>{{ lane.label }}</h3>
            <span class="lane-count">{{ byLane[lane.key]?.length ?? 0 }}</span>
          </header>

          <div class="lane-body">
            <div
              v-for="n in byLane[lane.key]"
              :key="n.id"
              class="card card-interactive task-card"
              :class="{ dragging: dragPayload?.id === n.id }"
              :data-status="n.status"
              role="button"
              tabindex="0"
              :draggable="n.status !== 'InProgress'"
              @dragstart="n.status !== 'InProgress' && onDragStart(n, $event)"
              @dragend="onDragEnd"
              @click="openEditTask(n)"
              @keydown.enter="openEditTask(n)"
            >
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

    <TaskModal
      v-if="epic"
      :open="taskModalOpen"
      :project-id="epic.project_id"
      :task="editingTask"
      @close="taskModalOpen = false"
    />
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

/* --- Drag and drop ---------------------------------------------------------*/

.task-card[draggable="true"] {
  cursor: grab;
}

.task-card.dragging {
  opacity: 0.45;
  cursor: grabbing;
}

.lane.drop-target {
  border-color: var(--color-signal-teal);
  background: rgba(255, 255, 255, 0.03);
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
