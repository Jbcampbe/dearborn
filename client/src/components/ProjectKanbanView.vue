<script setup lang="ts">
import { computed, onBeforeUnmount, onMounted, reactive, ref } from "vue";
import { RouterLink } from "vue-router";

import { useAuthStore } from "../stores/auth";
import { ApiError } from "../api/client";
import { getBoard, setEpicLane, type EpicLane, type EpicProgress } from "../api/board";
import type { Epic } from "../api/epics";
import { patchTask, retryTask, runTask, type Task, type TaskStatus } from "../api/tasks";
import { hydrateBoard, initialBoardState, type BoardState } from "../board/stream";
import { useBoardStream, type StreamStatus } from "../board/useBoardStream";
import {
  canDropOnProjectLane,
  permittedEpicTargets,
  taskStatusForLane,
  type DragKind,
} from "../board/dnd";
import {
  canCancelEpic,
  canRetryTask,
  canRunTask,
  describeControlError,
  describeFailureReason,
  prLabel,
  showPrLink,
} from "../board/controls";
import AppIcon from "./AppIcon.vue";
import StatusIcon from "./StatusIcon.vue";
import TaskModal from "./TaskModal.vue";

// Project-detail kanban (T-401). Loads the project board (epics + standalone
// tasks), subscribes to `project:<id>` for live `board_updated` frames, and
// renders a lane-based kanban. Each epic card has a lane-move control limited
// to the permitted transitions; the WS frame drives re-render after a move.
// Standalone tasks map to lanes by status; they have no lane-move control,
// but are created (via the page header's `+ New` menu, through the exposed
// `openCreateTask`) and edited (click a card) through the TaskModal.
//
// T-561 control surface: a `Failed` task card (standalone here, or an
// epic-scoped one on `EpicKanbanView.vue`) gets a Retry button
// (`POST /tasks/{id}/retry`); a standalone `Todo` card gets a Run button
// (`POST /tasks/{id}/run`); an `InProgress` epic card gets a Cancel button
// (the existing `POST /epics/{id}/lane` → `Cancelled`, which T-542 wires to
// an actual kill server-side — nothing new to call here). `Completed`/`Done`
// and `InReview` epics/tasks with a `pr_url` get a PR link (the §4 review
// loop attaches `pr_url` when the item lands in `InReview`, and it survives
// through merge). Every control
// only fires the request and reports a `409`/other failure via
// `board/controls.ts`'s `describeControlError` — it never mutates local
// state or refetches the board; the existing `board_updated`/`epic_updated`
// WS frame these endpoints already publish is what drives the re-render.
const props = defineProps<{ id: string }>();

const auth = useAuthStore();
const state = reactive<BoardState>(initialBoardState());
const loading = ref(true);
const error = ref<string | null>(null);
const streamStatus = ref<StreamStatus>("connecting");

/** Task-modal state: open flag + the task being edited (null = create mode). */
const taskModalOpen = ref(false);
const editingTask = ref<Task | null>(null);

function openCreateTask() {
  editingTask.value = null;
  taskModalOpen.value = true;
}

function openEditTask(task: Task) {
  editingTask.value = task;
  taskModalOpen.value = true;
}

// The project header's `+ New → Task` menu item opens the create dialog from
// outside this component (the modal lives here so card click-to-edit and
// create share one instance).
defineExpose({ openCreateTask });

let stream: ReturnType<typeof useBoardStream> | null = null;
onBeforeUnmount(() => stream?.close());

/** Lane definitions: stored key → display label. Order = column order. */
const LANES: { key: EpicLane; label: string }[] = [
  { key: "Planning", label: "Planning" },
  { key: "Ready", label: "Ready" },
  { key: "InProgress", label: "In Progress" },
  { key: "InReview", label: "In Review" },
  { key: "Completed", label: "Completed" },
  { key: "Cancelled", label: "Cancelled" },
  { key: "Blocked", label: "Blocked" },
];

/** Permitted `current → target` transitions live in `board/dnd.ts` (shared
 * with drag-and-drop); the lane-move select below offers the same set. */

/** Map a standalone task's status to a lane key. */
function taskLane(task: Task): EpicLane {
  switch (task.status) {
    case "Todo":
      return "Ready";
    case "InProgress":
      return "InProgress";
    case "InReview":
      return "InReview";
    case "Done":
      return "Completed";
    case "Failed":
      return "Blocked";
    case "Cancelled":
      return "Cancelled";
    default:
      return "Ready";
  }
}

const epics = computed(() => state.epics);
const tasks = computed(() => state.tasks);

/** Epics grouped by lane. */
const epicsByLane = computed<Record<string, Epic[]>>(() => {
  const map: Record<string, Epic[]> = {};
  for (const lane of LANES) {
    map[lane.key] = [];
  }
  for (const epic of epics.value) {
    const lane = (LANES.find((l) => l.key === epic.status)?.key ?? "Planning") as EpicLane;
    map[lane]?.push(epic);
  }
  return map;
});

/** Standalone tasks grouped by lane (mapped from task status). */
const tasksByLane = computed<Record<string, Task[]>>(() => {
  const map: Record<string, Task[]> = {};
  for (const lane of LANES) {
    map[lane.key] = [];
  }
  for (const task of tasks.value) {
    map[taskLane(task)]?.push(task);
  }
  return map;
});

function permittedTargets(currentStatus: string): EpicLane[] {
  return permittedEpicTargets(currentStatus);
}

/** Task progress per epic id, for the epic cards' done/total badge. */
const progressByEpic = computed<Record<string, EpicProgress>>(() => {
  const map: Record<string, EpicProgress> = {};
  for (const p of state.epicProgress) {
    map[p.epic_id] = p;
  }
  return map;
});

function progressOf(epicId: string): EpicProgress | null {
  const p = progressByEpic.value[epicId];
  return p && p.total > 0 ? p : null;
}

function snippet(text: string | null, max = 100): string | null {
  if (!text) return null;
  return text.length > max ? `${text.slice(0, max)}…` : text;
}

/* --- Drag and drop --------------------------------------------------------
 * Native HTML5 DnD; drop rules live in `board/dnd.ts`. The dragged card is
 * kept in component state (dragover handlers can't read dataTransfer data),
 * and `dropLane` tracks the highlighted valid drop target. Mutations reuse
 * the same REST calls as the select/click paths — the `board_updated` frame
 * drives the re-render, so there is no optimistic state to roll back.
 */
interface DragPayload {
  kind: DragKind;
  id: string;
  status: string;
}
const dragPayload = ref<DragPayload | null>(null);
const dropLane = ref<EpicLane | null>(null);

function onDragStart(kind: DragKind, card: Epic | Task, event: DragEvent) {
  dragPayload.value = { kind, id: card.id, status: card.status };
  event.dataTransfer?.setData("text/plain", card.id);
  if (event.dataTransfer) {
    event.dataTransfer.effectAllowed = "move";
  }
}

function onDragEnd() {
  dragPayload.value = null;
  dropLane.value = null;
}

/** Whether the current drag may drop on `lane` (drives highlight + cursor). */
function laneAccepts(lane: EpicLane): boolean {
  const drag = dragPayload.value;
  return drag !== null && canDropOnProjectLane(drag.kind, drag.status, lane);
}

function onLaneDragOver(lane: EpicLane, event: DragEvent) {
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

async function onLaneDrop(lane: EpicLane, event: DragEvent) {
  event.preventDefault();
  const drag = dragPayload.value;
  onDragEnd();
  const token = auth.accessToken;
  if (drag === null || token === null || !canDropOnProjectLane(drag.kind, drag.status, lane)) {
    return;
  }
  error.value = null;
  try {
    if (drag.kind === "epic") {
      await setEpicLane(token, drag.id, lane);
    } else {
      const status = taskStatusForLane(lane);
      if (status !== null && status !== drag.status) {
        await patchTask(token, drag.id, { status: status as TaskStatus });
      }
    }
    // The board_updated WS frame drives the re-render.
  } catch (err) {
    if (bounceIfAuth(err)) {
      return;
    }
    error.value = err instanceof Error ? err.message : "failed to move the card";
  }
}

function laneLabel(key: string): string {
  return LANES.find((l) => l.key === key)?.label ?? key;
}

function bounceIfAuth(err: unknown): boolean {
  if (err instanceof ApiError && err.isAuth) {
    auth.logout(`Token rejected (401): ${err.message}. Please re-enter it.`);
    return true;
  }
  return false;
}

async function load() {
  const token = auth.accessToken;
  if (token === null) {
    return;
  }
  loading.value = true;
  error.value = null;
  try {
    const board = await getBoard(token, props.id);
    hydrateBoard(state, board);
    state.projectId = props.id;
    stream = useBoardStream(props.id, () => auth.ensureFresh(), state, streamStatus);
  } catch (err) {
    if (bounceIfAuth(err)) {
      return;
    }
    error.value = err instanceof Error ? err.message : "failed to load the board";
  } finally {
    loading.value = false;
  }
}

async function moveLane(epic: Epic, target: EpicLane) {
  const token = auth.accessToken;
  if (token === null) {
    return;
  }
  error.value = null;
  try {
    await setEpicLane(token, epic.id, target);
    // The board_updated WS frame will drive the re-render.
  } catch (err) {
    if (bounceIfAuth(err)) {
      return;
    }
    error.value = err instanceof Error ? err.message : "failed to move epic";
  }
}

/* --- T-561 control surface: Retry / Run / Cancel --------------------------
 * `busyIds` tracks in-flight control calls by card id so a button disables
 * itself rather than allowing a double-click to fire two requests; it is not
 * used to predict the outcome — the actual state change always arrives over
 * the WS frame the endpoint publishes, never from this local set.
 */
const busyIds = reactive(new Set<string>());

async function retryCard(task: Task) {
  const token = auth.accessToken;
  if (token === null || busyIds.has(task.id)) {
    return;
  }
  busyIds.add(task.id);
  error.value = null;
  try {
    await retryTask(token, task.id);
    // dag_updated/epic_updated/board_updated (T-541) drive the re-render.
  } catch (err) {
    if (bounceIfAuth(err)) {
      return;
    }
    error.value = describeControlError("retry", err);
  } finally {
    busyIds.delete(task.id);
  }
}

async function runCard(task: Task) {
  const token = auth.accessToken;
  if (token === null || busyIds.has(task.id)) {
    return;
  }
  busyIds.add(task.id);
  error.value = null;
  try {
    await runTask(token, task.id);
    // board_updated (T-551) drives the re-render.
  } catch (err) {
    if (bounceIfAuth(err)) {
      return;
    }
    error.value = describeControlError("run", err);
  } finally {
    busyIds.delete(task.id);
  }
}

async function cancelEpic(epic: Epic) {
  const token = auth.accessToken;
  if (token === null || busyIds.has(epic.id)) {
    return;
  }
  busyIds.add(epic.id);
  error.value = null;
  try {
    await setEpicLane(token, epic.id, "Cancelled");
    // epic_updated/board_updated drive the re-render; T-542 handles the kill.
  } catch (err) {
    if (bounceIfAuth(err)) {
      return;
    }
    error.value = describeControlError("cancel", err);
  } finally {
    busyIds.delete(epic.id);
  }
}

onMounted(load);
</script>

<template>
  <section class="kanban">
    <div class="section-head">
      <h2>Board</h2>
      <span class="conn" :data-status="streamStatus">{{ streamStatus === "open" ? "live" : streamStatus }}</span>
    </div>

    <div v-if="loading" class="lanes-skeleton" aria-label="Loading board">
      <div v-for="i in 4" :key="i" class="skeleton sk-lane" />
    </div>
    <p v-else-if="error" class="banner banner-error" role="alert">{{ error }}</p>

    <div v-else class="lanes fade-in">
      <div
        v-for="lane in LANES"
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
          <span class="lane-count">
            {{ (epicsByLane[lane.key]?.length ?? 0) + (tasksByLane[lane.key]?.length ?? 0) }}
          </span>
        </header>

        <div class="lane-body">
          <div
            v-for="epic in epicsByLane[lane.key]"
            :key="epic.id"
            class="card card-interactive epic-card"
            :class="{ dragging: dragPayload?.id === epic.id }"
            draggable="true"
            @dragstart="onDragStart('epic', epic, $event)"
            @dragend="onDragEnd"
          >
            <RouterLink
              class="card-title"
              :to="{ name: 'epic-planning', params: { id: epic.id } }"
            >
              {{ epic.title }}
            </RouterLink>
            <p v-if="snippet(epic.description)" class="card-desc">{{ snippet(epic.description) }}</p>
            <p v-if="epic.status === 'Blocked' && describeFailureReason(epic.blocked_reason)" class="card-reason">
              {{ describeFailureReason(epic.blocked_reason) }}
            </p>
            <div v-if="progressOf(epic.id)" class="card-progress">
              <span class="progress-track">
                <span
                  class="progress-fill"
                  :style="{ width: `${(progressOf(epic.id)!.done / progressOf(epic.id)!.total) * 100}%` }"
                />
              </span>
              <span class="progress-label">
                {{ progressOf(epic.id)!.done }} / {{ progressOf(epic.id)!.total }} tasks
              </span>
            </div>
            <div class="card-foot">
              <span class="badge">
                <StatusIcon :status="epic.status" :size="11" />
                Epic
              </span>
              <RouterLink
                class="card-open"
                :to="{ name: 'epic-board', params: { id: epic.id } }"
              >
                Board
              </RouterLink>
              <a
                v-if="showPrLink(epic.status, epic.pr_url)"
                class="card-open pr-link"
                :href="epic.pr_url!"
                target="_blank"
                rel="noopener noreferrer"
                @click.stop
              >
                <AppIcon name="link" :size="11" />
                {{ prLabel(epic.pr_number) }}
              </a>
              <button
                v-if="canCancelEpic(epic)"
                class="btn btn-sm btn-danger control-btn"
                type="button"
                :disabled="busyIds.has(epic.id)"
                @click.stop="cancelEpic(epic)"
              >
                Cancel
              </button>
              <select
                v-if="permittedTargets(epic.status).length"
                class="lane-move select"
                :value="epic.status"
                aria-label="Move epic to lane"
                @change="moveLane(epic, ($event.target as HTMLSelectElement).value as EpicLane)"
              >
                <option :value="epic.status" disabled>Move to…</option>
                <option v-for="t in permittedTargets(epic.status)" :key="t" :value="t">
                  {{ laneLabel(t) }}
                </option>
              </select>
            </div>
          </div>

          <div
            v-for="task in tasksByLane[lane.key]"
            :key="task.id"
            class="card card-interactive task-card"
            :class="{ dragging: dragPayload?.id === task.id }"
            role="button"
            tabindex="0"
            draggable="true"
            @dragstart="onDragStart('task', task, $event)"
            @dragend="onDragEnd"
            @click="openEditTask(task)"
            @keydown.enter="openEditTask(task)"
          >
            <span class="card-title">{{ task.title }}</span>
            <p v-if="task.status === 'Failed' && describeFailureReason(task.failure_reason)" class="card-reason">
              {{ describeFailureReason(task.failure_reason) }}
            </p>
            <div class="card-foot">
              <span class="badge">
                <StatusIcon :status="task.status" :size="11" />
                Task
              </span>
              <a
                v-if="showPrLink(task.status, task.pr_url)"
                class="card-open pr-link"
                :href="task.pr_url!"
                target="_blank"
                rel="noopener noreferrer"
                @click.stop
              >
                <AppIcon name="link" :size="11" />
                {{ prLabel(task.pr_number) }}
              </a>
              <button
                v-if="canRunTask(task)"
                class="btn btn-sm btn-ghost control-btn"
                type="button"
                :disabled="busyIds.has(task.id)"
                @click.stop="runCard(task)"
              >
                Run
              </button>
              <button
                v-if="canRetryTask(task)"
                class="btn btn-sm btn-ghost control-btn"
                type="button"
                :disabled="busyIds.has(task.id)"
                @click.stop="retryCard(task)"
              >
                Retry
              </button>
            </div>
          </div>

          <p v-if="!epicsByLane[lane.key]?.length && !tasksByLane[lane.key]?.length" class="empty-lane">
            No cards
          </p>
        </div>
      </div>
    </div>

    <TaskModal
      :open="taskModalOpen"
      :project-id="props.id"
      :task="editingTask"
      @close="taskModalOpen = false"
    />
  </section>
</template>

<style scoped>
.kanban {
  margin-bottom: var(--spacing-32);
}

.section-head {
  display: flex;
  align-items: center;
  gap: var(--spacing-12);
  margin-bottom: var(--spacing-12);
}

.section-head h2 {
  font-size: var(--text-body-sm);
  font-weight: var(--weight-medium);
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
  max-height: 70vh;
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

.epic-card,
.task-card {
  display: flex;
  flex-direction: column;
  gap: var(--spacing-8);
  padding: 10px var(--spacing-12);
}

.card-title {
  font-size: var(--text-caption);
  font-weight: var(--weight-regular);
  color: var(--text-primary);
  line-height: 1.4;
}

.card-foot {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: var(--spacing-8);
}

.lane-move {
  width: auto;
  padding: 2px 22px 2px 8px;
  font-size: 11.5px;
  line-height: 1.5;
  background-position: right 6px center;
  opacity: 0;
  transition:
    opacity var(--duration-fast) var(--ease-out),
    border-color var(--duration-fast) var(--ease-out);
}

.card-open {
  font-size: 11.5px;
  color: var(--text-faint);
  opacity: 0;
  transition:
    opacity var(--duration-fast) var(--ease-out),
    color var(--duration-fast) var(--ease-out);
}

.card-open:hover {
  color: var(--text-primary);
}

.epic-card:hover .lane-move,
.epic-card:hover .card-open,
.lane-move:focus,
.card-open:focus {
  opacity: 1;
}

/* --- T-561 control surface ---------------------------------------------- *
 * Unlike `.lane-move`/`.card-open` (revealed on hover — secondary), a
 * failure reason and the Retry/Run/Cancel controls are the point of looking
 * at a Failed/Todo/InProgress card, so they render at full opacity always. */

.card-reason {
  font-size: var(--text-label);
  color: var(--color-coral-red);
  line-height: 1.4;
}

.control-btn {
  flex-shrink: 0;
}

.pr-link {
  display: inline-flex;
  align-items: center;
  gap: 4px;
  opacity: 1;
}

/* --- Drag and drop ---------------------------------------------------------*/

.card[draggable="true"] {
  cursor: grab;
}

.card.dragging {
  opacity: 0.45;
  cursor: grabbing;
}

.lane.drop-target {
  border-color: var(--color-signal-teal);
  background: rgba(255, 255, 255, 0.03);
}

/* --- Epic card extras ------------------------------------------------------*/

.card-desc {
  font-size: var(--text-label);
  color: var(--text-faint);
  line-height: 1.45;
}

.card-progress {
  display: flex;
  align-items: center;
  gap: var(--spacing-8);
}

.progress-track {
  flex: 1;
  height: 3px;
  border-radius: var(--radius-pills);
  background: rgba(255, 255, 255, 0.08);
  overflow: hidden;
}

.progress-fill {
  display: block;
  height: 100%;
  border-radius: var(--radius-pills);
  background: var(--color-pulse-green);
  transition: width var(--duration-fast) var(--ease-out);
}

.progress-label {
  font-size: 11px;
  color: var(--text-faint);
  white-space: nowrap;
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
  height: 240px;
}
</style>
