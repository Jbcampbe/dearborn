<script setup lang="ts">
import { computed, onBeforeUnmount, onMounted, reactive, ref } from "vue";
import { RouterLink } from "vue-router";

import { useAuthStore } from "../stores/auth";
import { ApiError } from "../api/client";
import { getBoard, setEpicLane, type EpicLane } from "../api/board";
import type { Epic } from "../api/epics";
import type { Task } from "../api/tasks";
import { hydrateBoard, initialBoardState, type BoardState } from "../board/stream";
import { useBoardStream, type StreamStatus } from "../board/useBoardStream";

// Project-detail kanban (T-401). Loads the project board (epics + standalone
// tasks), subscribes to `project:<id>` for live `board_updated` frames, and
// renders a lane-based kanban. Each epic card has a lane-move control limited
// to the permitted transitions; the WS frame drives re-render after a move.
// Standalone tasks map to lanes by status but have no lane-move control in
// T-401.
const props = defineProps<{ id: string }>();

const auth = useAuthStore();
const state = reactive<BoardState>(initialBoardState());
const loading = ref(true);
const error = ref<string | null>(null);
const streamStatus = ref<StreamStatus>("connecting");

let stream: ReturnType<typeof useBoardStream> | null = null;
onBeforeUnmount(() => stream?.close());

/** Lane definitions: stored key → display label. Order = column order. */
const LANES: { key: EpicLane; label: string }[] = [
  { key: "Planning", label: "Planning" },
  { key: "Ready", label: "Ready" },
  { key: "InProgress", label: "In Progress" },
  { key: "Completed", label: "Completed" },
  { key: "Cancelled", label: "Cancelled" },
  { key: "Blocked", label: "Blocked" },
];

/** Permitted `current → target` transitions (must match the server table). */
const PERMITTED_TRANSITIONS: Record<string, EpicLane[]> = {
  Planning: ["Cancelled"],
  Ready: ["InProgress", "Cancelled"],
  InProgress: ["Cancelled", "Blocked"],
  Blocked: ["Ready", "Cancelled"],
  Completed: [],
  Cancelled: [],
};

/** Map a standalone task's status to a lane key. */
function taskLane(task: Task): EpicLane {
  switch (task.status) {
    case "Todo":
      return "Ready";
    case "InProgress":
      return "InProgress";
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
  return PERMITTED_TRANSITIONS[currentStatus] ?? [];
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
    const board = await getBoard(token, props.id);
    hydrateBoard(state, board);
    state.projectId = props.id;
    stream = useBoardStream(props.id, token, state, streamStatus);
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
  const token = auth.token;
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

onMounted(load);
</script>

<template>
  <section class="kanban">
    <div class="kanban-head">
      <h2>Board</h2>
      <span class="conn" :data-status="streamStatus">{{ streamStatus === "open" ? "live" : streamStatus }}</span>
    </div>

    <p v-if="loading">Loading board…</p>
    <p v-else-if="error" class="error" role="alert">{{ error }}</p>

    <div v-else class="lanes">
      <div v-for="lane in LANES" :key="lane.key" class="lane" :data-lane="lane.key">
        <h3>{{ lane.label }}</h3>

        <template v-if="epicsByLane[lane.key]?.length || tasksByLane[lane.key]?.length">
          <div v-for="epic in epicsByLane[lane.key]" :key="epic.id" class="card epic-card">
            <div class="card-links">
              <RouterLink
                class="card-title"
                :to="{ name: 'epic-planning', params: { id: epic.id } }"
              >
                {{ epic.title }}
              </RouterLink>
              <RouterLink
                class="board-link"
                :to="{ name: 'epic-board', params: { id: epic.id } }"
              >
                Board
              </RouterLink>
            </div>
            <span class="tag">Epic</span>
            <div v-if="permittedTargets(epic.status).length" class="lane-move">
              <select
                :value="epic.status"
                @change="moveLane(epic, ($event.target as HTMLSelectElement).value as EpicLane)"
              >
                <option :value="epic.status" disabled>{{ epic.status }}</option>
                <option v-for="t in permittedTargets(epic.status)" :key="t" :value="t">{{ t }}</option>
              </select>
            </div>
          </div>

          <div v-for="task in tasksByLane[lane.key]" :key="task.id" class="card task-card">
            <span class="card-title">{{ task.title }}</span>
            <span class="tag task-tag">Task</span>
          </div>
        </template>

        <p v-else class="empty-lane">—</p>
      </div>
    </div>
  </section>
</template>

<style scoped>
.kanban {
  margin-top: 2rem;
}
.kanban-head {
  display: flex;
  align-items: center;
  gap: 0.75rem;
}
.kanban-head h2 {
  margin: 0;
}
.conn {
  font-size: 0.75rem;
  color: #6b7280;
}
.conn[data-status="open"] {
  color: #059669;
}
.error {
  color: #b91c1c;
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
.epic-card {
  border-left: 3px solid #6366f1;
}
.task-card {
  border-left: 3px solid #f59e0b;
}
.card-title {
  font-weight: 600;
  font-size: 0.9rem;
  text-decoration: none;
  color: #1f2937;
}
.card-title:hover {
  text-decoration: underline;
}
.card-links {
  display: flex;
  align-items: baseline;
  gap: 0.5rem;
}
.board-link {
  font-size: 0.75rem;
  color: #2563eb;
  text-decoration: none;
}
.board-link:hover {
  text-decoration: underline;
}
.tag {
  display: inline-block;
  margin-left: 0.4rem;
  font-size: 0.65rem;
  padding: 0.05rem 0.4rem;
  border-radius: 999px;
  background: #eef2ff;
  color: #3730a3;
}
.task-tag {
  background: #fef3c7;
  color: #92400e;
}
.lane-move {
  margin-top: 0.35rem;
}
.lane-move select {
  font: inherit;
  font-size: 0.8rem;
  padding: 0.15rem 0.3rem;
  border: 1px solid #d1d5db;
  border-radius: 6px;
  width: 100%;
}
</style>
