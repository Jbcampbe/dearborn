<script setup lang="ts">
import { computed, onBeforeUnmount, onMounted, reactive, ref } from "vue";
import { RouterLink } from "vue-router";

import { useAuthStore } from "../stores/auth";
import { ApiError } from "../api/client";
import { getEpic } from "../api/epics";
import {
  createTask,
  deleteTask,
  getDag,
  linkDependency,
  patchTask,
  unlinkDependency,
  type DagNode,
  type TaskStatus,
} from "../api/tasks";
import {
  hydrateDag,
  initialDagState,
  nodeById,
  type DagState,
} from "../dag/stream";
import { useDagStream, type StreamStatus } from "../dag/useDagStream";

// Ready-lane DAG editor (T-303). Loads an epic's task DAG, subscribes to
// `epic:<id>` for live `dag_updated`/`epic_updated` frames, and lets the user
// create/edit/delete tasks and wire/rewire dependencies by hand. Invalid edits
// (cycles, cross-epic, missing fields) are surfaced from the server's error
// envelope. This is the highest-ROI human checkpoint before execution: the
// breakdown agent's DAG is fully editable here before the epic hits In Progress.
const props = defineProps<{ id: string }>();

const auth = useAuthStore();
const state = reactive<DagState>(initialDagState());
const loading = ref(true);
const error = ref<string | null>(null);
const streamStatus = ref<StreamStatus>("connecting");

// --- create-task form ---
const newTitle = ref("");
const newDescription = ref("");
const newAcceptance = ref("");
const creating = ref(false);

// --- edit-task panel ---
const editingId = ref<string | null>(null);
const editTitle = ref("");
const editDescription = ref<string | null>(null);
const editAcceptance = ref<string | null>(null);
const editStatus = ref<TaskStatus>("Todo");
const saving = ref(false);
const deleting = ref(false);

// --- add-dependency form ---
const depBlocker = ref("");
const depBlocked = ref("");
const linking = ref(false);

let stream: ReturnType<typeof useDagStream> | null = null;
onBeforeUnmount(() => stream?.close());

const epic = computed(() => state.epic);
const nodes = computed(() => state.nodes);
const edges = computed(() => state.edges);
const isReady = computed(() => state.epic?.status === "Ready");

const statusOptions: TaskStatus[] = ["Todo", "InProgress", "Done", "Failed", "Cancelled"];

function titleOf(id: string): string {
  return nodeById(state, id)?.title ?? id.slice(0, 6);
}

function readinessLabel(n: DagNode): string {
  if (n.status !== "Todo") return n.status;
  return n.ready ? "ready" : "blocked";
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
    error.value = err instanceof Error ? err.message : "failed to load the DAG";
  } finally {
    loading.value = false;
  }
}

async function addTask() {
  const token = auth.token;
  const title = newTitle.value.trim();
  if (token === null || title.length === 0 || creating.value) {
    return;
  }
  creating.value = true;
  error.value = null;
  try {
    await createTask(token, props.id, {
      title,
      description: newDescription.value.trim() || undefined,
      acceptance: newAcceptance.value.trim() || undefined,
    });
    newTitle.value = "";
    newDescription.value = "";
    newAcceptance.value = "";
  } catch (err) {
    if (bounceIfAuth(err)) {
      return;
    }
    error.value = err instanceof Error ? err.message : "failed to create task";
  } finally {
    creating.value = false;
  }
}

function startEdit(n: DagNode) {
  editingId.value = n.id;
  editTitle.value = n.title;
  // `null` in the input means "clear"; an empty string is a valid clear too. We
  // seed the textarea with the current value (or "" when null) so the user sees
  // something; saving sends `null` only when the field is emptied.
  editDescription.value = n.description;
  editAcceptance.value = n.acceptance;
  editStatus.value = n.status;
}

function cancelEdit() {
  editingId.value = null;
}

async function saveEdit() {
  const token = auth.token;
  const id = editingId.value;
  if (token === null || id === null || saving.value) {
    return;
  }
  const title = editTitle.value.trim();
  if (title.length === 0) {
    error.value = "title must not be empty";
    return;
  }
  saving.value = true;
  error.value = null;
  try {
    // Double-option PATCH: send `null` to clear a nullable field, a non-empty
    // string to set it, and omit to leave untouched. An emptied textarea yields
    // "" — coerce that to `null` so the field is cleared to NULL, not stored as
    // an empty string (matches the server's documented clear semantics).
    const clearIfEmpty = (v: string | null): string | null =>
      v !== null && v.trim() === "" ? null : v;
    await patchTask(token, id, {
      title,
      status: editStatus.value,
      description: clearIfEmpty(editDescription.value),
      acceptance: clearIfEmpty(editAcceptance.value),
    });
    editingId.value = null;
  } catch (err) {
    if (bounceIfAuth(err)) {
      return;
    }
    error.value = err instanceof Error ? err.message : "failed to update task";
  } finally {
    saving.value = false;
  }
}

async function removeTask(id: string) {
  const token = auth.token;
  if (token === null || deleting.value) {
    return;
  }
  if (!window.confirm("Delete this task and its dependency edges?")) {
    return;
  }
  deleting.value = true;
  error.value = null;
  try {
    await deleteTask(token, id);
    if (editingId.value === id) {
      editingId.value = null;
    }
  } catch (err) {
    if (bounceIfAuth(err)) {
      return;
    }
    error.value = err instanceof Error ? err.message : "failed to delete task";
  } finally {
    deleting.value = false;
  }
}

async function addDependency() {
  const token = auth.token;
  if (token === null || linking.value) {
    return;
  }
  if (!depBlocker.value || !depBlocked.value) {
    error.value = "pick both a blocker and a blocked task";
    return;
  }
  if (depBlocker.value === depBlocked.value) {
    error.value = "a task cannot depend on itself";
    return;
  }
  linking.value = true;
  error.value = null;
  try {
    await linkDependency(token, props.id, depBlocker.value, depBlocked.value);
    depBlocker.value = "";
    depBlocked.value = "";
  } catch (err) {
    if (bounceIfAuth(err)) {
      return;
    }
    // Cycles come back as 409 conflict; surface the server's message.
    error.value = err instanceof Error ? err.message : "failed to link dependency";
  } finally {
    linking.value = false;
  }
}

async function removeDependency(blockerId: string, blockedId: string) {
  const token = auth.token;
  if (token === null) {
    return;
  }
  error.value = null;
  try {
    await unlinkDependency(token, props.id, blockerId, blockedId);
  } catch (err) {
    if (bounceIfAuth(err)) {
      return;
    }
    error.value = err instanceof Error ? err.message : "failed to unlink dependency";
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
      </template>
    </p>

    <p v-if="loading">Loading…</p>
    <p v-else-if="error && !epic" class="error" role="alert">{{ error }}</p>

    <template v-else-if="epic">
      <header>
        <div>
          <h1>{{ epic.title }}</h1>
          <span class="status" :data-status="epic.status">{{ epic.status }}</span>
          <span class="conn" :data-status="streamStatus">{{ streamStatus === "open" ? "live" : streamStatus }}</span>
        </div>
        <p v-if="!isReady" class="hint">
          The epic isn't <strong>Ready</strong> yet — the editor is read-mostly until breakdown completes.
        </p>
      </header>

      <p v-if="error" class="error inline" role="alert">{{ error }}</p>

      <div class="columns">
        <!-- Tasks ---------------------------------------------------------- -->
        <section class="tasks">
          <h2>Tasks ({{ nodes.length }})</h2>

          <details class="add-form">
            <summary>Add a task{{ !isReady ? " (Ready lane only)" : "" }}</summary>
            <form @submit.prevent="addTask">
              <input v-model="newTitle" placeholder="Title" :disabled="creating || !isReady" />
              <textarea v-model="newDescription" rows="2" placeholder="Description (end-to-end behavior)" :disabled="creating || !isReady" />
              <textarea v-model="newAcceptance" rows="2" placeholder="Acceptance criteria" :disabled="creating || !isReady" />
              <button :disabled="creating || !isReady || newTitle.trim().length === 0">
                {{ creating ? "Adding…" : "Add task" }}
              </button>
            </form>
          </details>

          <p v-if="nodes.length === 0" class="empty">
            No tasks yet. Run breakdown from the planning view, or add one by hand.
          </p>

          <ul class="task-list">
            <li v-for="n in nodes" :key="n.id" class="task" :data-status="n.status" :data-ready="n.ready">
              <div class="task-head">
                <span class="task-title">{{ n.title }}</span>
                <span class="badge" :data-ready="n.ready">{{ readinessLabel(n) }}</span>
              </div>
              <p v-if="n.description" class="task-desc">{{ n.description }}</p>
              <p v-if="n.acceptance" class="task-acc"><strong>Acceptance:</strong> {{ n.acceptance }}</p>
              <p v-if="n.blocked_by.length" class="task-blockers">
                <strong>Blocked by:</strong>
                <span v-for="b in n.blocked_by" :key="b" class="chip">{{ titleOf(b) }}</span>
              </p>

              <div v-if="editingId === n.id" class="edit-panel">
                <input v-model="editTitle" placeholder="Title" :disabled="saving" />
                <textarea v-model="editDescription" rows="2" placeholder="Description (empty to clear)" :disabled="saving" />
                <textarea v-model="editAcceptance" rows="2" placeholder="Acceptance (empty to clear)" :disabled="saving" />
                <label>
                  Status
                  <select v-model="editStatus" :disabled="saving">
                    <option v-for="s in statusOptions" :key="s" :value="s">{{ s }}</option>
                  </select>
                </label>
                <div class="row">
                  <button :disabled="saving" @click="saveEdit">{{ saving ? "Saving…" : "Save" }}</button>
                  <button :disabled="saving" @click="cancelEdit">Cancel</button>
                </div>
              </div>

              <div v-else class="row">
                <button :disabled="!isReady || deleting || saving" @click="startEdit(n)">Edit</button>
                <button :disabled="!isReady || deleting || saving" class="danger" @click="removeTask(n.id)">Delete</button>
              </div>
            </li>
          </ul>
        </section>

        <!-- Dependencies --------------------------------------------------- -->
        <aside class="deps">
          <h2>Dependencies ({{ edges.length }})</h2>

          <details class="add-form">
            <summary>Add a dependency{{ !isReady ? " (Ready lane only)" : "" }}</summary>
            <form @submit.prevent="addDependency">
              <label>
                Blocker (must finish first)
                <select v-model="depBlocker" :disabled="linking || !isReady">
                  <option value="" disabled>pick…</option>
                  <option v-for="n in nodes" :key="n.id" :value="n.id">{{ n.title }}</option>
                </select>
              </label>
              <label>
                Blocked (waits on the blocker)
                <select v-model="depBlocked" :disabled="linking || !isReady">
                  <option value="" disabled>pick…</option>
                  <option v-for="n in nodes" :key="n.id" :value="n.id">{{ n.title }}</option>
                </select>
              </label>
              <button :disabled="linking || !isReady || !depBlocker || !depBlocked">{{ linking ? "Linking…" : "Link" }}</button>
            </form>
          </details>

          <p v-if="edges.length === 0" class="empty">No dependencies.</p>
          <ul class="edge-list">
            <li v-for="e in edges" :key="`${e.blocker_id}-${e.blocked_id}`" class="edge">
              <span class="edge-names">{{ titleOf(e.blocker_id) }} → {{ titleOf(e.blocked_id) }}</span>
              <button class="danger" @click="removeDependency(e.blocker_id, e.blocked_id)">Remove</button>
            </li>
          </ul>
        </aside>
      </div>
    </template>
  </main>
</template>

<style scoped>
main {
  max-width: 72rem;
  margin: 2rem auto;
  padding: 0 1rem;
}
.crumb { margin: 0 0 1rem; }
.crumb a { color: #2563eb; text-decoration: none; }
.crumb .sep { margin: 0 0.5rem; color: #9ca3af; }
header { display: flex; align-items: flex-start; justify-content: space-between; gap: 1rem; }
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
.hint { font-size: 0.85rem; color: #6b7280; max-width: 24rem; text-align: right; }
.error { color: #b91c1c; }
.error.inline { margin: 1rem 0; }

.columns {
  display: grid;
  grid-template-columns: 2fr 1fr;
  gap: 1.5rem;
  margin-top: 1rem;
}
@media (max-width: 60rem) {
  .columns { grid-template-columns: 1fr; }
}
h2 { font-size: 1.1rem; margin: 0 0 0.75rem; }

.add-form {
  border: 1px solid #e5e7eb;
  border-radius: 8px;
  padding: 0.5rem 0.75rem;
  margin-bottom: 1rem;
  background: #f9fafb;
}
.add-form summary { cursor: pointer; font-weight: 600; color: #374151; }
.add-form form { display: flex; flex-direction: column; gap: 0.5rem; margin-top: 0.75rem; }
.add-form input,
.add-form textarea,
.add-form select {
  font: inherit;
  padding: 0.4rem 0.5rem;
  border: 1px solid #d1d5db;
  border-radius: 6px;
}
.add-form button {
  font: inherit;
  padding: 0.4rem 0.9rem;
  border: 1px solid #2563eb;
  border-radius: 6px;
  background: #2563eb;
  color: white;
  cursor: pointer;
}
.add-form button:disabled { opacity: 0.5; cursor: not-allowed; }

.empty { color: #6b7280; font-size: 0.9rem; }
.task-list { list-style: none; margin: 0; padding: 0; display: flex; flex-direction: column; gap: 0.75rem; }
.task {
  border: 1px solid #e5e7eb;
  border-radius: 8px;
  padding: 0.75rem 1rem;
  background: white;
}
.task[data-ready="true"] { border-color: #a7f3d0; }
.task-head { display: flex; align-items: center; justify-content: space-between; gap: 0.5rem; }
.task-title { font-weight: 600; }
.badge {
  font-size: 0.7rem;
  padding: 0.1rem 0.5rem;
  border-radius: 999px;
  background: #f3f4f6;
  color: #374151;
}
.badge[data-ready="true"] { background: #ecfdf5; color: #065f46; }
.task-desc { margin: 0.4rem 0 0; font-size: 0.9rem; color: #374151; white-space: pre-wrap; }
.task-acc { margin: 0.3rem 0 0; font-size: 0.85rem; color: #6b7280; }
.task-blockers { margin: 0.3rem 0 0; font-size: 0.8rem; color: #6b7280; }
.chip {
  display: inline-block;
  margin-left: 0.3rem;
  padding: 0.05rem 0.4rem;
  border-radius: 6px;
  background: #fef3c7;
  color: #92400e;
  font-size: 0.75rem;
}
.row { display: flex; gap: 0.5rem; margin-top: 0.6rem; }
.row button, .edge button {
  font: inherit;
  padding: 0.3rem 0.8rem;
  border: 1px solid #d1d5db;
  border-radius: 6px;
  background: white;
  cursor: pointer;
}
.row button.danger, .edge button.danger { color: #b91c1c; border-color: #fca5a5; }
button:disabled { opacity: 0.5; cursor: not-allowed; }

.edit-panel {
  margin-top: 0.6rem;
  display: flex;
  flex-direction: column;
  gap: 0.5rem;
}
.edit-panel input,
.edit-panel textarea,
.edit-panel select {
  font: inherit;
  padding: 0.4rem 0.5rem;
  border: 1px solid #d1d5db;
  border-radius: 6px;
}
.edit-panel label { display: flex; flex-direction: column; gap: 0.2rem; font-size: 0.8rem; color: #6b7280; }

.deps { border-left: 1px solid #e5e7eb; padding-left: 1.5rem; }
@media (max-width: 60rem) { .deps { border-left: none; padding-left: 0; } }
.edge-list { list-style: none; margin: 0; padding: 0; display: flex; flex-direction: column; gap: 0.4rem; }
.edge {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 0.5rem;
  font-size: 0.9rem;
  padding: 0.3rem 0.5rem;
  border: 1px solid #f3f4f6;
  border-radius: 6px;
}
.edge-names { color: #374151; }
</style>
