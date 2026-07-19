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
import AppIcon from "./AppIcon.vue";
import StatusIcon from "./StatusIcon.vue";
import ConfirmModal from "./ConfirmModal.vue";

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
const confirmDeleteId = ref<string | null>(null);

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

function askRemoveTask(id: string) {
  confirmDeleteId.value = id;
}

async function confirmRemoveTask() {
  const token = auth.token;
  const id = confirmDeleteId.value;
  if (token === null || id === null || deleting.value) {
    return;
  }
  deleting.value = true;
  error.value = null;
  try {
    await deleteTask(token, id);
    if (editingId.value === id) {
      editingId.value = null;
    }
    confirmDeleteId.value = null;
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
  <main class="page page-wide">
    <nav class="crumbs">
      <RouterLink :to="{ name: 'projects' }">Projects</RouterLink>
      <span class="sep">/</span>
      <RouterLink :to="{ name: 'epic-planning', params: { id: props.id } }">Planning</RouterLink>
      <span class="sep">/</span>
      <span class="current">DAG editor</span>
    </nav>

    <div v-if="loading" class="loading-stack" aria-label="Loading DAG">
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
            <span v-if="!isReady" class="head-hint">
              The epic isn't <strong>Ready</strong> yet — the editor is read-mostly until
              breakdown completes.
            </span>
          </div>
        </div>
        <div class="head-side">
          <span class="conn" :data-status="streamStatus">{{ streamStatus === "open" ? "live" : streamStatus }}</span>
          <RouterLink class="btn btn-ghost" :to="{ name: 'epic-board', params: { id: props.id } }">
            <AppIcon name="board" :size="13" />
            Board view
          </RouterLink>
        </div>
      </header>

      <p v-if="error" class="banner banner-error" role="alert">{{ error }}</p>

      <div class="columns">
        <!-- Tasks ---------------------------------------------------------- -->
        <section class="tasks">
          <div class="section-head">
            <h2>Tasks</h2>
            <span class="count">{{ nodes.length }}</span>
          </div>

          <details class="add-form">
            <summary>
              <AppIcon name="plus" :size="13" />
              Add a task{{ !isReady ? " (Ready lane only)" : "" }}
            </summary>
            <form @submit.prevent="addTask">
              <input v-model="newTitle" class="input" placeholder="Title" :disabled="creating || !isReady" />
              <textarea v-model="newDescription" class="textarea" rows="2" placeholder="Description (end-to-end behavior)" :disabled="creating || !isReady" />
              <textarea v-model="newAcceptance" class="textarea" rows="2" placeholder="Acceptance criteria" :disabled="creating || !isReady" />
              <div class="form-actions">
                <button class="btn btn-primary" :disabled="creating || !isReady || newTitle.trim().length === 0">
                  {{ creating ? "Adding…" : "Add task" }}
                </button>
              </div>
            </form>
          </details>

          <div v-if="nodes.length === 0" class="empty-state">
            <AppIcon name="diagram" :size="20" />
            <p>No tasks yet. Run breakdown from the planning view, or add one by hand.</p>
          </div>

          <ul v-else class="task-list">
            <li v-for="n in nodes" :key="n.id" class="card task" :data-status="n.status" :data-ready="n.ready">
              <div class="task-head">
                <StatusIcon :status="n.status" :size="13" />
                <span class="task-title">{{ n.title }}</span>
                <span class="badge" :data-tone="n.status === 'Todo' && n.ready ? 'green' : 'neutral'">
                  {{ readinessLabel(n) }}
                </span>
              </div>
              <p v-if="n.description" class="task-desc">{{ n.description }}</p>
              <p v-if="n.acceptance" class="task-acc"><strong>Acceptance:</strong> {{ n.acceptance }}</p>
              <div v-if="n.blocked_by.length" class="task-blockers">
                <span class="blockers-label">Blocked by</span>
                <span v-for="b in n.blocked_by" :key="b" class="chip">{{ titleOf(b) }}</span>
              </div>

              <div v-if="editingId === n.id" class="edit-panel">
                <input v-model="editTitle" class="input" placeholder="Title" :disabled="saving" />
                <textarea v-model="editDescription" class="textarea" rows="2" placeholder="Description (empty to clear)" :disabled="saving" />
                <textarea v-model="editAcceptance" class="textarea" rows="2" placeholder="Acceptance (empty to clear)" :disabled="saving" />
                <label class="edit-status">
                  <span class="label">Status</span>
                  <select v-model="editStatus" class="select" :disabled="saving">
                    <option v-for="s in statusOptions" :key="s" :value="s">{{ s }}</option>
                  </select>
                </label>
                <div class="row">
                  <button class="btn btn-white" :disabled="saving" @click="saveEdit">
                    {{ saving ? "Saving…" : "Save" }}
                  </button>
                  <button class="btn" :disabled="saving" @click="cancelEdit">Cancel</button>
                </div>
              </div>

              <div v-else class="row">
                <button class="btn btn-ghost btn-sm" :disabled="!isReady || deleting || saving" @click="startEdit(n)">
                  <AppIcon name="pencil" :size="12" />
                  Edit
                </button>
                <button class="btn btn-danger btn-sm" :disabled="!isReady || deleting || saving" @click="askRemoveTask(n.id)">
                  <AppIcon name="trash" :size="12" />
                  Delete
                </button>
              </div>
            </li>
          </ul>
        </section>

        <!-- Dependencies --------------------------------------------------- -->
        <aside class="deps">
          <div class="section-head">
            <h2>Dependencies</h2>
            <span class="count">{{ edges.length }}</span>
          </div>

          <details class="add-form">
            <summary>
              <AppIcon name="plus" :size="13" />
              Add a dependency{{ !isReady ? " (Ready lane only)" : "" }}
            </summary>
            <form @submit.prevent="addDependency">
              <label class="dep-field">
                <span class="label">Blocker (must finish first)</span>
                <select v-model="depBlocker" class="select" :disabled="linking || !isReady">
                  <option value="" disabled>pick…</option>
                  <option v-for="n in nodes" :key="n.id" :value="n.id">{{ n.title }}</option>
                </select>
              </label>
              <label class="dep-field">
                <span class="label">Blocked (waits on the blocker)</span>
                <select v-model="depBlocked" class="select" :disabled="linking || !isReady">
                  <option value="" disabled>pick…</option>
                  <option v-for="n in nodes" :key="n.id" :value="n.id">{{ n.title }}</option>
                </select>
              </label>
              <div class="form-actions">
                <button class="btn btn-white" :disabled="linking || !isReady || !depBlocker || !depBlocked">
                  {{ linking ? "Linking…" : "Link" }}
                </button>
              </div>
            </form>
          </details>

          <p v-if="edges.length === 0" class="deps-empty">No dependencies.</p>
          <ul v-else class="edge-list">
            <li v-for="e in edges" :key="`${e.blocker_id}-${e.blocked_id}`" class="edge">
              <span class="edge-names">
                {{ titleOf(e.blocker_id) }}
                <AppIcon name="arrow-right" :size="12" />
                {{ titleOf(e.blocked_id) }}
              </span>
              <button class="btn btn-icon" aria-label="Remove dependency" @click="removeDependency(e.blocker_id, e.blocked_id)">
                <AppIcon name="x" :size="12" />
              </button>
            </li>
          </ul>
        </aside>
      </div>
    </template>

    <ConfirmModal
      :open="confirmDeleteId !== null"
      title="Delete task"
      message="Delete this task and its dependency edges? This cannot be undone."
      :busy="deleting"
      @confirm="confirmRemoveTask"
      @cancel="confirmDeleteId = null"
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

.head-hint {
  font-size: var(--text-label);
  color: var(--text-faint);
  line-height: 1.4;
}

.head-hint strong {
  color: var(--text-muted);
  font-weight: var(--weight-medium);
}

.head-side {
  display: flex;
  align-items: center;
  gap: var(--spacing-16);
  flex-shrink: 0;
}

.columns {
  display: grid;
  grid-template-columns: minmax(0, 2fr) minmax(0, 1fr);
  gap: var(--spacing-32);
  margin-top: var(--spacing-16);
}

@media (max-width: 60rem) {
  .columns {
    grid-template-columns: 1fr;
  }
}

.section-head {
  display: flex;
  align-items: center;
  gap: var(--spacing-8);
  margin-bottom: var(--spacing-12);
}

.section-head h2 {
  font-size: var(--text-body-sm);
  font-weight: var(--weight-medium);
}

.count {
  font-size: var(--text-label);
  color: var(--text-faint);
}

.add-form {
  border: 1px solid var(--border-hairline);
  border-radius: var(--radius-cards);
  padding: 10px var(--spacing-12);
  margin-bottom: var(--spacing-16);
  background: var(--surface-carbon);
}

.add-form summary {
  cursor: pointer;
  display: flex;
  align-items: center;
  gap: 6px;
  font-size: var(--text-caption);
  font-weight: var(--weight-medium);
  color: var(--text-muted);
  user-select: none;
  list-style: none;
}

.add-form summary:hover {
  color: var(--text-primary);
}

.add-form form {
  display: flex;
  flex-direction: column;
  gap: var(--spacing-8);
  margin-top: var(--spacing-12);
}

.form-actions {
  display: flex;
  justify-content: flex-end;
}

.dep-field {
  display: flex;
  flex-direction: column;
  gap: 4px;
}

.task-list {
  list-style: none;
  margin: 0;
  padding: 0;
  display: flex;
  flex-direction: column;
  gap: var(--spacing-8);
}

.task {
  padding: var(--spacing-12) var(--spacing-16);
  transition: border-color var(--duration-fast) var(--ease-out);
}

.task[data-ready="true"][data-status="Todo"] {
  border-color: rgba(39, 166, 68, 0.35);
}

.task-head {
  display: flex;
  align-items: center;
  gap: var(--spacing-8);
}

.task-title {
  flex: 1;
  min-width: 0;
  font-size: 13.5px;
  font-weight: var(--weight-medium);
  color: var(--text-primary);
}

.task-desc {
  margin-top: var(--spacing-8);
  font-size: var(--text-caption);
  color: var(--text-muted);
  line-height: 1.5;
  white-space: pre-wrap;
}

.task-acc {
  margin-top: var(--spacing-8);
  font-size: var(--text-label);
  color: var(--text-faint);
  line-height: 1.5;
}

.task-acc strong {
  color: var(--text-muted);
  font-weight: var(--weight-medium);
}

.task-blockers {
  display: flex;
  align-items: center;
  flex-wrap: wrap;
  gap: 6px;
  margin-top: var(--spacing-8);
}

.blockers-label {
  font-size: 11px;
  font-weight: var(--weight-medium);
  color: var(--text-faint);
  text-transform: uppercase;
  letter-spacing: 0.04em;
}

.row {
  display: flex;
  gap: var(--spacing-8);
  margin-top: var(--spacing-12);
}

.edit-panel {
  margin-top: var(--spacing-12);
  display: flex;
  flex-direction: column;
  gap: var(--spacing-8);
  padding-top: var(--spacing-12);
  border-top: 1px solid var(--border-hairline);
}

.edit-status {
  display: flex;
  flex-direction: column;
  gap: 4px;
  max-width: 200px;
}

.deps-empty {
  font-size: var(--text-caption);
  color: var(--text-faint);
}

.edge-list {
  list-style: none;
  margin: 0;
  padding: 0;
  display: flex;
  flex-direction: column;
  gap: 4px;
}

.edge {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: var(--spacing-8);
  padding: 6px 10px;
  border: 1px solid var(--border-hairline);
  border-radius: var(--radius-buttons);
  background: rgba(255, 255, 255, 0.015);
  font-size: var(--text-caption);
}

.edge-names {
  display: flex;
  align-items: center;
  gap: 6px;
  color: var(--text-body);
  min-width: 0;
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
  height: 320px;
}
</style>
