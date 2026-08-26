<script setup lang="ts">
import { computed, nextTick, ref, watch } from "vue";

import { useAuthStore } from "../stores/auth";
import { ApiError } from "../api/client";
import {
  createProjectTask,
  deleteTask,
  patchTask,
  type Task,
  type TaskStatus,
} from "../api/tasks";
import { TASK_LANES } from "../board/epicLanes";
import AppIcon from "./AppIcon.vue";
import AppModal from "./AppModal.vue";
import ConfirmModal from "./ConfirmModal.vue";
import TaskPipelinePanel from "./TaskPipelinePanel.vue";

// Task dialog (create standalone + full edit for standalone and epic tasks).
// Create mode (`task === null`): title required, description/acceptance
// optional — the task lands in `Todo` on the project board. Edit mode (`task`
// set): title, description, acceptance, and status are editable, plus a
// destructive delete behind a ConfirmModal. Every mutation fires a WS frame
// (`board_updated` on `project:<id>` for standalone tasks, `dag_updated` on
// `epic:<id>` for epic tasks), which the kanban's stream folds in — no refetch
// here.
//
// View-first: edit mode opens on a read-only Details tab (`isViewMode`) —
// most clicks are inspection, not editing — and an Edit footer button flips
// the same panel into the form in-place. Cancel from edit returns to view, not
// close. Create mode skips all of that: no tabs, `isViewMode` false, form
// renders immediately.
// T-562: edit mode gains a second "Pipeline" tab (`TaskPipelinePanel.vue`) —
// the stage timeline for this task's `agent_run` history. This dialog is the
// one surface every task card already opens on click, standalone or
// epic-scoped, on either kanban (`ProjectKanbanView.vue`/`EpicKanbanView.
// vue`) — reusing it here means the pipeline view needs no new route and no
// duplicated "which task is this" plumbing. The panel only mounts while its
// tab is active (`v-if`, not `v-show`), so switching away or closing the
// dialog actually tears it down — the mount/unmount boundary T-563's
// subscribe-on-open/unsubscribe-on-close will use.
const props = defineProps<{ open: boolean; projectId: string; task: Task | null }>();
const emit = defineEmits<{ close: [] }>();

const auth = useAuthStore();

const title = ref("");
const description = ref("");
const acceptance = ref("");
const status = ref<TaskStatus>("Todo");
const busy = ref(false);
const error = ref<string | null>(null);
const confirmingDelete = ref(false);
const inputEl = ref<HTMLInputElement | null>(null);
const activeTab = ref<"details" | "pipeline">("details");
/** Read-only Details display when true; the edit form renders when false. */
const isViewMode = ref(true);

const isEdit = computed(() => props.task !== null);

/** The pipeline tab needs more room for a log than the edit form does. */
const modalWidth = computed(() => (activeTab.value === "pipeline" ? 760 : 480));

/** Epic-scoped tasks edit against the epic's board, not the project board. */
const isEpicTask = computed(() => props.task?.epic_id != null);

// The hint reflects where the task lives: standalone tasks land on the project
// board; epic tasks are edited from the epic's task kanban.
const hint = computed(() =>
  isEpicTask.value
    ? "A task in this epic — edits publish live to the epic's board and DAG."
    : "A standalone task lands on the project board — no epic, no planning session. For small, self-contained work.",
);

// The worker owns transitions out of InProgress for epic tasks (drag-and-drop
// on the epic board enforces the same rule), so the status select is hidden
// for a running epic task.
const statusEditable = computed(
  () => isEdit.value && !(isEpicTask.value && props.task?.status === "InProgress"),
);

watch(
  () => props.open,
  async (open) => {
    if (open) {
      // Reset from the task being edited (or blank for create).
      title.value = props.task?.title ?? "";
      description.value = props.task?.description ?? "";
      acceptance.value = props.task?.acceptance ?? "";
      status.value = props.task?.status ?? "Todo";
      error.value = null;
      confirmingDelete.value = false;
      activeTab.value = "details";
      // Create mode starts editing immediately; edit mode starts in view.
      isViewMode.value = props.task !== null;
      await nextTick();
      inputEl.value?.focus();
    }
  },
);

function bounceIfAuth(err: unknown): boolean {
  if (err instanceof ApiError && err.isAuth) {
    auth.logout(`Token rejected (401): ${err.message}. Please re-enter it.`);
    return true;
  }
  return false;
}

/** Cancel: back to read-only Details when editing; close when creating. */
function cancelEdit() {
  if (isEdit.value) {
    isViewMode.value = true;
  } else {
    emit("close");
  }
}

/** Empty textarea input means "clear" (NULL), matching the PATCH double-option. */
function nullable(text: string): string | null {
  const trimmed = text.trim();
  return trimmed.length === 0 ? null : trimmed;
}

async function submit() {
  const token = auth.accessToken;
  const trimmed = title.value.trim();
  if (token === null || trimmed.length === 0 || busy.value) {
    return;
  }
  busy.value = true;
  error.value = null;
  try {
    if (props.task === null) {
      await createProjectTask(token, props.projectId, {
        title: trimmed,
        ...(description.value.trim() ? { description: description.value.trim() } : {}),
        ...(acceptance.value.trim() ? { acceptance: acceptance.value.trim() } : {}),
      });
    } else {
      await patchTask(token, props.task.id, {
        title: trimmed,
        description: nullable(description.value),
        acceptance: nullable(acceptance.value),
        status: status.value,
      });
    }
    // The board_updated WS frame drives the kanban re-render.
    emit("close");
  } catch (err) {
    if (bounceIfAuth(err)) {
      return;
    }
    error.value = err instanceof Error ? err.message : "failed to save the task";
  } finally {
    busy.value = false;
  }
}

async function confirmDelete() {
  const token = auth.accessToken;
  if (token === null || props.task === null || busy.value) {
    return;
  }
  busy.value = true;
  error.value = null;
  try {
    await deleteTask(token, props.task.id);
    confirmingDelete.value = false;
    emit("close");
  } catch (err) {
    if (bounceIfAuth(err)) {
      return;
    }
    confirmingDelete.value = false;
    error.value = err instanceof Error ? err.message : "failed to delete the task";
  } finally {
    busy.value = false;
  }
}
</script>

<template>
  <AppModal
    :open="open"
    :title="isEdit ? (isViewMode ? 'Task' : 'Edit task') : 'New task'"
    :width="modalWidth"
    @close="emit('close')"
  >
    <nav v-if="isEdit" class="task-tabs" aria-label="Task view">
      <button
        type="button"
        class="task-tab"
        :data-active="activeTab === 'details'"
        @click="activeTab = 'details'"
      >
        <AppIcon name="pencil" :size="13" />
        Details
      </button>
      <button
        type="button"
        class="task-tab"
        :data-active="activeTab === 'pipeline'"
        @click="activeTab = 'pipeline'"
      >
        <AppIcon name="layers" :size="13" />
        Pipeline
      </button>
    </nav>

    <template v-if="activeTab === 'details'">
      <!-- View mode: read-only display of the task fields. -->
      <div v-if="isViewMode" class="task-view">
        <p class="task-hint">{{ hint }}</p>
        <div>
          <span class="label">Title</span>
          <p class="view-value">{{ task?.title }}</p>
        </div>
        <div v-if="task?.description">
          <span class="label">Description</span>
          <p class="view-value">{{ task.description }}</p>
        </div>
        <div v-if="task?.acceptance">
          <span class="label">Acceptance</span>
          <p class="view-value">{{ task.acceptance }}</p>
        </div>
        <div>
          <span class="label">Status</span>
          <span class="badge">{{ task?.status }}</span>
        </div>
      </div>

      <!-- Edit mode: the existing form, unchanged. -->
      <form v-else class="form" @submit.prevent="submit">
        <p class="task-hint">{{ hint }}</p>
        <p v-if="error" class="banner banner-error" role="alert">{{ error }}</p>
        <div>
          <label class="label" for="task-title">Title</label>
          <input
            id="task-title"
            ref="inputEl"
            v-model="title"
            class="input"
            type="text"
            placeholder="Task title"
            :disabled="busy"
            @keydown.enter.prevent="submit"
          />
        </div>
        <div>
          <label class="label" for="task-description">Description <span class="optional">(optional)</span></label>
          <textarea
            id="task-description"
            v-model="description"
            class="input textarea"
            rows="3"
            placeholder="What needs to happen"
            :disabled="busy"
          />
        </div>
        <div>
          <label class="label" for="task-acceptance">Acceptance <span class="optional">(optional)</span></label>
          <textarea
            id="task-acceptance"
            v-model="acceptance"
            class="input textarea"
            rows="2"
            placeholder="How you'll know it's done"
            :disabled="busy"
          />
        </div>
        <div v-if="statusEditable">
          <label class="label" for="task-status">Status</label>
          <select id="task-status" v-model="status" class="select" :disabled="busy">
            <option v-for="lane in TASK_LANES" :key="lane.key" :value="lane.key">
              {{ lane.label }}
            </option>
          </select>
        </div>
      </form>
    </template>

    <TaskPipelinePanel v-else-if="task" :key="task.id" :task-id="task.id" />

    <template #footer>
      <template v-if="activeTab === 'details'">
        <!-- View mode footer: inspect, then close or flip into edit. -->
        <template v-if="isViewMode">
          <button
            v-if="isEdit"
            class="btn btn-danger"
            :disabled="busy"
            @click="confirmingDelete = true"
          >
            Delete
          </button>
          <span class="foot-spacer" />
          <button class="btn" @click="emit('close')">Close</button>
          <button
            v-if="isEdit"
            class="btn btn-primary"
            @click="isViewMode = false"
          >
            Edit
          </button>
        </template>
        <!-- Edit mode footer: same actions as the previous always-edit form. -->
        <template v-else>
          <button
            v-if="isEdit"
            class="btn btn-danger"
            :disabled="busy"
            @click="confirmingDelete = true"
          >
            Delete
          </button>
          <span class="foot-spacer" />
          <button class="btn" :disabled="busy" @click="cancelEdit">Cancel</button>
          <button
            class="btn btn-primary"
            :disabled="busy || title.trim().length === 0"
            @click="submit"
          >
            {{ busy ? "Saving…" : isEdit ? "Save" : "Create task" }}
          </button>
        </template>
      </template>
      <template v-else>
        <span class="foot-spacer" />
        <button class="btn" @click="emit('close')">Close</button>
      </template>
    </template>
  </AppModal>

  <ConfirmModal
    :open="confirmingDelete"
    title="Delete task"
    :message="`Delete “${task?.title ?? ''}”? This cannot be undone.`"
    :busy="busy"
    @confirm="confirmDelete"
    @cancel="confirmingDelete = false"
  />
</template>

<style scoped>
.task-tabs {
  display: flex;
  align-items: center;
  gap: var(--spacing-4);
  border-bottom: 1px solid var(--border-hairline);
  margin-bottom: var(--spacing-16);
}

/* Same underline-tab look as EpicTabs.vue — the base reset (base.css) only
   inherits font-family/color on buttons, so the UA background/border/padding
   must be cleared explicitly or the tab renders as a boxed button. */
.task-tab {
  display: inline-flex;
  align-items: center;
  gap: 6px;
  padding: 6px 10px 8px;
  margin-bottom: -1px;
  background: none;
  border: none;
  border-bottom: 2px solid transparent;
  font-size: var(--text-caption);
  line-height: var(--leading-body-sm);
  letter-spacing: var(--tracking-body-sm);
  color: var(--text-muted);
  cursor: pointer;
  transition:
    color var(--duration-fast) var(--ease-out),
    border-color var(--duration-fast) var(--ease-out);
}

.task-tab:hover {
  color: var(--text-primary);
}

.task-tab[data-active="true"] {
  color: var(--text-primary);
  font-weight: var(--weight-medium);
  border-bottom-color: var(--text-primary);
}

.form {
  display: flex;
  flex-direction: column;
  gap: var(--spacing-12);
}

.task-hint {
  font-size: var(--text-caption);
  color: var(--text-muted);
  line-height: 1.5;
}

.task-view {
  display: flex;
  flex-direction: column;
  gap: var(--spacing-12);
}

.view-value {
  font-size: var(--text-body);
  color: var(--text-primary);
  line-height: 1.5;
  margin-top: var(--spacing-4);
  white-space: pre-wrap;
}

.optional {
  color: var(--text-muted);
  font-weight: var(--weight-regular);
}

.textarea {
  resize: vertical;
  min-height: 56px;
  font-family: inherit;
  line-height: 1.45;
}

.foot-spacer {
  flex: 1;
}
</style>
