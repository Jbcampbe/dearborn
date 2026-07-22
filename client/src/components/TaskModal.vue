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
import AppModal from "./AppModal.vue";
import ConfirmModal from "./ConfirmModal.vue";

// Standalone-task dialog (create + full edit). Create mode (`task === null`):
// title required, description/acceptance optional — the task lands in `Todo`
// on the project board. Edit mode (`task` set): title, description,
// acceptance, and status are editable, plus a destructive delete behind a
// ConfirmModal. Every mutation fires a `board_updated` WS frame on
// `project:<id>`, which the kanban's stream folds in — no refetch here.
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

const isEdit = computed(() => props.task !== null);

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

/** Empty textarea input means "clear" (NULL), matching the PATCH double-option. */
function nullable(text: string): string | null {
  const trimmed = text.trim();
  return trimmed.length === 0 ? null : trimmed;
}

async function submit() {
  const token = auth.token;
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
  const token = auth.token;
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
    :title="isEdit ? 'Edit task' : 'New task'"
    :width="480"
    @close="emit('close')"
  >
    <form class="form" @submit.prevent="submit">
      <p class="task-hint">
        A standalone task lands on the project board — no epic, no planning session. For
        small, self-contained work.
      </p>
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
      <div v-if="isEdit">
        <label class="label" for="task-status">Status</label>
        <select id="task-status" v-model="status" class="select" :disabled="busy">
          <option v-for="lane in TASK_LANES" :key="lane.key" :value="lane.key">
            {{ lane.label }}
          </option>
        </select>
      </div>
    </form>
    <template #footer>
      <button
        v-if="isEdit"
        class="btn btn-danger"
        :disabled="busy"
        @click="confirmingDelete = true"
      >
        Delete
      </button>
      <span class="foot-spacer" />
      <button class="btn" :disabled="busy" @click="emit('close')">Cancel</button>
      <button
        class="btn btn-primary"
        :disabled="busy || title.trim().length === 0"
        @click="submit"
      >
        {{ busy ? "Saving…" : isEdit ? "Save" : "Create task" }}
      </button>
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
