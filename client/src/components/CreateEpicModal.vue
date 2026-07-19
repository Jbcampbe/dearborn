<script setup lang="ts">
import { nextTick, ref, watch } from "vue";
import { useRouter } from "vue-router";
import { useAuthStore } from "../stores/auth";
import { ApiError } from "../api/client";
import { createEpic } from "../api/epics";
import AppModal from "./AppModal.vue";

// "Start planning" dialog: name the epic, create it, and drop straight into
// the planning chat. Replaces the old window.prompt flow.
const props = defineProps<{ open: boolean; projectId: string }>();
const emit = defineEmits<{ close: [] }>();

const auth = useAuthStore();
const router = useRouter();
const title = ref("");
const busy = ref(false);
const error = ref<string | null>(null);
const inputEl = ref<HTMLInputElement | null>(null);

watch(
  () => props.open,
  async (open) => {
    if (open) {
      title.value = "";
      error.value = null;
      await nextTick();
      inputEl.value?.focus();
    }
  },
);

async function submit() {
  const token = auth.token;
  const trimmed = title.value.trim();
  if (token === null || trimmed.length === 0 || busy.value) {
    return;
  }
  busy.value = true;
  error.value = null;
  try {
    const epic = await createEpic(token, props.projectId, trimmed);
    emit("close");
    await router.push({ name: "epic-planning", params: { id: epic.id } });
  } catch (err) {
    if (err instanceof ApiError && err.isAuth) {
      auth.logout(`Token rejected (401): ${err.message}. Please re-enter it.`);
      return;
    }
    error.value = err instanceof Error ? err.message : "failed to start planning";
  } finally {
    busy.value = false;
  }
}
</script>

<template>
  <AppModal :open="open" title="New epic" :width="440" @close="emit('close')">
    <form class="form" @submit.prevent="submit">
      <p class="epic-hint">
        A new epic lands in the <strong>Planning</strong> lane — you'll define it with the
        planning agent.
      </p>
      <p v-if="error" class="banner banner-error" role="alert">{{ error }}</p>
      <div>
        <label class="label" for="epic-title">What do you want to build?</label>
        <input
          id="epic-title"
          ref="inputEl"
          v-model="title"
          class="input"
          type="text"
          placeholder="Epic title"
          :disabled="busy"
          @keydown.enter.prevent="submit"
        />
      </div>
    </form>
    <template #footer>
      <button class="btn" :disabled="busy" @click="emit('close')">Cancel</button>
      <button
        class="btn btn-primary"
        :disabled="busy || title.trim().length === 0"
        @click="submit"
      >
        {{ busy ? "Creating…" : "Start planning" }}
      </button>
    </template>
  </AppModal>
</template>

<style scoped>
.form {
  display: flex;
  flex-direction: column;
  gap: var(--spacing-12);
}

.epic-hint {
  font-size: var(--text-caption);
  color: var(--text-muted);
  line-height: 1.5;
}

.epic-hint strong {
  color: var(--text-body);
  font-weight: var(--weight-medium);
}
</style>
