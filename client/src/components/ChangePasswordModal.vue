<script setup lang="ts">
import { nextTick, ref, watch } from "vue";
import { useAuthStore } from "../stores/auth";
import { ApiError } from "../api/client";
import { changePassword } from "../api/auth";
import AppModal from "./AppModal.vue";

// "Change password" dialog: current + new password, posted to
// /auth/password. No client-side rules — the server is authoritative for the
// 12-character minimum and for rejecting a wrong current password; its
// message surfaces verbatim in the error banner. On success the modal closes
// and the user stays signed in on this device (the server revokes only the
// *other* sessions).
const props = defineProps<{ open: boolean }>();
const emit = defineEmits<{ close: [] }>();

const auth = useAuthStore();
const currentPassword = ref("");
const newPassword = ref("");
const busy = ref(false);
const error = ref<string | null>(null);
const currentEl = ref<HTMLInputElement | null>(null);

watch(
  () => props.open,
  async (open) => {
    if (open) {
      currentPassword.value = "";
      newPassword.value = "";
      error.value = null;
      await nextTick();
      currentEl.value?.focus();
    }
  },
);

async function submit() {
  const token = auth.accessToken;
  if (token === null || busy.value) {
    return;
  }
  busy.value = true;
  error.value = null;
  try {
    await changePassword(token, {
      current_password: currentPassword.value,
      new_password: newPassword.value,
    });
    emit("close");
  } catch (err) {
    if (err instanceof ApiError && err.isAuth) {
      auth.logout(`Token rejected (401): ${err.message}. Please re-enter it.`);
      return;
    }
    // The server's message verbatim — "current password is incorrect" and
    // "password must be at least 12 characters" both land here.
    error.value =
      err instanceof Error ? err.message : "could not change password";
  } finally {
    busy.value = false;
  }
}
</script>

<template>
  <AppModal :open="open" title="Change password" @close="emit('close')">
    <form class="form" @submit.prevent="submit">
      <p v-if="error" class="banner banner-error" role="alert">{{ error }}</p>
      <div>
        <label class="label" for="current-password">Current password</label>
        <input
          id="current-password"
          ref="currentEl"
          v-model="currentPassword"
          class="input"
          type="password"
          autocomplete="current-password"
          :disabled="busy"
        />
      </div>
      <div>
        <label class="label" for="new-password">New password</label>
        <input
          id="new-password"
          v-model="newPassword"
          class="input"
          type="password"
          autocomplete="new-password"
          :disabled="busy"
        />
        <p class="hint">At least 12 characters.</p>
      </div>
    </form>
    <template #footer>
      <button class="btn" :disabled="busy" @click="emit('close')">Cancel</button>
      <button class="btn btn-primary" :disabled="busy" @click="submit">
        {{ busy ? "Saving…" : "Change password" }}
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

.hint {
  margin-top: var(--spacing-4);
  font-size: var(--text-label);
  color: var(--text-faint);
}
</style>
