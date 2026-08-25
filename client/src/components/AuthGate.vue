<script setup lang="ts">
import { computed, ref } from "vue";
import { useAuthStore } from "../stores/auth";
import AppLogo from "./AppLogo.vue";

// Auth-entry screen: a create-admin form while the instance is unclaimed
// (`setupRequired`, probed at boot), the login form once any user exists.
// Submitting either signs the browser straight in and flips the app over to
// the authenticated view; the server's error message (e.g. the 12-character
// password minimum) is surfaced verbatim in the banner.
const auth = useAuthStore();

const username = ref("");
const displayName = ref("");
const password = ref("");
const busy = ref(false);

const isSetup = computed(() => auth.setupRequired);
const canSubmit = computed(
  () =>
    !busy.value &&
    username.value.trim().length > 0 &&
    password.value.length > 0 &&
    (!isSetup.value || displayName.value.trim().length > 0),
);

/** The failure to display: this form's own rejection, or an expired session's. */
const error = ref<string | null>(null);

async function submit() {
  busy.value = true;
  error.value = null;
  try {
    if (isSetup.value) {
      await auth.setup(username.value.trim(), displayName.value.trim(), password.value);
    } else {
      await auth.login(username.value.trim(), password.value);
    }
    // isAuthenticated flips → App.vue swaps AuthGate for AppShell.
  } catch (cause) {
    error.value =
      cause instanceof Error ? cause.message : "request failed";
  } finally {
    busy.value = false;
  }
}
</script>

<template>
  <section class="gate">
    <div class="gate-floor" aria-hidden="true"></div>

    <div class="gate-card fade-in">
      <div class="gate-brand">
        <AppLogo :size="28" />
        <h1>Dearborn</h1>
      </div>
      <p v-if="isSetup" class="lead">
        This instance has no users yet. Create the first admin account to claim it.
      </p>
      <p v-else class="lead">Sign in to continue.</p>

      <p
        v-if="error || auth.authError"
        class="banner banner-error"
        role="alert"
      >
        {{ error ?? auth.authError }}
      </p>

      <form @submit.prevent="submit">
        <label class="label" for="username">Username</label>
        <input
          id="username"
          v-model="username"
          class="input"
          type="text"
          autocomplete="username"
          autofocus
        />

        <template v-if="isSetup">
          <label class="label" for="display-name">Display name</label>
          <input
            id="display-name"
            v-model="displayName"
            class="input"
            type="text"
            autocomplete="name"
          />
        </template>

        <label class="label" for="password">Password</label>
        <input
          id="password"
          v-model="password"
          class="input"
          type="password"
          :autocomplete="isSetup ? 'new-password' : 'current-password'"
        />
        <p v-if="isSetup" class="hint">At least 12 characters.</p>

        <button class="btn btn-primary gate-submit" type="submit" :disabled="!canSubmit">
          {{ isSetup ? "Create admin account" : "Sign in" }}
        </button>
      </form>
    </div>
  </section>
</template>

<style scoped>
.gate {
  position: relative;
  display: flex;
  align-items: center;
  justify-content: center;
  min-height: 100vh;
  padding: var(--spacing-24);
  overflow: hidden;
}

/* Atmospheric gradient floor — the system's only decorative gradient. */
.gate-floor {
  position: absolute;
  inset: auto 0 0 0;
  height: 45vh;
  background: linear-gradient(
    to top,
    rgba(208, 214, 224, 0.05),
    rgba(8, 9, 10, 0) 70%
  );
  pointer-events: none;
}

.gate-card {
  position: relative;
  width: 100%;
  max-width: 360px;
  padding: var(--spacing-32);
  background: var(--surface-carbon);
  border: 1px solid var(--border-hairline);
  border-radius: var(--radius-cards);
}

.gate-brand {
  display: flex;
  align-items: center;
  gap: var(--spacing-12);
  margin-bottom: var(--spacing-8);
}

.gate-brand h1 {
  font-size: var(--text-subheading);
  font-weight: var(--weight-medium);
  letter-spacing: var(--tracking-subheading, -0.288px);
}

.lead {
  font-size: var(--text-caption);
  color: var(--text-muted);
  margin-bottom: var(--spacing-24);
}

form {
  display: flex;
  flex-direction: column;
  gap: var(--spacing-12);
}

.hint {
  font-size: var(--text-caption);
  color: var(--text-muted);
  margin-top: calc(-1 * var(--spacing-8));
}

.gate-submit {
  margin-top: var(--spacing-4);
  padding: 8px 16px;
}

.banner {
  margin-bottom: var(--spacing-12);
}
</style>
