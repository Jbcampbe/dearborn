<script setup lang="ts">
import { ref } from "vue";
import { useAuthStore } from "../stores/auth";
import AppLogo from "./AppLogo.vue";

// Token-entry screen. Shown whenever no token is stored (or the stored one was
// rejected). Submitting persists the token via the auth store, which flips the
// app over to the authenticated view.
const auth = useAuthStore();
const entered = ref("");

function submit() {
  auth.setToken(entered.value);
}
</script>

<template>
  <section class="gate">
    <div class="gate-floor" aria-hidden="true"></div>

    <div class="gate-card fade-in">
      <div class="gate-brand">
        <AppLogo :size="28" />
        <h1>Deerborn</h1>
      </div>
      <p class="lead">Enter your access token to continue.</p>

      <p v-if="auth.authError" class="banner banner-error" role="alert">
        {{ auth.authError }}
      </p>

      <form @submit.prevent="submit">
        <label class="label" for="token">Bearer token</label>
        <input
          id="token"
          v-model="entered"
          class="input"
          type="password"
          autocomplete="off"
          placeholder="DEERBORN_TOKEN"
          autofocus
        />
        <button
          class="btn btn-primary gate-submit"
          type="submit"
          :disabled="entered.trim().length === 0"
        >
          Continue
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

.gate-submit {
  margin-top: var(--spacing-4);
  padding: 8px 16px;
}

.banner {
  margin-bottom: var(--spacing-12);
}
</style>
