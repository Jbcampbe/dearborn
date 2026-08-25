<script setup lang="ts">
import { useAuthStore } from "./stores/auth";
import AppLogo from "./components/AppLogo.vue";
import AuthGate from "./components/AuthGate.vue";
import AppShell from "./components/AppShell.vue";

// Boot flow: a neutral splash while the auth store rehydrates the stored
// session or probes /auth/status (so neither the gate nor the app flashes),
// then the auth gate (create-admin vs login) or the app shell around the
// routed view. Keying the shell on the *user id* remounts it only when a
// different account signs in — an access-token refresh must not throw away
// in-flight views.
const auth = useAuthStore();
</script>

<template>
  <div v-if="auth.booting" class="boot-splash">
    <div class="boot-brand fade-in">
      <AppLogo :size="28" />
      <span>Dearborn</span>
    </div>
  </div>
  <AuthGate v-else-if="!auth.isAuthenticated" />
  <AppShell v-else :key="auth.user?.id ?? ''" />
</template>

<style scoped>
.boot-splash {
  display: flex;
  align-items: center;
  justify-content: center;
  min-height: 100vh;
}

.boot-brand {
  display: flex;
  align-items: center;
  gap: var(--spacing-12);
  font-size: var(--text-subheading);
  font-weight: var(--weight-medium);
  letter-spacing: var(--tracking-subheading, -0.288px);
}
</style>
