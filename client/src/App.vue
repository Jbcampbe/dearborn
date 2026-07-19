<script setup lang="ts">
import { useAuthStore } from "./stores/auth";
import TokenGate from "./components/TokenGate.vue";
import AppShell from "./components/AppShell.vue";

// Top-level token gate: no stored token → entry screen; otherwise the app
// shell (sidebar frame) around the routed view. Keying the shell on the token
// remounts the active route on token change so it re-fetches with the new
// credential.
const auth = useAuthStore();
</script>

<template>
  <TokenGate v-if="!auth.isAuthenticated" />
  <AppShell v-else :key="auth.token ?? ''" />
</template>
