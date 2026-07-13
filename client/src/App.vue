<script setup lang="ts">
import { useAuthStore } from "./stores/auth";
import TokenGate from "./components/TokenGate.vue";
import ProjectsView from "./components/ProjectsView.vue";

// Top-level token gate: no stored token → entry screen; otherwise the app.
// Keying ProjectsView on the token remounts it on token change so it re-fetches
// with the new credential.
const auth = useAuthStore();
</script>

<template>
  <TokenGate v-if="!auth.isAuthenticated" />
  <ProjectsView v-else :key="auth.token ?? ''" />
</template>
