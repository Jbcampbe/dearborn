<script setup lang="ts">
import { useAuthStore } from "./stores/auth";
import TokenGate from "./components/TokenGate.vue";

// Top-level token gate: no stored token → entry screen; otherwise the routed
// app (the projects list, a project detail page, …). Keying the router view on
// the token remounts the active route on token change so it re-fetches with the
// new credential.
const auth = useAuthStore();
</script>

<template>
  <TokenGate v-if="!auth.isAuthenticated" />
  <RouterView v-else :key="auth.token ?? ''" />
</template>
