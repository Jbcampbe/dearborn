<script setup lang="ts">
import { onMounted } from "vue";
import { useHealthStore } from "./stores/health";

const health = useHealthStore();

onMounted(() => {
  health.check();
});
</script>

<template>
  <main>
    <h1>Deerborn</h1>
    <p>Workspace scaffold is live.</p>
    <p>Server health: <strong>{{ health.status }}</strong></p>
  </main>
</template>

<style scoped>
main {
  font-family: system-ui, sans-serif;
  max-width: 40rem;
  margin: 4rem auto;
  padding: 0 1rem;
}
</style>
