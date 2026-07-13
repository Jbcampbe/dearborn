<script setup lang="ts">
import type { CloneStatus } from "../api/projects";

// Small coloured pill for a project's clone lifecycle state (pending/ready/
// error). Shared by the list and the detail page so the two never drift.
defineProps<{ status: CloneStatus }>();
</script>

<template>
  <span class="badge" :data-status="status">{{ status }}</span>
</template>

<style scoped>
.badge {
  display: inline-block;
  padding: 0.1rem 0.5rem;
  border-radius: 999px;
  font-size: 0.75rem;
  font-weight: 600;
  text-transform: uppercase;
  letter-spacing: 0.03em;
  border: 1px solid transparent;
}
.badge[data-status="pending"] {
  color: #92400e;
  background: #fef3c7;
  border-color: #fcd34d;
}
.badge[data-status="ready"] {
  color: #065f46;
  background: #d1fae5;
  border-color: #6ee7b7;
}
.badge[data-status="error"] {
  color: #991b1b;
  background: #fee2e2;
  border-color: #fca5a5;
}
</style>
