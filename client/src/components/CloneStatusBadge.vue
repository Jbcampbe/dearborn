<script setup lang="ts">
import { computed } from "vue";
import type { CloneStatus } from "../api/projects";
import StatusIcon from "./StatusIcon.vue";

// Small status pill for a project's clone lifecycle state (pending/ready/
// error). Shared by the list and the detail page so the two never drift.
const props = defineProps<{ status: CloneStatus }>();

const tone = computed(() => {
  switch (props.status) {
    case "ready":
      return "green";
    case "error":
      return "red";
    default:
      return "neutral";
  }
});
</script>

<template>
  <span class="badge" :data-tone="tone">
    <StatusIcon :status="status" :size="11" />
    {{ status }}
  </span>
</template>
