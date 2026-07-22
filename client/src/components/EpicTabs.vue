<script setup lang="ts">
import { RouterLink } from "vue-router";

import AppIcon from "./AppIcon.vue";

// Linear-style view switcher for the four epic detail pages (manual details
// editor, planning chat, Ready-lane DAG editor, task kanban). The breadcrumb
// above it stays stable (`Projects / <project name>`) across all four routes —
// this tab bar is what identifies which view of the epic you're on. Each view
// passes its own key as `tab`; navigation is plain route links, so a manual
// URL edit lands on the right tab too.
const props = defineProps<{ id: string; tab: "details" | "planning" | "tasks" | "board" }>();

const TABS = [
  { key: "details", label: "Details", icon: "pencil", route: "epic-details" },
  { key: "planning", label: "Planning", icon: "sparkle", route: "epic-planning" },
  { key: "tasks", label: "Tasks", icon: "diagram", route: "epic-dag" },
  { key: "board", label: "Board", icon: "board", route: "epic-board" },
] as const;
</script>

<template>
  <nav class="epic-tabs" aria-label="Epic views">
    <RouterLink
      v-for="t in TABS"
      :key="t.key"
      class="epic-tab"
      :data-active="t.key === props.tab"
      :aria-current="t.key === props.tab ? 'page' : undefined"
      :to="{ name: t.route, params: { id: props.id } }"
    >
      <AppIcon :name="t.icon" :size="13" />
      {{ t.label }}
    </RouterLink>
  </nav>
</template>

<style scoped>
.epic-tabs {
  display: flex;
  align-items: center;
  gap: var(--spacing-4);
  border-bottom: 1px solid var(--border-hairline);
  margin-bottom: var(--spacing-16);
}

.epic-tab {
  display: inline-flex;
  align-items: center;
  gap: 6px;
  padding: 6px 10px 8px;
  margin-bottom: -1px;
  font-size: var(--text-caption);
  color: var(--text-muted);
  border-bottom: 2px solid transparent;
  transition:
    color var(--duration-fast) var(--ease-out),
    border-color var(--duration-fast) var(--ease-out);
}

.epic-tab:hover {
  color: var(--text-primary);
}

.epic-tab[data-active="true"] {
  color: var(--text-primary);
  font-weight: var(--weight-medium);
  border-bottom-color: var(--text-primary);
}
</style>
