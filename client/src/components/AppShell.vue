<script setup lang="ts">
import { onMounted } from "vue";
import { RouterLink, useRoute } from "vue-router";
import { useAuthStore } from "../stores/auth";
import { useProjectsStore } from "../stores/projects";
import { ApiError } from "../api/client";
import AppLogo from "./AppLogo.vue";
import AppIcon from "./AppIcon.vue";
import StatusIcon from "./StatusIcon.vue";

// Authenticated app frame: a slim fixed sidebar (brand, primary nav, project
// switcher, session) beside a scrollable content canvas. The project list is
// shared with the Projects view via the projects store so both stay in sync.
const auth = useAuthStore();
const store = useProjectsStore();
const route = useRoute();

onMounted(async () => {
  const token = auth.token;
  if (token === null || store.loaded) {
    return;
  }
  try {
    await store.load(token);
  } catch (err) {
    if (err instanceof ApiError && err.isAuth) {
      auth.logout(`Token rejected (401): ${err.message}. Please re-enter it.`);
    }
    // Non-auth failures leave the sidebar list empty; the routed view renders
    // its own error state.
  }
});

function isActiveProject(id: string): boolean {
  return route.name === "project-detail" && route.params.id === id;
}
</script>

<template>
  <div class="shell">
    <aside class="sidebar">
      <div class="brand">
        <AppLogo :size="20" />
        <span class="brand-name">Dearborn</span>
      </div>

      <nav class="nav">
        <RouterLink
          class="nav-item"
          :class="{ active: route.name === 'projects' }"
          :to="{ name: 'projects' }"
        >
          <AppIcon name="home" :size="15" />
          <span>Projects</span>
        </RouterLink>
      </nav>

      <div class="side-section">
        <div class="side-label">Your projects</div>
        <div class="side-list">
          <RouterLink
            v-for="project in store.projects"
            :key="project.id"
            class="side-item"
            :class="{ active: isActiveProject(project.id) }"
            :to="{ name: 'project-detail', params: { id: project.id } }"
            :title="project.name"
          >
            <span class="side-item-icon">
              <StatusIcon :status="project.clone_status" :size="12" />
            </span>
            <span class="side-item-name">{{ project.name }}</span>
          </RouterLink>
          <p v-if="store.loaded && store.projects.length === 0" class="side-empty">
            No projects yet
          </p>
        </div>
      </div>

      <div class="side-footer">
        <button class="nav-item logout" @click="auth.logout()">
          <AppIcon name="logout" :size="15" />
          <span>Log out</span>
        </button>
      </div>
    </aside>

    <div class="content">
      <RouterView />
    </div>
  </div>
</template>

<style scoped>
.shell {
  display: flex;
  min-height: 100vh;
}

.sidebar {
  position: fixed;
  inset: 0 auto 0 0;
  width: var(--sidebar-width);
  display: flex;
  flex-direction: column;
  background: var(--surface-carbon);
  border-right: 1px solid var(--border-hairline);
  padding: var(--spacing-12) var(--spacing-8);
  z-index: 10;
}

.brand {
  display: flex;
  align-items: center;
  gap: var(--spacing-8);
  padding: var(--spacing-8) var(--spacing-8) var(--spacing-12);
}

.brand-name {
  font-size: var(--text-caption);
  font-weight: var(--weight-medium);
  color: var(--text-primary);
  letter-spacing: var(--tracking-body-sm);
}

.nav {
  display: flex;
  flex-direction: column;
  gap: 1px;
}

.nav-item {
  display: flex;
  align-items: center;
  gap: var(--spacing-8);
  padding: 5px var(--spacing-8);
  border: none;
  border-radius: var(--radius-buttons);
  background: transparent;
  color: var(--text-muted);
  font-size: var(--text-caption);
  font-weight: var(--weight-regular);
  letter-spacing: var(--tracking-body-sm);
  line-height: 20px;
  cursor: pointer;
  text-align: left;
  transition:
    background-color var(--duration-fast) var(--ease-out),
    color var(--duration-fast) var(--ease-out);
}

.nav-item:hover {
  background: rgba(255, 255, 255, 0.05);
  color: var(--text-body);
}

.nav-item.active {
  background: rgba(255, 255, 255, 0.07);
  color: var(--text-primary);
}

.side-section {
  margin-top: var(--spacing-20);
  min-height: 0;
  display: flex;
  flex-direction: column;
}

.side-label {
  padding: 0 var(--spacing-8) var(--spacing-8);
  font-size: 11px;
  font-weight: var(--weight-medium);
  color: var(--text-faint);
  letter-spacing: 0.01em;
}

.side-list {
  display: flex;
  flex-direction: column;
  gap: 1px;
  overflow-y: auto;
}

.side-item {
  display: flex;
  align-items: center;
  gap: var(--spacing-8);
  padding: 5px var(--spacing-8);
  border-radius: var(--radius-buttons);
  color: var(--text-muted);
  font-size: var(--text-caption);
  line-height: 20px;
  transition:
    background-color var(--duration-fast) var(--ease-out),
    color var(--duration-fast) var(--ease-out);
}

.side-item:hover {
  background: rgba(255, 255, 255, 0.05);
  color: var(--text-body);
}

.side-item.active {
  background: rgba(255, 255, 255, 0.07);
  color: var(--text-primary);
}

.side-item-icon {
  display: inline-flex;
  opacity: 0.9;
}

.side-item-name {
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.side-empty {
  padding: 0 var(--spacing-8);
  font-size: var(--text-label);
  color: var(--text-faint);
}

.side-footer {
  margin-top: auto;
  padding-top: var(--spacing-12);
  border-top: 1px solid var(--border-hairline);
}

.logout {
  width: 100%;
}

.content {
  flex: 1;
  min-width: 0;
  margin-left: var(--sidebar-width);
  background: var(--surface-void);
}
</style>
