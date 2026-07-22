<script setup lang="ts">
import { onBeforeUnmount, onMounted, ref } from "vue";
import { RouterLink } from "vue-router";
import { useAuthStore } from "../stores/auth";
import { ApiError } from "../api/client";
import { getProject, refreshProject, type Project } from "../api/projects";
import { listEpics, type Epic } from "../api/epics";
import CloneStatusBadge from "./CloneStatusBadge.vue";
import ProjectKanbanView from "./ProjectKanbanView.vue";
import CreateEpicModal from "./CreateEpicModal.vue";
import StatusIcon from "./StatusIcon.vue";
import AppIcon from "./AppIcon.vue";

// Project detail shell (T-104). Shows the project's identity + clone lifecycle,
// the project's epics, and a single "+ New" menu with the two creation entry
// points: **Epic** (T-204, creates an epic and drops the user into the
// planning chat) and **Task** (a standalone to-do, created through the board's
// TaskModal). "Re-clone" triggers a background `git fetch`; because the clone
// settles asynchronously, the user reloads to watch pending → ready/error.
const props = defineProps<{ id: string }>();

const auth = useAuthStore();
const project = ref<Project | null>(null);
const epics = ref<Epic[]>([]);
const loading = ref(true);
const error = ref<string | null>(null);
const refreshing = ref(false);
const epicModalOpen = ref(false);

/** The board's exposed create-task opener (its TaskModal serves create + edit). */
const kanban = ref<{ openCreateTask: () => void } | null>(null);

/** The "+ New" dropdown (Epic | Task). Closes on outside click and Escape. */
const newMenuOpen = ref(false);
const newMenuEl = ref<HTMLElement | null>(null);

function closeNewMenu() {
  newMenuOpen.value = false;
}

function onNewMenuDocMouseDown(event: MouseEvent) {
  if (!newMenuEl.value?.contains(event.target as Node)) {
    closeNewMenu();
  }
}

function onNewMenuKeydown(event: KeyboardEvent) {
  if (event.key === "Escape") {
    closeNewMenu();
  }
}

function toggleNewMenu() {
  newMenuOpen.value = !newMenuOpen.value;
  if (newMenuOpen.value) {
    document.addEventListener("mousedown", onNewMenuDocMouseDown);
    document.addEventListener("keydown", onNewMenuKeydown);
  } else {
    document.removeEventListener("mousedown", onNewMenuDocMouseDown);
    document.removeEventListener("keydown", onNewMenuKeydown);
  }
}

onBeforeUnmount(() => {
  document.removeEventListener("mousedown", onNewMenuDocMouseDown);
  document.removeEventListener("keydown", onNewMenuKeydown);
});

function chooseNewEpic() {
  closeNewMenu();
  epicModalOpen.value = true;
}

function chooseNewTask() {
  closeNewMenu();
  kanban.value?.openCreateTask();
}

async function load() {
  const token = auth.token;
  if (token === null) {
    return;
  }
  loading.value = true;
  error.value = null;
  try {
    const [proj, epicList] = await Promise.all([
      getProject(token, props.id),
      listEpics(token, props.id),
    ]);
    project.value = proj;
    epics.value = epicList;
  } catch (err) {
    if (err instanceof ApiError && err.isAuth) {
      auth.logout(`Token rejected (401): ${err.message}. Please re-enter it.`);
      return;
    }
    error.value = err instanceof Error ? err.message : "failed to load project";
  } finally {
    loading.value = false;
  }
}

async function reclone() {
  const token = auth.token;
  if (token === null) {
    return;
  }
  refreshing.value = true;
  error.value = null;
  try {
    project.value = await refreshProject(token, props.id);
  } catch (err) {
    if (err instanceof ApiError && err.isAuth) {
      auth.logout(`Token rejected (401): ${err.message}. Please re-enter it.`);
      return;
    }
    error.value = err instanceof Error ? err.message : "failed to refresh clone";
  } finally {
    refreshing.value = false;
  }
}

onMounted(load);
</script>

<template>
  <main class="page">
    <nav class="crumbs">
      <RouterLink :to="{ name: 'projects' }">Projects</RouterLink>
      <span class="sep">/</span>
      <span class="current">{{ project?.name ?? "…" }}</span>
    </nav>

    <div v-if="loading" class="loading-stack" aria-label="Loading project">
      <div class="skeleton sk-title" />
      <div class="skeleton sk-block" />
      <div class="skeleton sk-block" />
    </div>
    <p v-else-if="error" class="banner banner-error" role="alert">{{ error }}</p>

    <template v-else-if="project">
      <header class="head fade-in">
        <div class="head-main">
          <h1 class="page-title">{{ project.name }}</h1>
          <a class="repo mono" :href="project.repo_url" target="_blank" rel="noopener noreferrer">
            <AppIcon name="link" :size="12" />
            {{ project.repo_url }}
          </a>
        </div>
        <div class="head-actions">
          <button class="btn btn-ghost" :disabled="refreshing" @click="reclone">
            <AppIcon name="refresh" :size="13" />
            {{ refreshing ? "Re-cloning…" : "Re-clone" }}
          </button>
          <div ref="newMenuEl" class="new-menu">
            <button
              class="btn btn-primary"
              aria-haspopup="menu"
              :aria-expanded="newMenuOpen"
              @click="toggleNewMenu"
            >
              <AppIcon name="plus" :size="13" />
              New
              <AppIcon name="chevron-down" :size="12" />
            </button>
            <div v-if="newMenuOpen" class="new-menu-pop" role="menu">
              <button class="new-menu-item" role="menuitem" @click="chooseNewEpic">
                <span class="new-menu-title">Epic</span>
                <span class="new-menu-desc">Plan and break down a larger body of work</span>
              </button>
              <button class="new-menu-item" role="menuitem" @click="chooseNewTask">
                <span class="new-menu-title">Task</span>
                <span class="new-menu-desc">Small standalone to-do, straight to the board</span>
              </button>
            </div>
          </div>
        </div>
      </header>

      <section class="meta card card-pad">
        <div class="prop">
          <span class="prop-label">Clone status</span>
          <span class="prop-value"><CloneStatusBadge :status="project.clone_status" /></span>
        </div>
        <div v-if="project.clone_path" class="prop">
          <span class="prop-label">Clone path</span>
          <span class="prop-value mono">{{ project.clone_path }}</span>
        </div>
        <div v-if="project.clone_status === 'error' && project.clone_error" class="prop">
          <span class="prop-label">Clone error</span>
          <span class="prop-value error-text">{{ project.clone_error }}</span>
        </div>
        <div v-if="project.setup_cmd" class="prop">
          <span class="prop-label">Setup</span>
          <span class="prop-value mono">{{ project.setup_cmd }}</span>
        </div>
        <div v-if="project.test_cmd" class="prop">
          <span class="prop-label">Test</span>
          <span class="prop-value mono">{{ project.test_cmd }}</span>
        </div>
        <div v-if="project.run_cmd" class="prop">
          <span class="prop-label">Run</span>
          <span class="prop-value mono">{{ project.run_cmd }}</span>
        </div>
      </section>

      <section class="epics">
        <div class="section-head">
          <h2>Epics</h2>
          <span class="count">{{ epics.length }}</span>
        </div>

        <div v-if="epics.length === 0" class="empty-state">
          <AppIcon name="layers" :size="20" />
          <p>No epics yet. Start planning to create the first one.</p>
        </div>
        <ul v-else class="epic-list">
          <li v-for="epic in epics" :key="epic.id">
            <RouterLink
              class="epic-row card-interactive"
              :to="{ name: 'epic-planning', params: { id: epic.id } }"
            >
              <StatusIcon :status="epic.status" :size="14" />
              <span class="epic-title">{{ epic.title }}</span>
              <span class="badge epic-status">{{ epic.status }}</span>
              <AppIcon class="row-chevron" name="chevron-right" :size="14" />
            </RouterLink>
          </li>
        </ul>
      </section>

      <ProjectKanbanView :id="project.id" ref="kanban" />

      <CreateEpicModal
        :open="epicModalOpen"
        :project-id="project.id"
        @close="epicModalOpen = false"
      />
    </template>
  </main>
</template>

<style scoped>
.head {
  display: flex;
  align-items: flex-start;
  justify-content: space-between;
  gap: var(--spacing-16);
  margin-bottom: var(--spacing-20);
}

.head-main {
  display: flex;
  flex-direction: column;
  gap: var(--spacing-8);
  min-width: 0;
}

.repo {
  display: inline-flex;
  align-items: center;
  gap: 6px;
  color: var(--text-faint);
  font-size: 12px;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.repo:hover {
  color: var(--text-muted);
}

.head-actions {
  display: flex;
  align-items: center;
  gap: var(--spacing-8);
  flex-shrink: 0;
}

.new-menu {
  position: relative;
}

.new-menu-pop {
  position: absolute;
  top: calc(100% + 6px);
  right: 0;
  z-index: 40;
  min-width: 240px;
  display: flex;
  flex-direction: column;
  padding: 4px;
  background: var(--surface-obsidian);
  border: 1px solid var(--border-hairline);
  border-radius: var(--radius-cards);
  box-shadow: var(--shadow-xl);
}

.new-menu-item {
  display: flex;
  flex-direction: column;
  align-items: flex-start;
  gap: 2px;
  padding: 8px 10px;
  border: none;
  border-radius: var(--radius-cards);
  background: transparent;
  text-align: left;
  cursor: pointer;
}

.new-menu-item:hover {
  background: var(--surface-carbon);
}

.new-menu-title {
  font-size: var(--text-caption);
  font-weight: var(--weight-medium);
  color: var(--text-primary);
}

.new-menu-desc {
  font-size: var(--text-label);
  color: var(--text-faint);
}

.meta {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
  gap: var(--spacing-16) var(--spacing-24);
  margin-bottom: var(--spacing-32);
}

.prop {
  display: flex;
  flex-direction: column;
  gap: 5px;
  min-width: 0;
}

.prop-label {
  font-size: 11px;
  font-weight: var(--weight-medium);
  color: var(--text-faint);
  text-transform: uppercase;
  letter-spacing: 0.04em;
}

.prop-value {
  font-size: var(--text-caption);
  color: var(--text-body);
  overflow-wrap: break-word;
}

.error-text {
  color: var(--color-coral-red);
}

.epics {
  margin-bottom: var(--spacing-32);
}

.section-head {
  display: flex;
  align-items: center;
  gap: var(--spacing-8);
  margin-bottom: var(--spacing-12);
}

.section-head h2 {
  font-size: var(--text-body-sm);
  font-weight: var(--weight-medium);
}

.count {
  font-size: var(--text-label);
  color: var(--text-faint);
}

.epic-list {
  list-style: none;
  margin: 0;
  padding: 0;
  border: 1px solid var(--border-hairline);
  border-radius: var(--radius-cards);
  background: var(--surface-carbon);
  overflow: hidden;
}

.epic-list li + li {
  border-top: 1px solid var(--border-hairline);
}

.epic-row {
  display: flex;
  align-items: center;
  gap: var(--spacing-12);
  padding: 10px var(--spacing-16);
}

.epic-title {
  flex: 1;
  min-width: 0;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
  font-size: 13.5px;
  font-weight: var(--weight-regular);
  color: var(--text-primary);
}

.epic-status {
  flex-shrink: 0;
}

.row-chevron {
  color: var(--text-faint);
}

.epic-row:hover .row-chevron {
  color: var(--text-muted);
  transform: translateX(2px);
}

.loading-stack {
  display: flex;
  flex-direction: column;
  gap: var(--spacing-16);
}

.sk-title {
  height: 28px;
  width: 240px;
}

.sk-block {
  height: 96px;
}
</style>
