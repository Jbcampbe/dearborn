<script setup lang="ts">
import { onMounted, ref } from "vue";
import { RouterLink } from "vue-router";
import { useAuthStore } from "../stores/auth";
import { useProjectsStore } from "../stores/projects";
import { ApiError } from "../api/client";
import type { Project } from "../api/projects";
import CreateProjectForm from "./CreateProjectForm.vue";
import CloneStatusBadge from "./CloneStatusBadge.vue";
import AppModal from "./AppModal.vue";
import AppIcon from "./AppIcon.vue";

// The projects home: a Linear-style row list behind a modal create form. The
// list itself lives in the shared projects store (the sidebar consumes it
// too); a `401` bounces back to the token screen with a message (never a
// silent failure). A newly created project is prepended without a round trip.
const auth = useAuthStore();
const store = useProjectsStore();
const loading = ref(true);
const error = ref<string | null>(null);
const createOpen = ref(false);

async function load() {
  const token = auth.token;
  if (token === null) {
    return;
  }
  loading.value = true;
  error.value = null;
  try {
    await store.load(token);
  } catch (err) {
    if (err instanceof ApiError && err.isAuth) {
      auth.logout(`Token rejected (401): ${err.message}. Please re-enter it.`);
      return;
    }
    error.value = err instanceof Error ? err.message : "failed to load projects";
  } finally {
    loading.value = false;
  }
}

function onCreated(project: Project) {
  store.add(project);
  createOpen.value = false;
}

onMounted(load);
</script>

<template>
  <main class="page">
    <header class="head">
      <div>
        <h1 class="page-title">Projects</h1>
        <p class="page-sub">{{ store.projects.length }} total</p>
      </div>
      <button class="btn btn-primary" @click="createOpen = true">
        <AppIcon name="plus" :size="13" />
        New project
      </button>
    </header>

    <div v-if="loading" class="rows-skeleton" aria-label="Loading projects">
      <div v-for="i in 4" :key="i" class="skeleton sk-row" />
    </div>

    <p v-else-if="error" class="banner banner-error" role="alert">{{ error }}</p>

    <div v-else-if="store.projects.length === 0" class="empty-state">
      <AppIcon name="box" :size="20" />
      <p>No projects yet. Create one to start planning epics.</p>
      <button class="btn btn-ghost" @click="createOpen = true">
        <AppIcon name="plus" :size="13" />
        New project
      </button>
    </div>

    <ul v-else class="rows fade-in">
      <li v-for="project in store.projects" :key="project.id">
        <RouterLink
          class="row card-interactive"
          :to="{ name: 'project-detail', params: { id: project.id } }"
        >
          <span class="row-name">{{ project.name }}</span>
          <span class="row-repo mono">{{ project.repo_url }}</span>
          <CloneStatusBadge :status="project.clone_status" />
          <AppIcon class="row-chevron" name="chevron-right" :size="14" />
        </RouterLink>
        <p v-if="project.clone_status === 'error' && project.clone_error" class="clone-error">
          {{ project.clone_error }}
        </p>
      </li>
    </ul>

    <AppModal :open="createOpen" title="New project" :width="480" @close="createOpen = false">
      <CreateProjectForm @created="onCreated" @cancel="createOpen = false" />
    </AppModal>
  </main>
</template>

<style scoped>
.head {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: var(--spacing-16);
  margin-bottom: var(--spacing-24);
}

.rows {
  list-style: none;
  margin: 0;
  padding: 0;
  border: 1px solid var(--border-hairline);
  border-radius: var(--radius-cards);
  background: var(--surface-carbon);
  overflow: hidden;
}

.rows li + li {
  border-top: 1px solid var(--border-hairline);
}

.row {
  display: flex;
  align-items: center;
  gap: var(--spacing-16);
  padding: var(--spacing-12) var(--spacing-16);
}

.row-name {
  font-weight: var(--weight-medium);
  font-size: 13.5px;
  color: var(--text-primary);
  min-width: 10rem;
}

.row-repo {
  flex: 1;
  min-width: 0;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
  color: var(--text-faint);
}

.row-chevron {
  color: var(--text-faint);
  transition:
    color var(--duration-fast) var(--ease-out),
    transform var(--duration-fast) var(--ease-out);
}

.row:hover .row-chevron {
  color: var(--text-muted);
  transform: translateX(2px);
}

.clone-error {
  padding: 0 var(--spacing-16) var(--spacing-12);
  font-size: var(--text-label);
  color: var(--color-coral-red);
}

.rows-skeleton {
  display: flex;
  flex-direction: column;
  gap: var(--spacing-8);
}

.sk-row {
  height: 46px;
}

.empty-state .btn {
  margin-top: var(--spacing-8);
}
</style>
