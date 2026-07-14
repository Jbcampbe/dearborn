<script setup lang="ts">
import { onMounted, ref } from "vue";
import { RouterLink, useRouter } from "vue-router";
import { useAuthStore } from "../stores/auth";
import { ApiError } from "../api/client";
import { getProject, refreshProject, type Project } from "../api/projects";
import { createEpic, listEpics, type Epic } from "../api/epics";
import CloneStatusBadge from "./CloneStatusBadge.vue";
import ProjectKanbanView from "./ProjectKanbanView.vue";

// Project detail shell (T-104). Shows the project's identity + clone lifecycle,
// the project's epics, and a "Start planning" entry point (T-204) that creates
// an epic and drops the user into the planning chat. The kanban board lands in
// T-401. A "Re-clone" action triggers a background `git fetch`; because the
// clone settles asynchronously, the user reloads to watch pending → ready/error.
const props = defineProps<{ id: string }>();

const auth = useAuthStore();
const router = useRouter();
const project = ref<Project | null>(null);
const epics = ref<Epic[]>([]);
const loading = ref(true);
const error = ref<string | null>(null);
const refreshing = ref(false);
const planning = ref(false);

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

// Create a fresh epic (lands in the Planning lane) and open the planning chat.
async function startPlanning() {
  const token = auth.token;
  if (token === null || planning.value) {
    return;
  }
  const title = window.prompt("What do you want to build? (epic title)")?.trim();
  if (!title) {
    return;
  }
  planning.value = true;
  error.value = null;
  try {
    const epic = await createEpic(token, props.id, title);
    await router.push({ name: "epic-planning", params: { id: epic.id } });
  } catch (err) {
    if (err instanceof ApiError && err.isAuth) {
      auth.logout(`Token rejected (401): ${err.message}. Please re-enter it.`);
      return;
    }
    error.value = err instanceof Error ? err.message : "failed to start planning";
  } finally {
    planning.value = false;
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
  <main>
    <p class="crumb">
      <RouterLink :to="{ name: 'projects' }">← Projects</RouterLink>
    </p>

    <p v-if="loading">Loading…</p>
    <p v-else-if="error" class="error" role="alert">{{ error }}</p>

    <template v-else-if="project">
      <header>
        <div>
          <h1>{{ project.name }}</h1>
          <a class="repo" :href="project.repo_url" target="_blank" rel="noopener noreferrer">
            {{ project.repo_url }}
          </a>
        </div>
        <CloneStatusBadge :status="project.clone_status" />
      </header>

      <section class="meta">
        <dl>
          <div>
            <dt>Clone status</dt>
            <dd><CloneStatusBadge :status="project.clone_status" /></dd>
          </div>
          <div v-if="project.clone_path">
            <dt>Clone path</dt>
            <dd class="mono">{{ project.clone_path }}</dd>
          </div>
          <div v-if="project.clone_status === 'error' && project.clone_error">
            <dt>Clone error</dt>
            <dd class="error-text">{{ project.clone_error }}</dd>
          </div>
          <div v-if="project.setup_cmd">
            <dt>Setup</dt>
            <dd class="mono">{{ project.setup_cmd }}</dd>
          </div>
          <div v-if="project.test_cmd">
            <dt>Test</dt>
            <dd class="mono">{{ project.test_cmd }}</dd>
          </div>
          <div v-if="project.run_cmd">
            <dt>Run</dt>
            <dd class="mono">{{ project.run_cmd }}</dd>
          </div>
        </dl>
        <div class="actions">
          <button :disabled="loading" @click="load">Reload</button>
          <button :disabled="refreshing" @click="reclone">
            {{ refreshing ? "Re-cloning…" : "Re-clone" }}
          </button>
        </div>
      </section>

      <section class="epics">
        <div class="epics-head">
          <h2>Epics ({{ epics.length }})</h2>
          <button class="primary" :disabled="planning" @click="startPlanning">
            {{ planning ? "Starting…" : "Start planning" }}
          </button>
        </div>

        <p v-if="epics.length === 0" class="empty">
          No epics yet. Start planning to create the first one.
        </p>
        <ul v-else class="epic-list">
          <li v-for="epic in epics" :key="epic.id">
            <RouterLink
              class="epic-link"
              :to="{ name: 'epic-planning', params: { id: epic.id } }"
            >
              <span class="epic-title">{{ epic.title }}</span>
              <span class="epic-status">{{ epic.status }}</span>
            </RouterLink>
          </li>
        </ul>
      </section>

      <ProjectKanbanView :id="project.id" />
    </template>
  </main>
</template>

<style scoped>
main {
  max-width: 60rem;
  margin: 3rem auto;
  padding: 0 1rem;
}
.crumb {
  margin: 0 0 1rem;
}
.crumb a {
  color: #2563eb;
  text-decoration: none;
}
header {
  display: flex;
  align-items: flex-start;
  justify-content: space-between;
  gap: 1rem;
}
header h1 {
  margin: 0;
}
.repo {
  color: #555;
  font-size: 0.9rem;
  text-decoration: none;
}
.repo:hover {
  text-decoration: underline;
}
.meta {
  margin-top: 1.5rem;
  padding: 1.25rem;
  border: 1px solid #e5e7eb;
  border-radius: 10px;
  background: #fafafa;
}
dl {
  margin: 0 0 1rem;
  display: grid;
  gap: 0.75rem;
}
dl > div {
  display: grid;
  grid-template-columns: 8rem 1fr;
  gap: 1rem;
  align-items: baseline;
}
dt {
  font-size: 0.85rem;
  font-weight: 600;
  color: #374151;
}
dd {
  margin: 0;
}
.mono {
  font-family: ui-monospace, SFMono-Regular, Menlo, monospace;
  font-size: 0.85rem;
  word-break: break-all;
}
.error-text {
  color: #991b1b;
}
.actions {
  display: flex;
  gap: 0.6rem;
}
.actions button {
  font: inherit;
  padding: 0.35rem 0.8rem;
  border: 1px solid #ccc;
  border-radius: 6px;
  background: #f3f4f6;
  cursor: pointer;
}
.actions button:disabled {
  opacity: 0.5;
  cursor: not-allowed;
}
.epics {
  margin-top: 2rem;
}
.epics-head {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 1rem;
}
.primary {
  font: inherit;
  padding: 0.4rem 0.9rem;
  border: 1px solid #2563eb;
  border-radius: 6px;
  background: #2563eb;
  color: #fff;
  cursor: pointer;
}
.primary:disabled {
  opacity: 0.5;
  cursor: not-allowed;
}
.empty {
  color: #6b7280;
}
.epic-list {
  list-style: none;
  padding: 0;
  margin: 0.5rem 0 0;
}
.epic-list li {
  border-bottom: 1px solid #eee;
}
.epic-link {
  display: flex;
  align-items: center;
  gap: 1rem;
  padding: 0.7rem 0.4rem;
  text-decoration: none;
  color: inherit;
  border-radius: 6px;
}
.epic-link:hover {
  background: #f3f4f6;
}
.epic-title {
  font-weight: 600;
}
.epic-status {
  margin-left: auto;
  font-size: 0.8rem;
  padding: 0.1rem 0.5rem;
  border-radius: 999px;
  background: #eef2ff;
  color: #3730a3;
}
.board {
  margin-top: 2rem;
}
.placeholder {
  padding: 3rem 1rem;
  text-align: center;
  color: #6b7280;
  border: 2px dashed #d1d5db;
  border-radius: 10px;
}
.error {
  padding: 0.6rem 0.75rem;
  color: #991b1b;
  background: #fee2e2;
  border: 1px solid #fca5a5;
  border-radius: 6px;
}
</style>
