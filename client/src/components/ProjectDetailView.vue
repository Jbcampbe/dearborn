<script setup lang="ts">
import { onMounted, ref } from "vue";
import { RouterLink } from "vue-router";
import { useAuthStore } from "../stores/auth";
import { ApiError } from "../api/client";
import { getProject, refreshProject, type Project } from "../api/projects";
import CloneStatusBadge from "./CloneStatusBadge.vue";

// Project detail shell (T-104). Shows the project's identity + clone lifecycle
// and a placeholder where the kanban board lands in T-401. A "Re-clone" action
// triggers a background `git fetch`; because the clone settles asynchronously,
// the user reloads (or re-clones again) to watch pending → ready/error.
const props = defineProps<{ id: string }>();

const auth = useAuthStore();
const project = ref<Project | null>(null);
const loading = ref(true);
const error = ref<string | null>(null);
const refreshing = ref(false);

async function load() {
  const token = auth.token;
  if (token === null) {
    return;
  }
  loading.value = true;
  error.value = null;
  try {
    project.value = await getProject(token, props.id);
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

      <section class="board">
        <h2>Board</h2>
        <div class="placeholder">Kanban coming soon (T-401).</div>
      </section>
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
