<script setup lang="ts">
import { onMounted, ref } from "vue";
import { RouterLink } from "vue-router";
import { useAuthStore } from "../stores/auth";
import { ApiError } from "../api/client";
import { listProjects, type Project } from "../api/projects";
import CreateProjectForm from "./CreateProjectForm.vue";
import CloneStatusBadge from "./CloneStatusBadge.vue";

// The projects home: a create form beside the live list. Fetches `GET /projects`
// with the stored bearer token; a `401` bounces back to the token screen with a
// message (never a silent failure). A newly created project is prepended to the
// list without a round trip.
const auth = useAuthStore();
const projects = ref<Project[]>([]);
const loading = ref(true);
const error = ref<string | null>(null);

async function load() {
  const token = auth.token;
  if (token === null) {
    return;
  }
  loading.value = true;
  error.value = null;
  try {
    projects.value = await listProjects(token);
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
  projects.value = [project, ...projects.value];
}

onMounted(load);
</script>

<template>
  <main>
    <header>
      <h1>Deerborn</h1>
      <button class="logout" @click="auth.logout()">Log out</button>
    </header>

    <div class="layout">
      <CreateProjectForm @created="onCreated" />

      <section class="list-panel">
        <div class="row">
          <h2>Projects ({{ projects.length }})</h2>
          <button class="refresh" :disabled="loading" @click="load">Reload</button>
        </div>

        <p v-if="loading">Loading…</p>
        <p v-else-if="error" class="error" role="alert">{{ error }}</p>
        <p v-else-if="projects.length === 0" class="empty">
          No projects yet. Create one to get started.
        </p>
        <ul v-else class="projects">
          <li v-for="project in projects" :key="project.id">
            <RouterLink
              class="project-link"
              :to="{ name: 'project-detail', params: { id: project.id } }"
            >
              <span class="name">{{ project.name }}</span>
              <span class="repo">{{ project.repo_url }}</span>
              <CloneStatusBadge :status="project.clone_status" />
            </RouterLink>
            <p v-if="project.clone_status === 'error' && project.clone_error" class="clone-error">
              {{ project.clone_error }}
            </p>
          </li>
        </ul>
      </section>
    </div>
  </main>
</template>

<style scoped>
main {
  max-width: 60rem;
  margin: 3rem auto;
  padding: 0 1rem;
}
header {
  display: flex;
  align-items: baseline;
  justify-content: space-between;
}
.logout {
  font: inherit;
  padding: 0.3rem 0.7rem;
  border: 1px solid #ccc;
  border-radius: 6px;
  background: #f3f4f6;
  cursor: pointer;
}
.layout {
  display: grid;
  grid-template-columns: 22rem 1fr;
  gap: 2rem;
  align-items: start;
}
@media (max-width: 46rem) {
  .layout {
    grid-template-columns: 1fr;
  }
}
.row {
  display: flex;
  align-items: center;
  gap: 1rem;
}
.refresh {
  font: inherit;
  padding: 0.3rem 0.7rem;
  border: 1px solid #ccc;
  border-radius: 6px;
  background: #f3f4f6;
  cursor: pointer;
}
.refresh:disabled {
  opacity: 0.5;
  cursor: not-allowed;
}
.empty {
  color: #555;
}
.error {
  padding: 0.6rem 0.75rem;
  color: #991b1b;
  background: #fee2e2;
  border: 1px solid #fca5a5;
  border-radius: 6px;
}
.projects {
  list-style: none;
  padding: 0;
  margin: 0;
}
.projects li {
  border-bottom: 1px solid #eee;
}
.project-link {
  display: flex;
  gap: 1rem;
  align-items: center;
  padding: 0.7rem 0.4rem;
  text-decoration: none;
  color: inherit;
  border-radius: 6px;
}
.project-link:hover {
  background: #f3f4f6;
}
.name {
  font-weight: 600;
}
.repo {
  color: #555;
  font-size: 0.9rem;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}
.project-link :deep(.badge) {
  margin-left: auto;
}
.clone-error {
  margin: 0 0.4rem 0.5rem;
  font-size: 0.8rem;
  color: #991b1b;
}
</style>
