<script setup lang="ts">
import { onMounted, ref } from "vue";
import { useAuthStore } from "../stores/auth";
import { apiFetch, ApiError, type Collection } from "../api/client";

// The authenticated view. Proves the round trip by fetching `GET /projects`
// with the stored bearer token and rendering the result. A `401` (wrong token)
// logs the user out with an auth-error message, bouncing them back to the token
// screen — never a silent failure.

interface Project {
  id: string;
  name: string;
  repo_url: string;
  clone_status: string;
}

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
    const data = await apiFetch<Collection<Project>>("/projects", token);
    projects.value = data.items;
  } catch (err) {
    if (err instanceof ApiError && err.isAuth) {
      // Wrong/expired token: surface the auth error on the token screen.
      auth.logout(`Token rejected (401): ${err.message}. Please re-enter it.`);
      return;
    }
    error.value = err instanceof Error ? err.message : "failed to load projects";
  } finally {
    loading.value = false;
  }
}

onMounted(load);
</script>

<template>
  <main>
    <header>
      <h1>Deerborn</h1>
      <button class="logout" @click="auth.logout()">Log out</button>
    </header>

    <section>
      <div class="row">
        <h2>Projects ({{ projects.length }})</h2>
        <button class="refresh" :disabled="loading" @click="load">Refresh</button>
      </div>

      <p v-if="loading">Loading…</p>
      <p v-else-if="error" class="error" role="alert">{{ error }}</p>
      <p v-else-if="projects.length === 0" class="empty">
        No projects yet. (The authenticated round trip succeeded.)
      </p>
      <ul v-else class="projects">
        <li v-for="project in projects" :key="project.id">
          <span class="name">{{ project.name }}</span>
          <span class="repo">{{ project.repo_url }}</span>
          <span class="status" :data-status="project.clone_status">{{ project.clone_status }}</span>
        </li>
      </ul>
    </section>
  </main>
</template>

<style scoped>
main {
  max-width: 48rem;
  margin: 3rem auto;
  padding: 0 1rem;
}
header {
  display: flex;
  align-items: baseline;
  justify-content: space-between;
}
.row {
  display: flex;
  align-items: center;
  gap: 1rem;
}
button {
  font: inherit;
  padding: 0.3rem 0.7rem;
  border: 1px solid #ccc;
  border-radius: 6px;
  background: #f3f4f6;
  cursor: pointer;
}
button:disabled {
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
}
.projects li {
  display: flex;
  gap: 1rem;
  align-items: center;
  padding: 0.6rem 0;
  border-bottom: 1px solid #eee;
}
.name {
  font-weight: 600;
}
.repo {
  color: #555;
  font-size: 0.9rem;
}
.status {
  margin-left: auto;
  font-size: 0.8rem;
  color: #444;
}
</style>
