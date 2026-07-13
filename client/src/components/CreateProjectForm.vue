<script setup lang="ts">
import { reactive, ref } from "vue";
import { useAuthStore } from "../stores/auth";
import { ApiError } from "../api/client";
import { createProject, type CreateProjectInput, type Project } from "../api/projects";

// Create-project form (T-104). Required: name + repo URL. Optional: a GitHub
// PAT (password field, sent once, never returned) and the setup/test/run
// commands. Required fields are validated client-side; the server's structured
// validation errors ({ error: { code, message } }) surface inline on submit.
const emit = defineEmits<{ created: [project: Project] }>();

const auth = useAuthStore();

const form = reactive({
  name: "",
  repo_url: "",
  pat: "",
  setup_cmd: "",
  test_cmd: "",
  run_cmd: "",
});

const submitting = ref(false);
const error = ref<string | null>(null);
// Client-side required-field errors, keyed by field.
const fieldErrors = reactive<{ name: string | null; repo_url: string | null }>({
  name: null,
  repo_url: null,
});

function validate(): boolean {
  fieldErrors.name = form.name.trim().length === 0 ? "Name is required." : null;
  fieldErrors.repo_url =
    form.repo_url.trim().length === 0 ? "Repository URL is required." : null;
  return fieldErrors.name === null && fieldErrors.repo_url === null;
}

/** Drop blank optional fields so we never send empty strings to the API. */
function buildInput(): CreateProjectInput {
  const input: CreateProjectInput = {
    name: form.name.trim(),
    repo_url: form.repo_url.trim(),
  };
  const optional: (keyof Omit<CreateProjectInput, "name" | "repo_url">)[] = [
    "pat",
    "setup_cmd",
    "test_cmd",
    "run_cmd",
  ];
  for (const key of optional) {
    const value = form[key].trim();
    if (value.length > 0) {
      input[key] = value;
    }
  }
  return input;
}

async function submit() {
  error.value = null;
  if (!validate()) {
    return;
  }
  const token = auth.token;
  if (token === null) {
    return;
  }
  submitting.value = true;
  try {
    const project = await createProject(token, buildInput());
    emit("created", project);
    // Reset for the next create.
    form.name = "";
    form.repo_url = "";
    form.pat = "";
    form.setup_cmd = "";
    form.test_cmd = "";
    form.run_cmd = "";
  } catch (err) {
    if (err instanceof ApiError && err.isAuth) {
      auth.logout(`Token rejected (401): ${err.message}. Please re-enter it.`);
      return;
    }
    error.value = err instanceof Error ? err.message : "failed to create project";
  } finally {
    submitting.value = false;
  }
}
</script>

<template>
  <form class="create" @submit.prevent="submit">
    <h2>New project</h2>

    <p v-if="error" class="error" role="alert">{{ error }}</p>

    <div class="field">
      <label for="name">Name <span class="req">*</span></label>
      <input id="name" v-model="form.name" type="text" placeholder="my-service" />
      <p v-if="fieldErrors.name" class="field-error">{{ fieldErrors.name }}</p>
    </div>

    <div class="field">
      <label for="repo_url">Repository URL <span class="req">*</span></label>
      <input
        id="repo_url"
        v-model="form.repo_url"
        type="text"
        placeholder="https://github.com/owner/repo.git"
      />
      <p v-if="fieldErrors.repo_url" class="field-error">{{ fieldErrors.repo_url }}</p>
    </div>

    <div class="field">
      <label for="pat">GitHub PAT (optional)</label>
      <input
        id="pat"
        v-model="form.pat"
        type="password"
        autocomplete="off"
        placeholder="required only for private repos"
      />
      <p class="hint">Stored encrypted; never shown again.</p>
    </div>

    <details class="advanced">
      <summary>Optional commands</summary>
      <div class="field">
        <label for="setup_cmd">Setup command</label>
        <input id="setup_cmd" v-model="form.setup_cmd" type="text" placeholder="npm install" />
      </div>
      <div class="field">
        <label for="test_cmd">Test command</label>
        <input id="test_cmd" v-model="form.test_cmd" type="text" placeholder="npm test" />
      </div>
      <div class="field">
        <label for="run_cmd">Run command</label>
        <input id="run_cmd" v-model="form.run_cmd" type="text" placeholder="npm run dev" />
      </div>
    </details>

    <button type="submit" :disabled="submitting">
      {{ submitting ? "Creating…" : "Create project" }}
    </button>
  </form>
</template>

<style scoped>
.create {
  border: 1px solid #e5e7eb;
  border-radius: 10px;
  padding: 1.25rem;
  background: #fafafa;
}
.create h2 {
  margin: 0 0 1rem;
  font-size: 1.1rem;
}
.field {
  display: flex;
  flex-direction: column;
  gap: 0.3rem;
  margin-bottom: 0.9rem;
}
label {
  font-size: 0.85rem;
  font-weight: 600;
}
.req {
  color: #dc2626;
}
input {
  padding: 0.5rem 0.6rem;
  font: inherit;
  border: 1px solid #ccc;
  border-radius: 6px;
}
.hint {
  margin: 0;
  font-size: 0.75rem;
  color: #6b7280;
}
.field-error {
  margin: 0;
  font-size: 0.8rem;
  color: #b91c1c;
}
.advanced {
  margin-bottom: 1rem;
}
.advanced summary {
  cursor: pointer;
  font-size: 0.85rem;
  font-weight: 600;
  margin-bottom: 0.6rem;
}
button {
  font: inherit;
  font-weight: 600;
  padding: 0.55rem 1rem;
  color: #fff;
  background: #2563eb;
  border: none;
  border-radius: 6px;
  cursor: pointer;
}
button:disabled {
  background: #9db8f0;
  cursor: not-allowed;
}
.error {
  padding: 0.6rem 0.75rem;
  color: #991b1b;
  background: #fee2e2;
  border: 1px solid #fca5a5;
  border-radius: 6px;
}
</style>
