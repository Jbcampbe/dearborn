<script setup lang="ts">
import { reactive, ref } from "vue";
import { useAuthStore } from "../stores/auth";
import { ApiError } from "../api/client";
import { createProject, type CreateProjectInput, type Project } from "../api/projects";

// Create-project form body (T-104), rendered inside AppModal from the Projects
// view. Required: name + repo URL. Optional: a GitHub PAT (password field,
// sent once, never returned) and the setup/test/run commands. Required fields
// are validated client-side; the server's structured validation errors
// ({ error: { code, message } }) surface inline on submit.
const emit = defineEmits<{ created: [project: Project]; cancel: [] }>();

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
    <p v-if="error" class="banner banner-error" role="alert">{{ error }}</p>

    <div class="field">
      <label class="label" for="name">Name <span class="req">*</span></label>
      <input id="name" v-model="form.name" class="input" type="text" placeholder="my-service" />
      <p v-if="fieldErrors.name" class="field-error">{{ fieldErrors.name }}</p>
    </div>

    <div class="field">
      <label class="label" for="repo_url">Repository URL <span class="req">*</span></label>
      <input
        id="repo_url"
        v-model="form.repo_url"
        class="input"
        type="text"
        placeholder="https://github.com/owner/repo.git"
      />
      <p v-if="fieldErrors.repo_url" class="field-error">{{ fieldErrors.repo_url }}</p>
    </div>

    <div class="field">
      <label class="label" for="pat">GitHub PAT <span class="opt">(optional)</span></label>
      <input
        id="pat"
        v-model="form.pat"
        class="input"
        type="password"
        autocomplete="off"
        placeholder="required only for private repos"
      />
      <p class="hint">Stored encrypted; never shown again.</p>
    </div>

    <details class="advanced">
      <summary>Optional commands</summary>
      <div class="advanced-grid">
        <div class="field">
          <label class="label" for="setup_cmd">Setup command</label>
          <input id="setup_cmd" v-model="form.setup_cmd" class="input mono" type="text" placeholder="npm install" />
        </div>
        <div class="field">
          <label class="label" for="test_cmd">Test command</label>
          <input id="test_cmd" v-model="form.test_cmd" class="input mono" type="text" placeholder="npm test" />
        </div>
        <div class="field">
          <label class="label" for="run_cmd">Run command</label>
          <input id="run_cmd" v-model="form.run_cmd" class="input mono" type="text" placeholder="npm run dev" />
        </div>
      </div>
    </details>

    <div class="form-actions">
      <button type="button" class="btn" :disabled="submitting" @click="emit('cancel')">
        Cancel
      </button>
      <button type="submit" class="btn btn-primary" :disabled="submitting">
        {{ submitting ? "Creating…" : "Create project" }}
      </button>
    </div>
  </form>
</template>

<style scoped>
.create {
  display: flex;
  flex-direction: column;
  gap: var(--spacing-16);
}

.field {
  display: flex;
  flex-direction: column;
  gap: 2px;
}

.opt {
  color: var(--text-faint);
  font-weight: var(--weight-regular);
}

.advanced summary {
  cursor: pointer;
  font-size: var(--text-caption);
  font-weight: var(--weight-medium);
  color: var(--text-muted);
  user-select: none;
  list-style: none;
  display: flex;
  align-items: center;
  gap: 6px;
}

.advanced summary::before {
  content: "";
  width: 5px;
  height: 5px;
  border-right: 1.5px solid currentColor;
  border-bottom: 1.5px solid currentColor;
  transform: rotate(-45deg);
  transition: transform var(--duration-fast) var(--ease-out);
}

.advanced[open] summary::before {
  transform: rotate(45deg);
}

.advanced summary:hover {
  color: var(--text-primary);
}

.advanced-grid {
  display: flex;
  flex-direction: column;
  gap: var(--spacing-12);
  margin-top: var(--spacing-12);
  padding-top: var(--spacing-12);
  border-top: 1px solid var(--border-hairline);
}

.form-actions {
  display: flex;
  justify-content: flex-end;
  gap: var(--spacing-8);
  padding-top: var(--spacing-4);
}
</style>
