<script setup lang="ts">
import { onMounted, ref } from "vue";
import { RouterLink } from "vue-router";
import { useAuthStore } from "../stores/auth";
import { ApiError } from "../api/client";
import {
  getGlobalSettings,
  harnessCanBeDefault,
  updateGlobalSettings,
  SUPPORTED_HARNESSES,
  type GlobalSettings,
} from "../api/settings";

// Global agent settings (design doc §8): which coding-agent harnesses are
// enabled, the default harness, and the default model per harness. One save
// button for the whole page — `PUT /settings` merges + validates server-side
// (default must be enabled; disabling a referenced harness is a 409 whose
// message names the referencing slots, shown inline here).
const auth = useAuthStore();

const loading = ref(true);
const loadError = ref<string | null>(null);
const busy = ref(false);
const error = ref<string | null>(null);
const saved = ref(false);

/** The stored settings as last fetched (source of truth for "dirty" checks). */
const stored = ref<GlobalSettings | null>(null);
/** Editable copies of each facet. */
const enabled = ref<Set<string>>(new Set());
const models = ref<Record<string, string>>({});
const defaultHarness = ref("");

const customHarness = ref("");

async function load() {
  const token = auth.token;
  if (token === null) {
    return;
  }
  loading.value = true;
  loadError.value = null;
  try {
    const s = await getGlobalSettings(token);
    stored.value = s;
    enabled.value = new Set(s.enabled_harnesses);
    models.value = Object.fromEntries(
      Object.entries(s.default_models).map(([k, v]) => [k, v ?? ""]),
    );
    defaultHarness.value = s.default_harness;
  } catch (err) {
    if (err instanceof ApiError && err.isAuth) {
      auth.logout(`Token rejected (401): ${err.message}. Please re-enter it.`);
      return;
    }
    loadError.value = err instanceof Error ? err.message : "failed to load settings";
  } finally {
    loading.value = false;
  }
}

function toggle(harness: string, on: boolean) {
  if (on) {
    enabled.value.add(harness);
    // Trigger reactivity for the Set.
    enabled.value = new Set(enabled.value);
  } else {
    enabled.value.delete(harness);
    enabled.value = new Set(enabled.value);
    // A disabled harness can't stay the default; fall back to a survivor that
    // can actually *be* the default (it must run every slot) rather than
    // letting PUT fail validation.
    if (defaultHarness.value === harness) {
      const survivors = [...enabled.value];
      defaultHarness.value =
        survivors.find(harnessCanBeDefault) ?? survivors[0] ?? "";
    }
    delete models.value[harness];
    models.value = { ...models.value };
  }
}

function addCustomHarness() {
  const key = customHarness.value.trim();
  if (key.length === 0 || enabled.value.has(key)) {
    return;
  }
  enabled.value.add(key);
  enabled.value = new Set(enabled.value);
  customHarness.value = "";
}

/** Normalized model map for comparisons: sorted keys, blanks folded to null. */
function normalizedModels(map: Record<string, string | null>): string {
  const entries = Object.entries(map).map(([k, v]) => {
    const t = (v ?? "").trim();
    return [k, t.length > 0 ? t : null] as const;
  });
  entries.sort(([a], [b]) => a.localeCompare(b));
  return JSON.stringify(entries);
}

function dirty(): boolean {
  const s = stored.value;
  if (s === null) {
    return false;
  }
  const sorted = (arr: string[]) => [...arr].sort().join(",");
  return (
    defaultHarness.value !== s.default_harness ||
    sorted([...enabled.value]) !== sorted(s.enabled_harnesses) ||
    // Key-order-insensitive so toggle/add cycles don't fake a dirty state.
    normalizedModels(models.value) !== normalizedModels(s.default_models)
  );
}

/** Every model-map key this page knows about: stored ones plus edited ones. */
function allModelKeys(): string[] {
  return [...new Set([...Object.keys(stored.value?.default_models ?? {}), ...Object.keys(models.value)])];
}

async function save() {
  const token = auth.token;
  if (token === null || busy.value) {
    return;
  }
  busy.value = true;
  error.value = null;
  saved.value = false;
  try {
    const merged = await updateGlobalSettings(token, {
      default_harness: defaultHarness.value,
      enabled_harnesses: [...enabled.value],
      // Cover every known model key, not just currently-enabled ones: PUT
      // /settings *merges*, keeping omitted keys — so a disabled harness's
      // stored model must be cleared with an explicit null or it silently
      // resurfaces on re-enable.
      default_models: Object.fromEntries(
        allModelKeys().map((h) => [
          h,
          enabled.value.has(h) ? blankToNull(models.value[h] ?? "") : null,
        ]),
      ),
    });
    stored.value = merged;
    enabled.value = new Set(merged.enabled_harnesses);
    models.value = Object.fromEntries(
      Object.entries(merged.default_models).map(([k, v]) => [k, v ?? ""]),
    );
    defaultHarness.value = merged.default_harness;
    saved.value = true;
  } catch (err) {
    if (err instanceof ApiError && err.isAuth) {
      auth.logout(`Token rejected (401): ${err.message}. Please re-enter it.`);
      return;
    }
    error.value = err instanceof Error ? err.message : "failed to save settings";
  } finally {
    busy.value = false;
  }
}

function blankToNull(value: string): string | null {
  const trimmed = value.trim();
  return trimmed.length > 0 ? trimmed : null;
}

onMounted(load);
</script>

<template>
  <main class="page">
    <nav class="crumbs">
      <RouterLink class="crumb-home" :to="{ name: 'projects' }">Projects</RouterLink>
      <span class="sep">/</span>
      <span class="current">Settings</span>
    </nav>

    <header class="head">
      <h1 class="page-title">Agent settings</h1>
      <p class="page-sub">
        Which coding agents may run, and what they run as by default. Projects can override
        any of this per agent slot.
      </p>
    </header>

    <div v-if="loading" class="loading-stack" aria-label="Loading settings">
      <div class="skeleton sk-block" />
      <div class="skeleton sk-block" />
    </div>
    <p v-else-if="loadError" class="banner banner-error" role="alert">{{ loadError }}</p>

    <template v-else>
      <section class="card card-pad section">
        <h2 class="section-title">Enabled harnesses</h2>
        <p class="hint">A harness must be enabled before it can be picked anywhere.</p>
        <div class="toggle-list">
          <label v-for="h in SUPPORTED_HARNESSES" :key="h" class="toggle-row">
            <input
              type="checkbox"
              class="checkbox"
              :checked="enabled.has(h)"
              @change="toggle(h, ($event.target as HTMLInputElement).checked)"
            />
            <span class="mono toggle-key">{{ h }}</span>
            <span v-if="(SUPPORTED_HARNESSES as readonly string[]).includes(h)" class="tag">
              installed adapter
            </span>
          </label>
          <label v-for="h in [...enabled].filter((x) => !SUPPORTED_HARNESSES.includes(x))" :key="h" class="toggle-row">
            <input
              type="checkbox"
              class="checkbox"
              :checked="true"
              @change="toggle(h, ($event.target as HTMLInputElement).checked)"
            />
            <span class="mono toggle-key">{{ h }}</span>
          </label>
        </div>
        <div class="add-harness">
          <input
            v-model="customHarness"
            class="input input-sm mono"
            type="text"
            placeholder="Add another harness key…"
            @keydown.enter.prevent="addCustomHarness"
          />
          <button class="btn btn-sm" :disabled="customHarness.trim().length === 0" @click="addCustomHarness">
            Add
          </button>
        </div>
      </section>

      <section class="card card-pad section">
        <h2 class="section-title">Default harness</h2>
        <p class="hint">
          Used by every agent slot without its own override — so it must be able to run
          <em>every</em> slot. A harness that can't (pi has no MCP client, which planning and
          breakdown need) is picked per slot instead, on a project's settings.
        </p>
        <div class="radio-list">
          <label
            v-for="h in [...enabled]"
            :key="h"
            class="radio-row"
            :class="{ 'radio-row-disabled': !harnessCanBeDefault(h) }"
          >
            <input
              v-model="defaultHarness"
              type="radio"
              name="default-harness"
              :value="h"
              class="radio"
              :disabled="!harnessCanBeDefault(h)"
            />
            <span class="mono">{{ h }}</span>
            <span v-if="!harnessCanBeDefault(h)" class="hint">can't run every slot</span>
          </label>
        </div>
      </section>

      <section class="card card-pad section">
        <h2 class="section-title">Default model per harness</h2>
        <p class="hint">
          Passed verbatim to the CLI (<code>--model</code>). Blank = let the CLI use its own
          configured default.
        </p>
        <div class="model-list">
          <div v-for="h in [...enabled]" :key="h" class="model-row">
            <label class="label model-label" :for="`model-${h}`"><span class="mono">{{ h }}</span></label>
            <input
              :id="`model-${h}`"
              v-model="models[h]"
              class="input"
              type="text"
              placeholder="CLI default"
            />
          </div>
        </div>
      </section>

      <p v-if="error" class="banner banner-error" role="alert">{{ error }}</p>
      <p v-else-if="saved" class="banner banner-ok" role="status">Settings saved.</p>

      <footer class="save-bar">
        <button class="btn btn-primary" :disabled="busy || !dirty()" @click="save">
          {{ busy ? "Saving…" : "Save changes" }}
        </button>
      </footer>
    </template>
  </main>
</template>

<style scoped>
.head {
  margin-bottom: var(--spacing-24);
}

.page-sub {
  margin-top: var(--spacing-4);
  font-size: var(--text-caption);
  color: var(--text-muted);
  max-width: 560px;
  line-height: 1.5;
}

.section {
  margin-bottom: var(--spacing-16);
}

.section-title {
  font-size: var(--text-body-sm);
  font-weight: var(--weight-medium);
  margin-bottom: 2px;
}

.hint {
  font-size: var(--text-label);
  color: var(--text-faint);
  margin-bottom: var(--spacing-12);
}

.hint code {
  font-family: var(--font-mono);
  font-size: 11px;
}

.toggle-list,
.radio-list,
.model-list {
  display: flex;
  flex-direction: column;
  gap: var(--spacing-8);
}

.toggle-row {
  display: flex;
  align-items: center;
  gap: var(--spacing-8);
  cursor: pointer;
}

.toggle-key,
.radio-row .mono {
  font-family: var(--font-mono);
  font-size: 12px;
  color: var(--text-body);
}

.tag {
  font-size: 11px;
  color: var(--text-faint);
  border: 1px solid var(--border-hairline);
  border-radius: 999px;
  padding: 1px 8px;
}

.checkbox,
.radio {
  accent-color: var(--color-accent, currentColor);
}

.add-harness {
  display: flex;
  gap: var(--spacing-8);
  margin-top: var(--spacing-12);
}

.add-harness .input-sm {
  max-width: 260px;
  font-size: 12px;
}

.radio-row {
  display: flex;
  align-items: center;
  gap: var(--spacing-8);
  cursor: pointer;
}

.radio-row-disabled {
  cursor: not-allowed;
  opacity: 0.55;
}

.model-row {
  display: grid;
  grid-template-columns: 120px 1fr;
  align-items: center;
  gap: var(--spacing-12);
}

.model-label {
  margin-bottom: 0;
}

.banner-ok {
  border: 1px solid var(--border-hairline);
  background: var(--surface-carbon);
  color: var(--text-body);
}

.save-bar {
  display: flex;
  justify-content: flex-end;
  margin-top: var(--spacing-16);
}

.loading-stack {
  display: flex;
  flex-direction: column;
  gap: var(--spacing-16);
}

.sk-block {
  height: 96px;
}
</style>
