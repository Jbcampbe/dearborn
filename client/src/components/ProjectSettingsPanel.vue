<script setup lang="ts">
import { computed, onMounted, ref, watch } from "vue";
import { useAuthStore } from "../stores/auth";
import { ApiError } from "../api/client";
import { getProject, updateProject, type Project } from "../api/projects";
import {
  SLOT_LABELS,
  blankClears,
  getGlobalSettings,
  harnessSupportsSlot,
  listProjectAgentSettings,
  promptSaveValue,
  updateProjectAgentSetting,
  type AgentSlot,
  type GlobalSettings,
  type SlotSetting,
} from "../api/settings";
import AppModal from "./AppModal.vue";
import AppIcon from "./AppIcon.vue";

// Project-level agent settings (design doc §8): a project default base branch
// plus per-slot overrides of harness / model / system prompt. Every facet
// resolves live server-side; each card shows its effective values so the
// global → override layering stays legible. Reset = write `null` (never copy
// defaults), so server-side prompt improvements keep flowing to unoverridden
// slots.
const props = defineProps<{ projectId: string }>();

const auth = useAuthStore();

const loading = ref(true);
const loadError = ref<string | null>(null);

const project = ref<Project | null>(null);
const global = ref<GlobalSettings | null>(null);
const slots = ref<SlotSetting[]>([]);

// Per-card editable copies keyed by slot (blank string = cleared on save).
const harnessDraft = ref<Record<string, string>>({});
const modelDraft = ref<Record<string, string>>({});
const busySlot = ref<AgentSlot | null>(null);
/** Per-slot transient error/saved notes, keyed by slot key. */
const slotErrors = ref<Record<string, string>>({});
const slotSaved = ref<Record<string, boolean>>({});

// Default-base-branch editor state.
const baseBranchDraft = ref("");
const baseBranchBusy = ref(false);
const baseBranchError = ref<string | null>(null);
const baseBranchSaved = ref(false);

// Prompt-editor modal state.
const editingSlot = ref<AgentSlot | null>(null);
const promptDraft = ref("");
const promptBusy = ref(false);
const promptError = ref<string | null>(null);

/** The slot currently open in the prompt modal. */
const editing = computed<SlotSetting | null>(
  () => slots.value.find((s) => s.slot === editingSlot.value) ?? null,
);

async function load() {
  const token = auth.token;
  if (token === null) {
    return;
  }
  loading.value = true;
  loadError.value = null;
  try {
    const [proj, glob, items] = await Promise.all([
      getProject(token, props.projectId),
      getGlobalSettings(token),
      listProjectAgentSettings(token, props.projectId),
    ]);
    project.value = proj;
    global.value = glob;
    setSlots(items);
    baseBranchDraft.value = proj.base_branch ?? "";
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

function setSlots(items: SlotSetting[]) {
  slots.value = items;
  const harness: Record<string, string> = {};
  const model: Record<string, string> = {};
  for (const s of items) {
    harness[s.slot] = s.harness ?? "";
    model[s.slot] = s.model ?? "";
  }
  harnessDraft.value = harness;
  modelDraft.value = model;
}

watch(() => props.projectId, load);

async function saveBaseBranch() {
  const token = auth.token;
  if (token === null || baseBranchBusy.value) {
    return;
  }
  baseBranchBusy.value = true;
  baseBranchError.value = null;
  baseBranchSaved.value = false;
  try {
    // Blank clears back to "repo default" — the field is always rendered, so
    // blank → null is the natural mapping.
    project.value = await updateProject(token, props.projectId, {
      base_branch: blankClears(baseBranchDraft.value),
    });
    baseBranchDraft.value = project.value.base_branch ?? "";
    baseBranchSaved.value = true;
  } catch (err) {
    if (err instanceof ApiError && err.isAuth) {
      auth.logout(`Token rejected (401): ${err.message}. Please re-enter it.`);
      return;
    }
    baseBranchError.value = err instanceof Error ? err.message : "failed to save base branch";
  } finally {
    baseBranchBusy.value = false;
  }
}

async function saveSlotFacets(slot: AgentSlot) {
  const token = auth.token;
  if (token === null) {
    return;
  }
  busySlot.value = slot;
  delete slotErrors.value[slot];
  delete slotSaved.value[slot];
  try {
    const view = await updateProjectAgentSetting(token, props.projectId, slot, {
      harness: blankClears(harnessDraft.value[slot] ?? ""),
      model: blankClears(modelDraft.value[slot] ?? ""),
    });
    replaceSlot(view);
    slotSaved.value[slot] = true;
  } catch (err) {
    if (err instanceof ApiError && err.isAuth) {
      auth.logout(`Token rejected (401): ${err.message}. Please re-enter it.`);
      return;
    }
    slotErrors.value = { ...slotErrors.value, [slot]: err instanceof Error ? err.message : "failed to save slot" };
  } finally {
    busySlot.value = null;
  }
}

function openPromptEditor(slot: AgentSlot) {
  const view = slots.value.find((s) => s.slot === slot);
  if (!view) {
    return;
  }
  editingSlot.value = slot;
  // Default-source slots prefill with the built-in text so the user tweaks
  // from it; override-source slots show the override as-is.
  promptDraft.value = view.system_prompt ?? view.default_prompt;
  promptError.value = null;
}

function closePromptEditor() {
  editingSlot.value = null;
}

async function savePrompt() {
  const token = auth.token;
  const slot = editingSlot.value;
  if (token === null || slot === null || promptBusy.value) {
    return;
  }
  promptBusy.value = true;
  promptError.value = null;
  try {
    const current = editing.value;
    if (current === null) {
      return;
    }
    // Unchanged default-source text sends null (not an override) so a casual
    // open-save never freezes the built-in prompt (design §4 reset=clear).
    const system_prompt = promptSaveValue(promptDraft.value, current);
    const view = await updateProjectAgentSetting(token, props.projectId, slot, {
      system_prompt,
    });
    replaceSlot(view);
    closePromptEditor();
  } catch (err) {
    if (err instanceof ApiError && err.isAuth) {
      auth.logout(`Token rejected (401): ${err.message}. Please re-enter it.`);
      return;
    }
    promptError.value = err instanceof Error ? err.message : "failed to save prompt";
  } finally {
    promptBusy.value = false;
  }
}

async function resetPrompt() {
  const token = auth.token;
  const slot = editingSlot.value;
  if (token === null || slot === null || promptBusy.value) {
    return;
  }
  promptBusy.value = true;
  promptError.value = null;
  try {
    // Reset = clear the override (null), never copy the default text.
    const view = await updateProjectAgentSetting(token, props.projectId, slot, {
      system_prompt: null,
    });
    replaceSlot(view);
    closePromptEditor();
  } catch (err) {
    if (err instanceof ApiError && err.isAuth) {
      auth.logout(`Token rejected (401): ${err.message}. Please re-enter it.`);
      return;
    }
    promptError.value = err instanceof Error ? err.message : "failed to reset prompt";
  } finally {
    promptBusy.value = false;
  }
}

function replaceSlot(view: SlotSetting) {
  slots.value = slots.value.map((s) => (s.slot === view.slot ? view : s));
  harnessDraft.value[view.slot] = view.harness ?? "";
  modelDraft.value[view.slot] = view.model ?? "";
}

/** The always-visible resolution line under each card. */
function effectiveLine(s: SlotSetting): string {
  const model = s.effective.model ?? "CLI default";
  const prompt = s.effective.prompt_source === "override" ? "custom prompt" : "default prompt";
  return `runs on ${s.effective.harness} · ${model} · ${prompt}`;
}

onMounted(load);
</script>

<template>
  <div class="panel">
    <div v-if="loading" class="loading-stack" aria-label="Loading settings">
      <div class="skeleton sk-block" />
      <div class="skeleton sk-block" />
    </div>
    <p v-else-if="loadError" class="banner banner-error" role="alert">{{ loadError }}</p>

    <template v-else>
      <!-- Project default base branch -->
      <section class="card card-pad section">
        <h2 class="section-title">Default base branch</h2>
        <p class="hint">
          New epics branch off of and PR into this branch. Leave blank for the repo's default
          branch. Each epic can still pick its own at creation.
        </p>
        <form class="branch-row" @submit.prevent="saveBaseBranch">
          <input
            v-model="baseBranchDraft"
            class="input mono"
            type="text"
            placeholder="(repo default)"
          />
          <button
            class="btn btn-primary"
            type="submit"
            :disabled="baseBranchBusy || baseBranchDraft === (project?.base_branch ?? '')"
          >
            {{ baseBranchBusy ? "Saving…" : "Save" }}
          </button>
        </form>
        <p v-if="baseBranchError" class="banner banner-error inline-banner" role="alert">
          {{ baseBranchError }}
        </p>
        <p v-else-if="baseBranchSaved" class="inline-ok">Base branch saved.</p>
      </section>

      <!-- One card per agent slot -->
      <section v-for="view in slots" :key="view.slot" class="card card-pad section slot-card">
        <div class="slot-head">
          <h2 class="section-title">{{ SLOT_LABELS[view.slot] }}</h2>
          <button class="btn btn-sm btn-ghost" @click="openPromptEditor(view.slot)">
            <AppIcon name="pencil" :size="12" />
            Edit prompt
            <span class="tag">{{ view.effective.prompt_source === "override" ? "custom" : "default" }}</span>
          </button>
        </div>

        <div class="slot-grid">
          <div>
            <label class="label" :for="`harness-${view.slot}`">Harness</label>
            <select
              :id="`harness-${view.slot}`"
              v-model="harnessDraft[view.slot]"
              class="input select"
            >
              <option value="">(global default)</option>
              <option
                v-for="h in global?.enabled_harnesses ?? []"
                :key="h"
                :value="h"
                :disabled="!harnessSupportsSlot(h, view.slot)"
              >
                {{ h }}{{ harnessSupportsSlot(h, view.slot) ? "" : " — can't run this slot" }}
              </option>
            </select>
          </div>
          <div>
            <label class="label" :for="`model-${view.slot}`">Model</label>
            <input
              :id="`model-${view.slot}`"
              v-model="modelDraft[view.slot]"
              class="input mono"
              type="text"
              placeholder="(inherit)"
            />
          </div>
          <div class="slot-actions">
            <button
              class="btn btn-primary btn-sm"
              :disabled="
                busySlot === view.slot ||
                ((harnessDraft[view.slot] ?? '') === (view.harness ?? '') &&
                  (modelDraft[view.slot] ?? '') === (view.model ?? ''))
              "
              @click="saveSlotFacets(view.slot)"
            >
              {{ busySlot === view.slot ? "Saving…" : "Save" }}
            </button>
          </div>
        </div>

        <p v-if="slotErrors[view.slot]" class="banner banner-error inline-banner" role="alert">
          {{ slotErrors[view.slot] }}
        </p>
        <p v-if="slotSaved[view.slot]" class="inline-ok">Saved.</p>
        <p v-if="effectiveLine(view)" class="effective mono">{{ effectiveLine(view) }}</p>
      </section>
    </template>

    <!-- Prompt editor -->
    <AppModal
      :open="editingSlot !== null"
      :title="`Edit prompt — ${editing ? SLOT_LABELS[editing.slot] : ''}`"
      :width="640"
      @close="closePromptEditor"
    >
      <template v-if="editing">
        <p v-if="promptError" class="banner banner-error" role="alert">{{ promptError }}</p>
        <p v-if="editing.slot === 'review'" class="banner verdict-warning" role="note">
          <AppIcon name="warning" :size="13" />
          This prompt must instruct the agent to emit a <code>VERDICT:</code> first line —
          reviews can't be parsed without it.
        </p>
        <p class="hint">
          The instruction portion only. Dearborn always appends its own context blocks (rendered
          spec, epic background, sibling manifest) after this text.
        </p>
        <p v-if="editing.system_prompt === null" class="hint" data-testid="default-copy-note">
          Editing a copy of the built-in default. Reset restores the built-in version.
        </p>
        <textarea
          v-model="promptDraft"
          class="input prompt-textarea mono"
          rows="16"
          spellcheck="false"
        />
      </template>
      <template #footer>
        <button
          class="btn btn-danger btn-ghost-left"
          :disabled="promptBusy || editing?.system_prompt === null"
          title="Clear the override and fall back to the built-in prompt"
          @click="resetPrompt"
        >
          Reset to default
        </button>
        <span class="footer-spacer" />
        <button class="btn" :disabled="promptBusy" @click="closePromptEditor">Cancel</button>
        <button class="btn btn-primary" :disabled="promptBusy" @click="savePrompt">
          {{ promptBusy ? "Saving…" : "Save prompt" }}
        </button>
      </template>
    </AppModal>
  </div>
</template>

<style scoped>
.panel {
  display: flex;
  flex-direction: column;
}

.section {
  margin-bottom: var(--spacing-16);
}

.section-title {
  font-size: var(--text-body-sm);
  font-weight: var(--weight-medium);
}

.hint {
  font-size: var(--text-label);
  color: var(--text-faint);
  margin-top: 2px;
  margin-bottom: var(--spacing-12);
  line-height: 1.5;
}

.hint code {
  font-family: var(--font-mono);
  font-size: 11px;
}

.branch-row {
  display: flex;
  gap: var(--spacing-8);
  max-width: 420px;
}

.inline-banner {
  margin-top: var(--spacing-8);
}

.inline-ok {
  margin-top: var(--spacing-8);
  font-size: var(--text-label);
  color: var(--text-muted);
}

.slot-head {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: var(--spacing-12);
  margin-bottom: var(--spacing-10);
}

.slot-grid {
  display: grid;
  grid-template-columns: 180px 1fr auto;
  align-items: end;
  gap: var(--spacing-12);
}

.slot-grid .label {
  margin-bottom: 4px;
}

.effective {
  margin-top: var(--spacing-10);
  font-family: var(--font-mono);
  font-size: 11px;
  color: var(--text-faint);
}

.verdict-warning {
  display: flex;
  align-items: center;
  gap: var(--spacing-8);
  border: 1px solid rgba(255, 176, 32, 0.35);
  background: rgba(255, 176, 32, 0.08);
  color: var(--text-body);
  font-size: var(--text-caption);
  line-height: 1.5;
  margin-bottom: var(--spacing-8);
}

.verdict-warning code {
  font-family: var(--font-mono);
  font-size: 11px;
}

.prompt-textarea {
  width: 100%;
  resize: vertical;
  line-height: 1.55;
  white-space: pre;
}

.btn-ghost-left {
  margin-right: auto;
}

.footer-spacer {
  display: none;
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
