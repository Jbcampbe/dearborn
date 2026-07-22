<script setup lang="ts">
import { computed, onBeforeUnmount, onMounted, reactive, ref, watch } from "vue";
import { RouterLink } from "vue-router";

import { useAuthStore } from "../stores/auth";
import { ApiError } from "../api/client";
import { getEpic, updateEpic } from "../api/epics";
import { getProject } from "../api/projects";
import { hydrateDag, initialDagState, type DagState } from "../dag/stream";
import { useDagStream, type StreamStatus } from "../dag/useDagStream";
import {
  applyLiveEpic,
  diffEpicEdits,
  draftFromEpic,
  isDirty,
  type EpicDraft,
} from "../lib/epicEdit";
import { renderMarkdown } from "../lib/markdown";
import AppIcon from "./AppIcon.vue";
import EpicTabs from "./EpicTabs.vue";
import StatusIcon from "./StatusIcon.vue";

// Manual epic-details page — the leftmost tab of the epic detail pages. It
// defaults to a read-only *view mode* (title + contexts rendered as markdown,
// like the planning view's Epic record panel); an Edit button switches into
// *edit mode* (a form over the same fields), and Save / Cancel returns to
// view mode. Saves go through `PATCH /epics/:id`; the server replies with the
// updated epic and broadcasts the same `epic_updated` frame the agent's
// `update_epic` tool produces, so every other view of this epic (and a second
// open tab) updates live.
//
// Live updates vs. local edits: the view subscribes to `epic:<id>` (reusing
// the DAG stream composable — only the `epic` slice of its state is used) and
// folds each `epic_updated` into the draft via `applyLiveEpic`, which follows
// the server for fields the user hasn't touched and never clobbers an unsaved
// local edit. The epic's *status* is deliberately not editable here — lane
// transitions are governed by `POST /epics/:id/lane`'s transition table.
const props = defineProps<{ id: string }>();

const auth = useAuthStore();
const state = reactive<DagState>(initialDagState());
const loading = ref(true);
const error = ref<string | null>(null);
const editing = ref(false);
const saving = ref(false);
const savedFlash = ref(false);
const streamStatus = ref<StreamStatus>("connecting");
// The breadcrumb's project name (the epic only carries `project_id`); fills in
// after load and falls back to "…" if the fetch fails.
const projectName = ref<string | null>(null);

// The local edit buffer and the last-known server values it diffs against.
// Only meaningful in edit mode; view mode renders straight from `state.epic`.
const draft = reactive<EpicDraft>({ title: "", product_context: "", technical_context: "" });
const baseline = reactive<EpicDraft>({ title: "", product_context: "", technical_context: "" });

const epic = computed(() => state.epic);
const projectId = computed<string | null>(() => state.epic?.project_id ?? null);
const dirty = computed(() => isDirty(baseline, draft));
const titleEmpty = computed(() => draft.title.trim().length === 0);
const saveDisabled = computed(() => saving.value || !dirty.value || titleEmpty.value);

// The live stream is opened after the async hydrate (below), so cleanup is
// registered here synchronously and wired to it once it exists.
let stream: ReturnType<typeof useDagStream> | null = null;
onBeforeUnmount(() => stream?.close());

let flashTimer: ReturnType<typeof setTimeout> | null = null;
onBeforeUnmount(() => {
  if (flashTimer !== null) {
    clearTimeout(flashTimer);
  }
});

function bounceIfAuth(err: unknown): boolean {
  if (err instanceof ApiError && err.isAuth) {
    auth.logout(`Token rejected (401): ${err.message}. Please re-enter it.`);
    return true;
  }
  return false;
}

async function load() {
  const token = auth.token;
  if (token === null) {
    return;
  }
  loading.value = true;
  error.value = null;
  try {
    const epicObj = await getEpic(token, props.id);
    // Reuse the DAG state/reducer for its `epic_updated` handling; the DAG
    // itself (nodes/edges) is irrelevant to this view, so hydrate it empty.
    hydrateDag(state, epicObj, { epic_id: epicObj.id, nodes: [], edges: [] });
    Object.assign(baseline, draftFromEpic(epicObj));
    Object.assign(draft, draftFromEpic(epicObj));
    // Only open the live stream once the initial state is in place. Pass our
    // own status ref so no extra watcher is needed (we're past an `await`, so
    // the setup effect scope is no longer current).
    stream = useDagStream(props.id, token, state, streamStatus);
    // Non-blocking + non-fatal: the breadcrumb falls back to "…" without it.
    void getProject(token, epicObj.project_id)
      .then((p) => (projectName.value = p.name))
      .catch((err) => bounceIfAuth(err));
  } catch (err) {
    if (bounceIfAuth(err)) {
      return;
    }
    error.value = err instanceof Error ? err.message : "failed to load the epic";
  } finally {
    loading.value = false;
  }
}

// Fold live `epic_updated` frames (agent edits, lane changes, a second tab's
// manual save) into baseline + draft without clobbering unsaved local edits.
// View mode renders `state.epic` directly, so it always follows the server.
watch(
  () => state.epic,
  (updated) => {
    if (updated !== null && !loading.value) {
      applyLiveEpic(baseline, draft, updated);
    }
  },
);

// Enter edit mode with the draft synced to the last-known server values (any
// stale unsaved edit from a previous edit session is dropped).
function enterEdit() {
  Object.assign(draft, baseline);
  error.value = null;
  savedFlash.value = false;
  editing.value = true;
}

// Cancel edit mode: drop the local edits and return to view mode.
function cancelEdit() {
  Object.assign(draft, baseline);
  error.value = null;
  editing.value = false;
}

async function save() {
  const token = auth.token;
  const body = diffEpicEdits(baseline, draft);
  if (token === null || saveDisabled.value || Object.keys(body).length === 0) {
    return;
  }
  saving.value = true;
  error.value = null;
  try {
    const updated = await updateEpic(token, props.id, body);
    // The server is the source of truth for what was saved (it trims the
    // title); resync both buffers from it. The broadcast `epic_updated` frame
    // arrives over WS too and is an idempotent no-op by then.
    state.epic = updated;
    Object.assign(baseline, draftFromEpic(updated));
    Object.assign(draft, draftFromEpic(updated));
    editing.value = false;
    savedFlash.value = true;
    if (flashTimer !== null) {
      clearTimeout(flashTimer);
    }
    flashTimer = setTimeout(() => (savedFlash.value = false), 2000);
  } catch (err) {
    if (bounceIfAuth(err)) {
      return;
    }
    error.value = err instanceof Error ? err.message : "failed to save the epic";
  } finally {
    saving.value = false;
  }
}

onMounted(load);
</script>
<template>
  <main class="page page-wide">
    <nav class="crumbs">
      <RouterLink :to="{ name: 'projects' }">Projects</RouterLink>
      <template v-if="projectId">
        <span class="sep">/</span>
        <RouterLink :to="{ name: 'project-detail', params: { id: projectId } }">
          {{ projectName ?? "…" }}
        </RouterLink>
      </template>
    </nav>

    <div v-if="loading" class="loading-stack" aria-label="Loading epic">
      <div class="skeleton sk-title" />
      <div class="skeleton sk-block" />
    </div>
    <p v-else-if="error && !epic" class="banner banner-error" role="alert">{{ error }}</p>

    <template v-else-if="epic">
      <header class="head fade-in">
        <div class="head-main">
          <h1 class="page-title">{{ epic.title }}</h1>
          <div class="head-badges">
            <span class="badge">
              <StatusIcon :status="epic.status" :size="11" />
              {{ epic.status }}
            </span>
          </div>
        </div>
        <span class="conn" :data-status="streamStatus">
          {{ streamStatus === "open" ? "live" : streamStatus }}
        </span>
      </header>

      <EpicTabs :id="props.id" tab="details" />

      <p v-if="error && !editing" class="banner banner-error" role="alert">{{ error }}</p>

      <!-- View mode (default): the epic record rendered read-only. -------- -->
      <section v-if="!editing" class="editor card">
        <div class="editor-head">
          <h2>Epic details</h2>
          <div class="editor-head-side">
            <span v-if="savedFlash" class="saved-hint">
              <AppIcon name="check" :size="12" />
              Saved
            </span>
            <button class="btn btn-ghost btn-sm" @click="enterEdit">
              <AppIcon name="pencil" :size="12" />
              Edit
            </button>
          </div>
        </div>

        <dl class="record-props">
          <div class="prop">
            <dt>Title</dt>
            <dd>{{ epic.title }}</dd>
          </div>
          <div class="prop">
            <dt>Status</dt>
            <dd>{{ epic.status }}</dd>
          </div>
        </dl>

        <hr class="divider" />

        <section class="context">
          <h3>Product context</h3>
          <div
            v-if="epic.product_context"
            class="context-body md"
            v-html="renderMarkdown(epic.product_context)"
          />
          <p v-else class="context-empty">
            No product context yet — it fills in during product planning, or add it via Edit.
          </p>
        </section>

        <section class="context">
          <h3>Technical context</h3>
          <div
            v-if="epic.technical_context"
            class="context-body md"
            v-html="renderMarkdown(epic.technical_context)"
          />
          <p v-else class="context-empty">
            No technical context yet — it fills in during technical planning, or add it via Edit.
          </p>
        </section>
      </section>

      <!-- Edit mode: a form over the same fields. Save/Cancel → view mode. - -->
      <form v-else class="editor card" @submit.prevent="save" @keydown.escape="cancelEdit">
        <div class="editor-head">
          <h2>Edit epic details</h2>
        </div>

        <div class="field">
          <label class="label" for="epic-title">Title</label>
          <input
            id="epic-title"
            v-model="draft.title"
            class="input"
            type="text"
            placeholder="Epic title"
            :disabled="saving"
          />
          <p v-if="titleEmpty" class="field-error" role="alert">Title must not be empty.</p>
        </div>

        <div class="field">
          <label class="label" for="epic-product-context">Product context</label>
          <textarea
            id="epic-product-context"
            v-model="draft.product_context"
            class="textarea context-input"
            rows="9"
            placeholder="What and why — the planning agent fills this in during product planning; edit it directly here. Markdown supported."
            :disabled="saving"
          ></textarea>
        </div>

        <div class="field">
          <label class="label" for="epic-technical-context">Technical context</label>
          <textarea
            id="epic-technical-context"
            v-model="draft.technical_context"
            class="textarea context-input"
            rows="9"
            placeholder="How — the technical approach from technical planning; edit it directly here. Markdown supported."
            :disabled="saving"
          ></textarea>
        </div>

        <p v-if="error" class="banner banner-error" role="alert">{{ error }}</p>

        <div class="editor-foot">
          <span class="save-hint">
            Changes save to the epic record and update every open view live.
          </span>
          <div class="editor-actions">
            <button type="button" class="btn btn-ghost" :disabled="saving" @click="cancelEdit">
              Cancel
            </button>
            <button type="submit" class="btn btn-primary" :disabled="saveDisabled">
              {{ saving ? "Saving…" : "Save changes" }}
            </button>
          </div>
        </div>
      </form>
    </template>
  </main>
</template>

<style scoped>
.head {
  display: flex;
  align-items: flex-start;
  justify-content: space-between;
  gap: var(--spacing-16);
  margin-bottom: var(--spacing-16);
}

.head-main {
  display: flex;
  flex-direction: column;
  gap: var(--spacing-8);
  min-width: 0;
}

.head-badges {
  display: flex;
  align-items: center;
  gap: var(--spacing-8);
  flex-wrap: wrap;
}

.editor {
  display: flex;
  flex-direction: column;
  gap: var(--spacing-16);
  padding: var(--spacing-20);
  max-width: 760px;
}

.editor-head {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: var(--spacing-12);
}

.editor-head h2 {
  font-size: var(--text-caption);
  font-weight: var(--weight-medium);
}

.editor-head-side {
  display: flex;
  align-items: center;
  gap: var(--spacing-8);
}

.saved-hint {
  display: inline-flex;
  align-items: center;
  gap: 5px;
  font-size: 11px;
  color: var(--color-pulse-green);
}

/* --- View mode (mirrors the planning view's Epic record panel) ------------ */

.record-props {
  display: flex;
  flex-direction: column;
  gap: var(--spacing-12);
}

.prop dt {
  font-size: 11px;
  font-weight: var(--weight-medium);
  color: var(--text-faint);
  text-transform: uppercase;
  letter-spacing: 0.04em;
  margin-bottom: 3px;
}

.prop dd {
  font-size: var(--text-caption);
  color: var(--text-body);
}

.context h3 {
  font-size: var(--text-label);
  font-weight: var(--weight-medium);
  color: var(--text-muted);
  margin-bottom: var(--spacing-8);
}

.context-body {
  word-break: break-word;
  font-size: var(--text-caption);
  line-height: 1.55;
  color: var(--text-body);
  padding: var(--spacing-12);
  border: 1px solid var(--border-hairline);
  border-radius: var(--radius-buttons);
  background: rgba(255, 255, 255, 0.015);
  max-height: 320px;
  overflow-y: auto;
}

.context-empty {
  font-size: var(--text-label);
  color: var(--text-faint);
}

/* --- Edit mode ------------------------------------------------------------ */

.field {
  display: flex;
  flex-direction: column;
  gap: var(--spacing-8);
}

.context-input {
  font-family: var(--font-mono);
  font-size: var(--text-caption);
  line-height: 1.55;
  resize: vertical;
  min-height: 140px;
}

.editor-foot {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: var(--spacing-12);
  border-top: 1px solid var(--border-hairline);
  padding-top: var(--spacing-16);
}

.save-hint {
  font-size: 11px;
  color: var(--text-faint);
}

.editor-actions {
  display: flex;
  align-items: center;
  gap: var(--spacing-8);
}

.loading-stack {
  display: flex;
  flex-direction: column;
  gap: var(--spacing-16);
}

.sk-title {
  height: 28px;
  width: 280px;
}

.sk-block {
  height: 320px;
}
</style>
