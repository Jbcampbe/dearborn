<script setup lang="ts">
import { computed, nextTick, onBeforeUnmount, onMounted, reactive, ref, watch } from "vue";
import { RouterLink, useRouter } from "vue-router";

import { useAuthStore } from "../stores/auth";
import { ApiError } from "../api/client";
import {
  advancePhase,
  getEpic,
  getSessions,
  getTranscript,
  postMessage,
  type PlanningPhase,
  type PlanningSession,
} from "../api/epics";
import { getProject } from "../api/projects";
import { triggerBreakdown } from "../api/tasks";
import {
  appendUserTurn,
  hydrate,
  initialState,
  type PlanningState,
} from "../planning/stream";
import { useEpicStream, type EpicStream, type StreamStatus } from "../planning/useEpicStream";
import AppIcon from "./AppIcon.vue";
import EpicTabs from "./EpicTabs.vue";
import StatusIcon from "./StatusIcon.vue";
import { renderMarkdown } from "../lib/markdown";

// Planning chat UI + live Epic record (T-204/T-205). Two panes: a streaming chat
// on the left (transcript history + the in-flight agent reply, token by token,
// with tool-call chips) and the Epic record on the right, which fills in live as
// the agent calls `update_epic` (an `epic_updated` WS frame). Planning runs in
// two phases — product then technical — on ONE transcript; the user advances
// with a control, and messages after that carry `phase: "technical"`. On mount we
// hydrate from REST (epic + transcript + sessions) then open the WebSocket; the
// pure reducer folds every frame into `state`.
const props = defineProps<{ id: string }>();

const auth = useAuthStore();
const router = useRouter();
const state = reactive<PlanningState>(initialState());
const loading = ref(true);
const error = ref<string | null>(null);
const draft = ref("");
const sending = ref(false);
const advancing = ref(false);
const breakingDown = ref(false);
const sessions = ref<PlanningSession[]>([]);
const streamStatus = ref<StreamStatus>("connecting");
const scroller = ref<HTMLElement | null>(null);
// The breadcrumb's project name (the epic only carries `project_id`); fills in
// after load and falls back to "…" if the fetch fails.
const projectName = ref<string | null>(null);

// A run is in flight while the reducer holds a streaming turn; gate the composer.
const runInFlight = computed(() => state.streaming !== null);
const projectId = computed<string | null>(() => state.epic?.project_id ?? null);

// Phase state, derived from the epic's planning sessions. The technical session
// only exists after advancing; while it doesn't, we're in the product phase.
const hasTechnical = computed(() => sessions.value.some((s) => s.phase === "technical"));
const currentPhase = computed<PlanningPhase>(() => (hasTechnical.value ? "technical" : "product"));
// Keep the reducer's stamp-phase in sync so live-finalized turns land under the
// active phase (hydrated turns keep their own persisted phase).
watch(currentPhase, (phase) => (state.phase = phase), { immediate: true });

// Advance is offered only while still in product planning; it's disabled until
// there's some product progress, and while a run/advance is in flight.
const canAdvance = computed(() => state.epic !== null && !hasTechnical.value);
const advanceDisabled = computed(
  () =>
    advancing.value ||
    runInFlight.value ||
    (!state.epic?.product_context && state.turns.length === 0),
);

// One continuous transcript, tagged with a divider at the product → technical
// boundary so the two phases read as distinct sections without splitting the list.
const transcriptItems = computed(() =>
  state.turns.map((turn, i) => ({
    turn,
    dividerBefore:
      turn.phase === "technical" && (i === 0 || state.turns[i - 1].phase !== "technical"),
  })),
);
// The live stream is opened after the async hydrate (below), so cleanup is
// registered here synchronously and wired to it once it exists.
let stream: EpicStream | null = null;
onBeforeUnmount(() => stream?.close());

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
    const [epic, transcript, sessionList] = await Promise.all([
      getEpic(token, props.id),
      getTranscript(token, props.id),
      getSessions(token, props.id),
    ]);
    hydrate(state, epic, transcript);
    sessions.value = sessionList;
    // Only open the live stream once the history is in place. Pass our own
    // status ref so no extra watcher is needed (we're past an `await`, so the
    // setup effect scope is no longer current).
    stream = useEpicStream(props.id, token, state, streamStatus);
    // Non-blocking + non-fatal: the breadcrumb falls back to "…" without it.
    void getProject(token, epic.project_id)
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

async function send() {
  const token = auth.token;
  const content = draft.value.trim();
  if (token === null || content.length === 0 || runInFlight.value || sending.value) {
    return;
  }
  sending.value = true;
  error.value = null;
  // Optimistic echo, then trigger the run (its reply streams over WS).
  appendUserTurn(state, content);
  draft.value = "";
  try {
    await postMessage(token, props.id, currentPhase.value, content);
  } catch (err) {
    if (bounceIfAuth(err)) {
      return;
    }
    error.value = err instanceof Error ? err.message : "failed to send message";
  } finally {
    sending.value = false;
  }
}

// Advance product → technical planning: the transcript continues on the same
// sequence, and the composer flips to `phase: "technical"` (via `currentPhase`).
async function advance() {
  const token = auth.token;
  if (token === null || hasTechnical.value || advancing.value || runInFlight.value) {
    return;
  }
  advancing.value = true;
  error.value = null;
  try {
    sessions.value = await advancePhase(token, props.id);
  } catch (err) {
    if (bounceIfAuth(err)) {
      return;
    }
    error.value = err instanceof Error ? err.message : "failed to advance the phase";
  } finally {
    advancing.value = false;
  }
}

// Breakdown is offered once the epic has advanced to technical planning and is
// still in the Planning lane. It runs the one-shot breakdown agent (T-301);
// the DAG populates live in the editor (`epic:<id>` `dag_updated` frames), so we
// navigate there immediately on 202. Disabled while a run/advance/breakdown is
// in flight.
const canBreakDown = computed(
  () => state.epic !== null && state.epic.status === "Planning" && hasTechnical.value,
);
const breakDownDisabled = computed(
  () => advancing.value || runInFlight.value || breakingDown.value,
);
async function breakDown() {
  const token = auth.token;
  if (token === null || !canBreakDown.value || breakDownDisabled.value) {
    return;
  }
  breakingDown.value = true;
  error.value = null;
  try {
    await triggerBreakdown(token, props.id);
    // The breakdown run streams on `epic:<id>` and populates the DAG live; the
    // editor subscribes to that same topic, so drop the user there to watch it.
    await router.push({ name: "epic-dag", params: { id: props.id } });
  } catch (err) {
    if (bounceIfAuth(err)) {
      return;
    }
    error.value = err instanceof Error ? err.message : "failed to start breakdown";
  } finally {
    breakingDown.value = false;
  }
}
function onKeydown(event: KeyboardEvent) {
  if (event.key === "Enter" && !event.shiftKey) {
    event.preventDefault();
    void send();
  }
}

// Keep the transcript pinned to the newest content as it streams in.
watch(
  () => [state.turns.length, state.streaming?.text, state.streaming?.toolCalls.length],
  () => {
    void nextTick(() => {
      const el = scroller.value;
      if (el) {
        el.scrollTop = el.scrollHeight;
      }
    });
  },
);

onMounted(load);
</script>
<template>
  <main class="page page-wide planning">
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
    <p v-else-if="error && !state.epic" class="banner banner-error" role="alert">{{ error }}</p>

    <template v-else-if="state.epic">
      <header class="head fade-in">
        <div class="head-main">
          <h1 class="page-title">{{ state.epic.title }}</h1>
          <div class="head-badges">
            <span class="badge">
              <StatusIcon :status="state.epic.status" :size="11" />
              {{ state.epic.status }}
            </span>
            <span class="badge" :data-tone="currentPhase === 'technical' ? 'teal' : 'violet'">
              <AppIcon name="sparkle" :size="11" />
              {{ currentPhase === "technical" ? "Technical planning" : "Product planning" }}
            </span>
          </div>
        </div>
        <span class="conn" :data-status="streamStatus">
          {{ streamStatus === "open" ? "live" : streamStatus }}
        </span>
      </header>

      <EpicTabs :id="props.id" tab="planning" />

      <!-- Advance product → technical planning (T-205). Shown only while still in
           product planning; disappears once the technical phase is active. -->
      <div v-if="canAdvance" class="action-bar">
        <AppIcon name="sparkle" :size="15" class="action-icon" />
        <p>
          Done defining the product? Advance to <strong>technical planning</strong> — the agent
          will inspect the codebase and plan the technical approach on this same transcript.
        </p>
        <button class="btn btn-white" :disabled="advanceDisabled" @click="advance">
          {{ advancing ? "Advancing…" : "Advance to technical" }}
        </button>
      </div>

      <!-- Breakdown: offered once technical planning has begun and the epic is
           still in the Planning lane (T-301/T-303). Runs the one-shot
           breakdown agent and drops the user in the live DAG editor. -->
      <div v-if="canBreakDown" class="action-bar">
        <AppIcon name="diagram" :size="15" class="action-icon" />
        <p>
          Planning look good? Run <strong>breakdown</strong> — the agent turns this epic into a
          task DAG you can hand-edit in the Ready lane before execution.
        </p>
        <button class="btn btn-white" :disabled="breakDownDisabled" @click="breakDown">
          {{ breakingDown ? "Starting…" : "Break down into tasks" }}
        </button>
      </div>

      <div class="panes">
        <!-- Chat panel ------------------------------------------------------ -->
        <section class="chat card">
          <div ref="scroller" class="transcript">
            <div v-if="state.turns.length === 0 && !state.streaming" class="chat-empty">
              <AppIcon name="chat" :size="20" />
              <p>
                Say what you want to build. The planning agent will ask questions and fill
                in the epic as you talk.
              </p>
            </div>

            <template v-for="item in transcriptItems" :key="item.turn.id">
              <div v-if="item.dividerBefore" class="phase-divider">
                <span>Technical planning</span>
              </div>
              <div class="turn" :data-role="item.turn.role">
                <template v-if="item.turn.role === 'tool'">
                  <span class="tool-chip" :data-status="item.turn.tool?.status">
                    <span class="tool-dot" />
                    <span class="tool-name mono">{{ item.turn.tool?.name }}</span>
                    <span class="tool-state">{{ item.turn.tool?.status }}</span>
                  </span>
                </template>
                <template v-else>
                  <span class="role">{{ item.turn.role === "agent" ? "Planning agent" : "You" }}</span>
                  <div class="bubble md" v-html="renderMarkdown(item.turn.text)" />
                </template>
              </div>
            </template>

            <!-- The in-flight agent turn (streams token by token). -->
            <div v-if="state.streaming" class="turn streaming" data-role="agent">
              <span class="role">Planning agent</span>
              <div class="stream-body">
                <div v-if="state.streaming.toolCalls.length" class="tool-row">
                  <span
                    v-for="(call, i) in state.streaming.toolCalls"
                    :key="call.toolCallId || i"
                    class="tool-chip"
                    :data-status="call.status"
                  >
                    <span class="tool-dot" />
                    <span class="tool-name mono">{{ call.name }}</span>
                    <span class="tool-state">{{ call.status }}</span>
                  </span>
                </div>
                <div v-if="state.streaming.text" class="bubble md" v-html="renderMarkdown(state.streaming.text)" />
                <div v-else class="thinking">
                  <span class="thinking-dot" />
                  <span class="thinking-dot" />
                  <span class="thinking-dot" />
                </div>
              </div>
            </div>
          </div>

          <p v-if="error" class="banner banner-error inline-error" role="alert">{{ error }}</p>

          <div class="composer">
            <textarea
              v-model="draft"
              class="textarea"
              rows="2"
              :disabled="runInFlight || sending"
              :placeholder="runInFlight ? 'Agent is replying…' : 'Message the planning agent'"
              @keydown="onKeydown"
            ></textarea>
            <div class="composer-foot">
              <span class="composer-hint">
                <kbd class="kbd">↵</kbd> to send · <kbd class="kbd">⇧↵</kbd> for newline
              </span>
              <button
                class="btn btn-primary"
                :disabled="runInFlight || sending || draft.trim().length === 0"
                @click="send"
              >
                <AppIcon name="send" :size="13" />
                {{ runInFlight ? "Running…" : "Send" }}
              </button>
            </div>
          </div>
        </section>

        <!-- Live Epic record ------------------------------------------------ -->
        <aside class="record card">
          <div class="record-head">
            <h2>Epic record</h2>
          </div>
          <dl class="record-props">
            <div class="prop">
              <dt>Title</dt>
              <dd>{{ state.epic.title }}</dd>
            </div>
            <div class="prop">
              <dt>Status</dt>
              <dd>{{ state.epic.status }}</dd>
            </div>
          </dl>

          <hr class="divider" />

          <section class="context">
            <h3>Product context</h3>
            <div v-if="state.epic.product_context" class="context-body md" v-html="renderMarkdown(state.epic.product_context)" />
            <p v-else class="context-empty">Fills in as you plan…</p>
          </section>

          <section class="context">
            <h3>Technical context</h3>
            <div v-if="state.epic.technical_context" class="context-body md" v-html="renderMarkdown(state.epic.technical_context)" />
            <p v-else class="context-empty">
              {{
                currentPhase === "technical"
                  ? "Fills in as you plan the technical approach…"
                  : "Advance to technical planning to start."
              }}
            </p>
          </section>
        </aside>
      </div>
    </template>
  </main>
</template>

<style scoped>
.planning {
  display: flex;
  flex-direction: column;
  min-height: 100vh;
}

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

.action-bar {
  display: flex;
  align-items: center;
  gap: var(--spacing-12);
  padding: var(--spacing-12) var(--spacing-16);
  margin-bottom: var(--spacing-12);
  border: 1px solid var(--border-hairline);
  border-radius: var(--radius-cards);
  background: var(--surface-carbon);
}

.action-icon {
  color: var(--text-faint);
  flex-shrink: 0;
}

.action-bar p {
  flex: 1;
  font-size: var(--text-caption);
  color: var(--text-muted);
  line-height: 1.5;
}

.action-bar strong {
  color: var(--text-body);
  font-weight: var(--weight-medium);
}

.action-bar .btn {
  flex-shrink: 0;
}

.panes {
  display: grid;
  grid-template-columns: minmax(0, 1fr) 320px;
  gap: var(--spacing-16);
  align-items: start;
  flex: 1;
}

@media (max-width: 64rem) {
  .panes {
    grid-template-columns: 1fr;
  }
}

/* --- Chat ---------------------------------------------------------------- */

.chat {
  display: flex;
  flex-direction: column;
  overflow: hidden;
}

.transcript {
  display: flex;
  flex-direction: column;
  gap: var(--spacing-16);
  padding: var(--spacing-20);
  overflow-y: auto;
  height: 56vh;
  min-height: 320px;
}

.chat-empty {
  display: flex;
  flex-direction: column;
  align-items: center;
  gap: var(--spacing-8);
  margin: auto;
  max-width: 300px;
  text-align: center;
  color: var(--text-faint);
  font-size: var(--text-caption);
  line-height: 1.5;
}

.turn {
  display: flex;
  flex-direction: column;
  align-items: flex-start;
  gap: 4px;
}

.turn[data-role="user"] {
  align-items: flex-end;
}

.role {
  font-size: 11px;
  font-weight: var(--weight-medium);
  color: var(--text-faint);
  letter-spacing: 0.01em;
}

.bubble {
  max-width: 85%;
  white-space: pre-wrap;
  word-break: break-word;
  font-size: 13.5px;
  line-height: 1.55;
  color: var(--text-body);
}

.turn[data-role="user"] .bubble {
  background: rgba(255, 255, 255, 0.06);
  border: 1px solid var(--border-hairline);
  border-radius: var(--radius-cards);
  border-bottom-right-radius: var(--radius-small);
  padding: 8px 12px;
  color: var(--text-primary);
}

.turn[data-role="agent"] .bubble {
  padding: 0;
  border-bottom-left-radius: var(--radius-small);
}

.stream-body {
  display: flex;
  flex-direction: column;
  gap: var(--spacing-8);
  max-width: 85%;
}

.thinking {
  display: inline-flex;
  gap: 5px;
  padding: 4px 0;
}

.thinking-dot {
  width: 5px;
  height: 5px;
  border-radius: var(--radius-pills);
  background: var(--color-ash);
  animation: thinking-bounce 1.2s ease-in-out infinite;
}

.thinking-dot:nth-child(2) {
  animation-delay: 0.15s;
}

.thinking-dot:nth-child(3) {
  animation-delay: 0.3s;
}

@keyframes thinking-bounce {
  0%, 100% { opacity: 0.25; }
  50% { opacity: 1; }
}

.tool-row {
  display: flex;
  flex-wrap: wrap;
  gap: 6px;
}

.tool-chip {
  display: inline-flex;
  align-items: center;
  gap: 6px;
  padding: 3px 9px;
  border-radius: var(--radius-pills);
  border: 1px solid var(--border-hairline);
  background: rgba(255, 255, 255, 0.02);
  font-size: 11.5px;
  color: var(--text-muted);
}

.tool-dot {
  width: 5px;
  height: 5px;
  border-radius: var(--radius-pills);
  background: var(--color-ash);
}

.tool-chip[data-status="running"] .tool-dot {
  background: var(--color-signal-teal);
  animation: pulse-dot 1.2s ease-in-out infinite;
}

.tool-chip[data-status="ok"] .tool-dot {
  background: var(--color-pulse-green);
}

.tool-chip[data-status="error"] .tool-dot {
  background: var(--color-coral-red);
}

.tool-name {
  font-size: 11px;
}

.tool-state {
  color: var(--text-faint);
  font-size: 11px;
}

.phase-divider {
  display: flex;
  align-items: center;
  gap: var(--spacing-12);
  color: var(--color-signal-teal);
  font-size: 11px;
  font-weight: var(--weight-medium);
  text-transform: uppercase;
  letter-spacing: 0.06em;
}

.phase-divider::before,
.phase-divider::after {
  content: "";
  flex: 1;
  height: 1px;
  background: var(--border-hairline);
}

.inline-error {
  margin: 0 var(--spacing-16) var(--spacing-8);
}

.composer {
  border-top: 1px solid var(--border-hairline);
  padding: var(--spacing-12) var(--spacing-16);
  background: var(--surface-carbon);
}

.composer .textarea {
  border: none;
  background: transparent;
  padding: 0;
  min-height: 44px;
  font-size: 13.5px;
}

.composer .textarea:focus {
  border: none;
  background: transparent;
}

.composer-foot {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: var(--spacing-12);
  margin-top: var(--spacing-8);
}

.composer-hint {
  font-size: 11px;
  color: var(--text-faint);
}

.kbd {
  display: inline-block;
  padding: 1px 5px;
  border: 1px solid var(--border-hairline);
  border-bottom-width: 2px;
  border-radius: var(--radius-badges);
  background: rgba(255, 255, 255, 0.03);
  font-family: var(--font-mono);
  font-size: 10px;
  color: var(--text-muted);
}

/* --- Epic record ---------------------------------------------------------- */

.record {
  padding: var(--spacing-16) var(--spacing-20);
  position: sticky;
  top: var(--spacing-16);
}

.record-head {
  display: flex;
  align-items: center;
  justify-content: space-between;
  margin-bottom: var(--spacing-12);
}

.record-head h2 {
  font-size: var(--text-caption);
  font-weight: var(--weight-medium);
}

.record-props {
  display: flex;
  flex-direction: column;
  gap: var(--spacing-12);
  margin-bottom: var(--spacing-16);
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

.context {
  margin-top: var(--spacing-16);
}

.context h3 {
  font-size: var(--text-label);
  font-weight: var(--weight-medium);
  color: var(--text-muted);
  margin-bottom: var(--spacing-8);
}

.context-body {
  white-space: pre-wrap;
  word-break: break-word;
  font-size: var(--text-caption);
  line-height: 1.55;
  color: var(--text-body);
  padding: var(--spacing-12);
  border: 1px solid var(--border-hairline);
  border-radius: var(--radius-buttons);
  background: rgba(255, 255, 255, 0.015);
  max-height: 220px;
  overflow-y: auto;
}

.context-empty {
  font-size: var(--text-label);
  color: var(--text-faint);
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
