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
import { triggerBreakdown } from "../api/tasks";
import {
  appendUserTurn,
  hydrate,
  initialState,
  type PlanningState,
} from "../planning/stream";
import { useEpicStream, type EpicStream, type StreamStatus } from "../planning/useEpicStream";

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
  <main>
    <p class="crumb">
      <RouterLink :to="{ name: 'projects' }">← Projects</RouterLink>
      <template v-if="projectId">
        <span class="sep">/</span>
        <RouterLink :to="{ name: 'project-detail', params: { id: projectId } }">
          Project
        </RouterLink>
      </template>
    </p>

    <p v-if="loading">Loading…</p>
    <p v-else-if="error && !state.epic" class="error" role="alert">{{ error }}</p>

    <template v-else-if="state.epic">
      <header>
        <div>
          <h1>{{ state.epic.title }}</h1>
          <span class="status" :data-status="state.epic.status">
            {{ state.epic.status }}
          </span>
          <span class="phase" :data-phase="currentPhase">
            {{ currentPhase === "technical" ? "Technical planning" : "Product planning" }}
          </span>
        </div>
        <span class="conn" :data-status="streamStatus">
          {{ streamStatus === "open" ? "live" : streamStatus }}
        </span>
      </header>

      <!-- Advance product → technical planning (T-205). Shown only while still in
           product planning; disappears once the technical phase is active. -->
      <div v-if="canAdvance" class="advance-bar">
        <p>
          Done defining the product? Advance to <strong>technical planning</strong> — the agent
          will inspect the codebase and plan the technical approach on this same transcript.
        </p>
        <button :disabled="advanceDisabled" @click="advance">
          {{ advancing ? "Advancing…" : "Advance to technical planning" }}
        </button>
      </div>

      <!-- Breakdown: offered once technical planning has begun and the epic is
           still in the Planning lane (T-301/T-303). Runs the one-shot
           breakdown agent and drops the user in the live DAG editor. -->
      <div v-if="canBreakDown" class="advance-bar breakdown-bar">
        <p>
          Planning look good? Run <strong>breakdown</strong> — the agent turns this epic into a
          task DAG you can hand-edit in the Ready lane before execution.
        </p>
        <button :disabled="breakDownDisabled" @click="breakDown">
          {{ breakingDown ? "Starting…" : "Break down into tasks" }}
        </button>
      </div>

      <div class="panes">
        <!-- Chat panel ------------------------------------------------------ -->
        <section class="chat">
          <div ref="scroller" class="transcript">
            <p v-if="state.turns.length === 0 && !state.streaming" class="empty">
              Say what you want to build. The planning agent will ask questions and
              fill in the epic as you talk.
            </p>

            <template v-for="item in transcriptItems" :key="item.turn.id">
              <div v-if="item.dividerBefore" class="phase-divider">
                <span>Technical planning</span>
              </div>
              <div class="turn" :data-role="item.turn.role">
                <template v-if="item.turn.role === 'tool'">
                  <span class="tool-chip" :data-status="item.turn.tool?.status">
                    <span class="tool-name">{{ item.turn.tool?.name }}</span>
                    <span class="tool-state">{{ item.turn.tool?.status }}</span>
                  </span>
                </template>
                <template v-else>
                  <span class="role">{{ item.turn.role }}</span>
                  <div class="bubble">{{ item.turn.text }}</div>
                </template>
              </div>
            </template>

            <!-- The in-flight agent turn (streams token by token). -->
            <div v-if="state.streaming" class="turn streaming" data-role="agent">
              <span class="role">agent</span>
              <div class="stream-body">
                <div v-if="state.streaming.toolCalls.length" class="tool-row">
                  <span
                    v-for="(call, i) in state.streaming.toolCalls"
                    :key="call.toolCallId || i"
                    class="tool-chip"
                    :data-status="call.status"
                  >
                    <span class="tool-name">{{ call.name }}</span>
                    <span class="tool-state">{{ call.status }}</span>
                  </span>
                </div>
                <div v-if="state.streaming.text" class="bubble">{{ state.streaming.text }}</div>
                <div v-else class="thinking">thinking…</div>
              </div>
            </div>
          </div>

          <p v-if="error" class="error inline" role="alert">{{ error }}</p>

          <div class="composer">
            <textarea
              v-model="draft"
              rows="2"
              :disabled="runInFlight || sending"
              :placeholder="runInFlight ? 'Agent is replying…' : 'Message the planning agent (Enter to send)'"
              @keydown="onKeydown"
            ></textarea>
            <button :disabled="runInFlight || sending || draft.trim().length === 0" @click="send">
              {{ runInFlight ? "Running…" : "Send" }}
            </button>
          </div>
        </section>

        <!-- Live Epic record ------------------------------------------------ -->
        <aside class="record">
          <h2>Epic record</h2>
          <dl>
            <div>
              <dt>Title</dt>
              <dd>{{ state.epic.title }}</dd>
            </div>
            <div>
              <dt>Status</dt>
              <dd>{{ state.epic.status }}</dd>
            </div>
          </dl>

          <section class="context">
            <h3>Product context</h3>
            <div v-if="state.epic.product_context" class="context-body">
              {{ state.epic.product_context }}
            </div>
            <p v-else class="context-empty">Fills in as you plan…</p>
          </section>

          <section class="context">
            <h3>Technical context</h3>
            <div v-if="state.epic.technical_context" class="context-body">
              {{ state.epic.technical_context }}
            </div>
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
main {
  max-width: 72rem;
  margin: 2rem auto;
  padding: 0 1rem;
}
.crumb {
  margin: 0 0 1rem;
}
.crumb a {
  color: #2563eb;
  text-decoration: none;
}
.crumb .sep {
  margin: 0 0.5rem;
  color: #9ca3af;
}
header {
  display: flex;
  align-items: flex-start;
  justify-content: space-between;
  gap: 1rem;
}
header h1 {
  margin: 0 0 0.3rem;
}
.status {
  font-size: 0.8rem;
  padding: 0.1rem 0.5rem;
  border-radius: 999px;
  background: #eef2ff;
  color: #3730a3;
}
.phase {
  font-size: 0.8rem;
  margin-left: 0.4rem;
  padding: 0.1rem 0.5rem;
  border-radius: 999px;
  background: #f3f4f6;
  color: #374151;
}
.phase[data-phase="technical"] {
  background: #ecfdf5;
  color: #065f46;
}
.advance-bar {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 1rem;
  margin-top: 1rem;
  padding: 0.75rem 1rem;
  border: 1px solid #ddd6fe;
  border-radius: 10px;
  background: #f5f3ff;
}
.advance-bar p {
  margin: 0;
  font-size: 0.85rem;
  color: #4c1d95;
}
.advance-bar button {
  flex-shrink: 0;
  font: inherit;
  padding: 0.5rem 1.1rem;
  border: 1px solid #7c3aed;
  border-radius: 8px;
  background: #7c3aed;
  color: #fff;
  cursor: pointer;
}
.advance-bar button:disabled {
  opacity: 0.5;
  cursor: not-allowed;
}
.breakdown-bar {
  border-color: #d1fae5;
  background: #ecfdf5;
}
.breakdown-bar p {
  color: #065f46;
}
.breakdown-bar button {
  border-color: #059669;
  background: #059669;
  color: white;
}
.breakdown-bar button:disabled {
  opacity: 0.5;
  cursor: not-allowed;
}
.phase-divider {
  display: flex;
  align-items: center;
  text-align: center;
  color: #065f46;
  font-size: 0.72rem;
  text-transform: uppercase;
  letter-spacing: 0.06em;
  gap: 0.6rem;
  margin: 0.4rem 0;
}
.phase-divider::before,
.phase-divider::after {
  content: "";
  flex: 1;
  height: 1px;
  background: #a7f3d0;
}
.conn {
  font-size: 0.75rem;
  padding: 0.15rem 0.55rem;
  border-radius: 999px;
  background: #f3f4f6;
  color: #6b7280;
}
.conn[data-status="open"] {
  background: #dcfce7;
  color: #166534;
}
.conn[data-status="connecting"] {
  background: #fef9c3;
  color: #854d0e;
}
.panes {
  display: grid;
  grid-template-columns: 1fr 22rem;
  gap: 1.5rem;
  margin-top: 1.25rem;
  align-items: start;
}
@media (max-width: 52rem) {
  .panes {
    grid-template-columns: 1fr;
  }
}
.chat {
  display: flex;
  flex-direction: column;
  border: 1px solid #e5e7eb;
  border-radius: 10px;
  background: #fff;
  min-height: 28rem;
}
.transcript {
  flex: 1;
  overflow-y: auto;
  max-height: 60vh;
  padding: 1rem;
  display: flex;
  flex-direction: column;
  gap: 0.85rem;
}
.empty {
  color: #6b7280;
}
.turn {
  display: flex;
  flex-direction: column;
  gap: 0.25rem;
}
.turn[data-role="user"] {
  align-items: flex-end;
}
.role {
  font-size: 0.7rem;
  text-transform: uppercase;
  letter-spacing: 0.05em;
  color: #9ca3af;
}
.bubble {
  white-space: pre-wrap;
  word-break: break-word;
  padding: 0.55rem 0.75rem;
  border-radius: 10px;
  background: #f3f4f6;
  max-width: 44rem;
}
.turn[data-role="user"] .bubble {
  background: #2563eb;
  color: #fff;
}
.stream-body {
  display: flex;
  flex-direction: column;
  gap: 0.4rem;
}
.thinking {
  color: #9ca3af;
  font-style: italic;
}
.tool-row {
  display: flex;
  flex-wrap: wrap;
  gap: 0.4rem;
}
.tool-chip {
  display: inline-flex;
  gap: 0.4rem;
  align-items: center;
  font-size: 0.75rem;
  padding: 0.15rem 0.5rem;
  border-radius: 999px;
  border: 1px solid #e5e7eb;
  background: #fafafa;
}
.tool-name {
  font-family: ui-monospace, SFMono-Regular, Menlo, monospace;
}
.tool-state {
  color: #6b7280;
}
.tool-chip[data-status="ok"] {
  border-color: #86efac;
  background: #f0fdf4;
}
.tool-chip[data-status="error"] {
  border-color: #fca5a5;
  background: #fef2f2;
}
.tool-chip[data-status="running"] {
  border-color: #fcd34d;
  background: #fffbeb;
}
.composer {
  display: flex;
  gap: 0.6rem;
  padding: 0.75rem;
  border-top: 1px solid #e5e7eb;
}
.composer textarea {
  flex: 1;
  font: inherit;
  padding: 0.5rem;
  border: 1px solid #d1d5db;
  border-radius: 8px;
  resize: vertical;
}
.composer textarea:disabled {
  background: #f9fafb;
  color: #9ca3af;
}
.composer button {
  font: inherit;
  align-self: flex-end;
  padding: 0.5rem 1.1rem;
  border: 1px solid #2563eb;
  border-radius: 8px;
  background: #2563eb;
  color: #fff;
  cursor: pointer;
}
.composer button:disabled {
  opacity: 0.5;
  cursor: not-allowed;
}
.record {
  border: 1px solid #e5e7eb;
  border-radius: 10px;
  padding: 1.1rem;
  background: #fafafa;
  position: sticky;
  top: 1rem;
}
.record h2 {
  margin: 0 0 0.75rem;
  font-size: 1rem;
}
.record dl {
  margin: 0 0 1rem;
  display: grid;
  gap: 0.6rem;
}
.record dt {
  font-size: 0.75rem;
  font-weight: 600;
  color: #6b7280;
  text-transform: uppercase;
  letter-spacing: 0.04em;
}
.record dd {
  margin: 0.15rem 0 0;
}
.context {
  margin-top: 1rem;
}
.context h3 {
  margin: 0 0 0.4rem;
  font-size: 0.85rem;
}
.context-body {
  white-space: pre-wrap;
  word-break: break-word;
  font-size: 0.9rem;
  line-height: 1.5;
  padding: 0.6rem;
  border: 1px solid #e5e7eb;
  border-radius: 8px;
  background: #fff;
}
.context-empty {
  margin: 0;
  color: #9ca3af;
  font-size: 0.85rem;
}
.error {
  padding: 0.6rem 0.75rem;
  color: #991b1b;
  background: #fee2e2;
  border: 1px solid #fca5a5;
  border-radius: 6px;
}
.error.inline {
  margin: 0 0.75rem;
}
</style>
