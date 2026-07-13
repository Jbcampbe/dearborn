<script setup lang="ts">
import { computed, nextTick, onBeforeUnmount, onMounted, reactive, ref, watch } from "vue";
import { RouterLink } from "vue-router";

import { useAuthStore } from "../stores/auth";
import { ApiError } from "../api/client";
import { getEpic, getTranscript, postMessage } from "../api/epics";
import {
  appendUserTurn,
  hydrate,
  initialState,
  type PlanningState,
} from "../planning/stream";
import { useEpicStream, type EpicStream, type StreamStatus } from "../planning/useEpicStream";

// Planning chat UI + live Epic record (T-204). Two panes: a streaming chat on
// the left (transcript history + the in-flight agent reply, token by token, with
// tool-call chips) and the Epic record on the right, which fills in live as the
// agent calls `update_epic` (an `epic_updated` WS frame). On mount we hydrate
// from REST (epic + transcript) then open the WebSocket; the pure reducer folds
// every frame into `state`.
const props = defineProps<{ id: string }>();
const PHASE = "product" as const;

const auth = useAuthStore();
const state = reactive<PlanningState>(initialState());
const loading = ref(true);
const error = ref<string | null>(null);
const draft = ref("");
const sending = ref(false);
const streamStatus = ref<StreamStatus>("connecting");
const scroller = ref<HTMLElement | null>(null);
// The live stream is opened after the async hydrate (below), so cleanup is
// registered here synchronously and wired to it once it exists.
let stream: EpicStream | null = null;
onBeforeUnmount(() => stream?.close());

// A run is in flight while the reducer holds a streaming turn; gate the composer.
const runInFlight = computed(() => state.streaming !== null);
const projectId = computed<string | null>(() => state.epic?.project_id ?? null);

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
    const [epic, transcript] = await Promise.all([
      getEpic(token, props.id),
      getTranscript(token, props.id),
    ]);
    hydrate(state, epic, transcript);
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
    await postMessage(token, props.id, PHASE, content);
  } catch (err) {
    if (bounceIfAuth(err)) {
      return;
    }
    error.value = err instanceof Error ? err.message : "failed to send message";
  } finally {
    sending.value = false;
  }
}

// Enter sends; Shift+Enter inserts a newline.
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
        </div>
        <span class="conn" :data-status="streamStatus">
          {{ streamStatus === "open" ? "live" : streamStatus }}
        </span>
      </header>

      <div class="panes">
        <!-- Chat panel ------------------------------------------------------ -->
        <section class="chat">
          <div ref="scroller" class="transcript">
            <p v-if="state.turns.length === 0 && !state.streaming" class="empty">
              Say what you want to build. The planning agent will ask questions and
              fill in the epic as you talk.
            </p>

            <div v-for="turn in state.turns" :key="turn.id" class="turn" :data-role="turn.role">
              <template v-if="turn.role === 'tool'">
                <span class="tool-chip" :data-status="turn.tool?.status">
                  <span class="tool-name">{{ turn.tool?.name }}</span>
                  <span class="tool-state">{{ turn.tool?.status }}</span>
                </span>
              </template>
              <template v-else>
                <span class="role">{{ turn.role }}</span>
                <div class="bubble">{{ turn.text }}</div>
              </template>
            </div>

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
            <p v-else class="context-empty">Technical planning arrives in T-205.</p>
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
