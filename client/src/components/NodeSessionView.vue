<script setup lang="ts">
import { computed, nextTick, onBeforeUnmount, onMounted, reactive, ref, watch } from "vue";
import { RouterLink, useRouter } from "vue-router";

import { useAuthStore } from "../stores/auth";
import { ApiError } from "../api/client";
import { getEpic, type Epic } from "../api/epics";
import { getProject } from "../api/projects";
import {
  getNodeSession,
  getParticipants,
  openNodeSession,
  postNodeMessage,
  resolveNode,
  type GraduateInput,
  type NodeMessage,
  type OutOfScopeInput,
  type Participant,
  type ResolveNodeResult,
} from "../api/nodes";
import { getMapNode, type MapNodeView } from "../api/map";
import {
  appendMessage,
  hydrateNode,
  initialNodeState,
  setSession,
  type NodeStreamState,
} from "../node/stream";
import { useNodeStream, type StreamStatus } from "../node/useNodeStream";
import AppIcon from "./AppIcon.vue";
import AppModal from "./AppModal.vue";
import { renderMarkdown } from "../lib/markdown";

// Node session view (wayfinder epic §11): the old linear planning chat
// (PlanningView) refactored down to ONE map node's multi-party conversation.
// The transcript is the node-scoped `node_message` table, every human turn is
// attributed (`actor_user_id` → participant display name), ANY authenticated
// user may post, and the agent's reply streams live over `node:<id>` (the
// RunEvents relay in `dearborn-server/src/node_engine.rs`). The resolve
// affordance triggers the grilling resolution flow (`POST …/resolve`): record
// the decision, graduate the next frontier layer, rule things out of scope.
//
// Reached from the Map graph (click-to-open) via the `epic-node` route; the
// map re-fetches on return, so nothing here has to keep the map in sync.
const props = defineProps<{ id: string; nodeId: string }>();

const auth = useAuthStore();
const router = useRouter();
const state = reactive<NodeStreamState>(initialNodeState());
const node = ref<MapNodeView | null>(null);
const epic = ref<Epic | null>(null);
const participants = ref<Participant[]>([]);
const loading = ref(true);
const error = ref<string | null>(null);
const draft = ref("");
const sending = ref(false);
const streamStatus = ref<StreamStatus>("connecting");
const scroller = ref<HTMLElement | null>(null);
// Set when a post landed while a reply was already in flight: the turn is
// stored but did not start a run — say so instead of leaving the poster
// wondering why nothing answered.
const replyQueued = ref(false);
// The breadcrumb's project name (the epic only carries `project_id`); fills in
// after load and falls back to "…" if the fetch fails.
const projectName = ref<string | null>(null);

// ---- resolve affordance -----------------------------------------------------

const resolveOpen = ref(false);
const resolving = ref(false);
const resolveError = ref<string | null>(null);
const gist = ref("");
const graduates = ref<GraduateDraft[]>([]);
const rulings = ref<RulingDraft[]>([]);
const fog = ref("");
// The outcome of a resolve performed here, surfaced until the view changes.
const outcome = ref<ResolveNodeResult | null>(null);

/** A graduation row being drafted in the resolve form. */
interface GraduateDraft {
  kind: string;
  title: string;
  question: string;
}

/** An out-of-scope row being drafted in the resolve form. */
interface RulingDraft {
  title: string;
  reason: string;
}

// Graduate kinds offered to the human resolver. Task nodes come out of
// breakdown (or the later task-node client work) — a human charting the map
// graduates decisions and investigations, not implementation tasks; and the
// resolve endpoint would additionally require a `task_mode` for them.
const GRADUATE_KINDS = ["grilling", "research", "prototype"] as const;

// ---- derived ----------------------------------------------------------------

/** Kinds with an interactive engine (node_engine.rs `INTERACTIVE_KINDS`). */
function isInteractiveKind(kind: string): boolean {
  return kind === "grilling" || kind === "prototype";
}

/** Kinds that resolve through the grilling bundle (map.rs `MAP_RESHAPING_KINDS`). */
function canReshapeMap(kind: string): boolean {
  return kind === "grilling" || kind === "prototype";
}

const isInteractive = computed(() => node.value !== null && isInteractiveKind(node.value.kind));
const isSettled = computed(
  () =>
    node.value !== null &&
    (node.value.state === "resolved" || node.value.state === "out_of_scope"),
);
/** The composer only exists for live interactive nodes. */
const canPost = computed(() => isInteractive.value && !isSettled.value);
const canResolve = computed(
  () => node.value !== null && canReshapeMap(node.value.kind) && !isSettled.value,
);

const KIND_LABELS: Record<string, string> = {
  grilling: "Grilling",
  research: "Research",
  prototype: "Prototype",
  task: "Task",
};

function kindLabel(kind: string): string {
  return KIND_LABELS[kind] ?? kind;
}

/** Participant display names keyed by user id (attribution, plan §9). */
const participantNames = computed(() => {
  const names = new Map<string, string>();
  for (const p of participants.value) {
    names.set(p.id, p.display_name || p.username);
  }
  return names;
});

/** Who a transcript turn belongs to: the human's name, or the agent. */
function actorName(message: NodeMessage): string {
  if (message.role === "agent") {
    return "Agent";
  }
  if (message.actor_user_id === null) {
    return kindLabel(message.role);
  }
  return (
    participantNames.value.get(message.actor_user_id) ??
    `user ${message.actor_user_id.slice(0, 8)}`
  );
}

function formatTime(ts: number): string {
  return new Date(ts).toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" });
}

/** A run is in flight while the reducer holds a streaming turn. */
const runInFlight = computed(() => state.streaming !== null);

// ---- lifecycle --------------------------------------------------------------

// The live stream is opened after the async hydrate (below), so cleanup is
// registered here synchronously and wired to it once it exists. A blocked-by
// chip navigates to another node's session in-place (same route name), so the
// watcher tears the old stream down and reloads.
let stream: ReturnType<typeof useNodeStream> | null = null;
onBeforeUnmount(() => stream?.close());

watch(
  () => [props.id, props.nodeId],
  () => {
    stream?.close();
    stream = null;
    void load();
  },
);

function bounceIfAuth(err: unknown): boolean {
  if (err instanceof ApiError && err.isAuth) {
    auth.logout(`Token rejected (401): ${err.message}. Please re-enter it.`);
    return true;
  }
  return false;
}

async function load() {
  const token = auth.accessToken;
  if (token === null) {
    return;
  }
  // A param-change reload (e.g. a blocked-by chip) must tear down the old
  // node's stream before re-hydrating.
  stream?.close();
  stream = null;
  loading.value = true;
  error.value = null;
  outcome.value = null;
  replyQueued.value = false;
  Object.assign(state, initialNodeState());
  try {
    const [epicObj, nodeObj, participantsList] = await Promise.all([
      getEpic(token, props.id),
      getMapNode(token, props.id, props.nodeId),
      getParticipants(token, props.id).catch(() => [] as Participant[]),
    ]);
    epic.value = epicObj;
    node.value = nodeObj;
    participants.value = participantsList;

    // Interactive kinds open (or resume) their node-scoped session; a settled
    // node's transcript is read-only, and one that was never opened simply has
    // no session row — don't create one just to display it.
    if (isInteractiveKind(nodeObj.kind)) {
      if (nodeObj.state === "open" || nodeObj.state === "in_progress") {
        hydrateNode(state, props.nodeId, await openNodeSession(token, props.id, props.nodeId));
      } else {
        try {
          hydrateNode(state, props.nodeId, await getNodeSession(token, props.id, props.nodeId));
        } catch (err) {
          if (!(err instanceof ApiError && err.status === 404)) {
            throw err;
          }
        }
      }
      stream = useNodeStream(props.nodeId, () => auth.ensureFresh(), state, streamStatus, resync);
    }

    // Non-blocking + non-fatal: the breadcrumb falls back to "…" without it.
    void getProject(token, epicObj.project_id)
      .then((p) => (projectName.value = p.name))
      .catch((err) => bounceIfAuth(err));
  } catch (err) {
    if (bounceIfAuth(err)) {
      return;
    }
    error.value = err instanceof Error ? err.message : "failed to load the node session";
  } finally {
    loading.value = false;
  }
}

/**
 * Re-hydrate the session + transcript from REST. Invoked by the stream
 * composable on every WS *re*-subscribe, healing frames missed while offline.
 */
async function resync() {
  const token = auth.accessToken;
  if (token === null || !isInteractive.value) {
    return;
  }
  try {
    hydrateNode(state, props.nodeId, await getNodeSession(token, props.id, props.nodeId));
  } catch {
    // A 404 (never opened) or a transient failure — keep what we have.
  }
}

// ---- posting ----------------------------------------------------------------

async function send() {
  const token = auth.accessToken;
  const content = draft.value.trim();
  if (token === null || content.length === 0 || sending.value || !canPost.value) {
    return;
  }
  sending.value = true;
  error.value = null;
  try {
    // The REST response carries the stored turn (id + seq); the WS fan-out of
    // the same turn dedupes in the reducer. A reply run only starts when the
    // per-node lock was free (`reply_started`).
    const result = await postNodeMessage(token, props.id, props.nodeId, content);
    draft.value = "";
    replyQueued.value = !result.reply_started;
    appendMessage(state, result.message);
  } catch (err) {
    if (bounceIfAuth(err)) {
      return;
    }
    error.value = err instanceof Error ? err.message : "failed to send message";
  } finally {
    sending.value = false;
  }
}

// The queued note is moot once a reply starts streaming for the next turn.
watch(runInFlight, (inFlight) => {
  if (inFlight) {
    replyQueued.value = false;
  }
});

function onKeydown(event: KeyboardEvent) {
  if (event.key === "Enter" && !event.shiftKey) {
    event.preventDefault();
    void send();
  }
}

// ---- resolution -------------------------------------------------------------

function openResolve() {
  gist.value = "";
  graduates.value = [];
  rulings.value = [];
  fog.value = "";
  resolveError.value = null;
  resolveOpen.value = true;
}

function addGraduate() {
  graduates.value.push({ kind: "grilling", title: "", question: "" });
}

function addRuling() {
  rulings.value.push({ title: "", reason: "" });
}

async function submitResolve() {
  const token = auth.accessToken;
  if (token === null || resolving.value || node.value === null) {
    return;
  }
  const decision = gist.value.trim();
  if (decision.length === 0) {
    resolveError.value = "Record the decision this node settled — one line.";
    return;
  }
  const graduations: GraduateInput[] = [];
  for (const row of graduates.value) {
    const title = row.title.trim();
    if (title.length === 0) {
      resolveError.value = "Every graduated node needs a title.";
      return;
    }
    const question = row.question.trim();
    graduations.push({ kind: row.kind, title, question: question || undefined });
  }
  const outOfScope: OutOfScopeInput[] = [];
  for (const row of rulings.value) {
    const title = row.title.trim();
    const reason = row.reason.trim();
    if (title.length === 0 || reason.length === 0) {
      resolveError.value = "An out-of-scope ruling states what AND why.";
      return;
    }
    outOfScope.push({ title, reason });
  }
  const fogTrimmed = fog.value.trim();

  resolving.value = true;
  resolveError.value = null;
  try {
    const result = await resolveNode(token, props.id, props.nodeId, {
      gist: decision,
      graduations: graduations.length > 0 ? graduations : undefined,
      out_of_scope: outOfScope.length > 0 ? outOfScope : undefined,
      trim_fog: fogTrimmed.length > 0 ? fogTrimmed : undefined,
    });
    // Adopt the resolved node; the recomputed map has already fanned out to
    // any open Map views as `map_updated`.
    node.value = { ...node.value!, ...result.node };
    if (state.session !== null) {
      setSession(state, { ...state.session, status: "complete" });
    }
    outcome.value = result;
    resolveOpen.value = false;
  } catch (err) {
    if (bounceIfAuth(err)) {
      return;
    }
    resolveError.value = err instanceof Error ? err.message : "failed to resolve the node";
  } finally {
    resolving.value = false;
  }
}

/** Human phrasing for the resolve outcome summary. */
const outcomeSummary = computed(() => {
  const result = outcome.value;
  if (result === null) {
    return null;
  }
  const parts: string[] = [];
  if (result.created.length > 0) {
    parts.push(
      `graduated ${result.created.length} new node${result.created.length === 1 ? "" : "s"}`,
    );
  }
  if (result.out_of_scope.length > 0) {
    parts.push(
      `ruled ${result.out_of_scope.length} thing${result.out_of_scope.length === 1 ? "" : "s"} out of scope`,
    );
  }
  if (result.updated.length > 0) {
    parts.push(`updated ${result.updated.length} node${result.updated.length === 1 ? "" : "s"}`);
  }
  if (result.document !== null) {
    parts.push(`document at v${result.document.version}`);
  }
  return parts;
});

// Keep the transcript pinned to the newest content as it streams in.
watch(
  () => [state.messages.length, state.streaming?.text, state.streaming?.toolCalls.length],
  () => {
    void nextTick(() => {
      const el = scroller.value;
      if (el) {
        el.scrollTop = el.scrollHeight;
      }
    });
  },
);

function openBlockedBy(nodeId: string): void {
  void router.push({ name: "epic-node", params: { id: props.id, nodeId } });
}

onMounted(load);
</script>

<template>
  <main class="page page-wide node-session">
    <nav class="crumbs">
      <RouterLink :to="{ name: 'projects' }">Projects</RouterLink>
      <template v-if="epic">
        <span class="sep">/</span>
        <RouterLink :to="{ name: 'project-detail', params: { id: epic.project_id } }">
          {{ projectName ?? "…" }}
        </RouterLink>
        <span class="sep">/</span>
        <RouterLink :to="{ name: 'epic-map', params: { id: epic.id } }">
          {{ epic.title }}
        </RouterLink>
      </template>
    </nav>

    <div v-if="loading" class="loading-stack" aria-label="Loading node session">
      <div class="skeleton sk-title" />
      <div class="skeleton sk-block" />
    </div>
    <p v-else-if="error && !node" class="banner banner-error" role="alert">{{ error }}</p>

    <template v-else-if="node">
      <header class="head fade-in">
        <div class="head-main">
          <h1 class="page-title">{{ node.title }}</h1>
          <div class="head-badges">
            <span class="kind-pill" :data-kind="node.kind">{{ kindLabel(node.kind) }}</span>
            <span class="badge" :data-tone="node.state === 'resolved' ? 'green' : node.state === 'out_of_scope' ? 'neutral' : node.state === 'in_progress' ? 'teal' : 'violet'">
              {{ node.state.replace("_", " ") }}
            </span>
            <span v-if="node.task_mode" class="badge">{{ node.task_mode }}</span>
          </div>
        </div>
        <span
          v-if="isInteractive"
          class="conn"
          :data-status="streamStatus"
        >{{ streamStatus === "open" ? "live" : streamStatus }}</span>
      </header>

      <p v-if="error && !isInteractive" class="banner banner-error" role="alert">{{ error }}</p>

      <!-- Resolve outcome (this client's own resolve; the map re-renders live). -->
      <div v-if="outcome" class="banner banner-resolved" role="status">
        <AppIcon name="check" :size="13" />
        <span>
          Decision recorded.
          <template v-if="outcomeSummary">({{ outcomeSummary.join(", ") }}.)</template>
          <RouterLink class="outcome-link" :to="{ name: 'epic-map', params: { id: props.id } }">
            Back to the map
          </RouterLink>
        </span>
      </div>

      <div class="panes">
        <!-- Conversation --------------------------------------------------- -->
        <section class="chat card">
          <div v-if="!isInteractive" class="transcript">
            <div class="chat-empty">
              <AppIcon name="box" :size="20" />
              <p>
                {{ kindLabel(node.kind) }} nodes run unattended — there is no conversation to
                join. The run's findings land on the node's decision, visible from the map.
              </p>
            </div>
          </div>
          <div v-else ref="scroller" class="transcript">
            <div v-if="state.messages.length === 0 && !state.streaming" class="chat-empty">
              <AppIcon name="chat" :size="20" />
              <p>
                Open the conversation. Anyone on the team can post here — the grilling agent
                works this node's decision with you, one sharp question at a time.
              </p>
            </div>

            <template v-for="message in state.messages" :key="message.id">
              <div class="turn" :data-role="message.role">
                <span class="role">
                  {{ actorName(message) }}
                  <span class="turn-time">{{ formatTime(message.created_at) }}</span>
                </span>
                <div class="bubble md" v-html="renderMarkdown(message.content)" />
              </div>
            </template>

            <!-- The in-flight agent turn (streams token by token). -->
            <div v-if="state.streaming" class="turn streaming" data-role="agent">
              <span class="role">
                Agent
                <span v-if="state.streaming.ended" class="turn-time">recording…</span>
              </span>
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
                <div
                  v-if="state.streaming.text"
                  class="bubble md"
                  v-html="renderMarkdown(state.streaming.text)"
                />
                <div v-else class="thinking">
                  <span class="thinking-dot" />
                  <span class="thinking-dot" />
                  <span class="thinking-dot" />
                </div>
              </div>
            </div>
          </div>

          <p v-if="replyQueued" class="banner banner-queued inline-note" role="status">
            Your message is stored — a reply was already in flight, so send again once it
            finishes to get a response.
          </p>

          <p v-if="error && isInteractive" class="banner banner-error inline-error" role="alert">
            {{ error }}
          </p>

          <div v-if="canPost" class="composer">
            <textarea
              v-model="draft"
              class="textarea"
              rows="2"
              :disabled="sending"
              :placeholder="runInFlight ? 'Agent is replying — your message will be stored' : 'Post to this node’s conversation'"
              @keydown="onKeydown"
            ></textarea>
            <div class="composer-foot">
              <span class="composer-hint">
                Anyone can post · <kbd class="kbd">↵</kbd> to send ·
                <kbd class="kbd">⇧↵</kbd> for newline
              </span>
              <button
                class="btn btn-primary"
                :disabled="sending || draft.trim().length === 0"
                @click="send"
              >
                <AppIcon name="send" :size="13" />
                Send
              </button>
            </div>
          </div>
          <div v-else-if="isInteractive && isSettled" class="composer composer-closed">
            <p class="composer-hint">
              This node is {{ node.state.replace("_", " ") }} — its session is complete and the
              conversation is read-only.
            </p>
          </div>
        </section>

        <!-- Node record ----------------------------------------------------- -->
        <aside class="rail card">
          <div class="rail-head">
            <h2>Node</h2>
          </div>

          <div v-if="canResolve && !isSettled" class="resolve-box">
            <p class="resolve-copy">
              Decision settled? Resolve the node — record the decision, graduate the next layer,
              and rule things out of scope.
            </p>
            <button class="btn btn-white" @click="openResolve">
              <AppIcon name="check" :size="13" />
              Resolve decision
            </button>
          </div>
          <div v-else-if="isSettled" class="resolve-box resolve-done">
            <AppIcon name="check" :size="13" />
            <span>Settled{{ node.resolved_by !== null && participantNames.get(node.resolved_by) ? ` by ${participantNames.get(node.resolved_by)}` : "" }}.</span>
          </div>

          <dl class="rail-props">
            <div v-if="node.question" class="prop">
              <dt>Question</dt>
              <dd>{{ node.question }}</dd>
            </div>
            <div v-if="node.gist" class="prop">
              <dt>Decision</dt>
              <dd>{{ node.gist }}</dd>
            </div>
            <div v-if="node.out_of_scope_reason" class="prop">
              <dt>Why out of scope</dt>
              <dd>{{ node.out_of_scope_reason }}</dd>
            </div>
            <div v-if="node.blocked_by.length" class="prop">
              <dt>Blocked by</dt>
              <dd class="chip-row">
                <button
                  v-for="blocker in node.blocked_by"
                  :key="blocker"
                  class="chip chip-link"
                  type="button"
                  @click="openBlockedBy(blocker)"
                >
                  {{ blocker.slice(0, 8) }}
                </button>
              </dd>
            </div>
          </dl>
        </aside>
      </div>

      <!-- Resolve modal: the grilling resolution flow, driven by a human. -->
      <AppModal :open="resolveOpen" title="Resolve decision" :width="560" @close="resolveOpen = false">
        <div class="resolve-form">
          <label class="field">
            <span class="field-label">Decision</span>
            <textarea
              v-model="gist"
              class="textarea"
              rows="2"
              placeholder="One line: what was decided?"
            ></textarea>
          </label>

          <div class="field-group">
            <div class="field-group-head">
              <span class="field-label">Graduate next nodes</span>
              <button class="btn btn-ghost btn-sm" type="button" @click="addGraduate">
                <AppIcon name="plus" :size="11" />
                Add
              </button>
            </div>
            <p class="field-hint">
              Each becomes an open frontier node, unblocked by this resolution.
            </p>
            <div v-for="(g, i) in graduates" :key="i" class="draft-row">
              <select v-model="g.kind" class="select" aria-label="Kind">
                <option v-for="k in GRADUATE_KINDS" :key="k" :value="k">{{ kindLabel(k) }}</option>
              </select>
              <input v-model="g.title" class="input" placeholder="Title" aria-label="Title" />
              <input
                v-model="g.question"
                class="input"
                placeholder="Question (optional)"
                aria-label="Question"
              />
              <button
                class="btn btn-icon"
                type="button"
                aria-label="Remove node"
                @click="graduates.splice(i, 1)"
              >
                <AppIcon name="x" :size="12" />
              </button>
            </div>
          </div>

          <div class="field-group">
            <div class="field-group-head">
              <span class="field-label">Rule out of scope</span>
              <button class="btn btn-ghost btn-sm" type="button" @click="addRuling">
                <AppIcon name="plus" :size="11" />
                Add
              </button>
            </div>
            <p class="field-hint">Each records a closed node with its reason and a prose line.</p>
            <div v-for="(r, i) in rulings" :key="i" class="draft-row">
              <input v-model="r.title" class="input" placeholder="What" aria-label="What" />
              <input v-model="r.reason" class="input" placeholder="Why" aria-label="Why" />
              <button
                class="btn btn-icon"
                type="button"
                aria-label="Remove ruling"
                @click="rulings.splice(i, 1)"
              >
                <AppIcon name="x" :size="12" />
              </button>
            </div>
          </div>

          <label class="field">
            <span class="field-label">Remaining fog</span>
            <textarea
              v-model="fog"
              class="textarea"
              rows="2"
              placeholder="In-scope decisions that are still fog (replaces the current prose)"
            ></textarea>
          </label>
        </div>

        <template #footer>
          <p v-if="resolveError" class="banner banner-error modal-error" role="alert">
            {{ resolveError }}
          </p>
          <div class="modal-actions">
            <button class="btn" :disabled="resolving" @click="resolveOpen = false">Cancel</button>
            <button class="btn btn-primary" :disabled="resolving" @click="submitResolve">
              <AppIcon name="check" :size="13" />
              {{ resolving ? "Resolving…" : "Resolve node" }}
            </button>
          </div>
        </template>
      </AppModal>
    </template>
  </main>
</template>

<style scoped>
.node-session {
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

.kind-pill {
  font-size: var(--text-micro);
  font-weight: var(--weight-medium);
  text-transform: uppercase;
  letter-spacing: 0.06em;
  color: var(--text-muted);
  border: 1px solid var(--border-hairline);
  border-radius: var(--radius-pills);
  padding: 2px 8px;
}

.banner-resolved {
  display: flex;
  align-items: center;
  gap: var(--spacing-8);
  border-color: rgba(39, 166, 68, 0.35);
  color: #4ec96b;
  margin-bottom: var(--spacing-12);
}

.outcome-link {
  margin-left: var(--spacing-8);
  color: var(--text-body);
  text-decoration: underline;
}

.banner-queued {
  display: flex;
  align-items: center;
  gap: var(--spacing-8);
  color: var(--text-muted);
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

/* --- Conversation --------------------------------------------------------- */

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
  max-width: 320px;
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

.turn-time {
  margin-left: 6px;
  font-weight: var(--weight-normal);
  color: var(--text-faint);
  opacity: 0.7;
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

.turn[data-role="system"] .bubble {
  padding: 0;
  color: var(--text-faint);
  font-size: var(--text-caption);
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

/* Tool call chips (.tool-row/.tool-chip et al.) are global utilities in
   client/src/styles/ui.css so other panels can reuse them. */

.inline-error,
.inline-note {
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

.composer-closed {
  padding: var(--spacing-12) var(--spacing-16);
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

/* --- Node record rail ------------------------------------------------------ */

.rail {
  padding: var(--spacing-16) var(--spacing-20);
  position: sticky;
  top: var(--spacing-16);
  display: flex;
  flex-direction: column;
  gap: var(--spacing-16);
}

.rail-head h2 {
  font-size: var(--text-caption);
  font-weight: var(--weight-medium);
}

.resolve-box {
  display: flex;
  flex-direction: column;
  gap: var(--spacing-10);
  padding: var(--spacing-12);
  border: 1px solid var(--border-hairline);
  border-radius: var(--radius-cards);
  background: rgba(255, 255, 255, 0.015);
}

.resolve-copy {
  margin: 0;
  font-size: var(--text-caption);
  color: var(--text-muted);
  line-height: 1.5;
}

.resolve-done {
  flex-direction: row;
  align-items: center;
  gap: 6px;
  color: #4ec96b;
  font-size: var(--text-caption);
}

.rail-props {
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
  margin: 0;
  font-size: var(--text-caption);
  color: var(--text-body);
  white-space: pre-wrap;
  line-height: 1.5;
}

.chip-row {
  display: flex;
  align-items: center;
  flex-wrap: wrap;
  gap: 6px;
}

.chip-link {
  cursor: pointer;
}

.chip-link:hover {
  color: var(--text-primary);
  border-color: var(--border-strong);
}

/* --- Resolve form ----------------------------------------------------------- */

.resolve-form {
  display: flex;
  flex-direction: column;
  gap: var(--spacing-16);
}

.field {
  display: flex;
  flex-direction: column;
  gap: 4px;
}

.field-label {
  font-size: var(--text-label);
  font-weight: var(--weight-medium);
  color: var(--text-muted);
}

.field-hint {
  margin: 0;
  font-size: var(--text-micro);
  color: var(--text-faint);
}

.field-group {
  display: flex;
  flex-direction: column;
  gap: 6px;
}

.field-group-head {
  display: flex;
  align-items: center;
  justify-content: space-between;
}

.draft-row {
  display: grid;
  grid-template-columns: 110px minmax(0, 1fr) minmax(0, 1fr) auto;
  gap: 6px;
  align-items: center;
}

@media (max-width: 40rem) {
  .draft-row {
    grid-template-columns: 1fr 1fr;
  }
}

.modal-error {
  margin: 0 0 var(--spacing-8);
}

.modal-actions {
  display: flex;
  justify-content: flex-end;
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
  height: 360px;
}
</style>
