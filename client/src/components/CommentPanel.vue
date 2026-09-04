<script setup lang="ts">
import { computed, onBeforeUnmount, onMounted, reactive, ref, watch } from "vue";
import { useRouter } from "vue-router";

import { useAuthStore } from "../stores/auth";
import { ApiError } from "../api/client";
import { getMap } from "../api/map";
import { getDocument } from "../api/document";
import { getParticipants } from "../api/nodes";
import {
  listComments,
  postComment,
  promoteComment,
  resolveComment,
  type Comment,
  type CommentAnchorKind,
} from "../api/comments";
import {
  applyPromotedThread,
  groupThreads,
  hydrateComments,
  initialCommentState,
  type CommentState,
  type CommentThread,
} from "../comments/stream";
import { useCommentStream, type StreamStatus } from "../comments/useCommentStream";
import AppIcon from "./AppIcon.vue";

// The shared comment panel (wayfinder epic §9, client slice — net-new).
// Threaded comments anchored to map nodes OR living-Document sections, with
// flat-permission attribution (every authenticated user can post), thread
// resolve, and the promote-to-node affordance wired to the promotion backend
// (`POST /epics/{id}/comments/:commentId/promote`): a thread becomes a NEW
// open frontier node of a chosen kind (grilling | research | prototype —
// never `task`), carrying optional extra context.
//
// Live via `comments_updated` frames on `epic:<id>` (the full comment list)
// folded through the pure reducer (`comments/stream.ts`) by the composable
// (`comments/useCommentStream.ts`) — the same plumbing as the Map and
// Document views. Node titles / section titles resolve through the map and
// document REST reads; a failed read degrades labels to id prefixes.
const props = defineProps<{
  epicId: string;
  /** Scope the panel to a single anchor; omit both for the epic-wide panel. */
  anchorKind?: CommentAnchorKind;
  anchorId?: string;
}>();

const emit = defineEmits<{ (e: "count", count: number): void }>();

const auth = useAuthStore();
const router = useRouter();
const state = reactive<CommentState>(initialCommentState());
const loading = ref(true);
const error = ref<string | null>(null);
const actionError = ref<string | null>(null);
const streamStatus = ref<StreamStatus>("connecting");

// Attribution: participant display names keyed by user id (the epic-scoped
// participants endpoint works for any authenticated user, unlike /users).
const participantNames = ref<Map<string, string>>(new Map());
// Anchor labels: node id → title, section id → heading.
const nodeTitles = ref<Map<string, string>>(new Map());
const sectionTitles = ref<Map<string, string>>(new Map());

const scoped = computed(
  () => props.anchorKind !== undefined && props.anchorId !== undefined && props.anchorId !== "",
);

/** Threads grouped by anchor, most recently active first. */
const threads = computed(() => groupThreads(state.comments));

// ---- composer (new thread) --------------------------------------------------

const newKind = ref<CommentAnchorKind>("node");
const newAnchorId = ref("");
const newBody = ref("");
const posting = ref(false);
const replyBodies = ref<Record<string, string>>({});
const resolving = ref<string | null>(null);

/** Anchor choices for the epic-wide composer, per selected anchor kind. */
const anchorOptions = computed<{ id: string; label: string }[]>(() => {
  if (newKind.value === "section") {
    return [...sectionTitles.value.entries()].map(([id, title]) => ({ id, label: title }));
  }
  return [...nodeTitles.value.entries()].map(([id, title]) => ({ id, label: title }));
});

// ---- promote-to-node ----------------------------------------------------------

/** Kinds a thread may be promoted into (`comments.rs` `PROMOTABLE_KINDS`). */
const PROMOTE_KINDS = ["grilling", "research", "prototype"] as const;

const KIND_LABELS: Record<string, string> = {
  grilling: "Grilling",
  research: "Research",
  prototype: "Prototype",
  task: "Task",
};

function kindLabel(kind: string): string {
  return KIND_LABELS[kind] ?? kind;
}

const promotingThread = ref<string | null>(null);
const promoteKind = ref<string>("grilling");
const promoteTitle = ref("");
const promoteQuestion = ref("");
const promoting = ref(false);

/** A thread can still be promoted: it has never been promoted before (the
 * server's only gate — a thread becomes one node, once; resolved threads stay
 * promotable, since a settled discussion can still merit a node). */
function canPromote(thread: CommentThread): boolean {
  return thread.promotedNodeId === null;
}

// ---- labels -----------------------------------------------------------------

function bounceIfAuth(err: unknown): boolean {
  if (err instanceof ApiError && err.isAuth) {
    auth.logout(`Token rejected (401): ${err.message}. Please re-enter it.`);
    return true;
  }
  return false;
}

/** Attribution line on a comment: agent runs have no user id. */
function authorLabel(comment: Comment): string {
  if (comment.is_agent || comment.author_user_id === null) {
    return "Agent";
  }
  if (comment.author_user_id === auth.user?.id) {
    return "You";
  }
  return (
    participantNames.value.get(comment.author_user_id) ??
    `user ${comment.author_user_id.slice(0, 8)}`
  );
}

/** The thread's anchor label: node title or section heading, id prefix fallback. */
function anchorLabel(thread: CommentThread): string {
  if (thread.anchorKind === "section") {
    return sectionTitles.value.get(thread.anchorId) ?? thread.anchorId.slice(0, 12);
  }
  return nodeTitles.value.get(thread.anchorId) ?? thread.anchorId.slice(0, 12);
}

function formatTime(ts: number): string {
  return new Date(ts).toLocaleString([], {
    month: "short",
    day: "numeric",
    hour: "2-digit",
    minute: "2-digit",
  });
}

/** Click an anchor chip: a node opens its session, a section the Document. */
function openAnchor(thread: CommentThread): void {
  if (thread.anchorKind === "section") {
    void router.push({ name: "epic-document", params: { id: props.epicId } });
    return;
  }
  void router.push({
    name: "epic-node",
    params: { id: props.epicId, nodeId: thread.anchorId },
  });
}

/** Follow a promotion to the fresh node's session. */
function openPromoted(thread: CommentThread): void {
  if (thread.promotedNodeId !== null) {
    void router.push({
      name: "epic-node",
      params: { id: props.epicId, nodeId: thread.promotedNodeId },
    });
  }
}

// ---- lifecycle ----------------------------------------------------------------

let stream: ReturnType<typeof useCommentStream> | null = null;
onBeforeUnmount(() => stream?.close());

async function load() {
  const token = auth.accessToken;
  if (token === null) {
    return;
  }
  loading.value = true;
  error.value = null;
  try {
    const filter = scoped.value
      ? { anchor_kind: props.anchorKind, anchor_id: props.anchorId }
      : {};
    const comments = await listComments(token, props.epicId, filter);
    hydrateComments(state, props.epicId, comments);
    stream = useCommentStream(
      props.epicId,
      () => auth.ensureFresh(),
      state,
      streamStatus,
      scoped.value ? { anchorKind: props.anchorKind!, anchorId: props.anchorId! } : undefined,
    );

    // Non-blocking + non-fatal enrichments: names and anchor labels degrade
    // to id prefixes when these fail (a 401 bounces via the api layer).
    void getParticipants(token, props.epicId)
      .then((ps) => {
        participantNames.value = new Map(
          ps.map((p) => [p.id, p.display_name || p.username]),
        );
      })
      .catch((err) => bounceIfAuth(err));
    void getMap(token, props.epicId)
      .then((map) => {
        nodeTitles.value = new Map(map.nodes.map((n) => [n.id, n.title]));
      })
      .catch((err) => bounceIfAuth(err));
    void getDocument(token, props.epicId)
      .then((doc) => {
        sectionTitles.value = new Map(
          doc.sections.map((s) => [s.section_id, s.title ?? s.section_id]),
        );
      })
      .catch((err) => bounceIfAuth(err));
  } catch (err) {
    if (bounceIfAuth(err)) {
      return;
    }
    error.value = err instanceof Error ? err.message : "failed to load the comments";
  } finally {
    loading.value = false;
  }
}

onMounted(load);

// Surface the comment count for the host view's toggle badge.
watch(
  () => state.comments.length,
  (count) => emit("count", count),
  { immediate: true },
);

// ---- posting / resolving ------------------------------------------------------

async function postNew(): Promise<void> {
  const token = auth.accessToken;
  const body = newBody.value.trim();
  if (token === null || body === "" || posting.value) {
    return;
  }
  const anchorKind = scoped.value ? props.anchorKind : newKind.value;
  const anchorId = scoped.value ? props.anchorId : newAnchorId.value;
  if (anchorKind === undefined || anchorId === undefined || anchorId === "") {
    actionError.value = "Pick where the thread should anchor — a node or a section.";
    return;
  }
  posting.value = true;
  actionError.value = null;
  try {
    await postComment(token, props.epicId, { anchor_kind: anchorKind, anchor_id: anchorId, body });
    newBody.value = "";
    // `comments_updated` carries the full list; the WS replaces the state. If
    // the socket is down the comment simply appears on the next reload.
  } catch (err) {
    if (!bounceIfAuth(err)) {
      actionError.value = err instanceof Error ? err.message : "failed to post the comment";
    }
  } finally {
    posting.value = false;
  }
}

async function postReply(thread: CommentThread): Promise<void> {
  const token = auth.accessToken;
  const body = (replyBodies.value[thread.threadId] ?? "").trim();
  if (token === null || body === "" || posting.value) {
    return;
  }
  posting.value = true;
  actionError.value = null;
  try {
    await postComment(token, props.epicId, { thread_id: thread.threadId, body });
    replyBodies.value[thread.threadId] = "";
  } catch (err) {
    if (!bounceIfAuth(err)) {
      actionError.value = err instanceof Error ? err.message : "failed to post the reply";
    }
  } finally {
    posting.value = false;
  }
}

async function resolve(thread: CommentThread): Promise<void> {
  const token = auth.accessToken;
  if (token === null || resolving.value !== null) {
    return;
  }
  resolving.value = thread.threadId;
  actionError.value = null;
  try {
    await resolveComment(token, props.epicId, thread.comments[0].id);
  } catch (err) {
    if (!bounceIfAuth(err)) {
      actionError.value = err instanceof Error ? err.message : "failed to resolve the thread";
    }
  } finally {
    resolving.value = null;
  }
}

// ---- promotion ------------------------------------------------------------------

function startPromote(thread: CommentThread): void {
  promotingThread.value = thread.threadId;
  promoteKind.value = "grilling";
  // Blank = the server derives the title from the head comment's first line.
  promoteTitle.value = "";
  promoteQuestion.value = "";
  actionError.value = null;
}

function cancelPromote(): void {
  promotingThread.value = null;
}

async function submitPromote(thread: CommentThread): Promise<void> {
  const token = auth.accessToken;
  if (token === null || promoting.value) {
    return;
  }
  promoting.value = true;
  actionError.value = null;
  try {
    const outcome = await promoteComment(token, props.epicId, thread.comments[0].id, {
      kind: promoteKind.value,
      title: promoteTitle.value.trim() === "" ? undefined : promoteTitle.value.trim(),
      question: promoteQuestion.value.trim() === "" ? undefined : promoteQuestion.value.trim(),
    });
    // Heal the panel immediately (the WS frame repeats it) and keep the fresh
    // node's title resolvable for the anchor chip.
    applyPromotedThread(state, outcome.thread);
    nodeTitles.value = new Map(nodeTitles.value).set(outcome.node.id, outcome.node.title);
    promotingThread.value = null;
  } catch (err) {
    if (!bounceIfAuth(err)) {
      actionError.value = err instanceof Error ? err.message : "failed to promote the thread";
    }
  } finally {
    promoting.value = false;
  }
}
</script>

<template>
  <aside class="comment-panel" aria-label="Comments">
    <header class="panel-head">
      <h2><AppIcon name="chat" :size="12" /> Comments</h2>
      <span class="conn" :data-status="streamStatus">
        {{ streamStatus === "open" ? "live" : streamStatus }}
      </span>
    </header>

    <div v-if="loading" class="panel-loading">
      <div class="skeleton sk-line" />
      <div class="skeleton sk-line" />
    </div>
    <template v-else>
      <p v-if="error" class="banner banner-error" role="alert">{{ error }}</p>

      <!-- New-thread composer: the anchor is fixed when scoped, picked epic-wide. -->
      <form class="composer" @submit.prevent="postNew">
        <div v-if="!scoped" class="anchor-pick">
          <select v-model="newKind" class="select" aria-label="Anchor type">
            <option value="node">Node</option>
            <option value="section">Section</option>
          </select>
          <select v-model="newAnchorId" class="select" aria-label="Anchor">
            <option value="" disabled>Choose…</option>
            <option v-for="opt in anchorOptions" :key="opt.id" :value="opt.id">
              {{ opt.label }}
            </option>
          </select>
        </div>
        <textarea
          v-model="newBody"
          class="textarea"
          rows="2"
          :placeholder="scoped ? 'Start a thread on this anchor…' : 'Start a thread…'"
          aria-label="New comment"
        />
        <div class="composer-foot">
          <span v-if="actionError" class="action-error" role="alert">{{ actionError }}</span>
          <button
            type="submit"
            class="btn btn-primary btn-sm"
            :disabled="posting || newBody.trim() === ''"
          >
            <AppIcon name="send" :size="11" />
            Comment
          </button>
        </div>
      </form>

      <p v-if="threads.length === 0" class="panel-empty">
        No comments yet{{ scoped ? "" : " — start one on a node or a document section" }}.
      </p>

      <section
        v-for="thread in threads"
        :key="thread.threadId"
        class="thread"
        :data-resolved="thread.resolved"
      >
        <header class="thread-head">
          <button type="button" class="anchor-link" @click="openAnchor(thread)">
            <AppIcon :name="thread.anchorKind === 'section' ? 'document' : 'map'" :size="10" />
            <span class="anchor-kind">{{ thread.anchorKind === "section" ? "§" : kindLabel(thread.anchorKind) }}</span>
            {{ anchorLabel(thread) }}
          </button>
          <button
            v-if="thread.promotedNodeId"
            type="button"
            class="promoted-chip"
            :title="`Promoted to node ${anchorLabel({ ...thread, anchorKind: 'node' })}`"
            @click="openPromoted(thread)"
          >
            <AppIcon name="arrow-up-right" :size="10" />
            promoted
          </button>
        </header>

        <div v-for="c in thread.comments" :key="c.id" class="comment">
          <span class="comment-meta">
            <span class="author" :data-agent="c.is_agent">{{ authorLabel(c) }}</span>
            <span class="time">{{ formatTime(c.created_at) }}</span>
          </span>
          <span class="body">{{ c.body }}</span>
        </div>

        <!-- Promote form: kind + optional extra context for the new frontier node. -->
        <form
          v-if="promotingThread === thread.threadId"
          class="promote-form"
          @submit.prevent="submitPromote(thread)"
        >
          <p class="promote-copy">
            Turn this thread into a new open frontier node{{ " " }}
            <span class="anchor-kind">{{ kindLabel(promoteKind) }}</span> — it lands unblocked on the map.
          </p>
          <select v-model="promoteKind" class="select" aria-label="Node kind">
            <option v-for="k in PROMOTE_KINDS" :key="k" :value="k">{{ kindLabel(k) }}</option>
          </select>
          <input
            v-model="promoteTitle"
            class="input"
            placeholder="Title (blank: from the first comment)"
            aria-label="Node title"
          />
          <input
            v-model="promoteQuestion"
            class="input"
            placeholder="Question (optional)"
            aria-label="Node question"
          />
          <div class="promote-actions">
            <button type="button" class="btn btn-sm" :disabled="promoting" @click="cancelPromote">
              Cancel
            </button>
            <button type="submit" class="btn btn-primary btn-sm" :disabled="promoting">
              <AppIcon name="arrow-up-right" :size="11" />
              {{ promoting ? "Promoting…" : "Promote" }}
            </button>
          </div>
        </form>

        <div class="thread-actions">
          <button
            v-if="canPromote(thread)"
            type="button"
            class="ghost-btn"
            @click="startPromote(thread)"
          >
            <AppIcon name="arrow-up-right" :size="10" />
            Promote to node
          </button>
          <button
            v-if="!thread.resolved"
            type="button"
            class="ghost-btn"
            :disabled="resolving === thread.threadId"
            @click="resolve(thread)"
          >
            Resolve
          </button>
          <span v-else class="resolved-tag">Resolved</span>
        </div>

        <form
          v-if="!thread.resolved && promotingThread !== thread.threadId"
          class="reply"
          @submit.prevent="postReply(thread)"
        >
          <textarea
            v-model="replyBodies[thread.threadId]"
            rows="2"
            placeholder="Reply…"
            aria-label="Reply"
          />
          <button
            type="submit"
            class="ghost-btn"
            :disabled="posting || (replyBodies[thread.threadId] ?? '').trim() === ''"
          >
            Reply
          </button>
        </form>
      </section>
    </template>
  </aside>
</template>

<style scoped>
.comment-panel {
  display: flex;
  flex-direction: column;
  gap: var(--spacing-12);
  padding: var(--spacing-12);
  border: 1px solid var(--border-hairline);
  border-radius: var(--radius-cards);
  background: var(--surface-carbon);
  position: sticky;
  top: var(--spacing-16);
  max-height: calc(100vh - 160px);
  overflow-y: auto;
}

.panel-head {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: var(--spacing-8);
}

.panel-head h2 {
  display: flex;
  align-items: center;
  gap: 6px;
  margin: 0;
  font-size: var(--text-micro);
  font-weight: var(--weight-medium);
  text-transform: uppercase;
  letter-spacing: 0.06em;
  color: var(--text-faint);
}

.panel-loading {
  display: flex;
  flex-direction: column;
  gap: var(--spacing-8);
}

.sk-line {
  height: 40px;
}

.panel-empty {
  margin: 0;
  font-size: var(--text-caption);
  color: var(--text-faint);
}

/* Composer ------------------------------------------------------------------ */
.composer {
  display: flex;
  flex-direction: column;
  gap: var(--spacing-8);
  padding: var(--spacing-8);
  border: 1px solid var(--border-hairline);
  border-radius: var(--radius-cards);
  background: var(--surface-obsidian);
}

.anchor-pick {
  display: grid;
  grid-template-columns: 88px minmax(0, 1fr);
  gap: 6px;
}

.composer .textarea {
  width: 100%;
  padding: var(--spacing-8);
  border: 1px solid var(--border-hairline);
  border-radius: var(--radius-cards);
  background: var(--surface-carbon);
  color: var(--text-body);
  font: inherit;
  font-size: var(--text-caption);
  resize: vertical;
  box-sizing: border-box;
}

.composer-foot {
  display: flex;
  align-items: center;
  justify-content: flex-end;
  gap: var(--spacing-8);
}

.action-error {
  flex: 1;
  font-size: var(--text-micro);
  color: var(--color-coral-red);
}

/* Threads --------------------------------------------------------------------- */
.thread {
  display: flex;
  flex-direction: column;
  gap: var(--spacing-8);
  padding: var(--spacing-8);
  border: 1px solid var(--border-hairline);
  border-radius: var(--radius-cards);
}

.thread[data-resolved="true"] {
  opacity: 0.6;
}

.thread-head {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: var(--spacing-8);
}

.anchor-link {
  display: inline-flex;
  align-items: center;
  gap: 5px;
  min-width: 0;
  padding: 0;
  border: none;
  background: transparent;
  color: var(--text-muted);
  font: inherit;
  font-size: var(--text-micro);
  font-weight: var(--weight-medium);
  cursor: pointer;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.anchor-link:hover {
  color: var(--text-primary);
}

.anchor-kind {
  flex-shrink: 0;
  text-transform: uppercase;
  letter-spacing: 0.05em;
  color: var(--text-faint);
}

.promoted-chip {
  flex-shrink: 0;
  display: inline-flex;
  align-items: center;
  gap: 4px;
  padding: 1px 7px;
  border: 1px solid rgba(39, 166, 68, 0.35);
  border-radius: var(--radius-pills);
  background: rgba(39, 166, 68, 0.08);
  color: #4ec96b;
  font-size: var(--text-micro);
  cursor: pointer;
}

.promoted-chip:hover {
  border-color: rgba(39, 166, 68, 0.6);
}

.comment {
  display: flex;
  flex-direction: column;
  gap: 2px;
}

.comment-meta {
  display: flex;
  align-items: baseline;
  gap: var(--spacing-8);
}

.author {
  font-size: var(--text-micro);
  font-weight: var(--weight-medium);
  color: var(--text-muted);
  text-transform: uppercase;
  letter-spacing: 0.04em;
}

.author[data-agent="true"] {
  color: var(--color-signal-teal);
}

.time {
  font-size: var(--text-micro);
  color: var(--text-faint);
}

.body {
  font-size: var(--text-caption);
  color: var(--text-body);
  white-space: pre-wrap;
  word-break: break-word;
}

/* Promote form ----------------------------------------------------------------- */
.promote-form {
  display: flex;
  flex-direction: column;
  gap: 6px;
  padding: var(--spacing-8);
  border: 1px dashed var(--border-hairline);
  border-radius: var(--radius-cards);
  background: var(--surface-obsidian);
}

.promote-copy {
  margin: 0;
  font-size: var(--text-micro);
  color: var(--text-muted);
  line-height: 1.4;
}

.promote-form .input {
  width: 100%;
  padding: 5px 8px;
  border: 1px solid var(--border-hairline);
  border-radius: var(--radius-cards);
  background: var(--surface-carbon);
  color: var(--text-body);
  font: inherit;
  font-size: var(--text-caption);
  box-sizing: border-box;
}

.promote-actions {
  display: flex;
  align-items: center;
  justify-content: flex-end;
  gap: var(--spacing-8);
}

/* Thread footer ----------------------------------------------------------------- */
.thread-actions {
  display: flex;
  align-items: center;
  justify-content: flex-end;
  gap: var(--spacing-8);
}

.resolved-tag {
  font-size: var(--text-micro);
  color: var(--color-pulse-green);
}

.ghost-btn {
  display: inline-flex;
  align-items: center;
  gap: 4px;
  padding: 3px 10px;
  border: 1px solid var(--border-hairline);
  border-radius: var(--radius-pills);
  background: transparent;
  color: var(--text-muted);
  font-size: var(--text-micro);
  cursor: pointer;
}

.ghost-btn:hover {
  color: var(--text-primary);
}

.ghost-btn:disabled {
  opacity: 0.5;
  cursor: default;
}

.reply {
  display: flex;
  flex-direction: column;
  gap: var(--spacing-8);
}

.reply textarea {
  width: 100%;
  padding: var(--spacing-8);
  border: 1px solid var(--border-hairline);
  border-radius: var(--radius-cards);
  background: var(--surface-obsidian);
  color: var(--text-body);
  font: inherit;
  font-size: var(--text-caption);
  resize: vertical;
  box-sizing: border-box;
}

.reply .ghost-btn {
  align-self: flex-end;
}
</style>
