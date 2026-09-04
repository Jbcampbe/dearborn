<script setup lang="ts">
import { computed, onBeforeUnmount, onMounted, reactive, ref } from "vue";
import { RouterLink } from "vue-router";

import { useAuthStore } from "../stores/auth";
import { ApiError } from "../api/client";
import { getEpic, type Epic } from "../api/epics";
import { getProject } from "../api/projects";
import { getDocument, type DocumentSection } from "../api/document";
import { getMap } from "../api/map";
import { listComments, postComment, resolveComment } from "../api/comments";
import {
  hydrateDocument,
  initialDocumentState,
  threadsForSection,
  type DocumentStreamState,
} from "../document/stream";
import { useDocumentStream, type StreamStatus } from "../document/useDocumentStream";
import AppIcon from "./AppIcon.vue";
import EpicTabs from "./EpicTabs.vue";

// The living-Document view (wayfinder epic §4.5/§10, client slice): the epic's
// settled-decisions HTML spec, rendered inline (v1 renders the agent-authored
// HTML as-is — no sanitization on either side of the pipeline), with a TOC
// built from the server's section anchor/provenance index, per-section
// provenance chips (the map node that last wrote each section), and
// section-anchored comment threads.
//
// Live via `document_updated` (version + section index — the frame never
// carries the HTML, so the view re-reads the document over REST) and
// `comments_updated` (the full comment list) frames on `epic:<id>`, folded
// through the pure reducer (`document/stream.ts`) by the composable
// (`document/useDocumentStream.ts`) — the same plumbing as the Map view.
const props = defineProps<{ id: string }>();

const auth = useAuthStore();
const state = reactive<DocumentStreamState>(initialDocumentState());
const epic = ref<Epic | null>(null);
const loading = ref(true);
const error = ref<string | null>(null);
const streamStatus = ref<StreamStatus>("connecting");
// The breadcrumb's project name (the epic only carries `project_id`).
const projectName = ref<string | null>(null);
// Node titles for the provenance chips (node id → display label).
const nodeLabels = ref<Map<string, string>>(new Map());
// The section whose comment threads the side panel shows (null = hidden).
const selectedSectionId = ref<string | null>(null);
// Composer state for the panel: the new-thread body plus per-thread replies.
const newBody = ref("");
const replyBodies = ref<Record<string, string>>({});
const posting = ref(false);
// Guards double `document_updated` reloads while one is in flight.
let reloading = false;

let stream: ReturnType<typeof useDocumentStream> | null = null;
onBeforeUnmount(() => stream?.close());

const doc = computed(() => state.doc);
const sections = computed(() => state.doc?.sections ?? []);
const selectedSection = computed<DocumentSection | null>(
  () => sections.value.find((s) => s.section_id === selectedSectionId.value) ?? null,
);
const selectedThreads = computed(() =>
  selectedSectionId.value === null ? [] : threadsForSection(state, selectedSectionId.value),
);

function sectionCommentCount(sectionId: string): number {
  return state.comments.filter((c) => c.anchor_kind === "section" && c.anchor_id === sectionId).length;
}

/** The provenance chip label: the node's title, or a human-edited fallback. */
function provenanceLabel(section: DocumentSection): string {
  if (section.provenance === null) {
    return "no node edits";
  }
  return nodeLabels.value.get(section.provenance) ?? section.provenance.slice(0, 10);
}

/** Attribution line on a comment: agent runs have no user id. */
function authorLabel(comment: { is_agent: boolean; author_user_id: string | null }): string {
  if (comment.is_agent) {
    return "agent";
  }
  if (comment.author_user_id === auth.user?.id) {
    return "you";
  }
  return "user";
}

function bounceIfAuth(err: unknown): boolean {
  if (err instanceof ApiError && err.isAuth) {
    auth.logout(`Token rejected (401): ${err.message}. Please re-enter it.`);
    return true;
  }
  return false;
}

/** Re-read the document over REST (a `document_updated` frame never carries the HTML). */
async function reloadDocument(): Promise<void> {
  if (reloading) {
    return;
  }
  const token = auth.accessToken;
  if (token === null) {
    return;
  }
  reloading = true;
  try {
    hydrateDocument(state, props.id, await getDocument(token, props.id));
  } catch {
    // Best-effort: the frame already stamped the section index; the next
    // frame (or a reload) heals the HTML. A 401 bounces via the api layer.
  } finally {
    reloading = false;
  }
}

async function load() {
  const token = auth.accessToken;
  if (token === null) {
    return;
  }
  loading.value = true;
  error.value = null;
  try {
    const epicObj = await getEpic(token, props.id);
    epic.value = epicObj;
    const [documentObj, comments] = await Promise.all([
      getDocument(token, props.id),
      listComments(token, props.id, { anchor_kind: "section" }),
    ]);
    hydrateDocument(state, props.id, documentObj);
    state.comments = comments;

    // Provenance chips resolve node ids to titles via the map. Non-blocking +
    // non-fatal: a failed fetch degrades the chips to id prefixes.
    void getMap(token, props.id)
      .then((map) => {
        nodeLabels.value = new Map(map.nodes.map((n) => [n.id, n.title]));
      })
      .catch((err) => bounceIfAuth(err));
    void getProject(token, epicObj.project_id)
      .then((p) => (projectName.value = p.name))
      .catch((err) => bounceIfAuth(err));

    stream = useDocumentStream(props.id, () => auth.ensureFresh(), state, () => void reloadDocument(), streamStatus);
  } catch (err) {
    if (bounceIfAuth(err)) {
      return;
    }
    error.value = err instanceof Error ? err.message : "failed to load the document";
  } finally {
    loading.value = false;
  }
}

onMounted(load);

// ---- TOC + selection --------------------------------------------------------

function selectSection(sectionId: string): void {
  selectedSectionId.value = sectionId;
}

/** TOC click: open the comments panel AND scroll the rendered section into view. */
function jumpToSection(sectionId: string): void {
  selectedSectionId.value = sectionId;
  const el = window.document.getElementById(sectionId);
  el?.scrollIntoView({ behavior: "smooth", block: "start" });
}

/** Click-to-discuss on the rendered document: select the clicked section's anchor. */
function onDocumentClick(event: MouseEvent): void {
  const target = event.target as Element | null;
  const anchored = target?.closest?.("[id]");
  if (anchored !== null && anchored !== undefined && anchored.id !== "") {
    selectSection(anchored.id);
  }
}

// ---- comments ---------------------------------------------------------------

async function postNew(sectionId: string): Promise<void> {
  const token = auth.accessToken;
  const body = newBody.value.trim();
  if (token === null || body === "" || posting.value) {
    return;
  }
  posting.value = true;
  try {
    await postComment(token, props.id, {
      anchor_kind: "section",
      anchor_id: sectionId,
      body,
    });
    newBody.value = "";
    // `comments_updated` carries the full list; the WS replaces the state. If
    // the socket is down the comment simply appears on the next reload.
  } catch (err) {
    if (!bounceIfAuth(err)) {
      error.value = err instanceof Error ? err.message : "failed to post the comment";
    }
  } finally {
    posting.value = false;
  }
}

async function postReply(threadId: string): Promise<void> {
  const token = auth.accessToken;
  const body = (replyBodies.value[threadId] ?? "").trim();
  if (token === null || body === "" || posting.value) {
    return;
  }
  posting.value = true;
  try {
    await postComment(token, props.id, { thread_id: threadId, body });
    replyBodies.value[threadId] = "";
  } catch (err) {
    if (!bounceIfAuth(err)) {
      error.value = err instanceof Error ? err.message : "failed to post the reply";
    }
  } finally {
    posting.value = false;
  }
}

async function resolve(threadId: string): Promise<void> {
  const token = auth.accessToken;
  const head = state.comments.find((c) => c.thread_id === threadId);
  if (token === null || head === undefined) {
    return;
  }
  try {
    await resolveComment(token, props.id, head.id);
  } catch (err) {
    if (!bounceIfAuth(err)) {
      error.value = err instanceof Error ? err.message : "failed to resolve the thread";
    }
  }
}
</script>

<template>
  <main class="page page-wide">
    <nav class="crumbs">
      <RouterLink :to="{ name: 'projects' }">Projects</RouterLink>
      <template v-if="epic">
        <span class="sep">/</span>
        <RouterLink :to="{ name: 'project-detail', params: { id: epic.project_id } }">
          {{ projectName ?? "…" }}
        </RouterLink>
      </template>
    </nav>

    <div v-if="loading" class="loading-stack" aria-label="Loading document">
      <div class="skeleton sk-title" />
      <div class="skeleton sk-block" />
    </div>
    <p v-else-if="error && !doc" class="banner banner-error" role="alert">{{ error }}</p>

    <template v-else-if="doc">
      <header class="head fade-in">
        <div class="head-main">
          <h1 class="page-title">{{ epic?.title ?? "Document" }}</h1>
          <div class="head-badges">
            <span class="badge">v{{ doc.version }}</span>
            <span v-if="doc.html === null" class="badge">not yet synced</span>
          </div>
        </div>
        <span class="conn" :data-status="streamStatus">{{ streamStatus === "open" ? "live" : streamStatus }}</span>
      </header>

      <EpicTabs :id="props.id" tab="document" />

      <p v-if="error" class="banner banner-error" role="alert">{{ error }}</p>

      <!-- Empty state: no version has ever been synced. -->
      <div v-if="doc.html === null" class="empty-state">
        <AppIcon name="repo" :size="20" />
        <p>
          No document yet. The plan's living document appears here once the first
          node resolution lands version 1.
        </p>
      </div>

      <div v-else class="doc-layout">
        <!-- TOC: the server's section anchor/provenance index, in document order. -->
        <aside class="doc-toc" aria-label="Table of contents">
          <h2 class="toc-title">Contents</h2>
          <p v-if="sections.length === 0" class="toc-empty">No sections indexed yet.</p>
          <ol class="toc-list">
            <li v-for="s in sections" :key="s.section_id">
              <button
                type="button"
                class="toc-item"
                :data-active="s.section_id === selectedSectionId"
                @click="jumpToSection(s.section_id)"
              >
                <span class="toc-label">{{ s.title ?? s.section_id }}</span>
                <span class="toc-meta">
                  <span
                    class="chip"
                    :title="s.provenance ? `Last written by node ${s.provenance}` : 'Never node-edited'"
                  >
                    <AppIcon name="sparkle" :size="10" />
                    {{ provenanceLabel(s) }}
                  </span>
                  <span v-if="sectionCommentCount(s.section_id) > 0" class="count">
                    {{ sectionCommentCount(s.section_id) }}
                  </span>
                </span>
              </button>
            </li>
          </ol>
        </aside>

        <!-- The document itself: inline HTML, no sanitization (v1). -->
        <article
          class="doc-body"
          data-testid="document-html"
          @click="onDocumentClick"
          v-html="doc.html"
        />

        <!-- Section-anchored comments panel for the selected section. -->
        <aside v-if="selectedSection" class="doc-comments" aria-label="Section comments">
          <header class="comments-head">
            <h2>Comments</h2>
            <p class="comments-anchor" :title="selectedSection.section_id">
              § {{ selectedSection.title ?? selectedSection.section_id }}
            </p>
            <span class="chip">
              <AppIcon name="sparkle" :size="10" />
              {{ provenanceLabel(selectedSection) }}
            </span>
          </header>

          <p v-if="selectedThreads.length === 0" class="comments-empty">
            No comments on this section yet.
          </p>

          <section
            v-for="thread in selectedThreads"
            :key="thread.threadId"
            class="thread"
            :data-resolved="thread.resolved"
          >
            <div v-for="c in thread.comments" :key="c.id" class="comment">
              <span class="comment-author">{{ authorLabel(c) }}</span>
              <span class="comment-body">{{ c.body }}</span>
            </div>
            <div class="thread-actions">
              <button
                v-if="!thread.resolved"
                type="button"
                class="ghost-btn"
                @click="resolve(thread.threadId)"
              >
                Resolve
              </button>
              <span v-else class="resolved-tag">Resolved</span>
            </div>
            <form
              v-if="!thread.resolved"
              class="reply"
              @submit.prevent="postReply(thread.threadId)"
            >
              <textarea
                v-model="replyBodies[thread.threadId]"
                rows="2"
                placeholder="Reply…"
                aria-label="Reply"
              />
              <button type="submit" class="ghost-btn" :disabled="posting">Reply</button>
            </form>
          </section>

          <form class="composer" @submit.prevent="postNew(selectedSection.section_id)">
            <textarea
              v-model="newBody"
              rows="3"
              placeholder="Comment on this section…"
              aria-label="New comment"
            />
            <button type="submit" :disabled="posting || newBody.trim() === ''">Comment</button>
          </form>
        </aside>
      </div>
    </template>
  </main>
</template>

<style scoped>
.head {
  display: flex;
  align-items: flex-start;
  justify-content: space-between;
  gap: var(--spacing-16);
  margin-bottom: var(--spacing-20);
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
}

.doc-layout {
  display: grid;
  grid-template-columns: 240px minmax(0, 1fr);
  gap: var(--spacing-24);
  align-items: start;
  margin-top: var(--spacing-16);
}

/* TOC --------------------------------------------------------------------- */
.doc-toc {
  position: sticky;
  top: var(--spacing-16);
  display: flex;
  flex-direction: column;
  gap: var(--spacing-8);
  padding: var(--spacing-12);
  border: 1px solid var(--border-hairline);
  border-radius: var(--radius-cards);
  background: var(--surface-carbon);
}

.toc-title {
  margin: 0;
  font-size: var(--text-micro);
  font-weight: var(--weight-medium);
  text-transform: uppercase;
  letter-spacing: 0.06em;
  color: var(--text-faint);
}

.toc-empty {
  margin: 0;
  font-size: var(--text-caption);
  color: var(--text-faint);
}

.toc-list {
  display: flex;
  flex-direction: column;
  gap: 2px;
  margin: 0;
  padding: 0;
  list-style: none;
}

.toc-item {
  display: flex;
  flex-direction: column;
  align-items: flex-start;
  gap: 4px;
  width: 100%;
  padding: 6px 8px;
  border: none;
  border-radius: var(--radius-pills);
  background: transparent;
  color: var(--text-body);
  font: inherit;
  text-align: left;
  cursor: pointer;
}

.toc-item:hover {
  background: var(--surface-obsidian);
}

.toc-item[data-active="true"] {
  background: var(--surface-obsidian);
  color: var(--text-primary);
}

.toc-label {
  font-size: var(--text-caption);
  line-height: 1.3;
}

.toc-meta {
  display: inline-flex;
  align-items: center;
  gap: var(--spacing-8);
  max-width: 100%;
}

/* Provenance chip ---------------------------------------------------------- */
.chip {
  display: inline-flex;
  align-items: center;
  gap: 4px;
  padding: 1px 7px;
  border: 1px solid var(--border-hairline);
  border-radius: var(--radius-pills);
  background: var(--surface-obsidian);
  font-size: var(--text-micro);
  color: var(--text-muted);
  max-width: 100%;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.count {
  display: inline-flex;
  align-items: center;
  justify-content: center;
  min-width: 16px;
  height: 16px;
  padding: 0 4px;
  border-radius: var(--radius-pills);
  background: var(--color-signal-teal);
  color: var(--text-primary);
  font-size: var(--text-micro);
  font-weight: var(--weight-medium);
}

/* Document body ------------------------------------------------------------ */
.doc-body {
  min-width: 0;
  padding: var(--spacing-24);
  border: 1px solid var(--border-hairline);
  border-radius: var(--radius-cards);
  background: var(--surface-carbon);
  color: var(--text-body);
  font-size: var(--text-body);
  line-height: 1.6;
}

.doc-body :deep(h1),
.doc-body :deep(h2),
.doc-body :deep(h3),
.doc-body :deep(h4),
.doc-body :deep(h5),
.doc-body :deep(h6) {
  margin: var(--spacing-24) 0 var(--spacing-8);
  color: var(--text-primary);
  line-height: 1.3;
}

.doc-body :deep(h1:first-child),
.doc-body :deep(h2:first-child),
.doc-body :deep(h3:first-child) {
  margin-top: 0;
}

.doc-body :deep(p) {
  margin: 0 0 var(--spacing-12);
}

.doc-body :deep(ul),
.doc-body :deep(ol) {
  margin: 0 0 var(--spacing-12);
  padding-left: var(--spacing-24);
}

.doc-body :deep(code) {
  padding: 1px 5px;
  border-radius: var(--radius-pills);
  background: var(--surface-obsidian);
  font-family: "JetBrains Mono Variable", monospace;
  font-size: 0.9em;
}

.doc-body :deep(pre) {
  padding: var(--spacing-12);
  border-radius: var(--radius-cards);
  background: var(--surface-obsidian);
  overflow-x: auto;
}

.doc-body :deep(blockquote) {
  margin: 0 0 var(--spacing-12);
  padding-left: var(--spacing-12);
  border-left: 2px solid var(--border-hairline);
  color: var(--text-muted);
}

.doc-body :deep(table) {
  width: 100%;
  border-collapse: collapse;
  margin-bottom: var(--spacing-12);
}

.doc-body :deep(th),
.doc-body :deep(td) {
  padding: 6px 8px;
  border: 1px solid var(--border-hairline);
  text-align: left;
}

/* Comments panel ------------------------------------------------------------ */
.doc-comments {
  display: none;
  position: sticky;
  top: var(--spacing-16);
  flex-direction: column;
  gap: var(--spacing-12);
  padding: var(--spacing-12);
  border: 1px solid var(--border-hairline);
  border-radius: var(--radius-cards);
  background: var(--surface-carbon);
}

.comments-head {
  display: flex;
  flex-direction: column;
  gap: 4px;
}

.comments-head h2 {
  margin: 0;
  font-size: var(--text-micro);
  font-weight: var(--weight-medium);
  text-transform: uppercase;
  letter-spacing: 0.06em;
  color: var(--text-faint);
}

.comments-anchor {
  margin: 0;
  font-size: var(--text-caption);
  color: var(--text-primary);
}

.comments-empty {
  margin: 0;
  font-size: var(--text-caption);
  color: var(--text-faint);
}

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

.comment {
  display: flex;
  flex-direction: column;
  gap: 2px;
}

.comment-author {
  font-size: var(--text-micro);
  font-weight: var(--weight-medium);
  color: var(--text-muted);
  text-transform: uppercase;
  letter-spacing: 0.04em;
}

.comment-body {
  font-size: var(--text-caption);
  color: var(--text-body);
  white-space: pre-wrap;
}

.thread-actions {
  display: flex;
  align-items: center;
  justify-content: flex-end;
}

.resolved-tag {
  font-size: var(--text-micro);
  color: var(--color-pulse-green);
}

.ghost-btn {
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

.reply,
.composer {
  display: flex;
  flex-direction: column;
  gap: var(--spacing-8);
}

.reply textarea,
.composer textarea {
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

.composer button {
  align-self: flex-end;
  padding: 5px 12px;
  border: none;
  border-radius: var(--radius-pills);
  background: var(--color-iris-violet);
  color: var(--text-primary);
  font-size: var(--text-caption);
  cursor: pointer;
}

.composer button:disabled {
  opacity: 0.5;
  cursor: default;
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

@media (min-width: 1200px) {
  .doc-layout {
    grid-template-columns: 240px minmax(0, 1fr) 300px;
  }

  .doc-comments {
    display: flex;
  }
}
</style>
