<script setup lang="ts">
import { computed, onBeforeUnmount, onMounted, reactive, ref, watch } from "vue";

import { useAuthStore } from "../stores/auth";
import { renderMarkdown } from "../lib/markdown";
import { ApiError } from "../api/client";
import { getRunEvents, getRunLog, getTaskRuns, type AgentRunDetail, type AgentRunSummary, type ToolCallEvent } from "../api/tasks";
import {
  attemptLabel,
  durationLabel,
  hydratePipeline,
  initialPipelineState,
  reconcileLiveLog,
  resetLiveTail,
  runStatusLabel,
  runningRun,
  splitLog,
  stageLabel,
  verdictLabel,
  type PipelineState,
  type ToolCall,
} from "../task/pipeline";
import { usePipelineStream, type PipelineStream, type StreamStatus } from "../task/usePipelineStream";
import AppIcon from "./AppIcon.vue";
import StatusIcon from "./StatusIcon.vue";

// T-562/T-563: the task detail pipeline view — a stage timeline for one task
// (implement → test ×N → commit → review round N → verdict), hydrated from
// `GET /tasks/{id}/runs` (cheap: no `log`). Each row expands to its full
// `agent_run` log via a separate, on-demand `GET /runs/{id}` call — the two-
// endpoint split CONVENTIONS.md documents for exactly this reason (a busy
// task's stages can each carry up to 256KB of transcript; a timeline view
// shouldn't download all of them just to render the list).
//
// T-563 adds the live tail: `load()` now subscribes to `task:<id>`
// (`usePipelineStream`) BEFORE issuing either REST call, then (if a row is
// `running`) fetches that one row's current log and reconciles it against
// whatever streamed in during the round trip (`reconcileLiveLog`) — see
// `src/task/pipeline.ts`'s header comment for why that ordering and that
// merge are what make "no gap or duplication" true, and
// `src/task/usePipelineStream.ts`'s header for why it deliberately does NOT
// follow `DagEditorView.vue`'s hydrate-then-subscribe order. The parent
// (`TaskModal.vue`) mounts this component only while its tab is active
// (`v-if`, keyed by task id) — `onBeforeUnmount` below is what actually tears
// the subscription down when the tab switches or the modal closes (mirroring
// `DagEditorView.vue`'s own `onBeforeUnmount(() => stream?.close())`, since
// `useXStream`'s automatic `onScopeDispose` only fires when the composable is
// constructed synchronously inside setup — here it's constructed inside an
// async `load()`, after an `await`, where no effect scope is current).
const props = defineProps<{ taskId: string }>();

const auth = useAuthStore();
const state = reactive<PipelineState>(initialPipelineState());
const loading = ref(true);
const error = ref<string | null>(null);
const streamStatus = ref<StreamStatus>("connecting");
let stream: PipelineStream | null = null;
onBeforeUnmount(() => stream?.close());

/** The one expanded row's id, or `null` when every row is collapsed. */
const expandedId = ref<string | null>(null);
/** Per-run full-log cache, keyed by `agent_run.id` — fetched once per row. */
const logCache = reactive(new Map<string, AgentRunDetail>());
/**
 * Per-run historical tool-call pills, keyed by `agent_run.id` — folded from
 * `GET /runs/{id}/events` on expand, fetched once per row alongside the log.
 */
const eventsCache = reactive(new Map<string, ToolCall[]>());
const logLoading = reactive(new Set<string>());
const logError = reactive(new Map<string, string>());

const expandedDetail = computed<AgentRunDetail | null>(() =>
  expandedId.value !== null ? logCache.get(expandedId.value) ?? null : null,
);
/** The row `state.liveLog` belongs to, or `null` if nothing is running. */
const runningRunId = computed(() => runningRun(state)?.id ?? null);
/**
 * The expanded row's log text: the live-tailed buffer while it's the running
 * row (growing in real time, no `getRunLog` re-fetch needed — `load()`
 * already seeded it once via `reconcileLiveLog`), otherwise whatever
 * `toggle()` fetched into `logCache`. Once a row goes terminal (`stage_changed`
 * or a fresh hydrate), it stops being `runningRunId` and this falls back to
 * the ordinary on-demand fetch path unchanged from T-562.
 */
const expandedLog = computed<string | null>(() => {
  if (expandedId.value === null) {
    return null;
  }
  if (expandedId.value === runningRunId.value) {
    return state.liveLog;
  }
  return expandedDetail.value?.log ?? null;
});
const expandedSegments = computed(() => (expandedLog.value !== null ? splitLog(expandedLog.value) : null));

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
  loading.value = true;
  error.value = null;
  expandedId.value = null;
  logCache.clear();
  eventsCache.clear();
  logLoading.clear();
  logError.clear();

  // Subscribe FIRST (T-563): `resetLiveTail` starts a clean buffer for this
  // task, then `usePipelineStream` opens the socket and starts folding every
  // `text`/`error`/`stage_changed` frame into `state` immediately — including
  // any that arrive during the two REST calls below. Doing this before either
  // `await` is what rules out the "subscribe after hydrate" gap; see
  // `src/task/pipeline.ts`'s header for the full rationale.
  stream?.close();
  resetLiveTail(state);
  stream = usePipelineStream(props.taskId, token, state, streamStatus);

  try {
    const runs = await getTaskRuns(token, props.taskId);
    hydratePipeline(state, props.taskId, runs);
    const running = runningRun(state);
    if (running !== null) {
      // The one row a "live tail" needs a REST snapshot for -- `AgentRunSummary`
      // (above) never carries `log`. Reconcile whatever streamed in during
      // this call against the snapshot (`reconcileLiveLog`), then auto-expand
      // it so the running stage's output is visible without a click.
      expandedId.value = running.id;
      try {
        const detail = await getRunLog(token, running.id);
        reconcileLiveLog(state, detail.log);
      } catch (err) {
        // Best-effort: the live tail still renders from the buffered text
        // alone (`state.liveLog`, un-reconciled) -- just not proven gap-free
        // from the stage's own start. Auth still bounces; anything else is
        // silent (a banner here would compete with the main `error` banner
        // for a fetch that succeeded).
        bounceIfAuth(err);
      }
    }
  } catch (err) {
    if (bounceIfAuth(err)) {
      return;
    }
    error.value = err instanceof Error ? err.message : "failed to load the run history";
  } finally {
    loading.value = false;
  }
}

async function toggle(run: AgentRunSummary) {
  if (expandedId.value === run.id) {
    expandedId.value = null;
    return;
  }
  expandedId.value = run.id;
  // The running row is already live-tailed via `state.liveLog` (seeded and
  // kept current by `load()`/`applyPipelineFrame`) -- no fetch needed, and
  // re-fetching here would just be a stale snapshot `expandedLog` ignores.
  if (run.id === runningRunId.value) {
    return;
  }
  if (logCache.has(run.id) || logLoading.has(run.id)) {
    return;
  }
  const token = auth.accessToken;
  if (token === null) {
    return;
  }
  // Historical pills are best-effort: a failed events fetch must not affect
  // the log below it, so swallow everything except an auth bounce.
  void getRunEvents(token, run.id)
    .then((events) => eventsCache.set(run.id, foldToolEvents(events)))
    .catch(() => {});
  logLoading.add(run.id);
  logError.delete(run.id);
  try {
    const detail = await getRunLog(token, run.id);
    logCache.set(run.id, detail);
  } catch (err) {
    if (bounceIfAuth(err)) {
      return;
    }
    logError.set(run.id, err instanceof Error ? err.message : "failed to load the log");
  } finally {
    logLoading.delete(run.id);
  }
}

/**
 * Pair `tool_start`/`tool_end` events by `toolCallId` into resolved pills —
 * same pairing logic as planning/stream.ts's foldMessages, but over the
 * persisted event rows rather than WS frames.
 */
function foldToolEvents(events: ToolCallEvent[]): ToolCall[] {
  const starts = new Map<string, string>(); // toolCallId → name
  const result: ToolCall[] = [];
  for (const e of events) {
    if (e.kind === "tool_start") {
      starts.set(e.toolCallId, e.name);
    } else {
      result.push({
        toolCallId: e.toolCallId,
        name: starts.get(e.toolCallId) ?? "tool",
        status: e.ok ? "ok" : "error",
      });
      starts.delete(e.toolCallId);
    }
  }
  for (const [id, name] of starts) {
    result.push({ toolCallId: id, name, status: "running" });
  }
  return result;
}

/** The verdict badge's tone: PASS reads green, BLOCKED red, NEEDS_CHANGES neutral. */
function verdictTone(verdict: string): "green" | "red" | "neutral" {
  if (verdict === "PASS") return "green";
  if (verdict === "BLOCKED") return "red";
  return "neutral";
}

// Defensive: this component is normally remounted fresh per task (the parent
// keys it by task id), but reload if the prop ever changes under an existing
// instance rather than silently showing the wrong task's history.
watch(() => props.taskId, load);
onMounted(load);
</script>

<template>
  <div class="pipeline">
    <div v-if="loading" class="loading-stack" aria-label="Loading run history">
      <div v-for="i in 3" :key="i" class="skeleton sk-row" />
    </div>
    <p v-else-if="error" class="banner banner-error" role="alert">{{ error }}</p>
    <div v-else-if="state.runs.length === 0" class="empty-state">
      <AppIcon name="layers" :size="20" />
      <p>No pipeline runs yet — this task hasn't been claimed by the worker.</p>
    </div>

    <ul v-else class="run-list">
      <li v-for="run in state.runs" :key="run.id" class="run-row">
        <button
          class="run-head"
          type="button"
          :aria-expanded="expandedId === run.id"
          @click="toggle(run)"
        >
          <AppIcon :name="expandedId === run.id ? 'chevron-down' : 'chevron-right'" :size="12" />
          <StatusIcon :status="run.status" :size="12" />
          <span class="run-stage">{{ stageLabel(run.stage) }}</span>
          <span class="run-attempt">{{ attemptLabel(run) }}</span>
          <span
            v-if="run.verdict"
            class="badge"
            :data-tone="verdictTone(run.verdict)"
          >
            {{ verdictLabel(run.verdict) }}
          </span>
          <span class="run-spacer" />
          <span class="run-status" :data-status="run.status">{{ runStatusLabel(run.status) }}</span>
          <span class="run-duration">{{ durationLabel(run) }}</span>
        </button>

        <div v-if="expandedId === run.id" class="run-body">
          <!-- Live pills: the currently-running stage renders directly from
               state.liveTools, which applyPipelineFrame keeps in sync as
               tool_start/tool_end frames stream over the WebSocket. -->
          <div v-if="run.id === runningRunId && state.liveTools.length > 0" class="tool-row">
            <span
              v-for="call in state.liveTools"
              :key="call.toolCallId"
              class="tool-chip"
              :data-status="call.status"
            >
              <span class="tool-dot" />
              <span class="tool-name mono">{{ call.name }}</span>
              <span class="tool-state">{{ call.status }}</span>
            </span>
          </div>
          <!-- Historical pills: completed stages only -- fetched on expand
               from GET /runs/{id}/events (see toggle()). -->
          <div v-if="run.id !== runningRunId && (eventsCache.get(run.id)?.length ?? 0) > 0" class="tool-row">
            <span
              v-for="call in eventsCache.get(run.id) ?? []"
              :key="call.toolCallId"
              class="tool-chip"
              :data-status="call.status"
            >
              <span class="tool-dot" />
              <span class="tool-name mono">{{ call.name }}</span>
              <span class="tool-state">{{ call.status }}</span>
            </span>
          </div>
          <p v-if="logLoading.has(run.id)" class="log-note">Loading log…</p>
          <p v-else-if="logError.has(run.id)" class="banner banner-error" role="alert">
            {{ logError.get(run.id) }}
          </p>
          <template v-else-if="expandedSegments">
            <div
              v-if="expandedSegments.head"
              class="log-md md"
              v-html="renderMarkdown(expandedSegments.head)"
            />
            <p v-if="expandedSegments.tail !== null" class="log-elided">
              Log elided — exceeded 256 KB; showing head + tail
            </p>
            <div
              v-if="expandedSegments.tail"
              class="log-md md"
              v-html="renderMarkdown(expandedSegments.tail)"
            />
            <p v-if="!expandedSegments.head && expandedSegments.tail === null" class="log-note">
              No output.
            </p>
          </template>
        </div>
      </li>
    </ul>
  </div>
</template>

<style scoped>
.pipeline {
  display: flex;
  flex-direction: column;
  gap: var(--spacing-8);
}

.run-list {
  list-style: none;
  margin: 0;
  padding: 0;
  display: flex;
  flex-direction: column;
  gap: 6px;
}

.run-row {
  border: 1px solid var(--border-hairline);
  border-radius: var(--radius-cards);
  background: rgba(255, 255, 255, 0.015);
  overflow: hidden;
}

.run-head {
  width: 100%;
  display: flex;
  align-items: center;
  gap: var(--spacing-8);
  padding: 8px var(--spacing-12);
  text-align: left;
  color: var(--text-body);
  cursor: pointer;
}

.run-head:hover {
  background: rgba(255, 255, 255, 0.03);
}

.run-stage {
  font-size: var(--text-caption);
  font-weight: var(--weight-medium);
  color: var(--text-primary);
}

.run-attempt {
  font-size: var(--text-label);
  color: var(--text-muted);
}

.run-spacer {
  flex: 1;
}

.run-status {
  font-size: var(--text-label);
  color: var(--text-muted);
}

.run-status[data-status="error"],
.run-status[data-status="timeout"] {
  color: var(--color-coral-red);
}

.run-duration {
  font-size: var(--text-label);
  color: var(--text-muted);
  white-space: nowrap;
  min-width: 5em;
  text-align: right;
}

.run-body {
  padding: 0 var(--spacing-12) var(--spacing-12);
  border-top: 1px solid var(--border-hairline);
}

/* Rendered stage-log markdown — same container as the old `.log` <pre>, but
   flowing text (no `white-space: pre-wrap`, no monospace): `.md` (ui.css)
   handles code blocks/spans with its own monospace styling. */
.log-md {
  margin: var(--spacing-8) 0 0;
  padding: var(--spacing-8) var(--spacing-12);
  background: var(--surface-void);
  border: 1px solid var(--border-hairline);
  border-radius: var(--radius-buttons);
  max-height: 320px;
  overflow: auto;
  font-size: 12px;
  line-height: 1.55;
  color: var(--text-body);
}

.log-elided {
  margin: var(--spacing-8) 0 0;
  padding: 6px var(--spacing-12);
  text-align: center;
  font-size: var(--text-label);
  color: var(--text-faint);
  font-style: italic;
  border-top: 1px dashed var(--border-hairline);
  border-bottom: 1px dashed var(--border-hairline);
}

.log-note {
  margin: var(--spacing-8) 0 0;
  font-size: var(--text-label);
  color: var(--text-faint);
}

.loading-stack {
  display: flex;
  flex-direction: column;
  gap: 6px;
}

.sk-row {
  height: 36px;
}
</style>
