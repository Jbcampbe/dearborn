// T-562/T-563: pure view-model helpers for the task detail pipeline view —
// stage labeling, attempt/round-number display, duration formatting, log
// elision-marker splitting over a task's `agent_run` history (`GET
// /tasks/{id}/runs` + `GET /runs/{id}`, T-512), and (T-563) the `task:<id>`
// WS reducer that live-tails the running stage. Mirrors `board/controls.ts`/
// `dag/stream.ts`: framework-free, dependency-free (no Vue, no fetch, no WS)
// so every formatting/ordering/reconciliation decision here is unit-tested
// without a browser — `TaskPipelinePanel.vue` (via `usePipelineStream.ts`
// for the socket lifecycle) is the only consumer.
//
// `PipelineState` keeps the same `initialState` + whole-list-replacing
// `hydrate` shape `dag/stream.ts`'s `DagState` does; T-563 adds
// `applyPipelineFrame(state, frame)` alongside it, the reducer T-562 named
// this file's future seam for.
//
// ## The hydration-boundary reconciliation (T-563's hard part)
//
// `GET /tasks/{id}/runs` (this module's `hydratePipeline`, unchanged from
// T-562) never carries `log` — it's the cheap summary list. A live tail of
// the *currently running* row therefore needs a second REST call most
// callers don't otherwise need: `GET /runs/{id}` for that one row, to seed
// the partial log the server has flushed so far (D14, every
// `PARTIAL_FLUSH_INTERVAL` ~2s — see `task_agent.rs`). That flushed text and
// the live `text`/`error` `RunEvent`s streamed on `task:<id>` are the SAME
// underlying, monotonically-growing string (`AgentStageOutcome::absorb`'s
// accumulation, server-side) observed from two different vantage points:
// the REST log is always some PREFIX of it (whatever was flushed by the time
// the GET executed); the client's own live-accumulated text
// (`PipelineState.liveLog`) is always a SUFFIX of it, starting wherever the
// WS subscription began receiving frames.
//
// The ordering rule this module assumes (enforced by `usePipelineStream.ts`,
// not here): **subscribe to `task:<id>` before issuing either REST call.**
// That ordering is what rules out ever *missing* a live event — nothing
// published after the subscription is live is ever dropped (no replay, but
// also no drops once subscribed) — which is the failure mode of the
// opposite order ("subscribe after hydrate, losing events in between").
// Subscribing first instead risks the other direction: some of the text the
// client received live may ALREADY be included in the REST snapshot (the
// flush that produced it happened to catch up past — or exactly to — the
// point the client started buffering). `mergeHydratedLog` is the fix: find
// the longest suffix of the REST log that matches a prefix of the buffered
// live text, and drop that overlap before appending. This is one rule that
// is correct in BOTH directions the AC asks for: buffered text with no
// overlap (the hydrate response lands before some of the already-buffered
// live events are "new") appends in full; buffered text wholly contained in
// the REST snapshot (the hydrate already contains text that then arrives
// again live) contributes nothing further. `reconcileLiveLog` applies this
// exactly once, when the running row's `GET /runs/{id}` resolves; every
// frame received before OR after that point is folded into `liveLog` by the
// plain, unconditional append in `applyPipelineFrame` — the merge is a
// one-time seam at the hydration boundary, not an ongoing concern.
//
// ## What this module does NOT solve
//
// `stage_changed` (§2.6) is, as of T-560/T-561, published only for a
// `review`/`verify_complete` stage's D9 verdict — it is the only live signal
// this client has for "a stage transitioned," full stop (a bare `RunEvent`
// carries a harness `run_id`, never a stage name or `agent_run` id, so a
// `started`/`text` frame alone can never tell this reducer which stage it
// belongs to). For a task that keeps running non-verdict stages
// (`test_gate`, `fix`, `commit`, `push`, …) past the client's one-time
// hydrate, this reducer has no way to detect the transition live; the
// timeline only catches back up on the next hydrate (re-opening the detail
// view). `applyStageChanged` below still does the one correct thing it CAN
// do — advance the timeline for the transitions it IS told about, including
// one for a stage this client's hydrate never saw a row for at all (a
// review that started and finished between hydrate and now) — rather than
// silently dropping a `stage_changed` frame with no matching row.

import type { AgentRunSummary } from "../api/tasks";

// ---- stage labels ----------------------------------------------------------

/**
 * MILESTONE_2 §2.2's fixed stage vocabulary → a human label. A stage string
 * not in this map (forward-compat: a future stage this client predates)
 * degrades to a title-cased rendering of the raw value — rendered, never
 * dropped, matching `controls.ts`'s `describeFailureReason` fallback.
 */
const STAGE_LABELS: Record<string, string> = {
  setup: "Setup",
  preflight: "Preflight",
  implement: "Implement",
  test_gate: "Test",
  fix: "Fix",
  verify_complete: "Verify complete",
  review: "Review",
  commit: "Commit",
  push: "Push",
  summarize: "Summarize",
};

export function stageLabel(stage: string): string {
  return STAGE_LABELS[stage] ?? titleCase(stage);
}

function titleCase(snake: string): string {
  const words = snake.split("_").filter((w) => w.length > 0);
  if (words.length === 0) {
    return snake;
  }
  return words.map((w) => w.charAt(0).toUpperCase() + w.slice(1)).join(" ");
}

// ---- attempt / round numbering ---------------------------------------------

/**
 * Stages whose `agent_run.attempt` is a 0-based counter in the server
 * (`worker.rs`: "T-531's own numbering starts a task's first review at
 * `attempt = 0`; a human reading '0 review rounds' on a task that was in
 * fact reviewed once would misread it as 'never reviewed'"). Every other
 * stage's attempt is already 1 at every call site and needs no adjustment.
 * `humanAttempt` applies the same +1 a human reader expects.
 */
const ZERO_INDEXED_STAGES = new Set(["test_gate", "fix", "review"]);

export function humanAttempt(run: Pick<AgentRunSummary, "stage" | "attempt">): number {
  return ZERO_INDEXED_STAGES.has(run.stage) ? run.attempt + 1 : run.attempt;
}

/**
 * The row's attempt/round badge text — "Round N" for `review` (matching
 * MILESTONE_2 §9's own "review round N" phrasing, since a review round is
 * the more natural unit than a raw "attempt" there), "Attempt N" for every
 * other stage.
 */
export function attemptLabel(run: Pick<AgentRunSummary, "stage" | "attempt">): string {
  const n = humanAttempt(run);
  return run.stage === "review" ? `Round ${n}` : `Attempt ${n}`;
}

// ---- status / verdict labels -----------------------------------------------

const STATUS_LABELS: Record<string, string> = {
  running: "Running",
  ok: "OK",
  error: "Error",
  timeout: "Timed out",
  cancelled: "Cancelled",
};

/** Human text for `agent_run.status` (running|ok|error|timeout|cancelled). */
export function runStatusLabel(status: string): string {
  return STATUS_LABELS[status] ?? titleCase(status);
}

const VERDICT_LABELS: Record<string, string> = {
  PASS: "Pass",
  NEEDS_CHANGES: "Needs changes",
  BLOCKED: "Blocked",
};

/** Human text for a `review`/`verify_complete` row's D9 `verdict`. */
export function verdictLabel(verdict: string): string {
  return VERDICT_LABELS[verdict] ?? verdict;
}

// ---- duration ---------------------------------------------------------------

/**
 * Duration text for a row: "12s" while short, "1m 04s" past a minute, "1h
 * 02m" past an hour. While the row is still `running` (no `ended_at` yet)
 * the duration is measured against `now` and suffixed "so far" so it reads
 * as still counting rather than a finished number. `started_at` is always
 * set at row-open time (`evidence.rs`'s `open_stage` stamps it in the same
 * `INSERT` that writes `status = 'running'`); the `null` branch is defensive
 * only — the schema allows it, this function must not crash on it.
 */
export function durationLabel(
  run: Pick<AgentRunSummary, "started_at" | "ended_at">,
  now: number = Date.now(),
): string {
  if (run.started_at === null) {
    return "—";
  }
  const end = run.ended_at ?? now;
  const ms = Math.max(0, end - run.started_at);
  const text = formatMs(ms);
  return run.ended_at === null ? `${text} so far` : text;
}

function formatMs(ms: number): string {
  const totalSeconds = Math.floor(ms / 1000);
  const hours = Math.floor(totalSeconds / 3600);
  const minutes = Math.floor((totalSeconds % 3600) / 60);
  const seconds = totalSeconds % 60;
  if (hours > 0) {
    return `${hours}h ${String(minutes).padStart(2, "0")}m`;
  }
  if (minutes > 0) {
    return `${minutes}m ${String(seconds).padStart(2, "0")}s`;
  }
  return `${seconds}s`;
}

// ---- log elision marker -----------------------------------------------------

/**
 * Mirrors `dearborn-server/src/evidence.rs`'s private `ELISION_MARKER`
 * exactly (D13: a log over 256 KB keeps head + tail with this text spliced
 * in between). Duplicated here rather than shipped over the wire as a
 * separate field, because the marker is *part of* `log` — the server has no
 * reason to expose it any other way. If the server's text ever changes,
 * `splitLog` degrades to treating the whole string as one un-elided segment
 * (still fully readable, just unstyled as a plain `<pre>`) rather than
 * mis-rendering or throwing.
 */
export const ELISION_MARKER =
  "\n\n... [dearborn: log elided — exceeded 256 KB; showing head + tail] ...\n\n";

/**
 * A log split at the elision marker, so the view can render the marker as a
 * distinct, styled divider between head and tail rather than raw text
 * embedded inline in a `<pre>` block — the AC's "logs are readable including
 * the elision marker". `tail` is `null` when the log was never capped (the
 * common case — most stages never come close to 256 KB).
 */
export interface LogSegments {
  head: string;
  tail: string | null;
}

export function splitLog(log: string): LogSegments {
  const idx = log.indexOf(ELISION_MARKER);
  if (idx === -1) {
    return { head: log, tail: null };
  }
  return { head: log.slice(0, idx), tail: log.slice(idx + ELISION_MARKER.length) };
}

// ---- live tool calls --------------------------------------------------------

/**
 * One tool invocation observed live on `task:<id>` for the currently-running
 * stage: a `tool_start` frame opens a pill with `status = "running"`; the
 * matching `tool_end` (same `toolCallId`) settles it to `"ok"` or `"error"`.
 * Name + status only — no output/arguments — matching the planning view's
 * chips.
 */
export interface ToolCall {
  toolCallId: string;
  name: string;
  status: "running" | "ok" | "error";
}

// ---- view-model: hydrate + live tail (T-563) --------------------------------

/**
 * The task detail pipeline view's model: a task id and its ordered
 * `agent_run` rows (oldest first, exactly as `GET /tasks/{id}/runs` returns
 * them — no client-side re-sort; the server's `created_at, rowid` order *is*
 * the true execution order, including repeated `test_gate`/`fix`/`review`
 * attempts, since each is one real row per real attempt).
 */
export interface PipelineState {
  taskId: string | null;
  runs: AgentRunSummary[];
  /**
   * The live tail: `text`/`error` `RunEvent` deltas for whichever row is
   * currently `status === "running"`, accumulated in EXACTLY the format the
   * server persists into `agent_run.log` (`AgentStageOutcome::absorb`: `Text`
   * deltas concatenated verbatim, `Error` appended as `\n[error] {message}\n`)
   * so `mergeHydratedLog` can find a true byte-for-byte overlap against the
   * REST-fetched snapshot. Grows unconditionally from the moment
   * `usePipelineStream` subscribes — including before `reconcileLiveLog` has
   * run, which is exactly the buffer that seam merges against. Reset to `""`
   * when the row it belongs to goes terminal (see `applyStageChanged`).
   */
  liveLog: string;
  /**
   * Whether `liveLog` has been reconciled against a REST `GET /runs/{id}`
   * snapshot yet (`reconcileLiveLog`) for the row currently running. `false`
   * means `liveLog` is still a raw, un-merged buffer of whatever streamed in
   * since subscribe — still fine to display (it's a true suffix of the
   * running row's log), just not yet known to be gap-free from the start of
   * that row's own log.
   */
  liveLogReconciled: boolean;
  /**
   * Tool-call pills for the currently-running stage, folded from live
   * `tool_start`/`tool_end` frames (client-only state — historical stages get
   * their pills from the REST events endpoint instead). Cleared whenever the
   * running stage ends so one stage's tools never bleed into the next.
   */
  liveTools: ToolCall[];
}

/** A fresh, empty view model. */
export function initialPipelineState(): PipelineState {
  return { taskId: null, runs: [], liveLog: "", liveLogReconciled: false, liveTools: [] };
}

/**
 * Hydrate the state from a REST load (`GET /tasks/{id}/runs`). Replaces any
 * prior runs. An empty `runs` array is a legitimate, common state (a task
 * that hasn't been claimed yet) — the view renders that as an empty state,
 * never an error; distinguishing the two is the component's job (reading
 * `state.runs.length`), not this function's.
 *
 * Deliberately does NOT touch `liveLog`/`liveLogReconciled` — see
 * `resetLiveTail`'s doc for why that reset has to happen strictly BEFORE
 * this call (at subscribe time), not here. By the time this runs, `liveLog`
 * may already hold text buffered from live frames that arrived during this
 * very REST round trip; wiping it here would reintroduce the "gap" failure
 * mode `usePipelineStream.ts`'s subscribe-before-hydrate ordering exists to
 * prevent.
 */
export function hydratePipeline(
  state: PipelineState,
  taskId: string,
  runs: AgentRunSummary[],
): PipelineState {
  state.taskId = taskId;
  state.runs = runs;
  return state;
}

/**
 * Reset the live-tail buffer. Call this exactly once, synchronously, right
 * before opening a NEW `task:<id>` subscription — the initial mount, or
 * `TaskPipelinePanel.vue`'s defensive reload when `taskId` changes under an
 * existing instance (a genuinely different task's buffer must not carry
 * over). Deliberately separate from `hydratePipeline`: the correct sequence
 * is subscribe (reset here, before any frame can possibly have arrived) ->
 * REST hydrate -> `reconcileLiveLog`, so any text buffered during the
 * hydrate's own round trip survives into the merge instead of being wiped by
 * the hydrate call that follows it.
 */
export function resetLiveTail(state: PipelineState): PipelineState {
  state.liveLog = "";
  state.liveLogReconciled = false;
  state.liveTools = [];
  return state;
}

/** The row currently `status === "running"`, or `null`. At most one ever is
 * (MILESTONE_2 §2.3's DAG walk serializes: one stage in flight per task). */
export function runningRun(state: PipelineState): AgentRunSummary | null {
  return state.runs.find((r) => r.status === "running") ?? null;
}

// ---- hydration-boundary reconciliation (T-563) ------------------------------

/**
 * Merge a REST-fetched log snapshot (`restLog`, some prefix of the running
 * row's true accumulated text as of the last D14 flush) with text the client
 * already buffered live (`bufferedText`, a suffix of that same true text
 * starting wherever the WS subscription began). See this file's header
 * comment for the full "no gap or duplication" rationale — in short: find
 * the longest suffix of `restLog` that exactly matches a prefix of
 * `bufferedText`, and append only what's left of `bufferedText` past that
 * overlap. `bufferedText` with no overlap at all appends in full (the "lands
 * before some buffered live events" case); `bufferedText` wholly contained
 * in `restLog`'s tail contributes nothing further (the "already contains
 * text that then arrives again live" case) — one rule, both directions.
 */
export function mergeHydratedLog(restLog: string, bufferedText: string): string {
  if (bufferedText.length === 0) {
    return restLog;
  }
  const maxOverlap = Math.min(restLog.length, bufferedText.length);
  for (let len = maxOverlap; len > 0; len--) {
    if (restLog.endsWith(bufferedText.slice(0, len))) {
      return restLog + bufferedText.slice(len);
    }
  }
  return restLog + bufferedText;
}

/**
 * Apply the hydration-boundary merge to `state.liveLog` in place, once the
 * running row's `GET /runs/{id}` snapshot (`restLog`) resolves. Idempotent
 * only in the sense that calling it twice with the true final log and an
 * empty buffer is harmless — callers only ever call this once per running
 * row, right after fetching its detail log (see `usePipelineStream.ts`'s doc
 * for where this sits in the subscribe/hydrate sequence).
 */
export function reconcileLiveLog(state: PipelineState, restLog: string): PipelineState {
  state.liveLog = mergeHydratedLog(restLog, state.liveLog);
  state.liveLogReconciled = true;
  return state;
}

// ---- WS reducer (T-563) -----------------------------------------------------

/** A WS frame as delivered on `task:<id>` (same envelope as every other stream). */
export interface PipelineFrame {
  topic: string;
  type: string;
  payload: unknown;
}

interface DeltaPayload {
  runId: string;
  delta: string;
}

interface ErrorPayload {
  message: string;
}

/** `stage_changed`'s payload (CONVENTIONS.md §2.6, T-530/T-532). */
interface StageChangedPayload {
  task_id: string;
  stage: string;
  attempt: number;
  status: string;
  verdict?: string | null;
}

/**
 * Fold one WS frame (`task:<id>`) into the pipeline state.
 *
 * - `text`: append the delta to `liveLog` — the running stage's live tail.
 * - `error`: append `\n[error] {message}\n`, matching
 *   `AgentStageOutcome::absorb` exactly so `mergeHydratedLog` can find a true
 *   overlap against the eventual REST log.
 * - `stage_changed`: advance the timeline (see `applyStageChanged`).
 * - `tool_start`/`tool_end`: open/close a pill in `liveTools` for the
 *   currently-running stage (see `ToolCall`).
 * - everything else (`thinking`, `started`,
 *   `session`, `exited`, `usage`, `activity`, `suggested_edits`,
 *   `ask_question`, acks, future kinds): ignored. None of these are part of
 *   `agent_run.log` server-side (only `Text`/`Error` are folded into it — see
 *   `absorb`) or carry enough to update the timeline, matching
 *   `dag/stream.ts`'s own `default` branches.
 */
export function applyPipelineFrame(state: PipelineState, frame: PipelineFrame): PipelineState {
  switch (frame.type) {
    case "text": {
      const p = frame.payload as DeltaPayload;
      state.liveLog += p?.delta ?? "";
      break;
    }
    case "error": {
      const p = frame.payload as ErrorPayload;
      state.liveLog += `\n[error] ${p?.message ?? "unknown error"}\n`;
      break;
    }
    case "stage_changed": {
      applyStageChanged(state, frame.payload as StageChangedPayload);
      break;
    }
    case "tool_start": {
      const p = frame.payload as { toolCallId?: string; name?: string } | null;
      state.liveTools.push({
        toolCallId: p?.toolCallId ?? "",
        name: p?.name ?? "tool",
        status: "running",
      });
      break;
    }
    case "tool_end": {
      const p = frame.payload as { toolCallId?: string; ok?: boolean } | null;
      const call = state.liveTools.find((c) => c.toolCallId === p?.toolCallId);
      if (call) {
        call.status = p?.ok ? "ok" : "error";
      }
      break;
    }
    default:
      break;
  }
  return state;
}

/**
 * `stage_changed` advances the timeline: find the row it describes by
 * `(stage, attempt)` (the pair `agent_run.attempt` is scoped by, matching how
 * the server itself identifies a stage's row) and update its terminal
 * status/verdict in place. When no such row exists — a stage this client's
 * hydrate never saw, e.g. a review round that both started and finished
 * between hydrate and now — synthesize one rather than dropping the frame;
 * the fields `stage_changed` doesn't carry (`id`, `session_id`, `started_at`,
 * `exit_code`) get the same "unknown, render it anyway" treatment
 * `controls.ts`'s `describeFailureReason` uses for a reason string it
 * doesn't recognize. `id` is a stable synthetic key (not a real `agent_run`
 * id — expanding this row would 404 against `GET /runs/{id}`, an accepted
 * gap since this client has no other id to use and inserting nothing at all
 * would be worse).
 *
 * If the row this frame just closed out is the one `liveLog` was tailing,
 * reset the buffer: the transcript for a NEXT stage (which this client has
 * no live signal to identify by name — see this file's header) must not
 * silently continue appending onto the just-finished stage's text.
 */
function applyStageChanged(state: PipelineState, p: StageChangedPayload): void {
  if (typeof p?.stage !== "string" || typeof p.attempt !== "number") {
    return;
  }
  const existing = state.runs.find((r) => r.stage === p.stage && r.attempt === p.attempt);
  const wasRunning = existing !== undefined && existing.status === "running";
  if (existing) {
    existing.status = p.status;
    existing.verdict = p.verdict ?? null;
  } else {
    const now = Date.now();
    state.runs.push({
      id: `stage_changed:${p.stage}:${p.attempt}`,
      task_id: state.taskId,
      epic_id: null,
      stage: p.stage,
      attempt: p.attempt,
      status: p.status,
      verdict: p.verdict ?? null,
      session_id: null,
      actual_model: null,
      started_at: null,
      ended_at: now,
      exit_code: null,
      created_at: now,
    });
  }
  if (wasRunning) {
    state.liveLog = "";
    state.liveLogReconciled = false;
    // Tools belong to the stage that just ended; don't let them persist into
    // whatever runs next.
    state.liveTools = [];
  }
}
