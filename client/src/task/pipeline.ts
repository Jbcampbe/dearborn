// T-562: pure view-model helpers for the task detail pipeline view — stage
// labeling, attempt/round-number display, duration formatting, and log
// elision-marker splitting over a task's `agent_run` history (`GET
// /tasks/{id}/runs` + `GET /runs/{id}`, T-512). Mirrors `board/controls.ts`/
// `dag/stream.ts`: framework-free, dependency-free (no Vue, no fetch, no WS)
// so every formatting/ordering decision here is unit-tested without a
// browser — `TaskPipelinePanel.vue` is the only consumer.
//
// `PipelineState` deliberately keeps the same shape `dag/stream.ts`'s
// `DagState` does (an `initialState` + a `hydrate` that replaces the whole
// list from a REST load) so a *future* `applyPipelineFrame(state, frame)`
// reducer has an obvious place to fold WS frames into — that's T-563's job
// (subscribing `task:<id>`, appending live `RunEvent` text to the running
// stage's log, and folding `stage_changed` into the matching row's
// status/verdict/`ended_at`), not built here. This module stops at hydrate;
// no reducer or WS composable lives in this file.

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

// ---- view-model: hydrate seam for T-563 ------------------------------------

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
}

/** A fresh, empty view model. */
export function initialPipelineState(): PipelineState {
  return { taskId: null, runs: [] };
}

/**
 * Hydrate the state from a REST load (`GET /tasks/{id}/runs`). Replaces any
 * prior runs. An empty `runs` array is a legitimate, common state (a task
 * that hasn't been claimed yet) — the view renders that as an empty state,
 * never an error; distinguishing the two is the component's job (reading
 * `state.runs.length`), not this function's.
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
