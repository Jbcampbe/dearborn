// Unit tests for the pure task-pipeline view-model helpers (T-562). No
// browser, no fetch — hand-built `AgentRunSummary` fixtures folded/formatted
// through pure functions, mirroring `dag.test.ts`/`controls.test.ts`.

import { describe, expect, it } from "vitest";

import type { AgentRunSummary } from "../src/api/tasks";
import {
  applyPipelineFrame,
  attemptLabel,
  ELISION_MARKER,
  durationLabel,
  humanAttempt,
  hydratePipeline,
  initialPipelineState,
  mergeHydratedLog,
  reconcileLiveLog,
  reconcileLiveThinking,
  resetLiveTail,
  runStatusLabel,
  runningRun,
  splitLog,
  stageLabel,
  verdictLabel,
  type PipelineFrame,
} from "../src/task/pipeline";

function run(overrides: Partial<AgentRunSummary> = {}): AgentRunSummary {
  return {
    id: "R1",
    task_id: "T1",
    epic_id: null,
    stage: "implement",
    attempt: 1,
    status: "ok",
    verdict: null,
    session_id: "sess-1",
    started_at: 1_000,
    ended_at: 2_000,
    exit_code: 0,
    created_at: 1_000,
    ...overrides,
  };
}

describe("stageLabel", () => {
  it("labels every §2.2 stage", () => {
    expect(stageLabel("setup")).toBe("Setup");
    expect(stageLabel("preflight")).toBe("Preflight");
    expect(stageLabel("implement")).toBe("Implement");
    expect(stageLabel("test_gate")).toBe("Test");
    expect(stageLabel("fix")).toBe("Fix");
    expect(stageLabel("verify_complete")).toBe("Verify complete");
    expect(stageLabel("review")).toBe("Review");
    expect(stageLabel("commit")).toBe("Commit");
    expect(stageLabel("push")).toBe("Push");
    expect(stageLabel("summarize")).toBe("Summarize");
  });

  it("falls back to a title-cased rendering of an unknown stage rather than dropping it", () => {
    expect(stageLabel("future_stage")).toBe("Future Stage");
  });
});

describe("humanAttempt / attemptLabel", () => {
  it("adds 1 for the 0-indexed stages (test_gate, fix, review)", () => {
    expect(humanAttempt(run({ stage: "test_gate", attempt: 0 }))).toBe(1);
    expect(humanAttempt(run({ stage: "fix", attempt: 2 }))).toBe(3);
    expect(humanAttempt(run({ stage: "review", attempt: 0 }))).toBe(1);
  });

  it("leaves the already-1-based singleton stages alone", () => {
    expect(humanAttempt(run({ stage: "implement", attempt: 1 }))).toBe(1);
    expect(humanAttempt(run({ stage: "commit", attempt: 1 }))).toBe(1);
  });

  it("labels a review row as a Round, everything else as an Attempt", () => {
    expect(attemptLabel(run({ stage: "review", attempt: 0 }))).toBe("Round 1");
    expect(attemptLabel(run({ stage: "test_gate", attempt: 2 }))).toBe("Attempt 3");
    expect(attemptLabel(run({ stage: "implement", attempt: 1 }))).toBe("Attempt 1");
  });
});

describe("runStatusLabel / verdictLabel", () => {
  it("labels the known agent_run.status vocabulary", () => {
    expect(runStatusLabel("running")).toBe("Running");
    expect(runStatusLabel("ok")).toBe("OK");
    expect(runStatusLabel("error")).toBe("Error");
    expect(runStatusLabel("timeout")).toBe("Timed out");
    expect(runStatusLabel("cancelled")).toBe("Cancelled");
  });

  it("falls back to a title-cased rendering of an unknown status", () => {
    expect(runStatusLabel("weird_status")).toBe("Weird Status");
  });

  it("labels the D9 verdict vocabulary and falls back to the raw string otherwise", () => {
    expect(verdictLabel("PASS")).toBe("Pass");
    expect(verdictLabel("NEEDS_CHANGES")).toBe("Needs changes");
    expect(verdictLabel("BLOCKED")).toBe("Blocked");
    expect(verdictLabel("SOMETHING_ELSE")).toBe("SOMETHING_ELSE");
  });
});

describe("durationLabel", () => {
  it("renders seconds, minutes, and hours", () => {
    expect(durationLabel(run({ started_at: 0, ended_at: 12_000 }))).toBe("12s");
    expect(durationLabel(run({ started_at: 0, ended_at: 64_000 }))).toBe("1m 04s");
    expect(durationLabel(run({ started_at: 0, ended_at: 3_722_000 }))).toBe("1h 02m");
  });

  it("measures a still-running row against `now` and marks it as still counting", () => {
    const r = run({ started_at: 1_000, ended_at: null });
    expect(durationLabel(r, 9_000)).toBe("8s so far");
  });

  it("never goes negative (a clock skew between started_at and now)", () => {
    const r = run({ started_at: 5_000, ended_at: null });
    expect(durationLabel(r, 1_000)).toBe("0s so far");
  });

  it("is defensive against a null started_at (schema allows it; should never crash)", () => {
    expect(durationLabel(run({ started_at: null, ended_at: null }))).toBe("—");
  });
});

describe("splitLog", () => {
  it("returns the whole log with a null tail when never elided", () => {
    const segments = splitLog("plain short log\n");
    expect(segments).toEqual({ head: "plain short log\n", tail: null });
  });

  it("splits head/tail at the exact server elision marker", () => {
    const log = `first line${ELISION_MARKER}last line`;
    const segments = splitLog(log);
    expect(segments.head).toBe("first line");
    expect(segments.tail).toBe("last line");
  });

  it("the marker text itself is legible (readable, not mangled) once split out", () => {
    // The AC: "logs are readable including the elision marker" — assert the
    // real marker text (not a guessed shape) survives verbatim so a caller
    // can render it as its own styled divider between head and tail.
    expect(ELISION_MARKER).toContain("dearborn: log elided");
    expect(ELISION_MARKER).toContain("256 KB");
    expect(ELISION_MARKER).toContain("showing head + tail");
  });
});

describe("initialPipelineState / hydratePipeline", () => {
  it("starts empty", () => {
    const state = initialPipelineState();
    expect(state.taskId).toBeNull();
    expect(state.runs).toEqual([]);
  });

  it("hydrate stamps the task id and replaces runs, oldest-first order preserved", () => {
    const state = initialPipelineState();
    const runs = [run({ id: "R1", stage: "setup" }), run({ id: "R2", stage: "implement" })];
    hydratePipeline(state, "T1", runs);

    expect(state.taskId).toBe("T1");
    expect(state.runs.map((r) => r.id)).toEqual(["R1", "R2"]);
  });

  it("an empty runs list hydrates cleanly (the AC's empty state, not an error)", () => {
    const state = initialPipelineState();
    hydratePipeline(state, "T1", []);

    expect(state.taskId).toBe("T1");
    expect(state.runs).toEqual([]);
  });

  it("re-hydrating replaces the prior list rather than appending", () => {
    const state = initialPipelineState();
    hydratePipeline(state, "T1", [run({ id: "R1" })]);
    hydratePipeline(state, "T1", [run({ id: "R2" }), run({ id: "R3" })]);

    expect(state.runs.map((r) => r.id)).toEqual(["R2", "R3"]);
  });

  it("starts with an empty, un-reconciled live-tail buffer", () => {
    const state = initialPipelineState();
    expect(state.liveLog).toBe("");
    expect(state.liveLogReconciled).toBe(false);
    expect(state.liveThinking).toBe("");
  });

  it("does NOT reset liveLog -- resetLiveTail owns that (see its own doc)", () => {
    const state = initialPipelineState();
    state.liveLog = "buffered while the REST call was in flight";
    hydratePipeline(state, "T1", []);
    expect(state.liveLog).toBe("buffered while the REST call was in flight");
  });
});

describe("runningRun", () => {
  it("finds the one row with status running", () => {
    const state = initialPipelineState();
    hydratePipeline(state, "T1", [
      run({ id: "R1", stage: "implement", status: "ok" }),
      run({ id: "R2", stage: "test_gate", status: "running" }),
    ]);
    expect(runningRun(state)?.id).toBe("R2");
  });

  it("returns null when nothing is running", () => {
    const state = initialPipelineState();
    hydratePipeline(state, "T1", [run({ status: "ok" })]);
    expect(runningRun(state)).toBeNull();
  });
});

describe("mergeHydratedLog", () => {
  it("appends in full when there is no overlap at all", () => {
    // The joiner's hydrate response lands before some of the already-
    // buffered live events represent genuinely NEW text -- nothing to trim.
    expect(mergeHydratedLog("line one\n", "line two\n")).toBe("line one\nline two\n");
  });

  it("trims the overlap when the hydrated log already contains text that then arrives again live", () => {
    // The REST snapshot's flush caught up past (or to) the point the client
    // started buffering: "line one\nline two\n" is common to both.
    const restLog = "line one\nline two\n";
    const buffered = "line two\nline three\n";
    expect(mergeHydratedLog(restLog, buffered)).toBe("line one\nline two\nline three\n");
  });

  it("contributes nothing further when the buffer is wholly contained in the rest log's tail", () => {
    expect(mergeHydratedLog("all of it here\n", "all of it here\n")).toBe("all of it here\n");
    expect(mergeHydratedLog("all of it here\nand more", "and more")).toBe("all of it here\nand more");
  });

  it("an empty buffer is a no-op", () => {
    expect(mergeHydratedLog("whatever was flushed", "")).toBe("whatever was flushed");
  });

  it("an empty rest log (no flush happened yet) just takes the whole buffer", () => {
    expect(mergeHydratedLog("", "everything streamed live")).toBe("everything streamed live");
  });
});

describe("resetLiveTail / reconcileLiveLog", () => {
  it("resetLiveTail clears the live-tail fields", () => {
    const state = initialPipelineState();
    state.liveLog = "stale from a previous task";
    state.liveLogReconciled = true;
    state.liveThinking = "stale reasoning";
    resetLiveTail(state);
    expect(state.liveLog).toBe("");
    expect(state.liveLogReconciled).toBe(false);
    expect(state.liveThinking).toBe("");
  });

  it("reconcileLiveLog merges and marks reconciled", () => {
    const state = initialPipelineState();
    state.liveLog = "tail two\n";
    reconcileLiveLog(state, "tail one\ntail two\n");
    expect(state.liveLog).toBe("tail one\ntail two\n");
    expect(state.liveLogReconciled).toBe(true);
  });

  it("reconcileLiveThinking merges the buffered reasoning against the REST snapshot, no duplication", () => {
    const state = initialPipelineState();
    // Buffered live thinking that the flushed snapshot already caught up past.
    state.liveThinking = "considering the diff";
    reconcileLiveThinking(state, "earlier reasoning\nconsidering the diff");
    expect(state.liveThinking).toBe("earlier reasoning\nconsidering the diff");
  });

  it("reconcileLiveThinking appends buffered reasoning the snapshot predates, no gap", () => {
    const state = initialPipelineState();
    state.liveThinking = "newest reasoning";
    reconcileLiveThinking(state, "flushed reasoning so far\n");
    expect(state.liveThinking).toBe("flushed reasoning so far\nnewest reasoning");
  });
});

describe("applyPipelineFrame -- the hydration boundary (T-563's hard part)", () => {
  const TOPIC = "task:T1";
  function frame(type: string, payload: unknown): PipelineFrame {
    return { topic: TOPIC, type, payload };
  }

  it("text frames append verbatim to liveLog", () => {
    const state = initialPipelineState();
    applyPipelineFrame(state, frame("text", { runId: "r1", delta: "hello " }));
    applyPipelineFrame(state, frame("text", { runId: "r1", delta: "world" }));
    expect(state.liveLog).toBe("hello world");
  });

  it("error frames append the same `[error] ...` shape AgentStageOutcome::absorb writes server-side", () => {
    const state = initialPipelineState();
    applyPipelineFrame(state, frame("text", { runId: "r1", delta: "partial output" }));
    applyPipelineFrame(state, frame("error", { message: "boom" }));
    expect(state.liveLog).toBe("partial output\n[error] boom\n");
  });

  it("keeps thinking out of liveLog (not part of agent_run.log) but folds it into liveThinking", () => {
    const state = initialPipelineState();
    applyPipelineFrame(state, frame("thinking", { runId: "r1", delta: "pondering " }));
    applyPipelineFrame(state, frame("thinking", { runId: "r1", delta: "the plan" }));
    applyPipelineFrame(state, frame("tool_start", { runId: "r1", toolCallId: "c1", name: "bash" }));
    applyPipelineFrame(state, frame("started", { runId: "r1" }));
    applyPipelineFrame(state, frame("subscribed", {}));
    // Thinking accumulates on its own field...
    expect(state.liveThinking).toBe("pondering the plan");
    // ...and never leaks into the persisted-log tail.
    expect(state.liveLog).toBe("");
  });

  it("interleaved thinking and text deltas land in their own buffers, never mixed", () => {
    const state = initialPipelineState();
    applyPipelineFrame(state, frame("thinking", { runId: "r1", delta: "let me check X" }));
    applyPipelineFrame(state, frame("text", { runId: "r1", delta: "Here is the answer." }));
    applyPipelineFrame(state, frame("thinking", { runId: "r1", delta: " and now Y" }));
    expect(state.liveThinking).toBe("let me check X and now Y");
    expect(state.liveLog).toBe("Here is the answer.");
  });

  it("a joiner whose hydrate lands before some buffered live events sees all of it, no gap", () => {
    // Subscribe-first ordering: frames folded into `liveLog` BEFORE the REST
    // hydrate resolves must survive the merge in full when they're new text.
    const state = initialPipelineState();
    resetLiveTail(state);
    // Buffered while `GET /tasks/{id}/runs` + `GET /runs/{id}` were in flight:
    applyPipelineFrame(state, frame("text", { runId: "r1", delta: "chunk A" }));
    applyPipelineFrame(state, frame("text", { runId: "r1", delta: "chunk B" }));

    hydratePipeline(state, "T1", [run({ id: "R1", stage: "test_gate", status: "running" })]);
    // The REST snapshot's last flush predates both buffered chunks entirely.
    reconcileLiveLog(state, "earlier flushed text\n");

    expect(state.liveLog).toBe("earlier flushed text\nchunk Achunk B");
  });

  it("a joiner whose hydrate already contains text that then arrives again live does not duplicate it", () => {
    const state = initialPipelineState();
    resetLiveTail(state);
    // The flush that produced the REST snapshot caught up past the client's
    // subscribe point, so these two deltas are ALSO present in restLog.
    applyPipelineFrame(state, frame("text", { runId: "r1", delta: "chunk A" }));
    applyPipelineFrame(state, frame("text", { runId: "r1", delta: "chunk B" }));

    hydratePipeline(state, "T1", [run({ id: "R1", stage: "test_gate", status: "running" })]);
    reconcileLiveLog(state, "earlier flushed text\nchunk Achunk B");

    // Not duplicated -- "chunk Achunk B" appears exactly once.
    expect(state.liveLog).toBe("earlier flushed text\nchunk Achunk B");
    expect(state.liveLog.split("chunk Achunk B")).toHaveLength(2);
  });

  it("frames received AFTER reconciliation simply keep appending (past the boundary, no more merging)", () => {
    const state = initialPipelineState();
    hydratePipeline(state, "T1", [run({ id: "R1", stage: "test_gate", status: "running" })]);
    reconcileLiveLog(state, "flushed so far\n");
    applyPipelineFrame(state, frame("text", { runId: "r1", delta: "brand new live text" }));
    expect(state.liveLog).toBe("flushed so far\nbrand new live text");
  });
});

describe("applyPipelineFrame -- stage_changed advances the timeline", () => {
  const TOPIC = "task:T1";
  function frame(type: string, payload: unknown): PipelineFrame {
    return { topic: TOPIC, type, payload };
  }

  it("updates an existing row's status/verdict in place", () => {
    const state = initialPipelineState();
    hydratePipeline(state, "T1", [run({ id: "R1", stage: "review", attempt: 0, status: "running" })]);

    applyPipelineFrame(
      state,
      frame("stage_changed", {
        task_id: "T1",
        stage: "review",
        attempt: 0,
        status: "ok",
        verdict: "NEEDS_CHANGES",
      }),
    );

    expect(state.runs).toHaveLength(1);
    expect(state.runs[0].status).toBe("ok");
    expect(state.runs[0].verdict).toBe("NEEDS_CHANGES");
  });

  it("resets the live-tail buffer when the row it closed out was the one being tailed", () => {
    const state = initialPipelineState();
    hydratePipeline(state, "T1", [run({ id: "R1", stage: "review", attempt: 0, status: "running" })]);
    applyPipelineFrame(state, frame("text", { runId: "r1", delta: "reviewing..." }));
    applyPipelineFrame(state, frame("thinking", { runId: "r1", delta: "considering the diff" }));
    expect(state.liveLog).toBe("reviewing...");
    expect(state.liveThinking).toBe("considering the diff");

    applyPipelineFrame(
      state,
      frame("stage_changed", { task_id: "T1", stage: "review", attempt: 0, status: "ok", verdict: "PASS" }),
    );

    expect(state.liveLog).toBe("");
    expect(state.liveLogReconciled).toBe(false);
    // Reasoning belongs to the stage that just ended — it must not bleed forward.
    expect(state.liveThinking).toBe("");
  });

  it("inserts a synthesized row for a stage this client's hydrate never saw, rather than dropping it", () => {
    // E.g. a review round that both started and finished between this
    // client's one-time hydrate and now.
    const state = initialPipelineState();
    hydratePipeline(state, "T1", [run({ id: "R1", stage: "implement", status: "ok" })]);

    applyPipelineFrame(
      state,
      frame("stage_changed", {
        task_id: "T1",
        stage: "verify_complete",
        attempt: 0,
        status: "ok",
        verdict: "PASS",
      }),
    );

    expect(state.runs).toHaveLength(2);
    const inserted = state.runs[1];
    expect(inserted.stage).toBe("verify_complete");
    expect(inserted.attempt).toBe(0);
    expect(inserted.status).toBe("ok");
    expect(inserted.verdict).toBe("PASS");
    expect(inserted.task_id).toBe("T1");
  });

  it("does not reset liveLog when the closed-out row was not the one running (defensive)", () => {
    const state = initialPipelineState();
    hydratePipeline(state, "T1", [
      run({ id: "R1", stage: "review", attempt: 0, status: "ok", verdict: "NEEDS_CHANGES" }),
      run({ id: "R2", stage: "test_gate", attempt: 1, status: "running" }),
    ]);
    applyPipelineFrame(state, frame("text", { runId: "r1", delta: "still testing" }));

    // A late/duplicate stage_changed for the ALREADY-closed review row.
    applyPipelineFrame(
      state,
      frame("stage_changed", {
        task_id: "T1",
        stage: "review",
        attempt: 0,
        status: "ok",
        verdict: "NEEDS_CHANGES",
      }),
    );

    expect(state.liveLog).toBe("still testing");
  });

  it("ignores a malformed stage_changed payload rather than throwing", () => {
    const state = initialPipelineState();
    hydratePipeline(state, "T1", [run({ id: "R1" })]);
    const before = JSON.stringify(state);

    applyPipelineFrame(state, frame("stage_changed", null));
    applyPipelineFrame(state, frame("stage_changed", {}));
    applyPipelineFrame(state, frame("stage_changed", { stage: "review" }));

    expect(JSON.stringify(state)).toBe(before);
  });
});
