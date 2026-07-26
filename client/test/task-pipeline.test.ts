// Unit tests for the pure task-pipeline view-model helpers (T-562). No
// browser, no fetch — hand-built `AgentRunSummary` fixtures folded/formatted
// through pure functions, mirroring `dag.test.ts`/`controls.test.ts`.

import { describe, expect, it } from "vitest";

import type { AgentRunSummary } from "../src/api/tasks";
import {
  attemptLabel,
  ELISION_MARKER,
  durationLabel,
  humanAttempt,
  hydratePipeline,
  initialPipelineState,
  runStatusLabel,
  splitLog,
  stageLabel,
  verdictLabel,
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
});
