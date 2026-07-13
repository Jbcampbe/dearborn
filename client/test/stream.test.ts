// Unit tests for the pure planning stream reducer (T-204). No browser, no WS —
// just fold hand-built frames and assert the resulting view model. This is the
// risk-bearing logic (accumulating deltas, matching tool start/end, finalizing a
// turn, applying live epic updates), so it is tested in isolation.

import { describe, expect, it } from "vitest";

import type { Epic, TranscriptMessage } from "../src/api/epics";
import {
  appendUserTurn,
  applyFrame,
  hydrate,
  initialState,
  type EpicFrame,
} from "../src/planning/stream";

const TOPIC = "epic:E1";

function frame(type: string, payload: unknown): EpicFrame {
  return { topic: TOPIC, type, payload };
}

function makeEpic(overrides: Partial<Epic> = {}): Epic {
  return {
    id: "E1",
    project_id: "P1",
    title: "Ship it",
    product_context: null,
    technical_context: null,
    status: "Planning",
    created_at: 1,
    updated_at: 1,
    ...overrides,
  };
}

describe("planning stream reducer", () => {
  it("accumulates multiple text deltas into one streaming agent message", () => {
    const state = initialState();
    applyFrame(state, frame("started", { runId: "r1" }));
    applyFrame(state, frame("text", { runId: "r1", delta: "Hello" }));
    applyFrame(state, frame("text", { runId: "r1", delta: ", " }));
    applyFrame(state, frame("text", { runId: "r1", delta: "world" }));

    expect(state.streaming).not.toBeNull();
    expect(state.streaming?.text).toBe("Hello, world");
    // Nothing is finalized until `exited`.
    expect(state.turns).toHaveLength(0);
  });

  it("finalizes the streaming turn into history on exited", () => {
    const state = initialState();
    applyFrame(state, frame("started", { runId: "r1" }));
    applyFrame(state, frame("text", { runId: "r1", delta: "done" }));
    applyFrame(state, frame("exited", { runId: "r1", exitCode: 0, cancelled: false }));

    expect(state.streaming).toBeNull();
    expect(state.turns).toHaveLength(1);
    expect(state.turns[0]).toMatchObject({ role: "agent", text: "done" });
  });

  it("tracks tool_start/tool_end and finalizes tool turns before the agent text", () => {
    const state = initialState();
    applyFrame(state, frame("started", { runId: "r1" }));
    applyFrame(
      state,
      frame("tool_start", { runId: "r1", toolCallId: "t1", name: "mcp__deerborn__update_epic" }),
    );
    // While running, the chip is pending.
    expect(state.streaming?.toolCalls[0]).toMatchObject({
      toolCallId: "t1",
      name: "mcp__deerborn__update_epic",
      status: "running",
    });

    applyFrame(state, frame("tool_end", { runId: "r1", toolCallId: "t1", ok: true, output: "ok" }));
    expect(state.streaming?.toolCalls[0]).toMatchObject({ status: "ok", output: "ok" });

    applyFrame(state, frame("text", { runId: "r1", delta: "updated the epic" }));
    applyFrame(state, frame("exited", { runId: "r1", exitCode: 0, cancelled: false }));

    // Server persistence order: tool turn(s) first, then the agent text turn.
    expect(state.turns).toHaveLength(2);
    expect(state.turns[0]).toMatchObject({ role: "tool" });
    expect(state.turns[0].tool).toMatchObject({ name: "mcp__deerborn__update_epic", status: "ok" });
    expect(state.turns[1]).toMatchObject({ role: "agent", text: "updated the epic" });
  });

  it("marks a failed tool_end as an error", () => {
    const state = initialState();
    applyFrame(state, frame("started", { runId: "r1" }));
    applyFrame(state, frame("tool_start", { runId: "r1", toolCallId: "t1", name: "Read" }));
    applyFrame(state, frame("tool_end", { runId: "r1", toolCallId: "t1", ok: false }));
    expect(state.streaming?.toolCalls[0]).toMatchObject({ status: "error", output: null });
  });

  it("applies epic_updated to the live epic record", () => {
    const state = initialState();
    hydrate(state, makeEpic(), []);
    expect(state.epic?.product_context).toBeNull();

    const updated = makeEpic({ product_context: "Users need X because Y.", updated_at: 2 });
    applyFrame(state, frame("epic_updated", updated));

    expect(state.epic?.product_context).toBe("Users need X because Y.");
    expect(state.epic?.updated_at).toBe(2);
  });

  it("captures a terminal error frame", () => {
    const state = initialState();
    applyFrame(state, frame("started", { runId: "r1" }));
    applyFrame(state, frame("error", { runId: "r1", message: "model exploded" }));
    expect(state.error).toBe("model exploded");

    // A subsequent run clears the prior error.
    applyFrame(state, frame("started", { runId: "r2" }));
    expect(state.error).toBeNull();
  });

  it("ignores acks and unknown/forward-compat frames", () => {
    const state = initialState();
    applyFrame(state, frame("subscribed", {}));
    applyFrame(state, frame("usage", { runId: "r1", inputTokens: 5 }));
    applyFrame(state, frame("session", { runId: "r1", sessionId: "s1" }));
    applyFrame(state, frame("event", { kind: "somethingNew" }));
    expect(state.turns).toHaveLength(0);
    expect(state.streaming).toBeNull();
    expect(state.error).toBeNull();
  });

  it("hydrates history from a transcript, parsing tool messages", () => {
    const messages: TranscriptMessage[] = [
      msg("m1", "user", "build me a thing"),
      msg("m2", "tool", JSON.stringify({ kind: "toolEnd", toolCallId: "t1", name: "Read", ok: true, output: "file.rs" })),
      msg("m3", "agent", "here is the plan"),
    ];
    const state = initialState();
    hydrate(state, makeEpic(), messages);

    expect(state.turns).toHaveLength(3);
    expect(state.turns[0]).toMatchObject({ role: "user", text: "build me a thing" });
    expect(state.turns[1]).toMatchObject({ role: "tool" });
    expect(state.turns[1].tool).toMatchObject({ name: "Read", status: "ok", output: "file.rs" });
    expect(state.turns[2]).toMatchObject({ role: "agent", text: "here is the plan" });
  });

  it("appends an optimistic user turn", () => {
    const state = initialState();
    hydrate(state, makeEpic(), []);
    appendUserTurn(state, "hi there");
    expect(state.turns).toHaveLength(1);
    expect(state.turns[0]).toMatchObject({ role: "user", text: "hi there" });
    // Distinct keys for successive local turns.
    appendUserTurn(state, "again");
    expect(state.turns[0].id).not.toBe(state.turns[1].id);
  });

  it("stamps hydrated turns with their persisted phase", () => {
    const messages: TranscriptMessage[] = [
      { ...msg("m1", "user", "product idea"), phase: "product" },
      { ...msg("m2", "agent", "the plan"), phase: "technical" },
    ];
    const state = initialState();
    hydrate(state, makeEpic(), messages);
    expect(state.turns[0].phase).toBe("product");
    expect(state.turns[1].phase).toBe("technical");
  });

  it("stamps local turns with the state's current phase", () => {
    const state = initialState();
    hydrate(state, makeEpic(), []);
    // Default phase is product.
    appendUserTurn(state, "product msg");
    expect(state.turns[0].phase).toBe("product");

    // After advancing, the view flips the phase; new turns take it.
    state.phase = "technical";
    appendUserTurn(state, "technical msg");
    expect(state.turns[1].phase).toBe("technical");

    // A streamed technical turn finalizes under the current phase too.
    applyFrame(state, frame("started", { runId: "r1" }));
    applyFrame(state, frame("text", { runId: "r1", delta: "tech reply" }));
    applyFrame(state, frame("exited", { runId: "r1", exitCode: 0, cancelled: false }));
    expect(state.turns[2]).toMatchObject({ role: "agent", text: "tech reply", phase: "technical" });
  });

  it("tolerates a text delta arriving before started", () => {
    const state = initialState();
    applyFrame(state, frame("text", { runId: "r1", delta: "eager" }));
    expect(state.streaming?.text).toBe("eager");
  });
});

function msg(id: string, role: TranscriptMessage["role"], content: string): TranscriptMessage {
  return { id, epic_id: "E1", phase: "product", role, content, seq: 1, created_at: 1 };
}
