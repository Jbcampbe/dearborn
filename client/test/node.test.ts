// Unit tests for the node session stream reducer. No browser, no WS — fold
// hand-built frames and assert the resulting view model. Mirrors the map
// reducer tests (`map.test.ts`) and the old planning-chat reducer tests this
// module was refactored from.

import { describe, expect, it } from "vitest";

import type { NodeMessage, NodeSessionView } from "../src/api/nodes";
import {
  appendMessage,
  applyNodeFrame,
  hydrateNode,
  initialNodeState,
  setSession,
  type NodeFrame,
  type NodeStreamState,
} from "../src/node/stream";

const TOPIC = "node:N1";

function frame(type: string, payload: unknown): NodeFrame {
  return { topic: TOPIC, type, payload };
}

function message(overrides: Partial<NodeMessage> = {}): NodeMessage {
  return {
    id: "M1",
    node_id: "N1",
    role: "user",
    actor_user_id: "U1",
    content: "Which store?",
    seq: 1,
    created_at: 100,
    ...overrides,
  };
}

function sessionView(overrides: Partial<NodeSessionView> = {}): NodeSessionView {
  return {
    node_id: "N1",
    harness_session_id: null,
    status: "active",
    created_at: 1,
    updated_at: 1,
    messages: [],
    ...overrides,
  };
}

function hydrated(overrides: Partial<NodeSessionView> = {}): NodeStreamState {
  const state = initialNodeState();
  hydrateNode(state, "N1", sessionView(overrides));
  return state;
}

describe("node stream hydration", () => {
  it("hydrate stamps the node id, session, and transcript", () => {
    const state = hydrated({
      harness_session_id: "sess-1",
      messages: [message(), message({ id: "M2", role: "agent", actor_user_id: null, seq: 2 })],
    });

    expect(state.nodeId).toBe("N1");
    expect(state.session?.harness_session_id).toBe("sess-1");
    expect(state.messages).toHaveLength(2);
    expect(state.streaming).toBeNull();
  });

  it("setSession replaces just the resume handle", () => {
    const state = hydrated();
    setSession(state, {
      node_id: "N1",
      harness_session_id: "sess-9",
      status: "complete",
      created_at: 1,
      updated_at: 2,
    });

    expect(state.session?.harness_session_id).toBe("sess-9");
    expect(state.session?.status).toBe("complete");
  });
});

describe("message folding", () => {
  it("message frames append persisted turns", () => {
    const state = hydrated();
    applyNodeFrame(state, frame("message", message()));

    expect(state.messages).toHaveLength(1);
    expect(state.messages[0].content).toBe("Which store?");
  });

  it("dedupes by id (the POST response races its own WS fan-out)", () => {
    const state = hydrated();
    appendMessage(state, message());
    applyNodeFrame(state, frame("message", message()));

    expect(state.messages).toHaveLength(1);
  });

  it("keeps the transcript in (seq, id) order regardless of arrival order", () => {
    const state = hydrated();
    applyNodeFrame(state, frame("message", message({ id: "M3", seq: 3 })));
    applyNodeFrame(state, frame("message", message({ id: "M1", seq: 1 })));
    applyNodeFrame(state, frame("message", message({ id: "M2", seq: 2 })));

    expect(state.messages.map((m) => m.id)).toEqual(["M1", "M2", "M3"]);
  });

  it("an agent message finalizes the streaming turn and clears run errors", () => {
    const state = hydrated({ messages: [message()] });
    applyNodeFrame(state, frame("started", { runId: "r1" }));
    applyNodeFrame(state, frame("text", { delta: "Let me grill you." }));
    applyNodeFrame(state, frame("error", { message: "boom" }));
    applyNodeFrame(state, frame("exited", { runId: "r1" }));
    applyNodeFrame(state, frame("message", message({ id: "M2", role: "agent", actor_user_id: null, seq: 2, content: "Let me grill you." })));

    expect(state.streaming).toBeNull();
    expect(state.error).toBeNull();
    expect(state.messages.map((m) => m.role)).toEqual(["user", "agent"]);
  });

  it("a user message does not clear a streaming turn (other participants post mid-run)", () => {
    const state = hydrated({ messages: [message()] });
    applyNodeFrame(state, frame("started", { runId: "r1" }));
    applyNodeFrame(state, frame("text", { delta: "thinking out loud" }));
    applyNodeFrame(state, frame("message", message({ id: "M2", actor_user_id: "U2", seq: 2, content: "chiming in" })));

    expect(state.streaming?.text).toBe("thinking out loud");
    expect(state.messages).toHaveLength(2);
  });

  it("ignores messages for other nodes and malformed payloads", () => {
    const state = hydrated();
    const before = JSON.stringify(state);

    applyNodeFrame(state, frame("message", message({ node_id: "OTHER" })));
    applyNodeFrame(state, frame("message", null));
    applyNodeFrame(state, frame("message", { nope: true }));

    expect(JSON.stringify(state.messages)).toBe("[]");
    expect(state.nodeId).toBe("N1");
    expect(JSON.stringify(state.session)).toBe(JSON.stringify(hydrated().session));
  });

  it("drops malformed local appends without throwing", () => {
    const state = hydrated();
    appendMessage(state, {} as NodeMessage);
    expect(state.messages).toHaveLength(0);
  });
});

describe("streaming turn folding", () => {
  it("started opens a fresh turn and clears the prior error", () => {
    const state = hydrated();
    state.error = "old failure";
    applyNodeFrame(state, frame("started", { runId: "r1" }));

    expect(state.error).toBeNull();
    expect(state.streaming).toEqual({
      runId: "r1",
      text: "",
      thinking: "",
      toolCalls: [],
      ended: false,
    });
  });

  it("session annotates the run id but never opens a turn", () => {
    const state = hydrated();
    applyNodeFrame(state, frame("session", { runId: "r0" }));
    expect(state.streaming).toBeNull();

    applyNodeFrame(state, frame("started", {}));
    applyNodeFrame(state, frame("session", { runId: "r1", sessionId: "sess-1" }));
    expect(state.streaming?.runId).toBe("r1");
  });

  it("text/thinking deltas accumulate (stray deltas still land)", () => {
    const state = hydrated();
    // A stray delta before `started` still lands (ensureStreaming creates the turn)…
    applyNodeFrame(state, frame("text", { delta: "he" }));
    expect(state.streaming?.text).toBe("he");
    // …though `started` opens a FRESH turn, so the stray text is dropped.
    applyNodeFrame(state, frame("started", { runId: "r1" }));
    applyNodeFrame(state, frame("text", { delta: "llo " }));
    applyNodeFrame(state, frame("thinking", { delta: "hmm" }));

    expect(state.streaming?.text).toBe("llo ");
    expect(state.streaming?.thinking).toBe("hmm");
  });

  it("tool_start/tool_end pair into chips by id; unpaired ends record themselves", () => {
    const state = hydrated();
    applyNodeFrame(state, frame("started", { runId: "r1" }));
    applyNodeFrame(state, frame("tool_start", { toolCallId: "t1", name: "shell" }));
    applyNodeFrame(state, frame("tool_start", { toolCallId: "t2", name: "edit" }));
    applyNodeFrame(state, frame("tool_end", { toolCallId: "t1", ok: true, output: "done" }));
    applyNodeFrame(state, frame("tool_end", { toolCallId: "t3", ok: false }));

    const calls = state.streaming!.toolCalls;
    expect(calls).toHaveLength(3);
    expect(calls[0]).toEqual({ toolCallId: "t1", name: "shell", status: "ok", output: "done" });
    expect(calls[1]).toEqual({ toolCallId: "t2", name: "edit", status: "running", output: null });
    expect(calls[2]).toEqual({ toolCallId: "t3", name: "tool", status: "error", output: null });
  });

  it("exited marks the turn ended (the persisted message lands a beat later)", () => {
    const state = hydrated();
    applyNodeFrame(state, frame("started", { runId: "r1" }));
    applyNodeFrame(state, frame("text", { delta: "the decision is X" }));
    applyNodeFrame(state, frame("exited", { runId: "r1", exitCode: 0 }));

    expect(state.streaming).not.toBeNull();
    expect(state.streaming?.ended).toBe(true);
    expect(state.streaming?.text).toBe("the decision is X");
  });

  it("exited with an empty turn clears it (the server persists nothing)", () => {
    const state = hydrated();
    applyNodeFrame(state, frame("started", { runId: "r1" }));
    applyNodeFrame(state, frame("tool_start", { toolCallId: "t1", name: "shell" }));
    applyNodeFrame(state, frame("exited", { runId: "r1", exitCode: 0 }));

    expect(state.streaming).toBeNull();
  });

  it("error frames surface the failure message", () => {
    const state = hydrated();
    applyNodeFrame(state, frame("error", { message: "harness exited 1" }));
    expect(state.error).toBe("harness exited 1");

    applyNodeFrame(state, frame("error", {}));
    expect(state.error).toBe("the agent reply failed");
  });

  it("a new started replaces a lingering ended turn (empty prior turn)", () => {
    const state = hydrated();
    applyNodeFrame(state, frame("started", { runId: "r1" }));
    applyNodeFrame(state, frame("text", { delta: "partial" }));
    applyNodeFrame(state, frame("exited", { runId: "r1" }));
    applyNodeFrame(state, frame("started", { runId: "r2" }));

    expect(state.streaming).toEqual({
      runId: "r2",
      text: "",
      thinking: "",
      toolCalls: [],
      ended: false,
    });
  });
});

describe("forward compatibility", () => {
  it("ignores unknown frame types and acks without corrupting state", () => {
    const state = hydrated();
    applyNodeFrame(state, frame("message", message()));
    const before = JSON.stringify(state);

    applyNodeFrame(state, frame("subscribed", {}));
    applyNodeFrame(state, frame("unsubscribed", {}));
    applyNodeFrame(state, frame("usage", { runId: "r1", totalTokens: 10 }));
    applyNodeFrame(state, frame("suggested_edits", { edits: [] }));
    applyNodeFrame(state, frame("brand_new_kind", { whatever: true }));

    expect(JSON.stringify(state)).toBe(before);
  });
});
