// Pure WS-event → view-state reducer for the planning chat (T-204).
//
// This module is deliberately framework-free and dependency-free (no Vue, no
// fetch) so it can be unit-tested without a browser. It folds the ordered stream
// of WebSocket frames published on `epic:<id>` (see CONVENTIONS.md §WebSocket)
// into a display model: a flat list of finalized transcript turns plus the one
// in-flight streaming agent turn (accumulated `text` deltas + inline tool-call
// markers), and applies `epic_updated` frames to the live Epic record.
//
// The state is mutated in place (and returned for convenience). Callers wrap the
// state object in Vue reactivity; the reducer itself never touches a framework.
//
// Convergence note: on `exited` we finalize the streaming turn into `turns` in
// the SAME order the server persists them (tool messages first, then the agent
// text), so a live-finalized transcript is identical to one re-fetched from
// `GET /epics/:id/transcript` after a reload. One wrinkle: the server persists
// each tool call as TWO `role: "tool"` messages (the serialized `toolStart`,
// then the `toolEnd`), so `hydrate` folds each pair by `toolCallId` into the
// single turn the live path produces.

import type { Epic, PlanningPhase, TranscriptMessage, TranscriptRole } from "../api/epics";

/** A single tool call the agent made during a run, rendered as a chip. */
export interface ToolCall {
  toolCallId: string;
  name: string;
  status: "running" | "ok" | "error";
  /** The tool's result, once `toolEnd` arrives (may be absent). */
  output: string | null;
}

/** A finalized turn in the transcript history (one per rendered bubble/chip). */
export interface Turn {
  /** Stable key for list rendering. */
  id: string;
  role: TranscriptRole;
  /** Plain text for `user`/`agent`/`system`; empty for a `tool` turn. */
  text: string;
  /** Present only on `tool` turns. */
  tool: ToolCall | null;
  /**
   * The planning phase this turn belongs to (T-205). Hydrated turns take it from
   * the persisted message; locally-finalized turns take the state's current
   * phase. Lets the view draw a divider where the transcript crosses product →
   * technical without splitting the one continuous list.
   */
  phase: PlanningPhase;
}

/** The in-flight agent turn while a run streams. `null` when no run is active. */
export interface StreamingTurn {
  runId: string | null;
  /** Accumulated `text` deltas — the streaming answer. */
  text: string;
  /** Accumulated `thinking` deltas — rendered distinctly (or hidden). */
  thinking: string;
  /** Tool calls started this turn, in arrival order. */
  toolCalls: ToolCall[];
}

/** The whole planning view model. */
export interface PlanningState {
  epic: Epic | null;
  /** Finalized history (hydrated from the transcript + finalized live turns). */
  turns: Turn[];
  /** The active run's streaming turn, or `null` when idle. */
  streaming: StreamingTurn | null;
  /** The last terminal run error (`error` frame), or `null`. */
  error: string | null;
  /** Monotonic counter for stable keys on locally-finalized turns. */
  nextKey: number;
  /**
   * The phase new local turns are stamped with (the composer's active phase).
   * Defaults to `product`; the view flips it to `technical` after advancing so a
   * streamed technical turn finalizes under the right phase. Hydrated turns keep
   * their own persisted phase regardless of this value.
   */
  phase: PlanningPhase;
}

// ---- WS frame payload shapes --------------------------------------------
//
// The frame `payload` is the serialized `RunEvent` verbatim (camelCase,
// `kind`-tagged). We type only the fields we consume; `epic_updated` carries the
// full Epic record instead.

interface TextPayload {
  runId: string;
  delta: string;
}
interface ToolStartPayload {
  runId: string;
  toolCallId: string;
  name: string;
}
interface ToolEndPayload {
  runId: string;
  toolCallId: string;
  ok: boolean;
  output?: string | null;
}
interface ErrorPayload {
  message: string;
}
interface SessionPayload {
  runId: string;
}

/** A WS frame as delivered on `epic:<id>`. `type` is the server's mapped name. */
export interface EpicFrame {
  topic: string;
  type: string;
  payload: unknown;
}

/** A fresh, empty view model. */
export function initialState(): PlanningState {
  return { epic: null, turns: [], streaming: null, error: null, nextKey: 0, phase: "product" };
}

/**
 * Seed the state from a REST hydrate: the current Epic record and its full
 * transcript (in `seq` order). Replaces any prior turns.
 */
export function hydrate(state: PlanningState, epic: Epic, messages: TranscriptMessage[]): void {
  state.epic = epic;
  state.turns = foldMessages(messages);
  state.streaming = null;
}

/**
 * Fold persisted messages into display turns. The server stores each tool call
 * as TWO `role: "tool"` messages — the serialized `toolStart`, then the
 * `toolEnd` — so pair them by `toolCallId`: one call renders as ONE chip with
 * its terminal status. A start left unpaired means the run died mid-tool (tool
 * events persist only after the run drains, so no end is coming); it degrades
 * to `error` rather than spinning "running" forever.
 */
function foldMessages(messages: TranscriptMessage[]): Turn[] {
  const turns: Turn[] = [];
  const open = new Map<string, Turn>();
  for (const m of messages) {
    if (m.role !== "tool") {
      turns.push(messageToTurn(m));
      continue;
    }
    const parsed = parseToolEvent(m.content);
    if (parsed.kind === "toolEnd") {
      const startTurn = open.get(parsed.call.toolCallId);
      if (startTurn?.tool) {
        startTurn.tool.status = parsed.call.status;
        startTurn.tool.output = parsed.call.output;
        open.delete(parsed.call.toolCallId);
        continue;
      }
    }
    const turn: Turn = { id: m.id, role: "tool", text: "", tool: parsed.call, phase: m.phase };
    if (parsed.kind === "toolStart" && parsed.call.toolCallId !== "") {
      open.set(parsed.call.toolCallId, turn);
    }
    turns.push(turn);
  }
  for (const turn of open.values()) {
    if (turn.tool) {
      turn.tool.status = "error";
    }
  }
  return turns;
}

/** Map a persisted non-tool transcript message to a display turn. */
function messageToTurn(message: TranscriptMessage): Turn {
  return {
    id: message.id,
    role: message.role,
    text: message.content,
    tool: null,
    phase: message.phase,
  };
}

/** A `role: "tool"` message parsed back into its event kind + display chip. */
interface ParsedToolEvent {
  kind: "toolStart" | "toolEnd" | "unknown";
  call: ToolCall;
}

/**
 * A `role: "tool"` message stores a serialized `RunEvent` (`toolStart`/
 * `toolEnd`). Recover the kind + display `ToolCall` from it; fall back to a
 * generic chip if it is not the expected shape.
 */
function parseToolEvent(content: string): ParsedToolEvent {
  try {
    const raw = JSON.parse(content) as Record<string, unknown>;
    const kind = typeof raw.kind === "string" ? raw.kind : "";
    const toolCallId = typeof raw.toolCallId === "string" ? raw.toolCallId : "";
    const name = typeof raw.name === "string" ? raw.name : "tool";
    if (kind === "toolEnd") {
      const ok = raw.ok === true;
      const output = typeof raw.output === "string" ? raw.output : null;
      return { kind: "toolEnd", call: { toolCallId, name, status: ok ? "ok" : "error", output } };
    }
    if (kind === "toolStart") {
      return { kind: "toolStart", call: { toolCallId, name, status: "running", output: null } };
    }
    return { kind: "unknown", call: { toolCallId, name, status: "ok", output: null } };
  } catch {
    return { kind: "unknown", call: { toolCallId: "", name: "tool", status: "ok", output: content } };
  }
}

/** Append an optimistic local `user` turn (the composer echoes it immediately). */
export function appendUserTurn(state: PlanningState, content: string): void {
  state.turns.push({
    id: nextKey(state, "user"),
    role: "user",
    text: content,
    tool: null,
    phase: state.phase,
  });
}

/**
 * Fold one WS frame into the state. Unknown frame `type`s (acks, `usage`,
 * `session`, forward-compat kinds) are ignored. Returns the same state object.
 */
export function applyFrame(state: PlanningState, frame: EpicFrame): PlanningState {
  switch (frame.type) {
    case "started": {
      // A new run began — start a fresh streaming turn and clear any prior error.
      const p = frame.payload as SessionPayload;
      state.error = null;
      state.streaming = { runId: p?.runId ?? null, text: "", thinking: "", toolCalls: [] };
      break;
    }
    case "session": {
      // A companion to `started` (which already opened the turn); only annotate
      // the run id, never begin a turn on its own.
      const p = frame.payload as SessionPayload;
      if (state.streaming !== null && state.streaming.runId === null && typeof p?.runId === "string") {
        state.streaming.runId = p.runId;
      }
      break;
    }
    case "text": {
      const p = frame.payload as TextPayload;
      ensureStreaming(state).text += p?.delta ?? "";
      break;
    }
    case "thinking": {
      const p = frame.payload as TextPayload;
      ensureStreaming(state).thinking += p?.delta ?? "";
      break;
    }
    case "tool_start": {
      const p = frame.payload as ToolStartPayload;
      const s = ensureStreaming(state);
      s.toolCalls.push({
        toolCallId: p?.toolCallId ?? "",
        name: p?.name ?? "tool",
        status: "running",
        output: null,
      });
      break;
    }
    case "tool_end": {
      const p = frame.payload as ToolEndPayload;
      const s = ensureStreaming(state);
      const call = s.toolCalls.find((c) => c.toolCallId === p?.toolCallId);
      if (call) {
        call.status = p?.ok ? "ok" : "error";
        call.output = p?.output ?? null;
      } else {
        // A `toolEnd` with no matching `toolStart` (shouldn't happen) — record it.
        s.toolCalls.push({
          toolCallId: p?.toolCallId ?? "",
          name: "tool",
          status: p?.ok ? "ok" : "error",
          output: p?.output ?? null,
        });
      }
      break;
    }
    case "error": {
      const p = frame.payload as ErrorPayload;
      state.error = p?.message ?? "the planning run failed";
      break;
    }
    case "exited": {
      // Terminal, exactly once — finalize the streaming turn into history.
      finalizeStreaming(state);
      break;
    }
    case "epic_updated": {
      // The agent called `update_epic`; payload is the full updated record.
      state.epic = frame.payload as Epic;
      break;
    }
    default:
      // acks (`subscribed`/`unsubscribed`), `usage`, `activity`, future kinds.
      break;
  }
  return state;
}

/** Ensure a streaming turn exists (a stray delta before `started` still lands). */
function ensureStreaming(state: PlanningState): StreamingTurn {
  if (state.streaming === null) {
    state.streaming = { runId: null, text: "", thinking: "", toolCalls: [] };
  }
  return state.streaming;
}

/**
 * Move the in-flight streaming turn into `turns` and clear it. Tool turns are
 * pushed first (in arrival order), then the agent text turn — matching the
 * server's persistence order so a reload renders identically.
 */
function finalizeStreaming(state: PlanningState): void {
  const s = state.streaming;
  if (s === null) {
    return;
  }
  for (const call of s.toolCalls) {
    state.turns.push({
      id: nextKey(state, "tool"),
      role: "tool",
      text: "",
      tool: { ...call },
      phase: state.phase,
    });
  }
  if (s.text.length > 0) {
    state.turns.push({
      id: nextKey(state, "agent"),
      role: "agent",
      text: s.text,
      tool: null,
      phase: state.phase,
    });
  }
  state.streaming = null;
}

/** A stable, unique key for a locally-finalized turn. */
function nextKey(state: PlanningState, tag: string): string {
  state.nextKey += 1;
  return `live-${tag}-${state.nextKey}`;
}
