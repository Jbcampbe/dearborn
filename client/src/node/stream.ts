// Pure WS-event → view-state reducer for a node's multi-party session view.
//
// Framework-free and dependency-free (no Vue, no fetch) so it can be
// unit-tested without a browser — the same shape as `map/stream.ts` and the
// old planning-chat reducer it was refactored from: fold the ordered stream
// of WebSocket frames published on `node:<id>` (see CONVENTIONS.md
// §WebSocket) into a display model — a flat list of persisted transcript
// messages (`node_message`) plus the one in-flight streaming agent turn
// (accumulated `text` deltas + inline tool-call chips).
//
// Convergence contract with the server (dearborn-server/src/node_engine.rs):
// a reply's `RunEvent`s relay live, and only AFTER the run drains does the
// server persist the assembled agent turn as one `message` frame. So the
// reducer does NOT finalize on `exited` — it marks the streaming turn `ended`
// and keeps it on screen until the persisted `message` lands, then swaps.
// (The server persists an empty turn as nothing at all, so an `exited` with
// no text clears the turn outright.) A reconnect re-hydrates from REST
// (`useNodeStream` invokes the view's resync on every re-subscribe), which
// heals any frames missed while offline.
//
// The state is mutated in place (and returned for convenience). Callers wrap
// it in Vue reactivity; the reducer never touches a framework.

import type { NodeMessage, NodeSession, NodeSessionView } from "../api/nodes";

/** A single tool call the agent made during a run, rendered as a chip. */
export interface ToolCall {
  toolCallId: string;
  name: string;
  status: "running" | "ok" | "error";
  /** The tool's result, once `tool_end` arrives (may be absent). */
  output: string | null;
}

/** The in-flight agent turn while a reply streams. `null` when no run is active. */
export interface StreamingTurn {
  runId: string | null;
  /** Accumulated `text` deltas — the streaming answer. */
  text: string;
  /** Accumulated `thinking` deltas (not rendered; kept for parity/debugging). */
  thinking: string;
  /** Tool calls started this turn, in arrival order. */
  toolCalls: ToolCall[];
  /**
   * Set on `exited`: the run is over but the assembled turn is not persisted
   * yet — the `message` frame will land a beat later and finalize.
   */
  ended: boolean;
}

/** The node session view model. */
export interface NodeStreamState {
  /** The node this state is bound to (set on hydrate; frames are guarded on it). */
  nodeId: string | null;
  /** The node's resume handle (live-updated when a reply captures a session id). */
  session: NodeSession | null;
  /** Persisted transcript, in `seq` order (deduped by id). */
  messages: NodeMessage[];
  /** The active run's streaming turn, or `null` when idle. */
  streaming: StreamingTurn | null;
  /** The last terminal run error (`error` frame), or `null`. */
  error: string | null;
  /** Monotonic counter for stable keys on locally-created turns. */
  nextKey: number;
}

/** A WS frame as delivered on `node:<id>` (same envelope as every topic). */
export interface NodeFrame {
  topic: string;
  type: string;
  payload: unknown;
}

/** A fresh, empty view model. */
export function initialNodeState(): NodeStreamState {
  return { nodeId: null, session: null, messages: [], streaming: null, error: null, nextKey: 0 };
}

/**
 * Hydrate the state from a REST load (the `session` endpoint's view).
 * Replaces any prior transcript and clears any streaming turn — the REST view
 * is the source of truth (a reconnect's resync heals any gap).
 */
export function hydrateNode(state: NodeStreamState, nodeId: string, view: NodeSessionView): void {
  state.nodeId = nodeId;
  state.session = sessionOf(view);
  state.messages = view.messages.slice();
  state.streaming = null;
}

/** Replace just the resume handle (e.g. after a resolve marks the session complete). */
export function setSession(state: NodeStreamState, session: NodeSession): void {
  state.session = session;
}

/**
 * Append one persisted message, deduped by id and kept in `(seq, id)` order —
 * the POST response and its own WS fan-out can arrive in either order, and
 * another participant's turn may interleave. An `agent` message is the
 * persisted form of the streaming turn: clear it on arrival.
 */
export function appendMessage(state: NodeStreamState, message: NodeMessage): void {
  if (typeof message?.id !== "string" || message.id === "") {
    return;
  }
  if (state.messages.some((m) => m.id === message.id)) {
    return;
  }
  const key = seqKey(message);
  let at = state.messages.length;
  while (at > 0 && seqKey(state.messages[at - 1]) > key) {
    at -= 1;
  }
  state.messages.splice(at, 0, message);
  if (message.role === "agent") {
    state.streaming = null;
    state.error = null;
  }
}

function seqKey(message: NodeMessage): string {
  return `${String(message.seq ?? 0).padStart(16, "0")}:${message.id}`;
}

// ---- WS frame payload shapes ------------------------------------------------
//
// The frame `payload` is the serialized `RunEvent` verbatim (camelCase,
// `kind`-tagged). We type only the fields we consume; `message` frames carry a
// full `NodeMessage`.

interface RunIdPayload {
  runId?: string;
}
interface TextPayload {
  delta?: string;
}
interface ToolStartPayload {
  toolCallId?: string;
  name?: string;
}
interface ToolEndPayload {
  toolCallId?: string;
  ok?: boolean;
  output?: string | null;
}
interface ErrorPayload {
  message?: string;
}

/**
 * Fold one WS frame into the state. `message` appends a persisted turn (and
 * finalizes a streaming agent turn); the run-event frames accumulate the
 * in-flight turn. Unknown frame `type`s (acks, `usage`, `suggested_edits`,
 * `activity`, `ask_question`, forward-compat kinds) are ignored. Returns the
 * same state object.
 */
export function applyNodeFrame(state: NodeStreamState, frame: NodeFrame): NodeStreamState {
  switch (frame.type) {
    case "message": {
      const m = frame.payload as NodeMessage | null;
      // Guard to the bound node (the WS is a trust boundary) before folding.
      if (m && typeof m.id === "string" && (state.nodeId === null || m.node_id === state.nodeId)) {
        appendMessage(state, m);
      }
      break;
    }
    case "started": {
      // A new run began — start a fresh streaming turn and clear any prior error.
      state.error = null;
      state.streaming = {
        runId: (frame.payload as RunIdPayload | null)?.runId ?? null,
        text: "",
        thinking: "",
        toolCalls: [],
        ended: false,
      };
      break;
    }
    case "session": {
      // A companion to `started` (which already opened the turn); only annotate
      // the run id, never begin a turn on its own.
      const p = frame.payload as RunIdPayload | null;
      if (state.streaming !== null && state.streaming.runId === null && typeof p?.runId === "string") {
        state.streaming.runId = p.runId;
      }
      break;
    }
    case "text": {
      ensureStreaming(state).text += (frame.payload as TextPayload | null)?.delta ?? "";
      break;
    }
    case "thinking": {
      ensureStreaming(state).thinking += (frame.payload as TextPayload | null)?.delta ?? "";
      break;
    }
    case "tool_start": {
      const p = frame.payload as ToolStartPayload | null;
      ensureStreaming(state).toolCalls.push({
        toolCallId: p?.toolCallId ?? "",
        name: p?.name ?? "tool",
        status: "running",
        output: null,
      });
      break;
    }
    case "tool_end": {
      const p = frame.payload as ToolEndPayload | null;
      const s = ensureStreaming(state);
      const call = s.toolCalls.find((c) => c.toolCallId === p?.toolCallId);
      if (call) {
        call.status = p?.ok ? "ok" : "error";
        call.output = p?.output ?? null;
      } else {
        // A `tool_end` with no matching `tool_start` (shouldn't happen) — record it.
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
      state.error = (frame.payload as ErrorPayload | null)?.message ?? "the agent reply failed";
      break;
    }
    case "exited": {
      // Terminal, exactly once. The server persists the assembled turn (when
      // it has text) and fans it out as `message` a beat later — mark the turn
      // `ended` and wait. An empty turn persists as nothing, so clear it now.
      const s = state.streaming;
      if (s !== null) {
        if (s.text.trim().length === 0) {
          state.streaming = null;
        } else {
          s.ended = true;
        }
      }
      break;
    }
    default:
      // acks (`subscribed`/`unsubscribed`), `usage`, `thinking`, future kinds.
      break;
  }
  return state;
}

/** Ensure a streaming turn exists (a stray delta before `started` still lands). */
function ensureStreaming(state: NodeStreamState): StreamingTurn {
  if (state.streaming === null) {
    state.streaming = { runId: null, text: "", thinking: "", toolCalls: [], ended: false };
  }
  return state.streaming;
}

/** The session slice of a session endpoint's response. */
function sessionOf(view: NodeSessionView): NodeSession {
  return {
    node_id: view.node_id,
    harness_session_id: view.harness_session_id,
    status: view.status,
    created_at: view.created_at,
    updated_at: view.updated_at,
  };
}
