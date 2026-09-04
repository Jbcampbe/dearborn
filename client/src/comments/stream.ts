// Pure WS-event → view-state reducer for the shared comment panel (wayfinder
// epic §9, client slice).
//
// Framework-free and dependency-free (no Vue, no fetch) so it can be
// unit-tested without a browser — the same shape as `map/stream.ts`, which
// this module deliberately mirrors: fold the ordered stream of WebSocket
// frames published on `epic:<id>` into the view model. The server publishes a
// `comments_updated` frame on EVERY comment mutation (post / resolve /
// promote / an agent's reply) carrying the epic's FULL comment list, so the
// fold is a whole-list replace — no ordering logic lives here.
//
// The state is mutated in place (and returned for convenience). Callers wrap
// it in Vue reactivity; the reducer never touches a framework.

import type { Comment } from "../api/comments";

/** A WS frame as delivered on `epic:<id>` (same envelope as the map stream). */
export interface CommentFrame {
  topic: string;
  type: string;
  payload: unknown;
}

/** The comment panel's view model. */
export interface CommentState {
  /** The epic this state is bound to (set on hydrate; unchanged after). */
  epicId: string | null;
  /** The epic's comments, oldest first (a `comments_updated` payload / REST load). */
  comments: Comment[];
}

/** A fresh, empty view model. */
export function initialCommentState(): CommentState {
  return { epicId: null, comments: [] };
}

/**
 * Hydrate the state from a REST load (`GET /epics/{id}/comments`). Replaces
 * any prior comments and stamps the bound epic id.
 */
export function hydrateComments(
  state: CommentState,
  epicId: string,
  comments: Comment[],
): CommentState {
  state.epicId = epicId;
  state.comments = comments;
  return state;
}

/**
 * Fold one WS frame into the state. `comments_updated` replaces the whole
 * comment list from the payload (guarded to the bound epic — a payload that
 * isn't an array is ignored rather than corrupting the model). Other frame
 * types on the shared `epic:<id>` topic (map/document/epic updates, …) are
 * ignored — the panel only cares about comments. Returns the state.
 */
export function applyCommentFrame(state: CommentState, frame: CommentFrame): CommentState {
  if (frame.type !== "comments_updated") {
    return state;
  }
  if (Array.isArray(frame.payload)) {
    state.comments = frame.payload as Comment[];
  }
  return state;
}

/**
 * Fold a promote outcome's stamped thread into the state: replaces the
 * thread's previous members with the refreshed ones (every comment now
 * carries the same `promoted_node_id`), keeping the thread at its prior
 * position in the list so other threads' relative order is untouched.
 * The `comments_updated` frame delivers the same list; this heals the model
 * when the socket is down.
 */
export function applyPromotedThread(state: CommentState, thread: Comment[]): CommentState {
  if (thread.length === 0) {
    return state;
  }
  const threadId = thread[0].thread_id;
  const priorIndex = state.comments.findIndex((c) => c.thread_id === threadId);
  const kept = state.comments.filter((c) => c.thread_id !== threadId);
  const at = priorIndex === -1 ? kept.length : Math.min(priorIndex, kept.length);
  kept.splice(at, 0, ...thread);
  state.comments = kept;
  return state;
}

/** A thread: the head comment that minted `thread_id` plus every reply. */
export interface CommentThread {
  threadId: string;
  /** The head comment's anchor — replies inherit it. */
  anchorKind: string;
  anchorId: string;
  /** Every comment in posting order (head first). */
  comments: Comment[];
  /** Thread-wide: resolving any comment resolves the conversation. */
  resolved: boolean;
  /** Set across the thread once it has been promoted to a frontier node. */
  promotedNodeId: string | null;
  /** The newest member's timestamp (drives the panel's ordering). */
  lastActivityAt: number;
}

/**
 * Group a flat comment list into threads, most-recently-active first. Within
 * a thread the comments stay in posting order (head comment first).
 */
export function groupThreads(comments: Comment[]): CommentThread[] {
  const byThread = new Map<string, Comment[]>();
  for (const c of comments) {
    const list = byThread.get(c.thread_id);
    if (list === undefined) {
      byThread.set(c.thread_id, [c]);
    } else {
      list.push(c);
    }
  }
  const threads: CommentThread[] = [];
  for (const [threadId, members] of byThread) {
    const ordered = [...members].sort((a, b) => a.created_at - b.created_at);
    const head = ordered[0];
    threads.push({
      threadId,
      anchorKind: head.anchor_kind,
      anchorId: head.anchor_id,
      comments: ordered,
      resolved: ordered.some((c) => c.resolved),
      promotedNodeId:
        ordered.find((c) => c.promoted_node_id !== null)?.promoted_node_id ?? null,
      lastActivityAt: ordered[ordered.length - 1].created_at,
    });
  }
  return threads.sort((a, b) => b.lastActivityAt - a.lastActivityAt);
}
