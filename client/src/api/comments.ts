// Threaded, anchored comments REST surface (wayfinder epic §4.8/§9, Phase 5).
// Mirrors `document.ts`/`map.ts`: typed DTOs matching the server's shapes
// (see `dearborn-server/src/comments.rs`) wrapped around the generic
// `apiFetch`.
//
// A comment hangs off an anchor — a map node or a living-Document section —
// and lives in a thread (`thread_id`): the first post chooses the anchor,
// every reply joins by thread id. The Document view anchors its comments to
// `section` anchors (anchor_id = the `document_section.section_id`); the node
// session / map surfaces own the `node` anchor. Every mutation publishes a
// `comments_updated` frame on `epic:<id>` carrying the epic's full comment
// list, so a subscribed view re-renders without polling.

import { apiFetch, type Collection } from "./client";

/** What a comment is anchored to (the server's `VALID_ANCHOR_KINDS`). */
export type CommentAnchorKind = "node" | "section";

/** A comment as stored (`comments.rs` `Comment`). */
export interface Comment {
  id: string;
  epic_id: string;
  /** Every comment of a conversation shares this; the head post minted it. */
  thread_id: string;
  anchor_kind: CommentAnchorKind | string;
  anchor_id: string;
  /** Null (and `is_agent` true) when the author was an agent run. */
  author_user_id: string | null;
  is_agent: boolean;
  body: string;
  /** Thread-wide: resolving any comment resolves the conversation. */
  resolved: boolean;
  /** Set across a thread once it has been promoted to a frontier node. */
  promoted_node_id: string | null;
  created_at: number;
}

/** Optional `GET /epics/{id}/comments` filters. Every supplied one must match. */
export interface CommentFilter {
  anchor_kind?: CommentAnchorKind;
  anchor_id?: string;
  thread_id?: string;
}

/** `POST /epics/{id}/comments` body — start a thread or reply into one. */
export interface PostCommentInput {
  /** Required to start a thread; a reply inherits the thread's anchor. */
  anchor_kind?: CommentAnchorKind;
  anchor_id?: string;
  /** Present → the comment joins that thread (anchor fields optional). */
  thread_id?: string;
  body: string;
}

/**
 * `GET /epics/{id}/comments` → the epic's comments, oldest first, optionally
 * narrowed by anchor or thread. 404 if the epic is gone.
 */
export async function listComments(
  token: string,
  epicId: string,
  filter: CommentFilter = {},
): Promise<Comment[]> {
  const params = new URLSearchParams();
  if (filter.anchor_kind !== undefined) {
    params.set("anchor_kind", filter.anchor_kind);
  }
  if (filter.anchor_id !== undefined) {
    params.set("anchor_id", filter.anchor_id);
  }
  if (filter.thread_id !== undefined) {
    params.set("thread_id", filter.thread_id);
  }
  const query = params.toString();
  const page = await apiFetch<Collection<Comment>>(
    `/epics/${encodeURIComponent(epicId)}/comments${query === "" ? "" : `?${query}`}`,
    token,
  );
  return page.items;
}

/**
 * `POST /epics/{id}/comments` → `201` with the persisted comment. Starts a
 * new thread under the given anchor, or joins `thread_id` when present.
 * Publishes `comments_updated` on `epic:<id>`.
 */
export function postComment(
  token: string,
  epicId: string,
  input: PostCommentInput,
): Promise<Comment> {
  return apiFetch<Comment>(`/epics/${encodeURIComponent(epicId)}/comments`, token, {
    method: "POST",
    body: JSON.stringify(input),
  });
}

/**
 * `POST /epics/{id}/comments/{commentId}/resolve` → the resolved thread (the
 * flag is thread-wide; any member's id works). Publishes `comments_updated`.
 */
export async function resolveComment(
  token: string,
  epicId: string,
  commentId: string,
): Promise<Comment[]> {
  const page = await apiFetch<Collection<Comment>>(
    `/epics/${encodeURIComponent(epicId)}/comments/${encodeURIComponent(commentId)}/resolve`,
    token,
    { method: "POST" },
  );
  return page.items;
}
