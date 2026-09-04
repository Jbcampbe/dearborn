// Pure WS-event → view-state reducer for the living-Document view.
//
// Framework-free and dependency-free (no Vue, no fetch) so it can be
// unit-tested without a browser — the same shape as `map/stream.ts` and
// `node/stream.ts`. It folds the ordered stream of WebSocket frames published
// on `epic:<id>` (see dearborn-server's hub) into a display model.
//
// Convergence contract with the server (dearborn-server/src/document.rs): a
// `document_updated` frame carries the new version, timestamp, and the
// section index — deliberately NOT the HTML (a re-rendering client re-reads
// the document). So the reducer marks the state `stale` and stamps what the
// frame carries; the view's composable invokes a reload callback (REST
// re-hydrate) on the same frame, which heals the HTML and clears the flag.
// `comments_updated` carries the epic's FULL comment list, so a replace is
// always correct — no merging, no dedupe.

import type { Comment } from "../api/comments";
import type { DocumentSection, DocumentView } from "../api/document";

/** The Document view model the reducer folds frames into. */
export interface DocumentStreamState {
  /** The epic this state is bound to (set on hydrate; frames are guarded). */
  epicId: string | null;
  /** The last REST-hydrated document view (null until the first load). */
  doc: DocumentView | null;
  /** The epic's comments, oldest first (the `comments_updated` payload). */
  comments: Comment[];
  /**
   * Set by `document_updated`: the server has a newer version than
   * `doc.version`. The view re-fetches the document (the frame never carries
   * the HTML) and clears the flag on hydrate.
   */
  stale: boolean;
}

/** A WS frame as delivered on `epic:<id>` (same envelope as every topic). */
export interface DocumentFrame {
  topic: string;
  type: string;
  payload: unknown;
}

/** The `document_updated` payload (document.rs `publish_document_updated`). */
export interface DocumentUpdatedPayload {
  epic_id?: string;
  version?: number;
  updated_at?: number | null;
  sections?: DocumentSection[];
}

/** A fresh, empty view model. */
export function initialDocumentState(): DocumentStreamState {
  return { epicId: null, doc: null, comments: [], stale: false };
}

/**
 * Hydrate the state from a REST load. Replaces the document wholesale and
 * clears `stale` — the REST view is the source of truth (a reconnect's
 * re-hydrate heals anything missed while offline). Comments are left alone:
 * they only arrive via `comments_updated` (or the view's own REST load).
 */
export function hydrateDocument(
  state: DocumentStreamState,
  epicId: string,
  doc: DocumentView,
): void {
  state.epicId = epicId;
  state.doc = doc;
  state.stale = false;
}

/** Replace the comment list (a `comments_updated` payload or a REST load). */
export function setComments(state: DocumentStreamState, comments: Comment[]): void {
  state.comments = Array.isArray(comments) ? comments.slice() : [];
}

/**
 * Fold one WS frame into the state.
 *
 * `document_updated` stamps version/sections/updated_at from the payload and
 * marks the state `stale` (the HTML must be re-read); `comments_updated`
 * replaces the comment list. Unknown frame `type`s (acks, `map_updated`,
 * `epic_updated`, future kinds) are ignored — sibling views subscribe to the
 * same `epic:<id>` topic. Returns the same state object.
 */
export function applyDocumentFrame(state: DocumentStreamState, frame: DocumentFrame): DocumentStreamState {
  switch (frame.type) {
    case "document_updated": {
      const p = frame.payload as DocumentUpdatedPayload | null;
      if (p === null || typeof p !== "object") {
        break;
      }
      if (state.doc !== null) {
        // Stamp the index live so TOC + provenance chips update immediately,
        // ahead of the HTML re-read the view performs on the same frame.
        if (Array.isArray(p.sections)) {
          state.doc.sections = p.sections;
        }
        if (typeof p.updated_at === "number") {
          state.doc.updated_at = p.updated_at;
        }
      }
      if (typeof p.version === "number") {
        if (state.doc === null || p.version > state.doc.version) {
          state.stale = true;
          if (state.doc !== null) {
            state.doc.version = p.version;
          }
        }
      } else {
        // No version in the frame — treat as stale defensively; the re-read
        // reconciles whatever the truth is.
        state.stale = true;
      }
      break;
    }
    case "comments_updated": {
      const items = frame.payload as Comment[] | null;
      if (Array.isArray(items)) {
        setComments(state, items);
      }
      break;
    }
    default:
      // acks (`subscribed`/`unsubscribed`) and sibling-view kinds.
      break;
  }
  return state;
}

// ---- view helpers (pure — the template renders these directly) -------------

/** The comments of one section, oldest first (the API's order is stable). */
export function commentsForSection(state: DocumentStreamState, sectionId: string): Comment[] {
  return state.comments.filter((c) => c.anchor_kind === "section" && c.anchor_id === sectionId);
}

/**
 * The section's comment threads: open threads first (oldest head first),
 * resolved threads last. A thread is the comments sharing a `thread_id`.
 */
export function threadsForSection(
  state: DocumentStreamState,
  sectionId: string,
): { threadId: string; comments: Comment[]; resolved: boolean }[] {
  const byThread = new Map<string, Comment[]>();
  for (const comment of commentsForSection(state, sectionId)) {
    const thread = byThread.get(comment.thread_id);
    if (thread === undefined) {
      byThread.set(comment.thread_id, [comment]);
    } else {
      thread.push(comment);
    }
  }
  const threads = [...byThread.entries()].map(([threadId, comments]) => ({
    threadId,
    comments,
    resolved: comments.some((c) => c.resolved),
  }));
  // Open threads first, oldest head first; resolved threads sink below.
  threads.sort((a, b) => {
    if (a.resolved !== b.resolved) {
      return Number(a.resolved) - Number(b.resolved);
    }
    return (a.comments[0]?.created_at ?? 0) - (b.comments[0]?.created_at ?? 0);
  });
  return threads;
}
