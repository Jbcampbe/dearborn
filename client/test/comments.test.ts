// Unit tests for the shared comment panel's stream reducer + thread grouping
// (wayfinder epic §9). No browser, no WS — fold hand-built frames and assert
// the resulting view model. Mirrors the map/document reducer tests
// (`map.test.ts` / `document.test.ts`).

import { describe, expect, it } from "vitest";

import type { Comment } from "../src/api/comments";
import {
  applyCommentFrame,
  applyPromotedThread,
  groupThreads,
  hydrateComments,
  initialCommentState,
  type CommentFrame,
  type CommentState,
} from "../src/comments/stream";

const TOPIC = "epic:E1";

function frame(type: string, payload: unknown): CommentFrame {
  return { topic: TOPIC, type, payload };
}

function comment(overrides: Partial<Comment> = {}): Comment {
  return {
    id: "C1",
    epic_id: "E1",
    thread_id: "T1",
    anchor_kind: "node",
    anchor_id: "N1",
    author_user_id: "U1",
    is_agent: false,
    body: "Have we considered X?",
    resolved: false,
    promoted_node_id: null,
    created_at: 100,
    ...overrides,
  };
}

describe("hydrateComments", () => {
  it("replaces the list and stamps the bound epic", () => {
    const state: CommentState = initialCommentState();
    hydrateComments(state, "E1", [comment()]);
    expect(state.epicId).toBe("E1");
    expect(state.comments).toHaveLength(1);
  });
});

describe("applyCommentFrame", () => {
  it("replaces the whole list from a comments_updated payload", () => {
    const state = hydrateComments(initialCommentState(), "E1", [comment({ id: "C0" })]);
    applyCommentFrame(state, frame("comments_updated", [comment(), comment({ id: "C2", is_agent: true, author_user_id: null })]));
    expect(state.comments.map((c) => c.id)).toEqual(["C1", "C2"]);
  });

  it("ignores other frames on the shared epic:<id> topic", () => {
    const state = hydrateComments(initialCommentState(), "E1", [comment()]);
    applyCommentFrame(state, frame("map_updated", { nodes: [] }));
    applyCommentFrame(state, frame("document_updated", { version: 3 }));
    expect(state.comments).toHaveLength(1);
  });

  it("ignores a malformed (non-array) payload rather than corrupting the model", () => {
    const state = hydrateComments(initialCommentState(), "E1", [comment()]);
    applyCommentFrame(state, frame("comments_updated", null));
    applyCommentFrame(state, frame("comments_updated", { nope: true }));
    expect(state.comments).toHaveLength(1);
  });
});

describe("applyPromotedThread", () => {
  it("drops the thread's prior members and appends the stamped ones", () => {
    const state = hydrateComments(initialCommentState(), "E1", [
      comment({ id: "C1" }),
      comment({ id: "C2", thread_id: "T1", body: "Yes — and Y.", created_at: 200 }),
    ]);
    applyPromotedThread(state, [
      comment({ id: "C1", promoted_node_id: "N9", resolved: false }),
      comment({ id: "C2", thread_id: "T1", body: "Yes — and Y.", created_at: 200, promoted_node_id: "N9" }),
    ]);
    expect(state.comments.map((c) => c.id)).toEqual(["C1", "C2"]);
    expect(state.comments.every((c) => c.promoted_node_id === "N9")).toBe(true);
  });

  it("leaves other threads untouched", () => {
    const state = hydrateComments(initialCommentState(), "E1", [
      comment({ id: "C1", thread_id: "T1" }),
      comment({ id: "C3", thread_id: "T2", anchor_kind: "section", anchor_id: "S1", body: "Section note.", created_at: 300 }),
    ]);
    applyPromotedThread(state, [comment({ id: "C1", thread_id: "T1", promoted_node_id: "N9" })]);
    expect(state.comments.map((c) => c.id)).toEqual(["C1", "C3"]);
    expect(state.comments[1].promoted_node_id).toBeNull();
  });

  it("ignores an empty thread", () => {
    const state = hydrateComments(initialCommentState(), "E1", [comment()]);
    applyPromotedThread(state, []);
    expect(state.comments).toHaveLength(1);
  });
});

describe("groupThreads", () => {
  it("groups by thread_id with the head comment first", () => {
    const threads = groupThreads([
      comment({ id: "C2", thread_id: "T1", body: "Reply.", created_at: 200 }),
      comment({ id: "C1", thread_id: "T1", body: "Head.", created_at: 100 }),
    ]);
    expect(threads).toHaveLength(1);
    expect(threads[0].threadId).toBe("T1");
    expect(threads[0].comments.map((c) => c.id)).toEqual(["C1", "C2"]);
    expect(threads[0].anchorKind).toBe("node");
    expect(threads[0].anchorId).toBe("N1");
  });

  it("orders threads by most recent activity", () => {
    const threads = groupThreads([
      comment({ id: "C1", thread_id: "T1", created_at: 100 }),
      comment({ id: "C2", thread_id: "T2", body: "Newer thread.", created_at: 500 }),
    ]);
    expect(threads.map((t) => t.threadId)).toEqual(["T2", "T1"]);
    expect(threads[0].lastActivityAt).toBe(500);
  });

  it("resolving any member resolves the whole thread", () => {
    const threads = groupThreads([
      comment({ id: "C1", thread_id: "T1", resolved: false }),
      comment({ id: "C2", thread_id: "T1", body: "Reply.", created_at: 200, resolved: true }),
    ]);
    expect(threads[0].resolved).toBe(true);
  });

  it("carries the thread's promoted node id from any member", () => {
    const threads = groupThreads([
      comment({ id: "C1", thread_id: "T1", promoted_node_id: null }),
      comment({ id: "C2", thread_id: "T1", body: "Reply.", created_at: 200, promoted_node_id: "N9" }),
    ]);
    expect(threads[0].promotedNodeId).toBe("N9");
  });

  it("keeps section-anchored threads distinct from node-anchored ones", () => {
    const threads = groupThreads([
      comment({ id: "C1", thread_id: "T1", anchor_kind: "node", anchor_id: "N1" }),
      comment({ id: "C2", thread_id: "T2", anchor_kind: "section", anchor_id: "S1", body: "§", created_at: 200 }),
    ]);
    expect(threads).toHaveLength(2);
    expect(threads.map((t) => t.anchorKind)).toEqual(["section", "node"]);
  });

  it("returns an empty list for an empty epic", () => {
    expect(groupThreads([])).toEqual([]);
  });
});
