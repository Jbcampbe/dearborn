// Unit tests for the living-Document stream reducer (`src/document/stream.ts`).
// No browser, no WS — fold hand-built frames and assert the resulting view
// model. Mirrors the map stream reducer tests (`map.test.ts`).

import { describe, expect, it } from "vitest";

import type { Comment } from "../src/api/comments";
import type { DocumentSection, DocumentView } from "../src/api/document";
import {
  applyDocumentFrame,
  commentsForSection,
  hydrateDocument,
  initialDocumentState,
  threadsForSection,
  type DocumentFrame,
  type DocumentStreamState,
} from "../src/document/stream";

const TOPIC = "epic:E1";

function frame(type: string, payload: unknown): DocumentFrame {
  return { topic: TOPIC, type, payload };
}

function section(overrides: Partial<DocumentSection> = {}): DocumentSection {
  return {
    epic_id: "E1",
    section_id: "decisions",
    title: "Decisions",
    provenance: "N1",
    last_edited_by: null,
    version: 1,
    ...overrides,
  };
}

function docView(overrides: Partial<DocumentView> = {}): DocumentView {
  return {
    epic_id: "E1",
    html: "<h1 id=\"decisions\">Decisions</h1><p>Use blobs.</p>",
    version: 1,
    last_edited_by: null,
    updated_at: 100,
    sections: [section()],
    ...overrides,
  };
}

function comment(overrides: Partial<Comment> = {}): Comment {
  return {
    id: "C1",
    epic_id: "E1",
    thread_id: "T1",
    anchor_kind: "section",
    anchor_id: "decisions",
    author_user_id: "U1",
    is_agent: false,
    body: "Why blobs?",
    resolved: false,
    promoted_node_id: null,
    created_at: 50,
    ...overrides,
  };
}

describe("hydrateDocument", () => {
  it("replaces the document wholesale and clears staleness", () => {
    const state = initialDocumentState();
    state.doc = docView({ version: 9, sections: [] });
    state.stale = true;

    hydrateDocument(state, "E1", docView());

    expect(state.epicId).toBe("E1");
    expect(state.doc?.version).toBe(1);
    expect(state.doc?.sections).toHaveLength(1);
    expect(state.stale).toBe(false);
  });
});

describe("applyDocumentFrame: document_updated", () => {
  it("stamps the version + section index and marks the state stale (the HTML must be re-read)", () => {
    const state = initialDocumentState();
    hydrateDocument(state, "E1", docView());

    applyDocumentFrame(
      state,
      frame("document_updated", {
        epic_id: "E1",
        version: 2,
        updated_at: 200,
        sections: [section({ section_id: "risks", title: "Risks", provenance: null, version: 2 })],
      }),
    );

    expect(state.stale).toBe(true);
    expect(state.doc?.version).toBe(2);
    expect(state.doc?.updated_at).toBe(200);
    expect(state.doc?.sections.map((s) => s.section_id)).toEqual(["risks"]);
    // The frame never carries the HTML — the view's REST re-read heals it.
    expect(state.doc?.html).toContain("Decisions");
  });

  it("ignores a frame whose version is not newer", () => {
    const state = initialDocumentState();
    hydrateDocument(state, "E1", docView({ version: 3 }));

    applyDocumentFrame(state, frame("document_updated", { epic_id: "E1", version: 3, sections: [] }));

    expect(state.doc?.version).toBe(3);
  });

  it("marks stale defensively when the frame carries no version", () => {
    const state = initialDocumentState();
    hydrateDocument(state, "E1", docView());

    applyDocumentFrame(state, frame("document_updated", { epic_id: "E1" }));

    expect(state.stale).toBe(true);
  });

  it("stamps nothing when no document is loaded yet (but still marks stale)", () => {
    const state = initialDocumentState();

    applyDocumentFrame(
      state,
      frame("document_updated", { epic_id: "E1", version: 1, sections: [section()] }),
    );

    expect(state.doc).toBeNull();
    expect(state.stale).toBe(true);
  });
});

describe("applyDocumentFrame: comments_updated", () => {
  it("replaces the comment list wholesale (the frame carries the epic's full list)", () => {
    const state = initialDocumentState();
    state.comments = [comment({ id: "C0" })];

    applyDocumentFrame(
      state,
      frame("comments_updated", [comment(), comment({ id: "C2", is_agent: true, author_user_id: null })]),
    );

    expect(state.comments.map((c) => c.id)).toEqual(["C1", "C2"]);
  });

  it("ignores non-array payloads", () => {
    const state = initialDocumentState();
    state.comments = [comment()];

    applyDocumentFrame(state, frame("comments_updated", null));
    applyDocumentFrame(state, frame("comments_updated", { nope: true }));

    expect(state.comments).toHaveLength(1);
  });
});

describe("applyDocumentFrame: sibling-view frames", () => {
  it("ignores other kinds on the shared epic:<id> topic", () => {
    const state = initialDocumentState();
    hydrateDocument(state, "E1", docView());

    applyDocumentFrame(state, frame("subscribed", {}));
    applyDocumentFrame(state, frame("map_updated", {}));
    applyDocumentFrame(state, frame("epic_updated", {}));
    applyDocumentFrame(state, frame("document_updated", undefined));

    expect(state.doc?.version).toBe(1);
  });
});

describe("section helpers", () => {
  it("filters comments to section anchors of one section", () => {
    const state: DocumentStreamState = initialDocumentState();
    state.comments = [
      comment({ id: "C1" }),
      comment({ id: "C2", anchor_id: "risks" }),
      comment({ id: "C3", anchor_kind: "node", anchor_id: "N9" }),
    ];

    expect(commentsForSection(state, "decisions").map((c) => c.id)).toEqual(["C1"]);
  });

  it("groups threads, oldest first, resolved last", () => {
    const state = initialDocumentState();
    state.comments = [
      comment({ id: "A", thread_id: "T2", created_at: 2 }),
      comment({ id: "B", thread_id: "T1", created_at: 1 }),
      comment({ id: "C", thread_id: "T3", created_at: 0, resolved: true }),
      comment({ id: "D", thread_id: "T3", created_at: 1 }),
    ];

    const threads = threadsForSection(state, "decisions");
    expect(threads.map((t) => t.threadId)).toEqual(["T1", "T2", "T3"]);
    expect(threads.map((t) => t.resolved)).toEqual([false, false, true]);
    expect(threads[2].comments.map((c) => c.id)).toEqual(["C", "D"]);
  });
});
