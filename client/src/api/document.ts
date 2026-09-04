// Living-Document REST surface (wayfinder epic §4.5/§10, Phase 3) consumed by
// the Document view. Mirrors `map.ts`: typed DTOs matching the server's
// shapes (see `dearborn-server/src/document.rs`) wrapped around the generic
// `apiFetch`.
//
// Only the read lives here (`GET /epics/{id}/document`): the write
// (`POST /epics/{id}/document/sync`) is the agent CLI's `document sync` verb,
// so the browser never syncs — it re-reads. Every sync publishes a
// `document_updated` frame on `epic:<id>` carrying the new version + section
// index (not the HTML), which the view folds through `src/document/stream.ts`
// and then re-hydrates via this endpoint.

import { apiFetch } from "./client";

/**
 * One entry in the section anchor/provenance index (`document_section`,
 * §4.6): an `id=` attribute in the document's HTML, the heading text it
 * carries (when the anchor sits on an `h1`–`h6`), and the map node that last
 * wrote it (`provenance`, null when only humans ever edited the section).
 */
export interface DocumentSection {
  epic_id: string;
  /** Matches an `id=` attribute in the document's HTML. */
  section_id: string;
  /** The anchor's heading text, when it sits on an `h1`–`h6`. */
  title: string | null;
  /** The node id that last wrote this section (null when a human edited it). */
  provenance: string | null;
  /** Which human last edited it (null when the editor was an agent run). */
  last_edited_by: string | null;
  /** The version at which this entry was last (re-)indexed. */
  version: number | null;
}

/**
 * The epic's living document (`GET /epics/{id}/document` → `document.rs`
 * `DocumentView`): the current HTML blob (null until the first sync), its
 * `vNN` version, and the section index — one response re-renders the whole
 * view. Also the shape a document sync returns, so a future editor panel can
 * reuse the type.
 */
export interface DocumentView {
  epic_id: string;
  /** The document's HTML; null before the first sync (`version: 0`). */
  html: string | null;
  /** Monotonic lineage number; 0 before the first sync. */
  version: number;
  last_edited_by: string | null;
  updated_at: number | null;
  /** The section anchor/provenance index, in document order. */
  sections: DocumentSection[];
}

/**
 * `GET /epics/{id}/document` → the epic's document + section index. 404 if
 * the epic is gone. No sanitization on either side (v1) — the HTML is the
 * agent-authored plan prose, rendered as-is by the Document view.
 */
export function getDocument(token: string, epicId: string): Promise<DocumentView> {
  return apiFetch<DocumentView>(
    `/epics/${encodeURIComponent(epicId)}/document`,
    token,
  );
}
