// Pure helpers for the epic Details editor (`EpicDetailView.vue`).
// Framework-free and dependency-free (no Vue, no fetch) so they can be
// unit-tested without a browser — mirrors `planning/stream.ts`. The view keeps
// a local `draft` of the editable fields plus a `baseline` of the last-known
// server values; these helpers diff the two into a minimal PATCH body and fold
// live `epic_updated` frames in without clobbering unsaved local edits.

import type { Epic, UpdateEpicBody } from "../api/epics";

/**
 * The editable epic fields, as local draft strings. `null` contexts edit as
 * `""` (an emptied field is sent back as `null` — see {@link diffEpicEdits}).
 */
export interface EpicDraft {
  title: string;
  description: string;
  product_context: string;
  technical_context: string;
}

/** The editable field keys, for iteration that stays type-safe. */
const FIELDS = ["title", "description", "product_context", "technical_context"] as const;

/** Snapshot an epic's editable fields into draft strings. */
export function draftFromEpic(epic: Epic): EpicDraft {
  return {
    title: epic.title,
    description: epic.description ?? "",
    product_context: epic.product_context ?? "",
    technical_context: epic.technical_context ?? "",
  };
}

/**
 * Whether one draft field still matches its baseline (i.e. the user has no
 * unsaved local edit to it). The title comparison trims the draft, matching
 * the server's own trim-on-save.
 */
export function fieldPristine(key: keyof EpicDraft, baseline: EpicDraft, draft: EpicDraft): boolean {
  return key === "title"
    ? draft.title.trim() === baseline.title
    : draft[key] === baseline[key];
}

/**
 * The minimal `PATCH /epics/:id` body for the fields that differ from
 * `baseline`. Changed contexts map an emptied draft to `null` (clears the
 * column); a changed title is sent trimmed (the server rejects an empty
 * title). Unchanged fields are absent, so they are never written.
 */
export function diffEpicEdits(baseline: EpicDraft, draft: EpicDraft): UpdateEpicBody {
  const body: UpdateEpicBody = {};
  if (draft.title.trim() !== baseline.title) {
    body.title = draft.title.trim();
  }
  if (draft.description !== baseline.description) {
    body.description = draft.description.length > 0 ? draft.description : null;
  }
  if (draft.product_context !== baseline.product_context) {
    body.product_context = draft.product_context.length > 0 ? draft.product_context : null;
  }
  if (draft.technical_context !== baseline.technical_context) {
    body.technical_context = draft.technical_context.length > 0 ? draft.technical_context : null;
  }
  return body;
}

/** Whether any field differs from the baseline (gates the Save button). */
export function isDirty(baseline: EpicDraft, draft: EpicDraft): boolean {
  return Object.keys(diffEpicEdits(baseline, draft)).length > 0;
}

/**
 * Fold a live `epic_updated` payload into the baseline + draft. Pristine
 * fields follow the server value; dirty fields keep the user's unsaved local
 * edit (their baseline still moves to the server value, so they stay dirty).
 * Mutates both objects in place (callers wrap them in Vue reactivity).
 */
export function applyLiveEpic(baseline: EpicDraft, draft: EpicDraft, epic: Epic): void {
  const incoming = draftFromEpic(epic);
  for (const key of FIELDS) {
    if (fieldPristine(key, baseline, draft)) {
      draft[key] = incoming[key];
    }
    baseline[key] = incoming[key];
  }
}
