// Epics REST surface consumed by the epic views. Mirrors `projects.ts`:
// typed DTOs matching the server's shapes (see `dearborn-server/src/epics.rs`)
// wrapped around the generic `apiFetch`.
//
// The old linear planning-chat endpoints (transcript, sessions, advance-phase,
// messages) were removed with the wayfinder cutover — planning history lives
// on map nodes, and the map/node APIs arrive with their own tasks.

import { apiFetch, type Collection } from "./client";

/** Planning lifecycle status. Lands in `Planning` on create. */
export type EpicStatus = "Planning" | string;

/**
 * An epic as returned by the API (`epics.rs` `Epic`). `destination` is the
 * required, human-typed statement of what the finished plan looks like; `notes`
 * is its optional companion prose. `description` is an optional user-facing
 * short blurb shown on kanban cards.
 *
 * `pr_url` / `pr_number` are populated together, exactly once, by the
 * executor's finalize step the moment `status` becomes `Completed` — `null`
 * until then. `blocked_reason` is one of the executor reason strings whenever
 * `status === "Blocked"` and `null` on every other transition, including a
 * manual recovery via `POST /tasks/{id}/retry`, which clears it.
 * `failure_detail` is the human-readable companion to `blocked_reason`: the
 * redacted, length-capped agent error text that makes a Blocked epic
 * triageable.
 */
export interface Epic {
  id: string;
  project_id: string;
  title: string;
  description: string | null;
  destination: string | null;
  notes: string | null;
  status: EpicStatus;
  pr_url: string | null;
  pr_number: number | null;
  blocked_reason: string | null;
  failure_detail: string | null;
  created_at: number;
  updated_at: number;
}

/** Body for `POST /projects/{id}/epics`. `destination` is required. */
export interface CreateEpicInput {
  title: string;
  /** Required: what the finished plan looks like — fixes scope. */
  destination: string;
  description?: string;
  /** Optional freeform prose alongside the destination. */
  notes?: string;
  /**
   * Optional base-branch override: this epic provisions from and PRs into this
   * branch instead of the project default / repo default. Validated against
   * the remote at creation time (unknown branch → 400) and immutable
   * afterwards — set it here or not at all.
   */
  base_branch?: string;
}

/** `POST /projects/{id}/epics` → the created epic (201, `status='Planning'`). */
export function createEpic(
  token: string,
  projectId: string,
  input: CreateEpicInput,
): Promise<Epic> {
  return apiFetch<Epic>(`/projects/${encodeURIComponent(projectId)}/epics`, token, {
    method: "POST",
    body: JSON.stringify(input),
  });
}

/** `GET /projects/{id}/epics` → a project's epics (newest first). */
export async function listEpics(token: string, projectId: string): Promise<Epic[]> {
  const data = await apiFetch<Collection<Epic>>(
    `/projects/${encodeURIComponent(projectId)}/epics`,
    token,
  );
  return data.items;
}

/** `GET /epics/{id}` → a single epic. */
export function getEpic(token: string, id: string): Promise<Epic> {
  return apiFetch<Epic>(`/epics/${encodeURIComponent(id)}`, token);
}

/**
 * `PATCH /epics/{id}` body — manual edits from the Details tab. Absent keys
 * are left untouched; a `null` description clears it. `title` must be
 * non-empty when present. (`destination`/`notes` are not editable here — the
 * map workflow owns them.)
 */
export interface UpdateEpicBody {
  title?: string;
  description?: string | null;
}

/**
 * `PATCH /epics/{id}` → the updated epic (200). The server also publishes an
 * `epic_updated` frame on `epic:<id>`, so every subscribed view re-renders
 * live with the manual edit.
 */
export function updateEpic(token: string, id: string, body: UpdateEpicBody): Promise<Epic> {
  return apiFetch<Epic>(`/epics/${encodeURIComponent(id)}`, token, {
    method: "PATCH",
    body: JSON.stringify(body),
  });
}
