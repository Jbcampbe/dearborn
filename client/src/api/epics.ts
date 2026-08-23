// Epics + planning-transcript REST surface (T-201/T-204) consumed by the
// planning UI. Mirrors `projects.ts`: typed DTOs matching the server's shapes
// (see `dearborn-server/src/epics.rs`) wrapped around the generic `apiFetch`.
//
// The live planning stream does NOT come through here — a `postMessage` triggers
// an agent run whose reply arrives over the WebSocket (`epic:<id>`), not in the
// HTTP response. This module only covers the request/response REST calls; the WS
// side lives in `planning/`.

import { apiFetch, type Collection } from "./client";

/** Planning lifecycle status. Lands in `Planning` on create. */
export type EpicStatus = "Planning" | string;

/**
 * An epic as returned by the API (`epics.rs` `Epic`). `product_context` /
 * `technical_context` are `null` until the planning agent fills them in via its
 * `update_epic` tool (surfaced live as `epic_updated` WS frames). `description`
 * is an optional user-facing short blurb shown on kanban cards.
 *
 * `pr_url` / `pr_number` (MILESTONE_2 §2.1) are populated together, exactly
 * once, by the executor's finalize step the moment `status` becomes
 * `Completed` — `null` until then. `blocked_reason` is one of the
 * MILESTONE_2 §2.3 reason strings whenever `status === "Blocked"` (T-540)
 * and `null` on every other transition, including a manual recovery via
 * `POST /tasks/{id}/retry` (T-541), which clears it.
 */
export interface Epic {
  id: string;
  project_id: string;
  title: string;
  description: string | null;
  product_context: string | null;
  technical_context: string | null;
  status: EpicStatus;
  pr_url: string | null;
  pr_number: number | null;
  blocked_reason: string | null;
  created_at: number;
  updated_at: number;
}

/** Transcript role (`epics.rs` `TranscriptMessage`). */
export type TranscriptRole = "user" | "agent" | "tool" | "system";

/** Planning phase. `product` runs first; the user advances to `technical`. */
export type PlanningPhase = "product" | "technical";

/** Planning-session lifecycle status (`planning_session.status`). */
export type SessionStatus = "active" | "complete";

/**
 * A planning session (`planning_session`), one per `(epic, phase)`. The internal
 * `harness_session_id` resume handle is intentionally NOT exposed by the API.
 */
export interface PlanningSession {
  epic_id: string;
  phase: PlanningPhase;
  status: SessionStatus;
  created_at: number;
  updated_at: number;
}

/**
 * A durable planning-transcript message (`transcript_message`, ordered by
 * `seq`). For `role: "tool"` the `content` is a serialized `RunEvent` JSON
 * string (a `toolStart`/`toolEnd`); for the other roles it is plain text.
 */
export interface TranscriptMessage {
  id: string;
  epic_id: string;
  phase: PlanningPhase;
  role: TranscriptRole;
  content: string;
  seq: number;
  created_at: number;
}

/** Body for `POST /projects/{id}/epics`. `description` is optional. */
export interface CreateEpicInput {
  title: string;
  description?: string;
  /**
   * Optional base-branch override (design §5): this epic provisions from and
   * PRs into this branch instead of the project default / repo default.
   * Validated against the remote at creation time (unknown branch → 400) and
   * immutable afterwards — set it here or not at all.
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
 * are left untouched; a `null` context clears it. `title` must be non-empty
 * when present.
 */
export interface UpdateEpicBody {
  title?: string;
  description?: string | null;
  product_context?: string | null;
  technical_context?: string | null;
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

/** `GET /epics/{id}/transcript` → the epic's messages in `seq` order. */
export async function getTranscript(token: string, id: string): Promise<TranscriptMessage[]> {
  const data = await apiFetch<Collection<TranscriptMessage>>(
    `/epics/${encodeURIComponent(id)}/transcript`,
    token,
  );
  return data.items;
}

/**
 * `POST /epics/{id}/messages` → the stored user message (201). This also
 * triggers the background agent run; its reply streams over the WebSocket, not
 * in this response.
 */
export function postMessage(
  token: string,
  id: string,
  phase: PlanningPhase,
  content: string,
): Promise<TranscriptMessage> {
  return apiFetch<TranscriptMessage>(`/epics/${encodeURIComponent(id)}/messages`, token, {
    method: "POST",
    body: JSON.stringify({ phase, content }),
  });
}

/** `GET /epics/{id}/sessions` → the epic's planning sessions (product first). */
export async function getSessions(token: string, id: string): Promise<PlanningSession[]> {
  const data = await apiFetch<Collection<PlanningSession>>(
    `/epics/${encodeURIComponent(id)}/sessions`,
    token,
  );
  return data.items;
}

/**
 * `POST /epics/{id}/advance-phase` → the epic's sessions after advancing product
 * → technical (`201`). The transcript continues on the same `seq`; subsequent
 * messages are sent with `phase: "technical"`.
 */
export async function advancePhase(token: string, id: string): Promise<PlanningSession[]> {
  const data = await apiFetch<Collection<PlanningSession>>(
    `/epics/${encodeURIComponent(id)}/advance-phase`,
    token,
    { method: "POST" },
  );
  return data.items;
}
