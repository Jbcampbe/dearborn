// Epics + planning-transcript REST surface (T-201/T-204) consumed by the
// planning UI. Mirrors `projects.ts`: typed DTOs matching the server's shapes
// (see `deerborn-server/src/epics.rs`) wrapped around the generic `apiFetch`.
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
 * `update_epic` tool (surfaced live as `epic_updated` WS frames).
 */
export interface Epic {
  id: string;
  project_id: string;
  title: string;
  product_context: string | null;
  technical_context: string | null;
  status: EpicStatus;
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

/** `POST /projects/{id}/epics` → the created epic (201, `status='Planning'`). */
export function createEpic(token: string, projectId: string, title: string): Promise<Epic> {
  return apiFetch<Epic>(`/projects/${encodeURIComponent(projectId)}/epics`, token, {
    method: "POST",
    body: JSON.stringify({ title }),
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
