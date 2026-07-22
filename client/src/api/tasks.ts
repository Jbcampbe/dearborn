// Task DAG REST surface (T-302/T-303) consumed by the Ready-lane DAG editor.
// Mirrors `projects.ts`/`epics.ts`: typed DTOs matching the server's shapes
// (see `dearborn-server/src/tasks.rs`) wrapped around the generic `apiFetch`.
//
// The live DAG does NOT come through here on mutations — every mutating call
// triggers a `dag_updated` frame over the WebSocket (`epic:<id>`), which the
// editor's reducer folds into its view model. This module covers only the
// request/response REST calls (initial load + commands); the WS side lives in
// `dag/`.

import { apiFetch } from "./client";

/** Task lifecycle status (§2.2). Readiness is computed, not stored. */
export type TaskStatus = "Todo" | "InProgress" | "Done" | "Failed" | "Cancelled";

/**
 * A task as returned by the API (`tasks.rs` `Task`). The Half-2 columns
 * (`failure_reason`, `agent_session_id`) round-trip as `null` in Half 1.
 */
export interface Task {
  id: string;
  epic_id: string | null;
  project_id: string;
  title: string;
  description: string | null;
  acceptance: string | null;
  status: TaskStatus;
  failure_reason: string | null;
  agent_session_id: string | null;
  position: number | null;
  created_at: number;
  updated_at: number;
}

/** A dependency edge: `blocker_id` blocks `blocked_id` (blocker must finish first). */
export interface Dependency {
  blocker_id: string;
  blocked_id: string;
}

/**
 * A task node in the DAG with computed readiness (§2.3). The `Task` fields are
 * flattened in alongside `ready` and `blocked_by`, matching the server's
 * `DagNode` (`#[serde(flatten)] task`).
 */
export interface DagNode extends Task {
  /** `true` iff `status === "Todo"` and every blocker is `Done`. */
  ready: boolean;
  /** Blocker ids not yet `Done` (non-empty only when `Todo` and not ready). */
  blocked_by: string[];
}

/** The epic's task DAG (`GET /epics/{id}/dag`). */
export interface Dag {
  epic_id: string;
  nodes: DagNode[];
  edges: Dependency[];
}

/** `GET /epics/{id}/dag` → the DAG with per-task readiness. */
export function getDag(token: string, epicId: string): Promise<Dag> {
  return apiFetch<Dag>(`/epics/${encodeURIComponent(epicId)}/dag`, token);
}

/** `GET /tasks/{id}` → a single task. */
export function getTask(token: string, id: string): Promise<Task> {
  return apiFetch<Task>(`/tasks/${encodeURIComponent(id)}`, token);
}

/** Body for `POST /epics/{id}/tasks`. */
export interface CreateTaskInput {
  title: string;
  description?: string;
  acceptance?: string;
  /** Ids of existing tasks this new task blocks (optional). */
  blocks?: string[];
}

/** `POST /epics/{id}/tasks` → the created task (201). */
export function createTask(token: string, epicId: string, input: CreateTaskInput): Promise<Task> {
  return apiFetch<Task>(`/epics/${encodeURIComponent(epicId)}/tasks`, token, {
    method: "POST",
    body: JSON.stringify(input),
  });
}

/** Body for `POST /projects/{id}/tasks` (no `blocks` — standalone tasks carry no dependencies). */
export interface CreateStandaloneTaskInput {
  title: string;
  description?: string;
  acceptance?: string;
}

/**
 * `POST /projects/{id}/tasks` → the created standalone task (201, `epic_id`
 * is `null`). A `board_updated` frame on `project:<id>` carries it to the
 * project kanban — no manual refetch needed.
 */
export function createProjectTask(
  token: string,
  projectId: string,
  input: CreateStandaloneTaskInput,
): Promise<Task> {
  return apiFetch<Task>(`/projects/${encodeURIComponent(projectId)}/tasks`, token, {
    method: "POST",
    body: JSON.stringify(input),
  });
}

/**
 * Body for `PATCH /tasks/{id}`. `description`/`acceptance` use the double-option
 * convention: absent = untouched, `null` = clear, a string = set. `title`/`status`
 * are plain optional (absent = untouched).
 */
export interface PatchTaskInput {
  title?: string;
  description?: string | null;
  acceptance?: string | null;
  status?: TaskStatus;
}

/** `PATCH /tasks/{id}` → the updated task (200). */
export function patchTask(token: string, id: string, input: PatchTaskInput): Promise<Task> {
  return apiFetch<Task>(`/tasks/${encodeURIComponent(id)}`, token, {
    method: "PATCH",
    body: JSON.stringify(input),
  });
}

/** `DELETE /tasks/{id}` → 204 (resolves to `undefined`). */
export function deleteTask(token: string, id: string): Promise<void> {
  return apiFetch<void>(`/tasks/${encodeURIComponent(id)}`, token, { method: "DELETE" });
}

/** `POST /epics/{id}/dependencies` → the created edge (201). */
export function linkDependency(
  token: string,
  epicId: string,
  blockerId: string,
  blockedId: string,
): Promise<Dependency> {
  return apiFetch<Dependency>(`/epics/${encodeURIComponent(epicId)}/dependencies`, token, {
    method: "POST",
    body: JSON.stringify({ blocker_id: blockerId, blocked_id: blockedId }),
  });
}

/** `DELETE /epics/{id}/dependencies?blocker_id=X&blocked_id=Y` → 204. Idempotent. */
export function unlinkDependency(
  token: string,
  epicId: string,
  blockerId: string,
  blockedId: string,
): Promise<void> {
  const q = `?blocker_id=${encodeURIComponent(blockerId)}&blocked_id=${encodeURIComponent(blockedId)}`;
  return apiFetch<void>(
    `/epics/${encodeURIComponent(epicId)}/dependencies${q}`,
    token,
    { method: "DELETE" },
  );
}

/**
 * `POST /epics/{id}/breakdown` → 202 (`{ status: "breakdown_started" }`). The
 * breakdown agent's `RunEvent`s stream over WS on `epic:<id>`; the DAG + the
 * `Planning → Ready` lane change land when the run completes. (T-301)
 */
export async function triggerBreakdown(
  token: string,
  epicId: string,
): Promise<{ status: string }> {
  // The server returns 202 with a JSON body, which `apiFetch` accepts as 2xx.
  return apiFetch<{ status: string }>(
    `/epics/${encodeURIComponent(epicId)}/breakdown`,
    token,
    { method: "POST" },
  );
}
