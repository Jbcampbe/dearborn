// Task DAG REST surface (T-302/T-303) consumed by the Ready-lane DAG editor.
// Mirrors `projects.ts`/`epics.ts`: typed DTOs matching the server's shapes
// (see `dearborn-server/src/tasks.rs`) wrapped around the generic `apiFetch`.
//
// The live DAG does NOT come through here on mutations — every mutating call
// triggers a `dag_updated` frame over the WebSocket (`epic:<id>`), which the
// editor's reducer folds into its view model. This module covers only the
// request/response REST calls (initial load + commands); the WS side lives in
// `dag/`.
//
// T-562 adds the two `agent_run` evidence endpoints (`dearborn-server/src/
// evidence.rs`, T-512) the task detail pipeline view hydrates from:
// `getTaskRuns` (cheap — no `log`, one row per pipeline stage attempt) and
// `getRunLog` (one row's full capped log, fetched only when a timeline row is
// expanded). Neither is live — the pipeline view's WS follow (`task:<id>`,
// T-563) is a separate, not-yet-built seam; see `src/task/pipeline.ts`.

import { apiFetch, type Collection } from "./client";

/** Task lifecycle status (§2.2). Readiness is computed, not stored. */
export type TaskStatus = "Todo" | "InProgress" | "Done" | "Failed" | "Cancelled";

/**
 * A task as returned by the API (`tasks.rs` `Task`). `failure_reason` is one
 * of the MILESTONE_2 §2.3 reason strings, set alongside `status: "Failed"`
 * (T-540) and cleared by `POST /tasks/{id}/retry` (T-541). `branch_name` /
 * `pr_url` / `pr_number` are populated only for a task that has actually run
 * the executor pipeline (an epic-scoped task claimed as part of its epic's
 * walk, or a standalone task via `POST /tasks/{id}/run`, T-551) — `null`
 * until then, and for `branch_name` also for the lifetime of a task that
 * never reaches that pipeline (e.g. one still `Todo`/epic-scoped-and-not-yet-
 * claimed). `pr_url`/`pr_number` land together, once, on a standalone task's
 * own successful finalize (there is no epic to carry them instead).
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
  branch_name: string | null;
  pr_url: string | null;
  pr_number: number | null;
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

/**
 * `POST /tasks/{id}/retry` → the updated task (200). T-541/T-551: a
 * `Failed` task only — `409` (`ApiError`) otherwise. An epic-scoped task
 * returns to `Todo` and un-blocks its epic; a standalone task returns
 * directly to `InProgress` and is re-claimed on its own. Either way the
 * resulting `dag_updated`/`epic_updated`/`board_updated` WS frame(s) drive
 * the re-render — this call has no other visible effect to wait on.
 */
export function retryTask(token: string, id: string): Promise<Task> {
  return apiFetch<Task>(`/tasks/${encodeURIComponent(id)}/retry`, token, { method: "POST" });
}

/**
 * `POST /tasks/{id}/run` → the updated task (200). T-551 §2.5: a standalone
 * (`epic_id: null`) `Todo` task only — `409` (`ApiError`) otherwise,
 * including for any epic-scoped task regardless of its own status. The
 * resulting `board_updated` WS frame drives the re-render.
 */
export function runTask(token: string, id: string): Promise<Task> {
  return apiFetch<Task>(`/tasks/${encodeURIComponent(id)}/run`, token, { method: "POST" });
}

/**
 * One `agent_run` row as returned by `GET /tasks/{id}/runs` (T-512 evidence,
 * §2.1/§2.5) — every pipeline stage this task has run through, oldest first.
 * Mirrors the server's `AgentRunSummary` (`evidence.rs`) field for field,
 * **without** `log` — the list endpoint deliberately omits it (a busy task
 * can carry several capped-256KB stage logs; a timeline view only needs to
 * know what happened, not download every stage's full transcript). `verdict`
 * is non-null only for a `review` or `verify_complete` row; `session_id` is
 * `null` for every non-agent stage (`setup`/`preflight`/`test_gate`/`commit`/
 * `push`). `task_id`/`epic_id` are nullable on the row in general (an epic
 * finalize's `push` row and, since T-560, an epic's own `summarize` row can
 * both be task-less) but every row *this* endpoint returns has `task_id`
 * equal to the id in the URL, by construction of the underlying query.
 */
export interface AgentRunSummary {
  id: string;
  task_id: string | null;
  epic_id: string | null;
  stage: string;
  attempt: number;
  status: string; // running | ok | error | timeout | cancelled
  verdict: string | null; // PASS | NEEDS_CHANGES | BLOCKED
  session_id: string | null;
  started_at: number | null;
  ended_at: number | null;
  exit_code: number | null;
  created_at: number;
}

/** One `agent_run` row **with** its full (capped) log — `GET /runs/{id}`. */
export interface AgentRunDetail extends AgentRunSummary {
  log: string;
}

/**
 * `GET /tasks/{id}/runs` → a task's stage history, oldest first (T-512
 * §2.5). `404` (`ApiError`) if the task does not exist. Unwraps the
 * `{ items }` envelope — callers get the array directly, matching how
 * `getDag`/`getBoard` already hand back their payload's real shape rather
 * than the transport envelope.
 */
export function getTaskRuns(token: string, taskId: string): Promise<AgentRunSummary[]> {
  return apiFetch<Collection<AgentRunSummary>>(
    `/tasks/${encodeURIComponent(taskId)}/runs`,
    token,
  ).then((c) => c.items);
}

/**
 * `GET /runs/{id}` → one stage's full (capped) log. `404` (`ApiError`) if
 * unknown. Called only on demand — once per expanded pipeline row, not as
 * part of the initial timeline hydrate.
 */
export function getRunLog(token: string, runId: string): Promise<AgentRunDetail> {
  return apiFetch<AgentRunDetail>(`/runs/${encodeURIComponent(runId)}`, token);
}

/**
 * One persisted tool-call event for an agent run (`agent_run_events` table).
 * `tool_start` rows carry the tool `name` with `ok: null`; `tool_end` rows
 * carry an empty `name` and the outcome in `ok`. Pair them by `toolCallId`.
 */
export interface ToolCallEvent {
  kind: "tool_start" | "tool_end";
  toolCallId: string;
  name: string;
  ok: boolean | null;
}

/**
 * `GET /runs/{id}/events` → one stage run's persisted tool-call events,
 * oldest first. `404` (`ApiError`) if unknown. Called only on demand — once
 * per expanded pipeline row, alongside `getRunLog`.
 */
export function getRunEvents(token: string, runId: string): Promise<ToolCallEvent[]> {
  return apiFetch<ToolCallEvent[]>(`/runs/${encodeURIComponent(runId)}/events`, token);
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
