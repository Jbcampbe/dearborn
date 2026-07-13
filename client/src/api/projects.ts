// Projects REST surface (T-101/T-103) consumed by the Projects UI.
//
// Wraps the generic `apiFetch` helper with the `Project` shape and the handful
// of calls the UI needs. All secrets stay server-side: `pat` is only ever sent
// on create, never returned (see CONVENTIONS.md — the response never carries a
// PAT).

import { apiFetch, type Collection } from "./client";

/** Clone lifecycle state (T-103). */
export type CloneStatus = "pending" | "ready" | "error";

/** A project as returned by the API — never carries a PAT. */
export interface Project {
  id: string;
  name: string;
  repo_url: string;
  setup_cmd: string | null;
  test_cmd: string | null;
  run_cmd: string | null;
  clone_path: string | null;
  clone_status: CloneStatus;
  clone_error: string | null;
  created_at: number;
  updated_at: number;
}

/** Body for `POST /projects`. Optional fields are omitted when blank. */
export interface CreateProjectInput {
  name: string;
  repo_url: string;
  pat?: string;
  setup_cmd?: string;
  test_cmd?: string;
  run_cmd?: string;
}

/** `GET /projects` → the project list (newest first, per the server). */
export async function listProjects(token: string): Promise<Project[]> {
  const data = await apiFetch<Collection<Project>>("/projects", token);
  return data.items;
}

/** `GET /projects/{id}` → a single project. */
export function getProject(token: string, id: string): Promise<Project> {
  return apiFetch<Project>(`/projects/${encodeURIComponent(id)}`, token);
}

/** `POST /projects` → the created project (201). */
export function createProject(token: string, input: CreateProjectInput): Promise<Project> {
  return apiFetch<Project>("/projects", token, {
    method: "POST",
    body: JSON.stringify(input),
  });
}

/** `POST /projects/{id}/refresh` → the project, now `clone_status='pending'`. */
export function refreshProject(token: string, id: string): Promise<Project> {
  return apiFetch<Project>(`/projects/${encodeURIComponent(id)}/refresh`, token, {
    method: "POST",
  });
}
