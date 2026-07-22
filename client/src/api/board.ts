// Project board + epic lane REST surface (T-401) consumed by the project
// kanban. Mirrors `tasks.ts`/`epics.ts`: typed DTOs matching the server's
// shapes (see `deerborn-server/src/board.rs` + `lanes.rs`) wrapped around the
// generic `apiFetch`.
//
// The live board does NOT come through here on mutations — a lane change
// triggers a `board_updated` frame over the WebSocket (`project:<id>`), which
// the kanban's reducer folds into its view model. This module covers only the
// request/response REST calls (initial load + lane-move command); the WS side
// lives in `board/`.

import { apiFetch } from "./client";
import type { Epic } from "./epics";
import type { Task } from "./tasks";

/** Task progress for one epic: `done` = strictly `Done`; `total` excludes `Cancelled`. */
export interface EpicProgress {
  epic_id: string;
  done: number;
  total: number;
}

/** The project board: its epics (in lane order), standalone tasks, and per-epic progress. */
export interface Board {
  epics: Epic[];
  tasks: Task[];
  epic_progress: EpicProgress[];
}

/** The epic lane set (§2.2 stored values — no spaces). */
export type EpicLane =
  | "Planning"
  | "Ready"
  | "InProgress"
  | "Completed"
  | "Cancelled"
  | "Blocked";

/** `GET /projects/{id}/board` → the project's kanban board. */
export function getBoard(token: string, projectId: string): Promise<Board> {
  return apiFetch<Board>(`/projects/${encodeURIComponent(projectId)}/board`, token);
}

/** `POST /epics/{id}/lane` → the updated epic (200). 409 on a disallowed transition. */
export function setEpicLane(
  token: string,
  epicId: string,
  status: EpicLane,
): Promise<Epic> {
  return apiFetch<Epic>(`/epics/${encodeURIComponent(epicId)}/lane`, token, {
    method: "POST",
    body: JSON.stringify({ status }),
  });
}
