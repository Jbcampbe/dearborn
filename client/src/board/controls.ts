// T-561: Client control surface — pure decisions for the kanban cards' new
// controls (Retry / Run / Cancel) and metadata (failure/blocked reasons, PR
// links). Mirrors `epicLanes.ts`/`dnd.ts`: framework-free, dependency-free
// (no Vue, no fetch, no WS) so the "which controls does this card show" and
// "what does this failure mean" logic can be unit-tested without a browser.
//
// This module deliberately does NOT call `apiFetch` and does NOT touch board/
// DAG state. The components call the REST functions (`tasks.ts`'s `retryTask`/
// `runTask`, `board.ts`'s `setEpicLane`) directly and let the resulting
// `board_updated`/`dag_updated`/`epic_updated` WS frame — already wired by the
// existing stream reducers — drive the re-render (the AC's "without a manual
// refresh"). What this module owns is everything that decides *whether* a
// button renders and *what a rejected call says* to a human.

import { ApiError } from "../api/client";
import type { Epic } from "../api/epics";
import type { Task } from "../api/tasks";

/**
 * A `Failed` task — standalone or epic-scoped — can be retried
 * (`POST /tasks/{id}/retry`, T-541/T-551). The server's own fenced `UPDATE`
 * is the real guard (a `409` is always possible on a race); this mirrors it
 * client-side only to decide whether the button is worth showing at all.
 */
export function canRetryTask(task: Pick<Task, "status">): boolean {
  return task.status === "Failed";
}

/**
 * Only a standalone `Todo` task (`epic_id === null`) can be run directly
 * (`POST /tasks/{id}/run`, T-551 §2.5) — an epic-scoped task is only ever run
 * as part of its epic's own `Ready → InProgress` move.
 */
export function canRunTask(task: Pick<Task, "status" | "epic_id">): boolean {
  return task.status === "Todo" && task.epic_id === null;
}

/**
 * Only an `InProgress` epic has a live agent stage worth killing (T-542,
 * D12) — `POST /epics/{id}/lane` with `{ status: "Cancelled" }` is a no-op
 * kill-wise for any other status, and the lane's own transition table
 * already forbids the move from anywhere but `InProgress`/`Ready`/`Planning`/
 * `Blocked`, so gating on `InProgress` here shows the button only where a
 * cancel actually stops something in flight.
 */
export function canCancelEpic(epic: Pick<Epic, "status">): boolean {
  return epic.status === "InProgress";
}

/**
 * Whether a card shows its PR link (`PR #N`, opening the stored `pr_url` in a
 * new tab). The §4 review loop means `pr_url` is attached when the item lands
 * in `InReview` (PR open, waiting on the human) and stays attached through
 * `Completed`/`Done` (merged) — the link is shown in both cases and hidden
 * whenever the item has no `pr_url` yet (or is Blocked/Cancelled).
 */
export function showPrLink(status: string, prUrl: string | null | undefined): boolean {
  return (status === "InReview" || status === "Completed" || status === "Done") && !!prUrl;
}

/** The action a rejected control call is described relative to. */
export type ControlAction = "retry" | "run" | "cancel";

const ACTION_VERB: Record<ControlAction, string> = {
  retry: "retry this task",
  run: "run this task",
  cancel: "cancel this epic",
};

/**
 * Turn a failed control call into a message fit for an inline error banner.
 * This is the AC's "a `409` surfaces a readable message rather than a silent
 * no-op": a `409` here always means the card's state moved between render
 * and click (a worker claimed it, another tab retried/cancelled it first,
 * the epic already left `InProgress`, …) — worth saying plainly, with the
 * server's own (already fairly specific) conflict text folded in, rather
 * than swallowing the rejection or showing a generic "failed" toast.
 */
export function describeControlError(action: ControlAction, err: unknown): string {
  const verb = ACTION_VERB[action];
  if (err instanceof ApiError) {
    if (err.status === 409) {
      return `Can't ${verb} — ${err.message}. Its status likely changed just now; no refresh needed, the card will update.`;
    }
    if (err.status === 404) {
      return `Can't ${verb} — it no longer exists.`;
    }
    return `Can't ${verb} — ${err.message}.`;
  }
  if (err instanceof Error) {
    return `Can't ${verb} — ${err.message}.`;
  }
  return `Can't ${verb} — something went wrong.`;
}

/**
 * Human-readable text for a `task.failure_reason` / `epic.blocked_reason`
 * value (MILESTONE_2 §2.3's fixed vocabulary). Falls back to a title-cased
 * rendering of the raw string for a reason this list doesn't (yet) know
 * about, rather than hiding it — the AC is "render the new metadata", not
 * "render only the reasons we anticipated."
 */
const REASON_TEXT: Record<string, string> = {
  preflight_red: "Preflight tests failed before any work began",
  setup_failed: "Workspace setup command failed",
  workspace_error: "Workspace provisioning failed",
  test_gate_exhausted: "Tests never went green after repeated fixes",
  review_not_converged: "Review didn't converge within the fix-round limit",
  blocked: "The agent reported this as blocked",
  agent_error: "An agent stage failed",
  timeout: "An agent stage timed out",
  cancelled: "Cancelled",
  pr_failed: "Push or PR creation failed",
  provider_rate_limited: "Provider rate limit hit during implementation",
};

export function describeFailureReason(reason: string | null | undefined): string | null {
  if (reason === null || reason === undefined || reason === "") {
    return null;
  }
  return REASON_TEXT[reason] ?? titleCase(reason);
}

function titleCase(snake: string): string {
  const words = snake.split("_").filter((w) => w.length > 0);
  if (words.length === 0) {
    return snake;
  }
  return words[0]!.charAt(0).toUpperCase() + words[0]!.slice(1) + " " + words.slice(1).join(" ");
}

/** `PR #123` when the number is known, else a generic label — for the link text. */
export function prLabel(prNumber: number | null | undefined): string {
  return typeof prNumber === "number" ? `PR #${prNumber}` : "View PR";
}
