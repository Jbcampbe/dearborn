// Unit tests for the T-561 control-surface pure module (`src/board/controls.ts`):
// which controls a card offers (Retry / Run / Cancel), and what a rejected
// call means to a human. No browser, no fetch — `ApiError` instances are
// hand-built exactly like `client.ts` throws them.

import { describe, expect, it } from "vitest";

import { ApiError } from "../src/api/client";
import type { Epic } from "../src/api/epics";
import type { Task } from "../src/api/tasks";
import {
  canCancelEpic,
  canRetryTask,
  canRunTask,
  describeControlError,
  describeFailureReason,
  prLabel,
  showPrLink,
} from "../src/board/controls";

function makeTask(overrides: Partial<Task> = {}): Task {
  return {
    id: "T1",
    epic_id: null,
    project_id: "P1",
    title: "Slice",
    description: null,
    acceptance: null,
    status: "Todo",
    failure_reason: null,
    agent_session_id: null,
    position: null,
    branch_name: null,
    pr_url: null,
    pr_number: null,
    created_at: 1,
    updated_at: 1,
    ...overrides,
  };
}

function makeEpic(overrides: Partial<Epic> = {}): Epic {
  return {
    id: "E1",
    project_id: "P1",
    title: "Ship it",
    description: null,
    product_context: null,
    technical_context: null,
    status: "Planning",
    pr_url: null,
    pr_number: null,
    blocked_reason: null,
    created_at: 1,
    updated_at: 1,
    ...overrides,
  };
}

describe("canRetryTask", () => {
  it("is true only for a Failed task, standalone or epic-scoped", () => {
    expect(canRetryTask(makeTask({ status: "Failed" }))).toBe(true);
    expect(canRetryTask(makeTask({ status: "Failed", epic_id: "E1" }))).toBe(true);
    expect(canRetryTask(makeTask({ status: "Todo" }))).toBe(false);
    expect(canRetryTask(makeTask({ status: "InProgress" }))).toBe(false);
    expect(canRetryTask(makeTask({ status: "Done" }))).toBe(false);
    expect(canRetryTask(makeTask({ status: "Cancelled" }))).toBe(false);
  });
});

describe("canRunTask", () => {
  it("is true only for a standalone Todo task", () => {
    expect(canRunTask(makeTask({ status: "Todo", epic_id: null }))).toBe(true);
  });

  it("is false for an epic-scoped task regardless of status", () => {
    expect(canRunTask(makeTask({ status: "Todo", epic_id: "E1" }))).toBe(false);
    expect(canRunTask(makeTask({ status: "InProgress", epic_id: "E1" }))).toBe(false);
  });

  it("is false for a standalone task in any other status", () => {
    expect(canRunTask(makeTask({ status: "InProgress", epic_id: null }))).toBe(false);
    expect(canRunTask(makeTask({ status: "Done", epic_id: null }))).toBe(false);
    expect(canRunTask(makeTask({ status: "Failed", epic_id: null }))).toBe(false);
    expect(canRunTask(makeTask({ status: "Cancelled", epic_id: null }))).toBe(false);
  });
});

describe("canCancelEpic", () => {
  it("is true only for an InProgress epic", () => {
    expect(canCancelEpic(makeEpic({ status: "InProgress" }))).toBe(true);
    for (const status of ["Planning", "Ready", "Completed", "Cancelled", "Blocked"]) {
      expect(canCancelEpic(makeEpic({ status }))).toBe(false);
    }
  });
});

describe("describeControlError", () => {
  it("names the conflict and the action for a 409", () => {
    const err = new ApiError(409, "conflict", "task T1 is not Failed");
    const msg = describeControlError("retry", err);
    expect(msg).toContain("retry this task");
    expect(msg).toContain("task T1 is not Failed");
  });

  it("gives a distinct message for a 404", () => {
    const err = new ApiError(404, "not_found", "task T1 not found");
    expect(describeControlError("run", err)).toBe("Can't run this task — it no longer exists.");
  });

  it("folds the server message in for any other ApiError status", () => {
    const err = new ApiError(500, "internal", "boom");
    expect(describeControlError("cancel", err)).toBe("Can't cancel this epic — boom.");
  });

  it("falls back to a plain Error's message", () => {
    expect(describeControlError("retry", new Error("network down"))).toBe(
      "Can't retry this task — network down.",
    );
  });

  it("falls back to a generic message for a non-Error throw", () => {
    expect(describeControlError("run", "nope")).toBe(
      "Can't run this task — something went wrong.",
    );
  });
});

describe("describeFailureReason", () => {
  it("maps every MILESTONE_2 §2.3 reason to human text", () => {
    expect(describeFailureReason("preflight_red")).toBe(
      "Preflight tests failed before any work began",
    );
    expect(describeFailureReason("test_gate_exhausted")).toBe(
      "Tests never went green after repeated fixes",
    );
    expect(describeFailureReason("pr_failed")).toBe("Push or PR creation failed");
  });

  it("falls back to a title-cased rendering for an unknown reason", () => {
    expect(describeFailureReason("some_new_reason")).toBe("Some new reason");
  });

  it("is null for null/undefined/empty", () => {
    expect(describeFailureReason(null)).toBeNull();
    expect(describeFailureReason(undefined)).toBeNull();
    expect(describeFailureReason("")).toBeNull();
  });
});

describe("prLabel", () => {
  it("renders the PR number when known", () => {
    expect(prLabel(42)).toBe("PR #42");
  });

  it("falls back to a generic label without a number", () => {
    expect(prLabel(null)).toBe("View PR");
    expect(prLabel(undefined)).toBe("View PR");
  });
});

describe("showPrLink", () => {
  it("is true for an InReview item with a pr_url (§4: PR open, waiting on the human)", () => {
    expect(showPrLink("InReview", "https://github.com/acme/demo/pull/7")).toBe(true);
  });

  it("stays true through merge (Completed / Done) — the link survives", () => {
    expect(showPrLink("Completed", "https://github.com/acme/demo/pull/7")).toBe(true);
    expect(showPrLink("Done", "https://github.com/acme/demo/pull/7")).toBe(true);
  });

  it("is hidden whenever pr_url is absent, whatever the status", () => {
    expect(showPrLink("InReview", null)).toBe(false);
    expect(showPrLink("InReview", undefined)).toBe(false);
    expect(showPrLink("InReview", "")).toBe(false);
    expect(showPrLink("Completed", null)).toBe(false);
  });

  it("is false for statuses that carry no PR", () => {
    expect(showPrLink("InProgress", "https://x/pull/1")).toBe(false);
    expect(showPrLink("Blocked", "https://x/pull/1")).toBe(false);
    expect(showPrLink("Cancelled", "https://x/pull/1")).toBe(false);
    expect(showPrLink("Todo", "https://x/pull/1")).toBe(false);
  });
});
