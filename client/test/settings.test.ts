// Unit tests for the agent-settings API module (`src/api/settings.ts`):
// the pure mapping helpers and the closed slot vocabulary. No browser, no
// fetch — the fetchers themselves are thin `apiFetch` wrappers exercised by
// the server integration tests.

import { describe, expect, it } from "vitest";

import {
  AGENT_SLOTS,
  SLOT_LABELS,
  blankClears,
  type SlotSetting,
} from "../src/api/settings";

describe("AGENT_SLOTS", () => {
  it("lists exactly the eight server slots in canonical order", () => {
    expect(AGENT_SLOTS).toEqual([
      "planning_product",
      "planning_technical",
      "breakdown",
      "implement",
      "fix",
      "review",
      "verify_complete",
      "summarize",
    ]);
  });

  it("has a human label for every slot", () => {
    for (const slot of AGENT_SLOTS) {
      expect(SLOT_LABELS[slot].length).toBeGreaterThan(0);
    }
  });
});

describe("blankClears", () => {
  it("maps blank/whitespace input to null (clear the override)", () => {
    expect(blankClears("")).toBeNull();
    expect(blankClears("   ")).toBeNull();
  });

  it("maps non-blank input to its trimmed value", () => {
    expect(blankClears("claude")).toBe("claude");
    expect(blankClears("  sonnet-4-5 ")).toBe("sonnet-4-5");
  });
});

describe("effectiveLine inputs", () => {
  // The panel renders `runs on <harness> · <model|CLI default> · <prompt
  // source>`; pin the shape of the data it formats so UI regressions surface
  // as type breaks here.
  function makeView(overrides: Partial<SlotSetting> = {}): SlotSetting {
    return {
      slot: "implement",
      harness: null,
      model: null,
      system_prompt: null,
      effective: { harness: "claude", model: null, prompt_source: "default" },
      ...overrides,
    };
  }

  it("distinguishes override from default prompt source", () => {
    const def = makeView();
    const custom = makeView({
      system_prompt: "# custom",
      effective: { harness: "claude", model: "sonnet-4-5", prompt_source: "override" },
    });
    expect(def.effective.prompt_source).toBe("default");
    expect(custom.effective.prompt_source).toBe("override");
    expect(custom.effective.model).toBe("sonnet-4-5");
  });
});
