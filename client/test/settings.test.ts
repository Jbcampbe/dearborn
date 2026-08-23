// Unit tests for the agent-settings API module (`src/api/settings.ts`):
// the pure mapping helpers and the closed slot vocabulary. No browser, no
// fetch — the fetchers themselves are thin `apiFetch` wrappers exercised by
// the server integration tests.

import { describe, expect, it } from "vitest";

import {
  AGENT_SLOTS,
  SLOT_LABELS,
  blankClears,
  promptSaveValue,
  type SlotSetting,
} from "../src/api/settings";

// Shared view factory for both suites below: the panel renders `runs on
// <harness> · <model|CLI default> · <prompt source>`; pin the shape of the
// data it formats so UI regressions surface as type breaks here.
function makeView(overrides: Partial<SlotSetting> = {}): SlotSetting {
  return {
    slot: "implement",
    harness: null,
    model: null,
    system_prompt: null,
    default_prompt: "Built-in implement instructions.\n",
    effective: { harness: "claude", model: null, prompt_source: "default" },
    ...overrides,
  };
}

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

  it("every slot view carries a non-empty default_prompt for editor prefill", () => {
    for (const slot of AGENT_SLOTS) {
      const view = makeView({ slot });
      expect(view.default_prompt.length).toBeGreaterThan(0);
    }
  });
});

describe("promptSaveValue (editor save semantics)", () => {
  const defaultView = () => makeView(); // default_prompt: "Built-in implement instructions.\n"
  const overrideView = () =>
    makeView({ system_prompt: "# my custom prompt", effective: { harness: "claude", model: null, prompt_source: "override" } });

  it("blank draft sends the explicit-empty shape regardless of source", () => {
    expect(promptSaveValue("   ", defaultView())).toBe("");
    expect(promptSaveValue("", overrideView())).toBe("");
  });

  it("unchanged default-source text sends null, never freezes the built-in as an override", () => {
    expect(promptSaveValue(defaultView().default_prompt, defaultView())).toBeNull();
    // Whitespace-padded edits of an untouched prefill still count as unchanged.
    expect(promptSaveValue(`  ${defaultView().default_prompt.trim()}\n`, defaultView())).toBeNull();
  });

  it("edited text on a default-source slot becomes an explicit override", () => {
    expect(promptSaveValue("Tweaked instructions.", defaultView())).toBe("Tweaked instructions.");
  });

  it("an existing override always saves as trimmed text — even byte-identical to the default", () => {
    // Re-saving today's override must not silently reset it to inherited.
    const view = makeView({ system_prompt: defaultView().default_prompt.trim() });
    expect(promptSaveValue(view.default_prompt.trim(), view)).toBe(view.default_prompt.trim());
    expect(promptSaveValue("# still mine", overrideView())).toBe("# still mine");
  });
});
