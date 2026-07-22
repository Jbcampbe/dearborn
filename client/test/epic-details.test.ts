// Unit tests for the epic Details editor helpers (`src/lib/epicEdit.ts`). The
// view (`EpicDetailView.vue`) is a thin shell over these pure functions: they
// diff the local draft against the last-known server baseline into a minimal
// PATCH body, and fold live `epic_updated` frames in without clobbering
// unsaved local edits.

import { describe, expect, it } from "vitest";

import type { Epic } from "../src/api/epics";
import {
  applyLiveEpic,
  diffEpicEdits,
  draftFromEpic,
  fieldPristine,
  isDirty,
  type EpicDraft,
} from "../src/lib/epicEdit";

function epic(overrides: Partial<Epic> = {}): Epic {
  return {
    id: "E1",
    project_id: "P1",
    title: "Ship it",
    product_context: null,
    technical_context: null,
    status: "Planning",
    created_at: 1,
    updated_at: 1,
    ...overrides,
  };
}

function draft(overrides: Partial<EpicDraft> = {}): EpicDraft {
  return { title: "Ship it", product_context: "", technical_context: "", ...overrides };
}

describe("draftFromEpic", () => {
  it("maps null contexts to empty strings for editing", () => {
    expect(draftFromEpic(epic())).toEqual(draft());
    expect(
      draftFromEpic(epic({ product_context: "why", technical_context: "how" })),
    ).toEqual(draft({ product_context: "why", technical_context: "how" }));
  });
});

describe("diffEpicEdits", () => {
  it("returns an empty body when nothing changed", () => {
    expect(diffEpicEdits(draft(), draft())).toEqual({});
    expect(isDirty(draft(), draft())).toBe(false);
  });

  it("emits only the changed fields", () => {
    const body = diffEpicEdits(draft(), draft({ product_context: "why" }));
    expect(body).toEqual({ product_context: "why" });
  });

  it("trims a changed title (the server trims on save too)", () => {
    const body = diffEpicEdits(draft(), draft({ title: "  Renamed  " }));
    expect(body).toEqual({ title: "Renamed" });
  });

  it("treats trailing-whitespace-only title edits as unchanged", () => {
    expect(diffEpicEdits(draft(), draft({ title: "Ship it " }))).toEqual({});
  });

  it("maps an emptied context back to null (clears the column)", () => {
    const body = diffEpicEdits(
      draft({ product_context: "why" }),
      draft({ product_context: "" }),
    );
    expect(body).toEqual({ product_context: null });
  });
});

describe("applyLiveEpic", () => {
  it("moves pristine fields to the incoming server values", () => {
    const baseline = draft();
    const local = draft();
    applyLiveEpic(baseline, local, epic({ title: "Agent rename", product_context: "ctx" }));

    expect(local).toEqual(draft({ title: "Agent rename", product_context: "ctx" }));
    expect(baseline).toEqual(local);
  });

  it("never clobbers an unsaved local edit, and the field stays dirty", () => {
    const baseline = draft();
    const local = draft({ technical_context: "my unsaved edit" });
    applyLiveEpic(
      baseline,
      local,
      epic({ title: "Agent rename", technical_context: "agent overwrite" }),
    );

    // The dirty field kept the local edit; the pristine title followed the server.
    expect(local.technical_context).toBe("my unsaved edit");
    expect(local.title).toBe("Agent rename");
    // The baseline moved to the server value, so the field is still dirty.
    expect(baseline.technical_context).toBe("agent overwrite");
    expect(isDirty(baseline, local)).toBe(true);
    expect(diffEpicEdits(baseline, local)).toEqual({ technical_context: "my unsaved edit" });
  });

  it("resolves dirty state when the server converges on the local edit", () => {
    const baseline = draft();
    const local = draft({ title: "Renamed" });
    // Our own save's broadcast comes back with the same value.
    applyLiveEpic(baseline, local, epic({ title: "Renamed" }));

    expect(isDirty(baseline, local)).toBe(false);
  });
});

describe("fieldPristine", () => {
  it("ignores untrimmed title whitespace", () => {
    expect(fieldPristine("title", draft(), draft({ title: " Ship it" }))).toBe(true);
    expect(fieldPristine("title", draft(), draft({ title: "Other" }))).toBe(false);
  });
});
