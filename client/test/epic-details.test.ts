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
    description: null,
    destination: "A working exporter, end to end",
    notes: null,
    status: "Planning",
    created_at: 1,
    updated_at: 1,
    ...overrides,
  };
}

function draft(overrides: Partial<EpicDraft> = {}): EpicDraft {
  return {
    title: "Ship it",
    description: "",
    ...overrides,
  };
}

describe("draftFromEpic", () => {
  it("maps a null description to an empty string for editing", () => {
    expect(draftFromEpic(epic())).toEqual(draft());
    expect(draftFromEpic(epic({ description: "A blurb" }))).toEqual(
      draft({ description: "A blurb" }),
    );
  });
});

describe("diffEpicEdits", () => {
  it("returns an empty body when nothing changed", () => {
    expect(diffEpicEdits(draft(), draft())).toEqual({});
    expect(isDirty(draft(), draft())).toBe(false);
  });

  it("emits only the changed fields", () => {
    const body = diffEpicEdits(draft(), draft({ description: "A blurb" }));
    expect(body).toEqual({ description: "A blurb" });
  });

  it("trims a changed title (the server trims on save too)", () => {
    const body = diffEpicEdits(draft(), draft({ title: "  Renamed  " }));
    expect(body).toEqual({ title: "Renamed" });
  });

  it("treats trailing-whitespace-only title edits as unchanged", () => {
    expect(diffEpicEdits(draft(), draft({ title: "Ship it " }))).toEqual({});
  });

  it("maps an emptied description back to null (clears the column)", () => {
    const body = diffEpicEdits(
      draft({ description: "A blurb" }),
      draft({ description: "" }),
    );
    expect(body).toEqual({ description: null });
  });

  it("never touches destination/notes (the map workflow owns them)", () => {
    const body = diffEpicEdits(draft(), draft({ description: "x" }));
    expect(Object.keys(body)).not.toContain("destination");
    expect(Object.keys(body)).not.toContain("notes");
  });
});

describe("applyLiveEpic", () => {
  it("moves pristine fields to the incoming server values", () => {
    const baseline = draft();
    const local = draft();
    applyLiveEpic(baseline, local, epic({ title: "Agent rename", description: "ctx" }));

    expect(local).toEqual(draft({ title: "Agent rename", description: "ctx" }));
    expect(baseline).toEqual(local);
  });

  it("never clobbers an unsaved local edit, and the field stays dirty", () => {
    const baseline = draft();
    const local = draft({ description: "my unsaved edit" });
    applyLiveEpic(
      baseline,
      local,
      epic({ title: "Agent rename", description: "agent overwrite" }),
    );

    // The dirty field kept the local edit; the pristine title followed the server.
    expect(local.description).toBe("my unsaved edit");
    expect(local.title).toBe("Agent rename");
    // The baseline moved to the server value, so the field is still dirty.
    expect(baseline.description).toBe("agent overwrite");
    expect(isDirty(baseline, local)).toBe(true);
    expect(diffEpicEdits(baseline, local)).toEqual({ description: "my unsaved edit" });
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
  it("trims the title comparison but compares other fields verbatim", () => {
    expect(fieldPristine("title", draft(), draft({ title: "Ship it " }))).toBe(true);
    expect(fieldPristine("title", draft(), draft({ title: "Renamed" }))).toBe(false);
    expect(fieldPristine("description", draft(), draft({ description: "x" }))).toBe(false);
    expect(fieldPristine("description", draft(), draft())).toBe(true);
  });
});
