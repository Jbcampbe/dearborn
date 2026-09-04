// Agent-settings REST surface (design doc §7) consumed by the Settings UI.
//
// Mirrors `projects.ts`/`epics.ts`: typed DTOs matching the server's shapes
// (see `dearborn-server/src/agent_settings.rs`) wrapped around the generic
// `apiFetch`. Slot keys are a closed string-literal union mirroring the
// server's `AgentSlot` enum — a new slot arrives with a server change and a
// one-word addition here.
//
// Update bodies lean on JSON semantics: an **absent** key leaves that facet
// untouched, an explicit **`null`** clears the override (= reset to inherited,
// design §6 "reset = write NULL"). `JSON.stringify` drops `undefined` keys but
// keeps `null` ones, which is exactly the wire contract — build bodies with
// `undefined` for "don't touch" and `null` for "clear".

import { apiFetch, type Collection } from "./client";

/** The agent slots, canonical order (server `AgentSlot::ALL`). */
export const AGENT_SLOTS = [
  "breakdown",
  "implement",
  "fix",
  "review",
  "verify_complete",
  "summarize",
  "triage",
] as const;

/** One agent slot's stable snake_case key. */
export type AgentSlot = (typeof AGENT_SLOTS)[number];

/**
 * Harness keys Dearborn has an adapter for, in picker order. Mirrors the
 * server's `agent_settings::SUPPORTED_HARNESSES`. Anything else is storable —
 * the settings schema stays open — but fails at spawn, so the UI marks these
 * as the ones that actually run.
 */
export const SUPPORTED_HARNESSES = ["claude", "pi"] as const;

/**
 * Harnesses whose engine is currently wired to the Claude Code adapter.
 * Mirrors the server's `agent_settings::PLANNING_CAPABLE_HARNESSES`: the
 * breakdown run engine is Claude-Code-bound today — the `dearborn` CLI it
 * calls back through is itself harness-agnostic, and pi gains the engine when
 * the per-node planning engines land.
 */
export const PLANNING_CAPABLE_HARNESSES = ["claude"] as const;

/**
 * The slots that are Claude-Code-bound: breakdown calls back into the server
 * through the harness-agnostic `dearborn` CLI, but its engine itself runs on
 * the Claude Code adapter. The task-stage slots act only on their workspace
 * and run on every supported harness. (The per-node planning engines pick up
 * their own slots when they land.)
 */
export const MCP_BOUND_SLOTS: readonly AgentSlot[] = ["breakdown"];

/**
 * Whether `harness` can run `slot`. The client-side mirror of the server's
 * `harness_supports_slot`, so a picker never offers a combination the API
 * will reject with a 400. Unknown harness keys are permitted here for the
 * same reason the server permits them: Dearborn makes no capability claims
 * about a key it has no adapter for.
 */
export function harnessSupportsSlot(harness: string, slot: AgentSlot): boolean {
  if (!(SUPPORTED_HARNESSES as readonly string[]).includes(harness)) {
    return true;
  }
  return (
    !MCP_BOUND_SLOTS.includes(slot) ||
    (PLANNING_CAPABLE_HARNESSES as readonly string[]).includes(harness)
  );
}

/**
 * Whether `harness` may be the **global default**. Every slot without an
 * override inherits the default, so a default that cannot run some slot would
 * break it silently — the server refuses one with a 400 and so does the
 * picker.
 */
export function harnessCanBeDefault(harness: string): boolean {
  return AGENT_SLOTS.every((slot) => harnessSupportsSlot(harness, slot));
}

/**
 * Global agent settings (`global_settings` singleton). `default_models` maps
 * harness key → model id; a `null` model means "let that CLI use its own
 * configured default". Missing map key = CLI default too.
 */
export interface GlobalSettings {
  default_harness: string;
  default_models: Record<string, string | null>;
  enabled_harnesses: string[];
}

/**
 * `PUT /settings` body. Every field optional: absent → keep the stored value,
 * present → replace it. Validation errors (default not enabled, empty enabled
 * set) come back as 400s; disabling a referenced harness is a 409 whose
 * message lists the referencing slots.
 */
export interface UpdateGlobalSettingsBody {
  default_harness?: string;
  default_models?: Record<string, string | null>;
  enabled_harnesses?: string[];
}

/** Where a slot's instruction prompt came from. */
export type PromptSource = "override" | "default";

/** The config a stage run actually uses after folding globals + overrides. */
export interface EffectiveConfig {
  harness: string;
  /** Model passed verbatim to the CLI; `null` → the CLI's own default. */
  model: string | null;
  prompt_source: PromptSource;
}

/**
 * One slot's settings as rendered by the API: the raw override facets plus
 * the server-resolved effective config. An absent override row renders as
 * all-`null` raw fields with a fully-resolved `effective`.
 */
export interface SlotSetting {
  slot: AgentSlot;
  harness: string | null;
  model: string | null;
  system_prompt: string | null;
  /** The slot's compiled default instruction text (editor prefill source). */
  default_prompt: string;
  effective: EffectiveConfig;
}

/**
 * `PUT /projects/{id}/agent-settings/{slot}` body. Absent key = untouched,
 * `null` = clear that facet (= reset to inherited), value = set it. Storing
 * whitespace-only prompts is treated as cleared by the server.
 */
export interface UpdateAgentSettingBody {
  harness?: string | null;
  model?: string | null;
  system_prompt?: string | null;
}

/** `GET /settings` → the global agent settings singleton. */
export function getGlobalSettings(token: string): Promise<GlobalSettings> {
  return apiFetch<GlobalSettings>("/settings", token);
}

/** `PUT /settings` → the merged + validated globals (200). */
export function updateGlobalSettings(
  token: string,
  body: UpdateGlobalSettingsBody,
): Promise<GlobalSettings> {
  return apiFetch<GlobalSettings>("/settings", token, {
    method: "PUT",
    body: JSON.stringify(body),
  });
}

/** `GET /projects/{id}/agent-settings` → all slots in canonical order. */
export async function listProjectAgentSettings(
  token: string,
  projectId: string,
): Promise<SlotSetting[]> {
  const data = await apiFetch<Collection<SlotSetting>>(
    `/projects/${encodeURIComponent(projectId)}/agent-settings`,
    token,
  );
  return data.items;
}

/**
 * `PUT /projects/{id}/agent-settings/{slot}` → the slot's view after applying
 * the partial update (200). Unknown slot or project → 404.
 */
export function updateProjectAgentSetting(
  token: string,
  projectId: string,
  slot: AgentSlot,
  body: UpdateAgentSettingBody,
): Promise<SlotSetting> {
  return apiFetch<SlotSetting>(
    `/projects/${encodeURIComponent(projectId)}/agent-settings/${encodeURIComponent(slot)}`,
    token,
    {
      method: "PUT",
      body: JSON.stringify(body),
    },
  );
}

/**
 * Map form-input state onto a partial-update facet: a blank (whitespace-only)
 * input means "clear this override" (`null`), anything else is the trimmed
 * value. Used by the slot cards: their harness / model inputs are always-
 * present text fields, so blank → `null` (reset) is the natural mapping onto
 * the double-option wire shape.
 */
export function blankClears(value: string): string | null {
  const trimmed = value.trim();
  return trimmed.length > 0 ? trimmed : null;
}

/**
 * Prompt-editor save semantics for one slot (design §4's reset=clear rule).
 * Returns the `system_prompt` facet to send in the PUT body:
 * - blank draft → `""` (server trims; explicit-empty shape as before);
 * - editing a **default-source** slot whose text is unchanged from the served
 *   default → `null`, so an open-tweak-nothing-save pass never freezes
 *   today's built-in text as an override (the slot keeps receiving future
 *   built-in prompt updates);
 * - anything else → the trimmed text as an explicit override.
 */
export function promptSaveValue(draft: string, slot: SlotSetting): string | null {
  const trimmed = draft.trim();
  if (trimmed.length === 0) {
    return "";
  }
  const hadOverride = slot.system_prompt !== null;
  const unchangedFromDefault =
    !hadOverride && trimmed === slot.default_prompt.trim();
  return unchangedFromDefault ? null : trimmed;
}

/**
 * Human-readable slot labels for the settings UI, keyed by slot key. Kept
 * next to `AGENT_SLOTS` so the two can never drift apart silently — a new
 * server slot without a label still renders (with its raw key) via fallback.
 */
export const SLOT_LABELS: Record<AgentSlot, string> = {
  breakdown: "Breakdown",
  implement: "Implement",
  fix: "Fix loop",
  review: "Review",
  verify_complete: "Verify complete",
  summarize: "Summarize",
  triage: "PR triage",
};
