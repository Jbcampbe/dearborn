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

/** The eight agent slots, canonical order (server `AgentSlot::ALL`). */
export const AGENT_SLOTS = [
  "planning_product",
  "planning_technical",
  "breakdown",
  "implement",
  "fix",
  "review",
  "verify_complete",
  "summarize",
] as const;

/** One agent slot's stable snake_case key. */
export type AgentSlot = (typeof AGENT_SLOTS)[number];

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

/** `GET /projects/{id}/agent-settings` → all eight slots in canonical order. */
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
 * Human-readable slot labels for the settings UI, keyed by slot key. Kept
 * next to `AGENT_SLOTS` so the two can never drift apart silently — a new
 * server slot without a label still renders (with its raw key) via fallback.
 */
export const SLOT_LABELS: Record<AgentSlot, string> = {
  planning_product: "Planning — product",
  planning_technical: "Planning — technical",
  breakdown: "Breakdown",
  implement: "Implement",
  fix: "Fix loop",
  review: "Review",
  verify_complete: "Verify complete",
  summarize: "Summarize",
};
