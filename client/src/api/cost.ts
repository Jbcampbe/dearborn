// Project cost REST surface (Cost Tracking epic) consumed by the project
// overview cost graphs. Mirrors `board.ts`/`tasks.ts`: typed DTOs matching the
// server's shapes (see `dearborn-server/src/cost.rs`) wrapped around the
// generic `apiFetch`.
//
// Estimated USD comes from the server's static rate table and is
// API-equivalent pricing, never an actual bill. A `null` estimate means the
// bucket's model coverage was incomplete — callers must render that as "no
// rate-table entry" (muted bar / "?" label), not as $0.

import { apiFetch } from "./client";

/**
 * Token sums plus estimated USD for one aggregation bucket. The estimated
 * fields are serialized JSON `null` when the bucket contains runs whose model
 * is unknown or missing from the server's rate table.
 */
export interface CostRow {
  input_tokens: number;
  output_tokens: number;
  estimated_input_usd: number | null;
  estimated_output_usd: number | null;
}

/** One `by_slot` row: all closed successful runs of one agent slot, summed. */
export interface CostBySlot extends CostRow {
  /** Agent-slot key in the server's `AgentSlot::as_str()` vocabulary (`"implement"`, …). */
  slot: string;
}

/** One `by_harness_model` row. Either field may be `null` for rows predating those columns; NULL groups as its own bucket. */
export interface CostByHarnessModel extends CostRow {
  harness: string | null;
  model: string | null;
}

/** One `by_day` row: calendar day of run completion (`YYYY-MM-DD`), ascending. */
export interface CostByDay extends CostRow {
  date: string;
}

/** All three aggregations for one project — `GET /projects/{id}/cost`. */
export interface ProjectCost {
  by_slot: CostBySlot[];
  by_harness_model: CostByHarnessModel[];
  by_day: CostByDay[];
}

/** `GET /projects/{id}/cost` → token + estimated-cost totals for the project. */
export function getProjectCost(token: string, projectId: string): Promise<ProjectCost> {
  return apiFetch<ProjectCost>(
    `/projects/${encodeURIComponent(projectId)}/cost`,
    token,
  );
}
