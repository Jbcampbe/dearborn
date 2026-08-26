<script setup lang="ts">
// Cost graphs for the project Overview tab (Cost Tracking epic). Three
// chart.js charts driven by `GET /projects/{id}/cost`:
//   1. Bar   — tokens/est. cost by agent slot (`AgentSlot::as_str()` keys)
//   2. Bar   — tokens/est. cost by harness/model combination
//   3. Line  — totals per calendar day (only days with runs; no zero-fill)
//
// A single Tokens / Est. cost (USD) toggle switches all three charts at once.
// Estimated cost is server-side API-equivalent pricing from the rate table —
// never an actual bill — so when it is selected we label it as such. Buckets
// whose model has no rate-table entry (`estimated_*_usd === null`) render in a
// muted color rather than as $0.
import { computed, onMounted, ref, watch } from "vue";
import {
  Bar,
  Line,
} from "vue-chartjs";
import {
  BarElement,
  CategoryScale,
  Chart as ChartJS,
  Legend,
  LineElement,
  LinearScale,
  PointElement,
  Tooltip,
  type ChartData,
  type ChartOptions,
} from "chart.js";
import { useAuthStore } from "../stores/auth";
import { ApiError } from "../api/client";
import {
  getProjectCost,
  type CostRow,
  type ProjectCost,
} from "../api/cost";
import AppIcon from "./AppIcon.vue";

ChartJS.register(
  CategoryScale,
  LinearScale,
  BarElement,
  LineElement,
  PointElement,
  Tooltip,
  Legend,
);

const props = defineProps<{ projectId: string }>();

const auth = useAuthStore();
const cost = ref<ProjectCost | null>(null);
const loading = ref(true);
const error = ref<string | null>(null);
/** The single unit driving all three charts. */
const unit = ref<"tokens" | "cost">("tokens");

async function load() {
  const token = auth.accessToken;
  if (token === null) {
    return;
  }
  loading.value = true;
  error.value = null;
  try {
    cost.value = await getProjectCost(token, props.projectId);
    // A project whose models are all missing from the rate table has no
    // coverage at all — fall back to tokens so the toggle is honest.
    if (!hasRateCoverage.value) {
      unit.value = "tokens";
    }
  } catch (err) {
    if (err instanceof ApiError && err.isAuth) {
      auth.logout(`Token rejected (401): ${err.message}. Please re-enter it.`);
      return;
    }
    error.value = err instanceof Error ? err.message : "failed to load cost data";
  } finally {
    loading.value = false;
  }
}

onMounted(load);

// Same reuse caveat as ProjectDetailView: vue-router keeps this component
// alive across /project/:id navigations, so re-fetch on id change.
watch(
  () => props.projectId,
  () => {
    void load();
  },
);

/** True when at least one bucket anywhere has a non-null estimated USD value. */
const hasRateCoverage = computed<boolean>(() => {
  const c = cost.value;
  if (c === null) {
    return false;
  }
  const priced = (r: CostRow) =>
    r.estimated_input_usd !== null || r.estimated_output_usd !== null;
  return (
    c.by_slot.some(priced) ||
    c.by_harness_model.some(priced) ||
    c.by_day.some(priced)
  );
});

/** Empty state: no rows in any bucket means the project has no runs yet. */
const isEmpty = computed<boolean>(() => {
  const c = cost.value;
  if (c === null) {
    return false;
  }
  return (
    c.by_slot.length === 0 &&
    c.by_harness_model.length === 0 &&
    c.by_day.length === 0
  );
});

// ---- design-system colors ----------------------------------------------------
// Charts draw onto a canvas, so CSS custom properties must be resolved to
// concrete values via getComputedStyle before handing them to chart.js.
// Reading them through the variables keeps the charts consistent if the
// palette is ever light/dark themed.

function cssVar(name: string): string {
  return getComputedStyle(document.documentElement).getPropertyValue(name).trim();
}

interface ChartPalette {
  barPrimary: string;
  barSecondary: string;
  line: string;
  muted: string;
  grid: string;
  tick: string;
}

function palette(): ChartPalette {
  return {
    barPrimary: cssVar("--color-signal-teal"),
    barSecondary: cssVar("--color-iris-violet"),
    line: cssVar("--color-acid-lime"),
    muted: cssVar("--color-smoke"),
    grid: cssVar("--border-hairline"),
    tick: cssVar("--text-faint"),
  };
}

/** A bucket is unpriced only when both estimates are null (no rate entry). */
function isUnpriced(row: CostRow): boolean {
  return row.estimated_input_usd === null && row.estimated_output_usd === null;
}

function totalTokens(row: CostRow): number {
  return row.input_tokens + row.output_tokens;
}

function estUsd(row: CostRow): number {
  return (row.estimated_input_usd ?? 0) + (row.estimated_output_usd ?? 0);
}

function bucketValue(row: CostRow): number {
  return unit.value === "tokens" ? totalTokens(row) : estUsd(row);
}

// ---- formatting --------------------------------------------------------------

function formatTokens(n: number): string {
  return n.toLocaleString("en-US");
}

function formatUsd(n: number): string {
  // Rate-derived values are often sub-cent; keep precision below $0.01.
  return `$${n >= 0.01 || n === 0 ? n.toFixed(2) : n.toFixed(4)}`;
}

function formatValue(n: number): string {
  return unit.value === "tokens" ? `${formatTokens(n)} tokens` : formatUsd(n);
}

// ---- chart 1: by agent slot --------------------------------------------------

const slotChartData = computed<ChartData<"bar">>(() => {
  const rows = cost.value?.by_slot ?? [];
  const colors = palette();
  const muted = unit.value === "cost";
  return {
    labels: rows.map((r) => r.slot),
    datasets: [
      {
        data: rows.map(bucketValue),
        backgroundColor: rows.map((r) =>
          muted && isUnpriced(r) ? colors.muted : colors.barPrimary,
        ),
        borderRadius: 2,
      },
    ],
  };
});

// ---- chart 2: by harness/model ----------------------------------------------

const harnessModelChartData = computed<ChartData<"bar">>(() => {
  const colors = palette();
  const muted = unit.value === "cost";
  const rows = [...(cost.value?.by_harness_model ?? [])].sort(
    (a, b) => totalTokens(b) - totalTokens(a),
  );
  return {
    labels: rows.map((r) => `${r.harness ?? "—"}/${r.model ?? "unknown"}`),
    datasets: [
      {
        data: rows.map(bucketValue),
        backgroundColor: rows.map((r) =>
          muted && isUnpriced(r) ? colors.muted : colors.barSecondary,
        ),
        borderRadius: 2,
      },
    ],
  };
});

// ---- chart 3: over time ------------------------------------------------------

const dayChartData = computed<ChartData<"line">>(() => {
  const rows = cost.value?.by_day ?? [];
  const colors = palette();
  const muted = unit.value === "cost";
  return {
    labels: rows.map((r) => r.date),
    datasets: [
      {
        data: rows.map(bucketValue),
        borderColor: colors.line,
        backgroundColor: colors.line,
        pointBackgroundColor: rows.map((r) =>
          muted && isUnpriced(r) ? colors.muted : colors.line,
        ),
        pointRadius: 3,
        tension: 0.25,
      },
    ],
  };
});

// ---- shared options ----------------------------------------------------------

const baseOptions = computed<Record<string, unknown>>(() => {
  const colors = palette();
  const fmt = (n: number | string) =>
    typeof n === "number" ? formatValue(n) : String(n);
  return {
    responsive: true,
    maintainAspectRatio: false,
    plugins: {
      legend: { display: false },
      tooltip: {
        backgroundColor: cssVar("--surface-obsidian"),
        borderColor: cssVar("--border-strong"),
        borderWidth: 1,
        titleColor: cssVar("--text-body"),
        bodyColor: cssVar("--text-body"),
        callbacks: {
          label: (ctx: { parsed: { y: number } }) => fmt(ctx.parsed.y),
        },
      },
    },
    scales: {
      x: {
        ticks: { color: colors.tick },
        grid: { display: false },
      },
      y: {
        beginAtZero: true,
        ticks: { color: colors.tick, callback: fmt },
        grid: { color: colors.grid },
      },
    },
  };
});

const barOptions = computed<ChartOptions<"bar">>(
  () => baseOptions.value as ChartOptions<"bar">,
);
const lineOptions = computed<ChartOptions<"line">>(
  () => baseOptions.value as ChartOptions<"line">,
);

function onUnitClick(next: "tokens" | "cost") {
  unit.value = next;
}
</script>

<template>
  <section class="cost-graphs card card-pad" aria-label="Project cost graphs">
    <div class="section-head">
      <h2>Usage &amp; cost</h2>
      <div class="toggle" role="group" aria-label="Chart units">
        <button
          class="btn btn-ghost toggle-btn"
          :class="{ active: unit === 'tokens' }"
          @click="onUnitClick('tokens')"
        >
          Tokens
        </button>
        <button
          class="btn btn-ghost toggle-btn"
          :class="{ active: unit === 'cost' }"
          :disabled="!hasRateCoverage"
          :title="hasRateCoverage ? undefined : 'No rate-table coverage for this project’s models'"
          @click="onUnitClick('cost')"
        >
          Est. cost (USD)
        </button>
      </div>
    </div>

    <p v-if="unit === 'cost'" class="estimate-note">
      API-equivalent pricing — not your actual bill
    </p>

    <div v-if="loading" class="loading-stack" aria-label="Loading cost data">
      <div class="skeleton sk-block" />
      <div class="skeleton sk-block" />
    </div>
    <p v-else-if="error" class="banner banner-error" role="alert">{{ error }}</p>

    <div v-else-if="isEmpty" class="empty-state">
      <AppIcon name="box" :size="20" />
      <p>No runs yet — cost data appears after your first agent run completes.</p>
    </div>

    <template v-else>
      <div class="chart-block">
        <h3>By agent slot</h3>
        <div class="chart-canvas">
          <Bar :data="slotChartData" :options="barOptions" />
        </div>
      </div>

      <div class="chart-block">
        <h3>By harness / model</h3>
        <div class="chart-canvas">
          <Bar :data="harnessModelChartData" :options="barOptions" />
        </div>
      </div>

      <div class="chart-block">
        <h3>Over time</h3>
        <div class="chart-canvas">
          <Line :data="dayChartData" :options="lineOptions" />
        </div>
      </div>
    </template>
  </section>
</template>

<style scoped>
.cost-graphs {
  margin-bottom: var(--spacing-32);
}

.section-head {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: var(--spacing-16);
}

.toggle {
  display: inline-flex;
  gap: var(--spacing-4);
}

.toggle-btn.active {
  color: var(--text-primary);
  background: var(--surface-slate);
}

.toggle-btn:disabled {
  opacity: 0.45;
  cursor: not-allowed;
}

.estimate-note {
  margin-top: var(--spacing-8);
  font-size: var(--text-label);
  font-style: italic;
  color: var(--text-muted);
}

.loading-stack {
  display: flex;
  flex-direction: column;
  gap: var(--spacing-16);
  margin-top: var(--spacing-16);
}

.sk-block {
  height: 96px;
}

.chart-block {
  margin-top: var(--spacing-24);
}

.chart-block h3 {
  margin-bottom: var(--spacing-8);
  font-size: var(--text-caption);
  font-weight: var(--weight-medium);
  color: var(--text-muted);
}

.chart-canvas {
  position: relative;
  height: 220px;
}
</style>
