<script setup lang="ts">
import { computed } from "vue";

// Linear-style status glyphs on a 14×14 grid: the circle language (dashed =
// draft, hollow = queued, half-filled = active, checked = done) plus the
// octagon for failure states. Tones come from the design system's status hues.
const props = withDefaults(defineProps<{ status: string; size?: number }>(), {
  size: 14,
});

type Glyph = "dashed" | "hollow" | "half" | "check" | "cancel" | "octagon";
type Tone = "violet" | "neutral" | "teal" | "green" | "red" | "dim";

const MAP: Record<string, { glyph: Glyph; tone: Tone }> = {
  // Epic lanes
  Planning: { glyph: "dashed", tone: "violet" },
  Ready: { glyph: "hollow", tone: "neutral" },
  InProgress: { glyph: "half", tone: "teal" },
  Completed: { glyph: "check", tone: "green" },
  Cancelled: { glyph: "cancel", tone: "dim" },
  Blocked: { glyph: "octagon", tone: "red" },
  // Task statuses
  Todo: { glyph: "hollow", tone: "neutral" },
  Done: { glyph: "check", tone: "green" },
  Failed: { glyph: "octagon", tone: "red" },
  // Clone lifecycle
  pending: { glyph: "dashed", tone: "neutral" },
  ready: { glyph: "check", tone: "green" },
  error: { glyph: "octagon", tone: "red" },
};

const entry = computed(() => MAP[props.status] ?? { glyph: "hollow", tone: "neutral" as Tone });
</script>

<template>
  <svg
    :width="props.size"
    :height="props.size"
    viewBox="0 0 14 14"
    fill="none"
    aria-hidden="true"
    class="status-icon"
    :data-tone="entry.tone"
  >
    <!-- dashed circle: draft / pending -->
    <circle
      v-if="entry.glyph === 'dashed'"
      cx="7"
      cy="7"
      r="5.5"
      stroke="currentColor"
      stroke-width="1.5"
      stroke-dasharray="2.4 2"
      stroke-linecap="round"
    />
    <!-- hollow circle: queued / todo -->
    <circle
      v-else-if="entry.glyph === 'hollow'"
      cx="7"
      cy="7"
      r="5.5"
      stroke="currentColor"
      stroke-width="1.5"
    />
    <!-- half-filled circle: active -->
    <template v-else-if="entry.glyph === 'half'">
      <circle cx="7" cy="7" r="5.5" stroke="currentColor" stroke-width="1.5" />
      <path d="M7 1.5A5.5 5.5 0 0 1 7 12.5V1.5Z" fill="currentColor" />
    </template>
    <!-- checked circle: done -->
    <template v-else-if="entry.glyph === 'check'">
      <circle cx="7" cy="7" r="6.25" fill="currentColor" />
      <path
        d="M4.5 7.2 6.3 9 9.5 5.3"
        stroke="var(--color-void)"
        stroke-width="1.5"
        stroke-linecap="round"
        stroke-linejoin="round"
      />
    </template>
    <!-- cancelled: filled circle + slash -->
    <template v-else-if="entry.glyph === 'cancel'">
      <circle cx="7" cy="7" r="6.25" fill="currentColor" />
      <path
        d="M4.7 4.7l4.6 4.6M9.3 4.7l-4.6 4.6"
        stroke="var(--color-void)"
        stroke-width="1.4"
        stroke-linecap="round"
      />
    </template>
    <!-- octagon: blocked / failed -->
    <template v-else-if="entry.glyph === 'octagon'">
      <path
        d="M4.4 1h5.2L13 4.4v5.2L9.6 13H4.4L1 9.6V4.4L4.4 1Z"
        fill="currentColor"
      />
      <path
        d="M7 4.2v3.6M7 9.7v.01"
        stroke="var(--color-void)"
        stroke-width="1.6"
        stroke-linecap="round"
      />
    </template>
  </svg>
</template>

<style scoped>
.status-icon {
  display: block;
  flex-shrink: 0;
}
.status-icon[data-tone="violet"] {
  color: var(--color-lavender);
}
.status-icon[data-tone="neutral"] {
  color: var(--color-fog);
}
.status-icon[data-tone="teal"] {
  color: var(--color-signal-teal);
}
.status-icon[data-tone="green"] {
  color: #4ec96b;
}
.status-icon[data-tone="red"] {
  color: var(--color-coral-red);
}
.status-icon[data-tone="dim"] {
  color: var(--color-ash);
}
</style>
