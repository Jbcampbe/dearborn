<script setup lang="ts">
// Minimal line-art icon set, hand-inlined per the design system (single-color,
// 1.5px stroke, round caps, 16×16 grid). One component keeps the dependency
// footprint at zero; consumers pick by name.
const props = withDefaults(defineProps<{ name: string; size?: number }>(), {
  size: 16,
});

// Each entry is the inner SVG markup for a 16×16, stroke-based icon.
const ICONS: Record<string, string> = {
  home: `<path d="M3 6.8 8 3l5 3.8V12a1 1 0 0 1-1 1H9.8V9.7H6.2V13H4a1 1 0 0 1-1-1V6.8Z"/>`,
  plus: `<path d="M8 3.5v9M3.5 8h9"/>`,
  "chevron-right": `<path d="M6 4l4 4-4 4"/>`,
  "chevron-down": `<path d="M4 6l4 4 4-4"/>`,
  "arrow-left": `<path d="M9.5 3.5 5 8l4.5 4.5M5 8h8"/>`,
  "arrow-right": `<path d="M6.5 3.5 11 8l-4.5 4.5M11 8H3"/>`,
  "arrow-up-right": `<path d="M4.5 11.5 11.5 4.5M5.5 4.5h6v6"/>`,
  x: `<path d="M4 4l8 8M12 4l-8 8"/>`,
  check: `<path d="M3 8.5 6.5 12 13 4.5"/>`,
  refresh: `<path d="M13.5 8a5.5 5.5 0 1 1-1.61-3.89M13.5 2.5v3h-3"/>`,
  repo: `<path d="M4 2.5h8A1.5 1.5 0 0 1 13.5 4v8A1.5 1.5 0 0 1 12 13.5H4A1.5 1.5 0 0 1 2.5 12V4A1.5 1.5 0 0 1 4 2.5ZM6 5.5h4M6 8h4M6 10.5h2.5"/>`,
  box: `<path d="M8 1.8 13.8 4.4v7.2L8 14.2l-5.8-2.6V4.4L8 1.8ZM8 8v6M2.3 4.5 8 7l5.7-2.5"/>`,
  layers: `<path d="M8 2.5 14 5.5 8 8.5 2 5.5l6-3ZM2.7 8.5 8 11l5.3-2.5M2.7 11 8 13.5 13.3 11"/>`,
  board: `<path d="M3 2.5h3v11H3zM9.5 2.5H13v7H9.5z"/>`,
  chat: `<path d="M13.5 8.2c0 2.9-2.5 5.3-5.5 5.3-.6 0-1.2-.1-1.8-.3L2.5 14l.9-3.2c-.6-.9-.9-1.7-.9-2.6C2.5 5.3 5 3 8 3s5.5 2.3 5.5 5.2Z"/>`,
  diagram: `<circle cx="4" cy="4" r="1.8"/><circle cx="12" cy="4" r="1.8"/><circle cx="8" cy="12" r="1.8"/><path d="M5.2 5.4 6.9 10.6M10.8 5.4 9.1 10.6M5.8 4h4.4"/>`,
  send: `<path d="M14 2 7.3 8.7M14 2 9.5 14l-2.2-5.3L2 6.5 14 2Z"/>`,
  trash: `<path d="M2.5 4h11M6.5 4V2.8c0-.4.3-.8.8-.8h1.4c.5 0 .8.4.8.8V4M12.5 4l-.5 9a1 1 0 0 1-1 1H5a1 1 0 0 1-1-1l-.5-9M6.5 7v4M9.5 7v4"/>`,
  pencil: `<path d="M9.7 3.3 12.7 6.3M3 13l.7-3.2L11 2.5a1.4 1.4 0 0 1 2 0l.5.5a1.4 1.4 0 0 1 0 2L6.2 12.3 3 13Z"/>`,
  link: `<path d="M6.5 9.5a3 3 0 0 0 4.2.3l2-2a3 3 0 0 0-4.2-4.2l-1 1M9.5 6.5a3 3 0 0 0-4.2-.3l-2 2a3 3 0 0 0 4.2 4.2l1-1"/>`,
  sparkle: `<path d="M8 2.5 9.4 6.1 13 7.5 9.4 8.9 8 12.5 6.6 8.9 3 7.5 6.6 6.1 8 2.5ZM12.5 10.5l.7 1.8 1.8.7-1.8.7-.7 1.8-.7-1.8-1.8-.7 1.8-.7.7-1.8Z"/>`,
  logout: `<path d="M9.5 2.5H4A1.5 1.5 0 0 0 2.5 4v8A1.5 1.5 0 0 0 4 13.5h5.5M6 8h7.5M11 5.5 13.5 8 11 10.5"/>`,
  search: `<circle cx="7" cy="7" r="4.5"/><path d="M10.5 10.5 14 14"/>`,
  dot: `<circle cx="8" cy="8" r="3" fill="currentColor" stroke="none"/>`,
  warning: `<path d="M8 2.2 14.5 13.5H1.5L8 2.2ZM8 6.5v3.5M8 11.5v.01"/>`,
};
</script>

<template>
  <svg
    :width="props.size"
    :height="props.size"
    viewBox="0 0 16 16"
    fill="none"
    stroke="currentColor"
    stroke-width="1.5"
    stroke-linecap="round"
    stroke-linejoin="round"
    aria-hidden="true"
    class="app-icon"
    v-html="ICONS[props.name] ?? ICONS.dot"
  />
</template>

<style scoped>
.app-icon {
  display: block;
  flex-shrink: 0;
}
</style>
