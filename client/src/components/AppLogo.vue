<script setup lang="ts">
// Deerborn wordmark lockup. The glyph is a geometric "antler" mark — a stem
// forking into two branches tipped with nodes, deliberately echoing a git
// graph. Rendered in void ink on the acid-lime tile (the brand's primary
// lockup); `bare` renders the glyph alone in currentColor.
withDefaults(defineProps<{ size?: number; tile?: boolean }>(), {
  size: 20,
  tile: true,
});
</script>

<template>
  <span class="logo" :style="{ '--logo-size': `${size}px` }">
    <svg
      v-if="tile"
      :width="size"
      :height="size"
      viewBox="0 0 20 20"
      fill="none"
      aria-hidden="true"
      class="logo-tile"
    >
      <rect width="20" height="20" rx="5" fill="var(--color-acid-lime)" />
      <g stroke="var(--color-void)" stroke-width="1.7" stroke-linecap="round">
        <path d="M10 15.8V9.8" />
        <path d="M10 9.8 5.4 5.8" />
        <path d="M10 9.8l4.6-4" />
      </g>
      <circle cx="5" cy="4.8" r="1.5" fill="var(--color-void)" />
      <circle cx="15" cy="4.8" r="1.5" fill="var(--color-void)" />
    </svg>
    <svg
      v-else
      :width="size"
      :height="size"
      viewBox="0 0 20 20"
      fill="none"
      aria-hidden="true"
    >
      <g stroke="currentColor" stroke-width="1.7" stroke-linecap="round">
        <path d="M10 17.8V11" />
        <path d="M10 11 5 6.6" />
        <path d="M10 11l5-4.4" />
      </g>
      <circle cx="4.6" cy="5.4" r="1.6" fill="currentColor" />
      <circle cx="15.4" cy="5.4" r="1.6" fill="currentColor" />
    </svg>
  </span>
</template>

<style scoped>
.logo {
  display: inline-flex;
  width: var(--logo-size);
  height: var(--logo-size);
  flex-shrink: 0;
}
</style>
