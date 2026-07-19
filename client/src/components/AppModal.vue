<script setup lang="ts">
import { watch } from "vue";
import AppIcon from "./AppIcon.vue";

// Shared modal surface: teleported overlay, Esc/overlay-click to close, body
// scroll locked while open. Panels are obsidian with a hairline border and the
// system's only real drop shadow (shadow-xl).
const props = withDefaults(
  defineProps<{ open: boolean; title: string; width?: number }>(),
  { width: 460 },
);
const emit = defineEmits<{ close: [] }>();

function onKeydown(event: KeyboardEvent) {
  if (event.key === "Escape") {
    emit("close");
  }
}

watch(
  () => props.open,
  (open) => {
    if (open) {
      document.addEventListener("keydown", onKeydown);
      document.body.style.overflow = "hidden";
    } else {
      document.removeEventListener("keydown", onKeydown);
      document.body.style.overflow = "";
    }
  },
  { immediate: false },
);
</script>

<template>
  <Teleport to="body">
    <Transition name="modal">
      <div
        v-if="open"
        class="modal-overlay"
        role="dialog"
        aria-modal="true"
        :aria-label="title"
        @mousedown.self="emit('close')"
      >
        <div class="modal-panel" :style="{ maxWidth: `${width}px` }">
          <header class="modal-head">
            <h2>{{ title }}</h2>
            <button class="btn btn-icon" aria-label="Close" @click="emit('close')">
              <AppIcon name="x" :size="14" />
            </button>
          </header>
          <div class="modal-body">
            <slot />
          </div>
          <footer v-if="$slots.footer" class="modal-foot">
            <slot name="footer" />
          </footer>
        </div>
      </div>
    </Transition>
  </Teleport>
</template>

<style scoped>
.modal-overlay {
  position: fixed;
  inset: 0;
  z-index: 100;
  display: flex;
  align-items: flex-start;
  justify-content: center;
  padding: 12vh var(--spacing-16) var(--spacing-16);
  background: rgba(4, 5, 6, 0.7);
  backdrop-filter: blur(3px);
  overflow-y: auto;
}

.modal-panel {
  width: 100%;
  background: var(--surface-obsidian);
  border: 1px solid var(--border-hairline);
  border-radius: var(--radius-cards);
  box-shadow: var(--shadow-xl);
}

.modal-head {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: var(--spacing-12);
  padding: var(--spacing-16) var(--spacing-20) 0;
}

.modal-head h2 {
  font-size: var(--text-body-sm);
  font-weight: var(--weight-medium);
}

.modal-body {
  padding: var(--spacing-16) var(--spacing-20);
}

.modal-foot {
  display: flex;
  justify-content: flex-end;
  gap: var(--spacing-8);
  padding: 0 var(--spacing-20) var(--spacing-16);
}

.modal-enter-active,
.modal-leave-active {
  transition: opacity var(--duration-normal) var(--ease-out);
}
.modal-enter-active .modal-panel,
.modal-leave-active .modal-panel {
  transition:
    transform var(--duration-normal) var(--ease-out),
    opacity var(--duration-normal) var(--ease-out);
}
.modal-enter-from,
.modal-leave-to {
  opacity: 0;
}
.modal-enter-from .modal-panel,
.modal-leave-to .modal-panel {
  transform: translateY(6px) scale(0.99);
  opacity: 0;
}
</style>
