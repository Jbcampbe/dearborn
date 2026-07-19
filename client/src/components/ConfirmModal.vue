<script setup lang="ts">
import AppModal from "./AppModal.vue";

// Small confirmation dialog for destructive actions (replaces window.confirm).
withDefaults(
  defineProps<{
    open: boolean;
    title: string;
    message: string;
    confirmLabel?: string;
    busy?: boolean;
  }>(),
  { confirmLabel: "Delete", busy: false },
);
const emit = defineEmits<{ confirm: []; cancel: [] }>();
</script>

<template>
  <AppModal :open="open" :title="title" :width="400" @close="emit('cancel')">
    <p class="confirm-message">{{ message }}</p>
    <template #footer>
      <button class="btn" :disabled="busy" @click="emit('cancel')">Cancel</button>
      <button class="btn btn-danger-solid" :disabled="busy" @click="emit('confirm')">
        {{ busy ? "Working…" : confirmLabel }}
      </button>
    </template>
  </AppModal>
</template>

<style scoped>
.confirm-message {
  font-size: var(--text-caption);
  color: var(--text-muted);
  line-height: 1.55;
}

.btn-danger-solid {
  background: var(--color-coral-red);
  color: var(--color-void);
  font-weight: var(--weight-medium);
}

.btn-danger-solid:hover:not(:disabled) {
  background: var(--color-coral-red);
  filter: brightness(1.08);
  color: var(--color-void);
}
</style>
