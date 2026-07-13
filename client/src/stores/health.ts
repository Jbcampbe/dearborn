import { defineStore } from "pinia";
import { ref } from "vue";

/**
 * Minimal Pinia store proving state management is wired.
 * Later tasks (T-006+) replace this with real app state.
 */
export const useHealthStore = defineStore("health", () => {
  const status = ref<string>("unknown");

  async function check() {
    try {
      const res = await fetch("/health");
      const body = await res.json();
      status.value = body.status ?? "unknown";
    } catch {
      status.value = "unreachable";
    }
  }

  return { status, check };
});
