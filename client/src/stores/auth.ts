import { computed, ref } from "vue";
import { defineStore } from "pinia";

const STORAGE_KEY = "dearborn.token";

/**
 * Auth state for the single-user bearer token.
 *
 * The token is the app's only credential. It is persisted to `localStorage` so
 * it survives reloads, and mirrored into reactive state so the UI can gate the
 * app behind a token-entry screen. A `401` from any API call should call
 * {@link logout} with a message, which clears the token and surfaces the error
 * on the token screen (see the API client / ProjectsView).
 */
export const useAuthStore = defineStore("auth", () => {
  const token = ref<string | null>(localStorage.getItem(STORAGE_KEY));
  /** Set when the last token was rejected, shown on the token-entry screen. */
  const authError = ref<string | null>(null);

  const isAuthenticated = computed(() => token.value !== null);

  /** Store a token (trimmed) and clear any prior auth error. */
  function setToken(value: string): void {
    const trimmed = value.trim();
    if (trimmed.length === 0) {
      return;
    }
    token.value = trimmed;
    localStorage.setItem(STORAGE_KEY, trimmed);
    authError.value = null;
  }

  /**
   * Clear the stored token, sending the user back to token entry. An optional
   * `reason` (e.g. a `401` message) is displayed there.
   */
  function logout(reason?: string): void {
    token.value = null;
    localStorage.removeItem(STORAGE_KEY);
    authError.value = reason ?? null;
  }

  return { token, authError, isAuthenticated, setToken, logout };
});
