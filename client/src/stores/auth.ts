import { computed, ref } from "vue";
import { defineStore } from "pinia";

import { ApiError } from "../api/client";

const STORAGE_KEY = "dearborn.auth";
/** Pre-epic single-user credential; deleted on boot so it cannot linger. */
const LEGACY_STORAGE_KEY = "dearborn.token";

/**
 * Refresh when the access token's remaining life falls inside this window.
 * Called out by name so the WS composables' "fresh before every connect"
 * contract (`ensureFresh`) and the store agree on what "fresh" means.
 */
const REFRESH_WINDOW_MS = 60_000;

/** The user object as the server serializes it (never a `password_hash`). */
export interface AuthUser {
  id: string;
  username: string;
  display_name: string;
  role: "admin" | "user";
  active: boolean;
}

interface SessionEnvelope {
  access_token: string;
  expires_at: number;
  refresh_token: string;
  refresh_expires_at: number;
  user: AuthUser;
}

/** `POST /auth/refresh` response — no new refresh token (they do not rotate). */
interface RefreshEnvelope {
  access_token: string;
  expires_at: number;
  user: AuthUser;
}

/** The one JSON blob persisted under `dearborn.auth` across reloads. */
interface StoredSession {
  accessToken: string | null;
  accessExpiresAt: number | null;
  refreshToken: string | null;
  user: AuthUser | null;
}

function readStoredSession(): StoredSession | null {
  const raw = localStorage.getItem(STORAGE_KEY);
  if (raw === null) {
    return null;
  }
  try {
    return JSON.parse(raw) as StoredSession;
  } catch {
    // Corrupt blob — treat as absent; bootstrap() clears it before probing.
    return null;
  }
}

/**
 * Module-level singleton for the in-flight refresh. N concurrent 401s all
 * await the same promise, so exactly one `POST /auth/refresh` goes out; the
 * `.finally` in {@link refresh} clears it once settled.
 */
let refreshInFlight: Promise<void> | null = null;

/**
 * Auth state for named-user sessions.
 *
 * The browser holds two credentials: a short-lived signed **access token**
 * sent as the bearer on every API call, and a long-lived opaque **refresh
 * token** used only against `POST /auth/refresh`. Both live in one JSON blob
 * under `localStorage["dearborn.auth"]` so a session survives reloads without
 * re-prompting for a password.
 *
 * Boot order (see {@link bootstrap}): App.vue shows a neutral splash while
 * `booting`; the store either rehydrates a stored session or probes
 * `/auth/status` to decide between the create-admin form and the login form.
 */
export const useAuthStore = defineStore("auth", () => {
  const accessToken = ref<string | null>(null);
  /** Unix ms when the access token expires (server-issued `expires_at`). */
  const accessExpiresAt = ref<number | null>(null);
  const refreshToken = ref<string | null>(null);
  const user = ref<AuthUser | null>(null);

  /** True until the stored session is rehydrated or the probe resolves. */
  const booting = ref(true);
  /** Server says this instance has zero users → show create-admin, not login. */
  const setupRequired = ref(false);
  /** Set when a session was rejected/expired, shown on the auth screen. */
  const authError = ref<string | null>(null);

  const isAuthenticated = computed(
    () => accessToken.value !== null && user.value !== null,
  );
  const isAdmin = computed(() => user.value?.role === "admin");

  function persist(): void {
    const blob: StoredSession = {
      accessToken: accessToken.value,
      accessExpiresAt: accessExpiresAt.value,
      refreshToken: refreshToken.value,
      user: user.value,
    };
    localStorage.setItem(STORAGE_KEY, JSON.stringify(blob));
  }

  function applyStored(stored: StoredSession): void {
    accessToken.value = stored.accessToken ?? null;
    accessExpiresAt.value = stored.accessExpiresAt ?? null;
    refreshToken.value = stored.refreshToken ?? null;
    user.value = stored.user ?? null;
  }

  function applyEnvelope(envelope: SessionEnvelope): void {
    accessToken.value = envelope.access_token;
    accessExpiresAt.value = envelope.expires_at;
    refreshToken.value = envelope.refresh_token;
    user.value = envelope.user;
    persist();
  }

  function clearSession(): void {
    accessToken.value = null;
    accessExpiresAt.value = null;
    refreshToken.value = null;
    user.value = null;
    localStorage.removeItem(STORAGE_KEY);
  }

  /**
   * Boot the app: delete any stale legacy token, rehydrate a stored session,
   * or probe `/auth/status` to pick the create-admin vs login form. Resolves
   * with `booting === false` either way, releasing App.vue's splash.
   */
  async function bootstrap(): Promise<void> {
    localStorage.removeItem(LEGACY_STORAGE_KEY);

    const stored = readStoredSession();
    if (
      stored !== null &&
      stored.refreshToken != null &&
      stored.user != null
    ) {
      applyStored(stored);
      booting.value = false;
      return;
    }
    if (stored !== null) {
      // A partial/corrupt blob is not a session — start clean.
      clearSession();
    }

    try {
      const response = await fetch("/auth/status");
      const body = (await response.json().catch(() => null)) as {
        setup_required?: boolean;
      } | null;
      setupRequired.value = body?.setup_required === true;
    } catch {
      // Probe failed (server unreachable): fall through to the login form;
      // the first real request will surface the network error itself.
      setupRequired.value = false;
    } finally {
      booting.value = false;
    }
  }

  /** Shared parse of the server's `{ error: { code, message } }` envelope. */
  async function errorFrom(response: Response): Promise<ApiError> {
    const body = (await response.json().catch(() => null)) as {
      error?: { code?: string; message?: string };
    } | null;
    return new ApiError(
      response.status,
      body?.error?.code ?? "unknown",
      body?.error?.message ??
        (response.statusText || "request failed"),
    );
  }

  async function postSession(path: string, body: unknown): Promise<void> {
    let response: Response;
    try {
      response = await fetch(path, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(body),
      });
    } catch {
      throw new ApiError(0, "network_error", "could not reach the server");
    }
    if (!response.ok) {
      throw await errorFrom(response);
    }
    applyEnvelope((await response.json()) as SessionEnvelope);
  }

  /**
   * Claim an unclaimed instance: create the first admin and sign straight in.
   * Throws the server's `ApiError` verbatim (e.g. the 12-character minimum).
   */
  async function setup(
    username: string,
    displayName: string,
    password: string,
  ): Promise<void> {
    await postSession("/auth/setup", {
      username,
      display_name: displayName,
      password,
    });
  }

  /** Username + password login. Throws the server's generic failure message. */
  async function login(username: string, password: string): Promise<void> {
    await postSession("/auth/login", { username, password });
  }

  /**
   * Exchange the refresh token for a fresh access token.
   *
   * Guarded by the module-level singleton promise: concurrent callers share
   * one in-flight request instead of stampeding the endpoint.
   */
  function refresh(): Promise<void> {
    if (refreshInFlight === null) {
      refreshInFlight = doRefresh().finally(() => {
        refreshInFlight = null;
      });
    }
    return refreshInFlight;
  }

  async function doRefresh(): Promise<void> {
    const token = refreshToken.value;
    if (token === null) {
      throw new ApiError(401, "unauthorized", "not signed in");
    }
    let response: Response;
    try {
      response = await fetch("/auth/refresh", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ refresh_token: token }),
      });
    } catch {
      throw new ApiError(0, "network_error", "could not reach the server");
    }
    if (!response.ok) {
      // Deliberately not clearing local state here: apiFetch's failure path
      // routes through onAuthFailure, which owns the "return to login" flow.
      throw await errorFrom(response);
    }
    const envelope = (await response.json()) as RefreshEnvelope;
    accessToken.value = envelope.access_token;
    accessExpiresAt.value = envelope.expires_at;
    user.value = envelope.user;
    persist();
  }

  /**
   * The access token, refreshed first if it expires within
   * {@link REFRESH_WINDOW_MS}. WS composables call this before each connect
   * attempt so a reconnect after idle presents a live token.
   */
  async function ensureFresh(): Promise<string | null> {
    if (!isAuthenticated.value) {
      return null;
    }
    const expiresAt = accessExpiresAt.value;
    if (expiresAt !== null && expiresAt - Date.now() < REFRESH_WINDOW_MS) {
      await refresh();
    }
    return accessToken.value;
  }

  /**
   * End the session: fire `POST /auth/logout` best-effort, then clear local
   * state regardless of the result. An optional `reason` (e.g. a rejected
   * session's message) is displayed on the auth screen.
   */
  async function logout(reason?: string): Promise<void> {
    const token = accessToken.value;
    if (token !== null) {
      try {
        await fetch("/auth/logout", {
          method: "POST",
          headers: { Authorization: `Bearer ${token}` },
        });
      } catch {
        // Best-effort only — the local session dies regardless.
      }
    }
    clearSession();
    authError.value = reason ?? null;
  }

  return {
    accessToken,
    accessExpiresAt,
    refreshToken,
    user,
    setupRequired,
    booting,
    authError,
    isAuthenticated,
    isAdmin,
    bootstrap,
    setup,
    login,
    refresh,
    ensureFresh,
    logout,
  };
});
