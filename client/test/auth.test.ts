// Unit tests for the client auth layer: the session store (`stores/auth.ts`)
// and the transport's refresh-and-retry (`api/client.ts`). No DOM — a minimal
// `localStorage` stub and a recording fetch stub stand in for the browser, and
// Pinia is driven directly (`setActivePinia`), matching the suite's no-browser
// convention elsewhere.

import { beforeEach, describe, expect, it, vi } from "vitest";
import { createPinia, setActivePinia } from "pinia";

import { ApiError, apiFetch, setAuthBridge } from "../src/api/client";
import { useAuthStore } from "../src/stores/auth";

// ---- Stubs ------------------------------------------------------------------

class MemoryStorage implements Storage {
  private map = new Map<string, string>();
  get length(): number {
    return this.map.size;
  }
  key(index: number): string | null {
    return [...this.map.keys()][index] ?? null;
  }
  getItem(key: string): string | null {
    return this.map.has(key) ? (this.map.get(key) as string) : null;
  }
  setItem(key: string, value: string): void {
    this.map.set(key, String(value));
  }
  removeItem(key: string): void {
    this.map.delete(key);
  }
  clear(): void {
    this.map.clear();
  }
}

interface RecordedCall {
  url: string;
  init?: RequestInit;
  authHeader: string | null;
}

/** Install a fetch stub; `respond` maps each call to a response-like object. */
function stubFetch(
  respond: (call: RecordedCall, index: number) =>
    | { status: number; body?: unknown }
    | Promise<{ status: number; body?: unknown }>,
): RecordedCall[] {
  const calls: RecordedCall[] = [];
  let index = 0;
  vi.stubGlobal("fetch", async (url: string, init?: RequestInit) => {
    const call: RecordedCall = {
      url,
      init,
      authHeader:
        init?.headers instanceof Headers
          ? (init.headers.get("Authorization") ?? null)
          : ((init?.headers as Record<string, string> | undefined)?.Authorization ?? null),
    };
    calls.push(call);
    const result = await respond(call, index++);
    const headers = new Headers(init?.headers);
    return {
      ok: result.status >= 200 && result.status < 300,
      status: result.status,
      statusText: result.status === 401 ? "Unauthorized" : "",
      headers,
      json: async () => result.body ?? null,
    };
  });
  return calls;
}

const USER = {
  id: "u-1",
  username: "josiah",
  display_name: "Josiah",
  role: "admin" as const,
  active: true,
};

function envelope(overrides: Partial<Record<string, unknown>> = {}) {
  return {
    access_token: "access-a",
    expires_at: Date.now() + 3_600_000,
    refresh_token: "refresh-r",
    refresh_expires_at: Date.now() + 30 * 24 * 3_600_000,
    user: USER,
    ...overrides,
  };
}

function storedBlob(overrides: Partial<Record<string, unknown>> = {}): string {
  return JSON.stringify({
    accessToken: "access-a",
    accessExpiresAt: Date.now() + 3_600_000,
    refreshToken: "refresh-r",
    user: USER,
    ...overrides,
  });
}

beforeEach(() => {
  vi.unstubAllGlobals();
  vi.stubGlobal("localStorage", new MemoryStorage());
  setActivePinia(createPinia());
});

// ---- bootstrap branches ------------------------------------------------------

describe("auth store bootstrap()", () => {
  it("with no stored blob probes /auth/status and lands on create-admin when unclaimed", async () => {
    const calls = stubFetch(() => ({ status: 200, body: { setup_required: true } }));

    const auth = useAuthStore();
    expect(auth.booting).toBe(true);
    await auth.bootstrap();

    expect(calls.map((c) => c.url)).toEqual(["/auth/status"]);
    expect(auth.setupRequired).toBe(true);
    expect(auth.booting).toBe(false);
    expect(auth.isAuthenticated).toBe(false);
  });

  it("probes /auth/status and shows login once the instance is claimed", async () => {
    stubFetch(() => ({ status: 200, body: { setup_required: false } }));

    const auth = useAuthStore();
    await auth.bootstrap();

    expect(auth.setupRequired).toBe(false);
    expect(auth.booting).toBe(false);
  });

  it("rehydrates a stored blob without any probe", async () => {
    localStorage.setItem("dearborn.auth", storedBlob());
    const calls = stubFetch(() => ({ status: 200, body: { setup_required: false } }));

    const auth = useAuthStore();
    await auth.bootstrap();

    expect(calls).toHaveLength(0); // no /auth/status request
    expect(auth.accessToken).toBe("access-a");
    expect(auth.refreshToken).toBe("refresh-r");
    expect(auth.user?.id).toBe("u-1");
    expect(auth.isAuthenticated).toBe(true);
    expect(auth.booting).toBe(false);
  });

  it("deletes a legacy dearborn.token key at boot", async () => {
    localStorage.setItem("dearborn.token", "stale-shared-token");
    localStorage.setItem("dearborn.auth", storedBlob());

    const auth = useAuthStore();
    await auth.bootstrap();

    expect(localStorage.getItem("dearborn.token")).toBeNull();
    // The new blob survives.
    expect(auth.refreshToken).toBe("refresh-r");
  });

  it("treats a corrupt blob as absent and falls back to the probe", async () => {
    localStorage.setItem("dearborn.auth", "{not json");
    const calls = stubFetch(() => ({ status: 200, body: { setup_required: true } }));

    const auth = useAuthStore();
    await auth.bootstrap();

    expect(calls.map((c) => c.url)).toEqual(["/auth/status"]);
    expect(auth.setupRequired).toBe(true);
  });
});

// ---- setup / login -----------------------------------------------------------

describe("auth store setup()/login()", () => {
  it("setup() applies the session envelope and persists it", async () => {
    const calls = stubFetch((call) => {
      expect(call.url).toBe("/auth/setup");
      return { status: 201, body: envelope({ access_token: "access-s1" }) };
    });

    const auth = useAuthStore();
    await auth.setup("josiah", "Josiah", "long-enough-pass");

    expect(calls).toHaveLength(1);
    expect(auth.accessToken).toBe("access-s1");
    expect(auth.isAuthenticated).toBe(true);
    expect(JSON.parse(localStorage.getItem("dearborn.auth") as string)).toMatchObject({
      accessToken: "access-s1",
      user: { username: "josiah" },
    });
  });

  it("login() surfaces the server's error verbatim", async () => {
    stubFetch(() => ({
      status: 401,
      body: { error: { code: "invalid_credentials", message: "invalid username or password" } },
    }));

    const auth = useAuthStore();
    await expect(auth.login("josiah", "wrong")).rejects.toMatchObject({
      message: "invalid username or password",
      code: "invalid_credentials",
    });
    expect(auth.isAuthenticated).toBe(false);
  });
});

// ---- singleton refresh ---------------------------------------------------------

describe("auth store refresh()", () => {
  async function rehydratedStore(): Promise<ReturnType<typeof useAuthStore>> {
    localStorage.setItem("dearborn.auth", storedBlob());
    const auth = useAuthStore();
    await auth.bootstrap(); // no probe: the stored blob short-circuits it
    return auth;
  }

  it("collapses N concurrent calls into exactly one POST /auth/refresh", async () => {
    const auth = await rehydratedStore();
    const calls = stubFetch(() => ({
      status: 200,
      body: {
        access_token: "access-b",
        expires_at: Date.now() + 3_600_000,
        user: USER,
      },
    }));

    await Promise.all([
      auth.refresh(),
      auth.refresh(),
      auth.refresh(),
      auth.refresh(),
      auth.refresh(),
    ]);

    const refreshCalls = calls.filter((c) => c.url === "/auth/refresh");
    expect(refreshCalls).toHaveLength(1);
    expect(JSON.parse(String(refreshCalls[0].init?.body))).toEqual({
      refresh_token: "refresh-r",
    });
    expect(auth.accessToken).toBe("access-b");
    expect(auth.refreshToken).toBe("refresh-r"); // refresh tokens do not rotate
  });

  it("rejects every concurrent caller when the single refresh fails", async () => {
    const auth = await rehydratedStore();
    const calls = stubFetch(() => ({
      status: 401,
      body: { error: { code: "unauthorized", message: "session expired" } },
    }));

    const results = await Promise.allSettled([auth.refresh(), auth.refresh(), auth.refresh()]);
    expect(results.every((r) => r.status === "rejected")).toBe(true);

    // Still exactly one request — the singleton collapses failures too.
    expect(calls.filter((c) => c.url === "/auth/refresh")).toHaveLength(1);
    // The store keeps its state; onAuthFailure owns the return-to-login flow.
    expect(auth.refreshToken).toBe("refresh-r");
  });
});

// ---- ensureFresh ---------------------------------------------------------------

describe("auth store ensureFresh()", () => {
  function blobExpiringIn(ms: number): string {
    return storedBlob({ accessExpiresAt: Date.now() + ms });
  }

  it("refreshes when the access token expires within the 60s window", async () => {
    localStorage.setItem("dearborn.auth", blobExpiringIn(30_000));
    let auth = useAuthStore();
    await auth.bootstrap();
    const calls = stubFetch(() => ({
      status: 200,
      body: { access_token: "access-fresh", expires_at: Date.now() + 3_600_000, user: USER },
    }));

    const token = await auth.ensureFresh();

    expect(token).toBe("access-fresh");
    expect(calls.filter((c) => c.url === "/auth/refresh")).toHaveLength(1);
  });

  it("does not refresh while the token is comfortably fresh", async () => {
    localStorage.setItem("dearborn.auth", blobExpiringIn(10 * 60_000));
    let auth = useAuthStore();
    await auth.bootstrap();
    const calls = stubFetch(() => ({ status: 200, body: {} }));

    const token = await auth.ensureFresh();

    expect(token).toBe("access-a");
    expect(calls).toHaveLength(0);
  });

  it("returns null when signed out", async () => {
    stubFetch(() => ({ status: 200, body: { setup_required: false } }));
    const auth = useAuthStore();
    await auth.bootstrap();

    expect(await auth.ensureFresh()).toBeNull();
  });
});

// ---- logout ----------------------------------------------------------------------

describe("auth store logout()", () => {
  it("clears storage even when the network call fails", async () => {
    localStorage.setItem("dearborn.auth", storedBlob());
    let auth = useAuthStore();
    await auth.bootstrap();
    vi.stubGlobal("fetch", async () => {
      throw new TypeError("network down");
    });

    await auth.logout();

    expect(localStorage.getItem("dearborn.auth")).toBeNull();
    expect(auth.accessToken).toBeNull();
    expect(auth.user).toBeNull();
    expect(auth.isAuthenticated).toBe(false);
  });

  it("fires POST /auth/logout with the access token, then clears state", async () => {
    localStorage.setItem("dearborn.auth", storedBlob());
    let auth = useAuthStore();
    await auth.bootstrap();
    let sawLogout = false;
    let clearedBeforeLogout = false;
    const calls = stubFetch((call) => {
      if (call.url === "/auth/logout") {
        sawLogout = true;
        clearedBeforeLogout = auth.accessToken === null;
        return { status: 204 };
      }
      return { status: 200, body: {} };
    });

    await auth.logout();

    expect(calls.some((c) => c.url === "/auth/logout" && c.authHeader === "Bearer access-a")).toBe(true);
    expect(sawLogout).toBe(true);
    expect(clearedBeforeLogout).toBe(false); // logout fires before clearing
    expect(auth.isAuthenticated).toBe(false);
  });

  it("clears immediately even with no access token to revoke", async () => {
    const auth = useAuthStore();
    const calls = stubFetch(() => ({ status: 200, body: {} }));

    await auth.logout("session expired");

    expect(calls).toHaveLength(0); // nothing to revoke — no network call
    expect(auth.authError).toBe("session expired");
  });
});

// ---- apiFetch refresh-and-retry ---------------------------------------------------

describe("apiFetch refresh-and-retry", () => {
  interface BridgeHarness {
    accessToken: string | null;
    refreshCalls: number;
    failures: string[];
    failRefreshWith?: Error;
  }

  /** Install a controllable bridge and return its state handle. */
  function installBridge(harness: BridgeHarness): void {
    setAuthBridge({
      getAccessToken: () => harness.accessToken,
      refresh: async () => {
        harness.refreshCalls += 1;
        if (harness.failRefreshWith !== undefined) {
          throw harness.failRefreshWith;
        }
        harness.accessToken = "access-new";
      },
      onAuthFailure: (message) => {
        harness.failures.push(message);
      },
    });
  }

  it("on a 401 performs exactly one refresh and one retry carrying the new token", async () => {
    const harness: BridgeHarness = { accessToken: null, refreshCalls: 0, failures: [] };
    installBridge(harness);

    let attempt = 0;
    const seen: (string | null)[] = [];
    vi.stubGlobal("fetch", async (_url: string, init?: RequestInit) => {
      seen.push(
        init?.headers instanceof Headers ? init.headers.get("Authorization") : null,
      );
      attempt += 1;
      if (attempt === 1) {
        return {
          ok: false,
          status: 401,
          statusText: "Unauthorized",
          json: async () => ({ error: { code: "unauthorized", message: "expired" } }),
        };
      }
      return { ok: true, status: 200, statusText: "OK", json: async () => ({ ok: true }) };
    });
    harness.accessToken = "access-old";

    const result = await apiFetch<{ ok: boolean }>("/projects", "access-old");

    expect(result).toEqual({ ok: true });
    expect(attempt).toBe(2); // one original + one retry
    expect(seen).toEqual(["Bearer access-old", "Bearer access-new"]);
    expect(harness.refreshCalls).toBe(1);
    expect(harness.failures).toHaveLength(0);
  });

  it("when the refresh fails, calls onAuthFailure and rethrows an ApiError with isAuth", async () => {
    const harness: BridgeHarness = {
      accessToken: "access-old",
      refreshCalls: 0,
      failures: [],
      failRefreshWith: new ApiError(401, "unauthorized", "session expired"),
    };
    installBridge(harness);
    let attempts = 0;
    vi.stubGlobal("fetch", async () => {
      attempts += 1;
      return {
        ok: false,
        status: 401,
        statusText: "Unauthorized",
        json: async () => ({ error: { code: "unauthorized", message: "expired" } }),
      };
    });

    const promise = apiFetch("/projects", "access-old");
    await expect(promise).rejects.toMatchObject({ isAuth: true, status: 401 });

    expect(attempts).toBe(1); // no retry after a failed refresh
    expect(harness.refreshCalls).toBe(1);
    expect(harness.failures).toEqual(["session expired"]);
  });

  it("a 401 on the retry does not loop", async () => {
    const harness: BridgeHarness = { accessToken: null, refreshCalls: 0, failures: [] };
    installBridge(harness);
    let attempts = 0;
    vi.stubGlobal("fetch", async () => {
      attempts += 1;
      return {
        ok: false,
        status: 401,
        statusText: "Unauthorized",
        json: async () => ({ error: { code: "unauthorized", message: "expired" } }),
      };
    });
    harness.accessToken = "access-still-bad";

    await expect(apiFetch("/projects", "access-old")).rejects.toMatchObject({ isAuth: true });

    expect(attempts).toBe(2); // original + single retry, then stop
    expect(harness.refreshCalls).toBe(1);
    expect(harness.failures).toHaveLength(0); // the refresh itself succeeded
  });

  it("propagates non-401 failures without touching the bridge", async () => {
    const harness: BridgeHarness = { accessToken: null, refreshCalls: 0, failures: [] };
    installBridge(harness);
    vi.stubGlobal("fetch", async () => ({
      ok: false,
      status: 500,
      statusText: "Internal Server Error",
      json: async () => ({ error: { code: "internal", message: "boom" } }),
    }));

    await expect(apiFetch("/projects", "access-a")).rejects.toMatchObject({
      status: 500,
      isAuth: false,
    });
    expect(harness.refreshCalls).toBe(0);
    expect(harness.failures).toHaveLength(0);
  });
});

// ---- boot/gating wiring -------------------------------------------------------------

describe("App.vue shell keying", () => {
  it("keys AppShell on the user id, not the token, so a refresh never remounts it", async () => {
    const { readFileSync } = await import("node:fs");
    const source = readFileSync(new URL("../src/App.vue", import.meta.url), "utf8");

    expect(source).toContain(':key="auth.user?.id');
    expect(source).not.toContain('auth.token');
  });

  it("shows the splash while booting and AuthGate otherwise", async () => {
    const { readFileSync } = await import("node:fs");
    const source = readFileSync(new URL("../src/App.vue", import.meta.url), "utf8");

    expect(source).toContain('auth.booting');
    expect(source).toContain('AuthGate');
    expect(source).not.toContain('TokenGate');
  });
});
