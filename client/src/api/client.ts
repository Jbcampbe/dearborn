// Thin fetch wrapper for the Dearborn REST API.
//
// Attaches `Authorization: Bearer <token>` to every call and understands the
// server's envelopes (CONVENTIONS.md): collections come back as `{ items: [] }`
// and every error as `{ error: { code, message } }`. Non-2xx responses are
// turned into a thrown `ApiError` so callers `try/catch` instead of inspecting
// status codes by hand; a `401` is flagged via `isAuth` so the UI can bounce
// the user back to token entry.

/** A structured API failure carrying the server's stable error `code`. */
export class ApiError extends Error {
  readonly status: number;
  readonly code: string;

  constructor(status: number, code: string, message: string) {
    super(message);
    this.name = "ApiError";
    this.status = status;
    this.code = code;
  }

  /** True when the request was rejected for auth reasons (missing/bad token). */
  get isAuth(): boolean {
    return this.status === 401;
  }
}

/** Shape of a collection response, e.g. `GET /projects`. */
export interface Collection<T> {
  items: T[];
}

/**
 * How the API layer reaches the auth store without importing it (a store →
 * api → store import cycle). `main.ts` installs the real bridge at boot via
 * {@link setAuthBridge}; until then 401s simply propagate as before.
 */
export interface AuthBridge {
  /** The current access token, or null when signed out. */
  getAccessToken: () => string | null;
  /** Refresh the access token. Concurrent callers share one request. */
  refresh: () => Promise<void>;
  /** Invoked once when a session is definitively dead — return to login. */
  onAuthFailure: (message: string) => void;
}

let authBridge: AuthBridge | null = null;

/** Install the auth bridge. Called once from `main.ts` at boot. */
export function setAuthBridge(bridge: AuthBridge): void {
  authBridge = bridge;
}

async function request<T>(
  path: string,
  token: string,
  init: RequestInit,
): Promise<T> {
  const response = await requestResponse(path, token, init);

  if (response.status === 204) {
    return undefined as T;
  }

  const body = await response.json().catch(() => null);

  if (!response.ok) {
    throw toApiError(response.status, body);
  }

  return body as T;
}

/** Map a non-2xx response (with the server's error envelope, if any) to `ApiError`. */
function toApiError(
  status: number,
  body: { error?: { code?: string; message?: string } } | null,
): ApiError {
  const err = body?.error;
  return new ApiError(
    status,
    err?.code ?? "unknown",
    err?.message ?? "request failed",
  );
}

/**
 * Perform one authenticated request and return the raw `Response` — the
 * shared transport behind {@link apiFetch} (JSON bodies) and
 * {@link apiFetchText} (text bodies, e.g. a prototype artifact's HTML).
 * Network failures become a thrown `ApiError(0, "network_error")`; non-2xx
 * responses are turned into `ApiError`s from the server's error envelope
 * (the body is consumed here — the caller never sees those responses).
 */
async function requestResponse(
  path: string,
  token: string,
  init: RequestInit,
): Promise<Response> {
  const headers = new Headers(init.headers);
  headers.set("Authorization", `Bearer ${token}`);
  if (init.body !== undefined && !headers.has("Content-Type")) {
    headers.set("Content-Type", "application/json");
  }

  let response: Response;
  try {
    response = await fetch(path, { ...init, headers });
  } catch (cause) {
    throw new ApiError(0, "network_error", "could not reach the server");
  }

  if (!response.ok && response.status !== 204) {
    const body = await response.json().catch(() => null);
    throw toApiError(response.status, body);
  }

  return response;
}

/**
 * Perform an authenticated JSON request and return the parsed body.
 *
 * On a `401`, runs the single refresh-and-retry: ask the auth bridge to
 * refresh, then retry exactly once with the new token. If the refresh fails,
 * notify {@link AuthBridge.onAuthFailure} (which returns the SPA to login)
 * and rethrow the original {@link ApiError} — its `isAuth` flag still drives
 * each calling component's existing error branch. A second `401` on the
 * retry propagates directly; there is no loop.
 *
 * Throws {@link ApiError} on any non-2xx response (or a network failure). A
 * `204 No Content` resolves to `undefined`.
 */
export async function apiFetch<T>(
  path: string,
  token: string,
  init: RequestInit = {},
): Promise<T> {
  try {
    return await request<T>(path, token, init);
  } catch (cause) {
    if (!(cause instanceof ApiError) || cause.status !== 401) {
      throw cause;
    }
    const bridge = authBridge;
    if (bridge === null) {
      throw cause;
    }
    try {
      await bridge.refresh();
    } catch (refreshCause) {
      bridge.onAuthFailure(
        refreshCause instanceof Error
          ? refreshCause.message
          : String(refreshCause),
      );
      throw cause;
    }
    const freshToken = bridge.getAccessToken();
    if (freshToken === null) {
      bridge.onAuthFailure(cause.message);
      throw cause;
    }
    // One retry only — `request` never recurses into this logic.
    return request<T>(path, freshToken, init);
  }
}

/**
 * Perform an authenticated request and return the body as **text** — for the
 * rare non-JSON endpoint (the prototype artifact read that feeds the
 * sandboxed iframe). Same 401 refresh-and-retry-once behavior as
 * {@link apiFetch}.
 */
export async function apiFetchText(
  path: string,
  token: string,
  init: RequestInit = {},
): Promise<string> {
  const read = async (t: string): Promise<string> => {
    const response = await requestResponse(path, t, init);
    return response.text();
  };
  try {
    return await read(token);
  } catch (cause) {
    if (!(cause instanceof ApiError) || cause.status !== 401) {
      throw cause;
    }
    const bridge = authBridge;
    if (bridge === null) {
      throw cause;
    }
    try {
      await bridge.refresh();
    } catch (refreshCause) {
      bridge.onAuthFailure(
        refreshCause instanceof Error
          ? refreshCause.message
          : String(refreshCause),
      );
      throw cause;
    }
    const freshToken = bridge.getAccessToken();
    if (freshToken === null) {
      bridge.onAuthFailure(cause.message);
      throw cause;
    }
    // One retry only — `read` never recurses into this logic.
    return read(freshToken);
  }
}
