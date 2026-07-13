// Thin fetch wrapper for the Deerborn REST API.
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
 * Perform an authenticated JSON request and return the parsed body.
 *
 * Throws {@link ApiError} on any non-2xx response (or a network failure). A
 * `204 No Content` resolves to `undefined`.
 */
export async function apiFetch<T>(
  path: string,
  token: string,
  init: RequestInit = {},
): Promise<T> {
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

  if (response.status === 204) {
    return undefined as T;
  }

  const body = await response.json().catch(() => null);

  if (!response.ok) {
    const err = (body as { error?: { code?: string; message?: string } } | null)?.error;
    throw new ApiError(
      response.status,
      err?.code ?? "unknown",
      err?.message ?? (response.statusText || "request failed"),
    );
  }

  return body as T;
}
