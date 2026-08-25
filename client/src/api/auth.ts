// Auth self-service REST surface. Session establishment (login/setup/refresh)
// lives in the auth store — it must own the envelope handling — so this module
// only covers the authenticated, non-session-shaping calls.

import { apiFetch } from "./client";

/** `POST /auth/password` body — self-service password change. */
export interface ChangePasswordInput {
  current_password: string;
  new_password: string;
}

/**
 * `POST /auth/password` — change the caller's own password. Resolves on
 * `204`; the server keeps the calling session alive and revokes every other
 * one, so the browser stays logged in. Throws the server's `ApiError`
 * verbatim: a wrong current password and a too-short new password are both
 * server-authoritative messages the caller surfaces inline.
 */
export function changePassword(
  token: string,
  input: ChangePasswordInput,
): Promise<void> {
  return apiFetch("/auth/password", token, {
    method: "POST",
    body: JSON.stringify(input),
  });
}
