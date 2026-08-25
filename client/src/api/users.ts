// Admin user-management REST surface consumed by the Users screen.
//
// Mirrors `settings.ts`/`projects.ts`: typed DTOs matching the server's shapes
// (see `dearborn-server/src/users.rs`) wrapped around the generic `apiFetch`.
// Every route here is admin-only server-side (`AdminUser`); a regular user's
// token gets `403 forbidden` — never re-implemented client-side.

import { apiFetch, type Collection } from "./client";

/** A user as the API serializes it — conspicuously without a password hash. */
export interface User {
  id: string;
  username: string;
  display_name: string;
  role: "admin" | "user";
  active: boolean;
}

/** `POST /users` body — create a user with an initial password. */
export interface CreateUserInput {
  username: string;
  display_name: string;
  password: string;
  role: "admin" | "user";
}

/**
 * `PATCH /users/:id` body. Every field optional: absent → untouched. The
 * lockout guards are server-authoritative; deactivating or demoting the last
 * active admin (and self-deactivation) come back as `409`s whose messages the
 * UI surfaces verbatim.
 */
export interface UpdateUserInput {
  display_name?: string;
  role?: "admin" | "user";
  active?: boolean;
}

/** `GET /users` → every user, active and inactive, ordered by username. */
export async function listUsers(token: string): Promise<User[]> {
  const data = await apiFetch<Collection<User>>("/users", token);
  return data.items;
}

/** `POST /users` → the created user (201). */
export function createUser(
  token: string,
  input: CreateUserInput,
): Promise<User> {
  return apiFetch<User>("/users", token, {
    method: "POST",
    body: JSON.stringify(input),
  });
}

/** `PATCH /users/:id` → the updated user (200). */
export function updateUser(
  token: string,
  id: string,
  input: UpdateUserInput,
): Promise<User> {
  return apiFetch<User>(`/users/${encodeURIComponent(id)}`, token, {
    method: "PATCH",
    body: JSON.stringify(input),
  });
}

/**
 * `POST /users/:id/password` → `204`. The server revokes all of the target's
 * sessions; the admin communicates the new password out of band.
 */
export function resetUserPassword(
  token: string,
  id: string,
  password: string,
): Promise<void> {
  return apiFetch<void>(`/users/${encodeURIComponent(id)}/password`, token, {
    method: "POST",
    body: JSON.stringify({ password }),
  });
}
