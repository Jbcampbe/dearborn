-- Multi-user authentication (technical plan §3): named users, each with their
-- own username + password login, plus the refresh-token sessions they hold.
-- Replaces the single shared `DEARBORN_TOKEN`. An existing installation picks
-- this up at boot with zero users and lands on the create-admin screen — the
-- intended alpha upgrade path (no backfill, no compatibility shim).

CREATE TABLE user (
  id            TEXT PRIMARY KEY,                    -- ulid
  -- The login identifier. `COLLATE NOCASE` on the column makes both the UNIQUE
  -- index and every `WHERE username = ?1` lookup case-insensitive in one
  -- stroke, so there is no shadow `username_ci` column to keep in sync.
  -- SQLite's NOCASE folds **ASCII only**: `Josiah` and `josiah` collide, but
  -- `Ä` and `ä` do not. That is the correct trade for a login identifier —
  -- full Unicode case folding is locale-dependent and would make "which
  -- account am I logging into" ambiguous.
  username      TEXT NOT NULL COLLATE NOCASE UNIQUE,
  -- Human-facing name, separate from the login id; what future authorship and
  -- comments render.
  display_name  TEXT NOT NULL,
  password_hash TEXT NOT NULL,                       -- argon2id PHC string
  role          TEXT NOT NULL CHECK (role IN ('admin','user')),
  -- 0/1. Deactivation flips this flag; `user` rows are **never deleted**, so
  -- future authorship foreign keys keep resolving.
  active        INTEGER NOT NULL DEFAULT 1,
  created_at    INTEGER NOT NULL,
  updated_at    INTEGER NOT NULL
);

-- One row per logged-in device. The refresh token itself is 256 bits of OsRng
-- and is stored **only** as a SHA-256 digest: it is already high-entropy, so a
-- fast digest is the right primitive and Argon2 would be pure cost.
CREATE TABLE session (
  id                 TEXT PRIMARY KEY,               -- ulid; the `sid` claim
  user_id            TEXT NOT NULL REFERENCES user(id),
  refresh_token_hash TEXT NOT NULL UNIQUE,           -- SHA-256 hex of the opaque token
  created_at         INTEGER NOT NULL,
  expires_at         INTEGER NOT NULL,               -- unix ms, absolute
  last_used_at       INTEGER NOT NULL
);

CREATE INDEX session_user ON session(user_id);
