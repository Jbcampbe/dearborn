# Deerborn

Self-hosted Rust server that turns an approved epic into a PR autonomously. See
[VISION.md](./VISION.md) for product intent, [ARCHITECTURE.md](./ARCHITECTURE.md)
for resolved v1 decisions, and [MILESTONE_1.md](./MILESTONE_1.md) for the current
task plan.

The HTTP/REST API contract (routes, JSON success/error envelopes, status codes)
is documented in [`deerborn-server/CONVENTIONS.md`](./deerborn-server/CONVENTIONS.md).

## Layout

```
.
├── Cargo.toml            # Cargo workspace root
├── deerborn-server/      # Rust server crate (tokio + axum)
│   └── src/
│       ├── main.rs       # binary entrypoint (binds + serves)
│       └── lib.rs        # router + handlers (extended by later tasks)
├── client/               # Vue 3 + TypeScript + Vite SPA (Pinia state)
├── justfile              # dev / test / build task runner
└── references/           # ralph-v2 blueprint (source of truth for Half 2)
```

## Prerequisites

- **Rust** (stable; edition 2021) — <https://rustup.rs>
- **Node.js** 20+ and npm — <https://nodejs.org>
- **just** — the task runner. Install with one of:
  - `cargo install just`
  - `brew install just`

## Getting started

Install client dependencies once:

```bash
cd client && npm install
```

## Running

### Server only

```bash
cargo run -p deerborn-server
# → deerborn-server listening on http://127.0.0.1:8787
curl http://127.0.0.1:8787/health
# → {"status":"ok"}
```

The server reads its configuration from the environment (see the
[Configuration](#configuration) table below). `DEERBORN_TOKEN` and
`DEERBORN_MASTER_KEY` are **required** — the server refuses to start without
them:

```bash
DEERBORN_TOKEN=my-secret-token DEERBORN_MASTER_KEY=... cargo run -p deerborn-server
# → deerborn-server listening on http://127.0.0.1:8787
```

Every route except `GET /health` requires an `Authorization: Bearer <token>`
header matching `DEERBORN_TOKEN`; requests without it get `401`:

```bash
curl http://127.0.0.1:8787/health                                   # → 200 (public)
curl -H "Authorization: Bearer my-secret-token" \
     http://127.0.0.1:8787/whoami                                   # → 200 {"status":"authenticated"}
```

### Everything (server + Vite dev server)

```bash
just dev
```

Runs the Rust server and the Vite dev server together. Vite serves the SPA on
<http://localhost:5173> and proxies `/health` to the Rust server. Ctrl-C stops both.

## Testing

```bash
just test      # == cargo test  (the whole-repo gate)
```

## Building

```bash
just build     # cargo build --release  +  vite production build (client/dist)
```

## Configuration

Config is read from the process environment. As an **optional** fallback, point
`DEERBORN_CONFIG` at a `KEY=VALUE` file (`#` comments and blank lines ignored);
environment variables always take precedence over the file.

| Variable              | Required | Default          | Purpose                                                                 |
| --------------------- | :------: | ---------------- | ----------------------------------------------------------------------- |
| `DEERBORN_TOKEN`      |   yes    | —                | Single-user bearer token; every route except `GET /health` requires it. |
| `DEERBORN_MASTER_KEY` |   yes    | —                | Secret material for encrypting PATs at rest (see [Secret handling](#secret-handling)).|
| `DEERBORN_BIND`       |    no    | `127.0.0.1:8787` | Server bind address.                                                     |
| `DEERBORN_DB`         |    no    | `./deerborn.db`  | Path to the local libSQL database file (T-003).                         |
| `DEERBORN_CLONE_ROOT` |    no    | `./clones`       | Root directory under which per-project clones live (T-103).             |
| `DEERBORN_CONFIG`     |    no    | —                | Optional path to a `KEY=VALUE` config file used as a fallback source.    |

The server **fails fast at boot** with a clear error (non-zero exit) if
`DEERBORN_TOKEN` or `DEERBORN_MASTER_KEY` is missing or empty.

## Canonical read-only clone (T-103)

On project create, Deerborn clones the repo's default branch (git-over-HTTPS,
using the decrypted PAT when present) into `<DEERBORN_CLONE_ROOT>/<project id>` —
the canonical **read-only** checkout later planning/execution reads from. The
clone runs **asynchronously**: `POST /projects` returns immediately with
`clone_status='pending'`; a background task then sets `clone_status` to `ready`
or, on failure, `error` (with a readable, token-redacted `clone_error`), and
publishes a `clone_status` event on the `project:<id>` WebSocket topic.

`POST /projects/{id}/refresh` re-syncs an existing checkout (`git fetch` +
hard-reset to origin's default branch), moving it back through
`pending → ready/error`.

The PAT is shelled out to `git` as an argument only and is **never** written to
a log or persisted in `.git/config` (the remote is reset to the token-free URL
after clone; fetch re-injects credentials transiently). Git operations that fail
capture git's stderr with any token redacted.

## Secret handling

Per-project GitHub PATs are **encrypted at rest** with **AES-256-GCM** (T-102):

- **Key derivation.** The 256-bit AES key is `SHA-256(DEERBORN_MASTER_KEY)` — the
  master-key material may be any non-empty string (any length/format); SHA-256
  deterministically maps it to the 32 bytes AES-256 needs. Derivation is
  validated at boot, so a key that cannot form a valid 256-bit key (i.e. empty
  material) fails fast with a non-zero exit.
- **Nonce & storage layout.** A fresh random **96-bit nonce** is generated per
  encryption; the value stored in the `project.pat_encrypted` BLOB is
  `nonce || ciphertext` (the 12-byte nonce prepended to the AES-GCM ciphertext,
  which already carries its 128-bit auth tag).
- **Rotation.** Changing `DEERBORN_MASTER_KEY` changes the derived key, so PATs
  encrypted under the old value stop decrypting (a wrong/rotated key yields a
  GCM authentication error, never plaintext) and must be re-entered.
- **Never returned, never logged.** A PAT is accepted only on `POST`/`PATCH
  /projects`; it is never included in any API response and never written to a
  log line (the request field is a redacted-`Debug` `Secret`). The decrypt path
  is crate-internal, used only by cloning (T-103).
