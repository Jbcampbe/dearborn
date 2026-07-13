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
| `DEERBORN_MASTER_KEY` |   yes    | —                | AES-256-GCM key material for encrypting PATs at rest (consumed in T-102).|
| `DEERBORN_BIND`       |    no    | `127.0.0.1:8787` | Server bind address.                                                     |
| `DEERBORN_DB`         |    no    | `./deerborn.db`  | Path to the local libSQL database file (T-003).                         |
| `DEERBORN_CLONE_ROOT` |    no    | `./clones`       | Root directory under which per-project clones live (T-103).             |
| `DEERBORN_CONFIG`     |    no    | —                | Optional path to a `KEY=VALUE` config file used as a fallback source.    |

The server **fails fast at boot** with a clear error (non-zero exit) if
`DEERBORN_TOKEN` or `DEERBORN_MASTER_KEY` is missing or empty.
