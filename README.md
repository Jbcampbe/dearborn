# Deerborn

Self-hosted Rust server that turns an approved epic into a PR autonomously. See
[VISION.md](./VISION.md) for product intent, [ARCHITECTURE.md](./ARCHITECTURE.md)
for resolved v1 decisions, and [MILESTONE_1.md](./MILESTONE_1.md) for the current
task plan.

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

The bind address defaults to `127.0.0.1:8787`. Override it with the `DEERBORN_BIND`
env var (full config handling arrives in T-002):

```bash
DEERBORN_BIND=127.0.0.1:9000 cargo run -p deerborn-server
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

## Environment variables

| Variable        | Default            | Purpose                                  |
| --------------- | ------------------ | ---------------------------------------- |
| `DEERBORN_BIND` | `127.0.0.1:8787`   | Server bind address. Full config: T-002. |
