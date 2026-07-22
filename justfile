# Dearborn task runner. Install `just`: `cargo install just` or `brew install just`.

# List available recipes.
default:
    @just --list

# Run the Rust backend alone.
#
# Required env vars (`DEARBORN_TOKEN`, `DEARBORN_MASTER_KEY`) are sourced from a
# gitignored `.env` in the repo root if one exists; otherwise sensible dev
# defaults are used so `just backend` works out of the box. Any var already
# exported in your shell wins over both. See `.env.example`.
backend:
    #!/usr/bin/env bash
    set -euo pipefail
    if [[ -f .env ]]; then
        set -a
        # shellcheck disable=SC1091
        . ./.env
        set +a
    fi
    : "${DEARBORN_TOKEN:=dev-token}"
    : "${DEARBORN_MASTER_KEY:=dev-master-key}"
    : "${DEARBORN_BIND:=127.0.0.1:8787}"
    export DEARBORN_TOKEN DEARBORN_MASTER_KEY DEARBORN_BIND
    # cargo run -p dearborn-server
    cargo watch -x run

# Run the Vite frontend dev server alone.
frontend:
    #!/usr/bin/env bash
    set -euo pipefail
    cd client && npm run dev

# Run the backend and frontend together. Ctrl-C stops both.
dev:
    #!/usr/bin/env bash
    set -euo pipefail
    echo "starting dearborn-server + vite..."
    just backend &
    backend_pid=$!
    trap 'kill "$backend_pid" 2>/dev/null || true' EXIT INT TERM
    just frontend

# Whole-repo test gate (Rust + client). Becomes Dearborn's own test_cmd later.
test:
    cargo test
    cd client && npm test

# Build the release binary and the Vite production assets.
build:
    cargo build --release
    cd client && npm run build
