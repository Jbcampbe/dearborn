# Deerborn task runner. Install `just`: `cargo install just` or `brew install just`.

# List available recipes.
default:
    @just --list

# Run the Rust server and the Vite dev server together.
# Both run in the foreground; Ctrl-C stops both (trap kills the server child).
dev:
    #!/usr/bin/env bash
    set -euo pipefail
    echo "starting deerborn-server + vite..."
    cargo run -p deerborn-server &
    server_pid=$!
    trap 'kill "$server_pid" 2>/dev/null || true' EXIT INT TERM
    (cd client && npm run dev)

# Whole-repo test gate (Rust + client). Becomes Deerborn's own test_cmd later.
test:
    cargo test
    cd client && npm test

# Build the release binary and the Vite production assets.
build:
    cargo build --release
    cd client && npm run build
