# Dearborn

Self-hosted Rust server that turns an approved epic into a PR autonomously. See
[VISION.md](./VISION.md) for product intent, [ARCHITECTURE.md](./ARCHITECTURE.md)
for resolved v1 decisions, [MILESTONE_1.md](./MILESTONE_1.md) for the completed
planning/task-creation half, and [MILESTONE_2.md](./MILESTONE_2.md) for the
current task plan (the executor).

The HTTP/REST API contract (routes, JSON success/error envelopes, status codes)
is documented in [`dearborn-server/CONVENTIONS.md`](./dearborn-server/CONVENTIONS.md).

## Layout

```
.
├── Cargo.toml            # Cargo workspace root
├── dearborn-server/      # Rust server crate (tokio + axum)
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

The above are build/dev-time only. The **executor** (Milestone 2) additionally
shells out to two binaries that must be on the host's `PATH` at *run* time —
neither is a Cargo/npm dependency, so `cargo build`/`npm install` succeed
without them, but every epic and standalone-task run will fail immediately
without them:

- **`git`** — every workspace operation (canonical clone/refresh, per-epic and
  per-task clone, commit, push) shells out to the system `git`, the same way
  the T-103 canonical-clone path already does.
- **a coding-agent CLI** — every agent stage shells out to one, headless. Which
  binary depends on the slot's configured harness (Settings → agent slots); the
  two Dearborn has adapters for are:
  - **`claude`** (the Claude Code CLI) — runs **every** slot, and is the default
    everywhere. Driven via the
    [`agent-harness`](https://github.com/getlatentic/agent-harness) crate's
    Claude adapter (`claude -p --permission-mode ...`). It must be logged in (or
    `ANTHROPIC_API_KEY` set in the server's environment).
  - **`pi`** ([pi.dev](https://pi.dev)) — runs the five **task stages** only
    (`implement`, `fix`, `review`, `verify_complete`, `summarize`). Driven via
    Dearborn's own adapter (`dearborn-server/src/harness_pi.rs`: `pi --mode json
    -p ...`), which parses pi's NDJSON into the same normalized `RunEvent`
    stream. It must have a provider configured (`pi auth`, or any of the
    provider API-key env vars pi documents).

    **Not** the three planning-side slots (`planning_product`,
    `planning_technical`, `breakdown`): those call *back* into Dearborn over MCP
    to maintain the epic record and write the task DAG, and pi has no MCP
    client. Selecting pi for one is refused with a 400 at configuration time,
    and refused again at spawn if a settings row is hand-edited past that.

  Real agent stages spend real tokens. `just test` never invokes either binary
  (see [Testing](#testing)); `dearborn-server/tests/worker_live.rs` and
  `dearborn-server/tests/harness_pi_live.rs` are the `#[ignore]`d proofs that
  exercise the real binaries on demand.

## Getting started

Install client dependencies once:

```bash
cd client && npm install
```

## Running

### Server only

```bash
cargo run -p dearborn-server
# → dearborn-server listening on http://127.0.0.1:8787
curl http://127.0.0.1:8787/health
# → {"status":"ok"}
```

The server reads its configuration from the environment (see the
[Configuration](#configuration) table below). `DEARBORN_TOKEN` and
`DEARBORN_MASTER_KEY` are **required** — the server refuses to start without
them:

```bash
DEARBORN_TOKEN=my-secret-token DEARBORN_MASTER_KEY=... cargo run -p dearborn-server
# → dearborn-server listening on http://127.0.0.1:8787
```

Every route except `GET /health` requires an `Authorization: Bearer <token>`
header matching `DEARBORN_TOKEN`; requests without it get `401`:

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
<http://localhost:5173> with hot-reload and proxies the API it calls (`/health`,
`/whoami`, `/projects`, and the `/ws` WebSocket) to the Rust server on `:8787`.
Ctrl-C stops both.

## Serving the client (T-006)

In **production** the Rust binary serves the built Vite SPA itself — no separate
web server. `just build` (or `cd client && npm run build`) emits the assets to
`client/dist`; the binary serves them at `/` with an **SPA fallback**: any path
that isn't an API route and isn't a real asset file returns `index.html`, so
client-side routing works on deep links / refreshes.

- The assets dir is `DEARBORN_STATIC_DIR` (default `./client/dist`, relative to
  the working directory — the workspace root under `cargo run`).
- API routes always win: `/health`, `/ws`, `/projects*` etc. are matched before
  the static fallback, so serving the SPA never shadows or unauth-exposes them.
  The static/SPA files are served **without** auth (so the shell can load and
  prompt for a token); auth is enforced on the API calls the SPA then makes.
- If the assets dir is missing (e.g. you ran `cargo run` without building the
  client), the server logs a warning and serves the **API only** — it does not
  crash. Build the client to get the SPA back.

The SPA persists the bearer token in `localStorage`, shows a token-entry screen
when none is set, attaches `Authorization: Bearer <token>` to API calls, and on
a `401` clears the token and returns to the entry screen with an auth error.

## Testing

```bash
just test      # cargo test  +  cd client && npm test — the whole-repo gate
```

Both suites are fully **hermetic** — no network, no real agent CLI, no GitHub —
so the gate runs anywhere without credentials. The three exceptions are
deliberate, `#[ignore]`d, and excluded from `just test`/`cargo test` by
default: `dearborn-server/tests/mcp_live.rs` (T-203),
`dearborn-server/tests/worker_live.rs` (T-515), and
`dearborn-server/tests/harness_pi_live.rs` (the pi adapter's wire-format
tripwire), each documenting its own `cargo test -- --ignored` run command for
exercising the real path on demand.

## Building

```bash
just build     # cargo build --release  +  vite production build (client/dist)
```

## Configuration

Config is read from the process environment. As an **optional** fallback, point
`DEARBORN_CONFIG` at a `KEY=VALUE` file (`#` comments and blank lines ignored);
environment variables always take precedence over the file.

| Variable              | Required | Default          | Purpose                                                                 |
| --------------------- | :------: | ---------------- | ----------------------------------------------------------------------- |
| `DEARBORN_TOKEN`      |   yes    | —                | Single-user bearer token; every route except `GET /health` requires it. |
| `DEARBORN_MASTER_KEY` |   yes    | —                | Secret material for encrypting PATs at rest (see [Secret handling](#secret-handling)).|
| `DEARBORN_BIND`       |    no    | `127.0.0.1:8787` | Server bind address.                                                     |
| `DEARBORN_DB`         |    no    | `./dearborn.db`  | Path to the local libSQL database file (T-003).                         |
| `DEARBORN_CLONE_ROOT` |    no    | `./clones`       | Root directory under which per-project clones live (T-103).             |
| `DEARBORN_STATIC_DIR` |    no    | `./client/dist`  | Directory of built Vite SPA assets served at `/` (T-006).               |
| `DEARBORN_CONFIG`     |    no    | —                | Optional path to a `KEY=VALUE` config file used as a fallback source.    |
| `DEARBORN_WORKER_CONCURRENCY` | no | `2`            | Number of executor worker loops.                                         |
| `DEARBORN_LEASE_TTL_SECS` | no | `300`              | Executor lease lifetime.                                                 |
| `DEARBORN_HEARTBEAT_SECS` | no | `30`               | Executor lease renewal interval.                                         |
| `DEARBORN_AGENT_STAGE_TIMEOUT_SECS` | no | `1800`  | Wall-clock ceiling per agent stage.                                      |
| `DEARBORN_CMD_TIMEOUT_SECS` | no | `900`            | Wall-clock ceiling per `setup_cmd` / `test_cmd` run.                     |
| `DEARBORN_MAX_TEST_FIX_ATTEMPTS` | no | `3`         | Max attempts of the test-driven fix loop (ralph parity).                 |
| `DEARBORN_MAX_FIX_ROUNDS` | no | `3`                | Max rounds of the review-convergence fix loop (ralph parity).            |
| `DEARBORN_VERDICT_RETRIES` | no | `1`               | Extra re-runs when a review reply lacks a parseable verdict (ralph parity). |
| `DEARBORN_POLL_INTERVAL_MS` | no | `1500`           | Fallback poll interval for workers, behind the notify.                   |

The server **fails fast at boot** with a clear error (non-zero exit) if
`DEARBORN_TOKEN` or `DEARBORN_MASTER_KEY` is missing or empty. The executor
variables above are best-effort: an invalid or unparseable value falls back to
its default with a logged warning rather than failing boot (see
`dearborn-server/src/config.rs`).

## Executor operational model

The executor (Milestone 2) is a leased worker pool that claims an `In
Progress` epic or a `Todo` standalone task and drives it through
`implement → test-gate → commit → review+verdict → fix-loop → close`, opening
one PR at the end. This section is the plain-language write-up of how that
actually runs; `MILESTONE_2.md` §1–§8 has the full design history and
`dearborn-server/CONVENTIONS.md` has the exact HTTP/WS contract.

### Worker pool, leases, and claiming

`DEARBORN_WORKER_CONCURRENCY` long-lived worker loops start at boot (default
`2`), each with a stable identity used as its lease owner. An idle loop waits
on an in-process notify with `DEARBORN_POLL_INTERVAL_MS` as a fallback timeout
(the safety net for a wakeup that lands in the small window between a claim
attempt and re-entering the wait); a successful claim skips the wait entirely
and tries to claim again immediately, so a burst of enqueued work drains as
fast as workers are free rather than one item per poll tick.

A claim is one atomic `UPDATE ... RETURNING`, tried against epics first (any
`InProgress` epic with no live lease) and, only if none is claimable,
standalone tasks (any `Todo`, parentless task) as a fallback — so a flood of
standalone work can never starve an epic. SQLite/libSQL's single-writer
serialization **is** the mutual-exclusion lock; there is no separate
application-level mutex. Once claimed, a **heartbeat** renews the lease every
`DEARBORN_HEARTBEAT_SECS` with a fenced write (`WHERE lease_owner = ?`) — if
another worker's claim has already stolen the row, the fenced write affects
zero rows, which is the sole signal the heartbeat needs to know its own lease
is gone; it stops renewing and the pipeline body abandons the item at its next
check, making no further writes.

A lease's expiry (`DEARBORN_LEASE_TTL_SECS`, default `300`s) is purely
**implicit** — there is no background reaper task scanning for and clearing
expired leases. The claim predicate itself (`lease_expires_at < now`) is what
makes an expired lease reclaimable; the next claim attempt against that row
simply succeeds. Because Dearborn assumes a single server process, every
lease on `epic` and `task` is unconditionally cleared at **boot**, so a
restart resumes in-flight work on the very first poll/notify instead of
waiting out however much of the TTL happened to elapse. A dead worker's
`InProgress` task (abandoned mid-flight, never finished) is reset to `Todo` as
part of the next successful claim on its epic, so the DAG walk picks it back
up rather than leaving it stuck.

### Workspaces

Every project has a canonical, read-only checkout at
`<DEARBORN_CLONE_ROOT>/<project id>` (T-103), kept in sync with origin. An
**epic workspace** is a full local `git clone` of that canonical checkout at
`<DEARBORN_CLONE_ROOT>/epics/<epic id>`, with its origin repointed at the
real remote (no token ever written to disk) and checked out on the epic's own
branch (`dearborn/<slug(epic.title)>-<last 6 of epic id>`). A **standalone
task** gets the identical treatment at `<DEARBORN_CLONE_ROOT>/tasks/<task
id>`, on its own branch (`dearborn/task-<slug(task.title)>-<last 6 of task
id>`). A real clone (not a `git worktree`) is deliberate: worktrees share
their parent's `.git` and ref locks, which is exactly the kind of collision
two concurrent epics (or an epic and the canonical checkout's own refresh)
would otherwise hit; a clone gives each workspace its own `.git`, at the cost
of a one-time local object-store copy per epic/task. Every provision first
refreshes the shared canonical checkout under a **per-project lock**, so two
workers provisioning in the same project never interleave their `git reset
--hard`/`fetch` calls against the one shared mirror.

A workspace **persists across re-claims** — a worker restart, a lease theft,
or a retry — rather than being deleted and recreated. Re-provisioning an
existing workspace **re-attaches** instead of re-cloning: `git reset --hard
HEAD` + `git clean -fd` drop only whatever uncommitted mess the previous
attempt left behind, while every real commit already on the branch survives.
`setup_cmd` re-runs on every provision, including a re-attach — it is
documented (MILESTONE_1 §5) as idempotent by contract, and re-running it is
cheap next to trying to durably track "has setup already run here" across a
restart.

A workspace is **deleted** once its PR has actually opened (the epic reaches
`Completed`, or the standalone task's own PR opens) — there is nothing left
worth keeping on disk at that point. It is **retained** on every other exit
path: `Blocked`, `Cancelled`, `Failed`, or a lost lease — so a human (or a
subsequent retry) can inspect exactly what the last attempt left behind,
including any uncommitted, in-progress diff a failed task never got to
commit.

### Recovery: retry, re-attach, re-run

A structured failure (§2.3's reason set) sets the task `Failed` and, for an
epic-scoped task, its epic `Blocked` — both carrying the identical reason
string — releases the lease, retains the workspace, and (best-effort) pushes
whatever is already committed on the branch so a human can `git clone`/`fetch`
and triage locally without VPS access. The failure is scoped to that one
epic or task; the same worker loop immediately claims its next item.

`POST /tasks/{id}/retry` is the one-shot recovery transition, and it differs
by shape:

- An **epic-scoped** task retries `Failed → Todo`, and — iff its epic is
  currently `Blocked` — the epic also moves `Blocked → InProgress`, clearing
  `blocked_reason` and its lease. There are two rows to restore here (the
  claimable item, the epic, and the unit of work, the task) and the endpoint
  restores each.
- A **standalone** task retries `Failed → InProgress` directly, **not**
  `Todo` — the worker's claim query only ever selects `status = 'InProgress'
  AND epic_id IS NULL` for a standalone task, so a task left in `Todo` would
  never be picked up by anything. Unlike the epic case, a standalone task is
  one row playing both roles (claimable item and unit of work), so restoring
  its claimability *is* resetting its work.

Either way, once a worker re-claims, provisioning re-attaches the retained
workspace exactly as described above, which is what actually drops the failed
attempt's dirty tree before the pipeline re-enters at the now-runnable task.
Editing the spec first via `PATCH /tasks/{id}` needs no special support: the
next `implement`/`fix` stage simply re-renders whatever `description`/
`acceptance` are on the row at claim time, so an edited spec reaches the
re-run for free.

### Cancellation

Cancelling is a **kill**, not a status flag a slow worker eventually notices.
The server holds a live run handle for whatever agent stage is currently
executing, keyed by epic id, in an in-process registry populated for the
duration of exactly one agent stage (`implement`/`fix`/`review`/
`verify_complete`/`summarize` — a non-agent stage like `setup`/`test_gate`/
`commit`/`push` has no process to kill). `POST /epics/{id}/lane` with `{
status: "Cancelled" }` against an `InProgress` epic sets the epic `Cancelled`
and then looks the epic up in that registry, killing whatever it finds —
best-effort and fire-and-forget; the HTTP response never waits on the process
actually exiting. The worker's own stage-boundary check is the backstop for
the (rare) case where nothing was in the registry to kill — between tasks, or
during a non-agent stage. Once the kill is observed, the in-flight task
resets to `Todo` (not `Failed` — a cancellation isn't a failure, the work is
resumable), the lease releases, and the workspace is retained exactly as it
is on any other stop path. No PR is ever opened on a cancelled epic.

**There is deliberately no cancellation surface for a standalone task** — no
`POST /tasks/{id}/cancel` exists. Cancellation today is issued only through
the epic lane endpoint above; a standalone task in flight can only be waited
out or, once it fails, recovered via `retry`.

## Canonical read-only clone (T-103)

On project create, Dearborn clones the repo's default branch (git-over-HTTPS,
using the decrypted PAT when present) into `<DEARBORN_CLONE_ROOT>/<project id>` —
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

The same PAT is reused by the executor (Milestone 2) to push a completed
epic's branch and to open its pull request via the GitHub API — see
[Required PAT scope](#required-pat-scope) below for what the token needs to be
able to do.

## Required PAT scope

A project's PAT needs, at minimum, GitHub's classic **`repo`** scope (or, for
a fine-grained token, **Contents: Read and write** + **Pull requests: Read and
write** on the target repository) — Dearborn uses it to clone/fetch (read),
push the epic branch (write), and open the pull request via the REST API
(pull-request write). A token missing write access fails at push or PR-open
time with a redacted, readable `Blocked(pr_failed)` reason on the epic (see
`dearborn-server/CONVENTIONS.md`'s epic lane-transition section); it is never
silently ignored.

## Secret handling

Per-project GitHub PATs are **encrypted at rest** with **AES-256-GCM** (T-102):

- **Key derivation.** The 256-bit AES key is `SHA-256(DEARBORN_MASTER_KEY)` — the
  master-key material may be any non-empty string (any length/format); SHA-256
  deterministically maps it to the 32 bytes AES-256 needs. Derivation is
  validated at boot, so a key that cannot form a valid 256-bit key (i.e. empty
  material) fails fast with a non-zero exit.
- **Nonce & storage layout.** A fresh random **96-bit nonce** is generated per
  encryption; the value stored in the `project.pat_encrypted` BLOB is
  `nonce || ciphertext` (the 12-byte nonce prepended to the AES-GCM ciphertext,
  which already carries its 128-bit auth tag).
- **Rotation.** Changing `DEARBORN_MASTER_KEY` changes the derived key, so PATs
  encrypted under the old value stop decrypting (a wrong/rotated key yields a
  GCM authentication error, never plaintext) and must be re-entered.
- **Never returned, never logged.** A PAT is accepted only on `POST`/`PATCH
  /projects`; it is never included in any API response and never written to a
  log line (the request field is a redacted-`Debug` `Secret`). The decrypt path
  is crate-internal, used only by cloning (T-103).
