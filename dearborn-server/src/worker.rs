//! The executor worker pool: N long-lived loops that claim leased epics and
//! drive them to completion (T-510, Milestone 2 §2.4/§6, decisions D2/D4).
//!
//! ## The shape (D2)
//!
//! Milestone 1's model was "the lane handler spawns a worker per epic." That
//! doesn't survive a restart (an in-flight epic's `tokio::spawn`'d task dies
//! with the process) and doesn't bound concurrency (every `Ready → InProgress`
//! move spawned its own task, unbounded). [`spawn_pool`] replaces it with
//! `config.executor.worker_concurrency` long-lived loops ([`worker_loop`]),
//! each with a stable identity (`worker_id`) used as the row's `lease_owner`.
//! The lane handler ([`crate::lanes::set_epic_lane`]) now only enqueues
//! (`status='InProgress'`, lease cleared) and calls
//! `state.notify.notify_waiters()` — it never spawns anything. A worker loop
//! survives any single epic's failure and keeps serving the queue for the
//! life of the process; restart-safety comes from the DB being the only
//! source of truth (§13) plus the boot-time lease clear ([`clear_all_leases`]).
//!
//! ## Notify-or-poll (idle loop)
//!
//! An idle worker waits on `tokio::time::timeout(poll_interval,
//! notify.notified())`. `notify_waiters()` is the fast path (near-instant
//! wake on enqueue); the `poll_interval_ms` timeout is the safety net for a
//! missed wakeup — `Notify::notify_waiters()` only wakes futures that are
//! *already* registered as waiting, so a notify that lands in the small
//! window between a worker finishing its claim attempt and re-entering the
//! wait is otherwise lost. Neither path busy-waits: the loop is parked on
//! `.await` the entire time.
//!
//! After a successful claim, a worker skips the wait entirely and tries to
//! claim again immediately ([`worker_loop`]'s inner loop) — otherwise a burst
//! of enqueues would only drain one epic per `poll_interval_ms`, however many
//! workers are idle.
//!
//! ## The claim (§2.4)
//!
//! [`claim_epic`] is exactly the §2.4 statement: an `UPDATE ... WHERE id =
//! (SELECT ... ORDER BY updated_at ASC LIMIT 1) RETURNING id, project_id`.
//! SQLite/libSQL serialize writers against one connection — that serialization
//! **is** the mutual-exclusion lock (§6); no application-level mutex sits on
//! top of it. Two workers racing this statement concurrently: the subquery
//! picks at most one row, so at most one UPDATE can match it; the loser's
//! `WHERE` clause (now failing the `lease_owner IS NULL OR lease_expires_at <
//! now` predicate, or simply finding no matching id if it was the only
//! candidate) affects zero rows and returns `Ok(None)`.
//!
//! This uses libSQL's `RETURNING` clause through the ordinary `query()` path
//! (libsql 0.9's bundled SQLite supports `UPDATE ... RETURNING` since SQLite
//! 3.35) rather than the UPDATE-then-SELECT fallback the task allows — one
//! round trip, and no need to reason about a follow-up read racing another
//! worker's claim. If a libSQL version ever regresses `RETURNING` support, the
//! safe fallback is an `UPDATE` using `changes()`/a `RETURNING`-free affected
//! check followed by `SELECT ... WHERE id = ?1 AND lease_owner = ?2` (by id
//! **and** the worker's own `lease_owner`, never a bare re-SELECT — a bare
//! `SELECT ... WHERE status='InProgress' ORDER BY updated_at LIMIT 1` after a
//! blind `UPDATE` could read a different worker's freshly-claimed row).
//!
//! ## Orphaned tasks (part of the claim path)
//!
//! A dead worker's lease eventually expires, but any task it left
//! `InProgress` did not finish — that work was abandoned mid-flight.
//! [`reset_orphaned_tasks`] resets those back to `Todo` as part of the same
//! claim (called immediately after a successful [`claim_epic`]), so the new
//! owner's DAG walk sees them as pending again rather than permanently stuck.
//!
//! ## Heartbeat with fencing (D4)
//!
//! [`spawn_heartbeat`] renews the claimed epic's `lease_expires_at` every
//! `heartbeat_secs` via the fencing update: `UPDATE epic SET
//! lease_expires_at = ? WHERE id = ? AND lease_owner = ?`. The `WHERE
//! lease_owner = ?` clause is the fence — if another worker's claim already
//! stole the row (because this worker's lease expired and nobody renewed it
//! in time), the predicate matches nothing and the UPDATE affects zero rows.
//! Zero rows is the **only** signal needed: there is no separate "am I still
//! the owner?" read to race against, because the write's own affected-row
//! count is authoritative. On zero rows the heartbeat flips the shared
//! [`LeaseHandle`] to lost and stops renewing; the claimed-epic body
//! ([`run_epic_pipeline_inner`]) checks the handle at the top of the loop and
//! again immediately before each task's finalizing writes, abandoning the
//! item — no further writes — the moment it observes the loss.
//!
//! ## No reaper (D4)
//!
//! Lease expiry is **implicit**: the claim predicate itself
//! (`lease_expires_at < now`) is what makes an expired lease reclaimable.
//! There is no background task scanning for expired leases to clear them —
//! nothing needs to; the next claim attempt against that epic simply
//! succeeds. This trades a small, bounded delay (up to `lease_ttl_secs`)
//! after a genuine worker death for one less moving part.
//!
//! ## Boot-time lease clear (D4, §13)
//!
//! [`clear_all_leases`] NULLs every lease column on `epic` and `task` at
//! startup (`main`, before [`spawn_pool`]). Dearborn assumes a single server
//! process (§13): nothing else could legitimately hold a lease across a
//! restart, so waiting out the TTL would only delay resumption for no
//! benefit. Clearing immediately means a restart resumes in-flight work on
//! the very first poll/notify rather than after however much of the TTL
//! happened to elapse.
//!
//! ## The preflight gate (T-521, D5, §2.2/§5)
//!
//! Immediately after [`workspace::provision_epic_workspace`] returns (so
//! `setup_cmd` has already run) and strictly before the DAG walk below ever
//! looks at a task, [`run_preflight`] runs the project's `test_cmd` **once**
//! against the untouched tree — no task's `Stage::Implement` has run yet, so
//! this is the one moment in the whole claim where "the tree is green" is a
//! claim about the *repository*, not about anything Dearborn did to it.
//!
//! ### Why it exists
//!
//! Every later test gate (T-522's per-task `test_gate`, T-530's review) only
//! means something if a red result can be blamed on the task that just ran.
//! If the repo's tests were already red before Dearborn touched anything,
//! that inference is broken — a red `test_gate` after `Stage::Implement`
//! could be the agent's fault or a pre-existing break, and the executor has
//! no way to tell them apart after the fact. ralph's own preflight
//! (`references/ralph-v2.sh`, the `# --- preflight ---` section: it runs
//! `$TEST_CMD` once, before its main loop, and `die`s the whole run if that
//! fails) exists for the identical reason; D5 keeps it.
//!
//! ### Absent `test_cmd`, and why a timeout still counts as red
//!
//! No `test_cmd` configured ⇒ [`cmd::run_stage_command`]'s own
//! [`StageOutcome::Skipped`] contract applies unchanged: no `agent_run` row,
//! no gate, the walk proceeds — T-521 does not invent a stricter rule than
//! T-520 already promises. A `test_cmd` that *times out* is treated the same
//! as one that exits non-zero: both become `blocked_reason = 'preflight_red'`
//! (never `'timeout'`, despite §2.3 listing both as valid reasons — see
//! [`run_preflight`]'s doc for the reasoning, in short: `timeout` reads as
//! "Dearborn's own tooling got stuck," which is the right story for T-543's
//! agent-stage timeouts but the wrong one here, where the actionable fact for
//! a human triaging the board is simply "this repo's tests did not come back
//! green in time" — indistinguishable in consequence from an ordinary
//! failure). The finer distinction is not lost: the `preflight` `agent_run`
//! row's own `status` column still says `"timeout"`; only the coarse,
//! board-facing `blocked_reason` collapses the two.
//!
//! ### Once per claim, including a re-claim (design decision)
//!
//! [`run_preflight`] is called exactly once per invocation of
//! [`run_epic_pipeline_inner`] — i.e. once per successful claim, never once
//! per task — and there is deliberately no special-casing for "this is a
//! re-claim of an already-provisioned workspace, skip it." A re-attached
//! workspace was just reset (`git reset --hard HEAD` + `git clean -fd`,
//! inside `provision_epic_workspace`) to the tip of whatever this epic has
//! actually committed so far — that tree is just as "untouched by the
//! upcoming task" as a first claim's fresh clone, so the gate is exactly as
//! meaningful, and re-running `test_cmd` is cheap next to a full agent stage
//! (mirroring the same "cheap to redo, unsafe to skip" argument
//! `workspace.rs` already makes for re-running `setup_cmd` on re-attach).
//!
//! ### No agent is ever spawned on a red preflight
//!
//! A red/absent-green preflight `return`s out of
//! [`run_epic_pipeline_inner`] before the DAG walk loop below is even
//! entered — no task is looked up, no `Stage::Implement` request is ever
//! built, and [`crate::task_agent::TaskAgent::run`] is never called. The
//! epic's tasks stay exactly `Todo` (or whatever they already were), the
//! workspace is retained (routed through [`fail_item`], the same T-540
//! router every other failure path in this module funnels through — never a
//! bespoke write), and the lease is released by [`try_claim_and_run`]
//! exactly as it is on every other exit path.
//!
//! ## The real implement walk (T-513)
//!
//! [`run_epic_pipeline_inner`] is the real DAG walk that replaced Milestone
//! 1's DB-only stub walk (that stub's pipeline functions are deleted
//! outright, not kept around behind a flag; see MILESTONE_2 §10's definition
//! of done). After [`workspace::provision_epic_workspace`] and the T-521
//! preflight gate above both clear (see the sections above), the walk
//! processes **ready** tasks (per [`compute_dag`]'s §2.3 readiness) one at a
//! time, in full, before ever looking for the next one:
//!
//! 1. **`base_sha`** — the workspace's current `HEAD` (`git rev-parse HEAD`),
//!    recorded on the task *before* anything else touches the tree. This has
//!    to happen now, not after the implement stage runs, because the
//!    implement stage's own commit (step 5) moves `HEAD` — capturing it any
//!    later would record the *wrong* base, and the whole reason `base_sha`
//!    exists (T-530's cumulative-diff review) is to diff against exactly the
//!    tree this task started from, not the tree some other step happened to
//!    leave behind.
//! 2. **`Todo → InProgress`**, publishing `dag_updated` — identical in shape
//!    to the M1 stub's transition, just earlier in a much longer step.
//! 3. **The D8 prompt**: [`crate::spec::build_context`] assembled from the
//!    task's own rendered spec, the epic's background (title/description/
//!    product & technical context), and a sibling manifest built from every
//!    *other* task in the epic, partitioned `Done` vs. not — this is what
//!    stops an autonomous implement agent from building the whole epic in
//!    one task (D7 gives it no other way to learn the epic's scope), so it is
//!    wired from the real DAG state on every run, never a bare spec string.
//! 4. **`Stage::Implement`** through the [`crate::task_agent::TaskAgent`]
//!    seam (`RunMode::Edit`, `cwd` = the provisioned workspace), evidence
//!    recorded by [`crate::task_agent::run_agent_stage`] exactly as T-512
//!    built it. A stage that does not come back `ok`
//!    ([`crate::task_agent::AgentStageOutcome::is_ok`]) — or fails to even
//!    start — routes the *task* to `Failed(agent_error)` and the epic to
//!    `Blocked(agent_error)` via [`fail_item`] (T-540's centralized router;
//!    see its own section below) and stops the walk. `agent_error` stays
//!    deliberately coarse here: MILESTONE_2 §4 calls Phase 1 a tracer bullet
//!    and this slice still does not attempt to distinguish *why* the stage
//!    failed — T-540 only centralizes *where* every failure lands, not the
//!    granularity of this particular reason.
//! 5. **`git add -A`**, then a commit **only if there is something to
//!    commit** ([`git::status_porcelain`] after staging) — an agent that made
//!    no changes (it judged the task already satisfied by earlier work) is
//!    committed as *nothing*, per MILESTONE_2 §4's explicit tracer-bullet AC;
//!    verifying that "no diff" genuinely means "already done" is
//!    [`crate::task_agent::Stage::VerifyComplete`]'s job, landing in T-532,
//!    not this one. A real commit uses the frozen §2.8 subject
//!    (`impl(<short task id>): <task title>`, [`crate::spec::short_id`]
//!    reused rather than re-derived) and a deterministic committer identity
//!    (`-c user.name=`/`-c user.email=`, never written to the workspace's own
//!    `.git/config` — see [`git::commit_all`]'s doc for why) so a commit
//!    succeeds even on a host with no configured global git identity. The
//!    resulting SHA is recorded in a `Stage::Commit` `agent_run` row's `log`
//!    (§2.2: "records the SHA in `log`") — opened only when a commit actually
//!    happens, matching D13's "every stage that runs gets a row", not "every
//!    stage that could have run".
//! 6. **`Done`**, publishing `dag_updated`. The loop then returns to its top:
//!    re-fetch the epic (still `InProgress`?), re-check the lease, recompute
//!    the DAG, and only *then* look for the next ready task — this is the
//!    same "one ready task at a time, no sibling ever `InProgress`
//!    concurrently" (§2.3) discipline the M1 stub already had, just now
//!    guarding a much more expensive step.
//!
//! ### `InReview` only after a real PR opens (T-514)
//!
//! Unlike the M1 stub, this walk does not land the epic in `InReview` the
//! moment the DAG goes fully `Done` — [`finalize_epic`] does, and only after
//! the epic's branch has been pushed **and** a PR has actually opened (D1).
//! In the post-PR-review loop `InReview` is the factory-done-waiting-on-the-
//! human state; `Completed` is reached only from `InReview`, on a human
//! merge, by a later poller task. An epic whose DAG is fully `Done` but has
//! not yet been pushed/PR'd is not "in review" in any sense a human watching
//! the board should trust, so
//! the walk calls straight into [`finalize_epic`] the moment it observes
//! `all_done` (still holding the lease, still `InProgress`) rather than
//! stopping and leaving that step for something else to notice later. See
//! [`finalize_epic`]'s own doc for the push/PR sequence, the `pr_failed`
//! failure path, and why this also closes the re-claim spin a fully-`Done`-
//! but-still-`InProgress` epic would otherwise cause (T513 left exactly
//! that gap open, by design, for this task to close).
//!
//! ### Failure and cancellation both stop the walk the same way
//!
//! Every exit path out of the loop below — DAG fully done, DAG stuck, epic no
//! longer `InProgress`, lease lost, an implement/commit failure routed to
//! `Blocked(agent_error)` — is a plain `return`: no further writes, ever,
//! after the decision to stop. In particular, cancelling an epic mid-walk (a
//! lane move away from `InProgress`, or another worker stealing the lease)
//! is checked **both** at the top of the loop (the "between tasks" moment)
//! **and** again immediately after the implement stage returns but before
//! the commit/`Done` writes (a slow agent run racing an external cancel must
//! not finalize a task after the cancel landed) — mirroring the same
//! belt-and-suspenders re-check the provisioning-failure call site (just
//! above, in [`run_epic_pipeline_inner`]) already uses around its own
//! [`fail_item`] call. This section describes the DB-boundary half of
//! cancellation — the backstop D12 keeps even after T-542 below adds the
//! actual kill; see that section for the other half (an agent stage
//! terminated *while it's running*, not just observed as stale at the next
//! boundary).
//!
//! ## The test gate & fix loop (T-522, §2.2/§5)
//!
//! [`run_test_gate_loop`] slots into "The real implement walk" above between
//! its step 4 (`Stage::Implement` returns `ok`) and step 5 (`git add -A` +
//! commit-if-dirty): a task's changes land in the working tree, and *then*
//! Dearborn asks "does the project's own `test_cmd` pass against that tree?"
//! before ever staging or committing anything. This is
//! `references/ralph-v2.sh`'s `test_attempt` loop (its `# ---- test gate
//! ----` section, lines ~243–259) reimplemented against Dearborn's
//! stage/evidence machinery rather than bash + log files.
//!
//! ### Commit only at known-green
//!
//! A red `test_cmd` never reaches the commit step — [`process_one_task`]
//! only runs `git add -A`/`git commit` after [`run_test_gate_loop`] returns
//! [`GateOutcome::Proceed`]. This is deliberate, not incidental: an
//! automated pipeline with no human watching each commit land has no cheap
//! way to *un*-commit a red tree once T-530's review loop and T-531's
//! re-review start diffing against `base_sha` — every later stage's "diff
//! since this task started" reasoning only holds if every commit that
//! exists is one the tests actually passed against. Never committing a red
//! tree in the first place is far cheaper than teaching every later stage to
//! tolerate one that might be red, and it matches ralph's own shape
//! (`git add -A`/`git commit` sit *after* the whole `test_attempt` loop in
//! the reference script, not inside it).
//!
//! ### Salvaging completed-but-uncommitted work on an ordinary implement
//! ### failure
//!
//! "Commit only at known-green" describes every commit that follows a green
//! gate — and it stays true: a red `test_cmd`, a fix-loop exhaustion, a red
//! preflight all still leave the tree exactly as the agent left it. But the
//! *implement stage itself* failing is different, because nothing downstream
//! ever runs to give that attempt's work a second chance: the next claim re-
//! provisions the workspace (`git reset --hard HEAD` + `git clean -fd`, see
//! `provision_epic_workspace`) and destroys a dirty tree outright, and the
//! failure triage push only ever pushes what is already committed — so hours
//! of finished work whose only sin was a transient provider hiccup surfacing
//! as one `RunEvent::Error` would simply vanish before any human saw it.
//! The not-ok implement branch therefore salvages first: for an ordinary
//! failure (`timed_out`/`cancelled` excluded), it calls [`commit_if_dirty`]
//! with the same §2.8 `impl(<short id>): <title>` subject step 5 below uses
//! (this diff is the task's first real commit) and the same lease fencing as
//! every other write, committing whatever the agent completed onto the task
//! branch **before** routing to [`route_stage_failure`] — which means
//! [`fail_item`]'s `PushIntent::Attempt(workspace)` triage push then carries
//! the salvaged commit to the remote, so a human triaging the board sees the
//! work instead of losing it. A `cancelled` outcome deliberately skips this:
//! a cancelled task resets to `Todo` and must keep its resumable dirty tree
//! ([`handle_cancelled_task`] depends on that), and `timed_out` gets the
//! same treatment (a deadline kill is a visible stop, not salvageable
//! partial state worth enshrining in history).
//!
//! ### Attempt numbering starts at 0, and why a fix and the gate that follows
//! ### it share a number
//!
//! The **first** gate run — before any fix has ever been attempted — is
//! `attempt = 0`. It isn't a retry of anything, so numbering it "1" would
//! misname the one gate run in the whole loop that has no fix behind it.
//! Every subsequent round bumps the counter *before* running `Stage::Fix`
//! (so `Stage::Fix`'s own row opens at the new, post-increment value), and
//! the gate re-run that immediately follows that fix reuses the *same*
//! value — because it's testing the output of that specific fix round, not
//! starting a new one. A red→red→green run's rows read, in order:
//! `test_gate@0(error) → fix@1(ok) → test_gate@1(error) → fix@2(ok) →
//! test_gate@2(ok)`. This is exactly ralph's own `test_attempt` counter
//! (`references/ralph-v2.sh` initializes `test_attempt=0`, increments it
//! *before* invoking the fix agent, and the following loop iteration's log
//! file name — `test-${test_attempt}.log` — already reflects the bumped
//! value) — Dearborn's `attempt` column is that same counter, just persisted
//! per row instead of encoded in a filename.
//!
//! ### Exhaustion: the task fails, the epic blocks, nothing is committed
//!
//! Once `attempt` reaches `DEARBORN_MAX_TEST_FIX_ATTEMPTS` and the gate is
//! still red, [`run_test_gate_loop`] gives up: [`fail_item`] (T-540's
//! centralized router — see its own section below) sets `task.status =
//! 'Failed'`/`task.failure_reason = 'test_gate_exhausted'` *and* routes the
//! epic to `Blocked` with the identical reason string (D10: a failed task
//! halts its epic immediately). Nothing above this point ever called
//! `git add`, so the dirty tree the last fix round produced simply stays in
//! the workspace exactly as it was — retained on disk (this path never
//! deletes anything, same as every other failure path in this module) but
//! never staged, never committed, never pushed. A human inspecting the
//! retained workspace sees precisely what the last fix attempt left behind,
//! which is the whole point of not committing it: there's nothing to `git
//! revert`, no history to clean up, just an ordinary dirty working tree.
//!
//! ### Why the fix agent sees only the failing output (D19)
//!
//! [`task_agent::assemble_fix_prompt`] builds the `Fix` stage's prompt from
//! `prompts/fix.md` plus *only* this round's test output — never
//! [`task_agent::assemble_prompt`] + [`crate::spec::TaskContext`], which is
//! what `Stage::Implement` gets (the rendered spec, the epic's background,
//! the sibling manifest). See that function's doc for the full rationale
//! and an open concern about it worth a human's attention.
//!
//! ### Lease/cancellation checks inside a long fix loop
//!
//! A test-driven fix loop can run several full agent turns back to back —
//! easily the longest single-task stretch in the whole walk. [`run_test_gate_loop`]
//! re-checks the lease and the epic's `InProgress` status at the top of
//! every iteration (before spending time on a `test_cmd` run) *and* again
//! immediately before every `Stage::Fix` invocation (before spending time on
//! a whole agent turn) — the same belt-and-suspenders discipline the section
//! above describes for the rest of the walk, just applied at finer grain
//! because this loop's body is where a lost lease or a cancelled epic is
//! most likely to be sitting unnoticed the longest.
//!
//! ## Review, verdict, and convergence (T-530)
//!
//! [`run_review_stage`] slots into "The real implement walk" above between
//! step 5 (commit-if-dirty) and step 6 (`Done`, T-513's numbering): once a
//! task's changes are committed, [`process_one_task`] asks a fresh `Ask`-mode
//! agent to review the **cumulative** diff since `base_sha` — the *entire*
//! diff this task has produced across however many commits, not just the
//! latest one — against the task's own rendered spec (its Acceptance
//! Criteria), and to end its reply with exactly one D9 `VERDICT:` line.
//!
//! ### Why the reviewer needs `base_sha` in its own context
//!
//! `prompts/review.md` (T-502) already promised the agent "the base commit
//! SHA this task branched from" and told it to run `git diff <base
//! sha>..HEAD` itself — a promise [`crate::spec::build_context`] could not
//! keep until this task, because nothing populated `TaskContext::base_sha`
//! before now. `process_one_task` already captures `base_sha` at the top of
//! its walk (step 1, before anything could move `HEAD`) for exactly this
//! reason; this task's only new wiring is threading that same string into a
//! second, `Copy`-cloned `TaskContext` (`TaskContext { base_sha:
//! Some(&base_sha), ..task_ctx }`) built right before the review stage runs,
//! never re-derived.
//!
//! ### One reviewer, not ralph's reviewer+judge split
//!
//! `references/ralph-v2.sh` splits this into two agents: a free-form
//! `review` agent that writes findings with no verdict, and a separate
//! `judge` agent (`judge_verdict`, its `# ---- judge ----` section) that
//! reads those findings plus a fresh copy of the spec and *only* emits the
//! machine-readable `VERDICT: ...` line, retried up to `VERDICT_RETRIES`
//! times on a parse miss. `prompts/review.md`'s own doc explains why
//! Dearborn collapses both jobs into one stage instead: the same agent turn
//! writes the findings *and* the verdict, with the verdict required to be
//! the *last* matching line (D9) specifically so a reviewer can front-load
//! its findings and commit to a verdict once it has finished reasoning,
//! instead of a second agent re-deriving the same judgment from a findings
//! transcript it didn't write. [`run_review_stage`] is Dearborn's
//! `judge_verdict` equivalent, just driving one stage instead of two.
//!
//! ### The bounded contract-miss retry
//!
//! `Stage::Review` is `Ask`-mode free text — nothing forces the model to
//! actually end its reply with a parseable `VERDICT:` line, so
//! [`spec::parse_verdict`] returning `None` is a real, expected outcome, not
//! a bug. [`run_review_stage`] handles it exactly like ralph's
//! `judge_verdict` loop: one bounded re-run (`1 +
//! config.executor.verdict_retries` attempts total — **never** a hardcoded
//! `1`, so the config knob is never re-derived at this call site) of the
//! **same** review prompt with [`VERDICT_CONTRACT_REMINDER`] appended — a
//! short, literal restatement of the exact required line, not a second
//! review request. If the re-run still doesn't parse, the contract is
//! considered broken: [`fail_item`] routes the task to `Failed(agent_error)`
//! and the epic to `Blocked` — the same T-540 router T-522's exhausted
//! test-gate loop calls, reused rather than duplicated, because both are
//! "this stage could not produce a usable result after its bounded retries"
//! in the same shape.
//!
//! ### Both raw outputs survive, by construction
//!
//! The miss and the re-run are **two separate calls** into
//! [`task_agent::run_agent_stage`], each opening its own `agent_run` row
//! (D13: every stage that runs gets a row) at successive `attempt` values —
//! nothing here overwrites or discards the first attempt's transcript before
//! trying again, so a human looking at `GET /tasks/{id}/runs` after a
//! contract-miss failure sees both the agent's original (unparseable) reply
//! and the reminder-prompted re-run's reply, in order. Attempt numbering for
//! this stage starts at `1` for the reviewer's first try at *this task's*
//! verdict and increments once per contract-miss retry — a much shallower
//! counter than T-522's fix-loop `attempt` (which also counts *test*
//! re-runs), because there is nothing to retry here except the parse itself;
//! T-531 introduces the review-*round* concept (re-reviewing after a
//! `NEEDS_CHANGES` fix) and will need to decide how round and contract-miss
//! attempt compose — deliberately left to that task, not pre-built here.
//!
//! ### Storing the verdict after the row is already closed
//!
//! [`task_agent::run_agent_stage`] closes its `agent_run` row with
//! `verdict: None` unconditionally (T-512 has no idea whether a given stage
//! even emits a verdict) — by the time [`run_review_stage`] has parsed
//! [`task_agent::AgentStageOutcome::text`] for a `VERDICT:` line, the row is
//! already closed and its `StageHandle` is gone.
//! [`task_agent::AgentStageOutcome`] now carries the row's own id
//! (`agent_run_id`, stamped by `run_agent_stage` right after the drain
//! finishes) precisely so a caller in this position can go back and set the
//! column with [`evidence::set_verdict`] — a plain, independent `UPDATE ...
//! WHERE id = ?` — instead of `CloseStage` growing a second "verdict, but
//! only sometimes" field every non-review caller would have to remember to
//! pass `None` for.
//!
//! ### `stage_changed`, and why it's a shared helper
//!
//! Once a verdict is known and recorded, [`publish_stage_changed`] fans out
//! MILESTONE_2 §2.6's `{ task_id, stage, attempt, status, verdict? }` frame
//! on **two** topics: `task:<id>` (a task detail view already subscribed to
//! the stage's `RunEvent` firehose gets the same summary a `dag_updated`-
//! style consumer would want) and, coarse, `epic:<id>` (so a project board or
//! epic detail view can drive a task card's sub-label — "reviewing", "2nd
//! review round" — without subscribing to every task's token stream, exactly
//! the concern CONVENTIONS.md's `task:<id>` section already explains for the
//! `RunEvent` firehose itself). One small function rather than two inlined
//! `state.hub.publish(...)` calls at the one call site this task adds,
//! because "publish the same payload on two topics" is exactly the kind of
//! thing a second call site (T-531's re-review rounds, or a future non-review
//! stage transition) would otherwise be tempted to copy-paste instead of
//! reuse.
//!
//! ### The reviewer cannot edit files
//!
//! `Stage::Review.run_mode()` is `RunMode::Ask` and
//! `Stage::Review.denies_edit_tools()` is `true` (both decided in T-512,
//! `task_agent.rs`) — this task adds no new enforcement, only a test in this
//! module's own `mod tests` asserting it directly via
//! [`task_agent::build_extra_args`], per this task's own AC line ("the
//! reviewer cannot edit files"). See `task_agent.rs`'s "soft read-only
//! enforcement" doc section for the caveat MILESTONE_2 §11 risk 2 already
//! names: `--disallowedTools` narrows Edit/Write/MultiEdit/NotebookEdit, not
//! `Bash` — the real backstop is the test gate plus this very
//! cumulative-diff review, not the permission flag.
//!
//! ### `PASS`, `NEEDS_CHANGES`, and `BLOCKED` each get their real treatment
//!
//! [`process_one_task`] hands every verdict to
//! [`run_review_fix_converge`] (T-531, next section): `PASS` proceeds
//! straight to `Done`, exactly as an epic with no review stage at all would
//! have; `BLOCKED` routes through [`fail_item`] as `Failed(blocked)` (§2.3's
//! reason for "the agent returned BLOCKED"); and
//! `NEEDS_CHANGES` re-enters `Stage::Fix` and re-reviews against the same
//! `base_sha`, capped by `MAX_FIX_ROUNDS` — the real ralph-equivalent
//! treatment (`references/ralph-v2.sh`'s `# ---- review / judge / fix loop
//! ----`) this section originally deferred to T-531. See the next section for
//! the full loop.
//!
//! ### No review for a no-diff task — routes to `Stage::VerifyComplete` instead
//!
//! [`process_one_task`] only reaches the review/convergence loop above inside
//! the "there is something to commit" branch — a task whose implement stage
//! produced no diff (the agent judged the task already satisfied) never runs
//! `Stage::Review` at all, exactly as it never ran `Stage::Commit` either. It
//! runs `Stage::VerifyComplete` instead — see "Already-complete verification
//! (T-532)" below.
//!
//! ## Review → fix → re-test → re-commit (T-531, §6)
//!
//! [`run_review_fix_converge`] is the loop `references/ralph-v2.sh`'s
//! `# ---- review / judge / fix loop ----` reimplements: a `NEEDS_CHANGES`
//! verdict is not a terminal failure by itself — it means "one more round of
//! `Stage::Fix`, driven by the reviewer's own findings, then re-test, then
//! re-commit, then ask the reviewer again against the identical `base_sha`."
//! `PASS` on any round closes the task exactly as a first-try `PASS` always
//! did; `BLOCKED` on any round fails immediately (a human must resolve it, no
//! amount of re-fixing helps); only exhausting `MAX_FIX_ROUNDS` while still
//! `NEEDS_CHANGES` is a real failure, `Failed(review_not_converged)`.
//!
//! ### Two independent counters, not one — and why
//!
//! This loop tracks **two** numbers, deliberately kept separate rather than
//! collapsed into a single "round" integer:
//!
//! - **`round`** — the business-facing counter ralph's own script names
//!   (`round=$(( round + 1 ))`, its log lines, its `fix(...) review round
//!   N` commit subject). It increments exactly once per `NEEDS_CHANGES` —
//!   never on the loop's first, baseline review — and is what
//!   `MAX_FIX_ROUNDS` bounds: `round > max_fix_rounds` is the exhaustion
//!   check.
//! - **`review_attempt`** — the `agent_run.attempt` value threaded into
//!   [`run_review_stage`]'s new `start_attempt` parameter. The baseline
//!   review opens at `0` — not a retry or a re-review of anything, exactly
//!   mirroring T-522's `test_gate@0` convention above. Every fix opens its
//!   own row at `used_attempt + 1` (the review call's own returned
//!   `attempt`, which may itself have advanced past `start_attempt` if that
//!   review needed a contract-miss retry — see below), and the re-review
//!   that follows a fix reuses that **same** value as its own
//!   `start_attempt`. A red→red→green shape (two `NEEDS_CHANGES` rounds
//!   then `PASS`, no contract misses) reads: `review@0(NEEDS_CHANGES) →
//!   fix@1(ok) → review@1(NEEDS_CHANGES) → fix@2(ok) → review@2(PASS)` —
//!   the exact same "a fix and the [stage] that follows it share a number"
//!   shape T-522's module doc section above documents for `test_gate`/`fix`,
//!   with `review` standing in for `test_gate`.
//!
//!   Why not just use `round` as the attempt value directly (the simplest
//!   possible reading of "mirror T-522")? Because `run_review_stage`'s own
//!   contract-miss retry (T-530, unchanged by this task) can consume more
//!   than one attempt value within a single round — if round *R*'s review
//!   needs one retry, that call alone spans attempts `[start, start+1]`. If
//!   `round` itself were the attempt number, round *R+1*'s baseline review
//!   would then collide with round *R*'s own contract-miss retry (both
//!   `stage='review'` rows claiming the identical `attempt`), which breaks
//!   the very property MILESTONE_2 §2.6 wants `attempt` for — a client
//!   subscribed to `stage_changed` frames driving a task card's "2nd review
//!   round" sub-label (the module doc's T-530 section already flags this as
//!   the payload's future consumer) needs `attempt` to identify a round
//!   *unambiguously*, not just "usually." Deriving each round's starting
//!   attempt from the *previous* round's actual last-used attempt (rather
//!   than from a fixed stride or the round number itself) is the simplest
//!   scheme that guarantees no two different rounds' `review` rows ever
//!   share an `attempt`, while still keeping the common (no-contract-miss)
//!   case read exactly like T-522's `gate@N`/`fix@N` pairing.
//!
//!   One known, accepted imprecision: `run_test_gate_loop` (reused
//!   unmodified — see below) always restarts its **own** internal
//!   `test_gate`/`fix` attempt counter at `0` on every call, independently
//!   of `review_attempt`/`round`. A review round's own `Stage::Fix` row and
//!   a *nested* test-driven `Stage::Fix` row (from that same round's
//!   post-fix `run_test_gate_loop` call, if the review-driven fix happened
//!   to also break the tests) can therefore legitimately share an
//!   `attempt` value while being two different events — distinguishable by
//!   `created_at` order and by `log` content (review findings vs. test
//!   output), just not by `attempt` alone. Avoiding this fully would mean
//!   threading a starting offset into `run_test_gate_loop` too, which this
//!   task does not do — the instruction to reuse that loop **unmodified**
//!   (not extend its own numbering scheme) is more important than closing
//!   this narrow, already-legible ambiguity.
//!
//! ### Why `review_prompt` is never rebuilt between rounds
//!
//! Unlike a test-driven fix round (whose prompt embeds the specific failing
//! `test_cmd` output, different every retry), the review prompt never
//! embeds a diff at all — `prompts/review.md` tells the agent to run `git
//! diff <base_sha>..HEAD` itself. Reusing the exact same `review_prompt`
//! string on every round (built once by [`process_one_task`], passed down
//! unchanged) is therefore not a shortcut, it *is* "each round re-reviews
//! the cumulative diff": `base_sha` never advances (T-531's AC line, and D9)
//! and the instruction to diff against it never changes; only `HEAD` moves
//! (via each round's own fix commit), so the same `git diff` command the
//! agent runs turns up more content each round for free.
//!
//! ### A fix round with no diff: skip the commit, keep the round counter
//!
//! A review-driven `Stage::Fix` can legitimately produce no changes at all
//! (the agent judged the reviewer's own findings already addressed, or
//! disagreed and made no edit). [`commit_if_dirty`] — the exact same "no
//! diff is committed as nothing" helper T-513's original `impl(...)` commit
//! step now also goes through — makes this a silent no-op rather than an
//! error: no `fix(...) review round N` commit lands, `HEAD` doesn't move,
//! and the loop proceeds straight to the re-review. Crucially, **`round`
//! already advanced** the moment the `NEEDS_CHANGES` verdict was seen,
//! before the fix even ran — so a reviewer that keeps returning
//! `NEEDS_CHANGES` against a fix agent that keeps producing no diff still
//! terminates in exactly `MAX_FIX_ROUNDS` rounds (`Failed(review_not_
//! converged)`), not an infinite loop. This is deliberately the *only*
//! guard against a stuck no-op fix — there is no separate "N consecutive
//! no-diff rounds" counter, because the round bound already covers it for
//! free.
//!
//! ### Reusing `run_test_gate_loop` unmodified — a fix that breaks the
//! ### tests never gets committed
//!
//! After each review-driven `Stage::Fix`, the loop calls
//! [`run_test_gate_loop`] — the identical T-522 function, no new parameter,
//! no forked copy — before ever staging or committing anything. This is the
//! same "commit only at known-green" discipline T-522's module doc section
//! above explains at length, just invoked a second (and third, …) time per
//! task: if the review-driven fix broke the tests, `run_test_gate_loop`
//! tries its own bounded test-driven fix retries and, failing to recover,
//! fails the *task* itself (`Failed(test_gate_exhausted)`) and blocks the
//! epic from **inside that call** — `run_review_fix_converge`'s only job on
//! `GateOutcome::Stop` is to stop, exactly like every other reused-helper
//! call site in this module. The task never reaches
//! `Failed(review_not_converged)` in this scenario; `test_gate_exhausted` is
//! the more precise reason (the tests, not the reviewer, are why the task
//! failed), and reusing the existing helper is what makes that the natural
//! outcome rather than something this task has to special-case.
//!
//! ### Belt-and-suspenders checks, same discipline, more of them
//!
//! A full round (fix → test-gate-with-its-own-retries → commit → re-review)
//! is easily the longest stretch of agent turns anywhere in this walk.
//! [`run_review_fix_converge`] re-checks `lease.is_lost()` and
//! `container_still_in_progress` at the top of every round, immediately before
//! the fix stage, immediately before the post-fix test gate, and
//! immediately before the commit — the same pattern T-522's fix loop and
//! T-530's review call already established, just applied at every one of
//! this loop's several pause points instead of one or two.
//!
//! ## Already-complete verification (T-532, D5, §6)
//!
//! [`run_verify_complete`] is `references/ralph-v2.sh`'s "already-complete"
//! path (its `# ---- already-complete verification ----` section, run
//! whenever the implement stage's `git diff` came back empty) reimplemented
//! against Dearborn's stage/evidence machinery: an implement stage can
//! legitimately produce **no diff** at all — the agent judged this task's
//! acceptance criteria already satisfied by earlier work in the epic (D8's
//! sibling manifest exists precisely so it can reach that judgment) — and
//! that judgment deserves independent verification before Dearborn trusts it
//! enough to close the task with zero commits. `process_one_task`'s step 5
//! (`commit_if_dirty` returning `None`) routes here instead of straight to
//! `Done`; see "No review for a no-diff task", above, for where this plugs
//! into the walk.
//!
//! ### One more verdict-emitting stage, not a special case
//!
//! `Stage::VerifyComplete` is `Ask`-mode with edit tools denied, exactly like
//! `Stage::Review` (`task_agent.rs`'s `Stage::run_mode`/`denies_edit_tools`,
//! decided in T-512, unchanged by this task) — an independent agent that
//! reads the code and ends its reply with the identical D9 `VERDICT:` line
//! `prompts/verify_complete.md` (T-502) already promised it would. Because
//! the contract is identical, this task **generalizes** T-530's
//! `run_review_stage` into [`run_verdict_stage`] (a `stage: Stage` parameter
//! added where the function used to hardcode `Stage::Review`) rather than
//! writing a second copy of the retry-bounding/verdict-storage logic — see
//! that function's own doc for the full contract-miss/storage rationale,
//! which did not change, only which stage it's parameterized over.
//! [`run_review_fix_converge`]'s own call site was updated to pass
//! `Stage::Review` explicitly; behavior there is otherwise identical.
//!
//! ### Not a diff review — `base_sha` stays `None`
//!
//! Unlike `Stage::Review`'s context, `Stage::VerifyComplete`'s prompt
//! (`prompts/verify_complete.md`, "This is NOT a diff review" section)
//! explicitly tells the agent there is no meaningful diff to read and to
//! verify the **end state** of the code instead — so [`run_verify_complete`]
//! is handed the *same* `task_ctx` [`process_one_task`] built for
//! `Stage::Implement` (`base_sha: None`), never a `base_sha`-bearing copy the
//! way the review branch builds one. `spec::build_context`'s "Base Commit"
//! section simply never renders for this stage, matching the prompt's own
//! instruction exactly.
//!
//! ### `PASS`, `BLOCKED`, and `NEEDS_CHANGES` — three real branches, per the AC
//!
//! - **`PASS`** — the claim holds; [`run_verify_complete`] returns
//!   [`TaskStepOutcome::Continue`] and `process_one_task` proceeds straight to
//!   `Done` with the branch's commit count **unchanged** — no `Stage::Commit`
//!   row, no `Stage::Review` row, ever, on this path. This is the AC's
//!   headline scripted test.
//! - **`BLOCKED`** — routed through [`fail_item`] as `Failed(blocked)`,
//!   byte-for-byte the same treatment a `BLOCKED` review verdict gets
//!   (§2.3's reason for "the agent returned BLOCKED" makes no distinction
//!   by stage).
//! - **`NEEDS_CHANGES`** — the interesting case, and where MILESTONE_2 §6's
//!   own wording ("route findings to `Fix` and **re-enter the normal
//!   pipeline**") is load-bearing: this is deliberately *not* a bounded
//!   "verify-complete round" loop mirroring T-531's review rounds. Instead,
//!   [`run_verify_complete`] runs **exactly one** `Stage::Fix` off the
//!   verifier's findings (D19: [`task_agent::assemble_fix_prompt`], the
//!   identical helper T-522/T-531 use — no spec, no epic context, only the
//!   findings), then calls the *same* [`run_test_gate_loop`] and
//!   [`commit_if_dirty`] [`process_one_task`]'s own step 4/5 call — with the
//!   identical `impl(<short id>): <title>` §2.8 subject, because this fix's
//!   diff **is** the task's first real commit, not a secondary "fix" commit
//!   layered on top of one that doesn't exist yet. If that produces a commit,
//!   control falls straight into the unmodified T-530/T-531
//!   [`run_review_fix_converge`] loop — from here on, a task that started
//!   life as "implement wrote nothing" is indistinguishable in the evidence
//!   trail's *shape* from an ordinary implemented task: one `impl(...)`
//!   commit, a baseline review at `attempt=0`, and (if `NEEDS_CHANGES`) real
//!   review rounds on top of it. This is what "re-enter the normal pipeline"
//!   means literally: zero new pipeline code runs after the one `Stage::Fix`
//!   call — every step past it is a call into a helper `Stage::Implement`'s
//!   own path already uses.
//!
//! ### Attempt numbering: verify-complete slots in exactly where review would
//!
//! `Stage::VerifyComplete`'s own [`run_verdict_stage`] call opens at
//! `start_attempt = 0` — not a retry of anything, mirroring the baseline
//! review's identical convention (T-531's module-doc section). A
//! `NEEDS_CHANGES` verdict's `Stage::Fix` opens at `used_attempt + 1` (`1` in
//! the common no-contract-miss case) — the same "a fix and the [verdict
//! stage] that follows it share a number" scheme T-522 established for
//! `test_gate`/`fix` and T-531 reused for `review`/`fix`, with
//! `verify_complete` now standing in for `test_gate`/`review` as the stage a
//! fix's attempt number is borrowed from. `run_test_gate_loop`'s own nested
//! `test_gate`/`fix` counter restarts at `0` regardless (reused unmodified,
//! same accepted imprecision T-531's module-doc section already names for
//! its own nested test-gate call), and once control reaches
//! `run_review_fix_converge`, that loop's `review_attempt`/`round` counters
//! start fresh at `0` — a task that took this path reads, in the common case,
//! `verify_complete@0(NEEDS_CHANGES) → fix@1(ok) → test_gate@0(ok) →
//! commit@1 → review@0(PASS)`: legible on its own, and identical in shape to
//! T-522's own `gate@N`/`fix@N` pairing.
//!
//! ### An unscripted no-op fix fails rather than silently closing `Done`
//!
//! A `NEEDS_CHANGES`-driven `Stage::Fix` can, in principle, still produce no
//! diff (the fix agent disagreed with the verifier, or simply declined to
//! act). Unlike T-531's own "a fix round with no diff" case — which loops
//! back to re-review the unchanged diff, relying on `MAX_FIX_ROUNDS` to bound
//! a reviewer that keeps disagreeing — there is no existing diff here to
//! re-review, and looping back into a second `Stage::VerifyComplete` call
//! would be an unbounded ping-pong between two verdict-emitting stages with
//! nothing shaped like `MAX_FIX_ROUNDS` to stop it. [`run_verify_complete`]
//! instead fails the task `Failed(agent_error)`: never silently closing
//! `Done` a task the verifier just said was **not** complete is more
//! important than trying to recover automatically from an agent declining to
//! act on its own findings. This is deliberately conservative — a judgment
//! call MILESTONE_2 §6 does not specify directly, locked in by this module's
//! own `verify_complete_needs_changes_with_a_no_op_fix_fails_rather_than_closing_done`
//! test and flagged here for a human to double-check rather than quietly
//! assumed.
//!
//! ### Visible in the run history, by construction
//!
//! The AC's "a human can see *why* nothing was built" falls out of reusing
//! [`run_verdict_stage`] rather than inventing a parallel evidence path: the
//! `verify_complete` `agent_run` row it opens carries the verdict
//! ([`evidence::set_verdict`]) exactly like a review row does, so `GET
//! /tasks/{id}/runs` lists it with `stage='verify_complete'`,
//! `verdict='PASS'` (or `NEEDS_CHANGES`/`BLOCKED`) for any task that took
//! this path — nothing extra to build for this half of the AC.
//!
//! ## T-540: structured failure & Blocked (§2.3, §7)
//!
//! Every section above this one was written against a *scattered* failure
//! story: T-513's tracer bullet introduced `block_epic_on_agent_error`
//! (blocks the epic, leaves the failing task `InProgress`); T-522 needed
//! more — a **task**-level `Failed(reason)` T-541's retry contract could find
//! — and added a second, narrower helper (`fail_task_and_block_epic`)
//! rather than fix the first one, explicitly deferring that unification to
//! "T-540" by name in both helpers' own doc comments; T-511's provisioning
//! failures and T-514's finalize failures each grew their own thin
//! `block_epic_on_*` wrapper around a shared `set_epic_blocked` write. Four
//! call shapes, one inconsistency (a failing task's own `status` depended on
//! *which* helper happened to fail it), and §2.3's full ten-reason
//! vocabulary reachable only by accident of which call site a future task
//! happened to touch.
//!
//! [`fail_item`] replaces all of it: **one** router, taking a
//! [`FailureContext`], that every failure path above (and T-543's timeout
//! route, once it landed — see "T-543: agent stage timeouts" near the end of
//! this doc) now calls. [`FailureReason`] makes §2.3's vocabulary a
//! type instead of bare string literals — the same discipline [`Stage`]
//! already applies to §2.2 — so a reason reaching [`fail_item`] is something
//! the compiler enforces (a match must name every variant) and a test
//! enumerates ([`FailureReason::ALL`]) rather than something only visible by
//! grepping call sites.
//!
//! ### One shape for a task-scoped and a no-task failure
//!
//! [`FailureContext::task_id`] is `Option<&str>`: `Some` for every failure
//! that has one task at fault (`agent_error`, `test_gate_exhausted`,
//! `review_not_converged`, `blocked`, and T-543's `timeout` —
//! `FailureContext` can express `cancelled` too, but T-542's cancel path
//! never actually constructs one; see "T-542: cancellation as a kill"
//! below), `None` for the four that don't (`preflight_red`,
//! `setup_failed`, `workspace_error` — the DAG walk never even started — and
//! `pr_failed` — every task already finished; the failure is finalize's
//! own). [`fail_item`] only ever touches the `task` table when `task_id` is
//! `Some`; every other step (the epic write, the publishes, the push) runs
//! unconditionally. This is what "a failure with no associated task and one
//! with a task both fit without a second function" (this task's own design
//! brief) means concretely: nothing about the router's shape assumes a task
//! exists.
//!
//! ### Fixing the inconsistency: every failure now reaches `Failed`/`Blocked`
//! ### together
//!
//! The five former `block_epic_on_agent_error` call sites (in
//! [`process_one_task`], [`run_verdict_stage`], and
//! [`run_verify_complete`]) used to leave the failing task `InProgress` —
//! the exact gap those functions' own (now-deleted) doc comments named T-540
//! as the fix for. Migrating them onto [`fail_item`] with `task_id: Some(..)`
//! closes it: every one of those five now sets `task.status = 'Failed'`
//! (with the identical `agent_error` reason the epic gets) as part of the
//! same call. A base-`sha` read failure is a special case worth naming: it
//! happens *before* [`process_one_task`]'s own `Todo → InProgress` write, so
//! pre-T-540 that task was left `Todo`, not even `InProgress` — post-T-540 it
//! reaches `Failed(agent_error)` exactly like every other implement-walk
//! failure, which is the more useful state for a human (and for T-541's
//! retry) to find it in either way.
//!
//! ### Push, and where it's skipped (§7: "push the epic branch so the user
//! ### clones & triages locally")
//!
//! [`fail_item`]'s last step, [`push_on_failure`], pushes whatever is
//! already committed on the failing epic's branch — never anything more,
//! because [`push_on_failure`] itself never stages anything: only explicitly
//! committed content can reach `origin` through it. What changed (with the
//! implement-failure salvage above) is *which* commits can be sitting there
//! when it runs: an ordinary implement failure now runs [`commit_if_dirty`]
//! *before* routing to [`fail_item`], so the triage push deliberately
//! carries that salvaged commit — that is the point of salvaging at all.
//! Everything else about the old structural guarantee still holds: a
//! timed-out/cancelled outcome skips the salvage (a cancelled task keeps its
//! resumable dirty tree), every other failure path still has nothing between
//! its last commit and its failure, and raw uncommitted working-tree state
//! still cannot reach `origin` through this function by construction, not by
//! a check it has to remember to perform.
//!
//! Whether to even attempt it is [`PushIntent`], decided per call site:
//!
//! - **`workspace_error`/`setup_failed`** — `PushIntent::Skip`, and not by
//!   choice: the old `block_epic_on_provision_failure` helper's call site
//!   (now inlined directly in [`run_epic_pipeline_inner`]) never obtains a
//!   [`ProvisionedWorkspace`] at all — [`workspace::provision_epic_workspace`]
//!   returned `Err`, so there is no local clone/branch this process knows
//!   about to push. There is nothing to skip *past*; the type system simply
//!   never offers `Attempt` a value to construct here.
//! - **`preflight_red`** — `PushIntent::Attempt`, even though a *first*
//!   claim's preflight failure pushes a branch with nothing Dearborn-authored
//!   on it yet (harmless — it just mirrors canonical under the epic's branch
//!   name). [`run_preflight`] runs after provisioning fully succeeds, so a
//!   real, checked-out [`ProvisionedWorkspace`] exists; on a *re-claim*'s
//!   preflight failure that branch may already carry earlier tasks' committed
//!   work from a prior successful claim, which is exactly what a human
//!   triaging the board benefits from seeing.
//! - **every task-scoped reason** (`agent_error`, `test_gate_exhausted`,
//!   `review_not_converged`, `blocked`) — `PushIntent::Attempt`: every call
//!   site sits inside [`process_one_task`]'s walk or a function it calls,
//!   all of which already hold a [`ProvisionedWorkspace`].
//! - **`pr_failed`** — `PushIntent::Skip`: [`finalize_epic`] *is* the push
//!   (and, on success, the open-PR call) — it already ran its own push
//!   attempt with its own `Stage::Push` evidence row before ever reaching a
//!   failure exit. Routing back through [`fail_item`]'s own push step would
//!   either push nothing new (a project/PAT load failure, which happens
//!   before any push attempt) or push a second, redundant time (the
//!   push-itself-failed and open-PR-failed cases, each already evidenced).
//!
//! ### A push failure is never fatal to the failure it's trying to surface
//!
//! By the time [`push_on_failure`] runs, the task (if any) is already
//! `Failed` and the epic is already `Blocked(ctx.reason)` — both committed
//! writes, not provisional. A push failure here can only add a
//! `Stage::Push` evidence row with `status = 'error'` and a `tracing::warn!`
//! line; there is no code path from it back to `ctx.reason`, so the epic's
//! `blocked_reason` can never become `pr_failed` as a side effect of a
//! *different* failure's best-effort triage push failing to land.
//!
//! ### Losing the fencing race skips the push too
//!
//! [`fail_item`]'s epic `UPDATE ... WHERE status = 'InProgress'` is fenced
//! exactly like the pre-T-540 `set_epic_blocked` was — a race with an
//! external `Cancel` (T-542) makes it a no-op. When that happens (`took_epic
//! == false`), [`fail_item`] skips the push step entirely, even if
//! `ctx.push` was `Attempt`: something else already moved this epic on, and
//! pushing on its behalf would be guessing at intent that belongs to
//! whatever actually won the race — the same "no further writes once you
//! observe you no longer own this" discipline every other pause point in
//! this walk already follows.
//!
//! ### The lease, and moving on to the next epic
//!
//! [`fail_item`] never touches the lease — releasing it remains
//! [`try_claim_and_run`]'s job, exactly as it was before this task. Every
//! call site's failure branch still ends in a plain `return`/`Stop`
//! (unchanged by this task) that propagates back up through
//! [`run_epic_pipeline_inner`] to [`try_claim_and_run`], which releases the
//! lease and lets [`worker_loop`]'s inner loop try another claim immediately
//! — a failure is epic-scoped (D10, §7), so the worker is free to pick up a
//! different epic (or the same project's next one) on its very next
//! iteration, no poll-interval delay involved.
//!
//! ### `timeout`/`cancelled`: one constructed, one deliberately never
//! ### (revised by T-542, settled by T-543)
//!
//! [`FailureReason::Timeout`] and [`FailureReason::Cancelled`] both exist in
//! the enum and are handled by [`fail_item`] exactly like every other
//! reason — [`FailureReason::ALL`]'s own test still drives both through the
//! router directly to prove the plumbing works generically. `Timeout` is now
//! genuinely constructed: [`route_stage_failure`]'s `outcome.timed_out`
//! branch (T-543, see that function's own doc) is the one and only call site
//! that ever builds a `FailureContext { reason: FailureReason::Timeout, .. }`
//! — a timed-out stage takes T-540's ordinary `fail_item` route, same as
//! `AgentError`, just with a more precise reason string. `Cancelled` is
//! different: T-542 (below) landed and, after actually building the cancel
//! path,
//! **deliberately does not** route a cancelled stage through [`fail_item`]
//! at all — the paragraph above this section, written before T-542 existed,
//! predicted "they only ever need to call `fail_item` with the right
//! reason"; building the feature showed that prediction wrong. See the
//! "T-542: cancellation as a kill" section immediately below for why:
//! `fail_item`'s task write is unconditionally `Failed`, but a cancelled
//! task must land `Todo` (T-542's own AC), so `fail_item` cannot be reused
//! unmodified — and modifying it to branch on `Cancelled` would have made
//! the one router two routers wearing a single function's skin. This is a
//! documented deviation from this section's original plan, not an oversight:
//! `FailureReason::Cancelled` stays defined (§2.3 names `cancelled` as a
//! valid `task.failure_reason`/`epic.blocked_reason` value, and
//! [`fail_item`] genuinely can express it if some future call site ever
//! legitimately needs to), it is simply never the reason a *cancel* reaches
//! `Failed` through, because a cancel never reaches `Failed` at all.
//!
//! ## T-542: cancellation as a kill (§7, D12)
//!
//! Every section above this one describes cancellation as something the
//! walk *notices* — a DB read at a boundary that happens to find the epic no
//! longer `InProgress`. That is real and stays load-bearing (see "Failure
//! and cancellation both stop the walk the same way", above), but by itself
//! it only stops the walk **between** stages; an agent turn already in
//! flight runs to its own completion regardless, which could be however
//! long `claude` takes to finish its current turn. D12 ("Cancel is a
//! **kill**") and this task's AC ("terminates it in seconds, not at the next
//! stage boundary") require more: the in-flight process itself has to die.
//!
//! ### The registry and the guard
//!
//! [`AppState::cancel_registry`] holds the live [`harness::RunHandle`] for
//! whatever agent stage is currently running, keyed by the claimed item's
//! id. [`task_agent::run_agent_stage`] populates it — via a private
//! `task_agent::CancelGuard`, RAII, so the entry is removed on **every**
//! exit path (normal completion, an ordinary error, a harness spawn
//! failure, a panicked drain thread, or a cancel itself) by construction,
//! not by review — for the duration of exactly one agent stage. Because
//! `run_agent_stage` is the single choke point every agent stage already
//! goes through (implement/fix/review/verify_complete/summarize alike, per
//! D6), this is what makes every one of them cancellable with **zero**
//! opt-in from any call site in this module: `process_one_task`,
//! `run_test_gate_loop`, `run_verdict_stage`, `run_review_fix_converge`, and
//! `run_verify_complete` call `run_agent_stage` exactly as they did before
//! T-542, and the registration/removal happens underneath them. See
//! [`AppState::cancel_registry`]'s own doc for the registry's full shape
//! (including the 1:1-not-1:many assumption it leans on) and
//! `task_agent::run_agent_stage`'s doc for exactly when an entry exists.
//!
//! Non-agent stages (`setup`/`preflight`/`test_gate`/`commit`/`push`) never
//! register a handle — there is no `RunHandle` for a shell command, only a
//! [`crate::cmd`] child process this module does not expose for killing.
//! MILESTONE_2 T-542's own AC only asks for agent-stage cancellation; a
//! cancel that lands while a shell stage is running is caught by the
//! ordinary stage-boundary check the moment that command returns, exactly as
//! it always was — the "stage-boundary DB check still catches a cancel
//! issued between stages" AC.
//!
//! ### Issuing the kill (`lanes.rs`)
//!
//! [`crate::lanes::set_epic_lane`]'s `InProgress → Cancelled` transition is
//! the only thing that ever calls `RunControl::cancel()`. See that module's
//! own doc for the full sequence; the short version: the epic's `status`
//! column commits `Cancelled` first, *then* the registry is consulted — so a
//! cancelled outcome this module later observes is guaranteed to find the
//! epic already `Cancelled` in the DB, never a race against it. The lookup
//! finding nothing is a silent, correct no-op (D12's stage-boundary backstop
//! applies). `RunControl::cancel()` itself is fire-and-forget — it signals
//! the process and returns immediately, it does not block on the process
//! actually exiting — so the HTTP handler issuing the cancel never waits on
//! this module noticing it.
//!
//! ### Observing the kill: `handle_cancelled_task`, not `fail_item`
//!
//! Every call site in this module that inspects an
//! [`task_agent::AgentStageOutcome`] for `is_ok() == false` now routes
//! through [`route_stage_failure`] instead of calling [`fail_item`]
//! directly. That helper checks `outcome.cancelled` first: an ordinary
//! failure (a non-zero exit, an `Error` event, no exit at all) still goes to
//! [`fail_item`] exactly as before T-542; a cancelled outcome goes to
//! [`handle_cancelled_task`] instead. The two cannot share `fail_item`'s
//! path unmodified because their task-side writes disagree by design: a
//! *failure* sets the task `Failed(reason)` so [`crate::tasks::retry_task`]
//! (T-541) can find and resume it; a *cancellation* sets the task back to
//! `Todo` directly — T-542's own AC — because nothing about the task's work
//! was wrong, a human just asked to stop. [`handle_cancelled_task`] also
//! does **not** touch the epic (already `Cancelled`, written by
//! `lanes::set_epic_lane` before it ever looked in the registry) and does
//! **not** push anything (nothing between a task's last successful commit
//! and a mid-stage cancellation ever calls `git add`/`git commit`, so there
//! is nothing new on top of what a prior stop-and-triage push already
//! covered) — see that function's own doc for the complete accounting.
//!
//! The net effect satisfies every clause of this task's AC without a new
//! epic-level write: the task returns to `Todo` (resumable, not `Failed`),
//! the epic stays exactly `Cancelled` (never flipped to `Blocked` — there is
//! no epic write here at all to race `fail_item`'s fencing against), the
//! lease is released and the workspace retained by the same
//! [`try_claim_and_run`]/`return`-propagation machinery every other stop
//! path already uses, and `finalize_epic` (the only place a PR ever opens)
//! is never reached because the walk stops mid-task, long before the DAG
//! could ever read fully `Done`.
//!
//! ## T-543: agent stage timeouts (D18)
//!
//! D18 ("per-stage wall-clock timeouts... no epic-level budget") is enforced
//! for agent stages inside [`task_agent::run_agent_stage`] itself — see that
//! function's own "T-543: agent stage timeouts" doc section for the
//! deadline/grace-period mechanics — never at a call site in this module.
//! What belongs here is the other half: once `run_agent_stage` hands back a
//! not-`ok` [`task_agent::AgentStageOutcome`] whose `timed_out` field is set,
//! what does the walk *do* about it?
//!
//! ### The same choke point T-542 already built
//!
//! Every call site in this module that inspects an agent stage's outcome —
//! `process_one_task`'s implement step, `run_test_gate_loop`'s fix step,
//! `run_verdict_stage`'s review/verify-complete step,
//! `run_review_fix_converge`'s review-driven fix step — already goes through
//! [`route_stage_failure`] (T-542's own choke point, built to decide
//! ordinary-failure-vs-cancelled). T-543 does not add a fourth call site or a
//! second router: it adds a third branch, checked *first*, to the one that
//! already exists. See [`route_stage_failure`]'s own doc for the exact
//! three-way decision and why `timed_out` has to be checked before
//! `cancelled` rather than after (a deadline-killed outcome has
//! `cancelled: true` too — the kill mechanism is D12's single
//! `RunControl::cancel()`, T-542's and T-543's own "T-543" doc section make
//! the same point from the `task_agent.rs` side).
//!
//! ### An implement timeout blocks the epic; a fix/review timeout fails that
//! ### stage's own way
//!
//! Because `route_stage_failure`'s `timed_out` branch calls the *identical*
//! [`fail_item`] every ordinary `AgentError` failure already calls (just with
//! [`FailureReason::Timeout`] instead), a timed-out stage inherits whichever
//! call site it happened at, with no new logic to keep in sync:
//!
//! - A timed-out `Stage::Implement` (`process_one_task`) fails the task
//!   `Failed(timeout)` and blocks the epic `Blocked(timeout)` exactly as an
//!   ordinary implement failure does — this task's own headline AC line.
//! - A timed-out `Stage::Fix` inside the test-gate loop
//!   (`run_test_gate_loop`) fails exactly as that loop's other fix failures
//!   do — the loop never gets a chance to retry a timed-out fix, because
//!   `route_stage_failure` already stopped the walk before the loop's own
//!   retry logic would run again.
//! - A timed-out `Stage::Review`/`Stage::VerifyComplete`
//!   (`run_verdict_stage`) fails exactly as a contract-miss-after-retry does.
//! - A timed-out `Stage::Fix` inside the review/fix/re-review loop
//!   (`run_review_fix_converge`) fails exactly as that loop's other fix
//!   failures do.
//!
//! No new "what should a timeout at *this particular* stage mean" judgment
//! call was made anywhere — the whole point of routing through the existing
//! per-stage failure handling is that a timeout is indistinguishable, in its
//! *consequences*, from any other way that same stage could have failed.
//! Only `agent_run.status` (`"timeout"` vs `"error"`) and
//! `task.failure_reason`/`epic.blocked_reason` (`"timeout"` vs
//! `"agent_error"`) tell a human which one actually happened.
//!
//! ### The worker slot
//!
//! Unchanged from every other stop path this module has: `route_stage_failure`
//! (or, on the `Stop`-returning path out of whichever loop observed the
//! timeout) ends in a plain `return`, no further writes, which propagates
//! back to [`try_claim_and_run`] — the lease is released, the workspace is
//! retained (`PushIntent::Attempt` — the same triage-push every task-scoped
//! failure gets), and the worker's own loop tries its next claim immediately,
//! exactly as it does after any other epic-scoped failure (D10, §7).
//!
//! ## T-550: `WorkItem` unification (§2.4, §8, D17)
//!
//! Every section above this one talks about "the claimed epic" as if that
//! were the only kind of claim there could ever be — because through T-543 it
//! was. D17 always meant otherwise: a standalone task (`task.epic_id IS
//! NULL`) is its own leasable work item, with its own branch and PR, "via a
//! unified `WorkItem` enum, *not* a parallel code path." [`WorkItem::Epic`] /
//! [`WorkItem::Standalone`] is that enum — what a successful claim now
//! returns — and this section is the seam that makes room for it without
//! touching a single line of what Phases 1–4 already proved out.
//!
//! ### What actually had to change, and what didn't
//!
//! [`claim_epic`] itself is untouched in signature and behavior — every
//! Phase 1–4 test that calls it directly still compiles and still passes.
//! What moved is the SQL *inside* it: the `UPDATE ... WHERE id = (SELECT ...)
//! ... RETURNING` race is now [`claim_row`], parameterized on
//! [`LeaseTable`] (`epic` or `task`) and an extra `WHERE` fragment (the
//! standalone claim's `AND epic_id IS NULL`, §2.4's own words for "same
//! shape with…"). [`claim_task`] is the second, real caller of that shared
//! core — not a hand-copied second race to keep in sync by hand. The same
//! split happened to the heartbeat/lease trio: [`renew_lease_once`],
//! [`spawn_heartbeat`], and [`release_lease`] all kept their exact
//! pre-T-550 signatures (so, again, no existing call site — production or
//! test — had to change), but each is now a one-line wrapper over a
//! `LeaseTable`-parameterized core ([`renew_lease_generic`],
//! [`spawn_heartbeat_generic`], [`release_lease_generic`]) that
//! [`renew_task_lease_once`], [`spawn_task_heartbeat`], and
//! [`release_task_lease`] call into for the `task` table. This is the literal
//! meaning of the AC's "no duplicated lease/heartbeat/... code": there is
//! exactly one fencing UPDATE, one heartbeat loop, one release UPDATE, each
//! written once and reused for both tables — not two copies that happen to
//! agree today and drift tomorrow.
//!
//! [`claim_next`] is the new thing with no pre-T-550 analogue: try
//! [`claim_epic`], and only on `None` fall back to [`claim_task`]. This one
//! function is where "epic claims are tried first so standalone work never
//! starves an epic" is actually decided — every other function in the
//! cluster above just does whatever table it's told to, with no opinion
//! about order. [`try_claim_and_run`] calls `claim_next` and dispatches the
//! `WorkItem` it gets back to [`run_claimed_epic`] (T-513/T-514's existing
//! reset-orphans → heartbeat → [`run_epic_pipeline_inner`] → release
//! sequence, moved out of `try_claim_and_run` verbatim, not rewritten) or
//! [`run_claimed_standalone`] (the new, much smaller mirror: no orphans to
//! reset, a task-table heartbeat, and [`run_standalone_pipeline_inner`]).
//! `worker_loop` itself — the actual "one loop" — never changed at all: it
//! still just calls `try_claim_and_run` in an inner drain loop, exactly as
//! before.
//!
//! ### Why the pipeline body stops where it does
//!
//! [`run_standalone_pipeline_inner`] is the real, load-bearing hand-off point
//! for T-551 — the same role [`pr::SUMMARY_MARKER`] plays for T-560, a named
//! seam instead of a restructure deferred to later. It deliberately does not
//! provision a workspace, run an implement stage, or open a PR: none of
//! that's possible yet ([`workspace::provision_epic_workspace`] only knows
//! how to provision an *epic's* workspace; there is no standalone-task
//! finalize; [`fail_item`]'s `FailureContext` has no way to fail a task with
//! no epic to `Block`), and building any of it now — on spec, before T-551's
//! own endpoint exists to drive it — would mean guessing at shapes that
//! task is better positioned to settle for real. MILESTONE_2 §8 says as much
//! directly: T-550 builds the seam and proves it with tests; T-551 thickens
//! it. See that function's own doc for exactly what it does today (log and
//! return) and the one constraint that doc leaves for T-551: nothing in
//! production ever sets a task `InProgress` with `epic_id IS NULL` yet (no
//! endpoint does it — that's T-551's own addition), so a live pool never
//! actually exercises this function's do-nothing body in practice. Every
//! T-550 test that does reach it calls [`claim_task`] or
//! [`try_claim_and_run`] directly — a single, bounded claim-run-release —
//! never [`worker_loop`]'s continuously-draining inner loop, which would spin
//! on a row this function leaves immediately re-claimable the moment its
//! lease is released.
//!
//! ### Proven by test, not by inspection
//!
//! Every clause of this task's AC has a direct test: [`claim_task`] racing
//! (many claimants, exactly one winner, mirroring [`claim_epic`]'s own
//! concurrency test), an expired standalone lease being re-claimable and a
//! live one not, [`renew_task_lease_once`]/[`spawn_task_heartbeat`] fencing
//! out a stolen lease the same way the epic versions already did, and
//! [`claim_next`]/[`try_claim_and_run`] preferring a queued epic over a
//! queued standalone task. The boot-time lease clear covering `task` was
//! already true and already tested before this task started — [`clear_all_leases`]
//! has cleared both tables since T-510, once T-500's migration put the lease
//! columns on `task` in the first place — so nothing there needed to change.
//!
//! ## T-551: run a standalone task end-to-end (§8, D17)
//!
//! T-550 built the claim/lease/heartbeat seam and left [`run_standalone_pipeline_inner`]
//! as a documented stub, naming exactly three things as this task's to build:
//! a standalone workspace, a "run just this one task" selector in place of
//! the DAG walk, and a `FailureContext`/`fail_item` that can express "no epic
//! to Block." This section is that design, plus the two judgment calls
//! MILESTONE_2 §8 left open (the retry contract, cancellation's scope).
//!
//! ### Generalizing, not forking, the pipeline
//!
//! [`process_one_task`] — the T-513/T-522/T-530/T-531/T-532 sequence a single
//! task walks (implement → test gate → commit → review-or-verify-complete →
//! `Done`) — took an epic-shaped trio (`epic_id: &str`, `epic: &Epic`,
//! `dag`/`ready: &DagNode`) through T-550. All three existed only to derive
//! things a standalone task either has directly (its own spec fields, now
//! read off a plain `&Task` — an epic's `DagNode::task` and a standalone
//! claim's fetched row are the identical type) or doesn't have at all (an
//! epic background, a sibling manifest — both already `Option`/slice-shaped
//! in `spec::TaskContext`, D17, since T-502). Swapping those three params for
//! `task: &Task`, `epic_ctx: Option<EpicContext>`, and `siblings:
//! &[SiblingTask]` let one function serve both claims: `epic_id` itself is no
//! longer even a separate argument — it's `task.epic_id.as_deref()`, since
//! that column **is** the epic/standalone distinction. Every helper
//! `process_one_task` calls ([`commit_if_dirty`], [`run_test_gate_loop`],
//! [`run_verdict_stage`], [`run_review_fix_converge`], [`run_verify_complete`],
//! [`route_stage_failure`], [`run_preflight`]) had its own `epic_id: &str`
//! widened to `Option<&str>` the same way — a mechanical, call-site-preserving
//! change for every existing (epic) caller, which is why none of Phase 1–4's
//! tests needed their assertions touched, only their call sites recompiled.
//! [`container_still_in_progress`] (renamed from `epic_still_in_progress`) is
//! the one function whose *logic* actually branches: `Some(epic_id)` asks the
//! epic row exactly as before; `None` asks the standalone task's own row,
//! since a standalone task has no separate container to ask — it **is** its
//! own claimable container (the same duality [`FailureContext`] documents).
//!
//! [`run_standalone_pipeline_inner`] is the new, flatter orchestration shell
//! this generalization makes possible: provision ([`workspace::provision_task_workspace`])
//! → preflight ([`run_preflight`]) → [`process_one_task`] → finalize
//! ([`finalize_task`]) — [`run_epic_pipeline_inner`]'s exact shape minus the
//! DAG walk (one task, not several to pick a "ready" one from) and minus
//! `reset_orphaned_tasks` (no sub-tasks to orphan).
//!
//! ### Workspace provisioning: one body, two containers
//!
//! `workspace.rs`'s [`WorkspaceContainer`] enum (`Epic` | `Task`) plus a
//! shared `provision_workspace` core is the identical move T-550 made for
//! claim/lease/heartbeat (`LeaseTable`): [`workspace::provision_epic_workspace`]
//! keeps its exact pre-T-551 signature and behavior (every T-511+ test still
//! passes unchanged), and [`workspace::provision_task_workspace`] is the
//! second real caller of the shared core, not a hand-copied second
//! clone/reattach/setup sequence. `task_workspace_path`/`task_branch_name`
//! supply §2.8's `<clone_root>/tasks/<task id>` and
//! `dearborn/task-<slug>-<id>` shapes; everything else — the per-project
//! refresh lock, the reattach-vs-reclone decision, `setup_cmd`, persisting a
//! branch name once — is one function, run for both.
//!
//! ### One PR-opening core, two finalizers
//!
//! [`push_and_ensure_pr`] is [`finalize_epic`]'s pre-T-551 push/open-PR
//! sequence, factored out so [`finalize_task`] calls the identical code
//! rather than a copy. What's left in each caller is only what genuinely
//! differs: which row's checklist builds the PR body
//! ([`build_task_checklist`]'s DAG walk vs. [`build_standalone_checklist`]'s
//! one-item list), and which row's terminal write persists the opened PR.
//! That terminal write is the one place a standalone task's story actually
//! diverges from an epic's: an epic has `Completed`, a status genuinely
//! distinct from any task's own `Done` (the epic and its tasks are separate
//! rows tracking separate things). A task's status enum has no `Completed`
//! value — [`process_one_task`]'s own step 6 already left it `Done` before
//! `finalize_task` ever runs, and opening the PR doesn't change what "done"
//! means, only where to find the PR. `finalize_task`'s persisting `UPDATE` is
//! fenced on `WHERE status = 'Done'`, mirroring `finalize_epic`'s own `WHERE
//! status = 'InProgress'` fence.
//!
//! ### `FailureContext.epic_id: Option<&str>` — no epic to Block
//!
//! Every §2.3 reason a standalone task can fail with — including
//! `preflight_red`/`setup_failed`/`workspace_error`, which for an epic have
//! no task at fault yet (the DAG walk hasn't started) — names the standalone
//! task itself (`task_id: Some(_)`), because there is only one row to be
//! "the item that fails." [`fail_item`]'s `epic_id: None` branch skips
//! everything epic-shaped (the fenced `Blocked` write, `dag_updated`,
//! `epic_updated`) and does the two things that still apply: the task's own
//! `Failed` write (unconditional, unchanged), and — since there's no epic to
//! fetch a `project_id` from — a direct task fetch so `board_updated` still
//! publishes and [`push_on_failure`] still runs. There is no "did this call
//! win a race" gate on the standalone side (`took_epic`'s job on the epic
//! side): a standalone task has no sibling ever concurrently touching the
//! same row, so every standalone failure call pushes unconditionally.
//!
//! ### Retry: `Failed → InProgress`, not `Failed → Todo` (a T-541 contract
//! ### revision)
//!
//! T-541 shipped `POST /tasks/{id}/retry` returning a standalone task to
//! `Todo`, its own doc explicit that resuming it was "T-551's job." Taken
//! literally that leaves a dead end: [`claim_task`]'s predicate only ever
//! selects `status = 'InProgress'` (§2.4), so a task sitting in `Todo` is
//! never picked up by any worker — retry would need a *second*, human-driven
//! `POST /tasks/{id}/run` to actually resume anything, silently, with no
//! error telling anyone that "retried" didn't mean "resumed." T-551 corrects
//! this: for a standalone task specifically, `retry_task`'s single fenced
//! `UPDATE` now writes `status = 'InProgress'` (via a `CASE WHEN epic_id IS
//! NULL` in the same statement that writes `Todo` for an epic-scoped task),
//! clears `failure_reason`, and clears the lease columns defensively — the
//! literal mirror of what the epic branch already does to *its* container
//! (`Blocked → InProgress`, `blocked_reason` cleared, lease cleared). The
//! reasoning: a standalone task is simultaneously the claimable item and the
//! unit of work, so "restore the claimable state" and "reset the unit of
//! work" are the same write, not two — unlike an epic, where those are two
//! different rows and thus two different statuses (task `Todo`, epic
//! `InProgress`). `tasks::tests::retry_task_endpoint_standalone_task_returns_directly_to_in_progress`
//! (renamed from `..._has_no_epic_to_unblock`, its assertion updated from
//! `Todo` to `InProgress`) is that HTTP-level proof; `state.notify.notify_waiters()`
//! (already called unconditionally) is what actually wakes a worker to
//! reclaim it — this module's own test
//! `retried_standalone_task_is_reclaimed_and_rerun` proves the whole loop
//! actually resumes, not just that the HTTP response looks right.
//!
//! ### Cancellation: explicitly out of scope
//!
//! T-542's cancel path is `lanes::set_epic_lane`'s `InProgress → Cancelled`
//! transition — an epic-only endpoint. There is no `POST /tasks/{id}/lane` or
//! any other surface that could move a standalone task to `Cancelled`, so
//! nothing today ever looks a standalone task's id up in
//! `state.cancel_registry` expecting to find a live handle worth killing.
//! T-561's own AC (client control surface) names "Cancel on in-flight
//! **epics**" only — this milestone never asked for a standalone-task cancel
//! surface at all, in Phase 4 or here. [`route_stage_failure`]/
//! [`handle_cancelled_task`] are still widened to accept `epic_id:
//! Option<&str>` for consistency with every other failure-adjacent function
//! in this module, and degrade correctly if a future task ever does wire up
//! a standalone cancel (task → `Todo`, `board_updated` in place of
//! `dag_updated`) — but building the HTTP surface itself is left alone
//! deliberately, not half-built.
//!
//! ### `board_updated` on every transition
//!
//! `Task` (`tasks.rs`) already serializes `pr_url`/`failure_reason` (T-500),
//! and `board.rs`'s `Board.tasks` is exactly that struct — so a standalone
//! task's PR link and failure reason reach the board for free once the
//! underlying row has them; no board-side change was needed. What *did* need
//! adding was making sure every standalone status transition actually
//! publishes: `POST /tasks/{id}/run` (below) publishes on `Todo → InProgress`;
//! [`process_one_task`]'s own `Done` step publishes when `epic_id` is `None`
//! (an epic-owned task's `Done` doesn't — it's not board-visible on its own,
//! the epic's lane is); [`fail_item`]'s standalone branch publishes on
//! `Failed`; `retry_task` already published on every transition it makes
//! (unchanged); [`finalize_task`] publishes once the PR persists.
//!
//! ## T-560: PR body — template + agent summary (§9, D16)
//!
//! `pr.rs` shipped with T-514 deliberately half-built: a pure template with a
//! fixed [`pr::SUMMARY_MARKER`] line where the D16 agent summary belongs, its
//! own module doc naming this task as the one that fills it in. This section
//! is that other half, plus the two §9 scaffold elements `pr::build_pr_body`
//! never rendered at all (review-round counts, verified-already-complete
//! slices) — four small additions layered onto the T-551 finalize shape
//! rather than a rewrite of it.
//!
//! ### The summarize run is epic-scoped, not task-scoped — widening
//! ### `AgentStageParams`
//!
//! Every agent stage before this one belongs to exactly one task — even
//! `Stage::Review`'s cumulative diff is still *that task's* diff. An epic's
//! summary is different: it describes the epic as a whole, over every task's
//! combined diff, so there is no single task to attach its `agent_run` row
//! to. [`task_agent::AgentStageParams::task_id`] widens from a bare `&str` to
//! `Option<&str>` to say so directly — `None` for [`run_epic_summary`]'s call
//! (`epic_id: Some(_)`), `Some(_)` for every other stage exactly as before,
//! including [`run_task_summary`]'s own call (a *standalone* task's summary
//! **is** task-scoped — see "standalone tasks get one too" below). This
//! ripples in exactly the two places that read `task_id` off the struct:
//! [`task_agent::cancel_registry_key`] (unaffected in practice — it already
//! preferred `epic_id` over `task_id`, so an epic-scoped summarize keys under
//! the epic id like every other stage in that epic's walk) and
//! [`task_agent::run_agent_stage`]'s own WS topic selection, which now falls
//! back to `epic:<epic_id>` when there is no task — the same topic
//! [`crate::planning::ClaudePlanningAgent`]'s epic-chat stream already uses
//! (T-202) for the identical "this belongs to the epic, not a task" reason.
//! `CONVENTIONS.md`'s T-512 section previously stated flatly that every
//! agent stage streams on `task:<id>`; it's amended alongside this task to
//! note the one exception.
//!
//! One real, flagged gap this creates: `GET /tasks/{id}/runs` lists rows by
//! `task_id`, so an epic-scoped summarize run's `agent_run` row (`task_id:
//! NULL`) is unreachable through that endpoint — it satisfies the AC ("the
//! summary is stored as an `agent_run` row") but is only ever *visible*
//! live, via the `epic:<id>` WS stream while it runs, or by `GET /runs/{id}`
//! to someone who already has its id from a log line. There is no
//! `GET /epics/{id}/runs` today, and adding one is client-surface work
//! (T-562's own territory, not this task's — its deps are T-512/T-514 only).
//! Noted here rather than quietly worked around.
//!
//! ### Ordering: the summary runs *before* `push_and_ensure_pr`, on purpose
//!
//! [`crate::git_host::GitHost`] (T-514) has `push`/`open_pr`/`check_auth` —
//! no "edit an already-opened PR's body." That means the summary text has to
//! exist **before** `open_pr` is called; there is no later point to patch it
//! in. [`finalize_epic`]/[`finalize_task`] both call [`run_epic_summary`]/
//! [`run_task_summary`] first, build the full body via `pr::build_pr_body`,
//! *then* call [`push_and_ensure_pr`] — never the other way around.
//!
//! The consequence flagged in this task's own brief: a summarize run that
//! hangs burns the full `DEARBORN_AGENT_STAGE_TIMEOUT_SECS` **before** the PR
//! opens, where a fast/failed summary would have cost nothing. This is
//! accepted, not engineered around with a second, tighter timeout knob, for
//! three reasons. First, it isn't a *new* class of latency — every other
//! agent stage in the same pipeline (`implement`, `review`, a fix round) can
//! already take up to the identical deadline, and finalize already runs
//! after all of them; one more stage under the same budget doesn't change
//! the shape of "how long can this epic take," which D18 already leaves
//! unbounded by task count (per-task/per-stage caps only, no epic-level
//! budget). Second, D20 says new tunables are global env vars — a second
//! timeout for one stage that's otherwise identical to five others would be
//! a special case bought for a marginal worst-case improvement, and it would
//! be surprising for `summarize` alone to answer to a different clock than
//! `review`/`verify_complete`, its two `RunMode::Ask` siblings. Third, and
//! most directly: this task's AC is "never **blocked**," not "never slow" —
//! [`run_summarize_stage`] reusing [`task_agent::run_agent_stage`] unchanged
//! means a hung summarize run *does* eventually time out (D18, exactly like
//! every other stage), closes its row `status = 'timeout'`, and
//! [`run_summarize_stage`] turns that into `None` — the PR still opens, just
//! later than it would have with a fast summary. Structural non-blocking,
//! not fast-by-construction, is what was actually promised.
//!
//! ### Never fails upward: every summary failure mode, proven
//!
//! [`run_summarize_stage`] returns `Option<String>`, never a `Result` — there
//! is no error variant for a caller to mishandle. Every way the stage can go
//! wrong collapses to the same `None`: [`task_agent::run_agent_stage`]
//! returning `Err` (harness failed to spawn, or its drain thread panicked —
//! the `_ => None` arm), a non-`ok` [`task_agent::AgentStageOutcome`]
//! (`status() != "ok"` covers `error`, `timeout`, and `cancelled` alike — the
//! same match arm), and an `ok` outcome whose `text` is empty or
//! whitespace-only after trimming (handled explicitly, since "ran cleanly,
//! said nothing" is not a harness-level failure at all). `finalize_epic`/
//! `finalize_task` never branch on *which* of these happened — they always
//! call `pr::build_pr_body(.., summary.as_deref())`, and `build_pr_body`'s own
//! blank-is-absent filtering means even a stray whitespace-only `Some` would
//! render identically to `None`. Each path has its own worker test using
//! `ScriptedTaskAgent` to force it: a non-ok exit
//! (`.script(Stage::Summarize, ScriptedRun { exit_code: Some(1), .. })`), an
//! empty/whitespace reply (`text: vec![""]` / `vec!["   "]`), a harness spawn
//! failure (a `TaskAgent` fake whose `run()` itself returns `Err`), and a
//! timeout (`with_gate_on(Stage::Summarize, ..)` left ungated, driven under a
//! test's short `agent_stage_timeout_secs` the same way T-543's own timeout
//! tests are) — all asserting the PR still opens with the template's own
//! content intact and no "Summary of changes" heading in the body.
//!
//! ### Standalone tasks get one too
//!
//! D16 draws no epic/standalone distinction, and `finalize_task` already
//! mirrors `finalize_epic` in every other respect (T-551's "one core, two
//! thin callers" shape, `push_and_ensure_pr` chief among them) — treating the
//! summary as epic-only would have been the one place that symmetry broke
//! for no stated reason. [`run_task_summary`] is the standalone mirror of
//! [`run_epic_summary`]: the task's own [`spec::SpecFields`] stand in for the
//! epic's title/description (exactly the ordinary spec an implement/review
//! stage already sees for this task), no siblings (D17), `base_sha` read
//! straight off the task row via [`task_summary_base_sha`] rather than
//! derived from the earliest `Stage::Implement` row the way
//! [`epic_summary_base_sha`] has to (a standalone task has only the one row —
//! no "which task ran first" question to answer).
//!
//! ### Sourcing the two new §9 scaffold elements
//!
//! [`pr::build_pr_body`] rendered three of §9's five scaffold elements from
//! day one (description, task checklist with commit SHAs, footer); the other
//! two — review-round counts, verified-already-complete slices — sat unbuilt
//! because both need data this module's DB layer, not `pr.rs`, has to
//! gather. Both ride on [`pr::TaskChecklistItem`] as two new fields
//! (`review_rounds: u32`, `verified_complete_reasoning: Option<String>`)
//! rather than arriving as separate parallel collections, because both are
//! facts about a specific task, exactly like `commit_sha` already is.
//!
//! [`fetch_review_round_counts`] counts completed `Stage::Review`
//! `agent_run` rows (`status = 'ok'`) per task — a plain `GROUP BY task_id`,
//! one row per round, not the stage's own 0-based `attempt` value (T-531's
//! own numbering starts a task's first review at `attempt = 0`; a human
//! reading "0 review rounds" on a task that was in fact reviewed once would
//! misread it as "never reviewed"). [`fetch_verified_complete_reasoning`]
//! reads the `log` of whichever `Stage::VerifyComplete` row closed `status =
//! 'ok', verdict = 'PASS'` — the exact T-532 evidence that already exists for
//! "why this task closed with zero commits," now surfaced in the PR body
//! itself rather than only in the task's own run history (T-532's AC talked
//! about the latter; this task's own AC — "verified-already-complete
//! slices" — is that same information one hop closer to a reviewer who never
//! opens the task detail view). Both functions take a `scope_column`/
//! `scope_value` pair (`"epic_id"`/epic id for [`build_task_checklist`],
//! `"task_id"`/task id for [`build_standalone_checklist`]) rather than being
//! written twice — the query shape is identical, only which column scopes it
//! differs, and `scope_column` is always one of exactly two hardcoded
//! literals, never caller-supplied text, so interpolating it into the SQL
//! string carries no injection risk (SQLite has no bind-parameter syntax for
//! a column/table name in the first place).
//!
//! `pr::build_pr_body` renders each new section only when at least one task
//! qualifies (omitted entirely otherwise) — see that function's own doc for
//! why that differs from `## Tasks`' "always render, with a placeholder"
//! rule.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use libsql::{params, Connection};
use tokio::task::JoinHandle;

use crate::board;
use crate::capability;
use crate::cmd::{self, StageCommand, StageOutcome};
use crate::epics::{fetch_epic, get_epic_project_id};
use crate::evidence::{self, CloseStage, OpenStage, StageHandle};
use crate::git;
use crate::git_host::{OpenPrRequest, PushRequest};
use crate::pr;
use crate::spec::{self, EpicContext, SiblingTask, SpecFields, TaskContext};
use crate::task_agent::{self, AgentStageParams, Stage, TaskRunRequest};
use crate::tasks::compute_dag;
use crate::workspace::{self, ProvisionFailure, ProvisionedWorkspace};
use crate::AppState;

/// The deterministic git identity every T-513 commit is attributed to (§2.8's
/// "Commits" naming section fixes the *subject* format but not an identity —
/// this fills that gap). Passed as `-c user.name=`/`-c user.email=` on the
/// commit invocation itself ([`git::commit_all`]), never written to the
/// workspace's `.git/config`, so a commit succeeds even on a host with no
/// configured global git identity, and every Dearborn-authored commit is
/// attributable to the tool rather than to whatever OS user happens to run
/// the server process.
const COMMITTER_NAME: &str = "Dearborn";
const COMMITTER_EMAIL: &str = "dearborn@noreply.localhost";

/// Test-only pipeline hook (T-510): an async closure the claimed-epic body
/// awaits once, immediately after a claim, before doing any DB work. See
/// [`crate::AppState::test_pipeline_hook`].
#[cfg(test)]
pub type PipelineHook =
    Arc<dyn Fn() -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>> + Send + Sync>;

/// The row identity returned by a successful [`claim_epic`] — just enough to
/// drive the pipeline body and resolve the project for board publishes.
#[derive(Debug, Clone)]
pub struct ClaimedEpic {
    pub id: String,
    #[allow(dead_code)] // not yet read; T-511+ uses it for workspace paths
    pub project_id: String,
}

/// The row identity returned by a successful [`claim_task`] (T-550) — the
/// `task` table's mirror of [`ClaimedEpic`]. See [`claim_task`]'s own doc for
/// the query this backs.
#[derive(Debug, Clone)]
pub struct ClaimedTask {
    pub id: String,
    #[allow(dead_code)] // not yet read; T-551 uses it for workspace paths
    pub project_id: String,
}

/// What a successful claim returns (T-550, D17): the epic id or the
/// standalone task id, tagged so everything downstream of the claim —
/// which lease table to heartbeat/release against, which pipeline body to
/// run, eventually (T-551) which workspace path and branch/PR naming apply —
/// can dispatch on shape instead of re-deriving "is this epic-scoped" from a
/// bare `Option`. This mirrors the convention [`task_agent::AgentStageParams`]
/// (`epic_id: Option<&str>`) and [`FailureContext`] (`epic_id: &str`,
/// `task_id: Option<&str>`) already use at the epic/standalone boundary —
/// see [`try_claim_and_run`] for the one place a claim turns into a
/// `WorkItem`, and the module doc's "T-550: `WorkItem` unification" section
/// for why the claim order below (epic, then standalone) is load-bearing.
#[derive(Debug, Clone)]
pub enum WorkItem {
    Epic(ClaimedEpic),
    Standalone(ClaimedTask),
}

impl WorkItem {
    /// The epic id or standalone task id this claim carries — whichever the
    /// variant holds. Named `.id()` rather than exposing the variants'
    /// fields directly at every call site, mirroring
    /// [`task_agent::cancel_registry_key`]'s "whichever id the claimed item
    /// has" framing.
    #[allow(dead_code)] // read once a caller needs "just the id, don't care which kind" (T-551)
    pub fn id(&self) -> &str {
        match self {
            WorkItem::Epic(c) => &c.id,
            WorkItem::Standalone(c) => &c.id,
        }
    }
}

/// Shared flag signalling whether a claimed lease is still held (D4).
///
/// Cloned into the heartbeat task and the claimed-epic body. The heartbeat is
/// the only writer: it flips this to "lost" the instant its fencing UPDATE
/// affects zero rows. The body only reads it, once per loop iteration, to
/// decide whether to keep writing or abandon the item. A plain
/// `Arc<AtomicBool>` is enough — there is nothing to wake (the body is
/// already polling its own DB reads every iteration; it just needs to check
/// one more flag on each pass), so a `Notify`/watch-channel would add
/// complexity with no benefit here.
#[derive(Clone)]
pub struct LeaseHandle(Arc<AtomicBool>);

impl LeaseHandle {
    /// A fresh handle, valid until [`mark_lost`](Self::mark_lost) is called.
    fn new() -> LeaseHandle {
        LeaseHandle(Arc::new(AtomicBool::new(true)))
    }

    /// Whether the lease has been fenced out (a heartbeat renewal affected
    /// zero rows). Checked by the pipeline body at the top of every iteration.
    pub fn is_lost(&self) -> bool {
        !self.0.load(Ordering::SeqCst)
    }

    /// Record that the lease was lost. Idempotent; called by the heartbeat
    /// task only.
    fn mark_lost(&self) {
        self.0.store(false, Ordering::SeqCst);
    }
}

/// Which table a lease operation targets (T-550) — the *only* axis
/// [`claim_epic`]/[`claim_task`], [`renew_lease_once`]/[`renew_task_lease_once`],
/// and [`release_lease`]/[`release_task_lease`] differ on. Kept private:
/// nothing outside this claim/lease/heartbeat cluster needs to name a table —
/// every caller already knows which kind of item it holds via [`WorkItem`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum LeaseTable {
    Epic,
    Task,
}

impl LeaseTable {
    fn as_str(self) -> &'static str {
        match self {
            LeaseTable::Epic => "epic",
            LeaseTable::Task => "task",
        }
    }
}

/// The shared guts of [`claim_epic`]/[`claim_task`]: the §2.4
/// `UPDATE ... WHERE id = (SELECT ...) RETURNING id, project_id` race,
/// against whichever `table` the caller names plus whatever extra predicate
/// `extra_where` supplies (the standalone claim's `AND epic_id IS NULL`;
/// the epic claim has none). `table`/`extra_where` are both compile-time
/// string literals chosen by [`LeaseTable`] at the two call sites, never
/// caller-supplied data, so building the query with [`format!`] carries none
/// of the injection risk that would come with interpolating anything a
/// request handler passed through — the same pattern `epics.rs`/`tasks.rs`
/// already use for their column-list `SELECT`/dynamic-`SET` queries.
async fn claim_row(
    conn: &Connection,
    table: LeaseTable,
    extra_where: &str,
    worker_id: &str,
    lease_ttl_secs: u64,
) -> Result<Option<(String, String)>, libsql::Error> {
    let now = now_ms();
    let expires_at = now + (lease_ttl_secs as i64) * 1000;
    let table = table.as_str();
    let sql = format!(
        "UPDATE {table} SET lease_owner = ?1, lease_expires_at = ?2, updated_at = ?3 \
         WHERE id = (SELECT id FROM {table} \
                     WHERE status = 'InProgress' {extra_where} \
                       AND (lease_owner IS NULL OR lease_expires_at < ?3) \
                     ORDER BY updated_at ASC LIMIT 1) \
         RETURNING id, project_id"
    );
    let mut rows = conn
        .query(&sql, params![worker_id, expires_at, now])
        .await?;
    match rows.next().await? {
        Some(row) => Ok(Some((row.get::<String>(0)?, row.get::<String>(1)?))),
        None => Ok(None),
    }
}

/// §2.4 epic claim (tried first — standalone-task claim is [`claim_task`],
/// T-550). See the module docs for the full race/RETURNING rationale.
/// `lease_ttl_secs` sets how far in the future `lease_expires_at` is
/// written; `worker_id` becomes `lease_owner`.
pub async fn claim_epic(
    conn: &Connection,
    worker_id: &str,
    lease_ttl_secs: u64,
) -> Result<Option<ClaimedEpic>, libsql::Error> {
    let claimed = claim_row(conn, LeaseTable::Epic, "", worker_id, lease_ttl_secs).await?;
    Ok(claimed.map(|(id, project_id)| ClaimedEpic { id, project_id }))
}

/// §2.4 standalone-task claim (T-550, D17) — tried only after [`claim_epic`]
/// finds nothing (see [`claim_next`], the one call site that orders the two
/// this way, and the module doc's "T-550" section for why that order is
/// load-bearing). Identical shape to [`claim_epic`] against the `task` table,
/// restricted to `epic_id IS NULL` so a task that belongs to an epic (only
/// ever claimed as part of that epic's own DAG walk, never directly) can
/// never be picked up here.
pub async fn claim_task(
    conn: &Connection,
    worker_id: &str,
    lease_ttl_secs: u64,
) -> Result<Option<ClaimedTask>, libsql::Error> {
    let claimed = claim_row(
        conn,
        LeaseTable::Task,
        "AND epic_id IS NULL",
        worker_id,
        lease_ttl_secs,
    )
    .await?;
    Ok(claimed.map(|(id, project_id)| ClaimedTask { id, project_id }))
}

/// The full §2.4 claim (T-550, D17): try [`claim_epic`] first, falling back
/// to [`claim_task`] only when no epic is claimable. This is the one place
/// the "epic claims are tried first so standalone work never starves an
/// epic" AC is actually decided — every other claim-adjacent function just
/// takes whichever table it's told to. See [`try_claim_and_run`], the sole
/// production caller, and the module doc's "T-550" section.
async fn claim_next(
    conn: &Connection,
    worker_id: &str,
    lease_ttl_secs: u64,
) -> Result<Option<WorkItem>, libsql::Error> {
    if let Some(claimed) = claim_epic(conn, worker_id, lease_ttl_secs).await? {
        return Ok(Some(WorkItem::Epic(claimed)));
    }
    Ok(claim_task(conn, worker_id, lease_ttl_secs)
        .await?
        .map(WorkItem::Standalone))
}

/// Part of the claim path (see module docs): reset any task of `epic_id` left
/// `InProgress` by a previous (now-dead or fenced-out) owner back to `Todo`,
/// so the new owner's DAG walk treats that abandoned work as pending again.
/// Returns the number of tasks reset (`0` is the common case — a fresh claim
/// with no orphans). Epic-only: a standalone claim has no sub-tasks of its
/// own to orphan, so [`claim_task`] has no counterpart to call here.
async fn reset_orphaned_tasks(conn: &Connection, epic_id: &str) -> Result<u64, libsql::Error> {
    let now = now_ms();
    conn.execute(
        "UPDATE task SET status = 'Todo', updated_at = ?1 WHERE epic_id = ?2 AND status = 'InProgress'",
        params![now, epic_id],
    )
    .await
}

/// The shared guts of [`renew_lease_once`]/[`renew_task_lease_once`] (T-550's
/// fencing update, D4): `Ok(true)` if the lease is still ours (the UPDATE
/// affected a row), `Ok(false)` if it was fenced out (zero rows — someone
/// else's claim now owns this id). See [`claim_row`]'s doc for why
/// interpolating `table` into the query text is safe here.
async fn renew_lease_generic(
    conn: &Connection,
    table: LeaseTable,
    id: &str,
    worker_id: &str,
    lease_ttl_secs: u64,
) -> Result<bool, libsql::Error> {
    let now = now_ms();
    let expires_at = now + (lease_ttl_secs as i64) * 1000;
    let table = table.as_str();
    let sql =
        format!("UPDATE {table} SET lease_expires_at = ?1 WHERE id = ?2 AND lease_owner = ?3");
    let affected = conn
        .execute(&sql, params![expires_at, id, worker_id])
        .await?;
    Ok(affected > 0)
}

/// A single heartbeat renewal attempt against the `epic` table (D4's fencing
/// update) — the pre-T-550 direct-unit-test seam, kept at its original
/// signature so every Phase 1–4 test exercising the fencing check in
/// isolation (no timer, no `spawn_heartbeat`) still compiles unchanged.
/// [`spawn_heartbeat_generic`] itself calls [`renew_lease_generic`] straight
/// through (it already carries a `LeaseTable`, so routing back through this
/// table-fixed wrapper would add a layer for nothing) — this function's only
/// caller since T-550 is this module's own tests. See
/// [`renew_task_lease_once`] for the `task`-table mirror.
#[allow(dead_code)] // test-only since T-550 — see doc above
async fn renew_lease_once(
    conn: &Connection,
    epic_id: &str,
    worker_id: &str,
    lease_ttl_secs: u64,
) -> Result<bool, libsql::Error> {
    renew_lease_generic(conn, LeaseTable::Epic, epic_id, worker_id, lease_ttl_secs).await
}

/// The `task`-table mirror of [`renew_lease_once`] (T-550): a standalone
/// claim's heartbeat fences itself the identical way an epic's does, just
/// against `task.lease_owner`/`task.lease_expires_at`. Test-only for the same
/// reason `renew_lease_once` is — see its doc.
#[allow(dead_code)] // test-only since T-550 — see renew_lease_once's doc
async fn renew_task_lease_once(
    conn: &Connection,
    task_id: &str,
    worker_id: &str,
    lease_ttl_secs: u64,
) -> Result<bool, libsql::Error> {
    renew_lease_generic(conn, LeaseTable::Task, task_id, worker_id, lease_ttl_secs).await
}

/// The shared guts of [`spawn_heartbeat`]/[`spawn_task_heartbeat`] (D4,
/// T-550): renews `id`'s lease in `table` every `period` via
/// [`renew_lease_generic`]; the first renewal it observes fail flips `lease`
/// to lost and the task exits (no further renewals are meaningful once
/// fenced out). The caller (`try_claim_and_run`) aborts the returned handle
/// when the item is released, on every exit path — see the module docs' "no
/// reaper" note for why there is nothing else watching leases.
///
/// ## Fence-out is a kill, not just an abandonment
///
/// A fenced-out heartbeat means another owner now holds this item — and if
/// an agent stage is still in flight, that stage's process must die *now*.
/// Merely marking [`LeaseHandle`] lost left the walk oblivious until its
/// current stage returned, which for a long implement run meant the old
/// agent kept editing the workspace alongside (or after) the new owner's own
/// re-run. So on fence-out we also call `RunControl::cancel()` on whatever
/// handle the cancel registry holds under this item's id
/// (`task_agent::cancel_registry_key`'s contract: epic id for an epic's
/// walk, task id for a standalone claim — exactly what [`LeaseTable`] +
/// `id` name here). Finding nothing registered is the correct silent no-op
/// (no stage in flight — e.g. between tasks, or after the body finished).
/// The cancelled stage's outcome flows back through `run_agent_stage` →
/// `route_stage_failure`, which already knows how to route a cancellation
/// without failing the task.
#[allow(clippy::too_many_arguments)]
fn spawn_heartbeat_generic(
    conn: Connection,
    table: LeaseTable,
    id: String,
    worker_id: String,
    period: Duration,
    lease_ttl_secs: u64,
    lease: LeaseHandle,
    cancel_registry: Arc<task_agent::CancelRegistry>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(period).await;
            match renew_lease_generic(&conn, table, &id, &worker_id, lease_ttl_secs).await {
                Ok(true) => continue,
                Ok(false) => {
                    tracing::warn!(
                        item = %id,
                        table = table.as_str(),
                        worker = %worker_id,
                        "heartbeat: lease fenced out (0 rows affected); abandoning"
                    );
                    // Fence-out is a kill — see this function's doc. Registry
                    // lookup is cheap and unconditional; finding nothing is
                    // the normal no-stage-in-flight case. Fire-and-forget,
                    // mirroring lanes.rs's T-542 cancel: never wait on the
                    // kill itself.
                    let cancelled = {
                        let registry = cancel_registry
                            .lock()
                            .expect("cancel_registry mutex poisoned");
                        match registry.get(&id) {
                            Some(handle) => {
                                if let Err(err) = handle.cancel() {
                                    tracing::warn!(
                                        item = %id,
                                        error = %err,
                                        "heartbeat fence-out: RunControl::cancel() failed \
                                         (best-effort; the walk's lease.is_lost() checks remain)"
                                    );
                                }
                                true
                            }
                            None => false,
                        }
                    };
                    tracing::info!(
                        item = %id,
                        cancelled,
                        "heartbeat: fenced out; in-flight stage killed"
                    );
                    lease.mark_lost();
                    return;
                }
                Err(err) => {
                    tracing::warn!(
                        item = %id,
                        table = table.as_str(),
                        worker = %worker_id,
                        error = %err,
                        "heartbeat: renewal query failed; will retry next tick"
                    );
                }
            }
        }
    })
}

/// Spawn the per-claimed-epic heartbeat task (D4). See
/// [`spawn_heartbeat_generic`] for the shared mechanics and
/// [`spawn_task_heartbeat`] for the `task`-table mirror (T-550).
fn spawn_heartbeat(
    conn: Connection,
    epic_id: String,
    worker_id: String,
    period: Duration,
    lease_ttl_secs: u64,
    lease: LeaseHandle,
    cancel_registry: Arc<task_agent::CancelRegistry>,
) -> JoinHandle<()> {
    spawn_heartbeat_generic(
        conn,
        LeaseTable::Epic,
        epic_id,
        worker_id,
        period,
        lease_ttl_secs,
        lease,
        cancel_registry,
    )
}

/// The `task`-table mirror of [`spawn_heartbeat`] (T-550): a claimed
/// standalone task's lease is fenced and renewed by the identical mechanism,
/// just against `task.lease_owner`/`task.lease_expires_at`.
fn spawn_task_heartbeat(
    conn: Connection,
    task_id: String,
    worker_id: String,
    period: Duration,
    lease_ttl_secs: u64,
    lease: LeaseHandle,
    cancel_registry: Arc<task_agent::CancelRegistry>,
) -> JoinHandle<()> {
    spawn_heartbeat_generic(
        conn,
        LeaseTable::Task,
        task_id,
        worker_id,
        period,
        lease_ttl_secs,
        lease,
        cancel_registry,
    )
}

/// The shared guts of [`release_lease`]/[`release_task_lease`]: clear
/// `lease_owner`/`lease_expires_at` in `table`, fenced by `lease_owner = ?`
/// so a lease already stolen by another worker (this one was fenced out
/// mid-run) is never clobbered — releasing is a no-op in that case, which is
/// correct: the new owner's lease must survive.
async fn release_lease_generic(conn: &Connection, table: LeaseTable, id: &str, worker_id: &str) {
    let table_name = table.as_str();
    let sql = format!(
        "UPDATE {table_name} SET lease_owner = NULL, lease_expires_at = NULL \
         WHERE id = ?1 AND lease_owner = ?2"
    );
    let result = conn.execute(&sql, params![id, worker_id]).await;
    if let Err(err) = result {
        tracing::warn!(item = %id, table = table_name, worker = %worker_id, error = %err, "failed to release lease");
    }
}

/// Release a held epic lease. See [`release_lease_generic`] for the shared
/// mechanics and [`release_task_lease`] for the `task`-table mirror (T-550).
async fn release_lease(conn: &Connection, epic_id: &str, worker_id: &str) {
    release_lease_generic(conn, LeaseTable::Epic, epic_id, worker_id).await
}

/// The `task`-table mirror of [`release_lease`] (T-550): a claimed
/// standalone task's lease releases through the identical fenced clear, just
/// against `task.lease_owner`/`task.lease_expires_at`.
async fn release_task_lease(conn: &Connection, task_id: &str, worker_id: &str) {
    release_lease_generic(conn, LeaseTable::Task, task_id, worker_id).await
}

/// Boot-time lease clear (D4, §13). NULLs every `lease_owner`/
/// `lease_expires_at` on `epic` **and** `task` (task has carried the same
/// columns since T-500, for the standalone-task claim T-550 uses). Clearing
/// both here — rather than adding a second boot-time call once T-550
/// landed — is why this function's signature and query pair never had to
/// change: it was already claim-agnostic. Single-server assumption: nothing
/// else could legitimately hold a lease across a restart, so this makes
/// every previously-claimed row immediately claimable rather than making the
/// pool wait out the TTL. Call once at boot, before [`spawn_pool`].
pub async fn clear_all_leases(db: &crate::Db) -> Result<(), libsql::Error> {
    let conn = db.conn();
    let epics = conn
        .execute(
            "UPDATE epic SET lease_owner = NULL, lease_expires_at = NULL \
             WHERE lease_owner IS NOT NULL OR lease_expires_at IS NOT NULL",
            (),
        )
        .await?;
    let tasks = conn
        .execute(
            "UPDATE task SET lease_owner = NULL, lease_expires_at = NULL \
             WHERE lease_owner IS NOT NULL OR lease_expires_at IS NOT NULL",
            (),
        )
        .await?;
    if epics > 0 || tasks > 0 {
        tracing::info!(epics, tasks, "boot: cleared stale leases");
    }
    Ok(())
}

/// Start the worker pool: `config.executor.worker_concurrency` long-lived
/// loops ([`worker_loop`]), each with a stable `worker_id`. Returns the
/// handles so the caller can hold/await/abort them (production drops them —
/// the pool runs for the life of the process; tests hold them so the runtime
/// keeps polling for the test's duration and they're cleaned up when the
/// test's own runtime shuts down).
pub fn spawn_pool(state: AppState) -> Vec<JoinHandle<()>> {
    let n = state.config.executor.worker_concurrency.max(1);
    (0..n)
        .map(|i| {
            let worker_id = format!("worker-{i}-{}", ulid::Ulid::new());
            let state = state.clone();
            tokio::spawn(worker_loop(state, worker_id))
        })
        .collect()
}

/// One long-lived worker loop (D2). Idles on notify-or-poll, then drains the
/// queue (claim → run → release, repeating immediately on every successful
/// claim) until nothing is left to claim, then idles again. Never returns —
/// the pool's `JoinHandle`s only resolve if the process is torn down.
async fn worker_loop(state: AppState, worker_id: String) {
    let poll_interval = Duration::from_millis(state.config.executor.poll_interval_ms.max(1));
    loop {
        // Idle path: wait for the fast-path wake or the poll fallback,
        // whichever comes first. Never busy-waits — this `.await` parks the
        // task until one of the two futures resolves.
        let _ = tokio::time::timeout(poll_interval, state.notify.notified()).await;

        // Drain: keep claiming (and running) without waiting in between, so a
        // burst of enqueues drains at claim speed, not poll-interval speed.
        loop {
            match try_claim_and_run(&state, &worker_id).await {
                ClaimOutcome::Claimed => continue,
                ClaimOutcome::EmptyOrError => break,
            }
        }
    }
}

enum ClaimOutcome {
    Claimed,
    EmptyOrError,
}

/// One claim attempt and, if it succeeds, the full claimed-item lifecycle:
/// claim (epic first, standalone-task fallback — [`claim_next`], T-550) →
/// run whichever [`WorkItem`] came back → `ClaimOutcome::Claimed`. This is
/// the one loop the T-550 AC asks for: a single dispatch point rather than a
/// second, parallel `try_claim_and_run`-shaped function for standalone work.
/// See [`run_claimed_epic`]/[`run_claimed_standalone`] for the two lifecycles
/// this delegates to and the module doc's "T-550" section for why splitting
/// into two small functions here (rather than one function with an `if`
/// keeping the borrow of `conn` and the lease alive across both arms) reads
/// clearer without actually duplicating anything — every step either arm
/// takes bottoms out in the shared [`claim_row`]/[`renew_lease_generic`]/
/// [`spawn_heartbeat_generic`]/[`release_lease_generic`] core.
async fn try_claim_and_run(state: &AppState, worker_id: &str) -> ClaimOutcome {
    let conn = state.db.conn();

    let claimed = match claim_next(conn, worker_id, state.config.executor.lease_ttl_secs).await {
        Ok(Some(claimed)) => claimed,
        Ok(None) => return ClaimOutcome::EmptyOrError,
        Err(err) => {
            tracing::warn!(worker = %worker_id, error = %err, "claim query failed");
            return ClaimOutcome::EmptyOrError;
        }
    };

    match claimed {
        WorkItem::Epic(claimed) => run_claimed_epic(state, conn, worker_id, claimed).await,
        WorkItem::Standalone(claimed) => {
            run_claimed_standalone(state, conn, worker_id, claimed).await
        }
    }

    ClaimOutcome::Claimed
}

/// The claimed-**epic** half of [`try_claim_and_run`]'s dispatch: reset
/// orphaned tasks → start the epic heartbeat → run [`run_epic_pipeline_inner`]
/// → stop the heartbeat → release the lease. Byte-for-byte the same sequence
/// this module ran inline before T-550 split it out of `try_claim_and_run` so
/// the standalone counterpart ([`run_claimed_standalone`]) could sit next to
/// it instead of behind an `if`/`else` sharing one function's local
/// variables. The release happens on **every** exit path, including a panic
/// in the body, because the body runs in its own `tokio::spawn`'d task — a
/// panic there resolves the `JoinHandle` as `Err` rather than unwinding into
/// the long-lived worker loop, so the release/heartbeat-abort below always
/// runs.
async fn run_claimed_epic(
    state: &AppState,
    conn: &Connection,
    worker_id: &str,
    claimed: ClaimedEpic,
) {
    if let Err(err) = reset_orphaned_tasks(conn, &claimed.id).await {
        tracing::warn!(
            epic = %claimed.id,
            error = %err,
            "failed to reset orphaned InProgress tasks after claim"
        );
    }

    let lease = LeaseHandle::new();
    let heartbeat = spawn_heartbeat(
        conn.clone(),
        claimed.id.clone(),
        worker_id.to_string(),
        Duration::from_secs(state.config.executor.heartbeat_secs.max(1)),
        state.config.executor.lease_ttl_secs,
        lease.clone(),
        state.cancel_registry.clone(),
    );

    // Run the body in its own task: isolates a panic from this long-lived
    // loop (a panicking claimed-epic body must not take the whole worker
    // down — the epic just stays InProgress with a soon-to-expire lease and
    // gets picked up again). Still awaited immediately: this worker handles
    // one item at a time; concurrency comes from having N worker loops, not
    // from overlapping bodies within one.
    let body = tokio::spawn(run_epic_pipeline_inner(
        state.clone(),
        claimed.id.clone(),
        lease,
    ));
    let result = body.await;

    heartbeat.abort();
    release_lease(conn, &claimed.id, worker_id).await;

    if let Err(join_err) = result {
        tracing::error!(
            epic = %claimed.id,
            worker = %worker_id,
            error = %join_err,
            "claimed-epic body panicked; lease released for re-claim"
        );
    }
}

/// The claimed-**standalone-task** half of [`try_claim_and_run`]'s dispatch
/// (T-550): start the task heartbeat → run [`run_standalone_pipeline_inner`]
/// → stop the heartbeat → release the lease. No `reset_orphaned_tasks`
/// equivalent — a standalone task has no sub-tasks of its own to orphan.
///
/// T-551 added the `tokio::spawn` isolation T-550 deliberately left out
/// (that task's own doc: "there is nothing here that could panic in a way
/// worth isolating" — true only while the body was a stub). Now that
/// [`run_standalone_pipeline_inner`] drives a real workspace, real agent
/// stages, and real git operations, a panic inside it must not be allowed to
/// unwind into this long-lived worker loop — the identical concern
/// [`run_claimed_epic`] already isolates its own body against. The task just
/// stays `InProgress` with a soon-to-expire lease and gets picked up again,
/// same as an epic's panicking body would.
async fn run_claimed_standalone(
    state: &AppState,
    conn: &Connection,
    worker_id: &str,
    claimed: ClaimedTask,
) {
    let lease = LeaseHandle::new();
    let heartbeat = spawn_task_heartbeat(
        conn.clone(),
        claimed.id.clone(),
        worker_id.to_string(),
        Duration::from_secs(state.config.executor.heartbeat_secs.max(1)),
        state.config.executor.lease_ttl_secs,
        lease.clone(),
        state.cancel_registry.clone(),
    );

    let body_state = state.clone();
    let task_id = claimed.id.clone();
    let body_lease = lease.clone();
    let body = tokio::spawn(async move {
        run_standalone_pipeline_inner(&body_state, &task_id, &body_lease).await;
    });
    let result = body.await;

    heartbeat.abort();
    release_task_lease(conn, &claimed.id, worker_id).await;

    if let Err(join_err) = result {
        tracing::error!(
            task = %claimed.id,
            worker = %worker_id,
            error = %join_err,
            "claimed-standalone-task body panicked; lease released for re-claim"
        );
    }
}

/// The claimed-standalone-task pipeline body (T-551) — the seam T-550 left
/// (D21: tracer-bullet first, then thicken; see the module doc's "T-550"
/// section, and the "T-551" section below for the full design). Provisions
/// the standalone workspace, runs the preflight gate, runs the task through
/// the shared [`process_one_task`] sequence, and — on success — finalizes
/// (push + open PR). Every step mirrors [`run_epic_pipeline_inner`]'s own
/// shape exactly, one level flatter: no DAG walk (there is exactly one task,
/// not several to pick a "ready" one from) and no `reset_orphaned_tasks`
/// (a standalone task has no sub-tasks of its own to orphan).
async fn run_standalone_pipeline_inner(state: &AppState, task_id: &str, lease: &LeaseHandle) {
    // Mirrors run_epic_pipeline_inner's own opening guard: only act on a
    // task that is actually InProgress — a claim racing a retry/edit or
    // (defensively) any other status must leave the row untouched here
    // exactly as the epic walk's own status guard does.
    let workspace = {
        if lease.is_lost() {
            return;
        }
        let conn = state.db.conn();
        let Ok(Some(task)) = crate::tasks::fetch_task(conn, task_id).await else {
            return;
        };
        if task.status != "InProgress" {
            return;
        }
        match workspace::provision_task_workspace(state, task_id, &task.project_id).await {
            Ok(ws) => ws,
            Err(failure) => {
                // Re-check the lease right before writing — mirrors the
                // epic branch's identical belt-and-suspenders check.
                if !lease.is_lost() {
                    let (reason, message) = match failure {
                        ProvisionFailure::Workspace(message) => {
                            (FailureReason::WorkspaceError, message)
                        }
                        ProvisionFailure::Setup { message, exit_code } => (
                            FailureReason::SetupFailed,
                            format!("exit_code={exit_code:?}: {message}"),
                        ),
                    };
                    fail_item(
                        state,
                        FailureContext {
                            epic_id: None,
                            // T-551: unlike the epic branch's `task_id: None`
                            // here (no task exists yet when an epic's
                            // provisioning fails), a standalone task IS the
                            // item that fails — every reason, including
                            // these two, names it (see `FailureContext`'s own
                            // doc on this invariant).
                            task_id: Some(task_id),
                            reason,
                            message: &message,
                            push: PushIntent::Skip,
                        },
                    )
                    .await;
                }
                return;
            }
        }
    };

    // The preflight gate (T-521/D5), re-fetching the task first exactly as
    // the epic branch re-fetches the epic — a Cancel/retry landing while
    // provisioning was in flight must be honored before spending any more
    // time running `test_cmd`. There is no standalone-task Cancel surface
    // today (see the module doc's "T-551" section), but re-checking costs
    // nothing and keeps this function's shape identical to the epic one.
    {
        if lease.is_lost() {
            return;
        }
        let conn = state.db.conn();
        let Ok(Some(task)) = crate::tasks::fetch_task(conn, task_id).await else {
            return;
        };
        if task.status != "InProgress" {
            return;
        }
        let pat = crate::projects::load_decrypted_pat(state, &task.project_id)
            .await
            .ok()
            .flatten();
        if let PreflightOutcome::Blocked =
            run_preflight(state, None, Some(task_id), &workspace, pat.as_deref()).await
        {
            return;
        }
    }

    // Re-fetch once more immediately before driving the task through the
    // shared pipeline — the same "check right before the long stretch of
    // agent turns" discipline every pause point in this module already
    // follows.
    if lease.is_lost() {
        return;
    }
    let conn = state.db.conn();
    let Ok(Some(task)) = crate::tasks::fetch_task(conn, task_id).await else {
        return;
    };
    if task.status != "InProgress" {
        return;
    }

    // No epic background, no siblings — a standalone task has neither
    // (D17; `spec::TaskContext` already treats both as optional/empty by
    // design). `process_one_task` is the identical T-513/T-522/T-530/T-531/
    // T-532 sequence an epic-owned task runs; see that function's own doc,
    // "Generalized for T-551, not duplicated," for how it derives
    // `epic_id: None` straight from `task.epic_id`.
    match process_one_task(state, &task, None, &[], &workspace, lease).await {
        TaskStepOutcome::Stop => return,
        TaskStepOutcome::Continue => {}
    }

    // Finalize (push + open PR) — mirrors run_epic_pipeline_inner's own
    // "all Done -> finalize" hand-off. Re-fetch once more: `process_one_task`
    // already left `task.status = 'Done'`, but `finalize_task` wants a fresh
    // row (title/description could theoretically have moved under a
    // concurrent `PATCH /tasks/{id}`, exactly the same staleness concern
    // `finalize_epic`'s own fresh `epic` re-fetch avoids).
    if lease.is_lost() {
        return;
    }
    let conn = state.db.conn();
    let Ok(Some(task)) = crate::tasks::fetch_task(conn, task_id).await else {
        return;
    };
    finalize_task(state, task_id, &task, &workspace, lease).await;
}

/// Run the claimed-epic pipeline body to completion on `epic_id`,
/// lease-unaware (always treats the lease as held). Kept as the direct-call
/// seam tests use to drive the walk hermetically without going through the
/// claim/heartbeat machinery at all; the pool calls the lease-aware
/// [`run_epic_pipeline_inner`] instead (see [`try_claim_and_run`]).
pub async fn run_epic_pipeline(state: AppState, epic_id: String) {
    run_epic_pipeline_inner(state, epic_id, LeaseHandle::new()).await;
}

/// The standalone-task mirror of [`run_epic_pipeline`] (T-551): run
/// [`run_standalone_pipeline_inner`] to completion on `task_id`,
/// lease-unaware, for tests that want to drive a standalone claim's pipeline
/// directly without going through `claim_task`/the pool at all. The pool
/// itself calls the lease-aware body through [`try_claim_and_run`] instead.
pub async fn run_standalone_pipeline(state: AppState, task_id: String) {
    run_standalone_pipeline_inner(&state, &task_id, &LeaseHandle::new()).await;
}

/// The claimed-epic pipeline body: workspace provisioning (T-511) followed by
/// the real per-task implement walk (T-513). See the module doc's "The real
/// implement walk" section for the full per-task sequence and the rationale
/// behind each step (`base_sha` timing, why the epic never reaches
/// `Completed` here, how failure and cancellation both stop the walk the
/// same way). This function is the orchestration shell around that sequence:
/// the provisioning gate, then a loop that re-validates the epic/lease before
/// every single task, processes exactly one task per iteration
/// ([`process_one_task`]), and returns the moment there is nothing left to do
/// or something says to stop.
///
/// Lease-aware (T-510): checks `lease.is_lost()` at the top of every loop
/// iteration and returns immediately, with no further writes, the moment the
/// heartbeat has flagged the lease as fenced out. Also awaits the T-510
/// test-only pipeline hook exactly once, before the first check, so a test
/// can gate/observe the body without sleeps (see
/// [`crate::AppState::test_pipeline_hook`]).
async fn run_epic_pipeline_inner(state: AppState, epic_id: String, lease: LeaseHandle) {
    #[cfg(test)]
    if let Some(hook) = state.test_pipeline_hook.clone() {
        hook().await;
    }

    // T-511: provision the workspace once per claim, before the walk below
    // ever runs. Only when the epic is actually InProgress — a claim racing a
    // Cancel/Block, or (defensively) any other status, must leave the epic
    // untouched here exactly as the walk's own status guard below would.
    let workspace = {
        if lease.is_lost() {
            return;
        }
        let conn = state.db.conn();
        let Ok(Some(epic)) = fetch_epic(conn, &epic_id).await else {
            return;
        };
        if epic.status != "InProgress" {
            return;
        }
        match workspace::provision_epic_workspace(&state, &epic_id, &epic.project_id).await {
            Ok(ws) => ws,
            Err(failure) => {
                // Re-check the lease right before writing: a slow
                // provisioning failure racing a fenced-out lease must not
                // stomp on the new owner's epic (mirrors the same
                // belt-and-suspenders fencing the walk's own writes use).
                //
                // T-540: no `ProvisionedWorkspace` exists at this point
                // (`provision_epic_workspace` returned `Err`, never `Ok`) —
                // `PushIntent::Skip` is the only option this call site can
                // even construct, not a choice among alternatives.
                if !lease.is_lost() {
                    let (reason, message) = match failure {
                        ProvisionFailure::Workspace(message) => {
                            (FailureReason::WorkspaceError, message)
                        }
                        ProvisionFailure::Setup { message, exit_code } => (
                            FailureReason::SetupFailed,
                            format!("exit_code={exit_code:?}: {message}"),
                        ),
                    };
                    fail_item(
                        &state,
                        FailureContext {
                            epic_id: Some(&epic_id),
                            task_id: None,
                            reason,
                            message: &message,
                            push: PushIntent::Skip,
                        },
                    )
                    .await;
                }
                return;
            }
        }
    };

    // T-521: the green-tree gate — see the module doc's "The preflight gate"
    // section for the full rationale (why it exists, the timeout mapping,
    // and why it runs on every claim including a re-claim). Gated on a fresh
    // re-fetch of the epic, exactly like the provisioning block above, so a
    // Cancel/Block landing while provisioning was in flight is honored
    // before spending any more time running `test_cmd`.
    {
        if lease.is_lost() {
            return;
        }
        let conn = state.db.conn();
        let Ok(Some(epic)) = fetch_epic(conn, &epic_id).await else {
            return;
        };
        if epic.status != "InProgress" {
            return;
        }
        let pat = crate::projects::load_decrypted_pat(&state, &epic.project_id)
            .await
            .ok()
            .flatten();
        if let PreflightOutcome::Blocked =
            run_preflight(&state, Some(&epic_id), None, &workspace, pat.as_deref()).await
        {
            return;
        }
    }

    // ---- the real DAG walk (T-513) ----
    loop {
        // Lease-aware bail: a heartbeat renewal failure means another worker
        // now owns this epic. Stop writing immediately — any further mutation
        // here could race the new owner's own walk. Checked first thing on
        // every iteration — this is the "between tasks" re-check the module
        // doc describes.
        if lease.is_lost() {
            tracing::warn!(
                epic = %epic_id,
                "pipeline: lease lost (fenced out); abandoning without further writes"
            );
            return;
        }

        let conn = state.db.conn();

        // 1. Guard: only act on an InProgress epic. A Cancel/Block during the
        //    walk makes this a clean no-op — the other half of the "between
        //    tasks" re-check.
        let Some(epic) = fetch_epic(conn, &epic_id).await.unwrap_or(None) else {
            tracing::debug!(epic = %epic_id, "pipeline: epic vanished; stopping");
            return;
        };
        if epic.status != "InProgress" {
            tracing::debug!(
                epic = %epic_id,
                status = %epic.status,
                "pipeline: epic no longer InProgress; stopping"
            );
            return;
        }

        // 2. Compute the DAG with readiness.
        let dag = match compute_dag(conn, &epic_id).await {
            Ok(dag) => dag,
            Err(err) => {
                tracing::warn!(
                    epic = %epic_id,
                    error = %err,
                    "pipeline: failed to compute DAG; stopping"
                );
                return;
            }
        };

        // 3. Defensive: no task should ever be InProgress at loop-top — this
        //    walk fully serializes (one task claimed, run to a terminal
        //    state, before the next is even looked up), and any orphan left
        //    by a previous owner was already reset to Todo as part of the
        //    claim (`reset_orphaned_tasks`, called before this body ever
        //    runs). Seeing one here means the DAG cannot be trusted; stop
        //    rather than spin.
        if dag.nodes.iter().any(|n| n.task.status == "InProgress") {
            tracing::warn!(
                epic = %epic_id,
                "pipeline: found an InProgress task at loop-top (unexpected); stopping"
            );
            return;
        }

        // 4. Find a ready task (Todo + all blockers Done).
        let Some(ready) = dag.nodes.iter().find(|n| n.ready) else {
            // 5. No ready task.
            let all_done = dag.nodes.iter().all(|n| n.task.status == "Done");
            if all_done {
                // The DAG is fully Done (or the epic has no tasks at all).
                // Publish the final DAG state, then hand off to T-514's
                // finalize step (push + open PR); see the module doc's
                // "Completed only after a real PR opens" section. A lost
                // lease between the DAG check above and here must still be
                // re-checked — finalize does its own writes.
                capability::publish_dag(&state, &epic_id).await;
                if !lease.is_lost() {
                    finalize_epic(&state, &epic_id, &epic, &dag, &workspace, &lease).await;
                }
            } else {
                // Some Todo tasks remain but none are ready (all blocked) and
                // none InProgress — the DAG cannot progress. A valid acyclic
                // DAG walked in dependency order never hits this (cycles are
                // rejected at link time). Log and stop; do NOT infinite-loop.
                tracing::warn!(
                    epic = %epic_id,
                    "pipeline: no ready task but not all Done; DAG is stuck; stopping"
                );
            }
            return;
        };

        // The sibling manifest (D8): every *other* task in the epic,
        // partitioned Done vs. not by `build_context`. Built here (rather
        // than inside `process_one_task`, since T-551 generalized that
        // function to take a plain `siblings: &[SiblingTask]` an epic-less
        // standalone caller can pass as `&[]`) from the DAG already in hand
        // — fresher than any separate query, and avoids a second round trip.
        let siblings: Vec<(String, String, bool)> = dag
            .nodes
            .iter()
            .filter(|n| n.task.id != ready.task.id)
            .map(|n| {
                (
                    n.task.id.clone(),
                    n.task.title.clone(),
                    n.task.status == "Done",
                )
            })
            .collect();
        let sibling_refs: Vec<SiblingTask> = siblings
            .iter()
            .map(|(id, title, done)| SiblingTask {
                id,
                title,
                done: *done,
            })
            .collect();
        let epic_ctx = EpicContext {
            title: &epic.title,
            description: epic.description.as_deref(),
        };

        match process_one_task(
            &state,
            &ready.task,
            Some(epic_ctx),
            &sibling_refs,
            &workspace,
            &lease,
        )
        .await
        {
            TaskStepOutcome::Continue => continue,
            TaskStepOutcome::Stop => return,
        }
    }
}

/// What [`process_one_task`] tells the walk's loop to do next.
enum TaskStepOutcome {
    /// The task reached a terminal state (`Done`, committed or not); loop
    /// back to the top and look for the next ready task.
    Continue,
    /// Something said to stop: a failure (routed to `Blocked(agent_error)`),
    /// a cancelled/fenced-out epic observed mid-task, or a git-level error.
    /// The caller returns immediately — no further writes.
    Stop,
}

/// This task's next `Stage::Implement` attempt number ([`evidence::next_attempt`
///]: one past the highest recorded, so a re-run of a previously-attempted task
/// reads "Attempt 2", never a second indistinguishable "Attempt 1"), or the
/// standard `agent_error` failure route. A whole-future box exactly like
/// [`resolve_or_fail`] — same rationale: keeping the lookup *and* [`fail_item`]'s
/// own sizeable future out of [`process_one_task`]'s generator layout and polled
/// stack depth (that function sits at the bottom of every overflow margin this
/// suite has ever hit). `Err(())` means the failure was routed — stop writing.
#[allow(clippy::too_many_arguments)]
fn next_implement_attempt_or_fail<'a>(
    state: &'a AppState,
    conn: &'a Connection,
    epic_id: Option<&'a str>,
    task_id: &'a str,
    workspace: &'a ProvisionedWorkspace,
    lease: &'a LeaseHandle,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<i64, ()>> + Send + 'a>> {
    Box::pin(async move {
        match evidence::next_attempt(conn, task_id, Stage::Implement.as_str()).await {
            Ok(n) => Ok(n),
            Err(err) => {
                if !lease.is_lost() {
                    fail_item(
                        state,
                        FailureContext {
                            epic_id,
                            task_id: Some(task_id),
                            reason: FailureReason::AgentError,
                            message: &format!("failed to compute next attempt number: {err}"),
                            push: PushIntent::Attempt(workspace),
                        },
                    )
                    .await;
                }
                Err(())
            }
        }
    })
}

/// `git add -A` + commit **iff** there is something to commit, opening the
/// §2.2 `Stage::Commit` evidence row only when a commit actually happens
/// (D13: every stage that *runs* gets a row, not every stage that could
/// have). This is T-513's original "no diff is committed as nothing"
/// contract for the initial `impl(...)` commit, factored out here so T-531's
/// `fix(...) review round N` commits reuse the *identical* check rather than
/// growing a second, subtly different "is there anything to commit"
/// implementation — see the module doc's T-531 section, "a fix round with no
/// diff", for why that reuse matters (it's what makes the round counter the
/// sole thing guaranteeing termination, not "did this round happen to
/// produce a commit").
///
/// `commit_attempt` is only the `Stage::Commit` row's own `attempt` value
/// (never read back by anything in this module — [`build_task_checklist`]
/// keys its SHA lookup on `task_id` and `created_at`, not `attempt`); it
/// exists purely so a task with more than one commit (any task that goes
/// through a fix round) doesn't leave several `commit` rows all claiming
/// `attempt = 1`, keeping `GET /tasks/{id}/runs` legible for a human. T-513's
/// call passes `1` (unchanged); T-531's fix-round call passes `1 + round` so
/// it never collides with the initial commit's `1`.
///
/// Returns `Ok(None)` for "nothing to commit" (not an error — the caller
/// decides what that means for its own step), `Ok(Some(sha))` for a real
/// commit, `Err` for any git-level failure (`add`, `status`, or `commit`
/// itself).
async fn commit_if_dirty(
    conn: &Connection,
    task_id: &str,
    epic_id: Option<&str>,
    workspace_path: &std::path::Path,
    subject: &str,
    commit_attempt: i64,
) -> Result<Option<String>, git::GitError> {
    git::add_all(workspace_path).await?;
    let status = git::status_porcelain(workspace_path).await?;
    if status.trim().is_empty() {
        return Ok(None);
    }
    let sha = git::commit_all(workspace_path, subject, COMMITTER_NAME, COMMITTER_EMAIL).await?;
    // §2.2: the Commit stage "records the SHA in log". Opened only now that
    // a commit actually happened (D13, see this function's own doc).
    let open = OpenStage {
        task_id: Some(task_id),
        epic_id,
        stage: Stage::Commit.as_str(),
        attempt: commit_attempt,
        harness: None,
        model: None,
        prompt_hash: None,
    };
    if let Ok(handle) = evidence::open_stage(conn, open).await {
        let _ = evidence::close_stage(
            conn,
            &handle,
            CloseStage {
                status: "ok",
                session_id: None,
                verdict: None,
                exit_code: Some(0),
                log: format!("commit {sha}: {subject}"),
                input_tokens: None,
                output_tokens: None,
            },
        )
        .await;
    }
    Ok(Some(sha))
}

/// Process exactly one task (an epic's ready DAG node, or — T-551 — the sole
/// task of a standalone claim) through the full
/// T-513/T-522/T-530/T-531/T-532 sequence: record `base_sha`, `Todo →
/// InProgress`, assemble the D8 prompt, run `Stage::Implement`, the T-522
/// test-gate/fix loop, `git add -A` + commit-if-dirty
/// ([`commit_if_dirty`]), and then exactly one of two branches on whether
/// that produced a commit: a real diff runs the T-530/T-531 review → fix →
/// re-test → re-commit → re-review convergence loop
/// ([`run_review_fix_converge`]); no diff at all runs T-532's
/// already-complete verification instead ([`run_verify_complete`]) — either
/// way, `Done`. An implement run that comes back not-`ok` takes the failure
/// exit instead: an ordinary failure first salvages whatever the agent
/// completed with one more [`commit_if_dirty`] call (see the module doc's
/// "Salvaging completed-but-uncommitted work" section), then routes through
/// [`route_stage_failure`]. See the module doc's "The real implement walk",
/// "Review, verdict, and convergence", "Review → fix → re-test → re-commit",
/// and "Already-complete verification" sections for the rationale behind each
/// step; this function is the literal implementation of that sequence.
///
/// ## Generalized for T-551, not duplicated
///
/// Through T-550 this function took `epic_id: &str` plus an `epic: &Epic`,
/// `dag: &Dag`, and `ready: &DagNode` — three epic-shaped inputs it used only
/// to derive the task's own spec fields, the epic background, and the
/// sibling manifest. T-551 needs the identical sequence to run for a
/// standalone task, which has a `Task` row but no epic, no DAG, and no
/// siblings — rather than fork a second copy of this (large) function, the
/// three epic-shaped inputs are replaced with what every step actually
/// consumes: `task` (a plain `&Task` — an epic's `DagNode::task` and a
/// standalone claim's directly-fetched row are both exactly this type),
/// `epic_ctx` (`None` for a standalone task — `spec::TaskContext` already
/// treats that as "no epic background" by design, D17/T-502), and `siblings`
/// (an empty slice for a standalone task — likewise already a supported,
/// tested shape). `epic_id` itself is no longer a separate parameter at all:
/// it is `task.epic_id.as_deref()`, since that column **is** the
/// epic/standalone distinction (`Task::epic_id`'s own doc: "`NULL` =>
/// standalone"). The caller ([`run_epic_pipeline_inner`]'s DAG-walk loop, or
/// — new — [`run_standalone_pipeline_inner`]) builds whichever of
/// `epic_ctx`/`siblings` its own shape supports and hands this function a
/// task row; every line below this point runs identically for both.
async fn process_one_task(
    state: &AppState,
    task: &crate::tasks::Task,
    epic_ctx: Option<EpicContext<'_>>,
    siblings: &[SiblingTask<'_>],
    workspace: &ProvisionedWorkspace,
    lease: &LeaseHandle,
) -> TaskStepOutcome {
    let conn = state.db.conn();
    let epic_id: Option<&str> = task.epic_id.as_deref();
    let task_id: &str = &task.id;
    let task_title: &str = &task.title;
    let project_id: &str = &task.project_id;

    // 1. base_sha: the workspace's HEAD *before* this task's work — recorded
    //    now, before the implement stage (or its eventual commit) can move
    //    HEAD out from under us. See the module doc for why this ordering is
    //    load-bearing, not incidental.
    let base_sha = match git::current_commit(&workspace.workspace_path).await {
        Ok(sha) => sha,
        Err(err) => {
            if !lease.is_lost() {
                fail_item(
                    state,
                    FailureContext {
                        epic_id,
                        task_id: Some(task_id),
                        reason: FailureReason::AgentError,
                        message: &format!("failed to read base_sha: {err}"),
                        push: PushIntent::Attempt(workspace),
                    },
                )
                .await;
            }
            return TaskStepOutcome::Stop;
        }
    };

    let now = now_ms();
    let _ = conn
        .execute(
            "UPDATE task SET status = 'InProgress', base_sha = ?1, updated_at = ?2 WHERE id = ?3",
            // Cloned, not moved: T-530's review stage (step 5b below) needs
            // `base_sha` again, well after this write.
            params![base_sha.clone(), now, task_id],
        )
        .await;
    if let Some(epic_id) = epic_id {
        capability::publish_dag(state, epic_id).await;
    }
    // A standalone task's own status is what the project board shows
    // directly (there is no epic lane it moves instead) — this
    // Todo→InProgress write is already reflected in the row `POST
    // /tasks/{id}/run` returned, so no `board_updated` is needed here for
    // that transition specifically; see `run_standalone_pipeline_inner`'s own
    // doc for where this task's board-visible transitions actually get
    // published.

    // 2. The D8 prompt: rendered spec + epic background (if any) + sibling
    //    manifest (empty for a standalone task). The instruction text is the
    //    slot's live-resolved effective prompt (T6): the project override when
    //    set, else prompts/implement.md — read at spawn time (design §9).
    let task_ctx = TaskContext {
        spec: SpecFields {
            title: task_title,
            description: task.description.as_deref(),
            acceptance: task.acceptance.as_deref(),
        },
        epic: epic_ctx,
        siblings,
        // No cumulative-diff concept for Implement — only Review (T-530)
        // populates this. See spec::TaskContext's doc.
        base_sha: None,
    };
    let implement_cfg = match resolve_or_fail(
        state,
        epic_id,
        project_id,
        task_id,
        Stage::Implement,
        workspace,
        lease,
    )
    .await
    {
        Ok(cfg) => cfg,
        Err(()) => return TaskStepOutcome::Stop,
    };
    let prompt = task_agent::assemble_prompt_text(&implement_cfg.prompt, &task_ctx);

    // 3. Run the implement stage through the TaskAgent seam — with a bounded
    //    auto-retry when the run fails on what looks like a *transient*
    //    provider condition (`DEARBORN_IMPLEMENT_TRANSIENT_RETRIES`, see
    //    [`is_transient_provider_error`]). The incident this closes: pi
    //    recovered from a mid-run HTTP 429, exited cleanly, and produced the
    //    full fix — but an outcome carrying any `RunEvent::Error` can still
    //    come back not-`ok` here (e.g. the recovery itself failed the
    //    final-state check), and routing straight to `route_stage_failure`
    //    discarded completed-but-uncommitted work whose only sin was an
    //    upstream hiccup. One extra attempt (the default) re-runs the whole
    //    stage against the same workspace; because every prior attempt's
    //    partial output is just dirty-tree content at that point, a retry
    //    overwrites it exactly like a human re-running would. Total attempts
    //    are `1 + retries`; each attempt opens its own `agent_run` row under
    //    its own incremented `attempt` number so the evidence trail shows the
    //    full history rather than one overwritten row. Only a not-`ok`
    //    outcome that is neither timed out nor cancelled and whose recorded
    //    error text matches a transient signal earns a retry — anything else
    //    falls through to the ordinary [`route_stage_failure`] handling
    //    unchanged, and exhausted retries land there too.
    let max_attempts = 1 + state.config.executor.implement_transient_retries as i64;
    // The starting attempt number is computed, not hardcoded: one past
    // whatever `implement` attempts this task already has, so a re-run of a
    // previously attempted task (a failed stage reset to Todo, or an
    // orphaned InProgress task reset by a new owner after a crash/restart)
    // reads "Attempt 2" in the timeline instead of a second
    // indistinguishable "Attempt 1" sitting next to the first one. The retry
    // loop below increments from wherever that lands.
    let mut attempt =
        match next_implement_attempt_or_fail(state, conn, epic_id, task_id, workspace, lease).await
        {
            Ok(n) => n,
            Err(()) => return TaskStepOutcome::Stop,
        };
    // Total *tries this call* may make = the first try plus the bounded
    // transient retries; expressed against the absolute `attempt` counter
    // (which starts at one-past-the-highest-recorded, see above) so the
    // guard below needs no separate per-call tally.
    let last_attempt = attempt + max_attempts - 1;
    let outcome = loop {
        let run_id = ulid::Ulid::new().to_string();
        // Cloned per attempt, not built once outside the loop: `TaskRunRequest`
        // is consumed by `run_agent_stage`'s harness call (`agent.run(req)`),
        // and each attempt needs its own fresh `run_id` anyway.
        let req = TaskRunRequest {
            run_id,
            stage: Stage::Implement,
            prompt: prompt.clone(),
            cwd: workspace.workspace_path.clone(),
            harness: implement_cfg.harness.clone(),
            model: implement_cfg.model.clone(),
            prompt_hash: implement_cfg.prompt_hash.clone(),
        };
        let outcome = task_agent::run_agent_stage(
            state,
            &*state.task_agent,
            AgentStageParams {
                task_id: Some(task_id),
                epic_id,
                attempt,
            },
            req,
        )
        .await;

        let outcome = match outcome {
            Ok(outcome) => outcome,
            Err(err) => {
                if !lease.is_lost() {
                    fail_item(
                        state,
                        FailureContext {
                            epic_id,
                            task_id: Some(task_id),
                            reason: FailureReason::AgentError,
                            message: &format!("implement stage failed to start: {err}"),
                            push: PushIntent::Attempt(workspace),
                        },
                    )
                    .await;
                }
                return TaskStepOutcome::Stop;
            }
        };

        // Retry decision, kept next to the run itself so the four "this does
        // not earn another attempt" conditions read as one guard: success,
        // last attempt, timeout/cancel (both mean a human- or config-visible
        // kill, never an upstream hiccup — `timed_out` is checked before
        // `cancelled` for the same reason `route_stage_failure` checks it
        // first), or error text that doesn't look transient. Everything else
        // falls through to the ordinary `route_stage_failure` handling below,
        // which is also where retries-exhausted lands (the loop simply exits
        // on its last attempt without retrying).
        let err_text = outcome
            .last_error_message
            .as_deref()
            .unwrap_or(&outcome.text);
        if outcome.is_ok()
            || outcome.timed_out
            || outcome.cancelled
            || attempt >= last_attempt
            || !is_transient_provider_error(err_text)
        {
            break outcome;
        }
        tracing::warn!(
            epic = ?epic_id,
            task = %task_id,
            attempt,
            max_attempts,
            error_text = %err_text,
            "implement stage failed on a transient provider error; retrying"
        );
        attempt += 1;
    };

    // MILESTONE_2 §4 (tracer bullet): anything that fails here Blocks the
    // epic (and, since T-540, fails the task itself) with `agent_error` via
    // the centralized `fail_item` router — unless T-542 killed it, in which
    // case `route_stage_failure` sends this to `handle_cancelled_task`
    // instead (see the module doc's "T-542: cancellation as a kill" section).
    if !outcome.is_ok() {
        // Salvage commit (Recommendation 3): an ordinary failure can still
        // leave hours of *finished* work sitting uncommitted in the workspace
        // (the recovered-from-a-transient-hiccup incident this closes), and
        // nothing downstream would ever save it — the next claim's re-
        // provision (`git reset --hard HEAD` + `git clean -fd`) destroys a
        // dirty tree outright, and `fail_item`'s triage push only pushes what
        // is already committed. So commit whatever the agent managed to
        // complete onto the task branch BEFORE routing to the failure path:
        // the same §2.8 subject step 5 below would have used (this diff is
        // the task's first real commit), the same attempt number `1`, and the
        // same lease fencing every other write here respects (a lost lease
        // means someone else owns the task; neither the salvage commit nor
        // the failure routing that follows may act on the workspace). A
        // `timed_out` or `cancelled` outcome deliberately skips this — a
        // cancelled task resets to `Todo` and must keep its resumable dirty
        // tree (see [`handle_cancelled_task`]), and a deadline kill deserves
        // the same treatment — see the module doc's "Salvaging
        // completed-but-uncommitted work" section for the full rationale.
        if !outcome.timed_out && !outcome.cancelled && !lease.is_lost() {
            let subject = format!("impl({}): {}", spec::short_id(task_id), task_title);
            if let Err(err) = commit_if_dirty(
                conn,
                task_id,
                epic_id,
                &workspace.workspace_path,
                &subject,
                1,
            )
            .await
            {
                tracing::warn!(
                    epic = ?epic_id,
                    task = %task_id,
                    error = %err,
                    "salvage commit after an ordinary implement failure failed; \
                     proceeding to the failure route with the tree as-is"
                );
            }
        }
        route_stage_failure(
            state,
            epic_id,
            task_id,
            &outcome,
            "implement stage did not complete successfully",
            workspace,
            lease,
            // Rec 5: only the implement stage opts into the finer taxonomy —
            // its retry loop already consults `is_transient_provider_error`,
            // so exhausted-retries transient failures land here and get
            // classified `provider_rate_limited` (with the provider's error
            // text as the persisted detail) instead of `agent_error`.
            true,
        )
        .await;
        return TaskStepOutcome::Stop;
    }

    // Re-check the container's status *and* the lease immediately before the
    // test-gate/commit/Done writes below — a slow implement run racing an
    // external cancel (a lane move away from InProgress) or a lease theft
    // must not finalize this task after either happened. This is the
    // "cancelling mid-walk stops cleanly" AC (the D12 stage-boundary
    // backstop); the implement stage's own `is_ok()` check just above is
    // where an actual kill (T-542, `outcome.cancelled`) gets observed.
    if lease.is_lost() || !container_still_in_progress(conn, epic_id, task_id).await {
        tracing::warn!(
            epic = ?epic_id,
            task = %task_id,
            "pipeline: container cancelled or lease lost mid-task; stopping without finalizing"
        );
        return TaskStepOutcome::Stop;
    }

    // 4. T-522: the test gate + test-driven fix loop. See the module doc's
    //    "The test gate & fix loop" section for the full rationale — in
    //    short, a red test_cmd never reaches the commit step below, and the
    //    fix agent this loop drives sees only the failing output, nothing
    //    else (D19).
    let pat = crate::projects::load_decrypted_pat(state, project_id)
        .await
        .ok()
        .flatten();
    match run_test_gate_loop(
        state,
        epic_id,
        project_id,
        task_id,
        workspace,
        pat.as_deref(),
        lease,
    )
    .await
    {
        GateOutcome::Proceed => {}
        GateOutcome::Stop => return TaskStepOutcome::Stop,
    }

    // 5. git add -A, then commit iff there is something to commit
    //    ([`commit_if_dirty`]). An agent that made no changes is committed as
    //    *nothing* — see below (T-532) for why that's not the end of the
    //    story.
    let subject = format!("impl({}): {}", spec::short_id(task_id), task_title);
    let committed = match commit_if_dirty(
        conn,
        task_id,
        epic_id,
        &workspace.workspace_path,
        &subject,
        1,
    )
    .await
    {
        Ok(committed) => committed,
        Err(err) => {
            if !lease.is_lost() {
                fail_item(
                    state,
                    FailureContext {
                        epic_id,
                        task_id: Some(task_id),
                        reason: FailureReason::AgentError,
                        message: &format!("git commit failed: {err}"),
                        push: PushIntent::Attempt(workspace),
                    },
                )
                .await;
            }
            return TaskStepOutcome::Stop;
        }
    };

    match committed {
        Some(_sha) => {
            // 5b. T-530/T-531: review the cumulative diff now that there's a
            //     commit to review, converging on a verdict via the review ->
            //     fix -> re-test -> re-commit -> re-review loop
            //     ([`run_review_fix_converge`]).
            if lease.is_lost() || !container_still_in_progress(conn, epic_id, task_id).await {
                tracing::warn!(
                    epic = ?epic_id,
                    task = %task_id,
                    "pipeline: container cancelled or lease lost before the review stage; stopping without finalizing"
                );
                return TaskStepOutcome::Stop;
            }

            let review_ctx = TaskContext {
                base_sha: Some(base_sha.as_str()),
                ..task_ctx
            };
            let review_cfg = match resolve_or_fail(
                state,
                epic_id,
                project_id,
                task_id,
                Stage::Review,
                workspace,
                lease,
            )
            .await
            {
                Ok(cfg) => cfg,
                Err(()) => return TaskStepOutcome::Stop,
            };
            let review_prompt = task_agent::assemble_prompt_text(&review_cfg.prompt, &review_ctx);

            match run_review_fix_converge(
                state,
                epic_id,
                project_id,
                task_id,
                task_title,
                workspace,
                &review_prompt,
                &review_cfg,
                pat.as_deref(),
                lease,
            )
            .await
            {
                ConvergenceOutcome::Done => {
                    // Proceed to Done below, exactly as if there were no
                    // review stage at all.
                }
                ConvergenceOutcome::Stop => return TaskStepOutcome::Stop,
            }
        }
        None => {
            // 5c. T-532: the implement stage produced no diff at all — the
            //     agent judged the task already satisfied by earlier work.
            //     `Stage::VerifyComplete` independently checks that claim
            //     against the spec before this task is allowed to close with
            //     zero commits. See the module doc's "Already-complete
            //     verification (T-532)" section for the full rationale.
            if lease.is_lost() || !container_still_in_progress(conn, epic_id, task_id).await {
                tracing::warn!(
                    epic = ?epic_id,
                    task = %task_id,
                    "pipeline: container cancelled or lease lost before verify-complete; stopping without finalizing"
                );
                return TaskStepOutcome::Stop;
            }

            // No `base_sha` here (unlike the review branch above): there is
            // no diff to speak of yet, and `prompts/verify_complete.md`
            // explicitly tells the agent this is NOT a diff review — it must
            // read the end state of the code, not `git diff`. `task_ctx`
            // already has `base_sha: None`; reused as-is rather than cloning
            // a `base_sha`-bearing copy the way the review branch above does.
            let verify_cfg = match resolve_or_fail(
                state,
                epic_id,
                project_id,
                task_id,
                Stage::VerifyComplete,
                workspace,
                lease,
            )
            .await
            {
                Ok(cfg) => cfg,
                Err(()) => return TaskStepOutcome::Stop,
            };
            let verify_prompt = task_agent::assemble_prompt_text(&verify_cfg.prompt, &task_ctx);

            match run_verify_complete(
                state,
                epic_id,
                project_id,
                task_id,
                task_title,
                workspace,
                &verify_prompt,
                &verify_cfg,
                task_ctx,
                &base_sha,
                pat.as_deref(),
                lease,
            )
            .await
            {
                TaskStepOutcome::Continue => {
                    // PASS (zero commits) or NEEDS_CHANGES that converged
                    // through the ordinary pipeline (its own commit(s)
                    // already landed) — proceed to Done below either way.
                }
                TaskStepOutcome::Stop => return TaskStepOutcome::Stop,
            }
        }
    }

    // 6. Done.
    if lease.is_lost() {
        return TaskStepOutcome::Stop;
    }
    let now = now_ms();
    let _ = conn
        .execute(
            "UPDATE task SET status = 'Done', updated_at = ?1 WHERE id = ?2",
            params![now, task_id],
        )
        .await;
    match epic_id {
        Some(epic_id) => capability::publish_dag(state, epic_id).await,
        // T-551: a standalone task reaching `Done` *is* the board-visible
        // change (there is no epic lane it moves separately) — publish
        // `board_updated` directly rather than relying on
        // `run_standalone_pipeline_inner`'s later finalize publish, since a
        // task that produces no diff (T-532's PASS path) still needs this
        // announced even when finalize's own push/PR never has new work to
        // report beyond what's already committed.
        None => board::publish_board(state, project_id).await,
    }

    TaskStepOutcome::Continue
}

/// Whether the work item backing this task is still active enough to keep
/// writing to it — the "between tasks" / "before a slow step's finalizing
/// writes" re-check every stop-worthy pause in this walk performs. Factored
/// out once [`process_one_task`]'s pre-existing T-513 check and
/// [`run_test_gate_loop`]'s T-522 checks both needed the identical query.
///
/// T-551 generalizes this from "is the epic InProgress" to "is *the
/// container* InProgress": for an epic-owned task (`epic_id: Some`) the
/// container is the epic, exactly as before; for a standalone task
/// (`epic_id: None`) there is no epic to ask — the task **is** its own
/// claimable container (see [`FailureContext`]'s doc for the same
/// epic/standalone duality), so this checks `task_id`'s own row instead. A
/// standalone task's own status is what a Cancel (were one ever wired up for
/// tasks — see the module doc's T-551 section on why that's out of scope
/// here) or an external mutation would move off `InProgress`, so re-reading
/// it here catches exactly the same class of race the epic branch already
/// guarded against.
async fn container_still_in_progress(
    conn: &Connection,
    epic_id: Option<&str>,
    task_id: &str,
) -> bool {
    match epic_id {
        Some(epic_id) => {
            matches!(fetch_epic(conn, epic_id).await, Ok(Some(e)) if e.status == "InProgress")
        }
        None => {
            matches!(crate::tasks::fetch_task(conn, task_id).await, Ok(Some(t)) if t.status == "InProgress")
        }
    }
}

/// What [`run_test_gate_loop`] tells [`process_one_task`] to do next. See the
/// module doc's "The test gate & fix loop" section for the full rationale.
enum GateOutcome {
    /// The gate is green, or there is no `test_cmd` configured at all
    /// ([`StageOutcome::Skipped`] — T-520's contract) — proceed to the
    /// ordinary commit step exactly as if this loop didn't exist.
    Proceed,
    /// The loop already routed the task to `Failed` and the epic to
    /// `Blocked` (attempts exhausted, the fix agent itself failed, or a
    /// lease/cancellation was observed mid-loop) — the caller's only job is
    /// to stop, with no further writes, exactly like every other failure
    /// exit in this module.
    Stop,
}

/// T-522: run `test_cmd` as the `test_gate` stage; on red, run `Stage::Fix`
/// with the failing output as its **sole** feedback (D19) and retry, up to
/// `DEARBORN_MAX_TEST_FIX_ATTEMPTS`. See the module doc's "The test gate &
/// fix loop" section for the full rationale (commit-only-at-green, the
/// attempt-numbering scheme, why exhaustion fails the *task* and not just the
/// epic, and the D19 concern) — this function is the literal translation of
/// `references/ralph-v2.sh`'s `test_attempt` loop (its `# ---- test gate
/// ----` section) into Dearborn's stage/evidence machinery.
/// Resolve a pipeline stage's live [`SpawnConfig`](crate::agent_settings::SpawnConfig)
/// for `project_id` (T6/T7): maps `stage` onto its [`AgentSlot`], then folds
/// global settings + the project's override around the stage's compiled
/// default prompt. Called at every spawn site **immediately before** prompt
/// assembly — never cached — so a mid-epic settings edit is picked up by the
/// very next stage run (design §9); only meaningful for agent stages
/// (`Stage::is_agent_stage`), since non-agent stages have nothing to resolve.
///
/// Returns a boxed future on purpose: every caller sits inside the already
/// sizeable `process_one_task`/gate/convergence futures, and inlining this
/// resolution's state into each of them overflowed the test runtime's 2 MiB
/// thread stack (the same hazard that boxes the `cmd::run_stage_command`
/// calls below). Boxing keeps the callers' generator layout unchanged.
fn stage_spawn_config<'a>(
    state: &'a AppState,
    project_id: &'a str,
    stage: Stage,
) -> std::pin::Pin<
    Box<
        impl std::future::Future<
                Output = Result<
                    crate::agent_settings::SpawnConfig,
                    crate::agent_settings::SettingsError,
                >,
            > + Send
            + 'a,
    >,
> {
    Box::pin(async move {
        // Both unwraps are guarded by the callers' agent-stage-only contract:
        // every agent stage has a compiled default prompt and a slot mapping.
        let default = crate::spec::prompt_for(stage)
            .expect("stage_spawn_config is only called for agent stages");
        let slot = crate::agent_slot::AgentSlot::from_stage(stage)
            .expect("stage_spawn_config is only called for agent stages");
        crate::agent_settings::spawn_config(&state.db, project_id, slot, default).await
    })
}

/// Resolve a claimed item's slot config or route the standard `agent_error`
/// failure (T6/T7): the resolve-or-fail preamble every agent-stage spawn site
/// shares, in one boxed future so neither the resolution nor [`fail_item`]'s
/// own sizeable future is inlined into the already-huge pipeline generators
/// (the same stack-overflow hazard [`stage_spawn_config`]'s doc describes).
/// `Ok(None)` is impossible; `Err(())` means the failure was routed — the
/// caller must stop without further writes.
#[allow(clippy::too_many_arguments)]
fn resolve_or_fail<'a>(
    state: &'a AppState,
    epic_id: Option<&'a str>,
    project_id: &'a str,
    task_id: &'a str,
    stage: Stage,
    workspace: &'a ProvisionedWorkspace,
    lease: &'a LeaseHandle,
) -> std::pin::Pin<
    Box<
        dyn std::future::Future<Output = Result<crate::agent_settings::SpawnConfig, ()>>
            + Send
            + 'a,
    >,
> {
    Box::pin(async move {
        match stage_spawn_config(state, project_id, stage).await {
            Ok(cfg) => Ok(cfg),
            Err(err) => {
                if !lease.is_lost() {
                    fail_item(
                        state,
                        FailureContext {
                            epic_id,
                            task_id: Some(task_id),
                            reason: FailureReason::AgentError,
                            message: &format!(
                                "failed to resolve {} agent settings: {err}",
                                stage.as_str()
                            ),
                            push: PushIntent::Attempt(workspace),
                        },
                    )
                    .await;
                }
                Err(())
            }
        }
    })
}

async fn run_test_gate_loop(
    state: &AppState,
    epic_id: Option<&str>,
    project_id: &str,
    task_id: &str,
    workspace: &ProvisionedWorkspace,
    pat: Option<&str>,
    lease: &LeaseHandle,
) -> GateOutcome {
    let conn = state.db.conn();
    let cmd_timeout = Duration::from_secs(state.config.executor.cmd_timeout_secs);
    let max_attempts = state.config.executor.max_test_fix_attempts as i64;

    // Attempt 0 is the first gate run — not a retry of anything (see the
    // module doc for why it doesn't start at 1).
    let mut attempt: i64 = 0;
    loop {
        // Belt-and-suspenders re-check, same discipline as every other pause
        // in this walk — a fix round runs a whole agent turn, long enough
        // for a lease to expire or the epic to be cancelled out from under
        // it.
        if lease.is_lost() || !container_still_in_progress(conn, epic_id, task_id).await {
            tracing::warn!(
                epic = ?epic_id,
                task = %task_id,
                "pipeline: epic cancelled or lease lost mid-test-gate; stopping without finalizing"
            );
            return GateOutcome::Stop;
        }

        // Boxed for the same reason `run_preflight`'s call is (see that
        // function's doc): this transitively embeds `run_shell_timed`'s own
        // sizeable stack state, and this call site is itself nested inside
        // the already-large `process_one_task`/`run_epic_pipeline_inner`
        // frames.
        let gate_result = Box::pin(cmd::run_stage_command(
            conn,
            StageCommand {
                task_id: Some(task_id),
                epic_id,
                stage: Stage::TestGate.as_str(),
                attempt,
                cwd: &workspace.workspace_path,
                timeout: cmd_timeout,
            },
            workspace.test_cmd.as_deref(),
            |raw: &str| git::redact(raw, pat),
        ))
        .await;

        let ran = match gate_result {
            // No test_cmd configured: T-520's "skip means no row" contract
            // applies unchanged — no gate, no fix loop, proceed to commit
            // exactly as T-513 already did before this task existed.
            Ok(StageOutcome::Skipped) => return GateOutcome::Proceed,
            // Green: the *only* path out of this loop that leads to a
            // commit — see the module doc for why that's load-bearing.
            Ok(StageOutcome::Ran(ran)) if ran.status == "ok" => return GateOutcome::Proceed,
            Ok(StageOutcome::Ran(ran)) => ran,
            Err(err) => {
                if !lease.is_lost() {
                    fail_item(
                        state,
                        FailureContext {
                            epic_id,
                            task_id: Some(task_id),
                            reason: FailureReason::AgentError,
                            message: &format!("failed to record test_gate evidence: {err}"),
                            push: PushIntent::Attempt(workspace),
                        },
                    )
                    .await;
                }
                return GateOutcome::Stop;
            }
        };

        // Red. Out of attempts?
        if attempt >= max_attempts {
            tracing::warn!(
                epic = ?epic_id,
                task = %task_id,
                attempt,
                "test gate still red after the configured fix attempts; task -> Failed(test_gate_exhausted)"
            );
            if !lease.is_lost() {
                fail_item(
                    state,
                    FailureContext {
                        epic_id,
                        task_id: Some(task_id),
                        reason: FailureReason::TestGateExhausted,
                        message: &format!(
                            "tests still failing after {max_attempts} fix attempt(s)"
                        ),
                        push: PushIntent::Attempt(workspace),
                    },
                )
                .await;
            }
            return GateOutcome::Stop;
        }

        attempt += 1;

        if lease.is_lost() || !container_still_in_progress(conn, epic_id, task_id).await {
            tracing::warn!(
                epic = ?epic_id,
                task = %task_id,
                "pipeline: epic cancelled or lease lost before the fix round; stopping without finalizing"
            );
            return GateOutcome::Stop;
        }

        // D19: the fix agent's entire context is the fix instructions plus
        // this one round's failing output — see
        // `task_agent::assemble_fix_prompt` for the full rationale and an
        // open concern about it. The instruction text re-resolves live each
        // round (T6/design §9): a settings change mid-loop applies from the
        // next fix attempt on.
        let fix_cfg = match resolve_or_fail(
            state,
            epic_id,
            project_id,
            task_id,
            Stage::Fix,
            workspace,
            lease,
        )
        .await
        {
            Ok(cfg) => cfg,
            Err(()) => return GateOutcome::Stop,
        };
        let fix_prompt = task_agent::assemble_fix_prompt_text(&fix_cfg.prompt, &ran.output);
        let run_id = ulid::Ulid::new().to_string();
        let fix_outcome = task_agent::run_agent_stage(
            state,
            &*state.task_agent,
            AgentStageParams {
                task_id: Some(task_id),
                epic_id,
                attempt,
            },
            TaskRunRequest {
                run_id,
                stage: Stage::Fix,
                prompt: fix_prompt,
                cwd: workspace.workspace_path.clone(),
                harness: fix_cfg.harness,
                model: fix_cfg.model,
                prompt_hash: fix_cfg.prompt_hash,
            },
        )
        .await;

        match fix_outcome {
            Ok(outcome) if outcome.is_ok() => {
                // Loop back to the top: re-run the gate at this same
                // `attempt` — that retest is what decides whether this fix
                // round actually worked.
            }
            Ok(outcome) => {
                route_stage_failure(
                    state,
                    epic_id,
                    task_id,
                    &outcome,
                    "fix stage did not complete successfully",
                    workspace,
                    lease,
                    // Not the implement stage — no transient-provider
                    // classification; stays `agent_error`.
                    false,
                )
                .await;
                return GateOutcome::Stop;
            }
            Err(err) => {
                if !lease.is_lost() {
                    fail_item(
                        state,
                        FailureContext {
                            epic_id,
                            task_id: Some(task_id),
                            reason: FailureReason::AgentError,
                            message: &format!("fix stage failed to start: {err}"),
                            push: PushIntent::Attempt(workspace),
                        },
                    )
                    .await;
                }
                return GateOutcome::Stop;
            }
        }
    }
}

// ---- Review, verdict, and convergence (T-530) ------------------------------

/// The terse contract reminder [`run_review_stage`] appends to the **same**
/// review prompt on its single bounded re-run, when [`spec::parse_verdict`]
/// found no `VERDICT:` line in the first attempt's output. Named so a test
/// can assert on it directly (this task's AC: "a terse contract reminder").
/// Deliberately short — the agent already has the full review prompt and
/// context from the first attempt's prompt (repeated verbatim, this text
/// appended after it); the goal is a nudge back to the exact output shape,
/// not a second explanation of the review's job.
const VERDICT_CONTRACT_REMINDER: &str = "## Contract reminder\n\n\
Your previous response did not end with a line matching exactly one of:\n\n\
```\n\
VERDICT: PASS\n\
VERDICT: NEEDS_CHANGES\n\
VERDICT: BLOCKED\n\
```\n\n\
Write your findings, then finish your **final** message with exactly one such \
line — alone, as the very last line, nothing before or after it on that line, \
uppercase, exact spelling.";

/// What [`run_verdict_stage`] tells its caller ([`run_review_fix_converge`],
/// and — T-532 — [`process_one_task`]'s no-diff branch) to do next.
enum VerdictOutcome {
    /// A verdict parsed (on the first try or the one bounded contract-miss
    /// re-run) and has already been recorded on its `agent_run` row and
    /// published as `stage_changed`. `attempt` is the `agent_run.attempt`
    /// value the *winning* (parseable) call landed on — a caller driving a
    /// fix off a `NEEDS_CHANGES` verdict needs it to number that fix's own
    /// row (see [`run_review_fix_converge`]'s doc, "a fix and the review that
    /// follows it share a number", and the module doc's T-532 section for
    /// the identical scheme applied to `Stage::VerifyComplete`). `findings`
    /// is that same call's raw [`task_agent::AgentStageOutcome::text`] — the
    /// agent's own prose — exactly what [`task_agent::assemble_fix_prompt`]
    /// expects as feedback for a `NEEDS_CHANGES` verdict.
    Verdict {
        verdict: spec::Verdict,
        attempt: i64,
        findings: String,
    },
    /// The stage failed to start, errored, or never produced a parseable
    /// verdict after the bounded retry — already routed to
    /// `Failed(agent_error)`/`Blocked` (or the lease was already lost/the
    /// epic already left `InProgress`) — the caller's only job is to stop,
    /// with no further writes, exactly like every other failure exit in this
    /// module.
    Stop,
}

/// T-530/T-531/T-532: run a **verdict-emitting** stage (`Stage::Review` or,
/// T-532, `Stage::VerifyComplete` — the only two stages whose prompt ends
/// with a D9 `VERDICT:` line) against `prompt` (the D8 context already
/// assembled by the caller — see [`task_agent::assemble_prompt`]), parse the
/// D9 verdict out of the transcript, and on a parse miss re-run **once**
/// (bounded by `config.executor.verdict_retries`) with
/// [`VERDICT_CONTRACT_REMINDER`] appended. See the module doc's "Review,
/// verdict, and convergence" section for the full contract-miss/
/// verdict-storage rationale — this function is still the literal
/// translation of that section into code, now parameterized over which
/// verdict stage it drives rather than hardcoded to `Stage::Review`. T-532
/// reuses this function rather than duplicating its retry/storage logic — see
/// the module doc's own T-532 section for why a `stage` parameter was the
/// right way to generalize it (the two stages' contract-miss/verdict-storage
/// behavior is byte-for-byte identical; only the `Stage` value and the prompt
/// text differ).
///
/// `start_attempt` is the `agent_run.attempt` value this call's first try
/// opens its row at (T-531: `run_review_fix_converge` threads in each
/// round's own starting point; T-532's callers pass `0`, mirroring the
/// baseline review's own "not a retry of anything" convention). Every
/// contract-miss retry *within* this one call increments from wherever it
/// started.
#[allow(clippy::too_many_arguments)]
async fn run_verdict_stage(
    state: &AppState,
    epic_id: Option<&str>,
    task_id: &str,
    workspace: &ProvisionedWorkspace,
    stage: Stage,
    prompt: &str,
    lease: &LeaseHandle,
    start_attempt: i64,
    cfg: &crate::agent_settings::SpawnConfig,
) -> VerdictOutcome {
    let conn = state.db.conn();
    // Total *tries this call may make* = the first try + the bounded number
    // of contract-miss re-runs (default 1, never hardcoded — see the module
    // doc). Deliberately a separate counter from `attempt` below — `attempt`
    // is the absolute, task-wide `agent_run.attempt` the caller threads in via
    // `start_attempt`; `try_index` only ever counts 1, 2, … within *this*
    // call, which is what actually bounds the contract-miss retry.
    let max_tries = 1 + state.config.executor.verdict_retries as i64;
    let reminded_prompt = format!("{prompt}\n\n---\n\n{VERDICT_CONTRACT_REMINDER}");
    let stage_label = stage.as_str();

    let mut attempt: i64 = start_attempt;
    let mut try_index: i64 = 1;
    loop {
        // Same belt-and-suspenders re-check every long stretch of this walk
        // performs before spending a whole agent turn.
        if lease.is_lost() || !container_still_in_progress(conn, epic_id, task_id).await {
            tracing::warn!(
                epic = ?epic_id,
                task = %task_id,
                stage = stage_label,
                "pipeline: epic cancelled or lease lost before the verdict stage; stopping without finalizing"
            );
            return VerdictOutcome::Stop;
        }

        let this_try_prompt = if try_index == 1 {
            prompt.to_string()
        } else {
            reminded_prompt.clone()
        };

        let run_id = ulid::Ulid::new().to_string();
        let outcome = task_agent::run_agent_stage(
            state,
            &*state.task_agent,
            AgentStageParams {
                task_id: Some(task_id),
                epic_id,
                attempt,
            },
            TaskRunRequest {
                run_id,
                stage,
                prompt: this_try_prompt,
                cwd: workspace.workspace_path.clone(),
                harness: cfg.harness.clone(),
                model: cfg.model.clone(),
                prompt_hash: cfg.prompt_hash.clone(),
            },
        )
        .await;

        let outcome = match outcome {
            Ok(outcome) => outcome,
            Err(err) => {
                if !lease.is_lost() {
                    fail_item(
                        state,
                        FailureContext {
                            epic_id,
                            task_id: Some(task_id),
                            reason: FailureReason::AgentError,
                            message: &format!("{stage_label} stage failed to start: {err}"),
                            push: PushIntent::Attempt(workspace),
                        },
                    )
                    .await;
                }
                return VerdictOutcome::Stop;
            }
        };

        if !outcome.is_ok() {
            route_stage_failure(
                state,
                epic_id,
                task_id,
                &outcome,
                &format!("{stage_label} stage did not complete successfully"),
                workspace,
                lease,
                // Not the implement stage — no transient-provider
                // classification; stays `agent_error`.
                false,
            )
            .await;
            return VerdictOutcome::Stop;
        }

        if let Some(verdict) = spec::parse_verdict(&outcome.text) {
            if !lease.is_lost() {
                let _ = evidence::set_verdict(conn, &outcome.agent_run_id, verdict.as_str()).await;
                publish_stage_changed(
                    state,
                    task_id,
                    epic_id,
                    stage,
                    attempt,
                    "ok",
                    Some(verdict.as_str()),
                )
                .await;
            }
            return VerdictOutcome::Verdict {
                verdict,
                attempt,
                findings: outcome.text,
            };
        }

        // Contract miss: out of retries for *this call*?
        if try_index >= max_tries {
            tracing::warn!(
                epic = ?epic_id,
                task = %task_id,
                stage = stage_label,
                attempt,
                "verdict stage did not emit a parseable VERDICT: line after the configured retries; task -> Failed(agent_error)"
            );
            if !lease.is_lost() {
                fail_item(
                    state,
                    FailureContext {
                        epic_id,
                        task_id: Some(task_id),
                        reason: FailureReason::AgentError,
                        message: &format!(
                            "{stage_label} stage produced no parseable VERDICT: line after {max_tries} attempt(s)"
                        ),
                        push: PushIntent::Attempt(workspace),
                    },
                )
                .await;
            }
            return VerdictOutcome::Stop;
        }

        attempt += 1;
        try_index += 1;
    }
}

/// What [`run_review_fix_converge`] tells [`process_one_task`] to do next.
enum ConvergenceOutcome {
    /// `PASS` on some round — proceed to `Done` below exactly as if there
    /// were no review stage at all.
    Done,
    /// Terminal: already routed to `Failed`/`Blocked` (a `BLOCKED` verdict, a
    /// `MAX_FIX_ROUNDS` exhaustion, a review-round fix/test-gate/commit
    /// failure, or the lease/epic was already lost) — the caller's only job
    /// is to stop, with no further writes, exactly like every other failure
    /// exit in this module.
    Stop,
}

/// T-531: the review → fix → re-test → re-commit → re-review convergence
/// loop `references/ralph-v2.sh`'s `# ---- review / judge / fix loop ----`
/// reimplements. See the module doc's "Review → fix → re-test → re-commit
/// (T-531)" section for the full numbering/no-diff/reuse rationale — this
/// function is that section's literal implementation, called once per task
/// ([`process_one_task`], only once a commit exists to review — see "No
/// review for a no-diff task" above it in the module doc).
///
/// `review_prompt` is built **once** by the caller and reused, byte-for-byte,
/// on every round: it never embeds a diff itself (the D8/D9 context tells
/// the agent to run `git diff <base_sha>..HEAD` itself), so replaying the
/// exact same prompt against a tree whose `HEAD` has moved (via a fix
/// round's commit) is what "each round re-reviews the cumulative diff"
/// actually means here — `base_sha` and the prompt text never change between
/// rounds; only what `git diff` returns when the agent runs it does.
#[allow(clippy::too_many_arguments)]
async fn run_review_fix_converge(
    state: &AppState,
    epic_id: Option<&str>,
    project_id: &str,
    task_id: &str,
    task_title: &str,
    workspace: &ProvisionedWorkspace,
    review_prompt: &str,
    review_cfg: &crate::agent_settings::SpawnConfig,
    pat: Option<&str>,
    lease: &LeaseHandle,
) -> ConvergenceOutcome {
    let conn = state.db.conn();
    let max_fix_rounds = state.config.executor.max_fix_rounds;

    // `review_attempt` is the `agent_run.attempt` value fed to the *next*
    // `run_review_stage` call — 0 for the very first review (mirroring
    // T-522's `test_gate@0`: it isn't a retry or a re-review of anything).
    // `round` is a *separate* counter: the business "review round N" ralph's
    // own script names in its log lines and commit subjects, bounded by
    // `max_fix_rounds`, incrementing only on a `NEEDS_CHANGES` that earns a
    // fix — never on the baseline review. See the module doc for why these
    // two numbers are deliberately not the same counter.
    let mut review_attempt: i64 = 0;
    let mut round: u32 = 0;

    loop {
        if lease.is_lost() || !container_still_in_progress(conn, epic_id, task_id).await {
            tracing::warn!(
                epic = ?epic_id,
                task = %task_id,
                "pipeline: epic cancelled or lease lost before a review round; stopping without finalizing"
            );
            return ConvergenceOutcome::Stop;
        }

        let (verdict, used_attempt, findings) = match run_verdict_stage(
            state,
            epic_id,
            task_id,
            workspace,
            Stage::Review,
            review_prompt,
            lease,
            review_attempt,
            review_cfg,
        )
        .await
        {
            VerdictOutcome::Stop => return ConvergenceOutcome::Stop,
            VerdictOutcome::Verdict {
                verdict,
                attempt,
                findings,
            } => (verdict, attempt, findings),
        };

        match verdict {
            spec::Verdict::Pass => return ConvergenceOutcome::Done,
            spec::Verdict::Blocked => {
                if !lease.is_lost() {
                    fail_item(
                        state,
                        FailureContext {
                            epic_id,
                            task_id: Some(task_id),
                            reason: FailureReason::Blocked,
                            message: "reviewer returned BLOCKED — needs a human to resolve",
                            push: PushIntent::Attempt(workspace),
                        },
                    )
                    .await;
                }
                return ConvergenceOutcome::Stop;
            }
            spec::Verdict::NeedsChanges => {
                round += 1;
                if round > max_fix_rounds {
                    tracing::warn!(
                        epic = ?epic_id,
                        task = %task_id,
                        max_fix_rounds,
                        "review did not converge after the configured fix rounds; task -> Failed(review_not_converged)"
                    );
                    if !lease.is_lost() {
                        fail_item(
                            state,
                            FailureContext {
                                epic_id,
                                task_id: Some(task_id),
                                reason: FailureReason::ReviewNotConverged,
                                message: &format!(
                                    "review still NEEDS_CHANGES after {max_fix_rounds} fix round(s)"
                                ),
                                push: PushIntent::Attempt(workspace),
                            },
                        )
                        .await;
                    }
                    return ConvergenceOutcome::Stop;
                }

                if lease.is_lost() || !container_still_in_progress(conn, epic_id, task_id).await {
                    tracing::warn!(
                        epic = ?epic_id,
                        task = %task_id,
                        "pipeline: epic cancelled or lease lost before the review-round fix; stopping without finalizing"
                    );
                    return ConvergenceOutcome::Stop;
                }

                // The fix shares its attempt number with the review that
                // produced its feedback — T-522's "a fix and the gate that
                // follows it share a number", with review standing in for
                // gate. D19: the fix agent's only context is the fix
                // instructions + this round's findings — never the
                // spec/epic/sibling context Implement gets; see
                // `assemble_fix_prompt`'s doc (shared with T-522) for the
                // full rationale and its open concern. The instruction text
                // re-resolves live each round (T6/design §9).
                let fix_attempt = used_attempt + 1;
                let fix_cfg = match resolve_or_fail(
                    state,
                    epic_id,
                    project_id,
                    task_id,
                    Stage::Fix,
                    workspace,
                    lease,
                )
                .await
                {
                    Ok(cfg) => cfg,
                    Err(()) => return ConvergenceOutcome::Stop,
                };
                let fix_prompt = task_agent::assemble_fix_prompt_text(&fix_cfg.prompt, &findings);
                let run_id = ulid::Ulid::new().to_string();
                let fix_outcome = task_agent::run_agent_stage(
                    state,
                    &*state.task_agent,
                    AgentStageParams {
                        task_id: Some(task_id),
                        epic_id,
                        attempt: fix_attempt,
                    },
                    TaskRunRequest {
                        run_id,
                        stage: Stage::Fix,
                        prompt: fix_prompt,
                        cwd: workspace.workspace_path.clone(),
                        harness: fix_cfg.harness,
                        model: fix_cfg.model,
                        prompt_hash: fix_cfg.prompt_hash,
                    },
                )
                .await;

                match fix_outcome {
                    Ok(outcome) if outcome.is_ok() => {}
                    Ok(outcome) => {
                        route_stage_failure(
                            state,
                            epic_id,
                            task_id,
                            &outcome,
                            "review-round fix stage did not complete successfully",
                            workspace,
                            lease,
                            // Not the implement stage — no transient-provider
                            // classification; stays `agent_error`.
                            false,
                        )
                        .await;
                        return ConvergenceOutcome::Stop;
                    }
                    Err(err) => {
                        if !lease.is_lost() {
                            fail_item(
                                state,
                                FailureContext {
                                    epic_id,
                                    task_id: Some(task_id),
                                    reason: FailureReason::AgentError,
                                    message: &format!(
                                        "review-round fix stage failed to start: {err}"
                                    ),
                                    push: PushIntent::Attempt(workspace),
                                },
                            )
                            .await;
                        }
                        return ConvergenceOutcome::Stop;
                    }
                }

                if lease.is_lost() || !container_still_in_progress(conn, epic_id, task_id).await {
                    tracing::warn!(
                        epic = ?epic_id,
                        task = %task_id,
                        "pipeline: epic cancelled or lease lost before the post-fix test gate; stopping without finalizing"
                    );
                    return ConvergenceOutcome::Stop;
                }

                // Re-run the test gate (T-522, reused unmodified — see the
                // module doc for why this is deliberately not duplicated): a
                // review-driven fix that breaks the tests must never reach
                // the commit below. `run_test_gate_loop` performs its own
                // belt-and-suspenders lease/epic checks and its own bounded
                // test-driven fix retries; a red gate that never recovers
                // already routes the task to `Failed(test_gate_exhausted)`
                // and the epic to `Blocked` from inside that call — this
                // loop's only job on `GateOutcome::Stop` is to stop.
                match run_test_gate_loop(state, epic_id, project_id, task_id, workspace, pat, lease)
                    .await
                {
                    GateOutcome::Proceed => {}
                    GateOutcome::Stop => return ConvergenceOutcome::Stop,
                }

                if lease.is_lost() || !container_still_in_progress(conn, epic_id, task_id).await {
                    tracing::warn!(
                        epic = ?epic_id,
                        task = %task_id,
                        "pipeline: epic cancelled or lease lost before the review-round commit; stopping without finalizing"
                    );
                    return ConvergenceOutcome::Stop;
                }

                // §2.8's frozen commit subject. A round whose fix produced no
                // diff at all commits nothing ([`commit_if_dirty`] — see the
                // module doc's "a fix round with no diff" section); `round`
                // has already advanced above regardless, which is what
                // actually guarantees this loop terminates within
                // `max_fix_rounds` even when every fix round is a no-op.
                let subject = format!(
                    "fix({}) review round {}: {}",
                    spec::short_id(task_id),
                    round,
                    task_title
                );
                if let Err(err) = commit_if_dirty(
                    conn,
                    task_id,
                    epic_id,
                    &workspace.workspace_path,
                    &subject,
                    1 + round as i64,
                )
                .await
                {
                    if !lease.is_lost() {
                        fail_item(
                            state,
                            FailureContext {
                                epic_id,
                                task_id: Some(task_id),
                                reason: FailureReason::AgentError,
                                message: &format!(
                                    "git commit failed (review round {round}): {err}"
                                ),
                                push: PushIntent::Attempt(workspace),
                            },
                        )
                        .await;
                    }
                    return ConvergenceOutcome::Stop;
                }

                // Loop back: re-review against the SAME base_sha (the prompt
                // never changes — see this function's own doc) at the
                // attempt the fix just used, so the fix and the review that
                // follows it read as a pair in `GET /tasks/{id}/runs`.
                review_attempt = fix_attempt;
            }
        }
    }
}

// ---- Already-complete verification (T-532) --------------------------------

/// T-532: `process_one_task`'s no-diff branch — the implement stage judged
/// this task already satisfied by earlier work, so before closing it with
/// zero commits, an independent `Ask`-mode agent checks that claim against
/// the task's own acceptance criteria (`prompts/verify_complete.md`, T-502).
/// See the module doc's "Already-complete verification (T-532)" section for
/// the full rationale; this function is that section's literal
/// implementation.
///
/// Reuses [`run_verdict_stage`] (generalized from T-530's `run_review_stage`
/// by this task, see that function's own doc) for the verdict/contract-miss
/// machinery — nothing here re-implements retry bounding or verdict storage.
/// `PASS` closes the task with zero commits ([`TaskStepOutcome::Continue`],
/// caller proceeds straight to `Done`). `BLOCKED` fails the task
/// `Failed(blocked)`, identical to a `BLOCKED` review verdict. `NEEDS_CHANGES`
/// is the interesting case: MILESTONE_2 §6 says "route findings to `Fix` and
/// **re-enter the normal pipeline**" — not "build a second one" — so this
/// runs exactly one `Stage::Fix` off the verifier's findings (D19: the fix's
/// only context) and then calls the *same* [`run_test_gate_loop`] and
/// [`commit_if_dirty`] helpers [`process_one_task`]'s own step 4/5 call, with
/// the identical `impl(...)` subject (this fix's diff **is** the task's first
/// real commit — nothing landed before it). If that produces a commit, this
/// falls straight into [`run_review_fix_converge`] — the ordinary T-530/T-531
/// review loop, unmodified — exactly as if `Stage::Implement` itself had
/// written the diff. No parallel pipeline exists anywhere in this function;
/// every non-trivial step is a call into a helper `Stage::Implement`'s own
/// path already uses.
#[allow(clippy::too_many_arguments)]
async fn run_verify_complete(
    state: &AppState,
    epic_id: Option<&str>,
    project_id: &str,
    task_id: &str,
    task_title: &str,
    workspace: &ProvisionedWorkspace,
    verify_prompt: &str,
    verify_cfg: &crate::agent_settings::SpawnConfig,
    task_ctx: TaskContext<'_>,
    base_sha: &str,
    pat: Option<&str>,
    lease: &LeaseHandle,
) -> TaskStepOutcome {
    let conn = state.db.conn();

    // `start_attempt = 0`: mirrors the baseline review's own "not a retry of
    // anything" convention (T-531's module-doc section) — this is the first
    // and only verify-complete call this task will ever make.
    let (verdict, used_attempt, findings) = match run_verdict_stage(
        state,
        epic_id,
        task_id,
        workspace,
        Stage::VerifyComplete,
        verify_prompt,
        lease,
        0,
        verify_cfg,
    )
    .await
    {
        VerdictOutcome::Stop => return TaskStepOutcome::Stop,
        VerdictOutcome::Verdict {
            verdict,
            attempt,
            findings,
        } => (verdict, attempt, findings),
    };

    match verdict {
        spec::Verdict::Pass => {
            // The headline AC: PASS closes the task with zero commits. There
            // is nothing left to do here — the caller proceeds straight to
            // `Done`.
            TaskStepOutcome::Continue
        }
        spec::Verdict::Blocked => {
            if !lease.is_lost() {
                fail_item(
                    state,
                    FailureContext {
                        epic_id,
                        task_id: Some(task_id),
                        reason: FailureReason::Blocked,
                        message:
                            "verify-complete verifier returned BLOCKED — needs a human to resolve",
                        push: PushIntent::Attempt(workspace),
                    },
                )
                .await;
            }
            TaskStepOutcome::Stop
        }
        spec::Verdict::NeedsChanges => {
            if lease.is_lost() || !container_still_in_progress(conn, epic_id, task_id).await {
                tracing::warn!(
                    epic = ?epic_id,
                    task = %task_id,
                    "pipeline: epic cancelled or lease lost before the verify-complete fix; stopping without finalizing"
                );
                return TaskStepOutcome::Stop;
            }

            // The fix shares its attempt number with the verify-complete call
            // that produced its feedback — the identical "a fix and the
            // [verdict stage] that follows it share a number" convention
            // T-522/T-531 already established, with verify_complete standing
            // in for test_gate/review. D19: the fix agent's only context is
            // the fix instructions + the verifier's findings — never the
            // spec/epic/sibling context Implement gets. The instruction text
            // re-resolves live each round (T6/design §9).
            let fix_attempt = used_attempt + 1;
            let fix_cfg = match resolve_or_fail(
                state,
                epic_id,
                project_id,
                task_id,
                Stage::Fix,
                workspace,
                lease,
            )
            .await
            {
                Ok(cfg) => cfg,
                Err(()) => return TaskStepOutcome::Stop,
            };
            let fix_prompt = task_agent::assemble_fix_prompt_text(&fix_cfg.prompt, &findings);
            let run_id = ulid::Ulid::new().to_string();
            let fix_outcome = task_agent::run_agent_stage(
                state,
                &*state.task_agent,
                AgentStageParams {
                    task_id: Some(task_id),
                    epic_id,
                    attempt: fix_attempt,
                },
                TaskRunRequest {
                    run_id,
                    stage: Stage::Fix,
                    prompt: fix_prompt,
                    cwd: workspace.workspace_path.clone(),
                    harness: fix_cfg.harness,
                    model: fix_cfg.model,
                    prompt_hash: fix_cfg.prompt_hash,
                },
            )
            .await;

            match fix_outcome {
                Ok(outcome) if outcome.is_ok() => {}
                Ok(outcome) => {
                    route_stage_failure(
                        state,
                        epic_id,
                        task_id,
                        &outcome,
                        "verify-complete fix stage did not complete successfully",
                        workspace,
                        lease,
                        // Not the implement stage — no transient-provider
                        // classification; stays `agent_error`.
                        false,
                    )
                    .await;
                    return TaskStepOutcome::Stop;
                }
                Err(err) => {
                    if !lease.is_lost() {
                        fail_item(
                            state,
                            FailureContext {
                                epic_id,
                                task_id: Some(task_id),
                                reason: FailureReason::AgentError,
                                message: &format!(
                                    "verify-complete fix stage failed to start: {err}"
                                ),
                                push: PushIntent::Attempt(workspace),
                            },
                        )
                        .await;
                    }
                    return TaskStepOutcome::Stop;
                }
            }

            if lease.is_lost() || !container_still_in_progress(conn, epic_id, task_id).await {
                tracing::warn!(
                    epic = ?epic_id,
                    task = %task_id,
                    "pipeline: epic cancelled or lease lost before the post-verify-complete test gate; stopping without finalizing"
                );
                return TaskStepOutcome::Stop;
            }

            // Re-enter the normal pipeline (§6's own words): the identical
            // T-522 test gate `Stage::Implement`'s own path runs, reused
            // unmodified.
            match run_test_gate_loop(state, epic_id, project_id, task_id, workspace, pat, lease)
                .await
            {
                GateOutcome::Proceed => {}
                GateOutcome::Stop => return TaskStepOutcome::Stop,
            }

            if lease.is_lost() || !container_still_in_progress(conn, epic_id, task_id).await {
                tracing::warn!(
                    epic = ?epic_id,
                    task = %task_id,
                    "pipeline: epic cancelled or lease lost before the verify-complete-driven commit; stopping without finalizing"
                );
                return TaskStepOutcome::Stop;
            }

            // The identical §2.8 `impl(...)` subject T-513's own first commit
            // uses — this commit **is** the task's first real commit, so it
            // reads in `git log` exactly as if `Stage::Implement` had
            // produced this diff directly, not as some secondary "fix"
            // commit. `commit_attempt = 1` for the same reason: nothing has
            // been committed for this task before now.
            let subject = format!("impl({}): {}", spec::short_id(task_id), task_title);
            let committed = match commit_if_dirty(
                conn,
                task_id,
                epic_id,
                &workspace.workspace_path,
                &subject,
                1,
            )
            .await
            {
                Ok(committed) => committed,
                Err(err) => {
                    if !lease.is_lost() {
                        fail_item(
                            state,
                            FailureContext {
                                epic_id,
                                task_id: Some(task_id),
                                reason: FailureReason::AgentError,
                                message: &format!("git commit failed (verify-complete fix): {err}"),
                                push: PushIntent::Attempt(workspace),
                            },
                        )
                        .await;
                    }
                    return TaskStepOutcome::Stop;
                }
            };

            let Some(_sha) = committed else {
                // Edge case: the fix agent disagreed with, or declined to
                // act on, the verifier's own NEEDS_CHANGES findings and
                // produced no diff (see
                // `verify_complete_needs_changes_with_a_no_op_fix_fails_rather_than_closing_done`
                // in this module's own tests). Looping back into a second
                // `Stage::VerifyComplete` call here would risk an unbounded
                // ping-pong between the two verdict-emitting stages (nothing
                // bounds it the way `MAX_FIX_ROUNDS` bounds the review loop);
                // silently proceeding to `Done` would close a task the
                // verifier just said was NOT complete, with zero evidence of
                // why. Failing here is the conservative choice: a human sees
                // exactly what happened (NEEDS_CHANGES verdict, then a no-op
                // fix) in the retained `agent_run` rows and decides how to
                // proceed — via T-541's retry once that lands. Still worth a
                // team lead's second look: this is a judgment call, not
                // something MILESTONE_2 §6 specifies directly.
                if !lease.is_lost() {
                    fail_item(
                        state,
                        FailureContext {
                            epic_id,
                            task_id: Some(task_id),
                            reason: FailureReason::AgentError,
                            message: "verify-complete verdict was NEEDS_CHANGES but the fix stage produced no changes",
                            push: PushIntent::Attempt(workspace),
                        },
                    )
                    .await;
                }
                return TaskStepOutcome::Stop;
            };

            if lease.is_lost() || !container_still_in_progress(conn, epic_id, task_id).await {
                tracing::warn!(
                    epic = ?epic_id,
                    task = %task_id,
                    "pipeline: epic cancelled or lease lost before the post-verify-complete review; stopping without finalizing"
                );
                return TaskStepOutcome::Stop;
            }

            // From here on this is byte-for-byte "an ordinary implemented
            // task": build the Review context/prompt exactly as
            // `process_one_task`'s own step 5b does, and hand off to the
            // unmodified T-530/T-531 convergence loop. The Review slot's
            // config resolves live here too (T6).
            let review_ctx = TaskContext {
                base_sha: Some(base_sha),
                ..task_ctx
            };
            let review_cfg = match resolve_or_fail(
                state,
                epic_id,
                project_id,
                task_id,
                Stage::Review,
                workspace,
                lease,
            )
            .await
            {
                Ok(cfg) => cfg,
                Err(()) => return TaskStepOutcome::Stop,
            };
            let review_prompt = task_agent::assemble_prompt_text(&review_cfg.prompt, &review_ctx);

            match run_review_fix_converge(
                state,
                epic_id,
                project_id,
                task_id,
                task_title,
                workspace,
                &review_prompt,
                &review_cfg,
                pat,
                lease,
            )
            .await
            {
                ConvergenceOutcome::Done => TaskStepOutcome::Continue,
                ConvergenceOutcome::Stop => TaskStepOutcome::Stop,
            }
        }
    }
}

/// The §2.6 `stage_changed` frame: `{ task_id, stage, attempt, status,
/// verdict? }`, published on `task:<id>` and — coarse, same payload — on
/// `epic:<id>` when the task belongs to one. See the module doc's
/// "`stage_changed`, and why it's a shared helper" section for why this is
/// one function rather than two inlined `hub.publish` calls at each call
/// site.
async fn publish_stage_changed(
    state: &AppState,
    task_id: &str,
    epic_id: Option<&str>,
    stage: Stage,
    attempt: i64,
    status: &str,
    verdict: Option<&str>,
) {
    let payload = serde_json::json!({
        "task_id": task_id,
        "stage": stage.as_str(),
        "attempt": attempt,
        "status": status,
        "verdict": verdict,
    });
    state
        .hub
        .publish(&format!("task:{task_id}"), "stage_changed", payload.clone());
    if let Some(epic_id) = epic_id {
        state
            .hub
            .publish(&format!("epic:{epic_id}"), "stage_changed", payload);
    }
}

// ---- T-540: structured failure & Blocked -----------------------------------
//
// See the module doc's own "T-540: structured failure & Blocked" section for
// the full design rationale. [`FailureReason`] is the §2.3 vocabulary made a
// type (mirroring [`Stage::as_str`]); [`fail_item`] is the single router
// every failure path in this module now calls through, replacing the
// scattered `fail_task_and_block_epic`/`block_epic_on_agent_error`/
// `block_epic_on_provision_failure`/`block_epic_on_pr_failure`/
// `set_epic_blocked` cluster T-513/T-522 left behind (see those functions'
// own now-deleted doc comments, quoted in the module doc section, for
// exactly what inconsistency this replaces).

/// The full §2.3 failure-reason vocabulary, typed rather than left as bare
/// string literals scattered across call sites (mirrors [`Stage::as_str`]).
/// `Timeout` (T-543) and `Cancelled` (T-542) both exist because [`fail_item`]
/// was built to accept the whole vocabulary up front, not just the reasons
/// its first caller needed — but only `Timeout` is actually ever constructed,
/// by [`route_stage_failure`] (see that function's own doc for why a timeout
/// takes the same route as any other agent-stage failure). `Cancelled` stays
/// permanently unconstructed by design; see [`FailureReason::Cancelled`]'s
/// own doc.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FailureReason {
    /// `test_cmd` was not green on the untouched tree — including a timeout;
    /// see [`run_preflight`]'s own doc for why a slow `test_cmd` still
    /// collapses into this reason rather than [`FailureReason::Timeout`].
    /// No task at fault: the DAG walk never even started.
    PreflightRed,
    /// `setup_cmd` exited non-zero (or failed to spawn). No task at fault —
    /// provisioning failed before any task's implement stage could run.
    SetupFailed,
    /// Any other provisioning failure: a git/filesystem error, or a project
    /// whose clone isn't `ready`. No task at fault.
    WorkspaceError,
    /// A task's test gate never went green within
    /// `DEARBORN_MAX_TEST_FIX_ATTEMPTS` (T-522).
    TestGateExhausted,
    /// A task's review never converged within `DEARBORN_MAX_FIX_ROUNDS`
    /// (T-531).
    ReviewNotConverged,
    /// A review or verify-complete verdict was `BLOCKED` — the agent itself
    /// asked for a human (T-530/T-532).
    Blocked,
    /// The coarse catch-all MILESTONE_2 §4 names for Phase 1: an agent stage
    /// that never started, exited non-`ok`, or never produced a parseable
    /// `VERDICT:` line after its bounded retry, or a `git`-level failure
    /// (`rev-parse`/`add`/`commit`) around one. Still the right label for
    /// all of these post-T-540 — they are "the agent/git step itself failed
    /// in a way nothing more specific names," not a distinct §2.3 reason of
    /// their own.
    AgentError,
    /// An agent stage was killed for exceeding
    /// `DEARBORN_AGENT_STAGE_TIMEOUT_SECS` (T-543) — constructed by
    /// [`route_stage_failure`] when [`task_agent::AgentStageOutcome::timed_out`]
    /// is set, and routed through [`fail_item`] exactly like
    /// [`FailureReason::AgentError`]: a timed-out implement fails the task
    /// and blocks the epic exactly as a failed implement does (this task's
    /// AC — "an implement timeout follows the ordinary failure route, not a
    /// special one").
    Timeout,
    /// §2.3 names `cancelled` as a valid `task.failure_reason`/
    /// `epic.blocked_reason` value, so this variant stays defined and
    /// [`fail_item`] can genuinely express it — but T-542 (the cancel
    /// registry) deliberately never constructs it: `fail_item`'s task write
    /// is unconditionally `Failed`, which is exactly wrong for a cancelled
    /// task (it returns to `Todo` instead, and the epic is already
    /// `Cancelled`, never `Blocked`, by the time a cancel is observed). See
    /// the module doc's "T-542: cancellation as a kill" section and
    /// [`handle_cancelled_task`], the actual (separate, `fail_item`-free)
    /// path a cancel takes.
    #[allow(dead_code)] // see the doc above — genuinely never constructed by design
    Cancelled,
    /// The epic's branch failed to push, or `open_pr` failed, after every
    /// task had already reached `Done` (T-514's finalize step). No task at
    /// fault — every task already succeeded; the failure is finalize's own.
    PrFailed,
    /// Recommendation 5's finer taxonomy for an implement-stage failure whose
    /// recorded error text matched [`is_transient_provider_error`] (an HTTP
    /// 429 rate limit, a provider-overload notice, a gateway-level 5xx) after
    /// the bounded retry loop had exhausted its extra attempts. This is the
    /// incident that motivated the taxonomy: pi *recovered* from a mid-run 429
    /// and finished the whole fix, but a residual error event failed the stage,
    /// and the generic `agent_error` label told the triager nothing about the
    /// upstream hiccup that actually caused it. Only the implement stage opts
    /// into this classification (see [`route_stage_failure`]'s own doc); every
    /// other agent-stage failure keeps [`FailureReason::AgentError`].
    ProviderRateLimited,
}

impl FailureReason {
    /// The exact `task.failure_reason` / `epic.blocked_reason` string
    /// (§2.3), frozen exactly like [`Stage::as_str`] — every persisted row
    /// and every published frame reads this same value back out of the
    /// database, so it must never drift from the milestone doc's list.
    fn as_str(self) -> &'static str {
        match self {
            FailureReason::PreflightRed => "preflight_red",
            FailureReason::SetupFailed => "setup_failed",
            FailureReason::WorkspaceError => "workspace_error",
            FailureReason::TestGateExhausted => "test_gate_exhausted",
            FailureReason::ReviewNotConverged => "review_not_converged",
            FailureReason::Blocked => "blocked",
            FailureReason::AgentError => "agent_error",
            FailureReason::Timeout => "timeout",
            FailureReason::Cancelled => "cancelled",
            FailureReason::PrFailed => "pr_failed",
            FailureReason::ProviderRateLimited => "provider_rate_limited",
        }
    }

    /// Every §2.3 reason, exactly once — a test iterates this so "every
    /// reason reaches [`fail_item`]" is checked by the compiler (add a
    /// variant, the array below won't compile until it's listed) and by a
    /// test that actually drives each one through the router, rather than
    /// trusting the `as_str` match arms above by inspection alone.
    #[cfg(test)]
    const ALL: [FailureReason; 11] = [
        FailureReason::PreflightRed,
        FailureReason::SetupFailed,
        FailureReason::WorkspaceError,
        FailureReason::TestGateExhausted,
        FailureReason::ReviewNotConverged,
        FailureReason::Blocked,
        FailureReason::AgentError,
        FailureReason::Timeout,
        FailureReason::Cancelled,
        FailureReason::PrFailed,
        FailureReason::ProviderRateLimited,
    ];
}

/// What [`fail_item`] should do about pushing the epic branch. See the
/// module doc's "push, and where it's skipped" section for the full
/// rationale behind each variant.
enum PushIntent<'a> {
    /// Push whatever is already committed on `workspace`'s branch. Used by
    /// every failure whose call site has a real, provisioned workspace —
    /// [`push_on_failure`] never stages or commits anything itself, so a
    /// failing task's dirty tree cannot reach `origin` through this path no
    /// matter what state the workspace is in.
    Attempt(&'a ProvisionedWorkspace),
    /// Skip cleanly — no evidence row opened at all (D13: a stage that never
    /// ran gets no row), no push attempted. Two distinct reasons collapse to
    /// this one variant:
    ///
    /// - There is no [`ProvisionedWorkspace`] to push from at all
    ///   (`workspace_error`/`setup_failed`): the failure happened *inside*
    ///   [`workspace::provision_epic_workspace`], which returned `Err`
    ///   before this process ever obtained a local clone/branch — there is
    ///   structurally nothing to push, not a choice to skip something that
    ///   exists.
    /// - The caller already handled its own push (`pr_failed`):
    ///   [`finalize_epic`]'s push/`open_pr` sequence already ran, with its
    ///   own `Stage::Push` evidence row — pushing again here would be
    ///   redundant at best, and doubly so when `open_pr` (not `push`) is
    ///   what actually failed, since the branch is already on `origin`.
    Skip,
}

/// Everything [`fail_item`] needs to route one failure through the T-540
/// path. `task_id: None` is what lets a no-task failure (`preflight_red`,
/// `setup_failed`, `workspace_error`, `pr_failed`) and a task-scoped one
/// (`agent_error`, `test_gate_exhausted`, `review_not_converged`, `blocked`,
/// T-543's `timeout`, and Rec 5's `provider_rate_limited`) share the
/// identical call shape — nothing else about the struct varies by failure kind.
///
/// `epic_id: Option<&'a str>` (widened by T-551 from a bare `&'a str`) is the
/// other half of the same duality, one level up: `Some(epic_id)` is every
/// pre-T-551 call site, unchanged — an epic-scoped failure always has an
/// epic to `Block`. `None` is new: a standalone task (D17) has no epic at
/// all, so there is nothing to `Block` — see [`fail_item`]'s own doc,
/// "One container to fail, not two," for what that means for the rest of the
/// function. The two `Option`s are **not** independent in practice: a
/// standalone failure (`epic_id: None`) always carries `task_id: Some(_)` —
/// unlike an epic, where `setup_failed`/`workspace_error`/`preflight_red`/
/// `pr_failed` genuinely have no task at fault yet, a standalone task *is*
/// the item that fails, for every §2.3 reason including those four. Nothing
/// enforces that invariant at the type level (it would cost more clarity than
/// it buys for two call sites total — [`run_standalone_pipeline_inner`] and
/// the functions it calls); every standalone call site simply follows it.
struct FailureContext<'a> {
    epic_id: Option<&'a str>,
    task_id: Option<&'a str>,
    reason: FailureReason,
    /// Human-readable detail persisted as `failure_detail` (Rec 5) and logged
    /// via `tracing::warn!`. Redacted and length-capped by [`fail_item`] before
    /// it lands in the column. §2.3's structured vocabulary lives in `reason`;
    /// `message` is the free-text companion that makes a failure triageable.
    message: &'a str,
    push: PushIntent<'a>,
}

/// T-540: the single, centralized failure router every failure path in this
/// module funnels through — task → `Failed(reason)` (if there is a task at
/// fault), epic → `Blocked(reason)`, push the branch for triage, retain the
/// workspace, publish, and return so the caller's own `return`/`Stop`
/// propagates back to [`try_claim_and_run`], which releases the lease and
/// lets the worker claim its next item. See the module doc's "T-540:
/// structured failure & Blocked" section for the full design rationale; this
/// is that section's literal implementation.
///
/// 1. If `ctx.task_id` is `Some`, that task moves to `Failed` with
///    `failure_reason = ctx.reason.as_str()` — **unconditionally**, not
///    fenced on the epic's own status. This matches every pre-T-540 helper
///    it replaces: §2.3's "no sibling task ever `InProgress` concurrently"
///    invariant means no other worker is touching this task, so there is
///    nothing for the write to race against.
/// 2. The epic moves to `Blocked` with the identical reason, fenced by
///    `WHERE status = 'InProgress'` — exactly the fencing the pre-T-540
///    `set_epic_blocked` used. A race with an external Cancel (T-542) makes
///    this a no-op rather than an overwrite.
/// 3. `dag_updated` (unconditional — even a no-task failure re-renders the
///    epic's DAG view with whatever the current task statuses are) +
///    `epic_updated` + `board_updated` are published, the latter two by
///    re-fetching the epic once after both writes above.
/// 4. Iff step 2's fenced UPDATE actually took the epic (this call didn't
///    lose a race for it), [`push_on_failure`] runs per `ctx.push` —
///    best-effort and never fatal to the failure already recorded (see that
///    function's own doc). Losing the race is the same "no further writes
///    once you observe you no longer own this" discipline every other pause
///    point in this walk already follows — if something else already moved
///    the epic on, pushing on its behalf here would be guessing at intent
///    that belongs to whatever actually won the race.
///
/// ## One container to fail, not two (T-551)
///
/// Everything above describes the `ctx.epic_id: Some(epic_id)` path,
/// unchanged since T-540. `ctx.epic_id: None` (a standalone task, D17) skips
/// steps 2-4 entirely — there is no epic row to fence a `Blocked` write
/// against, no `dag_updated` (a standalone task has no DAG), and no
/// `epic_updated`. What survives is: the task's own `Failed` write (step 1,
/// identical), and — in its place — a direct fetch of the task (for
/// `project_id`, since there is no epic to fetch it from) so `board_updated`
/// still fires (the AC: "`board_updated` published on every transition") and
/// [`push_on_failure`] still runs per `ctx.push`. There is no "did this call
/// win a race" gate the way `took_epic` gates the epic branch's push: a
/// standalone task has no sibling ever concurrently touching the same row
/// (the identical invariant step 1's doc already leans on), so every
/// standalone failure call that reaches this point pushes unconditionally.
async fn fail_item(state: &AppState, ctx: FailureContext<'_>) {
    let reason = ctx.reason.as_str();
    tracing::warn!(
        epic = ?ctx.epic_id,
        task = ?ctx.task_id,
        reason,
        error = %ctx.message,
        "structured failure (T-540/T-551): task -> Failed (if any), epic -> Blocked (if any)"
    );

    let conn = state.db.conn();
    let now = now_ms();

    // Rec 5: pair the structured reason with the human-readable message.
    // The message is redacted against the owning project's PAT (and, in every
    // case, against URL userinfo) and length-capped before it ever lands in a
    // column — `failure_detail` is user-facing (it rides the task/epic DTOs
    // onto the board frames), so it obeys the same redaction discipline as
    // every other persisted failure text (`push_on_failure`, `clone_error`).
    // The project has to be resolved first so the PAT can be decrypted; an
    // epic-scoped failure reads it off the epic, a standalone one off the
    // task itself. A vanished row or a failed decrypt degrades to no PAT —
    // redaction then still strips URL userinfo, never blocks the failure
    // already being recorded.
    let conn_project_id = match ctx.epic_id {
        Some(epic_id) => fetch_epic(conn, epic_id)
            .await
            .ok()
            .flatten()
            .map(|epic| epic.project_id),
        None => match ctx.task_id {
            Some(task_id) => crate::tasks::fetch_task(conn, task_id)
                .await
                .ok()
                .flatten()
                .map(|task| task.project_id),
            None => None,
        },
    };
    let pat = match &conn_project_id {
        Some(project_id) => crate::projects::load_decrypted_pat(state, project_id)
            .await
            .ok()
            .flatten(),
        None => None,
    };
    let failure_detail = cap_failure_detail(&git::redact(ctx.message, pat.as_deref()));

    if let Some(task_id) = ctx.task_id {
        let _ = conn
            .execute(
                "UPDATE task SET status = 'Failed', failure_reason = ?1, failure_detail = ?2, \
                     updated_at = ?3 WHERE id = ?4",
                params![reason, failure_detail.clone(), now, task_id],
            )
            .await;
    }

    let Some(epic_id) = ctx.epic_id else {
        // T-551: no epic to Block — the standalone-task branch. `task_id` is
        // always `Some` here (see `FailureContext`'s own doc on the
        // invariant); fetch the task directly (there is no epic row to pull
        // `project_id` from) so the board still refreshes and a real push
        // can still run.
        let Some(task_id) = ctx.task_id else {
            tracing::error!(
                "fail_item: epic_id is None but task_id is also None — nothing to fail"
            );
            return;
        };
        if let Ok(Some(task)) = crate::tasks::fetch_task(conn, task_id).await {
            board::publish_board(state, &task.project_id).await;
            if let PushIntent::Attempt(workspace) = ctx.push {
                push_on_failure(state, None, Some(task_id), &task.project_id, workspace).await;
            }
        }
        return;
    };

    capability::publish_dag(state, epic_id).await;

    let took_epic = conn
        .execute(
            "UPDATE epic SET status = 'Blocked', blocked_reason = ?1, failure_detail = ?2, \
                 updated_at = ?3 \
             WHERE id = ?4 AND status = 'InProgress'",
            params![reason, failure_detail, now, epic_id],
        )
        .await
        .map(|n| n > 0)
        .unwrap_or(false);

    let project_id = match fetch_epic(conn, epic_id).await {
        Ok(Some(updated)) => {
            let payload = serde_json::to_value(&updated).unwrap_or(serde_json::Value::Null);
            state
                .hub
                .publish(&format!("epic:{epic_id}"), "epic_updated", payload);
            board::publish_board(state, &updated.project_id).await;
            Some(updated.project_id)
        }
        _ => None,
    };

    if took_epic {
        if let (PushIntent::Attempt(workspace), Some(project_id)) = (ctx.push, project_id) {
            push_on_failure(state, Some(epic_id), None, &project_id, workspace).await;
        }
    }
}

/// Longest [`cap_failure_detail`] output, in chars: `FAILURE_DETAIL_CAP_CHARS`
/// plus the elision marker below (only ever appended together).
const FAILURE_DETAIL_CAP_CHARS: usize = 2000;

/// Rec 5's elision marker for an over-cap `failure_detail` — same shape and
/// rationale as [`crate::evidence::cap_log`]'s own marker: a triager must be
/// able to tell a truncated column from a short one at a glance.
const FAILURE_DETAIL_ELISION_MARKER: &str =
    "... [dearborn: failure_detail elided — exceeded 2000 chars] ...";

/// Cap a redacted failure message to [`FAILURE_DETAIL_CAP_CHARS`] **chars**,
/// keeping head + tail around [`FAILURE_DETAIL_ELISION_MARKER`] when it
/// doesn't fit whole — the exact discipline [`crate::evidence::cap_log`] uses
/// for transcripts, scaled down for one message: both ends of a failure text
/// are informative (the operation that failed opens it; the actual error
/// line usually closes it), while the middle of a long provider transcript
/// is the likeliest place for repetitive noise. UTF-8 safe by construction:
/// every slice comes from `char`-iterator takes/skips, never byte offsets.
fn cap_failure_detail(text: &str) -> String {
    let total = text.chars().count();
    if total <= FAILURE_DETAIL_CAP_CHARS {
        return text.to_string();
    }
    let budget =
        FAILURE_DETAIL_CAP_CHARS.saturating_sub(FAILURE_DETAIL_ELISION_MARKER.chars().count());
    let head_len = budget / 2;
    let tail_len = budget - head_len;
    let head: String = text.chars().take(head_len).collect();
    let tail: String = text.chars().skip(total - tail_len).collect();
    format!("{head}{FAILURE_DETAIL_ELISION_MARKER}{tail}")
}

/// Best-effort push of `workspace`'s branch, the last thing [`fail_item`]
/// does — the task/epic have already reached their terminal states by the
/// time this runs, so nothing here can change *whether* the failure landed,
/// only whether a human can `git clone`/`fetch` the branch to see it (§7:
/// "on Blocked, push the epic branch to the remote so the user clones &
/// triages locally"). A failure here is recorded (`Stage::Push`, redacted)
/// and logged, never propagated — there is no code path back to
/// [`fail_item`] that could overwrite `ctx.reason` with `pr_failed`, which is
/// exactly the AC ("the epic still reaches `Blocked(<original reason>)`, not
/// `pr_failed`").
///
/// Never stages or commits anything: [`GitHost::push`] (in production,
/// [`git::push_branch`]) only ever pushes what is already committed on
/// `workspace`'s branch. Whatever commits exist when it runs were made
/// explicitly upstream — including, on an ordinary implement failure, the
/// deliberate salvage [`commit_if_dirty`] that runs just before
/// [`route_stage_failure`] hands control here (see the module doc's
/// "Salvaging completed-but-uncommitted work" section): that commit is meant
/// to ride this very push so triage can see it. Raw working-tree state is
/// still never swept up by this function — nothing here stages anything —
/// and every other failure path still has nothing between its last commit
/// and its failure, so "never staged, committed, or pushed" holds for
/// everything except that one intentional salvage commit, which is committed
/// on purpose, not swept up.
async fn push_on_failure(
    state: &AppState,
    epic_id: Option<&str>,
    task_id: Option<&str>,
    project_id: &str,
    workspace: &ProvisionedWorkspace,
) {
    let conn = state.db.conn();
    let project = match load_project_for_finalize(conn, project_id).await {
        Ok(Some(project)) => project,
        Ok(None) => {
            tracing::warn!(epic = ?epic_id, task = ?task_id, "failure push: project vanished; skipping push");
            return;
        }
        Err(err) => {
            tracing::warn!(
                epic = ?epic_id,
                task = ?task_id,
                error = %err,
                "failure push: failed to load project; skipping push"
            );
            return;
        }
    };
    let pat = crate::projects::load_decrypted_pat(state, project_id)
        .await
        .ok()
        .flatten();

    let open = OpenStage {
        task_id,
        epic_id,
        stage: Stage::Push.as_str(),
        attempt: 1,
        harness: None,
        model: None,
        prompt_hash: None,
    };
    let stage_handle = evidence::open_stage(conn, open).await.ok();

    let push_result = state
        .git_host
        .push(PushRequest {
            workspace_path: &workspace.workspace_path,
            branch: &workspace.branch_name,
            repo_url: &project.repo_url,
            pat: pat.as_deref(),
        })
        .await;

    match push_result {
        Ok(()) => {
            close_push_stage(
                conn,
                &stage_handle,
                "ok",
                &format!(
                    "pushed {} to origin (failure triage push)",
                    workspace.branch_name
                ),
            )
            .await;
        }
        Err(err) => {
            let message = git::redact(&err.message, pat.as_deref());
            tracing::warn!(
                epic = ?epic_id,
                task = ?task_id,
                error = %message,
                "failure push: push failed; non-fatal — the task/epic already reached their Failed/Blocked states"
            );
            close_push_stage(
                conn,
                &stage_handle,
                "error",
                &format!("push failed: {message}"),
            )
            .await;
        }
    }
}

// ---- T-542: cancellation as a kill -----------------------------------------
//
// See the module doc's own "T-542: cancellation as a kill" section for the
// full design (the registry, the guard, why `fail_item` doesn't fit). This
// is that section's literal implementation.

/// Whether a failed agent-stage run's recorded error text matches a
/// *transient upstream provider* signal — the class of condition that says
/// "the provider hiccuped, not that the task's work is broken": an HTTP 429
/// rate limit, a provider-overload notice, or a gateway-level 5xx. Matching
/// is case-insensitive substring matching over the needles below, because
/// these messages arrive as freeform harness/provider error lines (e.g.
/// `Error: API returned 429 Too Many Requests`), never as structured codes.
///
/// Deliberately narrow: this predicate earns [`process_one_task`]'s implement
/// stage one extra attempt (`DEARBORN_IMPLEMENT_TRANSIENT_RETRIES`), so it
/// must not match ordinary failure text — a compile error, a red test, or a
/// non-zero exit with no provider signal routes to [`route_stage_failure`] on
/// the first attempt exactly as before. Some needles are subsumed by broader
/// ones (`"temporarily rate-limited"` by `"rate-limited"`, `"502"` by itself
/// inside any larger number containing it) — they are listed individually for
/// traceability to the recommendation's match set, and substring matching is
/// accepted as slightly over-eager here because a false positive costs only
/// one bounded retry of a stage whose failure text mentioned a provider
/// status line somewhere in its output.
fn is_transient_provider_error(text: &str) -> bool {
    const TRANSIENT_SIGNALS: &[&str] = &[
        "429",
        "rate-limited",
        "rate limit",
        "temporarily rate-limited",
        "overloaded",
        "502",
        "503",
        "504",
        "bad gateway",
        "service unavailable",
    ];
    let lowered = text.to_ascii_lowercase();
    TRANSIENT_SIGNALS
        .iter()
        .any(|needle| lowered.contains(needle))
}

/// Route a not-`ok` [`task_agent::AgentStageOutcome`] to whichever of three
/// paths matches *why* it isn't `ok` — the single decision every call site in
/// this module that inspects an agent stage's outcome now makes through this
/// function instead of calling `fail_item` inline:
///
/// 1. **`outcome.timed_out`** (T-543) — `DEARBORN_AGENT_STAGE_TIMEOUT_SECS`
///    fired. Routed through T-540's [`fail_item`] with
///    [`FailureReason::Timeout`], **exactly** the same route
///    `outcome.errored`/a non-zero exit takes below — this task's AC is
///    explicit that a timeout is "that stage's ordinary failure," not a
///    special case, so the only thing that changes relative to the
///    `AgentError` branch is which reason string lands in the column. Checked
///    *before* `outcome.cancelled` because a deadline-killed outcome has
///    `cancelled: true` too (see [`task_agent::AgentStageOutcome::timed_out`]'s
///    own doc for why one flag alone can't distinguish the two callers of the
///    identical `RunControl::cancel()`).
/// 2. **`outcome.cancelled`** (and not timed out) — a human moved the epic
///    `InProgress → Cancelled` (T-542) and this stage was in flight. Routed
///    to [`handle_cancelled_task`], **not** `fail_item` — see the module
///    doc's "Observing the kill" section for why the two paths cannot share
///    `fail_item` unmodified (a cancelled task must land `Todo`, not
///    `Failed`).
/// 3. Anything else — an ordinary agent-stage failure (non-zero exit, an
///    `Error` event, or a stage that never produced a clean `ok`). Routed
///    through `fail_item` — with [`FailureReason::AgentError`] by default,
///    or [`FailureReason::ProviderRateLimited`] when `classify_transient_provider`
///    is set and the outcome's recorded error text matches
///    [`is_transient_provider_error`] (Rec 5's finer taxonomy; see below).
///
/// ## Rec 5: the transient-provider classification (`classify_transient_provider`)
///
/// Only the implement stage passes `true`. That stage already consults
/// [`is_transient_provider_error`] for its bounded retry loop (the same
/// predicate), so an implement failure that *still* looks transient after
/// every attempt was exhausted is precisely the incident class Rec 5 names:
/// a provider-side hiccup (HTTP 429 rate limit, overload notice, gateway
/// 5xx) that says nothing about the task's work. Those land
/// `provider_rate_limited` instead of the generic `agent_error`, with the
/// provider's own error text as the `message` (so `fail_item` persists it
/// redacted into `failure_detail`) rather than the caller's fixed label.
/// Every other stage keeps `false`: their failures stay `agent_error`,
/// exactly as before this parameter existed.
///
/// `message`/`workspace` are exactly what the caller would have passed
/// `fail_item` directly before T-542; they are simply ignored on the
/// cancelled branch (there is no `FailureContext` to build — see
/// `handle_cancelled_task`'s own, much smaller, argument list). Checking
/// `lease.is_lost()` here (rather than at each call site, as every
/// pre-existing `fail_item` call already did) keeps that fencing discipline
/// intact for all three branches with one check instead of three copies of
/// it.
async fn route_stage_failure(
    state: &AppState,
    epic_id: Option<&str>,
    task_id: &str,
    outcome: &task_agent::AgentStageOutcome,
    message: &str,
    workspace: &ProvisionedWorkspace,
    lease: &LeaseHandle,
    classify_transient_provider: bool,
) {
    if lease.is_lost() {
        return;
    }
    if outcome.timed_out {
        fail_item(
            state,
            FailureContext {
                epic_id,
                task_id: Some(task_id),
                reason: FailureReason::Timeout,
                message,
                push: PushIntent::Attempt(workspace),
            },
        )
        .await;
    } else if outcome.cancelled {
        handle_cancelled_task(state, epic_id, task_id).await;
    } else {
        // Prefer the last Error-event message (most specific), then the
        // full agent output (less specific but still real signal), then a
        // synthesized fallback when the agent produced no output at all —
        // a silent crash or startup failure where the exit code is the
        // only available diagnostic.
        let no_output_fallback: String;
        let err_text: &str = if let Some(msg) = outcome.last_error_message.as_deref() {
            msg
        } else if !outcome.text.is_empty() {
            &outcome.text
        } else {
            no_output_fallback = match outcome.exit_code {
                Some(code) => format!("{message} (exit code {code}, no output captured)"),
                None => format!("{message} (no output captured)"),
            };
            &no_output_fallback
        };
        let (reason, detail_message) =
            if classify_transient_provider && is_transient_provider_error(err_text) {
                tracing::warn!(
                    epic = ?epic_id,
                    task = %task_id,
                    error = %err_text,
                    "agent stage failed on a persistent transient-looking provider error; \
                     recording provider_rate_limited"
                );
                (
                    FailureReason::ProviderRateLimited,
                    // The provider's own error text is what makes the failure
                    // triageable — persist it (redacted by `fail_item`) instead
                    // of the caller's generic stage-failure label.
                    err_text,
                )
            } else {
                // Use the actual agent error text rather than the caller's fixed
                // label — the outcome text is what makes an AgentError triageable
                // (the fixed label "implement stage did not complete successfully"
                // tells the user nothing they couldn't already infer from the
                // blocked_reason / failure_reason column itself).
                (FailureReason::AgentError, err_text)
            };
        fail_item(
            state,
            FailureContext {
                epic_id,
                task_id: Some(task_id),
                reason,
                message: detail_message,
                push: PushIntent::Attempt(workspace),
            },
        )
        .await;
    }
}

/// T-542: an agent stage came back `Exited { cancelled: true }` — a human
/// moved the epic `InProgress → Cancelled` (`lanes::set_epic_lane`) while
/// this stage was in flight, and the `RunControl::cancel()` that transition
/// issued killed it. Unlike every other stage outcome this module handles,
/// this is **not** routed through [`fail_item`] (T-540) — see
/// [`route_stage_failure`]'s doc and the module doc's "Observing the kill"
/// section for the full reasoning; in short, `fail_item`'s task write is
/// unconditionally `Failed`, but this task's own AC requires `Todo` instead
/// (a cancelled task is resumable — it did not fail, a human just asked to
/// stop).
///
/// Resets the task straight to `Todo` (mirrors [`reset_orphaned_tasks`]'s own
/// write — the same "this task's in-flight attempt is abandoned, treat it as
/// pending again" shape, just triggered by an observed cancel instead of a
/// stale lease) and publishes `dag_updated` so a subscribed DAG editor sees
/// the card move back. Nothing else:
///
/// - **No epic write.** By the time this runs, `lanes::set_epic_lane` has
///   already committed `epic.status = 'Cancelled'` — that happened *before*
///   it ever looked in the cancel registry, let alone before this stage's
///   `RunEvent::Exited` could propagate all the way back here. Writing the
///   epic again would be redundant, and there is nothing new to decide: it
///   is already exactly `Cancelled`, never `Blocked`.
/// - **No `epic_updated`/`board_updated`.** Also already published by that
///   same handler; this function only has a task-level change to announce.
/// - **No push, no PR.** Nothing between a task's last successful commit and
///   a mid-stage cancellation ever calls `git add`/`git commit`: the
///   implement-failure salvage step (module doc, "Salvaging
///   completed-but-uncommitted work") explicitly skips `outcome.cancelled`
///   outcomes for exactly this reason — a cancelled task resets to `Todo`
///   below and must keep its resumable dirty tree — so there is nothing new
///   on the branch to push, and [`finalize_epic`] (the only place a PR ever
///   opens) is never reached — the walk stops mid-task, long before the DAG
///   could read fully `Done`.
/// - **No lease release.** Unchanged from every other stop path in this
///   module: [`try_claim_and_run`] releases the lease on every exit,
///   including this one, once this function's caller's plain `return`/`Stop`
///   propagates back up to it.
/// - **The workspace is retained.** Nothing in this function (or anywhere
///   upstream of it once a cancel is observed) deletes anything — the same
///   "never clean up on a stop path" property `fail_item` and every
///   between-stage cancel check already have.
///
/// T-551 widens `epic_id` to `Option<&str>` for the same reason every other
/// failure-adjacent function in this module was widened, but cancellation
/// itself stays out of that task's scope (see the module doc's own T-551
/// section): nothing today ever issues a `RunControl::cancel()` against a
/// standalone task's registry entry (there is no `POST /tasks/{id}/lane` or
/// equivalent cancel surface for a task — only `lanes::set_epic_lane`
/// cancels, and only epics), so `epic_id: None` reaching this function is not
/// a path any current caller can actually exercise. It is still handled
/// correctly rather than left to panic or silently do the wrong thing: the
/// task still resets to `Todo`, and the publish becomes `board_updated`
/// (there is no DAG to re-render for a standalone task) instead of
/// `dag_updated` — whichever surface a future standalone-cancel task adds
/// would need this same branch anyway.
async fn handle_cancelled_task(state: &AppState, epic_id: Option<&str>, task_id: &str) {
    tracing::info!(
        epic = ?epic_id,
        task = %task_id,
        "T-542: agent stage was cancelled; resetting task -> Todo"
    );
    let conn = state.db.conn();
    let now = now_ms();
    let _ = conn
        .execute(
            "UPDATE task SET status = 'Todo', updated_at = ?1 WHERE id = ?2",
            params![now, task_id],
        )
        .await;
    match epic_id {
        Some(epic_id) => capability::publish_dag(state, epic_id).await,
        None => {
            if let Ok(Some(task)) = crate::tasks::fetch_task(conn, task_id).await {
                board::publish_board(state, &task.project_id).await;
            }
        }
    }
}

/// What [`run_preflight`] tells its caller to do next. See the module doc's
/// "The preflight gate" section for the full rationale.
enum PreflightOutcome {
    /// Either no `test_cmd` was configured ([`StageOutcome::Skipped`] —
    /// silent, no row, per T-520's contract) or `test_cmd` ran and exited
    /// `0`. The walk proceeds exactly as if this gate did not exist.
    Proceed,
    /// `test_cmd` failed, timed out, or could not even be evaluated (a
    /// database error recording the attempt). The epic is already `Blocked`
    /// by the time this variant is returned — [`run_preflight`] performs that
    /// write itself via [`fail_item`] — so the caller's only job is to
    /// `return` immediately without ever looking at a task.
    Blocked,
}

/// T-521: run the project's `test_cmd` exactly once, in `workspace`, as the
/// `preflight` stage (§2.2) — the green-tree gate D5 keeps from
/// `references/ralph-v2.sh`. See the module doc's "The preflight gate"
/// section for why this exists, why an absent `test_cmd` is a silent no-op,
/// and — the one genuinely subjective call this task makes — why a timeout
/// is reported as `preflight_red` rather than `timeout`:
///
/// §2.3 offers both `preflight_red` and `timeout` as valid `blocked_reason`
/// values, and a `test_cmd` that runs past `DEARBORN_CMD_TIMEOUT_SECS` could
/// honestly be described either way. This function picks `preflight_red` for
/// every non-`ok` outcome — error exit *and* timeout alike — because the
/// question a human reads `blocked_reason` to answer is "why didn't my epic
/// start?", and the answer is the same in both cases: *this repository's own
/// test suite did not come back green on an untouched tree*. Reporting
/// `timeout` instead would misleadingly suggest Dearborn's own tooling got
/// stuck (the story `timeout` correctly tells for T-543's agent-stage
/// timeouts, a different kind of failure entirely) rather than that the
/// project itself has a problem to go fix. The `agent_run` row backing this
/// call still records the precise `status` (`"ok"` | `"error"` | `"timeout"`)
/// via [`cmd::run_stage_command`] — nothing about the finer-grained truth is
/// lost, only the coarse board-facing label is collapsed to one value.
///
/// T-551 widens this to `epic_id: Option<&str>` / a new `task_id: Option<&str>`
/// so a standalone task's own preflight run reaches [`fail_item`] with
/// `epic_id: None, task_id: Some(_)` (there is no epic to Block, and unlike
/// an epic's preflight — which has no task at fault yet, since the DAG walk
/// hasn't started — a standalone's one task *is* the thing that fails). The
/// evidence row's own `task_id`/`epic_id` (`StageCommand`, below) uses
/// exactly the pair the caller passes in, so a standalone's `preflight`
/// `agent_run` row is keyed by `task_id`, not `epic_id`, mirroring every
/// other stage this module runs for a standalone task.
async fn run_preflight(
    state: &AppState,
    epic_id: Option<&str>,
    task_id: Option<&str>,
    workspace: &ProvisionedWorkspace,
    pat: Option<&str>,
) -> PreflightOutcome {
    let conn = state.db.conn();
    // Boxed rather than awaited inline: `cmd::run_stage_command` transitively
    // embeds `run_shell_timed`'s own state (including its 32 KB read-chunk
    // buffer, live across an internal `tokio::select!` await) — inlining
    // that directly into `run_preflight`'s generated state machine, which is
    // itself inlined into the already-large `run_epic_pipeline_inner`, stacks
    // up enough live state to overflow the default thread stack on the test
    // harness's direct (non-`tokio::spawn`) call path. `Box::pin` moves that
    // storage to the heap so this call site contributes only a pointer's
    // worth of state to its caller, exactly like `try_claim_and_run`'s own
    // `tokio::spawn` already isolates the *whole* pipeline body's size in
    // production — this is the equivalent guard for the one nested call this
    // task adds.
    let outcome = Box::pin(cmd::run_stage_command(
        conn,
        StageCommand {
            task_id,
            epic_id,
            stage: Stage::Preflight.as_str(),
            attempt: 1,
            cwd: &workspace.workspace_path,
            timeout: Duration::from_secs(state.config.executor.cmd_timeout_secs),
        },
        workspace.test_cmd.as_deref(),
        |raw: &str| git::redact(raw, pat),
    ))
    .await;

    // T-540: preflight *does* have a real, provisioned `workspace` at this
    // point (unlike `workspace_error`/`setup_failed` above) — on a re-claim
    // it may already carry earlier tasks' committed work from a prior
    // successful claim, so `PushIntent::Attempt` (not `Skip`) is the right
    // call here even though a *first* claim's preflight failure pushes a
    // branch with nothing Dearborn-authored on it yet (harmless: the push
    // just mirrors canonical under the epic's branch name).
    match outcome {
        Ok(StageOutcome::Skipped) => PreflightOutcome::Proceed,
        Ok(StageOutcome::Ran(ran)) if ran.status == "ok" => PreflightOutcome::Proceed,
        Ok(StageOutcome::Ran(ran)) => {
            tracing::warn!(
                epic = ?epic_id,
                task = ?task_id,
                status = ran.status,
                exit_code = ?ran.exit_code,
                "preflight: test_cmd is not green on the untouched tree; -> Blocked/Failed(preflight_red)"
            );
            fail_item(
                state,
                FailureContext {
                    epic_id,
                    task_id,
                    reason: FailureReason::PreflightRed,
                    message: &format!(
                        "test_cmd not green (status={}, exit_code={:?})",
                        ran.status, ran.exit_code
                    ),
                    push: PushIntent::Attempt(workspace),
                },
            )
            .await;
            PreflightOutcome::Blocked
        }
        Err(err) => {
            tracing::warn!(
                epic = ?epic_id,
                task = ?task_id,
                error = %err,
                "preflight: failed to record test_cmd evidence; -> Blocked/Failed(preflight_red)"
            );
            fail_item(
                state,
                FailureContext {
                    epic_id,
                    task_id,
                    reason: FailureReason::PreflightRed,
                    message: &format!("failed to record test_cmd evidence: {err}"),
                    push: PushIntent::Attempt(workspace),
                },
            )
            .await;
            PreflightOutcome::Blocked
        }
    }
}

/// Finalize a fully-`Done` epic (T-514, D1): push the branch and open a PR
/// (or, on a feedback re-run, push only and reuse the recorded PR), persist
/// its identity, flip the epic to `InReview`, retain the workspace, and
/// publish. This is the **only** place `epic.status` ever becomes
/// `Completed` — see the module doc's "`Completed` only after a real PR
/// opens" section for why that transition waits this long. (In the
/// post-PR-review flow `Completed` is reached only from `InReview` on a
/// human merge — this finalize step lands in `InReview`, not `Completed`.)
///
/// A failed push or a failed `open_pr` routes the epic to
/// `Blocked(pr_failed)` (never `Completed`/`InReview`) via [`fail_item`] — the same
/// T-540 router, same workspace-retained/lease-released contract every other
/// failure path in this module already uses, called here with
/// `PushIntent::Skip` (this function *is* the push — see [`PushIntent::Skip`]'s
/// own doc for why routing `pr_failed` back through the router's own push
/// step would be redundant or a no-op) — with the readable, redacted failure
/// reason recorded in a `Stage::Push` `agent_run` row (§2.2 lists `push` as a
/// non-agent stage; this finalize step is the one place that stage's row
/// gets opened/closed). Persisting a short `blocked_reason` code on the epic
/// plus a full message in evidence mirrors exactly how `setup_failed` splits
/// reason-code vs. captured-output between the epic row and `agent_run`.
///
/// Either exit (`InReview` or `Blocked(pr_failed)`) moves the epic out of
/// `InProgress`, so [`claim_epic`]'s predicate excludes it from then on —
/// this is what closes the re-claim spin T-513 deliberately left open (its
/// module doc says so): before this function existed, a fully-`Done` epic
/// stayed `InProgress` with its lease released, so the pool would re-claim
/// and re-walk it in a tight loop forever. Now every path out of a
/// fully-`Done` DAG ends in a terminal-for-the-queue status.
///
/// ## Shared with [`finalize_task`] (T-551): [`push_and_ensure_pr`]
///
/// The project/PAT load, the single `Stage::Push` evidence row, the push
/// itself, and the `open_pr` call — every step through "the PR now exists on
/// GitHub" — is factored into [`push_and_ensure_pr`], the one place that
/// sequence is written. This function (and [`finalize_task`], the standalone
/// mirror) calls it once with its own title/body and `epic_id`/`task_id`
/// pair, then does only what's left that genuinely differs: which row's
/// checklist to build the body from, and which row's terminal write persists
/// the opened PR.
async fn finalize_epic(
    state: &AppState,
    epic_id: &str,
    epic: &crate::epics::Epic,
    dag: &crate::tasks::Dag,
    workspace: &ProvisionedWorkspace,
    lease: &LeaseHandle,
) {
    let conn = state.db.conn();

    let title = pr::epic_pr_title(&epic.title);
    let items = build_task_checklist(conn, epic_id, dag).await;
    // T-560: best-effort — see `run_epic_summary`'s own doc for why this can
    // never fail this function, and the module doc's own T-560 section for
    // why it runs *before* `push_and_ensure_pr` rather than after.
    let summary = run_epic_summary(state, epic_id, epic, dag, workspace, lease).await;
    let body = pr::build_pr_body(epic.description.as_deref(), &items, summary.as_deref());

    // A feedback re-run already has a recorded PR (set on the first
    // finalize): reuse it — push only — rather than opening a duplicate.
    let existing_pr = epic.pr_url.as_ref().and_then(|url| {
        epic.pr_number.map(|number| crate::git_host::OpenedPr {
            url: url.clone(),
            number,
        })
    });
    let first_open = existing_pr.is_none();

    let Some(opened) = push_and_ensure_pr(
        state,
        Some(epic_id),
        None,
        &epic.project_id,
        workspace,
        existing_pr.as_ref(),
        lease,
        &title,
        &body,
    )
    .await
    else {
        return;
    };

    // Re-check immediately before the terminal writes: a slow push/PR racing
    // an external cancel or a stolen lease must not overwrite whatever that
    // race already did. The PR itself cannot be un-opened at this point —
    // the fenced UPDATE below simply becomes a no-op if the epic moved on —
    // but no further Dearborn-side state changes to a no-longer-ours epic.
    if lease.is_lost() {
        return;
    }

    let now = now_ms();

    // Land the epic in `InReview` (not `Completed`) with the PR attached —
    // the PR link/number are set only on the first open (`first_open`); on a
    // feedback re-run the recorded PR is preserved (we only push). The
    // workspace is *retained* so feedback rounds can reuse the branch.
    let affected = if first_open {
        conn.execute(
            "UPDATE epic SET status = 'InReview', pr_url = ?1, pr_number = ?2, updated_at = ?3 \
             WHERE id = ?4 AND status = 'InProgress'",
            params![opened.url.clone(), opened.number, now, epic_id],
        )
        .await
    } else {
        conn.execute(
            "UPDATE epic SET status = 'InReview', updated_at = ?1 \
             WHERE id = ?2 AND status = 'InProgress'",
            params![now, epic_id],
        )
        .await
    };

    match affected {
        Ok(n) if n > 0 => {
            if let Ok(Some(updated)) = fetch_epic(conn, epic_id).await {
                let payload = serde_json::to_value(&updated).unwrap_or(serde_json::Value::Null);
                state
                    .hub
                    .publish(&format!("epic:{epic_id}"), "epic_updated", payload);
                board::publish_board(state, &updated.project_id).await;
            }
            // The workspace is deliberately retained (not deleted) so
            // feedback rounds can reuse the same branch and update the PR.
        }
        Ok(_) => {
            tracing::warn!(
                epic = %epic_id,
                "finalize: epic was no longer InProgress when persisting the opened PR; \
                 leaving DB state as-is (the PR already opened on GitHub and cannot be un-opened)"
            );
        }
        Err(err) => {
            tracing::error!(
                epic = %epic_id,
                error = %err,
                "finalize: failed to persist the opened PR; the PR exists on GitHub but Dearborn's \
                 record of it does not — a human needs to reconcile this"
            );
        }
    }
}

/// The claimed-**standalone-task** counterpart to [`finalize_epic`] (T-551):
/// push the branch, open a PR on the first finalize (or, on a feedback
/// re-run, push only and reuse the recorded PR), persist `pr_url`/
/// `pr_number` on the *task* row (there is no epic row for a standalone
/// claim), land it in `InReview`, retain the workspace, and publish. Shares
/// [`push_and_ensure_pr`]'s push/ensure-PR core with `finalize_epic` — see
/// that function's own doc — rather than duplicating it; the only things
/// that differ are the title/body construction (one task, not a checklist
/// over a DAG — [`build_standalone_checklist`]) and the terminal write
/// below.
///
/// ## `InReview`, not `Done` (post-PR-review loop)
///
/// Before the post-PR-review loop a standalone task's terminal success
/// state was its own `Done` (set by [`process_one_task`]'s step 6 while it
/// is just reporting "all tasks are done"). Now that the PR's opened,
/// `Done` would mean the factory never tracked the human review phase — a
/// standalone task has no second row, so its PR lifecycle lives on the
/// *task* row itself (epic-owned tasks keep `Done`; the epic row carries
/// `InReview`). Finalize therefore moves the standalone task to `InReview`
/// — its "factory done, waiting on the human reviewer" state — mirroring
/// the epic path. The persisting `UPDATE` is fenced on `WHERE status =
/// 'Done'` (mirroring `finalize_epic`'s own `WHERE status = 'InProgress'`
/// fence) so a race with, say, a retry landing at exactly the wrong instant
/// is a no-op rather than a clobber.
async fn finalize_task(
    state: &AppState,
    task_id: &str,
    task: &crate::tasks::Task,
    workspace: &ProvisionedWorkspace,
    lease: &LeaseHandle,
) {
    let conn = state.db.conn();

    // `pr::epic_pr_title` is a pure passthrough (`title.to_string()`, D16 —
    // "the epic's title, verbatim") with nothing epic-specific in it; reused
    // as-is for a standalone task's title rather than adding a same-bodied
    // twin.
    let title = pr::epic_pr_title(&task.title);
    let items = build_standalone_checklist(conn, task).await;
    // T-560: a standalone task gets a summary too — see the module doc's own
    // T-560 section, "standalone tasks get one too," for why this isn't
    // epic-only.
    let summary = run_task_summary(state, task_id, task, workspace, lease).await;
    let body = pr::build_pr_body(task.description.as_deref(), &items, summary.as_deref());

    // A feedback re-run already has a recorded PR: reuse it — push only.
    let existing_pr = task.pr_url.as_ref().and_then(|url| {
        task.pr_number.map(|number| crate::git_host::OpenedPr {
            url: url.clone(),
            number,
        })
    });
    let first_open = existing_pr.is_none();

    let Some(opened) = push_and_ensure_pr(
        state,
        None,
        Some(task_id),
        &task.project_id,
        workspace,
        existing_pr.as_ref(),
        lease,
        &title,
        &body,
    )
    .await
    else {
        return;
    };

    if lease.is_lost() {
        return;
    }

    let now = now_ms();

    // Land the standalone task in `InReview` (its PR lifecycle lives on the
    // task row) rather than leaving it `Done` — the PR link/number are set
    // only on the first open; a feedback re-run just pushes and returns to
    // `InReview`. The workspace is retained for feedback rounds.
    let affected = if first_open {
        conn.execute(
            "UPDATE task SET status = 'InReview', pr_url = ?1, pr_number = ?2, updated_at = ?3 \
             WHERE id = ?4 AND status = 'Done'",
            params![opened.url.clone(), opened.number, now, task_id],
        )
        .await
    } else {
        conn.execute(
            "UPDATE task SET status = 'InReview', updated_at = ?1 \
             WHERE id = ?2 AND status = 'Done'",
            params![now, task_id],
        )
        .await
    };

    match affected {
        Ok(n) if n > 0 => {
            board::publish_board(state, &task.project_id).await;
            // The workspace is deliberately retained (not deleted) so
            // feedback rounds can reuse the same branch and update the PR.
        }
        Ok(_) => {
            tracing::warn!(
                task = %task_id,
                "finalize: task was no longer Done when persisting the opened PR; \
                 leaving DB state as-is (the PR already opened on GitHub and cannot be un-opened)"
            );
        }
        Err(err) => {
            tracing::error!(
                task = %task_id,
                error = %err,
                "finalize: failed to persist the opened PR; the PR exists on GitHub but Dearborn's \
                 record of it does not — a human needs to reconcile this"
            );
        }
    }
}

/// The shared push+ensure-PR core [`finalize_epic`]/[`finalize_task`] (T-551)
/// both call: load the project + PAT, open a single `Stage::Push` evidence
/// row spanning the push (and, on the item's first finalize, the `open_pr`)
/// network operations (§2.2 has one `push` stage, no separate "open PR"
/// entry), push `workspace`'s branch, then **conditionally** open a PR
/// titled `title` with body `body`. `existing_pr` carries the item's
/// already-recorded PR (from a prior finalize) when one exists: a feedback
/// re-run always pushes — which updates the existing PR in place — and
/// returns `existing_pr` unchanged instead of calling `open_pr` again (a
/// second `open_pr` on the same head branch would create a duplicate). Only
/// when `existing_pr` is `None` (first finalize) does it open a PR. Every
/// failure routes through [`fail_item`] with [`FailureReason::PrFailed`]
/// and `PushIntent::Skip` — this function *is* the push, so routing back
/// through `fail_item`'s own push step would either push nothing new (the
/// project/PAT load failures, which happen before any push is even
/// attempted) or push a second, redundant time (the
/// push-itself-failed/open-PR-failed cases, each of which already recorded
/// its own evidence via [`close_push_stage`] immediately before the call) —
/// see [`PushIntent::Skip`]'s own doc. `epic_id`/`task_id` are threaded
/// straight into `FailureContext` and the evidence row exactly as the
/// caller's own shape dictates: `Some(epic_id), None` for an epic, `None,
/// Some(task_id)` for a standalone task. Returns the PR to persist — the
/// freshly-opened one on a first finalize, or `existing_pr` unchanged on a
/// re-run — on success; `None` on any failure (already routed to
/// `Failed`/`Blocked` by the time this returns) — the caller's only job on
/// `None` is to `return`, exactly like every other failure exit in this
/// module.
#[allow(clippy::too_many_arguments)]
async fn push_and_ensure_pr(
    state: &AppState,
    epic_id: Option<&str>,
    task_id: Option<&str>,
    project_id: &str,
    workspace: &ProvisionedWorkspace,
    existing_pr: Option<&crate::git_host::OpenedPr>,
    lease: &LeaseHandle,
    title: &str,
    body: &str,
) -> Option<crate::git_host::OpenedPr> {
    let conn = state.db.conn();

    let project = match load_project_for_finalize(conn, project_id).await {
        Ok(Some(project)) => project,
        Ok(None) => {
            if !lease.is_lost() {
                fail_item(
                    state,
                    FailureContext {
                        epic_id,
                        task_id,
                        reason: FailureReason::PrFailed,
                        message: "project vanished before finalize",
                        push: PushIntent::Skip,
                    },
                )
                .await;
            }
            return None;
        }
        Err(err) => {
            if !lease.is_lost() {
                fail_item(
                    state,
                    FailureContext {
                        epic_id,
                        task_id,
                        reason: FailureReason::PrFailed,
                        message: &format!("failed to load project for finalize: {err}"),
                        push: PushIntent::Skip,
                    },
                )
                .await;
            }
            return None;
        }
    };
    let pat = match crate::projects::load_decrypted_pat(state, project_id).await {
        Ok(pat) => pat,
        Err(err) => {
            if !lease.is_lost() {
                fail_item(
                    state,
                    FailureContext {
                        epic_id,
                        task_id,
                        reason: FailureReason::PrFailed,
                        message: &format!("failed to load project PAT for finalize: {err}"),
                        push: PushIntent::Skip,
                    },
                )
                .await;
            }
            return None;
        }
    };

    let open = OpenStage {
        task_id,
        epic_id,
        stage: Stage::Push.as_str(),
        attempt: 1,
        harness: None,
        model: None,
        prompt_hash: None,
    };
    let stage_handle = evidence::open_stage(conn, open).await.ok();

    let push_result = state
        .git_host
        .push(PushRequest {
            workspace_path: &workspace.workspace_path,
            branch: &workspace.branch_name,
            repo_url: &project.repo_url,
            pat: pat.as_deref(),
        })
        .await;

    if let Err(err) = push_result {
        let message = git::redact(&err.message, pat.as_deref());
        close_push_stage(
            conn,
            &stage_handle,
            "error",
            &format!("push failed: {message}"),
        )
        .await;
        if !lease.is_lost() {
            fail_item(
                state,
                FailureContext {
                    epic_id,
                    task_id,
                    reason: FailureReason::PrFailed,
                    message: &message,
                    push: PushIntent::Skip,
                },
            )
            .await;
        }
        return None;
    }

    // A feedback re-run already has a recorded PR: pushing the same head
    // branch is enough — GitHub updates the existing PR in place — and
    // re-opening it here would create a duplicate. Open a PR only on the
    // item's *first* finalize (`existing_pr` is `None`); the caller passes
    // the recorded PR so the pushed round-trip returns it unchanged.
    if let Some(existing) = existing_pr {
        close_push_stage(
            conn,
            &stage_handle,
            "ok",
            &format!(
                "pushed {} to origin; updated existing PR #{} ({})",
                workspace.branch_name, existing.number, existing.url
            ),
        )
        .await;
        return Some(existing.clone());
    }

    // Resolve the PR's base branch (design doc §5) *before* opening the PR,
    // so a resolution failure routes through the same
    // `pr_failed` path with its own readable evidence. Chain: the recorded
    // explicit base (the epic's provision-time snapshot, or — standalone
    // tasks having no per-item record by design — the project default), else
    // the workspace clone's own `origin/HEAD` (offline; no GitHub API call).
    let recorded_base =
        match recorded_base_branch(conn, epic_id, project.base_branch.as_deref()).await {
            Ok(base) => base,
            Err(err) => {
                if !lease.is_lost() {
                    fail_item(
                        state,
                        FailureContext {
                            epic_id,
                            task_id,
                            reason: FailureReason::PrFailed,
                            message: &format!("failed to load recorded base branch: {err}"),
                            push: PushIntent::Skip,
                        },
                    )
                    .await;
                }
                return None;
            }
        };
    let base = match recorded_base {
        Some(base) => base,
        None => match git::origin_default_branch(&workspace.workspace_path).await {
            Ok(branch) => branch,
            Err(err) => {
                if !lease.is_lost() {
                    fail_item(
                        state,
                        FailureContext {
                            epic_id,
                            task_id,
                            reason: FailureReason::PrFailed,
                            message: &format!(
                                "could not resolve the PR's base branch from the workspace \
                                 clone (no explicit base is recorded): {}",
                                err.message
                            ),
                            push: PushIntent::Skip,
                        },
                    )
                    .await;
                }
                return None;
            }
        },
    };

    tracing::info!(head = %workspace.branch_name, base = %base, "open_pr: resolved head and base");
    let open_result = state
        .git_host
        .open_pr(OpenPrRequest {
            repo_url: &project.repo_url,
            pat: pat.as_deref(),
            head: &workspace.branch_name,
            base: &base,
            title,
            body,
        })
        .await;

    let opened = match open_result {
        Ok(opened) => opened,
        Err(err) => {
            let message = git::redact(&err.message, pat.as_deref());
            close_push_stage(
                conn,
                &stage_handle,
                "error",
                &format!("open_pr failed: {message}"),
            )
            .await;
            if !lease.is_lost() {
                fail_item(
                    state,
                    FailureContext {
                        epic_id,
                        task_id,
                        reason: FailureReason::PrFailed,
                        message: &message,
                        push: PushIntent::Skip,
                    },
                )
                .await;
            }
            return None;
        }
    };

    close_push_stage(
        conn,
        &stage_handle,
        "ok",
        &format!(
            "pushed {} to origin; opened PR {} (#{})",
            workspace.branch_name, opened.url, opened.number
        ),
    )
    .await;

    Some(opened)
}

// ---- T-560: the PR-body agent summary --------------------------------------
//
// See the module doc's own "T-560" section for the full ordering/scoping
// design; the short version: [`run_summarize_stage`] is the one place a
// `Stage::Summarize` run through `run_agent_stage` and its outcome is turned
// into `Option<String>`, shared by [`run_epic_summary`] (epic-scoped) and
// [`run_task_summary`] (standalone-task-scoped) exactly the way
// [`push_and_ensure_pr`] is shared by [`finalize_epic`]/[`finalize_task`] —
// only the [`spec::TaskContext`] fed in (and how the "what's the base commit
// to diff from" question is answered) differs between the two.

/// Run the `Stage::Summarize` agent stage once against `context`, in
/// `workspace_path`, and hand back its prose — or `None` on *any* way the run
/// can come up short. This is the single choke point both
/// [`run_epic_summary`] and [`run_task_summary`] call; unlike
/// [`build_task_checklist`]/[`build_standalone_checklist`] (kept deliberately
/// separate — see that pair's own doc for why a DAG walk and a one-task
/// checklist are different enough shapes to duplicate rather than share),
/// running one agent stage and interpreting its
/// [`task_agent::AgentStageOutcome`] is *exactly* the same operation whether
/// the diff spans a whole epic or a single standalone task.
///
/// ## Never fails upward (D16, this task's AC: "the PR is never blocked on
/// the summary")
///
/// Every non-happy path collapses to `None`, never a propagated error:
/// [`task_agent::run_agent_stage`] itself erroring (the harness never
/// spawned, or its drain thread panicked); a non-`ok`
/// [`task_agent::AgentStageOutcome`] (`error`/`timeout`/`cancelled` — the
/// same [`DEARBORN_AGENT_STAGE_TIMEOUT_SECS`](crate::config::ExecutorConfig::agent_stage_timeout_secs)
/// deadline every other agent stage already runs under, D18 — see the module
/// doc's T-560 section for why this isn't given its own, tighter budget); and
/// a stage that exits `ok` but whose text is empty or whitespace-only (an
/// agent that ran cleanly and simply had nothing to add — `prompts/
/// summarize.md` asks for prose, not a verdict, so unlike `Stage::Review`/
/// `Stage::VerifyComplete` there is no contract-miss concept here to retry).
/// [`finalize_epic`]/[`finalize_task`] pass the result straight into
/// [`pr::build_pr_body`]'s own `summary: Option<&str>`, which applies the
/// identical trim-and-filter-blank treatment `epic_description` already gets
/// — belt and suspenders, not because this function is expected to hand back
/// untrimmed/blank text, but because keeping "blank means absent" in exactly
/// one place ([`pr::build_pr_body`]) for every optional text field the
/// template renders is simpler than asserting two functions agree on it
/// independently.
async fn run_summarize_stage(
    state: &AppState,
    project_id: &str,
    epic_id: Option<&str>,
    task_id: Option<&str>,
    context: &TaskContext<'_>,
    workspace_path: &std::path::Path,
) -> Option<String> {
    // Live-resolved Summarize slot config (T6): override or prompts/
    // summarize.md, read at spawn time. Every failure path below collapses
    // to `None` — the summary never blocks the PR (D16).
    let cfg = match stage_spawn_config(state, project_id, Stage::Summarize).await {
        Ok(cfg) => cfg,
        Err(err) => {
            tracing::warn!(project = %project_id, error = %err, "failed to resolve summarize agent settings; skipping summary");
            return None;
        }
    };
    let prompt = task_agent::assemble_prompt_text(&cfg.prompt, context);
    let run_id = ulid::Ulid::new().to_string();
    let outcome = task_agent::run_agent_stage(
        state,
        &*state.task_agent,
        AgentStageParams {
            task_id,
            epic_id,
            attempt: 1,
        },
        TaskRunRequest {
            run_id,
            stage: Stage::Summarize,
            prompt,
            cwd: workspace_path.to_path_buf(),
            harness: cfg.harness,
            model: cfg.model,
            prompt_hash: cfg.prompt_hash,
        },
    )
    .await;

    match outcome {
        Ok(outcome) if outcome.is_ok() => {
            let text = outcome.text.trim();
            if text.is_empty() {
                None
            } else {
                Some(text.to_string())
            }
        }
        _ => None,
    }
}

/// The commit an epic's cumulative diff should be measured from, for
/// [`run_epic_summary`]'s `git diff <sha>..HEAD` instruction (via
/// [`spec::TaskContext::base_sha`]): the `base_sha` recorded by whichever
/// task's `Stage::Implement` opened **first** (`agent_run.created_at ASC`).
/// The DAG walk fully serializes one task at a time (T-513: one task
/// `InProgress` at a time, run to a terminal state before the next begins),
/// so that task's `base_sha` — "the workspace's `HEAD` right before this
/// task's own work" (`process_one_task`'s own step 1) — is exactly the
/// workspace's `HEAD` the instant it was provisioned, before *any* task in
/// the epic touched anything; `position` order was deliberately not used
/// instead, since a DAG's topological run order need not match `position` for
/// independent branches. `None` when the epic has no `implement` rows at all
/// (an empty DAG, or one whose every task somehow failed before reaching
/// `Stage::Implement` — in practice this only happens for an epic with zero
/// tasks, since every task's first pipeline step *is* recording `base_sha`
/// and running `Stage::Implement`): nothing was ever built, so there is
/// nothing to summarize either, and [`run_epic_summary`] skips the agent
/// call entirely rather than running one with no diff to point at.
async fn epic_summary_base_sha(conn: &Connection, epic_id: &str) -> Option<String> {
    let mut rows = conn
        .query(
            "SELECT t.base_sha FROM agent_run a \
             JOIN task t ON t.id = a.task_id \
             WHERE a.epic_id = ?1 AND a.stage = 'implement' \
             ORDER BY a.created_at ASC, a.rowid ASC LIMIT 1",
            params![epic_id],
        )
        .await
        .ok()?;
    let row = rows.next().await.ok()??;
    row.get::<Option<String>>(0).ok().flatten()
}

/// The standalone-task counterpart to [`epic_summary_base_sha`]: just the
/// one task's own `base_sha` column, read directly (it is deliberately not
/// projected onto [`crate::tasks::Task`] — see that struct's own doc — so
/// this is its own small raw query rather than a field access).
async fn task_summary_base_sha(conn: &Connection, task_id: &str) -> Option<String> {
    let mut rows = conn
        .query("SELECT base_sha FROM task WHERE id = ?1", params![task_id])
        .await
        .ok()?;
    let row = rows.next().await.ok()??;
    row.get::<Option<String>>(0).ok().flatten()
}

/// [`finalize_epic`]'s T-560 summary step: build the epic-scoped
/// [`spec::TaskContext`] (the epic's own title/description standing in for
/// [`spec::SpecFields`], no epic-context section since the epic *is* the
/// context here, every task in the DAG listed as an "Already built" sibling —
/// reusing [`spec::build_context`]'s existing sibling-manifest rendering
/// gives the summarizer a plain-language checklist of what the epic built for
/// free) and run it through [`run_summarize_stage`]. Re-checks `lease` first:
/// by the time [`finalize_epic`] runs the lease was already confirmed live a
/// moment ago (its own caller's guard), but this spends a whole agent turn,
/// so it re-checks immediately before doing that — the same discipline every
/// other pause point in this module follows. `None` (skipping the agent call
/// entirely) when [`epic_summary_base_sha`] finds nothing to diff from.
async fn run_epic_summary(
    state: &AppState,
    epic_id: &str,
    epic: &crate::epics::Epic,
    dag: &crate::tasks::Dag,
    workspace: &ProvisionedWorkspace,
    lease: &LeaseHandle,
) -> Option<String> {
    if lease.is_lost() {
        return None;
    }
    let conn = state.db.conn();
    let base_sha = epic_summary_base_sha(conn, epic_id).await?;

    let siblings: Vec<SiblingTask> = dag
        .nodes
        .iter()
        .map(|n| SiblingTask {
            id: &n.task.id,
            title: &n.task.title,
            done: n.task.status == "Done",
        })
        .collect();
    let context = TaskContext {
        spec: SpecFields {
            title: &epic.title,
            description: epic.description.as_deref(),
            acceptance: None,
        },
        epic: None,
        siblings: &siblings,
        base_sha: Some(base_sha.as_str()),
    };

    run_summarize_stage(
        state,
        &epic.project_id,
        Some(epic_id),
        None,
        &context,
        &workspace.workspace_path,
    )
    .await
}

/// [`finalize_task`]'s T-560 summary step — the standalone mirror of
/// [`run_epic_summary`]: the task's own spec fields stand in for the epic's
/// title/description (this *is* the ordinary [`spec::SpecFields`] an
/// implement/review stage would see for this task), no siblings (a
/// standalone task has none, D17), `base_sha` from
/// [`task_summary_base_sha`]. `None` (skipping the agent call) when the task
/// somehow has no recorded `base_sha` (in practice: unreachable by the time
/// `finalize_task` runs, since every claimed task records it before
/// `Stage::Implement`, but handled rather than `.expect()`-ed for the same
/// "don't let a summary-step invariant miss take down finalize" reason
/// [`run_summarize_stage`] never returns anything but `Option`).
async fn run_task_summary(
    state: &AppState,
    task_id: &str,
    task: &crate::tasks::Task,
    workspace: &ProvisionedWorkspace,
    lease: &LeaseHandle,
) -> Option<String> {
    if lease.is_lost() {
        return None;
    }
    let conn = state.db.conn();
    let base_sha = task_summary_base_sha(conn, task_id).await?;

    let context = TaskContext {
        spec: SpecFields {
            title: &task.title,
            description: task.description.as_deref(),
            acceptance: task.acceptance.as_deref(),
        },
        epic: None,
        siblings: &[],
        base_sha: Some(base_sha.as_str()),
    };

    run_summarize_stage(
        state,
        &task.project_id,
        None,
        Some(task_id),
        &context,
        &workspace.workspace_path,
    )
    .await
}

/// Close the finalize step's single `Stage::Push` evidence row, if one was
/// successfully opened (best-effort: a failure to open it at the very start
/// of [`finalize_epic`] must not additionally block finalize from
/// proceeding — the push/PR outcome itself is what matters).
async fn close_push_stage(
    conn: &Connection,
    handle: &Option<StageHandle>,
    status: &'static str,
    log: &str,
) {
    let Some(handle) = handle else { return };
    let _ = evidence::close_stage(
        conn,
        handle,
        CloseStage {
            status,
            session_id: None,
            verdict: None,
            exit_code: if status == "ok" { Some(0) } else { None },
            log: log.to_string(),
            input_tokens: None,
            output_tokens: None,
        },
    )
    .await;
}

/// Just enough of a project row for [`finalize_epic`]'s push/PR step.
struct ProjectForFinalize {
    repo_url: String,
    /// §5 project default (`None` = repo default). Only consulted for
    /// standalone tasks — an epic always reads its own snapshot first.
    base_branch: Option<String>,
}

/// The §5-recorded explicit base branch for this finalize, or `None` when the
/// chain terminates at "repo default": an **epic** reads its own
/// `epic.base_branch` — written once at first provision and never recomputed,
/// so a later project-default edit can never retarget an in-flight epic. A
/// **standalone task** has no per-item record by design, so it falls straight
/// to the project default.
async fn recorded_base_branch(
    conn: &Connection,
    epic_id: Option<&str>,
    project_base: Option<&str>,
) -> Result<Option<String>, libsql::Error> {
    match epic_id {
        Some(epic_id) => {
            let mut rows = conn
                .query(
                    "SELECT base_branch FROM epic WHERE id = ?1",
                    params![epic_id],
                )
                .await?;
            match rows.next().await? {
                Some(row) => Ok(row.get::<Option<String>>(0)?),
                None => Ok(None),
            }
        }
        None => Ok(project_base.map(str::to_string)),
    }
}

async fn load_project_for_finalize(
    conn: &Connection,
    project_id: &str,
) -> Result<Option<ProjectForFinalize>, libsql::Error> {
    let mut rows = conn
        .query(
            "SELECT repo_url, base_branch FROM project WHERE id = ?1",
            params![project_id],
        )
        .await?;
    match rows.next().await? {
        Some(row) => Ok(Some(ProjectForFinalize {
            repo_url: row.get(0)?,
            base_branch: row.get(1)?,
        })),
        None => Ok(None),
    }
}

/// Build the PR body's task checklist (D16's template half): every task in
/// `dag`, in `position` order, paired with the commit SHA its `Stage::Commit`
/// evidence row recorded (`None` for a task that produced no diff), how many
/// `Stage::Review` rounds it went through (T-560, §9), and — for a task
/// closed via T-532 with zero commits — the verify-complete reasoning that
/// justified it (T-560, §9). Reads the SHA back out of `agent_run.log` via
/// [`pr::parse_commit_sha_from_commit_log`] — the same format
/// `process_one_task`'s commit step writes — rather than re-deriving it from
/// `git log`, so this stays a plain DB read next to everything else finalize
/// already does; the two new fields are plain DB reads of the same shape,
/// scoped by the identical `epic_id`.
async fn build_task_checklist(
    conn: &Connection,
    epic_id: &str,
    dag: &crate::tasks::Dag,
) -> Vec<pr::TaskChecklistItem> {
    let mut shas: std::collections::HashMap<String, String> = std::collections::HashMap::new();
    if let Ok(mut rows) = conn
        .query(
            "SELECT task_id, log FROM agent_run \
             WHERE epic_id = ?1 AND stage = 'commit' AND status = 'ok' \
             ORDER BY created_at ASC",
            params![epic_id],
        )
        .await
    {
        while let Ok(Some(row)) = rows.next().await {
            let task_id: Option<String> = row.get(0).unwrap_or(None);
            let log: String = row.get(1).unwrap_or_default();
            if let (Some(task_id), Some(sha)) =
                (task_id, pr::parse_commit_sha_from_commit_log(&log))
            {
                shas.insert(task_id, sha.to_string());
            }
        }
    }

    let review_rounds = fetch_review_round_counts(conn, "epic_id", epic_id).await;
    let verified = fetch_verified_complete_reasoning(conn, "epic_id", epic_id).await;

    let mut nodes: Vec<&crate::tasks::DagNode> = dag.nodes.iter().collect();
    nodes.sort_by_key(|n| n.task.position.unwrap_or(i64::MAX));

    nodes
        .into_iter()
        .map(|n| pr::TaskChecklistItem {
            title: n.task.title.clone(),
            short_id: spec::short_id(&n.task.id).to_string(),
            commit_sha: shas.get(&n.task.id).cloned(),
            review_rounds: review_rounds.get(&n.task.id).copied().unwrap_or(0),
            verified_complete_reasoning: verified.get(&n.task.id).cloned(),
        })
        .collect()
}

/// The standalone-task mirror of [`build_task_checklist`] (T-551): a
/// one-item PR-body checklist for the task itself, reading its own commit
/// SHA the identical way — `agent_run.log`'s `"commit {sha}: {subject}"`
/// format via [`pr::parse_commit_sha_from_commit_log`] — just filtered by
/// `task_id` instead of `epic_id` (a standalone task's evidence rows carry
/// `epic_id: None, task_id: Some(_)`, the opposite of an epic-owned task's —
/// see `commit_if_dirty`). Ascending `created_at` order means the *last*
/// insert into `sha` wins on a task with more than one commit (a review fix
/// round), mirroring `build_task_checklist`'s own "later commit overwrites
/// the map entry" behavior. Not a generalization of `build_task_checklist`
/// itself: that function's whole shape is "walk a `Dag`'s many nodes," which
/// a standalone task — one task, no DAG — has nothing to walk; duplicating
/// its ~15 lines here reads clearer than threading an `Either<Dag, &Task>`
/// through a function built around iterating many nodes. T-560's
/// review-round-count/verify-complete-reasoning fields reuse
/// [`fetch_review_round_counts`]/[`fetch_verified_complete_reasoning`] scoped
/// by `task_id` instead of `epic_id` — the same two functions
/// [`build_task_checklist`] calls, just filtered on the other column, since a
/// standalone task's evidence rows are `task_id`-keyed the way an epic-owned
/// task's are `epic_id`-keyed for this same lookup.
async fn build_standalone_checklist(
    conn: &Connection,
    task: &crate::tasks::Task,
) -> Vec<pr::TaskChecklistItem> {
    let mut sha: Option<String> = None;
    if let Ok(mut rows) = conn
        .query(
            "SELECT log FROM agent_run \
             WHERE task_id = ?1 AND stage = 'commit' AND status = 'ok' \
             ORDER BY created_at ASC",
            params![task.id.clone()],
        )
        .await
    {
        while let Ok(Some(row)) = rows.next().await {
            let log: String = row.get(0).unwrap_or_default();
            if let Some(s) = pr::parse_commit_sha_from_commit_log(&log) {
                sha = Some(s.to_string());
            }
        }
    }

    let review_rounds = fetch_review_round_counts(conn, "task_id", &task.id).await;
    let verified = fetch_verified_complete_reasoning(conn, "task_id", &task.id).await;

    vec![pr::TaskChecklistItem {
        title: task.title.clone(),
        short_id: spec::short_id(&task.id).to_string(),
        commit_sha: sha,
        review_rounds: review_rounds.get(&task.id).copied().unwrap_or(0),
        verified_complete_reasoning: verified.get(&task.id).cloned(),
    }]
}

/// T-560: how many completed `Stage::Review` rounds each task under
/// `scope_value` (an epic id when `scope_column = "epic_id"`, a task id when
/// `scope_column = "task_id"`) went through — one `agent_run` row
/// (`status = 'ok'`) per round (T-530/T-531's `attempt` is 0-based, but this
/// counts *rows*, matching the plain "how many times was this reviewed"
/// reading [`pr::build_pr_body`]'s "Review rounds" section wants). Feeds both
/// [`build_task_checklist`] (`scope_column = "epic_id"`, one row per task in
/// the epic) and [`build_standalone_checklist`] (`scope_column = "task_id"`,
/// the map has at most the one entry). `scope_column` is one of exactly two
/// known-safe literals, never caller-supplied text, so interpolating it into
/// the SQL (rather than a bind parameter — column names can't be bound in
/// SQLite anyway) carries no injection risk.
async fn fetch_review_round_counts(
    conn: &Connection,
    scope_column: &'static str,
    scope_value: &str,
) -> std::collections::HashMap<String, u32> {
    let mut counts = std::collections::HashMap::new();
    let sql = format!(
        "SELECT task_id, COUNT(*) FROM agent_run \
         WHERE {scope_column} = ?1 AND stage = 'review' AND status = 'ok' \
         GROUP BY task_id"
    );
    if let Ok(mut rows) = conn.query(&sql, params![scope_value]).await {
        while let Ok(Some(row)) = rows.next().await {
            let task_id: Option<String> = row.get(0).unwrap_or(None);
            let count: i64 = row.get(1).unwrap_or(0);
            if let Some(task_id) = task_id {
                counts.insert(task_id, count.max(0) as u32);
            }
        }
    }
    counts
}

/// T-560: the T-532 verify-complete reasoning that closed a task with zero
/// commits, per task, scoped the same way [`fetch_review_round_counts`] is
/// (`scope_column`/`scope_value`). Only a `PASS`ing `Stage::VerifyComplete`
/// row counts — a `NEEDS_CHANGES`/`BLOCKED` verdict does not close the task
/// with zero commits (§6: `NEEDS_CHANGES` re-enters the ordinary pipeline and
/// ends up committed; `BLOCKED` fails the task outright), so surfacing either
/// here would misrepresent a task that has (or will have) a real commit as
/// one that was "verified already complete." Feeds
/// [`pr::TaskChecklistItem::verified_complete_reasoning`].
async fn fetch_verified_complete_reasoning(
    conn: &Connection,
    scope_column: &'static str,
    scope_value: &str,
) -> std::collections::HashMap<String, String> {
    let mut reasoning = std::collections::HashMap::new();
    let sql = format!(
        "SELECT task_id, log FROM agent_run \
         WHERE {scope_column} = ?1 AND stage = 'verify_complete' AND status = 'ok' AND verdict = 'PASS' \
         ORDER BY created_at ASC"
    );
    if let Ok(mut rows) = conn.query(&sql, params![scope_value]).await {
        while let Ok(Some(row)) = rows.next().await {
            let task_id: Option<String> = row.get(0).unwrap_or(None);
            let log: String = row.get(1).unwrap_or_default();
            if let Some(task_id) = task_id {
                reasoning.insert(task_id, log);
            }
        }
    }
    reasoning
}

/// Resolve the project id for an epic (best-effort, for the board publish).
/// Re-fetches the epic to read `.project_id` directly. Kept for completeness;
/// the pipeline body uses `fetch_epic` + `.project_id` instead.
#[allow(dead_code)]
async fn resolve_project_id(state: &AppState, epic_id: &str) -> Option<String> {
    get_epic_project_id(state.db.conn(), epic_id)
        .await
        .ok()
        .flatten()
}

/// Current unix time in milliseconds (matches the `*_at` columns).
fn now_ms() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::breakdown::testing::SilentBreakdownAgent;
    use crate::git_host::testing::FakeHost;
    use crate::git_host::GitHost;
    use crate::planning::testing::{Gate, SilentPlanningAgent};
    use crate::task_agent::testing::{ScriptedRun, ScriptedTaskAgent};
    use crate::{app, Config, Db, TaskAgent};
    use axum::body::Body;
    use axum::http::{
        header::{AUTHORIZATION, CONTENT_TYPE},
        Request, StatusCode,
    };
    use harness::{HarnessError, RunEvent, RunHandle, RunMode};
    use libsql::params;
    use serde_json::{json, Value};
    use std::collections::HashMap;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};
    use std::sync::mpsc::Receiver;
    use std::sync::{Arc, Mutex};
    use std::time::Duration;
    use tower::ServiceExt;

    /// The bearer credential HTTP tests present, minted **once per process**
    /// from a seeded active admin (`crate::users::testing::seed_user` +
    /// `crate::sessions::testing::login_as`) — the replacement for the deleted
    /// static `TOKEN` constant. Access-token verification is stateless (one
    /// HMAC check against the fixed test master key, no database read), so a
    /// token minted here authenticates against every in-memory instance these
    /// tests boot.
    fn auth_bearer() -> &'static str {
        static BEARER: std::sync::OnceLock<String> = std::sync::OnceLock::new();
        BEARER.get_or_init(|| {
            // Seeding and login are async store calls, and `req` below is
            // synchronous. Mint on a dedicated OS thread: `Runtime::block_on`
            // panics if called from inside a test's own async context, but a
            // plain thread has none, so a throwaway current-thread runtime is
            // legal there.
            let (tx, rx) = std::sync::mpsc::channel();
            std::thread::spawn(move || {
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .expect("test runtime");
                let token = runtime.block_on(async {
                    let db = crate::Db::connect(":memory:").await.unwrap();
                    db.run_migrations().await.unwrap();
                    let state = crate::AppState::new(crate::Config::for_test(), db);
                    let user = crate::users::testing::seed_user(
                        &state,
                        "tester",
                        crate::users::Role::Admin,
                        true,
                    )
                    .await;
                    crate::sessions::testing::login_as(&state, &user).await
                });
                tx.send(token).expect("bearer receiver dropped");
            });
            rx.recv().expect("bearer minter panicked")
        })
    }

    fn req(method: &str, uri: &str, body: Option<Value>) -> Request<Body> {
        let builder = Request::builder()
            .method(method)
            .uri(uri)
            .header(AUTHORIZATION, format!("Bearer {}", auth_bearer()));
        match body {
            Some(v) => builder
                .header(CONTENT_TYPE, "application/json")
                .body(Body::from(v.to_string()))
                .unwrap(),
            None => builder.body(Body::empty()).unwrap(),
        }
    }

    async fn body_json(response: axum::response::Response) -> Value {
        let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        if bytes.is_empty() {
            return Value::Null;
        }
        serde_json::from_slice(&bytes).unwrap()
    }

    /// Boot an app over an in-memory db with silent planning/breakdown agents
    /// and a bare [`ScriptedTaskAgent`] (no scripted runs — every stage falls
    /// back to [`crate::task_agent::testing::ScriptedRun::default`]: exit 0,
    /// no files written, i.e. a no-op success). Fine for every test that
    /// doesn't care what the implement stage does, just that it succeeds
    /// (fast, via `Config::for_test`). Returns (state, app).
    async fn test_app() -> (AppState, axum::Router) {
        test_app_with_task_agent(Arc::new(ScriptedTaskAgent::new())).await
    }

    /// Like [`test_app`] but with an explicit [`TaskAgent`] — the seam T-513's
    /// tests use to script the implement stage's behavior (write files,
    /// fail, or gate in-flight) instead of accepting the bare no-op default.
    ///
    /// Uses [`FakeHost`] (T-514) rather than the default production
    /// [`git_host::GithubHost`) so that once a test's DAG walk goes fully
    /// `Done`, finalize's push (real, local — the fixture repos this module
    /// uses have no PAT and no real network) + open-PR (faked) both succeed
    /// deterministically: every pre-existing T-513 test in this module that
    /// drives a walk to completion now also exercises T-514's finalize step,
    /// which is why several of them assert `InReview` (not `InProgress`)
    /// below — that assertion changed *because* T-514 landed, not because
    /// this test scaffolding changed independently of it.
    async fn test_app_with_task_agent(task_agent: Arc<dyn TaskAgent>) -> (AppState, axum::Router) {
        let db = Db::connect(":memory:").await.unwrap();
        db.run_migrations().await.unwrap();
        let state = AppState::with_all_agents_and_host(
            Config::for_test(),
            db,
            Arc::new(SilentPlanningAgent),
            Arc::new(SilentBreakdownAgent),
            task_agent,
            Arc::new(FakeHost::new()),
        );
        let app = app(state.clone());
        (state, app)
    }

    async fn seed_project(state: &AppState) -> String {
        let conn = state.db.conn();
        let id = ulid::Ulid::new().to_string();
        let now = now_ms();
        conn.execute(
            "INSERT INTO project (id, name, repo_url, clone_status, created_at, updated_at) \
             VALUES (?1, 'P', 'https://example.com/p.git', 'ready', ?2, ?2)",
            params![id.clone(), now],
        )
        .await
        .unwrap();
        id
    }

    async fn seed_epic(state: &AppState, project_id: &str, status: &str) -> String {
        let conn = state.db.conn();
        let id = ulid::Ulid::new().to_string();
        let now = now_ms();
        conn.execute(
            "INSERT INTO epic (id, project_id, title, status, created_at, updated_at) \
             VALUES (?1, ?2, 'E', ?3, ?4, ?4)",
            params![id.clone(), project_id, status, now],
        )
        .await
        .unwrap();
        id
    }

    // ---- T-511: a real (local, hermetic) git fixture for pipeline-body tests ----
    //
    // Since T-511, the claimed-epic body provisions a workspace (a real
    // `git clone`/`git fetch`) before the DAG walk. Any test that drives the
    // body to completion (`run_epic_pipeline`, or the pool via `spawn_pool`)
    // needs a project whose `clone_path`/`repo_url` point at something git can
    // actually clone from — the plain `seed_project` above (no `clone_path`,
    // a fake `repo_url`) is intentionally kept for the claim/heartbeat/lease
    // tests that never reach provisioning (they call `claim_epic`/
    // `renew_lease_once` directly, or seed a non-`InProgress` epic).

    /// A local git fixture: `git init`'s a source repo with one commit in a
    /// fresh temp dir, entirely offline. Cleans itself up on drop.
    struct GitFixture {
        dir: std::path::PathBuf,
    }

    impl GitFixture {
        async fn new() -> GitFixture {
            let dir = std::env::temp_dir().join(format!(
                "dearborn-worker-fixture-{}-{}",
                std::process::id(),
                ulid::Ulid::new()
            ));
            std::fs::create_dir_all(&dir).unwrap();
            for args in [
                &["init", "-b", "main"][..],
                &["config", "user.email", "test@example.com"],
                &["config", "user.name", "Test"],
            ] {
                git_ok(&dir, args).await;
            }
            std::fs::write(dir.join("README.md"), "hello\n").unwrap();
            git_ok(&dir, &["add", "."]).await;
            git_ok(&dir, &["commit", "-m", "init"]).await;
            GitFixture { dir }
        }

        fn path_str(&self) -> String {
            self.dir.to_string_lossy().to_string()
        }
    }

    impl Drop for GitFixture {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.dir);
        }
    }

    async fn git_ok(dir: &std::path::Path, args: &[&str]) {
        let status = tokio::process::Command::new("git")
            .args(args)
            .current_dir(dir)
            .status()
            .await
            .unwrap();
        assert!(status.success(), "git {args:?} failed in {dir:?}");
    }

    /// Like [`seed_project`] but with a real `clone_path`/`repo_url` pointing
    /// at `fixture`, so a claimed epic under this project can actually
    /// provision a workspace (T-511) instead of failing `workspace_error`.
    async fn seed_project_with_workspace(state: &AppState, fixture: &GitFixture) -> String {
        let conn = state.db.conn();
        let id = ulid::Ulid::new().to_string();
        let now = now_ms();
        let clone_path = std::path::Path::new(&state.config.clone_root).join(&id);
        conn.execute(
            "INSERT INTO project (id, name, repo_url, clone_path, clone_status, created_at, updated_at) \
             VALUES (?1, 'P', ?2, ?3, 'ready', ?4, ?4)",
            params![
                id.clone(),
                fixture.path_str(),
                clone_path.to_string_lossy().to_string(),
                now
            ],
        )
        .await
        .unwrap();
        id
    }

    /// Remove the on-disk clone directories a `seed_project_with_workspace`
    /// test created, so repeated local runs don't accumulate temp dirs.
    fn cleanup_clone_root(state: &AppState, project_id: &str, epic_ids: &[&str]) {
        let root = std::path::Path::new(&state.config.clone_root);
        let _ = std::fs::remove_dir_all(root.join(project_id));
        for epic_id in epic_ids {
            let _ = std::fs::remove_dir_all(root.join("epics").join(epic_id));
        }
    }

    /// Create a task under `epic_id` with `status='Todo'` via direct SQL (mirrors
    /// `tasks::create_task` but keeps the test self-contained).
    async fn seed_task(state: &AppState, epic_id: &str, project_id: &str, title: &str) -> String {
        let conn = state.db.conn();
        let id = ulid::Ulid::new().to_string();
        let now = now_ms();
        conn.execute(
            "INSERT INTO task \
             (id, epic_id, project_id, title, status, position, created_at, updated_at) \
             VALUES (?1, ?2, ?3, ?4, 'Todo', \
             (SELECT COALESCE(MAX(position), 0) + 1 FROM task WHERE epic_id = ?2), \
             ?5, ?5)",
            params![id.clone(), epic_id, project_id, title, now],
        )
        .await
        .unwrap();
        id
    }

    /// Set a task's status directly (used to seed an "orphaned InProgress"
    /// task left by a dead owner).
    async fn set_task_status(state: &AppState, task_id: &str, status: &str) {
        let conn = state.db.conn();
        conn.execute(
            "UPDATE task SET status = ?1 WHERE id = ?2",
            params![status, task_id],
        )
        .await
        .unwrap();
    }

    /// Link `blocker_id → blocked_id` via direct SQL (no cycle guard needed —
    /// tests build valid acyclic DAGs).
    async fn link(state: &AppState, blocker_id: &str, blocked_id: &str) {
        let conn = state.db.conn();
        conn.execute(
            "INSERT OR IGNORE INTO task_dependency (blocker_id, blocked_id) VALUES (?1, ?2)",
            params![blocker_id, blocked_id],
        )
        .await
        .unwrap();
    }

    /// Fetch all task statuses for an epic, keyed by title.
    async fn task_statuses(
        state: &AppState,
        epic_id: &str,
    ) -> std::collections::HashMap<String, String> {
        let conn = state.db.conn();
        let mut rows = conn
            .query(
                "SELECT title, status FROM task WHERE epic_id = ?1",
                params![epic_id],
            )
            .await
            .unwrap();
        let mut map = std::collections::HashMap::new();
        while let Some(row) = rows.next().await.unwrap() {
            map.insert(row.get::<String>(0).unwrap(), row.get::<String>(1).unwrap());
        }
        map
    }

    async fn epic_status(state: &AppState, epic_id: &str) -> String {
        fetch_epic(state.db.conn(), epic_id)
            .await
            .unwrap()
            .unwrap()
            .status
    }

    async fn epic_lease(state: &AppState, epic_id: &str) -> (Option<String>, Option<i64>) {
        let conn = state.db.conn();
        let mut rows = conn
            .query(
                "SELECT lease_owner, lease_expires_at FROM epic WHERE id = ?1",
                params![epic_id],
            )
            .await
            .unwrap();
        let row = rows.next().await.unwrap().unwrap();
        (row.get(0).unwrap(), row.get(1).unwrap())
    }

    /// The `task`-table mirror of [`epic_lease`] (T-550).
    async fn task_lease(state: &AppState, task_id: &str) -> (Option<String>, Option<i64>) {
        let conn = state.db.conn();
        let mut rows = conn
            .query(
                "SELECT lease_owner, lease_expires_at FROM task WHERE id = ?1",
                params![task_id],
            )
            .await
            .unwrap();
        let row = rows.next().await.unwrap().unwrap();
        (row.get(0).unwrap(), row.get(1).unwrap())
    }

    /// Create a **standalone** task (`epic_id IS NULL`, T-550/D17) directly
    /// via SQL, with whatever `status` the test needs — mirrors [`seed_task`]
    /// but for the claim/lease tests that need a task with no parent epic at
    /// all rather than one seeded `Todo` under an epic.
    async fn seed_standalone_task(
        state: &AppState,
        project_id: &str,
        title: &str,
        status: &str,
    ) -> String {
        let conn = state.db.conn();
        let id = ulid::Ulid::new().to_string();
        let now = now_ms();
        conn.execute(
            "INSERT INTO task (id, epic_id, project_id, title, status, created_at, updated_at) \
             VALUES (?1, NULL, ?2, ?3, ?4, ?5, ?5)",
            params![id.clone(), project_id, title, status, now],
        )
        .await
        .unwrap();
        id
    }

    /// A single task's `status` column, by id — used where a test needs to
    /// check one standalone task (which has no `epic_id` for [`task_statuses`]
    /// to key its map by).
    async fn single_task_status(state: &AppState, task_id: &str) -> String {
        let mut rows = state
            .db
            .conn()
            .query("SELECT status FROM task WHERE id = ?1", params![task_id])
            .await
            .unwrap();
        rows.next().await.unwrap().unwrap().get(0).unwrap()
    }

    // ---- run_epic_pipeline direct tests: real DAG walk (T-513) ----
    //
    // These use the bare `test_app()` (a `ScriptedTaskAgent` with no scripted
    // runs, i.e. every implement stage is a no-op success — see `test_app`'s
    // doc). A no-op implement stage produces no diff, so no commit ever
    // lands for these tests; that's fine, they're only asserting the DAG
    // walk's task-status/epic-status contract, not the commit machinery
    // (covered separately below). Since T-514, a full walk's finalize step
    // pushes the branch (real, local — `FakeHost::push` delegates to the
    // genuine `git::push_branch`) and opens a (faked) PR, so the epic now
    // reaches `InReview`, not the `InProgress`-forever state T-513 alone
    // left it in (see `finalize_epic`'s doc for why that transition waits
    // this long, and `enqueue_via_lane_drives_dag_to_done` below for the
    // dedicated proof that an `InReview` epic is never re-claimed).

    /// Linear DAG (A → B → C): after the walk, all Done, epic InReview.
    ///
    /// The dependency ORDER is respected implicitly: B can only become ready
    /// after A is Done (its only blocker), and C after B. So asserting the
    /// final state (all Done) IS the order assertion — a reversed walk could
    /// never reach all-Done. See `implement_stage_runs_respect_dependency_order`
    /// below for a stronger, order-observing proof.
    #[tokio::test]
    async fn linear_dag_walks_every_task_to_done_epic_completes() {
        let (state, _app) = test_app().await;
        let fixture = GitFixture::new().await;
        let project_id = seed_project_with_workspace(&state, &fixture).await;
        let epic_id = seed_epic(&state, &project_id, "InProgress").await;

        let a = seed_task(&state, &epic_id, &project_id, "A").await;
        let b = seed_task(&state, &epic_id, &project_id, "B").await;
        let c = seed_task(&state, &epic_id, &project_id, "C").await;
        // A blocks B, B blocks C (A → B → C).
        link(&state, &a, &b).await;
        link(&state, &b, &c).await;

        run_epic_pipeline(state.clone(), epic_id.clone()).await;

        let statuses = task_statuses(&state, &epic_id).await;
        assert_eq!(statuses["A"], "Done");
        assert_eq!(statuses["B"], "Done");
        assert_eq!(statuses["C"], "Done");
        assert_eq!(
            epic_status(&state, &epic_id).await,
            "InReview",
            "T-514's finalize step must land the epic in InReview once every task is Done"
        );
        cleanup_clone_root(&state, &project_id, &[&epic_id]);
    }

    /// Branching DAG (A blocks B and C; B and C both block D): all Done,
    /// epic InReview.
    #[tokio::test]
    async fn branching_dag_walks_every_task_to_done_epic_completes() {
        let (state, _app) = test_app().await;
        let fixture = GitFixture::new().await;
        let project_id = seed_project_with_workspace(&state, &fixture).await;
        let epic_id = seed_epic(&state, &project_id, "InProgress").await;

        let a = seed_task(&state, &epic_id, &project_id, "A").await;
        let b = seed_task(&state, &epic_id, &project_id, "B").await;
        let c = seed_task(&state, &epic_id, &project_id, "C").await;
        let d = seed_task(&state, &epic_id, &project_id, "D").await;
        // A → B, A → C, B → D, C → D.
        link(&state, &a, &b).await;
        link(&state, &a, &c).await;
        link(&state, &b, &d).await;
        link(&state, &c, &d).await;

        run_epic_pipeline(state.clone(), epic_id.clone()).await;

        let statuses = task_statuses(&state, &epic_id).await;
        assert_eq!(statuses["A"], "Done");
        assert_eq!(statuses["B"], "Done");
        assert_eq!(statuses["C"], "Done");
        assert_eq!(statuses["D"], "Done");
        assert_eq!(epic_status(&state, &epic_id).await, "InReview");
        cleanup_clone_root(&state, &project_id, &[&epic_id]);
    }

    /// Empty epic (no tasks): the walk finds the (vacuously) fully-Done DAG
    /// immediately, and finalize still pushes + opens a PR for it — an
    /// epic with zero tasks is a degenerate but valid case, not a special
    /// one finalize needs to skip.
    #[tokio::test]
    async fn empty_epic_still_completes() {
        let (state, _app) = test_app().await;
        let fixture = GitFixture::new().await;
        let project_id = seed_project_with_workspace(&state, &fixture).await;
        let epic_id = seed_epic(&state, &project_id, "InProgress").await;

        run_epic_pipeline(state.clone(), epic_id.clone()).await;

        assert_eq!(epic_status(&state, &epic_id).await, "InReview");
        cleanup_clone_root(&state, &project_id, &[&epic_id]);
    }

    /// Non-InProgress epic is a no-op: no task or epic status changes (the
    /// walk never even reaches provisioning).
    #[tokio::test]
    async fn non_in_progress_epic_is_no_op() {
        let (state, _app) = test_app().await;
        let project_id = seed_project(&state).await;
        let epic_id = seed_epic(&state, &project_id, "Ready").await;
        seed_task(&state, &epic_id, &project_id, "A").await;

        run_epic_pipeline(state.clone(), epic_id.clone()).await;

        let statuses = task_statuses(&state, &epic_id).await;
        assert_eq!(statuses["A"], "Todo", "task untouched");
        assert_eq!(
            epic_status(&state, &epic_id).await,
            "Ready",
            "epic untouched"
        );
    }

    /// No sibling InProgress invariant: after a full run, the final state is
    /// consistent — all Done, none InProgress, epic InReview. The walk
    /// serializes by construction (one ready task at a time); this
    /// final-state assertion confirms it. See
    /// `implement_stage_never_observes_a_sibling_in_progress` below for a
    /// stronger, moment-by-moment proof via the DB itself.
    #[tokio::test]
    async fn no_sibling_in_progress_after_run() {
        let (state, _app) = test_app().await;
        let fixture = GitFixture::new().await;
        let project_id = seed_project_with_workspace(&state, &fixture).await;
        let epic_id = seed_epic(&state, &project_id, "InProgress").await;
        let a = seed_task(&state, &epic_id, &project_id, "A").await;
        let b = seed_task(&state, &epic_id, &project_id, "B").await;
        // A and B are independent (no edge between them) — both are ready from
        // the start. The walk still claims one at a time.
        link(&state, &a, &b).await; // A → B: only A is ready initially.

        run_epic_pipeline(state.clone(), epic_id.clone()).await;

        let statuses = task_statuses(&state, &epic_id).await;
        assert_eq!(statuses["A"], "Done");
        assert_eq!(statuses["B"], "Done");
        assert!(statuses.values().all(|s| s != "InProgress"));
        assert_eq!(epic_status(&state, &epic_id).await, "InReview");
        cleanup_clone_root(&state, &project_id, &[&epic_id]);
    }

    // ---- T-510: claim semantics ----

    /// Two (many) workers racing the claim SQL against one enqueued epic:
    /// exactly one succeeds. Hammers `claim_epic` concurrently on the same
    /// underlying connection — SQLite/libSQL's write serialization is what
    /// makes this deterministic (§6), not any application-level mutex.
    #[tokio::test]
    async fn concurrent_claims_on_one_epic_yield_exactly_one_success() {
        let (state, _app) = test_app().await;
        let project_id = seed_project(&state).await;
        let epic_id = seed_epic(&state, &project_id, "InProgress").await;

        let mut handles = Vec::new();
        for i in 0..25 {
            let db = state.db.clone();
            handles.push(tokio::spawn(async move {
                claim_epic(db.conn(), &format!("racer-{i}"), 30).await
            }));
        }

        let mut successes = 0;
        let mut winner = None;
        for h in handles {
            if let Ok(Ok(Some(claimed))) = h.await {
                successes += 1;
                winner = Some(claimed.id);
            }
        }
        assert_eq!(successes, 1, "exactly one racer must claim the epic");
        assert_eq!(winner.as_deref(), Some(epic_id.as_str()));
    }

    /// An expired lease is re-claimable by a new owner, and the new owner's
    /// claim path resets the previous owner's abandoned `InProgress` task
    /// back to `Todo`.
    #[tokio::test]
    async fn expired_lease_is_reclaimable_and_resets_orphaned_tasks() {
        let (state, _app) = test_app().await;
        let project_id = seed_project(&state).await;
        let epic_id = seed_epic(&state, &project_id, "InProgress").await;
        let a = seed_task(&state, &epic_id, &project_id, "A").await;
        set_task_status(&state, &a, "InProgress").await; // abandoned mid-flight

        let conn = state.db.conn();
        let past = now_ms() - 60_000;
        conn.execute(
            "UPDATE epic SET lease_owner = 'dead-worker', lease_expires_at = ?1 WHERE id = ?2",
            params![past, epic_id.clone()],
        )
        .await
        .unwrap();

        let claimed = claim_epic(conn, "new-worker", 30).await.unwrap();
        let claimed = claimed.expect("expired lease must be reclaimable");
        assert_eq!(claimed.id, epic_id);

        let (owner, _expires) = epic_lease(&state, &epic_id).await;
        assert_eq!(owner.as_deref(), Some("new-worker"));

        reset_orphaned_tasks(conn, &epic_id).await.unwrap();
        let statuses = task_statuses(&state, &epic_id).await;
        assert_eq!(
            statuses["A"], "Todo",
            "orphaned InProgress task reset to Todo"
        );
    }

    /// A lease that is still live (not expired) is NOT re-claimable — the
    /// negative case alongside the expired-lease test above.
    #[tokio::test]
    async fn live_lease_is_not_reclaimable() {
        let (state, _app) = test_app().await;
        let project_id = seed_project(&state).await;
        let epic_id = seed_epic(&state, &project_id, "InProgress").await;

        let conn = state.db.conn();
        let future = now_ms() + 60_000;
        conn.execute(
            "UPDATE epic SET lease_owner = 'alive-worker', lease_expires_at = ?1 WHERE id = ?2",
            params![future, epic_id.clone()],
        )
        .await
        .unwrap();

        let claimed = claim_epic(conn, "other-worker", 30).await.unwrap();
        assert!(claimed.is_none(), "a live lease must not be reclaimable");
    }

    // ---- T-510: heartbeat + fencing ----

    /// A heartbeat renewal against a lease already stolen by another worker
    /// (lease_owner changed out from under us) reports the loss (0 rows
    /// affected) directly — the pure fencing check, no timers involved.
    #[tokio::test]
    async fn heartbeat_against_stolen_lease_reports_lost() {
        let (state, _app) = test_app().await;
        let project_id = seed_project(&state).await;
        let epic_id = seed_epic(&state, &project_id, "InProgress").await;
        let conn = state.db.conn();

        // We hold the lease as "me"...
        conn.execute(
            "UPDATE epic SET lease_owner = 'me', lease_expires_at = ?1 WHERE id = ?2",
            params![now_ms() + 60_000, epic_id.clone()],
        )
        .await
        .unwrap();

        // ...then someone else's claim steals it (simulating our lease having
        // expired and a second worker claiming in the meantime).
        conn.execute(
            "UPDATE epic SET lease_owner = 'thief', lease_expires_at = ?1 WHERE id = ?2",
            params![now_ms() + 60_000, epic_id.clone()],
        )
        .await
        .unwrap();

        let still_held = renew_lease_once(conn, &epic_id, "me", 30).await.unwrap();
        assert!(
            !still_held,
            "renewal against a stolen lease must report 0 rows / lost"
        );

        // The row still belongs to the thief — our renewal must not have
        // clobbered it.
        let (owner, _) = epic_lease(&state, &epic_id).await;
        assert_eq!(owner.as_deref(), Some("thief"));
    }

    /// A live lease renews successfully (the positive case).
    #[tokio::test]
    async fn heartbeat_against_live_lease_succeeds() {
        let (state, _app) = test_app().await;
        let project_id = seed_project(&state).await;
        let epic_id = seed_epic(&state, &project_id, "InProgress").await;
        let conn = state.db.conn();
        conn.execute(
            "UPDATE epic SET lease_owner = 'me', lease_expires_at = ?1 WHERE id = ?2",
            params![now_ms() + 60_000, epic_id.clone()],
        )
        .await
        .unwrap();

        let still_held = renew_lease_once(conn, &epic_id, "me", 30).await.unwrap();
        assert!(still_held);
    }

    /// End-to-end wiring of `spawn_heartbeat` + [`LeaseHandle`]: a stolen
    /// lease flips the shared handle to lost within one heartbeat period.
    /// Uses a short `Duration` directly (not the config-parsed
    /// `heartbeat_secs`, which rejects sub-second values) and a bounded
    /// deadline poll rather than a fixed sleep, matching the rest of the
    /// suite's polling convention.
    #[tokio::test]
    async fn spawn_heartbeat_flags_lease_handle_lost_on_theft() {
        let (state, _app) = test_app().await;
        let project_id = seed_project(&state).await;
        let epic_id = seed_epic(&state, &project_id, "InProgress").await;
        let conn = state.db.conn();
        conn.execute(
            "UPDATE epic SET lease_owner = 'me', lease_expires_at = ?1 WHERE id = ?2",
            params![now_ms() + 60_000, epic_id.clone()],
        )
        .await
        .unwrap();

        let lease = LeaseHandle::new();
        let handle = spawn_heartbeat(
            state.db.conn().clone(),
            epic_id.clone(),
            "me".to_string(),
            Duration::from_millis(15),
            30,
            lease.clone(),
            state.cancel_registry.clone(),
        );

        // Steal the lease.
        conn.execute(
            "UPDATE epic SET lease_owner = 'thief' WHERE id = ?1",
            params![epic_id.clone()],
        )
        .await
        .unwrap();

        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        loop {
            if lease.is_lost() {
                break;
            }
            if tokio::time::Instant::now() >= deadline {
                panic!("heartbeat never observed the stolen lease");
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }

        handle.abort();
    }

    /// T-565: a fenced-out heartbeat must **kill** any in-flight agent stage
    /// it owns — not just flag the lease lost. A gated `ScriptedTaskAgent`
    /// run stands in for the real in-flight stage; its handle sits in the
    /// cancel registry under the claimed item's id exactly as
    /// `task_agent::CancelGuard` would hold it while the stage ran. After the
    /// lease is stolen out from under the heartbeat, that registered handle
    /// must observe `was_cancelled()` within one heartbeat period.
    #[tokio::test]
    async fn spawn_heartbeat_fence_out_cancels_a_registered_in_flight_handle() {
        use crate::planning::testing::Gate;
        use crate::task_agent::testing::ScriptedTaskAgent;
        use crate::task_agent::{Stage, TaskAgent, TaskRunRequest};

        let (state, _app) = test_app().await;
        let project_id = seed_project(&state).await;
        let epic_id = seed_epic(&state, &project_id, "InProgress").await;
        let conn = state.db.conn();
        conn.execute(
            "UPDATE epic SET lease_owner = 'me', lease_expires_at = ?1 WHERE id = ?2",
            params![now_ms() + 60_000, epic_id.clone()],
        )
        .await
        .unwrap();

        // The in-flight stage: gated so it never reaches Exited on its own.
        let gate = Arc::new(Gate::default());
        let agent = ScriptedTaskAgent::new().with_gate(gate.clone());
        let (handle, rx) = agent
            .run(TaskRunRequest {
                run_id: "run-fence-out-cancel".to_string(),
                stage: Stage::Implement,
                prompt: "go".to_string(),
                cwd: std::env::temp_dir(),
                harness: "claude".to_string(),
                model: None,
                prompt_hash: "test-prompt-hash".to_string(),
            })
            .unwrap();
        state
            .cancel_registry
            .lock()
            .unwrap()
            .insert(epic_id.clone(), handle);

        let lease = LeaseHandle::new();
        let hb = spawn_heartbeat(
            state.db.conn().clone(),
            epic_id.clone(),
            "me".to_string(),
            Duration::from_millis(15),
            30,
            lease.clone(),
            state.cancel_registry.clone(),
        );

        // Steal the lease.
        conn.execute(
            "UPDATE epic SET lease_owner = 'thief' WHERE id = ?1",
            params![epic_id.clone()],
        )
        .await
        .unwrap();

        // The fenced-out heartbeat must have cancelled the registered handle
        // within one period (bounded deadline poll per suite convention).
        let registry = state.cancel_registry.clone();
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        loop {
            let was_cancelled = {
                let map = registry.lock().unwrap();
                map.get(&epic_id).map(|h| h.was_cancelled())
            };
            if was_cancelled == Some(true) {
                break;
            }
            if tokio::time::Instant::now() >= deadline {
                panic!("fenced-out heartbeat never cancelled the in-flight stage");
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        assert!(lease.is_lost(), "fence-out must also flag the lease lost");

        hb.abort();
        gate.release();
        drop(rx);
    }

    // ---- T-510: boot-time lease clear ----

    #[tokio::test]
    async fn boot_clears_all_leases_on_epic_and_task() {
        let (state, _app) = test_app().await;
        let project_id = seed_project(&state).await;
        let epic_id = seed_epic(&state, &project_id, "InProgress").await;
        let task_id = seed_task(&state, &epic_id, &project_id, "A").await;
        let conn = state.db.conn();
        conn.execute(
            "UPDATE epic SET lease_owner = 'w', lease_expires_at = 99999999999 WHERE id = ?1",
            params![epic_id.clone()],
        )
        .await
        .unwrap();
        conn.execute(
            "UPDATE task SET lease_owner = 'w', lease_expires_at = 99999999999 WHERE id = ?1",
            params![task_id.clone()],
        )
        .await
        .unwrap();

        clear_all_leases(&state.db).await.unwrap();

        let (owner, expires) = epic_lease(&state, &epic_id).await;
        assert!(owner.is_none());
        assert!(expires.is_none());

        let mut rows = conn
            .query(
                "SELECT lease_owner, lease_expires_at FROM task WHERE id = ?1",
                params![task_id],
            )
            .await
            .unwrap();
        let row = rows.next().await.unwrap().unwrap();
        let t_owner: Option<String> = row.get(0).unwrap();
        let t_expires: Option<i64> = row.get(1).unwrap();
        assert!(t_owner.is_none());
        assert!(t_expires.is_none());
    }

    /// Clearing is a no-op (touches nothing, errors on nothing) when there is
    /// nothing to clear.
    #[tokio::test]
    async fn boot_clear_is_a_noop_with_no_leases_held() {
        let (state, _app) = test_app().await;
        let project_id = seed_project(&state).await;
        seed_epic(&state, &project_id, "InProgress").await;

        clear_all_leases(&state.db).await.unwrap();
    }

    // ---- T-550: WorkItem unification — the standalone claim ----
    //
    // Mirrors the T-510 claim/heartbeat/fencing tests above, one table over:
    // every test here proves `claim_task`/`renew_task_lease_once`/
    // `spawn_task_heartbeat` honor the identical rules `claim_epic`/
    // `renew_lease_once`/`spawn_heartbeat` already do, plus the one genuinely
    // new behavior T-550 adds — `claim_next`/`try_claim_and_run` trying an
    // epic before ever falling back to a standalone task. None of these go
    // through `spawn_pool`/`worker_loop`'s continuously-draining loop — see
    // `run_standalone_pipeline_inner`'s own doc for why that would be unsafe
    // against a task this module's pipeline body doesn't yet move out of
    // `InProgress`. `boot_clears_all_leases_on_epic_and_task` above already
    // covers this AC's boot-time-clear clause (true since T-510, unchanged
    // by this task).

    /// Many workers racing the claim SQL against one enqueued standalone
    /// task: exactly one succeeds — the `claim_task` counterpart to
    /// `concurrent_claims_on_one_epic_yield_exactly_one_success`.
    #[tokio::test]
    async fn concurrent_claims_on_one_standalone_task_yield_exactly_one_success() {
        let (state, _app) = test_app().await;
        let project_id = seed_project(&state).await;
        let task_id = seed_standalone_task(&state, &project_id, "Standalone", "InProgress").await;

        let mut handles = Vec::new();
        for i in 0..25 {
            let db = state.db.clone();
            handles.push(tokio::spawn(async move {
                claim_task(db.conn(), &format!("racer-{i}"), 30).await
            }));
        }

        let mut successes = 0;
        let mut winner = None;
        for h in handles {
            if let Ok(Ok(Some(claimed))) = h.await {
                successes += 1;
                winner = Some(claimed.id);
            }
        }
        assert_eq!(
            successes, 1,
            "exactly one racer must claim the standalone task"
        );
        assert_eq!(winner.as_deref(), Some(task_id.as_str()));
    }

    /// An expired standalone-task lease is re-claimable by a new owner — the
    /// `claim_task` counterpart to `expired_lease_is_reclaimable_and_resets_orphaned_tasks`
    /// (minus the orphan-reset clause: a standalone task has no sub-tasks).
    #[tokio::test]
    async fn expired_standalone_lease_is_reclaimable() {
        let (state, _app) = test_app().await;
        let project_id = seed_project(&state).await;
        let task_id = seed_standalone_task(&state, &project_id, "Standalone", "InProgress").await;

        let conn = state.db.conn();
        let past = now_ms() - 60_000;
        conn.execute(
            "UPDATE task SET lease_owner = 'dead-worker', lease_expires_at = ?1 WHERE id = ?2",
            params![past, task_id.clone()],
        )
        .await
        .unwrap();

        let claimed = claim_task(conn, "new-worker", 30).await.unwrap();
        let claimed = claimed.expect("expired standalone lease must be reclaimable");
        assert_eq!(claimed.id, task_id);

        let (owner, _expires) = task_lease(&state, &task_id).await;
        assert_eq!(owner.as_deref(), Some("new-worker"));
    }

    /// A live standalone-task lease is NOT re-claimable — the negative case
    /// alongside the expired-lease test above.
    #[tokio::test]
    async fn live_standalone_lease_is_not_reclaimable() {
        let (state, _app) = test_app().await;
        let project_id = seed_project(&state).await;
        let task_id = seed_standalone_task(&state, &project_id, "Standalone", "InProgress").await;

        let conn = state.db.conn();
        let future = now_ms() + 60_000;
        conn.execute(
            "UPDATE task SET lease_owner = 'alive-worker', lease_expires_at = ?1 WHERE id = ?2",
            params![future, task_id.clone()],
        )
        .await
        .unwrap();

        let claimed = claim_task(conn, "other-worker", 30).await.unwrap();
        assert!(
            claimed.is_none(),
            "a live standalone lease must not be reclaimable"
        );
    }

    /// A standalone task with `epic_id` set (owned by an epic's DAG walk) is
    /// never picked up by `claim_task`, even if some other bug left it
    /// `InProgress` with no lease — the `AND epic_id IS NULL` predicate is
    /// the whole reason `claim_task` exists as a distinct query rather than
    /// `claim_epic`'s WHERE clause with the table swapped.
    #[tokio::test]
    async fn claim_task_never_picks_up_an_epic_owned_task() {
        let (state, _app) = test_app().await;
        let project_id = seed_project(&state).await;
        let epic_id = seed_epic(&state, &project_id, "InProgress").await;
        let a = seed_task(&state, &epic_id, &project_id, "A").await;
        set_task_status(&state, &a, "InProgress").await;

        let claimed = claim_task(state.db.conn(), "worker", 30).await.unwrap();
        assert!(
            claimed.is_none(),
            "an epic-owned task must never satisfy the standalone claim"
        );
    }

    /// A heartbeat renewal against a standalone task's lease already stolen
    /// by another worker reports the loss directly — the `task`-table mirror
    /// of `heartbeat_against_stolen_lease_reports_lost`.
    #[tokio::test]
    async fn standalone_heartbeat_against_stolen_lease_reports_lost() {
        let (state, _app) = test_app().await;
        let project_id = seed_project(&state).await;
        let task_id = seed_standalone_task(&state, &project_id, "Standalone", "InProgress").await;
        let conn = state.db.conn();

        conn.execute(
            "UPDATE task SET lease_owner = 'me', lease_expires_at = ?1 WHERE id = ?2",
            params![now_ms() + 60_000, task_id.clone()],
        )
        .await
        .unwrap();
        conn.execute(
            "UPDATE task SET lease_owner = 'thief', lease_expires_at = ?1 WHERE id = ?2",
            params![now_ms() + 60_000, task_id.clone()],
        )
        .await
        .unwrap();

        let still_held = renew_task_lease_once(conn, &task_id, "me", 30)
            .await
            .unwrap();
        assert!(
            !still_held,
            "renewal against a stolen standalone lease must report 0 rows / lost"
        );

        let (owner, _) = task_lease(&state, &task_id).await;
        assert_eq!(owner.as_deref(), Some("thief"));
    }

    /// A live standalone-task lease renews successfully — the positive case.
    #[tokio::test]
    async fn standalone_heartbeat_against_live_lease_succeeds() {
        let (state, _app) = test_app().await;
        let project_id = seed_project(&state).await;
        let task_id = seed_standalone_task(&state, &project_id, "Standalone", "InProgress").await;
        let conn = state.db.conn();
        conn.execute(
            "UPDATE task SET lease_owner = 'me', lease_expires_at = ?1 WHERE id = ?2",
            params![now_ms() + 60_000, task_id.clone()],
        )
        .await
        .unwrap();

        let still_held = renew_task_lease_once(conn, &task_id, "me", 30)
            .await
            .unwrap();
        assert!(still_held);
    }

    /// End-to-end wiring of `spawn_task_heartbeat` + `LeaseHandle`: a stolen
    /// standalone-task lease flips the shared handle to lost within one
    /// heartbeat period — the `task`-table mirror of
    /// `spawn_heartbeat_flags_lease_handle_lost_on_theft`.
    #[tokio::test]
    async fn spawn_task_heartbeat_flags_lease_handle_lost_on_theft() {
        let (state, _app) = test_app().await;
        let project_id = seed_project(&state).await;
        let task_id = seed_standalone_task(&state, &project_id, "Standalone", "InProgress").await;
        let conn = state.db.conn();
        conn.execute(
            "UPDATE task SET lease_owner = 'me', lease_expires_at = ?1 WHERE id = ?2",
            params![now_ms() + 60_000, task_id.clone()],
        )
        .await
        .unwrap();

        let lease = LeaseHandle::new();
        let handle = spawn_task_heartbeat(
            state.db.conn().clone(),
            task_id.clone(),
            "me".to_string(),
            Duration::from_millis(15),
            30,
            lease.clone(),
            state.cancel_registry.clone(),
        );

        conn.execute(
            "UPDATE task SET lease_owner = 'thief' WHERE id = ?1",
            params![task_id.clone()],
        )
        .await
        .unwrap();

        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        loop {
            if lease.is_lost() {
                break;
            }
            if tokio::time::Instant::now() >= deadline {
                panic!("standalone heartbeat never observed the stolen lease");
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }

        handle.abort();
    }

    // ---- T-550: epic claims tried first ----

    /// `claim_next` with both an `InProgress` epic and an `InProgress`
    /// standalone task queued must return the epic — the literal AC clause
    /// "epic claims are tried first so standalone work never starves an
    /// epic". Run many times (fresh state each time is unnecessary; the
    /// claim is deterministic, not racy, since only one call happens) is
    /// unnecessary — `claim_epic`'s own SELECT ... LIMIT 1 always wins ties
    /// deterministically by `updated_at`, and this test seeds only one of
    /// each, so a single call already proves the order.
    #[tokio::test]
    async fn claim_next_prefers_a_queued_epic_over_a_queued_standalone_task() {
        let (state, _app) = test_app().await;
        let project_id = seed_project(&state).await;
        let epic_id = seed_epic(&state, &project_id, "InProgress").await;
        let task_id = seed_standalone_task(&state, &project_id, "Standalone", "InProgress").await;

        let claimed = claim_next(state.db.conn(), "worker", 30)
            .await
            .unwrap()
            .expect("something must be claimable");
        match claimed {
            WorkItem::Epic(c) => assert_eq!(c.id, epic_id),
            WorkItem::Standalone(c) => panic!(
                "expected the queued epic {epic_id} to be claimed first, got standalone task {}",
                c.id
            ),
        }

        // The standalone task must still be untouched — genuinely not
        // starved, not just "not returned this one time".
        let (owner, _) = task_lease(&state, &task_id).await;
        assert!(
            owner.is_none(),
            "the standalone task must not have been claimed at all"
        );
    }

    /// With no epic queued, `claim_next` falls back to the standalone task —
    /// the fallback half of the same AC clause.
    #[tokio::test]
    async fn claim_next_falls_back_to_a_standalone_task_when_no_epic_is_queued() {
        let (state, _app) = test_app().await;
        let project_id = seed_project(&state).await;
        let task_id = seed_standalone_task(&state, &project_id, "Standalone", "InProgress").await;

        let claimed = claim_next(state.db.conn(), "worker", 30)
            .await
            .unwrap()
            .expect("the standalone task must be claimable");
        match claimed {
            WorkItem::Standalone(c) => assert_eq!(c.id, task_id),
            WorkItem::Epic(c) => panic!("expected the standalone task, got epic {}", c.id),
        }
    }

    /// With nothing queued at all, `claim_next` returns `None` — the same
    /// "empty queue" contract `claim_epic` alone already had.
    #[tokio::test]
    async fn claim_next_returns_none_when_nothing_is_queued() {
        let (state, _app) = test_app().await;
        assert!(claim_next(state.db.conn(), "worker", 30)
            .await
            .unwrap()
            .is_none());
    }

    /// `try_claim_and_run` itself — not just `claim_next` in isolation —
    /// prefers a queued epic over a queued standalone task. A single,
    /// bounded call (never `worker_loop`'s continuously-draining inner loop —
    /// see `run_standalone_pipeline_inner`'s doc for why that distinction
    /// matters): the epic here has no ready task, so `run_claimed_epic`'s own
    /// walk finalizes it as `InReview` immediately, proving the *real*
    /// dispatch function — the one `worker_loop` actually calls — makes the
    /// same choice `claim_next`'s own direct test already proved the query
    /// makes.
    #[tokio::test]
    async fn try_claim_and_run_prefers_the_epic_end_to_end() {
        let (state, _app) = test_app().await;
        let fixture = GitFixture::new().await;
        let project_id = seed_project_with_workspace(&state, &fixture).await;
        let epic_id = seed_epic(&state, &project_id, "InProgress").await;
        let task_id = seed_standalone_task(&state, &project_id, "Standalone", "InProgress").await;

        try_claim_and_run(&state, "worker").await;

        // The epic (no tasks at all) went straight to InReview via
        // finalize_epic; the standalone task was never touched at all — not
        // claimed (no lease) and not even re-fetched into `InProgress`'s
        // sibling states, still exactly the `InProgress` this test seeded.
        assert_eq!(epic_status(&state, &epic_id).await, "InReview");
        let (owner, _) = task_lease(&state, &task_id).await;
        assert!(
            owner.is_none(),
            "the standalone task must not have been claimed"
        );
        assert_eq!(single_task_status(&state, &task_id).await, "InProgress");
        cleanup_clone_root(&state, &project_id, &[&epic_id]);
    }

    /// `try_claim_and_run` claims and runs the standalone-task branch when
    /// that is the only thing queued: the lease is taken, held for the real
    /// (T-551) pipeline body, and released again — proving
    /// `run_claimed_standalone`'s wiring end to end, not just its pieces in
    /// isolation. `seed_project` (not `seed_project_with_workspace`) is
    /// deliberate here: this test is about the claim/lease lifecycle, not the
    /// pipeline's happy path (that's the dedicated T-551 tests further down),
    /// so a project with no `clone_path` is exactly right — `run_standalone_pipeline_inner`
    /// still runs for real now, fails provisioning immediately
    /// (`workspace_error`, no task ever "at fault" for lacking a clone), and
    /// the claim lifecycle must complete (lease released) regardless.
    #[tokio::test]
    async fn try_claim_and_run_claims_and_releases_a_standalone_task() {
        let (state, _app) = test_app().await;
        let project_id = seed_project(&state).await;
        let task_id = seed_standalone_task(&state, &project_id, "Standalone", "InProgress").await;

        try_claim_and_run(&state, "worker").await;

        // The claim lifecycle must be exact regardless of what the pipeline
        // body itself does: leased, then released, on the way through
        // `run_claimed_standalone`.
        let (owner, expires) = task_lease(&state, &task_id).await;
        assert!(
            owner.is_none(),
            "the lease must be released once the body returns"
        );
        assert!(expires.is_none());

        // The pipeline body is real now (T-551): a project with no
        // `clone_path` fails provisioning, which fails the task itself (no
        // epic to Block) — proving this isn't a no-op stub anymore.
        let task = fetch_task_row(&state, &task_id).await;
        assert_eq!(task.0, "Failed");
        assert_eq!(task.1.as_deref(), Some("workspace_error"));
    }

    // ---- T-510: pool concurrency ----

    /// A tiny async-friendly gate a test can hold N pipeline-body calls behind
    /// until it has observed the concurrency it wants, then release them all.
    /// Mirrors `planning::testing::Gate`'s one-shot-release shape but async
    /// (the pipeline body runs on the tokio runtime, not a blocking thread),
    /// using the standard check-register-check `Notify` pattern to avoid a
    /// missed-wakeup race between the `released` check and `notified().await`.
    struct ConcurrencyGate {
        active: AtomicUsize,
        released: std::sync::atomic::AtomicBool,
        notify: tokio::sync::Notify,
    }

    impl ConcurrencyGate {
        fn new() -> Arc<ConcurrencyGate> {
            Arc::new(ConcurrencyGate {
                active: AtomicUsize::new(0),
                released: std::sync::atomic::AtomicBool::new(false),
                notify: tokio::sync::Notify::new(),
            })
        }

        fn active(&self) -> usize {
            self.active.load(AtomicOrdering::SeqCst)
        }

        async fn enter(&self) {
            self.active.fetch_add(1, AtomicOrdering::SeqCst);
            loop {
                if self.released.load(AtomicOrdering::SeqCst) {
                    break;
                }
                let notified = self.notify.notified();
                if self.released.load(AtomicOrdering::SeqCst) {
                    break;
                }
                notified.await;
            }
            self.active.fetch_sub(1, AtomicOrdering::SeqCst);
        }

        fn release(&self) {
            self.released.store(true, AtomicOrdering::SeqCst);
            self.notify.notify_waiters();
        }
    }

    /// With `worker_concurrency = 2` and 3 enqueued (InProgress, unleased)
    /// epics, exactly 2 run concurrently: the pool only ever has 2 worker
    /// loops, so at most 2 claims can be outstanding at once. Proven
    /// deterministically (no sleeps) via the T-510 test-only pipeline hook:
    /// each claimed epic's body blocks in `ConcurrencyGate::enter` until the
    /// test releases it, so the test can poll (bounded) until exactly 2 are
    /// blocked, assert the 3rd epic is still unclaimed, then release and
    /// confirm all 3 eventually complete.
    #[tokio::test]
    async fn pool_runs_exactly_worker_concurrency_epics_at_once() {
        let db = Db::connect(":memory:").await.unwrap();
        db.run_migrations().await.unwrap();
        let mut config = Config::for_test();
        config.executor.worker_concurrency = 2;
        let state = AppState::with_all_agents_and_host(
            config,
            db,
            Arc::new(SilentPlanningAgent),
            Arc::new(SilentBreakdownAgent),
            Arc::new(ScriptedTaskAgent::new()),
            Arc::new(FakeHost::new()),
        );

        let fixture = GitFixture::new().await;
        let project_id = seed_project_with_workspace(&state, &fixture).await;
        let epic_a = seed_epic(&state, &project_id, "InProgress").await;
        let epic_b = seed_epic(&state, &project_id, "InProgress").await;
        let epic_c = seed_epic(&state, &project_id, "InProgress").await;
        for epic_id in [&epic_a, &epic_b, &epic_c] {
            seed_task(&state, epic_id, &project_id, "A").await;
        }

        let gate = ConcurrencyGate::new();
        let hook_gate = gate.clone();
        let state = state.with_pipeline_hook(Arc::new(move || {
            let gate = hook_gate.clone();
            Box::pin(async move { gate.enter().await })
        }));

        let _handles = spawn_pool(state.clone());
        state.notify.notify_waiters();

        // Bounded poll: wait until exactly 2 claimed-epic bodies are blocked
        // in the gate. With only 2 worker loops this is a ceiling, not a
        // race — the 3rd loop simply doesn't exist.
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        loop {
            if gate.active() == 2 {
                break;
            }
            if tokio::time::Instant::now() >= deadline {
                panic!(
                    "pool never reached 2 concurrently-claimed epics (active={})",
                    gate.active()
                );
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        assert_eq!(gate.active(), 2, "must not exceed worker_concurrency");

        // The 3rd epic must still be unclaimed — no 3rd worker loop exists to
        // claim it while the other two are held in the gate.
        let (c_owner, _) = epic_lease(&state, &epic_c).await;
        assert!(
            c_owner.is_none(),
            "a 3rd epic must remain unclaimed while worker_concurrency=2 workers are busy"
        );

        gate.release();

        // All 3 epics eventually reach InReview (bounded poll; the released
        // bodies run their tasks to Done, then T-514's finalize step pushes
        // + opens a (faked) PR and flips each epic to InReview — the freed
        // workers pick up the 3rd along the way).
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        loop {
            let statuses = (
                epic_status(&state, &epic_a).await,
                epic_status(&state, &epic_b).await,
                epic_status(&state, &epic_c).await,
            );
            if statuses.0 == "InReview" && statuses.1 == "InReview" && statuses.2 == "InReview" {
                break;
            }
            if tokio::time::Instant::now() >= deadline {
                panic!("not all epics reached InReview in time: {statuses:?}");
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }

        for epic_id in [&epic_a, &epic_b, &epic_c] {
            let statuses = task_statuses(&state, epic_id).await;
            assert_eq!(statuses["A"], "Done");
        }

        cleanup_clone_root(&state, &project_id, &[&epic_a, &epic_b, &epic_c]);
    }

    // ---- end-to-end AC test via the lane endpoint + pool ----

    /// Enqueue writes the contract shape: hitting `POST /epics/:id/lane
    /// { status: "InProgress" }` on a Ready epic with a task, with a worker
    /// pool running, drives the DAG to Done and then (T-514) all the way to
    /// `InReview` — push (real, local, via `FakeHost::push` delegating to
    /// `git::push_branch`) + a faked PR, `pr_url`/`pr_number` persisted and
    /// returned by `GET /epics/{id}`, and the workspace retained. This is the
    /// full happy-path end-to-end proof MILESTONE_2 T-514's AC asks for
    /// (`ScriptedTaskAgent` + `FakeHost` + the local git fixture, enqueue all
    /// the way to a retained InReview workspace), plus the dedicated proof
    /// that the re-claim spin T-513's module doc flagged is now closed: an
    /// `InReview` epic is never claimable again, and a fresh pool notify
    /// leaves it alone (see also `completed_epic_is_never_reclaimable` below
    /// for the minimal, pipeline-independent version of the same claim).
    #[tokio::test]
    async fn enqueue_via_lane_drives_dag_to_done_and_completes_with_pr() {
        let (state, app) = test_app().await;
        let fixture = GitFixture::new().await;
        let project_id = seed_project_with_workspace(&state, &fixture).await;
        let epic_id = seed_epic(&state, &project_id, "Ready").await;
        let a = seed_task(&state, &epic_id, &project_id, "A").await;
        let b = seed_task(&state, &epic_id, &project_id, "B").await;
        link(&state, &a, &b).await; // A → B.

        let workspace_path = workspace::epic_workspace_path(&state.config.clone_root, &epic_id);

        // Start the pool (T-510): the lane handler no longer spawns anything
        // itself, so a pool must be running to consume the enqueue+notify.
        let _handles = spawn_pool(state.clone());

        // Hit the lane endpoint — enqueues + notifies; the pool claims it.
        let response = app
            .clone()
            .oneshot(req(
                "POST",
                &format!("/epics/{epic_id}/lane"),
                Some(json!({ "status": "InProgress" })),
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(body_json(response).await["status"], "InProgress");

        // Poll (bounded) until the epic reaches InReview — finalize runs
        // strictly after the DAG's last task-status write, in the same
        // pipeline body, so bounding on the epic's own terminal status (not
        // just the tasks') is what actually proves finalize ran.
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        loop {
            if epic_status(&state, &epic_id).await == "InReview" {
                break;
            }
            if tokio::time::Instant::now() >= deadline {
                panic!(
                    "worker pool never completed the epic in time: status={}, tasks={:?}",
                    epic_status(&state, &epic_id).await,
                    task_statuses(&state, &epic_id).await,
                );
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        let statuses = task_statuses(&state, &epic_id).await;
        assert_eq!(statuses["A"], "Done");
        assert_eq!(statuses["B"], "Done");

        // pr_url/pr_number persisted and returned by GET /epics/{id}.
        let get_response = app
            .clone()
            .oneshot(req("GET", &format!("/epics/{epic_id}"), None))
            .await
            .unwrap();
        assert_eq!(get_response.status(), StatusCode::OK);
        let epic_body = body_json(get_response).await;
        assert_eq!(epic_body["status"], "InReview");
        assert!(epic_body["pr_url"]
            .as_str()
            .expect("pr_url must be persisted and returned")
            .starts_with("https://"));
        assert!(
            epic_body["pr_number"].as_i64().is_some(),
            "pr_number must be persisted and returned"
        );

        // The workspace is retained (never deleted) once the PR opens — the
        // post-PR-review loop needs the branch for feedback rounds, so
        // finalize deliberately leaves it on disk.
        assert!(
            workspace_path.join(".git").exists(),
            "the workspace must be retained (not deleted) after finalize lands the epic in InReview"
        );

        // ---- the re-claim spin T-513 left behind is closed (T-514) ----
        //
        // T-513's module doc flagged this explicitly: a fully-Done-but-
        // still-InProgress epic would remain claimable, so the pool would
        // re-claim and re-walk it in a tight loop forever. Now that the epic
        // is InReview, `claim_epic`'s own predicate (`status = 'InProgress'`)
        // excludes it — proven directly, then again by observing the live
        // pool leave it untouched across a fresh notify.
        let direct_claim = claim_epic(state.db.conn(), "re-claim-prober", 30)
            .await
            .unwrap();
        assert!(
            direct_claim.is_none(),
            "an InReview epic must never be claimable again"
        );

        state.notify.notify_waiters();
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(
            epic_status(&state, &epic_id).await,
            "InReview",
            "an InReview epic must not be disturbed by a fresh pool notify"
        );
        let (lease_owner, lease_expires_at) = epic_lease(&state, &epic_id).await;
        assert!(
            lease_owner.is_none(),
            "an InReview epic must never hold a lease"
        );
        assert!(lease_expires_at.is_none());

        cleanup_clone_root(&state, &project_id, &[&epic_id]);
    }

    /// The minimal, pipeline-independent version of the same regression: a
    /// `Completed` epic (seeded directly, however it got there) is never
    /// claimable. See `enqueue_via_lane_drives_dag_to_done_and_completes_with_pr`
    /// above for the full pipeline-driven proof.
    #[tokio::test]
    async fn completed_epic_is_never_reclaimable() {
        let (state, _app) = test_app().await;
        let project_id = seed_project(&state).await;
        seed_epic(&state, &project_id, "Completed").await;

        let claimed = claim_epic(state.db.conn(), "prober", 30).await.unwrap();
        assert!(
            claimed.is_none(),
            "a Completed epic must never be claimable"
        );
    }

    // ---- T-511: provisioning-failure wiring (workspace_error / setup_failed) ----

    /// A project whose repo is unreachable (mirrors `git.rs`'s own bad-url
    /// fixture): the canonical refresh inside provisioning fails fast
    /// (`GIT_TERMINAL_PROMPT=0`), forcing `ProvisionFailure::Workspace`.
    async fn seed_project_bad_repo(state: &AppState) -> String {
        let conn = state.db.conn();
        let id = ulid::Ulid::new().to_string();
        let now = now_ms();
        let clone_path = std::path::Path::new(&state.config.clone_root).join(&id);
        conn.execute(
            "INSERT INTO project (id, name, repo_url, clone_path, clone_status, created_at, updated_at) \
             VALUES (?1, 'P', 'https://dearborn.invalid/nope/nope.git', ?2, 'ready', ?3, ?3)",
            params![id.clone(), clone_path.to_string_lossy().to_string(), now],
        )
        .await
        .unwrap();
        id
    }

    /// Like [`seed_project_with_workspace`] but with a `setup_cmd`, so a
    /// provisioned workspace's setup step can be made to fail on demand.
    async fn seed_project_with_setup_cmd(
        state: &AppState,
        fixture: &GitFixture,
        setup_cmd: &str,
    ) -> String {
        let conn = state.db.conn();
        let id = ulid::Ulid::new().to_string();
        let now = now_ms();
        let clone_path = std::path::Path::new(&state.config.clone_root).join(&id);
        conn.execute(
            "INSERT INTO project (id, name, repo_url, setup_cmd, clone_path, clone_status, created_at, updated_at) \
             VALUES (?1, 'P', ?2, ?3, ?4, 'ready', ?5, ?5)",
            params![
                id.clone(),
                fixture.path_str(),
                setup_cmd,
                clone_path.to_string_lossy().to_string(),
                now
            ],
        )
        .await
        .unwrap();
        id
    }

    /// Like [`seed_project_with_workspace`] but with a `test_cmd`, so T-521's
    /// preflight gate has something to run against a real (local, hermetic)
    /// workspace.
    async fn seed_project_with_test_cmd(
        state: &AppState,
        fixture: &GitFixture,
        test_cmd: &str,
    ) -> String {
        let conn = state.db.conn();
        let id = ulid::Ulid::new().to_string();
        let now = now_ms();
        let clone_path = std::path::Path::new(&state.config.clone_root).join(&id);
        conn.execute(
            "INSERT INTO project (id, name, repo_url, test_cmd, clone_path, clone_status, created_at, updated_at) \
             VALUES (?1, 'P', ?2, ?3, ?4, 'ready', ?5, ?5)",
            params![
                id.clone(),
                fixture.path_str(),
                test_cmd,
                clone_path.to_string_lossy().to_string(),
                now
            ],
        )
        .await
        .unwrap();
        id
    }

    /// Drain `sub` (bounded) until an `epic_updated` frame carrying
    /// `status` matches, or panic after 5s. Draining rather than asserting a
    /// fixed frame position keeps this robust against the lane handler's own
    /// `Ready → InProgress` `epic_updated`/`board_updated` publishes landing
    /// on the same subscriber ahead of the provisioning-failure ones.
    async fn recv_epic_updated_with_status(
        sub: &mut tokio::sync::broadcast::Receiver<crate::hub::Envelope>,
        status: &str,
    ) -> Value {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        loop {
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            let frame = tokio::time::timeout(remaining, sub.recv())
                .await
                .unwrap_or_else(|_| panic!("never saw epic_updated(status={status})"))
                .unwrap();
            let v: Value = serde_json::from_str(&frame).unwrap();
            if v["type"] == "epic_updated" && v["payload"]["status"] == status {
                return v;
            }
        }
    }

    /// A workspace-provisioning failure (unreachable repo) drives the epic to
    /// `Blocked(workspace_error)`: the lease is released, the seeded task
    /// never leaves `Todo` (the stub DAG walk never runs), and both the
    /// `epic_updated` and `board_updated` frames land.
    #[tokio::test]
    async fn workspace_error_blocks_epic_releases_lease_and_publishes() {
        let (state, app) = test_app().await;
        let project_id = seed_project_bad_repo(&state).await;
        let epic_id = seed_epic(&state, &project_id, "Ready").await;
        seed_task(&state, &epic_id, &project_id, "A").await;

        let mut epic_sub = state.hub.subscribe(&format!("epic:{epic_id}"));
        let mut proj_sub = state.hub.subscribe(&format!("project:{project_id}"));

        let _handles = spawn_pool(state.clone());
        let response = app
            .oneshot(req(
                "POST",
                &format!("/epics/{epic_id}/lane"),
                Some(json!({ "status": "InProgress" })),
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let blocked_frame = recv_epic_updated_with_status(&mut epic_sub, "Blocked").await;
        assert_eq!(
            blocked_frame["payload"]["blocked_reason"],
            "workspace_error"
        );

        // board_updated must have landed too (either for the InProgress
        // enqueue, the Blocked transition, or both) — drain for one.
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        loop {
            let frame = tokio::time::timeout(Duration::from_secs(5), proj_sub.recv())
                .await
                .expect("never saw a board_updated frame")
                .unwrap();
            let v: Value = serde_json::from_str(&frame).unwrap();
            if v["type"] == "board_updated" {
                break;
            }
            if tokio::time::Instant::now() >= deadline {
                panic!("never saw board_updated");
            }
        }

        let epic = fetch_epic(state.db.conn(), &epic_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(epic.status, "Blocked");
        assert_eq!(epic.blocked_reason.as_deref(), Some("workspace_error"));

        let (lease_owner, lease_expires_at) = epic_lease(&state, &epic_id).await;
        assert!(lease_owner.is_none(), "lease must be released on Blocked");
        assert!(lease_expires_at.is_none());

        // The DAG walk never ran: the seeded task is still Todo.
        let statuses = task_statuses(&state, &epic_id).await;
        assert_eq!(statuses["A"], "Todo");
    }

    /// A failing `setup_cmd` drives the epic to `Blocked(setup_failed)` with
    /// the workspace retained on disk (never deleted) and the captured
    /// output landed in an `agent_run` row.
    #[tokio::test]
    async fn setup_cmd_failure_blocks_epic_and_retains_workspace() {
        let (state, app) = test_app().await;
        let fixture = GitFixture::new().await;
        let project_id =
            seed_project_with_setup_cmd(&state, &fixture, "echo setup-boom && exit 5").await;
        let epic_id = seed_epic(&state, &project_id, "Ready").await;
        seed_task(&state, &epic_id, &project_id, "A").await;

        let _handles = spawn_pool(state.clone());
        let response = app
            .oneshot(req(
                "POST",
                &format!("/epics/{epic_id}/lane"),
                Some(json!({ "status": "InProgress" })),
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        loop {
            if epic_status(&state, &epic_id).await == "Blocked" {
                break;
            }
            if tokio::time::Instant::now() >= deadline {
                panic!("epic never reached Blocked");
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        let epic = fetch_epic(state.db.conn(), &epic_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(epic.blocked_reason.as_deref(), Some("setup_failed"));

        // Workspace retained: the provisioned directory (and its .git) is
        // still on disk, not deleted on this failure path.
        let workspace_path =
            crate::workspace::epic_workspace_path(&state.config.clone_root, &epic_id);
        assert!(
            workspace_path.join(".git").exists(),
            "workspace must be retained on setup_failed"
        );

        // Evidence: the captured setup_cmd output landed in agent_run.
        let mut rows = state
            .db
            .conn()
            .query(
                "SELECT status, exit_code, log FROM agent_run WHERE epic_id = ?1 AND stage = 'setup'",
                params![epic_id.clone()],
            )
            .await
            .unwrap();
        let row = rows.next().await.unwrap().expect("a setup agent_run row");
        assert_eq!(row.get::<String>(0).unwrap(), "error");
        assert_eq!(row.get::<Option<i64>>(1).unwrap(), Some(5));
        let log: String = row.get(2).unwrap();
        assert!(log.contains("setup-boom"));

        cleanup_clone_root(&state, &project_id, &[&epic_id]);
    }

    // ---- T-521: the preflight gate --------------------------------------------

    /// The headline AC: a red `test_cmd` on the untouched tree blocks the
    /// epic with `preflight_red`, the output lands in evidence, the
    /// workspace is retained, and — the part that actually matters — the
    /// implement stage is **never** invoked. A bare `ScriptedTaskAgent`'s
    /// `recorded()` list staying empty is the proof: if the walk had ever
    /// reached `Stage::Implement` for either seeded task, this would record
    /// at least one entry.
    #[tokio::test]
    async fn red_preflight_blocks_epic_and_spawns_no_agent() {
        let agent = Arc::new(ScriptedTaskAgent::new());
        let recorded = agent.recorded();
        let (state, _app) = test_app_with_task_agent(agent.clone()).await;
        let fixture = GitFixture::new().await;
        let project_id =
            seed_project_with_test_cmd(&state, &fixture, "echo tree-is-red && exit 1").await;
        let epic_id = seed_epic(&state, &project_id, "InProgress").await;
        seed_task(&state, &epic_id, &project_id, "A").await;
        seed_task(&state, &epic_id, &project_id, "B").await;

        run_epic_pipeline(state.clone(), epic_id.clone()).await;

        let epic = fetch_epic(state.db.conn(), &epic_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(epic.status, "Blocked");
        assert_eq!(epic.blocked_reason.as_deref(), Some("preflight_red"));

        // The important assertion: no agent stage was ever spawned.
        assert_eq!(
            recorded.lock().unwrap().len(),
            0,
            "a red preflight must never reach Stage::Implement"
        );

        // No task ever left Todo — the walk never got that far.
        let statuses = task_statuses(&state, &epic_id).await;
        assert_eq!(statuses["A"], "Todo");
        assert_eq!(statuses["B"], "Todo");

        // Preflight evidence: the red output landed in agent_run.
        let mut rows = state
            .db
            .conn()
            .query(
                "SELECT status, exit_code, log FROM agent_run WHERE epic_id = ?1 AND stage = 'preflight'",
                params![epic_id.clone()],
            )
            .await
            .unwrap();
        let row = rows
            .next()
            .await
            .unwrap()
            .expect("a preflight agent_run row");
        assert_eq!(row.get::<String>(0).unwrap(), "error");
        assert_eq!(row.get::<Option<i64>>(1).unwrap(), Some(1));
        let log: String = row.get(2).unwrap();
        assert!(log.contains("tree-is-red"));

        // Workspace retained: the provisioned directory is still on disk.
        let workspace_path =
            crate::workspace::epic_workspace_path(&state.config.clone_root, &epic_id);
        assert!(
            workspace_path.join(".git").exists(),
            "workspace must be retained on preflight_red"
        );

        // Lease released, matching every other Blocked path.
        let (lease_owner, lease_expires_at) = epic_lease(&state, &epic_id).await;
        assert!(lease_owner.is_none());
        assert!(lease_expires_at.is_none());

        cleanup_clone_root(&state, &project_id, &[&epic_id]);
    }

    /// A green preflight is a no-op from the walk's point of view: the epic
    /// still reaches `InReview` through the full pipeline (workspace →
    /// preflight → implement → commit → push → PR), and the preflight
    /// `agent_run` row records `status = "ok"`.
    #[tokio::test]
    async fn green_preflight_proceeds_to_completed() {
        let (state, _app) = test_app().await;
        let fixture = GitFixture::new().await;
        let project_id = seed_project_with_test_cmd(&state, &fixture, "true").await;
        let epic_id = seed_epic(&state, &project_id, "InProgress").await;
        seed_task(&state, &epic_id, &project_id, "A").await;

        run_epic_pipeline(state.clone(), epic_id.clone()).await;

        let epic = fetch_epic(state.db.conn(), &epic_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            epic.status, "InReview",
            "a green preflight must let the rest of the pipeline run to completion"
        );

        let mut rows = state
            .db
            .conn()
            .query(
                "SELECT status FROM agent_run WHERE epic_id = ?1 AND stage = 'preflight'",
                params![epic_id.clone()],
            )
            .await
            .unwrap();
        let row = rows
            .next()
            .await
            .unwrap()
            .expect("a preflight agent_run row");
        assert_eq!(row.get::<String>(0).unwrap(), "ok");

        cleanup_clone_root(&state, &project_id, &[&epic_id]);
    }

    /// No `test_cmd` configured ⇒ the gate is skipped silently: zero
    /// `preflight` rows, and the walk proceeds exactly as if T-521 did not
    /// exist (mirrors T-520's `StageOutcome::Skipped` contract, just proven
    /// from the pipeline's side rather than `cmd.rs`'s own unit tests).
    #[tokio::test]
    async fn absent_test_cmd_skips_preflight_silently() {
        let (state, _app) = test_app().await;
        let fixture = GitFixture::new().await;
        let project_id = seed_project_with_workspace(&state, &fixture).await; // no test_cmd
        let epic_id = seed_epic(&state, &project_id, "InProgress").await;
        seed_task(&state, &epic_id, &project_id, "A").await;

        run_epic_pipeline(state.clone(), epic_id.clone()).await;

        let epic = fetch_epic(state.db.conn(), &epic_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(epic.status, "InReview");

        let mut rows = state
            .db
            .conn()
            .query(
                "SELECT COUNT(*) FROM agent_run WHERE epic_id = ?1 AND stage = 'preflight'",
                params![epic_id.clone()],
            )
            .await
            .unwrap();
        let count: i64 = rows.next().await.unwrap().unwrap().get(0).unwrap();
        assert_eq!(
            count, 0,
            "an absent test_cmd must record zero preflight rows"
        );

        cleanup_clone_root(&state, &project_id, &[&epic_id]);
    }

    /// The epic card's `blocked_reason` (T-500's DTO field) actually reaches
    /// both `GET /epics/{id}` and the project board payload once a red
    /// preflight has blocked the epic — the client rendering of it is
    /// T-561's job, out of scope here, but the data has to be there for it.
    #[tokio::test]
    async fn blocked_epic_surfaces_reason_through_epic_api_and_board() {
        let (state, app) = test_app().await;
        let fixture = GitFixture::new().await;
        let project_id = seed_project_with_test_cmd(&state, &fixture, "exit 1").await;
        let epic_id = seed_epic(&state, &project_id, "InProgress").await;
        seed_task(&state, &epic_id, &project_id, "A").await;

        run_epic_pipeline(state.clone(), epic_id.clone()).await;

        let epic_response = app
            .clone()
            .oneshot(req("GET", &format!("/epics/{epic_id}"), None))
            .await
            .unwrap();
        assert_eq!(epic_response.status(), StatusCode::OK);
        let epic_body = body_json(epic_response).await;
        assert_eq!(epic_body["blocked_reason"], "preflight_red");

        let board_response = app
            .oneshot(req("GET", &format!("/projects/{project_id}/board"), None))
            .await
            .unwrap();
        assert_eq!(board_response.status(), StatusCode::OK);
        let board_body = body_json(board_response).await;
        let epic_on_board = board_body["epics"]
            .as_array()
            .unwrap()
            .iter()
            .find(|e| e["id"] == epic_id)
            .expect("the blocked epic must be on the board");
        assert_eq!(epic_on_board["blocked_reason"], "preflight_red");

        cleanup_clone_root(&state, &project_id, &[&epic_id]);
    }

    /// `run_preflight` is called once per invocation of
    /// `run_epic_pipeline_inner` (i.e. once per claim), never once per
    /// task — a multi-task epic that completes still has exactly one
    /// `preflight` row, even though its DAG walk processes two tasks.
    #[tokio::test]
    async fn preflight_runs_exactly_once_per_claim_not_once_per_task() {
        let (state, _app) = test_app().await;
        let fixture = GitFixture::new().await;
        let project_id = seed_project_with_test_cmd(&state, &fixture, "true").await;
        let epic_id = seed_epic(&state, &project_id, "InProgress").await;
        let a = seed_task(&state, &epic_id, &project_id, "A").await;
        let b = seed_task(&state, &epic_id, &project_id, "B").await;
        link(&state, &a, &b).await;

        run_epic_pipeline(state.clone(), epic_id.clone()).await;

        let statuses = task_statuses(&state, &epic_id).await;
        assert_eq!(statuses["A"], "Done");
        assert_eq!(statuses["B"], "Done");

        let mut rows = state
            .db
            .conn()
            .query(
                "SELECT COUNT(*) FROM agent_run WHERE epic_id = ?1 AND stage = 'preflight'",
                params![epic_id.clone()],
            )
            .await
            .unwrap();
        let count: i64 = rows.next().await.unwrap().unwrap().get(0).unwrap();
        assert_eq!(
            count, 1,
            "preflight must run once per claim, not once per task"
        );

        cleanup_clone_root(&state, &project_id, &[&epic_id]);
    }

    // ---- T-513: the real implement walk's commit machinery -------------------

    /// Read `git log`'s subjects, oldest first, in `dir`. Used to prove the
    /// walk's commit *order* and *subjects* directly from git itself rather
    /// than trusting the DB's task-status transitions alone.
    async fn git_log_subjects(dir: &std::path::Path) -> Vec<String> {
        git_log_subjects_for_ref(dir, "HEAD").await
    }

    /// Like [`git_log_subjects`] but against an explicit ref. Since T-514,
    /// finalize pushes the branch (and keeps the workspace for feedback
    /// rounds), so a test that wants the exact commits (subjects, order, SHA)
    /// reads them back from where the push landed — the `GitFixture`'s own
    /// directory, which doubles as the project's `repo_url`/canonical
    /// checkout/origin all at once in these tests — on the epic's own branch.
    async fn git_log_subjects_for_ref(dir: &std::path::Path, git_ref: &str) -> Vec<String> {
        let output = tokio::process::Command::new("git")
            .args(["log", "--reverse", "--format=%s", git_ref])
            .current_dir(dir)
            .output()
            .await
            .unwrap();
        assert!(output.status.success(), "git log failed: {output:?}");
        String::from_utf8_lossy(&output.stdout)
            .lines()
            .map(|s| s.to_string())
            .collect()
    }

    /// `git rev-parse <git_ref>` in `dir`, trimmed — used the same way
    /// [`git_log_subjects_for_ref`] is: reading a commit SHA back from the
    /// fixture on the epic branch where the push landed.
    async fn git_rev_parse(dir: &std::path::Path, git_ref: &str) -> String {
        let output = tokio::process::Command::new("git")
            .args(["rev-parse", git_ref])
            .current_dir(dir)
            .output()
            .await
            .unwrap();
        assert!(output.status.success(), "git rev-parse failed: {output:?}");
        String::from_utf8_lossy(&output.stdout).trim().to_string()
    }

    /// Read back the `branch_name` T-511's provisioning persisted on the
    /// epic row — needed (post-T-514) to look up commits on the pushed
    /// branch once the workspace itself is deleted.
    async fn epic_branch_name_column(state: &AppState, epic_id: &str) -> String {
        let mut rows = state
            .db
            .conn()
            .query(
                "SELECT branch_name FROM epic WHERE id = ?1",
                params![epic_id],
            )
            .await
            .unwrap();
        let row = rows.next().await.unwrap().unwrap();
        row.get::<Option<String>>(0)
            .unwrap()
            .expect("branch_name must be persisted by provisioning")
    }

    fn writes_file(path: &str, content: &str) -> ScriptedRun {
        ScriptedRun {
            files: vec![(PathBuf::from(path), content.to_string())],
            ..ScriptedRun::default()
        }
    }

    /// A linear DAG (A → B → C) with a `ScriptedTaskAgent` that writes a
    /// distinct file per task: exactly one commit lands per task, each with
    /// the §2.8 subject `impl(<short task id>): <title>`, in dependency
    /// order — read directly out of `git log`, not just inferred from task
    /// statuses.
    #[tokio::test]
    async fn implement_writes_produce_one_commit_per_task_with_section_2_8_subject() {
        let agent = Arc::new(
            ScriptedTaskAgent::new()
                .script(Stage::Implement, writes_file("a.txt", "a"))
                .script(Stage::Implement, writes_file("b.txt", "b"))
                .script(Stage::Implement, writes_file("c.txt", "c")),
        );
        let (state, _app) = test_app_with_task_agent(agent).await;
        let fixture = GitFixture::new().await;
        let project_id = seed_project_with_workspace(&state, &fixture).await;
        let epic_id = seed_epic(&state, &project_id, "InProgress").await;

        let a = seed_task(&state, &epic_id, &project_id, "A").await;
        let b = seed_task(&state, &epic_id, &project_id, "B").await;
        let c = seed_task(&state, &epic_id, &project_id, "C").await;
        link(&state, &a, &b).await;
        link(&state, &b, &c).await;

        run_epic_pipeline(state.clone(), epic_id.clone()).await;

        let statuses = task_statuses(&state, &epic_id).await;
        assert_eq!(statuses["A"], "Done");
        assert_eq!(statuses["B"], "Done");
        assert_eq!(statuses["C"], "Done");
        assert_eq!(epic_status(&state, &epic_id).await, "InReview");

        // The workspace is retained post-finalize; read the pushed commits back
        // from the fixture (the project's origin) on the epic branch.
        let branch = epic_branch_name_column(&state, &epic_id).await;
        let subjects = git_log_subjects_for_ref(&fixture.dir, &branch).await;
        assert_eq!(
            subjects,
            vec![
                "init".to_string(),
                format!("impl({}): A", spec::short_id(&a)),
                format!("impl({}): B", spec::short_id(&b)),
                format!("impl({}): C", spec::short_id(&c)),
            ],
        );

        cleanup_clone_root(&state, &project_id, &[&epic_id]);
    }

    /// A branching (diamond) DAG: A blocks B and C; B and C both block D.
    /// Every task's commit lands, in an order that is a valid topological
    /// order of the DAG — checked both as an exact sequence (this walk always
    /// picks the lowest-`position` ready task, and B/C were created in that
    /// order, so the sequence is fully deterministic) and generically (every
    /// blocker's commit index precedes every task it blocks).
    #[tokio::test]
    async fn branching_dag_commits_land_in_a_valid_topological_order() {
        let agent = Arc::new(
            ScriptedTaskAgent::new()
                .script(Stage::Implement, writes_file("a.txt", "a"))
                .script(Stage::Implement, writes_file("b.txt", "b"))
                .script(Stage::Implement, writes_file("c.txt", "c"))
                .script(Stage::Implement, writes_file("d.txt", "d")),
        );
        let (state, _app) = test_app_with_task_agent(agent).await;
        let fixture = GitFixture::new().await;
        let project_id = seed_project_with_workspace(&state, &fixture).await;
        let epic_id = seed_epic(&state, &project_id, "InProgress").await;

        let a = seed_task(&state, &epic_id, &project_id, "A").await;
        let b = seed_task(&state, &epic_id, &project_id, "B").await;
        let c = seed_task(&state, &epic_id, &project_id, "C").await;
        let d = seed_task(&state, &epic_id, &project_id, "D").await;
        link(&state, &a, &b).await;
        link(&state, &a, &c).await;
        link(&state, &b, &d).await;
        link(&state, &c, &d).await;

        run_epic_pipeline(state.clone(), epic_id.clone()).await;

        // The workspace is retained post-finalize; read the pushed commits back
        // from the fixture (the project's origin) on the epic branch.
        let branch = epic_branch_name_column(&state, &epic_id).await;
        let subjects = git_log_subjects_for_ref(&fixture.dir, &branch).await;
        assert_eq!(
            subjects,
            vec![
                "init".to_string(),
                format!("impl({}): A", spec::short_id(&a)),
                format!("impl({}): B", spec::short_id(&b)),
                format!("impl({}): C", spec::short_id(&c)),
                format!("impl({}): D", spec::short_id(&d)),
            ],
        );

        // Generic topological check, independent of this walk's specific
        // tie-break: every blocker's commit index precedes its blocked task's.
        let index_of = |short: &str| {
            subjects
                .iter()
                .position(|s| s.contains(short))
                .unwrap_or_else(|| panic!("no commit found for short id {short}"))
        };
        for (blocker, blocked) in [(&a, &b), (&a, &c), (&b, &d), (&c, &d)] {
            assert!(
                index_of(spec::short_id(blocker)) < index_of(spec::short_id(blocked)),
                "{blocker} must commit before {blocked}: {subjects:?}"
            );
        }

        cleanup_clone_root(&state, &project_id, &[&epic_id]);
    }

    /// A `TaskAgent` wrapper that, on every `Stage::Implement` `run()` call,
    /// synchronously records how many tasks are `InProgress` **at the exact
    /// moment** the stage starts (before delegating to `inner`) — the
    /// deterministic, no-sleep proof that the walk never runs two tasks
    /// concurrently (§2.3's "no sibling InProgress" invariant), preferred per
    /// MILESTONE_2 T-513's AC over a sleep-based probe. The probe query runs
    /// on its own single-thread tokio runtime inside a plain `std::thread` —
    /// `run()` itself is synchronous, so a fresh runtime gives the query
    /// somewhere to `.await` without needing the caller's own async context
    /// here.
    ///
    /// Scoped to `Stage::Implement` only (not every stage): an unscripted
    /// `inner` produces no diff, so T-532's `Stage::VerifyComplete` also runs
    /// for every task here — recording *its* probe reading too would still
    /// read `1` (the task stays `InProgress` across both stages), but it
    /// would double this test's own reading count for a reason this test
    /// isn't about, breaking the "one probe reading per task" assertion for
    /// no reason connected to the invariant under test.
    struct ConcurrencyProbeAgent {
        inner: ScriptedTaskAgent,
        conn: libsql::Connection,
        observed: Arc<std::sync::Mutex<Vec<i64>>>,
    }

    impl TaskAgent for ConcurrencyProbeAgent {
        fn run(
            &self,
            req: TaskRunRequest,
        ) -> Result<(RunHandle, Receiver<RunEvent>), HarnessError> {
            if req.stage == Stage::Implement {
                let conn = self.conn.clone();
                let count = std::thread::spawn(move || {
                    let rt = tokio::runtime::Builder::new_current_thread()
                        .enable_all()
                        .build()
                        .unwrap();
                    rt.block_on(async move {
                        let mut rows = conn
                            .query("SELECT COUNT(*) FROM task WHERE status = 'InProgress'", ())
                            .await
                            .unwrap();
                        let row = rows.next().await.unwrap().unwrap();
                        row.get::<i64>(0).unwrap()
                    })
                })
                .join()
                .unwrap();
                self.observed.lock().unwrap().push(count);
            }
            self.inner.run(req)
        }
    }

    /// Two independent, simultaneously-ready tasks (A, B — no edge between
    /// them) plus a third (C) that depends on both: nothing here *forces*
    /// sequential ordering by dependency alone, so this is the strongest
    /// exercise of the "no sibling InProgress" invariant — only the walk's
    /// own serialization keeps A and B from ever running together.
    #[tokio::test]
    async fn implement_stage_never_observes_a_sibling_in_progress() {
        let db = Db::connect(":memory:").await.unwrap();
        db.run_migrations().await.unwrap();
        let probe_conn = db.conn().clone();
        let observed = Arc::new(std::sync::Mutex::new(Vec::new()));
        let agent = Arc::new(ConcurrencyProbeAgent {
            inner: ScriptedTaskAgent::new(),
            conn: probe_conn,
            observed: observed.clone(),
        });
        let state = AppState::with_all_agents_and_host(
            Config::for_test(),
            db,
            Arc::new(SilentPlanningAgent),
            Arc::new(SilentBreakdownAgent),
            agent,
            Arc::new(FakeHost::new()),
        );

        let fixture = GitFixture::new().await;
        let project_id = seed_project_with_workspace(&state, &fixture).await;
        let epic_id = seed_epic(&state, &project_id, "InProgress").await;
        let a = seed_task(&state, &epic_id, &project_id, "A").await;
        let b = seed_task(&state, &epic_id, &project_id, "B").await;
        let c = seed_task(&state, &epic_id, &project_id, "C").await;
        link(&state, &a, &c).await;
        link(&state, &b, &c).await;

        run_epic_pipeline(state.clone(), epic_id.clone()).await;

        let statuses = task_statuses(&state, &epic_id).await;
        assert_eq!(statuses["A"], "Done");
        assert_eq!(statuses["B"], "Done");
        assert_eq!(statuses["C"], "Done");

        let counts = observed.lock().unwrap().clone();
        assert_eq!(
            counts.len(),
            3,
            "one probe reading per task's implement call"
        );
        assert!(
            counts.iter().all(|&n| n == 1),
            "exactly one InProgress task at every implement call: {counts:?}"
        );

        cleanup_clone_root(&state, &project_id, &[&epic_id]);
    }

    /// An implement stage that makes no changes: no commit lands, no `commit`
    /// stage `agent_run` row is written, `Stage::VerifyComplete` runs instead
    /// of `Stage::Review` (T-532; the default-scripted verdict is `PASS` —
    /// see `task_agent::testing::default_script_for`), and the task is still
    /// left `Done`.
    #[tokio::test]
    async fn no_diff_implement_stage_creates_no_commit_and_leaves_task_done() {
        // Bare ScriptedTaskAgent (test_app's default): its ScriptedRun::default
        // writes no files, so the implement stage produces no diff.
        let (state, _app) = test_app().await;
        let fixture = GitFixture::new().await;
        let project_id = seed_project_with_workspace(&state, &fixture).await;
        let epic_id = seed_epic(&state, &project_id, "InProgress").await;
        let task_id = seed_task(&state, &epic_id, &project_id, "A").await;

        run_epic_pipeline(state.clone(), epic_id.clone()).await;

        let statuses = task_statuses(&state, &epic_id).await;
        assert_eq!(statuses["A"], "Done", "a no-diff task is still left Done");

        // The workspace is retained post-finalize; read the pushed branch back
        // from the fixture (the project's origin).
        let branch = epic_branch_name_column(&state, &epic_id).await;
        let subjects = git_log_subjects_for_ref(&fixture.dir, &branch).await;
        assert_eq!(
            subjects,
            vec!["init".to_string()],
            "no commit must land when the implement stage made no changes"
        );

        let mut rows = state
            .db
            .conn()
            .query(
                "SELECT COUNT(*) FROM agent_run WHERE task_id = ?1 AND stage = 'commit'",
                params![task_id.clone()],
            )
            .await
            .unwrap();
        let count: i64 = rows.next().await.unwrap().unwrap().get(0).unwrap();
        assert_eq!(
            count, 0,
            "the commit stage never runs when there is nothing to commit"
        );

        // No commit means no review either.
        let mut review_rows = state
            .db
            .conn()
            .query(
                "SELECT COUNT(*) FROM agent_run WHERE task_id = ?1 AND stage = 'review'",
                params![task_id.clone()],
            )
            .await
            .unwrap();
        let review_count: i64 = review_rows.next().await.unwrap().unwrap().get(0).unwrap();
        assert_eq!(
            review_count, 0,
            "the review stage never runs for a no-diff task"
        );

        // T-532: verify-complete runs exactly once instead, and (defaulted
        // to PASS) records that verdict.
        let mut vc_rows = state
            .db
            .conn()
            .query(
                "SELECT COUNT(*), verdict FROM agent_run WHERE task_id = ?1 AND stage = 'verify_complete'",
                params![task_id],
            )
            .await
            .unwrap();
        let vc_row = vc_rows.next().await.unwrap().unwrap();
        let vc_count: i64 = vc_row.get(0).unwrap();
        let vc_verdict: Option<String> = vc_row.get(1).unwrap();
        assert_eq!(
            vc_count, 1,
            "verify-complete runs exactly once for a no-diff task"
        );
        assert_eq!(vc_verdict.as_deref(), Some("PASS"));

        cleanup_clone_root(&state, &project_id, &[&epic_id]);
    }

    /// The `implement` and `commit` `agent_run` rows are both written, with
    /// the right stages/status, and the commit row's `log` carries the
    /// resulting SHA (§2.2: the Commit stage "records the SHA in log").
    #[tokio::test]
    async fn implement_and_commit_agent_run_rows_are_written_with_sha_in_commit_log() {
        let agent = Arc::new(
            ScriptedTaskAgent::new().script(Stage::Implement, writes_file("out.txt", "hello")),
        );
        let (state, _app) = test_app_with_task_agent(agent).await;
        let fixture = GitFixture::new().await;
        let project_id = seed_project_with_workspace(&state, &fixture).await;
        let epic_id = seed_epic(&state, &project_id, "InProgress").await;
        let task_id = seed_task(&state, &epic_id, &project_id, "A").await;

        run_epic_pipeline(state.clone(), epic_id.clone()).await;

        // The workspace is retained post-finalize; HEAD, read back from the
        // fixture (the project's origin) on the epic branch instead.
        let branch = epic_branch_name_column(&state, &epic_id).await;
        let head_sha = git_rev_parse(&fixture.dir, &branch).await;

        let mut rows = state
            .db
            .conn()
            .query(
                "SELECT status FROM agent_run WHERE task_id = ?1 AND stage = 'implement'",
                params![task_id.clone()],
            )
            .await
            .unwrap();
        let row = rows
            .next()
            .await
            .unwrap()
            .expect("an implement agent_run row");
        assert_eq!(row.get::<String>(0).unwrap(), "ok");

        let mut rows = state
            .db
            .conn()
            .query(
                "SELECT status, log FROM agent_run WHERE task_id = ?1 AND stage = 'commit'",
                params![task_id],
            )
            .await
            .unwrap();
        let row = rows.next().await.unwrap().expect("a commit agent_run row");
        assert_eq!(row.get::<String>(0).unwrap(), "ok");
        let log: String = row.get(1).unwrap();
        assert!(
            log.contains(&head_sha),
            "commit row's log must carry the SHA: {log:?}"
        );

        cleanup_clone_root(&state, &project_id, &[&epic_id]);
    }

    /// The D8 prompt actually carries the epic's background and the sibling
    /// manifest, not a bare spec: A's prompt lists B under "Owned by later
    /// tasks" (with the epic's description present); once A is Done, B's
    /// prompt lists A under "Already built".
    #[tokio::test]
    async fn implement_prompt_includes_epic_context_and_sibling_manifest() {
        let agent = Arc::new(ScriptedTaskAgent::new());
        let recorded = agent.recorded();
        let (state, _app) = test_app_with_task_agent(agent.clone()).await;
        let fixture = GitFixture::new().await;
        let project_id = seed_project_with_workspace(&state, &fixture).await;
        let epic_id = seed_epic(&state, &project_id, "InProgress").await;

        state
            .db
            .conn()
            .execute(
                "UPDATE epic SET description = ?1 WHERE id = ?2",
                params!["Let users manage their profile.", epic_id.clone()],
            )
            .await
            .unwrap();

        let a = seed_task(&state, &epic_id, &project_id, "Add the profile form").await;
        let b = seed_task(&state, &epic_id, &project_id, "Wire the profile API").await;
        link(&state, &a, &b).await; // A runs first, B second.

        run_epic_pipeline(state.clone(), epic_id.clone()).await;

        let all_runs = recorded.lock().unwrap();
        // T-532: both tasks' implement stages produce no diff (an unscripted
        // ScriptedTaskAgent writes no files), so each also runs
        // Stage::VerifyComplete (default-scripted PASS) — filter down to
        // just the Implement calls this test actually cares about, rather
        // than assuming `recorded` holds only those.
        let runs: Vec<_> = all_runs
            .iter()
            .filter(|r| r.stage == Stage::Implement)
            .collect();
        assert_eq!(runs.len(), 2, "one implement call per task");

        // A's prompt: epic context present; B listed as owned by a later task.
        assert!(runs[0].prompt.contains("Epic Context"));
        assert!(runs[0].prompt.contains("Let users manage their profile."));
        assert!(runs[0].prompt.contains("Owned by later tasks"));
        assert!(runs[0].prompt.contains("Wire the profile API"));

        // B's prompt: A now shows up under "Already built".
        assert!(runs[1].prompt.contains("Already built"));
        assert!(runs[1].prompt.contains("Add the profile form"));

        drop(all_runs);
        cleanup_clone_root(&state, &project_id, &[&epic_id]);
    }

    // ---- T-514: finalize (push + open PR) -------------------------------

    /// Like [`test_app_with_task_agent`] but also injecting an explicit
    /// [`GitHost`] — the seam T-514's tests use to script/inspect the
    /// finalize step's push/PR calls instead of accepting the default
    /// [`FakeHost`].
    async fn test_app_with_task_agent_and_host(
        task_agent: Arc<dyn TaskAgent>,
        git_host: Arc<dyn GitHost>,
    ) -> (AppState, axum::Router) {
        let db = Db::connect(":memory:").await.unwrap();
        db.run_migrations().await.unwrap();
        let state = AppState::with_all_agents_and_host(
            Config::for_test(),
            db,
            Arc::new(SilentPlanningAgent),
            Arc::new(SilentBreakdownAgent),
            task_agent,
            git_host,
        );
        let app = app(state.clone());
        (state, app)
    }

    /// `open_pr` sends the right title/head/base: asserted via `FakeHost`'s
    /// recorded call, against a walk that actually completes.
    #[tokio::test]
    async fn finalize_open_pr_sends_the_right_title_head_and_base() {
        let fake = Arc::new(FakeHost::new());
        let (state, _app) =
            test_app_with_task_agent_and_host(Arc::new(ScriptedTaskAgent::new()), fake.clone())
                .await;
        let fixture = GitFixture::new().await;
        let project_id = seed_project_with_workspace(&state, &fixture).await;
        let epic_id = seed_epic(&state, &project_id, "InProgress").await;
        seed_task(&state, &epic_id, &project_id, "A").await;

        run_epic_pipeline(state.clone(), epic_id.clone()).await;

        assert_eq!(epic_status(&state, &epic_id).await, "InReview");

        let branch = epic_branch_name_column(&state, &epic_id).await;
        let calls = fake.open_pr_calls();
        assert_eq!(calls.len(), 1, "exactly one open_pr call per finalize");
        assert_eq!(
            calls[0].head, branch,
            "PR must be opened from the epic branch"
        );
        assert_eq!(
            calls[0].base, "main",
            "PR must target the (fake) default branch"
        );
        assert_eq!(
            calls[0].title, "E",
            "PR title must be the epic's own title (seed_epic's 'E')"
        );
        assert!(calls[0].body.contains("## Tasks"));

        cleanup_clone_root(&state, &project_id, &[&epic_id]);
    }

    /// §5: an epic with a recorded `base_branch` opens its PR against exactly
    /// that branch — the provision-time snapshot wins over anything else,
    /// including the workspace clone's own origin/HEAD (which is `main` in
    /// this fixture).
    #[tokio::test]
    async fn finalize_open_pr_targets_the_epics_recorded_base_branch() {
        let fake = Arc::new(FakeHost::new());
        let (state, _app) =
            test_app_with_task_agent_and_host(Arc::new(ScriptedTaskAgent::new()), fake.clone())
                .await;
        let fixture = GitFixture::new().await;
        // The recorded base must actually exist on the remote, or provisioning
        // (which resets the canonical checkout to origin/<base>) fails first.
        git_ok(&fixture.dir, &["branch", "release/1"]).await;
        let project_id = seed_project_with_workspace(&state, &fixture).await;
        let epic_id = seed_epic(&state, &project_id, "InProgress").await;
        state
            .db
            .conn()
            .execute(
                "UPDATE epic SET base_branch = 'release/1' WHERE id = ?1",
                params![epic_id.clone()],
            )
            .await
            .unwrap();
        seed_task(&state, &epic_id, &project_id, "A").await;

        run_epic_pipeline(state.clone(), epic_id.clone()).await;

        assert_eq!(epic_status(&state, &epic_id).await, "InReview");
        let calls = fake.open_pr_calls();
        assert_eq!(calls.len(), 1);
        assert_eq!(
            calls[0].base, "release/1",
            "the recorded epic base branch must win over the clone's origin/HEAD"
        );

        cleanup_clone_root(&state, &project_id, &[&epic_id]);
    }

    /// §5: a standalone task has no per-item record by design, so its PR
    /// targets the project default when one is set.
    #[tokio::test]
    async fn finalize_open_pr_standalone_task_targets_the_project_base() {
        let fake = Arc::new(FakeHost::new());
        let (state, _app) =
            test_app_with_task_agent_and_host(Arc::new(ScriptedTaskAgent::new()), fake.clone())
                .await;
        let fixture = GitFixture::new().await;
        // The project default must exist on the remote for provisioning.
        git_ok(&fixture.dir, &["branch", "develop"]).await;
        let project_id = seed_project_with_workspace(&state, &fixture).await;
        state
            .db
            .conn()
            .execute(
                "UPDATE project SET base_branch = 'develop' WHERE id = ?1",
                params![project_id.clone()],
            )
            .await
            .unwrap();
        // The standalone claim predicate only ever selects `InProgress`
        // (§2.4) — seed it claim-ready.
        let task_id = seed_standalone_task(&state, &project_id, "Solo", "InProgress").await;

        run_standalone_pipeline(state.clone(), task_id.clone()).await;

        assert_eq!(single_task_status(&state, &task_id).await, "InReview");
        let calls = fake.open_pr_calls();
        assert_eq!(calls.len(), 1);
        assert_eq!(
            calls[0].base, "develop",
            "a standalone task's PR must target the project default"
        );

        cleanup_clone_root(&state, &project_id, &[]);
    }

    /// A feedback re-run reuses the recorded PR instead of opening a new one:
    /// the first finalize pushes + opens exactly one PR and lands the epic in
    /// `InReview` (workspace retained); once the (simulated) review poller
    /// hands the epic back to `InProgress` with its existing `pr_url`, a
    /// *second* finalize pushes only — `open_pr` is never called again — and
    /// returns the epic to `InReview`, preserving the recorded PR and keeping
    /// the workspace for further feedback rounds.
    #[tokio::test]
    async fn finalize_rerun_reuses_existing_pr_without_reopening() {
        let fake = Arc::new(FakeHost::new());
        let (state, _app) =
            test_app_with_task_agent_and_host(Arc::new(ScriptedTaskAgent::new()), fake.clone())
                .await;
        let fixture = GitFixture::new().await;
        let project_id = seed_project_with_workspace(&state, &fixture).await;
        let epic_id = seed_epic(&state, &project_id, "InProgress").await;
        seed_task(&state, &epic_id, &project_id, "A").await;

        let ws = workspace::provision_epic_workspace(&state, &epic_id, &project_id)
            .await
            .expect("provisioning against the local fixture must succeed");

        // First finalize (directly, like `failed_open_pr...`): open the PR,
        // land in InReview, retain the workspace.
        let epic = fetch_epic(state.db.conn(), &epic_id)
            .await
            .unwrap()
            .unwrap();
        let dag = compute_dag(state.db.conn(), &epic_id).await.unwrap();
        finalize_epic(&state, &epic_id, &epic, &dag, &ws, &LeaseHandle::new()).await;

        let epic = fetch_epic(state.db.conn(), &epic_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            epic.status, "InReview",
            "first finalize lands the epic in InReview"
        );
        let pr_url = epic
            .pr_url
            .clone()
            .expect("first finalize must record a PR url");
        assert_eq!(
            fake.open_pr_calls().len(),
            1,
            "exactly one open_pr on the first finalize"
        );
        let ws_path = workspace::epic_workspace_path(&state.config.clone_root, &epic_id);
        assert!(
            ws_path.join(".git").exists(),
            "the workspace must be retained after the first finalize"
        );

        // Simulate the review poller handing feedback-induced work back to the
        // worker pool: epic back to InProgress, carrying its existing PR.
        state
            .db
            .conn()
            .execute(
                "UPDATE epic SET status = 'InProgress', updated_at = ?1 WHERE id = ?2",
                params![now_ms(), epic_id.clone()],
            )
            .await
            .unwrap();

        // Second finalize (re-run): push only — no duplicate open_pr, the
        // recorded PR is preserved, and it returns to InReview.
        let epic = fetch_epic(state.db.conn(), &epic_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            epic.pr_url.as_deref(),
            Some(pr_url.as_str()),
            "the re-run sees the recorded PR and reuses it"
        );
        let dag = compute_dag(state.db.conn(), &epic_id).await.unwrap();
        finalize_epic(&state, &epic_id, &epic, &dag, &ws, &LeaseHandle::new()).await;

        let epic = fetch_epic(state.db.conn(), &epic_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            epic.status, "InReview",
            "the re-run returns the epic to InReview"
        );
        assert_eq!(epic.pr_url.as_deref(), Some(pr_url.as_str()));
        assert_eq!(
            fake.open_pr_calls().len(),
            1,
            "a second finalize must NOT re-open the PR (no duplicate open_pr)"
        );
        assert!(
            ws_path.join(".git").exists(),
            "the workspace must be retained after the re-run"
        );

        cleanup_clone_root(&state, &project_id, &[&epic_id]);
    }

    /// `InReview` is set **only** after the PR opens: a `FakeHost` scripted
    /// to fail `open_pr` leaves the epic `Blocked(pr_failed)`, the workspace
    /// retained, and `pr_url`/`pr_number` unset — and the readable, redacted
    /// failure reason lands in the `Stage::Push` evidence row without ever
    /// leaking the token, even when the (contrived) failure message itself
    /// contained it.
    ///
    /// Calls [`finalize_epic`] directly rather than through the full
    /// `run_epic_pipeline` walk, and stubs `push` to succeed trivially
    /// (`FakeHost::stub_push_success`): a project's PAT reaches the
    /// *canonical* checkout's own refresh during provisioning too
    /// (`workspace::provision_epic_workspace`), and separately reaches
    /// `push` itself — and [`git::authenticated_url`] requires an
    /// `https://` `repo_url` the instant a PAT is present, which this test's
    /// local git-fixture `repo_url` never is (there is no network in `just
    /// test`). Provisioning without a PAT first, then setting one and
    /// calling `finalize_epic` directly with push stubbed out, isolates
    /// exactly the thing this test cares about — does *finalize's own*
    /// redaction hold on the `open_pr` failure path when the project
    /// genuinely has a PAT configured — from both of those unrelated
    /// PAT/https constraints.
    #[tokio::test]
    async fn failed_open_pr_blocks_epic_retains_workspace_and_never_persists_a_pr() {
        let pat = "ghp_openPrFailureLeak123";
        let fake = Arc::new(
            FakeHost::new()
                .stub_push_success()
                .fail_open_pr(format!("GitHub API returned HTTP 422: bad token {pat}")),
        );
        let (state, _app) =
            test_app_with_task_agent_and_host(Arc::new(ScriptedTaskAgent::new()), fake).await;
        let fixture = GitFixture::new().await;
        let project_id = seed_project_with_workspace(&state, &fixture).await;
        let epic_id = seed_epic(&state, &project_id, "InProgress").await;
        seed_task(&state, &epic_id, &project_id, "A").await;

        let ws = workspace::provision_epic_workspace(&state, &epic_id, &project_id)
            .await
            .expect("provisioning without a PAT must succeed against the local fixture");

        // Only now give the project a real, decryptable PAT — see the doc
        // comment above for why this has to happen after provisioning, not
        // before.
        let blob = state.crypto.encrypt_pat(pat).unwrap();
        state
            .db
            .conn()
            .execute(
                "UPDATE project SET pat_encrypted = ?1 WHERE id = ?2",
                params![blob, project_id.clone()],
            )
            .await
            .unwrap();

        let epic = fetch_epic(state.db.conn(), &epic_id)
            .await
            .unwrap()
            .unwrap();
        let dag = compute_dag(state.db.conn(), &epic_id).await.unwrap();
        finalize_epic(&state, &epic_id, &epic, &dag, &ws, &LeaseHandle::new()).await;

        let epic = fetch_epic(state.db.conn(), &epic_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(epic.status, "Blocked");
        assert_eq!(epic.blocked_reason.as_deref(), Some("pr_failed"));
        assert!(
            epic.pr_url.is_none(),
            "pr_url must never be set when open_pr fails"
        );
        assert!(epic.pr_number.is_none());

        let (lease_owner, lease_expires_at) = epic_lease(&state, &epic_id).await;
        assert!(lease_owner.is_none(), "lease must be released on Blocked");
        assert!(lease_expires_at.is_none());

        let workspace_path = workspace::epic_workspace_path(&state.config.clone_root, &epic_id);
        assert!(
            workspace_path.join(".git").exists(),
            "the workspace must be retained (never deleted) when finalize fails"
        );

        let mut rows = state
            .db
            .conn()
            .query(
                "SELECT status, log FROM agent_run WHERE epic_id = ?1 AND stage = 'push'",
                params![epic_id.clone()],
            )
            .await
            .unwrap();
        let row = rows.next().await.unwrap().expect("a push agent_run row");
        assert_eq!(row.get::<String>(0).unwrap(), "error");
        let log: String = row.get(1).unwrap();
        assert!(
            log.contains("422"),
            "the failure reason must be readable: {log:?}"
        );
        assert!(
            !log.contains(pat),
            "the token must never leak into evidence: {log:?}"
        );
        assert!(!log.contains("ghp_"));

        cleanup_clone_root(&state, &project_id, &[&epic_id]);
    }

    /// A failed `push` blocks the epic the same way, with the workspace
    /// retained and `open_pr` never even attempted.
    #[tokio::test]
    async fn failed_push_blocks_epic_retains_workspace_and_never_calls_open_pr() {
        let fake = Arc::new(FakeHost::new().fail_push("simulated push failure"));
        let (state, _app) =
            test_app_with_task_agent_and_host(Arc::new(ScriptedTaskAgent::new()), fake.clone())
                .await;
        let fixture = GitFixture::new().await;
        let project_id = seed_project_with_workspace(&state, &fixture).await;
        let epic_id = seed_epic(&state, &project_id, "InProgress").await;
        seed_task(&state, &epic_id, &project_id, "A").await;

        run_epic_pipeline(state.clone(), epic_id.clone()).await;

        let epic = fetch_epic(state.db.conn(), &epic_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(epic.status, "Blocked");
        assert_eq!(epic.blocked_reason.as_deref(), Some("pr_failed"));
        assert!(epic.pr_url.is_none());

        let workspace_path = workspace::epic_workspace_path(&state.config.clone_root, &epic_id);
        assert!(
            workspace_path.join(".git").exists(),
            "the workspace must be retained when the push fails"
        );

        assert!(
            fake.open_pr_calls().is_empty(),
            "open_pr must never be attempted once the push has failed"
        );

        cleanup_clone_root(&state, &project_id, &[&epic_id]);
    }

    /// A `TaskAgent` wrapper that gates only the *Nth call of one specific
    /// stage's* `Exited` event (0-indexed, per-`gate_stage`) behind a
    /// [`Gate`], letting every other call — including every call of any
    /// *other* stage — through untouched. Unlike `ScriptedTaskAgent::with_gate`
    /// (gates every call uniformly), needed so an earlier task can finish
    /// completely while a later one is deliberately held in flight (the
    /// "cancel mid-walk" test below).
    ///
    /// Indexing is per-stage, not per overall call (T-530): a walk now makes
    /// more than one agent call per task (`Implement` then `Review`), and
    /// T-531/T-532 add still more, so "the Nth agent call overall" silently
    /// points at a different stage every time a new stage lands upstream of
    /// the one a test actually cares about gating. Counting "the Nth call of
    /// `gate_stage`" instead stays pinned to the call the test names, no
    /// matter how many other-stage calls happen around it.
    struct SelectiveGateAgent {
        inner: ScriptedTaskAgent,
        /// Per-stage call counters, keyed by [`Stage`] — bumped once per
        /// call to `run` for that stage, independent of every other stage's
        /// count.
        call_index: Mutex<HashMap<Stage, usize>>,
        /// Which stage's calls to count at all; calls of any other stage
        /// always pass through ungated.
        gate_stage: Stage,
        /// Gate the `gate_stage` call at this index (0-indexed, counting
        /// only `gate_stage` calls).
        gate_at_index: usize,
        gate: Arc<Gate>,
    }

    impl TaskAgent for SelectiveGateAgent {
        fn run(
            &self,
            req: TaskRunRequest,
        ) -> Result<(RunHandle, Receiver<RunEvent>), HarnessError> {
            let stage = req.stage;
            let (handle, inner_rx) = self.inner.run(req)?;
            if stage != self.gate_stage {
                return Ok((handle, inner_rx));
            }
            let idx = {
                let mut counts = self.call_index.lock().unwrap();
                let idx = *counts.get(&stage).unwrap_or(&0);
                counts.insert(stage, idx + 1);
                idx
            };
            if idx != self.gate_at_index {
                return Ok((handle, inner_rx));
            }
            let gate = self.gate.clone();
            let (tx, rx) = std::sync::mpsc::channel();
            std::thread::spawn(move || {
                for event in inner_rx {
                    if matches!(event, RunEvent::Exited { .. }) {
                        gate.wait();
                    }
                    if tx.send(event).is_err() {
                        break;
                    }
                }
            });
            Ok((handle, rx))
        }
    }

    /// Cancelling mid-walk stops cleanly: while task B's implement stage is
    /// deliberately held in flight (gated before its terminal `Exited`), an
    /// external cancel (a lane move away from `InProgress`, simulated by
    /// writing the epic's status directly) lands. Releasing the gate lets
    /// B's implement stage *finish*, but the walk's mid-task recheck must
    /// catch the cancel before finalizing B — so B is never committed or
    /// marked Done, C (never even reached) stays Todo, and no further
    /// commits land beyond A's.
    #[tokio::test]
    async fn cancel_mid_walk_stops_cleanly_without_further_writes() {
        let gate = Arc::new(Gate::default());
        let agent = Arc::new(SelectiveGateAgent {
            inner: ScriptedTaskAgent::new()
                .script(Stage::Implement, writes_file("a.txt", "a"))
                .script(Stage::Implement, writes_file("b.txt", "b"))
                .script(Stage::Implement, writes_file("c.txt", "c")),
            call_index: Mutex::new(HashMap::new()),
            gate_stage: Stage::Implement,
            gate_at_index: 1, // gate task B's implement call
            gate: gate.clone(),
        });

        let (state, _app) = test_app_with_task_agent(agent).await;
        let fixture = GitFixture::new().await;
        let project_id = seed_project_with_workspace(&state, &fixture).await;
        let epic_id = seed_epic(&state, &project_id, "InProgress").await;
        let a = seed_task(&state, &epic_id, &project_id, "A").await;
        let b = seed_task(&state, &epic_id, &project_id, "B").await;
        let c = seed_task(&state, &epic_id, &project_id, "C").await;
        link(&state, &a, &b).await;
        link(&state, &b, &c).await;

        let walk_state = state.clone();
        let walk_epic = epic_id.clone();
        let handle = tokio::spawn(async move {
            run_epic_pipeline(walk_state, walk_epic).await;
        });

        // Bounded, no-sleep-as-the-proof readiness poll: wait until task B is
        // InProgress — proves A already finished and B's implement call is
        // now gated in flight.
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        loop {
            let statuses = task_statuses(&state, &epic_id).await;
            if statuses
                .get("B")
                .map(|s| s == "InProgress")
                .unwrap_or(false)
            {
                break;
            }
            if tokio::time::Instant::now() >= deadline {
                panic!("task B never reached InProgress");
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }

        // Simulate an external cancel while B's implement stage is gated.
        state
            .db
            .conn()
            .execute(
                "UPDATE epic SET status = 'Cancelled' WHERE id = ?1",
                params![epic_id.clone()],
            )
            .await
            .unwrap();

        gate.release();
        handle.await.unwrap();

        let statuses = task_statuses(&state, &epic_id).await;
        assert_eq!(statuses["C"], "Todo", "the walk must never have reached C");
        assert_ne!(
            statuses["B"], "Done",
            "B must not be finalized once the cancel was observed mid-task"
        );

        let workspace_path = workspace::epic_workspace_path(&state.config.clone_root, &epic_id);
        let subjects = git_log_subjects(&workspace_path).await;
        assert_eq!(
            subjects,
            vec![
                "init".to_string(),
                format!("impl({}): A", spec::short_id(&a))
            ],
            "only A's commit may have landed before the cancel stopped the walk"
        );

        cleanup_clone_root(&state, &project_id, &[&epic_id]);
    }

    // ---- T-522: the test gate + test-driven fix loop --------------------------

    /// A project whose `test_cmd` is flippable by the scripted `Fix` stage —
    /// the canonical trick this task's own AC prescribes: a sentinel file
    /// (`.fixed`) a scripted `Fix` run creates like any other scripted file
    /// write. Deterministic red-then-green, no sleeps, no real test
    /// framework.
    ///
    /// This is **not** simply `test -f .fixed` — this same `test_cmd` also
    /// runs as T-521's *preflight* gate, on the untouched tree, before any
    /// task's implement stage has run at all. A bare `test -f .fixed` would
    /// be red on that untouched tree too (`.fixed` doesn't exist yet
    /// anywhere), blocking the epic with `preflight_red` before the DAG walk
    /// ever starts — exactly the failure these tests hit before this
    /// three-way branch was added. So the command distinguishes three
    /// states: **pristine** (neither file exists — green, preflight passes),
    /// **implemented but not yet fixed** (`work.txt` exists, `.fixed`
    /// doesn't — red, this is the state T-522's gate is supposed to catch),
    /// and **fixed** (`.fixed` exists — green). Every test using this
    /// constant scripts `Stage::Implement` to write `work.txt`, which is
    /// what actually flips the tree from "pristine" into "implemented but
    /// not yet fixed".
    const FLIPPABLE_TEST_CMD: &str =
        "if test -f .fixed; then exit 0; elif test -f work.txt; then exit 1; else exit 0; fi";

    /// Like [`FLIPPABLE_TEST_CMD`] but the red branch also echoes a
    /// distinctive marker — used by the D19 test to prove the exact test
    /// output reached the fix agent's prompt.
    const FLIPPABLE_TEST_CMD_WITH_MARKER: &str = "if test -f .fixed; then exit 0; \
         elif test -f work.txt; then echo THE_TESTS_ARE_BROKEN_MARKER; exit 1; \
         else exit 0; fi";

    /// A `ScriptedRun` that writes `.fixed` into the workspace — the `Fix`
    /// stage's half of the flippable-`test_cmd` trick.
    fn writes_fixed_marker() -> ScriptedRun {
        writes_file(".fixed", "fixed\n")
    }

    /// All `(stage, attempt, status)` rows for `task_id`, oldest first,
    /// restricted to the two T-522 stages — the exact shape T-522's AC asks
    /// tests to assert against ("the exact sequence of (stage, attempt,
    /// status) rows").
    async fn gate_and_fix_rows(state: &AppState, task_id: &str) -> Vec<(String, i64, String)> {
        evidence::list_runs_for_task(state.db.conn(), task_id)
            .await
            .unwrap()
            .into_iter()
            .filter(|r| r.stage == "test_gate" || r.stage == "fix")
            .map(|r| (r.stage, r.attempt, r.status))
            .collect()
    }

    /// `test_gate`/`fix` rows tend to be pinned to specific `(stage,
    /// attempt)` pairs by attempt count elsewhere, but exhaustion needs the
    /// raw `base_sha` off the task row (deliberately not part of the public
    /// `tasks::Task`/JSON surface — T-500's AC keeps it internal — so read
    /// it directly).
    async fn task_base_sha(state: &AppState, task_id: &str) -> Option<String> {
        let mut rows = state
            .db
            .conn()
            .query("SELECT base_sha FROM task WHERE id = ?1", params![task_id])
            .await
            .unwrap();
        rows.next().await.unwrap().unwrap().get(0).unwrap()
    }

    /// `git show <ref>:<path>` in `dir`, trimmed — reads a file's content out
    /// of a specific commit rather than the working tree, so a test can
    /// confirm exactly what landed in the one commit a red-then-green run
    /// produces.
    async fn git_show_file(dir: &std::path::Path, git_ref: &str, path: &str) -> String {
        let output = tokio::process::Command::new("git")
            .args(["show", &format!("{git_ref}:{path}")])
            .current_dir(dir)
            .output()
            .await
            .unwrap();
        assert!(
            output.status.success(),
            "git show {git_ref}:{path} failed: {output:?}"
        );
        String::from_utf8_lossy(&output.stdout).trim().to_string()
    }

    /// Directly set a task's `description`/`acceptance` (the plain
    /// `seed_task` helper only takes a title) — used by the D19 test to give
    /// the implement stage's prompt a distinctive marker to check the fix
    /// prompt never inherits.
    async fn set_task_spec(state: &AppState, task_id: &str, description: &str, acceptance: &str) {
        state
            .db
            .conn()
            .execute(
                "UPDATE task SET description = ?1, acceptance = ?2 WHERE id = ?3",
                params![description, acceptance, task_id],
            )
            .await
            .unwrap();
    }

    /// The headline AC: a red gate that a scripted `Fix` round turns green
    /// commits exactly once, and that one commit contains the fix. A red
    /// gate never reaches the commit step at all (by construction — see
    /// `run_test_gate_loop`), so "exactly one commit" here is already strong
    /// evidence the commit happened *after* green, not on some earlier red
    /// pass; reading the committed tree back and finding both the
    /// implement-stage file and the fix-stage file confirms it further.
    #[tokio::test]
    async fn red_then_green_test_gate_commits_once_after_green() {
        let agent = Arc::new(
            ScriptedTaskAgent::new()
                .script(Stage::Implement, writes_file("work.txt", "work\n"))
                .script(Stage::Fix, writes_fixed_marker()),
        );
        let (state, _app) = test_app_with_task_agent(agent).await;
        let fixture = GitFixture::new().await;
        let project_id = seed_project_with_test_cmd(&state, &fixture, FLIPPABLE_TEST_CMD).await;
        let epic_id = seed_epic(&state, &project_id, "InProgress").await;
        let a = seed_task(&state, &epic_id, &project_id, "A").await;

        run_epic_pipeline(state.clone(), epic_id.clone()).await;

        assert_eq!(epic_status(&state, &epic_id).await, "InReview");

        let branch = epic_branch_name_column(&state, &epic_id).await;
        let subjects = git_log_subjects_for_ref(&fixture.dir, &branch).await;
        assert_eq!(
            subjects,
            vec![
                "init".to_string(),
                format!("impl({}): A", spec::short_id(&a))
            ],
            "exactly one commit must land, after the gate went green"
        );

        // The single commit contains both the implement stage's file and
        // the fix stage's file — the fix really did land in the commit that
        // finally went green, not get discarded.
        assert_eq!(
            git_show_file(&fixture.dir, &branch, "work.txt").await,
            "work"
        );
        assert_eq!(
            git_show_file(&fixture.dir, &branch, ".fixed").await,
            "fixed"
        );

        let rows = gate_and_fix_rows(&state, &a).await;
        assert_eq!(
            rows,
            vec![
                ("test_gate".to_string(), 0, "error".to_string()),
                ("fix".to_string(), 1, "ok".to_string()),
                ("test_gate".to_string(), 1, "ok".to_string()),
            ]
        );

        cleanup_clone_root(&state, &project_id, &[&epic_id]);
    }

    /// Two red rounds before green: the exact `(stage, attempt, status)`
    /// sequence T-522's AC asks for, proving both that attempts increase
    /// monotonically and that a `fix@N` always pairs with the `test_gate@N`
    /// retest that follows it (see the module doc's attempt-numbering
    /// section).
    #[tokio::test]
    async fn each_fix_round_writes_its_own_increasing_attempt_rows() {
        let agent = Arc::new(
            ScriptedTaskAgent::new()
                .script(Stage::Implement, writes_file("work.txt", "work\n"))
                // First fix round doesn't actually satisfy the gate...
                .script(Stage::Fix, writes_file(".attempt1", "nope\n"))
                // ...the second one does.
                .script(Stage::Fix, writes_fixed_marker()),
        );
        let (state, _app) = test_app_with_task_agent(agent).await;
        let fixture = GitFixture::new().await;
        let project_id = seed_project_with_test_cmd(&state, &fixture, FLIPPABLE_TEST_CMD).await;
        let epic_id = seed_epic(&state, &project_id, "InProgress").await;
        let a = seed_task(&state, &epic_id, &project_id, "A").await;

        run_epic_pipeline(state.clone(), epic_id.clone()).await;

        assert_eq!(epic_status(&state, &epic_id).await, "InReview");

        let rows = gate_and_fix_rows(&state, &a).await;
        assert_eq!(
            rows,
            vec![
                ("test_gate".to_string(), 0, "error".to_string()),
                ("fix".to_string(), 1, "ok".to_string()),
                ("test_gate".to_string(), 1, "error".to_string()),
                ("fix".to_string(), 2, "ok".to_string()),
                ("test_gate".to_string(), 2, "ok".to_string()),
            ],
            "red -> red -> green must produce exactly this (stage, attempt, status) sequence"
        );

        cleanup_clone_root(&state, &project_id, &[&epic_id]);
    }

    /// A gate that never goes green exhausts `DEARBORN_MAX_TEST_FIX_ATTEMPTS`
    /// (3 in `Config::for_test`): the task fails, the epic blocks with the
    /// identical reason, the lease is released, the workspace (and its dirty
    /// tree) is retained, and — the headline negative assertion — nothing is
    /// ever committed: `HEAD` in the retained workspace is still exactly
    /// `base_sha`.
    #[tokio::test]
    async fn exhausting_attempts_fails_the_task_blocks_the_epic_and_commits_nothing() {
        let agent = Arc::new(
            ScriptedTaskAgent::new().script(Stage::Implement, writes_file("broken.txt", "oops\n")),
        );
        let (state, _app) = test_app_with_task_agent(agent).await;
        let fixture = GitFixture::new().await;
        // Green on the untouched tree (so preflight passes and the walk
        // actually reaches this task's implement stage), red forever once
        // `broken.txt` exists — the scripted Fix stage (default: no files
        // written) never creates anything that would satisfy it.
        let project_id = seed_project_with_test_cmd(&state, &fixture, "! test -f broken.txt").await;
        let epic_id = seed_epic(&state, &project_id, "InProgress").await;
        let a = seed_task(&state, &epic_id, &project_id, "A").await;

        run_epic_pipeline(state.clone(), epic_id.clone()).await;

        let task = fetch_task_row(&state, &a).await;
        assert_eq!(task.0, "Failed", "the task itself must be Failed");
        assert_eq!(task.1.as_deref(), Some("test_gate_exhausted"));

        let epic = fetch_epic(state.db.conn(), &epic_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(epic.status, "Blocked");
        assert_eq!(
            epic.blocked_reason.as_deref(),
            Some("test_gate_exhausted"),
            "the epic must carry the identical reason string as the task"
        );

        let (lease_owner, lease_expires_at) = epic_lease(&state, &epic_id).await;
        assert!(
            lease_owner.is_none(),
            "lease must be released on exhaustion"
        );
        assert!(lease_expires_at.is_none());

        // Attempts: test_gate@0..3 all error (4 rows), fix@1..3 all ok (3
        // rows) — attempt 3 (== max_test_fix_attempts) is where the loop
        // gives up rather than trying a 4th fix.
        let rows = gate_and_fix_rows(&state, &a).await;
        assert_eq!(
            rows,
            vec![
                ("test_gate".to_string(), 0, "error".to_string()),
                ("fix".to_string(), 1, "ok".to_string()),
                ("test_gate".to_string(), 1, "error".to_string()),
                ("fix".to_string(), 2, "ok".to_string()),
                ("test_gate".to_string(), 2, "error".to_string()),
                ("fix".to_string(), 3, "ok".to_string()),
                ("test_gate".to_string(), 3, "error".to_string()),
            ]
        );

        // Workspace retained, dirty tree still there, nothing committed.
        let workspace_path = workspace::epic_workspace_path(&state.config.clone_root, &epic_id);
        assert!(
            workspace_path.join(".git").exists(),
            "workspace must be retained on test_gate_exhausted"
        );
        let base_sha = task_base_sha(&state, &a)
            .await
            .expect("base_sha must have been recorded before the implement stage ran");
        let head = git_rev_parse(&workspace_path, "HEAD").await;
        assert_eq!(
            head, base_sha,
            "HEAD must be unchanged from base_sha — nothing was ever committed"
        );
        let status = git::status_porcelain(&workspace_path).await.unwrap();
        assert!(
            !status.trim().is_empty(),
            "the last fix round's dirty tree must still be sitting there, uncommitted"
        );

        cleanup_clone_root(&state, &project_id, &[&epic_id]);
    }

    /// Read `(status, failure_reason)` directly off the task row (bypassing
    /// the public `tasks::Task`/JSON surface, same reasoning as
    /// [`task_base_sha`]).
    async fn fetch_task_row(state: &AppState, task_id: &str) -> (String, Option<String>) {
        let mut rows = state
            .db
            .conn()
            .query(
                "SELECT status, failure_reason FROM task WHERE id = ?1",
                params![task_id],
            )
            .await
            .unwrap();
        let row = rows.next().await.unwrap().unwrap();
        (row.get(0).unwrap(), row.get(1).unwrap())
    }

    /// Read `failure_detail` directly off the task row (Rec 5) — same direct-
    /// SQL discipline as [`fetch_task_row`], so these tests pin what the
    /// executor *persisted* rather than what a DTO projection happens to map.
    async fn fetch_task_detail(state: &AppState, task_id: &str) -> Option<String> {
        let mut rows = state
            .db
            .conn()
            .query(
                "SELECT failure_detail FROM task WHERE id = ?1",
                params![task_id],
            )
            .await
            .unwrap();
        rows.next().await.unwrap().unwrap().get(0).unwrap()
    }

    /// D19's explicit AC: the fix agent's prompt contains the failing test
    /// output and **nothing** from the implement stage's own context — no
    /// spec block, no epic-context heading, no sibling manifest. Proven
    /// against what the `ScriptedTaskAgent` actually recorded, with the
    /// implement stage's recorded prompt checked too (containing the same
    /// markers) so the negative assertion on the fix prompt is meaningful
    /// rather than vacuously true.
    #[tokio::test]
    async fn fix_prompt_contains_only_the_test_output_not_the_implement_stage_context() {
        let agent = Arc::new(
            ScriptedTaskAgent::new()
                .script(Stage::Implement, writes_file("work.txt", "work\n"))
                .script(Stage::Fix, writes_fixed_marker()),
        );
        let recorded = agent.recorded();
        let (state, _app) = test_app_with_task_agent(agent).await;
        let fixture = GitFixture::new().await;
        let project_id =
            seed_project_with_test_cmd(&state, &fixture, FLIPPABLE_TEST_CMD_WITH_MARKER).await;
        let epic_id = seed_epic(&state, &project_id, "InProgress").await;
        let a = seed_task(&state, &epic_id, &project_id, "D19 Task").await;
        set_task_spec(
            &state,
            &a,
            "SPEC_MARKER_ONLY_IN_IMPLEMENT_CONTEXT",
            "ACCEPTANCE_MARKER_ONLY_IN_IMPLEMENT_CONTEXT",
        )
        .await;

        run_epic_pipeline(state.clone(), epic_id.clone()).await;

        assert_eq!(epic_status(&state, &epic_id).await, "InReview");

        let runs = recorded.lock().unwrap();
        let implement_run = runs
            .iter()
            .find(|r| r.stage == Stage::Implement)
            .expect("implement stage must have run");
        let fix_run = runs
            .iter()
            .find(|r| r.stage == Stage::Fix)
            .expect("fix stage must have run");

        // Sanity: the markers really are in the implement stage's prompt —
        // otherwise their absence from the fix prompt would prove nothing.
        assert!(implement_run
            .prompt
            .contains("SPEC_MARKER_ONLY_IN_IMPLEMENT_CONTEXT"));
        assert!(implement_run
            .prompt
            .contains("ACCEPTANCE_MARKER_ONLY_IN_IMPLEMENT_CONTEXT"));
        assert!(implement_run.prompt.contains("## Epic Context"));

        // The headline assertion: the fix prompt has the test output...
        assert!(
            fix_run.prompt.contains("THE_TESTS_ARE_BROKEN_MARKER"),
            "fix prompt must contain the failing test output: {}",
            fix_run.prompt
        );
        // ...and none of the implement stage's own context.
        assert!(!fix_run
            .prompt
            .contains("SPEC_MARKER_ONLY_IN_IMPLEMENT_CONTEXT"));
        assert!(!fix_run
            .prompt
            .contains("ACCEPTANCE_MARKER_ONLY_IN_IMPLEMENT_CONTEXT"));
        assert!(!fix_run.prompt.contains("## Epic Context"));
        assert!(!fix_run.prompt.contains("D19 Task"));

        cleanup_clone_root(&state, &project_id, &[&epic_id]);
    }

    /// Absent `test_cmd` ⇒ no gate at all: zero `test_gate`/`fix` rows, and
    /// the commit still lands (mirrors T-521's identically-named preflight
    /// proof, but for the per-task gate, and with an implement stage that
    /// actually writes something so "commit still happens" is a real
    /// assertion rather than the no-diff-no-commit case).
    #[tokio::test]
    async fn absent_test_cmd_skips_the_gate_and_commits_immediately() {
        let agent = Arc::new(
            ScriptedTaskAgent::new().script(Stage::Implement, writes_file("a.txt", "a\n")),
        );
        let (state, _app) = test_app_with_task_agent(agent).await;
        let fixture = GitFixture::new().await;
        let project_id = seed_project_with_workspace(&state, &fixture).await; // no test_cmd
        let epic_id = seed_epic(&state, &project_id, "InProgress").await;
        let a = seed_task(&state, &epic_id, &project_id, "A").await;

        run_epic_pipeline(state.clone(), epic_id.clone()).await;

        assert_eq!(epic_status(&state, &epic_id).await, "InReview");

        let rows = gate_and_fix_rows(&state, &a).await;
        assert!(
            rows.is_empty(),
            "no test_cmd must mean zero test_gate/fix rows: {rows:?}"
        );

        let branch = epic_branch_name_column(&state, &epic_id).await;
        let subjects = git_log_subjects_for_ref(&fixture.dir, &branch).await;
        assert_eq!(
            subjects,
            vec![
                "init".to_string(),
                format!("impl({}): A", spec::short_id(&a))
            ],
            "the commit must still happen with no gate in the way"
        );

        cleanup_clone_root(&state, &project_id, &[&epic_id]);
    }

    /// A green-first-try gate writes exactly one `test_gate` row at
    /// `attempt = 0` and no `fix` row at all.
    #[tokio::test]
    async fn green_first_try_writes_one_test_gate_row_and_no_fix_row() {
        let agent = Arc::new(
            ScriptedTaskAgent::new().script(Stage::Implement, writes_file("a.txt", "a\n")),
        );
        let (state, _app) = test_app_with_task_agent(agent).await;
        let fixture = GitFixture::new().await;
        let project_id = seed_project_with_test_cmd(&state, &fixture, "true").await;
        let epic_id = seed_epic(&state, &project_id, "InProgress").await;
        let a = seed_task(&state, &epic_id, &project_id, "A").await;

        run_epic_pipeline(state.clone(), epic_id.clone()).await;

        assert_eq!(epic_status(&state, &epic_id).await, "InReview");

        let rows = gate_and_fix_rows(&state, &a).await;
        assert_eq!(rows, vec![("test_gate".to_string(), 0, "ok".to_string())]);

        cleanup_clone_root(&state, &project_id, &[&epic_id]);
    }

    // ==== T-530: Review stage + verdict contract ===========================

    /// Realistic, preamble-laden reviewer output ending in a real `VERDICT:`
    /// line — prose findings with severity tags first, the machine-readable
    /// verdict last (D9), exactly the shape a real Claude Code review turn
    /// produces and `prompts/review.md` asks for.
    fn review_text(preamble: &str, verdict: &str) -> String {
        format!("{preamble}\n\nVERDICT: {verdict}")
    }

    fn review_pass() -> ScriptedRun {
        ScriptedRun {
            text: vec![review_text(
                "Reviewed the cumulative diff against this task's acceptance criteria. \
                 [NIT] `a.txt` — trivial, purely stylistic. Everything the acceptance \
                 criteria require is met; no in-scope correctness/security/data bug remains.",
                "PASS",
            )],
            ..ScriptedRun::default()
        }
    }

    fn review_needs_changes() -> ScriptedRun {
        ScriptedRun {
            text: vec![review_text(
                "[BLOCKING] `a.txt:1` — this violates the stated acceptance criterion; a fix \
                 agent should address it before this slice can ship.",
                "NEEDS_CHANGES",
            )],
            ..ScriptedRun::default()
        }
    }

    /// Like [`review_needs_changes`] but with `marker` baked into the
    /// findings text — the T-531 counterpart to [`unparseable_review`]: lets
    /// a test tell several different rounds' `NEEDS_CHANGES` findings apart
    /// in the retained `agent_run` evidence.
    fn review_needs_changes_marked(marker: &str) -> ScriptedRun {
        ScriptedRun {
            text: vec![review_text(
                &format!(
                    "[BLOCKING] marker={marker} — distinct findings for this round, so a test \
                     can tell rounds apart in the retained evidence."
                ),
                "NEEDS_CHANGES",
            )],
            ..ScriptedRun::default()
        }
    }

    fn review_blocked() -> ScriptedRun {
        ScriptedRun {
            text: vec![review_text(
                "[SPEC-CONFLICT] the acceptance criteria contradict a stated convention; this \
                 needs a human to resolve the spec, not a code fix.",
                "BLOCKED",
            )],
            ..ScriptedRun::default()
        }
    }

    /// A review reply with **no** parseable `VERDICT:` line at all — a
    /// contract miss — carrying `marker` in its text so a test can tell two
    /// separate miss attempts apart in the retained evidence.
    fn unparseable_review(marker: &str) -> ScriptedRun {
        ScriptedRun {
            text: vec![format!(
                "Some findings here, but this reply never ends with a parseable verdict line. \
                 marker={marker}"
            )],
            ..ScriptedRun::default()
        }
    }

    /// `review` `agent_run` rows for `task_id`, oldest first, as `(attempt,
    /// status, verdict, log)` — the T-530 counterpart to
    /// [`gate_and_fix_rows`], reading `log`/`verdict` too (which
    /// `list_runs_for_task` omits) since several tests below need to inspect
    /// both raw retained outputs directly.
    async fn review_rows(
        state: &AppState,
        task_id: &str,
    ) -> Vec<(i64, String, Option<String>, String)> {
        let mut rows = state
            .db
            .conn()
            .query(
                "SELECT attempt, status, verdict, log FROM agent_run \
                 WHERE task_id = ?1 AND stage = 'review' ORDER BY created_at ASC, rowid ASC",
                params![task_id],
            )
            .await
            .unwrap();
        let mut out = Vec::new();
        while let Some(row) = rows.next().await.unwrap() {
            out.push((
                row.get(0).unwrap(),
                row.get(1).unwrap(),
                row.get(2).unwrap(),
                row.get(3).unwrap(),
            ));
        }
        out
    }

    /// Await the next `stage_changed` frame on `sub`, skipping any other
    /// frame type the topic also carries (`dag_updated`/`epic_updated` on
    /// `epic:<id>`, the `RunEvent` firehose on `task:<id>`).
    async fn recv_stage_changed(
        sub: &mut tokio::sync::broadcast::Receiver<crate::hub::Envelope>,
    ) -> Value {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        loop {
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            let frame = tokio::time::timeout(remaining, sub.recv())
                .await
                .expect("never saw a stage_changed frame")
                .unwrap();
            let v: Value = serde_json::from_str(&frame).unwrap();
            if v["type"] == "stage_changed" {
                return v;
            }
        }
    }

    /// `Stage::Review` runs `Ask`-mode with edit tools denied — decided in
    /// T-512 (`task_agent.rs`'s `Stage::run_mode`/`denies_edit_tools`, and
    /// `build_extra_args`'s own tests), asserted again here at the call site
    /// this task wires up, per this task's own AC line ("the reviewer cannot
    /// edit files").
    #[test]
    fn review_stage_runs_ask_mode_with_edit_tools_denied() {
        assert_eq!(Stage::Review.run_mode(), Some(RunMode::Ask));
        assert!(Stage::Review.denies_edit_tools());
    }

    /// The headline AC: a `PASS` review closes the task `Done` end-to-end
    /// through the scripted walk, exactly like a walk with no review stage
    /// at all.
    #[tokio::test]
    async fn pass_review_closes_the_task_done_end_to_end() {
        let agent = Arc::new(
            ScriptedTaskAgent::new()
                .script(Stage::Implement, writes_file("a.txt", "a\n"))
                .script(Stage::Review, review_pass()),
        );
        let (state, _app) = test_app_with_task_agent(agent).await;
        let fixture = GitFixture::new().await;
        let project_id = seed_project_with_workspace(&state, &fixture).await;
        let epic_id = seed_epic(&state, &project_id, "InProgress").await;
        let a = seed_task(&state, &epic_id, &project_id, "A").await;

        run_epic_pipeline(state.clone(), epic_id.clone()).await;

        let task = fetch_task_row(&state, &a).await;
        assert_eq!(task.0, "Done");
        assert_eq!(epic_status(&state, &epic_id).await, "InReview");

        let rows = review_rows(&state, &a).await;
        assert_eq!(
            rows.len(),
            1,
            "exactly one review attempt on a first-try PASS"
        );
        assert_eq!(rows[0].0, 0, "attempt (T-531: the baseline review opens at 0, not 1 — see the module doc's T-531 numbering section)");
        assert_eq!(rows[0].1, "ok", "status");
        assert_eq!(rows[0].2.as_deref(), Some("PASS"), "verdict");

        cleanup_clone_root(&state, &project_id, &[&epic_id]);
    }

    /// A `BLOCKED` verdict on the D9 unit-parser side is covered by
    /// [`crate::spec::parse_verdict`]'s own tests (preamble, severity tags, a
    /// fenced code block that itself mentions "VERDICT:"); the tests below
    /// are the integration half for `NEEDS_CHANGES`, driving the whole
    /// T-531 convergence loop end-to-end. `BLOCKED`'s own integration test
    /// ([`blocked_review_fails_the_task_with_the_blocked_reason`], next)
    /// predates this task and needs no change — a `BLOCKED` verdict fails
    /// immediately regardless of which round it lands on.

    #[tokio::test]
    async fn blocked_review_fails_the_task_with_the_blocked_reason() {
        let agent = Arc::new(
            ScriptedTaskAgent::new()
                .script(Stage::Implement, writes_file("a.txt", "a\n"))
                .script(Stage::Review, review_blocked()),
        );
        let (state, _app) = test_app_with_task_agent(agent).await;
        let fixture = GitFixture::new().await;
        let project_id = seed_project_with_workspace(&state, &fixture).await;
        let epic_id = seed_epic(&state, &project_id, "InProgress").await;
        let a = seed_task(&state, &epic_id, &project_id, "A").await;

        run_epic_pipeline(state.clone(), epic_id.clone()).await;

        let task = fetch_task_row(&state, &a).await;
        assert_eq!(task.0, "Failed");
        assert_eq!(task.1.as_deref(), Some("blocked"));

        let epic = fetch_epic(state.db.conn(), &epic_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(epic.status, "Blocked");
        assert_eq!(epic.blocked_reason.as_deref(), Some("blocked"));

        let rows = review_rows(&state, &a).await;
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].2.as_deref(), Some("BLOCKED"));

        cleanup_clone_root(&state, &project_id, &[&epic_id]);
    }

    // ==== T-531: review -> fix -> re-test -> re-commit loop ================

    /// The headline AC: a `NEEDS_CHANGES` → fix → `PASS` sequence produces
    /// **exactly two** commits on the branch (the initial `impl(...)` and the
    /// one `fix(...) review round 1`, with the frozen §2.8 subjects) and
    /// closes the task `Done`.
    #[tokio::test]
    async fn needs_changes_then_pass_converges_with_two_commits_and_closes_the_task() {
        let agent = Arc::new(
            ScriptedTaskAgent::new()
                .script(Stage::Implement, writes_file("a.txt", "a\n"))
                .script(Stage::Review, review_needs_changes())
                .script(Stage::Fix, writes_file("b.txt", "b\n"))
                .script(Stage::Review, review_pass()),
        );
        let (state, _app) = test_app_with_task_agent(agent).await;
        let fixture = GitFixture::new().await;
        let project_id = seed_project_with_workspace(&state, &fixture).await;
        let epic_id = seed_epic(&state, &project_id, "InProgress").await;
        let a = seed_task(&state, &epic_id, &project_id, "A").await;

        run_epic_pipeline(state.clone(), epic_id.clone()).await;

        let task = fetch_task_row(&state, &a).await;
        assert_eq!(task.0, "Done");
        assert_eq!(epic_status(&state, &epic_id).await, "InReview");

        let branch = epic_branch_name_column(&state, &epic_id).await;
        let subjects = git_log_subjects_for_ref(&fixture.dir, &branch).await;
        assert_eq!(
            subjects,
            vec![
                "init".to_string(),
                format!("impl({}): A", spec::short_id(&a)),
                format!("fix({}) review round 1: A", spec::short_id(&a)),
            ],
            "exactly two Dearborn commits: the initial impl and the one fix round"
        );

        // Baseline review (NEEDS_CHANGES) + the re-review after the fix round
        // (PASS) — both retained as separate rows.
        let rows = review_rows(&state, &a).await;
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].2.as_deref(), Some("NEEDS_CHANGES"));
        assert_eq!(rows[1].2.as_deref(), Some("PASS"));

        cleanup_clone_root(&state, &project_id, &[&epic_id]);
    }

    /// Exceeding `MAX_FIX_ROUNDS` (3 in `Config::for_test`) while the
    /// reviewer keeps returning `NEEDS_CHANGES` fails the task
    /// `Failed(review_not_converged)`, blocks the epic with the identical
    /// reason, and — the headline retained-evidence AC — every one of the
    /// four review rounds' own distinct findings text is still readable in
    /// `agent_run`, none overwritten or dropped. Round 0 (the baseline, no
    /// fix behind it) plus a re-review after each of the 3 permitted fix
    /// rounds is exactly 4 review calls; the 4th's `NEEDS_CHANGES` is what
    /// exceeds the bound (see the module doc's T-531 section for why this
    /// loop always re-reviews the final permitted fix, unlike
    /// `references/ralph-v2.sh`'s own script).
    #[tokio::test]
    async fn exceeding_max_fix_rounds_fails_review_not_converged_with_every_rounds_findings_retained(
    ) {
        let agent = Arc::new(
            ScriptedTaskAgent::new()
                .script(Stage::Implement, writes_file("a.txt", "a\n"))
                .script(Stage::Review, review_needs_changes_marked("round-0"))
                .script(Stage::Review, review_needs_changes_marked("round-1"))
                .script(Stage::Review, review_needs_changes_marked("round-2"))
                .script(Stage::Review, review_needs_changes_marked("round-3")),
        );
        let (state, _app) = test_app_with_task_agent(agent).await;
        let fixture = GitFixture::new().await;
        let project_id = seed_project_with_workspace(&state, &fixture).await;
        let epic_id = seed_epic(&state, &project_id, "InProgress").await;
        let a = seed_task(&state, &epic_id, &project_id, "A").await;

        assert_eq!(
            state.config.executor.max_fix_rounds, 3,
            "this test's marker count assumes Config::for_test's default"
        );

        run_epic_pipeline(state.clone(), epic_id.clone()).await;

        let task = fetch_task_row(&state, &a).await;
        assert_eq!(task.0, "Failed");
        assert_eq!(task.1.as_deref(), Some("review_not_converged"));

        let epic = fetch_epic(state.db.conn(), &epic_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(epic.status, "Blocked");
        assert_eq!(
            epic.blocked_reason.as_deref(),
            Some("review_not_converged"),
            "the epic must carry the identical reason string as the task"
        );

        let rows = review_rows(&state, &a).await;
        assert_eq!(
            rows.len(),
            4,
            "the baseline review plus one re-review per fix round (3 rounds)"
        );
        for (i, marker) in ["round-0", "round-1", "round-2", "round-3"]
            .iter()
            .enumerate()
        {
            assert_eq!(rows[i].2.as_deref(), Some("NEEDS_CHANGES"));
            assert!(
                rows[i].3.contains(marker),
                "round {i}'s own findings text must survive verbatim, unoverwritten by later rounds"
            );
        }

        // Only 3 fix rounds actually ran — the 4th review's NEEDS_CHANGES is
        // what exceeds MAX_FIX_ROUNDS; there is no 4th fix.
        let fix_calls = evidence::list_runs_for_task(state.db.conn(), &a)
            .await
            .unwrap()
            .into_iter()
            .filter(|r| r.stage == "fix")
            .count();
        assert_eq!(fix_calls, 3);

        cleanup_clone_root(&state, &project_id, &[&epic_id]);
    }

    /// A review-driven fix that breaks the tests fails the task rather than
    /// committing red: the fix writes `broken.txt`, which flips the seeded
    /// `test_cmd` red; the unscripted (default, no-op) nested `Stage::Fix`
    /// inside `run_test_gate_loop` never resolves it, so the gate exhausts
    /// `MAX_TEST_FIX_ATTEMPTS` and fails the task `Failed(test_gate_exhausted)`
    /// — reusing T-522's existing exhaustion path unmodified (see the module
    /// doc's "Reusing `run_test_gate_loop` unmodified" section). The
    /// headline negative assertion: the `fix(...) review round 1` commit
    /// never lands — the branch's commit log stops at the initial
    /// `impl(...)` commit.
    #[tokio::test]
    async fn review_driven_fix_that_breaks_tests_fails_the_task_without_committing_red() {
        let agent = Arc::new(
            ScriptedTaskAgent::new()
                .script(Stage::Implement, writes_file("work.txt", "work\n"))
                .script(Stage::Review, review_needs_changes())
                .script(Stage::Fix, writes_file("broken.txt", "oops\n")),
        );
        let (state, _app) = test_app_with_task_agent(agent).await;
        let fixture = GitFixture::new().await;
        // Green on the untouched tree and once `work.txt` (the implement
        // stage's own diff) exists, red forever once `broken.txt` (the
        // review-driven fix's diff) exists — the nested, unscripted
        // test-driven fix never removes it.
        let project_id = seed_project_with_test_cmd(&state, &fixture, "! test -f broken.txt").await;
        let epic_id = seed_epic(&state, &project_id, "InProgress").await;
        let a = seed_task(&state, &epic_id, &project_id, "A").await;

        // Run the pipeline in its own spawned task rather than a direct
        // `.await`: the review-driven fix loop produces the largest per-task
        // async frames in this module (implement + baseline review + the fix
        // round + the nested unscripted test-gate fix attempts all live in
        // one walk), and on the test harness's own thread they overflow the
        // stack — the same hazard `assert_no_push_row`'s and
        // `workspace_error_skips_the_push_cleanly`'s doc comments describe.
        let walk_state = state.clone();
        let walk_epic = epic_id.clone();
        tokio::spawn(async move {
            run_epic_pipeline(walk_state, walk_epic).await;
        })
        .await
        .unwrap();

        let task = fetch_task_row(&state, &a).await;
        assert_eq!(task.0, "Failed", "the task itself must be Failed");
        assert_eq!(task.1.as_deref(), Some("test_gate_exhausted"));

        let epic = fetch_epic(state.db.conn(), &epic_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(epic.status, "Blocked");
        assert_eq!(epic.blocked_reason.as_deref(), Some("test_gate_exhausted"));

        // Workspace retained (a Failed/Blocked walk never pushes); the
        // review-round fix's diff was never staged or committed.
        let workspace_path = workspace::epic_workspace_path(&state.config.clone_root, &epic_id);
        let subjects = git_log_subjects(&workspace_path).await;
        assert_eq!(
            subjects,
            vec![
                "init".to_string(),
                format!("impl({}): A", spec::short_id(&a))
            ],
            "the review-round fix's broken diff must never be committed"
        );
        assert!(
            !subjects.iter().any(|s| s.contains("review round")),
            "no fix(...) review round commit must land when the fix broke the tests"
        );

        cleanup_clone_root(&state, &project_id, &[&epic_id]);
    }

    /// Each round re-reviews the **cumulative** diff, against the same
    /// `base_sha` every time (the AC's own wording, and D9): the recorded
    /// review prompt for both the baseline round and the re-review after the
    /// one fix round instructs the agent to `git diff <base_sha>..HEAD`
    /// against the identical SHA — `base_sha` never advances mid-task.
    #[tokio::test]
    async fn every_review_round_prompt_carries_the_identical_base_sha() {
        let agent = Arc::new(
            ScriptedTaskAgent::new()
                .script(Stage::Implement, writes_file("a.txt", "a\n"))
                .script(Stage::Review, review_needs_changes())
                .script(Stage::Fix, writes_file("b.txt", "b\n"))
                .script(Stage::Review, review_pass()),
        );
        let recorded = agent.recorded();
        let (state, _app) = test_app_with_task_agent(agent).await;
        let fixture = GitFixture::new().await;
        let project_id = seed_project_with_workspace(&state, &fixture).await;
        let epic_id = seed_epic(&state, &project_id, "InProgress").await;
        let a = seed_task(&state, &epic_id, &project_id, "A").await;

        run_epic_pipeline(state.clone(), epic_id.clone()).await;

        let base_sha = task_base_sha(&state, &a)
            .await
            .expect("base_sha must have been recorded before the implement stage ran");

        let review_runs: Vec<_> = recorded
            .lock()
            .unwrap()
            .iter()
            .filter(|r| r.stage == Stage::Review)
            .cloned()
            .collect();
        assert_eq!(
            review_runs.len(),
            2,
            "baseline review + the re-review after the one fix round"
        );
        for run in &review_runs {
            assert!(
                run.prompt.contains(&format!("git diff {base_sha}..HEAD")),
                "every round must instruct the agent to diff against the SAME base_sha"
            );
        }

        cleanup_clone_root(&state, &project_id, &[&epic_id]);
    }

    /// §2.6's `stage_changed` frame keeps publishing across rounds, with the
    /// T-531 numbering scheme visible in `attempt`: the baseline review
    /// publishes `attempt=0`/`verdict=NEEDS_CHANGES`, and the re-review after
    /// the one fix round publishes `attempt=1`/`verdict=PASS` (sharing its
    /// number with the fix that produced it — see the module doc).
    #[tokio::test]
    async fn stage_changed_publishes_for_every_review_round_with_the_scheme_numbering() {
        let agent = Arc::new(
            ScriptedTaskAgent::new()
                .script(Stage::Implement, writes_file("a.txt", "a\n"))
                .script(Stage::Review, review_needs_changes())
                .script(Stage::Fix, writes_file("b.txt", "b\n"))
                .script(Stage::Review, review_pass()),
        );
        let (state, _app) = test_app_with_task_agent(agent).await;
        let fixture = GitFixture::new().await;
        let project_id = seed_project_with_workspace(&state, &fixture).await;
        let epic_id = seed_epic(&state, &project_id, "InProgress").await;
        let a = seed_task(&state, &epic_id, &project_id, "A").await;

        let mut task_sub = state.hub.subscribe(&format!("task:{a}"));

        run_epic_pipeline(state.clone(), epic_id.clone()).await;
        assert_eq!(epic_status(&state, &epic_id).await, "InReview");

        let first = recv_stage_changed(&mut task_sub).await;
        assert_eq!(first["payload"]["stage"], "review");
        assert_eq!(first["payload"]["attempt"], 0);
        assert_eq!(first["payload"]["verdict"], "NEEDS_CHANGES");

        let second = recv_stage_changed(&mut task_sub).await;
        assert_eq!(second["payload"]["stage"], "review");
        assert_eq!(second["payload"]["attempt"], 1);
        assert_eq!(second["payload"]["verdict"], "PASS");

        cleanup_clone_root(&state, &project_id, &[&epic_id]);
    }

    /// The headline negative-path AC: a contract miss (no parseable
    /// `VERDICT:` line at all) triggers **exactly one** re-run — bounded by
    /// `config.executor.verdict_retries` (1 in `Config::for_test`, matching
    /// the §2.7 default) — and, still unparseable after that, the task fails
    /// `Failed(agent_error)` with the epic `Blocked(agent_error)`. Both raw
    /// outputs (the miss and the re-run) survive as separate `agent_run`
    /// rows, and the re-run's recorded prompt carries
    /// `VERDICT_CONTRACT_REMINDER`.
    #[tokio::test]
    async fn contract_miss_triggers_exactly_one_rerun_then_fails_with_both_outputs_retained() {
        let agent = Arc::new(
            ScriptedTaskAgent::new()
                .script(Stage::Implement, writes_file("a.txt", "a\n"))
                .script(Stage::Review, unparseable_review("first-miss"))
                .script(Stage::Review, unparseable_review("second-miss")),
        );
        let recorded = agent.recorded();
        let (state, _app) = test_app_with_task_agent(agent).await;
        let fixture = GitFixture::new().await;
        let project_id = seed_project_with_workspace(&state, &fixture).await;
        let epic_id = seed_epic(&state, &project_id, "InProgress").await;
        let a = seed_task(&state, &epic_id, &project_id, "A").await;

        run_epic_pipeline(state.clone(), epic_id.clone()).await;

        let task = fetch_task_row(&state, &a).await;
        assert_eq!(task.0, "Failed");
        assert_eq!(task.1.as_deref(), Some("agent_error"));

        let epic = fetch_epic(state.db.conn(), &epic_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(epic.status, "Blocked");
        assert_eq!(epic.blocked_reason.as_deref(), Some("agent_error"));

        // Exactly one re-run: two review attempts total, never a third.
        let review_calls: Vec<_> = recorded
            .lock()
            .unwrap()
            .iter()
            .filter(|r| r.stage == Stage::Review)
            .cloned()
            .collect();
        assert_eq!(
            review_calls.len(),
            2,
            "exactly one re-run after the first contract miss"
        );
        assert!(
            !review_calls[0].prompt.contains("Contract reminder"),
            "the first attempt's prompt must not carry the reminder"
        );
        assert!(
            review_calls[1].prompt.contains(VERDICT_CONTRACT_REMINDER),
            "the re-run's prompt must carry the named contract-reminder constant"
        );

        // Both raw outputs retained as two separate agent_run rows, in order,
        // each with the terminal status recorded and no verdict (neither
        // ever parsed).
        let rows = review_rows(&state, &a).await;
        assert_eq!(rows.len(), 2);
        assert_eq!(
            rows[0].0, 0,
            "first attempt (T-531: the baseline review opens at 0 — see the module doc's T-531 numbering section)"
        );
        assert_eq!(
            rows[0].1, "ok",
            "the agent itself exited cleanly, just with no verdict line"
        );
        assert_eq!(rows[0].2, None);
        assert!(rows[0].3.contains("first-miss"));
        assert_eq!(rows[1].0, 1, "second attempt (the bounded re-run)");
        assert_eq!(rows[1].2, None);
        assert!(rows[1].3.contains("second-miss"));

        cleanup_clone_root(&state, &project_id, &[&epic_id]);
    }

    /// §2.6: `stage_changed` publishes on **both** `task:<id>` (fine-grained)
    /// and `epic:<id>` (coarse) with the identical `{ task_id, stage,
    /// attempt, status, verdict }` payload once the review verdict is known.
    #[tokio::test]
    async fn review_verdict_publishes_stage_changed_on_task_and_epic_topics() {
        let agent = Arc::new(
            ScriptedTaskAgent::new()
                .script(Stage::Implement, writes_file("a.txt", "a\n"))
                .script(Stage::Review, review_pass()),
        );
        let (state, _app) = test_app_with_task_agent(agent).await;
        let fixture = GitFixture::new().await;
        let project_id = seed_project_with_workspace(&state, &fixture).await;
        let epic_id = seed_epic(&state, &project_id, "InProgress").await;
        let a = seed_task(&state, &epic_id, &project_id, "A").await;

        let mut task_sub = state.hub.subscribe(&format!("task:{a}"));
        let mut epic_sub = state.hub.subscribe(&format!("epic:{epic_id}"));

        run_epic_pipeline(state.clone(), epic_id.clone()).await;
        assert_eq!(epic_status(&state, &epic_id).await, "InReview");

        let task_frame = recv_stage_changed(&mut task_sub).await;
        assert_eq!(task_frame["topic"], format!("task:{a}"));
        assert_eq!(task_frame["payload"]["task_id"], a);
        assert_eq!(task_frame["payload"]["stage"], "review");
        assert_eq!(
            task_frame["payload"]["attempt"], 0,
            "T-531: the baseline review opens at attempt 0 — see the module doc's T-531 numbering section"
        );
        assert_eq!(task_frame["payload"]["status"], "ok");
        assert_eq!(task_frame["payload"]["verdict"], "PASS");

        let epic_frame = recv_stage_changed(&mut epic_sub).await;
        assert_eq!(epic_frame["topic"], format!("epic:{epic_id}"));
        assert_eq!(
            epic_frame["payload"], task_frame["payload"],
            "the epic topic carries the identical payload, just coarse"
        );

        cleanup_clone_root(&state, &project_id, &[&epic_id]);
    }

    /// The verdict lands on the `agent_run` row and is visible through `GET
    /// /tasks/{id}/runs` — the read path a human (or the client) actually
    /// uses to see *why* a task closed the way it did.
    #[tokio::test]
    async fn review_verdict_is_visible_through_the_task_runs_endpoint() {
        let agent = Arc::new(
            ScriptedTaskAgent::new()
                .script(Stage::Implement, writes_file("a.txt", "a\n"))
                .script(Stage::Review, review_pass()),
        );
        let (state, app) = test_app_with_task_agent(agent).await;
        let fixture = GitFixture::new().await;
        let project_id = seed_project_with_workspace(&state, &fixture).await;
        let epic_id = seed_epic(&state, &project_id, "InProgress").await;
        let a = seed_task(&state, &epic_id, &project_id, "A").await;

        run_epic_pipeline(state.clone(), epic_id.clone()).await;
        assert_eq!(epic_status(&state, &epic_id).await, "InReview");

        let response = app
            .oneshot(req("GET", &format!("/tasks/{a}/runs"), None))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = body_json(response).await;
        let items = body["items"].as_array().unwrap();
        let review_item = items
            .iter()
            .find(|r| r["stage"] == "review")
            .expect("a review run must be listed");
        assert_eq!(review_item["verdict"], "PASS");
        assert_eq!(review_item["status"], "ok");
        assert_eq!(
            review_item["attempt"], 0,
            "T-531: the baseline review opens at attempt 0 — see the module doc's T-531 numbering section"
        );

        cleanup_clone_root(&state, &project_id, &[&epic_id]);
    }

    /// The base-SHA context extension (item 1 of this task): the review
    /// stage's prompt carries the exact task `base_sha` and the `git diff
    /// <sha>..HEAD` instruction, closing the gap `prompts/review.md`
    /// promised but `spec::build_context` didn't yet deliver.
    #[tokio::test]
    async fn review_prompt_includes_the_recorded_base_sha_diff_instruction() {
        let agent = Arc::new(
            ScriptedTaskAgent::new()
                .script(Stage::Implement, writes_file("a.txt", "a\n"))
                .script(Stage::Review, review_pass()),
        );
        let recorded = agent.recorded();
        let (state, _app) = test_app_with_task_agent(agent).await;
        let fixture = GitFixture::new().await;
        let project_id = seed_project_with_workspace(&state, &fixture).await;
        let epic_id = seed_epic(&state, &project_id, "InProgress").await;
        let a = seed_task(&state, &epic_id, &project_id, "A").await;

        run_epic_pipeline(state.clone(), epic_id.clone()).await;

        let base_sha = task_base_sha(&state, &a)
            .await
            .expect("base_sha must have been recorded before the implement stage ran");

        let runs = recorded.lock().unwrap();
        let review_run = runs
            .iter()
            .find(|r| r.stage == Stage::Review)
            .expect("the review stage must have run");
        assert!(review_run.prompt.contains("## Base Commit"));
        assert!(review_run.prompt.contains(&base_sha));
        assert!(review_run
            .prompt
            .contains(&format!("git diff {base_sha}..HEAD")));

        // The implement stage's own prompt never mentions base_sha — only
        // Review's context does (spec::TaskContext::base_sha is None there).
        let implement_run = runs
            .iter()
            .find(|r| r.stage == Stage::Implement)
            .expect("the implement stage must have run");
        assert!(!implement_run.prompt.contains("## Base Commit"));

        cleanup_clone_root(&state, &project_id, &[&epic_id]);
    }

    // ==== T-532: already-complete verification ==============================

    fn verify_complete_text(preamble: &str, verdict: &str) -> String {
        format!("{preamble}\n\nVERDICT: {verdict}")
    }

    fn verify_complete_pass() -> ScriptedRun {
        ScriptedRun {
            text: vec![verify_complete_text(
                "Walked every acceptance criterion against the current codebase: the endpoint \
                 is wired up at routes.rs:42 and covered by a test at routes_test.rs:10. \
                 Everything the acceptance criteria require is genuinely present, correct, and \
                 already built by an earlier task.",
                "PASS",
            )],
            ..ScriptedRun::default()
        }
    }

    fn verify_complete_needs_changes() -> ScriptedRun {
        ScriptedRun {
            text: vec![verify_complete_text(
                "[BLOCKING] the acceptance criteria require a `/widgets` endpoint, but no such \
                 route exists anywhere in the codebase — the claimed-already-done work does not \
                 actually exist. A fix agent needs to add it.",
                "NEEDS_CHANGES",
            )],
            ..ScriptedRun::default()
        }
    }

    fn verify_complete_blocked() -> ScriptedRun {
        ScriptedRun {
            text: vec![verify_complete_text(
                "[SPEC-CONFLICT] the acceptance criteria contradict each other; a human must \
                 resolve the spec before this slice can be verified either way.",
                "BLOCKED",
            )],
            ..ScriptedRun::default()
        }
    }

    /// A verify-complete reply with **no** parseable `VERDICT:` line — the
    /// T-532 counterpart to [`unparseable_review`].
    fn unparseable_verify_complete(marker: &str) -> ScriptedRun {
        ScriptedRun {
            text: vec![format!(
                "Some findings here, but this reply never ends with a parseable verdict line. \
                 marker={marker}"
            )],
            ..ScriptedRun::default()
        }
    }

    /// `verify_complete` `agent_run` rows for `task_id`, oldest first, as
    /// `(attempt, status, verdict, log)` — the T-532 counterpart to
    /// [`review_rows`].
    async fn verify_complete_rows(
        state: &AppState,
        task_id: &str,
    ) -> Vec<(i64, String, Option<String>, String)> {
        let mut rows = state
            .db
            .conn()
            .query(
                "SELECT attempt, status, verdict, log FROM agent_run \
                 WHERE task_id = ?1 AND stage = 'verify_complete' ORDER BY created_at ASC, rowid ASC",
                params![task_id],
            )
            .await
            .unwrap();
        let mut out = Vec::new();
        while let Some(row) = rows.next().await.unwrap() {
            out.push((
                row.get(0).unwrap(),
                row.get(1).unwrap(),
                row.get(2).unwrap(),
                row.get(3).unwrap(),
            ));
        }
        out
    }

    /// `Stage::VerifyComplete` runs `Ask`-mode with edit tools denied —
    /// decided in T-512 (`task_agent.rs`), asserted again here at the call
    /// site this task wires up, mirroring
    /// [`review_stage_runs_ask_mode_with_edit_tools_denied`] for the second
    /// verdict-emitting stage.
    #[test]
    fn verify_complete_stage_runs_ask_mode_with_edit_tools_denied() {
        assert_eq!(Stage::VerifyComplete.run_mode(), Some(RunMode::Ask));
        assert!(Stage::VerifyComplete.denies_edit_tools());
    }

    /// The headline PASS AC: an implement stage that writes nothing, followed
    /// by a `PASS` verify-complete verdict, closes the task `Done` with the
    /// branch's commit count **unchanged**, and the verdict is visible
    /// through `GET /tasks/{id}/runs` — the AC's "a human can see *why*
    /// nothing was built".
    #[tokio::test]
    async fn verify_complete_pass_closes_the_task_done_with_zero_commits_and_is_visible_in_run_history(
    ) {
        // No Stage::Implement script: the default ScriptedRun writes no
        // files, so the implement stage produces no diff and this branch
        // routes to Stage::VerifyComplete.
        let agent = Arc::new(
            ScriptedTaskAgent::new().script(Stage::VerifyComplete, verify_complete_pass()),
        );
        let (state, app) = test_app_with_task_agent(agent).await;
        let fixture = GitFixture::new().await;
        let project_id = seed_project_with_workspace(&state, &fixture).await;
        let epic_id = seed_epic(&state, &project_id, "InProgress").await;
        let a = seed_task(&state, &epic_id, &project_id, "A").await;

        run_epic_pipeline(state.clone(), epic_id.clone()).await;

        let task = fetch_task_row(&state, &a).await;
        assert_eq!(task.0, "Done");
        assert_eq!(epic_status(&state, &epic_id).await, "InReview");

        let branch = epic_branch_name_column(&state, &epic_id).await;
        let subjects = git_log_subjects_for_ref(&fixture.dir, &branch).await;
        assert_eq!(
            subjects,
            vec!["init".to_string()],
            "PASS must leave the branch's commit count unchanged"
        );

        let rows = verify_complete_rows(&state, &a).await;
        assert_eq!(rows.len(), 1, "exactly one verify-complete call on a PASS");
        assert_eq!(
            rows[0].0, 0,
            "T-532: the sole verify-complete call opens at attempt 0, mirroring the baseline review"
        );
        assert_eq!(rows[0].1, "ok");
        assert_eq!(rows[0].2.as_deref(), Some("PASS"));

        // No commit and no review ever ran on this path.
        let mut commit_rows = state
            .db
            .conn()
            .query(
                "SELECT COUNT(*) FROM agent_run WHERE task_id = ?1 AND stage IN ('commit', 'review')",
                params![a.clone()],
            )
            .await
            .unwrap();
        let commit_count: i64 = commit_rows.next().await.unwrap().unwrap().get(0).unwrap();
        assert_eq!(commit_count, 0);

        // The AC's own words: visible through the run-history endpoint.
        let response = app
            .oneshot(req("GET", &format!("/tasks/{a}/runs"), None))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = body_json(response).await;
        let items = body["items"].as_array().unwrap();
        let vc_item = items
            .iter()
            .find(|r| r["stage"] == "verify_complete")
            .expect("a verify_complete run must be listed");
        assert_eq!(vc_item["verdict"], "PASS");
        assert_eq!(vc_item["status"], "ok");

        cleanup_clone_root(&state, &project_id, &[&epic_id]);
    }

    /// The headline NEEDS_CHANGES AC: a `NEEDS_CHANGES` verify-complete
    /// verdict routes its findings to `Stage::Fix` (D19: the fix agent's
    /// **only** context), whose diff then goes through the ordinary T-522
    /// test gate, lands one `impl(...)` commit, and converges through the
    /// unmodified T-530/T-531 review loop to close the task `Done` — exactly
    /// "re-enter the normal pipeline" (MILESTONE_2 §6).
    #[tokio::test]
    async fn verify_complete_needs_changes_routes_to_fix_and_reenters_the_normal_pipeline() {
        let agent = Arc::new(
            ScriptedTaskAgent::new()
                .script(Stage::VerifyComplete, verify_complete_needs_changes())
                .script(Stage::Fix, writes_file("widgets.rs", "route\n"))
                .script(Stage::Review, review_pass()),
        );
        let recorded = agent.recorded();
        let (state, _app) = test_app_with_task_agent(agent).await;
        let fixture = GitFixture::new().await;
        let project_id = seed_project_with_workspace(&state, &fixture).await;
        let epic_id = seed_epic(&state, &project_id, "InProgress").await;
        let a = seed_task(&state, &epic_id, &project_id, "A").await;

        run_epic_pipeline(state.clone(), epic_id.clone()).await;

        let task = fetch_task_row(&state, &a).await;
        assert_eq!(task.0, "Done");
        assert_eq!(epic_status(&state, &epic_id).await, "InReview");

        // Exactly one commit lands — the fix's diff, using the SAME §2.8
        // `impl(...)` subject as an ordinary Stage::Implement commit would,
        // because this fix's diff *is* the task's first real commit.
        let branch = epic_branch_name_column(&state, &epic_id).await;
        let subjects = git_log_subjects_for_ref(&fixture.dir, &branch).await;
        assert_eq!(
            subjects,
            vec![
                "init".to_string(),
                format!("impl({}): A", spec::short_id(&a))
            ],
            "the verify-complete-driven fix's diff must land as the task's impl(...) commit"
        );

        // The fix agent's prompt carried the verifier's own findings and
        // nothing else (D19) — the same assertion style
        // fix_prompt_contains_only_the_test_output_not_the_implement_stage_context
        // uses for T-522's test-driven fix. Scoped to its own block so the
        // `MutexGuard` is dropped before the `.await`s below.
        {
            let runs = recorded.lock().unwrap();
            let fix_run = runs
                .iter()
                .find(|r| r.stage == Stage::Fix)
                .expect("the fix stage must have run");
            assert!(
                fix_run.prompt.contains("/widgets` endpoint"),
                "the fix prompt must carry the verifier's own findings: {:?}",
                fix_run.prompt
            );
            assert!(
                !fix_run.prompt.contains("Acceptance Criteria"),
                "the fix prompt must not carry the spec/epic/sibling context Implement gets"
            );
        }

        // The verify-complete call itself: attempt 0, NEEDS_CHANGES.
        let vc_rows = verify_complete_rows(&state, &a).await;
        assert_eq!(vc_rows.len(), 1);
        assert_eq!(vc_rows[0].0, 0);
        assert_eq!(vc_rows[0].2.as_deref(), Some("NEEDS_CHANGES"));

        // The test gate ran (green first try) and the ordinary review loop
        // ran on top of the new commit and passed.
        let review_rows = review_rows(&state, &a).await;
        assert_eq!(
            review_rows.len(),
            1,
            "the ordinary review loop runs once more, starting fresh at attempt 0"
        );
        assert_eq!(review_rows[0].0, 0);
        assert_eq!(review_rows[0].2.as_deref(), Some("PASS"));

        cleanup_clone_root(&state, &project_id, &[&epic_id]);
    }

    /// The headline BLOCKED AC: the task fails `Failed(blocked)`, the epic
    /// blocks with the identical reason, nothing is ever committed, and the
    /// workspace is retained (a Blocked walk never pushes).
    #[tokio::test]
    async fn verify_complete_blocked_fails_the_task_blocks_the_epic_and_commits_nothing() {
        let agent = Arc::new(
            ScriptedTaskAgent::new().script(Stage::VerifyComplete, verify_complete_blocked()),
        );
        let (state, _app) = test_app_with_task_agent(agent).await;
        let fixture = GitFixture::new().await;
        let project_id = seed_project_with_workspace(&state, &fixture).await;
        let epic_id = seed_epic(&state, &project_id, "InProgress").await;
        let a = seed_task(&state, &epic_id, &project_id, "A").await;

        run_epic_pipeline(state.clone(), epic_id.clone()).await;

        let task = fetch_task_row(&state, &a).await;
        assert_eq!(task.0, "Failed");
        assert_eq!(task.1.as_deref(), Some("blocked"));

        let epic = fetch_epic(state.db.conn(), &epic_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(epic.status, "Blocked");
        assert_eq!(epic.blocked_reason.as_deref(), Some("blocked"));

        let rows = verify_complete_rows(&state, &a).await;
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].2.as_deref(), Some("BLOCKED"));

        // Nothing committed, workspace retained (a Failed/Blocked walk never
        // deletes the workspace or pushes).
        let workspace_path = workspace::epic_workspace_path(&state.config.clone_root, &epic_id);
        let subjects = git_log_subjects(&workspace_path).await;
        assert_eq!(
            subjects,
            vec!["init".to_string()],
            "nothing must ever be committed on a BLOCKED verdict"
        );

        cleanup_clone_root(&state, &project_id, &[&epic_id]);
    }

    /// A contract miss (no parseable `VERDICT:` line) on `Stage::VerifyComplete`
    /// behaves exactly like T-530's review contract miss: **exactly one**
    /// bounded re-run, then `Failed(agent_error)` with both raw outputs
    /// retained as separate `agent_run` rows.
    #[tokio::test]
    async fn verify_complete_contract_miss_triggers_exactly_one_rerun_then_fails_with_both_outputs_retained(
    ) {
        let agent = Arc::new(
            ScriptedTaskAgent::new()
                .script(
                    Stage::VerifyComplete,
                    unparseable_verify_complete("first-miss"),
                )
                .script(
                    Stage::VerifyComplete,
                    unparseable_verify_complete("second-miss"),
                ),
        );
        let recorded = agent.recorded();
        let (state, _app) = test_app_with_task_agent(agent).await;
        let fixture = GitFixture::new().await;
        let project_id = seed_project_with_workspace(&state, &fixture).await;
        let epic_id = seed_epic(&state, &project_id, "InProgress").await;
        let a = seed_task(&state, &epic_id, &project_id, "A").await;

        run_epic_pipeline(state.clone(), epic_id.clone()).await;

        let task = fetch_task_row(&state, &a).await;
        assert_eq!(task.0, "Failed");
        assert_eq!(task.1.as_deref(), Some("agent_error"));

        let epic = fetch_epic(state.db.conn(), &epic_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(epic.status, "Blocked");
        assert_eq!(epic.blocked_reason.as_deref(), Some("agent_error"));

        let vc_calls: Vec<_> = recorded
            .lock()
            .unwrap()
            .iter()
            .filter(|r| r.stage == Stage::VerifyComplete)
            .cloned()
            .collect();
        assert_eq!(
            vc_calls.len(),
            2,
            "exactly one re-run after the first contract miss"
        );
        assert!(!vc_calls[0].prompt.contains("Contract reminder"));
        assert!(vc_calls[1].prompt.contains(VERDICT_CONTRACT_REMINDER));

        let rows = verify_complete_rows(&state, &a).await;
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].0, 0);
        assert_eq!(rows[0].2, None);
        assert!(rows[0].3.contains("first-miss"));
        assert_eq!(rows[1].0, 1);
        assert_eq!(rows[1].2, None);
        assert!(rows[1].3.contains("second-miss"));

        cleanup_clone_root(&state, &project_id, &[&epic_id]);
    }

    /// Edge case documented in the module doc's T-532 section: a
    /// `NEEDS_CHANGES`-driven fix that itself produces no diff (the fix agent
    /// declined to act on the verifier's findings) fails the task rather than
    /// silently closing it `Done` — never trusting a fix that didn't actually
    /// fix anything.
    #[tokio::test]
    async fn verify_complete_needs_changes_with_a_no_op_fix_fails_rather_than_closing_done() {
        // No Stage::Fix script: the default ScriptedRun writes no files, so
        // the verify-complete-driven fix produces no diff either.
        let agent = Arc::new(
            ScriptedTaskAgent::new().script(Stage::VerifyComplete, verify_complete_needs_changes()),
        );
        let (state, _app) = test_app_with_task_agent(agent).await;
        let fixture = GitFixture::new().await;
        let project_id = seed_project_with_workspace(&state, &fixture).await;
        let epic_id = seed_epic(&state, &project_id, "InProgress").await;
        let a = seed_task(&state, &epic_id, &project_id, "A").await;

        run_epic_pipeline(state.clone(), epic_id.clone()).await;

        let task = fetch_task_row(&state, &a).await;
        assert_eq!(
            task.0, "Failed",
            "a no-op fix must never leave the task Done"
        );
        assert_eq!(task.1.as_deref(), Some("agent_error"));

        let epic = fetch_epic(state.db.conn(), &epic_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(epic.status, "Blocked");

        let workspace_path = workspace::epic_workspace_path(&state.config.clone_root, &epic_id);
        let subjects = git_log_subjects(&workspace_path).await;
        assert_eq!(
            subjects,
            vec!["init".to_string()],
            "nothing must ever be committed"
        );

        cleanup_clone_root(&state, &project_id, &[&epic_id]);
    }

    // ==== T-540: structured failure & Blocked ================================

    /// [`FailureReason::as_str`] frozen against the literal §2.3 vocabulary —
    /// independent of the `ALL`-iterating test below, which only proves each
    /// variant is *distinguishable*, not that its string matches the milestone
    /// doc (mirrors `task_agent.rs`'s own
    /// `as_str_matches_the_section_2_2_table` convention for `Stage`).
    #[test]
    fn failure_reason_as_str_matches_the_section_2_3_vocabulary() {
        assert_eq!(FailureReason::PreflightRed.as_str(), "preflight_red");
        assert_eq!(FailureReason::SetupFailed.as_str(), "setup_failed");
        assert_eq!(FailureReason::WorkspaceError.as_str(), "workspace_error");
        assert_eq!(
            FailureReason::TestGateExhausted.as_str(),
            "test_gate_exhausted"
        );
        assert_eq!(
            FailureReason::ReviewNotConverged.as_str(),
            "review_not_converged"
        );
        assert_eq!(FailureReason::Blocked.as_str(), "blocked");
        assert_eq!(FailureReason::AgentError.as_str(), "agent_error");
        assert_eq!(FailureReason::Timeout.as_str(), "timeout");
        assert_eq!(FailureReason::Cancelled.as_str(), "cancelled");
        assert_eq!(FailureReason::PrFailed.as_str(), "pr_failed");
        assert_eq!(
            FailureReason::ProviderRateLimited.as_str(),
            "provider_rate_limited"
        );
    }

    /// The AC's "every §2.3 reason reaches this path": every [`FailureReason`]
    /// variant, driven directly through [`fail_item`], lands correctly in
    /// both shapes a real call site ever uses — task-scoped (`task_id:
    /// Some`, covering `agent_error`/`test_gate_exhausted`/
    /// `review_not_converged`/`blocked`/T-543's `timeout`, and — though no
    /// real call site ever does this, per `FailureReason::Cancelled`'s own
    /// doc — `cancelled`) and no-task (`task_id: None`, covering
    /// `preflight_red`/`setup_failed`/`workspace_error`/`pr_failed`).
    #[tokio::test]
    async fn every_section_2_3_reason_reaches_fail_item_and_lands_correctly() {
        let (state, _app) = test_app().await;
        let project_id = seed_project(&state).await;

        for reason in FailureReason::ALL {
            // Task-scoped shape.
            let epic_id = seed_epic(&state, &project_id, "InProgress").await;
            let task_id = seed_task(&state, &epic_id, &project_id, "A").await;
            set_task_status(&state, &task_id, "InProgress").await;

            fail_item(
                &state,
                FailureContext {
                    epic_id: Some(&epic_id),
                    task_id: Some(&task_id),
                    reason,
                    message: "task-scoped test failure",
                    push: PushIntent::Skip,
                },
            )
            .await;

            let task = fetch_task_row(&state, &task_id).await;
            assert_eq!(task.0, "Failed", "{reason:?}: task must reach Failed");
            assert_eq!(
                task.1.as_deref(),
                Some(reason.as_str()),
                "{reason:?}: task.failure_reason must match the router's reason"
            );

            let epic = fetch_epic(state.db.conn(), &epic_id)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(
                epic.status, "Blocked",
                "{reason:?}: epic must reach Blocked"
            );
            assert_eq!(
                epic.blocked_reason.as_deref(),
                Some(reason.as_str()),
                "{reason:?}: epic.blocked_reason must match the task's failure_reason"
            );

            // No-task shape: a fresh epic, no task at all.
            let epic_id_no_task = seed_epic(&state, &project_id, "InProgress").await;
            fail_item(
                &state,
                FailureContext {
                    epic_id: Some(&epic_id_no_task),
                    task_id: None,
                    reason,
                    message: "no-task test failure",
                    push: PushIntent::Skip,
                },
            )
            .await;
            let epic_no_task = fetch_epic(state.db.conn(), &epic_id_no_task)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(
                epic_no_task.status, "Blocked",
                "{reason:?}: no-task epic must reach Blocked"
            );
            assert_eq!(
                epic_no_task.blocked_reason.as_deref(),
                Some(reason.as_str())
            );
        }
    }

    // ---- Rec 5: triageable failures (`failure_detail`) ----------------------

    /// Rec 5's persistence contract, driven directly through [`fail_item`]:
    /// the failure message lands as `failure_detail` on both containers —
    /// **redacted** (URL userinfo stripped even with no project PAT) and
    /// **capped** (an over-cap message keeps exactly
    /// [`FAILURE_DETAIL_CAP_CHARS`] chars, head + tail around the elision
    /// marker, never a raw byte-slice through a multi-byte char).
    #[tokio::test]
    async fn fail_item_persists_redacted_and_capped_failure_detail() {
        let (state, _app) = test_app().await;
        let project_id = seed_project(&state).await;
        let epic_id = seed_epic(&state, &project_id, "InProgress").await;
        let task_id = seed_task(&state, &epic_id, &project_id, "A").await;
        set_task_status(&state, &task_id, "InProgress").await;

        // Over-cap message whose head carries a credentialed URL and whose
        // tail is a distinctive marker: both ends must survive capping, and
        // the secret must not.
        let secret = "super-secret-token";
        let mut message = String::new();
        message.push_str("clone failed: https://ci-bot:");
        message.push_str(secret);
        message.push_str("@github.com/acme/demo.git — ");
        message.push_str(&"x".repeat(3000));
        message.push_str("TAIL_MARKER: exit code 128");

        fail_item(
            &state,
            FailureContext {
                epic_id: Some(&epic_id),
                task_id: Some(&task_id),
                reason: FailureReason::AgentError,
                message: &message,
                push: PushIntent::Skip,
            },
        )
        .await;

        let detail = fetch_task_detail(&state, &task_id)
            .await
            .expect("fail_item must persist failure_detail on the task");
        assert_eq!(
            detail.chars().count(),
            FAILURE_DETAIL_CAP_CHARS,
            "an over-cap message is capped to exactly {FAILURE_DETAIL_CAP_CHARS} chars"
        );
        assert!(detail.contains(FAILURE_DETAIL_ELISION_MARKER));
        // Redaction: the PAT-less path still strips URL userinfo...
        assert!(detail.contains("https://***@github.com"), "{detail}");
        assert!(!detail.contains(secret), "the token must not survive");
        // ...and both informative ends survive the cap.
        assert!(detail.starts_with("clone failed:"), "{detail}");
        assert!(detail.ends_with("TAIL_MARKER: exit code 128"), "{detail}");

        // The epic carries the same redacted, capped text alongside its own
        // blocked_reason.
        let epic = fetch_epic(state.db.conn(), &epic_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(epic.failure_detail.as_deref(), Some(detail.as_str()));

        // A short message is stored verbatim — the cap is an upper bound,
        // not a transformation.
        let epic2 = seed_epic(&state, &project_id, "InProgress").await;
        let task2 = seed_task(&state, &epic2, &project_id, "B").await;
        set_task_status(&state, &task2, "InProgress").await;
        fail_item(
            &state,
            FailureContext {
                epic_id: Some(&epic2),
                task_id: Some(&task2),
                reason: FailureReason::AgentError,
                message: "short failure",
                push: PushIntent::Skip,
            },
        )
        .await;
        assert_eq!(
            fetch_task_detail(&state, &task2).await.as_deref(),
            Some("short failure")
        );
    }

    /// Rec 5's surfacing + recovery contract end-to-end over the HTTP API on
    /// a **standalone** task (which exercises `fail_item`'s no-epic branch —
    /// the project must be resolved off the task row itself): `GET /tasks/{id}`
    /// JSON includes `failure_detail` right next to `failure_reason` (the AC:
    /// it surfaces wherever the reason does), and `POST /tasks/{id}/retry`
    /// clears it so the fresh attempt doesn't inherit stale detail.
    #[tokio::test]
    async fn retry_clears_stale_failure_detail_and_api_surfaces_it() {
        let (state, app) = test_app().await;
        let project_id = seed_project(&state).await;
        let task_id = seed_standalone_task(&state, &project_id, "Standalone A", "Todo").await;

        fail_item(
            &state,
            FailureContext {
                epic_id: None,
                task_id: Some(&task_id),
                reason: FailureReason::ProviderRateLimited,
                message: "Error: API returned 429 Too Many Requests",
                push: PushIntent::Skip,
            },
        )
        .await;

        // API JSON: failure_detail rides next to failure_reason.
        let response = app
            .clone()
            .oneshot(req("GET", &format!("/tasks/{task_id}"), None))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = body_json(response).await;
        assert_eq!(body["failure_reason"], "provider_rate_limited");
        assert_eq!(
            body["failure_detail"],
            "Error: API returned 429 Too Many Requests"
        );

        // Retry: 200, and the stale detail is gone from both the response
        // and the row.
        let response = app
            .clone()
            .oneshot(req("POST", &format!("/tasks/{task_id}/retry"), None))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let retried = body_json(response).await;
        assert_eq!(retried["status"], "InProgress");
        assert_eq!(retried["failure_reason"], serde_json::Value::Null);
        assert_eq!(retried["failure_detail"], serde_json::Value::Null);
        assert_eq!(fetch_task_detail(&state, &task_id).await, None);
    }

    /// The AC's headline push behavior: the epic branch is pushed on failure
    /// with the committed work only — a later task's dirty, uncommitted tree
    /// never reaches the pushed branch. Task A completes normally (one
    /// commit, `test_cmd` green); task B's implement stage writes a file that
    /// never satisfies `test_cmd`, exhausting the fix loop with a dirty tree
    /// still sitting, uncommitted, in the workspace. The pushed branch (read
    /// back from the fixture, which doubles as `origin` in this module's
    /// tests) must carry A's commit and nothing from B.
    #[tokio::test]
    async fn failure_pushes_committed_work_but_never_a_later_tasks_dirty_tree() {
        let agent = Arc::new(
            ScriptedTaskAgent::new()
                .script(Stage::Implement, writes_file("a.txt", "a\n"))
                .script(Stage::Implement, writes_file("broken.txt", "oops\n")),
        );
        let (state, _app) = test_app_with_task_agent(agent).await;
        let fixture = GitFixture::new().await;
        let project_id = seed_project_with_test_cmd(&state, &fixture, "! test -f broken.txt").await;
        let epic_id = seed_epic(&state, &project_id, "InProgress").await;
        let a = seed_task(&state, &epic_id, &project_id, "A").await;
        let b = seed_task(&state, &epic_id, &project_id, "B").await;
        link(&state, &a, &b).await; // A must run (and commit) before B fails.

        run_epic_pipeline(state.clone(), epic_id.clone()).await;

        let task_a = fetch_task_row(&state, &a).await;
        assert_eq!(task_a.0, "Done");
        let task_b = fetch_task_row(&state, &b).await;
        assert_eq!(task_b.0, "Failed");
        assert_eq!(task_b.1.as_deref(), Some("test_gate_exhausted"));

        let epic = fetch_epic(state.db.conn(), &epic_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(epic.status, "Blocked");
        assert_eq!(epic.blocked_reason.as_deref(), Some("test_gate_exhausted"));

        // The branch was pushed: origin (the fixture) has exactly A's commit
        // on the epic branch — nothing from B's failed attempt.
        let branch = epic_branch_name_column(&state, &epic_id).await;
        let subjects = git_log_subjects_for_ref(&fixture.dir, &branch).await;
        assert_eq!(
            subjects,
            vec![
                "init".to_string(),
                format!("impl({}): A", spec::short_id(&a))
            ],
            "the pushed branch must carry A's commit and nothing from B's failed attempt"
        );

        // B's dirty file must never have reached the pushed branch.
        let show = tokio::process::Command::new("git")
            .args(["show", &format!("{branch}:broken.txt")])
            .current_dir(&fixture.dir)
            .output()
            .await
            .unwrap();
        assert!(
            !show.status.success(),
            "broken.txt (B's uncommitted dirty file) must never appear on the pushed branch"
        );

        // The push itself lands in evidence, ok.
        let mut rows = state
            .db
            .conn()
            .query(
                "SELECT status FROM agent_run WHERE epic_id = ?1 AND stage = 'push'",
                params![epic_id.clone()],
            )
            .await
            .unwrap();
        let row = rows.next().await.unwrap().expect("a push agent_run row");
        assert_eq!(row.get::<String>(0).unwrap(), "ok");

        // The workspace itself still has B's dirty tree, retained on disk —
        // pushing never touched the working tree or the index.
        let workspace_path = workspace::epic_workspace_path(&state.config.clone_root, &epic_id);
        let status = git::status_porcelain(&workspace_path).await.unwrap();
        assert!(
            !status.trim().is_empty(),
            "B's dirty tree must still be sitting, uncommitted, in the retained workspace"
        );

        cleanup_clone_root(&state, &project_id, &[&epic_id]);
    }

    /// A push failure during a failure is non-fatal: the epic still reaches
    /// `Blocked(<original reason>)`, never `pr_failed`, and the push failure
    /// itself lands in a `Stage::Push` evidence row rather than being
    /// silently dropped.
    #[tokio::test]
    async fn push_failure_during_a_failure_is_non_fatal_and_recorded_in_evidence() {
        let fake = Arc::new(FakeHost::new().fail_push("simulated push failure"));
        let agent = Arc::new(
            ScriptedTaskAgent::new().script(Stage::Implement, writes_file("broken.txt", "oops\n")),
        );
        let (state, _app) = test_app_with_task_agent_and_host(agent, fake).await;
        let fixture = GitFixture::new().await;
        let project_id = seed_project_with_test_cmd(&state, &fixture, "! test -f broken.txt").await;
        let epic_id = seed_epic(&state, &project_id, "InProgress").await;
        let a = seed_task(&state, &epic_id, &project_id, "A").await;

        run_epic_pipeline(state.clone(), epic_id.clone()).await;

        let task = fetch_task_row(&state, &a).await;
        assert_eq!(task.0, "Failed");
        assert_eq!(task.1.as_deref(), Some("test_gate_exhausted"));

        let epic = fetch_epic(state.db.conn(), &epic_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(epic.status, "Blocked");
        assert_eq!(
            epic.blocked_reason.as_deref(),
            Some("test_gate_exhausted"),
            "a push failure during triage must never overwrite the original reason with pr_failed"
        );

        let mut rows = state
            .db
            .conn()
            .query(
                "SELECT status, log FROM agent_run WHERE epic_id = ?1 AND stage = 'push'",
                params![epic_id.clone()],
            )
            .await
            .unwrap();
        let row = rows.next().await.unwrap().expect("a push agent_run row");
        assert_eq!(row.get::<String>(0).unwrap(), "error");
        let log: String = row.get(1).unwrap();
        assert!(log.contains("simulated push failure"));

        let (lease_owner, lease_expires_at) = epic_lease(&state, &epic_id).await;
        assert!(
            lease_owner.is_none(),
            "the lease must still be released even though the triage push itself failed"
        );
        assert!(lease_expires_at.is_none());

        cleanup_clone_root(&state, &project_id, &[&epic_id]);
    }

    /// Assert that `epic_id` opened no `Stage::Push` `agent_run` row at
    /// all — used by the two provisioning-failure push-skip tests below.
    /// Split into a plain (non-`#[tokio::test]`) helper called from two
    /// separate test functions, each with its own `run_epic_pipeline` call
    /// — rather than one test running the pipeline twice — because this
    /// module's large per-task async frames (see e.g. `run_preflight`'s own
    /// doc on why its nested call is `Box::pin`ned) can overflow the test
    /// harness's thread stack if two full pipeline runs share one `async
    /// fn`'s generated state machine.
    async fn assert_no_push_row(state: &AppState, epic_id: &str) {
        let mut rows = state
            .db
            .conn()
            .query(
                "SELECT COUNT(*) FROM agent_run WHERE epic_id = ?1 AND stage = 'push'",
                params![epic_id.to_string()],
            )
            .await
            .unwrap();
        let count: i64 = rows.next().await.unwrap().unwrap().get(0).unwrap();
        assert_eq!(
            count, 0,
            "no ProvisionedWorkspace ever existed — there must be no push row at all"
        );
    }

    /// `workspace_error` (an unreachable repo) predates any
    /// `ProvisionedWorkspace` at all — the push is skipped cleanly, not
    /// merely non-fatal. Driven through `spawn_pool` + the lane endpoint
    /// (matching `workspace_error_blocks_epic_releases_lease_and_publishes`'s
    /// own convention above) rather than an unspawned direct
    /// `run_epic_pipeline(...).await` — the pipeline body's per-task async
    /// frames are large enough (see `run_preflight`'s own doc on why its
    /// nested call is `Box::pin`ned) that running one outside its own
    /// `tokio::spawn`'d task, on the test harness's own thread, can overflow
    /// the stack.
    #[tokio::test]
    async fn workspace_error_skips_the_push_cleanly() {
        let (state, app) = test_app().await;
        let project_id = seed_project_bad_repo(&state).await;
        let epic_id = seed_epic(&state, &project_id, "Ready").await;
        seed_task(&state, &epic_id, &project_id, "A").await;

        let _handles = spawn_pool(state.clone());
        let response = app
            .oneshot(req(
                "POST",
                &format!("/epics/{epic_id}/lane"),
                Some(json!({ "status": "InProgress" })),
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        loop {
            if epic_status(&state, &epic_id).await == "Blocked" {
                break;
            }
            if tokio::time::Instant::now() >= deadline {
                panic!("epic never reached Blocked");
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        let epic = fetch_epic(state.db.conn(), &epic_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(epic.blocked_reason.as_deref(), Some("workspace_error"));
        assert_no_push_row(&state, &epic_id).await;
    }

    /// `setup_failed` (a failing `setup_cmd`) likewise predates any
    /// `ProvisionedWorkspace` at all — the push is skipped cleanly. Same
    /// `spawn_pool` + lane-endpoint convention as the test above, for the
    /// identical stack-size reason.
    #[tokio::test]
    async fn setup_failed_skips_the_push_cleanly() {
        let (state, app) = test_app().await;
        let fixture = GitFixture::new().await;
        let project_id =
            seed_project_with_setup_cmd(&state, &fixture, "echo setup-boom && exit 5").await;
        let epic_id = seed_epic(&state, &project_id, "Ready").await;
        seed_task(&state, &epic_id, &project_id, "A").await;

        let _handles = spawn_pool(state.clone());
        let response = app
            .oneshot(req(
                "POST",
                &format!("/epics/{epic_id}/lane"),
                Some(json!({ "status": "InProgress" })),
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        loop {
            if epic_status(&state, &epic_id).await == "Blocked" {
                break;
            }
            if tokio::time::Instant::now() >= deadline {
                panic!("epic never reached Blocked");
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        let epic = fetch_epic(state.db.conn(), &epic_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(epic.blocked_reason.as_deref(), Some("setup_failed"));
        assert_no_push_row(&state, &epic_id).await;

        cleanup_clone_root(&state, &project_id, &[&epic_id]);
    }

    /// The AC's "the worker immediately claims a different epic (a failure is
    /// epic-scoped, not fatal)": with `worker_concurrency = 1`
    /// (`Config::for_test`'s default), a single worker loop fails the first
    /// epic and — with no manual re-trigger and no reliance on the slow poll
    /// fallback (`state.notify.notify_waiters()` is called exactly once, up
    /// front) — goes straight on to claim and complete the second.
    #[tokio::test]
    async fn worker_moves_on_to_a_different_epic_immediately_after_a_failure() {
        assert_eq!(
            Config::for_test().executor.worker_concurrency,
            1,
            "this test's proof depends on exactly one worker loop existing"
        );

        let (state, _app) = test_app().await;
        let fixture1 = GitFixture::new().await;
        let project1 = seed_project_with_test_cmd(&state, &fixture1, "exit 1").await;
        let epic1 = seed_epic(&state, &project1, "InProgress").await;
        seed_task(&state, &epic1, &project1, "A").await;

        let fixture2 = GitFixture::new().await;
        let project2 = seed_project_with_workspace(&state, &fixture2).await;
        let epic2 = seed_epic(&state, &project2, "InProgress").await;
        seed_task(&state, &epic2, &project2, "B").await;

        let _handles = spawn_pool(state.clone());
        state.notify.notify_waiters();

        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        loop {
            let (s1, s2) = (
                epic_status(&state, &epic1).await,
                epic_status(&state, &epic2).await,
            );
            if s1 == "Blocked" && s2 == "InReview" {
                break;
            }
            if tokio::time::Instant::now() >= deadline {
                panic!(
                    "epics never reached their expected terminal states: epic1={s1:?} epic2={s2:?}"
                );
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }

        let epic1_row = fetch_epic(state.db.conn(), &epic1).await.unwrap().unwrap();
        assert_eq!(epic1_row.blocked_reason.as_deref(), Some("preflight_red"));

        cleanup_clone_root(&state, &project1, &[&epic1]);
        cleanup_clone_root(&state, &project2, &[&epic2]);
    }

    /// The AC's "a second epic in the same project is unaffected": both
    /// epics share a project (the same canonical checkout, the same
    /// per-project refresh lock); the first epic's only task fails via a
    /// scripted implement error, and — the fix this task makes, not just an
    /// existing behavior — reaches `Failed(agent_error)` itself (not left
    /// `InProgress`, as every `block_epic_on_agent_error` call site used to
    /// leave it pre-T-540); the second epic's task still completes normally.
    #[tokio::test]
    async fn second_epic_in_the_same_project_is_unaffected_by_a_failure() {
        let agent = Arc::new(ScriptedTaskAgent::new().script(
            Stage::Implement,
            ScriptedRun {
                exit_code: Some(1),
                ..ScriptedRun::default()
            },
        ));
        let (state, _app) = test_app_with_task_agent(agent).await;
        let fixture = GitFixture::new().await;
        let project_id = seed_project_with_workspace(&state, &fixture).await;

        let epic_a = seed_epic(&state, &project_id, "InProgress").await;
        let task_a_id = seed_task(&state, &epic_a, &project_id, "A").await;
        let epic_b = seed_epic(&state, &project_id, "InProgress").await;
        seed_task(&state, &epic_b, &project_id, "B").await;

        let _handles = spawn_pool(state.clone());
        state.notify.notify_waiters();

        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        loop {
            let (sa, sb) = (
                epic_status(&state, &epic_a).await,
                epic_status(&state, &epic_b).await,
            );
            if sa == "Blocked" && sb == "InReview" {
                break;
            }
            if tokio::time::Instant::now() >= deadline {
                panic!("epics never reached their expected terminal states: epic_a={sa:?} epic_b={sb:?}");
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }

        let epic_a_row = fetch_epic(state.db.conn(), &epic_a).await.unwrap().unwrap();
        assert_eq!(epic_a_row.blocked_reason.as_deref(), Some("agent_error"));
        let task_a = fetch_task_row(&state, &task_a_id).await;
        assert_eq!(
            task_a.0, "Failed",
            "the T-540 fix: a failing implement stage now fails the task too"
        );
        assert_eq!(task_a.1.as_deref(), Some("agent_error"));

        let statuses_b = task_statuses(&state, &epic_b).await;
        assert_eq!(
            statuses_b["B"], "Done",
            "epic B must be unaffected by epic A's failure"
        );

        cleanup_clone_root(&state, &project_id, &[&epic_a, &epic_b]);
    }

    // ---- Bounded implement-stage retry on transient provider errors ----
    //
    // The incident this hardens against: an implement run whose harness
    // surfaced a mid-run provider 429 came back not-`ok`, was routed straight
    // to `route_stage_failure`, and a fully-completed-but-uncommitted fix was
    // discarded. These tests pin the contract of
    // `DEARBORN_IMPLEMENT_TRANSIENT_RETRIES`: only a matching transient error
    // earns one extra attempt (per the configured bound), each attempt opens
    // its own `agent_run` row under an incremented `attempt` number, a
    // recovered retry completes the whole pipeline normally, exhausted
    // retries land in exactly Rec 5's `Failed(provider_rate_limited)` route,
    // and any non-transient failure keeps the old single-attempt behavior.

    /// `(attempt, status)` per `implement` agent_run row for `task_id`,
    /// oldest first — the implement-retry counterpart to [`review_rows`],
    /// trimmed to the two columns these tests actually assert on.
    async fn implement_rows(state: &AppState, task_id: &str) -> Vec<(i64, String)> {
        let mut rows = state
            .db
            .conn()
            .query(
                "SELECT attempt, status FROM agent_run \
                 WHERE task_id = ?1 AND stage = 'implement' ORDER BY created_at ASC, rowid ASC",
                params![task_id],
            )
            .await
            .unwrap();
        let mut out = Vec::new();
        while let Some(row) = rows.next().await.unwrap() {
            out.push((row.get(0).unwrap(), row.get(1).unwrap()));
        }
        out
    }

    /// A scripted implement run that fails like the incident's provider 429:
    /// a non-zero exit whose output carries the transient signal (no files,
    /// so a *retry* starting from a clean dirty-tree state is realistic).
    fn transient_rate_limit_run() -> ScriptedRun {
        ScriptedRun {
            exit_code: Some(1),
            text: vec!["Error: provider returned HTTP 429 rate limited\n".to_string()],
            ..ScriptedRun::default()
        }
    }

    /// A transient-looking implement error that recovers on the retry:
    /// attempt 1 fails on a 429, attempt 2 writes a real file and exits clean,
    /// and the rest of the pipeline (commit, default-scripted PASS review)
    /// runs untouched to `Done`. Both attempts must be recorded under their
    /// own incremented `attempt` number.
    #[tokio::test]
    async fn transient_implement_failure_is_retried_and_retry_completes_the_pipeline() {
        let agent = Arc::new(
            ScriptedTaskAgent::new()
                .script(Stage::Implement, transient_rate_limit_run())
                .script(Stage::Implement, writes_file("a.txt", "a")),
        );
        let (state, _app) = test_app_with_task_agent(agent).await;
        let fixture = GitFixture::new().await;
        let project_id = seed_project_with_workspace(&state, &fixture).await;
        let epic_id = seed_epic(&state, &project_id, "InProgress").await;
        let task_id = seed_task(&state, &epic_id, &project_id, "A").await;

        run_epic_pipeline(state.clone(), epic_id.clone()).await;

        // The retry succeeded, so nothing about this task looks failed.
        assert_eq!(fetch_task_row(&state, &task_id).await.0, "Done");
        assert_eq!(fetch_task_row(&state, &task_id).await.1, None);
        assert_eq!(epic_status(&state, &epic_id).await, "InReview");

        // Exactly the two expected attempts, each under its own incremented
        // number: attempt 1 closed `error` (the 429), attempt 2 closed `ok`.
        assert_eq!(
            implement_rows(&state, &task_id).await,
            vec![(1, "error".to_string()), (2, "ok".to_string())],
            "the 429 attempt and the recovering retry must both leave evidence rows"
        );

        cleanup_clone_root(&state, &project_id, &[&epic_id]);
    }

    /// Transient errors that keep happening exhaust the bound (`for_test`
    /// keeps the production default of 1 extra retry, so 2 attempts total)
    /// and fall through to the ordinary `route_stage_failure` handling —
    /// but Rec 5's finer taxonomy classifies them `provider_rate_limited`
    /// (not the generic `agent_error`), with the provider's own error text
    /// persisted (redacted, capped) as `failure_detail` on both the task and
    /// the epic. Consequences are otherwise identical: task `Failed`, epic
    /// `Blocked`, same push/retention behavior.
    #[tokio::test]
    async fn transient_implement_errors_exhausting_retries_fail_as_provider_rate_limited() {
        let agent = Arc::new(
            ScriptedTaskAgent::new()
                .script(Stage::Implement, transient_rate_limit_run())
                .script(Stage::Implement, transient_rate_limit_run()),
        );
        let (state, _app) = test_app_with_task_agent(agent).await;
        let fixture = GitFixture::new().await;
        let project_id = seed_project_with_workspace(&state, &fixture).await;
        let epic_id = seed_epic(&state, &project_id, "InProgress").await;
        let task_id = seed_task(&state, &epic_id, &project_id, "A").await;

        run_epic_pipeline(state.clone(), epic_id.clone()).await;

        assert_eq!(fetch_task_row(&state, &task_id).await.0, "Failed");
        assert_eq!(
            fetch_task_row(&state, &task_id).await.1.as_deref(),
            Some("provider_rate_limited")
        );
        assert_eq!(epic_status(&state, &epic_id).await, "Blocked");

        // Rec 5: the provider's error text rides along on both containers —
        // this is what makes the failure triageable without DB spelunking.
        assert!(
            fetch_task_detail(&state, &task_id)
                .await
                .unwrap()
                .contains("429"),
            "task.failure_detail must carry the provider's rate-limit error"
        );
        let epic = fetch_epic(state.db.conn(), &epic_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            epic.blocked_reason.as_deref(),
            Some("provider_rate_limited")
        );
        assert!(
            epic.failure_detail
                .as_deref()
                .unwrap_or_default()
                .contains("429"),
            "epic.failure_detail must carry the same redacted detail"
        );

        // Both bounded attempts ran — never a third — each under its own
        // incremented attempt number.
        assert_eq!(
            implement_rows(&state, &task_id).await,
            vec![(1, "error".to_string()), (2, "error".to_string())],
            "exactly 1 + DEARBORN_IMPLEMENT_TRANSIENT_RETRIES attempts must run"
        );

        cleanup_clone_root(&state, &project_id, &[&epic_id]);
    }

    /// A non-transient failure (plain non-zero exit, no provider signal in
    /// the output) must fail immediately on a single attempt — the retry is
    /// earned only by a matching transient error, never by ordinary breakage.
    #[tokio::test]
    async fn non_transient_implement_failure_fails_immediately_on_a_single_attempt() {
        let agent = Arc::new(ScriptedTaskAgent::new().script(
            Stage::Implement,
            ScriptedRun {
                exit_code: Some(1),
                text: vec!["error: could not compile the task's crate\n".to_string()],
                ..ScriptedRun::default()
            },
        ));
        let (state, _app) = test_app_with_task_agent(agent).await;
        let fixture = GitFixture::new().await;
        let project_id = seed_project_with_workspace(&state, &fixture).await;
        let epic_id = seed_epic(&state, &project_id, "InProgress").await;
        let task_id = seed_task(&state, &epic_id, &project_id, "A").await;

        run_epic_pipeline(state.clone(), epic_id.clone()).await;

        assert_eq!(fetch_task_row(&state, &task_id).await.0, "Failed");
        assert_eq!(
            fetch_task_row(&state, &task_id).await.1.as_deref(),
            Some("agent_error")
        );
        assert_eq!(epic_status(&state, &epic_id).await, "Blocked");

        // One attempt, no retry: the compile-error text matches none of the
        // transient signals.
        assert_eq!(
            implement_rows(&state, &task_id).await,
            vec![(1, "error".to_string())]
        );

        cleanup_clone_root(&state, &project_id, &[&epic_id]);
    }

    // ---- Recommendation 3: salvaging completed-but-uncommitted implement work
    //
    // The incident this hardens against: pi recovered from a mid-run 429 and
    // finished the whole fix, but a residual `RunEvent::Error` made the
    // outcome not-`ok`; the walk routed straight to `route_stage_failure`,
    // `commit_if_dirty` (sitting after the ok-check) never ran, and the
    // next claim's `git reset --hard HEAD && git clean -fd` destroyed the
    // finished diff while the triage push pushed nothing. These tests pin
    // the two halves of the salvage contract: an ordinary implement failure
    // commits whatever the agent left behind onto the task branch *before*
    // failing (`Failed(agent_error)`, triage push carries it), while a
    // cancelled outcome commits nothing and keeps its resumable dirty tree.

    /// A non-transient implement failure (plain non-zero exit) whose agent
    /// still wrote real work: the salvage commit lands on the task branch
    /// with the ordinary §2.8 subject, then the walk fails exactly as before
    /// — `Failed(agent_error)` / epic `Blocked(agent_error)` — and
    /// `fail_item`'s best-effort triage push now actually carries the
    /// salvaged commit to origin (read back from the fixture, which doubles
    /// as `origin` in these tests). The workspace is retained and clean: the
    /// work is safe in history instead of sitting dirty in front of a reset.
    #[tokio::test]
    async fn failed_implement_salvages_completed_work_as_a_commit_and_fails_as_agent_error() {
        let agent = Arc::new(ScriptedTaskAgent::new().script(
            Stage::Implement,
            ScriptedRun {
                exit_code: Some(1),
                text: vec!["error: could not compile the task's crate\n".to_string()],
                files: vec![(PathBuf::from("work.txt"), "work\n".to_string())],
                ..ScriptedRun::default()
            },
        ));
        let (state, _app) = test_app_with_task_agent(agent).await;
        let fixture = GitFixture::new().await;
        let project_id = seed_project_with_workspace(&state, &fixture).await;
        let epic_id = seed_epic(&state, &project_id, "InProgress").await;
        let task_id = seed_task(&state, &epic_id, &project_id, "A").await;

        run_epic_pipeline(state.clone(), epic_id.clone()).await;

        // The task still fails as an ordinary agent-stage failure — salvage
        // changes what survives, not how the failure reads.
        assert_eq!(fetch_task_row(&state, &task_id).await.0, "Failed");
        assert_eq!(
            fetch_task_row(&state, &task_id).await.1.as_deref(),
            Some("agent_error")
        );
        assert_eq!(epic_status(&state, &epic_id).await, "Blocked");

        // The salvaged commit exists on the task branch, with step 5's own
        // §2.8 subject and the failed attempt's file inside it.
        let workspace_path = workspace::epic_workspace_path(&state.config.clone_root, &epic_id);
        assert!(
            workspace_path.exists(),
            "workspace must be retained on failure"
        );
        let branch = epic_branch_name_column(&state, &epic_id).await;
        let expected = format!("impl({}): A", spec::short_id(&task_id));
        assert_eq!(
            git_log_subjects_for_ref(&workspace_path, &branch).await,
            vec!["init".to_string(), expected.clone()],
            "the salvage commit must land on the task branch before the failure route"
        );
        assert_eq!(
            git_show_file(&workspace_path, &branch, "work.txt").await,
            "work",
            "the salvaged commit must contain what the failed agent completed"
        );
        let status = git::status_porcelain(&workspace_path).await.unwrap();
        assert!(
            status.trim().is_empty(),
            "after the salvage commit nothing may be left unstaged in the workspace"
        );

        // The triage push carried the salvaged commit to origin.
        assert_eq!(
            git_log_subjects_for_ref(&fixture.dir, &branch).await,
            vec!["init".to_string(), expected],
            "fail_item's push must carry the salvaged commit to the remote"
        );
        let mut rows = state
            .db
            .conn()
            .query(
                "SELECT status FROM agent_run WHERE epic_id = ?1 AND stage = 'push'",
                params![epic_id.clone()],
            )
            .await
            .unwrap();
        let row = rows.next().await.unwrap().expect("a push agent_run row");
        assert_eq!(
            row.get::<String>(0).unwrap(),
            "ok",
            "the triage push must succeed"
        );

        cleanup_clone_root(&state, &project_id, &[&epic_id]);
    }

    /// The cancel half of the salvage contract: an implement run killed by a
    /// real mid-flight cancellation (T-542's `POST /epics/{id}/lane` path,
    /// `Exited { cancelled: true }`) must NOT be salvaged into a commit — a
    /// cancelled task resets to `Todo` and must stay resumable from exactly
    /// where it was, so its dirty tree survives uncommitted in the retained
    /// workspace and HEAD never moves off `base_sha`. The kill/gate plumbing
    /// mirrors
    /// `cancel_during_implement_kills_it_in_flight_resets_task_retains_workspace_no_pr`
    /// (every wait below is a bounded condition poll, no sleeps); what is new
    /// here is the scripted stage writing a file first, so there is real
    /// dirty-tree content a naive salvage would have committed.
    #[tokio::test]
    async fn cancelled_implement_commits_nothing_and_keeps_its_resumable_dirty_tree() {
        let gate = Arc::new(Gate::default());
        let agent: Arc<dyn TaskAgent> =
            Arc::new(ScriptedTaskAgent::new().with_gate(gate.clone()).script(
                Stage::Implement,
                ScriptedRun {
                    text: vec!["partial output before the kill".to_string()],
                    files: vec![(PathBuf::from("work.txt"), "work\n".to_string())],
                    ..ScriptedRun::default()
                },
            ));
        let (state, app) = test_app_with_task_agent(agent).await;
        let fixture = GitFixture::new().await;
        let project_id = seed_project_with_workspace(&state, &fixture).await;
        let epic_id = seed_epic(&state, &project_id, "InProgress").await;
        let task_id = seed_task(&state, &epic_id, &project_id, "A").await;

        let _handles = spawn_pool(state.clone());
        state.notify.notify_waiters();

        // Bounded readiness poll: wait until the cancel registry holds the
        // implement stage's handle — the run is gated in flight.
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        loop {
            if state.cancel_registry.lock().unwrap().contains_key(&epic_id) {
                break;
            }
            if tokio::time::Instant::now() >= deadline {
                panic!("the cancel registry never gained an entry for the gated implement stage");
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }

        // Issue the real cancel over HTTP, then let the scripted stage exit
        // carrying `cancelled: true` (its file write already happened before
        // the gate — the tree is dirty at kill time).
        let response = app
            .clone()
            .oneshot(req(
                "POST",
                &format!("/epics/{epic_id}/lane"),
                Some(json!({ "status": "Cancelled" })),
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        gate.release();

        // Bounded readiness poll: the worker observed the cancelled outcome
        // and reset the task to Todo.
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        loop {
            if fetch_task_row(&state, &task_id).await.0 == "Todo" {
                break;
            }
            if tokio::time::Instant::now() >= deadline {
                panic!(
                    "task never reset to Todo after the cancel: {:?}",
                    fetch_task_row(&state, &task_id).await
                );
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }

        // Resumable, not failed.
        let (status, failure_reason) = fetch_task_row(&state, &task_id).await;
        assert_eq!(status, "Todo");
        assert_eq!(
            failure_reason, None,
            "a cancelled task must carry no failure_reason"
        );
        assert_eq!(epic_status(&state, &epic_id).await, "Cancelled");

        // NO commit: HEAD is still exactly base_sha.
        let workspace_path = workspace::epic_workspace_path(&state.config.clone_root, &epic_id);
        assert!(
            workspace_path.exists(),
            "workspace must be retained on a cancel"
        );
        let base_sha = task_base_sha(&state, &task_id)
            .await
            .expect("base_sha must have been recorded before the implement stage ran");
        assert_eq!(
            git_rev_parse(&workspace_path, "HEAD").await,
            base_sha,
            "a cancelled implement must never be salvaged into a commit"
        );

        // The dirty tree is preserved exactly as the agent left it — resumable.
        let status = git::status_porcelain(&workspace_path).await.unwrap();
        assert!(
            !status.trim().is_empty(),
            "the dirty tree must survive uncommitted for resume"
        );
        assert_eq!(
            std::fs::read_to_string(workspace_path.join("work.txt")).unwrap(),
            "work\n",
            "the killed agent's completed work must still be in the working tree"
        );

        cleanup_clone_root(&state, &project_id, &[&epic_id]);
    }

    /// documented transient signals, and no false positives on ordinary
    /// failure text (including empty output).
    #[test]
    fn transient_provider_predicate_matches_only_upstream_signals() {
        assert!(is_transient_provider_error(
            "Error: API returned 429 Too Many Requests"
        ));
        assert!(is_transient_provider_error(
            "provider is temporarily rate-limited, back off"
        ));
        assert!(is_transient_provider_error("model overloaded, try again"));
        assert!(is_transient_provider_error("502 Bad Gateway from upstream"));
        assert!(is_transient_provider_error("SERVICE UNAVAILABLE"));
        assert!(!is_transient_provider_error("error: could not compile"));
        assert!(!is_transient_provider_error(""));
    }

    // ---- T-541: POST /tasks/{id}/retry — the full worker-side recovery loop ----
    //
    // `tasks.rs`'s own `mod tests` covers the HTTP-level contract (404/409/200,
    // WS frames, notify) against directly-seeded rows. What only this module
    // can prove is the AC that actually matters end to end: a real failure
    // driven through the walk, `retry`, and a **second** real walk that a
    // worker re-claims, re-attaches (dropping the failed attempt's dirty
    // tree per T-511), and runs to `InReview` — with an edited spec (T-541's
    // `PATCH`-then-retry AC) reaching the re-run's own prompt. Per this
    // module's own T-522 note (`test_app`'s doc + the module-level guidance
    // that shipped with T-522), a two-walk test like this drives the pool via
    // `spawn_pool` + HTTP, never `run_epic_pipeline(...)` called directly —
    // this module's async frames are large enough that a second inline await
    // risks the stack overflow flagged after T-522 landed.

    /// `git ls-tree -r --name-only <ref>` in `dir` — the set of file paths
    /// actually committed at `ref`, used (once the workspace itself is
    /// deleted by a successful finalize, same reasoning as
    /// `git_log_subjects_for_ref`) to prove a file was **never** committed,
    /// not just that it isn't in some particular commit's diff.
    async fn git_ls_tree(dir: &std::path::Path, git_ref: &str) -> Vec<String> {
        let output = tokio::process::Command::new("git")
            .args(["ls-tree", "-r", "--name-only", git_ref])
            .current_dir(dir)
            .output()
            .await
            .unwrap();
        assert!(
            output.status.success(),
            "git ls-tree {git_ref} failed: {output:?}"
        );
        String::from_utf8_lossy(&output.stdout)
            .lines()
            .map(|s| s.to_string())
            .collect()
    }

    /// The full recovery loop (MILESTONE_2 §7 T-541's AC, essentially
    /// verbatim): a task fails via test-gate exhaustion (mirrors
    /// `exhausting_attempts_fails_the_task_blocks_the_epic_and_commits_nothing`
    /// above), the spec is edited via `PATCH /tasks/{id}`, `POST
    /// /tasks/{id}/retry` moves the task back to `Todo` and the epic back to
    /// `InProgress`, a worker re-claims and re-attaches (T-511's `reset
    /// --hard` + `clean -fd` drop the first attempt's dirty file), and the
    /// re-run — scripted to behave differently the second time, as a real
    /// agent acting on the edited spec would — completes the epic. Asserts,
    /// in order: the first failure lands exactly as T-540 promises; the PATCH
    /// + retry round-trip; the epic returns to the In Progress lane
    /// immediately (before the worker even wakes) and the board reflects it;
    /// the second walk reaches `InReview`; the failed attempt's file was
    /// never committed while the second attempt's file was; and the retried
    /// implement run's prompt carried the edited spec, not the original.
    #[tokio::test]
    async fn retry_recovers_a_failed_task_end_to_end_with_edited_spec_and_dropped_dirty_tree() {
        let agent = Arc::new(
            ScriptedTaskAgent::new()
                // First attempt: writes the file that keeps the gate red
                // forever (the default Fix stage is a no-op) until re-attach
                // drops it.
                .script(Stage::Implement, writes_file("broken.txt", "dirty\n"))
                // Second attempt (after retry): doesn't recreate it, so the
                // gate is green on the first try.
                .script(Stage::Implement, writes_file("good.txt", "clean\n")),
        );
        let recorded = agent.recorded();
        let (state, app) = test_app_with_task_agent(agent).await;
        let fixture = GitFixture::new().await;
        // Green on the untouched tree (preflight passes); red for as long as
        // broken.txt exists — identical fixture to the plain exhaustion test.
        let project_id = seed_project_with_test_cmd(&state, &fixture, "! test -f broken.txt").await;
        let epic_id = seed_epic(&state, &project_id, "InProgress").await;
        let task_id = seed_task(&state, &epic_id, &project_id, "A").await;
        set_task_spec(
            &state,
            &task_id,
            "ORIGINAL_SPEC_MARKER",
            "original acceptance",
        )
        .await;

        let _handles = spawn_pool(state.clone());
        state.notify.notify_waiters();

        // ---- first walk: fails and blocks, exactly like T-540's own test ----
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        loop {
            if epic_status(&state, &epic_id).await == "Blocked" {
                break;
            }
            if tokio::time::Instant::now() >= deadline {
                panic!(
                    "epic never blocked: status={}, tasks={:?}",
                    epic_status(&state, &epic_id).await,
                    task_statuses(&state, &epic_id).await
                );
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        let failed = fetch_task_row(&state, &task_id).await;
        assert_eq!(failed.0, "Failed");
        assert_eq!(failed.1.as_deref(), Some("test_gate_exhausted"));
        let epic = fetch_epic(state.db.conn(), &epic_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(epic.blocked_reason.as_deref(), Some("test_gate_exhausted"));

        // ---- edit the spec before retrying (T-541's PATCH-then-retry AC) ----
        let patch_response = app
            .clone()
            .oneshot(req(
                "PATCH",
                &format!("/tasks/{task_id}"),
                Some(json!({
                    "description": "EDITED_SPEC_MARKER",
                    "acceptance": "edited acceptance"
                })),
            ))
            .await
            .unwrap();
        assert_eq!(patch_response.status(), StatusCode::OK);

        // ---- retry: 200, Todo, and the epic is back In Progress immediately ----
        let retry_response = app
            .clone()
            .oneshot(req("POST", &format!("/tasks/{task_id}/retry"), None))
            .await
            .unwrap();
        assert_eq!(retry_response.status(), StatusCode::OK);
        let retried = body_json(retry_response).await;
        assert_eq!(retried["status"], "Todo");
        assert_eq!(retried["failure_reason"], Value::Null);
        assert_eq!(
            epic_status(&state, &epic_id).await,
            "InProgress",
            "the epic must return to the In Progress lane synchronously with the retry response, \
             before the worker even wakes"
        );

        // The board reflects the lane move (T-541's AC), independent of
        // whatever the worker does next.
        let board_response = app
            .clone()
            .oneshot(req("GET", &format!("/projects/{project_id}/board"), None))
            .await
            .unwrap();
        let board = body_json(board_response).await;
        assert_eq!(board["epics"][0]["status"], "InProgress");

        // ---- the pool re-claims, re-attaches, and completes the walk ----
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        loop {
            if epic_status(&state, &epic_id).await == "InReview" {
                break;
            }
            if tokio::time::Instant::now() >= deadline {
                panic!(
                    "epic never completed after retry: status={}, task={:?}",
                    epic_status(&state, &epic_id).await,
                    fetch_task_row(&state, &task_id).await
                );
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        assert_eq!(fetch_task_row(&state, &task_id).await.0, "Done");

        // ---- the re-attach dropped the failed attempt's dirty tree ----
        //
        // The workspace is retained post-finalize (for feedback rounds), so read the
        // final committed tree back from the fixture (the project's origin)
        // on the epic's own branch.
        let branch = epic_branch_name_column(&state, &epic_id).await;
        let files = git_ls_tree(&fixture.dir, &branch).await;
        assert!(
            !files.contains(&"broken.txt".to_string()),
            "the failed attempt's dirty file must never have been committed: {files:?}"
        );
        assert!(
            files.contains(&"good.txt".to_string()),
            "the retried attempt's own file must be committed: {files:?}"
        );

        // ---- the retried run's prompt carried the edited spec ----
        let runs = recorded.lock().unwrap();
        let implement_runs: Vec<_> = runs
            .iter()
            .filter(|r| r.stage == Stage::Implement)
            .collect();
        assert_eq!(
            implement_runs.len(),
            2,
            "implement must run once per attempt"
        );
        assert!(implement_runs[0].prompt.contains("ORIGINAL_SPEC_MARKER"));
        assert!(
            implement_runs[1].prompt.contains("EDITED_SPEC_MARKER"),
            "the retried run must see the spec edited before the retry: {}",
            implement_runs[1].prompt
        );
        assert!(
            !implement_runs[1].prompt.contains("ORIGINAL_SPEC_MARKER"),
            "the retried run must not still see the pre-edit spec"
        );

        cleanup_clone_root(&state, &project_id, &[&epic_id]);
    }

    // ---- T-542: cancellation as a kill =====================================
    //
    // `spawn_pool` + the lane endpoint throughout (never `run_epic_pipeline(
    // ...).await` called directly for the multi-step scenarios) per this
    // module's own T-522/T-541 convention — see those sections' own notes on
    // why a second inline await risks the test thread's stack.

    /// This task's headline AC, end to end through the real
    /// `POST /epics/{id}/lane` cancel path: while `Stage::Implement` is
    /// gated in flight, cancelling the epic kills it — proven by the
    /// registered handle's `was_cancelled()` turning true **while the stage
    /// is still gated** (the proof the kill reached the process promptly,
    /// not merely that the walk eventually noticed the epic was gone at the
    /// next boundary; no sleep is ever used as that proof — every wait below
    /// is a bounded, condition-polling loop). Once the gate is released and
    /// the stage's `Exited { cancelled: true }` propagates back: the
    /// `implement` `agent_run` row closes `status='cancelled'` with its
    /// partial (pre-kill) log; the task returns to `Todo`; the epic stays
    /// exactly `Cancelled` (never `Blocked`); the workspace is retained on
    /// disk; no PR is ever opened; and the registry entry for the epic is
    /// gone.
    #[tokio::test]
    async fn cancel_during_implement_kills_it_in_flight_resets_task_retains_workspace_no_pr() {
        let gate = Arc::new(Gate::default());
        let agent: Arc<dyn TaskAgent> =
            Arc::new(ScriptedTaskAgent::new().with_gate(gate.clone()).script(
                Stage::Implement,
                ScriptedRun {
                    text: vec!["partial output before the kill".to_string()],
                    ..ScriptedRun::default()
                },
            ));
        let fake = Arc::new(FakeHost::new());
        let (state, app) = test_app_with_task_agent_and_host(agent, fake.clone()).await;
        let fixture = GitFixture::new().await;
        let project_id = seed_project_with_workspace(&state, &fixture).await;
        let epic_id = seed_epic(&state, &project_id, "InProgress").await;
        let task_id = seed_task(&state, &epic_id, &project_id, "A").await;

        let _handles = spawn_pool(state.clone());
        state.notify.notify_waiters();

        // Bounded readiness poll: wait until the cancel registry actually
        // holds an entry for this epic — the precise signal that
        // `Stage::Implement`'s handle is registered and the run is gated in
        // flight (stronger than polling the task's own status, which can
        // flip to `InProgress` slightly before `run_agent_stage` gets far
        // enough to register the handle).
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        loop {
            if state.cancel_registry.lock().unwrap().contains_key(&epic_id) {
                break;
            }
            if tokio::time::Instant::now() >= deadline {
                panic!("the cancel registry never gained an entry for the gated implement stage");
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }

        // Issue the real cancel over HTTP.
        let response = app
            .clone()
            .oneshot(req(
                "POST",
                &format!("/epics/{epic_id}/lane"),
                Some(json!({ "status": "Cancelled" })),
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        // The kill reached the live handle immediately — while the stage is
        // STILL gated, before this test ever releases it. This is the
        // "seconds, not the next stage boundary" proof.
        let was_cancelled_while_gated = state
            .cancel_registry
            .lock()
            .unwrap()
            .get(&epic_id)
            .map(|h| h.was_cancelled())
            .unwrap_or(false);
        assert!(
            was_cancelled_while_gated,
            "RunControl::cancel() must reach the registered handle promptly, \
             while the stage is still in flight"
        );

        // Now let the scripted stage actually exit, carrying `cancelled: true`.
        gate.release();

        // Bounded readiness poll: wait for the worker to observe the
        // cancelled outcome and reset the task.
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        loop {
            if fetch_task_row(&state, &task_id).await.0 == "Todo" {
                break;
            }
            if tokio::time::Instant::now() >= deadline {
                panic!(
                    "task never reset to Todo after the cancel: {:?}",
                    fetch_task_row(&state, &task_id).await
                );
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }

        // The task is Todo, not Failed — resumable, no failure_reason.
        let (status, failure_reason) = fetch_task_row(&state, &task_id).await;
        assert_eq!(status, "Todo");
        assert_eq!(failure_reason, None);

        // The epic stayed exactly Cancelled — never flipped to Blocked.
        assert_eq!(epic_status(&state, &epic_id).await, "Cancelled");

        // The `implement` agent_run row closed `cancelled` with its partial
        // log intact.
        let mut rows = state
            .db
            .conn()
            .query(
                "SELECT status, log FROM agent_run WHERE task_id = ?1 AND stage = 'implement'",
                params![task_id.clone()],
            )
            .await
            .unwrap();
        let row = rows
            .next()
            .await
            .unwrap()
            .expect("the implement row must exist");
        assert_eq!(row.get::<String>(0).unwrap(), "cancelled");
        let log: String = row.get(1).unwrap();
        assert!(
            log.contains("partial output before the kill"),
            "the partial (pre-kill) log must be preserved: {log:?}"
        );

        // The workspace is retained on disk.
        let workspace_path = workspace::epic_workspace_path(&state.config.clone_root, &epic_id);
        assert!(
            workspace_path.join(".git").exists(),
            "the workspace must be retained after a cancel"
        );

        // No PR was ever opened.
        assert!(
            fake.open_pr_calls().is_empty(),
            "a cancelled epic must never reach finalize/open_pr"
        );

        // The registry entry is gone — removed on this (cancelled) exit path.
        assert!(
            state.cancel_registry.lock().unwrap().is_empty(),
            "the registry entry must be removed once the cancelled stage's drain finishes"
        );

        cleanup_clone_root(&state, &project_id, &[&epic_id]);
    }

    /// The registry entry is removed on every exit path (this task's own
    /// AC), the successful-walk case: once a walk with no cancel at all
    /// finishes (`InReview`), nothing is left behind in
    /// `state.cancel_registry`.
    #[tokio::test]
    async fn cancel_registry_is_empty_after_a_normal_successful_walk() {
        let agent =
            Arc::new(ScriptedTaskAgent::new().script(Stage::Implement, writes_file("a.txt", "a")));
        let (state, _app) = test_app_with_task_agent(agent).await;
        let fixture = GitFixture::new().await;
        let project_id = seed_project_with_workspace(&state, &fixture).await;
        let epic_id = seed_epic(&state, &project_id, "InProgress").await;
        seed_task(&state, &epic_id, &project_id, "A").await;

        run_epic_pipeline(state.clone(), epic_id.clone()).await;

        assert_eq!(epic_status(&state, &epic_id).await, "InReview");
        assert!(
            state.cancel_registry.lock().unwrap().is_empty(),
            "the registry must be empty after every stage's guard has dropped"
        );

        cleanup_clone_root(&state, &project_id, &[&epic_id]);
    }

    /// The registry-empty AC, the failed-walk case: an ordinary (non-cancel)
    /// agent-stage failure still routes through `fail_item` exactly as
    /// before T-542 (`route_stage_failure`'s `outcome.cancelled == false`
    /// branch), and the registry is just as empty afterward as it is on a
    /// cancel — `CancelGuard::drop` doesn't care which way the stage ended.
    #[tokio::test]
    async fn cancel_registry_is_empty_after_a_failed_walk() {
        let agent = Arc::new(ScriptedTaskAgent::new().script(
            Stage::Implement,
            ScriptedRun {
                exit_code: Some(1),
                ..ScriptedRun::default()
            },
        ));
        let (state, _app) = test_app_with_task_agent(agent).await;
        let fixture = GitFixture::new().await;
        let project_id = seed_project_with_workspace(&state, &fixture).await;
        let epic_id = seed_epic(&state, &project_id, "InProgress").await;
        let task_id = seed_task(&state, &epic_id, &project_id, "A").await;

        run_epic_pipeline(state.clone(), epic_id.clone()).await;

        assert_eq!(epic_status(&state, &epic_id).await, "Blocked");
        assert_eq!(fetch_task_row(&state, &task_id).await.0, "Failed");
        assert!(
            state.cancel_registry.lock().unwrap().is_empty(),
            "the registry must be empty after an ordinary (non-cancel) failure too"
        );

        cleanup_clone_root(&state, &project_id, &[&epic_id]);
    }

    /// D12's stage-boundary backstop, specifically for a cancel that lands
    /// while a **non-agent** stage is running: `test_gate`'s `test_cmd` is a
    /// real (slow) shell command, so nothing is ever registered in
    /// `state.cancel_registry` while it runs (only agent stages register a
    /// handle) — a cancel issued during that window is a pure DB no-op at
    /// the registry layer, and the walk only stops once the shell command
    /// returns and the next `container_still_in_progress` check (already built
    /// by T-513/T-522, renamed but not behaviorally changed by T-551) observes
    /// the epic is no longer `InProgress`.
    #[tokio::test]
    async fn cancel_during_a_non_agent_stage_never_touches_the_registry() {
        let agent = Arc::new(ScriptedTaskAgent::new());
        let (state, app) = test_app_with_task_agent(agent).await;
        let fixture = GitFixture::new().await;
        // A real, slow-ish green test_cmd — long enough to reliably observe
        // it "running" and issue the cancel mid-command, short enough to
        // keep the test fast.
        let project_id = seed_project_with_test_cmd(&state, &fixture, "sleep 0.5").await;
        let epic_id = seed_epic(&state, &project_id, "InProgress").await;
        let task_id = seed_task(&state, &epic_id, &project_id, "A").await;

        let _handles = spawn_pool(state.clone());
        state.notify.notify_waiters();

        // Bounded readiness poll: wait until the test_gate stage's own
        // agent_run row is open (`status = 'running'`) — proof the shell
        // command is actually in flight.
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        loop {
            let mut rows = state
                .db
                .conn()
                .query(
                    "SELECT status FROM agent_run WHERE task_id = ?1 AND stage = 'test_gate'",
                    params![task_id.clone()],
                )
                .await
                .unwrap();
            if let Some(row) = rows.next().await.unwrap() {
                if row.get::<String>(0).unwrap() == "running" {
                    break;
                }
            }
            if tokio::time::Instant::now() >= deadline {
                panic!("test_gate never started running");
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }

        // Nothing is registered — `test_gate` is not an agent stage.
        assert!(
            state.cancel_registry.lock().unwrap().is_empty(),
            "a non-agent stage must never appear in the cancel registry"
        );

        let response = app
            .clone()
            .oneshot(req(
                "POST",
                &format!("/epics/{epic_id}/lane"),
                Some(json!({ "status": "Cancelled" })),
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        // Still nothing registered — the lookup found no entry, so
        // `RunControl::cancel()` was never called at all.
        assert!(state.cancel_registry.lock().unwrap().is_empty());

        // The walk stops cleanly once the shell command returns and the next
        // stage-boundary check observes the epic is gone. There is no
        // "the task changed status" signal to poll on here — the
        // stage-boundary stop is a plain `return`, deliberately with no
        // further writes (module doc: "Failure and cancellation both stop
        // the walk the same way") — so the unambiguous "the walk has
        // finished" signal is [`try_claim_and_run`] releasing the lease it
        // took to run this claim.
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        loop {
            let (lease_owner, _) = epic_lease(&state, &epic_id).await;
            if lease_owner.is_none() {
                break;
            }
            if tokio::time::Instant::now() >= deadline {
                panic!("the claimed epic's lease was never released after the boundary check should have stopped the walk");
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }

        // The task was never finalized — the stage-boundary check stopped
        // the walk before `commit_if_dirty`/`Done` could run (it was left
        // exactly where the D12 backstop caught it, unchanged from every
        // pre-T-542 between-stage stop).
        assert_ne!(
            fetch_task_row(&state, &task_id).await.0,
            "Done",
            "the task must never be finalized once the cancel landed mid-stage"
        );
        assert_eq!(epic_status(&state, &epic_id).await, "Cancelled");
        assert!(state.cancel_registry.lock().unwrap().is_empty());

        cleanup_clone_root(&state, &project_id, &[&epic_id]);
    }

    // ---- T-543: agent stage timeouts ---------------------------------------
    //
    // `run_agent_stage`'s own test module (`task_agent.rs`) already proves the
    // deadline/grace-period mechanics in isolation: the row closes
    // `status='timeout'` with the flushed partial log, `AgentStageOutcome::
    // timed_out` is set, and the cancel-registry entry is removed. What this
    // module owns, and what wasn't yet proven anywhere before this test, is
    // [`route_stage_failure`]'s own three-way branch actually being reached by
    // a *real* deadline (not a hand-built `AgentStageOutcome`) — i.e. that a
    // timed-out stage genuinely takes T-540's ordinary `fail_item` route
    // (`Failed(timeout)` / `Blocked(timeout)`), not T-542's
    // `handle_cancelled_task` route, even though both share the identical
    // `RunControl::cancel()` kill underneath.

    /// This task's headline AC, end to end: with
    /// `agent_stage_timeout_secs` configured tiny and `Stage::Implement`
    /// gated so it never exits, the deadline fires for real inside
    /// `run_agent_stage` (no hand-built outcome) and the walk observes a
    /// `timed_out` outcome through the live pool — proving
    /// `route_stage_failure` sends it through `fail_item` exactly like any
    /// other agent-stage failure (this task's AC line: "the stage counts as
    /// that stage's ordinary failure... not a special one"), not through
    /// T-542's `Todo`-resetting cancel path: the task lands `Failed` with
    /// `failure_reason = 'timeout'`, the epic blocks `Blocked` with
    /// `blocked_reason = 'timeout'` (never `Cancelled` — nobody cancelled
    /// anything), the `implement` row's partial log survives, the workspace
    /// is retained, no PR ever opens, the cancel-registry entry is gone, and
    /// — the AC's own "the worker slot is released" clause — the claimed
    /// epic's lease is released same as any other task-scoped failure.
    #[tokio::test]
    async fn implement_timeout_fails_the_task_and_blocks_the_epic_the_ordinary_way() {
        let mut config = Config::for_test();
        config.executor.agent_stage_timeout_secs = 1;

        let gate = Arc::new(Gate::default());
        let agent: Arc<dyn TaskAgent> =
            Arc::new(ScriptedTaskAgent::new().with_gate(gate.clone()).script(
                Stage::Implement,
                ScriptedRun {
                    text: vec!["partial output before the deadline kill".to_string()],
                    ..ScriptedRun::default()
                },
            ));
        let fake = Arc::new(FakeHost::new());
        let db = Db::connect(":memory:").await.unwrap();
        db.run_migrations().await.unwrap();
        let state = AppState::with_all_agents_and_host(
            config,
            db,
            Arc::new(SilentPlanningAgent),
            Arc::new(SilentBreakdownAgent),
            agent,
            fake.clone(),
        );
        let fixture = GitFixture::new().await;
        let project_id = seed_project_with_workspace(&state, &fixture).await;
        let epic_id = seed_epic(&state, &project_id, "InProgress").await;
        let task_id = seed_task(&state, &epic_id, &project_id, "A").await;

        let _handles = spawn_pool(state.clone());
        state.notify.notify_waiters();

        // Bounded readiness poll: first prove the epic was actually claimed
        // (lease held) — otherwise a lease that reads `None` from the very
        // first check below would be a false positive (nothing claimed yet),
        // not the "released after a real walk" signal this test needs.
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        loop {
            let (lease_owner, _) = epic_lease(&state, &epic_id).await;
            if lease_owner.is_some() {
                break;
            }
            if tokio::time::Instant::now() >= deadline {
                panic!("the epic was never claimed at all");
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }

        // Now the unambiguous "the walk has stopped" signal (same pattern
        // `cancel_during_a_non_agent_stage_never_touches_the_registry` uses
        // above): the lease released, well past the 1s deadline plus
        // `AGENT_TIMEOUT_KILL_GRACE_PERIOD`.
        let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
        loop {
            let (lease_owner, _) = epic_lease(&state, &epic_id).await;
            if lease_owner.is_none() {
                break;
            }
            if tokio::time::Instant::now() >= deadline {
                panic!("the claimed epic's lease was never released after the timeout should have stopped the walk");
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }

        // The task failed with the timeout reason, not agent_error/cancelled.
        let (status, failure_reason) = fetch_task_row(&state, &task_id).await;
        assert_eq!(status, "Failed");
        assert_eq!(failure_reason.as_deref(), Some("timeout"));

        // The epic blocked with the matching reason — never Cancelled.
        let epic = fetch_epic(state.db.conn(), &epic_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(epic.status, "Blocked");
        assert_eq!(epic.blocked_reason.as_deref(), Some("timeout"));

        // The `implement` agent_run row closed `timeout` with its partial
        // (pre-kill) log intact.
        let mut rows = state
            .db
            .conn()
            .query(
                "SELECT status, log FROM agent_run WHERE task_id = ?1 AND stage = 'implement'",
                params![task_id.clone()],
            )
            .await
            .unwrap();
        let row = rows
            .next()
            .await
            .unwrap()
            .expect("the implement row must exist");
        assert_eq!(row.get::<String>(0).unwrap(), "timeout");
        let log: String = row.get(1).unwrap();
        assert!(
            log.contains("partial output before the deadline kill"),
            "the flushed partial log must be preserved: {log:?}"
        );

        // The workspace is retained on disk — same triage-push every
        // task-scoped failure gets (D10, §7), unchanged by this task.
        let workspace_path = workspace::epic_workspace_path(&state.config.clone_root, &epic_id);
        assert!(
            workspace_path.join(".git").exists(),
            "the workspace must be retained after a timeout"
        );

        // No PR was ever opened.
        assert!(
            fake.open_pr_calls().is_empty(),
            "a timed-out epic must never reach finalize/open_pr"
        );

        // The registry entry is gone — removed once run_agent_stage returned,
        // timeout or not (T-542's guard, unchanged by this task).
        assert!(
            state.cancel_registry.lock().unwrap().is_empty(),
            "the registry entry must be removed once the timed-out stage's drain finishes"
        );

        cleanup_clone_root(&state, &project_id, &[&epic_id]);
        // Deliberately never `gate.release()`d during the assertions above —
        // release now, purely as teardown hygiene (same reasoning as
        // `task_agent.rs`'s own gated timeout test): by this point the
        // deadline, the cancel, and the full grace period have already
        // happened and every assertion above already passed, so releasing
        // here only lets the scripted thread's `tx` drop and the pool's
        // background loops wind down cleanly instead of leaking a parked
        // thread for the rest of the test binary's life.
        gate.release();
    }

    // ==== T-551: run a standalone task end-to-end ===========================

    /// `POST /tasks/{id}/run` — `409` unless the task is `Todo` **and**
    /// `epic_id IS NULL` (§2.5). An epic-scoped task fails this even while
    /// `Todo`: the `WHERE epic_id IS NULL` clause excludes it regardless of
    /// its own status — it is only ever run as part of its epic.
    #[tokio::test]
    async fn run_task_endpoint_409s_for_an_epic_scoped_task_even_when_todo() {
        let (state, app) = test_app().await;
        let project_id = seed_project(&state).await;
        let epic_id = seed_epic(&state, &project_id, "Ready").await;
        let task_id = seed_task(&state, &epic_id, &project_id, "A").await; // Todo, epic-scoped

        let response = app
            .oneshot(req("POST", &format!("/tasks/{task_id}/run"), None))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::CONFLICT);
        assert_eq!(
            single_task_status(&state, &task_id).await,
            "Todo",
            "must be untouched"
        );
    }

    /// `409` for a standalone task in every status other than `Todo`.
    #[tokio::test]
    async fn run_task_endpoint_409s_for_every_non_todo_standalone_status() {
        let (state, app) = test_app().await;
        let project_id = seed_project(&state).await;

        for status in ["InProgress", "Done", "Failed", "Cancelled"] {
            let task_id = seed_standalone_task(&state, &project_id, "Standalone", status).await;
            let response = app
                .clone()
                .oneshot(req("POST", &format!("/tasks/{task_id}/run"), None))
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::CONFLICT, "status={status}");
            assert_eq!(
                single_task_status(&state, &task_id).await,
                status,
                "status={status}: must be untouched"
            );
        }
    }

    /// `404` for a task that doesn't exist at all.
    #[tokio::test]
    async fn run_task_endpoint_404s_for_unknown_task() {
        let (_state, app) = test_app().await;
        let response = app
            .oneshot(req("POST", "/tasks/nope/run", None))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    /// The endpoint's own happy path in isolation (no pool involved): `Todo →
    /// InProgress`, the returned row reflects it, and `board_updated`
    /// publishes on `project:<id>` — the AC's "enqueue, notify" plus "board
    /// shows it" made concrete before any pipeline work happens at all.
    #[tokio::test]
    async fn run_task_endpoint_moves_todo_to_in_progress_and_publishes_board_updated() {
        let (state, app) = test_app().await;
        let project_id = seed_project(&state).await;
        let task_id = seed_standalone_task(&state, &project_id, "Standalone", "Todo").await;

        let mut proj_sub = state.hub.subscribe(&format!("project:{project_id}"));

        let response = app
            .oneshot(req("POST", &format!("/tasks/{task_id}/run"), None))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let task = body_json(response).await;
        assert_eq!(task["status"], "InProgress");
        assert_eq!(task["epic_id"], Value::Null);

        let frame = proj_sub.recv().await.unwrap();
        let v: Value = serde_json::from_str(&frame).unwrap();
        assert_eq!(v["type"], "board_updated");
        assert_eq!(v["payload"]["tasks"][0]["status"], "InProgress");
    }

    /// The full T-551 happy path (MILESTONE_2 §8's own AC), driven exactly the
    /// way a human would: `POST /tasks/{id}/run` with a worker pool running.
    /// Mirrors `enqueue_via_lane_drives_dag_to_done_and_completes_with_pr`'s
    /// epic version one level flatter (no DAG, one task): preflight →
    /// implement → test gate → commit → review → push → PR, all the way to
    /// `InReview` with `pr_url`/`pr_number` persisted on the *task* row
    /// (there is no epic) and the workspace retained.
    #[tokio::test]
    async fn standalone_task_happy_path_end_to_end_with_pr() {
        let agent = Arc::new(
            ScriptedTaskAgent::new()
                .script(Stage::Implement, writes_file("a.txt", "a\n"))
                .script(Stage::Review, review_pass()),
        );
        let (state, app) = test_app_with_task_agent(agent).await;
        let fixture = GitFixture::new().await;
        let project_id = seed_project_with_workspace(&state, &fixture).await;
        let task_id = seed_standalone_task(&state, &project_id, "Standalone A", "Todo").await;

        let workspace_path = workspace::task_workspace_path(&state.config.clone_root, &task_id);

        // Start the pool: the run endpoint no longer spawns anything itself
        // (mirrors the lane handler's own contract), so a pool must be
        // running to consume the enqueue+notify.
        let _handles = spawn_pool(state.clone());

        let response = app
            .clone()
            .oneshot(req("POST", &format!("/tasks/{task_id}/run"), None))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(body_json(response).await["status"], "InProgress");

        // Poll (bounded) until the task reaches InReview — the standalone
        // task's post-PR terminal success state (finalize pushes + opens the
        // PR and lands it there, retaining the workspace for feedback
        // rounds).
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        loop {
            if single_task_status(&state, &task_id).await == "InReview" {
                break;
            }
            if tokio::time::Instant::now() >= deadline {
                panic!(
                    "worker pool never landed the standalone task in InReview in time: status={}",
                    single_task_status(&state, &task_id).await
                );
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        // pr_url/pr_number persisted and returned by GET /tasks/{id} — poll a
        // little further since finalize's push/PR runs strictly after the
        // task's own `Done` write, in the same pipeline body.
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        let task_body = loop {
            let get_response = app
                .clone()
                .oneshot(req("GET", &format!("/tasks/{task_id}"), None))
                .await
                .unwrap();
            assert_eq!(get_response.status(), StatusCode::OK);
            let body = body_json(get_response).await;
            if body["pr_url"].as_str().is_some() {
                break body;
            }
            if tokio::time::Instant::now() >= deadline {
                panic!("pr_url was never persisted in time: {body:?}");
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        };
        assert_eq!(task_body["status"], "InReview");
        assert_eq!(task_body["epic_id"], Value::Null);
        assert!(task_body["pr_url"]
            .as_str()
            .expect("pr_url must be persisted and returned")
            .starts_with("https://"));
        assert!(
            task_body["pr_number"].as_i64().is_some(),
            "pr_number must be persisted and returned"
        );
        assert!(
            task_body["branch_name"]
                .as_str()
                .expect("branch_name must be persisted")
                .starts_with("dearborn/task-"),
            "§2.8: standalone branch names are `dearborn/task-<slug>-<id>`"
        );

        // The AC's "pr_url is ... shown on the board": `GET /projects/{id}/board`
        // carries the same `Task` DTO (`board.rs`'s `Board.tasks: Vec<Task>`),
        // so the completed standalone task's PR link must be visible there too
        // — no board-side change was needed for this (see the module doc's
        // "board_updated on every transition" section), but the AC is about
        // what a client actually sees, so assert the board response directly
        // rather than trusting that by inspection alone.
        let board_response = app
            .clone()
            .oneshot(req("GET", &format!("/projects/{project_id}/board"), None))
            .await
            .unwrap();
        assert_eq!(board_response.status(), StatusCode::OK);
        let board = body_json(board_response).await;
        let board_task = board["tasks"]
            .as_array()
            .unwrap()
            .iter()
            .find(|t| t["id"] == task_id.as_str())
            .expect("the standalone task must appear on its project's board");
        assert_eq!(board_task["status"], "InReview");
        assert_eq!(board_task["pr_url"], task_body["pr_url"]);
        assert_eq!(board_task["pr_number"], task_body["pr_number"]);

        // The workspace is retained (never deleted) once the PR opens — the
        // post-PR-review loop needs the branch for feedback rounds.
        assert!(
            workspace_path.join(".git").exists(),
            "the workspace must be retained (not deleted) after the standalone task lands in InReview"
        );

        // Review ran exactly once (a first-try PASS) — the identical T-530
        // evidence trail an epic-owned task's review leaves.
        let rows = review_rows(&state, &task_id).await;
        assert_eq!(
            rows.len(),
            1,
            "exactly one review attempt on a first-try PASS"
        );
        assert_eq!(rows[0].2.as_deref(), Some("PASS"));

        cleanup_clone_root(&state, &project_id, &[]);
    }

    /// A failing standalone task leaves it `Failed` with its branch pushed and
    /// the workspace retained — and, per this task's own AC, "there is no
    /// epic to Block": no epic row is ever seeded in this test, so there is
    /// nothing for a `Blocked` write to even touch. `board_updated` still
    /// publishes on the failure (there is no `epic_updated`/`dag_updated` to
    /// publish instead — see `fail_item`'s own doc, "One container to fail,
    /// not two").
    #[tokio::test]
    async fn standalone_task_failure_leaves_it_failed_with_branch_pushed_and_workspace_retained() {
        let agent = Arc::new(
            ScriptedTaskAgent::new()
                .script(Stage::Implement, writes_file("a.txt", "a\n"))
                .script(Stage::Review, review_blocked()),
        );
        let (state, _app) = test_app_with_task_agent(agent).await;
        let fixture = GitFixture::new().await;
        let project_id = seed_project_with_workspace(&state, &fixture).await;
        let task_id = seed_standalone_task(&state, &project_id, "Standalone A", "InProgress").await;

        let mut proj_sub = state.hub.subscribe(&format!("project:{project_id}"));

        run_standalone_pipeline(state.clone(), task_id.clone()).await;

        let task = fetch_task_row(&state, &task_id).await;
        assert_eq!(task.0, "Failed");
        assert_eq!(task.1.as_deref(), Some("blocked"));

        // board_updated fired for the failure.
        let frame = proj_sub.recv().await.unwrap();
        let v: Value = serde_json::from_str(&frame).unwrap();
        assert_eq!(v["type"], "board_updated");
        assert_eq!(v["payload"]["tasks"][0]["status"], "Failed");
        assert_eq!(v["payload"]["tasks"][0]["failure_reason"], "blocked");

        // The workspace is retained (never deleted on a failure, D10/§7).
        let workspace_path = workspace::task_workspace_path(&state.config.clone_root, &task_id);
        assert!(
            workspace_path.join(".git").exists(),
            "the workspace must be retained on failure"
        );

        // The branch was pushed — the D10/§7 triage push, unconditional on
        // the standalone side (no `took_epic`-style race gate; see
        // `fail_item`'s own doc).
        let branch = {
            let mut rows = state
                .db
                .conn()
                .query(
                    "SELECT branch_name FROM task WHERE id = ?1",
                    params![task_id.clone()],
                )
                .await
                .unwrap();
            rows.next()
                .await
                .unwrap()
                .expect("branch_name must be set once the workspace was provisioned")
                .get::<String>(0)
                .unwrap()
        };
        let subjects = git_log_subjects_for_ref(&fixture.dir, &branch).await;
        assert!(
            subjects.contains(&format!("impl({}): Standalone A", spec::short_id(&task_id))),
            "the committed impl must be on the pushed branch: {subjects:?}"
        );

        cleanup_clone_root(&state, &project_id, &[]);
        let _ = std::fs::remove_dir_all(&workspace_path);
    }

    /// D11/T-551's revised retry contract, proven live: `POST
    /// /tasks/{id}/retry` on a `Failed` standalone task doesn't just flip a
    /// status the HTTP response shows — it leaves the task genuinely
    /// re-claimable, and the same dispatch `worker_loop` itself calls
    /// (`try_claim_and_run`) picks it up and runs it to completion. If retry
    /// had left the task in `Todo` (T-541's original contract, unrevised),
    /// `claim_task`'s predicate (`status = 'InProgress' AND epic_id IS NULL`)
    /// would find nothing here and the second `try_claim_and_run` below would
    /// be a no-op — the task would stay stuck `Todo` forever without a human
    /// separately calling `POST /tasks/{id}/run` again.
    #[tokio::test]
    async fn retried_standalone_task_is_reclaimed_and_rerun() {
        let agent = Arc::new(
            ScriptedTaskAgent::new()
                .script(Stage::Implement, writes_file("a.txt", "a\n"))
                .script(Stage::Review, review_blocked()),
        );
        let (state, app) = test_app_with_task_agent(agent).await;
        let fixture = GitFixture::new().await;
        let project_id = seed_project_with_workspace(&state, &fixture).await;
        let task_id = seed_standalone_task(&state, &project_id, "Standalone A", "Todo").await;

        // Start it, then run the (scripted-to-fail) first attempt directly —
        // a single, bounded claim/run/release, exactly what `try_claim_and_run`
        // itself is.
        let response = app
            .clone()
            .oneshot(req("POST", &format!("/tasks/{task_id}/run"), None))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        try_claim_and_run(&state, "worker-1").await;
        let task = fetch_task_row(&state, &task_id).await;
        assert_eq!(
            task.0, "Failed",
            "first attempt must fail on the scripted BLOCKED verdict"
        );
        assert_eq!(task.1.as_deref(), Some("blocked"));

        // Retry: T-551's revised contract sends a standalone task straight
        // back to InProgress (not Todo — see `retry_task`'s own doc).
        let retry_response = app
            .clone()
            .oneshot(req("POST", &format!("/tasks/{task_id}/retry"), None))
            .await
            .unwrap();
        assert_eq!(retry_response.status(), StatusCode::OK);
        let retried = body_json(retry_response).await;
        assert_eq!(retried["status"], "InProgress");
        assert_eq!(retried["failure_reason"], Value::Null);

        // The real proof: `try_claim_and_run` (the exact dispatch
        // `worker_loop` calls, §2.4's claim query unchanged) actually claims
        // and runs it a second time. The implement stage has nothing left
        // scripted (`ScriptedTaskAgent`'s unscripted default: exit 0, no
        // files), so this run produces no diff and routes through T-532's
        // verify-complete (also unscripted -> default PASS), entering the
        // pipeline's finalize step — which lands the standalone task in
        // `InReview` (T-514/T-551: completion waits on the human PR review,
        // never `Done`) — with zero new commits before it opens the PR.
        try_claim_and_run(&state, "worker-2").await;

        let task = fetch_task_row(&state, &task_id).await;
        assert_eq!(
            task.0, "InReview",
            "the retried task must have actually resumed and completed to InReview"
        );
        assert_eq!(task.1, None, "failure_reason must stay cleared");

        let workspace_path = workspace::task_workspace_path(&state.config.clone_root, &task_id);
        cleanup_clone_root(&state, &project_id, &[]);
        let _ = std::fs::remove_dir_all(&workspace_path);
    }

    // ---- T-560: PR body — template + agent summary ------------------------
    //
    // See the module doc's own "T-560" section for the full design; these
    // tests prove, one failure mode at a time, that the PR is genuinely
    // **never** blocked on the summarize stage, plus that the two new §9
    // scaffold elements (review-round counts, verified-already-complete
    // slices) actually reach the rendered body end-to-end, not just in
    // `pr.rs`'s own unit tests.

    /// A `TaskAgent` wrapper whose `run()` fails to *start* (mimics `claude`
    /// missing from `PATH` — [`HarnessError::Other`]) for exactly one
    /// [`Stage`], delegating to `inner` for every other stage. Used for T-560's
    /// "the harness never spawned" fallback proof, which
    /// [`crate::task_agent::testing::ScriptedTaskAgent`] alone cannot express
    /// (every one of its scripted runs still starts; only its *outcome*
    /// varies).
    struct FailToSpawnAtStage {
        stage: Stage,
        inner: ScriptedTaskAgent,
    }

    impl TaskAgent for FailToSpawnAtStage {
        fn run(
            &self,
            req: TaskRunRequest,
        ) -> Result<(RunHandle, Receiver<RunEvent>), HarnessError> {
            if req.stage == self.stage {
                return Err(HarnessError::Other(format!(
                    "boom: {} failed to spawn",
                    self.stage.as_str()
                )));
            }
            self.inner.run(req)
        }
    }

    /// The one `agent_run` row for `stage`, however many there are expected
    /// to be (asserts exactly one) — used by this section's tests to check
    /// the summarize row's `task_id`/`epic_id`/`status` shape directly,
    /// rather than only inferring it from the rendered PR body.
    async fn sole_agent_run_row(
        state: &AppState,
        stage: &str,
    ) -> (Option<String>, Option<String>, String) {
        let mut rows = state
            .db
            .conn()
            .query(
                "SELECT task_id, epic_id, status FROM agent_run WHERE stage = ?1",
                params![stage],
            )
            .await
            .unwrap();
        let row =
            rows.next().await.unwrap().unwrap_or_else(|| {
                panic!("expected exactly one '{stage}' agent_run row, found none")
            });
        // Extract every value **before** advancing the cursor again — a
        // `Row` is a view onto libsql's current cursor position, and calling
        // `rows.next()` a second time invalidates it (observed directly: a
        // deferred `row.get(..)` after the "no second row" check below
        // returned a spurious `NullValue`, not this row's real columns).
        let values = (
            row.get::<Option<String>>(0).unwrap(),
            row.get::<Option<String>>(1).unwrap(),
            row.get::<String>(2).unwrap(),
        );
        assert!(
            rows.next().await.unwrap().is_none(),
            "expected exactly one '{stage}' agent_run row"
        );
        values
    }

    /// Happy path: a clean, non-blank `Stage::Summarize` reply lands under
    /// its own "## Summary of changes" heading in the opened PR's body, and
    /// is recorded as an **epic-scoped** `agent_run` row (`task_id: NULL`,
    /// `epic_id: Some(_)`) — the AC's "the summary is stored as an
    /// `agent_run` row," checked directly rather than only inferred from the
    /// rendered body.
    #[tokio::test]
    async fn summarize_stage_text_appears_under_its_own_heading_in_the_pr_body() {
        let agent = Arc::new(
            ScriptedTaskAgent::new()
                .script(Stage::Implement, writes_file("a.txt", "a\n"))
                .script(Stage::Review, review_pass())
                .script(
                    Stage::Summarize,
                    ScriptedRun {
                        text: vec!["This epic adds a.txt with a short greeting.".to_string()],
                        ..ScriptedRun::default()
                    },
                ),
        );
        let fake = Arc::new(FakeHost::new());
        let (state, _app) = test_app_with_task_agent_and_host(agent, fake.clone()).await;
        let fixture = GitFixture::new().await;
        let project_id = seed_project_with_workspace(&state, &fixture).await;
        let epic_id = seed_epic(&state, &project_id, "InProgress").await;
        seed_task(&state, &epic_id, &project_id, "A").await;

        run_epic_pipeline(state.clone(), epic_id.clone()).await;

        assert_eq!(epic_status(&state, &epic_id).await, "InReview");
        let calls = fake.open_pr_calls();
        assert_eq!(calls.len(), 1);
        assert!(calls[0].body.contains("## Summary of changes"));
        assert!(calls[0]
            .body
            .contains("This epic adds a.txt with a short greeting."));

        let (task_id, epic_id_col, status) = sole_agent_run_row(&state, "summarize").await;
        assert_eq!(task_id, None, "an epic-scoped summarize run has no task_id");
        assert_eq!(epic_id_col.as_deref(), Some(epic_id.as_str()));
        assert_eq!(status, "ok");

        cleanup_clone_root(&state, &project_id, &[&epic_id]);
    }

    /// A `Stage::Summarize` run that exits non-zero (the harness started but
    /// the agent itself failed/errored) still opens the PR, with the
    /// template alone — no "Summary of changes" heading at all.
    #[tokio::test]
    async fn summarize_stage_nonzero_exit_still_opens_pr_with_template_only() {
        let agent = Arc::new(
            ScriptedTaskAgent::new()
                .script(Stage::Implement, writes_file("a.txt", "a\n"))
                .script(Stage::Review, review_pass())
                .script(
                    Stage::Summarize,
                    ScriptedRun {
                        text: vec!["partial output before the agent errored".to_string()],
                        exit_code: Some(1),
                        ..ScriptedRun::default()
                    },
                ),
        );
        let fake = Arc::new(FakeHost::new());
        let (state, _app) = test_app_with_task_agent_and_host(agent, fake.clone()).await;
        let fixture = GitFixture::new().await;
        let project_id = seed_project_with_workspace(&state, &fixture).await;
        let epic_id = seed_epic(&state, &project_id, "InProgress").await;
        seed_task(&state, &epic_id, &project_id, "A").await;

        run_epic_pipeline(state.clone(), epic_id.clone()).await;

        assert_eq!(
            epic_status(&state, &epic_id).await,
            "InReview",
            "the PR must open even though the summary run itself failed"
        );
        let calls = fake.open_pr_calls();
        assert_eq!(calls.len(), 1);
        assert!(
            calls[0].body.contains("## Tasks"),
            "the template must still render"
        );
        assert!(!calls[0].body.contains("## Summary of changes"));

        let (_, _, status) = sole_agent_run_row(&state, "summarize").await;
        assert_eq!(status, "error");

        cleanup_clone_root(&state, &project_id, &[&epic_id]);
    }

    /// An empty `Stage::Summarize` reply (the agent exited cleanly and said
    /// nothing) is treated identically to an absent summary — the PR opens
    /// with the template alone.
    #[tokio::test]
    async fn summarize_stage_empty_reply_still_opens_pr_with_template_only() {
        let agent = Arc::new(
            ScriptedTaskAgent::new()
                .script(Stage::Implement, writes_file("a.txt", "a\n"))
                .script(Stage::Review, review_pass())
                .script(
                    Stage::Summarize,
                    ScriptedRun {
                        text: vec![String::new()],
                        ..ScriptedRun::default()
                    },
                ),
        );
        let fake = Arc::new(FakeHost::new());
        let (state, _app) = test_app_with_task_agent_and_host(agent, fake.clone()).await;
        let fixture = GitFixture::new().await;
        let project_id = seed_project_with_workspace(&state, &fixture).await;
        let epic_id = seed_epic(&state, &project_id, "InProgress").await;
        seed_task(&state, &epic_id, &project_id, "A").await;

        run_epic_pipeline(state.clone(), epic_id.clone()).await;

        assert_eq!(epic_status(&state, &epic_id).await, "InReview");
        let calls = fake.open_pr_calls();
        assert_eq!(calls.len(), 1);
        assert!(!calls[0].body.contains("## Summary of changes"));

        // The stage itself still ran cleanly — this is "said nothing," not
        // "errored" — so its own row still closes `ok`.
        let (_, _, status) = sole_agent_run_row(&state, "summarize").await;
        assert_eq!(status, "ok");

        cleanup_clone_root(&state, &project_id, &[&epic_id]);
    }

    /// A whitespace-only `Stage::Summarize` reply is treated identically to
    /// an empty one — [`pr::build_pr_body`]'s own trim-and-filter-blank rule
    /// applies uniformly, but this proves the worker-side wiring doesn't
    /// short-circuit on `is_empty()` alone and skip the trim.
    #[tokio::test]
    async fn summarize_stage_whitespace_only_reply_still_opens_pr_with_template_only() {
        let agent = Arc::new(
            ScriptedTaskAgent::new()
                .script(Stage::Implement, writes_file("a.txt", "a\n"))
                .script(Stage::Review, review_pass())
                .script(
                    Stage::Summarize,
                    ScriptedRun {
                        text: vec!["   \n\t  ".to_string()],
                        ..ScriptedRun::default()
                    },
                ),
        );
        let fake = Arc::new(FakeHost::new());
        let (state, _app) = test_app_with_task_agent_and_host(agent, fake.clone()).await;
        let fixture = GitFixture::new().await;
        let project_id = seed_project_with_workspace(&state, &fixture).await;
        let epic_id = seed_epic(&state, &project_id, "InProgress").await;
        seed_task(&state, &epic_id, &project_id, "A").await;

        run_epic_pipeline(state.clone(), epic_id.clone()).await;

        assert_eq!(epic_status(&state, &epic_id).await, "InReview");
        let calls = fake.open_pr_calls();
        assert_eq!(calls.len(), 1);
        assert!(!calls[0].body.contains("## Summary of changes"));

        cleanup_clone_root(&state, &project_id, &[&epic_id]);
    }

    /// The harness never spawning at all for `Stage::Summarize` (the T-512
    /// `AgentStageError::Harness` path) still opens the PR with the template
    /// alone — [`FailToSpawnAtStage`] forces exactly this, while every other
    /// stage runs normally through the wrapped [`ScriptedTaskAgent`].
    #[tokio::test]
    async fn summarize_stage_harness_spawn_failure_still_opens_pr_with_template_only() {
        let agent: Arc<dyn TaskAgent> = Arc::new(FailToSpawnAtStage {
            stage: Stage::Summarize,
            inner: ScriptedTaskAgent::new()
                .script(Stage::Implement, writes_file("a.txt", "a\n"))
                .script(Stage::Review, review_pass()),
        });
        let fake = Arc::new(FakeHost::new());
        let (state, _app) = test_app_with_task_agent_and_host(agent, fake.clone()).await;
        let fixture = GitFixture::new().await;
        let project_id = seed_project_with_workspace(&state, &fixture).await;
        let epic_id = seed_epic(&state, &project_id, "InProgress").await;
        seed_task(&state, &epic_id, &project_id, "A").await;

        run_epic_pipeline(state.clone(), epic_id.clone()).await;

        assert_eq!(
            epic_status(&state, &epic_id).await,
            "InReview",
            "the PR must open even though the harness never spawned for the summary stage"
        );
        let calls = fake.open_pr_calls();
        assert_eq!(calls.len(), 1);
        assert!(!calls[0].body.contains("## Summary of changes"));

        cleanup_clone_root(&state, &project_id, &[&epic_id]);
    }

    /// A `Stage::Summarize` run that never exits (gated, ungated release)
    /// hits `DEARBORN_AGENT_STAGE_TIMEOUT_SECS` (D18) exactly like any other
    /// agent stage, closes its own `agent_run` row `status = 'timeout'`, and
    /// still lets the PR open with the template alone — the module doc's own
    /// "ordering" section's whole point: non-blocking, not fast.
    #[tokio::test]
    async fn summarize_stage_timeout_still_opens_pr_with_template_only() {
        let mut config = Config::for_test();
        config.executor.agent_stage_timeout_secs = 1;

        // Not released until teardown at the very end (see the comment
        // there) — mirrors `implement_timeout_fails_the_task_and_blocks_the_epic_the_ordinary_way`'s
        // own gated-timeout test exactly: releasing only after every
        // assertion below has already passed avoids leaking a thread parked
        // on this gate forever, which would otherwise hang this test
        // binary's process exit waiting for `run_agent_stage`'s detached
        // drain thread to notice the channel close (see that function's own
        // "waiting for the kill to land, bounded" doc section for why the
        // drain thread itself can never be force-stopped).
        let gate = Arc::new(Gate::default());
        let agent: Arc<dyn TaskAgent> = Arc::new(
            ScriptedTaskAgent::new()
                .script(Stage::Implement, writes_file("a.txt", "a\n"))
                .script(Stage::Review, review_pass())
                .with_gate_on(Stage::Summarize, gate.clone()),
        );
        let fake = Arc::new(FakeHost::new());
        let db = Db::connect(":memory:").await.unwrap();
        db.run_migrations().await.unwrap();
        let state = AppState::with_all_agents_and_host(
            config,
            db,
            Arc::new(SilentPlanningAgent),
            Arc::new(SilentBreakdownAgent),
            agent,
            fake.clone(),
        );
        let fixture = GitFixture::new().await;
        let project_id = seed_project_with_workspace(&state, &fixture).await;
        let epic_id = seed_epic(&state, &project_id, "InProgress").await;
        seed_task(&state, &epic_id, &project_id, "A").await;

        run_epic_pipeline(state.clone(), epic_id.clone()).await;

        assert_eq!(
            epic_status(&state, &epic_id).await,
            "InReview",
            "the PR must open even though the summary run timed out"
        );
        let calls = fake.open_pr_calls();
        assert_eq!(calls.len(), 1);
        assert!(!calls[0].body.contains("## Summary of changes"));

        let (_, _, status) = sole_agent_run_row(&state, "summarize").await;
        assert_eq!(status, "timeout");

        cleanup_clone_root(&state, &project_id, &[&epic_id]);
        // Teardown hygiene, not part of the assertions above — see the gate
        // construction comment for why this must happen last, not be skipped.
        gate.release();
    }

    /// §9's "review-round counts" scaffold element, end to end: a
    /// `NEEDS_CHANGES` → fix → `PASS` convergence (mirrors
    /// `needs_changes_then_pass_converges_with_two_commits_and_closes_the_task`)
    /// renders "2 review rounds" for the task in the opened PR's body.
    #[tokio::test]
    async fn review_round_count_appears_in_the_pr_body() {
        let agent = Arc::new(
            ScriptedTaskAgent::new()
                .script(Stage::Implement, writes_file("a.txt", "a\n"))
                .script(Stage::Review, review_needs_changes())
                .script(Stage::Fix, writes_file("b.txt", "b\n"))
                .script(Stage::Review, review_pass()),
        );
        let fake = Arc::new(FakeHost::new());
        let (state, _app) = test_app_with_task_agent_and_host(agent, fake.clone()).await;
        let fixture = GitFixture::new().await;
        let project_id = seed_project_with_workspace(&state, &fixture).await;
        let epic_id = seed_epic(&state, &project_id, "InProgress").await;
        let a = seed_task(&state, &epic_id, &project_id, "A").await;

        run_epic_pipeline(state.clone(), epic_id.clone()).await;

        assert_eq!(epic_status(&state, &epic_id).await, "InReview");
        let calls = fake.open_pr_calls();
        assert_eq!(calls.len(), 1);
        assert!(calls[0].body.contains("## Review rounds"));
        assert!(calls[0]
            .body
            .contains(&format!("A (`{}`): 2 review rounds", spec::short_id(&a))));

        cleanup_clone_root(&state, &project_id, &[&epic_id]);
    }

    /// §9's "verified-already-complete slices" scaffold element, end to end:
    /// a `PASS`ing `Stage::VerifyComplete` (mirrors
    /// `verify_complete_pass_closes_the_task_done_with_zero_commits_and_is_visible_in_run_history`)
    /// puts the task's own reasoning text under a "Verified already
    /// complete" heading in the opened PR's body — the same evidence T-532's
    /// own AC put in the task's run history, one hop closer to a reviewer.
    #[tokio::test]
    async fn verified_already_complete_reasoning_appears_in_the_pr_body() {
        let agent = Arc::new(
            ScriptedTaskAgent::new().script(Stage::VerifyComplete, verify_complete_pass()),
        );
        let fake = Arc::new(FakeHost::new());
        let (state, _app) = test_app_with_task_agent_and_host(agent, fake.clone()).await;
        let fixture = GitFixture::new().await;
        let project_id = seed_project_with_workspace(&state, &fixture).await;
        let epic_id = seed_epic(&state, &project_id, "InProgress").await;
        let a = seed_task(&state, &epic_id, &project_id, "A").await;

        run_epic_pipeline(state.clone(), epic_id.clone()).await;

        assert_eq!(epic_status(&state, &epic_id).await, "InReview");
        let calls = fake.open_pr_calls();
        assert_eq!(calls.len(), 1);
        assert!(calls[0].body.contains("## Verified already complete"));
        assert!(calls[0]
            .body
            .contains(&format!("**A** (`{}`)", spec::short_id(&a))));
        assert!(calls[0].body.contains("already built by an earlier task"));
        // The task never committed, so it must not also show up in the
        // "Review rounds" section (T-532's PASS-on-first-look skips review
        // entirely).
        assert!(!calls[0].body.contains("## Review rounds"));

        cleanup_clone_root(&state, &project_id, &[&epic_id]);
    }

    /// Standalone tasks get a summary too (module doc: "standalone tasks get
    /// one too") — happy path, and the row is **task-scoped**
    /// (`task_id: Some(_)`, `epic_id: NULL`), the mirror image of the
    /// epic-scoped row's shape above.
    #[tokio::test]
    async fn standalone_task_summary_appears_in_the_pr_body_and_is_task_scoped() {
        let agent = Arc::new(
            ScriptedTaskAgent::new()
                .script(Stage::Implement, writes_file("a.txt", "a\n"))
                .script(Stage::Review, review_pass())
                .script(
                    Stage::Summarize,
                    ScriptedRun {
                        text: vec!["This change adds a.txt.".to_string()],
                        ..ScriptedRun::default()
                    },
                ),
        );
        let fake = Arc::new(FakeHost::new());
        let (state, _app) = test_app_with_task_agent_and_host(agent, fake.clone()).await;
        let fixture = GitFixture::new().await;
        let project_id = seed_project_with_workspace(&state, &fixture).await;
        let task_id = seed_standalone_task(&state, &project_id, "Standalone A", "InProgress").await;

        run_standalone_pipeline(state.clone(), task_id.clone()).await;

        assert_eq!(single_task_status(&state, &task_id).await, "InReview");
        let calls = fake.open_pr_calls();
        assert_eq!(calls.len(), 1);
        assert!(calls[0].body.contains("## Summary of changes"));
        assert!(calls[0].body.contains("This change adds a.txt."));

        let (row_task_id, row_epic_id, status) = sole_agent_run_row(&state, "summarize").await;
        assert_eq!(row_task_id.as_deref(), Some(task_id.as_str()));
        assert_eq!(
            row_epic_id, None,
            "a standalone task's summarize run has no epic_id"
        );
        assert_eq!(status, "ok");

        let workspace_path = workspace::task_workspace_path(&state.config.clone_root, &task_id);
        cleanup_clone_root(&state, &project_id, &[]);
        let _ = std::fs::remove_dir_all(&workspace_path);
    }

    /// The standalone mirror of the epic-scoped fallback proofs above: a
    /// blank summary reply still opens the standalone task's own PR with the
    /// template alone.
    #[tokio::test]
    async fn standalone_task_summary_failure_still_opens_pr_with_template_only() {
        let agent = Arc::new(
            ScriptedTaskAgent::new()
                .script(Stage::Implement, writes_file("a.txt", "a\n"))
                .script(Stage::Review, review_pass())
                .script(
                    Stage::Summarize,
                    ScriptedRun {
                        text: vec![String::new()],
                        ..ScriptedRun::default()
                    },
                ),
        );
        let fake = Arc::new(FakeHost::new());
        let (state, _app) = test_app_with_task_agent_and_host(agent, fake.clone()).await;
        let fixture = GitFixture::new().await;
        let project_id = seed_project_with_workspace(&state, &fixture).await;
        let task_id = seed_standalone_task(&state, &project_id, "Standalone A", "InProgress").await;

        run_standalone_pipeline(state.clone(), task_id.clone()).await;

        assert_eq!(single_task_status(&state, &task_id).await, "InReview");
        let calls = fake.open_pr_calls();
        assert_eq!(calls.len(), 1);
        assert!(!calls[0].body.contains("## Summary of changes"));
        assert!(calls[0].body.contains("## Tasks"));

        let workspace_path = workspace::task_workspace_path(&state.config.clone_root, &task_id);
        cleanup_clone_root(&state, &project_id, &[]);
        let _ = std::fs::remove_dir_all(&workspace_path);
    }
}
