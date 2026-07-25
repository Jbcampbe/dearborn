//! The non-agent-stage shell command runner (T-520): `setup`, `preflight`,
//! and `test_gate` (§2.2) all boil down to "run one shell command in the
//! workspace and record what happened" — this module is the one place that
//! logic lives, on top of [`crate::evidence`]'s stage lifecycle (T-512).
//!
//! ## Two layers
//!
//! [`run_shell_timed`] is the low-level primitive: `sh -c <cmd>` in a
//! directory, combined stdout+stderr, a wall-clock timeout, process-group
//! kill on expiry. It knows nothing about `agent_run` — it is plumbing, and
//! is exercised directly by this module's timeout/process-group tests so
//! those tests don't need a database at all.
//!
//! [`run_stage_command`] is what `setup`/`preflight`/`test_gate` actually
//! call: it wraps [`run_shell_timed`] with the D18/§2.2 evidence contract —
//! open a `running` row, run the command, close the row with a terminal
//! status — and it is the **only** place that decides "no command configured
//! means no row" (see [`StageOutcome`]).
//!
//! ## Combined output
//!
//! Reading a child's stdout and stderr as two separate pipes does not
//! preserve their relative interleaving (the OS makes no ordering guarantee
//! across two independent pipes). Instead, the caller's command is wrapped as
//! `( <cmd> ) 2>&1` and handed to `sh -c` as a single string: the shell
//! itself merges stderr into stdout *before* either reaches us, so the
//! captured bytes are in true execution order — a maintainer reading the log
//! later sees output and errors interleaved the way they actually happened,
//! not one after the other.
//!
//! ## Why process-group kill, not just killing `sh`
//!
//! `sh -c "cargo test"` (or any nontrivial `test_cmd`/`setup_cmd`) spawns
//! child processes of its own. Killing only the `sh` parent when the timeout
//! expires leaves those children running and still holding the workspace
//! (open files, file locks, ports) — exactly the "runaway child that forks
//! its own children is fully reaped" failure the T-511 module doc for this
//! file already flagged as T-520's job. [`run_shell_timed`] instead spawns
//! `sh` as the leader of a **new process group**
//! ([`tokio::process::Command::process_group`]`(0)` — the same call
//! std's own `CommandExt::process_group` provides, exposed directly by
//! tokio's builder, `#[cfg(unix)]`-gated, so no extra import is needed to
//! set it) and, on timeout, signals the whole
//! group (`libc::kill(-pid, ...)`, negative pid is POSIX's "the whole
//! process group" target) rather than just the one child pid. `libc` is
//! already in `Cargo.lock` at v0.2.186 (pulled in transitively via `tokio`),
//! so declaring it as a direct dependency here compiles nothing new — it's
//! preferred over shelling out to the `kill` binary, which would mean
//! spawning *another* process (with its own failure modes: missing binary,
//! another fork/exec round trip) just to deliver a signal libc can send
//! directly. We signal `SIGTERM` first and give the group a short grace
//! period to exit cleanly (flush buffers, run destructors) before following
//! up with `SIGKILL` for anything still alive — a well-behaved child gets a
//! chance to shut down; a stuck one is still guaranteed to die.
//!
//! This is Unix-only (`#[cfg(unix)]` gates the process-group/signal code);
//! Dearborn's server is not a supported host on Windows (see README's host
//! prerequisites), so no non-Unix fallback is implemented beyond "compiles,
//! kills only the direct child" — acceptable degradation for a platform we
//! do not ship on, rather than a silent correctness gap on the one we do.
//!
//! ## In-memory accumulation is *not* bounded
//!
//! [`run_shell_timed`] accumulates the full combined output in memory as it
//! streams in, uncapped, for the whole lifetime of the run. Only
//! [`crate::evidence::cap_log`] (D13, 256 KB head+tail) trims it, and that
//! happens once, at the very end, on the way into the `agent_run.log`
//! column — [`run_stage_command`] passes the raw (post-sanitize) output to
//! [`crate::evidence::close_stage`], which caps it before the `UPDATE` ever
//! runs. A pathological `test_cmd` that emits tens of MB before being killed
//! by the timeout will transiently hold all of it in this process's memory;
//! bounding the in-flight buffer too (e.g. a ring buffer that only keeps the
//! last N bytes while streaming) would be the nicer-still version of this,
//! but it is not required by T-520's AC (which only asks that the *stored*
//! log be capped), and a single stage's timeout (default 900s, §2.7) already
//! bounds how long any one run can spend accumulating before something has
//! to give.

use std::path::Path;
use std::time::Duration;

use libsql::Connection;
use tokio::io::AsyncReadExt;
use tokio::process::{Child, Command};
use tokio::time::Instant;

use crate::evidence::{self, CloseStage, OpenStage};

/// Grace period between `SIGTERM` and `SIGKILL` when a timed-out process
/// group doesn't exit on its own. Short — the timeout has already elapsed by
/// the time we get here, so this is strictly extra wall-clock on top of the
/// configured budget; long enough for a well-behaved child to flush and
/// exit, short enough that a stuck one doesn't meaningfully extend the wait.
const KILL_GRACE_PERIOD: Duration = Duration::from_millis(300);

/// Bound on draining any output still buffered in the pipe after the process
/// group has been killed. In the ordinary case the pipe's write end closes
/// (and our read hits EOF) almost immediately once every process in the
/// group is dead; this is a backstop against a pathological case (e.g. a
/// process outside the group somehow still holding the write end open) so a
/// timed-out run can never itself hang forever waiting to report `timeout`.
const POST_KILL_DRAIN_BOUND: Duration = Duration::from_secs(2);

/// Bytes read from the child's stdout at a time.
const READ_CHUNK_SIZE: usize = 32 * 1024;

/// One `sh -c` invocation's raw result: exit code and combined stdout+stderr.
/// `exit_code` is `None` exactly when there is no ordinary exit code to
/// report — the process was killed by [`run_shell_timed`]'s own timeout (a
/// signal death, not a `sh`-reported exit(2)-style code).
#[derive(Debug, Clone)]
pub struct CommandOutput {
    pub exit_code: Option<i32>,
    pub output: String,
}

/// How one [`run_shell_timed`] call ended.
#[derive(Debug)]
pub enum ShellOutcome {
    /// The process (and, transitively, its process group) exited on its own
    /// within the timeout.
    Exited(CommandOutput),
    /// The timeout elapsed first; the whole process group was signaled dead.
    /// `CommandOutput.output` holds whatever combined output had already
    /// been read by the moment the timeout fired (see the module doc's
    /// "in-memory accumulation" note) plus anything drained immediately
    /// after the kill.
    TimedOut(CommandOutput),
    /// `sh` itself could not even be spawned (missing binary, bad `cwd`,
    /// permission error, ...). Distinct from a non-zero exit, which is an
    /// ordinary, expected outcome the caller inspects rather than an I/O
    /// failure.
    SpawnFailed(std::io::Error),
}

/// Run `cmd` via `sh -c` in `cwd`, capturing combined stdout+stderr, killing
/// the whole process group if `timeout` elapses first (D18). See the module
/// doc for why combined capture and process-group kill are done the way they
/// are.
pub async fn run_shell_timed(cmd: &str, cwd: &Path, timeout: Duration) -> ShellOutcome {
    let wrapped = format!("( {cmd} ) 2>&1");

    let mut command = Command::new("sh");
    command
        .arg("-c")
        .arg(&wrapped)
        .current_dir(cwd)
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::piped());

    // New process group, led by `sh` itself, so a timeout can kill the
    // entire tree `sh -c` spawned rather than only `sh` — see the module
    // doc's "why process-group kill" section. `process_group(0)` asks the
    // kernel to make the spawned process its own group leader (pgid ==
    // pid); tokio's `Command` exposes this directly (`#[cfg(unix)]`-gated
    // in tokio itself), so no extra `std::os::unix::process::CommandExt`
    // import is needed.
    #[cfg(unix)]
    command.process_group(0);

    let mut child = match command.spawn() {
        Ok(child) => child,
        Err(err) => return ShellOutcome::SpawnFailed(err),
    };

    // Taken once, immediately after a successful spawn — always `Some` here
    // since we asked for a piped stdout above.
    let mut stdout = child.stdout.take().expect("stdout was requested as piped");

    let deadline = Instant::now() + timeout;
    let mut buf = Vec::new();
    let mut chunk = [0u8; READ_CHUNK_SIZE];

    loop {
        tokio::select! {
            biased;
            read = stdout.read(&mut chunk) => {
                match read {
                    Ok(0) => break, // EOF: the process (group) has exited.
                    Ok(n) => buf.extend_from_slice(&chunk[..n]),
                    Err(_) => break, // pipe error; treat as end of output.
                }
            }
            _ = tokio::time::sleep_until(deadline) => {
                return timed_out(child, stdout, buf, chunk).await;
            }
        }
    }

    // EOF reached before the deadline: the process is done or dying on its
    // own; `wait()` reaps it and gives us its real exit status.
    let status = child.wait().await;
    let exit_code = status.ok().and_then(|s| exit_code_of(&s));
    ShellOutcome::Exited(CommandOutput {
        exit_code,
        output: String::from_utf8_lossy(&buf).into_owned(),
    })
}

/// The timeout fired: kill the whole process group, drain whatever output
/// remains (bounded — see [`POST_KILL_DRAIN_BOUND`]), and reap the child.
async fn timed_out(
    mut child: Child,
    mut stdout: tokio::process::ChildStdout,
    mut buf: Vec<u8>,
    mut chunk: [u8; READ_CHUNK_SIZE],
) -> ShellOutcome {
    kill_process_group(&mut child).await;

    let _ = tokio::time::timeout(POST_KILL_DRAIN_BOUND, async {
        loop {
            match stdout.read(&mut chunk).await {
                Ok(0) | Err(_) => break,
                Ok(n) => buf.extend_from_slice(&chunk[..n]),
            }
        }
    })
    .await;

    // Reap so the killed process doesn't linger as a zombie. Everything in
    // the group should already be dead by now; this just collects the
    // status the kernel already has for us.
    let _ = child.wait().await;

    ShellOutcome::TimedOut(CommandOutput {
        exit_code: None, // signal death, not an ordinary exit code
        output: String::from_utf8_lossy(&buf).into_owned(),
    })
}

/// `SIGTERM` the process group, wait [`KILL_GRACE_PERIOD`] for it to exit on
/// its own, then `SIGKILL` if it hasn't. Unix-only — see the module doc.
#[cfg(unix)]
async fn kill_process_group(child: &mut Child) {
    let Some(pid) = child.id() else {
        // Already reaped by something else; nothing to signal.
        return;
    };
    let pgid = -(pid as libc::pid_t);
    // SAFETY: `libc::kill` is a plain syscall wrapper; `pgid` is a negative
    // pid (POSIX's "signal the whole process group" convention) computed
    // from a pid the OS gave us moments ago via `child.id()`. Passing an
    // invalid/already-reaped pgid is not memory-unsafe — `kill(2)` just
    // returns `ESRCH`, which we ignore (there is nothing left to kill).
    unsafe {
        libc::kill(pgid, libc::SIGTERM);
    }
    if tokio::time::timeout(KILL_GRACE_PERIOD, child.wait())
        .await
        .is_err()
    {
        unsafe {
            libc::kill(pgid, libc::SIGKILL);
        }
    }
}

/// Non-Unix stub kept only so the crate compiles on other targets (kills the
/// direct child, not a process group — see the module doc: Windows is not a
/// supported host).
#[cfg(not(unix))]
async fn kill_process_group(child: &mut Child) {
    let _ = child.start_kill();
}

/// `ExitStatus::code()`, kept as its own function so a future switch to
/// inspecting `ExitStatusExt::signal()` on Unix has one call site to change.
fn exit_code_of(status: &std::process::ExitStatus) -> Option<i32> {
    status.code()
}

// ---- the stage-level runner: agent_run row + skip semantics ---------------

/// Which stage/attempt a [`run_stage_command`] call writes its `agent_run`
/// row under. Mirrors [`evidence::OpenStage`] — a thin, cmd.rs-local copy
/// rather than reusing it directly because this struct also carries the
/// command's own inputs (`cwd`, `timeout`), which `OpenStage` has no reason
/// to know about.
pub struct StageCommand<'a> {
    pub task_id: Option<&'a str>,
    pub epic_id: Option<&'a str>,
    /// The §2.2 vocabulary string (`setup` | `preflight` | `test_gate`).
    pub stage: &'a str,
    /// `test_gate`'s retries pass `0..N`; `setup`/`preflight` always pass `0`
    /// or `1` (whatever the caller's own convention is — this module does
    /// not interpret the value, only stores it).
    pub attempt: i64,
    pub cwd: &'a Path,
    pub timeout: Duration,
}

/// A [`run_stage_command`] call's outcome. Deliberately **not** a status
/// enum/string with a "skipped" variant sitting alongside "ok"/"error" —
/// §5's contract is that an absent `test_cmd`/`setup_cmd` records **nothing**
/// (no `agent_run` row at all), which is a structurally different thing from
/// "a row was written recording that nothing happened." Splitting the return
/// type into `Skipped` (no row, ever) vs. `Ran` (a row exists, look at its
/// `status`) makes that distinction something the caller's `match` has to
/// handle rather than a status string it could accidentally treat the same
/// as `"ok"`.
#[derive(Debug)]
pub enum StageOutcome {
    /// `cmd` was `None`, or blank after trimming (§5 treats the two the
    /// same). No `agent_run` row was opened or written.
    Skipped,
    /// A command was spawned and reached some terminal state. An
    /// `agent_run` row exists for it (see [`RanCommand::run_id`]) no matter
    /// which of `ok`/`error`/`timeout` it ended in, or whether it never
    /// spawned at all — see [`run_stage_command`]'s use of
    /// [`evidence::guard_stage_close`] for the "always closes" guarantee.
    Ran(RanCommand),
}

impl StageOutcome {
    /// `true` only for [`StageOutcome::Ran`] whose row closed `status =
    /// "ok"`. `Skipped` is neither a pass nor a fail — callers that need to
    /// tell "no gate configured" apart from "gate passed" must match on the
    /// enum directly rather than call this and lose that distinction.
    pub fn passed(&self) -> bool {
        matches!(self, StageOutcome::Ran(ran) if ran.status == "ok")
    }
}

/// The result of a command that actually ran, and the `agent_run` row it was
/// recorded under.
#[derive(Debug, Clone)]
pub struct RanCommand {
    /// `agent_run.id` for this run — lets a caller fetch the full row later
    /// (`GET /runs/{id}`) without a second query.
    pub run_id: String,
    /// `ok` | `error` | `timeout` (§2.1's `agent_run.status` vocabulary).
    pub status: &'static str,
    /// `None` only when the command could not be spawned, or when it was
    /// killed by the timeout (a signal death carries no ordinary exit code).
    pub exit_code: Option<i32>,
    /// Combined stdout+stderr, after the caller's `sanitize` transform —
    /// the same text written to the row's `log` (modulo D13 capping, which
    /// [`evidence::close_stage`] applies on the way into the database; this
    /// field is **not** itself capped, matching `CommandOutput.output` from
    /// the pre-T-520 API — see the module doc's "in-memory accumulation" note).
    pub output: String,
}

/// Run `cmd` (`setup_cmd`/`test_cmd`, but genuinely any shell command a
/// non-agent stage needs) as one `agent_run` row for `req.stage`/`req.attempt`,
/// or skip entirely if `cmd` is absent — §5's `test_cmd IS NULL` /
/// `setup_cmd IS NULL` contract, expressed once here so `preflight` (T-521)
/// and `test_gate` (T-522) can't independently get the "skip means no row"
/// rule wrong.
///
/// `sanitize` is applied to the raw combined output before it is stored or
/// returned — the hook a caller with secrets to strip (`setup_cmd` echoing a
/// project's PAT, say — see [`crate::git::redact`]) uses; callers with
/// nothing to redact pass `|s: &str| s.to_string()`.
///
/// The `agent_run` row opens (via [`evidence::open_stage`]) before the
/// command is even spawned and closes on **every** exit path — normal
/// completion, non-zero exit, timeout, or a spawn failure — via
/// [`evidence::guard_stage_close`], so a caller can never observe a stage
/// stuck `running` because of a branch this function forgot to close.
pub async fn run_stage_command(
    conn: &Connection,
    req: StageCommand<'_>,
    cmd: Option<&str>,
    sanitize: impl Fn(&str) -> String,
) -> Result<StageOutcome, libsql::Error> {
    let Some(cmd) = cmd.map(str::trim).filter(|c| !c.is_empty()) else {
        return Ok(StageOutcome::Skipped);
    };

    let handle = evidence::open_stage(
        conn,
        OpenStage {
            task_id: req.task_id,
            epic_id: req.epic_id,
            stage: req.stage,
            attempt: req.attempt,
        },
    )
    .await?;
    let run_id = handle.id.clone();
    let cwd = req.cwd;
    let timeout = req.timeout;

    let ran: RanCommand = evidence::guard_stage_close(conn, &handle, move || async move {
        let (status, exit_code, raw_output) = match run_shell_timed(cmd, cwd, timeout).await {
            ShellOutcome::Exited(out) => (
                if out.exit_code == Some(0) { "ok" } else { "error" },
                out.exit_code,
                out.output,
            ),
            ShellOutcome::TimedOut(out) => ("timeout", out.exit_code, out.output),
            ShellOutcome::SpawnFailed(err) => {
                ("error", None, format!("failed to spawn command: {err}"))
            }
        };
        let log = sanitize(&raw_output);
        let ran = RanCommand {
            run_id,
            status,
            exit_code,
            output: log.clone(),
        };
        Ok::<_, std::convert::Infallible>((
            ran,
            CloseStage {
                status,
                session_id: None,
                verdict: None,
                exit_code,
                log,
            },
        ))
    })
    .await
    .unwrap(); // body above never returns `Err` (`Infallible`).

    Ok(StageOutcome::Ran(ran))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_dir(name: &str) -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "dearborn-cmd-test-{name}-{}-{}",
            std::process::id(),
            ulid::Ulid::new()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn identity(s: &str) -> String {
        s.to_string()
    }

    // ---- run_shell_timed: exit code, combined capture, cwd ----------------

    #[tokio::test]
    async fn captures_stdout_and_exit_code_zero() {
        let dir = temp_dir("ok");
        let out = run_shell_timed("echo hello", &dir, Duration::from_secs(5)).await;
        match out {
            ShellOutcome::Exited(out) => {
                assert_eq!(out.exit_code, Some(0));
                assert!(out.output.contains("hello"));
            }
            other => panic!("expected Exited, got {other:?}"),
        }
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn captures_nonzero_exit_code() {
        let dir = temp_dir("fail");
        let out = run_shell_timed("exit 7", &dir, Duration::from_secs(5)).await;
        match out {
            ShellOutcome::Exited(out) => assert_eq!(out.exit_code, Some(7)),
            other => panic!("expected Exited, got {other:?}"),
        }
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn interleaves_stdout_and_stderr_in_execution_order() {
        let dir = temp_dir("interleave");
        let out = run_shell_timed(
            "echo one; echo two 1>&2; echo three",
            &dir,
            Duration::from_secs(5),
        )
        .await;
        let out = match out {
            ShellOutcome::Exited(out) => out,
            other => panic!("expected Exited, got {other:?}"),
        };
        assert_eq!(out.exit_code, Some(0));
        let one = out.output.find("one").unwrap();
        let two = out.output.find("two").unwrap();
        let three = out.output.find("three").unwrap();
        assert!(
            one < two && two < three,
            "output not in execution order: {}",
            out.output
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn runs_in_the_given_cwd() {
        let dir = temp_dir("cwd");
        std::fs::write(dir.join("marker.txt"), "hi").unwrap();
        let out = run_shell_timed("cat marker.txt", &dir, Duration::from_secs(5)).await;
        match out {
            ShellOutcome::Exited(out) => assert!(out.output.contains("hi")),
            other => panic!("expected Exited, got {other:?}"),
        }
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn spawn_failure_is_reported_distinctly() {
        // A `cwd` that does not exist makes even `sh` itself fail to spawn.
        let dir = temp_dir("spawn-fail-parent").join("does-not-exist");
        let out = run_shell_timed("echo hi", &dir, Duration::from_secs(5)).await;
        match out {
            ShellOutcome::SpawnFailed(_) => {}
            other => panic!("expected SpawnFailed, got {other:?}"),
        }
    }

    // ---- run_shell_timed: timeout + process-group kill ---------------------

    #[tokio::test]
    async fn a_command_exceeding_the_timeout_is_reported_as_timed_out() {
        let dir = temp_dir("timeout");
        let out = run_shell_timed("sleep 30", &dir, Duration::from_millis(100)).await;
        match out {
            ShellOutcome::TimedOut(out) => {
                assert_eq!(out.exit_code, None, "a signal death carries no exit code");
            }
            other => panic!("expected TimedOut, got {other:?}"),
        }
        std::fs::remove_dir_all(&dir).ok();
    }

    /// The canonical proof that the *whole process group* dies, not just the
    /// `sh` parent: the script backgrounds a grandchild (`sleep 30 &`) that
    /// would keep running long after `sh` itself exits if only `sh` were
    /// killed, then records that grandchild's pid to a file. After the
    /// timeout, we assert the grandchild is actually dead via `kill -0`
    /// against the recorded pid — not merely that `run_shell_timed` returned
    /// in time, which would also be true of a strategy that killed only
    /// `sh` and orphaned the grandchild.
    ///
    /// This is about as deterministic as a real-process test gets: `kill -0`
    /// against a *specific* recorded pid, polled for up to a couple of
    /// seconds to absorb scheduler jitter between `SIGKILL` and the kernel
    /// actually reaping the process, rather than a bare "did the runner
    /// return" check.
    #[cfg(unix)]
    #[tokio::test]
    async fn timeout_kills_the_whole_process_group_including_a_backgrounded_grandchild() {
        let dir = temp_dir("pgroup-kill");
        let pid_file = dir.join("grandchild.pid");
        let script = format!(
            "sleep 30 & echo $! > {} ; sleep 30",
            pid_file.to_string_lossy()
        );

        let out = run_shell_timed(&script, &dir, Duration::from_millis(200)).await;
        assert!(
            matches!(out, ShellOutcome::TimedOut(_)),
            "expected TimedOut, got {out:?}"
        );

        // The pid file write races the kill signal only slightly (it happens
        // essentially immediately after the backgrounded `sleep 30 &`
        // returns) — give it a brief moment to have landed on disk.
        let mut waited = Duration::ZERO;
        while !pid_file.exists() && waited < Duration::from_secs(2) {
            tokio::time::sleep(Duration::from_millis(20)).await;
            waited += Duration::from_millis(20);
        }
        let grandchild_pid: i32 = std::fs::read_to_string(&pid_file)
            .expect("grandchild pid file must have been written before the timeout killed it")
            .trim()
            .parse()
            .expect("pid file must contain a plain pid");

        // Poll `kill -0` (signal 0: no-op, only checks existence/permission)
        // against the recorded pid for a couple of seconds — long enough to
        // absorb the gap between SIGKILL delivery and the kernel finishing
        // teardown, short enough that a real leak fails the test rather than
        // hanging it.
        let mut dead = false;
        let mut waited = Duration::ZERO;
        while waited < Duration::from_secs(3) {
            let still_alive = unsafe { libc::kill(grandchild_pid, 0) } == 0;
            if !still_alive {
                dead = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
            waited += Duration::from_millis(50);
        }
        assert!(
            dead,
            "grandchild pid {grandchild_pid} must be dead after the process-group kill"
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    // ---- run_stage_command: skip semantics, evidence rows ------------------

    use crate::Db;

    async fn seeded_db() -> Db {
        let db = Db::connect(":memory:").await.unwrap();
        db.run_migrations().await.unwrap();
        let conn = db.conn();
        conn.execute(
            "INSERT INTO project (id, name, repo_url, clone_status, created_at, updated_at) \
             VALUES ('proj-1', 'P', 'https://example.com/p.git', 'ready', 0, 0)",
            (),
        )
        .await
        .unwrap();
        conn.execute(
            "INSERT INTO epic (id, project_id, title, status, created_at, updated_at) \
             VALUES ('epic-1', 'proj-1', 'E', 'InProgress', 0, 0)",
            (),
        )
        .await
        .unwrap();
        conn.execute(
            "INSERT INTO task (id, epic_id, project_id, title, status, position, created_at, updated_at) \
             VALUES ('task-1', 'epic-1', 'proj-1', 'T', 'InProgress', 1, 0, 0)",
            (),
        )
        .await
        .unwrap();
        db
    }

    async fn agent_run_count(db: &Db, epic_id: &str) -> i64 {
        let mut rows = db
            .conn()
            .query(
                "SELECT COUNT(*) FROM agent_run WHERE epic_id = ?1",
                libsql::params![epic_id],
            )
            .await
            .unwrap();
        rows.next().await.unwrap().unwrap().get(0).unwrap()
    }

    #[tokio::test]
    async fn none_command_is_skipped_and_writes_no_row() {
        let db = seeded_db().await;
        let dir = temp_dir("skip-none");

        let outcome = run_stage_command(
            db.conn(),
            StageCommand {
                task_id: None,
                epic_id: Some("epic-1"),
                stage: "preflight",
                attempt: 0,
                cwd: &dir,
                timeout: Duration::from_secs(5),
            },
            None,
            identity,
        )
        .await
        .unwrap();

        assert!(matches!(outcome, StageOutcome::Skipped));
        assert!(!outcome.passed(), "Skipped must not read as passed");
        assert_eq!(
            agent_run_count(&db, "epic-1").await,
            0,
            "an absent command must record zero agent_run rows"
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn blank_command_is_treated_the_same_as_none() {
        let db = seeded_db().await;
        let dir = temp_dir("skip-blank");

        let outcome = run_stage_command(
            db.conn(),
            StageCommand {
                task_id: None,
                epic_id: Some("epic-1"),
                stage: "setup",
                attempt: 0,
                cwd: &dir,
                timeout: Duration::from_secs(5),
            },
            Some("   "),
            identity,
        )
        .await
        .unwrap();

        assert!(matches!(outcome, StageOutcome::Skipped));
        assert_eq!(agent_run_count(&db, "epic-1").await, 0);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn a_passing_command_writes_an_ok_row() {
        let db = seeded_db().await;
        let dir = temp_dir("gate-ok");

        let outcome = run_stage_command(
            db.conn(),
            StageCommand {
                task_id: Some("task-1"),
                epic_id: Some("epic-1"),
                stage: "test_gate",
                attempt: 0,
                cwd: &dir,
                timeout: Duration::from_secs(5),
            },
            Some("echo all good"),
            identity,
        )
        .await
        .unwrap();

        let ran = match outcome {
            StageOutcome::Ran(ran) => ran,
            other => panic!("expected Ran, got {other:?}"),
        };
        assert_eq!(ran.status, "ok");
        assert_eq!(ran.exit_code, Some(0));
        assert!(ran.output.contains("all good"));

        let row = evidence::fetch_run_detail(db.conn(), &ran.run_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(row.summary.stage, "test_gate");
        assert_eq!(row.summary.task_id.as_deref(), Some("task-1"));
        assert_eq!(row.summary.epic_id.as_deref(), Some("epic-1"));
        assert_eq!(row.summary.session_id, None, "non-agent stage: no session");
        assert_eq!(row.summary.status, "ok");
        assert_eq!(row.summary.exit_code, Some(0));
        assert!(row.log.contains("all good"));
        assert!(row.summary.started_at.is_some());
        assert!(row.summary.ended_at.is_some());
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn a_failing_command_writes_an_error_row_with_output_retained() {
        let db = seeded_db().await;
        let dir = temp_dir("gate-error");

        let outcome = run_stage_command(
            db.conn(),
            StageCommand {
                task_id: Some("task-1"),
                epic_id: Some("epic-1"),
                stage: "test_gate",
                attempt: 2,
                cwd: &dir,
                timeout: Duration::from_secs(5),
            },
            Some("echo it broke; exit 1"),
            identity,
        )
        .await
        .unwrap();

        let ran = match outcome {
            StageOutcome::Ran(ran) => ran,
            other => panic!("expected Ran, got {other:?}"),
        };
        assert_eq!(ran.status, "error");
        assert_eq!(ran.exit_code, Some(1));
        assert!(ran.output.contains("it broke"));

        let row = evidence::fetch_run_detail(db.conn(), &ran.run_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(row.summary.attempt, 2);
        assert_eq!(row.summary.status, "error");
        assert!(row.log.contains("it broke"));
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn a_timed_out_command_writes_a_timeout_row() {
        let db = seeded_db().await;
        let dir = temp_dir("gate-timeout");

        let outcome = run_stage_command(
            db.conn(),
            StageCommand {
                task_id: Some("task-1"),
                epic_id: Some("epic-1"),
                stage: "test_gate",
                attempt: 0,
                cwd: &dir,
                timeout: Duration::from_millis(150),
            },
            Some("sleep 30"),
            identity,
        )
        .await
        .unwrap();

        let ran = match outcome {
            StageOutcome::Ran(ran) => ran,
            other => panic!("expected Ran, got {other:?}"),
        };
        assert_eq!(ran.status, "timeout");
        assert_eq!(ran.exit_code, None);

        let row = evidence::fetch_run_detail(db.conn(), &ran.run_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(row.summary.status, "timeout");
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn a_spawn_failure_still_closes_its_row_with_a_terminal_status() {
        let db = seeded_db().await;
        // A cwd that does not exist forces the spawn itself to fail.
        let dir = temp_dir("gate-spawn-fail-parent").join("missing");

        let outcome = run_stage_command(
            db.conn(),
            StageCommand {
                task_id: Some("task-1"),
                epic_id: Some("epic-1"),
                stage: "preflight",
                attempt: 0,
                cwd: &dir,
                timeout: Duration::from_secs(5),
            },
            Some("echo hi"),
            identity,
        )
        .await
        .unwrap();

        let ran = match outcome {
            StageOutcome::Ran(ran) => ran,
            other => panic!("expected Ran, got {other:?}"),
        };
        assert_eq!(ran.status, "error");
        assert_eq!(ran.exit_code, None);

        let row = evidence::fetch_run_detail(db.conn(), &ran.run_id)
            .await
            .unwrap()
            .unwrap();
        assert_ne!(row.summary.status, "running", "must not be left running");
        assert!(row.summary.ended_at.is_some());
    }

    #[tokio::test]
    async fn sanitize_hook_redacts_before_storage_and_before_returning() {
        let db = seeded_db().await;
        let dir = temp_dir("gate-sanitize");
        let secret = "s3cr3t-token";

        let outcome = run_stage_command(
            db.conn(),
            StageCommand {
                task_id: None,
                epic_id: Some("epic-1"),
                stage: "setup",
                attempt: 0,
                cwd: &dir,
                timeout: Duration::from_secs(5),
            },
            Some(&format!("echo {secret} && exit 1")),
            |s: &str| s.replace(secret, "***"),
        )
        .await
        .unwrap();

        let ran = match outcome {
            StageOutcome::Ran(ran) => ran,
            other => panic!("expected Ran, got {other:?}"),
        };
        assert!(!ran.output.contains(secret));
        assert!(ran.output.contains("***"));

        let row = evidence::fetch_run_detail(db.conn(), &ran.run_id)
            .await
            .unwrap()
            .unwrap();
        assert!(!row.log.contains(secret), "log must not contain the secret");
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn stage_outcome_passed_is_false_for_skipped_and_non_ok() {
        assert!(!StageOutcome::Skipped.passed());
        assert!(!StageOutcome::Ran(RanCommand {
            run_id: "x".to_string(),
            status: "error",
            exit_code: Some(1),
            output: String::new(),
        })
        .passed());
        assert!(StageOutcome::Ran(RanCommand {
            run_id: "x".to_string(),
            status: "ok",
            exit_code: Some(0),
            output: String::new(),
        })
        .passed());
    }
}
