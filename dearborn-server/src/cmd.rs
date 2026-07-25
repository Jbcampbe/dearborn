//! Minimal shell-command runner (T-511's slice; **T-520 thickens this**).
//!
//! This module is deliberately thin: `sh -c <cmd>` in a directory, combined
//! stdout+stderr capture, exit code. T-511 needs exactly this much to run a
//! project's `setup_cmd` in a freshly provisioned epic workspace. **Do not**
//! add more here — `DEARBORN_CMD_TIMEOUT_SECS`, process-group kill (so a
//! runaway child that forks its own children is fully reaped, not just its
//! immediate `sh`), and output capping (the ~256 KB head+tail policy, §2.1)
//! are T-520's job, once `test_cmd` needs the same runner under real time
//! pressure. Landing them here now would duplicate what T-520 is scoped to
//! build.
//!
//! ## Combined output
//!
//! Reading a child's stdout and stderr as two separate pipes does not
//! preserve their relative interleaving (the OS makes no ordering guarantee
//! across two independent pipes). Instead, the caller's command is wrapped as
//! `( <cmd> ) 2>&1` and handed to `sh -c` as a single string: the shell
//! itself merges stderr into stdout *before* either reaches us, so the
//! captured bytes are in true execution order — a `setup_cmd` maintainer
//! reading the log later sees output and errors interleaved the way they
//! actually happened, not one after the other.

use std::path::Path;

use tokio::process::Command;

/// The result of running a shell command: its exit code and combined
/// stdout+stderr.
#[derive(Debug, Clone)]
pub struct CommandOutput {
    /// The process's exit code. `sh` always reports one (even for a signal
    /// death it maps to 128+signal), so this is never absent in practice;
    /// [`run_shell`] only returns `Err` for an I/O failure to even spawn `sh`.
    pub exit_code: i32,
    /// Combined stdout+stderr, uncapped (T-520 adds the cap).
    pub output: String,
}

/// Run `cmd` via `sh -c` in `cwd`, capturing combined stdout+stderr. Returns
/// `Err` only if `sh` itself could not be spawned (missing binary, bad `cwd`,
/// etc.) — a non-zero exit from the command itself is `Ok` with `exit_code`
/// set accordingly, since "the command failed" is an ordinary, expected
/// outcome the caller inspects, not an I/O error.
pub async fn run_shell(cmd: &str, cwd: &Path) -> std::io::Result<CommandOutput> {
    let wrapped = format!("( {cmd} ) 2>&1");
    let output = Command::new("sh")
        .arg("-c")
        .arg(&wrapped)
        .current_dir(cwd)
        .output()
        .await?;

    // Merged into stdout by the `2>&1` wrapper above; stderr is always empty.
    let combined = String::from_utf8_lossy(&output.stdout).into_owned();
    let exit_code = output.status.code().unwrap_or(-1);
    Ok(CommandOutput {
        exit_code,
        output: combined,
    })
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

    #[tokio::test]
    async fn captures_stdout_and_exit_code_zero() {
        let dir = temp_dir("ok");
        let out = run_shell("echo hello", &dir).await.unwrap();
        assert_eq!(out.exit_code, 0);
        assert!(out.output.contains("hello"));
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn captures_nonzero_exit_code() {
        let dir = temp_dir("fail");
        let out = run_shell("exit 7", &dir).await.unwrap();
        assert_eq!(out.exit_code, 7);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn interleaves_stdout_and_stderr_in_execution_order() {
        let dir = temp_dir("interleave");
        let out = run_shell("echo one; echo two 1>&2; echo three", &dir)
            .await
            .unwrap();
        assert_eq!(out.exit_code, 0);
        let one = out.output.find("one").unwrap();
        let two = out.output.find("two").unwrap();
        let three = out.output.find("three").unwrap();
        assert!(one < two && two < three, "output not in execution order: {}", out.output);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn runs_in_the_given_cwd() {
        let dir = temp_dir("cwd");
        std::fs::write(dir.join("marker.txt"), "hi").unwrap();
        let out = run_shell("cat marker.txt", &dir).await.unwrap();
        assert_eq!(out.exit_code, 0);
        assert!(out.output.contains("hi"));
        std::fs::remove_dir_all(&dir).ok();
    }
}
