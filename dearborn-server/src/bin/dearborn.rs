//! The `dearborn` agent CLI binary — argument parsing + output contract around
//! [`dearborn_server::cli::CliClient`].
//!
//! Shape (this is the exact string the breakdown agent's access block issues):
//!
//! ```text
//! dearborn --url <base> --token <cap> <verb> [flags]
//! ```
//!
//! Verbs:
//!
//! - `task create --title "..." [--description "..."] [--acceptance "..."] [--blocks id1,id2]`
//! - `task link BLOCKER BLOCKED`
//! - `dag`
//! - `scope`
//!
//! Every verb prints the API's JSON to stdout on success (exit 0). Any failure
//! — transport, HTTP, or usage — prints `dearborn: <error>` to stderr and
//! exits non-zero, so a failing call is unmistakable to the harness that ran
//! it (breakdown's DAG-write guard greps run output for exactly that marker).

use dearborn_server::cli::{CliClient, ERROR_PREFIX};

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let code = match run(args) {
        Ok(()) => 0,
        Err(code) => code,
    };
    std::process::exit(code);
}

/// Parse argv, run the verb, print the result. Returns the process exit code:
/// 0 on success, 1 on a runtime (API/transport) failure, 2 on a usage error.
fn run(args: Vec<String>) -> Result<(), i32> {
    let mut args = args.as_slice();
    let mut base_url: Option<String> = None;
    let mut token: Option<String> = None;

    // Leading global flags, in either `--flag value` or `--flag=value` form.
    while let Some(flag) = args.first() {
        let (name, inline_value) = match flag.split_once('=') {
            Some((name, value)) => (name, Some(value.to_string())),
            None => (flag.as_str(), None),
        };
        match name {
            "--url" => {
                base_url = Some(take_value(&mut args, inline_value, "--url")?);
            }
            "--token" => {
                token = Some(take_value(&mut args, inline_value, "--token")?);
            }
            _ => break,
        }
    }

    let (base_url, token) = match (base_url, token) {
        (Some(url), Some(token)) => (url, token),
        _ => return usage("both --url and --token are required"),
    };

    let verb = match args.first() {
        Some(verb) => verb.as_str(),
        None => return usage("expected a verb: task create | task link | dag | scope"),
    };
    args = &args[1..];

    match verb {
        "scope" => {
            if !args.is_empty() {
                return usage("`scope` takes no flags");
            }
            let client = client(&base_url, &token)?;
            block_on(async move {
                let scope = client.scope().await?;
                println!("{}", serde_json::to_string(&scope).expect("scope is JSON"));
                Ok(())
            })
        }
        "dag" => {
            if !args.is_empty() {
                return usage("`dag` takes no flags");
            }
            let client = client(&base_url, &token)?;
            block_on(async move {
                let dag = client.dag().await?;
                println!("{}", serde_json::to_string(&dag).expect("dag is JSON"));
                Ok(())
            })
        }
        "task" => {
            let sub = match args.first() {
                Some(sub) => sub.as_str(),
                None => return usage("expected `task create` or `task link`"),
            };
            args = &args[1..];
            match sub {
                "create" => {
                    let (title, description, acceptance, blocks) = task_create_flags(args)?;
                    let client = client(&base_url, &token)?;
                    block_on(async move {
                        let task = client
                            .task_create(&title, description.as_deref(), acceptance.as_deref(), &blocks)
                            .await?;
                        println!("{}", serde_json::to_string(&task).expect("task is JSON"));
                        Ok(())
                    })
                }
                "link" => {
                    let (blocker, blocked) = match args {
                        [a, b] => (a.clone(), b.clone()),
                        _ => return usage("expected `task link BLOCKER BLOCKED`"),
                    };
                    let client = client(&base_url, &token)?;
                    block_on(async move {
                        let edge = client.task_link(&blocker, &blocked).await?;
                        println!("{}", serde_json::to_string(&edge).expect("edge is JSON"));
                        Ok(())
                    })
                }
                other => usage(&format!("unknown task verb `{other}` (expected create or link)")),
            }
        }
        other => usage(&format!(
            "unknown verb `{other}` (expected: task create | task link | dag | scope)"
        )),
    }
}

/// Drive a verb's future to completion, mapping a [`dearborn_server::cli::CliError`]
/// to the `dearborn: <error>` stderr line + exit code 1.
fn block_on(fut: impl std::future::Future<Output = Result<(), dearborn_server::cli::CliError>>) -> Result<(), i32> {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|err| {
            eprintln!("{ERROR_PREFIX}failed to start the async runtime: {err}");
            1
        })?;
    rt.block_on(fut).map_err(|err| {
        eprintln!("{ERROR_PREFIX}{err}");
        1
    })
}

fn client(base_url: &str, token: &str) -> Result<CliClient, i32> {
    CliClient::new(base_url, token).map_err(|err| {
        eprintln!("{ERROR_PREFIX}{err}");
        1
    })
}

/// Consume a flag's value from argv: either the inline `--flag=value` remainder
/// or the next argument.
fn take_value(args: &mut &[String], inline: Option<String>, flag: &str) -> Result<String, i32> {
    if let Some(value) = inline {
        *args = &args[1..];
        return Ok(value);
    }
    if args.len() < 2 {
        return usage(&format!("{flag} requires a value"));
    }
    let value = args[1].clone();
    *args = &args[2..];
    Ok(value)
}

/// The parsed `task create` flag set: `(--title, --description, --acceptance, --blocks)`.
type TaskCreateFlags = (String, Option<String>, Option<String>, Vec<String>);

/// `task create` flags: `--title` (required), `--description`, `--acceptance`
/// (optional), `--blocks id1,id2` (optional, comma-separated task ids). Flags
/// may be `--flag value` or `--flag=value`.
fn task_create_flags(args: &[String]) -> Result<TaskCreateFlags, i32> {
    let mut title: Option<String> = None;
    let mut description: Option<String> = None;
    let mut acceptance: Option<String> = None;
    let mut blocks: Vec<String> = Vec::new();

    let mut i = 0;
    while i < args.len() {
        let (name, inline) = match args[i].split_once('=') {
            Some((name, value)) => (name, Some(value.to_string())),
            None => (args[i].as_str(), None),
        };
        // The flag's value: the inline remainder, else the next argv slot.
        let take = || -> Result<String, i32> {
            if let Some(value) = inline.clone() {
                return Ok(value);
            }
            if i + 1 >= args.len() {
                return usage(&format!("{name} requires a value"));
            }
            Ok(args[i + 1].clone())
        };
        match name {
            "--title" => title = Some(take()?),
            "--description" => description = Some(take()?),
            "--acceptance" => acceptance = Some(take()?),
            "--blocks" => {
                let raw = take()?;
                blocks = raw
                    .split(',')
                    .map(str::trim)
                    .filter(|s| !s.is_empty())
                    .map(str::to_string)
                    .collect();
                if blocks.is_empty() {
                    return usage("--blocks requires a comma-separated id list");
                }
            }
            other => return usage(&format!("unknown task create flag `{other}`")),
        }
        i += if inline.is_some() { 1 } else { 2 };
    }

    match title {
        Some(title) if !title.trim().is_empty() => Ok((title, description, acceptance, blocks)),
        _ => usage("--title is required and must not be empty"),
    }
}

/// Print a usage error with the CLI error marker (exit code 2).
fn usage<T>(message: &str) -> Result<T, i32> {
    eprintln!(
        "{ERROR_PREFIX}{message}
usage: dearborn --url <base> --token <cap> <verb>
  task create --title \"...\" [--description \"...\"] [--acceptance \"...\"] [--blocks id1,id2]
  task link BLOCKER BLOCKED
  dag
  scope"
    );
    Err(2)
}
