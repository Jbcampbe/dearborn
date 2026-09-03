//! The `dearborn` agent CLI binary — argument parsing + output contract around
//! [`dearborn_server::cli::CliClient`].
//!
//! Shape (this is the exact string an agent's access block issues):
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
//! - `node create --kind grilling|research|prototype|task --title "..."`
//!   `[--question "..."] [--task-mode afk|hitl] [--blocked-by id1,id2] [--blocks id1,id2]`
//! - `node link BLOCKER BLOCKED`
//! - `node resolve NODE [--gist "..."]`
//! - `map` — print the epic's full planning map
//! - `map set-destination|set-notes|set-fog|set-out-of-scope "TEXT"`
//! - `document pull [PATH]` — write the epic's living HTML document to a
//!   scratch workspace file (default `./document.html`) for editing with the
//!   harness's native file tools; prints `{ "path", "version", ... }` where
//!   `version` is the base version the sync must carry
//! - `document sync PATH --base-version N [--node NODE_ID]` — commit the
//!   edited scratch file as a new document version (per-epic write semaphore,
//!   base-version check, section index, `document_updated` WS frame); a stale
//!   base version exits non-zero naming the current version — re-pull and retry
//! - `scope`
//!
//! Every verb prints the API's JSON to stdout on success (exit 0). Any failure
//! — transport, HTTP, or usage — prints `dearborn: <error>` to stderr and
//! exits non-zero, so a failing call is unmistakable to the harness that ran
//! it (breakdown's DAG-write guard greps run output for exactly that marker).

use dearborn_server::cli::{CliClient, ERROR_PREFIX};

use std::path::Path;

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
        None => return usage("expected a verb: task create | task link | dag | node create | node link | node resolve | map | map set-* | document pull | document sync | scope"),
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
        "map" => {
            if args.is_empty() {
                // Bare `map`: query the epic's full planning map.
                let client = client(&base_url, &token)?;
                return block_on(async move {
                    let map = client.map().await?;
                    println!("{}", serde_json::to_string(&map).expect("map is JSON"));
                    Ok(())
                });
            }
            // `map set-destination|set-notes|set-fog|set-out-of-scope "TEXT"`
            let sub = args[0].as_str();
            let field = match sub {
                "set-destination" => "destination",
                "set-notes" => "notes",
                "set-fog" => "not_yet_specified",
                "set-out-of-scope" => "out_of_scope",
                other => {
                    return usage(&format!(
                        "unknown map verb `{other}` (expected: set-destination | set-notes | set-fog | set-out-of-scope)"
                    ))
                }
            };
            let text = match args[1..].first() {
                Some(text) if args.len() == 2 => text.clone(),
                _ => return usage(&format!("expected `map {sub} \"TEXT\"`")),
            };
            let client = client(&base_url, &token)?;
            block_on(async move {
                let map = client.map_set_prose(field, &text).await?;
                println!("{}", serde_json::to_string(&map).expect("map is JSON"));
                Ok(())
            })
        }
        "task" | "node" => {
            let sub = match args.first() {
                Some(sub) => sub.as_str(),
                None => return usage("expected a `task` or `node` sub-verb (create | link, node also resolve)"),
            };
            args = &args[1..];
            match (verb, sub) {
                ("task", "create") => {
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
                ("task", "link") => {
                    let (blocker, blocked) = positional_pair(args, "task link BLOCKER BLOCKED")?;
                    let client = client(&base_url, &token)?;
                    block_on(async move {
                        let edge = client.task_link(&blocker, &blocked).await?;
                        println!("{}", serde_json::to_string(&edge).expect("edge is JSON"));
                        Ok(())
                    })
                }
                ("node", "create") => {
                    let (kind, title, question, task_mode, blocked_by, blocks) =
                        node_create_flags(args)?;
                    let client = client(&base_url, &token)?;
                    block_on(async move {
                        let node = client
                            .node_create(
                                &kind,
                                &title,
                                question.as_deref(),
                                task_mode.as_deref(),
                                &blocked_by,
                                &blocks,
                            )
                            .await?;
                        println!("{}", serde_json::to_string(&node).expect("node is JSON"));
                        Ok(())
                    })
                }
                ("node", "link") => {
                    let (blocker, blocked) = positional_pair(args, "node link BLOCKER BLOCKED")?;
                    let client = client(&base_url, &token)?;
                    block_on(async move {
                        let edge = client.node_link(&blocker, &blocked).await?;
                        println!("{}", serde_json::to_string(&edge).expect("edge is JSON"));
                        Ok(())
                    })
                }
                ("node", "resolve") => {
                    let (node_id, gist) = node_resolve_args(args)?;
                    let client = client(&base_url, &token)?;
                    block_on(async move {
                        let node = client.node_resolve(&node_id, gist.as_deref()).await?;
                        println!("{}", serde_json::to_string(&node).expect("node is JSON"));
                        Ok(())
                    })
                }
                (_, other) => usage(&format!(
                    "unknown {verb} verb `{other}` (expected create or link; node also resolve)"
                )),
            }
        }
        "document" => {
            let sub = match args.first() {
                Some(sub) => sub.as_str(),
                None => return usage("expected a `document` sub-verb (pull | sync)"),
            };
            args = &args[1..];
            match sub {
                "pull" => document_pull(args, &base_url, &token),
                "sync" => {
                    let (path, base_version, node) = document_sync_flags(args)?;
                    let client = client(&base_url, &token)?;
                    block_on(async move {
                        let synced = client
                            .document_sync_file(Path::new(&path), base_version, node.as_deref())
                            .await?;
                        println!("{}", serde_json::to_string(&synced).expect("sync result is JSON"));
                        Ok(())
                    })
                }
                other => usage(&format!(
                    "unknown document verb `{other}` (expected: pull | sync)"
                )),
            }
        }
        other => usage(&format!(
            "unknown verb `{other}` (expected: task create | task link | dag | node create | node link | node resolve | map | map set-* | document pull | document sync | scope)"
        )),
    }
}

/// `document pull [PATH]` — optional positional scratch-file path (default
/// `./document.html`); writes the epic's HTML document to it and prints the
/// pulled state (path + base version) as JSON.
fn document_pull(args: &[String], base_url: &str, token: &str) -> Result<(), i32> {
    if args.len() > 1 {
        return usage("expected `document pull [PATH]` (one optional path)");
    }
    let path = args
        .first()
        .cloned()
        .unwrap_or_else(|| "document.html".to_string());
    let client = client(base_url, token)?;
    block_on(async move {
        let pulled = client.document_pull(Path::new(&path)).await?;
        println!("{}", serde_json::to_string(&pulled).expect("pull result is JSON"));
        Ok(())
    })
}

/// The parsed `document sync` flag set: `(path, base_version, node_id)`.
type DocumentSyncFlags = (String, i64, Option<String>);

/// `document sync PATH --base-version N [--node NODE_ID]` — the scratch-file
/// path (required), the version it was read at (required; 0 before the first
/// sync), and the optional map-node provenance stamp.
fn document_sync_flags(args: &[String]) -> Result<DocumentSyncFlags, i32> {
    let mut path: Option<String> = None;
    let mut base_version: Option<i64> = None;
    let mut node: Option<String> = None;

    let mut i = 0;
    while i < args.len() {
        let (name, inline) = match args[i].split_once('=') {
            Some((name, value)) => (name, Some(value.to_string())),
            None => (args[i].as_str(), None),
        };
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
            "--base-version" => {
                let raw = take()?;
                base_version = Some(raw.parse::<i64>().map_err(|_| {
                    eprintln!("{ERROR_PREFIX}--base-version must be an integer, got `{raw}`");
                    2
                })?);
            }
            "--node" => node = Some(take()?),
            other if other.starts_with("--") => {
                return usage(&format!(
                    "unknown document sync flag `{other}` (expected: --base-version, --node)"
                ))
            }
            _ => {
                if path.is_some() {
                    return usage("expected `document sync PATH --base-version N` (one positional path)");
                }
                path = Some(args[i].clone());
            }
        }
        i += if inline.is_some() { 1 } else { 2 };
    }

    let path = match path {
        Some(path) if !path.trim().is_empty() => path,
        _ => return usage("expected `document sync PATH --base-version N [--node NODE_ID]`"),
    };
    match base_version {
        Some(base_version) => Ok((path, base_version, node)),
        None => usage("--base-version is required (the version you pulled; 0 before the first sync)"),
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

/// Require exactly two positional arguments, or print the given usage line.
fn positional_pair(args: &[String], usage_line: &str) -> Result<(String, String), i32> {
    match args {
        [a, b] => Ok((a.clone(), b.clone())),
        _ => usage(&format!("expected `{usage_line}`")),
    }
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
                blocks = parse_id_list(&raw, "--blocks")?;
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

/// The parsed `node create` flag set:
/// `(kind, title, question, task_mode, blocked_by, blocks)`.
type NodeCreateFlags = (
    String,
    String,
    Option<String>,
    Option<String>,
    Vec<String>,
    Vec<String>,
);

/// `node create` flags: `--kind` (required:
/// grilling|research|prototype|task), `--title` (required), `--question`
/// (optional), `--task-mode afk|hitl` (required for `--kind task`, rejected
/// by the server for every other kind — fixed at creation), `--blocked-by
/// id1,id2` (optional: existing nodes that block the new node — the
/// graduation shape), `--blocks id1,id2` (optional: existing nodes the new
/// node blocks, matching `task create`).
fn node_create_flags(args: &[String]) -> Result<NodeCreateFlags, i32> {
    let mut kind: Option<String> = None;
    let mut title: Option<String> = None;
    let mut question: Option<String> = None;
    let mut task_mode: Option<String> = None;
    let mut blocked_by: Vec<String> = Vec::new();
    let mut blocks: Vec<String> = Vec::new();

    let mut i = 0;
    while i < args.len() {
        let (name, inline) = match args[i].split_once('=') {
            Some((name, value)) => (name, Some(value.to_string())),
            None => (args[i].as_str(), None),
        };
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
            "--kind" => kind = Some(take()?),
            "--title" => title = Some(take()?),
            "--question" => question = Some(take()?),
            "--task-mode" => task_mode = Some(take()?),
            "--blocked-by" => {
                let raw = take()?;
                blocked_by = parse_id_list(&raw, "--blocked-by")?;
            }
            "--blocks" => {
                let raw = take()?;
                blocks = parse_id_list(&raw, "--blocks")?;
            }
            other => return usage(&format!("unknown node create flag `{other}`")),
        }
        i += if inline.is_some() { 1 } else { 2 };
    }

    let kind = match kind {
        Some(kind) if !kind.trim().is_empty() => kind,
        _ => return usage("--kind is required (grilling | research | prototype | task)"),
    };
    let title = match title {
        Some(title) if !title.trim().is_empty() => title,
        _ => return usage("--title is required and must not be empty"),
    };
    Ok((kind, title, question, task_mode, blocked_by, blocks))
}

/// `node resolve NODE [--gist "..."]` — the positional node id plus the
/// optional one-line resolution gist.
fn node_resolve_args(args: &[String]) -> Result<(String, Option<String>), i32> {
    let mut positional: Option<String> = None;
    let mut gist: Option<String> = None;

    let mut i = 0;
    while i < args.len() {
        let (name, inline) = match args[i].split_once('=') {
            Some((name, value)) => (name, Some(value.to_string())),
            None => (args[i].as_str(), None),
        };
        match name {
            "--gist" => {
                gist = Some(if let Some(value) = inline {
                    value
                } else {
                    if i + 1 >= args.len() {
                        return usage("--gist requires a value");
                    }
                    i += 1;
                    args[i].clone()
                });
            }
            other if other.starts_with("--") => {
                return usage(&format!("unknown node resolve flag `{other}`"));
            }
            _ => {
                if positional.is_some() {
                    return usage("expected `node resolve NODE [--gist \"...\"]` (one positional node id)");
                }
                positional = Some(args[i].clone());
            }
        }
        i += 1;
    }

    match positional {
        Some(node_id) if !node_id.trim().is_empty() => Ok((node_id, gist)),
        _ => usage("expected `node resolve NODE [--gist \"...\"]`"),
    }
}

/// Parse a comma-separated id list flag value into trimmed, non-empty ids.
fn parse_id_list(raw: &str, flag: &str) -> Result<Vec<String>, i32> {
    let ids: Vec<String> = raw
        .split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_string)
        .collect();
    if ids.is_empty() {
        return usage(&format!("{flag} requires a comma-separated id list"));
    }
    Ok(ids)
}

/// Print a usage error with the CLI error marker (exit code 2).
fn usage<T>(message: &str) -> Result<T, i32> {
    eprintln!(
        "{ERROR_PREFIX}{message}
usage: dearborn --url <base> --token <cap> <verb>
  task create --title \"...\" [--description \"...\"] [--acceptance \"...\"] [--blocks id1,id2]
  task link BLOCKER BLOCKED
  dag
  node create --kind grilling|research|prototype|task --title \"...\" [--question \"...\"] [--task-mode afk|hitl] [--blocked-by id1,id2] [--blocks id1,id2]
  node link BLOCKER BLOCKED
  node resolve NODE [--gist \"...\"]
  map
  map set-destination|set-notes|set-fog|set-out-of-scope \"TEXT\"
  document pull [PATH]
  document sync PATH --base-version N [--node NODE_ID]
  scope"
    );
    Err(2)
}
