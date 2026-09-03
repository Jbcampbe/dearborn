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
//! - `node resolve NODE [--gist "..."] [--document PATH --base-version N]`
//!   `[--graduate "kind=grilling; title=...; question=..."]...`
//!   `[--out-of-scope "title=...; reason=..."]...`
//!   `[--update "id=NODE_ID; state=...; ..."]... [--trim-fog "..."]`
//!   — the grilling resolution bundle (wayfinder epic §6): record the decision,
//!   fold the edited document in as a new version under the per-epic write
//!   semaphore, graduate fog into new frontier nodes (blocked by this node),
//!   rule things out of scope (create+close an out_of_scope node + prose
//!   line), and update/invalidate affected nodes — one call. A stale
//!   `--base-version` exits non-zero naming the current version — re-pull,
//!   re-edit, retry. HITL kinds only (grilling/prototype).
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

use serde_json::json;
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
                    let (node_id, resolution) = node_resolve_args(args)?;
                    let client = client(&base_url, &token)?;
                    block_on(async move {
                        let outcome = client
                            .node_resolve_bundle(&node_id, &resolution)
                            .await?;
                        println!("{}", serde_json::to_string(&outcome).expect("resolve result is JSON"));
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
        // Advance: an inline `--flag=value` or a positional consumes only
        // itself; a separate-value flag consumed the NEXT token too. (A
        // positional must not skip the following flag — `document sync
        // PATH --base-version N` depends on it.)
        i += if inline.is_some() || !name.starts_with("--") { 1 } else { 2 };
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
        i += if inline.is_some() || !name.starts_with("--") { 1 } else { 2 };
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

/// `node resolve` resolution flags: the optional one-line decision (`--gist`),
/// the folded document edit (`--document PATH` + `--base-version N`, the
/// version the file was pulled at — big HTML through file tools, not
/// tool-args), repeated `key=value; key=value` specs for the map-reshaping
/// parts (`--graduate`, `--out-of-scope`, `--update`), and the replacement fog
/// prose (`--trim-fog`). Assembles the resolution-bundle request body.
fn node_resolve_args(args: &[String]) -> Result<(String, serde_json::Value), i32> {
    use serde_json::{Map, Value};

    let mut positional: Option<String> = None;
    let mut gist: Option<String> = None;
    let mut document: Option<String> = None;
    let mut base_version: Option<i64> = None;
    let mut graduations: Vec<String> = Vec::new();
    let mut out_of_scope: Vec<String> = Vec::new();
    let mut updates: Vec<String> = Vec::new();
    let mut trim_fog: Option<String> = None;

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
            "--gist" => gist = Some(take()?),
            "--document" => document = Some(take()?),
            "--base-version" => {
                let raw = take()?;
                base_version = Some(raw.parse::<i64>().map_err(|_| {
                    eprintln!("{ERROR_PREFIX}--base-version must be an integer, got `{raw}`");
                    2
                })?);
            }
            "--graduate" => graduations.push(take()?),
            "--out-of-scope" => out_of_scope.push(take()?),
            "--update" => updates.push(take()?),
            "--trim-fog" => trim_fog = Some(take()?),
            other if other.starts_with("--") => {
                return usage(&format!(
                    "unknown node resolve flag `{other}` (expected: --gist, --document, \
                     --base-version, --graduate, --out-of-scope, --update, --trim-fog)"
                ));
            }
            _ => {
                if positional.is_some() {
                    return usage(
                        "expected `node resolve NODE [flags]` (one positional node id)",
                    );
                }
                positional = Some(args[i].clone());
            }
        }
        // Advance: an inline `--flag=value` or a positional consumes only
        // itself; a separate-value flag consumed the NEXT token too. (A
        // positional must not skip the following flag — `document sync PATH
        // --base-version N` and `node resolve NODE --gist "..."` depend on it.)
        i += if inline.is_some() || !name.starts_with("--") { 1 } else { 2 };
    }

    let node_id = match positional {
        Some(node_id) if !node_id.trim().is_empty() => node_id,
        _ => return usage("expected `node resolve NODE [flags]`"),
    };

    // `--document` and `--base-version` go together: the sync must carry the
    // version the scratch file was pulled at.
    let document = match (document, base_version) {
        (Some(path), Some(base_version)) => {
            let html = std::fs::read_to_string(&path).map_err(|err| {
                eprintln!("{ERROR_PREFIX}failed to read {path}: {err}");
                1
            })?;
            Some(json!({ "html": html, "base_version": base_version }))
        }
        (Some(_), None) => {
            return usage("--document requires --base-version N (the version you pulled)")
        }
        (None, Some(_)) => {
            return usage("--base-version requires --document PATH (the file you edited)")
        }
        (None, None) => None,
    };

    let mut body = Map::new();
    if let Some(gist) = gist {
        body.insert("gist".into(), Value::String(gist));
    }
    if let Some(document) = document {
        body.insert("document".into(), document);
    }
    if !graduations.is_empty() {
        body.insert(
            "graduations".into(),
            Value::Array(parse_kv_specs(
                &graduations,
                "--graduate",
                &["kind", "title"],
                &["kind", "title", "question", "task_mode"],
            )?),
        );
    }
    if !out_of_scope.is_empty() {
        body.insert(
            "out_of_scope".into(),
            Value::Array(parse_kv_specs(
                &out_of_scope,
                "--out-of-scope",
                &["title", "reason"],
                &["title", "kind", "reason"],
            )?),
        );
    }
    if !updates.is_empty() {
        body.insert(
            "updates".into(),
            Value::Array(parse_kv_specs(
                &updates,
                "--update",
                &["id"],
                &["id", "state", "title", "question", "gist", "out_of_scope_reason"],
            )?),
        );
    }
    if let Some(trim_fog) = trim_fog {
        body.insert("trim_fog".into(), Value::String(trim_fog));
    }
    Ok((node_id, Value::Object(body)))
}

/// Parse repeated `key=value; key=value` spec strings (the `--graduate` /
/// `--out-of-scope` / `--update` flags) into JSON objects. Every key must be
/// in `allowed` and every key in `required` must be present; a malformed pair
/// is a usage error naming the flag.
fn parse_kv_specs(
    specs: &[String],
    flag: &str,
    required: &[&str],
    allowed: &[&str],
) -> Result<Vec<serde_json::Value>, i32> {
    use serde_json::{Map, Value};

    let mut objects = Vec::with_capacity(specs.len());
    for spec in specs {
        let mut object = Map::new();
        for pair in spec.split(';') {
            let pair = pair.trim();
            if pair.is_empty() {
                continue;
            }
            let Some((key, value)) = pair.split_once('=') else {
                return usage(&format!(
                    "{flag} specs are `key=value; key=value` pairs, got `{pair}`"
                ));
            };
            let key = key.trim();
            if !allowed.contains(&key) {
                return usage(&format!(
                    "unknown {flag} key `{key}` (expected: {})",
                    allowed.join(", ")
                ));
            }
            object.insert(key.to_string(), Value::String(value.trim().to_string()));
        }
        for key in required {
            if !object.contains_key(*key) {
                let example = allowed
                    .iter()
                    .map(|k| format!("{k}=..."))
                    .collect::<Vec<_>>()
                    .join("; ");
                return usage(&format!(
                    "{flag} requires a `{key}` (e.g. `{flag} \"{example}\"`)"
                ));
            }
        }
        objects.push(Value::Object(object));
    }
    Ok(objects)
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
  node resolve NODE [--gist \"...\"] [--document PATH --base-version N]
    [--graduate \"kind=grilling; title=...; question=...\"]...
    [--out-of-scope \"title=...; reason=...\"]...
    [--update \"id=NODE_ID; state=out_of_scope; out_of_scope_reason=...\"]...
    [--trim-fog \"...\"]
  map
  map set-destination|set-notes|set-fog|set-out-of-scope \"TEXT\"
  document pull [PATH]
  document sync PATH --base-version N [--node NODE_ID]
  scope"
    );
    Err(2)
}

// ---- tests ------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    // ---- the grilling resolution bundle's flag parsing -----------------------

    #[test]
    fn a_bare_resolve_assembles_a_body_with_only_the_node_id() {
        let args: Vec<String> = ["01ABC"].iter().map(|s| s.to_string()).collect();
        let (node_id, body) = node_resolve_args(&args).unwrap();
        assert_eq!(node_id, "01ABC");
        assert!(body.as_object().unwrap().is_empty());
    }

    #[test]
    fn the_full_bundle_assembles_every_resolution_part() {
        let scratch = std::env::temp_dir().join(format!("dearborn-cli-test-{}", ulid::Ulid::new()));
        std::fs::create_dir_all(&scratch).unwrap();
        let doc = scratch.join("document.html");
        std::fs::write(&doc, "<h1 id=\"dec\">Decisions</h1>").unwrap();

        let args: Vec<String> = [
            "01NODE",
            "--gist", "Use the evidence blob store",
            "--document", doc.to_str().unwrap(),
            "--base-version", "3",
            "--graduate", "kind=grilling; title=Which events export?; question=Scope",
            "--graduate", "kind=task; title=Provision bucket; task_mode=afk",
            "--out-of-scope", "title=Multi-region; reason=Single-region only",
            "--update", "id=01OTHER; state=out_of_scope; out_of_scope_reason=Superseded",
            "--trim-fog", "Retention policy",
        ]
        .iter()
        .map(|s| s.to_string())
        .collect();

        let (node_id, body) = node_resolve_args(&args).unwrap();
        assert_eq!(node_id, "01NODE");

        assert_eq!(body["gist"], "Use the evidence blob store");
        // The document HTML came from the scratch file, not tool-args.
        assert_eq!(body["document"]["html"], "<h1 id=\"dec\">Decisions</h1>");
        assert_eq!(body["document"]["base_version"], 3);
        let graduations = body["graduations"].as_array().unwrap();
        assert_eq!(graduations.len(), 2);
        assert_eq!(graduations[0]["kind"], "grilling");
        assert_eq!(graduations[0]["title"], "Which events export?");
        assert_eq!(graduations[1]["task_mode"], "afk");
        let oos = body["out_of_scope"].as_array().unwrap();
        assert_eq!(oos[0]["title"], "Multi-region");
        assert_eq!(oos[0]["reason"], "Single-region only");
        let updates = body["updates"].as_array().unwrap();
        assert_eq!(updates[0]["id"], "01OTHER");
        assert_eq!(updates[0]["state"], "out_of_scope");
        assert_eq!(body["trim_fog"], "Retention policy");

        std::fs::remove_dir_all(&scratch).ok();
    }

    #[test]
    fn document_sync_parses_the_positional_path_followed_by_flags() {
        // Regression: a positional PATH followed by `--base-version` used to
        // skip the flag (the positional advanced the cursor by two).
        let (path, base_version, node) = document_sync_flags(
            &["doc.html", "--base-version", "7"]
                .iter()
                .map(|s| s.to_string())
                .collect::<Vec<_>>(),
        )
        .unwrap();
        assert_eq!(path, "doc.html");
        assert_eq!(base_version, 7);
        assert_eq!(node, None);

        // Flags before the path keep working, as does `--flag=value` form.
        let (path, base_version, node) = document_sync_flags(
            &["--node=01N", "--base-version", "2", "doc.html"]
                .iter()
                .map(|s| s.to_string())
                .collect::<Vec<_>>(),
        )
        .unwrap();
        assert_eq!(path, "doc.html");
        assert_eq!(base_version, 2);
        assert_eq!(node.as_deref(), Some("01N"));
    }

    #[test]
    fn malformed_resolution_flags_are_usage_errors() {
        // --document without --base-version (the sync must carry the base).
        let args: Vec<String> = ["01NODE", "--document", "x.html"]
            .iter()
            .map(|s| s.to_string())
            .collect();
        assert_eq!(node_resolve_args(&args).unwrap_err(), 2);

        // --base-version without --document.
        let args: Vec<String> = ["01NODE", "--base-version", "1"]
            .iter()
            .map(|s| s.to_string()).collect();
        assert_eq!(node_resolve_args(&args).unwrap_err(), 2);

        // A --graduate spec missing its required `kind`.
        let args: Vec<String> = ["01NODE", "--graduate", "title=Only title"]
            .iter()
            .map(|s| s.to_string()).collect();
        assert_eq!(node_resolve_args(&args).unwrap_err(), 2);

        // An unknown spec key.
        let args: Vec<String> = [
            "01NODE",
            "--graduate", "kind=grilling; title=T; cargo= cult",
        ]
        .iter()
        .map(|s| s.to_string()).collect();
        assert_eq!(node_resolve_args(&args).unwrap_err(), 2);

        // A spec pair without `=`.
        let args: Vec<String> = ["01NODE", "--update", "01OTHER"]
            .iter()
            .map(|s| s.to_string()).collect();
        assert_eq!(node_resolve_args(&args).unwrap_err(), 2);
    }
}
