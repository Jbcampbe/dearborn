//! T-200 SPIKE — how Deerborn passes its LOCAL MCP server to the shelled-out
//! Claude Code, given that `agent-harness` v0.3.5 has NO first-class MCP field.
//!
//! Finding: the harness has no `mcp`/`McpServer` config anywhere. But
//! `RunTuning.extra_args` is appended verbatim to the `claude` argv (see
//! `build_claude_args` in the crate), and Claude Code supports `--mcp-config`
//! plus tool-allow flags. So Deerborn wires MCP purely through `extra_args` —
//! no fork, no adapter change.
//!
//! This binary does NOT spawn claude (a real MCP endpoint would need to be
//! serving). It builds the exact RunRequest Deerborn's planning agent (T-203)
//! would send and prints the resulting knobs, proving the wiring compiles
//! against the real API.
//!
//! Run:  cargo run --bin mcp_config_demo

use harness::{RunMode, RunRequest, RunTuning};

fn main() {
    // Deerborn would write this JSON to a temp file (or pass inline) describing
    // its local MCP server. Claude Code reads it via `--mcp-config`. Tools then
    // surface to the agent as `mcp__deerborn__update_epic`, etc.
    let mcp_config_json = r#"{
  "mcpServers": {
    "deerborn": {
      "type": "http",
      "url": "http://127.0.0.1:PORT/mcp",
      "headers": { "Authorization": "Bearer <planning-session-token>" }
    }
  }
}"#;
    // (An stdio server — "command"/"args" instead of "type":"http" — works the
    // same way; Deerborn picks whichever transport its MCP server exposes.)

    // Phase-scoped allow-list per MILESTONE §2.4 / ARCHITECTURE §11: the
    // planning agent may ONLY call update_epic + read_codebase_context. Claude
    // Code namespaces MCP tools as `mcp__<server>__<tool>`.
    let allowed_tools =
        "mcp__deerborn__update_epic,mcp__deerborn__read_codebase_context";

    let extra_args = vec![
        "--mcp-config".to_owned(),
        mcp_config_json.to_owned(), // Claude accepts inline JSON or a file path
        "--allowedTools".to_owned(),
        allowed_tools.to_owned(),
        // Headless auto-approval so the gated MCP tools run without a TTY prompt.
        // (`build_claude_args` only injects its own --permission-mode default in
        // Edit mode and only if the host hasn't set one, so this is respected.)
        "--permission-mode".to_owned(),
        "bypassPermissions".to_owned(),
    ];

    let request = RunRequest {
        run_id: "t203-planning-demo".to_owned(),
        prompt: "Draft the product context for this epic, then call update_epic.".to_owned(),
        cwd: Some(std::path::PathBuf::from("/path/to/project/canonical-clone")),
        mode: RunMode::Ask,
        tuning: RunTuning { extra_args, ..RunTuning::default() },
        resume: None,
    };

    println!("Deerborn planning RunRequest (MCP wired via extra_args):\n");
    println!("run_id : {}", request.run_id);
    println!("cwd    : {:?}", request.cwd);
    println!("mode   : {:?}", request.mode);
    println!("\nextra_args appended verbatim to the `claude` argv:");
    for arg in &request.tuning.extra_args {
        println!("  {arg}");
    }
    println!(
        "\nResulting (abridged) argv the adapter spawns:\n  \
         claude -p \"<prompt>\" --output-format stream-json --verbose \
         --include-partial-messages --mcp-config <json> \
         --allowedTools {allowed_tools} --permission-mode bypassPermissions"
    );
    println!("\nTool calls then surface as RunEvent::ToolStart {{ name: \"mcp__deerborn__update_epic\", .. }}");
    println!("(NOTE: for Claude, ToolStart.input is always None — args stream as input_json_delta");
    println!(" and the adapter does not reconstruct them. Deerborn reads the real args server-side");
    println!(" inside its own MCP handler, so this is not a blocker.)");
}
