//! LIVE end-to-end smoke test for T-203 (the local MCP server), driving the REAL
//! `claude` CLI. Excluded from the hermetic `cargo test` gate — it needs `claude`
//! installed + authenticated and spawns a real agent that connects back to
//! Deerborn's `/mcp/:cap` endpoint over loopback.
//!
//! ## How to run
//!
//! ```sh
//! # from the repo root; `claude` must be on PATH and logged in
//! cargo test -p deerborn-server --test mcp_live -- --ignored --nocapture
//! ```
//!
//! No env is required beyond a working `claude` (the same prerequisite as the
//! T-202 live test). The test:
//!   1. binds the real router on an ephemeral port and advertises that origin so
//!      the planning run builds a reachable MCP config URL;
//!   2. seeds a project whose `clone_path` is a temp fixture repo containing a
//!      file with a unique marker string, plus a `Planning` epic;
//!   3. posts a user message instructing the agent to read that file with
//!      `read_codebase_context` and store its contents via `update_epic`;
//!   4. polls until the epic's `product_context` is populated and asserts it
//!      contains the marker — proving BOTH tools ran end-to-end (the agent read
//!      real code from the clone AND mutated the epic record through MCP), and
//!      that the mutation is confined to the scoped column.

use std::net::SocketAddr;
use std::time::Duration;

use deerborn_server::{app, AppState, Config, Db};

const TOKEN: &str = "s3cret-token";
/// A unique string that only exists inside the fixture clone, so finding it in
/// the epic record proves the agent truly read the file (not hallucinated it).
const MARKER: &str = "DEERBORN_MAGIC_PINEAPPLE_42";

#[tokio::test]
#[ignore = "drives the live `claude` CLI; run with --ignored"]
async fn live_planning_agent_reads_clone_and_updates_epic_via_mcp() {
    // ---- fixture clone on disk (read-only content the agent will quote) ----
    let clone_dir = std::env::temp_dir().join(format!("deerborn-t203-live-{}", ulid::Ulid::new()));
    std::fs::create_dir_all(clone_dir.join("src")).unwrap();
    std::fs::write(
        clone_dir.join("src/marker.rs"),
        format!("// project marker\npub const MARKER: &str = \"{MARKER}\";\n"),
    )
    .unwrap();

    // ---- real server on an ephemeral port (claude connects back to /mcp/:cap) ----
    let db = Db::connect(":memory:").await.unwrap();
    db.run_migrations().await.unwrap();
    let config = Config {
        bind: "127.0.0.1:0".to_string(),
        token: TOKEN.to_string(),
        master_key: "test-master-key".to_string(),
        db_path: ":memory:".to_string(),
        clone_root: "./clones".to_string(),
        static_dir: "./client/dist".to_string(),
        auto_clone: false,
    };
    let state = AppState::new(config, db); // production ClaudePlanningAgent

    // Seed a project pointing at the fixture clone, plus a Planning epic.
    let now = 1_700_000_000_000i64;
    let project_id = ulid::Ulid::new().to_string();
    let epic_id = ulid::Ulid::new().to_string();
    state
        .db
        .conn()
        .execute(
            "INSERT INTO project (id, name, repo_url, clone_path, clone_status, created_at, updated_at) \
             VALUES (?1, 'Live', 'https://example.com/p.git', ?2, 'ready', ?3, ?3)",
            libsql::params![project_id.clone(), clone_dir.to_string_lossy().to_string(), now],
        )
        .await
        .unwrap();
    state
        .db
        .conn()
        .execute(
            "INSERT INTO epic (id, project_id, title, status, created_at, updated_at) \
             VALUES (?1, ?2, 'Live epic', 'Planning', ?3, ?3)",
            libsql::params![epic_id.clone(), project_id.clone(), now],
        )
        .await
        .unwrap();
    state
        .db
        .conn()
        .execute(
            "INSERT INTO planning_session (epic_id, phase, status, created_at, updated_at) \
             VALUES (?1, 'product', 'active', ?2, ?2)",
            libsql::params![epic_id.clone(), now],
        )
        .await
        .unwrap();

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr: SocketAddr = listener.local_addr().unwrap();
    state.set_advertised_base(format!("http://127.0.0.1:{}", addr.port()));
    let served = state.clone();
    tokio::spawn(async move {
        axum::serve(listener, app(served)).await.unwrap();
    });

    // ---- trigger a planning run via the real HTTP handler path ----
    // (Posting through the router shares the same AppState the server is serving,
    // so the minted capability is resolvable by the live /mcp/:cap endpoint.)
    use axum::body::Body;
    use axum::http::{header::AUTHORIZATION, header::CONTENT_TYPE, Request, StatusCode};
    use tower::ServiceExt;
    let prompt = format!(
        "Use the read_codebase_context tool to read the file `src/marker.rs`, then call \
         update_epic with the product context set to exactly the value of the MARKER \
         constant you find in that file. The marker looks like {MARKER}."
    );
    let posted = app(state.clone())
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/epics/{epic_id}/messages"))
                .header(AUTHORIZATION, format!("Bearer {TOKEN}"))
                .header(CONTENT_TYPE, "application/json")
                .body(Body::from(
                    serde_json::json!({ "phase": "product", "content": prompt }).to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(posted.status(), StatusCode::CREATED);

    // ---- wait for the agent to call update_epic (real runs take a while) ----
    let deadline = tokio::time::Instant::now() + Duration::from_secs(180);
    let mut product_context: Option<String> = None;
    while tokio::time::Instant::now() < deadline {
        let mut rows = state
            .db
            .conn()
            .query(
                "SELECT product_context FROM epic WHERE id = ?1",
                libsql::params![epic_id.clone()],
            )
            .await
            .unwrap();
        let row = rows.next().await.unwrap().unwrap();
        if let Some(ctx) = row.get::<Option<String>>(0).unwrap() {
            product_context = Some(ctx);
            break;
        }
        tokio::time::sleep(Duration::from_secs(2)).await;
    }

    let ctx = product_context.expect("agent should have populated product_context via update_epic");
    assert!(
        ctx.contains(MARKER),
        "product_context must quote the marker read from the clone; got: {ctx}"
    );

    std::fs::remove_dir_all(&clone_dir).ok();
}

/// LIVE end-to-end for T-205: after product planning, the epic advances to
/// TECHNICAL planning; the technical run reads the same read-only clone and
/// fills `technical_context` (a separate column) via `update_epic` — proving the
/// second phase config shares the one chat/MCP engine and has code-inspection
/// context.
///
/// Run with:
/// ```sh
/// cargo test -p deerborn-server --test mcp_live \
///   live_technical_planning_reads_clone_and_fills_technical_context \
///   -- --ignored --nocapture
/// ```
#[tokio::test]
#[ignore = "drives the live `claude` CLI; run with --ignored"]
async fn live_technical_planning_reads_clone_and_fills_technical_context() {
    use axum::body::Body;
    use axum::http::{header::AUTHORIZATION, header::CONTENT_TYPE, Request, StatusCode};
    use tower::ServiceExt;

    let clone_dir = std::env::temp_dir().join(format!("deerborn-t205-live-{}", ulid::Ulid::new()));
    std::fs::create_dir_all(clone_dir.join("src")).unwrap();
    std::fs::write(
        clone_dir.join("src/marker.rs"),
        format!("// project marker\npub const MARKER: &str = \"{MARKER}\";\n"),
    )
    .unwrap();

    let db = Db::connect(":memory:").await.unwrap();
    db.run_migrations().await.unwrap();
    let config = Config {
        bind: "127.0.0.1:0".to_string(),
        token: TOKEN.to_string(),
        master_key: "test-master-key".to_string(),
        db_path: ":memory:".to_string(),
        clone_root: "./clones".to_string(),
        static_dir: "./client/dist".to_string(),
        auto_clone: false,
    };
    let state = AppState::new(config, db);

    let now = 1_700_000_000_000i64;
    let project_id = ulid::Ulid::new().to_string();
    let epic_id = ulid::Ulid::new().to_string();
    state
        .db
        .conn()
        .execute(
            "INSERT INTO project (id, name, repo_url, clone_path, clone_status, created_at, updated_at) \
             VALUES (?1, 'Live', 'https://example.com/p.git', ?2, 'ready', ?3, ?3)",
            libsql::params![project_id.clone(), clone_dir.to_string_lossy().to_string(), now],
        )
        .await
        .unwrap();
    // A Planning epic with an already-agreed product context, plus its product
    // session (as epic creation would create).
    state
        .db
        .conn()
        .execute(
            "INSERT INTO epic (id, project_id, title, product_context, status, created_at, updated_at) \
             VALUES (?1, ?2, 'Live', 'Users need a documented MARKER constant.', 'Planning', ?3, ?3)",
            libsql::params![epic_id.clone(), project_id.clone(), now],
        )
        .await
        .unwrap();
    state
        .db
        .conn()
        .execute(
            "INSERT INTO planning_session (epic_id, phase, status, created_at, updated_at) \
             VALUES (?1, 'product', 'active', ?2, ?2)",
            libsql::params![epic_id.clone(), now],
        )
        .await
        .unwrap();

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr: SocketAddr = listener.local_addr().unwrap();
    state.set_advertised_base(format!("http://127.0.0.1:{}", addr.port()));
    let served = state.clone();
    tokio::spawn(async move {
        axum::serve(listener, app(served)).await.unwrap();
    });

    // Advance to technical planning.
    let advanced = app(state.clone())
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/epics/{epic_id}/advance-phase"))
                .header(AUTHORIZATION, format!("Bearer {TOKEN}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(advanced.status(), StatusCode::CREATED);

    // A technical-phase message; the technical run reads the clone + writes
    // technical_context.
    let prompt = format!(
        "Use read_codebase_context to read `src/marker.rs`, then call update_epic with the \
         technical context set to exactly the value of the MARKER constant you find. It looks \
         like {MARKER}."
    );
    let posted = app(state.clone())
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/epics/{epic_id}/messages"))
                .header(AUTHORIZATION, format!("Bearer {TOKEN}"))
                .header(CONTENT_TYPE, "application/json")
                .body(Body::from(
                    serde_json::json!({ "phase": "technical", "content": prompt }).to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(posted.status(), StatusCode::CREATED);

    let deadline = tokio::time::Instant::now() + Duration::from_secs(180);
    let mut technical_context: Option<String> = None;
    while tokio::time::Instant::now() < deadline {
        let mut rows = state
            .db
            .conn()
            .query(
                "SELECT technical_context FROM epic WHERE id = ?1",
                libsql::params![epic_id.clone()],
            )
            .await
            .unwrap();
        let row = rows.next().await.unwrap().unwrap();
        if let Some(ctx) = row.get::<Option<String>>(0).unwrap() {
            technical_context = Some(ctx);
            break;
        }
        tokio::time::sleep(Duration::from_secs(2)).await;
    }

    let ctx = technical_context.expect("technical run should have populated technical_context");
    assert!(
        ctx.contains(MARKER),
        "technical_context must quote the marker read from the clone; got: {ctx}"
    );

    std::fs::remove_dir_all(&clone_dir).ok();
}
