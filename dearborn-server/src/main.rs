//! Dearborn server binary entrypoint.

use dearborn_server::{
    app, evidence, init_tracing, review_poll, worker, AppState, Config, Db, MasterKey,
};

#[tokio::main]
async fn main() {
    init_tracing();

    // Fail fast on bad configuration (e.g. a missing DEARBORN_MASTER_KEY)
    // before we bind a socket or touch the database.
    let config = match Config::from_env() {
        Ok(config) => config,
        Err(err) => {
            eprintln!("dearborn-server: configuration error: {err}");
            std::process::exit(1);
        }
    };

    // Fail fast if DEARBORN_MASTER_KEY can't form a valid 256-bit key (see
    // `crypto::MasterKey::derive`) — before binding a socket or touching the db.
    if let Err(err) = MasterKey::derive(&config.master_key) {
        eprintln!("dearborn-server: master key error: {err}");
        std::process::exit(1);
    }

    // Open the database and apply migrations at boot (idempotent).
    let db = match Db::connect(&config.db_path).await {
        Ok(db) => db,
        Err(err) => {
            eprintln!(
                "dearborn-server: failed to open database `{}`: {err}",
                config.db_path
            );
            std::process::exit(1);
        }
    };
    match db.run_migrations().await {
        Ok(n) => tracing::info!(newly_applied = n, "migrations up to date"),
        Err(err) => {
            eprintln!("dearborn-server: migration error: {err}");
            std::process::exit(1);
        }
    }

    // Boot-time lease clear (D4, §13): single-server assumption means nothing
    // else could legitimately hold a lease across a restart, so clear every
    // lease now rather than making the pool wait out the TTL before resuming
    // in-flight work. Must run before `spawn_pool` claims anything.
    if let Err(err) = worker::clear_all_leases(&db).await {
        tracing::warn!(error = %err, "boot: failed to clear stale leases");
    }

    // Boot-time evidence reconciliation: any `agent_run` row still `running`
    // belonged to a stage owned by the previous process — under the same
    // single-server assumption as the lease clear above, nothing can
    // legitimately hold an open stage across a restart, and its agent is
    // gone. Closing those rows here is what keeps a task's pipeline view from
    // presenting a dead run's zombie `running` row next to the new owner's
    // fresh attempt (i.e. "two implementation agents" for one task).
    match evidence::cancel_orphaned_running(db.conn()).await {
        Ok(0) => {}
        Ok(n) => tracing::info!(
            rows = n,
            "boot: closed orphaned running agent runs as cancelled"
        ),
        Err(err) => {
            // Best-effort hygiene only: correctness comes from the lease
            // clear + the claim path's own orphan reset; a failure must not
            // block boot.
            tracing::warn!(error = %err, "boot: failed to close orphaned running agent runs");
        }
    }

    let addr = config.bind.clone();
    let state = AppState::new(config, db);

    let listener = tokio::net::TcpListener::bind(&addr)
        .await
        .unwrap_or_else(|e| panic!("failed to bind {addr}: {e}"));

    // Advertise Dearborn's loopback origin so planning runs can build the MCP
    // config URL the shelled-out agent connects back to (T-203). Use the bound
    // port; force the host to loopback (a 0.0.0.0 bind is not a dialable host).
    if let Ok(local) = listener.local_addr() {
        state.set_advertised_base(format!("http://127.0.0.1:{}", local.port()));
    }

    // Start the worker pool (D2, T-510): N long-lived loops that claim and
    // drive leased epics for the life of the process. Handles are dropped —
    // the pool runs until the process exits.
    let _worker_handles = worker::spawn_pool(state.clone());

    // Start the single review-poller (post-PR-review loop §5): a separate,
    // single-sequential task (concurrency 1, no lease) that periodically scans
    // `InReview` items for PR merge/close state (and, in later tasks,
    // feedback). Handle is dropped — the poller runs for the life of the
    // process, like the worker pool.
    let _review_poller_handle = review_poll::spawn_review_poller(state.clone());

    tracing::info!(%addr, "dearborn-server listening on http://{addr}");

    axum::serve(listener, app(state))
        .await
        .expect("server error");
}
