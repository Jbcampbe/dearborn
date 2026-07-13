//! Deerborn server binary entrypoint.

use deerborn_server::{app, init_tracing, AppState, Config, Db, MasterKey};

#[tokio::main]
async fn main() {
    init_tracing();

    // Fail fast on bad configuration (e.g. missing DEERBORN_MASTER_KEY / TOKEN)
    // before we bind a socket or touch the database.
    let config = match Config::from_env() {
        Ok(config) => config,
        Err(err) => {
            eprintln!("deerborn-server: configuration error: {err}");
            std::process::exit(1);
        }
    };

    // Fail fast if DEERBORN_MASTER_KEY can't form a valid 256-bit key (see
    // `crypto::MasterKey::derive`) — before binding a socket or touching the db.
    if let Err(err) = MasterKey::derive(&config.master_key) {
        eprintln!("deerborn-server: master key error: {err}");
        std::process::exit(1);
    }

    // Open the database and apply migrations at boot (idempotent).
    let db = match Db::connect(&config.db_path).await {
        Ok(db) => db,
        Err(err) => {
            eprintln!("deerborn-server: failed to open database `{}`: {err}", config.db_path);
            std::process::exit(1);
        }
    };
    match db.run_migrations().await {
        Ok(n) => tracing::info!(newly_applied = n, "migrations up to date"),
        Err(err) => {
            eprintln!("deerborn-server: migration error: {err}");
            std::process::exit(1);
        }
    }

    let addr = config.bind.clone();
    let state = AppState::new(config, db);

    let listener = tokio::net::TcpListener::bind(&addr)
        .await
        .unwrap_or_else(|e| panic!("failed to bind {addr}: {e}"));

    // Advertise Deerborn's loopback origin so planning runs can build the MCP
    // config URL the shelled-out agent connects back to (T-203). Use the bound
    // port; force the host to loopback (a 0.0.0.0 bind is not a dialable host).
    if let Ok(local) = listener.local_addr() {
        state.set_advertised_base(format!("http://127.0.0.1:{}", local.port()));
    }

    tracing::info!(%addr, "deerborn-server listening on http://{addr}");

    axum::serve(listener, app(state))
        .await
        .expect("server error");
}
