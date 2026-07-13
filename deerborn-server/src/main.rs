//! Deerborn server binary entrypoint.

use deerborn_server::{app, AppState, Config, Db};

#[tokio::main]
async fn main() {
    // Fail fast on bad configuration (e.g. missing DEERBORN_MASTER_KEY / TOKEN)
    // before we bind a socket or touch the database.
    let config = match Config::from_env() {
        Ok(config) => config,
        Err(err) => {
            eprintln!("deerborn-server: configuration error: {err}");
            std::process::exit(1);
        }
    };

    // Open the database and apply migrations at boot (idempotent).
    let db = match Db::connect(&config.db_path).await {
        Ok(db) => db,
        Err(err) => {
            eprintln!("deerborn-server: failed to open database `{}`: {err}", config.db_path);
            std::process::exit(1);
        }
    };
    match db.run_migrations().await {
        Ok(n) => println!("deerborn-server: migrations up to date ({n} newly applied)"),
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

    println!("deerborn-server listening on http://{addr}");

    axum::serve(listener, app(state))
        .await
        .expect("server error");
}
