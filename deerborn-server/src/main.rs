//! Deerborn server binary entrypoint.

use deerborn_server::{app, AppState, Config};

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

    let addr = config.bind.clone();
    let state = AppState::new(config);

    let listener = tokio::net::TcpListener::bind(&addr)
        .await
        .unwrap_or_else(|e| panic!("failed to bind {addr}: {e}"));

    println!("deerborn-server listening on http://{addr}");

    axum::serve(listener, app(state))
        .await
        .expect("server error");
}
