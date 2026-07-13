//! Deerborn server binary entrypoint.

use deerborn_server::{app, bind_addr};

#[tokio::main]
async fn main() {
    let addr = bind_addr();
    let listener = tokio::net::TcpListener::bind(&addr)
        .await
        .unwrap_or_else(|e| panic!("failed to bind {addr}: {e}"));

    println!("deerborn-server listening on http://{addr}");

    axum::serve(listener, app())
        .await
        .expect("server error");
}
