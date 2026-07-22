//! Integration tests for the authenticated WebSocket endpoint (T-005).
//!
//! Each test binds the real `app` router to an ephemeral port, serves it on a
//! background task, and drives it with a `tokio-tungstenite` client — exercising
//! the genuine handshake auth and upgrade path rather than an in-process shim.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use dearborn_server::{app, AppState, Config, Db, Hub};
use futures_util::{SinkExt, StreamExt};
use serde_json::{json, Value};
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Error as WsError;
use tokio_tungstenite::tungstenite::Message;

const TOKEN: &str = "s3cret-token";

/// Boot the real router on an ephemeral port; return the bound address and a
/// handle to the shared `Hub` so the test can publish server-side events.
async fn serve() -> (SocketAddr, Arc<Hub>) {
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
        stub_worker_delay_ms: 0,
    };
    let state = AppState::new(config, db);
    let hub = state.hub.clone();

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app(state)).await.unwrap();
    });
    (addr, hub)
}

/// Read the next text frame, decoded as JSON, failing if the socket closes or a
/// non-text frame arrives first.
async fn next_json<S>(socket: &mut S) -> Value
where
    S: StreamExt<Item = Result<Message, WsError>> + Unpin,
{
    loop {
        let frame = tokio::time::timeout(Duration::from_secs(5), socket.next())
            .await
            .expect("timed out waiting for a frame")
            .expect("socket closed unexpectedly")
            .expect("websocket error");
        match frame {
            Message::Text(text) => return serde_json::from_str(&text).unwrap(),
            Message::Ping(_) | Message::Pong(_) => continue,
            other => panic!("unexpected frame: {other:?}"),
        }
    }
}

#[tokio::test]
async fn connect_subscribe_and_receive_a_server_pushed_event() {
    let (addr, hub) = serve().await;
    let url = format!("ws://{addr}/ws?token={TOKEN}");

    let (mut socket, _resp) = connect_async(&url).await.expect("handshake should succeed");

    // Subscribe to a topic and wait for the ack — this guarantees the server has
    // registered the subscription before we publish (no race).
    socket
        .send(Message::Text(
            json!({ "type": "subscribe", "topic": "epic:42" }).to_string(),
        ))
        .await
        .unwrap();

    let ack = next_json(&mut socket).await;
    assert_eq!(ack, json!({ "topic": "epic:42", "type": "subscribed", "payload": {} }));

    // Server-side code publishes an event to the topic (this is the API T-202 /
    // T-401 will call).
    let delivered = hub.publish("epic:42", "message", json!({ "text": "hello" }));
    assert_eq!(delivered, 1, "exactly one subscriber should receive it");

    let event = next_json(&mut socket).await;
    assert_eq!(
        event,
        json!({ "topic": "epic:42", "type": "message", "payload": { "text": "hello" } })
    );
}

#[tokio::test]
async fn events_only_reach_subscribers_of_that_topic() {
    let (addr, hub) = serve().await;
    let url = format!("ws://{addr}/ws?token={TOKEN}");
    let (mut socket, _resp) = connect_async(&url).await.unwrap();

    socket
        .send(Message::Text(
            json!({ "type": "subscribe", "topic": "epic:1" }).to_string(),
        ))
        .await
        .unwrap();
    let _ack = next_json(&mut socket).await;

    // A publish to an unsubscribed topic must not arrive; a later publish to the
    // subscribed topic must. Ordering proves the first was not delivered.
    assert_eq!(hub.publish("project:other", "kanban", json!({})), 0);
    hub.publish("epic:1", "message", json!({ "n": 7 }));

    let event = next_json(&mut socket).await;
    assert_eq!(event["topic"], "epic:1");
    assert_eq!(event["payload"]["n"], 7);
}

#[tokio::test]
async fn unsubscribe_stops_delivery() {
    let (addr, hub) = serve().await;
    let url = format!("ws://{addr}/ws?token={TOKEN}");
    let (mut socket, _resp) = connect_async(&url).await.unwrap();

    socket
        .send(Message::Text(
            json!({ "type": "subscribe", "topic": "epic:9" }).to_string(),
        ))
        .await
        .unwrap();
    assert_eq!(next_json(&mut socket).await["type"], "subscribed");

    socket
        .send(Message::Text(
            json!({ "type": "unsubscribe", "topic": "epic:9" }).to_string(),
        ))
        .await
        .unwrap();
    assert_eq!(next_json(&mut socket).await["type"], "unsubscribed");

    // After the unsubscribe ack, the connection is no longer a subscriber.
    assert_eq!(hub.publish("epic:9", "message", json!({})), 0);
}

#[tokio::test]
async fn unauthenticated_connect_is_rejected() {
    let (addr, _hub) = serve().await;

    // No token at all.
    let url = format!("ws://{addr}/ws");
    match connect_async(&url).await {
        Err(WsError::Http(resp)) => assert_eq!(resp.status(), 401),
        other => panic!("expected 401 handshake rejection, got {other:?}"),
    }

    // Wrong token.
    let url = format!("ws://{addr}/ws?token=not-the-token");
    match connect_async(&url).await {
        Err(WsError::Http(resp)) => assert_eq!(resp.status(), 401),
        other => panic!("expected 401 handshake rejection, got {other:?}"),
    }
}

#[tokio::test]
async fn authorization_header_also_authenticates() {
    let (addr, _hub) = serve().await;
    let url = format!("ws://{addr}/ws");

    // Native clients can present the bearer token via the header instead.
    use tokio_tungstenite::tungstenite::client::IntoClientRequest;
    let mut request = url.into_client_request().unwrap();
    request
        .headers_mut()
        .insert("Authorization", format!("Bearer {TOKEN}").parse().unwrap());

    let (mut socket, _resp) = connect_async(request)
        .await
        .expect("header auth should succeed");

    socket
        .send(Message::Text(
            json!({ "type": "subscribe", "topic": "project:1" }).to_string(),
        ))
        .await
        .unwrap();
    assert_eq!(next_json(&mut socket).await["type"], "subscribed");
}
