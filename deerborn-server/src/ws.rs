//! Authenticated WebSocket endpoint and subscription protocol.
//!
//! `GET /ws` upgrades to a WebSocket carrying the pub/sub protocol that live
//! features (planning `RunEvent` streaming, kanban updates) push through the
//! [`crate::hub::Hub`]. See `CONVENTIONS.md` for the full wire contract.
//!
//! ## Authentication
//!
//! Browsers cannot set an `Authorization` header on the WS handshake, so the
//! token is accepted from **either**:
//!   * the `token` query parameter — `GET /ws?token=<DEERBORN_TOKEN>` (browsers), or
//!   * an `Authorization: Bearer <DEERBORN_TOKEN>` header (native clients / tools).
//!
//! Validation happens **before** the upgrade: a missing/invalid token is rejected
//! with the standard `401` error envelope and the socket is never opened. `/ws` is
//! therefore registered outside the header-only auth middleware, which would
//! otherwise reject every browser handshake.
//!
//! ## Protocol
//!
//! Every frame is a JSON object `{ "topic", "type", "payload" }`.
//!
//! Client → server control frames:
//!   * `{ "type": "subscribe",   "topic": "epic:<id>" }`
//!   * `{ "type": "unsubscribe", "topic": "epic:<id>" }`
//!
//! Server → client frames:
//!   * `{ "topic": "<t>", "type": "subscribed",   "payload": {} }` — ack
//!   * `{ "topic": "<t>", "type": "unsubscribed", "payload": {} }` — ack
//!   * `{ "topic": "<t>", "type": "<event>",      "payload": {…} }` — published event
//!   * `{ "topic": "",    "type": "error",        "payload": { "message": "…" } }`

use std::collections::HashMap;
use std::sync::Arc;

use axum::{
    extract::{
        ws::{Message, WebSocket, WebSocketUpgrade},
        Query, State,
    },
    http::{header::AUTHORIZATION, HeaderMap},
    response::{IntoResponse, Response},
};
use futures_util::{sink::SinkExt, stream::StreamExt};
use serde::Deserialize;
use serde_json::{json, Value};
use tokio::sync::{broadcast, mpsc};
use tokio::task::JoinHandle;

use crate::hub::{encode, Envelope, Hub};
use crate::{auth, AppError, AppState};

/// Per-connection buffer of outbound frames awaiting the socket writer.
const OUTBOX_CAPACITY: usize = 256;

/// Token supplied on the handshake query string (`/ws?token=…`).
#[derive(Debug, Deserialize)]
pub struct WsAuth {
    token: Option<String>,
}

/// `GET /ws` — authenticate the handshake, then upgrade to the pub/sub protocol.
pub async fn ws_handler(
    State(state): State<AppState>,
    Query(auth_params): Query<WsAuth>,
    headers: HeaderMap,
    ws: WebSocketUpgrade,
) -> Response {
    // Accept the token from the Authorization header or the `token` query param.
    let header_token = headers
        .get(AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(auth::bearer_token);
    let presented = header_token.or(auth_params.token.as_deref());

    if presented != Some(state.config.token.as_str()) {
        // Reject before upgrading: no socket is opened for an unauthenticated peer.
        return AppError::Unauthorized.into_response();
    }

    ws.on_upgrade(move |socket| handle_socket(socket, state.hub.clone()))
}

/// Drive one authenticated connection: read control frames, fan hub events out.
async fn handle_socket(socket: WebSocket, hub: Arc<Hub>) {
    let (mut ws_sink, mut ws_stream) = socket.split();

    // Every outbound frame (acks + forwarded events) flows through one mpsc so a
    // single task owns the write half of the socket.
    let (outbox_tx, mut outbox_rx) = mpsc::channel::<Envelope>(OUTBOX_CAPACITY);
    let writer: JoinHandle<()> = tokio::spawn(async move {
        while let Some(frame) = outbox_rx.recv().await {
            if ws_sink.send(Message::Text(frame.to_string())).await.is_err() {
                break; // client went away
            }
        }
    });

    // One forwarder task per active subscription, keyed by topic for unsubscribe.
    let mut forwarders: HashMap<String, JoinHandle<()>> = HashMap::new();

    while let Some(Ok(message)) = ws_stream.next().await {
        match message {
            Message::Text(text) => {
                handle_control(&text, &hub, &outbox_tx, &mut forwarders).await;
            }
            Message::Close(_) => break,
            // axum answers Ping automatically; Pong/Binary are not part of the protocol.
            _ => {}
        }
    }

    // Client disconnected: tear down forwarders and the writer.
    for (_, handle) in forwarders {
        handle.abort();
    }
    writer.abort();
}

/// Parse and act on a single client control frame.
async fn handle_control(
    text: &str,
    hub: &Arc<Hub>,
    outbox_tx: &mpsc::Sender<Envelope>,
    forwarders: &mut HashMap<String, JoinHandle<()>>,
) {
    let control: Control = match serde_json::from_str(text) {
        Ok(control) => control,
        Err(err) => {
            let _ = outbox_tx
                .send(encode("", "error", json!({ "message": format!("invalid frame: {err}") })))
                .await;
            return;
        }
    };

    match control.control_type.as_str() {
        "subscribe" => {
            let Some(topic) = control.topic else {
                let _ = outbox_tx
                    .send(encode("", "error", json!({ "message": "subscribe requires a topic" })))
                    .await;
                return;
            };
            subscribe(hub, outbox_tx, forwarders, topic).await;
        }
        "unsubscribe" => {
            let Some(topic) = control.topic else {
                let _ = outbox_tx
                    .send(encode("", "error", json!({ "message": "unsubscribe requires a topic" })))
                    .await;
                return;
            };
            if let Some(handle) = forwarders.remove(&topic) {
                handle.abort();
            }
            let _ = outbox_tx.send(encode(&topic, "unsubscribed", json!({}))).await;
        }
        other => {
            let _ = outbox_tx
                .send(encode("", "error", json!({ "message": format!("unknown type: {other}") })))
                .await;
        }
    }
}

/// Register a subscription (idempotent) and ack it.
async fn subscribe(
    hub: &Arc<Hub>,
    outbox_tx: &mpsc::Sender<Envelope>,
    forwarders: &mut HashMap<String, JoinHandle<()>>,
    topic: String,
) {
    if !forwarders.contains_key(&topic) {
        let mut receiver = hub.subscribe(&topic);
        let tx = outbox_tx.clone();
        let handle = tokio::spawn(async move {
            loop {
                match receiver.recv().await {
                    Ok(frame) => {
                        if tx.send(frame).await.is_err() {
                            break; // connection closed
                        }
                    }
                    // Slow consumer dropped messages; keep going with newer ones.
                    Err(broadcast::error::RecvError::Lagged(_)) => continue,
                    Err(broadcast::error::RecvError::Closed) => break,
                }
            }
        });
        forwarders.insert(topic.clone(), handle);
    }
    // Ack after the forwarder is registered, so a client that waits for the ack
    // before triggering a publish is guaranteed to receive that event.
    let _ = outbox_tx.send(encode(&topic, "subscribed", json!({}))).await;
}

/// A client control frame. `payload` is accepted but unused for control types.
#[derive(Debug, Deserialize)]
struct Control {
    #[serde(rename = "type")]
    control_type: String,
    topic: Option<String>,
    #[allow(dead_code)]
    #[serde(default)]
    payload: Value,
}
