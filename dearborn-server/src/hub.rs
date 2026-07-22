//! In-process pub/sub hub for live WebSocket subscriptions.
//!
//! The hub is a `topic -> broadcast::Sender` registry. Server-side code publishes
//! an event to a topic; every WebSocket connection currently subscribed to that
//! topic receives it. Topics are opaque strings — the executor/planning tasks use
//! conventions like `epic:<id>` and `project:<id>` (see `CONVENTIONS.md`), but the
//! hub itself does not interpret them.
//!
//! Events are serialised **once** per publish into the wire envelope
//! `{ "topic", "type", "payload" }` and shared across subscribers as an
//! `Arc<str>`, so a fan-out to N connections is N cheap clones, not N re-encodes.
//!
//! Channels are created lazily on first subscribe and are retained for the life
//! of the process (topic cardinality is small — a bounded set of epics/projects).
//! Publishing to a topic with no subscribers is a no-op.

use std::collections::HashMap;
use std::sync::Mutex;

use serde_json::{json, Value};
use tokio::sync::broadcast;

/// How many undelivered messages a single subscriber may buffer before the
/// broadcast channel starts dropping the oldest (surfaced as a `Lagged` error,
/// which forwarders skip). Generous for planning `RunEvent` bursts.
const CHANNEL_CAPACITY: usize = 1024;

/// A serialised event envelope ready to write to a socket, shared across all
/// subscribers of a topic.
pub type Envelope = std::sync::Arc<str>;

/// Topic-based publish/subscribe broadcaster shared via [`crate::AppState`].
///
/// Cloneable-by-reference through the `Arc` held in `AppState`; internally
/// synchronised, so `publish` is callable from any handler or background task:
///
/// ```ignore
/// state.hub.publish("epic:123", "message", json!({ "text": "hello" }));
/// ```
#[derive(Default)]
pub struct Hub {
    channels: Mutex<HashMap<String, broadcast::Sender<Envelope>>>,
}

impl Hub {
    /// Create an empty hub.
    pub fn new() -> Hub {
        Hub::default()
    }

    /// Publish `payload` to everyone subscribed to `topic`.
    ///
    /// The event is delivered as `{ "topic": topic, "type": event_type,
    /// "payload": payload }`. Returns the number of subscribers it was delivered
    /// to (0 when the topic has no live subscribers — a no-op).
    pub fn publish(&self, topic: &str, event_type: &str, payload: Value) -> usize {
        let envelope = encode(topic, event_type, payload);
        let channels = self.channels.lock().expect("hub mutex poisoned");
        match channels.get(topic) {
            // `send` errors only when there are no receivers; treat as 0 delivered.
            Some(sender) => sender.send(envelope).unwrap_or(0),
            None => 0,
        }
    }

    /// Subscribe to `topic`, receiving every event published to it from now on.
    ///
    /// Lazily creates the topic's channel on first use. Used by the WS connection
    /// handler; not part of the public publish API.
    pub(crate) fn subscribe(&self, topic: &str) -> broadcast::Receiver<Envelope> {
        let mut channels = self.channels.lock().expect("hub mutex poisoned");
        channels
            .entry(topic.to_string())
            .or_insert_with(|| broadcast::channel(CHANNEL_CAPACITY).0)
            .subscribe()
    }
}

/// Serialise an event into the wire envelope, once, as a shareable `Arc<str>`.
pub(crate) fn encode(topic: &str, event_type: &str, payload: Value) -> Envelope {
    let value = json!({ "topic": topic, "type": event_type, "payload": payload });
    // Serialising a `serde_json::Value` cannot fail.
    let text = serde_json::to_string(&value).expect("event serialisation is infallible");
    std::sync::Arc::from(text.as_str())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::sync::broadcast::error::TryRecvError;

    #[test]
    fn encode_produces_topic_type_payload_envelope() {
        let env = encode("epic:1", "message", json!({ "text": "hi" }));
        let value: Value = serde_json::from_str(&env).unwrap();
        assert_eq!(
            value,
            json!({ "topic": "epic:1", "type": "message", "payload": { "text": "hi" } })
        );
    }

    #[test]
    fn publish_without_subscribers_is_a_noop() {
        let hub = Hub::new();
        assert_eq!(hub.publish("epic:1", "message", json!({})), 0);
    }

    #[test]
    fn subscriber_receives_only_its_topic() {
        let hub = Hub::new();
        let mut epic = hub.subscribe("epic:1");
        let mut other = hub.subscribe("epic:2");

        assert_eq!(hub.publish("epic:1", "message", json!({ "n": 1 })), 1);

        let got: Value = serde_json::from_str(&epic.try_recv().unwrap()).unwrap();
        assert_eq!(got["topic"], "epic:1");
        assert_eq!(got["payload"]["n"], 1);

        // The unrelated topic saw nothing.
        assert!(matches!(other.try_recv(), Err(TryRecvError::Empty)));
    }

    #[test]
    fn multiple_subscribers_all_receive() {
        let hub = Hub::new();
        let mut a = hub.subscribe("project:x");
        let mut b = hub.subscribe("project:x");
        assert_eq!(hub.publish("project:x", "kanban", json!({})), 2);
        assert!(a.try_recv().is_ok());
        assert!(b.try_recv().is_ok());
    }
}
