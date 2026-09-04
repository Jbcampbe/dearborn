// WebSocket client composable for a node's multi-party session view.
//
// Mirrors `map/useMapStream.ts` (same transport pattern) but subscribes the
// single-topic `node:<id>` instead of `epic:<id>`: it opens
// `GET /ws?token=<token>`, waits for the `subscribed` ack, then feeds every
// subsequent frame through the pure reducer (`node/stream.ts`) into a
// caller-provided reactive `NodeStreamState`. Owns the socket lifecycle
// (unsubscribe + close on unmount, bounded reconnect with backoff).
//
// The reducer folds only persisted `message` frames and live `RunEvent`
// relays; frames missed while disconnected cannot be replayed, so every
// *re*-subscribe (not the first open) invokes the caller's `onResubscribe`
// hook — the view re-hydrates the session + transcript from REST to converge.

import { getCurrentScope, onScopeDispose, ref, type Ref } from "vue";

import { applyNodeFrame, type NodeFrame, type NodeStreamState } from "./stream";

/** Connection lifecycle, surfaced to the view for a small status line. */
export type StreamStatus = "connecting" | "open" | "closed";

export interface NodeStream {
  /** Live connection status. */
  status: Ref<StreamStatus>;
  /** Manually tear down. Also runs automatically if an effect scope is active. */
  close: () => void;
}

/** How many reconnect attempts before giving up, and the base backoff (ms). */
const MAX_RECONNECTS = 5;
const BACKOFF_BASE_MS = 500;

/**
 * Async token provider: mints/returns a fresh access token before each connect
 * attempt (including reconnects), so a socket reconnecting after a long idle
 * presents a live token instead of an expired one. Resolving `null` means the
 * caller is not authenticated; connecting then stops rather than retrying.
 */
export type TokenProvider = () => Promise<string | null>;

/** Build the `ws(s)://…/ws?token=…` URL from the current origin. */
function wsUrl(token: string): string {
  const proto = window.location.protocol === "https:" ? "wss:" : "ws:";
  return `${proto}//${window.location.host}/ws?token=${encodeURIComponent(token)}`;
}

/**
 * Subscribe a reactive `NodeStreamState` to a node's live session stream.
 *
 * @param nodeId    the node to subscribe to (`node:<id>`).
 * @param getToken  awaited on every connect attempt (including reconnects) to
 *                  obtain the access token for the WS query string.
 * @param state     the reactive view model the reducer folds frames into.
 * @param status    an optional external status ref to drive; one is created if
 *                  omitted. Passing the component's own ref avoids a `watch`
 *                  when this is called outside the setup scope (e.g. after
 *                  `await` in an `onMounted` handler).
 * @param onResubscribe  invoked after every `subscribed` ack except the first
 *                  (i.e. on reconnects), so the view can re-hydrate from REST
 *                  and heal frames missed while offline.
 */
export function useNodeStream(
  nodeId: string,
  getToken: TokenProvider,
  state: NodeStreamState,
  status: Ref<StreamStatus> = ref<StreamStatus>("connecting"),
  onResubscribe?: () => void,
): NodeStream {
  const topic = `node:${nodeId}`;

  let socket: WebSocket | null = null;
  let attempts = 0;
  let firstOpen = true;
  let reconnectTimer: ReturnType<typeof setTimeout> | null = null;
  let disposed = false;

  async function connect(): Promise<void> {
    if (disposed) {
      return;
    }
    status.value = "connecting";

    // Re-await the provider on every attempt so a reconnect after idle mints a
    // fresh token instead of replaying an expired one.
    let token: string | null;
    try {
      token = await getToken();
    } catch {
      scheduleReconnect();
      return;
    }
    if (token === null) {
      // Not authenticated (e.g. logged out); reconnecting cannot succeed.
      status.value = "closed";
      return;
    }

    let ws: WebSocket;
    try {
      ws = new WebSocket(wsUrl(token));
    } catch {
      scheduleReconnect();
      return;
    }
    socket = ws;

    ws.onopen = () => {
      ws.send(JSON.stringify({ type: "subscribe", topic }));
    };

    ws.onmessage = (event: MessageEvent<string>) => {
      let frame: NodeFrame;
      try {
        frame = JSON.parse(event.data) as NodeFrame;
      } catch {
        return;
      }
      if (frame.type === "subscribed") {
        attempts = 0;
        status.value = "open";
        // The first open follows the initial REST hydrate; only reconnects
        // need the resync (frames may have been missed while offline).
        if (firstOpen) {
          firstOpen = false;
        } else {
          onResubscribe?.();
        }
        return;
      }
      if (frame.type === "unsubscribed") {
        return;
      }
      if (frame.topic === topic) {
        applyNodeFrame(state, frame);
      }
    };

    ws.onerror = () => {
      // `onclose` always follows; let it drive reconnect.
    };

    ws.onclose = () => {
      socket = null;
      if (disposed) {
        status.value = "closed";
        return;
      }
      scheduleReconnect();
    };
  }

  function scheduleReconnect(): void {
    if (disposed || reconnectTimer !== null) {
      return;
    }
    if (attempts >= MAX_RECONNECTS) {
      status.value = "closed";
      return;
    }
    status.value = "connecting";
    const delay = BACKOFF_BASE_MS * 2 ** attempts;
    attempts += 1;
    reconnectTimer = setTimeout(() => {
      reconnectTimer = null;
      connect();
    }, delay);
  }

  function close(): void {
    disposed = true;
    if (reconnectTimer !== null) {
      clearTimeout(reconnectTimer);
      reconnectTimer = null;
    }
    const ws = socket;
    socket = null;
    if (ws !== null) {
      if (ws.readyState === WebSocket.OPEN) {
        try {
          ws.send(JSON.stringify({ type: "unsubscribe", topic }));
        } catch {
          // ignore — closing anyway
        }
      }
      ws.onclose = null;
      ws.close();
    }
    status.value = "closed";
  }

  connect();
  if (getCurrentScope()) {
    onScopeDispose(close);
  }

  return { status, close };
}
