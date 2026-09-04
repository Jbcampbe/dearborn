// WebSocket client composable for the living-Document view's live updates.
//
// Mirrors `map/useMapStream.ts` (same transport pattern, shared `epic:<id>`
// topic): opens `GET /ws?token=<token>`, subscribes to `epic:<id>`, waits for
// the `subscribed` ack, then feeds every subsequent frame through the pure
// reducer (`document/stream.ts`) into a caller-provided reactive
// `DocumentStreamState`. Owns the socket lifecycle (unsubscribe + close on
// unmount, bounded reconnect with backoff).
//
// One extra edge over the map composable: a `document_updated` frame carries
// the version + section index but NOT the HTML, so the view must re-read the
// document over REST to heal it. Every `document_updated` frame therefore
// also fires the `onDocumentUpdated` callback (deduped while a reload is in
// flight — the view's load re-reads the latest version anyway).

import { getCurrentScope, onScopeDispose, ref, type Ref } from "vue";

import { applyDocumentFrame, type DocumentFrame, type DocumentStreamState } from "./stream";

/** Connection lifecycle, surfaced to the view for a small status line. */
export type StreamStatus = "connecting" | "open" | "closed";

export interface DocumentStream {
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
 * Subscribe a reactive `DocumentStreamState` to an epic's live stream.
 *
 * @param epicId             the epic to subscribe to (`epic:<id>`).
 * @param getToken           awaited on every connect attempt (including
 *                           reconnects) to obtain the access token for the WS
 *                           query string.
 * @param state              the reactive view model the reducer folds frames
 *                           into.
 * @param onDocumentUpdated  invoked on every `document_updated` frame (the
 *                           view re-reads the document's HTML over REST).
 * @param status             an optional external status ref to drive; one is
 *                           created if omitted. Passing the component's own
 *                           ref avoids a `watch` when this is called outside
 *                           the setup scope (e.g. after `await`).
 */
export function useDocumentStream(
  epicId: string,
  getToken: TokenProvider,
  state: DocumentStreamState,
  onDocumentUpdated: () => void,
  status: Ref<StreamStatus> = ref<StreamStatus>("connecting"),
): DocumentStream {
  const topic = `epic:${epicId}`;

  let socket: WebSocket | null = null;
  let attempts = 0;
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
      let frame: DocumentFrame;
      try {
        frame = JSON.parse(event.data) as DocumentFrame;
      } catch {
        return;
      }
      if (frame.type === "subscribed") {
        attempts = 0;
        status.value = "open";
        return;
      }
      if (frame.type === "unsubscribed") {
        return;
      }
      if (frame.topic === topic) {
        applyDocumentFrame(state, frame);
        if (frame.type === "document_updated") {
          onDocumentUpdated();
        }
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
