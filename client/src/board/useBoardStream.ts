// WebSocket client composable for the project kanban's live updates (T-401).
//
// Mirrors `dag/useDagStream.ts`: opens `GET /ws?token=<token>`, subscribes to
// `project:<id>`, waits for the `subscribed` ack, then feeds every subsequent
// frame through the pure reducer (`board/stream.ts`) into a caller-provided
// reactive `BoardState`. Owns the socket lifecycle (unsubscribe + close on
// unmount, bounded reconnect with backoff). The reducer holds all the state
// logic; this file is just transport + wiring.

import { getCurrentScope, onScopeDispose, ref, type Ref } from "vue";

import { applyBoardFrame, type BoardFrame, type BoardState } from "./stream";

/** Connection lifecycle, surfaced to the view for a small status line. */
export type StreamStatus = "connecting" | "open" | "closed";

export interface BoardStream {
  /** Live connection status. */
  status: Ref<StreamStatus>;
  /** Manually tear down. Also runs automatically if an effect scope is active. */
  close: () => void;
}

/** How many reconnect attempts before giving up, and the base backoff (ms). */
const MAX_RECONNECTS = 5;
const BACKOFF_BASE_MS = 500;

/** Build the `ws(s)://…/ws?token=…` URL from the current origin. */
function wsUrl(token: string): string {
  const proto = window.location.protocol === "https:" ? "wss:" : "ws:";
  return `${proto}//${window.location.host}/ws?token=${encodeURIComponent(token)}`;
}

/**
 * Subscribe a reactive `BoardState` to a project's live board stream.
 *
 * @param projectId  the project to subscribe to (`project:<id>`).
 * @param token      the bearer token (passed in the WS query string).
 * @param state      the reactive view model the reducer folds frames into.
 * @param status     an optional external status ref to drive; one is created if
 *                   omitted. Passing the component's own ref avoids a `watch`
 *                   when this is called outside the setup scope (e.g. after
 *                   `await`).
 */
export function useBoardStream(
  projectId: string,
  token: string,
  state: BoardState,
  status: Ref<StreamStatus> = ref<StreamStatus>("connecting"),
): BoardStream {
  const topic = `project:${projectId}`;

  let socket: WebSocket | null = null;
  let attempts = 0;
  let reconnectTimer: ReturnType<typeof setTimeout> | null = null;
  let disposed = false;

  function connect(): void {
    if (disposed) {
      return;
    }
    status.value = "connecting";

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
      let frame: BoardFrame;
      try {
        frame = JSON.parse(event.data) as BoardFrame;
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
        applyBoardFrame(state, frame);
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
