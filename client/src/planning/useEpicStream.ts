// WebSocket client composable for the planning stream (T-204).
//
// Opens `GET /ws?token=<token>` (browsers pass the token in the query string —
// see CONVENTIONS.md §Handshake auth), subscribes to `epic:<id>`, waits for the
// `subscribed` ack, then feeds every subsequent frame through the pure reducer
// (`stream.ts`) into a caller-provided reactive `PlanningState`. It owns the
// socket lifecycle: unsubscribe + close on unmount, and a bounded reconnect with
// backoff on an unexpected drop.
//
// The reducer holds all the state logic; this file is just transport + wiring,
// so it stays small and the risk-bearing folding is unit-tested in isolation.

import { getCurrentScope, onScopeDispose, ref, type Ref } from "vue";

import { applyFrame, type EpicFrame, type PlanningState } from "./stream";

/** Connection lifecycle, surfaced to the view for a small status line. */
export type StreamStatus = "connecting" | "open" | "closed";

export interface EpicStream {
  /** Live connection status. */
  status: Ref<StreamStatus>;
  /** Manually tear down. Also runs automatically if an effect scope is active. */
  close: () => void;
}

/** How many reconnect attempts before giving up, and the base backoff (ms). */
const MAX_RECONNECTS = 5;
const BACKOFF_BASE_MS = 500;

/**
 * Build the `ws(s)://…/ws?token=…` URL from the current origin so it works both
 * behind the Vite dev proxy (which forwards `/ws` with `ws:true`) and in
 * production (the Rust binary upgrades `/ws` itself).
 */
function wsUrl(token: string): string {
  const proto = window.location.protocol === "https:" ? "wss:" : "ws:";
  return `${proto}//${window.location.host}/ws?token=${encodeURIComponent(token)}`;
}

/**
 * Subscribe a reactive `PlanningState` to an epic's live planning stream.
 *
 * @param epicId  the epic to subscribe to (`epic:<id>`).
 * @param token   the bearer token (passed in the WS query string).
 * @param state   the reactive view model the reducer folds frames into.
 * @param status  an optional external status ref to drive (e.g. a component's);
 *                one is created if omitted. Passing the component's own ref
 *                avoids a `watch` when this is called outside the setup scope
 *                (e.g. after `await` in an `onMounted` handler).
 */
export function useEpicStream(
  epicId: string,
  token: string,
  state: PlanningState,
  status: Ref<StreamStatus> = ref<StreamStatus>("connecting"),
): EpicStream {
  const topic = `epic:${epicId}`;

  let socket: WebSocket | null = null;
  let attempts = 0;
  let reconnectTimer: ReturnType<typeof setTimeout> | null = null;
  // Set once the caller (or scope teardown) asks us to stop — suppresses reconnect.
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
      // Ask for the epic topic; frames start flowing after the `subscribed` ack.
      ws.send(JSON.stringify({ type: "subscribe", topic }));
    };

    ws.onmessage = (event: MessageEvent<string>) => {
      let frame: EpicFrame;
      try {
        frame = JSON.parse(event.data) as EpicFrame;
      } catch {
        return; // ignore anything that isn't a JSON frame
      }
      if (frame.type === "subscribed") {
        attempts = 0; // a clean subscribe resets the backoff budget
        status.value = "open";
        return;
      }
      if (frame.type === "unsubscribed") {
        return;
      }
      // Only fold frames for our topic (the socket is single-topic, but be safe).
      if (frame.topic === topic) {
        applyFrame(state, frame);
      }
    };

    ws.onerror = () => {
      // `onclose` always follows; let it drive reconnect so we don't double-fire.
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
      // Best-effort clean unsubscribe before closing (server also tears down on
      // socket close, so this is belt-and-suspenders).
      if (ws.readyState === WebSocket.OPEN) {
        try {
          ws.send(JSON.stringify({ type: "unsubscribe", topic }));
        } catch {
          // ignore — we're closing anyway
        }
      }
      ws.onclose = null; // don't let the manual close trigger a reconnect
      ws.close();
    }
    status.value = "closed";
  }

  connect();
  // Automatic cleanup when the owning effect scope unmounts — only if one is
  // active (this may be called after `await`, where no scope is current; the
  // caller then owns teardown via the returned `close`).
  if (getCurrentScope()) {
    onScopeDispose(close);
  }

  return { status, close };
}
