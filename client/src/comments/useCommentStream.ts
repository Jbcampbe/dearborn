// WebSocket client composable for the shared comment panel's live updates.
//
// Mirrors `map/useMapStream.ts` (same transport pattern, shared `epic:<id>`
// topic): opens `GET /ws?token=<token>`, subscribes to `epic:<id>`, waits for
// the `subscribed` ack, then feeds every subsequent frame through the pure
// reducer (`comments/stream.ts`) into a caller-provided reactive
// `CommentState`. Owns the socket lifecycle (unsubscribe + close on unmount,
// bounded reconnect with backoff). `comments_updated` is the only frame the
// reducer folds — the panel ignores the rest of the topic's traffic.
//
// A view that already holds an `epic:<id>` subscription (Map, Document) opens
// this alongside its own — one socket per surface, matching the per-view
// transport convention.

import { getCurrentScope, onScopeDispose, ref, type Ref } from "vue";

import { applyCommentFrame, type CommentFrame, type CommentState } from "./stream";
import type { Comment } from "../api/comments";

/** Connection lifecycle, surfaced to the view for a small status line. */
export type StreamStatus = "connecting" | "open" | "closed";

export interface CommentStream {
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

/** A frame filter: only comments anchored to this node/section are folded. */
export interface CommentScope {
  anchorKind: string;
  anchorId: string;
}

/**
 * Subscribe a reactive `CommentState` to an epic's live comment stream.
 *
 * @param epicId    the epic to subscribe to (`epic:<id>`).
 * @param getToken  awaited on every connect attempt (including reconnects) to
 *                  obtain the access token for the WS query string.
 * @param state     the reactive view model the reducer folds frames into.
 * @param status  an optional external status ref to drive; one is created if
 *                omitted. Passing the component's own ref avoids a `watch` when
 *                this is called outside the setup scope (e.g. after `await`).
 * @param scope   an optional anchor filter — the `comments_updated` frame
 *                carries the epic's FULL list, so a panel scoped to one anchor
 *                (its REST hydrate was filtered) must narrow frames the same
 *                way or a live frame would widen it back to the whole epic.
 */
export function useCommentStream(
  epicId: string,
  getToken: TokenProvider,
  state: CommentState,
  status: Ref<StreamStatus> = ref<StreamStatus>("connecting"),
  scope?: CommentScope,
): CommentStream {
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
      let frame: CommentFrame;
      try {
        frame = JSON.parse(event.data) as CommentFrame;
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
        if (scope !== undefined && frame.type === "comments_updated" && Array.isArray(frame.payload)) {
          frame = {
            ...frame,
            payload: (frame.payload as Comment[]).filter(
              (c) => c.anchor_kind === scope.anchorKind && c.anchor_id === scope.anchorId,
            ),
          };
        }
        applyCommentFrame(state, frame);
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
