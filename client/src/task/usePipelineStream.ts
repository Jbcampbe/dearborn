// WebSocket client composable for the task detail pipeline's live tail
// (T-563). Mirrors `dag/useDagStream.ts`: opens
// `GET /ws?token=<token>`, subscribes to `task:<id>`, then feeds every
// subsequent frame through the pure reducer (`./pipeline.ts`'s
// `applyPipelineFrame`) into a caller-provided reactive `PipelineState`. Owns
// the socket lifecycle (unsubscribe + close on unmount, bounded reconnect
// with backoff) — the reducer holds all the state logic, this file is just
// transport + wiring.
//
// ## Ordering: subscribe BEFORE the REST hydrate — deliberately the opposite
// of `DagEditorView.vue`'s own sequence
//
// `DagEditorView.vue` awaits its REST hydrate (`getEpic`/`getDag`) and only
// THEN constructs `useDagStream`. That's fine there: a missed `dag_updated`
// in the gap is just a staleness window a later mutation's own frame heals,
// not a correctness bug — the DAG is fully-replaced state, not an
// accumulating string.
//
// This stream is different: `text`/`error` frames accumulate into
// `PipelineState.liveLog`, and `./pipeline.ts`'s header comment works through
// why the REST snapshot and the live feed can overlap (D14's ~2s partial
// flush). The one property that makes the reconciliation in `pipeline.ts`
// (`mergeHydratedLog`) sound is that the client never MISSES a live event —
// which is only true if the subscription is already live before either REST
// call (`GET /tasks/{id}/runs`, `GET /runs/{id}`) is issued. So the caller
// (`TaskPipelinePanel.vue`) must construct this composable FIRST, then run
// its REST hydrate — not the other order. `PipelineState.liveLog` grows from
// the moment this composable starts applying frames regardless of whether
// the caller's hydrate has resolved yet; there is deliberately no internal
// buffering here beyond that — `pipeline.ts`'s reducer already treats
// "haven't reconciled against a REST snapshot yet" as a valid, displayable
// state (`liveLogReconciled: false`, still a true suffix of the real log).

import { getCurrentScope, onScopeDispose, ref, type Ref } from "vue";

import { applyPipelineFrame, type PipelineFrame, type PipelineState } from "./pipeline";

/** Connection lifecycle, surfaced to the view for a small status line. */
export type StreamStatus = "connecting" | "open" | "closed";

export interface PipelineStream {
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
 * Subscribe a reactive `PipelineState` to a task's live `RunEvent`/
 * `stage_changed` stream.
 *
 * @param taskId  the task to subscribe to (`task:<id>`) — NOT `epic:<id>`;
 *                §2.6's whole point is keeping this fine-grained firehose off
 *                the epic/project topics a board view subscribes to.
 * @param token   the bearer token (passed in the WS query string).
 * @param state   the reactive view model the reducer folds frames into.
 * @param status  an optional external status ref to drive; one is created if
 *                omitted.
 */
export function usePipelineStream(
  taskId: string,
  token: string,
  state: PipelineState,
  status: Ref<StreamStatus> = ref<StreamStatus>("connecting"),
): PipelineStream {
  const topic = `task:${taskId}`;

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
      let frame: PipelineFrame;
      try {
        frame = JSON.parse(event.data) as PipelineFrame;
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
        applyPipelineFrame(state, frame);
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
