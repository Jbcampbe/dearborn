import { defineConfig } from "vite";
import vue from "@vitejs/plugin-vue";

// https://vite.dev/config/
export default defineConfig({
  plugins: [vue()],
  server: {
    port: 5173,
    // Dev-only: proxy the API the SPA calls to the Rust server (port 8787) so
    // `just dev` gives a full working app without CORS. In production the Rust
    // binary serves the built assets itself and no proxy is involved.
    proxy: {
      "/health": "http://127.0.0.1:8787",
      "/whoami": "http://127.0.0.1:8787",
      "/projects": "http://127.0.0.1:8787",
      // Epics + planning transcript REST surface (T-201/T-204). Singular
      // `/epic/:id` is a client route (see router) so it does not clash here.
      "/epics": "http://127.0.0.1:8787",
      // WebSocket — planning `RunEvent` live stream (T-202/T-204). `ws:true`
      // makes the dev proxy forward the Upgrade handshake to the Rust server.
      "/ws": { target: "ws://127.0.0.1:8787", ws: true },
    },
  },
});
