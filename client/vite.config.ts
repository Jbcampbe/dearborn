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
      // WebSocket (T-005) — planning/kanban live streams land here later.
      "/ws": { target: "ws://127.0.0.1:8787", ws: true },
    },
  },
});
