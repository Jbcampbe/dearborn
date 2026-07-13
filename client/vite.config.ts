import { defineConfig } from "vite";
import vue from "@vitejs/plugin-vue";

// https://vite.dev/config/
export default defineConfig({
  plugins: [vue()],
  server: {
    port: 5173,
    // Dev-only: proxy API/health calls to the Rust server so the SPA can talk
    // to it without CORS. T-006 formalizes serving the built SPA from the binary.
    proxy: {
      "/health": "http://127.0.0.1:8787",
    },
  },
});
