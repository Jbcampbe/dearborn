import { createApp } from "vue";
import { createPinia } from "pinia";
import App from "./App.vue";
import { router } from "./router";
import { setAuthBridge } from "./api/client";
import { useAuthStore } from "./stores/auth";

// Fonts (self-hosted via Fontsource) and the design-system layers. Order
// matters: tokens → base → primitives.
import "@fontsource-variable/inter";
import "@fontsource/jetbrains-mono/400.css";
import "./styles/tokens.css";
import "./styles/base.css";
import "./styles/ui.css";

const pinia = createPinia();
const app = createApp(App);
app.use(pinia);
app.use(router);

// Wire the API transport to the auth store over the bridge (avoids a store →
// api → store import cycle), then start boot: rehydrate the stored session or
// probe /auth/status while App.vue shows its splash.
const auth = useAuthStore(pinia);
setAuthBridge({
  getAccessToken: () => auth.accessToken,
  refresh: () => auth.refresh(),
  onAuthFailure: (message) => {
    void auth.logout(message);
  },
});
void auth.bootstrap();

app.mount("#app");
