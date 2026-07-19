import { createApp } from "vue";
import { createPinia } from "pinia";
import App from "./App.vue";
import { router } from "./router";

// Fonts (self-hosted via Fontsource) and the design-system layers. Order
// matters: tokens → base → primitives.
import "@fontsource-variable/inter";
import "@fontsource/jetbrains-mono/400.css";
import "./styles/tokens.css";
import "./styles/base.css";
import "./styles/ui.css";

const app = createApp(App);
app.use(createPinia());
app.use(router);
app.mount("#app");
