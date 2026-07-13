import { createRouter, createWebHistory, type RouteRecordRaw } from "vue-router";

import ProjectsView from "../components/ProjectsView.vue";
import ProjectDetailView from "../components/ProjectDetailView.vue";

// Client-side routes. The top-level token gate lives in App.vue (an
// unauthenticated user sees the token screen regardless of route), so these
// routes are all "inside" the authenticated app.
const routes: RouteRecordRaw[] = [
  { path: "/", name: "projects", component: ProjectsView },
  {
    // Singular `/project/:id` deliberately avoids the API's `/projects`
    // namespace: axum registers `GET /projects/:id` (and the Vite dev proxy
    // forwards `/projects`), both of which would otherwise shadow this route on
    // a hard reload / deep link. Name-based <RouterLink>s are unaffected.
    path: "/project/:id",
    name: "project-detail",
    component: ProjectDetailView,
    props: true,
  },
  // Unknown paths fall back to the projects list.
  { path: "/:pathMatch(.*)*", redirect: { name: "projects" } },
];

export const router = createRouter({
  history: createWebHistory(),
  routes,
});
