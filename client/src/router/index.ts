import { createRouter, createWebHistory, type RouteRecordRaw } from "vue-router";

import ProjectsView from "../components/ProjectsView.vue";
import ProjectDetailView from "../components/ProjectDetailView.vue";
import EpicDetailView from "../components/EpicDetailView.vue";
import MapView from "../components/MapView.vue";
import DagEditorView from "../components/DagEditorView.vue";
import EpicKanbanView from "../components/EpicKanbanView.vue";
import SettingsView from "../components/SettingsView.vue";
import UsersView from "../components/UsersView.vue";
import { useAuthStore } from "../stores/auth";

// Client-side routes. The top-level token gate lives in App.vue (an
// unauthenticated user sees the token screen regardless of route), so these
// routes are all "inside" the authenticated app.
const routes: RouteRecordRaw[] = [
  { path: "/", name: "projects", component: ProjectsView },
  {
    // Admin user management (multi-user auth epic). `/team` deliberately avoids
    // the API's `/users` namespace: axum registers `GET /users` (and the Vite
    // dev proxy forwards it), which would shadow this route on a hard reload /
    // deep link — the same reason `/agent-settings` exists alongside `/settings`.
    // The `beforeEnter` guard is cosmetic defense in depth; the server's `403`
    // on every /users route is the real control.
    path: "/team",
    name: "users",
    component: UsersView,
    beforeEnter: (_to, _from) => {
      const auth = useAuthStore();
      if (!auth.isAdmin) {
        return { name: "projects" };
      }
    },
  },
  {
    // Global agent settings (design doc §8). `/settings` is the API's own
    // namespace, so the client route is the distinct `/agent-settings` path to
    // avoid shadowing it on hard reload / deep link (same pattern as
    // `/project/:id`).
    path: "/agent-settings",
    name: "settings",
    component: SettingsView,
  },
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
  {
    // The planning-map workflow's own views (map graph, node sessions,
    // Document) land with their client tasks; for now `/epic/:id` redirects to
    // the epic's Details page. The API owns `/epics/:id` (GET/PATCH), so the
    // singular path avoids shadowing the REST namespace on a hard reload /
    // deep link (same pattern as `/project/:id`).
    path: "/epic/:id",
    name: "epic-details-default",
    redirect: (to) => ({ name: "epic-details", params: { id: to.params.id } }),
  },
  {
    // The planning-map graph view (wayfinder epic): the epic's decision nodes
    // colored by kind + computed readiness, dependency edges, click-to-open,
    // live via `map_updated` frames on `epic:<id>`. Singular `/epic/:id/map`
    // keeps it under the epic client route; the API owns `/epics/:id/map`.
    path: "/epic/:id/map",
    name: "epic-map",
    component: MapView,
    props: true,
  },
  {
    // Manual epic-details editor: view/edit the epic's title and product /
    // technical context. Singular `/epic/:id/details` keeps it under the epic
    // client route; the API owns `/epics/:id` (GET/PATCH).
    path: "/epic/:id/details",
    name: "epic-details",
    component: EpicDetailView,
    props: true,
  },
  {
    // The Ready-lane DAG editor for an epic (T-303). Singular `/epic/:id/tasks`
    // keeps it under the epic client route; the API owns `/epics/:id/tasks`.
    path: "/epic/:id/tasks",
    name: "epic-dag",
    component: DagEditorView,
    props: true,
  },
  {
    // The epic-detail task kanban (T-402): a task-lane view of the same DAG
    // the editor above uses. Singular `/epic/:id/board` keeps it under the
    // epic client route; the API owns `/epics`.
    path: "/epic/:id/board",
    name: "epic-board",
    component: EpicKanbanView,
    props: true,
  },
  // Unknown paths fall back to the projects list.
  { path: "/:pathMatch(.*)*", redirect: { name: "projects" } },
];

export const router = createRouter({
  history: createWebHistory(),
  routes,
});
