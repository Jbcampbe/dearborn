import { createRouter, createWebHistory, type RouteRecordRaw } from "vue-router";

import ProjectsView from "../components/ProjectsView.vue";
import ProjectDetailView from "../components/ProjectDetailView.vue";
import PlanningView from "../components/PlanningView.vue";
import DagEditorView from "../components/DagEditorView.vue";
import EpicKanbanView from "../components/EpicKanbanView.vue";

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
  {
    // Singular `/epic/:id` for the same reason as `/project/:id`: the API owns
    // `/epics` (and the Vite dev proxy forwards it), so the singular path avoids
    // shadowing the REST namespace on a hard reload / deep link.
    path: "/epic/:id",
    name: "epic-planning",
    component: PlanningView,
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
