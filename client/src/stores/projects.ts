import { ref } from "vue";
import { defineStore } from "pinia";
import { listProjects, type Project } from "../api/projects";

/**
 * Shared project list, consumed by the app-shell sidebar (navigation) and the
 * Projects view. Kept in a store so the two never fetch independently or drift
 * after a create. Callers handle auth bounces (401 → logout) themselves.
 */
export const useProjectsStore = defineStore("projects", () => {
  const projects = ref<Project[]>([]);
  const loaded = ref(false);

  async function load(token: string): Promise<void> {
    projects.value = await listProjects(token);
    loaded.value = true;
  }

  /** Prepend a freshly created project without a round trip. */
  function add(project: Project): void {
    projects.value = [project, ...projects.value];
  }

  return { projects, loaded, load, add };
});
