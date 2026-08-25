<script setup lang="ts">
import { computed, onMounted, ref } from "vue";
import { RouterLink } from "vue-router";
import { useAuthStore } from "../stores/auth";
import { ApiError } from "../api/client";
import {
  createUser,
  listUsers,
  resetUserPassword,
  updateUser,
  type User,
} from "../api/users";
import AppIcon from "./AppIcon.vue";
import AppModal from "./AppModal.vue";
import ConfirmModal from "./ConfirmModal.vue";

// Admin user management ("Users"): every user with username, display name,
// role, and active state — deactivated rows stay visible, marked inactive.
//
// Flows: create, edit (display name + role), reset password (the admin types
// the new password and communicates it out of band), deactivate/reactivate.
// The destructive actions go through ConfirmModal. Every server rejection is
// surfaced **inline verbatim** — the lockout guards (last active admin,
// self-deactivation) and the 12-character password minimum are the server's
// to enforce; this screen never invents its own wording for them.
const auth = useAuthStore();

const loading = ref(true);
const loadError = ref<string | null>(null);
const users = ref<User[]>([]);

/** True while a row-level action (reactivate) is in flight for that id. */
const busyId = ref<string | null>(null);
/** Row-level action error (e.g. a guard refusal), shown above the table. */
const rowError = ref<string | null>(null);

async function load() {
  const token = auth.accessToken;
  if (token === null) {
    return;
  }
  loading.value = true;
  loadError.value = null;
  try {
    users.value = await listUsers(token);
  } catch (err) {
    if (err instanceof ApiError && err.isAuth) {
      auth.logout(`Token rejected (401): ${err.message}. Please re-enter it.`);
      return;
    }
    loadError.value = err instanceof Error ? err.message : "failed to load users";
  } finally {
    loading.value = false;
  }
}

onMounted(load);

function replaceUser(updated: User): void {
  users.value = users.value.map((u) => (u.id === updated.id ? updated : u));
}

// ---- Create ------------------------------------------------------------------

const createOpen = ref(false);
const createUsername = ref("");
const createDisplayName = ref("");
const createPassword = ref("");
const createRole = ref<"admin" | "user">("user");
const createBusy = ref(false);
const createError = ref<string | null>(null);

function openCreate(): void {
  createUsername.value = "";
  createDisplayName.value = "";
  createPassword.value = "";
  createRole.value = "user";
  createError.value = null;
  createOpen.value = true;
}

async function submitCreate(): Promise<void> {
  const token = auth.accessToken;
  if (token === null || createBusy.value) {
    return;
  }
  createBusy.value = true;
  createError.value = null;
  try {
    const user = await createUser(token, {
      username: createUsername.value.trim(),
      display_name: createDisplayName.value.trim(),
      password: createPassword.value,
      role: createRole.value,
    });
    users.value = [...users.value, user].sort((a, b) =>
      a.username.localeCompare(b.username),
    );
    createOpen.value = false;
  } catch (err) {
    if (err instanceof ApiError && err.isAuth) {
      auth.logout(`Token rejected (401): ${err.message}. Please re-enter it.`);
      return;
    }
    // Verbatim: e.g. "password must be at least 12 characters" or a duplicate
    // username conflict — the server's wording, never rewritten here.
    createError.value =
      err instanceof Error ? err.message : "could not create user";
  } finally {
    createBusy.value = false;
  }
}

// ---- Edit --------------------------------------------------------------------

const editUser = ref<User | null>(null);
const editDisplayName = ref("");
const editRole = ref<"admin" | "user">("user");
const editBusy = ref(false);
const editError = ref<string | null>(null);

function openEdit(user: User): void {
  editUser.value = user;
  editDisplayName.value = user.display_name;
  editRole.value = user.role;
  editError.value = null;
}

async function submitEdit(): Promise<void> {
  const token = auth.accessToken;
  const target = editUser.value;
  if (token === null || target === null || editBusy.value) {
    return;
  }
  editBusy.value = true;
  editError.value = null;
  try {
    const updated = await updateUser(token, target.id, {
      display_name: editDisplayName.value.trim(),
      role: editRole.value,
    });
    replaceUser(updated);
    // Keep the footer identity in sync when an admin edits their own record.
    if (auth.user !== null && auth.user.id === updated.id) {
      auth.user.display_name = updated.display_name;
      auth.user.role = updated.role;
    }
    editUser.value = null;
  } catch (err) {
    if (err instanceof ApiError && err.isAuth) {
      auth.logout(`Token rejected (401): ${err.message}. Please re-enter it.`);
      return;
    }
    // Verbatim: "cannot demote the last active admin" lands here untouched.
    editError.value =
      err instanceof Error ? err.message : "could not update user";
  } finally {
    editBusy.value = false;
  }
}

// ---- Reset password ----------------------------------------------------------

const resetUser = ref<User | null>(null);
const resetPassword = ref("");
const resetBusy = ref(false);
const resetError = ref<string | null>(null);

function openReset(user: User): void {
  resetUser.value = user;
  resetPassword.value = "";
  resetError.value = null;
}

async function submitReset(): Promise<void> {
  const token = auth.accessToken;
  const target = resetUser.value;
  if (token === null || target === null || resetBusy.value) {
    return;
  }
  resetBusy.value = true;
  resetError.value = null;
  try {
    await resetUserPassword(token, target.id, resetPassword.value);
    resetUser.value = null;
    rowError.value = null;
  } catch (err) {
    if (err instanceof ApiError && err.isAuth) {
      auth.logout(`Token rejected (401): ${err.message}. Please re-enter it.`);
      return;
    }
    // Verbatim: "password must be at least 12 characters" lands here.
    resetError.value =
      err instanceof Error ? err.message : "could not reset password";
  } finally {
    resetBusy.value = false;
  }
}

// ---- Deactivate / reactivate -------------------------------------------------

/**
 * The user pending deactivation confirmation. Reactivation is not destructive
 * and skips ConfirmModal; only deactivation asks.
 */
const deactivateTarget = ref<User | null>(null);
const deactivateBusy = ref(false);

async function confirmDeactivate(): Promise<void> {
  const token = auth.accessToken;
  const target = deactivateTarget.value;
  if (token === null || target === null || deactivateBusy.value) {
    return;
  }
  deactivateBusy.value = true;
  rowError.value = null;
  try {
    const updated = await updateUser(token, target.id, { active: false });
    replaceUser(updated);
    deactivateTarget.value = null;
  } catch (err) {
    if (err instanceof ApiError && err.isAuth) {
      auth.logout(`Token rejected (401): ${err.message}. Please re-enter it.`);
      return;
    }
    // Verbatim guard refusals: "you cannot deactivate your own account" /
    // "cannot deactivate the last active admin" surface above the table.
    rowError.value =
      err instanceof Error ? err.message : "could not deactivate user";
  } finally {
    deactivateBusy.value = false;
  }
}

async function reactivate(user: User): Promise<void> {
  const token = auth.accessToken;
  if (token === null || busyId.value !== null) {
    return;
  }
  busyId.value = user.id;
  rowError.value = null;
  try {
    replaceUser(await updateUser(token, user.id, { active: true }));
  } catch (err) {
    if (err instanceof ApiError && err.isAuth) {
      auth.logout(`Token rejected (401): ${err.message}. Please re-enter it.`);
      return;
    }
    rowError.value =
      err instanceof Error ? err.message : "could not reactivate user";
  } finally {
    busyId.value = null;
  }
}

const deactivateTargetName = computed(
  () => deactivateTarget.value?.display_name ?? "",
);
</script>

<template>
  <main class="page">
    <nav class="crumbs">
      <RouterLink class="crumb-home" :to="{ name: 'projects' }">Projects</RouterLink>
      <span class="sep">/</span>
      <span class="current">Users</span>
    </nav>

    <header class="head">
      <h1 class="page-title">Users</h1>
      <p class="page-sub">
        Everyone with access to this instance. All data stays shared — roles
        differ only in who may manage this list.
      </p>
    </header>

    <div v-if="loading" class="loading-stack" aria-label="Loading users">
      <div class="skeleton sk-block" />
      <div class="skeleton sk-block" />
    </div>
    <p v-else-if="loadError" class="banner banner-error" role="alert">{{ loadError }}</p>

    <template v-else>
      <p v-if="rowError" class="banner banner-error" role="alert">{{ rowError }}</p>

      <section class="card">
        <table class="user-table">
          <thead>
            <tr>
              <th>Username</th>
              <th>Display name</th>
              <th>Role</th>
              <th>Status</th>
              <th class="actions-head">Actions</th>
            </tr>
          </thead>
          <tbody>
            <tr v-for="u in users" :key="u.id" :class="{ inactive: !u.active }">
              <td class="mono">{{ u.username }}</td>
              <td>{{ u.display_name }}</td>
              <td>
                <span class="badge" :data-tone="u.role === 'admin' ? 'violet' : 'neutral'">
                  {{ u.role }}
                </span>
              </td>
              <td>
                <span v-if="!u.active" class="badge" data-tone="red">inactive</span>
                <span v-else class="status-active">active</span>
              </td>
              <td class="actions-cell">
                <button class="btn btn-sm" @click="openEdit(u)">Edit</button>
                <button class="btn btn-sm" @click="openReset(u)">Reset password</button>
                <button
                  v-if="u.active"
                  class="btn btn-sm btn-danger"
                  @click="deactivateTarget = u"
                >
                  Deactivate
                </button>
                <button
                  v-else
                  class="btn btn-sm"
                  :disabled="busyId === u.id"
                  @click="reactivate(u)"
                >
                  {{ busyId === u.id ? "Working…" : "Reactivate" }}
                </button>
              </td>
            </tr>
            <tr v-if="users.length === 0">
              <td colspan="5" class="empty-row">No users yet.</td>
            </tr>
          </tbody>
        </table>
      </section>

      <footer class="table-foot">
        <button class="btn btn-primary" @click="openCreate">
          <AppIcon name="plus" :size="14" />
          Add user
        </button>
      </footer>
    </template>

    <!-- Create -->
    <AppModal :open="createOpen" title="Add user" @close="createOpen = false">
      <form class="form" @submit.prevent="submitCreate">
        <p v-if="createError" class="banner banner-error" role="alert">{{ createError }}</p>
        <div>
          <label class="label" for="create-username">Username</label>
          <input
            id="create-username"
            v-model="createUsername"
            class="input mono-input"
            type="text"
            autocomplete="off"
            spellcheck="false"
            :disabled="createBusy"
          />
        </div>
        <div>
          <label class="label" for="create-display-name">Display name</label>
          <input
            id="create-display-name"
            v-model="createDisplayName"
            class="input"
            type="text"
            autocomplete="off"
            :disabled="createBusy"
          />
        </div>
        <div>
          <label class="label" for="create-password">Initial password</label>
          <input
            id="create-password"
            v-model="createPassword"
            class="input"
            type="password"
            autocomplete="new-password"
            :disabled="createBusy"
          />
          <p class="hint">At least 12 characters.</p>
        </div>
        <div>
          <label class="label" for="create-role">Role</label>
          <select id="create-role" v-model="createRole" class="input select" :disabled="createBusy">
            <option value="user">user</option>
            <option value="admin">admin</option>
          </select>
        </div>
      </form>
      <template #footer>
        <button class="btn" :disabled="createBusy" @click="createOpen = false">Cancel</button>
        <button class="btn btn-primary" :disabled="createBusy" @click="submitCreate">
          {{ createBusy ? "Creating…" : "Create user" }}
        </button>
      </template>
    </AppModal>

    <!-- Edit -->
    <AppModal
      :open="editUser !== null"
      :title="`Edit ${editUser?.username ?? ''}`"
      @close="editUser = null"
    >
      <form v-if="editUser !== null" class="form" @submit.prevent="submitEdit">
        <p v-if="editError" class="banner banner-error" role="alert">{{ editError }}</p>
        <div>
          <label class="label" for="edit-display-name">Display name</label>
          <input
            id="edit-display-name"
            v-model="editDisplayName"
            class="input"
            type="text"
            autocomplete="off"
            :disabled="editBusy"
          />
        </div>
        <div>
          <label class="label" for="edit-role">Role</label>
          <select id="edit-role" v-model="editRole" class="input select" :disabled="editBusy">
            <option value="user">user</option>
            <option value="admin">admin</option>
          </select>
        </div>
      </form>
      <template #footer>
        <button class="btn" :disabled="editBusy" @click="editUser = null">Cancel</button>
        <button class="btn btn-primary" :disabled="editBusy" @click="submitEdit">
          {{ editBusy ? "Saving…" : "Save changes" }}
        </button>
      </template>
    </AppModal>

    <!-- Reset password -->
    <AppModal
      :open="resetUser !== null"
      :title="`Reset password — ${resetUser?.username ?? ''}`"
      @close="resetUser = null"
    >
      <form v-if="resetUser !== null" class="form" @submit.prevent="submitReset">
        <p v-if="resetError" class="banner banner-error" role="alert">{{ resetError }}</p>
        <p class="hint">
          Type the new password here and share it with
          {{ resetUser.display_name }} out of band. Their current sessions are
          revoked.
        </p>
        <div>
          <label class="label" for="reset-password">New password</label>
          <input
            id="reset-password"
            v-model="resetPassword"
            class="input"
            type="password"
            autocomplete="new-password"
            :disabled="resetBusy"
          />
          <p class="hint">At least 12 characters.</p>
        </div>
      </form>
      <template #footer>
        <button class="btn" :disabled="resetBusy" @click="resetUser = null">Cancel</button>
        <button class="btn btn-primary" :disabled="resetBusy" @click="submitReset">
          {{ resetBusy ? "Working…" : "Set password" }}
        </button>
      </template>
    </AppModal>

    <!-- Deactivate (destructive → ConfirmModal) -->
    <ConfirmModal
      :open="deactivateTarget !== null"
      title="Deactivate user"
      :message="`Deactivate ${deactivateTargetName}? They will no longer be able to sign in. Their row stays in this list and they can be reactivated later.`"
      confirm-label="Deactivate"
      :busy="deactivateBusy"
      @confirm="confirmDeactivate"
      @cancel="deactivateTarget = null"
    />
  </main>
</template>

<style scoped>
.head {
  margin-bottom: var(--spacing-24);
}

.page-sub {
  margin-top: var(--spacing-4);
  font-size: var(--text-caption);
  color: var(--text-muted);
  max-width: 560px;
  line-height: 1.5;
}

.user-table {
  width: 100%;
  border-collapse: collapse;
  font-size: var(--text-caption);
}

.user-table th {
  text-align: left;
  padding: var(--spacing-8) var(--spacing-12);
  font-size: 11px;
  font-weight: var(--weight-medium);
  color: var(--text-faint);
  letter-spacing: 0.01em;
  border-bottom: 1px solid var(--border-hairline);
}

.user-table td {
  padding: var(--spacing-8) var(--spacing-12);
  border-bottom: 1px solid var(--border-hairline);
  color: var(--text-body);
}

.user-table tbody tr:last-child td {
  border-bottom: none;
}

.user-table .mono {
  font-family: var(--font-mono);
  font-size: 12px;
}

.user-table tr.inactive td {
  color: var(--text-faint);
}

.status-active {
  font-size: var(--text-label);
  color: var(--text-muted);
}

.actions-cell {
  text-align: right;
  white-space: nowrap;
}

.actions-cell .btn + .btn {
  margin-left: var(--spacing-8);
}

.actions-head {
  width: 1%;
  text-align: right;
}

.empty-row {
  text-align: center;
  color: var(--text-faint);
  padding: var(--spacing-20) 0;
}

.table-foot {
  margin-top: var(--spacing-16);
  display: flex;
  justify-content: flex-end;
}

.form {
  display: flex;
  flex-direction: column;
  gap: var(--spacing-12);
}

.mono-input {
  font-family: var(--font-mono);
  font-size: 12px;
}

.select {
  appearance: auto;
}

.hint {
  margin-top: var(--spacing-4);
  font-size: var(--text-label);
  color: var(--text-faint);
}
</style>
