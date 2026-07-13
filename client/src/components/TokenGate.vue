<script setup lang="ts">
import { ref } from "vue";
import { useAuthStore } from "../stores/auth";

// Token-entry screen. Shown whenever no token is stored (or the stored one was
// rejected). Submitting persists the token via the auth store, which flips the
// app over to the authenticated view.
const auth = useAuthStore();
const entered = ref("");

function submit() {
  auth.setToken(entered.value);
}
</script>

<template>
  <section class="gate">
    <h1>Deerborn</h1>
    <p class="lead">Enter your access token to continue.</p>

    <p v-if="auth.authError" class="error" role="alert">{{ auth.authError }}</p>

    <form @submit.prevent="submit">
      <label for="token">Bearer token</label>
      <input
        id="token"
        v-model="entered"
        type="password"
        autocomplete="off"
        placeholder="DEERBORN_TOKEN"
        autofocus
      />
      <button type="submit" :disabled="entered.trim().length === 0">Continue</button>
    </form>
  </section>
</template>

<style scoped>
.gate {
  max-width: 24rem;
  margin: 6rem auto;
  padding: 0 1rem;
}
.lead {
  color: #555;
}
form {
  display: flex;
  flex-direction: column;
  gap: 0.5rem;
  margin-top: 1.5rem;
}
label {
  font-size: 0.85rem;
  font-weight: 600;
}
input {
  padding: 0.6rem 0.7rem;
  font-size: 1rem;
  border: 1px solid #ccc;
  border-radius: 6px;
}
button {
  padding: 0.6rem 0.7rem;
  font-size: 1rem;
  font-weight: 600;
  color: #fff;
  background: #2563eb;
  border: none;
  border-radius: 6px;
  cursor: pointer;
}
button:disabled {
  background: #9db8f0;
  cursor: not-allowed;
}
.error {
  padding: 0.6rem 0.75rem;
  color: #991b1b;
  background: #fee2e2;
  border: 1px solid #fca5a5;
  border-radius: 6px;
}
</style>
