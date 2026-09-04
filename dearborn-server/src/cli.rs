//! The `dearborn` agent CLI — a thin, authenticated REST client (T-301
//! follow-up to the MCP retirement).
//!
//! Dearborn's agent runs (breakdown today; the per-node planning engines as
//! they land) mint a per-run capability token ([`crate::capability`]) and hand
//! the agent a shell command of the form
//!
//! ```text
//! dearborn --url <base> --token <cap> <verb> [flags]
//! ```
//!
//! Every invocation is a fresh process: the global `--url`/`--token` flags
//! travel with each call (an `export` would not survive between the agent's
//! tool calls). The CLI speaks plain HTTPS/JSON to Dearborn's REST API with an
//! `Authorization: Bearer <token>` header; the server's
//! [`crate::capability::authorize_cap_request`] allow-list keeps a scoped token
//! able to act **only on its own epic**, so the client itself carries no
//! privilege logic — the epic is never an argument, it is resolved from the
//! token's scope via `GET /auth/capability`.
//!
//! The verbs (mirrored by the breakdown prompt in [`crate::breakdown`], and
//! by the per-node planning engines as they land):
//!
//! - `task create --title ... [--description ...] [--acceptance ...] [--blocks id1,id2]`
//! - `task link BLOCKER BLOCKED`
//! - `dag` — print the epic's current task DAG
//! - `node create --kind ... --title ... [--question ...] [--task-mode afk|hitl]
//!   [--blocked-by id1,id2] [--blocks id1,id2]` — planning-map node creation
//! - `node link BLOCKER BLOCKED` — planning-map dependency edge (cycle-rejected)
//! - `node resolve NODE [--gist "..."] [--document PATH --base-version N]
//!   [--graduate "kind=grilling; title=...; question=..."]...
//!   [--out-of-scope "title=...; reason=..."]...
//!   [--update "id=NODE_ID; state=...; ..."]... [--trim-fog "..."] — the grilling
//!   resolution bundle (wayfinder epic §6): record the decision, fold the
//!   document edit in under the per-epic semaphore, graduate fog into new
//!   frontier nodes, rule things out of scope, update affected nodes — one
//!   call, HITL kinds only (grilling/prototype)
//! - `map` — print the epic's full planning map (nodes + computed frontier/blocked + prose)
//! - `map set-destination|set-notes|set-fog|set-out-of-scope "TEXT"` — the four wayfinder prose fields
//! - `document pull PATH` — write the epic's living HTML document to a scratch
//!   workspace file (for the harness's native Edit/Write); prints `{ "path",
//!   "version", "html": null, "epic_id" }` (no HTML in tool output — the file
//!   carries it)
//! - `document sync PATH --base-version N [--node id]` — commit the edited
//!   scratch file as a new version: per-epic semaphore, base-version check
//!   (a stale base is a clean 409 for re-read/retry), version + section-index
//!   persistence, `document_updated` on `epic:<id>`
//! - `comment post (--anchor node|section --id ANCHOR_ID | --thread THREAD_ID)
//!   --body "TEXT"` — post a threaded comment anchored to a map node or a
//!   Document section, or reply into an existing thread (the agent's reply
//!   path); attribution comes from the token (a capability token posts as
//!   the agent, `is_agent = 1`)
//! - `comment list [--anchor-kind node|section --anchor-id ANCHOR_ID]` — the
//!   epic's comments (optionally narrowed to one anchor)
//! - `comment resolve COMMENT` — resolve a comment's whole thread
//! - `comment promote COMMENT --kind grilling|research|prototype
//!   [--title "..."] [--question "..."]` — promote the comment's whole
//!   thread into a new open frontier node of the chosen kind (wayfinder epic
//!   §9 promote-to-node), stamping `promoted_node_id` on the source thread;
//!   an absent `--title` is derived from the thread's head comment. HITL
//!   phases only (it creates a map node)
//! - `scope` — print the token's own capability scope (`GET /auth/capability`)
//!
//! Output contract (assumed by the prompt text and the breakdown
//! `dag_write_failed` guard, which greps run output for [`ERROR_PREFIX`]):
//! every verb prints the API's JSON on stdout on success, and on any failure
//! prints `dearborn: <error>` (see [`ERROR_PREFIX`]) to stderr and exits
//! non-zero.

use serde_json::{json, Value};

/// Percent-encode a query-string value (anchor ids are ULIDs — plain — but
/// never assume: keep the encoding total and cheap).
fn urlencode(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for byte in value.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(byte as char)
            }
            _ => out.push_str(&format!("%{byte:02X}")),
        }
    }
    out
}

/// The substring every CLI failure prints to stderr. [`crate::breakdown`]
/// greps agent run output for exactly this marker to tell a failed DAG write
/// (fatal: the epic must stay in `Planning`) from harness-side noise.
pub const ERROR_PREFIX: &str = "dearborn: ";

/// A CLI failure: an HTTP status (when the API answered) plus a message.
///
/// `Display` renders only the message; the binary's error path formats the
/// full `dearborn: <error>` line around it.
#[derive(Debug)]
pub struct CliError {
    /// HTTP status of the API response, when one arrived.
    pub status: Option<u16>,
    /// Human-readable failure message.
    pub message: String,
}

impl std::fmt::Display for CliError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.status {
            Some(status) => write!(f, "{status}: {}", self.message),
            None => f.write_str(&self.message),
        }
    }
}

impl CliError {
    fn transport(err: reqwest::Error) -> CliError {
        CliError {
            status: None,
            message: err.to_string(),
        }
    }
}

/// The authenticated REST client the verbs run through. Cheap to construct —
/// each CLI invocation builds exactly one.
pub struct CliClient {
    /// Dearborn's base URL (e.g. `http://127.0.0.1:8787`), no trailing slash.
    base_url: String,
    /// The per-run capability token, sent as `Authorization: Bearer`.
    token: String,
    http: reqwest::Client,
}

impl CliClient {
    /// Build a client for `base_url` authenticated with `token`.
    pub fn new(base_url: &str, token: &str) -> Result<CliClient, CliError> {
        let http = reqwest::Client::builder()
            .build()
            .map_err(CliError::transport)?;
        Ok(CliClient {
            base_url: base_url.trim_end_matches('/').to_string(),
            token: token.to_string(),
            http,
        })
    }

    fn url(&self, path: &str) -> String {
        format!("{}{path}", self.base_url)
    }

    /// Perform one authenticated request and decode the JSON body.
    ///
    /// Error envelope: every API failure renders
    /// `{ "error": { "code", "message" } }` (see [`crate::error`]) — the
    /// message is surfaced verbatim so the agent sees the server's exact
    /// complaint (e.g. a cycle rejection or the list of valid task ids).
    async fn request(
        &self,
        method: reqwest::Method,
        path: &str,
        body: Option<Value>,
    ) -> Result<Value, CliError> {
        let mut req = self
            .http
            .request(method, self.url(path))
            .bearer_auth(&self.token);
        if let Some(body) = body {
            req = req.json(&body);
        }
        let response = req.send().await.map_err(CliError::transport)?;
        let status = response.status();
        let text = response.text().await.map_err(CliError::transport)?;

        if !status.is_success() {
            let message = serde_json::from_str::<Value>(&text)
                .ok()
                .and_then(|v| {
                    v["error"]["message"]
                        .as_str()
                        .map(|m| m.to_string())
                })
                .unwrap_or_else(|| {
                    // Non-JSON body (proxy page, empty 401): show a truncated
                    // raw body so the agent still gets something actionable.
                    let snippet: String = text.chars().take(200).collect();
                    if snippet.is_empty() {
                        format!("HTTP {status} with an empty body")
                    } else {
                        snippet
                    }
                });
            return Err(CliError {
                status: Some(status.as_u16()),
                message,
            });
        }

        serde_json::from_str(&text).map_err(|err| CliError {
            status: Some(status.as_u16()),
            message: format!("malformed JSON from the API: {err}"),
        })
    }

    /// Resolve the token's capability scope (`GET /auth/capability`). Every
    /// epic-addressed verb derives its path epic from this — the scope is a
    /// property of the token, never of the agent's arguments.
    pub async fn scope(&self) -> Result<Value, CliError> {
        self.request(reqwest::Method::GET, "/auth/capability", None)
            .await
    }

    async fn epic_id(&self) -> Result<String, CliError> {
        let scope = self.scope().await?;
        Ok(scope["epic_id"].as_str().unwrap_or_default().to_string())
    }

    /// `task create` — `POST /epics/{scoped epic}/tasks`. Returns the created
    /// task (its `id` is what later `task link` calls must copy verbatim).
    pub async fn task_create(
        &self,
        title: &str,
        description: Option<&str>,
        acceptance: Option<&str>,
        blocks: &[String],
    ) -> Result<Value, CliError> {
        let epic_id = self.epic_id().await?;
        let body = json!({
            "title": title,
            "description": description,
            "acceptance": acceptance,
            "blocks": blocks,
        });
        self.request(
            reqwest::Method::POST,
            &format!("/epics/{epic_id}/tasks"),
            Some(body),
        )
        .await
    }

    /// `task link` — `POST /epics/{scoped epic}/dependencies`, wiring
    /// `blocker → blocked` (the blocker must finish first). Both tasks must
    /// belong to the scoped epic; cycles are rejected by the server.
    pub async fn task_link(&self, blocker_id: &str, blocked_id: &str) -> Result<Value, CliError> {
        let epic_id = self.epic_id().await?;
        let body = json!({
            "blocker_id": blocker_id,
            "blocked_id": blocked_id,
        });
        self.request(
            reqwest::Method::POST,
            &format!("/epics/{epic_id}/dependencies"),
            Some(body),
        )
        .await
    }

    /// `dag` — `GET /epics/{scoped epic}/dag`, the current task DAG with
    /// computed per-node readiness.
    pub async fn dag(&self) -> Result<Value, CliError> {
        let epic_id = self.epic_id().await?;
        self.request(reqwest::Method::GET, &format!("/epics/{epic_id}/dag"), None)
            .await
    }

    // ---- planning-map verbs (wayfinder epic §10) ---------------------------

    /// `node create` — `POST /epics/{scoped epic}/map-nodes`. Creates a
    /// decision node (`kind`: grilling|research|prototype|task; `task_mode`
    /// afk|hitl is required for kind=task, fixed at creation). `blocked_by`
    /// lists ids of existing nodes that block the new node (the graduation
    /// shape); `blocks` — matching `task create`'s convention — lists ids of
    /// existing nodes the new node blocks. Returns the created node.
    pub async fn node_create(
        &self,
        kind: &str,
        title: &str,
        question: Option<&str>,
        task_mode: Option<&str>,
        blocked_by: &[String],
        blocks: &[String],
    ) -> Result<Value, CliError> {
        let epic_id = self.epic_id().await?;
        let body = json!({
            "kind": kind,
            "title": title,
            "question": question,
            "task_mode": task_mode,
            "blocked_by": blocked_by,
            "blocks": blocks,
        });
        self.request(
            reqwest::Method::POST,
            &format!("/epics/{epic_id}/map-nodes"),
            Some(body),
        )
        .await
    }

    /// `node link` — `POST /epics/{scoped epic}/map-node-dependencies`,
    /// wiring `blocker → blocked` (the blocker must settle first). Both map
    /// nodes must belong to the scoped epic; cycles are rejected by the
    /// server with 409.
    pub async fn node_link(&self, blocker_id: &str, blocked_id: &str) -> Result<Value, CliError> {
        let epic_id = self.epic_id().await?;
        let body = json!({
            "blocker_id": blocker_id,
            "blocked_id": blocked_id,
        });
        self.request(
            reqwest::Method::POST,
            &format!("/epics/{epic_id}/map-node-dependencies"),
            Some(body),
        )
        .await
    }

    /// `node resolve` — `PATCH /epics/{scoped epic}/map-nodes/{node}`, the
    /// minimal resolution state transition: `state = "resolved"` plus the
    /// optional one-line `gist`. (The rich grilling resolution bundle is
    /// [`Self::node_resolve_bundle`](Self::node_resolve_bundle); this minimal
    /// surface remains for simple state flips.)
    pub async fn node_resolve(&self, node_id: &str, gist: Option<&str>) -> Result<Value, CliError> {
        let epic_id = self.epic_id().await?;
        let body = match gist {
            Some(gist) => json!({ "state": "resolved", "gist": gist }),
            None => json!({ "state": "resolved" }),
        };
        self.request(
            reqwest::Method::PATCH,
            &format!("/epics/{epic_id}/map-nodes/{node_id}"),
            Some(body),
        )
        .await
    }

    /// `node resolve NODE [flags]` — the grilling resolution bundle (wayfinder
    /// epic §6): one call that records the decision (gist + resolved), folds
    /// the Document edit in (a `document sync` under the per-epic write
    /// semaphore, base-version checked), graduates fog into new frontier
    /// nodes (blocked by this node), rules things out of scope (create+close
    /// an `out_of_scope` node + prose line), and updates/invalidate affected
    /// nodes. `resolution` is the request body the binary assembles from the
    /// verb's flags. A stale document `base_version` is a clean `409` —
    /// re-pull, re-edit, retry. Only HITL kinds (grilling/prototype) may
    /// resolve through this surface; AFK kinds are refused by the server.
    pub async fn node_resolve_bundle(
        &self,
        node_id: &str,
        resolution: &Value,
    ) -> Result<Value, CliError> {
        let epic_id = self.epic_id().await?;
        self.request(
            reqwest::Method::POST,
            &format!("/epics/{epic_id}/map-nodes/{node_id}/resolve"),
            Some(resolution.clone()),
        )
        .await
    }

    /// `map` — `GET /epics/{scoped epic}/map`, the epic's full planning map:
    /// the four prose fields plus nodes with computed frontier/blocked state.
    pub async fn map(&self) -> Result<Value, CliError> {
        let epic_id = self.epic_id().await?;
        self.request(reqwest::Method::GET, &format!("/epics/{epic_id}/map"), None)
            .await
    }

    /// `map set-destination|set-notes|set-fog|set-out-of-scope` —
    /// `PATCH /epics/{scoped epic}/map` with the one prose field. `field` is
    /// the API's JSON key (`destination` | `notes` | `not_yet_specified` |
    /// `out_of_scope`); the binary maps the verb spellings onto it.
    pub async fn map_set_prose(&self, field: &str, text: &str) -> Result<Value, CliError> {
        let epic_id = self.epic_id().await?;
        let body = json!({ field: text });
        self.request(
            reqwest::Method::PATCH,
            &format!("/epics/{epic_id}/map"),
            Some(body),
        )
        .await
    }

    // ---- living-document verbs (wayfinder epic §10, Phase 3) ----------------

    /// `document pull PATH` — `GET /epics/{scoped epic}/document`, the epic's
    /// living document plus its section index. Returns the raw API JSON (the
    /// binary is responsible for writing `html` to the scratch file at `path`
    /// and stamping the version alongside it for the sync round trip).
    pub async fn document(&self) -> Result<Value, CliError> {
        let epic_id = self.epic_id().await?;
        self.request(
            reqwest::Method::GET,
            &format!("/epics/{epic_id}/document"),
            None,
        )
        .await
    }

    /// `document sync PATH --base-version N` — `POST /epics/{scoped
    /// epic}/document/sync` with the edited scratch file's HTML and the
    /// version it was read at. Takes the server's per-epic write semaphore;
    /// a stale base version is a `409` (re-read and retry). `node_id` is the
    /// optional provenance stamp for the sections this sync touches.
    pub async fn document_sync(
        &self,
        html: &str,
        base_version: i64,
        node_id: Option<&str>,
    ) -> Result<Value, CliError> {
        let epic_id = self.epic_id().await?;
        let body = json!({
            "html": html,
            "base_version": base_version,
            "node_id": node_id,
        });
        self.request(
            reqwest::Method::POST,
            &format!("/epics/{epic_id}/document/sync"),
            Some(body),
        )
        .await
    }

    /// `document pull PATH` — fetch the living document and write its HTML to
    /// the scratch workspace file at `path` for the harness's native Edit/
    /// Write (big HTML through file tools, not tool-args). Returns
    /// `{ path, version, updated_at, epic_id }` — `version` is the base
    /// version the follow-up [`Self::document_sync_file`](Self::document_sync_file)
    /// must carry (0 before the first sync).
    pub async fn document_pull(&self, path: &std::path::Path) -> Result<Value, CliError> {
        let doc = self.document().await?;
        let html = doc["html"].as_str().unwrap_or("");
        std::fs::write(path, html).map_err(|err| CliError {
            status: None,
            message: format!("failed to write {}: {err}", path.display()),
        })?;
        Ok(json!({
            "path": path.display().to_string(),
            "version": doc["version"],
            "updated_at": doc["updated_at"],
            "epic_id": doc["epic_id"],
        }))
    }

    /// `document sync PATH --base-version N` — read the edited scratch file
    /// and commit it as a new document version. A stale `base_version` is a
    /// `409` here: re-pull, re-edit, retry. See
    /// [`Self::document_sync`](Self::document_sync) for the write itself.
    pub async fn document_sync_file(
        &self,
        path: &std::path::Path,
        base_version: i64,
        node_id: Option<&str>,
    ) -> Result<Value, CliError> {
        let html = std::fs::read_to_string(path).map_err(|err| CliError {
            status: None,
            message: format!("failed to read {}: {err}", path.display()),
        })?;
        self.document_sync(&html, base_version, node_id).await
    }

    // ---- comment verbs (wayfinder epic §9) --------------------------------

    /// `comment post` — `POST /epics/{scoped epic}/comments`. Starts a thread
    /// under an anchor (`anchor_kind`: node|section + `anchor_id`) or replies
    /// into one (`thread_id`; the anchor is inherited from the thread — an
    /// agent reply never re-anchors it). Attribution is the token's identity:
    /// a capability token posts as the agent (`is_agent = 1`, no human
    /// author). Returns the created comment (its `thread_id` is what replies
    /// and a later resolve copy).
    pub async fn comment_post(
        &self,
        anchor_kind: Option<&str>,
        anchor_id: Option<&str>,
        body: &str,
        thread_id: Option<&str>,
    ) -> Result<Value, CliError> {
        let epic_id = self.epic_id().await?;
        let body = json!({
            "anchor_kind": anchor_kind,
            "anchor_id": anchor_id,
            "body": body,
            "thread_id": thread_id,
        });
        self.request(
            reqwest::Method::POST,
            &format!("/epics/{epic_id}/comments"),
            Some(body),
        )
        .await
    }

    /// `comment list [--anchor-kind node|section --anchor-id ID]` —
    /// `GET /epics/{scoped epic}/comments`, the epic's comments (optionally
    /// narrowed to one anchor).
    pub async fn comment_list(
        &self,
        anchor_kind: Option<&str>,
        anchor_id: Option<&str>,
    ) -> Result<Value, CliError> {
        let epic_id = self.epic_id().await?;
        let mut path = format!("/epics/{epic_id}/comments");
        match (anchor_kind, anchor_id) {
            (Some(kind), Some(id)) => {
                path += &format!(
                    "?anchor_kind={}&anchor_id={}",
                    urlencode(kind),
                    urlencode(id)
                );
            }
            _ => {}
        }
        self.request(reqwest::Method::GET, &path, None).await
    }

    /// `comment resolve COMMENT` — `POST
    /// /epics/{scoped epic}/comments/{comment}/resolve`, resolving the
    /// comment's whole thread. Returns the resolved thread.
    pub async fn comment_resolve(&self, comment_id: &str) -> Result<Value, CliError> {
        let epic_id = self.epic_id().await?;
        self.request(
            reqwest::Method::POST,
            &format!("/epics/{epic_id}/comments/{comment_id}/resolve"),
            None,
        )
        .await
    }

    /// `comment promote COMMENT --kind KIND [--title "..."] [--question
    /// "..."]` — `POST /epics/{scoped epic}/comments/{comment}/promote`,
    /// promoting the comment's whole thread into a new open frontier node of
    /// `kind` (grilling|research|prototype) carrying the optional extra
    /// context, stamping `promoted_node_id` on the thread. Returns
    /// `{ node, thread }` — the node is on the map's frontier; the thread's
    /// comments now name it. A HITL-phase token only (it reshapes the map).
    pub async fn comment_promote(
        &self,
        comment_id: &str,
        kind: &str,
        title: Option<&str>,
        question: Option<&str>,
    ) -> Result<Value, CliError> {
        let epic_id = self.epic_id().await?;
        let body = json!({
            "kind": kind,
            "title": title,
            "question": question,
        });
        self.request(
            reqwest::Method::POST,
            &format!("/epics/{epic_id}/comments/{comment_id}/promote"),
            Some(body),
        )
        .await
    }
}

// ---- tests ----------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::capability::now_ms;
    use crate::users::{self, Role};
    use crate::{app, AppState, Config, Db};
    use std::path::PathBuf;

    /// Boot state + router and serve it on a random loopback port, so the CLI
    /// client is exercised over real HTTP against the real API (criterion:
    /// the CLI client is unit-tested against the API — not a mock).
    async fn boot() -> (AppState, CliClient) {
        let db = Db::connect(":memory:").await.unwrap();
        db.run_migrations().await.unwrap();
        let state = AppState::new(Config::for_test(), db);
        let router = app(state.clone());

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, router).await.unwrap();
        });

        let client = CliClient::new(&format!("http://{addr}"), "token-to-be-replaced").unwrap();
        (state, client)
    }

    /// Insert a project + epic; return ids.
    async fn seed_epic(state: &AppState) -> (String, String) {
        let conn = state.db.conn();
        let now = now_ms();
        let project_id = ulid::Ulid::new().to_string();
        conn.execute(
            "INSERT INTO project (id, name, repo_url, clone_path, clone_status, created_at, updated_at) \
             VALUES (?1, 'P', 'https://example.com/p.git', NULL, 'ready', ?2, ?2)",
            libsql::params![project_id.clone(), now],
        )
        .await
        .unwrap();
        let epic_id = ulid::Ulid::new().to_string();
        conn.execute(
            "INSERT INTO epic (id, project_id, title, status, created_at, updated_at) \
             VALUES (?1, ?2, 'E', 'Planning', ?3, ?3)",
            libsql::params![epic_id.clone(), project_id.clone(), now],
        )
        .await
        .unwrap();
        (project_id, epic_id)
    }

    /// Mint a capability scoped to the epic and a matching client. The guard
    /// is returned and must stay alive in the test — dropping it revokes the
    /// token. `phase` is the minting run's phase: only the HITL
    /// grilling/prototype phases may call the map-reshaping verbs (see
    /// `authorize_cap_request`).
    fn scoped(
        state: &AppState,
        client: &CliClient,
        project_id: &str,
        epic_id: &str,
        phase: &str,
    ) -> (CliClient, crate::capability::CapabilityGuard) {
        let guard = state.caps.mint(
            epic_id.to_string(),
            project_id.to_string(),
            phase.into(),
            PathBuf::from("/tmp"),
        );
        let cli = CliClient::new(
            // Reuse the client's base URL; swap only the token.
            client.base_url.trim_end_matches('/'),
            guard.token(),
        )
        .unwrap();
        (cli, guard)
    }

    fn rendered(err: &CliError) -> String {
        format!("{ERROR_PREFIX}{err}")
    }

    #[tokio::test]
    async fn scope_verb_names_the_tokens_capability() {
        let (state, client) = boot().await;
        let (project_id, epic_id) = seed_epic(&state).await;
        let (cli, _guard) = scoped(&state, &client, &project_id, &epic_id, "breakdown");

        let scope = cli.scope().await.unwrap();
        assert_eq!(scope["kind"], "capability");
        assert_eq!(scope["epic_id"], epic_id.as_str());
        assert_eq!(scope["project_id"], project_id.as_str());
        assert_eq!(scope["phase"], "breakdown");
        assert!(scope["expires_at"].as_i64().is_some());
    }

    #[tokio::test]
    async fn task_create_and_link_and_dag_round_trip_through_the_api() {
        let (state, client) = boot().await;
        let (project_id, epic_id) = seed_epic(&state).await;
        let (cli, _guard) = scoped(&state, &client, &project_id, &epic_id, "breakdown");

        // Create two tasks; the first carries `--blocks` for the second.
        let blocker = cli
            .task_create("Blocker", Some("End-to-end slice one"), Some("Demoable"), &[])
            .await
            .unwrap();
        let blocked = cli
            .task_create("Blocked", None, None, &[blocker["id"].as_str().unwrap().to_string()])
            .await
            .unwrap();

        assert_eq!(blocker["epic_id"], epic_id.as_str());
        assert_eq!(blocker["title"], "Blocker");
        assert_eq!(blocked["title"], "Blocked");

        // `task link` wires an additional edge (blocker → blocked is already
        // there via --blocks; link the blocked task back under a second task
        // to exercise the verb itself). Create a third task first.
        let third = cli.task_create("Third", None, None, &[]).await.unwrap();
        let edge = cli
            .task_link(blocked["id"].as_str().unwrap(), third["id"].as_str().unwrap())
            .await
            .unwrap();
        assert_eq!(edge["blocker_id"], blocked["id"].as_str().unwrap());
        assert_eq!(edge["blocked_id"], third["id"].as_str().unwrap());

        // `dag` reflects the two edges.
        let dag = cli.dag().await.unwrap();
        let nodes = dag["nodes"].as_array().unwrap();
        assert_eq!(nodes.len(), 3);
        let edges = dag["edges"].as_array().unwrap();
        assert_eq!(edges.len(), 2);
    }

    #[tokio::test]
    async fn failed_verbs_carry_the_error_marker_and_the_api_message() {
        let (state, client) = boot().await;
        let (project_id, epic_id) = seed_epic(&state).await;
        let (cli, _guard) = scoped(&state, &client, &project_id, &epic_id, "breakdown");

        // Missing required title → the API's 400 message, verbatim, behind
        // the `dearborn: ` marker the breakdown guard greps for.
        let err = cli.task_create("   ", None, None, &[]).await.unwrap_err();
        assert_eq!(err.status, Some(400));
        assert!(err.message.contains("`title` is required"));
        let line = rendered(&err);
        assert!(line.starts_with(crate::breakdown::DEARBORN_CLI_ERROR_MARKER));

        // Linking a task that does not belong to the epic → 400 with the
        // server's message (the prompt tells the agent to read and retry).
        let err = cli.task_link("01JZZZNOPE", "01JZZZALSOPE").await.unwrap_err();
        assert_eq!(err.status, Some(400));
        assert!(err.message.contains("not part of epic"));

        // Cycle rejection → 409.
        let a = cli.task_create("A", None, None, &[]).await.unwrap();
        let b = cli.task_create("B", None, None, &[]).await.unwrap();
        let (a_id, b_id) = (a["id"].as_str().unwrap(), b["id"].as_str().unwrap());
        cli.task_link(a_id, b_id).await.unwrap(); // a → b
        let err = cli.task_link(b_id, a_id).await.unwrap_err(); // b → a closes the cycle
        assert_eq!(err.status, Some(409));
        assert!(rendered(&err).starts_with(ERROR_PREFIX));
    }

    /// The `comment` verbs round trip through the API: an agent-run
    /// capability token posts a thread (anchored to a map node), replies into
    /// it, lists, and resolves — attributed `is_agent = 1` with no human
    /// author. (AC: users and the agent can post threaded comments; the
    /// users' side is covered in `crate::comments`'s tests.)
    #[tokio::test]
    async fn the_comment_verbs_round_trip_with_agent_attribution() {
        let (state, client) = boot().await;
        let (project_id, epic_id) = seed_epic(&state).await;
        let node = crate::map::create_node(
            state.db.conn(),
            &epic_id,
            "grilling",
            None,
            "Which store?",
            Some("Pick the blob store"),
            None,
            None,
            None,
        )
        .await
        .unwrap();
        let (cli, _guard) = scoped(&state, &client, &project_id, &epic_id, "grilling");

        // `comment post --anchor node --id ... --body ...` starts a thread.
        let head = cli
            .comment_post(Some("node"), Some(&node.id), "Which store are we picking?", None)
            .await
            .unwrap();
        assert_eq!(head["anchor_kind"], "node");
        assert_eq!(head["anchor_id"], node.id.as_str());
        assert_eq!(head["is_agent"], true);
        assert_eq!(head["author_user_id"], Value::Null);
        assert_eq!(head["resolved"], false);
        let thread_id = head["thread_id"].as_str().unwrap().to_string();

        // `comment post --thread ...` replies (the anchor is inherited).
        let reply = cli
            .comment_post(None, None, "Leaning the evidence store.", Some(&thread_id))
            .await
            .unwrap();
        assert_eq!(reply["thread_id"], thread_id.as_str());
        assert_eq!(reply["anchor_id"], node.id.as_str());
        assert_eq!(reply["is_agent"], true);

        // `comment list` shows both; the anchor filter narrows to them.
        let list = cli.comment_list(Some("node"), Some(&node.id)).await.unwrap();
        let items = list["items"].as_array().unwrap();
        assert_eq!(items.len(), 2);
        for item in items {
            assert_eq!(item["thread_id"], thread_id.as_str());
            assert_eq!(item["is_agent"], true);
        }

        // `comment resolve COMMENT` resolves the whole thread.
        let resolved = cli
            .comment_resolve(head["id"].as_str().unwrap())
            .await
            .unwrap();
        let items = resolved["items"].as_array().unwrap();
        assert_eq!(items.len(), 2);
        for item in items {
            assert_eq!(item["resolved"], true);
        }

        // An unknown anchor is the API's 400 message, verbatim, behind the
        // `dearborn: ` marker.
        let err = cli
            .comment_post(Some("section"), Some("no-such-section"), "hi", None)
            .await
            .unwrap_err();
        assert_eq!(err.status, Some(400));
        assert!(err.message.contains("not part of epic"));
        assert!(rendered(&err).starts_with(ERROR_PREFIX));
    }

    /// `comment promote` round trips: the thread becomes a fresh open
    /// frontier node of the chosen kind (with the carried context), the
    /// thread is stamped with `promoted_node_id`, and the node shows on the
    /// map. A HITL (grilling/prototype) phase token may promote — it reshapes
    /// the map — while an AFK (research) phase token is 403'd before any
    /// handler runs.
    #[tokio::test]
    async fn comment_promote_creates_a_frontier_node_and_is_hitl_only() {
        let (state, client) = boot().await;
        let (project_id, epic_id) = seed_epic(&state).await;
        let node = crate::map::create_node(
            state.db.conn(),
            &epic_id,
            "grilling",
            None,
            "Which store?",
            Some("Pick the blob store"),
            None,
            None,
            None,
        )
        .await
        .unwrap();
        let (cli, _guard) = scoped(&state, &client, &project_id, &epic_id, "grilling");

        // The thread to promote: one head comment from the agent's session.
        let head = cli
            .comment_post(Some("node"), Some(&node.id), "Which store are we picking?", None)
            .await
            .unwrap();
        let head_id = head["id"].as_str().unwrap().to_string();

        // Promote with explicit title + question.
        let outcome = cli
            .comment_promote(&head_id, "research", Some("Survey blob stores"), Some("Which fits evidence?"))
            .await
            .unwrap();
        let promoted = outcome["node"].clone();
        assert_eq!(promoted["kind"], "research");
        assert_eq!(promoted["state"], "open");
        assert_eq!(promoted["title"], "Survey blob stores");
        assert_eq!(promoted["question"], "Which fits evidence?");
        assert_eq!(promoted["created_by"], Value::Null);
        let promoted_id = promoted["id"].as_str().unwrap().to_string();

        // The thread is stamped.
        let thread = outcome["thread"].as_array().unwrap();
        assert_eq!(thread.len(), 1);
        assert_eq!(thread[0]["promoted_node_id"], promoted_id.as_str());

        // The node is on the map's frontier.
        let map = cli.map().await.unwrap();
        let view = map["nodes"]
            .as_array()
            .unwrap()
            .iter()
            .find(|n| n["id"] == promoted_id.as_str())
            .unwrap();
        assert_eq!(view["frontier"], json!(true));

        // Promoting again is a 409; task kind is a 400 (the handler checks
        // the kind vocabulary before the thread's promotion state).
        let err = cli
            .comment_promote(&head_id, "grilling", None, None)
            .await
            .unwrap_err();
        assert_eq!(err.status, Some(409));
        assert!(err.message.contains("already been promoted"));
        let err = cli
            .comment_promote(&head_id, "task", None, None)
            .await
            .unwrap_err();
        assert_eq!(err.status, Some(400));

        // An AFK phase token (a leaked research run) cannot promote: it
        // creates a node, a map reshaping act (§6).
        let other_head = cli
            .comment_post(Some("node"), Some(&node.id), "Another thread?", None)
            .await
            .unwrap();
        let (afk, _afk_guard) = scoped(&state, &client, &project_id, &epic_id, "research");
        let err = afk
            .comment_promote(
                other_head["id"].as_str().unwrap(),
                "research",
                None,
                None,
            )
            .await
            .unwrap_err();
        assert_eq!(err.status, Some(403));

        // Nothing was promoted by the AFK attempt.
        let comments = cli.comment_list(None, None).await.unwrap();
        for item in comments["items"].as_array().unwrap() {
            if item["thread_id"] == other_head["thread_id"] {
                assert_eq!(item["promoted_node_id"], Value::Null);
            }
        }
    }

    #[tokio::test]
    async fn an_unscoped_token_fails_with_a_401_behind_the_marker() {
        let (_state, client) = boot().await;
        // No token minted — this client's token is garbage.

        let err = client.scope().await.unwrap_err();
        assert_eq!(err.status, Some(401));
        assert!(rendered(&err).starts_with(ERROR_PREFIX));

        // And so does every verb built on the scope resolution.
        let err = client.task_create("x", None, None, &[]).await.unwrap_err();
        assert_eq!(err.status, Some(401));
    }

    #[tokio::test]
    async fn a_session_token_is_not_a_capability_token() {
        let (state, client) = boot().await;
        let (_project_id, _epic_id) = seed_epic(&state).await;

        // A browser session token authenticates (kind 1) but the CLI's verbs
        // are capability-token territory: the scope verb rejects it with 403.
        let user = users::testing::seed_user(&state, "tester", Role::Admin, true).await;
        let session_token = crate::sessions::testing::login_as(&state, &user).await;
        let session_cli = CliClient::new(
            client.base_url.trim_end_matches('/'),
            &session_token,
        )
        .unwrap();
        let err = session_cli.scope().await.unwrap_err();
        assert_eq!(err.status, Some(403));
    }

    // ---- planning-map verbs (wayfinder epic §10) ---------------------------

    #[tokio::test]
    async fn map_node_verbs_round_trip_through_the_api() {
        let (state, client) = boot().await;
        let (project_id, epic_id) = seed_epic(&state).await;
        let (cli, _guard) = scoped(&state, &client, &project_id, &epic_id, "grilling");

        // Create nodes of each kind; the research and prototype nodes are
        // blocked by the grilling one (`blocked_by` = the graduation shape).
        let grilling = cli
            .node_create("grilling", "Which blob store?", Some("Pick one"), None, &[], &[])
            .await
            .unwrap();
        let grilling_id = grilling["id"].as_str().unwrap().to_string();
        let research = cli
            .node_create("research", "Survey libsql blobs", None, None, &[grilling_id.clone()], &[])
            .await
            .unwrap();
        let task = cli
            .node_create("task", "Provision the bucket", None, Some("afk"), &[], &[])
            .await
            .unwrap();
        assert_eq!(grilling["kind"], "grilling");
        assert_eq!(task["task_mode"], "afk");

        // `node link` wires an additional edge (grilling blocks prototype).
        let prototype = cli
            .node_create("prototype", "Spike the reader", None, None, &[], &[])
            .await
            .unwrap();
        let edge = cli
            .node_link(&grilling_id, prototype["id"].as_str().unwrap())
            .await
            .unwrap();
        assert_eq!(edge["blocker_id"], grilling["id"]);
        assert_eq!(edge["blocked_id"], prototype["id"]);

        // `map` shows the graph with computed frontier/blocked state: the
        // blocked research node is off the frontier until its dependency
        // resolves.
        let map = cli.map().await.unwrap();
        assert_eq!(map["epic_id"], epic_id.as_str());
        assert_eq!(map["nodes"].as_array().unwrap().len(), 4);
        assert_eq!(map["edges"].as_array().unwrap().len(), 2);
        let frontier_of = |map: &Value, id: &Value| {
            map["nodes"]
                .as_array()
                .unwrap()
                .iter()
                .find(|n| n["id"] == *id)
                .unwrap()["frontier"]
                .clone()
        };
        assert_eq!(frontier_of(&map, &grilling["id"]), json!(true));
        assert_eq!(frontier_of(&map, &research["id"]), json!(false));
        assert_eq!(frontier_of(&map, &prototype["id"]), json!(false));

        // Resolving the grilling node releases the research node.
        cli.node_resolve(grilling["id"].as_str().unwrap(), Some("Use evidence blobs"))
            .await
            .unwrap();
        let map = cli.map().await.unwrap();
        assert_eq!(frontier_of(&map, &research["id"]), json!(true));
        let resolved = map["nodes"]
            .as_array()
            .unwrap()
            .iter()
            .find(|n| n["id"] == grilling["id"])
            .unwrap();
        assert_eq!(resolved["gist"], "Use evidence blobs");
        assert_eq!(resolved["state"], "resolved");
    }

    #[tokio::test]
    async fn map_prose_verbs_set_the_four_fields() {
        let (state, client) = boot().await;
        let (project_id, epic_id) = seed_epic(&state).await;
        let (cli, _guard) = scoped(&state, &client, &project_id, &epic_id, "grilling");

        for (field, text) in [
            ("destination", "An exporter that works end to end"),
            ("notes", "Executor stays untouched"),
            ("not_yet_specified", "Which events export; retention"),
            ("out_of_scope", "Multi-region replication"),
        ] {
            let map = cli.map_set_prose(field, text).await.unwrap();
            assert_eq!(map[field], text);
        }

        let map = cli.map().await.unwrap();
        assert_eq!(map["destination"], "An exporter that works end to end");
        assert_eq!(map["not_yet_specified"], "Which events export; retention");
        assert_eq!(map["out_of_scope"], "Multi-region replication");
    }

    #[tokio::test]
    async fn map_verbs_surface_the_servers_error_messages_behind_the_marker() {
        let (state, client) = boot().await;
        let (project_id, epic_id) = seed_epic(&state).await;
        let (cli, _guard) = scoped(&state, &client, &project_id, &epic_id, "grilling");

        // Bad kind → 400 with the server's vocabulary message.
        let err = cli
            .node_create("charting", "T", None, None, &[], &[])
            .await
            .unwrap_err();
        assert_eq!(err.status, Some(400));
        assert!(err.message.contains("grilling|research|prototype|task"));

        // kind=task without a task_mode → 400 (fixed at creation).
        let err = cli.node_create("task", "T", None, None, &[], &[]).await.unwrap_err();
        assert_eq!(err.status, Some(400));
        assert!(err.message.contains("task_mode"));

        // Cycle rejection → 409, same contract as task link.
        let a = cli.node_create("grilling", "A", None, None, &[], &[]).await.unwrap();
        let b = cli.node_create("grilling", "B", None, None, &[], &[]).await.unwrap();
        let (a_id, b_id) = (a["id"].as_str().unwrap(), b["id"].as_str().unwrap());
        cli.node_link(a_id, b_id).await.unwrap();
        let err = cli.node_link(b_id, a_id).await.unwrap_err();
        assert_eq!(err.status, Some(409));
        assert!(err.message.contains("cycle"));
        assert!(rendered(&err).starts_with(ERROR_PREFIX));

        // A blank destination → 400 (it fixes the map's scope).
        let err = cli.map_set_prose("destination", "   ").await.unwrap_err();
        assert_eq!(err.status, Some(400));
    }

    // ---- living-document verbs (wayfinder epic §10, Phase 3) -----------------

    #[tokio::test]
    async fn document_pull_and_sync_round_trip_through_the_scratch_file() {
        let (state, client) = boot().await;
        let (project_id, epic_id) = seed_epic(&state).await;
        let (cli, _guard) = scoped(&state, &client, &project_id, &epic_id, "grilling");

        // The scratch workspace file the agent edits with native file tools.
        let scratch = std::env::temp_dir().join(format!("dearborn-doc-test-{}", ulid::Ulid::new()));
        std::fs::create_dir_all(&scratch).unwrap();
        let path = scratch.join("document.html");

        // Pull the never-synced document: empty file, base version 0.
        let pulled = cli.document_pull(&path).await.unwrap();
        assert_eq!(pulled["version"], 0);
        assert_eq!(pulled["epic_id"], epic_id.as_str());
        assert_eq!(path.is_file(), true);
        assert_eq!(std::fs::read_to_string(&path).unwrap(), "");

        // The agent edits the file, then syncs carrying the base version.
        let v1 = "<h1 id=\"decisions\">Decisions</h1><p>Use libsql blobs.</p>";
        std::fs::write(&path, v1).unwrap();
        let synced = cli.document_sync_file(&path, 0, None).await.unwrap();
        assert_eq!(synced["version"], 1);
        assert_eq!(synced["html"], v1);

        // Syncing the same stale base again is rejected with the server's
        // current-version message — the clean re-read/retry signal.
        let err = cli.document_sync_file(&path, 0, None).await.unwrap_err();
        assert_eq!(err.status, Some(409));
        assert!(err.message.contains("current version is 1"));

        // Re-pull (fresh base version), edit, retry: the round trip closes.
        let pulled = cli.document_pull(&path).await.unwrap();
        assert_eq!(pulled["version"], 1);
        assert_eq!(std::fs::read_to_string(&path).unwrap(), v1);
        let v2 = "<h1 id=\"decisions\">Decisions</h1><p>Use libsql blobs, v2.</p>";
        std::fs::write(&path, v2).unwrap();
        let synced = cli.document_sync_file(&path, 1, None).await.unwrap();
        assert_eq!(synced["version"], 2);
        assert_eq!(synced["sections"][0]["section_id"], "decisions");

        std::fs::remove_dir_all(&scratch).ok();
    }

    #[tokio::test]
    async fn document_verbs_surface_errors_behind_the_marker() {
        let (state, client) = boot().await;
        let (project_id, epic_id) = seed_epic(&state).await;
        let (cli, _guard) = scoped(&state, &client, &project_id, &epic_id, "grilling");

        // A missing scratch file → a local (no-status) CLI error.
        let missing = std::env::temp_dir().join(format!("dearborn-missing-{}", ulid::Ulid::new()));
        let err = cli.document_sync_file(&missing, 0, None).await.unwrap_err();
        assert_eq!(err.status, None);
        assert!(rendered(&err).starts_with(ERROR_PREFIX));

        // A provenance node outside the epic → the server's 400 message.
        let scratch = std::env::temp_dir().join(format!("dearborn-doc-err-{}", ulid::Ulid::new()));
        std::fs::create_dir_all(&scratch).unwrap();
        let path = scratch.join("document.html");
        std::fs::write(&path, "<p>x</p>").unwrap();
        let err = cli
            .document_sync_file(&path, 0, Some("01JZZNOPE"))
            .await
            .unwrap_err();
        assert_eq!(err.status, Some(400));
        assert!(err.message.contains("not part of epic"));

        std::fs::remove_dir_all(&scratch).ok();
    }

    // ---- the grilling resolution bundle (wayfinder epic §6, this task) ------

    #[tokio::test]
    async fn node_resolve_bundle_does_everything_in_one_call() {
        let (state, client) = boot().await;
        let (project_id, epic_id) = seed_epic(&state).await;
        let (cli, _guard) = scoped(&state, &client, &project_id, &epic_id, "grilling");

        // Seed the fog the resolution will graduate from, and the node.
        cli.map_set_prose("not_yet_specified", "Which events export; retention")
            .await
            .unwrap();
        let node = cli
            .node_create("grilling", "Which store?", Some("Pick the blob store"), None, &[], &[])
            .await
            .unwrap();
        let node_id = node["id"].as_str().unwrap().to_string();

        // One resolution: gist + folded document sync + graduations + fog trim
        // + out-of-scope ruling.
        let resolution = json!({
            "gist": "Use the evidence blob store",
            "document": {
                "html": "<h1 id=\"decisions\">Decisions</h1><p>Use the evidence blob store.</p>",
                "base_version": 0
            },
            "graduations": [
                { "kind": "grilling", "title": "Which events export?", "question": "Scope the export" },
                { "kind": "research", "title": "Survey libsql blob support" }
            ],
            "trim_fog": "Retention policy",
            "out_of_scope": [
                { "title": "Multi-region replication", "reason": "Single-region only" }
            ]
        });
        let outcome = cli.node_resolve_bundle(&node_id, &resolution).await.unwrap();

        assert_eq!(outcome["node"]["state"], "resolved");
        assert_eq!(outcome["node"]["gist"], "Use the evidence blob store");
        assert_eq!(outcome["document"]["version"], 1);
        assert_eq!(outcome["created"].as_array().unwrap().len(), 2);
        assert_eq!(outcome["out_of_scope"].as_array().unwrap().len(), 1);

        // The map reflects everything: the graduated layer is on the frontier,
        // the fog is trimmed, the out-of-scope prose line landed.
        let map = cli.map().await.unwrap();
        let nodes = map["nodes"].as_array().unwrap();
        assert_eq!(nodes.len(), 4);
        assert!(nodes.iter().all(|n| n["frontier"] == (n["state"] == "open")));
        assert_eq!(map["not_yet_specified"], "Retention policy");
        assert_eq!(map["out_of_scope"], "Single-region only");
    }

    #[tokio::test]
    async fn an_afk_phase_token_cannot_reach_the_map_reshaping_verbs() {
        let (state, client) = boot().await;
        let (project_id, epic_id) = seed_epic(&state).await;
        let node = {
            let (cli, _guard) = scoped(&state, &client, &project_id, &epic_id, "grilling");
            cli.node_create("grilling", "Which store?", None, None, &[], &[])
                .await
                .unwrap()
        };
        let node_id = node["id"].as_str().unwrap().to_string();

        // A research run's token (hypothetically leaked) is authenticated but
        // barred from every map-mutating surface (wayfinder epic §6: AFK kinds
        // never reshape the map).
        let (afk, _afk_guard) = scoped(&state, &client, &project_id, &epic_id, "research");

        let err = afk
            .node_resolve_bundle(&node_id, &json!({ "gist": "x" }))
            .await
            .unwrap_err();
        assert_eq!(err.status, Some(403));
        let err = afk
            .node_create("grilling", "Sneaky", None, None, &[], &[])
            .await
            .unwrap_err();
        assert_eq!(err.status, Some(403));
        let err = afk.node_resolve(&node_id, Some("sneaky")).await.unwrap_err();
        assert_eq!(err.status, Some(403));
        let err = afk.map_set_prose("out_of_scope", "sneaky").await.unwrap_err();
        assert_eq!(err.status, Some(403));

        // Reads stay open: an AFK run may still look at the map it reports on.
        let map = afk.map().await.unwrap();
        assert_eq!(map["nodes"].as_array().unwrap().len(), 1);

        // Nothing was reshaped.
        let node = afk
            .request(reqwest::Method::GET, &format!("/epics/{epic_id}/map-nodes/{node_id}"), None)
            .await
            .unwrap();
        assert_eq!(node["state"], "open");
        assert_eq!(node["gist"], Value::Null);
    }
}
