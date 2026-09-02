//! Agent-settings storage and resolution (design §§3, 6; tasks T3–T5).
//!
//! Three concerns live here:
//!
//! 1. **Global settings** ([`GlobalSettings`], `global_settings` singleton
//!    row): which harnesses are enabled anywhere, the default harness, and
//!    the default model *per harness* — a map, because model ids are
//!    harness-specific (design §3).
//! 2. **Per-project per-slot overrides** ([`AgentSetting`], `agent_setting`
//!    rows): every column nullable, absent row = inherit globals everywhere.
//!    "Reset" is a delete / NULL write — defaults are **never copied into
//!    rows**, so a Dearborn update that improves its built-in prompts still
//!    reaches every non-overridden slot (design §6).
//! 3. **Resolution** ([`resolve_effective`]): the pure function that folds
//!    globals + an optional override into the config a stage run actually
//!    uses. Inheritance is **harness-scoped**: a model inherits only from the
//!    map entry of the *effective* harness, so overriding a slot's harness
//!    without naming a model drops to that CLI's own default rather than
//!    passing a foreign model string (design §3).
//!
//! The HTTP surface (T10–T12) is the thin layer at the bottom of this file:
//! `GET`/`PUT /settings`, `GET /projects/{id}/agent-settings`, and
//! `PUT /projects/{id}/agent-settings/{slot}`. It owns only validation and
//! merge semantics — all reads/writes go through the store functions above it.

use std::collections::HashMap;

use axum::{
    extract::{Path, State},
    Json,
};
use libsql::params;
use serde::{Deserialize, Serialize};
use serde_json::json;
use thiserror::Error;

use crate::agent_slot::AgentSlot;
use crate::db::Db;
use crate::{AppError, AppResult, AppState};

/// Errors surfaced while reading or writing agent settings.
#[derive(Debug, Error)]
pub enum SettingsError {
    #[error("libsql error: {0}")]
    Libsql(#[from] libsql::Error),
    /// The stored JSON maps in `global_settings` failed to parse — a corrupted
    /// row, not bad input (API payloads are validated before they reach here).
    #[error("invalid JSON stored in global_settings: {0}")]
    StoredJson(#[from] serde_json::Error),
    /// An `agent_setting.slot` value outside the [`AgentSlot`] vocabulary.
    /// Surfaced loudly rather than dropped: silently hiding a row would make
    /// its settings appear not to apply.
    #[error("unknown agent slot key `{0}`")]
    UnknownSlot(String),
}

impl From<SettingsError> for AppError {
    fn from(err: SettingsError) -> Self {
        match err {
            // Query failures flow into the same generic-500 path as raw libsql
            // errors everywhere else (logged in full, reported generically).
            SettingsError::Libsql(e) => AppError::Db(crate::DbError::Libsql(e)),
            // Corrupted settings rows / out-of-vocabulary slot keys are server
            // state problems, not client mistakes — internal, never leaked.
            err @ (SettingsError::StoredJson(_) | SettingsError::UnknownSlot(_)) => {
                AppError::Internal(err.to_string())
            }
        }
    }
}

// ---- Global settings (T3) ---------------------------------------------------

/// The global layer of the resolution chain (`global_settings`, singleton).
///
/// `default_models` maps harness key → optional model; a `None` model for an
/// enabled harness means "let that CLI use its own configured default", which
/// is exactly today's behavior and the seeded state.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct GlobalSettings {
    /// Default harness applied to slots without an override.
    pub default_harness: String,
    /// Default model per harness; missing key = CLI default.
    pub default_models: HashMap<String, Option<String>>,
    /// Harnesses selectable as default or in any project override.
    pub enabled_harnesses: Vec<String>,
}

/// Defaults used when the singleton row is missing entirely. Matches the
/// migration's seed values (§6): Claude-only, no models — byte-for-byte
/// today's behavior.
impl Default for GlobalSettings {
    fn default() -> Self {
        let mut default_models = HashMap::new();
        default_models.insert("claude".to_string(), None);
        GlobalSettings {
            default_harness: "claude".to_string(),
            default_models,
            enabled_harnesses: vec!["claude".to_string()],
        }
    }
}

/// Read the global-settings singleton. An empty table (or a NULL/blank JSON
/// column) resolves through [`GlobalSettings::default`] rather than erroring —
/// a degraded settings row should fall back to today's behavior, not take the
/// whole API down.
pub async fn get_global_settings(db: &Db) -> Result<GlobalSettings, SettingsError> {
    let mut rows = db
        .conn()
        .query(
            "SELECT default_harness, default_models, enabled_harnesses \
             FROM global_settings WHERE id = 1",
            (),
        )
        .await?;
    let Some(row) = rows.next().await? else {
        return Ok(GlobalSettings::default());
    };
    let default_harness: String = row.get(0)?;
    let default_models_raw: Option<String> = row.get(1)?;
    let enabled_raw: Option<String> = row.get(2)?;

    let default_models = match default_models_raw.filter(|s| !s.is_empty()) {
        Some(raw) => serde_json::from_str(&raw)?,
        None => HashMap::new(),
    };
    let enabled_harnesses = match enabled_raw.filter(|s| !s.is_empty()) {
        Some(raw) => serde_json::from_str(&raw)?,
        None => vec![],
    };

    Ok(GlobalSettings {
        default_harness,
        default_models,
        enabled_harnesses,
    })
}

/// Write the global-settings singleton (upsert on `id = 1`) and bump
/// `updated_at`. Full-row replace: the API layer owns merge semantics; this
/// function owns durability.
pub async fn save_global_settings(db: &Db, settings: &GlobalSettings) -> Result<(), SettingsError> {
    let default_models = serde_json::to_string(&settings.default_models)?;
    let enabled = serde_json::to_string(&settings.enabled_harnesses)?;
    db.conn()
        .execute(
            "INSERT INTO global_settings (id, default_harness, default_models, \
                 enabled_harnesses, updated_at) \
             VALUES (1, ?1, ?2, ?3, unixepoch() * 1000) \
             ON CONFLICT(id) DO UPDATE SET \
                 default_harness = excluded.default_harness, \
                 default_models = excluded.default_models, \
                 enabled_harnesses = excluded.enabled_harnesses, \
                 updated_at = excluded.updated_at",
            params![settings.default_harness.clone(), default_models, enabled],
        )
        .await?;
    Ok(())
}

// ---- Per-slot overrides (T5) ------------------------------------------------

/// One project's override row for one slot (`agent_setting`). Every field is
/// nullable: `None` = inherit from globals for that facet alone.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct AgentSetting {
    pub slot: AgentSlot,
    pub harness: Option<String>,
    pub model: Option<String>,
    pub system_prompt: Option<String>,
}

const SETTING_COLUMNS: &str = "slot, harness, model, system_prompt";

fn row_to_setting(row: &libsql::Row) -> Result<AgentSetting, SettingsError> {
    let slot_key: String = row.get(0)?;
    // A stored key outside the enum means the DB predates or postdates this
    // binary's vocabulary. Surface it loudly rather than dropping the row:
    // silently hiding it would make settings appear to "not apply".
    let slot =
        AgentSlot::parse(&slot_key).ok_or_else(|| SettingsError::UnknownSlot(slot_key.clone()))?;
    Ok(AgentSetting {
        slot,
        harness: row.get(1)?,
        model: row.get(2)?,
        system_prompt: row.get(3)?,
    })
}

/// Read one project's override for one slot; `None` when no row exists.
pub async fn get_agent_setting(
    db: &Db,
    project_id: &str,
    slot: AgentSlot,
) -> Result<Option<AgentSetting>, SettingsError> {
    let mut rows = db
        .conn()
        .query(
            format!(
                "SELECT {SETTING_COLUMNS} FROM agent_setting \
                 WHERE project_id = ?1 AND slot = ?2",
            )
            .as_str(),
            params![project_id, slot.as_str()],
        )
        .await?;
    Ok(rows
        .next()
        .await?
        .map(|row| row_to_setting(&row))
        .transpose()?)
}

/// Read all of a project's override rows. Order follows [`AgentSlot::ALL`] so
/// the API response is stable regardless of insertion order.
pub async fn list_agent_settings(
    db: &Db,
    project_id: &str,
) -> Result<Vec<AgentSetting>, SettingsError> {
    let mut rows = db
        .conn()
        .query(
            format!(
                "SELECT {SETTING_COLUMNS} FROM agent_setting \
                 WHERE project_id = ?1",
            )
            .as_str(),
            params![project_id],
        )
        .await?;
    let mut found = Vec::new();
    while let Some(row) = rows.next().await? {
        found.push(row_to_setting(&row)?);
    }
    found.sort_by_key(|s| {
        AgentSlot::ALL
            .iter()
            .position(|a| a == &s.slot)
            .unwrap_or(usize::MAX)
    });
    Ok(found)
}

/// Insert or replace a project's full override row for `setting.slot`.
///
/// Full-row upsert: `None` fields write SQL `NULL`. That is the reset path —
/// clearing a facet means inheriting again, never freezing the old default
/// into the row (design §6). Callers wanting to clear *everything* should use
/// [`delete_agent_setting`] so the row disappears entirely.
pub async fn upsert_agent_setting(
    db: &Db,
    project_id: &str,
    setting: &AgentSetting,
) -> Result<(), SettingsError> {
    db.conn()
        .execute(
            "INSERT INTO agent_setting (project_id, slot, harness, model, \
                 system_prompt, updated_at) \
             VALUES (?1, ?2, ?3, ?4, ?5, unixepoch() * 1000) \
             ON CONFLICT(project_id, slot) DO UPDATE SET \
                 harness = excluded.harness, \
                 model = excluded.model, \
                 system_prompt = excluded.system_prompt, \
                 updated_at = excluded.updated_at",
            params![
                project_id,
                setting.slot.as_str(),
                setting.harness.clone(),
                setting.model.clone(),
                setting.system_prompt.clone(),
            ],
        )
        .await?;
    Ok(())
}

/// Delete a project's override row for `slot`. This **is** reset-to-default:
/// the effective value re-resolves live against globals + compiled defaults.
pub async fn delete_agent_setting(
    db: &Db,
    project_id: &str,
    slot: AgentSlot,
) -> Result<bool, SettingsError> {
    let changed = db
        .conn()
        .execute(
            "DELETE FROM agent_setting WHERE project_id = ?1 AND slot = ?2",
            params![project_id, slot.as_str()],
        )
        .await?;
    Ok(changed > 0)
}

/// Every `(project_id, slot)` whose override row names `harness` — the T10
/// disable-guard's reference check. Only explicit per-slot harness overrides
/// count as references: a stale entry in the global model map or a row that
/// only pins a model/prompt inherits its harness and therefore does not block
/// disabling (it re-keys to whatever is enabled next).
pub async fn harness_references(
    db: &Db,
    harness: &str,
) -> Result<Vec<(String, String)>, SettingsError> {
    let mut rows = db
        .conn()
        .query(
            "SELECT project_id, slot FROM agent_setting WHERE harness = ?1 \
             ORDER BY project_id, slot",
            params![harness],
        )
        .await?;
    let mut refs = Vec::new();
    while let Some(row) = rows.next().await? {
        refs.push((row.get(0)?, row.get(1)?));
    }
    Ok(refs)
}

// ---- Per-run spawn config (T6/T7) ------------------------------------------

/// Every harness key whose CLI Dearborn has an adapter for and can actually
/// spawn (design §2). An effective harness outside this list fails loudly at
/// spawn-validation time rather than silently running some *other* CLI under
/// its name — the settings schema stays open (a hand-written key is still
/// storable), the spawn path does not.
pub const SUPPORTED_HARNESSES: &[&str] = &["claude", crate::harness_pi::PI_HARNESS_ID];

/// The harnesses that can reach Dearborn's local MCP server ([`crate::mcp`]).
///
/// Only Claude Code speaks MCP among the CLIs Dearborn drives: pi has no MCP
/// client at all (verified against the shipped CLI — no `--mcp-config`, no MCP
/// transport). This is a **capability**, not a preference, which is why it is
/// a list rather than a special case at each spawn site.
pub const MCP_CAPABLE_HARNESSES: &[&str] = &["claude"];

/// Whether `harness` is one Dearborn can spawn at all.
pub fn is_supported_harness(harness: &str) -> bool {
    SUPPORTED_HARNESSES.contains(&harness)
}

/// Whether a slot's run needs the agent to call *back* into Dearborn over MCP.
///
/// True for exactly the three planning-side slots: `planning_product` and
/// `planning_technical` read the canonical clone through
/// `read_codebase_context`; `breakdown` builds the task DAG through
/// `create_task`/`link_dependency`. The five task-stage slots act on a
/// checked-out workspace with the CLI's own file tools and never call home,
/// so they impose no such requirement.
pub fn slot_requires_mcp(slot: AgentSlot) -> bool {
    match slot {
        AgentSlot::PlanningProduct | AgentSlot::PlanningTechnical | AgentSlot::Breakdown => true,
        AgentSlot::Implement
        | AgentSlot::Fix
        | AgentSlot::Review
        | AgentSlot::VerifyComplete
        | AgentSlot::Summarize
        | AgentSlot::Triage => false,
    }
}

/// Whether `harness` can run `slot` — supported at all, and MCP-capable when
/// the slot needs MCP. The single predicate every spawn site and the settings
/// API validate against, so "which harness may run where" is stated once.
pub fn harness_supports_slot(harness: &str, slot: AgentSlot) -> bool {
    is_supported_harness(harness)
        && (!slot_requires_mcp(slot) || MCP_CAPABLE_HARNESSES.contains(&harness))
}

/// The error message a spawn site (or the settings API) reports when
/// [`harness_supports_slot`] says no. Written once so the planning, breakdown,
/// and task-stage paths phrase the same refusal identically, and so the reason
/// — unsupported vs. MCP-incapable — is never lost.
pub fn unsupported_harness_message(harness: &str, slot: AgentSlot) -> String {
    if !is_supported_harness(harness) {
        format!(
            "unsupported harness `{harness}`: Dearborn can spawn only {}",
            SUPPORTED_HARNESSES.join(", ")
        )
    } else {
        format!(
            "harness `{harness}` cannot run the `{slot}` slot: that slot calls back into \
             Dearborn over MCP, and only {} can do that",
            MCP_CAPABLE_HARNESSES.join(", ")
        )
    }
}

/// The per-run config a spawn site needs (T6/T7): everything folds globals +
/// overrides into one value read **at spawn time** (live-read, design §9 — no
/// caching anywhere; every stage run re-resolves). `prompt` is the *effective
/// instruction text* — the slot's override when set, else the caller-supplied
/// compiled default — and `prompt_hash` is its SHA-256 hex digest, written to
/// the `agent_run` evidence row so historical runs stay auditable after the
/// override that produced them changes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SpawnConfig {
    /// Harness key validated against [`SUPPORTED_HARNESS`] by each spawn site.
    pub harness: String,
    /// Model passed verbatim to the CLI; `None` → the CLI's own default.
    pub model: Option<String>,
    /// Effective instruction text (override or compiled default).
    pub prompt: String,
    /// SHA-256 hex of `prompt` (evidence column `agent_run.prompt_hash`).
    pub prompt_hash: String,
}

/// Resolve a slot's live [`SpawnConfig`] for `project_id`, falling back to
/// `default_prompt` (the site's compiled `include_str!` text) when the slot
/// carries no system-prompt override. Reads the DB fresh on every call — the
/// whole point is that a mid-epic settings edit is picked up by the next
/// stage run without any invalidation machinery (design §9).
pub async fn spawn_config(
    db: &Db,
    project_id: &str,
    slot: AgentSlot,
    default_prompt: &str,
) -> Result<SpawnConfig, SettingsError> {
    let global = get_global_settings(db).await?;
    let override_row = get_agent_setting(db, project_id, slot).await?;
    let effective = resolve_effective(&global, override_row.as_ref());
    // Same "empty counts as absent" rule resolve_effective applies to the
    // prompt-source flag: an empty override must not replace the default with
    // blank instructions just because it survived validation as `Some("")`.
    let prompt = match override_row
        .as_ref()
        .and_then(|o| o.system_prompt.as_deref())
        .filter(|p| !p.is_empty())
    {
        Some(override_prompt) => override_prompt.to_string(),
        None => default_prompt.to_string(),
    };
    Ok(SpawnConfig {
        harness: effective.harness,
        model: effective.model,
        prompt_hash: prompt_hash(&prompt),
        prompt,
    })
}

/// SHA-256 hex digest of an instruction prompt — the T8 evidence hash. Full
/// digest (not truncated): prompts are user-authored text where two distinct
/// overrides colliding on a short prefix is a real possibility, and 64 hex
/// chars cost nothing in a TEXT column.
pub fn prompt_hash(prompt: &str) -> String {
    use sha2::{Digest, Sha256};
    let digest = Sha256::digest(prompt.as_bytes());
    digest.iter().map(|b| format!("{b:02x}")).collect()
}

// ---- Resolution (T4) --------------------------------------------------------

/// Where a slot's effective instruction prompt came from. Reported by the
/// settings API so the UI can show "custom prompt" vs "default prompt"
/// (design §7) without duplicating the resolution logic client-side.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum PromptSource {
    /// The project's `agent_setting.system_prompt` override.
    Override,
    /// The compiled `include_str!` default for the slot.
    Default,
}

/// The config a stage run actually uses, after folding globals + overrides.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct EffectiveConfig {
    /// Harness whose CLI will be spawned.
    pub harness: String,
    /// Model passed verbatim to the CLI; `None` → the CLI's own default.
    pub model: Option<String>,
    /// Whether the instruction prompt is a project override or the built-in.
    pub prompt_source: PromptSource,
}

/// Fold the global layer and one slot's optional override into the effective
/// config (design §3). Pure — no I/O — so it is unit-tested exhaustively over
/// the null-combination space below.
///
/// Resolution rules:
/// - **Harness:** override wins, else `global.default_harness`.
/// - **Model (harness-scoped):** override's model, else the map entry of the
///   *effective* harness, else `None`. Note the second step keys off the
///   resolved harness: overriding `harness` alone re-keys the model lookup,
///   which is what stops a claude model id reaching another CLI.
/// - **Prompt source:** `Override` iff the override carries a non-empty
///   `system_prompt`; an empty string counts as no prompt (the API trims, but
///   this defends the invariant at the resolution boundary too).
pub fn resolve_effective(
    global: &GlobalSettings,
    slot_override: Option<&AgentSetting>,
) -> EffectiveConfig {
    let harness = slot_override
        .and_then(|o| o.harness.clone())
        .unwrap_or_else(|| global.default_harness.clone());
    let model = slot_override
        .and_then(|o| o.model.clone())
        .or_else(|| global.default_models.get(&harness).cloned().unwrap_or(None));
    let prompt_source = match slot_override.map(|o| o.system_prompt.as_deref()) {
        Some(Some(prompt)) if !prompt.is_empty() => PromptSource::Override,
        _ => PromptSource::Default,
    };
    EffectiveConfig {
        harness,
        model,
        prompt_source,
    }
}

// ---- HTTP surface (T10–T12) --------------------------------------------------

/// A `PUT` field that must not be blank when a value is given: trim, then
/// reject whitespace-only input. Used for harness keys and model ids — both
/// are passed verbatim to CLI spawn, where an empty string would silently mean
/// "no flag" instead of surfacing as the user's intended (bad) value.
fn clean_value(value: String, field: &str) -> AppResult<String> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Err(AppError::BadRequest(format!("{field} must not be empty")));
    }
    Ok(trimmed.to_string())
}

/// `GET /settings` — the global agent settings singleton.
pub async fn get_settings(State(state): State<AppState>) -> AppResult<Json<GlobalSettings>> {
    Ok(Json(get_global_settings(&state.db).await?))
}

/// `PUT /settings` body. Every field optional: absent → keep the stored value,
/// present → replace it. (There are no per-field `null` clears here — the
/// global row always exists and every facet has a well-defined empty state: an
/// empty model map / empty enabled list is rejected by validation below.)
#[derive(Debug, Deserialize)]
pub struct UpdateGlobalSettings {
    #[serde(default)]
    default_harness: Option<String>,
    #[serde(default)]
    default_models: Option<HashMap<String, Option<String>>>,
    #[serde(default)]
    enabled_harnesses: Option<Vec<String>>,
}

/// `PUT /settings` — merge + validate + save globals.
///
/// Validation of the *merged* result:
/// - `default_harness` must be in `enabled_harnesses` (a default nobody can
///   select would make new overrides silently unresolvable).
/// - `enabled_harnesses` must be non-empty (an empty enablement set would
///   strand every slot with no harness at all).
/// - Disabling a harness that any `agent_setting.harness` still names is a
///   **409** listing the referencing slots — explicit cleanup, never a silent
///   fallback to another CLI mid-pipeline (design §2).
pub async fn put_settings(
    State(state): State<AppState>,
    Json(req): Json<UpdateGlobalSettings>,
) -> AppResult<Json<GlobalSettings>> {
    let previous = get_global_settings(&state.db).await?;
    let mut merged = previous.clone();

    if let Some(harness) = req.default_harness {
        merged.default_harness = clean_value(harness, "default_harness")?;
    }
    if let Some(models) = req.default_models {
        let mut cleaned = HashMap::new();
        for (harness, model) in models {
            let key = clean_value(harness, "default_models key")?;
            let value = match model {
                Some(m) => Some(clean_value(m, "default_models value")?),
                None => None,
            };
            cleaned.insert(key, value);
        }
        merged.default_models = cleaned;
    }
    if let Some(enabled) = req.enabled_harnesses {
        let mut cleaned: Vec<String> = Vec::new();
        for harness in enabled {
            let key = clean_value(harness, "enabled_harnesses entry")?;
            if !cleaned.contains(&key) {
                cleaned.push(key);
            }
        }
        if cleaned.is_empty() {
            return Err(AppError::BadRequest(
                "enabled_harnesses must contain at least one harness".to_string(),
            ));
        }
        merged.enabled_harnesses = cleaned;
    }

    if !merged.enabled_harnesses.contains(&merged.default_harness) {
        return Err(AppError::BadRequest(format!(
            "default harness `{}` is not in enabled_harnesses {:?}",
            merged.default_harness, merged.enabled_harnesses
        )));
    }

    // Every slot without an override inherits the default, so a default that
    // cannot run some slot would silently break that slot's next run — which
    // is exactly the "no silent fallback to another CLI mid-pipeline" the
    // design rules out (§2). Refuse it here instead, naming the slot. Only
    // checked for harnesses Dearborn can actually spawn: for an unknown key it
    // makes no capability claims, and the long-standing behavior (storable,
    // fails at spawn) is left alone.
    if is_supported_harness(&merged.default_harness) {
        if let Some(slot) = AgentSlot::ALL
            .iter()
            .copied()
            .find(|slot| !harness_supports_slot(&merged.default_harness, *slot))
        {
            return Err(AppError::BadRequest(format!(
                "{} — pick it per slot instead of as the global default",
                unsupported_harness_message(&merged.default_harness, slot)
            )));
        }
    }

    // The disable guard compares against the *stored* enablement set: any
    // harness leaving the set must have no explicit slot references left.
    for harness in &previous.enabled_harnesses {
        if merged.enabled_harnesses.contains(harness) {
            continue;
        }
        let refs = harness_references(&state.db, harness).await?;
        if !refs.is_empty() {
            let listed = refs
                .iter()
                .map(|(project_id, slot)| format!("project {project_id} slot {slot}"))
                .collect::<Vec<_>>()
                .join(", ");
            return Err(AppError::Conflict(format!(
                "cannot disable harness `{harness}`: still referenced by {listed}"
            )));
        }
    }

    save_global_settings(&state.db, &merged).await?;
    Ok(Json(merged))
}

/// One slot's settings as rendered by the API: the raw override facets plus
/// the server-resolved effective config, so the layered scheme is debuggable
/// at a glance (design §3/§7). Absent override row → all-`null` raw fields.
#[derive(Debug, Serialize)]
pub struct SlotSettingView {
    pub slot: AgentSlot,
    pub harness: Option<String>,
    pub model: Option<String>,
    pub system_prompt: Option<String>,
    /// The slot's compiled default instruction text (design §4), served so a
    /// client editing a default-source slot can prefill the editor with it.
    pub default_prompt: String,
    pub effective: EffectiveConfig,
}

fn slot_view(
    slot: AgentSlot,
    setting: Option<&AgentSetting>,
    global: &GlobalSettings,
) -> SlotSettingView {
    SlotSettingView {
        effective: resolve_effective(global, setting),
        harness: setting.and_then(|s| s.harness.clone()),
        model: setting.and_then(|s| s.model.clone()),
        system_prompt: setting.and_then(|s| s.system_prompt.clone()),
        default_prompt: default_prompt(slot).to_string(),
        slot,
    }
}

/// The compiled default instruction text for `slot` (design §4) — exactly the
/// text the spawn sites serve when the slot carries no override. The settings
/// API exposes it so a client editing a default-source slot can prefill the
/// editor with something to tweak instead of a blank box, without this module
/// duplicating any prompt constant (task stages come from [`crate::spec::prompt_for`];
/// planning/breakdown from their own modules' single constants).
pub fn default_prompt(slot: AgentSlot) -> &'static str {
    use crate::breakdown::BREAKDOWN_PROMPT;
    use crate::planning::{PRODUCT_PLANNING_PROMPT, TECHNICAL_PLANNING_PROMPT};
    use crate::spec::prompt_for;
    use crate::task_agent::Stage;
    match slot {
        AgentSlot::PlanningProduct => PRODUCT_PLANNING_PROMPT,
        AgentSlot::PlanningTechnical => TECHNICAL_PLANNING_PROMPT,
        AgentSlot::Breakdown => BREAKDOWN_PROMPT,
        // Every agent stage has a compiled prompt (`spec.rs`'s own test
        // asserts non-empty for each); `expect` mirrors the spawn sites.
        AgentSlot::Implement => {
            prompt_for(Stage::Implement).expect("Stage::Implement always has a prompt")
        }
        AgentSlot::Fix => prompt_for(Stage::Fix).expect("Stage::Fix always has a prompt"),
        AgentSlot::Review => prompt_for(Stage::Review).expect("Stage::Review always has a prompt"),
        AgentSlot::VerifyComplete => {
            prompt_for(Stage::VerifyComplete).expect("Stage::VerifyComplete always has a prompt")
        }
        AgentSlot::Summarize => {
            prompt_for(Stage::Summarize).expect("Stage::Summarize always has a prompt")
        }
        AgentSlot::Triage => prompt_for(Stage::Triage).expect("Stage::Triage always has a prompt"),
    }
}

/// 404 unless the project row exists. Settings rows are meaningless without
/// their project; rather than letting orphans linger, addressing one errors.
async fn ensure_project(db: &Db, project_id: &str) -> AppResult<()> {
    let mut rows = db
        .conn()
        .query("SELECT 1 FROM project WHERE id = ?1", params![project_id])
        .await?;
    if rows.next().await?.is_none() {
        return Err(AppError::NotFound(format!(
            "project {project_id} not found"
        )));
    }
    Ok(())
}

/// `GET /projects/{id}/agent-settings` — all nine slots in canonical order,
/// each with its raw overrides and resolved effective config.
pub async fn get_project_agent_settings(
    State(state): State<AppState>,
    Path(project_id): Path<String>,
) -> AppResult<Json<serde_json::Value>> {
    ensure_project(&state.db, &project_id).await?;
    let global = get_global_settings(&state.db).await?;
    let overrides = list_agent_settings(&state.db, &project_id).await?;
    // `list_agent_settings` already sorts into `AgentSlot::ALL` order; walk ALL
    // so unset slots render too (absent row = inherit everything, §6).
    let items: Vec<SlotSettingView> = AgentSlot::ALL
        .iter()
        .map(|slot| {
            let setting = overrides.iter().find(|s| s.slot == *slot);
            slot_view(*slot, setting, &global)
        })
        .collect();
    Ok(Json(json!({ "items": items })))
}

/// `PUT /projects/{id}/agent-settings/{slot}` body — partial update with
/// double-option semantics (same shape as `PATCH /projects/{id}`):
/// absent → untouched, `null` → clear that override (= reset to inherited),
/// value → set it.
#[derive(Debug, Deserialize)]
pub struct UpdateAgentSetting {
    #[serde(default, deserialize_with = "crate::projects::double_option")]
    harness: Option<Option<String>>,
    #[serde(default, deserialize_with = "crate::projects::double_option")]
    model: Option<Option<String>>,
    #[serde(default, deserialize_with = "crate::projects::double_option")]
    system_prompt: Option<Option<String>>,
}

/// `PUT /projects/{id}/agent-settings/{slot}` — partial update of one slot's
/// override row. Unknown slot → **404** (the closed vocabulary is the API
/// surface); unknown project → **404**; a `harness` value must be globally
/// enabled; `model` values must be non-empty once trimmed. Clearing **all**
/// three facets deletes the row outright rather than parking a NULL-only row
/// (reset = delete, design §6).
pub async fn put_agent_setting(
    State(state): State<AppState>,
    Path((project_id, slot_key)): Path<(String, String)>,
    Json(req): Json<UpdateAgentSetting>,
) -> AppResult<Json<SlotSettingView>> {
    let slot = AgentSlot::parse(&slot_key)
        .ok_or_else(|| AppError::NotFound(format!("unknown agent slot `{slot_key}`")))?;
    ensure_project(&state.db, &project_id).await?;
    let global = get_global_settings(&state.db).await?;

    let mut setting = get_agent_setting(&state.db, &project_id, slot)
        .await?
        .unwrap_or(AgentSetting {
            slot,
            harness: None,
            model: None,
            system_prompt: None,
        });

    match req.harness {
        Some(None) => setting.harness = None,
        Some(Some(value)) => {
            let cleaned = clean_value(value, "harness")?;
            if !global.enabled_harnesses.contains(&cleaned) {
                return Err(AppError::BadRequest(format!(
                    "harness `{cleaned}` is not in enabled_harnesses {:?}",
                    global.enabled_harnesses
                )));
            }
            // A harness Dearborn *can* spawn but that cannot run this
            // particular slot is refused here, at configuration time, rather
            // than at the next stage run. Unknown keys stay storable — the
            // spawn sites already fail loudly on those, and refusing them here
            // would break the existing "schema is open, spawn path is not"
            // split (see `SUPPORTED_HARNESSES`).
            if is_supported_harness(&cleaned) && !harness_supports_slot(&cleaned, slot) {
                return Err(AppError::BadRequest(unsupported_harness_message(
                    &cleaned, slot,
                )));
            }
            setting.harness = Some(cleaned);
        }
        None => {}
    }
    match req.model {
        Some(None) => setting.model = None,
        Some(Some(value)) => setting.model = Some(clean_value(value, "model")?),
        None => {}
    }
    match req.system_prompt {
        Some(None) => setting.system_prompt = None,
        Some(Some(value)) => {
            let trimmed = value.trim().to_string();
            // An empty/whitespace prompt stores as cleared: resolution already
            // treats empty overrides as absent (§3), so persisting one would
            // only fake an "override" the resolver ignores.
            setting.system_prompt = if trimmed.is_empty() {
                None
            } else {
                Some(trimmed)
            };
        }
        None => {}
    }

    if setting.harness.is_none() && setting.model.is_none() && setting.system_prompt.is_none() {
        delete_agent_setting(&state.db, &project_id, slot).await?;
    } else {
        upsert_agent_setting(&state.db, &project_id, &setting).await?;
    }

    Ok(Json(slot_view(slot, Some(&setting), &global)))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn global(default_harness: &str, models: &[(&str, Option<&str>)]) -> GlobalSettings {
        let mut default_models = HashMap::new();
        for (harness, model) in models {
            default_models.insert(harness.to_string(), model.map(str::to_string));
        }
        GlobalSettings {
            default_harness: default_harness.to_string(),
            default_models,
            enabled_harnesses: vec!["claude".to_string(), "codex".to_string()],
        }
    }

    fn setting(harness: Option<&str>, model: Option<&str>, prompt: Option<&str>) -> AgentSetting {
        AgentSetting {
            slot: AgentSlot::Implement,
            harness: harness.map(str::to_string),
            model: model.map(str::to_string),
            system_prompt: prompt.map(str::to_string),
        }
    }

    // ---- resolve_effective: exhaustive null-combination space --------------

    #[test]
    fn no_override_inherits_everything_from_globals() {
        let g = global("claude", &[("claude", Some("sonnet"))]);
        assert_eq!(
            resolve_effective(&g, None),
            EffectiveConfig {
                harness: "claude".to_string(),
                model: Some("sonnet".to_string()),
                prompt_source: PromptSource::Default,
            }
        );
    }

    #[test]
    fn empty_override_row_inherits_everything() {
        let g = global("claude", &[("claude", Some("sonnet"))]);
        assert_eq!(resolve_effective(&g, Some(&setting(None, None, None))), {
            resolve_effective(&g, None)
        });
    }

    #[test]
    fn no_model_anywhere_resolves_to_none_cli_default() {
        let g = global("claude", &[("claude", None)]);
        assert_eq!(
            resolve_effective(&g, None).model,
            None,
            "seeded state (no models configured) must mean CLI default"
        );
    }

    #[test]
    fn harness_map_entry_missing_for_effective_harness_means_none() {
        let g = global("claude", &[("claude", Some("sonnet"))]);
        // Override switches harness; the map has no codex entry → CLI default.
        assert_eq!(
            resolve_effective(&g, Some(&setting(Some("codex"), None, None))).model,
            None
        );
    }

    #[test]
    fn model_lookup_rekeys_to_the_overridden_harness_not_the_default() {
        let g = global(
            "claude",
            &[("claude", Some("sonnet")), ("codex", Some("gpt-5"))],
        );
        // Harness-scoped inheritance: the codex map entry applies, NOT sonnet.
        assert_eq!(
            resolve_effective(&g, Some(&setting(Some("codex"), None, None))),
            EffectiveConfig {
                harness: "codex".to_string(),
                model: Some("gpt-5".to_string()),
                prompt_source: PromptSource::Default,
            }
        );
    }

    #[test]
    fn override_model_beats_the_map_even_when_keys_exist() {
        let g = global("claude", &[("claude", Some("sonnet"))]);
        let cfg = resolve_effective(&g, Some(&setting(Some("claude"), Some("haiku"), None)));
        assert_eq!(cfg.model, Some("haiku".to_string()));
    }

    #[test]
    fn override_model_with_new_harness_does_not_inherit_old_harness_model() {
        let g = global("claude", &[("claude", Some("sonnet")), ("codex", None)]);
        // Explicit codex model override stands; had it been None, the codex
        // map entry (None) would also win over sonnet.
        assert_eq!(
            resolve_effective(&g, Some(&setting(Some("codex"), Some("o4"), None))).model,
            Some("o4".to_string())
        );
        assert_eq!(
            resolve_effective(&g, Some(&setting(Some("codex"), None, None))).model,
            None
        );
    }

    #[test]
    fn prompt_source_is_override_only_for_a_nonempty_prompt() {
        let g = global("claude", &[]);
        for (prompt, expected) in [
            (Some("custom instructions"), PromptSource::Override),
            (None, PromptSource::Default),
        ] {
            let cfg = resolve_effective(&g, Some(&setting(None, None, prompt)));
            assert_eq!(cfg.prompt_source, expected);
        }
    }

    #[test]
    fn facets_combine_independently() {
        let g = global(
            "claude",
            &[("claude", Some("sonnet")), ("codex", Some("gpt-5"))],
        );
        // Harness + prompt overridden; model inherited from codex map entry.
        let cfg = resolve_effective(
            &g,
            Some(&setting(Some("codex"), None, Some("my review prompt"))),
        );
        assert_eq!(
            cfg,
            EffectiveConfig {
                harness: "codex".to_string(),
                model: Some("gpt-5".to_string()),
                prompt_source: PromptSource::Override,
            }
        );
        // Model + prompt overridden; harness inherited.
        let cfg = resolve_effective(&g, Some(&setting(None, Some("haiku"), Some("p"))));
        assert_eq!(cfg.harness, "claude");
        assert_eq!(cfg.model, Some("haiku".to_string()));
        assert_eq!(cfg.prompt_source, PromptSource::Override);
    }

    // ---- Store round-trips (real :memory: DB through migrations) -----------

    async fn test_db() -> Db {
        let db = Db::connect(":memory:").await.unwrap();
        db.run_migrations().await.unwrap();
        db
    }

    async fn seed_project(db: &Db) -> String {
        db.conn()
            .execute(
                "INSERT INTO project (id, name, repo_url, clone_status, created_at, updated_at) \
                 VALUES ('proj-1', 'P', 'https://github.com/o/r', 'pending', 1, 1)",
                (),
            )
            .await
            .unwrap();
        "proj-1".to_string()
    }

    #[tokio::test]
    async fn migration_seeds_global_settings_to_todays_behavior() {
        let db = test_db().await;
        let s = get_global_settings(&db).await.unwrap();
        assert_eq!(s.default_harness, "claude");
        assert_eq!(s.enabled_harnesses, vec!["claude".to_string()]);
        assert_eq!(s.default_models.get("claude"), Some(&None));
    }

    #[tokio::test]
    async fn empty_global_table_falls_back_to_defaults() {
        let db = test_db().await;
        db.conn()
            .execute("DELETE FROM global_settings", ())
            .await
            .unwrap();
        let s = get_global_settings(&db).await.unwrap();
        assert_eq!(s, GlobalSettings::default());
    }

    #[tokio::test]
    async fn global_settings_upsert_round_trips() {
        let db = test_db().await;
        let mut models = HashMap::new();
        models.insert("claude".to_string(), Some("sonnet-4-5".to_string()));
        models.insert("codex".to_string(), None);
        let settings = GlobalSettings {
            default_harness: "codex".to_string(),
            default_models: models,
            enabled_harnesses: vec!["claude".to_string(), "codex".to_string()],
        };
        save_global_settings(&db, &settings).await.unwrap();
        assert_eq!(get_global_settings(&db).await.unwrap(), settings);

        // Second save updates in place (still exactly one row).
        save_global_settings(&db, &GlobalSettings::default())
            .await
            .unwrap();
        let mut rows = db
            .conn()
            .query("SELECT COUNT(*) FROM global_settings", ())
            .await
            .unwrap();
        let count: i64 = rows.next().await.unwrap().unwrap().get(0).unwrap();
        assert_eq!(count, 1);
        assert_eq!(
            get_global_settings(&db).await.unwrap(),
            GlobalSettings::default()
        );
    }

    #[tokio::test]
    async fn agent_setting_crud_round_trip() {
        let db = test_db().await;
        let project = seed_project(&db).await;

        // Absent row → None.
        assert!(get_agent_setting(&db, &project, AgentSlot::Review)
            .await
            .unwrap()
            .is_none());
        assert!(list_agent_settings(&db, &project).await.unwrap().is_empty());

        // Upsert creates; partial override keeps other facets NULL.
        upsert_agent_setting(
            &db,
            &project,
            &AgentSetting {
                slot: AgentSlot::Review,
                harness: None,
                model: Some("haiku".to_string()),
                system_prompt: None,
            },
        )
        .await
        .unwrap();
        let got = get_agent_setting(&db, &project, AgentSlot::Review)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(got.harness, None);
        assert_eq!(got.model, Some("haiku".to_string()));
        assert_eq!(got.system_prompt, None);

        // Second upsert replaces the whole row (clearing = writing NULL).
        upsert_agent_setting(
            &db,
            &project,
            &AgentSetting {
                slot: AgentSlot::Review,
                harness: Some("codex".to_string()),
                model: None,
                system_prompt: Some("harsh but fair".to_string()),
            },
        )
        .await
        .unwrap();
        let got = get_agent_setting(&db, &project, AgentSlot::Review)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(got.harness, Some("codex".to_string()));
        assert_eq!(got.model, None, "reset must be a NULL write, never a copy");
        assert_eq!(got.system_prompt, Some("harsh but fair".to_string()));

        // List returns all overridden slots in canonical ALL order.
        upsert_agent_setting(
            &db,
            &project,
            &AgentSetting {
                slot: AgentSlot::PlanningProduct,
                harness: Some("claude".to_string()),
                model: None,
                system_prompt: Some("plan well".to_string()),
            },
        )
        .await
        .unwrap();
        let listed = list_agent_settings(&db, &project).await.unwrap();
        let slots: Vec<AgentSlot> = listed.iter().map(|s| s.slot).collect();
        assert_eq!(slots, vec![AgentSlot::PlanningProduct, AgentSlot::Review]);

        // Delete removes exactly the targeted slot.
        assert!(delete_agent_setting(&db, &project, AgentSlot::Review)
            .await
            .unwrap());
        assert!(!delete_agent_setting(&db, &project, AgentSlot::Review)
            .await
            .unwrap());
        assert_eq!(list_agent_settings(&db, &project).await.unwrap().len(), 1);
    }

    #[tokio::test]
    async fn base_branch_columns_exist_and_default_null() {
        let db = test_db().await;
        let project = seed_project(&db).await;
        let project_base: Option<String> = {
            let mut rows = db
                .conn()
                .query(
                    "SELECT base_branch FROM project WHERE id = ?1",
                    params![project.as_str()],
                )
                .await
                .unwrap();
            rows.next().await.unwrap().unwrap().get(0).unwrap()
        };
        assert_eq!(project_base, None);

        db.conn()
            .execute(
                "INSERT INTO epic (id, project_id, title, created_at, updated_at, base_branch) \
                 VALUES ('e-1', ?1, 'T', 1, 1, 'develop')",
                params![project.as_str()],
            )
            .await
            .unwrap();
        let epic_base: String = {
            let mut rows = db
                .conn()
                .query("SELECT base_branch FROM epic WHERE id = 'e-1'", ())
                .await
                .unwrap();
            rows.next().await.unwrap().unwrap().get(0).unwrap()
        };
        assert_eq!(epic_base, "develop");
    }

    #[tokio::test]
    async fn unknown_stored_slot_key_surfaces_as_decode_error() {
        let db = test_db().await;
        let project = seed_project(&db).await;
        db.conn()
            .execute(
                "INSERT INTO agent_setting (project_id, slot, harness, model, system_prompt, \
                     updated_at) VALUES (?1, 'time_traveler', NULL, NULL, NULL, 1)",
                params![project.as_str()],
            )
            .await
            .unwrap();
        assert!(list_agent_settings(&db, &project).await.is_err());
    }

    // ---- spawn_config (T6/T7) ----------------------------------------------

    const DEFAULT_PROMPT: &str = "compiled default instructions";

    #[tokio::test]
    async fn spawn_config_without_override_serves_the_compiled_default() {
        let db = test_db().await;
        let project = seed_project(&db).await;
        let cfg = spawn_config(&db, &project, AgentSlot::Implement, DEFAULT_PROMPT)
            .await
            .unwrap();
        assert_eq!(cfg.harness, "claude");
        assert_eq!(cfg.model, None, "seeded globals configure no model");
        assert_eq!(cfg.prompt, DEFAULT_PROMPT);
        assert_eq!(cfg.prompt_hash, prompt_hash(DEFAULT_PROMPT));
    }

    #[tokio::test]
    async fn spawn_config_applies_the_slot_override() {
        let db = test_db().await;
        let project = seed_project(&db).await;
        upsert_agent_setting(
            &db,
            &project,
            &AgentSetting {
                slot: AgentSlot::Review,
                harness: None,
                model: Some("haiku".to_string()),
                system_prompt: Some("be harsh but fair".to_string()),
            },
        )
        .await
        .unwrap();
        let cfg = spawn_config(&db, &project, AgentSlot::Review, "compiled review prompt")
            .await
            .unwrap();
        assert_eq!(cfg.harness, "claude");
        assert_eq!(cfg.model, Some("haiku".to_string()));
        assert_eq!(cfg.prompt, "be harsh but fair");
        assert_eq!(cfg.prompt_hash, prompt_hash("be harsh but fair"));
    }

    #[tokio::test]
    async fn empty_prompt_override_counts_as_absent() {
        let db = test_db().await;
        let project = seed_project(&db).await;
        upsert_agent_setting(
            &db,
            &project,
            &AgentSetting {
                slot: AgentSlot::Implement,
                harness: None,
                model: None,
                system_prompt: Some(String::new()),
            },
        )
        .await
        .unwrap();
        let cfg = spawn_config(&db, &project, AgentSlot::Implement, DEFAULT_PROMPT)
            .await
            .unwrap();
        assert_eq!(cfg.prompt, DEFAULT_PROMPT);
    }

    #[tokio::test]
    async fn live_read_picks_up_a_mid_flight_settings_change_on_the_next_call() {
        // T9's core property at the resolution seam: no caching. Two calls
        // around an override write must observe different effective configs.
        let db = test_db().await;
        let project = seed_project(&db).await;
        let before = spawn_config(&db, &project, AgentSlot::Fix, DEFAULT_PROMPT)
            .await
            .unwrap();
        assert_eq!(before.prompt, DEFAULT_PROMPT);

        upsert_agent_setting(
            &db,
            &project,
            &AgentSetting {
                slot: AgentSlot::Fix,
                harness: Some("claude".to_string()),
                model: Some("sonnet-4-5".to_string()),
                system_prompt: Some("revised fix instructions".to_string()),
            },
        )
        .await
        .unwrap();

        let after = spawn_config(&db, &project, AgentSlot::Fix, DEFAULT_PROMPT)
            .await
            .unwrap();
        assert_eq!(after.prompt, "revised fix instructions");
        assert_eq!(after.model, Some("sonnet-4-5".to_string()));
        assert_ne!(before, after, "the next run must see the new settings");

        // Reset (delete) also takes effect on the very next read.
        delete_agent_setting(&db, &project, AgentSlot::Fix)
            .await
            .unwrap();
        let reset = spawn_config(&db, &project, AgentSlot::Fix, DEFAULT_PROMPT)
            .await
            .unwrap();
        assert_eq!(reset, before);
    }

    #[test]
    fn prompt_hash_is_a_full_sha256_hex_digest_and_discriminates_inputs() {
        let a = prompt_hash("instructions A");
        let b = prompt_hash("instructions B");
        assert_eq!(a.len(), 64, "full digest, not a truncated prefix");
        assert!(a.chars().all(|c| c.is_ascii_hexdigit()));
        assert_ne!(a, b, "distinct prompts must hash distinctly");
        // Stability: hashing twice yields the same digest (no randomness).
        assert_eq!(a, prompt_hash("instructions A"));
    }

    // ---- HTTP surface (T10–T12) --------------------------------------------

    use crate::{app, Config};
    use axum::body::Body;
    use axum::http::{
        header::{AUTHORIZATION, CONTENT_TYPE},
        Request, StatusCode,
    };
    use serde_json::Value as Json;
    use tower::ServiceExt; // for `oneshot`

    async fn test_app() -> (axum::Router, crate::AppState) {
        let db = Db::connect(":memory:").await.unwrap();
        db.run_migrations().await.unwrap();
        let state = crate::AppState::new(Config::for_test(), db);
        (app(state.clone()), state)
    }

    /// The bearer credential HTTP tests present, minted **once per process**
    /// from a seeded active admin (`crate::users::testing::seed_user` +
    /// `crate::sessions::testing::login_as`) — the replacement for the deleted
    /// static `TOKEN` constant. Access-token verification is stateless (one
    /// HMAC check against the fixed test master key, no database read), so a
    /// token minted here authenticates against every in-memory instance these
    /// tests boot.
    fn auth_bearer() -> &'static str {
        static BEARER: std::sync::OnceLock<String> = std::sync::OnceLock::new();
        BEARER.get_or_init(|| {
            // Seeding and login are async store calls, and `req` below is
            // synchronous. Mint on a dedicated OS thread: `Runtime::block_on`
            // panics if called from inside a test's own async context, but a
            // plain thread has none, so a throwaway current-thread runtime is
            // legal there.
            let (tx, rx) = std::sync::mpsc::channel();
            std::thread::spawn(move || {
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .expect("test runtime");
                let token = runtime.block_on(async {
                    let db = crate::Db::connect(":memory:").await.unwrap();
                    db.run_migrations().await.unwrap();
                    let state = crate::AppState::new(crate::Config::for_test(), db);
                    let user = crate::users::testing::seed_user(
                        &state,
                        "tester",
                        crate::users::Role::Admin,
                        true,
                    )
                    .await;
                    crate::sessions::testing::login_as(&state, &user).await
                });
                tx.send(token).expect("bearer receiver dropped");
            });
            rx.recv().expect("bearer minter panicked")
        })
    }

    fn req(method: &str, uri: &str, body: Option<Json>) -> Request<Body> {
        let builder = Request::builder()
            .method(method)
            .uri(uri)
            .header(AUTHORIZATION, format!("Bearer {}", auth_bearer()));
        match body {
            Some(v) => builder
                .header(CONTENT_TYPE, "application/json")
                .body(Body::from(v.to_string()))
                .unwrap(),
            None => builder.body(Body::empty()).unwrap(),
        }
    }

    async fn body_json(response: axum::response::Response) -> Json {
        let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        if bytes.is_empty() {
            return Json::Null;
        }
        serde_json::from_slice(&bytes).unwrap()
    }

    /// Create one project through the real API and return its id.
    async fn create_project(app: &axum::Router) -> String {
        let created = app
            .clone()
            .oneshot(req(
                "POST",
                "/projects",
                Some(json!({
                    "name": "P",
                    "repo_url": "https://example.com/p.git"
                })),
            ))
            .await
            .unwrap();
        assert_eq!(created.status(), StatusCode::CREATED);
        body_json(created).await["id"].as_str().unwrap().to_string()
    }

    #[tokio::test]
    async fn get_settings_returns_the_seeded_defaults() {
        let (app, _state) = test_app().await;
        let got = app.oneshot(req("GET", "/settings", None)).await.unwrap();
        assert_eq!(got.status(), StatusCode::OK);
        assert_eq!(
            body_json(got).await,
            json!({
                "default_harness": "claude",
                "default_models": { "claude": null },
                "enabled_harnesses": ["claude"]
            })
        );
    }

    #[tokio::test]
    async fn put_settings_round_trips_and_merges_partially() {
        let (app, _state) = test_app().await;

        let put = app
            .clone()
            .oneshot(req(
                "PUT",
                "/settings",
                Some(json!({
                    "default_harness": "  codex  ",
                    "default_models": {
                        "claude": "  sonnet-4-5  ",
                        "codex": "gpt-5"
                    },
                    "enabled_harnesses": ["claude", " codex "]
                })),
            ))
            .await
            .unwrap();
        assert_eq!(put.status(), StatusCode::OK);
        let first = body_json(put).await;
        assert_eq!(first["default_harness"], json!("codex"));
        // Values are trimmed on save (design §7: trimmed, non-empty only).
        assert_eq!(
            first["default_models"],
            json!({ "claude": "sonnet-4-5", "codex": "gpt-5" })
        );

        // Partial PUT: only the model map changes; harness + enablement stay.
        let patch = app
            .clone()
            .oneshot(req(
                "PUT",
                "/settings",
                Some(json!({ "default_models": { "codex": "o4-mini" } })),
            ))
            .await
            .unwrap();
        assert_eq!(patch.status(), StatusCode::OK);
        assert_eq!(
            body_json(patch).await,
            json!({
                "default_harness": "codex",
                "default_models": { "codex": "o4-mini" },
                "enabled_harnesses": ["claude", "codex"]
            })
        );
    }

    #[tokio::test]
    async fn put_settings_rejects_a_default_outside_the_enabled_set() {
        let (app, _state) = test_app().await;
        let put = app
            .clone()
            .oneshot(req(
                "PUT",
                "/settings",
                Some(json!({ "default_harness": "codex" })),
            ))
            .await
            .unwrap();
        assert_eq!(put.status(), StatusCode::BAD_REQUEST);
        assert_eq!(body_json(put).await["error"]["code"], json!("bad_request"));
    }

    // ---- harness/slot capability (pi) --------------------------------------

    #[test]
    fn slot_capability_splits_the_mcp_bound_slots_from_the_task_stages() {
        use crate::harness_pi::PI_HARNESS_ID;

        // The three planning-side slots call back into Dearborn over MCP.
        for slot in [
            AgentSlot::PlanningProduct,
            AgentSlot::PlanningTechnical,
            AgentSlot::Breakdown,
        ] {
            assert!(slot_requires_mcp(slot), "{slot}");
            assert!(harness_supports_slot("claude", slot), "{slot}");
            assert!(!harness_supports_slot(PI_HARNESS_ID, slot), "{slot}");
        }
        // The six task stages act only on their workspace, so both harnesses
        // run them.
        for slot in [
            AgentSlot::Implement,
            AgentSlot::Fix,
            AgentSlot::Review,
            AgentSlot::VerifyComplete,
            AgentSlot::Summarize,
            AgentSlot::Triage,
        ] {
            assert!(!slot_requires_mcp(slot), "{slot}");
            assert!(harness_supports_slot("claude", slot), "{slot}");
            assert!(harness_supports_slot(PI_HARNESS_ID, slot), "{slot}");
        }
        // A harness with no adapter runs nothing at all.
        assert!(!is_supported_harness("codex"));
        assert!(!harness_supports_slot("codex", AgentSlot::Implement));
    }

    #[test]
    fn the_refusal_message_distinguishes_unsupported_from_mcp_incapable() {
        use crate::harness_pi::PI_HARNESS_ID;

        let unknown = unsupported_harness_message("codex", AgentSlot::Implement);
        assert!(unknown.contains("unsupported"), "{unknown}");
        assert!(unknown.contains("codex"), "{unknown}");

        let incapable = unsupported_harness_message(PI_HARNESS_ID, AgentSlot::Breakdown);
        assert!(!incapable.contains("unsupported"), "{incapable}");
        assert!(incapable.contains("MCP"), "{incapable}");
        assert!(incapable.contains("breakdown"), "{incapable}");
    }

    #[tokio::test]
    async fn put_slot_rejects_a_harness_that_cannot_run_that_slot() {
        let (app, state) = test_app().await;
        let project_id = seed_project(&state.db).await;
        // Enable pi so the enablement check passes and the capability check is
        // what actually refuses.
        let enable = app
            .clone()
            .oneshot(req(
                "PUT",
                "/settings",
                Some(json!({ "enabled_harnesses": ["claude", "pi"] })),
            ))
            .await
            .unwrap();
        assert_eq!(enable.status(), StatusCode::OK);

        // pi on an MCP-bound slot: refused at configuration time.
        let refused = app
            .clone()
            .oneshot(req(
                "PUT",
                &format!("/projects/{project_id}/agent-settings/breakdown"),
                Some(json!({ "harness": "pi" })),
            ))
            .await
            .unwrap();
        assert_eq!(refused.status(), StatusCode::BAD_REQUEST);
        let message = body_json(refused).await["error"]["message"]
            .as_str()
            .unwrap_or_default()
            .to_string();
        assert!(message.contains("MCP"), "{message}");

        // The same harness on a task-stage slot is accepted.
        let ok = app
            .clone()
            .oneshot(req(
                "PUT",
                &format!("/projects/{project_id}/agent-settings/implement"),
                Some(json!({ "harness": "pi" })),
            ))
            .await
            .unwrap();
        assert_eq!(ok.status(), StatusCode::OK);
        let view = body_json(ok).await;
        assert_eq!(view["harness"], json!("pi"));
        assert_eq!(view["effective"]["harness"], json!("pi"));
    }

    #[tokio::test]
    async fn put_settings_rejects_a_default_that_cannot_run_every_slot() {
        // Every non-overridden slot inherits the default, so a default that
        // cannot run the planning slots would break them silently. Refused,
        // and the message points at the per-slot escape hatch.
        let (app, _state) = test_app().await;
        let put = app
            .clone()
            .oneshot(req(
                "PUT",
                "/settings",
                Some(json!({
                    "default_harness": "pi",
                    "enabled_harnesses": ["claude", "pi"]
                })),
            ))
            .await
            .unwrap();
        assert_eq!(put.status(), StatusCode::BAD_REQUEST);
        let message = body_json(put).await["error"]["message"]
            .as_str()
            .unwrap_or_default()
            .to_string();
        assert!(message.contains("MCP"), "{message}");
        assert!(message.contains("per slot"), "{message}");
    }

    #[tokio::test]
    async fn put_settings_rejects_empty_enablement_and_blank_models() {
        let (app, _state) = test_app().await;

        let no_harnesses = app
            .clone()
            .oneshot(req(
                "PUT",
                "/settings",
                Some(json!({ "enabled_harnesses": [] })),
            ))
            .await
            .unwrap();
        assert_eq!(no_harnesses.status(), StatusCode::BAD_REQUEST);

        let blank_model = app
            .clone()
            .oneshot(req(
                "PUT",
                "/settings",
                Some(json!({ "default_models": { "claude": "   " } })),
            ))
            .await
            .unwrap();
        assert_eq!(blank_model.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn put_settings_refuses_to_disable_a_referenced_harness_until_reset() {
        let (app, _state) = test_app().await;
        let project = create_project(&app).await;

        // Enable codex, then point one slot's override at it.
        let enable = app
            .clone()
            .oneshot(req(
                "PUT",
                "/settings",
                Some(json!({ "enabled_harnesses": ["claude", "codex"] })),
            ))
            .await
            .unwrap();
        assert_eq!(enable.status(), StatusCode::OK);
        let set = app
            .clone()
            .oneshot(req(
                "PUT",
                &format!("/projects/{project}/agent-settings/review"),
                Some(json!({ "harness": "codex" })),
            ))
            .await
            .unwrap();
        assert_eq!(set.status(), StatusCode::OK);

        // Disabling codex now conflicts, and the 409 names the referencing slot.
        let disable = app
            .clone()
            .oneshot(req(
                "PUT",
                "/settings",
                Some(json!({ "enabled_harnesses": ["claude"] })),
            ))
            .await
            .unwrap();
        assert_eq!(disable.status(), StatusCode::CONFLICT);
        let body = body_json(disable).await;
        assert_eq!(body["error"]["code"], json!("conflict"));
        assert!(
            body["error"]["message"]
                .as_str()
                .unwrap()
                .contains(&format!("project {project} slot review")),
            "409 must list the referencing slot: {:?}",
            body["error"]["message"]
        );

        // Reset the slot override (null harness) — then disabling succeeds.
        let reset = app
            .clone()
            .oneshot(req(
                "PUT",
                &format!("/projects/{project}/agent-settings/review"),
                Some(json!({ "harness": null })),
            ))
            .await
            .unwrap();
        assert_eq!(reset.status(), StatusCode::OK);
        let disable = app
            .oneshot(req(
                "PUT",
                "/settings",
                Some(json!({ "enabled_harnesses": ["claude"] })),
            ))
            .await
            .unwrap();
        assert_eq!(disable.status(), StatusCode::OK);
    }

    /// The editor-prefill contract: `default_prompt` is non-empty for all
    /// nine slots and byte-identical to the constant each spawn site uses
    /// (no duplication, no drift between API and spawn path).
    #[tokio::test]
    async fn every_slot_serves_its_compiled_default_prompt() {
        use crate::breakdown::BREAKDOWN_PROMPT;
        use crate::planning::{PRODUCT_PLANNING_PROMPT, TECHNICAL_PLANNING_PROMPT};
        use crate::spec::prompt_for;
        use crate::task_agent::Stage;

        let expected = [
            (AgentSlot::PlanningProduct, PRODUCT_PLANNING_PROMPT),
            (AgentSlot::PlanningTechnical, TECHNICAL_PLANNING_PROMPT),
            (AgentSlot::Breakdown, BREAKDOWN_PROMPT),
            (AgentSlot::Implement, prompt_for(Stage::Implement).unwrap()),
            (AgentSlot::Fix, prompt_for(Stage::Fix).unwrap()),
            (AgentSlot::Review, prompt_for(Stage::Review).unwrap()),
            (
                AgentSlot::VerifyComplete,
                prompt_for(Stage::VerifyComplete).unwrap(),
            ),
            (AgentSlot::Summarize, prompt_for(Stage::Summarize).unwrap()),
            (AgentSlot::Triage, prompt_for(Stage::Triage).unwrap()),
        ];
        for (slot, text) in expected {
            assert!(!text.trim().is_empty());
            assert_eq!(default_prompt(slot), text, "slot {}", slot);
        }

        // And the API view carries the same text per slot.
        let (app, _state) = test_app().await;
        let project = create_project(&app).await;
        let got = app
            .oneshot(req(
                "GET",
                &format!("/projects/{project}/agent-settings"),
                None,
            ))
            .await
            .unwrap();
        assert_eq!(got.status(), StatusCode::OK);
        let body = body_json(got).await;
        for item in body["items"].as_array().unwrap() {
            let key = item["slot"].as_str().unwrap();
            let served = item["default_prompt"].as_str().unwrap();
            assert_eq!(served, default_prompt(AgentSlot::parse(key).unwrap()));
            assert!(!served.is_empty(), "slot {key} must prefill the editor");
        }
    }

    #[tokio::test]
    async fn get_agent_settings_lists_all_nine_slots_with_effective_values() {
        let (app, _state) = test_app().await;
        let project = create_project(&app).await;

        let got = app
            .clone()
            .oneshot(req(
                "GET",
                &format!("/projects/{project}/agent-settings"),
                None,
            ))
            .await
            .unwrap();
        assert_eq!(got.status(), StatusCode::OK);
        let body = body_json(got).await;
        let items = body["items"].as_array().unwrap();
        assert_eq!(items.len(), 9, "the closed slot vocabulary, all present");
        let slot_keys: Vec<&str> = items.iter().map(|i| i["slot"].as_str().unwrap()).collect();
        assert_eq!(
            slot_keys,
            vec![
                "planning_product",
                "planning_technical",
                "breakdown",
                "implement",
                "fix",
                "review",
                "verify_complete",
                "summarize",
                "triage"
            ]
        );
        // No overrides yet: raw facets null, effective resolves to the seed.
        let first = &items[0];
        assert_eq!(first["harness"], Json::Null);
        assert_eq!(first["model"], Json::Null);
        assert_eq!(first["system_prompt"], Json::Null);
        assert_eq!(first["effective"]["harness"], json!("claude"));
        assert_eq!(first["effective"]["model"], Json::Null);
        assert_eq!(first["effective"]["prompt_source"], json!("default"));

        // Unknown project → 404 envelope.
        let missing = app
            .oneshot(req("GET", "/projects/does-not-exist/agent-settings", None))
            .await
            .unwrap();
        assert_eq!(missing.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn put_agent_setting_sets_clears_and_deletes_on_full_reset() {
        let (app, state) = test_app().await;
        let project = create_project(&app).await;
        let uri = format!("/projects/{project}/agent-settings/implement");

        // Set harness (already enabled), model, and prompt.
        let set = app
            .clone()
            .oneshot(req(
                "PUT",
                &uri,
                Some(json!({
                    "harness": "claude",
                    "model": "  haiku  ",
                    "system_prompt": "  implement carefully  "
                })),
            ))
            .await
            .unwrap();
        assert_eq!(set.status(), StatusCode::OK);
        let view = body_json(set).await;
        assert_eq!(view["harness"], json!("claude"));
        assert_eq!(view["model"], json!("haiku"), "model is trimmed");
        assert_eq!(
            view["system_prompt"],
            json!("implement carefully"),
            "prompt is trimmed"
        );
        assert_eq!(view["effective"]["prompt_source"], json!("override"));

        // Clear one facet: null → cleared, others untouched.
        let clear_model = app
            .clone()
            .oneshot(req("PUT", &uri, Some(json!({ "model": null }))))
            .await
            .unwrap();
        assert_eq!(clear_model.status(), StatusCode::OK);
        let view = body_json(clear_model).await;
        assert_eq!(view["model"], Json::Null);
        assert_eq!(view["harness"], json!("claude"));

        // Clear the rest: the row is deleted outright, not parked as NULLs.
        let reset = app
            .clone()
            .oneshot(req(
                "PUT",
                &uri,
                Some(json!({ "harness": null, "system_prompt": null })),
            ))
            .await
            .unwrap();
        assert_eq!(reset.status(), StatusCode::OK);
        let view = body_json(reset).await;
        assert_eq!(view["harness"], Json::Null);
        assert_eq!(view["system_prompt"], Json::Null);
        assert_eq!(view["effective"]["prompt_source"], json!("default"));
        let mut rows = state
            .db
            .conn()
            .query(
                "SELECT COUNT(*) FROM agent_setting WHERE project_id = ?1",
                libsql::params![project.as_str()],
            )
            .await
            .unwrap();
        let count: i64 = rows.next().await.unwrap().unwrap().get(0).unwrap();
        assert_eq!(count, 0, "a fully-cleared override must not linger");
    }

    #[tokio::test]
    async fn put_agent_setting_validates_slot_project_and_harness() {
        let (app, _state) = test_app().await;
        let project = create_project(&app).await;

        // Unknown slot → 404.
        let bad_slot = app
            .clone()
            .oneshot(req(
                "PUT",
                &format!("/projects/{project}/agent-settings/time_traveler"),
                Some(json!({ "model": "m" })),
            ))
            .await
            .unwrap();
        assert_eq!(bad_slot.status(), StatusCode::NOT_FOUND);

        // Unknown project → 404.
        let bad_project = app
            .clone()
            .oneshot(req(
                "PUT",
                "/projects/does-not-exist/agent-settings/review",
                Some(json!({ "model": "m" })),
            ))
            .await
            .unwrap();
        assert_eq!(bad_project.status(), StatusCode::NOT_FOUND);

        // Harness outside the global enablement set → 400.
        let disabled = app
            .clone()
            .oneshot(req(
                "PUT",
                &format!("/projects/{project}/agent-settings/review"),
                Some(json!({ "harness": "codex" })),
            ))
            .await
            .unwrap();
        assert_eq!(disabled.status(), StatusCode::BAD_REQUEST);

        // Blank model → 400.
        let blank = app
            .oneshot(req(
                "PUT",
                &format!("/projects/{project}/agent-settings/review"),
                Some(json!({ "model": "" })),
            ))
            .await
            .unwrap();
        assert_eq!(blank.status(), StatusCode::BAD_REQUEST);
    }
}
