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
//! This module deliberately holds no HTTP surface — endpoints land in a later
//! phase and are thin wrappers over these functions.

use std::collections::HashMap;

use libsql::params;
use serde::Serialize;
use thiserror::Error;

use crate::agent_slot::AgentSlot;
use crate::db::Db;

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
pub async fn save_global_settings(
    db: &Db,
    settings: &GlobalSettings,
) -> Result<(), SettingsError> {
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
            params![
                settings.default_harness.clone(),
                default_models,
                enabled
            ],
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
    let slot = AgentSlot::parse(&slot_key)
        .ok_or_else(|| SettingsError::UnknownSlot(slot_key.clone()))?;
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
    Ok(rows.next().await?.map(|row| row_to_setting(&row)).transpose()?)
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
        .or_else(|| {
            global
                .default_models
                .get(&harness)
                .cloned()
                .unwrap_or(None)
        });
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
            resolve_effective(&g, None).model, None,
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
        let g = global(
            "claude",
            &[("claude", Some("sonnet")), ("codex", None)],
        );
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
        assert_eq!(get_global_settings(&db).await.unwrap(), GlobalSettings::default());
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
}
