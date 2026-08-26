//! Project cost aggregation — `GET /projects/{id}/cost` (cost-tracking epic,
//! Phase 3).
//!
//! Aggregates the project's **closed successful** agent runs (`status='ok'`,
//! `ended_at` stamped) into three bucketings for the cost graphs:
//!
//! - [`ProjectCost::by_slot`]: one row per `agent_run.stage` (the §2.2
//!   vocabulary — "agent slot" in product terms).
//! - [`ProjectCost::by_harness_model`]: one row per `(harness, model)` pair;
//!   NULL harness/model groups as its own bucket, exactly like SQL GROUP BY
//!   treats NULLs.
//! - [`ProjectCost::by_day`]: one row per calendar day of `ended_at`,
//!   ordered ascending.
//!
//! Every row sums input/output tokens and carries server-computed
//! `estimated_input_usd` / `estimated_output_usd` derived from the static
//! [`rate_for`] table. Estimates are **`null` when any contributing model is
//! missing from the table** (or the model column is NULL) — never a silent
//! `$0`, so the client can render an unmistakable "unknown rate" indicator
//! instead of an understated number.
//!
//! Runs join to their project via task/epic, per the technical plan's locked-in
//! SQL: an `agent_run` belongs either directly to an epic (`epic_id`, the
//! planning/breakdown runs) or to a task that hangs off an epic.

use std::collections::BTreeMap;

use axum::extract::{Path, State};
use axum::Json;
use libsql::params;
use serde::Serialize;

use crate::{AppError, AppResult, AppState};

// ---- the static rate table --------------------------------------------------

/// Strip known provider prefixes from a raw model string so every spelling of
/// the same model resolves to one rate-table key: `"anthropic/claude-sonnet-5"`
/// and `"claude-sonnet-5"` must land on the same entry. Applied repeatedly (an
/// OpenRouter id often nests another provider prefix inside), lowercased.
fn normalize_model(model: &str) -> String {
    let mut m = model.trim().to_ascii_lowercase();
    loop {
        if let Some(rest) = m.strip_prefix("openrouter/") {
            m = rest.to_string();
        } else if let Some(rest) = m.strip_prefix("anthropic/") {
            m = rest.to_string();
        } else if let Some(rest) = m.strip_prefix("us.anthropic.") {
            m = rest.to_string();
        } else {
            return m;
        }
    }
}

/// Rate per 1M tokens → dollars for the given token count.
fn usd(usd_per_1m: f64, tokens: i64) -> f64 {
    usd_per_1m * tokens as f64 / 1_000_000.0
}

/// The bundled static rate table: normalized model string →
/// `(input_usd_per_1m, output_usd_per_1m)`. Public API-equivalent list prices;
/// deliberately approximate — the UI labels everything derived from it as an
/// estimate ("API-equivalent pricing — not your actual bill").
///
/// Covers the Claude 3/4/5 families plus common OpenRouter catalog ids.
/// Anything unknown returns `None`, and callers surface that as JSON `null`
/// estimates rather than pretending $0.
pub fn rate_for(model: &str) -> Option<(f64, f64)> {
    let m = normalize_model(model);
    // Claude families: date-suffixed ids (`claude-sonnet-4-20250514`) share the
    // rate of their base model, hence prefix guards rather than exact matches.
    if m.starts_with("claude-opus-3")
        || m.starts_with("claude-opus-4")
        || m.starts_with("claude-opus-5")
    {
        return Some((15.0, 75.0));
    }
    if m.starts_with("claude-sonnet-5") {
        return Some((3.0, 15.0));
    }
    if m.starts_with("claude-sonnet-4")
        || m.starts_with("claude-3-7-sonnet")
        || m.starts_with("claude-3-5-sonnet")
    {
        return Some((3.0, 15.0));
    }
    if m.starts_with("claude-haiku-5")
        || m.starts_with("claude-haiku-4")
        || m.starts_with("claude-3-5-haiku")
    {
        return Some((1.0, 5.0));
    }
    if m.starts_with("claude-3-haiku") {
        return Some((0.25, 1.25));
    }

    // Common OpenRouter catalog ids (`normalize_model` already stripped the
    // `openrouter/` wrapper, so these are `<publisher>/<model>` bodies).
    match m.as_str() {
        "deepseek/deepseek-r1" => Some((0.55, 2.19)),
        "deepseek/deepseek-chat" => Some((0.27, 1.1)),
        "meta-llama/llama-3.3-70b-instruct" => Some((0.12, 0.3)),
        "meta-llama/llama-3.1-8b-instruct" => Some((0.02, 0.05)),
        "google/gemini-2.0-flash-001" => Some((0.1, 0.4)),
        "google/gemini-flash-1.5" => Some((0.075, 0.3)),
        "qwen/qwen-2.5-72b-instruct" => Some((0.35, 0.4)),
        "mistralai/mistral-large" => Some((2.0, 6.0)),
        _ => None,
    }
}

// ---- aggregation rows --------------------------------------------------------

/// Token sums plus estimated USD for one aggregation bucket. The USD fields are
/// `None` whenever the bucket's model coverage is incomplete — serialized as
/// JSON `null`.
#[derive(Debug, Clone, Serialize)]
pub struct CostTotals {
    pub input_tokens: i64,
    pub output_tokens: i64,
    pub estimated_input_usd: Option<f64>,
    pub estimated_output_usd: Option<f64>,
}

/// Fold the per-model sub-totals of one bucket into its [`CostTotals`]. Tokens
/// always add up; the USD estimate is `Σ rate(model) × tokens(model)` and drops
/// to `null` if any sub-total with nonzero tokens has an unknown (or NULL)
/// model — a partial estimate would look authoritative while being wrong.
fn fold_bucket(sub_totals: Vec<(Option<String>, i64, i64)>) -> CostTotals {
    let mut input_tokens = 0i64;
    let mut output_tokens = 0i64;
    let mut fully_priced = true;
    let mut est_input = 0f64;
    let mut est_output = 0f64;

    for (model, input, output) in sub_totals {
        input_tokens += input;
        output_tokens += output;
        match model.as_deref().and_then(rate_for) {
            Some((in_rate, out_rate)) => {
                est_input += usd(in_rate, input);
                est_output += usd(out_rate, output);
            }
            None => {
                if input != 0 || output != 0 {
                    fully_priced = false;
                }
            }
        }
    }

    let (estimated_input_usd, estimated_output_usd) = if fully_priced {
        (Some(est_input), Some(est_output))
    } else {
        (None, None)
    };
    CostTotals {
        input_tokens,
        output_tokens,
        estimated_input_usd,
        estimated_output_usd,
    }
}

// ---- wire shape --------------------------------------------------------------

/// One `by_slot` row: all closed successful runs of one stage, summed.
#[derive(Debug, Clone, Serialize)]
pub struct CostBySlot {
    pub slot: String,
    #[serde(flatten)]
    pub totals: CostTotals,
}

/// One `by_harness_model` row. `harness`/`model` may be `null` on the wire
/// (rows predating those columns); NULL groups as its own bucket.
#[derive(Debug, Clone, Serialize)]
pub struct CostByHarnessModel {
    pub harness: Option<String>,
    pub model: Option<String>,
    #[serde(flatten)]
    pub totals: CostTotals,
}

/// One `by_day` row: calendar day of `ended_at`, ascending.
#[derive(Debug, Clone, Serialize)]
pub struct CostByDay {
    pub date: String,
    #[serde(flatten)]
    pub totals: CostTotals,
}

/// All three aggregations for one project — the wire shape of
/// `GET /projects/{id}/cost`.
#[derive(Debug, Clone, Serialize)]
pub struct ProjectCost {
    pub by_slot: Vec<CostBySlot>,
    pub by_harness_model: Vec<CostByHarnessModel>,
    pub by_day: Vec<CostByDay>,
}

// ---- queries -----------------------------------------------------------------

/// The shared join pattern: only runs reachable from `project_id` whose stage
/// closed successfully contribute to any bucket.
const COST_RUNS_FROM: &str = "\
     FROM agent_run ar \
     LEFT JOIN task t ON ar.task_id = t.id \
     LEFT JOIN epic e ON COALESCE(ar.epic_id, t.epic_id) = e.id \
     WHERE e.project_id = ?1 \
       AND ar.status = 'ok' \
       AND ar.ended_at IS NOT NULL";

/// Load the three aggregations for `project_id`. Does **not** check project
/// existence — callers guard with [`crate::epics::project_exists`] for a clean
/// 404 (same convention as `board::load_board`).
pub async fn load_project_cost(
    conn: &libsql::Connection,
    project_id: &str,
) -> AppResult<ProjectCost> {
    // Per-slot totals. A bucket can mix models, so the SQL groups by
    // (stage, model) and `fold_bucket` combines each stage's sub-rows.
    // (`BTreeMap` keeps bucket order stable and, for free, orders `by_day`
    // ascending by its date key.)
    let mut rows = conn
        .query(
            &format!(
                "SELECT ar.stage, ar.model, \
                        SUM(COALESCE(ar.input_tokens, 0)), \
                        SUM(COALESCE(ar.output_tokens, 0)) \
                 {COST_RUNS_FROM} \
                 GROUP BY ar.stage, ar.model"
            ),
            params![project_id],
        )
        .await?;
    let mut slot_subs: BTreeMap<String, Vec<(Option<String>, i64, i64)>> = BTreeMap::new();
    while let Some(row) = rows.next().await? {
        let slot: String = row.get(0)?;
        let model: Option<String> = row.get(1)?;
        slot_subs
            .entry(slot)
            .or_default()
            .push((model, row.get(2)?, row.get(3)?));
    }

    // Per (harness, model) totals — one row per pair straight out of SQL; the
    // model is uniform here so each row folds alone. NULLs group together.
    let mut rows = conn
        .query(
            &format!(
                "SELECT ar.harness, ar.model, \
                        SUM(COALESCE(ar.input_tokens, 0)), \
                        SUM(COALESCE(ar.output_tokens, 0)) \
                 {COST_RUNS_FROM} \
                 GROUP BY ar.harness, ar.model"
            ),
            params![project_id],
        )
        .await?;
    let mut by_harness_model = Vec::new();
    while let Some(row) = rows.next().await? {
        let harness: Option<String> = row.get(0)?;
        let model: Option<String> = row.get(1)?;
        let sub = vec![(model.clone(), row.get::<i64>(2)?, row.get::<i64>(3)?)];
        by_harness_model.push(CostByHarnessModel {
            harness,
            model,
            totals: fold_bucket(sub),
        });
    }

    // Calendar-day buckets of ended_at (ms epoch → UTC date), ascending.
    let mut rows = conn
        .query(
            &format!(
                "SELECT DATE(datetime(ar.ended_at / 1000, 'unixepoch')) AS day, ar.model, \
                        SUM(COALESCE(ar.input_tokens, 0)), \
                        SUM(COALESCE(ar.output_tokens, 0)) \
                 {COST_RUNS_FROM} \
                 GROUP BY day, ar.model \
                 ORDER BY day ASC"
            ),
            params![project_id],
        )
        .await?;
    let mut day_subs: BTreeMap<String, Vec<(Option<String>, i64, i64)>> = BTreeMap::new();
    while let Some(row) = rows.next().await? {
        let date: String = row.get(0)?;
        let model: Option<String> = row.get(1)?;
        day_subs
            .entry(date)
            .or_default()
            .push((model, row.get(2)?, row.get(3)?));
    }

    Ok(ProjectCost {
        by_slot: slot_subs
            .into_iter()
            .map(|(slot, subs)| CostBySlot {
                slot,
                totals: fold_bucket(subs),
            })
            .collect(),
        by_harness_model,
        by_day: day_subs
            .into_iter()
            .map(|(date, subs)| CostByDay {
                date,
                totals: fold_bucket(subs),
            })
            .collect(),
    })
}

/// `GET /projects/:id/cost` — aggregated token + estimated-USD data for the
/// project's closed successful agent runs. `404` if the project does not exist.
pub async fn get_project_cost(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> AppResult<Json<ProjectCost>> {
    let conn = state.db.conn();
    if !crate::epics::project_exists(conn, &id).await? {
        return Err(AppError::NotFound(format!("project {id} not found")));
    }
    Ok(Json(load_project_cost(conn, &id).await?))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::evidence::{close_stage, open_stage, CloseStage, OpenStage};
    use crate::planning::testing::SilentPlanningAgent;
    use crate::{app, Config, Db};
    use axum::body::Body;
    use axum::http::{header::AUTHORIZATION, Request, StatusCode};
    use serde_json::{json, Value as Json};
    use tower::ServiceExt;

    // ---- rate table + normalization ------------------------------------

    #[test]
    fn provider_prefixes_normalize_to_the_same_rate() {
        let bare = rate_for("claude-sonnet-5");
        assert_eq!(rate_for("anthropic/claude-sonnet-5"), bare);
        assert_eq!(rate_for("openrouter/anthropic/claude-sonnet-5"), bare);
        assert_eq!(rate_for("us.anthropic.claude-sonnet-5"), bare);
        assert_eq!(rate_for("Claude-Sonnet-5"), bare, "case-insensitive");

        let opus = rate_for("claude-opus-4");
        assert_eq!(rate_for("openrouter/anthropic/claude-opus-4"), opus);
    }

    #[test]
    fn unknown_models_yield_no_rate() {
        assert!(rate_for("gibberish-model-v99").is_none());
        assert!(rate_for("").is_none());
    }

    #[test]
    fn date_suffixed_claude_ids_share_their_base_models_rate() {
        assert_eq!(
            rate_for("claude-sonnet-4-20250514"),
            rate_for("claude-sonnet-4")
        );
        assert_eq!(
            rate_for("anthropic/claude-3-5-haiku-20241022"),
            rate_for("claude-3-5-haiku")
        );
    }

    #[test]
    fn fold_bucket_nulls_estimates_when_any_contributing_model_is_unknown() {
        let priced = fold_bucket(vec![(Some("claude-sonnet-5".into()), 1_000_000, 500_000)]);
        assert_eq!(priced.input_tokens, 1_000_000);
        assert_eq!(priced.estimated_input_usd, Some(3.0));
        assert_eq!(priced.estimated_output_usd, Some(7.5));

        // Mixing in an unpriced model with tokens poisons the whole estimate.
        let mixed = fold_bucket(vec![
            (Some("claude-sonnet-5".into()), 1_000_000, 0),
            (Some("mystery-model".into()), 1_000_000, 0),
        ]);
        assert_eq!(mixed.input_tokens, 2_000_000);
        assert_eq!(mixed.estimated_input_usd, None);

        // A NULL-model sub-bucket with zero tokens does not poison anything.
        let harmless = fold_bucket(vec![(Some("claude-sonnet-5".into()), 0, 0), (None, 0, 0)]);
        assert_eq!(harmless.estimated_input_usd, Some(0.0));

        // An entirely NULL-model bucket is unpriced.
        let unpriced = fold_bucket(vec![(None, 100, 200)]);
        assert_eq!(unpriced.input_tokens, 100);
        assert_eq!(unpriced.estimated_input_usd, None);
    }

    // ---- HTTP endpoint ---------------------------------------------------

    async fn test_app() -> (AppState, axum::Router) {
        let db = Db::connect(":memory:").await.unwrap();
        db.run_migrations().await.unwrap();
        let state = AppState::with_planner(
            Config::for_test(),
            db,
            std::sync::Arc::new(SilentPlanningAgent),
        );
        let app = app(state.clone());
        (state, app)
    }

    /// The bearer credential HTTP tests present, minted **once per process**
    /// from a seeded active admin (`crate::users::testing::seed_user` +
    /// `crate::sessions::testing::login_as`). Stateless HMAC verification means
    /// one mint authenticates against every in-memory instance these tests boot.
    fn auth_bearer() -> &'static str {
        static BEARER: std::sync::OnceLock<String> = std::sync::OnceLock::new();
        BEARER.get_or_init(|| {
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

    fn get(uri: &str) -> Request<Body> {
        Request::builder()
            .uri(uri)
            .header(AUTHORIZATION, format!("Bearer {}", auth_bearer()))
            .body(Body::empty())
            .unwrap()
    }

    /// Float equality within f64 rounding noise (rate arithmetic passes through
    /// non-exact binary fractions like 0.55).
    fn approx(got: f64, want: f64) {
        assert!((got - want).abs() < 1e-9, "got {got}, want {want}");
    }

    async fn body_json(response: axum::response::Response) -> Json {
        let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        serde_json::from_slice(&bytes).unwrap()
    }

    /// Seed project → epic → task rows and return the ids.
    async fn seed_project_epic_task(state: &AppState, project_id: &str) -> (String, String) {
        let conn = state.db.conn();
        let epic_id = ulid::Ulid::new().to_string();
        let task_id = ulid::Ulid::new().to_string();
        conn.execute(
            "INSERT INTO project (id, name, repo_url, clone_status, created_at, updated_at) \
             VALUES (?1, 'P', 'https://example.com/p.git', 'ready', 0, 0)",
            params![project_id],
        )
        .await
        .unwrap();
        conn.execute(
            "INSERT INTO epic (id, project_id, title, status, created_at, updated_at) \
             VALUES (?1, ?2, 'E', 'InProgress', 0, 0)",
            params![epic_id.clone(), project_id],
        )
        .await
        .unwrap();
        conn.execute(
            "INSERT INTO task (id, epic_id, project_id, title, status, position, created_at, updated_at) \
             VALUES (?1, ?2, ?3, 'T', 'InProgress', 1, 0, 0)",
            params![task_id.clone(), epic_id.clone(), project_id],
        )
        .await
        .unwrap();
        (epic_id, task_id)
    }

    /// Open + close one `agent_run` row through the real lifecycle helpers.
    /// `ended_at_ms: None` leaves the row running; otherwise the row closes at
    /// that timestamp with `status` and the given tokens/harness/model.
    #[allow(clippy::too_many_arguments)]
    async fn seed_run(
        state: &AppState,
        epic_id: &str,
        task_id: &str,
        stage: &str,
        ended_at_ms: Option<i64>,
        status: &'static str,
        input_tokens: Option<u64>,
        output_tokens: Option<u64>,
        harness: Option<&str>,
        model: Option<&str>,
    ) {
        let conn = state.db.conn();
        let handle = open_stage(
            conn,
            OpenStage {
                task_id: Some(task_id),
                epic_id: Some(epic_id),
                stage,
                attempt: 1,
                harness,
                model,
                prompt_hash: None,
            },
        )
        .await
        .unwrap();
        match ended_at_ms {
            None => {} // left `running` on purpose
            Some(ms) => {
                close_stage(
                    conn,
                    &handle,
                    CloseStage {
                        status,
                        session_id: None,
                        verdict: None,
                        exit_code: Some(0),
                        log: String::new(),
                        input_tokens,
                        output_tokens,
                    },
                )
                .await
                .unwrap();
                // Stamp the deterministic ended_at after the fact so day
                // bucketing is testable.
                conn.execute(
                    "UPDATE agent_run SET ended_at = ?1 WHERE id = ?2",
                    params![ms, handle.id],
                )
                .await
                .unwrap();
            }
        }
    }

    /// 2026-08-24 12:00 UTC and 2026-08-25 06:00 UTC, ms epoch.
    const DAY_A_MS: i64 = 1_787_572_800_000;
    const DAY_B_MS: i64 = 1_787_637_600_000;

    #[tokio::test]
    async fn cost_endpoint_aggregates_only_closed_successful_runs() {
        let (state, app) = test_app().await;
        let project_id = ulid::Ulid::new().to_string();
        let (epic_id, task_id) = seed_project_epic_task(&state, &project_id).await;

        // Contributes: implement @ claude-sonnet-5 on day A.
        seed_run(
            &state,
            &epic_id,
            &task_id,
            "implement",
            Some(DAY_A_MS),
            "ok",
            Some(1_000_000),
            Some(500_000),
            Some("pi"),
            Some("anthropic/claude-sonnet-5"),
        )
        .await;
        // Contributes: review @ claude-sonnet-5 also on day A (same bucket).
        seed_run(
            &state,
            &epic_id,
            &task_id,
            "review",
            Some(DAY_A_MS + 60_000),
            "ok",
            Some(300_000),
            Some(100_000),
            Some("pi"),
            Some("claude-sonnet-5"),
        )
        .await;
        // Contributes: fix @ deepseek-r1 on day B.
        seed_run(
            &state,
            &epic_id,
            &task_id,
            "fix",
            Some(DAY_B_MS),
            "ok",
            Some(2_000_000),
            Some(1_000_000),
            Some("pi"),
            Some("openrouter/deepseek/deepseek-r1"),
        )
        .await;
        // Excluded: still running.
        seed_run(
            &state,
            &epic_id,
            &task_id,
            "implement",
            None,
            "ok",
            Some(999_999),
            Some(999_999),
            Some("pi"),
            Some("claude-sonnet-5"),
        )
        .await;
        // Excluded: closed unsuccessfully (error status, tokens stamped anyway).
        seed_run(
            &state,
            &epic_id,
            &task_id,
            "implement",
            Some(DAY_A_MS),
            "error",
            Some(888_888),
            Some(888_888),
            Some("pi"),
            Some("claude-sonnet-5"),
        )
        .await;
        // Excluded: belongs to another project.
        let other = ulid::Ulid::new().to_string();
        let (other_epic, other_task) = seed_project_epic_task(&state, &other).await;
        seed_run(
            &state,
            &other_epic,
            &other_task,
            "implement",
            Some(DAY_A_MS),
            "ok",
            Some(777_777),
            Some(777_777),
            Some("pi"),
            Some("claude-sonnet-5"),
        )
        .await;

        let response = app
            .oneshot(get(&format!("/projects/{project_id}/cost")))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let cost = body_json(response).await;

        // by_slot: implement 1M/0.5M, fix 2M/1M, review 0.3M/0.1M.
        let slots = cost["by_slot"].as_array().unwrap();
        assert_eq!(slots.len(), 3, "{cost}");
        let find_slot = |name: &str| {
            slots
                .iter()
                .find(|s| s["slot"] == name)
                .expect("slot present")
                .clone()
        };
        let implement = find_slot("implement");
        assert_eq!(implement["input_tokens"], 1_000_000);
        assert_eq!(implement["output_tokens"], 500_000);
        // Sonnet 5: $3/$15 per 1M → 3.00 / 7.50.
        assert_eq!(implement["estimated_input_usd"], json!(3.0));
        assert_eq!(implement["estimated_output_usd"], json!(7.5));

        // by_harness_model: buckets group on the *stored* model string, so
        // `anthropic/claude-sonnet-5` and `claude-sonnet-5` are separate rows —
        // but normalization prices them identically ($3/$15 per 1M).
        let hm = cost["by_harness_model"].as_array().unwrap();
        assert_eq!(hm.len(), 3, "{cost}");
        let find_bucket = |model: &str| {
            hm.iter()
                .find(|r| r["model"] == model)
                .expect("bucket present")
                .clone()
        };
        let prefixed = find_bucket("anthropic/claude-sonnet-5");
        assert_eq!(prefixed["harness"], "pi");
        assert_eq!(prefixed["input_tokens"], 1_000_000);
        assert_eq!(prefixed["output_tokens"], 500_000);
        approx(prefixed["estimated_input_usd"].as_f64().unwrap(), 3.0);
        approx(prefixed["estimated_output_usd"].as_f64().unwrap(), 7.5);
        let bare = find_bucket("claude-sonnet-5");
        // Same rate despite the different stored spelling.
        approx(bare["estimated_input_usd"].as_f64().unwrap(), 0.9);
        assert_eq!(bare["estimated_output_usd"], json!(1.5));
        let r1 = find_bucket("openrouter/deepseek/deepseek-r1");
        assert_eq!(r1["input_tokens"], 2_000_000);
        approx(r1["estimated_input_usd"].as_f64().unwrap(), 1.1);

        // by_day: ascending dates, both days present.
        let days = cost["by_day"].as_array().unwrap();
        assert_eq!(days.len(), 2, "{cost}");
        assert_eq!(days[0]["date"], "2026-08-24");
        assert_eq!(days[1]["date"], "2026-08-25");
        assert_eq!(days[0]["input_tokens"], 1_300_000);
        assert_eq!(days[1]["input_tokens"], 2_000_000);
    }

    #[tokio::test]
    async fn unknown_or_missing_models_get_null_estimates_never_zero() {
        let (state, app) = test_app().await;
        let project_id = ulid::Ulid::new().to_string();
        let (epic_id, task_id) = seed_project_epic_task(&state, &project_id).await;

        // Unknown model with real tokens → null estimates, tokens intact.
        seed_run(
            &state,
            &epic_id,
            &task_id,
            "implement",
            Some(DAY_A_MS),
            "ok",
            Some(50_000),
            Some(20_000),
            Some("pi"),
            Some("mystery-model-v99"),
        )
        .await;
        // NULL model entirely (pre-column row shape).
        seed_run(
            &state,
            &epic_id,
            &task_id,
            "review",
            Some(DAY_B_MS),
            "ok",
            Some(10_000),
            Some(5_000),
            None,
            None,
        )
        .await;

        let response = app
            .oneshot(get(&format!("/projects/{project_id}/cost")))
            .await
            .unwrap();
        let cost = body_json(response).await;

        for section in ["by_slot", "by_harness_model", "by_day"] {
            for row in cost[section].as_array().unwrap() {
                assert_eq!(
                    row["estimated_input_usd"],
                    Json::Null,
                    "{section} row must have null estimates: {row}"
                );
                assert_eq!(row["estimated_output_usd"], Json::Null);
            }
        }

        // Tokens still aggregate even when unpriced.
        let implement = cost["by_slot"]
            .as_array()
            .unwrap()
            .iter()
            .find(|s| s["slot"] == "implement")
            .unwrap();
        assert_eq!(implement["input_tokens"], 50_000);
        assert_eq!(implement["output_tokens"], 20_000);
        // NULL harness/model is its own bucket, serialized as JSON nulls.
        let null_row = cost["by_harness_model"]
            .as_array()
            .unwrap()
            .iter()
            .find(|r| r["harness"] == Json::Null && r["model"] == Json::Null)
            .expect("NULL harness/model forms its own bucket");
        assert_eq!(null_row["input_tokens"], 10_000);
    }

    #[tokio::test]
    async fn empty_project_returns_empty_arrays() {
        let (state, app) = test_app().await;
        let project_id = ulid::Ulid::new().to_string();
        seed_project_epic_task(&state, &project_id).await;

        let response = app
            .oneshot(get(&format!("/projects/{project_id}/cost")))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let cost = body_json(response).await;
        assert!(cost["by_slot"].as_array().unwrap().is_empty());
        assert!(cost["by_harness_model"].as_array().unwrap().is_empty());
        assert!(cost["by_day"].as_array().unwrap().is_empty());
    }

    #[tokio::test]
    async fn unknown_project_is_404() {
        let (_state, app) = test_app().await;
        let response = app.oneshot(get("/projects/nope/cost")).await.unwrap();
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
        assert_eq!(body_json(response).await["error"]["code"], "not_found");
    }
}
