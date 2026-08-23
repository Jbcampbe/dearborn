//! The agent-slot vocabulary (design §1).
//!
//! An **agent slot** is one configurable point in Dearborn's pipeline: a
//! closed, compile-time enum of the eight places Dearborn runs a coding agent.
//! Settings (harness, model, system prompt) are keyed per slot; the closed
//! enum guarantees the settings API, the stores, and the worker can never
//! disagree about what exists — a new slot arrives with a code change, never
//! with a stray settings row.
//!
//! Wire format is the stable snake_case key (`"planning_product"`, …), the
//! same convention as the stage vocabulary in [`crate::task_agent::Stage`].
//! Slot keys are stable forever: they are persisted in `agent_setting.slot`
//! and appear in API paths, so renaming one would be a data migration, not a
//! refactor.

use serde::Deserialize;
use serde::Serialize;
use std::fmt;

/// One configurable agent point in the pipeline (design §1's table).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AgentSlot {
    /// Interactive epic planning, product phase (`PRODUCT_PLANNING_PROMPT`).
    PlanningProduct,
    /// Interactive epic planning, technical phase (`TECHNICAL_PLANNING_PROMPT`).
    PlanningTechnical,
    /// One-shot epic → task DAG breakdown.
    Breakdown,
    /// Per-task implementation stage.
    Implement,
    /// Test-gate / review fix-loop stage.
    Fix,
    /// Review + VERDICT stage.
    Review,
    /// Completion check stage.
    VerifyComplete,
    /// Task summary stage (feeds the PR body).
    Summarize,
}

impl AgentSlot {
    /// Every slot, in the canonical display order used by the settings API
    /// and the client's slot cards (design §1's table order).
    pub const ALL: &'static [AgentSlot] = &[
        AgentSlot::PlanningProduct,
        AgentSlot::PlanningTechnical,
        AgentSlot::Breakdown,
        AgentSlot::Implement,
        AgentSlot::Fix,
        AgentSlot::Review,
        AgentSlot::VerifyComplete,
        AgentSlot::Summarize,
    ];

    /// The stable snake_case wire/storage key.
    pub fn as_str(&self) -> &'static str {
        match self {
            AgentSlot::PlanningProduct => "planning_product",
            AgentSlot::PlanningTechnical => "planning_technical",
            AgentSlot::Breakdown => "breakdown",
            AgentSlot::Implement => "implement",
            AgentSlot::Fix => "fix",
            AgentSlot::Review => "review",
            AgentSlot::VerifyComplete => "verify_complete",
            AgentSlot::Summarize => "summarize",
        }
    }

    /// Parse a slot key (a path param or a `agent_setting.slot` value).
    /// Inverse of [`AgentSlot::as_str`]; case-sensitive on purpose — keys are
    /// machine-generated, and silently accepting `Implement` would let a
    /// typo'd client create settings rows the real key never matches.
    pub fn parse(key: &str) -> Option<AgentSlot> {
        AgentSlot::ALL.iter().copied().find(|s| s.as_str() == key)
    }
}

impl fmt::Display for AgentSlot {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn keys_are_stable_snake_case() {
        assert_eq!(AgentSlot::PlanningProduct.as_str(), "planning_product");
        assert_eq!(AgentSlot::PlanningTechnical.as_str(), "planning_technical");
        assert_eq!(AgentSlot::Breakdown.as_str(), "breakdown");
        assert_eq!(AgentSlot::Implement.as_str(), "implement");
        assert_eq!(AgentSlot::Fix.as_str(), "fix");
        assert_eq!(AgentSlot::Review.as_str(), "review");
        assert_eq!(AgentSlot::VerifyComplete.as_str(), "verify_complete");
        assert_eq!(AgentSlot::Summarize.as_str(), "summarize");
    }

    #[test]
    fn parse_is_the_inverse_of_as_str_for_every_slot() {
        for slot in AgentSlot::ALL {
            assert_eq!(AgentSlot::parse(slot.as_str()), Some(*slot));
        }
    }

    #[test]
    fn parse_rejects_unknown_and_mismatched_case() {
        assert_eq!(AgentSlot::parse("nonexistent"), None);
        assert_eq!(AgentSlot::parse("Implement"), None);
        assert_eq!(AgentSlot::parse(""), None);
    }

    #[test]
    fn serde_round_trips_through_the_snake_case_key() {
        for slot in AgentSlot::ALL {
            let json = serde_json::to_string(slot).unwrap();
            assert_eq!(json, format!("\"{}\"", slot.as_str()));
            assert_eq!(serde_json::from_str::<AgentSlot>(&json).unwrap(), *slot);
        }
    }

    #[test]
    fn all_covers_every_variant() {
        // Compile-time exhaustiveness guard: if a variant is added but not
        // listed in ALL, this non-exhaustive match fails to compile.
        for slot in AgentSlot::ALL {
            match slot {
                AgentSlot::PlanningProduct => {}
                AgentSlot::PlanningTechnical => {}
                AgentSlot::Breakdown => {}
                AgentSlot::Implement => {}
                AgentSlot::Fix => {}
                AgentSlot::Review => {}
                AgentSlot::VerifyComplete => {}
                AgentSlot::Summarize => {}
            }
        }
        assert_eq!(AgentSlot::ALL.len(), 8);
    }
}
