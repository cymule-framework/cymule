use std::collections::{BTreeMap, BTreeSet};

use cymule_core::{ArtifactRef, SealedPlan};
use serde::{Deserialize, Serialize};

/// One declared semantic change between immutable Plans.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PatchOperation {
    /// Stable operation kind such as `add`, `remove`, or `replace`.
    pub kind: String,
    /// Stable definition, site, schema, or contract target.
    pub target: String,
    /// Optional prior semantic digest.
    pub before: Option<String>,
    /// Optional replacement semantic digest.
    pub after: Option<String>,
}

/// Immutable Plan DAG edge with reviewed patch evidence.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PlanEdge {
    /// Content-addressed edge identity.
    pub edge_id: String,
    /// Parent Plan.
    pub from_plan: String,
    /// Child Plan.
    pub to_plan: String,
    /// Declared semantic operations.
    pub operations: Vec<PatchOperation>,
    /// Review or compiler evidence artifact.
    pub evidence: ArtifactRef,
}

/// One immutable node in the Plan DAG.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PlanNode {
    /// Sealed Plan.
    pub plan: SealedPlan,
    /// Incoming edge identities.
    pub incoming: BTreeSet<String>,
}

/// Conservative impact analysis for active execution.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ImpactCone {
    /// Patch edge analyzed.
    pub edge_id: String,
    /// Changed stable targets.
    pub changed_targets: BTreeSet<String>,
    /// Run identities with an affected active frame.
    pub affected_runs: BTreeSet<String>,
    /// Released effect identities requiring old interpretation.
    pub pinned_effects: BTreeSet<String>,
    /// Whether state migration evidence is required.
    pub requires_migration: bool,
}

/// Future-occurrence rollout policy.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "mode", rename_all = "snake_case")]
pub enum RolloutMode {
    /// Keep the current Plan authoritative and run target only for evidence.
    Shadow,
    /// Deterministically select target for a bounded share of future work.
    Canary {
        /// Target share in basis points, 0..=10000.
        basis_points: u16,
    },
    /// Select target for all future work.
    Active,
    /// Select fallback for all future work after a failed rollout.
    RolledBack,
}

/// Versioned decision for future occurrence selection.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RolloutDecision {
    /// Stable decision identity.
    pub decision_id: String,
    /// Current/fallback Plan.
    pub fallback_plan: String,
    /// Candidate/target Plan.
    pub target_plan: String,
    /// Selection mode.
    pub mode: RolloutMode,
}

/// Safe-point state migration receipt.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MigrationReceipt {
    /// Stable migration identity.
    pub migration_id: String,
    /// Migrated Run.
    pub run_id: String,
    /// Source Plan.
    pub from_plan: String,
    /// Target Plan.
    pub to_plan: String,
    /// Source state artifact.
    pub input_state: ArtifactRef,
    /// Migrated state artifact.
    pub output_state: ArtifactRef,
    /// Schema/migration evidence.
    pub evidence: ArtifactRef,
}

/// Evidence comparing shadow and authoritative results.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ShadowComparison {
    /// Stable comparison identity.
    pub comparison_id: String,
    /// Run or occurrence identity.
    pub subject: String,
    /// Authoritative Plan.
    pub primary_plan: String,
    /// Shadow Plan.
    pub shadow_plan: String,
    /// Authoritative result digest.
    pub primary_digest: String,
    /// Shadow result digest.
    pub shadow_digest: String,
    /// Whether results are equivalent under declared comparison semantics.
    pub equivalent: bool,
}

/// Portable complete live-evolution state.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EvolutionSnapshot {
    /// Plan nodes keyed by Plan ID.
    pub plans: BTreeMap<String, PlanNode>,
    /// DAG edges keyed by edge ID.
    pub edges: BTreeMap<String, PlanEdge>,
    /// Current rollout decision.
    pub rollout: Option<RolloutDecision>,
    /// Immutable Plan assignment per admitted occurrence.
    pub occurrence_plans: BTreeMap<String, String>,
    /// Migration receipts keyed by migration ID.
    pub migrations: BTreeMap<String, MigrationReceipt>,
    /// Shadow comparisons keyed by comparison ID.
    pub shadows: BTreeMap<String, ShadowComparison>,
}
