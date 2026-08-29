use std::collections::BTreeSet;

use cymule_core::{ArtifactRef, PlanCandidate, SealedPlan};
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
    #[serde(deserialize_with = "deserialize_required_nullable")]
    pub before: Option<String>,
    /// Optional replacement semantic digest.
    #[serde(deserialize_with = "deserialize_required_nullable")]
    pub after: Option<String>,
}

pub(crate) fn deserialize_required_nullable<'de, D, T>(
    deserializer: D,
) -> Result<Option<T>, D::Error>
where
    D: serde::Deserializer<'de>,
    T: Deserialize<'de>,
{
    Option::<T>::deserialize(deserializer)
}

/// Immutable structural Plan DAG edge.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PlanEdge {
    /// Structural transition identity.
    pub edge_id: String,
    /// Parent Plan.
    pub from_plan: String,
    /// Child Plan.
    pub to_plan: String,
    /// Declared semantic operations.
    pub operations: Vec<PatchOperation>,
}

/// Reviewed source-to-target patch candidate before child Plan admission.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PlanPatch {
    /// Immutable parent Plan identity.
    pub from_plan: String,
    /// Complete target candidate produced by a compiler or review tool.
    pub target: PlanCandidate,
    /// Exact deterministic operations expected from parent to target.
    pub operations: Vec<PatchOperation>,
    /// Review/compiler evidence artifact.
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
#[serde(tag = "mode", rename_all = "snake_case", deny_unknown_fields)]
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

/// Immutable rollout and execution lineage for one admitted occurrence.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OccurrencePin {
    /// Stable runtime occurrence identity.
    pub occurrence_id: String,
    /// Registered parent template that owned selection.
    pub template_id: String,
    /// Retained rollout decision used for selection.
    pub decision_id: String,
    /// Exact semantic Plan selected for this occurrence.
    pub plan_id: String,
    /// Exact `cymule.execution-binding/2` Artifact admitted for execution.
    pub execution_binding: ArtifactRef,
    /// Stable identity of the future-work selection that created this pin.
    pub selection_id: String,
}

/// Safe-point state migration receipt.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MigrationReceipt {
    /// Complete semantic intent and sole caller-authored replay authority.
    pub request: super::adapters::MigrationRequest,
    /// Digest of the exact Durable-derived source witness consumed by the CAS.
    pub source_witness_id: String,
    /// Exact source execution binding derived from the pinned Run.
    pub source_binding: ArtifactRef,
    /// Exact target execution binding derived and admitted by Durable.
    pub target_binding: ArtifactRef,
    /// Source execution fence preserved by the migrated Continuation.
    pub source_execution_fence: u64,
    /// Next execution epoch published by the atomic migration CAS. The
    /// migration itself does not acquire a driver claim or create an Attempt.
    pub target_epoch: u64,
    /// Pinned migration adapter identity.
    pub adapter_id: String,
    /// Pinned migration adapter revision.
    pub adapter_revision: String,
    /// Declared source state-schema digest.
    pub from_schema: String,
    /// Declared target state-schema digest.
    pub to_schema: String,
    /// Migrated state artifact.
    pub output_state: ArtifactRef,
    /// Complete mapped target Continuation published ready for a later claim.
    pub target_continuation: cymule_durable_protocol::Continuation,
    /// Schema/migration evidence.
    pub evidence: ArtifactRef,
}

/// Request to abandon one quiescent Run lineage and start a replacement under
/// an exact new Plan without interpreting old state under new semantics.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RestartRequest {
    /// Stable restart/idempotency identity.
    pub restart_id: String,
    /// Caller-assigned replacement Run identity.
    pub replacement_run: String,
    /// Exact source Run expected at the Durable boundary.
    pub run_id: String,
    /// Exact source Plan expected at the Durable boundary.
    pub from_plan: String,
    /// Exact source epoch expected at the Durable boundary.
    pub expected_source_epoch: u64,
    /// Exact target Plan selected for the replacement.
    pub to_plan: String,
    /// Explicit replacement input; old state is not reinterpreted implicitly.
    pub input: ArtifactRef,
    /// Policy or operator evidence authorizing the restart.
    pub evidence: ArtifactRef,
}

/// Immutable authorization receipt for `restart_under_new_plan`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RestartReceipt {
    /// Exact admitted request.
    pub request: RestartRequest,
    /// Digest of the exact Durable-derived source witness consumed by the CAS.
    pub source_witness_id: String,
    /// Exact target Plan returned to the runtime for new-Run initialization.
    pub target_plan: SealedPlan,
}

/// Evidence comparing shadow and authoritative results.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ShadowComparison {
    /// Stable comparison identity.
    pub comparison_id: String,
    /// Run or occurrence identity.
    pub subject: String,
    /// Rollout decision that requested this comparison.
    pub decision_id: String,
    /// Authoritative Plan.
    pub primary_plan: String,
    /// Shadow Plan.
    pub shadow_plan: String,
    /// Pinned shadow driver identity.
    pub driver_id: String,
    /// Pinned shadow driver revision.
    pub driver_revision: String,
    /// Declared comparison-policy identity.
    pub comparison_policy: String,
    /// Authoritative result digest.
    pub primary_digest: String,
    /// Shadow result digest.
    pub shadow_digest: String,
    /// Whether results are equivalent under declared comparison semantics.
    pub equivalent: bool,
    /// Immutable execution/comparison evidence.
    pub evidence: ArtifactRef,
}

/// Observed terminal outcome for one rollout occurrence.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ObservationOutcome {
    /// The selected Plan completed within the rollout's success contract.
    Succeeded,
    /// The selected Plan failed its rollout success contract.
    Failed,
}

/// Immutable rollout observation used by deterministic gates.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RolloutObservation {
    /// Stable observation identity.
    pub observation_id: String,
    /// Decision under which the occurrence was admitted.
    pub decision_id: String,
    /// Immutable occurrence identity.
    pub occurrence_id: String,
    /// Plan pinned for the occurrence.
    pub plan_id: String,
    /// Closed success/failure outcome.
    pub outcome: ObservationOutcome,
    /// Immutable observation evidence.
    pub evidence: ArtifactRef,
}

/// Deterministic admission thresholds for promotion or rollback.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RolloutGate {
    /// Stable policy identity.
    pub gate_id: String,
    /// Rollout decision evaluated by this policy.
    pub decision_id: String,
    /// Minimum terminal target observations before promotion.
    pub min_target_observations: u64,
    /// Maximum target failures tolerated before rollback.
    pub max_target_failures: u64,
    /// Minimum equivalent shadow comparisons before promotion.
    pub min_equivalent_shadows: u64,
    /// Maximum inequivalent shadow comparisons tolerated before rollback.
    pub max_inequivalent_shadows: u64,
}

/// Closed deterministic result of evaluating one rollout gate.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GateOutcome {
    /// More evidence is required and no transition is legal.
    Pending,
    /// Evidence admits target activation.
    Promote,
    /// Evidence requires fallback selection for future work.
    Rollback,
}

/// Reproducible gate evaluation with exact evidence counts.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RolloutEvaluation {
    /// Content-addressed evaluation identity.
    pub evaluation_id: String,
    /// Gate policy.
    pub gate: RolloutGate,
    /// Count of terminal target observations.
    pub target_observations: u64,
    /// Count of failed target observations.
    pub target_failures: u64,
    /// Count of equivalent shadow comparisons.
    pub equivalent_shadows: u64,
    /// Count of inequivalent shadow comparisons.
    pub inequivalent_shadows: u64,
    /// Closed evaluation result.
    pub outcome: GateOutcome,
    /// Exact number of observation and comparison records committed into the
    /// evidence accumulator.
    pub evidence_count: u64,
    /// Append-authenticated evidence accumulator root at evaluation time.
    pub evidence_root: String,
}

/// Auditable promotion or rollback transition for future selection.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RolloutTransition {
    /// Content-addressed transition identity.
    pub transition_id: String,
    /// Decision evaluated by the gate.
    pub from_decision: String,
    /// Newly admitted future-selection decision.
    pub to_decision: String,
    /// Exact deterministic gate evaluation.
    pub evaluation: RolloutEvaluation,
}
