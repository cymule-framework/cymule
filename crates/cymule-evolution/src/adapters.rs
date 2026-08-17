use cymule_core::ArtifactRef;
use serde::{Deserialize, Serialize};

use crate::{EvolutionResult, ShadowComparison};

/// State domain covered by a migration implementation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MigrationStateCoverage {
    /// Every state reachable under the exact source Plan.
    TotalReachableState,
}

/// Required preservation of one semantic axis during migration.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MigrationPreservation {
    /// The source meaning is preserved in the target state.
    Preserved,
}

/// Required authority/effect capability relation across migration.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MigrationCapabilityChange {
    /// The target state grants no wider authority or effect capability.
    NoWidening,
}

/// Pinned, provider-neutral contract for one state-migration implementation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MigrationAdapterDescriptor {
    /// Stable adapter identity.
    pub adapter_id: String,
    /// Immutable implementation revision.
    pub adapter_revision: String,
    /// Exact source Plan accepted by this adapter.
    pub from_plan: String,
    /// Exact target Plan produced by this adapter.
    pub to_plan: String,
    /// Source state-schema digest.
    pub from_schema: String,
    /// Target state-schema digest.
    pub to_schema: String,
    /// State domain for which the transformation is total.
    pub state_coverage: MigrationStateCoverage,
    /// Failure and cancellation preservation claim.
    pub failure_and_cancellation: MigrationPreservation,
    /// Budget and ownership preservation claim.
    pub budget_and_ownership: MigrationPreservation,
    /// Authority and effect capability relation.
    pub authority_and_effects: MigrationCapabilityChange,
}

/// One checked state-migration request.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MigrationRequest {
    /// Stable migration/idempotency identity.
    pub migration_id: String,
    /// Run being migrated at a safe point.
    pub run_id: String,
    /// Exact source Plan.
    pub from_plan: String,
    /// Exact target Plan.
    pub to_plan: String,
    /// Immutable source-state artifact.
    pub input_state: ArtifactRef,
}

/// Immutable products returned by a migration adapter.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MigrationOutput {
    /// Migrated state artifact.
    pub output_state: ArtifactRef,
    /// Verification or transformation evidence.
    pub evidence: ArtifactRef,
}

/// Provider plugin interface for state transformation.
pub trait MigrationAdapter {
    /// Advertise the immutable compatibility and safety contract.
    fn describe(&mut self) -> EvolutionResult<MigrationAdapterDescriptor>;

    /// Transform one immutable source-state artifact.
    fn migrate(&mut self, request: &MigrationRequest) -> EvolutionResult<MigrationOutput>;
}

/// Pinned contract for shadow execution and comparison.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ShadowDriverDescriptor {
    /// Stable driver identity.
    pub driver_id: String,
    /// Immutable implementation revision.
    pub driver_revision: String,
    /// Required target effect treatment.
    pub target_effects: ShadowEffectMode,
    /// Required implementation-binding treatment.
    pub occurrence_bindings: ShadowBindingMode,
}

/// Treatment of target-side mutating effects during shadow execution.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ShadowEffectMode {
    /// Mutations are suppressed or simulated and cannot reach authority.
    SuppressedOrSimulated,
}

/// Binding behavior required for repeatable shadow evidence.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ShadowBindingMode {
    /// Primary and shadow occurrences both pin immutable implementations.
    Pinned,
}

/// Provider-neutral request to execute and compare a shadow pair.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ShadowRequest {
    /// Stable comparison/idempotency identity.
    pub comparison_id: String,
    /// Rollout decision requesting evidence.
    pub decision_id: String,
    /// Run or occurrence identity.
    pub subject: String,
    /// Exact authoritative Plan.
    pub primary_plan: String,
    /// Exact non-authoritative Plan.
    pub shadow_plan: String,
    /// Immutable input or state artifact.
    pub input: ArtifactRef,
    /// Versioned comparison-policy identity.
    pub comparison_policy: String,
}

/// Driver result before the controller admits it as shadow evidence.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ShadowOutput {
    /// Authoritative result digest.
    pub primary_digest: String,
    /// Shadow result digest.
    pub shadow_digest: String,
    /// Policy-specific equivalence result.
    pub equivalent: bool,
    /// Immutable execution and comparison evidence.
    pub evidence: ArtifactRef,
}

/// Provider plugin interface for isolated shadow execution.
pub trait ShadowDriver {
    /// Advertise immutable execution-safety properties.
    fn describe(&mut self) -> EvolutionResult<ShadowDriverDescriptor>;

    /// Execute and compare one pair without making shadow output authoritative.
    fn execute(&mut self, request: &ShadowRequest) -> EvolutionResult<ShadowOutput>;
}

impl ShadowComparison {
    /// Confirm the comparison belongs to an exact rollout pair.
    pub fn matches_pair(&self, decision_id: &str, primary: &str, shadow: &str) -> bool {
        self.decision_id == decision_id
            && self.primary_plan == primary
            && self.shadow_plan == shadow
    }
}
