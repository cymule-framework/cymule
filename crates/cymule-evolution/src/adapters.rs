use cymule_core::{ArtifactRecord, ArtifactRef, ROOT_SCOPE_ID, canonical_digest, content_id};
use cymule_durable::{Continuation, ContinuationStatus};
use serde::{Deserialize, Serialize};

use crate::{EvolutionResult, ShadowComparison};

/// Frozen proof domain for an M4 migration-safe Continuation cut.
pub const MIGRATION_SAFE_POINT_VERSION: &str = "cymule.migration-safe-point/1";

/// Verified source Continuation cut at which state migration may execute.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MigrationSafePoint {
    /// Proof schema and semantic version.
    pub safe_point_version: String,
    /// Content-addressed proof identity.
    pub safe_point_id: String,
    /// Exact durable Run.
    pub run_id: String,
    /// Exact source Plan.
    pub plan_id: String,
    /// Source Attempt fence.
    pub epoch: u64,
    /// Optional state Artifact at the cut.
    pub state: Option<ArtifactRef>,
    /// Digest of the complete persisted Continuation.
    pub continuation_digest: String,
}

impl MigrationSafePoint {
    /// Derive a proof only from a quiescent, root-scoped Continuation.
    pub fn derive(continuation: &Continuation) -> EvolutionResult<Self> {
        if continuation.status != ContinuationStatus::Ready
            || continuation.frames.is_empty()
            || !continuation.wait_set.is_empty()
            || continuation.scope_stack != [ROOT_SCOPE_ID]
            || !continuation.effect_obligations.is_empty()
            || !continuation.authority_leases.is_empty()
        {
            return Err(crate::EvolutionError::Conflict(
                "migration requires a ready root-scoped Continuation without waits, obligations, or leases"
                    .to_owned(),
            ));
        }
        let continuation_digest = canonical_digest(continuation)?;
        let safe_point_id = content_id(
            MIGRATION_SAFE_POINT_VERSION,
            &(
                continuation.run_id.as_str(),
                continuation.plan_id.as_str(),
                continuation.epoch,
                &continuation.state,
                &continuation_digest,
            ),
        )?;
        Ok(Self {
            safe_point_version: MIGRATION_SAFE_POINT_VERSION.to_owned(),
            safe_point_id,
            run_id: continuation.run_id.clone(),
            plan_id: continuation.plan_id.clone(),
            epoch: continuation.epoch,
            state: continuation.state.clone(),
            continuation_digest,
        })
    }

    /// Re-derive and compare this proof against current durable authority.
    pub fn verify_continuation(&self, continuation: &Continuation) -> EvolutionResult<()> {
        self.verify()?;
        let expected = Self::derive(continuation)?;
        if self != &expected {
            return Err(crate::EvolutionError::Conflict(
                "migration safe-point proof does not match the durable Continuation".to_owned(),
            ));
        }
        Ok(())
    }

    /// Verify the proof envelope independently of durable lookup.
    pub fn verify(&self) -> EvolutionResult<()> {
        if self.safe_point_version != MIGRATION_SAFE_POINT_VERSION
            || self.run_id.is_empty()
            || self.plan_id.is_empty()
            || self.continuation_digest.len() != 64
        {
            return Err(crate::EvolutionError::Validation(
                "migration safe-point proof is malformed".to_owned(),
            ));
        }
        let expected_id = content_id(
            MIGRATION_SAFE_POINT_VERSION,
            &(
                self.run_id.as_str(),
                self.plan_id.as_str(),
                self.epoch,
                &self.state,
                &self.continuation_digest,
            ),
        )?;
        if self.safe_point_id != expected_id {
            return Err(crate::EvolutionError::Validation(
                "migration safe-point identity does not match its content".to_owned(),
            ));
        }
        Ok(())
    }
}

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
    /// Verified source Continuation cut.
    pub safe_point_id: String,
    /// Source Attempt fence at that cut.
    pub source_epoch: u64,
    /// Immutable source-state artifact.
    pub input_state: ArtifactRef,
    /// Exact source `ExecutionBinding` Artifact pinned by the safe Continuation.
    pub source_binding: ArtifactRef,
    /// Exact target `ExecutionBinding` Artifact admitted against the target Plan.
    pub target_binding: ArtifactRef,
}

/// Immutable products returned by a migration adapter.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MigrationOutput {
    /// Migrated state bytes and their verified content reference.
    pub output_state: ArtifactRecord,
    /// Verification or transformation evidence bytes.
    pub evidence: ArtifactRecord,
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
    /// Immutable execution and comparison evidence bytes.
    pub evidence: ArtifactRecord,
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
