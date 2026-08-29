use std::collections::BTreeSet;

use cymule_core::{ArtifactRecord, ArtifactRef, canonical_bytes, canonical_digest, content_id};
use cymule_durable_protocol::Continuation;
use serde::{Deserialize, Serialize};

use super::{EvolutionResult, ShadowComparison};

/// Sole content-identity domain for an exact-head Run quiescence witness.
pub const RUN_QUIESCENCE_ID_DOMAIN: &str = "cymule.run-quiescence/1";

/// Frozen proof domain for an M4 migration-safe Continuation cut.
pub const MIGRATION_SAFE_POINT_VERSION: &str = "cymule.migration-safe-point/2";

/// Verified source Continuation cut at which state migration may execute.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MigrationSafePoint {
    /// Proof schema and semantic version.
    pub safe_point_version: String,
    /// Content-addressed proof identity.
    pub safe_point_id: String,
    /// Exact store-head revision whose complete domain state was inspected.
    pub domain_revision: String,
    /// Exact durable Run.
    pub run_id: String,
    /// Exact source Plan.
    pub plan_id: String,
    /// Exact source executable binding Artifact identity.
    pub binding_context: String,
    /// Source execution epoch.
    pub epoch: u64,
    /// Optional state Artifact at the cut.
    pub state: Option<ArtifactRef>,
    /// Digest of the complete persisted Continuation.
    pub continuation_digest: String,
}

impl MigrationSafePoint {
    /// Derive the one normalized safe-point value from a Continuation observed
    /// at an exact `StateRoot` revision. Durable remains responsible for proving
    /// whole-Run quiescence before it calls this constructor.
    ///
    /// # Errors
    ///
    /// Returns an error when the physical revision or Continuation is malformed,
    /// outside shared exact-number bounds, or cannot derive canonical evidence.
    pub fn new(
        domain_revision: impl Into<String>,
        continuation: &Continuation,
    ) -> EvolutionResult<Self> {
        let domain_revision = domain_revision.into();
        cymule_core::validate_content_id("migration StateRoot revision", &domain_revision)
            .map_err(|error| super::EvolutionError::Validation(error.to_string()))?;
        continuation
            .verify_wire()
            .map_err(|error| super::EvolutionError::Validation(error.to_string()))?;
        verify_continuation_safe_integers(continuation)?;
        let continuation_digest = canonical_digest(continuation)?;
        let mut safe_point = Self {
            safe_point_version: MIGRATION_SAFE_POINT_VERSION.to_owned(),
            safe_point_id: String::new(),
            domain_revision,
            run_id: continuation.run_id.clone(),
            plan_id: continuation.plan_id.clone(),
            binding_context: continuation.binding_context.clone(),
            epoch: continuation.epoch,
            state: continuation.state.clone(),
            continuation_digest,
        };
        safe_point.safe_point_id = safe_point.derived_id()?;
        safe_point.verify_source_continuation(continuation)?;
        Ok(safe_point)
    }

    /// Verify only that the supplied adapter input is the Continuation covered
    /// by this proof. Quiescence itself is owned and rechecked by the durable
    /// coordinator, never by these duplicated fields.
    ///
    /// # Errors
    ///
    /// Returns an error when the proof or Continuation is malformed, or when
    /// any field or digest differs from the pinned source.
    pub fn verify_source_continuation(&self, continuation: &Continuation) -> EvolutionResult<()> {
        self.verify()?;
        if continuation.run_id != self.run_id
            || continuation.plan_id != self.plan_id
            || continuation.binding_context != self.binding_context
            || continuation.epoch != self.epoch
            || continuation.state != self.state
            || canonical_digest(continuation)? != self.continuation_digest
        {
            return Err(super::EvolutionError::Conflict(
                "migration safe-point proof does not cover the supplied Continuation".to_owned(),
            ));
        }
        Ok(())
    }

    /// Verify the proof envelope independently of durable lookup.
    ///
    /// # Errors
    ///
    /// Returns an error when the proof generation, identities, exact integer,
    /// optional state reference, or derived identity is invalid.
    pub fn verify(&self) -> EvolutionResult<()> {
        validate_run_identity(&self.run_id)?;
        if self.safe_point_version != MIGRATION_SAFE_POINT_VERSION
            || !is_content_id(&self.safe_point_id)
            || !is_content_id(&self.domain_revision)
            || !is_content_id(&self.plan_id)
            || !is_content_id(&self.binding_context)
            || !is_digest(&self.continuation_digest)
            || self.epoch > cymule_core::MAX_EXACT_INTEGER
        {
            return Err(super::EvolutionError::Validation(
                "migration safe-point proof is malformed".to_owned(),
            ));
        }
        if let Some(state) = &self.state {
            state.validate().map_err(|error| {
                super::EvolutionError::Validation(format!(
                    "migration safe-point state is malformed: {error}"
                ))
            })?;
        }
        let expected_id = self.derived_id()?;
        if self.safe_point_id != expected_id {
            return Err(super::EvolutionError::Validation(
                "migration safe-point identity does not match its content".to_owned(),
            ));
        }
        Ok(())
    }

    fn derived_id(&self) -> EvolutionResult<String> {
        content_id(
            RUN_QUIESCENCE_ID_DOMAIN,
            &(
                self.domain_revision.as_str(),
                self.run_id.as_str(),
                self.plan_id.as_str(),
                self.binding_context.as_str(),
                self.epoch,
                &self.state,
                self.continuation_digest.as_str(),
            ),
        )
        .map_err(Into::into)
    }
}

pub(crate) fn validate_run_identity(value: &str) -> EvolutionResult<()> {
    let scalar_count = value.chars().count();
    if !(1..=512).contains(&scalar_count) || value.chars().any(char::is_control) {
        return Err(super::EvolutionError::Validation(
            "Run identity must contain 1..=512 non-control Unicode scalars".to_owned(),
        ));
    }
    Ok(())
}

pub(crate) fn is_content_id(value: &str) -> bool {
    value
        .strip_prefix("sha256:")
        .is_some_and(is_lower_hex_digest)
}

fn is_digest(value: &str) -> bool {
    is_lower_hex_digest(value)
}

fn is_lower_hex_digest(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
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
    /// Exact reviewed Plan edge admitted by this implementation.
    pub plan_edge_id: String,
    /// Deterministic source-to-target compatibility report identity.
    pub compatibility_id: String,
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

/// Semantic state-migration intent accepted at the public M4 command boundary.
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
    /// Exact reviewed Plan edge authorizing this source-to-target transition.
    pub plan_edge_id: String,
    /// Exact deterministic compatibility report accepted for this transition.
    pub compatibility_id: String,
    /// Exact source epoch the caller expects Durable to observe.
    pub expected_source_epoch: u64,
    /// Semantic migration-adapter identity selected for this transition.
    pub adapter_id: String,
    /// Immutable semantic adapter revision selected for this transition.
    pub adapter_revision: String,
}

impl MigrationRequest {
    /// Verify the complete public migration intent.
    ///
    /// # Errors
    ///
    /// Returns an error when an identity, content reference, or optimistic
    /// source epoch is outside the closed command domain.
    pub fn verify(&self) -> EvolutionResult<()> {
        super::control::validate_identity("migration", &self.migration_id)?;
        validate_run_identity(&self.run_id)?;
        for (kind, identity) in [
            ("migration source Plan", self.from_plan.as_str()),
            ("migration target Plan", self.to_plan.as_str()),
            ("migration Plan edge", self.plan_edge_id.as_str()),
            ("migration compatibility", self.compatibility_id.as_str()),
            ("migration adapter revision", self.adapter_revision.as_str()),
        ] {
            cymule_core::validate_content_id(kind, identity)
                .map_err(|error| super::EvolutionError::Validation(error.to_string()))?;
        }
        if self.from_plan == self.to_plan {
            return Err(super::EvolutionError::Validation(
                "migration requires distinct source and target Plans".to_owned(),
            ));
        }
        super::control::validate_identity("migration adapter", &self.adapter_id)?;
        self.verify_safe_integers()
    }

    pub(crate) fn verify_safe_integers(&self) -> EvolutionResult<()> {
        if self.expected_source_epoch > cymule_core::MAX_EXACT_INTEGER {
            return Err(super::EvolutionError::Validation(
                "migration source epoch exceeds the JSON safe-integer range".to_owned(),
            ));
        }
        Ok(())
    }
}

/// Provider request assembled only after Durable verifies the exact source Run,
/// quiescence, Continuation, and source binding at one pinned root, then admits
/// the registry-resolved target binding against the prepared target Plan.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MigrationAdapterRequest {
    /// Public semantic intent.
    pub intent: MigrationRequest,
    /// Content-derived exact source witness identity.
    pub source_witness_id: String,
    /// Complete exact source Continuation.
    pub source_continuation: Continuation,
    /// Immutable source-state Artifact derived from the source Continuation.
    pub input_state: ArtifactRef,
    /// Exact source `ExecutionBinding` Artifact retained by Durable.
    pub source_binding: ArtifactRef,
    /// Exact target `ExecutionBinding` Artifact derived and admitted by Durable.
    pub target_binding: ArtifactRef,
}

impl MigrationAdapterRequest {
    /// Verify one complete Durable-derived provider request independently of
    /// the non-serializable reducer token that carried it.
    ///
    /// # Errors
    ///
    /// Returns an error when the semantic intent, exact source Continuation,
    /// source witness, state, or execution bindings do not form one quiescent
    /// source authority.
    pub fn verify(&self) -> EvolutionResult<()> {
        self.intent.verify()?;
        cymule_core::validate_content_id("migration source witness", &self.source_witness_id)
            .map_err(|error| super::EvolutionError::Validation(error.to_string()))?;
        self.source_continuation
            .verify_wire()
            .map_err(|error| super::EvolutionError::Validation(error.to_string()))?;
        verify_continuation_safe_integers(&self.source_continuation)?;
        self.input_state
            .validate()
            .map_err(|error| super::EvolutionError::Validation(error.to_string()))?;
        for binding in [&self.source_binding, &self.target_binding] {
            binding
                .validate()
                .map_err(|error| super::EvolutionError::Validation(error.to_string()))?;
            if binding.kind != cymule_runtime::EXECUTION_BINDING_VERSION {
                return Err(super::EvolutionError::Validation(
                    "migration provider request requires exact ExecutionBinding Artifacts"
                        .to_owned(),
                ));
            }
        }
        if self.source_continuation.run_id != self.intent.run_id
            || self.source_continuation.plan_id != self.intent.from_plan
            || self.source_continuation.epoch != self.intent.expected_source_epoch
            || self.source_continuation.binding_context != self.source_binding.artifact_id
            || self.source_continuation.state.as_ref() != Some(&self.input_state)
            || self.source_continuation.status != cymule_durable_protocol::ContinuationStatus::Ready
            || self.source_continuation.execution_claim.is_some()
            || self.source_continuation.frames.is_empty()
            || !self.source_continuation.wait_set.is_empty()
            || self.source_continuation.scope_stack != [cymule_core::ROOT_SCOPE_ID]
        {
            return Err(super::EvolutionError::Conflict(
                "migration provider request does not bind one exact quiescent source".to_owned(),
            ));
        }
        Ok(())
    }
}

pub(crate) fn verify_continuation_safe_integers(
    continuation: &Continuation,
) -> EvolutionResult<()> {
    let invalid_index = |index: usize| {
        u64::try_from(index).map_or(true, |value| value > cymule_core::MAX_EXACT_INTEGER)
    };
    if continuation.epoch > cymule_core::MAX_EXACT_INTEGER
        || continuation.frames.iter().any(|frame| {
            invalid_index(frame.next_step)
                || frame.region_path.iter().copied().any(invalid_index)
                || frame
                    .invocation_path
                    .iter()
                    .any(|segment| segment.region_path.iter().copied().any(invalid_index))
        })
    {
        return Err(super::EvolutionError::Validation(
            "migration Continuation exceeds the JSON safe-integer range".to_owned(),
        ));
    }
    Ok(())
}

/// Immutable products returned by a migration adapter.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MigrationOutput {
    /// Complete target interpreter state, including every mapped frame and
    /// exact program counter under the target Plan.
    pub continuation: Continuation,
    /// Exact `ArtifactRef` closure introduced by the mapped Continuation relative
    /// to the authenticated source Continuation. Retained source records are
    /// reused from the canonical Machine and must not be repeated here.
    pub artifacts: Vec<ArtifactRecord>,
    /// Separately owned verification or transformation evidence bytes. This
    /// record must not alias Continuation or execution-binding data.
    pub evidence: ArtifactRecord,
}

impl MigrationOutput {
    /// Stable plugin-defect code for a malformed target Continuation or
    /// descriptor returned after the migration provider was invoked.
    pub const INVALID_OUTPUT_DEFECT_CODE: &str = "invalid_migration_output";
    /// Stable plugin-defect code for any record-bound, identity-partition, or
    /// closure violation in a migration Artifact product.
    pub const INVALID_ARTIFACT_PRODUCT_DEFECT_CODE: &str = "invalid_migration_artifact_product";
    /// Hard maximum number of Artifact records one migration plugin result may
    /// return, including the separately owned evidence record.
    pub const MAX_ARTIFACT_RECORDS: usize = 1_024;
    /// Hard maximum JCS bytes of the complete Artifact-record product, including
    /// the separately owned evidence record.
    pub const MAX_ARTIFACT_CANONICAL_BYTES: usize = 4 * 1024 * 1024;

    /// Verify the complete bounded Artifact product independently of a
    /// migration request.
    ///
    /// # Errors
    ///
    /// Returns a plugin defect when the record count, raw payload/reference
    /// bytes, or canonical Artifact-product bytes exceed the provider-result
    /// contract.
    pub fn verify_artifact_limits(&self) -> EvolutionResult<()> {
        let record_count = self.artifacts.len().checked_add(1).ok_or_else(|| {
            invalid_migration_artifact_product("migration Artifact record count overflowed")
        })?;
        if record_count > Self::MAX_ARTIFACT_RECORDS {
            return Err(invalid_migration_artifact_product(format!(
                "migration plugin returned {record_count} Artifact records, above the {} record limit",
                Self::MAX_ARTIFACT_RECORDS
            )));
        }
        let payload_bytes = self
            .artifacts
            .iter()
            .chain(std::iter::once(&self.evidence))
            .try_fold(0_usize, |total, record| {
                total
                    .checked_add(record.reference.identity_version.len())
                    .and_then(|total| total.checked_add(record.reference.artifact_id.len()))
                    .and_then(|total| total.checked_add(record.reference.kind.len()))
                    .and_then(|total| total.checked_add(record.bytes.len()))
                    .ok_or_else(|| {
                        invalid_migration_artifact_product(
                            "migration Artifact product byte count overflowed",
                        )
                    })
            })?;
        if payload_bytes > Self::MAX_ARTIFACT_CANONICAL_BYTES {
            return Err(invalid_migration_artifact_product(format!(
                "migration plugin returned at least {payload_bytes} raw Artifact payload/reference bytes, above the {} byte limit",
                Self::MAX_ARTIFACT_CANONICAL_BYTES
            )));
        }
        let encoded = canonical_bytes(&MigrationArtifactProducts {
            artifacts: &self.artifacts,
            evidence: &self.evidence,
        })
        .map_err(|error| {
            invalid_migration_artifact_product(format!(
                "migration Artifact product cannot be canonically encoded: {error}"
            ))
        })?;
        if encoded.len() > Self::MAX_ARTIFACT_CANONICAL_BYTES {
            return Err(invalid_migration_artifact_product(format!(
                "migration plugin returned {} canonical Artifact bytes, above the {} byte limit",
                encoded.len(),
                Self::MAX_ARTIFACT_CANONICAL_BYTES
            )));
        }
        Ok(())
    }
}

#[derive(Serialize)]
struct MigrationArtifactProducts<'a> {
    artifacts: &'a [ArtifactRecord],
    evidence: &'a ArtifactRecord,
}

pub(crate) struct MigrationArtifactClosure {
    pub(crate) introduced: BTreeSet<ArtifactRef>,
    pub(crate) retained: BTreeSet<ArtifactRef>,
    pub(crate) plugin_records: BTreeSet<ArtifactRef>,
}

/// Derive the one exact Artifact partition for a migration product. Artifact
/// references retained from the source Continuation are canonical Machine
/// inputs; only target references introduced by the mapped Continuation belong
/// in `MigrationOutput::artifacts`. Evidence is a separate authority and may
/// not alias continuation state, frame data, or either execution binding.
pub(crate) fn migration_artifact_closure(
    request: &MigrationAdapterRequest,
    target: &Continuation,
    evidence: &ArtifactRef,
) -> EvolutionResult<MigrationArtifactClosure> {
    let source = continuation_artifact_closure(&request.source_continuation)?;
    let target = continuation_artifact_closure(target).map_err(|error| {
        invalid_migration_artifact_product(format!(
            "migration target Continuation has an invalid Artifact reference: {error}"
        ))
    })?;
    evidence.validate().map_err(|error| {
        invalid_migration_artifact_product(format!(
            "migration evidence has an invalid Artifact reference: {error}"
        ))
    })?;
    if source.contains(evidence)
        || target.contains(evidence)
        || evidence == &request.source_binding
        || evidence == &request.target_binding
    {
        return Err(invalid_migration_artifact_product(
            "migration evidence must be a distinct Artifact authority and cannot shadow continuation or binding data",
        ));
    }
    let introduced = target.difference(&source).cloned().collect::<BTreeSet<_>>();
    let retained = target
        .intersection(&source)
        .cloned()
        .collect::<BTreeSet<_>>();
    let mut plugin_records = introduced.clone();
    if !plugin_records.insert(evidence.clone()) {
        return Err(invalid_migration_artifact_product(
            "migration evidence duplicates an introduced Continuation Artifact",
        ));
    }
    Ok(MigrationArtifactClosure {
        introduced,
        retained,
        plugin_records,
    })
}

pub(crate) fn invalid_migration_artifact_product(
    message: impl Into<String>,
) -> super::EvolutionError {
    super::EvolutionError::PluginDefect {
        code: MigrationOutput::INVALID_ARTIFACT_PRODUCT_DEFECT_CODE.to_owned(),
        message: message.into(),
    }
}

pub(crate) fn invalid_migration_output(message: impl Into<String>) -> super::EvolutionError {
    super::EvolutionError::PluginDefect {
        code: MigrationOutput::INVALID_OUTPUT_DEFECT_CODE.to_owned(),
        message: message.into(),
    }
}

fn continuation_artifact_closure(
    continuation: &Continuation,
) -> EvolutionResult<BTreeSet<ArtifactRef>> {
    let mut references = BTreeSet::new();
    if let Some(state) = &continuation.state {
        retain_continuation_artifact(&mut references, state)?;
    }
    if let Some(claim) = &continuation.execution_claim {
        retain_continuation_artifact(&mut references, &claim.execution_binding_ref)?;
    }
    for frame in &continuation.frames {
        retain_continuation_artifact(&mut references, &frame.input)?;
        for local in frame.locals.values() {
            retain_continuation_artifact(&mut references, local)?;
        }
    }
    Ok(references)
}

fn retain_continuation_artifact(
    references: &mut BTreeSet<ArtifactRef>,
    reference: &ArtifactRef,
) -> EvolutionResult<()> {
    reference.validate()?;
    references.insert(reference.clone());
    Ok(())
}

/// Provider plugin interface for state transformation.
pub trait MigrationAdapter {
    /// Advertise the immutable compatibility and safety contract.
    ///
    /// # Errors
    ///
    /// Returns an error when the provider cannot produce its descriptor.
    fn describe(&mut self) -> EvolutionResult<MigrationAdapterDescriptor>;

    /// Transform one immutable source continuation into a complete target
    /// continuation. The output, not the source frame stack, is resumed.
    ///
    /// # Errors
    ///
    /// Returns an error when the provider cannot complete the exact request.
    fn migrate(&mut self, request: &MigrationAdapterRequest) -> EvolutionResult<MigrationOutput>;
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
    /// Semantic identity of the selected admitted shadow driver.
    pub driver_id: String,
    /// Immutable content revision of the selected admitted shadow driver.
    pub driver_revision: String,
    /// Versioned comparison-policy identity.
    pub comparison_policy: String,
}

impl ShadowRequest {
    /// Verify the complete public shadow intent.
    ///
    /// # Errors
    ///
    /// Returns an error when an identity, exact Plan, input Artifact, driver
    /// revision, or comparison policy is malformed.
    pub fn verify(&self) -> EvolutionResult<()> {
        for (kind, identity) in [
            ("shadow comparison", self.comparison_id.as_str()),
            ("rollout decision", self.decision_id.as_str()),
            ("shadow subject", self.subject.as_str()),
            ("shadow driver", self.driver_id.as_str()),
            ("comparison policy", self.comparison_policy.as_str()),
        ] {
            super::control::validate_identity(kind, identity)?;
        }
        for (kind, plan) in [
            ("shadow primary Plan", self.primary_plan.as_str()),
            ("shadow target Plan", self.shadow_plan.as_str()),
            ("shadow driver revision", self.driver_revision.as_str()),
        ] {
            cymule_core::validate_content_id(kind, plan)
                .map_err(|error| super::EvolutionError::Validation(error.to_string()))?;
        }
        if self.primary_plan == self.shadow_plan {
            return Err(super::EvolutionError::Validation(
                "shadow request requires distinct primary and shadow Plans".to_owned(),
            ));
        }
        self.input
            .validate()
            .map_err(|error| super::EvolutionError::Validation(error.to_string()))
    }
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

impl ShadowOutput {
    /// Stable plugin-defect code for a malformed descriptor, digest, or
    /// evidence product returned by a shadow provider.
    pub const INVALID_OUTPUT_DEFECT_CODE: &str = "invalid_shadow_output";
    /// Hard maximum raw payload/reference bytes for the evidence Artifact.
    pub const MAX_EVIDENCE_RAW_BYTES: usize = 4 * 1024 * 1024;
    /// Hard maximum canonical bytes for the complete evidence Artifact.
    pub const MAX_EVIDENCE_CANONICAL_BYTES: usize = 4 * 1024 * 1024;

    /// Verify the self-contained evidence Artifact resource bounds.
    ///
    /// # Errors
    ///
    /// Returns a plugin defect when raw payload/reference accounting
    /// overflows, exceeds the fixed bound, or the complete canonical record is
    /// larger than the fixed process-independent provider-result contract.
    pub fn verify_evidence_limits(&self) -> EvolutionResult<()> {
        let raw_bytes = self
            .evidence
            .reference
            .identity_version
            .len()
            .checked_add(self.evidence.reference.artifact_id.len())
            .and_then(|total| total.checked_add(self.evidence.reference.kind.len()))
            .and_then(|total| total.checked_add(self.evidence.bytes.len()))
            .ok_or_else(|| {
                invalid_shadow_output("shadow evidence raw byte accounting overflowed")
            })?;
        if raw_bytes > Self::MAX_EVIDENCE_RAW_BYTES {
            return Err(invalid_shadow_output(format!(
                "shadow evidence uses {raw_bytes} raw payload/reference bytes, above the {} byte limit",
                Self::MAX_EVIDENCE_RAW_BYTES
            )));
        }
        let canonical_bytes = canonical_bytes(&self.evidence)
            .map_err(|error| {
                invalid_shadow_output(format!(
                    "shadow evidence cannot be canonically encoded: {error}"
                ))
            })?
            .len();
        if canonical_bytes > Self::MAX_EVIDENCE_CANONICAL_BYTES {
            return Err(invalid_shadow_output(format!(
                "shadow evidence uses {canonical_bytes} canonical bytes, above the {} byte limit",
                Self::MAX_EVIDENCE_CANONICAL_BYTES
            )));
        }
        Ok(())
    }
}

pub(crate) fn invalid_shadow_output(message: impl Into<String>) -> super::EvolutionError {
    super::EvolutionError::PluginDefect {
        code: ShadowOutput::INVALID_OUTPUT_DEFECT_CODE.to_owned(),
        message: message.into(),
    }
}

/// Provider plugin interface for isolated shadow execution.
pub trait ShadowDriver {
    /// Advertise immutable execution-safety properties.
    ///
    /// # Errors
    ///
    /// Returns an error when the provider cannot produce its descriptor.
    fn describe(&mut self) -> EvolutionResult<ShadowDriverDescriptor>;

    /// Execute and compare one pair without making shadow output authoritative.
    ///
    /// # Errors
    ///
    /// Returns an error when the provider cannot complete the exact request.
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
