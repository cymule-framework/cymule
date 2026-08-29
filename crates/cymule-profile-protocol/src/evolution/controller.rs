use std::collections::{BTreeMap, BTreeSet};

use cymule_core::{ArtifactRecord, SealedPlan, artifact_ref, canonical_digest, content_id};
use cymule_durable_protocol::{Continuation, FrameState};
use serde::Serialize;

use super::{
    EvolutionError, EvolutionResult, MigrationAdapter, MigrationAdapterDescriptor,
    MigrationAdapterRequest, MigrationOutput, MigrationReceipt, PatchOperation, RolloutEvaluation,
    ShadowComparison, ShadowDriver, ShadowDriverDescriptor, ShadowRequest,
    control::validate_identity,
};

pub(crate) const PLAN_EDGE_ID_DOMAIN: &str = "cymule.plan-edge/2";
pub(crate) const ROLLOUT_EVALUATION_ID_DOMAIN: &str = "cymule.rollout-evaluation/1";
pub(crate) const ROLLOUT_TRANSITION_ID_DOMAIN: &str = "cymule.rollout-transition/2";

pub(crate) fn derive_plan_edge_id(
    from_plan: &str,
    to_plan: &str,
    operations: &[PatchOperation],
) -> EvolutionResult<String> {
    content_id(PLAN_EDGE_ID_DOMAIN, &(from_plan, to_plan, operations)).map_err(Into::into)
}

pub(crate) fn derive_rollout_evaluation_id(
    evaluation: &RolloutEvaluation,
) -> EvolutionResult<String> {
    content_id(
        ROLLOUT_EVALUATION_ID_DOMAIN,
        &(
            &evaluation.gate,
            evaluation.target_observations,
            evaluation.target_failures,
            evaluation.equivalent_shadows,
            evaluation.inequivalent_shadows,
            evaluation.outcome,
            evaluation.evidence_count,
            &evaluation.evidence_root,
        ),
    )
    .map_err(Into::into)
}

pub(crate) fn derive_rollout_transition_id(
    from_decision: &str,
    to_decision: &str,
    evaluation: &RolloutEvaluation,
) -> EvolutionResult<String> {
    content_id(
        ROLLOUT_TRANSITION_ID_DOMAIN,
        &(from_decision, to_decision, evaluation),
    )
    .map_err(Into::into)
}

/// Compute a deterministic conservative diff between two sealed Plans.
///
/// # Errors
///
/// Returns an error when either Plan is invalid or canonical identities for
/// changed semantic fields cannot be derived.
pub fn diff_plans(from: &SealedPlan, to: &SealedPlan) -> EvolutionResult<Vec<PatchOperation>> {
    from.verify()?;
    to.verify()?;
    let mut operations = Vec::new();
    if from.candidate.ir_version != to.candidate.ir_version {
        operations.push(PatchOperation {
            kind: "replace".to_owned(),
            target: "ir_version".to_owned(),
            before: Some(canonical_digest(&from.candidate.ir_version)?),
            after: Some(canonical_digest(&to.candidate.ir_version)?),
        });
    }
    if from.candidate.entry != to.candidate.entry {
        operations.push(PatchOperation {
            kind: "replace".to_owned(),
            target: "entry".to_owned(),
            before: Some(canonical_digest(&from.candidate.entry)?),
            after: Some(canonical_digest(&to.candidate.entry)?),
        });
    }
    diff_named(
        "component",
        &from.candidate.components,
        &to.candidate.components,
        |component| &component.id,
        &mut operations,
    )?;
    diff_named(
        "effect",
        &from.candidate.effects,
        &to.candidate.effects,
        |effect| &effect.id,
        &mut operations,
    )?;
    diff_named(
        "definition",
        &from.candidate.definitions,
        &to.candidate.definitions,
        |definition| &definition.id,
        &mut operations,
    )?;
    operations.sort_by(|left, right| {
        left.target
            .cmp(&right.target)
            .then_with(|| left.kind.cmp(&right.kind))
    });
    Ok(operations)
}

fn diff_named<T: Serialize>(
    prefix: &str,
    from: &[T],
    to: &[T],
    identity: impl Fn(&T) -> &String,
    operations: &mut Vec<PatchOperation>,
) -> EvolutionResult<()> {
    let before: BTreeMap<&str, &T> = from
        .iter()
        .map(|value| (identity(value).as_str(), value))
        .collect();
    let after: BTreeMap<&str, &T> = to
        .iter()
        .map(|value| (identity(value).as_str(), value))
        .collect();
    for key in before
        .keys()
        .chain(after.keys())
        .copied()
        .collect::<BTreeSet<_>>()
    {
        let old = before.get(key).copied();
        let new = after.get(key).copied();
        let old_digest = old.map(canonical_digest).transpose()?;
        let new_digest = new.map(canonical_digest).transpose()?;
        if old_digest == new_digest {
            continue;
        }
        let kind = match (old, new) {
            (None, Some(_)) => "add",
            (Some(_), None) => "remove",
            (Some(_), Some(_)) => "replace",
            (None, None) => continue,
        };
        operations.push(PatchOperation {
            kind: kind.to_owned(),
            target: format!("{prefix}:{key}"),
            before: old_digest,
            after: new_digest,
        });
    }
    Ok(())
}

fn verify_artifact_record(record: &ArtifactRecord) -> EvolutionResult<()> {
    record
        .reference
        .validate()
        .map_err(|error| EvolutionError::Validation(error.to_string()))?;
    let derived = artifact_ref(&record.reference.kind, &record.bytes)
        .map_err(|error| EvolutionError::Validation(error.to_string()))?;
    if derived != record.reference {
        return Err(EvolutionError::Validation(format!(
            "Artifact {} does not match its immutable bytes",
            record.reference.artifact_id
        )));
    }
    Ok(())
}

fn validate_migration_descriptor(
    descriptor: &MigrationAdapterDescriptor,
    request: &MigrationAdapterRequest,
) -> EvolutionResult<()> {
    validate_identity("migration adapter", &descriptor.adapter_id)?;
    validate_implementation_revision("migration adapter", &descriptor.adapter_revision)?;
    validate_identity("migration source schema", &descriptor.from_schema)?;
    validate_identity("migration target schema", &descriptor.to_schema)?;
    if descriptor.adapter_id != request.intent.adapter_id
        || descriptor.adapter_revision != request.intent.adapter_revision
        || descriptor.from_plan != request.intent.from_plan
        || descriptor.to_plan != request.intent.to_plan
        || descriptor.plan_edge_id != request.intent.plan_edge_id
        || descriptor.compatibility_id != request.intent.compatibility_id
    {
        return Err(EvolutionError::Conflict(
            "migration adapter descriptor does not match the exact reviewed transition".to_owned(),
        ));
    }
    Ok(())
}

fn validate_shadow_descriptor(
    descriptor: &ShadowDriverDescriptor,
    request: &ShadowRequest,
) -> EvolutionResult<()> {
    validate_identity("shadow driver", &descriptor.driver_id)?;
    validate_implementation_revision("shadow driver", &descriptor.driver_revision)?;
    if descriptor.driver_id != request.driver_id
        || descriptor.driver_revision != request.driver_revision
    {
        return Err(EvolutionError::Conflict(
            "shadow driver descriptor does not match the exact selected driver".to_owned(),
        ));
    }
    Ok(())
}

fn validate_implementation_revision(kind: &str, revision: &str) -> EvolutionResult<()> {
    if super::adapters::is_content_id(revision) {
        Ok(())
    } else {
        Err(EvolutionError::Validation(format!(
            "{kind} revision must be a lowercase SHA-256 content ID"
        )))
    }
}

fn validate_shadow_request(request: &ShadowRequest) -> EvolutionResult<()> {
    validate_identity("shadow comparison", &request.comparison_id)?;
    validate_identity("rollout decision", &request.decision_id)?;
    validate_identity("shadow subject", &request.subject)?;
    validate_identity("shadow driver", &request.driver_id)?;
    validate_implementation_revision("shadow driver", &request.driver_revision)?;
    validate_identity("comparison policy", &request.comparison_policy)?;
    if !super::adapters::is_content_id(&request.primary_plan)
        || !super::adapters::is_content_id(&request.shadow_plan)
        || request.primary_plan == request.shadow_plan
    {
        return Err(EvolutionError::Validation(
            "shadow request requires distinct exact primary and shadow Plans".to_owned(),
        ));
    }
    request
        .input
        .validate()
        .map_err(|error| EvolutionError::Validation(error.to_string()))
}

fn map_invalid_migration_output(error: EvolutionError) -> EvolutionError {
    match error {
        error @ EvolutionError::PluginDefect { .. } => error,
        error => super::adapters::invalid_migration_output(format!(
            "migration plugin returned malformed output: {error}"
        )),
    }
}

fn map_migration_plugin_call_error(error: EvolutionError) -> EvolutionError {
    match error {
        EvolutionError::Validation(_)
        | EvolutionError::NotFound(_)
        | EvolutionError::ReadRequired { .. }
        | EvolutionError::Conflict(_) => super::adapters::invalid_migration_output(format!(
            "migration plugin returned an invalid protocol response: {error}"
        )),
        error => error,
    }
}

fn map_invalid_shadow_output(error: EvolutionError) -> EvolutionError {
    match error {
        error @ EvolutionError::PluginDefect { .. } => error,
        error => super::adapters::invalid_shadow_output(format!(
            "shadow plugin returned malformed output: {error}"
        )),
    }
}

fn map_shadow_plugin_call_error(error: EvolutionError) -> EvolutionError {
    match error {
        EvolutionError::Validation(_)
        | EvolutionError::NotFound(_)
        | EvolutionError::ReadRequired { .. }
        | EvolutionError::Conflict(_) => super::adapters::invalid_shadow_output(format!(
            "shadow plugin returned an invalid protocol response: {error}"
        )),
        error => error,
    }
}

fn verify_migration_artifact_record(record: &ArtifactRecord) -> EvolutionResult<()> {
    verify_artifact_record(record).map_err(|error| {
        super::adapters::invalid_migration_artifact_product(format!(
            "migration plugin returned an invalid Artifact record: {error}"
        ))
    })
}

fn verify_migration_output(
    request: &MigrationAdapterRequest,
    target_epoch: u64,
    output: &MigrationOutput,
) -> EvolutionResult<()> {
    let target = &output.continuation;
    target.verify_wire().map_err(|error| {
        super::adapters::invalid_migration_output(format!(
            "migration adapter returned an invalid target Continuation: {error}"
        ))
    })?;
    if target.run_id != request.intent.run_id
        || target.plan_id != request.intent.to_plan
        || target.binding_context != request.target_binding.artifact_id
        || target.epoch != target_epoch
        || target.status != cymule_durable_protocol::ContinuationStatus::Ready
        || target.execution_claim.is_some()
        || target.execution_fence != request.source_continuation.execution_fence
        || target.frames.is_empty()
        || !target.wait_set.is_empty()
        || target.scope_stack != [cymule_core::ROOT_SCOPE_ID]
        || target.state.is_none()
    {
        return Err(super::adapters::invalid_migration_output(
            "migration adapter did not return a complete fenced target Continuation",
        ));
    }
    let closure =
        super::adapters::migration_artifact_closure(request, target, &output.evidence.reference)?;
    let mut references = BTreeSet::new();
    for artifact in &output.artifacts {
        verify_migration_artifact_record(artifact)?;
        if !references.insert(artifact.reference.clone()) {
            return Err(super::adapters::invalid_migration_artifact_product(
                "migration adapter returned a duplicate Artifact record",
            ));
        }
    }
    if references != closure.introduced {
        let missing = closure.introduced.difference(&references).count();
        let unreferenced = references.difference(&closure.introduced).count();
        return Err(super::adapters::invalid_migration_artifact_product(
            format!(
                "migration adapter Artifact records do not equal the target Continuation's introduced reference closure ({missing} missing, {unreferenced} unreferenced)"
            ),
        ));
    }
    Ok(())
}

pub(crate) fn verify_target_program_counters(
    plan: &SealedPlan,
    continuation: &Continuation,
) -> EvolutionResult<()> {
    for (index, frame) in continuation.frames.iter().enumerate() {
        let expected_invocation = cymule_core::plan_invocation_id(
            &continuation.run_id,
            &plan.plan_id,
            &plan.candidate.entry,
            &frame.invocation_path,
        )?;
        if frame.invocation_id != expected_invocation {
            return Err(EvolutionError::Validation(format!(
                "Continuation {} frame {index} has an invalid invocation identity",
                continuation.run_id
            )));
        }
        let definition = plan
            .candidate
            .definitions
            .iter()
            .find(|definition| definition.id == frame.definition_id)
            .ok_or_else(|| {
                EvolutionError::Validation(format!(
                    "Continuation {} frame {index} references missing definition {}",
                    continuation.run_id, frame.definition_id
                ))
            })?;
        let region = region_at_path(&definition.body, &frame.region_path)?;
        if frame.next_step > region.steps.len() {
            return Err(EvolutionError::Validation(format!(
                "Continuation {} frame {index} program counter is outside its Region",
                continuation.run_id
            )));
        }
        if index == 0 {
            if frame.definition_id != plan.candidate.entry
                || !frame.invocation_path.is_empty()
                || !frame.region_path.is_empty()
            {
                return Err(EvolutionError::Validation(format!(
                    "Continuation {} first frame is not the entry invocation",
                    continuation.run_id
                )));
            }
            continue;
        }
        verify_child_frame(plan, continuation, index, frame)?;
    }
    Ok(())
}

fn verify_child_frame(
    plan: &SealedPlan,
    continuation: &Continuation,
    index: usize,
    frame: &FrameState,
) -> EvolutionResult<()> {
    let parent = &continuation.frames[index - 1];
    let parent_definition = plan
        .candidate
        .definitions
        .iter()
        .find(|definition| definition.id == parent.definition_id)
        .ok_or_else(|| {
            EvolutionError::Validation(format!(
                "Continuation {} parent frame references missing definition {}",
                continuation.run_id, parent.definition_id
            ))
        })?;
    let parent_region = region_at_path(&parent_definition.body, &parent.region_path)?;
    let parent_step = parent_region.steps.get(parent.next_step).ok_or_else(|| {
        EvolutionError::Validation(format!(
            "Continuation {} parent frame does not point at its active child step",
            continuation.run_id
        ))
    })?;
    match &parent_step.operation {
        cymule_core::Operation::Scope { .. } => {
            let mut expected_path = parent.region_path.clone();
            expected_path.push(parent.next_step);
            if frame.invocation_path != parent.invocation_path
                || frame.invocation_id != parent.invocation_id
                || frame.definition_id != parent.definition_id
                || frame.region_path != expected_path
            {
                return Err(EvolutionError::Validation(format!(
                    "Continuation {} scope frame {index} is not owned by its parent step",
                    continuation.run_id
                )));
            }
        }
        cymule_core::Operation::Invoke { definition, .. } => {
            let Some(segment) = frame.invocation_path.last() else {
                return Err(EvolutionError::Validation(format!(
                    "Continuation {} invoked frame {index} has no call-site segment",
                    continuation.run_id
                )));
            };
            if frame.invocation_path.len() != parent.invocation_path.len() + 1
                || !frame.invocation_path.starts_with(&parent.invocation_path)
                || segment.site_id != parent_step.id
                || segment.region_path != parent.region_path
                || segment.scope_id != parent.scope_id
                || frame.definition_id != *definition
                || !frame.region_path.is_empty()
            {
                return Err(EvolutionError::Validation(format!(
                    "Continuation {} invoked frame {index} is not owned by its parent step",
                    continuation.run_id
                )));
            }
        }
        _ => {
            return Err(EvolutionError::Validation(format!(
                "Continuation {} parent frame points at a non-structured child step",
                continuation.run_id
            )));
        }
    }
    Ok(())
}

fn region_at_path<'a>(
    root: &'a cymule_core::Region,
    path: &[usize],
) -> EvolutionResult<&'a cymule_core::Region> {
    let mut region = root;
    for index in path {
        let step = region.steps.get(*index).ok_or_else(|| {
            EvolutionError::Validation("Continuation Region path is outside its Plan".to_owned())
        })?;
        let cymule_core::Operation::Scope { body, .. } = &step.operation else {
            return Err(EvolutionError::Validation(
                "Continuation Region path crosses a non-scope step".to_owned(),
            ));
        };
        region = body;
    }
    Ok(region)
}

/// Execute and fully validate one migration provider product without mutating
/// controller state. The normalized persistence reducer admits the returned
/// receipt only after this function succeeds.
pub(crate) fn execute_migration_product<A: MigrationAdapter + ?Sized>(
    adapter: &mut A,
    request: MigrationAdapterRequest,
    target_plan: &SealedPlan,
) -> EvolutionResult<(MigrationReceipt, Vec<ArtifactRecord>)> {
    let target_epoch = request
        .intent
        .expected_source_epoch
        .checked_add(1)
        .ok_or_else(|| {
            EvolutionError::Validation("migration target epoch overflowed".to_owned())
        })?;
    if target_epoch > cymule_core::MAX_EXACT_INTEGER {
        return Err(EvolutionError::Validation(
            "migration target epoch exceeds the JSON safe-integer range".to_owned(),
        ));
    }
    let descriptor = adapter
        .describe()
        .map_err(map_migration_plugin_call_error)?;
    validate_migration_descriptor(&descriptor, &request).map_err(map_invalid_migration_output)?;
    let output = adapter
        .migrate(&request)
        .map_err(map_migration_plugin_call_error)?;
    output.verify_artifact_limits()?;
    verify_migration_artifact_record(&output.evidence)?;
    verify_migration_output(&request, target_epoch, &output)?;
    verify_target_program_counters(target_plan, &output.continuation).map_err(|error| {
        super::adapters::invalid_migration_output(format!(
            "migration plugin returned an invalid target program counter: {error}"
        ))
    })?;
    let output_state = output.continuation.state.clone().ok_or_else(|| {
        super::adapters::invalid_migration_output(
            "migration plugin returned a target Continuation without state",
        )
    })?;
    let mut artifacts = output.artifacts;
    artifacts.push(output.evidence.clone());
    let receipt = MigrationReceipt {
        request: request.intent.clone(),
        source_witness_id: request.source_witness_id,
        source_binding: request.source_binding,
        target_binding: request.target_binding,
        source_execution_fence: request.source_continuation.execution_fence,
        target_epoch,
        adapter_id: descriptor.adapter_id,
        adapter_revision: descriptor.adapter_revision,
        from_schema: descriptor.from_schema,
        to_schema: descriptor.to_schema,
        output_state,
        target_continuation: output.continuation,
        evidence: output.evidence.reference,
    };
    Ok((receipt, artifacts))
}

/// Execute and fully validate one shadow provider product without mutating
/// rollout state.
pub(crate) fn execute_shadow_product<D: ShadowDriver + ?Sized>(
    driver: &mut D,
    request: &ShadowRequest,
) -> EvolutionResult<(ShadowComparison, ArtifactRecord)> {
    validate_shadow_request(request)?;
    let descriptor = driver.describe().map_err(map_shadow_plugin_call_error)?;
    validate_shadow_descriptor(&descriptor, request).map_err(map_invalid_shadow_output)?;
    let output = driver
        .execute(request)
        .map_err(map_shadow_plugin_call_error)?;
    output.verify_evidence_limits()?;
    verify_artifact_record(&output.evidence).map_err(|error| {
        super::adapters::invalid_shadow_output(format!(
            "shadow plugin returned invalid evidence: {error}"
        ))
    })?;
    let evidence = output.evidence;
    let comparison = ShadowComparison {
        comparison_id: request.comparison_id.clone(),
        subject: request.subject.clone(),
        decision_id: request.decision_id.clone(),
        primary_plan: request.primary_plan.clone(),
        shadow_plan: request.shadow_plan.clone(),
        driver_id: descriptor.driver_id,
        driver_revision: descriptor.driver_revision,
        comparison_policy: request.comparison_policy.clone(),
        primary_digest: output.primary_digest,
        shadow_digest: output.shadow_digest,
        equivalent: output.equivalent,
        evidence: evidence.reference.clone(),
    };
    super::live_control::verify_shadow_comparison(&comparison).map_err(|error| {
        super::adapters::invalid_shadow_output(format!(
            "shadow plugin returned an invalid comparison: {error}"
        ))
    })?;
    Ok((comparison, evidence))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::evolution::ShadowOutput;
    use cymule_core::canonical_bytes;

    struct FixedShadowDriver {
        descriptor: ShadowDriverDescriptor,
        output: ShadowOutput,
    }

    impl ShadowDriver for FixedShadowDriver {
        fn describe(&mut self) -> EvolutionResult<ShadowDriverDescriptor> {
            Ok(self.descriptor.clone())
        }

        fn execute(&mut self, _request: &ShadowRequest) -> EvolutionResult<ShadowOutput> {
            Ok(self.output.clone())
        }
    }

    fn valid_artifact() -> ArtifactRecord {
        let bytes = b"evolution evidence".to_vec();
        ArtifactRecord {
            reference: artifact_ref("cymule.test-evidence/1", &bytes).expect("valid reference"),
            bytes,
        }
    }

    fn maximum_canonical_shadow_evidence() -> ArtifactRecord {
        let empty = ArtifactRecord {
            reference: artifact_ref("cymule.test-shadow-evidence/1", &[])
                .expect("valid empty evidence reference"),
            bytes: Vec::new(),
        };
        let empty_len = canonical_bytes(&empty).unwrap().len();
        // Artifact bytes use padded Base64: N raw bytes add
        // `4 * ceil(N / 3)` canonical string bytes relative to the empty value.
        let payload_len = 3 * ((ShadowOutput::MAX_EVIDENCE_CANONICAL_BYTES - empty_len) / 4);
        let bytes = vec![0; payload_len];
        let record = ArtifactRecord {
            reference: artifact_ref("cymule.test-shadow-evidence/1", &bytes)
                .expect("valid boundary evidence reference"),
            bytes,
        };
        assert!(
            canonical_bytes(&record).unwrap().len() <= ShadowOutput::MAX_EVIDENCE_CANONICAL_BYTES
        );
        let mut next_bytes = record.bytes.clone();
        next_bytes.push(0);
        let next = ArtifactRecord {
            reference: artifact_ref("cymule.test-shadow-evidence/1", &next_bytes)
                .expect("valid next evidence reference"),
            bytes: next_bytes,
        };
        assert!(
            canonical_bytes(&next).unwrap().len() > ShadowOutput::MAX_EVIDENCE_CANONICAL_BYTES,
            "the next Base64 quantum must exceed the canonical evidence bound"
        );
        record
    }

    fn shadow_request() -> ShadowRequest {
        ShadowRequest {
            comparison_id: "comparison-1".to_owned(),
            decision_id: "decision-1".to_owned(),
            subject: "subject-1".to_owned(),
            primary_plan: format!("sha256:{}", "1".repeat(64)),
            shadow_plan: format!("sha256:{}", "2".repeat(64)),
            input: artifact_ref("cymule.test-shadow-input/1", b"input")
                .expect("valid shadow input"),
            driver_id: "shadow-main".to_owned(),
            driver_revision: format!("sha256:{}", "3".repeat(64)),
            comparison_policy: "exact-output/1".to_owned(),
        }
    }

    fn fixed_shadow_driver(output: ShadowOutput) -> FixedShadowDriver {
        FixedShadowDriver {
            descriptor: ShadowDriverDescriptor {
                driver_id: "shadow-main".to_owned(),
                driver_revision: format!("sha256:{}", "3".repeat(64)),
                target_effects: crate::evolution::ShadowEffectMode::SuppressedOrSimulated,
                occurrence_bindings: crate::evolution::ShadowBindingMode::Pinned,
            },
            output,
        }
    }

    #[test]
    fn artifact_record_verification_rejects_forged_kind_bytes_and_reference() {
        assert!(verify_artifact_record(&valid_artifact()).is_ok());

        let mut forged_kind = valid_artifact();
        forged_kind.reference.kind = "cymule.other-evidence/1".to_owned();
        assert!(matches!(
            verify_artifact_record(&forged_kind),
            Err(EvolutionError::Validation(_))
        ));

        let mut forged_bytes = valid_artifact();
        forged_bytes.bytes.push(0);
        assert!(matches!(
            verify_artifact_record(&forged_bytes),
            Err(EvolutionError::Validation(_))
        ));

        let mut forged_reference = valid_artifact();
        forged_reference.reference.artifact_id = format!("sha256:{}", "0".repeat(64));
        assert!(matches!(
            verify_artifact_record(&forged_reference),
            Err(EvolutionError::Validation(_))
        ));
    }

    #[test]
    fn in_process_shadow_product_enforces_the_exact_evidence_budget() {
        let boundary = maximum_canonical_shadow_evidence();
        let output = ShadowOutput {
            primary_digest: "1".repeat(64),
            shadow_digest: "2".repeat(64),
            equivalent: false,
            evidence: boundary.clone(),
        };
        execute_shadow_product(&mut fixed_shadow_driver(output), &shadow_request())
            .expect("maximum canonical evidence product is admitted in process");

        let mut oversized_bytes = boundary.bytes;
        oversized_bytes.push(0);
        let oversized = ShadowOutput {
            primary_digest: "1".repeat(64),
            shadow_digest: "2".repeat(64),
            equivalent: false,
            evidence: ArtifactRecord {
                reference: artifact_ref("cymule.test-shadow-evidence/1", &oversized_bytes)
                    .expect("valid oversized evidence reference"),
                bytes: oversized_bytes,
            },
        };
        assert!(matches!(
            execute_shadow_product(&mut fixed_shadow_driver(oversized), &shadow_request()),
            Err(EvolutionError::PluginDefect { code, .. })
                if code == ShadowOutput::INVALID_OUTPUT_DEFECT_CODE
        ));

        let raw_oversized_bytes = vec![0; ShadowOutput::MAX_EVIDENCE_RAW_BYTES];
        let raw_oversized = ShadowOutput {
            primary_digest: "1".repeat(64),
            shadow_digest: "2".repeat(64),
            equivalent: false,
            evidence: ArtifactRecord {
                reference: artifact_ref("cymule.test-shadow-evidence/1", &raw_oversized_bytes)
                    .expect("valid raw-oversized evidence reference"),
                bytes: raw_oversized_bytes,
            },
        };
        assert!(matches!(
            raw_oversized.verify_evidence_limits(),
            Err(EvolutionError::PluginDefect { code, .. })
                if code == ShadowOutput::INVALID_OUTPUT_DEFECT_CODE
        ));
    }

    #[test]
    fn malformed_migration_plugin_calls_have_one_stable_defect_code() {
        for error in [
            EvolutionError::Validation("wrong response variant".to_owned()),
            EvolutionError::NotFound("missing response member".to_owned()),
            EvolutionError::Conflict("descriptor revision mismatch".to_owned()),
        ] {
            assert!(matches!(
                map_migration_plugin_call_error(error),
                EvolutionError::PluginDefect { code, .. }
                    if code == super::MigrationOutput::INVALID_OUTPUT_DEFECT_CODE
            ));
        }
        assert!(matches!(
            map_migration_plugin_call_error(EvolutionError::TimedOut {
                code: "provider_deadline".to_owned(),
                message: "deadline elapsed".to_owned(),
            }),
            EvolutionError::TimedOut { code, .. } if code == "provider_deadline"
        ));
        assert!(matches!(
            map_migration_plugin_call_error(EvolutionError::Cancelled {
                code: "provider_cancelled".to_owned(),
                message: "cancelled before dispatch".to_owned(),
            }),
            EvolutionError::Cancelled { code, .. } if code == "provider_cancelled"
        ));
        assert!(matches!(
            map_migration_plugin_call_error(EvolutionError::Integrity {
                code: "provider_integrity".to_owned(),
                message: "provider binding changed".to_owned(),
            }),
            EvolutionError::Integrity { code, .. } if code == "provider_integrity"
        ));
        assert!(matches!(
            map_migration_plugin_call_error(EvolutionError::Substrate {
                code: "provider_unavailable".to_owned(),
                message: "provider unavailable".to_owned(),
            }),
            EvolutionError::Substrate { code, message }
                if code == "provider_unavailable" && message == "provider unavailable"
        ));
    }

    #[test]
    fn malformed_shadow_plugin_calls_have_one_stable_defect_code() {
        for error in [
            EvolutionError::Validation("wrong response variant".to_owned()),
            EvolutionError::NotFound("missing response member".to_owned()),
            EvolutionError::Conflict("descriptor revision mismatch".to_owned()),
        ] {
            assert!(matches!(
                map_shadow_plugin_call_error(error),
                EvolutionError::PluginDefect { code, .. }
                    if code == crate::evolution::ShadowOutput::INVALID_OUTPUT_DEFECT_CODE
            ));
        }
        assert!(matches!(
            map_shadow_plugin_call_error(EvolutionError::TimedOut {
                code: "provider_deadline".to_owned(),
                message: "deadline elapsed".to_owned(),
            }),
            EvolutionError::TimedOut { code, .. } if code == "provider_deadline"
        ));
        assert!(matches!(
            map_shadow_plugin_call_error(EvolutionError::Cancelled {
                code: "provider_cancelled".to_owned(),
                message: "cancelled before dispatch".to_owned(),
            }),
            EvolutionError::Cancelled { code, .. } if code == "provider_cancelled"
        ));
        assert!(matches!(
            map_shadow_plugin_call_error(EvolutionError::Integrity {
                code: "provider_integrity".to_owned(),
                message: "provider binding changed".to_owned(),
            }),
            EvolutionError::Integrity { code, .. } if code == "provider_integrity"
        ));
        assert!(matches!(
            map_shadow_plugin_call_error(EvolutionError::Substrate {
                code: "provider_unavailable".to_owned(),
                message: "provider unavailable".to_owned(),
            }),
            EvolutionError::Substrate { code, message }
                if code == "provider_unavailable" && message == "provider unavailable"
        ));
    }
}
