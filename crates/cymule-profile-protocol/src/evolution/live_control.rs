use std::collections::BTreeSet;

use cymule_core::MAX_EXACT_INTEGER;
use cymule_core::ROOT_SCOPE_ID;
use cymule_durable_protocol::ContinuationStatus;
use serde::{Deserialize, Serialize};

use super::{
    EvolutionCommand, EvolutionError, EvolutionResult, LinkedPlan, LivePublicationCommand,
    LivePublicationReceipt, MigrationReceipt, OccurrencePin, PlanEdge, PlanPatch, PlanTemplate,
    RestartReceipt, RolloutDecision, RolloutGate, RolloutObservation, RolloutTransition,
    ShadowComparison, SubflowRevision, control::validate_identity,
};

/// Complete cross-language live-evolution control version.
pub const LIVE_EVOLUTION_CONTROL_VERSION: &str = "cymule.live-evolution-control/6";

/// Closed commands for the unified registry, DAG, rollout, and pin authority.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "operation", rename_all = "snake_case", deny_unknown_fields)]
pub enum LiveEvolutionCommand {
    /// Publish one reusable definition before or between parent registrations.
    PublishDefinition {
        /// Control protocol version.
        control_version: String,
        /// Stable idempotency identity.
        command_id: String,
        /// Logical reusable-definition reference.
        logical_ref: String,
        /// Immutable definition content.
        definition: cymule_core::Definition,
        /// Strictly ordered exact reusable-definition dependencies.
        references: Vec<super::SubflowReference>,
    },
    /// Register one parent template and its initial future decision.
    RegisterTemplate {
        /// Control protocol version.
        control_version: String,
        /// Stable idempotency identity.
        command_id: String,
        /// Unsealed parent source and exact logical references.
        template: PlanTemplate,
    },
    /// Publish a revision and atomically relink every compatible dependent.
    PublishAndRelink {
        /// Control protocol version.
        control_version: String,
        /// Stable idempotency identity.
        command_id: String,
        /// Exact publication semantics.
        publication: LivePublicationCommand,
    },
    /// Apply one template-scoped DAG, rollout, migration, shadow, or pin command.
    Apply {
        /// Control protocol version.
        control_version: String,
        /// Stable unified idempotency identity.
        command_id: String,
        /// Registered parent template.
        template_id: String,
        /// Existing closed evolution operation.
        command: Box<EvolutionCommand>,
    },
}

/// Closed semantic outcome returned by one unified durable command.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "result", rename_all = "snake_case", deny_unknown_fields)]
pub enum LiveEvolutionOutcome {
    /// One immutable definition revision was published.
    DefinitionPublished {
        /// Published revision.
        revision: SubflowRevision,
    },
    /// One parent template and initial Plan were registered.
    TemplateRegistered {
        /// Initial exact linked Plan.
        linked: LinkedPlan,
    },
    /// One publication atomically updated every compatible dependent.
    PublicationApplied {
        /// Original idempotent publication receipt.
        receipt: LivePublicationReceipt,
    },
    /// One reviewed Plan edge was admitted.
    PatchApplied {
        /// Immutable DAG edge.
        edge: PlanEdge,
    },
    /// One future decision or observation was stored.
    Applied,
    /// One occurrence received complete immutable rollout and execution lineage.
    OccurrenceSelected {
        /// Exact retained occurrence pin.
        pin: OccurrencePin,
    },
    /// One checked migration completed.
    Migrated {
        /// Migration receipt.
        receipt: Box<MigrationReceipt>,
    },
    /// One replacement Run was authorized.
    RestartAuthorized {
        /// Restart receipt.
        receipt: Box<RestartReceipt>,
    },
    /// One isolated shadow comparison completed.
    ShadowRecorded {
        /// Shadow evidence.
        comparison: ShadowComparison,
    },
    /// One deterministic gate changed future selection.
    GateApplied {
        /// Promotion or rollback transition.
        transition: RolloutTransition,
    },
}

impl LiveEvolutionCommand {
    /// Stable outer idempotency identity.
    pub fn command_id(&self) -> &str {
        match self {
            Self::PublishDefinition { command_id, .. }
            | Self::RegisterTemplate { command_id, .. }
            | Self::PublishAndRelink { command_id, .. }
            | Self::Apply { command_id, .. } => command_id,
        }
    }

    /// Validate the complete transport envelope before stateful admission.
    ///
    /// # Errors
    ///
    /// Returns an error when the generation, identity, nested command, exact
    /// references, or immutable publication evidence is invalid.
    pub fn verify(&self) -> EvolutionResult<()> {
        let (control_version, command_id) = match self {
            Self::PublishDefinition {
                control_version,
                command_id,
                logical_ref,
                definition,
                references,
            } => {
                validate_identity("definition reference", logical_ref)?;
                validate_identity("definition", &definition.id)?;
                super::linker::validate_publication_references(
                    logical_ref,
                    definition,
                    references,
                )?;
                (control_version, command_id)
            }
            Self::RegisterTemplate {
                control_version,
                command_id,
                template,
            } => {
                validate_identity("template", &template.template_id)?;
                super::linker::validate_template_shape(template)?;
                (control_version, command_id)
            }
            Self::PublishAndRelink {
                control_version,
                command_id,
                publication,
            } => {
                validate_identity("definition reference", &publication.logical_ref)?;
                validate_identity("definition", &publication.definition.id)?;
                super::linker::validate_publication_references(
                    &publication.logical_ref,
                    &publication.definition,
                    &publication.references,
                )?;
                validate_artifact_record(&publication.evidence)?;
                (control_version, command_id)
            }
            Self::Apply {
                control_version,
                command_id,
                template_id,
                command,
            } => {
                validate_identity("template", template_id)?;
                command.verify()?;
                (control_version, command_id)
            }
        };
        if control_version != LIVE_EVOLUTION_CONTROL_VERSION {
            return Err(EvolutionError::Validation(format!(
                "unsupported live-evolution control version {control_version}"
            )));
        }
        validate_identity("live-evolution command", command_id)
    }
}

impl LiveEvolutionOutcome {
    /// Validate every nested value in one typed Engine success before an SDK
    /// exposes it. This is deliberately context-free: durable history and
    /// rollout admission remain owned by the controller, while wire identities,
    /// versions, immutable objects, and receipt self-consistency fail closed.
    ///
    /// # Errors
    ///
    /// Returns an error when any nested semantic result is malformed or fails
    /// its content-derived identity checks.
    pub fn verify_wire(&self) -> EvolutionResult<()> {
        match self {
            Self::DefinitionPublished { revision } => verify_subflow_revision(revision),
            Self::TemplateRegistered { linked } => verify_linked_plan(linked),
            Self::PublicationApplied { receipt } => verify_publication_receipt(receipt),
            Self::PatchApplied { edge } => verify_plan_edge(edge),
            Self::Applied => Ok(()),
            Self::OccurrenceSelected { pin } => verify_occurrence_pin(pin),
            Self::Migrated { receipt } => verify_migration_receipt(receipt),
            Self::RestartAuthorized { receipt } => verify_restart_receipt(receipt),
            Self::ShadowRecorded { comparison } => verify_shadow_comparison(comparison),
            Self::GateApplied { transition } => verify_rollout_transition(transition),
        }
    }
}

pub(crate) fn verify_command_outcome(
    command: &LiveEvolutionCommand,
    outcome: &LiveEvolutionOutcome,
) -> EvolutionResult<()> {
    let matches = match (command, outcome) {
        (
            LiveEvolutionCommand::PublishDefinition {
                logical_ref,
                definition,
                references,
                ..
            },
            LiveEvolutionOutcome::DefinitionPublished { revision },
        ) => {
            revision.logical_ref == *logical_ref
                && revision.definition == *definition
                && revision.references == *references
        }
        (
            LiveEvolutionCommand::RegisterTemplate { template, .. },
            LiveEvolutionOutcome::TemplateRegistered { linked },
        ) => {
            let referenced = template
                .references
                .iter()
                .map(|reference| reference.logical_ref.as_str())
                .collect::<BTreeSet<_>>();
            linked.template_id == template.template_id
                && referenced.len() == template.references.len()
                && referenced
                    .iter()
                    .all(|logical_ref| linked.resolved_revisions.contains_key(*logical_ref))
        }
        (
            LiveEvolutionCommand::PublishAndRelink { publication, .. },
            LiveEvolutionOutcome::PublicationApplied { receipt },
        ) => {
            receipt.revision.logical_ref == publication.logical_ref
                && receipt.revision.definition == publication.definition
                && receipt.revision.references == publication.references
        }
        (
            LiveEvolutionCommand::Apply {
                template_id,
                command,
                ..
            },
            outcome,
        ) => apply_outcome_matches(template_id, command, outcome)?,
        _ => false,
    };
    if matches {
        Ok(())
    } else {
        Err(EvolutionError::Validation(
            "live-evolution receipt outcome does not match its complete command".to_owned(),
        ))
    }
}

fn apply_outcome_matches(
    template_id: &str,
    command: &EvolutionCommand,
    outcome: &LiveEvolutionOutcome,
) -> EvolutionResult<bool> {
    Ok(match (command, outcome) {
        (
            EvolutionCommand::ApplyPatch { patch, .. },
            LiveEvolutionOutcome::PatchApplied { edge },
        ) => {
            let target = cymule_core::seal_plan(patch.target.clone())?;
            edge.from_plan == patch.from_plan
                && edge.to_plan == target.plan_id
                && edge.operations == patch.operations
        }
        (
            EvolutionCommand::SetRollout { .. } | EvolutionCommand::Observe { .. },
            LiveEvolutionOutcome::Applied,
        ) => true,
        (
            EvolutionCommand::SelectOccurrence {
                occurrence_id,
                selection_id,
                execution_binding,
                ..
            },
            LiveEvolutionOutcome::OccurrenceSelected { pin },
        ) => {
            pin.template_id == template_id
                && pin.occurrence_id == *occurrence_id
                && pin.selection_id == *selection_id
                && pin.execution_binding == *execution_binding
        }
        (EvolutionCommand::Migrate { request, .. }, LiveEvolutionOutcome::Migrated { receipt }) => {
            receipt.request == **request
        }
        (
            EvolutionCommand::RestartUnderNewPlan { request, .. },
            LiveEvolutionOutcome::RestartAuthorized { receipt },
        ) => receipt.request == **request,
        (
            EvolutionCommand::Shadow { request, .. },
            LiveEvolutionOutcome::ShadowRecorded { comparison },
        ) => {
            comparison.comparison_id == request.comparison_id
                && comparison.decision_id == request.decision_id
                && comparison.subject == request.subject
                && comparison.primary_plan == request.primary_plan
                && comparison.shadow_plan == request.shadow_plan
                && comparison.comparison_policy == request.comparison_policy
        }
        (
            EvolutionCommand::ApplyGate {
                gate,
                next_decision_id,
                ..
            },
            LiveEvolutionOutcome::GateApplied { transition },
        ) => transition.evaluation.gate == *gate && transition.to_decision == *next_decision_id,
        _ => false,
    })
}

pub(crate) fn verify_subflow_revision(revision: &SubflowRevision) -> EvolutionResult<()> {
    if revision.revision_version != super::SUBFLOW_REVISION_VERSION
        || revision.sequence == 0
        || revision.sequence > MAX_EXACT_INTEGER
    {
        return Err(EvolutionError::Validation(
            "subflow revision version or sequence is malformed".to_owned(),
        ));
    }
    validate_content_id("subflow revision", &revision.revision_id)?;
    validate_identity("definition reference", &revision.logical_ref)?;

    super::linker::validate_revision(&revision.logical_ref, revision)
}

pub(crate) fn verify_linked_plan(linked: &LinkedPlan) -> EvolutionResult<()> {
    validate_identity("template", &linked.template_id)?;
    linked.plan.verify()?;
    for (logical_ref, revision_id) in &linked.resolved_revisions {
        validate_identity("definition reference", logical_ref)?;
        validate_content_id("subflow revision", revision_id)?;
    }
    Ok(())
}

fn verify_publication_receipt(receipt: &LivePublicationReceipt) -> EvolutionResult<()> {
    verify_subflow_revision(&receipt.revision)?;
    let mut previous_template = None;
    for update in &receipt.updates {
        validate_identity("template", &update.template_id)?;
        validate_content_id("previous Plan", &update.previous_plan_id)?;
        validate_content_id("current Plan", &update.current_plan_id)?;
        if previous_template.is_some_and(|previous| previous >= update.template_id.as_str()) {
            return Err(EvolutionError::Validation(
                "live publication updates are not strictly template-ordered".to_owned(),
            ));
        }
        previous_template = Some(update.template_id.as_str());
        match (update.advanced, &update.decision_id) {
            (true, Some(decision_id)) if update.previous_plan_id != update.current_plan_id => {
                validate_content_id("rollout decision", decision_id)?;
            }
            (false, None) if update.previous_plan_id == update.current_plan_id => {}
            _ => {
                return Err(EvolutionError::Validation(
                    "live publication update does not match its Plan advance".to_owned(),
                ));
            }
        }
    }
    Ok(())
}

pub(crate) fn verify_plan_edge(edge: &PlanEdge) -> EvolutionResult<()> {
    validate_content_id("Plan edge", &edge.edge_id)?;
    validate_content_id("source Plan", &edge.from_plan)?;
    validate_content_id("target Plan", &edge.to_plan)?;
    if edge.from_plan == edge.to_plan {
        return Err(EvolutionError::Validation(
            "Plan edge requires distinct source and target Plans".to_owned(),
        ));
    }
    verify_patch_operations(&edge.operations)?;
    let expected_id =
        super::controller::derive_plan_edge_id(&edge.from_plan, &edge.to_plan, &edge.operations)?;
    if edge.edge_id != expected_id {
        return Err(EvolutionError::Validation(
            "Plan edge identity does not match its immutable structural transition".to_owned(),
        ));
    }
    Ok(())
}

pub(crate) fn verify_plan_patch(patch: &PlanPatch) -> EvolutionResult<()> {
    validate_content_id("patch source Plan", &patch.from_plan)?;
    let target = cymule_core::seal_plan(patch.target.clone())?;
    if target.plan_id == patch.from_plan {
        return Err(EvolutionError::Validation(
            "Plan patch requires distinct source and target Plans".to_owned(),
        ));
    }
    verify_patch_operations(&patch.operations)?;
    validate_artifact(&patch.evidence)
}

fn verify_patch_operations(operations: &[super::PatchOperation]) -> EvolutionResult<()> {
    if operations.is_empty() {
        return Err(EvolutionError::Validation(
            "Plan transition must contain a non-empty structural diff".to_owned(),
        ));
    }
    let mut previous = None;
    for operation in operations {
        validate_identity("patch target", &operation.target)?;
        let valid_shape = match operation.kind.as_str() {
            "add" => {
                operation.before.is_none() && operation.after.as_deref().is_some_and(is_digest)
            }
            "remove" => {
                operation.before.as_deref().is_some_and(is_digest) && operation.after.is_none()
            }
            "replace" => {
                operation.before.as_deref().is_some_and(is_digest)
                    && operation.after.as_deref().is_some_and(is_digest)
                    && operation.before != operation.after
            }
            _ => false,
        };
        if !valid_shape {
            return Err(EvolutionError::Validation(
                "Plan edge contains a malformed patch operation".to_owned(),
            ));
        }
        let current = (operation.target.as_str(), operation.kind.as_str());
        if previous.is_some_and(|previous| previous >= current) {
            return Err(EvolutionError::Validation(
                "Plan edge operations are not in canonical order".to_owned(),
            ));
        }
        previous = Some(current);
    }
    Ok(())
}

pub(crate) fn verify_rollout_decision(decision: &RolloutDecision) -> EvolutionResult<()> {
    validate_identity("rollout decision", &decision.decision_id)?;
    validate_content_id("rollout fallback Plan", &decision.fallback_plan)?;
    validate_content_id("rollout target Plan", &decision.target_plan)?;
    if let super::RolloutMode::Canary { basis_points } = decision.mode
        && basis_points > 10_000
    {
        return Err(EvolutionError::Validation(
            "canary basis_points must be <= 10000".to_owned(),
        ));
    }
    Ok(())
}

pub(crate) fn verify_rollout_observation(observation: &RolloutObservation) -> EvolutionResult<()> {
    validate_identity("rollout observation", &observation.observation_id)?;
    validate_identity("rollout decision", &observation.decision_id)?;
    validate_identity("occurrence", &observation.occurrence_id)?;
    validate_content_id("observed Plan", &observation.plan_id)?;
    validate_artifact(&observation.evidence)
}

pub(crate) fn verify_rollout_gate(gate: &RolloutGate) -> EvolutionResult<()> {
    validate_identity("rollout gate", &gate.gate_id)?;
    validate_identity("rollout decision", &gate.decision_id)?;
    for value in [
        gate.min_target_observations,
        gate.max_target_failures,
        gate.min_equivalent_shadows,
        gate.max_inequivalent_shadows,
    ] {
        if value > MAX_EXACT_INTEGER {
            return Err(EvolutionError::Validation(
                "rollout gate exceeds the JSON safe-integer range".to_owned(),
            ));
        }
    }
    Ok(())
}

pub(crate) fn verify_occurrence_pin(pin: &OccurrencePin) -> EvolutionResult<()> {
    validate_identity("occurrence", &pin.occurrence_id)?;
    validate_identity("template", &pin.template_id)?;
    validate_identity("rollout decision", &pin.decision_id)?;
    validate_content_id("selected Plan", &pin.plan_id)?;
    validate_identity("occurrence selection", &pin.selection_id)?;
    validate_execution_binding(&pin.execution_binding)
}

pub(crate) fn verify_migration_receipt(receipt: &MigrationReceipt) -> EvolutionResult<()> {
    let request = &receipt.request;
    validate_identity("migration", &request.migration_id)?;
    super::adapters::validate_run_identity(&request.run_id)?;
    validate_content_id("source Plan", &request.from_plan)?;
    validate_content_id("target Plan", &request.to_plan)?;
    validate_content_id("migration Plan edge", &request.plan_edge_id)?;
    validate_content_id("migration compatibility", &request.compatibility_id)?;
    validate_identity("migration adapter", &request.adapter_id)?;
    validate_content_id("migration adapter revision", &request.adapter_revision)?;
    validate_content_id("migration source witness", &receipt.source_witness_id)?;
    if request.from_plan == request.to_plan
        || request.expected_source_epoch > MAX_EXACT_INTEGER
        || receipt.source_execution_fence > MAX_EXACT_INTEGER
    {
        return Err(EvolutionError::Validation(
            "migration requires distinct Plans and an exact-range source epoch".to_owned(),
        ));
    }
    validate_execution_binding(&receipt.source_binding)?;
    validate_execution_binding(&receipt.target_binding)?;
    validate_identity("migration adapter", &receipt.adapter_id)?;
    validate_content_id("migration adapter revision", &receipt.adapter_revision)?;
    if receipt.adapter_id != request.adapter_id
        || receipt.adapter_revision != request.adapter_revision
    {
        return Err(EvolutionError::Validation(
            "migration receipt adapter does not match its semantic intent".to_owned(),
        ));
    }
    validate_identity("source schema", &receipt.from_schema)?;
    validate_identity("target schema", &receipt.to_schema)?;
    validate_artifact(&receipt.output_state)?;
    validate_artifact(&receipt.evidence)?;
    receipt
        .target_continuation
        .verify_wire()
        .map_err(|error| EvolutionError::Validation(error.to_string()))?;
    let target = &receipt.target_continuation;
    if request.expected_source_epoch.checked_add(1) != Some(receipt.target_epoch)
        || receipt.target_epoch > MAX_EXACT_INTEGER
        || target.run_id != request.run_id
        || target.plan_id != request.to_plan
        || target.binding_context != receipt.target_binding.artifact_id
        || target.epoch != receipt.target_epoch
        || target.state.as_ref() != Some(&receipt.output_state)
        || target.status != ContinuationStatus::Ready
        || target.execution_fence != receipt.source_execution_fence
        || target.execution_claim.is_some()
        || target.frames.is_empty()
        || !target.wait_set.is_empty()
        || target.scope_stack != [ROOT_SCOPE_ID]
    {
        return Err(EvolutionError::Validation(
            "migration receipt has an inconsistent target Continuation".to_owned(),
        ));
    }
    Ok(())
}

pub(crate) fn verify_restart_request(request: &super::RestartRequest) -> EvolutionResult<()> {
    validate_identity("restart", &request.restart_id)?;
    super::adapters::validate_run_identity(&request.run_id)?;
    super::adapters::validate_run_identity(&request.replacement_run)?;
    validate_content_id("source Plan", &request.from_plan)?;
    validate_content_id("target Plan", &request.to_plan)?;
    if request.run_id == request.replacement_run
        || request.from_plan == request.to_plan
        || request.expected_source_epoch > MAX_EXACT_INTEGER
    {
        return Err(EvolutionError::Validation(
            "restart intent has invalid source or replacement lineage".to_owned(),
        ));
    }
    validate_artifact(&request.input)?;
    validate_artifact(&request.evidence)
}

pub(crate) fn verify_restart_receipt(receipt: &RestartReceipt) -> EvolutionResult<()> {
    let request = &receipt.request;
    verify_restart_request(request)?;
    validate_content_id("restart source witness", &receipt.source_witness_id)?;
    receipt.target_plan.verify()?;
    if receipt.target_plan.plan_id != request.to_plan {
        return Err(EvolutionError::Validation(
            "restart receipt target Plan does not match its request".to_owned(),
        ));
    }
    Ok(())
}

pub(crate) fn verify_shadow_comparison(comparison: &ShadowComparison) -> EvolutionResult<()> {
    validate_identity("shadow comparison", &comparison.comparison_id)?;
    validate_identity("shadow subject", &comparison.subject)?;
    validate_identity("rollout decision", &comparison.decision_id)?;
    validate_content_id("primary Plan", &comparison.primary_plan)?;
    validate_content_id("shadow Plan", &comparison.shadow_plan)?;
    validate_identity("shadow driver", &comparison.driver_id)?;
    validate_content_id("shadow driver revision", &comparison.driver_revision)?;
    validate_identity("comparison policy", &comparison.comparison_policy)?;
    if !is_digest(&comparison.primary_digest) || !is_digest(&comparison.shadow_digest) {
        return Err(EvolutionError::Validation(
            "shadow result digests must be lowercase SHA-256 values".to_owned(),
        ));
    }
    validate_artifact(&comparison.evidence)
}

pub(crate) fn verify_rollout_transition(transition: &RolloutTransition) -> EvolutionResult<()> {
    validate_content_id("rollout transition", &transition.transition_id)?;
    validate_identity("source rollout decision", &transition.from_decision)?;
    validate_identity("target rollout decision", &transition.to_decision)?;
    if transition.from_decision == transition.to_decision {
        return Err(EvolutionError::Validation(
            "rollout transition must create a distinct decision".to_owned(),
        ));
    }
    let evaluation = &transition.evaluation;
    validate_content_id("rollout evaluation", &evaluation.evaluation_id)?;
    verify_rollout_gate(&evaluation.gate)?;
    if evaluation.gate.decision_id != transition.from_decision {
        return Err(EvolutionError::Validation(
            "rollout transition does not match its gate decision".to_owned(),
        ));
    }
    for value in [
        evaluation.gate.min_target_observations,
        evaluation.gate.max_target_failures,
        evaluation.gate.min_equivalent_shadows,
        evaluation.gate.max_inequivalent_shadows,
        evaluation.target_observations,
        evaluation.target_failures,
        evaluation.equivalent_shadows,
        evaluation.inequivalent_shadows,
    ] {
        if value > MAX_EXACT_INTEGER {
            return Err(EvolutionError::Validation(
                "rollout transition exceeds the JSON safe-integer range".to_owned(),
            ));
        }
    }
    if evaluation.target_failures > evaluation.target_observations {
        return Err(EvolutionError::Validation(
            "rollout failures exceed target observations".to_owned(),
        ));
    }
    let evidence_count = evaluation
        .target_observations
        .checked_add(evaluation.equivalent_shadows)
        .and_then(|count| count.checked_add(evaluation.inequivalent_shadows))
        .ok_or_else(|| {
            EvolutionError::Validation("rollout evidence count overflowed".to_owned())
        })?;
    if evidence_count != evaluation.evidence_count || evaluation.evidence_count > MAX_EXACT_INTEGER
    {
        return Err(EvolutionError::Validation(
            "rollout evidence counts do not match the frozen accumulator".to_owned(),
        ));
    }
    validate_content_id("rollout evidence root", &evaluation.evidence_root)?;
    let expected_outcome = if evaluation.target_failures > evaluation.gate.max_target_failures
        || evaluation.inequivalent_shadows > evaluation.gate.max_inequivalent_shadows
    {
        super::GateOutcome::Rollback
    } else if evaluation.target_observations >= evaluation.gate.min_target_observations
        && evaluation.equivalent_shadows >= evaluation.gate.min_equivalent_shadows
    {
        super::GateOutcome::Promote
    } else {
        super::GateOutcome::Pending
    };
    if evaluation.outcome != expected_outcome || evaluation.outcome == super::GateOutcome::Pending {
        return Err(EvolutionError::Validation(
            "rollout transition outcome does not match its exact evidence".to_owned(),
        ));
    }
    let expected_evaluation_id = super::controller::derive_rollout_evaluation_id(evaluation)?;
    if evaluation.evaluation_id != expected_evaluation_id {
        return Err(EvolutionError::Validation(
            "rollout evaluation identity does not match its immutable evidence".to_owned(),
        ));
    }
    let expected_transition_id = super::controller::derive_rollout_transition_id(
        &transition.from_decision,
        &transition.to_decision,
        evaluation,
    )?;
    if transition.transition_id != expected_transition_id {
        return Err(EvolutionError::Validation(
            "rollout transition identity does not match its immutable content".to_owned(),
        ));
    }
    Ok(())
}

fn validate_execution_binding(binding: &cymule_core::ArtifactRef) -> EvolutionResult<()> {
    validate_artifact(binding)?;
    if binding.kind != cymule_runtime::EXECUTION_BINDING_VERSION {
        return Err(EvolutionError::Validation(
            "execution binding is not an exact ExecutionBinding Artifact".to_owned(),
        ));
    }
    Ok(())
}

fn validate_content_id(kind: &str, value: &str) -> EvolutionResult<()> {
    if !super::adapters::is_content_id(value) {
        return Err(EvolutionError::Validation(format!(
            "{kind} identity must be a lowercase sha256 content ID"
        )));
    }
    Ok(())
}

fn is_digest(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn validate_artifact(artifact: &cymule_core::ArtifactRef) -> EvolutionResult<()> {
    artifact
        .validate()
        .map_err(|error| EvolutionError::Validation(error.to_string()))
}

pub(crate) fn validate_artifact_record(
    artifact: &cymule_core::ArtifactRecord,
) -> EvolutionResult<()> {
    validate_artifact(&artifact.reference)?;
    let derived = cymule_core::artifact_ref(&artifact.reference.kind, &artifact.bytes)
        .map_err(|error| EvolutionError::Validation(error.to_string()))?;
    if derived != artifact.reference {
        return Err(EvolutionError::Validation(format!(
            "Artifact {} does not match its exact bytes",
            artifact.reference.artifact_id
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use cymule_core::content_id;
    use serde_json::Value;

    use super::*;

    fn rollout_transition() -> RolloutTransition {
        let gate = RolloutGate {
            gate_id: "gate-1".to_owned(),
            decision_id: "decision-from".to_owned(),
            min_target_observations: 0,
            max_target_failures: 0,
            min_equivalent_shadows: 0,
            max_inequivalent_shadows: 0,
        };
        let evidence_root = super::super::empty_evolution_evidence_root().unwrap();
        let mut evaluation = super::super::RolloutEvaluation {
            evaluation_id: String::new(),
            gate,
            target_observations: 0,
            target_failures: 0,
            equivalent_shadows: 0,
            inequivalent_shadows: 0,
            outcome: super::super::GateOutcome::Promote,
            evidence_count: 0,
            evidence_root,
        };
        evaluation.evaluation_id =
            super::super::controller::derive_rollout_evaluation_id(&evaluation).unwrap();
        let mut transition = RolloutTransition {
            transition_id: String::new(),
            from_decision: "decision-from".to_owned(),
            to_decision: "decision-to".to_owned(),
            evaluation,
        };
        transition.transition_id = super::super::controller::derive_rollout_transition_id(
            &transition.from_decision,
            &transition.to_decision,
            &transition.evaluation,
        )
        .unwrap();
        transition
    }

    #[test]
    fn plan_edge_v2_binds_every_structural_field_and_rejects_legacy_evidence_member() {
        let from_plan = content_id("cymule.test-plan/1", &"from").unwrap();
        let to_plan = content_id("cymule.test-plan/1", &"to").unwrap();
        let operations = vec![super::super::PatchOperation {
            kind: "replace".to_owned(),
            target: "definition:main".to_owned(),
            before: Some("1".repeat(64)),
            after: Some("2".repeat(64)),
        }];
        let edge_id =
            super::super::controller::derive_plan_edge_id(&from_plan, &to_plan, &operations)
                .unwrap();
        let edge = PlanEdge {
            edge_id,
            from_plan,
            to_plan,
            operations,
        };
        verify_plan_edge(&edge).unwrap();

        let legacy_evidence =
            cymule_core::artifact_ref("cymule.test-edge-evidence/1", b"evidence").unwrap();
        let mut legacy_id = edge.clone();
        legacy_id.edge_id = content_id(
            "cymule.plan-edge/1",
            &(
                edge.from_plan.as_str(),
                &edge.to_plan,
                &edge.operations,
                &legacy_evidence,
            ),
        )
        .unwrap();
        assert!(verify_plan_edge(&legacy_id).is_err());

        let mut changed_id = edge.clone();
        changed_id.edge_id = content_id("cymule.test-edge/1", &()).unwrap();
        assert!(verify_plan_edge(&changed_id).is_err());
        let mut changed_source = edge.clone();
        changed_source.from_plan = content_id("cymule.test-plan/1", &"other-from").unwrap();
        assert!(verify_plan_edge(&changed_source).is_err());
        let mut changed_target = edge.clone();
        changed_target.to_plan = content_id("cymule.test-plan/1", &"other-to").unwrap();
        assert!(verify_plan_edge(&changed_target).is_err());
        let mut changed_operations = edge.clone();
        changed_operations.operations[0].after = Some("3".repeat(64));
        assert!(verify_plan_edge(&changed_operations).is_err());

        let wire = serde_json::to_value(&edge).unwrap();
        for member in ["edge_id", "from_plan", "to_plan", "operations"] {
            let mut missing = wire.clone();
            missing.as_object_mut().unwrap().remove(member);
            assert!(serde_json::from_value::<PlanEdge>(missing).is_err());
        }
        let mut legacy = wire;
        legacy.as_object_mut().unwrap().insert(
            "evidence".to_owned(),
            serde_json::to_value(legacy_evidence).unwrap(),
        );
        assert!(serde_json::from_value::<PlanEdge>(legacy).is_err());
    }

    #[test]
    fn rollout_transition_v2_recomputes_every_retained_identity_input() {
        let transition = rollout_transition();
        verify_rollout_transition(&transition).unwrap();

        macro_rules! rejects_change {
            ($change:expr) => {{
                let mut changed = transition.clone();
                $change(&mut changed);
                assert!(verify_rollout_transition(&changed).is_err());
            }};
        }

        rejects_change!(|value: &mut RolloutTransition| value.transition_id =
            content_id("cymule.test-transition/1", &()).unwrap());
        rejects_change!(|value: &mut RolloutTransition| value.from_decision =
            "decision-other-source".to_owned());
        rejects_change!(
            |value: &mut RolloutTransition| value.to_decision = "decision-other-target".to_owned()
        );
        rejects_change!(
            |value: &mut RolloutTransition| value.evaluation.evaluation_id =
                content_id("cymule.test-evaluation/1", &()).unwrap()
        );
        rejects_change!(
            |value: &mut RolloutTransition| value.evaluation.gate.gate_id = "gate-other".to_owned()
        );
        rejects_change!(
            |value: &mut RolloutTransition| value.evaluation.gate.decision_id =
                "decision-other-source".to_owned()
        );
        rejects_change!(|value: &mut RolloutTransition| value
            .evaluation
            .gate
            .min_target_observations = 1);
        rejects_change!(|value: &mut RolloutTransition| value
            .evaluation
            .gate
            .max_target_failures = 1);
        rejects_change!(|value: &mut RolloutTransition| value
            .evaluation
            .gate
            .min_equivalent_shadows = 1);
        rejects_change!(|value: &mut RolloutTransition| value
            .evaluation
            .gate
            .max_inequivalent_shadows = 1);
        rejects_change!(|value: &mut RolloutTransition| value.evaluation.target_observations = 1);
        rejects_change!(|value: &mut RolloutTransition| value.evaluation.target_failures = 1);
        rejects_change!(|value: &mut RolloutTransition| value.evaluation.equivalent_shadows = 1);
        rejects_change!(|value: &mut RolloutTransition| value.evaluation.inequivalent_shadows = 1);
        rejects_change!(|value: &mut RolloutTransition| value.evaluation.outcome =
            super::super::GateOutcome::Rollback);
        rejects_change!(|value: &mut RolloutTransition| value.evaluation.evidence_count = 1);
        rejects_change!(
            |value: &mut RolloutTransition| value.evaluation.evidence_root =
                content_id("cymule.test-evidence-root/1", &()).unwrap()
        );
    }

    #[test]
    fn rollout_transition_rejects_v1_identity_and_missing_required_members() {
        let transition = rollout_transition();
        let mut legacy = transition.clone();
        legacy.transition_id = content_id(
            "cymule.rollout-transition/1",
            &(
                legacy.from_decision.as_str(),
                legacy.to_decision.as_str(),
                &legacy.evaluation,
            ),
        )
        .unwrap();
        assert!(verify_rollout_transition(&legacy).is_err());

        let wire = serde_json::to_value(&transition).unwrap();
        for member in [
            "transition_id",
            "from_decision",
            "to_decision",
            "evaluation",
        ] {
            let mut missing = wire.clone();
            missing.as_object_mut().unwrap().remove(member);
            assert!(serde_json::from_value::<RolloutTransition>(missing).is_err());
        }
        for member in [
            "evaluation_id",
            "gate",
            "target_observations",
            "target_failures",
            "equivalent_shadows",
            "inequivalent_shadows",
            "outcome",
            "evidence_count",
            "evidence_root",
        ] {
            let mut missing = wire.clone();
            let evaluation = missing
                .get_mut("evaluation")
                .and_then(Value::as_object_mut)
                .unwrap();
            evaluation.remove(member);
            assert!(serde_json::from_value::<RolloutTransition>(missing).is_err());
        }
        for member in [
            "gate_id",
            "decision_id",
            "min_target_observations",
            "max_target_failures",
            "min_equivalent_shadows",
            "max_inequivalent_shadows",
        ] {
            let mut missing = wire.clone();
            let gate = missing
                .get_mut("evaluation")
                .and_then(|value| value.get_mut("gate"))
                .and_then(Value::as_object_mut)
                .unwrap();
            gate.remove(member);
            assert!(serde_json::from_value::<RolloutTransition>(missing).is_err());
        }
    }
}
