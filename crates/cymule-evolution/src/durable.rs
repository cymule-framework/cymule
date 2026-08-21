use std::collections::BTreeSet;

use cymule_core::{
    ArtifactRecord, ArtifactRef, COMMAND_VERSION, Command, CommandEnvelope, CommandReceiptStatus,
    Machine, SealedPlan,
};
use cymule_durable::{DurableCoordinator, DurableStore, JournalBatch, JournalRecord};
use serde::{Deserialize, Serialize};

use crate::{
    EvolutionController, EvolutionError, EvolutionResult, EvolutionSnapshot, MigrationAdapter,
    MigrationReceipt, MigrationRequest, MigrationSafePoint, PlanEdge, PlanPatch, RestartReceipt,
    RestartRequest, RolloutDecision, RolloutGate, RolloutObservation, RolloutTransition,
    ShadowComparison, ShadowDriver, ShadowRequest,
};

/// Versioned M4 checkpoint stored in the generic M1 journal.
pub const EVOLUTION_CHECKPOINT_SCHEMA: &str = "cymule.evolution-checkpoint/4";

/// One complete portable evolution checkpoint with explicit lineage.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EvolutionCheckpoint {
    /// Checkpoint schema and semantic version.
    pub checkpoint_version: String,
    /// Stable caller-supplied idempotency identity.
    pub checkpoint_id: String,
    /// Previous checkpoint in this journal.
    pub parent_checkpoint: Option<String>,
    /// Complete M4 reducer state.
    pub snapshot: EvolutionSnapshot,
}

/// M1 journal integration for provider-neutral live-evolution control.
pub struct DurableEvolutionController;

impl DurableEvolutionController {
    /// Rebuild the evolution controller from one ordered M1 journal.
    pub fn load<S: DurableStore>(
        coordinator: &DurableCoordinator<S>,
        journal_id: &str,
    ) -> EvolutionResult<EvolutionController> {
        let records = coordinator
            .journal_records(journal_id)
            .map_err(durable_error)?;
        if records.is_empty() {
            return Ok(EvolutionController::new());
        }
        let mut parent = None;
        let mut controller = None;
        for record in records {
            let checkpoint = decode(record)?;
            if checkpoint.parent_checkpoint != parent {
                return Err(EvolutionError::Validation(format!(
                    "evolution checkpoint {} has discontinuous lineage",
                    checkpoint.checkpoint_id
                )));
            }
            parent = Some(checkpoint.checkpoint_id);
            controller = Some(EvolutionController::restore(checkpoint.snapshot)?);
        }
        controller.ok_or_else(|| {
            EvolutionError::Validation("evolution journal did not restore".to_owned())
        })
    }

    /// Persist one complete idempotent M4 checkpoint.
    pub fn checkpoint<S: DurableStore>(
        coordinator: &mut DurableCoordinator<S>,
        controller: &EvolutionController,
        journal_id: &str,
        checkpoint_id: &str,
    ) -> EvolutionResult<String> {
        let record = checkpoint_record(coordinator, controller, journal_id, checkpoint_id)?;
        coordinator
            .append_journal_record(journal_id, record)
            .map_err(durable_error)
    }

    /// Diff two immutable Plans, add their edge, and checkpoint atomically.
    pub fn add_diff_edge_and_checkpoint<S: DurableStore>(
        coordinator: &mut DurableCoordinator<S>,
        controller: &mut EvolutionController,
        journal_id: &str,
        checkpoint_id: &str,
        from_plan: &str,
        child: &SealedPlan,
        evidence: ArtifactRef,
    ) -> EvolutionResult<PlanEdge> {
        apply_and_checkpoint(
            coordinator,
            controller,
            journal_id,
            checkpoint_id,
            |controller| controller.add_diff_edge(from_plan, child, evidence),
        )
    }

    /// Seal an exact reviewed patch, admit its DAG edge, and checkpoint.
    pub fn apply_patch_and_checkpoint<S: DurableStore>(
        coordinator: &mut DurableCoordinator<S>,
        controller: &mut EvolutionController,
        journal_id: &str,
        checkpoint_id: &str,
        patch: PlanPatch,
    ) -> EvolutionResult<PlanEdge> {
        apply_and_checkpoint(
            coordinator,
            controller,
            journal_id,
            checkpoint_id,
            |controller| controller.apply_patch(patch),
        )
    }

    /// Change future rollout selection and checkpoint the decision atomically.
    pub fn set_rollout_and_checkpoint<S: DurableStore>(
        coordinator: &mut DurableCoordinator<S>,
        controller: &mut EvolutionController,
        journal_id: &str,
        checkpoint_id: &str,
        decision: RolloutDecision,
    ) -> EvolutionResult<()> {
        apply_and_checkpoint(
            coordinator,
            controller,
            journal_id,
            checkpoint_id,
            |controller| controller.set_rollout(decision),
        )
    }

    /// Pin one mixed-version occurrence and checkpoint before dispatch.
    pub fn select_occurrence_and_checkpoint<S: DurableStore>(
        coordinator: &mut DurableCoordinator<S>,
        controller: &mut EvolutionController,
        journal_id: &str,
        checkpoint_id: &str,
        occurrence_id: &str,
    ) -> EvolutionResult<String> {
        apply_and_checkpoint(
            coordinator,
            controller,
            journal_id,
            checkpoint_id,
            |controller| controller.select_for_occurrence(occurrence_id),
        )
    }

    /// Record shadow evidence and checkpoint before it can gate promotion.
    pub fn record_shadow_and_checkpoint<S: DurableStore>(
        coordinator: &mut DurableCoordinator<S>,
        controller: &mut EvolutionController,
        journal_id: &str,
        checkpoint_id: &str,
        comparison: ShadowComparison,
    ) -> EvolutionResult<()> {
        apply_and_checkpoint(
            coordinator,
            controller,
            journal_id,
            checkpoint_id,
            |controller| controller.record_shadow(comparison),
        )
    }

    /// Execute a checked migration and checkpoint its pinned receipt atomically.
    pub fn execute_migration_and_checkpoint<S: DurableStore, A: MigrationAdapter>(
        coordinator: &mut DurableCoordinator<S>,
        controller: &mut EvolutionController,
        journal_id: &str,
        checkpoint_id: &str,
        adapter: &mut A,
        request: MigrationRequest,
        safe_point: &MigrationSafePoint,
    ) -> EvolutionResult<MigrationReceipt> {
        if let Some(receipt) = replay_migration(coordinator, journal_id, checkpoint_id, &request)? {
            verify_committed_migration(coordinator, &receipt)?;
            return Ok(receipt);
        }
        verify_durable_safe_point(coordinator, safe_point)?;
        let reviewed_edge = controller.edge(&request.plan_edge_id).ok_or_else(|| {
            EvolutionError::NotFound(format!(
                "reviewed migration edge {} is missing",
                request.plan_edge_id
            ))
        })?;
        let machine = coordinator.restore_machine().map_err(durable_error)?;
        if machine.artifact(&reviewed_edge.evidence).is_none() {
            return Err(EvolutionError::NotFound(
                "reviewed migration-edge evidence Artifact is missing".to_owned(),
            ));
        }
        let before = controller.snapshot();
        let (receipt, artifacts) =
            controller.execute_migration_with_artifacts(adapter, request, safe_point)?;
        let record = checkpoint_record(coordinator, controller, journal_id, checkpoint_id)?;
        let target_plan = controller.plan(&receipt.to_plan).cloned().ok_or_else(|| {
            EvolutionError::NotFound("migration target Plan is missing".to_owned())
        })?;
        if let Err(error) = checkpoint_migrated_run(
            coordinator,
            &receipt,
            safe_point,
            &target_plan,
            artifacts,
            &[JournalBatch {
                journal_id: journal_id.to_owned(),
                records: vec![record],
            }],
        ) {
            *controller = EvolutionController::restore(before)
                .expect("previously valid evolution snapshot restores");
            return Err(error);
        }
        Ok(receipt)
    }

    /// Authorize one replacement Run and checkpoint its exact target Plan.
    pub fn restart_under_new_plan_and_checkpoint<S: DurableStore>(
        coordinator: &mut DurableCoordinator<S>,
        controller: &mut EvolutionController,
        journal_id: &str,
        checkpoint_id: &str,
        request: RestartRequest,
        safe_point: &MigrationSafePoint,
    ) -> EvolutionResult<RestartReceipt> {
        verify_durable_safe_point(coordinator, safe_point)?;
        apply_and_checkpoint(
            coordinator,
            controller,
            journal_id,
            checkpoint_id,
            |controller| controller.restart_under_new_plan(request, safe_point),
        )
    }

    /// Execute isolated shadow work and checkpoint comparison evidence.
    pub fn execute_shadow_and_checkpoint<S: DurableStore, D: ShadowDriver>(
        coordinator: &mut DurableCoordinator<S>,
        controller: &mut EvolutionController,
        journal_id: &str,
        checkpoint_id: &str,
        driver: &mut D,
        request: ShadowRequest,
    ) -> EvolutionResult<ShadowComparison> {
        let before = controller.snapshot();
        let (comparison, artifacts) = controller.execute_shadow_with_artifacts(driver, request)?;
        if let Err(error) = checkpoint_artifacts(
            coordinator,
            controller,
            journal_id,
            checkpoint_id,
            artifacts,
            &BTreeSet::from([comparison.evidence.clone()]),
        ) {
            *controller = EvolutionController::restore(before)
                .expect("previously valid evolution snapshot restores");
            return Err(error);
        }
        Ok(comparison)
    }

    /// Record one terminal rollout observation and checkpoint before gating.
    pub fn record_observation_and_checkpoint<S: DurableStore>(
        coordinator: &mut DurableCoordinator<S>,
        controller: &mut EvolutionController,
        journal_id: &str,
        checkpoint_id: &str,
        observation: RolloutObservation,
    ) -> EvolutionResult<()> {
        apply_and_checkpoint(
            coordinator,
            controller,
            journal_id,
            checkpoint_id,
            |controller| controller.record_observation(observation),
        )
    }

    /// Apply a ready promotion/rollback gate and checkpoint the new decision.
    pub fn apply_gate_and_checkpoint<S: DurableStore>(
        coordinator: &mut DurableCoordinator<S>,
        controller: &mut EvolutionController,
        journal_id: &str,
        checkpoint_id: &str,
        gate: RolloutGate,
        next_decision_id: impl Into<String>,
    ) -> EvolutionResult<RolloutTransition> {
        apply_and_checkpoint(
            coordinator,
            controller,
            journal_id,
            checkpoint_id,
            |controller| controller.apply_gate(gate, next_decision_id),
        )
    }
}

pub(crate) fn checkpoint_migrated_run<S: DurableStore>(
    coordinator: &mut DurableCoordinator<S>,
    receipt: &MigrationReceipt,
    safe_point: &MigrationSafePoint,
    target_plan: &SealedPlan,
    artifacts: Vec<ArtifactRecord>,
    batches: &[JournalBatch],
) -> EvolutionResult<()> {
    let source = coordinator
        .state()
        .map_err(durable_error)?
        .continuations
        .get(&receipt.run_id)
        .cloned()
        .ok_or_else(|| {
            EvolutionError::NotFound("migration source Continuation is missing".to_owned())
        })?;
    safe_point.verify_continuation(&source)?;
    let expected_source_binding = ArtifactRef {
        identity_version: cymule_core::ARTIFACT_IDENTITY_VERSION.to_owned(),
        artifact_id: source.binding_context.clone(),
        kind: cymule_runtime::EXECUTION_BINDING_VERSION.to_owned(),
    };
    if receipt.source_binding != expected_source_binding
        || receipt.target_binding.artifact_id == receipt.to_plan
        || receipt.target_epoch != source.epoch.saturating_add(1)
    {
        return Err(EvolutionError::Conflict(
            "migration receipt does not match the source binding and next epoch".to_owned(),
        ));
    }
    let mut machine = coordinator.restore_machine().map_err(durable_error)?;
    let target_binding_record = machine.artifact(&receipt.target_binding).ok_or_else(|| {
        EvolutionError::NotFound("migration target ExecutionBinding Artifact is missing".to_owned())
    })?;
    let target_binding: cymule_runtime::ExecutionBinding =
        serde_json::from_slice(&target_binding_record.bytes)
            .map_err(|error| EvolutionError::Validation(error.to_string()))?;
    if target_binding
        .artifact_ref()
        .map_err(|error| EvolutionError::Validation(error.to_string()))?
        != receipt.target_binding
    {
        return Err(EvolutionError::Validation(
            "migration target ExecutionBinding Artifact identity is invalid".to_owned(),
        ));
    }
    target_binding
        .admit_plan(target_plan)
        .map_err(|error| EvolutionError::Validation(error.to_string()))?;
    machine
        .insert_plan(target_plan.clone())
        .map_err(|error| EvolutionError::Validation(error.to_string()))?;
    let required = artifacts
        .iter()
        .map(|artifact| artifact.reference.clone())
        .collect::<BTreeSet<_>>();
    for artifact in artifacts {
        let derived = machine
            .put_artifact(artifact.reference.kind.clone(), artifact.bytes)
            .map_err(|error| EvolutionError::Validation(error.to_string()))?;
        if derived != artifact.reference {
            return Err(EvolutionError::Validation(
                "migration Artifact bytes do not match their references".to_owned(),
            ));
        }
    }
    submit_machine(
        &mut machine,
        &source.run_id,
        format!("migration:{}:plan", receipt.migration_id),
        Command::MigrateRun {
            from_plan: receipt.from_plan.clone(),
            to_plan: receipt.to_plan.clone(),
            from_binding: receipt.source_binding.artifact_id.clone(),
            to_binding: receipt.target_binding.artifact_id.clone(),
            safe_point_id: receipt.safe_point_id.clone(),
        },
    )?;
    submit_machine(
        &mut machine,
        &source.run_id,
        format!("migration:{}:epoch", receipt.migration_id),
        Command::AdvanceEpoch,
    )?;
    submit_machine(
        &mut machine,
        &source.run_id,
        format!("migration:{}:attempt", receipt.migration_id),
        Command::BeginAttempt {
            attempt_id: format!("attempt:{}:{}", source.run_id, receipt.target_epoch),
            continuation_id: format!("continuation:{}", source.run_id),
            occurrence_binding: receipt.target_binding.artifact_id.clone(),
            epoch: receipt.target_epoch,
        },
    )?;
    let target = receipt.target_continuation.clone();
    if !required.contains(&receipt.output_state) || !required.contains(&receipt.evidence) {
        return Err(EvolutionError::Validation(
            "migration output state and evidence must be returned as complete Artifact records"
                .to_owned(),
        ));
    }
    coordinator
        .checkpoint_run_migration_journals(cymule_durable::RunMigrationCheckpoint {
            machine: &machine,
            source: &source,
            target: &target,
            target_plan,
            artifacts: &required,
            safe_point_id: &receipt.safe_point_id,
            batches,
        })
        .map(|_| ())
        .map_err(durable_error)
}

fn submit_machine(
    machine: &mut Machine,
    run_id: &str,
    command_id: String,
    command: Command,
) -> EvolutionResult<()> {
    let expected_precondition = Some(
        machine
            .projection()
            .runs
            .get(run_id)
            .ok_or_else(|| EvolutionError::NotFound(format!("Run {run_id} is missing")))?
            .precondition_token(),
    );
    let receipt = machine
        .submit(CommandEnvelope {
            command_version: COMMAND_VERSION.to_owned(),
            command_id,
            actor: "actor:live-evolution".to_owned(),
            run_id: run_id.to_owned(),
            expected_precondition,
            command,
        })
        .map_err(|error| EvolutionError::Validation(error.to_string()))?;
    if receipt.status != CommandReceiptStatus::Applied {
        return Err(EvolutionError::Conflict(
            "migration Machine command observed a stale precondition".to_owned(),
        ));
    }
    Ok(())
}

fn replay_migration<S: DurableStore>(
    coordinator: &DurableCoordinator<S>,
    journal_id: &str,
    checkpoint_id: &str,
    request: &MigrationRequest,
) -> EvolutionResult<Option<MigrationReceipt>> {
    let Some(record) = coordinator
        .journal_records(journal_id)
        .map_err(durable_error)?
        .iter()
        .find(|record| record.record_id == checkpoint_id)
    else {
        return Ok(None);
    };
    let checkpoint = decode(record)?;
    let receipt = checkpoint
        .snapshot
        .migrations
        .get(&request.migration_id)
        .cloned()
        .ok_or_else(|| EvolutionError::Conflict("checkpoint is not this migration".to_owned()))?;
    if receipt.run_id != request.run_id
        || receipt.from_plan != request.from_plan
        || receipt.to_plan != request.to_plan
        || receipt.safe_point_id != request.safe_point_id
        || receipt.source_epoch != request.source_epoch
        || receipt.input_state != request.input_state
        || receipt.plan_edge_id != request.plan_edge_id
        || receipt.compatibility_id != request.compatibility_id
        || receipt.target_continuation.run_id != request.run_id
        || receipt.source_binding != request.source_binding
        || receipt.target_binding != request.target_binding
    {
        return Err(EvolutionError::Conflict(
            "migration checkpoint identity was reused with different semantics".to_owned(),
        ));
    }
    Ok(Some(receipt))
}

pub(crate) fn verify_committed_migration<S: DurableStore>(
    coordinator: &DurableCoordinator<S>,
    receipt: &MigrationReceipt,
) -> EvolutionResult<()> {
    let state = coordinator.state().map_err(durable_error)?;
    let continuation = state.continuations.get(&receipt.run_id).ok_or_else(|| {
        EvolutionError::NotFound("committed migration Continuation is missing".to_owned())
    })?;
    let machine = coordinator.restore_machine().map_err(durable_error)?;
    let run = machine
        .projection()
        .runs
        .get(&receipt.run_id)
        .ok_or_else(|| EvolutionError::NotFound("committed migration Run is missing".to_owned()))?;
    if continuation.plan_id != receipt.to_plan
        || continuation.binding_context != receipt.target_binding.artifact_id
        || continuation.state.as_ref() != Some(&receipt.output_state)
        || continuation.epoch != receipt.target_epoch
        || continuation.status != cymule_durable::ContinuationStatus::Running
        || continuation != &receipt.target_continuation
        || run.current_plan != receipt.to_plan
        || run.current_binding_context != receipt.target_binding.artifact_id
        || run.epoch != receipt.target_epoch
        || !run.attempts.values().any(|attempt| {
            attempt.epoch == receipt.target_epoch
                && attempt.active
                && attempt.occurrence_binding == receipt.target_binding.artifact_id
        })
    {
        return Err(EvolutionError::Conflict(
            "committed migration journal and durable Run disagree".to_owned(),
        ));
    }
    Ok(())
}

fn verify_durable_safe_point<S: DurableStore>(
    coordinator: &DurableCoordinator<S>,
    safe_point: &MigrationSafePoint,
) -> EvolutionResult<()> {
    let continuation = coordinator
        .state()
        .map_err(durable_error)?
        .continuations
        .get(&safe_point.run_id)
        .ok_or_else(|| {
            EvolutionError::NotFound(format!(
                "migration Continuation {} is missing",
                safe_point.run_id
            ))
        })?;
    safe_point.verify_continuation(continuation)
}

fn apply_and_checkpoint<S: DurableStore, T>(
    coordinator: &mut DurableCoordinator<S>,
    controller: &mut EvolutionController,
    journal_id: &str,
    checkpoint_id: &str,
    apply: impl FnOnce(&mut EvolutionController) -> EvolutionResult<T>,
) -> EvolutionResult<T> {
    let before = controller.snapshot();
    let result = match apply(controller) {
        Ok(result) => result,
        Err(error) => {
            *controller = EvolutionController::restore(before)
                .expect("previously valid evolution snapshot restores");
            return Err(error);
        }
    };
    if let Err(error) =
        DurableEvolutionController::checkpoint(coordinator, controller, journal_id, checkpoint_id)
    {
        *controller = EvolutionController::restore(before)
            .expect("previously valid evolution snapshot restores");
        return Err(error);
    }
    Ok(result)
}

fn checkpoint_artifacts<S: DurableStore>(
    coordinator: &mut DurableCoordinator<S>,
    controller: &EvolutionController,
    journal_id: &str,
    checkpoint_id: &str,
    artifacts: Vec<ArtifactRecord>,
    required: &BTreeSet<ArtifactRef>,
) -> EvolutionResult<()> {
    let record = checkpoint_record(coordinator, controller, journal_id, checkpoint_id)?;
    let mut machine = coordinator.restore_machine().map_err(durable_error)?;
    for artifact in artifacts {
        let derived = machine
            .put_artifact(artifact.reference.kind.clone(), artifact.bytes)
            .map_err(|error| EvolutionError::Validation(error.to_string()))?;
        if derived != artifact.reference {
            return Err(EvolutionError::Validation(format!(
                "Artifact {} does not match plugin output bytes",
                artifact.reference.artifact_id
            )));
        }
    }
    coordinator
        .checkpoint_artifact_journals(
            &machine,
            required,
            &[JournalBatch {
                journal_id: journal_id.to_owned(),
                records: vec![record],
            }],
        )
        .map(|_| ())
        .map_err(durable_error)
}

fn checkpoint_record<S: DurableStore>(
    coordinator: &DurableCoordinator<S>,
    controller: &EvolutionController,
    journal_id: &str,
    checkpoint_id: &str,
) -> EvolutionResult<JournalRecord> {
    if checkpoint_id.is_empty() {
        return Err(EvolutionError::Validation(
            "evolution checkpoint identity must not be empty".to_owned(),
        ));
    }
    let records = coordinator
        .journal_records(journal_id)
        .map_err(durable_error)?;
    if let Some(existing) = records
        .iter()
        .find(|record| record.record_id == checkpoint_id)
    {
        let checkpoint = decode(existing)?;
        if checkpoint.snapshot == controller.snapshot() {
            return Ok(existing.clone());
        }
        return Err(EvolutionError::Conflict(format!(
            "evolution checkpoint {checkpoint_id} has conflicting state"
        )));
    }
    let parent_checkpoint = records
        .last()
        .map(decode)
        .transpose()?
        .map(|checkpoint| checkpoint.checkpoint_id);
    let checkpoint = EvolutionCheckpoint {
        checkpoint_version: EVOLUTION_CHECKPOINT_SCHEMA.to_owned(),
        checkpoint_id: checkpoint_id.to_owned(),
        parent_checkpoint,
        snapshot: controller.snapshot(),
    };
    JournalRecord::new(
        checkpoint_id,
        EVOLUTION_CHECKPOINT_SCHEMA,
        serde_json::to_value(checkpoint)
            .map_err(|error| EvolutionError::Validation(error.to_string()))?,
    )
    .map_err(durable_error)
}

fn decode(record: &JournalRecord) -> EvolutionResult<EvolutionCheckpoint> {
    record.verify().map_err(durable_error)?;
    if record.schema != EVOLUTION_CHECKPOINT_SCHEMA {
        return Err(EvolutionError::Validation(format!(
            "unexpected evolution checkpoint schema {}",
            record.schema
        )));
    }
    let checkpoint: EvolutionCheckpoint = serde_json::from_value(record.payload.clone())
        .map_err(|error| EvolutionError::Validation(error.to_string()))?;
    if checkpoint.checkpoint_version != EVOLUTION_CHECKPOINT_SCHEMA
        || checkpoint.checkpoint_id != record.record_id
    {
        return Err(EvolutionError::Validation(
            "evolution checkpoint envelope does not match its journal record".to_owned(),
        ));
    }
    Ok(checkpoint)
}

fn durable_error(error: cymule_durable::DurableError) -> EvolutionError {
    match error {
        cymule_durable::DurableError::Contract(error) => EvolutionError::Contract(error),
        cymule_durable::DurableError::Validation(message)
        | cymule_durable::DurableError::Encoding(message) => EvolutionError::Validation(message),
        cymule_durable::DurableError::NotFound(message) => EvolutionError::NotFound(message),
        error @ (cymule_durable::DurableError::Conflict { .. }
        | cymule_durable::DurableError::IllegalTransition(_)
        | cymule_durable::DurableError::Substrate(_)) => {
            EvolutionError::Conflict(error.to_string())
        }
    }
}
