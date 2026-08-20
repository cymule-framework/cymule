use std::collections::BTreeSet;

use cymule_core::{ArtifactRecord, ArtifactRef, SealedPlan};
use cymule_durable::{DurableCoordinator, DurableStore, JournalBatch, JournalRecord};
use serde::{Deserialize, Serialize};

use crate::{
    EvolutionController, EvolutionError, EvolutionResult, EvolutionSnapshot, MigrationAdapter,
    MigrationReceipt, MigrationRequest, MigrationSafePoint, PlanEdge, PlanPatch, RestartReceipt,
    RestartRequest, RolloutDecision, RolloutGate, RolloutObservation, RolloutTransition,
    ShadowComparison, ShadowDriver, ShadowRequest,
};

/// Versioned M4 checkpoint stored in the generic M1 journal.
pub const EVOLUTION_CHECKPOINT_SCHEMA: &str = "cymule.evolution-checkpoint/3";

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

    /// Record one safe-point migration and checkpoint its evidence.
    pub fn record_migration_and_checkpoint<S: DurableStore>(
        coordinator: &mut DurableCoordinator<S>,
        controller: &mut EvolutionController,
        journal_id: &str,
        checkpoint_id: &str,
        receipt: MigrationReceipt,
        safe_point: &MigrationSafePoint,
    ) -> EvolutionResult<()> {
        verify_durable_safe_point(coordinator, safe_point)?;
        apply_and_checkpoint(
            coordinator,
            controller,
            journal_id,
            checkpoint_id,
            |controller| controller.record_migration(receipt, safe_point),
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
        verify_durable_safe_point(coordinator, safe_point)?;
        let before = controller.snapshot();
        let (receipt, artifacts) =
            controller.execute_migration_with_artifacts(adapter, request, safe_point)?;
        if let Err(error) = checkpoint_artifacts(
            coordinator,
            controller,
            journal_id,
            checkpoint_id,
            artifacts,
            &BTreeSet::from([receipt.output_state.clone(), receipt.evidence.clone()]),
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
    let result = apply(controller)?;
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
        let derived = machine.put_artifact(artifact.reference.kind.clone(), artifact.bytes);
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
