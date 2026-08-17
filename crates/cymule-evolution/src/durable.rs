use cymule_core::{ArtifactRef, SealedPlan};
use cymule_durable::{DurableCoordinator, DurableStore, JournalRecord};
use serde::{Deserialize, Serialize};

use crate::{
    EvolutionController, EvolutionError, EvolutionResult, EvolutionSnapshot, MigrationReceipt,
    PlanEdge, RolloutDecision, ShadowComparison,
};

/// Versioned M4 checkpoint stored in the generic M1 journal.
pub const EVOLUTION_CHECKPOINT_SCHEMA: &str = "cymule.evolution-checkpoint/1";

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
        safe_point: bool,
    ) -> EvolutionResult<()> {
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
