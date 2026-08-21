use cymule_core::Definition;
use cymule_durable::{DurableCoordinator, DurableStore, JournalRecord};
use serde::{Deserialize, Serialize};

use crate::{
    DefinitionRegistry, DefinitionRegistrySnapshot, EvolutionError, EvolutionResult, LinkedPlan,
    PlanTemplate, SubflowRevision,
};

/// Versioned definition-registry checkpoint stored in the generic M1 journal.
pub const DEFINITION_REGISTRY_CHECKPOINT_SCHEMA: &str = "cymule.definition-registry-checkpoint/2";

/// One complete registry checkpoint with explicit journal lineage.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DefinitionRegistryCheckpoint {
    /// Checkpoint schema and semantic version.
    pub checkpoint_version: String,
    /// Stable caller-supplied idempotency identity.
    pub checkpoint_id: String,
    /// Previous checkpoint in this registry journal.
    pub parent_checkpoint: Option<String>,
    /// Complete verified registry state.
    pub snapshot: DefinitionRegistrySnapshot,
}

/// Durable M4 control surface for reusable-definition publication and linking.
pub struct DurableDefinitionRegistry;

impl DurableDefinitionRegistry {
    /// Rebuild a registry from one ordered M1 application journal.
    pub fn load<S: DurableStore>(
        coordinator: &DurableCoordinator<S>,
        journal_id: &str,
    ) -> EvolutionResult<DefinitionRegistry> {
        let records = coordinator
            .journal_records(journal_id)
            .map_err(durable_error)?;
        if records.is_empty() {
            return Ok(DefinitionRegistry::new());
        }
        let mut parent = None;
        let mut registry = None;
        for record in records {
            let checkpoint = decode(record)?;
            if checkpoint.parent_checkpoint != parent {
                return Err(EvolutionError::Validation(format!(
                    "definition registry checkpoint {} has discontinuous lineage",
                    checkpoint.checkpoint_id
                )));
            }
            parent = Some(checkpoint.checkpoint_id);
            registry = Some(DefinitionRegistry::restore(checkpoint.snapshot)?);
        }
        registry.ok_or_else(|| {
            EvolutionError::Validation("definition registry journal did not restore".to_owned())
        })
    }

    /// Persist one complete idempotent registry checkpoint.
    pub fn checkpoint<S: DurableStore>(
        coordinator: &mut DurableCoordinator<S>,
        registry: &DefinitionRegistry,
        journal_id: &str,
        checkpoint_id: &str,
    ) -> EvolutionResult<String> {
        let record = checkpoint_record(coordinator, registry, journal_id, checkpoint_id)?;
        coordinator
            .append_journal_record(journal_id, record)
            .map_err(durable_error)
    }

    /// Publish a revision, relink future callers, and checkpoint atomically.
    pub fn publish_and_relink_and_checkpoint<S: DurableStore>(
        coordinator: &mut DurableCoordinator<S>,
        registry: &mut DefinitionRegistry,
        journal_id: &str,
        checkpoint_id: &str,
        logical_ref: impl Into<String>,
        definition: Definition,
    ) -> EvolutionResult<(SubflowRevision, Vec<LinkedPlan>)> {
        apply_and_checkpoint(
            coordinator,
            registry,
            journal_id,
            checkpoint_id,
            |registry| registry.publish_and_relink(logical_ref, definition),
        )
    }

    /// Publish a reusable module, relink transitive callers, and checkpoint.
    pub fn publish_module_and_relink_and_checkpoint<S: DurableStore>(
        coordinator: &mut DurableCoordinator<S>,
        registry: &mut DefinitionRegistry,
        journal_id: &str,
        checkpoint_id: &str,
        logical_ref: impl Into<String>,
        definition: Definition,
        references: Vec<crate::SubflowReference>,
    ) -> EvolutionResult<(SubflowRevision, Vec<LinkedPlan>)> {
        apply_and_checkpoint(
            coordinator,
            registry,
            journal_id,
            checkpoint_id,
            |registry| registry.publish_module_and_relink(logical_ref, definition, references),
        )
    }

    /// Register and link a parent template, then checkpoint atomically.
    pub fn register_template_and_checkpoint<S: DurableStore>(
        coordinator: &mut DurableCoordinator<S>,
        registry: &mut DefinitionRegistry,
        journal_id: &str,
        checkpoint_id: &str,
        template: PlanTemplate,
    ) -> EvolutionResult<LinkedPlan> {
        apply_and_checkpoint(
            coordinator,
            registry,
            journal_id,
            checkpoint_id,
            |registry| registry.register_template(template),
        )
    }
}

fn apply_and_checkpoint<S: DurableStore, T>(
    coordinator: &mut DurableCoordinator<S>,
    registry: &mut DefinitionRegistry,
    journal_id: &str,
    checkpoint_id: &str,
    apply: impl FnOnce(&mut DefinitionRegistry) -> EvolutionResult<T>,
) -> EvolutionResult<T> {
    let before = registry.snapshot();
    let result = match apply(registry) {
        Ok(result) => result,
        Err(error) => {
            *registry = DefinitionRegistry::restore(before)
                .expect("previously valid definition registry snapshot restores");
            return Err(error);
        }
    };
    if let Err(error) =
        DurableDefinitionRegistry::checkpoint(coordinator, registry, journal_id, checkpoint_id)
    {
        *registry = DefinitionRegistry::restore(before)
            .expect("previously valid definition registry snapshot restores");
        return Err(error);
    }
    Ok(result)
}

fn checkpoint_record<S: DurableStore>(
    coordinator: &DurableCoordinator<S>,
    registry: &DefinitionRegistry,
    journal_id: &str,
    checkpoint_id: &str,
) -> EvolutionResult<JournalRecord> {
    if checkpoint_id.is_empty() {
        return Err(EvolutionError::Validation(
            "definition registry checkpoint identity must not be empty".to_owned(),
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
        if checkpoint.snapshot == registry.snapshot() {
            return Ok(existing.clone());
        }
        return Err(EvolutionError::Conflict(format!(
            "definition registry checkpoint {checkpoint_id} has conflicting state"
        )));
    }
    let parent_checkpoint = records
        .last()
        .map(decode)
        .transpose()?
        .map(|checkpoint| checkpoint.checkpoint_id);
    let checkpoint = DefinitionRegistryCheckpoint {
        checkpoint_version: DEFINITION_REGISTRY_CHECKPOINT_SCHEMA.to_owned(),
        checkpoint_id: checkpoint_id.to_owned(),
        parent_checkpoint,
        snapshot: registry.snapshot(),
    };
    JournalRecord::new(
        checkpoint_id,
        DEFINITION_REGISTRY_CHECKPOINT_SCHEMA,
        serde_json::to_value(checkpoint)
            .map_err(|error| EvolutionError::Validation(error.to_string()))?,
    )
    .map_err(durable_error)
}

fn decode(record: &JournalRecord) -> EvolutionResult<DefinitionRegistryCheckpoint> {
    record.verify().map_err(durable_error)?;
    if record.schema != DEFINITION_REGISTRY_CHECKPOINT_SCHEMA {
        return Err(EvolutionError::Validation(format!(
            "unexpected definition registry checkpoint schema {}",
            record.schema
        )));
    }
    let checkpoint: DefinitionRegistryCheckpoint =
        serde_json::from_value(record.payload.clone())
            .map_err(|error| EvolutionError::Validation(error.to_string()))?;
    if checkpoint.checkpoint_version != DEFINITION_REGISTRY_CHECKPOINT_SCHEMA
        || checkpoint.checkpoint_id != record.record_id
    {
        return Err(EvolutionError::Validation(
            "definition registry checkpoint envelope does not match its journal record".to_owned(),
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
