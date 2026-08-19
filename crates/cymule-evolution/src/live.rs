use std::collections::{BTreeMap, BTreeSet};

use cymule_core::{ArtifactRecord, ArtifactRef, Definition, Machine, content_id};
use cymule_durable::{DurableCoordinator, DurableStore, JournalBatch, JournalRecord};
use serde::{Deserialize, Serialize};

use crate::{
    DefinitionRegistry, DefinitionRegistrySnapshot, EvolutionCommand, EvolutionController,
    EvolutionError, EvolutionResult, EvolutionSnapshot, LinkedPlan, LiveEvolutionCommand,
    LiveEvolutionResponse, MigrationAdapter, MigrationReceipt, MigrationRequest,
    MigrationSafePoint, PlanEdge, PlanPatch, PlanTemplate, RestartReceipt, RestartRequest,
    RolloutDecision, RolloutGate, RolloutMode, RolloutObservation, RolloutTransition,
    ShadowComparison, ShadowDriver, ShadowRequest, SubflowRevision,
};

/// Complete provider-neutral live-evolution state version.
pub const LIVE_EVOLUTION_VERSION: &str = "cymule.live-evolution/1";
/// One-journal durable checkpoint for the complete live-evolution authority.
pub const LIVE_EVOLUTION_CHECKPOINT_SCHEMA: &str = "cymule.live-evolution-checkpoint/1";

/// Complete portable authority for definitions, linked Plans, rollout, and pins.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LiveEvolutionSnapshot {
    /// Snapshot schema and semantic version.
    pub live_version: String,
    /// Reusable definitions, reverse dependencies, and exact link history.
    pub registry: DefinitionRegistrySnapshot,
    /// Per-template Plan DAG, future decisions, evidence, and occurrence pins.
    pub templates: BTreeMap<String, EvolutionSnapshot>,
}

/// One template affected by a reusable-definition publication.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LiveTemplateUpdate {
    /// Stable parent template identity.
    pub template_id: String,
    /// Future Plan before the publication.
    pub previous_plan_id: String,
    /// Future candidate Plan after compatibility admission.
    pub current_plan_id: String,
    /// New rollout decision when the future Plan advanced.
    pub decision_id: Option<String>,
    /// Whether a new immutable parent Plan became eligible for future work.
    pub advanced: bool,
}

/// Result of publishing one immutable reusable-definition revision.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LivePublicationReceipt {
    /// Published immutable revision, including incompatible retained revisions.
    pub revision: SubflowRevision,
    /// Every transitively affected registered parent in stable template order.
    pub updates: Vec<LiveTemplateUpdate>,
}

/// Exact idempotent request retained with a durable publication receipt.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LivePublicationCommand {
    /// Reusable definition reference being updated.
    pub logical_ref: String,
    /// Immutable reusable definition content.
    pub definition: Definition,
    /// Review or compiler evidence retained by every resulting DAG edge.
    pub evidence: ArtifactRef,
    /// Future-selection policy for newly linked Plans.
    pub mode: RolloutMode,
}

/// Exact publication command and original durable receipt.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LivePublicationRecord {
    /// Admitted command semantics.
    pub command: LivePublicationCommand,
    /// Original result returned on every identical retry.
    pub receipt: LivePublicationReceipt,
}

/// Template-scoped migration request with an exact durable safe-point proof.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LiveMigrationCommand {
    /// Registered parent template owning the Plan DAG.
    pub template_id: String,
    /// Checked migration request.
    pub request: MigrationRequest,
    /// Current content-addressed safe-point proof.
    pub safe_point: MigrationSafePoint,
}

/// One atomic live-version selection plus fenced virtual worker claim.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LiveVirtualClaimCommand {
    /// Registered parent template selecting a Plan.
    pub template_id: String,
    /// Stable identity for the exact future-work selection.
    pub selection_id: String,
    /// Stable worker claim command identity.
    pub command_id: String,
    /// Worker identity.
    pub owner: String,
    /// Capacity-slot resource.
    pub slot_id: String,
    /// Explicit worker capabilities.
    pub capabilities: BTreeSet<String>,
    /// Logical time supplied by the Clock substrate.
    pub logical_now: u64,
    /// Positive logical lease duration.
    pub lease_ttl: u64,
}

/// Exact Plan selection and virtual claim committed in one CAS revision.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LiveVirtualClaimReceipt {
    /// Exact Plan pinned before worker dispatch.
    pub plan_id: String,
    /// Fenced virtual-work claim receipt.
    pub claim: cymule_virtual::VirtualClaimReceipt,
}

/// One complete provider-neutral live-evolution authority.
#[derive(Default)]
pub struct LiveEvolutionController {
    registry: DefinitionRegistry,
    templates: BTreeMap<String, EvolutionController>,
}

impl LiveEvolutionController {
    /// Create an empty authority.
    pub fn new() -> Self {
        Self::default()
    }

    /// Export one complete portable snapshot.
    pub fn snapshot(&self) -> LiveEvolutionSnapshot {
        LiveEvolutionSnapshot {
            live_version: LIVE_EVOLUTION_VERSION.to_owned(),
            registry: self.registry.snapshot(),
            templates: self
                .templates
                .iter()
                .map(|(template_id, controller)| (template_id.clone(), controller.snapshot()))
                .collect(),
        }
    }

    /// Restore and verify the registry and every per-template evolution reducer.
    pub fn restore(snapshot: LiveEvolutionSnapshot) -> EvolutionResult<Self> {
        if snapshot.live_version != LIVE_EVOLUTION_VERSION {
            return Err(EvolutionError::Validation(format!(
                "unsupported live-evolution version {}",
                snapshot.live_version
            )));
        }
        let registry = DefinitionRegistry::restore(snapshot.registry)?;
        let registry_snapshot = registry.snapshot();
        if snapshot.templates.len() != registry_snapshot.templates.len() {
            return Err(EvolutionError::Validation(
                "live evolution requires one controller per registered template".to_owned(),
            ));
        }
        let mut templates = BTreeMap::new();
        for (template_id, template) in snapshot.templates {
            if !registry_snapshot.templates.contains_key(&template_id) {
                return Err(EvolutionError::Validation(format!(
                    "live evolution controller {template_id} has no registered template"
                )));
            }
            let controller = EvolutionController::restore(template)?;
            let current = registry_snapshot
                .current_links
                .get(&template_id)
                .ok_or_else(|| {
                    EvolutionError::Validation(format!(
                        "registered template {template_id} has no current link"
                    ))
                })?;
            if controller.plan(&current.plan.plan_id).is_none() {
                return Err(EvolutionError::Validation(format!(
                    "live evolution controller {template_id} is missing its current linked Plan"
                )));
            }
            templates.insert(template_id, controller);
        }
        Ok(Self {
            registry,
            templates,
        })
    }

    /// Publish a reusable definition before any parent template is registered.
    pub fn publish(
        &mut self,
        logical_ref: impl Into<String>,
        definition: Definition,
    ) -> EvolutionResult<SubflowRevision> {
        self.registry.publish(logical_ref, definition)
    }

    /// Register one parent and establish its initial immutable future decision.
    pub fn register_template(&mut self, template: PlanTemplate) -> EvolutionResult<LinkedPlan> {
        let template_id = template.template_id.clone();
        if self.templates.contains_key(&template_id) {
            let current = self.registry.current_link(&template_id).ok_or_else(|| {
                EvolutionError::Validation(format!(
                    "live template {template_id} lost its current link"
                ))
            })?;
            return Ok(current.clone());
        }
        let linked = self.registry.register_template(template)?;
        let mut controller = EvolutionController::new();
        controller.register_plan(linked.plan.clone())?;
        let decision = initial_decision(&template_id, &linked.plan.plan_id)?;
        controller.set_rollout(decision)?;
        self.templates.insert(template_id, controller);
        Ok(linked)
    }

    /// Publish one revision and atomically advance every compatible dependent.
    pub fn publish_and_relink(
        &mut self,
        command: LivePublicationCommand,
    ) -> EvolutionResult<LivePublicationReceipt> {
        let before = self.snapshot();
        match self.publish_and_relink_inner(command) {
            Ok(receipt) => Ok(receipt),
            Err(error) => {
                *self = Self::restore(before)
                    .expect("previously valid live-evolution snapshot restores");
                Err(error)
            }
        }
    }

    fn publish_and_relink_inner(
        &mut self,
        command: LivePublicationCommand,
    ) -> EvolutionResult<LivePublicationReceipt> {
        let LivePublicationCommand {
            logical_ref,
            definition,
            evidence,
            mode,
        } = command;
        let previous = self.registry.snapshot().current_links;
        let (revision, linked) = self.registry.publish_and_relink(logical_ref, definition)?;
        let mut updates = Vec::with_capacity(linked.len());
        for current in linked {
            let prior = previous.get(&current.template_id).ok_or_else(|| {
                EvolutionError::Validation(format!(
                    "affected template {} had no previous link",
                    current.template_id
                ))
            })?;
            let advanced = prior.plan.plan_id != current.plan.plan_id;
            let decision_id = if advanced {
                let controller = self
                    .templates
                    .get_mut(&current.template_id)
                    .ok_or_else(|| {
                        EvolutionError::Validation(format!(
                            "affected template {} has no evolution controller",
                            current.template_id
                        ))
                    })?;
                controller.add_diff_edge(&prior.plan.plan_id, &current.plan, evidence.clone())?;
                let decision = update_decision(
                    &current.template_id,
                    &prior.plan.plan_id,
                    &current.plan.plan_id,
                    mode.clone(),
                )?;
                let decision_id = decision.decision_id.clone();
                controller.set_rollout(decision)?;
                Some(decision_id)
            } else {
                None
            };
            updates.push(LiveTemplateUpdate {
                template_id: current.template_id,
                previous_plan_id: prior.plan.plan_id.clone(),
                current_plan_id: current.plan.plan_id,
                decision_id,
                advanced,
            });
        }
        Ok(LivePublicationReceipt { revision, updates })
    }

    /// Select and permanently pin one occurrence under a registered template.
    pub fn select_occurrence(
        &mut self,
        template_id: &str,
        occurrence_id: &str,
    ) -> EvolutionResult<String> {
        self.templates
            .get_mut(template_id)
            .ok_or_else(|| {
                EvolutionError::NotFound(format!(
                    "live evolution template {template_id} is missing"
                ))
            })?
            .select_for_occurrence(occurrence_id)
    }

    /// Admit one exact reviewed patch under a registered parent template.
    pub fn apply_patch(
        &mut self,
        template_id: &str,
        patch: PlanPatch,
    ) -> EvolutionResult<PlanEdge> {
        self.template_mut(template_id)?.apply_patch(patch)
    }

    /// Change future selection for one registered parent template.
    pub fn set_rollout(
        &mut self,
        template_id: &str,
        decision: RolloutDecision,
    ) -> EvolutionResult<()> {
        self.template_mut(template_id)?.set_rollout(decision)
    }

    /// Execute one checked safe-point migration for a registered parent.
    pub fn execute_migration<A: MigrationAdapter>(
        &mut self,
        template_id: &str,
        adapter: &mut A,
        request: MigrationRequest,
        safe_point: &MigrationSafePoint,
    ) -> EvolutionResult<MigrationReceipt> {
        self.template_mut(template_id)?
            .execute_migration(adapter, request, safe_point)
    }

    fn execute_migration_with_artifacts<A: MigrationAdapter>(
        &mut self,
        template_id: &str,
        adapter: &mut A,
        request: MigrationRequest,
        safe_point: &MigrationSafePoint,
    ) -> EvolutionResult<(MigrationReceipt, Vec<ArtifactRecord>)> {
        self.template_mut(template_id)?
            .execute_migration_with_artifacts(adapter, request, safe_point)
    }

    /// Authorize one distinct replacement Run under an exact Plan.
    pub fn restart_under_new_plan(
        &mut self,
        template_id: &str,
        request: RestartRequest,
        safe_point: &MigrationSafePoint,
    ) -> EvolutionResult<RestartReceipt> {
        self.template_mut(template_id)?
            .restart_under_new_plan(request, safe_point)
    }

    /// Execute one isolated shadow comparison for a registered parent.
    pub fn execute_shadow<D: ShadowDriver>(
        &mut self,
        template_id: &str,
        driver: &mut D,
        request: ShadowRequest,
    ) -> EvolutionResult<ShadowComparison> {
        self.template_mut(template_id)?
            .execute_shadow(driver, request)
    }

    fn execute_shadow_with_artifacts<D: ShadowDriver>(
        &mut self,
        template_id: &str,
        driver: &mut D,
        request: ShadowRequest,
    ) -> EvolutionResult<(ShadowComparison, Vec<ArtifactRecord>)> {
        self.template_mut(template_id)?
            .execute_shadow_with_artifacts(driver, request)
    }

    /// Record one terminal rollout observation under an occurrence pin.
    pub fn record_observation(
        &mut self,
        template_id: &str,
        observation: RolloutObservation,
    ) -> EvolutionResult<()> {
        self.template_mut(template_id)?
            .record_observation(observation)
    }

    /// Apply one deterministic promotion or rollback gate.
    pub fn apply_gate(
        &mut self,
        template_id: &str,
        gate: RolloutGate,
        next_decision_id: impl Into<String>,
    ) -> EvolutionResult<RolloutTransition> {
        self.template_mut(template_id)?
            .apply_gate(gate, next_decision_id)
    }

    /// Current exact immutable link for one registered parent.
    pub fn current_link(&self, template_id: &str) -> Option<&LinkedPlan> {
        self.registry.current_link(template_id)
    }

    /// Historical exact link by Plan identity.
    pub fn historical_link(&self, plan_id: &str) -> Option<&LinkedPlan> {
        self.registry.historical_link(plan_id)
    }

    /// Historical exact link under one parent template and Plan identity.
    pub fn historical_link_for(&self, template_id: &str, plan_id: &str) -> Option<&LinkedPlan> {
        self.registry.historical_link_for(template_id, plan_id)
    }

    fn template_mut(&mut self, template_id: &str) -> EvolutionResult<&mut EvolutionController> {
        self.templates.get_mut(template_id).ok_or_else(|| {
            EvolutionError::NotFound(format!("live evolution template {template_id} is missing"))
        })
    }
}

/// One complete live-evolution checkpoint with explicit lineage.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LiveEvolutionCheckpoint {
    /// Checkpoint schema and semantic version.
    pub checkpoint_version: String,
    /// Stable idempotency identity.
    pub checkpoint_id: String,
    /// Previous checkpoint in the same journal.
    pub parent_checkpoint: Option<String>,
    /// Complete live-evolution authority.
    pub snapshot: LiveEvolutionSnapshot,
    /// Publication command/receipt when this checkpoint admitted an update.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub publication: Option<LivePublicationRecord>,
}

/// Durable single-journal integration for complete live evolution.
pub struct DurableLiveEvolutionController;

impl DurableLiveEvolutionController {
    /// Restore one complete controller from its ordered durable journal.
    pub fn load<S: DurableStore>(
        coordinator: &DurableCoordinator<S>,
        journal_id: &str,
    ) -> EvolutionResult<LiveEvolutionController> {
        let records = coordinator
            .journal_records(journal_id)
            .map_err(durable_error)?;
        if records.is_empty() {
            return Ok(LiveEvolutionController::new());
        }
        let mut parent = None;
        let mut controller = None;
        for record in records {
            let checkpoint = decode(record)?;
            if checkpoint.parent_checkpoint != parent {
                return Err(EvolutionError::Validation(format!(
                    "live-evolution checkpoint {} has discontinuous lineage",
                    checkpoint.checkpoint_id
                )));
            }
            parent = Some(checkpoint.checkpoint_id);
            controller = Some(LiveEvolutionController::restore(checkpoint.snapshot)?);
        }
        controller.ok_or_else(|| {
            EvolutionError::Validation("live-evolution journal did not restore".to_owned())
        })
    }

    /// Persist the complete controller under one idempotent checkpoint.
    pub fn checkpoint<S: DurableStore>(
        coordinator: &mut DurableCoordinator<S>,
        controller: &LiveEvolutionController,
        journal_id: &str,
        checkpoint_id: &str,
    ) -> EvolutionResult<String> {
        let record = checkpoint_record(coordinator, controller, journal_id, checkpoint_id, None)?;
        coordinator
            .append_journal_record(journal_id, record)
            .map_err(durable_error)
    }

    /// Publish an initial definition and checkpoint it before templates use it.
    pub fn publish_and_checkpoint<S: DurableStore>(
        coordinator: &mut DurableCoordinator<S>,
        controller: &mut LiveEvolutionController,
        journal_id: &str,
        checkpoint_id: &str,
        logical_ref: impl Into<String>,
        definition: Definition,
    ) -> EvolutionResult<SubflowRevision> {
        apply_and_checkpoint(
            coordinator,
            controller,
            journal_id,
            checkpoint_id,
            |controller| controller.publish(logical_ref, definition),
        )
    }

    /// Publish a definition and checkpoint every relink and rollout atomically.
    pub fn publish_and_relink_and_checkpoint<S: DurableStore>(
        coordinator: &mut DurableCoordinator<S>,
        controller: &mut LiveEvolutionController,
        machine: &Machine,
        journal_id: &str,
        checkpoint_id: &str,
        command: LivePublicationCommand,
    ) -> EvolutionResult<LivePublicationReceipt> {
        if let Some(replayed) =
            replay_publication(coordinator, journal_id, checkpoint_id, &command)?
        {
            return Ok(replayed);
        }
        let before = controller.snapshot();
        let receipt = controller.publish_and_relink(command.clone())?;
        let publication = LivePublicationRecord {
            command: command.clone(),
            receipt: receipt.clone(),
        };
        let record = match checkpoint_record(
            coordinator,
            controller,
            journal_id,
            checkpoint_id,
            Some(publication),
        ) {
            Ok(record) => record,
            Err(error) => {
                *controller = LiveEvolutionController::restore(before)
                    .expect("previously valid live-evolution snapshot restores");
                return Err(error);
            }
        };
        if let Err(error) = coordinator.checkpoint_artifact_journals(
            machine,
            &BTreeSet::from([command.evidence]),
            &[JournalBatch {
                journal_id: journal_id.to_owned(),
                records: vec![record],
            }],
        ) {
            *controller = LiveEvolutionController::restore(before)
                .expect("previously valid live-evolution snapshot restores");
            return Err(durable_error(error));
        }
        Ok(receipt)
    }

    /// Register one initial parent and checkpoint its future decision.
    pub fn register_template_and_checkpoint<S: DurableStore>(
        coordinator: &mut DurableCoordinator<S>,
        controller: &mut LiveEvolutionController,
        journal_id: &str,
        checkpoint_id: &str,
        template: PlanTemplate,
    ) -> EvolutionResult<LinkedPlan> {
        apply_and_checkpoint(
            coordinator,
            controller,
            journal_id,
            checkpoint_id,
            |controller| controller.register_template(template),
        )
    }

    /// Select and checkpoint one exact occurrence Plan before dispatch.
    pub fn select_occurrence_and_checkpoint<S: DurableStore>(
        coordinator: &mut DurableCoordinator<S>,
        controller: &mut LiveEvolutionController,
        journal_id: &str,
        checkpoint_id: &str,
        template_id: &str,
        occurrence_id: &str,
    ) -> EvolutionResult<String> {
        apply_and_checkpoint(
            coordinator,
            controller,
            journal_id,
            checkpoint_id,
            |controller| controller.select_occurrence(template_id, occurrence_id),
        )
    }

    /// Apply one exact reviewed patch and checkpoint the unified authority.
    pub fn apply_patch_and_checkpoint<S: DurableStore>(
        coordinator: &mut DurableCoordinator<S>,
        controller: &mut LiveEvolutionController,
        journal_id: &str,
        checkpoint_id: &str,
        template_id: &str,
        patch: PlanPatch,
    ) -> EvolutionResult<PlanEdge> {
        let evidence = patch.evidence.clone();
        apply_and_checkpoint_existing_artifacts(
            coordinator,
            controller,
            journal_id,
            checkpoint_id,
            |controller| controller.apply_patch(template_id, patch),
            &BTreeSet::from([evidence]),
        )
    }

    /// Change one template's future rollout and checkpoint it atomically.
    pub fn set_rollout_and_checkpoint<S: DurableStore>(
        coordinator: &mut DurableCoordinator<S>,
        controller: &mut LiveEvolutionController,
        journal_id: &str,
        checkpoint_id: &str,
        template_id: &str,
        decision: RolloutDecision,
    ) -> EvolutionResult<()> {
        apply_and_checkpoint(
            coordinator,
            controller,
            journal_id,
            checkpoint_id,
            |controller| controller.set_rollout(template_id, decision),
        )
    }

    /// Execute a checked migration under one template and durable safe point.
    pub fn execute_migration_and_checkpoint<S: DurableStore, A: MigrationAdapter>(
        coordinator: &mut DurableCoordinator<S>,
        controller: &mut LiveEvolutionController,
        journal_id: &str,
        checkpoint_id: &str,
        adapter: &mut A,
        command: LiveMigrationCommand,
    ) -> EvolutionResult<MigrationReceipt> {
        verify_durable_safe_point(coordinator, &command.safe_point)?;
        let before = controller.snapshot();
        let (receipt, artifacts) = controller.execute_migration_with_artifacts(
            &command.template_id,
            adapter,
            command.request,
            &command.safe_point,
        )?;
        let required = BTreeSet::from([receipt.output_state.clone(), receipt.evidence.clone()]);
        if let Err(error) = checkpoint_artifacts(
            coordinator,
            controller,
            journal_id,
            checkpoint_id,
            artifacts,
            &required,
        ) {
            *controller = LiveEvolutionController::restore(before)
                .expect("previously valid live-evolution snapshot restores");
            return Err(error);
        }
        Ok(receipt)
    }

    /// Authorize a replacement Run and checkpoint it under one template.
    pub fn restart_under_new_plan_and_checkpoint<S: DurableStore>(
        coordinator: &mut DurableCoordinator<S>,
        controller: &mut LiveEvolutionController,
        journal_id: &str,
        checkpoint_id: &str,
        template_id: &str,
        request: RestartRequest,
        safe_point: &MigrationSafePoint,
    ) -> EvolutionResult<RestartReceipt> {
        verify_durable_safe_point(coordinator, safe_point)?;
        let required = BTreeSet::from([request.input.clone(), request.evidence.clone()]);
        apply_and_checkpoint_existing_artifacts(
            coordinator,
            controller,
            journal_id,
            checkpoint_id,
            |controller| controller.restart_under_new_plan(template_id, request, safe_point),
            &required,
        )
    }

    /// Execute isolated shadow work and checkpoint its comparison evidence.
    pub fn execute_shadow_and_checkpoint<S: DurableStore, D: ShadowDriver>(
        coordinator: &mut DurableCoordinator<S>,
        controller: &mut LiveEvolutionController,
        journal_id: &str,
        checkpoint_id: &str,
        template_id: &str,
        driver: &mut D,
        request: ShadowRequest,
    ) -> EvolutionResult<ShadowComparison> {
        let before = controller.snapshot();
        let (comparison, artifacts) =
            controller.execute_shadow_with_artifacts(template_id, driver, request)?;
        if let Err(error) = checkpoint_artifacts(
            coordinator,
            controller,
            journal_id,
            checkpoint_id,
            artifacts,
            &BTreeSet::from([comparison.evidence.clone()]),
        ) {
            *controller = LiveEvolutionController::restore(before)
                .expect("previously valid live-evolution snapshot restores");
            return Err(error);
        }
        Ok(comparison)
    }

    /// Record one rollout observation and checkpoint before gate evaluation.
    pub fn record_observation_and_checkpoint<S: DurableStore>(
        coordinator: &mut DurableCoordinator<S>,
        controller: &mut LiveEvolutionController,
        journal_id: &str,
        checkpoint_id: &str,
        template_id: &str,
        observation: RolloutObservation,
    ) -> EvolutionResult<()> {
        let evidence = observation.evidence.clone();
        apply_and_checkpoint_existing_artifacts(
            coordinator,
            controller,
            journal_id,
            checkpoint_id,
            |controller| controller.record_observation(template_id, observation),
            &BTreeSet::from([evidence]),
        )
    }

    /// Apply one deterministic gate and checkpoint the new future decision.
    pub fn apply_gate_and_checkpoint<S: DurableStore>(
        coordinator: &mut DurableCoordinator<S>,
        controller: &mut LiveEvolutionController,
        journal_id: &str,
        checkpoint_id: &str,
        template_id: &str,
        gate: RolloutGate,
        next_decision_id: impl Into<String>,
    ) -> EvolutionResult<RolloutTransition> {
        apply_and_checkpoint(
            coordinator,
            controller,
            journal_id,
            checkpoint_id,
            |controller| controller.apply_gate(template_id, gate, next_decision_id),
        )
    }

    /// Execute one closed unified command against the durable authority.
    pub fn submit<S, A, D>(
        coordinator: &mut DurableCoordinator<S>,
        controller: &mut LiveEvolutionController,
        journal_id: &str,
        command: LiveEvolutionCommand,
        migration_adapter: &mut A,
        shadow_driver: &mut D,
    ) -> EvolutionResult<LiveEvolutionResponse>
    where
        S: DurableStore,
        A: MigrationAdapter,
        D: ShadowDriver,
    {
        command.verify()?;
        match command {
            LiveEvolutionCommand::PublishDefinition {
                command_id,
                logical_ref,
                definition,
                ..
            } => Self::publish_and_checkpoint(
                coordinator,
                controller,
                journal_id,
                &command_id,
                logical_ref,
                definition,
            )
            .map(|revision| LiveEvolutionResponse::DefinitionPublished { revision }),
            LiveEvolutionCommand::RegisterTemplate {
                command_id,
                template,
                ..
            } => Self::register_template_and_checkpoint(
                coordinator,
                controller,
                journal_id,
                &command_id,
                template,
            )
            .map(|linked| LiveEvolutionResponse::TemplateRegistered { linked }),
            LiveEvolutionCommand::PublishAndRelink {
                command_id,
                publication,
                ..
            } => {
                let machine = coordinator.restore_machine().map_err(durable_error)?;
                Self::publish_and_relink_and_checkpoint(
                    coordinator,
                    controller,
                    &machine,
                    journal_id,
                    &command_id,
                    publication,
                )
                .map(|receipt| LiveEvolutionResponse::PublicationApplied { receipt })
            }
            LiveEvolutionCommand::Apply {
                command_id,
                template_id,
                command,
                safe_point,
                ..
            } => match *command {
                EvolutionCommand::ApplyPatch { patch, .. } => Self::apply_patch_and_checkpoint(
                    coordinator,
                    controller,
                    journal_id,
                    &command_id,
                    &template_id,
                    patch,
                )
                .map(|edge| LiveEvolutionResponse::PatchApplied { edge }),
                EvolutionCommand::SetRollout { decision, .. } => {
                    Self::set_rollout_and_checkpoint(
                        coordinator,
                        controller,
                        journal_id,
                        &command_id,
                        &template_id,
                        decision,
                    )?;
                    Ok(LiveEvolutionResponse::Applied)
                }
                EvolutionCommand::SelectOccurrence { occurrence_id, .. } => {
                    Self::select_occurrence_and_checkpoint(
                        coordinator,
                        controller,
                        journal_id,
                        &command_id,
                        &template_id,
                        &occurrence_id,
                    )
                    .map(|plan_id| LiveEvolutionResponse::OccurrenceSelected { plan_id })
                }
                EvolutionCommand::Migrate { request, .. } => {
                    let safe_point = safe_point.ok_or_else(|| {
                        EvolutionError::Validation(
                            "verified migration command lost its safe-point proof".to_owned(),
                        )
                    })?;
                    Self::execute_migration_and_checkpoint(
                        coordinator,
                        controller,
                        journal_id,
                        &command_id,
                        migration_adapter,
                        LiveMigrationCommand {
                            template_id,
                            request,
                            safe_point,
                        },
                    )
                    .map(|receipt| LiveEvolutionResponse::Migrated { receipt })
                }
                EvolutionCommand::RestartUnderNewPlan { request, .. } => {
                    let safe_point = safe_point.ok_or_else(|| {
                        EvolutionError::Validation(
                            "verified restart command lost its safe-point proof".to_owned(),
                        )
                    })?;
                    Self::restart_under_new_plan_and_checkpoint(
                        coordinator,
                        controller,
                        journal_id,
                        &command_id,
                        &template_id,
                        request,
                        &safe_point,
                    )
                    .map(|receipt| LiveEvolutionResponse::RestartAuthorized { receipt })
                }
                EvolutionCommand::Shadow { request, .. } => Self::execute_shadow_and_checkpoint(
                    coordinator,
                    controller,
                    journal_id,
                    &command_id,
                    &template_id,
                    shadow_driver,
                    request,
                )
                .map(|comparison| LiveEvolutionResponse::ShadowRecorded { comparison }),
                EvolutionCommand::Observe { observation, .. } => {
                    Self::record_observation_and_checkpoint(
                        coordinator,
                        controller,
                        journal_id,
                        &command_id,
                        &template_id,
                        observation,
                    )?;
                    Ok(LiveEvolutionResponse::Applied)
                }
                EvolutionCommand::ApplyGate {
                    gate,
                    next_decision_id,
                    ..
                } => Self::apply_gate_and_checkpoint(
                    coordinator,
                    controller,
                    journal_id,
                    &command_id,
                    &template_id,
                    gate,
                    next_decision_id,
                )
                .map(|transition| LiveEvolutionResponse::GateApplied { transition }),
            },
        }
    }

    /// Atomically pin a future Plan and claim virtual work under one lease CAS.
    pub fn claim_virtual_work_and_checkpoint<S: DurableStore>(
        coordinator: &mut DurableCoordinator<S>,
        controller: &mut LiveEvolutionController,
        scheduler: &mut cymule_virtual::VirtualScheduler,
        live_journal_id: &str,
        virtual_journal_id: &str,
        command: &LiveVirtualClaimCommand,
    ) -> EvolutionResult<LiveVirtualClaimReceipt> {
        let live_before = controller.snapshot();
        let scheduler_before = scheduler.clone();
        let plan_id = controller.select_occurrence(&command.template_id, &command.selection_id)?;
        let live_record = match checkpoint_record(
            coordinator,
            controller,
            live_journal_id,
            &command.selection_id,
            None,
        ) {
            Ok(record) => record,
            Err(error) => {
                *controller = LiveEvolutionController::restore(live_before)
                    .expect("previously valid live-evolution snapshot restores");
                return Err(error);
            }
        };
        let claim_command = cymule_virtual::VirtualClaimCommand {
            control_version: cymule_virtual::VIRTUAL_CLAIM_CONTROL_VERSION.to_owned(),
            command_id: command.command_id.clone(),
            owner: command.owner.clone(),
            slot_id: command.slot_id.clone(),
            occurrence_binding: plan_id.clone(),
            capabilities: command.capabilities.clone(),
            logical_now: command.logical_now,
            lease_ttl: command.lease_ttl,
        };
        let result =
            cymule_virtual::DurableVirtualController::claim_command_and_checkpoint_with_journals(
                coordinator,
                scheduler,
                &claim_command,
                virtual_journal_id,
                &[JournalBatch {
                    journal_id: live_journal_id.to_owned(),
                    records: vec![live_record],
                }],
            );
        match result {
            Ok(claim) => Ok(LiveVirtualClaimReceipt { plan_id, claim }),
            Err(error) => {
                *controller = LiveEvolutionController::restore(live_before)
                    .expect("previously valid live-evolution snapshot restores");
                *scheduler = scheduler_before;
                Err(virtual_error(error))
            }
        }
    }
}

fn initial_decision(template_id: &str, plan_id: &str) -> EvolutionResult<RolloutDecision> {
    let decision_id = content_id("cymule.live-initial-decision/1", &(template_id, plan_id))?;
    Ok(RolloutDecision {
        decision_id,
        fallback_plan: plan_id.to_owned(),
        target_plan: plan_id.to_owned(),
        mode: RolloutMode::Active,
    })
}

fn update_decision(
    template_id: &str,
    previous_plan: &str,
    current_plan: &str,
    mode: RolloutMode,
) -> EvolutionResult<RolloutDecision> {
    let decision_id = content_id(
        "cymule.live-update-decision/1",
        &(template_id, previous_plan, current_plan, &mode),
    )?;
    Ok(RolloutDecision {
        decision_id,
        fallback_plan: previous_plan.to_owned(),
        target_plan: current_plan.to_owned(),
        mode,
    })
}

fn apply_and_checkpoint<S: DurableStore, T>(
    coordinator: &mut DurableCoordinator<S>,
    controller: &mut LiveEvolutionController,
    journal_id: &str,
    checkpoint_id: &str,
    apply: impl FnOnce(&mut LiveEvolutionController) -> EvolutionResult<T>,
) -> EvolutionResult<T> {
    let before = controller.snapshot();
    let result = apply(controller)?;
    if let Err(error) = DurableLiveEvolutionController::checkpoint(
        coordinator,
        controller,
        journal_id,
        checkpoint_id,
    ) {
        *controller = LiveEvolutionController::restore(before)
            .expect("previously valid live-evolution snapshot restores");
        return Err(error);
    }
    Ok(result)
}

fn apply_and_checkpoint_existing_artifacts<S: DurableStore, T>(
    coordinator: &mut DurableCoordinator<S>,
    controller: &mut LiveEvolutionController,
    journal_id: &str,
    checkpoint_id: &str,
    apply: impl FnOnce(&mut LiveEvolutionController) -> EvolutionResult<T>,
    required: &BTreeSet<ArtifactRef>,
) -> EvolutionResult<T> {
    let before = controller.snapshot();
    let result = apply(controller)?;
    if let Err(error) = checkpoint_artifacts(
        coordinator,
        controller,
        journal_id,
        checkpoint_id,
        Vec::new(),
        required,
    ) {
        *controller = LiveEvolutionController::restore(before)
            .expect("previously valid live-evolution snapshot restores");
        return Err(error);
    }
    Ok(result)
}

fn checkpoint_artifacts<S: DurableStore>(
    coordinator: &mut DurableCoordinator<S>,
    controller: &LiveEvolutionController,
    journal_id: &str,
    checkpoint_id: &str,
    artifacts: Vec<ArtifactRecord>,
    required: &BTreeSet<ArtifactRef>,
) -> EvolutionResult<()> {
    let record = checkpoint_record(coordinator, controller, journal_id, checkpoint_id, None)?;
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
    controller: &LiveEvolutionController,
    journal_id: &str,
    checkpoint_id: &str,
    publication: Option<LivePublicationRecord>,
) -> EvolutionResult<JournalRecord> {
    if checkpoint_id.is_empty() {
        return Err(EvolutionError::Validation(
            "live-evolution checkpoint identity must not be empty".to_owned(),
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
        if checkpoint.snapshot == controller.snapshot() && checkpoint.publication == publication {
            return Ok(existing.clone());
        }
        return Err(EvolutionError::Conflict(format!(
            "live-evolution checkpoint {checkpoint_id} has conflicting state"
        )));
    }
    let parent_checkpoint = records
        .last()
        .map(decode)
        .transpose()?
        .map(|checkpoint| checkpoint.checkpoint_id);
    let checkpoint = LiveEvolutionCheckpoint {
        checkpoint_version: LIVE_EVOLUTION_CHECKPOINT_SCHEMA.to_owned(),
        checkpoint_id: checkpoint_id.to_owned(),
        parent_checkpoint,
        snapshot: controller.snapshot(),
        publication,
    };
    JournalRecord::new(
        checkpoint_id,
        LIVE_EVOLUTION_CHECKPOINT_SCHEMA,
        serde_json::to_value(checkpoint)
            .map_err(|error| EvolutionError::Validation(error.to_string()))?,
    )
    .map_err(durable_error)
}

fn replay_publication<S: DurableStore>(
    coordinator: &DurableCoordinator<S>,
    journal_id: &str,
    checkpoint_id: &str,
    command: &LivePublicationCommand,
) -> EvolutionResult<Option<LivePublicationReceipt>> {
    let records = coordinator
        .journal_records(journal_id)
        .map_err(durable_error)?;
    let Some(record) = records
        .iter()
        .find(|record| record.record_id == checkpoint_id)
    else {
        return Ok(None);
    };
    let checkpoint = decode(record)?;
    let publication = checkpoint.publication.ok_or_else(|| {
        EvolutionError::Conflict(format!(
            "live-evolution checkpoint {checkpoint_id} is not a publication"
        ))
    })?;
    if publication.command != *command {
        return Err(EvolutionError::Conflict(format!(
            "live-evolution publication {checkpoint_id} was reused with different semantics"
        )));
    }
    Ok(Some(publication.receipt))
}

fn decode(record: &JournalRecord) -> EvolutionResult<LiveEvolutionCheckpoint> {
    record.verify().map_err(durable_error)?;
    if record.schema != LIVE_EVOLUTION_CHECKPOINT_SCHEMA {
        return Err(EvolutionError::Validation(format!(
            "unexpected live-evolution checkpoint schema {}",
            record.schema
        )));
    }
    let checkpoint: LiveEvolutionCheckpoint = serde_json::from_value(record.payload.clone())
        .map_err(|error| EvolutionError::Validation(error.to_string()))?;
    if checkpoint.checkpoint_version != LIVE_EVOLUTION_CHECKPOINT_SCHEMA
        || checkpoint.checkpoint_id != record.record_id
    {
        return Err(EvolutionError::Validation(
            "live-evolution checkpoint envelope does not match its journal record".to_owned(),
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

fn virtual_error(error: cymule_virtual::VirtualError) -> EvolutionError {
    match error {
        cymule_virtual::VirtualError::Validation(message)
        | cymule_virtual::VirtualError::Source(message) => EvolutionError::Validation(message),
        cymule_virtual::VirtualError::NotFound(message) => EvolutionError::NotFound(message),
        cymule_virtual::VirtualError::Conflict(message)
        | cymule_virtual::VirtualError::Durable(message) => EvolutionError::Conflict(message),
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
