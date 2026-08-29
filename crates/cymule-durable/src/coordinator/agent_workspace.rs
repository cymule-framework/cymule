//! Exact Agent workspace admission, dispatch, and observation over one pinned root.

use super::{
    DurableCoordinator, ExecutorEffectRead, ExecutorRunRead, ExecutorStepRead,
    derive_executor_boundary, derive_pinned_effect_enqueue, derive_pinned_effect_settlement,
    executor_evaluate, pinned_batch_final_run, pinned_durable_run_current, proposed_pinned_lease,
    region_at_path, require_agent_session, synchronize_pinned_effect_projection,
    verify_agent_occurrence_origin, verify_agent_session_origin,
};
use crate::model::{
    AgentWorkspaceCheckpoint, DURABLE_RUNTIME_ACTOR, EFFECT_RESULT_ARTIFACT_KIND,
    SCOPE_RESULT_ARTIFACT_KIND, agent_workspace_continuation_digest, agent_workspace_coupling_id,
    validate_workspace_checkpoint_batch, verify_workspace_receipt_link,
    workspace_checkpoint_commands,
};
use crate::state_root::pinned_machine::{
    PinnedMachineBatchOutcome, PinnedMachinePreparedMaterial, PinnedMachineStagedMutation,
};
use crate::{
    ClockObservation, Continuation, ContinuationStatus, CoordinationLease, CoupledCheckpoint,
    CoupledCheckpointReceipt, DurableDelta, DurableError, DurableOperation, DurableResult,
    DurableStore, EffectDispatch, OutboxState,
};
use agent_protocol::{
    AgentCommand, AgentCommandAction, AgentCommandOutcome, AgentCommandReceipt, AgentCommandSource,
    AgentCommit, AgentHostOccurrence, AgentHostOccurrenceState, AgentHostRequest,
    AgentHostResponse, AgentOccurrenceSource, AgentProviders, AgentWorkspaceCommand,
    AgentWorkspaceCommandPhase, AgentWorkspaceCommitOutcome, AgentWorkspaceM1Witness,
    AgentWorkspaceProviderProduct, AgentWorkspaceSource, WorkspaceScopeRequest,
};
use cymule_core::durable_internal::{
    MachineMaterialAdmission, MachinePinnedBatchCommand, MachinePinnedBatchPrecondition,
    MachineRunCurrent,
};
use cymule_core::{
    ArtifactRef, Command, EffectTransition, Operation, ReconciliationResolution, SealedPlan,
    WorldOutcome,
};
use cymule_profile_protocol::agent as agent_protocol;
use cymule_runtime::{ExecutionBinding, ExecutionOperationKind};
use std::collections::{BTreeMap, BTreeSet};

struct WorkspaceM1Source {
    authority_root: String,
    run: MachineRunCurrent,
    continuation: Continuation,
    effect: Option<cymule_core::EffectProjection>,
    outbox: Option<EffectDispatch>,
    lease: Option<CoordinationLease>,
}

struct WorkspacePreparedCommit {
    source: WorkspaceM1Source,
    continuation: Continuation,
    outbox: Option<EffectDispatch>,
    lease: Option<CoordinationLease>,
    dispatch_clock: Option<ClockObservation>,
    stage: Option<WorkspaceStage>,
}

enum WorkspaceStage {
    Commands(Box<PinnedMachineStagedMutation>),
    Material(Box<PinnedMachinePreparedMaterial>),
}

impl WorkspaceStage {
    fn commands(stage: PinnedMachineStagedMutation) -> Self {
        Self::Commands(Box::new(stage))
    }
    fn material(prepared: PinnedMachinePreparedMaterial) -> Self {
        Self::Material(Box::new(prepared))
    }
}

struct WorkspaceM1Target {
    effect: Option<cymule_core::EffectProjection>,
    run: MachineRunCurrent,
    authority_root: String,
    batch_id: Option<String>,
    batch_receipt_id: Option<String>,
}

impl<S: DurableStore> DurableCoordinator<S> {
    fn workspace_m1_source(
        &self,
        read: &ExecutorRunRead,
        effect: Option<cymule_core::EffectProjection>,
        outbox: Option<EffectDispatch>,
        lease: Option<CoordinationLease>,
    ) -> DurableResult<WorkspaceM1Source> {
        let pinned = self
            .pinned
            .as_ref()
            .ok_or_else(|| DurableError::NotFound("durable state is not initialized".to_owned()))?;
        if pinned.revision() != read.revision {
            return Err(DurableError::Conflict {
                expected: Some(read.revision.clone()),
                current: Some(pinned.revision().to_owned()),
            });
        }
        Ok(WorkspaceM1Source {
            authority_root: pinned.manifest.machine_frontier().authority_root.clone(),
            run: read.run.clone(),
            continuation: read.continuation.clone(),
            effect,
            outbox,
            lease,
        })
    }

    pub(crate) fn read_agent_workspace_admission(
        &mut self,
        query: &agent_protocol::AgentWorkspaceAdmissionQuery,
    ) -> DurableResult<agent_protocol::AgentWorkspaceAdmissionRead> {
        query.verify()?;
        let read = self.read_current_state_root(|manifest, resolver| {
            workspace_admission_at_manifest(manifest, resolver, query)
        })?;
        read.verify_for(query)?;
        Ok(read)
    }

    pub(super) fn verify_agent_workspace_source_session(
        &mut self,
        command: &AgentCommand,
    ) -> DurableResult<()> {
        let session_id = match &command.action {
            AgentCommandAction::SessionUpdate { session_id, .. } => session_id,
            AgentCommandAction::Occurrence { occurrence } => &occurrence.session_id,
            AgentCommandAction::Stream(stream) => stream.session_id(),
            AgentCommandAction::Input(input) => input.session_id(),
            AgentCommandAction::Workspace(workspace) => &workspace.request().session_id,
        };
        let witness = self.read_current_state_root(|manifest, resolver| {
            Ok(
                crate::state_root::load_agent_session_current(manifest, resolver, session_id)?
                    .and_then(|current| current.last_transition),
            )
        })?;
        if let Some(witness) = witness {
            self.verify_agent_workspace_origin(&witness.command_id)?;
        }
        Ok(())
    }

    pub(crate) fn commit_agent_workspace(
        &mut self,
        command: &AgentCommand,
        providers: &mut dyn AgentProviders,
        clock: &mut dyn crate::ExecutionClockAuthority,
    ) -> DurableResult<AgentWorkspaceCommitOutcome> {
        command.verify()?;
        let AgentCommandAction::Workspace(workspace) = &command.action else {
            return Err(DurableError::Validation(
                "workspace persistence requires one closed Workspace command".to_owned(),
            ));
        };
        if let Some(commit) = self.replay_workspace(command)? {
            return Ok(AgentWorkspaceCommitOutcome::Committed {
                commit: Box::new(commit),
            });
        }
        require_workspace_revision(command, self.current_revision()?)?;
        let source = self.workspace_agent_source(workspace.request())?;
        let commit = match workspace.as_ref() {
            AgentWorkspaceCommand::StartEffect { .. } => {
                self.start_workspace_effect(command, workspace, source, providers, clock)?
            }
            AgentWorkspaceCommand::StartAbort { .. } => {
                self.start_workspace_abort(command, workspace, source, providers)?
            }
            AgentWorkspaceCommand::SettleEffect { .. } => {
                return self.settle_workspace_effect(command, workspace, source, providers);
            }
            AgentWorkspaceCommand::SettleAbort { .. } => {
                return self.settle_workspace_abort(command, workspace, source, providers);
            }
        };
        let AgentCommandOutcome::Workspace(checkpoint) = &commit.receipt.outcome else {
            return Err(workspace_integrity(
                "agent_workspace_started_checkpoint_missing",
                "workspace Start committed another Agent outcome",
            ));
        };
        let occurrence = &checkpoint.occurrence.current.occurrence;
        if occurrence.state != AgentHostOccurrenceState::Started {
            return Err(workspace_integrity(
                "agent_workspace_dispatch_not_started",
                "workspace dispatch lost its committed Started occurrence",
            ));
        }
        providers
            .dispatch_agent_workspace(workspace, occurrence)
            .map_err(|error| DurableError::CommitOutcomeUnknown {
                message: format!(
                    "Agent workspace Session {} occurrence {} is durably Started but dispatch acknowledgement is unknown: {error}; reconcile that occurrence without redispatch",
                    occurrence.session_id, occurrence.occurrence_id,
                ),
            })?;
        Ok(AgentWorkspaceCommitOutcome::Committed {
            commit: Box::new(commit),
        })
    }

    fn replay_workspace(&mut self, command: &AgentCommand) -> DurableResult<Option<AgentCommit>> {
        let receipt = self.read_current_state_root(|manifest, resolver| {
            crate::state_root::load_agent_command_receipt(manifest, resolver, &command.command_id)
        })?;
        let Some(receipt) = receipt else {
            return Ok(None);
        };
        let retained = self.read_current_state_root(|manifest, resolver| {
            crate::state_root::load_agent_command(manifest, resolver, &command.command_id)
        })?;
        if retained.as_ref() != Some(command) {
            return Err(DurableError::HistoryConflict {
                code: "agent_workspace_command_reused".to_owned(),
                message: "workspace command does not equal its exact retained admission".to_owned(),
            });
        }
        receipt.verify_for(command)?;
        self.verify_agent_workspace_origin(&command.command_id)?;
        Ok(Some(AgentCommit {
            observed_revision: self.current_revision()?.to_owned(),
            committed_revision: None,
            receipt,
        }))
    }

    fn workspace_agent_source(
        &mut self,
        request: &WorkspaceScopeRequest,
    ) -> DurableResult<AgentWorkspaceSource> {
        let source = self.read_current_state_root(|manifest, resolver| {
            let session = require_agent_session(manifest, resolver, &request.session_id)?;
            verify_agent_session_origin(manifest, resolver, &session)?;
            let current = crate::state_root::load_agent_occurrence_current(
                manifest,
                resolver,
                &request.session_id,
                &request.occurrence_id,
            )?;
            if let Some(current) = &current {
                verify_agent_occurrence_origin(manifest, resolver, current)?;
            }
            Ok(AgentWorkspaceSource {
                occurrence: AgentOccurrenceSource { session, current },
            })
        })?;
        if let Some(witness) = &source.occurrence.session.last_transition {
            self.verify_agent_workspace_origin(&witness.command_id)?;
        }
        if let Some(current) = &source.occurrence.current
            && source
                .occurrence
                .session
                .last_transition
                .as_ref()
                .is_none_or(|witness| witness.command_id != current.admitted_by)
        {
            self.verify_agent_workspace_origin(&current.admitted_by)?;
        }
        Ok(source)
    }

    fn start_workspace_effect(
        &mut self,
        command: &AgentCommand,
        workspace: &AgentWorkspaceCommand,
        source: AgentWorkspaceSource,
        providers: &mut dyn AgentProviders,
        clock: &mut dyn crate::ExecutionClockAuthority,
    ) -> DurableResult<AgentCommit> {
        let AgentWorkspaceCommand::StartEffect {
            request,
            effect_intent_id,
            execution_binding,
            operation_occurrence_binding,
        } = workspace
        else {
            return Err(DurableError::Validation(
                "workspace is not StartEffect".to_owned(),
            ));
        };
        self.verify_workspace_start_admission(workspace)?;
        let mut read = self.read_execution_step(&request.run_id)?;
        require_workspace_frame(&read, request)?;
        let args = self.workspace_artifact(&request.overlay)?;
        let before = self.workspace_m1_source(&read.run, None, None, None)?;
        let dispatch = EffectDispatch {
            intent_id: effect_intent_id.clone(),
            run_id: request.run_id.clone(),
            origin_plan_id: read.run.plan.plan_id.clone(),
            operation: request.operation.clone(),
            input: request.overlay.clone(),
            execution_binding: execution_binding.clone(),
            occurrence_binding: operation_occurrence_binding.clone(),
            execution_availability: cymule_core::EffectExecutionAvailability::Available,
            reconciliation: cymule_core::ReconciliationState::NotRequired,
            state: OutboxState::Pending,
            claim_epoch: 0,
            claim_owner: None,
            result: None,
        };
        let (proposal, after_effect, eager) =
            derive_pinned_effect_enqueue(&read, &args, &dispatch)?;
        if eager {
            return Err(DurableError::Validation(
                "workspace admission cannot eagerly dispatch an observational Effect".to_owned(),
            ));
        }
        read.run.continuation = after_effect;
        let (continuation, artifacts) = workspace_scope_commit_continuation(&read)?;
        let lease_request = request.dispatch_lease.as_ref().ok_or_else(|| {
            DurableError::Validation("workspace StartEffect has no dispatch lease".to_owned())
        })?;
        let observation = clock.resolve(&lease_request.clock)?;
        observation.verify()?;
        if observation.reference() != lease_request.clock {
            return Err(workspace_integrity(
                "agent_workspace_clock_observation_mismatch",
                "workspace lease resolved another exact Clock receipt",
            ));
        }
        let previous = self.workspace_lease(effect_intent_id)?;
        if previous.is_some() {
            return Err(DurableError::HistoryConflict {
                code: "agent_workspace_orphan_dispatch_lease".to_owned(),
                message: "new workspace Effect already has a dispatch lease without its admission"
                    .to_owned(),
            });
        }
        let lease = proposed_pinned_lease(
            previous.as_ref(),
            effect_intent_id,
            &lease_request.owner,
            observation.logical_time,
            lease_request.ttl,
        )?;
        let commands = workspace_effect_start_commands(workspace, &before.run, proposal)?;
        let stage =
            self.prepare_workspace_stage(commands, workspace_material(command, artifacts)?)?;
        let product =
            agent_protocol::execute_agent_workspace_provider(&source, command, providers)?;
        source.preview_occurrence(workspace, &product)?;
        let mut dispatch = dispatch;
        dispatch.state = OutboxState::Claimed;
        dispatch.claim_epoch = lease.epoch;
        dispatch.claim_owner = Some(lease.owner.clone());
        let prepared = WorkspacePreparedCommit {
            source: before,
            continuation,
            outbox: Some(dispatch),
            lease: Some(lease),
            dispatch_clock: Some(observation.clone()),
            stage: Some(WorkspaceStage::commands(stage)),
        };
        self.with_current_clock(clock, &lease_request.clock, |coordinator, current| {
            if current != observation {
                return Err(workspace_integrity(
                    "agent_workspace_clock_changed",
                    "workspace final Clock guard changed its prepared observation",
                ));
            }
            coordinator.publish_workspace(command, workspace, source, &product, prepared)
        })
    }

    fn start_workspace_abort(
        &mut self,
        command: &AgentCommand,
        workspace: &AgentWorkspaceCommand,
        source: AgentWorkspaceSource,
        providers: &mut dyn AgentProviders,
    ) -> DurableResult<AgentCommit> {
        self.verify_workspace_start_admission(workspace)?;
        let read = self.read_execution_step(&workspace.request().run_id)?;
        require_workspace_frame(&read, workspace.request())?;
        let _ = workspace_abort_continuation(&read)?;
        let preflight = self.prepare_workspace_stage(
            vec![workspace_abort_command(workspace.request(), &read.run.run)?],
            None,
        )?;
        drop(preflight);
        let prepared = WorkspacePreparedCommit {
            source: self.workspace_m1_source(&read.run, None, None, None)?,
            continuation: read.run.continuation,
            outbox: None,
            lease: None,
            dispatch_clock: None,
            stage: None,
        };
        let product =
            agent_protocol::execute_agent_workspace_provider(&source, command, providers)?;
        self.publish_workspace(command, workspace, source, &product, prepared)
    }

    fn verify_workspace_start_admission(
        &mut self,
        workspace: &AgentWorkspaceCommand,
    ) -> DurableResult<()> {
        let (decision, execution_binding, operation_binding, expected_intent) = match workspace {
            AgentWorkspaceCommand::StartEffect {
                execution_binding,
                operation_occurrence_binding,
                effect_intent_id,
                ..
            } => (
                agent_protocol::AgentWorkspaceDecision::Commit,
                execution_binding,
                operation_occurrence_binding,
                Some(effect_intent_id),
            ),
            AgentWorkspaceCommand::StartAbort {
                execution_binding,
                operation_occurrence_binding,
                ..
            } => (
                agent_protocol::AgentWorkspaceDecision::Abort,
                execution_binding,
                operation_occurrence_binding,
                None,
            ),
            _ => {
                return Err(DurableError::Validation(
                    "workspace is not a Start phase".to_owned(),
                ));
            }
        };
        let read =
            self.read_agent_workspace_admission(&agent_protocol::AgentWorkspaceAdmissionQuery {
                request: workspace.request().clone(),
                decision,
                expected_revision: Some(self.current_revision()?.to_owned()),
            })?;
        if read.execution_binding != *execution_binding
            || read.operation_occurrence_binding != *operation_binding
            || read
                .host_request
                .m1_owner()
                .and_then(|owner| owner.effect_intent_id.as_ref())
                != expected_intent
        {
            return Err(workspace_integrity(
                "agent_workspace_start_admission_mismatch",
                "workspace Start changed its exact M1-derived execution authority",
            ));
        }
        Ok(())
    }

    fn prepare_workspace_stage(
        &mut self,
        commands: Vec<MachinePinnedBatchCommand>,
        material: Option<MachineMaterialAdmission>,
    ) -> DurableResult<PinnedMachineStagedMutation> {
        match self.prepare_pinned_command_batch(commands, material, None)? {
            PinnedMachineBatchOutcome::Staged(stage) => Ok(stage),
            PinnedMachineBatchOutcome::Replay(_) => Err(DurableError::HistoryConflict {
                code: "agent_workspace_partial_core_history".to_owned(),
                message: "workspace Core commands exist without their exact atomic Agent receipt"
                    .to_owned(),
            }),
            PinnedMachineBatchOutcome::PagedBegin(_) | PinnedMachineBatchOutcome::Pending(_) => {
                Err(DurableError::Validation(
                    "workspace requires a complete bounded inline Scope transition before provider I/O"
                        .to_owned(),
                ))
            }
            PinnedMachineBatchOutcome::NeedsArchive(_)
            | PinnedMachineBatchOutcome::NeedsArchivedBatch(_) => Err(workspace_integrity(
                "agent_workspace_unresolved_core_preparation",
                "workspace Core preparation returned unresolved archive authority",
            )),
        }
    }

    fn workspace_artifact(
        &mut self,
        reference: &ArtifactRef,
    ) -> DurableResult<cymule_core::ArtifactRecord> {
        self.read_current_state_root(|manifest, resolver| {
            let record =
                crate::state_root::pinned_machine::PinnedMachineView::open(manifest, resolver)?
                    .artifact(&reference.artifact_id)?
                    .ok_or_else(|| {
                        DurableError::NotFound(format!(
                            "workspace Artifact {} does not exist",
                            reference.artifact_id
                        ))
                    })?;
            if record.reference != *reference {
                return Err(workspace_integrity(
                    "agent_workspace_artifact_mismatch",
                    "workspace Artifact changed its exact reference",
                ));
            }
            Ok(record)
        })
    }

    fn workspace_lease(&mut self, intent_id: &str) -> DurableResult<Option<CoordinationLease>> {
        self.read_current_state_root(|manifest, resolver| {
            crate::state_root::load_typed_state_map_value(
                &manifest.roots().leases,
                intent_id,
                crate::StateRootLeafKind::Lease,
                resolver,
            )
        })
    }

    fn settle_workspace_effect(
        &mut self,
        command: &AgentCommand,
        workspace: &AgentWorkspaceCommand,
        source: AgentWorkspaceSource,
        providers: &mut dyn AgentProviders,
    ) -> DurableResult<AgentWorkspaceCommitOutcome> {
        let current = source.occurrence.current.as_ref().ok_or_else(|| {
            DurableError::Validation(
                "workspace settlement requires its retained occurrence".to_owned(),
            )
        })?;
        let request = workspace.request();
        let intent = workspace_occurrence_intent(&current.occurrence)?.ok_or_else(|| {
            workspace_integrity(
                "agent_workspace_effect_intent_missing",
                "workspace Effect settlement has no retained intent",
            )
        })?;
        let read = self
            .read_effect_execution(
                &request.run_id,
                &intent,
                self.current_revision()?.to_owned().as_str(),
            )?
            .ok_or_else(|| {
                workspace_integrity(
                    "agent_workspace_effect_missing",
                    "workspace occurrence lost its exact M1 Effect",
                )
            })?;
        verify_workspace_effect_origin(request, &current.occurrence, &read)?;
        let lease = self.workspace_effect_lease(&intent, &read.dispatch)?;
        let product =
            agent_protocol::execute_agent_workspace_provider(&source, command, providers)?;
        let preview = source.preview_occurrence(workspace, &product)?;
        let mut artifacts = self.workspace_observation_material(&product)?;
        if preview == current.occurrence {
            return unchanged_workspace(command, self.current_revision()?, current);
        }
        let before = self.workspace_m1_source(
            &read.run,
            Some(read.effect.clone()),
            Some(read.dispatch.clone()),
            Some(lease.clone()),
        )?;
        let mut dispatch = read.dispatch.clone();
        let stage = if preview.state == AgentHostOccurrenceState::Unknown
            && dispatch.state == OutboxState::Unknown
        {
            self.prepare_workspace_material(command, artifacts)?
        } else {
            let (outcome, resolution, result) = workspace_effect_observation(&preview)?;
            let settlement = match dispatch.state {
                OutboxState::Claimed => {
                    crate::executor::ExecutorEffectSettlement::Observation { outcome, result }
                }
                OutboxState::Unknown => {
                    crate::executor::ExecutorEffectSettlement::Reconciliation { resolution, result }
                }
                _ => {
                    return Err(DurableError::IllegalTransition(
                        "workspace settlement requires the original Claimed or Unknown outbox"
                            .to_owned(),
                    ));
                }
            };
            let (_, transition, state, result) =
                derive_pinned_effect_settlement(&read, settlement)?;
            dispatch.state = state;
            dispatch.result = result.as_ref().map(|artifact| artifact.reference.clone());
            artifacts.extend(result);
            let batch = workspace_batch_command(
                request,
                workspace.phase_for(&preview)?,
                Command::TransitionEffect {
                    intent_id: intent,
                    transition,
                },
                MachinePinnedBatchPrecondition::Parent(Some(read.run.run.precondition_token())),
            )?;
            Some(WorkspaceStage::commands(self.prepare_workspace_stage(
                vec![batch],
                workspace_material(command, artifacts)?,
            )?))
        };
        let prepared = WorkspacePreparedCommit {
            source: before,
            continuation: read.run.continuation,
            outbox: Some(dispatch),
            lease: Some(lease),
            dispatch_clock: None,
            stage,
        };
        let commit = self.publish_workspace(command, workspace, source, &product, prepared)?;
        Ok(AgentWorkspaceCommitOutcome::Committed {
            commit: Box::new(commit),
        })
    }

    fn settle_workspace_abort(
        &mut self,
        command: &AgentCommand,
        workspace: &AgentWorkspaceCommand,
        source: AgentWorkspaceSource,
        providers: &mut dyn AgentProviders,
    ) -> DurableResult<AgentWorkspaceCommitOutcome> {
        let current = source.occurrence.current.as_ref().ok_or_else(|| {
            DurableError::Validation(
                "workspace abort settlement requires its retained occurrence".to_owned(),
            )
        })?;
        let read = self.read_execution_step(&workspace.request().run_id)?;
        require_workspace_frame(&read, workspace.request())?;
        self.verify_workspace_abort_binding(
            workspace.request(),
            &current.occurrence,
            &read.run.plan,
        )?;
        let target = workspace_abort_continuation(&read)?;
        let prepared_abort = self.prepare_workspace_stage(
            vec![workspace_abort_command(workspace.request(), &read.run.run)?],
            None,
        )?;
        let product =
            agent_protocol::execute_agent_workspace_provider(&source, command, providers)?;
        let preview = source.preview_occurrence(workspace, &product)?;
        let artifacts = self.workspace_observation_material(&product)?;
        if preview == current.occurrence {
            return unchanged_workspace(command, self.current_revision()?, current);
        }
        let applied = preview.state == AgentHostOccurrenceState::Completed;
        let stage = if applied {
            if artifacts.is_empty() {
                Some(WorkspaceStage::commands(prepared_abort))
            } else {
                Some(WorkspaceStage::commands(self.prepare_workspace_stage(
                    vec![workspace_abort_command(workspace.request(), &read.run.run)?],
                    workspace_material(command, artifacts)?,
                )?))
            }
        } else {
            self.prepare_workspace_material(command, artifacts)?
        };
        let before = self.workspace_m1_source(&read.run, None, None, None)?;
        let prepared = WorkspacePreparedCommit {
            source: before,
            continuation: if applied {
                target
            } else {
                read.run.continuation
            },
            outbox: None,
            lease: None,
            dispatch_clock: None,
            stage,
        };
        let commit = self.publish_workspace(command, workspace, source, &product, prepared)?;
        Ok(AgentWorkspaceCommitOutcome::Committed {
            commit: Box::new(commit),
        })
    }

    fn workspace_effect_lease(
        &mut self,
        intent_id: &str,
        dispatch: &EffectDispatch,
    ) -> DurableResult<CoordinationLease> {
        let lease = self.workspace_lease(intent_id)?.ok_or_else(|| {
            workspace_integrity(
                "agent_workspace_claim_lease_missing",
                "workspace Effect lost its exact dispatch lease",
            )
        })?;
        if dispatch.claim_owner.as_deref() != Some(lease.owner.as_str())
            || dispatch.claim_epoch != lease.epoch
            || lease.resource != intent_id
        {
            return Err(workspace_integrity(
                "agent_workspace_claim_lease_changed",
                "workspace settlement was fenced by a different dispatch lease",
            ));
        }
        Ok(lease)
    }

    fn verify_workspace_abort_binding(
        &mut self,
        request: &WorkspaceScopeRequest,
        occurrence: &AgentHostOccurrence,
        plan: &SealedPlan,
    ) -> DurableResult<()> {
        let (reference, operation, occurrence_binding) = occurrence
            .occurrence_binding
            .m1_effect_operation_closure()?;
        if operation != request.operation {
            return Err(workspace_integrity(
                "agent_workspace_abort_operation_mismatch",
                "workspace abort changed its retained operation",
            ));
        }
        let record = self.workspace_artifact(reference)?;
        let binding = ExecutionBinding::decode(&record.bytes)?;
        binding.admit_plan(plan)?;
        if binding.artifact_ref()? != *reference
            || binding.occurrence_binding(ExecutionOperationKind::Effect, operation)?
                != occurrence_binding
        {
            return Err(workspace_integrity(
                "agent_workspace_abort_binding_mismatch",
                "workspace abort cannot resolve its exact historical binding",
            ));
        }
        Ok(())
    }

    fn workspace_observation_material(
        &mut self,
        product: &AgentWorkspaceProviderProduct,
    ) -> DurableResult<Vec<cymule_core::ArtifactRecord>> {
        let required = product.required_artifacts()?;
        self.read_current_state_root(|manifest, resolver| {
            let mut view =
                crate::state_root::pinned_machine::PinnedMachineView::open(manifest, resolver)?;
            let supplied = product
                .artifacts()
                .iter()
                .map(|record| (&record.reference, record))
                .collect::<BTreeMap<_, _>>();
            let mut fresh = Vec::new();
            for reference in required {
                let existing = view.artifact(&reference.artifact_id)?;
                let provided = supplied.get(&reference).copied();
                match (existing, provided) {
                    (Some(existing), provided) => {
                        if existing.reference != reference
                            || provided.is_some_and(|record| record != &existing)
                        {
                            return Err(workspace_integrity(
                                "agent_workspace_observation_artifact_conflict",
                                "workspace observer changed an exact parent Artifact",
                            ));
                        }
                    }
                    (None, Some(record)) => fresh.push(record.clone()),
                    (None, None) => {
                        return Err(workspace_integrity(
                            "agent_workspace_observation_artifact_missing",
                            "workspace observer omitted the bytes of a new typed evidence Artifact",
                        ));
                    }
                }
            }
            Ok(fresh)
        })
    }

    fn prepare_workspace_material(
        &mut self,
        command: &AgentCommand,
        artifacts: Vec<cymule_core::ArtifactRecord>,
    ) -> DurableResult<Option<WorkspaceStage>> {
        let Some(material) = workspace_material(command, artifacts)? else {
            return Ok(None);
        };
        self.read_current_state_root(|manifest, resolver| {
            crate::state_root::pinned_machine::PinnedMachineView::open(manifest, resolver)?
                .prepare_material(&material)
                .map(|prepared| Some(WorkspaceStage::material(prepared)))
        })
    }

    fn publish_workspace(
        &mut self,
        command: &AgentCommand,
        workspace: &AgentWorkspaceCommand,
        source: AgentWorkspaceSource,
        product: &AgentWorkspaceProviderProduct,
        mut prepared: WorkspacePreparedCommit,
    ) -> DurableResult<AgentCommit> {
        require_workspace_revision(command, self.current_revision()?)?;
        let preview = source.preview_occurrence(workspace, product)?;
        let phase = workspace.phase_for(&preview)?;
        let target = workspace_m1_target(&mut prepared)?;
        self.verify_workspace_retained_artifacts(
            workspace,
            &preview,
            [prepared.source.outbox.as_ref(), prepared.outbox.as_ref()],
            Some(product),
        )?;
        let target_digest = agent_workspace_continuation_digest(&prepared.continuation)?;
        let m1_receipt = workspace_coupled_receipt(
            command,
            workspace,
            phase,
            &prepared,
            &target,
            &target_digest,
        )?;
        let intent = workspace_occurrence_intent(&preview)?;
        let witness = AgentWorkspaceM1Witness {
            run_id: workspace.request().run_id.clone(),
            scope_id: workspace.request().scope_id.clone(),
            phase,
            continuation_digest: target_digest,
            effect_intent_id: intent.clone(),
            obligation_id: intent
                .as_deref()
                .map(cymule_core::effect_obligation_id)
                .transpose()?,
            m1_receipt_id: m1_receipt.receipt_id.clone(),
        };
        let postcondition =
            source.reduce_with_provider(&command.command_id, workspace, product, witness)?;
        let receipt = AgentCommandReceipt::new(
            command,
            AgentCommandSource::Workspace(source),
            AgentCommandOutcome::Workspace(Box::new(postcondition.clone())),
        )?;
        let mut operations = vec![
            DurableOperation::PutAgentCommand {
                value: command.clone(),
            },
            DurableOperation::PutAgentCommandReceipt {
                value: Box::new(receipt.clone()),
            },
            DurableOperation::PutAgentSessionCurrent {
                value: postcondition.occurrence.session,
            },
            DurableOperation::PutAgentOccurrenceCurrent {
                value: postcondition.occurrence.current,
            },
            DurableOperation::PutCoupledCheckpointReceipt { value: m1_receipt },
        ];
        operations.extend(workspace_execution_operations(&prepared, &target, phase)?);
        let revision = match prepared.stage {
            Some(WorkspaceStage::Commands(stage)) => {
                self.publish_pinned_stage(*stage, Some(DurableDelta::new(operations)?))?
                    .1
            }
            Some(WorkspaceStage::Material(material)) => {
                let stage = (*material).bind_outer_receipt(&receipt.receipt_id)?;
                self.publish_pinned_stage(stage, Some(DurableDelta::new(operations)?))?
                    .1
            }
            None => self.commit_profile_operations(operations)?,
        };
        let commit = AgentCommit {
            observed_revision: revision.clone(),
            committed_revision: Some(revision),
            receipt,
        };
        commit.verify_for(command)?;
        Ok(commit)
    }

    pub(super) fn verify_agent_workspace_origin(&mut self, command_id: &str) -> DurableResult<()> {
        let origin = self.read_current_state_root(|manifest, resolver| {
            let command = crate::state_root::load_agent_command(manifest, resolver, command_id)?
                .ok_or_else(|| {
                    workspace_integrity(
                        "agent_workspace_origin_command_missing",
                        "Agent origin has no exact command",
                    )
                })?;
            if !matches!(&command.action, AgentCommandAction::Workspace(_)) {
                return Ok(None);
            }
            let receipt =
                crate::state_root::load_agent_command_receipt(manifest, resolver, command_id)?
                    .ok_or_else(|| {
                        workspace_integrity(
                            "agent_workspace_origin_receipt_missing",
                            "Agent workspace origin has no exact receipt",
                        )
                    })?;
            receipt.verify_for(&command)?;
            let coupled = crate::state_root::load_coupled_checkpoint_receipt(
                manifest,
                resolver,
                &agent_workspace_coupling_id(command_id)?,
            )?
            .ok_or_else(|| {
                workspace_integrity(
                    "agent_workspace_m1_receipt_missing",
                    "Agent workspace origin has no real M1 coupled receipt",
                )
            })?;
            Ok(Some((command, receipt, coupled)))
        })?;
        let Some((command, receipt, coupled)) = origin else {
            return Ok(());
        };
        let (
            AgentCommandAction::Workspace(workspace),
            AgentCommandOutcome::Workspace(agent_checkpoint),
            CoupledCheckpoint::AgentWorkspace { checkpoint },
        ) = (&command.action, &receipt.outcome, &coupled.checkpoint)
        else {
            return Err(workspace_integrity(
                "agent_workspace_origin_shape_mismatch",
                "Agent workspace origin has inconsistent typed receipt kinds",
            ));
        };
        verify_workspace_receipt_link(&command, workspace, agent_checkpoint, &coupled, checkpoint)?;
        if let Some((batch, entries)) = self.workspace_origin_batch(workspace, checkpoint)? {
            validate_workspace_checkpoint_batch(
                &command,
                workspace,
                agent_checkpoint,
                checkpoint,
                &batch,
                &entries,
            )?;
            self.verify_workspace_batch_material(&command, &batch, entries.is_empty())?;
        }
        self.verify_workspace_retained_artifacts(
            workspace,
            &agent_checkpoint.occurrence.current.occurrence,
            [
                checkpoint.outbox_before.as_ref(),
                checkpoint.outbox_after.as_ref(),
            ],
            None,
        )
    }

    fn verify_workspace_batch_material(
        &mut self,
        command: &AgentCommand,
        batch: &cymule_core::MachineCommandBatchRecord,
        required: bool,
    ) -> DurableResult<()> {
        batch.verify()?;
        let Some(source) = &batch.material_source else {
            return if required {
                Err(workspace_integrity(
                    "agent_workspace_empty_material_batch",
                    "workspace material receipt has no actual material admission",
                ))
            } else {
                Ok(())
            };
        };
        if source.source_command_id != command.command_id
            || !source.plan_ids.is_empty()
            || source.artifacts.is_empty()
        {
            return Err(workspace_integrity(
                "agent_workspace_material_source_mismatch",
                "workspace material batch changed its exact outer command or Artifact-only input",
            ));
        }
        let mut artifacts = Vec::with_capacity(source.artifacts.len());
        for reference in &source.artifacts {
            artifacts.push(self.workspace_artifact(reference)?);
        }
        let material =
            MachineMaterialAdmission::new(source.source_command_id.clone(), Vec::new(), artifacts)?;
        if batch.material_digest.as_deref() != Some(material.material_digest()) {
            return Err(workspace_integrity(
                "agent_workspace_material_digest_mismatch",
                "workspace Core batch does not bind its exact immutable material bytes",
            ));
        }
        Ok(())
    }

    fn workspace_origin_batch(
        &mut self,
        workspace: &AgentWorkspaceCommand,
        checkpoint: &AgentWorkspaceCheckpoint,
    ) -> DurableResult<
        Option<(
            cymule_core::MachineCommandBatchRecord,
            Vec<cymule_core::MachineCommandArchiveEntry>,
        )>,
    > {
        let Some(batch_id) = &checkpoint.core_batch_id else {
            return Ok(None);
        };
        let manifest = self
            .pinned
            .as_ref()
            .ok_or_else(|| DurableError::NotFound("durable state is not initialized".to_owned()))?
            .manifest
            .clone();
        let expected = workspace_checkpoint_commands(workspace, checkpoint)?;
        if expected.is_empty() {
            let hot = self.read_current_state_root(|manifest, resolver| {
                crate::state_root::load_typed_state_map_value::<
                    cymule_core::MachineCommandBatchRecord,
                    _,
                >(
                    &manifest.roots().machine_command_batches,
                    batch_id,
                    crate::StateRootLeafKind::MachineCommandBatch,
                    resolver,
                )
            })?;
            let batch = match hot {
                Some(batch) => batch,
                None => self
                    .store
                    .load_machine_command_archive_batch(batch_id)?
                    .ok_or_else(|| {
                        workspace_integrity(
                            "agent_workspace_material_batch_missing",
                            "workspace material receipt lost its real immutable Core batch",
                        )
                    })?,
            };
            return Ok(Some((batch, Vec::new())));
        }
        let mut retained = None;
        let mut entries = Vec::with_capacity(expected.len());
        for (phase, _) in expected {
            let core_id = agent_protocol::agent_workspace_command_id(workspace.request(), phase)?;
            let (entry, batch) =
                crate::store::load_pinned_machine_command(&mut self.store, &manifest, &core_id)?
                    .ok_or_else(|| {
                        workspace_integrity(
                            "agent_workspace_core_command_missing",
                            format!("workspace receipt lost Core command {core_id}"),
                        )
                    })?;
            if retained.as_ref().is_some_and(|old| old != &batch) {
                return Err(workspace_integrity(
                    "agent_workspace_core_batch_mismatch",
                    "workspace receipt does not bind its complete ordered applied Core batch",
                ));
            }
            retained = Some(batch);
            entries.push(entry);
        }
        let batch = retained.ok_or_else(|| {
            workspace_integrity(
                "agent_workspace_core_batch_missing",
                "workspace command list has no retained Core batch",
            )
        })?;
        Ok(Some((batch, entries)))
    }

    fn verify_workspace_retained_artifacts(
        &mut self,
        workspace: &AgentWorkspaceCommand,
        occurrence: &AgentHostOccurrence,
        outboxes: [Option<&EffectDispatch>; 2],
        product: Option<&AgentWorkspaceProviderProduct>,
    ) -> DurableResult<()> {
        let (references, effect_result) =
            workspace_retained_artifact_closure(workspace, occurrence, outboxes)?;
        let mut proposed = product
            .into_iter()
            .flat_map(AgentWorkspaceProviderProduct::artifacts)
            .map(|record| (&record.reference, record))
            .collect::<BTreeMap<_, _>>();
        if product.is_some()
            && let Some(record) = &effect_result
        {
            proposed.insert(&record.reference, record);
        }
        let limit = workspace_retained_byte_limit(occurrence)?;
        let mut bytes = 0_usize;
        for reference in references {
            let retained;
            let record = if let Some(record) = proposed.get(&reference) {
                *record
            } else {
                retained = self.workspace_artifact(&reference)?;
                &retained
            };
            bytes = bytes
                .checked_add(record.bytes.len())
                .filter(|bytes| *bytes <= limit)
                .ok_or_else(|| {
                    DurableError::Validation(
                        "workspace retained evidence exceeds its exact bounded material read set"
                            .to_owned(),
                    )
                })?;
        }
        Ok(())
    }
}

fn workspace_retained_byte_limit(occurrence: &AgentHostOccurrence) -> DurableResult<usize> {
    let total = cymule_core::durable_internal::MAX_PINNED_MACHINE_READ_SET_BYTES;
    if occurrence.is_terminal() {
        return Ok(total);
    }
    let effect_result = if workspace_occurrence_intent(occurrence)?.is_some() {
        cymule_core::MAX_ARTIFACT_BYTES
    } else {
        0
    };
    Ok(total - agent_protocol::MAX_AGENT_WORKSPACE_ARTIFACT_BYTES - effect_result)
}

fn workspace_coupled_receipt(
    command: &AgentCommand,
    workspace: &AgentWorkspaceCommand,
    phase: AgentWorkspaceCommandPhase,
    prepared: &WorkspacePreparedCommit,
    target: &WorkspaceM1Target,
    continuation_digest: &str,
) -> DurableResult<CoupledCheckpointReceipt> {
    CoupledCheckpointReceipt::new(CoupledCheckpoint::AgentWorkspace {
        checkpoint: Box::new(AgentWorkspaceCheckpoint {
            agent_command_id: command.command_id.clone(),
            run_id: workspace.request().run_id.clone(),
            scope_id: workspace.request().scope_id.clone(),
            occurrence_id: workspace.request().occurrence_id.clone(),
            phase,
            source_machine_authority_root: prepared.source.authority_root.clone(),
            machine_authority_root: target.authority_root.clone(),
            core_batch_id: target.batch_id.clone(),
            core_batch_receipt_id: target.batch_receipt_id.clone(),
            source_continuation_digest: agent_workspace_continuation_digest(
                &prepared.source.continuation,
            )?,
            continuation: Box::new(prepared.continuation.clone()),
            continuation_digest: continuation_digest.to_owned(),
            effect_before: prepared.source.effect.clone(),
            effect_after: target.effect.clone(),
            outbox_before: prepared.source.outbox.clone(),
            outbox_after: prepared.outbox.clone(),
            lease_before: prepared.source.lease.clone(),
            lease_after: prepared.lease.clone(),
            dispatch_clock: prepared.dispatch_clock.clone(),
        }),
    })
}

fn workspace_execution_operations(
    prepared: &WorkspacePreparedCommit,
    target: &WorkspaceM1Target,
    phase: AgentWorkspaceCommandPhase,
) -> DurableResult<Vec<DurableOperation>> {
    let mut operations = Vec::new();
    if !matches!(&prepared.stage, Some(WorkspaceStage::Commands(_))) {
        return Ok(operations);
    }
    if prepared.continuation != prepared.source.continuation {
        operations.push(DurableOperation::PutContinuation {
            value: prepared.continuation.clone(),
        });
    }
    let current = pinned_durable_run_current(&target.run, &prepared.continuation)?;
    if current != pinned_durable_run_current(&prepared.source.run, &prepared.source.continuation)? {
        operations.push(DurableOperation::PutRunCurrent { value: current });
    }
    if phase == AgentWorkspaceCommandPhase::StartEffectDispatch
        && let Some(lease) = &prepared.lease
    {
        operations.push(DurableOperation::PutLease {
            value: lease.clone(),
        });
    }
    if let Some(outbox) = &prepared.outbox {
        operations.push(DurableOperation::PutOutbox {
            value: outbox.clone(),
        });
    }
    Ok(operations)
}

fn workspace_retained_artifact_closure(
    workspace: &AgentWorkspaceCommand,
    occurrence: &AgentHostOccurrence,
    outboxes: [Option<&EffectDispatch>; 2],
) -> DurableResult<(BTreeSet<ArtifactRef>, Option<cymule_core::ArtifactRecord>)> {
    let mut references = BTreeSet::from([workspace.request().overlay.clone()]);
    let (binding, _, _) = occurrence
        .occurrence_binding
        .m1_effect_operation_closure()?;
    references.insert(binding.clone());
    if let Some(AgentHostResponse::Workspace(receipt)) = &occurrence.response {
        references.insert(receipt.evidence.clone());
    }
    for observation in &occurrence.recovery_observations {
        for block in &observation.evidence {
            if let agent_protocol::ContentBlock::Artifact { artifact } = block {
                references.insert(artifact.clone());
            }
        }
    }
    for outbox in outboxes.into_iter().flatten() {
        references.insert(outbox.input.clone());
        references.insert(outbox.execution_binding.clone());
        references.extend(outbox.result.iter().cloned());
    }
    let effect_result = if let Some(outbox) = outboxes[1]
        && outbox.state == OutboxState::Applied
    {
        let (_, _, record) = workspace_effect_observation(occurrence)?;
        let record = record.ok_or_else(|| {
            workspace_integrity(
                "agent_workspace_applied_receipt_missing",
                "Applied workspace origin has no provider receipt",
            )
        })?;
        if outbox.result.as_ref() != Some(&record.reference) {
            return Err(workspace_integrity(
                "agent_workspace_result_artifact_mismatch",
                "workspace applied outbox does not retain the exact provider receipt Artifact",
            ));
        }
        Some(record)
    } else {
        None
    };
    Ok((references, effect_result))
}

fn workspace_m1_target(prepared: &mut WorkspacePreparedCommit) -> DurableResult<WorkspaceM1Target> {
    match &prepared.stage {
        Some(WorkspaceStage::Commands(stage)) => {
            let batch = stage.batch_transition().ok_or_else(|| {
                workspace_integrity(
                    "agent_workspace_batch_transition_missing",
                    "workspace stage has no complete Core batch",
                )
            })?;
            let final_run = pinned_batch_final_run(batch)?;
            let effect = if let Some(outbox) = &mut prepared.outbox {
                let effect = final_run.effects.get(&outbox.intent_id).ok_or_else(|| {
                    workspace_integrity(
                        "agent_workspace_batch_effect_missing",
                        "workspace batch lost its final Effect projection",
                    )
                })?;
                synchronize_pinned_effect_projection(effect, outbox)?;
                Some(effect.clone())
            } else {
                None
            };
            Ok(WorkspaceM1Target {
                effect,
                run: final_run.result_current.clone(),
                authority_root: batch.frontier.authority_root.clone(),
                batch_id: Some(batch.batch.batch_id.clone()),
                batch_receipt_id: Some(batch.batch.batch_receipt_id.clone()),
            })
        }
        Some(WorkspaceStage::Material(material)) => {
            let transition = material.transition();
            if transition.delta.batches.len() != 1 {
                return Err(workspace_integrity(
                    "agent_workspace_material_batch_missing",
                    "workspace material has no unique real Core batch",
                ));
            }
            let batch = transition.delta.batches.values().next().ok_or_else(|| {
                workspace_integrity(
                    "agent_workspace_material_batch_missing",
                    "workspace material lost its real Core batch",
                )
            })?;
            Ok(WorkspaceM1Target {
                effect: prepared.source.effect.clone(),
                run: prepared.source.run.clone(),
                authority_root: transition.frontier.authority_root.clone(),
                batch_id: Some(batch.batch_id.clone()),
                batch_receipt_id: Some(batch.batch_receipt_id.clone()),
            })
        }
        None => Ok(WorkspaceM1Target {
            effect: prepared.source.effect.clone(),
            run: prepared.source.run.clone(),
            authority_root: prepared.source.authority_root.clone(),
            batch_id: None,
            batch_receipt_id: None,
        }),
    }
}

fn workspace_admission_at_manifest(
    manifest: &crate::StateRootManifest,
    resolver: &mut dyn crate::StateRootResolver,
    query: &agent_protocol::AgentWorkspaceAdmissionQuery,
) -> DurableResult<agent_protocol::AgentWorkspaceAdmissionRead> {
    if query
        .expected_revision
        .as_deref()
        .is_some_and(|expected| expected != manifest.revision())
    {
        return Err(DurableError::Conflict {
            expected: query.expected_revision.clone(),
            current: Some(manifest.revision().to_owned()),
        });
    }
    let request = &query.request;
    let mut view = crate::state_root::pinned_machine::PinnedMachineView::open(manifest, resolver)?;
    let material = view.run_execution_material(&request.run_id)?;
    let scope = view
        .scope_current(&material.run, &request.scope_id)?
        .ok_or_else(|| {
            DurableError::NotFound(format!("Machine Scope {} does not exist", request.scope_id))
        })?;
    if scope.current.status != cymule_core::ScopeStatus::Open
        || scope.current.invocation_id != request.invocation_id
    {
        return Err(DurableError::IllegalTransition(format!(
            "workspace scope {} is not the requested open invocation",
            request.scope_id
        )));
    }
    verify_workspace_declared_site(&material.plan, &scope, request)?;
    let overlay = view
        .artifact(&request.overlay.artifact_id)?
        .ok_or_else(|| {
            DurableError::NotFound(format!(
                "workspace overlay Artifact {} does not exist",
                request.overlay.artifact_id
            ))
        })?;
    if overlay.reference != request.overlay {
        return Err(workspace_integrity(
            "agent_workspace_overlay_mismatch",
            "workspace overlay Artifact changed its exact reference",
        ));
    }
    let binding = ExecutionBinding::decode(&material.binding.bytes)?;
    binding.admit_plan(&material.plan)?;
    if binding.artifact_ref()? != material.binding.reference {
        return Err(workspace_integrity(
            "agent_workspace_binding_mismatch",
            "workspace execution binding changed its exact Artifact identity",
        ));
    }
    let operation_occurrence_binding =
        binding.occurrence_binding(ExecutionOperationKind::Effect, &request.operation)?;
    let effect_intent_id = if query.decision.commit() {
        let intent = cymule_core::effect_intent_id(&cymule_core::EffectIntentIdentityInput {
            run_id: &request.run_id,
            plan_id: &material.plan.plan_id,
            invocation_id: &request.invocation_id,
            site_id: &request.site_id,
            scope_id: &request.scope_id,
            occurrence: &request.occurrence_key,
            args: &request.overlay,
            effect_schema_version: cymule_core::EFFECT_SCHEMA_VERSION,
        })?;
        if view.effect_current(&material.run, &intent)?.is_some() {
            return Err(DurableError::IllegalTransition(format!(
                "workspace Effect {intent} already exists"
            )));
        }
        Some(intent)
    } else {
        None
    };
    let host_request = agent_protocol::WorkspaceHostRequest::m1_scope(
        agent_protocol::WorkspaceOccurrenceOwner {
            run_id: request.run_id.clone(),
            scope_id: request.scope_id.clone(),
            invocation_id: request.invocation_id.clone(),
            site_id: request.site_id.clone(),
            occurrence_key: request.occurrence_key.clone(),
            operation: request.operation.clone(),
            effect_intent_id,
        },
        request.change(query.decision.commit()),
    )?;
    Ok(agent_protocol::AgentWorkspaceAdmissionRead {
        revision: manifest.revision().to_owned(),
        host_request,
        execution_binding: material.binding.reference,
        operation_occurrence_binding,
    })
}

fn verify_workspace_declared_site(
    plan: &SealedPlan,
    scope: &crate::state_root::pinned_machine::PinnedMachineScopeRead,
    request: &WorkspaceScopeRequest,
) -> DurableResult<()> {
    let definition = plan
        .candidate
        .definitions
        .iter()
        .find(|definition| definition.id == scope.current.definition_id)
        .ok_or_else(|| {
            workspace_integrity(
                "agent_workspace_definition_missing",
                "workspace Scope has no admitted definition",
            )
        })?;
    let step = region_at_path(&definition.body, &scope.region_path)?
        .steps
        .iter()
        .find(|step| step.id == request.site_id)
        .ok_or_else(|| {
            DurableError::NotFound(format!(
                "workspace Effect site {} does not exist",
                request.site_id
            ))
        })?;
    let Operation::Effect {
        effect, occurrence, ..
    } = &step.operation
    else {
        return Err(DurableError::Validation(format!(
            "workspace site {} is not an Effect",
            request.site_id
        )));
    };
    if effect != &request.operation || occurrence != &request.occurrence_key {
        return Err(DurableError::Validation(
            "workspace request changed its Plan-declared Effect operation or occurrence".to_owned(),
        ));
    }
    let contract = plan
        .candidate
        .effects
        .iter()
        .find(|contract| contract.id == request.operation)
        .ok_or_else(|| {
            workspace_integrity(
                "agent_workspace_effect_contract_missing",
                "workspace operation has no sealed Effect contract",
            )
        })?;
    if contract.profile.mutation != cymule_core::MutationKind::Mutating {
        return Err(DurableError::Validation(
            "workspace commit authority requires a mutating Effect contract".to_owned(),
        ));
    }
    Ok(())
}

fn workspace_integrity(code: &str, message: impl Into<String>) -> DurableError {
    DurableError::Integrity {
        code: code.to_owned(),
        message: message.into(),
    }
}

fn require_workspace_revision(command: &AgentCommand, current: &str) -> DurableResult<()> {
    if command.source_revision != current {
        return Err(DurableError::Conflict {
            expected: Some(command.source_revision.clone()),
            current: Some(current.to_owned()),
        });
    }
    Ok(())
}

fn require_workspace_frame(
    read: &ExecutorStepRead,
    request: &WorkspaceScopeRequest,
) -> DurableResult<()> {
    let continuation = &read.run.continuation;
    let frame = continuation.frames.last().ok_or_else(|| {
        DurableError::Validation("workspace requires an active execution frame".to_owned())
    })?;
    if continuation.status != ContinuationStatus::Running
        || continuation.execution_claim.is_none()
        || read.run.run.active_attempt_id.as_deref()
            != continuation
                .execution_claim
                .as_ref()
                .map(|claim| claim.continuation_attempt_id.as_str())
        || frame.scope_id != request.scope_id
        || frame.invocation_id != request.invocation_id
        || read.current_scope.scope_id != request.scope_id
        || read.current_scope.status != cymule_core::ScopeStatus::Open
    {
        return Err(DurableError::Validation(
            "workspace requires its exact currently executing open Scope".to_owned(),
        ));
    }
    Ok(())
}

fn workspace_scope_commit_continuation(
    read: &ExecutorStepRead,
) -> DurableResult<(Continuation, Vec<cymule_core::ArtifactRecord>)> {
    let frame = read
        .run
        .continuation
        .frames
        .last()
        .ok_or_else(|| DurableError::Validation("workspace scope has no frame".to_owned()))?;
    let definition = read
        .run
        .plan
        .candidate
        .definitions
        .iter()
        .find(|definition| definition.id == frame.definition_id)
        .ok_or_else(|| {
            workspace_integrity(
                "agent_workspace_definition_missing",
                "workspace frame has no admitted definition",
            )
        })?;
    let region = region_at_path(&definition.body, &frame.region_path)?;
    if frame.next_step != region.steps.len() {
        return Err(DurableError::Validation(
            "workspace Effect must be the final step of its exact Scope body".to_owned(),
        ));
    }
    let (boundary, artifacts) = if read.run.continuation.frames.len() == 1 {
        (
            crate::executor::ExecutorCoreBoundary::CommitRootScope,
            Vec::new(),
        )
    } else {
        let value = executor_evaluate(read, &region.result)?;
        let bytes = cymule_core::canonical_bytes(&value)?;
        let result = cymule_core::ArtifactRecord {
            reference: cymule_core::artifact_ref(SCOPE_RESULT_ARTIFACT_KIND, &bytes)?,
            bytes,
        };
        (
            crate::executor::ExecutorCoreBoundary::CommitScope {
                result: result.clone(),
            },
            vec![result],
        )
    };
    let derived = derive_executor_boundary(read, &boundary, None, None)?;
    Ok((derived.next, artifacts))
}

fn workspace_abort_continuation(read: &ExecutorStepRead) -> DurableResult<Continuation> {
    let source = &read.run.continuation;
    if read.current_scope.effect_count != 0 {
        return Err(DurableError::Validation(
            "workspace abort requires a child Scope with no pre-existing Effect neighborhood"
                .to_owned(),
        ));
    }
    if source.frames.len() < 2 || source.scope_stack.len() < 2 {
        return Err(DurableError::Validation(
            "workspace abort requires a child Scope with a legal parent continuation".to_owned(),
        ));
    }
    let parent_index = source.frames.len() - 2;
    let parent = &source.frames[parent_index];
    let definition = read
        .run
        .plan
        .candidate
        .definitions
        .iter()
        .find(|definition| definition.id == parent.definition_id)
        .ok_or_else(|| {
            workspace_integrity(
                "agent_workspace_abort_parent_definition_missing",
                "workspace abort parent definition is absent",
            )
        })?;
    let step = region_at_path(&definition.body, &parent.region_path)?
        .steps
        .get(parent.next_step)
        .ok_or_else(|| {
            workspace_integrity(
                "agent_workspace_abort_parent_step_missing",
                "workspace abort parent no longer points at its Scope",
            )
        })?;
    if !matches!(step.operation, Operation::Scope { bind: None, .. }) {
        return Err(DurableError::Validation(
            "workspace abort cannot manufacture a required Scope result binding".to_owned(),
        ));
    }
    let mut next = source.clone();
    next.frames.pop();
    next.scope_stack.pop();
    let parent = &mut next.frames[parent_index];
    parent.next_step = parent.next_step.checked_add(1).ok_or_else(|| {
        DurableError::Validation("workspace abort parent step overflowed".to_owned())
    })?;
    next.verify_wire()?;
    crate::validate_continuation_plan_frames(&read.run.plan, &next)?;
    Ok(next)
}

fn workspace_batch_command(
    request: &WorkspaceScopeRequest,
    phase: AgentWorkspaceCommandPhase,
    command: Command,
    precondition: MachinePinnedBatchPrecondition,
) -> DurableResult<MachinePinnedBatchCommand> {
    Ok(MachinePinnedBatchCommand {
        command_id: agent_protocol::agent_workspace_command_id(request, phase)?,
        actor: DURABLE_RUNTIME_ACTOR.to_owned(),
        run_id: request.run_id.clone(),
        precondition,
        command,
    })
}

fn workspace_abort_command(
    request: &WorkspaceScopeRequest,
    run: &MachineRunCurrent,
) -> DurableResult<MachinePinnedBatchCommand> {
    workspace_batch_command(
        request,
        AgentWorkspaceCommandPhase::SettleAbortApplied,
        Command::AbortScope {
            scope_id: request.scope_id.clone(),
        },
        MachinePinnedBatchPrecondition::Parent(Some(run.precondition_token())),
    )
}

fn workspace_effect_start_commands(
    workspace: &AgentWorkspaceCommand,
    run: &MachineRunCurrent,
    proposal: Command,
) -> DurableResult<Vec<MachinePinnedBatchCommand>> {
    let AgentWorkspaceCommand::StartEffect {
        request,
        effect_intent_id,
        ..
    } = workspace
    else {
        return Err(DurableError::Validation(
            "workspace is not StartEffect".to_owned(),
        ));
    };
    let phases = [
        (AgentWorkspaceCommandPhase::ProposeEffect, proposal),
        (
            AgentWorkspaceCommandPhase::PrepareEffect,
            Command::TransitionEffect {
                intent_id: effect_intent_id.clone(),
                transition: EffectTransition::Prepare,
            },
        ),
        (
            AgentWorkspaceCommandPhase::CommitScope,
            Command::CommitScope {
                scope_id: request.scope_id.clone(),
            },
        ),
        (
            AgentWorkspaceCommandPhase::AuthorizeEffect,
            Command::TransitionEffect {
                intent_id: effect_intent_id.clone(),
                transition: EffectTransition::AuthorizeRelease,
            },
        ),
        (
            AgentWorkspaceCommandPhase::StartEffectDispatch,
            Command::TransitionEffect {
                intent_id: effect_intent_id.clone(),
                transition: EffectTransition::StartDispatch,
            },
        ),
    ];
    phases
        .into_iter()
        .enumerate()
        .map(|(index, (phase, command))| {
            workspace_batch_command(
                request,
                phase,
                command,
                if index == 0 {
                    MachinePinnedBatchPrecondition::Parent(Some(run.precondition_token()))
                } else {
                    MachinePinnedBatchPrecondition::Derived
                },
            )
        })
        .collect()
}

fn workspace_material(
    command: &AgentCommand,
    artifacts: Vec<cymule_core::ArtifactRecord>,
) -> DurableResult<Option<MachineMaterialAdmission>> {
    if artifacts.is_empty() {
        return Ok(None);
    }
    Ok(Some(MachineMaterialAdmission::new(
        command.command_id.clone(),
        Vec::new(),
        artifacts,
    )?))
}

fn workspace_occurrence_intent(occurrence: &AgentHostOccurrence) -> DurableResult<Option<String>> {
    let AgentHostRequest::Workspace(request) = &occurrence.request else {
        return Err(workspace_integrity(
            "agent_workspace_request_missing",
            "workspace occurrence lost its typed request",
        ));
    };
    let owner = request.m1_owner().ok_or_else(|| {
        workspace_integrity(
            "agent_workspace_owner_missing",
            "workspace occurrence lost its exact M1 owner",
        )
    })?;
    Ok(owner.effect_intent_id.clone())
}

fn verify_workspace_effect_origin(
    request: &WorkspaceScopeRequest,
    occurrence: &AgentHostOccurrence,
    read: &ExecutorEffectRead,
) -> DurableResult<()> {
    let effect = &read.effect;
    if effect.scope_id != request.scope_id
        || effect.invocation_id != request.invocation_id
        || effect.site_id != request.site_id
        || effect.occurrence != request.occurrence_key
        || effect.operation != request.operation
        || effect.args != request.overlay
        || effect.profile.mutation != cymule_core::MutationKind::Mutating
    {
        return Err(workspace_integrity(
            "agent_workspace_effect_origin_mismatch",
            "workspace settlement changed its exact original Effect authority",
        ));
    }
    occurrence.occurrence_binding.verify_m1_effect_operation(
        &effect.execution_binding,
        &effect.operation,
        &effect.occurrence_binding,
    )?;
    Ok(())
}

fn unchanged_workspace(
    command: &AgentCommand,
    revision: &str,
    current: &agent_protocol::AgentOccurrenceCurrent,
) -> DurableResult<AgentWorkspaceCommitOutcome> {
    let outcome = AgentWorkspaceCommitOutcome::Unchanged {
        command_id: command.command_id.clone(),
        observed_revision: revision.to_owned(),
        current: Box::new(current.clone()),
    };
    outcome.verify_for(command)?;
    Ok(outcome)
}

fn workspace_effect_observation(
    occurrence: &AgentHostOccurrence,
) -> DurableResult<(
    WorldOutcome,
    ReconciliationResolution,
    Option<cymule_core::ArtifactRecord>,
)> {
    match occurrence.state {
        AgentHostOccurrenceState::Completed => {
            let Some(AgentHostResponse::Workspace(receipt)) = &occurrence.response else {
                return Err(workspace_integrity(
                    "agent_workspace_terminal_response_missing",
                    "completed workspace has no exact provider receipt",
                ));
            };
            let bytes = cymule_core::canonical_bytes(receipt)?;
            let record = cymule_core::ArtifactRecord {
                reference: cymule_core::artifact_ref(EFFECT_RESULT_ARTIFACT_KIND, &bytes)?,
                bytes,
            };
            Ok((
                WorldOutcome::Applied,
                ReconciliationResolution::ResolvedApplied,
                Some(record),
            ))
        }
        AgentHostOccurrenceState::NotApplied => Ok((
            WorldOutcome::NotApplied,
            ReconciliationResolution::ResolvedNotApplied,
            None,
        )),
        AgentHostOccurrenceState::Unknown => Ok((
            WorldOutcome::Unknown,
            ReconciliationResolution::StillUnknown,
            None,
        )),
        AgentHostOccurrenceState::Prepared | AgentHostOccurrenceState::Started => {
            Err(DurableError::Validation(
                "workspace observer returned no terminal or unknown observation".to_owned(),
            ))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::coordinator::PinnedStartRunOutcome;
    use crate::model::COMPONENT_INPUT_ARTIFACT_KIND;
    use crate::{ContinuationExecutionClaim, ExecutionClaimRequest, execution_clock_scope};
    use agent_protocol::{
        AgentOccurrenceResolution, AgentWorkspaceObservation, AgentWorkspaceSubmission,
        WorkspaceScopeCheckpoint,
    };
    use cymule_core::{
        Definition, EffectContract, EffectProfile, Expression, PlanCandidate, Region, Step,
        content_id,
    };
    use cymule_durable_protocol::{ClockObservationRef, FrameState};
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};

    const RUN: &str = "run:workspace-test";
    const SESSION: &str = "session:workspace-test";

    struct IssuedWorkspaceClock {
        observation: ClockObservation,
        held: Arc<AtomicBool>,
        resolves: usize,
        guards: usize,
        fail_after_commit: bool,
    }

    impl IssuedWorkspaceClock {
        fn new() -> Self {
            let generation = content_id("test.workspace-clock-generation/1", &()).unwrap();
            let scope = execution_clock_scope(RUN).unwrap();
            let observation = ClockObservation {
                clock_version: cymule_durable_protocol::CLOCK_OBSERVATION_VERSION.to_owned(),
                observation_id: cymule_durable_protocol::clock_observation_id(
                    "clock:workspace-test",
                    &generation,
                    &scope,
                    20,
                    20,
                )
                .unwrap(),
                source_id: "clock:workspace-test".to_owned(),
                source_generation: generation,
                scope,
                logical_time: 20,
                observed_unix_ms: 20,
            };
            Self {
                observation,
                held: Arc::new(AtomicBool::new(false)),
                resolves: 0,
                guards: 0,
                fail_after_commit: false,
            }
        }
    }

    impl crate::ClockObservationAuthority for IssuedWorkspaceClock {
        fn resolve(&mut self, reference: &ClockObservationRef) -> DurableResult<ClockObservation> {
            self.resolves += 1;
            if self.observation.reference() != *reference {
                return Err(DurableError::NotFound(
                    "test Clock reference was not issued".to_owned(),
                ));
            }
            Ok(self.observation.clone())
        }
    }

    impl crate::ExecutionClockAuthority for IssuedWorkspaceClock {
        fn with_current_head(
            &mut self,
            reference: &ClockObservationRef,
            commit: &mut dyn FnMut(&ClockObservation) -> DurableResult<()>,
        ) -> DurableResult<()> {
            self.guards += 1;
            if self.observation.reference() != *reference {
                return Err(DurableError::Validation(
                    "test Clock head is stale".to_owned(),
                ));
            }
            assert!(!self.held.swap(true, Ordering::SeqCst));
            let result = commit(&self.observation);
            self.held.store(false, Ordering::SeqCst);
            result?;
            if self.fail_after_commit {
                return Err(DurableError::Substrate {
                    code: "test_clock_post_cas_failure".to_owned(),
                    message: "Clock acknowledgement was lost".to_owned(),
                });
            }
            Ok(())
        }
    }

    #[derive(Clone, Copy)]
    enum CommitFault {
        BeforeHead,
        LostAcknowledgement,
    }

    struct WorkspaceCommitFaultStore {
        inner: crate::MemoryStore,
        fault: Option<CommitFault>,
        clock_held: Arc<AtomicBool>,
    }

    impl DurableStore for WorkspaceCommitFaultStore {
        fn load_head(&mut self) -> DurableResult<Option<crate::StoreHead>> {
            self.inner.load_head()
        }
        fn load_state_root_manifest(
            &mut self,
            id: &str,
        ) -> DurableResult<Option<crate::StateRootManifest>> {
            self.inner.load_state_root_manifest(id)
        }
        fn with_state_root_resolver<T>(
            &mut self,
            manifest: &crate::StateRootManifest,
            read: impl FnOnce(&mut dyn crate::StateRootResolver) -> DurableResult<T>,
        ) -> DurableResult<T> {
            self.inner.with_state_root_resolver(manifest, read)
        }
        fn application_journal_prefix(
            &mut self,
            manifest: &crate::StateRootManifest,
            id: &str,
            count: u64,
        ) -> DurableResult<crate::ApplicationJournalPrefix> {
            self.inner.application_journal_prefix(manifest, id, count)
        }
        fn application_journal_record_manifest(
            &mut self,
            manifest: &crate::StateRootManifest,
            journal: &str,
            record: &str,
        ) -> DurableResult<Option<crate::JournalRecordManifest>> {
            self.inner
                .application_journal_record_manifest(manifest, journal, record)
        }
        fn application_journal_prefix_replacement_authority(
            &mut self,
            manifest: &crate::StateRootManifest,
            id: &str,
        ) -> DurableResult<Option<crate::ApplicationJournalPrefixReplacementAuthority>> {
            self.inner
                .application_journal_prefix_replacement_authority(manifest, id)
        }
        fn coupled_checkpoint_receipt(
            &mut self,
            manifest: &crate::StateRootManifest,
            id: &str,
        ) -> DurableResult<Option<CoupledCheckpointReceipt>> {
            self.inner.coupled_checkpoint_receipt(manifest, id)
        }
        fn load_machine_command_archive_segment(
            &mut self,
            id: &str,
        ) -> DurableResult<Option<cymule_core::MachineCommandArchiveSegment>> {
            self.inner.load_machine_command_archive_segment(id)
        }
        fn load_machine_command_archive_entry(
            &mut self,
            id: &str,
        ) -> DurableResult<Option<cymule_core::MachineCommandArchiveEntry>> {
            self.inner.load_machine_command_archive_entry(id)
        }
        fn load_machine_command_archive_batch(
            &mut self,
            id: &str,
        ) -> DurableResult<Option<cymule_core::MachineCommandBatchRecord>> {
            self.inner.load_machine_command_archive_batch(id)
        }
        fn load_machine_command_index_node(
            &mut self,
            id: &str,
        ) -> DurableResult<Option<cymule_core::MachineCommandIndexNode>> {
            self.inner.load_machine_command_index_node(id)
        }
        fn reconcile_cold_reclamation(
            &mut self,
            request: &crate::StoreReclamation,
        ) -> DurableResult<crate::GcReceipt> {
            self.inner.reconcile_cold_reclamation(request)
        }
        fn advance_cold_reclamation(
            &mut self,
            request: &crate::StoreReclamation,
        ) -> DurableResult<crate::GcReceipt> {
            self.inner.advance_cold_reclamation(request)
        }
        fn stats(&self) -> DurableResult<crate::StoreStats> {
            self.inner.stats()
        }
        fn compare_and_commit(
            &mut self,
            expected: Option<&crate::StoreHead>,
            batch: &crate::StoreBatch,
        ) -> DurableResult<crate::StoreCommit> {
            assert!(
                self.clock_held.load(Ordering::SeqCst),
                "the current Clock guard encloses the real workspace Store CAS"
            );
            let fault = self.fault.take();
            if matches!(fault, Some(CommitFault::BeforeHead)) {
                return Err(DurableError::Substrate {
                    code: "workspace_test_pre_head_failure".to_owned(),
                    message: "Store rejected the batch before publishing".to_owned(),
                });
            }
            let committed = self.inner.compare_and_commit(expected, batch)?;
            if matches!(fault, Some(CommitFault::LostAcknowledgement)) {
                return Err(DurableError::CommitOutcomeUnknown {
                    message: "workspace Store head committed but its acknowledgement was lost"
                        .to_owned(),
                });
            }
            Ok(committed)
        }
    }

    struct WorkspaceProviders {
        held: Arc<AtomicBool>,
        binds: usize,
        dispatches: usize,
        observes: usize,
        fail_dispatch: bool,
        submission: AgentWorkspaceSubmission,
        concurrent_store: Option<crate::MemoryStore>,
        observation: Option<AgentWorkspaceObservation>,
    }

    impl WorkspaceProviders {
        fn new(clock: &IssuedWorkspaceClock) -> Self {
            Self {
                held: clock.held.clone(),
                binds: 0,
                dispatches: 0,
                observes: 0,
                fail_dispatch: false,
                submission: AgentWorkspaceSubmission::Submitted,
                concurrent_store: None,
                observation: None,
            }
        }

        fn unknown(&mut self, text: &str) {
            let record = record("test.workspace-evidence/1", text.as_bytes().to_vec());
            self.unknown_record(record);
        }

        fn unknown_record(&mut self, record: cymule_core::ArtifactRecord) {
            self.observation = Some(AgentWorkspaceObservation {
                resolution: AgentOccurrenceResolution::Unknown {
                    evidence: vec![agent_protocol::ContentBlock::Artifact {
                        artifact: record.reference.clone(),
                    }],
                },
                artifacts: vec![record],
            });
        }

        fn applied(&mut self, occurrence: &AgentHostOccurrence) {
            self.applied_record(
                occurrence,
                record("test.workspace-evidence/1", b"provider-applied".to_vec()),
            );
        }

        fn applied_record(
            &mut self,
            occurrence: &AgentHostOccurrence,
            evidence: cymule_core::ArtifactRecord,
        ) {
            let AgentHostRequest::Workspace(request) = &occurrence.request else {
                unreachable!()
            };
            self.observation = Some(AgentWorkspaceObservation {
                resolution: AgentOccurrenceResolution::Completed {
                    response: AgentHostResponse::Workspace(agent_protocol::WorkspaceReceipt {
                        change_id: request.change().change_id.clone(),
                        committed: request.change().commit,
                        evidence: evidence.reference.clone(),
                        occurrence_binding: occurrence.occurrence_binding.binding_id().to_owned(),
                    }),
                },
                artifacts: vec![evidence],
            });
        }
    }

    impl AgentProviders for WorkspaceProviders {
        fn publish_agent_stream(
            &mut self,
            _: &agent_protocol::AgentStreamPublicationIntent,
        ) -> cymule_profile_protocol::ProtocolResult<
            agent_protocol::AgentStreamPublicationObservation,
        > {
            Err(cymule_profile_protocol::ProtocolError::Validation(
                "workspace test has no stream provider".to_owned(),
            ))
        }
        fn observe_agent_stream_publication(
            &mut self,
            _: &agent_protocol::AgentStreamPublicationIntent,
        ) -> cymule_profile_protocol::ProtocolResult<
            agent_protocol::AgentStreamPublicationObservation,
        > {
            Err(cymule_profile_protocol::ProtocolError::Validation(
                "workspace test has no stream provider".to_owned(),
            ))
        }
        fn bind_agent_workspace(
            &mut self,
            command: &AgentWorkspaceCommand,
        ) -> cymule_profile_protocol::ProtocolResult<agent_protocol::AgentHostBinding> {
            self.binds += 1;
            if let Some(store) = self.concurrent_store.take() {
                let mut other = DurableCoordinator::open(store).unwrap();
                let command = AgentCommand::new(
                    other.current_revision().unwrap(),
                    AgentCommandAction::SessionUpdate {
                        session_id: "session:concurrent-workspace-writer".to_owned(),
                        update: agent_protocol::AgentUpdate::Message {
                            update_id: "concurrent".to_owned(),
                            message: agent_protocol::AgentMessage {
                                message_id: "concurrent".to_owned(),
                                role: agent_protocol::MessageRole::User,
                                content: vec![agent_protocol::ContentBlock::Text {
                                    text: "another writer won the CAS".to_owned(),
                                }],
                            },
                        },
                    },
                )
                .unwrap();
                other.commit_agent_local(&command).unwrap();
            }
            let (AgentWorkspaceCommand::StartEffect {
                execution_binding,
                operation_occurrence_binding,
                ..
            }
            | AgentWorkspaceCommand::StartAbort {
                execution_binding,
                operation_occurrence_binding,
                ..
            }) = command
            else {
                panic!("settlement may not bind another provider");
            };
            agent_protocol::AgentHostBinding::m1_effect_operation(
                "workspace-host:test",
                execution_binding.clone(),
                command.request().operation.clone(),
                operation_occurrence_binding.clone(),
            )
        }
        fn dispatch_agent_workspace(
            &mut self,
            _: &AgentWorkspaceCommand,
            occurrence: &AgentHostOccurrence,
        ) -> cymule_profile_protocol::ProtocolResult<AgentWorkspaceSubmission> {
            assert!(
                !self.held.load(Ordering::SeqCst),
                "dispatch occurs only after the Clock guard returns"
            );
            assert_eq!(occurrence.state, AgentHostOccurrenceState::Started);
            self.dispatches += 1;
            if self.fail_dispatch {
                return Err(cymule_profile_protocol::ProtocolError::Substrate {
                    code: "test_workspace_dispatch_lost".to_owned(),
                    message: "submission response was lost".to_owned(),
                });
            }
            Ok(self.submission)
        }
        fn observe_agent_workspace(
            &mut self,
            _: &AgentWorkspaceCommand,
            _: &AgentHostOccurrence,
        ) -> cymule_profile_protocol::ProtocolResult<AgentWorkspaceObservation> {
            self.observes += 1;
            Ok(self
                .observation
                .clone()
                .expect("test configures exact observer output"))
        }
    }

    #[derive(Clone, Copy)]
    enum ScopeShape {
        Root,
        Child,
        BoundChild,
    }

    struct WorkspaceFixture {
        coordinator: DurableCoordinator<crate::MemoryStore>,
        clock: IssuedWorkspaceClock,
        request: WorkspaceScopeRequest,
    }

    fn record(kind: &str, bytes: Vec<u8>) -> cymule_core::ArtifactRecord {
        cymule_core::ArtifactRecord {
            reference: cymule_core::artifact_ref(kind, &bytes).unwrap(),
            bytes,
        }
    }

    fn workspace_plan(shape: ScopeShape, trailing: bool) -> SealedPlan {
        let mut steps = vec![
            Step {
                id: "prepare".to_owned(),
                operation: Operation::Call {
                    component: "prepare".to_owned(),
                    input: Expression::Input,
                    bind: Some("overlay".to_owned()),
                },
            },
            Step {
                id: "commit".to_owned(),
                operation: Operation::Effect {
                    effect: "workspace.commit".to_owned(),
                    input: Expression::Binding {
                        name: "overlay".to_owned(),
                    },
                    occurrence: "primary".to_owned(),
                    bind: None,
                },
            },
        ];
        if trailing {
            steps.push(Step {
                id: "later".to_owned(),
                operation: Operation::Call {
                    component: "prepare".to_owned(),
                    input: Expression::Input,
                    bind: None,
                },
            });
        }
        let child = Region {
            steps,
            result: Expression::Literal {
                value: serde_json::Value::Null,
            },
        };
        let body = match shape {
            ScopeShape::Root => child,
            ScopeShape::Child | ScopeShape::BoundChild => Region {
                steps: vec![Step {
                    id: "scope".to_owned(),
                    operation: Operation::Scope {
                        body: Box::new(child),
                        bind: matches!(shape, ScopeShape::BoundChild)
                            .then(|| "scope-result".to_owned()),
                    },
                }],
                result: Expression::Literal {
                    value: serde_json::Value::Null,
                },
            },
        };
        cymule_core::seal_plan(PlanCandidate {
            ir_version: cymule_core::IR_VERSION.to_owned(),
            name: "workspace-test".to_owned(),
            entry: "main".to_owned(),
            components: vec![cymule_core::ComponentContract {
                id: "prepare".to_owned(),
                input_schema: serde_json::Value::Bool(true),
                output_schema: serde_json::Value::Bool(true),
                output_artifact_kind: cymule_core::EFFECT_ARGS_ARTIFACT_KIND.to_owned(),
                requirements: BTreeMap::new(),
            }],
            effects: vec![EffectContract {
                id: "workspace.commit".to_owned(),
                input_schema: serde_json::Value::Bool(true),
                output_schema: serde_json::Value::Bool(true),
                profile: EffectProfile {
                    mutation: cymule_core::MutationKind::Mutating,
                    dispatch: cymule_core::DispatchPolicy::OnScopeCommit,
                    reconciliation: cymule_core::ReconciliationMode::Queryable,
                    keyed_idempotency: true,
                    irreversible: false,
                },
                requirements: BTreeMap::new(),
            }],
            definitions: vec![Definition {
                id: "main".to_owned(),
                input_schema: serde_json::Value::Bool(true),
                output_schema: serde_json::Value::Bool(true),
                body,
            }],
            metadata: BTreeMap::new(),
        })
        .unwrap()
    }

    fn workspace_binding() -> ExecutionBinding {
        ExecutionBinding::for_local_process(
            &cymule_runtime::PluginManifest {
                plugin_version: cymule_runtime::PLUGIN_VERSION.to_owned(),
                implementation_id: "workspace-test".to_owned(),
                components: BTreeMap::from([(
                    "prepare".to_owned(),
                    cymule_runtime::PluginOperation {
                        implementation_revision: "1".to_owned(),
                    },
                )]),
                effects: BTreeMap::from([(
                    "workspace.commit".to_owned(),
                    cymule_runtime::PluginEffect {
                        implementation_revision: "1".to_owned(),
                        can_reconcile: true,
                    },
                )]),
            },
            content_id("test.workspace-runtime/1", &()).unwrap(),
        )
        .unwrap()
    }

    fn begin_workspace_run(
        coordinator: &mut DurableCoordinator<crate::MemoryStore>,
        clock: &mut IssuedWorkspaceClock,
        plan: SealedPlan,
    ) -> ContinuationExecutionClaim {
        let binding = workspace_binding();
        let binding = record(
            cymule_runtime::EXECUTION_BINDING_VERSION,
            binding.canonical_bytes().unwrap(),
        );
        let input = record(
            cymule_core::RUN_INPUT_ARTIFACT_KIND,
            cymule_core::canonical_bytes(&serde_json::json!({"prepared":true})).unwrap(),
        );
        let invocation = cymule_core::plan_invocation_id(RUN, &plan.plan_id, "main", &[]).unwrap();
        let continuation = Continuation {
            continuation_version: cymule_durable_protocol::CONTINUATION_STATE_VERSION.to_owned(),
            run_id: RUN.to_owned(),
            plan_id: plan.plan_id.clone(),
            binding_context: binding.reference.artifact_id.clone(),
            frames: vec![FrameState {
                definition_id: "main".to_owned(),
                invocation_id: invocation,
                invocation_path: Vec::new(),
                scope_id: cymule_core::ROOT_SCOPE_ID.to_owned(),
                input: input.reference.clone(),
                region_path: Vec::new(),
                next_step: 0,
                locals: BTreeMap::new(),
            }],
            state: Some(input.reference.clone()),
            wait_set: BTreeSet::new(),
            scope_stack: vec![cymule_core::ROOT_SCOPE_ID.to_owned()],
            epoch: 0,
            execution_fence: 0,
            execution_claim: None,
            status: ContinuationStatus::Ready,
        };
        let execution = ExecutionClaimRequest {
            owner: "executor:workspace-test".to_owned(),
            clock: clock.observation.reference(),
            ttl: 100,
        };
        let reference = execution.clock.clone();
        match coordinator
            .with_current_clock(clock, &reference, |coordinator, observation| {
                coordinator.start_run_pinned(
                    plan,
                    binding,
                    input,
                    continuation,
                    &execution,
                    observation,
                )
            })
            .unwrap()
        {
            PinnedStartRunOutcome::Committed(claim) => *claim,
            PinnedStartRunOutcome::Replayed => panic!("test Run starts once"),
        }
    }

    // Fixture setup uses the same typed Run, Scope, and Component boundaries
    // as the runtime. The prepared overlay is never inserted by a raw delta.
    fn workspace_fixture(shape: ScopeShape, trailing: bool) -> WorkspaceFixture {
        let mut coordinator = DurableCoordinator::open(crate::MemoryStore::new())
            .unwrap()
            .initialize()
            .unwrap();
        let mut clock = IssuedWorkspaceClock::new();
        let claim = begin_workspace_run(
            &mut coordinator,
            &mut clock,
            workspace_plan(shape, trailing),
        );
        if !matches!(shape, ScopeShape::Root) {
            let read = coordinator.read_execution_step(RUN).unwrap();
            coordinator
                .commit_executor_boundary(
                    &claim,
                    &read.run.revision,
                    &read.run.continuation,
                    &crate::executor::ExecutorCoreBoundary::OpenScope,
                )
                .unwrap();
        }
        let read = coordinator.read_execution_step(RUN).unwrap();
        let bytes = cymule_core::canonical_bytes(&serde_json::json!({"prepared":true})).unwrap();
        let input = record(COMPONENT_INPUT_ARTIFACT_KIND, bytes.clone());
        let attempt = match coordinator
            .commit_component_attempt_pinned(
                &claim,
                &read.run.revision,
                &read.run.continuation,
                input,
            )
            .unwrap()
        {
            crate::executor::ExecutorComponentAttemptAdmission::Admitted(attempt) => attempt,
            crate::executor::ExecutorComponentAttemptAdmission::InFlight(_) => {
                panic!("test component is fresh")
            }
        };
        let overlay = record(cymule_core::EFFECT_ARGS_ARTIFACT_KIND, bytes);
        coordinator
            .commit_component_result_pinned(
                &claim,
                &attempt.attempt_id,
                &crate::executor::ExecutorComponentResult::Succeeded {
                    output: overlay.clone(),
                },
            )
            .unwrap();
        let session = AgentCommand::new(
            coordinator.current_revision().unwrap(),
            AgentCommandAction::SessionUpdate {
                session_id: SESSION.to_owned(),
                update: agent_protocol::AgentUpdate::Message {
                    update_id: "seed".to_owned(),
                    message: agent_protocol::AgentMessage {
                        message_id: "seed".to_owned(),
                        role: agent_protocol::MessageRole::User,
                        content: vec![agent_protocol::ContentBlock::Text {
                            text: "workspace".to_owned(),
                        }],
                    },
                },
            },
        )
        .unwrap();
        coordinator.commit_agent_local(&session).unwrap();
        let read = coordinator.read_execution_step(RUN).unwrap();
        let frame = read.run.continuation.frames.last().unwrap();
        let request = WorkspaceScopeRequest {
            session_id: SESSION.to_owned(),
            run_id: RUN.to_owned(),
            scope_id: frame.scope_id.clone(),
            occurrence_id: "occurrence:workspace-test".to_owned(),
            change_id: "change:workspace-test".to_owned(),
            overlay: overlay.reference,
            operation: "workspace.commit".to_owned(),
            invocation_id: frame.invocation_id.clone(),
            site_id: "commit".to_owned(),
            occurrence_key: "primary".to_owned(),
            dispatch_lease: None,
        };
        WorkspaceFixture {
            coordinator,
            clock,
            request,
        }
    }

    impl WorkspaceFixture {
        fn sequence(&self) -> u64 {
            self.coordinator.pinned.as_ref().unwrap().head.sequence
        }
        fn events(&self) -> u64 {
            self.coordinator
                .pinned
                .as_ref()
                .unwrap()
                .manifest
                .machine_frontier()
                .event_count
        }
        fn start(&mut self, effect: bool) -> AgentCommand {
            let mut request = self.request.clone();
            if effect {
                request.dispatch_lease = Some(
                    agent_protocol::AgentWorkspaceDispatchLeaseRequest::new(
                        &request,
                        self.clock.observation.reference(),
                        10,
                    )
                    .unwrap(),
                );
            }
            let read = self
                .coordinator
                .read_agent_workspace_admission(&agent_protocol::AgentWorkspaceAdmissionQuery {
                    request: request.clone(),
                    decision: if effect {
                        agent_protocol::AgentWorkspaceDecision::Commit
                    } else {
                        agent_protocol::AgentWorkspaceDecision::Abort
                    },
                    expected_revision: None,
                })
                .unwrap();
            let workspace = if effect {
                AgentWorkspaceCommand::StartEffect {
                    request,
                    effect_intent_id: read
                        .host_request
                        .m1_owner()
                        .unwrap()
                        .effect_intent_id
                        .clone()
                        .unwrap(),
                    execution_binding: read.execution_binding,
                    operation_occurrence_binding: read.operation_occurrence_binding,
                }
            } else {
                AgentWorkspaceCommand::StartAbort {
                    request,
                    execution_binding: read.execution_binding,
                    operation_occurrence_binding: read.operation_occurrence_binding,
                }
            };
            AgentCommand::new(
                read.revision,
                AgentCommandAction::Workspace(Box::new(workspace)),
            )
            .unwrap()
        }
        fn settle(&self, effect: bool) -> AgentCommand {
            let workspace = if effect {
                AgentWorkspaceCommand::SettleEffect {
                    request: self.request.clone(),
                }
            } else {
                AgentWorkspaceCommand::SettleAbort {
                    request: self.request.clone(),
                }
            };
            AgentCommand::new(
                self.coordinator.current_revision().unwrap(),
                AgentCommandAction::Workspace(Box::new(workspace)),
            )
            .unwrap()
        }
        fn commit(
            &mut self,
            command: &AgentCommand,
            providers: &mut WorkspaceProviders,
        ) -> DurableResult<AgentWorkspaceCommitOutcome> {
            self.coordinator
                .commit_agent_workspace(command, providers, &mut self.clock)
        }
        fn reopen(&mut self) {
            self.coordinator = DurableCoordinator::open(self.coordinator.store.clone()).unwrap();
        }
    }

    fn checkpoint(outcome: &AgentWorkspaceCommitOutcome) -> &WorkspaceScopeCheckpoint {
        let AgentWorkspaceCommitOutcome::Committed { commit } = outcome else {
            panic!("test expects a committed checkpoint")
        };
        let AgentCommandOutcome::Workspace(checkpoint) = &commit.receipt.outcome else {
            panic!("test expects a workspace checkpoint")
        };
        checkpoint
    }

    #[test]
    fn workspace_root_scope_start_has_a_real_batch_and_provider_submission() {
        let mut fixture = workspace_fixture(ScopeShape::Root, false);
        let mut providers = WorkspaceProviders::new(&fixture.clock);
        let start = fixture.start(true);
        let before = (fixture.sequence(), fixture.events());
        let result = fixture.commit(&start, &mut providers).unwrap();
        assert_eq!(
            (fixture.sequence(), fixture.events()),
            (before.0 + 1, before.1 + 5)
        );
        assert_eq!(providers.dispatches, 1);
        assert_eq!(
            checkpoint(&result).occurrence.current.occurrence.state,
            AgentHostOccurrenceState::Started
        );
        fixture.reopen();
        fixture
            .coordinator
            .verify_agent_workspace_origin(&start.command_id)
            .unwrap();
    }

    #[test]
    fn workspace_start_is_one_cas_and_exact_replay_never_dispatches_again() {
        let mut fixture = workspace_fixture(ScopeShape::Child, false);
        let mut providers = WorkspaceProviders::new(&fixture.clock);
        let start = fixture.start(true);
        let sequence = fixture.sequence();
        let events = fixture.events();
        let first = fixture.commit(&start, &mut providers).unwrap();
        assert_eq!(fixture.sequence(), sequence + 1);
        assert_eq!(fixture.events(), events + 5);
        assert_eq!(
            (providers.binds, providers.dispatches, providers.observes),
            (1, 1, 0)
        );
        assert_eq!(
            checkpoint(&first).occurrence.current.occurrence.state,
            AgentHostOccurrenceState::Started
        );
        fixture.reopen();
        let clock_calls = (fixture.clock.resolves, fixture.clock.guards);
        let replay = fixture.commit(&start, &mut providers).unwrap();
        assert_eq!(checkpoint(&replay), checkpoint(&first));
        assert_eq!(fixture.sequence(), sequence + 1);
        assert_eq!(
            (providers.binds, providers.dispatches, providers.observes),
            (1, 1, 0)
        );
        assert_eq!((fixture.clock.resolves, fixture.clock.guards), clock_calls);
    }

    #[test]
    fn workspace_observation_new_evidence_is_atomic_and_duplicate_unknown_is_zero_cas() {
        workspace_observation_chain(ScopeShape::Child);
    }

    #[test]
    fn workspace_root_observation_material_and_terminal_settlement_reopen_exactly() {
        workspace_observation_chain(ScopeShape::Root);
    }

    #[test]
    fn workspace_not_applied_observation_replays_the_original_start_after_settlement() {
        let mut fixture = workspace_fixture(ScopeShape::Root, false);
        let mut providers = WorkspaceProviders::new(&fixture.clock);
        let start = fixture.start(true);
        let started = fixture.commit(&start, &mut providers).unwrap();
        providers.observation = Some(AgentWorkspaceObservation {
            resolution: AgentOccurrenceResolution::NotApplied {
                evidence: vec![agent_protocol::ContentBlock::Text {
                    text: "original provider proves no workspace mutation".to_owned(),
                }],
            },
            artifacts: Vec::new(),
        });
        let events = fixture.events();
        let settle = fixture.settle(true);
        let terminal = fixture.commit(&settle, &mut providers).unwrap();
        assert_eq!(fixture.events(), events + 1);
        assert_eq!(
            checkpoint(&terminal).occurrence.current.occurrence.state,
            AgentHostOccurrenceState::NotApplied
        );
        fixture.reopen();
        fixture
            .coordinator
            .verify_agent_workspace_origin(&settle.command_id)
            .unwrap();
        let before = (
            fixture.sequence(),
            providers.observes,
            fixture.clock.resolves,
        );
        let replay = fixture.commit(&start, &mut providers).unwrap();
        assert_eq!(checkpoint(&replay), checkpoint(&started));
        assert_eq!(
            (
                fixture.sequence(),
                providers.observes,
                fixture.clock.resolves
            ),
            before
        );
        assert_eq!((providers.binds, providers.dispatches), (1, 1));
    }

    #[test]
    fn workspace_abort_not_applied_keeps_the_scope_open_without_a_core_batch() {
        let mut fixture = workspace_fixture(ScopeShape::Child, false);
        let mut providers = WorkspaceProviders::new(&fixture.clock);
        let start = fixture.start(false);
        fixture.commit(&start, &mut providers).unwrap();
        let before = fixture.coordinator.read_execution_step(RUN).unwrap();
        let events = fixture.events();
        providers.observation = Some(AgentWorkspaceObservation {
            resolution: AgentOccurrenceResolution::NotApplied {
                evidence: vec![agent_protocol::ContentBlock::Text {
                    text: "original provider proves abort did not apply".to_owned(),
                }],
            },
            artifacts: Vec::new(),
        });
        let settle = fixture.settle(false);
        let terminal = fixture.commit(&settle, &mut providers).unwrap();
        assert_eq!(fixture.events(), events);
        assert_eq!(
            checkpoint(&terminal).occurrence.current.occurrence.state,
            AgentHostOccurrenceState::NotApplied
        );
        fixture.reopen();
        let after = fixture.coordinator.read_execution_step(RUN).unwrap();
        assert_eq!(after.run.continuation, before.run.continuation);
        assert_eq!(after.current_scope, before.current_scope);
        fixture
            .coordinator
            .read_current_state_root(|manifest, resolver| {
                let receipt = crate::state_root::load_coupled_checkpoint_receipt(
                    manifest,
                    resolver,
                    &agent_workspace_coupling_id(&settle.command_id)?,
                )?
                .unwrap();
                let CoupledCheckpoint::AgentWorkspace { checkpoint } = receipt.checkpoint else {
                    panic!("test expects a workspace coupled receipt")
                };
                assert!(checkpoint.core_batch_id.is_none());
                assert_eq!(
                    checkpoint.source_machine_authority_root,
                    checkpoint.machine_authority_root
                );
                Ok(())
            })
            .unwrap();
        fixture
            .coordinator
            .verify_agent_workspace_origin(&settle.command_id)
            .unwrap();
    }

    #[test]
    fn workspace_body_capacity_rejects_one_extra_byte_and_still_settles_applied() {
        let mut fixture = workspace_fixture(ScopeShape::Root, false);
        let mut providers = WorkspaceProviders::new(&fixture.clock);
        let start = fixture.start(true);
        let started = fixture.commit(&start, &mut providers).unwrap();
        let (padding, terminal_observation, probe) = workspace_body_capacity_fixture(
            &checkpoint(&started).occurrence.current.occurrence,
            &mut providers,
        );
        let before = (fixture.sequence(), fixture.events());
        providers.observation = Some(workspace_text_observation("x".repeat(padding + 1)));
        let unknown_command = fixture.settle(true);
        let error = fixture
            .commit(&unknown_command, &mut providers)
            .unwrap_err();
        assert!(error.to_string().contains("terminal occurrence capacity"));
        assert_eq!((fixture.sequence(), fixture.events()), before);
        fixture.reopen();
        let replay = fixture.commit(&start, &mut providers).unwrap();
        assert_eq!(checkpoint(&replay), checkpoint(&started));
        providers.observation = Some(workspace_text_observation("x".repeat(padding)));
        let unknown = fixture.commit(&unknown_command, &mut providers).unwrap();
        fixture.reopen();
        let replay = fixture.commit(&unknown_command, &mut providers).unwrap();
        assert_eq!(checkpoint(&replay), checkpoint(&unknown));
        providers.observation = Some(terminal_observation);
        let settled_command = fixture.settle(true);
        let settled = fixture.commit(&settled_command, &mut providers).unwrap();
        let completed = &checkpoint(&settled).occurrence.current.occurrence;
        assert_eq!(completed.state, AgentHostOccurrenceState::Completed);
        assert_eq!(
            cymule_core::canonical_bytes(completed).unwrap().len(),
            agent_protocol::MAX_AGENT_VALUE_BYTES
        );
        fixture.reopen();
        let replay = fixture.commit(&settled_command, &mut providers).unwrap();
        assert_eq!(checkpoint(&replay), checkpoint(&settled));
        fixture
            .coordinator
            .read_current_state_root(|manifest, resolver| {
                assert!(
                    crate::state_root::pinned_machine::PinnedMachineView::open(
                        manifest, resolver,
                    )?
                    .artifact(&probe.artifact_id)?
                    .is_none(),
                    "the typed capacity probe is never admitted as provider evidence"
                );
                Ok(())
            })
            .unwrap();
        assert_eq!((providers.binds, providers.dispatches), (1, 1));
    }

    fn workspace_body_capacity_fixture(
        started: &AgentHostOccurrence,
        providers: &mut WorkspaceProviders,
    ) -> (usize, AgentWorkspaceObservation, ArtifactRef) {
        let suffix = "/1";
        let kind = format!(
            "{}{suffix}",
            "a".repeat(cymule_core::MAX_ARTIFACT_KIND_BYTES - suffix.len()),
        );
        let evidence = record(&kind, b"real maximum-width terminal evidence".to_vec());
        let probe = cymule_core::artifact_ref(kind, &[]).unwrap();
        providers.applied_record(started, evidence);
        let observation = providers.observation.clone().unwrap();
        let AgentOccurrenceResolution::Completed { response } = &observation.resolution else {
            panic!("body fixture expects a real provider completion")
        };
        let seed = started
            .mark_unknown_with_evidence(
                "capacity sizing",
                vec![agent_protocol::ContentBlock::Text {
                    text: String::new(),
                }],
            )
            .unwrap();
        let completed = seed.complete(response.clone()).unwrap();
        let padding = agent_protocol::MAX_AGENT_VALUE_BYTES
            - cymule_core::canonical_bytes(&completed).unwrap().len();
        (padding, observation, probe)
    }

    fn workspace_text_observation(text: String) -> AgentWorkspaceObservation {
        AgentWorkspaceObservation {
            resolution: AgentOccurrenceResolution::Unknown {
                evidence: vec![agent_protocol::ContentBlock::Text { text }],
            },
            artifacts: Vec::new(),
        }
    }

    #[test]
    fn workspace_retained_evidence_budget_rejects_before_cas_and_keeps_exact_replay() {
        let mut fixture = workspace_fixture(ScopeShape::Root, false);
        let mut providers = WorkspaceProviders::new(&fixture.clock);
        let (last_command, original) =
            fill_workspace_retained_evidence_budget(&mut fixture, &mut providers);
        fixture.reopen();
        let replay = fixture.commit(&last_command, &mut providers).unwrap();
        assert_eq!(checkpoint(&replay), &original);
        let before = (fixture.sequence(), fixture.events());
        let overflow = record("test.workspace-evidence/1", vec![255]);
        let absent = overflow.reference.clone();
        providers.unknown_record(overflow);
        let rejected = fixture.settle(true);
        assert!(matches!(
            fixture.commit(&rejected, &mut providers),
            Err(DurableError::Validation(message))
                if message.contains("retained evidence exceeds")
        ));
        assert_eq!((fixture.sequence(), fixture.events()), before);
        fixture.reopen();
        fixture
            .coordinator
            .read_current_state_root(|manifest, resolver| {
                assert!(
                    crate::state_root::load_agent_command_receipt(
                        manifest,
                        resolver,
                        &rejected.command_id,
                    )?
                    .is_none()
                );
                assert!(
                    crate::state_root::pinned_machine::PinnedMachineView::open(
                        manifest, resolver,
                    )?
                    .artifact(&absent.artifact_id)?
                    .is_none()
                );
                Ok(())
            })
            .unwrap();
        let replay = fixture.commit(&last_command, &mut providers).unwrap();
        assert_eq!(checkpoint(&replay), &original);
        assert_eq!((fixture.sequence(), fixture.events()), before);
        providers.applied_record(
            &original.occurrence.current.occurrence,
            record(
                "test.workspace-evidence/1",
                vec![17; agent_protocol::MAX_AGENT_WORKSPACE_ARTIFACT_BYTES],
            ),
        );
        let settled = fixture.settle(true);
        let terminal = fixture.commit(&settled, &mut providers).unwrap();
        assert_eq!(
            checkpoint(&terminal).occurrence.current.occurrence.state,
            AgentHostOccurrenceState::Completed
        );
        assert_eq!(
            (fixture.sequence(), fixture.events()),
            (before.0 + 1, before.1 + 1)
        );
        fixture.reopen();
        let replay = fixture.commit(&settled, &mut providers).unwrap();
        assert_eq!(checkpoint(&replay), checkpoint(&terminal));
        assert_eq!(providers.dispatches, 1);
    }

    fn fill_workspace_retained_evidence_budget(
        fixture: &mut WorkspaceFixture,
        providers: &mut WorkspaceProviders,
    ) -> (AgentCommand, WorkspaceScopeCheckpoint) {
        let mut last_command = fixture.start(true);
        let started = fixture.commit(&last_command, providers).unwrap();
        let mut last = checkpoint(&started).clone();
        let AgentCommandAction::Workspace(workspace) = &last_command.action else {
            panic!("test expects the workspace Start command")
        };
        let (references, _) = workspace_retained_artifact_closure(
            workspace,
            &last.occurrence.current.occurrence,
            [None, None],
        )
        .unwrap();
        let mut remaining =
            workspace_retained_byte_limit(&last.occurrence.current.occurrence).unwrap();
        for reference in references {
            remaining -= fixture
                .coordinator
                .workspace_artifact(&reference)
                .unwrap()
                .bytes
                .len();
        }
        let expected_observations =
            remaining.div_ceil(agent_protocol::MAX_AGENT_WORKSPACE_ARTIFACT_BYTES);
        let mut marker = 1_u8;
        while remaining != 0 {
            let size = remaining.min(agent_protocol::MAX_AGENT_WORKSPACE_ARTIFACT_BYTES);
            let mut bytes = vec![0; size];
            bytes[0] = marker;
            providers.unknown_record(record("test.workspace-evidence/1", bytes));
            last_command = fixture.settle(true);
            let committed = fixture.commit(&last_command, providers).unwrap();
            last = checkpoint(&committed).clone();
            remaining -= size;
            marker += 1;
        }
        assert_eq!(
            last.occurrence
                .current
                .occurrence
                .recovery_observations
                .len(),
            expected_observations
        );
        (last_command, last)
    }

    fn workspace_observation_chain(shape: ScopeShape) {
        let mut fixture = workspace_fixture(shape, false);
        let mut providers = WorkspaceProviders::new(&fixture.clock);
        let start = fixture.start(true);
        fixture.commit(&start, &mut providers).unwrap();
        providers.unknown("first-observation");
        let first_command = fixture.settle(true);
        let first = fixture.commit(&first_command, &mut providers).unwrap();
        assert_eq!(
            checkpoint(&first)
                .occurrence
                .current
                .occurrence
                .recovery_observations
                .len(),
            1
        );
        let sequence = fixture.sequence();
        let events = fixture.events();
        let repeated = fixture.settle(true);
        assert!(matches!(
            fixture.commit(&repeated, &mut providers).unwrap(),
            AgentWorkspaceCommitOutcome::Unchanged { .. }
        ));
        assert_eq!((fixture.sequence(), fixture.events()), (sequence, events));
        providers.unknown("second-observation");
        let second = fixture.commit(&repeated, &mut providers).unwrap();
        assert_eq!(fixture.sequence(), sequence + 1);
        assert_eq!(fixture.events(), events);
        assert_eq!(
            checkpoint(&second)
                .occurrence
                .current
                .occurrence
                .recovery_observations
                .len(),
            2
        );
        fixture.reopen();
        fixture
            .coordinator
            .verify_agent_workspace_origin(&repeated.command_id)
            .unwrap();
        providers.applied(&checkpoint(&second).occurrence.current.occurrence);
        let terminal_command = fixture.settle(true);
        let terminal = fixture.commit(&terminal_command, &mut providers).unwrap();
        assert_eq!(
            checkpoint(&terminal).occurrence.current.occurrence.state,
            AgentHostOccurrenceState::Completed
        );
        assert_eq!(providers.dispatches, 1);
        fixture.reopen();
        fixture
            .coordinator
            .verify_agent_workspace_origin(&terminal_command.command_id)
            .unwrap();
        assert_eq!(
            fixture
                .coordinator
                .read_executor_run(RUN)
                .unwrap()
                .unwrap()
                .run
                .world_settlement,
            cymule_core::WorldSettlementStatus::Settled
        );
    }

    #[test]
    fn workspace_abort_applies_only_after_observation_and_unwinds_unbound_child() {
        let mut fixture = workspace_fixture(ScopeShape::Child, false);
        let mut providers = WorkspaceProviders::new(&fixture.clock);
        let start = fixture.start(false);
        let source = fixture
            .coordinator
            .read_execution_step(RUN)
            .unwrap()
            .run
            .continuation;
        let events = fixture.events();
        let started = fixture.commit(&start, &mut providers).unwrap();
        assert_eq!(fixture.events(), events);
        assert_eq!(
            fixture
                .coordinator
                .read_execution_step(RUN)
                .unwrap()
                .run
                .continuation,
            source
        );
        providers.unknown("abort-not-yet-observed");
        let unknown_command = fixture.settle(false);
        let unknown = fixture.commit(&unknown_command, &mut providers).unwrap();
        assert_eq!(fixture.events(), events);
        assert_eq!(
            fixture
                .coordinator
                .read_execution_step(RUN)
                .unwrap()
                .run
                .continuation,
            source
        );
        providers.applied(&checkpoint(&unknown).occurrence.current.occurrence);
        let settle = fixture.settle(false);
        fixture.commit(&settle, &mut providers).unwrap();
        let target = fixture
            .coordinator
            .read_execution_step(RUN)
            .unwrap()
            .run
            .continuation;
        assert_eq!(target.frames.len(), source.frames.len() - 1);
        assert_eq!(target.scope_stack.len(), source.scope_stack.len() - 1);
        assert_eq!(fixture.events(), events + 1);
        assert_eq!(providers.dispatches, 1);
        fixture.reopen();
        let replay = fixture.commit(&start, &mut providers).unwrap();
        assert_eq!(checkpoint(&replay), checkpoint(&started));
        assert_eq!(providers.dispatches, 1);
    }

    #[test]
    fn workspace_rejects_unsupported_source_before_any_provider_or_cas() {
        for (shape, trailing, effect) in [
            (ScopeShape::Root, false, false),
            (ScopeShape::BoundChild, false, false),
            (ScopeShape::Child, true, true),
        ] {
            let mut fixture = workspace_fixture(shape, trailing);
            let mut providers = WorkspaceProviders::new(&fixture.clock);
            let command = fixture.start(effect);
            let before = (fixture.sequence(), fixture.events());
            assert!(matches!(
                fixture.commit(&command, &mut providers),
                Err(DurableError::Validation(_))
            ));
            assert_eq!((fixture.sequence(), fixture.events()), before);
            assert_eq!(
                (providers.binds, providers.dispatches, providers.observes),
                (0, 0, 0)
            );
        }
        let mut fixture = workspace_fixture(ScopeShape::Child, false);
        let providers = WorkspaceProviders::new(&fixture.clock);
        fixture.request.overlay = fixture
            .coordinator
            .read_executor_run(RUN)
            .unwrap()
            .unwrap()
            .root_input
            .reference;
        let mut request = fixture.request.clone();
        request.dispatch_lease = Some(
            agent_protocol::AgentWorkspaceDispatchLeaseRequest::new(
                &request,
                fixture.clock.observation.reference(),
                10,
            )
            .unwrap(),
        );
        let before = fixture.sequence();
        assert!(matches!(
            fixture.coordinator.read_agent_workspace_admission(
                &agent_protocol::AgentWorkspaceAdmissionQuery {
                    request,
                    decision: agent_protocol::AgentWorkspaceDecision::Commit,
                    expected_revision: None,
                }
            ),
            Err(DurableError::Validation(_))
        ));
        assert_eq!(fixture.sequence(), before);
        assert_eq!((providers.binds, providers.dispatches), (0, 0));
    }

    #[test]
    fn workspace_dispatch_failure_retains_started_and_never_retries_submission() {
        let mut fixture = workspace_fixture(ScopeShape::Child, false);
        let mut providers = WorkspaceProviders::new(&fixture.clock);
        providers.fail_dispatch = true;
        let command = fixture.start(true);
        let before = fixture.sequence();
        assert!(
            matches!(fixture.commit(&command, &mut providers), Err(DurableError::CommitOutcomeUnknown { message }) if message.contains("occurrence:workspace-test"))
        );
        assert_eq!(fixture.sequence(), before + 1);
        assert_eq!(providers.dispatches, 1);
        fixture.reopen();
        let replay = fixture.commit(&command, &mut providers).unwrap();
        assert_eq!(
            checkpoint(&replay).occurrence.current.occurrence.state,
            AgentHostOccurrenceState::Started
        );
        assert_eq!(
            (providers.binds, providers.dispatches, providers.observes),
            (1, 1, 0)
        );
    }

    #[test]
    fn workspace_clock_failure_after_cas_prevents_dispatch_and_replays_real_receipt() {
        let mut fixture = workspace_fixture(ScopeShape::Child, false);
        let mut providers = WorkspaceProviders::new(&fixture.clock);
        let command = fixture.start(true);
        fixture.clock.fail_after_commit = true;
        let before = fixture.sequence();
        assert!(matches!(
            fixture.commit(&command, &mut providers),
            Err(DurableError::CommitOutcomeUnknown { .. })
        ));
        assert_eq!(fixture.sequence(), before + 1);
        assert_eq!((providers.binds, providers.dispatches), (1, 0));
        fixture.reopen();
        let replay = fixture.commit(&command, &mut providers).unwrap();
        assert_eq!(
            checkpoint(&replay).occurrence.current.occurrence.state,
            AgentHostOccurrenceState::Started
        );
        assert_eq!((providers.binds, providers.dispatches), (1, 0));
    }

    #[test]
    fn workspace_missing_observer_bytes_and_wrong_binding_do_not_settle() {
        let mut fixture = workspace_fixture(ScopeShape::Child, false);
        let mut providers = WorkspaceProviders::new(&fixture.clock);
        let start = fixture.start(true);
        let started = fixture.commit(&start, &mut providers).unwrap();
        providers.applied(&checkpoint(&started).occurrence.current.occurrence);
        let mut complete = providers.observation.clone().unwrap();
        providers.observation.as_mut().unwrap().artifacts.clear();
        let command = fixture.settle(true);
        let before = (fixture.sequence(), fixture.events());
        assert!(matches!(
            fixture.commit(&command, &mut providers),
            Err(DurableError::Integrity { .. })
        ));
        assert_eq!((fixture.sequence(), fixture.events()), before);
        let AgentOccurrenceResolution::Completed {
            response: AgentHostResponse::Workspace(receipt),
        } = &mut complete.resolution
        else {
            unreachable!()
        };
        receipt.occurrence_binding = "binding:another".to_owned();
        providers.observation = Some(complete);
        assert!(fixture.commit(&command, &mut providers).is_err());
        assert_eq!((fixture.sequence(), fixture.events()), before);
        assert_eq!(providers.dispatches, 1);
    }

    #[test]
    fn workspace_ambiguous_submission_returns_only_started_not_a_terminal_receipt() {
        let mut fixture = workspace_fixture(ScopeShape::Root, false);
        let mut providers = WorkspaceProviders::new(&fixture.clock);
        providers.submission = AgentWorkspaceSubmission::Unknown;
        let command = fixture.start(true);
        let outcome = fixture.commit(&command, &mut providers).unwrap();
        let checkpoint = checkpoint(&outcome);
        assert_eq!(
            checkpoint.occurrence.current.occurrence.state,
            AgentHostOccurrenceState::Started
        );
        assert!(checkpoint.receipt.is_none());
        assert_eq!(providers.dispatches, 1);
    }

    #[test]
    fn workspace_stale_writer_never_publishes_a_claim_or_dispatches() {
        let mut fixture = workspace_fixture(ScopeShape::Root, false);
        let mut providers = WorkspaceProviders::new(&fixture.clock);
        let command = fixture.start(true);
        let before = (fixture.sequence(), fixture.events());
        providers.concurrent_store = Some(fixture.coordinator.store.clone());
        assert!(fixture.commit(&command, &mut providers).is_err());
        assert_eq!(providers.dispatches, 0);
        fixture.reopen();
        assert_eq!(
            (fixture.sequence(), fixture.events()),
            (before.0 + 1, before.1)
        );
        let current = fixture
            .coordinator
            .read_agent_occurrence(&agent_protocol::AgentOccurrenceQuery {
                session_id: SESSION.to_owned(),
                occurrence_id: fixture.request.occurrence_id.clone(),
                expected_revision: None,
            })
            .unwrap();
        assert!(current.current.is_none());
    }

    #[test]
    fn workspace_store_faults_preserve_atomicity_and_never_dispatch_without_acknowledgement() {
        for fault in [CommitFault::BeforeHead, CommitFault::LostAcknowledgement] {
            let mut fixture = workspace_fixture(ScopeShape::Root, false);
            let mut providers = WorkspaceProviders::new(&fixture.clock);
            let command = fixture.start(true);
            let sequence = fixture.sequence();
            let store = WorkspaceCommitFaultStore {
                inner: fixture.coordinator.store.clone(),
                fault: Some(fault),
                clock_held: fixture.clock.held.clone(),
            };
            let mut coordinator = DurableCoordinator::open(store).unwrap();
            assert!(
                coordinator
                    .commit_agent_workspace(&command, &mut providers, &mut fixture.clock)
                    .is_err()
            );
            assert_eq!(providers.dispatches, 0);
            fixture.reopen();
            match fault {
                CommitFault::BeforeHead => {
                    assert_eq!(fixture.sequence(), sequence);
                    let current = fixture
                        .coordinator
                        .read_agent_occurrence(&agent_protocol::AgentOccurrenceQuery {
                            session_id: SESSION.to_owned(),
                            occurrence_id: fixture.request.occurrence_id.clone(),
                            expected_revision: None,
                        })
                        .unwrap();
                    assert!(current.current.is_none());
                }
                CommitFault::LostAcknowledgement => {
                    assert_eq!(fixture.sequence(), sequence + 1);
                    let replay = fixture.commit(&command, &mut providers).unwrap();
                    assert_eq!(
                        checkpoint(&replay).occurrence.current.occurrence.state,
                        AgentHostOccurrenceState::Started
                    );
                    assert_eq!(providers.dispatches, 0);
                }
            }
        }
    }
}
