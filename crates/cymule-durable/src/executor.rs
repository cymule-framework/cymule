use std::collections::{BTreeMap, BTreeSet};

use cymule_core::{
    ArtifactRecord, DECLARED_FAILURE_ARTIFACT_KIND, DispatchPolicy, EFFECT_ARGS_ARTIFACT_KIND,
    EFFECT_SCHEMA_VERSION, EffectExecutionAvailability, EffectIntentIdentityInput, Expression,
    MutationKind, Operation, PlanCandidate, ROOT_SCOPE_ID, RUN_INPUT_ARTIFACT_KIND,
    ReconciliationMode, ReconciliationResolution, Region, RunFailure, RunFailureClass, Step,
    WorldOutcome, artifact_ref, canonical_bytes, effect_intent_id, plan_invocation_id, seal_plan,
};
use cymule_runtime::{
    BoundOperationAdmission, BoundPluginHost, ContractTarget, ContractValidator,
    EXECUTION_BINDING_VERSION, EffectProviderAttempt, EffectReconciliationDecision,
    ExecutionBinding, ExecutionBindingAdmission, ExecutionOperationKind, ExecutionResult,
    PlanContracts, PluginRequest, PluginResponse, RESULT_ARTIFACT_KIND, RuntimeResult,
};
use serde_json::Value;

use crate::coordinator::{
    DurableCoordinator, ExecutorEffectRead, ExecutorRunRead, ExecutorStepRead,
    PinnedStartRunOutcome,
};
use crate::model::{
    COMPONENT_INPUT_ARTIFACT_KIND, EFFECT_RESULT_ARTIFACT_KIND, INVOCATION_INPUT_ARTIFACT_KIND,
    INVOCATION_RESULT_ARTIFACT_KIND, SCOPE_RESULT_ARTIFACT_KIND, decode_artifact_value,
    derive_wait_id, evaluate_expression_with,
};
use crate::{
    ClockObservation, Continuation, ContinuationExecutionClaim, ContinuationStatus, DurableError,
    DurableResult, DurableStore, EffectDispatch, EffectResolutionCommand, EffectResolutionReceipt,
    ExecutionClaimRequest, ExecutionClockAuthority, FrameState, MAX_WAIT_DELIVERY_TARGETS,
    OperationAttempt, OutboxState, WaitActivationDisposition, WaitCondition, WaitKind,
    WaitSourceDriver, execution_clock_scope,
};

/// Result of driving one Run until its next durable boundary.
#[derive(Debug, Clone, PartialEq)]
pub enum DriveOutcome {
    /// Run parked at a durable wait.
    Suspended {
        /// Stable wait identity used to deliver completion.
        wait_id: String,
    },
    /// Effect outcome remains unknown and requires later reconciliation.
    ReconciliationRequired {
        /// Original structural effect intent.
        intent_id: String,
    },
    /// The effect's exact admitted implementation is unavailable and the
    /// original world outcome remains unresolved for governance.
    EffectUnavailable {
        /// Original structural effect intent.
        intent_id: String,
    },
    /// A bound eager effect reached the closed terminal `NotApplied` outcome and
    /// therefore has no value to bind. The execution claim is released before
    /// this boundary is returned.
    EffectNotApplied {
        /// Original structural effect intent.
        intent_id: String,
    },
    /// Run has committed scopes with effects awaiting an explicit release.
    ReleaseRequired {
        /// Structural intents that the caller may release independently.
        intent_ids: BTreeSet<String>,
    },
    /// Run reached a terminal Result.
    Completed(ExecutionResult),
    /// Run committed one typed terminal failure.
    Failed {
        /// Canonical failure classification and evidence.
        failure: RunFailure,
    },
    /// Run committed one semantic cancellation.
    Cancelled {
        /// Content-addressed cancellation reason.
        reason: cymule_core::ArtifactRef,
    },
}

/// Closed result of one identified wait-delivery admission.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WaitAdmissionOutcome {
    /// Whether the delivery won or arrived after terminal cancellation.
    pub disposition: WaitActivationDisposition,
    /// Runs made ready by an applied delivery.
    pub ready_run_ids: BTreeSet<String>,
}

/// Closed interpreter-owned Core boundary admitted through the pinned
/// coordinator. The coordinator derives every command envelope and sidecar;
/// executor code cannot submit an arbitrary Machine command or durable delta.
pub(crate) enum ExecutorCoreBoundary {
    /// Enter the exact Invoke site selected by the current Plan step.
    EnterInvocation { input: ArtifactRecord },
    /// Complete the exact invoked Definition selected by the parent frame.
    CompleteInvocation { result: ArtifactRecord },
    /// Advance one eager Effect site from its exact retained settlement.
    AdvanceSettledEffect {
        intent_id: String,
        result: Option<cymule_core::ArtifactRef>,
    },
    /// Open the child Scope selected by the current Plan step.
    OpenScope,
    /// Commit the current nested Scope and bind its exact immutable result.
    CommitScope { result: ArtifactRecord },
    /// Commit the root scope before dispatch selection or terminal completion.
    CommitRootScope,
    /// Yield the active Attempt and atomically park one typed Wait.
    ParkWait,
    /// Yield the active Attempt while keeping the Continuation ready for a
    /// later explicit effect release or reconciliation boundary.
    YieldReady { reason: ExecutorYieldReadyReason },
    /// Yield the active Attempt and complete the Run with one typed result.
    CompleteRun { result: ArtifactRecord },
}

/// Exact nonterminal authority which permits a Running Continuation to yield
/// as claim-free Ready without advancing its Plan position.
pub(crate) enum ExecutorYieldReadyReason {
    /// One exact Effect owns a terminal, unavailable, or reconciliation
    /// boundary.
    EffectBoundary { intent_id: String },
    /// The exact complete explicit-release set owns the root boundary.
    ReleaseBoundary { intent_ids: BTreeSet<String> },
}

/// Closed provider Component outcome admitted with the exact Attempt and
/// post-call Continuation. Terminal failure state is derived by the
/// coordinator from the Core transition rather than trusted from executor
/// fields.
pub(crate) enum ExecutorComponentResult {
    /// Schema-valid Component output from which the coordinator derives the
    /// unique successor interpreter position.
    Succeeded { output: ArtifactRecord },
    /// Declared expected failure and its immutable typed detail.
    ExpectedFailure {
        failure: RunFailure,
        detail: ArtifactRecord,
    },
}

/// Closed admission result for one Component provider Attempt. Only a newly
/// committed Attempt authorizes provider I/O; a retained Running Attempt is
/// deliberately ambiguous and must await explicit takeover.
pub(crate) enum ExecutorComponentAttemptAdmission {
    /// This exact CAS newly admitted the Running Attempt.
    Admitted(OperationAttempt),
    /// The Running Attempt already existed; provider invocation is forbidden.
    InFlight(OperationAttempt),
}

/// Closed Effect transition emitted after exact binding admission or provider
/// evidence. The coordinator derives the corresponding Core command and
/// outbox mutation atomically.
pub(crate) enum ExecutorEffectSettlement {
    /// First-dispatch observation under the retained claim.
    Observation {
        outcome: WorldOutcome,
        result: Option<ArtifactRecord>,
    },
    /// Queryable reconciliation under the original retained claim.
    Reconciliation {
        resolution: ReconciliationResolution,
        result: Option<ArtifactRecord>,
    },
    /// Exact selected implementation is unavailable; world outcome remains
    /// unresolved when a claim already exists.
    Unavailable,
}

struct FrameStepContext<'a> {
    run_id: &'a str,
    read: &'a ExecutorStepRead,
    claim: &'a ContinuationExecutionClaim,
    historical_binding: &'a ExecutionBinding,
    contracts: &'a PlanContracts,
    frame_index: usize,
    frame: &'a FrameState,
    region: &'a Region,
    input: Value,
}

struct EffectStepContext<'a> {
    read: &'a ExecutorStepRead,
    claim: &'a ContinuationExecutionClaim,
    historical_binding: &'a ExecutionBinding,
    contracts: &'a PlanContracts,
    effect: &'a str,
    expression: &'a Expression,
    occurrence: &'a str,
    bind: Option<&'a String>,
}

struct PreparedEffectStep {
    args: ArtifactRecord,
    value: Value,
    intent_id: String,
    execution_binding: cymule_core::ArtifactRef,
    occurrence_binding: String,
}

/// Resumable sequential interpreter backed by a provider-neutral durable store.
/// Frame paths and the scope stack are persisted so nested regions can resume
/// without reconstructing a host-language call stack.
pub(crate) struct ResumableRuntime<S, P> {
    coordinator: DurableCoordinator<S>,
    plugin: P,
    binding: ExecutionBinding,
    clock: Box<dyn ExecutionClockAuthority>,
}

/// Provider-only terminal Effect settlement runtime. It owns no execution
/// Clock because it neither acquires a Run claim nor resumes interpretation.
pub(crate) struct EffectResolutionRuntime<S, P> {
    coordinator: DurableCoordinator<S>,
    plugin: P,
    binding: ExecutionBinding,
}

impl<S: DurableStore, P: BoundPluginHost> EffectResolutionRuntime<S, P> {
    /// Open provider-linearized settlement over one exact admitted binding.
    pub(crate) fn open(store: S, admission: ExecutionBindingAdmission<P>) -> DurableResult<Self> {
        let (plugin, binding) = admission.into_parts();
        Ok(Self {
            coordinator: DurableCoordinator::open(store)?,
            plugin,
            binding,
        })
    }

    /// Linearize and persist one requested terminal Effect resolution.
    pub(crate) fn resolve_effect_with_provider(
        &mut self,
        command: &EffectResolutionCommand,
    ) -> DurableResult<EffectResolutionReceipt> {
        ResumableRuntime::<S, P>::resolve_effect_with_provider_parts(
            &mut self.coordinator,
            &mut self.plugin,
            &self.binding,
            command,
        )
    }

    /// Consume the runtime into its Store and provider.
    pub(crate) fn into_parts(self) -> (S, P) {
        (self.coordinator.into_store(), self.plugin)
    }
}

/// Return a retained terminal Effect-resolution acknowledgement without
/// constructing or invoking the historical provider. `None` means the
/// original Effect is still unknown and provider linearization is required.
pub(crate) fn replay_effect_resolution<S: DurableStore>(
    coordinator: &mut DurableCoordinator<S>,
    command: &EffectResolutionCommand,
) -> DurableResult<Option<EffectResolutionReceipt>> {
    command.verify()?;
    let Some(receipt) = coordinator.effect_resolution_receipt(&command.resolution_id)? else {
        return Ok(None);
    };
    receipt.verify()?;
    if receipt.command_matches(command) {
        return Ok(Some(receipt));
    }
    Err(DurableError::HistoryConflict {
        code: "effect_resolution_command_reused".to_owned(),
        message: format!(
            "Effect resolution identity {} was reused with different command semantics",
            command.resolution_id
        ),
    })
}

impl<S: DurableStore, P: BoundPluginHost> ResumableRuntime<S, P> {
    /// Open a durable runtime over an existing or empty store using a one-shot
    /// plugin/binding admission produced before writable Store construction.
    /// This method performs no provider I/O.
    ///
    /// # Errors
    ///
    /// Returns a durable error when the admitted Store state cannot be opened
    /// or verified.
    pub(crate) fn open<C: ExecutionClockAuthority + 'static>(
        store: S,
        admission: ExecutionBindingAdmission<P>,
        clock: C,
    ) -> DurableResult<Self> {
        let (plugin, binding) = admission.into_parts();
        Ok(Self {
            coordinator: DurableCoordinator::open(store)?,
            plugin,
            binding,
            clock: Box::new(clock),
        })
    }

    /// Borrow the immutable execution binding admitted before Store access.
    pub(crate) const fn execution_binding(&self) -> &ExecutionBinding {
        &self.binding
    }

    /// Seal and start a new Run, then drive it to wait or completion.
    pub fn start(
        &mut self,
        candidate: PlanCandidate,
        input: &Value,
        run_id: impl Into<String>,
        execution: &ExecutionClaimRequest,
    ) -> DurableResult<DriveOutcome> {
        execution.verify()?;
        let run_id = run_id.into();
        let plan = seal_plan(candidate)?;
        let contracts = PlanContracts::compile(&plan.candidate)?;
        contracts.validate_definition_input(&plan.candidate.entry, input)?;
        self.binding.admit_plan(&plan)?;
        let input_bytes = canonical_bytes(input)?;
        let binding_bytes = self.binding.canonical_bytes()?;
        let binding_record = immutable_artifact(EXECUTION_BINDING_VERSION, binding_bytes)?;
        if binding_record.reference != self.binding.artifact_ref()? {
            return Err(DurableError::Validation(
                "execution binding Artifact identity is inconsistent".to_owned(),
            ));
        }
        let input_record = immutable_artifact(RUN_INPUT_ARTIFACT_KIND, input_bytes)?;
        let root_invocation_id =
            plan_invocation_id(&run_id, &plan.plan_id, &plan.candidate.entry, &[])?;
        let continuation = Continuation {
            continuation_version: cymule_durable_protocol::CONTINUATION_STATE_VERSION.to_owned(),
            run_id: run_id.clone(),
            plan_id: plan.plan_id.clone(),
            binding_context: binding_record.reference.artifact_id.clone(),
            frames: vec![FrameState {
                definition_id: plan.candidate.entry.clone(),
                invocation_id: root_invocation_id,
                invocation_path: Vec::new(),
                scope_id: ROOT_SCOPE_ID.to_owned(),
                input: input_record.reference.clone(),
                region_path: Vec::new(),
                next_step: 0,
                locals: BTreeMap::new(),
            }],
            state: Some(input_record.reference.clone()),
            wait_set: BTreeSet::new(),
            scope_stack: vec![ROOT_SCOPE_ID.to_owned()],
            epoch: 0,
            execution_fence: 0,
            execution_claim: None,
            status: ContinuationStatus::Ready,
        };
        self.coordinator.initialize_if_empty()?;
        if self.coordinator.replay_start_run_pinned(
            plan.clone(),
            binding_record.clone(),
            input_record.clone(),
            &continuation,
        )? {
            return self.replay_started_run(&run_id);
        }
        let outcome = self.commit_with_current_execution_clock(
            &run_id,
            execution,
            move |coordinator, clock| {
                coordinator.start_run_pinned(
                    plan,
                    binding_record,
                    input_record,
                    continuation,
                    execution,
                    clock,
                )
            },
        )?;
        match outcome {
            PinnedStartRunOutcome::Committed(claim) => self.drive(&run_id, &claim),
            PinnedStartRunOutcome::Replayed => self.replay_started_run(&run_id),
        }
    }

    fn replay_started_run(&mut self, run_id: &str) -> DurableResult<DriveOutcome> {
        let read = self
            .coordinator
            .read_executor_run(run_id)?
            .ok_or_else(|| DurableError::NotFound(format!("Run {run_id} is missing")))?;
        Self::verify_executor_run_binding(&read)?;
        match read.continuation.status {
            ContinuationStatus::Waiting => {
                let mut waits = read.continuation.wait_set.iter();
                let wait_id = waits
                    .next()
                    .cloned()
                    .ok_or_else(|| DurableError::Integrity {
                        code: "waiting_continuation_wait_missing".to_owned(),
                        message: format!("waiting Run {run_id} has no active Wait"),
                    })?;
                if waits.next().is_some() {
                    return Err(DurableError::Integrity {
                        code: "waiting_continuation_wait_ambiguous".to_owned(),
                        message: format!("waiting Run {run_id} has more than one active Wait"),
                    });
                }
                Ok(DriveOutcome::Suspended { wait_id })
            }
            ContinuationStatus::Ready => {
                let boundary = self.coordinator.read_ready_boundary(run_id)?;
                if boundary.revision != read.revision {
                    return Err(DurableError::Conflict {
                        expected: Some(read.revision),
                        current: Some(boundary.revision),
                    });
                }
                if let Some(intent_id) = boundary.unknown_intent {
                    return Ok(DriveOutcome::ReconciliationRequired { intent_id });
                }
                if !boundary.explicit_intents.is_empty() {
                    return Ok(DriveOutcome::ReleaseRequired {
                        intent_ids: boundary.explicit_intents,
                    });
                }
                Err(DurableError::IllegalTransition(format!(
                    "Run {run_id} is already Ready; use resume instead of replaying start"
                )))
            }
            ContinuationStatus::Running => {
                if self
                    .coordinator
                    .recover_admitted_component_failure(run_id)?
                {
                    let terminal =
                        self.coordinator.read_executor_run(run_id)?.ok_or_else(|| {
                            DurableError::Integrity {
                                code: "component_failure_recovery_result_missing".to_owned(),
                                message: format!("recovered failure Run {run_id} disappeared"),
                            }
                        })?;
                    return terminal_drive_outcome_from_read(&terminal);
                }
                let claim =
                    read.continuation
                        .execution_claim
                        .ok_or_else(|| DurableError::Integrity {
                            code: "running_continuation_claim_missing".to_owned(),
                            message: format!("running Run {run_id} has no execution claim"),
                        })?;
                Err(DurableError::Busy {
                    run_id: run_id.to_owned(),
                    owner: claim.owner,
                    fence: claim.fence,
                })
            }
            ContinuationStatus::Completed => self
                .coordinator
                .completed_execution_result(run_id)
                .map(DriveOutcome::Completed),
            ContinuationStatus::Failed | ContinuationStatus::Cancelled => {
                terminal_drive_outcome_from_read(&read)
            }
        }
    }

    /// Receive and atomically admit one bounded signal or timer delivery.
    ///
    /// Transport polling and acknowledgement remain in the driver. A lost
    /// acknowledgement may cause redelivery, which replays the existing M1
    /// activation before acknowledging it again.
    pub fn drive_wait_source<D: WaitSourceDriver>(
        &mut self,
        driver: &mut D,
        max_targets: usize,
    ) -> DurableResult<Option<WaitAdmissionOutcome>> {
        if max_targets == 0 || max_targets > MAX_WAIT_DELIVERY_TARGETS {
            return Err(DurableError::Validation(format!(
                "wait source target limit must be between 1 and {MAX_WAIT_DELIVERY_TARGETS}"
            )));
        }
        let Some((activation_id, outcome)) = self
            .coordinator
            .admit_wait_source_delivery_pinned(driver, max_targets)?
        else {
            return Ok(None);
        };
        driver.acknowledge(&activation_id)?;
        Ok(Some(outcome))
    }

    /// Resume an existing ready Run under a new durable execution claim.
    pub fn resume(
        &mut self,
        run_id: &str,
        execution: &ExecutionClaimRequest,
    ) -> DurableResult<DriveOutcome> {
        execution.verify()?;
        let read = self
            .coordinator
            .read_executor_run(run_id)?
            .ok_or_else(|| DurableError::NotFound(format!("Run {run_id} is missing")))?;
        Self::verify_executor_run_binding(&read)?;
        match read.continuation.status {
            ContinuationStatus::Ready => {
                let claim = self.commit_with_current_execution_clock(
                    run_id,
                    execution,
                    |coordinator, clock| coordinator.claim_ready_pinned(run_id, execution, clock),
                )?;
                self.drive(run_id, &claim)
            }
            ContinuationStatus::Running => {
                if self
                    .coordinator
                    .recover_admitted_component_failure(run_id)?
                {
                    let terminal =
                        self.coordinator.read_executor_run(run_id)?.ok_or_else(|| {
                            DurableError::Integrity {
                                code: "component_failure_recovery_result_missing".to_owned(),
                                message: format!("recovered failure Run {run_id} disappeared"),
                            }
                        })?;
                    return terminal_drive_outcome_from_read(&terminal);
                }
                let active =
                    read.continuation
                        .execution_claim
                        .ok_or_else(|| DurableError::Integrity {
                            code: "running_continuation_claim_missing".to_owned(),
                            message: format!("running Run {run_id} has no execution claim"),
                        })?;
                Err(DurableError::Busy {
                    run_id: run_id.to_owned(),
                    owner: active.owner,
                    fence: active.fence,
                })
            }
            ContinuationStatus::Waiting => Err(DurableError::IllegalTransition(format!(
                "continuation {run_id} is still waiting"
            ))),
            ContinuationStatus::Completed => self
                .coordinator
                .completed_execution_result(run_id)
                .map(DriveOutcome::Completed),
            ContinuationStatus::Failed | ContinuationStatus::Cancelled => {
                terminal_drive_outcome_from_read(&read)
            }
        }
    }

    /// Explicitly take over one expired persisted Running Continuation.
    pub fn takeover(
        &mut self,
        run_id: &str,
        expected_fence: u64,
        execution: &ExecutionClaimRequest,
    ) -> DurableResult<DriveOutcome> {
        execution.verify()?;
        let read = self
            .coordinator
            .preflight_takeover_pinned(run_id, expected_fence)?;
        Self::verify_executor_run_binding(&read)?;
        let claim =
            self.commit_with_current_execution_clock(run_id, execution, |coordinator, clock| {
                coordinator.takeover_running_pinned(run_id, expected_fence, execution, clock)
            })?;
        self.drive(run_id, &claim)
    }

    fn commit_with_current_execution_clock<T>(
        &mut self,
        run_id: &str,
        execution: &ExecutionClaimRequest,
        commit: impl FnOnce(&mut DurableCoordinator<S>, ClockObservation) -> DurableResult<T>,
    ) -> DurableResult<T> {
        execution.verify()?;
        if execution.clock.scope != execution_clock_scope(run_id)? {
            return Err(DurableError::Validation(
                "execution Clock reference does not match its exact Run scope".to_owned(),
            ));
        }
        let Self {
            coordinator, clock, ..
        } = self;
        let mut commit = Some(commit);
        let mut outcome: Option<DurableResult<T>> = None;
        let guard_result = {
            let mut guarded_commit = |observation: &ClockObservation| {
                if outcome.is_some() {
                    return Err(DurableError::Validation(
                        "Clock authority invoked the execution Store commit more than once"
                            .to_owned(),
                    ));
                }
                let result = (|| {
                    observation.verify()?;
                    if observation.reference() != execution.clock {
                        return Err(DurableError::Validation(
                            "Clock authority returned a different observation receipt".to_owned(),
                        ));
                    }
                    let commit = commit.take().ok_or_else(|| {
                        DurableError::Validation(
                            "Clock authority invoked the execution Store commit more than once"
                                .to_owned(),
                        )
                    })?;
                    commit(coordinator, observation.clone())
                })();
                match result {
                    Ok(value) => {
                        outcome = Some(Ok(value));
                        Ok(())
                    }
                    Err(error) => {
                        let message = error.to_string();
                        outcome = Some(Err(error));
                        Err(DurableError::Validation(format!(
                            "execution Store commit callback failed: {message}"
                        )))
                    }
                }
            };
            clock.with_current_head(&execution.clock, &mut guarded_commit)
        };
        match outcome {
            Some(Err(error)) => Err(error),
            Some(Ok(value)) => match guard_result {
                Ok(()) => Ok(value),
                Err(error @ DurableError::CommitOutcomeUnknown { .. }) => Err(error),
                Err(error) => Err(DurableError::CommitOutcomeUnknown {
                    message: format!(
                        "Clock authority failed after the execution Store CAS completed: {error}"
                    ),
                }),
            },
            None => match guard_result {
                Err(error) => Err(error),
                Ok(()) => Err(DurableError::Validation(
                    "Clock authority did not invoke the execution Store commit".to_owned(),
                )),
            },
        }
    }

    /// Explicitly release one prepared effect after its owning scope commits.
    ///
    /// The release is idempotent after a lost receipt. Once the fenced claim is
    /// durable, recovery reconciles under that claim and never redispatches the
    /// semantic intent.
    pub fn release_effect(
        &mut self,
        intent_id: &str,
        execution: &ExecutionClaimRequest,
    ) -> DurableResult<DriveOutcome> {
        execution.verify()?;
        let read = self
            .coordinator
            .read_release_effect(intent_id)?
            .ok_or_else(|| DurableError::NotFound(format!("Effect {intent_id} is missing")))?;
        if read.effect.profile.dispatch != DispatchPolicy::Explicit {
            return Err(DurableError::IllegalTransition(format!(
                "Effect {intent_id} does not require explicit release"
            )));
        }
        if read.scope.status != cymule_core::ScopeStatus::ClosedCommitted {
            return Err(DurableError::IllegalTransition(format!(
                "Effect {intent_id} cannot release before its Scope commits"
            )));
        }
        let run_id = read.run.run.run_id.clone();
        let continuation = &read.run.continuation;
        if matches!(
            continuation.status,
            ContinuationStatus::Completed
                | ContinuationStatus::Failed
                | ContinuationStatus::Cancelled
        ) {
            if read.dispatch.state == OutboxState::CancelledBeforeRelease {
                return Err(DurableError::IllegalTransition(format!(
                    "Effect {intent_id} was cancelled before release"
                )));
            }
            if matches!(
                continuation.status,
                ContinuationStatus::Failed | ContinuationStatus::Cancelled
            ) && !matches!(
                read.dispatch.state,
                OutboxState::Claimed | OutboxState::Unknown
            ) {
                return terminal_drive_outcome_from_read(&read.run);
            }
            if continuation.status == ContinuationStatus::Completed {
                return self
                    .coordinator
                    .completed_execution_result(&run_id)
                    .map(DriveOutcome::Completed);
            }
        }
        if continuation.status != ContinuationStatus::Ready {
            if let Some(active) = &continuation.execution_claim {
                return Err(DurableError::Busy {
                    run_id,
                    owner: active.owner.clone(),
                    fence: active.fence,
                });
            }
            return Err(DurableError::IllegalTransition(format!(
                "Continuation {run_id} is not ready for Effect release"
            )));
        }
        let claim =
            self.commit_with_current_execution_clock(&run_id, execution, |coordinator, clock| {
                coordinator.claim_ready_pinned(&run_id, execution, clock)
            })?;
        if let Some(outcome) =
            self.dispatch_outbox_pinned(&run_id, Some(intent_id), &claim, false)?
        {
            return self.finish_nonterminal_boundary(&run_id, &claim, outcome);
        }
        self.drive(&run_id, &claim)
    }

    /// Ask the exact historical Effect provider to linearize one terminal
    /// resolution against any stale first-dispatch attempt, then persist only
    /// the provider's authoritative terminal outcome.
    pub(crate) fn resolve_effect_with_provider(
        &mut self,
        command: &EffectResolutionCommand,
    ) -> DurableResult<EffectResolutionReceipt> {
        Self::resolve_effect_with_provider_parts(
            &mut self.coordinator,
            &mut self.plugin,
            &self.binding,
            command,
        )
    }

    fn resolve_effect_with_provider_parts(
        coordinator: &mut DurableCoordinator<S>,
        plugin: &mut P,
        binding: &ExecutionBinding,
        command: &EffectResolutionCommand,
    ) -> DurableResult<EffectResolutionReceipt> {
        command.verify()?;
        if let Some(replayed) = replay_effect_resolution(coordinator, command)? {
            return Ok(replayed);
        }
        let material = coordinator.read_effect_resolution_material(command)?;
        let origin_binding = ExecutionBinding::decode(&material.origin_binding.bytes)?;
        if origin_binding.artifact_ref()? != material.dispatch.execution_binding
            || origin_binding
                .occurrence_binding(ExecutionOperationKind::Effect, &material.dispatch.operation)?
                != material.dispatch.occurrence_binding
        {
            return Err(DurableError::Integrity {
                code: "effect_resolution_origin_binding_mismatch".to_owned(),
                message: format!(
                    "Effect {} origin binding changed its exact occurrence authority",
                    command.intent_id
                ),
            });
        }
        let input = decode_artifact_value(&material.dispatch.input, &material.input)?;
        let contracts = PlanContracts::compile(&material.origin_plan.candidate)?;
        contracts.validate_effect_input(&material.dispatch.operation, &input)?;
        validate_optional_reconciliation_output(
            &contracts,
            &material.dispatch.operation,
            command.resolution,
            command.value.as_ref(),
        )?;
        let admission = plugin
            .admit_bound_operation(
                binding,
                &origin_binding,
                ExecutionOperationKind::Effect,
                &material.dispatch.operation,
            )
            .map_err(DurableError::from)?;
        if !admission.is_available() {
            return Err(DurableError::ReconciliationRequired {
                intent_id: command.intent_id.clone(),
            });
        }
        let provider_attempt = EffectProviderAttempt::new(
            &command.intent_id,
            &command.claim_owner,
            command.claim_epoch,
        )?;
        let decision = match command.resolution {
            ReconciliationResolution::ResolvedApplied => {
                EffectReconciliationDecision::ResolveApplied
            }
            ReconciliationResolution::ResolvedNotApplied => {
                EffectReconciliationDecision::ResolveNotApplied
            }
            ReconciliationResolution::StillUnknown
            | ReconciliationResolution::GovernanceRequired => {
                return Err(DurableError::Validation(
                    "terminal Effect resolution requires Applied or NotApplied".to_owned(),
                ));
            }
        };
        let response = admission.invoke(PluginRequest::ReconcileEffect {
            operation: material.dispatch.operation.clone(),
            intent_id: command.intent_id.clone(),
            attempt: provider_attempt.clone(),
            decision,
            resolution_value: command.value.clone(),
            input,
        });
        let (resolution, result) = terminal_resolution_observation(
            command,
            &material.dispatch.operation,
            &contracts,
            &provider_attempt,
            response,
        )?;
        coordinator.commit_effect_resolution_pinned(command, resolution, result)
    }

    pub(crate) fn coordinator_mut(&mut self) -> &mut DurableCoordinator<S> {
        &mut self.coordinator
    }

    /// Borrow the two distinct profile mutation authorities without exposing
    /// raw durable state or a public runtime capability.
    pub(crate) fn profile_authorities(
        &mut self,
    ) -> (&mut DurableCoordinator<S>, &mut dyn ExecutionClockAuthority) {
        let Self {
            coordinator, clock, ..
        } = self;
        (coordinator, clock.as_mut())
    }

    /// Consume the runtime and return its store and plugin.
    pub(crate) fn into_parts(self) -> (S, P) {
        (self.coordinator.into_store(), self.plugin)
    }

    fn drive(
        &mut self,
        run_id: &str,
        claim: &ContinuationExecutionClaim,
    ) -> DurableResult<DriveOutcome> {
        loop {
            self.coordinator.require_execution_claim_pinned(claim)?;
            let read = self.coordinator.read_execution_step(run_id)?;
            let source = &read.run.continuation;
            if source.execution_claim.as_ref() != Some(claim) {
                return Err(DurableError::Conflict {
                    expected: Some(format!("{}:{}", claim.owner, claim.fence)),
                    current: source
                        .execution_claim
                        .as_ref()
                        .map(|current| format!("{}:{}", current.owner, current.fence)),
                });
            }
            if matches!(
                source.status,
                ContinuationStatus::Failed | ContinuationStatus::Cancelled
            ) {
                return terminal_drive_outcome_from_read(&read.run);
            }
            if source.status != ContinuationStatus::Running {
                return Err(DurableError::IllegalTransition(format!(
                    "Run {run_id} is not Running under its execution claim"
                )));
            }
            let historical_binding = Self::verify_executor_run_binding(&read.run)?;
            let contracts = PlanContracts::compile(&read.run.plan.candidate)?;
            let context =
                frame_step_context(run_id, &read, claim, &historical_binding, &contracts)?;
            let outcome = match context.region.steps.get(context.frame.next_step) {
                Some(step) => self.execute_plan_step(&context, step)?,
                None => self.finish_frame(&context)?,
            };
            if let Some(outcome) = outcome {
                return Ok(outcome);
            }
        }
    }

    fn execute_plan_step(
        &mut self,
        context: &FrameStepContext<'_>,
        step: &Step,
    ) -> DurableResult<Option<DriveOutcome>> {
        match &step.operation {
            Operation::Call {
                component, input, ..
            } => self.execute_component_step(context, component, input),
            Operation::Invoke {
                definition,
                input: expression,
                ..
            } => {
                let value = evaluate_step(
                    context.read,
                    expression,
                    &context.input,
                    &context.frame.locals,
                )?;
                context
                    .contracts
                    .validate_definition_input(definition, &value)?;
                let input_record =
                    immutable_artifact(INVOCATION_INPUT_ARTIFACT_KIND, canonical_bytes(&value)?)?;
                self.coordinator.commit_executor_boundary(
                    context.claim,
                    &context.read.run.revision,
                    &context.read.run.continuation,
                    &ExecutorCoreBoundary::EnterInvocation {
                        input: input_record,
                    },
                )?;
                Ok(None)
            }
            Operation::Wait { .. } => {
                let wait_id = derive_wait_id(
                    context.run_id,
                    &context.read.run.plan.plan_id,
                    &context.frame.invocation_id,
                    &step.id,
                )?;
                self.coordinator.commit_executor_boundary(
                    context.claim,
                    &context.read.run.revision,
                    &context.read.run.continuation,
                    &ExecutorCoreBoundary::ParkWait,
                )?;
                Ok(Some(DriveOutcome::Suspended { wait_id }))
            }
            Operation::Effect {
                effect,
                input,
                occurrence,
                bind,
            } => self.execute_effect_step_pinned(&EffectStepContext {
                read: context.read,
                claim: context.claim,
                historical_binding: context.historical_binding,
                contracts: context.contracts,
                effect,
                expression: input,
                occurrence,
                bind: bind.as_ref(),
            }),
            Operation::Scope { .. } => {
                self.coordinator.commit_executor_boundary(
                    context.claim,
                    &context.read.run.revision,
                    &context.read.run.continuation,
                    &ExecutorCoreBoundary::OpenScope,
                )?;
                Ok(None)
            }
        }
    }

    fn finish_frame(
        &mut self,
        context: &FrameStepContext<'_>,
    ) -> DurableResult<Option<DriveOutcome>> {
        let value = evaluate_step(
            context.read,
            &context.region.result,
            &context.input,
            &context.frame.locals,
        )?;
        if context.frame_index > 0 {
            self.finish_nested_frame(context, &value)
        } else {
            self.finish_root_frame(context, &value)
        }
    }

    fn finish_nested_frame(
        &mut self,
        context: &FrameStepContext<'_>,
        value: &Value,
    ) -> DurableResult<Option<DriveOutcome>> {
        let run_id = context.run_id;
        let source = &context.read.run.continuation;
        let parent_frame = &source.frames[context.frame_index - 1];
        let parent_definition = context
            .read
            .run
            .plan
            .candidate
            .definitions
            .iter()
            .find(|definition| definition.id == parent_frame.definition_id)
            .ok_or_else(|| DurableError::Integrity {
                code: "executor_parent_definition_missing".to_owned(),
                message: format!(
                    "Run {run_id} parent definition {} is missing",
                    parent_frame.definition_id
                ),
            })?;
        let parent_region = region_at_path(&parent_definition.body, &parent_frame.region_path)?;
        let parent_step = parent_region
            .steps
            .get(parent_frame.next_step)
            .ok_or_else(|| DurableError::Integrity {
                code: "executor_parent_step_missing".to_owned(),
                message: format!("Run {run_id} parent frame no longer points at a child step"),
            })?;
        let (closes_scope, result_kind) = match &parent_step.operation {
            Operation::Scope { .. } => (true, SCOPE_RESULT_ARTIFACT_KIND),
            Operation::Invoke { definition, .. } => {
                if definition != &context.frame.definition_id {
                    return Err(DurableError::Integrity {
                        code: "executor_invocation_definition_mismatch".to_owned(),
                        message: format!("Run {run_id} child frame changed its invoked definition"),
                    });
                }
                context
                    .contracts
                    .validate_definition_output(&context.frame.definition_id, value)?;
                (false, INVOCATION_RESULT_ARTIFACT_KIND)
            }
            _ => {
                return Err(DurableError::Integrity {
                    code: "executor_parent_step_kind_mismatch".to_owned(),
                    message: format!("Run {run_id} parent frame points at a non-structured step"),
                });
            }
        };
        let result = immutable_artifact(result_kind, canonical_bytes(value)?)?;
        let boundary = if closes_scope {
            ExecutorCoreBoundary::CommitScope { result }
        } else {
            ExecutorCoreBoundary::CompleteInvocation { result }
        };
        self.coordinator.commit_executor_boundary(
            context.claim,
            &context.read.run.revision,
            source,
            &boundary,
        )?;
        if closes_scope
            && let Some(outcome) =
                self.dispatch_outbox_pinned(run_id, None, context.claim, false)?
        {
            return self
                .finish_nonterminal_boundary(run_id, context.claim, outcome)
                .map(Some);
        }
        Ok(None)
    }

    fn finish_root_frame(
        &mut self,
        context: &FrameStepContext<'_>,
        value: &Value,
    ) -> DurableResult<Option<DriveOutcome>> {
        let run_id = context.run_id;
        context
            .contracts
            .validate_definition_output(&context.frame.definition_id, value)?;
        if context.read.current_scope.status == cymule_core::ScopeStatus::Open {
            self.coordinator.commit_executor_boundary(
                context.claim,
                &context.read.run.revision,
                &context.read.run.continuation,
                &ExecutorCoreBoundary::CommitRootScope,
            )?;
            return Ok(None);
        }
        if context.read.current_scope.status != cymule_core::ScopeStatus::ClosedCommitted {
            return Err(DurableError::Integrity {
                code: "executor_root_scope_terminal_mismatch".to_owned(),
                message: format!("Run {run_id} active root Scope is neither open nor committed"),
            });
        }
        if let Some(outcome) = self.dispatch_outbox_pinned(run_id, None, context.claim, true)? {
            return self
                .finish_nonterminal_boundary(run_id, context.claim, outcome)
                .map(Some);
        }
        self.complete_root_run(run_id, context.claim).map(Some)
    }

    fn complete_root_run(
        &mut self,
        run_id: &str,
        claim: &ContinuationExecutionClaim,
    ) -> DurableResult<DriveOutcome> {
        // Outbox settlement may have advanced the same Run's pinned revision.
        // Derive completion from that exact successor, without replaying any
        // provider work or relaxing the coordinator's source/claim fence.
        self.coordinator.require_execution_claim_pinned(claim)?;
        let read = self.coordinator.read_execution_step(run_id)?;
        let source = &read.run.continuation;
        if source.execution_claim.as_ref() != Some(claim) {
            return Err(DurableError::Conflict {
                expected: Some(format!("{}:{}", claim.owner, claim.fence)),
                current: source
                    .execution_claim
                    .as_ref()
                    .map(|current| format!("{}:{}", current.owner, current.fence)),
            });
        }
        let historical_binding = Self::verify_executor_run_binding(&read.run)?;
        let contracts = PlanContracts::compile(&read.run.plan.candidate)?;
        let context = frame_step_context(run_id, &read, claim, &historical_binding, &contracts)?;
        let value = evaluate_step(
            &read,
            &context.region.result,
            &context.input,
            &context.frame.locals,
        )?;
        contracts.validate_definition_output(&context.frame.definition_id, &value)?;
        let result = immutable_artifact(RESULT_ARTIFACT_KIND, canonical_bytes(&value)?)?;
        self.coordinator.commit_executor_boundary(
            claim,
            &read.run.revision,
            source,
            &ExecutorCoreBoundary::CompleteRun { result },
        )?;
        self.coordinator
            .completed_execution_result(run_id)
            .map(DriveOutcome::Completed)
    }

    fn execute_component_step(
        &mut self,
        context: &FrameStepContext<'_>,
        component: &str,
        expression: &Expression,
    ) -> DurableResult<Option<DriveOutcome>> {
        let value = evaluate_step(
            context.read,
            expression,
            &context.input,
            &context.frame.locals,
        )?;
        context
            .contracts
            .validate_component_input(component, &value)?;
        let input_record =
            immutable_artifact(COMPONENT_INPUT_ARTIFACT_KIND, canonical_bytes(&value)?)?;
        let output_artifact_kind = context
            .read
            .run
            .plan
            .candidate
            .components
            .iter()
            .find(|contract| contract.id == component)
            .ok_or_else(|| DurableError::Integrity {
                code: "executor_component_contract_missing".to_owned(),
                message: format!(
                    "Run {} component contract {component} is missing",
                    context.run_id
                ),
            })?
            .output_artifact_kind
            .clone();
        let attempt = match self.coordinator.commit_component_attempt_pinned(
            context.claim,
            &context.read.run.revision,
            &context.read.run.continuation,
            input_record,
        )? {
            ExecutorComponentAttemptAdmission::Admitted(attempt) => attempt,
            ExecutorComponentAttemptAdmission::InFlight(attempt) => {
                return Err(DurableError::Busy {
                    run_id: attempt.run_id,
                    owner: attempt.execution_claim_owner,
                    fence: attempt.execution_claim_fence,
                });
            }
        };
        self.coordinator
            .require_execution_claim_pinned(context.claim)?;
        let response = self
            .plugin
            .invoke_bound(
                &self.binding,
                context.historical_binding,
                PluginRequest::Call {
                    component: component.to_owned(),
                    input: value,
                },
            )
            .map_err(|error| match error {
                cymule_runtime::RuntimeError::Substrate { code, message } => {
                    DurableError::TimedOut {
                        code: "component_invocation_interrupted".to_owned(),
                        message: format!("{code}: {message}"),
                    }
                }
                other => DurableError::from(other),
            })?;
        self.finish_component_response(
            context,
            component,
            &output_artifact_kind,
            &attempt,
            response,
        )
    }

    fn finish_component_response(
        &mut self,
        context: &FrameStepContext<'_>,
        component: &str,
        output_artifact_kind: &str,
        attempt: &OperationAttempt,
        response: PluginResponse,
    ) -> DurableResult<Option<DriveOutcome>> {
        match response {
            PluginResponse::CallResult { value } => {
                context
                    .contracts
                    .validate_component_output(component, &value)?;
                let output = immutable_artifact(output_artifact_kind, canonical_bytes(&value)?)?;
                self.coordinator.commit_component_result_pinned(
                    context.claim,
                    &attempt.attempt_id,
                    &ExecutorComponentResult::Succeeded { output },
                )?;
                Ok(None)
            }
            PluginResponse::ExpectedFailure { error } => {
                error.verify()?;
                let detail =
                    immutable_artifact(DECLARED_FAILURE_ARTIFACT_KIND, canonical_bytes(&error)?)?;
                let failure = RunFailure {
                    class: RunFailureClass::DeclaredFailure,
                    code: error.code,
                    detail: detail.reference.clone(),
                };
                self.coordinator.commit_component_result_pinned(
                    context.claim,
                    &attempt.attempt_id,
                    &ExecutorComponentResult::ExpectedFailure {
                        failure: failure.clone(),
                        detail,
                    },
                )?;
                Ok(Some(DriveOutcome::Failed { failure }))
            }
            PluginResponse::Defect { code, message } => {
                Err(DurableError::RuntimeDefect { code, message })
            }
            response => Err(DurableError::RuntimeDefect {
                code: "component_response_variant_invalid".to_owned(),
                message: format!("component {component} returned {response:?}"),
            }),
        }
    }

    fn execute_effect_step_pinned(
        &mut self,
        context: &EffectStepContext<'_>,
    ) -> DurableResult<Option<DriveOutcome>> {
        let prepared = prepare_effect_step(context)?;
        let run_id = &context.read.run.run.run_id;
        if let Some(existing) = self.coordinator.read_effect_execution(
            run_id,
            &prepared.intent_id,
            &context.read.run.revision,
        )? {
            return self.advance_existing_effect_step(context, prepared.intent_id, existing);
        }
        self.prepare_and_enqueue_effect(context, prepared)
    }

    fn advance_existing_effect_step(
        &mut self,
        context: &EffectStepContext<'_>,
        intent_id: String,
        existing: ExecutorEffectRead,
    ) -> DurableResult<Option<DriveOutcome>> {
        let run_id = &context.read.run.run.run_id;
        match existing.dispatch.state {
            OutboxState::Applied => {
                let result = existing
                    .result
                    .map(|record| record.reference)
                    .ok_or_else(|| DurableError::Integrity {
                        code: "executor_effect_result_missing".to_owned(),
                        message: format!(
                            "applied eager Effect {intent_id} has no exact result Artifact"
                        ),
                    })?;
                self.coordinator.commit_executor_boundary(
                    context.claim,
                    &existing.revision,
                    &existing.run.continuation,
                    &ExecutorCoreBoundary::AdvanceSettledEffect {
                        intent_id,
                        result: Some(result),
                    },
                )?;
                Ok(None)
            }
            OutboxState::NotApplied | OutboxState::CancelledBeforeRelease
                if context.bind.is_none() =>
            {
                self.coordinator.commit_executor_boundary(
                    context.claim,
                    &existing.revision,
                    &existing.run.continuation,
                    &ExecutorCoreBoundary::AdvanceSettledEffect {
                        intent_id,
                        result: None,
                    },
                )?;
                Ok(None)
            }
            OutboxState::NotApplied | OutboxState::CancelledBeforeRelease => self
                .finish_nonterminal_boundary(
                    run_id,
                    context.claim,
                    DriveOutcome::EffectNotApplied { intent_id },
                )
                .map(Some),
            OutboxState::Pending | OutboxState::Claimed | OutboxState::Unknown => {
                if let Some(outcome) =
                    self.dispatch_outbox_pinned(run_id, None, context.claim, false)?
                {
                    return self
                        .finish_nonterminal_boundary(run_id, context.claim, outcome)
                        .map(Some);
                }
                Ok(None)
            }
        }
    }

    fn prepare_and_enqueue_effect(
        &mut self,
        context: &EffectStepContext<'_>,
        prepared: PreparedEffectStep,
    ) -> DurableResult<Option<DriveOutcome>> {
        let read = context.read;
        let run_id = &read.run.run.run_id;
        let plan = &read.run.plan;
        let effect = context.effect;
        let contract = plan
            .candidate
            .effects
            .iter()
            .find(|contract| contract.id == effect)
            .ok_or_else(|| DurableError::Integrity {
                code: "executor_effect_contract_missing".to_owned(),
                message: format!("Run {run_id} Effect contract {effect} is missing"),
            })?;
        let eager = contract.profile.mutation == MutationKind::Observational
            && contract.profile.dispatch == DispatchPolicy::Eager;
        self.coordinator
            .require_execution_claim_pinned(context.claim)?;
        let response = self
            .plugin
            .invoke_bound(
                &self.binding,
                context.historical_binding,
                PluginRequest::PrepareEffect {
                    operation: effect.to_owned(),
                    intent_id: prepared.intent_id.clone(),
                    input: prepared.value,
                },
            )
            .map_err(DurableError::from)?;
        if response != PluginResponse::Prepared {
            return Err(match response {
                PluginResponse::Defect { code, message } => {
                    DurableError::RuntimeDefect { code, message }
                }
                _ => DurableError::RuntimeDefect {
                    code: "effect_prepare_response_variant_invalid".to_owned(),
                    message: format!("Effect {effect} prepare returned {response:?}"),
                },
            });
        }
        let dispatch = EffectDispatch {
            intent_id: prepared.intent_id,
            run_id: run_id.clone(),
            origin_plan_id: plan.plan_id.clone(),
            operation: effect.to_owned(),
            input: prepared.args.reference.clone(),
            execution_binding: prepared.execution_binding,
            occurrence_binding: prepared.occurrence_binding,
            execution_availability: EffectExecutionAvailability::Available,
            reconciliation: cymule_core::ReconciliationState::NotRequired,
            state: OutboxState::Pending,
            claim_epoch: 0,
            claim_owner: None,
            result: None,
        };
        self.coordinator.commit_effect_enqueue_pinned(
            context.claim,
            &read.run.revision,
            &read.run.continuation,
            prepared.args,
            dispatch,
        )?;
        if eager
            && let Some(outcome) =
                self.dispatch_outbox_pinned(run_id, None, context.claim, false)?
        {
            return self
                .finish_nonterminal_boundary(run_id, context.claim, outcome)
                .map(Some);
        }
        Ok(None)
    }

    fn finish_nonterminal_boundary(
        &mut self,
        run_id: &str,
        claim: &ContinuationExecutionClaim,
        outcome: DriveOutcome,
    ) -> DurableResult<DriveOutcome> {
        self.coordinator.require_execution_claim_pinned(claim)?;
        let read = self.coordinator.read_execution_step(run_id)?;
        if read.run.continuation.execution_claim.as_ref() != Some(claim) {
            return Err(DurableError::Conflict {
                expected: Some(format!("{}:{}", claim.owner, claim.fence)),
                current: read
                    .run
                    .continuation
                    .execution_claim
                    .as_ref()
                    .map(|current| format!("{}:{}", current.owner, current.fence)),
            });
        }
        let reason = match &outcome {
            DriveOutcome::ReconciliationRequired { intent_id }
            | DriveOutcome::EffectUnavailable { intent_id }
            | DriveOutcome::EffectNotApplied { intent_id } => {
                ExecutorYieldReadyReason::EffectBoundary {
                    intent_id: intent_id.clone(),
                }
            }
            DriveOutcome::ReleaseRequired { intent_ids } => {
                ExecutorYieldReadyReason::ReleaseBoundary {
                    intent_ids: intent_ids.clone(),
                }
            }
            DriveOutcome::Suspended { .. }
            | DriveOutcome::Completed(_)
            | DriveOutcome::Failed { .. }
            | DriveOutcome::Cancelled { .. } => {
                return Err(DurableError::RuntimeDefect {
                    code: "executor_yield_outcome_invalid".to_owned(),
                    message: "terminal or Wait outcome reached the Ready-yield boundary".to_owned(),
                });
            }
        };
        self.coordinator.commit_executor_boundary(
            claim,
            &read.run.revision,
            &read.run.continuation,
            &ExecutorCoreBoundary::YieldReady { reason },
        )?;
        Ok(outcome)
    }

    fn verify_executor_run_binding(read: &ExecutorRunRead) -> DurableResult<ExecutionBinding> {
        read.binding.validate()?;
        read.root_input.validate()?;
        let binding = ExecutionBinding::decode(&read.binding.bytes)?;
        if binding.artifact_ref()? != read.binding.reference
            || read.continuation.binding_context != read.binding.reference.artifact_id
            || read.continuation.plan_id != read.plan.plan_id
            || read.continuation.frames.first().map(|frame| &frame.input)
                != Some(&read.root_input.reference)
        {
            return Err(DurableError::Integrity {
                code: "executor_run_material_mismatch".to_owned(),
                message: format!(
                    "Run {} execution material does not form one exact pinned boundary",
                    read.run.run_id
                ),
            });
        }
        binding.admit_plan(&read.plan)?;
        Ok(binding)
    }

    fn dispatch_outbox_pinned(
        &mut self,
        run_id: &str,
        explicit_release: Option<&str>,
        claim: &ContinuationExecutionClaim,
        include_explicit_boundary: bool,
    ) -> DurableResult<Option<DriveOutcome>> {
        loop {
            self.coordinator.require_execution_claim_pinned(claim)?;
            let selection = self
                .coordinator
                .read_next_dispatch(run_id, explicit_release)?;
            let Some(entry) = selection.next else {
                if include_explicit_boundary && !selection.explicit_intents.is_empty() {
                    return Ok(Some(DriveOutcome::ReleaseRequired {
                        intent_ids: selection.explicit_intents,
                    }));
                }
                return Ok(None);
            };
            if let Some(outcome) =
                self.dispatch_selected_effect(run_id, &selection.revision, entry, claim)?
            {
                return Ok(Some(outcome));
            }
        }
    }

    fn dispatch_selected_effect(
        &mut self,
        run_id: &str,
        selection_revision: &str,
        entry: ExecutorEffectRead,
        claim: &ContinuationExecutionClaim,
    ) -> DurableResult<Option<DriveOutcome>> {
        if entry.revision != selection_revision {
            return Err(DurableError::Integrity {
                code: "executor_dispatch_revision_mismatch".to_owned(),
                message: format!(
                    "Run {run_id} dispatch selection combined different StateRoot revisions"
                ),
            });
        }
        if entry.dispatch.execution_availability == EffectExecutionAvailability::Unavailable {
            return Ok(Some(DriveOutcome::EffectUnavailable {
                intent_id: entry.dispatch.intent_id,
            }));
        }
        let origin_binding = dispatch_origin_binding(&entry)?;
        let exact_binding = self
            .binding
            .verify_selected_operation_equivalence(
                &origin_binding,
                ExecutionOperationKind::Effect,
                &entry.dispatch.operation,
            )
            .is_ok();
        if !exact_binding {
            return Self::settle_unavailable_effect(&mut self.coordinator, &entry, claim);
        }
        if entry.dispatch.state == OutboxState::Claimed {
            self.coordinator.commit_effect_settlement_pinned(
                claim,
                &entry,
                ExecutorEffectSettlement::Observation {
                    outcome: WorldOutcome::Unknown,
                    result: None,
                },
            )?;
            return Ok(None);
        }
        let admission = self
            .plugin
            .admit_bound_operation(
                &self.binding,
                &origin_binding,
                ExecutionOperationKind::Effect,
                &entry.dispatch.operation,
            )
            .map_err(DurableError::from)?;
        if !admission.is_available() {
            return Self::settle_unavailable_effect(&mut self.coordinator, &entry, claim);
        }
        let contracts = PlanContracts::compile(&entry.origin_plan.candidate)?;
        let input = decode_artifact_value(&entry.dispatch.input, &entry.input)?;
        contracts.validate_effect_input(&entry.dispatch.operation, &input)?;
        if entry.dispatch.state == OutboxState::Pending {
            Self::dispatch_pending_effect(
                &mut self.coordinator,
                &entry,
                claim,
                admission,
                &contracts,
                input,
            )
        } else {
            Self::reconcile_unknown_effect(
                &mut self.coordinator,
                entry,
                claim,
                admission,
                &contracts,
                input,
            )
        }
    }

    fn settle_unavailable_effect(
        coordinator: &mut DurableCoordinator<S>,
        entry: &ExecutorEffectRead,
        claim: &ContinuationExecutionClaim,
    ) -> DurableResult<Option<DriveOutcome>> {
        let unresolved = entry.dispatch.state != OutboxState::Pending;
        let intent_id = entry.dispatch.intent_id.clone();
        coordinator.commit_effect_settlement_pinned(
            claim,
            entry,
            ExecutorEffectSettlement::Unavailable,
        )?;
        if unresolved {
            Ok(Some(DriveOutcome::EffectUnavailable { intent_id }))
        } else {
            Ok(None)
        }
    }

    fn dispatch_pending_effect(
        coordinator: &mut DurableCoordinator<S>,
        entry: &ExecutorEffectRead,
        claim: &ContinuationExecutionClaim,
        admission: BoundOperationAdmission<'_>,
        contracts: &PlanContracts,
        input: Value,
    ) -> DurableResult<Option<DriveOutcome>> {
        let claim_read = coordinator.commit_effect_claim_pinned(claim, entry)?;
        let claim_read = coordinator.require_effect_claim_current(claim, claim_read)?;
        let entry = claim_read.read;
        if entry.dispatch.claim_owner.as_deref() != Some(claim_read.owner.as_str())
            || entry.dispatch.claim_epoch != claim_read.epoch
            || claim_read.provider_attempt
                != EffectProviderAttempt::new(
                    &entry.dispatch.intent_id,
                    &claim_read.owner,
                    claim_read.epoch,
                )?
        {
            return Err(DurableError::Integrity {
                code: "executor_effect_provider_attempt_mismatch".to_owned(),
                message: format!(
                    "Effect {} claim returned a mismatched provider Attempt",
                    entry.dispatch.intent_id
                ),
            });
        }
        let response = admission.invoke(PluginRequest::DispatchEffect {
            operation: entry.dispatch.operation.clone(),
            intent_id: entry.dispatch.intent_id.clone(),
            attempt: claim_read.provider_attempt.clone(),
            input,
        });
        let response = dispatch_observation(
            contracts,
            &entry.dispatch.operation,
            &claim_read.provider_attempt,
            response,
        );
        let Some((outcome, value)) = response else {
            let intent_id = entry.dispatch.intent_id.clone();
            coordinator.commit_effect_settlement_pinned(
                claim,
                &entry,
                ExecutorEffectSettlement::Observation {
                    outcome: WorldOutcome::Unknown,
                    result: None,
                },
            )?;
            return Ok(Some(DriveOutcome::ReconciliationRequired { intent_id }));
        };
        let result = match outcome {
            WorldOutcome::Applied => Some(immutable_artifact(
                EFFECT_RESULT_ARTIFACT_KIND,
                canonical_bytes(&value.unwrap_or(Value::Null))?,
            )?),
            WorldOutcome::NotApplied | WorldOutcome::Unknown => None,
            WorldOutcome::Unobserved => {
                return Err(DurableError::RuntimeDefect {
                    code: "effect_dispatch_unobserved_outcome".to_owned(),
                    message: "Effect provider returned the pre-dispatch outcome".to_owned(),
                });
            }
        };
        let intent_id = entry.dispatch.intent_id.clone();
        coordinator.commit_effect_settlement_pinned(
            claim,
            &entry,
            ExecutorEffectSettlement::Observation { outcome, result },
        )?;
        if outcome == WorldOutcome::Unknown {
            Ok(Some(DriveOutcome::ReconciliationRequired { intent_id }))
        } else {
            Ok(None)
        }
    }

    fn reconcile_unknown_effect(
        coordinator: &mut DurableCoordinator<S>,
        entry: ExecutorEffectRead,
        claim: &ContinuationExecutionClaim,
        admission: BoundOperationAdmission<'_>,
        contracts: &PlanContracts,
        input: Value,
    ) -> DurableResult<Option<DriveOutcome>> {
        if entry.effect.profile.reconciliation != ReconciliationMode::Queryable {
            return Ok(Some(DriveOutcome::ReconciliationRequired {
                intent_id: entry.dispatch.intent_id,
            }));
        }
        let owner =
            entry
                .dispatch
                .claim_owner
                .as_deref()
                .ok_or_else(|| DurableError::Integrity {
                    code: "executor_effect_claim_owner_missing".to_owned(),
                    message: format!(
                        "unknown Effect {} has no retained claim owner",
                        entry.dispatch.intent_id
                    ),
                })?;
        let provider_attempt = EffectProviderAttempt::new(
            &entry.dispatch.intent_id,
            owner,
            entry.dispatch.claim_epoch,
        )?;
        coordinator.require_execution_claim_pinned(claim)?;
        let response = admission.invoke(PluginRequest::ReconcileEffect {
            operation: entry.dispatch.operation.clone(),
            intent_id: entry.dispatch.intent_id.clone(),
            attempt: provider_attempt.clone(),
            decision: EffectReconciliationDecision::Observe,
            resolution_value: None,
            input,
        });
        let Some((resolution, value)) = reconciliation_observation(
            contracts,
            &entry.dispatch.operation,
            &provider_attempt,
            response,
        )?
        else {
            return Ok(Some(DriveOutcome::ReconciliationRequired {
                intent_id: entry.dispatch.intent_id,
            }));
        };
        let result = match resolution {
            ReconciliationResolution::ResolvedApplied => Some(immutable_artifact(
                EFFECT_RESULT_ARTIFACT_KIND,
                canonical_bytes(&value.unwrap_or(Value::Null))?,
            )?),
            ReconciliationResolution::ResolvedNotApplied
            | ReconciliationResolution::StillUnknown => None,
            ReconciliationResolution::GovernanceRequired => {
                return Err(DurableError::RuntimeDefect {
                    code: "provider_governance_resolution_invalid".to_owned(),
                    message: "Effect provider cannot author governance escalation".to_owned(),
                });
            }
        };
        let intent_id = entry.dispatch.intent_id.clone();
        coordinator.commit_effect_settlement_pinned(
            claim,
            &entry,
            ExecutorEffectSettlement::Reconciliation { resolution, result },
        )?;
        if resolution == ReconciliationResolution::StillUnknown {
            Ok(Some(DriveOutcome::ReconciliationRequired { intent_id }))
        } else {
            Ok(None)
        }
    }
}

fn frame_step_context<'a>(
    run_id: &'a str,
    read: &'a ExecutorStepRead,
    claim: &'a ContinuationExecutionClaim,
    historical_binding: &'a ExecutionBinding,
    contracts: &'a PlanContracts,
) -> DurableResult<FrameStepContext<'a>> {
    let source = &read.run.continuation;
    let frame_index =
        source
            .frames
            .len()
            .checked_sub(1)
            .ok_or_else(|| DurableError::Integrity {
                code: "executor_frame_missing".to_owned(),
                message: format!("Run {run_id} has no active frame"),
            })?;
    let frame = &source.frames[frame_index];
    let definition = read
        .run
        .plan
        .candidate
        .definitions
        .iter()
        .find(|definition| definition.id == frame.definition_id)
        .ok_or_else(|| DurableError::Integrity {
            code: "executor_frame_definition_missing".to_owned(),
            message: format!(
                "Run {run_id} frame definition {} is missing",
                frame.definition_id
            ),
        })?;
    let input = read_step_value(read, &frame.input)?;
    contracts.validate_definition_input(&frame.definition_id, &input)?;
    let region = region_at_path(&definition.body, &frame.region_path)?;
    let current_scope =
        source
            .scope_stack
            .last()
            .cloned()
            .ok_or_else(|| DurableError::Integrity {
                code: "executor_scope_stack_empty".to_owned(),
                message: format!("Run {run_id} has no active scope"),
            })?;
    if read.current_scope.scope_id != current_scope {
        return Err(DurableError::Integrity {
            code: "executor_scope_read_mismatch".to_owned(),
            message: format!("Run {run_id} exact Scope read does not match its active scope"),
        });
    }
    Ok(FrameStepContext {
        run_id,
        read,
        claim,
        historical_binding,
        contracts,
        frame_index,
        frame,
        region,
        input,
    })
}

fn prepare_effect_step(context: &EffectStepContext<'_>) -> DurableResult<PreparedEffectStep> {
    let read = context.read;
    let run_id = &read.run.run.run_id;
    let plan = &read.run.plan;
    let frame_index = read
        .run
        .continuation
        .frames
        .len()
        .checked_sub(1)
        .ok_or_else(|| DurableError::Integrity {
            code: "executor_effect_frame_missing".to_owned(),
            message: format!("Run {run_id} has no Effect frame"),
        })?;
    let frame = &read.run.continuation.frames[frame_index];
    let input = read_step_value(read, &frame.input)?;
    let value = evaluate_step(read, context.expression, &input, &frame.locals)?;
    context
        .contracts
        .validate_effect_input(context.effect, &value)?;
    let args = immutable_artifact(EFFECT_ARGS_ARTIFACT_KIND, canonical_bytes(&value)?)?;
    let execution_binding = context.historical_binding.artifact_ref()?;
    let occurrence_binding = context
        .historical_binding
        .occurrence_binding(ExecutionOperationKind::Effect, context.effect)?;
    let definition = plan
        .candidate
        .definitions
        .iter()
        .find(|definition| definition.id == frame.definition_id)
        .ok_or_else(|| DurableError::Integrity {
            code: "executor_effect_definition_missing".to_owned(),
            message: format!(
                "Run {run_id} Effect frame definition {} is missing",
                frame.definition_id
            ),
        })?;
    let step = region_at_path(&definition.body, &frame.region_path)?
        .steps
        .get(frame.next_step)
        .ok_or_else(|| DurableError::Integrity {
            code: "executor_effect_step_missing".to_owned(),
            message: format!("Run {run_id} Effect step is missing"),
        })?;
    let intent_id = effect_intent_id(&EffectIntentIdentityInput {
        run_id,
        plan_id: &plan.plan_id,
        invocation_id: &frame.invocation_id,
        site_id: &step.id,
        scope_id: &frame.scope_id,
        occurrence: context.occurrence,
        args: &args.reference,
        effect_schema_version: EFFECT_SCHEMA_VERSION,
    })?;
    Ok(PreparedEffectStep {
        args,
        value,
        intent_id,
        execution_binding,
        occurrence_binding,
    })
}

fn terminal_resolution_observation(
    command: &EffectResolutionCommand,
    operation: &str,
    contracts: &PlanContracts,
    provider_attempt: &EffectProviderAttempt,
    response: RuntimeResult<PluginResponse>,
) -> DurableResult<(ReconciliationResolution, Option<ArtifactRecord>)> {
    let (resolution, value) = match response {
        Ok(PluginResponse::ReconciliationResult {
            attempt,
            resolution,
            value,
        }) if &attempt == provider_attempt
            && resolution != ReconciliationResolution::GovernanceRequired
            && validate_optional_reconciliation_output(
                contracts,
                operation,
                resolution,
                value.as_ref(),
            )
            .is_ok() =>
        {
            (resolution, value)
        }
        Ok(_) | Err(_) => {
            return Err(DurableError::ReconciliationRequired {
                intent_id: command.intent_id.clone(),
            });
        }
    };
    if resolution == ReconciliationResolution::StillUnknown {
        return Err(DurableError::ReconciliationRequired {
            intent_id: command.intent_id.clone(),
        });
    }
    let result = match resolution {
        ReconciliationResolution::ResolvedApplied => Some(immutable_artifact(
            EFFECT_RESULT_ARTIFACT_KIND,
            canonical_bytes(&value.unwrap_or(Value::Null))?,
        )?),
        ReconciliationResolution::ResolvedNotApplied => None,
        ReconciliationResolution::StillUnknown | ReconciliationResolution::GovernanceRequired => {
            return Err(DurableError::RuntimeDefect {
                code: "effect_resolution_provider_terminal_mismatch".to_owned(),
                message: "Effect provider did not return a terminal resolution".to_owned(),
            });
        }
    };
    Ok((resolution, result))
}

fn dispatch_origin_binding(entry: &ExecutorEffectRead) -> DurableResult<ExecutionBinding> {
    let origin_binding = ExecutionBinding::decode(&entry.origin_binding.bytes)?;
    if origin_binding.artifact_ref()? != entry.dispatch.execution_binding
        || origin_binding
            .occurrence_binding(ExecutionOperationKind::Effect, &entry.dispatch.operation)?
            != entry.dispatch.occurrence_binding
    {
        return Err(DurableError::Integrity {
            code: "executor_effect_origin_binding_mismatch".to_owned(),
            message: format!(
                "Effect {} origin binding changed its exact occurrence authority",
                entry.dispatch.intent_id
            ),
        });
    }
    Ok(origin_binding)
}

fn dispatch_observation(
    contracts: &PlanContracts,
    operation: &str,
    provider_attempt: &EffectProviderAttempt,
    response: RuntimeResult<PluginResponse>,
) -> Option<(WorldOutcome, Option<Value>)> {
    match response {
        Ok(PluginResponse::EffectResult {
            attempt,
            outcome,
            value,
        }) if &attempt == provider_attempt => {
            if validate_optional_effect_output(contracts, operation, outcome, value.as_ref())
                .is_err()
            {
                None
            } else {
                Some((outcome, value))
            }
        }
        Ok(_) | Err(_) => None,
    }
}

fn reconciliation_observation(
    contracts: &PlanContracts,
    operation: &str,
    provider_attempt: &EffectProviderAttempt,
    response: RuntimeResult<PluginResponse>,
) -> DurableResult<Option<(ReconciliationResolution, Option<Value>)>> {
    match response {
        Ok(PluginResponse::ReconciliationResult {
            attempt,
            resolution,
            value,
        }) if &attempt == provider_attempt
            && resolution != ReconciliationResolution::GovernanceRequired
            && validate_optional_reconciliation_output(
                contracts,
                operation,
                resolution,
                value.as_ref(),
            )
            .is_ok() =>
        {
            Ok(Some((resolution, value)))
        }
        Ok(PluginResponse::ReconciliationResult {
            resolution: ReconciliationResolution::GovernanceRequired,
            ..
        }) => Err(DurableError::RuntimeDefect {
            code: "invalid_reconciliation_resolution".to_owned(),
            message: "Effect provider cannot author governance escalation".to_owned(),
        }),
        Ok(PluginResponse::Defect { code, message })
        | Err(cymule_runtime::RuntimeError::PluginDefect { code, message })
            if code == "invalid_reconciliation_resolution" =>
        {
            Err(DurableError::RuntimeDefect { code, message })
        }
        Ok(_) | Err(_) => Ok(None),
    }
}

pub(crate) fn validate_wait_completion(wait: &WaitCondition, value: &Value) -> DurableResult<()> {
    if let WaitKind::Input { schema, .. } = &wait.kind {
        ContractValidator::compile(ContractTarget::wait(&wait.wait_id), schema)?.validate(value)?;
    }
    Ok(())
}

fn validate_optional_effect_output(
    contracts: &PlanContracts,
    operation: &str,
    outcome: WorldOutcome,
    value: Option<&Value>,
) -> DurableResult<()> {
    match (outcome, value) {
        (WorldOutcome::Applied, Some(value)) => {
            contracts.validate_effect_output(operation, value)?;
        }
        (WorldOutcome::Applied, None) => {
            contracts.validate_effect_output(operation, &Value::Null)?;
        }
        (WorldOutcome::NotApplied | WorldOutcome::Unknown, None) => {}
        (WorldOutcome::NotApplied | WorldOutcome::Unknown, Some(_))
        | (WorldOutcome::Unobserved, _) => {
            return Err(DurableError::Validation(
                "NotApplied or Unknown Effect response cannot carry a result".to_owned(),
            ));
        }
    }
    Ok(())
}

fn validate_optional_reconciliation_output(
    contracts: &PlanContracts,
    operation: &str,
    resolution: ReconciliationResolution,
    value: Option<&Value>,
) -> DurableResult<()> {
    match (resolution, value) {
        (ReconciliationResolution::ResolvedApplied, Some(value)) => {
            contracts.validate_effect_output(operation, value)?;
        }
        (ReconciliationResolution::ResolvedApplied, None) => {
            contracts.validate_effect_output(operation, &Value::Null)?;
        }
        (
            ReconciliationResolution::ResolvedNotApplied
            | ReconciliationResolution::StillUnknown
            | ReconciliationResolution::GovernanceRequired,
            None,
        ) => {}
        (
            ReconciliationResolution::ResolvedNotApplied
            | ReconciliationResolution::StillUnknown
            | ReconciliationResolution::GovernanceRequired,
            Some(_),
        ) => {
            return Err(DurableError::Validation(
                "non-Applied reconciliation response cannot carry a result".to_owned(),
            ));
        }
    }
    Ok(())
}

fn region_at_path<'a>(root: &'a Region, path: &[usize]) -> DurableResult<&'a Region> {
    let mut region = root;
    for step_index in path {
        let step = region
            .steps
            .get(*step_index)
            .ok_or_else(|| DurableError::Integrity {
                code: "executor_region_path_step_missing".to_owned(),
                message: format!("persisted Region path references missing step {step_index}"),
            })?;
        let Operation::Scope { body, .. } = &step.operation else {
            return Err(DurableError::Integrity {
                code: "executor_region_path_step_kind_mismatch".to_owned(),
                message: format!("persisted Region path step {step_index} is not a Scope"),
            });
        };
        region = body;
    }
    Ok(region)
}

fn terminal_drive_outcome_from_read(read: &ExecutorRunRead) -> DurableResult<DriveOutcome> {
    match (&read.continuation.status, &read.run.execution_status) {
        (ContinuationStatus::Failed, cymule_core::RunExecutionStatus::Failed { failure }) => {
            Ok(DriveOutcome::Failed {
                failure: failure.clone(),
            })
        }
        (ContinuationStatus::Cancelled, cymule_core::RunExecutionStatus::Cancelled { reason }) => {
            Ok(DriveOutcome::Cancelled {
                reason: reason.clone(),
            })
        }
        _ => Err(DurableError::Integrity {
            code: "executor_terminal_state_mismatch".to_owned(),
            message: format!(
                "Run {} terminal Continuation and Core execution status disagree",
                read.run.run_id
            ),
        }),
    }
}

fn immutable_artifact(kind: &str, bytes: Vec<u8>) -> DurableResult<ArtifactRecord> {
    let record = ArtifactRecord {
        reference: artifact_ref(kind, &bytes)?,
        bytes,
    };
    record.validate()?;
    Ok(record)
}

fn read_step_value(
    read: &ExecutorStepRead,
    reference: &cymule_core::ArtifactRef,
) -> DurableResult<Value> {
    let record = read
        .referenced_artifacts
        .get(&reference.artifact_id)
        .ok_or_else(|| DurableError::Integrity {
            code: "executor_step_artifact_not_loaded".to_owned(),
            message: format!(
                "Run {} exact step read omitted Artifact {}",
                read.run.run.run_id, reference.artifact_id
            ),
        })?;
    decode_artifact_value(reference, record)
}

fn evaluate_step(
    read: &ExecutorStepRead,
    expression: &Expression,
    input: &Value,
    locals: &BTreeMap<String, cymule_core::ArtifactRef>,
) -> DurableResult<Value> {
    let mut load = |reference: &cymule_core::ArtifactRef| {
        read.referenced_artifacts
            .get(&reference.artifact_id)
            .cloned()
            .ok_or_else(|| DurableError::Integrity {
                code: "executor_step_artifact_not_loaded".to_owned(),
                message: format!(
                    "Run {} exact step read omitted Artifact {}",
                    read.run.run.run_id, reference.artifact_id
                ),
            })
    };
    evaluate_expression_with(expression, input, locals, &mut load)
}
