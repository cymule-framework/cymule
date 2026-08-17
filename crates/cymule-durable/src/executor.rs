use std::collections::{BTreeMap, BTreeSet};

use cymule_core::{
    COMMAND_VERSION, Command, CommandEnvelope, CommandReceiptStatus, DispatchPolicy,
    EffectTransition, Expression, Machine, MutationKind, Operation, PlanCandidate, ROOT_SCOPE_ID,
    ReconciliationResolution, Region, SealedPlan, WaitSpec, WorldOutcome, canonical_bytes,
    content_id, effect_intent_id,
};
use cymule_runtime::{ExecutionResult, PluginHost, PluginManifest, PluginRequest, PluginResponse};
use serde::Serialize;
use serde_json::Value;

use crate::{
    ComponentOccurrence, Continuation, ContinuationStatus, DurableCoordinator, DurableError,
    DurableResult, DurableStore, EffectDispatch, FrameState, OutboxState, WaitActivation,
    WaitActivationSource, WaitCondition, WaitKind, WaitState,
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
    /// Run has committed scopes with effects awaiting an explicit release.
    ReleaseRequired {
        /// Structural intents that the caller may release independently.
        intent_ids: BTreeSet<String>,
    },
    /// Run reached a terminal Result.
    Completed(ExecutionResult),
}

/// Resumable sequential interpreter backed by a provider-neutral durable store.
/// Frame paths and the scope stack are persisted so nested regions can resume
/// without reconstructing a host-language call stack.
pub struct ResumableRuntime<S, P> {
    coordinator: DurableCoordinator<S>,
    plugin: P,
    manifest: PluginManifest,
}

impl<S: DurableStore, P: PluginHost> ResumableRuntime<S, P> {
    /// Open a durable runtime over an existing or empty store.
    pub fn open(store: S, mut plugin: P) -> DurableResult<Self> {
        let manifest = plugin
            .describe()
            .map_err(|error| DurableError::Substrate(error.to_string()))?;
        Ok(Self {
            coordinator: DurableCoordinator::open(store)?,
            plugin,
            manifest,
        })
    }

    /// Seal and start a new Run, then drive it to wait or completion.
    pub fn start(
        &mut self,
        candidate: PlanCandidate,
        input: &Value,
        run_id: impl Into<String>,
    ) -> DurableResult<DriveOutcome> {
        if self.coordinator.revision().is_some() {
            return Err(DurableError::IllegalTransition(
                "start currently requires an empty durable store".to_owned(),
            ));
        }
        let run_id = run_id.into();
        let mut machine = Machine::new();
        let plan = machine.seal_plan(candidate)?;
        validate_manifest(&plan, &self.manifest)?;
        submit(
            &mut machine,
            &run_id,
            format!("{run_id}:start"),
            Command::StartRun {
                plan_id: plan.plan_id.clone(),
                binding_context: format!("binding:plugin/{}", self.manifest.implementation_id),
            },
        )?;
        begin_attempt(&mut machine, &run_id, &self.manifest, 0)?;
        let input_ref = machine.put_artifact("cymule.input/1", canonical_bytes(input)?);
        let continuation = Continuation {
            run_id: run_id.clone(),
            plan_id: plan.plan_id,
            binding_context: format!("binding:plugin/{}", self.manifest.implementation_id),
            frames: vec![FrameState {
                invocation_id: plan.candidate.entry,
                region_path: Vec::new(),
                next_step: 0,
                locals: BTreeMap::new(),
            }],
            state: Some(input_ref),
            wait_set: BTreeSet::new(),
            scope_stack: vec![ROOT_SCOPE_ID.to_owned()],
            effect_obligations: BTreeSet::new(),
            authority_leases: BTreeSet::new(),
            budget: BTreeMap::new(),
            causal_frontier: BTreeSet::new(),
            epoch: 0,
            status: ContinuationStatus::Running,
        };
        self.coordinator.initialize_in_place(&machine)?;
        self.coordinator.put_continuation(continuation)?;
        self.drive(&run_id)
    }

    /// Complete a durable wait with typed JSON and resume its owning Run.
    pub fn complete_wait(&mut self, wait_id: &str, value: &Value) -> DurableResult<DriveOutcome> {
        let mut machine = self.coordinator.restore_machine()?;
        let result = machine.put_artifact("cymule.wait-result/1", canonical_bytes(value)?);
        let run_id = self
            .coordinator
            .state()?
            .waits
            .get(wait_id)
            .ok_or_else(|| DurableError::NotFound(format!("wait {wait_id} does not exist")))?
            .run_id
            .clone();
        self.coordinator
            .complete_wait_with_machine(&machine, wait_id, result)?;
        self.resume(&run_id)
    }

    /// Admit one externally identified signal or timer delivery.
    ///
    /// The activation and all selected wait completions enter one durable CAS
    /// revision. This method does not run ready Continuations; the caller or
    /// scheduler may resume each returned Run independently. Concrete signal
    /// and clock substrates remain plugins and can safely redeliver the same
    /// activation ID after losing an acknowledgement.
    pub fn admit_wait_activation(
        &mut self,
        activation_id: impl Into<String>,
        source: WaitActivationSource,
        wait_ids: BTreeSet<String>,
        value: &Value,
    ) -> DurableResult<BTreeSet<String>> {
        let mut machine = self.coordinator.restore_machine()?;
        let result =
            machine.put_artifact("cymule.wait-activation-result/1", canonical_bytes(value)?);
        let mut run_ids = BTreeSet::new();
        for wait_id in &wait_ids {
            let wait = self
                .coordinator
                .state()?
                .waits
                .get(wait_id)
                .ok_or_else(|| DurableError::NotFound(format!("wait {wait_id} does not exist")))?;
            run_ids.insert(wait.run_id.clone());
        }
        let activation = WaitActivation::new(activation_id, source, wait_ids, result)?;
        self.coordinator.activate_waits(&machine, activation)?;
        let state = self.coordinator.state()?;
        run_ids.retain(|run_id| {
            state
                .continuations
                .get(run_id)
                .is_some_and(|continuation| continuation.status == ContinuationStatus::Ready)
        });
        Ok(run_ids)
    }

    /// Resume an existing ready/running Run after process reopen or a recoverable
    /// adapter failure.
    pub fn resume(&mut self, run_id: &str) -> DurableResult<DriveOutcome> {
        let status = self
            .coordinator
            .state()?
            .continuations
            .get(run_id)
            .ok_or_else(|| DurableError::NotFound(format!("continuation {run_id} is missing")))?
            .status;
        match status {
            ContinuationStatus::Ready => {
                let mut machine = self.coordinator.restore_machine()?;
                let mut continuation = self.coordinator.state()?.continuations[run_id].clone();
                let epoch = continuation.epoch + 1;
                submit(
                    &mut machine,
                    run_id,
                    format!("{run_id}:advance:{epoch}"),
                    Command::AdvanceEpoch,
                )?;
                begin_attempt(&mut machine, run_id, &self.manifest, epoch)?;
                continuation.epoch = epoch;
                continuation.status = ContinuationStatus::Running;
                self.coordinator.checkpoint(&machine, continuation, None)?;
            }
            ContinuationStatus::Running => {}
            ContinuationStatus::Waiting => {
                return Err(DurableError::IllegalTransition(format!(
                    "continuation {run_id} is still waiting"
                )));
            }
            ContinuationStatus::Completed => {
                return Err(DurableError::IllegalTransition(format!(
                    "continuation {run_id} is already completed"
                )));
            }
        }
        self.drive(run_id)
    }

    /// Explicitly release one prepared effect after its owning scope commits.
    ///
    /// The release is idempotent after a lost receipt. Once the fenced claim is
    /// durable, recovery reconciles under that claim and never redispatches the
    /// semantic intent.
    pub fn release_effect(&mut self, intent_id: &str) -> DurableResult<DriveOutcome> {
        let machine = self.coordinator.restore_machine()?;
        let state = self.coordinator.state()?;
        let dispatch = state
            .outbox
            .get(intent_id)
            .ok_or_else(|| DurableError::NotFound(format!("effect {intent_id} is missing")))?;
        let run_id = dispatch.run_id.clone();
        let run = machine
            .projection()
            .runs
            .get(&run_id)
            .ok_or_else(|| DurableError::NotFound(format!("Run {run_id} is missing")))?;
        let effect = run
            .effects
            .get(intent_id)
            .ok_or_else(|| DurableError::NotFound(format!("effect {intent_id} is missing")))?;
        let contract = effect_contract(&machine, run, &effect.operation)?;
        if contract.profile.dispatch != DispatchPolicy::Explicit {
            return Err(DurableError::IllegalTransition(format!(
                "effect {intent_id} does not require explicit release"
            )));
        }
        let scope = run.scopes.get(&effect.scope_id).ok_or_else(|| {
            DurableError::NotFound(format!("scope {} is missing", effect.scope_id))
        })?;
        if scope.status != cymule_core::ScopeStatus::ClosedCommitted {
            return Err(DurableError::IllegalTransition(format!(
                "effect {intent_id} cannot release before its scope commits"
            )));
        }
        let status = state
            .continuations
            .get(&run_id)
            .ok_or_else(|| DurableError::NotFound(format!("continuation {run_id} is missing")))?
            .status;
        if status == ContinuationStatus::Completed {
            return Ok(DriveOutcome::Completed(completed_result(
                &machine, &run_id,
            )?));
        }
        if let Some(outcome) = self.dispatch_outbox(&run_id, Some(intent_id))? {
            return Ok(outcome);
        }
        self.drive(&run_id)
    }

    /// Access durable state for inspection.
    pub const fn coordinator(&self) -> &DurableCoordinator<S> {
        &self.coordinator
    }

    /// Consume the runtime and return its store and plugin.
    pub fn into_parts(self) -> (S, P) {
        (self.coordinator.into_store(), self.plugin)
    }

    fn drive(&mut self, run_id: &str) -> DurableResult<DriveOutcome> {
        loop {
            let mut machine = self.coordinator.restore_machine()?;
            let mut continuation = self.coordinator.state()?.continuations[run_id].clone();
            let plan = machine
                .plan(&continuation.plan_id)
                .cloned()
                .ok_or_else(|| DurableError::NotFound("continuation Plan is missing".to_owned()))?;
            let definition = plan
                .candidate
                .definitions
                .iter()
                .find(|definition| definition.id == plan.candidate.entry)
                .ok_or_else(|| {
                    DurableError::Validation("entry definition is missing".to_owned())
                })?;
            let input = read_value(
                &machine,
                continuation.state.as_ref().ok_or_else(|| {
                    DurableError::Validation("input artifact is missing".to_owned())
                })?,
            )?;
            let frame_index =
                continuation.frames.len().checked_sub(1).ok_or_else(|| {
                    DurableError::Validation("continuation has no frame".to_owned())
                })?;
            let region = region_at_path(
                &definition.body,
                &continuation.frames[frame_index].region_path,
            )?
            .clone();
            let current_scope =
                continuation.scope_stack.last().cloned().ok_or_else(|| {
                    DurableError::Validation("continuation has no scope".to_owned())
                })?;
            let frame = continuation
                .frames
                .get_mut(frame_index)
                .expect("frame index exists");

            let Some(step) = region.steps.get(frame.next_step) else {
                let value = evaluate(&machine, &region.result, &input, &frame.locals)?;
                if frame_index > 0 {
                    let parent_index = frame_index - 1;
                    let parent_path = continuation.frames[parent_index].region_path.clone();
                    let parent_step_index = continuation.frames[parent_index].next_step;
                    let parent_region = region_at_path(&definition.body, &parent_path)?;
                    let parent_step =
                        parent_region.steps.get(parent_step_index).ok_or_else(|| {
                            DurableError::Validation(
                                "parent scope frame no longer points at its child step".to_owned(),
                            )
                        })?;
                    let Operation::Scope { bind, .. } = &parent_step.operation else {
                        return Err(DurableError::Validation(
                            "parent scope frame points at a non-scope step".to_owned(),
                        ));
                    };
                    submit(
                        &mut machine,
                        run_id,
                        format!("{run_id}:commit:{current_scope}:{}", continuation.epoch),
                        Command::CommitScope {
                            scope_id: current_scope.clone(),
                        },
                    )?;
                    let result_ref =
                        machine.put_artifact("cymule.scope-result/1", canonical_bytes(&value)?);
                    continuation.frames.pop();
                    continuation.scope_stack.pop();
                    let parent = continuation
                        .frames
                        .get_mut(parent_index)
                        .expect("parent frame remains");
                    parent.next_step += 1;
                    if let Some(binding) = bind {
                        parent.locals.insert(binding.clone(), result_ref);
                    }
                    self.coordinator
                        .checkpoint(&machine, continuation.clone(), None)?;
                    if let Some(outcome) = self.dispatch_outbox(run_id, None)? {
                        return Ok(outcome);
                    }
                    continue;
                }
                if machine.projection().runs[run_id].scopes[ROOT_SCOPE_ID].status
                    == cymule_core::ScopeStatus::Open
                {
                    submit(
                        &mut machine,
                        run_id,
                        format!("{run_id}:commit-root:{}", continuation.epoch),
                        Command::CommitScope {
                            scope_id: ROOT_SCOPE_ID.to_owned(),
                        },
                    )?;
                    self.coordinator
                        .checkpoint(&machine, continuation.clone(), None)?;
                }
                if let Some(outcome) = self.dispatch_outbox(run_id, None)? {
                    return Ok(outcome);
                }
                let explicit = pending_explicit_effects(&machine, run_id)?;
                if !explicit.is_empty() {
                    return Ok(DriveOutcome::ReleaseRequired {
                        intent_ids: explicit,
                    });
                }
                machine = self.coordinator.restore_machine()?;
                yield_attempt(&mut machine, run_id, continuation.epoch)?;
                let result_ref = machine.put_artifact("cymule.result/1", canonical_bytes(&value)?);
                submit(
                    &mut machine,
                    run_id,
                    format!("{run_id}:complete:{}", continuation.epoch),
                    Command::CompleteRun {
                        result: Some(result_ref),
                    },
                )?;
                continuation.status = ContinuationStatus::Completed;
                self.coordinator.checkpoint(&machine, continuation, None)?;
                machine.verify_replay()?;
                let run = &machine.projection().runs[run_id];
                return Ok(DriveOutcome::Completed(ExecutionResult {
                    run_id: run_id.to_owned(),
                    plan_id: plan.plan_id,
                    value,
                    projection_digest: machine.projection().digest()?,
                    precondition_token: run.precondition_token(),
                    effects: run.effects.keys().cloned().collect(),
                }));
            };

            match &step.operation {
                Operation::Call {
                    component,
                    input: expression,
                    bind,
                } => {
                    let value = evaluate(&machine, expression, &input, &frame.locals)?;
                    let input_ref =
                        machine.put_artifact("cymule.component-input/1", canonical_bytes(&value)?);
                    let operation = self.manifest.components.get(component).ok_or_else(|| {
                        DurableError::Validation(format!(
                            "plugin does not implement component {component}"
                        ))
                    })?;
                    let occurrence_binding = format!(
                        "binding:{}/component/{}/{}",
                        self.manifest.implementation_id,
                        component,
                        operation.implementation_revision
                    );
                    let occurrence_id = component_occurrence_id(
                        run_id,
                        &step.id,
                        continuation.epoch,
                        &input_ref,
                        &occurrence_binding,
                    )?;
                    let (output_ref, occurrence) = if let Some(recorded) = self
                        .coordinator
                        .state()?
                        .component_occurrences
                        .get(&occurrence_id)
                    {
                        (recorded.output.clone(), None)
                    } else {
                        let response = self
                            .plugin
                            .invoke(PluginRequest::Call {
                                component: component.clone(),
                                input: value,
                            })
                            .map_err(|error| DurableError::Substrate(error.to_string()))?;
                        let PluginResponse::CallResult { value } = response else {
                            return Err(DurableError::Substrate(format!(
                                "component {component} returned {response:?}"
                            )));
                        };
                        let output_ref = machine
                            .put_artifact("cymule.component-output/1", canonical_bytes(&value)?);
                        let occurrence = ComponentOccurrence {
                            occurrence_id,
                            run_id: run_id.to_owned(),
                            site_id: step.id.clone(),
                            component: component.clone(),
                            input: input_ref,
                            output: output_ref.clone(),
                            occurrence_binding,
                            implementation_revision: operation.implementation_revision.clone(),
                        };
                        (output_ref, Some(occurrence))
                    };
                    if let Some(binding) = bind {
                        frame.locals.insert(binding.clone(), output_ref);
                    }
                    frame.next_step += 1;
                    self.coordinator
                        .checkpoint(&machine, continuation, occurrence)?;
                }
                Operation::Wait { wait } => {
                    frame.next_step += 1;
                    yield_attempt(&mut machine, run_id, continuation.epoch)?;
                    let wait_id = wait_id(run_id, &step.id, continuation.epoch)?;
                    let condition = WaitCondition {
                        wait_id: wait_id.clone(),
                        run_id: run_id.to_owned(),
                        kind: match wait {
                            WaitSpec::Signal { key, .. } => WaitKind::Signal { key: key.clone() },
                            WaitSpec::Timer { timer_id } => WaitKind::Timer {
                                timer_id: timer_id.clone(),
                            },
                            WaitSpec::Input {
                                correlation,
                                schema,
                            } => WaitKind::Input {
                                correlation: correlation.clone(),
                                schema: schema.clone(),
                            },
                        },
                        consume_once: matches!(
                            wait,
                            WaitSpec::Signal {
                                consume_once: true,
                                ..
                            }
                        ),
                        state: WaitState::Pending,
                        result: None,
                    };
                    self.coordinator.park(&machine, continuation, condition)?;
                    return Ok(DriveOutcome::Suspended { wait_id });
                }
                Operation::Effect {
                    effect,
                    input: expression,
                    occurrence,
                    bind,
                } => {
                    let contract = plan
                        .candidate
                        .effects
                        .iter()
                        .find(|contract| contract.id == *effect)
                        .ok_or_else(|| {
                            DurableError::Validation(format!("effect contract {effect} is missing"))
                        })?;
                    let eager = contract.profile.mutation == MutationKind::Observational
                        && contract.profile.dispatch == DispatchPolicy::Eager;
                    if bind.is_some() && !eager {
                        return Err(DurableError::Validation(
                            "deferred durable effects cannot bind inside their open scope"
                                .to_owned(),
                        ));
                    }
                    let value = evaluate(&machine, expression, &input, &frame.locals)?;
                    let args =
                        machine.put_artifact("cymule.effect-args/1", canonical_bytes(&value)?);
                    let implementation = self.manifest.effects.get(effect).ok_or_else(|| {
                        DurableError::Validation(format!(
                            "plugin does not implement effect {effect}"
                        ))
                    })?;
                    let occurrence_binding = format!(
                        "binding:{}/effect/{}/{}",
                        self.manifest.implementation_id,
                        effect,
                        implementation.implementation_revision
                    );
                    let intent_id = effect_intent_id(
                        run_id,
                        &plan.candidate.entry,
                        &step.id,
                        &current_scope,
                        continuation.epoch,
                        occurrence,
                        &args,
                        "cymule.effect-schema/1",
                    )?;
                    if eager
                        && let Some(dispatch) =
                            self.coordinator.state()?.outbox.get(&intent_id).cloned()
                    {
                        match dispatch.state {
                            OutboxState::Applied => {
                                if let Some(binding) = bind {
                                    let result = dispatch.result.ok_or_else(|| {
                                        DurableError::Validation(format!(
                                            "eager effect {intent_id} produced no bound result"
                                        ))
                                    })?;
                                    frame.locals.insert(binding.clone(), result);
                                }
                                frame.next_step += 1;
                                self.coordinator.checkpoint(&machine, continuation, None)?;
                            }
                            OutboxState::NotApplied if bind.is_none() => {
                                frame.next_step += 1;
                                self.coordinator.checkpoint(&machine, continuation, None)?;
                            }
                            OutboxState::NotApplied => {
                                return Err(DurableError::IllegalTransition(format!(
                                    "eager effect {intent_id} was not applied and has no result"
                                )));
                            }
                            OutboxState::Pending | OutboxState::Claimed | OutboxState::Unknown => {
                                if let Some(outcome) = self.dispatch_outbox(run_id, None)? {
                                    return Ok(outcome);
                                }
                            }
                        }
                        continue;
                    }
                    submit(
                        &mut machine,
                        run_id,
                        format!("{run_id}:effect-propose:{}", step.id),
                        Command::ProposeEffect {
                            scope_id: current_scope.clone(),
                            invocation_id: plan.candidate.entry.clone(),
                            site_id: step.id.clone(),
                            occurrence: occurrence.clone(),
                            operation: effect.clone(),
                            args: args.clone(),
                            occurrence_binding: occurrence_binding.clone(),
                        },
                    )?;
                    let response = self
                        .plugin
                        .invoke(PluginRequest::PrepareEffect {
                            operation: effect.clone(),
                            intent_id: intent_id.clone(),
                            input: value,
                        })
                        .map_err(|error| DurableError::Substrate(error.to_string()))?;
                    if response != PluginResponse::Prepared {
                        return Err(DurableError::Substrate(format!(
                            "effect {effect} prepare returned {response:?}"
                        )));
                    }
                    submit(
                        &mut machine,
                        run_id,
                        format!("{run_id}:effect-prepare:{}", step.id),
                        Command::TransitionEffect {
                            intent_id: intent_id.clone(),
                            transition: EffectTransition::Prepare,
                        },
                    )?;
                    if !eager {
                        frame.next_step += 1;
                    }
                    self.coordinator.checkpoint_effect_enqueue(
                        &machine,
                        continuation.clone(),
                        EffectDispatch {
                            intent_id,
                            run_id: run_id.to_owned(),
                            operation: effect.clone(),
                            input: args,
                            occurrence_binding,
                            state: OutboxState::Pending,
                            claim_epoch: 0,
                            claim_owner: None,
                            result: None,
                        },
                    )?;
                    if eager && let Some(outcome) = self.dispatch_outbox(run_id, None)? {
                        return Ok(outcome);
                    }
                }
                Operation::Scope { .. } => {
                    let mut child_path = frame.region_path.clone();
                    child_path.push(frame.next_step);
                    let child_scope = durable_scope_id(
                        run_id,
                        &frame.invocation_id,
                        &child_path,
                        &step.id,
                        continuation.epoch,
                    )?;
                    submit(
                        &mut machine,
                        run_id,
                        format!("{run_id}:open:{child_scope}:{}", continuation.epoch),
                        Command::OpenScope {
                            scope_id: child_scope.clone(),
                            parent_scope: current_scope,
                        },
                    )?;
                    let child_locals = frame.locals.clone();
                    let invocation_id = frame.invocation_id.clone();
                    continuation.scope_stack.push(child_scope);
                    continuation.frames.push(FrameState {
                        invocation_id,
                        region_path: child_path,
                        next_step: 0,
                        locals: child_locals,
                    });
                    self.coordinator.checkpoint(&machine, continuation, None)?;
                }
            }
        }
    }

    fn dispatch_outbox(
        &mut self,
        run_id: &str,
        explicit_release: Option<&str>,
    ) -> DurableResult<Option<DriveOutcome>> {
        let scheduling_machine = self.coordinator.restore_machine()?;
        let scheduling_run = scheduling_machine
            .projection()
            .runs
            .get(run_id)
            .ok_or_else(|| DurableError::NotFound(format!("Run {run_id} is missing")))?;
        let mut entries = Vec::new();
        for dispatch in self.coordinator.state()?.outbox.values() {
            if dispatch.run_id != run_id {
                continue;
            }
            let effect = scheduling_run
                .effects
                .get(&dispatch.intent_id)
                .ok_or_else(|| {
                    DurableError::NotFound(format!("effect {} is missing", dispatch.intent_id))
                })?;
            let contract = effect_contract(&scheduling_machine, scheduling_run, &effect.operation)?;
            let scope = scheduling_run.scopes.get(&effect.scope_id).ok_or_else(|| {
                DurableError::NotFound(format!("scope {} is missing", effect.scope_id))
            })?;
            let eligible = match dispatch.state {
                OutboxState::Pending => match contract.profile.dispatch {
                    DispatchPolicy::Eager => {
                        contract.profile.mutation == MutationKind::Observational
                    }
                    DispatchPolicy::OnScopeCommit => {
                        scope.status == cymule_core::ScopeStatus::ClosedCommitted
                    }
                    DispatchPolicy::Explicit => {
                        scope.status == cymule_core::ScopeStatus::ClosedCommitted
                            && explicit_release == Some(dispatch.intent_id.as_str())
                    }
                },
                OutboxState::Claimed | OutboxState::Unknown => true,
                OutboxState::Applied | OutboxState::NotApplied => false,
            };
            if eligible {
                entries.push(dispatch.clone());
            }
        }
        for entry in entries {
            let mut machine = self.coordinator.restore_machine()?;
            let input = read_value(&machine, &entry.input)?;
            let (owner, claim_epoch) = if entry.state == OutboxState::Pending {
                let owner = "dispatcher:durable-runtime";
                let lease = self.coordinator.acquire_lease(
                    &format!("effect:{}", entry.intent_id),
                    owner,
                    self.coordinator.state()?.continuations[run_id].epoch,
                    1,
                )?;
                submit(
                    &mut machine,
                    run_id,
                    format!("{run_id}:effect-authorize:{}", entry.intent_id),
                    Command::TransitionEffect {
                        intent_id: entry.intent_id.clone(),
                        transition: EffectTransition::AuthorizeRelease,
                    },
                )?;
                submit(
                    &mut machine,
                    run_id,
                    format!("{run_id}:effect-dispatch-start:{}", entry.intent_id),
                    Command::TransitionEffect {
                        intent_id: entry.intent_id.clone(),
                        transition: EffectTransition::StartDispatch,
                    },
                )?;
                self.coordinator.checkpoint_effect_claim(
                    &machine,
                    &entry.intent_id,
                    owner,
                    lease.epoch,
                )?;
                let response = self
                    .plugin
                    .invoke(PluginRequest::DispatchEffect {
                        operation: entry.operation.clone(),
                        intent_id: entry.intent_id.clone(),
                        input: input.clone(),
                    })
                    .map_err(|error| DurableError::Substrate(error.to_string()))?;
                let PluginResponse::EffectResult { outcome, value } = response else {
                    return Err(DurableError::Substrate(format!(
                        "effect {} dispatch returned {response:?}",
                        entry.operation
                    )));
                };
                if outcome != WorldOutcome::Unknown {
                    submit(
                        &mut machine,
                        run_id,
                        format!("{run_id}:effect-observe:{}", entry.intent_id),
                        Command::TransitionEffect {
                            intent_id: entry.intent_id.clone(),
                            transition: EffectTransition::Observe(outcome),
                        },
                    )?;
                    let result = value
                        .map(|value| {
                            canonical_bytes(&value)
                                .map(|bytes| machine.put_artifact("cymule.effect-result/1", bytes))
                        })
                        .transpose()?;
                    self.coordinator.checkpoint_effect_settlement(
                        &machine,
                        &entry.intent_id,
                        owner,
                        lease.epoch,
                        if outcome == WorldOutcome::Applied {
                            OutboxState::Applied
                        } else {
                            OutboxState::NotApplied
                        },
                        result,
                    )?;
                    continue;
                }
                submit(
                    &mut machine,
                    run_id,
                    format!("{run_id}:effect-unknown:{}", entry.intent_id),
                    Command::TransitionEffect {
                        intent_id: entry.intent_id.clone(),
                        transition: EffectTransition::Observe(WorldOutcome::Unknown),
                    },
                )?;
                self.coordinator.checkpoint_effect_settlement(
                    &machine,
                    &entry.intent_id,
                    owner,
                    lease.epoch,
                    OutboxState::Unknown,
                    None,
                )?;
                (owner.to_owned(), lease.epoch)
            } else {
                let owner = entry.claim_owner.clone().ok_or_else(|| {
                    DurableError::Validation("claimed or unknown effect has no owner".to_owned())
                })?;
                let effect = &machine.projection().runs[run_id].effects[&entry.intent_id];
                if effect.outcome == WorldOutcome::Unobserved {
                    submit(
                        &mut machine,
                        run_id,
                        format!("{run_id}:effect-recovery-unknown:{}", entry.intent_id),
                        Command::TransitionEffect {
                            intent_id: entry.intent_id.clone(),
                            transition: EffectTransition::Observe(WorldOutcome::Unknown),
                        },
                    )?;
                    self.coordinator.checkpoint_effect_settlement(
                        &machine,
                        &entry.intent_id,
                        &owner,
                        entry.claim_epoch,
                        OutboxState::Unknown,
                        None,
                    )?;
                }
                (owner, entry.claim_epoch)
            };

            let response = self
                .plugin
                .invoke(PluginRequest::ReconcileEffect {
                    operation: entry.operation.clone(),
                    intent_id: entry.intent_id.clone(),
                    input,
                })
                .map_err(|error| DurableError::Substrate(error.to_string()))?;
            let PluginResponse::ReconciliationResult { resolution, value } = response else {
                return Err(DurableError::Substrate(format!(
                    "effect {} reconciliation returned {response:?}",
                    entry.operation
                )));
            };
            submit(
                &mut machine,
                run_id,
                format!(
                    "{run_id}:effect-reconcile:{}:{}",
                    entry.intent_id,
                    reconciliation_suffix(resolution)
                ),
                Command::TransitionEffect {
                    intent_id: entry.intent_id.clone(),
                    transition: EffectTransition::Reconcile(resolution),
                },
            )?;
            let result = value
                .map(|value| {
                    canonical_bytes(&value)
                        .map(|bytes| machine.put_artifact("cymule.effect-result/1", bytes))
                })
                .transpose()?;
            let settled = match resolution {
                ReconciliationResolution::ResolvedApplied => OutboxState::Applied,
                ReconciliationResolution::ResolvedNotApplied => OutboxState::NotApplied,
                ReconciliationResolution::StillUnknown
                | ReconciliationResolution::GovernanceRequired => OutboxState::Unknown,
            };
            self.coordinator.checkpoint_effect_settlement(
                &machine,
                &entry.intent_id,
                &owner,
                claim_epoch,
                settled,
                result,
            )?;
            if settled == OutboxState::Unknown {
                return Ok(Some(DriveOutcome::ReconciliationRequired {
                    intent_id: entry.intent_id,
                }));
            }
        }
        Ok(None)
    }
}

const fn reconciliation_suffix(resolution: ReconciliationResolution) -> &'static str {
    match resolution {
        ReconciliationResolution::ResolvedApplied => "resolved-applied",
        ReconciliationResolution::ResolvedNotApplied => "resolved-not-applied",
        ReconciliationResolution::StillUnknown => "still-unknown",
        ReconciliationResolution::GovernanceRequired => "governance-required",
    }
}

fn region_at_path<'a>(root: &'a Region, path: &[usize]) -> DurableResult<&'a Region> {
    let mut region = root;
    for step_index in path {
        let step = region.steps.get(*step_index).ok_or_else(|| {
            DurableError::Validation(format!(
                "nested region path references missing step {step_index}"
            ))
        })?;
        let Operation::Scope { body, .. } = &step.operation else {
            return Err(DurableError::Validation(format!(
                "nested region path step {step_index} is not a scope"
            )));
        };
        region = body;
    }
    Ok(region)
}

fn durable_scope_id(
    run_id: &str,
    invocation_id: &str,
    region_path: &[usize],
    step_id: &str,
    epoch: u64,
) -> DurableResult<String> {
    content_id(
        "cymule.durable-scope/1",
        &(run_id, invocation_id, region_path, step_id, epoch),
    )
    .map_err(Into::into)
}

fn effect_contract<'a>(
    machine: &'a Machine,
    run: &cymule_core::RunProjection,
    operation: &str,
) -> DurableResult<&'a cymule_core::EffectContract> {
    let plan = machine
        .plan(&run.current_plan)
        .ok_or_else(|| DurableError::NotFound(format!("Plan {} is missing", run.current_plan)))?;
    plan.candidate
        .effects
        .iter()
        .find(|contract| contract.id == operation)
        .ok_or_else(|| DurableError::NotFound(format!("effect contract {operation} is missing")))
}

fn pending_explicit_effects(machine: &Machine, run_id: &str) -> DurableResult<BTreeSet<String>> {
    let run = machine
        .projection()
        .runs
        .get(run_id)
        .ok_or_else(|| DurableError::NotFound(format!("Run {run_id} is missing")))?;
    run.effects
        .values()
        .filter(|effect| effect.phase == cymule_core::EffectPhase::Prepared)
        .filter_map(|effect| {
            let result = effect_contract(machine, run, &effect.operation);
            match result {
                Ok(contract) if contract.profile.dispatch == DispatchPolicy::Explicit => {
                    Some(Ok(effect.intent_id.clone()))
                }
                Ok(_) => None,
                Err(error) => Some(Err(error)),
            }
        })
        .collect()
}

fn completed_result(machine: &Machine, run_id: &str) -> DurableResult<ExecutionResult> {
    let run = machine
        .projection()
        .runs
        .get(run_id)
        .ok_or_else(|| DurableError::NotFound(format!("Run {run_id} is missing")))?;
    let value = run
        .result
        .as_ref()
        .map(|reference| read_value(machine, reference))
        .transpose()?
        .unwrap_or(Value::Null);
    Ok(ExecutionResult {
        run_id: run_id.to_owned(),
        plan_id: run.current_plan.clone(),
        value,
        projection_digest: machine.projection().digest()?,
        precondition_token: run.precondition_token(),
        effects: run.effects.keys().cloned().collect(),
    })
}

fn validate_manifest(plan: &SealedPlan, manifest: &PluginManifest) -> DurableResult<()> {
    for component in &plan.candidate.components {
        if !manifest.components.contains_key(&component.id) {
            return Err(DurableError::Validation(format!(
                "plugin does not implement component {}",
                component.id
            )));
        }
    }
    for effect in &plan.candidate.effects {
        if !manifest.effects.contains_key(&effect.id) {
            return Err(DurableError::Validation(format!(
                "plugin does not implement effect {}",
                effect.id
            )));
        }
    }
    Ok(())
}

fn submit(
    machine: &mut Machine,
    run_id: &str,
    command_id: String,
    command: Command,
) -> DurableResult<()> {
    let expected_precondition = if matches!(command, Command::StartRun { .. }) {
        None
    } else {
        Some(
            machine
                .projection()
                .runs
                .get(run_id)
                .ok_or_else(|| DurableError::NotFound(format!("Run {run_id} is missing")))?
                .precondition_token(),
        )
    };
    let receipt = machine.submit(CommandEnvelope {
        command_version: COMMAND_VERSION.to_owned(),
        command_id,
        actor: "actor:durable-runtime".to_owned(),
        run_id: run_id.to_owned(),
        expected_precondition,
        command,
    })?;
    if receipt.status != CommandReceiptStatus::Applied {
        return Err(DurableError::Conflict {
            expected: receipt.observed_precondition,
            current: receipt.current_precondition,
        });
    }
    Ok(())
}

fn begin_attempt(
    machine: &mut Machine,
    run_id: &str,
    manifest: &PluginManifest,
    epoch: u64,
) -> DurableResult<()> {
    submit(
        machine,
        run_id,
        format!("{run_id}:begin-attempt:{epoch}"),
        Command::BeginAttempt {
            attempt_id: format!("attempt:{run_id}:{epoch}"),
            continuation_id: format!("continuation:{run_id}"),
            occurrence_binding: format!("binding:{}/runtime", manifest.implementation_id),
            epoch,
        },
    )
}

fn yield_attempt(machine: &mut Machine, run_id: &str, epoch: u64) -> DurableResult<()> {
    submit(
        machine,
        run_id,
        format!("{run_id}:yield-attempt:{epoch}"),
        Command::YieldAttempt {
            attempt_id: format!("attempt:{run_id}:{epoch}"),
            epoch,
        },
    )
}

fn read_value(machine: &Machine, reference: &cymule_core::ArtifactRef) -> DurableResult<Value> {
    let artifact = machine.artifact(reference).ok_or_else(|| {
        DurableError::NotFound(format!("artifact {} is missing", reference.artifact_id))
    })?;
    serde_json::from_slice(&artifact.bytes).map_err(Into::into)
}

fn evaluate(
    machine: &Machine,
    expression: &Expression,
    input: &Value,
    locals: &BTreeMap<String, cymule_core::ArtifactRef>,
) -> DurableResult<Value> {
    match expression {
        Expression::Input => Ok(input.clone()),
        Expression::Literal { value } => Ok(value.clone()),
        Expression::Binding { name } => read_value(
            machine,
            locals
                .get(name)
                .ok_or_else(|| DurableError::NotFound(format!("binding {name} is missing")))?,
        ),
        Expression::Object { fields } => fields
            .iter()
            .map(|(name, expression)| {
                evaluate(machine, expression, input, locals).map(|value| (name.clone(), value))
            })
            .collect::<DurableResult<serde_json::Map<String, Value>>>()
            .map(Value::Object),
        Expression::Array { items } => items
            .iter()
            .map(|expression| evaluate(machine, expression, input, locals))
            .collect::<DurableResult<Vec<_>>>()
            .map(Value::Array),
    }
}

fn wait_id(run_id: &str, site_id: &str, epoch: u64) -> DurableResult<String> {
    content_id("cymule.wait/1", &(run_id, site_id, epoch)).map_err(Into::into)
}

fn component_occurrence_id(
    run_id: &str,
    site_id: &str,
    epoch: u64,
    input: &cymule_core::ArtifactRef,
    binding: &str,
) -> DurableResult<String> {
    #[derive(Serialize)]
    struct Preimage<'a> {
        run_id: &'a str,
        site_id: &'a str,
        epoch: u64,
        input: &'a cymule_core::ArtifactRef,
        binding: &'a str,
    }
    content_id(
        "cymule.component-occurrence/1",
        &Preimage {
            run_id,
            site_id,
            epoch,
            input,
            binding,
        },
    )
    .map_err(Into::into)
}
