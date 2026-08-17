use std::collections::{BTreeMap, BTreeSet};

use cymule_core::{
    COMMAND_VERSION, Command, CommandEnvelope, CommandReceiptStatus, DispatchPolicy,
    EffectTransition, Expression, Machine, MutationKind, Operation, PlanCandidate, ROOT_SCOPE_ID,
    ReconciliationResolution, SealedPlan, WaitSpec, WorldOutcome, canonical_bytes, content_id,
    effect_intent_id,
};
use cymule_runtime::{ExecutionResult, PluginHost, PluginManifest, PluginRequest, PluginResponse};
use serde::Serialize;
use serde_json::Value;

use crate::{
    ComponentOccurrence, Continuation, ContinuationStatus, DurableCoordinator, DurableError,
    DurableResult, DurableStore, EffectDispatch, FrameState, OutboxState, WaitCondition, WaitKind,
    WaitState,
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
    /// Run reached a terminal Result.
    Completed(ExecutionResult),
}

/// Resumable sequential interpreter backed by a provider-neutral durable store.
/// Nested scopes and effects remain delegated to later M1 integration work.
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
        let epoch = self.coordinator.state()?.continuations[&run_id].epoch + 1;
        submit(
            &mut machine,
            &run_id,
            format!("{run_id}:advance:{epoch}"),
            Command::AdvanceEpoch,
        )?;
        begin_attempt(&mut machine, &run_id, &self.manifest, epoch)?;
        let mut continuation = self.coordinator.state()?.continuations[&run_id].clone();
        continuation.epoch = epoch;
        continuation.status = ContinuationStatus::Running;
        self.coordinator.checkpoint(&machine, continuation, None)?;
        self.drive(&run_id)
    }

    /// Resume an existing ready/running Run after process reopen or a recoverable
    /// adapter failure.
    pub fn resume(&mut self, run_id: &str) -> DurableResult<DriveOutcome> {
        self.drive(run_id)
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
            let frame = continuation
                .frames
                .last_mut()
                .ok_or_else(|| DurableError::Validation("continuation has no frame".to_owned()))?;

            let Some(step) = definition.body.steps.get(frame.next_step) else {
                let value = evaluate(&machine, &definition.body.result, &input, &frame.locals)?;
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
                if let Some(outcome) = self.dispatch_outbox(run_id)? {
                    return Ok(outcome);
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
                    if bind.is_some() {
                        return Err(DurableError::Validation(
                            "commit-gated durable effects cannot bind inside the open scope"
                                .to_owned(),
                        ));
                    }
                    let contract = plan
                        .candidate
                        .effects
                        .iter()
                        .find(|contract| contract.id == *effect)
                        .ok_or_else(|| {
                            DurableError::Validation(format!("effect contract {effect} is missing"))
                        })?;
                    if contract.profile.mutation != MutationKind::Mutating
                        || contract.profile.dispatch != DispatchPolicy::OnScopeCommit
                    {
                        return Err(DurableError::Validation(
                            "resumable M1 currently requires mutating on_scope_commit effects"
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
                        ROOT_SCOPE_ID,
                        continuation.epoch,
                        occurrence,
                        &args,
                        "cymule.effect-schema/1",
                    )?;
                    submit(
                        &mut machine,
                        run_id,
                        format!("{run_id}:effect-propose:{}", step.id),
                        Command::ProposeEffect {
                            scope_id: ROOT_SCOPE_ID.to_owned(),
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
                    frame.next_step += 1;
                    self.coordinator.checkpoint_effect_enqueue(
                        &machine,
                        continuation,
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
                }
                Operation::Scope { .. } => {
                    return Err(DurableError::Validation(format!(
                        "resumable M1 interpreter does not yet support step {}",
                        step.id
                    )));
                }
            }
        }
    }

    fn dispatch_outbox(&mut self, run_id: &str) -> DurableResult<Option<DriveOutcome>> {
        let entries: Vec<EffectDispatch> = self
            .coordinator
            .state()?
            .outbox
            .values()
            .filter(|dispatch| {
                dispatch.run_id == run_id
                    && matches!(dispatch.state, OutboxState::Pending | OutboxState::Claimed)
            })
            .cloned()
            .collect();
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
                self.coordinator.checkpoint(
                    &machine,
                    self.coordinator.state()?.continuations[run_id].clone(),
                    None,
                )?;
                (owner.to_owned(), lease.epoch)
            } else {
                let owner = entry.claim_owner.clone().ok_or_else(|| {
                    DurableError::Validation("claimed effect has no owner".to_owned())
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
                format!("{run_id}:effect-reconcile:{}", entry.intent_id),
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
