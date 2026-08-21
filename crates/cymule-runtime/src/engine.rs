use std::collections::BTreeMap;

use cymule_core::{
    COMMAND_VERSION, Command, CommandEnvelope, CommandReceiptStatus, DispatchPolicy,
    EffectTransition, Expression, InvocationPathSegment, Machine, MutationKind, Operation,
    PlanCandidate, ROOT_SCOPE_ID, ReconciliationMode, ReconciliationResolution, Region, SealedPlan,
    WorldOutcome, canonical_bytes, effect_intent_id, plan_invocation_id, plan_scope_id,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{
    ExecutionBinding, ExecutionOperationKind, PlanAdmissionResult, PlanContracts,
    PluginExpectedFailure, PluginHost, PluginRequest, PluginResponse, RuntimeError, RuntimeResult,
};

/// Verify canonical Plan identity and compile every executable contract.
pub fn verify_plan(plan: &SealedPlan) -> PlanAdmissionResult<PlanContracts> {
    plan.verify()?;
    PlanContracts::compile(&plan.candidate).map_err(Into::into)
}

/// Result returned by one-shot embedded execution.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExecutionResult {
    /// Run identity.
    pub run_id: String,
    /// Sealed plan identity.
    pub plan_id: String,
    /// Typed Flow result.
    pub value: Value,
    /// Deterministic replay projection digest.
    pub projection_digest: String,
    /// Final stale-action token.
    pub precondition_token: String,
    /// Structural effect intent identities admitted by the run.
    pub effects: Vec<String>,
}

/// Typed result of driving an Embedded Run to its next semantic boundary.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case", deny_unknown_fields)]
pub enum ExecutionOutcome {
    /// The Run reached its terminal typed result.
    Completed {
        /// Terminal execution result.
        result: ExecutionResult,
    },
    /// The Run reached a durable wait that the Embedded profile cannot resume.
    Suspended {
        /// Typed suspension contract for a durable runtime.
        suspension: SuspensionBoundary,
    },
    /// Prepared explicit effects require caller-owned durable release.
    ReleaseRequired {
        /// Exact release boundary.
        release: EffectReleaseBoundary,
    },
    /// An ambiguous effect requires reconciliation under its original intent.
    ReconciliationRequired {
        /// Exact reconciliation boundary.
        reconciliation: EffectReconciliationBoundary,
    },
}

impl ExecutionOutcome {
    /// Return a terminal result or the typed non-resumable Embedded boundary.
    pub fn into_completed(self) -> RuntimeResult<ExecutionResult> {
        match self {
            Self::Completed { result } => Ok(result),
            Self::Suspended { suspension } => Err(RuntimeError::Suspended(Box::new(suspension))),
            Self::ReleaseRequired { release } => Err(RuntimeError::ReleaseRequired {
                intent_ids: release.intent_ids,
            }),
            Self::ReconciliationRequired { reconciliation } => Err(RuntimeError::unknown_world(
                "effect_reconciliation_required",
                format!(
                    "effect {} requires reconciliation",
                    reconciliation.intent_id
                ),
            )),
        }
    }
}

/// A typed Embedded suspension boundary without a resumable Continuation.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SuspensionBoundary {
    /// Run identity that reached the boundary.
    pub run_id: String,
    /// Immutable Plan identity.
    pub plan_id: String,
    /// Definition containing the wait site.
    pub definition_id: String,
    /// Structural invocation containing the wait site.
    pub invocation_id: String,
    /// Stable wait operation site.
    pub site_id: String,
    /// Provider-neutral wait contract.
    pub wait: cymule_core::WaitSpec,
    /// Optional local binding for a future durable completion result.
    pub result_bind: Option<String>,
}

/// Typed Embedded boundary for explicit effects that remain prepared.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EffectReleaseBoundary {
    /// Run identity that owns the intents.
    pub run_id: String,
    /// Immutable Plan identity.
    pub plan_id: String,
    /// Exact stable intent identities requiring caller release.
    pub intent_ids: Vec<String>,
}

/// Typed Embedded boundary for one ambiguous effect intent.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EffectReconciliationBoundary {
    /// Run identity that owns the intent.
    pub run_id: String,
    /// Immutable Plan identity.
    pub plan_id: String,
    /// Original structural intent identity.
    pub intent_id: String,
}

#[derive(Debug)]
struct PendingEffect {
    intent_id: String,
    operation: String,
    input: Value,
    bind: Option<String>,
    dispatch: DispatchPolicy,
}

#[derive(Debug)]
enum RegionOutcome {
    Completed {
        value: Value,
        pending: Vec<PendingEffect>,
    },
    Suspended(SuspensionBoundary),
    ReconciliationRequired {
        intent_id: String,
    },
}

enum EffectDispatchOutcome {
    Settled(Option<Value>),
    ReconciliationRequired { intent_id: String },
}

enum PendingDispatchOutcome {
    Settled(Vec<PendingEffect>),
    ReconciliationRequired { intent_id: String },
}

/// Synchronous reference runtime over the trusted in-memory Machine.
pub struct EmbeddedRuntime<P: PluginHost> {
    machine: Machine,
    plugin: P,
    binding: ExecutionBinding,
    command_sequence: u64,
}

impl<P: PluginHost> EmbeddedRuntime<P> {
    /// Construct an embedded runtime with one explicitly admitted binding.
    pub fn new(mut plugin: P, binding: ExecutionBinding) -> RuntimeResult<Self> {
        binding.verify()?;
        plugin.admit_execution_binding(&binding)?;
        Ok(Self {
            machine: Machine::new(),
            plugin,
            binding,
            command_sequence: 0,
        })
    }

    /// Access the underlying machine for queries and conformance assertions.
    pub const fn machine(&self) -> &Machine {
        &self.machine
    }

    /// Seal a language-neutral candidate using the trusted Rust kernel.
    pub fn seal(&mut self, candidate: PlanCandidate) -> RuntimeResult<SealedPlan> {
        let plan = cymule_core::seal_plan(candidate)?;
        self.machine.insert_plan(plan.clone())?;
        Ok(plan)
    }

    /// Execute a sealed plan to a terminal Result in the Embedded profile.
    pub fn execute(
        &mut self,
        plan: SealedPlan,
        input: &Value,
        run_id: impl Into<String>,
    ) -> RuntimeResult<ExecutionOutcome> {
        let contracts = verify_plan(&plan)?;
        let definition = plan
            .candidate
            .definitions
            .iter()
            .find(|definition| definition.id == plan.candidate.entry)
            .ok_or_else(|| RuntimeError::plugin_defect("entry definition disappeared"))?
            .clone();
        contracts.validate_definition_input(&definition.id, input)?;
        self.binding.admit_plan(&plan)?;
        self.machine.insert_plan(plan.clone())?;
        let binding_bytes = self.binding.canonical_bytes()?;
        let binding_ref = self
            .machine
            .put_artifact(crate::EXECUTION_BINDING_VERSION, binding_bytes)?;
        if binding_ref != self.binding.artifact_ref()? {
            return Err(RuntimeError::plugin_defect(
                "execution binding Artifact identity is inconsistent",
            ));
        }
        let run_id = run_id.into();
        let binding_context = binding_ref.artifact_id.clone();

        self.submit(
            &run_id,
            Command::StartRun {
                plan_id: plan.plan_id.clone(),
                binding_context,
            },
        )?;
        self.submit(
            &run_id,
            Command::BeginAttempt {
                attempt_id: "attempt:root/1".to_owned(),
                continuation_id: "continuation:root".to_owned(),
                occurrence_binding: binding_ref.artifact_id,
                epoch: 0,
            },
        )?;

        let mut environment = BTreeMap::new();
        let outcome = self.execute_region(
            &run_id,
            &plan,
            &contracts,
            &definition.body,
            input,
            ROOT_SCOPE_ID,
            &definition.id,
            &[],
            &definition.id,
            &[],
            Some(&definition.id),
            &mut environment,
        )?;
        let (value, pending) = match outcome {
            RegionOutcome::Completed { value, pending } => (value, pending),
            RegionOutcome::Suspended(mut suspension) => {
                suspension.run_id.clone_from(&run_id);
                suspension.plan_id.clone_from(&plan.plan_id);
                self.submit(
                    &run_id,
                    Command::YieldAttempt {
                        attempt_id: "attempt:root/1".to_owned(),
                        epoch: 0,
                    },
                )?;
                self.machine.verify_replay()?;
                return Ok(ExecutionOutcome::Suspended { suspension });
            }
            RegionOutcome::ReconciliationRequired { intent_id } => {
                self.yield_root_attempt(&run_id)?;
                self.machine.verify_replay()?;
                return Ok(ExecutionOutcome::ReconciliationRequired {
                    reconciliation: EffectReconciliationBoundary {
                        run_id,
                        plan_id: plan.plan_id,
                        intent_id,
                    },
                });
            }
        };

        self.submit(
            &run_id,
            Command::CommitScope {
                scope_id: ROOT_SCOPE_ID.to_owned(),
            },
        )?;
        let pending = match self.dispatch_pending(&run_id, &contracts, pending, &mut environment)? {
            PendingDispatchOutcome::Settled(pending) => pending,
            PendingDispatchOutcome::ReconciliationRequired { intent_id } => {
                self.yield_root_attempt(&run_id)?;
                self.machine.verify_replay()?;
                return Ok(ExecutionOutcome::ReconciliationRequired {
                    reconciliation: EffectReconciliationBoundary {
                        run_id,
                        plan_id: plan.plan_id,
                        intent_id,
                    },
                });
            }
        };
        let mut explicit: Vec<String> =
            pending.into_iter().map(|effect| effect.intent_id).collect();
        if !explicit.is_empty() {
            explicit.sort();
            explicit.dedup();
            self.yield_root_attempt(&run_id)?;
            self.machine.verify_replay()?;
            return Ok(ExecutionOutcome::ReleaseRequired {
                release: EffectReleaseBoundary {
                    run_id,
                    plan_id: plan.plan_id,
                    intent_ids: explicit,
                },
            });
        }
        self.yield_root_attempt(&run_id)?;

        let result_bytes = canonical_bytes(&value)?;
        let result_ref = self.machine.put_artifact("cymule.result/1", result_bytes)?;
        self.submit(
            &run_id,
            Command::CompleteRun {
                result: Some(result_ref),
            },
        )?;
        self.machine.verify_replay()?;
        let run = self
            .machine
            .projection()
            .runs
            .get(&run_id)
            .ok_or_else(|| RuntimeError::plugin_defect("Run projection is missing"))?;
        Ok(ExecutionOutcome::Completed {
            result: ExecutionResult {
                run_id,
                plan_id: plan.plan_id,
                value,
                projection_digest: self.machine.projection().digest()?,
                precondition_token: run.precondition_token(),
                effects: run.effects.keys().cloned().collect(),
            },
        })
    }

    #[allow(clippy::too_many_arguments)]
    fn execute_region(
        &mut self,
        run_id: &str,
        plan: &SealedPlan,
        contracts: &PlanContracts,
        region: &Region,
        input: &Value,
        scope_id: &str,
        invocation_id: &str,
        invocation_path: &[InvocationPathSegment],
        definition_id: &str,
        region_path: &[usize],
        result_definition: Option<&str>,
        environment: &mut BTreeMap<String, Value>,
    ) -> RuntimeResult<RegionOutcome> {
        let mut pending = Vec::new();
        for (step_index, step) in region.steps.iter().enumerate() {
            match &step.operation {
                Operation::Call {
                    component,
                    input: expression,
                    bind,
                } => {
                    let value = evaluate(expression, input, environment)?;
                    contracts.validate_component_input(component, &value)?;
                    let response = self.plugin.invoke(PluginRequest::Call {
                        component: component.clone(),
                        input: value,
                    })?;
                    let value = match response {
                        PluginResponse::CallResult { value } => value,
                        PluginResponse::ExpectedFailure { error } => {
                            return Err(expected_failure(error)?);
                        }
                        PluginResponse::Defect { code, message } => {
                            return Err(plugin_reported_defect(code, message)?);
                        }
                        _ => {
                            return Err(RuntimeError::plugin_defect(format!(
                                "component {component} returned an invalid response variant"
                            )));
                        }
                    };
                    contracts.validate_component_output(component, &value)?;
                    if let Some(binding) = bind {
                        environment.insert(binding.clone(), value);
                    }
                }
                Operation::Invoke {
                    definition,
                    input: expression,
                    bind,
                } => {
                    let value = evaluate(expression, input, environment)?;
                    let target = plan
                        .candidate
                        .definitions
                        .iter()
                        .find(|candidate| candidate.id == *definition)
                        .expect("plan validation guarantees invoked definition");
                    contracts.validate_definition_input(definition, &value)?;
                    let mut child_invocation_path = invocation_path.to_vec();
                    child_invocation_path.push(InvocationPathSegment {
                        site_id: step.id.clone(),
                        region_path: region_path.to_vec(),
                        scope_id: scope_id.to_owned(),
                        epoch: self.current_epoch(run_id)?,
                    });
                    let child_invocation = plan_invocation_id(
                        run_id,
                        &plan.plan_id,
                        &plan.candidate.entry,
                        &child_invocation_path,
                        self.current_epoch(run_id)?,
                    )?;
                    let mut child_environment = BTreeMap::new();
                    let child = self.execute_region(
                        run_id,
                        plan,
                        contracts,
                        &target.body,
                        &value,
                        scope_id,
                        &child_invocation,
                        &child_invocation_path,
                        definition,
                        &[],
                        Some(definition),
                        &mut child_environment,
                    )?;
                    let RegionOutcome::Completed {
                        value: child_value,
                        pending: child_pending,
                    } = child
                    else {
                        return Ok(child);
                    };
                    pending.extend(child_pending);
                    if let Some(binding) = bind {
                        environment.insert(binding.clone(), child_value);
                    }
                }
                Operation::Wait { wait, bind } => {
                    return Ok(RegionOutcome::Suspended(SuspensionBoundary {
                        run_id: String::new(),
                        plan_id: String::new(),
                        definition_id: definition_id.to_owned(),
                        invocation_id: invocation_id.to_owned(),
                        site_id: step.id.clone(),
                        wait: wait.clone(),
                        result_bind: bind.clone(),
                    }));
                }
                Operation::Effect {
                    effect,
                    input: expression,
                    occurrence,
                    bind,
                } => {
                    let value = evaluate(expression, input, environment)?;
                    contracts.validate_effect_input(effect, &value)?;
                    let args = self
                        .machine
                        .put_artifact("cymule.effect-args/1", canonical_bytes(&value)?)?;
                    let occurrence_binding = self
                        .binding
                        .occurrence_binding(ExecutionOperationKind::Effect, effect)?;
                    let epoch = self.current_epoch(run_id)?;
                    let intent_id = effect_intent_id(
                        run_id,
                        invocation_id,
                        &step.id,
                        scope_id,
                        epoch,
                        occurrence,
                        &args,
                        "cymule.effect-schema/1",
                    )?;
                    self.submit(
                        run_id,
                        Command::ProposeEffect {
                            scope_id: scope_id.to_owned(),
                            invocation_id: invocation_id.to_owned(),
                            invocation_path: invocation_path.to_vec(),
                            definition_id: definition_id.to_owned(),
                            region_path: region_path.to_vec(),
                            site_id: step.id.clone(),
                            occurrence: occurrence.clone(),
                            operation: effect.clone(),
                            args,
                            occurrence_binding,
                        },
                    )?;
                    match self.plugin.invoke(PluginRequest::PrepareEffect {
                        operation: effect.clone(),
                        intent_id: intent_id.clone(),
                        input: value.clone(),
                    })? {
                        PluginResponse::Prepared => {}
                        PluginResponse::ExpectedFailure { error } => {
                            return Err(expected_failure(error)?);
                        }
                        PluginResponse::Defect { code, message } => {
                            return Err(plugin_reported_defect(code, message)?);
                        }
                        _ => {
                            return Err(RuntimeError::plugin_defect(format!(
                                "effect {effect} prepare returned an invalid response variant"
                            )));
                        }
                    }
                    self.submit(
                        run_id,
                        Command::TransitionEffect {
                            intent_id: intent_id.clone(),
                            transition: EffectTransition::Prepare,
                        },
                    )?;

                    let contract = plan
                        .candidate
                        .effects
                        .iter()
                        .find(|contract| contract.id == *effect)
                        .expect("plan validation guarantees effect contract");
                    if contract.profile.mutation == MutationKind::Observational
                        && contract.profile.dispatch == DispatchPolicy::Eager
                    {
                        match self.dispatch_effect(run_id, contracts, effect, intent_id, value)? {
                            EffectDispatchOutcome::Settled(result) => {
                                if let (Some(binding), Some(result)) = (bind, result) {
                                    environment.insert(binding.clone(), result);
                                }
                            }
                            EffectDispatchOutcome::ReconciliationRequired { intent_id } => {
                                return Ok(RegionOutcome::ReconciliationRequired { intent_id });
                            }
                        }
                    } else {
                        if bind.is_some() {
                            return Err(RuntimeError::plugin_defect(format!(
                                "deferred mutating effect {effect} cannot bind a value inside its open scope"
                            )));
                        }
                        pending.push(PendingEffect {
                            intent_id,
                            operation: effect.clone(),
                            input: value,
                            bind: bind.clone(),
                            dispatch: contract.profile.dispatch,
                        });
                    }
                }
                Operation::Scope { body, bind, .. } => {
                    let mut child_region_path = region_path.to_vec();
                    child_region_path.push(step_index);
                    let child_scope = plan_scope_id(
                        run_id,
                        &plan.plan_id,
                        invocation_id,
                        definition_id,
                        &child_region_path,
                        self.current_epoch(run_id)?,
                    )?;
                    self.submit(
                        run_id,
                        Command::OpenScope {
                            scope_id: child_scope.clone(),
                            parent_scope: scope_id.to_owned(),
                            invocation_id: invocation_id.to_owned(),
                            invocation_path: invocation_path.to_vec(),
                            definition_id: definition_id.to_owned(),
                            region_path: region_path.to_vec(),
                            site_id: step.id.clone(),
                        },
                    )?;
                    let mut child_environment = environment.clone();
                    let child = self.execute_region(
                        run_id,
                        plan,
                        contracts,
                        body,
                        input,
                        &child_scope,
                        invocation_id,
                        invocation_path,
                        definition_id,
                        &child_region_path,
                        None,
                        &mut child_environment,
                    )?;
                    let RegionOutcome::Completed {
                        value: child_value,
                        pending: child_pending,
                    } = child
                    else {
                        return Ok(child);
                    };
                    self.submit(
                        run_id,
                        Command::CommitScope {
                            scope_id: child_scope,
                        },
                    )?;
                    match self.dispatch_pending(
                        run_id,
                        contracts,
                        child_pending,
                        &mut child_environment,
                    )? {
                        PendingDispatchOutcome::Settled(child_pending) => {
                            pending.extend(child_pending);
                        }
                        PendingDispatchOutcome::ReconciliationRequired { intent_id } => {
                            return Ok(RegionOutcome::ReconciliationRequired { intent_id });
                        }
                    }
                    if let Some(binding) = bind {
                        environment.insert(binding.clone(), child_value);
                    }
                }
            }
        }
        let value = evaluate(&region.result, input, environment)?;
        if let Some(definition) = result_definition {
            contracts.validate_definition_output(definition, &value)?;
        }
        Ok(RegionOutcome::Completed { value, pending })
    }

    fn dispatch_pending(
        &mut self,
        run_id: &str,
        contracts: &PlanContracts,
        pending: Vec<PendingEffect>,
        environment: &mut BTreeMap<String, Value>,
    ) -> RuntimeResult<PendingDispatchOutcome> {
        let mut explicit = Vec::new();
        for effect in pending {
            if effect.dispatch == DispatchPolicy::Explicit {
                explicit.push(effect);
                continue;
            }
            let result = self.dispatch_effect(
                run_id,
                contracts,
                &effect.operation,
                effect.intent_id,
                effect.input,
            )?;
            match result {
                EffectDispatchOutcome::Settled(result) => {
                    if let (Some(binding), Some(result)) = (effect.bind, result) {
                        environment.insert(binding, result);
                    }
                }
                EffectDispatchOutcome::ReconciliationRequired { intent_id } => {
                    return Ok(PendingDispatchOutcome::ReconciliationRequired { intent_id });
                }
            }
        }
        Ok(PendingDispatchOutcome::Settled(explicit))
    }

    fn dispatch_effect(
        &mut self,
        run_id: &str,
        contracts: &PlanContracts,
        operation: &str,
        intent_id: String,
        input: Value,
    ) -> RuntimeResult<EffectDispatchOutcome> {
        contracts.validate_effect_input(operation, &input)?;
        self.submit(
            run_id,
            Command::TransitionEffect {
                intent_id: intent_id.clone(),
                transition: EffectTransition::AuthorizeRelease,
            },
        )?;
        self.submit(
            run_id,
            Command::TransitionEffect {
                intent_id: intent_id.clone(),
                transition: EffectTransition::StartDispatch,
            },
        )?;
        let Ok(response) = self.plugin.invoke(PluginRequest::DispatchEffect {
            operation: operation.to_owned(),
            intent_id: intent_id.clone(),
            input: input.clone(),
        }) else {
            self.observe_unknown(run_id, &intent_id)?;
            return Ok(EffectDispatchOutcome::ReconciliationRequired { intent_id });
        };
        let PluginResponse::EffectResult { outcome, mut value } = response else {
            self.observe_unknown(run_id, &intent_id)?;
            return Ok(EffectDispatchOutcome::ReconciliationRequired { intent_id });
        };
        if validate_optional_effect_output(contracts, operation, outcome, value.as_ref()).is_err() {
            self.observe_unknown(run_id, &intent_id)?;
            return Ok(EffectDispatchOutcome::ReconciliationRequired { intent_id });
        }
        self.submit(
            run_id,
            Command::TransitionEffect {
                intent_id: intent_id.clone(),
                transition: EffectTransition::Observe(outcome),
            },
        )?;
        if outcome == WorldOutcome::Unknown {
            let reconciliation_mode = self
                .machine
                .projection()
                .runs
                .get(run_id)
                .and_then(|run| run.effects.get(&intent_id))
                .map(|effect| effect.profile.reconciliation)
                .ok_or_else(|| RuntimeError::plugin_defect("effect projection is missing"))?;
            if reconciliation_mode != ReconciliationMode::Queryable {
                return Ok(EffectDispatchOutcome::ReconciliationRequired { intent_id });
            }
            let Ok(response) = self.plugin.invoke(PluginRequest::ReconcileEffect {
                operation: operation.to_owned(),
                intent_id: intent_id.clone(),
                input,
            }) else {
                return Ok(EffectDispatchOutcome::ReconciliationRequired { intent_id });
            };
            let PluginResponse::ReconciliationResult {
                resolution,
                value: reconciled_value,
            } = response
            else {
                return Ok(EffectDispatchOutcome::ReconciliationRequired { intent_id });
            };
            if validate_optional_reconciliation_output(
                contracts,
                operation,
                resolution,
                reconciled_value.as_ref(),
            )
            .is_err()
            {
                return Ok(EffectDispatchOutcome::ReconciliationRequired { intent_id });
            }
            self.submit(
                run_id,
                Command::TransitionEffect {
                    intent_id,
                    transition: EffectTransition::Reconcile(resolution),
                },
            )?;
            if matches!(
                resolution,
                ReconciliationResolution::ResolvedApplied
                    | ReconciliationResolution::ResolvedNotApplied
            ) {
                value = reconciled_value.or(value);
            }
        }
        Ok(EffectDispatchOutcome::Settled(value))
    }

    fn observe_unknown(&mut self, run_id: &str, intent_id: &str) -> RuntimeResult<()> {
        self.submit(
            run_id,
            Command::TransitionEffect {
                intent_id: intent_id.to_owned(),
                transition: EffectTransition::Observe(WorldOutcome::Unknown),
            },
        )
    }

    fn submit(&mut self, run_id: &str, command: Command) -> RuntimeResult<()> {
        self.command_sequence += 1;
        let expected_precondition = if matches!(command, Command::StartRun { .. }) {
            None
        } else {
            Some(
                self.machine
                    .projection()
                    .runs
                    .get(run_id)
                    .ok_or_else(|| RuntimeError::plugin_defect(format!("Run {run_id} is missing")))?
                    .precondition_token(),
            )
        };
        let receipt = self.machine.submit(CommandEnvelope {
            command_version: COMMAND_VERSION.to_owned(),
            command_id: format!("{run_id}:command:{}", self.command_sequence),
            actor: "actor:embedded-runtime".to_owned(),
            run_id: run_id.to_owned(),
            expected_precondition,
            command,
        })?;
        if receipt.status != CommandReceiptStatus::Applied {
            return Err(RuntimeError::plugin_defect(format!(
                "runtime command unexpectedly conflicted: {receipt:?}"
            )));
        }
        Ok(())
    }

    fn current_epoch(&self, run_id: &str) -> RuntimeResult<u64> {
        self.machine
            .projection()
            .runs
            .get(run_id)
            .map(|run| run.epoch)
            .ok_or_else(|| RuntimeError::plugin_defect(format!("Run {run_id} is missing")))
    }

    fn yield_root_attempt(&mut self, run_id: &str) -> RuntimeResult<()> {
        self.submit(
            run_id,
            Command::YieldAttempt {
                attempt_id: "attempt:root/1".to_owned(),
                epoch: 0,
            },
        )
    }
}

fn expected_failure(error: PluginExpectedFailure) -> RuntimeResult<RuntimeError> {
    error.verify()?;
    Ok(RuntimeError::ExpectedPluginFailure(error))
}

fn plugin_reported_defect(code: String, message: String) -> RuntimeResult<RuntimeError> {
    PluginExpectedFailure {
        code: code.clone(),
        message: message.clone(),
    }
    .verify()?;
    Ok(RuntimeError::PluginDefect { code, message })
}

fn validate_optional_effect_output(
    contracts: &PlanContracts,
    operation: &str,
    outcome: WorldOutcome,
    value: Option<&Value>,
) -> RuntimeResult<()> {
    if let Some(value) = value {
        return contracts
            .validate_effect_output(operation, value)
            .map_err(Into::into);
    }
    if outcome == WorldOutcome::Applied {
        contracts.validate_effect_output(operation, &Value::Null)?;
    }
    Ok(())
}

fn validate_optional_reconciliation_output(
    contracts: &PlanContracts,
    operation: &str,
    resolution: ReconciliationResolution,
    value: Option<&Value>,
) -> RuntimeResult<()> {
    if let Some(value) = value {
        return contracts
            .validate_effect_output(operation, value)
            .map_err(Into::into);
    }
    if resolution == ReconciliationResolution::ResolvedApplied {
        contracts.validate_effect_output(operation, &Value::Null)?;
    }
    Ok(())
}

fn evaluate(
    expression: &Expression,
    input: &Value,
    environment: &BTreeMap<String, Value>,
) -> RuntimeResult<Value> {
    match expression {
        Expression::Input => Ok(input.clone()),
        Expression::Literal { value } => Ok(value.clone()),
        Expression::Binding { name } => environment.get(name).cloned().ok_or_else(|| {
            RuntimeError::plugin_defect(format!("binding {name} is unavailable during execution"))
        }),
        Expression::Object { fields } => {
            let mut object = serde_json::Map::new();
            for (key, expression) in fields {
                object.insert(key.clone(), evaluate(expression, input, environment)?);
            }
            Ok(Value::Object(object))
        }
        Expression::Array { items } => items
            .iter()
            .map(|expression| evaluate(expression, input, environment))
            .collect::<RuntimeResult<Vec<_>>>()
            .map(Value::Array),
    }
}
