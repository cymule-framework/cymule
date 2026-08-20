use std::collections::BTreeMap;

use cymule_core::{
    COMMAND_VERSION, Command, CommandEnvelope, CommandReceiptStatus, DispatchPolicy,
    EffectTransition, Expression, Machine, MutationKind, Operation, PlanCandidate, ROOT_SCOPE_ID,
    ReconciliationResolution, Region, SealedPlan, WorldOutcome, canonical_bytes, effect_intent_id,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{
    ExecutionBinding, ExecutionOperationKind, PlanAdmissionResult, PlanContracts,
    PluginExpectedFailure, PluginHost, PluginRequest, PluginResponse, RuntimeError, RuntimeResult,
};

/// Validate every semantic and executable contract, then seal the unchanged
/// Plan Candidate under its canonical identity.
pub fn seal_plan(candidate: PlanCandidate) -> PlanAdmissionResult<SealedPlan> {
    candidate.validate()?;
    PlanContracts::compile(&candidate)?;
    candidate.seal().map_err(Into::into)
}

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

#[derive(Debug)]
struct PendingEffect {
    intent_id: String,
    operation: String,
    input: Value,
    bind: Option<String>,
}

#[derive(Debug)]
struct RegionOutcome {
    value: Value,
    pending: Vec<PendingEffect>,
}

/// Synchronous reference runtime over the trusted in-memory Machine.
pub struct EmbeddedRuntime<P: PluginHost> {
    machine: Machine,
    plugin: P,
    binding: ExecutionBinding,
    command_sequence: u64,
    scope_sequence: u64,
    invocation_sequence: u64,
}

impl<P: PluginHost> EmbeddedRuntime<P> {
    /// Construct an embedded runtime with one explicitly admitted binding.
    pub fn new(mut plugin: P, binding: ExecutionBinding) -> RuntimeResult<Self> {
        binding.verify()?;
        let manifest = plugin.describe()?;
        binding.verify_manifest(&manifest)?;
        Ok(Self {
            machine: Machine::new(),
            plugin,
            binding,
            command_sequence: 0,
            scope_sequence: 0,
            invocation_sequence: 0,
        })
    }

    /// Access the underlying machine for queries and conformance assertions.
    pub const fn machine(&self) -> &Machine {
        &self.machine
    }

    /// Seal a language-neutral candidate using the trusted Rust kernel.
    pub fn seal(&mut self, candidate: PlanCandidate) -> RuntimeResult<SealedPlan> {
        let plan = seal_plan(candidate)?;
        self.machine.insert_plan(plan.clone())?;
        Ok(plan)
    }

    /// Execute a sealed plan to a terminal Result in the Embedded profile.
    pub fn execute(
        &mut self,
        plan: SealedPlan,
        input: &Value,
        run_id: impl Into<String>,
    ) -> RuntimeResult<ExecutionResult> {
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
            Some(&definition.id),
            &mut environment,
        )?;

        self.submit(
            &run_id,
            Command::CommitScope {
                scope_id: ROOT_SCOPE_ID.to_owned(),
            },
        )?;
        self.dispatch_pending(&run_id, &contracts, outcome.pending, &mut environment)?;
        self.submit(
            &run_id,
            Command::YieldAttempt {
                attempt_id: "attempt:root/1".to_owned(),
                epoch: 0,
            },
        )?;

        let result_bytes = canonical_bytes(&outcome.value)?;
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
        Ok(ExecutionResult {
            run_id,
            plan_id: plan.plan_id,
            value: outcome.value,
            projection_digest: self.machine.projection().digest()?,
            precondition_token: run.precondition_token(),
            effects: run.effects.keys().cloned().collect(),
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
        result_definition: Option<&str>,
        environment: &mut BTreeMap<String, Value>,
    ) -> RuntimeResult<RegionOutcome> {
        let mut pending = Vec::new();
        for step in &region.steps {
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
                    self.invocation_sequence += 1;
                    let child_invocation =
                        format!("{invocation_id}/{}:{}", step.id, self.invocation_sequence);
                    let mut child_environment = BTreeMap::new();
                    let child = self.execute_region(
                        run_id,
                        plan,
                        contracts,
                        &target.body,
                        &value,
                        scope_id,
                        &child_invocation,
                        Some(definition),
                        &mut child_environment,
                    )?;
                    pending.extend(child.pending);
                    if let Some(binding) = bind {
                        environment.insert(binding.clone(), child.value);
                    }
                }
                Operation::Wait { wait } => {
                    return Err(RuntimeError::Suspended(format!("waiting for {wait:?}")));
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
                        let result =
                            self.dispatch_effect(run_id, contracts, effect, intent_id, value)?;
                        if let (Some(binding), Some(result)) = (bind, result) {
                            environment.insert(binding.clone(), result);
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
                        });
                    }
                }
                Operation::Scope { body, bind, .. } => {
                    self.scope_sequence += 1;
                    let child_scope = format!("scope:{}:{}", step.id, self.scope_sequence);
                    self.submit(
                        run_id,
                        Command::OpenScope {
                            scope_id: child_scope.clone(),
                            parent_scope: scope_id.to_owned(),
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
                        None,
                        &mut child_environment,
                    )?;
                    self.submit(
                        run_id,
                        Command::CommitScope {
                            scope_id: child_scope,
                        },
                    )?;
                    self.dispatch_pending(
                        run_id,
                        contracts,
                        child.pending,
                        &mut child_environment,
                    )?;
                    if let Some(binding) = bind {
                        environment.insert(binding.clone(), child.value);
                    }
                }
            }
        }
        let value = evaluate(&region.result, input, environment)?;
        if let Some(definition) = result_definition {
            contracts.validate_definition_output(definition, &value)?;
        }
        Ok(RegionOutcome { value, pending })
    }

    fn dispatch_pending(
        &mut self,
        run_id: &str,
        contracts: &PlanContracts,
        pending: Vec<PendingEffect>,
        environment: &mut BTreeMap<String, Value>,
    ) -> RuntimeResult<()> {
        for effect in pending {
            let result = self.dispatch_effect(
                run_id,
                contracts,
                &effect.operation,
                effect.intent_id,
                effect.input,
            )?;
            if let (Some(binding), Some(result)) = (effect.bind, result) {
                environment.insert(binding, result);
            }
        }
        Ok(())
    }

    fn dispatch_effect(
        &mut self,
        run_id: &str,
        contracts: &PlanContracts,
        operation: &str,
        intent_id: String,
        input: Value,
    ) -> RuntimeResult<Option<Value>> {
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
            return Err(RuntimeError::unknown_world(
                "effect_dispatch_response_lost",
                "effect dispatch started but no authoritative outcome was received",
            ));
        };
        let PluginResponse::EffectResult { outcome, mut value } = response else {
            self.observe_unknown(run_id, &intent_id)?;
            return Err(RuntimeError::unknown_world(
                "effect_dispatch_response_invalid",
                "effect dispatch started but returned no authoritative world outcome",
            ));
        };
        self.submit(
            run_id,
            Command::TransitionEffect {
                intent_id: intent_id.clone(),
                transition: EffectTransition::Observe(outcome),
            },
        )?;
        validate_optional_effect_output(contracts, operation, outcome, value.as_ref())?;
        if outcome == WorldOutcome::Unknown {
            let response = self
                .plugin
                .invoke(PluginRequest::ReconcileEffect {
                    operation: operation.to_owned(),
                    intent_id: intent_id.clone(),
                    input,
                })
                .map_err(|_| {
                    RuntimeError::unknown_world(
                        "effect_reconciliation_response_lost",
                        "effect outcome remains unknown because reconciliation returned no authoritative result",
                    )
                })?;
            let PluginResponse::ReconciliationResult {
                resolution,
                value: reconciled_value,
            } = response
            else {
                return Err(RuntimeError::unknown_world(
                    "effect_reconciliation_response_invalid",
                    "effect outcome remains unknown because reconciliation returned an invalid response",
                ));
            };
            validate_optional_reconciliation_output(
                contracts,
                operation,
                resolution,
                reconciled_value.as_ref(),
            )?;
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
        Ok(value)
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
