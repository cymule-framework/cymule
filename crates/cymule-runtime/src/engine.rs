use std::collections::BTreeMap;

use cymule_core::{
    COMMAND_VERSION, Command, CommandEnvelope, CommandReceiptStatus, DispatchPolicy,
    EffectTransition, Expression, Machine, MutationKind, Operation, PlanCandidate, ROOT_SCOPE_ID,
    ReconciliationResolution, Region, SealedPlan, WorldOutcome, canonical_bytes, effect_intent_id,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{
    PluginHost, PluginManifest, PluginRequest, PluginResponse, RuntimeError, RuntimeResult,
};

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
    command_sequence: u64,
    scope_sequence: u64,
    invocation_sequence: u64,
}

impl<P: PluginHost> EmbeddedRuntime<P> {
    /// Construct an embedded runtime with one plugin host.
    pub fn new(plugin: P) -> Self {
        Self {
            machine: Machine::new(),
            plugin,
            command_sequence: 0,
            scope_sequence: 0,
            invocation_sequence: 0,
        }
    }

    /// Access the underlying machine for queries and conformance assertions.
    pub const fn machine(&self) -> &Machine {
        &self.machine
    }

    /// Seal a language-neutral candidate using the trusted Rust kernel.
    pub fn seal(&mut self, candidate: PlanCandidate) -> RuntimeResult<SealedPlan> {
        self.machine.seal_plan(candidate).map_err(Into::into)
    }

    /// Execute a sealed plan to a terminal Result in the Embedded profile.
    pub fn execute(
        &mut self,
        plan: SealedPlan,
        input: &Value,
        run_id: impl Into<String>,
    ) -> RuntimeResult<ExecutionResult> {
        plan.verify()?;
        self.machine.insert_plan(plan.clone())?;
        let manifest = self.plugin.describe()?;
        validate_manifest(&plan, &manifest)?;
        let run_id = run_id.into();
        let binding_context = format!("binding:plugin/{}", manifest.implementation_id);

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
                occurrence_binding: format!("binding:{}/runtime", manifest.implementation_id),
                epoch: 0,
            },
        )?;

        let definition = plan
            .candidate
            .definitions
            .iter()
            .find(|definition| definition.id == plan.candidate.entry)
            .ok_or_else(|| RuntimeError::plugin_defect("entry definition disappeared"))?
            .clone();
        let mut environment = BTreeMap::new();
        let outcome = self.execute_region(
            &run_id,
            &plan,
            &manifest,
            &definition.body,
            input,
            ROOT_SCOPE_ID,
            &definition.id,
            &mut environment,
        )?;

        self.submit(
            &run_id,
            Command::CommitScope {
                scope_id: ROOT_SCOPE_ID.to_owned(),
            },
        )?;
        self.dispatch_pending(&run_id, &manifest, outcome.pending, &mut environment)?;
        self.submit(
            &run_id,
            Command::YieldAttempt {
                attempt_id: "attempt:root/1".to_owned(),
                epoch: 0,
            },
        )?;

        let result_bytes = canonical_bytes(&outcome.value)?;
        let result_ref = self.machine.put_artifact("cymule.result/1", result_bytes);
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
        manifest: &PluginManifest,
        region: &Region,
        input: &Value,
        scope_id: &str,
        invocation_id: &str,
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
                    let response = self.plugin.invoke(PluginRequest::Call {
                        component: component.clone(),
                        input: value,
                    })?;
                    let PluginResponse::CallResult { value } = response else {
                        return Err(RuntimeError::plugin_defect(format!(
                            "component {component} returned unexpected response {response:?}"
                        )));
                    };
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
                    self.invocation_sequence += 1;
                    let child_invocation =
                        format!("{invocation_id}/{}:{}", step.id, self.invocation_sequence);
                    let mut child_environment = BTreeMap::new();
                    let child = self.execute_region(
                        run_id,
                        plan,
                        manifest,
                        &target.body,
                        &value,
                        scope_id,
                        &child_invocation,
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
                    let args = self
                        .machine
                        .put_artifact("cymule.effect-args/1", canonical_bytes(&value)?);
                    let occurrence_binding = format!(
                        "binding:{}/effect/{}/{}",
                        manifest.implementation_id,
                        effect,
                        manifest
                            .effects
                            .get(effect)
                            .expect("manifest was validated")
                            .implementation_revision
                    );
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
                        response => {
                            return Err(RuntimeError::plugin_defect(format!(
                                "effect {effect} prepare returned {response:?}"
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
                        let result = self.dispatch_effect(run_id, effect, intent_id, value)?;
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
                        manifest,
                        body,
                        input,
                        &child_scope,
                        invocation_id,
                        &mut child_environment,
                    )?;
                    self.submit(
                        run_id,
                        Command::CommitScope {
                            scope_id: child_scope,
                        },
                    )?;
                    self.dispatch_pending(run_id, manifest, child.pending, &mut child_environment)?;
                    if let Some(binding) = bind {
                        environment.insert(binding.clone(), child.value);
                    }
                }
            }
        }
        let value = evaluate(&region.result, input, environment)?;
        Ok(RegionOutcome { value, pending })
    }

    fn dispatch_pending(
        &mut self,
        run_id: &str,
        _manifest: &PluginManifest,
        pending: Vec<PendingEffect>,
        environment: &mut BTreeMap<String, Value>,
    ) -> RuntimeResult<()> {
        for effect in pending {
            let result =
                self.dispatch_effect(run_id, &effect.operation, effect.intent_id, effect.input)?;
            if let (Some(binding), Some(result)) = (effect.bind, result) {
                environment.insert(binding, result);
            }
        }
        Ok(())
    }

    fn dispatch_effect(
        &mut self,
        run_id: &str,
        operation: &str,
        intent_id: String,
        input: Value,
    ) -> RuntimeResult<Option<Value>> {
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
        let response = self.plugin.invoke(PluginRequest::DispatchEffect {
            operation: operation.to_owned(),
            intent_id: intent_id.clone(),
            input: input.clone(),
        })?;
        let PluginResponse::EffectResult { outcome, mut value } = response else {
            return Err(RuntimeError::plugin_defect(format!(
                "effect {operation} dispatch returned {response:?}"
            )));
        };
        self.submit(
            run_id,
            Command::TransitionEffect {
                intent_id: intent_id.clone(),
                transition: EffectTransition::Observe(outcome),
            },
        )?;
        if outcome == WorldOutcome::Unknown {
            let response = self.plugin.invoke(PluginRequest::ReconcileEffect {
                operation: operation.to_owned(),
                intent_id: intent_id.clone(),
                input,
            })?;
            let PluginResponse::ReconciliationResult {
                resolution,
                value: reconciled_value,
            } = response
            else {
                return Err(RuntimeError::plugin_defect(format!(
                    "effect {operation} reconciliation returned {response:?}"
                )));
            };
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

fn validate_manifest(plan: &SealedPlan, manifest: &PluginManifest) -> RuntimeResult<()> {
    for contract in &plan.candidate.components {
        if !manifest.components.contains_key(&contract.id) {
            return Err(RuntimeError::plugin_defect(format!(
                "plugin does not implement component {}",
                contract.id
            )));
        }
    }
    for contract in &plan.candidate.effects {
        if !manifest.effects.contains_key(&contract.id) {
            return Err(RuntimeError::plugin_defect(format!(
                "plugin does not implement effect {}",
                contract.id
            )));
        }
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
