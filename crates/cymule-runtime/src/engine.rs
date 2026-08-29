use std::collections::BTreeMap;

use cymule_core::{
    COMMAND_VERSION, Command, CommandEnvelope, CommandReceiptStatus,
    DECLARED_FAILURE_ARTIFACT_KIND, Definition, DispatchPolicy, EFFECT_ARGS_ARTIFACT_KIND,
    EFFECT_SCHEMA_VERSION, EffectIntentIdentityInput, EffectTransition, Expression,
    InitialAttemptSpec, InvocationPathSegment, Machine, MutationKind, Operation, PlanCandidate,
    ROOT_SCOPE_ID, RUN_INPUT_ARTIFACT_KIND, ReconciliationMode, ReconciliationResolution, Region,
    RunFailure, RunFailureClass, SealedPlan, WorldOutcome, canonical_bytes, content_id,
    effect_intent_id, plan_invocation_id, plan_scope_id, validate_identity,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{
    BoundPluginHost, EffectProviderAttempt, EffectReconciliationDecision, ExecutionBinding,
    ExecutionOperationKind, PlanAdmissionResult, PlanContracts, PluginExpectedFailure,
    PluginRequest, PluginResponse, RuntimeError, RuntimeResult,
};

/// Canonical Artifact kind for a terminal Flow result.
pub const RESULT_ARTIFACT_KIND: &str = "cymule.result/1";
const EMBEDDED_CONTINUATION_ID_DOMAIN: &str = "cymule.embedded-continuation/1";
const EMBEDDED_ATTEMPT_ID_DOMAIN: &str = "cymule.embedded-attempt/1";

#[derive(Serialize)]
struct EmbeddedContinuationIdentity<'a> {
    run_id: &'a str,
    role: &'static str,
}

#[derive(Serialize)]
struct EmbeddedAttemptIdentity<'a> {
    continuation_id: &'a str,
    continuation_epoch: u64,
    execution_fence: u64,
}

/// Verify canonical Plan identity and compile every executable contract.
///
/// # Errors
///
/// Returns a Plan-admission error when identity verification or any executable
/// schema compilation fails.
pub fn verify_plan(plan: &SealedPlan) -> PlanAdmissionResult<PlanContracts> {
    plan.verify()?;
    PlanContracts::compile(&plan.candidate).map_err(Into::into)
}

/// Validate one complete Embedded execution request without constructing or
/// invoking its selected plugin.
///
/// # Errors
///
/// Returns a typed runtime error when the Run identity, sealed Plan, entry
/// contract, or input value is invalid.
pub fn verify_execution_request(
    plan: &SealedPlan,
    input: &Value,
    run_id: &str,
) -> RuntimeResult<()> {
    admit_execution_request(plan, input, run_id).map(|_| ())
}

fn admit_execution_request(
    plan: &SealedPlan,
    input: &Value,
    run_id: &str,
) -> RuntimeResult<(PlanContracts, Definition)> {
    validate_identity("Run", run_id)?;
    let contracts = verify_plan(plan)?;
    let definition = plan
        .candidate
        .definitions
        .iter()
        .find(|definition| definition.id == plan.candidate.entry)
        .ok_or_else(|| RuntimeError::plugin_defect("entry definition disappeared"))?
        .clone();
    contracts.validate_definition_input(&definition.id, input)?;
    Ok((contracts, definition))
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
    ///
    /// # Errors
    ///
    /// Returns the typed suspension, release, or reconciliation boundary when
    /// this outcome is not terminal completion.
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
    #[serde(deserialize_with = "deserialize_required_nullable")]
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

#[derive(Clone, Copy)]
struct RegionExecution<'a> {
    run_id: &'a str,
    plan: &'a SealedPlan,
    contracts: &'a PlanContracts,
    input: &'a Value,
    scope_id: &'a str,
    invocation_id: &'a str,
    invocation_path: &'a [InvocationPathSegment],
    definition_id: &'a str,
    region_path: &'a [usize],
    result_definition: Option<&'a str>,
}

enum RegionStepOutcome {
    Continue(Vec<PendingEffect>),
    Return(RegionOutcome),
}

enum EffectDispatchOutcome {
    Settled(Option<Value>),
    ReconciliationRequired { intent_id: String },
}

#[derive(Clone, Copy)]
struct EffectDispatch<'a> {
    run_id: &'a str,
    contracts: &'a PlanContracts,
    operation: &'a str,
    intent_id: &'a str,
    input: &'a Value,
    provider_attempt: &'a EffectProviderAttempt,
}

enum PendingDispatchOutcome {
    Settled(Vec<PendingEffect>),
    ReconciliationRequired { intent_id: String },
}

/// Synchronous reference runtime over the trusted in-memory Machine.
pub struct EmbeddedRuntime<P: BoundPluginHost> {
    machine: Machine,
    plugin: P,
    binding: ExecutionBinding,
    command_sequence: u64,
}

impl<P: BoundPluginHost> EmbeddedRuntime<P> {
    /// Construct an embedded runtime with one explicitly admitted binding.
    ///
    /// # Errors
    ///
    /// Returns a runtime error when the binding is invalid or the bound host
    /// does not realize it exactly.
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
    ///
    /// # Errors
    ///
    /// Returns a runtime error when sealing or inserting the canonical Plan
    /// fails.
    pub fn seal(&mut self, candidate: PlanCandidate) -> RuntimeResult<SealedPlan> {
        let plan = cymule_core::seal_plan(candidate)?;
        self.machine.insert_plan(plan.clone())?;
        Ok(plan)
    }

    /// Execute a sealed plan to a terminal Result in the Embedded profile.
    ///
    /// # Errors
    ///
    /// Returns a runtime error when request admission, Machine transition,
    /// contract validation, plugin execution, or terminal settlement fails.
    pub fn execute(
        &mut self,
        plan: SealedPlan,
        input: &Value,
        run_id: impl Into<String>,
    ) -> RuntimeResult<ExecutionOutcome> {
        let run_id = run_id.into();
        let (contracts, definition) = self.prepare_execution(&plan, input, &run_id)?;
        let outcome = self.execute_root_region(&run_id, &plan, &contracts, &definition, input)?;
        self.finish_execution(&run_id, plan, &contracts, outcome)
    }

    fn prepare_execution(
        &mut self,
        plan: &SealedPlan,
        input: &Value,
        run_id: &str,
    ) -> RuntimeResult<(PlanContracts, Definition)> {
        let (contracts, definition) = admit_execution_request(plan, input, run_id)?;
        self.binding.admit_plan(plan)?;
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
        let binding_context = binding_ref.artifact_id.clone();
        let input_ref = self
            .machine
            .put_artifact(RUN_INPUT_ARTIFACT_KIND, canonical_bytes(input)?)?;

        let initial_attempt = InitialAttemptSpec {
            attempt_id: root_attempt_id(run_id)?,
            continuation_id: root_continuation_id(run_id)?,
            occurrence_binding: binding_ref.artifact_id.clone(),
            continuation_epoch: 0,
            execution_fence: 1,
        };
        self.submit_start_run(
            run_id,
            plan,
            &binding_ref,
            &input_ref,
            binding_context,
            initial_attempt,
        )?;
        Ok((contracts, definition))
    }

    fn execute_root_region(
        &mut self,
        run_id: &str,
        plan: &SealedPlan,
        contracts: &PlanContracts,
        definition: &Definition,
        input: &Value,
    ) -> RuntimeResult<RegionOutcome> {
        let root_invocation =
            plan_invocation_id(run_id, &plan.plan_id, &plan.candidate.entry, &[])?;
        let mut environment = BTreeMap::new();
        self.execute_region(
            &definition.body,
            RegionExecution {
                run_id,
                plan,
                contracts,
                input,
                scope_id: ROOT_SCOPE_ID,
                invocation_id: &root_invocation,
                invocation_path: &[],
                definition_id: &definition.id,
                region_path: &[],
                result_definition: Some(&definition.id),
            },
            &mut environment,
        )
    }

    fn finish_execution(
        &mut self,
        run_id: &str,
        plan: SealedPlan,
        contracts: &PlanContracts,
        outcome: RegionOutcome,
    ) -> RuntimeResult<ExecutionOutcome> {
        let plan_id = plan.plan_id;
        match outcome {
            RegionOutcome::Suspended(mut suspension) => {
                run_id.clone_into(&mut suspension.run_id);
                suspension.plan_id.clone_from(&plan_id);
                self.yield_root_attempt(run_id)?;
                self.machine.verify_replay()?;
                Ok(ExecutionOutcome::Suspended { suspension })
            }
            RegionOutcome::ReconciliationRequired { intent_id } => {
                self.reconciliation_outcome(run_id, &plan_id, intent_id)
            }
            RegionOutcome::Completed { value, pending } => {
                self.finish_completed_execution(run_id, &plan_id, contracts, value, pending)
            }
        }
    }

    fn finish_completed_execution(
        &mut self,
        run_id: &str,
        plan_id: &str,
        contracts: &PlanContracts,
        value: Value,
        pending: Vec<PendingEffect>,
    ) -> RuntimeResult<ExecutionOutcome> {
        self.submit(
            run_id,
            Command::CommitScope {
                scope_id: ROOT_SCOPE_ID.to_owned(),
            },
        )?;
        let pending = match self.dispatch_pending(run_id, contracts, pending)? {
            PendingDispatchOutcome::Settled(pending) => pending,
            PendingDispatchOutcome::ReconciliationRequired { intent_id } => {
                return self.reconciliation_outcome(run_id, plan_id, intent_id);
            }
        };
        let mut explicit: Vec<String> =
            pending.into_iter().map(|effect| effect.intent_id).collect();
        if !explicit.is_empty() {
            explicit.sort();
            explicit.dedup();
            self.yield_root_attempt(run_id)?;
            self.machine.verify_replay()?;
            return Ok(ExecutionOutcome::ReleaseRequired {
                release: EffectReleaseBoundary {
                    run_id: run_id.to_owned(),
                    plan_id: plan_id.to_owned(),
                    intent_ids: explicit,
                },
            });
        }
        self.yield_root_attempt(run_id)?;

        let result_bytes = canonical_bytes(&value)?;
        let result_ref = self
            .machine
            .put_artifact(RESULT_ARTIFACT_KIND, result_bytes)?;
        self.submit(
            run_id,
            Command::CompleteRun {
                result: Some(result_ref),
            },
        )?;
        self.machine.verify_replay()?;
        let run = self
            .machine
            .projection()
            .runs
            .get(run_id)
            .ok_or_else(|| RuntimeError::plugin_defect("Run projection is missing"))?;
        Ok(ExecutionOutcome::Completed {
            result: ExecutionResult {
                run_id: run_id.to_owned(),
                plan_id: plan_id.to_owned(),
                value,
                projection_digest: self.machine.projection().digest()?,
                precondition_token: run.precondition_token(),
                effects: run.effects.keys().cloned().collect(),
            },
        })
    }

    fn reconciliation_outcome(
        &mut self,
        run_id: &str,
        plan_id: &str,
        intent_id: String,
    ) -> RuntimeResult<ExecutionOutcome> {
        self.yield_root_attempt(run_id)?;
        self.machine.verify_replay()?;
        Ok(ExecutionOutcome::ReconciliationRequired {
            reconciliation: EffectReconciliationBoundary {
                run_id: run_id.to_owned(),
                plan_id: plan_id.to_owned(),
                intent_id,
            },
        })
    }

    fn execute_region(
        &mut self,
        region: &Region,
        context: RegionExecution<'_>,
        environment: &mut BTreeMap<String, Value>,
    ) -> RuntimeResult<RegionOutcome> {
        let mut pending = Vec::new();
        for (step_index, step) in region.steps.iter().enumerate() {
            match &step.operation {
                Operation::Call { .. } => {
                    self.execute_call_step(context, step, environment)?;
                }
                Operation::Invoke { .. } => {
                    match self.execute_invoke_step(context, step, environment)? {
                        RegionStepOutcome::Continue(child_pending) => {
                            pending.extend(child_pending);
                        }
                        RegionStepOutcome::Return(outcome) => return Ok(outcome),
                    }
                }
                Operation::Wait { wait, bind } => {
                    return Ok(RegionOutcome::Suspended(SuspensionBoundary {
                        run_id: String::new(),
                        plan_id: String::new(),
                        definition_id: context.definition_id.to_owned(),
                        invocation_id: context.invocation_id.to_owned(),
                        site_id: step.id.clone(),
                        wait: wait.clone(),
                        result_bind: bind.clone(),
                    }));
                }
                Operation::Effect { .. } => {
                    match self.execute_effect_step(context, step, environment)? {
                        RegionStepOutcome::Continue(step_pending) => {
                            pending.extend(step_pending);
                        }
                        RegionStepOutcome::Return(outcome) => return Ok(outcome),
                    }
                }
                Operation::Scope { .. } => {
                    match self.execute_scope_step(context, step_index, step, environment)? {
                        RegionStepOutcome::Continue(child_pending) => {
                            pending.extend(child_pending);
                        }
                        RegionStepOutcome::Return(outcome) => return Ok(outcome),
                    }
                }
            }
        }
        let value = evaluate(&region.result, context.input, environment)?;
        if let Some(definition) = context.result_definition {
            context
                .contracts
                .validate_definition_output(definition, &value)?;
        }
        Ok(RegionOutcome::Completed { value, pending })
    }

    fn execute_call_step(
        &mut self,
        context: RegionExecution<'_>,
        step: &cymule_core::Step,
        environment: &mut BTreeMap<String, Value>,
    ) -> RuntimeResult<()> {
        let Operation::Call {
            component,
            input: expression,
            bind,
        } = &step.operation
        else {
            return Err(RuntimeError::plugin_defect(
                "component handler received a different operation",
            ));
        };
        let value = evaluate(expression, context.input, environment)?;
        context
            .contracts
            .validate_component_input(component, &value)?;
        let response = self.plugin.invoke_bound(
            &self.binding,
            &self.binding,
            PluginRequest::Call {
                component: component.clone(),
                input: value,
            },
        )?;
        let value = match response {
            PluginResponse::CallResult { value } => value,
            PluginResponse::ExpectedFailure { error } => {
                return Err(self.expected_failure(context.run_id, error)?);
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
        context
            .contracts
            .validate_component_output(component, &value)?;
        if let Some(binding) = bind {
            environment.insert(binding.clone(), value);
        }
        Ok(())
    }

    fn execute_invoke_step(
        &mut self,
        context: RegionExecution<'_>,
        step: &cymule_core::Step,
        environment: &mut BTreeMap<String, Value>,
    ) -> RuntimeResult<RegionStepOutcome> {
        let Operation::Invoke {
            definition,
            input: expression,
            bind,
        } = &step.operation
        else {
            return Err(RuntimeError::plugin_defect(
                "definition handler received a different operation",
            ));
        };
        let value = evaluate(expression, context.input, environment)?;
        let target = context
            .plan
            .candidate
            .definitions
            .iter()
            .find(|candidate| candidate.id == *definition)
            .ok_or_else(|| {
                RuntimeError::plugin_defect("invoked definition is missing after Plan admission")
            })?;
        context
            .contracts
            .validate_definition_input(definition, &value)?;
        let mut child_invocation_path = context.invocation_path.to_vec();
        child_invocation_path.push(InvocationPathSegment {
            site_id: step.id.clone(),
            region_path: context.region_path.to_vec(),
            scope_id: context.scope_id.to_owned(),
        });
        let child_invocation = plan_invocation_id(
            context.run_id,
            &context.plan.plan_id,
            &context.plan.candidate.entry,
            &child_invocation_path,
        )?;
        let mut child_environment = BTreeMap::new();
        let child = self.execute_region(
            &target.body,
            RegionExecution {
                input: &value,
                invocation_id: &child_invocation,
                invocation_path: &child_invocation_path,
                definition_id: definition,
                region_path: &[],
                result_definition: Some(definition),
                ..context
            },
            &mut child_environment,
        )?;
        let RegionOutcome::Completed {
            value: child_value,
            pending: child_pending,
        } = child
        else {
            return Ok(RegionStepOutcome::Return(child));
        };
        if let Some(binding) = bind {
            environment.insert(binding.clone(), child_value);
        }
        Ok(RegionStepOutcome::Continue(child_pending))
    }

    fn execute_effect_step(
        &mut self,
        context: RegionExecution<'_>,
        step: &cymule_core::Step,
        environment: &mut BTreeMap<String, Value>,
    ) -> RuntimeResult<RegionStepOutcome> {
        let Operation::Effect { effect, bind, .. } = &step.operation else {
            return Err(RuntimeError::plugin_defect(
                "effect handler received a different operation",
            ));
        };
        let (pending, eager_observational) = self.prepare_effect(context, step, environment)?;
        if !eager_observational {
            return Ok(RegionStepOutcome::Continue(vec![pending]));
        }
        let PendingEffect {
            intent_id, input, ..
        } = pending;
        match self.dispatch_effect(context.run_id, context.contracts, effect, intent_id, &input)? {
            EffectDispatchOutcome::Settled(result) => {
                if let (Some(binding), Some(result)) = (bind, result) {
                    environment.insert(binding.clone(), result);
                }
                Ok(RegionStepOutcome::Continue(Vec::new()))
            }
            EffectDispatchOutcome::ReconciliationRequired { intent_id } => Ok(
                RegionStepOutcome::Return(RegionOutcome::ReconciliationRequired { intent_id }),
            ),
        }
    }

    fn prepare_effect(
        &mut self,
        context: RegionExecution<'_>,
        step: &cymule_core::Step,
        environment: &BTreeMap<String, Value>,
    ) -> RuntimeResult<(PendingEffect, bool)> {
        let Operation::Effect {
            effect,
            input: expression,
            occurrence,
            ..
        } = &step.operation
        else {
            return Err(RuntimeError::plugin_defect(
                "effect preparation received a different operation",
            ));
        };
        let value = evaluate(expression, context.input, environment)?;
        context.contracts.validate_effect_input(effect, &value)?;
        let args = self
            .machine
            .put_artifact(EFFECT_ARGS_ARTIFACT_KIND, canonical_bytes(&value)?)?;
        let occurrence_binding = self
            .binding
            .occurrence_binding(ExecutionOperationKind::Effect, effect)?;
        let intent_id = effect_intent_id(&EffectIntentIdentityInput {
            run_id: context.run_id,
            plan_id: &context.plan.plan_id,
            invocation_id: context.invocation_id,
            site_id: &step.id,
            scope_id: context.scope_id,
            occurrence,
            args: &args,
            effect_schema_version: EFFECT_SCHEMA_VERSION,
        })?;
        self.submit(
            context.run_id,
            Command::ProposeEffect {
                scope_id: context.scope_id.to_owned(),
                invocation_id: context.invocation_id.to_owned(),
                invocation_path: context.invocation_path.to_vec(),
                definition_id: context.definition_id.to_owned(),
                region_path: context.region_path.to_vec(),
                site_id: step.id.clone(),
                occurrence: occurrence.clone(),
                operation: effect.clone(),
                args,
                execution_binding: self.binding.artifact_ref()?,
                occurrence_binding,
            },
        )?;
        self.invoke_effect_prepare(effect, &intent_id, &value)?;
        self.submit(
            context.run_id,
            Command::TransitionEffect {
                intent_id: intent_id.clone(),
                transition: EffectTransition::Prepare,
            },
        )?;
        let contract = context
            .plan
            .candidate
            .effects
            .iter()
            .find(|contract| contract.id == *effect)
            .ok_or_else(|| {
                RuntimeError::plugin_defect("effect contract is missing after Plan admission")
            })?;
        Ok((
            PendingEffect {
                intent_id,
                operation: effect.clone(),
                input: value,
                dispatch: contract.profile.dispatch,
            },
            contract.profile.mutation == MutationKind::Observational
                && contract.profile.dispatch == DispatchPolicy::Eager,
        ))
    }

    fn invoke_effect_prepare(
        &mut self,
        effect: &str,
        intent_id: &str,
        value: &Value,
    ) -> RuntimeResult<()> {
        match self.plugin.invoke_bound(
            &self.binding,
            &self.binding,
            PluginRequest::PrepareEffect {
                operation: effect.to_owned(),
                intent_id: intent_id.to_owned(),
                input: value.clone(),
            },
        )? {
            PluginResponse::Prepared => Ok(()),
            PluginResponse::ExpectedFailure { error } => Err(RuntimeError::PluginDefect {
                code: "effect_prepare_response_variant_invalid".to_owned(),
                message: format!("effect {effect} prepare returned expected failure {error:?}"),
            }),
            PluginResponse::Defect { code, message } => Err(plugin_reported_defect(code, message)?),
            _ => Err(RuntimeError::plugin_defect(format!(
                "effect {effect} prepare returned an invalid response variant"
            ))),
        }
    }

    fn execute_scope_step(
        &mut self,
        context: RegionExecution<'_>,
        step_index: usize,
        step: &cymule_core::Step,
        environment: &mut BTreeMap<String, Value>,
    ) -> RuntimeResult<RegionStepOutcome> {
        let Operation::Scope { body, bind, .. } = &step.operation else {
            return Err(RuntimeError::plugin_defect(
                "scope handler received a different operation",
            ));
        };
        let mut child_region_path = context.region_path.to_vec();
        child_region_path.push(step_index);
        let child_scope = plan_scope_id(
            context.run_id,
            &context.plan.plan_id,
            context.invocation_id,
            context.definition_id,
            &child_region_path,
        )?;
        self.submit(
            context.run_id,
            Command::OpenScope {
                scope_id: child_scope.clone(),
                parent_scope: context.scope_id.to_owned(),
                invocation_id: context.invocation_id.to_owned(),
                invocation_path: context.invocation_path.to_vec(),
                definition_id: context.definition_id.to_owned(),
                region_path: context.region_path.to_vec(),
                site_id: step.id.clone(),
            },
        )?;
        let mut child_environment = environment.clone();
        let child = self.execute_region(
            body,
            RegionExecution {
                scope_id: &child_scope,
                region_path: &child_region_path,
                result_definition: None,
                ..context
            },
            &mut child_environment,
        )?;
        let RegionOutcome::Completed {
            value: child_value,
            pending: child_pending,
        } = child
        else {
            return Ok(RegionStepOutcome::Return(child));
        };
        self.submit(
            context.run_id,
            Command::CommitScope {
                scope_id: child_scope,
            },
        )?;
        let child_pending =
            match self.dispatch_pending(context.run_id, context.contracts, child_pending)? {
                PendingDispatchOutcome::Settled(child_pending) => child_pending,
                PendingDispatchOutcome::ReconciliationRequired { intent_id } => {
                    return Ok(RegionStepOutcome::Return(
                        RegionOutcome::ReconciliationRequired { intent_id },
                    ));
                }
            };
        if let Some(binding) = bind {
            environment.insert(binding.clone(), child_value);
        }
        Ok(RegionStepOutcome::Continue(child_pending))
    }

    fn dispatch_pending(
        &mut self,
        run_id: &str,
        contracts: &PlanContracts,
        pending: Vec<PendingEffect>,
    ) -> RuntimeResult<PendingDispatchOutcome> {
        let mut explicit = Vec::new();
        for effect in pending {
            if effect.dispatch == DispatchPolicy::Explicit {
                explicit.push(effect);
                continue;
            }
            let PendingEffect {
                intent_id,
                operation,
                input,
                ..
            } = effect;
            let result = self.dispatch_effect(run_id, contracts, &operation, intent_id, &input)?;
            match result {
                EffectDispatchOutcome::Settled(_) => {}
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
        input: &Value,
    ) -> RuntimeResult<EffectDispatchOutcome> {
        contracts.validate_effect_input(operation, input)?;
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
        let provider_attempt = EffectProviderAttempt::new(&intent_id, "runtime:embedded", 1)?;
        let dispatch = EffectDispatch {
            run_id,
            contracts,
            operation,
            intent_id: &intent_id,
            input,
            provider_attempt: &provider_attempt,
        };
        let Some((outcome, value)) = self.invoke_effect_dispatch(dispatch)? else {
            return Ok(EffectDispatchOutcome::ReconciliationRequired { intent_id });
        };
        self.submit(
            run_id,
            Command::TransitionEffect {
                intent_id: intent_id.clone(),
                transition: EffectTransition::Observe(outcome),
            },
        )?;
        if outcome == WorldOutcome::Unknown {
            return self.reconcile_unknown_effect(dispatch, value);
        }
        Ok(EffectDispatchOutcome::Settled(value))
    }

    fn invoke_effect_dispatch(
        &mut self,
        dispatch: EffectDispatch<'_>,
    ) -> RuntimeResult<Option<(WorldOutcome, Option<Value>)>> {
        let response = self.plugin.invoke_bound(
            &self.binding,
            &self.binding,
            PluginRequest::DispatchEffect {
                operation: dispatch.operation.to_owned(),
                intent_id: dispatch.intent_id.to_owned(),
                attempt: dispatch.provider_attempt.clone(),
                input: dispatch.input.clone(),
            },
        );
        let Ok(PluginResponse::EffectResult {
            attempt,
            outcome,
            value,
        }) = response
        else {
            self.observe_unknown(dispatch.run_id, dispatch.intent_id)?;
            return Ok(None);
        };
        if attempt != *dispatch.provider_attempt
            || validate_optional_effect_output(
                dispatch.contracts,
                dispatch.operation,
                outcome,
                value.as_ref(),
            )
            .is_err()
        {
            self.observe_unknown(dispatch.run_id, dispatch.intent_id)?;
            return Ok(None);
        }
        Ok(Some((outcome, value)))
    }

    fn reconcile_unknown_effect(
        &mut self,
        dispatch: EffectDispatch<'_>,
        observed_value: Option<Value>,
    ) -> RuntimeResult<EffectDispatchOutcome> {
        let reconciliation_mode = self
            .machine
            .projection()
            .runs
            .get(dispatch.run_id)
            .and_then(|run| run.effects.get(dispatch.intent_id))
            .map(|effect| effect.profile.reconciliation)
            .ok_or_else(|| RuntimeError::plugin_defect("effect projection is missing"))?;
        if reconciliation_mode != ReconciliationMode::Queryable {
            return Ok(EffectDispatchOutcome::ReconciliationRequired {
                intent_id: dispatch.intent_id.to_owned(),
            });
        }
        let response = match self.plugin.invoke_bound(
            &self.binding,
            &self.binding,
            PluginRequest::ReconcileEffect {
                operation: dispatch.operation.to_owned(),
                intent_id: dispatch.intent_id.to_owned(),
                attempt: dispatch.provider_attempt.clone(),
                decision: EffectReconciliationDecision::Observe,
                resolution_value: None,
                input: dispatch.input.clone(),
            },
        ) {
            Ok(response) => response,
            Err(RuntimeError::PluginDefect { code, message })
                if code == "invalid_reconciliation_resolution" =>
            {
                return Err(RuntimeError::PluginDefect { code, message });
            }
            Err(_) => {
                return Ok(EffectDispatchOutcome::ReconciliationRequired {
                    intent_id: dispatch.intent_id.to_owned(),
                });
            }
        };
        let PluginResponse::ReconciliationResult {
            attempt,
            resolution,
            value,
        } = response
        else {
            return Ok(EffectDispatchOutcome::ReconciliationRequired {
                intent_id: dispatch.intent_id.to_owned(),
            });
        };
        if attempt != *dispatch.provider_attempt
            || validate_optional_reconciliation_output(
                dispatch.contracts,
                dispatch.operation,
                resolution,
                value.as_ref(),
            )
            .is_err()
        {
            return Ok(EffectDispatchOutcome::ReconciliationRequired {
                intent_id: dispatch.intent_id.to_owned(),
            });
        }
        if resolution == ReconciliationResolution::GovernanceRequired {
            return Err(RuntimeError::PluginDefect {
                code: "invalid_reconciliation_resolution".to_owned(),
                message: "queryable reconciliation cannot delegate to governance".to_owned(),
            });
        }
        self.submit(
            dispatch.run_id,
            Command::TransitionEffect {
                intent_id: dispatch.intent_id.to_owned(),
                transition: EffectTransition::Reconcile(resolution),
            },
        )?;
        match resolution {
            ReconciliationResolution::ResolvedApplied => {
                Ok(EffectDispatchOutcome::Settled(value.or(observed_value)))
            }
            ReconciliationResolution::ResolvedNotApplied => {
                Ok(EffectDispatchOutcome::Settled(None))
            }
            ReconciliationResolution::StillUnknown => {
                Ok(EffectDispatchOutcome::ReconciliationRequired {
                    intent_id: dispatch.intent_id.to_owned(),
                })
            }
            ReconciliationResolution::GovernanceRequired => Err(RuntimeError::PluginDefect {
                code: "invalid_reconciliation_resolution".to_owned(),
                message: "queryable reconciliation cannot delegate to governance".to_owned(),
            }),
        }
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
        let command_id = self.next_command_id(run_id)?;
        self.submit_with_id(run_id, command_id, command)
    }

    fn submit_start_run(
        &mut self,
        run_id: &str,
        plan: &SealedPlan,
        binding: &cymule_core::ArtifactRef,
        input: &cymule_core::ArtifactRef,
        binding_context: String,
        initial_attempt: InitialAttemptSpec,
    ) -> RuntimeResult<()> {
        let command_id = self.next_command_id(run_id)?;
        let binding_record =
            self.machine.artifact(binding).cloned().ok_or_else(|| {
                RuntimeError::plugin_defect("execution binding Artifact is missing")
            })?;
        let input_record = self
            .machine
            .artifact(input)
            .cloned()
            .ok_or_else(|| RuntimeError::plugin_defect("Run input Artifact is missing"))?;
        let material = cymule_core::durable_internal::MachineStartRunMaterial::new(
            command_id.clone(),
            plan.clone(),
            binding_record,
            input_record,
        )?;
        self.submit_with_id(
            run_id,
            command_id,
            Command::StartRun {
                plan_id: plan.plan_id.clone(),
                binding_context,
                input: input.clone(),
                material_digest: material.material_digest().to_owned(),
                initial_attempt,
            },
        )
    }

    fn next_command_id(&mut self, run_id: &str) -> RuntimeResult<String> {
        self.command_sequence = next_embedded_command_sequence(self.command_sequence)?;
        Ok(content_id(
            COMMAND_VERSION,
            &("embedded-runtime", run_id, self.command_sequence),
        )?)
    }

    fn submit_with_id(
        &mut self,
        run_id: &str,
        command_id: String,
        command: Command,
    ) -> RuntimeResult<()> {
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
            command_id,
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

    fn expected_failure(
        &mut self,
        run_id: &str,
        error: PluginExpectedFailure,
    ) -> RuntimeResult<RuntimeError> {
        error.verify()?;
        let detail = self
            .machine
            .put_artifact(DECLARED_FAILURE_ARTIFACT_KIND, canonical_bytes(&error)?)?;
        self.submit(
            run_id,
            Command::FailRun {
                failure: RunFailure {
                    class: RunFailureClass::DeclaredFailure,
                    code: error.code.clone(),
                    detail,
                },
            },
        )?;
        Ok(RuntimeError::ExpectedPluginFailure(error))
    }
    fn yield_root_attempt(&mut self, run_id: &str) -> RuntimeResult<()> {
        self.submit(
            run_id,
            Command::YieldAttempt {
                attempt_id: root_attempt_id(run_id)?,
                continuation_epoch: 0,
                execution_fence: 1,
            },
        )
    }
}

fn next_embedded_command_sequence(current: u64) -> RuntimeResult<u64> {
    current
        .checked_add(1)
        .filter(|sequence| *sequence <= cymule_core::MAX_EXACT_INTEGER)
        .ok_or_else(|| RuntimeError::PluginDefect {
            code: "embedded_command_sequence_exhausted".to_owned(),
            message: "embedded runtime command sequence exhausted the exact integer range"
                .to_owned(),
        })
}

fn root_continuation_id(run_id: &str) -> RuntimeResult<String> {
    content_id(
        EMBEDDED_CONTINUATION_ID_DOMAIN,
        &EmbeddedContinuationIdentity {
            run_id,
            role: "root",
        },
    )
    .map_err(RuntimeError::from)
}

fn root_attempt_id(run_id: &str) -> RuntimeResult<String> {
    let continuation_id = root_continuation_id(run_id)?;
    content_id(
        EMBEDDED_ATTEMPT_ID_DOMAIN,
        &EmbeddedAttemptIdentity {
            continuation_id: &continuation_id,
            continuation_epoch: 0,
            execution_fence: 1,
        },
    )
    .map_err(RuntimeError::from)
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

fn deserialize_required_nullable<'de, D, T>(deserializer: D) -> Result<Option<T>, D::Error>
where
    D: serde::Deserializer<'de>,
    T: Deserialize<'de>,
{
    Option::<T>::deserialize(deserializer)
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

#[cfg(test)]
mod required_nullable_tests {
    use super::*;

    #[test]
    fn suspension_result_bind_is_required_nullable_on_wire() {
        let boundary = SuspensionBoundary {
            run_id: "run:required-nullable".to_owned(),
            plan_id: "plan:required-nullable".to_owned(),
            definition_id: "definition:required-nullable".to_owned(),
            invocation_id: "invocation:required-nullable".to_owned(),
            site_id: "site:required-nullable".to_owned(),
            wait: cymule_core::WaitSpec::Signal {
                key: "signal:required-nullable".to_owned(),
                consume_once: true,
            },
            result_bind: None,
        };
        let Value::Object(fields) = serde_json::to_value(&boundary).expect("boundary serializes")
        else {
            panic!("SuspensionBoundary must serialize as an object");
        };
        assert_eq!(fields.get("result_bind"), Some(&Value::Null));
        serde_json::from_value::<SuspensionBoundary>(Value::Object(fields.clone()))
            .expect("explicit null decodes");
        let mut missing = fields;
        missing.remove("result_bind");
        assert!(
            serde_json::from_value::<SuspensionBoundary>(Value::Object(missing)).is_err(),
            "missing required-nullable result_bind must fail closed"
        );
    }

    #[test]
    fn embedded_command_sequence_has_an_exact_terminal_bound() {
        assert_eq!(
            next_embedded_command_sequence(cymule_core::MAX_EXACT_INTEGER - 1)
                .expect("the exact terminal command sequence is admitted"),
            cymule_core::MAX_EXACT_INTEGER
        );
        assert!(matches!(
            next_embedded_command_sequence(cymule_core::MAX_EXACT_INTEGER),
            Err(RuntimeError::PluginDefect { code, .. })
                if code == "embedded_command_sequence_exhausted"
        ));
        assert!(next_embedded_command_sequence(u64::MAX).is_err());
    }
}
