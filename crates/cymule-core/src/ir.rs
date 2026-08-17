use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{CoreError, Result, content_id};

/// Frozen canonical IR version.
pub const IR_VERSION: &str = "cymule.ir/2";

/// An unsealed, language-neutral semantic plan.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PlanCandidate {
    /// IR version.
    pub ir_version: String,
    /// Human-readable plan name.
    pub name: String,
    /// Entry definition ID.
    pub entry: String,
    /// Abstract component contracts.
    #[serde(default)]
    pub components: Vec<ComponentContract>,
    /// Abstract effect contracts.
    #[serde(default)]
    pub effects: Vec<EffectContract>,
    /// Structured definitions.
    pub definitions: Vec<Definition>,
    /// Non-semantic author metadata. Keys and values are still content-addressed.
    #[serde(default)]
    pub metadata: BTreeMap<String, String>,
}

/// An immutable sealed plan.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SealedPlan {
    /// Content-addressed plan identity.
    pub plan_id: String,
    /// Validated canonical candidate.
    pub candidate: PlanCandidate,
}

/// Abstract executable component contract.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ComponentContract {
    /// Stable operation ID.
    pub id: String,
    /// Input JSON Schema.
    pub input_schema: Value,
    /// Output JSON Schema.
    pub output_schema: Value,
    /// Provider-neutral required properties.
    #[serde(default)]
    pub requirements: BTreeMap<String, String>,
}

/// Abstract world-effect contract.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EffectContract {
    /// Stable operation ID.
    pub id: String,
    /// Input JSON Schema.
    pub input_schema: Value,
    /// Output JSON Schema.
    pub output_schema: Value,
    /// Effect safety and recovery properties.
    pub profile: EffectProfile,
    /// Provider-neutral required properties.
    #[serde(default)]
    pub requirements: BTreeMap<String, String>,
}

/// Orthogonal effect properties used by admission and binding.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EffectProfile {
    /// Whether the operation can change the external world.
    pub mutation: MutationKind,
    /// When dispatch may be released.
    pub dispatch: DispatchPolicy,
    /// How an ambiguous result can be reconciled.
    pub reconciliation: ReconciliationMode,
    /// Whether the provider accepts a stable idempotency key.
    #[serde(default)]
    pub keyed_idempotency: bool,
    /// Whether the operation is irreversible after application.
    #[serde(default)]
    pub irreversible: bool,
}

/// Effect mutation class.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MutationKind {
    /// Read-only or observational nondeterminism.
    Observational,
    /// External mutation.
    Mutating,
}

/// Effect release policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DispatchPolicy {
    /// The observation may execute immediately.
    Eager,
    /// Release only after scope commit.
    OnScopeCommit,
    /// Release requires a separate explicit command.
    Explicit,
}

/// Reconciliation capability.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReconciliationMode {
    /// Adapter can query or otherwise prove the outcome.
    Queryable,
    /// An external attestation can prove the outcome.
    ExternallyAttested,
    /// Human governance is required.
    Human,
    /// No authoritative reconciliation is available.
    Impossible,
}

/// A named Flow definition.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Definition {
    /// Stable logical definition ID.
    pub id: String,
    /// Input JSON Schema.
    pub input_schema: Value,
    /// Output JSON Schema.
    pub output_schema: Value,
    /// Structured body.
    pub body: Region,
}

/// A structured sequence with an explicit result expression.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Region {
    /// Ordered semantic operations.
    #[serde(default)]
    pub steps: Vec<Step>,
    /// Region result.
    pub result: Expression,
}

/// A stable operation site.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Step {
    /// Stable source-assigned site ID.
    pub id: String,
    /// Operation at the site.
    #[serde(flatten)]
    pub operation: Operation,
}

/// Frozen structured operations. Complex frontend syntax lowers to these five
/// semantic boundaries.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum Operation {
    /// Call an abstract component.
    Call {
        /// Component contract ID.
        component: String,
        /// Typed input expression.
        input: Expression,
        /// Optional result binding.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        bind: Option<String>,
    },
    /// Invoke another definition in the same immutable Plan.
    Invoke {
        /// Referenced definition ID.
        definition: String,
        /// Typed invocation input expression.
        input: Expression,
        /// Optional result binding.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        bind: Option<String>,
    },
    /// Durably suspend.
    Wait {
        /// Wait description.
        wait: WaitSpec,
    },
    /// Propose an abstract external effect.
    Effect {
        /// Effect contract ID.
        effect: String,
        /// Typed argument expression.
        input: Expression,
        /// Intentional occurrence key. Retries reuse it.
        occurrence: String,
        /// Optional result binding.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        bind: Option<String>,
    },
    /// Execute a nested transactional scope.
    Scope {
        /// Scope behavior profile.
        mode: ScopeMode,
        /// Nested body.
        body: Box<Region>,
        /// Optional result binding.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        bind: Option<String>,
    },
}

/// Scope behavior profile.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ScopeMode {
    /// Normal nested state/evidence transaction.
    Transactional,
    /// Speculative branch; mutating effects remain staged until commit.
    Speculative,
}

/// Durable wait descriptions.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum WaitSpec {
    /// Wait for an external signal key.
    Signal {
        /// Stable signal key.
        key: String,
        /// Whether at most one waiter may consume it.
        #[serde(default)]
        consume_once: bool,
    },
    /// Wait for a logical timer identifier.
    Timer {
        /// Stable timer ID. Wall-clock observation belongs in a receipt.
        timer_id: String,
    },
    /// Wait for typed external input.
    Input {
        /// Correlation key.
        correlation: String,
        /// Input schema.
        schema: Value,
    },
}

/// Pure expressions over input, prior bindings, and literals.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Expression {
    /// Definition input.
    Input,
    /// Literal canonical JSON.
    Literal {
        /// Literal value.
        value: Value,
    },
    /// A prior binding.
    Binding {
        /// Binding name.
        name: String,
    },
    /// An object constructed from expressions.
    Object {
        /// Deterministically ordered fields.
        fields: BTreeMap<String, Expression>,
    },
    /// An array constructed from expressions.
    Array {
        /// Array items.
        items: Vec<Expression>,
    },
}

impl PlanCandidate {
    /// Validate and content-address a Plan Candidate.
    pub fn seal(self) -> Result<SealedPlan> {
        self.validate()?;
        let plan_id = content_id("cymule.plan/1", &self)?;
        Ok(SealedPlan {
            plan_id,
            candidate: self,
        })
    }

    /// Validate semantic structure before canonical identity is computed.
    pub fn validate(&self) -> Result<()> {
        if self.ir_version != IR_VERSION {
            return Err(CoreError::Validation(format!(
                "unsupported IR version {:?}; expected {IR_VERSION}",
                self.ir_version
            )));
        }
        validate_id("plan name", &self.name)?;

        let component_ids = unique_contract_ids(
            "component",
            self.components.iter().map(|contract| contract.id.as_str()),
        )?;
        let effect_ids = unique_contract_ids(
            "effect",
            self.effects.iter().map(|contract| contract.id.as_str()),
        )?;
        for contract in &self.components {
            validate_schema("component input", &contract.input_schema)?;
            validate_schema("component output", &contract.output_schema)?;
        }
        for contract in &self.effects {
            validate_schema("effect input", &contract.input_schema)?;
            validate_schema("effect output", &contract.output_schema)?;
            if contract.profile.mutation == MutationKind::Mutating
                && contract.profile.dispatch == DispatchPolicy::Eager
            {
                return Err(CoreError::Validation(format!(
                    "mutating effect {} cannot use eager dispatch",
                    contract.id
                )));
            }
        }

        let definition_ids = unique_contract_ids(
            "definition",
            self.definitions
                .iter()
                .map(|definition| definition.id.as_str()),
        )?;
        if !definition_ids.contains(&self.entry) {
            return Err(CoreError::Validation(format!(
                "entry definition {:?} does not exist",
                self.entry
            )));
        }

        let mut sites = BTreeSet::new();
        for definition in &self.definitions {
            validate_schema("definition input", &definition.input_schema)?;
            validate_schema("definition output", &definition.output_schema)?;
            validate_region(
                &definition.body,
                &component_ids,
                &effect_ids,
                &definition_ids,
                &mut sites,
                &BTreeSet::new(),
            )?;
        }
        Ok(())
    }
}

impl SealedPlan {
    /// Verify that the embedded candidate still matches its Plan ID.
    pub fn verify(&self) -> Result<()> {
        self.candidate.validate()?;
        let expected = content_id("cymule.plan/1", &self.candidate)?;
        if expected != self.plan_id {
            return Err(CoreError::IdentityMismatch(format!(
                "plan ID {} does not match {expected}",
                self.plan_id
            )));
        }
        Ok(())
    }
}

fn unique_contract_ids<'a>(
    kind: &str,
    ids: impl Iterator<Item = &'a str>,
) -> Result<BTreeSet<String>> {
    let mut unique = BTreeSet::new();
    for id in ids {
        validate_id(kind, id)?;
        if !unique.insert(id.to_owned()) {
            return Err(CoreError::Validation(format!("duplicate {kind} ID {id:?}")));
        }
    }
    Ok(unique)
}

fn validate_region(
    region: &Region,
    components: &BTreeSet<String>,
    effects: &BTreeSet<String>,
    definitions: &BTreeSet<String>,
    sites: &mut BTreeSet<String>,
    inherited: &BTreeSet<String>,
) -> Result<()> {
    let mut bindings = inherited.clone();
    for step in &region.steps {
        validate_id("step", &step.id)?;
        if !sites.insert(step.id.clone()) {
            return Err(CoreError::Validation(format!(
                "duplicate stable step site {:?}",
                step.id
            )));
        }

        let result_binding: Option<&String> = match &step.operation {
            Operation::Call {
                component,
                input,
                bind,
            } => {
                if !components.contains(component) {
                    return Err(CoreError::Validation(format!(
                        "step {} references unknown component {component:?}",
                        step.id
                    )));
                }
                validate_expression(input, &bindings)?;
                bind.as_ref()
            }
            Operation::Invoke {
                definition,
                input,
                bind,
            } => {
                if !definitions.contains(definition) {
                    return Err(CoreError::Validation(format!(
                        "step {} references unknown definition {definition:?}",
                        step.id
                    )));
                }
                validate_expression(input, &bindings)?;
                bind.as_ref()
            }
            Operation::Wait { wait } => {
                validate_wait(wait)?;
                None
            }
            Operation::Effect {
                effect,
                input,
                occurrence,
                bind,
            } => {
                if !effects.contains(effect) {
                    return Err(CoreError::Validation(format!(
                        "step {} references unknown effect {effect:?}",
                        step.id
                    )));
                }
                validate_id("occurrence", occurrence)?;
                validate_expression(input, &bindings)?;
                bind.as_ref()
            }
            Operation::Scope { body, bind, .. } => {
                validate_region(body, components, effects, definitions, sites, &bindings)?;
                bind.as_ref()
            }
        };

        if let Some(binding) = result_binding {
            validate_id("binding", binding)?;
            if !bindings.insert(binding.clone()) {
                return Err(CoreError::Validation(format!(
                    "binding {binding:?} is assigned more than once"
                )));
            }
        }
    }
    validate_expression(&region.result, &bindings)
}

fn validate_wait(wait: &WaitSpec) -> Result<()> {
    match wait {
        WaitSpec::Signal { key, .. } => validate_id("signal key", key),
        WaitSpec::Timer { timer_id } => validate_id("timer", timer_id),
        WaitSpec::Input {
            correlation,
            schema,
        } => {
            validate_id("input correlation", correlation)?;
            validate_schema("wait input", schema)
        }
    }
}

fn validate_expression(expression: &Expression, bindings: &BTreeSet<String>) -> Result<()> {
    match expression {
        Expression::Input | Expression::Literal { .. } => Ok(()),
        Expression::Binding { name } => {
            if bindings.contains(name) {
                Ok(())
            } else {
                Err(CoreError::Validation(format!(
                    "expression references undefined binding {name:?}"
                )))
            }
        }
        Expression::Object { fields } => {
            for expression in fields.values() {
                validate_expression(expression, bindings)?;
            }
            Ok(())
        }
        Expression::Array { items } => {
            for expression in items {
                validate_expression(expression, bindings)?;
            }
            Ok(())
        }
    }
}

fn validate_schema(kind: &str, schema: &Value) -> Result<()> {
    if !schema.is_object() && !schema.is_boolean() {
        return Err(CoreError::Validation(format!(
            "{kind} schema must be a JSON object or boolean"
        )));
    }
    Ok(())
}

fn validate_id(kind: &str, id: &str) -> Result<()> {
    let valid = !id.is_empty()
        && id.len() <= 160
        && id
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || b"._-/:".contains(&byte));
    if valid {
        Ok(())
    } else {
        Err(CoreError::Validation(format!(
            "{kind} ID {id:?} must be 1..=160 ASCII identifier characters"
        )))
    }
}
