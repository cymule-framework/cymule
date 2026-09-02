use std::collections::{BTreeMap, BTreeSet};

use jsonschema::{Retrieve, Uri};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{CoreError, Result, canonical_bytes, content_id, decode_json, validate_artifact_kind};

const JSON_SCHEMA_DIALECT: &str = "https://json-schema.org/draft/2020-12/schema";

/// Frozen canonical IR version.
pub const IR_VERSION: &str = "cymule.ir/3";
const PLAN_ID_DOMAIN: &str = "cymule.plan/1";

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
    pub components: Vec<ComponentContract>,
    /// Abstract effect contracts.
    pub effects: Vec<EffectContract>,
    /// Structured definitions.
    pub definitions: Vec<Definition>,
    /// Execution-neutral, identity-bearing author metadata.
    ///
    /// Reducers do not interpret these values, but they remain part of the
    /// canonical Plan preimage and therefore change Plan identity.
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
    /// Exact Artifact kind used to retain every successful output.
    pub output_artifact_kind: String,
    /// Provider-neutral required properties.
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
    pub keyed_idempotency: bool,
    /// Whether the operation is irreversible after application.
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
#[serde(tag = "op", rename_all = "snake_case", deny_unknown_fields)]
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
        /// Optional local binding for the admitted wait result.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        bind: Option<String>,
    },
    /// Propose an abstract external effect.
    Effect {
        /// Effect contract ID.
        effect: String,
        /// Typed argument expression.
        input: Expression,
        /// Intentional occurrence key. Retries reuse it.
        occurrence: String,
        /// Optional result binding. Only eager observational effects may bind.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        bind: Option<String>,
    },
    /// Execute a nested auto-commit scope.
    Scope {
        /// Nested body.
        body: Box<Region>,
        /// Optional result binding.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        bind: Option<String>,
    },
}

/// Durable wait descriptions.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum WaitSpec {
    /// Wait for an external signal key.
    Signal {
        /// Stable signal key.
        key: String,
        /// Whether at most one waiter may consume it.
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
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
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
    /// Validate semantic structure before canonical identity is computed.
    ///
    /// # Errors
    ///
    /// Returns a validation or schema-compilation error when the candidate is
    /// not one closed, executable `cymule.ir/3` graph.
    pub fn validate(&self) -> Result<()> {
        if self.ir_version != IR_VERSION {
            return Err(CoreError::Validation(format!(
                "unsupported IR version {:?}; expected {IR_VERSION}",
                self.ir_version
            )));
        }
        validate_semantic_id("plan name", &self.name)?;

        let component_ids = unique_contract_ids(
            "component",
            self.components.iter().map(|contract| contract.id.as_str()),
        )?;
        unique_contract_ids(
            "effect",
            self.effects.iter().map(|contract| contract.id.as_str()),
        )?;
        let effect_profiles = self
            .effects
            .iter()
            .map(|contract| (contract.id.as_str(), &contract.profile))
            .collect::<BTreeMap<_, _>>();
        for contract in &self.components {
            validate_schema("component input", &contract.input_schema)?;
            validate_schema("component output", &contract.output_schema)?;
            validate_artifact_kind(&contract.output_artifact_kind)?;
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
                &effect_profiles,
                &definition_ids,
                &mut sites,
                &BTreeSet::new(),
            )?;
        }
        validate_invocation_graph(&self.definitions)?;
        Ok(())
    }
}

/// Validate every semantic and Draft 2020-12 contract, then seal one Plan.
///
/// # Errors
///
/// Returns an error when the candidate cannot be canonicalized, validated, or
/// assigned its content identity.
pub fn seal_plan(candidate: PlanCandidate) -> Result<SealedPlan> {
    let normalized = canonical_plan_candidate(&candidate)?;
    let candidate = if normalized == candidate {
        candidate
    } else {
        normalized
    };
    candidate.validate()?;
    let plan_id = content_id(PLAN_ID_DOMAIN, &candidate)?;
    Ok(SealedPlan { plan_id, candidate })
}

impl SealedPlan {
    /// Verify that the embedded candidate still matches its Plan ID.
    ///
    /// # Errors
    ///
    /// Returns an error when the candidate is not canonical and valid or its
    /// declared Plan ID is not its exact content identity.
    pub fn verify(&self) -> Result<()> {
        if canonical_plan_candidate(&self.candidate)? != self.candidate {
            return Err(CoreError::Validation(
                "sealed Plan candidate does not use canonical JSON number forms".to_owned(),
            ));
        }
        self.candidate.validate()?;
        let expected = content_id(PLAN_ID_DOMAIN, &self.candidate)?;
        if expected != self.plan_id {
            return Err(CoreError::IdentityMismatch(format!(
                "plan ID {} does not match {expected}",
                self.plan_id
            )));
        }
        Ok(())
    }
}

fn canonical_plan_candidate(candidate: &PlanCandidate) -> Result<PlanCandidate> {
    decode_json(&canonical_bytes(candidate)?)
}

fn unique_contract_ids<'a>(
    kind: &str,
    ids: impl Iterator<Item = &'a str>,
) -> Result<BTreeSet<String>> {
    let mut unique = BTreeSet::new();
    for id in ids {
        validate_semantic_id(kind, id)?;
        if !unique.insert(id.to_owned()) {
            return Err(CoreError::Validation(format!("duplicate {kind} ID {id:?}")));
        }
    }
    Ok(unique)
}

fn validate_region(
    region: &Region,
    components: &BTreeSet<String>,
    effects: &BTreeMap<&str, &EffectProfile>,
    definitions: &BTreeSet<String>,
    sites: &mut BTreeSet<String>,
    inherited: &BTreeSet<String>,
) -> Result<()> {
    let mut bindings = inherited.clone();
    for step in &region.steps {
        validate_semantic_id("step", &step.id)?;
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
            Operation::Wait { wait, bind } => {
                validate_wait(wait)?;
                bind.as_ref()
            }
            Operation::Effect {
                effect,
                input,
                occurrence,
                bind,
            } => {
                let Some(profile) = effects.get(effect.as_str()) else {
                    return Err(CoreError::Validation(format!(
                        "step {} references unknown effect {effect:?}",
                        step.id
                    )));
                };
                if bind.is_some()
                    && (profile.mutation != MutationKind::Observational
                        || profile.dispatch != DispatchPolicy::Eager)
                {
                    return Err(CoreError::Validation(format!(
                        "step {} binds effect {effect:?}, but effect results may bind only for observational eager dispatch",
                        step.id
                    )));
                }
                validate_semantic_id("occurrence", occurrence)?;
                validate_expression(input, &bindings)?;
                bind.as_ref()
            }
            Operation::Scope { body, bind, .. } => {
                validate_region(body, components, effects, definitions, sites, &bindings)?;
                bind.as_ref()
            }
        };

        if let Some(binding) = result_binding {
            validate_semantic_id("binding", binding)?;
            if !bindings.insert(binding.clone()) {
                return Err(CoreError::Validation(format!(
                    "binding {binding:?} is assigned more than once"
                )));
            }
        }
    }
    validate_expression(&region.result, &bindings)
}

fn validate_invocation_graph(definitions: &[Definition]) -> Result<()> {
    let graph = definitions
        .iter()
        .map(|definition| {
            let mut targets = BTreeSet::new();
            collect_invocations(&definition.body, &mut targets);
            (definition.id.as_str(), targets)
        })
        .collect::<BTreeMap<_, _>>();
    validate_acyclic_graph(&graph)
}

fn collect_invocations<'a>(region: &'a Region, targets: &mut BTreeSet<&'a str>) {
    for step in &region.steps {
        match &step.operation {
            Operation::Invoke { definition, .. } => {
                targets.insert(definition);
            }
            Operation::Scope { body, .. } => collect_invocations(body, targets),
            Operation::Call { .. } | Operation::Wait { .. } | Operation::Effect { .. } => {}
        }
    }
}

fn validate_acyclic_graph<'a>(graph: &BTreeMap<&'a str, BTreeSet<&'a str>>) -> Result<()> {
    enum Visit<'a> {
        Enter(&'a str),
        Exit(&'a str),
    }

    let mut visiting = BTreeSet::new();
    let mut complete = BTreeSet::new();
    let mut stack = Vec::new();
    for definition in graph.keys().copied() {
        if complete.contains(definition) {
            continue;
        }
        stack.push(Visit::Enter(definition));
        while let Some(visit) = stack.pop() {
            match visit {
                Visit::Enter(definition) => {
                    if complete.contains(definition) {
                        continue;
                    }
                    if !visiting.insert(definition) {
                        return Err(CoreError::Validation(format!(
                            "recursive definition invocation reaches {definition:?}"
                        )));
                    }
                    stack.push(Visit::Exit(definition));
                    for target in graph[definition].iter().rev() {
                        if !complete.contains(target) {
                            stack.push(Visit::Enter(target));
                        }
                    }
                }
                Visit::Exit(definition) => {
                    visiting.remove(definition);
                    complete.insert(definition);
                }
            }
        }
    }
    Ok(())
}

fn validate_wait(wait: &WaitSpec) -> Result<()> {
    match wait {
        WaitSpec::Signal { key, .. } => validate_semantic_id("signal key", key),
        WaitSpec::Timer { timer_id } => validate_semantic_id("timer", timer_id),
        WaitSpec::Input {
            correlation,
            schema,
        } => {
            validate_semantic_id("input correlation", correlation)?;
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
    if let Some(declared) = schema.get("$schema")
        && declared.as_str() != Some(JSON_SCHEMA_DIALECT)
    {
        return Err(CoreError::Validation(format!(
            "{kind} schema dialect must be exactly {JSON_SCHEMA_DIALECT:?}"
        )));
    }
    jsonschema::draft202012::options()
        .with_retriever(DenyExternalReferences)
        .build(schema)
        .map_err(|error| CoreError::Validation(format!("{kind} schema is invalid: {error}")))?;
    Ok(())
}

pub(crate) fn validate_schema_instance(kind: &str, schema: &Value, instance: &Value) -> Result<()> {
    jsonschema::draft202012::options()
        .with_retriever(DenyExternalReferences)
        .build(schema)
        .map_err(|error| CoreError::Validation(format!("{kind} schema is invalid: {error}")))?
        .validate(instance)
        .map_err(|error| {
            CoreError::Validation(format!("{kind} does not match its Plan schema: {error}"))
        })
}

#[derive(Debug)]
struct DenyExternalReferences;

impl Retrieve for DenyExternalReferences {
    fn retrieve(
        &self,
        uri: &Uri<String>,
    ) -> std::result::Result<Value, Box<dyn std::error::Error + Send + Sync>> {
        Err(format!("external schema reference {uri} is forbidden").into())
    }
}

/// Validate one stable Plan/runtime semantic identifier.
///
/// # Errors
///
/// Returns a validation error unless the identifier contains 1..=160 ASCII
/// alphanumeric or `._-/:` characters.
pub fn validate_semantic_id(kind: &str, id: &str) -> Result<()> {
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

#[cfg(test)]
mod tests {
    use super::*;

    fn invocation_definition(index: usize, target: Option<usize>) -> Definition {
        Definition {
            id: format!("definition.{index}"),
            input_schema: Value::Bool(true),
            output_schema: Value::Bool(true),
            body: Region {
                steps: target
                    .map(|target| Step {
                        id: format!("invoke.{index}"),
                        operation: Operation::Invoke {
                            definition: format!("definition.{target}"),
                            input: Expression::Input,
                            bind: None,
                        },
                    })
                    .into_iter()
                    .collect(),
                result: Expression::Input,
            },
        }
    }

    #[test]
    fn invocation_graph_handles_a_very_deep_chain_and_still_detects_its_cycle() {
        const DEFINITION_COUNT: usize = 20_000;

        let mut definitions = (0..DEFINITION_COUNT)
            .map(|index| {
                invocation_definition(index, (index + 1 < DEFINITION_COUNT).then_some(index + 1))
            })
            .collect::<Vec<_>>();
        validate_invocation_graph(&definitions).expect("deep acyclic chain is valid");

        definitions[DEFINITION_COUNT - 1] = invocation_definition(DEFINITION_COUNT - 1, Some(0));
        assert!(matches!(
            validate_invocation_graph(&definitions),
            Err(CoreError::Validation(message))
                if message.contains("recursive definition invocation")
        ));
    }
}
