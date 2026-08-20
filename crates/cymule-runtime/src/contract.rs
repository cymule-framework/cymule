use std::collections::BTreeMap;
use std::fmt::{Display, Formatter};

use cymule_core::{Operation, PlanCandidate, Region, WaitSpec};
use jsonschema::{Retrieve, Uri, Validator};
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// JSON Schema dialect used by every executable Plan contract.
pub const CONTRACT_SCHEMA_DIALECT: &str = "https://json-schema.org/draft/2020-12/schema";

/// Result type for executable contract compilation and validation.
pub type ContractResult<T> = std::result::Result<T, ContractViolation>;

/// Result type for canonical and executable Plan admission.
pub type PlanAdmissionResult<T> = std::result::Result<T, PlanAdmissionError>;

/// Exact failure domain of Plan sealing and verification.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PlanAdmissionError {
    /// The trusted semantic kernel rejected structure or canonical identity.
    Core(cymule_core::CoreError),
    /// An executable schema contract was invalid.
    Contract(ContractViolation),
}

impl Display for PlanAdmissionError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Core(error) => Display::fmt(error, formatter),
            Self::Contract(error) => Display::fmt(error, formatter),
        }
    }
}

impl std::error::Error for PlanAdmissionError {}

impl From<cymule_core::CoreError> for PlanAdmissionError {
    fn from(error: cymule_core::CoreError) -> Self {
        Self::Core(error)
    }
}

impl From<ContractViolation> for PlanAdmissionError {
    fn from(error: ContractViolation) -> Self {
        Self::Contract(error)
    }
}

/// Whether a contract failed while being admitted or while checking a value.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ContractPhase {
    /// The submitted JSON Schema could not become an executable contract.
    Admission,
    /// A value did not satisfy an already admitted contract.
    Execution,
}

/// Semantic boundary that owns a JSON Schema.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ContractBoundary {
    /// A named Flow definition.
    Definition,
    /// An abstract component operation.
    Component,
    /// An abstract external effect.
    Effect,
    /// A typed durable input wait site.
    Wait,
}

/// Which side of a semantic boundary is being checked.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ContractSide {
    /// Value entering the boundary.
    Input,
    /// Value returned by the boundary.
    Output,
}

/// Exact semantic contract selected for compilation or validation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ContractTarget {
    /// Boundary class.
    pub boundary: ContractBoundary,
    /// Definition, operation, or stable wait-site identity.
    pub id: String,
    /// Input or output side.
    pub side: ContractSide,
}

impl ContractTarget {
    fn definition(id: &str, side: ContractSide) -> Self {
        Self {
            boundary: ContractBoundary::Definition,
            id: id.to_owned(),
            side,
        }
    }

    fn component(id: &str, side: ContractSide) -> Self {
        Self {
            boundary: ContractBoundary::Component,
            id: id.to_owned(),
            side,
        }
    }

    fn effect(id: &str, side: ContractSide) -> Self {
        Self {
            boundary: ContractBoundary::Effect,
            id: id.to_owned(),
            side,
        }
    }

    /// Select the input contract for one stable typed-wait site.
    pub fn wait(site_id: impl Into<String>) -> Self {
        Self {
            boundary: ContractBoundary::Wait,
            id: site_id.into(),
            side: ContractSide::Input,
        }
    }
}

/// One path-addressed JSON Schema issue without retaining the checked value.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ContractIssue {
    /// JSON Pointer into the submitted schema during admission or checked value
    /// during execution.
    pub instance_path: String,
    /// JSON Pointer to the failing schema keyword.
    pub schema_path: String,
    /// Human-readable issue summary with instance content masked.
    pub message: String,
}

/// Structured failure at one exact semantic contract boundary.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ContractViolation {
    /// Admission or execution phase.
    pub phase: ContractPhase,
    /// Exact contract that failed.
    pub target: ContractTarget,
    /// All validation issues reported for the value.
    pub issues: Vec<ContractIssue>,
}

impl Display for ContractViolation {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "contract {:?} {:?} {:?} failed during {:?}",
            self.target.boundary, self.target.id, self.target.side, self.phase
        )?;
        for issue in &self.issues {
            write!(
                formatter,
                "; instance {} schema {}: {}",
                issue.instance_path, issue.schema_path, issue.message
            )?;
        }
        Ok(())
    }
}

impl std::error::Error for ContractViolation {}

#[derive(Debug)]
struct DenyExternalReferences;

impl Retrieve for DenyExternalReferences {
    fn retrieve(
        &self,
        uri: &Uri<String>,
    ) -> Result<Value, Box<dyn std::error::Error + Send + Sync>> {
        Err(format!("external schema reference {uri} is forbidden").into())
    }
}

/// One compiled, deterministic Draft 2020-12 contract.
pub struct ContractValidator {
    target: ContractTarget,
    validator: Validator,
}

impl ContractValidator {
    /// Compile a schema without permitting filesystem, network, or ambient
    /// registry resolution.
    pub fn compile(target: ContractTarget, schema: &Value) -> ContractResult<Self> {
        if let Some(declared) = schema.get("$schema")
            && declared.as_str() != Some(CONTRACT_SCHEMA_DIALECT)
        {
            return Err(admission_issue(
                target,
                "/$schema",
                format!(
                    "schema dialect must be exactly {CONTRACT_SCHEMA_DIALECT:?}, received {declared}"
                ),
            ));
        }
        let validator = jsonschema::draft202012::options()
            .with_retriever(DenyExternalReferences)
            .build(schema)
            .map_err(|error| ContractViolation {
                phase: ContractPhase::Admission,
                target: target.clone(),
                issues: vec![issue_from_error(&error)],
            })?;
        Ok(Self { target, validator })
    }

    /// Validate one boundary value and retain every path-addressed issue.
    pub fn validate(&self, value: &Value) -> ContractResult<()> {
        let mut issues = self
            .validator
            .iter_errors(value)
            .map(|error| issue_from_error(&error))
            .collect::<Vec<_>>();
        if issues.is_empty() {
            return Ok(());
        }
        issues.sort_by(|left, right| {
            (&left.instance_path, &left.schema_path, &left.message).cmp(&(
                &right.instance_path,
                &right.schema_path,
                &right.message,
            ))
        });
        issues.dedup();
        Err(ContractViolation {
            phase: ContractPhase::Execution,
            target: self.target.clone(),
            issues,
        })
    }

    /// Return the exact semantic boundary owned by this validator.
    pub const fn target(&self) -> &ContractTarget {
        &self.target
    }
}

/// Every executable schema compiled from one unchanged Plan Candidate.
pub struct PlanContracts {
    definitions: BTreeMap<(String, ContractSide), ContractValidator>,
    components: BTreeMap<(String, ContractSide), ContractValidator>,
    effects: BTreeMap<(String, ContractSide), ContractValidator>,
    waits: BTreeMap<String, ContractValidator>,
}

impl PlanContracts {
    /// Compile all definition, component, effect, and typed-wait schemas from a
    /// candidate. This never normalizes or rewrites the candidate used for its
    /// canonical Plan identity.
    pub fn compile(candidate: &PlanCandidate) -> ContractResult<Self> {
        let mut definitions = BTreeMap::new();
        for definition in &candidate.definitions {
            definitions.insert(
                (definition.id.clone(), ContractSide::Input),
                ContractValidator::compile(
                    ContractTarget::definition(&definition.id, ContractSide::Input),
                    &definition.input_schema,
                )?,
            );
            definitions.insert(
                (definition.id.clone(), ContractSide::Output),
                ContractValidator::compile(
                    ContractTarget::definition(&definition.id, ContractSide::Output),
                    &definition.output_schema,
                )?,
            );
        }
        let mut components = BTreeMap::new();
        for component in &candidate.components {
            components.insert(
                (component.id.clone(), ContractSide::Input),
                ContractValidator::compile(
                    ContractTarget::component(&component.id, ContractSide::Input),
                    &component.input_schema,
                )?,
            );
            components.insert(
                (component.id.clone(), ContractSide::Output),
                ContractValidator::compile(
                    ContractTarget::component(&component.id, ContractSide::Output),
                    &component.output_schema,
                )?,
            );
        }
        let mut effects = BTreeMap::new();
        for effect in &candidate.effects {
            effects.insert(
                (effect.id.clone(), ContractSide::Input),
                ContractValidator::compile(
                    ContractTarget::effect(&effect.id, ContractSide::Input),
                    &effect.input_schema,
                )?,
            );
            effects.insert(
                (effect.id.clone(), ContractSide::Output),
                ContractValidator::compile(
                    ContractTarget::effect(&effect.id, ContractSide::Output),
                    &effect.output_schema,
                )?,
            );
        }
        let mut waits = BTreeMap::new();
        for definition in &candidate.definitions {
            compile_waits(&definition.body, &mut waits)?;
        }
        Ok(Self {
            definitions,
            components,
            effects,
            waits,
        })
    }

    /// Validate input entering a named definition, including the Run entry.
    pub fn validate_definition_input(&self, id: &str, value: &Value) -> ContractResult<()> {
        validate_selected(
            &self.definitions,
            ContractTarget::definition(id, ContractSide::Input),
            value,
        )
    }

    /// Validate a named definition result before it returns to its caller or
    /// becomes the terminal Run result.
    pub fn validate_definition_output(&self, id: &str, value: &Value) -> ContractResult<()> {
        validate_selected(
            &self.definitions,
            ContractTarget::definition(id, ContractSide::Output),
            value,
        )
    }

    /// Validate component input before invoking its plugin realization.
    pub fn validate_component_input(&self, id: &str, value: &Value) -> ContractResult<()> {
        validate_selected(
            &self.components,
            ContractTarget::component(id, ContractSide::Input),
            value,
        )
    }

    /// Validate a component response before recording or binding it.
    pub fn validate_component_output(&self, id: &str, value: &Value) -> ContractResult<()> {
        validate_selected(
            &self.components,
            ContractTarget::component(id, ContractSide::Output),
            value,
        )
    }

    /// Validate effect input before preparation or dispatch.
    pub fn validate_effect_input(&self, id: &str, value: &Value) -> ContractResult<()> {
        validate_selected(
            &self.effects,
            ContractTarget::effect(id, ContractSide::Input),
            value,
        )
    }

    /// Validate an observed or reconciled effect result before recording it.
    pub fn validate_effect_output(&self, id: &str, value: &Value) -> ContractResult<()> {
        validate_selected(
            &self.effects,
            ContractTarget::effect(id, ContractSide::Output),
            value,
        )
    }

    /// Validate typed external input completing a stable wait site.
    pub fn validate_wait_input(&self, site_id: &str, value: &Value) -> ContractResult<()> {
        let target = ContractTarget::wait(site_id);
        let Some(validator) = self.waits.get(site_id) else {
            return Err(missing_contract(target));
        };
        validator.validate(value)
    }
}

fn validate_selected(
    validators: &BTreeMap<(String, ContractSide), ContractValidator>,
    target: ContractTarget,
    value: &Value,
) -> ContractResult<()> {
    let key = (target.id.clone(), target.side);
    let Some(validator) = validators.get(&key) else {
        return Err(missing_contract(target));
    };
    validator.validate(value)
}

fn missing_contract(target: ContractTarget) -> ContractViolation {
    ContractViolation {
        phase: ContractPhase::Execution,
        target,
        issues: vec![ContractIssue {
            instance_path: String::new(),
            schema_path: String::new(),
            message: "contract target was not compiled from the admitted Plan".to_owned(),
        }],
    }
}

fn admission_issue(
    target: ContractTarget,
    instance_path: &str,
    message: String,
) -> ContractViolation {
    ContractViolation {
        phase: ContractPhase::Admission,
        target,
        issues: vec![ContractIssue {
            instance_path: instance_path.to_owned(),
            schema_path: String::new(),
            message,
        }],
    }
}

fn compile_waits(
    region: &Region,
    validators: &mut BTreeMap<String, ContractValidator>,
) -> ContractResult<()> {
    for step in &region.steps {
        match &step.operation {
            Operation::Wait {
                wait: WaitSpec::Input { schema, .. },
                ..
            } => {
                validators.insert(
                    step.id.clone(),
                    ContractValidator::compile(ContractTarget::wait(&step.id), schema)?,
                );
            }
            Operation::Scope { body, .. } => compile_waits(body, validators)?,
            Operation::Call { .. }
            | Operation::Invoke { .. }
            | Operation::Wait { .. }
            | Operation::Effect { .. } => {}
        }
    }
    Ok(())
}

fn issue_from_error(error: &jsonschema::ValidationError<'_>) -> ContractIssue {
    ContractIssue {
        instance_path: error.instance_path().to_string(),
        schema_path: error.schema_path().to_string(),
        message: error.masked().to_string(),
    }
}
