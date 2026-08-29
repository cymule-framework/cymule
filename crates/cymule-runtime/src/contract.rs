use std::collections::{BTreeMap, BTreeSet};
use std::fmt::{Display, Formatter};

use cymule_core::{Operation, PlanCandidate, Region, WaitSpec, canonical_bytes, validate_identity};
use jsonschema::{Retrieve, Uri, Validator};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::composition::deserialize_bounded_vec;

/// JSON Schema dialect used by every executable Plan contract.
pub const CONTRACT_SCHEMA_DIALECT: &str = "https://json-schema.org/draft/2020-12/schema";
/// Maximum concrete validation issues retained by the contract authority.
pub const MAX_CONCRETE_CONTRACT_ISSUES: usize = 99;
/// Maximum complete issue set, including one omission summary.
pub const MAX_CONTRACT_ISSUES: usize = MAX_CONCRETE_CONTRACT_ISSUES + 1;
/// Maximum Unicode scalars in one retained JSON Pointer.
pub const MAX_CONTRACT_POINTER_SCALARS: usize = 1_000;
/// Maximum Unicode scalars in one retained issue message.
pub const MAX_CONTRACT_MESSAGE_SCALARS: usize = 2_000;
/// Maximum canonical bytes in one complete structured contract violation.
pub const MAX_CONTRACT_VIOLATION_BYTES: usize = 1024 * 1024;
const CONTRACT_ISSUE_BYTE_BUDGET: usize = MAX_CONTRACT_VIOLATION_BYTES - 16 * 1024;
const INVALID_CONTRACT_TARGET_ID: &str = "invalid-contract-target";
const CONTRACT_ISSUES_OMITTED_MESSAGE: &str =
    "additional contract issues were omitted after the fixed validation budget";

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

    /// Validate the exact semantic target identity before authority lookup.
    ///
    /// # Errors
    ///
    /// Returns an error when the target identity is empty, control-bearing, or
    /// exceeds the shared 512-scalar semantic identity bound.
    pub fn verify(&self) -> Result<(), String> {
        validate_identity("contract target", &self.id).map_err(|error| error.to_string())
    }

    fn invalid_projection(&self) -> Self {
        Self {
            boundary: self.boundary,
            id: INVALID_CONTRACT_TARGET_ID.to_owned(),
            side: self.side,
        }
    }
}

/// One path-addressed JSON Schema issue without retaining the checked value.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ContractIssueKind {
    /// One concrete validator issue.
    Validation,
    /// Fixed terminal summary indicating that the source issue budget ended.
    Omitted,
}

/// One path-addressed JSON Schema issue without retaining the checked value.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ContractIssue {
    /// Closed issue class.
    pub kind: ContractIssueKind,
    /// JSON Pointer into the submitted schema during admission or checked value
    /// during execution.
    pub instance_path: String,
    /// JSON Pointer to the failing schema keyword.
    pub schema_path: String,
    /// Human-readable issue summary with instance content masked.
    pub message: String,
}

impl ContractIssue {
    /// Validate one retained masked issue independently of its source validator.
    ///
    /// # Errors
    ///
    /// Returns an error when either path or the message is outside the closed
    /// bounded projection contract.
    pub fn verify(&self) -> Result<(), String> {
        verify_contract_pointer(&self.instance_path, "contract instance path")?;
        verify_contract_pointer(&self.schema_path, "contract schema path")?;
        let message_scalars = self.message.chars().count();
        if message_scalars == 0
            || message_scalars > MAX_CONTRACT_MESSAGE_SCALARS
            || self.message.chars().any(char::is_control)
        {
            return Err(format!(
                "contract issue message must contain 1..={MAX_CONTRACT_MESSAGE_SCALARS} non-control Unicode scalar values"
            ));
        }
        match self.kind {
            ContractIssueKind::Validation => {}
            ContractIssueKind::Omitted
                if self.instance_path.is_empty()
                    && self.schema_path.is_empty()
                    && self.message == CONTRACT_ISSUES_OMITTED_MESSAGE => {}
            ContractIssueKind::Omitted => {
                return Err("contract omission issue has invalid fields".to_owned());
            }
        }
        Ok(())
    }
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
    #[serde(deserialize_with = "deserialize_contract_issues")]
    pub issues: Vec<ContractIssue>,
}

fn deserialize_contract_issues<'de, D>(deserializer: D) -> Result<Vec<ContractIssue>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    deserialize_bounded_vec::<D, ContractIssue, MAX_CONTRACT_ISSUES>(
        deserializer,
        "contract issues",
    )
}

impl ContractViolation {
    /// Validate one complete closed violation after deserialization or before
    /// cross-boundary projection.
    ///
    /// # Errors
    ///
    /// Returns an error when the target, issue set, or canonical byte envelope
    /// exceeds the contract authority's fixed bounds.
    pub fn verify(&self) -> Result<(), String> {
        self.target.verify()?;
        if self.issues.is_empty() || self.issues.len() > MAX_CONTRACT_ISSUES {
            return Err(format!(
                "contract violation must contain 1..={MAX_CONTRACT_ISSUES} issues"
            ));
        }
        let mut previous = None;
        for (index, issue) in self.issues.iter().enumerate() {
            issue.verify()?;
            if matches!(issue.kind, ContractIssueKind::Omitted) {
                if index + 1 != self.issues.len() {
                    return Err("contract omission summary must be the final issue".to_owned());
                }
                continue;
            }
            if index >= MAX_CONCRETE_CONTRACT_ISSUES {
                return Err(format!(
                    "contract violation exceeds {MAX_CONCRETE_CONTRACT_ISSUES} concrete issues"
                ));
            }
            if previous.is_some_and(|previous: &ContractIssue| previous >= issue) {
                return Err("contract concrete issues are not strictly ordered".to_owned());
            }
            previous = Some(issue);
        }
        let canonical_size = canonical_bytes(self)
            .map_err(|error| error.to_string())?
            .len();
        if canonical_size > MAX_CONTRACT_VIOLATION_BYTES {
            return Err(format!(
                "contract violation uses {canonical_size} canonical bytes, above the {MAX_CONTRACT_VIOLATION_BYTES} byte bound"
            ));
        }
        Ok(())
    }
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
    ///
    /// # Errors
    ///
    /// Returns a contract violation when the dialect is not exact or the schema
    /// cannot be compiled under the closed Draft 2020-12 resolver.
    pub fn compile(target: ContractTarget, schema: &Value) -> ContractResult<Self> {
        if let Err(message) = target.verify() {
            return Err(admission_issue(target.invalid_projection(), "", &message));
        }
        if let Some(declared) = schema.get("$schema")
            && declared.as_str() != Some(CONTRACT_SCHEMA_DIALECT)
        {
            let message = format!(
                "schema dialect must be exactly {CONTRACT_SCHEMA_DIALECT:?}, received {declared}"
            );
            return Err(admission_issue(target, "/$schema", &message));
        }
        let validator = jsonschema::draft202012::options()
            .with_retriever(DenyExternalReferences)
            .build(schema)
            .map_err(|error| {
                closed_violation(
                    ContractPhase::Admission,
                    target.clone(),
                    vec![issue_from_error(&error)],
                )
            })?;
        Ok(Self { target, validator })
    }

    /// Validate one boundary value and retain a fixed bounded issue prefix.
    ///
    /// # Errors
    ///
    /// Returns an execution-phase contract violation containing at most 99
    /// concrete path-addressed issues plus one omission summary.
    pub fn validate(&self, value: &Value) -> ContractResult<()> {
        let issues = bounded_validation_issues(&self.validator, value);
        if issues.is_empty() {
            return Ok(());
        }
        Err(closed_violation(
            ContractPhase::Execution,
            self.target.clone(),
            issues,
        ))
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
    ///
    /// # Errors
    ///
    /// Returns a contract violation when any executable schema or typed wait
    /// schema cannot be compiled under the closed resolver.
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
    ///
    /// # Errors
    ///
    /// Returns a contract violation when the definition has no compiled input
    /// contract or the value fails it.
    pub fn validate_definition_input(&self, id: &str, value: &Value) -> ContractResult<()> {
        validate_selected(
            &self.definitions,
            ContractTarget::definition(id, ContractSide::Input),
            value,
        )
    }

    /// Validate a named definition result before it returns to its caller or
    /// becomes the terminal Run result.
    ///
    /// # Errors
    ///
    /// Returns a contract violation when the definition has no compiled output
    /// contract or the value fails it.
    pub fn validate_definition_output(&self, id: &str, value: &Value) -> ContractResult<()> {
        validate_selected(
            &self.definitions,
            ContractTarget::definition(id, ContractSide::Output),
            value,
        )
    }

    /// Validate component input before invoking its plugin realization.
    ///
    /// # Errors
    ///
    /// Returns a contract violation when the component has no compiled input
    /// contract or the value fails it.
    pub fn validate_component_input(&self, id: &str, value: &Value) -> ContractResult<()> {
        validate_selected(
            &self.components,
            ContractTarget::component(id, ContractSide::Input),
            value,
        )
    }

    /// Validate a component response before recording or binding it.
    ///
    /// # Errors
    ///
    /// Returns a contract violation when the component has no compiled output
    /// contract or the value fails it.
    pub fn validate_component_output(&self, id: &str, value: &Value) -> ContractResult<()> {
        validate_selected(
            &self.components,
            ContractTarget::component(id, ContractSide::Output),
            value,
        )
    }

    /// Validate effect input before preparation or dispatch.
    ///
    /// # Errors
    ///
    /// Returns a contract violation when the effect has no compiled input
    /// contract or the value fails it.
    pub fn validate_effect_input(&self, id: &str, value: &Value) -> ContractResult<()> {
        validate_selected(
            &self.effects,
            ContractTarget::effect(id, ContractSide::Input),
            value,
        )
    }

    /// Validate an observed or reconciled effect result before recording it.
    ///
    /// # Errors
    ///
    /// Returns a contract violation when the effect has no compiled output
    /// contract or the value fails it.
    pub fn validate_effect_output(&self, id: &str, value: &Value) -> ContractResult<()> {
        validate_selected(
            &self.effects,
            ContractTarget::effect(id, ContractSide::Output),
            value,
        )
    }

    /// Validate typed external input completing a stable wait site.
    ///
    /// # Errors
    ///
    /// Returns a contract violation when the wait site has no compiled contract
    /// or the external value fails it.
    pub fn validate_wait_input(&self, site_id: &str, value: &Value) -> ContractResult<()> {
        let target = ContractTarget::wait(site_id);
        if let Err(message) = target.verify() {
            return Err(execution_issue(target.invalid_projection(), &message));
        }
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
    if let Err(message) = target.verify() {
        return Err(execution_issue(target.invalid_projection(), &message));
    }
    let key = (target.id.clone(), target.side);
    let Some(validator) = validators.get(&key) else {
        return Err(missing_contract(target));
    };
    validator.validate(value)
}

fn missing_contract(target: ContractTarget) -> ContractViolation {
    closed_violation(
        ContractPhase::Execution,
        target,
        vec![ContractIssue {
            kind: ContractIssueKind::Validation,
            instance_path: String::new(),
            schema_path: String::new(),
            message: "contract target was not compiled from the admitted Plan".to_owned(),
        }],
    )
}

fn admission_issue(
    target: ContractTarget,
    instance_path: &str,
    message: &str,
) -> ContractViolation {
    closed_violation(
        ContractPhase::Admission,
        target,
        vec![ContractIssue {
            kind: ContractIssueKind::Validation,
            instance_path: bounded_pointer(instance_path),
            schema_path: String::new(),
            message: bounded_message(message),
        }],
    )
}

fn execution_issue(target: ContractTarget, message: &str) -> ContractViolation {
    closed_violation(
        ContractPhase::Execution,
        target,
        vec![ContractIssue {
            kind: ContractIssueKind::Validation,
            instance_path: String::new(),
            schema_path: String::new(),
            message: bounded_message(message),
        }],
    )
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

fn closed_violation(
    phase: ContractPhase,
    target: ContractTarget,
    issues: Vec<ContractIssue>,
) -> ContractViolation {
    let violation = ContractViolation {
        phase,
        target,
        issues,
    };
    debug_assert!(violation.verify().is_ok());
    violation
}

fn bounded_validation_issues(validator: &Validator, value: &Value) -> Vec<ContractIssue> {
    let mut errors = validator.iter_errors(value);
    let mut unique = BTreeSet::new();
    let mut retained_bytes = 0_usize;
    let mut omitted = false;
    for _ in 0..MAX_CONCRETE_CONTRACT_ISSUES {
        let Some(error) = errors.next() else {
            break;
        };
        let issue = issue_from_error(&error);
        if unique.contains(&issue) {
            continue;
        }
        let issue_bytes =
            canonical_bytes(&issue).map_or(CONTRACT_ISSUE_BYTE_BUDGET, |bytes| bytes.len());
        let Some(next_bytes) = retained_bytes.checked_add(issue_bytes) else {
            omitted = true;
            break;
        };
        if next_bytes > CONTRACT_ISSUE_BYTE_BUDGET {
            omitted = true;
            break;
        }
        retained_bytes = next_bytes;
        unique.insert(issue);
    }
    if !omitted && errors.next().is_some() {
        omitted = true;
    }
    let mut issues = unique.into_iter().collect::<Vec<_>>();
    if omitted {
        issues.push(ContractIssue {
            kind: ContractIssueKind::Omitted,
            instance_path: String::new(),
            schema_path: String::new(),
            message: CONTRACT_ISSUES_OMITTED_MESSAGE.to_owned(),
        });
    }
    issues
}

fn verify_contract_pointer(value: &str, label: &str) -> Result<(), String> {
    if value.chars().count() > MAX_CONTRACT_POINTER_SCALARS
        || value.chars().any(char::is_control)
        || !value.is_empty() && !value.starts_with('/')
    {
        return Err(format!(
            "{label} must be an empty or slash-prefixed JSON Pointer of at most {MAX_CONTRACT_POINTER_SCALARS} non-control Unicode scalar values"
        ));
    }
    Ok(())
}

fn bounded_pointer(value: &str) -> String {
    if value.chars().any(char::is_control) {
        return String::new();
    }
    let value = value
        .chars()
        .take(MAX_CONTRACT_POINTER_SCALARS)
        .collect::<String>();
    if value.is_empty() || value.starts_with('/') {
        value
    } else {
        String::new()
    }
}

fn bounded_message(value: &str) -> String {
    let mut message = value
        .chars()
        .map(|character| {
            if character.is_control() {
                ' '
            } else {
                character
            }
        })
        .take(MAX_CONTRACT_MESSAGE_SCALARS)
        .collect::<String>();
    if message.trim().is_empty() {
        "contract validation failed".clone_into(&mut message);
    }
    message
}

fn issue_from_error(error: &jsonschema::ValidationError<'_>) -> ContractIssue {
    ContractIssue {
        kind: ContractIssueKind::Validation,
        instance_path: bounded_pointer(&error.instance_path().to_string()),
        schema_path: bounded_pointer(&error.schema_path().to_string()),
        message: bounded_message(&error.masked().to_string()),
    }
}
