use std::collections::{BTreeMap, BTreeSet};

use cymule_core::{Definition, Operation, PlanCandidate, Region, SealedPlan, content_id};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::{EvolutionError, EvolutionResult, control::validate_identity};

/// Immutable reusable-definition revision domain.
pub const SUBFLOW_REVISION_VERSION: &str = "cymule.subflow-revision/2";
/// Maximum direct exact dependencies retained by one reusable definition.
pub const MAX_SUBFLOW_REFERENCES: usize = 1_024;
/// Maximum aggregate canonical bytes retained by one reusable-definition
/// dependency list.
pub const MAX_SUBFLOW_REFERENCE_BYTES: usize = 1024 * 1024;
/// Maximum transitive reusable-definition dependency depth admitted by the
/// pure linker.
pub const MAX_SUBFLOW_REFERENCE_DEPTH: usize = 128;

/// Content domain for one injected reusable-definition identity.
const LINKED_DEFINITION_ID_DOMAIN: &str = "cymule.linked-definition/1";

/// Resolution strategy retained by an unsealed parent template.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "strategy", rename_all = "snake_case", deny_unknown_fields)]
pub enum ReferenceStrategy {
    /// Resolve the newest revision whose declared contract remains compatible.
    LatestCompatible,
    /// Resolve one exact immutable revision.
    Pinned {
        /// Required revision identity.
        revision_id: String,
    },
}

/// One immutable reusable definition revision.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SubflowRevision {
    /// Revision schema and semantic version.
    pub revision_version: String,
    /// Content-addressed revision identity.
    pub revision_id: String,
    /// Stable logical reference followed by parent templates.
    pub logical_ref: String,
    /// Monotonic registry order used only for latest selection.
    pub sequence: u64,
    /// Reusable definition content.
    pub definition: Definition,
    /// Logical dependencies used by this reusable definition.
    pub references: Vec<SubflowReference>,
}

/// One parent-template reference to a reusable definition.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SubflowReference {
    /// Stable registry reference.
    pub logical_ref: String,
    /// Definition ID injected into the linked parent Plan.
    pub local_definition: String,
    /// Contract expected by the parent call sites.
    pub input_schema: Value,
    /// Contract expected by parent result consumers.
    pub output_schema: Value,
    /// Future link resolution policy.
    pub strategy: ReferenceStrategy,
}

impl SubflowReference {
    /// Construct a logical reference with the safe default strategy.
    pub fn latest_compatible(
        logical_ref: impl Into<String>,
        local_definition: impl Into<String>,
        input_schema: Value,
        output_schema: Value,
    ) -> Self {
        Self {
            logical_ref: logical_ref.into(),
            local_definition: local_definition.into(),
            input_schema,
            output_schema,
            strategy: ReferenceStrategy::LatestCompatible,
        }
    }
}

/// Unsealed parent source plus its logical reusable-definition references.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PlanTemplate {
    /// Stable template identity used by reverse dependency indexes.
    pub template_id: String,
    /// Candidate containing invocation sites for injected local definitions.
    pub candidate: PlanCandidate,
    /// Logical references resolved when linking.
    pub references: Vec<SubflowReference>,
}

/// One immutable parent Plan produced by exact reference resolution.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LinkedPlan {
    /// Source template.
    pub template_id: String,
    /// Sealed immutable parent Plan.
    pub plan: SealedPlan,
    /// Exact revision selected for each logical reference.
    pub resolved_revisions: BTreeMap<String, String>,
}

/// Link one template from an exact bounded revision view without constructing
/// registry history. The supplied map must equal the transitive revision
/// closure reached from the template.
pub(crate) fn link_from_revision_view(
    template: &PlanTemplate,
    revisions: &BTreeMap<String, SubflowRevision>,
) -> EvolutionResult<LinkedPlan> {
    validate_template_shape(template)?;
    for (logical_ref, revision) in revisions {
        validate_revision(logical_ref, revision)?;
    }

    let mut resolved_revisions = BTreeMap::new();
    for reference in &template.references {
        collect_resolution(
            reference,
            revisions,
            &mut resolved_revisions,
            &mut BTreeSet::new(),
            0,
        )?;
    }

    let mut candidate = template.candidate.clone();
    let mut used_revisions = BTreeSet::new();
    for reference in &template.references {
        materialize_reference(
            reference,
            &reference.local_definition,
            revisions,
            &mut candidate,
            &mut BTreeSet::new(),
            &mut used_revisions,
            0,
        )?;
    }
    if used_revisions.len() != revisions.len()
        || !revisions
            .keys()
            .all(|logical_ref| used_revisions.contains(logical_ref))
    {
        return Err(EvolutionError::Validation(format!(
            "template {} has extraneous exact revisions",
            template.template_id
        )));
    }
    let plan = cymule_core::seal_plan(candidate)?;
    Ok(LinkedPlan {
        template_id: template.template_id.clone(),
        plan,
        resolved_revisions,
    })
}

/// Seal a definition-only revision at the caller-owned monotonic sequence.
pub(crate) fn seal_definition_revision(
    logical_ref: String,
    definition: Definition,
    references: Vec<SubflowReference>,
    sequence: u64,
) -> EvolutionResult<SubflowRevision> {
    validate_identity("subflow reference", &logical_ref)?;
    validate_core_name("definition", &definition.id)?;
    if sequence == 0 || sequence > cymule_core::MAX_EXACT_INTEGER {
        return Err(EvolutionError::Validation(
            "subflow sequence exceeds the JSON exact-integer range".to_owned(),
        ));
    }
    validate_publication_references(&logical_ref, &definition, &references)?;
    let revision_id = content_id(
        SUBFLOW_REVISION_VERSION,
        &(logical_ref.as_str(), &definition, &references),
    )?;
    Ok(SubflowRevision {
        revision_version: SUBFLOW_REVISION_VERSION.to_owned(),
        revision_id,
        logical_ref,
        sequence,
        definition,
        references,
    })
}

pub(crate) fn validate_revision(
    logical_ref: &str,
    revision: &SubflowRevision,
) -> EvolutionResult<()> {
    validate_identity("subflow reference", logical_ref)?;
    if revision.revision_version != SUBFLOW_REVISION_VERSION
        || revision.logical_ref != logical_ref
        || revision.sequence == 0
        || revision.sequence > cymule_core::MAX_EXACT_INTEGER
    {
        return Err(EvolutionError::Validation(format!(
            "subflow revision {} has an invalid normalized envelope",
            revision.revision_id
        )));
    }
    validate_core_name("definition", &revision.definition.id)?;
    validate_publication_references(logical_ref, &revision.definition, &revision.references)?;
    let expected_id = content_id(
        SUBFLOW_REVISION_VERSION,
        &(logical_ref, &revision.definition, &revision.references),
    )?;
    if revision.revision_id != expected_id {
        return Err(EvolutionError::Validation(format!(
            "subflow revision {} does not match its immutable content",
            revision.revision_id
        )));
    }
    Ok(())
}

fn resolve_reference<'a>(
    reference: &SubflowReference,
    revisions: &'a BTreeMap<String, SubflowRevision>,
) -> EvolutionResult<&'a SubflowRevision> {
    let revision = revisions.get(&reference.logical_ref).ok_or_else(|| {
        EvolutionError::NotFound(format!(
            "subflow reference {} has no exact revision",
            reference.logical_ref
        ))
    })?;
    if revision.definition.input_schema != reference.input_schema
        || revision.definition.output_schema != reference.output_schema
        || matches!(
            &reference.strategy,
            ReferenceStrategy::Pinned { revision_id }
                if revision_id != &revision.revision_id
        )
    {
        return Err(EvolutionError::Conflict(format!(
            "subflow reference {} does not match its exact compatible revision",
            reference.logical_ref
        )));
    }
    Ok(revision)
}

fn collect_resolution(
    reference: &SubflowReference,
    revisions: &BTreeMap<String, SubflowRevision>,
    resolved: &mut BTreeMap<String, String>,
    visiting: &mut BTreeSet<String>,
    depth: usize,
) -> EvolutionResult<()> {
    if depth >= MAX_SUBFLOW_REFERENCE_DEPTH {
        return Err(EvolutionError::Validation(format!(
            "reusable module dependency depth exceeds {MAX_SUBFLOW_REFERENCE_DEPTH}"
        )));
    }
    if !visiting.insert(reference.logical_ref.clone()) {
        return Err(EvolutionError::Conflict(format!(
            "reusable module dependency cycle reaches {}",
            reference.logical_ref
        )));
    }
    let revision = resolve_reference(reference, revisions)?;
    match resolved.get(&reference.logical_ref) {
        Some(existing) if existing != &revision.revision_id => {
            return Err(EvolutionError::Conflict(format!(
                "subflow reference {} resolves to incompatible revision choices",
                reference.logical_ref
            )));
        }
        Some(_) => {
            visiting.remove(&reference.logical_ref);
            return Ok(());
        }
        None => {
            resolved.insert(reference.logical_ref.clone(), revision.revision_id.clone());
        }
    }
    for dependency in &revision.references {
        collect_resolution(dependency, revisions, resolved, visiting, depth + 1)?;
    }
    visiting.remove(&reference.logical_ref);
    Ok(())
}

fn materialize_reference(
    reference: &SubflowReference,
    injected_id: &str,
    revisions: &BTreeMap<String, SubflowRevision>,
    candidate: &mut PlanCandidate,
    visiting: &mut BTreeSet<String>,
    used_revisions: &mut BTreeSet<String>,
    depth: usize,
) -> EvolutionResult<()> {
    if depth >= MAX_SUBFLOW_REFERENCE_DEPTH {
        return Err(EvolutionError::Validation(format!(
            "reusable module dependency depth exceeds {MAX_SUBFLOW_REFERENCE_DEPTH}"
        )));
    }
    if !visiting.insert(reference.logical_ref.clone()) {
        return Err(EvolutionError::Conflict(format!(
            "reusable module dependency cycle reaches {}",
            reference.logical_ref
        )));
    }
    let revision = resolve_reference(reference, revisions)?;
    used_revisions.insert(reference.logical_ref.clone());
    let mut definition = revision.definition.clone();
    let original_id = definition.id.clone();
    injected_id.clone_into(&mut definition.id);
    rewrite_invocation(&mut definition.body, &original_id, injected_id);
    for dependency in &revision.references {
        let dependency_id = linked_dependency_id(injected_id, dependency)?;
        rewrite_invocation(
            &mut definition.body,
            &dependency.local_definition,
            &dependency_id,
        );
        materialize_reference(
            dependency,
            &dependency_id,
            revisions,
            candidate,
            visiting,
            used_revisions,
            depth + 1,
        )?;
    }
    visiting.remove(&reference.logical_ref);
    if candidate
        .definitions
        .iter()
        .any(|existing| existing.id == definition.id)
    {
        return Err(EvolutionError::Conflict(format!(
            "linked definition {} collides with another definition",
            definition.id
        )));
    }
    candidate.definitions.push(definition);
    Ok(())
}

fn rewrite_invocation(region: &mut Region, original: &str, linked: &str) {
    for step in &mut region.steps {
        match &mut step.operation {
            Operation::Invoke { definition, .. } if definition == original => {
                definition.clone_from(&linked.to_owned());
            }
            Operation::Scope { body, .. } => rewrite_invocation(body, original, linked),
            _ => {}
        }
    }
}

fn linked_dependency_id(parent: &str, reference: &SubflowReference) -> EvolutionResult<String> {
    let identity = content_id(
        LINKED_DEFINITION_ID_DOMAIN,
        &(parent, &reference.logical_ref, &reference.local_definition),
    )?;
    let digest = identity.strip_prefix("sha256:").ok_or_else(|| {
        EvolutionError::Validation("linked definition identity is malformed".to_owned())
    })?;
    Ok(format!("linked.{}", &digest[..32]))
}

fn validate_module_references(
    logical_ref: &str,
    definition: &Definition,
    references: &[SubflowReference],
) -> EvolutionResult<()> {
    let mut logical_refs = BTreeSet::new();
    let mut local_definitions = BTreeSet::from([definition.id.clone()]);
    for reference in references {
        validate_identity("subflow reference", &reference.logical_ref)?;
        validate_core_name("local definition", &reference.local_definition)?;
        if !logical_refs.insert(reference.logical_ref.clone()) {
            return Err(EvolutionError::Validation(format!(
                "reusable module {logical_ref} repeats subflow reference {}",
                reference.logical_ref
            )));
        }
        if !local_definitions.insert(reference.local_definition.clone()) {
            return Err(EvolutionError::Validation(format!(
                "reusable module {logical_ref} repeats local definition {}",
                reference.local_definition
            )));
        }
    }
    Ok(())
}

pub(crate) fn validate_publication_references(
    logical_ref: &str,
    definition: &Definition,
    references: &[SubflowReference],
) -> EvolutionResult<()> {
    validate_reference_bounds("reusable definition", logical_ref, references)?;
    validate_module_references(logical_ref, definition, references)?;
    let mut previous = None;
    for reference in references {
        if previous.is_some_and(|previous| previous >= reference.logical_ref.as_str()) {
            return Err(EvolutionError::Validation(format!(
                "reusable definition {logical_ref} references are not strictly logical-reference ordered"
            )));
        }
        previous = Some(reference.logical_ref.as_str());
        let ReferenceStrategy::Pinned { revision_id } = &reference.strategy else {
            return Err(EvolutionError::Validation(format!(
                "reusable definition {logical_ref} dependency {} must pin one exact revision",
                reference.logical_ref
            )));
        };
        cymule_core::validate_content_id("pinned reusable-definition revision", revision_id)?;
        validate_reference_contract(reference)?;
    }
    for (side, schema) in [
        (
            cymule_runtime::ContractSide::Input,
            &definition.input_schema,
        ),
        (
            cymule_runtime::ContractSide::Output,
            &definition.output_schema,
        ),
    ] {
        cymule_runtime::ContractValidator::compile(
            cymule_runtime::ContractTarget {
                boundary: cymule_runtime::ContractBoundary::Definition,
                id: definition.id.clone(),
                side,
            },
            schema,
        )?;
    }
    Ok(())
}

pub(crate) fn validate_template_shape(template: &PlanTemplate) -> EvolutionResult<()> {
    validate_identity("template", &template.template_id)?;
    validate_reference_bounds("template", &template.template_id, &template.references)?;
    let mut logical_refs = BTreeSet::new();
    let mut local_definitions: BTreeSet<String> = template
        .candidate
        .definitions
        .iter()
        .map(|definition| definition.id.clone())
        .collect();
    let mut previous = None;
    for reference in &template.references {
        validate_identity("subflow reference", &reference.logical_ref)?;
        validate_core_name("local definition", &reference.local_definition)?;
        if previous.is_some_and(|previous| previous >= reference.logical_ref.as_str()) {
            return Err(EvolutionError::Validation(format!(
                "template {} references are not strictly logical-reference ordered",
                template.template_id
            )));
        }
        previous = Some(reference.logical_ref.as_str());
        if let ReferenceStrategy::Pinned { revision_id } = &reference.strategy {
            cymule_core::validate_content_id("pinned reusable-definition revision", revision_id)?;
        }
        validate_reference_contract(reference)?;
        if !logical_refs.insert(reference.logical_ref.clone()) {
            return Err(EvolutionError::Validation(format!(
                "template {} repeats subflow reference {}",
                template.template_id, reference.logical_ref
            )));
        }
        if !local_definitions.insert(reference.local_definition.clone()) {
            return Err(EvolutionError::Validation(format!(
                "template {} repeats local definition {}",
                template.template_id, reference.local_definition
            )));
        }
    }
    Ok(())
}

fn validate_reference_bounds(
    kind: &str,
    identity: &str,
    references: &[SubflowReference],
) -> EvolutionResult<()> {
    if references.len() > MAX_SUBFLOW_REFERENCES {
        return Err(EvolutionError::Validation(format!(
            "{kind} {identity} exceeds the {MAX_SUBFLOW_REFERENCES} direct-reference bound"
        )));
    }
    let encoded = cymule_core::canonical_bytes(&references)?;
    if encoded.len() > MAX_SUBFLOW_REFERENCE_BYTES {
        return Err(EvolutionError::Validation(format!(
            "{kind} {identity} references use {} canonical bytes, above the {MAX_SUBFLOW_REFERENCE_BYTES} byte bound",
            encoded.len()
        )));
    }
    Ok(())
}

fn validate_reference_contract(reference: &SubflowReference) -> EvolutionResult<()> {
    for (side, schema) in [
        (cymule_runtime::ContractSide::Input, &reference.input_schema),
        (
            cymule_runtime::ContractSide::Output,
            &reference.output_schema,
        ),
    ] {
        cymule_runtime::ContractValidator::compile(
            cymule_runtime::ContractTarget {
                boundary: cymule_runtime::ContractBoundary::Definition,
                id: reference.local_definition.clone(),
                side,
            },
            schema,
        )?;
    }
    Ok(())
}

fn validate_core_name(kind: &str, value: &str) -> EvolutionResult<()> {
    let valid = !value.is_empty()
        && value.len() <= 256
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b'.'));
    if valid {
        Ok(())
    } else {
        Err(EvolutionError::Validation(format!(
            "{kind} identity contains unsupported characters"
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cymule_core::{Expression, Region};
    use serde_json::json;

    fn definition() -> Definition {
        Definition {
            id: "module".to_owned(),
            input_schema: json!({}),
            output_schema: json!({}),
            body: Region {
                steps: Vec::new(),
                result: Expression::Literal { value: json!(null) },
            },
        }
    }

    fn pinned(revision: &SubflowRevision) -> SubflowReference {
        SubflowReference {
            logical_ref: revision.logical_ref.clone(),
            local_definition: format!("local-{}", revision.logical_ref),
            input_schema: json!({}),
            output_schema: json!({}),
            strategy: ReferenceStrategy::Pinned {
                revision_id: revision.revision_id.clone(),
            },
        }
    }

    #[test]
    fn reference_strategy_is_an_explicit_required_wire_member() {
        let reference = SubflowReference::latest_compatible(
            "module-main",
            "module-main-local",
            json!({}),
            json!({}),
        );
        let mut wire = serde_json::to_value(reference).unwrap();
        wire.as_object_mut().unwrap().remove("strategy");
        assert!(serde_json::from_value::<SubflowReference>(wire).is_err());
    }

    #[test]
    fn exact_dependency_depth_accepts_limit_and_rejects_next_level() {
        let leaf =
            seal_definition_revision("dependency-000".to_owned(), definition(), Vec::new(), 1)
                .unwrap();
        let mut revisions = BTreeMap::from([(leaf.logical_ref.clone(), leaf.clone())]);
        let mut head = leaf;
        for index in 1..MAX_SUBFLOW_REFERENCE_DEPTH {
            let revision = seal_definition_revision(
                format!("dependency-{index:03}"),
                definition(),
                vec![pinned(&head)],
                1,
            )
            .unwrap();
            revisions.insert(revision.logical_ref.clone(), revision.clone());
            head = revision;
        }
        let mut resolved = BTreeMap::new();
        collect_resolution(
            &pinned(&head),
            &revisions,
            &mut resolved,
            &mut BTreeSet::new(),
            0,
        )
        .unwrap();
        assert_eq!(resolved.len(), MAX_SUBFLOW_REFERENCE_DEPTH);

        let overflow = seal_definition_revision(
            "dependency-overflow".to_owned(),
            definition(),
            vec![pinned(&head)],
            1,
        )
        .unwrap();
        revisions.insert(overflow.logical_ref.clone(), overflow.clone());
        let error = collect_resolution(
            &pinned(&overflow),
            &revisions,
            &mut BTreeMap::new(),
            &mut BTreeSet::new(),
            0,
        )
        .unwrap_err();
        assert!(matches!(error, EvolutionError::Validation(_)));
    }
}
