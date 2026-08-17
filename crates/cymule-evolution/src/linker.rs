use std::collections::{BTreeMap, BTreeSet};

use cymule_core::{Definition, Operation, PlanCandidate, Region, SealedPlan, content_id};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{EvolutionError, EvolutionResult};

/// Immutable reusable-definition revision domain.
pub const SUBFLOW_REVISION_VERSION: &str = "cymule.subflow-revision/2";

/// Portable registry snapshot schema and semantic version.
pub const DEFINITION_REGISTRY_VERSION: &str = "cymule.definition-registry/1";

/// Resolution strategy retained by an unsealed parent template.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "strategy", rename_all = "snake_case")]
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
    #[serde(default)]
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

/// Complete portable state for deterministic registry recovery.
///
/// The reverse-dependency index is intentionally omitted because it is a
/// derived acceleration structure. Restore rebuilds it from the templates and
/// verifies every current and historical link before accepting the snapshot.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DefinitionRegistrySnapshot {
    /// Snapshot schema and semantic version.
    pub registry_version: String,
    /// Immutable revisions grouped by stable logical reference.
    pub revisions: BTreeMap<String, Vec<SubflowRevision>>,
    /// Registered parent sources.
    pub templates: BTreeMap<String, PlanTemplate>,
    /// Latest immutable link selected for each template.
    pub current_links: BTreeMap<String, LinkedPlan>,
    /// All previously emitted links keyed by immutable Plan ID.
    pub link_history: BTreeMap<String, LinkedPlan>,
}

/// Provider-neutral reusable definition registry and deterministic linker.
#[derive(Debug, Clone, Default)]
pub struct DefinitionRegistry {
    revisions: BTreeMap<String, Vec<SubflowRevision>>,
    templates: BTreeMap<String, PlanTemplate>,
    reverse_dependencies: BTreeMap<String, BTreeSet<String>>,
    current_links: BTreeMap<String, LinkedPlan>,
    link_history: BTreeMap<String, LinkedPlan>,
}

impl DefinitionRegistry {
    /// Create an empty registry.
    pub fn new() -> Self {
        Self::default()
    }

    /// Export complete provider-neutral state for durable checkpointing.
    pub fn snapshot(&self) -> DefinitionRegistrySnapshot {
        DefinitionRegistrySnapshot {
            registry_version: DEFINITION_REGISTRY_VERSION.to_owned(),
            revisions: self.revisions.clone(),
            templates: self.templates.clone(),
            current_links: self.current_links.clone(),
            link_history: self.link_history.clone(),
        }
    }

    /// Restore and fully verify a portable registry snapshot.
    pub fn restore(snapshot: DefinitionRegistrySnapshot) -> EvolutionResult<Self> {
        if snapshot.registry_version != DEFINITION_REGISTRY_VERSION {
            return Err(EvolutionError::Validation(format!(
                "unsupported definition registry version {}",
                snapshot.registry_version
            )));
        }

        let mut registry = Self::new();
        for (logical_ref, revisions) in &snapshot.revisions {
            validate_name("subflow reference", logical_ref)?;
            for (index, revision) in revisions.iter().enumerate() {
                let expected_sequence = u64::try_from(index)
                    .map_err(|error| EvolutionError::Validation(error.to_string()))?
                    .checked_add(1)
                    .ok_or_else(|| {
                        EvolutionError::Validation("subflow sequence exhausted".to_owned())
                    })?;
                if revision.revision_version != SUBFLOW_REVISION_VERSION
                    || revision.logical_ref != *logical_ref
                    || revision.sequence != expected_sequence
                {
                    return Err(EvolutionError::Validation(format!(
                        "subflow revision {} has an invalid envelope or sequence",
                        revision.revision_id
                    )));
                }
                validate_name("definition", &revision.definition.id)?;
                let expected_id = content_id(
                    SUBFLOW_REVISION_VERSION,
                    &(
                        logical_ref.as_str(),
                        &revision.definition,
                        &revision.references,
                    ),
                )?;
                if revision.revision_id != expected_id {
                    return Err(EvolutionError::Validation(format!(
                        "subflow revision {} does not match its content",
                        revision.revision_id
                    )));
                }
                validate_module_references(
                    logical_ref,
                    &revision.definition,
                    &revision.references,
                )?;
            }
        }
        registry.revisions = snapshot.revisions.clone();

        for template in snapshot.templates.values() {
            registry.register_template(template.clone())?;
        }
        if registry.current_links != snapshot.current_links {
            return Err(EvolutionError::Validation(
                "definition registry current links do not match deterministic resolution"
                    .to_owned(),
            ));
        }

        for (plan_id, linked) in &snapshot.link_history {
            if linked.plan.plan_id != *plan_id {
                return Err(EvolutionError::Validation(format!(
                    "historical link key {plan_id} does not match its Plan ID"
                )));
            }
            linked.plan.verify()?;
            let template = snapshot.templates.get(&linked.template_id).ok_or_else(|| {
                EvolutionError::Validation(format!(
                    "historical link {plan_id} references missing template {}",
                    linked.template_id
                ))
            })?;
            let expected = registry.link_exact(template, &linked.resolved_revisions)?;
            if expected != *linked {
                return Err(EvolutionError::Validation(format!(
                    "historical link {plan_id} does not match its exact revisions"
                )));
            }
        }
        for linked in snapshot.current_links.values() {
            if snapshot.link_history.get(&linked.plan.plan_id) != Some(linked) {
                return Err(EvolutionError::Validation(format!(
                    "current Plan {} is missing from link history",
                    linked.plan.plan_id
                )));
            }
        }
        registry.link_history = snapshot.link_history;
        Ok(registry)
    }

    /// Publish one immutable revision and relink every transitive dependent.
    pub fn publish_and_relink(
        &mut self,
        logical_ref: impl Into<String>,
        definition: Definition,
    ) -> EvolutionResult<(SubflowRevision, Vec<LinkedPlan>)> {
        self.publish_module_and_relink(logical_ref, definition, Vec::new())
    }

    /// Publish one immutable reusable module and relink all transitive callers.
    pub fn publish_module_and_relink(
        &mut self,
        logical_ref: impl Into<String>,
        definition: Definition,
        references: Vec<SubflowReference>,
    ) -> EvolutionResult<(SubflowRevision, Vec<LinkedPlan>)> {
        let revision = self.publish_module(logical_ref, definition, references)?;
        let impacted_refs = self.transitive_dependents(&revision.logical_ref);
        let template_ids: BTreeSet<String> = impacted_refs
            .iter()
            .filter_map(|logical_ref| self.reverse_dependencies.get(logical_ref))
            .flat_map(BTreeSet::iter)
            .cloned()
            .collect();
        let mut linked = Vec::new();
        for template_id in template_ids {
            linked.push(self.link_registered(&template_id)?);
        }
        Ok((revision, linked))
    }

    /// Publish one immutable revision without changing dependent defaults.
    pub fn publish(
        &mut self,
        logical_ref: impl Into<String>,
        definition: Definition,
    ) -> EvolutionResult<SubflowRevision> {
        self.publish_module(logical_ref, definition, Vec::new())
    }

    /// Publish one immutable reusable module without relinking callers.
    pub fn publish_module(
        &mut self,
        logical_ref: impl Into<String>,
        definition: Definition,
        references: Vec<SubflowReference>,
    ) -> EvolutionResult<SubflowRevision> {
        let logical_ref = logical_ref.into();
        validate_name("subflow reference", &logical_ref)?;
        validate_name("definition", &definition.id)?;
        validate_module_references(&logical_ref, &definition, &references)?;
        let revision_id = content_id(
            SUBFLOW_REVISION_VERSION,
            &(logical_ref.as_str(), &definition, &references),
        )?;
        let revisions = self.revisions.entry(logical_ref.clone()).or_default();
        if let Some(existing) = revisions
            .iter()
            .find(|revision| revision.revision_id == revision_id)
        {
            return Ok(existing.clone());
        }
        let sequence = u64::try_from(revisions.len())
            .map_err(|error| EvolutionError::Validation(error.to_string()))?
            .checked_add(1)
            .ok_or_else(|| EvolutionError::Validation("subflow sequence exhausted".to_owned()))?;
        let revision = SubflowRevision {
            revision_version: SUBFLOW_REVISION_VERSION.to_owned(),
            revision_id,
            logical_ref,
            sequence,
            definition,
            references,
        };
        revisions.push(revision.clone());
        Ok(revision)
    }

    /// Register a parent template and link its current exact dependencies.
    pub fn register_template(&mut self, template: PlanTemplate) -> EvolutionResult<LinkedPlan> {
        validate_name("template", &template.template_id)?;
        let template_id = template.template_id.clone();
        let mut logical_refs = BTreeSet::new();
        let mut local_definitions: BTreeSet<String> = template
            .candidate
            .definitions
            .iter()
            .map(|definition| definition.id.clone())
            .collect();
        for reference in &template.references {
            validate_name("subflow reference", &reference.logical_ref)?;
            validate_name("local definition", &reference.local_definition)?;
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
        match self.templates.get(&template.template_id) {
            Some(existing) if existing != &template => {
                return Err(EvolutionError::Conflict(format!(
                    "template {} already has different content",
                    template.template_id
                )));
            }
            Some(_) => {}
            None => {
                for reference in &template.references {
                    self.reverse_dependencies
                        .entry(reference.logical_ref.clone())
                        .or_default()
                        .insert(template.template_id.clone());
                }
                self.templates
                    .insert(template.template_id.clone(), template);
            }
        }
        self.link_registered(&template_id)
    }

    /// Latest linked immutable Plan for one template.
    pub fn current_link(&self, template_id: &str) -> Option<&LinkedPlan> {
        self.current_links.get(template_id)
    }

    /// Historical linked Plan by content identity.
    pub fn historical_link(&self, plan_id: &str) -> Option<&LinkedPlan> {
        self.link_history.get(plan_id)
    }

    fn link_registered(&mut self, template_id: &str) -> EvolutionResult<LinkedPlan> {
        let template = self.templates.get(template_id).cloned().ok_or_else(|| {
            EvolutionError::NotFound(format!("template {template_id} is missing"))
        })?;
        let linked = self.link(&template)?;
        self.link_history
            .insert(linked.plan.plan_id.clone(), linked.clone());
        self.current_links
            .insert(template_id.to_owned(), linked.clone());
        Ok(linked)
    }

    fn link(&self, template: &PlanTemplate) -> EvolutionResult<LinkedPlan> {
        let mut resolved_revisions = BTreeMap::new();
        for reference in &template.references {
            self.collect_resolution(reference, &mut resolved_revisions, &mut BTreeSet::new())?;
        }
        self.link_exact(template, &resolved_revisions)
    }

    fn link_exact(
        &self,
        template: &PlanTemplate,
        resolved_revisions: &BTreeMap<String, String>,
    ) -> EvolutionResult<LinkedPlan> {
        let mut candidate = template.candidate.clone();
        let mut used_revisions = BTreeSet::new();
        for reference in &template.references {
            self.materialize_reference(
                reference,
                &reference.local_definition,
                resolved_revisions,
                &mut candidate,
                &mut BTreeSet::new(),
                &mut used_revisions,
            )?;
        }
        if used_revisions.len() != resolved_revisions.len()
            || !resolved_revisions
                .keys()
                .all(|logical_ref| used_revisions.contains(logical_ref))
        {
            return Err(EvolutionError::Validation(format!(
                "template {} has extraneous exact revisions",
                template.template_id
            )));
        }
        let plan = candidate.seal()?;
        Ok(LinkedPlan {
            template_id: template.template_id.clone(),
            plan,
            resolved_revisions: resolved_revisions.clone(),
        })
    }

    fn resolve(&self, reference: &SubflowReference) -> EvolutionResult<&SubflowRevision> {
        let revisions = self.revisions.get(&reference.logical_ref).ok_or_else(|| {
            EvolutionError::NotFound(format!(
                "subflow reference {} has no revisions",
                reference.logical_ref
            ))
        })?;
        let compatible = |revision: &&SubflowRevision| {
            revision.definition.input_schema == reference.input_schema
                && revision.definition.output_schema == reference.output_schema
        };
        match &reference.strategy {
            ReferenceStrategy::LatestCompatible => {
                revisions.iter().rev().find(compatible).ok_or_else(|| {
                    EvolutionError::Conflict(format!(
                        "subflow reference {} has no compatible revision",
                        reference.logical_ref
                    ))
                })
            }
            ReferenceStrategy::Pinned { revision_id } => revisions
                .iter()
                .find(|revision| revision.revision_id == *revision_id)
                .filter(compatible)
                .ok_or_else(|| {
                    EvolutionError::Conflict(format!(
                        "pinned subflow revision {revision_id} is missing or incompatible"
                    ))
                }),
        }
    }

    fn collect_resolution(
        &self,
        reference: &SubflowReference,
        resolved: &mut BTreeMap<String, String>,
        visiting: &mut BTreeSet<String>,
    ) -> EvolutionResult<()> {
        if visiting.contains(&reference.logical_ref) {
            return Err(EvolutionError::Conflict(format!(
                "reusable module dependency cycle reaches {}",
                reference.logical_ref
            )));
        }
        let revision = self.resolve(reference)?;
        match resolved.get(&reference.logical_ref) {
            Some(existing) if existing != &revision.revision_id => {
                return Err(EvolutionError::Conflict(format!(
                    "subflow reference {} resolves to incompatible revision choices",
                    reference.logical_ref
                )));
            }
            Some(_) => return Ok(()),
            None => {
                resolved.insert(reference.logical_ref.clone(), revision.revision_id.clone());
            }
        }
        visiting.insert(reference.logical_ref.clone());
        for dependency in &revision.references {
            self.collect_resolution(dependency, resolved, visiting)?;
        }
        visiting.remove(&reference.logical_ref);
        Ok(())
    }

    fn materialize_reference(
        &self,
        reference: &SubflowReference,
        injected_id: &str,
        resolved: &BTreeMap<String, String>,
        candidate: &mut PlanCandidate,
        visiting: &mut BTreeSet<String>,
        used_revisions: &mut BTreeSet<String>,
    ) -> EvolutionResult<()> {
        let revision = self.resolve_exact(reference, resolved)?;
        used_revisions.insert(reference.logical_ref.clone());
        if !visiting.insert(reference.logical_ref.clone()) {
            return Err(EvolutionError::Conflict(format!(
                "reusable module dependency cycle reaches {}",
                reference.logical_ref
            )));
        }
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
            self.materialize_reference(
                dependency,
                &dependency_id,
                resolved,
                candidate,
                visiting,
                used_revisions,
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

    fn resolve_exact<'a>(
        &'a self,
        reference: &SubflowReference,
        resolved: &BTreeMap<String, String>,
    ) -> EvolutionResult<&'a SubflowRevision> {
        let revision_id = resolved.get(&reference.logical_ref).ok_or_else(|| {
            EvolutionError::Validation(format!(
                "exact revision set is missing {}",
                reference.logical_ref
            ))
        })?;
        self.revisions
            .get(&reference.logical_ref)
            .and_then(|revisions| {
                revisions
                    .iter()
                    .find(|revision| revision.revision_id == *revision_id)
            })
            .filter(|revision| {
                revision.definition.input_schema == reference.input_schema
                    && revision.definition.output_schema == reference.output_schema
            })
            .ok_or_else(|| {
                EvolutionError::Conflict(format!(
                    "exact subflow revision {revision_id} is missing or incompatible"
                ))
            })
    }

    fn transitive_dependents(&self, changed: &str) -> BTreeSet<String> {
        let mut impacted = BTreeSet::from([changed.to_owned()]);
        loop {
            let before = impacted.len();
            for (logical_ref, revisions) in &self.revisions {
                if revisions.iter().any(|revision| {
                    revision
                        .references
                        .iter()
                        .any(|reference| impacted.contains(&reference.logical_ref))
                }) {
                    impacted.insert(logical_ref.clone());
                }
            }
            if impacted.len() == before {
                return impacted;
            }
        }
    }
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
        "cymule.linked-definition/1",
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
        validate_name("subflow reference", &reference.logical_ref)?;
        validate_name("local definition", &reference.local_definition)?;
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

fn validate_name(kind: &str, value: &str) -> EvolutionResult<()> {
    if value.is_empty() || value.len() > 160 {
        return Err(EvolutionError::Validation(format!(
            "{kind} must contain 1..=160 characters"
        )));
    }
    Ok(())
}
