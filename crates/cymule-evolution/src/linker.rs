use std::collections::{BTreeMap, BTreeSet};

use cymule_core::{Definition, Operation, PlanCandidate, Region, SealedPlan, content_id};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{EvolutionError, EvolutionResult};

/// Immutable reusable-definition revision domain.
pub const SUBFLOW_REVISION_VERSION: &str = "cymule.subflow-revision/1";

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

    /// Publish one immutable revision and relink every direct dependent.
    pub fn publish_and_relink(
        &mut self,
        logical_ref: impl Into<String>,
        definition: Definition,
    ) -> EvolutionResult<(SubflowRevision, Vec<LinkedPlan>)> {
        let revision = self.publish(logical_ref, definition)?;
        let template_ids = self
            .reverse_dependencies
            .get(&revision.logical_ref)
            .cloned()
            .unwrap_or_default();
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
        let logical_ref = logical_ref.into();
        validate_name("subflow reference", &logical_ref)?;
        validate_name("definition", &definition.id)?;
        let revision_id = content_id(
            SUBFLOW_REVISION_VERSION,
            &(logical_ref.as_str(), &definition),
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
        let mut candidate = template.candidate.clone();
        let mut resolved_revisions = BTreeMap::new();
        for reference in &template.references {
            let revision = self.resolve(reference)?;
            let mut definition = revision.definition.clone();
            let original_id = definition.id.clone();
            definition.id.clone_from(&reference.local_definition);
            rewrite_self_invocation(
                &mut definition.body,
                &original_id,
                &reference.local_definition,
            );
            candidate.definitions.push(definition);
            resolved_revisions.insert(reference.logical_ref.clone(), revision.revision_id.clone());
        }
        let plan = candidate.seal()?;
        Ok(LinkedPlan {
            template_id: template.template_id.clone(),
            plan,
            resolved_revisions,
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
}

fn rewrite_self_invocation(region: &mut Region, original: &str, linked: &str) {
    for step in &mut region.steps {
        match &mut step.operation {
            Operation::Invoke { definition, .. } if definition == original => {
                definition.clone_from(&linked.to_owned());
            }
            Operation::Scope { body, .. } => rewrite_self_invocation(body, original, linked),
            _ => {}
        }
    }
}

fn validate_name(kind: &str, value: &str) -> EvolutionResult<()> {
    if value.is_empty() || value.len() > 160 {
        return Err(EvolutionError::Validation(format!(
            "{kind} must contain 1..=160 characters"
        )));
    }
    Ok(())
}
