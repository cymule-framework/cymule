use std::collections::{BTreeMap, BTreeSet};

use cymule_core::{
    ComponentContract, Definition, EffectContract, Operation, Region, SealedPlan, canonical_digest,
    content_id,
};
use serde::{Deserialize, Serialize};

use crate::{EvolutionError, EvolutionResult};

/// Frozen compatibility-report domain for automatic future-head relinking.
pub const RELINK_COMPATIBILITY_VERSION: &str = "cymule.relink-compatibility/1";

/// One conservative reason why a candidate cannot automatically replace a
/// future-work default.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum RelinkViolation {
    /// Entry input schema changed.
    EntryInputChanged,
    /// Entry output schema changed.
    EntryOutputChanged,
    /// A reachable component was not reachable under the current Plan.
    ComponentAdded {
        /// Abstract component identity.
        component: String,
    },
    /// A reachable component contract or its provider-neutral requirements changed.
    ComponentContractChanged {
        /// Abstract component identity.
        component: String,
    },
    /// A reachable world effect was not reachable under the current Plan.
    EffectAdded {
        /// Abstract effect identity.
        effect: String,
    },
    /// A reachable effect contract, safety profile, or requirements changed.
    EffectContractChanged {
        /// Abstract effect identity.
        effect: String,
    },
    /// A new external wait contract became reachable.
    WaitAdded {
        /// Canonical wait-contract digest.
        wait_digest: String,
    },
}

/// Deterministic no-widening report for one proposed future-head change.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RelinkCompatibility {
    /// Report schema and semantic version.
    pub compatibility_version: String,
    /// Content-addressed report identity.
    pub compatibility_id: String,
    /// Current immutable parent Plan.
    pub from_plan: String,
    /// Candidate immutable parent Plan.
    pub to_plan: String,
    /// Closed, deterministic violations. Empty means automatic relink is legal.
    pub violations: Vec<RelinkViolation>,
}

impl RelinkCompatibility {
    /// Whether the candidate may become the future-work default automatically.
    pub const fn is_compatible(&self) -> bool {
        self.violations.is_empty()
    }
}

#[derive(Default)]
struct ReachableSurface {
    components: BTreeSet<String>,
    effects: BTreeSet<String>,
    waits: BTreeSet<String>,
}

/// Analyze an immutable parent Plan update under the strict no-widening
/// `latest_compatible` profile.
pub fn analyze_relink(from: &SealedPlan, to: &SealedPlan) -> EvolutionResult<RelinkCompatibility> {
    from.verify()?;
    to.verify()?;
    let from_entry = entry_definition(from)?;
    let to_entry = entry_definition(to)?;
    let from_surface = reachable_surface(from)?;
    let to_surface = reachable_surface(to)?;
    let from_components: BTreeMap<&str, &ComponentContract> = from
        .candidate
        .components
        .iter()
        .map(|contract| (contract.id.as_str(), contract))
        .collect();
    let to_components: BTreeMap<&str, &ComponentContract> = to
        .candidate
        .components
        .iter()
        .map(|contract| (contract.id.as_str(), contract))
        .collect();
    let from_effects: BTreeMap<&str, &EffectContract> = from
        .candidate
        .effects
        .iter()
        .map(|contract| (contract.id.as_str(), contract))
        .collect();
    let to_effects: BTreeMap<&str, &EffectContract> = to
        .candidate
        .effects
        .iter()
        .map(|contract| (contract.id.as_str(), contract))
        .collect();

    let mut violations = Vec::new();
    if from_entry.input_schema != to_entry.input_schema {
        violations.push(RelinkViolation::EntryInputChanged);
    }
    if from_entry.output_schema != to_entry.output_schema {
        violations.push(RelinkViolation::EntryOutputChanged);
    }
    for component in &to_surface.components {
        if !from_surface.components.contains(component) {
            violations.push(RelinkViolation::ComponentAdded {
                component: component.clone(),
            });
        } else if from_components.get(component.as_str()) != to_components.get(component.as_str()) {
            violations.push(RelinkViolation::ComponentContractChanged {
                component: component.clone(),
            });
        }
    }
    for effect in &to_surface.effects {
        if !from_surface.effects.contains(effect) {
            violations.push(RelinkViolation::EffectAdded {
                effect: effect.clone(),
            });
        } else if from_effects.get(effect.as_str()) != to_effects.get(effect.as_str()) {
            violations.push(RelinkViolation::EffectContractChanged {
                effect: effect.clone(),
            });
        }
    }
    for wait_digest in to_surface.waits.difference(&from_surface.waits) {
        violations.push(RelinkViolation::WaitAdded {
            wait_digest: wait_digest.clone(),
        });
    }
    let compatibility_id = content_id(
        RELINK_COMPATIBILITY_VERSION,
        &(from.plan_id.as_str(), to.plan_id.as_str(), &violations),
    )?;
    Ok(RelinkCompatibility {
        compatibility_version: RELINK_COMPATIBILITY_VERSION.to_owned(),
        compatibility_id,
        from_plan: from.plan_id.clone(),
        to_plan: to.plan_id.clone(),
        violations,
    })
}

fn entry_definition(plan: &SealedPlan) -> EvolutionResult<&Definition> {
    plan.candidate
        .definitions
        .iter()
        .find(|definition| definition.id == plan.candidate.entry)
        .ok_or_else(|| {
            EvolutionError::Validation(format!(
                "Plan {} has no entry definition {}",
                plan.plan_id, plan.candidate.entry
            ))
        })
}

fn reachable_surface(plan: &SealedPlan) -> EvolutionResult<ReachableSurface> {
    let definitions: BTreeMap<&str, &Definition> = plan
        .candidate
        .definitions
        .iter()
        .map(|definition| (definition.id.as_str(), definition))
        .collect();
    let mut surface = ReachableSurface::default();
    visit_definition(
        &plan.candidate.entry,
        &definitions,
        &mut BTreeSet::new(),
        &mut surface,
    )?;
    Ok(surface)
}

fn visit_definition(
    definition_id: &str,
    definitions: &BTreeMap<&str, &Definition>,
    visited: &mut BTreeSet<String>,
    surface: &mut ReachableSurface,
) -> EvolutionResult<()> {
    if !visited.insert(definition_id.to_owned()) {
        return Ok(());
    }
    let definition = definitions.get(definition_id).ok_or_else(|| {
        EvolutionError::Validation(format!("reachable definition {definition_id} is missing"))
    })?;
    visit_region(&definition.body, definitions, visited, surface)
}

fn visit_region(
    region: &Region,
    definitions: &BTreeMap<&str, &Definition>,
    visited: &mut BTreeSet<String>,
    surface: &mut ReachableSurface,
) -> EvolutionResult<()> {
    for step in &region.steps {
        match &step.operation {
            Operation::Call { component, .. } => {
                surface.components.insert(component.clone());
            }
            Operation::Invoke { definition, .. } => {
                visit_definition(definition, definitions, visited, surface)?;
            }
            Operation::Wait { wait } => {
                surface.waits.insert(canonical_digest(wait)?);
            }
            Operation::Effect { effect, .. } => {
                surface.effects.insert(effect.clone());
            }
            Operation::Scope { body, .. } => {
                visit_region(body, definitions, visited, surface)?;
            }
        }
    }
    Ok(())
}
