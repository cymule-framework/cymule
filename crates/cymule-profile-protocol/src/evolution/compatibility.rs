use std::collections::{BTreeMap, BTreeSet};

use cymule_core::{
    ComponentContract, Definition, DispatchPolicy, EffectContract, MutationKind, Operation, Region,
    SealedPlan, canonical_digest, content_id,
};
use serde::{Deserialize, Serialize};

use super::{EvolutionError, EvolutionResult};

/// Frozen compatibility-report domain for automatic future-head relinking.
pub const RELINK_COMPATIBILITY_VERSION: &str = "cymule.relink-compatibility/1";

/// One conservative reason why a candidate cannot automatically replace a
/// future-work default.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
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
    effect_sites: BTreeMap<String, String>,
    waits: BTreeSet<String>,
}

/// Analyze an immutable parent Plan update under the strict no-widening
/// `latest_compatible` profile.
///
/// # Errors
///
/// Returns an error when either Plan is invalid or its reachable semantic
/// surface cannot be analyzed deterministically.
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
    for (site_digest, effect) in &to_surface.effect_sites {
        if from_surface.effects.contains(effect)
            && !from_surface.effect_sites.contains_key(site_digest)
        {
            violations.push(RelinkViolation::EffectAdded {
                effect: format!("{effect}@{site_digest}"),
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

/// Enforce the migration descriptor's directional no-widening contract.
///
/// Migration may deliberately adapt entry schemas and interpreter state, so it
/// is stricter than accepting every relink violation but narrower than the
/// automatic-relink profile. A target may remove a reachable operation, but it
/// may not add a reachable Effect, make an existing Effect less constrained,
/// or delete/change an existing component or Effect requirement. New target
/// requirements are additional constraints and therefore remain legal.
pub(crate) fn validate_migration_no_widening(
    from: &SealedPlan,
    to: &SealedPlan,
) -> EvolutionResult<()> {
    from.verify()?;
    to.verify()?;
    let from_surface = reachable_surface(from)?;
    let to_surface = reachable_surface(to)?;
    let from_components = from
        .candidate
        .components
        .iter()
        .map(|contract| (contract.id.as_str(), contract))
        .collect::<BTreeMap<_, _>>();
    let to_components = to
        .candidate
        .components
        .iter()
        .map(|contract| (contract.id.as_str(), contract))
        .collect::<BTreeMap<_, _>>();
    for component in &to_surface.components {
        let target = to_components.get(component.as_str()).ok_or_else(|| {
            EvolutionError::Validation(format!(
                "reachable target component {component} has no contract"
            ))
        })?;
        let source = from_surface
            .components
            .contains(component)
            .then(|| from_components.get(component.as_str()).copied())
            .flatten();
        if component_requirements_widened(source, target) {
            return Err(EvolutionError::Conflict(format!(
                "migration target component {component} widens authority or capability requirements through provider properties"
            )));
        }
    }

    let from_effects = from
        .candidate
        .effects
        .iter()
        .map(|contract| (contract.id.as_str(), contract))
        .collect::<BTreeMap<_, _>>();
    let to_effects = to
        .candidate
        .effects
        .iter()
        .map(|contract| (contract.id.as_str(), contract))
        .collect::<BTreeMap<_, _>>();
    for effect in &to_surface.effects {
        let target = to_effects.get(effect.as_str()).ok_or_else(|| {
            EvolutionError::Validation(format!("reachable target Effect {effect} has no contract"))
        })?;
        let Some(source) = from_surface
            .effects
            .contains(effect)
            .then(|| from_effects.get(effect.as_str()).copied())
            .flatten()
        else {
            return Err(EvolutionError::Conflict(format!(
                "migration target adds reachable Effect {effect}"
            )));
        };
        if effect_contract_widened(source, target) {
            return Err(EvolutionError::Conflict(format!(
                "migration target Effect {effect} widens its executable contract"
            )));
        }
    }
    if let Some((site_digest, effect)) = to_surface.effect_sites.iter().find(|(site, effect)| {
        from_surface.effects.contains(*effect) && !from_surface.effect_sites.contains_key(*site)
    }) {
        return Err(EvolutionError::Conflict(format!(
            "migration target adds reachable Effect {effect} at semantic site {site_digest}"
        )));
    }
    Ok(())
}

fn component_requirements_widened(
    source: Option<&ComponentContract>,
    target: &ComponentContract,
) -> bool {
    source.is_some_and(|contract| {
        contract
            .requirements
            .iter()
            .any(|(property, source_value)| target.requirements.get(property) != Some(source_value))
    }) || source.is_none() && !target.requirements.is_empty()
}

fn effect_contract_widened(source: &EffectContract, target: &EffectContract) -> bool {
    source.input_schema != target.input_schema
        || source.output_schema != target.output_schema
        || matches!(
            (source.profile.mutation, target.profile.mutation),
            (MutationKind::Observational, MutationKind::Mutating)
        )
        || dispatch_authority(target.profile.dispatch) > dispatch_authority(source.profile.dispatch)
        || source.profile.reconciliation != target.profile.reconciliation
        || (source.profile.keyed_idempotency && !target.profile.keyed_idempotency)
        || (!source.profile.irreversible && target.profile.irreversible)
        || source
            .requirements
            .iter()
            .any(|(property, source_value)| target.requirements.get(property) != Some(source_value))
}

const fn dispatch_authority(policy: DispatchPolicy) -> u8 {
    match policy {
        DispatchPolicy::Explicit => 0,
        DispatchPolicy::OnScopeCommit => 1,
        DispatchPolicy::Eager => 2,
    }
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
    let mut call_path = vec![plan.candidate.entry.clone()];
    visit_definition(
        &plan.candidate.entry,
        &definitions,
        &mut BTreeSet::new(),
        &mut call_path,
        &mut surface,
    )?;
    Ok(surface)
}

fn visit_definition(
    definition_id: &str,
    definitions: &BTreeMap<&str, &Definition>,
    visiting: &mut BTreeSet<String>,
    call_path: &mut Vec<String>,
    surface: &mut ReachableSurface,
) -> EvolutionResult<()> {
    if !visiting.insert(definition_id.to_owned()) {
        return Err(EvolutionError::Conflict(format!(
            "reachable definition cycle includes {definition_id}"
        )));
    }
    let definition = definitions.get(definition_id).ok_or_else(|| {
        EvolutionError::Validation(format!("reachable definition {definition_id} is missing"))
    })?;
    call_path.push(canonical_digest(*definition)?);
    let result = visit_region(
        &definition.body,
        definitions,
        visiting,
        call_path,
        &mut Vec::new(),
        surface,
    );
    call_path.pop();
    visiting.remove(definition_id);
    result
}

fn visit_region(
    region: &Region,
    definitions: &BTreeMap<&str, &Definition>,
    visiting: &mut BTreeSet<String>,
    call_path: &mut Vec<String>,
    scope_path: &mut Vec<String>,
    surface: &mut ReachableSurface,
) -> EvolutionResult<()> {
    for step in &region.steps {
        match &step.operation {
            Operation::Call { component, .. } => {
                surface.components.insert(component.clone());
            }
            Operation::Invoke { definition, .. } => {
                call_path.push(step.id.clone());
                call_path.push(canonical_digest(&step.operation)?);
                let result =
                    visit_definition(definition, definitions, visiting, call_path, surface);
                call_path.pop();
                call_path.pop();
                result?;
            }
            Operation::Wait { wait, .. } => {
                surface.waits.insert(canonical_digest(wait)?);
            }
            Operation::Effect {
                effect, occurrence, ..
            } => {
                surface.effects.insert(effect.clone());
                let site_digest = canonical_digest(&(
                    call_path.as_slice(),
                    scope_path.as_slice(),
                    step.id.as_str(),
                    effect.as_str(),
                    occurrence.as_str(),
                    &step.operation,
                ))?;
                surface.effect_sites.insert(site_digest, effect.clone());
            }
            Operation::Scope { body, .. } => {
                scope_path.push(step.id.clone());
                let result =
                    visit_region(body, definitions, visiting, call_path, scope_path, surface);
                scope_path.pop();
                result?;
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use cymule_core::{
        EffectProfile, Expression, PlanCandidate, ReconciliationMode, Step, seal_plan,
    };
    use serde_json::json;

    fn definition(steps: Vec<Step>) -> Definition {
        Definition {
            id: "main".to_owned(),
            input_schema: json!({}),
            output_schema: json!({}),
            body: Region {
                steps,
                result: Expression::Literal { value: json!(null) },
            },
        }
    }

    fn plan(
        name: &str,
        components: Vec<ComponentContract>,
        effects: Vec<EffectContract>,
        steps: Vec<Step>,
    ) -> SealedPlan {
        seal_plan(PlanCandidate {
            ir_version: cymule_core::IR_VERSION.to_owned(),
            name: name.to_owned(),
            entry: "main".to_owned(),
            components,
            effects,
            definitions: vec![definition(steps)],
            metadata: BTreeMap::new(),
        })
        .expect("test Plan seals")
    }

    #[test]
    fn migration_no_widening_rejects_new_reachable_component_authority() {
        let component = ComponentContract {
            id: "test.compute".to_owned(),
            input_schema: json!({}),
            output_schema: json!({}),
            output_artifact_kind: cymule_core::COMPONENT_OUTPUT_ARTIFACT_KIND.to_owned(),
            requirements: BTreeMap::from([("capability".to_owned(), "network".to_owned())]),
        };
        let source = plan(
            "source_component",
            vec![component.clone()],
            Vec::new(),
            Vec::new(),
        );
        let target = plan(
            "target_component",
            vec![component],
            Vec::new(),
            vec![Step {
                id: "call.compute".to_owned(),
                operation: Operation::Call {
                    component: "test.compute".to_owned(),
                    input: Expression::Input,
                    bind: None,
                },
            }],
        );
        assert!(matches!(
            validate_migration_no_widening(&source, &target),
            Err(EvolutionError::Conflict(message))
                if message.contains("authority or capability")
        ));
    }

    #[test]
    fn migration_no_widening_treats_added_requirements_as_narrowing() {
        let component_step = Step {
            id: "call.provider".to_owned(),
            operation: Operation::Call {
                component: "test.provider".to_owned(),
                input: Expression::Input,
                bind: None,
            },
        };
        let source_component = ComponentContract {
            id: "test.provider".to_owned(),
            input_schema: json!({}),
            output_schema: json!({}),
            output_artifact_kind: cymule_core::COMPONENT_OUTPUT_ARTIFACT_KIND.to_owned(),
            requirements: BTreeMap::from([("runtime".to_owned(), "sandbox-v1".to_owned())]),
        };
        let source = plan(
            "source_provider_property",
            vec![source_component.clone()],
            Vec::new(),
            vec![component_step.clone()],
        );
        let mut target_component = source_component;
        target_component
            .requirements
            .insert("isolation".to_owned(), "network-denied".to_owned());
        let target = plan(
            "target_provider_property",
            vec![target_component],
            Vec::new(),
            vec![component_step],
        );
        validate_migration_no_widening(&source, &target)
            .expect("an additional target requirement narrows provider admission");
        let mut deleted_component = target.candidate.clone();
        deleted_component.name = "target_deleted_provider_property".to_owned();
        deleted_component.components[0]
            .requirements
            .remove("runtime");
        let deleted_component =
            seal_plan(deleted_component).expect("deleted component requirement Plan seals");
        assert!(matches!(
            validate_migration_no_widening(&target, &deleted_component),
            Err(EvolutionError::Conflict(message))
                if message.contains("authority or capability")
        ));
    }

    #[test]
    fn migration_no_widening_is_directional_for_reachable_effects() {
        let source_effect = EffectContract {
            id: "test.write".to_owned(),
            input_schema: json!({}),
            output_schema: json!({}),
            profile: EffectProfile {
                mutation: MutationKind::Mutating,
                dispatch: DispatchPolicy::Explicit,
                reconciliation: ReconciliationMode::Queryable,
                keyed_idempotency: true,
                irreversible: false,
            },
            requirements: BTreeMap::from([("authority".to_owned(), "workspace-write".to_owned())]),
        };
        let effect_step = Step {
            id: "effect.write".to_owned(),
            operation: Operation::Effect {
                effect: "test.write".to_owned(),
                input: Expression::Input,
                occurrence: "once".to_owned(),
                bind: None,
            },
        };
        let source = plan(
            "source_effect",
            Vec::new(),
            vec![source_effect.clone()],
            vec![effect_step.clone()],
        );
        let mut constrained_effect = source_effect;
        constrained_effect
            .requirements
            .insert("tenant".to_owned(), "workspace".to_owned());
        let target = plan(
            "target_effect",
            Vec::new(),
            vec![constrained_effect],
            vec![effect_step],
        );
        validate_migration_no_widening(&source, &target)
            .expect("an additional target Effect requirement narrows provider admission");
        let mut deleted_effect = target.candidate.clone();
        deleted_effect.name = "target_deleted_effect_requirement".to_owned();
        deleted_effect.effects[0].requirements.remove("authority");
        let deleted_effect =
            seal_plan(deleted_effect).expect("deleted Effect requirement Plan seals");
        assert!(matches!(
            validate_migration_no_widening(&target, &deleted_effect),
            Err(EvolutionError::Conflict(message))
                if message.contains("Effect test.write widens")
        ));

        let mut duplicate_site_candidate = source.candidate.clone();
        duplicate_site_candidate.name = "target_duplicate_effect_site".to_owned();
        duplicate_site_candidate.definitions[0]
            .body
            .steps
            .push(Step {
                id: "effect.write-again".to_owned(),
                operation: Operation::Effect {
                    effect: "test.write".to_owned(),
                    input: Expression::Input,
                    occurrence: "again".to_owned(),
                    bind: None,
                },
            });
        let duplicate_site =
            seal_plan(duplicate_site_candidate).expect("duplicate Effect site Plan seals");
        assert!(matches!(
            validate_migration_no_widening(&source, &duplicate_site),
            Err(EvolutionError::Conflict(message))
                if message.contains("adds reachable Effect test.write")
        ));
        assert!(
            analyze_relink(&source, &duplicate_site)
                .expect("duplicate Effect site analyzes")
                .violations
                .iter()
                .any(|violation| matches!(
                    violation,
                    RelinkViolation::EffectAdded { effect }
                        if effect.starts_with("test.write@")
                )),
        );

        let mut target_candidate = source.candidate.clone();
        target_candidate.name = "target_effect_provider_property".to_owned();
        target_candidate.effects[0]
            .requirements
            .insert("authority".to_owned(), "organization-write".to_owned());
        let target = seal_plan(target_candidate).expect("target Effect Plan seals");
        assert!(matches!(
            validate_migration_no_widening(&source, &target),
            Err(EvolutionError::Conflict(message))
                if message.contains("Effect test.write widens")
        ));
    }
}
