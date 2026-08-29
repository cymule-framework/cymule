use serde::{Deserialize, Serialize};

use cymule_core::ArtifactRef;

use super::{
    EvolutionError, EvolutionResult, MigrationRequest, PlanPatch, RestartRequest, RolloutDecision,
    RolloutGate, RolloutObservation, ShadowRequest,
};

/// Frozen cross-language M4 control envelope version.
pub const EVOLUTION_CONTROL_VERSION: &str = "cymule.evolution-control/5";

/// Closed idempotent M4 commands shared by every SDK.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "operation", rename_all = "snake_case", deny_unknown_fields)]
pub enum EvolutionCommand {
    /// Seal and admit one exact reviewed Plan patch.
    ApplyPatch {
        /// Control protocol version.
        control_version: String,
        /// Stable command/idempotency identity.
        command_id: String,
        /// Reviewed patch candidate.
        patch: PlanPatch,
    },
    /// Change future rollout selection.
    SetRollout {
        /// Control protocol version.
        control_version: String,
        /// Stable command/idempotency identity.
        command_id: String,
        /// Immutable decision.
        decision: RolloutDecision,
    },
    /// Select and pin an exact Plan for one occurrence.
    SelectOccurrence {
        /// Control protocol version.
        control_version: String,
        /// Stable command/idempotency identity.
        command_id: String,
        /// Stable occurrence identity.
        occurrence_id: String,
        /// Stable identity used by deterministic future selection.
        selection_id: String,
        /// Exact already-admitted `ExecutionBinding` Artifact selected for this occurrence.
        execution_binding: ArtifactRef,
    },
    /// Execute a checked safe-point migration through a pinned plugin.
    Migrate {
        /// Control protocol version.
        control_version: String,
        /// Stable command/idempotency identity.
        command_id: String,
        /// Migration request.
        request: Box<MigrationRequest>,
    },
    /// Authorize a replacement Run under a different exact Plan.
    RestartUnderNewPlan {
        /// Control protocol version.
        control_version: String,
        /// Stable command/idempotency identity.
        command_id: String,
        /// Restart authorization request.
        request: Box<RestartRequest>,
    },
    /// Execute isolated non-authoritative shadow work through a pinned plugin.
    Shadow {
        /// Control protocol version.
        control_version: String,
        /// Stable command/idempotency identity.
        command_id: String,
        /// Shadow request.
        request: ShadowRequest,
    },
    /// Record one terminal rollout occurrence observation.
    Observe {
        /// Control protocol version.
        control_version: String,
        /// Stable command/idempotency identity.
        command_id: String,
        /// Immutable observation.
        observation: RolloutObservation,
    },
    /// Apply one ready deterministic promotion or rollback gate.
    ApplyGate {
        /// Control protocol version.
        control_version: String,
        /// Stable command/idempotency identity.
        command_id: String,
        /// Evidence gate.
        gate: RolloutGate,
        /// Identity for the resulting future-selection decision.
        next_decision_id: String,
    },
}

impl EvolutionCommand {
    /// Validate the closed transport envelope before stateful admission.
    ///
    /// # Errors
    ///
    /// Returns an error when the generation, identity, request, evidence, or
    /// exact integer fields are malformed.
    pub fn verify(&self) -> EvolutionResult<()> {
        let (control_version, command_id) = match self {
            Self::ApplyPatch {
                control_version,
                command_id,
                patch,
            } => {
                super::live_control::verify_plan_patch(patch)?;
                (control_version, command_id)
            }
            Self::SetRollout {
                control_version,
                command_id,
                decision,
            } => {
                super::live_control::verify_rollout_decision(decision)?;
                (control_version, command_id)
            }
            Self::SelectOccurrence {
                control_version,
                command_id,
                occurrence_id,
                selection_id,
                execution_binding,
            } => {
                validate_identity("occurrence", occurrence_id)?;
                validate_identity("occurrence selection", selection_id)?;
                execution_binding
                    .validate()
                    .map_err(|error| EvolutionError::Validation(error.to_string()))?;
                if execution_binding.kind != cymule_runtime::EXECUTION_BINDING_VERSION {
                    return Err(EvolutionError::Validation(
                        "occurrence binding must reference an exact ExecutionBinding Artifact"
                            .to_owned(),
                    ));
                }
                (control_version, command_id)
            }
            Self::Migrate {
                control_version,
                command_id,
                request,
            } => {
                request.verify()?;
                (control_version, command_id)
            }
            Self::RestartUnderNewPlan {
                control_version,
                command_id,
                request,
            } => {
                super::live_control::verify_restart_request(request)?;
                (control_version, command_id)
            }
            Self::Shadow {
                control_version,
                command_id,
                request,
            } => {
                request.verify()?;
                (control_version, command_id)
            }
            Self::Observe {
                control_version,
                command_id,
                observation,
            } => {
                super::live_control::verify_rollout_observation(observation)?;
                (control_version, command_id)
            }
            Self::ApplyGate {
                control_version,
                command_id,
                gate,
                next_decision_id,
            } => {
                super::live_control::verify_rollout_gate(gate)?;
                validate_identity("rollout decision", next_decision_id)?;
                (control_version, command_id)
            }
        };
        if control_version != EVOLUTION_CONTROL_VERSION {
            return Err(EvolutionError::Validation(format!(
                "unsupported evolution control version {control_version}"
            )));
        }
        validate_identity("evolution command", command_id)
    }
}

pub(crate) fn validate_identity(kind: &str, value: &str) -> EvolutionResult<()> {
    let scalar_count = value.chars().count();
    if !(1..=256).contains(&scalar_count) || value.chars().any(char::is_control) {
        return Err(EvolutionError::Validation(format!(
            "{kind} identity must contain 1..=256 non-control Unicode scalars"
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use cymule_core::{Definition, Expression, PlanCandidate, Region, content_id};
    use serde_json::json;

    use super::*;

    fn target_candidate() -> PlanCandidate {
        PlanCandidate {
            ir_version: cymule_core::IR_VERSION.to_owned(),
            name: "command-shape-target".to_owned(),
            entry: "main".to_owned(),
            components: Vec::new(),
            effects: Vec::new(),
            definitions: vec![Definition {
                id: "main".to_owned(),
                input_schema: json!({}),
                output_schema: json!({}),
                body: Region {
                    steps: Vec::new(),
                    result: Expression::Literal { value: json!(null) },
                },
            }],
            metadata: BTreeMap::new(),
        }
    }

    #[test]
    fn command_shape_fails_before_any_state_dependent_admission() {
        let target = target_candidate();
        let source_plan = content_id("cymule.test-source-plan/1", &()).unwrap();
        let evidence = cymule_core::artifact_ref("cymule.test-review/1", b"review").unwrap();
        let empty_patch = EvolutionCommand::ApplyPatch {
            control_version: EVOLUTION_CONTROL_VERSION.to_owned(),
            command_id: "patch-empty".to_owned(),
            patch: super::super::PlanPatch {
                from_plan: source_plan,
                target,
                operations: Vec::new(),
                evidence,
            },
        };
        assert!(empty_patch.verify().is_err());

        let malformed_rollout = EvolutionCommand::SetRollout {
            control_version: EVOLUTION_CONTROL_VERSION.to_owned(),
            command_id: "rollout-malformed".to_owned(),
            decision: super::super::RolloutDecision {
                decision_id: "decision-1".to_owned(),
                fallback_plan: "not-a-content-id".to_owned(),
                target_plan: content_id("cymule.test-target-plan/1", &()).unwrap(),
                mode: super::super::RolloutMode::Active,
            },
        };
        assert!(malformed_rollout.verify().is_err());

        let malformed_observation = EvolutionCommand::Observe {
            control_version: EVOLUTION_CONTROL_VERSION.to_owned(),
            command_id: "observation-malformed".to_owned(),
            observation: super::super::RolloutObservation {
                observation_id: "observation-1".to_owned(),
                decision_id: "decision-1".to_owned(),
                occurrence_id: "occurrence-1".to_owned(),
                plan_id: "not-a-content-id".to_owned(),
                outcome: super::super::ObservationOutcome::Succeeded,
                evidence: cymule_core::artifact_ref("cymule.test-observation/1", b"observation")
                    .unwrap(),
            },
        };
        assert!(malformed_observation.verify().is_err());

        let oversized_gate = EvolutionCommand::ApplyGate {
            control_version: EVOLUTION_CONTROL_VERSION.to_owned(),
            command_id: "gate-oversized".to_owned(),
            gate: super::super::RolloutGate {
                gate_id: "gate-1".to_owned(),
                decision_id: "decision-1".to_owned(),
                min_target_observations: cymule_core::MAX_EXACT_INTEGER + 1,
                max_target_failures: 0,
                min_equivalent_shadows: 0,
                max_inequivalent_shadows: 0,
            },
            next_decision_id: "decision-2".to_owned(),
        };
        assert!(oversized_gate.verify().is_err());
    }
}
