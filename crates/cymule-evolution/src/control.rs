use serde::{Deserialize, Serialize};

use crate::{
    EvolutionError, EvolutionResult, MigrationRequest, PlanPatch, RestartRequest, RolloutDecision,
    RolloutGate, RolloutObservation, ShadowRequest,
};

/// Frozen cross-language M4 control envelope version.
pub const EVOLUTION_CONTROL_VERSION: &str = "cymule.evolution-control/2";

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
    },
    /// Execute a checked safe-point migration through a pinned plugin.
    Migrate {
        /// Control protocol version.
        control_version: String,
        /// Stable command/idempotency identity.
        command_id: String,
        /// Migration request.
        request: MigrationRequest,
    },
    /// Authorize a replacement Run under a different exact Plan.
    RestartUnderNewPlan {
        /// Control protocol version.
        control_version: String,
        /// Stable command/idempotency identity.
        command_id: String,
        /// Restart authorization request.
        request: RestartRequest,
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
    pub fn verify(&self) -> EvolutionResult<()> {
        let (control_version, command_id) = match self {
            Self::ApplyPatch {
                control_version,
                command_id,
                patch,
            } => {
                validate_identity("parent Plan", &patch.from_plan)?;
                (control_version, command_id)
            }
            Self::SetRollout {
                control_version,
                command_id,
                decision,
            } => {
                validate_identity("rollout decision", &decision.decision_id)?;
                (control_version, command_id)
            }
            Self::SelectOccurrence {
                control_version,
                command_id,
                occurrence_id,
            } => {
                validate_identity("occurrence", occurrence_id)?;
                (control_version, command_id)
            }
            Self::Migrate {
                control_version,
                command_id,
                request,
            } => {
                validate_identity("migration", &request.migration_id)?;
                (control_version, command_id)
            }
            Self::RestartUnderNewPlan {
                control_version,
                command_id,
                request,
            } => {
                validate_identity("restart", &request.restart_id)?;
                (control_version, command_id)
            }
            Self::Shadow {
                control_version,
                command_id,
                request,
            } => {
                validate_identity("shadow comparison", &request.comparison_id)?;
                (control_version, command_id)
            }
            Self::Observe {
                control_version,
                command_id,
                observation,
            } => {
                validate_identity("rollout observation", &observation.observation_id)?;
                (control_version, command_id)
            }
            Self::ApplyGate {
                control_version,
                command_id,
                gate,
                next_decision_id,
            } => {
                validate_identity("rollout gate", &gate.gate_id)?;
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

fn validate_identity(kind: &str, value: &str) -> EvolutionResult<()> {
    if value.is_empty() || value.len() > 256 {
        return Err(EvolutionError::Validation(format!(
            "{kind} identity must contain 1..=256 characters"
        )));
    }
    Ok(())
}
