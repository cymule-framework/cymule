use serde::{Deserialize, Serialize};

use crate::{
    EvolutionCommand, EvolutionError, EvolutionResult, LinkedPlan, LivePublicationCommand,
    LivePublicationReceipt, MigrationReceipt, MigrationSafePoint, PlanEdge, PlanTemplate,
    RestartReceipt, RolloutTransition, ShadowComparison, SubflowRevision,
};

/// Complete cross-language live-evolution control version.
pub const LIVE_EVOLUTION_CONTROL_VERSION: &str = "cymule.live-evolution-control/1";

/// Closed commands for the unified registry, DAG, rollout, and pin authority.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "operation", rename_all = "snake_case", deny_unknown_fields)]
pub enum LiveEvolutionCommand {
    /// Publish one reusable definition before or between parent registrations.
    PublishDefinition {
        /// Control protocol version.
        control_version: String,
        /// Stable command/checkpoint identity.
        command_id: String,
        /// Logical reusable-definition reference.
        logical_ref: String,
        /// Immutable definition content.
        definition: cymule_core::Definition,
    },
    /// Register one parent template and its initial future decision.
    RegisterTemplate {
        /// Control protocol version.
        control_version: String,
        /// Stable command/checkpoint identity.
        command_id: String,
        /// Unsealed parent source and exact logical references.
        template: PlanTemplate,
    },
    /// Publish a revision and atomically relink every compatible dependent.
    PublishAndRelink {
        /// Control protocol version.
        control_version: String,
        /// Stable command/checkpoint identity.
        command_id: String,
        /// Exact publication semantics.
        publication: LivePublicationCommand,
    },
    /// Apply one template-scoped DAG, rollout, migration, shadow, or pin command.
    Apply {
        /// Control protocol version.
        control_version: String,
        /// Stable unified command/checkpoint identity.
        command_id: String,
        /// Registered parent template.
        template_id: String,
        /// Existing closed evolution operation.
        command: Box<EvolutionCommand>,
        /// Required durable proof for migration or replacement-Run restart.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        safe_point: Option<MigrationSafePoint>,
    },
}

/// Typed result union returned by the unified durable controller.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "result", rename_all = "snake_case", deny_unknown_fields)]
pub enum LiveEvolutionResponse {
    /// One immutable definition revision was published.
    DefinitionPublished {
        /// Published revision.
        revision: SubflowRevision,
    },
    /// One parent template and initial Plan were registered.
    TemplateRegistered {
        /// Initial exact linked Plan.
        linked: LinkedPlan,
    },
    /// One publication atomically updated every compatible dependent.
    PublicationApplied {
        /// Original idempotent publication receipt.
        receipt: LivePublicationReceipt,
    },
    /// One reviewed Plan edge was admitted.
    PatchApplied {
        /// Immutable DAG edge.
        edge: PlanEdge,
    },
    /// One future decision or observation was stored.
    Applied,
    /// One occurrence received an immutable Plan pin.
    OccurrenceSelected {
        /// Exact selected Plan.
        plan_id: String,
    },
    /// One checked migration completed.
    Migrated {
        /// Migration receipt.
        receipt: MigrationReceipt,
    },
    /// One replacement Run was authorized.
    RestartAuthorized {
        /// Restart receipt.
        receipt: RestartReceipt,
    },
    /// One isolated shadow comparison completed.
    ShadowRecorded {
        /// Shadow evidence.
        comparison: ShadowComparison,
    },
    /// One deterministic gate changed future selection.
    GateApplied {
        /// Promotion or rollback transition.
        transition: RolloutTransition,
    },
}

impl LiveEvolutionCommand {
    /// Validate the complete transport envelope before stateful admission.
    pub fn verify(&self) -> EvolutionResult<()> {
        let (control_version, command_id) = match self {
            Self::PublishDefinition {
                control_version,
                command_id,
                logical_ref,
                definition,
            } => {
                validate_identity("definition reference", logical_ref)?;
                validate_identity("definition", &definition.id)?;
                (control_version, command_id)
            }
            Self::RegisterTemplate {
                control_version,
                command_id,
                template,
            } => {
                validate_identity("template", &template.template_id)?;
                (control_version, command_id)
            }
            Self::PublishAndRelink {
                control_version,
                command_id,
                publication,
            } => {
                validate_identity("definition reference", &publication.logical_ref)?;
                validate_identity("definition", &publication.definition.id)?;
                validate_artifact(&publication.evidence)?;
                (control_version, command_id)
            }
            Self::Apply {
                control_version,
                command_id,
                template_id,
                command,
                safe_point,
            } => {
                validate_identity("template", template_id)?;
                command.verify()?;
                let requires_safe_point = matches!(
                    command.as_ref(),
                    EvolutionCommand::Migrate { .. } | EvolutionCommand::RestartUnderNewPlan { .. }
                );
                match (requires_safe_point, safe_point) {
                    (true, Some(proof)) => {
                        proof.verify()?;
                        let matches_request = match command.as_ref() {
                            EvolutionCommand::Migrate { request, .. } => {
                                request.safe_point_id == proof.safe_point_id
                                    && request.run_id == proof.run_id
                                    && request.from_plan == proof.plan_id
                                    && request.source_epoch == proof.epoch
                                    && proof.state.as_ref() == Some(&request.input_state)
                            }
                            EvolutionCommand::RestartUnderNewPlan { request, .. } => {
                                request.safe_point_id == proof.safe_point_id
                                    && request.source_run == proof.run_id
                                    && request.from_plan == proof.plan_id
                                    && request.source_epoch == proof.epoch
                            }
                            _ => false,
                        };
                        if !matches_request {
                            return Err(EvolutionError::Validation(
                                "safe-point proof does not match its migration or restart request"
                                    .to_owned(),
                            ));
                        }
                    }
                    (true, None) => {
                        return Err(EvolutionError::Validation(
                            "migration and restart commands require a safe-point proof".to_owned(),
                        ));
                    }
                    (false, Some(_)) => {
                        return Err(EvolutionError::Validation(
                            "only migration and restart commands accept a safe-point proof"
                                .to_owned(),
                        ));
                    }
                    (false, None) => {}
                }
                (control_version, command_id)
            }
        };
        if control_version != LIVE_EVOLUTION_CONTROL_VERSION {
            return Err(EvolutionError::Validation(format!(
                "unsupported live-evolution control version {control_version}"
            )));
        }
        validate_identity("live-evolution command", command_id)
    }
}

fn validate_identity(kind: &str, value: &str) -> EvolutionResult<()> {
    if value.is_empty() || value.len() > 256 || value.chars().any(char::is_control) {
        return Err(EvolutionError::Validation(format!(
            "{kind} identity must contain 1..=256 printable characters"
        )));
    }
    Ok(())
}

fn validate_artifact(artifact: &cymule_core::ArtifactRef) -> EvolutionResult<()> {
    let digest = artifact
        .artifact_id
        .strip_prefix("sha256:")
        .ok_or_else(|| {
            EvolutionError::Validation("publication evidence must be content-addressed".to_owned())
        })?;
    if digest.len() != 64
        || !digest.bytes().all(|byte| byte.is_ascii_hexdigit())
        || artifact.kind.is_empty()
    {
        return Err(EvolutionError::Validation(
            "publication evidence Artifact is malformed".to_owned(),
        ));
    }
    Ok(())
}
