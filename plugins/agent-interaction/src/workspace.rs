//! Typed workspace Effect/abort orchestration.

pub use cymule_profile_protocol::agent::{WorkspaceScopeCheckpoint, WorkspaceScopeRequest};

use cymule_profile_protocol::agent::{
    AgentCommand, AgentCommandAction, AgentHostOccurrenceState, AgentHostRequest,
    AgentOccurrenceQuery, AgentOccurrenceRead, AgentWorkspaceAdmissionQuery, AgentWorkspaceCommand,
    AgentWorkspaceCommitOutcome, AgentWorkspaceDecision,
};

use crate::{AgentError, AgentPersistence, AgentResult};

/// Closed workspace controller over the framework-owned M1/provider authority.
///
/// Prepared commands contain only semantic intent and the M1 structural
/// bindings returned by a revision-pinned read. Host bindings, provider
/// responses, and reconciliation evidence never become caller-supplied command
/// input: [`commit`](Self::commit) hands every phase to the specialized
/// persistence capability which owns the binding-keyed provider registry.
pub struct WorkspaceScopeController;

impl WorkspaceScopeController {
    /// Prepare the semantic command which starts one admitted mutating Effect.
    ///
    /// # Errors
    ///
    /// Returns an error when the request lacks its framework-issued dispatch
    /// lease or M1 cannot resolve an exact revision-pinned Effect admission and
    /// binding.
    pub fn prepare_start_effect<P: AgentPersistence + ?Sized>(
        persistence: &mut P,
        request: &WorkspaceScopeRequest,
    ) -> AgentResult<AgentCommand> {
        Self::prepare_start(persistence, request, AgentWorkspaceDecision::Commit)
    }

    /// Prepare the semantic command which starts one admitted abort operation.
    ///
    /// # Errors
    ///
    /// Returns an error when the request carries an Effect dispatch lease or M1
    /// cannot resolve an exact revision-pinned abort admission and binding.
    pub fn prepare_start_abort<P: AgentPersistence + ?Sized>(
        persistence: &mut P,
        request: &WorkspaceScopeRequest,
    ) -> AgentResult<AgentCommand> {
        Self::prepare_start(persistence, request, AgentWorkspaceDecision::Abort)
    }

    fn prepare_start<P: AgentPersistence + ?Sized>(
        persistence: &mut P,
        request: &WorkspaceScopeRequest,
        decision: AgentWorkspaceDecision,
    ) -> AgentResult<AgentCommand> {
        let query = AgentWorkspaceAdmissionQuery {
            request: request.clone(),
            decision,
            expected_revision: None,
        };
        let admission = persistence.read_agent_workspace_admission(&query)?;
        admission.verify_for(&query)?;
        let action = match decision {
            AgentWorkspaceDecision::Commit => {
                let effect_intent_id = admission
                    .host_request
                    .m1_owner()
                    .and_then(|owner| owner.effect_intent_id.clone())
                    .ok_or_else(|| {
                        AgentError::persistence(
                            "agent_workspace_effect_intent_missing",
                            "workspace Effect admission lost its structural intent",
                        )
                    })?;
                AgentWorkspaceCommand::StartEffect {
                    request: request.clone(),
                    effect_intent_id,
                    execution_binding: admission.execution_binding,
                    operation_occurrence_binding: admission.operation_occurrence_binding,
                }
            }
            AgentWorkspaceDecision::Abort => AgentWorkspaceCommand::StartAbort {
                request: request.clone(),
                execution_binding: admission.execution_binding,
                operation_occurrence_binding: admission.operation_occurrence_binding,
            },
        };
        AgentCommand::new(
            admission.revision,
            AgentCommandAction::Workspace(Box::new(action)),
        )
        .map_err(Into::into)
    }

    /// Prepare settlement of the binding-pinned commit occurrence at its exact
    /// currently observed revision. The provider result is resolved later by
    /// the specialized persistence capability and is never accepted here.
    ///
    /// # Errors
    ///
    /// Returns an error when the request carries a new dispatch lease, the
    /// retained occurrence is missing, not started or unknown, or does not
    /// match the complete Effect owner.
    pub fn prepare_settle_effect<P: AgentPersistence + ?Sized>(
        persistence: &mut P,
        request: &WorkspaceScopeRequest,
    ) -> AgentResult<AgentCommand> {
        Self::prepare_settlement(persistence, request, AgentWorkspaceDecision::Commit)
    }

    /// Prepare settlement of the binding-pinned abort occurrence at its exact
    /// currently observed revision.
    ///
    /// # Errors
    ///
    /// Returns an error when the request carries a dispatch lease, the retained
    /// occurrence is missing, not started or unknown, or does not match the
    /// complete abort owner.
    pub fn prepare_settle_abort<P: AgentPersistence + ?Sized>(
        persistence: &mut P,
        request: &WorkspaceScopeRequest,
    ) -> AgentResult<AgentCommand> {
        Self::prepare_settlement(persistence, request, AgentWorkspaceDecision::Abort)
    }

    fn prepare_settlement<P: AgentPersistence + ?Sized>(
        persistence: &mut P,
        request: &WorkspaceScopeRequest,
        decision: AgentWorkspaceDecision,
    ) -> AgentResult<AgentCommand> {
        request.verify()?;
        if request.dispatch_lease.is_some() {
            return Err(AgentError::Validation(
                "workspace settlement cannot carry a new dispatch lease".to_owned(),
            ));
        }
        let query = AgentOccurrenceQuery {
            session_id: request.session_id.clone(),
            occurrence_id: request.occurrence_id.clone(),
            expected_revision: None,
        };
        let read = Self::load(persistence, &query)?;
        let current = read.current.ok_or_else(|| {
            AgentError::NotFound(format!(
                "workspace occurrence {} does not exist",
                request.occurrence_id
            ))
        })?;
        if !matches!(
            current.occurrence.state,
            AgentHostOccurrenceState::Started | AgentHostOccurrenceState::Unknown
        ) {
            return Err(AgentError::IllegalTransition(format!(
                "workspace occurrence {} cannot settle from {:?}",
                request.occurrence_id, current.occurrence.state
            )));
        }
        let AgentHostRequest::Workspace(host_request) = &current.occurrence.request else {
            return Err(AgentError::Validation(
                "workspace settlement requires a typed M1 occurrence".to_owned(),
            ));
        };
        let owner = host_request.m1_owner().ok_or_else(|| {
            AgentError::Validation(
                "workspace settlement occurrence lost its complete M1 owner".to_owned(),
            )
        })?;
        if current.occurrence.session_id != request.session_id
            || current.occurrence.occurrence_id != request.occurrence_id
            || host_request.change() != &request.change(decision.commit())
            || owner.run_id != request.run_id
            || owner.scope_id != request.scope_id
            || owner.invocation_id != request.invocation_id
            || owner.site_id != request.site_id
            || owner.occurrence_key != request.occurrence_key
            || owner.operation != request.operation
            || owner.effect_intent_id.is_some() != decision.commit()
        {
            return Err(AgentError::Validation(
                "workspace settlement request changed its exact admitted owner".to_owned(),
            ));
        }
        let workspace = match decision {
            AgentWorkspaceDecision::Commit => AgentWorkspaceCommand::SettleEffect {
                request: request.clone(),
            },
            AgentWorkspaceDecision::Abort => AgentWorkspaceCommand::SettleAbort {
                request: request.clone(),
            },
        };
        AgentCommand::new(
            read.revision,
            AgentCommandAction::Workspace(Box::new(workspace)),
        )
        .map_err(Into::into)
    }

    /// Commit or replay one workspace phase through the binding-keyed provider
    /// registry. Generic Agent commit is intentionally not used.
    ///
    /// # Errors
    ///
    /// Returns an error when the command is not workspace-owned or the
    /// specialized provider/M1 commit cannot return an exact verified receipt.
    pub fn commit<P: AgentPersistence + ?Sized>(
        persistence: &mut P,
        command: &AgentCommand,
    ) -> AgentResult<AgentWorkspaceCommitOutcome> {
        if !matches!(command.action, AgentCommandAction::Workspace(_)) {
            return Err(AgentError::Validation(
                "WorkspaceScopeController accepts only Agent workspace commands".to_owned(),
            ));
        }
        let outcome = persistence.commit_agent_workspace(command)?;
        outcome.verify_for(command)?;
        Ok(outcome)
    }

    /// Read one exact workspace occurrence at an optional pinned revision.
    ///
    /// # Errors
    ///
    /// Returns an error when the query is invalid or stale, the read cannot be
    /// verified, or the occurrence is not owned by an M1 workspace operation.
    pub fn load<P: AgentPersistence + ?Sized>(
        persistence: &mut P,
        query: &AgentOccurrenceQuery,
    ) -> AgentResult<AgentOccurrenceRead> {
        let read = persistence.read_agent_occurrence(query)?;
        read.verify_for(query)?;
        if let Some(current) = &read.current
            && !current.occurrence.request.is_m1_workspace()
        {
            return Err(AgentError::Validation(format!(
                "occurrence {} is not owned by an M1 workspace scope",
                current.occurrence.occurrence_id
            )));
        }
        Ok(read)
    }
}
