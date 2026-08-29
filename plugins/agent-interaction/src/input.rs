//! Typed Agent input commands over the closed persistence capability.

pub use cymule_profile_protocol::agent::{AgentInputCheckpoint, AgentInputResult};

use cymule_durable_protocol::WaitOwner;
use cymule_profile_protocol::agent::{
    AgentCommand, AgentCommandAction, AgentCommit, AgentElicitationQuery, AgentInputCommand,
    AgentSessionQuery, ElicitationRequest, ElicitationResponse,
};

use crate::{AgentError, AgentPersistence, AgentResult};

/// Two-phase constructor and committer for one atomic Agent/M1 input transition.
///
/// The caller retains the prepared [`AgentCommand`] until it receives an
/// [`AgentCommit`]. Retrying the exact command is idempotent; preparing again
/// after an unknown commit outcome would create a different source revision and
/// is therefore intentionally not a retry mechanism.
pub struct AgentInputController;

impl AgentInputController {
    /// Resolve one Session and request alias at a single revision, then prepare
    /// the exact command which attaches that request to an existing pending M1
    /// input Wait.
    ///
    /// # Errors
    ///
    /// Returns an error when the Session is absent, the revision changes, the
    /// request alias already exists, or the command is invalid.
    pub fn prepare_suspend<P: AgentPersistence + ?Sized>(
        persistence: &mut P,
        session_id: &str,
        wait_id: &str,
        expected_run_id: &str,
        expected_owner: &WaitOwner,
        request: ElicitationRequest,
    ) -> AgentResult<AgentCommand> {
        let revision = require_session_revision(persistence, session_id)?;
        let elicitation = persistence.read_agent_elicitation(&AgentElicitationQuery {
            session_id: session_id.to_owned(),
            request_id: request.request_id.clone(),
            expected_revision: Some(revision.clone()),
        })?;
        if elicitation.current.is_some() {
            return Err(AgentError::IllegalTransition(format!(
                "elicitation {} already exists in Session {session_id}",
                request.request_id
            )));
        }
        AgentCommand::new(
            revision,
            AgentCommandAction::Input(AgentInputCommand::Suspend {
                session_id: session_id.to_owned(),
                wait_id: wait_id.to_owned(),
                expected_run_id: expected_run_id.to_owned(),
                expected_owner: expected_owner.clone(),
                request,
            }),
        )
        .map_err(Into::into)
    }

    /// Resolve one Session and pending request at a single revision, then
    /// prepare the exact command which commits its response, M1 Wait result,
    /// Continuation advance, and Session projection in one CAS.
    ///
    /// # Errors
    ///
    /// Returns an error when the Session or pending elicitation is absent,
    /// stale, already complete, or owned by another Wait.
    pub fn prepare_complete<P: AgentPersistence + ?Sized>(
        persistence: &mut P,
        session_id: &str,
        wait_id: &str,
        expected_run_id: &str,
        expected_owner: &WaitOwner,
        response: ElicitationResponse,
    ) -> AgentResult<AgentCommand> {
        let revision = require_session_revision(persistence, session_id)?;
        let elicitation = persistence.read_agent_elicitation(&AgentElicitationQuery {
            session_id: session_id.to_owned(),
            request_id: response.request_id.clone(),
            expected_revision: Some(revision.clone()),
        })?;
        let current = elicitation.current.ok_or_else(|| {
            AgentError::NotFound(format!(
                "elicitation {} does not exist in Session {session_id}",
                response.request_id
            ))
        })?;
        if current.elicitation.wait_id != wait_id {
            return Err(AgentError::Validation(format!(
                "elicitation {} belongs to {}, not {wait_id}",
                response.request_id, current.elicitation.wait_id
            )));
        }
        if current.elicitation.response.is_some() {
            return Err(AgentError::IllegalTransition(format!(
                "elicitation {} is already complete; replay its retained command instead",
                response.request_id
            )));
        }
        AgentCommand::new(
            revision,
            AgentCommandAction::Input(AgentInputCommand::Complete {
                session_id: session_id.to_owned(),
                wait_id: wait_id.to_owned(),
                expected_run_id: expected_run_id.to_owned(),
                expected_owner: expected_owner.clone(),
                response,
            }),
        )
        .map_err(Into::into)
    }

    /// Commit or replay one previously prepared input command.
    ///
    /// # Errors
    ///
    /// Returns an error when the command is not input-owned or persistence
    /// cannot return a receipt which verifies against that exact command.
    pub fn commit<P: AgentPersistence + ?Sized>(
        persistence: &mut P,
        command: &AgentCommand,
    ) -> AgentResult<AgentCommit> {
        if !matches!(command.action, AgentCommandAction::Input(_)) {
            return Err(AgentError::Validation(
                "AgentInputController accepts only Agent input commands".to_owned(),
            ));
        }
        let commit = persistence.commit_agent(command)?;
        commit.verify_for(command)?;
        Ok(commit)
    }
}

fn require_session_revision<P: AgentPersistence + ?Sized>(
    persistence: &mut P,
    session_id: &str,
) -> AgentResult<String> {
    let read = persistence.read_agent_session(&AgentSessionQuery {
        session_id: session_id.to_owned(),
        expected_revision: None,
    })?;
    if read.current.is_none() {
        return Err(AgentError::NotFound(format!(
            "Session {session_id} does not exist"
        )));
    }
    Ok(read.revision)
}
