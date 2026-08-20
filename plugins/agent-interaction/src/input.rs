use cymule_core::{ArtifactRef, canonical_digest};
use cymule_durable::{DurableCoordinator, DurableStore, WaitCondition, WaitKind, WaitState};

use crate::{
    AgentError, AgentJournal, AgentResult, AgentSession, AgentState, AgentUpdate,
    ElicitationProjection, ElicitationRequest, ElicitationResponse, journal::agent_update_record,
};

/// Result of one atomic durable input suspension or completion checkpoint.
#[derive(Debug, Clone, PartialEq)]
pub struct AgentInputCheckpoint {
    /// Replayed Session projection after the checkpoint.
    pub session: AgentSession,
    /// M1 wait identity correlated with the input request.
    pub wait_id: String,
    /// Newly committed semantic revision behind the segmented head CAS.
    pub revision: String,
}

/// M2 durable typed-input operations over the shared M1 CAS authority.
pub struct AgentInputController;

impl AgentInputController {
    /// Atomically project `RequiresAction` and register an M1 input wait.
    pub fn suspend<S: DurableStore>(
        coordinator: &mut DurableCoordinator<S>,
        session_id: &str,
        run_id: &str,
        request: ElicitationRequest,
    ) -> AgentResult<AgentInputCheckpoint> {
        if session_id.is_empty() || run_id.is_empty() || request.request_id.is_empty() {
            return Err(AgentError::Validation(
                "Session, Run, and elicitation identities must not be empty".to_owned(),
            ));
        }
        compile_input_schema(&request)?;
        let digest = canonical_digest(&(session_id, run_id, &request))
            .map_err(|error| AgentError::Validation(error.to_string()))?;
        let wait_id = format!("wait:agent-input:{digest}");
        let update_base = format!("update:agent-input:{digest}");
        let updates = [
            AgentUpdate::Elicitation {
                update_id: format!("{update_base}:pending"),
                elicitation: ElicitationProjection {
                    wait_id: wait_id.clone(),
                    request: request.clone(),
                    response: None,
                },
            },
            AgentUpdate::State {
                update_id: format!("{update_base}:requires-action"),
                state: AgentState::RequiresAction,
                stop_reason: None,
            },
        ];
        let mut session = load_session(coordinator, session_id)?;
        for update in &updates {
            session.apply(update.clone())?;
        }
        let records = updates
            .iter()
            .map(agent_update_record)
            .collect::<AgentResult<Vec<_>>>()?;
        let revision = coordinator
            .checkpoint_journal_wait(
                session_id,
                &records,
                &WaitCondition {
                    wait_id: wait_id.clone(),
                    run_id: run_id.to_owned(),
                    kind: WaitKind::Input {
                        correlation: request.request_id,
                        schema: request.schema,
                    },
                    consume_once: true,
                    state: WaitState::Pending,
                    result: None,
                },
            )
            .map_err(|error| AgentError::Persistence(error.to_string()))?;
        Ok(AgentInputCheckpoint {
            session,
            wait_id,
            revision,
        })
    }

    /// Atomically resolve an M1 input wait and advance the Session projection.
    pub fn complete<S: DurableStore>(
        coordinator: &mut DurableCoordinator<S>,
        session_id: &str,
        wait_id: &str,
        result: ArtifactRef,
        response: ElicitationResponse,
    ) -> AgentResult<AgentInputCheckpoint> {
        let mut session = load_session(coordinator, session_id)?;
        let current = session
            .elicitations
            .get(&response.request_id)
            .cloned()
            .ok_or_else(|| {
                AgentError::NotFound(format!(
                    "elicitation {} does not exist",
                    response.request_id
                ))
            })?;
        if current.wait_id != wait_id {
            return Err(AgentError::Validation(format!(
                "elicitation {} is correlated with {}, not {wait_id}",
                response.request_id, current.wait_id
            )));
        }
        if let Some(existing) = &current.response {
            if existing != &response {
                return Err(AgentError::IllegalTransition(format!(
                    "elicitation {} was already completed with different content",
                    response.request_id
                )));
            }
            let wait = coordinator
                .state()
                .map_err(|error| AgentError::Persistence(error.to_string()))?
                .waits
                .get(wait_id)
                .ok_or_else(|| AgentError::NotFound(format!("wait {wait_id} does not exist")))?;
            if wait.state != WaitState::Completed || wait.result.as_ref() != Some(&result) {
                return Err(AgentError::Persistence(format!(
                    "completed elicitation {} is inconsistent with wait {wait_id}",
                    response.request_id
                )));
            }
            let revision = coordinator.revision().ok_or_else(|| {
                AgentError::Persistence("durable state is not initialized".to_owned())
            })?;
            return Ok(AgentInputCheckpoint {
                session,
                wait_id: wait_id.to_owned(),
                revision: revision.to_owned(),
            });
        }
        validate_input_completion(wait_id, &current.request, &response)?;
        let update_base = wait_id.replacen("wait:", "update:", 1);
        let elicitation_update = AgentUpdate::Elicitation {
            update_id: format!("{update_base}:completed"),
            elicitation: ElicitationProjection {
                wait_id: wait_id.to_owned(),
                request: current.request,
                response: Some(response),
            },
        };
        session.apply(elicitation_update.clone())?;
        let state = if session
            .elicitations
            .values()
            .any(|elicitation| elicitation.response.is_none())
        {
            AgentState::RequiresAction
        } else {
            AgentState::Running
        };
        let state_update = AgentUpdate::State {
            update_id: format!("{update_base}:{}", state_update_suffix(state)),
            state,
            stop_reason: None,
        };
        session.apply(state_update.clone())?;
        let updates = [elicitation_update, state_update];
        let records = updates
            .iter()
            .map(agent_update_record)
            .collect::<AgentResult<Vec<_>>>()?;
        let revision = coordinator
            .checkpoint_journal_wait_completion(session_id, &records, wait_id, result)
            .map_err(|error| AgentError::Persistence(error.to_string()))?;
        Ok(AgentInputCheckpoint {
            session,
            wait_id: wait_id.to_owned(),
            revision,
        })
    }
}

fn compile_input_schema(request: &ElicitationRequest) -> AgentResult<jsonschema::Validator> {
    jsonschema::draft202012::options()
        .build(&request.schema)
        .map_err(|error| {
            AgentError::Validation(format!(
                "elicitation {} has an invalid Draft 2020-12 schema: {error}",
                request.request_id
            ))
        })
}

fn validate_input_completion(
    wait_id: &str,
    request: &ElicitationRequest,
    response: &ElicitationResponse,
) -> AgentResult<()> {
    ElicitationProjection {
        wait_id: wait_id.to_owned(),
        request: request.clone(),
        response: Some(response.clone()),
    }
    .validate()?;
    let Some(value) = response.value.as_ref() else {
        return Ok(());
    };
    let validator = compile_input_schema(request)?;
    validator.validate(value).map_err(|error| {
        AgentError::Validation(format!(
            "elicitation {} value at {} does not satisfy schema at {}: {error}",
            request.request_id,
            error.instance_path(),
            error.schema_path()
        ))
    })
}

const fn state_update_suffix(state: AgentState) -> &'static str {
    match state {
        AgentState::RequiresAction => "requires-action",
        AgentState::Running => "running",
        AgentState::Idle => "idle",
        AgentState::Closed => "closed",
    }
}

fn load_session<S: DurableStore>(
    coordinator: &mut DurableCoordinator<S>,
    session_id: &str,
) -> AgentResult<AgentSession> {
    let updates = AgentJournal::load(coordinator, session_id)?;
    AgentSession::replay(session_id, updates)
}
