use std::collections::BTreeMap;

use crate::{
    AgentError, AgentHost, AgentHostOccurrence, AgentHostOccurrenceState, AgentHostRequest,
    AgentHostResponse, AgentOccurrenceStore, AgentResult, NoopAgentJournal,
};

/// Provider-neutral durable boundary for individually identified agent interactions.
///
/// The caller owns its Agent or script loop and supplies a stable occurrence
/// identity for each interaction. Cymule owns only binding pinning, lifecycle
/// persistence, retained-response replay, and ambiguity recovery boundaries.
#[must_use]
pub struct AgentInteractionController<H, S = NoopAgentJournal> {
    host: H,
    store: S,
    session_id: String,
    occurrences: BTreeMap<String, AgentHostOccurrence>,
}

impl<H: AgentHost> AgentInteractionController<H, NoopAgentJournal> {
    /// Create a process-local interaction controller.
    pub fn new(session_id: impl Into<String>, host: H) -> AgentResult<Self> {
        Self::resume(session_id, host, NoopAgentJournal)
    }
}

impl<H: AgentHost, S: AgentOccurrenceStore> AgentInteractionController<H, S> {
    /// Reopen every retained occurrence for one Session.
    pub fn resume(session_id: impl Into<String>, host: H, mut store: S) -> AgentResult<Self> {
        let session_id = session_id.into();
        if session_id.is_empty() {
            return Err(AgentError::Validation(
                "interaction controller requires a Session identity".to_owned(),
            ));
        }
        let occurrences = store
            .load_occurrences(&session_id)?
            .into_iter()
            .map(|occurrence| (occurrence.occurrence_id.clone(), occurrence))
            .collect();
        Ok(Self {
            host,
            store,
            session_id,
            occurrences,
        })
    }

    /// Execute a newly identified interaction or return its retained response.
    ///
    /// Reusing `occurrence_id` with the same request is idempotent. A completed
    /// occurrence returns without binding or dispatching again. Prepared,
    /// started, unknown, or explicitly not-applied occurrences require caller
    /// recovery or a separately admitted replacement identity.
    pub fn execute(
        &mut self,
        occurrence_id: impl Into<String>,
        request: AgentHostRequest,
    ) -> AgentResult<AgentHostResponse> {
        let occurrence_id = occurrence_id.into();
        if occurrence_id.is_empty() {
            return Err(AgentError::Validation(
                "agent interaction requires an occurrence identity".to_owned(),
            ));
        }
        if let Some(existing) = self.occurrences.get(&occurrence_id) {
            if existing.request != request {
                return Err(AgentError::IllegalTransition(format!(
                    "host occurrence {occurrence_id} was reused with a different request"
                )));
            }
            return retained_response(existing);
        }

        let occurrence_binding = self.host.bind_occurrence(&request)?;
        let prepared = AgentHostOccurrence::prepare(
            &occurrence_id,
            &self.session_id,
            request.clone(),
            occurrence_binding,
        )?;
        self.store.record_occurrence(&prepared)?;
        self.occurrences
            .insert(occurrence_id.clone(), prepared.clone());

        let started = prepared.start()?;
        self.store.record_occurrence(&started)?;
        self.occurrences
            .insert(occurrence_id.clone(), started.clone());

        match dispatch(&mut self.host, request) {
            Ok(response) => {
                let completed = started.complete(response.clone())?;
                self.store.record_occurrence(&completed)?;
                self.occurrences.insert(occurrence_id, completed);
                Ok(response)
            }
            Err(error) => {
                let unknown = started.mark_unknown(error.to_string())?;
                self.store.record_occurrence(&unknown)?;
                self.occurrences.insert(occurrence_id, unknown);
                Err(error)
            }
        }
    }

    /// Inspect the latest retained lifecycle snapshot without invoking the host.
    pub fn occurrence(&self, occurrence_id: &str) -> Option<&AgentHostOccurrence> {
        self.occurrences.get(occurrence_id)
    }

    /// Consume the controller and return its host and occurrence store.
    pub fn into_parts(self) -> (H, S) {
        (self.host, self.store)
    }
}

fn retained_response(occurrence: &AgentHostOccurrence) -> AgentResult<AgentHostResponse> {
    match occurrence.state {
        AgentHostOccurrenceState::Completed => occurrence.response.clone().ok_or_else(|| {
            AgentError::Persistence(format!(
                "completed host occurrence {} has no retained response",
                occurrence.occurrence_id
            ))
        }),
        AgentHostOccurrenceState::NotApplied => Err(AgentError::RecoveryRequired(format!(
            "host occurrence {} is not_applied; admit a separate replacement identity or terminate the caller-owned loop",
            occurrence.occurrence_id
        ))),
        state => Err(AgentError::RecoveryRequired(format!(
            "host occurrence {} is {state:?}; reconcile or cancel it before reuse",
            occurrence.occurrence_id
        ))),
    }
}

fn dispatch<H: AgentHost>(
    host: &mut H,
    request: AgentHostRequest,
) -> AgentResult<AgentHostResponse> {
    match request {
        AgentHostRequest::Context(request) => {
            host.select_context(request).map(AgentHostResponse::Context)
        }
        AgentHostRequest::Model(request) => {
            host.invoke_model(request).map(AgentHostResponse::Model)
        }
        AgentHostRequest::Permission(request) => host
            .request_permission(request)
            .map(AgentHostResponse::Permission),
        AgentHostRequest::Tool(request) => host.invoke_tool(request).map(AgentHostResponse::Tool),
        AgentHostRequest::Elicitation(request) => {
            host.elicit(request).map(AgentHostResponse::Elicitation)
        }
        AgentHostRequest::Workspace(request) => host
            .apply_workspace(request)
            .map(AgentHostResponse::Workspace),
    }
}
