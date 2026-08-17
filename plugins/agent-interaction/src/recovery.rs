use crate::{
    AgentError, AgentHost, AgentHostOccurrence, AgentHostOccurrenceState,
    AgentOccurrenceResolution, AgentOccurrenceStore, AgentResult, ContentBlock,
};

/// Explicit recovery operations for prepared, started, or unknown host calls.
pub struct AgentRecoveryController;

impl AgentRecoveryController {
    /// Query the original host binding and persist an authoritative resolution.
    pub fn reconcile<H: AgentHost, J: AgentOccurrenceStore>(
        host: &mut H,
        store: &mut J,
        session_id: &str,
        occurrence_id: &str,
    ) -> AgentResult<AgentHostOccurrence> {
        let current = load_occurrence(store, session_id, occurrence_id)?;
        if current.is_terminal() {
            return Ok(current);
        }
        if current.state == AgentHostOccurrenceState::Prepared {
            return Err(AgentError::IllegalTransition(format!(
                "prepared occurrence {} was never dispatched; cancel it instead",
                current.occurrence_id
            )));
        }
        match host.reconcile_occurrence(&current)? {
            AgentOccurrenceResolution::Completed { response } => {
                let completed = current.complete(response)?;
                store.record_occurrence(&completed)?;
                Ok(completed)
            }
            AgentOccurrenceResolution::NotApplied { evidence } => {
                let not_applied = current.mark_not_applied(evidence)?;
                store.record_occurrence(&not_applied)?;
                Ok(not_applied)
            }
            AgentOccurrenceResolution::Unknown { evidence } => {
                if current.state == AgentHostOccurrenceState::Started {
                    let unknown = current.mark_unknown_with_evidence(
                        "reconciliation could not determine the original outcome",
                        evidence,
                    )?;
                    store.record_occurrence(&unknown)?;
                }
                Err(AgentError::RecoveryRequired(format!(
                    "host occurrence {} remains unknown",
                    current.occurrence_id
                )))
            }
        }
    }

    /// Cancel a prepared occurrence that is proven never to have started.
    pub fn cancel_prepared<J: AgentOccurrenceStore>(
        store: &mut J,
        session_id: &str,
        occurrence_id: &str,
        evidence: Vec<ContentBlock>,
    ) -> AgentResult<AgentHostOccurrence> {
        let current = load_occurrence(store, session_id, occurrence_id)?;
        if current.is_terminal() {
            return Ok(current);
        }
        if current.state != AgentHostOccurrenceState::Prepared {
            return Err(AgentError::IllegalTransition(format!(
                "occurrence {} may have started and cannot be cancelled without reconciliation",
                current.occurrence_id
            )));
        }
        let not_applied = current.mark_not_applied(evidence)?;
        store.record_occurrence(&not_applied)?;
        Ok(not_applied)
    }
}

fn load_occurrence<J: AgentOccurrenceStore>(
    store: &mut J,
    session_id: &str,
    occurrence_id: &str,
) -> AgentResult<AgentHostOccurrence> {
    store
        .load_occurrences(session_id)?
        .into_iter()
        .find(|occurrence| occurrence.occurrence_id == occurrence_id)
        .ok_or_else(|| {
            AgentError::NotFound(format!(
                "host occurrence {occurrence_id} does not exist in Session {session_id}"
            ))
        })
}
