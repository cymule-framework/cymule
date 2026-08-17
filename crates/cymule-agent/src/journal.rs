use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex, MutexGuard, TryLockError},
};

use cymule_core::canonical_digest;
use cymule_durable::{DurableCoordinator, DurableStore, JournalRecord};

use crate::{AgentError, AgentHostOccurrence, AgentResult, AgentUpdate};

/// Ordered durable update boundary for one agent Session.
///
/// Implementations provide storage mechanics only. Update validity remains the
/// responsibility of `AgentSession`, and an implementation must make one
/// successful append durable before returning.
pub trait AgentJournal {
    /// Load every accepted update for `session_id` in append order.
    fn load(&mut self, session_id: &str) -> AgentResult<Vec<AgentUpdate>>;

    /// Append one update idempotently.
    ///
    /// Repeating the same update identity and content succeeds without adding
    /// another record. Reusing an identity with different content fails closed.
    fn append(&mut self, session_id: &str, update: &AgentUpdate) -> AgentResult<()>;
}

/// Durable lifecycle boundary for agent host interaction occurrences.
pub trait AgentOccurrenceStore {
    /// Load the latest verified snapshot of each Session occurrence.
    fn load_occurrences(&mut self, session_id: &str) -> AgentResult<Vec<AgentHostOccurrence>>;

    /// Append one legal occurrence transition idempotently.
    fn record_occurrence(&mut self, occurrence: &AgentHostOccurrence) -> AgentResult<()>;
}

/// Journal used by the non-durable convenience driver.
#[derive(Debug, Clone, Copy, Default)]
pub struct NoopAgentJournal;

impl AgentJournal for NoopAgentJournal {
    fn load(&mut self, _session_id: &str) -> AgentResult<Vec<AgentUpdate>> {
        Ok(Vec::new())
    }

    fn append(&mut self, _session_id: &str, _update: &AgentUpdate) -> AgentResult<()> {
        Ok(())
    }
}

impl AgentOccurrenceStore for NoopAgentJournal {
    fn load_occurrences(&mut self, _session_id: &str) -> AgentResult<Vec<AgentHostOccurrence>> {
        Ok(Vec::new())
    }

    fn record_occurrence(&mut self, _occurrence: &AgentHostOccurrence) -> AgentResult<()> {
        Ok(())
    }
}

#[derive(Debug, Default)]
struct MemoryAgentJournalState {
    updates: BTreeMap<String, Vec<AgentUpdate>>,
    occurrences: BTreeMap<String, Vec<AgentHostOccurrence>>,
}

/// Shareable in-memory journal for tests and embedded single-process use.
///
/// The mutex is adapter-local storage exclusion, never semantic authority. It
/// is acquired with `try_lock`; contention is returned immediately instead of
/// blocking a Session transition.
#[derive(Debug, Clone, Default)]
pub struct MemoryAgentJournal {
    state: Arc<Mutex<MemoryAgentJournalState>>,
}

impl MemoryAgentJournal {
    fn state(&self) -> AgentResult<MutexGuard<'_, MemoryAgentJournalState>> {
        match self.state.try_lock() {
            Ok(state) => Ok(state),
            Err(TryLockError::WouldBlock) => Err(AgentError::Persistence(
                "agent journal is busy; retry the command".to_owned(),
            )),
            Err(TryLockError::Poisoned(_)) => Err(AgentError::Persistence(
                "agent journal storage was poisoned".to_owned(),
            )),
        }
    }
}

impl AgentJournal for MemoryAgentJournal {
    fn load(&mut self, session_id: &str) -> AgentResult<Vec<AgentUpdate>> {
        Ok(self
            .state()?
            .updates
            .get(session_id)
            .cloned()
            .unwrap_or_default())
    }

    fn append(&mut self, session_id: &str, update: &AgentUpdate) -> AgentResult<()> {
        let digest =
            canonical_digest(update).map_err(|error| AgentError::Validation(error.to_string()))?;
        let mut state = self.state()?;
        let session_entries = state.updates.entry(session_id.to_owned()).or_default();
        if let Some(existing) = session_entries
            .iter()
            .find(|existing| existing.update_id() == update.update_id())
        {
            let existing_digest = canonical_digest(existing)
                .map_err(|error| AgentError::Validation(error.to_string()))?;
            return if existing_digest == digest {
                Ok(())
            } else {
                Err(AgentError::IllegalTransition(format!(
                    "update ID {} was reused with different content",
                    update.update_id()
                )))
            };
        }
        session_entries.push(update.clone());
        Ok(())
    }
}

impl AgentOccurrenceStore for MemoryAgentJournal {
    fn load_occurrences(&mut self, session_id: &str) -> AgentResult<Vec<AgentHostOccurrence>> {
        reduce_occurrences(
            session_id,
            self.state()?
                .occurrences
                .get(session_id)
                .cloned()
                .unwrap_or_default(),
        )
    }

    fn record_occurrence(&mut self, occurrence: &AgentHostOccurrence) -> AgentResult<()> {
        occurrence.validate()?;
        let mut state = self.state()?;
        let snapshots = state
            .occurrences
            .entry(occurrence.session_id.clone())
            .or_default();
        append_occurrence_snapshot(snapshots, occurrence)
    }
}

const AGENT_UPDATE_SCHEMA: &str = "cymule.agent-update/1";
const AGENT_OCCURRENCE_SCHEMA: &str = "cymule.agent-host-occurrence/1";

fn occurrence_journal_id(session_id: &str) -> String {
    format!("cymule.agent.occurrences/{session_id}")
}

impl<S: DurableStore> AgentJournal for DurableCoordinator<S> {
    fn load(&mut self, session_id: &str) -> AgentResult<Vec<AgentUpdate>> {
        self.journal_records(session_id)
            .map_err(|error| AgentError::Persistence(error.to_string()))?
            .iter()
            .map(|record| {
                if record.schema != AGENT_UPDATE_SCHEMA {
                    return Err(AgentError::Persistence(format!(
                        "session {session_id} contains unsupported record schema {}",
                        record.schema
                    )));
                }
                let update: AgentUpdate = serde_json::from_value(record.payload.clone())
                    .map_err(|error| AgentError::Persistence(error.to_string()))?;
                if update.update_id() != record.record_id {
                    return Err(AgentError::Persistence(format!(
                        "journal record {} does not match update identity {}",
                        record.record_id,
                        update.update_id()
                    )));
                }
                Ok(update)
            })
            .collect()
    }

    fn append(&mut self, session_id: &str, update: &AgentUpdate) -> AgentResult<()> {
        let payload = serde_json::to_value(update)
            .map_err(|error| AgentError::Persistence(error.to_string()))?;
        let record = JournalRecord::new(update.update_id(), AGENT_UPDATE_SCHEMA, payload)
            .map_err(|error| AgentError::Persistence(error.to_string()))?;
        self.append_journal_record(session_id, record)
            .map(|_| ())
            .map_err(|error| AgentError::Persistence(error.to_string()))
    }
}

impl<S: DurableStore> AgentOccurrenceStore for DurableCoordinator<S> {
    fn load_occurrences(&mut self, session_id: &str) -> AgentResult<Vec<AgentHostOccurrence>> {
        let journal_id = occurrence_journal_id(session_id);
        let snapshots = self
            .journal_records(&journal_id)
            .map_err(|error| AgentError::Persistence(error.to_string()))?
            .iter()
            .map(|record| {
                if record.schema != AGENT_OCCURRENCE_SCHEMA {
                    return Err(AgentError::Persistence(format!(
                        "Session {session_id} contains unsupported occurrence schema {}",
                        record.schema
                    )));
                }
                let occurrence: AgentHostOccurrence =
                    serde_json::from_value(record.payload.clone())
                        .map_err(|error| AgentError::Persistence(error.to_string()))?;
                if occurrence.transition_id() != record.record_id {
                    return Err(AgentError::Persistence(format!(
                        "occurrence record {} does not match transition {}",
                        record.record_id,
                        occurrence.transition_id()
                    )));
                }
                Ok(occurrence)
            })
            .collect::<AgentResult<Vec<_>>>()?;
        reduce_occurrences(session_id, snapshots)
    }

    fn record_occurrence(&mut self, occurrence: &AgentHostOccurrence) -> AgentResult<()> {
        occurrence.validate()?;
        let existing = self
            .load_occurrences(&occurrence.session_id)?
            .into_iter()
            .find(|existing| existing.occurrence_id == occurrence.occurrence_id);
        if let Some(existing) = existing {
            existing.validate_successor(occurrence)?;
        } else if occurrence.state != crate::AgentHostOccurrenceState::Prepared {
            return Err(AgentError::IllegalTransition(format!(
                "host occurrence {} must begin prepared",
                occurrence.occurrence_id
            )));
        }
        let payload = serde_json::to_value(occurrence)
            .map_err(|error| AgentError::Persistence(error.to_string()))?;
        let record =
            JournalRecord::new(occurrence.transition_id(), AGENT_OCCURRENCE_SCHEMA, payload)
                .map_err(|error| AgentError::Persistence(error.to_string()))?;
        self.append_journal_record(&occurrence_journal_id(&occurrence.session_id), record)
            .map(|_| ())
            .map_err(|error| AgentError::Persistence(error.to_string()))
    }
}

fn reduce_occurrences(
    session_id: &str,
    snapshots: Vec<AgentHostOccurrence>,
) -> AgentResult<Vec<AgentHostOccurrence>> {
    let mut latest = BTreeMap::<String, AgentHostOccurrence>::new();
    for occurrence in snapshots {
        occurrence.validate()?;
        if occurrence.session_id != session_id {
            return Err(AgentError::Persistence(format!(
                "occurrence {} belongs to Session {}, not {session_id}",
                occurrence.occurrence_id, occurrence.session_id
            )));
        }
        match latest.get(&occurrence.occurrence_id) {
            Some(previous) => previous.validate_successor(&occurrence)?,
            None if occurrence.state != crate::AgentHostOccurrenceState::Prepared => {
                return Err(AgentError::IllegalTransition(format!(
                    "host occurrence {} does not begin prepared",
                    occurrence.occurrence_id
                )));
            }
            None => {}
        }
        latest.insert(occurrence.occurrence_id.clone(), occurrence);
    }
    Ok(latest.into_values().collect())
}

fn append_occurrence_snapshot(
    snapshots: &mut Vec<AgentHostOccurrence>,
    occurrence: &AgentHostOccurrence,
) -> AgentResult<()> {
    let latest = reduce_occurrences(&occurrence.session_id, snapshots.clone())?
        .into_iter()
        .find(|existing| existing.occurrence_id == occurrence.occurrence_id);
    match latest {
        Some(existing) => existing.validate_successor(occurrence)?,
        None if occurrence.state != crate::AgentHostOccurrenceState::Prepared => {
            return Err(AgentError::IllegalTransition(format!(
                "host occurrence {} must begin prepared",
                occurrence.occurrence_id
            )));
        }
        None => {}
    }
    if snapshots
        .iter()
        .any(|existing| existing.transition_id() == occurrence.transition_id())
    {
        return if snapshots.iter().any(|existing| existing == occurrence) {
            Ok(())
        } else {
            Err(AgentError::IllegalTransition(format!(
                "occurrence transition {} has conflicting content",
                occurrence.transition_id()
            )))
        };
    }
    snapshots.push(occurrence.clone());
    Ok(())
}
