use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex, MutexGuard, TryLockError},
};

use cymule_core::canonical_digest;
use cymule_durable::{DurableCoordinator, DurableStore, JournalRecord};

use crate::{AgentError, AgentResult, AgentUpdate};

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

/// Shareable in-memory journal for tests and embedded single-process use.
///
/// The mutex is adapter-local storage exclusion, never semantic authority. It
/// is acquired with `try_lock`; contention is returned immediately instead of
/// blocking a Session transition.
#[derive(Debug, Clone, Default)]
pub struct MemoryAgentJournal {
    entries: Arc<Mutex<BTreeMap<String, Vec<AgentUpdate>>>>,
}

impl MemoryAgentJournal {
    fn entries(&self) -> AgentResult<MutexGuard<'_, BTreeMap<String, Vec<AgentUpdate>>>> {
        match self.entries.try_lock() {
            Ok(entries) => Ok(entries),
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
        Ok(self.entries()?.get(session_id).cloned().unwrap_or_default())
    }

    fn append(&mut self, session_id: &str, update: &AgentUpdate) -> AgentResult<()> {
        let digest =
            canonical_digest(update).map_err(|error| AgentError::Validation(error.to_string()))?;
        let mut entries = self.entries()?;
        let session_entries = entries.entry(session_id.to_owned()).or_default();
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

const AGENT_UPDATE_SCHEMA: &str = "cymule.agent-update/1";

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
