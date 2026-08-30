use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex, MutexGuard, TryLockError},
};

use cymule_core::content_id;
use cymule_profile_protocol::agent::{
    AgentCommand, AgentCommandAction, AgentCommandOutcome, AgentCommandReceipt, AgentCommit,
    AgentElicitationQuery, AgentElicitationRead, AgentHostRequest, AgentMessageCurrent,
    AgentMessagePage, AgentMessagePageQuery, AgentMessagePageRead, AgentMessageQuery,
    AgentMessageRead, AgentOccurrenceCurrent, AgentOccurrencePage, AgentOccurrencePageQuery,
    AgentOccurrencePageRead, AgentOccurrenceQuery, AgentOccurrenceRead, AgentOccurrenceSource,
    AgentSessionCurrent, AgentSessionEntrySource, AgentSessionPostcondition, AgentSessionQuery,
    AgentSessionRead, AgentSessionUpdateEffect, AgentSessionUpdateSource, AgentStreamCommand,
    AgentStreamEffect, AgentStreamFinalizeOutcome, AgentStreamPublicationIntent, AgentStreamQuery,
    AgentStreamRead, AgentStreamSource, AgentStreamTarget, AgentStreamTargetSource,
    AgentTargetClaimCurrent, AgentTargetClaimSource, AgentTargetClaimTarget,
    AgentTargetClaimTransition, AgentToolQuery, AgentToolRead, AgentUpdate, AgentUpdateCurrent,
    AgentWorkspaceAdmissionQuery, AgentWorkspaceAdmissionRead, AgentWorkspaceCommitOutcome,
    ContextRequest, ContextSnapshot, MAX_AGENT_PAGE, MAX_AGENT_PAGE_BYTES,
};

use crate::{AgentError, AgentResult};

const EPHEMERAL_AGENT_REVISION_DOMAIN: &str = "cymule.ephemeral-agent-revision/1";

/// Closed Agent persistence and bounded-query capability.
///
/// Implementations may persist only [`AgentCommand`] and may expose only exact
/// or bounded revision-pinned reads. There is no generic journal, record, raw
/// `StateRoot`, or all-history escape through this boundary.
pub trait AgentPersistence {
    /// Commit one exact closed Agent command or replay its retained receipt.
    /// Only this call's acknowledged new write returns `committed_revision`;
    /// exact replay returns null even when the observed head has not advanced.
    ///
    /// # Errors
    ///
    /// Returns an error when the command is invalid or stale, the retained
    /// receipt conflicts, or the underlying commit has no definitive outcome.
    fn commit_agent(&mut self, command: &AgentCommand) -> AgentResult<AgentCommit>;

    /// Finalize one stream through the framework-owned delivery authority.
    ///
    /// Implementations may use the ordinary reducer for staged delivery, but
    /// external delivery must resolve the stream's pinned provider registry
    /// entry and commit its publication, catalog record, Resource pin, and
    /// Agent receipt in one CAS. No caller-supplied publication is accepted.
    ///
    /// # Errors
    ///
    /// Returns an error when the command is not a finalization, the pinned
    /// delivery authority cannot be resolved, or the coupled commit fails.
    fn finalize_agent_stream(
        &mut self,
        command: &AgentCommand,
    ) -> AgentResult<AgentStreamFinalizeOutcome>;

    /// Reconcile one prior external finalization without issuing another publish.
    ///
    /// # Errors
    ///
    /// Returns an error when `expected_intent` is invalid, the exact Finalize
    /// command/touched source no longer derives it, or the retained provider
    /// binding cannot observe it.
    fn reconcile_agent_stream(
        &mut self,
        command: &AgentCommand,
        expected_intent: &AgentStreamPublicationIntent,
    ) -> AgentResult<AgentStreamFinalizeOutcome>;

    /// Commit one workspace phase through the framework-owned binding/provider
    /// authority. The serialized command contains only semantic intent; host
    /// bindings and settlement observations never cross this interface.
    ///
    /// # Errors
    ///
    /// Returns an error when the command is not workspace-owned, its original
    /// binding cannot be resolved, or the Agent/M1 commit fails.
    fn commit_agent_workspace(
        &mut self,
        command: &AgentCommand,
    ) -> AgentResult<AgentWorkspaceCommitOutcome>;

    /// Read one exact bounded Session metadata current.
    ///
    /// # Errors
    ///
    /// Returns an error when the query is invalid, stale, or cannot be read
    /// and verified from the selected revision.
    fn read_agent_session(&mut self, query: &AgentSessionQuery) -> AgentResult<AgentSessionRead>;

    /// Read one bounded backward page of immutable messages.
    ///
    /// # Errors
    ///
    /// Returns an error when the pinned revision, head, cursor, or page budget
    /// is invalid, or the page cannot be verified.
    fn read_agent_messages(
        &mut self,
        query: &AgentMessagePageQuery,
    ) -> AgentResult<AgentMessagePageRead>;

    /// Read one exact immutable message.
    ///
    /// # Errors
    ///
    /// Returns an error when the exact query is invalid or stale, or its read
    /// cannot be verified.
    fn read_agent_message(&mut self, query: &AgentMessageQuery) -> AgentResult<AgentMessageRead>;

    /// Read one exact tool current.
    ///
    /// # Errors
    ///
    /// Returns an error when the exact query is invalid or stale, or its read
    /// cannot be verified.
    fn read_agent_tool(&mut self, query: &AgentToolQuery) -> AgentResult<AgentToolRead>;

    /// Read one exact elicitation current.
    ///
    /// # Errors
    ///
    /// Returns an error when the exact query is invalid or stale, or its read
    /// cannot be verified.
    fn read_agent_elicitation(
        &mut self,
        query: &AgentElicitationQuery,
    ) -> AgentResult<AgentElicitationRead>;

    /// Read one exact host occurrence current.
    ///
    /// # Errors
    ///
    /// Returns an error when the exact query is invalid or stale, or its read
    /// cannot be verified.
    fn read_agent_occurrence(
        &mut self,
        query: &AgentOccurrenceQuery,
    ) -> AgentResult<AgentOccurrenceRead>;

    /// Read one bounded forward page of unresolved occurrences.
    ///
    /// # Errors
    ///
    /// Returns an error when the pinned revision, generation, cursor, or page
    /// budget is invalid, or the page cannot be verified.
    fn read_agent_occurrences(
        &mut self,
        query: &AgentOccurrencePageQuery,
    ) -> AgentResult<AgentOccurrencePageRead>;

    /// Read one exact stream current.
    ///
    /// # Errors
    ///
    /// Returns an error when the exact query is invalid or stale, or its read
    /// cannot be verified.
    fn read_agent_stream(&mut self, query: &AgentStreamQuery) -> AgentResult<AgentStreamRead>;

    /// Resolve one revision-pinned M1 workspace Effect/abort admission.
    ///
    /// # Errors
    ///
    /// Returns an error when the request is invalid or stale, or the M1
    /// structural admission and provider binding cannot be resolved exactly.
    fn read_agent_workspace_admission(
        &mut self,
        query: &AgentWorkspaceAdmissionQuery,
    ) -> AgentResult<AgentWorkspaceAdmissionRead>;
}

impl<S: cymule_durable::DurableStore> AgentPersistence
    for cymule_durable::DurableAgentControl<'_, S>
{
    fn commit_agent(&mut self, command: &AgentCommand) -> AgentResult<AgentCommit> {
        cymule_durable::DurableAgentControl::commit_agent(self, command).map_err(AgentError::from)
    }

    fn finalize_agent_stream(
        &mut self,
        command: &AgentCommand,
    ) -> AgentResult<AgentStreamFinalizeOutcome> {
        cymule_durable::DurableAgentControl::finalize_agent_stream(self, command)
            .map_err(AgentError::from)
    }

    fn reconcile_agent_stream(
        &mut self,
        command: &AgentCommand,
        expected_intent: &AgentStreamPublicationIntent,
    ) -> AgentResult<AgentStreamFinalizeOutcome> {
        cymule_durable::DurableAgentControl::reconcile_agent_stream(self, command, expected_intent)
            .map_err(AgentError::from)
    }

    fn commit_agent_workspace(
        &mut self,
        command: &AgentCommand,
    ) -> AgentResult<AgentWorkspaceCommitOutcome> {
        cymule_durable::DurableAgentControl::commit_agent_workspace(self, command)
            .map_err(AgentError::from)
    }

    fn read_agent_session(&mut self, query: &AgentSessionQuery) -> AgentResult<AgentSessionRead> {
        cymule_durable::DurableAgentControl::read_agent_session(self, query)
            .map_err(AgentError::from)
    }

    fn read_agent_messages(
        &mut self,
        query: &AgentMessagePageQuery,
    ) -> AgentResult<AgentMessagePageRead> {
        cymule_durable::DurableAgentControl::read_agent_messages(self, query)
            .map_err(AgentError::from)
    }

    fn read_agent_message(&mut self, query: &AgentMessageQuery) -> AgentResult<AgentMessageRead> {
        cymule_durable::DurableAgentControl::read_agent_message(self, query)
            .map_err(AgentError::from)
    }

    fn read_agent_tool(&mut self, query: &AgentToolQuery) -> AgentResult<AgentToolRead> {
        cymule_durable::DurableAgentControl::read_agent_tool(self, query).map_err(AgentError::from)
    }

    fn read_agent_elicitation(
        &mut self,
        query: &AgentElicitationQuery,
    ) -> AgentResult<AgentElicitationRead> {
        cymule_durable::DurableAgentControl::read_agent_elicitation(self, query)
            .map_err(AgentError::from)
    }

    fn read_agent_occurrence(
        &mut self,
        query: &AgentOccurrenceQuery,
    ) -> AgentResult<AgentOccurrenceRead> {
        cymule_durable::DurableAgentControl::read_agent_occurrence(self, query)
            .map_err(AgentError::from)
    }

    fn read_agent_occurrences(
        &mut self,
        query: &AgentOccurrencePageQuery,
    ) -> AgentResult<AgentOccurrencePageRead> {
        cymule_durable::DurableAgentControl::read_agent_occurrences(self, query)
            .map_err(AgentError::from)
    }

    fn read_agent_stream(&mut self, query: &AgentStreamQuery) -> AgentResult<AgentStreamRead> {
        cymule_durable::DurableAgentControl::read_agent_stream(self, query)
            .map_err(AgentError::from)
    }

    fn read_agent_workspace_admission(
        &mut self,
        query: &AgentWorkspaceAdmissionQuery,
    ) -> AgentResult<AgentWorkspaceAdmissionRead> {
        cymule_durable::DurableAgentControl::read_agent_workspace_admission(self, query)
            .map_err(AgentError::from)
    }
}

/// Monotonic, non-resettable view over one revision- and source-pinned message scan.
pub trait AgentMessageReader {
    /// Read the next older page, or `None` after the pinned beginning is reached.
    ///
    /// # Errors
    ///
    /// Returns an error when the requested page size is zero, the cumulative
    /// capability budget is exhausted, or persistence cannot return a verified page.
    fn read_previous(&mut self, max_entries: u64) -> AgentResult<Option<AgentMessagePageRead>>;
}

/// Framework-owned context selection capability over bounded message pages.
///
/// The adapter cannot choose a new revision, reset the cursor, or renew the
/// cumulative entry/byte budget by issuing another query.
pub struct PinnedAgentMessageReader<'a, P: AgentPersistence + ?Sized> {
    persistence: &'a mut P,
    session_id: String,
    message_head: Option<String>,
    message_count: u64,
    revision: String,
    next_end_exclusive: Option<u64>,
    expected_page_terminal_head: Option<String>,
    remaining_entries: u64,
    remaining_bytes: u64,
    observed_messages: BTreeMap<u64, AgentMessageCurrent>,
    finished: bool,
}

impl<'a, P: AgentPersistence + ?Sized> PinnedAgentMessageReader<'a, P> {
    /// Pin the exact Session revision, message head, and count for one selection call.
    ///
    /// # Errors
    ///
    /// Returns an error when the context request is invalid, the Session is
    /// absent, or the requested source count exceeds its retained message history.
    pub fn new(persistence: &'a mut P, request: &ContextRequest) -> AgentResult<Self> {
        AgentHostRequest::Context(request.clone()).validate_for_session(&request.session_id)?;
        let session_query = AgentSessionQuery {
            session_id: request.session_id.clone(),
            expected_revision: None,
        };
        let session = persistence.read_agent_session(&session_query)?;
        session.verify_for(&session_query)?;
        let current = session.current.ok_or_else(|| {
            AgentError::NotFound(format!("Session {} does not exist", request.session_id))
        })?;
        if current.message_count < request.source_message_count {
            return Err(AgentError::persistence(
                "agent_context_message_source_stale",
                "context request message source exceeds the retained Session history",
            ));
        }
        Ok(Self {
            persistence,
            session_id: request.session_id.clone(),
            message_head: request.source_message_head.clone(),
            message_count: request.source_message_count,
            revision: session.revision,
            next_end_exclusive: None,
            expected_page_terminal_head: request.source_message_head.clone(),
            remaining_entries: request.scan_limits.max_entries,
            remaining_bytes: request.scan_limits.max_canonical_bytes,
            observed_messages: BTreeMap::new(),
            finished: request.source_message_count == 0,
        })
    }

    /// Verify that a context provider selected only exact messages delivered
    /// through this one pinned reader capability.
    ///
    /// # Errors
    ///
    /// Returns an error when the snapshot changes the pinned head or count,
    /// cites a message which was not read, or changes a delivered ordinal,
    /// identity, or immutable payload digest.
    pub fn verify_snapshot(&self, snapshot: &ContextSnapshot) -> AgentResult<()> {
        if snapshot.source_message_head != self.message_head
            || snapshot.source_message_count != self.message_count
        {
            return Err(AgentError::persistence(
                "agent_context_source_descriptor_mismatch",
                "context snapshot changed its pinned message source descriptor",
            ));
        }
        for selected in &snapshot.selected_messages {
            let observed = self.observed_messages.get(&selected.index).ok_or_else(|| {
                AgentError::persistence(
                    "agent_context_message_not_read",
                    format!(
                        "context snapshot selected unread message ordinal {}",
                        selected.index
                    ),
                )
            })?;
            if let Err(error) = selected.verify_for(observed) {
                return Err(AgentError::persistence(
                    "agent_context_message_mismatch",
                    format!(
                        "context snapshot changed delivered message ordinal {}: {error}",
                        selected.index,
                    ),
                ));
            }
        }
        Ok(())
    }
}

impl<P: AgentPersistence + ?Sized> AgentMessageReader for PinnedAgentMessageReader<'_, P> {
    fn read_previous(&mut self, max_entries: u64) -> AgentResult<Option<AgentMessagePageRead>> {
        if self.finished {
            return Ok(None);
        }
        if max_entries == 0 || self.remaining_entries == 0 || self.remaining_bytes == 0 {
            return Err(AgentError::Validation(
                "context message scan exhausted its cumulative capability budget".to_owned(),
            ));
        }
        let query = AgentMessagePageQuery {
            session_id: self.session_id.clone(),
            expected_message_head: self.message_head.clone(),
            source_message_count: self.message_count,
            end_exclusive: self.next_end_exclusive,
            max_entries: max_entries
                .min(self.remaining_entries)
                .min(MAX_AGENT_PAGE as u64),
            max_message_canonical_bytes: self.remaining_bytes.min(MAX_AGENT_PAGE_BYTES as u64),
            max_canonical_bytes: MAX_AGENT_PAGE_BYTES as u64,
            expected_revision: Some(self.revision.clone()),
        };
        let read = self.persistence.read_agent_messages(&query)?;
        read.verify_for(&query)?;
        let actual_terminal_head = read
            .page
            .entries
            .last()
            .map(|entry| entry.order.head.as_str());
        if actual_terminal_head != self.expected_page_terminal_head.as_deref() {
            return Err(AgentError::persistence(
                "agent_context_message_page_chain_mismatch",
                "context message page does not continue the previously verified order chain",
            ));
        }
        let consumed_entries = u64::try_from(read.page.entries.len())
            .expect("verified Agent page length fits the cumulative u64 budget");
        let consumed_bytes = read.page.entries.iter().try_fold(0_u64, |total, current| {
            let bytes = u64::try_from(
                cymule_core::canonical_bytes(current)
                    .map_err(|error| AgentError::Validation(error.to_string()))?
                    .len(),
            )
            .expect("verified Agent message-current bytes fit u64");
            total.checked_add(bytes).ok_or_else(|| {
                AgentError::Validation("context message-current byte count is exhausted".to_owned())
            })
        })?;
        for current in &read.page.entries {
            if self
                .observed_messages
                .insert(current.order.index, current.clone())
                .is_some_and(|previous| previous != *current)
            {
                return Err(AgentError::persistence(
                    "agent_context_message_changed_during_scan",
                    "pinned context scan returned two values for one message ordinal",
                ));
            }
        }
        self.remaining_entries = self
            .remaining_entries
            .checked_sub(consumed_entries)
            .ok_or_else(|| {
                AgentError::persistence(
                    "agent_context_entry_budget_underflow",
                    "context entry budget underflowed",
                )
            })?;
        self.remaining_bytes = self
            .remaining_bytes
            .checked_sub(consumed_bytes)
            .ok_or_else(|| {
                AgentError::persistence(
                    "agent_context_byte_budget_underflow",
                    "context byte budget underflowed",
                )
            })?;
        self.expected_page_terminal_head = if read.page.next_end_exclusive.is_some() {
            read.page
                .entries
                .first()
                .and_then(|entry| entry.order.previous_head.clone())
        } else {
            None
        };
        self.next_end_exclusive = read.page.next_end_exclusive;
        self.finished = self.next_end_exclusive.is_none();
        Ok(Some(read))
    }
}

#[derive(Debug)]
struct EphemeralAgentState {
    revision: String,
    sessions: BTreeMap<String, AgentSessionCurrent>,
    updates: BTreeMap<(String, String), AgentUpdateCurrent>,
    messages: BTreeMap<(String, String), AgentMessageCurrent>,
    message_order: BTreeMap<(String, u64), String>,
    tools: BTreeMap<(String, String), cymule_profile_protocol::agent::AgentToolCurrent>,
    target_claims: BTreeMap<(String, AgentTargetClaimTarget), AgentTargetClaimCurrent>,
    elicitations:
        BTreeMap<(String, String), cymule_profile_protocol::agent::AgentElicitationCurrent>,
    occurrences: BTreeMap<(String, String), AgentOccurrenceCurrent>,
    unresolved_occurrences: BTreeMap<(String, u64), String>,
    streams: BTreeMap<(String, String), cymule_profile_protocol::agent::AgentStreamCurrent>,
    stream_chunks:
        BTreeMap<(String, String, u64), cymule_profile_protocol::agent::AgentStreamChunkCurrent>,
    receipts: BTreeMap<String, AgentCommandReceipt>,
}

impl Default for EphemeralAgentState {
    fn default() -> Self {
        Self {
            revision: content_id(EPHEMERAL_AGENT_REVISION_DOMAIN, &"genesis")
                .expect("static ephemeral Agent genesis identity is valid"),
            sessions: BTreeMap::new(),
            updates: BTreeMap::new(),
            messages: BTreeMap::new(),
            message_order: BTreeMap::new(),
            tools: BTreeMap::new(),
            target_claims: BTreeMap::new(),
            elicitations: BTreeMap::new(),
            occurrences: BTreeMap::new(),
            unresolved_occurrences: BTreeMap::new(),
            streams: BTreeMap::new(),
            stream_chunks: BTreeMap::new(),
            receipts: BTreeMap::new(),
        }
    }
}

/// Explicit process-lifetime implementation of the closed Agent capability.
///
/// This type is useful for unit tests and local tools. It never implements or
/// emulates M1 input, Workspace, or cross-profile Resource atomicity; those
/// commands require the real Durable typed façade.
#[derive(Debug, Clone, Default)]
pub struct EphemeralAgentPersistence {
    state: Arc<Mutex<EphemeralAgentState>>,
}

impl EphemeralAgentPersistence {
    fn state(&self) -> AgentResult<MutexGuard<'_, EphemeralAgentState>> {
        match self.state.try_lock() {
            Ok(state) => Ok(state),
            Err(TryLockError::WouldBlock) => Err(AgentError::persistence(
                "ephemeral_agent_busy",
                "ephemeral Agent persistence is busy",
            )),
            Err(TryLockError::Poisoned(_)) => Err(AgentError::persistence(
                "ephemeral_agent_poisoned",
                "ephemeral Agent persistence was poisoned",
            )),
        }
    }
}

impl AgentPersistence for EphemeralAgentPersistence {
    fn commit_agent(&mut self, command: &AgentCommand) -> AgentResult<AgentCommit> {
        command.verify()?;
        let mut state = self.state()?;
        if let Some(receipt) = state.receipts.get(&command.command_id).cloned() {
            receipt.verify_for(command)?;
            verify_ephemeral_target_claim_receipt(&state, command, &receipt)?;
            let commit = AgentCommit {
                observed_revision: state.revision.clone(),
                committed_revision: None,
                receipt,
            };
            commit.verify_for(command)?;
            return Ok(commit);
        }
        if command.source_revision != state.revision {
            return Err(AgentError::persistence(
                "ephemeral_agent_revision_conflict",
                format!(
                    "Agent command {} expected revision {}, current revision is {}",
                    command.command_id, command.source_revision, state.revision
                ),
            ));
        }
        let source = resolve_source(&state, command)?;
        let outcome = reduce_ephemeral(command, &source)?;
        let result_revision = content_id(
            EPHEMERAL_AGENT_REVISION_DOMAIN,
            &(
                state.revision.as_str(),
                command.command_id.as_str(),
                &source,
                &outcome,
            ),
        )
        .map_err(|error| AgentError::Validation(error.to_string()))?;
        let receipt = AgentCommandReceipt::new(command, source.clone(), outcome.clone())?;
        let commit = AgentCommit {
            observed_revision: result_revision.clone(),
            committed_revision: Some(result_revision.clone()),
            receipt: receipt.clone(),
        };
        commit.verify_for(command)?;
        apply_outcome(&mut state, command, &source, outcome)?;
        state.revision = result_revision;
        state.receipts.insert(command.command_id.clone(), receipt);
        Ok(commit)
    }

    fn finalize_agent_stream(
        &mut self,
        command: &AgentCommand,
    ) -> AgentResult<AgentStreamFinalizeOutcome> {
        if !matches!(
            command.action,
            AgentCommandAction::Stream(AgentStreamCommand::Finalize { .. })
        ) {
            return Err(AgentError::Validation(
                "Agent stream finalization capability accepts only Finalize commands".to_owned(),
            ));
        }
        self.commit_agent(command)
            .map(|commit| AgentStreamFinalizeOutcome::Committed {
                commit: Box::new(commit),
            })
    }

    fn reconcile_agent_stream(
        &mut self,
        command: &AgentCommand,
        expected_intent: &AgentStreamPublicationIntent,
    ) -> AgentResult<AgentStreamFinalizeOutcome> {
        command.verify()?;
        expected_intent.verify()?;
        let AgentCommandAction::Stream(AgentStreamCommand::Finalize {
            session_id,
            stream_id,
        }) = &command.action
        else {
            return Err(AgentError::Validation(
                "Agent stream reconciliation accepts only Finalize commands".to_owned(),
            ));
        };
        if expected_intent.source_revision() != command.source_revision
            || expected_intent.command_id() != command.command_id
            || expected_intent.session_id() != session_id
            || expected_intent.stream_id() != stream_id
        {
            return Err(AgentError::Conflict {
                code: "agent_stream_publication_intent_changed".to_owned(),
                message: "Agent stream reconciliation intent does not match its command".to_owned(),
            });
        }
        let state = self.state()?;
        let receipt = state
            .receipts
            .get(&command.command_id)
            .cloned()
            .ok_or_else(|| {
                AgentError::NotFound(format!(
                    "ephemeral Agent persistence has no retained finalization {} to reconcile",
                    command.command_id
                ))
            })?;
        receipt.verify_for(command)?;
        let commit = AgentCommit {
            observed_revision: state.revision.clone(),
            committed_revision: None,
            receipt,
        };
        commit.verify_for(command)?;
        Ok(AgentStreamFinalizeOutcome::Committed {
            commit: Box::new(commit),
        })
    }

    fn commit_agent_workspace(
        &mut self,
        command: &AgentCommand,
    ) -> AgentResult<AgentWorkspaceCommitOutcome> {
        command.verify()?;
        if !matches!(command.action, AgentCommandAction::Workspace(_)) {
            return Err(AgentError::Validation(
                "Agent workspace capability accepts only Workspace commands".to_owned(),
            ));
        }
        Err(AgentError::persistence(
            "ephemeral_agent_workspace_authority_unavailable",
            "ephemeral Agent persistence has no M1 workspace authority",
        ))
    }

    fn read_agent_session(&mut self, query: &AgentSessionQuery) -> AgentResult<AgentSessionRead> {
        query.verify()?;
        let state = self.state()?;
        verify_ephemeral_revision(query.expected_revision.as_ref(), &state.revision)?;
        let read = AgentSessionRead {
            revision: state.revision.clone(),
            current: state.sessions.get(&query.session_id).cloned(),
        };
        read.verify_for(query)?;
        Ok(read)
    }

    fn read_agent_messages(
        &mut self,
        query: &AgentMessagePageQuery,
    ) -> AgentResult<AgentMessagePageRead> {
        query.verify()?;
        let state = self.state()?;
        verify_ephemeral_revision(query.expected_revision.as_ref(), &state.revision)?;
        let session = session_or_genesis(&state, &query.session_id)?;
        if query.source_message_count > session.message_count {
            return Err(AgentError::persistence(
                "agent_message_source_stale",
                "Agent message query source exceeds the retained Session history",
            ));
        }
        let source_head = ephemeral_message_source_head(&state, query)?;
        if source_head.as_deref() != query.expected_message_head.as_deref() {
            return Err(AgentError::persistence(
                "agent_message_source_stale",
                "Agent message query source is not the retained Session history prefix",
            ));
        }
        let end = query.end_exclusive.unwrap_or(query.source_message_count);
        if end > query.source_message_count {
            return Err(AgentError::Validation(
                "Agent message page cursor exceeds the pinned source count".to_owned(),
            ));
        }
        let entries = ephemeral_message_page_entries(&state, query, end)?;
        let read = message_page_read(&state.revision, query, entries);
        if end > 0 && read.page.entries.is_empty() {
            return Err(AgentError::Validation(
                "Agent message page byte budget cannot fit one bounded entry".to_owned(),
            ));
        }
        read.verify_for(query)?;
        Ok(read)
    }

    fn read_agent_message(&mut self, query: &AgentMessageQuery) -> AgentResult<AgentMessageRead> {
        query.verify()?;
        let state = self.state()?;
        verify_ephemeral_revision(query.expected_revision.as_ref(), &state.revision)?;
        let read = AgentMessageRead {
            revision: state.revision.clone(),
            current: state
                .messages
                .get(&(query.session_id.clone(), query.message_id.clone()))
                .cloned(),
        };
        read.verify_for(query)?;
        Ok(read)
    }

    fn read_agent_tool(&mut self, query: &AgentToolQuery) -> AgentResult<AgentToolRead> {
        query.verify()?;
        let state = self.state()?;
        verify_ephemeral_revision(query.expected_revision.as_ref(), &state.revision)?;
        let read = AgentToolRead {
            revision: state.revision.clone(),
            current: state
                .tools
                .get(&(query.session_id.clone(), query.tool_call_id.clone()))
                .cloned(),
        };
        read.verify_for(query)?;
        Ok(read)
    }

    fn read_agent_elicitation(
        &mut self,
        query: &AgentElicitationQuery,
    ) -> AgentResult<AgentElicitationRead> {
        query.verify()?;
        let state = self.state()?;
        verify_ephemeral_revision(query.expected_revision.as_ref(), &state.revision)?;
        let read = AgentElicitationRead {
            revision: state.revision.clone(),
            current: state
                .elicitations
                .get(&(query.session_id.clone(), query.request_id.clone()))
                .cloned(),
        };
        read.verify_for(query)?;
        Ok(read)
    }

    fn read_agent_occurrence(
        &mut self,
        query: &AgentOccurrenceQuery,
    ) -> AgentResult<AgentOccurrenceRead> {
        query.verify()?;
        let state = self.state()?;
        verify_ephemeral_revision(query.expected_revision.as_ref(), &state.revision)?;
        let read = AgentOccurrenceRead {
            revision: state.revision.clone(),
            current: state
                .occurrences
                .get(&(query.session_id.clone(), query.occurrence_id.clone()))
                .cloned(),
        };
        read.verify_for(query)?;
        Ok(read)
    }

    fn read_agent_occurrences(
        &mut self,
        query: &AgentOccurrencePageQuery,
    ) -> AgentResult<AgentOccurrencePageRead> {
        query.verify()?;
        let state = self.state()?;
        verify_ephemeral_revision(query.expected_revision.as_ref(), &state.revision)?;
        let session = session_or_genesis(&state, &query.session_id)?;
        if session.unresolved_occurrence_generation != query.index_generation {
            return Err(AgentError::persistence(
                "agent_unresolved_generation_stale",
                "Agent unresolved-occurrence generation is stale",
            ));
        }
        let max_entries =
            usize::try_from(query.max_entries).expect("verified Agent page limit fits usize");
        let max_canonical_bytes = usize::try_from(query.max_canonical_bytes)
            .expect("verified Agent occurrence-page byte limit fits usize");
        let candidates = state
            .unresolved_occurrences
            .range(
                (query.session_id.clone(), query.after_ordinal.unwrap_or(0))
                    ..=(query.session_id.clone(), u64::MAX),
            )
            .filter(|((_, ordinal), _)| query.after_ordinal.is_none_or(|after| *ordinal > after))
            .take(max_entries + 1)
            .map(|((_, ordinal), occurrence_id)| (*ordinal, occurrence_id.clone()))
            .collect::<Vec<_>>();
        let mut entries = Vec::new();
        let mut has_more = false;
        for (position, (_, occurrence_id)) in candidates.iter().enumerate() {
            if position >= max_entries {
                has_more = true;
                break;
            }
            let current = state
                .occurrences
                .get(&(query.session_id.clone(), occurrence_id.clone()))
                .cloned()
                .ok_or_else(|| {
                    AgentError::persistence(
                        "agent_unresolved_occurrence_missing",
                        format!("Agent unresolved occurrence {occurrence_id} is missing"),
                    )
                })?;
            entries.push(current);
            let candidate = occurrence_page_read(
                &state.revision,
                query,
                entries.clone(),
                position + 1 < candidates.len(),
            );
            let candidate_bytes = cymule_core::canonical_bytes(&candidate)
                .map_err(|error| AgentError::Validation(error.to_string()))?
                .len();
            if candidate_bytes > max_canonical_bytes {
                entries.pop();
                has_more = true;
                break;
            }
        }
        let read = occurrence_page_read(&state.revision, query, entries, has_more);
        if !candidates.is_empty() && read.page.entries.is_empty() {
            return Err(AgentError::Validation(
                "Agent occurrence page byte budget cannot fit one bounded entry".to_owned(),
            ));
        }
        read.verify_for(query)?;
        Ok(read)
    }

    fn read_agent_stream(&mut self, query: &AgentStreamQuery) -> AgentResult<AgentStreamRead> {
        query.verify()?;
        let state = self.state()?;
        verify_ephemeral_revision(query.expected_revision.as_ref(), &state.revision)?;
        let read = AgentStreamRead {
            revision: state.revision.clone(),
            current: state
                .streams
                .get(&(query.session_id.clone(), query.stream_id.clone()))
                .cloned(),
        };
        read.verify_for(query)?;
        Ok(read)
    }

    fn read_agent_workspace_admission(
        &mut self,
        query: &AgentWorkspaceAdmissionQuery,
    ) -> AgentResult<AgentWorkspaceAdmissionRead> {
        query.verify()?;
        Err(AgentError::persistence(
            "ephemeral_agent_workspace_admission_unavailable",
            "ephemeral Agent persistence cannot resolve M1 workspace authority",
        ))
    }
}

fn ephemeral_message_source_head(
    state: &EphemeralAgentState,
    query: &AgentMessagePageQuery,
) -> AgentResult<Option<String>> {
    if query.source_message_count == 0 {
        return Ok(None);
    }
    let source_index = query.source_message_count - 1;
    let message_id = state
        .message_order
        .get(&(query.session_id.clone(), source_index))
        .ok_or_else(|| {
            AgentError::persistence(
                "agent_message_source_entry_missing",
                format!("Agent message source entry {source_index} is missing"),
            )
        })?;
    let current = state
        .messages
        .get(&(query.session_id.clone(), message_id.clone()))
        .ok_or_else(|| {
            AgentError::persistence(
                "agent_message_source_payload_missing",
                format!("Agent message source payload {message_id} is missing"),
            )
        })?;
    current.verify()?;
    if current.session_id != query.session_id || current.order.index != source_index {
        return Err(AgentError::persistence(
            "agent_message_source_membership_mismatch",
            "Agent message source entry does not match its Session ordinal membership",
        ));
    }
    Ok(Some(current.order.head.clone()))
}

fn ephemeral_message_page_entries(
    state: &EphemeralAgentState,
    query: &AgentMessagePageQuery,
    end: u64,
) -> AgentResult<Vec<AgentMessageCurrent>> {
    let lower = end - end.min(query.max_entries);
    let max_message_canonical_bytes = usize::try_from(query.max_message_canonical_bytes)
        .expect("verified Agent message-current byte limit fits usize");
    let max_canonical_bytes = usize::try_from(query.max_canonical_bytes)
        .expect("verified Agent message-page byte limit fits usize");
    let mut entries = Vec::new();
    let mut message_canonical_bytes = 0_usize;
    for index in (lower..end).rev() {
        let message_id = state
            .message_order
            .get(&(query.session_id.clone(), index))
            .ok_or_else(|| {
                AgentError::persistence(
                    "agent_message_order_entry_missing",
                    format!("Agent message order entry {index} is missing"),
                )
            })?;
        let current = state
            .messages
            .get(&(query.session_id.clone(), message_id.clone()))
            .cloned()
            .ok_or_else(|| {
                AgentError::persistence(
                    "agent_message_payload_missing",
                    format!("Agent message payload {message_id} is missing"),
                )
            })?;
        let current_bytes = cymule_core::canonical_bytes(&current)
            .map_err(|error| AgentError::Validation(error.to_string()))?
            .len();
        let candidate_message_bytes = message_canonical_bytes
            .checked_add(current_bytes)
            .ok_or_else(|| {
                AgentError::Validation("Agent message-current byte count is exhausted".to_owned())
            })?;
        entries.insert(0, current);
        let candidate = message_page_read(&state.revision, query, entries.clone());
        let candidate_bytes = cymule_core::canonical_bytes(&candidate)
            .map_err(|error| AgentError::Validation(error.to_string()))?
            .len();
        if candidate_message_bytes > max_message_canonical_bytes
            || candidate_bytes > max_canonical_bytes
        {
            entries.remove(0);
            break;
        }
        message_canonical_bytes = candidate_message_bytes;
    }
    Ok(entries)
}

fn verify_ephemeral_revision(expected: Option<&String>, actual: &str) -> AgentResult<()> {
    if let Some(expected) = expected
        && expected != actual
    {
        return Err(AgentError::persistence(
            "ephemeral_agent_revision_conflict",
            format!("expected Agent revision {expected}, current revision is {actual}"),
        ));
    }
    Ok(())
}

fn resolve_source(
    state: &EphemeralAgentState,
    command: &AgentCommand,
) -> AgentResult<cymule_profile_protocol::agent::AgentCommandSource> {
    use cymule_profile_protocol::agent::AgentCommandSource;

    Ok(match &command.action {
        AgentCommandAction::SessionUpdate { session_id, update } => {
            let session = session_or_genesis(state, session_id)?;
            let update_current = state
                .updates
                .get(&(session_id.clone(), update.update_id().to_owned()))
                .cloned();
            let entry = match update {
                AgentUpdate::Message { message, .. } => AgentSessionEntrySource::Message {
                    current: state
                        .messages
                        .get(&(session_id.clone(), message.message_id.clone()))
                        .cloned(),
                },
                AgentUpdate::Tool { tool, .. } => AgentSessionEntrySource::Tool {
                    current: state
                        .tools
                        .get(&(session_id.clone(), tool.tool_call_id.clone()))
                        .cloned(),
                },
                AgentUpdate::Elicitation { .. } => {
                    return Err(AgentError::Validation(
                        "Agent elicitation mutation requires the durable input capability"
                            .to_owned(),
                    ));
                }
                AgentUpdate::State {
                    state: cymule_profile_protocol::agent::AgentState::Closed,
                    ..
                } => AgentSessionEntrySource::Close {
                    tools: session
                        .nonterminal_tools
                        .keys()
                        .map(|tool_call_id| {
                            state
                                .tools
                                .get(&(session_id.clone(), tool_call_id.clone()))
                                .cloned()
                                .ok_or_else(|| {
                                    AgentError::persistence(
                                        "agent_nonterminal_tool_missing",
                                        format!(
                                            "Agent Session {session_id} non-terminal Tool {tool_call_id} is missing"
                                        ),
                                    )
                                })
                        })
                        .collect::<AgentResult<Vec<_>>>()?,
                },
                AgentUpdate::State { .. }
                | AgentUpdate::Plan { .. }
                | AgentUpdate::Usage { .. } => AgentSessionEntrySource::Metadata,
            };
            let target_claims = ephemeral_target_claim_sources(state, session_id, update, &entry)?;
            AgentCommandSource::Session {
                session,
                update: AgentSessionUpdateSource {
                    update: update_current,
                    entry,
                    target_claims,
                },
            }
        }
        AgentCommandAction::Occurrence { occurrence } => {
            AgentCommandSource::Occurrence(AgentOccurrenceSource {
                session: session_or_genesis(state, &occurrence.session_id)?,
                current: state
                    .occurrences
                    .get(&(
                        occurrence.session_id.clone(),
                        occurrence.occurrence_id.clone(),
                    ))
                    .cloned(),
            })
        }
        AgentCommandAction::Stream(stream) => {
            AgentCommandSource::Stream(Box::new(resolve_stream_source(state, stream)?))
        }
        AgentCommandAction::Input(_) | AgentCommandAction::Workspace(_) => {
            return Err(AgentError::Validation(
                "ephemeral Agent persistence does not emulate M1-coupled commands".to_owned(),
            ));
        }
    })
}

fn ephemeral_target_claim_sources(
    state: &EphemeralAgentState,
    session_id: &str,
    update: &AgentUpdate,
    entry: &AgentSessionEntrySource,
) -> AgentResult<Vec<AgentTargetClaimSource>> {
    let targets = match (update, entry) {
        (AgentUpdate::Message { message, .. }, AgentSessionEntrySource::Message { .. }) => {
            vec![AgentTargetClaimTarget::Message {
                message_id: message.message_id.clone(),
            }]
        }
        (AgentUpdate::Tool { tool, .. }, AgentSessionEntrySource::Tool { .. }) => {
            vec![AgentTargetClaimTarget::Tool {
                tool_call_id: tool.tool_call_id.clone(),
            }]
        }
        (
            AgentUpdate::State {
                state: cymule_profile_protocol::agent::AgentState::Closed,
                ..
            },
            AgentSessionEntrySource::Close { tools },
        ) => tools
            .iter()
            .map(|tool| AgentTargetClaimTarget::Tool {
                tool_call_id: tool.tool.tool_call_id.clone(),
            })
            .collect(),
        (
            AgentUpdate::State { .. } | AgentUpdate::Plan { .. } | AgentUpdate::Usage { .. },
            AgentSessionEntrySource::Metadata,
        ) => Vec::new(),
        _ => {
            return Err(AgentError::persistence(
                "ephemeral_agent_target_claim_source_mismatch",
                "Agent update and entry source disagree before target-claim resolution",
            ));
        }
    };
    let mut keyed = targets
        .into_iter()
        .map(|target| {
            Ok((
                cymule_profile_protocol::agent::agent_target_claim_key(session_id, &target)?,
                target,
            ))
        })
        .collect::<AgentResult<Vec<_>>>()?;
    keyed.sort_by(|left, right| left.0.cmp(&right.0));
    Ok(keyed
        .into_iter()
        .map(|(_, target)| AgentTargetClaimSource {
            current: state
                .target_claims
                .get(&(session_id.to_owned(), target.clone()))
                .cloned(),
            target,
        })
        .collect())
}

fn resolve_stream_source(
    state: &EphemeralAgentState,
    command: &AgentStreamCommand,
) -> AgentResult<AgentStreamSource> {
    let session_id = command.session_id().to_owned();
    let stream_id = command.stream_id().to_owned();
    let stream = state
        .streams
        .get(&(session_id.clone(), stream_id.clone()))
        .cloned();
    Ok(match command {
        AgentStreamCommand::Open { target, .. } => AgentStreamSource::Open {
            session: session_or_genesis(state, &session_id)?,
            stream,
            target: stream_target_source(state, &session_id, target),
        },
        AgentStreamCommand::AppendChunk { chunk, .. } => AgentStreamSource::AppendChunk {
            stream: stream.ok_or_else(|| {
                AgentError::NotFound(format!("Agent stream {stream_id} does not exist"))
            })?,
            current_chunk: state
                .stream_chunks
                .get(&(session_id, stream_id, chunk.sequence))
                .cloned(),
        },
        AgentStreamCommand::Abort { .. } => {
            let stream = stream.ok_or_else(|| {
                AgentError::NotFound(format!("Agent stream {stream_id} does not exist"))
            })?;
            let target_claim = if stream.publication_reservation.is_some() {
                let target = AgentTargetClaimTarget::from_stream_target(&stream.target);
                state
                    .target_claims
                    .get(&(session_id.clone(), target))
                    .cloned()
                    .map(Box::new)
            } else {
                None
            };
            AgentStreamSource::Abort {
                session: state.sessions.get(&session_id).cloned().ok_or_else(|| {
                    AgentError::NotFound(format!("Session {session_id} does not exist"))
                })?,
                target_claim,
                stream,
                resource: None,
            }
        }
        AgentStreamCommand::Finalize { .. } => {
            let stream = stream.ok_or_else(|| {
                AgentError::NotFound(format!("Agent stream {stream_id} does not exist"))
            })?;
            if matches!(
                stream.delivery,
                cymule_profile_protocol::agent::AgentStreamDelivery::ExternalResource { .. }
            ) {
                return Err(AgentError::Validation(
                    "ephemeral Agent persistence cannot invoke an external Resource authority"
                        .to_owned(),
                ));
            }
            let chunks = (0..stream.next_chunk_sequence)
                .map(|sequence| {
                    state
                        .stream_chunks
                        .get(&(session_id.clone(), stream_id.clone(), sequence))
                        .cloned()
                        .ok_or_else(|| {
                            AgentError::persistence(
                                "agent_stream_chunk_missing",
                                format!("Agent stream {stream_id} chunk {sequence} is missing"),
                            )
                        })
                })
                .collect::<AgentResult<Vec<_>>>()?;
            let target_claim_target = AgentTargetClaimTarget::from_stream_target(&stream.target);
            AgentStreamSource::Finalize {
                session: state.sessions.get(&session_id).cloned().ok_or_else(|| {
                    AgentError::NotFound(format!("Session {session_id} does not exist"))
                })?,
                target: stream_target_source(state, &session_id, &stream.target),
                update: state
                    .updates
                    .get(&(
                        session_id.clone(),
                        cymule_profile_protocol::agent::agent_stream_final_update_id(
                            &session_id,
                            &stream_id,
                        )?,
                    ))
                    .cloned(),
                stream,
                chunks,
                resource: None,
                target_claim: state
                    .target_claims
                    .get(&(session_id, target_claim_target))
                    .cloned()
                    .map(Box::new),
            }
        }
    })
}

fn session_or_genesis(
    state: &EphemeralAgentState,
    session_id: &str,
) -> AgentResult<AgentSessionCurrent> {
    match state.sessions.get(session_id) {
        Some(session) => Ok(session.clone()),
        None => AgentSessionCurrent::new(session_id).map_err(Into::into),
    }
}

fn stream_target_source(
    state: &EphemeralAgentState,
    session_id: &str,
    target: &AgentStreamTarget,
) -> AgentStreamTargetSource {
    match target {
        AgentStreamTarget::Message { message_id, .. } => AgentStreamTargetSource::Message {
            current: state
                .messages
                .get(&(session_id.to_owned(), message_id.clone()))
                .cloned(),
        },
        AgentStreamTarget::Tool { tool_call_id } => AgentStreamTargetSource::Tool {
            current: state
                .tools
                .get(&(session_id.to_owned(), tool_call_id.clone()))
                .cloned(),
        },
    }
}

fn reduce_ephemeral(
    command: &AgentCommand,
    source: &cymule_profile_protocol::agent::AgentCommandSource,
) -> AgentResult<AgentCommandOutcome> {
    use cymule_profile_protocol::agent::AgentCommandSource;

    Ok(match (&command.action, source) {
        (
            AgentCommandAction::SessionUpdate { update, .. },
            AgentCommandSource::Session {
                session,
                update: source,
            },
        ) => AgentCommandOutcome::Session(session.reduce_update(
            &command.command_id,
            update,
            source,
        )?),
        (AgentCommandAction::Occurrence { occurrence }, AgentCommandSource::Occurrence(source)) => {
            AgentCommandOutcome::Occurrence(source.reduce(&command.command_id, occurrence)?)
        }
        (AgentCommandAction::Stream(stream), AgentCommandSource::Stream(source)) => {
            AgentCommandOutcome::Stream(source.reduce(&command.command_id, stream)?)
        }
        _ => {
            return Err(AgentError::Validation(
                "ephemeral Agent command/source shape mismatch".to_owned(),
            ));
        }
    })
}

fn ephemeral_target_claim_transitions(
    command: &AgentCommand,
    source: &cymule_profile_protocol::agent::AgentCommandSource,
    outcome: &AgentCommandOutcome,
) -> AgentResult<Vec<AgentTargetClaimTransition>> {
    use cymule_profile_protocol::agent::AgentCommandSource;

    match (source, outcome) {
        (
            AgentCommandSource::Session { session, update },
            AgentCommandOutcome::Session(postcondition),
        ) => cymule_profile_protocol::agent::agent_session_target_claim_transitions(
            command,
            session,
            update,
            postcondition,
        )
        .map_err(Into::into),
        (AgentCommandSource::Stream(source), AgentCommandOutcome::Stream(postcondition)) => Ok(
            cymule_profile_protocol::agent::agent_stream_target_claim_transition(
                command,
                source,
                postcondition,
            )?
            .into_iter()
            .collect(),
        ),
        _ => Ok(Vec::new()),
    }
}

fn verify_ephemeral_target_claim_receipt(
    state: &EphemeralAgentState,
    command: &AgentCommand,
    receipt: &AgentCommandReceipt,
) -> AgentResult<()> {
    for transition in
        ephemeral_target_claim_transitions(command, &receipt.source, &receipt.outcome)?
    {
        let retained = state.target_claims.get(&(
            transition.current.session_id.clone(),
            transition.current.target.clone(),
        ));
        if retained != Some(&transition.current) {
            return Err(AgentError::persistence(
                "ephemeral_agent_target_claim_replay_mismatch",
                "Agent receipt target claim changed after its process-local commit",
            ));
        }
    }
    Ok(())
}

fn apply_outcome(
    state: &mut EphemeralAgentState,
    command: &AgentCommand,
    source: &cymule_profile_protocol::agent::AgentCommandSource,
    outcome: AgentCommandOutcome,
) -> AgentResult<()> {
    let transitions = ephemeral_target_claim_transitions(command, source, &outcome)?;
    for transition in &transitions {
        let retained = state.target_claims.get(&(
            transition.current.session_id.clone(),
            transition.current.target.clone(),
        ));
        if retained != transition.source.as_ref() {
            return Err(AgentError::persistence(
                "ephemeral_agent_target_claim_source_changed",
                "Agent target claim no longer matches its exact source generation",
            ));
        }
    }
    match outcome {
        AgentCommandOutcome::Session(postcondition) => apply_session(state, postcondition),
        AgentCommandOutcome::Occurrence(postcondition) => {
            state.sessions.insert(
                postcondition.session.session_id.clone(),
                postcondition.session.clone(),
            );
            let current = postcondition.current;
            let key = (
                current.occurrence.session_id.clone(),
                current.occurrence.occurrence_id.clone(),
            );
            let unresolved_key = (current.occurrence.session_id.clone(), current.ordinal);
            if current.occurrence.is_terminal() {
                state.unresolved_occurrences.remove(&unresolved_key);
            } else {
                state
                    .unresolved_occurrences
                    .insert(unresolved_key, current.occurrence.occurrence_id.clone());
            }
            state.occurrences.insert(key, current);
        }
        AgentCommandOutcome::Stream(postcondition) => {
            let session_id = postcondition.stream.session_id.clone();
            let stream_id = postcondition.stream.stream_id.clone();
            match postcondition.effect {
                AgentStreamEffect::Opened { session }
                | AgentStreamEffect::Aborted { session, .. } => {
                    state.sessions.insert(session.session_id.clone(), session);
                }
                AgentStreamEffect::Chunk { current } => {
                    state.stream_chunks.insert(
                        (
                            current.session_id.clone(),
                            current.stream_id.clone(),
                            current.chunk.sequence,
                        ),
                        current,
                    );
                }
                AgentStreamEffect::Finalized { session, .. } => {
                    apply_session(state, *session);
                }
            }
            state
                .streams
                .insert((session_id, stream_id), postcondition.stream);
        }
        AgentCommandOutcome::Input(_) | AgentCommandOutcome::Workspace(_) => {
            return Err(AgentError::Validation(
                "ephemeral Agent persistence received an M1-only outcome".to_owned(),
            ));
        }
    }
    for transition in transitions {
        state.target_claims.insert(
            (
                transition.current.session_id.clone(),
                transition.current.target.clone(),
            ),
            transition.current,
        );
    }
    Ok(())
}

fn apply_session(state: &mut EphemeralAgentState, postcondition: AgentSessionPostcondition) {
    let session_id = postcondition.session.session_id.clone();
    state.updates.insert(
        (session_id.clone(), postcondition.update.update_id.clone()),
        postcondition.update,
    );
    match postcondition.effect {
        AgentSessionUpdateEffect::Metadata => {}
        AgentSessionUpdateEffect::Closed { tools } => {
            for current in tools {
                state.tools.insert(
                    (session_id.clone(), current.tool.tool_call_id.clone()),
                    current,
                );
            }
        }
        AgentSessionUpdateEffect::Message { current } => {
            state.message_order.insert(
                (session_id.clone(), current.order.index),
                current.message.message_id.clone(),
            );
            state.messages.insert(
                (session_id.clone(), current.message.message_id.clone()),
                current,
            );
        }
        AgentSessionUpdateEffect::Tool { current } => {
            state.tools.insert(
                (session_id.clone(), current.tool.tool_call_id.clone()),
                current,
            );
        }
    }
    state.sessions.insert(session_id, postcondition.session);
}

fn message_page_read(
    revision: &str,
    query: &AgentMessagePageQuery,
    entries: Vec<AgentMessageCurrent>,
) -> AgentMessagePageRead {
    let next_end_exclusive = entries
        .first()
        .and_then(|entry| (entry.order.index > 0).then_some(entry.order.index));
    AgentMessagePageRead {
        revision: revision.to_owned(),
        page: AgentMessagePage {
            session_id: query.session_id.clone(),
            expected_message_head: query.expected_message_head.clone(),
            source_message_count: query.source_message_count,
            end_exclusive: query.end_exclusive,
            entries,
            next_end_exclusive,
        },
    }
}

fn occurrence_page_read(
    revision: &str,
    query: &AgentOccurrencePageQuery,
    entries: Vec<AgentOccurrenceCurrent>,
    has_more: bool,
) -> AgentOccurrencePageRead {
    let next_after_ordinal = has_more
        .then(|| entries.last().map(|entry| entry.ordinal))
        .flatten();
    AgentOccurrencePageRead {
        revision: revision.to_owned(),
        page: AgentOccurrencePage {
            session_id: query.session_id.clone(),
            index_generation: query.index_generation.clone(),
            after_ordinal: query.after_ordinal,
            entries,
            next_after_ordinal,
        },
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use cymule_profile_protocol::agent::{
        AgentContextMessageRef, AgentContextScanLimits, AgentMessage, AgentMessageOrderEntry,
        AgentState, ContentBlock, ContextRequest, MAX_AGENT_CONTEXT_SCAN_BYTES, MessageRole,
        SessionStopReason,
    };

    use super::*;

    #[test]
    fn durable_agent_control_is_the_production_agent_persistence_adapter() {
        fn require_agent_persistence<T: AgentPersistence>() {}

        require_agent_persistence::<
            cymule_durable::DurableAgentControl<'static, cymule_durable::MemoryStore>,
        >();
    }

    fn seeded_message_session(
        persistence: &mut EphemeralAgentPersistence,
    ) -> (String, AgentSessionCurrent) {
        let mut revision = persistence
            .read_agent_session(&AgentSessionQuery {
                session_id: "session:pages".to_owned(),
                expected_revision: None,
            })
            .expect("initial Session read succeeds")
            .revision;
        for index in 0..3_u64 {
            let command = AgentCommand::new(
                revision,
                AgentCommandAction::SessionUpdate {
                    session_id: "session:pages".to_owned(),
                    update: AgentUpdate::Message {
                        update_id: format!("update:page:{index}"),
                        message: AgentMessage {
                            message_id: format!("message:page:{index}"),
                            role: MessageRole::Agent,
                            content: vec![ContentBlock::Text {
                                text: format!("page {index}"),
                            }],
                        },
                    },
                },
            )
            .expect("message command seals");
            revision = persistence
                .commit_agent(&command)
                .expect("message command commits")
                .observed_revision;
        }
        let session = persistence
            .read_agent_session(&AgentSessionQuery {
                session_id: "session:pages".to_owned(),
                expected_revision: Some(revision.clone()),
            })
            .expect("final Session read succeeds")
            .current
            .expect("Session current exists");
        (revision, session)
    }

    #[test]
    fn late_replay_returns_stable_receipt_at_current_observed_revision_without_writing() {
        let mut persistence = EphemeralAgentPersistence::default();
        let initial = persistence
            .read_agent_session(&AgentSessionQuery {
                session_id: "session:late-replay".to_owned(),
                expected_revision: None,
            })
            .expect("initial Session read succeeds");
        let first_command = AgentCommand::new(
            initial.revision,
            AgentCommandAction::SessionUpdate {
                session_id: "session:late-replay".to_owned(),
                update: AgentUpdate::State {
                    update_id: "update:late-replay:1".to_owned(),
                    state: AgentState::Running,
                    stop_reason: None,
                },
            },
        )
        .expect("first command seals");
        let first = persistence
            .commit_agent(&first_command)
            .expect("first command commits");
        assert_eq!(
            first.committed_revision.as_ref(),
            Some(&first.observed_revision)
        );
        let immediate_replay = persistence
            .commit_agent(&first_command)
            .expect("same-head replay resolves the retained receipt");
        assert_eq!(immediate_replay.receipt, first.receipt);
        assert_eq!(immediate_replay.observed_revision, first.observed_revision);
        assert_eq!(immediate_replay.committed_revision, None);
        let second_command = AgentCommand::new(
            first.observed_revision.clone(),
            AgentCommandAction::SessionUpdate {
                session_id: "session:late-replay".to_owned(),
                update: AgentUpdate::State {
                    update_id: "update:late-replay:2".to_owned(),
                    state: AgentState::Idle,
                    stop_reason: Some(SessionStopReason::EndTurn),
                },
            },
        )
        .expect("second command seals");
        let second = persistence
            .commit_agent(&second_command)
            .expect("second command advances the head");

        let before = {
            let state = persistence.state().expect("state is readable");
            (
                state.revision.clone(),
                state.receipts.len(),
                state.updates.len(),
                state
                    .sessions
                    .get("session:late-replay")
                    .cloned()
                    .expect("Session current exists"),
            )
        };
        let replay = persistence
            .commit_agent(&first_command)
            .expect("late replay resolves the retained receipt");
        let after = {
            let state = persistence.state().expect("state is readable");
            (
                state.revision.clone(),
                state.receipts.len(),
                state.updates.len(),
                state
                    .sessions
                    .get("session:late-replay")
                    .cloned()
                    .expect("Session current exists"),
            )
        };

        assert_eq!(replay.receipt, first.receipt);
        assert_eq!(replay.observed_revision, second.observed_revision);
        assert_eq!(replay.committed_revision, None);
        assert_eq!(before, after);
    }

    #[test]
    fn concurrent_same_started_command_has_only_one_fresh_acknowledgement() {
        let mut persistence = EphemeralAgentPersistence::default();
        let source = persistence
            .read_agent_session(&AgentSessionQuery {
                session_id: "session:concurrent-start".to_owned(),
                expected_revision: None,
            })
            .unwrap();
        let prepared = cymule_profile_protocol::agent::AgentHostOccurrence::prepare(
            "occurrence:concurrent-start",
            "session:concurrent-start",
            AgentHostRequest::Tool(cymule_profile_protocol::agent::ToolRequest {
                tool_call_id: "tool:concurrent-start".to_owned(),
                operation: "test.write".to_owned(),
                input: serde_json::json!({}),
            }),
            cymule_profile_protocol::agent::AgentHostBinding::standalone(
                "host:test/1",
                "binding:tool/1",
            )
            .unwrap(),
        )
        .unwrap();
        let prepare = AgentCommand::new(
            source.revision,
            AgentCommandAction::Occurrence {
                occurrence: Box::new(prepared.clone()),
            },
        )
        .unwrap();
        let source = persistence.commit_agent(&prepare).unwrap();
        let command = AgentCommand::new(
            source.observed_revision,
            AgentCommandAction::Occurrence {
                occurrence: Box::new(prepared.start().unwrap()),
            },
        )
        .unwrap();
        let mut left = persistence.clone();
        let mut right = persistence.clone();
        let results = std::thread::scope(|scope| {
            let left = scope.spawn(|| left.commit_agent(&command));
            let right = scope.spawn(|| right.commit_agent(&command));
            [left.join().unwrap(), right.join().unwrap()]
        });
        let mut fresh = None;
        for result in results {
            match result {
                Ok(commit) => {
                    commit.verify_for(&command).unwrap();
                    if commit.committed_revision.is_some() {
                        assert!(fresh.replace(commit).is_none(), "only one caller wins");
                    }
                }
                Err(AgentError::Persistence { code, .. }) if code == "ephemeral_agent_busy" => {}
                Err(error) => panic!("unexpected concurrent commit error: {error}"),
            }
        }
        let fresh = fresh.expect("one concurrent caller commits Started");
        let replay = persistence.commit_agent(&command).unwrap();
        replay.verify_for(&command).unwrap();
        assert_eq!(replay.committed_revision, None);
        assert_eq!(replay.receipt, fresh.receipt);
        assert_eq!(replay.observed_revision, fresh.observed_revision);
    }

    fn commit_tool_update(
        persistence: &mut EphemeralAgentPersistence,
        session_id: &str,
        update_id: &str,
        status: cymule_profile_protocol::agent::ToolCallStatus,
    ) -> AgentCommit {
        let source_revision = persistence
            .read_agent_session(&AgentSessionQuery {
                session_id: session_id.to_owned(),
                expected_revision: None,
            })
            .expect("Tool source Session reads")
            .revision;
        let command = AgentCommand::new(
            source_revision,
            AgentCommandAction::SessionUpdate {
                session_id: session_id.to_owned(),
                update: AgentUpdate::Tool {
                    update_id: update_id.to_owned(),
                    tool: cymule_profile_protocol::agent::ToolCall {
                        tool_call_id: "tool:close-persistence".to_owned(),
                        operation: "test.execute".to_owned(),
                        status,
                        input: serde_json::json!({"path": "README.md"}),
                        output: None,
                        locations: vec!["workspace:test".to_owned()],
                    },
                },
            },
        )
        .expect("Tool command seals");
        persistence
            .commit_agent(&command)
            .expect("Tool command commits")
    }

    fn close_state_snapshot(
        persistence: &EphemeralAgentPersistence,
        session_id: &str,
    ) -> (
        String,
        usize,
        usize,
        Option<AgentSessionCurrent>,
        Option<cymule_profile_protocol::agent::AgentToolCurrent>,
    ) {
        let state = persistence.state().expect("ephemeral state reads");
        (
            state.revision.clone(),
            state.receipts.len(),
            state.updates.len(),
            state.sessions.get(session_id).cloned(),
            state
                .tools
                .get(&(session_id.to_owned(), "tool:close-persistence".to_owned()))
                .cloned(),
        )
    }

    fn reject_close_update_id_conflict_and_advance_head(
        persistence: &mut EphemeralAgentPersistence,
        committed: &AgentCommit,
        session_id: &str,
    ) -> AgentCommit {
        let conflicting_update_id = AgentCommand::new(
            committed.observed_revision.clone(),
            AgentCommandAction::SessionUpdate {
                session_id: session_id.to_owned(),
                update: AgentUpdate::State {
                    update_id: "update:session:close".to_owned(),
                    state: AgentState::Running,
                    stop_reason: None,
                },
            },
        )
        .expect("conflicting update-ID command seals");
        let before_conflict = close_state_snapshot(persistence, session_id);
        assert!(persistence.commit_agent(&conflicting_update_id).is_err());
        assert_eq!(
            close_state_snapshot(persistence, session_id),
            before_conflict
        );

        let unrelated = AgentCommand::new(
            committed.observed_revision.clone(),
            AgentCommandAction::SessionUpdate {
                session_id: "session:after-close".to_owned(),
                update: AgentUpdate::State {
                    update_id: "update:after-close:running".to_owned(),
                    state: AgentState::Running,
                    stop_reason: None,
                },
            },
        )
        .expect("unrelated command seals");
        persistence
            .commit_agent(&unrelated)
            .expect("unrelated Session advances the global head")
    }

    #[test]
    fn close_commit_atomically_cancels_tools_conflicts_when_stale_and_replays_without_write() {
        use cymule_profile_protocol::agent::{AgentSessionUpdateEffect, ToolCallStatus};

        let mut persistence = EphemeralAgentPersistence::default();
        let session_id = "session:close-persistence";
        let pending = commit_tool_update(
            &mut persistence,
            session_id,
            "update:tool:pending",
            ToolCallStatus::Pending,
        );
        let stale_close = AgentCommand::new(
            pending.observed_revision.clone(),
            AgentCommandAction::SessionUpdate {
                session_id: session_id.to_owned(),
                update: AgentUpdate::State {
                    update_id: "update:session:close".to_owned(),
                    state: AgentState::Closed,
                    stop_reason: None,
                },
            },
        )
        .expect("stale close command seals");
        let in_progress = commit_tool_update(
            &mut persistence,
            session_id,
            "update:tool:in-progress",
            ToolCallStatus::InProgress,
        );
        let before_stale = close_state_snapshot(&persistence, session_id);
        assert!(matches!(
            persistence.commit_agent(&stale_close),
            Err(AgentError::Persistence { code, .. })
                if code == "ephemeral_agent_revision_conflict"
        ));
        assert_eq!(close_state_snapshot(&persistence, session_id), before_stale);

        let close = AgentCommand::new(
            in_progress.observed_revision,
            AgentCommandAction::SessionUpdate {
                session_id: session_id.to_owned(),
                update: AgentUpdate::State {
                    update_id: "update:session:close".to_owned(),
                    state: AgentState::Closed,
                    stop_reason: None,
                },
            },
        )
        .expect("fresh close command seals");
        let committed = persistence
            .commit_agent(&close)
            .expect("fresh close commits atomically");
        assert_eq!(
            committed.committed_revision.as_ref(),
            Some(&committed.observed_revision)
        );
        let AgentCommandOutcome::Session(postcondition) = &committed.receipt.outcome else {
            panic!("close returns Session postcondition")
        };
        let AgentSessionUpdateEffect::Closed { tools } = &postcondition.effect else {
            panic!("close returns explicit Tool cancellation effect")
        };
        assert_eq!(tools.len(), 1);
        assert_eq!(tools[0].tool.status, ToolCallStatus::Cancelled);
        {
            let state = persistence.state().expect("closed state reads");
            assert_eq!(state.sessions[session_id].state, AgentState::Closed);
            assert!(state.sessions[session_id].nonterminal_tools.is_empty());
            assert_eq!(
                state.tools[&(session_id.to_owned(), "tool:close-persistence".to_owned())]
                    .tool
                    .status,
                ToolCallStatus::Cancelled
            );
        }
        let unrelated = reject_close_update_id_conflict_and_advance_head(
            &mut persistence,
            &committed,
            session_id,
        );
        let before_replay = close_state_snapshot(&persistence, session_id);
        let replay = persistence
            .commit_agent(&close)
            .expect("exact close command replays");
        assert_eq!(replay.committed_revision, None);
        assert_eq!(replay.observed_revision, unrelated.observed_revision);
        assert_eq!(replay.receipt, committed.receipt);
        assert_eq!(
            close_state_snapshot(&persistence, session_id),
            before_replay
        );
    }

    #[test]
    fn message_pages_advance_exactly() {
        let mut persistence = EphemeralAgentPersistence::default();
        let (revision, session) = seeded_message_session(&mut persistence);
        let first_query = AgentMessagePageQuery {
            session_id: session.session_id.clone(),
            expected_message_head: session.message_head.clone(),
            source_message_count: session.message_count,
            end_exclusive: None,
            max_entries: 2,
            max_message_canonical_bytes: MAX_AGENT_PAGE_BYTES as u64,
            max_canonical_bytes: MAX_AGENT_PAGE_BYTES as u64,
            expected_revision: Some(revision.clone()),
        };
        let first = persistence
            .read_agent_messages(&first_query)
            .expect("first backward page reads");
        assert_eq!(
            first
                .page
                .entries
                .iter()
                .map(|entry| entry.order.index)
                .collect::<Vec<_>>(),
            vec![1, 2]
        );
        assert_eq!(first.page.next_end_exclusive, Some(1));
        let second_query = AgentMessagePageQuery {
            end_exclusive: first.page.next_end_exclusive,
            ..first_query.clone()
        };
        let second = persistence
            .read_agent_messages(&second_query)
            .expect("second backward page reads");
        assert_eq!(second.page.entries.len(), 1);
        assert_eq!(second.page.entries[0].order.index, 0);
        assert_eq!(second.page.next_end_exclusive, None);
    }

    #[test]
    fn message_page_reads_a_retained_history_prefix_after_append() {
        let mut persistence = EphemeralAgentPersistence::default();
        let (revision, source) = seeded_message_session(&mut persistence);
        let context = ContextRequest {
            session_id: source.session_id.clone(),
            source_message_head: source.message_head.clone(),
            source_message_count: source.message_count,
            budget: source.message_count,
            scan_limits: AgentContextScanLimits {
                max_entries: source.message_count,
                max_canonical_bytes: MAX_AGENT_CONTEXT_SCAN_BYTES,
            },
        };
        let append = AgentCommand::new(
            revision,
            AgentCommandAction::SessionUpdate {
                session_id: source.session_id.clone(),
                update: AgentUpdate::Message {
                    update_id: "update:page:3".to_owned(),
                    message: AgentMessage {
                        message_id: "message:page:3".to_owned(),
                        role: MessageRole::Agent,
                        content: vec![ContentBlock::Text {
                            text: "page 3".to_owned(),
                        }],
                    },
                },
            },
        )
        .expect("later message command seals");
        let appended_revision = persistence
            .commit_agent(&append)
            .expect("later message commits")
            .observed_revision;

        let page = persistence
            .read_agent_messages(&AgentMessagePageQuery {
                session_id: source.session_id.clone(),
                expected_message_head: source.message_head.clone(),
                source_message_count: source.message_count,
                end_exclusive: None,
                max_entries: MAX_AGENT_PAGE as u64,
                max_message_canonical_bytes: MAX_AGENT_PAGE_BYTES as u64,
                max_canonical_bytes: MAX_AGENT_PAGE_BYTES as u64,
                expected_revision: Some(appended_revision),
            })
            .expect("retained prefix reads from the newer current revision");
        assert_eq!(
            page.page
                .entries
                .iter()
                .map(|entry| entry.order.index)
                .collect::<Vec<_>>(),
            vec![0, 1, 2]
        );
        assert_eq!(page.page.source_message_count, 3);
        assert_eq!(page.page.next_end_exclusive, None);

        let mut reader = PinnedAgentMessageReader::new(&mut persistence, &context)
            .expect("the admitted Context source remains a readable immutable prefix");
        let retained = reader
            .read_previous(MAX_AGENT_PAGE as u64)
            .expect("retained Context prefix reads")
            .expect("retained Context prefix is non-empty");
        assert_eq!(
            retained
                .page
                .entries
                .iter()
                .map(|entry| entry.order.index)
                .collect::<Vec<_>>(),
            vec![0, 1, 2]
        );
        assert!(
            reader
                .read_previous(MAX_AGENT_PAGE as u64)
                .expect("retained Context prefix reaches its exact beginning")
                .is_none()
        );
        drop(reader);

        let mut wrong_head = context;
        wrong_head.source_message_head = Some("sha256:".to_owned() + &"f".repeat(64));
        let mut reader = PinnedAgentMessageReader::new(&mut persistence, &wrong_head)
            .expect("a retained count is resolved only by the first exact page read");
        let error = reader
            .read_previous(MAX_AGENT_PAGE as u64)
            .expect_err("a wrong retained prefix head fails before a snapshot can be selected");
        assert!(matches!(
            error,
            AgentError::Persistence { ref code, .. }
                if code == "agent_message_source_stale"
        ));
    }

    #[test]
    fn message_page_rejects_corrupt_source_membership() {
        let mut persistence = EphemeralAgentPersistence::default();
        let corruptor = persistence.clone();
        let (revision, session) = seeded_message_session(&mut persistence);
        let source_index = session.message_count - 1;
        {
            let mut state = corruptor.state().expect("ephemeral state can be corrupted");
            let message_id = state
                .message_order
                .get(&(session.session_id.clone(), source_index))
                .expect("source message membership exists")
                .clone();
            let mut foreign = state
                .messages
                .get(&(session.session_id.clone(), message_id.clone()))
                .expect("source message current exists")
                .clone();
            foreign.session_id = "session:foreign-source".to_owned();
            foreign.order.session_id = foreign.session_id.clone();
            foreign.order.head = content_id(
                "cymule.agent-message-order-head/1",
                &(
                    foreign.session_id.as_str(),
                    foreign.order.index,
                    foreign.message.message_id.as_str(),
                    foreign.order.message_digest.as_str(),
                    foreign.order.previous_head.as_deref(),
                    foreign.order.admitted_by.as_str(),
                ),
            )
            .expect("foreign source head derives");
            foreign
                .verify()
                .expect("corrupt map member is internally valid");
            state
                .messages
                .insert((session.session_id.clone(), message_id), foreign);
        }

        let error = persistence
            .read_agent_messages(&AgentMessagePageQuery {
                session_id: session.session_id,
                expected_message_head: session.message_head,
                source_message_count: session.message_count,
                end_exclusive: None,
                max_entries: MAX_AGENT_PAGE as u64,
                max_message_canonical_bytes: MAX_AGENT_PAGE_BYTES as u64,
                max_canonical_bytes: MAX_AGENT_PAGE_BYTES as u64,
                expected_revision: Some(revision),
            })
            .expect_err("source terminal current must match its external Session membership");
        assert!(matches!(
            error,
            AgentError::Persistence { ref code, .. }
                if code == "agent_message_source_membership_mismatch"
        ));
    }

    fn scanned_message_indexes(
        persistence: &mut EphemeralAgentPersistence,
        request: &ContextRequest,
        page_size: u64,
    ) -> BTreeSet<u64> {
        let mut reader = PinnedAgentMessageReader::new(persistence, request)
            .expect("context reader pins the exact Session descriptor");
        let mut indexes = BTreeSet::new();
        while let Some(page) = reader
            .read_previous(page_size)
            .expect("partitioned context page reads within the exact cumulative budget")
        {
            indexes.extend(page.page.entries.iter().map(|entry| entry.order.index));
        }
        indexes
    }

    #[test]
    fn context_message_budget_is_independent_of_page_partition() {
        let mut persistence = EphemeralAgentPersistence::default();
        let (revision, session) = seeded_message_session(&mut persistence);
        let full_page = persistence
            .read_agent_messages(&AgentMessagePageQuery {
                session_id: session.session_id.clone(),
                expected_message_head: session.message_head.clone(),
                source_message_count: session.message_count,
                end_exclusive: None,
                max_entries: MAX_AGENT_PAGE as u64,
                max_message_canonical_bytes: MAX_AGENT_PAGE_BYTES as u64,
                max_canonical_bytes: MAX_AGENT_PAGE_BYTES as u64,
                expected_revision: Some(revision),
            })
            .expect("complete message prefix reads for exact budget measurement");
        let exact_message_bytes = full_page
            .page
            .entries
            .iter()
            .map(|entry| {
                u64::try_from(
                    cymule_core::canonical_bytes(entry)
                        .expect("message current canonicalizes")
                        .len(),
                )
                .expect("bounded message current length fits u64")
            })
            .sum();
        let request = ContextRequest {
            session_id: session.session_id,
            source_message_head: session.message_head,
            source_message_count: session.message_count,
            budget: session.message_count,
            scan_limits: AgentContextScanLimits {
                max_entries: session.message_count,
                max_canonical_bytes: exact_message_bytes,
            },
        };

        let mut one_at_a_time = persistence.clone();
        let one = scanned_message_indexes(&mut one_at_a_time, &request, 1);
        let all = scanned_message_indexes(&mut persistence, &request, MAX_AGENT_PAGE as u64);
        assert_eq!(one, all);
        assert_eq!(all, BTreeSet::from([0, 1, 2]));
    }

    #[test]
    fn context_scan_budget_cannot_reset() {
        let mut persistence = EphemeralAgentPersistence::default();
        let (_, session) = seeded_message_session(&mut persistence);
        let request = ContextRequest {
            session_id: session.session_id,
            source_message_head: session.message_head,
            source_message_count: session.message_count,
            budget: 2,
            scan_limits: AgentContextScanLimits {
                max_entries: 2,
                max_canonical_bytes: MAX_AGENT_CONTEXT_SCAN_BYTES,
            },
        };
        let mut reader = PinnedAgentMessageReader::new(&mut persistence, &request)
            .expect("context reader pins the exact head");
        assert_eq!(
            reader
                .read_previous(1)
                .expect("first context page reads")
                .expect("first page exists")
                .page
                .entries
                .len(),
            1
        );
        assert_eq!(
            reader
                .read_previous(1)
                .expect("second context page reads")
                .expect("second page exists")
                .page
                .entries
                .len(),
            1
        );
        assert!(reader.read_previous(1).is_err());
        drop(reader);

        let wide_request = ContextRequest {
            session_id: request.session_id,
            source_message_head: request.source_message_head,
            source_message_count: request.source_message_count,
            budget: 3,
            scan_limits: AgentContextScanLimits {
                max_entries: cymule_profile_protocol::agent::MAX_AGENT_CONTEXT_SCAN_ENTRIES,
                max_canonical_bytes: MAX_AGENT_CONTEXT_SCAN_BYTES,
            },
        };
        let mut wide_reader = PinnedAgentMessageReader::new(&mut persistence, &wide_request)
            .expect("wide context reader pins the exact head");
        assert_eq!(
            wide_reader
                .read_previous(cymule_profile_protocol::agent::MAX_AGENT_CONTEXT_SCAN_ENTRIES)
                .expect("per-page entry limit is capped without resetting the scan")
                .expect("wide page exists")
                .page
                .entries
                .len(),
            3
        );
    }

    #[test]
    fn context_snapshot_can_select_only_exact_messages_read_from_its_pinned_head() {
        let mut persistence = EphemeralAgentPersistence::default();
        let (_, session) = seeded_message_session(&mut persistence);
        let request = ContextRequest {
            session_id: session.session_id,
            source_message_head: session.message_head,
            source_message_count: session.message_count,
            budget: 1,
            scan_limits: AgentContextScanLimits {
                max_entries: 1,
                max_canonical_bytes: MAX_AGENT_CONTEXT_SCAN_BYTES,
            },
        };
        let mut reader = PinnedAgentMessageReader::new(&mut persistence, &request)
            .expect("context reader pins the exact head");
        let page = reader
            .read_previous(1)
            .expect("one context page reads")
            .expect("the message page exists");
        let current = page.page.entries.last().expect("one message was read");
        let selected = AgentContextMessageRef::from_current(current)
            .expect("selection reference derives from the exact delivered current");
        let snapshot = ContextSnapshot {
            snapshot_id: "snapshot:reader-authority".to_owned(),
            source_message_head: request.source_message_head.clone(),
            source_message_count: request.source_message_count,
            selected_messages: vec![selected.clone()],
            content: Vec::new(),
            occurrence_binding: "binding:context-reader/1".to_owned(),
        };
        reader
            .verify_snapshot(&snapshot)
            .expect("an exact delivered message may be selected");

        let mut forged_digest = snapshot.clone();
        forged_digest.selected_messages[0].message_digest = "sha256:".to_owned() + &"a".repeat(64);
        assert!(reader.verify_snapshot(&forged_digest).is_err());

        let mut forged_id = snapshot.clone();
        forged_id.selected_messages[0].message_id = "message:forged".to_owned();
        assert!(reader.verify_snapshot(&forged_id).is_err());

        let mut forged_index = snapshot.clone();
        forged_index.selected_messages[0].index -= 1;
        assert!(reader.verify_snapshot(&forged_index).is_err());

        let mut unread = snapshot.clone();
        unread.selected_messages[0].index = 0;
        unread.selected_messages[0].message_id = "message:page:0".to_owned();
        assert!(reader.verify_snapshot(&unread).is_err());

        let mut crossed_count = snapshot.clone();
        crossed_count.source_message_count -= 1;
        assert!(reader.verify_snapshot(&crossed_count).is_err());

        let mut crossed_head = snapshot;
        crossed_head.source_message_head = Some("sha256:".to_owned() + &"b".repeat(64));
        assert!(reader.verify_snapshot(&crossed_head).is_err());
    }

    #[test]
    fn context_scan_rejects_a_disconnected_older_page() {
        let mut persistence = EphemeralAgentPersistence::default();
        let corruptor = persistence.clone();
        let (_, session) = seeded_message_session(&mut persistence);
        let request = ContextRequest {
            session_id: session.session_id.clone(),
            source_message_head: session.message_head,
            source_message_count: session.message_count,
            budget: 3,
            scan_limits: AgentContextScanLimits {
                max_entries: 3,
                max_canonical_bytes: MAX_AGENT_CONTEXT_SCAN_BYTES,
            },
        };
        let mut reader = PinnedAgentMessageReader::new(&mut persistence, &request)
            .expect("context reader pins the exact head");
        reader
            .read_previous(2)
            .expect("newest context page reads")
            .expect("newest context page exists");

        let replacement_message = AgentMessage {
            message_id: "message:disconnected-page:0".to_owned(),
            role: MessageRole::Agent,
            content: vec![ContentBlock::Text {
                text: "valid but disconnected history".to_owned(),
            }],
        };
        let replacement_digest = content_id("cymule.agent-message-current/1", &replacement_message)
            .expect("replacement message digest computes");
        let admitted_by = content_id("cymule.test-agent-command/1", &"disconnected older page")
            .expect("replacement command identity computes");
        let replacement_head = content_id(
            "cymule.agent-message-order-head/1",
            &(
                session.session_id.as_str(),
                0_u64,
                replacement_message.message_id.as_str(),
                replacement_digest.as_str(),
                Option::<&str>::None,
                admitted_by.as_str(),
            ),
        )
        .expect("replacement order head computes");
        let replacement = AgentMessageCurrent {
            session_id: session.session_id.clone(),
            order: AgentMessageOrderEntry {
                session_id: session.session_id.clone(),
                index: 0,
                message_id: replacement_message.message_id.clone(),
                message_digest: replacement_digest,
                previous_head: None,
                head: replacement_head,
                admitted_by,
            },
            message: replacement_message,
        };
        replacement
            .verify()
            .expect("replacement page entry is independently valid");
        {
            let mut state = corruptor.state().expect("ephemeral state can be corrupted");
            state.message_order.insert(
                (session.session_id.clone(), 0),
                replacement.message.message_id.clone(),
            );
            state.messages.insert(
                (
                    session.session_id.clone(),
                    replacement.message.message_id.clone(),
                ),
                replacement,
            );
        }

        let error = reader
            .read_previous(1)
            .expect_err("a locally valid but disconnected older page must fail closed");
        assert!(matches!(
            error,
            AgentError::Persistence { ref code, .. }
                if code == "agent_context_message_page_chain_mismatch"
        ));
    }
}
