use cymule_core::canonical_digest;
use cymule_durable::{DurableCoordinator, DurableStore, JournalBatch, JournalRecord};
use serde::{Deserialize, Serialize};

use crate::{
    AgentError, AgentJournal, AgentMessage, AgentResult, AgentSession, AgentUpdate, ContentBlock,
    MessageRole, ToolCallStatus, journal::agent_update_record,
};

/// Durable stream-record schema stored in an M1 application journal.
pub const AGENT_STREAM_SCHEMA: &str = "cymule.agent-stream/1";
/// Maximum canonical JSON bytes retained in one stream chunk.
pub const AGENT_STREAM_CHUNK_LIMIT: usize = 1024 * 1024;

/// Final Session object produced by one stream.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum AgentStreamTarget {
    /// One finalized Session message.
    Message {
        /// Stable message identity shared by all chunks.
        message_id: String,
        /// Final message author.
        role: MessageRole,
    },
    /// Final output of one already in-progress tool call.
    Tool {
        /// Stable tool-call identity.
        tool_call_id: String,
    },
}

/// One ordered, non-final stream chunk.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentStreamChunk {
    /// Zero-based contiguous sequence.
    pub sequence: u64,
    /// Protocol-neutral content fragments.
    pub content: Vec<ContentBlock>,
}

/// Durable stream lifecycle record.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "record", rename_all = "snake_case")]
pub enum AgentStreamRecord {
    /// Admit one caller-identified stream before chunks arrive.
    Opened {
        /// Stable stream identity.
        stream_id: String,
        /// Owning Session.
        session_id: String,
        /// Immutable final target.
        target: AgentStreamTarget,
    },
    /// Append one contiguous staging chunk.
    Chunk {
        /// Stable stream identity.
        stream_id: String,
        /// Owning Session.
        session_id: String,
        /// Ordered chunk.
        chunk: AgentStreamChunk,
    },
    /// Atomically publish the final Session update.
    Finalized {
        /// Stable stream identity.
        stream_id: String,
        /// Owning Session.
        session_id: String,
        /// Digest of the ordered finalized content blocks.
        content_digest: String,
        /// Exact update committed to the Session journal in the same M1 CAS.
        update: Box<AgentUpdate>,
    },
    /// Permanently discard staged chunks without publishing Session content.
    Aborted {
        /// Stable stream identity.
        stream_id: String,
        /// Owning Session.
        session_id: String,
        /// Stable non-empty abort reason.
        reason: String,
    },
}

impl AgentStreamRecord {
    /// Stable idempotency identity within the stream journal.
    pub fn record_id(&self) -> String {
        match self {
            Self::Opened { stream_id, .. } => format!("{stream_id}:opened"),
            Self::Chunk {
                stream_id, chunk, ..
            } => format!("{stream_id}:chunk:{}", chunk.sequence),
            Self::Finalized { stream_id, .. } => format!("{stream_id}:finalized"),
            Self::Aborted { stream_id, .. } => format!("{stream_id}:aborted"),
        }
    }

    fn stream_id(&self) -> &str {
        match self {
            Self::Opened { stream_id, .. }
            | Self::Chunk { stream_id, .. }
            | Self::Finalized { stream_id, .. }
            | Self::Aborted { stream_id, .. } => stream_id,
        }
    }

    fn session_id(&self) -> &str {
        match self {
            Self::Opened { session_id, .. }
            | Self::Chunk { session_id, .. }
            | Self::Finalized { session_id, .. }
            | Self::Aborted { session_id, .. } => session_id,
        }
    }
}

/// Stream lifecycle state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AgentStreamState {
    /// Chunks may still be appended.
    Open,
    /// One final Session update is durable.
    Finalized,
    /// No final Session update will be published.
    Aborted,
}

/// Rebuildable projection of one staged stream.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentStreamProjection {
    /// Stable stream identity.
    pub stream_id: String,
    /// Owning Session.
    pub session_id: String,
    /// Immutable final target.
    pub target: AgentStreamTarget,
    /// Ordered staging chunks.
    pub chunks: Vec<AgentStreamChunk>,
    /// Current lifecycle.
    pub state: AgentStreamState,
    /// Final Session update, when published.
    pub final_update: Option<AgentUpdate>,
    /// Final content digest, when published.
    pub content_digest: Option<String>,
    /// Abort reason, when discarded.
    pub abort_reason: Option<String>,
}

impl AgentStreamProjection {
    /// Replay one stream from its durable ordered records.
    ///
    /// # Errors
    ///
    /// Returns an error for missing open, mixed identities, out-of-order or
    /// conflicting chunks, illegal terminal transitions, or invalid final data.
    pub fn replay(records: impl IntoIterator<Item = AgentStreamRecord>) -> AgentResult<Self> {
        let mut projection = None;
        for record in records {
            apply_record(&mut projection, record)?;
        }
        projection.ok_or_else(|| AgentError::NotFound("agent stream has no open record".to_owned()))
    }

    /// Flatten staging chunks into the exact final block order.
    pub fn finalized_content(&self) -> Vec<ContentBlock> {
        self.chunks
            .iter()
            .flat_map(|chunk| chunk.content.iter().cloned())
            .collect()
    }
}

/// Result of one durable stream transition.
#[derive(Debug, Clone, PartialEq)]
pub struct AgentStreamCheckpoint {
    /// Replayed stream projection after the checkpoint.
    pub stream: AgentStreamProjection,
    /// Replayed Session after finalization, otherwise absent.
    pub session: Option<AgentSession>,
    /// Whole-state M1 revision containing the transition.
    pub revision: String,
}

/// M2 streaming staging and explicit finalization over M1 CAS journals.
pub struct AgentStreamController;

impl AgentStreamController {
    /// Open one caller-identified stream.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid/reused identities, an already finalized
    /// message, a missing/non-running tool target, or persistence failure.
    pub fn open<S: DurableStore>(
        coordinator: &mut DurableCoordinator<S>,
        session_id: &str,
        stream_id: &str,
        target: AgentStreamTarget,
    ) -> AgentResult<AgentStreamCheckpoint> {
        validate_identity("Session", session_id)?;
        validate_identity("stream", stream_id)?;
        validate_target(&target)?;
        if let Some(existing) = load_stream(coordinator, session_id, stream_id)? {
            if existing.target != target {
                return Err(AgentError::IllegalTransition(format!(
                    "stream {stream_id} was reused with a different target"
                )));
            }
            let session = if existing.state == AgentStreamState::Finalized {
                let session = load_session(coordinator, session_id)?;
                verify_final_session(&existing, &session)?;
                Some(session)
            } else {
                None
            };
            return checkpoint(existing, session, coordinator);
        }
        let session = load_session(coordinator, session_id)?;
        validate_new_target(&session, &target)?;
        let opened = AgentStreamRecord::Opened {
            stream_id: stream_id.to_owned(),
            session_id: session_id.to_owned(),
            target,
        };
        let revision = coordinator
            .append_journal_record(&stream_journal_id(session_id), stream_record(&opened)?)
            .map_err(persistence)?;
        Ok(AgentStreamCheckpoint {
            stream: AgentStreamProjection::replay([opened])?,
            session: None,
            revision,
        })
    }

    /// Append one contiguous staging chunk without publishing Session output.
    ///
    /// # Errors
    ///
    /// Returns an error for a missing/terminal stream, out-of-order or
    /// conflicting sequence, invalid content, stale CAS, or persistence failure.
    pub fn append<S: DurableStore>(
        coordinator: &mut DurableCoordinator<S>,
        session_id: &str,
        stream_id: &str,
        chunk: AgentStreamChunk,
    ) -> AgentResult<AgentStreamCheckpoint> {
        validate_chunk(&chunk)?;
        let stream = load_required_stream(coordinator, session_id, stream_id)?;
        if stream.state != AgentStreamState::Open {
            return Err(AgentError::IllegalTransition(format!(
                "stream {stream_id} is {:?}",
                stream.state
            )));
        }
        let expected = u64::try_from(stream.chunks.len())
            .map_err(|error| AgentError::Validation(error.to_string()))?;
        if chunk.sequence < expected {
            let existing = &stream.chunks[usize::try_from(chunk.sequence)
                .map_err(|error| AgentError::Validation(error.to_string()))?];
            if existing == &chunk {
                return checkpoint(stream, None, coordinator);
            }
            return Err(AgentError::IllegalTransition(format!(
                "stream {stream_id} chunk {} conflicts with retained content",
                chunk.sequence
            )));
        }
        if chunk.sequence != expected {
            return Err(AgentError::IllegalTransition(format!(
                "stream {stream_id} expected chunk {expected}, received {}",
                chunk.sequence
            )));
        }
        let record = AgentStreamRecord::Chunk {
            stream_id: stream_id.to_owned(),
            session_id: session_id.to_owned(),
            chunk,
        };
        let revision = coordinator
            .append_journal_record(&stream_journal_id(session_id), stream_record(&record)?)
            .map_err(persistence)?;
        let stream = load_required_stream(coordinator, session_id, stream_id)?;
        Ok(AgentStreamCheckpoint {
            stream,
            session: None,
            revision,
        })
    }

    /// Atomically publish finalized content to the Session journal.
    ///
    /// # Errors
    ///
    /// Returns an error for an empty/aborted stream, changed target state,
    /// invalid final update, stale CAS, or persistence failure.
    pub fn finalize<S: DurableStore>(
        coordinator: &mut DurableCoordinator<S>,
        session_id: &str,
        stream_id: &str,
    ) -> AgentResult<AgentStreamCheckpoint> {
        let stream = load_required_stream(coordinator, session_id, stream_id)?;
        if stream.state == AgentStreamState::Finalized {
            let session = load_session(coordinator, session_id)?;
            verify_final_session(&stream, &session)?;
            return checkpoint(stream, Some(session), coordinator);
        }
        if stream.state == AgentStreamState::Aborted {
            return Err(AgentError::IllegalTransition(format!(
                "stream {stream_id} was aborted"
            )));
        }
        if stream.chunks.is_empty() {
            return Err(AgentError::Validation(
                "agent stream requires at least one chunk before finalization".to_owned(),
            ));
        }
        let mut session = load_session(coordinator, session_id)?;
        let content = stream.finalized_content();
        let update = final_update(&stream, &session, content.clone())?;
        session.apply(update.clone())?;
        let content_digest = canonical_digest(&content)
            .map_err(|error| AgentError::Validation(error.to_string()))?;
        let finalized = AgentStreamRecord::Finalized {
            stream_id: stream_id.to_owned(),
            session_id: session_id.to_owned(),
            content_digest,
            update: Box::new(update.clone()),
        };
        let revision = coordinator
            .checkpoint_journals(&[
                JournalBatch {
                    journal_id: stream_journal_id(session_id),
                    records: vec![stream_record(&finalized)?],
                },
                JournalBatch {
                    journal_id: session_id.to_owned(),
                    records: vec![agent_update_record(&update)?],
                },
            ])
            .map_err(persistence)?;
        let stream = load_required_stream(coordinator, session_id, stream_id)?;
        verify_final_session(&stream, &session)?;
        Ok(AgentStreamCheckpoint {
            stream,
            session: Some(session),
            revision,
        })
    }

    /// Abort staging without publishing message or tool output.
    ///
    /// # Errors
    ///
    /// Returns an error for an empty reason, finalized stream, conflicting
    /// repeated abort, stale CAS, or persistence failure.
    pub fn abort<S: DurableStore>(
        coordinator: &mut DurableCoordinator<S>,
        session_id: &str,
        stream_id: &str,
        reason: &str,
    ) -> AgentResult<AgentStreamCheckpoint> {
        validate_identity("abort reason", reason)?;
        let stream = load_required_stream(coordinator, session_id, stream_id)?;
        match stream.state {
            AgentStreamState::Finalized => {
                return Err(AgentError::IllegalTransition(format!(
                    "finalized stream {stream_id} cannot abort"
                )));
            }
            AgentStreamState::Aborted => {
                return if stream.abort_reason.as_deref() == Some(reason) {
                    checkpoint(stream, None, coordinator)
                } else {
                    Err(AgentError::IllegalTransition(format!(
                        "stream {stream_id} was aborted with a different reason"
                    )))
                };
            }
            AgentStreamState::Open => {}
        }
        let aborted = AgentStreamRecord::Aborted {
            stream_id: stream_id.to_owned(),
            session_id: session_id.to_owned(),
            reason: reason.to_owned(),
        };
        let revision = coordinator
            .append_journal_record(&stream_journal_id(session_id), stream_record(&aborted)?)
            .map_err(persistence)?;
        Ok(AgentStreamCheckpoint {
            stream: load_required_stream(coordinator, session_id, stream_id)?,
            session: None,
            revision,
        })
    }

    /// Load one durable stream projection without changing state.
    ///
    /// # Errors
    ///
    /// Returns an error for a missing or invalid stream journal.
    pub fn load<S: DurableStore>(
        coordinator: &mut DurableCoordinator<S>,
        session_id: &str,
        stream_id: &str,
    ) -> AgentResult<AgentStreamProjection> {
        let stream = load_required_stream(coordinator, session_id, stream_id)?;
        if stream.state == AgentStreamState::Finalized {
            let session = load_session(coordinator, session_id)?;
            verify_final_session(&stream, &session)?;
        }
        Ok(stream)
    }
}

fn apply_record(
    projection: &mut Option<AgentStreamProjection>,
    record: AgentStreamRecord,
) -> AgentResult<()> {
    validate_identity("stream", record.stream_id())?;
    validate_identity("Session", record.session_id())?;
    match (projection.as_mut(), record) {
        (
            None,
            AgentStreamRecord::Opened {
                stream_id,
                session_id,
                target,
            },
        ) => {
            validate_target(&target)?;
            *projection = Some(AgentStreamProjection {
                stream_id,
                session_id,
                target,
                chunks: Vec::new(),
                state: AgentStreamState::Open,
                final_update: None,
                content_digest: None,
                abort_reason: None,
            });
            Ok(())
        }
        (None, _) => Err(AgentError::IllegalTransition(
            "agent stream must begin with opened".to_owned(),
        )),
        (Some(current), record)
            if current.stream_id != record.stream_id()
                || current.session_id != record.session_id() =>
        {
            Err(AgentError::IllegalTransition(
                "agent stream record changed stream or Session identity".to_owned(),
            ))
        }
        (
            Some(current),
            AgentStreamRecord::Chunk {
                chunk,
                stream_id: _,
                session_id: _,
            },
        ) if current.state == AgentStreamState::Open => {
            validate_chunk(&chunk)?;
            let expected = u64::try_from(current.chunks.len())
                .map_err(|error| AgentError::Validation(error.to_string()))?;
            if chunk.sequence != expected {
                return Err(AgentError::IllegalTransition(format!(
                    "agent stream expected chunk {expected}, received {}",
                    chunk.sequence
                )));
            }
            current.chunks.push(chunk);
            Ok(())
        }
        (
            Some(current),
            AgentStreamRecord::Finalized {
                content_digest,
                update,
                stream_id: _,
                session_id: _,
            },
        ) if current.state == AgentStreamState::Open => {
            if current.chunks.is_empty() {
                return Err(AgentError::Validation(
                    "empty agent stream cannot finalize".to_owned(),
                ));
            }
            validate_final_update(current, &update, &content_digest)?;
            current.state = AgentStreamState::Finalized;
            current.final_update = Some(*update);
            current.content_digest = Some(content_digest);
            Ok(())
        }
        (
            Some(current),
            AgentStreamRecord::Aborted {
                reason,
                stream_id: _,
                session_id: _,
            },
        ) if current.state == AgentStreamState::Open => {
            validate_identity("abort reason", &reason)?;
            current.state = AgentStreamState::Aborted;
            current.abort_reason = Some(reason);
            Ok(())
        }
        (Some(_), AgentStreamRecord::Opened { .. }) => Err(AgentError::IllegalTransition(
            "agent stream opened more than once".to_owned(),
        )),
        (Some(current), _) => Err(AgentError::IllegalTransition(format!(
            "agent stream {} is already {:?}",
            current.stream_id, current.state
        ))),
    }
}

fn validate_final_update(
    stream: &AgentStreamProjection,
    update: &AgentUpdate,
    content_digest: &str,
) -> AgentResult<()> {
    let content = stream.finalized_content();
    let expected_digest =
        canonical_digest(&content).map_err(|error| AgentError::Validation(error.to_string()))?;
    if content_digest != expected_digest || update.update_id() != final_update_id(&stream.stream_id)
    {
        return Err(AgentError::Validation(
            "agent stream final identity or content digest does not match".to_owned(),
        ));
    }
    match (&stream.target, update) {
        (AgentStreamTarget::Message { message_id, role }, AgentUpdate::Message { message, .. })
            if &message.message_id == message_id
                && &message.role == role
                && message.content == content =>
        {
            Ok(())
        }
        (AgentStreamTarget::Tool { tool_call_id }, AgentUpdate::Tool { tool, .. })
            if &tool.tool_call_id == tool_call_id
                && tool.status == ToolCallStatus::Completed
                && tool.output.as_ref() == Some(&content) =>
        {
            Ok(())
        }
        _ => Err(AgentError::Validation(
            "agent stream final update does not match its target or chunks".to_owned(),
        )),
    }
}

fn final_update(
    stream: &AgentStreamProjection,
    session: &AgentSession,
    content: Vec<ContentBlock>,
) -> AgentResult<AgentUpdate> {
    let update_id = final_update_id(&stream.stream_id);
    match &stream.target {
        AgentStreamTarget::Message { message_id, role } => Ok(AgentUpdate::Message {
            update_id,
            message: AgentMessage {
                message_id: message_id.clone(),
                role: *role,
                content,
            },
        }),
        AgentStreamTarget::Tool { tool_call_id } => {
            let mut tool = session.tools.get(tool_call_id).cloned().ok_or_else(|| {
                AgentError::NotFound(format!("tool {tool_call_id} does not exist"))
            })?;
            if tool.status != ToolCallStatus::InProgress {
                return Err(AgentError::IllegalTransition(format!(
                    "tool {tool_call_id} is {:?}, not in_progress",
                    tool.status
                )));
            }
            tool.status = ToolCallStatus::Completed;
            tool.output = Some(content);
            Ok(AgentUpdate::Tool { update_id, tool })
        }
    }
}

fn validate_new_target(session: &AgentSession, target: &AgentStreamTarget) -> AgentResult<()> {
    match target {
        AgentStreamTarget::Message { message_id, .. } => {
            if session.messages.contains_key(message_id) {
                return Err(AgentError::IllegalTransition(format!(
                    "message {message_id} is already finalized"
                )));
            }
        }
        AgentStreamTarget::Tool { tool_call_id } => {
            let tool = session.tools.get(tool_call_id).ok_or_else(|| {
                AgentError::NotFound(format!("tool {tool_call_id} does not exist"))
            })?;
            if tool.status != ToolCallStatus::InProgress {
                return Err(AgentError::IllegalTransition(format!(
                    "tool {tool_call_id} is {:?}, not in_progress",
                    tool.status
                )));
            }
        }
    }
    Ok(())
}

fn validate_target(target: &AgentStreamTarget) -> AgentResult<()> {
    match target {
        AgentStreamTarget::Message { message_id, .. } => validate_identity("message", message_id),
        AgentStreamTarget::Tool { tool_call_id } => validate_identity("tool", tool_call_id),
    }
}

fn validate_chunk(chunk: &AgentStreamChunk) -> AgentResult<()> {
    if chunk.content.is_empty() {
        return Err(AgentError::Validation(
            "agent stream chunk content must not be empty".to_owned(),
        ));
    }
    let encoded = cymule_core::canonical_bytes(&chunk.content)
        .map_err(|error| AgentError::Validation(error.to_string()))?;
    if encoded.len() > AGENT_STREAM_CHUNK_LIMIT {
        return Err(AgentError::Validation(format!(
            "agent stream chunk exceeds {AGENT_STREAM_CHUNK_LIMIT} canonical bytes"
        )));
    }
    for block in &chunk.content {
        match block {
            ContentBlock::Text { text } if text.is_empty() => {
                return Err(AgentError::Validation(
                    "agent stream text chunk must not be empty".to_owned(),
                ));
            }
            ContentBlock::Artifact { artifact }
                if artifact.artifact_id.is_empty() || artifact.kind.is_empty() =>
            {
                return Err(AgentError::Validation(
                    "agent stream Artifact reference is invalid".to_owned(),
                ));
            }
            ContentBlock::Resource { uri, .. } if uri.is_empty() => {
                return Err(AgentError::Validation(
                    "agent stream legacy resource URI must not be empty".to_owned(),
                ));
            }
            ContentBlock::ResourceHandle { resource } => resource
                .verify()
                .map_err(|error| AgentError::Validation(error.to_string()))?,
            _ => {}
        }
    }
    Ok(())
}

fn verify_final_session(stream: &AgentStreamProjection, session: &AgentSession) -> AgentResult<()> {
    let update = stream.final_update.as_ref().ok_or_else(|| {
        AgentError::Persistence(format!(
            "finalized stream {} has no final update",
            stream.stream_id
        ))
    })?;
    match update {
        AgentUpdate::Message { message, .. }
            if session.messages.get(&message.message_id) == Some(message) =>
        {
            Ok(())
        }
        AgentUpdate::Tool { tool, .. } if session.tools.get(&tool.tool_call_id) == Some(tool) => {
            Ok(())
        }
        _ => Err(AgentError::Persistence(format!(
            "finalized stream {} is absent from its Session projection",
            stream.stream_id
        ))),
    }
}

fn load_stream<S: DurableStore>(
    coordinator: &DurableCoordinator<S>,
    session_id: &str,
    stream_id: &str,
) -> AgentResult<Option<AgentStreamProjection>> {
    let records = coordinator
        .journal_records(&stream_journal_id(session_id))
        .map_err(persistence)?;
    let mut selected = Vec::new();
    for record in records {
        if record.schema != AGENT_STREAM_SCHEMA {
            return Err(AgentError::Persistence(format!(
                "Session {session_id} stream journal contains unsupported schema {}",
                record.schema
            )));
        }
        record.verify().map_err(persistence)?;
        let stream_record: AgentStreamRecord = serde_json::from_value(record.payload.clone())
            .map_err(|error| AgentError::Persistence(error.to_string()))?;
        if stream_record.record_id() != record.record_id {
            return Err(AgentError::Persistence(format!(
                "stream record {} has mismatched identity",
                record.record_id
            )));
        }
        if stream_record.stream_id() == stream_id {
            selected.push(stream_record);
        }
    }
    if selected.is_empty() {
        Ok(None)
    } else {
        AgentStreamProjection::replay(selected).map(Some)
    }
}

fn load_required_stream<S: DurableStore>(
    coordinator: &DurableCoordinator<S>,
    session_id: &str,
    stream_id: &str,
) -> AgentResult<AgentStreamProjection> {
    load_stream(coordinator, session_id, stream_id)?.ok_or_else(|| {
        AgentError::NotFound(format!(
            "stream {stream_id} does not exist in Session {session_id}"
        ))
    })
}

fn load_session<S: DurableStore>(
    coordinator: &mut DurableCoordinator<S>,
    session_id: &str,
) -> AgentResult<AgentSession> {
    let updates = AgentJournal::load(coordinator, session_id)?;
    AgentSession::replay(session_id, updates)
}

fn stream_record(record: &AgentStreamRecord) -> AgentResult<JournalRecord> {
    let payload =
        serde_json::to_value(record).map_err(|error| AgentError::Persistence(error.to_string()))?;
    JournalRecord::new(record.record_id(), AGENT_STREAM_SCHEMA, payload).map_err(persistence)
}

fn stream_journal_id(session_id: &str) -> String {
    format!("cymule.agent.streams/{session_id}")
}

fn final_update_id(stream_id: &str) -> String {
    format!("update:{stream_id}:finalized")
}

fn checkpoint<S: DurableStore>(
    stream: AgentStreamProjection,
    session: Option<AgentSession>,
    coordinator: &DurableCoordinator<S>,
) -> AgentResult<AgentStreamCheckpoint> {
    Ok(AgentStreamCheckpoint {
        stream,
        session,
        revision: coordinator.revision().map(str::to_owned).ok_or_else(|| {
            AgentError::Persistence("durable state is not initialized".to_owned())
        })?,
    })
}

fn validate_identity(kind: &str, value: &str) -> AgentResult<()> {
    if value.is_empty() || value.len() > 512 || value.chars().any(char::is_control) {
        return Err(AgentError::Validation(format!(
            "agent stream {kind} must contain 1..=512 non-control characters"
        )));
    }
    Ok(())
}

fn persistence(error: impl std::fmt::Display) -> AgentError {
    AgentError::Persistence(error.to_string())
}
