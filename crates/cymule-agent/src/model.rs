//! Versioned agent wire data. Field-level rustdoc is completed alongside the
//! frozen M2 schema; type-level contracts are authoritative during incubation.
#![allow(missing_docs)]

use std::collections::BTreeMap;

use cymule_core::{ArtifactRef, canonical_digest};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{AgentError, AgentResult};

/// Typed content shared by messages, model output, tools, and artifacts.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ContentBlock {
    /// UTF-8 text.
    Text { text: String },
    /// Structured canonical JSON.
    Json { value: Value },
    /// Immutable Cymule artifact.
    Artifact { artifact: ArtifactRef },
    /// URI reference interpreted by an external resource adapter.
    Resource {
        uri: String,
        mime_type: Option<String>,
    },
}

/// Message author role.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MessageRole {
    User,
    Agent,
    Tool,
    System,
}

/// Durable message projection.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentMessage {
    /// Stable message identity.
    pub message_id: String,
    /// Message author role.
    pub role: MessageRole,
    /// Finalized content blocks.
    pub content: Vec<ContentBlock>,
}

/// Session activity state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AgentState {
    Idle,
    Running,
    RequiresAction,
    Closed,
}

/// Terminal foreground stop reason.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SessionStopReason {
    EndTurn,
    Cancelled,
    Refusal,
    Error,
}

/// Plan entry lifecycle.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PlanEntryStatus {
    Pending,
    InProgress,
    Completed,
    Cancelled,
}

/// One user-visible Plan entry.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentPlanEntry {
    pub entry_id: String,
    pub content: String,
    pub status: PlanEntryStatus,
}

/// User-visible agent Plan projection.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentPlan {
    pub plan_id: String,
    pub entries: Vec<AgentPlanEntry>,
}

/// Tool call lifecycle.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ToolCallStatus {
    Pending,
    AwaitingPermission,
    InProgress,
    Completed,
    Failed,
    Cancelled,
}

/// Tool call projection.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ToolCall {
    pub tool_call_id: String,
    pub operation: String,
    pub status: ToolCallStatus,
    pub input: Value,
    pub output: Option<Vec<ContentBlock>>,
    pub locations: Vec<String>,
}

/// Token and monetary usage projection.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Usage {
    pub used: u64,
    pub capacity: u64,
    pub cost: Option<Value>,
}

/// Idempotent ordered update applied to one Session projection.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum AgentUpdate {
    Message {
        update_id: String,
        message: AgentMessage,
    },
    State {
        update_id: String,
        state: AgentState,
        stop_reason: Option<SessionStopReason>,
    },
    Plan {
        update_id: String,
        plan: AgentPlan,
    },
    Tool {
        update_id: String,
        tool: ToolCall,
    },
    Usage {
        update_id: String,
        usage: Usage,
    },
}

impl AgentUpdate {
    /// Stable idempotency identity for this update.
    pub fn update_id(&self) -> &str {
        match self {
            Self::Message { update_id, .. }
            | Self::State { update_id, .. }
            | Self::Plan { update_id, .. }
            | Self::Tool { update_id, .. }
            | Self::Usage { update_id, .. } => update_id,
        }
    }
}

/// Rebuildable agent Session projection.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentSession {
    /// Stable Session identity.
    pub session_id: String,
    /// Current activity state.
    pub state: AgentState,
    /// Finalized messages keyed by agent-owned identity.
    pub messages: BTreeMap<String, AgentMessage>,
    /// Message identities in durable presentation order.
    pub message_order: Vec<String>,
    /// Current user-visible Plan.
    pub plan: Option<AgentPlan>,
    /// Tool calls keyed by identity.
    pub tools: BTreeMap<String, ToolCall>,
    /// Latest cumulative usage report.
    pub usage: Option<Usage>,
    applied_updates: BTreeMap<String, String>,
}

impl AgentSession {
    /// Create an idle Session.
    pub fn new(session_id: impl Into<String>) -> Self {
        Self {
            session_id: session_id.into(),
            state: AgentState::Idle,
            messages: BTreeMap::new(),
            message_order: Vec::new(),
            plan: None,
            tools: BTreeMap::new(),
            usage: None,
            applied_updates: BTreeMap::new(),
        }
    }

    /// Apply one idempotent typed update.
    pub fn apply(&mut self, update: AgentUpdate) -> AgentResult<()> {
        let update_hash =
            canonical_digest(&update).map_err(|error| AgentError::Validation(error.to_string()))?;
        if let Some(existing) = self.applied_updates.get(update.update_id()) {
            return if existing == &update_hash {
                Ok(())
            } else {
                Err(AgentError::IllegalTransition(format!(
                    "update ID {} was reused with different content",
                    update.update_id()
                )))
            };
        }
        let update_id = update.update_id().to_owned();
        match update {
            AgentUpdate::Message { message, .. } => {
                if !self.messages.contains_key(&message.message_id) {
                    self.message_order.push(message.message_id.clone());
                }
                self.messages.insert(message.message_id.clone(), message);
            }
            AgentUpdate::State {
                state, stop_reason, ..
            } => {
                if self.state == AgentState::Closed {
                    return Err(AgentError::IllegalTransition(
                        "closed Session cannot change state".to_owned(),
                    ));
                }
                if state == AgentState::Idle && stop_reason.is_none() {
                    return Err(AgentError::Validation(
                        "idle transition requires stop_reason".to_owned(),
                    ));
                }
                self.state = state;
            }
            AgentUpdate::Plan { plan, .. } => self.plan = Some(plan),
            AgentUpdate::Tool { tool, .. } => {
                if let Some(current) = self.tools.get(&tool.tool_call_id)
                    && !valid_tool_transition(current.status, tool.status)
                {
                    return Err(AgentError::IllegalTransition(format!(
                        "tool {} cannot transition from {:?} to {:?}",
                        tool.tool_call_id, current.status, tool.status
                    )));
                }
                self.tools.insert(tool.tool_call_id.clone(), tool);
            }
            AgentUpdate::Usage { usage, .. } => self.usage = Some(usage),
        }
        self.applied_updates.insert(update_id, update_hash);
        Ok(())
    }

    /// Messages in durable presentation order.
    pub fn ordered_messages(&self) -> impl Iterator<Item = &AgentMessage> {
        self.message_order
            .iter()
            .filter_map(|id| self.messages.get(id))
    }

    /// Rebuild a Session projection from its ordered durable update journal.
    pub fn replay(
        session_id: impl Into<String>,
        updates: impl IntoIterator<Item = AgentUpdate>,
    ) -> AgentResult<Self> {
        let mut session = Self::new(session_id);
        for update in updates {
            session.apply(update)?;
        }
        Ok(session)
    }
}

fn valid_tool_transition(previous: ToolCallStatus, next: ToolCallStatus) -> bool {
    previous == next
        || matches!(
            (previous, next),
            (
                ToolCallStatus::Pending,
                ToolCallStatus::AwaitingPermission | ToolCallStatus::InProgress
            ) | (
                ToolCallStatus::AwaitingPermission,
                ToolCallStatus::InProgress | ToolCallStatus::Cancelled
            ) | (
                ToolCallStatus::InProgress,
                ToolCallStatus::Completed | ToolCallStatus::Failed | ToolCallStatus::Cancelled
            )
        )
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ContextRequest {
    pub session_id: String,
    pub messages: Vec<AgentMessage>,
    pub budget: u64,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ContextSnapshot {
    pub snapshot_id: String,
    pub content: Vec<ContentBlock>,
    pub occurrence_binding: String,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModelRequest {
    pub session_id: String,
    pub context: ContextSnapshot,
    pub tools: Vec<String>,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModelResponse {
    pub message: AgentMessage,
    pub tool_requests: Vec<ToolRequest>,
    pub occurrence_binding: String,
    pub usage: Usage,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PermissionRequest {
    pub request_id: String,
    pub tool: ToolRequest,
    pub options: Vec<String>,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PermissionDecision {
    AllowOnce,
    Deny,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PermissionResponse {
    pub decision: PermissionDecision,
    pub occurrence_binding: String,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ToolRequest {
    pub tool_call_id: String,
    pub operation: String,
    pub input: Value,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ToolResponse {
    pub tool_call_id: String,
    pub content: Vec<ContentBlock>,
    pub occurrence_binding: String,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ElicitationRequest {
    pub request_id: String,
    pub schema: Value,
    pub prompt: Vec<ContentBlock>,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ElicitationResponse {
    pub request_id: String,
    pub accepted: bool,
    pub value: Option<Value>,
    pub occurrence_binding: String,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkspaceChange {
    pub change_id: String,
    pub overlay: ArtifactRef,
    pub commit: bool,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkspaceReceipt {
    pub change_id: String,
    pub committed: bool,
    pub evidence: ArtifactRef,
    pub occurrence_binding: String,
}

/// Kind of replaceable host interaction recorded as a durable occurrence.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AgentHostCallKind {
    Context,
    Model,
    Permission,
    Tool,
    Elicitation,
    Workspace,
}

/// Typed request admitted at one agent host occurrence.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "request", rename_all = "snake_case")]
pub enum AgentHostRequest {
    Context(ContextRequest),
    Model(ModelRequest),
    Permission(PermissionRequest),
    Tool(ToolRequest),
    Elicitation(ElicitationRequest),
    Workspace(WorkspaceChange),
}

impl AgentHostRequest {
    /// Closed request kind used for response matching.
    pub const fn kind(&self) -> AgentHostCallKind {
        match self {
            Self::Context(_) => AgentHostCallKind::Context,
            Self::Model(_) => AgentHostCallKind::Model,
            Self::Permission(_) => AgentHostCallKind::Permission,
            Self::Tool(_) => AgentHostCallKind::Tool,
            Self::Elicitation(_) => AgentHostCallKind::Elicitation,
            Self::Workspace(_) => AgentHostCallKind::Workspace,
        }
    }
}

/// Typed response durably retained for exact host-call replay.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "response", rename_all = "snake_case")]
pub enum AgentHostResponse {
    Context(ContextSnapshot),
    Model(ModelResponse),
    Permission(PermissionResponse),
    Tool(ToolResponse),
    Elicitation(ElicitationResponse),
    Workspace(WorkspaceReceipt),
}

impl AgentHostResponse {
    /// Closed response kind used for request matching.
    pub const fn kind(&self) -> AgentHostCallKind {
        match self {
            Self::Context(_) => AgentHostCallKind::Context,
            Self::Model(_) => AgentHostCallKind::Model,
            Self::Permission(_) => AgentHostCallKind::Permission,
            Self::Tool(_) => AgentHostCallKind::Tool,
            Self::Elicitation(_) => AgentHostCallKind::Elicitation,
            Self::Workspace(_) => AgentHostCallKind::Workspace,
        }
    }

    /// Immutable implementation binding returned by the host adapter.
    pub fn occurrence_binding(&self) -> &str {
        match self {
            Self::Context(response) => &response.occurrence_binding,
            Self::Model(response) => &response.occurrence_binding,
            Self::Permission(response) => &response.occurrence_binding,
            Self::Tool(response) => &response.occurrence_binding,
            Self::Elicitation(response) => &response.occurrence_binding,
            Self::Workspace(response) => &response.occurrence_binding,
        }
    }
}

/// Durable lifecycle for one host interaction occurrence.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AgentHostOccurrenceState {
    Prepared,
    Started,
    Completed,
    Unknown,
}

impl AgentHostOccurrenceState {
    /// Stable record-key component.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Prepared => "prepared",
            Self::Started => "started",
            Self::Completed => "completed",
            Self::Unknown => "unknown",
        }
    }
}

/// Persisted host interaction with an immutable request and binding.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentHostOccurrence {
    pub occurrence_id: String,
    pub session_id: String,
    pub request: AgentHostRequest,
    pub request_digest: String,
    pub state: AgentHostOccurrenceState,
    pub response: Option<AgentHostResponse>,
    pub occurrence_binding: Option<String>,
    pub failure: Option<String>,
}

impl AgentHostOccurrence {
    /// Admit an immutable host request before any provider call begins.
    pub fn prepare(
        occurrence_id: impl Into<String>,
        session_id: impl Into<String>,
        request: AgentHostRequest,
    ) -> AgentResult<Self> {
        let request_digest = canonical_digest(&request)
            .map_err(|error| AgentError::Validation(error.to_string()))?;
        let occurrence = Self {
            occurrence_id: occurrence_id.into(),
            session_id: session_id.into(),
            request,
            request_digest,
            state: AgentHostOccurrenceState::Prepared,
            response: None,
            occurrence_binding: None,
            failure: None,
        };
        occurrence.validate()?;
        Ok(occurrence)
    }

    /// Mark that the host invocation may now have happened.
    pub fn start(&self) -> AgentResult<Self> {
        self.successor(AgentHostOccurrenceState::Started, None, None)
    }

    /// Commit a typed response and immutable occurrence binding.
    pub fn complete(&self, response: AgentHostResponse) -> AgentResult<Self> {
        let binding = response.occurrence_binding().to_owned();
        self.successor(
            AgentHostOccurrenceState::Completed,
            Some(response),
            Some(binding),
        )
    }

    /// Record an ambiguous host result without authorizing redispatch.
    pub fn mark_unknown(&self, failure: impl Into<String>) -> AgentResult<Self> {
        let next = Self {
            occurrence_id: self.occurrence_id.clone(),
            session_id: self.session_id.clone(),
            request: self.request.clone(),
            request_digest: self.request_digest.clone(),
            state: AgentHostOccurrenceState::Unknown,
            response: None,
            occurrence_binding: None,
            failure: Some(failure.into()),
        };
        self.validate_successor(&next)?;
        Ok(next)
    }

    /// Stable idempotency key for this lifecycle transition.
    pub fn transition_id(&self) -> String {
        format!("{}:{}", self.occurrence_id, self.state.as_str())
    }

    /// Verify the complete occurrence snapshot.
    pub fn validate(&self) -> AgentResult<()> {
        if self.occurrence_id.is_empty() || self.session_id.is_empty() {
            return Err(AgentError::Validation(
                "host occurrence and Session identities must not be empty".to_owned(),
            ));
        }
        let expected = canonical_digest(&self.request)
            .map_err(|error| AgentError::Validation(error.to_string()))?;
        if self.request_digest != expected {
            return Err(AgentError::Validation(format!(
                "host occurrence {} request digest does not match",
                self.occurrence_id
            )));
        }
        match self.state {
            AgentHostOccurrenceState::Prepared | AgentHostOccurrenceState::Started => {
                if self.response.is_some()
                    || self.occurrence_binding.is_some()
                    || self.failure.is_some()
                {
                    return Err(AgentError::Validation(
                        "prepared or started occurrence cannot contain an outcome".to_owned(),
                    ));
                }
            }
            AgentHostOccurrenceState::Completed => {
                let response = self.response.as_ref().ok_or_else(|| {
                    AgentError::Validation(
                        "completed occurrence requires a typed response".to_owned(),
                    )
                })?;
                if response.kind() != self.request.kind() {
                    return Err(AgentError::Validation(
                        "host response kind does not match its request".to_owned(),
                    ));
                }
                match (&self.request, response) {
                    (AgentHostRequest::Tool(request), AgentHostResponse::Tool(response))
                        if request.tool_call_id != response.tool_call_id =>
                    {
                        return Err(AgentError::Validation(
                            "tool response identity does not match its request".to_owned(),
                        ));
                    }
                    (
                        AgentHostRequest::Elicitation(request),
                        AgentHostResponse::Elicitation(response),
                    ) if request.request_id != response.request_id => {
                        return Err(AgentError::Validation(
                            "elicitation response identity does not match its request".to_owned(),
                        ));
                    }
                    (
                        AgentHostRequest::Workspace(request),
                        AgentHostResponse::Workspace(response),
                    ) if request.change_id != response.change_id => {
                        return Err(AgentError::Validation(
                            "workspace receipt identity does not match its request".to_owned(),
                        ));
                    }
                    _ => {}
                }
                let binding = self.occurrence_binding.as_deref().ok_or_else(|| {
                    AgentError::Validation(
                        "completed occurrence requires an immutable binding".to_owned(),
                    )
                })?;
                if binding.is_empty() || binding != response.occurrence_binding() {
                    return Err(AgentError::Validation(
                        "host occurrence binding does not match its response".to_owned(),
                    ));
                }
                if self.failure.is_some() {
                    return Err(AgentError::Validation(
                        "completed occurrence cannot contain a failure".to_owned(),
                    ));
                }
            }
            AgentHostOccurrenceState::Unknown => {
                if self.response.is_some() || self.occurrence_binding.is_some() {
                    return Err(AgentError::Validation(
                        "unknown occurrence cannot claim a response or binding".to_owned(),
                    ));
                }
                if self.failure.as_deref().is_none_or(str::is_empty) {
                    return Err(AgentError::Validation(
                        "unknown occurrence requires failure evidence".to_owned(),
                    ));
                }
            }
        }
        Ok(())
    }

    /// Verify that `next` is a legal immutable transition from this snapshot.
    pub fn validate_successor(&self, next: &Self) -> AgentResult<()> {
        self.validate()?;
        next.validate()?;
        if self == next {
            return Ok(());
        }
        if self.occurrence_id != next.occurrence_id
            || self.session_id != next.session_id
            || self.request != next.request
            || self.request_digest != next.request_digest
        {
            return Err(AgentError::IllegalTransition(
                "host occurrence identity or request changed".to_owned(),
            ));
        }
        if matches!(
            (self.state, next.state),
            (
                AgentHostOccurrenceState::Prepared,
                AgentHostOccurrenceState::Started
            ) | (
                AgentHostOccurrenceState::Started,
                AgentHostOccurrenceState::Completed | AgentHostOccurrenceState::Unknown
            ) | (
                AgentHostOccurrenceState::Unknown,
                AgentHostOccurrenceState::Completed
            )
        ) {
            Ok(())
        } else {
            Err(AgentError::IllegalTransition(format!(
                "host occurrence {} cannot transition from {:?} to {:?}",
                self.occurrence_id, self.state, next.state
            )))
        }
    }

    fn successor(
        &self,
        state: AgentHostOccurrenceState,
        response: Option<AgentHostResponse>,
        occurrence_binding: Option<String>,
    ) -> AgentResult<Self> {
        let next = Self {
            occurrence_id: self.occurrence_id.clone(),
            session_id: self.session_id.clone(),
            request: self.request.clone(),
            request_digest: self.request_digest.clone(),
            state,
            response,
            occurrence_binding,
            failure: None,
        };
        self.validate_successor(&next)?;
        Ok(next)
    }
}
