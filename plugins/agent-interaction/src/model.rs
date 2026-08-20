//! Versioned protocol-neutral Agent interaction wire data.

use std::collections::BTreeMap;

use cymule_core::{ArtifactRef, canonical_digest};
use cymule_resource::ResourceHandle;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{AgentError, AgentResult};

/// Typed content shared by messages, model output, tools, and artifacts.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ContentBlock {
    /// UTF-8 text.
    Text {
        /// UTF-8 content.
        text: String,
    },
    /// Structured canonical JSON.
    Json {
        /// Structured JSON content.
        value: Value,
    },
    /// Immutable Cymule artifact.
    Artifact {
        /// Immutable Artifact reference.
        artifact: ArtifactRef,
    },
    /// URI reference interpreted by an external resource adapter.
    Resource {
        /// Opaque resource URI interpreted by an adapter.
        uri: String,
        /// Optional media type supplied by the adapter.
        mime_type: Option<String>,
    },
    /// Provider-neutral cross-Run Resource Handle.
    ResourceHandle {
        /// Verified provider-neutral Resource Handle.
        resource: Box<ResourceHandle>,
    },
}

impl ContentBlock {
    pub(crate) fn validate_artifact_refs(&self) -> AgentResult<()> {
        if let Self::Artifact { artifact } = self {
            artifact
                .validate()
                .map_err(|error| AgentError::Validation(error.to_string()))?;
        }
        Ok(())
    }
}

fn validate_content_blocks(content: &[ContentBlock]) -> AgentResult<()> {
    for block in content {
        block.validate_artifact_refs()?;
    }
    Ok(())
}

fn validate_message_artifacts(message: &AgentMessage) -> AgentResult<()> {
    validate_content_blocks(&message.content)
}

/// Message author role.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MessageRole {
    /// Caller or human input.
    User,
    /// Agent-produced output.
    Agent,
    /// Tool-produced output.
    Tool,
    /// System or policy context.
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
    /// No foreground interaction is active.
    Idle,
    /// A caller-owned interaction is active.
    Running,
    /// Durable external input is required.
    RequiresAction,
    /// The Session cannot accept later updates.
    Closed,
}

/// Terminal foreground stop reason.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SessionStopReason {
    /// The caller-owned turn reached its ordinary boundary.
    EndTurn,
    /// The caller cancelled the foreground work.
    Cancelled,
    /// The agent or policy refused the requested work.
    Refusal,
    /// The caller ended the foreground work after an error.
    Error,
}

/// Plan entry lifecycle.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PlanEntryStatus {
    /// Work has not started.
    Pending,
    /// Work is currently active.
    InProgress,
    /// Work completed.
    Completed,
    /// Work was cancelled.
    Cancelled,
}

/// One user-visible Plan entry.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentPlanEntry {
    /// Stable entry identity.
    pub entry_id: String,
    /// User-visible description.
    pub content: String,
    /// Current projected lifecycle.
    pub status: PlanEntryStatus,
}

/// User-visible agent Plan projection.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentPlan {
    /// Stable user-visible Plan identity.
    pub plan_id: String,
    /// Ordered Plan entries.
    pub entries: Vec<AgentPlanEntry>,
}

/// Tool call lifecycle.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ToolCallStatus {
    /// Tool request is known but not admitted.
    Pending,
    /// Tool request is waiting for a separate permission decision.
    AwaitingPermission,
    /// Tool execution may have started.
    InProgress,
    /// Tool execution completed with output.
    Completed,
    /// Tool execution terminated with failure evidence.
    Failed,
    /// Tool execution was cancelled.
    Cancelled,
}

/// Tool call projection.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ToolCall {
    /// Stable caller-owned tool occurrence identity.
    pub tool_call_id: String,
    /// Abstract tool operation.
    pub operation: String,
    /// Current projected lifecycle.
    pub status: ToolCallStatus,
    /// Immutable structured input.
    pub input: Value,
    /// Finalized output, when available.
    pub output: Option<Vec<ContentBlock>>,
    /// Optional presentation locations attached by an adapter.
    pub locations: Vec<String>,
}

/// Token and monetary usage projection.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Usage {
    /// Cumulative consumed units.
    pub used: u64,
    /// Reported capacity or budget ceiling.
    pub capacity: u64,
    /// Optional provider-neutral structured cost observation.
    pub cost: Option<Value>,
}

/// Idempotent ordered update applied to one Session projection.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum AgentUpdate {
    /// Publish one finalized message.
    Message {
        /// Stable update identity.
        update_id: String,
        /// Finalized message.
        message: AgentMessage,
    },
    /// Change the Session activity projection.
    State {
        /// Stable update identity.
        update_id: String,
        /// New Session activity state.
        state: AgentState,
        /// Required reason when returning to idle.
        stop_reason: Option<SessionStopReason>,
    },
    /// Replace the current user-visible agent Plan.
    Plan {
        /// Stable update identity.
        update_id: String,
        /// Complete current Plan projection.
        plan: AgentPlan,
    },
    /// Advance one tool lifecycle projection.
    Tool {
        /// Stable update identity.
        update_id: String,
        /// Complete current tool projection.
        tool: ToolCall,
    },
    /// Replace the cumulative usage observation.
    Usage {
        /// Stable update identity.
        update_id: String,
        /// Latest cumulative usage.
        usage: Usage,
    },
    /// Create or resolve one durable elicitation.
    Elicitation {
        /// Stable update identity.
        update_id: String,
        /// Complete current elicitation projection.
        elicitation: ElicitationProjection,
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
            | Self::Usage { update_id, .. }
            | Self::Elicitation { update_id, .. } => update_id,
        }
    }

    pub(crate) fn validate_artifact_refs(&self) -> AgentResult<()> {
        match self {
            Self::Message { message, .. } => validate_message_artifacts(message),
            Self::Tool { tool, .. } => {
                if let Some(output) = &tool.output {
                    validate_content_blocks(output)?;
                }
                Ok(())
            }
            Self::Elicitation { elicitation, .. } => {
                validate_content_blocks(&elicitation.request.prompt)
            }
            Self::State { .. } | Self::Plan { .. } | Self::Usage { .. } => Ok(()),
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
    /// Reason the latest foreground interaction stopped, when idle.
    pub stop_reason: Option<SessionStopReason>,
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
    /// Durable input requests keyed by elicitation identity.
    pub elicitations: BTreeMap<String, ElicitationProjection>,
    applied_updates: BTreeMap<String, String>,
}

impl AgentSession {
    /// Create an idle Session.
    pub fn new(session_id: impl Into<String>) -> Self {
        Self {
            session_id: session_id.into(),
            state: AgentState::Idle,
            stop_reason: None,
            messages: BTreeMap::new(),
            message_order: Vec::new(),
            plan: None,
            tools: BTreeMap::new(),
            usage: None,
            elicitations: BTreeMap::new(),
            applied_updates: BTreeMap::new(),
        }
    }

    /// Apply one idempotent typed update.
    pub fn apply(&mut self, update: AgentUpdate) -> AgentResult<()> {
        update.validate_artifact_refs()?;
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
                if let Some(current) = self.messages.get(&message.message_id) {
                    if current != &message {
                        return Err(AgentError::IllegalTransition(format!(
                            "message {} changed finalized content",
                            message.message_id
                        )));
                    }
                } else {
                    self.message_order.push(message.message_id.clone());
                    self.messages.insert(message.message_id.clone(), message);
                }
            }
            AgentUpdate::State {
                state, stop_reason, ..
            } => {
                if self.state == AgentState::Closed {
                    return Err(AgentError::IllegalTransition(
                        "closed Session cannot change state".to_owned(),
                    ));
                }
                if (state == AgentState::Idle) != stop_reason.is_some() {
                    return Err(AgentError::Validation(
                        "only an idle transition carries exactly one stop_reason".to_owned(),
                    ));
                }
                self.state = state;
                self.stop_reason = stop_reason;
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
            AgentUpdate::Elicitation { elicitation, .. } => {
                elicitation.validate()?;
                if let Some(current) = self.elicitations.get(&elicitation.request.request_id)
                    && (current.wait_id != elicitation.wait_id
                        || current.request != elicitation.request
                        || current.response.is_some() && current.response != elicitation.response)
                {
                    return Err(AgentError::IllegalTransition(format!(
                        "elicitation {} changed immutable content or resolved twice",
                        elicitation.request.request_id
                    )));
                }
                self.elicitations
                    .insert(elicitation.request.request_id.clone(), elicitation);
            }
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
/// Request for an adapter-selected bounded context snapshot.
pub struct ContextRequest {
    /// Session whose finalized messages are supplied.
    pub session_id: String,
    /// Ordered finalized messages eligible for selection.
    pub messages: Vec<AgentMessage>,
    /// Caller-defined bounded selection budget.
    pub budget: u64,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
/// Immutable context selected for one model occurrence.
pub struct ContextSnapshot {
    /// Stable snapshot identity.
    pub snapshot_id: String,
    /// Selected content.
    pub content: Vec<ContentBlock>,
    /// Pinned context-adapter binding.
    pub occurrence_binding: String,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
/// One model-host request over a pinned context snapshot.
pub struct ModelRequest {
    /// Owning Session identity.
    pub session_id: String,
    /// Exact context visible to this occurrence.
    pub context: ContextSnapshot,
    /// Abstract tool operations offered by the caller.
    pub tools: Vec<String>,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
/// Final response from one model occurrence.
pub struct ModelResponse {
    /// Finalized Agent message.
    pub message: AgentMessage,
    /// Tool requests returned to the caller-owned loop.
    pub tool_requests: Vec<ToolRequest>,
    /// Pinned model-adapter binding.
    pub occurrence_binding: String,
    /// Cumulative or occurrence usage observation.
    pub usage: Usage,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
/// Separate authorization request for one proposed tool call.
pub struct PermissionRequest {
    /// Stable authorization request identity.
    pub request_id: String,
    /// Tool request being authorized.
    pub tool: ToolRequest,
    /// Closed options presented to the policy or user.
    pub options: Vec<String>,
}
/// Permission outcome kept separate from tool availability and execution.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PermissionDecision {
    /// Permit this occurrence only.
    AllowOnce,
    /// Refuse this occurrence.
    Deny,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
/// One pinned permission decision.
pub struct PermissionResponse {
    /// Explicit authorization outcome.
    pub decision: PermissionDecision,
    /// Pinned permission-adapter binding.
    pub occurrence_binding: String,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
/// Proposed invocation of an abstract tool operation.
pub struct ToolRequest {
    /// Stable tool-call identity.
    pub tool_call_id: String,
    /// Abstract operation name.
    pub operation: String,
    /// Structured immutable input.
    pub input: Value,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
/// Final response from one tool occurrence.
pub struct ToolResponse {
    /// Matching tool-call identity.
    pub tool_call_id: String,
    /// Finalized output content.
    pub content: Vec<ContentBlock>,
    /// Pinned tool-adapter binding.
    pub occurrence_binding: String,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
/// Typed request for human or external input.
pub struct ElicitationRequest {
    /// Stable input request identity.
    pub request_id: String,
    /// Self-contained JSON Schema for an accepted value.
    pub schema: Value,
    /// User-visible prompt content.
    pub prompt: Vec<ContentBlock>,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
/// Accepted or declined elicitation result.
pub struct ElicitationResponse {
    /// Matching input request identity.
    pub request_id: String,
    /// Whether the responder accepted the request.
    pub accepted: bool,
    /// Validated value when accepted; absent when declined.
    pub value: Option<Value>,
    /// Pinned elicitation-adapter binding.
    pub occurrence_binding: String,
}

/// Durable projection of one typed input request and optional completion.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ElicitationProjection {
    /// Owning M1 wait identity.
    pub wait_id: String,
    /// Immutable input request.
    pub request: ElicitationRequest,
    /// Optional terminal response.
    pub response: Option<ElicitationResponse>,
}

impl ElicitationProjection {
    /// Validate immutable request identity and completion shape.
    pub fn validate(&self) -> AgentResult<()> {
        if self.wait_id.is_empty() || self.request.request_id.is_empty() {
            return Err(AgentError::Validation(
                "elicitation and wait identities must not be empty".to_owned(),
            ));
        }
        if let Some(response) = &self.response {
            if response.request_id != self.request.request_id {
                return Err(AgentError::Validation(
                    "elicitation response identity does not match its request".to_owned(),
                ));
            }
            if response.occurrence_binding.is_empty() {
                return Err(AgentError::Validation(
                    "elicitation response requires an occurrence binding".to_owned(),
                ));
            }
            if response.accepted != response.value.is_some() {
                return Err(AgentError::Validation(
                    "accepted elicitation requires a value and declined elicitation forbids one"
                        .to_owned(),
                ));
            }
        }
        Ok(())
    }
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
/// Request to commit or abort a prepared workspace overlay.
pub struct WorkspaceChange {
    /// Stable workspace-change identity.
    pub change_id: String,
    /// Immutable overlay Artifact.
    pub overlay: ArtifactRef,
    /// Whether the overlay should commit rather than abort.
    pub commit: bool,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
/// Final receipt from a workspace adapter.
pub struct WorkspaceReceipt {
    /// Matching workspace-change identity.
    pub change_id: String,
    /// Observed commit outcome.
    pub committed: bool,
    /// Immutable provider evidence.
    pub evidence: ArtifactRef,
    /// Pinned workspace-adapter binding.
    pub occurrence_binding: String,
}

/// Kind of replaceable host interaction recorded as a durable occurrence.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AgentHostCallKind {
    /// Context selection.
    Context,
    /// Model invocation.
    Model,
    /// Permission decision.
    Permission,
    /// Tool invocation.
    Tool,
    /// Human or external input request.
    Elicitation,
    /// Workspace overlay application.
    Workspace,
}

/// Typed request admitted at one agent host occurrence.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "request", rename_all = "snake_case")]
pub enum AgentHostRequest {
    /// Context-selection request.
    Context(ContextRequest),
    /// Model-invocation request.
    Model(ModelRequest),
    /// Permission-decision request.
    Permission(PermissionRequest),
    /// Tool-invocation request.
    Tool(ToolRequest),
    /// Human or external input request.
    Elicitation(ElicitationRequest),
    /// Workspace overlay request.
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

    fn validate_artifact_refs(&self) -> AgentResult<()> {
        match self {
            Self::Context(request) => {
                for message in &request.messages {
                    validate_message_artifacts(message)?;
                }
                Ok(())
            }
            Self::Model(request) => validate_content_blocks(&request.context.content),
            Self::Elicitation(request) => validate_content_blocks(&request.prompt),
            Self::Workspace(request) => request
                .overlay
                .validate()
                .map_err(|error| AgentError::Validation(error.to_string())),
            Self::Permission(_) | Self::Tool(_) => Ok(()),
        }
    }
}

/// Typed response durably retained for exact host-call replay.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "response", rename_all = "snake_case")]
pub enum AgentHostResponse {
    /// Context-selection response.
    Context(ContextSnapshot),
    /// Model-invocation response.
    Model(ModelResponse),
    /// Permission-decision response.
    Permission(PermissionResponse),
    /// Tool-invocation response.
    Tool(ToolResponse),
    /// Human or external input response.
    Elicitation(ElicitationResponse),
    /// Workspace overlay response.
    Workspace(WorkspaceReceipt),
}

/// Query-only reconciliation observation for one original host occurrence.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "resolution", rename_all = "snake_case")]
pub enum AgentOccurrenceResolution {
    /// The original occurrence completed and returned this typed response.
    Completed {
        /// Original typed host response.
        response: AgentHostResponse,
    },
    /// The original occurrence definitely did not apply.
    NotApplied {
        /// Provider evidence that dispatch did not apply.
        evidence: Vec<ContentBlock>,
    },
    /// The provider still cannot determine the original outcome.
    Unknown {
        /// Evidence available while the result remains ambiguous.
        evidence: Vec<ContentBlock>,
    },
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

    fn validate_artifact_refs(&self) -> AgentResult<()> {
        match self {
            Self::Context(response) => validate_content_blocks(&response.content),
            Self::Model(response) => validate_message_artifacts(&response.message),
            Self::Tool(response) => validate_content_blocks(&response.content),
            Self::Workspace(response) => response
                .evidence
                .validate()
                .map_err(|error| AgentError::Validation(error.to_string())),
            Self::Permission(_) | Self::Elicitation(_) => Ok(()),
        }
    }
}

/// Durable lifecycle for one host interaction occurrence.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AgentHostOccurrenceState {
    /// Binding and request are durable; dispatch has not started.
    Prepared,
    /// Dispatch may have started and requires reconciliation after ambiguity.
    Started,
    /// Typed response is durable.
    Completed,
    /// Dispatch outcome remains ambiguous.
    Unknown,
    /// Provider proved the dispatch did not apply.
    NotApplied,
}

impl AgentHostOccurrenceState {
    /// Stable record-key component.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Prepared => "prepared",
            Self::Started => "started",
            Self::Completed => "completed",
            Self::Unknown => "unknown",
            Self::NotApplied => "not_applied",
        }
    }
}

/// Persisted host interaction with an immutable request and binding.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentHostOccurrence {
    /// Stable caller-supplied occurrence identity.
    pub occurrence_id: String,
    /// Owning Session identity.
    pub session_id: String,
    /// Immutable typed request.
    pub request: AgentHostRequest,
    /// Canonical request digest used for conflict detection.
    pub request_digest: String,
    /// Current durable lifecycle state.
    pub state: AgentHostOccurrenceState,
    /// Retained terminal response, when completed.
    pub response: Option<AgentHostResponse>,
    /// Pinned implementation binding selected before dispatch.
    pub occurrence_binding: String,
    /// Host error summary for an ambiguous outcome.
    pub failure: Option<String>,
    /// Evidence collected during cancellation or reconciliation.
    #[serde(default)]
    pub recovery_evidence: Vec<ContentBlock>,
}

impl AgentHostOccurrence {
    /// Admit an immutable host request before any provider call begins.
    pub fn prepare(
        occurrence_id: impl Into<String>,
        session_id: impl Into<String>,
        request: AgentHostRequest,
        occurrence_binding: impl Into<String>,
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
            occurrence_binding: occurrence_binding.into(),
            failure: None,
            recovery_evidence: Vec::new(),
        };
        occurrence.validate()?;
        Ok(occurrence)
    }

    /// Mark that the host invocation may now have happened.
    pub fn start(&self) -> AgentResult<Self> {
        self.successor(AgentHostOccurrenceState::Started, None)
    }

    /// Commit a typed response and immutable occurrence binding.
    pub fn complete(&self, response: AgentHostResponse) -> AgentResult<Self> {
        self.successor(AgentHostOccurrenceState::Completed, Some(response))
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
            occurrence_binding: self.occurrence_binding.clone(),
            failure: Some(failure.into()),
            recovery_evidence: Vec::new(),
        };
        self.validate_successor(&next)?;
        Ok(next)
    }

    /// Record that reconciliation still cannot determine the world outcome.
    pub fn mark_unknown_with_evidence(
        &self,
        failure: impl Into<String>,
        evidence: Vec<ContentBlock>,
    ) -> AgentResult<Self> {
        let mut next = self.mark_unknown(failure)?;
        next.recovery_evidence = evidence;
        self.validate_successor(&next)?;
        Ok(next)
    }

    /// Settle the occurrence as definitely not applied.
    pub fn mark_not_applied(&self, evidence: Vec<ContentBlock>) -> AgentResult<Self> {
        let next = Self {
            occurrence_id: self.occurrence_id.clone(),
            session_id: self.session_id.clone(),
            request: self.request.clone(),
            request_digest: self.request_digest.clone(),
            state: AgentHostOccurrenceState::NotApplied,
            response: None,
            occurrence_binding: self.occurrence_binding.clone(),
            failure: None,
            recovery_evidence: evidence,
        };
        self.validate_successor(&next)?;
        Ok(next)
    }

    /// Whether this occurrence no longer blocks Session recovery.
    pub const fn is_terminal(&self) -> bool {
        matches!(
            self.state,
            AgentHostOccurrenceState::Completed | AgentHostOccurrenceState::NotApplied
        )
    }

    /// Stable idempotency key for this lifecycle transition.
    pub fn transition_id(&self) -> String {
        format!("{}:{}", self.occurrence_id, self.state.as_str())
    }

    /// Verify the complete occurrence snapshot.
    pub fn validate(&self) -> AgentResult<()> {
        if self.occurrence_id.is_empty()
            || self.session_id.is_empty()
            || self.occurrence_binding.is_empty()
        {
            return Err(AgentError::Validation(
                "host occurrence, Session, and binding identities must not be empty".to_owned(),
            ));
        }
        self.request.validate_artifact_refs()?;
        if let Some(response) = &self.response {
            response.validate_artifact_refs()?;
        }
        validate_content_blocks(&self.recovery_evidence)?;
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
                    || self.failure.is_some()
                    || !self.recovery_evidence.is_empty()
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
                if self.occurrence_binding != response.occurrence_binding() {
                    return Err(AgentError::Validation(
                        "host occurrence binding does not match its response".to_owned(),
                    ));
                }
                if self.failure.is_some() || !self.recovery_evidence.is_empty() {
                    return Err(AgentError::Validation(
                        "completed occurrence cannot contain a failure".to_owned(),
                    ));
                }
            }
            AgentHostOccurrenceState::Unknown => {
                if self.response.is_some() {
                    return Err(AgentError::Validation(
                        "unknown occurrence cannot claim a response".to_owned(),
                    ));
                }
                if self.failure.as_deref().is_none_or(str::is_empty) {
                    return Err(AgentError::Validation(
                        "unknown occurrence requires failure evidence".to_owned(),
                    ));
                }
            }
            AgentHostOccurrenceState::NotApplied => {
                if self.response.is_some()
                    || self.failure.is_some()
                    || self.recovery_evidence.is_empty()
                {
                    return Err(AgentError::Validation(
                        "not-applied occurrence requires recovery evidence only".to_owned(),
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
            || self.occurrence_binding != next.occurrence_binding
        {
            return Err(AgentError::IllegalTransition(
                "host occurrence identity or request changed".to_owned(),
            ));
        }
        if matches!(
            (self.state, next.state),
            (
                AgentHostOccurrenceState::Prepared,
                AgentHostOccurrenceState::Started | AgentHostOccurrenceState::NotApplied
            ) | (
                AgentHostOccurrenceState::Started,
                AgentHostOccurrenceState::Completed
                    | AgentHostOccurrenceState::Unknown
                    | AgentHostOccurrenceState::NotApplied
            ) | (
                AgentHostOccurrenceState::Unknown,
                AgentHostOccurrenceState::Completed | AgentHostOccurrenceState::NotApplied
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
    ) -> AgentResult<Self> {
        let next = Self {
            occurrence_id: self.occurrence_id.clone(),
            session_id: self.session_id.clone(),
            request: self.request.clone(),
            request_digest: self.request_digest.clone(),
            state,
            response,
            occurrence_binding: self.occurrence_binding.clone(),
            failure: None,
            recovery_evidence: Vec::new(),
        };
        self.validate_successor(&next)?;
        Ok(next)
    }
}
