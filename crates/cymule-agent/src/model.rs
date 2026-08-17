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
    fn id(&self) -> &str {
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
        if let Some(existing) = self.applied_updates.get(update.id()) {
            return if existing == &update_hash {
                Ok(())
            } else {
                Err(AgentError::IllegalTransition(format!(
                    "update ID {} was reused with different content",
                    update.id()
                )))
            };
        }
        let update_id = update.id().to_owned();
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
pub struct ContextRequest {
    pub session_id: String,
    pub messages: Vec<AgentMessage>,
    pub budget: u64,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ContextSnapshot {
    pub snapshot_id: String,
    pub content: Vec<ContentBlock>,
    pub occurrence_binding: String,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ModelRequest {
    pub session_id: String,
    pub context: ContextSnapshot,
    pub tools: Vec<String>,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ModelResponse {
    pub message: AgentMessage,
    pub tool_requests: Vec<ToolRequest>,
    pub occurrence_binding: String,
    pub usage: Usage,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
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
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ToolRequest {
    pub tool_call_id: String,
    pub operation: String,
    pub input: Value,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ToolResponse {
    pub tool_call_id: String,
    pub content: Vec<ContentBlock>,
    pub occurrence_binding: String,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ElicitationRequest {
    pub request_id: String,
    pub schema: Value,
    pub prompt: Vec<ContentBlock>,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ElicitationResponse {
    pub request_id: String,
    pub accepted: bool,
    pub value: Option<Value>,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WorkspaceChange {
    pub change_id: String,
    pub overlay: ArtifactRef,
    pub commit: bool,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WorkspaceReceipt {
    pub change_id: String,
    pub committed: bool,
    pub evidence: ArtifactRef,
}
