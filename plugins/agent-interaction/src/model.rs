//! Agent wire types are owned by the closed profile protocol crate.

pub use cymule_profile_protocol::agent::{
    AGENT_HOST_BINDING_VERSION, AgentHostBinding, AgentHostCallKind, AgentHostOccurrence,
    AgentHostOccurrenceState, AgentHostRequest, AgentHostResponse, AgentMessage,
    AgentOccurrenceResolution, AgentPlan, AgentPlanEntry, AgentRecoveryObservation,
    AgentRecoveryObservationDisposition, AgentState, AgentUpdate, ContentBlock, ContextRequest,
    ContextSnapshot, ElicitationProjection, ElicitationRequest, ElicitationResponse, MessageRole,
    ModelRequest, ModelResponse, PermissionDecision, PermissionRequest, PermissionResponse,
    PlanEntryStatus, SessionStopReason, ToolCall, ToolCallStatus, ToolRequest, ToolResponse, Usage,
    WorkspaceChange, WorkspaceHostRequest, WorkspaceOccurrenceOwner, WorkspaceReceipt,
};
