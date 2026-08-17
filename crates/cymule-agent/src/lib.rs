//! Provider-neutral typed agent interaction contracts and projections.

mod driver;
mod error;
mod host;
mod journal;
mod model;

pub use driver::AgentTurnDriver;
pub use error::{AgentError, AgentResult};
pub use host::AgentHost;
pub use journal::{AgentJournal, MemoryAgentJournal, NoopAgentJournal};
pub use model::{
    AgentMessage, AgentPlan, AgentPlanEntry, AgentSession, AgentState, AgentUpdate, ContentBlock,
    ContextRequest, ContextSnapshot, ElicitationRequest, ElicitationResponse, MessageRole,
    ModelRequest, ModelResponse, PermissionDecision, PermissionRequest, PlanEntryStatus,
    SessionStopReason, ToolCall, ToolCallStatus, ToolRequest, ToolResponse, Usage, WorkspaceChange,
    WorkspaceReceipt,
};
