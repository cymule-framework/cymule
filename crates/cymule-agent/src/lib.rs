//! Provider-neutral typed agent interaction contracts and projections.

mod driver;
mod error;
mod host;
mod input;
mod interaction;
mod journal;
mod model;
mod recovery;

pub use driver::AgentTurnDriver;
pub use error::{AgentError, AgentResult};
pub use host::AgentHost;
pub use input::{AgentInputCheckpoint, AgentInputController};
pub use interaction::AgentInteractionController;
pub use journal::{AgentJournal, AgentOccurrenceStore, MemoryAgentJournal, NoopAgentJournal};
pub use model::{
    AgentHostCallKind, AgentHostOccurrence, AgentHostOccurrenceState, AgentHostRequest,
    AgentHostResponse, AgentMessage, AgentOccurrenceResolution, AgentPlan, AgentPlanEntry,
    AgentSession, AgentState, AgentUpdate, ContentBlock, ContextRequest, ContextSnapshot,
    ElicitationProjection, ElicitationRequest, ElicitationResponse, MessageRole, ModelRequest,
    ModelResponse, PermissionDecision, PermissionRequest, PermissionResponse, PlanEntryStatus,
    SessionStopReason, ToolCall, ToolCallStatus, ToolRequest, ToolResponse, Usage, WorkspaceChange,
    WorkspaceReceipt,
};
pub use recovery::AgentRecoveryController;
