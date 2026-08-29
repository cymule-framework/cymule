//! Provider-neutral typed agent interaction contracts and projections.

mod control;
mod driver;
mod error;
mod host;
mod input;
mod interaction;
mod model;
mod recovery;
mod stream;
mod workspace;

pub use control::{
    AgentMessageReader, AgentPersistence, EphemeralAgentPersistence, PinnedAgentMessageReader,
};
pub use cymule_profile_protocol::agent::{
    AGENT_COMMAND_RECEIPT_VERSION, AGENT_COMMAND_VERSION, AGENT_RECOVERY_OBSERVATION_VERSION,
    AGENT_STREAM_PUBLICATION_INTENT_VERSION, AgentCommand, AgentCommandAction, AgentCommandOutcome,
    AgentCommandReceipt, AgentCommandSource, AgentCommit, AgentContextMessageRef,
    AgentContextScanLimits, AgentElicitationCurrent, AgentElicitationQuery, AgentElicitationRead,
    AgentInputCommand, AgentInputSource, AgentInputWaitWitness, AgentMessageCurrent,
    AgentMessagePage, AgentMessagePageQuery, AgentMessagePageRead, AgentMessageQuery,
    AgentMessageRead, AgentNonterminalTool, AgentOccurrenceCurrent, AgentOccurrencePage,
    AgentOccurrencePageQuery, AgentOccurrencePageRead, AgentOccurrencePostcondition,
    AgentOccurrenceQuery, AgentOccurrenceRead, AgentOccurrenceSource, AgentProviders,
    AgentSessionCurrent, AgentSessionEntrySource, AgentSessionPostcondition, AgentSessionQuery,
    AgentSessionRead, AgentSessionTransitionKind, AgentSessionTransitionWitness,
    AgentSessionUpdateEffect, AgentSessionUpdateSource, AgentStreamCommand, AgentToolCurrent,
    AgentToolQuery, AgentToolRead, AgentUpdateCurrent, AgentWorkspaceAdmissionQuery,
    AgentWorkspaceAdmissionRead, AgentWorkspaceCommand, AgentWorkspaceCommandPhase,
    AgentWorkspaceCommitOutcome, AgentWorkspaceDecision, AgentWorkspaceDispatchLeaseRequest,
    AgentWorkspaceM1Witness, AgentWorkspaceResolution, MAX_AGENT_COMMAND_BYTES,
    MAX_AGENT_CONTEXT_SCAN_BYTES, MAX_AGENT_CONTEXT_SCAN_ENTRIES, MAX_AGENT_CURRENT_BYTES,
    MAX_AGENT_NONTERMINAL_TOOLS, MAX_AGENT_PAGE, MAX_AGENT_PAGE_BYTES, MAX_AGENT_RECEIPT_BYTES,
    MAX_AGENT_RECOVERY_OBSERVATIONS, MAX_AGENT_TOOL_CLOSE_BYTES, MAX_AGENT_VALUE_BYTES,
    MAX_AGENT_VALUE_ENTRIES,
};
pub use driver::{AgentTurnDriver, MAX_AGENT_MODEL_ROUNDS};
pub use error::{AgentError, AgentResult};
pub use host::AgentHost;
pub use input::{AgentInputCheckpoint, AgentInputController, AgentInputResult};
pub use interaction::AgentInteractionController;
pub use model::{
    AGENT_HOST_BINDING_VERSION, AgentHostBinding, AgentHostCallKind, AgentHostOccurrence,
    AgentHostOccurrenceState, AgentHostRequest, AgentHostResponse, AgentMessage,
    AgentOccurrenceResolution, AgentPlan, AgentPlanEntry, AgentRecoveryObservation,
    AgentRecoveryObservationDisposition, AgentState, AgentUpdate, ContentBlock, ContextRequest,
    ContextSnapshot, ElicitationProjection, ElicitationRequest, ElicitationResponse, MessageRole,
    ModelRequest, ModelResponse, PermissionDecision, PermissionRequest, PermissionResponse,
    PlanEntryStatus, SessionStopReason, ToolCall, ToolCallStatus, ToolRequest, ToolResponse, Usage,
    WorkspaceChange, WorkspaceHostRequest, WorkspaceOccurrenceOwner, WorkspaceReceipt,
};
pub use recovery::AgentRecoveryController;
pub use stream::{
    AGENT_STREAM_CHUNK_LIMIT, AGENT_STREAM_STAGING_BYTES_LIMIT, AgentStreamChunk,
    AgentStreamController, AgentStreamCurrent, AgentStreamDelivery, AgentStreamEffect,
    AgentStreamFinalizeOutcome, AgentStreamPostcondition, AgentStreamPublicationContent,
    AgentStreamPublicationIntent, AgentStreamPublicationObservation, AgentStreamPublicationResult,
    AgentStreamQuery, AgentStreamRead, AgentStreamState, AgentStreamTarget,
    MAX_AGENT_STREAM_CHUNKS,
};
pub use workspace::{WorkspaceScopeCheckpoint, WorkspaceScopeController, WorkspaceScopeRequest};
