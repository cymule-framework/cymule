use crate::{
    AgentHostRequest, AgentResult, ContextRequest, ContextSnapshot, ElicitationRequest,
    ElicitationResponse, ModelRequest, ModelResponse, PermissionRequest, PermissionResponse,
    ToolRequest, ToolResponse, WorkspaceChange, WorkspaceReceipt,
};

/// Replaceable host boundary for one agent interaction runtime.
pub trait AgentHost {
    /// Resolve and pin the implementation binding before an occurrence starts.
    fn bind_occurrence(&mut self, request: &AgentHostRequest) -> AgentResult<String>;

    /// Select and pin the exact context visible to a model occurrence.
    fn select_context(&mut self, request: ContextRequest) -> AgentResult<ContextSnapshot>;

    /// Execute one typed model occurrence.
    fn invoke_model(&mut self, request: ModelRequest) -> AgentResult<ModelResponse>;

    /// Obtain user/policy authorization separately from tool availability.
    fn request_permission(&mut self, request: PermissionRequest)
    -> AgentResult<PermissionResponse>;

    /// Execute one authorized typed tool occurrence.
    fn invoke_tool(&mut self, request: ToolRequest) -> AgentResult<ToolResponse>;

    /// Request typed human or external input.
    fn elicit(&mut self, request: ElicitationRequest) -> AgentResult<ElicitationResponse>;

    /// Commit or abort a workspace overlay through its owning substrate.
    fn apply_workspace(&mut self, change: WorkspaceChange) -> AgentResult<WorkspaceReceipt>;
}
