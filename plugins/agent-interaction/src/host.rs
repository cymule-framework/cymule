use crate::{
    AgentError, AgentHostBinding, AgentHostOccurrence, AgentHostRequest, AgentMessageReader,
    AgentOccurrenceResolution, AgentResult, ContextRequest, ContextSnapshot, ElicitationRequest,
    ElicitationResponse, ModelRequest, ModelResponse, PermissionRequest, PermissionResponse,
    ToolRequest, ToolResponse, WorkspaceChange, WorkspaceReceipt,
};

/// Replaceable host boundary for one agent interaction runtime.
pub trait AgentHost {
    /// Resolve and pin the complete Agent-host implementation descriptor before
    /// an occurrence starts. A workspace controller requires the explicit M1
    /// Effect-operation closure variant; this host remains distinct from a
    /// runtime `PluginHost`.
    ///
    /// # Errors
    ///
    /// Returns an error when no exact immutable binding can be selected for
    /// the request.
    fn bind_occurrence(&mut self, request: &AgentHostRequest) -> AgentResult<AgentHostBinding>;

    /// Select and pin the exact context visible to a model occurrence.
    ///
    /// # Errors
    ///
    /// Returns an error when selection cannot complete within the supplied
    /// reader capability or cannot produce a valid snapshot.
    fn select_context(
        &mut self,
        request: ContextRequest,
        messages: &mut dyn AgentMessageReader,
    ) -> AgentResult<ContextSnapshot>;

    /// Execute one typed model occurrence.
    ///
    /// # Errors
    ///
    /// Returns an error when the model provider cannot return a valid response.
    fn invoke_model(&mut self, request: ModelRequest) -> AgentResult<ModelResponse>;

    /// Obtain user/policy authorization separately from tool availability.
    ///
    /// # Errors
    ///
    /// Returns an error when the authorization authority cannot return a valid decision.
    fn request_permission(&mut self, request: PermissionRequest)
    -> AgentResult<PermissionResponse>;

    /// Execute one authorized typed tool occurrence.
    ///
    /// # Errors
    ///
    /// Returns an error when the tool provider cannot return a valid response.
    fn invoke_tool(&mut self, request: ToolRequest) -> AgentResult<ToolResponse>;

    /// Request typed human or external input.
    ///
    /// # Errors
    ///
    /// Returns an error when the input authority cannot return a valid response.
    fn elicit(&mut self, request: ElicitationRequest) -> AgentResult<ElicitationResponse>;

    /// Commit or abort a workspace overlay through its owning substrate.
    ///
    /// # Errors
    ///
    /// Returns an error when the substrate cannot return a binding-matched receipt.
    fn apply_workspace(&mut self, change: WorkspaceChange) -> AgentResult<WorkspaceReceipt>;

    /// Query the original binding for an ambiguous occurrence without
    /// redispatching its request.
    ///
    /// # Errors
    ///
    /// Returns an error when the retained binding cannot establish an exact
    /// reconciliation outcome.
    fn reconcile_occurrence(
        &mut self,
        occurrence: &AgentHostOccurrence,
    ) -> AgentResult<AgentOccurrenceResolution> {
        Err(AgentError::RecoveryRequired(format!(
            "host binding {} does not implement reconciliation for {}",
            occurrence.occurrence_binding.binding_id(),
            occurrence.occurrence_id
        )))
    }
}
