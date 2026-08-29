//! MCP adapter for Cymule agent interaction contracts.

use std::future::Future;

use cymule_agent::{AgentError, AgentResult, ContentBlock, ToolRequest, ToolResponse};
use rmcp::model::{
    CallToolRequestParams, CallToolResponse, CallToolResult, ContentBlock as McpContentBlock,
};
use rmcp::{Peer, RoleClient};
use tokio::runtime::{Builder, Runtime};

/// Typed MCP call failures that preserve incomplete protocol work.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum McpCallError {
    /// Transport, protocol, or remote service failure.
    Transport(String),
    /// MCP returned input-required or task work that a caller must drive.
    Incomplete(String),
}

/// Minimal asynchronous tool caller implemented by official RMCP peers.
pub trait McpToolCaller {
    /// Execute one MCP tool call without inventing an Agent Loop.
    fn call_tool(
        &self,
        request: CallToolRequestParams,
    ) -> impl Future<Output = Result<CallToolResult, McpCallError>> + Send;
}

impl McpToolCaller for Peer<RoleClient> {
    async fn call_tool(
        &self,
        request: CallToolRequestParams,
    ) -> Result<CallToolResult, McpCallError> {
        match self
            .call_tool_once(request)
            .await
            .map_err(|error| McpCallError::Transport(error.to_string()))?
        {
            CallToolResponse::Complete(result) => Ok(result),
            CallToolResponse::InputRequired(_) => Err(McpCallError::Incomplete(
                "MCP tool requires explicit input continuation".to_owned(),
            )),
            CallToolResponse::Task(_) => Err(McpCallError::Incomplete(
                "MCP tool materialized an explicit task".to_owned(),
            )),
            _ => Err(McpCallError::Incomplete(
                "unsupported future MCP tool result requires an adapter update".to_owned(),
            )),
        }
    }
}

/// MCP tool adapter with async and synchronous application surfaces.
pub struct McpToolAdapter<C> {
    caller: C,
    occurrence_binding: String,
    runtime: Option<Runtime>,
}

impl<C: McpToolCaller> McpToolAdapter<C> {
    /// Construct an adapter around an initialized RMCP-compatible caller.
    pub fn new(caller: C, occurrence_binding: impl Into<String>) -> AgentResult<Self> {
        let occurrence_binding = occurrence_binding.into();
        if occurrence_binding.is_empty()
            || occurrence_binding.chars().count() > 512
            || occurrence_binding.chars().any(char::is_control)
        {
            return Err(AgentError::Validation(
                "MCP occurrence binding must be printable and non-empty".to_owned(),
            ));
        }
        let runtime = Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .thread_name("cymule-agent-mcp")
            .build()
            .map_err(|error| AgentError::Host(error.to_string()))?;
        Ok(Self {
            caller,
            occurrence_binding,
            runtime: Some(runtime),
        })
    }

    /// Immutable MCP implementation binding.
    pub fn occurrence_binding(&self) -> &str {
        &self.occurrence_binding
    }

    /// Execute and map one MCP tool call in an async application.
    pub async fn invoke_tool_async(&self, request: ToolRequest) -> AgentResult<ToolResponse> {
        validate_tool_request(&request)?;
        let arguments = match request.input {
            serde_json::Value::Null => None,
            serde_json::Value::Object(arguments) => Some(arguments),
            _ => {
                return Err(AgentError::Validation(
                    "MCP tool input must be a JSON object or null".to_owned(),
                ));
            }
        };
        let mut params = CallToolRequestParams::new(request.operation);
        if let Some(arguments) = arguments {
            params = params.with_arguments(arguments);
        }
        let result = self
            .caller
            .call_tool(params)
            .await
            .map_err(|error| match error {
                McpCallError::Transport(message) => AgentError::Host(message),
                McpCallError::Incomplete(message) => AgentError::RecoveryRequired(message),
            })?;
        if result.is_error == Some(true) {
            return Err(AgentError::Host(tool_error_summary(&result)));
        }
        let mut content = Vec::new();
        for block in result.content {
            map_content(block, &mut content)?;
        }
        if let Some(value) = result.structured_content {
            content.push(ContentBlock::Json { value });
        }
        Ok(ToolResponse {
            tool_call_id: request.tool_call_id,
            content,
            occurrence_binding: self.occurrence_binding.clone(),
        })
    }

    /// Execute one MCP tool call from Cymule's synchronous `AgentHost` boundary.
    pub fn invoke_tool(&self, request: ToolRequest) -> AgentResult<ToolResponse> {
        let runtime = self
            .runtime
            .as_ref()
            .ok_or_else(|| AgentError::Host("MCP runtime is closed".to_owned()))?;
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            if handle.runtime_flavor() == tokio::runtime::RuntimeFlavor::CurrentThread {
                return Err(AgentError::Host(
                    "call synchronous MCP methods from spawn_blocking".to_owned(),
                ));
            }
            tokio::task::block_in_place(|| runtime.block_on(self.invoke_tool_async(request)))
        } else {
            runtime.block_on(self.invoke_tool_async(request))
        }
    }
}

impl<C> Drop for McpToolAdapter<C> {
    fn drop(&mut self) {
        if let Some(runtime) = self.runtime.take() {
            runtime.shutdown_background();
        }
    }
}

fn validate_tool_request(request: &ToolRequest) -> AgentResult<()> {
    for (kind, identity) in [
        ("call", request.tool_call_id.as_str()),
        ("operation", request.operation.as_str()),
    ] {
        if identity.is_empty()
            || identity.chars().count() > 512
            || identity.chars().any(char::is_control)
        {
            return Err(AgentError::Validation(format!(
                "MCP tool {kind} identity must be printable and non-empty"
            )));
        }
    }
    Ok(())
}

fn map_content(block: McpContentBlock, output: &mut Vec<ContentBlock>) -> AgentResult<()> {
    match block {
        McpContentBlock::Text(text) => output.push(ContentBlock::Text { text: text.text }),
        McpContentBlock::ResourceLink(_) | McpContentBlock::Resource(_) => {
            return Err(AgentError::RecoveryRequired(
                "MCP Resource content must be resolved and sealed as a Cymule ResourceHandle"
                    .to_owned(),
            ));
        }
        _ => {
            return Err(AgentError::RecoveryRequired(
                "unsupported future MCP content requires an adapter update".to_owned(),
            ));
        }
    }
    Ok(())
}

fn tool_error_summary(result: &CallToolResult) -> String {
    let text = result
        .content
        .iter()
        .filter_map(McpContentBlock::as_text)
        .map(|content| content.text.as_str())
        .collect::<Vec<_>>()
        .join("; ");
    if text.is_empty() {
        "MCP tool returned an error".to_owned()
    } else {
        format!("MCP tool returned an error: {}", truncate(&text, 2048))
    }
}

fn truncate(value: &str, limit: usize) -> &str {
    if value.len() <= limit {
        value
    } else {
        let mut boundary = limit;
        while !value.is_char_boundary(boundary) {
            boundary -= 1;
        }
        &value[..boundary]
    }
}
