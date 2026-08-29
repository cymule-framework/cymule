//! MCP tool mapping, errors, and binary externalization tests.

use std::future::{Future, ready};

use cymule_agent::{AgentError, ContentBlock, ToolRequest};
use cymule_agent_mcp::{McpCallError, McpToolAdapter, McpToolCaller};
use rmcp::model::{
    CallToolRequestParams, CallToolResult, ContentBlock as McpContentBlock, Resource,
    ResourceContents,
};
use serde_json::json;

#[derive(Clone)]
struct FakeCaller {
    result: Result<CallToolResult, McpCallError>,
}

impl McpToolCaller for FakeCaller {
    fn call_tool(
        &self,
        _request: CallToolRequestParams,
    ) -> impl Future<Output = Result<CallToolResult, McpCallError>> + Send {
        ready(self.result.clone())
    }
}

#[derive(Clone)]
struct NullOnlyCaller;

impl McpToolCaller for NullOnlyCaller {
    fn call_tool(
        &self,
        request: CallToolRequestParams,
    ) -> impl Future<Output = Result<CallToolResult, McpCallError>> + Send {
        assert_eq!(request.name, "search");
        assert_eq!(request.arguments, None);
        ready(Ok(CallToolResult::success(Vec::new())))
    }
}

fn request() -> ToolRequest {
    ToolRequest {
        tool_call_id: "tool-call:one".to_owned(),
        operation: "search".to_owned(),
        input: json!({"query": "cymule"}),
    }
}

#[test]
fn text_and_structured_content_map_to_agent_blocks() {
    let mut result = CallToolResult::success(vec![McpContentBlock::text("found")]);
    result.structured_content = Some(json!({"count": 1}));
    let adapter =
        McpToolAdapter::new(FakeCaller { result: Ok(result) }, "mcp:test").expect("adapter builds");
    let response = adapter.invoke_tool(request()).expect("tool maps");
    assert_eq!(response.tool_call_id, "tool-call:one");
    assert_eq!(response.occurrence_binding, "mcp:test");
    assert_eq!(
        response.content,
        vec![
            ContentBlock::Text {
                text: "found".to_owned()
            },
            ContentBlock::Json {
                value: json!({"count": 1})
            }
        ]
    );
}

#[test]
fn tool_error_and_binary_content_fail_explicitly() {
    let error = CallToolResult::error(vec![McpContentBlock::text("denied")]);
    let adapter =
        McpToolAdapter::new(FakeCaller { result: Ok(error) }, "mcp:test").expect("adapter builds");
    assert!(matches!(
        adapter.invoke_tool(request()),
        Err(AgentError::Host(_))
    ));

    let binary = CallToolResult::success(vec![McpContentBlock::image("AAAA", "image/png")]);
    let adapter =
        McpToolAdapter::new(FakeCaller { result: Ok(binary) }, "mcp:test").expect("adapter builds");
    assert!(matches!(
        adapter.invoke_tool(request()),
        Err(AgentError::RecoveryRequired(_))
    ));
}

#[test]
fn incomplete_mcp_work_is_not_driven_as_an_agent_loop() {
    let adapter = McpToolAdapter::new(
        FakeCaller {
            result: Err(McpCallError::Incomplete("input required".to_owned())),
        },
        "mcp:test",
    )
    .expect("adapter builds");
    assert!(matches!(
        adapter.invoke_tool(request()),
        Err(AgentError::RecoveryRequired(_))
    ));
}

#[test]
fn null_input_is_forwarded_without_mcp_arguments() {
    let adapter = McpToolAdapter::new(NullOnlyCaller, "mcp:test").expect("adapter builds");
    let response = adapter
        .invoke_tool(ToolRequest {
            input: serde_json::Value::Null,
            ..request()
        })
        .expect("null input maps to omitted arguments");
    assert!(response.content.is_empty());
}

#[test]
fn tool_identity_validation_enforces_exact_boundaries() {
    let adapter = McpToolAdapter::new(
        FakeCaller {
            result: Ok(CallToolResult::success(Vec::new())),
        },
        "mcp:test",
    )
    .expect("adapter builds");

    for (field, identity) in [
        ("call", String::new()),
        ("call", "x".repeat(513)),
        ("call", "call\ncontrol".to_owned()),
        ("operation", String::new()),
        ("operation", "x".repeat(513)),
        ("operation", "operation\ncontrol".to_owned()),
    ] {
        let mut invalid = request();
        if field == "call" {
            invalid.tool_call_id = identity;
        } else {
            invalid.operation = identity;
        }
        assert!(matches!(
            adapter.invoke_tool(invalid),
            Err(AgentError::Validation(_))
        ));
    }

    for field in ["call", "operation"] {
        let mut valid = request();
        if field == "call" {
            valid.tool_call_id = "x".repeat(512);
        } else {
            valid.operation = "x".repeat(512);
        }
        adapter
            .invoke_tool(valid)
            .expect("the documented maximum identity length is accepted");
    }

    let multibyte_boundary = "界".repeat(512);
    let multibyte_overflow = "界".repeat(513);
    for field in ["call", "operation"] {
        let mut valid = request();
        let mut invalid = request();
        if field == "call" {
            valid.tool_call_id = multibyte_boundary.clone();
            invalid.tool_call_id = multibyte_overflow.clone();
        } else {
            valid.operation = multibyte_boundary.clone();
            invalid.operation = multibyte_overflow.clone();
        }
        adapter
            .invoke_tool(valid)
            .expect("512 Unicode scalar values are accepted");
        assert!(matches!(
            adapter.invoke_tool(invalid),
            Err(AgentError::Validation(_))
        ));
    }
}

#[test]
fn occurrence_binding_length_uses_unicode_scalar_values() {
    let result = || FakeCaller {
        result: Ok(CallToolResult::success(Vec::new())),
    };
    let accepted = "界".repeat(512);
    let adapter = McpToolAdapter::new(result(), accepted.clone())
        .expect("512 Unicode scalar values are accepted");
    assert_eq!(adapter.occurrence_binding(), accepted);
    assert!(matches!(
        McpToolAdapter::new(result(), "界".repeat(513)),
        Err(AgentError::Validation(_))
    ));
}

#[test]
fn resource_links_and_embedded_text_require_a_resource_adapter() {
    let linked = Resource::new("s3://bucket/key", "result").with_mime_type("application/json");
    let embedded =
        ResourceContents::text("inline result", "memory://result").with_mime_type("text/markdown");
    for content in [
        McpContentBlock::resource_link(linked),
        McpContentBlock::resource(embedded),
    ] {
        let result = CallToolResult::success(vec![content]);
        let adapter = McpToolAdapter::new(FakeCaller { result: Ok(result) }, "mcp:test")
            .expect("adapter builds");
        assert!(matches!(
            adapter.invoke_tool(request()),
            Err(AgentError::RecoveryRequired(message))
                if message.contains("Cymule ResourceHandle")
        ));
    }
}

#[test]
fn embedded_blob_requires_a_resource_adapter() {
    let result = CallToolResult::success(vec![McpContentBlock::resource(ResourceContents::blob(
        "AAAA",
        "memory://blob",
    ))]);
    let adapter =
        McpToolAdapter::new(FakeCaller { result: Ok(result) }, "mcp:test").expect("adapter builds");

    assert!(matches!(
        adapter.invoke_tool(request()),
        Err(AgentError::RecoveryRequired(_))
    ));
}
