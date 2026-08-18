# Cymule MCP Agent Adapter

`cymule-agent-mcp` adapts official MCP client tool calls into `cymule-agent`
`ToolRequest`/`ToolResponse` contracts.

```sh
cargo add cymule-agent-mcp
```

Pass an initialized official `rmcp::Peer<RoleClient>` and an immutable binding
identity to `McpToolAdapter`. Tool arguments map exactly, text and structured
JSON are retained, and MCP Resource links remain opaque Resource references.
Inline binary/image/audio responses fail with `RecoveryRequired` until an
application-provided Resource adapter externalizes them.

The adapter intentionally does not select context, invoke a model, authorize a
tool, drive MCP elicitation rounds, or decide the next Agent Loop step.
