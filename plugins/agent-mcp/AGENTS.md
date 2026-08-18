# MCP Agent Adapter Guidance

- This crate adapts MCP tools and Resource references to `cymule-agent`; it does
  not own Agent Loop ordering, model calls, permission decisions, Sessions, or
  durable occurrence admission.
- Use the official `rmcp` protocol implementation. Do not duplicate JSON-RPC,
  negotiation, transports, cancellation, tasks, or OAuth.
- Preserve the caller's `tool_call_id` and the configured immutable MCP binding.
  MCP tool name and arguments map exactly; non-object arguments fail closed.
- Text and structured JSON map directly. Resource links retain opaque URI and
  MIME type. Inline binary/image/audio content requires a Resource adapter and
  returns recovery-required instead of embedding unbounded base64 in Session.
- A tool-level MCP error is a host failure. `input_required` or task results are
  explicit incomplete work and must not be silently driven as an Agent Loop.
- Sync calls own a private Tokio runtime and must run on a synchronous worker or
  `spawn_blocking`; async applications should use `invoke_tool_async`.
