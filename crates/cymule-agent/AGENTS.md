# Agent Interaction Guidance

- This crate owns protocol-neutral agent interaction contracts and projections,
  not a model loop, model SDK, UI transport, or tool catalog.
- Keep Messages, Tasks, Artifacts, session updates, permissions, elicitation,
  context selection, model calls, tool calls, and workspace changes distinct.
- External ACP, MCP, A2A, provider, and editor types belong in adapters. Preserve
  their opaque IDs and extensions without making them canonical authority.
- Session updates are idempotent ordered projections over durable occurrences.
  Streaming chunks are not durable output until explicitly finalized.
- Journal adapters persist accepted updates before the in-process projection is
  advanced. Reopen by replaying the journal; never treat an in-memory Session as
  durable authority. Adapter-local exclusion must be non-blocking and must
  surface contention instead of turning a lock into interaction semantics.
- Login, capability advertisement, permission, policy, credential access, and
  effect release are separate decisions and must remain fail closed.
- Add reducer and end-to-end tests for every new update or interaction state.
