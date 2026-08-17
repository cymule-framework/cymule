# M2 Agent Interaction Profile

Status: partial.

## Implemented foundation

- protocol-neutral typed content, messages, Plans, tool calls, usage, and
  Session state updates;
- idempotent update IDs with conflicting-reuse rejection;
- ordered message projection and fail-closed tool lifecycle transitions;
- `AgentHost` interfaces for context selection, model invocation, permission,
  tools, elicitation, and workspace overlays;
- bounded reference turn driver covering context, model, permission, tool,
  tool-result feedback, second model round, and terminal idle state;
- deterministic fake-host end-to-end tests;
- adapter boundaries for ACP, MCP, A2A, editors, and model providers.

## Remaining completion gates

- durable persistence of Session updates and all host occurrences through M1;
- explicit input-required suspension/resume through durable waits;
- workspace overlay commit/abort integration with scope obligations;
- streaming chunk staging and finalized durable content;
- ACP/MCP/A2A adapters and cross-language SDK interaction clients;
- cancellation, refusal, host failure, and restart-level fault suites;
- field-complete frozen M2 JSON Schemas and removal of incubation rustdoc
  allowances.

Capability advertisement, authentication, permission, credential access, and
effect release remain separate decisions. A tool catalog entry never grants
execution authority.
