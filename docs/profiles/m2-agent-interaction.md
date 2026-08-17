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
- provider-neutral `AgentJournal`, validate-before-append Session transitions,
  idempotent durable update append, and full projection replay after reopen;
- an M1 `DurableCoordinator` journal integration that commits agent updates
  through the same whole-state CAS authority as Continuations and waits;
- typed host occurrences for context, model, permission, tool, elicitation, and
  workspace calls with immutable request digests and occurrence bindings
  resolved before `prepared` is persisted;
- `prepared -> started -> completed | unknown` persistence through both the
  in-memory journal and M1 whole-state CAS, including retained typed responses;
- restart rejection for unresolved occurrences and a receipt-loss fault test
  proving a completed provider call is not automatically redispatched;
- non-blocking in-memory reference journal with conflicting update identity
  rejection;
- frozen Draft 2020-12 wire schema and a Rust-validated occurrence fixture;
- durable elicitation projections and `AgentInputController` checkpoints that
  atomically move Session state to `RequiresAction` with an M1 input wait, then
  retain `RequiresAction` until the final input wait completes and only then
  return the Session to `Running`;
- reopen, idempotent retry, and stale-CAS tests proving input wait and Session
  projection cannot commit independently;
- query-only `AgentRecoveryController` reconciliation against the original
  binding, typed `completed`/`not_applied` resolutions, and evidence-gated
  cancellation of calls that remained `prepared`;
- fault tests proving reconciliation never redispatches the original tool call
  and unresolved foreground turn control remains fail closed;
- deterministic fake-host end-to-end tests;
- adapter boundaries for ACP, MCP, A2A, editors, and model providers.

## Remaining completion gates

- durable foreground turn control and continuation from retained completed
  responses;
- completed input value validation against its declared JSON Schema;
- workspace overlay commit/abort integration with scope obligations;
- streaming chunk staging and finalized durable content;
- ACP/MCP/A2A adapters and cross-language SDK interaction clients;
- cancellation, refusal, host failure, and restart-level fault suites;
- field-complete Session projection schema and removal of incubation rustdoc
  allowances.

Capability advertisement, authentication, permission, credential access, and
effect release remain separate decisions. A tool catalog entry never grants
execution authority.
