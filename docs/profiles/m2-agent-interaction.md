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
- a caller-driven `AgentInteractionController` that accepts a stable occurrence
  ID and one typed host request, replays completed responses after M1 reopen,
  consumes reconciled responses, rejects conflicting ID reuse, and never owns
  or advances an Agent loop;
- restart rejection for unresolved occurrences and a receipt-loss fault test
  proving a completed provider call is not automatically redispatched;
- non-blocking in-memory reference journal with conflicting update identity
  rejection;
- frozen Draft 2020-12 wire schema and a Rust-validated occurrence fixture;
- durable elicitation projections and `AgentInputController` checkpoints that
  atomically move Session state to `RequiresAction` with an M1 input wait, then
  retain `RequiresAction` until the final input wait completes and only then
  return the Session to `Running`;
- self-contained Draft 2020-12 elicitation schema compilation before suspension
  and accepted-value validation before completion, with external retrieval
  disabled and fault tests proving rejection leaves the Session, wait,
  Continuation, journal, and CAS revision unchanged;
- reopen, idempotent retry, and stale-CAS tests proving input wait and Session
  projection cannot commit independently;
- query-only `AgentRecoveryController` reconciliation against the original
  binding, typed `completed`/`not_applied` resolutions, and evidence-gated
  cancellation of calls that remained `prepared`;
- fault tests proving the interaction controller replays a retained response,
  consumes a reconciled response, and never redispatches after an unresolved
  call or a lost completion receipt;
- `WorkspaceScopeController` commit integration that atomically couples the
  binding-pinned workspace occurrence, Plan-declared mutating Effect, scope
  closure, transferred obligation, outbox, Machine snapshot, and Continuation;
- workspace abort integration that leaves the scope open across ambiguity and
  closes it only after a retained receipt proves the overlay was not committed;
- workspace fault tests for successful commit/abort, exact replay, explicit
  non-application, provider failure, CAS receipt loss, process reopen, and
  reconciliation without redispatch;
- deterministic fake-host end-to-end tests;
- adapter boundaries for ACP, MCP, A2A, editors, and model providers.

## Remaining completion gates

- streaming chunk staging and finalized durable content;
- ACP/MCP/A2A adapters and cross-language SDK interaction clients;
- cancellation, refusal, host failure, and restart-level fault suites;
- field-complete Session projection schema and removal of incubation rustdoc
  allowances.

Capability advertisement, authentication, permission, credential access, and
effect release remain separate decisions. A tool catalog entry never grants
execution authority.

Cymule deliberately does not define Agent/script loop phases, ordering,
strategy, program counters, or model-tool continuation rules. Those belong to
the caller or protocol adapter. The profile governs only individually identified
interactions, their durable observations, and the boundary for resuming caller
code with a retained response.

Version decision: schema enforcement and the interaction controller complete
behavior already represented by the frozen elicitation and host-occurrence
fields. Workspace scope integration composes those existing fields with M0
scope/effect semantics and M1 application-journal/outbox records. These changes
alter no frozen wire shape or version while M2 remains partial.
