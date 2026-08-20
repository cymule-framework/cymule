# Agent Interaction Plugin Profile

Status: implemented optional plugin profile.

This profile belongs to `plugins/agent-interaction`, not to the Cymule
framework, semantic kernel, CLI, or language SDKs. It defines one possible
Agent-domain integration over the generic M1 durability interfaces.

## Boundary

The plugin owns:

- typed Agent content, messages, plans, tool lifecycle, usage, and Session
  projections;
- Agent-host occurrences and controllers for context, model, permission, tool,
  elicitation, and workspace interactions;
- Session input suspension and completion;
- Agent message and tool-output stream staging and finalization;
- the `cymule.agent-stream/1` domain version, Agent protocol schema, fixtures,
  and plugin conformance suite.

The plugin does not own the Cymule execution model. It lowers its state through
M1 application journals, waits, bindings, effects, scopes, resources, outbox
records, and whole-state CAS checkpoints. It cannot widen authority, treat a
catalog entry as permission, or bypass a Plan-declared world effect.

The framework does not interpret Agent Loop phases, model/tool ordering,
strategy, program counters, or termination. The included `AgentTurnDriver` is a
bounded reference convenience for plugin tests, not framework semantics.

ACP, MCP, A2A, editor, model-provider, and concrete Agent Loop support belongs
in separate adapters or plugins above this package. Those adapters are not
completion gates for this protocol-neutral plugin.

## Implemented profile

- ordered Session projection with validate-before-append updates, idempotent
  update IDs, conflicting-reuse rejection, tool lifecycle checks, and replay;
- `AgentJournal` and `AgentOccurrenceStore` interfaces with non-blocking memory
  references and M1 `DurableCoordinator` integration;
- request-digested, binding-pinned Agent-host occurrences with retained typed
  responses and `prepared -> started -> completed | unknown | not_applied`
  persistence;
- a caller-driven `AgentInteractionController` that replays completed or
  reconciled responses, rejects conflicting IDs, and never advances a loop;
- query-only recovery against the original pinned binding, including explicit
  non-dispatch evidence for prepared-call cancellation and no redispatch after
  ambiguity;
- input checkpoints that atomically couple the Session projection to an M1
  input wait and Continuation state across suspend, complete, reopen, and stale
  CAS attempts;
- self-contained Draft 2020-12 input schema compilation and accepted-value
  validation with filesystem and HTTP resolution disabled;
- workspace overlay commit and abort controllers that lower mutations to
  Plan-declared effects, scope obligations, outbox records, retained
  occurrences, Machine snapshots, and Continuations under one M1 CAS;
- `cymule.agent-stream/1` open, chunk, finalized, and aborted records with
  stable targets, contiguous sequence identity, immutable final content, and
  Resource Handles for large output;
- multi-journal stream finalization that keeps staged chunks outside Session
  authority and publishes the terminal stream record plus exact Session update
  atomically;
- plugin-owned JSON Schema validation for Agent occurrence and stream fixtures;
- fault-oriented Rust tests for reopen, retry, conflicting reuse, stale CAS,
  receipt loss, unknown reconciliation, abort, and immutable output identity.
- cancellation, refusal, and host-failure matrices across context, model,
  permission, tool, elicitation, and workspace calls, including proof that
  ambiguous retries never redispatch;
- real child-process death on both sides of every prepared/started/completed
  occurrence checkpoint plus the Session journal and atomic stream-finalization
  checkpoint, with SQLite reopen and provider-call counts;
- field-complete Session JSON Schema and fixture, including stop reason,
  ordered finalized messages, Plan, tools, usage, elicitations, and applied
  update identities;
- complete public rustdoc without incubation allowances.

Protocol-specific clients remain optional adapter work, not a gap in this
protocol-neutral profile.

Capability advertisement, authentication, permission, credential access, and
effect release remain separate decisions. A tool or model catalog entry never
grants execution authority.

## Compatibility

The Rust package and crate names remain `cymule-agent` and `cymule_agent` for
source compatibility. Location and ownership, rather than the crate identifier,
define the boundary: the package lives under `plugins/`, is an optional
workspace member, and is not a dependency of the framework CLI or SDK crates.

Streaming uses the plugin-owned `cymule.agent-stream/1` version domain. Changes
to Session, occurrence, or stream semantics require a plugin version decision;
they do not change `cymule.semantic/3` unless the generic framework laws change.
