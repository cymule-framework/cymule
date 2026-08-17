# Roadmap

The roadmap is profile-driven. A milestone is complete only when its complete
fault-oriented conformance family passes.

## M0 - Semantic kernel

Status: implemented in 0.1.0.

- frozen IR and canonical identity;
- VEC and attempt epoch fencing;
- command admission and causal events;
- scope/effect/obligation state machines;
- occurrence binding and future-default update;
- exact state-replay availability classification over required artifacts;
- embedded execution and four language SDKs.

## M1 - Durable single domain

Status: partial.

- provider-neutral durable state CAS, full Continuation data, waits, leases,
  outbox, component occurrences, and snapshot records are implemented;
- memory and atomic directory-store adapters pass reopen and stale-writer tests;
- sequential component/wait execution resumes after process reopen and replays
  recorded component outputs without reinvocation;
- root commit-gated effects persist outbox claims before provider execution and
  reconcile rather than redispatch after crash ambiguity;
- `unknown` outbox entries remain reconciliation-eligible across repeated
  process reopen and can later settle under the original claim;
- identified signal/timer activation receipts atomically match and complete
  selected waits, enforce consume-once competition, survive redelivery/reopen,
  and resume under a new fenced Attempt epoch;
- nested scope and non-commit-gated effect resumption, bounded clock/signal
  source drivers, parked-index integration, atomic event-plus-outbox, and
  remaining dispatch crash windows are proposed;
- process-level crash injection for every effect window;
- snapshot compaction and suffix rehydration;
- canonical component-call occurrences and exact execution replay without
  reinvoking plugins;
- provider-neutral cross-Run Resource descriptors, replay classification,
  bounded resolver/store interfaces, M1 handoff journals, and four SDK builders
  are implemented; production adapters and interpreter activation remain.

## Optional plugin track - Agent interaction

Status: partial.

This track is not a Cymule framework milestone or a requirement for M1, M3, or
M4 conformance. The optional
[`plugins/agent-interaction`](../plugins/agent-interaction) package owns Session,
Agent-host occurrence, input, workspace, and stream controllers. It uses the
generic M1 CAS, application journals, waits, effects, scopes, resources, and
binding rules without exporting Agent-domain types from the framework CLI or
language SDKs.

The Rust plugin currently includes durable projection/replay, input suspension,
binding-pinned host interactions, no-redispatch reconciliation, workspace
effect integration, staged stream finalization, and fault-oriented reopen/CAS
tests. Its remaining gates and exact behavior live in the
[plugin profile](../plugins/agent-interaction/PROFILE.md). ACP, MCP, A2A,
provider, editor, and concrete Agent Loop support are separate plugin layers and
do not block the framework roadmap.

## M3 - Large virtual work

Status: partial.

- virtual regions, opaque cursors, bounded materialization, parked indexes,
  capability-aware claims, fencing, deterministic Run fairness, and portable
  scheduler snapshots are implemented;
- M1-backed versioned checkpoints atomically persist source cursors and bounded
  frontiers, exact reason indexes avoid parked-work scans, and wait activation
  can commit its M3 indexed wake in the same CAS revision;
- every claim creates a binding-pinned occurrence; retry, park, success,
  terminal failure, and cancellation are durably recorded with owner/epoch
  fencing and atomic result/evidence Artifacts;
- Rust, TypeScript, Python, and Go expose the same occurrence and idempotent
  control-command contracts through transport-neutral interfaces;
- integer weighted-deficit selection accounts for item cost, durable priority
  aging prevents fixed-priority starvation, and region round-robin preserves
  visibility under a one-item frontier;
- opaque cursor split/merge uses pinned adapter verification and coverage
  evidence, atomically retires sources/activates targets, preserves historical
  work identity, and exposes four-language control contracts;
- million-item tests prove bounded frontiers, fairness, park/wake, stale-owner
  rejection, and restore behavior;
- subtree compaction, partial rehydration, scheduling control clients, and
  multi-worker crash matrices remain proposed.

## M4 - Live evolution

Status: partial.

- immutable future binding updates and occurrence pinning are implemented;
- sealed Plan DAG nodes, content-addressed patch edges, cycle rejection,
  conservative impact cones, deterministic future-only canaries, rollback,
  safe-point migration receipts, shadow evidence, and portable snapshots are
  implemented;
- automatic Plan diff/application, schema migration adapters, shadow execution,
  observation gates, promotion, mixed-version runtime dispatch, durable control,
  and crash tests remain proposed.

## M5 - Isolation and federation

Status: proposed.

- strong execution isolation and executable provenance;
- identity, secret, policy, and egress substrates;
- multi-domain causal and authority translation.

## M6 - Formalization and optimization

Status: proposed.

- mechanized minimal state machine;
- trace-to-flow compilation and guarded specialization;
- pure-region optimization and deoptimization.
