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
- effect enqueue, scope commit, dispatch-start, Applied/Unknown observation, and
  reconciliation fault windows retain exact Machine/outbox atomicity and reject
  unrelated canonical deltas; prepare response loss reuses the same intent;
- `unknown` outbox entries remain reconciliation-eligible across repeated
  process reopen and can later settle under the original claim;
- nested Region frames and scope stacks resume from sealed-Plan paths; child
  effects remain staged until child commit and survive lost enqueue/commit
  receipts without duplicate dispatch;
- observational eager effects bind durable results before scope commit, while
  explicit effects remain prepared until an idempotent caller release after
  commit; claim, `Unknown`, and settlement receipt loss are fault-tested;
- identified signal/timer activation receipts atomically match and complete
  selected waits, enforce consume-once competition, survive redelivery/reopen,
  and resume under a new fenced Attempt epoch;
- bounded clock/signal source drivers, parked-index integration, and their
  additional crash windows remain proposed;
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

Status: implemented.

- virtual regions, opaque cursors, bounded materialization, parked indexes,
  capability-aware claims, fencing, deterministic Run fairness, and portable
  scheduler snapshots are implemented;
- M1-backed versioned checkpoints atomically persist source cursors and bounded
  frontiers, exact reason indexes avoid parked-work scans, and wait activation
  can commit its M3 indexed wake in the same CAS revision;
- every claim creates a binding-pinned occurrence; retry, park, success,
  terminal failure, and cancellation are durably recorded with owner/epoch
  plus lease fencing and atomic result/evidence Artifacts;
- Rust, TypeScript, Python, and Go expose the same occurrence and idempotent
  control-command contracts through transport-neutral interfaces;
- integer weighted-deficit selection accounts for item cost, durable priority
  aging prevents fixed-priority starvation, and region round-robin preserves
  visibility under a one-item frontier;
- opaque cursor split/merge uses pinned adapter verification and coverage
  evidence, atomically retires sources/activates targets, preserves historical
  work identity, and exposes four-language control contracts;
- completed regions compact through a pinned immutable byte archive into
  authenticated summaries/certificates, retain terminal fence and binding
  evidence, checkpoint the manifest Artifact atomically, and partially
  rehydrate exact selected occurrences after full content verification;
- archive write/read failure, tamper, stale CAS, reopen, and old receipt replay
  are fault-tested; four SDKs expose the same compact/rehydrate controls without
  provider semantics;
- capacity-slot leases make claim and M1 authority atomic, renew the active
  lease fence, reject normal output at expiry, and require explicit fenced
  retry/fail/cancel recovery before a later worker claims a greater work epoch;
- claim, renewal, recovery, and Run-weight commands retain receipts across lost
  acknowledgements, process reopen, and stale CAS; Rust, TypeScript, Python, and
  Go expose the same transport-neutral scheduling controls;
- million-item tests prove bounded frontiers, fairness, park/wake, stale-owner
  rejection, multi-worker takeover, and restore behavior.

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
