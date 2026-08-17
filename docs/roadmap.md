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
- nested scope and non-commit-gated effect resumption, timer/signal activation,
  atomic event-plus-outbox, and remaining dispatch crash windows are proposed;
- process-level crash injection for every effect window;
- snapshot compaction and suffix rehydration;
- canonical component-call occurrences and exact execution replay without
  reinvoking plugins;

## M2 - Agent and script integration

Status: partial.

- typed content, Session updates, Plans, tool lifecycle, context/model/tool,
  permission, elicitation, and workspace interfaces are implemented;
- provider-neutral ordered Session journals, validate-before-append updates,
  idempotent append, and projection replay after reopen are implemented;
- Agent updates can use the same M1 whole-state CAS through typed durable
  application journal records;
- all six replaceable agent host boundaries persist request-digested,
  binding-pinned occurrences and fail closed after ambiguous receipt loss;
- typed input requests atomically couple `RequiresAction` and `Running` Session
  projections to M1 wait registration and completion across process reopen;
- ambiguous host calls reconcile through their original binding to typed
  `completed` or evidence-backed `not_applied` without redispatch;
- a bounded reference turn driver passes context-model-tool-model end-to-end
  tests;
- durable foreground turn control, completed-input schema enforcement,
  workspace scope semantics, streaming finalization, protocol adapters,
  debugger queries, and evidence views remain proposed.

## M3 - Large virtual work

Status: partial.

- virtual regions, opaque cursors, bounded materialization, parked indexes,
  capability-aware claims, fencing, deterministic Run fairness, and portable
  scheduler snapshots are implemented;
- million-item tests prove bounded frontiers, fairness, park/wake, stale-owner
  rejection, and restore behavior;
- durable cursor integration, retry/failure records, weighted fairness,
  partition migration, subtree compaction, and partial rehydration remain
  proposed.

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
