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
- resumable interpretation, timer/signal activation, atomic event-plus-outbox,
  and dispatch crash-window recovery remain proposed;
- process-level crash injection for every effect window;
- snapshot compaction and suffix rehydration;
- canonical component-call occurrences and exact execution replay without
  reinvoking plugins;

## M2 - Agent and script integration

Status: proposed.

- context snapshots and typed model/tool effects;
- workspace overlays and human input;
- debugger query protocol and evidence views.

## M3 - Large virtual work

Status: proposed.

- virtual regions, durable cursors, and parked indexes;
- bounded active frontiers, backpressure, and fairness;
- subtree compaction and partial rehydration.

## M4 - Live evolution

Status: partial.

- immutable future binding update and occurrence pinning are implemented;
- Plan DAG patches, impact cones, mixed-version adapters, safe-point state
  migration, shadow/canary, and rollback remain proposed.

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
