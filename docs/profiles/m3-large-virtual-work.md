# M3 Large Virtual Work Profile

Status: partial.

## Implemented foundation

- provider-neutral `RegionSource` with opaque versioned cursors and bounded page
  materialization;
- logical region cardinality independent from materialized work cardinality;
- explicit global, active, per-Run, and page-size frontier limits;
- deterministic per-Run round-robin fairness with priority ordering inside a
  Run;
- capability-aware claims with monotonically increasing fencing epochs;
- stale-completion rejection;
- indexed park/wake reasons for waits, dependencies, budgets, capabilities, and
  backpressure;
- portable scheduler snapshots and restore-time bound validation;
- million-item source tests proving an eight-item bounded frontier, fairness,
  parking, waking, fencing, and restart behavior.

## Remaining completion gates

- durable M1 persistence of virtual scheduler snapshots and cursor commits;
- active-work retry, cancellation, failure, and result occurrence records;
- weighted cost budgets, priority aging, and starvation proofs;
- partition split/merge and cursor migration;
- subtree completion summaries, compaction certificates, and partial
  rehydration;
- multi-worker crash tests and SDK query/control interfaces.

`RegionSource` implementations may enumerate a database, object store, API, or
generated range, but those technologies never enter M3 semantic state.
