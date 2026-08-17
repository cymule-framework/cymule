# M3 Large Virtual Work Profile

Status: partial.

## Implemented foundation

- provider-neutral `RegionSource` with opaque versioned cursors and bounded page
  materialization;
- transactional page admission that rejects cursor-version changes,
  non-terminal stalls, empty/repeated work IDs, and partial source failures
  without advancing any cursor or frontier;
- logical region cardinality independent from materialized work cardinality;
- explicit global, active, per-Run, and page-size frontier limits;
- deterministic per-Run round-robin fairness with priority ordering inside a
  Run;
- capability-aware claims with monotonically increasing fencing epochs;
- stale-completion rejection;
- indexed park/wake reasons for waits, dependencies, budgets, capabilities, and
  backpressure;
- portable scheduler snapshots and restore-time bound validation;
- restore validation for region/run identity, duplicate scheduler placement,
  known-set coverage, claim fencing, and global/per-Run active limits;
- a rebuildable exact `ParkReason -> work IDs` index, with M1 wait IDs used as
  activation keys and no parked-population scan on wake;
- versioned `cymule.virtual-checkpoint/1` records that persist opaque source
  cursors and the complete bounded frontier through an M1 application journal;
- checkpoint parent lineage, idempotent retry, conflicting-ID rejection,
  reopen, and in-process rollback after stale CAS;
- atomic M1 wait activation plus M3 indexed wake checkpoints, including a stale
  writer fault test proving neither side partially commits;
- million-item source tests proving an eight-item bounded frontier, fairness,
  parking, waking, fencing, and restart behavior.

## Remaining completion gates

- active-work retry, cancellation, failure, and result occurrence records;
- weighted cost budgets, priority aging, and starvation proofs;
- partition split/merge and cursor migration;
- subtree completion summaries, compaction certificates, and partial
  rehydration;
- multi-worker crash tests and SDK query/control interfaces.

`RegionSource` implementations may enumerate a database, object store, API, or
generated range, but those technologies never enter M3 semantic state.

Version decision: durable scheduler integration introduces the independent
`cymule.virtual-checkpoint/1` journal payload. `VirtualSnapshot` adds a derived
parked-reason index that restore always rebuilds from parked work. Neither change
alters `cymule.semantic/1`, the Plan IR, or M1's generic application-journal
envelope.
