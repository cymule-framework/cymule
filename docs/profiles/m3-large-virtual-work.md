# M3 Large Virtual Work Profile

Status: implemented.

## Implemented profile

- provider-neutral `RegionSource` with opaque versioned cursors and bounded page
  materialization;
- transactional page admission that rejects cursor-version changes,
  non-terminal stalls, empty/repeated work IDs, and partial source failures
  without advancing any cursor or frontier;
- logical region cardinality independent from materialized work cardinality;
- explicit global, active, per-Run, and page-size frontier limits;
- integer weighted-deficit Run fairness that debits exact item cost, with tests
  proving 1:3 shares and cost-normalized service;
- durable dispatch-sequence priority aging with a continuous high-priority
  adversarial test proving an older low-priority item is selected after a finite
  bound, including snapshot/restore;
- independent region round-robin materialization proving visibility fairness
  with a single frontier slot;
- capability-aware claims with monotonically increasing fencing epochs;
- stale-completion rejection;
- binding-pinned work occurrences created with every claim and retained across
  running, success, retry, park, terminal failure, and cancellation;
- idempotent disposition replay, conflicting-disposition rejection, retry under
  a new epoch/binding, and cancellation fencing of late output;
- historical command receipts that remain replayable after later checkpoints,
  without rolling the scheduler back to the command's snapshot;
- M1-backed claim and resolution checkpoints that atomically retain result,
  failure, or cancellation Artifacts with occurrence/frontier state;
- provider-neutral `VirtualWorkControl` interfaces and shared typed occurrence
  plus control fixtures in Rust, TypeScript, Python, and Go;
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
- M1 reopen of scheduling policy, Run weights/deficits, dispatch sequence,
  ready age, and last selections with identical next-claim evidence;
- provider-neutral `RegionMigrator` planning and verification for opaque cursor
  split/merge, with exact source preconditions and pinned migration binding;
- atomic source retirement, target activation, coverage Artifact, receipt, and
  scheduler checkpoint, while historical/materialized work keeps old region IDs;
- split-then-merge lineage, stale cursor, unverified evidence, target conflict,
  stale CAS, reopen, and historical command-replay tests;
- shared Rust, TypeScript, Python, and Go migration plan/control contracts that
  never interpret cursor positions;
- provider-neutral immutable `VirtualArchive` bytes with framework-computed
  manifest Artifact identity and pinned compactor binding;
- completed-region eligibility that rejects non-exhausted/non-retired regions,
  any hot work, or a non-terminal greatest occurrence;
- authenticated completion summaries and compaction certificates retaining the
  causal cut, manifest/summary/index digests, terminal work fences, occurrence
  bindings, replay availability, and compactor revision;
- exact occurrence-selection partial rehydration with full manifest/certificate
  readback validation and no implicit widening;
- M1 Artifact-plus-journal checkpoints for compaction and journal checkpoints
  for rehydration, including reopen, idempotent receipt replay, stale CAS
  rollback, injected archive read/write failure, and tamper rejection;
- shared Rust, TypeScript, Python, and Go compaction/rehydration commands and
  transport-neutral archive/control interfaces;
- provider-neutral worker capacity-slot claim commands whose M1 lease and M3
  occurrence/frontier enter one CAS, including durable empty-poll receipts;
- atomic active-claim renewal that advances the slot and occurrence lease fence
  without changing the work epoch or immutable binding;
- normal result admission fenced by owner, work epoch, lease epoch, and logical
  pre-expiry observation time;
- explicit expired-claim retry/fail/cancel recovery against the exact current
  durable lease, with evidence Artifact checkpointing and later work-epoch
  takeover that rejects old worker output;
- deterministic multi-worker tests for distinct-slot progress, same-slot
  exclusion, stale CAS, pre-expiry rejection, post-expiry takeover, late output,
  and lost claim/renewal/recovery receipts across reopen;
- idempotent future Run-weight controls with deficit reset, stale-CAS rollback,
  reopen, and historical receipt replay;
- shared Rust, TypeScript, Python, and Go `VirtualSchedulingControl` contracts
  and claim/renew/recover/Run-weight fixtures;
- million-item source tests proving an eight-item bounded frontier, fairness,
  parking, waking, fencing, and restart behavior.

## Completion boundary

This profile covers provider-neutral large virtual work inside one durable M1
domain. Cross-domain worker ownership, distributed consensus, infrastructure
autoscaling, queue delivery, and execution isolation belong to M5 or plugins and
are not M3 claims.

`RegionSource` implementations may enumerate a database, object store, API, or
generated range, but those technologies never enter M3 semantic state.

Version decision: durable scheduler integration introduces the independent
`cymule.virtual-checkpoint/1` journal payload. `VirtualSnapshot` adds a derived
parked-reason index that restore always rebuilds from parked work. Neither change
alters `cymule.semantic/2`, the Plan IR, or M1's generic application-journal
envelope. Work lifecycle adds independent `cymule.virtual-work-occurrence/1`
and `cymule.virtual-work-control/1` domains; SDKs expose their closed wire types
and transport interfaces but do not reduce scheduler state. Additive scheduling
policy, integer weight/deficit, dispatch-sequence, and ready-age fields remain
inside the `cymule.virtual-checkpoint/1` domain. Region topology adds
independent `cymule.virtual-region-migration/1` and
`cymule.virtual-region-migration-control/1` domains; receipts and retired lineage
remain in the same M3 checkpoint.
Cold history adds independent `cymule.virtual-archive-manifest/1`,
`cymule.virtual-compaction-certificate/1`,
`cymule.virtual-compaction-control/1`, and
`cymule.virtual-rehydration-control/1` domains. Their receipts, bounded summary,
and terminal fence index remain in the additive
`cymule.virtual-checkpoint/1` payload; exact occurrence bytes remain an ordinary
content-addressed Artifact behind `VirtualArchive`.
Multi-worker control adds independent `cymule.virtual-claim-control/1`,
`cymule.virtual-lease-renewal-control/1`,
`cymule.virtual-recovery-control/1`, and
`cymule.virtual-run-weight-control/1` domains. Their receipts and active lease
fences remain additive fields in `cymule.virtual-checkpoint/1`; Clock and worker
substrates supply proposals but never mutate scheduler state directly.
