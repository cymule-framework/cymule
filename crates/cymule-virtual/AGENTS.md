# Large Virtual Work Guidance

- Logical work cardinality must remain independent from materialized in-memory
  or ready-queue cardinality.
- `RegionSource` is the provider boundary. Never encode a database query,
  object-store listing, queue, or partition product in virtual-work semantics.
- `RegionMigrator` owns opaque cursor split/merge and MUST verify coverage under
  the pinned migration binding before apply. Framework code checks exact source
  cursors, cardinality, Run/source authority, IDs, and evidence retention; it
  never parses cursor positions.
- Migration retires source regions instead of deleting them. Existing ready,
  active, parked, known, and occurrence records retain the old region ID. New
  targets cover only future materialization.
- Migration command replay retains the original receipt after later checkpoints.
  Conflicting command/migration IDs, stale cursors, unverified evidence, target
  collisions, or stale CAS retire nothing.
- A completed region may move exact occurrence records to a `VirtualArchive`
  only when exhausted or retired and free of ready, active, and parked work.
  The framework computes the manifest Resource descriptor, summary digests,
  certificate identity, terminal fence index, retained bindings, and replay
  classification; an archive plugin only stores/loads exact immutable bytes.
- Cold archive manifest bytes never return to the hot Machine Artifact map.
  The compaction certificate retains only the verified semantic Resource
  descriptor; the pinned archive binding resolves those bytes for rehydration.
- Compaction failure may leave an unreferenced immutable archive object but MUST
  roll back scheduler and M1 state. Rehydration verifies bytes, Resource ID,
  manifest digest, certificate, work index, causal cut, and binding before
  restoring only the requested occurrence IDs.
- Cursors are immutable logical progress tokens returned by a source and stored
  before more work is requested.
- `cymule.virtual-checkpoint/2` records persist cursor and complete bounded
  frontier mutations as content-addressed incremental deltas through the M1
  application journal. Every delta has a hard encoded-size bound and
  authenticates its parent and resulting transition head; no record repeats a
  full `VirtualSnapshot`. Checkpoint IDs form an explicit parent chain;
  conflicting reuse and stale CAS roll back the in-process scheduler.
- A scheduler loaded from M1 caches its exact durable checkpoint anchor and
  prior projection only in process. Successful checkpoint APIs mutably advance
  that cache; they must reject a different journal head and never replay the
  full delta history merely to construct the next record.
- A `RegionSource` must return the same page and successor cursor for the same
  immutable region cursor. Receipt loss may cause the page request to be
  repeated after reopen.
- Reject cursor-version changes, non-terminal stalled cursors, empty work IDs,
  and repeated work IDs before committing any part of a materialized page.
- Enforce global and per-Run frontier bounds before materialization. Backpressure
  is a semantic scheduler result, not an out-of-memory fallback.
- Fairness must be deterministic over identical scheduler state. Capability
  mismatch parks or skips work; it must not silently discard it.
- Run fairness uses integer deficit accounting: add `base_quantum * weight` and
  debit exact `WorkItem.cost`. Never use floating point, wall time, queue length,
  or provider latency as replay authority.
- Priority aging uses durable successful-dispatch sequence and `ready_since`,
  not a clock. Within one Run, select the greatest base-priority-plus-age score;
  stable queue order breaks ties.
- Weighted throughput claims cover continuously backlogged, materialized,
  capability-compatible Runs. Region materialization has a separate
  round-robin visibility guarantee and must not pretend to know item cost before
  a source returns the item.
- Persist scheduling policy, Run weights/deficits, dispatch sequence, ready age,
  and last selected Run/region. Snapshot restore must produce the same next
  claim and reject zero weights, invalid policy, or future age timestamps.
  Changing a Run weight resets its prior deficit so old shares cannot create a
  burst under the new policy.
- `parked_index` is a rebuildable exact-reason index. Wake-up must look up the
  reason directly rather than scan all parked work. A wait park reason uses the
  exact M1 wait ID so one identified activation can wake only its selected work.
- Claims use monotonically increasing epochs and reject stale completion.
- Durable multi-worker claims also bind one abstract capacity-slot lease.
  Slot IDs express bounded capacity, not a queue, host, Pod, process, or worker
  registry. One slot has at most one active claim; separate slots may progress
  independently through optimistic M1 CAS without blocking locks.
- `claim_command_and_checkpoint_with_journals` is the exact cross-profile seam
  for atomic version selection plus worker claim. Additional records must be
  retained identically on replay; a claim receipt without its coupled record is
  corruption, not a reason to append the missing side later.
- Claim, renewal, and Run-weight controls retain stable command receipts. A
  no-eligible-work claim checkpoints an empty receipt but acquires no lease.
  Renewal atomically advances the M1 lease and active occurrence lease epoch.
- Normal worker resolution supplies exact work/lease epochs and Clock-provided
  logical observation time and must precede lease expiry. Expiry authorizes no
  implicit mutation; a recovery controller must explicitly retry, fail, or
  cancel under the current expired durable lease. Takeover fences old output.
- Resolve the immutable execution binding before claim admission. Each claim
  creates one `cymule.virtual-work-occurrence/1` record keyed by work and epoch;
  owner and binding are evidence, not mutable scheduler hints.
- Attempt dispositions are closed: success, retry, park, terminal failure, or
  cancellation. Retry preserves failure evidence and requeues or parks the same
  logical work; its next claim gets a new epoch and may select a new binding.
  Cancellation fences late worker output.
- Retry classification and limits belong to explicit policy/control callers.
  Never infer retryability from provider error strings or worker exit codes.
- M1 integration fixtures retain every Continuation input Artifact before
  checkpointing virtual waits; a digest-shaped dangling reference is invalid.
- Claim and disposition checkpoints use stable command IDs. Result, failure, or
  cancellation Artifacts commit atomically with the occurrence and frontier.
- Control checkpoints retain the exact command and occurrence receipt. Replaying
  an old command after later scheduler checkpoints returns that original receipt;
  command ID reuse with different semantics fails.
- Tests must use logical cardinalities much larger than active frontiers and
  prove snapshot/restore, fairness, parking, waking, bounded memory, linear
  journal growth, bounded record size, and exact reopen from delta history.
- Cross-profile tests must prove M1 wait activation and M3 exact-index wake are
  one CAS transition and that a projection conflict commits neither side.
