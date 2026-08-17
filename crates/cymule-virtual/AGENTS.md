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
- Cursors are immutable logical progress tokens returned by a source and stored
  before more work is requested.
- `cymule.virtual-checkpoint/1` records persist cursor and complete bounded
  frontier state through the M1 application journal. Checkpoint IDs form an
  explicit parent chain; conflicting reuse and stale CAS roll back the
  in-process scheduler.
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
- Resolve the immutable execution binding before claim admission. Each claim
  creates one `cymule.virtual-work-occurrence/1` record keyed by work and epoch;
  owner and binding are evidence, not mutable scheduler hints.
- Attempt dispositions are closed: success, retry, park, terminal failure, or
  cancellation. Retry preserves failure evidence and requeues or parks the same
  logical work; its next claim gets a new epoch and may select a new binding.
  Cancellation fences late worker output.
- Retry classification and limits belong to explicit policy/control callers.
  Never infer retryability from provider error strings or worker exit codes.
- Claim and disposition checkpoints use stable command IDs. Result, failure, or
  cancellation Artifacts commit atomically with the occurrence and frontier.
- Control checkpoints retain the exact command and occurrence receipt. Replaying
  an old command after later scheduler checkpoints returns that original receipt;
  command ID reuse with different semantics fails.
- Tests must use logical cardinalities much larger than active frontiers and
  prove snapshot/restore, fairness, parking, waking, and bounded memory.
- Cross-profile tests must prove M1 wait activation and M3 exact-index wake are
  one CAS transition and that a projection conflict commits neither side.
