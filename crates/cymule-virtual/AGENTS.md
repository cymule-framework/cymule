# Large Virtual Work Guidance

- Logical work cardinality must remain independent from materialized in-memory
  or ready-queue cardinality.
- `RegionSource` is the provider boundary. Never encode a database query,
  object-store listing, queue, or partition product in virtual-work semantics.
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
- `parked_index` is a rebuildable exact-reason index. Wake-up must look up the
  reason directly rather than scan all parked work. A wait park reason uses the
  exact M1 wait ID so one identified activation can wake only its selected work.
- Claims use monotonically increasing epochs and reject stale completion.
- Tests must use logical cardinalities much larger than active frontiers and
  prove snapshot/restore, fairness, parking, waking, and bounded memory.
- Cross-profile tests must prove M1 wait activation and M3 exact-index wake are
  one CAS transition and that a projection conflict commits neither side.
