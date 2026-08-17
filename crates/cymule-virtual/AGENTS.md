# Large Virtual Work Guidance

- Logical work cardinality must remain independent from materialized in-memory
  or ready-queue cardinality.
- `RegionSource` is the provider boundary. Never encode a database query,
  object-store listing, queue, or partition product in virtual-work semantics.
- Cursors are immutable logical progress tokens returned by a source and stored
  before more work is requested.
- Enforce global and per-Run frontier bounds before materialization. Backpressure
  is a semantic scheduler result, not an out-of-memory fallback.
- Fairness must be deterministic over identical scheduler state. Capability
  mismatch parks or skips work; it must not silently discard it.
- Claims use monotonically increasing epochs and reject stale completion.
- Tests must use logical cardinalities much larger than active frontiers and
  prove snapshot/restore, fairness, parking, waking, and bounded memory.
