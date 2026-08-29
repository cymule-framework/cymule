# Cymule SQLite Store

cymule-store-sqlite is the embedded SQLite realization of Cymule's
provider-neutral DurableStore. The current physical generation is
cymule.sqlite-store/6.

One small canonical StoreHead points to a fixed, content-addressed StateRoot
manifest. The StateRoot table stores the closed physical envelope whose map and
log variants use the lower authenticated-collection `/1` node preimages. Machine
command-archive objects use a second closed namespace
because they are cold Core replay authority rather than the materialized M1
projection. Atomic batch objects remain content-addressed by receipt ID and
carry one unique nullable stable `batch_id` column for exact lookup without a
segment scan. Checkpoints, state-segment chains, suffix pointers, and
whole-state rows do not exist in this generation.

    cargo add cymule-store-sqlite

The adapter enables WAL plus full synchronous durability for file databases and
uses zero busy timeout. A semantic commit runs in one immediate transaction:
it compares the exact canonical expected head, including its physical token and
GC sequence, inserts immutable objects with raw canonical-byte equality, and
conditionally publishes the complete next head. A missing response after commit
begins is CommitOutcomeUnknown; callers reopen and reconcile instead of retrying
blindly.

The Store domain is admitted before SQLite is opened and contains 1..=512
non-control Unicode scalar values, matching the Engine schema rather than a
UTF-8 byte-length limit.

SqliteStore::open_read_only performs no configuration or schema writes. Every
open requires exactly the five /6 non-system schema objects and exact DDL.
Older /5 checkpoint/segment stores, whole-state generations, partial schemas,
and databases containing any foreign table, index, view, or trigger are rejected
before mutation. There is no current-type importer, repair path, or compatibility
reader.

Application callers enter cold reclamation only through the no-argument
DurableStoreControl::reconcile_cold_reclamation() and
DurableStoreControl::advance_cold_reclamation() methods. SqliteStore receives
only their opaque coordinator-issued StoreReclamation capability and derives
the exact expected head from it; a caller-assembled StoreHead is not a physical
mutation command. Ordinary load_head pins and authenticates only the bounded
head in one deferred transaction, including the optional receipt content-ID
shape carried by the head. It performs no GC-receipt table probe and reads zero
receipt, StateRoot, or Machine archive bytes. Manifest reads, Durable-owned
resolver callbacks, exact-key queries, and semantic commits likewise never
follow the physical receipt. load_full_audit is the explicitly named complete
projection traversal; explicit GC pins the head, full receipt, and every
reachable object in one immediate transaction.
StoreStats also pins its archive inventory, StateRoot count, and receipt count
to one deferred read transaction, so a concurrent semantic commit or GC
generation cannot produce a cross-generation mixture.
Every head-pinned, reachable, or exact-loaded object rejects a missing value,
wrong closed variant, non-canonical BLOB, locator/content-identity mismatch, or
cross-family identity alias as durable integrity corruption. Ordinary
unreachable StateRoot and Machine archive rows are not semantic authority: GC
validates their bounded locator and physical-family uniqueness, then may delete
them without decoding an orphan payload. Immutable writes reject an existing
alias or stable-batch mapping conflict before semantic head publication. GC
retains the complete StateRoot, Machine archive, archive-entry, indexed batch,
and sparse-index closure; its bounded
receipt-authorized sweep, physical-token advance, and head publication are
atomic.
One semantic CAS supersedes, but never consumes, the optional physical receipt
pinned by its expected head. It reads zero receipt bytes, preserves the GC
sequence, and atomically publishes a successor without a receipt pointer; the
old receipt remains ordinary physical orphan inventory for a later explicit GC
generation.
StateRoot lowering and journal replacement previews stay inside Durable and
run only through SqliteStore's transaction-pinned resolver callback; the
adapter does not construct semantic transitions or expose a second preview
surface.
Reconciliation only verifies the exact receipt pinned by the reopened head,
proves its page disjoint from the current StateRoot and Machine archive/index
closure, and idempotently completes that deletion page;
it never advances a non-terminal page. Because reconciliation never changes
the head, a failed or lost response is retried against that same exact head;
CommitOutcomeUnknown is reserved for a transaction that may have published a
new head. Advancement is the sole operation that
publishes a new physical generation: in the same immediate transaction it first
replays the current page, then streams the next exact inventory while retaining
only bounded prefixes. The head-pinned receipt is selected first, then other
receipt-family rows in identity order; the remaining page slots take the
lexicographic prefix across StateRoot and Machine archive candidates. This drains
crash orphans without displacing the current lifecycle receipt. A
cross-family identity alias is corruption. Non-terminal generations retain an
exact remaining count; the terminal generation leaves only its new current
receipt row. Larger candidate sets drain only through explicit successive
advancements.
SQL length gates apply the owning Core or Durable protocol limit before a BLOB
is returned to Rust, so oversized physical corruption is rejected before JSON
allocation or decoding.

SQLite is appropriate for local development, desktop applications, and
single-node services. It does not provide distributed ownership or failover.
Conformance covers statement rollback, exact-head conflicts, process death
around object/head/commit and GC boundaries, full integrity checks, WAL
checkpointing, and exact reopen readback. It does not claim a custom SQLite VFS,
torn-sector, or physical power-loss fault model.
