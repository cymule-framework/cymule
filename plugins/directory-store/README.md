# Cymule Directory Store

cymule-directory-store is the local-filesystem realization of Cymule's
provider-neutral DurableStore.

The only physical generation is cymule.directory-store/5. Its root contains an
empty atomic cymule.directory-store-5 bootstrap-marker directory, an exact
store-meta.json completion marker, the state-root-objects, gc-receipts, and
command-archives families, and one dedicated object-staging directory.
Writable open durably ensures regular head.lock and objects.lock operational
files; read-only open neither requires nor creates them.
Unmarked, whole-state, checkpoint/segment, /3, partial, and unexpected layouts
fail with unsupported_store_generation before mutation; the online adapter
never repairs or migrates them.

Open freezes the configured root as an absolute lexical path without following
the final entry. Changing the process working directory cannot retarget an
existing handle. The versioned bootstrap marker is durably published before
head.lock, so only an explicitly marked partial initialization can resume and
even a `.` Store synchronizes its absolute entry in the parent directory.

Writable generation /5 currently requires Unix file and directory fsync plus
atomic replacement semantics. Other platforms fail before initialization
rather than acknowledge an unproven durability protocol; read-only inspection
of an existing exact layout is still available.

Closed typed StateRoot physical envelopes are immutable content-addressed files;
map/log variants use the lower authenticated-collection `/1` node preimages. The
shared command-archives namespace stores Machine archive segments,
independently addressable entries, complete atomic batch receipts, their
immutable stable-batch secondary indexes, and sparse command-index nodes.
Batch reads and GC traverse reachable entries through that index and never
scan archive segments. StateSegment is not persisted or used for reopen. One complete head.json is the sole CAS
head, including the exact physical token and GC sequence.
The physical token is an opaque monotonic CAS lineage and fencing value;
reopen authenticates the pinned manifest closure rather than treating that
token as a second state-content commitment.
Exact journal-record manifests, prefix-replacement authorities, and coupled
checkpoint receipts resolve only through the current head-pinned StateRoot and
its typed sparse-map nodes. Every current-head StateRoot read loads the exact
physical manifest and requires complete equality with any caller snapshot.
Ordinary head, manifest, resolver, exact-key, and semantic commit paths never
follow or decode the optional head-pinned GC receipt; that receipt is physical
reclamation authority only. These lookups reject stale, missing, or corrupt
StateRoot authority and never materialize or scan cumulative journal history.
Normal commit admission and the publication-lock recheck enforce the same
StateRoot authority before replacing a current head.
StateRoot lowering and journal replacement previews execute inside Durable
through the adapter's transaction-pinned resolver callback; DirectoryStore
does not construct semantic transitions or expose a separate preview seam.

A commit synchronizes every immutable object, atomically replaces the head, and
synchronizes the owning directories. A directory-sync failure after the head
rename is an unknown commit outcome and must be reconciled by reopen. All reads
are bounded no-follow regular-file reads with exact canonical-byte,
closed-kind, locator, and content-identity validation. Page-slot subtraction
and the one-byte over-limit read probe use checked exact arithmetic; an
unrepresentable capacity fails before I/O rather than saturating.
A semantic CAS supersedes, but never consumes, the optional physical GC receipt
pinned by its expected head: it reads zero receipt bytes, preserves the GC
sequence, and publishes a successor without a receipt pointer. The old receipt
is then ordinary physical orphan inventory for a later explicit GC generation;
semantic commit never replays or deletes it.
An identity present in more than one physical family is rejected on immutable
write and exact read, before a semantic head can publish over that alias.
Immutable staging and fsync use a separate non-blocking objects.lock; the small
head.lock critical section contains only exact head recheck, final family sync,
head replacement, and bounded GC sweep. The object lock remains held through
head publication so receipt replay cannot delete a just-reintroduced object.
Unique temporary files live only in object-staging, so cleanup is proportional
to crash residue and never scans committed object families.
Writable load_head cleans stale object staging when objects.lock is idle. If
another writer actively owns that lock, the bounded head plus its exact pinned
receipt identity remain readable. Ordinary load_head reads zero GC-receipt,
StateRoot, or Machine archive object bytes, does not replay a receipt, and does
not mutate cold inventory; load_full_audit is the explicitly named complete
projection traversal.
Physical stats count no-follow canonical locators behind a stable head read but
never open or decode object payloads, so stats are not a hidden receipt
validation path.

The root must remain an owner-exclusive trusted subtree while the adapter is
open. No-follow rejects unsafe static final entries; this local adapter is not
a hostile same-UID multi-tenant filesystem boundary against concurrent family
renames or head.lock replacement.

Machine command archives remain independent of materialized M1 state. Reopen
loads them only through typed accessors, including exact `batch_id` lookup of a
complete batch receipt. Application callers enter cold
reclamation only through the no-argument
DurableStoreControl::reconcile_cold_reclamation() and
DurableStoreControl::advance_cold_reclamation() methods. DirectoryStore receives
only their opaque coordinator-issued StoreReclamation capability and derives
the exact expected head from it; a caller-assembled StoreHead is not a physical
mutation command. Reconciliation requires that exact current head and its
pinned receipt, loads only that bounded receipt page, revalidates it against
the current reachable closure, and idempotently completes only its authorized
deletions. It never publishes another head, including when remaining_objects is
nonzero. Writable and read-only ordinary reopen validate only the bounded head
and the receipt identity shape carried by it, without reading receipt bytes;
reclamation recovery always requires an explicit coordinator reconcile call.
open_read_only performs no creation, cleanup, locking, or GC completion.

Coordinator advance_cold_reclamation() is the sole next-generation operation.
It first resolves the exact StateRoot and Machine archive closure outside both
adapter locks and rechecks head stability. Under objects.lock it rechecks the
exact head, completes any current receipt, and streams every physical family
without materializing the cold inventory. It retains only a 1,024-identity
page while counting the exact remainder. When the expected head pins a
predecessor receipt, that identity is mandatory. The remaining receipt-family
candidates form the next priority tier in lexical order; only the slots left
after that tier contain the lexicographically smallest ordinary candidates
across all other families. Even pathological pre-head crash residue stays bounded and
converges through explicit later pages instead of making reclamation
unavailable. The new receipt and exact physical head are then published by CAS
before its page is swept. A final successful generation with
remaining_objects=0 deletes every older receipt file and leaves only the new
current receipt.

The 1,024-object receipt and unlink bound is provider-local and independently
enforced on replay, regardless of the larger portable receipt limit.
GcReceipt.remaining_objects is the explicit continuation signal; a caller must
invoke advance explicitly for every later page or new post-receipt orphan
cycle. Retrying a lost acknowledgement reconciles the reopened head and never
implicitly advances it. During advance, a sweep failure after the newly
generated receipt/head may have become durable is an unknown commit outcome;
writable reopen verifies that exact head, after which explicit reconciliation
idempotently completes the same page. Reconciliation and the prior-page sweep
at the start of advance publish no new head, so a failure there leaves the old
expected head authoritative and its partially completed receipt page safely
replayable.
