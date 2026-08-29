# Directory Store Adapter Guidance

- This adapter is a concrete local-filesystem realization of DurableStore, not
  part of Cymule's canonical storage contract.
- cymule.directory-store/5 is the only physical generation. Its strict root
  contains the empty atomic cymule.directory-store-5 bootstrap marker,
  store-meta.json, the exact state-root-objects, gc-receipts, and
  command-archives families, and the dedicated object-staging directory.
  Writable open durably ensures regular head.lock and objects.lock operational
  files; read-only open neither requires nor creates them.
  Whole-state, checkpoint, segment, unmarked, every /1 through /4 marker,
  partial, and extended layouts fail before mutation and are never repaired.
- Writable /5 authority is Unix-only: it requires proven file and directory
  fsync plus atomic replacement semantics. Other platforms fail before
  initialization with a durability substrate error, never an unsupported-store
  generation error, instead of acknowledging an unproven durability protocol;
  read-only inspection of an existing exact layout remains separate.
- Persist only the closed StateRootObject physical envelope, including the
  authenticated-map/log node `/1` preimages owned by the lower collection
  crate, and independent Machine
  command-archive objects. StoreBatch admission is authenticated directly by
  the exact parent/result StateRoot manifests; no StateSegment exists in this
  generation.
- StateRoot lowering and journal replacement preview remain Durable-owned
  semantic operations. The adapter exposes only a transaction-pinned
  `with_state_root_resolver` callback and never constructs a transition or
  publishes a provider-specific preview API.
- `StateRootResolver` returns `None` for a physically absent content identity.
  Immutable map/log builders use that exact Optional contract to probe newly
  derived IDs before publication. A required reachable edge is validated by
  its owning read or GC traversal; never preclassify every absent probe as
  corruption inside the adapter.
- Exact journal-record manifest, prefix-replacement authority, and coupled
  checkpoint receipt lookups require the current head-pinned StateRoot and
  traverse only its typed sparse-map path. Every current-head StateRoot read
  loads the exact physical manifest and requires complete equality with a caller
  snapshot. Ordinary head, manifest, resolver, exact-key, and semantic commit
  paths never follow or decode the optional head-pinned GC receipt; that receipt
  is physical reclamation authority only. Never materialize cumulative history
  or accept stale, missing, or corrupt StateRoot authority for these lookups.
- One complete StoreHead is the sole mutable CAS location. CAS compares the
  complete expected head, including its exact physical_token and gc_sequence;
  semantic revision equality never substitutes for physical equality.
  physical_token is the provider-neutral opaque monotonic CAS lineage/fence:
  batch admission verifies it against the expected head and introduced
  persistent objects, while reopen verifies its format and the manifest
  closure rather than trying to reconstruct prior batch inputs.
- Create a missing directory hierarchy one level at a time and fsync each
  created or re-observed entry's parent. Atomically create and durably publish
  the versioned bootstrap-marker directory before creating head.lock; only
  that marker authorizes crash recovery of a partial initialization. A first
  head may not publish until the Store root's parent-directory entry has been
  durably synchronized.
- Freeze the configured root to an absolute lexical path at open without
  canonicalizing the final entry. Later process working-directory changes
  must never retarget an existing Store handle; opening `.` still synchronizes
  the absolute Store entry in its parent directory.
- The generation initializer and every writer operation release head.lock
  explicitly before closing its file and returning. Never wait on this
  adapter-local writer lock; contention is an immediate conflict.
- Immutable-object validation, staging, file fsync, and family-directory fsync
  run under the separate non-blocking objects.lock. A writer retains that lock
  while it acquires head.lock, so receipt replay cannot delete a reintroduced
  object between staging and head publication. head.lock itself covers only
  staging cleanup, exact head recheck, final family sync, head replacement, and
  bounded GC sweep; ordinary writable load remains available during object
  staging. Writable `load_head` cleans object staging when objects.lock is idle;
  when it is actively owned, it returns the already authenticated bounded head.
  Ordinary `load_head` authenticates only the bounded head bytes and validates
  the optional receipt content-ID shape already carried in that head. It reads
  zero GC-receipt bytes, never traverses StateRoot or Machine archive objects,
  replays a receipt, or mutates cold inventory. Complete projection traversal is
  only the explicitly named `load_full_audit` path.
  Unique immutable staging files live only in object-staging; cleanup is
  proportional to crash residue and never scans committed object families.
- `stats()` counts no-follow, canonical physical locators behind a stable head
  read but never opens or decodes their payloads; in particular, it is not a GC
  receipt validation path.
- Never acknowledge before every immutable object and the new head are durable.
  A final head rename followed by failed directory sync is an unknown commit
  outcome. An identical retained immutable object is synchronized again before
  the head may publish.
- A semantic CAS supersedes, but never consumes, the optional physical GC
  receipt pinned by its expected head: it reads zero receipt bytes, preserves
  `gc_sequence`, and publishes a semantic successor with no receipt pointer.
  The old receipt remains an ordinary physical orphan for a later explicit GC
  generation; semantic commit never replays or deletes it.
- Machine archive segments, independently addressable entries, complete batch
  records, and sparse command-index nodes share the command-archives
  content-ID namespace. A separate immutable `batch_id` index resolves the
  batch receipt object without scanning segments; equal stable IDs must map to
  byte-identical batch authority. GC reaches batches from each verified reachable
  segment's complete batch list, including material-only batches without any
  command Entry, and exact-compares the independent indexed record with the
  segment record. It retains both the index and receipt object.
  Every reachable command Entry additionally passes the Core-owned batch/entry
  verifier for complete member intent, receipt, and Event correspondence;
  matching the stable batch ID alone is not closure evidence.
  These are independent immutable objects, not materialized M1 state and not
  another head.
- open_read_only requires an existing root and performs no directory creation,
  lock creation, staging cleanup, or GC completion. It validates a stable head
  before returning a projection.
- A whole-state state.json, checkpoints, or segments family is an unsupported
  source generation. Reject it with unsupported_store_generation before
  creating current directories. Do not add an importer, fallback, or repair.
- Unsupported physical generations surface as `DurableError::Substrate` with
  the exact `unsupported_store_generation` code and a separate message. Never
  encode or recover that code through a Validation message prefix.
  Reclassify only observed marker/layout shape, canonical-byte, and generation
  mismatches. Filesystem I/O, persistence, and unknown-outcome failures retain
  their original variant, code, and message; never flatten a `DurableError`
  through Display while inspecting generation metadata.
- Every persisted read is bounded, no-follow, regular-file-only, exact
  canonical JSON. A locator must match the decoded closed kind and content
  identity; aliases, symlinks, FIFOs, devices, wrong variants, and malformed
  bytes are integrity failures. Page-slot subtraction and the one-byte
  over-limit read probe use checked exact arithmetic; an unrepresentable bound
  fails before file I/O and is never saturated into a different capacity.
- The configured root is an owner-exclusive trusted subtree for the lifetime
  of an open adapter. No-follow validates static final entries; this local
  adapter does not claim hostile same-UID subtree isolation against an actor
  concurrently renaming families or replacing head.lock. Use a substrate with
  native tenant isolation when that is part of the threat model.
- Application callers invoke cold reclamation only through the no-argument
  `DurableStoreControl::reconcile_cold_reclamation()` and
  `DurableStoreControl::advance_cold_reclamation()` entrypoints. The adapter
  receives only the coordinator-issued opaque `StoreReclamation` capability
  and derives the exact expected head from `expected_head()`; never accept a
  caller-assembled `StoreHead` as a physical reclamation command.
- Cold-reclamation reconciliation requires the exact current head and its
  pinned receipt. It loads only that receipt's bounded authorized page,
  revalidates it against current reachability, and idempotently completes the
  deletions without publishing another head. A non-final lost acknowledgement
  never advances implicitly; receipt.remaining_objects only tells the caller
  that a later explicit advance is available.
- Cold-reclamation advance resolves and validates the complete StateRoot and
  Machine archive closure outside both adapter locks, then rechecks head
  stability. Under objects.lock it rechecks the exact head, reconciles the prior
  page, and streams all physical families. Never materialize the complete cold
  inventory in a Vec, map, or set. Count candidates exactly while retaining a
  bounded page. When the expected head pins a predecessor receipt, select it
  mandatorily; fill the next priority tier with the lexicographically smallest
  other receipt-family candidates, then use any remaining slots for the
  lexicographically smallest ordinary candidates across all other families.
  Receipt crash residue beyond one page remains in the exact remaining count
  and converges through explicit later generations. The directory adapter
  enforces at most 1,024 authorized identities on both creation and replay.
  Under head.lock it rechecks the exact complete head, durably publishes the
  new receipt and advanced physical head, and only then sweeps that frozen
  page. A final successful generation with remaining_objects=0 deletes every
  older receipt file and retains only the new current receipt; later explicit
  generations also collect post-receipt orphans.
- Once a newly generated GC receipt/head may be durable, a failure in its
  post-publication sweep or directory sync is CommitOutcomeUnknown and requires
  reopen. Reconciliation and the prior-page sweep at the start of advance do
  not publish a new head: failure leaves the old expected head authoritative,
  and any receipt-authorized partial deletion is idempotently replayable.
- Explicit full audit reads only identities reachable from head.json. Unrelated
  immutable files are not authority until GC validates their canonical locator
  and either retains or reclaims them. Writable `load_head` removes staging
  residue but never
  completes GC deletion; lost acknowledgement recovery is an explicit
  no-argument coordinator `reconcile_cold_reclamation()` call after reopen.
- Tests use real process death at internal object/head/receipt/sweep durability
  boundaries, reopen and audit complete closure, preserve reachable archives,
  delete only valid orphans, and reject legacy generations and unsafe
  filesystem object kinds. The exact `66a432c` whole-state fixture is a tracked
  negative: both writable and read-only open reject its writer bytes without
  creating, repairing, or changing any current-generation entry.
- Runtime fixtures initialize only a zero-Run catalog Machine. Create the first
  Run through `DurableRuntimeControl::submit(DurableCommand::StartRun)` with its
  current Clock receipt so Run, Continuation, Attempt, claim, and fence share
  the atomic creation CAS. Query only through the closed Query/4 commands.
- History-maintenance conformance uses that real Runtime admission and the
  public `DurableStoreControl::compact_machine_history` request. Preserve two
  generations, physical reopen, historical receipt and cancellation replay,
  post-CAS acknowledgement loss, and stale-request rejection. Event-free
  material batches come from public wait activation, never a caller-assembled
  Machine snapshot or synthetic head/base-anchor pair. Missing independent
  archive Entry, Batch, index node, or stable batch index must make explicit
  full audit and GC fail with integrity while preserving the head and physical
  inventory. Fresh compaction does not scan unrelated historical cold objects.
