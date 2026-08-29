# SQLite Durable Store Guidance

- This plugin owns the exact cymule.sqlite-store/6 physical generation. Its
  only mutable semantic pointer is the complete canonical StoreHead; immutable
  StateRoot objects and Machine command-archive objects are separate typed,
  content-addressed namespaces. Checkpoints, state segments, suffixes, and
  whole-state rows are not current authority.
- Keep busy_timeout at zero. SQLite writer contention returns a Cymule
  conflict immediately; application retry policy remains outside this adapter.
- Admit the Store domain before opening SQLite. It uses the Engine contract of
  1..=512 non-control Unicode scalar values; never substitute UTF-8 byte length
  for the schema's string-length semantics.
- Observation-only callers use open_read_only. It performs no schema,
  journal-mode, or synchronous-mode writes, never creates a database, and
  rejects CAS and GC.
- An empty database creates the complete current schema in one immediate
  transaction. Every existing database must have exactly the five current
  non-system schema objects and their exact DDL. Reject every older generation,
  partial schema, foreign table, explicit index, view, and trigger before
  mutation; never repair, migrate, or fall back in-process.
- Unsupported physical generations surface as `DurableError::Substrate` with
  the exact `unsupported_store_generation` code and a separate message. Never
  encode or recover that code through a Validation message prefix.
  Only a successfully read schema/DDL/generation mismatch uses that code.
  SQLite open, configuration, and schema-query failures retain their exact
  substrate code and are never caught and reclassified as a generation.
- Every commit uses one immediate transaction to read the exact current
  canonical head, compare the complete expected head including physical_token
  and gc_sequence, write immutable StateRoot and command archive objects, and
  conditionally publish the complete next head. Immutable conflicts compare
  canonical raw BLOBs after a scoped primary-key ON CONFLICT; typed equality is
  not physical equality.
- Archived atomic batches are physically addressed by `batch_receipt_id` and
  carry one unique nullable `batch_id` column as their stable secondary index.
  Reads resolve that indexed column directly, verify both identities and exact
  canonical bytes, and never scan archive segments. Explicit GC derives reachable
  batch receipt IDs from each verified reachable segment's complete batch list
  through the same index and exact-compares both records. Material-only batches
  with no command Entry remain reachable; ordinary exact batch lookup still
  reads no segment.
  Every reachable entry must also pass Core's complete batch/entry verifier
  before its batch receipt is retained; stable-index equality alone does not
  authenticate the member intent, receipt, or Events.
- Once SQLite commit begins, a failed or missing response is
  CommitOutcomeUnknown. Reopen and reconcile the exact physical head; never
  retry a potentially committed head-transition request from that response.
  Reconciliation is the closed exception: it never changes the head, so any
  failed or lost response remains idempotently retryable against the same
  exact head and must not be reported as CommitOutcomeUnknown.
- A semantic CAS supersedes, but never consumes, the optional physical GC
  receipt pinned by its expected head: it reads zero receipt bytes, preserves
  `gc_sequence`, and publishes a semantic successor with no receipt pointer in
  the same transaction. The old receipt remains ordinary physical orphan
  inventory for a later explicit GC generation.
- Ordinary `load_head` authenticates only the bounded head in one deferred
  snapshot, including the optional receipt content-ID shape carried by the head.
  It performs no GC-receipt table probe and reads zero receipt, StateRoot, or
  Machine archive bytes. Manifest reads, Durable-owned lowering, journal
  callbacks, exact-key queries, and semantic commits likewise never follow the
  physical receipt. Complete projection traversal exists only as the explicitly
  named `load_full_audit` path; explicit GC alone loads receipt authority. The
  adapter supplies only `with_state_root_resolver`; it never constructs a
  semantic transition or exposes a provider-specific preview API. The manifest
  must be the exact physical object named by that head. Missing objects, wrong
  variants, non-canonical bytes, locator aliases, and incomplete reachable
  closure are durable integrity failures.
- `stats()` reads archive inventory, StateRoot count, and GC-receipt count from
  one deferred SQLite snapshot. Never assemble `StoreStats` from independently
  timed autocommit statements that can cross a concurrent commit generation.
- Every persisted BLOB is length-gated in SQL before Rust fetch or decode, and
  every insert is gated before SQLite binding, using the owning Core or Durable
  protocol constant. Never add provider-local byte limits or fetch an oversized
  BLOB merely to reject it afterward.
- `StateRootResolver` returns `None` for a physically absent content identity.
  Immutable collection builders must be able to probe newly derived node IDs
  before they exist; only a required reachable edge turns absence into an
  integrity error at the owning read/GC boundary. Do not weaken canonical-byte,
  locator, kind, or length checks for objects that are present.
- Application callers invoke cold reclamation only through the no-argument
  `DurableStoreControl::reconcile_cold_reclamation()` and
  `DurableStoreControl::advance_cold_reclamation()` entrypoints. The adapter
  receives only the coordinator-issued opaque `StoreReclamation` capability
  and derives the exact expected head from `expected_head()`; never accept a
  caller-assembled `StoreHead` as a physical reclamation command.
- Cold reclamation has two closed operations. Reconciliation requires the
  complete current head and its pinned receipt, idempotently completes only
  that receipt's authorized deletions, and never publishes a new head even for
  a non-terminal page. Before deletion it rebuilds the current StateRoot plus
  Machine archive/index closure in the same transaction and requires the
  authorized set to be disjoint from that authority and the current receipt.
  Advancement first performs that same idempotent replay for the pinned page,
  then inventories and creates one fresh bounded page, deletes it, and moves the
  exact head in the same immediate transaction.
- GC inventory and deletion cover the StateRoot, Machine archive, and GC
  receipt tables as one disjoint physical identity space. Reject an identity
  present in more than one family. Keep only bounded in-memory prefixes while
  streaming the exact inventory count. Select the head-pinned receipt first,
  then other receipt-family rows in identity order, then fill remaining slots
  from the lexicographically ordered StateRoot and Machine candidates. This keeps
  pre-head-crash receipt accumulation bounded and drainable without displacing
  the current lifecycle receipt. Report every unselected candidate exactly; a
  terminal generation with remaining_objects zero leaves exactly its new
  current receipt in the receipt table.
- Real process-death tests cover immutable-object, head, SQLite commit, GC
  receipt, GC sweep, and GC-head boundaries. After every kill, run complete
  PRAGMA integrity_check, checkpoint WAL, repeat integrity check, reopen through
  SqliteStore, and compare the exact canonical readback.
- M4 process-death conformance uses only the public provider-registry-bound
  Evolution control over the exact SQLite `/6` store. A successful
  provider-free catalog mutation plus provider-backed migration derives the
  real CAS boundary count; each pre/post-CAS child kill reopens through exact
  command alias and typed receipt. Persist provider binding lookup, adapter
  lookup, Describe, and migrate counts in a separate WAL ledger and require a
  post-CAS lost acknowledgement to replay without repeating any provider call.
- Paged terminal process-death coverage starts from a real in-flight provider
  Attempt with multiple pending Effects. Discover the exact Begin, every
  Progress, and Finalize CAS from one successful SQLite `/6` cancellation, then
  SIGKILL before and after each discovered boundary. Reopen through public
  control, converge and exactly replay the cancellation receipt, close the
  Attempt, Continuation, and outbox, and prove the provider was not reinvoked.
- A committed physical head may contain only parameter-free genesis or admitted
  material, with no Run yet. Process-death recovery uses the public typed
  Run-current query: an absent Run admits Start, Running requires explicit
  expired takeover, and other existing boundaries use Resume. Head presence
  alone never proves that StartRun committed.
- Process-death tests keep Clock authority in a separate SQLite database. A
  store database is dedicated to the exact /6 schema and may not colocate
  unrelated tables.
- History-maintenance conformance starts through real public Runtime admission
  and `DurableStoreControl::compact_machine_history`. Preserve two generations,
  physical reopen, historical receipt and cancellation replay, post-commit
  response loss, and stale-request rejection. Event-free material batches come
  from public wait activation, never a caller-assembled Machine snapshot or
  synthetic head/base-anchor pair. Missing independent archive Entry, Batch,
  or command-index nodes must make explicit full audit and GC fail with
  integrity while preserving the head and physical inventory. Fresh compaction
  does not scan unrelated historical cold objects.
