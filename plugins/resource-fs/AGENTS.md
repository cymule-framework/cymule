# Filesystem Resource Guidance

- Store bytes by verified content identity, never by caller path. Ordinary
  objects use their raw SHA-256; manifests use the root-derived
  `cymule.resource-manifest/3` descriptor ID. A resolver reference is a
  non-secret digest interpreted only under the configured immutable binding.
  Rebuild replaceable locator sets from the semantic Handle's content digest
  and this configured binding; require the sole opaque locator to equal that
  digest before read, list, stat, or deletion, and do not copy locators back into
  the Handle.
- Upload IDs derive from caller write IDs. Chunk retry at an already persisted
  offset succeeds only when the retained bytes match exactly; gaps and changed
  bytes fail closed. A whole-file import must also replay after publication by
  comparing every submitted chunk with the committed content; this closes the
  Resource-published/consumer-checkpoint-not-yet-acknowledged recovery window.
- Open imports carry their complete bounded-source byte length or semantic
  directory manifest into the same upload claim that admits `Publishing`.
  Verify the existing physical bytes and metadata, then compare the complete
  input with the derived publication before any terminal upload-record write.
  An equal retried range is not complete-source equality: a short source must
  leave the parent upload Open, unpublished, and without a cleanup receipt.
  Re-read the frontier under that final claim; do not use an earlier length
  preflight or re-read the whole source to recreate a completed input.
- Treat every failure while verifying a decoded persisted upload record as
  `filesystem_upload_record_invalid` integrity corruption. Caller intent
  validation remains a pre-record `Validation`; a later source mismatch must
  never reclassify malformed persisted intent, publication, manifest, cleanup,
  or identity metadata as caller input.
- The ordinary `begin_write` / `write_chunk` / `commit_write` sequence also
  replays across publication response loss. `Publishing` chunk retries read
  only the acknowledged upload range without creating, truncating, or extending
  data; `Committed` retries compare only the published object range. Neither
  retry writes a head or grants new publication authority. Equal retries never
  hash the full object once per chunk; the original commit still verifies the
  complete object or manifest before returning its original publication.
- The configured binding uses the shared Resource identity grammar: 1..=512
  Unicode scalars and no controls, validated before creating or repairing the
  root. Recursive import derives every child write ID from the typed parent ID
  and exact child name under `cymule.resource-fs-child-write/1`; never rebuild a
  child authority with delimiter-based string concatenation.
- Authenticate sessions by recomputing the exact upload ID from `write_id`, the
  adapter generation, and the complete configured binding before deriving
  paths. `cymule.resource-fs-upload/9` also retains and verifies that binding;
  upload keys are lowercase digest bytes only. Stores sharing one root under
  different bindings never continue or abort each other's uploads, and their
  object/catalog/manifest-index families live in binding-derived physical
  subdirectories so deletion under one binding cannot remove another's bytes.
- Deletion accepts only the retained `ResourceDeletionTarget`: semantic
  Resource audit identity plus the exact physical family, byte size, and
  optional manifest descriptor. It never accepts a locator, path, caller
  absence boolean, or removed-byte claim. Removal is idempotent and returns
  success only after synced exact-family absence readback; lifecycle admission
  and the terminal receipt remain M1-owned.
  The physical family's content digest, not the initiating target's optional
  manifest descriptor, owns removal of the payload, manifest-index directory,
  and manifest-index catalog record. A semantic object-shaped target sharing a
  family with a manifest publication must leave no permanent manifest metadata.
  Payload readback verifies the closed physical family encoding: either the raw
  content SHA-256 or a streamed canonical manifest whose descriptor ID equals
  that same family digest; target shape is not physical deletion authority.
  An already absent object still requires syncing its owning directory before
  acknowledging deletion, because an earlier unlink may have lost its sync.
  Publishing and deletion take the same non-blocking cross-process claim keyed
  only by the verified physical retention family. Deletion must durably write
  and read back that claim file's permanent tombstone before removing payload
  or manifest-index bytes. A tombstoned family is resolver-absent and can never
  publish again, including through another `write_id`; an interrupted
  Publishing upload closes as `Deleted` and cleans only its upload-owned
  staging targets. Never remove or time-expire the tombstone.
- Write and fsync a unique staging object before linking it into the content
  namespace. Never expose partial bytes at a committed Resource location.
- Whether the content link is newly created or already exists, verify its exact
  digest and size, fsync the objects directory, remove this upload's staging
  object, fsync staging, and only then persist the committed publication. A
  committed import replay compares source and retained content in one sequential
  pass; never hash the whole object once per retried chunk.
- `cymule.resource-fs-upload/9` records the only acknowledged chunk frontier,
  immutable cleanup plan, and terminal plan-derived cleanup receipt.
  Sync bytes and a newly created upload directory entry before atomically
  advancing that frontier. On reopen, truncate only bytes beyond it; bytes below
  it are immutable and missing acknowledged bytes are corruption.
- Every ordinary content-addressed stat verifies the complete raw SHA-256
  digest, while a manifest stat stream-parses canonical lines and reconstructs
  its sole descriptor ID and Merkle root. Exact manifest listing uses the
  publication-owned `cymule.resource-fs-manifest-index/3`: an immutable
  `ResourceCatalogRecord` header, fixed random-access offsets, and fixed Merkle
  nodes. One entry and one page have separate canonical-byte hard limits. A page
  selects offsets within the byte budget before allocating and then reads exact
  indexed ranges; it never scans or collects the complete manifest/index. Object reads
  go through `ResourceClient`: stat
  verifies before streaming and the client independently hashes the exact
  streamed bytes once. The adapter independently enforces the shared read bound.
  Sync both sides of cross-directory record publication and staging removal
  before acknowledging completion; an equal retry re-syncs retained files and
  both owning directories before it acknowledges durability.
- Directory and snapshot resources are sorted JSON-lines manifests of child
  Resource Handles. Import enumerates names into fixed-entry sorted runs beneath
  one upload-owned staging tree, merges with fixed fan-in, and streams the final
  run into bounded write chunks; source cardinality never materializes a full
  name or manifest-entry vector. The deterministic staging-tree target is part
  of the upload cleanup plan, so interruption/reopen rebuilds it and terminal
  cleanup streams its exact children. Reject symlinks, unsafe names, duplicates,
  and malformed manifests; list with the core self-contained cursor instead of
  materializing a complete tree. Every non-initial page reads exactly one additional indexed
  predecessor entry plus its Merkle path, so the framework can prove the
  cross-page strict-name boundary after restart without trusting the cursor as
  provenance.
- Recursive directory import treats the caller's root as depth zero and accepts
  at most 64 nested child-directory edges. Before descending into level 65,
  fresh import and published replay both return caller `Validation` with the
  stable message prefix `filesystem_import_depth_exceeded:`; depth never enters
  child write IDs, manifests, or Resource identity. Keep the guard before the
  recursive call so rejection never relies on host stack exhaustion.
- Directory sort preparation re-reads the actual upload phase under its short
  non-blocking claim. A completed abort between `begin_write` and preparation
  must fail before any staging mutation; a retained cleanup receipt never
  permits recreating the sort tree. Do not hold that claim across recursion.
- Cross-process writer claims are non-blocking. Contention returns a Resource
  conflict; retry policy belongs to the caller.
- `cymule.resource-fs-layout/2` is the physical namespace marker. An unmarked
  non-empty root or another marker generation is unsupported and must not gain
  a new upload authority for the same `write_id`. Open the root and every fixed
  directory once with no-follow descriptors; all owned entries and recursive
  imports resolve beneath those descriptors. Never restore path-based
  check-then-open traversal. Validate root and binding namespace inventories as
  streams; a malformed high-cardinality root must not first materialize every
  entry name.
  Generation `/1` has no compatibility reader or in-place migration. Internal
  test roots must follow the registered drain/reset/reseed runbook before `/2`
  admission; never reinterpret or copy `/1` physical entries into `/2`.
- Ordinary-file opens are non-blocking and no-follow, and verify the opened
  descriptor before reading, truncating or writing. A FIFO or device in an
  owned namespace must fail as an invalid entry, not wait for a peer.
- First initialization writes and syncs `layout.json.initializing`, atomically
  renames it to `layout.json`, and syncs the root directory. A writable open may
  remove that sole interrupted staging marker, or replace a zero-length final
  marker only while the namespace has no data directories; every other partial
  or mixed generation fails closed.
- `FsResourceStore::open_read_only` requires the complete current layout and
  performs no create, cleanup, truncation, or marker repair. Every mutation
  boundary rejects a read-only store while resolver reads remain available.
- Commit persists `Publishing` plus the complete final publication before any
  content-family entry. Commit, abort, and reopen converge that retained intent
  before upload cleanup, so a failed publication cannot leave unowned bytes.
  Import replay with an existing Publishing/Committed intent enters that same
  convergence path before comparing the source; a retained publication field
  is not itself a terminal checkpoint. A later input conflict never revokes that
  already admitted publication or changes its corruption classification.
- Before commit replay or abort removes anything, persist the exact sorted
  cleanup target plan inside the upload record. Delete only those targets,
  verify their absence, persist the unique plan-derived
  `cymule.resource-cleanup-receipt/2`, and return that same receipt forever.
- Catalog put computes the exact canonical record bytes and enforces the shared
  protocol-owned 16 MiB bound as catalog get before any staging or destination
  mutation.
- The `/9` upload record has the same exact 16 MiB physical bound. Keep it
  fixed-cardinality: scalar acknowledged frontier, compact publication, fixed
  cleanup plan/receipt, and no chunk or manifest-entry vector. `begin_write`
  must preflight the largest legal terminal record before acquiring the writer
  claim or creating a lock/upload entry; reads must bound bytes before decode.
