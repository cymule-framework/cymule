# Object Store Resource Guidance

- `ObjectStore` byte operations and GC inventory authority are different
  contracts. Keep `ObjectStoreInventory` private and sealed. Production
  constructors expose only reviewed provider wrappers whose official service guarantees
  strong visibility, complete lexicographic order, and exclusive continuation.
  Never accept a caller marker trait, boolean assertion, or erased
  `Arc<dyn ObjectStore>` as that authority. In-memory inventory and arbitrary
  inventory injection exist only inside non-published `cfg(test)` conformance.
- The workspace `object_store` dependency is exactly `=0.14.1`. Its public list
  methods still promise no order. The pin binds the reviewed Azure/GCS concrete
  adapter behavior; the three-marker startup canary detects obvious endpoint,
  visibility, ordering, and continuation misconfiguration but is not a
  capability proof. Conditional create/update belongs to each admitted concrete
  wrapper and is exercised by real head transitions, never by rewriting a live
  GC head during ordinary open.
- Admit only `AzureBlobInventory` on the official Azure Blob service and
  `GoogleCloudInventory` on the official GCS service. Reject Azure custom
  endpoints, Azurite, Fabric, GCS base-URL overrides, and credential sources
  whose endpoint selection cannot be inspected before construction. Provider
  wrappers construct the pinned upstream backend internally from closed
  account/container/credential inputs; never accept a caller-configured
  upstream builder or HTTP connector. Validate Azure's official lowercase
  account/container grammar before passing names to the upstream builder, so a
  delimiter cannot change its interpolated URL. Do not add generic S3: one Apache type
  covers general-purpose and directory buckets, but AWS does not promise
  lexicographic order for directory buckets. Local durable files belong to
  `cymule-resource-fs`; production `fs` and `http` features are not supported
  here.
- `cymule.resource-object-store-layout/2` is the only physical generation. A
  non-empty unmarked prefix or any other marker fails before a current upload
  authority can be created. Do not restore a layout-1 reader or migrator.
- The configured binding uses the shared Resource identity grammar: 1..=512
  Unicode scalars and no controls. Validate it before runtime construction,
  layout/canary publication, or GC initialization; provider-specific prefix
  validation is a separate physical-configuration contract.
- `cymule.resource-object-store-upload/7` is one fixed-cardinality conditional
  head. It binds write intent, binding, non-reusable generation, content epoch,
  scalar byte/chunk frontiers, one authenticated radix root, optional bounded
  migration progress, compact publication evidence, and an empty cleanup
  plan/receipt. Never retain a per-chunk vector or session staging prefix in the
  head.
- Upload heads live only in
  `uploads/<binding-digest>/<upload-key>/record.json`. Exact reads, writes,
  GC inventory, and relative cursors derive from the same binding-prefix
  function. Never scan all bindings and filter foreign heads: another binding's
  Open, Publishing, Committed, or Aborted history is outside this GC authority.
- Enforce the 16 MiB canonical upload-record budget by preflighting the largest
  legal terminal and migration shapes before provider mutation. Enforce the
  protocol-owned 16 MiB canonical catalog-record bound on writes. On read,
  inspect provider metadata before body
  materialization and exact-match the realized byte length. Radix nodes remain
  content addressed and at most 4 KiB.
- Publish chunk bytes and frontier nodes only in the global immutable
  `upload-content/<binding>/epochs/<epoch>` namespace. Make them visible solely
  by exact upload-head CAS. Every write checks the retained head and current
  epoch before publishing; every begin that races an epoch advance converges
  its still-empty head before returning a session. A stale writer may create an
  invisible immutable orphan, but it must never revive or advance a terminal or
  migrated head.
- Abort and commit terminalize the upload head and persist the unique empty
  cleanup plan/receipt. Never claim prefix absence: generic object stores cannot
  atomically exclude a late writer from a mutable staging tree. Orphan lifetime
  belongs to explicit epoch reclamation.
- `reconcile_upload_content` is the sole orphan-GC controller. A cycle advances
  the non-reusable epoch, fences/migrates every Open/Publishing head in bounded
  64-chunk pages, then sweeps old immutable content in bounded 512-object pages.
  Validate and CAS-persist each fixed-size relative-path page plan before its
  first deletion; execute and retire only that exact plan. Persist phase and
  exact relative cursor with CAS before acknowledging. Treat conflicts,
  response loss, a missing target during admitted-plan replay, and process death
  as replay of the same authority, never as a new generation or fallback scan.
- Operators must call reclamation repeatedly until `complete`, and must schedule
  future cycles. Completion means the persisted cursor reached inventory end
  and that traversal admitted no old-epoch target; it is not global absence under
  a concurrent stale writer. Content published after its lexicographic position
  was passed, even before completion is returned, is next-cycle work. Tests must prove
  restart, lost GC-head acknowledgement, concurrent drivers, real process
  death, eventual orphan reclaim, and no deletion of a shared active chunk.
- Reclamation progress exposes `confirmed_absent_objects`: the exact count of
  old-epoch targets in its already persisted page that passed absence readback.
  Same-page replay returns the same count; it is not a unique-delete metric and
  cannot be summed across retries or concurrent drivers. The retained page and
  GC head remain its only authority, independent of whether a provider returns
  success or NotFound when deleting an already absent object.
- Validate each inventory page and page boundary as strictly increasing and
  cursor-exclusive. Require listed objects to exist before deletion and to be
  absent immediately afterward. Adversarial tests cover unordered, duplicate,
  missing, inclusive-offset, and stale-after-delete inventory. Do not sort,
  deduplicate, skip, or otherwise repair provider inventory in production.
- If inventory readback finds a missing object or an object from a future
  epoch, re-read the exact source GC head and full conditional version before
  classifying that contradiction.
  A different head or version makes that stale read a Conflict with no writes;
  the unchanged head still makes it an Integrity failure. Compare the version
  even when completed page retirement recreates identical head bytes. This
  check never revokes an already admitted old-epoch deletion page's replay and
  must not catch or reclassify unrelated provider or validation errors.
- Commit records `Publishing` before mutation, creates immutable 8 MiB parts,
  and conditionally creates the small Published index last. That index key is
  the sole publication visibility authority: deletion CAS-transitions it to an
  exact permanent Deleted tombstone, or conditionally creates the tombstone
  when the index is absent. No path deletes that key or changes Deleted back to
  Published. A late part PUT can leave invisible payload but its index Create
  cannot resurrect the Resource. A head's bytes and complete conditional version
  come from the same bounded GET. Equal retries verify every retained byte. The
  sole locator is the content digest. Stat verifies complete bytes, bounded
  reads verify their exact indexed part ranges, and terminal EOF requires the
  complete physical family; Deleted maps to NotFound on each of these paths.
- Ordinary begin/chunk/commit replay remains closed before a consumer's
  publication catalog is acknowledged. `Publishing` chunk retries use only
  the retained epoch/root and original acknowledged offset/size/digest/bytes,
  even while GC is migrating that root. `Committed` retries use the published
  content index and exact part ranges, never the retired upload epoch. These
  paths are read-only and precede the Open-only epoch/migration write fence;
  neither can append, revive Aborted, mutate a head, or use a cleanup receipt
  to recreate a deleted publication. Equal replay never hashes the entire
  object once per chunk; the original commit still verifies complete bytes.
- The retained M1 `ResourceDeletionTarget` is the sole deletion authority.
  Derive its exact index and fixed-size part family without a second deletion
  journal or progress head. Before the first deletion, stream the entire
  present inventory and reject unordered, unknown, or wrong-sized members;
  verify any present index against the target, and retain full digest
  verification while all parts remain present. Missing authorized parts or
  index are legal partial completion, not corruption that blocks convergence.
  The first external mutation is the same-key permanent index fence, only after
  this complete preflight. Revalidate the streamed present parts while deleting
  them and prove both the exact Deleted head and the current payload absence.
  The tombstone is non-payload control metadata, not an absence violation or a
  second lifecycle receipt. Do not claim every provider key or every possible
  future orphan is physically absent. Never enumerate the target's declared
  part count to find missing objects or touch another binding.
- Upload-content GC finishes with a separate bounded deleted-family traversal.
  One page examines at most 512 inventory objects, including live/retained
  entries, and persists its final examined cursor plus exact part path,
  tombstone record ID, and size before deleting anything. Empty-target pages
  must still advance across live objects. Validate the entire admitted page
  before its first deletion; replay requires the identical immutable tombstone
  and accepts an absent authorized part as concurrent deletion completion.
  Preserve every index fence and every part without an exact Deleted head.
  Future cycles reclaim late parts even after upload-content epoch reclamation
  and lifecycle Completed receipts; completed lifecycle replay itself does no
  provider cleanup.
- A still-Publishing upload whose exact physical content is permanently Deleted
  may only CAS-terminalize to Aborted with publication/migration cleared and
  its existing empty plan/receipt. Preserve its acknowledged frontier; do not
  migrate its unreachable content forever. Explicit abort, commit rejection,
  and GC use this one helper. Absence of an index never authorizes abort, and a
  stale CAS must never change the winning Committed upload. Same binding/digest
  deletion is already a permanent Resource lifecycle terminal state.
- Feature-free production builds expose no constructible provider. Constructor,
  layout, and inventory-canary code belongs only to the Azure/GCS features or
  private conformance tests; do not silence dead-code or unused-macro diagnostics.
- Fault injection flags and exact pause points belong to each private test
  adapter instance. A parallel fixture must not consume another adapter's
  publication, cleanup, or GC fault. Real two-driver tests still share the
  backend and exercise concurrent CAS without a production lock.
- Credentials, endpoint secrets, provider names, and topology remain in the
  configured provider. They never enter Resource semantics, durable records,
  errors, or logs. Call the synchronous adapter from a worker or
  `spawn_blocking`, not a current-thread Tokio runtime.
