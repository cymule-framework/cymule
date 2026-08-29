# Cross-Run Resource Guidance

- `cymule-profile-protocol::resource` owns provider-neutral Resource DTOs and
  the sole pure lifecycle reducers; this crate re-exports them and owns the
  bounded resolver/store and typed M1 controller boundaries. Neither layer owns
  storage credentials or provider configuration.
- `cymule.resource/3` Resource identity excludes realization locations.
  `cymule.resource-locators/2` is the sole separate replaceable resolver
  record; signed URLs,
  access grants, sessions, and credential revisions never enter the semantic
  descriptor or locator set. Content digest, immutable
  version authority, logical shape, media type, and semantic annotations define
  identity; moving bytes must not create a different resource.
- One Resource descriptor contains at most 64 semantic annotations and at most
  4 MiB of canonical JSON. One locator set contains at most 16 locations and
  256 KiB of canonical JSON. Public URLs are exact canonical ASCII HTTP(S)
  wires within both 8,192 Unicode scalars and 8,192 UTF-8 bytes; equivalent but
  noncanonical or Unicode spellings are rejected rather than normalized.
  Percent escapes are uppercase and may not encode an unreserved byte.
- Chunked-write intents admit the same lowercase ASCII media-type grammar as
  the terminal Resource sealer. A store session must never be opened for an
  intent that cannot become a valid immutable Resource descriptor.
- Credentials, signed URLs, session tokens, and provider secrets never enter a
  Resource, Artifact, Event, Continuation, handoff, log, or fixture. Public URLs
  are credential-free; private resources use opaque resolver references.
- `live` resources are useful references but never exact replay evidence.
  Version-pinned resources require their original resolver binding. Only inline
  or verified content-addressed resources are location-independent exact data.
- Read and list APIs are bounded and cursor-based. Never require a directory,
  collection, snapshot, or large object to materialize in memory.
- Canonical manifest entries are at most 1 MiB including their newline, and one
  returned page is at most 8 MiB and 1,000 entries. Every inclusion, including
  the predecessor, has at most 53 sibling steps, matching the shared
  exact-integer tree bound. Constructors and public verification enforce both
  count bounds before deriving or examining paths. Sealers reject an oversized
  entry; adapters select an indexed range within both byte bounds before
  allocating; framework proof verification independently enforces the page bound.
- Every concrete `ArtifactResolver::read` enforces `MAX_READ_CHUNK` itself;
  facade validation is not adapter authority. `offset == size` returns an empty
  terminal chunk only after the adapter has proved the exact object still
  exists with the retained size.
- Exact list pages do not pre-scan the complete manifest through `stat`.
  Publication validates the complete canonical bytes and persists an immutable
  content-addressed Merkle index; each list call reads only its indexed byte
  range and sibling paths, and `ResourceClient` verifies the returned proof and
  cursor frontier.
- `ResourceManifestAccumulator` is the sole publication authority for canonical
  JSON-lines bytes, byte size, entry count, and Merkle root. The
  `cymule.resource-manifest/3` descriptor ID is recomputed only from that root,
  size, count, and canonical media type; no raw-byte SHA is a second manifest
  authority. An adapter writes the exact line returned by `push`, and complete
  copy verification reconstructs the same descriptor from streamed bytes.
- Exact-list cursors are canonical self-contained
  `cymule.resource-list-cursor/3` tokens. They bind Resource, manifest, resolver
  binding, request limit, predecessor cursor, exact page progress, and the
  preceding page's final name and remain valid across clients and restarts. A
  public content-hash cursor is continuity evidence, not provenance: every
  non-initial page independently carries the exact `start_index - 1` entry and
  its Merkle inclusion. Never retain cursor frontiers in an unbounded
  process-local map or return a cursor for an empty/non-progressing or terminal
  page.
- An initial list request and its complete first page both have no cursor. That
  is a valid terminal page, including an empty manifest; only a present repeated
  successor cursor is non-progress, and the proof must close the exact count.
- An exact directory, collection, or snapshot list is backed by canonical
  `cymule.resource-manifest/3` JSON-lines bytes and a semantic descriptor whose
  ID binds canonical byte size, entry count, media type, and the sole Merkle
  root. Every returned page carries `cymule.resource-list-proof/5`; framework
  code verifies strict page order, the root-proven predecessor boundary, each
  entry's mathematical tree position against that same root, and request/next
  cursor continuity. Initial, empty, non-terminal, and terminal page shapes are
  closed and older cursor/proof generations are rejected.
  At every odd-width level, the final node's right sibling is that exact node,
  not a caller-chosen digest for a nonexistent extra entry. Inclusion and
  predecessor proofs enforce this same canonical duplication rule.
- Higher-profile handoffs use exact typed M1 StateRoot authorities and stable
  caller-supplied transfer IDs. Transfer, Run, producer-occurrence, slot, and wait identities
  use the Core 1..=512 Unicode-scalar/no-control grammar; never count UTF-8
  bytes. Handoff and activation authorities are fixed typed identities; their
  target indexes are StateRoot-owned persistent logs, not application journals
  with separately derived IDs. Reuse with different semantics fails closed.
- Transfer IDs are unique within one M1 domain, not scoped by target Run. Each
  transfer has one exact keyed handoff authority and one payload-free target
  index entry. A target-Run-scoped keyed slot map makes slot uniqueness an
  exact conflict rather than a history scan. Lookup resolves the per-transfer
  authority and the selected target-index entry directly. `incoming_page`
  reads at most 256 contiguous target-index references and exact-resolves only
  that page; there is no unbounded incoming API or global journal scan.
  A successor index is legal only after a full page and must equal checked
  `start_index + returned_count` within the shared exact-integer range. Empty
  and short pages are terminal; never skip unseen target-index references.
- The handoff persistence closure owns exactly four wire selectors:
  `cymule.resource-handoff/5`, `cymule.resource-handoff-activation/3`, and the
  handoff and activation target-index `/1` records. Older generations are
  rejected. Dead application-journal identity selectors are not retained once
  typed StateRoot maps become the sole persistence authority.
- Activation uses the same authority/index split with
  `cymule.resource-handoff-activation/3`. It requires the exact committed source
  transfer receipt and cannot rewrite the handoff. The
  `ResourceHandoffInput` coupled receipt binds that source receipt, the exact
  Resource command, complete target Run/Wait owner and result, and resulting
  Continuation digest. The activation receipt separately binds its exact target
  index position to that coupled receipt. Reuse with another target, wait,
  command, result, or Continuation conflicts; receipt-loss recovery is the
  typed read/verify path.
- An M1 mutation whose commit receipt is lost remains typed
  `CommitOutcomeUnknown` through the Resource boundary. Callers reopen and use
  exact typed command-receipt or keyed-current lookup. They never parse a
  generic application journal, treat that outcome as retryable persistence
  failure, or resubmit the same mutation blindly.
- Handoff input activation requires a target input wait whose Run, complete
  structural owner, correlation, and schema match the handoff result. The
  handoff must name an existing producer component occurrence whose exact
  output is already a typed Resource Handle Artifact. Handoffs never wrap full
  external bytes in a second Artifact. Transfer commits its canonical handoff
  authority and target reference index first; activation later commits the
  exact source reference, canonical activation authority, target activation
  index, wait result, and Continuation readiness in one M1 CAS.
- Transfer may publish a future slot while the exact target Run is Active and
  Running; it does not change that Run's Continuation. Only activation requires
  the exact pending input Wait and its Waiting Continuation. Do not reuse the
  activation-only precondition to reject an early transfer.
- Resource handoff admission is one closed Durable operation. The sealed
  `DurableResourceControl` view
  resolves the producer occurrence, exact result Artifact, target Run/slot or
  Wait, and every keyed Resource current from one StateRoot revision, then
  commits the Resource receipt/index and any Wait/Continuation coupling in one
  CAS. A controller preflight is never admission authority, and no generic
  transaction or application-journal mutation surface is exposed.
- The single Resource-to-Durable error mapper preserves validation, not-found,
  history/revision conflict, substrate, integrity, persistence, and
  unknown-commit categories. `Conflict`, `Substrate`, `Persistence`, and
  `Integrity` retain separate machine-readable `code` and human-readable
  `message` fields; never encode or recover a code through message prefixes. A
  CAS conflict remains a Resource conflict even when Durable has no current
  head.
- Resource integrations import Clock, Continuation, execution-claim, WaitOwner,
  and activation DTOs from `cymule-durable-protocol`; persisted WaitCondition
  and WaitState remain Durable-owned. Provider-backed mutations enter through
  `DurableRuntimeControl`; provider-free Resource reads use
  `DurableStoreControl` with bounded occurrence pages and exact `RunItem`
  selectors. Retired whole-Run views and resumable-runtime facades are not a
  compatibility boundary.
- Concrete local, object-storage, drive, WebDAV, sandbox, and HTTP adapters live
  under `plugins/` and should reuse mature maintained libraries where practical.
- Typed Artifact contracts are immutable, content-addressed, and pure. The
  `cymule.artifact-type-contract/1` domain owns canonical JSON plus a complete
  bounded acyclic document-local JSON Schema Draft 2020-12 contract. Typed
  references encode the exact contract ID, and retained contract Artifacts
  rebuild the registry after restart; no kind-to-latest alias may reinterpret
  bytes. Resolver/store I/O and
  integrity verification finish before contract decode. Opaque file, directory,
  collection, and snapshot bytes remain valid Resources without a contract or
  schema. Schema errors expose pointer paths and never rejected values.
- One typed Artifact schema is at most 1 MiB of canonical JSON, 16,384 JSON
  values, and 64 value levels with the root at level one. Include all data and
  annotation keyword values in those structural budgets. Candidate sealing,
  descriptor verification, and registry registration check the same limits
  iteratively before schema cloning, canonical hashing, or compilation; local
  reference traversal and rejection of an owned over-deep value must not use
  recursive traversal or recursive destruction.
- Local schema references use the pinned compiler's prepared Registry and
  Resolver for pointers, anchors, and subresource IDs with external retrieval
  disabled. Before compilation, iteratively expand schema-child and reference
  edges to at most 64 value levels, 65,536 schema visits, and 16 MiB of cumulative
  canonical subschema bytes. Repeated targets count again in both work budgets;
  broad shallow references are not subject to an arbitrary reference-count cap.
  Recursive reference cycles and unresolvable references are explicitly
  unsupported. Recheck dialect and document-local reference grammar when data
  becomes a schema through a reference, and compile with that same registry.
- Framework Resource Handles, manifests, list proofs, and handoffs use the
  closed framework `ArtifactTypeContract` registry. Lifecycle commands and
  receipts are internal profile authority, never ordinary cross-Run Artifacts.
  Do not replace an exact framework contract with a caller-chosen schema.
- `ResourceRetentionFamily` is the sole physical pin/GC/delete identity and
  contains only store binding, content digest, and their retention key.
  `ResourceRetentionSubject` adds one semantic Resource ID only as pin/delete
  audit provenance. Different semantic descriptors for the same binding and
  bytes share one pin count and cannot collect each other.
- Pin, release, GC, delete intent, and reconciliation enter only through the
  closed Resource/Agent/Virtual command unions and the shared pure Resource
  reducer. Keyed StateRoot maps retain exact command receipts and bounded
  currents; currents contain a `ResourceLifecycleReceiptRef` edge to an exact
  owning profile command/receipt, never an embedded receipt chain or global
  lifecycle replay log. Durable resolution exact-loads that graph and verifies
  the owning typed outcome before using a current as authority.
- `cymule.resource-lifecycle-receipt-ref/2` is one versioned closed locator
  union. Resource and Agent variants carry only their exact command and outer
  receipt identities; the Virtual variant additionally carries its scheduler
  partition and always names the outer `VirtualPersistenceReceipt`. A
  certificate ID or nested pin/release receipt ID is never a lifecycle receipt
  alias, and non-Virtual variants cannot carry a fabricated scheduler field.
- Delete intent precedes provider I/O and fences every later Resource, Agent,
  or Virtual pin for the physical family. Public commands request
  reconciliation but never carry removed-byte counts or absence booleans;
  `begin_delete` derives the provider binding only from the verified
  publication and does not accept a duplicate caller-supplied binding;
  `ResourceDeleter` deletes the retained exact published target and proves that
  new reads cannot resolve it and its current payload objects are absent before
  Durable derives the terminal receipt. A provider may retain a permanent
  non-payload tombstone when that is the executable fence against an in-flight
  writer; the tombstone never resolves as content or permits republishing, and
  explicit bounded GC reclaims any late unreachable payload objects without
  deleting that fence. Terminal deletion does not claim that every provider
  control-plane metadata key disappeared. Abort and
  completed-write convergence remove every
  owned staging/chunk object and return verified cleanup evidence.
- Upload cleanup persists one immutable `cymule.resource-cleanup-plan/1` with
  the exact strictly ordered owned target set before deleting anything. Commit
  and abort reconcile a retained plan, prove every target absent, persist the
  sole plan-derived terminal receipt, and expose exact receipt lookup after a
  lost response; replay never recounts already absent objects.
- A public `ResourceCleanupPlan` is at most 16 MiB of canonical JSON. Both
  construction and verification enforce `MAX_RESOURCE_CLEANUP_PLAN_BYTES`;
  adapter upload-record bounds are not a substitute for this shared gate.
- A cold virtual archive receives its fixed-owner content-derived permanent pin
  in the same M1 CAS that publishes the verified compaction receipt. The pin
  survives restart and has no TTL or generic Resource release path. Only the
  Virtual archive-retirement command may atomically publish its terminal
  retirement receipt and release that exact pin. Agent stream pins follow the
  same fixed-owner rule and are created only by external stream finalization.
- Empty manifests have one canonical byte digest and Merkle root. Provider-side
  locator and proof metadata uses immutable
  `cymule.resource-catalog-record/2` values
  through `ResourceCatalogStore`; an in-memory catalog is not restart authority.
  The protocol owns the single 16 MiB canonical-JSON catalog-record bound;
  adapters gate provider metadata before decode and must not define another
  catalog capacity.
