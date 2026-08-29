# Core Kernel Guidance

- This crate is the complete trusted semantic core. Keep it small, synchronous,
  deterministic, and free of ambient I/O.
- Never read the clock, random source, environment, filesystem, or network.
- `cymule-authenticated-collections` is the sole pure Map/Log root and proof
  authority used by the pinned reducer. Core verifies supplied proofs; it never
  performs resolver/provider I/O. Collection provider errors retain the exact
  canonical `ProviderFailure` payload through lower protocol boundaries rather
  than becoming corruption or being classified through `Display`.
- `CommandEnvelope.actor` is provenance supplied by the embedding. Core checks
  its identity shape, not authentication or authorization.
- Canonical IDs are computed only after semantic validation with the versioned
  JCS encoding in `canonical.rs`.
- `decode_json`, `canonical_bytes`, `canonical_digest`, and `content_id` share
  one recursive I-JSON number authority. Finite fractional numbers are legal;
  every mathematical integer must be within
  `-9007199254740991..=9007199254740991`, and safe integral floats normalize to
  the same in-memory and JCS form as integers before a Plan is sealed.
- `validate_identity` is the single public cross-profile identity contract:
  1..=512 Unicode scalar values with no control characters. Measure scalars,
  not UTF-8 bytes, and derive framework-owned child identities through a typed,
  domain-separated content preimage instead of concatenating caller IDs.
- `decode_json` is the sole raw JSON decoder for trusted Rust wire and canonical
  bytes. It rejects duplicate object members recursively before typed Serde
  decoding; do not add a permissive parser or fallback path.
- A schema-required nullable member is not an optional wire member. Its Rust
  `Option<T>` must use the required-nullable deserializer so explicit `null` is
  accepted and an absent member is rejected; never add `serde(default)` for it.
- `artifact_ref` is the sole authority for the collision-free,
  length-prefixed `cymule.artifact/2` identity preimage. Artifact references pin
  this identity version; higher layers must call the helper rather than copy its
  framing. Artifact records encode raw bytes only as canonical padded Base64;
  raw bytes are bounded at 8 MiB and complete canonical records at 12 MiB.
  `MAX_ARTIFACT_KIND_BYTES` exposes the existing 255-byte ASCII kind limit;
  higher layers consume that constant without duplicating the grammar or bound.
  Snapshot v11 is the only accepted snapshot wire version.
- Every `ComponentContract` declares a required `output_artifact_kind`. Plan
  admission validates that kind with Core's single Artifact-kind grammar; there
  is no missing-field default or runtime inference. Ordinary components declare
  `cymule.component-output/1`, while typed producers declare their exact typed
  kind in the sealed Plan.
- Reducers are pure over prior projection plus event. Do not hide mutations in
  caches or global state.
- `cymule.command/6` `StartRun` binds its exact Plan, execution binding, input
  Artifact, material digest, and initial Attempt. One admission atomically emits
  ordered `cymule.event/8` `RunStarted` then `AttemptStarted` Events; the initial
  Attempt uses epoch zero, fence one, and that same binding. Receipts and
  `cymule.command-admission/3` carry the complete ordered `event_ids`, never an
  optional singleton Event. No replay or compaction cut may split that batch.
- `MachineStartRunMaterial` is a non-wire owner of one material admission;
  its Plan, execution binding, and input are borrowed from that single set,
  never retained or serialized again as parallel payload fields. Each actual
  Plan/Artifact leaf is independently bounded at 12 MiB, with Artifact raw
  bytes still bounded at 8 MiB. Proposed leaves are charged once; independently
  authenticated parent values and key/absence witnesses remain separate inputs
  to the same 64 MiB read-set total. Fixed read authority contains only the
  material source/digest, not an aggregate material object treated as one leaf.
- `Machine::insert_plan` and `put_artifact` stage immutable proposals. Their
  read accessors may expose staged values, but snapshots and authority roots
  include only the exact material admitted by a command. Future proposals may
  remain staged. Failed command admission restores every consumed proposal and
  the exact prior semantic authority so the retained material remains retryable.
- The materialized `Machine` is the M0/Embedded reference and explicit audit or
  compaction implementation, not a coordinator cache. It exposes no
  `begin_mutation`, `prepare_delta`, or prepared materialized-snapshot guard.
  Do not restore snapshot-frontier caches, Store-CAS rollback transactions, or
  ordinary-delta factories for those retired interfaces. Durable coordination
  consumes the opaque pinned reducer and compaction preparations instead.
- Preserve closed effect, scope, attempt, and Run state machines. At most one
  Attempt is active for a Run; terminal Attempt epochs precede the terminal Run
  fence; deferred Effect release requires committed scope authority; and every
  illegal state combination fails closed.
- Effect admission resolves the exact entry-reachable Plan site and retains its
  complete structural identity preimage and Effect profile in canonical state
  so replay enforces dispatch and reconciliation without a provider.
- Every proposed Effect argument is an exact `cymule.effect-args/1` Artifact
  whose bytes are duplicate-free strict JSON, byte-equal to Core JCS, and valid
  against the origin Plan's exact Effect input schema. Command admission,
  exact command-admission replay, compacted Projection validation, and anchored restore use
  the same validator; a reference kind or content-address alone is insufficient.
- Runs retain ordered Plan and execution-binding lineages. A durable frame may
  name only the current Plan or an explicit historical migration-lineage Plan,
  and every Run default or migration binding must resolve to a retained exact
  `cymule.execution-binding/2` Artifact.
- Historical frame inspection is structural only. Resumable frames require an
  Active Run, current Plan/binding/epoch, and open scope. Closed frames enter
  only through a whole-Continuation descriptor: one root terminal frame, root
  scope stack, and no waits. Completion additionally requires the live Running
  claim; Effect settlement returns the exact set of pending retained intents
  and forbids Waiting. A settled closed Effect resumes only through the separate
  claim-free post-Effect Ready seam; it is neither ordinary resumable state nor
  a completion boundary. An M4 replacement additionally requires the exact Core
  migration-frame receipt bound to target epoch and target Continuation digest;
  M1 still owns the later execution-claim fence.
- `Projection::apply_event` independently enforces an Effect proposal's current
  Plan, current execution-binding Artifact, and exact Effect schema generation.
  Public replay accepts only the complete command record, receipt, admission
  chain, and Event closure; an independently content-addressed Event is never
  replay authority.
- Scope and Effect commands carry an entry-rooted invocation path plus exact
  Plan, definition, and Region path. Core derives the invocation ID and rejects
  a nested or invoked site attached to an unrelated execution scope. Durable
  historical validation passes the retained Plan explicitly; it never infers
  that an old occurrence used the Run's current Plan.
- Scope closure rejects every open descendant and requires the exact
  reducer-derived obligation set; callers never author obligation membership or
  resolution.
- Every transition to `CancelledBeforeRelease`, including scope abort and Run
  termination after implementation unavailability, closes reconciliation. Work
  that provably never crossed the dispatch boundary cannot retain a governance
  obligation.
- Failure and cancellation convert every dispatch-started, unobserved Effect to
  `Unknown` atomically with terminal execution. Only `Reconcile` may settle it
  after terminal execution; `Observe` is active-execution evidence and cannot
  bypass the closed reconciliation path. Terminal execution never reopens.
- `CompleteRun` requires derived world settlement to be `Settled`. Observational
  Effects create no blocking obligation, but they must still settle before
  execution becomes `Completed`; `Completed` plus `Unknown` is never valid.
- `MarkUnavailable` before dispatch is authoritative `NotApplied` cancellation.
  An unavailable Effect may reconcile to `Applied` only when its retained phase
  proves dispatch started.
- IR scopes have one auto-commit shape and no mode. Plan admission rejects an
  Effect result binding unless the referenced profile is observational/eager,
  including in every nested scope and non-entry definition.
- `cymule.ir/3` is the sole accepted IR generation. It closes the scope-mode
  removal and bound-Effect admission semantics; Core has no `/2` reader,
  translator, or shape fallback.
- `Operation::Invoke` targets a definition in the same sealed Plan. Keep
  definition lookup semantic and immutable; logical registries and future-head
  selection belong in `cymule-evolution`.
- Reject self-recursion and every recursive invocation SCC before Plan identity
  is computed; collect invokes through nested scopes and permit acyclic diamonds.
- `seal_plan` is the sole Plan sealer. Its pure Draft 2020-12 compilation uses
  the maintained schema library with external retrieval disabled.
- Do not add a provider name or transport detail to IR, events, or projections.
- `MachineSnapshot::command_digests` exposes only stable validation evidence for
  durable exact-delta checks. Private hot command records retain the exact
  admitted envelope, semantic hash, and receipt. Applied and Conflict outcomes
  share one ordered `cymule.command-admission/3` hash chain with exact
  before/after Projection digests; Conflict creates no Event and advances no Run
  frontier. Cut admissions move into independent immutable
  `MachineCommandArchiveSegment` objects whose ordered entries are Merkle-bound.
  A cumulative 256-level sparse-Merkle map binds every archived command ID to
  its exact admission and independently addressable archive-entry digest. The
  materialized Machine retains only the current archive head/count, admission
  head, command-index root, Projection base, and a bounded hot suffix; never put
  the archive-object map back into `MachineSnapshot` or another materialized
  state.
- Every atomic batch retains its ordered manifest, exact per-command hashes
  and receipts, explicit material source, flattened Event sequence, and terminal
  receipt in `cymule.command-batch/1`. `verify_entry` is the single complete
  entry/member/position/receipt check used by archive readers and GC.
  The manifest's frozen source root is distinct from the actual linear
  admission parent: unrelated Runs or material admissions may advance the latter
  during paging. Full audit proves that source is an observed ancestor and no
  same-Run admission superseded it; no arbitrary old digest is sufficient.
- An explicit framework material admission records the complete proposed
  `material_source` (source command, Plan IDs, Artifact references) and its
  digest, not merely inserted values or the batch's required-input union.
  That source is framework provenance and may name the external Agent,
  component, or profile operation rather than an internal Core member. The
  external receipt binds source and batch in one direction; Core must not
  invent external authorization or a receipt-identity cycle.
  Replay loads those exact bytes and recomputes the digest. A zero-command
  batch is legal only for nonempty explicit material and has no command receipt
  or Event; the external profile receipt binds that source in the same CAS.
  `StartRun` intrinsic material remains independently bound by its command and
  need not be duplicated as an explicit batch material source.
- Public material-only preparation emits that real zero-command batch and
  advances the same batch commitment/order. Ordinary batch preparation uses a
  private material step and emits only its one outer batch. Never leave material
  root changes outside batch history or invent a canonical Event for them.
- Raw Event append is crate-private and can publish only a Projection already
  validated by typed command admission. Public canonical mutation enters through
  `Machine::submit` or its archive-proof variant; snapshot restore replays those
  same retained commands and receipts rather than admitting Event bodies.
- Attempt, Continuation, occurrence-binding, Effect-intent, Plan, binding, and
  safe-point identities at semantic execution boundaries are exact lowercase
  SHA-256 content IDs. Descriptive labels cannot enter those fields.
- `cymule.machine-delta/6` is the only durable incremental Machine
  representation. It retains hot command records privately, permits only
  additive canonical inputs or one exact archive-backed compaction cut, and
  binds parent/result semantic authority roots and exact base-anchor identities.
  Ordinary delta application rejects compaction; the explicit compaction seam
  requires the exact independently persisted segment, including exact equality
  between its Applied Event sequence and the delta cut. Both live `Machine` and
  portable `MachineSnapshot` application are transactional and run complete hot
  Event/command/batch closure before publishing anything. `Machine::replay`
  requires Plans, Artifacts, complete batch manifests, and command entries;
  never infer a missing singleton batch or pre-admit a final material catalog.
- Generic and pinned Machines share one `cymule.machine-authority-root/2`
  preimage over admitted material/batch commitments, the semantic Projection,
  cumulative Event count, and admission head. Physical compaction does not
  change it. Snapshot, delta, and Store authority separately bind the exact
  physical base anchor; do not remove those checks.
  The anchor's required `archive_batch_count` binds the complete cumulative
  cut count, including material-only batches, so Store verification requires
  `hot batch count + archive batch count == cumulative batch count` without
  hydrating the archived Projection.
- `Machine::restore` fully audits an uncompacted snapshot. A compacted untrusted
  import uses `restore_with_archive` with the complete independent segment chain.
  A caller may use `restore_anchored` or the matching anchored delta seam only
  after its external Store head supplies the exact content-addressed
  `MachineBaseAnchor`; that path verifies the anchored base Projection and
  reduces only hot admissions. Before accepting the base, both anchored and
  full archive restore compare the independently reconstructed Plan/Artifact
  admission commitments and counts with the exact cut fields. A valid base
  anchor never substitutes for checking those material prefixes, including
  fully cold snapshots with no hot batch to check the next parent root.
  On a base-backed hot miss, plain submit and
  receipt lookup return `ArchivedCommandReplayRequired`. The external immutable
  object Store resolves an O(256) current-root membership/non-membership proof:
  membership loads the exact entry by its authenticated digest for lost-ack
  replay or command-reuse rejection; non-membership alone authorizes a new hot
  admission. `cymule.command-index-proof/2` represents non-membership with the
  exact canonical empty-subtree depth and stores only siblings above that
  subtree; omitted lower siblings are the domain-defined empty hashes, never
  caller-selected values, and a redundant leading empty sibling is rejected
  rather than accepted as a second wire for the same proof. Segment, entry, and
  sparse-node objects share one closed outer tagged archive-object interface and
  the exported `MAX_MACHINE_COMMAND_ARCHIVE_OBJECT_BYTES` acceptance bound, but
  remain outside materialized state. A boolean,
  self-derived anchor, or hot per-command archive map is never trust authority.
  `compact_event_free_admissions` rotates conflict-only or material-only hot
  tails through the same archive protocol. Pure material segments have no
  command entry or Event; their
  required-nullable admission head remains the parent's head, including null at
  genesis, while batch/material commitments advance. No separate archive path
  or synthetic admission is permitted.
- A `cymule.machine-snapshot/11` `cymule.machine-base/4` replaces only a complete
  batch/admission cut and its causally closed Event prefix. The base retains the
  Projection and material/batch commitments at that cut, not at the end of the
  retained suffix. The independent archive retains complete ordered Events,
  commands, and batches. Full audit reconstructs that authority; anchored
  restore validates the trusted base and replays the complete suffix. Any
  missing parent, split batch, foreign archive batch, or wrong cut commitment
  fails before publishing a delta.
- A cut must also retain every frozen source needed by the hot suffix. Each
  retained batch's actual admission parent continues the cut's linear root
  chain; its frozen source must be the new cut root or an earlier root in that
  retained chain. An Event cut ends at its exact admission batch and never
  sweeps later material-only batches into the base. The same source-cut
  validator governs materialized creation and delta application; a requested
  cut which would discard a required source returns `Causal` before mutation.
  Do not weaken raw ancestor checks, keep an unbounded persisted ancestor list,
  or fetch cold archives as a hot-restore fallback.
- Base identity, restore, incremental compaction, and replay share one complete
  reducer-invariant validator. It closes Run terminal status against Attempts,
  scopes, Results, failure/cancellation evidence, Effect state combinations,
  scope membership, and exact obligations; Machine authority separately resolves
  every retained Artifact.
- Snapshot restore also closes Event and command authority in both directions:
  every Event has exactly one applied receipt, every applied receipt names a
  retained or compacted Event, and retained command IDs and hashes match.
  `MachineRootParts` additionally requires duplicate-free admission orders with
  exact map/order key-set equality for Plans, Artifacts, and batches. Every
  batch key equals its verified record identity, and complete batch, command,
  receipt, admission, Event, and proof closure is audited before reconstruction.
  Missing keys must never panic or allow unlisted records to be silently dropped;
  a valid zero-command material batch remains part of the same ordered closure.
- A compacted base retains the content-addressed archive head, cumulative
  admission/Event counts, required-nullable admission head, cumulative
  command-index root, material/batch cut commitments, and authenticated
  Projection digest. The independent segment chain retains complete batches,
  admissions, command records, receipts, Events, and sparse-map witnesses.
  Raw audit replays that chain; hot anchored restore does not traverse it.
- Paged Scope/Run closure persists the original bounded `batch_manifest` and
  separate proposal-only Plan/Artifact map roots. Those staged maps are GC
  roots but not global semantic admissions. Every page retains that manifest
  and the exact Run fence; finalization rechecks current material membership,
  deduplicates concurrent identical admission, and publishes the original
  complete batch/receipt once. Page progress exposes no partial semantic result.
  Run cancellation/failure finalization writes the exact active Attempt through
  typed `PutAttempts` on its shadow child root, then publishes that root with
  the terminal Run fence and complete batch. Intermediate pages never make the
  terminal Attempt visible through the live Run root.
- A Core-owned atomic batch may close a Scope inline only after complete
  authenticated Map membership and Log order proofs establish the exact bounded
  Effect/obligation set and a childless open Scope. Inline and paged paths use
  the same closure reducer. The inline budget is dynamic values at most
  `4 * 256`, plus only the exact target Scope and optional direct parent (at
  most two structural reads); total bytes remain 64 MiB. Ordinary read sets
  retain their 1024-entry bound. Extra leaves/proofs/scopes do not consume a
  generic allowance. An oversized multi-command closure returns typed
  `PagedScopeRequired` before provider I/O; a single large closure uses the
  explicit persisted page protocol, never a larger page or split semantic CAS.
- Wide compaction builds one lightweight Plan/Artifact/Projection authority and
  advances that Projection in place. Never clone the full Machine per cut Event.
  A fully compacted Machine still has an authenticated base Projection, so
  missing replay inputs classify as `ProjectionOnly`, not `Unavailable`.
- `durable_internal::prepare_pinned_compaction` is explicit offline maintenance,
  not an ordinary bounded transition. Its embedding reads one complete exact
  Core source from the pinned Store roots; Core checks keyed closure, the exact
  base anchor and command index, and the semantic frontier, then replays the hot
  source once using only that trusted base. The restored Run and Fact counts
  must exactly equal the corresponding normalized map-root entry counts;
  those physical roots are deliberately outside the semantic digest. This
  uses the existing Projection and never triggers another replay or cold scan.
  Both pending-command and paged-work
  maps must be authenticated empty. `MachineCompactionIntent::EventPrefix` and
  `EventFreeAdmissions` reuse the same causal-cut implementation. The opaque
  prepared result changes only physical base/archive/proof authority, preserves
  the semantic root and every cumulative count, and supplies the only root delta
  and archive the embedding may publish in one same-source CAS. It is never a
  caller-authored snapshot/delta, cached full-state transaction, or cold fallback.
- `MigrateRun` command admission and Event replay use one payload validator:
  source and target Plans differ and every Plan, binding, and safe-point proof
  identity is a lowercase SHA-256 content ID. The retained migration receipt
  also binds the target epoch and complete target Continuation digest. Both
  materialized and pinned reducers assign that validated target epoch before
  deriving the resulting Run precondition and receipt. Never let
  a forged Event or old receipt bypass command admission or a later epoch.
- Pinned frame inspection is structural and permits closed terminal frames;
  actual resume/command boundaries separately enforce open Scope and execution
  authority. Scope path and source-Run digests are canonical 64-hex digests, not
  `sha256:` content IDs. Paged processed Effect/Scope result records receive
  typed content IDs before extending the processed lineage.
- Property failures persist under `proptest-regressions/`. Commit the minimized
  corpus file with its fix; never depend on an ephemeral CI seed alone.
- Changes here require specification, schema, conformance, and SDK review.
