# Changelog

All notable changes to Cymule are documented here. The project follows Semantic
Versioning, with semantic compatibility additionally split into the version
domains described in `docs/specification.md`.

## [Unreleased]

- Establish `cymule-durable-protocol` as the sole lower-layer authority for
  Clock receipts, Continuations/frames, execution claims, wait ownership, and
  identified wait activation. Profile and Durable crates now import these
  contracts directly without copied DTOs or compatibility re-exports.

- Revalidate both closed npm archives against current registry state inside the
  protected tag writer before creating an immutable release tag.

- Advance Resource Handles to `cymule.resource/3`, manifests to
  `cymule.resource-manifest/3`, list proofs to `/5`, list cursors to `/3`,
  locator sets to `cymule.resource-locators/2`, and provider catalog records to
  `cymule.resource-catalog-record/2`, with no predecessor readers. Descriptors
  now admit at most 64 annotations and 4 MiB of canonical JSON; locator sets
  admit at most 16 canonical ASCII locations and 256 KiB; catalog records have
  one protocol-owned 16 MiB canonical JSON bound enforced before provider-body
  materialization.
  Canonical manifest entry bytes now derive the sole Merkle content authority;
  the descriptor ID binds that root, canonical byte size, entry count, and media
  type. Full copies stream-reconstruct the same descriptor, with no parallel
  raw-byte SHA on the manifest path.
- Replace global Resource lifecycle and handoff journals with typed keyed
  StateRoot authority. Resource commands retain exact command/outcome receipts;
  retention, pin, deletion, transfer, slot, and activation currents use direct
  keyed lookup, while target enumeration uses owner-bound persistent-log pages.
  Generic release is explicit-pin-only; Virtual retirement and Agent
  finalization own profile pins atomically. Deletion fences a normalized
  provider target, and only Durable may commit completion after binding-exact
  absence readback.
- Advance the internal Plan authority from `cymule.ir/2` to `cymule.ir/3`
  without a legacy reader. The new generation removes the non-semantic
  pre-release `scope.mode` field from Rust and all four SDK builders; scoped
  Plan bytes carrying the old transactional/speculative labels fail closed.
  Every component contract now also declares one required
  `output_artifact_kind`; successful Call output is validated against the
  component output schema and stored once under that exact declared kind.
- Admit Effect result bindings only for observational eager dispatch before
  sealing or execution, and expose that bound observation shape in every SDK.
- Classify legacy component Call as duplicate-possible before its durable result
  checkpoint instead of implying provider exactly-once behavior.
- Advance the process plugin protocol to `cymule.plugin/3`; dispatch and
  reconciliation now carry and echo one content-addressed
  `cymule.effect-provider-attempt/1` bound to the retained intent claim.
- Advance public M1 control to `cymule.durable-control/4`, close component
  occurrences to `succeeded` or `expected_failure` outcomes,
  project Run execution separately from external-world settlement, require
  every Effect (including observational Effects) to settle before Run
  completion, and add store-only Run cancellation that fences Attempts and
  cancels pending waits and unreleased Effects without discarding dispatched
  ambiguity. Failed and cancelled Runs settle that ambiguity only through the
  closed reconciliation path.
- Normalize Effect dispatch payloads under each Run's query roots and retain
  only an immutable global intent-to-Run locator. Wide failure and cancellation
  now page one Core-bound hidden sidecar companion, preserve unrelated Run
  commits, fence late same-Run results, and resume an already admitted
  `ExpectedFailure` without provider or Clock work.
- Resolve exact hot/cold Start replay before Clock admission. Retained replay
  verifies its singleton batch and exact Plan/binding/input material; a fresh
  Start no longer constructs and discards a duplicate Machine stage.
- Separate Plan-scoped component occurrence identity from Continuation and
  provider-Attempt realization in `cymule.semantic/6`. Persist the occurrence
  and current fenced Attempt before provider I/O, reuse the same occurrence on
  explicit expiry-proven takeover, and reject stale Attempt output.
- Require execution commands to carry only an issued
  `cymule.clock-observation/2` reference for their single-driver claim. The
  selected Clock generation resolves its retained receipt and holds a
  non-blocking current-head guard across the final claim CAS; callers cannot
  author logical time, infer expiry, or take over implicitly.
- Initialize the official SQLite Clock receipt ledger as one atomic exact-shape
  generation and reject partial, extended, or shape-different authorities
  without repair or fallback.
- Require retry decisions and restored retry streams to resolve opaque Clock
  references against the exact issued-receipt authority before logical time can
  advance.
- Make identified signal/timer activation a store-only transition that admits
  the result Artifact, wait completion, and Ready Continuation in one CAS
  without an executor, Clock, or implicit resume. The
  `cymule.wait-activation-receipt/3` record retains the complete selected targets,
  newly applied subset, and original ready-Run set;
  completed/cancelled nonwinners no longer poison broadcast delivery or wake
  M3 work.
- Freeze HTTP and timer persistence as
  `cymule.activation-http-spool/1` and
  `cymule.activation-timer-store/1`. Both initialize only an atomically
  rechecked empty database and reject every nonempty predecessor, partial, or
  extended shape as `unsupported_store_generation`; neither repairs or imports
  in place.
- Rename the misleading public `AuthorityLease` type to `CoordinationLease`
  without a wire-shape alias; coordination ownership and fencing do not grant
  capability authorization.
- Establish one machine-readable version-domain registry with canonical schema
  digests, source/package ownership, and a generated specification table. npm
  and crates staging bind the exact source SHA and registry digest;
  finalization materializes `cymule.release-bom/3` as the exact-SHA release
  asset. BOM/3 requires a `publication` member for every source package, records
  distinct non-null private and rewritten-public source SHAs,
  fresh closed Cargo/npm registry evidence, preserves the Fulcio-signed npm
  publisher ref/SHA, and uses explicit `null` for Python/Go packages without
  publication authority. The current finalizer commit is intentionally outside
  the stable BOM bytes and is instead bound by the finalization stage, Artifact
  Attestation, and same-run control-plane receipt.
  `scripts/release_contracts.py` now owns the four registered release-only
  selectors used at those boundaries.
- Split GitHub Release finalization into credential-free verification/freeze and
  one current-main `contents: write` controller over an authenticated notes/BOM
  bundle. Terminal npm, crates.io, and Release jobs now re-read the remote
  annotated tag target against the frozen release SHA immediately before
  mutation. npm moves trusted publication to the new inert
  `publish-npm-release.yml` caller generation `/1`, preserves exact calling-tag
  provenance, and runs contents-write/OIDC work only through the resolved
  current-main reusable controller; neither npm nor crates executes tag-carried
  code in its terminal job. Its inert call supplies the exact GitHub permission
  ceiling, while both tag mutation and npm publication require the protected
  `npm` environment. Tag push response loss now converges by exact remote
  readback; every crate or Release mutation rechecks current main and the peeled
  tag, and Release finalization rejects every asset outside the frozen BOM.
- Close optional Agent host execution behind one fresh-Started dispatch path.
  Context requests and snapshots now pin message head plus count, historical
  pages retain that prefix after later append, and cumulative message-current
  bytes are independent from page-wire bytes. Recovery cannot accept Context
  completion without the original reader evidence. Stream Open now preflights
  the exact final AgentUpdate size for staged and prederived external Object
  delivery, reserving the terminal NotApplied observation slot as well.
- Fence object-store deletion with a permanent non-payload Deleted index so a
  delayed publisher cannot restore visibility; later bounded GC reclaims any
  unreachable late parts. Filesystem imports validate the complete source before
  Publishing and classify malformed persisted upload records as integrity
  failures without changing normal caller validation.
  Release finalization also freezes the distinct annotated-tag object as
  `release_tag_sha` in finalization manifest schema 2; freeze, publish, and every
  Release mutation exact-match both the raw tag ref and peeled commit; terminal
  readback repeats that fence even on an already-converged no-op, so a
  same-commit retag with changed annotation or signature has no authority.
  Stable Release finalization now shares one global non-cancelling lock, reads
  every REST page plus exact Latest projection around each mutation, publishes
  historical recovery explicitly non-Latest, and accepts only one terminal
  Latest equal to the highest numeric stable version.
  Stable npm publication is globally serialized, closes package configuration
  and terminal flags to the official public registry plus `latest`, rejects
  version rollback, reads mutable tag state back, and verifies the complete
  Sigstore bundle before accepting its singleton provenance statement. The
  resolved current-main verifier, not tag-carried code, owns both irreversible
  tag admission and final registry verification. Every stable release workflow
  rejects non-canonical or multiline version input before constructing a Git ref
  or writing workflow outputs.
  Crates publication likewise resolves every PUT response or transport loss
  through bounded exact-checksum readback before succeeding, failing, or
  reporting an ambiguous unchanged retry identity.
- Prevent a stale private mirror pipeline from rolling public main backward:
  the controller re-reads current private main and force-publishes only against
  the exact previously observed public-tip lease before terminal readback. A
  failed push response now still receives one bounded exact-tip readback, so an
  already committed write converges in the same run and an unavailable
  readback is reported as ambiguous without changing the retry identity.
- Register every production Artifact-kind, content-ID, catalog, selector,
  persistence, SDK, schema, and release literal, and converge component
  occurrence preflight/checkpoint/restore on one typed identity preimage.
- Keep the public Run identity at 1..512 Unicode scalars across every layer and
  replace concatenated internal command, Continuation, embedded capacity-slot,
  and virtual archive-write IDs with typed fixed-length content identities.
- Remove unproven whole-state store importers. Exact `66a432c` bytes now fail
  before mutation with `unsupported_store_generation`; serving compatibility
  is generation routing with one writer, not decode fallback. The official
  physical contracts advance to `cymule.directory-store/5` and
  `cymule.sqlite-store/6` without readers for any predecessor generation;
  only an atomically rechecked empty authority may create its complete schema,
  while partial or shape-different authorities fail without repair.
- Pin each virtual region to an exact RegionSource binding/revision, publish
  bounded payload Artifacts with its frontier CAS, and replace caller-authored
  virtual Plan/binding strings with Rust-admitted typed ExecutionBinding
  Artifacts in checkpoint `/4`, occurrence `/3`, claim-control `/4`,
  migration/archive `/2`, and compaction-certificate `/4` domains. Claim
  receipts retain the complete normalized higher-profile journal manifest for
  exact replay. Generic post-initialization checkpointing is removed; typed M3
  commands and Clock-guarded final CAS are the only mutation paths.
- Return the closed `VirtualClaimOutcome` from public M3 claim admission:
  `NoWork` carries only the exact normalized receipt, while `Claimed` also
  carries the non-null claim and complete verified `SealedPlan` loaded from the
  same pinned StateRoot. Persistent claim receipts remain Plan-ID and
  execution-binding-reference only; there is no raw Plan reader.
- Replace whole-Machine segment/checkpoint persistence with bounded
  `cymule.machine-delta/4` transitions lowered into one fixed StateRoot object
  graph, advance exact durable state to `cymule.durable-state/7`, keep all-ever
  journals and receipts behind authenticated exact-key lookup, and materialize
  only the bounded active projection on reopen. Cold reclamation separates
  idempotent reconciliation of the current pinned page from explicit advancement
  to another page; predecessor physical generations have no reader.
- Advance archived-command membership authority to
  `cymule.command-index-proof/2`. Non-membership now records an exact canonical
  empty-subtree depth and only the siblings above it, eliminating hundreds of
  derivable hashes per hot command without weakening current-root proof
  validation or admitting a legacy proof reader.
- Admit historical provider work per exact operation pin, settle unavailable
  intents only through independent evidence, and replay committed replacement
  authorization before rechecking an obsolete safe point.
- Advance Engine transport to `cymule.engine/5`, live-evolution control to
  `cymule.live-evolution-control/6`, persistence commands and receipts to `/4`,
  normalized state leaves to `/3`, and the process-provider wire to
  `cymule.evolution-plugin/3`. `cymule-profile-protocol::evolution` now owns the
  portable reducer and exact bounded StateRoot DTOs; the provider-bound
  `DurableEvolutionControl` is the sole current/receipt read and typed commit
  authority. The former whole-state and generic-history persistence surfaces
  have no reader, writer, or compatibility bridge.
- Return one `EvolutionCommit` from every successful
  `execute_live_evolution(target, evolution_id, command)`. The semantic receipt
  binds the complete command, parent current, optional Durable source witness,
  closed outcome, and ordered normalized writes, while physical observed and
  committed revisions remain only in the commit envelope.
- Make every reusable-definition reference carry an explicit closed strategy;
  `LatestCompatible` is no longer an omitted-wire or Rust `Default` path.
- Establish `MigrationSafePoint::new(domain_revision, &Continuation)` as the
  sole `cymule.run-quiescence/1` identity derivation. Durable proves whole-Run
  quiescence and calls that constructor instead of owning a duplicate preimage.
- Require a fresh migration's fixed provider registry to resolve the complete
  target `ExecutionBinding` by the deterministically prepared Plan. The profile
  admits and canonicalizes that binding before provider I/O and retains its
  Artifact with the typed Core migration and target Continuation in one CAS;
  ambient and Ref-only authority are removed.
- Expose typed two-phase preparation for generic and Virtual occurrence
  selection, plus a fresh-only migration sidecar projection. Durable no longer
  needs private-leaf parsing to resolve a selected Plan, and exact migration
  record replay cannot reapply the Core migration.
- Reuse historical definition revisions, linked Plans, links, and exact
  `cymule.plan-edge/2` structural transitions. The first accepted `EdgeRecord`
  evidence remains immutable; later publication evidence remains independently
  retained by its complete command receipt and Artifact. Every generated future
  decision binds its source decision, so Plan cycles cannot reuse an evidence
  accumulator or overwrite prior evidence. `cymule.rollout-transition/2` now
  derives solely from the retained source decision, target decision, and
  complete verified gate evaluation, making every transition identity
  independently recomputable.
- Require every Engine v5 success to echo the complete inner request actually
  accepted by strict decoding beside its response. All SDKs compare the actual
  sent wire before response validation instead of recomputing Rust-derived
  Plan, Resource, Clock, durable-control, or rollout results. Failure carries no
  request because it may precede decoding; every predecessor success shape
  fails closed. Clock issuance now returns a typed Run-bound observation result
  so SDKs never duplicate opaque scope derivation. RPC stdin is cancellation-
  aware and capped at 64 MiB before JSON decoding, while empty Resource
  annotations have only the omitted wire.
- Separate coupled M3 claim execution into the typed
  `DurableVirtualControl::claim` facade. It is not an M4 control command,
  accepts no caller-authored M4 selector, and commits the occurrence selection,
  M3 claim, and M1 execution authority through one private atomic lowering path.
- Require every current or historical selected-operation binding and resolved
  provider dependency closure to match the runtime owner's admitted executable,
  then consume one privately constructed `FnOnce` provider token; manifests
  remain capability advertisements only.
- Make external Effect-resolution replay exact over both terminal outcome and
  canonical result Artifact instead of accepting any request after settlement.
- Share one complete migration-receipt validator across admission and restore,
  align positive epochs/non-empty scope stacks across SDKs, and atomically
  convert claimed immutable-binding loss to retained-fence `Unknown`
  governance before any live provider call.
- Publish a successful migration target as a claim-free `Ready` Continuation;
  a later ordinary resume independently acquires its next claim and Attempt.
- Share complete Restart receipt validation across authorization, restore, and
  all SDK success decoders; reject unsafe epochs and malformed input/evidence.
- Prevent provider error-code spoofing and replay retained migrations before
  adapter Describe.
- Require chunkless Agent stream finalization to observe an exact
  `ResourcePublication` through its resolver, and atomically retain that
  publication's non-secret locator authority as a `ResourceCatalogRecord`
  beside the terminal stream and Session records. Descriptor-only output now
  fails closed, while reopen restores the exact publication for provider readback.
- Reestablish filesystem catalog/manifest-index durability on equal retry,
  enforce the 8 MiB Resource read bound inside both official adapters, and
  return empty-object EOF without issuing a zero-length object-store range.

## [0.2.0] - 2026-08-21

- Replace `cymule.virtual-checkpoint/1` whole-snapshot journal records with
  `cymule.virtual-checkpoint/2` content-addressed incremental deltas, a 4 MiB
  canonical delta bound, authenticated lineage, linear history, and exact
  reopen.
- Make durable HTTP signal selection use fair parked-key cursor pages plus an
  indexed SQLite match, and reject timer acknowledgement before durable target
  selection.
- Complete M4 safe-point migration as one lost-acknowledgement-safe CAS that
  changes Machine Plan/binding, Continuation state, epoch, Attempt, Artifacts,
  and evolution receipt together.
- Separate virtual occurrence Plan pins from exact ExecutionBinding Artifact
  pins, retain the authoritative fallback across rollback/relink, and route
  composed operations to their exact admitted providers.
- Advance the affected command, Event, Machine, durable, evolution, live
  evolution, virtual occurrence, and virtual claim wire domains.
- Replace validation-only durable SDK calls with a real closed CLI transport
  for Run start, query, resume, signal admission, explicit effect release, and
  durable live evolution.
- Require component and effect realization requirements in every language
  builder and freeze Go candidates at `Finish`.
- Align Rust, TypeScript, Python, and Go on strict JSON, structured Engine
  failures, deadlines, cancellation, and unknown-world reconciliation.
- Publish the TypeScript API under both `cymule` and `@cymule/sdk`, and add
  installed-package quick starts plus four-language package witnesses.
- Close npm and crates.io bytes through independent no-OIDC builds, reduce
  terminal publishers to exact-byte upload, authenticate Release metadata, and
  remove the retired public mirror controller from the complete exported
  history.

## [0.1.4] - 2026-08-18

- Add day-one SQLite and filesystem/object-store persistence plugins with
  immediate contention, idempotent chunking, conditional publication, and
  content verification.
- Add HTTP signal and durable timer activation sources that acknowledge only
  after M1 admission, plus a hardened bounded process executor.
- Add composable OpenTelemetry/OTLP observations and an official RMCP tool
  adapter without introducing Agent Loop semantics into the framework.
- Split every plugin family into independently routed Rust verification suites
  and extend the ordered crates.io release catalog.
- Make first-publication recovery honor crates.io's bounded new-crate rate-limit
  timestamp without retrying unrelated registry failures.
- Separate the current release controller from immutable tag payloads so an old
  partial release can use reviewed recovery logic without changing its bytes.

## [0.1.3] - 2026-08-18

- Publish the canonical Rust facade as `cymule` plus the CLI, semantic profile
  crates, and official reusable plugins through an ordered crates.io release.
- Add deterministic whole-workspace Cargo archives, normalized-manifest
  compilation, checksum-checked retries, and fresh registry consumer/install
  verification.
- Add an idempotent GitHub Actions crates.io workflow with temporary
  first-release bootstrap and OIDC trusted publishing for normal releases.

## [0.1.2] - 2026-08-18

- Make `latest_compatible` the actual reference default and block automatic
  future-head changes that widen reachable component, effect, wait,
  capability, or authority surfaces.
- Replace caller-asserted migration booleans with content-addressed safe-point
  proofs verified against durable Continuations, and add first-class
  `restart_under_new_plan` authorization through `cymule.evolution-control/2`
  across all four SDKs.
- Add a separately sharded M4 mutation witness for compatibility, safe-point,
  automatic relink, and replacement-Run admission laws.

## [0.1.1] - 2026-08-18

- Introduce frozen `cymule.ir/2` with reusable local definition invocation,
  durable invocation frames, and matching TypeScript, Python, Rust, and Go
  authoring/execution conformance.
- Add latest-compatible reusable-module registry linking with exact-schema
  compatibility, transitive dependent relinking, pinned revisions, historical
  parent Plan retention, and durable tamper-checked recovery.
- Add a self-contained Hello World Flow and example plugin as the stable user
  quick start.
- Publish GitHub-native repository metadata, CI, and a clean-history public
  mirror workflow.
- Add the first M1 durable profile foundation: portable Machine snapshots,
  whole-state CAS, full Continuations, durable waits, leases, effect outbox,
  component occurrences, snapshot records, and an atomic directory adapter.
- Add resumable sequential call/wait execution with process reopen, Attempt
  epoch advancement, and exact component-result replay.
- Add the M2 agent interaction foundation with typed Session updates,
  context/model/tool/permission/elicitation/workspace interfaces, ordered
  projections, and a tested model-tool-model reference turn driver.
- Add the M3 virtual-work foundation with opaque cursors, bounded frontiers,
  deterministic Run fairness, capability claims, fencing, parked indexes, and
  million-item snapshot/restore tests.
- Publish the TypeScript SDK as the public `cymule` npm package through GitHub
  Actions trusted publishing with provenance.
- Adopt a project-wide build-versus-adopt policy and non-blocking lock boundary;
  prefer Tokio and other maintained mechanisms below Cymule semantics.
- Add the M4 evolution foundation with immutable Plan DAG edges, impact cones,
  occurrence pins, deterministic canary and rollback, safe-point migration
  receipts, shadow evidence, and cycle/fault tests.
- Complete the bounded M4 profile with transitive reusable modules, durable
  registry recovery, exact reviewed patches, checked migration/shadow plugins,
  deterministic evidence gates, mixed-version dispatch, and four-SDK controls.
- Add resumable commit-gated effect execution with fenced outbox claims and a
  crash-after-provider-application test proving recovery reconciles without
  redispatch.
- Add authenticated canonical Event-prefix compaction with exact suffix
  rehydration and atomic Resource handoff-to-input activation, both fault-tested
  for stale writers and lost acknowledgements.

## [0.1.0] - 2026-08-16

- Initial Rust-first semantic kernel and embedded runtime.
- Frozen `cymule.ir/1` plan candidate and canonical encoding.
- Causal event replay, command idempotency, epoch fencing, scopes, effect
  obligations, occurrence binding, and replay availability.
- TypeScript, Python, Rust, and Go SDKs with cross-language end-to-end tests.
- Provider-neutral process plugin protocol and conformance adapter.
- Partial optional MLIR workbench with generic-operation syntax and host-tool
  validation; a registered dialect and lowering passes remain proposed.
