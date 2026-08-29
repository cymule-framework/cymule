# TypeScript SDK Guidance

- Support maintained Node.js LTS lines and use strict TypeScript.
- Keep the package dependency-free at runtime.
- Control command builders use the implemented Engine/DurableEngine methods.
  Do not restore the caller-zero generic control-submit interfaces or aliases.
- Do not depend on object insertion order for identity; the Rust engine performs
  canonicalization and sealing.
- Keep Resource unions closed and dependency-free. Never normalize URLs or hash
  Resource Candidates in TypeScript; `CliEngine.sealResource` is authoritative.
- `cymule.resource-handoff/5` carries the producer's exact typed result Artifact
  to a distinct target Run. The builder validates closed Artifact references
  and exact equality; it never substitutes or hashes a Resource locally.
- Wait activation builders sort and deduplicate exact wait targets while
  preserving delivery, source, and Artifact identities. Engine verification is
  not stateful admission; consume-once remains a durable runtime CAS decision.
- `DurableControlBuilder` covers every M1 command/query variant and sorts only
  duplicate-free wait targets. It never selects a wait or resumes a Run in
  TypeScript.
- Claimed-Effect resolution and cancellation return complete typed receipts.
  Compare the original value/reason, historical binding, owner, and fences to
  the submitted command and require the returned reference to equal its
  boundary; never hash a reason or Effect result in TypeScript.
- Structural Effect intent identities are lowercase SHA-256 content IDs in
  release, reconciliation, resolution, and durable projection contracts. Never
  accept a generic non-empty identity at those boundaries.
- Virtual work query/control types keep logical work, attempt occurrence,
  binding, owner, epoch, and disposition separate. Builders require stable
  command IDs; transports never apply retry policy locally.
- Region migration DTOs preserve opaque cursors and coverage evidence.
  Migration Plans and requests retain their explicit immutable provider
  revision. Never parse cursor positions or synthesize coverage in the SDK.
- `DurableEngine` depends on the structural `EngineTransport` interface.
  `CliEngine` is the default implementation, not a high-level semantic
  authority; custom transports and Store-provider strings reach Engine ingress
  without SDK-side provider allowlists.
- Compaction and rehydration builders copy the Rust-issued command identity,
  exact archive binding/revision, and bounded work/occurrence/command selections.
  TypeScript exposes no scheduler/provider transport or normalized
  claim/compaction receipt mirror. Rust alone creates or verifies archive
  certificates and owns runtime/provider extensions.
- Scheduling builders sort/deduplicate capabilities and require explicit slot,
  work/lease fences, opaque Engine-issued Clock observation references, TTL,
  and recovery disposition. They never accept caller time, read `Date.now()`,
  manage workers, or infer retryability.
- Scheduling and resolution builders validate 512-scalar non-control
  identities, exact Artifact references, and closed resolution payloads before
  copying them. Recovery admits only retry/failed/cancelled; a Run weight is a
  positive u32, never an arbitrary safe integer.
- Use discriminated unions for IR and Engine protocol types.
- `FlowBuilder.component()` requires an explicit Plan-owned output Artifact
  kind and has no default. Ordinary JSON components use
  `cymule.component-output/1`; Resource producers use the exact
  `cymule.typed-json/sha256-...` kind derived from the Resource Handle contract,
  never its logical framework type key.
- Throw `EngineError` for both remote failures and local transport failure. Its
  `failure` field is authoritative; message text is display-only.
- Never copy Engine-process stderr into an `EngineFailure`; response-less
  process errors use bounded SDK-owned status text only.
- `EnginePluginTarget` contains only the exact process provider, a complete
  `EngineProcessConfig`, and an optional immutable revision. Never restore a
  `location` field, path-string overload, ambient environment, implicit working
  directory, or SDK-chosen process limits. Migration and shadow targets require
  the exact revision and no unrelated provider target.
- `EngineStoreTarget` is provider-neutral: `provider` and `location` are
  required strings and `domain` is optional. `directoryStore` and
  `sqliteStore` emit `cymule.directory-store/5` and
  `cymule.sqlite-store/6` as conveniences; the
  Engine, not the SDK, decides whether a provider generation is supported.
- `ArtifactRef.identity_version` is the literal `cymule.artifact/2`; never omit,
  infer, or locally hash a bare reference. Live publication evidence is a full
  `ArtifactRecord`; validate its exact bounded canonical Base64 bytes and recompute the artifact/2
  preimage on admission, but do not add an SDK record builder or sealer.
- Keep `invoke` as a closed discriminated variant and `definition()` as a pure
  candidate authoring operation; neither may resolve logical latest heads.
- Keep M4 operations as the closed `EvolutionCommand` discriminated union.
  `EvolutionControlBuilder` copies caller data but never executes adapters,
  counts evidence, or chooses promotion/rollback.
- Keep `LiveEvolutionCommand` as the complete discriminated union around
  definition publication, template registration, publish/relink, and
  template-scoped evolution. Do not emit sequential lower-level calls.
- Model occurrence selection and its response as closed typed lineage over the
  occurrence, selection, template, retained decision, Plan, and exact
  ExecutionBinding Artifact.
- Preserve the exact source Run, Plan, epoch, and explicit replacement input in
  migration and restart commands; never infer them from a local clock or cached
  Run. Durable derives the authenticated source witness from the same pinned
  StateRoot; the public safe-point shape is retired.
- `VirtualRegion.source_artifact` is required immutable provenance. Region
  migration types and validators retain it on every source and target.
- The public npm package name is `cymule`. Changes to exports, files, engine
  requirements, or minimum Node versions require a package dry-run and release
  workflow review.
- npm publication uses GitHub Actions trusted publishing with provenance. Never
  add a long-lived npm token to repository or organization secrets.
- Recursively validate durable Run views and live-evolution results before
  returning them; static types are not runtime admission.
- Durable wait, Effect-resolution, and cancellation successes use their nested
  current receipts. Validate the retained activation or normalized command,
  applied-target subset, requested-versus-actual Effect outcome, nullable result
  relationship, receipt identity shape, and cancellation boundary before return.
  Effect-resolution receipts do not duplicate Run world settlement, and the
  `effect_not_applied` Run boundary carries one exact content-addressed intent.
- Durable Run views preserve strict identity ordering, exact pending-wait set
  equality, and bidirectional component-occurrence/Attempt lifecycle closure.
  Execution boundaries require Rust-shaped digests and strictly ordered unique
  content IDs before a post-plugin success is exposed.
- Component occurrence, Attempt, Continuation Attempt, and transport request
  identities are content IDs. Per occurrence, Attempt ordinals are exactly
  `1..N`; only the last may be running or completed, earlier Attempts are
  superseded, a running Attempt matches the active Continuation claim, and a
  completed occurrence has exactly one final completed Attempt with the same
  outcome.
- Effect dispatches, component occurrences, and Effect-resolution commands all
  carry the exact lowercase SHA-256 occurrence-binding content identity.
- Bind every accepted success to its originating request before returning it.
  A malformed or wrong outer type or inner live-evolution result after a
  mutation begins is an unknown-world outcome requiring reconciliation; the
  same mismatch on a read-only request remains an invalid Engine response.
- Engine `/4` live execution returns an `EvolutionCommit` with observed
  revision, required-nullable committed revision, and one complete persistence
  receipt. Accept it only when the receipt's evolution identity and full
  semantic command equal the actual serialized request and the closed outcome
  and mutation set validate; partial result-field correlation and Engine `/3`
  are unsupported.
- Every Engine `/4` success envelope echoes the complete inner request. Compare
  that echo to the strict actual sent-wire snapshot before inspecting the typed
  payload, for every request variant. Failure envelopes never carry a request.
  This echo is the cross-language correlation authority for Rust-derived values
  such as Clock scope and cancellation reason; TypeScript never reimplements
  those derivations.
- Live `apply` carries only its exact nested semantic request. Reject the retired
  outer `safe_point`, nested safe-point, and caller-authored source Continuation
  shapes for every operation.
- Validate the complete live command before starting the Engine process.
  Registry definition wire closure is distinct from sealed-Plan admission:
  draft nested IDs and schemas accepted by the Rust registry must not be
  rejected as though the standalone definition were already a sealed Plan.
- Live-evolution wire validation closes every nested variant and its
  publication ordering, Plan-edge shape, migration/restart lineage, and rollout
  evidence relationships. Validate content-ID and digest shape in TypeScript,
  but leave authoritative content derivation and target Plan sealing to Rust.
- Before an `apply_patch` mutation, the CLI transport seals the exact target
  candidate through the same Rust Engine and binds success to that Plan ID.
  Template registration success must resolve exactly the template reference
  key set; the SDK never derives either authority locally.
- Before `start_run`, the same Rust Engine seals the exact candidate so a
  completed boundary can be bound to the authoritative Plan ID without a
  TypeScript content-identity implementation.
- Request encoding accepts only plain JSON data without accessors, `toJSON`,
  symbols, sparse or extended arrays, proxies, or unpaired surrogates. Response
  parsing retains enough numeric evidence to reject unsafe mathematical
  integers, then normalizes safe decimal/exponent integer tokens before typed
  validation and exposes ordinary JavaScript numbers.
- Every parsed integer-valued JSON number, including decimal and exponent
  lexemes, must remain inside the shared exact range. A child-process output
  overflow or post-spawn I/O failure is response loss, never a start failure;
  mutating requests therefore require reconciliation.
- Reject a mathematically fractional numeric lexeme whenever JavaScript Number
  conversion would underflow or round it to an integer; it must never compare
  equal to an integer request echo.
- CLI Engine methods are asynchronous. The transport owns the live PID and an
  isolated Unix process group, applies a finite default deadline, bounds stdout
  and stderr as bytes, and performs fatal UTF-8 decoding. Timeout or an
  in-flight `AbortSignal` latches termination, sends one group `SIGKILL`, and
  rejects only after direct-child close has reaped that child and every inherited
  transport pipe has reached EOF. A zombie descendant is already unable to run
  external effects and cannot be reaped by the SDK; do not use `kill(pid, 0)` as
  its liveness authority or resend a group signal after PID/PGID reuse becomes
  possible. Natural close that wins the state transition remains authoritative.
- A completed execution result carries an exact lowercase Plan content ID, a
  raw 64-character lowercase projection digest, strictly ordered lowercase
  effect content IDs, and one closed
  `pre:<safe-epoch>:sha256:<lowercase-digest>` token.
- Every field that semantically carries the Core/Durable Run identity uses the
  1..=512 Unicode-scalar contract and rejects C0/C1 controls. Count code points,
  not UTF-16 code units or UTF-8 bytes. Internal request identities derived
  from a caller Run ID use a fixed-size digest; never append the unbounded
  caller value to a prefix.
- A Continuation execution claim carries the Rust-issued lowercase SHA-256
  `cymule.continuation/1` content identity. Never reconstruct its
  `continuation_id` by prefixing the Run ID.
- M4 identities use 1..=256 non-control Unicode scalars, independently of the
  512-scalar Run identity. JSON Schema `maxLength` on Engine failure text,
  contract, issue, and path fields is also a Unicode-scalar limit, never a
  UTF-8 byte limit.
- Every Resource manifest response recomputes the `cymule.resource-manifest/3`
  descriptor ID from media type, canonical byte size, entry count, and Merkle
  root. The empty case uses the frozen empty Merkle root and the same descriptor
  derivation; a raw empty-bytes SHA is not manifest authority.
- Enforce the frozen Engine failure category-to-retry matrix on remote and
  locally synthesized failures. Pre-spawn cancellation is `cancelled/never`, a
  read timeout is `timed_out/retry_same_request`, and interruption after a
  mutating request begins is `unknown_world_outcome/reconcile`. A Rust-owned
  persisted-attempt timeout may instead be `timed_out/refresh_and_retry`.
- Environment-dependent conformance tests must report an explicit skip when
  their fixtures are absent. The configured release suite must provide every
  fixture and fail closed; a bare `return` must never appear as a passing test.
