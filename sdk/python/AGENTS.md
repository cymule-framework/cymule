# Python SDK Guidance

- Support maintained CPython versions with no runtime dependencies.
- Use typed dictionaries or dataclasses for public contracts and preserve exact
  wire names.
- Subprocess stderr is diagnostic-only and must never enter an `EngineFailure`
  message. Response-less errors use bounded SDK-owned text without exposing
  environment variables, credentials, or child output.
- `EnginePluginTarget` contains only the exact process provider, a complete
  `EngineProcessConfig`, and an optional immutable revision. Never restore a
  `location` field, path-string overload, ambient environment, implicit working
  directory, or SDK-chosen process limits. Migration and shadow targets require
  the exact revision and no unrelated provider target.
- Store targets retain provider-neutral `provider` and `location` strings plus
  an optional `domain`. Official constructors emit `cymule.directory-store/5`
  and `cymule.sqlite-store/6`, while Engine ingress alone decides provider
  support.
- Raise `EngineError` with the exact structured `failure` object for all Engine
  responses. A timed-out mutating Run is an unknown-world outcome, not a safe
  generic retry.
- The Rust engine remains the only authoritative sealer and reducer.
- Control command builders use the implemented Engine/DurableEngine methods.
  Do not restore the caller-zero generic control-submit protocols or aliases.
- `FlowBuilder.component()` requires an explicit Plan-owned output Artifact
  kind and has no default. Ordinary JSON components use
  `cymule.component-output/1`; Resource producers use the exact
  `cymule.typed-json/sha256-...` kind derived from the Resource Handle contract,
  never its logical framework type key.
- `ArtifactRef` is a closed `TypedDict` requiring `identity_version =
  cymule.artifact/2`; Python never derives an identity from a bare reference.
  `LivePublicationCommand.evidence` is a complete `ArtifactRecord`, so strict
  admission decodes bounded canonical padded Base64 and recomputes its
  artifact/2 preimage from the exact bytes; do not add
  a Python builder or alternate sealing authority.
- `definition()` and `invoke()` emit exact `cymule.ir/3` wire records. Python
  must not resolve or cache logical subflow heads.
- Resource builders preserve exact wire names and send candidates to the Rust
  engine. Do not add a Python Resource ID implementation or accept credentials
  in public URL helpers.
- Resource `/4` builders and response validation share the exact lowercase
  ASCII type/subtype token grammar and exact-reject `/3`; parameters are not
  normalized or stripped.
- Wait activation builders sort and deduplicate exact targets while preserving
  delivery, source, and Artifact identities. Rust Engine verification is not a
  substitute for durable CAS admission against pending waits.
- `DurableControlBuilder` emits the complete M1 command/query union and copies
  caller JSON defensively; no Python code reduces durable state.
- Virtual work query/control types preserve command, occurrence, binding,
  owner, epoch, and disposition fields. SDK transports do not classify errors
  or decide retry policy.
- Region migration types preserve opaque cursor maps, pinned adapter binding,
  coverage evidence, and every region's required `source_artifact` provenance.
  Python clients never split cursor strings locally.
- Archive authoring preserves the Rust-issued command identity, exact archive
  binding/revision, causal cut, bounded work/occurrence/command sets, and
  certificate. Python exposes no scheduler/provider transport or normalized
  claim/compaction receipt mirror; Rust alone derives identities and admits
  archive work. Region migration retains an explicit provider revision.
- Scheduling protocols keep capacity slots, opaque issued Clock references,
  work/lease fences, Run weight, and recovery disposition explicit. Do not
  accept caller-supplied logical time, read a local clock, or add a Python
  worker registry/reducer.
- Scheduling and resolution builders validate 512-scalar non-control
  identities and exact Artifact references. Resolution payloads are closed,
  recovery admits only retry/failed/cancelled, and Run weight remains a
  positive u32; none of these shape checks selects work or evaluates expiry.
- Evolution builders preserve the closed M4 operation tag, exact Plan and
  Artifact identities, adapter requests, observations, and gates. Python never
  resolves latest revisions or evaluates a gate locally.
- Unified live-evolution builders preserve the parent template, nested command,
  publication evidence, and exact semantic source intent. They never implement
  reverse-dependency relinking or split the durable command.
- Speak only `cymule.engine/5`; Engine `/4` is unsupported. Stateful
  live-evolution success returns the complete closed receipt containing the
  exact journal, serialized command, and recursively validated outcome.
- Every Engine `/5` success envelope carries the complete raw inner request
  echo. Compare it with the defensive JSON snapshot actually sent before
  reading or validating any success payload; member presence is exact, so an
  omitted optional field never equals an explicit null. Failure envelopes carry
  no request echo. Do not replace this universal correlation with local hashes
  or operation-specific derivation.
- Custom transports implement only `exchange(request)` and return the complete
  `{request, response}` success. A bare Clock, Durable, Seal, or Evolution
  payload is not accepted by the high-level facade. After `exchange` returns,
  shape, echo, payload, request binding, and unboxing validation catch every
  ordinary `Exception` and turn it into request-aware response loss; never
  catch `BaseException` at this boundary.
- Strict JSON admits at most 128 nesting levels, maps parser `RecursionError` to
  a structured fixed-depth rejection, and retains exact `Decimal` evidence for
  non-integral tokens until echo and typed admission finish. Number tokens are
  capped at 256 bytes and exponents at six digits before `Decimal` construction.
- Verification successes must return the exact wait activation or control
  command submitted on the wire. A `sealed_resource` success must recursively
  validate the complete Handle and equal the submitted Candidate after removing
  only the Rust-owned `resource_id`. Omitted Resource `annotations` is distinct
  from an explicit empty map: builders omit an empty map and admission rejects
  `{}` rather than normalizing it.
- Optional Engine failure and issue members are omission-only and reject
  explicit `null`. Failure codes use the ASCII-only
  `^[a-z][a-z0-9_]{0,199}$` contract, and category/retry pairs must match the
  closed Engine recovery matrix. When `issues` is present it contains 1..=100
  entries; omit it instead of sending an empty list. An invalid envelope
  received after a mutating request begins is an unknown world outcome requiring
  reconciliation.
- Interruption classification carries an explicit process-start boundary.
  Cancellation observed before `Popen` succeeds is `cancelled/never` even for a
  mutating request. After start, mutating cancellation or timeout is
  `unknown_world_outcome/reconcile`; a read-only timeout remains
  `timed_out/retry_same_request`. An authoritative remote durable timeout may
  instead return `timed_out/refresh_and_retry`; preserve that closed failure
  rather than reclassifying it as local response loss.
- `CliEngine` accepts only the SDK-owned one-way `EngineCancellation`, never an
  arbitrary cancellation callback. `cancel()` and the sole `Popen` factory use
  the token's same lock: cancellation linearized first never calls `Popen`,
  while launch linearized first makes later cancellation post-start. Admitted
  success and valid remote failure also race cancellation under that lock per
  invocation; completing one call never marks a shared token complete for its
  peers. Always kill and reap the direct Engine Child and close every local pipe
  in `finally`.
- Engine failure messages/contracts, issue codes/messages, and JSON Pointer
  paths apply their Schema `maxLength` limits in Unicode scalar values while
  rejecting surrogate code points. Do not reuse byte-counted identity/token
  helpers for these fields.
- Core and Durable Run identities use 1..=512 Unicode scalar values and reject
  control characters at builders and response boundaries, including M4
  migration/restart projections that carry those same Run IDs. SDK-owned query
  identities use a bounded digest preimage rather than appending an unbounded
  caller Run ID; internal Continuation identities remain Rust-issued content
  IDs.
- Ordinary M4 command, template, decision, observation, occurrence, and related
  identities use their separate 1..=256 Unicode scalar domain and reject
  control and surrogate code points. Do not apply either the Run limit or a
  UTF-8 byte limit to that domain.
- Bind every accepted live-evolution receipt to its actual serialized request
  before returning it. Wrong journal, any changed command field, a malformed
  receipt, or an operation/outcome mismatch after mutation begins is an
  unknown-world outcome requiring reconciliation. Never infer request identity
  from a subset of outcome fields.
- Durable projections enforce the Rust wire closure: wait sets equal pending
  waits; set-like IDs are strictly sorted and unique; terminal non-winner wait
  activation has no ready Run; component occurrences and provider Attempts form
  a closed matching lifecycle; execution digests and effect content IDs retain
  their exact shapes.
- Effect dispatches, component occurrences, and Effect-resolution commands all
  require an exact lowercase SHA-256 occurrence-binding content identity.
- One shared Effect-result validator owns summary, exact Effect dispatch, and
  Effect-resolution receipt admission. Applied requires a non-null Artifact of
  exact kind `cymule.effect-result/1`, including when the represented JSON value
  is null; every other state requires a null result.
- Durable wait, Effect-resolution, and cancellation successes use their nested
  current receipts. Validate the retained activation or normalized command,
  applied-target subset, requested-versus-actual Effect outcome, nullable result
  relationship, receipt identity shape, and cancellation boundary before return.
  Effect-resolution receipts omit Run world settlement; validate
  `effect_not_applied` as a distinct closed boundary with one exact
  content-addressed intent.
- Before applying a Plan patch, use the same Rust CLI transport to seal the
  exact target Candidate without mutation. The committed edge must name that
  Rust-issued target Plan ID; Python never hashes or derives it locally.
- Template-registration success resolves exactly the unique logical-reference
  set declared by the submitted template; missing or extra revision keys are
  an invalid post-mutation response requiring reconciliation.
- Live `apply` carries only its exact nested semantic request. Reject retired
  outer/nested safe-point and caller-authored source-Continuation shapes for
  migration, restart, and every other operation.
- Live-evolution wire validation closes every nested variant and its
  publication ordering, Plan-edge shape, migration/restart lineage, and rollout
  evidence relationships. Validate content-ID and digest shape in Python, but
  leave authoritative content derivation and target Plan sealing to Rust.
- Published reusable Definitions use their closed Rust Serde wire plus registry
  name/reference admission, not full sealed-Plan admission. Nested operation,
  target, binding, and site IDs may therefore be empty until a parent template
  attempts to link and seal them as a Plan.
- Occurrence selection carries distinct occurrence and selection identities and
  an exact ExecutionBinding Artifact; Python validates but never reconstructs
  the returned typed pin.
- Migration/restart builders preserve the exact source Run, Plan, and epoch,
  distinct replacement Run, explicit input, and evidence without local
  interpretation. Durable alone derives the authenticated source witness.
- Reject non-finite or non-positive deadlines before `Popen`. Poll the
  SDK-owned cancellation token in one selector-driven nonblocking
  stdin/stdout/stderr loop; timeout,
  cancellation, or overflow kills the direct Child if still live and closes
  every local descriptor without waiting on inherited-pipe threads.
- Stdout retains the 128 MiB plus 32-byte framing response-envelope bound plus one byte; stderr
  retains the independent 1 MiB diagnostic bound plus one byte. The SDK owns
  only its direct Child handle and local pipes; the Engine must close internal
  providers before exit. Non-POSIX process transport fails before spawn.
- Durable cancellation and claimed-effect reconciliation expose only their
  closed Rust receipts. Compare the original reason/value and every authority
  field with the nested command; validate but never derive the boundary reason
  or result Artifact identities.
