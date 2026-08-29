# Rust SDK Guidance

- The public Cargo package and library name is `cymule`; the repository path
  remains `crates/cymule-sdk` to make its facade ownership explicit.
- The SDK is an authoring and client facade. It does not own semantic reduction.
- `Engine`, its `CliEngine` implementation, and `DurableEngine` are the actual
  client surface. Do not restore unimplemented profile-control traits beside
  these interfaces. Virtual authoring DTOs remain typed values consumed by the
  owning Rust Durable controllers, not an invented SDK transport.
- Keep the Core, Durable, Virtual, Evolution, and Runtime facade contracts for
  user-facing candidates, commands, receipts, and query views
  dependency-closed. A consumer must not need a second profile crate merely to
  name one of their public field types, while reducer, Store, controller, and
  adapter implementation types remain in their owning crates.
- Builders must emit the same `cymule.ir/3` objects as other language SDKs.
- `FlowBuilder::component` requires an explicit output Artifact kind. Ordinary
  component callers use Core's sole `COMPONENT_OUTPUT_ARTIFACT_KIND` constant;
  Resource producers use the exact typed kind derived from their sealed
  Resource Handle Artifact contract. Never add a default or use the logical
  Resource framework type key as the persisted output kind.
- Rust builder definitions and invocations must match the TypeScript, Python,
  and Go wire shape; they never resolve logical registry heads locally.
- Keep convenient APIs lossless: effect risk, occurrence identity, scopes, and
  version information must remain explicit in the emitted plan.
- CLI transport is one Engine implementation, not the semantic definition.
- Every process-backed call carries a complete `EnginePluginTarget` containing
  the required `EngineProcessConfig`. This applies equally to Embedded `run`,
  durable execution, and evolution plugins. There is no path-only overload:
  arguments, ambient-cleared environment, working directory, runtime closure,
  timeout, and size limits must enter the request echo and execution binding.
  Provider ledger locators are explicit environment entries, never ambient or
  executable-relative fallbacks.
- Ordinary plugin targets use the exact 8 MiB plugin message limit. Migration
  and shadow targets use the exact 16 MiB Evolution message limit; both reject
  narrower and wider values before process spawn. Runtime-closure values are
  lowercase SHA-256 content identities rather than compatibility labels.
- CLI-backed clients accept only `cymule.engine/5`; a v4 envelope is malformed
  transport and must not enter a compatibility decoder.
- After typed response-envelope decoding and before failure, echo, or payload
  admission, compare the strict raw response with typed reserialization and
  reject every explicit member erased by omission-only/defaulted serialization.
  Classify this as an invalid transport response for reads and
  `unknown_world_outcome/reconcile` for mutations; required nullable members
  remain admitted.
- A typed v5 failure is authoritative only after `EngineFailure::verify`
  succeeds. Preserve a valid remote failure exactly. A semantically invalid
  failure is an invalid transport response for a read and
  `unknown_world_outcome/reconcile` after a mutation; never return the
  validator's own `invalid_engine_failure` as the operation result.
- Serialize one request envelope, strict-decode the exact emitted bytes, and
  require every success to echo that retained inner `EngineRequest` exactly
  before validating its response. Never rebuild correlation from a Rust-derived
  Plan/Resource/Clock/rollout identity or compare a pre-serialization host
  value. Preserve raw JSON member presence: omitted and explicit `null` do not
  match, even if typed decoding maps both to `None`. Failure has no request echo
  because request decoding may not have completed; a predecessor success without
  the echo fails closed. Classify invalid echo from a mutating request as
  `unknown_world_outcome/reconcile`; a non-mutating request receives an invalid
  response without inferred replay safety.
- Clock issuance returns the typed `ClockObservationResult`. The Durable facade
  verifies its exact Run and source generation before exposing the nested
  observation; SDK code never recomputes the opaque Clock scope.
- Serialization, strict-JSON admission, and typed round-trip failure before
  process spawn are local `validation/correct_and_retry`, never transport loss.
  Mathematical integer tokens normalize before typed admission; lexical
  decimal or exponent notation is not a second integer contract.
- Every `verify_*` success must return the exact request-owned activation or
  command object for that operation. A self-validating payload with the right
  response tag is not sufficient; compare its typed value with the request
  decoded from the retained submitted wire. The earlier raw-member-presence
  admission remains the sole authority for omission-versus-null fidelity.
- The Rust `Engine` surface returns `EngineFailure`, not `CoreError`. Preserve
  remote category, phase, code, contract issues, and retry disposition exactly;
  synthesize `transport_failure` only when no valid Engine envelope was received.
- Resource builders emit semantic-only `cymule.resource/3` candidates. Only the Rust Engine
  seals Resource IDs; the SDK must not duplicate the resource canonicalizer.
  A seal success self-verifies the returned handle and requires its complete
  semantic descriptor to equal the exact echoed candidate. Plan sealing uses
  the same rule. Durable Start first seals through that same Engine authority
  and binds a completed boundary to the returned Plan ID; the SDK never
  rederives either identity locally.
- Re-export the closed `ArtifactRef` with its required `cymule.artifact/2`
  identity version. Typed contract selection remains pinned by the reference,
  never by a mutable SDK alias.
- Re-export Core's complete `ArtifactRecord` for live publication evidence.
  Verification consumes its exact bytes and artifact/2 identity; this facade
  must not introduce a second record constructor or content-ID implementation.
- Wait activation DTOs preserve stable delivery, source, target, and Artifact
  identities. CLI verification covers the closed record; only a durable runtime
  CAS can admit it against pending waits and enforce consume-once semantics.
- `Engine::execute_durable` transports the complete
  `cymule.durable-control/4` union.
  SDKs may build start, resume, explicit-takeover, cancel, activation,
  explicit-release, claimed-Effect resolution, and the seven bounded query
  commands, but must not
  reduce Continuations or outbox state locally. Execution-bearing commands preserve the driver, issued
  Clock reference, TTL, and expected takeover fence; the SDK never constructs a
  Clock receipt or infers expiry.
- Query APIs expose only revision/root-pinned Run-index and child pages,
  bounded Run current, and exact typed leaf reads. They pass the caller's
  explicit revision, cursor, item, and canonical-byte bounds through unchanged;
  never restore the removed full-Run/domain mirrors or a query ID shim.
- Applied Effect summaries require their result Artifact even when its exact
  canonical JSON value is null; all other summary states retain null. The shared
  summary fixture exercises this rule through the real CLI response ingress.
- Cancellation and Effect-resolution successes return complete typed receipts.
  Their nested `command` is the exact accepted request semantics and binds the
  cancellation reason or every resolution identity, binding, owner, and fence.
  Effect `actual_resolution`/`actual_value` are the provider's independent
  linearized truth and may differ from the requested decision/value. The SDK
  never recomputes the Rust-owned receipt, reason, or result Artifact ID.
- `DurableEngine` obtains each Clock reference only through the Engine's
  `observe_clock` operation, and every success must bind the returned Clock
  scope to the exact requested Run in addition to source and generation.
  Queries, wait activation, and cancellation are store-only. Effect resolution
  attaches the exact historical executor but no Clock; only
  start/resume/takeover/release consume execution Clock authority. Missing
  executor or Clock configuration is a local validation failure with
  `correct_and_retry` before any custom or CLI transport call; it never becomes
  an unclassified Engine failure or ambient provider selection.
- `DurableEngine` validates every complete durable command before invoking a
  custom or CLI transport. Caller-invalid identities are local `validation`
  with `correct_and_retry`, never transport failure or unknown world outcome.
  SDK-owned query identities retain per-Run trace correlation through a
  fixed-length digest, independently of caller Run length. They use a distinct
  trace-only namespace and never duplicate the durable authority's
  content-addressed Continuation identity.
- Virtual command DTOs preserve stable command and occurrence IDs, typed
  ExecutionBinding Artifact identity, owner, work epoch, lease epoch, and
  opaque Clock reference; SDKs never author logical
  time or implement retry/failure reduction locally.
- Region migration clients preserve opaque cursors, exact source preconditions,
  pinned migration binding, coverage evidence, and every region's required
  `source_artifact` provenance. SDKs never split cursor strings or infer
  partition coverage.
- Re-export the provider-neutral archive and typed compaction/rehydration
  commands and receipts without adding a second validator. The facade includes the exact
  virtual command/occurrence proofs, typed archived command, cumulative
  work/command index proofs, and work-resolution receipt. Core
  Machine archive objects and Store implementation contracts remain owned by
  `cymule-core` and `cymule-durable`; do not leak those reducer internals through
  the top-level SDK. The public Durable Rust controllers remain admission
  authority; these re-exported values do not add an SDK transport.
- Preserve the complete virtual compaction certificate, including its parent
  work-index root, ordered-update digest, resulting work-index root, command
  count, and required-nullable command root. An omitted command-root member is
  not equivalent to an explicit null.
- Virtual claim, renewal, expired recovery, and future Run-weight authoring
  preserves both work and lease fences plus the opaque Clock observation.
  `VirtualClaimOutcome` retains the complete normalized persistence receipt;
  only `Claimed` carries the exact verified Plan. Never turn the SDK into a
  worker loop or scheduler.
- That existing claim/compaction receipt surface re-exports every public nested
  command, evidence, mutation, lifecycle, archive-binding, Evolution selection,
  and Resource-retention DTO needed to name its fields. The facade test imports
  these types only from `cymule`; no reducer, Store, controller, or provider
  implementation is added by this dependency closure.
- Virtual fixtures represent current Rust-produced commands only. Compaction
  retains the explicit work, occurrence, and archived-command selections plus
  its immutable archive generation, and its command ID is generated by the
  Rust constructor. Retired snapshots, journal bases, and old coupled-journal
  claim receipts are rejection fixtures, never shape-only successful evidence.
- Evolution DTOs describe closed `cymule.evolution-control/5` commands.
  Re-export Rust M4 DTOs without adding client-side latest resolution,
  migration/shadow execution, evidence counting, or rollout decisions.
- Occurrence selection preserves the stable selection identity, occurrence
  identity, and exact ExecutionBinding Artifact; the Rust response returns the
  complete immutable pin.
- `Engine::execute_live_evolution` transports the unified registry/DAG/rollout/pin
  envelope. It preserves template identity and exact migration/restart source
  intent without splitting one command into lower-level registry and rollout
  calls. Durable derives the authenticated source witness from its pinned
  StateRoot; the public safe-point shape is retired.
- Stateful live evolution returns one `EvolutionCommit` with the observed
  revision, required-nullable committed revision, and exact persistence
  receipt. Engine clients first compare the outer echoed request with the
  actual sent wire, then bind the receipt's evolution authority and complete
  semantic command to that request and verify its typed outcome and mutation
  set. They never infer durable correlation from selected outcome fields.
  Positive Rust transport fixtures obtain that complete commit through public
  Durable control; they never synthesize a StateRoot revision or assemble a
  detached reducer view as a substitute for persisted authority.
- `EngineEvolutionTarget` always carries both required-nullable provider
  members. A non-null migration adapter or shadow driver binds its semantic
  identity and content-addressed revision to an exact process target whose
  revision is identical; omitted required wire members and generic unbound
  plugin targets are not admitted.
- CLI transport preflight admits a completely provider-free migration or
  shadow target as an exact-replay candidate. It still rejects partial,
  mismatched, extra, or ambient provider authority locally; only the CLI's
  read-only retained-receipt check may distinguish replay from a fresh command
  and require the latter's complete provider target.
- Validate a complete live-evolution command before invoking either the CLI or
  a custom mutation transport. Local preflight rejection is ordinary
  `validation` with `correct_and_retry`, never an unknown world outcome.
- `DurableEngine` is generic over `Engine`, stores provider-neutral targets,
  omits executors for queries, wait activation, and cancellation, and forwards
  CLI timeout and cancellation. Effect resolution carries only its executor;
  execution commands carry executor plus Clock. Live evolution includes only
  the migration or shadow target required by the selected command, never both.
- CLI cancellation terminates the isolated Engine process group before reaping
  the direct child so provider descendants cannot retain transport pipes.
- Direct-child exit is not transport completion. The same absolute deadline
  remains active until bounded stdin, stdout, and stderr all close; at the
  deadline the SDK kills the group even when the direct child was already
  observed and reaped.
- Process-group conformance tests publish a ready marker and wait for an
  explicit release before the direct child exits. They must prove that exit
  occurred before triggering cancellation or timeout; a post-timeout PID-file
  read is not readiness evidence and creates a scheduler-dependent test race.
- Completed execution and durable boundaries require a lowercase Plan content
  ID, raw lowercase projection digest, strictly ordered lowercase effect
  content IDs, and the closed safe-range
  `pre:<epoch>:sha256:<lowercase-digest>` precondition token.
- Durable Run views recompute world settlement from the complete Effect set and
  reject `Completed` unless that aggregate is `Settled` and every Effect is in a
  settled state.
- All stdin/stdout/stderr work runs concurrently with the same absolute
  deadline. Nonzero exit, malformed output, or missing output after a mutating
  request becomes `unknown_world_outcome` with reconciliation disposition.
- A cancellation already signalled before process spawn is `cancelled/never`.
  After spawn, a read-only local timeout is `timed_out/retry_same_request` and a
  read-only cancellation remains `cancelled/never`; timeout or cancellation of
  a mutating request is `unknown_world_outcome/reconcile`. Local transport
  timeout never uses the server-side running-Attempt refresh disposition.
- Any malformed, mismatched, or incomplete live-evolution receipt after the
  mutation begins has the same unknown-world reconciliation classification.
- Cross-language Rust tests explicitly report an environment skip during an
  ordinary isolated `cargo test`. `scripts/verify-sdk.sh rust` sets
  `CYMULE_RUST_SDK_CONFORMANCE_REQUIRED=1`, making every missing binary or
  fixture environment variable a hard failure instead of a passing return.
