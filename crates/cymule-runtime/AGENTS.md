# Runtime Guidance

- The runtime interprets frozen IR and adapts abstract operations to substrate
  interfaces. It must not redefine core identities or transition laws.
- `PluginHost` is the authority boundary for external execution. A manifest is a
  capability advertisement, not authorization.
- `PlanContracts` compiles every submitted schema as Draft 2020-12 without an
  external resolver. Keep this executable contract layer outside `cymule-core`,
  preserve the submitted schema bytes in Plan identity, and return typed
  `ContractViolation` values with masked instance content.
- Validate definition, component, effect, typed-wait, and terminal-result values
  at their exact boundary. Input validation must finish before plugin dispatch;
  output validation must finish before recording or binding the response.
- Do not add a runtime Plan sealer. Core sealing owns schema admission; runtime
  compilation only materializes validators for boundary values.
- Embedded waits return a typed site, wait, and optional-result-binding boundary
  without synthesizing a Continuation or resume token.
- Every plugin call pins an immutable occurrence binding before execution.
- Reusable definition calls create a distinct deterministic invocation identity,
  receive only their explicit input, and return only their declared result.
  They do not inherit caller locals or imply a new transactional scope.
- Mutating effects remain staged until scope commit unless an explicit release
  policy says otherwise. Dispatch ambiguity must be recorded as `unknown` before
  reconciliation.
- `PrepareEffect` may be repeated after response loss with the same structural
  intent ID and input. A plugin must make that prepare idempotent and must not
  interpret the repeat as a new world mutation.
- Keep the reference runtime synchronous and dependency-light. Production async,
  durable, and distributed realizations should be separate adapters.
- Runtime binding admits exact, versioned `ServiceKey` contracts through a
  deterministic provider-before-consumer graph. The content-addressed binding
  descriptor serializes only normalized provider input: implementation
  identities, service dependencies, provider properties, schema digests, and
  an irreversible fingerprint of canonical non-secret configuration plus
  secret/version reference identity. It never serializes topology, derived
  binding tables, provider configuration, endpoints, credentials, or secrets.
- A service binding is not a capability advertisement, policy admission, or
  authority grant. `PluginManifest` remains capability advertisement only.
  Match existing Plan requirement maps exactly against provider properties,
  then perform policy and authority admission independently. Pin only the final
  opaque binding-context identity in core semantics.
- `AdmittedPluginRouter` dispatches each operation by the provider ID sealed in
  `ExecutionBinding`. Provider manifests may advertise extra capability, but
  extra advertisements never become routing or execution authority.
- `cymule.execution-binding/2` has no global plugin implementation identity.
  Admission consumes manifests keyed by provider ID, pins each operation's
  provider implementation and operation revision independently, and the router
  rechecks every selected provider manifest before dispatch.
- Runtime composition has one small closed envelope: at most 64 providers, 256
  provided/required services or properties per provider, 4096 services across
  the graph, and 128 selected operations per kind. Tokens and property values
  use shared scalar/control bounds, while the complete descriptor and
  `ExecutionBinding` must fit Core's 8 MiB Artifact bound. Bounded Serde
  visitors reject max-plus-one collections before allocating another typed
  element; persisted bindings decode only through `ExecutionBinding::decode`.
- Runtime binding owns no DI container, factory lifecycle, finalizer, or live
  provider object. Ordinary Rust ownership and provider adapters manage
  process-local resources; durable cleanup remains an explicit effect
  obligation and reconciliation concern.
- `cymule.execution-binding/2` is the executable binding authority. Persist its
  canonical bytes as an immutable Artifact; Run, Continuation, and Attempt pin
  that Artifact ID, while component and Effect occurrence bindings derive from
  it plus the exact selected operation. A live manifest may verify the pin but
  never replace it.
- `EngineFailure` is the cross-language failure projection. Keep its categories,
  phases, bounded issue tree, contract detail, and retry dispositions closed and
  versioned. Validate every deserialized failure; do not infer retry safety from
  transport loss or classify an undeclared plugin error as an expected failure.
  Owner cancellation is the typed `RuntimeError::Cancelled` boundary and maps to
  `cancelled/never`; once an Effect dispatch process has started, cancellation
  remains `UnknownWorld/reconcile` because the world outcome is not known.
- Core's typed `PagedScopeRequired` remains `paged_scope_required` with
  `AdmissionDenied/CorrectAndRetry`. It requires a different admissible command
  with paging authority; it never permits automatic same-request retry or
  becomes an unknown-world outcome.
- `CollectionProviderFailure` retains the canonical collection provider's
  Validation, Integrity, revision conflict, immutable-history conflict, or
  Substrate meaning. Preserve provider-owned codes/messages; only revision
  conflict permits refresh and only Substrate permits identical-request retry.
  Never classify these failures from Display text or flatten them into a
  generic Core defect.
- `cymule.engine/4` is the sole Engine transport generation. Every success
  requires the complete strictly decoded inner `EngineRequest` and its closed
  response; every failure contains only the structured error because it may
  originate before request decoding. Missing request echo, mismatched echo, and
  the older v4 success shape fail closed without fallback. Stateful M4 transport
  is exactly `execute_live_evolution(target, evolution_id, command)` and returns
  one `EvolutionCommit`; the host constructs `EvolutionPersistenceCommand` and
  delegates to the closed Durable Evolution control. There is no caller journal
  ID, journal-shaped receipt, or parallel controller path.
- An Engine M4 target carries a required bounded map of at most one target Plan
  SHA-256 identity to one revision-pinned ordinary `EnginePluginTarget`.
  Fresh migration may resolve only the command's exact target Plan from that
  fixed registry; every non-migration command and retained migration replay
  must not capture, infer, lazily default, or carry another execution binding.
- Official Store dispatch uses only the terminal physical provider generations:
  `cymule.directory-store/5` and `cymule.sqlite-store/6`. Earlier selectors are
  rejected without a compatibility alias or reader.
- Strictly parsed Engine values must retain raw object-member presence through
  typed admission. Recursively reject an explicit `null` member when typed
  reserialization omits that member; do not canonicalize unrelated legal JSON
  representations or reject required nullable members that remain present.
- Strict JSON treats integer as a mathematical type: normalize every safe
  integral token such as `1.0` or `1e0` to an integer before typed decoding and
  success echo construction. Preserve finite fractional values for untyped JSON
  fields, and reject every mathematically integral value outside the shared
  safe-integer range. Delegate this decoding to the Core duplicate-rejecting
  canonical-number authority; runtime must not maintain a second numeric parser.
- The runtime defines `cymule.plugin/3` messages but no process launcher.
- Runtime also owns the fixed `cymule.evolution-plugin/3` process generation and
  16 MiB raw-message limit used by Engine target admission. An Evolution
  process target must carry that exact limit; per-request configuration cannot
  narrow or widen the protocol domain.
- `EnginePluginTarget` carries one closed `EngineProcessConfig`; the executable,
  ordered arguments, explicit environment, required-nullable working directory,
  runtime closure, deadline, and limits are all wire-required. There is no
  separate process location or host-default constructor. A null working
  directory means the provider explicitly selected no captured tree and never
  authorizes inheriting the caller's ambient cwd.
- Every runtime-closure value is a lowercase SHA-256 identity of a frozen
  provider-owned closure descriptor. An OS/architecture label, mutable version
  name, or arbitrary nonempty string is not execution-binding authority.
- Runtime owns the shared exact 4096-entry ceilings for process arguments,
  environment, and runtime closure. Engine admission and every executor use
  those constants; adapters do not maintain a second count policy.
- `cymule.plugin/3` Effect providers own one atomic per-intent settlement
  ledger. `DispatchEffect` first enters `Dispatching` in that ledger before
  world mutation; `ReconcileEffect` seeing `Dispatching` returns
  `StillUnknown`. Reconciliation may create a `NotApplied` tombstone only while
  no dispatch has started, and every later dispatch is then a permanent no-op.
  Request/response `cymule.effect-provider-attempt/1` equality is mandatory;
  framework-local claim checks are not world authority.
  `ExpectedFailure` is an explicit component-call application outcome; using it
  for Effect preparation is a protocol defect. `Defect`, process termination,
  and malformed responses remain defects. Once Effect dispatch
  starts, every missing or unusable outcome first records `Unknown` and projects
  reconciliation, never same-request retry.
- Every plugin/3 request, response, and manifest has the same fixed semantic
  byte ceiling as a Core Artifact. An ordinary process `PluginHost` must carry
  that exact limit; both narrower and wider configurations fail before spawn.
  Runtime rejects oversized in-process products before contract validation or
  Artifact construction and applies the same limit to process stdin/output.
- `ExpectedFailure.message` and `Defect.message` share the exact 1..=2000
  Unicode-scalar domain. `PluginManifest.components` and `effects` are required
  wire members even when empty; Serde must not synthesize either map.
- Missing, invalid-variant, or schema-invalid dispatch and reconciliation
  outputs retain the original intent as `Unknown`. Embedded execution never
  auto-releases an `Explicit` effect.
- A queryable reconciliation result settles only for `ResolvedApplied` or
  `ResolvedNotApplied`. `StillUnknown` returns the typed reconciliation
  boundary without binding any provisional value; a provider-authored
  `GovernanceRequired` is a protocol defect because policy, not the provider,
  owns that escalation.
- Verified Plans already guarantee that only observational eager Effects bind.
  Do not restore a later execution-time guard or deferred pending-result bind.
- Component Call is unclassified computation, not Effect recovery authority. A
  lost Call response may be invoked again; provider observations that need
  ambiguity handling use an observational eager Effect.
- Historical Effect dispatch and reconciliation resolve the ExecutionBinding
  Artifact pinned by the occurrence. The runtime owner supplies its current
  binding, and one shared admission requires the complete selected operation
  binding plus the selected provider's resolved transitive dependency closure
  to equal the historical pin before checking the live manifest. A manifest
  does not prove executable bytes or configuration identity. Every current and
  historical component/effect invocation performs this admission; unrelated
  providers outside the selected closure are not dependencies. Routers never
  substitute the current binding for a missing or drifted origin provider.
- Admission returns a framework-owned token with private construction; the
  post-claim invocation seam consumes that token. Never expose an unchecked
  public binding/router invocation path. `PluginHost` is only the raw
  invoke/Describe/whole-binding seam. `BoundPluginHost` is sealed: ordinary raw
  hosts receive its single framework blanket implementation, while
  `AdmittedPluginRouter` has one framework-private routed implementation and
  deliberately does not implement raw `PluginHost`. Current/historical
  selected-operation equivalence is always checked by the public sealed method
  before either internal path may inspect a manifest or construct a token;
  external adapters cannot override that sequence or sign their own token.
- `CompositionError` is the stable closed owner of runtime-graph and immutable
  binding-admission failures. `RuntimeError::Composition` and unavailable
  operation tokens retain that typed variant through Engine and Durable
  projection; `Display` text is never classification or retry authority.
- Whole-runtime provider admission returns an `ExecutionBindingAdmission` that
  owns the exact host and binding. Direct providers derive the binding from the
  single manifest fetched by admission; `ResumableRuntime` consumes this token
  after Store open and performs no second Describe or provider I/O.
- Contract failure projection sorts and deduplicates issues, retains at most 99
  concrete issues plus one omission summary, and stops validator traversal at
  that fixed source budget. `ContractTarget`, every issue field, and the whole
  `ContractViolation` canonical envelope validate their own bounds before
  Engine or Durable projection; those consumers never re-truncate the issue
  set. An invalid internal projection still emits one valid `cymule.engine/4`
  failure envelope instead of escaping as stderr-only transport failure.
- Embedded completion, wait, explicit release, and reconciliation are closed
  success-side `ExecutionOutcome` variants. Never flatten release or
  reconciliation-required state into an Engine failure string.
