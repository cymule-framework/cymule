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
- Runtime binding owns no DI container, factory lifecycle, finalizer, or live
  provider object. Ordinary Rust ownership and provider adapters manage
  process-local resources; durable cleanup remains an explicit effect
  obligation and reconciliation concern.
- `cymule.execution-binding/1` is the executable binding authority. Persist its
  canonical bytes as an immutable Artifact; Run, Continuation, and Attempt pin
  that Artifact ID, while component and Effect occurrence bindings derive from
  it plus the exact selected operation. A live manifest may verify the pin but
  never replace it.
- `EngineFailure` is the cross-language failure projection. Keep its categories,
  phases, bounded issue tree, contract detail, and retry dispositions closed and
  versioned. Validate every deserialized failure; do not infer retry safety from
  transport loss or classify an undeclared plugin error as an expected failure.
- The runtime defines `cymule.plugin/2` messages but no process launcher.
  `ExpectedFailure` is an explicit application outcome; `Defect`, process
  termination, and malformed responses remain defects. Once Effect dispatch
  starts, every missing or unusable outcome first records `Unknown` and projects
  reconciliation, never same-request retry.
- Missing, invalid-variant, or schema-invalid dispatch and reconciliation
  outputs retain the original intent as `Unknown`. Embedded execution never
  auto-releases an `Explicit` effect.
- Embedded completion, wait, explicit release, and reconciliation are closed
  success-side `ExecutionOutcome` variants. Never flatten release or
  reconciliation-required state into an Engine failure string.
