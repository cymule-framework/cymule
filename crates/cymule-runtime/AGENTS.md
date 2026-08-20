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
