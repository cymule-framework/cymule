# Runtime Guidance

- The runtime interprets frozen IR and adapts abstract operations to substrate
  interfaces. It must not redefine core identities or transition laws.
- `PluginHost` is the authority boundary for external execution. A manifest is a
  capability advertisement, not authorization.
- Every plugin call pins an immutable occurrence binding before execution.
- Mutating effects remain staged until scope commit unless an explicit release
  policy says otherwise. Dispatch ambiguity must be recorded as `unknown` before
  reconciliation.
- Keep the reference runtime synchronous and dependency-light. Production async,
  durable, and distributed realizations should be separate adapters.

