# CLI Guidance

- The CLI is a transport and operator tool over the library contracts. It does
  not own semantic behavior.
- `rpc` JSON is the cross-language conformance boundary; changes require all SDK
  tests and a command-protocol version decision.
- `execute_durable` opens the configured store and immutable process binding,
  then delegates the complete command to `DurableRuntimeControl`.
  `execute_live_evolution` checkpoints through
  `DurableLiveEvolutionController`; neither route may return a verify-only
  receipt as if state changed.
- `seal_resource` is an additive request/response pair over
  `cymule.resource/2`. The CLI delegates validation and identity to
  `cymule-resource`; it must never compute Resource IDs independently.
- `verify_wait_activation` validates only the versioned provider-neutral
  delivery record. Stateful source matching, consume-once admission, and
  Continuation readiness remain `cymule-durable` CAS operations.
- `verify_evolution_command` validates only the closed
  `cymule.evolution-control/4` envelope. Plan linking, adapter execution,
  evidence counting, and durable promotion remain `cymule-evolution` authority.
- Write only the response JSON to stdout. Diagnostics go to stderr.
- RPC domain failures return a successful process transport containing one
  `cymule.engine/2` failure envelope. A nonzero process status is reserved for
  failure to carry the protocol itself; never duplicate a semantic failure on
  stderr or emit an unversioned success payload.
- Never expose unrestricted raw event append.
- Local process execution uses only `cymule-executor-process`. It copies the
  selected executable into a private sealed location, hashes those exact launch
  bytes, and seals the advertised manifest into `cymule.execution-binding/2`
  before constructing the runtime. There is no second launcher, mutable-path,
  ambient-environment, or implementation-ID-only binding fallback.
- The package is `cymule-cli` and installs the `cymule` binary. Keep binary
  rustdoc disabled so it cannot collide with the public `cymule` facade library;
  user API documentation belongs to the facade and profile crates.
- Dispatch provider-neutral store targets to directory or SQLite adapters.
  Read-only commands never construct an executor. Migration and shadow calls
  require a sealed process whose digest matches its returned descriptor.
