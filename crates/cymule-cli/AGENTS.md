# CLI Guidance

- The CLI is a transport and operator tool over the library contracts. It does
  not own semantic behavior.
- `rpc` JSON is the cross-language conformance boundary; changes require all SDK
  tests and a command-protocol version decision.
- `seal_resource` is an additive request/response pair over
  `cymule.resource/1`. The CLI delegates validation and identity to
  `cymule-resource`; it must never compute Resource IDs independently.
- `verify_wait_activation` validates only the versioned provider-neutral
  delivery record. Stateful source matching, consume-once admission, and
  Continuation readiness remain `cymule-durable` CAS operations.
- `verify_evolution_command` validates only the closed
  `cymule.evolution-control/2` envelope. Plan linking, adapter execution,
  evidence counting, and durable promotion remain `cymule-evolution` authority.
- Write only the response JSON to stdout. Diagnostics go to stderr.
- Never expose unrestricted raw event append.
- The package is `cymule-cli` and installs the `cymule` binary. Keep binary
  rustdoc disabled so it cannot collide with the public `cymule` facade library;
  user API documentation belongs to the facade and profile crates.
