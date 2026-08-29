# Plugin Guidance

- Plugins implement abstract operations and substrates. They do not own plan,
  event, command, scope, effect, or replay semantics.
- Integration plugins may own domain-specific projections and controllers such
  as Agent Sessions or transport streams, but these must lower to framework
  waits, effects, resources, and durable journals without becoming core truth.
- A manifest advertises implemented operations, stable revisions, and
  reconciliation capability; it never grants authority or selects itself.
- Process and persisted JSON ingress uses the core duplicate-rejecting decoder;
  a plugin must not collapse duplicate object members before protocol or state
  validation.
- Effect adapters must preserve structural intent identity across prepare,
  dispatch, retry, receipt verification, and reconciliation.
- An ambiguous dispatch returns `unknown`. Never hide it as a generic error or
  create a fresh intent.
- Concrete provider plugins must live in separately reviewable packages and must
  document credentials, egress, idempotency, reconciliation, and failure modes.
- Official reusable plugins may publish as independent crates only when their
  normalized package compiles against published Cymule contracts. Test adapters
  and examples remain `publish = false`.
- Day-one official adapters are SQLite/directory durable stores, filesystem and
  Apache object-store Resources, HTTP/timer activation, restart-monotonic clock
  observation, process execution, OpenTelemetry export, and RMCP tool mapping.
  Each directory owns focused conformance and must remain independently
  testable.
- Process-executor binding revisions cover the complete admitted launch
  configuration, not only executable bytes. Unix process occurrences use a
  fresh captured closure and an isolated process group; platform sandboxes
  remain separate plugins.
- Executable capture accepts only a bounded regular file through a nonblocking,
  no-symlink descriptor. FIFO, device, symlink, and over-limit inputs fail
  before any invocation deadline could be bypassed. The closure limit covers
  executable bytes plus the optional working tree.
- Malformed closed-protocol JSON is a deterministic plugin defect for describe,
  Call, migration, and shadow operations. The same response loss after Effect
  dispatch starts remains unknown-world reconciliation.
- Process owner cancellation is a typed cancellation before provider start and
  for non-mutating invocations. Cancellation after Effect dispatch starts is an
  unknown world outcome under the original intent, never a substrate retry.
