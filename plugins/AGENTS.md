# Plugin Guidance

- Plugins implement abstract operations and substrates. They do not own plan,
  event, command, scope, effect, or replay semantics.
- Integration plugins may own domain-specific projections and controllers such
  as Agent Sessions or transport streams, but these must lower to framework
  waits, effects, resources, and durable journals without becoming core truth.
- A manifest advertises implemented operations, stable revisions, and
  reconciliation capability; it never grants authority or selects itself.
- Effect adapters must preserve structural intent identity across prepare,
  dispatch, retry, receipt verification, and reconciliation.
- An ambiguous dispatch returns `unknown`. Never hide it as a generic error or
  create a fresh intent.
- Concrete provider plugins must live in separately reviewable packages and must
  document credentials, egress, idempotency, reconciliation, and failure modes.
- Official reusable plugins may publish as independent crates only when their
  normalized package compiles against published Cymule contracts. Test adapters
  and examples remain `publish = false`.
