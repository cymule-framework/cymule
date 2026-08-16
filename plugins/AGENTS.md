# Plugin Guidance

- Plugins implement abstract operations and substrates. They do not own plan,
  event, command, scope, effect, or replay semantics.
- A manifest advertises implemented operations, stable revisions, and
  reconciliation capability; it never grants authority or selects itself.
- Effect adapters must preserve structural intent identity across prepare,
  dispatch, retry, receipt verification, and reconciliation.
- An ambiguous dispatch returns `unknown`. Never hide it as a generic error or
  create a fresh intent.
- Concrete provider plugins must live in separately reviewable packages and must
  document credentials, egress, idempotency, reconciliation, and failure modes.
