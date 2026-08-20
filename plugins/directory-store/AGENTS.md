# Directory Store Adapter Guidance

- This adapter is a concrete local-filesystem realization of `DurableStore`, not
  part of Cymule's canonical storage contract.
- Keep immutable content-addressed segment/checkpoint files and one small
  `head.json`. Writes attempt non-blocking writer exclusion, verify the exact
  expected head, fsync new immutable objects, atomically replace the head, and
  fsync owning directories on Unix.
- Never wait on the adapter-local writer file. Contention returns a conflict so
  the caller can retry under its normal backoff and fencing policy. Prefer a
  substrate's native CAS in production adapters.
- Never acknowledge before immutable objects and the new head are durable. Do
  not give Plans, Events, Artifacts, continuations, waits, leases, or outbox
  state independently movable heads.
- Reject legacy `state.json` during normal open. Offline migration must hold the
  old writer claim, validate the legacy revision, and be recoverable only
  through the explicit migration path; never add mixed runtime fallback.
- Tests must cover bounded reopen, stale writers, interrupted staging residue,
  malformed bytes, explicit legacy migration, and cold reclamation. Production adapters for databases or object stores belong in
  separate plugin crates and must satisfy the same conformance behavior.
- Reopen fixtures use real Machine-retained Artifact inputs; never bypass
  durable reference closure with a syntactically valid missing digest.
