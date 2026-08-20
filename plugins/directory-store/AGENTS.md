# Directory Store Adapter Guidance

- This adapter is a concrete local-filesystem realization of `DurableStore`, not
  part of Cymule's canonical storage contract.
- Keep one complete canonical `StoredState` in `state.json`; writes attempt
  non-blocking writer exclusion, verify the expected revision, fsync a sibling
  staging file, atomically rename it, and fsync the directory on Unix.
- Never wait on the adapter-local writer file. Contention returns a conflict so
  the caller can retry under its normal backoff and fencing policy. Prefer a
  substrate's native CAS in production adapters.
- Never acknowledge before the complete replacement is durable. Do not split
  Plans, Events, Artifacts, continuations, waits, leases, or outbox state into
  independently acknowledged files.
- Tests must cover reopen, stale writers, interrupted staging residue, and
  malformed bytes. Production adapters for databases or object stores belong in
  separate plugin crates and must satisfy the same conformance behavior.
- Reopen fixtures use real Machine-retained Artifact inputs; never bypass
  durable reference closure with a syntactically valid missing digest.
