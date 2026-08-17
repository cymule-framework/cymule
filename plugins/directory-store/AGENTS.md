# Directory Store Adapter Guidance

- This adapter is a concrete local-filesystem realization of `DurableStore`, not
  part of Cymule's canonical storage contract.
- Keep one complete canonical `StoredState` in `state.json`; writes take the
  advisory lock, verify the expected revision, fsync a sibling staging file,
  atomically rename it, and fsync the directory on Unix.
- Never acknowledge before the complete replacement is durable. Do not split
  Plans, Events, Artifacts, continuations, waits, leases, or outbox state into
  independently acknowledged files.
- Tests must cover reopen, stale writers, interrupted staging residue, and
  malformed bytes. Production adapters for databases or object stores belong in
  separate plugin crates and must satisfy the same conformance behavior.
