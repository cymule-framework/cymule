# Core Kernel Guidance

- This crate is the complete trusted semantic core. Keep it small, synchronous,
  deterministic, and free of ambient I/O.
- Never read the clock, random source, environment, filesystem, or network.
- Canonical IDs are computed only after semantic validation with the versioned
  JCS encoding in `canonical.rs`.
- `decode_json` is the sole raw JSON decoder for trusted Rust wire and canonical
  bytes. It rejects duplicate object members recursively before typed Serde
  decoding; do not add a permissive parser or fallback path.
- `artifact_ref` is the sole authority for the collision-free,
  length-prefixed `cymule.artifact/2` identity preimage. Artifact references pin
  this identity version; higher layers must call the helper rather than copy its
  framing. Snapshot v5 is the only accepted snapshot wire version.
- Reducers are pure over prior projection plus event. Do not hide mutations in
  caches or global state.
- Preserve closed effect, scope, attempt, and Run state machines. Illegal jumps
  fail closed.
- Effect admission resolves the exact entry-reachable Plan site and retains its
  complete structural identity preimage and Effect profile in canonical state
  so replay enforces dispatch and reconciliation without a provider.
- Scope and Effect commands carry an entry-rooted invocation path plus exact
  definition and Region path. Core derives the invocation ID and rejects a
  nested or invoked site attached to an unrelated execution scope.
- Scope closure rejects every open descendant and requires the exact
  reducer-derived obligation set; callers never author obligation membership or
  resolution.
- `Operation::Invoke` targets a definition in the same sealed Plan. Keep
  definition lookup semantic and immutable; logical registries and future-head
  selection belong in `cymule-evolution`.
- Reject self-recursion and every recursive invocation SCC before Plan identity
  is computed; collect invokes through nested scopes and permit acyclic diamonds.
- `seal_plan` is the sole Plan sealer. Its pure Draft 2020-12 compilation uses
  the maintained schema library with external retrieval disabled.
- Do not add a provider name or transport detail to IR, events, or projections.
- `MachineSnapshot::command_digests` exposes only stable validation evidence for
  durable exact-delta checks. Keep the private command-record representation and
  reduction semantics inside this core.
- A compacted Machine snapshot may replace only a causally closed Event prefix
  with an authenticated base projection plus exact compacted Event identities.
  Resume replays the full retained suffix from that base, command receipts keep
  deduplication authority, and a parent outside base plus suffix fails closed.
- Snapshot restore also closes Event and command authority in both directions:
  every Event has exactly one applied receipt, every applied receipt names a
  retained or compacted Event, and retained command IDs and hashes match.
- A compacted prefix retains ordered Event ID, command ID/hash, and complete
  command-record digest evidence. Its prefix digest is recomputed from that
  cumulative evidence plus the authenticated projection digest; shape-only
  prefix digests are invalid.
- Property failures persist under `proptest-regressions/`. Commit the minimized
  corpus file with its fix; never depend on an ephemeral CI seed alone.
- Changes here require specification, schema, conformance, and SDK review.
