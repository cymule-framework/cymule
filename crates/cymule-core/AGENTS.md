# Core Kernel Guidance

- This crate is the complete trusted semantic core. Keep it small, synchronous,
  deterministic, and free of ambient I/O.
- Never read the clock, random source, environment, filesystem, or network.
- Canonical IDs are computed only after semantic validation with the versioned
  JCS encoding in `canonical.rs`.
- `artifact_ref` is the sole authority for the `cymule.artifact/1` identity
  preimage. Higher layers must call it rather than copy its framing.
- Reducers are pure over prior projection plus event. Do not hide mutations in
  caches or global state.
- Preserve closed effect, scope, attempt, and Run state machines. Illegal jumps
  fail closed.
- `Operation::Invoke` targets a definition in the same sealed Plan. Keep
  definition lookup semantic and immutable; logical registries and future-head
  selection belong in `cymule-evolution`.
- Do not add a provider name or transport detail to IR, events, or projections.
- `MachineSnapshot::command_digests` exposes only stable validation evidence for
  durable exact-delta checks. Keep the private command-record representation and
  reduction semantics inside this core.
- A compacted Machine snapshot may replace only a causally closed Event prefix
  with an authenticated base projection plus exact compacted Event identities.
  Resume replays the full retained suffix from that base, command receipts keep
  deduplication authority, and a parent outside base plus suffix fails closed.
- Property failures persist under `proptest-regressions/`. Commit the minimized
  corpus file with its fix; never depend on an ephemeral CI seed alone.
- Changes here require specification, schema, conformance, and SDK review.
