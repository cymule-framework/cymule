# Core Kernel Guidance

- This crate is the complete trusted semantic core. Keep it small, synchronous,
  deterministic, and free of ambient I/O.
- Never read the clock, random source, environment, filesystem, or network.
- Canonical IDs are computed only after semantic validation with the versioned
  JCS encoding in `canonical.rs`.
- Reducers are pure over prior projection plus event. Do not hide mutations in
  caches or global state.
- Preserve closed effect, scope, attempt, and Run state machines. Illegal jumps
  fail closed.
- Do not add a provider name or transport detail to IR, events, or projections.
- Changes here require specification, schema, conformance, and SDK review.

