# Conformance Asset Guidance

- Fixtures are shared across language SDKs and must stay language-neutral.
- The expected Plan ID is always computed by the Rust kernel from the checked-in
  candidate; never duplicate canonicalization in a test script.
- Cross-language tests must seal and execute through the real engine and process
  plugin, not mocks.
- Resource fixtures are sealed only by the Rust engine. Every SDK must submit
  the shared candidate and receive the same Resource ID; no fixture may contain
  credentials or a signed URL.
- Add fault-oriented tests for semantic changes, especially stale commands,
  fencing, scope closure, ambiguous effects, reconciliation, and replay.
- Agent occurrence fixtures must pass both Draft 2020-12 shape validation and
  Rust request-digest/lifecycle validation. Provider names never enter them.
- Agent stream fixtures must contain explicit stable IDs, contiguous chunks,
  and a Rust-verified final digest. Every SDK submits them to the Rust reducer;
  fixtures never authorize an SDK-local reducer.
